//! 先启用 stderr；文件日志在 store 有期限的初始化成功后才接入。

use std::io::{self, Write};
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_appender::rolling::RollingFileAppender;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[derive(Default)]
struct FileTarget {
    writer: Mutex<Option<NonBlocking>>,
    guard: Mutex<Option<WorkerGuard>>,
}

static FILE_TARGET: OnceLock<FileTarget> = OnceLock::new();

/// 此处不解析工作区，也不访问日志文件系统。
pub fn init() {
    let target = FILE_TARGET.get_or_init(FileTarget::default);
    let filter = EnvFilter::try_from_env("CXB_LOG").unwrap_or_else(|_| EnvFilter::new("info"));
    let stderr = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(false);
    let file = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_writer(move || {
            FileWriter(
                target
                    .writer
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .clone(),
            )
        });
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(stderr)
        .with(file)
        .try_init();
}

struct FileWriter(Option<NonBlocking>);
impl Write for FileWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match &mut self.0 {
            Some(writer) => writer.write(bytes),
            None => Ok(bytes.len()),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match &mut self.0 {
            Some(writer) => writer.flush(),
            None => Ok(()),
        }
    }
}

/// 只由存储初始化线程调用；返回值在初始化通过期限检查前不会接入订阅者。
pub(crate) fn prepare_file(workspace: &Path) -> Result<Option<RollingFileAppender>, String> {
    if FILE_TARGET.get().is_none() {
        return Ok(None);
    }
    build_file(workspace).map(Some)
}

fn build_file(workspace: &Path) -> Result<RollingFileAppender, String> {
    tracing_appender::rolling::Builder::new()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix("broker")
        .filename_suffix("log")
        .max_log_files(crate::store::BROKER_LOG_DAYS)
        .build(workspace.join("logs"))
        .map_err(|error| format!("创建文件日志失败：{error}"))
}

pub(crate) fn install_file(file: Option<RollingFileAppender>) {
    if let (Some(target), Some(file)) = (FILE_TARGET.get(), file) {
        target.install(file);
    }
}

impl FileTarget {
    fn install(&self, file: RollingFileAppender) {
        let (writer, guard) = tracing_appender::non_blocking::NonBlockingBuilder::default()
            .buffered_lines_limit(1024)
            .lossy(true)
            .finish(file);
        *self
            .writer
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(writer);
        let previous = self
            .guard
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .replace(guard);
        drop(previous);
    }

    fn shutdown(&self) {
        self.writer
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        let guard = self
            .guard
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        drop(guard);
    }
}

/// 主函数退出前排空文件日志；不持锁等待后台 I/O。
pub fn shutdown() {
    if let Some(target) = FILE_TARGET.get() {
        target.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 文件日志就绪前仅丢弃文件层_接入后可排空() {
        let root = std::env::temp_dir().join(format!(
            "cxb-logging-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let target = FileTarget::default();
        let mut unavailable = FileWriter(target.writer.lock().unwrap().clone());
        unavailable.write_all("初始化前\n".as_bytes()).unwrap();
        assert!(!root.join("logs").exists());
        target.install(build_file(&root).unwrap());
        FileWriter(target.writer.lock().unwrap().clone())
            .write_all("初始化后\n".as_bytes())
            .unwrap();
        target.shutdown();
        let path = std::fs::read_dir(root.join("logs"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        assert_eq!(std::fs::read_to_string(path).unwrap(), "初始化后\n");
        // 只清理本测试创建、位于系统临时目录的路径。
        assert!(root.starts_with(std::env::temp_dir()));
        std::fs::remove_dir_all(root).unwrap();
    }
}
