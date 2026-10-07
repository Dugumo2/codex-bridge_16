//! 第 3 档接入：私有子进程的 JSONL 管道、握手和有界回收。

use std::fmt;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use super::rpc::{self, ClientHandle, FailureReporter, MessageWriter};
use super::{AppConnection, AppError, AppHandle, ServerInfo};
use crate::ipc::frame::LineFramer;

const PROCESS_EXIT: &str = "App server process exited unexpectedly";
const STARTUP_RETRY_DELAY: Duration = Duration::from_millis(750);
const STDIN_EXIT_WAIT: Duration = Duration::from_secs(5);
const KILL_EXIT_WAIT: Duration = Duration::from_secs(3);
const STDERR_TAIL_LIMIT: usize = 4000;
// cmd 退出时，正常管道的 EOF 会很快到达；被子孙继承的管道不能无限等待。
const PIPE_DRAIN_WAIT: Duration = Duration::from_millis(250);

#[derive(Debug, Clone)]
pub struct PrivateOptions {
    pub cwd: PathBuf,
    /// 仅供可信配置使用的固定 argv；不接受来自 RPC 参数的命令或用户输入拼接。
    /// Windows 会经 cmd.exe /c 执行，并拒绝 shell 元字符。
    pub command: Vec<String>,
    pub request_timeout_ms: u64,
}

impl Default for PrivateOptions {
    fn default() -> Self {
        Self {
            cwd: PathBuf::from("."),
            command: vec!["codex".into(), "app-server".into()],
            request_timeout_ms: rpc::DEFAULT_REQUEST_TIMEOUT_MS,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateFailure {
    Locked,
    Unopenable,
    Uninitialized,
    None,
}

/// 优先识别具体 SQLite 原因，最后才识别 Codex 的初始化包装错误。
pub fn classify_state_failure(stderr: &str) -> StateFailure {
    let text = stderr.to_ascii_lowercase();
    if ["unable to open database", "sqlite_cantopen"]
        .iter()
        .any(|s| text.contains(s))
    {
        StateFailure::Unopenable
    } else if [
        "database is locked",
        "database table is locked",
        "sqlite_busy",
        "sqlite_locked",
    ]
    .iter()
    .any(|s| text.contains(s))
    {
        StateFailure::Locked
    } else if [
        "failed to initialize sqlite state runtime",
        "failed to initialize the sqlite state runtime",
        "failed to initialize state runtime",
    ]
    .iter()
    .any(|s| text.contains(s))
    {
        StateFailure::Uninitialized
    } else {
        StateFailure::None
    }
}

#[derive(Debug)]
pub struct StartupError {
    pub cause: AppError,
    pub stderr_tail: String,
    pub state_failure: StateFailure,
    pub retried: bool,
}

impl StartupError {
    fn new(cause: AppError, stderr_tail: String) -> Self {
        Self {
            cause,
            state_failure: classify_state_failure(&stderr_tail),
            stderr_tail,
            retried: false,
        }
    }

    fn is_transient(&self) -> bool {
        self.state_failure != StateFailure::Unopenable
            && matches!(&self.cause, AppError::Closed(reason) if reason.contains(PROCESS_EXIT))
    }
}

impl fmt::Display for StartupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "连接 Codex app-server 失败：")?;
        match &self.cause {
            AppError::Closed(reason) if reason == PROCESS_EXIT => {
                write!(f, "子进程在启动期间意外退出。")?
            }
            AppError::Timeout { method, timeout_ms } => {
                write!(f, "请求 {method} 超过 {timeout_ms} 毫秒未收到响应。")?
            }
            AppError::Rpc { code, message, .. } => write!(f, "JSON-RPC 错误 {code}：{message}。")?,
            AppError::Closed(reason) => write!(f, "{reason}。")?,
        }
        if self.retried {
            write!(f, "已重试一次，第二次仍然失败。")?;
        }
        let dir = state_directory(&self.stderr_tail);
        match self.state_failure {
            StateFailure::Unopenable => write!(f,
                "无法打开 {dir} 中的状态数据库。目录可能不存在、不可写或与 Codex 使用的位置不一致；重试无助于解决。请检查目录及 CODEX_HOME，然后运行 cxb doctor。")?,
            StateFailure::Locked => write!(f,
                "其他 Codex 进程正在使用 {dir} 中的共享状态数据库。app-server 启动时会独占加锁。请稍等几秒后再运行；若持续失败，请检查卡住的 app-server 进程并运行 cxb doctor。")?,
            StateFailure::Uninitialized => write!(f,
                "无法初始化 {dir} 中的状态数据库。可能是多个 Codex 同时启动，也可能是目录不可写、磁盘已满或数据库损坏。请根据 app-server stderr 中的底层原因检查运行中的进程，并运行 cxb doctor。")?,
            StateFailure::None => {}
        }
        if !self.stderr_tail.is_empty() {
            write!(f, "\napp-server stderr：\n{}", self.stderr_tail)?;
        }
        Ok(())
    }
}

impl std::error::Error for StartupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

fn state_directory(stderr: &str) -> String {
    for line in stderr.lines() {
        for marker in ["state runtime under ", "state runtime at "] {
            if let Some((_, path)) = line.split_once(marker) {
                // 分隔符是冒号加空白，不能误切 Windows 盘符或带空格的目录。
                let end = path
                    .char_indices()
                    .find_map(|(i, ch)| {
                        (ch == ':' && path[i + 1..].starts_with(char::is_whitespace)).then_some(i)
                    })
                    .unwrap_or(path.len());
                return path[..end].trim_end().to_owned();
            }
        }
    }
    std::env::var("CODEX_HOME").unwrap_or_else(|_| "~/.codex".into())
}

/// 只重试启动期间的意外退出；CODEX_HOME 初始化互斥量由 [`super::select`] 在外层持有，不在本层实现。
pub async fn connect_private(options: PrivateOptions) -> Result<AppConnection, StartupError> {
    match connect_once(&options).await {
        Ok(connection) => Ok(connection),
        Err(error) if error.is_transient() => {
            tracing::warn!(state_failure = ?error.state_failure,
                "app-server 启动期意外退出，750 毫秒后重试一次");
            tokio::time::sleep(STARTUP_RETRY_DELAY).await;
            connect_once(&options).await.map_err(|mut error| {
                error.retried = true;
                error
            })
        }
        Err(error) => Err(error),
    }
}

fn spawn_process(options: &PrivateOptions) -> Result<Child, StartupError> {
    let error = |reason: String| StartupError::new(AppError::Closed(reason), String::new());
    let Some(program) = options.command.first().filter(|s| !s.is_empty()) else {
        return Err(error("启动命令不能为空".into()));
    };
    #[cfg(windows)]
    let mut command =
        {
            // 不拼接 shell 字符串；额外限制 argv，防止 cmd 把配置参数当成命令语法。
            if options.command.iter().any(|arg| {
                arg.contains(['&', '|', '<', '>', '^', '%', '!', '(', ')', '"', '\r', '\n'])
            }) {
                return Err(error(
                    "启动命令只允许固定字面量参数，不能包含 cmd 元字符".into(),
                ));
            }
            let mut command = Command::new("cmd.exe");
            command.arg("/c").arg(program).args(&options.command[1..]);
            command.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
            command
        };
    #[cfg(not(windows))]
    let mut command = {
        let mut command = Command::new(program);
        command.args(&options.command[1..]);
        command
    };
    command
        .current_dir(&options.cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command
        .spawn()
        .map_err(|e| error(format!("无法启动 app-server：{e}。请确认 Codex CLI 已安装")))
}

struct StdinWriter {
    stdin: ChildStdin,
    exited: watch::Receiver<bool>,
}

impl MessageWriter for StdinWriter {
    async fn write(&mut self, mut text: String) -> Result<(), String> {
        text.push('\n');
        if let Err(error) = self.stdin.write_all(text.as_bytes()).await {
            // EPIPE 可能抢先于退出监控就绪，让监控先发布可重试的意外退出原因。
            let _ =
                tokio::time::timeout(PIPE_DRAIN_WAIT, self.exited.wait_for(|exited| *exited)).await;
            return Err(format!("app-server stdin：{error}"));
        }
        Ok(())
    }
}

/// 握手 future 被取消时也发出关闭信号，独立任务继续完成进程回收。
struct StartupGuard(Option<ClientHandle>);

impl Drop for StartupGuard {
    fn drop(&mut self) {
        if let Some(client) = self.0.take() {
            client.fail("app-server 握手已取消");
        }
    }
}

async fn connect_once(options: &PrivateOptions) -> Result<AppConnection, StartupError> {
    let mut child = spawn_process(options)?;
    let pid = child.id();
    #[cfg(windows)]
    let instance = child
        .raw_handle()
        .and_then(|h| crate::store::lifecycle::instance_from_handle(pid?, h as _));
    #[cfg(not(windows))]
    let instance: Option<crate::store::lifecycle::Instance> = None;
    let stdin = child.stdin.take().expect("已配置 stdin 管道");
    let mut stdout = child.stdout.take().expect("已配置 stdout 管道");
    let stderr = child.stderr.take().expect("已配置 stderr 管道");
    let tail = Arc::new(Mutex::new(String::new()));
    let stderr_tail = tail.clone();
    let stderr_task = tokio::spawn(collect_stderr(stderr, stderr_tail));
    let (input, inbound) = mpsc::unbounded_channel();
    let (stop, stop_rx) = oneshot::channel();
    let (done, mut done_rx) = watch::channel(false);
    let (exited, mut exited_rx) = watch::channel(false);
    let (client, events) = rpc::new_client(
        inbound,
        StdinWriter {
            stdin,
            exited: exited_rx.clone(),
        },
        move || async move {
            let _ = stop.send(());
            let _ = done_rx.wait_for(|done| *done).await;
        },
        Some(options.request_timeout_ms),
    );
    let mut guard = StartupGuard(Some(client.clone()));
    if let Some(ref instance) = instance {
        client.set_private_instance(instance.clone(), exited_rx.clone());
    }
    let stdout_reporter = client.failure_reporter();
    let stdout_input = input.clone();
    let stdout_task = tokio::spawn(async move {
        let mut framer = LineFramer::default();
        let mut buffer = [0; 8192];
        loop {
            match stdout.read(&mut buffer).await {
                Ok(0) => {
                    // EOF 可能比 Child::wait 更早就绪，短暂等监控确认进程退出原因。
                    if tokio::time::timeout(PIPE_DRAIN_WAIT, exited_rx.wait_for(|exited| *exited))
                        .await
                        .is_err()
                    {
                        stdout_reporter.fail("app-server stdout 已结束");
                    }
                    break;
                }
                Ok(n) => match framer.push(&buffer[..n]) {
                    Ok(lines) => {
                        for line in lines {
                            if stdout_input.send(line).is_err() {
                                return;
                            }
                        }
                    }
                    Err(error) => {
                        stdout_reporter
                            .fail(format!("app-server stdout 超出 10 MiB 上限：{error}"));
                        break;
                    }
                },
                Err(error) => {
                    stdout_reporter.fail(format!("读取 app-server stdout 失败：{error}"));
                    break;
                }
            }
        }
    });
    let reporter = client.failure_reporter();
    let exit_events = client.clone();
    tokio::spawn(async move {
        supervise(child, stop_rx, reporter, exited, exit_events, instance).await;
        // 保留入站发送端到退出原因已发布，避免 EOF 抢先覆盖意外退出的重试信号。
        drop(input);
        tokio::join!(finish_pipe(stdout_task), finish_pipe(stderr_task));
        done.send_replace(true);
    });
    match super::initialize(&client).await {
        Ok(user_agent) => {
            guard.0 = None;
            Ok(AppConnection {
                handle: AppHandle { inner: client },
                events,
                server: ServerInfo::Private { pid },
                user_agent,
            })
        }
        Err(cause) => {
            client.close().await;
            let stderr_tail = tail.lock().unwrap().clone();
            Err(StartupError::new(cause, stderr_tail))
        }
    }
}

async fn supervise(
    mut child: Child,
    mut stop: oneshot::Receiver<()>,
    reporter: FailureReporter,
    exited: watch::Sender<bool>,
    exit_events: super::rpc::ClientHandle,
    instance: Option<crate::store::lifecycle::Instance>,
) {
    tokio::select! {
        biased;
        result = child.wait() => {
            if let Err(error) = result {
                tracing::warn!(%error, "等待 app-server 退出失败");
            }
            exited.send_replace(true);
            if let Some(instance) = instance.clone() {
                exit_events.private_instance_exited(instance);
            }
            reporter.fail(PROCESS_EXIT);
            return;
        }
        _ = &mut stop => {}
    }
    // RPC 写任务先丢弃 stdin，再通知本任务；从这里起执行 5 秒 + 3 秒的关闭时序。
    match tokio::time::timeout(STDIN_EXIT_WAIT, child.wait()).await {
        Ok(Ok(_)) => {
            exited.send_replace(true);
            if let Some(instance) = instance.clone() {
                exit_events.private_instance_exited(instance);
            }
            return;
        }
        Ok(Err(error)) => tracing::warn!(%error, "等待 app-server 优雅退出失败"),
        Err(_) => {}
    }
    // Windows 只终止 cmd.exe 本身；整棵进程树由 broker 的 Job 模块兜底。
    if let Err(error) = child.start_kill() {
        tracing::warn!(%error, "强制结束 app-server 失败");
    }
    match tokio::time::timeout(KILL_EXIT_WAIT, child.wait()).await {
        Ok(Ok(_)) => {
            exited.send_replace(true);
        }
        Ok(Err(error)) => tracing::warn!(%error, "等待 app-server 强制退出失败"),
        Err(_) => tracing::warn!("强制结束 app-server 后等待超过 3 秒，交由 Job 兜底"),
    }
    if let Some(instance) = instance {
        exit_events.private_instance_exited(instance);
    }
}

pub(super) async fn finish_pipe(mut task: JoinHandle<()>) {
    if tokio::time::timeout(PIPE_DRAIN_WAIT, &mut task)
        .await
        .is_err()
    {
        task.abort();
        let _ = task.await;
    }
}

/// 私有档与 proxy 共用的有界 stderr 尾巴，保持跨块 UTF-8 解码和 4000 字符上限。
pub(super) async fn collect_stderr(
    mut stderr: tokio::process::ChildStderr,
    tail: Arc<Mutex<String>>,
) {
    let mut buffer = [0; 8192];
    let mut decoder = Utf8Decoder::default();
    loop {
        match stderr.read(&mut buffer).await {
            Ok(0) => {
                retain_stderr(&tail, &decoder.finish());
                break;
            }
            Ok(n) => retain_stderr(&tail, &decoder.push(&buffer[..n])),
            Err(error) => {
                tracing::warn!(%error, "读取 app-server stderr 失败");
                break;
            }
        }
    }
}

fn retain_stderr(tail: &Mutex<String>, text: &str) {
    if text.is_empty() {
        return;
    }
    tracing::info!("app-server stderr：{text}");
    let mut tail = tail.lock().unwrap();
    tail.push_str(text);
    let excess = tail.chars().count().saturating_sub(STDERR_TAIL_LIMIT);
    if excess > 0 {
        let end = tail
            .char_indices()
            .nth(excess)
            .map_or(tail.len(), |(i, _)| i);
        tail.drain(..end);
    }
}

/// 跨块拼接 UTF-8，避免把一个汉字拆成替换字符，影响原样尾巴与错误分类。
#[derive(Default)]
struct Utf8Decoder {
    pending: Vec<u8>,
}

impl Utf8Decoder {
    fn push(&mut self, chunk: &[u8]) -> String {
        self.pending.extend_from_slice(chunk);
        let mut text = String::new();
        let mut offset = 0;
        while offset < self.pending.len() {
            match std::str::from_utf8(&self.pending[offset..]) {
                Ok(valid) => {
                    text.push_str(valid);
                    offset = self.pending.len();
                }
                Err(error) => {
                    let end = offset + error.valid_up_to();
                    text.push_str(std::str::from_utf8(&self.pending[offset..end]).unwrap());
                    offset = end;
                    if let Some(len) = error.error_len() {
                        text.push('\u{fffd}');
                        offset += len;
                    } else {
                        break;
                    }
                }
            }
        }
        self.pending.drain(..offset);
        text
    }

    fn finish(&mut self) -> String {
        let text = String::from_utf8_lossy(&self.pending).into_owned();
        self.pending.clear();
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 状态库分类遵循具体原因优先且大小写不敏感() {
        for (text, expected) in [
            (
                "failed to initialize the sqlite state runtime: SQLITE_CANTOPEN; SQLITE_BUSY",
                StateFailure::Unopenable,
            ),
            ("unable to open database", StateFailure::Unopenable),
            ("database is locked", StateFailure::Locked),
            ("database table is locked", StateFailure::Locked),
            ("SQLITE_LOCKED", StateFailure::Locked),
            ("SQLITE_BUSY", StateFailure::Locked),
            (
                "failed to initialize sqlite state runtime",
                StateFailure::Uninitialized,
            ),
            (
                "failed to initialize state runtime",
                StateFailure::Uninitialized,
            ),
            ("other error", StateFailure::None),
        ] {
            assert_eq!(classify_state_failure(text), expected);
        }
    }

    #[test]
    fn stderr_跨块保留原文且按字符截取末尾四千字() {
        let tail = Mutex::new(String::new());
        let mut decoder = Utf8Decoder::default();
        let bytes = "中文 database is locked\n".as_bytes();
        retain_stderr(&tail, &decoder.push(&bytes[..1]));
        retain_stderr(&tail, &decoder.push(&bytes[1..20]));
        retain_stderr(&tail, &decoder.push(&bytes[20..]));
        assert_eq!(*tail.lock().unwrap(), "中文 database is locked\n");
        retain_stderr(&tail, &"甲".repeat(4100));
        assert_eq!(*tail.lock().unwrap(), "甲".repeat(4000));
        assert_eq!(decoder.push(b"\xffx\xe4"), "\u{fffd}x");
        assert_eq!(decoder.finish(), "\u{fffd}");
    }

    #[test]
    fn 失败说明保留带空格的状态路径且仅实际重试时标明() {
        let mut error = StartupError::new(AppError::Closed(PROCESS_EXIT.into()),
            "failed to initialize sqlite state runtime under C:\\Users\\First Last\\.codex: database is locked".into());
        assert!(error.is_transient());
        assert!(error.to_string().contains("C:\\Users\\First Last\\.codex"));
        assert!(error.to_string().contains("cxb doctor"));
        assert!(!error.to_string().contains("已重试"));
        error.retried = true;
        assert!(error.to_string().contains("已重试一次"));
        assert!(!StartupError::new(
            AppError::Closed(PROCESS_EXIT.into()),
            "unable to open database".into()
        )
        .is_transient());
        assert!(!StartupError::new(
            AppError::Timeout {
                method: "initialize".into(),
                timeout_ms: 30
            },
            String::new()
        )
        .is_transient());
    }
}
