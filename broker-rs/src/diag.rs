//! 诊断代运行：不初始化 broker 日志、数据根或进程级 Job。

use std::ffi::OsString;
use std::io::Write;
use std::path::{Path, PathBuf};

use clap::Args;
use serde::Serialize;

#[cfg(windows)]
#[path = "diag/windows.rs"]
mod windows;

#[derive(Debug, Args)]
pub struct DiagArgs {
    /// 诊断期限，单位毫秒，范围 1 到 600000。
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..=600000))]
    pub timeout_ms: u64,
    /// 结果文件的绝对路径，父目录必须已存在。
    #[arg(long, value_parser = status_path)]
    pub status_file: PathBuf,
    /// 要执行的程序及其参数，必须置于 -- 后，程序必须为绝对路径。
    #[arg(last = true, required = true, num_args = 1..)]
    pub command: Vec<OsString>,
}

fn status_path(value: &str) -> Result<PathBuf, String> {
    let path = PathBuf::from(value);
    if !path.is_absolute() || path.file_name().is_none() {
        return Err("状态文件必须为绝对文件路径".into());
    }
    if !path.parent().is_some_and(Path::is_dir) {
        return Err("状态文件所在目录必须已存在".into());
    }
    Ok(path)
}

/// 与 clap 同样的参数错误出口；在任何状态文件写入之前执行。
pub fn validate(args: &DiagArgs) -> Result<(), clap::Error> {
    if !args
        .command
        .first()
        .is_some_and(|p| Path::new(p).is_absolute())
    {
        return Err(clap::Error::raw(
            clap::error::ErrorKind::ValueValidation,
            "诊断程序必须为绝对路径",
        ));
    }
    Ok(())
}

#[derive(Debug, Serialize, PartialEq)]
#[serde(tag = "outcome", rename_all = "lowercase")]
pub enum Outcome {
    Exited {
        #[serde(rename = "exitCode")]
        exit_code: u32,
    },
    Timeout,
    Failed {
        reason: String,
    },
}

impl Outcome {
    fn code(&self) -> i32 {
        match self {
            Self::Exited { exit_code } => *exit_code as i32,
            Self::Timeout => 124,
            Self::Failed { .. } => 125,
        }
    }
}

pub fn run(args: &DiagArgs) -> i32 {
    if let Err(error) = validate(args) {
        let _ = error.print();
        return 2;
    }
    #[cfg(windows)]
    let result = windows::execute(args);
    #[cfg(not(windows))]
    let result: Result<Outcome, String> = Err("当前平台不支持诊断代运行".into());
    let outcome = result.unwrap_or_else(|reason| Outcome::Failed { reason });
    if let Err(error) = write_status(&args.status_file, &outcome) {
        eprintln!("诊断状态文件写入失败：{error}");
        return 125;
    }
    outcome.code()
}

fn write_status(path: &Path, outcome: &Outcome) -> std::io::Result<()> {
    use std::fs::OpenOptions;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let parent = path.parent().expect("已校验绝对文件路径");
    let mut attempts = 0;
    let (temporary, mut file) = loop {
        attempts += 1;
        if attempts > 64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "诊断临时文件连续重名",
            ));
        }
        let candidate = parent.join(format!(
            ".cxb-diag-{}-{}.tmp",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => break (candidate, file),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    };
    let result = (|| {
        serde_json::to_writer(&mut file, outcome)?;
        file.flush()?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, path)
    })();
    if result.is_err() {
        std::fs::remove_file(&temporary)?;
    }
    result
}

/// cmd 不解释反斜杠；这里只拒绝元字符，绝不套用 C 运行库的转义。
pub fn cmd_safe(value: &str) -> bool {
    !value.contains([
        '&', '|', '<', '>', '^', '%', '!', '"', '(', ')', '\r', '\n', '\0',
    ])
}

/// 返回 /d /s /c 后的原始命令。force_quote 用于保持 proxy 原有的路径引号。
pub fn cmd_line(parts: &[(&str, bool)]) -> Result<String, String> {
    let mut line = String::from("/d /s /c \"");
    for (index, (value, force_quote)) in parts.iter().enumerate() {
        if !cmd_safe(value) {
            return Err("批处理程序路径或参数含 cmd 元字符、换行或空字符".into());
        }
        if index != 0 {
            line.push(' ');
        }
        let quote = *force_quote || value.is_empty() || value.contains([' ', '\t']);
        if quote {
            line.push('"');
        }
        line.push_str(value);
        if quote {
            line.push('"');
        }
    }
    line.push('"');
    Ok(line)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 批处理拒绝元字符换行和空字符且反斜杠原样保留() {
        for ch in [
            '&', '|', '<', '>', '^', '%', '!', '"', '(', ')', '\r', '\n', '\0',
        ] {
            assert!(cmd_line(&[(&format!("a{ch}b"), false)]).is_err());
        }
        assert_eq!(
            cmd_line(&[
                (r"D:\有 空格\a.cmd", false),
                ("", false),
                (r"尾 空格\", false)
            ])
            .unwrap(),
            "/d /s /c \"\"D:\\有 空格\\a.cmd\" \"\" \"尾 空格\\\"\""
        );
    }

    #[test]
    fn 状态与完整退出码互不混淆() {
        for code in [0, 7, 124, 125, 256, 0x8000_0001, u32::MAX] {
            let outcome = Outcome::Exited { exit_code: code };
            assert_eq!(outcome.code() as u32, code);
            assert_eq!(
                serde_json::to_value(outcome).unwrap(),
                serde_json::json!({"outcome":"exited","exitCode":code})
            );
        }
    }
}
