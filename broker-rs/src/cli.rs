//! 命令行：`serve` 与 `launch` 两个子命令（详细设计 2.1 节、第 5 节）。
//! `serve` 的参数与上游 `bun run src/broker-server.ts serve` 一致，黑盒测试工具层按
//! `<CXB_BROKER_BIN> serve --endpoint <ep> --idle-timeout <ms> [--cwd <dir>]` 启动本程序。

use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};

use crate::endpoint::Endpoint;
use crate::error::BrokerError;

#[path = "diag.rs"]
pub mod diag;

/// 空闲退出的缺省时长：5 分钟（详细设计第 8 节；上游为 30 分钟）。
pub const DEFAULT_IDLE_TIMEOUT_MS: u64 = 5 * 60 * 1000;

#[derive(Debug, Parser)]
#[command(name = "cxb-broker", version, about = "codex-bridge broker")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// 在指定端点上提供 broker 服务（每个工作区一个进程）。
    Serve(ServeCommand),
    /// 经中间进程拉起 serve 后立即退出，使 serve 脱离调用方的进程树（详细设计第 5 节）。
    Launch(ServeArgs),
    /// 在独立 Job 中代运行诊断程序，并原子写入结果。
    DiagExec(diag::DiagArgs),
}

#[derive(Debug, Clone, Args)]
pub struct ServeArgs {
    /// `unix:<路径>` 或 `pipe:<名字>`。
    #[arg(long)]
    pub endpoint: String,
    /// 工作区目录，缺省为当前目录；内部一律转成绝对路径。
    #[arg(long)]
    pub cwd: Option<PathBuf>,
    /// 空闲多少毫秒后自动退出，必须为正数。
    #[arg(long = "idle-timeout", value_parser = clap::value_parser!(u64).range(1..), default_value_t = DEFAULT_IDLE_TIMEOUT_MS)]
    pub idle_timeout_ms: u64,
}

/// serve 子命令：公共参数加上只由 launch 传入的隐藏参数。
#[derive(Debug, Clone, Args)]
pub struct ServeCommand {
    #[command(flatten)]
    pub args: ServeArgs,
    /// launch 本次是否带脱离标志创建成功（详细设计第 5 节）；直接启动 serve 时不传。
    /// 只进 broker.json 与 health 的检测依据，不参与存活判定，所以不需要防伪造。
    #[arg(long = "launched-breakaway", hide = true)]
    pub launched_breakaway: Option<bool>,
}

/// 解析并校验后的 serve 配置。
#[derive(Debug, Clone)]
pub struct ServeConfig {
    pub endpoint: Endpoint,
    pub cwd: PathBuf,
    pub idle_timeout: Duration,
}

impl ServeArgs {
    pub fn resolve(&self) -> Result<ServeConfig, BrokerError> {
        let endpoint = Endpoint::parse(&self.endpoint)?;
        let cwd = match &self.cwd {
            Some(dir) => std::path::absolute(dir)?,
            None => std::env::current_dir()?,
        };
        Ok(ServeConfig {
            endpoint,
            cwd,
            idle_timeout: Duration::from_millis(self.idle_timeout_ms),
        })
    }
}

/// 执行子命令，返回进程退出码。
pub fn run(cli: Cli) -> Result<i32, BrokerError> {
    match cli.command {
        Command::DiagExec(args) => Ok(diag::run(&args)),
        Command::Serve(command) => {
            let config = command.args.resolve()?;
            tracing::info!(?config, launched_breakaway = ?command.launched_breakaway, "启动 serve");
            crate::serve::run(config, command.launched_breakaway)
        }
        Command::Launch(args) => {
            let config = args.resolve()?;
            let launched = crate::launch::launch(&args.endpoint, &config)?;
            tracing::info!(
                pid = launched.pid,
                breakaway = launched.breakaway,
                "已拉起 serve"
            );
            use std::io::Write;
            let mut stdout = std::io::stdout().lock();
            writeln!(stdout, "{}", crate::launch::report_line(&launched))?;
            stdout.flush()?;
            Ok(0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("cxb-broker").chain(args.iter().copied()))
    }

    #[test]
    fn serve_参数顺序与上游一致() {
        let cli = parse(&[
            "serve",
            "--endpoint",
            "pipe:x",
            "--idle-timeout",
            "1500",
            "--cwd",
            ".",
        ])
        .unwrap();
        let Command::Serve(command) = cli.command else {
            panic!("应解析为 serve")
        };
        assert_eq!(command.launched_breakaway, None);
        let config = command.args.resolve().unwrap();
        assert_eq!(config.endpoint, Endpoint::Pipe("x".to_owned()));
        assert_eq!(config.idle_timeout, Duration::from_millis(1500));
        assert!(config.cwd.is_absolute());
    }

    #[test]
    fn 空闲超时缺省五分钟且必须为正() {
        let cli = parse(&["serve", "--endpoint", "pipe:x"]).unwrap();
        let Command::Serve(command) = cli.command else {
            panic!("应解析为 serve")
        };
        assert_eq!(command.args.idle_timeout_ms, DEFAULT_IDLE_TIMEOUT_MS);
        assert!(parse(&["serve", "--endpoint", "pipe:x", "--idle-timeout", "0"]).is_err());
        assert!(parse(&["serve", "--endpoint", "pipe:x", "--idle-timeout", "-5"]).is_err());
    }

    #[test]
    fn 缺少端点时报错() {
        assert!(parse(&["serve"]).is_err());
        assert!(parse(&["launch"]).is_err());
    }

    #[test]
    fn 脱离结果只有_serve_接受() {
        let cli = parse(&[
            "serve",
            "--endpoint",
            "pipe:x",
            "--launched-breakaway",
            "false",
        ])
        .unwrap();
        let Command::Serve(command) = cli.command else {
            panic!("应解析为 serve")
        };
        assert_eq!(command.launched_breakaway, Some(false));
        assert!(parse(&[
            "launch",
            "--endpoint",
            "pipe:x",
            "--launched-breakaway",
            "true"
        ])
        .is_err());
        assert!(parse(&[
            "serve",
            "--endpoint",
            "pipe:x",
            "--launched-breakaway",
            "maybe"
        ])
        .is_err());
    }
}
