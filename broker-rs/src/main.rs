use std::process::ExitCode;

use clap::Parser;
use cxb_broker::cli::{self, Cli, Command};

fn main() -> ExitCode {
    let args = Cli::parse();
    if let Command::DiagExec(args) = &args.command {
        // Windows 退出码保留全部 32 位；不能经过 ExitCode 的 u8 转换。
        std::process::exit(cli::diag::run(args));
    }
    cxb_broker::logging::init();
    let code = match cli::run(args) {
        Ok(code) => ExitCode::from(u8::try_from(code).unwrap_or(1)),
        Err(err) => {
            tracing::error!(error = %err, "cxb-broker 退出");
            eprintln!("cxb-broker：{err}");
            ExitCode::FAILURE
        }
    };
    cxb_broker::logging::shutdown();
    code
}
