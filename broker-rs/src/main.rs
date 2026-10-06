use std::process::ExitCode;

use clap::Parser;
use cxb_broker::cli::{self, Cli};

fn main() -> ExitCode {
    let args = Cli::parse();
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
