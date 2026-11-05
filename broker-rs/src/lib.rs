//! cxb-broker：每个工作区一个的 broker 进程。
//!
//! 设计依据为 内部详细设计文档（S1 冻结版），模块划分见其第 1 节，
//! 实施顺序见 `S3-计划.md`。本 crate 同时提供库和二进制：纯逻辑（帧、消息分类、
//! 之后的认领状态机）放在库里，便于不起进程做表驱动单测。

pub mod appserver;
pub mod claim;
pub mod cli;
pub mod core;
pub mod endpoint;
pub mod error;
pub mod inbox;
pub mod ipc;
pub mod job;
pub mod launch;
pub mod logging;
pub mod policy;
pub mod proto;
pub mod relations;
pub mod requests;
pub mod results;
pub mod rollout;
pub mod runs;
pub mod sa1b;
pub mod serve;
pub mod stop;
pub mod store;
#[cfg(test)]
pub(crate) mod test_support;
pub mod watchdog;
