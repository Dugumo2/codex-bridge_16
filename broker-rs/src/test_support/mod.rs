//! 共同测试驱动由各路填充；不进入生产编译。
#[allow(dead_code)]
pub(crate) mod rollout;
pub(crate) mod status_read;
pub(crate) mod subagent;

pub(crate) mod background;

// A/B/C 可在各自单测中复用；E 的独立集成测试也按 path 引入同一实现。
#[allow(dead_code)]
pub(crate) mod sa1b;
