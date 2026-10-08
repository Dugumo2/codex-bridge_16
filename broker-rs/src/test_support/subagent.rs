//! SA-a 共享模拟驱动，按来源线程隔离，默认不处理。
use crate::claim::{Action, Event, Machine};
use serde_json::Value;
#[derive(Debug, Default)]
pub(crate) struct SubagentMock {
    pub threads: std::collections::BTreeMap<String, Value>,
}
impl SubagentMock {
    pub(crate) fn inject(&mut self, _machine: &mut Machine, _event: Event) -> Vec<Action> {
        Vec::new()
    }
    pub(crate) fn state(&self) -> Value {
        Value::Null
    }
}
