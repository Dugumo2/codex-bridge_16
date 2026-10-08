//! H2-a 单步驱动签名；保留请求快照可以重放迟到回包。
use crate::claim::{status_read::StatusDone, Action, Event, Machine, RequestContext};
use serde_json::Value;
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StatusPhase {
    Metadata,
    Turns,
    Items,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatusRequest {
    pub read_id: u64,
    pub generation: u64,
    pub phase: StatusPhase,
    pub candidate: Option<String>,
    pub method: String,
    pub params: Option<Value>,
    pub context: RequestContext,
}
pub(crate) enum StatusStep {
    Reply(StatusRequest, Result<Value, Value>),
    Event(Event),
}
pub(crate) fn status_requests(_actions: &[Action]) -> Vec<StatusRequest> {
    Vec::new()
}
pub(crate) fn reply_status(
    _machine: &mut Machine,
    _request: &StatusRequest,
    _result: Result<Value, Value>,
) -> Vec<Action> {
    Vec::new()
}
pub(crate) fn drive_status(
    _machine: &mut Machine,
    _script: Vec<StatusStep>,
) -> (Vec<Action>, Vec<StatusDone>) {
    (Vec::new(), Vec::new())
}
