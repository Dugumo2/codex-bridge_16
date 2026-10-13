use crate::claim::{status_read::StatusDone, Action, Machine};
use crate::runs::upstream_error::UpstreamError;
use serde_json::Value;

/// 由 C1 实现。
impl Machine {
    pub(crate) fn stall_arm(&mut self, _run: &str, _actions: &mut Vec<Action>) {
        // 由 C1 实现。
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn stall_progress(
        &mut self,
        _run: &str,
        _thread: &str,
        _turn: &str,
        _method: &str,
        _params: &Option<Value>,
        _replay: bool,
        _actions: &mut Vec<Action>,
    ) {
        // 由 C1 实现。
    }
    pub(crate) fn stall_timer(&mut self, _run: &str, _generation: u64, _actions: &mut Vec<Action>) {
        // 由 C1 实现。
    }
    pub(crate) fn stall_verify_done(&mut self, _done: StatusDone, _actions: &mut Vec<Action>) {
        // 由 C1 实现。
    }
    pub(crate) fn stall_pending_changed(
        &mut self,
        _run: &str,
        _real: bool,
        _actions: &mut Vec<Action>,
    ) {
        // 由 C1 实现。
    }
    pub(crate) fn stall_owned_settled(&mut self, _run: &str, _actions: &mut Vec<Action>) {
        // 由 C1 实现。
    }
    pub(crate) fn stall_joined(&mut self, _run: &str, _actions: &mut Vec<Action>) {
        // 由 C1 实现。
    }
    pub(crate) fn stall_released(&mut self, _run: &str, _actions: &mut Vec<Action>) {
        // 由 C1 实现。
    }
    pub(crate) fn stall_review_moved(&mut self, _run: &str, _actions: &mut Vec<Action>) {
        // 由 C1 实现。
    }
    pub(crate) fn stall_shutdown(&mut self, _actions: &mut Vec<Action>) {
        // 由 C1 实现。
    }
    pub(crate) fn upstream_access_check(
        &mut self,
        _run: &str,
        _error: &UpstreamError,
        _actions: &mut Vec<Action>,
    ) {
        // 由 C1 实现。
    }
}
