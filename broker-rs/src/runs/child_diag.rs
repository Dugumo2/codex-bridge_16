use crate::claim::{Action, Machine};
use crate::runs::{upstream_error::UpstreamError, ChildRunRef};

/// 由 C2 实现。
impl Machine {
    pub(crate) fn child_activity_run(
        &self,
        _thread: &str,
        _turn: &str,
        _completion: bool,
    ) -> Option<ChildRunRef> {
        #[cfg(test)]
        if let Some(child) = &self.runs.child_activity_override {
            return Some(child.clone());
        }
        // 由 C2 实现。
        None
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn child_diagnostic_event(
        &mut self,
        _child: &ChildRunRef,
        _thread: &str,
        _turn: &str,
        _error: UpstreamError,
        _completion: bool,
        _actions: &mut Vec<Action>,
    ) {
        // 由 C2 实现。
    }
}
