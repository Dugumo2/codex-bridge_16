use crate::claim::Machine;
use crate::runs::{InterruptReason, StopDiagSend, StopRpcOutcome};

impl Machine {
    pub(crate) fn stop_diag_began(
        &mut self,
        _run: &str,
        _reason: InterruptReason,
        _stop_generation: u64,
    ) {
        // 由 C1 实现。
    }

    pub(crate) fn stop_diag_goal_started(&mut self, _run: &str, _send: &StopDiagSend) {
        // 由 C1 实现。
    }

    pub(crate) fn stop_diag_goal_settled(&mut self, _run: &str, _send: &StopDiagSend) {
        // 由 C1 实现。
    }

    pub(crate) fn stop_diag_interrupt_sent(&mut self, _run: &str, _send: &StopDiagSend) {
        // 由 C1 实现。
    }

    pub(crate) fn stop_diag_interrupt_reply(
        &mut self,
        _run: &str,
        _send: &StopDiagSend,
        _outcome: StopRpcOutcome,
    ) {
        // 由 C1 实现。
    }

    pub(crate) fn stop_diag_turn_ended(&mut self, _run: &str, _send: &StopDiagSend) {
        // 由 C1 实现。
    }

    pub(crate) fn stop_diag_rpc_issue(
        &mut self,
        _run: &str,
        _send: &StopDiagSend,
        _method: &str,
        _kind: StopRpcOutcome,
        _message: &str,
    ) {
        // 由 C1 实现。
    }

    pub(crate) fn stop_diag_unconfirmed(&mut self, _run: &str) {
        // 由 C1 实现。
    }

    pub(crate) fn stop_diag_confirmed(&mut self, _run: &str) {
        // 由 C1 实现。
    }
}
