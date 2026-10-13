use super::*;
use crate::claim::status_read::{StatusCaller, StatusDone, StatusResult};
use crate::claim::Operation;

impl Machine {
    pub fn subagent_reconcile_reply(
        &mut self,
        context: &RequestContext,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        let Operation::StartReconciliation { token, op_id } = &context.operation else {
            return;
        };
        let Some(pending) = self.runs.reconciliation.operations.get_mut(&op_id.0) else {
            return;
        };
        if pending.operation.token != *token
            || !pending.operation.outcome_unknown
            || pending.reply_received
        {
            return;
        }
        pending.reply_received = true;
        if pending
            .operation
            .verify_deadline_mono_ms
            .is_some_and(|d| self.clock.mono_ms >= d)
        {
            self.drive_subagent_reconcile(actions);
            // 总窗之外仍保留原操作的回包事实，但不补授信、不推进覆盖、不重试。
            let pending = self
                .runs
                .reconciliation
                .operations
                .get_mut(&op_id.0)
                .unwrap();
            match &result {
                Ok(value) => pending.reply_turn = reply_turn(value).map(str::to_owned),
                Err(error) if definitely_not_submitted(error) => {
                    pending.operation.submission = ReconcileSubmission::DefinitelyNotSubmitted;
                    pending.operation.outcome_unknown = false;
                }
                Err(_) => {}
            }
            return;
        }
        match result {
            Ok(value) => {
                let turn = reply_turn(&value);
                pending.reply_turn = turn.map(str::to_owned);
                // 成功响应只证明上游响应过，既不证明承载，也不授予归属。
            }
            Err(error) if definitely_not_submitted(&error) => {
                // 只接受协议层明确拒绝或上游结构化 NotSteerable；不按错误文本猜测。
                pending.operation.submission = ReconcileSubmission::DefinitelyNotSubmitted;
                pending.operation.outcome_unknown = false;
                if let Some(read) = pending.status_read.take() {
                    self.cancel_status_read(read, actions);
                }
                actions.push(Action::Unschedule {
                    key: TimerKey::ReconcileVerifyDeadline(*op_id),
                });
                self.reconcile_changed(&token.run_id, actions);
                return;
            }
            Err(_) => {}
        }
        self.verify_reconciliation(*op_id, actions);
    }
    pub fn subagent_reconcile_status_done(&mut self, done: StatusDone, actions: &mut Vec<Action>) {
        let StatusCaller::SubagentReconcile { token, op_id } = done.caller else {
            return;
        };
        let Some(pending) = self.runs.reconciliation.operations.get_mut(&op_id.0) else {
            return;
        };
        if pending.verify_token != token
            || pending.status_read != Some(done.read_id)
            || done.thread != token.claim.thread_id
            || done.token.as_ref() != Some(&token.claim)
            || !pending.operation.outcome_unknown
            || pending.expired
        {
            return;
        }
        pending.status_read = None;
        if pending
            .operation
            .verify_deadline_mono_ms
            .is_some_and(|d| self.clock.mono_ms >= d)
        {
            self.drive_subagent_reconcile(actions);
            return;
        }
        let StatusResult::Carrier(turn) = done.result else {
            // NoCarrier 仅表示本次未看到，不能证明输入未提交；重读不刷新总窗。
            pending.next_verify = self.clock.mono_ms.saturating_add(500);
            let deadline = pending.operation.verify_deadline_mono_ms.unwrap();
            actions.push(Action::Schedule {
                token: token.claim.clone(),
                kind: super::super::TimerKind::ReconcileVerifyDeadline { token, op_id },
                delay_ms: 500.min(deadline.saturating_sub(self.clock.mono_ms)),
            });
            return;
        };
        pending.operation.target_turn_id = Some(turn.clone());
        let mut origin = pending.started.get(&turn).copied();
        let conflict = pending
            .reply_turn
            .as_ref()
            .is_some_and(|reply| reply != &turn)
            || (pending.known_turns.contains(&turn) && origin != Some(TurnOrigin::Goal));
        if !conflict
            && origin.is_none()
            && self.reconcile_token_current(&token)
            && !pending_revoked(self, op_id)
            && self.reconcile_new_parent_is_proven(&token, op_id, &turn)
        {
            // clientId 承载、提交之后唯一新开始、前轮与完整管理身份共同证明，响应本身不授信。
            self.remember_owned_turn(&token.claim.thread_id, &turn);
            if self.reconcile_parent_started(&token, &turn, TurnOrigin::SubagentReconcile, actions)
            {
                origin = Some(TurnOrigin::SubagentReconcile);
            }
        }
        // 身份过期只补原操作的承载事实；不能授予新控制权或覆盖新运行。
        if !self.reconcile_token_current(&token) || pending_revoked(self, op_id) {
            let p = self
                .runs
                .reconciliation
                .operations
                .get_mut(&op_id.0)
                .unwrap();
            p.operation.automatic_control_revoked = true;
            p.operation.outcome_unknown = false;
            p.operation.submission = match origin.filter(|_| !conflict) {
                Some(TurnOrigin::Goal) => ReconcileSubmission::MergedOwnGoalTurn,
                Some(TurnOrigin::SubagentReconcile) => ReconcileSubmission::ConfirmedOwnTurn,
                _ => ReconcileSubmission::MergedForeignOrUnknownTurn,
            };
            return;
        }
        if conflict || origin.is_none() || origin == Some(TurnOrigin::User) {
            let p = self
                .runs
                .reconciliation
                .operations
                .get_mut(&op_id.0)
                .unwrap();
            p.operation.submission = ReconcileSubmission::MergedForeignOrUnknownTurn;
            p.operation.outcome_unknown = false;
            p.operation.automatic_control_revoked = true;
            actions.push(Action::Unschedule {
                key: TimerKey::ReconcileVerifyDeadline(op_id),
            });
            self.reconcile_abnormal(&token, TerminalReason::OwnershipConflict, actions);
            return;
        }
        self.reconcile_turn_confirmed(&token, op_id, &turn, origin.unwrap(), actions);
    }
    fn reconcile_new_parent_is_proven(
        &self,
        token: &ConvergeToken,
        id: ReconcileOpId,
        turn: &str,
    ) -> bool {
        let p = &self.runs.reconciliation.operations[&id.0];
        p.method == "turn/start"
            && p.observed_starts.len() == 1
            && p.observed_starts.contains(turn)
            && !p.known_turns.contains(turn)
            && p.operation.expected_previous_turn_id
                == self.runs.runs[&token.run_id].current_turn_id
            && p.operation
                .expected_previous_turn_id
                .as_ref()
                .is_some_and(|previous| {
                    self.claims[&token.claim.thread_id]
                        .owned_turns
                        .contains(previous)
                })
    }
}
fn pending_revoked(machine: &Machine, id: ReconcileOpId) -> bool {
    machine.runs.reconciliation.operations[&id.0]
        .operation
        .automatic_control_revoked
}
fn reply_turn(value: &Value) -> Option<&str> {
    value
        .get("turn")
        .and_then(|t| t.get("id"))
        .or_else(|| value.get("turnId"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}
fn definitely_not_submitted(error: &Value) -> bool {
    matches!(
        error.get("code").and_then(Value::as_i64),
        Some(-32700 | -32601 | -32602)
    ) || error
        .get("data")
        .and_then(|v| v.get("codexErrorInfo"))
        .and_then(Value::as_object)
        .is_some_and(|v| v.contains_key("activeTurnNotSteerable"))
}
