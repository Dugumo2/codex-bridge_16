//! B 路汇总提交状态机。生产准入依赖 A 的事实和 C 的可信输入、预算接口。
use super::{Action, Machine, RequestContext, State, TimerKey};
use crate::runs::{RunState, TurnStatus};
use crate::sa1b::*;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};

pub(crate) mod evidence;
mod submit;
#[cfg(test)]
pub(crate) mod tests;
mod verify;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Pending {
    pub operation: ReconcileOperation,
    /// 只读核实可由当前交接授权重新发起；原提交身份保持不可变。
    pub verify_token: ConvergeToken,
    pub batch: ReconcileBatch,
    pub method: String,
    pub known_turns: HashSet<String>,
    pub started: HashMap<String, TurnOrigin>,
    pub observed_starts: HashSet<String>,
    pub completions: HashMap<String, Value>,
    pub reply_turn: Option<String>,
    pub reply_received: bool,
    pub status_read: Option<u64>,
    pub next_verify: u64,
    pub expired: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Parent {
    pub turn: String,
    pub origin: TurnOrigin,
    pub completed: Option<TurnStatus>,
    pub delivered_epoch: u64,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Reconciliations {
    pub changed: HashSet<String>,
    pub next_op: u64,
    pub operations: BTreeMap<u64, Pending>,
    pub completed_reads: BTreeMap<u64, (ConvergeToken, ReconcileBatch)>,
    pub ready: HashMap<String, (ConvergeToken, ReconcileBatch)>,
    pub parents: HashMap<String, Vec<Parent>>,
    #[cfg(test)]
    pub harness: Option<tests::Dependencies>,
}
impl Machine {
    pub(crate) fn classify_result_input(
        &self,
        run: &str,
        thread: &str,
        turn: &str,
        item: &Value,
    ) -> InputSource {
        #[cfg(test)]
        if let Some(h) = &self.runs.reconciliation.harness {
            if item["type"] == "userMessage"
                && h.registrations.iter().any(|r| {
                    r.token.run_id == run
                        && r.token.claim.thread_id == thread
                        && item["clientId"].as_str() == Some(&r.client_user_message_id)
                        && (self
                            .claims
                            .get(thread)
                            .is_some_and(|c| c.owned_turns.contains(turn))
                            || self
                                .runs
                                .reconciliation
                                .operations
                                .get(&r.op_id.0)
                                .is_some_and(|p| {
                                    p.verify_token == r.token
                                        && p.observed_starts.contains(turn)
                                        && !p.operation.automatic_control_revoked
                                }))
                })
            {
                return InputSource::TrustedReconcile;
            }
        }
        self.classify_run_input(run, thread, turn, item)
    }
    pub(crate) fn reconcile_facts(&self, run: &str) -> Option<ConvergenceFacts> {
        #[cfg(test)]
        if let Some(h) = &self.runs.reconciliation.harness {
            return h.facts.get(run).cloned();
        }
        self.convergence_facts(run)
    }
    pub(crate) fn reconcile_token_current(&self, token: &ConvergeToken) -> bool {
        !self.closing
            && self.relations.conn_epoch == token.conn_epoch
            && self
                .convergences
                .runs
                .get(&token.run_id)
                .is_some_and(|r| r.token == *token && !r.automatic_control_revoked)
            && self.claims.get(&token.claim.thread_id).is_some_and(|c| {
                c.claim_id == token.claim.claim_id
                    && c.run_id.as_ref() == Some(&token.run_id)
                    && !c.joined()
                    && !c.owner_gone
            })
            && self.manage_context(&token.run_id).is_some_and(|c| {
                c.gen == token.manage_gen
                    && matches!(
                        c.authority,
                        crate::relations::Authority::Owner(_)
                            | crate::relations::Authority::Broker { .. }
                    )
            })
            && self.runs.runs.get(&token.run_id).is_some_and(|r| {
                r.state == RunState::Running
                    && !r.joined
                    && !r
                        .subagent_join
                        .as_ref()
                        .is_some_and(|j| j.automatic_control_revoked)
            })
    }
    pub(crate) fn reconcile_budget(&self, token: &ConvergeToken) -> BudgetAdmission {
        #[cfg(test)]
        if let Some(h) = &self.runs.reconciliation.harness {
            return h.budget;
        }
        self.reconcile_budget_admission(token)
    }
    fn reconcile_register_input(
        &mut self,
        registration: TrustedInputRegistration,
        actions: &mut Vec<Action>,
    ) -> bool {
        #[cfg(test)]
        if let Some(h) = &mut self.runs.reconciliation.harness {
            h.registrations.push(registration);
            return h.trust;
        }
        self.register_reconcile_input(registration, actions)
    }
    fn reconcile_register_auto(
        &mut self,
        token: &ConvergeToken,
        turn: &str,
        origin: TurnOrigin,
        actions: &mut Vec<Action>,
    ) -> bool {
        #[cfg(test)]
        if let Some(h) = &mut self.runs.reconciliation.harness {
            if h.auto.insert((token.run_id.clone(), turn.into())) {
                let record = self.runs.runs.get_mut(&token.run_id).unwrap();
                *record.auto_turns_started.get_or_insert(0) += 1;
            }
            return true;
        }
        self.register_auto_parent_turn(token, turn, origin, actions)
    }
    pub fn child_results_done(
        &mut self,
        token: ConvergeToken,
        read_id: BatchReadId,
        batch: ReconcileBatch,
        actions: &mut Vec<Action>,
    ) {
        let Some((expected, original)) =
            self.runs.reconciliation.completed_reads.remove(&read_id.0)
        else {
            return;
        };
        if expected != token || original != batch || !self.reconcile_token_current(&token) {
            return;
        }
        if self.reconcile_facts(&token.run_id).is_none_or(|f| {
            f.token != token
                || f.work_epoch != batch.work_epoch
                || !crate::relations::results::settled_facts(&f)
        }) {
            return;
        }
        if let Some(join) = self
            .runs
            .runs
            .get_mut(&token.run_id)
            .and_then(|r| r.subagent_join.as_mut())
        {
            join.result_coverage = Some(batch.coverage.clone());
        }
        self.runs
            .reconciliation
            .ready
            .insert(token.run_id.clone(), (token.clone(), batch));
        self.reconcile_changed(&token.run_id, actions);
    }
    /// A 完成准入复查后取走该批；读取完成本身绝不自动发 start。
    pub fn take_child_results(&mut self, token: &ConvergeToken) -> Option<ReconcileBatch> {
        if !self.reconcile_token_current(token) {
            return None;
        }
        let (saved, batch) = self.runs.reconciliation.ready.remove(&token.run_id)?;
        (saved == *token).then_some(batch)
    }
    pub fn cancel_subagent_reconcile(
        &mut self,
        token: &ConvergeToken,
        reason: ReconcileCancel,
        actions: &mut Vec<Action>,
    ) {
        self.cancel_child_results(token, actions);
        if self
            .runs
            .reconciliation
            .ready
            .get(&token.run_id)
            .is_some_and(|(saved, _)| saved == token)
        {
            self.runs.reconciliation.ready.remove(&token.run_id);
        }
        for pending in self
            .runs
            .reconciliation
            .operations
            .values_mut()
            .filter(|p| p.verify_token == *token)
        {
            let op = &mut pending.operation;
            op.automatic_control_revoked = true;
            if matches!(
                op.submission,
                ReconcileSubmission::NotSent | ReconcileSubmission::DefinitelyNotSubmitted
            ) {
                op.submission = ReconcileSubmission::CancelledBeforeSubmit;
                op.outcome_unknown = false;
            }
            // 已发未知操作保留原身份、核实截止及在途读取，abandon 不能抹掉副作用。
        }
        if let Some(runtime) = self
            .convergences
            .runs
            .get_mut(&token.run_id)
            .filter(|r| r.token == *token)
        {
            runtime.automatic_control_revoked = true;
        }
        if self
            .convergences
            .runs
            .get(&token.run_id)
            .is_some_and(|r| r.token == *token)
        {
            if let Some(join) = self
                .runs
                .runs
                .get_mut(&token.run_id)
                .and_then(|r| r.subagent_join.as_mut())
            {
                join.automatic_control_revoked = true;
            }
        }
        if reason == ReconcileCancel::Shutdown {
            let reads: Vec<_> = self
                .runs
                .reconciliation
                .operations
                .values_mut()
                .filter(|p| p.verify_token == *token)
                .filter_map(|p| p.status_read.take())
                .collect();
            for id in reads {
                self.cancel_status_read(id, actions);
            }
        }
        self.reconcile_changed(&token.run_id, actions);
    }
    pub fn reconcile_unresolved(&self, run: &str) -> bool {
        self.runs
            .reconciliation
            .operations
            .values()
            .any(|p| p.operation.token.run_id == run && p.operation.outcome_unknown)
    }
    pub fn reconcile_operation(&self, id: ReconcileOpId) -> Option<&ReconcileOperation> {
        self.runs
            .reconciliation
            .operations
            .get(&id.0)
            .map(|p| &p.operation)
    }
    /// A 的计时分派先核对原操作身份，再驱动固定截止的核实。
    pub fn subagent_reconcile_timer(
        &mut self,
        token: &ConvergeToken,
        id: ReconcileOpId,
        actions: &mut Vec<Action>,
    ) {
        if self
            .runs
            .reconciliation
            .operations
            .get(&id.0)
            .is_some_and(|p| p.verify_token == *token)
        {
            self.drive_subagent_reconcile(actions);
        }
    }
    pub(crate) fn reconcile_changed(&mut self, run: &str, actions: &mut Vec<Action>) {
        self.persist_reconciliation(run, actions);
        // revision 由 A 唯一推进；step 末尾合并本步变化后统一发布。
        self.runs.reconciliation.changed.insert(run.into());
    }
    pub fn take_reconcile_changes(&mut self) -> Vec<String> {
        let mut runs: Vec<_> = self.runs.reconciliation.changed.drain().collect();
        runs.sort();
        runs
    }
    fn reconcile_abnormal(
        &mut self,
        token: &ConvergeToken,
        reason: TerminalReason,
        actions: &mut Vec<Action>,
    ) {
        if self
            .convergences
            .runs
            .get(&token.run_id)
            .is_none_or(|r| r.token != *token)
            || self
                .runs
                .runs
                .get(&token.run_id)
                .is_none_or(|r| r.state != RunState::Running)
        {
            return;
        }
        if let Some(join) = self
            .runs
            .runs
            .get_mut(&token.run_id)
            .and_then(|r| r.subagent_join.as_mut())
        {
            if join.terminal_reason.is_none() {
                join.terminal_reason = Some(reason);
            }
            join.phase = JoinPhase::Abnormal;
            join.automatic_control_revoked = true;
        }
        if let Some(r) = self
            .convergences
            .runs
            .get_mut(&token.run_id)
            .filter(|r| r.token == *token)
        {
            r.automatic_control_revoked = true;
        }
        self.reconcile_changed(&token.run_id, actions);
    }
    pub fn subagent_reconcile_transport(
        &mut self,
        context: &RequestContext,
        _event: ReconcileTransportEvent,
        actions: &mut Vec<Action>,
    ) {
        let super::Operation::StartReconciliation { token, op_id } = &context.operation else {
            return;
        };
        if self
            .runs
            .reconciliation
            .operations
            .get(&op_id.0)
            .is_none_or(|p| p.operation.token != *token || !p.operation.outcome_unknown)
        {
            return;
        }
        self.verify_reconciliation(*op_id, actions);
    }
    /// 原始 started 只记在原提交上；此时不授予自有权，也不写父轮账本。
    pub fn reconcile_observe_parent_started(&mut self, token: &ConvergeToken, turn: &str) {
        if !self.reconcile_token_current(token) || turn.is_empty() {
            return;
        }
        for p in self
            .runs
            .reconciliation
            .operations
            .values_mut()
            .filter(|p| {
                p.verify_token == *token
                    && p.operation.outcome_unknown
                    && !p.expired
                    && !p.operation.automatic_control_revoked
            })
        {
            if !p.known_turns.contains(turn) && p.observed_starts.len() < 16 {
                p.observed_starts.insert(turn.into());
            }
        }
    }

    /// 快轮可能在 clientId 核实前完成；只暂存有界完成壳，核实后走正常分页补读。
    pub(crate) fn reconcile_observe_parent_completed(
        &mut self,
        run: &str,
        thread: &str,
        turn: &Value,
        actions: &mut Vec<Action>,
    ) {
        let Some(id) = turn
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty() && id.len() <= 1024)
        else {
            return;
        };
        let Some(status @ ("completed" | "failed" | "interrupted")) =
            turn.get("status").and_then(Value::as_str)
        else {
            return;
        };
        let Some(token) = self
            .runs
            .reconciliation
            .operations
            .values()
            .find(|p| {
                p.operation.token.run_id == run
                    && p.operation.token.claim.thread_id == thread
                    && p.operation.outcome_unknown
                    && !p.known_turns.contains(id)
            })
            .map(|p| p.verify_token.clone())
        else {
            return;
        };
        if !self.reconcile_token_current(&token) {
            return;
        }
        if crate::results::full_items(turn).is_some_and(|items| {
            items.iter().any(|item| {
                self.classify_result_input(run, thread, id, item) == InputSource::ExternalUser
            })
        }) {
            self.cancel_subagent_reconcile(&token, ReconcileCancel::UserInput, actions);
            return;
        }
        let mut shell =
            serde_json::json!({"id":id,"status":status,"itemsView":"notLoaded","items":[]});
        if let Some(error) = turn.get("error").filter(|e| !e.is_null()) {
            if let Some(encoded) = evidence::json_bounded(error, 4096) {
                shell["error"] = serde_json::from_str(&encoded).expect("已序列化的错误对象");
            }
        }
        for p in self
            .runs
            .reconciliation
            .operations
            .values_mut()
            .filter(|p| p.verify_token == token && p.operation.outcome_unknown)
        {
            if p.completions.len() < 16 {
                p.completions
                    .entry(id.into())
                    .or_insert_with(|| shell.clone());
            }
        }
    }

    /// A 在已确认自有父轮的开始事件处调用。未知或外部 started 不得调用。
    pub fn reconcile_parent_started(
        &mut self,
        token: &ConvergeToken,
        turn: &str,
        origin: TurnOrigin,
        actions: &mut Vec<Action>,
    ) -> bool {
        if !self.reconcile_token_current(token)
            || turn.is_empty()
            || !self.claims[&token.claim.thread_id]
                .owned_turns
                .contains(turn)
        {
            return false;
        }
        let record = &self.runs.runs[&token.run_id];
        let first = record
            .start_confirmation
            .turn_id
            .as_ref()
            .filter(|_| record.start_confirmation.confirmed)
            .map(|id| Parent {
                turn: id.clone(),
                origin: TurnOrigin::User,
                completed: record
                    .turns
                    .iter()
                    .find(|t| t.turn_id == *id)
                    .map(|t| t.status),
                delivered_epoch: 0,
            });
        let parents = self
            .runs
            .reconciliation
            .parents
            .entry(token.run_id.clone())
            .or_default();
        if parents.is_empty() {
            if let Some(first) = first {
                parents.push(first);
            }
        }
        if parents.iter().any(|p| p.turn == turn && p.origin != origin) {
            return false;
        }
        if !parents.iter().any(|p| p.turn == turn) {
            parents.push(Parent {
                turn: turn.into(),
                origin,
                completed: None,
                delivered_epoch: 0,
            });
        }
        for p in self
            .runs
            .reconciliation
            .operations
            .values_mut()
            .filter(|p| p.verify_token == *token && p.operation.outcome_unknown)
        {
            p.started.insert(turn.into(), origin);
        }
        self.run_confirmed_parent_started(
            &token.run_id,
            &token.claim.thread_id,
            turn,
            origin,
            actions,
        );
        self.reconcile_parent_completed(&token.run_id, turn, actions);
        true
    }
    pub fn reconcile_turn_confirmed(
        &mut self,
        token: &ConvergeToken,
        op_id: ReconcileOpId,
        turn: &str,
        origin: TurnOrigin,
        actions: &mut Vec<Action>,
    ) {
        if !self.reconcile_token_current(token) {
            return;
        }
        let Some(pending) = self.runs.reconciliation.operations.get(&op_id.0) else {
            return;
        };
        if pending.verify_token != *token
            || !pending.operation.outcome_unknown
            || pending.operation.automatic_control_revoked
            || origin == TurnOrigin::User
            || pending.operation.target_turn_id.as_deref() != Some(turn)
            || pending.started.get(turn) != Some(&origin)
            || !self.claims[&token.claim.thread_id]
                .owned_turns
                .contains(turn)
        {
            return;
        }
        let epoch = pending.operation.work_epoch;
        if !self.reconcile_register_auto(token, turn, origin, actions) {
            return;
        }
        let pending = self
            .runs
            .reconciliation
            .operations
            .get_mut(&op_id.0)
            .unwrap();
        pending.operation.outcome_unknown = false;
        pending.operation.submission = if origin == TurnOrigin::Goal {
            ReconcileSubmission::MergedOwnGoalTurn
        } else {
            ReconcileSubmission::ConfirmedOwnTurn
        };
        let completed_shell = pending.completions.remove(turn);
        if let Some(parents) = self.runs.reconciliation.parents.get_mut(&token.run_id) {
            if let Some(parent) = parents.iter_mut().find(|p| p.turn == turn) {
                parent.delivered_epoch = parent.delivered_epoch.max(epoch);
            }
        }
        if let Some(join) = self
            .runs
            .runs
            .get_mut(&token.run_id)
            .and_then(|r| r.subagent_join.as_mut())
        {
            if origin == TurnOrigin::SubagentReconcile {
                join.wakeups_started += 1;
            }
            join.delivered_epoch = join.delivered_epoch.max(epoch);
            join.last_reconcile_turn_id = Some(turn.into());
            join.phase = JoinPhase::Reconciling;
        }
        actions.push(Action::Unschedule {
            key: TimerKey::ReconcileVerifyDeadline(op_id),
        });
        self.run_confirmed_parent_started(
            &token.run_id,
            &token.claim.thread_id,
            turn,
            origin,
            actions,
        );
        if let Some(shell) = completed_shell {
            self.run_turn_completed(&token.claim.thread_id, Some(&token.run_id), &shell, actions);
        }
        self.reconcile_parent_completed(&token.run_id, turn, actions);
        self.reconcile_changed(&token.run_id, actions);
    }
    /// completed 可以先于 clientId 核实到达；两条入口都调用，覆盖单调且旧轮不改最终指针。
    pub fn reconcile_parent_completed(&mut self, run: &str, turn: &str, actions: &mut Vec<Action>) {
        let Some(record) = self.runs.runs.get(run) else {
            return;
        };
        if record.state != RunState::Running {
            return;
        }
        let status = record
            .turns
            .iter()
            .find(|t| t.turn_id == turn)
            .map(|t| t.status);
        let Some(parent) = self
            .runs
            .reconciliation
            .parents
            .get_mut(run)
            .and_then(|ps| ps.iter_mut().find(|p| p.turn == turn))
        else {
            return;
        };
        parent.completed = status;
        if status == Some(TurnStatus::Completed) && parent.delivered_epoch != 0 {
            let epoch = parent.delivered_epoch;
            if let Some(join) = self
                .runs
                .runs
                .get_mut(run)
                .and_then(|r| r.subagent_join.as_mut())
            {
                join.reconciled_epoch = join.reconciled_epoch.max(epoch.min(join.delivered_epoch));
            }
            self.reconcile_changed(run, actions);
        }
    }
}

pub(crate) fn execution_keys() -> &'static [&'static str] {
    submit::EXECUTION_KEYS
}
