use super::super::{Operation, TimerKind};
use super::*;
use crate::claim::status_read::{StatusCaller, StatusQuery};
use serde_json::{json, Map};

impl Machine {
    fn reconcile_admission(
        &self,
        token: &ConvergeToken,
        batch: &ReconcileBatch,
    ) -> Result<(), ReconcileRefusal> {
        let Some(facts) = self.reconcile_facts(&token.run_id) else {
            return Err(ReconcileRefusal::Disabled);
        };
        if self.closing
            || self.stop_busy(&token.claim.thread_id)
            || self.tree_for_run(&token.run_id).is_some()
        {
            return Err(ReconcileRefusal::Stopping);
        }
        if self.tree_abandoned(&token.run_id) {
            return Err(ReconcileRefusal::Abandoned);
        }
        if !self.reconcile_token_current(token) || facts.token != *token {
            return Err(ReconcileRefusal::StaleToken);
        }
        if self.clock.mono_ms >= self.convergences.runs[&token.run_id].deadline_mono_ms {
            return Err(ReconcileRefusal::Deadline);
        }
        if facts.work_epoch != batch.work_epoch {
            return Err(ReconcileRefusal::WorkChanged);
        }
        if !facts.conflicts.is_empty() {
            return Err(ReconcileRefusal::OwnershipConflict);
        }
        if !crate::relations::results::settled_facts(&facts) {
            return Err(ReconcileRefusal::DiscoveryIncomplete);
        }
        match self.thread_capability(&token.claim.thread_id) {
            Some(true) => {}
            Some(false) => return Err(ReconcileRefusal::RootNoDirectInput),
            None => return Err(ReconcileRefusal::CapabilityUnknown),
        }
        let record = &self.runs.runs[&token.run_id];
        let config = record
            .convergence_config
            .as_ref()
            .ok_or(ReconcileRefusal::Disabled)?;
        let join = record
            .subagent_join
            .as_ref()
            .ok_or(ReconcileRefusal::Disabled)?;
        if batch.work_epoch == 0 || !evidence::valid_batch(batch, &config.result_limits) {
            return Err(ReconcileRefusal::InvalidBatch);
        }
        if join.delivered_epoch >= batch.work_epoch {
            return Err(ReconcileRefusal::AlreadyDelivered);
        }
        if join.work_epoch != facts.work_epoch {
            return Err(ReconcileRefusal::WorkChanged);
        }
        let unique_items: HashSet<_> = batch
            .items
            .iter()
            .map(|i| (&i.key.thread_id, i.key.activity_seq))
            .collect();
        let total = facts
            .activities
            .iter()
            .map(|a| (&a.key.thread_id, a.key.activity_seq))
            .collect::<HashSet<_>>()
            .len();
        if batch.coverage.total != total
            || unique_items.len() != batch.items.len()
            || batch.items.iter().any(|item| {
                !facts.activities.iter().any(|a| {
                    a.key.thread_id == item.key.thread_id
                        && a.key.activity_seq == item.key.activity_seq
                        && a.parent_thread_id == item.parent_thread_id
                        && a.result_status == Some(item.status)
                }) || item.locator.thread_id != item.key.thread_id
                    || facts
                        .result_refs
                        .iter()
                        .find(|r| {
                            r.key.thread_id == item.key.thread_id
                                && r.key.activity_seq == item.key.activity_seq
                        })
                        .is_some_and(|r| r.locator != item.locator)
            })
        {
            return Err(ReconcileRefusal::InvalidBatch);
        }
        if record
            .execution_config
            .as_ref()
            .and_then(|c| c.turn_params.as_ref())
            .and_then(Value::as_object)
            .is_none()
        {
            return Err(ReconcileRefusal::InvalidBatch);
        }
        if !record.start_confirmation.confirmed || record.start_confirmation.owned != Some(true) {
            return Err(ReconcileRefusal::OwnershipConflict);
        }
        if matches!(
            self.records
                .get(&token.claim.thread_id)
                .and_then(|r| r.goal_status.as_deref()),
            Some("paused" | "blocked" | "budgetLimited" | "usageLimited")
        ) {
            return Err(ReconcileRefusal::GoalInactive);
        }
        if self.reconcile_goal_target(token).is_none() {
            if matches!(
                self.claims[&token.claim.thread_id].state,
                State::Running { .. }
            ) {
                return Err(ReconcileRefusal::OwnershipConflict);
            }
            if join.wakeups_started >= config.max_wakeups {
                return Err(ReconcileRefusal::WakeLimit);
            }
            if record
                .auto_turns_started
                .is_none_or(|n| n >= config.max_auto_turns)
            {
                return Err(ReconcileRefusal::AutoTurnLimit);
            }
        }
        match self.reconcile_budget(token) {
            BudgetAdmission::Allowed => Ok(()),
            BudgetAdmission::Disabled => Err(ReconcileRefusal::Disabled),
            BudgetAdmission::Exhausted => Err(ReconcileRefusal::BudgetExhausted),
            BudgetAdmission::NeedsVerification => Err(ReconcileRefusal::BudgetIncomplete),
        }
    }
    fn reconcile_goal_target(&self, token: &ConvergeToken) -> Option<String> {
        let claim = self.claims.get(&token.claim.thread_id)?;
        let State::Running {
            turn_id,
            joined: false,
            ..
        } = &claim.state
        else {
            return None;
        };
        (claim.owned_turns.contains(turn_id)
            && self
                .runs
                .reconciliation
                .parents
                .get(&token.run_id)?
                .iter()
                .any(|p| {
                    p.turn == *turn_id && p.origin == TurnOrigin::Goal && p.completed.is_none()
                }))
        .then(|| turn_id.clone())
    }
    pub fn begin_subagent_reconcile(
        &mut self,
        token: ConvergeToken,
        batch: ReconcileBatch,
        _actions: &mut Vec<Action>,
    ) -> Result<ReconcileOpId, ReconcileRefusal> {
        self.reconcile_admission(&token, &batch)?;
        // 换代后旧回包只能补历史，但已证明承载的同批也不能获得再次发送的资格。
        for p in self.runs.reconciliation.operations.values().filter(|p| {
            p.operation.token.run_id == token.run_id && p.operation.work_epoch >= batch.work_epoch
        }) {
            match p.operation.submission {
                ReconcileSubmission::ConfirmedOwnTurn | ReconcileSubmission::MergedOwnGoalTurn => {
                    return Err(ReconcileRefusal::AlreadyDelivered)
                }
                ReconcileSubmission::MergedForeignOrUnknownTurn => {
                    return Err(ReconcileRefusal::OwnershipConflict)
                }
                _ => {}
            }
        }
        if self.runs.reconciliation.operations.values().any(|p| {
            p.operation.token.run_id == token.run_id
                && matches!(
                    p.operation.submission,
                    ReconcileSubmission::NotSent
                        | ReconcileSubmission::OutcomeUnknown
                        | ReconcileSubmission::DefinitelyNotSubmitted
                )
        }) {
            return Err(ReconcileRefusal::AlreadyInFlight);
        }
        let state = &mut self.runs.reconciliation;
        // 原操作号永不复用；落定后的原回包直接按未知编号丢弃，无需无限保存载荷。
        state
            .operations
            .retain(|_, p| p.operation.token.run_id != token.run_id || p.operation.outcome_unknown);
        state.next_op = state.next_op.checked_add(1).expect("汇总操作编号已耗尽");
        let op_id = ReconcileOpId(state.next_op);
        let operation = ReconcileOperation {
            token: token.clone(),
            work_epoch: batch.work_epoch,
            op_id,
            client_user_message_id: format!(
                "cxb-reconcile-{}-{}-{}-{}",
                self.runs.epoch, self.clock.entropy, token.run_id, op_id.0
            ),
            expected_previous_turn_id: self.runs.runs[&token.run_id].current_turn_id.clone(),
            target_turn_id: None,
            submission: ReconcileSubmission::NotSent,
            outcome_unknown: false,
            automatic_control_revoked: false,
            verify_deadline_mono_ms: None,
        };
        let mut known_turns = self.claims[&token.claim.thread_id].owned_turns.clone();
        known_turns.extend(
            self.runs.runs[&token.run_id]
                .turns
                .iter()
                .map(|t| t.turn_id.clone()),
        );
        if let Some(record) = self.records.get(&token.claim.thread_id) {
            known_turns.extend(
                record
                    .ended_turns
                    .iter()
                    .chain(&record.released_turns)
                    .cloned(),
            );
        }
        state.operations.insert(
            op_id.0,
            Pending {
                verify_token: token.clone(),
                operation,
                batch,
                method: String::new(),
                known_turns,
                started: HashMap::new(),
                observed_starts: HashSet::new(),
                completions: HashMap::new(),
                reply_turn: None,
                reply_received: false,
                status_read: None,
                next_verify: self.clock.mono_ms,
                expired: false,
            },
        );
        Ok(op_id)
    }
    /// detach/重连后由 A 用新的有效身份显式接续只读核实，不改原提交或刷新期限。
    pub fn reverify_subagent_reconcile(
        &mut self,
        token: &ConvergeToken,
        id: ReconcileOpId,
        actions: &mut Vec<Action>,
    ) -> Result<(), ReconcileRefusal> {
        if !self.reconcile_token_current(token) {
            return Err(ReconcileRefusal::StaleToken);
        }
        let pending = self
            .runs
            .reconciliation
            .operations
            .get(&id.0)
            .ok_or(ReconcileRefusal::InvalidBatch)?
            .clone();
        if pending.operation.token.run_id != token.run_id
            || pending.operation.token.claim != token.claim
        {
            return Err(ReconcileRefusal::StaleToken);
        }
        let deadline = pending
            .operation
            .verify_deadline_mono_ms
            .ok_or(ReconcileRefusal::InvalidBatch)?;
        if pending.expired || self.clock.mono_ms >= deadline {
            return Err(ReconcileRefusal::Deadline);
        }
        if !pending.operation.outcome_unknown {
            return Err(ReconcileRefusal::AlreadyDelivered);
        }
        if pending.operation.automatic_control_revoked {
            return Err(ReconcileRefusal::Abandoned);
        }
        if pending.verify_token == *token {
            return Ok(());
        }
        let registration = TrustedInputRegistration {
            token: token.clone(),
            op_id: id,
            client_user_message_id: pending.operation.client_user_message_id.clone(),
            work_epoch: pending.operation.work_epoch,
            expected_turn_id: (pending.method == "turn/steer")
                .then(|| pending.operation.expected_previous_turn_id.clone())
                .flatten(),
        };
        if !self.reconcile_register_input(registration, actions) {
            return Err(ReconcileRefusal::Disabled);
        }
        if let Some(read) = pending.status_read {
            self.cancel_status_read(read, actions);
        }
        let current = self
            .runs
            .reconciliation
            .operations
            .get_mut(&id.0)
            .expect("原操作仍在");
        current.verify_token = token.clone();
        current.status_read = None;
        current.next_verify = self.clock.mono_ms;
        self.verify_reconciliation(id, actions);
        Ok(())
    }

    /// A 在 step 末尾驱动；发送前再次检查撤权、版本和额度，登记先于 Request。
    pub fn drive_subagent_reconcile(&mut self, actions: &mut Vec<Action>) {
        if self.closing {
            return;
        }
        self.runs
            .reconciliation
            .parents
            .retain(|run, _| self.runs.runs.contains_key(run));
        self.runs.reconciliation.operations.retain(|_, p| {
            p.operation.outcome_unknown || self.runs.runs.contains_key(&p.operation.token.run_id)
        });
        let ids: Vec<_> = self
            .runs
            .reconciliation
            .operations
            .keys()
            .copied()
            .collect();
        for id in ids {
            let pending = self.runs.reconciliation.operations[&id].clone();
            match pending.operation.submission {
                ReconcileSubmission::NotSent => {
                    self.submit_reconciliation(ReconcileOpId(id), actions)
                }
                ReconcileSubmission::DefinitelyNotSubmitted
                    if !pending.operation.automatic_control_revoked =>
                {
                    let token = &pending.verify_token;
                    if !self.reconcile_token_current(token) {
                        continue;
                    }
                    let record = &self.runs.runs[&token.run_id];
                    let max = record
                        .convergence_config
                        .as_ref()
                        .map_or(2, |c| c.max_start_retries);
                    let used = record
                        .subagent_join
                        .as_ref()
                        .map_or(max, |j| j.start_retries_used);
                    if used >= max {
                        self.runs
                            .reconciliation
                            .operations
                            .get_mut(&id)
                            .unwrap()
                            .operation
                            .automatic_control_revoked = true;
                        self.reconcile_abnormal(token, TerminalReason::WakeFailed, actions);
                        continue;
                    }
                    if self.reconcile_admission(token, &pending.batch).is_err() {
                        continue;
                    }
                    self.runs
                        .reconciliation
                        .operations
                        .get_mut(&id)
                        .unwrap()
                        .operation
                        .submission = ReconcileSubmission::CancelledBeforeSubmit;
                    if let Ok(next) =
                        self.begin_subagent_reconcile(token.clone(), pending.batch, actions)
                    {
                        self.runs
                            .runs
                            .get_mut(&token.run_id)
                            .unwrap()
                            .subagent_join
                            .as_mut()
                            .unwrap()
                            .start_retries_used += 1;
                        self.submit_reconciliation(next, actions);
                    }
                }
                ReconcileSubmission::OutcomeUnknown => {
                    if pending
                        .operation
                        .verify_deadline_mono_ms
                        .is_some_and(|d| self.clock.mono_ms >= d)
                    {
                        if !pending.expired {
                            if let Some(read) = pending.status_read {
                                self.cancel_status_read(read, actions);
                            }
                            let p = self.runs.reconciliation.operations.get_mut(&id).unwrap();
                            p.expired = true;
                            p.status_read = None;
                            p.operation.automatic_control_revoked = true;
                            // 未知副作用继续阻塞 settled，不把期限到当成明确未提交。
                            self.reconcile_abnormal(
                                &pending.verify_token,
                                TerminalReason::WakeUnknown,
                                actions,
                            );
                        }
                    } else if self.clock.mono_ms >= pending.next_verify {
                        self.verify_reconciliation(ReconcileOpId(id), actions);
                    }
                }
                _ => {}
            }
        }
    }
    fn submit_reconciliation(&mut self, id: ReconcileOpId, actions: &mut Vec<Action>) {
        let pending = self.runs.reconciliation.operations[&id.0].clone();
        let token = &pending.operation.token;
        if self.reconcile_admission(token, &pending.batch).is_err() {
            self.runs
                .reconciliation
                .operations
                .get_mut(&id.0)
                .unwrap()
                .operation
                .submission = ReconcileSubmission::CancelledBeforeSubmit;
            return;
        }
        let target = self.reconcile_goal_target(token);
        let registration = TrustedInputRegistration {
            token: token.clone(),
            op_id: id,
            client_user_message_id: pending.operation.client_user_message_id.clone(),
            work_epoch: pending.operation.work_epoch,
            expected_turn_id: target.clone(),
        };
        if !self.reconcile_register_input(registration, actions) {
            self.runs
                .reconciliation
                .operations
                .get_mut(&id.0)
                .unwrap()
                .operation
                .submission = ReconcileSubmission::CancelledBeforeSubmit;
            return;
        }
        let record = &self.runs.runs[&token.run_id];
        let config = record.execution_config.as_ref().expect("准入已核实配置");
        let mut params = if target.is_none() {
            execution_params(config)
        } else {
            Map::new()
        };
        params.insert("threadId".into(), json!(token.claim.thread_id));
        params.insert(
            "clientUserMessageId".into(),
            json!(pending.operation.client_user_message_id),
        );
        params.insert(
            "input".into(),
            json!([{"type":"text","text":pending.batch.input,"text_elements":[]}]),
        );
        let method = if let Some(turn) = &target {
            params.insert("expectedTurnId".into(), json!(turn));
            "turn/steer"
        } else {
            params.insert("turnTrigger".into(), json!("cxb.subagent_reconcile"));
            "turn/start"
        };
        let deadline = self
            .clock
            .mono_ms
            .saturating_add(
                record
                    .convergence_config
                    .as_ref()
                    .unwrap()
                    .wake_verify_timeout_ms
                    .min(20_000),
            )
            .min(self.convergences.runs[&token.run_id].deadline_mono_ms);
        let p = self.runs.reconciliation.operations.get_mut(&id.0).unwrap();
        p.method = method.into();
        p.operation.submission = ReconcileSubmission::OutcomeUnknown;
        p.operation.outcome_unknown = true;
        p.operation.verify_deadline_mono_ms = Some(deadline);
        if let Some(turn) = target {
            p.started.insert(turn, TurnOrigin::Goal);
        }
        self.runs
            .runs
            .get_mut(&token.run_id)
            .unwrap()
            .subagent_join
            .as_mut()
            .unwrap()
            .wakeup_attempts += 1;
        self.reconcile_changed(&token.run_id, actions);
        actions.push(Action::Schedule {
            token: token.claim.clone(),
            kind: TimerKind::ReconcileVerifyDeadline {
                token: token.clone(),
                op_id: id,
            },
            delay_ms: deadline.saturating_sub(self.clock.mono_ms),
        });
        actions.push(Action::Request(RequestContext {
            resume_retried: false,
            stream_id: None,
            review_id: None,
            client: None,
            token: Some(token.claim.clone()),
            method: method.into(),
            params: Some(Value::Object(params)),
            operation: Operation::StartReconciliation {
                token: token.clone(),
                op_id: id,
            },
        }));
        self.verify_reconciliation(id, actions);
    }
    pub(super) fn verify_reconciliation(&mut self, id: ReconcileOpId, actions: &mut Vec<Action>) {
        let Some(pending) = self.runs.reconciliation.operations.get(&id.0) else {
            return;
        };
        if self.closing
            || pending.expired
            || !pending.operation.outcome_unknown
            || pending.status_read.is_some()
            || pending
                .operation
                .verify_deadline_mono_ms
                .is_some_and(|d| self.clock.mono_ms >= d)
        {
            return;
        }
        let token = pending.verify_token.clone();
        let deadline = pending
            .operation
            .verify_deadline_mono_ms
            .expect("已发操作有截止");
        // 撤权后仍可只读核实旧副作用；绝不因此创建新提交或认领。
        let query = StatusQuery::ClientMessage {
            client_user_message_id: pending.operation.client_user_message_id.clone(),
            expected_previous_turn_id: pending.operation.expected_previous_turn_id.clone(),
        };
        let read = self.begin_status_read(
            &token.claim.thread_id,
            Some(token.claim.clone()),
            query,
            StatusCaller::SubagentReconcile {
                token: token.clone(),
                op_id: id,
            },
            actions,
        );
        if read != 0 {
            self.runs
                .reconciliation
                .operations
                .get_mut(&id.0)
                .unwrap()
                .status_read = Some(read);
        }
        actions.push(Action::Schedule {
            token: token.claim.clone(),
            kind: TimerKind::ReconcileVerifyDeadline { token, op_id: id },
            delay_ms: deadline.saturating_sub(self.clock.mono_ms),
        });
    }
}

pub(crate) const EXECUTION_KEYS: &[&str] = &[
    "cwd",
    "runtimeWorkspaceRoots",
    "approvalPolicy",
    "approvalsReviewer",
    "sandboxPolicy",
    "permissionProfile",
    "permissions",
    "outputSchema",
    "serviceTier",
    "serviceTierForTurn",
    "cyberAccessProgram",
    "multiAgentMode",
    "summary",
    "personality",
    "collaborationMode",
    "environments",
    "disabledPluginIds",
];
pub(crate) fn execution_params(config: &RunExecutionConfig) -> Map<String, Value> {
    let mut result = Map::new();
    if let Some(params) = config.turn_params.as_ref().and_then(Value::as_object) {
        for key in EXECUTION_KEYS {
            if let Some(v) = params.get(*key) {
                result.insert((*key).into(), v.clone());
            }
        }
    }
    if let Some(model) = &config.model {
        result.insert("model".into(), json!(model));
    }
    if let Some(effort) = &config.effort {
        result.insert("effort".into(), json!(effort));
    }
    result
}
