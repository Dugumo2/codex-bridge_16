//! 串行推进门禁、发现、准入与终止。任何异步回调均复核完整身份及工作版本。
use super::*;
use crate::claim::finish_gate::ReleaseDecision;
use crate::relations::convergence::settled;
use crate::stop::tree::TreeScope;

impl Machine {
    pub(crate) fn convergence_parent_confirmed(
        &mut self,
        token: &ConvergeToken,
        turn: &str,
        actions: &mut Vec<Action>,
    ) {
        if !self.convergence_token_current(token)
            || self.convergences.runs[&token.run_id].automatic_control_revoked
            || !self.claims[&token.claim.thread_id]
                .owned_turns
                .contains(turn)
        {
            return;
        }
        if matches!(
            self.claims[&token.claim.thread_id].state,
            State::Converging { .. }
        ) {
            let owner = self.claims[&token.claim.thread_id]
                .owner()
                .cloned()
                .expect("已核实自有轮");
            self.claims.get_mut(&token.claim.thread_id).unwrap().state = State::Running {
                owner,
                turn_id: turn.into(),
                joined: false,
            };
            self.convergence_phase(&token.run_id, JoinPhase::Reconciling, actions);
        }
    }
    pub(crate) fn convergence_begin(
        &mut self,
        thread: &str,
        actions: &mut Vec<Action>,
    ) -> Option<String> {
        let claim = self.claims.get(thread)?;
        let run = claim.run_id.clone()?;
        if let Some(runtime) = self.convergences.runs.get_mut(&run) {
            if runtime.deadline_mono_ms == u64::MAX {
                let config = self.runs.runs[&run]
                    .convergence_config
                    .clone()
                    .unwrap_or_default();
                runtime.started_mono_ms = self.clock.mono_ms;
                runtime.deadline_mono_ms = self.clock.mono_ms.saturating_add(config.timeout_ms);
                if let Some(join) = self
                    .runs
                    .runs
                    .get_mut(&run)
                    .and_then(|r| r.subagent_join.as_mut())
                {
                    join.deadline_at = self.clock.now_ms.saturating_add(config.timeout_ms);
                }
                actions.push(Action::Schedule {
                    token: runtime.token.claim.clone(),
                    kind: TimerKind::ConvergeDeadline {
                        token: runtime.token.clone(),
                    },
                    delay_ms: config.timeout_ms,
                });
            }
            return Some(run);
        }
        if self.tree_abandoned(&run)
            || self
                .runs
                .runs
                .get(&run)?
                .subagent_join
                .as_ref()
                .is_some_and(|j| j.automatic_control_revoked)
        {
            return None;
        }
        // 管理凭据来自当前自有认领，不能由持久化摘要重建。
        if !self.ensure_context(&run) {
            return None;
        }
        let context = self.manage_context(&run)?;
        if !matches!(
            context.authority,
            Authority::Owner(_) | Authority::Broker { .. }
        ) {
            return None;
        }
        let manage_gen = context.gen;
        self.convergences.next_generation = self
            .convergences
            .next_generation
            .checked_add(1)
            .expect("协调器代号耗尽");
        let token = ConvergeToken {
            run_id: run.clone(),
            claim: self.token(thread)?,
            manage_gen,
            conn_epoch: self.relations.conn_epoch,
            generation: self.convergences.next_generation,
        };
        let config = self.runs.runs[&run]
            .convergence_config
            .clone()
            .unwrap_or_default();
        let runtime = ConvergeRuntime {
            token: token.clone(),
            started_mono_ms: self.clock.mono_ms,
            deadline_mono_ms: self.clock.mono_ms.saturating_add(config.timeout_ms),
            automatic_control_revoked: false,
            discovery_retry: 0,
        };
        self.convergences.runs.insert(run.clone(), runtime);
        self.convergences
            .progress
            .insert(run.clone(), Progress::default());
        self.convergence_sync_facts(&run);
        let epoch = self.convergences.ledgers[&run].work_epoch;
        let record = self.runs.runs.get_mut(&run)?;
        record.convergence_config = Some(config.clone());
        // 计数和预算由 C 的运行初始化共同建立。
        record.subagent_join = Some(SubagentJoin {
            phase: JoinPhase::WaitingSubtree,
            wakeups_started: 0,
            wakeup_attempts: 0,
            start_retries_used: 0,
            work_epoch: epoch,
            delivered_epoch: 0,
            reconciled_epoch: 0,
            last_reconcile_turn_id: None,
            deadline_at: self.clock.now_ms.saturating_add(config.timeout_ms),
            terminal_reason: None,
            result_coverage: None,
            automatic_control_revoked: false,
        });
        let budget = self.initial_convergence_budget(thread);
        self.initialize_run_safeguards(&token, budget, actions);
        actions.push(Action::Schedule {
            token: token.claim.clone(),
            kind: TimerKind::ConvergeDeadline { token },
            delay_ms: config.timeout_ms,
        });
        self.convergence_changed(&run, actions);
        Some(run)
    }

    pub fn convergence_release_gate(
        &mut self,
        thread: &str,
        goal_status: Option<&str>,
        turn_status: Option<TurnStatus>,
        actions: &mut Vec<Action>,
    ) -> Option<ReleaseDecision> {
        if !self.convergences.enabled || self.closing || self.quiescing {
            return None;
        }
        let claim = self.claims.get(thread)?;
        if claim.joined() || claim.settling() || claim.precheck() {
            return None;
        }
        let run = claim.run_id.clone()?;
        if self.tree_abandoned(&run) {
            return None;
        }
        if matches!(claim.state, State::FinishingTree { .. }) {
            return Some(ReleaseDecision::Hold);
        }
        if self.stop_busy(thread) || self.tree_for_run(&run).is_some() {
            return None;
        }
        if self.convergence_begin(thread, actions).is_none() {
            return Some(ReleaseDecision::Hold);
        }
        let reason = match turn_status {
            Some(TurnStatus::Failed) => Some(TerminalReason::ParentFailed),
            Some(TurnStatus::Interrupted) => Some(TerminalReason::ParentInterrupted),
            _ => goal_reason(goal_status),
        };
        if let Some(reason) = reason {
            self.convergence_fail(&run, reason, actions);
            return Some(ReleaseDecision::Hold);
        }
        let join = self.runs.runs[&run]
            .subagent_join
            .as_ref()
            .expect("摘要存在");
        let phase = if join.reconciled_epoch > 0 && join.reconciled_epoch == join.delivered_epoch {
            JoinPhase::FinalCheck
        } else {
            JoinPhase::WaitingSubtree
        };
        let claim = self.claims.get_mut(thread)?;
        let owner = claim.owner()?.clone();
        let turn_id = claim.turn_id().map(str::to_owned);
        let runtime = &self.convergences.runs[&run];
        claim.state = State::Converging {
            owner,
            turn_id,
            parent_status: turn_status,
            goal_status: goal_status.map(str::to_owned),
            token: runtime.token.clone(),
            phase,
        };
        self.convergence_phase(&run, phase, actions);
        self.convergence_drive(&run, actions);
        // drive 可以在最终核实后同步释放；调用者仍必须停止原来的释放流程。
        Some(ReleaseDecision::Hold)
    }

    pub fn convergence_event(
        &mut self,
        thread: &str,
        event: ConvergeEvent<'_>,
        actions: &mut Vec<Action>,
    ) -> bool {
        let Some(claim) = self.claims.get(thread) else {
            return false;
        };
        let Some(run) = claim.run_id.clone() else {
            return false;
        };
        let converging = matches!(claim.state, State::Converging { .. });
        let finishing = matches!(claim.state, State::FinishingTree { .. });
        if !self.convergences.runs.contains_key(&run) {
            if self.convergences.enabled
                && !claim.joined()
                && !claim.settling()
                && !claim.precheck()
                && matches!(event, ConvergeEvent::GoalChanged { .. })
            {
                let reason = goal_reason(
                    self.records
                        .get(thread)
                        .and_then(|r| r.goal_status.as_deref()),
                );
                if let Some(reason) = reason {
                    if self.convergence_begin(thread, actions).is_some() {
                        self.convergence_fail(&run, reason, actions);
                        return true;
                    }
                }
            }
            return false;
        }
        if event == ConvergeEvent::Detach {
            self.convergence_detach(&run, actions);
            return false;
        }
        if finishing {
            return false;
        }
        if self.tree_abandoned(&run) {
            return matches!(event, ConvergeEvent::ParentTurn { turn, started: true } if !self.claims[thread].owned_turns.contains(turn));
        }
        let revoked = self.convergences.runs[&run].automatic_control_revoked;
        match event {
            ConvergeEvent::ParentTurn {
                turn,
                started: true,
            } => {
                // started 本身不能授予自有权；B 确认并登记后才可进入 Running。
                let own = self.claims[thread].owned_turns.contains(turn);
                if self.claims[thread].owned_ended.iter().any(|t| t == turn)
                    || self.records.get(thread).is_some_and(|r| {
                        r.ended_turns
                            .iter()
                            .chain(&r.released_turns)
                            .any(|t| t == turn)
                    })
                {
                    return true;
                }
                if !own {
                    if revoked
                        && self.stop_busy(thread)
                        && !self.reconcile_unresolved(&run)
                        && self
                            .records
                            .get(thread)
                            .is_some_and(|r| r.goal_active && (!self.shared || r.goal_controlled))
                        && matches!(
                            self.claims[thread].state,
                            State::Running { .. } | State::AwaitingContinuation { .. }
                        )
                        && self.claims[thread].turn_id().is_none_or(|previous| {
                            self.claims[thread]
                                .owned_ended
                                .iter()
                                .any(|t| t == previous)
                        })
                    {
                        // 已确认自有 goal 的制动失败后续轮仍交原停止驱动，未知汇总不走此路。
                        return false;
                    }
                    if !revoked
                        && matches!(
                            self.claims[thread].state,
                            State::AwaitingContinuation { .. }
                        )
                    {
                        return false;
                    }
                    let token = self.convergences.runs[&run].token.clone();
                    self.reconcile_observe_parent_started(&token, turn);
                    self.reconcile_verify_started(&token, actions);
                    if revoked || self.convergence_root_unsettled(&run) {
                        return true;
                    }
                    self.convergence_fail(&run, TerminalReason::OwnershipConflict, actions);
                    return true;
                }
                if converging && !revoked {
                    let owner = self.claims[thread].owner().cloned().expect("自有认领");
                    self.claims.get_mut(thread).expect("认领存在").state = State::Running {
                        owner,
                        turn_id: turn.into(),
                        joined: false,
                    };
                    self.convergence_phase(&run, JoinPhase::Reconciling, actions);
                    return true;
                }
            }
            ConvergeEvent::ParentTurn {
                turn,
                started: false,
            } => {
                let status = self
                    .runs
                    .runs
                    .get(&run)
                    .and_then(|r| r.turns.iter().find(|t| t.turn_id == turn))
                    .map(|t| t.status);
                if matches!(status, Some(TurnStatus::Failed | TurnStatus::Interrupted)) {
                    self.convergence_fail(
                        &run,
                        if status == Some(TurnStatus::Failed) {
                            TerminalReason::ParentFailed
                        } else {
                            TerminalReason::ParentInterrupted
                        },
                        actions,
                    );
                    return true;
                }
                if status == Some(TurnStatus::Completed) {
                    let token = self.convergences.runs[&run].token.clone();
                    self.convergence_parent_reconciled(&token, turn, actions);
                }
                if converging {
                    self.convergence_drive(&run, actions);
                    return true;
                }
            }
            ConvergeEvent::GoalChanged { active } => {
                let goal = self.records.get(thread).and_then(|r| r.goal_status.clone());
                if goal.as_deref() == Some("paused") && self.goal_restoration_pending(thread) {
                    return false;
                }
                if let Some(reason) = goal_reason(goal.as_deref()) {
                    self.convergence_fail(&run, reason, actions);
                    return true;
                }
                if let Some(State::Converging { goal_status, .. }) =
                    self.claims.get_mut(thread).map(|c| &mut c.state)
                {
                    *goal_status = goal;
                }
                if converging {
                    self.convergence_drive(&run, actions);
                    return true;
                }
                if revoked && active {
                    return true;
                }
            }
            ConvergeEvent::Detach => {}
        }
        false
    }

    fn convergence_detach(&mut self, run: &str, actions: &mut Vec<Action>) {
        let Some(gen) = self.manage_context(run).map(|c| c.gen) else {
            return;
        };
        let runtime = self.convergences.runs.get_mut(run).expect("运行存在");
        let old = runtime.token.clone();
        if old.manage_gen == gen {
            return;
        }
        runtime.token.manage_gen = gen;
        let token = runtime.token.clone();
        let remaining = runtime.deadline_mono_ms.saturating_sub(self.clock.mono_ms);
        if let Some(State::Converging {
            token: state_token, ..
        }) = self
            .claims
            .get_mut(&token.claim.thread_id)
            .map(|c| &mut c.state)
        {
            *state_token = token.clone();
        }
        // detach 是授权交接；保留原提交，只以新身份重新读取承载证据。
        self.cancel_child_results(&old, actions);
        self.rebind_run_safeguards(&old, &token);
        let ids: Vec<_> = self
            .runs
            .reconciliation
            .operations
            .values()
            .filter(|p| p.verify_token == old && p.operation.outcome_unknown)
            .map(|p| p.operation.op_id)
            .collect();
        for id in ids {
            let _ = self.reverify_subagent_reconcile(&token, id, actions);
        }
        if let Some(progress) = self.convergences.progress.get_mut(run) {
            progress.discovery = None;
            progress.reading = None;
            progress.retry_at = None;
        }
        actions.push(Action::Schedule {
            token: token.claim.clone(),
            kind: TimerKind::ConvergeDeadline { token },
            delay_ms: remaining,
        });
        self.convergence_changed(run, actions);
        self.convergence_drive(run, actions);
    }

    pub(crate) fn convergence_drive(&mut self, run: &str, actions: &mut Vec<Action>) {
        let Some(runtime) = self.convergences.runs.get(run).cloned() else {
            return;
        };
        if runtime.automatic_control_revoked
            || self.closing
            || self.quiescing
            || !self.convergence_token_current(&runtime.token)
        {
            return;
        }
        if self
            .convergences
            .progress
            .get(run)
            .is_none_or(|p| p.driving)
        {
            return;
        }
        self.convergences
            .progress
            .get_mut(run)
            .expect("进度存在")
            .driving = true;
        self.convergence_drive_once(run, actions);
        if let Some(p) = self.convergences.progress.get_mut(run) {
            p.driving = false;
        }
    }

    fn convergence_drive_once(&mut self, run: &str, actions: &mut Vec<Action>) {
        let runtime = self.convergences.runs[run].clone();
        let thread = runtime.token.claim.thread_id.clone();
        if let Some(reason) = goal_reason(
            self.records
                .get(&thread)
                .and_then(|r| r.goal_status.as_deref()),
        )
        .filter(|_| !self.goal_restoration_pending(&thread))
        {
            self.convergence_fail(run, reason, actions);
            return;
        }
        if self.clock.mono_ms >= runtime.deadline_mono_ms {
            self.convergence_deadline(run, actions);
            return;
        }
        let merging_goal = self.reconcile_goal_target(&runtime.token).is_some();
        if !matches!(self.claims[&thread].state, State::Converging { .. }) && !merging_goal {
            return;
        }
        self.convergence_sync_facts(run);
        let Some(facts) = self.convergence_facts(run) else {
            return;
        };
        if merging_goal && facts.work_epoch == 0 {
            return;
        }
        if runtime.deadline_mono_ms == u64::MAX {
            self.convergence_begin(&thread, actions);
        }
        if let Some(join) = self
            .runs
            .runs
            .get_mut(run)
            .and_then(|r| r.subagent_join.as_mut())
        {
            join.work_epoch = facts.work_epoch;
        }
        if !facts.conflicts.is_empty() {
            self.convergence_fail(run, TerminalReason::OwnershipConflict, actions);
            return;
        }
        if self.convergence_root_unsettled(run) {
            return;
        }
        if self.claims[&thread].request_pending
            || self.stops.changes.values().any(|c| c.thread == thread)
        {
            self.convergence_retry(run, actions);
            return;
        }
        let p = &self.convergences.progress[run];
        if p.discovery.is_some()
            || p.reading.is_some()
            || p.retry_at.is_some()
            || p.operations.values().any(|o| {
                matches!(
                    o.submission,
                    ReconcileSubmission::NotSent | ReconcileSubmission::OutcomeUnknown
                )
            })
        {
            return;
        }
        if facts.discovery_covered_epoch != Some(facts.work_epoch) || !facts.discovery.complete {
            self.convergence_discover(run, actions);
            return;
        }
        if !settled(&facts) {
            self.convergence_retry(run, actions);
            return;
        }
        let join = self.runs.runs[run]
            .subagent_join
            .as_ref()
            .expect("摘要存在")
            .clone();
        if facts.work_epoch == 0 || join.reconciled_epoch == facts.work_epoch {
            if merging_goal {
                return;
            }
            self.convergence_phase(run, JoinPhase::Released, actions);
            self.release(&thread, actions);
            return;
        }
        if join.delivered_epoch >= facts.work_epoch {
            return;
        }
        match self.thread_capability(&thread) {
            Some(false) => {
                self.convergence_fail(run, TerminalReason::RootNoDirectInput, actions);
                return;
            }
            None => {
                self.convergence_retry(run, actions);
                return;
            }
            Some(true) => {}
        }
        let config = self.runs.runs[run]
            .convergence_config
            .clone()
            .unwrap_or_default();
        if !merging_goal && join.wakeups_started >= config.max_wakeups {
            self.convergence_fail(run, TerminalReason::WakeLimit, actions);
            return;
        }
        if !merging_goal
            && self.runs.runs[run].auto_turns_started.unwrap_or(0) >= config.max_auto_turns
        {
            self.convergence_fail(run, TerminalReason::AutoTurnLimit, actions);
            return;
        }
        match self.convergence_budget(&runtime.token) {
            BudgetAdmission::Exhausted => {
                self.convergence_fail(run, TerminalReason::BudgetExhausted, actions);
                return;
            }
            BudgetAdmission::NeedsVerification => {
                if self.convergences.progress[run]
                    .budget_verify_started
                    .is_some_and(|at| self.clock.mono_ms.saturating_sub(at) >= 20_000)
                {
                    self.convergence_fail(run, TerminalReason::BudgetIncomplete, actions);
                } else {
                    if self.convergences.progress[run]
                        .budget_verify_started
                        .is_none()
                    {
                        self.convergences
                            .progress
                            .get_mut(run)
                            .unwrap()
                            .budget_verify_started = Some(self.clock.mono_ms);
                        self.verify_run_budget(&runtime.token, actions);
                    }
                    self.convergence_retry(run, actions);
                }
                return;
            }
            BudgetAdmission::Disabled => {
                self.convergence_retry(run, actions);
                return;
            }
            BudgetAdmission::Allowed => {}
        }
        self.convergence_phase(run, JoinPhase::PreparingReconcile, actions);
        let mut limits = config.result_limits;
        limits.read_timeout_ms = limits
            .read_timeout_ms
            .min(runtime.deadline_mono_ms.saturating_sub(self.clock.mono_ms));
        let read = self.convergence_read(runtime.token, facts.work_epoch, limits, actions);
        if read == BatchReadId::DISABLED {
            self.convergence_retry(run, actions);
            return;
        }
        self.convergences
            .progress
            .get_mut(run)
            .expect("进度存在")
            .reading = Some((read, facts.work_epoch));
    }

    fn convergence_discover(&mut self, run: &str, actions: &mut Vec<Action>) {
        let token = self.convergences.runs[run].token.clone();
        let epoch = self.convergences.ledgers[run].work_epoch;
        // begin_discovery 仅登记动作，不同步执行回包。
        let op = self.begin_discovery(
            &token.claim.thread_id.clone(),
            DiscoveryScope::Run { run_id: run.into() },
            DiscoveryCaller::Converge {
                token,
                work_epoch: epoch,
            },
            actions,
        );
        let generation = self.relations.discoveries[&op].generation;
        let progress = self.convergences.progress.get_mut(run).expect("进度存在");
        progress.discovery = Some((op, epoch));
        progress.discovery_generation = generation;
    }

    pub fn convergence_discovery_done(
        &mut self,
        token: ConvergeToken,
        work_epoch: u64,
        result: DiscoveryResult,
        actions: &mut Vec<Action>,
    ) {
        if !self.convergence_token_current(&token)
            || self.convergences.runs[&token.run_id].automatic_control_revoked
        {
            return;
        }
        let run = &token.run_id;
        let Some((op, epoch)) = self
            .convergences
            .progress
            .get(run)
            .and_then(|p| p.discovery)
        else {
            return;
        };
        if result.generation != self.convergences.progress[run].discovery_generation
            || epoch != work_epoch
            || result.scope
                != (DiscoveryScope::Run {
                    run_id: run.clone(),
                })
            || result.root != token.claim.thread_id
        {
            return;
        }
        // 应用成员时保持 in-flight，避免成员回调递归重复启动发现。
        self.convergence_apply_discovery(
            op,
            DiscoveryCaller::Converge {
                token: token.clone(),
                work_epoch,
            },
            result.clone(),
            actions,
        );
        self.convergence_sync_facts(run);
        self.convergences.progress.get_mut(run).unwrap().discovery = None;
        let Some(facts) = self.convergence_facts(run) else {
            return;
        };
        if !result.cancelled
            && result.state.complete
            && facts.work_epoch == work_epoch
            && facts.pending_starts == 0
            && facts.unresolved_lookups == 0
            && facts.conflicts.is_empty()
        {
            self.convergences
                .ledgers
                .get_mut(run)
                .unwrap()
                .covered_epoch = Some(work_epoch);
            self.convergence_changed(run, actions);
            self.convergence_drive(run, actions);
        } else if !result.cancelled && result.state.complete && facts.work_epoch != work_epoch {
            // 在途发现期间的新结果要求立即覆盖新批次，不沿用失败退避。
            self.convergence_drive(run, actions);
        } else {
            self.convergence_retry(run, actions);
        }
    }

    fn convergence_retry(&mut self, run: &str, actions: &mut Vec<Action>) {
        if self.convergences.progress[run].retry_at.is_some() {
            return;
        }
        let config = self.runs.runs[run]
            .convergence_config
            .clone()
            .unwrap_or_default();
        let runtime = self.convergences.runs.get_mut(run).expect("协调器存在");
        let delay = config
            .discovery_backoff_ms
            .get(runtime.discovery_retry as usize)
            .copied()
            .unwrap_or(config.discovery_max_interval_ms)
            .min(runtime.deadline_mono_ms.saturating_sub(self.clock.mono_ms));
        runtime.discovery_retry = runtime.discovery_retry.saturating_add(1);
        let token = runtime.token.clone();
        let attempt = runtime.discovery_retry;
        self.convergences.progress.get_mut(run).unwrap().retry_at =
            Some(self.clock.mono_ms.saturating_add(delay));
        actions.push(Action::Schedule {
            token: token.claim.clone(),
            kind: TimerKind::ConvergeRetry { token, attempt },
            delay_ms: delay,
        });
    }

    pub fn convergence_timer(&mut self, kind: TimerKind, actions: &mut Vec<Action>) {
        match kind {
            TimerKind::BudgetVerifyDeadline { token } => {
                if self.convergence_token_current(&token)
                    && self.budget_verification_expired(&token)
                {
                    self.convergence_fail(&token.run_id, TerminalReason::BudgetIncomplete, actions);
                }
            }
            TimerKind::ConvergeDeadline { token } => {
                if self.convergence_token_current(&token)
                    && self.clock.mono_ms >= self.convergences.runs[&token.run_id].deadline_mono_ms
                {
                    self.convergence_deadline(&token.run_id, actions);
                }
            }
            TimerKind::ConvergeRetry { token, attempt } => {
                if !self.convergence_token_current(&token)
                    || self.convergences.runs[&token.run_id].discovery_retry != attempt
                {
                    return;
                }
                let p = self
                    .convergences
                    .progress
                    .get_mut(&token.run_id)
                    .expect("进度存在");
                if p.retry_at.is_none_or(|at| at > self.clock.mono_ms) {
                    return;
                }
                p.retry_at = None;
                // 回查目标是当前版本；覆盖不能跨新工作沿用。
                self.convergences
                    .ledgers
                    .get_mut(&token.run_id)
                    .unwrap()
                    .covered_epoch = None;
                self.convergence_drive(&token.run_id, actions);
            }
            TimerKind::ChildResultsDeadline { token, read_id } => {
                self.child_results_deadline(&token, read_id, actions)
            }
            TimerKind::ReconcileVerifyDeadline { token, op_id } => {
                self.subagent_reconcile_timer(&token, op_id, actions)
            }
            _ => {}
        }
    }

    fn convergence_deadline(&mut self, run: &str, actions: &mut Vec<Action>) {
        let token = self.convergences.runs[run].token.clone();
        if self.budget_verification_expired(&token) {
            self.convergence_fail(run, TerminalReason::BudgetIncomplete, actions);
            return;
        }
        if self
            .convergences
            .runs
            .get(run)
            .is_none_or(|r| r.automatic_control_revoked)
        {
            return;
        }
        let incomplete = self.convergence_facts(run).is_none_or(|f| {
            !f.discovery.complete
                || f.discovery_covered_epoch != Some(f.work_epoch)
                || f.unresolved_lookups > 0
        });
        self.convergence_fail(
            run,
            if incomplete {
                TerminalReason::DiscoveryIncomplete
            } else {
                TerminalReason::ConvergeDeadline
            },
            actions,
        );
    }

    pub fn convergence_fail(
        &mut self,
        run: &str,
        reason: TerminalReason,
        actions: &mut Vec<Action>,
    ) {
        if !self.convergences.runs.contains_key(run)
            && self.convergences.enabled
            && !self.closing
            && !self.quiescing
        {
            let thread = self
                .claims
                .iter()
                .find(|(_, c)| {
                    c.run_id.as_deref() == Some(run)
                        && !c.joined()
                        && !c.settling()
                        && !c.precheck()
                })
                .map(|(t, _)| t.clone());
            if let Some(thread) = thread {
                self.convergence_begin(&thread, actions);
            }
        }
        let Some(runtime) = self.convergences.runs.get(run).cloned() else {
            return;
        };
        if self.tree_abandoned(run) || self.closing || self.quiescing {
            return;
        }
        let thread = runtime.token.claim.thread_id.clone();
        if !self.current(&runtime.token.claim) {
            return;
        }
        if let Some(join) = self
            .runs
            .runs
            .get_mut(run)
            .and_then(|r| r.subagent_join.as_mut())
        {
            if join.terminal_reason.is_none()
                || matches!(
                    reason,
                    TerminalReason::ParentFailed | TerminalReason::ParentInterrupted
                )
            {
                join.terminal_reason = Some(reason);
            }
        }
        self.convergence_revoke(
            run,
            if reason == TerminalReason::ConvergeDeadline
                || reason == TerminalReason::DiscoveryIncomplete
            {
                ReconcileCancel::Deadline
            } else {
                ReconcileCancel::GoalInactive
            },
            actions,
        );
        if self.tree_for_run(run).is_some() || self.stop_busy(&thread) {
            return;
        }
        let Some(claim) = self.claims.get(&thread) else {
            return;
        };
        let parent_status = match &claim.state {
            State::Converging { parent_status, .. }
            | State::FinishingTree { parent_status, .. } => *parent_status,
            _ => None,
        };
        let trigger = terminal::terminal(&TerminalInput {
            reason,
            parent_status,
            existing_interrupt: self.runs.runs[run].interrupt_reason,
        })
        .and_then(|t| t.tree_trigger);
        let Some(trigger) = trigger else {
            return;
        };
        let mut request = self.tree_request(
            &thread,
            run,
            TreeScope::Run { run_id: run.into() },
            trigger,
            false,
            None,
        );
        request.snapshot.parent_status = parent_status;
        let waiting = matches!(claim.state, State::Converging { .. });
        if waiting {
            let owner = claim.owner().cloned().expect("自有认领");
            let turn_id = claim.turn_id().map(str::to_owned);
            self.claims.get_mut(&thread).unwrap().state = State::FinishingTree {
                owner,
                turn_id,
                parent_status,
                goal_status: request.snapshot.goal_status.clone(),
                identity: request.snapshot.identity.clone(),
                gen: request.snapshot.gen,
                tree_op: 0,
                release_kind: ReleaseKind::Natural,
            };
            request.root_already_idle = true;
        }
        let tree_op = self.begin_tree_stop(request, actions);
        if let Some(State::FinishingTree { tree_op: slot, .. }) =
            self.claims.get_mut(&thread).map(|c| &mut c.state)
        {
            *slot = tree_op;
        }
    }

    /// B 的 child_results_done 调用此入口；零号、重复回调和旧批次都不能提交。
    pub fn convergence_results_ready(
        &mut self,
        token: ConvergeToken,
        read_id: BatchReadId,
        batch: ReconcileBatch,
        actions: &mut Vec<Action>,
    ) {
        if read_id == BatchReadId::DISABLED || !self.convergence_token_current(&token) {
            return;
        }
        let run = token.run_id.clone();
        let p = self.convergences.progress.get_mut(&run).expect("进度存在");
        if p.reading != Some((read_id, batch.work_epoch)) {
            return;
        }
        p.reading = None;
        if self.convergences.runs[&run].automatic_control_revoked {
            return;
        }
        if self.clock.mono_ms >= self.convergences.runs[&run].deadline_mono_ms {
            self.convergence_deadline(&run, actions);
            return;
        }
        let Some(facts) = self.convergence_facts(&run) else {
            return;
        };
        if !settled(&facts)
            || facts.work_epoch != batch.work_epoch
            || facts.discovery_covered_epoch != Some(batch.work_epoch)
        {
            self.convergence_retry(&run, actions);
            return;
        }
        let coverage = &batch.coverage;
        if coverage.epoch != batch.work_epoch
            || coverage
                .available
                .checked_add(coverage.unavailable)
                .and_then(|v| v.checked_add(coverage.omitted))
                != Some(coverage.total)
            || coverage.truncated > coverage.available
            || coverage.omitted != batch.omitted
            || batch.total_bytes != batch.input.len()
            || coverage.total != facts.activities.len()
            || batch.items.len().checked_add(batch.omitted) != Some(coverage.total)
            || batch.total_bytes
                > self.runs.runs[&run]
                    .convergence_config
                    .as_ref()
                    .map_or(65536, |c| c.result_limits.total_bytes)
            || batch.items.iter().any(|item| {
                !facts.activities.iter().any(|a| {
                    a.key.thread_id == item.key.thread_id
                        && a.key.activity_seq == item.key.activity_seq
                        && a.key.turn_id == item.key.turn_id
                        && a.result_status == Some(item.status)
                })
            })
            || batch.items.iter().enumerate().any(|(i, item)| {
                batch.items[..i].iter().any(|old| {
                    old.key.thread_id == item.key.thread_id
                        && old.key.activity_seq == item.key.activity_seq
                })
            })
        {
            self.convergence_fail(&run, TerminalReason::WakeFailed, actions);
            return;
        }
        match self.convergence_submit(token, batch, actions) {
            Ok(op) if op.0 != 0 => {}
            Ok(_)
            | Err(
                ReconcileRefusal::Disabled
                | ReconcileRefusal::AlreadyInFlight
                | ReconcileRefusal::AlreadyDelivered
                | ReconcileRefusal::StaleToken
                | ReconcileRefusal::Stopping
                | ReconcileRefusal::Abandoned,
            ) => {}
            Err(
                ReconcileRefusal::WorkChanged
                | ReconcileRefusal::DiscoveryIncomplete
                | ReconcileRefusal::CapabilityUnknown,
            ) => self.convergence_retry(&run, actions),
            Err(ReconcileRefusal::BudgetIncomplete) => {
                let token = self.convergences.runs[&run].token.clone();
                self.verify_run_budget(&token, actions);
                self.convergence_retry(&run, actions);
            }
            Err(reason) => self.convergence_fail(&run, refusal_reason(reason), actions),
        }
    }

    /// B 只在 clientId、归属与完整批次覆盖已核实后调用；父 completed 后推进完成覆盖。
    pub fn convergence_delivered(
        &mut self,
        token: &ConvergeToken,
        op_id: ReconcileOpId,
        turn: &str,
        epoch: u64,
        actions: &mut Vec<Action>,
    ) -> bool {
        if !self.convergence_token_current(token)
            || self.convergences.runs[&token.run_id].automatic_control_revoked
        {
            return false;
        }
        let own = self.claims[&token.claim.thread_id]
            .owned_turns
            .contains(turn);
        let proven = self.convergences.progress[&token.run_id]
            .operations
            .get(&op_id.0)
            .is_some_and(|o| {
                (o.token == *token
                    || self
                        .runs
                        .reconciliation
                        .operations
                        .get(&op_id.0)
                        .is_some_and(|p| p.verify_token == *token && p.operation == *o))
                    && o.work_epoch == epoch
                    && o.target_turn_id.as_deref() == Some(turn)
                    && !o.outcome_unknown
                    && matches!(
                        o.submission,
                        ReconcileSubmission::ConfirmedOwnTurn
                            | ReconcileSubmission::MergedOwnGoalTurn
                    )
            });
        if !own || !proven || epoch > self.convergences.ledgers[&token.run_id].work_epoch {
            return false;
        }
        let join = self
            .runs
            .runs
            .get_mut(&token.run_id)
            .unwrap()
            .subagent_join
            .as_mut()
            .unwrap();
        if epoch < join.delivered_epoch
            || (epoch == join.delivered_epoch
                && join
                    .last_reconcile_turn_id
                    .as_deref()
                    .is_some_and(|id| id != turn))
        {
            return false;
        }
        join.delivered_epoch = join.delivered_epoch.max(epoch);
        join.last_reconcile_turn_id = Some(turn.into());
        self.convergence_changed(&token.run_id, actions);
        if self.convergence_parent_reconciled(token, turn, actions) {
            self.convergence_drive(&token.run_id, actions);
        }
        true
    }

    pub fn convergence_parent_reconciled(
        &mut self,
        token: &ConvergeToken,
        turn: &str,
        actions: &mut Vec<Action>,
    ) -> bool {
        if !self.convergence_token_current(token)
            || self.convergences.runs[&token.run_id].automatic_control_revoked
        {
            return false;
        }
        let record = &self.runs.runs[&token.run_id];
        let join = record.subagent_join.as_ref().unwrap();
        if join.last_reconcile_turn_id.as_deref() != Some(turn)
            || !record
                .turns
                .iter()
                .any(|t| t.turn_id == turn && t.status == TurnStatus::Completed)
        {
            return false;
        }
        if join.reconciled_epoch == join.delivered_epoch {
            return true;
        }
        let join = self
            .runs
            .runs
            .get_mut(&token.run_id)
            .unwrap()
            .subagent_join
            .as_mut()
            .unwrap();
        join.reconciled_epoch = join.delivered_epoch;
        self.convergences
            .ledgers
            .get_mut(&token.run_id)
            .unwrap()
            .covered_epoch = None;
        self.convergence_phase(&token.run_id, JoinPhase::FinalCheck, actions);
        self.convergence_changed(&token.run_id, actions);
        true
    }

    fn convergence_budget(&self, token: &ConvergeToken) -> BudgetAdmission {
        #[cfg(test)]
        if let Some(value) = self.convergences.test_budget {
            return value;
        }
        self.reconcile_budget_admission(token)
    }
    fn convergence_read(
        &mut self,
        token: ConvergeToken,
        epoch: u64,
        limits: ResultLimits,
        actions: &mut Vec<Action>,
    ) -> BatchReadId {
        #[cfg(test)]
        if let Some(value) = self.convergences.test_read {
            return value;
        }
        self.read_child_results(token, epoch, limits, actions)
    }
    fn convergence_submit(
        &mut self,
        token: ConvergeToken,
        batch: ReconcileBatch,
        actions: &mut Vec<Action>,
    ) -> Result<ReconcileOpId, ReconcileRefusal> {
        #[cfg(test)]
        if let Some(value) = self.convergences.test_submission {
            return value;
        }
        self.begin_subagent_reconcile(token, batch, actions)
    }
}

fn goal_reason(status: Option<&str>) -> Option<TerminalReason> {
    Some(match status? {
        "paused" => TerminalReason::GoalPaused,
        "blocked" => TerminalReason::GoalBlocked,
        "budgetLimited" => TerminalReason::GoalBudgetLimited,
        "usageLimited" => TerminalReason::GoalUsageLimited,
        _ => return None,
    })
}
fn refusal_reason(reason: ReconcileRefusal) -> TerminalReason {
    match reason {
        ReconcileRefusal::OwnershipConflict => TerminalReason::OwnershipConflict,
        ReconcileRefusal::RootNoDirectInput => TerminalReason::RootNoDirectInput,
        ReconcileRefusal::GoalInactive => TerminalReason::GoalPaused,
        ReconcileRefusal::BudgetExhausted => TerminalReason::BudgetExhausted,
        ReconcileRefusal::WakeLimit => TerminalReason::WakeLimit,
        ReconcileRefusal::AutoTurnLimit => TerminalReason::AutoTurnLimit,
        ReconcileRefusal::Deadline => TerminalReason::ConvergeDeadline,
        _ => TerminalReason::WakeFailed,
    }
}
