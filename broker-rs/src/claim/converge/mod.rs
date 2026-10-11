//! A 路协调器：生产开关默认关闭，集成确认 B/C 完整后才允许启用。
use super::{Action, ConnId, Machine, Owner, State, TimerKey, TimerKind};
use crate::relations::{Authority, DiscoveryCaller, DiscoveryResult, DiscoveryScope, MemberChange};
use crate::runs::{InterruptReason, ReleaseKind, TurnStatus};
use crate::sa1b::*;
use std::collections::{BTreeMap, HashMap};

mod drive;
pub(crate) mod terminal;
#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConvergeRuntime {
    pub token: ConvergeToken,
    pub started_mono_ms: u64,
    pub deadline_mono_ms: u64,
    pub automatic_control_revoked: bool,
    pub discovery_retry: u32,
}

/// 由集成层在完整组件验证通过后提供；没有任何环境变量或用户参数能默认打开门禁。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConvergenceReadiness {
    pub trusted_inputs: bool,
    pub budget_admission: bool,
    pub asynchronous_results: bool,
    pub terminal_mapping: bool,
    pub cancellation_verified: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Progress {
    pub discovery: Option<(u64, u64)>,
    pub discovery_generation: u64,
    pub retry_at: Option<u64>,
    pub reading: Option<(BatchReadId, u64)>,
    pub driving: bool,
    pub terminal_locked: bool,
    pub parent_before_stop: Option<TurnStatus>,
    pub interrupt_before_stop: Option<InterruptReason>,
    pub budget_verify_started: Option<u64>,
    pub operations: BTreeMap<u64, ReconcileOperation>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Convergences {
    enabled: bool,
    pub runs: HashMap<String, ConvergeRuntime>,
    pub next_generation: u64,
    pub ledgers: HashMap<String, crate::relations::convergence::ConvergenceLedger>,
    pub goals: BTreeMap<String, (u64, bool)>,
    pub capabilities: BTreeMap<String, (u64, bool)>,
    pub observations:
        BTreeMap<(String, String), crate::relations::convergence::ActivityObservation>,
    pub progress: HashMap<String, Progress>,
    #[cfg(test)]
    pub test_budget: Option<BudgetAdmission>,
    #[cfg(test)]
    pub test_read: Option<BatchReadId>,
    #[cfg(test)]
    pub test_submission: Option<Result<ReconcileOpId, ReconcileRefusal>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConvergeEvent<'a> {
    ParentTurn { turn: &'a str, started: bool },
    GoalChanged { active: bool },
    Detach,
}

impl Machine {
    pub fn enable_convergence(&mut self, ready: ConvergenceReadiness) -> bool {
        if !(ready.trusted_inputs
            && ready.budget_admission
            && ready.asynchronous_results
            && ready.terminal_mapping
            && ready.cancellation_verified)
            || self.closing
            || self.quiescing
        {
            return false;
        }
        self.convergences.enabled = true;
        true
    }

    pub(crate) fn convergence_enabled(&self) -> bool {
        self.convergences.enabled
    }

    pub fn convergence_token_current(&self, token: &ConvergeToken) -> bool {
        self.current(&token.claim)
            && self
                .claims
                .get(&token.claim.thread_id)
                .is_some_and(|c| c.run_id.as_deref() == Some(&token.run_id) && !c.joined())
            && self.relations.conn_epoch == token.conn_epoch
            && self.manage_context(&token.run_id).is_some_and(|c| {
                c.gen == token.manage_gen
                    && matches!(c.authority, Authority::Owner(_) | Authority::Broker { .. })
            })
            && self
                .convergences
                .runs
                .get(&token.run_id)
                .is_some_and(|r| r.token == *token)
    }

    pub fn convergence_summary(&self, run: &str) -> Option<ConvergenceSummary> {
        let r = self.runs.runs.get(run)?;
        if r.subagent_join.is_none()
            && r.run_budget.is_none()
            && r.auto_turns_started.is_none()
            && r.final_turn_id.is_none()
        {
            return None;
        }
        Some(ConvergenceSummary {
            subagent_join: r.subagent_join.clone(),
            run_budget: r.run_budget.clone(),
            final_turn_id: r.final_turn_id.clone(),
            auto_turns_started: r.auto_turns_started,
        })
    }

    pub fn notify_convergence_changed(&self, run: &str, actions: &mut Vec<Action>) {
        let (Some(context), Some(summary)) =
            (self.manage_context(run), self.convergence_summary(run))
        else {
            return;
        };
        let mut params = serde_json::to_value(summary).expect("协调摘要可序列化");
        params["runId"] = serde_json::json!(run);
        params["rootThreadId"] = serde_json::json!(context.identity.logical_root_thread_id);
        params["revision"] = serde_json::json!(context.revision);
        let message = crate::proto::notification("broker/run/convergenceChanged", Some(params));
        for conn in self.run_recipients(run) {
            self.send_ext(conn, message.clone(), actions);
        }
    }

    pub(crate) fn convergence_changed(&mut self, run: &str, actions: &mut Vec<Action>) {
        if let Some(context) = self.relations.contexts.get_mut(run) {
            context.revision = context.revision.saturating_add(1);
        }
        self.runs.put(run, actions);
        self.notify_convergence_changed(run, actions);
    }

    pub(crate) fn convergence_phase(
        &mut self,
        run: &str,
        phase: JoinPhase,
        actions: &mut Vec<Action>,
    ) {
        let Some(join) = self
            .runs
            .runs
            .get_mut(run)
            .and_then(|r| r.subagent_join.as_mut())
        else {
            return;
        };
        if join.phase == phase {
            return;
        }
        join.phase = phase;
        if let Some(thread) = self
            .convergences
            .runs
            .get(run)
            .map(|r| r.token.claim.thread_id.clone())
        {
            if let Some(State::Converging { phase: current, .. }) =
                self.claims.get_mut(&thread).map(|c| &mut c.state)
            {
                *current = phase;
            }
        }
        self.convergence_changed(run, actions);
    }

    pub fn on_member_changed(
        &mut self,
        run_id: &str,
        revision: u64,
        reason: MemberChange,
        actions: &mut Vec<Action>,
    ) {
        self.convergence_sync_facts(run_id);
        self.tree_member_changed(run_id, revision, reason, actions);
        self.convergence_member_changed(run_id, revision, reason, actions);
    }

    pub fn convergence_member_changed(
        &mut self,
        run: &str,
        _revision: u64,
        reason: MemberChange,
        actions: &mut Vec<Action>,
    ) {
        let epoch = self.convergences.ledgers.get(run).map(|l| l.work_epoch);
        if let Some(join) = self
            .runs
            .runs
            .get_mut(run)
            .and_then(|r| r.subagent_join.as_mut())
        {
            if let Some(epoch) = epoch {
                join.work_epoch = epoch;
            }
            self.runs.put(run, actions);
            self.notify_convergence_changed(run, actions);
        }
        if !matches!(reason, MemberChange::ConvergencePhase) {
            self.convergence_drive(run, actions);
        }
    }

    pub fn on_run_finishing(&mut self, run: &str, kind: ReleaseKind, actions: &mut Vec<Action>) {
        if !self.convergences.runs.contains_key(run) {
            self.convergences.ledgers.remove(run);
            return;
        }
        if kind == ReleaseKind::Shutdown || self.closing || self.quiescing {
            self.convergence_revoke(run, ReconcileCancel::Shutdown, actions);
        }
        // 这里只记录结果，绝不补写 delivered/reconciled 或伪造汇总成功。
        let phase = if self
            .runs
            .runs
            .get(run)
            .and_then(|r| r.subagent_join.as_ref())
            .is_some_and(|j| j.terminal_reason.is_some())
        {
            JoinPhase::Abnormal
        } else {
            JoinPhase::Released
        };
        self.convergence_phase(run, phase, actions);
        if let Some(runtime) = self.convergences.runs.remove(run) {
            actions.push(Action::Unschedule {
                key: TimerKey::ConvergeDeadline(run.into(), runtime.token.generation),
            });
            actions.push(Action::Unschedule {
                key: TimerKey::ConvergeRetry(run.into(), runtime.token.generation),
            });
        }
        // 普通 run_released 紧接此钩子计算终态，停止前快照由 step 末尾再回收。
        if self.tree_for_run(run).is_none() {
            self.convergences.ledgers.remove(run);
        }
    }

    fn convergence_revoke(
        &mut self,
        run: &str,
        reason: ReconcileCancel,
        actions: &mut Vec<Action>,
    ) {
        if let Some(progress) = self.convergences.progress.get_mut(run) {
            if !progress.terminal_locked {
                progress.terminal_locked = true;
                if let Some(record) = self.runs.runs.get(run) {
                    progress.parent_before_stop = record
                        .current_turn_id
                        .as_ref()
                        .and_then(|id| record.turns.iter().find(|t| &t.turn_id == id))
                        .map(|t| t.status);
                    progress.interrupt_before_stop = match reason {
                        ReconcileCancel::Stop => Some(InterruptReason::Stop),
                        ReconcileCancel::OwnerGone => Some(InterruptReason::ClientDisconnected),
                        _ => record.interrupt_reason,
                    };
                }
            }
        }
        let Some(runtime) = self.convergences.runs.get_mut(run) else {
            return;
        };
        // 屏障先于 B 的取消及任何停止 RPC；自动权限永远不能由后续事件恢复。
        runtime.automatic_control_revoked = true;
        let token = runtime.token.clone();
        if let Some(join) = self
            .runs
            .runs
            .get_mut(run)
            .and_then(|r| r.subagent_join.as_mut())
        {
            join.automatic_control_revoked = true;
            let terminal = match reason {
                ReconcileCancel::Stop => Some(TerminalReason::UserStop),
                ReconcileCancel::OwnerGone => Some(TerminalReason::OwnerGone),
                ReconcileCancel::Shutdown => Some(TerminalReason::BrokerShutdown),
                ReconcileCancel::Abandon => Some(TerminalReason::Abandoned),
                ReconcileCancel::AuthorityChanged | ReconcileCancel::UserInput => {
                    Some(TerminalReason::OwnershipConflict)
                }
                _ => None,
            };
            if join.terminal_reason.is_none()
                || matches!(reason, ReconcileCancel::Abandon | ReconcileCancel::Shutdown)
            {
                join.terminal_reason = terminal;
            }
        }
        self.cancel_subagent_reconcile(&token, reason, actions);
        self.convergence_phase(run, JoinPhase::Stopping, actions);
        self.convergence_changed(run, actions);
    }

    pub fn convergence_cancel_for_thread(
        &mut self,
        thread: &str,
        reason: ReconcileCancel,
        actions: &mut Vec<Action>,
    ) {
        if let Some(run) = self.claims.get(thread).and_then(|c| c.run_id.clone()) {
            self.convergence_revoke(&run, reason, actions);
            if matches!(
                reason,
                ReconcileCancel::UserInput | ReconcileCancel::AuthorityChanged
            ) {
                self.convergence_fail(&run, TerminalReason::OwnershipConflict, actions);
            }
        }
    }

    pub(crate) fn convergence_authority_lost(&mut self, run: &str, actions: &mut Vec<Action>) {
        // 外部接管只撤销资格，不借旧认领发停止请求。
        self.convergence_revoke(run, ReconcileCancel::AuthorityChanged, actions);
    }

    pub fn on_convergence_owner_disconnect(&mut self, conn: ConnId, actions: &mut Vec<Action>) {
        let runs: Vec<_> = self
            .claims
            .values()
            .filter(|c| c.owner() == Some(&Owner::Client(conn)))
            .filter_map(|c| c.run_id.clone())
            .collect();
        for run in runs {
            self.convergence_revoke(&run, ReconcileCancel::OwnerGone, actions);
        }
    }

    pub fn convergence_shutdown(&mut self, actions: &mut Vec<Action>) {
        self.convergences.enabled = false;
        let runs: Vec<_> = self.convergences.runs.keys().cloned().collect();
        for run in runs {
            self.convergence_revoke(&run, ReconcileCancel::Shutdown, actions);
            if let Some(record) = self.runs.runs.get_mut(&run) {
                record
                    .interrupt_reason
                    .get_or_insert(InterruptReason::BrokerLost);
            }
            self.runs.put(&run, actions);
        }
    }

    pub fn convergence_terminal(&self, input: &TerminalInput) -> Option<TerminalDisposition> {
        terminal::terminal(input)
    }

    pub fn convergence_run_terminal(
        &self,
        run: &str,
        parent_status: Option<TurnStatus>,
    ) -> Option<TerminalDisposition> {
        let record = self.runs.runs.get(run)?;
        let join = record.subagent_join.as_ref()?;
        let parent_status = record
            .current_turn_id
            .as_ref()
            .and_then(|id| record.turns.iter().find(|t| &t.turn_id == id))
            .map(|t| t.status)
            .or(parent_status);
        // 中断由本次 fx-stop 产生时，只证明停止成功；不能改写启动停止之前已锁定的限制原因。
        let progress = self
            .convergences
            .progress
            .get(run)
            .filter(|p| p.terminal_locked);
        let parent_status = if parent_status == Some(TurnStatus::Interrupted)
            && progress.is_some_and(|p| p.parent_before_stop != Some(TurnStatus::Interrupted))
            && join.terminal_reason != Some(TerminalReason::ParentInterrupted)
        {
            None
        } else {
            parent_status
        };
        let existing_interrupt =
            progress.map_or(record.interrupt_reason, |p| p.interrupt_before_stop);
        let reason = match parent_status {
            Some(TurnStatus::Failed) => TerminalReason::ParentFailed,
            Some(TurnStatus::Interrupted) => TerminalReason::ParentInterrupted,
            _ => join.terminal_reason?,
        };
        self.convergence_terminal(&TerminalInput {
            reason,
            parent_status,
            existing_interrupt,
        })
    }

    /// 历史摘要只用于展示；重启不重建运行时、操作或可信输入。
    pub(crate) fn convergence_restored(
        &mut self,
        previous: &[(String, Option<InterruptReason>)],
        actions: &mut Vec<Action>,
    ) {
        for (run, interrupt) in previous {
            let Some(record) = self.runs.runs.get_mut(run) else {
                continue;
            };
            if record.state != crate::runs::RunState::Lost {
                continue;
            }
            let Some(join) = record.subagent_join.as_mut() else {
                continue;
            };
            join.automatic_control_revoked = true;
            join.phase = JoinPhase::Abnormal;
            join.terminal_reason = Some(TerminalReason::BrokerShutdown);
            record.interrupt_reason = interrupt.or(Some(InterruptReason::BrokerLost));
            self.runs.put(run, actions);
        }
    }

    pub(crate) fn drive_convergences(&mut self, actions: &mut Vec<Action>) {
        let runs: Vec<_> = self.convergences.runs.keys().cloned().collect();
        for run in runs {
            if self.tree_abandoned(&run) {
                if let Some(thread) = self
                    .convergences
                    .runs
                    .get(&run)
                    .map(|r| r.token.claim.thread_id.clone())
                {
                    self.abandoned_root_goal_settled(&thread, actions);
                }
            } else {
                self.convergence_drive(&run, actions);
            }
        }
    }

    pub(crate) fn convergence_prune(&mut self) {
        self.convergences.ledgers.retain(|run, _| {
            self.relations.contexts.contains_key(run) || self.convergences.runs.contains_key(run)
        });
        self.convergences.progress.retain(|run, p| {
            self.convergences.runs.contains_key(run)
                || p.operations.values().any(|o| {
                    o.outcome_unknown || o.submission == ReconcileSubmission::OutcomeUnknown
                })
        });
        self.convergences.observations.retain(|(thread, turn), _| {
            self.relations
                .aux
                .observed
                .get(thread)
                .is_some_and(|list| list.iter().any(|o| &o.turn == turn))
                || self.convergences.ledgers.values().any(|l| {
                    l.activities.iter().any(|a| {
                        &a.key.thread_id == thread
                            && a.key.turn_id.as_deref().unwrap_or_default() == turn
                    })
                })
        });
        self.convergences
            .capabilities
            .retain(|thread, _| self.claims.contains_key(thread));
        self.convergences.goals.retain(|thread, _| {
            self.records.contains_key(thread)
                || self
                    .relations
                    .contexts
                    .values()
                    .any(|c| c.members.contains_key(thread))
        });
    }

    /// B 在登记、提交及核实变更时同步原操作；取消不能清掉 outcome_unknown。
    pub fn convergence_operation_changed(&mut self, operation: ReconcileOperation) {
        let Some(progress) = self.convergences.progress.get_mut(&operation.token.run_id) else {
            return;
        };
        if let Some(old) = progress.operations.get(&operation.op_id.0) {
            if old.token != operation.token
                || old.client_user_message_id != operation.client_user_message_id
            {
                return;
            }
        } else if self
            .convergences
            .runs
            .get(&operation.token.run_id)
            .is_none_or(|r| r.token != operation.token)
        {
            return;
        }
        progress.operations.insert(operation.op_id.0, operation);
    }

    pub(crate) fn convergence_root_unsettled(&self, run: &str) -> bool {
        self.convergences.progress.get(run).is_some_and(|p| {
            p.operations
                .values()
                .any(|o| o.outcome_unknown || o.submission == ReconcileSubmission::OutcomeUnknown)
        })
    }
}
