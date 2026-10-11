//! A 路接线入口。骨架只预留调用点，不创建协调器，也不替换 SA1a 门禁。

use super::{Action, ConnId, Machine, State};
use crate::relations::MemberChange;
use crate::runs::{ReleaseKind, TurnStatus};
use crate::sa1b::{ConvergeToken, ReconcileCancel, TerminalDisposition, TerminalInput};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConvergeRuntime {
    pub token: ConvergeToken,
    pub started_mono_ms: u64,
    pub deadline_mono_ms: u64,
    pub automatic_control_revoked: bool,
    pub discovery_retry: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Convergences {
    pub runs: HashMap<String, ConvergeRuntime>,
    pub next_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConvergeEvent<'a> {
    ParentTurn { turn: &'a str, started: bool },
    GoalChanged { active: bool },
    Detach,
}

impl Machine {
    pub fn convergence_discovery_done(
        &mut self,
        _token: ConvergeToken,
        _work_epoch: u64,
        _result: crate::relations::DiscoveryResult,
        actions: &mut Vec<Action>,
    ) {
        actions.push(Action::Warn("SA1b 发现覆盖回调尚未接线".into()));
    }

    pub fn convergence_summary(&self, _run: &str) -> Option<crate::sa1b::ConvergenceSummary> {
        None
    }

    /// A 接线后发送 broker/run/convergenceChanged；只改 phase 也必须推进 revision。
    pub fn notify_convergence_changed(&self, _run: &str, actions: &mut Vec<Action>) {
        actions.push(Action::Warn("SA1b 协调摘要通知尚未接线".into()));
    }

    /// None 表示骨架未接线，调用方继续现有 SA1a 门禁；不是 Release。
    pub fn convergence_release_gate(
        &mut self,
        _thread: &str,
        _goal_status: Option<&str>,
        _turn_status: Option<TurnStatus>,
        _actions: &mut Vec<Action>,
    ) -> Option<super::finish_gate::ReleaseDecision> {
        None
    }

    /// 返回是否消费新状态事件；现有状态永远不拦截。
    pub fn convergence_event(
        &mut self,
        thread: &str,
        event: ConvergeEvent<'_>,
        actions: &mut Vec<Action>,
    ) -> bool {
        if self
            .claims
            .get(thread)
            .is_some_and(|claim| matches!(claim.state, State::Converging { .. }))
        {
            actions.push(Action::Warn(format!(
                "SA1b 收敛事件尚未接线：{thread} {event:?}"
            )));
            return true;
        }
        false
    }

    /// 统一分派；停止协调器的既有回调必须继续执行。
    pub fn on_member_changed(
        &mut self,
        run_id: &str,
        revision: u64,
        reason: MemberChange,
        actions: &mut Vec<Action>,
    ) {
        self.tree_member_changed(run_id, revision, reason, actions);
        self.convergence_member_changed(run_id, revision, reason, actions);
    }

    pub fn convergence_member_changed(
        &mut self,
        run_id: &str,
        _revision: u64,
        _reason: MemberChange,
        actions: &mut Vec<Action>,
    ) {
        if self.convergences.runs.contains_key(run_id) {
            actions.push(Action::Warn(format!("SA1b 收敛事实回调尚未接线：{run_id}")));
        }
    }

    pub fn on_run_finishing(
        &mut self,
        run_id: &str,
        _kind: ReleaseKind,
        actions: &mut Vec<Action>,
    ) {
        if self.convergences.runs.contains_key(run_id) {
            actions.push(Action::Warn(format!("SA1b 释放防御钩子尚未接线：{run_id}")));
        }
    }

    pub fn convergence_cancel_for_thread(
        &mut self,
        thread: &str,
        reason: ReconcileCancel,
        actions: &mut Vec<Action>,
    ) {
        if let Some(token) = self
            .claims
            .get(thread)
            .and_then(|c| c.run_id.as_ref())
            .and_then(|run| self.convergences.runs.get(run))
            .map(|r| r.token.clone())
        {
            self.cancel_subagent_reconcile(&token, reason, actions);
        }
    }

    pub fn on_convergence_owner_disconnect(&mut self, conn: ConnId, actions: &mut Vec<Action>) {
        let threads: Vec<String> = self
            .claims
            .iter()
            .filter(|(_, claim)| claim.owner() == Some(&super::Owner::Client(conn)))
            .map(|(thread, _)| thread.clone())
            .collect();
        for thread in threads {
            self.convergence_cancel_for_thread(&thread, ReconcileCancel::OwnerGone, actions);
        }
    }

    pub fn convergence_shutdown(&mut self, actions: &mut Vec<Action>) {
        let tokens: Vec<_> = self
            .convergences
            .runs
            .values()
            .map(|r| r.token.clone())
            .collect();
        for token in tokens {
            self.cancel_subagent_reconcile(&token, ReconcileCancel::Shutdown, actions);
        }
    }

    pub fn convergence_timer(&mut self, kind: super::TimerKind, actions: &mut Vec<Action>) {
        actions.push(Action::Warn(format!("SA1b 收敛计时尚未接线：{kind:?}")));
    }

    /// A 实施后供普通释放与树提交共用；骨架不重写现有终态。
    pub fn convergence_terminal(&self, _input: &TerminalInput) -> Option<TerminalDisposition> {
        None
    }

    /// 两种结束出口共同调用；没有 SA1b 锁定原因时保留现有语义。
    pub fn convergence_run_terminal(
        &self,
        run: &str,
        parent_status: Option<TurnStatus>,
    ) -> Option<TerminalDisposition> {
        let record = self.runs.runs.get(run)?;
        let reason = record.subagent_join.as_ref()?.terminal_reason?;
        self.convergence_terminal(&TerminalInput {
            reason,
            parent_status,
            existing_interrupt: record.interrupt_reason,
        })
    }
}
