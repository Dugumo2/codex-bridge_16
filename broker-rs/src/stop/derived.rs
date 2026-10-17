//! SA-b 派生停止；不冒充父认领，不直接改父运行终态（接口第 8.1 节）。
//!
//! 以 `(tree_op, thread)` 为键，由整树停止的协调器发起、分配名额并汇总：
//! - 先读 goal：v1（能力为 true）按现有规则制动 active goal，`--clear` 时清除；v2（能力为 false）
//!   不发 goal 写入，读到仍 active（或要求清除而目标仍在）时标「能力限制」；
//! - 再用读取层的 Activity 查询核实（`StatusCaller::DerivedStopVerify`）：
//!   `Idle` 成立；`Active(ids)` 只中断已绑定到本次范围的轮次，未绑定的记冲突；
//!   `Active([])` 15 秒后再核实；`Unknown` 退避后再核实；
//! - 每次中断前重新核对绑定与授权；中断发出、等待确认时让出名额；
//! - 子线程的新轮次经 `on_unclaimed_turn` → `on_member_changed` 纳入同一次停止。
//!
//! 与 H2-b 的契约：只调用读取层，不改 `stop/mod.rs` 里单线程停止的核实逻辑。
use super::tree::{RootStopReport, TreePhase, TreeTrigger, TREE_VERIFY_MS};
use crate::claim::status_read::{StatusCaller, StatusDone, StatusQuery, StatusResult};
use crate::claim::{Action, Machine, Operation, RequestContext, State};
use crate::relations::{AgentVersion, Authority, BindingStatus};
use crate::runs::{StopCheck, StopDiagSend};
use serde_json::{json, Value};
use std::collections::BTreeSet;

/// 核实结果未知时的退避（毫秒），最后一档与 15 秒周期相同。
pub const DERIVED_BACKOFF_MS: [u64; 4] = [1_000, 2_000, 4_000, TREE_VERIFY_MS];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedTarget {
    pub thread: String,
    pub run_id: String,
    pub proven_turns: BTreeSet<String>,
    pub capability: Option<bool>,
    pub agent_version: AgentVersion,
    pub tree_op: u64,
    pub generation: u64,
    pub round: u64,
    pub authority_gen: u64,
    pub reason: TreeTrigger,
    pub clear: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedStopDone {
    pub tree_op: u64,
    pub run_id: String,
    pub thread: String,
    pub generation: u64,
    pub round: u64,
    pub report: RootStopReport,
    pub conflicts: Vec<String>,
    pub capability_limited: bool,
    pub cancelled: bool,
}

/// 派生停止所处的步骤。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivedStage {
    /// 排队等名额（协调器按 8 个在途名额放行）。
    Queued,
    Background,
    /// 已发 thread/goal/get。
    GoalRead,
    /// 已发 goal 暂停或清除。
    GoalWrite,
    /// 读取层 Activity 查询在途。
    Verify,
    /// 中断已发出、等回包（仍占名额）。
    Interrupting,
    /// 不占名额：等确认、等重试或已成立。
    Waiting,
}

/// 一个派生停止的内部状态（SA-b 内部登记，不属于冻结契约）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedOp {
    pub unloaded_read: bool,
    pub background: super::background::Background,
    pub last_verified_at: Option<u64>,
    pub connection_epoch: u64,
    pub target: DerivedTarget,
    pub stage: DerivedStage,
    /// 当前在途请求（goal 读写、中断、状态读取）的轮次；回包轮次不符一律丢弃。
    pub request_round: u64,
    pub read_id: Option<u64>,
    /// 占用一个在途名额。
    pub slot: bool,
    /// 本周期 goal 步骤已完成。
    pub goal_checked: bool,
    pub goal_braked: bool,
    pub goal_report: String,
    pub clear_done: bool,
    pub capability_limited: bool,
    pub interrupted: BTreeSet<String>,
    pub interrupts_pending: usize,
    /// 活动但不属于本次范围（未绑定、属于别的运行或已撤权）的轮次与原因。
    pub conflicts: BTreeSet<String>,
    pub turn_ended: bool,
    pub verified: bool,
    pub retry_at_ms: Option<u64>,
    pub backoff: usize,
    pub stopped: bool,
    pub error: Option<String>,
    /// 根线程（无认领或 FinishingTree）的停止也走这里，结果交 `tree_root_done`。
    pub root: bool,
    /// 上次看到的成员活动；变化时立即再核实。
    pub seen_activity: Option<crate::relations::MemberActivity>,
}

impl DerivedOp {
    pub fn new(target: DerivedTarget, root: bool) -> Self {
        Self {
            unloaded_read: false,
            background: Default::default(),
            last_verified_at: None,
            connection_epoch: 0,
            target,
            stage: DerivedStage::Queued,
            request_round: 0,
            read_id: None,
            slot: false,
            goal_checked: false,
            goal_braked: false,
            goal_report: "unchanged".into(),
            clear_done: false,
            capability_limited: false,
            interrupted: BTreeSet::new(),
            interrupts_pending: 0,
            conflicts: BTreeSet::new(),
            turn_ended: false,
            verified: false,
            retry_at_ms: None,
            backoff: 0,
            stopped: false,
            error: None,
            root,
            seen_activity: None,
        }
    }
    /// goal 条件成立：已制动、没有能力限制、要求清除时已清除。
    pub fn goal_ok(&self) -> bool {
        self.goal_braked && !self.capability_limited && (!self.target.clear || self.clear_done)
    }
    pub fn check(&self) -> StopCheck {
        StopCheck {
            ownership: self.conflicts.is_empty(),
            turn_ended: self.turn_ended,
            goal_braked: self.goal_ok(),
            settled: !self.slot,
            verify_done: self.verified && !self.slot && self.background.confirmed,
        }
    }
    /// 能写 goal：能力为 true，或能力未知但派生事件判定为 v1。
    pub fn writable(&self) -> bool {
        self.target.capability == Some(true)
            || (self.target.capability.is_none() && self.target.agent_version == AgentVersion::V1)
            || self.root
    }
}

impl Machine {
    pub(crate) fn derived_diagnostic_send(
        &self,
        key: &(u64, String),
        turn: Option<&str>,
    ) -> Option<(String, StopDiagSend)> {
        let op = self.trees.aux.derived.get(key)?;
        let tree = self.trees.operations.get(&key.0)?;
        let work = self.trees.aux.work.get(&key.0)?;
        if !op.root
            || work.historical_unloaded
            || tree.cancelled
            || tree.generation != op.target.generation
            || work.root_thread != op.target.thread
        {
            return None;
        }
        let run = self.runs.runs.get(&op.target.run_id)?;
        if run.joined
            || matches!(run.state, crate::runs::RunState::Lost)
            || run.state == crate::runs::RunState::Ended && run.stop_diagnostics.is_none()
        {
            return None;
        }
        Some((
            op.target.run_id.clone(),
            StopDiagSend {
                stop_generation: work.root_diag_generation?,
                thread: op.target.thread.clone(),
                turn: turn
                    .map(str::to_owned)
                    .or_else(|| {
                        self.runs
                            .stop_diag_sends
                            .get(&op.target.run_id)
                            .filter(|send| {
                                Some(send.stop_generation) == work.root_diag_generation
                                    && send.thread == op.target.thread
                            })
                            .and_then(|send| send.turn.clone())
                    })
                    .or_else(|| tree.request.snapshot.parent_turn_id.clone()),
            },
        ))
    }

    /// 登记并排队一个派生停止；同键已存在时只更新清除要求与已证明轮次并重新排队核实。
    pub fn begin_derived_stop(&mut self, target: DerivedTarget, actions: &mut Vec<Action>) {
        let key = (target.tree_op, target.thread.clone());
        let root = self
            .trees
            .aux
            .work
            .get(&target.tree_op)
            .is_some_and(|w| w.root_thread == target.thread && w.root_derived);
        match self.trees.aux.derived.get_mut(&key) {
            Some(op) => {
                if target.clear && !op.target.clear {
                    op.target.clear = true;
                    op.clear_done = false;
                    op.goal_checked = false;
                }
                op.target.proven_turns.extend(target.proven_turns);
                op.target.authority_gen = target.authority_gen;
            }
            None => {
                self.trees
                    .aux
                    .derived
                    .insert(key.clone(), DerivedOp::new(target, root));
            }
        }
        self.derived_requeue(&key);
        self.tree_pump(key.0, actions);
    }

    /// 派生停止的阶段结果交给树协调器；只接受当前操作与代号，不直接改父运行。
    pub fn derived_stop_done(&mut self, done: DerivedStopDone, actions: &mut Vec<Action>) {
        let Some(op) = self.trees.operations.get(&done.tree_op) else {
            return;
        };
        if op.generation != done.generation || op.cancelled {
            return;
        }
        if let Some(work) = self.trees.aux.work.get_mut(&done.tree_op) {
            if done.report.phase == TreePhase::Confirmed {
                work.final_pending.remove(&done.thread);
            } else if work.final_pending.contains(&done.thread) {
                // 收尾轮中成员重新不成立：等再次落定后重做收尾轮。
                work.final_started = false;
            }
            work.results.insert(done.thread.clone(), done);
        }
        self.tree_drive(op.tree_op, actions);
    }

    /// 读取层 Activity 查询的完成回调（`StatusCaller::DerivedStopVerify`）。
    pub fn derived_stop_verify_done(&mut self, done: StatusDone, actions: &mut Vec<Action>) {
        let StatusCaller::DerivedStopVerify {
            tree_op,
            thread,
            round,
        } = &done.caller
        else {
            return;
        };
        let key = (*tree_op, thread.clone());
        let authority = self.derived_background_authority(&key);
        let closed = self.closed_runtime(thread);
        let Some(op) = self.trees.aux.derived.get_mut(&key) else {
            return;
        };
        if op.background.verifying
            && (!authority || op.connection_epoch != self.relations.conn_epoch)
        {
            op.background
                .fail("conflict", "后置核实期间授权或连接代际变化".into());
        }
        if op.stage != DerivedStage::Verify
            || op.request_round != *round
            || op.read_id != Some(done.read_id)
        {
            return;
        }
        if matches!(done.result, StatusResult::Idle) {
            if let Some((run, send)) = self.derived_diagnostic_send(&key, None) {
                self.stop_diag_turn_ended(&run, &send);
            }
        }
        let op = self.trees.aux.derived.get_mut(&key).expect("派生停止存在");
        let historical = self
            .trees
            .aux
            .work
            .get(tree_op)
            .is_some_and(|w| w.historical_unloaded);
        let was_probe = historical && !op.goal_checked;
        op.read_id = None;
        op.last_verified_at = Some(self.clock.now_ms);
        op.verified = true;
        let now = self.clock.mono_ms;
        match done.result {
            StatusResult::Idle => {
                op.turn_ended = true;
                op.conflicts.clear();
                op.backoff = 0;
                op.error = None;
                if op.unloaded_read && (historical || op.goal_ok()) {
                    if let Some(proof) = closed {
                        op.background.confirm_closed(proof, self.clock.now_ms);
                        if historical {
                            op.goal_braked = true;
                            op.capability_limited = false;
                        }
                        op.stopped = !op.target.clear || op.clear_done;
                        self.derived_release_slot(&key);
                        self.derived_report(&key, actions);
                        return;
                    }
                }
                if was_probe {
                    self.derived_goal_read(&key, actions);
                    return;
                }
                if op.goal_ok() && !op.background.verifying && !op.background.failed {
                    self.derived_background_next(&key, actions);
                    return;
                }
                op.stopped = op.goal_ok()
                    && op.background.verifying
                    && self.clock.mono_ms < op.background.deadline;
                if op.stopped {
                    op.background.finish();
                }
                if !op.stopped {
                    // goal 未制动或能力限制：15 秒后连同 goal 一起再核实。
                    op.goal_checked = false;
                    op.retry_at_ms = Some(now + TREE_VERIFY_MS);
                }
                self.derived_release_slot(&key);
            }
            StatusResult::Active(ids) if !ids.is_empty() => {
                op.turn_ended = false;
                op.stopped = false;
                op.backoff = 0;
                op.conflicts.clear();
                let ids: Vec<String> = ids;
                let mut sent = 0;
                for turn in ids {
                    match self.derived_authorized(&key, &turn) {
                        Ok(()) => {
                            self.derived_interrupt(&key, &turn, actions);
                            sent += 1;
                        }
                        Err(reason) => {
                            let op = self.trees.aux.derived.get_mut(&key).expect("派生停止存在");
                            op.conflicts.insert(format!("{turn}：{reason}"));
                        }
                    }
                }
                let op = self.trees.aux.derived.get_mut(&key).expect("派生停止存在");
                op.retry_at_ms = Some(now + TREE_VERIFY_MS);
                if sent > 0 {
                    op.stage = DerivedStage::Interrupting;
                    op.interrupts_pending = sent;
                } else {
                    self.derived_release_slot(&key);
                }
            }
            StatusResult::Active(_) => {
                // 线程活动但存储页里还没有活动轮次：不成立，15 秒后再核实。
                op.turn_ended = false;
                op.stopped = false;
                op.retry_at_ms = Some(now + TREE_VERIFY_MS);
                self.derived_release_slot(&key);
            }
            StatusResult::Unknown(reason) if was_probe => {
                op.background.fail(
                    "unknown",
                    format!("历史成员当前运行时无法核验：{reason:?}；本次核验已释放"),
                );
                op.error = op.background.report.failure_reason.clone();
                self.derived_release_slot(&key);
                self.tree_release_unloaded_history(key.0, actions);
                return;
            }
            StatusResult::Unknown(reason) => {
                op.turn_ended = false;
                op.stopped = false;
                op.error = Some(format!("核实结果未知：{reason:?}"));
                let delay = DERIVED_BACKOFF_MS[op.backoff.min(DERIVED_BACKOFF_MS.len() - 1)];
                op.backoff += 1;
                op.retry_at_ms = Some(now + delay);
                self.derived_release_slot(&key);
            }
            other => {
                op.error = Some(format!("核实结果不适用：{other:?}"));
                op.retry_at_ms = Some(now + TREE_VERIFY_MS);
                self.derived_release_slot(&key);
            }
        }
        self.derived_report(&key, actions);
        self.tree_pump(key.0, actions);
    }
}

impl Machine {
    /// 名额放行后开始一个周期：先 goal，再核实。
    pub(crate) fn derived_start(&mut self, key: &(u64, String), actions: &mut Vec<Action>) {
        let Some(op) = self.trees.aux.derived.get_mut(key) else {
            return;
        };
        op.slot = true;
        op.target.authority_gen = self
            .relations
            .contexts
            .get(&op.target.run_id)
            .map_or(0, |c| c.gen);
        op.background.invalidate();
        op.background.deadline = self
            .trees
            .operations
            .get(&key.0)
            .map(|t| t.request.first_delivery_deadline_ms)
            .filter(|d| *d > self.clock.mono_ms)
            .unwrap_or(self.clock.mono_ms + TREE_VERIFY_MS);
        op.connection_epoch = self.relations.conn_epoch;
        op.goal_checked = false;
        op.retry_at_ms = None;
        if self
            .trees
            .aux
            .work
            .get(&key.0)
            .is_some_and(|w| w.historical_unloaded)
        {
            self.derived_verify(key, actions);
        } else {
            self.derived_goal_read(key, actions);
        }
    }

    fn derived_goal_read(&mut self, key: &(u64, String), actions: &mut Vec<Action>) {
        let op = self.trees.aux.derived.get_mut(key).expect("派生停止存在");
        op.stage = DerivedStage::GoalRead;
        op.request_round += 1;
        let context = derived_request(op, "thread/goal/get", json!({"threadId": key.1}));
        actions.push(Action::Request(context));
        if let Some((run, send)) = self.derived_diagnostic_send(key, None) {
            self.stop_diag_goal_started(&run, &send);
        }
    }

    fn derived_verify(&mut self, key: &(u64, String), actions: &mut Vec<Action>) {
        if self
            .trees
            .aux
            .derived
            .get(key)
            .is_some_and(|op| op.goal_checked)
            && !self.stops.changes.values().any(|c| c.thread == key.1)
        {
            if let Some((run, send)) = self.derived_diagnostic_send(key, None) {
                self.stop_diag_goal_settled(&run, &send);
            }
        }
        let op = self.trees.aux.derived.get_mut(key).expect("派生停止存在");
        op.unloaded_read = false;
        op.stage = DerivedStage::Verify;
        op.request_round += 1;
        let caller = StatusCaller::DerivedStopVerify {
            tree_op: key.0,
            thread: key.1.clone(),
            round: op.request_round,
        };
        let read_id = self.begin_status_read(&key.1, None, StatusQuery::Activity, caller, actions);
        if let Some(op) = self.trees.aux.derived.get_mut(key) {
            op.read_id = Some(read_id);
        }
    }

    fn derived_interrupt(&mut self, key: &(u64, String), turn: &str, actions: &mut Vec<Action>) {
        let op = self.trees.aux.derived.get_mut(key).expect("派生停止存在");
        op.interrupted.insert(turn.into());
        let context = derived_request(
            op,
            "turn/interrupt",
            json!({"threadId": key.1, "turnId": turn}),
        );
        actions.push(Action::Request(context));
        if let Some((run, send)) = self.derived_diagnostic_send(key, Some(turn)) {
            self.stop_diag_interrupt_sent(&run, &send);
        }
    }

    /// 中断前核对：根线程只中断本次结束快照里证明过的轮次；子线程的轮次必须仍绑定到本次范围内的运行，
    /// 且该运行没有撤权。
    pub(crate) fn derived_authorized(&self, key: &(u64, String), turn: &str) -> Result<(), String> {
        let op = self.trees.aux.derived.get(key).ok_or("派生停止已结束")?;
        let work = self.trees.aux.work.get(&key.0).ok_or("树停止已结束")?;
        if op.root {
            let held = self.claims.get(&key.1).is_some_and(|c| {
                c.run_id.as_deref() == Some(op.target.run_id.as_str())
                    && matches!(c.state, State::FinishingTree { .. })
            });
            if !held {
                return Err("根线程的结束快照已不在".into());
            }
            if !op.target.proven_turns.contains(turn) {
                return Err("未证明属于本次运行".into());
            }
            return Ok(());
        }
        let Some(binding) = self.binding(&key.1, turn) else {
            return Err("未绑定".into());
        };
        match &binding.status {
            BindingStatus::Bound => {}
            BindingStatus::Unbound => return Err("未绑定".into()),
            BindingStatus::Conflict(reason) => return Err(format!("冲突：{reason}")),
        }
        if !work.runs.contains(&binding.run_id) {
            return Err(format!("属于其他运行 {}", binding.run_id));
        }
        match self.manage_context(&binding.run_id) {
            Some(context) if context.authority != Authority::Revoked => Ok(()),
            _ => Err("运行已撤权".into()),
        }
    }

    pub(crate) fn derived_invalidate_background(
        &mut self,
        key: &(u64, String),
        actions: &mut Vec<Action>,
    ) {
        let Some(op) = self.trees.aux.derived.get_mut(key) else {
            return;
        };
        let pending = op.background.busy || op.background.verifying;
        op.background.invalidate();
        if pending {
            op.request_round += 1;
            let read = op.read_id.take();
            if let Some(read) = read {
                self.cancel_status_read(read, actions);
            }
            self.derived_release_slot(key);
        }
    }

    pub(crate) fn derived_release_slot(&mut self, key: &(u64, String)) {
        let Some(op) = self.trees.aux.derived.get_mut(key) else {
            return;
        };
        op.stage = DerivedStage::Waiting;
        op.interrupts_pending = 0;
        if op.slot {
            op.slot = false;
            if let Some(tree) = self.trees.operations.get_mut(&key.0) {
                tree.in_flight = tree.in_flight.saturating_sub(1);
            }
        }
    }

    /// 重新排队一次核实（已在队列或占名额时不重复）。
    pub(crate) fn derived_requeue(&mut self, key: &(u64, String)) {
        let Some(op) = self.trees.aux.derived.get_mut(key) else {
            return;
        };
        if op.slot {
            return;
        }
        let Some(work) = self.trees.aux.work.get_mut(&key.0) else {
            return;
        };
        op.retry_at_ms = None;
        if work.queue.contains(&key.1) {
            return;
        }
        op.stage = DerivedStage::Queued;
        if op.root {
            work.queue.push_front(key.1.clone());
        } else {
            work.queue.push_back(key.1.clone());
        }
    }

    /// `Operation::DerivedStop` 的回包。
    pub(crate) fn derived_reply(
        &mut self,
        context: &RequestContext,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        let Operation::DerivedStop {
            tree_op,
            generation,
            thread,
            round,
        } = &context.operation
        else {
            return;
        };
        let key = (*tree_op, thread.clone());
        let Some(op) = self.trees.aux.derived.get(&key) else {
            return;
        };
        if op.target.generation != *generation {
            return;
        }
        if context.method.starts_with("thread/backgroundTerminals/") {
            if op.request_round != *round
                || op.stage != DerivedStage::Background
                || !op.background.busy
            {
                return;
            }
            let narrow = matches!(
                self.trees.operations[tree_op].request.scope,
                super::tree::TreeScope::Run { .. }
            );
            let runs = self.trees.aux.work[tree_op].runs.clone();
            let allowed: BTreeSet<String> = self
                .stops
                .command_turns
                .keys()
                .filter(|(t, item)| t == thread && self.background_item_owned(thread, item, &runs))
                .map(|(_, item)| item.clone())
                .collect();
            let authority = self.derived_background_authority(&key);
            let op = self.trees.aux.derived.get_mut(&key).expect("派生停止存在");
            if !authority
                || op.connection_epoch != self.relations.conn_epoch
                || self.clock.mono_ms >= op.background.deadline
            {
                op.background
                    .fail("unknown", "所有权或连接代际变化，清理证据失效".into());
            } else {
                op.background.reply(result, self.clock.now_ms, |item| {
                    !narrow || allowed.contains(item)
                });
            }
            if op.background.report.state == "notLoaded"
                && (op.goal_ok()
                    || self
                        .trees
                        .aux
                        .work
                        .get(tree_op)
                        .is_some_and(|w| w.historical_unloaded))
            {
                let proof = self.closed_runtime(thread);
                let op = self.trees.aux.derived.get_mut(&key).expect("派生停止存在");
                if let Some(proof) = proof {
                    op.background.confirm_closed(proof, self.clock.now_ms);
                    op.goal_braked = true;
                    op.stopped = !op.target.clear || op.clear_done;
                } else {
                    op.error = op.background.report.failure_reason.clone();
                    op.stopped = false;
                    op.retry_at_ms = Some(self.clock.mono_ms + TREE_VERIFY_MS);
                }
                self.derived_release_slot(&key);
                self.derived_report(&key, actions);
                return;
            }
            if op.background.verifying {
                // 空列表之后重新读取 goal 和活动轮次，仍保留当前总期限。
                self.derived_goal_read(&key, actions);
            } else {
                self.derived_background_next(&key, actions);
            }
            return;
        }
        if context.method == "turn/interrupt" {
            if op.stage != DerivedStage::Interrupting || op.request_round != *round {
                return;
            }
            if let Err(error) = &result {
                actions.push(Action::Warn(format!(
                    "子线程 {thread} 的中断请求失败：{error}"
                )));
            }
            let op = self.trees.aux.derived.get_mut(&key).expect("派生停止存在");
            op.interrupts_pending = op.interrupts_pending.saturating_sub(1);
            if op.interrupts_pending == 0 {
                // 中断已全部回包：让出名额，等完成通知或 15 秒后再核实。
                self.derived_release_slot(&key);
                self.derived_report(&key, actions);
                self.tree_pump(key.0, actions);
            }
            return;
        }
        if op.request_round != *round {
            return;
        }
        match (op.stage, context.method.as_str()) {
            (DerivedStage::GoalRead, "thread/goal/get") => {
                self.derived_goal_read_reply(&key, result, actions)
            }
            (DerivedStage::GoalWrite, "thread/goal/set" | "thread/goal/clear") => {
                let clear = context.method == "thread/goal/clear";
                let op = self.trees.aux.derived.get_mut(&key).expect("派生停止存在");
                match result {
                    Ok(_) => {
                        op.goal_braked = true;
                        op.goal_report = if clear { "cleared" } else { "paused" }.into();
                        if clear {
                            op.clear_done = true;
                        }
                    }
                    Err(error) => {
                        op.goal_braked = false;
                        op.error = Some(format!("goal 制动失败：{error}"));
                        actions.push(Action::Warn(format!(
                            "子线程 {thread} 的 goal 制动失败，继续中断与核实"
                        )));
                    }
                }
                op.goal_checked = true;
                self.derived_verify(&key, actions);
            }
            _ => {}
        }
    }

    fn derived_goal_read_reply(
        &mut self,
        key: &(u64, String),
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        // 回包学习事实（learn_traffic）先于这里推进了 goal 版本，不能用版本号判断旧读数；
        // 读取期间本线程还有在途的 goal 写入时，读数可能已过时，重读。
        let writing = self.stops.changes.values().any(|c| c.thread == key.1);
        let authority_ok = self.derived_authority_ok(key);
        let op = self.trees.aux.derived.get_mut(key).expect("派生停止存在");
        let goal = match result {
            Ok(value) => value.get("goal").cloned().unwrap_or(Value::Null),
            Err(error) => {
                op.goal_braked = false;
                op.error = Some(format!("goal 读取失败：{error}"));
                op.goal_checked = true;
                self.derived_verify(key, actions);
                return;
            }
        };
        if writing {
            self.derived_goal_read(key, actions);
            return;
        }
        if goal.is_null() {
            op.goal_braked = true;
            op.goal_report = "none".into();
            op.clear_done = true;
            op.capability_limited = false;
            op.goal_checked = true;
            self.derived_verify(key, actions);
            return;
        }
        let active = goal.get("status").and_then(Value::as_str) == Some("active");
        let clear = op.target.clear;
        if !active && !clear {
            op.goal_braked = true;
            op.capability_limited = false;
            op.goal_checked = true;
            self.derived_verify(key, actions);
            return;
        }
        if !op.writable() {
            // v2 子线程不接受 goal 写入：读到仍 active（或要求清除而目标仍在）只能如实标能力限制。
            op.capability_limited = true;
            op.goal_braked = !active;
            if clear {
                op.goal_report = "notCleared".into();
            }
            op.goal_checked = true;
            self.derived_verify(key, actions);
            return;
        }
        if !authority_ok {
            op.conflicts.insert("goal：运行已撤权，不制动".into());
            op.goal_braked = !active;
            op.goal_checked = true;
            self.derived_verify(key, actions);
            return;
        }
        op.stage = DerivedStage::GoalWrite;
        op.request_round += 1;
        let (method, params) = if clear {
            ("thread/goal/clear", json!({"threadId": key.1}))
        } else {
            (
                "thread/goal/set",
                json!({"threadId": key.1, "status": "paused"}),
            )
        };
        let context = derived_request(op, method, params);
        actions.push(Action::Request(context));
    }

    fn derived_background_authority(&self, key: &(u64, String)) -> bool {
        let Some(op) = self.trees.aux.derived.get(key) else {
            return false;
        };
        self.trees
            .operations
            .get(&key.0)
            .is_some_and(|t| !t.cancelled)
            && self.claims.get(&key.1).is_none_or(|c| {
                c.run_id.as_deref() == Some(&op.target.run_id) && !c.joined() && !c.settling()
            })
            && self.manage_context(&op.target.run_id).is_some_and(|c| {
                c.authority != Authority::Revoked
                    && c.gen == op.target.authority_gen
                    && c.members.get(&key.1).is_none_or(|m| m.conflict.is_none())
            })
    }

    fn derived_background_next(&mut self, key: &(u64, String), actions: &mut Vec<Action>) {
        let narrow = self
            .trees
            .operations
            .get(&key.0)
            .is_some_and(|t| matches!(t.request.scope, super::tree::TreeScope::Run { .. }));
        let item_owned = self
            .trees
            .aux
            .derived
            .get(key)
            .and_then(|d| d.background.pending_item())
            .is_none_or(|item| {
                !narrow
                    || self
                        .trees
                        .aux
                        .work
                        .get(&key.0)
                        .is_some_and(|w| self.background_item_owned(&key.1, item, &w.runs))
            });
        let authorized = self.derived_background_authority(key) && item_owned;
        let op = self.trees.aux.derived.get_mut(key).expect("派生停止存在");
        if !authorized {
            op.background.fail("conflict", "清理授权已失效".into());
        }
        if let Some((method, params)) = op.background.next(&key.1, self.clock.mono_ms) {
            op.stage = DerivedStage::Background;
            op.request_round += 1;
            actions.push(Action::Request(derived_request(op, method, params)));
        } else if op.background.failed {
            op.error = op.background.report.failure_reason.clone();
            if op.background.report.state == "conflict" {
                op.conflicts.insert(
                    op.error
                        .clone()
                        .unwrap_or_else(|| "后台命令归属冲突".into()),
                );
            }
            op.capability_limited = op.background.report.state == "capabilityLimited";
            op.retry_at_ms = Some(self.clock.mono_ms + TREE_VERIFY_MS);
            op.stopped = false;
            self.derived_release_slot(key);
            self.derived_report(key, actions);
            self.tree_pump(key.0, actions);
        }
    }

    /// 发 goal 写入前复核目标归属；父运行未撤权本身不足以授权修改另一个线程。
    fn derived_authority_ok(&self, key: &(u64, String)) -> bool {
        let Some(op) = self.trees.aux.derived.get(key) else {
            return false;
        };
        if op.root {
            return self
                .claims
                .get(&key.1)
                .is_some_and(|c| c.run_id.as_deref() == Some(op.target.run_id.as_str()));
        }
        !self.claims.contains_key(&key.1)
            && self.manage_context(&op.target.run_id).is_some_and(|c| {
                c.authority != Authority::Revoked
                    && c.gen == op.target.authority_gen
                    && c.members.get(&key.1).is_some_and(|m| m.conflict.is_none())
            })
            && !self.relations.bindings.values().any(|b| {
                b.thread_id == key.1
                    && b.status == BindingStatus::Bound
                    && b.run_id != op.target.run_id
            })
    }

    /// 把派生停止的当前结论交给协调器。
    pub(crate) fn derived_report(&mut self, key: &(u64, String), actions: &mut Vec<Action>) {
        let Some(op) = self.trees.aux.derived.get(key) else {
            return;
        };
        let phase = if op.stopped {
            TreePhase::Confirmed
        } else if self
            .trees
            .aux
            .work
            .get(&key.0)
            .is_some_and(|w| w.deadline_passed)
        {
            TreePhase::Unconfirmed
        } else {
            TreePhase::Pending
        };
        let report = RootStopReport {
            background_cleanup: op.background.report.clone(),
            step: format!("{:?}", op.stage),
            last_verified_at: op.last_verified_at,
            thread: key.1.clone(),
            phase,
            check: op.check(),
            clear_requested: op.target.clear,
            clear_confirmed: op.target.clear && op.clear_done,
            error: op.error.clone(),
        };
        if op.root {
            let (generation, round) = (op.target.generation, op.request_round);
            self.tree_root_done(key.0, generation, round, report, actions);
            return;
        }
        let done = DerivedStopDone {
            tree_op: key.0,
            run_id: op.target.run_id.clone(),
            thread: key.1.clone(),
            generation: op.target.generation,
            round: op.request_round,
            report,
            conflicts: op.conflicts.iter().cloned().collect(),
            capability_limited: op.capability_limited,
            cancelled: false,
        };
        self.derived_stop_done(done, actions);
    }
}

fn derived_request(op: &DerivedOp, method: &str, params: Value) -> RequestContext {
    RequestContext {
        resume_retried: false,
        stream_id: None,
        review_id: None,
        client: None,
        // 不属于任何认领的启动或停止登记：迟到丢弃不按 token 查找。
        token: None,
        operation: Operation::DerivedStop {
            tree_op: op.target.tree_op,
            generation: op.target.generation,
            thread: op.target.thread.clone(),
            round: op.request_round,
        },
        method: method.into(),
        params: Some(params),
    }
}
