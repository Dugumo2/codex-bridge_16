//! SA-b 树协调契约。占位不开屏障、不截获旧停止、不提交运行。
use crate::claim::{Action, ClaimToken, ClientRequest, Machine, Owner, RequestContext, TimerKind};
use crate::relations::{DiscoveryState, MemberChange, RunIdentity};
use crate::runs::{InterruptReason, RunStatus, StopCheck, TurnStatus};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const FIRST_DELIVERY_MS: u64 = 40_000;
pub const TREE_VERIFY_MS: u64 = 15_000;
pub const MAX_TREE_REQUESTS: usize = 8;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TreeTrigger {
    UserStop,
    OwnerGone,
    ParentFinishedEarly,
    LoopGuard,
    ParentFailed,
    ParentInterrupted,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TreePhase {
    Pending,
    Confirmed,
    Unconfirmed,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RootStopReport {
    pub thread: String,
    pub phase: TreePhase,
    pub check: StopCheck,
    pub clear_requested: bool,
    pub clear_confirmed: bool,
    pub error: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StopConflict {
    pub thread: String,
    pub reason: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChildrenStopReport {
    pub trigger: TreeTrigger,
    pub phase: TreePhase,
    pub version: u64,
    pub root: RootStopReport,
    pub stopped: Vec<String>,
    pub unconfirmed: Vec<String>,
    pub failed: Vec<String>,
    pub conflicts: Vec<StopConflict>,
    pub capability_limited: Vec<String>,
    pub discovery: DiscoveryState,
    pub unsettled_spawns: usize,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TreeScope {
    Run { run_id: String },
    Thread { logical_root: String },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseSnapshot {
    pub identity: RunIdentity,
    pub token: Option<ClaimToken>,
    pub owner: Option<Owner>,
    pub gen: u64,
    pub parent_turn_id: Option<String>,
    pub parent_status: Option<TurnStatus>,
    pub goal_status: Option<String>,
    pub loop_guard: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeStopWaiter {
    pub id: u64,
    pub request: ClientRequest,
    pub entered_at_ms: u64,
    pub deadline_ms: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeStopRequest {
    pub scope: TreeScope,
    pub snapshot: ReleaseSnapshot,
    pub trigger: TreeTrigger,
    pub clear: bool,
    pub root_already_idle: bool,
    pub waiter: Option<TreeStopWaiter>,
    /// 内部触发也独立计时；不得借用首个外部 waiter 或因重试延期。
    pub started_at_ms: u64,
    pub first_delivery_deadline_ms: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeStopReport {
    pub tree_op: u64,
    pub run_id: String,
    pub generation: u64,
    pub round: u64,
    pub children: ChildrenStopReport,
    pub first_delivery_deadline_ms: u64,
    pub committed: bool,
    pub cancelled: bool,
    pub barrier: bool,
}
/// 专用提交入口的完整输入。SA-b 必须先写报告与 stopOutcome/stopCheck，再写 ended。
/// 优先级：父 failed > 父 interrupted > loopGuard > 树触发原因 > goal > 父 completed。
/// ParentFinishedEarly 固定 interrupted/subagentsUnfinished，普通释放映射不得覆盖。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeFinishCommit {
    pub snapshot: ReleaseSnapshot,
    pub report: TreeStopReport,
    pub status: RunStatus,
    pub interrupt_reason: Option<InterruptReason>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeStopOperation {
    pub tree_op: u64,
    pub generation: u64,
    pub round: u64,
    pub request: TreeStopRequest,
    pub waiters: BTreeMap<u64, TreeStopWaiter>,
    pub report: TreeStopReport,
    pub in_flight: usize,
    pub cancelled: bool,
    pub committed: bool,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Trees {
    pub operations: BTreeMap<u64, TreeStopOperation>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootStopStage {
    Confirmed,
    Verified,
    Deadline,
    WaiterDeadline(u64),
}
impl Machine {
    pub fn begin_tree_stop(&mut self, _request: TreeStopRequest, actions: &mut Vec<Action>) -> u64 {
        actions.push(Action::Warn("SA 整树停止尚未启用".into()));
        0
    }
    /// 三个根出口共用；true 表示树协调器已接管账本、释放与等待者回报。
    pub fn root_stop_report(
        &mut self,
        _op: &super::StopOp,
        _stage: RootStopStage,
        _actions: &mut Vec<Action>,
    ) -> bool {
        false
    }
    pub fn tree_root_done(
        &mut self,
        _tree_op: u64,
        _generation: u64,
        _round: u64,
        _report: RootStopReport,
        _actions: &mut Vec<Action>,
    ) {
    }
    pub fn on_member_changed(
        &mut self,
        _run_id: &str,
        _revision: u64,
        _reason: MemberChange,
        _actions: &mut Vec<Action>,
    ) {
    }
    pub fn on_lineage_changed(&mut self, _logical_root: &str, _actions: &mut Vec<Action>) {}
    /// false 保留原 begin_stop 行为；SA-b 在此统一升级所有内部停止来源。
    pub fn tree_stop_gate(
        &mut self,
        _thread: &str,
        _reason: crate::claim::StopReason,
        _clear: bool,
        _actions: &mut Vec<Action>,
    ) -> bool {
        false
    }
    pub fn tree_busy(&self, _thread: Option<&str>) -> bool {
        false
    }
    pub fn tree_timer(&mut self, _kind: TimerKind, _actions: &mut Vec<Action>) {}
    pub fn tree_reply(
        &mut self,
        _context: &RequestContext,
        _result: Result<Value, Value>,
        _actions: &mut Vec<Action>,
    ) {
    }
    pub fn trees_shutdown(&mut self, _actions: &mut Vec<Action>) {}
}

impl Machine {
    /// SA-b：客户端树等待者必须在单线程 alreadyStopped 快捷出口之前登记。
    pub fn tree_client_stop(
        &mut self,
        _request: &ClientRequest,
        _actions: &mut Vec<Action>,
    ) -> bool {
        false
    }
    /// SA-b：普通释放不能绕过已接管的树提交；关闭与专用提交分别收束。
    pub fn tree_release_gate(
        &mut self,
        _thread: &str,
        _kind: crate::runs::ReleaseKind,
        _actions: &mut Vec<Action>,
    ) -> bool {
        false
    }
}
