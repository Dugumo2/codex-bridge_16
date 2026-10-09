//! SA-b 整树停止与树结束提交（接口第 8.3–8.5 节）。
//!
//! - 步骤：立根屏障（`subagent_work_gate` 拒绝范围内的新工作）→ 冻结清理策略 → 停根（有认领走现有
//!   单线程停止并只回报根状态；结束快照或无认领走派生停止）→ 对已知成员发起派生停止 → 用发现与实时事件
//!   扩展集合 → 全部落定后再发现、再核实一遍形成报告。
//! - 并发：最多 8 个在途请求；中断已发出、等确认的成员让出名额。
//! - 等待：每个调用者从进入起最多 40 秒，新增成员与重试都不续期；另有独立的首次交付期限。
//!   到期只结束等待并交付 unconfirmed，清理与屏障继续，之后每 15 秒核实一次。
//! - 树协调器是唯一的结束提交者：先写 childrenStop 与聚合 stopOutcome/stopCheck，再写 ended；
//!   提交后的核实只更新报告并发 childChanged，不再发 RunEnded。
use super::derived::{DerivedOp, DerivedStage, DerivedStopDone, DerivedTarget};
use super::MAX_STOP_WAITERS;
use crate::claim::{
    Action, ClaimToken, ClientRequest, ConnId, Machine, Owner, RequestContext, State, StopReason,
    TimerKey, TimerKind,
};
use crate::proto;
use crate::relations::{
    Activity, AgentVersion, Authority, DiscoveryCaller, DiscoveryScope, DiscoveryState,
    MemberActivity, MemberChange, RelationKind, RunIdentity,
};
use crate::runs::{InterruptReason, ReleaseKind, RunStatus, StopCheck, TurnStatus};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

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
    #[serde(default)]
    pub background_cleanup: super::background::BackgroundReport,
    #[serde(default)]
    pub step: String,
    #[serde(default)]
    pub last_verified_at: Option<u64>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_release: Option<RootRelease>,
    #[serde(default)]
    pub members: BTreeMap<String, RootStopReport>,
    #[serde(default)]
    pub cleanup_state: String,
    #[serde(default)]
    pub abandon_reason: Option<String>,
    #[serde(default)]
    pub abandoned_at: Option<u64>,
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

/// 显式放弃时的根占用结论；不代表子代理或后台命令已退出。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RootRelease {
    pub thread: String,
    pub released: bool,
    pub reason: String,
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
    /// SA-b 内部登记（不属于冻结契约）：编号、协调进度、派生停止、生命周期检查。
    pub aux: TreesAux,
}

/// 树停止的协调进度（按 tree_op）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeWork {
    /// 仅在已结束、无根认领的历史核验中允许卸载无法核实时释放。
    pub historical_unloaded: bool,
    /// 停止范围内的运行；首个是提交终态的主运行。`--run R` 时只有 R。
    pub runs: BTreeSet<String>,
    pub root_thread: String,
    /// 根线程走派生停止（无认领或 FinishingTree）；否则走现有单线程停止并只回报根状态。
    pub root_derived: bool,
    /// 根单线程停止的代号（认领模式）。
    pub root_stop_gen: Option<u64>,
    pub root_done: bool,
    pub root_goal: String,
    pub root_interrupt_sent: bool,
    /// 等名额的派生停止（线程）。
    pub queue: VecDeque<String>,
    /// 各成员最近一次 `derived_stop_done` 的结论。
    pub results: BTreeMap<String, DerivedStopDone>,
    /// 在途发现编号与最近一次完成的发现状态。
    pub discovery: Option<u64>,
    pub discovery_state: Option<DiscoveryState>,
    /// 全部落定后的收尾轮：新的一次发现与全部成员（含根）再核实一遍。
    pub final_discovery: Option<u64>,
    pub final_complete: Option<bool>,
    pub final_pending: BTreeSet<String>,
    pub final_started: bool,
    /// 首次交付期限（40 秒）已到。
    pub deadline_passed: bool,
    /// 当前协调计时：(到期单调毫秒, 代号)。
    pub tick: Option<(u64, u64)>,
    /// 下一次周期核实（每 15 秒）。
    pub next_periodic_ms: u64,
    pub confirmed: bool,
}

/// 等待发现结论的 rm/purge 请求。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleCheck {
    pub request: ClientRequest,
    pub thread: String,
    pub attempt: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TreesAux {
    next_id: u64,
    pub work: BTreeMap<u64, TreeWork>,
    pub derived: BTreeMap<(u64, String), DerivedOp>,
    /// 树协调器内部发起根单线程停止时，`tree_stop_gate` 对该线程放行。
    pub(crate) bypass: Option<String>,
    /// 专用提交释放根认领时，`tree_release_gate` 对该线程放行。
    pub(crate) committing: Option<String>,
    driving: bool,
    dirty: BTreeSet<u64>,
    /// rm/purge 的发现检查：发现编号 → 请求。
    pub lifecycle: BTreeMap<u64, LifecycleCheck>,
    /// 已通过发现检查、重新分派的请求（连接与请求编号），门禁只放行一次。
    pub(crate) lifecycle_cleared: Option<(ConnId, String)>,
}

impl TreesAux {
    pub(crate) fn next(&mut self) -> u64 {
        self.next_id = self.next_id.checked_add(1).expect("树停止编号已耗尽");
        self.next_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootStopStage {
    Confirmed,
    Verified,
    Deadline,
    WaiterDeadline(u64),
}

fn tree_token(thread: &str) -> ClaimToken {
    ClaimToken {
        thread_id: thread.into(),
        claim_id: 0,
    }
}

fn empty_discovery() -> DiscoveryState {
    DiscoveryState {
        missing: Default::default(),
        complete: false,
        version: 0,
        incomplete_reasons: Vec::new(),
    }
}

fn root_report(thread: &str, phase: TreePhase) -> RootStopReport {
    RootStopReport {
        background_cleanup: Default::default(),
        step: "pending".into(),
        last_verified_at: None,
        thread: thread.into(),
        phase,
        check: StopCheck {
            ownership: true,
            turn_ended: false,
            goal_braked: false,
            settled: false,
            verify_done: false,
        },
        clear_requested: false,
        clear_confirmed: false,
        error: None,
    }
}

impl Machine {
    // ─── 查询 ────────────────────────────────────────────────────────

    /// 运行所在的（未取消）树停止。
    pub(crate) fn tree_for_run(&self, run: &str) -> Option<u64> {
        self.trees
            .operations
            .values()
            .find(|op| !op.cancelled && op.request.snapshot.identity.run_id == run)
            .map(|op| op.tree_op)
    }

    /// 运行有受管子树：已有树停止，或运行上下文未撤权且有子代理成员、在途派生或未落定活动。
    pub(crate) fn tree_applies(&self, run: &str) -> bool {
        if self.tree_for_run(run).is_some() {
            return true;
        }
        let Some(context) = self.manage_context(run) else {
            return self
                .runs
                .runs
                .get(run)
                .is_some_and(|r| !r.children.is_empty())
                && !self.tree_abandoned(run);
        };
        if context.authority == Authority::Revoked
            && context.cleanup.is_none()
            && !self.runs.runs.get(run).is_some_and(|r| {
                !r.joined
                    && !r.children.is_empty()
                    && matches!(
                        r.state,
                        crate::runs::RunState::Ended | crate::runs::RunState::Lost
                    )
            })
        {
            return false;
        }
        !context.in_flight_spawns.is_empty()
            || context
                .members
                .values()
                .any(|m| m.kind == RelationKind::Subagent)
            || self.has_managed_activity(run) != Activity::None
    }

    /// 线程正处于某次树停止的范围（根或成员）。
    fn tree_covers(&self, thread: &str) -> bool {
        self.trees.operations.values().any(|op| {
            !op.cancelled
                && (self
                    .trees
                    .aux
                    .work
                    .get(&op.tree_op)
                    .is_some_and(|w| w.root_thread == thread)
                    || self
                        .trees
                        .aux
                        .derived
                        .contains_key(&(op.tree_op, thread.to_owned())))
        })
    }

    /// 停止范围内的子代理成员：线程 → 所属运行。
    fn tree_scope_members(&self, tree_op: u64) -> BTreeMap<String, String> {
        let mut members = BTreeMap::new();
        let Some(work) = self.trees.aux.work.get(&tree_op) else {
            return members;
        };
        for run in &work.runs {
            if let Some(context) = self.manage_context(run) {
                for (thread, member) in &context.members {
                    if member.kind == RelationKind::Subagent && *thread != work.root_thread {
                        members.entry(thread.clone()).or_insert_with(|| run.clone());
                    }
                }
            }
        }
        members
    }

    /// 聚合五项：归属无冲突、根与全部成员有停止证据、全部可控目标已制动且无能力限制、
    /// 请求与派生均落定、发现与全部核实完成。
    pub(crate) fn tree_check(&self, tree_op: u64) -> StopCheck {
        let Some(op) = self.trees.operations.get(&tree_op) else {
            return root_report("", TreePhase::Pending).check;
        };
        let children = &op.report.children;
        let root = &children.root.check;
        let members = self.tree_scope_members(tree_op);
        let work = self.trees.aux.work.get(&tree_op);
        let all = |f: &dyn Fn(&StopCheck) -> bool| {
            members.keys().all(|t| {
                work.and_then(|w| w.results.get(t))
                    .is_some_and(|r| f(&r.report.check))
            })
        };
        StopCheck {
            ownership: root.ownership && children.conflicts.is_empty(),
            turn_ended: root.turn_ended && all(&|c| c.turn_ended),
            goal_braked: root.goal_braked
                && children.capability_limited.is_empty()
                && all(&|c| c.goal_braked),
            settled: root.settled && children.unsettled_spawns == 0 && all(&|c| c.settled),
            verify_done: children.discovery.complete && root.verify_done && all(&|c| c.verify_done),
        }
    }

    // ─── 发起与合并 ──────────────────────────────────────────────────

    /// 发起整树停止（第 8.3 节）：立根屏障、冻结清理策略、停根、对已知成员发起派生停止、发现扩展集合。
    pub fn begin_tree_stop(&mut self, request: TreeStopRequest, actions: &mut Vec<Action>) -> u64 {
        if self.closing {
            return 0;
        }
        let run = request.snapshot.identity.run_id.clone();
        if self.tree_abandoned(&run) {
            return 0;
        }
        if let Some(existing) = self.tree_for_run(&run) {
            self.tree_merge(existing, request.clear, request.waiter, actions);
            return existing;
        }
        let previously_ended = self
            .runs
            .runs
            .get(&run)
            .is_some_and(|r| r.state == crate::runs::RunState::Ended);
        if previously_ended {
            if let Some(record) = self.runs.runs.get_mut(&run) {
                record.stop_outcome = Some(crate::runs::StopOutcome::Unconfirmed);
            }
        }
        let tree_op = self.trees.aux.next();
        let generation = self.trees.aux.next();
        let root_thread = request.snapshot.identity.execution_thread_id.clone();
        let logical_root = request.snapshot.identity.logical_root_thread_id.clone();
        let mut runs = BTreeSet::from([run.clone()]);
        if let TreeScope::Thread { logical_root } = &request.scope {
            for (other, context) in &self.relations.contexts {
                if context.identity.logical_root_thread_id == *logical_root
                    && (context.authority != Authority::Revoked || context.cleanup.is_some())
                {
                    runs.insert(other.clone());
                }
            }
        }
        let now = self.clock.mono_ms;
        let first_deadline = request.first_delivery_deadline_ms;
        let waiter = request.waiter.clone();
        let root_derived = request.root_already_idle;
        let report = TreeStopReport {
            tree_op,
            run_id: run.clone(),
            generation,
            round: 1,
            children: ChildrenStopReport {
                root_release: None,
                members: BTreeMap::new(),
                cleanup_state: "active".into(),
                abandon_reason: None,
                abandoned_at: None,
                trigger: request.trigger,
                phase: TreePhase::Pending,
                version: 0,
                root: root_report(&root_thread, TreePhase::Pending),
                stopped: Vec::new(),
                unconfirmed: Vec::new(),
                failed: Vec::new(),
                conflicts: Vec::new(),
                capability_limited: Vec::new(),
                discovery: empty_discovery(),
                unsettled_spawns: 0,
            },
            first_delivery_deadline_ms: first_deadline,
            committed: previously_ended,
            cancelled: false,
            barrier: true,
        };
        self.trees.operations.insert(
            tree_op,
            TreeStopOperation {
                tree_op,
                generation,
                round: 1,
                request,
                waiters: BTreeMap::new(),
                report,
                in_flight: 0,
                cancelled: false,
                committed: previously_ended,
            },
        );
        self.trees.aux.work.insert(
            tree_op,
            TreeWork {
                historical_unloaded: previously_ended
                    && self.trees.operations[&tree_op]
                        .request
                        .snapshot
                        .token
                        .is_none(),
                runs,
                root_thread: root_thread.clone(),
                root_derived,
                root_stop_gen: None,
                root_done: false,
                root_goal: "unchanged".into(),
                root_interrupt_sent: false,
                queue: VecDeque::new(),
                results: BTreeMap::new(),
                discovery: None,
                discovery_state: None,
                final_discovery: None,
                final_complete: None,
                final_pending: BTreeSet::new(),
                final_started: false,
                deadline_passed: false,
                tick: None,
                next_periodic_ms: now + TREE_VERIFY_MS,
                confirmed: false,
            },
        );
        self.runs.tree_retained.insert(run.clone());
        actions.push(Action::Debug(format!(
            "运行 {run} 发起整树停止 {tree_op}（{:?}）",
            self.trees.operations[&tree_op].request.trigger
        )));
        // 重复停止只重建本次显式授权的清理上下文，不恢复旧 owner 的开工或回答权。
        let historical_cleanup = root_derived
            && self.runs.runs.get(&run).is_some_and(|r| {
                !r.joined
                    && matches!(
                        r.state,
                        crate::runs::RunState::Ended | crate::runs::RunState::Lost
                    )
            });
        if historical_cleanup && !self.relations.contexts.contains_key(&run) {
            let record = &self.runs.runs[&run];
            // 恢复成员的历史持久化证据，不重建轮次绑定或旧 owner 权限。
            for child in &record.children {
                self.relations
                    .aux
                    .member_evidence
                    .insert((run.clone(), child.thread_id.clone()), child.evidence);
            }
            let members = record
                .children
                .iter()
                .map(|child| {
                    let edge = self.thread_relation(&child.thread_id);
                    (
                        child.thread_id.clone(),
                        crate::relations::MemberState {
                            parent: child.parent_thread_id.clone(),
                            kind: child.relation_kind,
                            capability: edge.as_ref().and_then(|e| e.can_accept_direct_input),
                            agent_version: edge.map_or(AgentVersion::Unknown, |e| e.agent_version),
                            bindings: child.turn_ids.clone(),
                            conflict: None,
                            activity: MemberActivity::Unknown,
                            pending_requests: 0,
                            active_goal: None,
                        },
                    )
                })
                .collect();
            self.relations.contexts.insert(
                run.clone(),
                crate::relations::ManageContext {
                    identity: self.trees.operations[&tree_op]
                        .request
                        .snapshot
                        .identity
                        .clone(),
                    gen: 1,
                    authority: Authority::Cleanup { tree_op },
                    policy: crate::policy::Policy::OnRequest,
                    members,
                    in_flight_spawns: BTreeSet::new(),
                    revision: 0,
                    discovery: empty_discovery(),
                    cleanup: None,
                },
            );
        }
        // Lost 的旧清理上下文可能仍在内存中；新一次已核对身份的停止只重授清理权。
        if historical_cleanup {
            if let Some(context) = self.relations.contexts.get_mut(&run) {
                if context.authority == Authority::Revoked {
                    context.gen = context.gen.saturating_add(1);
                    context.authority = Authority::Cleanup { tree_op };
                }
            }
        }
        // 第 1 步：根屏障与清理策略在停止开始时冻结；已转发的子请求按清理策略撤回。
        self.relations_enter_cleanup(&run, tree_op, actions);
        if previously_ended {
            self.relations_commit_cleanup(&run, tree_op, actions);
        }
        // 独立首次交付期限：内部触发也从首次 begin_tree_stop 起算，重试与新成员都不续期。
        actions.push(Action::Schedule {
            token: tree_token(&root_thread),
            kind: TimerKind::TreeStopWait {
                tree_op,
                waiter: None,
                generation,
            },
            delay_ms: first_deadline.saturating_sub(now),
        });
        // 第 2 步：根线程。
        if root_derived {
            self.tree_root_derived(tree_op, BTreeSet::new(), false);
        } else {
            self.tree_root_claim_stop(tree_op, actions);
        }
        // 第 4 步：发现扩展集合。
        let discovery = self.begin_discovery(
            &logical_root,
            DiscoveryScope::Run { run_id: run },
            DiscoveryCaller::TreeStop { tree_op, round: 1 },
            actions,
        );
        if let Some(work) = self.trees.aux.work.get_mut(&tree_op) {
            work.discovery = (discovery != 0).then_some(discovery);
        }
        if let Some(waiter) = waiter {
            self.tree_add_waiter(tree_op, waiter, actions);
        }
        // 第 3、5 步：已知活动成员（含有待答请求的来源线程）发起派生停止。
        self.tree_drive(tree_op, actions);
        tree_op
    }

    /// 根线程有认领：用现有单线程停止，改为只回报根状态。已有的单线程停止直接收编，
    /// 其等待者转成树级等待者（从进入树级停止起各自计时）。
    fn tree_root_claim_stop(&mut self, tree_op: u64, actions: &mut Vec<Action>) {
        let (thread, clear, reason) = {
            let op = &self.trees.operations[&tree_op];
            let reason = match op.request.trigger {
                TreeTrigger::OwnerGone => StopReason::ClientDisconnected,
                TreeTrigger::LoopGuard => StopReason::GoalLoop,
                _ => StopReason::Stop,
            };
            (
                op.request.snapshot.identity.execution_thread_id.clone(),
                op.request.clear,
                reason,
            )
        };
        let existing = self
            .stops
            .operations
            .iter()
            .find(|(_, op)| op.thread == thread && !op.foreign_claim)
            .map(|(id, _)| id.clone());
        self.trees.aux.bypass = Some(thread.clone());
        let id = self.begin_stop(&thread, reason, clear, actions);
        self.trees.aux.bypass = None;
        let Some(id) = id else {
            // joined 等不能停止的根：只记根未确认，由核实与报告如实说明。
            return;
        };
        let now = self.clock.mono_ms;
        let mut moved = Vec::new();
        if let Some(op) = self.stops.operations.get_mut(&id) {
            op.narrow = matches!(
                self.trees.operations[&tree_op].request.scope,
                TreeScope::Run { .. }
            );
            op.goal_attempted = false;
            if op.clear {
                op.clear_done = false;
            }
            if existing.is_none() || self.trees.operations[&tree_op].request.waiter.is_some() {
                op.explicit_interrupts = true;
            }
            moved = std::mem::take(&mut op.waiters);
            let gen = op.generation;
            if let Some(work) = self.trees.aux.work.get_mut(&tree_op) {
                work.root_stop_gen = Some(gen);
            }
        }
        for waiter in moved {
            let id = self.trees.aux.next();
            self.tree_add_waiter(
                tree_op,
                TreeStopWaiter {
                    id,
                    request: waiter.request,
                    entered_at_ms: now,
                    deadline_ms: now + FIRST_DELIVERY_MS,
                },
                actions,
            );
        }
    }

    /// 根线程走派生停止（结束快照或无认领）；`stopped` 为真表示根停止已成立、只等后续变化。
    fn tree_root_derived(&mut self, tree_op: u64, proven: BTreeSet<String>, stopped: bool) {
        let Some(op) = self.trees.operations.get(&tree_op) else {
            return;
        };
        let identity = op.request.snapshot.identity.clone();
        let thread = self.trees.aux.work.get(&tree_op).map_or_else(
            || identity.execution_thread_id.clone(),
            |w| w.root_thread.clone(),
        );
        let target = DerivedTarget {
            thread: thread.clone(),
            run_id: identity.run_id.clone(),
            proven_turns: proven,
            capability: Some(true),
            agent_version: AgentVersion::Unknown,
            tree_op,
            generation: op.generation,
            round: op.round,
            authority_gen: op.request.snapshot.gen,
            reason: op.request.trigger,
            clear: op.request.clear,
        };
        if let Some(work) = self.trees.aux.work.get_mut(&tree_op) {
            work.root_derived = true;
        }
        let key = (tree_op, thread);
        let mut derived = DerivedOp::new(target, true);
        if stopped {
            derived.stage = DerivedStage::Waiting;
            derived.goal_checked = true;
            derived.goal_braked = true;
            derived.clear_done = true;
            derived.turn_ended = true;
            derived.verified = true;
            derived.stopped = true;
            self.trees.aux.derived.insert(key.clone(), derived);
            // 这里只登记，不回调协调器。调用方复制完根的清理证据后再统一报告，
            // 否则收尾复核会在交接未完成时重入，随后被旧的 confirmed 状态覆盖。
        } else {
            self.trees.aux.derived.insert(key.clone(), derived);
            self.derived_requeue(&key);
        }
    }

    /// 合并调用者与清除要求：调用者各自计时；新调用者促使等待确认的成员立即再核实。
    fn tree_merge(
        &mut self,
        tree_op: u64,
        clear: bool,
        waiter: Option<TreeStopWaiter>,
        actions: &mut Vec<Action>,
    ) {
        let explicit = waiter.is_some();
        if clear {
            self.tree_set_clear(tree_op, actions);
        }
        if let Some(waiter) = waiter {
            self.tree_add_waiter(tree_op, waiter, actions);
        }
        if explicit {
            let keys: Vec<(u64, String)> = self
                .trees
                .aux
                .derived
                .iter()
                .filter(|(k, op)| k.0 == tree_op && !op.stopped && !op.slot)
                .map(|(k, _)| k.clone())
                .collect();
            for key in keys {
                self.derived_requeue(&key);
            }
            if let Some(id) = self.tree_root_stop_key(tree_op) {
                if let Some(op) = self.stops.operations.get_mut(&id) {
                    op.explicit_interrupts = true;
                    op.goal_attempted = false;
                }
            }
        }
        self.tree_drive(tree_op, actions);
    }

    fn tree_set_clear(&mut self, tree_op: u64, actions: &mut Vec<Action>) {
        let Some(op) = self.trees.operations.get_mut(&tree_op) else {
            return;
        };
        if op.request.clear {
            return;
        }
        op.request.clear = true;
        let keys: Vec<(u64, String)> = self
            .trees
            .aux
            .derived
            .keys()
            .filter(|k| k.0 == tree_op)
            .cloned()
            .collect();
        for key in keys {
            if let Some(d) = self.trees.aux.derived.get_mut(&key) {
                d.target.clear = true;
                d.clear_done = false;
                d.goal_checked = false;
                d.stopped = false;
            }
            self.derived_requeue(&key);
        }
        if let Some(id) = self.tree_root_stop_key(tree_op) {
            let thread = self.stops.operations[&id].thread.clone();
            self.trees.aux.bypass = Some(thread.clone());
            self.begin_stop(&thread, StopReason::Stop, true, actions);
            self.trees.aux.bypass = None;
        }
    }

    fn tree_root_stop_key(&self, tree_op: u64) -> Option<String> {
        let work = self.trees.aux.work.get(&tree_op)?;
        let gen = work.root_stop_gen?;
        self.stops
            .operations
            .iter()
            .find(|(_, op)| op.generation == gen)
            .map(|(id, _)| id.clone())
    }

    fn tree_add_waiter(&mut self, tree_op: u64, waiter: TreeStopWaiter, actions: &mut Vec<Action>) {
        let Some(op) = self.trees.operations.get_mut(&tree_op) else {
            return;
        };
        if op.waiters.len() >= MAX_STOP_WAITERS {
            let reply = self.tree_result(tree_op, &waiter.request, "unconfirmed");
            actions.push(Action::Send {
                conn: waiter.request.conn,
                message: proto::response_ok(waiter.request.id.clone(), reply),
            });
            return;
        }
        let generation = op.generation;
        let delay = waiter.deadline_ms.saturating_sub(self.clock.mono_ms);
        let id = waiter.id;
        let thread = self
            .trees
            .aux
            .work
            .get(&tree_op)
            .map(|w| w.root_thread.clone())
            .unwrap_or_default();
        op.waiters.insert(id, waiter);
        actions.push(Action::Schedule {
            token: tree_token(&thread),
            kind: TimerKind::TreeStopWait {
                tree_op,
                waiter: Some(id),
                generation,
            },
            delay_ms: delay,
        });
    }

    /// 树级停止对等待者的回包：沿用单线程停止的字段，另加 childrenStop。
    fn tree_result(&self, tree_op: u64, request: &ClientRequest, outcome: &str) -> Value {
        let op = &self.trees.operations[&tree_op];
        let work = self.trees.aux.work.get(&tree_op);
        let run = &op.request.snapshot.identity.run_id;
        let interrupt_sent = work.is_some_and(|w| w.root_interrupt_sent)
            || self
                .trees
                .aux
                .derived
                .iter()
                .any(|(k, d)| k.0 == tree_op && !d.interrupted.is_empty());
        let thread = request
            .params
            .as_ref()
            .and_then(|p| p.get("threadId"))
            .cloned()
            .unwrap_or_else(|| json!(op.request.snapshot.identity.logical_root_thread_id));
        json!({
            "threadId": thread,
            "runId": run,
            "interruptSent": interrupt_sent,
            "goal": work.map_or("unchanged", |w| w.root_goal.as_str()),
            "alreadyStopped": false,
            "stopOutcome": outcome,
            "run": self.run_snapshot(run),
            "childrenStop": op.report.children,
        })
    }

    fn tree_answer(
        &mut self,
        tree_op: u64,
        only: Option<u64>,
        outcome: &str,
        actions: &mut Vec<Action>,
    ) {
        let Some(op) = self.trees.operations.get_mut(&tree_op) else {
            return;
        };
        let ids: Vec<u64> = op
            .waiters
            .keys()
            .copied()
            .filter(|id| only.is_none_or(|o| o == *id))
            .collect();
        let waiters: Vec<TreeStopWaiter> =
            ids.iter().filter_map(|id| op.waiters.remove(id)).collect();
        for waiter in waiters {
            actions.push(Action::Unschedule {
                key: TimerKey::TreeStopWait {
                    tree_op,
                    waiter: Some(waiter.id),
                },
            });
            if self.live_conns.contains(&waiter.request.conn) {
                let reply = self.tree_result(tree_op, &waiter.request, outcome);
                actions.push(Action::Send {
                    conn: waiter.request.conn,
                    message: proto::response_ok(waiter.request.id.clone(), reply),
                });
            }
        }
    }

    // ─── 根回报 ──────────────────────────────────────────────────────

    /// 三个根出口共用；true 表示树协调器已接管账本、释放与等待者回报。
    pub fn root_stop_report(
        &mut self,
        op: &super::StopOp,
        stage: RootStopStage,
        actions: &mut Vec<Action>,
    ) -> bool {
        let Some(run) = op.run_id.as_deref() else {
            return false;
        };
        let Some(tree_op) = self.tree_for_run(run) else {
            return false;
        };
        let owned = self
            .trees
            .aux
            .work
            .get(&tree_op)
            .is_some_and(|w| !w.root_derived && w.root_stop_gen == Some(op.generation));
        if !owned {
            return false;
        }
        if let Some(work) = self.trees.aux.work.get_mut(&tree_op) {
            // review 迁移后根停止跟随实际执行线程。
            work.root_thread = op.thread.clone();
            work.root_goal = op.goal.clone();
            work.root_interrupt_sent |= op.interrupt_sent;
        }
        let deadline_passed = self.trees.aux.work[&tree_op].deadline_passed;
        let mut report = RootStopReport {
            background_cleanup: op.background.report.clone(),
            step: op.background.report.state.clone(),
            last_verified_at: op.background.report.last_verified_at,
            thread: op.thread.clone(),
            phase: if deadline_passed {
                TreePhase::Unconfirmed
            } else {
                TreePhase::Pending
            },
            check: self.stop_check(op),
            clear_requested: op.clear,
            clear_confirmed: op.clear && op.clear_done && op.goal != "unchanged",
            error: None,
        };
        let generation = self.trees.operations[&tree_op].generation;
        match stage {
            RootStopStage::WaiterDeadline(id) => {
                // 树接管后单线程等待者已转成树级等待者；仍在本操作上的照原规则回报。
                return !op.waiters.iter().any(|w| w.id == id);
            }
            RootStopStage::Verified | RootStopStage::Deadline => {
                self.tree_root_done(tree_op, generation, 0, report, actions);
            }
            RootStopStage::Confirmed => {
                report.phase = TreePhase::Confirmed;
                // 根停止成立：认领转入结束快照，等树协调器统一提交；之后根上的新轮次由派生停止处理。
                self.tree_hold_root(tree_op, op, actions);
                if let Some(work) = self.trees.aux.work.get_mut(&tree_op) {
                    work.root_stop_gen = None;
                }
                let proven = BTreeSet::new();
                self.tree_root_derived(tree_op, proven, true);
                let key = (tree_op, op.thread.clone());
                if let Some(d) = self.trees.aux.derived.get_mut(&key) {
                    d.background = op.background.clone();
                    d.connection_epoch = op.connection_epoch;
                    d.target.clear = op.clear;
                    d.clear_done = !op.clear || op.clear_done;
                    d.goal_report = op.goal.clone();
                }
                self.tree_root_done(tree_op, generation, 0, report, actions);
            }
        }
        true
    }

    /// 根认领转为 `FinishingTree`：保留 owner 与父轮，运行保持原状态，等专用提交。
    fn tree_hold_root(&mut self, tree_op: u64, op: &super::StopOp, _actions: &mut Vec<Action>) {
        let Some(claim) = self
            .claims
            .get(&op.thread)
            .filter(|c| op.claim_id == Some(c.claim_id))
        else {
            return;
        };
        if matches!(claim.state, State::FinishingTree { .. }) {
            return;
        }
        let Some(owner) = claim.owner().cloned() else {
            return;
        };
        let turn_id = claim.turn_id().filter(|t| !t.is_empty()).map(str::to_owned);
        let run = claim.run_id.clone().unwrap_or_default();
        let parent_status = self
            .runs
            .runs
            .get(&run)
            .and_then(|r| r.turns.last())
            .map(|t| t.status);
        let goal_status = self
            .records
            .get(&op.thread)
            .and_then(|r| r.goal_status.clone());
        let identity = self.trees.operations[&tree_op]
            .request
            .snapshot
            .identity
            .clone();
        let gen = self.manage_context(&run).map_or(0, |c| c.gen);
        let claim = self.claims.get_mut(&op.thread).expect("已核对认领");
        claim.state = State::FinishingTree {
            owner,
            turn_id,
            parent_status,
            goal_status,
            identity: RunIdentity {
                execution_thread_id: op.thread.clone(),
                ..identity
            },
            gen,
            tree_op,
            release_kind: ReleaseKind::StopConfirmed,
        };
        claim.request_pending = false;
    }

    pub fn tree_root_done(
        &mut self,
        tree_op: u64,
        generation: u64,
        _round: u64,
        report: RootStopReport,
        actions: &mut Vec<Action>,
    ) {
        let Some(op) = self.trees.operations.get_mut(&tree_op) else {
            return;
        };
        if op.generation != generation || op.cancelled {
            return;
        }
        let done = report.phase == TreePhase::Confirmed;
        op.report.children.root = report;
        if let Some(work) = self.trees.aux.work.get_mut(&tree_op) {
            work.root_done = done;
            if !done {
                work.final_started = false;
            } else {
                let thread = work.root_thread.clone();
                work.final_pending.remove(&thread);
            }
        }
        self.tree_drive(tree_op, actions);
    }

    /// 无认领的历史根也接收活动失效通知，只重做核验，不给外部轮次授予旧运行归属。
    pub(crate) fn unclaimed_tree_root_changed(&mut self, thread: &str, actions: &mut Vec<Action>) {
        let ops: Vec<_> = self
            .trees
            .operations
            .values()
            .filter(|op| {
                !op.cancelled
                    && self
                        .trees
                        .aux
                        .work
                        .get(&op.tree_op)
                        .is_some_and(|w| w.root_derived && w.root_thread == thread)
            })
            .map(|op| op.tree_op)
            .collect();
        for tree_op in ops {
            let key = (tree_op, thread.to_owned());
            if let Some(work) = self.trees.aux.work.get_mut(&tree_op) {
                work.root_done = false;
                work.final_started = false;
                work.final_complete = None;
                work.final_pending.insert(thread.into());
            }
            self.derived_invalidate_background(&key, actions);
            if let Some(op) = self.trees.aux.derived.get_mut(&key) {
                op.stopped = false;
                op.turn_ended = false;
                op.verified = false;
                op.goal_checked = false;
            }
            self.derived_requeue(&key);
            self.tree_drive(tree_op, actions);
        }
    }

    // ─── 事实变化 ────────────────────────────────────────────────────

    pub fn on_member_changed(
        &mut self,
        run_id: &str,
        _revision: u64,
        reason: MemberChange,
        actions: &mut Vec<Action>,
    ) {
        if self.closing {
            return;
        }
        let ops: Vec<u64> = self
            .trees
            .operations
            .values()
            .filter(|op| {
                !op.cancelled
                    && self
                        .trees
                        .aux
                        .work
                        .get(&op.tree_op)
                        .is_some_and(|w| w.runs.contains(run_id))
            })
            .map(|op| op.tree_op)
            .collect();
        for tree_op in ops {
            if matches!(
                reason,
                MemberChange::Activity | MemberChange::Binding | MemberChange::Goal
            ) {
                self.tree_reopen_active(tree_op);
            }
            self.tree_drive(tree_op, actions);
        }
    }

    /// 已成立或等确认的成员重新活动（新轮次、goal 重新 active）：纳入同一次停止并立即再核实。
    fn tree_reopen_active(&mut self, tree_op: u64) {
        let members = self.tree_scope_members(tree_op);
        let mut requeue = Vec::new();
        for thread in members.keys() {
            let state = self.tree_scope_runs(tree_op).iter().find_map(|run| {
                self.manage_context(run)
                    .and_then(|c| c.members.get(thread).cloned())
            });
            let Some(member) = state else { continue };
            let key = (tree_op, thread.clone());
            let Some(op) = self.trees.aux.derived.get_mut(&key) else {
                continue;
            };
            let active = matches!(
                member.activity,
                MemberActivity::Running | MemberActivity::Starting | MemberActivity::Unknown
            );
            let changed = op.seen_activity != Some(member.activity);
            op.seen_activity = Some(member.activity);
            if member.active_goal == Some(true) && op.goal_ok() {
                op.goal_checked = false;
                op.stopped = false;
            }
            if (changed || member.active_goal == Some(true))
                && (op.background.busy || op.background.verifying || op.background.confirmed)
            {
                op.background.invalidate();
                op.request_round += 1;
                op.stopped = false;
                op.goal_checked = false;
                self.derived_release_slot(&key);
                requeue.push(key.clone());
                continue;
            }
            if active && op.stopped {
                op.stopped = false;
            }
            // 活动变化（新轮次、轮次结束）或重新需要制动：立即再核实；等中断回包的不打断。
            if (changed || !op.goal_checked)
                && !op.stopped
                && op.stage == DerivedStage::Waiting
                && op.interrupts_pending == 0
            {
                requeue.push(key);
            }
        }
        for key in requeue {
            self.derived_requeue(&key);
        }
    }

    fn tree_scope_runs(&self, tree_op: u64) -> Vec<String> {
        self.trees
            .aux
            .work
            .get(&tree_op)
            .map(|w| w.runs.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// 只有血缘变化：在途生命周期发现失效重查；树停止重新同步成员。
    pub fn on_lineage_changed(&mut self, logical_root: &str, actions: &mut Vec<Action>) {
        if self.closing {
            return;
        }
        self.lifecycle_lineage_changed(logical_root, actions);
        let ops: Vec<u64> = self
            .trees
            .operations
            .values()
            .filter(|op| {
                !op.cancelled && op.request.snapshot.identity.logical_root_thread_id == logical_root
            })
            .map(|op| op.tree_op)
            .collect();
        for tree_op in ops {
            // 新成员使整次收尾的空列表证据过时；完整性恢复后对全部成员重新核验。
            if let Some(work) = self.trees.aux.work.get_mut(&tree_op) {
                work.final_started = false;
                work.final_complete = None;
            }
            self.tree_drive(tree_op, actions);
        }
    }

    // ─── 协调 ────────────────────────────────────────────────────────

    /// 按名额放行排队的派生停止：最多 8 个在途请求；等确认的成员不占名额。
    pub(crate) fn tree_pump(&mut self, tree_op: u64, actions: &mut Vec<Action>) {
        loop {
            let Some(op) = self.trees.operations.get(&tree_op) else {
                return;
            };
            if op.cancelled || op.in_flight >= MAX_TREE_REQUESTS {
                return;
            }
            let Some(work) = self.trees.aux.work.get_mut(&tree_op) else {
                return;
            };
            let Some(thread) = work.queue.pop_front() else {
                return;
            };
            let key = (tree_op, thread);
            if !self
                .trees
                .aux
                .derived
                .get(&key)
                .is_some_and(|d| !d.slot && d.stage == DerivedStage::Queued)
            {
                continue;
            }
            if let Some(op) = self.trees.operations.get_mut(&tree_op) {
                op.in_flight += 1;
            }
            self.derived_start(&key, actions);
        }
    }

    /// 协调一次：同步成员、放行名额、更新报告、判定确认或首次交付、排计时。可重入时只记脏标记。
    pub(crate) fn tree_drive(&mut self, tree_op: u64, actions: &mut Vec<Action>) {
        if self.trees.aux.driving {
            self.trees.aux.dirty.insert(tree_op);
            return;
        }
        self.trees.aux.driving = true;
        self.trees.aux.dirty.insert(tree_op);
        let mut rounds = 0;
        while let Some(next) = self.trees.aux.dirty.pop_first() {
            rounds += 1;
            if rounds > 256 {
                actions.push(Action::Warn("树停止协调未收敛，留待下一次事件".into()));
                break;
            }
            self.tree_drive_once(next, actions);
        }
        self.trees.aux.driving = false;
    }

    fn tree_drive_once(&mut self, tree_op: u64, actions: &mut Vec<Action>) {
        let Some(op) = self.trees.operations.get(&tree_op) else {
            return;
        };
        if op.cancelled {
            return;
        }
        let run = op.request.snapshot.identity.run_id.clone();
        // 外部接管：撤权只能以冲突收尾，不伪造确认。
        if !op.committed
            && self
                .manage_context(&run)
                .is_some_and(|c| c.authority == Authority::Revoked)
        {
            self.tree_cancel(tree_op, "运行已被外部接管或撤权", true, actions);
            return;
        }
        self.tree_collect_discovery(tree_op);
        // 同步成员：新成员纳入同一次操作，期限不重置。
        let members = self.tree_scope_members(tree_op);
        let (generation, round, clear, trigger) = {
            let op = &self.trees.operations[&tree_op];
            (
                op.generation,
                op.round,
                op.request.clear,
                op.request.trigger,
            )
        };
        for (thread, member_run) in &members {
            let key = (tree_op, thread.clone());
            if self.trees.aux.derived.contains_key(&key) {
                continue;
            }
            let edge = self.thread_relation(thread);
            let member = self
                .manage_context(member_run)
                .and_then(|c| c.members.get(thread).cloned());
            let proven: BTreeSet<String> = member
                .as_ref()
                .map(|m| m.bindings.iter().cloned().collect())
                .unwrap_or_default();
            let target = DerivedTarget {
                thread: thread.clone(),
                run_id: member_run.clone(),
                proven_turns: proven,
                capability: member
                    .as_ref()
                    .and_then(|m| m.capability)
                    .or_else(|| edge.as_ref().and_then(|e| e.can_accept_direct_input)),
                agent_version: member
                    .as_ref()
                    .map(|m| m.agent_version)
                    .unwrap_or(AgentVersion::Unknown),
                tree_op,
                generation,
                round,
                authority_gen: self.manage_context(member_run).map_or(0, |c| c.gen),
                reason: trigger,
                clear,
            };
            self.trees
                .aux
                .derived
                .insert(key.clone(), DerivedOp::new(target, false));
            self.derived_requeue(&key);
            if let Some(work) = self.trees.aux.work.get_mut(&tree_op) {
                work.final_started = false;
            }
        }
        self.tree_pump(tree_op, actions);
        self.tree_update_report(tree_op, &members, actions);
        if self.trees.aux.work[&tree_op].historical_unloaded
            && self.trees.aux.derived.iter().any(|(k, d)| {
                k.0 == tree_op && !d.stopped && d.background.report.state == "notLoaded"
            })
        {
            self.tree_release_unloaded_history(tree_op, actions);
            return;
        }
        if self.tree_confirmable(tree_op, &members) {
            self.tree_confirmed(tree_op, actions);
            return;
        }
        self.tree_maybe_final_round(tree_op, &members, actions);
        let commit_due = {
            let op = &self.trees.operations[&tree_op];
            let work = &self.trees.aux.work[&tree_op];
            !op.committed && work.deadline_passed && work.root_done
        };
        if commit_due {
            self.tree_commit(tree_op, actions);
        }
        self.tree_schedule(tree_op, actions);
    }

    /// 取回本操作在途发现的结论。
    fn tree_collect_discovery(&mut self, tree_op: u64) {
        let Some(work) = self.trees.aux.work.get(&tree_op) else {
            return;
        };
        for (slot, id) in [(false, work.discovery), (true, work.final_discovery)] {
            let Some(id) = id else { continue };
            let finished = self
                .discovery_outcome(id)
                .map(|r| r.state.clone())
                .or_else(|| {
                    self.relations
                        .discoveries
                        .get(&id)
                        .filter(|d| d.completed)
                        .map(|_| DiscoveryState {
                            missing: Default::default(),
                            complete: false,
                            version: 0,
                            incomplete_reasons: vec!["发现已取消".into()],
                        })
                });
            let Some(state) = finished else { continue };
            let work = self.trees.aux.work.get_mut(&tree_op).expect("协调进度存在");
            if slot {
                work.final_discovery = None;
                work.final_complete = Some(state.complete);
                // 临时失败留给周期重查；原调用者期限与仍在途的核实保持不变。
                if !state.complete {
                    work.final_started = false;
                }
            } else {
                work.discovery = None;
            }
            work.discovery_state = Some(state);
        }
    }

    fn tree_unsettled_spawns(&self, tree_op: u64) -> usize {
        self.tree_scope_runs(tree_op)
            .iter()
            .filter_map(|run| self.manage_context(run))
            .map(|c| c.in_flight_spawns.len())
            .sum()
    }

    /// 汇总报告；有变化时加版本、写运行记录并发 childChanged（threadId 为空、带 stopPhase）。
    fn tree_update_report(
        &mut self,
        tree_op: u64,
        members: &BTreeMap<String, String>,
        actions: &mut Vec<Action>,
    ) {
        let work = &self.trees.aux.work[&tree_op];
        let mut stopped = Vec::new();
        let mut unconfirmed = Vec::new();
        let mut failed = Vec::new();
        let mut conflicts = Vec::new();
        let mut limited = Vec::new();
        for (thread, run) in members {
            let derived = self.trees.aux.derived.get(&(tree_op, thread.clone()));
            let result = work.results.get(thread);
            let member_conflict = self
                .manage_context(run)
                .and_then(|c| c.members.get(thread))
                .filter(|m| {
                    matches!(
                        m.activity,
                        MemberActivity::Running
                            | MemberActivity::Starting
                            | MemberActivity::Unknown
                    )
                })
                .and_then(|m| m.conflict.clone());
            let is_stopped = derived.is_some_and(|d| d.stopped)
                && result.is_some_and(|r| r.report.phase == TreePhase::Confirmed);
            if let Some(reason) = member_conflict {
                conflicts.push(StopConflict {
                    thread: thread.clone(),
                    reason,
                });
            } else if let Some(r) = result.filter(|r| !r.conflicts.is_empty() && !is_stopped) {
                conflicts.push(StopConflict {
                    thread: thread.clone(),
                    reason: r.conflicts.join("；"),
                });
            }
            if result.is_some_and(|r| r.capability_limited) {
                limited.push(thread.clone());
            }
            if is_stopped {
                stopped.push(thread.clone());
            } else if result.is_some_and(|r| r.report.error.is_some() && r.conflicts.is_empty()) {
                failed.push(thread.clone());
            } else if !conflicts.iter().any(|c| c.thread == *thread) {
                unconfirmed.push(thread.clone());
            }
        }
        let discovery = work
            .discovery_state
            .clone()
            .or_else(|| {
                self.manage_context(
                    &self.trees.operations[&tree_op]
                        .request
                        .snapshot
                        .identity
                        .run_id,
                )
                .map(|c| c.discovery.clone())
            })
            .unwrap_or_else(empty_discovery);
        let unsettled = self.tree_unsettled_spawns(tree_op);
        let phase = if work.confirmed {
            TreePhase::Confirmed
        } else if work.deadline_passed {
            TreePhase::Unconfirmed
        } else {
            TreePhase::Pending
        };
        let root_current = self
            .trees
            .aux
            .derived
            .get(&(tree_op, work.root_thread.clone()))
            .map(|d| RootStopReport {
                thread: work.root_thread.clone(),
                phase: if d.stopped {
                    TreePhase::Confirmed
                } else {
                    phase
                },
                check: d.check(),
                clear_requested: d.target.clear,
                clear_confirmed: d.target.clear && d.clear_done,
                error: d.error.clone(),
                background_cleanup: d.background.report.clone(),
                step: format!("{:?}", d.stage),
                last_verified_at: d.last_verified_at,
            })
            .or_else(|| {
                self.stops
                    .operations
                    .values()
                    .find(|s| Some(s.generation) == work.root_stop_gen)
                    .map(|s| RootStopReport {
                        thread: s.thread.clone(),
                        phase,
                        check: self.stop_check(s),
                        clear_requested: s.clear,
                        clear_confirmed: s.clear && s.clear_done,
                        error: s.background.report.failure_reason.clone(),
                        background_cleanup: s.background.report.clone(),
                        step: if !s.background.busy
                            && !s.background.verifying
                            && !s.background.failed
                        {
                            "turnAndGoal".into()
                        } else {
                            s.background.report.state.clone()
                        },
                        last_verified_at: s.background.report.last_verified_at,
                    })
            });
        let op = self.trees.operations.get_mut(&tree_op).expect("树停止存在");
        let mut next = op.report.children.clone();
        if let Some(root) = root_current {
            next.root = root;
        }
        next.members = members
            .keys()
            .filter_map(|thread| {
                self.trees
                    .aux
                    .derived
                    .get(&(tree_op, thread.clone()))
                    .map(|d| {
                        (
                            thread.clone(),
                            RootStopReport {
                                thread: thread.clone(),
                                phase: if d.stopped {
                                    TreePhase::Confirmed
                                } else {
                                    phase
                                },
                                check: d.check(),
                                clear_requested: d.target.clear,
                                clear_confirmed: d.target.clear && d.clear_done,
                                error: d.error.clone(),
                                background_cleanup: d.background.report.clone(),
                                step: format!("{:?}", d.stage),
                                last_verified_at: d.last_verified_at,
                            },
                        )
                    })
            })
            .collect();
        next.phase = phase;
        if phase == TreePhase::Confirmed {
            next.cleanup_state = "completed".into();
        }
        next.stopped = stopped;
        next.unconfirmed = unconfirmed;
        next.failed = failed;
        next.conflicts = conflicts;
        next.capability_limited = limited;
        next.discovery = discovery;
        next.unsettled_spawns = unsettled;
        if next == op.report.children {
            return;
        }
        next.version += 1;
        op.report.children = next;
        self.tree_publish(tree_op, actions);
    }

    /// 报告写入运行记录（提交前只更新报告与当前聚合检查；首次期限后标 unconfirmed）并通知。
    fn tree_publish(&mut self, tree_op: u64, actions: &mut Vec<Action>) {
        let op = &self.trees.operations[&tree_op];
        let run = op.request.snapshot.identity.run_id.clone();
        let report = op.report.children.clone();
        let check = self.tree_check(tree_op);
        self.run_tree_report(&run, report.clone(), check, actions);
        let Some(context) = self.manage_context(&run) else {
            return;
        };
        let params = json!({
            "runId": run,
            "rootThreadId": context.identity.logical_root_thread_id,
            "threadId": Value::Null,
            "parentThreadId": Value::Null,
            "relationKind": Value::Null,
            "activity": Value::Null,
            "pendingCount": Value::Null,
            "revision": context.revision,
            "stopPhase": report.phase,
            "stopVersion": report.version,
        });
        let message = proto::notification("broker/run/childChanged", Some(params));
        for conn in self.run_recipients(&run) {
            self.send_ext(conn, message.clone(), actions);
        }
    }

    /// 确认定义：根停止成立、全部纳入成员的停止成立、没有未落定派生、本轮发现完整、
    /// 没有活动的 unbound 或 conflict 成员；且收尾轮（再发现、再核实）已完成。
    fn tree_confirmable(&self, tree_op: u64, members: &BTreeMap<String, String>) -> bool {
        let work = &self.trees.aux.work[&tree_op];
        let op = &self.trees.operations[&tree_op];
        work.root_done
            && (!work.root_derived
                || self
                    .trees
                    .aux
                    .derived
                    .get(&(tree_op, work.root_thread.clone()))
                    .is_some_and(|d| d.stopped && !d.slot))
            && members.keys().all(|t| {
                self.trees
                    .aux
                    .derived
                    .get(&(tree_op, t.clone()))
                    .is_some_and(|d| d.stopped && !d.slot)
            })
            && op.report.children.conflicts.is_empty()
            && op.report.children.capability_limited.is_empty()
            && op.report.children.unsettled_spawns == 0
            && work.final_started
            && work.final_pending.is_empty()
            && work.final_complete == Some(true)
            && work.final_discovery.is_none()
            && work.discovery.is_none()
    }

    /// 全部落定后再做一次发现与核实。
    fn tree_maybe_final_round(
        &mut self,
        tree_op: u64,
        members: &BTreeMap<String, String>,
        actions: &mut Vec<Action>,
    ) {
        let settled = {
            let work = &self.trees.aux.work[&tree_op];
            let op = &self.trees.operations[&tree_op];
            !work.final_started
                && work.root_done
                && work.discovery.is_none()
                && work.discovery_state.as_ref().is_some_and(|d| d.complete)
                && op.report.children.unsettled_spawns == 0
                && op.report.children.conflicts.is_empty()
                && op.report.children.capability_limited.is_empty()
                && members.keys().all(|t| {
                    self.trees
                        .aux
                        .derived
                        .get(&(tree_op, t.clone()))
                        .is_some_and(|d| d.stopped && !d.slot)
                })
        };
        if !settled {
            return;
        }
        let (root, run) = {
            let op = &self.trees.operations[&tree_op];
            (
                op.request.snapshot.identity.logical_root_thread_id.clone(),
                op.request.snapshot.identity.run_id.clone(),
            )
        };
        let round = {
            let op = self.trees.operations.get_mut(&tree_op).expect("树停止存在");
            op.round += 1;
            op.round
        };
        let discovery = self.begin_discovery(
            &root,
            DiscoveryScope::Run { run_id: run },
            DiscoveryCaller::TreeStop { tree_op, round },
            actions,
        );
        let mut pending: BTreeSet<String> = members.keys().cloned().collect();
        let root_key = self.trees.aux.work[&tree_op].root_thread.clone();
        let root_derived = self.trees.aux.work[&tree_op].root_derived;
        if root_derived {
            pending.insert(root_key.clone());
        }
        {
            let work = self.trees.aux.work.get_mut(&tree_op).expect("协调进度存在");
            work.final_started = true;
            work.final_discovery = (discovery != 0).then_some(discovery);
            work.final_complete = None;
            work.final_pending = pending.clone();
        }
        for thread in pending {
            let key = (tree_op, thread);
            if let Some(d) = self.trees.aux.derived.get_mut(&key) {
                d.stopped = false;
            }
            self.derived_requeue(&key);
        }
        self.tree_pump(tree_op, actions);
    }

    /// 首次 confirmed：先提交（若尚未提交），再回答全部调用者、终结清理权与屏障，移除操作。
    fn tree_confirmed(&mut self, tree_op: u64, actions: &mut Vec<Action>) {
        if let Some(work) = self.trees.aux.work.get_mut(&tree_op) {
            work.confirmed = true;
        }
        let members = self.tree_scope_members(tree_op);
        self.tree_update_report(tree_op, &members, actions);
        let committed = self.trees.operations[&tree_op].committed;
        if committed {
            let check = self.tree_check(tree_op);
            let op = &self.trees.operations[&tree_op];
            let run = op.request.snapshot.identity.run_id.clone();
            let report = op.report.children.clone();
            self.run_tree_report(&run, report, check, actions);
        } else {
            self.tree_commit(tree_op, actions);
        }
        self.tree_answer(tree_op, None, "confirmed", actions);
        self.tree_finish(tree_op, actions);
    }

    /// 撤权或取消：剩余成员以冲突或未确认收尾；尚未提交且仍持有根认领时以未确认报告提交结束。
    fn tree_cancel(&mut self, tree_op: u64, reason: &str, commit: bool, actions: &mut Vec<Action>) {
        actions.push(Action::Warn(format!("整树停止 {tree_op} 中止：{reason}")));
        let keys: Vec<(u64, String)> = self
            .trees
            .aux
            .derived
            .keys()
            .filter(|k| k.0 == tree_op)
            .cloned()
            .collect();
        for key in &keys {
            if let Some(d) = self.trees.aux.derived.get_mut(key) {
                if !d.stopped {
                    d.conflicts.insert(reason.into());
                }
            }
            self.derived_report(key, actions);
        }
        if let Some(work) = self.trees.aux.work.get_mut(&tree_op) {
            work.deadline_passed = true;
        }
        let members = self.tree_scope_members(tree_op);
        self.tree_update_report(tree_op, &members, actions);
        if commit && !self.trees.operations[&tree_op].committed {
            self.tree_commit(tree_op, actions);
        }
        self.tree_answer(tree_op, None, "unconfirmed", actions);
        if let Some(op) = self.trees.operations.get_mut(&tree_op) {
            op.cancelled = true;
            op.report.cancelled = true;
        }
        self.tree_finish(tree_op, actions);
    }

    fn tree_cancel_work(&mut self, tree_op: u64, actions: &mut Vec<Action>) {
        let root_generation = self.trees.aux.work[&tree_op].root_stop_gen;
        let roots: Vec<_> = self
            .stops
            .operations
            .iter()
            .filter(|(_, op)| Some(op.generation) == root_generation)
            .map(|(id, _)| id.clone())
            .collect();
        for id in roots {
            if let Some(mut op) = self.stops.operations.remove(&id) {
                for waiter in &op.waiters {
                    actions.push(Action::Unschedule {
                        key: TimerKey::Stop(
                            id.clone(),
                            op.generation,
                            super::StopTimer::Waiter(waiter.id),
                        ),
                    });
                }
                self.answer_waiters(&mut op, None, "unconfirmed", actions);
                for kind in [super::StopTimer::Deadline, super::StopTimer::Verify] {
                    if let Some(key) = TimerKind::Stop(op.tag(&id, 0), kind).key(&op.token()) {
                        actions.push(Action::Unschedule { key });
                    }
                }
            }
        }
        self.cancel_tree_reads(tree_op, root_generation, actions);
        let discoveries: Vec<_> = self.relations.discoveries.iter().filter(|(_, op)| matches!(op.caller, DiscoveryCaller::TreeStop { tree_op: id, .. } if id == tree_op)).map(|(id, _)| *id).collect();
        for id in discoveries {
            self.relations.discoveries.remove(&id);
            self.relations.aux.work.remove(&id);
            actions.push(Action::Unschedule {
                key: TimerKey::DiscoveryDeadline(id),
            });
        }
        let runs = self.trees.aux.work[&tree_op].runs.clone();
        for run in runs {
            self.relations_end_cleanup(&run, actions);
        }
    }

    /// 历史树遇到无法核验的卸载成员，交付快照后释放；不等同于停止或显式 abandon。
    pub(crate) fn tree_release_unloaded_history(
        &mut self,
        tree_op: u64,
        actions: &mut Vec<Action>,
    ) {
        if !self
            .trees
            .aux
            .work
            .get(&tree_op)
            .is_some_and(|w| w.historical_unloaded)
        {
            return;
        }
        let members = self.tree_scope_members(tree_op);
        self.tree_update_report(tree_op, &members, actions);
        let op = self.trees.operations.get_mut(&tree_op).expect("树停止存在");
        op.report.children.phase = TreePhase::Unconfirmed;
        op.report.children.cleanup_state = "released".into();
        op.report.children.version += 1;
        op.cancelled = true;
        self.tree_publish(tree_op, actions);
        self.tree_cancel_work(tree_op, actions);
        self.tree_answer(tree_op, None, "unconfirmed", actions);
        self.tree_finish(tree_op, actions);
    }

    /// 结束一次树操作：撤计时、终结清理上下文与屏障、解除淘汰保护、移除登记。
    fn tree_finish(&mut self, tree_op: u64, actions: &mut Vec<Action>) {
        let Some(op) = self.trees.operations.remove(&tree_op) else {
            return;
        };
        let work = self.trees.aux.work.remove(&tree_op);
        let keys: Vec<(u64, String)> = self
            .trees
            .aux
            .derived
            .keys()
            .filter(|k| k.0 == tree_op)
            .cloned()
            .collect();
        for key in keys {
            self.trees.aux.derived.remove(&key);
        }
        actions.push(Action::Unschedule {
            key: TimerKey::TreeStopWait {
                tree_op,
                waiter: None,
            },
        });
        actions.push(Action::Unschedule {
            key: TimerKey::TreeStopVerify(tree_op),
        });
        let run = op.request.snapshot.identity.run_id.clone();
        self.runs.tree_retained.remove(&run);
        let runs = work.as_ref().map(|w| w.runs.clone()).unwrap_or_default();
        for scope_run in runs {
            if self
                .manage_context(&scope_run)
                .is_some_and(|c| c.cleanup.as_ref().is_some_and(|c| c.tree_op == tree_op))
            {
                self.relations_end_cleanup(&scope_run, actions);
            }
        }
        if let Some(work) = work {
            let thread = work.root_thread;
            if !self.claims.contains_key(&thread) {
                if let Some(claim_id) = self.records.get(&thread).map(|r| r.last_claim_id) {
                    self.schedule_unsubscribe(
                        ClaimToken {
                            thread_id: thread,
                            claim_id,
                        },
                        actions,
                    );
                }
            }
        }
    }

    /// 唯一的结束提交：先写报告与聚合停止账本，再写 ended；之后撤销旧 owner 权、保留清理权，释放根认领。
    fn tree_commit(&mut self, tree_op: u64, actions: &mut Vec<Action>) {
        let Some(op) = self.trees.operations.get(&tree_op) else {
            return;
        };
        if op.committed || op.cancelled {
            return;
        }
        let run = op.request.snapshot.identity.run_id.clone();
        let trigger = op.request.trigger;
        let thread = self.trees.aux.work[&tree_op].root_thread.clone();
        let claim = self
            .claims
            .get(&thread)
            .filter(|c| c.run_id.as_deref() == Some(run.as_str()))
            .cloned();
        let record = self.runs.runs.get(&run);
        let mut snapshot = op.request.snapshot.clone();
        snapshot.loop_guard = record.is_some_and(|r| r.loop_guard.is_some());
        if let Some(claim) = &claim {
            snapshot.owner = claim.owner().cloned();
            snapshot.token = Some(ClaimToken {
                thread_id: thread.clone(),
                claim_id: claim.claim_id,
            });
            if let State::FinishingTree {
                turn_id,
                parent_status,
                goal_status,
                gen,
                ..
            } = &claim.state
            {
                snapshot.parent_turn_id = turn_id.clone();
                snapshot.parent_status = *parent_status;
                snapshot.goal_status = goal_status.clone();
                snapshot.gen = *gen;
            }
        }
        if snapshot.parent_status.is_none() {
            snapshot.parent_status = record.and_then(|r| r.turns.last()).map(|t| t.status);
        }
        let existing = record.and_then(|r| r.interrupt_reason);
        let (status, interrupt_reason) = if op.report.children.cleanup_state == "abandoned" {
            (RunStatus::Interrupted, Some(InterruptReason::Stop))
        } else {
            commit_status(&snapshot, trigger, existing)
        };
        let mut report = op.report.clone();
        report.committed = true;
        let commit = TreeFinishCommit {
            snapshot,
            report: report.clone(),
            status,
            interrupt_reason,
        };
        self.commit_tree_finish(commit, actions);
        let op = self.trees.operations.get_mut(&tree_op).expect("树停止存在");
        op.committed = true;
        op.report.committed = true;
        self.relations_commit_cleanup(&run, tree_op, actions);
        if claim.is_some() {
            self.trees.aux.committing = Some(thread.clone());
            self.release_with_kind(&thread, ReleaseKind::StopConfirmed, actions);
            self.trees.aux.committing = None;
        }
    }

    /// 协调计时：最早的派生重试与 15 秒周期核实共用一个可撤销计时。
    fn tree_schedule(&mut self, tree_op: u64, actions: &mut Vec<Action>) {
        let Some(work) = self.trees.aux.work.get(&tree_op) else {
            return;
        };
        let mut due = work.next_periodic_ms;
        for (key, d) in &self.trees.aux.derived {
            if key.0 == tree_op {
                if let Some(at) = d.retry_at_ms {
                    if d.stage == DerivedStage::Waiting {
                        due = due.min(at);
                    }
                }
            }
        }
        if work.tick.is_some_and(|(at, _)| at == due) {
            return;
        }
        let thread = work.root_thread.clone();
        let generation = self.trees.aux.next();
        let round = self.trees.operations[&tree_op].round;
        let work = self.trees.aux.work.get_mut(&tree_op).expect("协调进度存在");
        work.tick = Some((due, generation));
        actions.push(Action::Schedule {
            token: tree_token(&thread),
            kind: TimerKind::TreeStopVerify {
                tree_op,
                round,
                generation,
            },
            delay_ms: due.saturating_sub(self.clock.mono_ms),
        });
    }

    pub fn tree_timer(&mut self, kind: TimerKind, actions: &mut Vec<Action>) {
        match kind {
            TimerKind::TreeStopWait {
                tree_op,
                waiter,
                generation,
            } => {
                let Some(op) = self.trees.operations.get(&tree_op) else {
                    return;
                };
                if op.generation != generation || op.cancelled {
                    return;
                }
                match waiter {
                    Some(id) => self.tree_answer(tree_op, Some(id), "unconfirmed", actions),
                    None => {
                        // 独立首次交付期限：未确认就以 unconfirmed 交付；清理与屏障继续。
                        if let Some(work) = self.trees.aux.work.get_mut(&tree_op) {
                            work.deadline_passed = true;
                        }
                        let keys: Vec<(u64, String)> = self
                            .trees
                            .aux
                            .derived
                            .keys()
                            .filter(|k| k.0 == tree_op)
                            .cloned()
                            .collect();
                        for key in keys {
                            self.derived_report(&key, actions);
                        }
                        self.tree_drive(tree_op, actions);
                    }
                }
            }
            TimerKind::TreeStopVerify {
                tree_op,
                generation,
                ..
            } => {
                let Some(work) = self.trees.aux.work.get_mut(&tree_op) else {
                    return;
                };
                if work.tick.is_none_or(|(_, g)| g != generation) {
                    return;
                }
                work.tick = None;
                let now = self.clock.mono_ms;
                let periodic = work.next_periodic_ms <= now;
                if periodic {
                    work.next_periodic_ms = now + TREE_VERIFY_MS;
                }
                let due: Vec<(u64, String)> = self
                    .trees
                    .aux
                    .derived
                    .iter()
                    .filter(|(k, d)| {
                        k.0 == tree_op
                            && d.stage == DerivedStage::Waiting
                            && (d.retry_at_ms.is_some_and(|at| at <= now) || periodic && !d.stopped)
                    })
                    .map(|(k, _)| k.clone())
                    .collect();
                for key in due {
                    self.derived_requeue(&key);
                }
                // 周期性重新发现：上一次不完整、或已过首次期限仍未确认。
                let rediscover = {
                    let work = &self.trees.aux.work[&tree_op];
                    periodic
                        && work.discovery.is_none()
                        && !work.final_started
                        && work
                            .discovery_state
                            .as_ref()
                            .is_none_or(|d| !d.complete || work.deadline_passed)
                };
                if rediscover {
                    let (root, run, round) = {
                        let op = self.trees.operations.get_mut(&tree_op).expect("树停止存在");
                        op.round += 1;
                        (
                            op.request.snapshot.identity.logical_root_thread_id.clone(),
                            op.request.snapshot.identity.run_id.clone(),
                            op.round,
                        )
                    };
                    let id = self.begin_discovery(
                        &root,
                        DiscoveryScope::Run { run_id: run },
                        DiscoveryCaller::TreeStop { tree_op, round },
                        actions,
                    );
                    if let Some(work) = self.trees.aux.work.get_mut(&tree_op) {
                        work.discovery = (id != 0).then_some(id);
                    }
                }
                self.tree_pump(tree_op, actions);
                self.tree_drive(tree_op, actions);
            }
            _ => {}
        }
    }

    pub fn tree_reply(
        &mut self,
        context: &RequestContext,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        if self.closing {
            return;
        }
        self.derived_reply(context, result, actions);
    }

    /// 关闭：在途树操作静默取消；等待者按单线程停止的关闭规则回 unconfirmed，生命周期检查回关闭错误。
    pub fn trees_shutdown(&mut self, actions: &mut Vec<Action>) {
        let ops: Vec<u64> = self.trees.operations.keys().copied().collect();
        for tree_op in ops {
            self.tree_answer(tree_op, None, "unconfirmed", actions);
            actions.push(Action::Unschedule {
                key: TimerKey::TreeStopWait {
                    tree_op,
                    waiter: None,
                },
            });
            actions.push(Action::Unschedule {
                key: TimerKey::TreeStopVerify(tree_op),
            });
        }
        self.trees.operations.clear();
        self.trees.aux.work.clear();
        self.trees.aux.derived.clear();
        self.trees.aux.dirty.clear();
        self.runs.tree_retained.clear();
        for (_, check) in std::mem::take(&mut self.trees.aux.lifecycle) {
            actions.push(Action::Send {
                conn: check.request.conn,
                message: proto::response_err(
                    check.request.id,
                    proto::error_object(
                        crate::proto::code::GENERAL_ERROR,
                        crate::claim::SHUTTING_DOWN,
                        Some(json!({"reason": "shuttingDown"})),
                    ),
                ),
            });
        }
    }

    /// 忙判定：有树停止或生命周期检查在途时整体忙；给定线程时看它是否在某次树停止范围内。
    pub fn tree_busy(&self, thread: Option<&str>) -> bool {
        match thread {
            None => !self.trees.operations.is_empty() || !self.trees.aux.lifecycle.is_empty(),
            Some(thread) => self.tree_covers(thread),
        }
    }

    /// false 保留原 begin_stop 行为；运行有受管子树时，所有内部停止来源统一升级为整树停止。
    pub fn tree_stop_gate(
        &mut self,
        thread: &str,
        reason: crate::claim::StopReason,
        clear: bool,
        actions: &mut Vec<Action>,
    ) -> bool {
        if self.closing || self.trees.aux.bypass.as_deref() == Some(thread) {
            return false;
        }
        let Some(claim) = self.claims.get(thread) else {
            return false;
        };
        if claim.joined() || claim.settling() || claim.precheck() {
            return false;
        }
        let Some(run) = claim.run_id.clone() else {
            return false;
        };
        if let Some(tree_op) = self.tree_for_run(&run) {
            self.tree_merge(tree_op, clear, None, actions);
            return true;
        }
        if !self.tree_applies(&run) {
            return false;
        }
        let trigger = match reason {
            StopReason::Stop => TreeTrigger::UserStop,
            StopReason::ClientDisconnected => TreeTrigger::OwnerGone,
            StopReason::GoalLoop => TreeTrigger::LoopGuard,
        };
        let request = self.tree_request(
            thread,
            &run,
            TreeScope::Run {
                run_id: run.clone(),
            },
            trigger,
            clear,
            None,
        );
        self.begin_tree_stop(request, actions) != 0
    }

    /// 由当前认领与运行记录构造树停止请求。
    pub(crate) fn tree_request(
        &self,
        thread: &str,
        run: &str,
        scope: TreeScope,
        trigger: TreeTrigger,
        clear: bool,
        waiter: Option<TreeStopWaiter>,
    ) -> TreeStopRequest {
        let claim = self
            .claims
            .get(thread)
            .filter(|c| c.run_id.as_deref() == Some(run));
        let identity = self
            .manage_context(run)
            .map(|c| c.identity.clone())
            .map(|mut i| {
                i.execution_thread_id = thread.into();
                i
            })
            .unwrap_or_else(|| RunIdentity {
                run_id: run.into(),
                logical_root_thread_id: self
                    .runs
                    .runs
                    .get(run)
                    .map(|r| r.thread_id.clone())
                    .unwrap_or_else(|| thread.into()),
                execution_thread_id: thread.into(),
            });
        let record = self.runs.runs.get(run);
        let now = self.clock.mono_ms;
        let finishing = claim.is_some_and(|c| matches!(c.state, State::FinishingTree { .. }));
        TreeStopRequest {
            scope,
            snapshot: ReleaseSnapshot {
                identity,
                token: claim.map(|c| ClaimToken {
                    thread_id: thread.into(),
                    claim_id: c.claim_id,
                }),
                owner: claim.and_then(|c| c.owner().cloned()),
                gen: self.manage_context(run).map_or(0, |c| c.gen),
                parent_turn_id: claim.and_then(|c| c.turn_id()).map(str::to_owned),
                parent_status: None,
                goal_status: self.records.get(thread).and_then(|r| r.goal_status.clone()),
                loop_guard: record.is_some_and(|r| r.loop_guard.is_some()),
            },
            trigger,
            clear,
            root_already_idle: claim.is_none() || finishing,
            waiter,
            started_at_ms: now,
            first_delivery_deadline_ms: now + FIRST_DELIVERY_MS,
        }
    }
}

/// 专用提交的终态优先级（第 8.4 节）：父 failed → failed；父 interrupted → interrupted（保留已有
/// stop/clientDisconnected/goalLoop，否则 null）；其余有 loopGuard → interrupted/goalLoop；其余按树触发原因。
/// 树触发总带异常或停止原因，因此不会落到 goalBlocked 或父 completed。
pub(crate) fn commit_status(
    snapshot: &ReleaseSnapshot,
    trigger: TreeTrigger,
    existing: Option<InterruptReason>,
) -> (RunStatus, Option<InterruptReason>) {
    let kept = existing.filter(|r| {
        matches!(
            r,
            InterruptReason::Stop | InterruptReason::ClientDisconnected | InterruptReason::GoalLoop
        )
    });
    match snapshot.parent_status {
        Some(TurnStatus::Failed) => return (RunStatus::Failed, kept),
        Some(TurnStatus::Interrupted) => return (RunStatus::Interrupted, kept),
        _ => {}
    }
    if snapshot.loop_guard {
        return (RunStatus::Interrupted, Some(InterruptReason::GoalLoop));
    }
    match trigger {
        TreeTrigger::ParentFinishedEarly => (
            RunStatus::Interrupted,
            Some(InterruptReason::SubagentsUnfinished),
        ),
        TreeTrigger::UserStop => (RunStatus::Interrupted, Some(InterruptReason::Stop)),
        TreeTrigger::OwnerGone => (
            RunStatus::Interrupted,
            Some(InterruptReason::ClientDisconnected),
        ),
        TreeTrigger::LoopGuard => (RunStatus::Interrupted, Some(InterruptReason::GoalLoop)),
        TreeTrigger::ParentFailed => (RunStatus::Failed, kept),
        TreeTrigger::ParentInterrupted => (RunStatus::Interrupted, kept),
    }
}

impl Machine {
    /// 只采用当前停止操作的证据，不用可能已过期的展示报告释放认领。
    fn tree_abandon_root(&self, tree_op: u64) -> RootRelease {
        let op = &self.trees.operations[&tree_op];
        let work = &self.trees.aux.work[&tree_op];
        let run = &op.request.snapshot.identity.run_id;
        let claim = self.claims.get(&work.root_thread);
        let stop = self
            .stops
            .operations
            .values()
            .find(|s| Some(s.generation) == work.root_stop_gen && s.thread == work.root_thread);
        let check = stop
            .filter(|s| s.connection_epoch == self.relations.conn_epoch)
            .map(|s| self.stop_check(s))
            .or_else(|| {
                self.trees
                    .aux
                    .derived
                    .get(&(tree_op, work.root_thread.clone()))
                    .filter(|d| d.connection_epoch == self.relations.conn_epoch)
                    .map(|d| StopCheck {
                        // 后台接口受限或仍在读取后台列表，不否定已经成立的轮次与 goal 证据。
                        goal_braked: d.goal_checked && d.goal_braked,
                        settled: (!d.slot || d.stage == DerivedStage::Background)
                            && d.interrupts_pending == 0,
                        ..d.check()
                    })
            });
        let claim_id = stop.and_then(|s| s.claim_id).or_else(|| {
            op.request
                .snapshot
                .token
                .as_ref()
                .filter(|t| t.thread_id == work.root_thread)
                .map(|t| t.claim_id)
        });
        let terminal = self.runs.runs.get(run).is_some_and(|r| {
            matches!(
                r.state,
                crate::runs::RunState::Ended | crate::runs::RunState::Lost
            )
        });
        let (released, reason) = if claim.is_some_and(|c| {
            c.run_id.as_ref() != Some(run)
                || Some(c.claim_id) != claim_id
                || c.joined()
                || c.settling()
        }) {
            (false, "根认领已变更或存在共享归属冲突，保留现有认领")
        } else if terminal && claim.is_none() {
            (true, "根运行此前已结束，根认领已释放")
        } else if check.as_ref().is_none_or(|c| !c.ownership) {
            (false, "根所有权证据不足，保留占用")
        } else if check.as_ref().is_none_or(|c| !c.turn_ended) {
            (false, "根轮次仍活动或缺少结束证据，保留占用")
        } else if self
            .stops
            .changes
            .values()
            .any(|c| c.thread == work.root_thread)
        {
            (false, "根请求未落定，保留占用")
        } else if check.as_ref().is_none_or(|c| !c.goal_braked)
            || self
                .records
                .get(&work.root_thread)
                .is_some_and(|r| r.goal_active)
        {
            (false, "根 goal 尚未确认制动，保留占用")
        } else if check.as_ref().is_none_or(|c| !c.settled)
            || claim.is_some_and(|c| c.request_pending)
        {
            (false, "根仍有未落定请求，保留占用")
        } else if terminal {
            (false, "根运行已结束但仍有认领，保留占用以免释放其他工作")
        } else {
            (true, "根轮次已结束且 goal 已制动，提交中断并释放根认领")
        };
        RootRelease {
            thread: work.root_thread.clone(),
            released,
            reason: reason.into(),
        }
    }

    /// SA-b：客户端树等待者必须在单线程 alreadyStopped 快捷出口之前登记。
    pub(crate) fn tree_abandoned(&self, run: &str) -> bool {
        self.runs
            .runs
            .get(run)
            .and_then(|r| r.children_stop.as_ref())
            .is_some_and(|r| r.cleanup_state == "abandoned")
    }

    /// 弃管只撤树责任；未释放的自有结束快照恢复普通生命周期，不能指向已删除的协调器。
    fn tree_abandon_restore_root(&mut self, tree_op: u64) {
        let op = &self.trees.operations[&tree_op];
        let thread = &self.trees.aux.work[&tree_op].root_thread;
        let Some(claim) = self.claims.get(thread).filter(|c| {
            c.run_id.as_ref() == Some(&op.request.snapshot.identity.run_id)
                && op
                    .request
                    .snapshot
                    .token
                    .as_ref()
                    .is_some_and(|t| t.thread_id == *thread && t.claim_id == c.claim_id)
        }) else {
            return;
        };
        let State::FinishingTree {
            owner,
            tree_op: held_op,
            ..
        } = &claim.state
        else {
            return;
        };
        if *held_op != tree_op {
            return;
        }
        let owner = owner.clone();
        let active = self
            .trees
            .aux
            .derived
            .get(&(tree_op, thread.clone()))
            .into_iter()
            .flat_map(|d| d.target.proven_turns.iter())
            .chain(claim.owned_turns.iter())
            .find(|turn| {
                !claim.owned_ended.contains(turn)
                    && !self
                        .records
                        .get(thread)
                        .is_some_and(|r| r.ended_turns.contains(turn))
            })
            .cloned();
        self.claims.get_mut(thread).expect("已核对自有根认领").state = match active {
            Some(turn_id) => State::Running {
                owner,
                turn_id,
                joined: false,
            },
            None => State::AwaitingContinuation { owner },
        };
    }

    pub(crate) fn tree_abandon_request(
        &mut self,
        request: &ClientRequest,
        actions: &mut Vec<Action>,
    ) -> bool {
        if self.closing {
            return false;
        }
        let Some(p) = &request.params else {
            return false;
        };
        let abandon = p.get("abandon");
        if abandon.is_some_and(|v| !v.is_boolean())
            || (abandon == Some(&Value::Bool(true))
                && p.get("runId")
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty))
            || (abandon == Some(&Value::Bool(true))
                && p.get("clear").is_some_and(|v| v != &Value::Bool(false)))
        {
            super::stop_error(
                request.clone(),
                proto::code::INVALID_PARAMS,
                "--abandon 必须带 --run，不能与 --clear 同用；abandon 必须是布尔值",
                None,
                actions,
            );
            return true;
        }
        if p.get("runId")
            .is_some_and(|v| v.as_str().is_none_or(str::is_empty))
        {
            return false;
        }
        let Some(thread) = p.get("threadId").and_then(Value::as_str) else {
            return false;
        };
        let run = p
            .get("runId")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| self.latest_run(thread).map(|r| r.run_id.clone()));
        let valid = run
            .as_ref()
            .and_then(|r| self.runs.runs.get(r))
            .is_some_and(|r| {
                r.thread_id == thread || r.review_thread_id.as_deref() == Some(thread)
            });
        let abandoned = valid && run.as_ref().is_some_and(|r| self.tree_abandoned(r));
        if abandon != Some(&Value::Bool(true)) && !abandoned {
            return false;
        }
        let tree = run
            .as_ref()
            .filter(|_| valid)
            .and_then(|r| self.tree_for_run(r));
        let mut applied = false;
        let reason;
        let mut report = run
            .as_ref()
            .filter(|_| valid)
            .and_then(|r| self.runs.runs.get(r))
            .and_then(|r| r.children_stop.clone());
        if abandoned {
            reason = "alreadyAbandoned";
        } else if let Some(tree_op) = tree {
            let root_release = self.tree_abandon_root(tree_op);
            let release = root_release.released;
            let op = self.trees.operations.get_mut(&tree_op).expect("树停止存在");
            op.report.children.root_release = Some(root_release);
            op.report.children.phase = TreePhase::Unconfirmed;
            op.report.children.cleanup_state = "abandoned".into();
            op.report.children.abandon_reason =
                Some("用户显式放弃本次树清理责任；可能仍有子代理或命令在运行".into());
            op.report.children.abandoned_at = Some(self.clock.now_ms);
            op.report.children.version += 1;
            self.tree_publish(tree_op, actions);
            if release {
                self.tree_commit(tree_op, actions);
            } else {
                self.tree_abandon_restore_root(tree_op);
            }
            report = Some(self.trees.operations[&tree_op].report.children.clone());
            self.trees
                .operations
                .get_mut(&tree_op)
                .expect("树停止存在")
                .cancelled = true;
            self.tree_cancel_work(tree_op, actions);
            self.tree_answer(tree_op, None, "unconfirmed", actions);
            self.tree_finish(tree_op, actions);
            applied = true;
            reason = "abandoned";
        } else if report
            .as_ref()
            .is_some_and(|r| r.phase == TreePhase::Confirmed)
        {
            reason = "alreadyConfirmed";
        } else {
            reason = "treeStopNotFound";
        }
        actions.push(Action::Send { conn: request.conn, message: proto::response_ok(request.id.clone(), json!({
            "threadId": thread, "runId": run, "stopOutcome": "unconfirmed", "actionApplied": applied,
            "reason": reason, "interruptSent": false, "goal": "unchanged", "alreadyStopped": false,
            "childrenStop": report, "run": run.as_deref().and_then(|r| self.run_snapshot(r))
        })) });
        true
    }

    /// 有受管子树（或已有树停止）的运行一律合并进整树停止；否则交回原路径（含参数与状态错误）。
    pub fn tree_client_stop(&mut self, request: &ClientRequest, actions: &mut Vec<Action>) -> bool {
        if self.closing {
            return false;
        }
        let params = request.params.as_ref();
        let Some(thread) = params
            .and_then(|p| p.get("threadId"))
            .and_then(Value::as_str)
            .map(str::to_owned)
        else {
            return false;
        };
        let clear = params.and_then(|p| p.get("clear"));
        let expected = params.and_then(|p| p.get("runId"));
        if clear.is_some_and(|v| !v.is_boolean())
            || expected.is_some_and(|v| v.as_str().is_none_or(str::is_empty))
        {
            return false;
        }
        let clear = clear == Some(&Value::Bool(true));
        let expected = expected.and_then(Value::as_str).map(str::to_owned);
        if self.lifecycle_error(&thread).is_some() {
            return false;
        }
        let Some((run, root)) = self.client_tree_run(&thread, expected.as_deref()) else {
            return false;
        };
        if !self.tree_applies(&run) {
            return false;
        }
        if let Some(claim) = self.claims.get(&root) {
            if claim.run_id.as_deref() == Some(run.as_str())
                && (claim.settling() || claim.joined() || claim.precheck())
            {
                return false;
            }
        }
        let now = self.clock.mono_ms;
        let waiter = TreeStopWaiter {
            id: self.trees.aux.next(),
            request: request.clone(),
            entered_at_ms: now,
            deadline_ms: now + FIRST_DELIVERY_MS,
        };
        if let Some(tree_op) = self.tree_for_run(&run) {
            self.tree_merge(tree_op, clear, Some(waiter), actions);
            return true;
        }
        let scope = match &expected {
            Some(run) => TreeScope::Run {
                run_id: run.clone(),
            },
            None => TreeScope::Thread {
                logical_root: self
                    .manage_context(&run)
                    .map(|c| c.identity.logical_root_thread_id.clone())
                    .unwrap_or_else(|| thread.clone()),
            },
        };
        let request = self.tree_request(
            &root,
            &run,
            scope,
            TreeTrigger::UserStop,
            clear,
            Some(waiter),
        );
        self.begin_tree_stop(request, actions) != 0
    }

    /// 客户端停止要作用的运行与实际执行线程，口径是 client_stop 定位规则的超集。
    fn client_tree_run(&self, thread: &str, expected: Option<&str>) -> Option<(String, String)> {
        if let Some(run) = expected {
            let belongs = self.runs.runs.get(run).is_some_and(|r| {
                r.thread_id == thread || r.review_thread_id.as_deref() == Some(thread)
            });
            if let Some((id, _)) = self.claims.iter().find(|(id, c)| {
                c.run_id.as_deref() == Some(run) && (belongs || id.as_str() == thread)
            }) {
                return Some((run.into(), id.clone()));
            }
            if let Some(tree_op) = self.tree_for_run(run) {
                let root = self.trees.aux.work.get(&tree_op)?.root_thread.clone();
                return Some((run.into(), root));
            }
            if belongs
                && !self.claims.contains_key(thread)
                && self.latest_run(thread).is_none_or(|r| r.run_id == run)
            {
                let record = &self.runs.runs[run];
                let actual = record.review_thread_id.as_deref().unwrap_or(thread);
                if !self.claims.contains_key(actual) {
                    return Some((run.into(), actual.into()));
                }
            }
            return None;
        }
        if let Some(run) = self.claims.get(thread).and_then(|c| c.run_id.clone()) {
            return Some((run, thread.into()));
        }
        if let Some(op) = self.trees.operations.values().find(|op| {
            !op.cancelled
                && (op.request.snapshot.identity.logical_root_thread_id == thread
                    || self
                        .trees
                        .aux
                        .work
                        .get(&op.tree_op)
                        .is_some_and(|w| w.root_thread == thread))
        }) {
            let root = self.trees.aux.work.get(&op.tree_op)?.root_thread.clone();
            return Some((op.request.snapshot.identity.run_id.clone(), root));
        }
        // 直接停止树停止范围内的子线程：合并进那次树停止，不按无认领线程误报已停止。
        if let Some((tree_op, _)) = self
            .trees
            .aux
            .derived
            .keys()
            .find(|(op, member)| member == thread && self.trees.operations.contains_key(op))
        {
            let op = &self.trees.operations[tree_op];
            let root = self.trees.aux.work.get(tree_op)?.root_thread.clone();
            return Some((op.request.snapshot.identity.run_id.clone(), root));
        }
        let latest = self.latest_run(thread)?;
        if (latest.children_stop.is_some() || !latest.children.is_empty())
            && !self.claims.contains_key(thread)
        {
            return Some((
                latest.run_id.clone(),
                latest
                    .review_thread_id
                    .clone()
                    .unwrap_or_else(|| thread.into()),
            ));
        }
        let child = latest.review_thread_id.as_ref()?;
        let claim = self.claims.get(child)?;
        (claim.run_id.as_deref() == Some(latest.run_id.as_str()))
            .then(|| (latest.run_id.clone(), child.clone()))
    }

    /// SA-b：普通释放不能绕过已接管的树提交；关闭与专用提交分别收束。joined 只撤权。
    pub fn tree_release_gate(
        &mut self,
        thread: &str,
        kind: crate::runs::ReleaseKind,
        actions: &mut Vec<Action>,
    ) -> bool {
        if self.closing || kind == ReleaseKind::Shutdown {
            return false;
        }
        if self.trees.aux.committing.as_deref() == Some(thread) {
            return false;
        }
        let Some(run) = self.claims.get(thread).and_then(|c| c.run_id.clone()) else {
            return false;
        };
        let Some(tree_op) = self.tree_for_run(&run) else {
            return false;
        };
        if self.trees.operations[&tree_op].committed {
            return false;
        }
        if kind == ReleaseKind::Joined {
            self.tree_cancel(tree_op, "根运行改为 joined", false, actions);
            return false;
        }
        actions.push(Action::Debug(format!(
            "线程 {thread} 的释放（{kind:?}）交给整树停止 {tree_op} 统一提交"
        )));
        true
    }
}

#[cfg(test)]
#[path = "sab_tests/mod.rs"]
mod sab_tests;
