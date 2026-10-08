//! SA-b 自然结束门禁、结束快照（`FinishingTree`）的迟到事件，以及新工作与生命周期门禁
//! （接口第 8.2、10 节）。
//!
//! - 自然释放之前：运行仍有受管活动（some 或 unknown）就 Hold——认领转为 `FinishingTree`，运行保持
//!   running，以触发原因发起只停子树的整树停止，由树协调器在报告产生后统一提交结束。父轮 failed 或
//!   interrupted 的调用先于 goal active 分支，Hold 也阻断异常后的自动续跑。
//! - joined 与外部接管不 Hold，只撤权（SA-a 的 `on_handoff`）。
//! - 新工作门禁：根屏障范围内拒绝新运行、续跑与对成员的开工；`canAcceptDirectInput=false` 的子线程拒绝
//!   直接输入，true 但仍在活动绑定或属于未提交结束的运行时拒绝，未知交给上游。
//! - 生命周期门禁：rm、purge 的安全检查集合（子树、发现不完整、目标正被使用），拒绝时逐条列原因。
use super::{Action, ClientRequest, Machine, State};
use crate::proto::{self, code};
use crate::relations::{
    Activity, Authority, BindingStatus, DiscoveryCaller, DiscoveryScope, MemberActivity,
    RelationKind, RunIdentity,
};
use crate::runs::{ReleaseKind, TurnStatus};
use crate::stop::tree::{LifecycleCheck, TreeScope, TreeTrigger};
use serde_json::{json, Value};
use std::collections::BTreeSet;

/// 生命周期检查期间血缘变化时最多重查的次数。
pub const LIFECYCLE_ATTEMPTS: u64 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseDecision {
    Release,
    Hold,
}

impl Machine {
    /// 父认领自然释放之前的门禁：两条自然释放路径（父轮完成、两轮之间 goal 不再 active）都经过这里。
    pub fn natural_release_gate(
        &mut self,
        thread: &str,
        goal_status: Option<&str>,
        turn_status: Option<TurnStatus>,
        actions: &mut Vec<Action>,
    ) -> ReleaseDecision {
        if self.closing {
            return ReleaseDecision::Release;
        }
        let Some(claim) = self.claims.get(thread) else {
            return ReleaseDecision::Release;
        };
        // joined、settling、预检都不是本运行的自有收尾；joined 与外部接管只撤权。
        if claim.joined() || claim.settling() || claim.precheck() {
            return ReleaseDecision::Release;
        }
        if matches!(claim.state, State::FinishingTree { .. }) {
            return ReleaseDecision::Hold;
        }
        let Some(run) = claim.run_id.clone() else {
            return ReleaseDecision::Release;
        };
        // 已有停止（单线程或整树）的运行按停止路径收尾；树协调器的释放门禁另行把关。
        if self.tree_for_run(&run).is_some() || self.stop_busy(thread) {
            return ReleaseDecision::Release;
        }
        if self.has_managed_activity(&run) == Activity::None {
            return ReleaseDecision::Release;
        }
        let trigger = match turn_status {
            Some(TurnStatus::Failed) => TreeTrigger::ParentFailed,
            Some(TurnStatus::Interrupted) => TreeTrigger::ParentInterrupted,
            _ => TreeTrigger::ParentFinishedEarly,
        };
        let owner = claim.owner().cloned().expect("非空闲认领有 owner");
        let turn_id = claim.turn_id().filter(|t| !t.is_empty()).map(str::to_owned);
        let identity = self
            .manage_context(&run)
            .map(|c| RunIdentity {
                execution_thread_id: thread.into(),
                ..c.identity.clone()
            })
            .unwrap_or_else(|| RunIdentity {
                run_id: run.clone(),
                logical_root_thread_id: self
                    .runs
                    .runs
                    .get(&run)
                    .map_or_else(|| thread.to_owned(), |r| r.thread_id.clone()),
                execution_thread_id: thread.into(),
            });
        let gen = self.manage_context(&run).map_or(0, |c| c.gen);
        actions.push(Action::Debug(format!(
            "线程 {thread} 的父轮已结束但子代理仍有活动，暂缓释放并停止子树（{trigger:?}）"
        )));
        // 认领先转入结束快照：运行保持 running，迟到事件交协调器，不恢复 Running。
        let claim = self.claims.get_mut(thread).expect("已核对认领");
        claim.state = State::FinishingTree {
            owner,
            turn_id,
            parent_status: turn_status,
            goal_status: goal_status.map(str::to_owned),
            identity,
            gen,
            tree_op: 0,
            release_kind: ReleaseKind::Natural,
        };
        let mut request = self.tree_request(
            thread,
            &run,
            TreeScope::Run {
                run_id: run.clone(),
            },
            trigger,
            false,
            None,
        );
        request.root_already_idle = true;
        request.snapshot.parent_status = turn_status;
        request.snapshot.goal_status = goal_status.map(str::to_owned);
        let tree_op = self.begin_tree_stop(request, actions);
        if let Some(State::FinishingTree { tree_op: slot, .. }) =
            self.claims.get_mut(thread).map(|c| &mut c.state)
        {
            *slot = tree_op;
        }
        ReleaseDecision::Hold
    }

    /// 结束快照期间的迟到 started（根上出现新轮次）或重复 completed：交协调器，不覆盖父 turn_id、不释放。
    pub fn finishing_tree_turn(
        &mut self,
        thread: &str,
        turn: &str,
        started: bool,
        actions: &mut Vec<Action>,
    ) {
        let Some(tree_op) = self.finishing_tree_op(thread) else {
            actions.push(Action::Warn(format!(
                "线程 {thread} 处于结束快照但没有对应的整树停止"
            )));
            return;
        };
        let key = (tree_op, thread.to_owned());
        self.invalidate_finishing_root(tree_op, thread);
        if let Some(op) = self.trees.aux.derived.get_mut(&key) {
            if started && !turn.is_empty() {
                // 结束快照持有线程：期间开始的轮次属于本运行，可以中断。
                op.target.proven_turns.insert(turn.into());
            }
            op.stopped = false;
            if started {
                op.turn_ended = false;
            }
        }
        if started {
            if let Some(run) = self.claims.get(thread).and_then(|c| c.run_id.clone()) {
                if !turn.is_empty() {
                    self.run_turn_started(&run, thread, turn, actions);
                }
            }
        }
        self.derived_requeue(&key);
        self.tree_drive(tree_op, actions);
    }

    /// 结束快照期间 goal 变化：只记录事实并交协调器（重新 active 时再制动），不自动转 AwaitingContinuation。
    pub fn finishing_tree_goal(&mut self, thread: &str, active: bool, actions: &mut Vec<Action>) {
        let Some(tree_op) = self.finishing_tree_op(thread) else {
            return;
        };
        if let Some(State::FinishingTree { goal_status, .. }) =
            self.claims.get_mut(thread).map(|c| &mut c.state)
        {
            *goal_status = self.records.get(thread).and_then(|r| r.goal_status.clone());
        }
        if !active {
            return;
        }
        let key = (tree_op, thread.to_owned());
        self.invalidate_finishing_root(tree_op, thread);
        if let Some(op) = self.trees.aux.derived.get_mut(&key) {
            op.goal_checked = false;
            op.goal_braked = false;
            op.stopped = false;
        }
        self.derived_requeue(&key);
        self.tree_drive(tree_op, actions);
    }

    fn invalidate_finishing_root(&mut self, tree_op: u64, thread: &str) {
        if let Some(work) = self.trees.aux.work.get_mut(&tree_op) {
            work.root_done = false;
            if work.final_started {
                work.final_pending.insert(thread.into());
            }
        }
    }

    fn finishing_tree_op(&self, thread: &str) -> Option<u64> {
        let claim = self.claims.get(thread)?;
        let State::FinishingTree { tree_op, .. } = &claim.state else {
            return None;
        };
        let run = claim.run_id.as_deref()?;
        self.tree_for_run(run)
            .or(Some(*tree_op).filter(|op| self.trees.operations.contains_key(op)))
    }
}

/// 子线程的直接输入与新工作需要被门禁检查的方法。
fn new_work(method: &str) -> bool {
    matches!(
        method,
        "thread/resume" | "turn/start" | "review/start" | "turn/steer"
    )
}

impl Machine {
    /// 新工作门禁（true 表示已回错误）：根屏障、子线程能力与占用。
    pub fn subagent_work_gate(
        &mut self,
        request: &super::ClientRequest,
        actions: &mut Vec<Action>,
    ) -> bool {
        if !new_work(&request.method) || self.closing {
            return false;
        }
        let Some(thread) = request
            .params
            .as_ref()
            .and_then(|p| p.get("threadId"))
            .and_then(Value::as_str)
            .map(str::to_owned)
        else {
            return false;
        };
        // 第 1 步的根屏障：本 broker 不再接受该范围的新工作（新运行、续跑、对成员的开工）。
        if self.tree_busy(Some(&thread)) {
            reject(
                request,
                code::THREAD_BUSY,
                "子代理树的停止尚未确认，暂不接受新的工作；请先 cxb wait 或 cxb stop 核实",
                json!({"reason": "treeStopping", "threadId": thread}),
                actions,
            );
            return true;
        }
        if request.method == "turn/steer" {
            return false;
        }
        let Some(edge) = self
            .thread_relation(&thread)
            .filter(|e| e.kind == RelationKind::Subagent)
        else {
            return false;
        };
        match edge.can_accept_direct_input {
            Some(false) => {
                reject(
                    request,
                    code::GENERAL_ERROR,
                    "子代理线程不接受直接输入，请从父线程继续",
                    json!({"reason": "subagentNoDirectInput", "threadId": thread,
                        "parentThreadId": edge.parent_thread_id}),
                    actions,
                );
                true
            }
            Some(true) => {
                let Some(run) = self.subagent_in_use(&thread) else {
                    return false;
                };
                reject(
                    request,
                    code::THREAD_BUSY,
                    "子代理线程仍在活动或属于尚未结束的运行，请先 cxb wait 或 cxb stop",
                    json!({"reason": "subagentBusy", "threadId": thread, "runId": run,
                        "parentThreadId": edge.parent_thread_id}),
                    actions,
                );
                true
            }
            // 未知：交给上游校验；回包或后续 thread/read 带回能力后门禁随之更新。
            None => false,
        }
    }

    /// 子线程是否处于活动绑定、或属于尚未提交结束的运行；是则返回该运行。
    fn subagent_in_use(&self, thread: &str) -> Option<String> {
        for (run, context) in &self.relations.contexts {
            let Some(member) = context.members.get(thread) else {
                continue;
            };
            let unsubmitted = self
                .claims
                .values()
                .any(|c| c.run_id.as_deref() == Some(run))
                || self
                    .tree_for_run(run)
                    .is_some_and(|op| !self.trees.operations[&op].committed);
            let active_binding = context.authority != Authority::Revoked
                && (matches!(
                    member.activity,
                    MemberActivity::Running | MemberActivity::Starting
                ) || member.pending_requests > 0);
            if unsubmitted || active_binding {
                return Some(run.clone());
            }
        }
        let bound_open = self
            .relations
            .aux
            .observed
            .get(thread)
            .is_some_and(|turns| {
                turns.iter().any(|t| {
                    !t.completed
                        && self
                            .binding(thread, &t.turn)
                            .is_some_and(|b| b.status == BindingStatus::Bound)
                })
            });
        if bound_open {
            return self.relations.aux.observed.get(thread).and_then(|turns| {
                turns
                    .iter()
                    .find_map(|t| self.binding(thread, &t.turn).map(|b| b.run_id))
            });
        }
        None
    }

    /// 生命周期门禁（true 表示已接管：拒绝或等发现结论）。
    pub fn subagent_lifecycle_gate(
        &mut self,
        request: &super::ClientRequest,
        actions: &mut Vec<Action>,
    ) -> bool {
        if !lifecycle_method(request) || self.closing {
            return false;
        }
        let Some(thread) = request
            .params
            .as_ref()
            .and_then(|p| p.get("threadId"))
            .and_then(Value::as_str)
            .map(str::to_owned)
        else {
            return false;
        };
        let key = (request.conn, crate::proto::request_id_key(&request.id));
        if self.trees.aux.lifecycle_cleared.as_ref() == Some(&key) {
            self.trees.aux.lifecycle_cleared = None;
            return false;
        }
        let blockers = self.subagent_blockers(&thread);
        if !blockers.is_empty() {
            reject_blockers(request, &thread, blockers, actions);
            return true;
        }
        let server_effect = request.method != "broker/threads/purge"
            || request
                .params
                .as_ref()
                .is_some_and(|p| p["serverDelete"] == true);
        if !server_effect && !self.lifecycle_needs_discovery(&thread) {
            return false;
        }
        // 本地没有血缘不等于服务器子树为空；有服务器副作用时必须先发现，再判定。
        self.lifecycle_discover(request.clone(), thread, 1, actions);
        true
    }

    fn lifecycle_discover(
        &mut self,
        request: ClientRequest,
        thread: String,
        attempt: u64,
        actions: &mut Vec<Action>,
    ) {
        let op = self.begin_discovery(
            &thread,
            DiscoveryScope::Subtree,
            DiscoveryCaller::Lifecycle {
                request: request.clone(),
                attempt,
            },
            actions,
        );
        if op == 0 {
            reject(
                &request,
                code::GENERAL_ERROR,
                crate::claim::SHUTTING_DOWN,
                json!({"reason": "shuttingDown"}),
                actions,
            );
            return;
        }
        self.trees.aux.lifecycle.insert(
            op,
            LifecycleCheck {
                request,
                thread,
                attempt,
            },
        );
    }

    /// 每次迁移末尾取回生命周期检查的发现结论：有子树或不完整就逐条拒绝，否则放行原请求。
    pub(crate) fn subagent_lifecycle_poll(&mut self, actions: &mut Vec<Action>) {
        if self.closing || self.trees.aux.lifecycle.is_empty() {
            return;
        }
        let done: Vec<u64> = self
            .trees
            .aux
            .lifecycle
            .keys()
            .copied()
            .filter(|op| {
                self.discovery_outcome(*op).is_some()
                    || self
                        .relations
                        .discoveries
                        .get(op)
                        .is_none_or(|d| d.completed)
            })
            .collect();
        for op in done {
            let Some(check) = self.trees.aux.lifecycle.remove(&op) else {
                continue;
            };
            let outcome = self.discovery_outcome(op).cloned();
            let mut blockers = self.subagent_blockers(&check.thread);
            match outcome {
                Some(result) => {
                    let children: Vec<String> = result
                        .members
                        .iter()
                        .filter(|(t, e)| **t != check.thread && e.kind == RelationKind::Subagent)
                        .map(|(t, _)| t.clone())
                        .collect();
                    if !children.is_empty() && !blockers.iter().any(|b| b["reason"] == "hasSubtree")
                    {
                        blockers.push(json!({"threadId": check.thread, "runId": null,
                            "reason": "hasSubtree", "children": children}));
                    }
                    if !result.state.complete {
                        blockers.push(json!({"threadId": check.thread, "runId": null,
                            "reason": "discoveryIncomplete",
                            "detail": result.state.incomplete_reasons}));
                    }
                }
                None => blockers.push(json!({"threadId": check.thread, "runId": null,
                    "reason": "discoveryIncomplete", "detail": ["发现已取消"]})),
            }
            if blockers.is_empty() {
                // 检查通过：原请求重新分派，门禁只放行这一次。
                self.trees.aux.lifecycle_cleared = Some((
                    check.request.conn,
                    crate::proto::request_id_key(&check.request.id),
                ));
                self.client_request(check.request, None, actions);
                self.trees.aux.lifecycle_cleared = None;
            } else {
                reject_blockers(&check.request, &check.thread, blockers, actions);
            }
        }
    }

    /// 检查期间血缘变化：在途检查重查一次；再变则中止并报告。
    pub(crate) fn lifecycle_lineage_changed(
        &mut self,
        logical_root: &str,
        actions: &mut Vec<Action>,
    ) {
        let affected: Vec<u64> = self
            .trees
            .aux
            .lifecycle
            .iter()
            .filter(|(_, c)| {
                c.thread == logical_root || self.logical_root_of(&c.thread) == logical_root
            })
            .map(|(op, _)| *op)
            .collect();
        for op in affected {
            let Some(check) = self.trees.aux.lifecycle.remove(&op) else {
                continue;
            };
            if check.attempt >= LIFECYCLE_ATTEMPTS {
                reject_blockers(
                    &check.request,
                    &check.thread,
                    vec![
                        json!({"threadId": check.thread, "runId": null, "reason": "relationsChanged"}),
                    ],
                    actions,
                );
                continue;
            }
            self.lifecycle_discover(check.request, check.thread, check.attempt + 1, actions);
        }
    }

    /// 安全检查集合（已知事实）：目标自身是否子代理及所属运行、目标子树、两者的待答与未确认停止、
    /// FinishingTree、发现不完整。fork 与 reviewExecution 不纳入。
    fn subagent_blockers(&self, thread: &str) -> Vec<Value> {
        let mut blockers = Vec::new();
        if let Some(run) = self.subagent_in_use(thread) {
            blockers.push(json!({"threadId": thread, "runId": run, "reason": "subagentInUse"}));
        }
        let children: BTreeSet<String> = self
            .relations
            .edges
            .values()
            .filter(|e| e.parent_thread_id == thread && e.kind == RelationKind::Subagent)
            .map(|e| e.child_thread_id.clone())
            .chain(
                self.runs
                    .index
                    .threads
                    .values()
                    .filter(|e| {
                        e.parent_thread_id.as_deref() == Some(thread)
                            && e.relation_kind.is_none_or(|k| k == RelationKind::Subagent)
                    })
                    .map(|e| e.thread_id.clone()),
            )
            .collect();
        if !children.is_empty() {
            blockers.push(
                json!({"threadId": thread, "runId": null, "reason": "hasSubtree",
                "children": children}),
            );
        }
        for (run, context) in &self.relations.contexts {
            if context.identity.logical_root_thread_id != thread
                && context.identity.execution_thread_id != thread
                && !context.members.contains_key(thread)
            {
                continue;
            }
            if let Some(tree_op) = self.tree_for_run(run) {
                blockers.push(
                    json!({"threadId": thread, "runId": run, "reason": "treeStopUnconfirmed",
                    "treeOp": tree_op}),
                );
            }
            if self.inbox_pending_run(run) > 0
                && (context.identity.logical_root_thread_id == thread
                    || context.members.contains_key(thread))
                && context.members.values().any(|m| m.pending_requests > 0)
            {
                blockers.push(
                    json!({"threadId": thread, "runId": run, "reason": "childRequestsPending"}),
                );
            }
            if (context.identity.logical_root_thread_id == thread)
                && !context.members.is_empty()
                && !context.discovery.complete
                && !context.discovery.incomplete_reasons.is_empty()
            {
                blockers.push(
                    json!({"threadId": thread, "runId": run, "reason": "discoveryIncomplete",
                    "detail": context.discovery.incomplete_reasons}),
                );
            }
        }
        if self
            .claims
            .get(thread)
            .is_some_and(|c| matches!(c.state, State::FinishingTree { .. }))
            && !blockers
                .iter()
                .any(|b| b["reason"] == "treeStopUnconfirmed")
        {
            blockers.push(
                json!({"threadId": thread, "runId": self.claims[thread].run_id,
                "reason": "treeStopUnconfirmed"}),
            );
        }
        blockers
    }

    /// 已知事实未挡住时，目标是子代理（其后代可能未被本 broker 观察到）或其运行记录过子线程，才向上游发现。
    fn lifecycle_needs_discovery(&self, thread: &str) -> bool {
        self.thread_relation(thread)
            .is_some_and(|e| e.kind == RelationKind::Subagent)
            || self.runs.runs.values().any(|r| {
                (r.thread_id == thread || r.review_thread_id.as_deref() == Some(thread))
                    && !r.children.is_empty()
            })
    }
}

fn lifecycle_method(request: &ClientRequest) -> bool {
    matches!(
        request.method.as_str(),
        "broker/threads/archive" | "thread/archive" | "thread/delete" | "broker/threads/purge"
    )
}

fn reject(
    request: &ClientRequest,
    code: i64,
    message: &str,
    data: Value,
    actions: &mut Vec<Action>,
) {
    actions.push(Action::Send {
        conn: request.conn,
        message: proto::response_err(
            request.id.clone(),
            proto::error_object(code, message, Some(data)),
        ),
    });
}

/// 与现有归档、删除阻塞项同一形状（`threadBusy` + `data.blockers`），CLI 逐条列出并退出 8。
fn reject_blockers(
    request: &ClientRequest,
    thread: &str,
    blockers: Vec<Value>,
    actions: &mut Vec<Action>,
) {
    reject(
        request,
        code::THREAD_BUSY,
        "线程属于子代理树或子代理仍在使用，未执行",
        json!({"reason": "threadBusy", "threadId": thread, "blockers": blockers}),
        actions,
    );
}
