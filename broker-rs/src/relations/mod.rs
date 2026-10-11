//! 子代理关系、活动归属、子线程服务端请求路由与按运行聚合（SA-a，接口第 1–6、9、12 节）。
//!
//! 只提供事实，不决定是否停止：停止、门禁与展示归 SA-b。三类关系分开保存：
//! - 血缘：`edges`（来自 thread/started、thread/read、thread/list 与父派生条目）；
//! - 活动归属：`bindings`，按 (线程, 轮次) 记，只有父自有轮次的派生证据能授予；
//! - 管理权：`contexts`，按运行记，代际只决定谁现在有权回答。
//!
//! 证据期限、回查期限和墓碑用单调毫秒（`Clock::mono_ms`）；请求的收到与截止时间沿用墙钟毫秒。
pub mod types;

pub mod convergence;
mod discovery;
mod facts;
mod notify;
pub mod results;
mod routing;
#[cfg(test)]
mod tests;

use crate::claim::{Action, ClaimToken, HeldNotification, HeldRequest, Machine, RequestContext};
use crate::claim::{Operation, TimerKind};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
pub use types::*;

/// 派生证据有效期：首次观察起最多 30 秒，父轮结束、撤权、连接换代时提前失效。
pub const EVIDENCE_TTL_MS: u64 = 30_000;
/// 回查队列：总期限、并发、全局与每线程上限、总载荷、重试次数与首次退避。
pub const LOOKUP_DEADLINE_MS: u64 = 30_000;
pub const LOOKUP_CONCURRENCY: usize = 8;
pub const LOOKUP_MAX_REQUESTS: usize = 256;
pub const LOOKUP_PER_THREAD: usize = 16;
pub const LOOKUP_MAX_BYTES: usize = 8 * 1024 * 1024;
pub const LOOKUP_RETRIES: u32 = 3;
pub const LOOKUP_BACKOFF_MS: u64 = 500;
/// 墓碑：按连接 epoch 记，保留 120 秒、最多 4096 条。
pub const TOMBSTONE_MS: u64 = 120_000;
pub const MAX_TOMBSTONES: usize = 4096;
/// 发现：期限 20 秒；根深度 0，最深 8 层；节点最多 256 个（不含根与 reviewExecution）。
pub const DISCOVERY_MS: u64 = 20_000;
pub const MAX_DEPTH: usize = 8;
pub const MAX_MEMBERS: usize = 256;
pub const PAGE_LIMIT: u64 = 100;
pub const MAX_PAGES: usize = 20;
pub const DISCOVERY_READS: usize = 8;
/// 运行记录里每个子线程最多保留的轮次。
pub const MAX_TURN_IDS: usize = 100;
/// 无认领线程的轮次观察：每线程最近 16 轮，最多 1024 个线程。
pub const MAX_OBSERVED_TURNS: usize = 16;
pub const MAX_OBSERVED_THREADS: usize = 1024;
/// 证据操作记录上限；超出淘汰最早的已落定记录。
pub const MAX_OPERATIONS: usize = 1024;
/// 附着证据上限。
pub const MAX_ATTACHMENTS: usize = 1024;
/// 保留的发现结果上限（供 SA-b 按操作编号取回）。
pub const MAX_RESULTS: usize = 64;
/// 回查与证据到期共用的整体扫描计时（`TimerKind::LookupDeadline{op_id: 0}`）。
pub(crate) const SWEEP_OP: u64 = 0;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Relations {
    pub conn_epoch: u64,
    pub edges: BTreeMap<String, RelationEdge>,
    pub bindings: BTreeMap<(String, String), ActivityBinding>,
    pub contexts: BTreeMap<String, ManageContext>,
    pub operations: BTreeMap<EvidenceKey, EvidenceOperation>,
    pub discoveries: BTreeMap<u64, DiscoveryOperation>,
    pub requests: BTreeMap<(u64, String), ChildRequest>,
    pub tombstones: BTreeMap<(u64, String), u64>,
    pub attachments: BTreeMap<String, AttachmentEvidence>,
    /// SA-a 实现所需的内部登记（不属于冻结契约）：编号、观察、回查队列、发现进度。
    pub aux: RelationsAux,
}

/// 无认领线程上观察到的一轮。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnObservation {
    pub turn: String,
    pub started: bool,
    pub completed: bool,
    pub seen_ms: u64,
}

/// 证据操作还在等什么才能确定父轮归属。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParentWait {
    /// 父认领 settling，等 `on_claim_settled`。
    Settle { thread: String, claim_id: u64 },
    /// 父线程本身是子线程，等它这一轮的绑定结论（继承）。
    Binding { thread: String, turn: String },
}

/// 一个线程的回查状态：同一线程只有一个在途 thread/read。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lookup {
    pub op_id: u64,
    pub generation: u64,
    pub attempt: u32,
    pub in_flight: bool,
    /// 失败后等待重试；None 表示排队等名额或无需再读。
    pub retry_at_ms: Option<u64>,
    pub exhausted: bool,
}

/// 子线程请求的内部附加信息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestMeta {
    pub forward_id: String,
    /// 回查期限（单调毫秒）。
    pub lookup_deadline_ms: u64,
    pub bytes: usize,
    /// 已经过一次 Held 定案；动态工具按「定案后回放」放行转发。
    pub from_held: bool,
    /// 进收件箱后的事项编号（Writing 阶段失败时回到收件箱阶段用）。
    pub inbox_item: Option<String>,
    /// 已请求撤权；写入失败或取消后不得恢复旧回答权。
    pub revoke_requested: bool,
}

/// 等待归属结论的 Guardian 拒绝通知。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingGuardian {
    pub thread: String,
    pub turn: Option<String>,
    pub params: Value,
    pub deadline_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelationsAux {
    pub next_id: u64,
    /// 关系版本：任一血缘边增删改时加一，发现据此判断期间是否变化。
    pub version: u64,
    pub observed: BTreeMap<String, VecDeque<TurnObservation>>,
    pub observed_order: VecDeque<String>,
    /// 子线程 → 等它首轮的派生操作。
    pub spawn_waits: BTreeMap<String, EvidenceKey>,
    pub parent_waits: BTreeMap<EvidenceKey, ParentWait>,
    /// thread/read 证明没有父线程的线程。
    pub lineage_roots: BTreeSet<String>,
    pub lookups: BTreeMap<String, Lookup>,
    pub lookup_queue: VecDeque<String>,
    pub request_meta: BTreeMap<(u64, String), RequestMeta>,
    /// 当前整体扫描计时：(到期单调毫秒, 代号)。
    pub sweep: Option<(u64, u64)>,
    pub tombstone_timer: Option<(u64, u64)>,
    pub work: BTreeMap<u64, discovery::DiscoveryWork>,
    pub results: BTreeMap<u64, DiscoveryResult>,
    pub guardians: Vec<PendingGuardian>,
    /// (运行, 子线程) → 首个归属证据，用于运行记录 children。
    pub member_evidence: BTreeMap<(String, String), BindingEvidence>,
    /// 最近一次发现中已加载但没有附着证据的线程：活动状态未知。
    pub unobserved_loaded: BTreeSet<String>,
}

impl RelationsAux {
    pub(crate) fn next(&mut self) -> u64 {
        self.next_id = self.next_id.checked_add(1).expect("关系操作编号已耗尽");
        self.next_id
    }
}

/// 一次请求在关系上的归属结论。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    /// 已证明属于运行。
    Bound(String),
    /// 已有证据但不能归属（unbound 或 conflict），附运行（若有）与说明。
    Unmanaged(String),
    /// 已证明不在任何自有运行的血缘下。
    External,
    /// 还可能等到证据。
    Unknown,
}

impl Machine {
    // ─── 查询（接口第 7 节，SA-a） ───────────────────────────────────────

    pub fn run_subtree(&self, run_id: &str) -> Option<SubtreeView> {
        let context = self.relations.contexts.get(run_id)?;
        Some(SubtreeView {
            members: context.members.clone(),
            discovery: context.discovery.clone(),
            revision: context.revision,
            gen: context.gen,
        })
    }
    pub fn binding(&self, thread_id: &str, turn_id: &str) -> Option<ActivityBinding> {
        self.relations
            .bindings
            .get(&(thread_id.to_owned(), turn_id.to_owned()))
            .cloned()
    }
    pub fn manage_context(&self, run_id: &str) -> Option<&ManageContext> {
        self.relations.contexts.get(run_id)
    }
    /// 内存里的实时关系优先；重启后只从线程索引恢复血缘历史，不恢复绑定与回答权。
    pub fn thread_relation(&self, thread_id: &str) -> Option<RelationEdge> {
        if let Some(edge) = self.relations.edges.get(thread_id) {
            return Some(edge.clone());
        }
        let entry = self
            .runs
            .index
            .threads
            .values()
            .find(|e| e.thread_id == thread_id)?;
        Some(RelationEdge {
            child_thread_id: thread_id.into(),
            parent_thread_id: entry.parent_thread_id.clone()?,
            kind: entry.relation_kind.unwrap_or(RelationKind::Subagent),
            source_summary: entry.source_summary.clone().unwrap_or(Value::Null),
            can_accept_direct_input: entry.can_accept_direct_input,
            agent_version: entry.agent_version.unwrap_or(AgentVersion::Unknown),
            discovered_by: DiscoveredBy::Lookup,
        })
    }
    pub fn thread_capability(&self, thread_id: &str) -> Option<bool> {
        self.convergences
            .capabilities
            .get(thread_id)
            .filter(|(epoch, _)| *epoch == self.relations.conn_epoch)
            .map(|(_, capability)| *capability)
            .or_else(|| {
                self.thread_relation(thread_id)
                    .and_then(|r| r.can_accept_direct_input)
            })
    }
    /// 本运行是否已纳管子代理（有子代理成员或在途派生）。没有时，共享空转刹车与 goal 非活动
    /// 保持 SA1b 之前的语义（用户 2026-10-11 定：只在确有子代理要收尾时才按异常处理）。
    /// 证据包括：在途派生、子代理成员、关系表中挂在本执行线程下的子代理边（含尚停在
    /// pendingInit、未成为成员的子代理），以及收敛账本里待启动的项。
    pub fn run_has_subagents(&self, run_id: &str) -> bool {
        self.relations.contexts.get(run_id).is_some_and(|c| {
            !c.in_flight_spawns.is_empty()
                || c.members.values().any(|m| m.kind == RelationKind::Subagent)
                || self.relations.edges.values().any(|e| {
                    e.kind == RelationKind::Subagent
                        && e.parent_thread_id == c.identity.execution_thread_id
                })
        }) || self
            .convergences
            .ledgers
            .get(run_id)
            .is_some_and(|l| !l.pending.is_empty())
    }
    /// 给自然结束门禁用：活动成员、待答请求、在途派生、未决候选算 some；
    /// 成员上有未归属的活动轮次、或可能属于本运行的回查未决算 unknown。
    pub fn has_managed_activity(&self, run_id: &str) -> Activity {
        let Some(context) = self.relations.contexts.get(run_id) else {
            return Activity::Unknown;
        };
        let now = self.clock.mono_ms;
        let mut unknown = false;
        if !context.in_flight_spawns.is_empty() {
            return Activity::Some;
        }
        for (thread, member) in &context.members {
            if member.kind != RelationKind::Subagent {
                continue;
            }
            match member.activity {
                MemberActivity::Running | MemberActivity::Starting => return Activity::Some,
                MemberActivity::Unknown => unknown = true,
                MemberActivity::Settled => {}
            }
            if member.pending_requests > 0 {
                return Activity::Some;
            }
            if self
                .relations
                .aux
                .spawn_waits
                .get(thread)
                .is_some_and(|key| {
                    self.relations
                        .operations
                        .get(key)
                        .is_some_and(|op| op.run_id == run_id && self.evidence_valid(op, now))
                })
            {
                return Activity::Some;
            }
        }
        if self
            .relations
            .requests
            .values()
            .any(|r| r.run_id.as_deref() == Some(run_id) && routing::live_stage(&r.stage))
        {
            return Activity::Some;
        }
        if self.relations.requests.iter().any(|(_, r)| {
            r.stage == ChildRequestStage::Lookup
                && self.lookup_may_belong(run_id, &r.source_thread_id)
        }) {
            unknown = true;
        }
        if unknown {
            Activity::Unknown
        } else {
            Activity::None
        }
    }

    /// 发现结果按操作编号保留（最多 [`MAX_RESULTS`] 个），供调用方在 `on_member_changed` 后取回。
    pub fn discovery_outcome(&self, op_id: u64) -> Option<&DiscoveryResult> {
        self.relations.aux.results.get(&op_id)
    }

    // ─── 钩子与回调入口 ──────────────────────────────────────────────

    /// 0 为未登记的占位编号；正式操作编号从 1 开始。
    pub fn begin_discovery(
        &mut self,
        root: &str,
        scope: DiscoveryScope,
        caller: DiscoveryCaller,
        actions: &mut Vec<Action>,
    ) -> u64 {
        self.discovery_begin(root, scope, caller, actions)
    }
    pub fn discovery_done(
        &mut self,
        op_id: u64,
        caller: DiscoveryCaller,
        result: DiscoveryResult,
        actions: &mut Vec<Action>,
    ) {
        if let DiscoveryCaller::Converge { token, work_epoch } = caller {
            if self
                .convergences
                .progress
                .get(&token.run_id)
                .and_then(|p| p.discovery)
                .map(|p| p.0)
                == Some(op_id)
            {
                self.convergence_discovery_done(token, work_epoch, result, actions);
            }
        } else {
            self.discovery_apply(op_id, caller, result, actions);
        }
    }
    pub fn on_claim_settled(
        &mut self,
        thread: &str,
        claim_id: u64,
        own: bool,
        actions: &mut Vec<Action>,
    ) {
        self.facts_claim_settled(thread, claim_id, own, actions);
        self.reroute_held(thread, actions);
        self.ensure_sweep(actions);
    }
    pub fn on_handoff(&mut self, run_id: &str, kind: HandoffKind, actions: &mut Vec<Action>) {
        if matches!(kind, HandoffKind::Joined | HandoffKind::External) {
            self.convergence_authority_lost(run_id, actions);
        }
        self.routing_handoff(run_id, kind, actions)
    }
    pub fn on_unclaimed_turn(
        &mut self,
        thread: &str,
        turn: &str,
        started: bool,
        actions: &mut Vec<Action>,
    ) {
        if self.closing {
            return;
        }
        self.observe_turn(thread, turn, started, !started, actions);
    }
    pub fn on_relation_notification(
        &mut self,
        event: &HeldNotification,
        actions: &mut Vec<Action>,
    ) {
        if self.closing {
            return;
        }
        // 流式增量等无关通知不触发整体扫描的重排。
        if self.facts_notification(event, actions) {
            self.ensure_sweep(actions);
        }
    }
    /// false 表示交回原路由：来源线程自己有认领、或请求不带线程。
    pub fn route_child_request(
        &mut self,
        request: &HeldRequest,
        actions: &mut Vec<Action>,
    ) -> bool {
        let taken = self.routing_arrive(request, actions);
        if taken {
            self.ensure_sweep(actions);
        }
        taken
    }
    pub fn relation_reply(
        &mut self,
        context: &RequestContext,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        match &context.operation {
            Operation::RelationLookup {
                op_id,
                generation,
                thread,
            } => self.lookup_reply(*op_id, *generation, thread, result, actions),
            Operation::Discovery { op_id, generation } => {
                self.discovery_reply(*op_id, *generation, context, result, actions)
            }
            // SA1a 不发附着请求；迟到的兼容回包只记录。
            Operation::AttachThread { .. } => {
                actions.push(Action::Debug("忽略未启用的附着回包".into()))
            }
            _ => {}
        }
        self.ensure_sweep(actions);
    }
    pub fn relation_timer(&mut self, kind: TimerKind, actions: &mut Vec<Action>) {
        match kind {
            TimerKind::LookupDeadline { op_id, generation } => {
                if op_id == SWEEP_OP {
                    if self
                        .relations
                        .aux
                        .sweep
                        .is_some_and(|(_, g)| g == generation)
                    {
                        self.relations.aux.sweep = None;
                        self.sweep(actions);
                    }
                } else {
                    self.lookup_retry_timer(op_id, generation, actions);
                }
            }
            TimerKind::DiscoveryDeadline { op_id, generation } => {
                self.discovery_deadline(op_id, generation, actions)
            }
            TimerKind::TombstoneExpire { generation, .. } => {
                self.tombstone_timer(generation, actions)
            }
            _ => {}
        }
        self.ensure_sweep(actions);
    }
    /// 关闭：在途回查、发现静默取消；已证明自有的未决请求按关闭规则回错误，其余不答。
    pub fn relations_shutdown(&mut self, actions: &mut Vec<Action>) {
        self.routing_shutdown(actions);
        self.discovery_shutdown(actions);
        let aux = &mut self.relations.aux;
        aux.lookups.clear();
        aux.lookup_queue.clear();
        aux.guardians.clear();
        if aux.sweep.take().is_some() {
            actions.push(Action::Unschedule {
                key: crate::claim::TimerKey::LookupDeadline(SWEEP_OP),
            });
        }
        if aux.tombstone_timer.take().is_some() {
            actions.push(Action::Unschedule {
                key: crate::claim::TimerKey::TombstoneExpire(self.relations.conn_epoch),
            });
        }
    }
    pub fn child_origin(&self, thread: &str, turn: Option<&str>) -> Option<CxbOrigin> {
        let turn = match turn {
            Some(turn) => turn.to_owned(),
            None => self.unique_active_bound_turn(thread)?,
        };
        let binding = self.relations.bindings.get(&(thread.into(), turn))?;
        if binding.status != BindingStatus::Bound {
            return None;
        }
        let context = self.relations.contexts.get(&binding.run_id)?;
        let edge = self.thread_relation(thread);
        Some(CxbOrigin {
            root_thread_id: context.identity.logical_root_thread_id.clone(),
            run_id: binding.run_id.clone(),
            parent_thread_id: edge
                .as_ref()
                .map(|e| e.parent_thread_id.clone())
                .unwrap_or_else(|| binding.parent_thread_id.clone()),
            relation_kind: edge.map_or(RelationKind::Subagent, |e| e.kind),
            source_thread_id: thread.into(),
        })
    }
    /// 运行 R 名下全部未决事项数：父线程自己的事项与子线程事项（含 gd_）。
    pub fn inbox_pending_run(&self, run_id: &str) -> u64 {
        self.inbox_pending_run_items(run_id).len() as u64
    }
    pub fn revoke_child_requests(&mut self, run_id: &str, actions: &mut Vec<Action>) {
        self.routing_revoke_run(run_id, actions)
    }
}

impl Machine {
    /// SA-a：仅对 cxb 目标在转发副本覆盖 cxbOrigin；内部参数保持原样。
    pub fn child_forward_params(
        &self,
        target: crate::claim::ConnId,
        thread: &str,
        params: &Option<Value>,
    ) -> Option<Value> {
        if !self.cxb_conns.contains(&target) {
            return params.clone();
        }
        let turn = params
            .as_ref()
            .and_then(|p| p.get("turnId"))
            .and_then(Value::as_str);
        let Some(origin) = self.child_origin(thread, turn) else {
            return params.clone();
        };
        let mut copy = params.clone().unwrap_or_else(|| serde_json::json!({}));
        if let Some(object) = copy.as_object_mut() {
            object.insert(
                "cxbOrigin".into(),
                serde_json::to_value(origin).expect("cxbOrigin 可以序列化"),
            );
        }
        Some(copy)
    }
}

impl Machine {
    pub fn child_reply_written(
        &mut self,
        op: u64,
        outcome: &crate::inbox::ReplyOutcome,
        actions: &mut Vec<Action>,
    ) {
        self.routing_reply_written(op, outcome, actions)
    }
    /// SA-a：已登记 watcher 后扩充 children/revision；骨架保持原快照。
    pub fn child_watch_snapshot(
        &self,
        thread: &str,
        run_id: Option<&str>,
        snapshot: Value,
    ) -> Value {
        self.notify_watch_snapshot(thread, run_id, snapshot)
    }
}

impl Machine {
    pub(crate) fn relation_token() -> ClaimToken {
        ClaimToken {
            thread_id: String::new(),
            claim_id: 0,
        }
    }

    /// 证据仍可授予新绑定：未过期、父轮未结束、连接未换代、运行未撤权。
    pub(crate) fn evidence_valid(&self, op: &EvidenceOperation, now_ms: u64) -> bool {
        !op.parent_ended
            && now_ms < op.expires_at_ms
            && op.conn_epoch == self.relations.conn_epoch
            && op.outcome == EvidenceOutcome::Pending
            && self
                .relations
                .contexts
                .get(&op.run_id)
                .is_none_or(|c| c.authority != Authority::Revoked)
    }

    /// 回查中的线程是否可能属于运行 R：血缘未知，或已知血缘通到 R 的线程或成员。
    pub(crate) fn lookup_may_belong(&self, run_id: &str, thread: &str) -> bool {
        let Some(context) = self.relations.contexts.get(run_id) else {
            return false;
        };
        let mut current = thread.to_owned();
        for _ in 0..=MAX_DEPTH {
            if current == context.identity.logical_root_thread_id
                || current == context.identity.execution_thread_id
                || context.members.contains_key(&current) && current != thread
            {
                return true;
            }
            match self.thread_relation(&current) {
                Some(edge) => current = edge.parent_thread_id,
                None => return !self.relations.aux.lineage_roots.contains(&current),
            }
        }
        false
    }

    /// 子线程当前恰好一个已知活动轮次且已绑定时返回它；否则未知。
    pub(crate) fn unique_active_bound_turn(&self, thread: &str) -> Option<String> {
        let observed = self.relations.aux.observed.get(thread)?;
        let mut active = observed.iter().filter(|o| !o.completed);
        let only = active.next()?;
        if active.next().is_some() {
            return None;
        }
        self.relations
            .bindings
            .get(&(thread.into(), only.turn.clone()))
            .filter(|b| b.status == BindingStatus::Bound)
            .map(|_| only.turn.clone())
    }

    /// 把整体扫描计时排到最早的到期点：回查期限、证据期限、回查重试、Guardian 等待。
    pub(crate) fn ensure_sweep(&mut self, actions: &mut Vec<Action>) {
        if self.closing {
            return;
        }
        let now = self.clock.mono_ms;
        let mut next: Option<u64> = None;
        let mut consider = |at: u64| next = Some(next.map_or(at, |n: u64| n.min(at)));
        for (key, meta) in &self.relations.aux.request_meta {
            if self
                .relations
                .requests
                .get(key)
                .is_some_and(|r| r.stage == ChildRequestStage::Lookup)
            {
                consider(meta.lookup_deadline_ms);
            }
        }
        for op in self.relations.operations.values() {
            if self.evidence_expirable(op) {
                consider(op.expires_at_ms);
            }
        }
        for g in &self.relations.aux.guardians {
            consider(g.deadline_ms);
        }
        for lookup in self.relations.aux.lookups.values() {
            if let Some(at) = lookup.retry_at_ms {
                consider(at);
            }
        }
        let Some(at) = next else {
            if self.relations.aux.sweep.take().is_some() {
                actions.push(Action::Unschedule {
                    key: crate::claim::TimerKey::LookupDeadline(SWEEP_OP),
                });
            }
            return;
        };
        if self.relations.aux.sweep.is_some_and(|(due, _)| due == at) {
            return;
        }
        let generation = self.relations.aux.next();
        self.relations.aux.sweep = Some((at, generation));
        actions.push(Action::Schedule {
            token: Self::relation_token(),
            kind: TimerKind::LookupDeadline {
                op_id: SWEEP_OP,
                generation,
            },
            delay_ms: at.saturating_sub(now),
        });
    }

    /// 到期处理：证据过期转 unbound、回查到期按未知不答、重试到点的回查重新发出。
    fn sweep(&mut self, actions: &mut Vec<Action>) {
        self.facts_expire(actions);
        self.lookup_expire(actions);
        self.guardian_expire();
    }
}
