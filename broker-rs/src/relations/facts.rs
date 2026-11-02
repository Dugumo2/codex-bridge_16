//! 关系事实：派生证据、首轮配对与绑定、子线程轮次观察、成员状态与持久化（接口第 2、3 节）。
//!
//! 事件矩阵以实测为准（`S4-子代理事件实测`）：
//! - 子线程的 thread/started 可能根本不来；子线程的状态、轮次与请求在同一连接上直接到达。
//! - v1 spawnAgent 的 started 不带接收方，completed 才带 C；sendInput/resumeAgent 的 started 已带接收方。
//! - v2 subAgentActivity 在操作成功后连续发出相同的 started/completed，两条只登记一次；
//!   kind=started 已带 agentThreadId，不代表操作刚开始。
//!
//! 绑定分两步：证据有效期内把派生操作与 C 的唯一首轮「配对」；父轮确认是运行 R 的自有轮之后才「落实」为绑定。
//! sendInput、resumeAgent、v2 interacted 在收敛启用时配对唯一承载活动；历史完成轮不作为新工作。
use super::*;
use crate::claim::{Owner, State};
use crate::policy::Policy;
use serde_json::json;

/// 父线程某一轮的归属判断。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParentRun {
    Own { run: String, inherited: bool },
    Settling { claim_id: u64, run: String },
    AwaitBinding,
    NotOwn,
}

pub(crate) fn notification_turn(params: Option<&Value>) -> Option<&str> {
    let params = params?;
    params
        .get("turn")
        .and_then(|t| t.get("id"))
        .and_then(Value::as_str)
        .or_else(|| params.get("turnId").and_then(Value::as_str))
        .filter(|t| !t.is_empty())
}

/// 从上游 thread 对象提取血缘与能力；没有父线程（含仅 fork 来源）时返回 None。
pub(crate) fn edge_from_thread(info: &Value, discovered_by: DiscoveredBy) -> Option<RelationEdge> {
    let id = info.get("id").and_then(Value::as_str)?;
    let source = info.get("source").cloned().unwrap_or(Value::Null);
    let spawn = source
        .get("subAgent")
        .and_then(|s| s.get("thread_spawn"))
        .cloned();
    let parent = info
        .get("parentThreadId")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            spawn
                .as_ref()
                .and_then(|s| s.get("parent_thread_id"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .filter(|p| !p.is_empty() && p != id)?;
    let review = source
        .get("subAgent")
        .is_some_and(|s| s == "review" || s.get("review").is_some());
    let kind = if review {
        RelationKind::ReviewExecution
    } else {
        RelationKind::Subagent
    };
    // 来源摘要只留身份字段，不含正文。
    let mut summary = serde_json::Map::new();
    if let Some(spawn) = &spawn {
        summary.insert("kind".into(), json!("threadSpawn"));
        for (from, to) in [
            ("agent_path", "agentPath"),
            ("agent_role", "agentRole"),
            ("agent_nickname", "agentNickname"),
            ("depth", "depth"),
        ] {
            if let Some(v) = spawn.get(from).filter(|v| !v.is_null()) {
                summary.insert(to.into(), v.clone());
            }
        }
    } else if source.get("subAgent").is_some() {
        summary.insert(
            "kind".into(),
            json!(if review { "review" } else { "other" }),
        );
    }
    for (from, to) in [
        ("agentNickname", "agentNickname"),
        ("agentRole", "agentRole"),
    ] {
        if let Some(v) = info.get(from).filter(|v| !v.is_null()) {
            summary.entry(to.to_owned()).or_insert_with(|| v.clone());
        }
    }
    Some(RelationEdge {
        child_thread_id: id.into(),
        parent_thread_id: parent,
        kind,
        source_summary: Value::Object(summary),
        can_accept_direct_input: info.get("canAcceptDirectInput").and_then(Value::as_bool),
        agent_version: AgentVersion::Unknown,
        discovered_by,
    })
}

impl Machine {
    // ─── 通知入口 ────────────────────────────────────────────────────

    /// 返回 true 表示这条通知可能改变了证据、请求或期限（需要重排扫描计时）。
    pub(super) fn facts_notification(
        &mut self,
        event: &HeldNotification,
        actions: &mut Vec<Action>,
    ) -> bool {
        let params = event.params.as_ref();
        if event.method == "thread/started" {
            if let Some(info) = params.and_then(|p| p.get("thread")) {
                if let Some(id) = info.get("id").and_then(Value::as_str) {
                    if !self.claims.contains_key(id) {
                        self.attach(id, AttachmentSource::ThreadStarted);
                    }
                }
                if let Some(edge) = edge_from_thread(info, DiscoveredBy::Started) {
                    self.record_edge(edge, actions);
                }
            }
            return true;
        }
        let Some(thread) = params
            .and_then(|p| p.get("threadId"))
            .and_then(Value::as_str)
        else {
            return false;
        };
        let claimed = self.claims.contains_key(thread);
        if !claimed {
            // 统一入口检查订阅责任：广播与旧订阅者快照中的事件都不能覆盖最后发出的操作。
            self.attach(thread, AttachmentSource::ThreadEvent);
        }
        let thread = thread.to_owned();
        let turn = notification_turn(params).map(str::to_owned);
        match event.method.as_str() {
            "item/started" | "item/completed" => {
                let item = params.and_then(|p| p.get("item"));
                let relevant = item
                    .and_then(|i| i.get("type"))
                    .and_then(Value::as_str)
                    .is_some_and(|t| t == "collabAgentToolCall" || t == "subAgentActivity");
                if let (true, Some(turn), Some(item)) = (relevant, &turn, item) {
                    self.item_evidence(
                        &thread,
                        turn,
                        item,
                        event.method == "item/completed",
                        actions,
                    );
                    self.convergence_agent_states(&thread, turn, item, actions);
                }
                relevant
            }
            "turn/started" => {
                if let (false, Some(turn)) = (claimed, &turn) {
                    self.convergence_observe_activity(&thread, turn, "running", false);
                    self.observe_turn(&thread, turn, true, false, actions);
                }
                true
            }
            "turn/completed" => {
                if let Some(turn) = &turn {
                    if !claimed {
                        let status = params
                            .and_then(|p| p.get("turn"))
                            .and_then(|t| t.get("status"))
                            .and_then(Value::as_str)
                            .unwrap_or("unknown");
                        let first_completion = !self
                            .relations
                            .aux
                            .observed
                            .get(&thread)
                            .is_some_and(|turns| {
                                turns.iter().any(|t| t.turn == *turn && t.completed)
                            });
                        self.convergence_observe_activity(&thread, turn, status, false);
                        self.observe_turn(&thread, turn, true, true, actions);
                        // 根已释放后才结束的子线程：此刻补退订；重复的完成通知不再重发。
                        if first_completion {
                            self.release_idle_children(Some(&thread), actions);
                        }
                    }
                    self.parent_turn_ended(&thread, turn, actions);
                }
                true
            }
            "serverRequest/resolved" => {
                if let Some(id) = params.and_then(|p| p.get("requestId")) {
                    let id = id.clone();
                    self.routing_resolved(&thread, claimed, &id, actions);
                }
                true
            }
            "item/autoApprovalReview/completed" if !claimed => {
                if let Some(params) = params {
                    let params = params.clone();
                    self.guardian_child(&thread, turn.as_deref(), params, actions);
                }
                true
            }
            "thread/closed" => {
                self.child_closed(&thread, actions);
                self.convergences.closed_threads.insert(thread.clone());
                self.convergence_observation_gap(&thread, actions);
                self.detach_evidence(&thread);
                false
            }
            "thread/status/changed" => {
                if params
                    .and_then(|p| p.get("status"))
                    .and_then(|s| s.get("type"))
                    .and_then(Value::as_str)
                    == Some("notLoaded")
                {
                    self.convergence_observation_gap(&thread, actions);
                    self.detach_evidence(&thread);
                    // 广播可能迟到，不能结束本连接的订阅责任。
                }
                false
            }
            "thread/deleted" | "thread/archived" | "thread/unarchived" => {
                self.child_probe(&thread, actions);
                if event.method == "thread/deleted" {
                    self.detach_evidence(&thread);
                }
                self.lineage_touched(&thread, event.method == "thread/deleted", actions);
                true
            }
            _ => false,
        }
    }

    /// 本连接确实收到了该线程的事件或请求：只证明当前 epoch 下被观察，不证明此前历史完整。
    pub(crate) fn attach(&mut self, thread: &str, source: AttachmentSource) {
        // 有释放责任时，只有匹配最后一个恢复操作的成功回包能建立附着证据。
        if self.relations.aux.released.get(thread).is_some_and(|r| {
            source != AttachmentSource::ExplicitResume
                || r.result != super::release::SubscriptionResult::Attached
        }) {
            return;
        }
        let (epoch, now, revision) = (
            self.relations.conn_epoch,
            self.clock.mono_ms,
            self.relations.aux.version,
        );
        let attachments = &mut self.relations.attachments;
        if let Some(existing) = attachments.get_mut(thread) {
            existing.conn_epoch = epoch;
            existing.source = source;
            existing.observed_at_ms = now;
            existing.revision = revision;
            existing.valid = true;
            return;
        }
        if attachments.len() >= MAX_ATTACHMENTS {
            if let Some(oldest) = attachments
                .iter()
                .min_by_key(|(_, a)| a.observed_at_ms)
                .map(|(t, _)| t.clone())
            {
                attachments.remove(&oldest);
            }
        }
        attachments.insert(
            thread.into(),
            AttachmentEvidence {
                thread_id: thread.into(),
                conn_epoch: epoch,
                source,
                observed_at_ms: now,
                revision,
                valid: true,
            },
        );
    }

    /// 受管成员、活动绑定与清理范围仍需要订阅，Guardian 结束不能解除它们的附着。
    pub(crate) fn relation_needs_subscription(&self, thread: &str) -> bool {
        self.relations.contexts.values().any(|c| {
            (c.authority != Authority::Revoked
                && (c.members.contains_key(thread)
                    || c.identity.execution_thread_id == thread
                    || c.identity.logical_root_thread_id == thread))
                || c.cleanup
                    .as_ref()
                    .is_some_and(|cleanup| cleanup.scope.contains(thread))
        }) || self.relations.bindings.values().any(|b| {
            b.thread_id == thread
                && b.status == BindingStatus::Bound
                && self
                    .relations
                    .aux
                    .observed
                    .get(thread)
                    .is_some_and(|turns| turns.iter().any(|t| t.turn == b.turn_id && !t.completed))
                && self
                    .relations
                    .contexts
                    .get(&b.run_id)
                    .is_none_or(|c| c.authority != Authority::Revoked || c.cleanup.is_some())
        }) || self
            .relations
            .requests
            .values()
            .any(|r| r.source_thread_id == thread)
    }

    /// 卸载、关闭、删除使附着证据立即失效。
    pub(crate) fn detach_evidence(&mut self, thread: &str) {
        if let Some(a) = self.relations.attachments.get_mut(thread) {
            a.valid = false;
        }
    }

    /// 删除或归档改变血缘集合：删除时去掉边；通知 SA-b 让在途生命周期发现失效。
    fn lineage_touched(&mut self, thread: &str, deleted: bool, actions: &mut Vec<Action>) {
        let root = self.logical_root_of(thread);
        let known = self.relations.edges.contains_key(thread)
            || self
                .relations
                .edges
                .values()
                .any(|e| e.parent_thread_id == thread);
        if deleted && self.relations.edges.remove(thread).is_some() {
            self.bump_version();
        }
        if known {
            self.on_lineage_changed(&root, actions);
        }
    }

    pub(crate) fn bump_version(&mut self) {
        self.relations.aux.version = self.relations.aux.version.saturating_add(1);
    }

    /// 沿血缘向上找逻辑根：遇到某运行的执行线程或逻辑根即停；否则取最上层已知线程。
    pub(crate) fn logical_root_of(&self, thread: &str) -> String {
        let mut current = thread.to_owned();
        for _ in 0..=MAX_DEPTH {
            if let Some(context) = self.relations.contexts.values().find(|c| {
                c.identity.execution_thread_id == current
                    || c.identity.logical_root_thread_id == current
            }) {
                return context.identity.logical_root_thread_id.clone();
            }
            match self.thread_relation(&current) {
                Some(edge) => current = edge.parent_thread_id,
                None => break,
            }
        }
        current
    }

    /// 登记或更新血缘边；字段变化才加版本。返回是否有变化。
    pub(crate) fn record_edge(
        &mut self,
        mut edge: RelationEdge,
        actions: &mut Vec<Action>,
    ) -> bool {
        let thread = edge.child_thread_id.clone();
        if let Some(old) = self.relations.edges.get(&thread) {
            // 版本只由派生事件判定；能力与摘要以较新观察补齐，不用缺失覆盖已知值。
            if edge.agent_version == AgentVersion::Unknown {
                edge.agent_version = old.agent_version;
            }
            if edge.can_accept_direct_input.is_none() {
                edge.can_accept_direct_input = old.can_accept_direct_input;
            }
            if edge.source_summary.as_object().is_none_or(|o| o.is_empty()) {
                edge.source_summary = old.source_summary.clone();
            }
            edge.discovered_by = old.discovered_by;
            if *old == edge {
                return false;
            }
        }
        self.relations.edges.insert(thread.clone(), edge);
        self.bump_version();
        let root = self.logical_root_of(&thread);
        self.persist_edge(&thread, actions);
        // 已是某运行成员的线程，能力或版本变化也要刷新成员快照。
        let runs: Vec<String> = self
            .relations
            .contexts
            .iter()
            .filter(|(_, c)| c.members.contains_key(&thread))
            .map(|(r, _)| r.clone())
            .collect();
        for run in runs {
            self.refresh_member(&run, &thread, MemberChange::Relation, actions);
        }
        self.on_lineage_changed(&root, actions);
        self.routing_reconsider(&thread, actions);
        true
    }

    // ─── 父轮归属 ────────────────────────────────────────────────────

    /// 父线程这一轮是不是某运行的自有轮：自有认领看 owned_turns；子线程看自身绑定（继承）。
    pub(crate) fn parent_run(&self, thread: &str, turn: &str) -> ParentRun {
        if let Some(claim) = self.claims.get(thread) {
            let Some(run) = claim.run_id.clone() else {
                return ParentRun::NotOwn;
            };
            if claim.precheck() || claim.joined() {
                return ParentRun::NotOwn;
            }
            if claim.settling() {
                return ParentRun::Settling {
                    claim_id: claim.claim_id,
                    run,
                };
            }
            if claim.turn_id() == Some(turn) || claim.owned_turns.contains(turn) {
                return ParentRun::Own {
                    run,
                    inherited: false,
                };
            }
            return ParentRun::NotOwn;
        }
        match self
            .relations
            .bindings
            .get(&(thread.to_owned(), turn.to_owned()))
        {
            Some(b) if b.status == BindingStatus::Bound => ParentRun::Own {
                run: b.run_id.clone(),
                inherited: true,
            },
            Some(_) => ParentRun::NotOwn,
            None => ParentRun::AwaitBinding,
        }
    }

    /// 为运行建立管理上下文；只有运行仍有认领时才能建立（结束后不新授）。
    pub(crate) fn ensure_context(&mut self, run: &str) -> bool {
        let claim = self
            .claims
            .iter()
            .find(|(_, c)| c.run_id.as_deref() == Some(run))
            .map(|(t, c)| (t.clone(), c.clone()));
        if let Some(context) = self.relations.contexts.get_mut(run) {
            if let Some((thread, _)) = &claim {
                context.identity.execution_thread_id = thread.clone();
            }
            return true;
        }
        let Some((thread, claim)) = claim else {
            return false;
        };
        let (authority, policy) = match claim.owner() {
            _ if claim.joined() || claim.precheck() => (Authority::Revoked, Policy::OnRequest),
            Some(Owner::Client(conn)) if !claim.owner_gone => {
                (Authority::Owner(*conn), Policy::OnRequest)
            }
            Some(Owner::Broker { policy, .. }) => (Authority::Broker { policy: *policy }, *policy),
            // owner 已断开：沿用断开前（运行登记）的策略快照；缺失时按需人工。
            _ => (
                Authority::Broker {
                    policy: Policy::OnRequest,
                },
                Policy::OnRequest,
            ),
        };
        let root = self
            .runs
            .runs
            .get(run)
            .map(|r| r.thread_id.clone())
            .unwrap_or_else(|| thread.clone());
        self.relations.contexts.insert(
            run.into(),
            ManageContext {
                identity: RunIdentity {
                    run_id: run.into(),
                    logical_root_thread_id: root,
                    execution_thread_id: thread,
                },
                gen: 1,
                authority,
                policy,
                members: BTreeMap::new(),
                in_flight_spawns: BTreeSet::new(),
                revision: 0,
                discovery: DiscoveryState {
                    missing: Default::default(),
                    complete: false,
                    version: 0,
                    incomplete_reasons: Vec::new(),
                },
                cleanup: None,
            },
        );
        true
    }

    // ─── 派生证据 ────────────────────────────────────────────────────

    fn item_evidence(
        &mut self,
        parent: &str,
        parent_turn: &str,
        item: &Value,
        completed: bool,
        actions: &mut Vec<Action>,
    ) {
        let Some(item_id) = item.get("id").and_then(Value::as_str) else {
            return;
        };
        let (kind, receivers, failed) = match item.get("type").and_then(Value::as_str) {
            Some("collabAgentToolCall") => {
                let kind = match item.get("tool").and_then(Value::as_str) {
                    Some("spawnAgent") => EvidenceKind::V1Spawn,
                    Some("sendInput") => EvidenceKind::V1SendInput,
                    Some("resumeAgent") => EvidenceKind::V1ResumeAgent,
                    // wait、closeAgent 等不涉及归属；v2 的 wait 也用这个条目类型，不能据此判断版本。
                    _ => return,
                };
                let receivers: BTreeSet<String> = item
                    .get("receiverThreadIds")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .filter(|t| !t.is_empty() && *t != parent)
                    .map(str::to_owned)
                    .collect();
                let status = item.get("status").and_then(Value::as_str);
                let failed = completed
                    && (matches!(status, Some("failed" | "interrupted"))
                        || kind == EvidenceKind::V1Spawn && receivers.is_empty());
                (kind, receivers, failed)
            }
            Some("subAgentActivity") => {
                let kind = match item.get("kind").and_then(Value::as_str) {
                    Some("started") => EvidenceKind::V2Started,
                    Some("interacted") => EvidenceKind::V2Interacted,
                    Some("interrupted") => EvidenceKind::V2Interrupted,
                    Some("completed") => EvidenceKind::V2Completed,
                    _ => return,
                };
                let receivers = item
                    .get("agentThreadId")
                    .and_then(Value::as_str)
                    .filter(|t| !t.is_empty() && *t != parent)
                    .map(|t| BTreeSet::from([t.to_owned()]))
                    .unwrap_or_default();
                (kind, receivers, false)
            }
            _ => return,
        };
        let key = EvidenceKey {
            parent_thread_id: parent.into(),
            parent_turn_id: parent_turn.into(),
            item_id: item_id.into(),
        };
        let now = self.clock.mono_ms;
        let first = !self.relations.operations.contains_key(&key);
        if first {
            if !self.reserve_operation() {
                actions.push(Action::Warn(format!(
                    "证据操作达到上限 {MAX_OPERATIONS}，忽略未证明归属的条目 {item_id}"
                )));
                return;
            }
            let parent_run = self.parent_run(parent, parent_turn);
            let run_id = match &parent_run {
                ParentRun::Own { run, .. } | ParentRun::Settling { run, .. } => run.clone(),
                _ => String::new(),
            };
            if let ParentRun::Own { run, .. } = &parent_run {
                self.ensure_context(run);
            }
            match &parent_run {
                ParentRun::Settling { claim_id, .. } => {
                    self.relations.aux.parent_waits.insert(
                        key.clone(),
                        ParentWait::Settle {
                            thread: parent.into(),
                            claim_id: *claim_id,
                        },
                    );
                }
                ParentRun::AwaitBinding => {
                    self.relations.aux.parent_waits.insert(
                        key.clone(),
                        ParentWait::Binding {
                            thread: parent.into(),
                            turn: parent_turn.into(),
                        },
                    );
                }
                _ => {}
            }
            let authority_gen = self.relations.contexts.get(&run_id).map_or(0, |c| c.gen);
            let outcome = if parent_run == ParentRun::NotOwn {
                EvidenceOutcome::Unproven
            } else {
                EvidenceOutcome::Pending
            };
            self.relations.operations.insert(
                key.clone(),
                EvidenceOperation {
                    key: key.clone(),
                    run_id,
                    conn_epoch: self.relations.conn_epoch,
                    authority_gen,
                    kind,
                    started_seen: false,
                    completed_seen: false,
                    outcome,
                    receivers: BTreeSet::new(),
                    rejected_receivers: BTreeMap::new(),
                    proven_turns: BTreeMap::new(),
                    first_seen_ms: now,
                    expires_at_ms: now.saturating_add(EVIDENCE_TTL_MS),
                    parent_ended: false,
                    failure: None,
                },
            );
        }
        let op = self
            .relations
            .operations
            .get_mut(&key)
            .expect("证据操作已登记");
        // 同键 started/completed 各处理一次；completed 先到时迟到 started 不回退状态。
        if completed {
            if op.completed_seen {
                return;
            }
            op.completed_seen = true;
        } else {
            if op.started_seen {
                return;
            }
            op.started_seen = true;
            if op.completed_seen {
                return;
            }
        }
        let new_receivers: Vec<String> = receivers
            .iter()
            .filter(|r| op.receivers.insert((*r).clone()))
            .cloned()
            .collect();
        let confirmed = self.op_confirmed(&key);
        let run = self.relations.operations[&key].run_id.clone();
        match kind {
            EvidenceKind::V1Spawn => {
                if !completed {
                    if confirmed {
                        if let Some(context) = self.relations.contexts.get_mut(&run) {
                            context.in_flight_spawns.insert(key.clone());
                        }
                        self.touch_context(&run, MemberChange::Activity, actions);
                    }
                    return;
                }
                let was_in_flight = self
                    .relations
                    .contexts
                    .get_mut(&run)
                    .is_some_and(|c| c.in_flight_spawns.remove(&key));
                if failed {
                    // 可确认的失败撤销操作及尚未落实的候选，保留诊断。
                    let op = self.relations.operations.get_mut(&key).expect("证据操作");
                    op.outcome = EvidenceOutcome::Failed;
                    op.failure = Some(
                        item.get("status")
                            .and_then(Value::as_str)
                            .unwrap_or("noReceiver")
                            .to_owned(),
                    );
                    op.proven_turns.clear();
                    if was_in_flight {
                        self.touch_context(&run, MemberChange::Activity, actions);
                    }
                    return;
                }
                for child in new_receivers {
                    self.spawn_candidate(&key, &child, AgentVersion::V1, actions);
                }
                if was_in_flight {
                    self.touch_context(&run, MemberChange::Activity, actions);
                }
            }
            EvidenceKind::V2Started => {
                for child in new_receivers {
                    self.spawn_candidate(&key, &child, AgentVersion::V2, actions);
                }
            }
            _ if self.convergence_enabled()
                && confirmed
                && !failed
                && matches!(
                    kind,
                    EvidenceKind::V1SendInput
                        | EvidenceKind::V1ResumeAgent
                        | EvidenceKind::V2Interacted
                ) =>
            {
                // 新轮可能早于 interacted，也可能是 sendInput 并入已有活动轮。
                // 只排除首次通知时已完成的旧轮；每个接收方只建一次基线，双通知不重新挂起。
                for child in new_receivers {
                    if self.reject_followup_receiver(&key, &child) {
                        continue;
                    }
                    let observed = self
                        .relations
                        .aux
                        .observed
                        .get(&child)
                        .map(|v| {
                            v.iter()
                                .filter(|o| o.completed)
                                .map(|o| o.turn.clone())
                                .collect()
                        })
                        .unwrap_or_default();
                    self.convergences
                        .reuse_candidates
                        .entry((key.clone(), child.clone()))
                        .or_insert(observed);
                    self.relations
                        .aux
                        .spawn_waits
                        .insert(child.clone(), key.clone());
                    self.pair_first_turn(&key, &child, actions);
                    self.materialize(&key, actions);
                }
            }
            _ => {
                // sendInput、resumeAgent、v2 interacted/interrupted/completed：不证明新轮，只记事实。
                let op = self.relations.operations.get_mut(&key).expect("证据操作");
                if op.outcome == EvidenceOutcome::Pending {
                    op.outcome = EvidenceOutcome::Unproven;
                }
                if failed {
                    op.outcome = EvidenceOutcome::Failed;
                    op.failure = item
                        .get("status")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                }
                self.relations.aux.parent_waits.remove(&key);
                if confirmed {
                    for child in receivers {
                        if self
                            .relations
                            .contexts
                            .get(&run)
                            .is_some_and(|c| c.members.contains_key(&child))
                        {
                            self.refresh_member(&run, &child, MemberChange::Activity, actions);
                        }
                    }
                }
            }
        }
    }

    /// 运行已确认：有运行编号且不在等父轮结论。
    pub(crate) fn op_confirmed(&self, key: &EvidenceKey) -> bool {
        self.relations
            .operations
            .get(key)
            .is_some_and(|op| !op.run_id.is_empty() && op.outcome != EvidenceOutcome::Unproven)
            && !self.relations.aux.parent_waits.contains_key(key)
    }

    /// 派生证据给出接收方 C：登记血缘边，配对 C 的唯一首轮；运行已确认时纳入成员。
    fn spawn_candidate(
        &mut self,
        key: &EvidenceKey,
        child: &str,
        version: AgentVersion,
        actions: &mut Vec<Action>,
    ) {
        let edge = RelationEdge {
            child_thread_id: child.into(),
            parent_thread_id: key.parent_thread_id.clone(),
            kind: RelationKind::Subagent,
            source_summary: Value::Null,
            can_accept_direct_input: None,
            agent_version: version,
            discovered_by: DiscoveredBy::SpawnEvent,
        };
        // 派生事件给出的版本可以修正此前未知的版本。
        let mut merged = edge;
        if let Some(old) = self.relations.edges.get(child) {
            merged.source_summary = old.source_summary.clone();
            merged.can_accept_direct_input = old.can_accept_direct_input;
            merged.kind = old.kind;
        }
        let changed = self.relations.edges.get(child).is_none_or(|old| {
            old.agent_version != version || old.parent_thread_id != key.parent_thread_id
        });
        if changed {
            let mut stored = merged.clone();
            if let Some(old) = self.relations.edges.get(child) {
                stored.discovered_by = old.discovered_by;
            }
            self.relations.edges.insert(child.into(), stored);
            self.bump_version();
        }
        self.pair_first_turn(key, child, actions);
        if self.op_confirmed(key) {
            self.materialize(key, actions);
        }
        if changed {
            self.persist_edge(child, actions);
            let root = self.logical_root_of(child);
            self.on_lineage_changed(&root, actions);
        }
        self.routing_reconsider(child, actions);
    }

    /// 首轮配对：C 恰好观察到一轮就配对；还没有就等；不止一轮说明首轮身份不明，保持 unbound。
    fn pair_first_turn(&mut self, key: &EvidenceKey, child: &str, actions: &mut Vec<Action>) {
        if self.reject_followup_receiver(key, child) {
            return;
        }
        let now = self.clock.mono_ms;
        let Some(op) = self.relations.operations.get(key) else {
            return;
        };
        if !self.evidence_valid_for_pairing(op, now) {
            return;
        }
        let observed: Vec<String> = self
            .relations
            .aux
            .observed
            .get(child)
            .map(|o| {
                o.iter()
                    .filter(|t| {
                        !self
                            .convergences
                            .reuse_candidates
                            .get(&(key.clone(), child.into()))
                            .is_some_and(|old| old.contains(&t.turn))
                    })
                    .map(|t| t.turn.clone())
                    .collect()
            })
            .unwrap_or_default();
        match observed.len() {
            0 => {
                self.relations
                    .aux
                    .spawn_waits
                    .insert(child.into(), key.clone());
            }
            1 => {
                self.relations.aux.spawn_waits.remove(child);
                let op = self.relations.operations.get_mut(key).expect("证据操作");
                op.proven_turns
                    .entry(child.into())
                    .or_default()
                    .insert(observed[0].clone());
            }
            _ => {
                self.relations.aux.spawn_waits.remove(child);
                self.candidate_unbound(key, child, "首轮身份不明", actions);
            }
        }
    }

    /// 配对只要求证据本身有效；运行确认（settling 定案、继承父轮）可以稍后到达。
    fn evidence_valid_for_pairing(&self, op: &EvidenceOperation, now: u64) -> bool {
        !op.parent_ended
            && now < op.expires_at_ms
            && op.conn_epoch == self.relations.conn_epoch
            && op.outcome == EvidenceOutcome::Pending
    }

    /// 运行确认后把配对落实为绑定，并把接收方纳入成员。
    fn materialize(&mut self, key: &EvidenceKey, actions: &mut Vec<Action>) {
        let Some(op) = self.relations.operations.get(key).cloned() else {
            return;
        };
        if op.run_id.is_empty() || !self.ensure_context(&op.run_id) {
            return;
        }
        if self
            .relations
            .contexts
            .get(&op.run_id)
            .is_some_and(|c| c.authority == Authority::Revoked)
        {
            return;
        }
        let inherited = !self.claims.contains_key(&key.parent_thread_id);
        let evidence = if inherited {
            BindingEvidence::Inherited
        } else {
            match op.kind {
                EvidenceKind::V2Started => BindingEvidence::V2Started,
                EvidenceKind::V2Interacted => BindingEvidence::V2Interacted,
                EvidenceKind::V1SendInput | EvidenceKind::V1ResumeAgent => {
                    BindingEvidence::V1Message
                }
                _ => BindingEvidence::V1Spawn,
            }
        };
        for child in &op.receivers {
            if self.reject_followup_receiver(key, child) {
                continue;
            }
            self.relations
                .aux
                .member_evidence
                .entry((op.run_id.clone(), child.clone()))
                .or_insert(evidence);
            self.persist_edge(child, actions);
            self.refresh_member(&op.run_id, child, MemberChange::Relation, actions);
        }
        for (child, turns) in &op.proven_turns {
            if self.reject_followup_receiver(key, child) {
                continue;
            }
            for turn in turns {
                self.bind(child, turn, &op.run_id, evidence, key, actions);
            }
        }
        if !op.receivers.is_empty()
            && op.receivers.iter().all(|c| {
                op.proven_turns.contains_key(c)
                    && !self.relations.operations[key]
                        .rejected_receivers
                        .contains_key(c)
            })
        {
            if let Some(op) = self.relations.operations.get_mut(key) {
                op.outcome = EvidenceOutcome::Proven;
            }
        }
    }

    /// 跟进只提供候选身份，血缘、独立认领与其他运行绑定必须在授予管理权前核实。
    fn reject_followup_receiver(&mut self, key: &EvidenceKey, child: &str) -> bool {
        let Some(op) = self.relations.operations.get(key) else {
            return false;
        };
        if !matches!(
            op.kind,
            EvidenceKind::V1SendInput | EvidenceKind::V1ResumeAgent | EvidenceKind::V2Interacted
        ) {
            return false;
        }
        if op.rejected_receivers.contains_key(child) {
            return true;
        }
        let reason = if !self.thread_relation(child).is_some_and(|edge| {
            edge.kind == RelationKind::Subagent && edge.parent_thread_id == key.parent_thread_id
        }) {
            "未核实为本父线程的子代理"
        } else if self.claims.contains_key(child) {
            "接收方已有独立认领"
        } else if self.relations.bindings.values().any(|binding| {
            binding.thread_id == child
                && binding.run_id != op.run_id
                && binding.status == BindingStatus::Bound
        }) {
            "接收方已绑定其他运行"
        } else {
            return false;
        };
        let op = self
            .relations
            .operations
            .get_mut(key)
            .expect("跟进操作存在");
        op.rejected_receivers.insert(child.into(), reason.into());
        op.proven_turns.remove(child);
        if op
            .receivers
            .iter()
            .all(|t| op.rejected_receivers.contains_key(t))
        {
            op.outcome = EvidenceOutcome::Unproven;
        }
        if self.relations.aux.spawn_waits.get(child) == Some(key) {
            self.relations.aux.spawn_waits.remove(child);
        }
        self.convergences
            .reuse_candidates
            .remove(&(key.clone(), child.into()));
        true
    }

    /// 已拒绝的跟进候选不能通过后续收敛发现重新入册；本运行已有合法管理证据时仍保留。
    pub(super) fn rejected_followup_for_run(&self, run: &str, child: &str) -> bool {
        if self
            .manage_context(run)
            .is_some_and(|c| c.members.contains_key(child))
        {
            return false;
        }
        let mut rejected = false;
        for op in self
            .relations
            .operations
            .values()
            .filter(|op| op.run_id == run && op.receivers.contains(child))
        {
            if !op.rejected_receivers.contains_key(child) {
                return false;
            }
            rejected = true;
        }
        rejected
    }

    /// 写入绑定；独立认领或已有其他运行绑定构成冲突，不覆盖。
    fn bind(
        &mut self,
        child: &str,
        turn: &str,
        run: &str,
        evidence: BindingEvidence,
        key: &EvidenceKey,
        actions: &mut Vec<Action>,
    ) {
        let binding_key = (child.to_owned(), turn.to_owned());
        let conflict = if let Some(claim) = self.claims.get(child) {
            Some(format!(
                "子线程有独立认领（运行 {}）",
                claim.run_id.as_deref().unwrap_or("无")
            ))
        } else {
            match self.relations.bindings.get(&binding_key) {
                Some(b) if b.run_id == run && b.status == BindingStatus::Bound => return,
                Some(b) if b.status == BindingStatus::Bound => {
                    Some(format!("该轮已归属运行 {}", b.run_id))
                }
                _ => None,
            }
        };
        if let Some(reason) = conflict {
            self.runs.log_source(
                run,
                child,
                Some(&key.parent_thread_id),
                "subagent",
                Some(turn),
                None,
                self.clock.now_ms,
                "归属冲突",
                &reason,
                false,
                false,
                Some("discoveryIncomplete"),
                actions,
            );
            if let Some(member) = self
                .relations
                .contexts
                .get_mut(run)
                .and_then(|c| c.members.get_mut(child))
            {
                member.conflict = Some(reason.clone());
            }
            // 已有的绑定（含其他运行的）不覆盖。
            self.relations
                .bindings
                .entry(binding_key)
                .or_insert_with(|| ActivityBinding {
                    thread_id: child.into(),
                    turn_id: turn.into(),
                    run_id: run.into(),
                    parent_thread_id: key.parent_thread_id.clone(),
                    evidence,
                    operation: key.clone(),
                    status: BindingStatus::Conflict(reason),
                });
            self.bump_context_revision(run, child, MemberChange::Conflict, actions);
            return;
        }
        self.relations.bindings.insert(
            binding_key,
            ActivityBinding {
                thread_id: child.into(),
                turn_id: turn.into(),
                run_id: run.into(),
                parent_thread_id: key.parent_thread_id.clone(),
                evidence,
                operation: key.clone(),
                status: BindingStatus::Bound,
            },
        );
        self.runs.log_source(
            run,
            child,
            Some(&key.parent_thread_id),
            "subagent",
            Some(turn),
            None,
            self.clock.now_ms,
            "纳入运行",
            "已确认活动绑定",
            false,
            false,
            None,
            actions,
        );
        self.flush_bound_event_log(run, child, turn, &key.parent_thread_id, actions);
        // 继承：等这一轮结论的孙线程派生现在可以确认。
        let waiting: Vec<EvidenceKey> = self
            .relations
            .aux
            .parent_waits
            .iter()
            .filter(|(_, w)| {
                matches!(w, ParentWait::Binding { thread, turn: t } if thread == child && t == turn)
            })
            .map(|(k, _)| k.clone())
            .collect();
        self.refresh_member(run, child, MemberChange::Binding, actions);
        for waiting_key in waiting {
            self.relations.aux.parent_waits.remove(&waiting_key);
            if let Some(op) = self.relations.operations.get_mut(&waiting_key) {
                op.run_id = run.into();
                op.authority_gen = self.relations.contexts.get(run).map_or(0, |c| c.gen);
            }
            self.confirm_waiting(&waiting_key, actions);
        }
        self.routing_reconsider(child, actions);
    }

    /// 等待中的操作获得运行确认：补登在途派生，落实已有配对。
    fn confirm_waiting(&mut self, key: &EvidenceKey, actions: &mut Vec<Action>) {
        let Some(op) = self.relations.operations.get(key).cloned() else {
            return;
        };
        if op.kind == EvidenceKind::V1Spawn
            && op.started_seen
            && !op.completed_seen
            && !op.parent_ended
        {
            self.ensure_context(&op.run_id);
            if let Some(context) = self.relations.contexts.get_mut(&op.run_id) {
                context.in_flight_spawns.insert(key.clone());
            }
            self.touch_context(&op.run_id, MemberChange::Activity, actions);
        }
        if matches!(op.kind, EvidenceKind::V1Spawn | EvidenceKind::V2Started) {
            self.materialize(key, actions);
        }
    }

    /// 候选不能落实：已观察而未归属的轮次记为 unbound（运行已知时），并刷新成员。
    fn candidate_unbound(
        &mut self,
        key: &EvidenceKey,
        child: &str,
        reason: &str,
        actions: &mut Vec<Action>,
    ) {
        let Some(op) = self.relations.operations.get(key).cloned() else {
            return;
        };
        if let Some(op) = self.relations.operations.get_mut(key) {
            op.failure.get_or_insert_with(|| reason.to_owned());
        }
        if op.run_id.is_empty() || !self.op_confirmed(key) {
            return;
        }
        let turns: Vec<String> = self
            .relations
            .aux
            .observed
            .get(child)
            .map(|o| o.iter().map(|t| t.turn.clone()).collect())
            .unwrap_or_default();
        for turn in turns {
            self.relations
                .bindings
                .entry((child.to_owned(), turn.clone()))
                .or_insert_with(|| ActivityBinding {
                    thread_id: child.into(),
                    turn_id: turn,
                    run_id: op.run_id.clone(),
                    parent_thread_id: key.parent_thread_id.clone(),
                    evidence: match op.kind {
                        EvidenceKind::V2Started => BindingEvidence::V2Started,
                        _ => BindingEvidence::V1Spawn,
                    },
                    operation: key.clone(),
                    status: BindingStatus::Unbound,
                });
        }
        if self
            .relations
            .contexts
            .get(&op.run_id)
            .is_some_and(|c| c.members.contains_key(child))
        {
            self.refresh_member(&op.run_id, child, MemberChange::Binding, actions);
        }
        self.routing_reconsider(child, actions);
    }

    /// 父轮结束：此后该轮的证据不能再授予绑定；未配对候选转 unbound，在途派生不再计入。
    pub(crate) fn parent_turn_ended(
        &mut self,
        thread: &str,
        turn: &str,
        actions: &mut Vec<Action>,
    ) {
        let keys: Vec<EvidenceKey> = self
            .relations
            .operations
            .range(
                EvidenceKey {
                    parent_thread_id: thread.into(),
                    parent_turn_id: turn.into(),
                    item_id: String::new(),
                }..,
            )
            .take_while(|(k, _)| k.parent_thread_id == thread && k.parent_turn_id == turn)
            .map(|(k, _)| k.clone())
            .collect();
        for key in keys {
            let op = self.relations.operations.get_mut(&key).expect("证据操作");
            if op.parent_ended {
                continue;
            }
            op.parent_ended = true;
            let run = op.run_id.clone();
            let receivers = op.receivers.clone();
            if self
                .relations
                .contexts
                .get_mut(&run)
                .is_some_and(|c| c.in_flight_spawns.remove(&key))
            {
                self.touch_context(&run, MemberChange::Activity, actions);
            }
            for child in receivers {
                if self.relations.aux.spawn_waits.get(&child) == Some(&key) {
                    self.relations.aux.spawn_waits.remove(&child);
                    self.candidate_unbound(&key, &child, "父轮结束前未见首轮", actions);
                }
            }
            if self.op_confirmed(&key) {
                self.materialize(&key, actions);
            }
        }
    }

    // ─── 子线程轮次 ──────────────────────────────────────────────────

    /// 无认领线程的轮次观察（通知或请求都算），重复观察幂等。
    pub(crate) fn observe_turn(
        &mut self,
        thread: &str,
        turn: &str,
        started: bool,
        completed: bool,
        actions: &mut Vec<Action>,
    ) {
        if turn.is_empty() || self.claims.contains_key(thread) {
            return;
        }
        let now = self.clock.mono_ms;
        let aux = &mut self.relations.aux;
        if !aux.observed.contains_key(thread) {
            if aux.observed.len() >= MAX_OBSERVED_THREADS {
                if let Some(oldest) = aux.observed_order.pop_front() {
                    aux.observed.remove(&oldest);
                }
            }
            aux.observed_order.push_back(thread.into());
        }
        let list = aux.observed.entry(thread.into()).or_default();
        let (new, changed) = match list.iter_mut().find(|o| o.turn == turn) {
            Some(o) => {
                let changed = (started && !o.started) || (completed && !o.completed);
                o.started |= started;
                o.completed |= completed;
                (false, changed)
            }
            None => {
                list.push_back(TurnObservation {
                    turn: turn.into(),
                    started,
                    completed,
                    seen_ms: now,
                });
                while list.len() > MAX_OBSERVED_TURNS {
                    list.pop_front();
                }
                (true, true)
            }
        };
        if !changed {
            return;
        }
        if new {
            if let Some(key) = self.relations.aux.spawn_waits.get(thread).cloned() {
                self.pair_first_turn(&key, thread, actions);
                if self.op_confirmed(&key) {
                    self.materialize(&key, actions);
                }
            }
        }
        if completed {
            self.parent_turn_ended(thread, turn, actions);
        }
        let runs: Vec<String> = self
            .relations
            .contexts
            .iter()
            .filter(|(_, c)| c.members.contains_key(thread))
            .map(|(r, _)| r.clone())
            .collect();
        for run in runs {
            self.refresh_member(&run, thread, MemberChange::Activity, actions);
        }
        if new {
            self.routing_reconsider(thread, actions);
        }
    }

    // ─── settling 定案与到期 ─────────────────────────────────────────

    pub(super) fn facts_claim_settled(
        &mut self,
        thread: &str,
        claim_id: u64,
        own: bool,
        actions: &mut Vec<Action>,
    ) {
        let keys: Vec<EvidenceKey> = self
            .relations
            .aux
            .parent_waits
            .iter()
            .filter(|(_, w)| {
                matches!(w, ParentWait::Settle { thread: t, claim_id: c } if t == thread && *c == claim_id)
            })
            .map(|(k, _)| k.clone())
            .collect();
        for key in keys {
            self.relations.aux.parent_waits.remove(&key);
            let mine = own
                && matches!(
                    self.parent_run(thread, &key.parent_turn_id),
                    ParentRun::Own { .. }
                );
            if !mine {
                if let Some(op) = self.relations.operations.get_mut(&key) {
                    op.outcome = EvidenceOutcome::Unproven;
                    op.proven_turns.clear();
                }
                for (child, k) in self.relations.aux.spawn_waits.clone() {
                    if k == key {
                        self.relations.aux.spawn_waits.remove(&child);
                    }
                }
                continue;
            }
            let run = self.relations.operations[&key].run_id.clone();
            self.ensure_context(&run);
            if let Some(op) = self.relations.operations.get_mut(&key) {
                op.authority_gen = self.relations.contexts.get(&run).map_or(0, |c| c.gen);
            }
            self.confirm_waiting(&key, actions);
        }
    }

    /// 证据到期：未配对候选转 unbound；等待父轮绑定的操作不能再确认。
    pub(super) fn facts_expire(&mut self, actions: &mut Vec<Action>) {
        let now = self.clock.mono_ms;
        let expired: Vec<EvidenceKey> = self
            .relations
            .operations
            .iter()
            .filter(|(_, op)| self.evidence_expirable(op) && now >= op.expires_at_ms)
            .map(|(k, _)| k.clone())
            .collect();
        for key in expired {
            let waiting_binding = matches!(
                self.relations.aux.parent_waits.get(&key),
                Some(ParentWait::Binding { .. })
            );
            if waiting_binding {
                self.relations.aux.parent_waits.remove(&key);
                if let Some(op) = self.relations.operations.get_mut(&key) {
                    op.outcome = EvidenceOutcome::Unproven;
                    op.proven_turns.clear();
                }
            }
            let receivers = self.relations.operations[&key].receivers.clone();
            for child in receivers {
                if self.relations.aux.spawn_waits.get(&child) == Some(&key) {
                    self.relations.aux.spawn_waits.remove(&child);
                    self.candidate_unbound(&key, &child, "证据过期", actions);
                }
            }
            let run = self.relations.operations[&key].run_id.clone();
            if self
                .relations
                .contexts
                .get_mut(&run)
                .is_some_and(|c| c.in_flight_spawns.remove(&key))
            {
                self.touch_context(&run, MemberChange::Activity, actions);
            }
            if let Some(op) = self.relations.operations.get_mut(&key) {
                if op.outcome == EvidenceOutcome::Pending {
                    op.outcome = if op.proven_turns.is_empty() {
                        EvidenceOutcome::Unproven
                    } else {
                        EvidenceOutcome::Proven
                    };
                }
            }
        }
        self.prune_contexts();
    }

    /// 调度与到期处理使用同一资格，settling 由定案回调重新登记期限。
    pub(crate) fn evidence_expirable(&self, op: &EvidenceOperation) -> bool {
        op.outcome == EvidenceOutcome::Pending
            && !matches!(
                self.relations.aux.parent_waits.get(&op.key),
                Some(ParentWait::Settle { .. })
            )
    }

    /// 插入前预留容量；全是未决操作时保守拒绝，避免淘汰当前键或无界增长。
    fn reserve_operation(&mut self) -> bool {
        while self.relations.operations.len() >= MAX_OPERATIONS {
            let oldest = self
                .relations
                .operations
                .iter()
                .filter(|(k, op)| {
                    op.outcome != EvidenceOutcome::Pending
                        && !self.relations.aux.parent_waits.contains_key(*k)
                })
                .min_by_key(|(_, op)| op.first_seen_ms)
                .map(|(k, _)| k.clone());
            let Some(oldest) = oldest else {
                return false;
            };
            self.relations.operations.remove(&oldest);
        }
        true
    }

    /// 运行已结束且请求、清理屏障均结算后，回收上下文及其附属记录。
    pub(crate) fn prune_contexts(&mut self) {
        let stale: Vec<String> = self
            .relations
            .contexts
            .iter()
            .filter(|(run, c)| {
                c.authority == Authority::Revoked
                    && c.cleanup.is_none()
                    && c.members.values().all(|m| m.pending_requests == 0)
                    && !self
                        .claims
                        .values()
                        .any(|cl| cl.run_id.as_deref() == Some(run))
                    && !self
                        .relations
                        .requests
                        .values()
                        .any(|r| r.run_id.as_deref() == Some(run.as_str()))
                    && self
                        .runs
                        .runs
                        .get(run.as_str())
                        .is_none_or(|r| r.ended_at.is_some())
                    && !self.runs.tree_retained.contains(run.as_str())
                    && !self.tree_busy(Some(&c.identity.execution_thread_id))
            })
            .map(|(r, _)| r.clone())
            .collect();
        for run in stale {
            self.relations.contexts.remove(&run);
            self.relations.bindings.retain(|_, b| b.run_id != run);
            self.relations
                .aux
                .member_evidence
                .retain(|(r, _), _| r != &run);
            for op in self
                .relations
                .operations
                .values_mut()
                .filter(|op| op.run_id == run)
            {
                op.outcome = EvidenceOutcome::Revoked;
                op.proven_turns.clear();
            }
            let operations = &self.relations.operations;
            self.relations
                .aux
                .parent_waits
                .retain(|k, _| operations.get(k).is_some_and(|op| op.run_id != run));
            self.relations
                .aux
                .spawn_waits
                .retain(|_, k| operations.get(k).is_some_and(|op| op.run_id != run));
        }
    }

    // ─── 成员 ────────────────────────────────────────────────────────

    /// 重新计算成员快照；变化时加 revision、写运行记录、发 childChanged 并通知 SA-b。
    pub(crate) fn refresh_member(
        &mut self,
        run: &str,
        thread: &str,
        change: MemberChange,
        actions: &mut Vec<Action>,
    ) {
        let Some(context) = self.relations.contexts.get(run) else {
            return;
        };
        let old = context.members.get(thread).cloned();
        let edge = self.thread_relation(thread);
        let bindings: Vec<String> = self
            .relations
            .bindings
            .range((thread.to_owned(), String::new())..)
            .take_while(|((t, _), _)| t == thread)
            .filter(|(_, b)| b.run_id == run && b.status == BindingStatus::Bound)
            .map(|((_, turn), _)| turn.clone())
            .collect();
        let parent = edge
            .as_ref()
            .map(|e| e.parent_thread_id.clone())
            .or_else(|| old.as_ref().map(|m| m.parent.clone()))
            .unwrap_or_default();
        let member = MemberState {
            parent,
            kind: edge.as_ref().map_or(RelationKind::Subagent, |e| e.kind),
            capability: edge.as_ref().and_then(|e| e.can_accept_direct_input),
            agent_version: edge
                .as_ref()
                .map_or(AgentVersion::Unknown, |e| e.agent_version),
            bindings,
            conflict: old.as_ref().and_then(|m| m.conflict.clone()),
            activity: self.member_activity(run, thread),
            pending_requests: self.member_pending(run, thread),
            active_goal: self.records.get(thread).map(|r| r.goal_active),
        };
        if old.as_ref() == Some(&member) {
            return;
        }
        let context = self.relations.contexts.get_mut(run).expect("上下文存在");
        context.members.insert(thread.into(), member);
        self.bump_context_revision(run, thread, change, actions);
    }

    /// 刷新运行全部成员（发现、撤销等批量变化后）。
    pub(crate) fn refresh_members(
        &mut self,
        run: &str,
        change: MemberChange,
        actions: &mut Vec<Action>,
    ) {
        let threads: Vec<String> = self
            .relations
            .contexts
            .get(run)
            .map(|c| c.members.keys().cloned().collect())
            .unwrap_or_default();
        for thread in threads {
            self.refresh_member(run, &thread, change, actions);
        }
    }

    /// 只影响运行整体（在途派生、发现状态）的变化。
    pub(crate) fn touch_context(
        &mut self,
        run: &str,
        change: MemberChange,
        actions: &mut Vec<Action>,
    ) {
        if self.relations.contexts.contains_key(run) {
            self.bump_context_revision(run, "", change, actions);
        }
    }

    pub(crate) fn bump_context_revision(
        &mut self,
        run: &str,
        thread: &str,
        change: MemberChange,
        actions: &mut Vec<Action>,
    ) {
        let Some(context) = self.relations.contexts.get_mut(run) else {
            return;
        };
        context.revision = context.revision.saturating_add(1);
        let revision = context.revision;
        // 同一 revision 的首条通知就必须携带新摘要，客户端可能丢弃后续同号通知。
        self.convergence_sync_facts(run);
        if let Some(epoch) = self.convergences.ledgers.get(run).map(|l| l.work_epoch) {
            if let Some(join) = self
                .runs
                .runs
                .get_mut(run)
                .and_then(|r| r.subagent_join.as_mut())
            {
                join.work_epoch = epoch;
            }
        }
        self.log_member_change(run, thread, change, actions);
        self.persist_children(run, actions);
        self.notify_child_changed(run, thread, actions);
        self.on_member_changed(run, revision, change, actions);
    }

    fn member_activity(&self, run: &str, thread: &str) -> MemberActivity {
        let observed = self.relations.aux.observed.get(thread);
        let open: Vec<&TurnObservation> = observed
            .map(|o| o.iter().filter(|t| !t.completed).collect())
            .unwrap_or_default();
        let bound_open = open.iter().any(|t| {
            self.relations
                .bindings
                .get(&(thread.to_owned(), t.turn.clone()))
                .is_some_and(|b| b.run_id == run && b.status == BindingStatus::Bound)
        });
        if bound_open {
            return MemberActivity::Running;
        }
        let now = self.clock.mono_ms;
        if self
            .relations
            .aux
            .spawn_waits
            .get(thread)
            .and_then(|k| self.relations.operations.get(k))
            .is_some_and(|op| op.run_id == run && self.evidence_valid_for_pairing(op, now))
        {
            return MemberActivity::Starting;
        }
        if !open.is_empty() {
            // 成员上正在跑的轮次没有归属证据：不能说空闲，也不能说是本运行的活动。
            return MemberActivity::Unknown;
        }
        if observed.is_none() && self.relations.aux.unobserved_loaded.contains(thread) {
            // 已加载但本连接从未观察到它，也没有附着证据：活动状态未知。
            return MemberActivity::Unknown;
        }
        MemberActivity::Settled
    }

    fn member_pending(&self, run: &str, thread: &str) -> usize {
        let requests = self
            .relations
            .requests
            .values()
            .filter(|r| {
                r.run_id.as_deref() == Some(run)
                    && r.source_thread_id == thread
                    && routing::live_stage(&r.stage)
            })
            .count();
        requests + self.inbox_guardian_pending(thread, run)
    }

    // ─── 持久化 ──────────────────────────────────────────────────────

    /// threads.json：成员线程的父线程、关系类型、来源摘要、能力、版本。只写自有运行的成员，不写外部线程。
    pub(crate) fn persist_edge(&mut self, thread: &str, actions: &mut Vec<Action>) {
        let Some(edge) = self.relations.edges.get(thread).cloned() else {
            return;
        };
        let member = self
            .relations
            .contexts
            .values()
            .any(|c| c.members.contains_key(thread))
            || self
                .relations
                .operations
                .values()
                .any(|op| op.receivers.contains(thread) && self.op_confirmed(&op.key));
        let indexed = self
            .runs
            .index
            .threads
            .values()
            .any(|e| e.thread_id == thread);
        if !member && !indexed {
            return;
        }
        let short = self
            .runs
            .ensure_thread(thread, self.clock.now_ms, self.clock.entropy);
        let entry = self
            .runs
            .index
            .threads
            .get_mut(&short)
            .expect("线程索引存在");
        let summary = Some(edge.source_summary.clone()).filter(|s| !s.is_null());
        let version = Some(edge.agent_version).filter(|v| *v != AgentVersion::Unknown);
        let changed = entry.parent_thread_id.as_deref() != Some(edge.parent_thread_id.as_str())
            || entry.relation_kind != Some(edge.kind)
            || (summary.is_some() && entry.source_summary != summary)
            || (edge.can_accept_direct_input.is_some()
                && entry.can_accept_direct_input != edge.can_accept_direct_input)
            || (version.is_some() && entry.agent_version != version);
        if !changed {
            return;
        }
        entry.parent_thread_id = Some(edge.parent_thread_id);
        entry.relation_kind = Some(edge.kind);
        if summary.is_some() {
            entry.source_summary = summary;
        }
        if edge.can_accept_direct_input.is_some() {
            entry.can_accept_direct_input = edge.can_accept_direct_input;
        }
        if version.is_some() {
            entry.agent_version = version;
        }
        self.runs.put_threads(actions);
    }

    /// RunRecord.children：子线程、直接父线程、关系类型、已绑定轮次（最多 100）、证据与最终活动。
    fn persist_children(&mut self, run: &str, actions: &mut Vec<Action>) {
        let Some(context) = self.relations.contexts.get(run) else {
            return;
        };
        let mut children = Vec::new();
        for (thread, member) in &context.members {
            if member.kind != RelationKind::Subagent {
                continue;
            }
            let Some(evidence) = self
                .relations
                .aux
                .member_evidence
                .get(&(run.to_owned(), thread.clone()))
                .copied()
            else {
                continue;
            };
            // 清理上下文没有恢复活动绑定；刷新时仍应保留账本中已经证明的历史轮次。
            let mut turn_ids = if context.cleanup.is_some() {
                self.runs
                    .runs
                    .get(run)
                    .and_then(|r| r.children.iter().find(|c| c.thread_id == *thread))
                    .map(|c| c.turn_ids.clone())
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            for turn in &member.bindings {
                if !turn_ids.contains(turn) {
                    turn_ids.push(turn.clone());
                }
            }
            if turn_ids.len() > MAX_TURN_IDS {
                turn_ids.drain(..turn_ids.len() - MAX_TURN_IDS);
            }
            children.push(ChildRecord {
                thread_id: thread.clone(),
                parent_thread_id: member.parent.clone(),
                relation_kind: member.kind,
                turn_ids,
                evidence,
                final_state: member.activity,
            });
        }
        self.run_set_children(run, children, actions);
    }

    // ─── Guardian ────────────────────────────────────────────────────

    /// 子线程的 Guardian 拒绝走同一条解析链：归属本运行才进 gd_；未知时等证据，期限同回查。
    fn guardian_child(
        &mut self,
        thread: &str,
        turn: Option<&str>,
        params: Value,
        actions: &mut Vec<Action>,
    ) {
        if !crate::inbox::guardian_denied(&params) {
            return;
        }
        match self.resolve(thread, turn) {
            Resolution::Bound(run) => self.guardian_deliver(thread, turn, &run, params, actions),
            Resolution::Unknown if self.relations.aux.guardians.len() < LOOKUP_MAX_REQUESTS => {
                let deadline_ms = self.clock.mono_ms.saturating_add(LOOKUP_DEADLINE_MS);
                self.relations.aux.guardians.push(PendingGuardian {
                    thread: thread.into(),
                    turn: turn.map(str::to_owned),
                    params,
                    deadline_ms,
                });
                self.lookup_ensure(thread, actions);
            }
            _ => {}
        }
    }

    fn guardian_deliver(
        &mut self,
        thread: &str,
        turn: Option<&str>,
        run: &str,
        params: Value,
        actions: &mut Vec<Action>,
    ) {
        let Some(context) = self.relations.contexts.get(run) else {
            return;
        };
        if context.authority == Authority::Revoked {
            return;
        }
        let root = context.identity.logical_root_thread_id.clone();
        let parent = self
            .thread_relation(thread)
            .map(|e| e.parent_thread_id)
            .unwrap_or_default();
        let turn = turn
            .map(str::to_owned)
            .or_else(|| self.unique_active_bound_turn(thread));
        self.inbox_guardian_child(thread, run, turn, &root, &parent, &params, actions);
        self.refresh_member(run, thread, MemberChange::Request, actions);
    }

    /// 归属变化后重新判定等待中的 Guardian 通知。
    pub(crate) fn guardian_reconsider(&mut self, thread: &str, actions: &mut Vec<Action>) {
        let waiting: Vec<PendingGuardian> = self
            .relations
            .aux
            .guardians
            .iter()
            .filter(|g| g.thread == thread)
            .cloned()
            .collect();
        for g in waiting {
            match self.resolve(&g.thread, g.turn.as_deref()) {
                Resolution::Unknown => {}
                resolution => {
                    self.relations.aux.guardians.retain(|x| *x != g);
                    if let Resolution::Bound(run) = resolution {
                        self.guardian_deliver(
                            &g.thread,
                            g.turn.as_deref(),
                            &run,
                            g.params,
                            actions,
                        );
                    }
                }
            }
        }
    }

    pub(super) fn guardian_expire(&mut self) {
        let now = self.clock.mono_ms;
        self.relations.aux.guardians.retain(|g| g.deadline_ms > now);
    }

    // ─── 归属判定 ────────────────────────────────────────────────────

    /// 按 (来源线程, 来源轮次) 判定归属。请求没有 turnId 时只认唯一已绑定的活动轮次；
    /// 不拿父线程的轮次回填。
    pub(crate) fn resolve(&self, thread: &str, turn: Option<&str>) -> Resolution {
        let turn = match turn {
            Some(t) => t.to_owned(),
            None => match self.unique_active_bound_turn(thread) {
                Some(t) => t,
                None => {
                    return match self.lineage_owner(thread) {
                        Some(false) => Resolution::External,
                        _ => Resolution::Unknown,
                    }
                }
            },
        };
        match self.relations.bindings.get(&(thread.to_owned(), turn)) {
            Some(b) => match &b.status {
                BindingStatus::Bound => Resolution::Bound(b.run_id.clone()),
                BindingStatus::Unbound => Resolution::Unmanaged("unbound".into()),
                BindingStatus::Conflict(reason) => Resolution::Unmanaged(reason.clone()),
            },
            None => {
                let now = self.clock.mono_ms;
                if self
                    .relations
                    .aux
                    .spawn_waits
                    .get(thread)
                    .and_then(|k| self.relations.operations.get(k))
                    .is_some_and(|op| self.evidence_valid_for_pairing(op, now))
                {
                    return Resolution::Unknown;
                }
                match self.lineage_owner(thread) {
                    Some(false) => Resolution::External,
                    None => Resolution::Unknown,
                    Some(true) => {
                        // 首轮已经处理过（有绑定、有 unbound 记录或派生操作已落定）：后续轮次没有可用证据。
                        let handled = self
                            .relations
                            .bindings
                            .range((thread.to_owned(), String::new())..)
                            .take_while(|((t, _), _)| t == thread)
                            .next()
                            .is_some()
                            || self.relations.operations.values().any(|op| {
                                op.receivers.contains(thread)
                                    && matches!(
                                        op.kind,
                                        EvidenceKind::V1Spawn | EvidenceKind::V2Started
                                    )
                                    && op.outcome != EvidenceOutcome::Pending
                            });
                        if handled {
                            Resolution::Unmanaged("unbound".into())
                        } else {
                            Resolution::Unknown
                        }
                    }
                }
            }
        }
    }

    /// 血缘是否通到某个自有运行：Some(true) 可能属于；Some(false) 已证明外部；None 血缘未知。
    pub(crate) fn lineage_owner(&self, thread: &str) -> Option<bool> {
        let mut current = thread.to_owned();
        for depth in 0..=MAX_DEPTH {
            if depth > 0 && self.thread_owns_work(&current) {
                return Some(true);
            }
            if depth > 0
                && self
                    .relations
                    .bindings
                    .range((current.clone(), String::new())..)
                    .take_while(|((t, _), _)| *t == current)
                    .any(|(_, b)| b.status == BindingStatus::Bound)
            {
                return Some(true);
            }
            match self.thread_relation(&current) {
                Some(edge) => current = edge.parent_thread_id,
                None if self.relations.aux.lineage_roots.contains(&current) => return Some(false),
                None if depth > 0 && self.claims.contains_key(&current) => return Some(false),
                None => return None,
            }
        }
        Some(false)
    }

    /// 线程上有自有运行（自有认领，或某运行上下文仍以它为执行线程/逻辑根且未撤权）。
    fn thread_owns_work(&self, thread: &str) -> bool {
        if let Some(claim) = self.claims.get(thread) {
            if claim.run_id.is_some() && !claim.joined() && !claim.precheck() {
                return true;
            }
        }
        self.relations.contexts.values().any(|c| {
            c.authority != Authority::Revoked
                && (c.identity.execution_thread_id == thread
                    || c.identity.logical_root_thread_id == thread)
        })
    }

    /// 运行仍处于 FinishingTree（SA-b 持有的结束快照）。
    pub(crate) fn run_finishing(&self, run: &str) -> bool {
        self.claims.values().any(|c| {
            c.run_id.as_deref() == Some(run) && matches!(c.state, State::FinishingTree { .. })
        })
    }
}

#[cfg(test)]
mod tree_log_tests {
    use super::*;
    use crate::claim::{Claim, ReqKind, State};
    use crate::runs::RunOpen;
    use crate::store::StoreOp;
    use serde_json::json;

    #[test]
    fn tree_log_独立认领与其他运行绑定冲突只写候选运行() {
        for independent in [true, false] {
            let mut m = Machine::new(false);
            let mut a = vec![];
            for (run, root) in [("candidate", "root"), ("existing", "other")] {
                m.run_opened(
                    RunOpen {
                        run_id: run.into(),
                        thread_id: root.into(),
                        conn: None,
                        kind: ReqKind::TurnStart,
                        joined: false,
                        input: String::new(),
                        continued_from: None,
                    },
                    &mut a,
                );
            }
            let key = EvidenceKey {
                parent_thread_id: "root".into(),
                parent_turn_id: "pt".into(),
                item_id: "spawn".into(),
            };
            if independent {
                m.claims.insert(
                    "child".into(),
                    Claim {
                        stream_id: None,
                        owned_ended: Default::default(),
                        owned_turns: ["ct".into()].into_iter().collect(),
                        claim_id: 7,
                        state: State::Running {
                            owner: crate::claim::Owner::Client(1),
                            turn_id: "ct".into(),
                            joined: false,
                        },
                        run_id: Some("existing".into()),
                        resume_goal: false,
                        request_pending: false,
                        owner_gone: false,
                        pending_start: None,
                        submitted_input: vec![],
                        held_requests: vec![],
                        held_deltas: 0,
                        held_bytes: 0,
                        held_dropped_items: 0,
                        overflowed: false,
                        settlement_reply: None,
                        pending_detach: None,
                    },
                );
            } else {
                m.relations.bindings.insert(
                    ("child".into(), "ct".into()),
                    ActivityBinding {
                        thread_id: "child".into(),
                        turn_id: "ct".into(),
                        run_id: "existing".into(),
                        parent_thread_id: "other".into(),
                        evidence: BindingEvidence::V1Spawn,
                        operation: key.clone(),
                        status: BindingStatus::Bound,
                    },
                );
            }
            a.clear();
            m.bind(
                "child",
                "ct",
                "candidate",
                BindingEvidence::V1Spawn,
                &key,
                &mut a,
            );
            let lines: Vec<_> = a
                .iter()
                .filter_map(|a| match a {
                    Action::Store(StoreOp::AppendLog { short_id, lines }) => {
                        Some((short_id, lines))
                    }
                    _ => None,
                })
                .collect();
            assert_eq!(lines.len(), 1);
            assert_eq!(*lines[0].0, m.runs.runs["candidate"].short_id);
            assert!(lines[0].1[0].contains(" candidate 归属冲突 @cxb2 "));
            let payload: Value =
                serde_json::from_str(lines[0].1[0].split("@cxb2 ").nth(1).unwrap()).unwrap();
            assert_eq!(payload["threadId"], "child");
            assert_eq!(payload["parentThreadId"], "root");
            assert_eq!(payload["coverageIssue"], "discoveryIncomplete");
            let binding = m.binding("child", "ct").unwrap();
            if independent {
                assert!(matches!(binding.status, BindingStatus::Conflict(_)));
            } else {
                assert_eq!(binding.run_id, "existing");
            }
            a.clear();
            m.log_bound_child_event("candidate", "child", "ct", "item/completed", &Some(json!({"turnId":"ct","item":{"id":"i","type":"agentMessage","text":"不可串账"}})), &mut a);
            assert!(!a
                .iter()
                .any(|a| matches!(a, Action::Store(StoreOp::AppendLog { .. }))));
        }
    }
}
