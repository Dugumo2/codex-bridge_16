//! 子线程服务端请求：全序路由、回查队列、撤销与墓碑、交接与收尾（接口第 4、5、6 节）。
//!
//! 子线程请求不进父认领的 held_requests，另记在 `Relations::requests`，键为 (连接 epoch, 上游 requestId)。
//! 处理子线程请求时一律不发 Dismiss：确定不答就静默丢弃，避免清掉上游去重登记。
//! 回包始终用原始上游 requestId（ServerResult/ServerError/AppReplyAcked 的 id），经原 app-server 连接返回。
use super::*;
use crate::claim::{ConnId, Owner};
use crate::inbox::InboxOrigin;
use crate::policy::{self, Policy};
use crate::proto;
use serde_json::json;

const DYNAMIC_TOOL: &str = "item/tool/call";
const SHUTDOWN: &str = "Broker shutting down";

/// 仍需要有人处理的阶段（计入待答）。
pub(crate) fn live_stage(stage: &ChildRequestStage) -> bool {
    !matches!(
        stage,
        ChildRequestStage::Answered | ChildRequestStage::Revoked
    )
}

fn request_key(epoch: u64, id: &Value) -> (u64, String) {
    (epoch, proto::request_id_key(id))
}

fn held(record: &ChildRequest, forward_id: String) -> HeldRequest {
    HeldRequest {
        id: record.upstream_request_id.clone(),
        forward_id,
        method: record.method.clone(),
        params: record.params.clone(),
    }
}

impl Machine {
    /// 是否存在任何可能拥有子请求的运行；没有时保持旧路由（无认领线程的请求按原规则交还）。
    fn relations_live(&self) -> bool {
        self.claims
            .values()
            .any(|c| c.run_id.is_some() && !c.joined() && !c.precheck())
            || self
                .relations
                .contexts
                .values()
                .any(|c| c.authority != Authority::Revoked || c.cleanup.is_some())
    }

    pub(super) fn routing_arrive(
        &mut self,
        request: &HeldRequest,
        actions: &mut Vec<Action>,
    ) -> bool {
        let Some(thread) = request
            .params
            .as_ref()
            .and_then(|p| p.get("threadId"))
            .and_then(Value::as_str)
            .map(str::to_owned)
        else {
            return false;
        };
        // 第 3 条：来源线程自己有认领，走现有逻辑。
        if self.claims.contains_key(&thread) {
            return false;
        }
        let key = request_key(self.relations.conn_epoch, &request.id);
        let known = self.relations.tombstones.contains_key(&key)
            || self.relations.requests.contains_key(&key)
            || self.thread_relation(&thread).is_some();
        if !known && !self.relations_live() {
            return false;
        }
        let turn = request
            .params
            .as_ref()
            .and_then(|p| p.get("turnId"))
            .and_then(Value::as_str)
            .filter(|t| !t.is_empty())
            .map(str::to_owned);
        // 第 1 条：关闭中，已证明自有的按关闭规则回错误，其余不答。
        if self.closing {
            if matches!(self.resolve(&thread, turn.as_deref()), Resolution::Bound(_)) {
                actions.push(Action::ServerError {
                    id: request.id.clone(),
                    error: crate::requests::handler_error(SHUTDOWN),
                });
            }
            return true;
        }
        // 第 2 条：已撤销（墓碑命中）或重复到达的请求丢弃。
        if self.relations.tombstones.contains_key(&key) {
            actions.push(Action::Debug(format!(
                "子线程请求 {} 已被上游关闭，丢弃",
                key.1
            )));
            return true;
        }
        if self.relations.requests.contains_key(&key) {
            actions.push(Action::Debug(format!("重复的子线程请求 {}，丢弃", key.1)));
            return true;
        }
        self.attach(&thread, AttachmentSource::ServerRequest);
        if let Some(turn) = &turn {
            self.observe_turn(&thread, turn, false, false, actions);
        }
        let bytes = request.method.len()
            + request
                .params
                .as_ref()
                .map_or(0, |p| serde_json::to_vec(p).map_or(0, |v| v.len()));
        if self.resolve(&thread, turn.as_deref()) == Resolution::Unknown {
            let lookups: Vec<&ChildRequest> = self
                .relations
                .requests
                .values()
                .filter(|r| r.stage == ChildRequestStage::Lookup)
                .collect();
            let per_thread = lookups
                .iter()
                .filter(|r| r.source_thread_id == thread)
                .count();
            let used: usize = self
                .relations
                .requests
                .iter()
                .filter(|(_, r)| r.stage == ChildRequestStage::Lookup)
                .filter_map(|(k, _)| self.relations.aux.request_meta.get(k))
                .map(|m| m.bytes)
                .sum();
            if lookups.len() >= LOOKUP_MAX_REQUESTS
                || per_thread >= LOOKUP_PER_THREAD
                || used.saturating_add(bytes) > LOOKUP_MAX_BYTES
            {
                actions.push(Action::Warn(format!(
                    "子线程请求回查队列已满，归属未知的请求 {} 不答",
                    key.1
                )));
                self.write_tombstone(key.clone(), actions);
                return true;
            }
        }
        let now = self.clock.now_ms;
        self.relations.requests.insert(
            key.clone(),
            ChildRequest {
                conn_epoch: self.relations.conn_epoch,
                upstream_request_id: request.id.clone(),
                method: request.method.clone(),
                params: request.params.clone(),
                source_thread_id: thread,
                source_turn_id: turn,
                run_id: None,
                received_at_ms: now,
                deadline_ms: now.saturating_add(crate::inbox::REQUEST_EXPIRY_MS),
                stage: ChildRequestStage::Lookup,
            },
        );
        self.relations.aux.request_meta.insert(
            key.clone(),
            RequestMeta {
                forward_id: request.forward_id.clone(),
                lookup_deadline_ms: self.clock.mono_ms.saturating_add(LOOKUP_DEADLINE_MS),
                bytes,
                from_held: false,
                inbox_item: None,
                revoke_requested: false,
            },
        );
        self.route_record(&key, actions);
        true
    }

    /// 从第 1 条起重新判定一条记录（证据、回查结果、定案后都会回到这里）。
    pub(crate) fn route_record(&mut self, key: &(u64, String), actions: &mut Vec<Action>) {
        let Some(record) = self.relations.requests.get(key).cloned() else {
            return;
        };
        if !matches!(
            record.stage,
            ChildRequestStage::Lookup | ChildRequestStage::Held
        ) {
            return;
        }
        let meta = self.relations.aux.request_meta.get(key).cloned();
        let forward_id = meta
            .as_ref()
            .map(|m| m.forward_id.clone())
            .unwrap_or_default();
        if self.claims.contains_key(&record.source_thread_id) {
            // 来源线程后来有了自己的认领：交回现有逻辑。
            // 请求仍然存活，RPC 去重登记随接手方结算，不进入关系墓碑。
            self.relations.aux.request_meta.remove(key);
            self.relations.requests.remove(key);
            let from_held = meta.is_some_and(|m| m.from_held);
            self.server_request(held(&record, forward_id), from_held, actions);
            return;
        }
        let resolution = self.resolve(&record.source_thread_id, record.source_turn_id.as_deref());
        let run = match resolution {
            Resolution::Unknown => {
                self.set_stage(key, ChildRequestStage::Lookup);
                self.lookup_ensure(&record.source_thread_id, actions);
                return;
            }
            Resolution::External => {
                self.drop_request(key, "外部线程", actions);
                return;
            }
            Resolution::Unmanaged(reason) => {
                self.drop_request(key, &reason, actions);
                return;
            }
            Resolution::Bound(run) => run,
        };
        if let Some(r) = self.relations.requests.get_mut(key) {
            r.run_id = Some(run.clone());
        }
        self.ensure_context(&run);
        let Some(context) = self.relations.contexts.get(&run) else {
            self.drop_request(key, "运行已结束", actions);
            return;
        };
        // 第 6 条：撤权的运行不答。
        if context.authority == Authority::Revoked && context.cleanup.is_none() {
            self.drop_request(key, "运行已撤权", actions);
            return;
        }
        // 第 7 条：停止、FinishingTree 或结束后的清理范围，按冻结策略处理，不转回旧 CLI、不重新回查。
        if let Some(policy) = self.cleanup_policy(&run) {
            self.cleanup_route(key, &policy, actions);
            return;
        }
        let authority = context.authority.clone();
        let execution = context.identity.execution_thread_id.clone();
        // 第 8 条：父认领预检或 settling 时暂存，定案后重新判定。
        if let Some(claim) = self
            .claims
            .get(&execution)
            .filter(|c| c.run_id.as_deref() == Some(run.as_str()))
        {
            if claim.settling() || claim.precheck() {
                self.set_stage(key, ChildRequestStage::Held);
                self.refresh_member(
                    &run,
                    &record.source_thread_id,
                    MemberChange::Request,
                    actions,
                );
                return;
            }
        }
        let from_held = meta.is_some_and(|m| m.from_held);
        match authority {
            Authority::Owner(conn) => {
                let online = self.live_conns.contains(&conn)
                    && self
                        .claims
                        .get(&execution)
                        .is_some_and(|c| !c.owner_gone && c.owner() == Some(&Owner::Client(conn)));
                if record.method == DYNAMIC_TOOL {
                    // 动态工具沿用现有规则，用运行的认领代替来源线程的认领判断。
                    if online && (from_held || !self.shared) {
                        self.child_forward(key, conn, actions);
                    } else {
                        self.drop_request(key, "动态工具无可转发的 owner", actions);
                    }
                } else if online {
                    self.child_forward(key, conn, actions);
                } else {
                    // 已证明自有但 owner 不在线：按现有失败规则回错误。
                    let request = held(&record, forward_id);
                    self.forget_request(key, actions);
                    crate::requests::reject_no_client(request, actions);
                    self.refresh_member(
                        &run,
                        &record.source_thread_id,
                        MemberChange::Request,
                        actions,
                    );
                }
            }
            Authority::Broker { policy } => self.hosted_child(key, policy, actions),
            Authority::Cleanup { .. } | Authority::Revoked => {
                self.drop_request(key, "运行处于清理且无清理策略", actions)
            }
        }
    }

    fn set_stage(&mut self, key: &(u64, String), stage: ChildRequestStage) {
        if let Some(r) = self.relations.requests.get_mut(key) {
            r.stage = stage;
        }
    }

    fn forget_request(
        &mut self,
        key: &(u64, String),
        actions: &mut Vec<Action>,
    ) -> Option<ChildRequest> {
        self.write_tombstone(key.clone(), actions);
        self.relations.aux.request_meta.remove(key);
        self.relations.requests.remove(key)
    }

    /// 确定不答：静默丢弃并记诊断，不发 Dismiss。
    fn drop_request(&mut self, key: &(u64, String), reason: &str, actions: &mut Vec<Action>) {
        let Some(record) = self.forget_request(key, actions) else {
            return;
        };
        actions.push(Action::Debug(format!(
            "子线程 {} 的请求 {} 不答：{reason}",
            record.source_thread_id, key.1
        )));
        if let Some(run) = &record.run_id {
            self.refresh_member(
                run,
                &record.source_thread_id,
                MemberChange::Request,
                actions,
            );
        }
    }

    /// 第 9 条：转发给运行 owner；期限从首次收到算起，各阶段不重新计时。
    fn child_forward(&mut self, key: &(u64, String), conn: ConnId, actions: &mut Vec<Action>) {
        let Some(record) = self.relations.requests.get(key).cloned() else {
            return;
        };
        let run = record.run_id.clone().unwrap_or_default();
        let mut forward_id = self
            .relations
            .aux
            .request_meta
            .get(key)
            .map(|m| m.forward_id.clone())
            .unwrap_or_default();
        while forward_id.is_empty() || self.requests.pending.contains_key(&forward_id) {
            forward_id = format!("{forward_id}-sa{}", self.relations.aux.next());
        }
        if let Some(meta) = self.relations.aux.request_meta.get_mut(key) {
            meta.forward_id = forward_id.clone();
        }
        let gen = self.relations.contexts.get(&run).map_or(0, |c| c.gen);
        self.set_stage(
            key,
            ChildRequestStage::Forwarded {
                forward_id: forward_id.clone(),
                conn,
                gen,
            },
        );
        self.forward_child(
            held(&record, forward_id),
            record.source_thread_id.clone(),
            conn,
            record.deadline_ms,
            actions,
        );
        self.refresh_member(
            &run,
            &record.source_thread_id,
            MemberChange::Request,
            actions,
        );
    }

    /// 第 10 条：按运行的托管策略应答；需要人工的进收件箱，沿用原截止时间。
    fn hosted_child(&mut self, key: &(u64, String), policy: Policy, actions: &mut Vec<Action>) {
        let Some(record) = self.relations.requests.get(key).cloned() else {
            return;
        };
        let params = record.params.clone().unwrap_or_else(|| json!({}));
        if record.method == DYNAMIC_TOOL
            && self.convergence_enabled()
            && record
                .run_id
                .as_ref()
                .is_some_and(|run| self.convergences.runs.contains_key(run))
        {
            // 托管交接保留待答动态工具及原期限；不能用“不支持”代答来消除收敛阻塞。
            self.child_inbox(key, actions);
            return;
        }
        match policy::decide(policy, &record.method, &params) {
            policy::Decision::Answer(result) => {
                self.forget_request(key, actions);
                actions.push(Action::ServerResult {
                    id: record.upstream_request_id.clone(),
                    result,
                });
            }
            policy::Decision::Inbox => {
                self.child_inbox(key, actions);
                return;
            }
            policy::Decision::MethodNotFound => {
                self.forget_request(key, actions);
                let forward_id = String::new();
                crate::requests::method_not_found(held(&record, forward_id), actions);
            }
        }
        if let Some(run) = &record.run_id {
            self.refresh_member(
                run,
                &record.source_thread_id,
                MemberChange::Request,
                actions,
            );
        }
    }

    /// 进收件箱：threadId 保持来源线程，另记逻辑根、父线程、运行和子轮次。
    fn child_inbox(&mut self, key: &(u64, String), actions: &mut Vec<Action>) {
        let Some(record) = self.relations.requests.get(key).cloned() else {
            return;
        };
        let run = record.run_id.clone().unwrap_or_default();
        let root = self
            .relations
            .contexts
            .get(&run)
            .map(|c| c.identity.logical_root_thread_id.clone());
        let parent = self
            .thread_relation(&record.source_thread_id)
            .map(|e| e.parent_thread_id);
        let origin = InboxOrigin {
            thread_id: record.source_thread_id.clone(),
            claim_id: 0,
            run_id: Some(run.clone()),
            turn_id: record.source_turn_id.clone(),
        };
        let forward_id = self
            .relations
            .aux
            .request_meta
            .get(key)
            .map(|m| m.forward_id.clone())
            .unwrap_or_default();
        // 先占住阶段，登记过程中的通知与计数看到的就是收件箱阶段。
        self.set_stage(
            key,
            ChildRequestStage::Inbox {
                item_id: String::new(),
            },
        );
        match self.inbox_register_child_request(
            origin,
            root,
            parent,
            held(&record, forward_id),
            record.deadline_ms,
            actions,
        ) {
            Some(item_id) => {
                self.set_stage(
                    key,
                    ChildRequestStage::Inbox {
                        item_id: item_id.clone(),
                    },
                );
                if let Some(meta) = self.relations.aux.request_meta.get_mut(key) {
                    meta.inbox_item = Some(item_id);
                }
            }
            // 容量已满时登记出口已按失败规则回答。
            None => {
                self.forget_request(key, actions);
            }
        }
        self.refresh_member(
            &run,
            &record.source_thread_id,
            MemberChange::Request,
            actions,
        );
    }

    // ─── 清理路由（审查三第 2、5 条） ────────────────────────────────

    /// 冻结的清理策略：显式清理上下文优先；运行有在途树停止或处于 FinishingTree 时按当前授权快照。
    pub(crate) fn cleanup_policy(&self, run: &str) -> Option<CleanupRequestPolicy> {
        let context = self.relations.contexts.get(run)?;
        if let Some(cleanup) = &context.cleanup {
            return Some(CleanupRequestPolicy {
                shared: self.shared,
                policy: cleanup.policy,
                authority_gen: cleanup.gen,
                owner_before_stop: None,
                tree_op: cleanup.tree_op,
                ended: cleanup.committed,
            });
        }
        let tree_op = self
            .trees
            .operations
            .values()
            .find(|op| !op.cancelled && op.request.snapshot.identity.run_id == run)
            .map(|op| op.tree_op);
        if tree_op.is_none()
            && !self.run_finishing(run)
            && !matches!(context.authority, Authority::Cleanup { .. })
        {
            return None;
        }
        let policy = match context.authority {
            Authority::Broker { policy } => policy,
            _ => context.policy,
        };
        Some(CleanupRequestPolicy {
            shared: self.shared,
            policy,
            authority_gen: context.gen,
            owner_before_stop: None,
            tree_op: tree_op.unwrap_or(match context.authority {
                Authority::Cleanup { tree_op } => tree_op,
                _ => 0,
            }),
            ended: matches!(context.authority, Authority::Cleanup { .. }),
        })
    }

    fn cleanup_route(
        &mut self,
        key: &(u64, String),
        policy: &CleanupRequestPolicy,
        actions: &mut Vec<Action>,
    ) {
        let Some(record) = self.relations.requests.get(key).cloned() else {
            return;
        };
        if record.method == DYNAMIC_TOOL {
            if policy.shared {
                self.drop_request(key, "清理期间共享模式不接管动态工具", actions);
            } else {
                self.forget_request(key, actions);
                crate::requests::method_not_found(held(&record, String::new()), actions);
                if let Some(run) = &record.run_id {
                    self.refresh_member(
                        run,
                        &record.source_thread_id,
                        MemberChange::Request,
                        actions,
                    );
                }
            }
            return;
        }
        self.hosted_child(key, policy.policy, actions);
    }

    /// SA-b 停止开始时调用：冻结清理策略，已转发的子请求撤回并按清理策略重新进入收件箱或应答，原期限不变。
    pub fn relations_enter_cleanup(&mut self, run: &str, tree_op: u64, actions: &mut Vec<Action>) {
        if !self.ensure_context(run) && !self.relations.contexts.contains_key(run) {
            return;
        }
        let bindings: Vec<ActivityBinding> = self
            .relations
            .bindings
            .values()
            .filter(|b| b.run_id == run && b.status == BindingStatus::Bound)
            .cloned()
            .collect();
        let context = self.relations.contexts.get(run).expect("上下文存在");
        let policy = match context.authority {
            Authority::Broker { policy } => policy,
            _ => context.policy,
        };
        let scope: BTreeSet<String> = context.members.keys().cloned().collect();
        let subscriptions = scope
            .iter()
            .filter_map(|t| {
                self.relations
                    .attachments
                    .get(t)
                    .map(|a| (t.clone(), a.clone()))
            })
            .collect();
        let gen = context.gen;
        let context = self.relations.contexts.get_mut(run).expect("上下文存在");
        context.cleanup = Some(CleanupContext {
            tree_op,
            gen,
            policy,
            scope,
            bindings,
            subscriptions,
            barrier: true,
            committed: false,
        });
        self.withdraw_forwarded(run, actions);
    }

    /// SA-b 树结束提交时调用：撤销旧 owner 回答权，保留既定范围的清理授权。
    pub fn relations_commit_cleanup(&mut self, run: &str, tree_op: u64, actions: &mut Vec<Action>) {
        if let Some(context) = self.relations.contexts.get_mut(run) {
            context.gen = context.gen.saturating_add(1);
            context.authority = Authority::Cleanup { tree_op };
            if let Some(cleanup) = context.cleanup.as_mut() {
                cleanup.committed = true;
                cleanup.gen = context.gen;
            }
        }
        self.routing_revoke_run(run, actions);
        self.refresh_members(run, MemberChange::Request, actions);
    }

    /// SA-b 停止确认或外部接管后调用：终结清理权、订阅与屏障。
    pub fn relations_end_cleanup(&mut self, run: &str, actions: &mut Vec<Action>) {
        if let Some(context) = self.relations.contexts.get_mut(run) {
            context.cleanup = None;
            context.gen = context.gen.saturating_add(1);
            context.authority = Authority::Revoked;
        }
        self.routing_revoke_run(run, actions);
        self.refresh_members(run, MemberChange::Request, actions);
        // 清理范围内的子线程此前被留住订阅；根已释放时随清理结束一并退订。
        self.release_idle_children(None, actions);
    }

    /// 撤回运行名下已转发的子请求，按当前授权与策略重新判定；旧 forwardId 失效，旧 CLI 迟到回答被忽略。
    fn withdraw_forwarded(&mut self, run: &str, actions: &mut Vec<Action>) {
        let keys: Vec<((u64, String), String)> = self
            .relations
            .requests
            .iter()
            .filter(|(_, r)| r.run_id.as_deref() == Some(run))
            .filter_map(|(k, r)| match &r.stage {
                ChildRequestStage::Forwarded { forward_id, .. } => {
                    Some((k.clone(), forward_id.clone()))
                }
                _ => None,
            })
            .collect();
        for (key, forward_id) in keys {
            // 先改阶段，撤转发的回调不会把它当成已回答。
            self.set_stage(&key, ChildRequestStage::Held);
            self.take_forward(&forward_id, actions);
            if let Some(meta) = self.relations.aux.request_meta.get_mut(&key) {
                meta.from_held = true;
            }
            self.route_record(&key, actions);
        }
    }

    // ─── 授权变化与收尾 ──────────────────────────────────────────────

    pub(super) fn routing_handoff(
        &mut self,
        run: &str,
        kind: HandoffKind,
        actions: &mut Vec<Action>,
    ) {
        if !self.relations.contexts.contains_key(run) {
            return;
        }
        let claim_policy = self
            .claims
            .values()
            .find(|c| c.run_id.as_deref() == Some(run))
            .and_then(|c| match c.owner() {
                Some(Owner::Broker { policy, .. }) => Some(*policy),
                _ => None,
            });
        let context = self.relations.contexts.get_mut(run).expect("上下文存在");
        context.gen = context.gen.saturating_add(1);
        match kind {
            HandoffKind::Detach => {
                let policy = claim_policy.unwrap_or(context.policy);
                context.policy = policy;
                context.authority = Authority::Broker { policy };
                // 原子接管：已转发给旧 owner 的子请求转入策略或收件箱，保留来源与原截止时间；
                // 回查、暂存中的请求解析后按新授权处理。
                self.withdraw_forwarded(run, actions);
            }
            HandoffKind::OwnerGone => {
                let policy = context.policy;
                context.authority = Authority::Broker { policy };
            }
            HandoffKind::Joined | HandoffKind::External => {
                context.authority = Authority::Revoked;
                self.routing_revoke_run(run, actions);
            }
        }
        self.refresh_members(run, MemberChange::Request, actions);
    }

    /// 按运行撤权；排队回答尝试取消，已写出的回答按提交边界结算一次。
    pub(super) fn routing_revoke_run(&mut self, run: &str, actions: &mut Vec<Action>) {
        let keys: Vec<(u64, String)> = self
            .relations
            .requests
            .iter()
            .filter(|(_, r)| r.run_id.as_deref() == Some(run))
            .map(|(k, _)| k.clone())
            .collect();
        for key in keys {
            let Some(record) = self.relations.requests.get(&key).cloned() else {
                continue;
            };
            match record.stage {
                ChildRequestStage::Writing { op, .. } => {
                    if let Some(meta) = self.relations.aux.request_meta.get_mut(&key) {
                        if !meta.revoke_requested {
                            meta.revoke_requested = true;
                            actions.push(Action::CancelAppReply { op });
                        }
                    }
                    continue;
                }
                ChildRequestStage::Forwarded { forward_id, .. } => {
                    self.set_stage(&key, ChildRequestStage::Revoked);
                    self.take_forward(&forward_id, actions);
                }
                ChildRequestStage::Inbox { item_id } => {
                    self.set_stage(&key, ChildRequestStage::Revoked);
                    self.inbox_revoke_child(&item_id, actions);
                }
                _ => {}
            }
            self.forget_request(&key, actions);
            actions.push(Action::Debug(format!(
                "运行 {run} 收尾，撤销子线程 {} 的请求 {}",
                record.source_thread_id, key.1
            )));
        }
        self.inbox_revoke_child_guardians(run, actions);
        self.relations.aux.guardians.retain(|g| {
            !matches!(self.relations.bindings.get(&(g.thread.clone(), g.turn.clone().unwrap_or_default())), Some(b) if b.run_id == run)
        });
    }

    /// 释放认领时补上子线程：执行线程是它、且运行已无认领的上下文，按运行撤销并撤权（清理授权除外）。
    pub(crate) fn relations_claim_released(&mut self, thread: &str, actions: &mut Vec<Action>) {
        let runs: Vec<String> = self
            .relations
            .contexts
            .iter()
            .filter(|(run, c)| {
                c.identity.execution_thread_id == thread
                    && !self
                        .claims
                        .values()
                        .any(|cl| cl.run_id.as_deref() == Some(run.as_str()))
            })
            .map(|(r, _)| r.clone())
            .collect();
        for run in runs {
            let context = self.relations.contexts.get_mut(&run).expect("上下文存在");
            if context.cleanup.is_none()
                && !matches!(context.authority, Authority::Cleanup { .. })
                && context.authority != Authority::Revoked
            {
                context.gen = context.gen.saturating_add(1);
                context.authority = Authority::Revoked;
            }
            self.routing_revoke_run(&run, actions);
            self.refresh_members(&run, MemberChange::Request, actions);
        }
    }

    /// 父认领定案后，回放暂存在该线程运行上的子请求。
    pub(super) fn reroute_held(&mut self, thread: &str, actions: &mut Vec<Action>) {
        let runs: BTreeSet<String> = self
            .relations
            .contexts
            .iter()
            .filter(|(_, c)| c.identity.execution_thread_id == thread)
            .map(|(r, _)| r.clone())
            .collect();
        let keys: Vec<(u64, String)> = self
            .relations
            .requests
            .iter()
            .filter(|(_, r)| {
                r.stage == ChildRequestStage::Held
                    && r.run_id.as_ref().is_some_and(|run| runs.contains(run))
            })
            .map(|(k, _)| k.clone())
            .collect();
        for key in keys {
            if let Some(meta) = self.relations.aux.request_meta.get_mut(&key) {
                meta.from_held = true;
            }
            self.route_record(&key, actions);
        }
    }

    /// 证据或血缘变化后，回查中的请求与等待中的 Guardian 通知全部重新判定。
    pub(crate) fn routing_reconsider(&mut self, thread: &str, actions: &mut Vec<Action>) {
        let keys: Vec<(u64, String)> = self
            .relations
            .requests
            .iter()
            .filter(|(_, r)| r.stage == ChildRequestStage::Lookup)
            .map(|(k, _)| k.clone())
            .collect();
        for key in keys {
            self.route_record(&key, actions);
        }
        let threads: BTreeSet<String> = self
            .relations
            .aux
            .guardians
            .iter()
            .map(|g| g.thread.clone())
            .chain(std::iter::once(thread.to_owned()))
            .collect();
        for t in threads {
            self.guardian_reconsider(&t, actions);
        }
    }

    // ─── 撤销与墓碑 ──────────────────────────────────────────────────

    /// serverRequest/resolved：任何阶段都转为撤销，撤掉转发、收件箱事项（由原有出口处理）与计时；
    /// 无认领线程上的 resolved 另记墓碑，迟到的同 ID 请求不会复活。
    pub(super) fn routing_resolved(
        &mut self,
        thread: &str,
        claimed: bool,
        id: &Value,
        actions: &mut Vec<Action>,
    ) {
        let key = request_key(self.relations.conn_epoch, id);
        if let Some(record) = self.relations.requests.get(&key).cloned() {
            if let ChildRequestStage::Forwarded { forward_id, .. } = &record.stage {
                self.set_stage(&key, ChildRequestStage::Revoked);
                self.take_forward(forward_id, actions);
            }
            // 收件箱阶段的事项由 resolve_held_request → inbox_server_resolved 按来源线程关闭。
            if !matches!(
                record.stage,
                ChildRequestStage::Inbox { .. } | ChildRequestStage::Writing { .. }
            ) {
                self.forget_request(&key, actions);
            }
            if let Some(run) = &record.run_id {
                self.refresh_member(
                    run,
                    &record.source_thread_id,
                    MemberChange::Request,
                    actions,
                );
            }
        }
        if claimed {
            return;
        }
        let _ = thread;
        self.write_tombstone(key, actions);
    }

    fn write_tombstone(&mut self, key: (u64, String), actions: &mut Vec<Action>) {
        let expires = self.clock.mono_ms.saturating_add(TOMBSTONE_MS);
        let tombstones = &mut self.relations.tombstones;
        if !tombstones.contains_key(&key) && tombstones.len() >= MAX_TOMBSTONES {
            if let Some(oldest) = tombstones
                .iter()
                .min_by_key(|(k, at)| (**at, (*k).clone()))
                .map(|(k, _)| k.clone())
            {
                tombstones.remove(&oldest);
                actions.push(Action::ForgetServerRequest {
                    request_key: oldest.1.clone(),
                });
                actions.push(Action::Warn(format!(
                    "子线程请求墓碑达到上限 {MAX_TOMBSTONES}，淘汰最旧一条 {}",
                    oldest.1
                )));
            }
        }
        tombstones.insert(key, expires);
        self.schedule_tombstones(actions);
    }

    fn schedule_tombstones(&mut self, actions: &mut Vec<Action>) {
        let Some(next) = self.relations.tombstones.values().min().copied() else {
            if self.relations.aux.tombstone_timer.take().is_some() {
                actions.push(Action::Unschedule {
                    key: crate::claim::TimerKey::TombstoneExpire(self.relations.conn_epoch),
                });
            }
            return;
        };
        if self
            .relations
            .aux
            .tombstone_timer
            .is_some_and(|(at, _)| at == next)
        {
            return;
        }
        let generation = self.relations.aux.next();
        self.relations.aux.tombstone_timer = Some((next, generation));
        actions.push(Action::Schedule {
            token: Self::relation_token(),
            kind: TimerKind::TombstoneExpire {
                conn_epoch: self.relations.conn_epoch,
                generation,
            },
            delay_ms: next.saturating_sub(self.clock.mono_ms),
        });
    }

    pub(super) fn tombstone_timer(&mut self, generation: u64, actions: &mut Vec<Action>) {
        if !self
            .relations
            .aux
            .tombstone_timer
            .is_some_and(|(_, g)| g == generation)
        {
            return;
        }
        self.relations.aux.tombstone_timer = None;
        let now = self.clock.mono_ms;
        self.relations.tombstones.retain(|key, at| {
            if *at <= now {
                actions.push(Action::ForgetServerRequest {
                    request_key: key.1.clone(),
                });
                false
            } else {
                true
            }
        });
        self.schedule_tombstones(actions);
    }

    // ─── 转发、收件箱与写入确认的回调 ────────────────────────────────

    /// 转发被回答、超时、断开、关闭或撤回时（requests::take_forward）。
    pub(crate) fn child_forward_taken(&mut self, forward_id: &str, actions: &mut Vec<Action>) {
        let key = self
            .relations
            .requests
            .iter()
            .find_map(|(k, r)| match &r.stage {
                ChildRequestStage::Forwarded { forward_id: f, .. } if f == forward_id => {
                    Some(k.clone())
                }
                _ => None,
            });
        let Some(key) = key else {
            return;
        };
        let record = self.forget_request(&key, actions).expect("子线程请求存在");
        if let Some(run) = &record.run_id {
            self.refresh_member(
                run,
                &record.source_thread_id,
                MemberChange::Request,
                actions,
            );
        }
    }

    /// 收件箱开始带确认写出回答：进入 Writing 阶段。
    pub(crate) fn child_inbox_writing(&mut self, item_id: &str, op: u64) {
        let gen_of = |run: &Option<String>, m: &Machine| {
            run.as_ref()
                .and_then(|r| m.relations.contexts.get(r))
                .map_or(0, |c| c.gen)
        };
        let key = self
            .relations
            .requests
            .iter()
            .find_map(|(k, r)| match &r.stage {
                ChildRequestStage::Inbox { item_id: i } if i == item_id => Some(k.clone()),
                _ => None,
            });
        if let Some(key) = key {
            let gen = gen_of(&self.relations.requests[&key].run_id, self);
            self.set_stage(&key, ChildRequestStage::Writing { op, gen });
        }
    }

    /// 写入确认：Written 已提交；撤权后的失败或取消关闭事项，其余失败才允许重试。
    pub(super) fn routing_reply_written(
        &mut self,
        op: u64,
        outcome: &crate::inbox::ReplyOutcome,
        actions: &mut Vec<Action>,
    ) {
        if *outcome == crate::inbox::ReplyOutcome::Written {
            return;
        }
        let key = self
            .relations
            .requests
            .iter()
            .find_map(|(k, r)| match &r.stage {
                ChildRequestStage::Writing { op: o, .. } if *o == op => Some(k.clone()),
                _ => None,
            });
        if let Some(key) = key {
            let item_id = self
                .relations
                .aux
                .request_meta
                .get(&key)
                .and_then(|m| m.inbox_item.clone())
                .unwrap_or_default();
            if self
                .relations
                .aux
                .request_meta
                .get(&key)
                .is_some_and(|m| m.revoke_requested)
            {
                // 写入结果已到达：清掉旧 op 后按普通撤销出口关闭，不能回到可重试状态。
                self.inbox_cancel_child_reply(&item_id, actions);
            } else {
                self.set_stage(&key, ChildRequestStage::Inbox { item_id });
            }
        }
    }

    /// 收件箱事项关闭（回答、过期、撤销）：子请求结束。
    pub(crate) fn child_inbox_closed(&mut self, item_id: &str, actions: &mut Vec<Action>) {
        let key = self
            .relations
            .requests
            .iter()
            .find_map(|(k, r)| match &r.stage {
                ChildRequestStage::Inbox { item_id: i } if i == item_id => Some(k.clone()),
                ChildRequestStage::Writing { .. }
                    if self
                        .relations
                        .aux
                        .request_meta
                        .get(k)
                        .and_then(|m| m.inbox_item.as_deref())
                        == Some(item_id) =>
                {
                    Some(k.clone())
                }
                _ => None,
            });
        let Some(key) = key else {
            return;
        };
        let record = self.forget_request(&key, actions).expect("子线程请求存在");
        if let Some(run) = &record.run_id {
            self.refresh_member(
                run,
                &record.source_thread_id,
                MemberChange::Request,
                actions,
            );
        }
    }

    // ─── 回查队列 ────────────────────────────────────────────────────

    /// 血缘链上第一个未知的线程需要回查（thread/read，不取轮次）。
    fn lineage_gap(&self, thread: &str) -> Option<String> {
        let mut current = thread.to_owned();
        for _ in 0..=MAX_DEPTH {
            if self.relations.aux.lineage_roots.contains(&current) {
                return None;
            }
            match self.thread_relation(&current) {
                Some(edge) => {
                    if self.claims.contains_key(&edge.parent_thread_id) {
                        return None;
                    }
                    current = edge.parent_thread_id;
                }
                None if current != thread && self.claims.contains_key(&current) => return None,
                None => return Some(current),
            }
        }
        None
    }

    pub(crate) fn lookup_ensure(&mut self, thread: &str, actions: &mut Vec<Action>) {
        let Some(target) = self.lineage_gap(thread) else {
            return;
        };
        if self.relations.aux.lookups.contains_key(&target) {
            return;
        }
        let op_id = self.relations.aux.next();
        self.relations.aux.lookups.insert(
            target.clone(),
            Lookup {
                op_id,
                generation: 0,
                attempt: 0,
                in_flight: false,
                retry_at_ms: None,
                exhausted: false,
            },
        );
        self.relations.aux.lookup_queue.push_back(target);
        self.lookup_pump(actions);
    }

    /// 同一线程只有一个在途回查，全局最多 8 个。
    fn lookup_pump(&mut self, actions: &mut Vec<Action>) {
        while self
            .relations
            .aux
            .lookups
            .values()
            .filter(|l| l.in_flight)
            .count()
            < LOOKUP_CONCURRENCY
        {
            let Some(target) = self.relations.aux.lookup_queue.pop_front() else {
                break;
            };
            let generation = self.relations.aux.next();
            let Some(lookup) = self.relations.aux.lookups.get_mut(&target) else {
                continue;
            };
            if lookup.in_flight || lookup.exhausted {
                continue;
            }
            lookup.in_flight = true;
            lookup.generation = generation;
            lookup.retry_at_ms = None;
            let op_id = lookup.op_id;
            actions.push(Action::Request(RequestContext {
                resume_retried: false,
                stream_id: None,
                review_id: None,
                client: None,
                token: None,
                operation: Operation::RelationLookup {
                    op_id,
                    generation,
                    thread: target.clone(),
                },
                method: "thread/read".into(),
                params: Some(json!({"threadId": target, "includeTurns": false})),
            }));
        }
    }

    /// 回查回包：核对编号与代号，迟到或重复回包丢弃；成功登记血缘，失败退避重试最多 3 次。
    pub(super) fn lookup_reply(
        &mut self,
        op_id: u64,
        generation: u64,
        thread: &str,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        let current = self
            .relations
            .aux
            .lookups
            .get(thread)
            .is_some_and(|l| l.op_id == op_id && l.generation == generation && l.in_flight);
        if !current {
            actions.push(Action::Debug(format!("丢弃线程 {thread} 迟到的回查回包")));
            return;
        }
        match result {
            Ok(value) => {
                self.relations.aux.lookups.remove(thread);
                let info = value.get("thread").cloned().unwrap_or(Value::Null);
                match edge_from_thread_checked(&info, thread) {
                    Some(edge) => {
                        self.record_edge(edge, actions);
                    }
                    None => {
                        let roots = &mut self.relations.aux.lineage_roots;
                        if roots.len() >= MAX_OBSERVED_THREADS {
                            if let Some(first) = roots.iter().next().cloned() {
                                roots.remove(&first);
                            }
                        }
                        roots.insert(thread.into());
                    }
                }
                self.routing_reconsider(thread, actions);
            }
            Err(error) => {
                let now = self.clock.mono_ms;
                let lookup = self
                    .relations
                    .aux
                    .lookups
                    .get_mut(thread)
                    .expect("回查存在");
                lookup.in_flight = false;
                lookup.attempt += 1;
                if lookup.attempt >= LOOKUP_RETRIES {
                    lookup.exhausted = true;
                    actions.push(Action::Warn(format!(
                        "线程 {thread} 回查失败 {} 次，等待证据或期限：{}",
                        LOOKUP_RETRIES,
                        error.get("message").and_then(Value::as_str).unwrap_or("")
                    )));
                } else {
                    lookup.retry_at_ms =
                        Some(now.saturating_add(LOOKUP_BACKOFF_MS << (lookup.attempt - 1)));
                }
            }
        }
        self.lookup_pump(actions);
    }

    /// 回查重试已并入整体扫描；单独的重试计时不使用，迟到的旧计时器直接忽略。
    pub(super) fn lookup_retry_timer(
        &mut self,
        _op_id: u64,
        _generation: u64,
        _actions: &mut Vec<Action>,
    ) {
    }

    /// 扫描：到点的重试重新排队；超过 30 秒仍在回查的请求按未知不答（记诊断）。
    pub(super) fn lookup_expire(&mut self, actions: &mut Vec<Action>) {
        let now = self.clock.mono_ms;
        let due: Vec<String> = self
            .relations
            .aux
            .lookups
            .iter()
            .filter(|(_, l)| l.retry_at_ms.is_some_and(|at| at <= now))
            .map(|(t, _)| t.clone())
            .collect();
        for thread in due {
            if let Some(l) = self.relations.aux.lookups.get_mut(&thread) {
                l.retry_at_ms = None;
            }
            self.relations.aux.lookup_queue.push_back(thread);
        }
        let expired: Vec<(u64, String)> = self
            .relations
            .requests
            .iter()
            .filter(|(k, r)| {
                r.stage == ChildRequestStage::Lookup
                    && self
                        .relations
                        .aux
                        .request_meta
                        .get(*k)
                        .is_some_and(|m| m.lookup_deadline_ms <= now)
            })
            .map(|(k, _)| k.clone())
            .collect();
        for key in expired {
            if let Some(record) = self.forget_request(&key, actions) {
                actions.push(Action::Warn(format!(
                    "子线程 {} 的请求 {} 回查 {} 秒仍未证明归属，不答",
                    record.source_thread_id,
                    key.1,
                    LOOKUP_DEADLINE_MS / 1000
                )));
            }
        }
        // 没有请求或通知再等的回查登记清掉（在途的等回包后自然结束）。
        let waiting: BTreeSet<String> = self
            .relations
            .requests
            .values()
            .filter(|r| r.stage == ChildRequestStage::Lookup)
            .map(|r| r.source_thread_id.clone())
            .chain(
                self.relations
                    .aux
                    .guardians
                    .iter()
                    .map(|g| g.thread.clone()),
            )
            .collect();
        let gaps: BTreeSet<String> = waiting.iter().filter_map(|t| self.lineage_gap(t)).collect();
        self.relations
            .aux
            .lookups
            .retain(|t, l| l.in_flight || gaps.contains(t));
        let lookups = &self.relations.aux.lookups;
        self.relations
            .aux
            .lookup_queue
            .retain(|t| lookups.contains_key(t));
        self.lookup_pump(actions);
    }

    // ─── 关闭 ────────────────────────────────────────────────────────

    pub(super) fn routing_shutdown(&mut self, actions: &mut Vec<Action>) {
        let keys: Vec<(u64, String)> = self.relations.requests.keys().cloned().collect();
        for key in keys {
            let record = self.relations.requests[&key].clone();
            match record.stage {
                // 已证明自有、暂存待定案的请求按关闭规则回错误。
                ChildRequestStage::Held => {
                    self.forget_request(&key, actions);
                    actions.push(Action::ServerError {
                        id: record.upstream_request_id,
                        error: crate::requests::handler_error(SHUTDOWN),
                    });
                }
                ChildRequestStage::Lookup => {
                    self.forget_request(&key, actions);
                }
                // 转发与收件箱阶段由 shutdown_forwards、inbox_shutdown 按现有关闭规则结算。
                _ => {}
            }
        }
    }
}

/// 回查回包里的线程必须就是被查的线程。
fn edge_from_thread_checked(info: &Value, thread: &str) -> Option<RelationEdge> {
    if info.get("id").and_then(Value::as_str) != Some(thread) {
        return None;
    }
    facts::edge_from_thread(info, DiscoveredBy::Lookup)
}
