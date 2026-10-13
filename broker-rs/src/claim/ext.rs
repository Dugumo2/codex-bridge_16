//! 扩展方法、托管交接与通知出口（任务说明第 6 节、第 8.4 节）。
use super::{
    Action, Claim, ClaimToken, ClientRequest, ConnId, HeldNotification, HeldRequest, Machine,
    Owner, PendingDetach, State, StopReason, TimerKey, TimerKind,
};
use crate::{
    inbox::InboxOrigin,
    policy::{self, Policy},
    proto,
    runs::{RunLookup, RunMeta, RunNotice, SummaryOwner, SummaryState, ThreadSummary},
};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};

pub const MAX_WATCHERS: usize = 64;
pub const MAX_WATCHERS_PER_CONN: usize = 256;
pub const MAX_WATCHERS_TOTAL: usize = 4096;
pub const MAX_META_BYTES: usize = 16 * 1024;
/// 批量一次性查询的输入预算，与既有扩展方法一样超限返回参数错误。
pub const MAX_MODEL_SUMMARY_THREADS: usize = 4096;
pub const MAX_PENDING_META: usize = 1024;
pub const DETACH_WAIT_MS: u64 = 30_000;
pub const RECLAIM_MS: u64 = 86_400_000;

/// 辅助登记随认领、待消费元数据清理，不另持有运行或请求副本。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Extensions {
    meta_order: VecDeque<(String, ConnId)>,
    reclaim: HashMap<String, u64>,
    detach: HashMap<String, u64>,
    generation: u64,
}

impl Extensions {
    fn next(&mut self) -> u64 {
        self.generation = self.generation.checked_add(1).expect("扩展计时代号已耗尽");
        self.generation
    }
}
fn ok(request: &ClientRequest, result: Value, actions: &mut Vec<Action>) {
    actions.push(Action::Send {
        conn: request.conn,
        message: proto::response_ok(request.id.clone(), result),
    });
}
fn fail(request: &ClientRequest, reason: &str, actions: &mut Vec<Action>) {
    actions.push(Action::Send {
        conn: request.conn,
        message: proto::response_err(
            request.id.clone(),
            proto::error_object(
                -32000,
                format!("扩展请求未完成：{reason}"),
                Some(json!({"reason":reason})),
            ),
        ),
    });
}
fn invalid(request: &ClientRequest, actions: &mut Vec<Action>) {
    actions.push(Action::Send {
        conn: request.conn,
        message: proto::response_err(
            request.id.clone(),
            proto::error_object(-32602, "扩展请求参数不合法或超出大小上限", None),
        ),
    });
}
fn owner_mut(claim: &mut Claim) -> Option<&mut Owner> {
    match &mut claim.state {
        State::Starting { owner, .. }
        | State::Running { owner, .. }
        | State::Converging { owner, .. }
        | State::FinishingTree { owner, .. }
        | State::AwaitingContinuation { owner } => Some(owner),
        State::Idle => None,
    }
}
impl Machine {
    /// 仅处理上游严格配置校验的准确错误，不能把任意无效请求当成开关不支持。
    pub(crate) fn retry_resume_unknown_key(
        &mut self,
        context: &super::RequestContext,
        result: &Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) -> bool {
        if context.resume_retried || self.closing || self.quiescing {
            return false;
        }
        let valid = match &context.operation {
            super::Operation::ResumeBarrier => context.token.as_ref().is_some_and(|token| {
                self.current(token)
                    && !self.stop_busy(&token.thread_id)
                    && self
                        .claims
                        .get(&token.thread_id)
                        .is_some_and(|claim| !claim.owner_gone && claim.pending_start.is_some())
            }),
            super::Operation::ContinueResume => self.continue_resume_current(context),
            super::Operation::Inbox(rpc) => self.guardian_resume_current(rpc),
            _ => false,
        };
        if !valid || context.method != "thread/resume" {
            return false;
        }
        let Err(error) = result else {
            return false;
        };
        if error.get("code").and_then(Value::as_i64) != Some(-32600) {
            return false;
        }
        let Some(key) = error
            .get("message")
            .and_then(Value::as_str)
            .and_then(|message| {
                message.strip_prefix("failed to load configuration: unknown configuration field `")
            })
            .and_then(|message| message.strip_suffix("` in -c/--config override"))
            .filter(|key| !key.is_empty() && !key.contains('`'))
        else {
            return false;
        };
        let Some(thread) = context
            .params
            .as_ref()
            .and_then(|p| p.get("threadId"))
            .and_then(Value::as_str)
        else {
            return false;
        };
        let Some(sent) = context
            .params
            .as_ref()
            .and_then(|p| p.get("config"))
            .and_then(|p| p.get(key))
        else {
            return false;
        };
        let Some(entry) = self
            .runs
            .index
            .threads
            .values_mut()
            .find(|e| e.thread_id == thread)
        else {
            return false;
        };
        let Some(config) = entry
            .resume_overrides
            .as_mut()
            .and_then(|o| o.config.as_mut())
        else {
            return false;
        };
        // 登记已经被另一个请求更新时，不删掉新值，也不扩大这次旧请求的降级范围。
        if config.get(key) != Some(sent) {
            return false;
        }
        config.remove(key);
        let mut retry = context.clone();
        retry.resume_retried = true;
        retry
            .params
            .as_mut()
            .and_then(|p| p.get_mut("config"))
            .and_then(Value::as_object_mut)
            .expect("内部恢复配置是对象")
            .remove(key);
        self.runs.put_threads(actions);
        actions.push(Action::Warn(
            "恢复配置有一个未知键，已移除该键并重试一次".into(),
        ));
        actions.push(Action::Request(retry));
        true
    }

    /// 仅三处内部恢复使用，调用方只传自建对象，不接收客户端参数。
    /// 已有顶层参数优先，唯独 excludeTurns 强制覆盖为 true；不深合并调用点的 config。
    pub(crate) fn resume_params(&self, thread: &str, mut params: Value) -> Value {
        params["excludeTurns"] = json!(true);
        if let Some(overrides) = self
            .runs
            .index
            .threads
            .values()
            .find(|entry| entry.thread_id == thread)
            .and_then(|entry| entry.resume_overrides.as_ref())
        {
            let object = params.as_object_mut().expect("内部恢复参数是对象");
            if let Some(config) = &overrides.config {
                object.entry("config").or_insert_with(|| json!(config));
            }
            if let Some(instructions) = &overrides.developer_instructions {
                object
                    .entry("developerInstructions")
                    .or_insert_with(|| json!(instructions));
            }
        }
        params
    }

    pub(crate) fn ext_request(&mut self, request: ClientRequest, actions: &mut Vec<Action>) {
        if self.closing
            && matches!(
                request.method.as_str(),
                "broker/threads/annotate"
                    | "broker/threads/detach"
                    | "broker/threads/resumeOverrides"
                    | "broker/threads/purge"
            )
        {
            fail(&request, "shuttingDown", actions);
            return;
        }
        let params = request
            .params
            .as_ref()
            .cloned()
            .unwrap_or_else(|| json!({}));
        if !params.is_object() {
            invalid(&request, actions);
            return;
        }
        if request.method == "broker/runs/modelSummaries" {
            let Some(ids) = params
                .get("threadIds")
                .and_then(Value::as_array)
                .filter(|ids| ids.len() <= MAX_MODEL_SUMMARY_THREADS)
            else {
                invalid(&request, actions);
                return;
            };
            let Some(ids) = ids
                .iter()
                .map(|id| id.as_str().filter(|id| !id.is_empty()))
                .collect::<Option<HashSet<_>>>()
            else {
                invalid(&request, actions);
                return;
            };
            let runs: Vec<_> = self
                .runs
                .runs
                .values()
                .filter(|run| {
                    ids.contains(run.thread_id.as_str())
                        || run
                            .review_thread_id
                            .as_deref()
                            .is_some_and(|thread| ids.contains(thread))
                })
                .map(|run| run.model_summary())
                .collect();
            ok(&request, json!({"runs":runs}), actions);
            return;
        }
        if request.method == "broker/threads/list" {
            let limit = match params.get("limit") {
                None => usize::MAX,
                Some(v) => match v.as_u64().and_then(|n| usize::try_from(n).ok()) {
                    Some(n) => n,
                    None => {
                        invalid(&request, actions);
                        return;
                    }
                },
            };
            let ids: HashSet<_> = self
                .runs
                .index
                .threads
                .values()
                .map(|e| e.thread_id.as_str())
                .chain(self.claims.keys().map(String::as_str))
                .chain(self.lifecycle_threads().map(String::as_str))
                .collect();
            let mut threads: Vec<_> = ids
                .into_iter()
                .filter_map(|id| self.thread_summary(id))
                .collect();
            threads.sort_by(|a, b| {
                b.updated_at
                    .cmp(&a.updated_at)
                    .then(a.thread_id.cmp(&b.thread_id))
            });
            threads.truncate(limit);
            ok(&request, json!({"threads":threads}), actions);
            return;
        }
        let Some(thread) = params
            .get("threadId")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        else {
            invalid(&request, actions);
            return;
        };
        if request.method == "broker/threads/detach" {
            self.detach_request(&request, thread, &params, actions);
            return;
        }
        if request.method == "broker/threads/purge" {
            self.run_purge_request(request.clone(), thread, actions);
            return;
        }
        if self.thread_summary(thread).is_none()
            || (request.method == "broker/threads/resumeOverrides"
                && !self.claims.contains_key(thread)
                && !self
                    .runs
                    .index
                    .threads
                    .values()
                    .any(|entry| entry.thread_id == thread))
        {
            fail(&request, "unknownThread", actions);
            return;
        }
        match request.method.as_str() {
            "broker/threads/resumeOverrides" => {
                let overrides = match params.get("overrides") {
                    Some(Value::Null) => None,
                    Some(value) if value.is_object() => {
                        match serde_json::from_value::<crate::runs::ResumeOverrides>(value.clone())
                        {
                            Ok(value) => Some(value),
                            Err(_) => {
                                invalid(&request, actions);
                                return;
                            }
                        }
                    }
                    _ => {
                        invalid(&request, actions);
                        return;
                    }
                };
                let short = self
                    .runs
                    .ensure_thread(thread, self.clock.now_ms, self.clock.entropy);
                self.runs
                    .index
                    .threads
                    .get_mut(&short)
                    .expect("线程索引存在")
                    .resume_overrides = overrides;
                self.runs.put_threads(actions);
                ok(&request, json!({"threadId":thread}), actions);
            }
            "broker/threads/annotate" => {
                let Some(meta) = params.get("meta") else {
                    invalid(&request, actions);
                    return;
                };
                if serde_json::from_value::<RunMeta>(meta.clone()).is_err()
                    || serde_json::to_vec(meta).map_or(true, |v| v.len() > MAX_META_BYTES)
                    || (meta.get("source").and_then(Value::as_str) == Some("review")
                        && (meta.get("model").is_some() || meta.get("effort").is_some()))
                {
                    invalid(&request, actions);
                    return;
                }
                self.prune_ext_meta();
                self.extensions
                    .meta_order
                    .retain(|(t, c)| t != thread || *c != request.conn);
                if self.extensions.meta_order.len() >= MAX_PENDING_META {
                    if let Some((old, conn)) = self.extensions.meta_order.pop_front() {
                        if let Some(record) = self.records.get_mut(&old) {
                            record.pending_meta.remove(&conn);
                        }
                        actions.push(Action::Warn("待认领元数据达到上限，已淘汰最旧一份".into()));
                    }
                }
                self.record(thread)
                    .pending_meta
                    .insert(request.conn, meta.clone());
                self.extensions
                    .meta_order
                    .push_back((thread.into(), request.conn));
                let short = self
                    .runs
                    .ensure_thread(thread, self.clock.now_ms, self.clock.entropy);
                self.runs.put_threads(actions);
                ok(
                    &request,
                    json!({"threadId":thread,"shortId":short}),
                    actions,
                );
            }
            "broker/threads/watch" => {
                // runId 可选：指定时快照给这次运行（须属于该线程或其 review 子线程），否则给最近一次运行。
                let run_id = match params.get("runId") {
                    None => None,
                    Some(Value::String(id)) if !id.is_empty() => Some(id.clone()),
                    Some(_) => {
                        invalid(&request, actions);
                        return;
                    }
                };
                let duplicate = self
                    .records
                    .get(thread)
                    .is_some_and(|r| r.watchers.contains(&request.conn));
                if !duplicate {
                    let total: usize = self.records.values().map(|r| r.watchers.len()).sum();
                    let per_conn = self
                        .records
                        .values()
                        .filter(|r| r.watchers.contains(&request.conn))
                        .count();
                    if total >= MAX_WATCHERS_TOTAL
                        || per_conn >= MAX_WATCHERS_PER_CONN
                        || self
                            .records
                            .get(thread)
                            .is_some_and(|r| r.watchers.len() >= MAX_WATCHERS)
                    {
                        fail(&request, "tooManyWatchers", actions);
                        return;
                    }
                    self.record(thread).watchers.push(request.conn);
                }
                self.watcher_activity(thread, actions);
                ok(
                    &request,
                    self.child_watch_snapshot(thread, run_id.as_deref(), json!({"thread":self.thread_summary(thread),
                    "run":if self.lifecycle_only(thread) {None} else {match &run_id {
                        Some(id) => self.thread_run(thread, id),
                        None => self.latest_run(thread),
                    }},
                    "pending":self.inbox_pending(thread),"lifecycle":self.lifecycle_projection(thread)})),
                    actions,
                );
            }
            _ => invalid(&request, actions),
        }
    }

    fn detach_request(
        &mut self,
        request: &ClientRequest,
        thread: &str,
        params: &Value,
        actions: &mut Vec<Action>,
    ) {
        let Some(run) = params
            .get("runId")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        else {
            invalid(request, actions);
            return;
        };
        let Some(policy) = params
            .get("policy")
            .and_then(Value::as_str)
            .and_then(Policy::parse)
        else {
            invalid(request, actions);
            return;
        };
        let reviewer = match params.get("approvalsReviewer") {
            None => None,
            Some(Value::String(s)) => Some(s.clone()),
            _ => {
                invalid(request, actions);
                return;
            }
        };
        match self.run_lookup(run) {
            RunLookup::Unknown => {
                fail(request, "unknownRun", actions);
                return;
            }
            RunLookup::Retired { thread_id } => {
                if thread_id != thread {
                    fail(request, "runMismatch", actions);
                } else {
                    self.detach_ended(request, thread, run, actions);
                }
                return;
            }
            RunLookup::Live => {
                let record = &self.runs.runs[run];
                if record.thread_id != thread && record.review_thread_id.as_deref() != Some(thread)
                {
                    fail(request, "runMismatch", actions);
                    return;
                }
            }
        }
        // review 的父线程标识仍属于该运行，但实际认领可能已经迁到子线程。
        // 只按运行身份查找，不能把父认领已移走或已换代当成运行结束。
        let Some((execution_thread, claim)) = self
            .claims
            .iter()
            .find(|(_, claim)| claim.run_id.as_deref() == Some(run))
        else {
            self.detach_ended(request, thread, run, actions);
            return;
        };
        let execution_thread = execution_thread.clone();
        let thread = execution_thread.as_str();
        if claim.joined() {
            fail(request, "joined", actions);
            return;
        }
        if matches!(claim.owner(), Some(Owner::Broker { .. })) {
            ok(
                request,
                json!({"outcome":"alreadyDetached","turnId":claim.turn_id(),"pending":self.inbox_pending_run_items(run)}),
                actions,
            );
            return;
        }
        if claim.owner() != Some(&Owner::Client(request.conn)) || claim.owner_gone {
            fail(request, "notOwner", actions);
            return;
        }
        if claim.pending_detach.is_some() {
            fail(request, "detachPending", actions);
            return;
        }
        let pending = PendingDetach {
            request: request.clone(),
            policy,
            approvals_reviewer: reviewer,
        };
        if claim.settling() {
            let token = self.token(thread).expect("交接认领存在");
            let generation = self.extensions.next();
            self.extensions.detach.insert(thread.into(), generation);
            self.claims
                .get_mut(thread)
                .expect("交接认领存在")
                .pending_detach = Some(pending);
            actions.push(Action::Schedule {
                token,
                kind: TimerKind::DetachDeadline { generation },
                delay_ms: DETACH_WAIT_MS,
            });
        } else {
            self.detach_apply(thread, pending, actions);
        }
    }
    fn detach_ended(
        &self,
        request: &ClientRequest,
        thread: &str,
        run: &str,
        actions: &mut Vec<Action>,
    ) {
        let snapshot = self.run_snapshot(run);
        let turn = self
            .runs
            .runs
            .get(run)
            .and_then(|r| r.turns.last())
            .map(|t| t.turn_id.as_str());
        let mut result = json!({"outcome":"runEnded","turnId":turn,"pending":self.inbox_pending_aggregate(thread)});
        if let Some(snapshot) = snapshot {
            result["result"] = snapshot;
        }
        ok(request, result, actions);
    }
    fn detach_apply(&mut self, thread: &str, pending: PendingDetach, actions: &mut Vec<Action>) {
        let claim = self.claims.get_mut(thread).expect("交接认领存在");
        let origin = InboxOrigin {
            thread_id: thread.into(),
            claim_id: claim.claim_id,
            run_id: claim.run_id.clone(),
            turn_id: claim.turn_id().map(str::to_owned),
        };
        *owner_mut(claim).expect("交接 owner 存在") = Owner::Broker {
            policy: pending.policy,
            approvals_reviewer: pending.approvals_reviewer,
            owned_at: self.clock.mono_ms,
            last_watcher_at: self.clock.mono_ms,
        };
        if let Some(run) = &origin.run_id {
            self.on_handoff(run, crate::relations::HandoffKind::Detach, actions);
        }
        self.convergence_event(thread, super::converge::ConvergeEvent::Detach, actions);
        let ids: Vec<_> = self
            .requests
            .pending
            .iter()
            .filter(|(_, p)| {
                p.thread_id == thread
                    && p.claim_id == origin.claim_id
                    && p.target == pending.request.conn
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            let forward = self.take_forward(&id, actions).expect("交接转发存在");
            let mut origin = origin.clone();
            origin.turn_id = forward
                .params
                .as_ref()
                .and_then(|p| p.get("turnId"))
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or(origin.turn_id);
            self.inbox_register_request(
                origin,
                HeldRequest {
                    id: forward.request_id,
                    forward_id: id,
                    method: forward.method,
                    params: forward.params,
                },
                Some(forward.deadline_ms),
                actions,
            );
        }
        // 回包的 pending 按运行汇总：父线程事项与子线程事项（SA-a 第 6 节）。
        let items = match &origin.run_id {
            Some(run) => self.inbox_pending_run_items(run),
            None => self.inbox_pending(thread),
        };
        ok(
            &pending.request,
            json!({"outcome":"detached","turnId":origin.turn_id,"pending":items}),
            actions,
        );
        self.reclaim_check(thread, actions);
    }
    pub(crate) fn hosted_server_request(
        &mut self,
        request: HeldRequest,
        actions: &mut Vec<Action>,
    ) {
        let params = request.params.clone().unwrap_or_else(|| json!({}));
        let Some(thread) = params.get("threadId").and_then(Value::as_str) else {
            return;
        };
        let Some(claim) = self.claims.get(thread) else {
            return;
        };
        let Some(Owner::Broker { policy, .. }) = claim.owner() else {
            return;
        };
        match policy::decide(*policy, &request.method, &params) {
            policy::Decision::Answer(result) => actions.push(Action::ServerResult {
                id: request.id,
                result,
            }),
            policy::Decision::Inbox => {
                let origin = InboxOrigin {
                    thread_id: thread.into(),
                    claim_id: claim.claim_id,
                    run_id: claim.run_id.clone(),
                    turn_id: params
                        .get("turnId")
                        .and_then(Value::as_str)
                        .or(claim.turn_id())
                        .map(str::to_owned),
                };
                self.inbox_register_request(origin, request, None, actions);
            }
            policy::Decision::MethodNotFound => crate::requests::method_not_found(request, actions),
        }
    }
    pub(crate) fn flush_run_notices(&mut self, actions: &mut Vec<Action>) {
        let notices = self.runs.take_notices();
        if notices.is_empty() {
            return;
        }
        for notice in notices {
            let (thread, run) = match &notice {
                RunNotice::TurnResult {
                    thread_id, run_id, ..
                }
                | RunNotice::RunEnded {
                    thread_id, run_id, ..
                } => (thread_id, run_id),
            };
            let child = self
                .runs
                .runs
                .get(run)
                .and_then(|r| r.review_thread_id.clone());
            let threads: HashSet<_> = std::iter::once(thread.clone()).chain(child).collect();
            let mut recipients = HashSet::new();
            for id in &threads {
                if let Some(record) = self.records.get(id) {
                    recipients.extend(record.watchers.iter().copied());
                }
            }
            let primary = thread.clone();
            let owner = match &notice {
                RunNotice::RunEnded { owner, .. } => *owner,
                _ => None,
            };
            let single_thread = threads.len() == 1;
            let (method, params) = match notice {
                RunNotice::TurnResult {
                    thread_id,
                    run_id,
                    turn_index,
                    result,
                } => (
                    "broker/thread/result",
                    json!({"threadId":thread_id,"runId":run_id,"turn":turn_index,"result":result}),
                ),
                RunNotice::RunEnded {
                    thread_id,
                    run_id,
                    owner,
                } => {
                    recipients.extend(owner);
                    let Some(run) = self.run_snapshot(&run_id) else {
                        continue;
                    };
                    for id in threads {
                        self.inbox_run_ended(&id, run.clone(), actions);
                    }
                    ("broker/run/ended", json!({"threadId":thread_id,"run":run}))
                }
            };
            if single_thread {
                self.notify_thread_ext(&primary, owner, method, params, actions);
                continue;
            }
            let message = proto::notification(method, Some(params));
            for conn in recipients {
                self.send_ext(conn, message.clone(), actions);
            }
        }
        // 通知已消费，解除其运行保护；没有通知的增量事件无需扫描账本和索引。
        self.runs.evict();
    }
    pub(crate) fn reclaim_timer(
        &mut self,
        token: ClaimToken,
        generation: u64,
        actions: &mut Vec<Action>,
    ) {
        if !self.current(&token)
            || self.extensions.reclaim.get(&token.thread_id) != Some(&generation)
        {
            return;
        }
        self.extensions.reclaim.remove(&token.thread_id);
        self.reclaim_check(&token.thread_id, actions);
    }
    pub(crate) fn reclaim_check(&mut self, thread: &str, actions: &mut Vec<Action>) {
        self.cancel_reclaim(thread, actions);
        if self.closing || self.stop_busy(thread) {
            return;
        }
        let Some(claim) = self.claims.get(thread) else {
            return;
        };
        let Some(Owner::Broker {
            owned_at,
            last_watcher_at,
            ..
        }) = claim.owner()
        else {
            return;
        };
        let deadline = (*owned_at).max(*last_watcher_at).saturating_add(RECLAIM_MS);
        if self.clock.mono_ms >= deadline {
            if self
                .records
                .get(thread)
                .is_none_or(|r| r.watchers.is_empty())
                && self.inbox_pending_count(thread) == 0
            {
                self.begin_stop(thread, StopReason::Stop, false, actions);
            }
            return;
        }
        let generation = self.extensions.next();
        self.extensions.reclaim.insert(thread.into(), generation);
        actions.push(Action::Schedule {
            token: self.token(thread).expect("托管认领存在"),
            kind: TimerKind::Reclaim { generation },
            delay_ms: deadline - self.clock.mono_ms,
        });
    }
    pub(crate) fn cancel_reclaim(&mut self, thread: &str, actions: &mut Vec<Action>) {
        if self.extensions.reclaim.remove(thread).is_some() {
            actions.push(Action::Unschedule {
                key: TimerKey::Reclaim(thread.into()),
            });
        }
    }
    pub(crate) fn watcher_activity(&mut self, thread: &str, actions: &mut Vec<Action>) {
        if let Some(Owner::Broker {
            last_watcher_at, ..
        }) = self.claims.get_mut(thread).and_then(owner_mut)
        {
            *last_watcher_at = self.clock.mono_ms;
            self.reclaim_check(thread, actions);
        }
    }
    fn take_detach(&mut self, thread: &str, actions: &mut Vec<Action>) -> Option<PendingDetach> {
        let pending = self.claims.get_mut(thread)?.pending_detach.take()?;
        self.cancel_detach_timer(thread, actions);
        Some(pending)
    }
    fn cancel_detach_timer(&mut self, thread: &str, actions: &mut Vec<Action>) {
        self.extensions.detach.remove(thread);
        actions.push(Action::Unschedule {
            key: TimerKey::DetachDeadline(thread.into()),
        });
    }
    pub(crate) fn detach_settled(
        &mut self,
        thread: &str,
        claim_id: u64,
        joined: bool,
        actions: &mut Vec<Action>,
    ) {
        if !self
            .claims
            .get(thread)
            .is_some_and(|c| c.claim_id == claim_id)
        {
            return;
        }
        let Some(pending) = self.take_detach(thread, actions) else {
            return;
        };
        if self.closing {
            fail(&pending.request, "shuttingDown", actions);
        } else if joined {
            fail(&pending.request, "joined", actions);
        } else {
            self.detach_apply(thread, pending, actions);
        }
    }
    pub(crate) fn detach_released(
        &mut self,
        thread: &str,
        claim: &mut Claim,
        actions: &mut Vec<Action>,
    ) {
        let Some(pending) = claim.pending_detach.take() else {
            return;
        };
        self.cancel_detach_timer(thread, actions);
        if self.closing {
            fail(&pending.request, "shuttingDown", actions);
        } else {
            self.detach_ended(
                &pending.request,
                thread,
                claim.run_id.as_deref().unwrap_or(""),
                actions,
            );
        }
    }
    pub(crate) fn detach_disconnected(
        &mut self,
        thread: &str,
        conn: ConnId,
        actions: &mut Vec<Action>,
    ) {
        if !self
            .claims
            .get(thread)
            .and_then(|c| c.pending_detach.as_ref())
            .is_some_and(|p| p.request.conn == conn)
        {
            return;
        }
        if self.take_detach(thread, actions).is_some() {
            if let Some(run) = self.claims.get(thread).and_then(|c| c.run_id.as_ref()) {
                self.runs.log(
                    run,
                    self.clock.now_ms,
                    "交接撤销",
                    "发起连接已断开",
                    actions,
                );
            }
        }
    }
    pub(crate) fn detach_deadline(
        &mut self,
        token: ClaimToken,
        generation: u64,
        actions: &mut Vec<Action>,
    ) {
        if !self.current(&token)
            || self.extensions.detach.get(&token.thread_id) != Some(&generation)
        {
            return;
        }
        if let Some(pending) = self.take_detach(&token.thread_id, actions) {
            fail(&pending.request, "settlingTimeout", actions);
        }
    }
    pub(crate) fn run_lookup(&self, run: &str) -> RunLookup {
        self.runs.lookup(run)
    }
    pub(crate) fn watched_results_pending(&self) -> bool {
        self.records
            .iter()
            .any(|(id, r)| !r.watchers.is_empty() && self.runs.thread_reads_pending(id))
    }
    pub(crate) fn prune_ext_meta(&mut self) {
        self.extensions.meta_order.retain(|(t, c)| {
            self.records
                .get(t)
                .is_some_and(|r| r.pending_meta.contains_key(c))
        });
    }
    pub(crate) fn prune_reclaims(&mut self, actions: &mut Vec<Action>) {
        let stopped: Vec<_> = self
            .extensions
            .reclaim
            .keys()
            .filter(|t| self.stop_busy(t) || !self.claims.contains_key(*t))
            .cloned()
            .collect();
        for thread in stopped {
            self.cancel_reclaim(&thread, actions);
        }
    }
    pub(crate) fn thread_summary(&self, thread: &str) -> Option<ThreadSummary> {
        let index = self
            .runs
            .index
            .threads
            .values()
            .find(|e| e.thread_id == thread);
        let claim = self.claims.get(thread);
        let record = self.records.get(thread);
        let lifecycle = self.lifecycle_record(thread);
        if index.is_none() {
            if let Some(lifecycle) = lifecycle {
                return Some(ThreadSummary {
                    thread_id: thread.into(),
                    short_id: lifecycle.short_id.clone(),
                    state: SummaryState::Idle,
                    owner: None,
                    turn_id: None,
                    goal_active: false,
                    pending_count: 0,
                    last_run_id: None,
                    updated_at: lifecycle.updated_at,
                    model: None,
                    preview: lifecycle.preview.clone(),
                    cwd: None,
                    meta: None,
                    lifecycle: self.lifecycle_projection(thread),
                });
            }
        }
        if index.is_none() && claim.is_none() && record.is_none() && lifecycle.is_none() {
            return None;
        }
        let state = if self.stop_busy(thread) {
            SummaryState::Stopping
        } else {
            match claim.map(|c| &c.state) {
                Some(State::Starting { .. }) => SummaryState::Starting,
                Some(State::Running { .. }) => SummaryState::Running,
                Some(State::Converging { .. }) => SummaryState::Running,
                Some(State::FinishingTree { .. }) => SummaryState::Stopping,
                Some(State::AwaitingContinuation { .. }) => SummaryState::Awaiting,
                _ => SummaryState::Idle,
            }
        };
        let owner = if state == SummaryState::Idle {
            None
        } else {
            match claim.and_then(Claim::owner) {
                Some(Owner::Client(conn))
                    if self.live_conns.contains(conn) && claim.is_some_and(|c| !c.owner_gone) =>
                {
                    Some(SummaryOwner::Client)
                }
                Some(Owner::Broker { .. }) => Some(SummaryOwner::Broker),
                _ => None,
            }
        };
        Some(ThreadSummary {
            lifecycle: self.lifecycle_projection(thread),
            thread_id: thread.into(),
            short_id: index
                .map(|i| i.short_id.clone())
                .or_else(|| lifecycle.map(|r| r.short_id.clone()))
                .unwrap_or_default(),
            state,
            owner,
            turn_id: claim.and_then(Claim::turn_id).map(str::to_owned),
            goal_active: record.is_some_and(|r| r.goal_active),
            pending_count: self.inbox_pending_count(thread),
            last_run_id: index.and_then(|i| i.last_run_id.clone()),
            updated_at: index
                .map(|i| i.updated_at)
                .or_else(|| lifecycle.map(|r| r.updated_at))
                .unwrap_or(0),
            model: index.and_then(|i| i.model.clone()),
            preview: index
                .and_then(|i| i.preview.clone())
                .or_else(|| lifecycle.and_then(|r| r.preview.clone())),
            cwd: index.and_then(|i| i.cwd.clone()),
            meta: index.and_then(|i| i.meta.clone()),
        })
    }
    // ─── 发送助手（骨架已实现，三路共用；改动须在交付说明里列出） ───────────────────────

    /// 只发给声明了 capabilities.cxb 的连接（扩展字段与 `broker/*` 推送的门控，3.6 节）。
    pub(crate) fn send_ext(&self, conn: ConnId, message: Value, actions: &mut Vec<Action>) {
        if self.cxb_conns.contains(&conn) {
            actions.push(Action::Send { conn, message });
        }
    }

    /// 在认领删除或迁移之前取收件人，诊断仅通过扩展连接发送。
    pub(crate) fn run_update_recipients(&self, run: &str) -> Vec<ConnId> {
        let Some(record) = self.runs.runs.get(run) else {
            return Vec::new();
        };
        let mut recipients = self.run_recipients(run);
        for thread in
            std::iter::once(record.thread_id.as_str()).chain(record.review_thread_id.as_deref())
        {
            let owner = self
                .claims
                .get(thread)
                .filter(|claim| claim.run_id.as_deref() == Some(run))
                .and_then(|_| self.owner_conn(thread));
            for conn in owner.into_iter().chain(
                self.records
                    .get(thread)
                    .into_iter()
                    .flat_map(|record| record.watchers.iter().copied()),
            ) {
                if !recipients.contains(&conn) {
                    recipients.push(conn);
                }
            }
        }
        recipients
    }

    pub(crate) fn notify_run_diagnostic_snapshot(
        &self,
        run: &str,
        recipients: &[ConnId],
        child: Option<&crate::runs::ChildUpstreamError>,
        actions: &mut Vec<Action>,
    ) {
        let Some(record) = self.runs.runs.get(run) else {
            return;
        };
        let message = proto::notification(
            "broker/run/updated",
            Some(json!({
                "runId": run,
                "threadId": record.thread_id,
                "diagnosticRevision": record.diagnostic_revision,
                "upstreamError": record.upstream_error,
                "lastProgressAt": record.last_progress_at,
                "state": record.state,
                "status": record.status,
                "interruptReason": record.interrupt_reason,
                "stopOutcome": record.stop_outcome,
                "stopCheck": record.stop_check,
                "stall": record.stall,
                "stopDiagnostics": record.stop_diagnostics,
                "childUpstreamErrors": record.child_upstream_errors,
                "childUpstreamError": child,
            })),
        );
        for &conn in recipients {
            self.send_ext(conn, message.clone(), actions);
        }
    }

    /// `broker/run/started{threadId, runId, shortId, requestId}`：认领建立（#2）或续跑核实通过（#2d）的那一刻、
    /// 在把流式请求转发给 app-server 之前，发给发起连接（只发 cxb 连接；astra T1）。须在 `run_opened` 之后调用。
    pub(crate) fn send_run_started(
        &self,
        thread_id: &str,
        run_id: &str,
        request: &ClientRequest,
        actions: &mut Vec<Action>,
    ) {
        let short_id = self.runs.runs.get(run_id).map(|run| run.short_id.clone());
        self.send_ext(
            request.conn,
            proto::notification(
                "broker/run/started",
                Some(serde_json::json!({
                    "threadId": thread_id,
                    "runId": run_id,
                    "shortId": short_id,
                    "requestId": request.id,
                })),
            ),
            actions,
        );
    }

    /// 当前认领在线的 Client owner（owner 已断开、托管、没有认领时为 None）。
    pub(crate) fn owner_conn(&self, thread_id: &str) -> Option<ConnId> {
        let claim = self.claims.get(thread_id)?;
        match claim.owner()? {
            Owner::Client(conn) if !claim.owner_gone && self.live_conns.contains(conn) => {
                Some(*conn)
            }
            _ => None,
        }
    }

    /// 线程的扩展通知：发给 `owner`（若给出）和该线程全部 watcher，只发 cxb 连接，同一连接只发一次。
    pub(crate) fn notify_thread_ext(
        &self,
        thread_id: &str,
        owner: Option<ConnId>,
        method: &str,
        params: Value,
        actions: &mut Vec<Action>,
    ) {
        let message = proto::notification(method, Some(params));
        let mut sent = Vec::new();
        for conn in owner.into_iter().chain(
            self.records
                .get(thread_id)
                .into_iter()
                .flat_map(|record| record.watchers.iter().copied()),
        ) {
            if !sent.contains(&conn) {
                sent.push(conn);
                self.send_ext(conn, message.clone(), actions);
            }
        }
    }

    /// 线程上的 app-server 通知（2.4 节「带 threadId 的只发给该线程的 owner 和所有 watcher」）：
    /// 发给 `owner`（Client 且未断开）与该线程全部 watcher，同一连接只发一次。上游通知不受 cxb 门控。
    pub(crate) fn forward_notification(
        &self,
        thread_id: &str,
        owner: Option<&Owner>,
        gone: bool,
        event: &HeldNotification,
        actions: &mut Vec<Action>,
    ) {
        let owner = match owner {
            Some(Owner::Client(conn)) if !gone => Some(*conn),
            _ => None,
        };
        let mut sent = Vec::new();
        for conn in owner.into_iter().chain(
            self.records
                .get(thread_id)
                .into_iter()
                .flat_map(|record| record.watchers.iter().copied()),
        ) {
            if !sent.contains(&conn) {
                sent.push(conn);
                actions.push(Action::Send {
                    conn,
                    message: proto::notification(&event.method, event.params.clone()),
                });
            }
        }
    }
}

#[cfg(test)]
mod n2_tests {
    use super::*;
    use crate::claim::{step, Event};

    fn call(m: &mut Machine, params: Value) -> Vec<Action> {
        let (next, actions) = step(
            std::mem::take(m),
            Event::ClientRequest {
                request: ClientRequest {
                    conn: 1,
                    id: json!(7),
                    method: "broker/threads/resumeOverrides".into(),
                    params: Some(params),
                },
                run_id: None,
            },
        );
        *m = next;
        actions
    }
    fn reply(actions: &[Action]) -> Value {
        actions
            .iter()
            .find_map(|a| match a {
                Action::Send { message, .. } if message["id"] == 7 => Some(message.clone()),
                _ => None,
            })
            .unwrap()
    }
    #[test]
    fn 恢复覆盖严格校验清除关闭与未知线程() {
        let mut m = Machine::new(false);
        assert_eq!(
            reply(&call(&mut m, json!({"threadId":"未知","overrides":{}})))["error"]["data"]
                ["reason"],
            "unknownThread"
        );
        m.record("仅缓存");
        assert_eq!(
            reply(&call(&mut m, json!({"threadId":"仅缓存","overrides":{}})))["error"]["data"]
                ["reason"],
            "unknownThread"
        );
        m.runs.ensure_thread("线程", 1, 1);
        for overrides in [
            json!([]),
            json!(true),
            json!({"other":1}),
            json!({"config":null}),
            json!({"config":[]}),
            json!({"developerInstructions":null}),
            json!({"developerInstructions":7}),
        ] {
            assert_eq!(
                reply(&call(
                    &mut m,
                    json!({"threadId":"线程","overrides":overrides})
                ))["error"]["code"],
                -32602
            );
        }
        for params in [
            json!({"threadId":"线程"}),
            json!({"threadId":"","overrides":{}}),
            json!({"threadId":1,"overrides":{}}),
            json!([]),
        ] {
            assert_eq!(reply(&call(&mut m, params))["error"]["code"], -32602);
        }
        let overrides = json!({"config":{"features.default_mode_request_user_input":true,"custom.value":{"nested":[1,null]}},"developerInstructions":"由用户决定时提问"});
        let actions = call(&mut m, json!({"threadId":"线程","overrides":overrides}));
        assert_eq!(reply(&actions)["result"], json!({"threadId":"线程"}));
        assert!(actions
            .iter()
            .any(|a| matches!(a, Action::Store(crate::store::StoreOp::PutThreads(_)))));
        let expected = json!({"threadId":"线程","excludeTurns":true,"config":overrides["config"],"developerInstructions":overrides["developerInstructions"]});
        assert_eq!(
            m.resume_params("线程", json!({"threadId":"线程"})),
            expected
        );
        let existing = json!({"threadId":"线程","excludeTurns":true,"config":{"explicit":false},"developerInstructions":"已有指令","persistExtendedHistory":false});
        assert_eq!(m.resume_params("线程", existing.clone()), existing);
        assert_eq!(
            m.resume_params(
                "线程",
                json!({"threadId":"线程","excludeTurns":false,"config":{"explicit":false},"developerInstructions":"已有指令","persistExtendedHistory":false})
            ),
            existing
        );
        call(&mut m, json!({"threadId":"线程","overrides":null}));
        assert!(m
            .runs
            .index
            .threads
            .values()
            .all(|entry| entry.resume_overrides.is_none()));
        assert_eq!(
            m.resume_params("线程", json!({"threadId":"线程"})),
            json!({"threadId":"线程","excludeTurns":true})
        );
        m.closing = true;
        assert_eq!(
            reply(&call(&mut m, json!({"threadId":"线程","overrides":{}})))["error"]["data"]
                ["reason"],
            "shuttingDown"
        );
    }

    #[test]
    fn 恢复屏障带覆盖且客户端恢复不注入() {
        let mut m = Machine::new(false);
        m.runs.ensure_thread("线程", 1, 1);
        call(
            &mut m,
            json!({"threadId":"线程","overrides":{"config":{"开关":true},"developerInstructions":"指令"}}),
        );
        let request = |method: &str| Event::ClientRequest {
            request: ClientRequest {
                conn: 1,
                id: json!(8),
                method: method.into(),
                params: Some(json!({"threadId":"线程"})),
            },
            run_id: None,
        };
        let (m, actions) = step(m, request("thread/resume"));
        let forwarded = actions
            .iter()
            .find_map(|a| match a {
                Action::Request(c) => Some(c),
                _ => None,
            })
            .unwrap();
        assert_eq!(forwarded.params, Some(json!({"threadId":"线程"})));
        let (_, actions) = step(m, request("turn/start"));
        let barrier = actions
            .iter()
            .find_map(|a| match a {
                Action::Request(c) if c.method == "thread/resume" => Some(c),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            barrier.params,
            Some(
                json!({"threadId":"线程","excludeTurns":true,"config":{"开关":true},"developerInstructions":"指令"})
            )
        );
    }
}
