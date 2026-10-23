//! 子线程订阅责任：以本连接最后发出的操作为准，通知不能覆盖操作顺序。
use super::*;
use crate::claim::{Event, TimerKey};
use crate::runs::ReleaseKind;
use serde_json::json;

const WAIT_MS: u64 = 15_000;
const MAX_COMPENSATIONS: u32 = 3;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ChildWaiter {
    Run { thread: String, claim_id: u64 },
    Discovery(u64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildBarrier {
    pub pending: BTreeSet<String>,
    pub parent_ready: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionResult {
    Pending,
    Attached,
    Unsubscribed,
    NotSubscribed,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Released {
    pub op: u64,
    pub resume: bool,
    pub result: SubscriptionResult,
    pub context: RequestContext,
    pub probe: Option<u64>,
    pub failures: u32,
    pub retry_at: u64,
    pub compensations: u32,
    pub compensation_at: u64,
}

impl Machine {
    pub(crate) fn child_release_capacity(
        &mut self,
        thread: &str,
        actions: &mut Vec<Action>,
    ) -> bool {
        if self.relations.aux.released.contains_key(thread)
            || self.relations.aux.released.len() < MAX_ATTACHMENTS
        {
            return true;
        }
        if !self.relations.aux.release_capacity_warned {
            self.relations.aux.release_capacity_warned = true;
            actions.push(Action::Debug("子线程释放责任表已满，暂停新增退订".into()));
        }
        false
    }

    pub(crate) fn release_idle_children(&mut self, only: Option<&str>, actions: &mut Vec<Action>) {
        if self.closing {
            return;
        }
        let candidates: Vec<_> = only.map_or_else(
            || self.relations.attachments.keys().cloned().collect(),
            |thread| vec![thread.to_owned()],
        );
        for thread in candidates {
            let attached = self
                .relations
                .attachments
                .get(&thread)
                .is_some_and(|a| a.valid && a.conn_epoch == self.relations.conn_epoch);
            if attached
                && self.child_subscription_idle(&thread)
                && self.run_root_released(&thread)
                && self.child_release_capacity(&thread, actions)
            {
                self.child_send_subscription(&thread, false, actions);
            }
        }
    }

    fn child_context(&mut self, thread: &str, method: &str) -> RequestContext {
        let op = self.relations.aux.next();
        RequestContext {
            resume_retried: false,
            stream_id: None,
            review_id: None,
            client: None,
            token: None,
            operation: Operation::ChildSubscription {
                thread: thread.into(),
                op,
            },
            method: method.into(),
            params: Some(match method {
                "thread/resume" => json!({"threadId":thread,"excludeTurns":true}),
                "thread/read" => json!({"threadId":thread,"includeTurns":false}),
                _ => json!({"threadId":thread}),
            }),
        }
    }

    fn child_register_subscription(&mut self, context: &RequestContext, actions: &mut Vec<Action>) {
        let Some(thread) = context.params.as_ref().and_then(|p| p["threadId"].as_str()) else {
            return;
        };
        let resume = context.method == "thread/resume";
        let old = self.relations.aux.released.get(thread).cloned();
        if old.as_ref().is_some_and(|r| r.context == *context) {
            return;
        }
        let op = self.relations.aux.next();
        self.relations.aux.released.insert(
            thread.into(),
            Released {
                op,
                resume,
                result: SubscriptionResult::Pending,
                context: context.clone(),
                probe: None,
                failures: old.as_ref().map_or(0, |r| r.failures),
                retry_at: old.as_ref().map_or(0, |r| r.retry_at),
                compensations: old.as_ref().map_or(0, |r| r.compensations),
                compensation_at: old.as_ref().map_or(0, |r| r.compensation_at),
            },
        );
        self.detach_evidence(thread);
        actions.push(Action::Unschedule {
            key: TimerKey::ChildRetry(thread.into()),
        });
        actions.push(Action::Schedule {
            token: Self::relation_token(),
            kind: TimerKind::ChildSubscription {
                thread: thread.into(),
                op,
            },
            delay_ms: WAIT_MS,
        });
    }

    fn child_send_subscription(&mut self, thread: &str, resume: bool, actions: &mut Vec<Action>) {
        let context = self.child_context(
            thread,
            if resume {
                "thread/resume"
            } else {
                "thread/unsubscribe"
            },
        );
        self.child_register_subscription(&context, actions);
        actions.push(Action::Request(context));
    }

    pub(super) fn child_probe(&mut self, thread: &str, actions: &mut Vec<Action>) {
        if self
            .relations
            .aux
            .released
            .get(thread)
            .is_none_or(|r| r.probe.is_some())
        {
            return;
        }
        let context = self.child_context(thread, "thread/read");
        let Operation::ChildSubscription { op, .. } = context.operation else {
            unreachable!()
        };
        self.relations.aux.released.get_mut(thread).unwrap().probe = Some(op);
        actions.push(Action::Request(context));
    }

    pub(crate) fn begin_child_run_barrier(&mut self, root: &str, actions: &mut Vec<Action>) {
        let Some(token) = self.token(root) else {
            return;
        };
        self.begin_child_barrier(
            root,
            ChildWaiter::Run {
                thread: root.into(),
                claim_id: token.claim_id,
            },
            actions,
        );
    }

    pub(super) fn begin_child_barrier(
        &mut self,
        root: &str,
        waiter: ChildWaiter,
        actions: &mut Vec<Action>,
    ) {
        if self.relations.aux.child_barriers.contains_key(&waiter) {
            return;
        }
        let pending: BTreeSet<_> = self
            .relations
            .aux
            .released
            .iter()
            .filter(|(t, r)| {
                (t.as_str() == root || self.descends_from(t, root))
                    && r.result != SubscriptionResult::Attached
            })
            .map(|(t, _)| t.clone())
            .collect();
        if pending.is_empty() {
            return;
        }
        self.relations.aux.child_barriers.insert(
            waiter.clone(),
            ChildBarrier {
                pending: pending.clone(),
                parent_ready: false,
            },
        );
        actions.push(Action::Schedule {
            token: Self::relation_token(),
            kind: TimerKind::ChildBarrier {
                waiter: waiter.clone(),
            },
            delay_ms: WAIT_MS,
        });
        for thread in pending {
            let r = self.relations.aux.released.get_mut(&thread).unwrap();
            r.compensations = 0;
            r.compensation_at = 0;
            if r.retry_at <= self.clock.mono_ms && r.result != SubscriptionResult::Pending {
                self.child_probe(&thread, actions);
            }
        }
    }

    /// 父屏障只登记就绪；最后一个子线程完成后再次进入实际提交入口。
    pub(crate) fn child_start_waiting(&mut self, root: &str) -> bool {
        let Some(token) = self.token(root) else {
            return false;
        };
        let waiter = ChildWaiter::Run {
            thread: root.into(),
            claim_id: token.claim_id,
        };
        if let Some(barrier) = self.relations.aux.child_barriers.get_mut(&waiter) {
            barrier.parent_ready = true;
            return true;
        }
        false
    }

    fn child_waited(&self, thread: &str) -> bool {
        self.relations
            .aux
            .child_barriers
            .values()
            .any(|b| b.pending.contains(thread))
    }

    fn child_observer_needs(&self, thread: &str) -> bool {
        self.claims
            .keys()
            .any(|root| root == thread || self.descends_from(thread, root))
            || self
                .relations
                .discoveries
                .values()
                .any(|d| !d.completed && (d.root == thread || self.descends_from(thread, &d.root)))
    }

    fn child_resolved(&mut self, thread: &str, success: bool, actions: &mut Vec<Action>) {
        let waiters: Vec<_> = self
            .relations
            .aux
            .child_barriers
            .iter()
            .filter(|(_, b)| b.pending.contains(thread))
            .map(|(w, _)| w.clone())
            .collect();
        for waiter in waiters {
            let b = self.relations.aux.child_barriers.get_mut(&waiter).unwrap();
            b.pending.remove(thread);
            let done = b.pending.is_empty();
            let ready = b.parent_ready;
            if !success {
                match &waiter {
                    ChildWaiter::Run {
                        thread: root,
                        claim_id,
                    } => {
                        let root = root.clone();
                        self.child_drop_waiter(&waiter, actions);
                        if self
                            .claims
                            .get(&root)
                            .is_some_and(|c| c.claim_id == *claim_id)
                        {
                            if let Some(request) = self
                                .claims
                                .get(&root)
                                .and_then(|c| c.pending_start.as_ref())
                            {
                                crate::claim::send_error(
                                    request,
                                    crate::proto::code::GENERAL_ERROR,
                                    &format!(
                                        "子代理线程 {thread} 恢复订阅失败，请稍后重试或新开线程"
                                    ),
                                    actions,
                                );
                            }
                            self.release_with_kind(&root, ReleaseKind::StartFailed, actions);
                        }
                        continue;
                    }
                    ChildWaiter::Discovery(id) => {
                        if let Some(work) = self.relations.aux.work.get_mut(id) {
                            work.reasons.insert("attachmentMissing".into());
                            work.missing
                                .entry(thread.into())
                                .or_default()
                                .push("attachmentMissing".into());
                        }
                    }
                }
            }
            if done {
                self.child_drop_waiter(&waiter, actions);
                match waiter {
                    ChildWaiter::Run { thread: root, .. } if ready => {
                        self.submit_stream(&root, actions)
                    }
                    ChildWaiter::Discovery(id) => self.discovery_start_sources(id, actions),
                    _ => {}
                }
            }
        }
    }

    fn child_drop_waiter(&mut self, waiter: &ChildWaiter, actions: &mut Vec<Action>) {
        self.relations.aux.child_barriers.remove(waiter);
        actions.push(Action::Unschedule {
            key: TimerKey::ChildBarrier(waiter.clone()),
        });
    }

    pub(crate) fn child_barrier_timeout(&mut self, waiter: ChildWaiter, actions: &mut Vec<Action>) {
        let Some(barrier) = self.relations.aux.child_barriers.get(&waiter).cloned() else {
            return;
        };
        // 每个等待者独立到期，不能结束其他等待者共享的挂回操作。
        self.child_drop_waiter(&waiter, actions);
        match waiter {
            ChildWaiter::Run { thread, claim_id } => {
                if self
                    .claims
                    .get(&thread)
                    .is_some_and(|c| c.claim_id == claim_id)
                {
                    let child = barrier.pending.first().cloned().unwrap_or_default();
                    if let Some(request) = self.claims[&thread].pending_start.as_ref() {
                        crate::claim::send_error(
                            request,
                            crate::proto::code::GENERAL_ERROR,
                            &format!("子代理线程 {child} 恢复订阅失败，请稍后重试或新开线程"),
                            actions,
                        );
                    }
                    self.release_with_kind(&thread, ReleaseKind::StartFailed, actions);
                }
            }
            ChildWaiter::Discovery(id) => {
                if let Some(work) = self.relations.aux.work.get_mut(&id) {
                    work.reasons.insert("attachmentMissing".into());
                    for child in barrier.pending {
                        work.missing
                            .entry(child)
                            .or_default()
                            .push("attachmentMissing".into());
                    }
                    self.discovery_start_sources(id, actions);
                }
            }
        }
    }

    /// 仅处理与最后发出上下文相同的回包；内部操作另有唯一编号，外部恢复含认领/客户端请求身份。
    pub(crate) fn child_subscription_event(&mut self, event: &Event, actions: &mut Vec<Action>) {
        self.relations.aux.ignored_subscription_reply = false;
        let (context, result) = match event {
            Event::Reply { context, result } | Event::LateReply { context, result } => {
                (context, Some(result))
            }
            Event::TimedOut { context, .. }
            | Event::TransportFailed { context, .. }
            | Event::LateDiscarded { context }
            | Event::LateEvicted { context, .. } => (context, None),
            _ => return,
        };
        let Some(thread) = context.params.as_ref().and_then(|p| p["threadId"].as_str()) else {
            return;
        };
        let Some(released) = self.relations.aux.released.get(thread).cloned() else {
            self.relations.aux.ignored_subscription_reply = self.thread_relation(thread).is_some()
                && self.convergences.closed_threads.contains(thread);
            return;
        };
        if context.method == "thread/read" {
            let Operation::ChildSubscription { op, .. } = context.operation else {
                return;
            };
            if released.probe != Some(op) {
                return;
            }
            self.relations.aux.released.get_mut(thread).unwrap().probe = None;
            // 0.160.0 read_thread_view：只读存储/get_thread，不调用 resume；此精确错误表示存储与运行时均不存在。
            let unloaded = match result {
                Some(Ok(value)) => value["thread"]["status"]["type"] == "notLoaded",
                Some(Err(error)) => {
                    error["message"].as_str() == Some(&format!("thread not loaded: {thread}"))
                }
                _ => false,
            };
            if unloaded {
                self.child_confirm_unloaded(thread, actions);
            } else if matches!(result, Some(Ok(_))) && self.child_waited(thread) {
                self.child_send_subscription(thread, true, actions);
            } else if self.child_waited(thread) {
                self.child_resolved(thread, false, actions);
            }
            return;
        }
        if released.context != *context {
            self.relations.aux.ignored_subscription_reply = true;
            return;
        }
        if result.is_none() && released.result == SubscriptionResult::Unknown {
            return;
        }
        actions.push(Action::Unschedule {
            key: TimerKey::ChildSubscription(thread.into()),
        });
        let status = match result {
            Some(Ok(_)) if released.resume => SubscriptionResult::Attached,
            Some(Ok(value)) if value["status"] == "notLoaded" => {
                self.child_confirm_unloaded(thread, actions);
                return;
            }
            Some(Ok(value)) if value["status"] == "unsubscribed" => {
                SubscriptionResult::Unsubscribed
            }
            Some(Ok(value)) if value["status"] == "notSubscribed" => {
                SubscriptionResult::NotSubscribed
            }
            _ => SubscriptionResult::Unknown,
        };
        let r = self.relations.aux.released.get_mut(thread).unwrap();
        r.result = status;
        if status == SubscriptionResult::Attached {
            r.failures = 0;
            r.retry_at = 0;
            r.compensations = 0;
            r.compensation_at = 0;
            self.attach(thread, AttachmentSource::ExplicitResume);
            self.child_resolved(thread, true, actions);
        } else if status == SubscriptionResult::Unknown {
            if released.resume {
                r.failures = r.failures.saturating_add(1);
            } else if r.compensations > 0 {
                r.compensation_at = self
                    .clock
                    .mono_ms
                    .saturating_add(5_000 << (r.compensations - 1).min(2));
            }
            if released.resume && r.failures >= 3 {
                r.retry_at = self
                    .clock
                    .mono_ms
                    .saturating_add(5_000 << (r.failures - 3).min(2));
            }
            self.detach_evidence(thread);
            // 退订结果未知时，仍可用同线程后续 resume 收敛；resume 未确认则本次开工失败。
            if released.resume && result.is_some() {
                self.child_resolved(thread, false, actions);
            }
        }
        if self.child_waited(thread) && status != SubscriptionResult::Attached {
            self.child_probe(thread, actions);
        }
    }

    pub(super) fn child_confirm_unloaded(&mut self, thread: &str, actions: &mut Vec<Action>) {
        self.convergences.closed_threads.insert(thread.into());
        self.relations.aux.released.remove(thread);
        self.relations.aux.release_capacity_warned = false;
        self.detach_evidence(thread);
        for key in [
            TimerKey::ChildSubscription(thread.into()),
            TimerKey::ChildRetry(thread.into()),
        ] {
            actions.push(Action::Unschedule { key });
        }
        self.child_resolved(thread, true, actions);
    }

    pub(super) fn child_closed(&mut self, thread: &str, actions: &mut Vec<Action>) {
        // closed 可以属于旧运行时：已发出但尚未确认的 resume 仍可能在它之后冷恢复。
        if self.relations.aux.released.get(thread).is_some_and(|r| {
            r.resume
                && matches!(
                    r.result,
                    SubscriptionResult::Pending | SubscriptionResult::Unknown
                )
        }) {
            return;
        }
        self.child_confirm_unloaded(thread, actions);
    }

    pub(crate) fn child_subscription_timeout(
        &mut self,
        thread: &str,
        op: u64,
        actions: &mut Vec<Action>,
    ) {
        let Some(r) = self
            .relations
            .aux
            .released
            .get(thread)
            .filter(|r| r.op == op && r.result == SubscriptionResult::Pending)
            .cloned()
        else {
            return;
        };
        self.child_subscription_event(&Event::LateDiscarded { context: r.context }, actions);
    }

    pub(crate) fn child_subscription_retry(
        &mut self,
        thread: &str,
        op: u64,
        actions: &mut Vec<Action>,
    ) {
        if self
            .relations
            .aux
            .released
            .get(thread)
            .is_some_and(|r| r.op == op)
        {
            self.child_reconcile(thread, actions);
        }
    }

    fn child_reconcile(&mut self, thread: &str, actions: &mut Vec<Action>) {
        let Some(r) = self.relations.aux.released.get(thread).cloned() else {
            return;
        };
        if self.closing {
            return;
        }
        if self.child_waited(thread) {
            if r.result != SubscriptionResult::Pending
                && r.result != SubscriptionResult::Attached
                && r.probe.is_none()
                && r.retry_at <= self.clock.mono_ms
            {
                self.child_probe(thread, actions);
            }
            return;
        }
        let needed = self.child_observer_needs(thread) || !self.child_subscription_idle(thread);
        if needed {
            return;
        }
        let compensate = r.resume || r.result == SubscriptionResult::Unknown;
        if !compensate || r.compensations >= MAX_COMPENSATIONS {
            return;
        }
        let retry_at = r.retry_at.max(r.compensation_at);
        if retry_at > self.clock.mono_ms {
            actions.push(Action::Schedule {
                token: Self::relation_token(),
                kind: TimerKind::ChildRetry {
                    thread: thread.into(),
                    op: r.op,
                },
                delay_ms: retry_at - self.clock.mono_ms,
            });
            return;
        }
        let next = r.compensations + 1;
        self.child_send_subscription(thread, false, actions);
        self.relations
            .aux
            .released
            .get_mut(thread)
            .unwrap()
            .compensations = next;
    }

    /// 所有入口发出的已知子线程订阅操作统一登记；结束后的扫描覆盖 goal、最后待答事项、运行和发现。
    pub(crate) fn child_subscriptions_after_step(&mut self, actions: &mut Vec<Action>) {
        let requests: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Request(c)
                    if matches!(c.method.as_str(), "thread/resume" | "thread/unsubscribe") =>
                {
                    Some(c.clone())
                }
                _ => None,
            })
            .collect();
        for context in requests {
            let Some(thread) = context.params.as_ref().and_then(|p| p["threadId"].as_str()) else {
                continue;
            };
            if self.thread_relation(thread).is_some()
                && (context.method == "thread/unsubscribe"
                    || self.relations.aux.released.contains_key(thread)
                    || self.convergences.closed_threads.contains(thread))
            {
                self.child_register_subscription(&context, actions);
            }
        }
        let backoff: Vec<_> = self
            .relations
            .aux
            .released
            .iter()
            .filter(|(t, r)| r.retry_at > self.clock.mono_ms && self.child_waited(t))
            .map(|(t, _)| t.clone())
            .collect();
        for thread in backoff {
            self.child_resolved(&thread, false, actions);
        }
        let obsolete: Vec<_> = self
            .relations
            .aux
            .child_barriers
            .keys()
            .filter(|w| {
                self.closing
                    || match w {
                        ChildWaiter::Run { thread, claim_id } => {
                            self.claims.get(thread).is_none_or(|c| {
                                c.claim_id != *claim_id || c.pending_start.is_none() || c.owner_gone
                            })
                        }
                        ChildWaiter::Discovery(id) => !self.relations.discoveries.contains_key(id),
                    }
            })
            .cloned()
            .collect();
        for waiter in obsolete {
            self.child_drop_waiter(&waiter, actions);
        }
        if self.closing {
            for thread in self.relations.aux.released.keys() {
                actions.push(Action::Unschedule {
                    key: TimerKey::ChildSubscription(thread.clone()),
                });
                actions.push(Action::Unschedule {
                    key: TimerKey::ChildRetry(thread.clone()),
                });
            }
            return;
        }
        self.release_idle_children(None, actions);
        let threads: Vec<_> = self.relations.aux.released.keys().cloned().collect();
        for thread in threads {
            self.child_reconcile(&thread, actions);
        }
    }

    /// `thread` 沿血缘向上能到达 `root`（不含自身）。
    fn descends_from(&self, thread: &str, root: &str) -> bool {
        let mut current = thread.to_owned();
        for _ in 0..=MAX_DEPTH {
            let Some(edge) = self.thread_relation(&current) else {
                return false;
            };
            if edge.parent_thread_id == root {
                return true;
            }
            current = edge.parent_thread_id;
        }
        false
    }

    /// 子线程本身已无活动：未被认领、不在停止树里、关系层不再需要、没有活跃 goal、没有未完成的轮次。
    fn child_subscription_idle(&self, thread: &str) -> bool {
        !self.child_observer_needs(thread)
            && !self.claims.contains_key(thread)
            && !self.tree_busy(Some(thread))
            && !self.relation_needs_subscription(thread)
            && !self
                .records
                .get(thread)
                .is_some_and(|r| r.goal_active || r.subscribed)
            && !self
                .relations
                .aux
                .observed
                .get(thread)
                .is_some_and(|turns| turns.iter().any(|t| t.started && !t.completed))
    }

    /// 沿血缘向上：必须是子线程（有父边），祖先里有 broker 跑过运行的线程，且没有任何祖先仍被认领或订阅。
    /// 不属于 broker 运行的线程（例如共享模式下别的客户端开的线程）一律不动。
    fn run_root_released(&self, thread: &str) -> bool {
        let mut current = thread.to_owned();
        let mut run_ancestor = false;
        for _ in 0..=MAX_DEPTH {
            let Some(edge) = self.thread_relation(&current) else {
                break;
            };
            current = edge.parent_thread_id;
            if self.claims.contains_key(&current) {
                return false;
            }
            if let Some(record) = self.records.get(&current) {
                if record.subscribed {
                    return false;
                }
                run_ancestor |= record.last_run.is_some();
            }
        }
        run_ancestor
    }
}
