use super::*;

fn encode(entry: &Entry, answer: &Value) -> Result<Value, ()> {
    match entry.item.kind {
        InboxKind::Approval => {
            let decision = answer
                .get("decision")
                .and_then(Value::as_str)
                .filter(|d| matches!(*d, "accept" | "acceptForSession" | "decline" | "cancel"))
                .ok_or(())?;
            if entry.item.method == "item/permissions/requestApproval" {
                // 魔改原文 local-vs-base.diff 1015–1035：接受授予请求权限，拒绝授予空权限。
                let permissions = if matches!(decision, "accept" | "acceptForSession") {
                    entry
                        .item
                        .params
                        .get("permissions")
                        .filter(|v| !v.is_null())
                        .cloned()
                        .unwrap_or(json!({}))
                } else {
                    json!({})
                };
                Ok(
                    json!({"scope":if decision == "acceptForSession" { "session" } else { "turn" },"permissions":permissions}),
                )
            } else {
                Ok(json!({"decision":decision}))
            }
        }
        InboxKind::Guardian => match answer.get("override").and_then(Value::as_bool) {
            Some(false) => Ok(Value::Null),
            Some(true) if answer.get("event").is_some_and(Value::is_object) => {
                Ok(answer["event"].clone())
            }
            _ => Err(()),
        },
        _ => Ok(answer.clone()),
    }
}

impl Machine {
    pub(super) fn resolve_inbox(
        &mut self,
        to: Recipient,
        params: Value,
        actions: &mut Vec<Action>,
    ) {
        let Some(id) = params.get("id").and_then(Value::as_str) else {
            invalid(to, actions);
            return;
        };
        let Some(answer) = params.get("answer") else {
            invalid(to, actions);
            return;
        };
        let Some(entry) = self.inbox.entries.get(id) else {
            reply(to, Err(error("notFound")), actions);
            return;
        };
        if let Some(error) = self.lifecycle_error(&entry.item.thread_id) {
            reply(to, Err(error), actions);
            return;
        }
        if self.run_purge_busy(&entry.item.thread_id) {
            reply(
                to,
                Err(proto::error_object(
                    -32001,
                    "线程正在删除本地记录",
                    Some(json!({"reason":"purgePending"})),
                )),
                actions,
            );
            return;
        }
        match entry.item.status {
            InboxStatus::Expired | InboxStatus::Cancelled => {
                reply(to, Err(error(status_event(entry.item.status))), actions);
                return;
            }
            InboxStatus::Resolved => {
                reply(
                    to,
                    if entry.answer.as_ref() == Some(answer) {
                        Ok(json!({"status":"resolved","duplicate":true}))
                    } else {
                        Err(error("alreadyResolved"))
                    },
                    actions,
                );
                return;
            }
            InboxStatus::Resolving => {
                if entry.answer.as_ref() != Some(answer) {
                    reply(to, Err(error("alreadyResolved")), actions);
                    return;
                }
                if entry.waiters.len() >= MAX_RESOLVERS {
                    reply(to, Err(error("tooManyWaiters")), actions);
                    return;
                }
                let count = self.inbox.entries.len();
                let bytes = serde_json::to_vec(&to).unwrap().len() + 1;
                if !self.room_inbox(bytes, 0, actions) {
                    reply(to, Err(error("capacityFull")), actions);
                    self.save_inbox(actions);
                    return;
                }
                self.inbox.entries.get_mut(id).unwrap().waiters.push(to);
                self.inbox.refresh_entry(id);
                if self.inbox.entries.len() != count {
                    self.save_inbox(actions);
                }
                return;
            }
            InboxStatus::Pending => (),
        }
        let Ok(payload) = encode(entry, answer) else {
            invalid(to, actions);
            return;
        };
        let bytes = serde_json::to_vec(answer).unwrap().len()
            + serde_json::to_vec(&to).unwrap().len()
            + 128;
        if !self.room_inbox(bytes, 0, actions) {
            reply(to, Err(error("capacityFull")), actions);
            self.save_inbox(actions);
            return;
        }
        let op = self.inbox.next();
        let entry = self.inbox.entries.get_mut(id).unwrap();
        entry.item.status = InboxStatus::Resolving;
        entry.answer = Some(answer.clone());
        entry.waiters.push(to);
        entry.first_alive = true;
        entry.op = Some(op);
        self.inbox.operations.insert(op, id.into());
        if let Some(app) = entry.app_id.clone() {
            entry.phase = Some(Phase::Reply);
            actions.push(Action::AppReplyAcked {
                op,
                id: app,
                payload: Ok(payload),
            });
        } else if answer["override"] == false {
            self.finish_inbox(id, InboxStatus::Resolved, None, actions);
            return;
        } else {
            entry.phase = Some(Phase::Resume);
            let thread_id = entry.item.thread_id.clone();
            self.guardian_request(
                id,
                InboxRpc::GuardianResumeFor { op, thread_id },
                None,
                actions,
            );
        }
        self.inbox.refresh_entry(id);
        self.save_inbox(actions);
    }
    fn guardian_request(
        &self,
        id: &str,
        rpc: InboxRpc,
        event: Option<Value>,
        actions: &mut Vec<Action>,
    ) {
        let entry = &self.inbox.entries[id];
        let thread = &entry.item.thread_id;
        let (method, params) = match rpc {
            InboxRpc::GuardianResume { .. } | InboxRpc::GuardianResumeFor { .. } => (
                "thread/resume",
                self.resume_params(
                    thread,
                    json!({"threadId":thread,"persistExtendedHistory":false}),
                ),
            ),
            InboxRpc::GuardianOverride { .. } => (
                "thread/approveGuardianDeniedAction",
                json!({"threadId":thread,"event":event}),
            ),
        };
        actions.push(Action::Request(RequestContext {
            resume_retried: false,
            stream_id: None,
            review_id: None,
            client: None,
            token: Some(entry.token()),
            operation: Operation::Inbox(rpc),
            method: method.into(),
            params: Some(params),
        }));
    }
    fn guardian_unsubscribe(&self, thread: &str, actions: &mut Vec<Action>) {
        if !self.claims.contains_key(thread) {
            let claim_id = self.records.get(thread).map_or(0, |r| r.last_claim_id);
            self.schedule_unsubscribe(
                ClaimToken {
                    thread_id: thread.into(),
                    claim_id,
                },
                actions,
            );
        }
    }
    fn retry_inbox(&mut self, id: &str, failure: Value, actions: &mut Vec<Action>) {
        let Some(entry) = self.inbox.entries.get_mut(id).filter(|e| e.active()) else {
            return;
        };
        if entry.upstream_closed {
            self.finish_inbox(id, InboxStatus::Cancelled, None, actions);
            return;
        }
        if entry.expiry_requested {
            self.expire_inbox(id, actions);
            return;
        }
        if let Some(op) = entry.op.take() {
            self.inbox.operations.remove(&op);
        }
        entry.item.status = InboxStatus::Pending;
        entry.answer = None;
        entry.phase = None;
        entry.first_alive = false;
        for to in std::mem::take(&mut entry.waiters) {
            reply(to, Err(failure.clone()), actions);
        }
        self.inbox.refresh_entry(id);
        self.save_inbox(actions);
    }
    pub(crate) fn inbox_reply_written(
        &mut self,
        op: u64,
        outcome: ReplyOutcome,
        actions: &mut Vec<Action>,
    ) {
        self.inbox.trace("inbox_reply_written");
        let Some(id) = self
            .inbox
            .operations
            .get(&op)
            .cloned()
            .filter(|id| self.inbox.entries[id].phase == Some(Phase::Reply))
        else {
            actions.push(Action::Debug("忽略已退役操作的写入确认".into()));
            return;
        };
        if self.inbox.entries[&id].shutdown_requested && outcome != ReplyOutcome::Written {
            self.finish_inbox_shutdown(&id, actions);
            return;
        }
        match outcome {
            ReplyOutcome::Written => self.finish_inbox(&id, InboxStatus::Resolved, None, actions),
            ReplyOutcome::Failed(message) => self.retry_inbox(
                &id,
                proto::error_object(
                    -32000,
                    "回答写入失败",
                    Some(json!({"reason":"writeFailed","detail":message})),
                ),
                actions,
            ),
            ReplyOutcome::Cancelled => {
                if self.inbox.entries[&id].expiry_requested {
                    self.expire_inbox(&id, actions);
                } else {
                    self.finish_inbox(&id, InboxStatus::Cancelled, None, actions);
                }
            }
        }
    }
    pub(crate) fn guardian_resume_current(&self, rpc: &InboxRpc) -> bool {
        let op = match rpc {
            InboxRpc::GuardianResume { op } | InboxRpc::GuardianResumeFor { op, .. } => op,
            _ => return false,
        };
        self.inbox
            .operations
            .get(op)
            .and_then(|id| self.inbox.entries.get(id))
            .is_some_and(|entry| entry.phase == Some(Phase::Resume))
    }

    /// 退役或删除前的 Guardian 恢复不再提供可重建索引的证据。
    pub(crate) fn guardian_resume_can_learn(&self, rpc: &InboxRpc) -> bool {
        let op = match rpc {
            InboxRpc::GuardianResume { op } | InboxRpc::GuardianResumeFor { op, .. } => op,
            _ => return true,
        };
        self.inbox
            .operations
            .get(op)
            .and_then(|id| self.inbox.entries.get(id))
            .is_some_and(|entry| {
                entry.phase == Some(Phase::Resume)
                    && !self.run_purge_busy(&entry.item.thread_id)
                    && !self.runs.index.removed.contains_key(&entry.item.thread_id)
            })
    }

    /// 保留待人工处理的事项：用户先回答或拒绝，再重试删除。
    pub(crate) fn purge_guardian_pending(&self, thread: &str) -> bool {
        self.inbox.entries.values().any(|entry| {
            entry.item.thread_id == thread
                && entry.item.kind == InboxKind::Guardian
                && entry.active()
        })
    }

    pub(crate) fn inbox_rpc_reply(
        &mut self,
        rpc: InboxRpc,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        self.inbox.trace("inbox_rpc_reply");
        let can_learn = self.guardian_resume_can_learn(&rpc);
        let (op, phase, resumed_thread) = match rpc {
            InboxRpc::GuardianResume { op } => (op, Phase::Resume, None),
            InboxRpc::GuardianResumeFor { op, thread_id } => (op, Phase::Resume, Some(thread_id)),
            InboxRpc::GuardianOverride { op } => (op, Phase::Override, None),
        };
        if result.is_ok() && !self.closing {
            if let Some(thread) = resumed_thread.as_deref() {
                // 已知请求线程的 resume 成功本身即证明订阅恢复，兼容不重复返回 thread.id 的服务器。
                self.record(thread).subscribed = true;
            }
        }
        let Some(id) = self
            .inbox
            .operations
            .get(&op)
            .cloned()
            .filter(|id| self.inbox.entries[id].phase.as_ref() == Some(&phase))
        else {
            if phase == Phase::Resume
                && result.is_ok()
                && !self.closing
                && !self.inbox.operations.contains_key(&op)
            {
                // 公共 apply_reply 已学习服务器事实；操作退役不抹去恢复订阅的副作用。
                let thread = resumed_thread
                    .as_deref()
                    .or_else(|| result.as_ref().ok()?.pointer("/thread/id")?.as_str());
                if let Some(thread) = thread {
                    self.guardian_unsubscribe(thread, actions);
                }
            }
            actions.push(Action::Debug(
                "忽略阶段或操作代号不匹配的 Guardian 回包".into(),
            ));
            return;
        };
        let thread = self.inbox.entries[&id].item.thread_id.clone();
        if phase == Phase::Resume {
            if let Some(error) = self.lifecycle_error(&thread) {
                self.retry_inbox(&id, error, actions);
                self.guardian_unsubscribe(&thread, actions);
                return;
            }
        }
        if phase == Phase::Resume && !can_learn {
            self.finish_inbox(&id, InboxStatus::Cancelled, None, actions);
            self.guardian_unsubscribe(&thread, actions);
            return;
        }
        match result {
            Err(upstream) => {
                // 原始线上错误完整保留在 cause；扩展协议错误仍使用 -32000 与稳定 reason。
                self.retry_inbox(
                    &id,
                    proto::error_object(
                        -32000,
                        "Guardian 操作失败",
                        Some(json!({"reason":"rpcFailed","cause":upstream})),
                    ),
                    actions,
                );
                self.guardian_unsubscribe(&thread, actions);
            }
            Ok(_) if phase == Phase::Resume => {
                let entry = self.inbox.entries.get_mut(&id).unwrap();
                entry.phase = Some(Phase::Override);
                let event = entry.answer.as_ref().unwrap()["event"].clone();
                self.inbox.refresh_entry(&id);
                self.guardian_request(&id, InboxRpc::GuardianOverride { op }, Some(event), actions);
            }
            Ok(_) => {
                self.finish_inbox(&id, InboxStatus::Resolved, None, actions);
                self.guardian_unsubscribe(&thread, actions);
            }
        }
    }
}
