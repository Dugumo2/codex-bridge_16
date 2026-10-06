use super::*;

impl Machine {
    /// 唯一终态出口：迟到结果只能记录日志，不能重新激活事项。
    pub(super) fn finish_inbox(
        &mut self,
        id: &str,
        status: InboxStatus,
        failure: Option<Value>,
        actions: &mut Vec<Action>,
    ) {
        if self.finish_inbox_state(id, status, failure, actions) {
            self.save_inbox(actions);
        }
    }
    /// 批量出口只做状态结算，调用方在整批结束后保存一次。
    fn finish_inbox_state(
        &mut self,
        id: &str,
        status: InboxStatus,
        failure: Option<Value>,
        actions: &mut Vec<Action>,
    ) -> bool {
        let Some(entry) = self.inbox.entries.get_mut(id).filter(|e| e.active()) else {
            actions.push(Action::Debug("忽略已关闭事项的迟到结果".into()));
            return false;
        };
        entry.item.status = status;
        entry.closed_at = Some(self.clock.now_ms);
        entry.generation = entry.generation.saturating_add(1);
        entry.phase = None;
        if let Some(op) = entry.op.take() {
            self.inbox.operations.remove(&op);
        }
        if let Some(app) = entry.app_id.take() {
            self.inbox
                .app_ids
                .remove(&(entry.item.thread_id.clone(), proto::request_id_key(&app)));
        }
        let first_alive = entry.first_alive;
        let item = entry.item.clone();
        for (i, to) in std::mem::take(&mut entry.waiters).into_iter().enumerate() {
            let result = if status == InboxStatus::Resolved {
                Ok(json!({"status":"resolved","duplicate": i != 0 || !first_alive}))
            } else {
                Err(failure
                    .clone()
                    .unwrap_or_else(|| error(status_event(status))))
            };
            reply(to, result, actions);
        }
        // 闭项只保留用于重复回答判断的那一份答案；其它操作状态全部退役。
        entry.first_alive = false;
        entry.expiry_requested = false;
        entry.shutdown_requested = false;
        self.inbox.refresh_entry(id);
        if !self.closing {
            self.schedule_inbox(id, CLOSED_RETENTION_MS, actions);
        }
        self.wake_inbox(
            Some(&item),
            &item.thread_id,
            json!({"event":status_event(status),"item":item}),
            actions,
        );
        if self.inbox_pending_count(&item.thread_id) == 0 {
            self.reclaim_check(&item.thread_id, actions);
        }
        true
    }
    pub(crate) fn inbox_server_resolved(
        &mut self,
        thread_id: &str,
        request_id: &Value,
        actions: &mut Vec<Action>,
    ) {
        self.inbox.trace("inbox_server_resolved");
        let Some(id) = self
            .inbox
            .app_ids
            .get(&(thread_id.into(), proto::request_id_key(request_id)))
            .cloned()
        else {
            actions.push(Action::Debug("忽略无活跃事项的上游请求关闭通知".into()));
            return;
        };
        let entry = self.inbox.entries.get_mut(&id).unwrap();
        entry.upstream_closed = true;
        if let Some(op) = entry.op {
            actions.push(Action::CancelAppReply { op });
        }
        self.finish_inbox(&id, InboxStatus::Cancelled, None, actions);
    }
    pub(crate) fn inbox_timer(
        &mut self,
        _token: ClaimToken,
        timer: InboxTimer,
        actions: &mut Vec<Action>,
    ) {
        self.inbox.trace("inbox_timer");
        self.inbox_flush_actions(actions);
        match timer {
            InboxTimer::WaitTimeout { waiter } => {
                if let Some(waiter) = self.take_inbox_waiter(waiter, actions) {
                    reply(waiter.recipient, Ok(json!({"event":"timeout"})), actions);
                }
            }
            InboxTimer::Expiry { id, generation } => {
                let Some(entry) = self
                    .inbox
                    .entries
                    .get_mut(&id)
                    .filter(|e| e.generation == generation)
                else {
                    actions.push(Action::Debug("忽略过期代号的事项计时器".into()));
                    return;
                };
                if !entry.active() {
                    self.remove_inbox(&id, actions);
                    self.save_inbox(actions);
                    return;
                }
                if entry.expiry_requested {
                    return;
                }
                if let Some(op) = entry.op.filter(|_| entry.app_id.is_some()) {
                    // 撤回排队项会返回 Cancelled；写出中的项不能撤回，等 Written 或 Failed。
                    entry.expiry_requested = true;
                    self.inbox.refresh_entry(&id);
                    actions.push(Action::CancelAppReply { op });
                } else {
                    self.expire_inbox(&id, actions);
                }
            }
        }
    }
    pub(super) fn expire_inbox(&mut self, id: &str, actions: &mut Vec<Action>) {
        if let Some(app) = self.inbox.entries.get(id).and_then(|e| e.app_id.clone()) {
            actions.push(Action::ServerError {
                id: app,
                error: requests::handler_error(TIMEOUT),
            });
        }
        self.finish_inbox(id, InboxStatus::Expired, None, actions);
    }
    pub(crate) fn inbox_claim_released(
        &mut self,
        thread_id: &str,
        claim_id: u64,
        actions: &mut Vec<Action>,
    ) {
        self.inbox.trace("inbox_claim_released");
        let ids: Vec<_> = self
            .inbox
            .entries
            .iter()
            .filter(|(_, e)| {
                e.active()
                    && e.app_id.is_some()
                    && !e.shutdown_requested
                    && e.item.thread_id == thread_id
                    && e.claim_id == claim_id
            })
            .map(|(id, _)| id.clone())
            .collect();
        let changed = !ids.is_empty();
        for id in ids {
            let e = &self.inbox.entries[&id];
            if let Some(op) = e.op {
                actions.push(Action::CancelAppReply { op });
            }
            if let Some(app) = &e.app_id {
                actions.push(Action::Dismiss { id: app.clone() });
            }
            self.finish_inbox_state(&id, InboxStatus::Cancelled, None, actions);
        }
        if changed {
            self.save_inbox(actions);
        }
    }
    pub(crate) fn inbox_shutdown(&mut self, actions: &mut Vec<Action>) {
        self.inbox.trace("inbox_shutdown");
        self.inbox_flush_actions(actions);
        for (waiter, pending) in std::mem::take(&mut self.inbox.waiters) {
            actions.push(Action::Unschedule {
                key: TimerKey::InboxWaiter(waiter),
            });
            reply(pending.recipient, Err(error("shuttingDown")), actions);
        }
        let ids: Vec<_> = self
            .inbox
            .entries
            .iter()
            .filter(|(_, e)| e.active())
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            let entry = self.inbox.entries.get_mut(&id).unwrap();
            // 关闭不继续让客户端等待；服务端提交结果仍由原 op 负责结算。
            for to in std::mem::take(&mut entry.waiters) {
                reply(to, Err(error("shuttingDown")), actions);
            }
            entry.first_alive = false;
            if let Some(app) = entry.app_id.clone() {
                if let Some(op) = entry.op {
                    entry.shutdown_requested = true;
                    self.inbox.refresh_entry(&id);
                    actions.push(Action::CancelAppReply { op });
                } else {
                    actions.push(Action::ServerError {
                        id: app,
                        error: requests::handler_error(SHUTDOWN),
                    });
                    self.finish_inbox_state(&id, InboxStatus::Cancelled, None, actions);
                }
            } else {
                // gd_ 保留为 pending；其 RPC 业务操作主动退役。
                if let Some(op) = entry.op.take() {
                    self.inbox.operations.remove(&op);
                }
                entry.phase = None;
                entry.answer = None;
                entry.item.status = InboxStatus::Pending;
                self.inbox.refresh_entry(&id);
            }
        }
        // 循环中从未构造完整快照；关闭计时由 core 统一撤销。
        self.save_inbox(actions);
    }
    /// 撤回成功或写入失败后才补关闭错误；Written 走正常 resolved 出口。
    pub(super) fn finish_inbox_shutdown(&mut self, id: &str, actions: &mut Vec<Action>) {
        if let Some(app) = self.inbox.entries.get(id).and_then(|e| e.app_id.clone()) {
            actions.push(Action::ServerError {
                id: app,
                error: requests::handler_error(SHUTDOWN),
            });
        }
        self.finish_inbox(id, InboxStatus::Cancelled, None, actions);
    }
    pub(crate) fn inbox_restore(&mut self, file: Option<InboxFile>, actions: &mut Vec<Action>) {
        self.inbox.trace("inbox_restore");
        let Some(file) = file else {
            return;
        };
        let mut items = file.items;
        // 先保留较新的旧记录；恢复不是重新发出任何旧操作。
        items.sort_by_key(|item| std::cmp::Reverse(item.created_at));
        let mut used = self.inbox.bytes();
        let mut dropped = 0;
        for item in items {
            if self.inbox.entries.contains_key(&item.id) {
                dropped += 1;
                continue;
            }
            let id = item.id.clone();
            let mut entry = Entry {
                item,
                app_id: None,
                claim_id: 0,
                generation: self.inbox.next(),
                closed_at: Some(self.clock.now_ms),
                answer: None,
                op: None,
                phase: None,
                upstream_closed: false,
                expiry_requested: false,
                shutdown_requested: false,
                byte_size: 0,
                waiters: Vec::new(),
                first_alive: false,
            };
            // 旧状态文字可能比 expired 短，预算同时容纳转换前后两种形状。
            let before = entry.bytes();
            entry.item.status = InboxStatus::Expired;
            let bytes = before.max(entry.bytes());
            if self.inbox.entries.len() >= MAX_ITEMS || used.saturating_add(bytes) > MAX_INBOX_BYTES
            {
                dropped += 1;
                continue;
            }
            used += bytes;
            self.inbox.insert_entry(entry);
            self.schedule_inbox(&id, CLOSED_RETENTION_MS, actions);
        }
        if dropped != 0 {
            actions.push(Action::Warn(format!(
                "恢复待回应事项时按预算裁剪 {dropped} 项"
            )));
        }
        self.save_inbox(actions);
    }
}
