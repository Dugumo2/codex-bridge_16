use super::*;

impl Machine {
    pub(super) fn wait_inbox(&mut self, to: Recipient, params: Value, actions: &mut Vec<Action>) {
        let id = params.get("id");
        let thread = params.get("threadId");
        let timeout = params
            .get("timeoutMs")
            .and_then(Value::as_u64)
            .filter(|t| *t <= MAX_WAIT_MS);
        if id.is_some_and(|v| !v.is_string())
            || thread.is_some_and(|v| !v.is_string())
            || timeout.is_none()
        {
            invalid(to, actions);
            return;
        }
        let id = id.and_then(Value::as_str).map(str::to_owned);
        let thread = thread.and_then(Value::as_str).map(str::to_owned);
        if let Some(id) = &id {
            let Some(entry) = self.inbox.entries.get(id) else {
                reply(to, Err(error("notFound")), actions);
                return;
            };
            if !entry.active() {
                reply(
                    to,
                    Ok(json!({"event":status_event(entry.item.status),"item":entry.item})),
                    actions,
                );
                return;
            }
        } else {
            let pending = self
                .inbox
                .entries
                .values()
                .filter(|e| e.active() && thread.as_ref().is_none_or(|t| t == &e.item.thread_id))
                .min_by_key(|e| (e.item.created_at, &e.item.id));
            if let Some(entry) = pending {
                reply(to, Ok(json!({"event":"added","item":entry.item})), actions);
                return;
            }
            if !self
                .claims
                .keys()
                .any(|t| thread.as_ref().is_none_or(|got| t == got))
            {
                reply(to, Ok(json!({"event":"idle"})), actions);
                return;
            }
        }
        if timeout == Some(0) {
            reply(to, Ok(json!({"event":"timeout"})), actions);
            return;
        }
        if self.inbox.waiters.len() + self.inbox.disconnected_timers.len() >= MAX_WAITERS {
            reply(to, Err(error("tooManyWaiters")), actions);
            return;
        }
        let waiter = self.inbox.next();
        self.inbox.waiters.insert(
            waiter,
            Waiter {
                recipient: to,
                id,
                thread: thread.clone(),
            },
        );
        actions.push(Action::Schedule {
            token: ClaimToken {
                thread_id: thread.unwrap_or_default(),
                claim_id: 0,
            },
            kind: TimerKind::Inbox(InboxTimer::WaitTimeout { waiter }),
            delay_ms: timeout.unwrap(),
        });
    }
    pub(super) fn take_inbox_waiter(
        &mut self,
        id: u64,
        actions: &mut Vec<Action>,
    ) -> Option<Waiter> {
        let waiter = self.inbox.waiters.remove(&id)?;
        actions.push(Action::Unschedule {
            key: TimerKey::InboxWaiter(id),
        });
        Some(waiter)
    }
    pub(super) fn wake_inbox(
        &mut self,
        item: Option<&InboxItem>,
        thread: &str,
        event: Value,
        actions: &mut Vec<Action>,
    ) {
        let ids: Vec<_> = self
            .inbox
            .waiters
            .iter()
            .filter(|(_, w)| match &w.id {
                Some(id) => item.is_some_and(|i| {
                    &i.id == id
                        && !matches!(i.status, InboxStatus::Pending | InboxStatus::Resolving)
                }),
                None => w.thread.as_deref().is_none_or(|t| t == thread),
            })
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            if let Some(waiter) = self.take_inbox_waiter(id, actions) {
                reply(waiter.recipient, Ok(event.clone()), actions);
            }
        }
    }
    pub(crate) fn inbox_run_ended(
        &mut self,
        thread_id: &str,
        run: Value,
        actions: &mut Vec<Action>,
    ) {
        self.inbox.trace("inbox_run_ended");
        self.inbox_flush_actions(actions);
        self.wake_inbox(
            None,
            thread_id,
            json!({"event":"runEnded","run":run}),
            actions,
        );
    }
    pub(crate) fn inbox_disconnect(&mut self, conn: ConnId) {
        self.inbox.trace("inbox_disconnect");
        self.inbox.waiters.retain(|id, w| {
            if w.recipient.0 == conn {
                self.inbox.disconnected_timers.push(*id);
                false
            } else {
                true
            }
        });
        let changed: Vec<_> = self
            .inbox
            .entries
            .iter()
            .filter(|(_, e)| e.waiters.iter().any(|to| to.0 == conn))
            .map(|(id, _)| id.clone())
            .collect();
        for id in changed {
            let e = self.inbox.entries.get_mut(&id).unwrap();
            if e.waiters.first().is_some_and(|to| to.0 == conn) {
                e.first_alive = false;
            }
            e.waiters.retain(|to| to.0 != conn);
            self.inbox.refresh_entry(&id);
        }
    }
    /// 保留骨架签名；由 step 的断开出口补调，撤销已经移除的等待者计时。
    /// 其它 inbox 动作入口也会排空，待撤销项最多占 MAX_WAITERS 个槽位。
    pub fn inbox_flush_actions(&mut self, actions: &mut Vec<Action>) {
        for waiter in std::mem::take(&mut self.inbox.disconnected_timers) {
            actions.push(Action::Unschedule {
                key: TimerKey::InboxWaiter(waiter),
            });
        }
    }
}
