use crate::claim::{Action, Machine};
use crate::runs::{ChildUpstreamError, DiagnosticField, ProgressTarget};
use serde_json::Value;

/// 冻结的进展方法清单；具体有效载荷与去重由 C1 判定。
pub const PROGRESS_METHODS: &[&str] = &[
    "turn/started",
    "turn/completed",
    "item/started",
    "item/completed",
    "item/agentMessage/delta",
    "item/plan/delta",
    "item/reasoning/summaryTextDelta",
    "item/commandExecution/outputDelta",
    "item/fileChange/outputDelta",
    "item/reasoning/summaryPartAdded",
    "item/commandExecution/terminalInteraction",
    "item/mcpToolCall/progress",
    "turn/diff/updated",
    "turn/plan/updated",
    "item/fileChange/patchUpdated",
    "hook/started",
    "hook/completed",
    "thread/tokenUsage/updated",
];

impl Machine {
    pub(crate) fn mark_run_diagnostic(
        &mut self,
        run: &str,
        field: DiagnosticField,
        child: Option<ChildUpstreamError>,
    ) {
        if !self.runs.runs.contains_key(run) {
            return;
        }
        let recipients = self.run_update_recipients(run);
        let dirty = self.runs.diagnostic_dirty.entry(run.into()).or_default();
        dirty.fields.insert(field);
        if child.is_some() {
            dirty.child = child;
        }
        for conn in recipients {
            if !dirty.recipients.contains(&conn) {
                dirty.recipients.push(conn);
            }
        }
    }

    pub(crate) fn flush_run_diagnostics(&mut self, actions: &mut Vec<Action>) {
        for (run, dirty) in std::mem::take(&mut self.runs.diagnostic_dirty) {
            let Some(record) = self.runs.runs.get_mut(&run) else {
                continue;
            };
            record.diagnostic_revision = record
                .diagnostic_revision
                .checked_add(1)
                .expect("诊断版本已耗尽");
            self.runs.updated.insert(run.clone(), self.clock.now_ms);
            if dirty
                .fields
                .iter()
                .any(|field| *field != DiagnosticField::LastProgressAt)
            {
                // 同事件其它账本钩子可能已经排盘，只保留最后的完整诊断快照。
                actions.retain(|action| !matches!(action, Action::Store(crate::store::StoreOp::PutRun(record)) if record.run_id == run));
                self.runs.put(&run, actions);
                self.notify_run_diagnostic_snapshot(
                    &run,
                    &dirty.recipients,
                    dirty.child.as_ref(),
                    actions,
                );
            } else {
                let already_queued = actions.iter().any(|action| matches!(action, Action::Store(crate::store::StoreOp::PutRun(record)) if record.run_id == run));
                if already_queued {
                    actions.retain(|action| !matches!(action, Action::Store(crate::store::StoreOp::PutRun(record)) if record.run_id == run));
                    self.runs.put(&run, actions);
                } else {
                    self.runs.progress_changed(&run, false, actions);
                }
            }
        }
    }

    pub(crate) fn progress_run_for(
        &self,
        thread: &str,
        turn: &str,
        replay: bool,
    ) -> Option<ProgressTarget> {
        if let Some(run_id) = self.diagnostic_run(thread, turn, replay) {
            return Some(ProgressTarget::Own { run_id });
        }
        self.child_activity_run(thread, turn, false)
            .map(ProgressTarget::Child)
    }

    pub(crate) fn dispatch_run_progress(
        &mut self,
        thread: &str,
        turn: &str,
        method: &str,
        params: &Option<Value>,
        replay: bool,
        actions: &mut Vec<Action>,
    ) {
        if !PROGRESS_METHODS.contains(&method) {
            return;
        }
        if let Some(target) = self.progress_run_for(thread, turn, replay) {
            let before = self
                .runs
                .runs
                .get(target.run_id())
                .and_then(|r| r.last_progress_at);
            self.stall_progress(
                target.run_id(),
                thread,
                turn,
                method,
                params,
                replay,
                actions,
            );
            if self
                .runs
                .runs
                .get(target.run_id())
                .and_then(|r| r.last_progress_at)
                != before
            {
                self.mark_run_diagnostic(target.run_id(), DiagnosticField::LastProgressAt, None);
            }
        }
    }

    /// 子线程先于 review 未归属缓冲分流，已处理的事件不再进入回放队列。
    pub(crate) fn dispatch_child_diagnostic(
        &mut self,
        thread: &str,
        method: &str,
        params: &Option<Value>,
        replay: bool,
        actions: &mut Vec<Action>,
    ) -> bool {
        if self.claims.contains_key(thread) || replay {
            return false;
        }
        let completion = method == "turn/completed";
        let turn = params
            .as_ref()
            .and_then(|p| {
                if matches!(method, "turn/started" | "turn/completed") {
                    p.get("turn").and_then(|t| t.get("id"))
                } else {
                    p.get("turnId")
                }
            })
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty());
        let Some(turn) = turn else {
            return false;
        };
        if completion
            && self
                .records
                .get(thread)
                .is_some_and(|r| r.ended_turns.iter().any(|id| id == turn))
        {
            return false;
        }
        let Some(child) = self.child_activity_run(thread, turn, completion) else {
            return false;
        };
        #[cfg(test)]
        self.runs
            .child_event_log
            .push((thread.into(), method.into()));
        if PROGRESS_METHODS.contains(&method) {
            let before = self
                .runs
                .runs
                .get(&child.run_id)
                .and_then(|r| r.last_progress_at);
            self.stall_progress(&child.run_id, thread, turn, method, params, false, actions);
            if self
                .runs
                .runs
                .get(&child.run_id)
                .and_then(|r| r.last_progress_at)
                != before
            {
                self.mark_run_diagnostic(&child.run_id, DiagnosticField::LastProgressAt, None);
            }
        }
        let error = params
            .as_ref()
            .and_then(|p| match method {
                "error" => p
                    .get("willRetry")
                    .and_then(Value::as_bool)
                    .zip(p.get("error"))
                    .map(|(retry, error)| {
                        (
                            error,
                            Some(retry),
                            super::upstream_error::UpstreamErrorSource::ErrorNotification,
                        )
                    }),
                "turn/completed" => p
                    .get("turn")
                    .filter(|t| t.get("status").and_then(Value::as_str) == Some("failed"))
                    .and_then(|t| t.get("error"))
                    .map(|error| {
                        (
                            error,
                            Some(false),
                            super::upstream_error::UpstreamErrorSource::TurnCompleted,
                        )
                    }),
                _ => None,
            })
            .filter(|(error, _, _)| error.is_object())
            .map(|(error, retry, source)| {
                super::upstream_error::UpstreamError::from_error(
                    thread,
                    Some(turn),
                    source,
                    error,
                    retry,
                    self.clock.now_ms,
                )
            });
        if let Some(error) = error {
            self.child_diagnostic_event(&child, thread, turn, error.clone(), completion, actions);
            self.upstream_access_check(&child.run_id, &error, actions);
        }
        true
    }

    pub(crate) fn pending_run_for(&self, thread: &str, params: &Option<Value>) -> Option<String> {
        self.claims
            .get(thread)
            .and_then(|c| c.run_id.clone())
            .or_else(|| {
                let turn = params.as_ref()?.get("turnId")?.as_str()?;
                self.child_activity_run(thread, turn, false)
                    .map(|c| c.run_id)
            })
    }
}
