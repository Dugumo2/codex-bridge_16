//! 当前 app-server 连接观察到的运行时关闭证据；不是操作系统进程退出证明。
use crate::claim::{Action, HeldNotification, Machine, TimerKey};
use crate::runs::RunState;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClosedRuntime {
    pub source: String,
    pub connection_epoch: u64,
    pub observed_at: u64,
}

impl Machine {
    pub(crate) fn closed_runtime(&self, thread: &str) -> Option<ClosedRuntime> {
        self.stops
            .closed_runtimes
            .get(thread)
            .filter(|p| p.connection_epoch == self.relations.conn_epoch)
            .cloned()
    }

    pub(crate) fn record_closed_runtime(&mut self, thread: &str, source: &str) {
        self.stops
            .closed_runtimes
            .retain(|_, p| p.connection_epoch == self.relations.conn_epoch);
        if self.stops.closed_runtimes.len() >= 1024 {
            if let Some(oldest) = self
                .stops
                .closed_runtimes
                .iter()
                .min_by_key(|(_, p)| p.observed_at)
                .map(|(t, _)| t.clone())
            {
                self.stops.closed_runtimes.remove(&oldest);
            }
        }
        if source == "thread/read:notLoaded"
            && self
                .closed_runtime(thread)
                .is_some_and(|p| p.source == "thread/closed")
        {
            return;
        }
        self.stops.closed_runtimes.insert(
            thread.into(),
            ClosedRuntime {
                source: source.into(),
                connection_epoch: self.relations.conn_epoch,
                observed_at: self.clock.now_ms,
            },
        );
    }

    pub(crate) fn forget_closed_runtime(&mut self, thread: &str) {
        self.stops.closed_runtimes.remove(thread);
    }

    pub(crate) fn runtime_notification(&mut self, event: &HeldNotification) {
        let Some(p) = &event.params else {
            return;
        };
        let Some(thread) = p
            .get("threadId")
            .or_else(|| p.get("thread").and_then(|t| t.get("id")))
            .and_then(Value::as_str)
        else {
            return;
        };
        if event.method == "thread/closed" {
            self.record_closed_runtime(thread, "thread/closed");
            self.status_activity_changed(thread);
        } else if matches!(event.method.as_str(), "thread/started" | "turn/started")
            || event.method == "item/started" && p["item"]["type"] == "commandExecution"
            || event.method == "thread/status/changed" && p["status"]["type"] != "notLoaded"
        {
            self.forget_closed_runtime(thread);
            if event.method != "turn/started" {
                self.status_activity_changed(thread);
            }
        }
    }

    pub(crate) fn historical_stop(&self, op: &super::StopOp) -> bool {
        op.claim_id.is_none()
            && op.target.is_none()
            && op.already_stopped
            && op
                .run_id
                .as_ref()
                .and_then(|r| self.runs.runs.get(r))
                .is_some_and(|r| r.state == RunState::Ended)
    }

    pub(crate) fn cancel_stop_work(
        &mut self,
        id: &str,
        op: &super::StopOp,
        actions: &mut Vec<Action>,
    ) {
        for kind in [super::StopTimer::Deadline, super::StopTimer::Verify]
            .into_iter()
            .chain(op.waiters.iter().map(|w| super::StopTimer::Waiter(w.id)))
        {
            actions.push(Action::Unschedule {
                key: TimerKey::Stop(id.into(), op.generation, kind),
            });
        }
        self.cancel_stop_reads(op.generation, actions);
    }

    /// 历史核验遇到卸载即收尾：有关闭证据可确认，否则未确认；两者都不留下停止屏障。
    pub(crate) fn finish_unloaded_history(
        &mut self,
        id: &str,
        op: &mut super::StopOp,
        actions: &mut Vec<Action>,
    ) -> bool {
        if !self.historical_stop(op) || op.foreign_claim || self.claims.contains_key(&op.thread) {
            return false;
        }
        let proof = self.closed_runtime(&op.thread);
        if !op.unloaded_read && op.background.report.state != "notLoaded" {
            return false;
        }
        if let Some(proof) = proof {
            op.background.confirm_closed(proof, self.clock.now_ms);
        } else {
            op.background.report.failure_reason = Some(format!(
                "{}；未观察到本连接代际的运行时关闭证据；历史核验已释放，不作周期重试",
                op.background
                    .report
                    .failure_reason
                    .as_deref()
                    .unwrap_or("线程未加载")
            ));
        }
        if op.clear && !op.clear_done {
            op.background.confirmed = false;
            op.background.report.failure_reason =
                Some("运行时未加载，目标清除尚未核验；历史核验已释放".into());
        }
        self.retire_history_stop(id, op, actions);
        true
    }
    pub(crate) fn retire_history_stop(
        &mut self,
        id: &str,
        op: &mut super::StopOp,
        actions: &mut Vec<Action>,
    ) {
        op.history_released = true;
        if let Some(run) = &op.run_id {
            self.runs.verifying_history.remove(run);
        }
        let outcome = if op.background.confirmed {
            "confirmed"
        } else {
            "unconfirmed"
        };
        self.publish_background(op, actions);
        self.cancel_stop_work(id, op, actions);
        self.answer_waiters(op, None, outcome, actions);
    }
}
