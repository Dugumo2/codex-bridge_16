//! 只核验 Codex 受管的后台命令；列表为空不证明操作系统没有残留进程。
use crate::claim::Machine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub const LIST: &str = "thread/backgroundTerminals/list";
pub const TERMINATE: &str = "thread/backgroundTerminals/terminate";

#[cfg(test)]
#[path = "background_tests.rs"]
mod tests;

fn capability_limited(error: &Value, method: &str) -> bool {
    if error["code"] == -32601 {
        return true;
    }
    if error["code"] != -32600 {
        return false;
    }
    let Some(message) = error["message"].as_str() else {
        return false;
    };
    message == format!("{method} requires experimentalApi capability")
        || message
            .strip_prefix(&format!(
                "Invalid request: unknown variant `{method}`, expected "
            ))
            .is_some_and(|expected| !expected.is_empty())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackgroundReport {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_basis: Option<super::unloaded::ClosedRuntime>,
    pub scope: String,
    pub state: String,
    pub discovered: usize,
    pub termination_results: BTreeMap<String, Option<bool>>,
    pub remaining: Vec<String>,
    pub last_verified_at: Option<u64>,
    pub failure_reason: Option<String>,
}
impl Default for BackgroundReport {
    fn default() -> Self {
        Self {
            verification_basis: None,
            scope: "Codex 受管的后台命令".into(),
            state: "pending".into(),
            discovered: 0,
            termination_results: BTreeMap::new(),
            remaining: vec![],
            last_verified_at: None,
            failure_reason: None,
        }
    }
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Background {
    pub report: BackgroundReport,
    pub epoch: u64,
    pub deadline: u64,
    pub busy: bool,
    pub verifying: bool,
    pub confirmed: bool,
    pub failed: bool,
    queue: VecDeque<String>,
    items: BTreeMap<String, String>,
    seen: BTreeSet<String>,
    attempted: BTreeSet<String>,
    cursor: Option<String>,
    cursors: BTreeSet<String>,
    page: Vec<Value>,
    paged: bool,
    terminating: Option<String>,
}
impl Background {
    pub fn new(deadline: u64) -> Self {
        Self {
            deadline,
            ..Self::default()
        }
    }
    pub fn invalidate(&mut self) {
        self.busy = false;
        self.verifying = false;
        self.confirmed = false;
        self.failed = false;
        self.epoch += 1;
        self.queue.clear();
        self.attempted.clear();
        self.page.clear();
        self.paged = false;
        self.cursor = None;
        self.cursors.clear();
        self.terminating = None;
        self.report.verification_basis = None;
        self.report.state = "pending".into();
        self.report.failure_reason = None;
    }
    pub fn fail(&mut self, state: &str, reason: String) {
        self.report.verification_basis = None;
        self.busy = false;
        self.verifying = false;
        self.confirmed = false;
        self.failed = true;
        self.report.state = state.into();
        self.report.failure_reason = Some(reason);
    }
    pub fn pending_item(&self) -> Option<&str> {
        self.queue
            .front()
            .and_then(|id| self.items.get(id))
            .map(String::as_str)
    }

    pub fn next(&mut self, thread: &str, now: u64) -> Option<(&'static str, Value)> {
        if self.busy || self.verifying || self.confirmed || self.failed {
            return None;
        }
        if now >= self.deadline {
            self.fail("unknown", "本轮清理总期限已到，尚未核验".into());
            return None;
        }
        self.busy = true;
        self.epoch += 1;
        if let Some(id) = self.queue.pop_front() {
            self.attempted.insert(id.clone());
            self.terminating = Some(id.clone());
            self.report.state = "terminating".into();
            Some((TERMINATE, json!({"threadId":thread,"processId":id})))
        } else {
            self.terminating = None;
            self.report.state = "listing".into();
            Some((LIST, json!({"threadId":thread,"cursor":self.cursor})))
        }
    }
    pub fn reply(
        &mut self,
        result: Result<Value, Value>,
        now: u64,
        authorized: impl Fn(&str) -> bool,
    ) {
        self.busy = false;
        let value = match result {
            Ok(v) => v,
            Err(e) => {
                // RPC 层已匹配请求 ID，调用方已校验轮次；在清除终止目标前确定本次方法。
                let method = if self.terminating.is_some() {
                    TERMINATE
                } else {
                    LIST
                };
                if let Some(id) = self.terminating.take() {
                    self.report.termination_results.insert(id, None);
                }
                let message = e.to_string();
                let lower = message.to_ascii_lowercase();
                let state = if capability_limited(&e, method) {
                    "capabilityLimited"
                } else if lower.contains("thread not found") {
                    "notLoaded"
                } else {
                    "unknown"
                };
                self.fail(state, message);
                return;
            }
        };
        if let Some(id) = self.terminating.take() {
            let result = value.get("terminated").and_then(Value::as_bool);
            self.report.termination_results.insert(id, result);
            if result.is_none() {
                self.fail("unknown", "terminate 回包缺少 terminated".into());
            }
            // false 可能是自然退出；必须重新从头列举，不把布尔值当作进程退出证据。
            return;
        }
        let Some(data) = value.get("data").and_then(Value::as_array) else {
            self.fail("unknown", "list 回包缺少 data".into());
            return;
        };
        self.page.extend(data.iter().cloned());
        self.cursor = value
            .get("nextCursor")
            .and_then(Value::as_str)
            .map(str::to_owned);
        if let Some(cursor) = &self.cursor {
            self.paged = true;
            if !self.cursors.insert(cursor.clone()) {
                self.fail("unknown", "list 分页游标重复".into());
            }
            return;
        }
        self.cursors.clear();
        let page = std::mem::take(&mut self.page);
        self.report.remaining.clear();
        for item in &page {
            let Some(id) = item.get("processId").and_then(Value::as_str) else {
                self.fail("unknown", "后台命令缺少 processId".into());
                return;
            };
            if let Some(item_id) = item.get("itemId").and_then(Value::as_str) {
                self.items.insert(id.into(), item_id.into());
            }
            self.seen.insert(id.into());
            self.report.remaining.push(id.into());
        }
        self.report.discovered = self.seen.len();
        self.report.last_verified_at = Some(now);
        let paged = std::mem::take(&mut self.paged);
        if page.is_empty() && paged {
            return;
        }
        if page.is_empty() {
            self.verifying = true;
            self.report.state = "verifying".into();
            self.report.failure_reason = None;
            return;
        }
        for item in &page {
            if !item
                .get("itemId")
                .and_then(Value::as_str)
                .is_some_and(&authorized)
            {
                self.fail(
                    "conflict",
                    "命令 itemId 无法归属到授权运行，未扩大清理范围".into(),
                );
                return;
            }
        }
        // 一轮每编号只尝试一次，仍在列表中的命令交下一核实周期，避免失败后忙循环。
        self.queue = self
            .report
            .remaining
            .iter()
            .filter(|id| !self.attempted.contains(*id))
            .cloned()
            .collect();
        if self.queue.is_empty() {
            self.fail("unconfirmed", "受管后台命令仍在列表中".into());
        }
    }
    pub fn confirm_closed(&mut self, proof: super::unloaded::ClosedRuntime, now: u64) {
        self.busy = false;
        self.verifying = false;
        self.failed = false;
        self.confirmed = true;
        self.report.state = "notLoaded".into();
        self.report.remaining.clear();
        self.report.failure_reason = None;
        self.report.last_verified_at = Some(now);
        self.report.verification_basis = Some(proof);
    }

    pub fn finish(&mut self) {
        self.verifying = false;
        self.confirmed = true;
        self.report.state = "confirmed".into();
    }
}

impl Machine {
    pub(crate) fn background_item_owned(
        &self,
        thread: &str,
        item: &str,
        runs: &BTreeSet<String>,
    ) -> bool {
        let Some(turn) = self.stops.command_turns.get(&(thread.into(), item.into())) else {
            return false;
        };
        if let Some(binding) = self.binding(thread, turn) {
            return runs.contains(&binding.run_id)
                && binding.status == crate::relations::BindingStatus::Bound;
        }
        self.stops
            .command_owners
            .get(&(thread.into(), item.into()))
            .is_some_and(|run| runs.contains(run))
            || self.claims.get(thread).is_some_and(|c| {
                c.run_id.as_ref().is_some_and(|r| runs.contains(r)) && c.owned_turns.contains(turn)
            })
            || runs.iter().any(|r| {
                self.runs.runs.get(r).is_some_and(|r| {
                    ((r.thread_id == thread || r.review_thread_id.as_deref() == Some(thread))
                        && r.turns.iter().any(|t| t.turn_id == *turn))
                        || r.children
                            .iter()
                            .any(|c| c.thread_id == thread && c.turn_ids.contains(turn))
                })
            })
    }
}
