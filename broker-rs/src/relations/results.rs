//! 子结果队列与父结果账本分离，固定批次期限、四路并发和逐页代号。
use crate::claim::subagent_reconcile::evidence::{build_batch, prefix};
use crate::claim::{Action, Machine, Operation, RequestContext, TimerKey, TimerKind};
use crate::runs::OutputSource;
use crate::sa1b::*;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};

const MAX_ITEMS: usize = 256;
const MAX_PAGES: usize = 256;
const PAGE_SIZE: usize = 64;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Text {
    value: String,
    truncated: bool,
}
impl Text {
    fn append(&mut self, text: &str, cap: usize) {
        if text.is_empty() {
            return;
        }
        if !self.value.is_empty() {
            if self.value.len() < cap {
                self.value.push('\n');
            } else {
                self.truncated = true;
            }
        }
        let part = prefix(text, cap.saturating_sub(self.value.len()));
        self.truncated |= part.len() != text.len();
        self.value.push_str(part);
    }
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Output {
    review: Text,
    final_answer: Text,
    accumulated: Text,
}
impl Output {
    fn observe(&mut self, item: &Value, cap: usize) -> Option<()> {
        match item.get("type")?.as_str()? {
            "exitedReviewMode" => {
                self.review = Text::default();
                self.review.append(item.get("review")?.as_str()?, cap);
            }
            "agentMessage" => {
                let text = item.get("text")?.as_str()?;
                if item
                    .get("phase")
                    .is_some_and(|v| !v.is_null() && !v.is_string())
                {
                    return None;
                }
                self.accumulated.append(text, cap);
                if item.get("phase").and_then(Value::as_str) == Some("final_answer") {
                    self.final_answer.append(text, cap);
                }
            }
            _ => {}
        }
        Some(())
    }
    fn finish(self) -> (Text, OutputSource) {
        if !self.review.value.is_empty() {
            (self.review, OutputSource::Review)
        } else if !self.final_answer.value.is_empty() {
            (self.final_answer, OutputSource::FinalAnswer)
        } else if !self.accumulated.value.is_empty() {
            (self.accumulated, OutputSource::Accumulated)
        } else {
            (Text::default(), OutputSource::None)
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct Child {
    item: ReconcileItem,
    generation: u64,
    pages: usize,
    cursor: Option<String>,
    cursors: HashSet<String>,
    output: Output,
    done: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct Reading {
    token: ConvergeToken,
    epoch: u64,
    limits: ResultLimits,
    deadline: u64,
    total: usize,
    children: Vec<Child>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ChildReads {
    next_id: u64,
    next_page: u64,
    reads: BTreeMap<u64, Reading>,
}
impl Machine {
    pub fn child_results_deadline(
        &mut self,
        token: &ConvergeToken,
        id: BatchReadId,
        actions: &mut Vec<Action>,
    ) {
        if self
            .runs
            .child_results
            .reads
            .get(&id.0)
            .is_some_and(|r| r.token == *token)
        {
            self.drive_child_results(actions);
        }
    }
    pub fn read_child_results(
        &mut self,
        token: ConvergeToken,
        work_epoch: u64,
        mut limits: ResultLimits,
        actions: &mut Vec<Action>,
    ) -> BatchReadId {
        let Some(facts) = self.reconcile_facts(&token.run_id) else {
            return BatchReadId::DISABLED;
        };
        if !self.reconcile_token_current(&token)
            || facts.token != token
            || facts.work_epoch != work_epoch
            || work_epoch == 0
            || !settled_facts(&facts)
            || self
                .runs
                .child_results
                .reads
                .values()
                .any(|r| r.token.run_id == token.run_id)
            || self.runs.child_results.reads.len() >= 64
        {
            return BatchReadId::DISABLED;
        }
        limits.total_bytes = limits.total_bytes.min(65536);
        limits.activity_bytes = limits.activity_bytes.min(8192);
        limits.max_concurrent_reads = limits.max_concurrent_reads.clamp(1, 4);
        limits.read_timeout_ms = limits.read_timeout_ms.min(20_000);
        if build_batch(work_epoch, 0, [], &limits).is_none() {
            return BatchReadId::DISABLED;
        }
        let deadline = self
            .clock
            .mono_ms
            .saturating_add(limits.read_timeout_ms)
            .min(self.convergences.runs[&token.run_id].deadline_mono_ms);
        if deadline <= self.clock.mono_ms {
            return BatchReadId::DISABLED;
        }
        let mut seen = HashSet::new();
        let mut children = Vec::new();
        let mut total = 0;
        for fact in &facts.activities {
            if !seen.insert((&fact.key.thread_id, fact.key.activity_seq)) {
                continue;
            }
            total += 1;
            if children.len() == MAX_ITEMS {
                continue;
            }
            let reference = facts.result_refs.iter().find(|r| {
                r.key.thread_id == fact.key.thread_id && r.key.activity_seq == fact.key.activity_seq
            });
            let locator = reference
                .map(|r| r.locator.clone())
                .unwrap_or(ResultLocator {
                    thread_id: fact.key.thread_id.clone(),
                    turn_id: fact.key.turn_id.clone(),
                    result_version: 0,
                });
            let valid_locator = locator.thread_id == fact.key.thread_id
                && locator.turn_id.is_some()
                && fact
                    .key
                    .turn_id
                    .as_ref()
                    .is_none_or(|turn| Some(turn) == locator.turn_id.as_ref());
            let unavailable = !valid_locator || reference.is_none_or(|r| !r.available);
            let item = ReconcileItem {
                key: fact.key.clone(),
                parent_thread_id: fact.parent_thread_id.clone(),
                status: fact
                    .result_status
                    .unwrap_or(ActivityResultStatus::Unavailable),
                output: String::new(),
                output_source: OutputSource::None,
                locator,
                truncated: false,
                unavailable_reason: unavailable.then(|| "结果定位或输出不可用".into()),
            };
            if crate::claim::subagent_reconcile::evidence::json_bounded(
                &item,
                limits.activity_bytes.saturating_sub(1),
            )
            .is_none()
            {
                continue;
            }
            children.push(Child {
                item,
                generation: 0,
                pages: 0,
                cursor: None,
                cursors: HashSet::new(),
                output: Output::default(),
                done: unavailable,
            });
        }
        let reads = &mut self.runs.child_results;
        reads.next_id = reads.next_id.checked_add(1).expect("子结果读号已耗尽");
        let read_id = BatchReadId(reads.next_id);
        reads.reads.insert(
            read_id.0,
            Reading {
                token: token.clone(),
                epoch: work_epoch,
                limits,
                deadline,
                total,
                children,
            },
        );
        actions.push(Action::Schedule {
            token: token.claim.clone(),
            kind: TimerKind::ChildResultsDeadline { token, read_id },
            delay_ms: deadline - self.clock.mono_ms,
        });
        // 即使所有输出均不可用，也由下一次 drive 异步完成，禁止同步回调重入。
        self.pump_child_results(read_id, actions);
        read_id
    }
    fn pump_child_results(&mut self, id: BatchReadId, actions: &mut Vec<Action>) {
        let state = &mut self.runs.child_results;
        let Some(read) = state.reads.get_mut(&id.0) else {
            return;
        };
        let mut live = read.children.iter().filter(|c| c.generation != 0).count();
        for child in &mut read.children {
            if live >= read.limits.max_concurrent_reads {
                break;
            }
            if child.done || child.generation != 0 {
                continue;
            }
            state.next_page = state.next_page.checked_add(1).expect("子结果页代号已耗尽");
            child.generation = state.next_page;
            child.pages += 1;
            live += 1;
            actions.push(Action::Request(RequestContext { resume_retried: false, stream_id: None, review_id: None, client: None,
                token: Some(read.token.claim.clone()), method: "thread/items/list".into(),
                params: Some(json!({"threadId":child.item.locator.thread_id,"turnId":child.item.locator.turn_id,"cursor":child.cursor,"limit":PAGE_SIZE,"sortDirection":"asc"})),
                operation: Operation::ChildResultRead { token: read.token.clone(), read_id: id, key: child.item.key.clone(), page_generation: child.generation } }));
        }
    }
    pub fn child_result_reply(
        &mut self,
        context: &RequestContext,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        let Operation::ChildResultRead {
            token,
            read_id,
            key,
            page_generation,
        } = &context.operation
        else {
            return;
        };
        if !self.reconcile_token_current(token) {
            self.cancel_child_results(token, actions);
            return;
        }
        let Some(read) = self.runs.child_results.reads.get_mut(&read_id.0) else {
            return;
        };
        if read.token != *token || self.clock.mono_ms >= read.deadline {
            self.drive_child_results(actions);
            return;
        }
        let Some(child) = read.children.iter_mut().find(|c| {
            c.item.key.thread_id == key.thread_id && c.item.key.activity_seq == key.activity_seq
        }) else {
            return;
        };
        if child.done || child.generation == 0 || child.generation != *page_generation {
            return;
        }
        child.generation = 0;
        let page = result
            .ok()
            .and_then(|v| accept_page(child, &v, read.limits.activity_bytes));
        match page {
            Some(true) => {
                let (text, source) = std::mem::take(&mut child.output).finish();
                if source == OutputSource::None {
                    // 读完了但没有任何输出条目（含合法空末页）：如实披露缺失，活动终态保持不变。
                    unavailable(child, "活动没有输出");
                } else {
                    child.item.output = text.value;
                    child.item.truncated = text.truncated;
                    child.item.output_source = source;
                    child.done = true;
                }
            }
            Some(false) => {}
            None => unavailable(child, "分页失败或数据不完整"),
        }
        self.pump_child_results(*read_id, actions);
        self.drive_child_results(actions);
    }
    /// A 的 step 末尾与 ChildResultsDeadline 均调用；回调前移除，重复计时无效果。
    pub fn drive_child_results(&mut self, actions: &mut Vec<Action>) {
        let ids: Vec<_> = self.runs.child_results.reads.keys().copied().collect();
        for id in ids {
            let token = self.runs.child_results.reads[&id].token.clone();
            if !self.reconcile_token_current(&token) {
                self.cancel_child_results(&token, actions);
                continue;
            }
            let read = self
                .runs
                .child_results
                .reads
                .get_mut(&id)
                .expect("读取存在");
            if self.clock.mono_ms >= read.deadline {
                for child in &mut read.children {
                    if !child.done {
                        unavailable(child, "批次读取期限已到");
                    }
                }
            }
            if !read.children.iter().all(|c| c.done) {
                continue;
            }
            let read = self.runs.child_results.reads.remove(&id).expect("读取存在");
            actions.push(Action::Unschedule {
                key: TimerKey::ChildResultsDeadline(BatchReadId(id)),
            });
            if let Some(batch) = build_batch(
                read.epoch,
                read.total,
                read.children.into_iter().map(|c| c.item),
                &read.limits,
            ) {
                self.runs
                    .reconciliation
                    .completed_reads
                    .insert(id, (token.clone(), batch.clone()));
                self.child_results_done(token, BatchReadId(id), batch, actions);
            }
        }
    }
    pub fn cancel_child_results(&mut self, token: &ConvergeToken, actions: &mut Vec<Action>) {
        self.runs.child_results.reads.retain(|id, read| {
            if read.token != *token {
                return true;
            }
            actions.push(Action::Unschedule {
                key: TimerKey::ChildResultsDeadline(BatchReadId(*id)),
            });
            false
        });
    }
}
pub(crate) fn settled_facts(facts: &ConvergenceFacts) -> bool {
    facts.discovery.complete
        && facts.discovery_covered_epoch == Some(facts.work_epoch)
        && facts.pending_starts == 0
        && facts.pending_requests == 0
        && facts.unresolved_lookups == 0
        && facts.conflicts.is_empty()
        && facts.activities.iter().all(|a| {
            a.verified
                && a.activity == super::MemberActivity::Settled
                && a.active_goal == Some(false)
                && a.pending_requests == 0
                && a.result_status.is_some()
        })
}
fn unavailable(child: &mut Child, reason: &str) {
    child.done = true;
    child.generation = 0;
    child.output = Output::default();
    child.item.output.clear();
    child.item.output_source = OutputSource::None;
    child.item.unavailable_reason = Some(reason.into());
}
fn accept_page(child: &mut Child, value: &Value, cap: usize) -> Option<bool> {
    let data = value.get("data")?.as_array()?;
    if data.len() > PAGE_SIZE {
        return None;
    }
    let cursor = match value.get("nextCursor")? {
        Value::Null => None,
        Value::String(s)
            if !s.is_empty()
                && s.len() <= 4096
                && !data.is_empty()
                && child.pages < MAX_PAGES
                && child.cursors.insert(s.clone()) =>
        {
            Some(s.clone())
        }
        _ => return None,
    };
    for entry in data {
        if entry.get("turnId")?.as_str() != child.item.locator.turn_id.as_deref() {
            return None;
        }
        child.output.observe(entry.get("item")?, cap)?;
    }
    child.cursor = cursor;
    Some(child.cursor.is_none())
}
