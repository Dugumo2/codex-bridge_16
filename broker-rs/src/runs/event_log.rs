//! 静态树日志：只保存规范化摘要，不重新驱动轮次、诊断、预算或空转状态。
use super::{state::schedule, Phase, RunTimer, Runs};
use crate::claim::{Action, Machine};
use crate::relations::{BindingStatus, EvidenceOutcome, MemberChange, RelationKind};
use crate::store::StoreOp;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::VecDeque;

const PER_THREAD_BYTES: usize = 64 * 1024;
const GLOBAL_BYTES: usize = 1024 * 1024;
const PENDING_MS: u64 = 30_000;
/// 记录消费方向的轮数上限，腾位规则见 `make_room_for_delivery`。
const DELIVERY_TURNS: usize = 1024;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct EventLog {
    sequence: u64,
    last_timestamp: u64,
    pending: VecDeque<Pending>,
    bytes: usize,
    timer: Option<u64>,
    deliveries: VecDeque<TurnDeliveries>,
    /// 曾经挤掉过含非协作消息、且可能还会收到消息的轮：此后新登记的轮无法证明记录完整
    /// （可能正是被挤掉的那一轮），一律不算纯协作，直到 broker 重启。
    deliveries_lossy: bool,
}

/// v2 子代理某一轮消费过的代理间消息的方向汇总。配对用的消费记录只留证据期限，
/// 这里按轮单独留一份，供暂存到期时判断这一轮是不是只被协作消息唤醒。
/// 只要消费过一条非协作消息，`collaboration_only` 就永久为假，不随后来的记录翻转；
/// 腾位时也不会把这样的轮挤掉后又当成新轮重新登记成纯协作（见 `make_room_for_delivery`）。
#[derive(Debug, Clone, PartialEq, Eq)]
struct TurnDeliveries {
    thread: String,
    turn: String,
    collaboration_only: bool,
}

/// 协作消息：发件人与收件人同属一棵代理树，且发件人不是收件人的上级（平级、跨分支、下级向上）。
/// 与线程关系上的平级、向上拒绝（`relations/facts.rs` 的 `followup_rejection`）同一语义。
/// 上级（含跨层的祖先）发来的不算，它们可能是本运行的跟进；自己发给自己也不算。
fn collaboration_delivery(author: &str, recipient: &str) -> bool {
    let root = |path: &str| path.split('/').nth(1).map(str::to_owned);
    author != recipient
        && root(author).is_some()
        && root(author) == root(recipient)
        && !recipient.starts_with(&format!("{author}/"))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct Summary {
    event: String,
    text: String,
    truncated: bool,
    item: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct Pending {
    thread: String,
    turn: String,
    now: u64,
    expires: u64,
    summary: Summary,
    candidates: Vec<(String, String)>,
    bytes: usize,
}

fn clean(text: &str, max: usize) -> (String, bool) {
    (
        text.chars()
            .take(max)
            .map(|c| if c.is_control() { ' ' } else { c })
            .collect(),
        text.chars().count() > max,
    )
}

impl Runs {
    pub(crate) fn log(
        &mut self,
        run: &str,
        now: u64,
        event: &str,
        content: &str,
        actions: &mut Vec<Action>,
    ) {
        let Some(record) = self.runs.get(run) else {
            return;
        };
        let thread = record.thread_id.clone();
        self.log_owned(
            run, &thread, None, None, now, event, content, false, actions,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn log_owned(
        &mut self,
        run: &str,
        thread: &str,
        turn: Option<&str>,
        item: Option<&str>,
        now: u64,
        event: &str,
        content: &str,
        truncated: bool,
        actions: &mut Vec<Action>,
    ) {
        let Some(record) = self.runs.get(run) else {
            return;
        };
        let role = if record.thread_id == thread {
            "run"
        } else {
            "execution"
        };
        let parent = (role == "execution").then(|| record.thread_id.clone());
        self.log_source(
            run,
            thread,
            parent.as_deref(),
            role,
            turn,
            item,
            now,
            event,
            content,
            truncated,
            false,
            None,
            actions,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn log_source(
        &mut self,
        run: &str,
        thread: &str,
        parent: Option<&str>,
        role: &str,
        turn: Option<&str>,
        item: Option<&str>,
        now: u64,
        event: &str,
        content: &str,
        truncated: bool,
        late: bool,
        issue: Option<&str>,
        actions: &mut Vec<Action>,
    ) {
        if self.phase != Phase::Open {
            return;
        }
        let Some(record) = self.runs.get(run) else {
            return;
        };
        let (summary, capped) = clean(content, 240);
        self.event_log.sequence = self
            .event_log
            .sequence
            .checked_add(1)
            .expect("日志序号已耗尽");
        let short = self
            .index
            .threads
            .values()
            .find(|entry| entry.thread_id == thread)
            .map(|entry| entry.short_id.as_str());
        let mut payload = json!({
            "eventId": format!("{}:{}", self.epoch, self.event_log.sequence),
            "rootThreadId": record.thread_id, "threadId": thread,
            "parentThreadId": parent, "shortId": short, "sourceRole": role,
            "turnId": turn, "itemId": item, "summary": summary, "truncated": truncated || capped,
        });
        if late || now < self.event_log.last_timestamp {
            payload["late"] = json!(true);
        }
        if let Some(issue) = issue {
            payload["coverageIssue"] = json!(issue);
        }
        self.event_log.last_timestamp = self.event_log.last_timestamp.max(now);
        actions.push(Action::Store(StoreOp::AppendLog {
            short_id: record.short_id.clone(),
            lines: vec![format!(
                // 旧 writer 将正文控制字符替换为空格；实际 TAB 是不与旧正文碰撞的格式边界。
                "{} {run} {event} \t@cxb2 {payload}",
                super::utc_timestamp(now)
            )],
        }));
    }
}

fn summaries(
    thread: &str,
    turn: &str,
    method: &str,
    params: &Option<Value>,
    now: u64,
) -> Vec<Summary> {
    let Some(params) = params.as_ref() else {
        return vec![];
    };
    let mut out = Vec::new();
    match method {
        "turn/started" | "turn/completed" => {
            let text = if method == "turn/started" {
                turn
            } else {
                params["turn"]["status"].as_str().unwrap_or("unknown")
            };
            let (text, truncated) = clean(text, 240);
            out.push(Summary {
                event: if method == "turn/started" {
                    "轮次开始"
                } else {
                    "轮次完成"
                }
                .into(),
                text,
                truncated,
                item: None,
            });
        }
        "item/started" | "item/completed" => {
            if let Some((item, truncated)) = params
                .get("item")
                .and_then(crate::results::summarize_item_with_truncation)
            {
                out.push(Summary {
                    event: if method == "item/started" {
                        "条目开始"
                    } else {
                        "条目完成"
                    }
                    .into(),
                    text: item.summary,
                    truncated,
                    item: Some(item.item_id),
                });
            }
        }
        _ => {}
    }
    let error = match method {
        "error" => params.get("error").filter(|e| e.is_object()).map(|error| {
            (
                error,
                params["willRetry"].as_bool(),
                super::upstream_error::UpstreamErrorSource::ErrorNotification,
            )
        }),
        "turn/completed" if params["turn"]["status"] == "failed" => params["turn"]
            .get("error")
            .filter(|e| e.is_object())
            .map(|error| {
                (
                    error,
                    Some(false),
                    super::upstream_error::UpstreamErrorSource::TurnCompleted,
                )
            }),
        _ => None,
    };
    if let Some((error, retry, source)) = error {
        let error = super::upstream_error::UpstreamError::from_error(
            thread,
            Some(turn),
            source,
            error,
            retry,
            now,
        );
        let (text, truncated) = clean(
            &format!(
                "willRetry={}；{}",
                retry.map_or("unknown", |r| if r { "true" } else { "false" }),
                error.message
            ),
            240,
        );
        out.push(Summary {
            event: "子线程错误".into(),
            text,
            truncated: truncated || error.truncated,
            item: None,
        });
    }
    out
}

impl Machine {
    /// 本入口只由完成首次接纳之后调用；不依赖进展是否推进。
    pub(crate) fn log_bound_child_event(
        &mut self,
        run: &str,
        thread: &str,
        turn: &str,
        method: &str,
        params: &Option<Value>,
        actions: &mut Vec<Action>,
    ) {
        let Some(binding) = self
            .binding(thread, turn)
            .filter(|b| b.run_id == run && b.status == BindingStatus::Bound)
        else {
            return;
        };
        for summary in summaries(thread, turn, method, params, self.clock.now_ms) {
            self.write_child_summary(
                run,
                thread,
                &binding.parent_thread_id,
                turn,
                self.clock.now_ms,
                &summary,
                false,
                actions,
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn write_child_summary(
        &mut self,
        run: &str,
        thread: &str,
        parent: &str,
        turn: &str,
        now: u64,
        summary: &Summary,
        late: bool,
        actions: &mut Vec<Action>,
    ) {
        self.runs.log_source(
            run,
            thread,
            Some(parent),
            "subagent",
            Some(turn),
            summary.item.as_deref(),
            now,
            &summary.event,
            &summary.text,
            summary.truncated,
            late,
            None,
            actions,
        );
    }

    /// 只为关系层已知候选保留摘要；独立认领或明确冲突永远不暂存。
    pub(crate) fn buffer_child_event_log(
        &mut self,
        thread: &str,
        method: &str,
        params: &Option<Value>,
        replay: bool,
        actions: &mut Vec<Action>,
    ) {
        if replay
            || self.runs.phase != Phase::Open
            || self.claims.contains_key(thread)
            || !matches!(
                method,
                "turn/started" | "turn/completed" | "item/started" | "item/completed" | "error"
            )
        {
            return;
        }
        let Some(turn) = params.as_ref().and_then(|p| {
            p.get("turnId").and_then(Value::as_str).or_else(|| {
                p.get("turn")
                    .and_then(|t| t.get("id"))
                    .and_then(Value::as_str)
            })
        }) else {
            return;
        };
        if self
            .binding(thread, turn)
            .is_some_and(|b| b.status != BindingStatus::Unbound)
        {
            return;
        }
        if self
            .runs
            .active
            .keys()
            .filter_map(|run| self.runs.runs.get(run))
            .any(|r| r.thread_id == thread || r.review_thread_id.as_deref() == Some(thread))
        {
            return;
        }
        if method == "turn/completed"
            && self
                .records
                .get(thread)
                .is_some_and(|r| r.ended_turns.iter().any(|t| t == turn))
        {
            return;
        }
        let candidates = self.event_log_candidates(thread);
        if candidates.is_empty() {
            return;
        }
        self.expire_event_log(actions);
        for summary in summaries(thread, turn, method, params, self.clock.now_ms) {
            let mut pending = Pending {
                thread: thread.into(),
                turn: turn.into(),
                now: self.clock.now_ms,
                expires: self.clock.mono_ms.saturating_add(PENDING_MS),
                summary,
                candidates: candidates.clone(),
                bytes: 0,
            };
            pending.bytes = serde_json::to_vec(&pending).expect("摘要可序列化").len();
            let thread_bytes: usize = self
                .runs
                .event_log
                .pending
                .iter()
                .filter(|p| p.thread == thread)
                .map(|p| p.bytes)
                .sum();
            if pending.bytes + thread_bytes > PER_THREAD_BYTES
                || pending.bytes + self.runs.event_log.bytes > GLOBAL_BYTES
            {
                self.settle_pending_candidates(&mut pending);
                self.pending_gap(&pending, "归属确认前摘要暂存超限", actions);
                continue;
            }
            self.runs.event_log.bytes += pending.bytes;
            self.runs.event_log.pending.push_back(pending);
        }
        self.schedule_event_log(actions);
    }

    fn event_log_candidates(&self, thread: &str) -> Vec<(String, String)> {
        let mut candidates = std::collections::BTreeMap::new();
        // 祖先尚未绑定时，后代操作的 run_id 为空。沿已知血缘与候选操作继续上溯；
        // 保存的 parent 始终是事件线程的直接父线程，不能改成找到运行的祖先。
        // 访问集防止畸形环；这里只决定能否暂存，实际写入仍须逐轮确认绑定。
        let mut pending = vec![(thread.to_owned(), None::<String>)];
        let mut visited = std::collections::BTreeSet::new();
        while let Some((current, direct_parent)) = pending.pop() {
            if !visited.insert((current.clone(), direct_parent.clone())) {
                continue;
            }
            for (run, context) in &self.relations.contexts {
                if let Some(member) = context
                    .members
                    .get(&current)
                    .filter(|m| m.kind == RelationKind::Subagent && m.conflict.is_none())
                {
                    candidates.insert(
                        run.clone(),
                        direct_parent.as_ref().unwrap_or(&member.parent).clone(),
                    );
                }
                if context.identity.execution_thread_id == current {
                    if let Some(parent) = &direct_parent {
                        candidates.insert(run.clone(), parent.clone());
                    }
                }
            }
            // 独立认领是新的执行根，不借其旧血缘继续向另一个运行追溯。
            if self.claims.contains_key(&current) {
                continue;
            }
            for op in self.relations.operations.values().filter(|op| {
                op.receivers.contains(&current) && !op.rejected_receivers.contains_key(&current)
            }) {
                let parent = direct_parent
                    .as_ref()
                    .unwrap_or(&op.key.parent_thread_id)
                    .clone();
                if self.runs.runs.contains_key(&op.run_id) {
                    candidates.insert(op.run_id.clone(), parent);
                } else if op.run_id.is_empty() {
                    pending.push((op.key.parent_thread_id.clone(), Some(parent)));
                }
            }
            if let Some(edge) = self
                .thread_relation(&current)
                .filter(|e| e.kind == RelationKind::Subagent)
            {
                let parent = direct_parent.unwrap_or_else(|| edge.parent_thread_id.clone());
                pending.push((edge.parent_thread_id, Some(parent)));
            }
        }
        candidates
            .into_iter()
            .filter(|(run, _)| {
                self.runs.runs.get(run).is_some_and(|r| {
                    matches!(
                        r.state,
                        super::RunState::Running | super::RunState::Stopping
                    )
                })
            })
            .collect()
    }

    /// 关系证据确认后仅补写摘要；已经结束的轮次也不重放通知。
    pub(crate) fn flush_bound_event_log(
        &mut self,
        run: &str,
        thread: &str,
        turn: &str,
        parent: &str,
        actions: &mut Vec<Action>,
    ) {
        if !self.binding(thread, turn).is_some_and(|b| {
            b.run_id == run && b.status == BindingStatus::Bound && b.parent_thread_id == parent
        }) {
            return;
        }
        self.expire_event_log(actions);
        let pending = std::mem::take(&mut self.runs.event_log.pending);
        for event in pending {
            if event.thread == thread && event.turn == turn {
                self.runs.event_log.bytes -= event.bytes;
                self.write_child_summary(
                    run,
                    thread,
                    parent,
                    turn,
                    event.now,
                    &event.summary,
                    true,
                    actions,
                );
            } else {
                self.runs.event_log.pending.push_back(event);
            }
        }
        if self.runs.event_log.pending.is_empty() {
            self.cancel_event_log_timer(actions);
        }
    }

    fn pending_gap(&mut self, pending: &Pending, reason: &str, actions: &mut Vec<Action>) {
        for (run, parent) in &pending.candidates {
            self.runs.log_source(
                run,
                &pending.thread,
                Some(parent),
                "subagent",
                Some(&pending.turn),
                None,
                self.clock.now_ms,
                "记录缺口",
                reason,
                false,
                false,
                Some("beforeBindingUnknown"),
                actions,
            );
        }
    }

    /// v2 子代理消费代理间消息时记下这一轮的消费方向（见 `TurnDeliveries`）。
    pub(crate) fn note_event_log_delivery(
        &mut self,
        thread: &str,
        turn: &str,
        author: &str,
        recipient: &str,
    ) {
        let collaboration = collaboration_delivery(author, recipient);
        if let Some(known) = self
            .runs
            .event_log
            .deliveries
            .iter_mut()
            .find(|t| t.thread == thread && t.turn == turn)
        {
            known.collaboration_only &= collaboration;
            return;
        }
        if self.runs.event_log.deliveries.len() >= DELIVERY_TURNS {
            self.make_room_for_delivery();
        }
        let trusted = !self.runs.event_log.deliveries_lossy;
        self.runs.event_log.deliveries.push_back(TurnDeliveries {
            thread: thread.into(),
            turn: turn.into(),
            collaboration_only: collaboration && trusted,
        });
    }

    /// 表满时腾出一个位置：
    /// 1. 优先删最旧的纯协作轮：删掉后这一轮只会变成「没有记录」照常记缺口，或重新登记时仍如实反映；
    /// 2. 没有纯协作轮时才挤掉最旧的轮。它含非协作消息，之后可能又收到同一轮的消费记录
    ///    （轮已结束也一样：会话文件里的记录可能迟到读到），被当新轮登记成纯协作，
    ///    所以记下 `deliveries_lossy`，此后新登记的轮一律不算纯协作。
    fn make_room_for_delivery(&mut self) {
        let log = &mut self.runs.event_log;
        if let Some(index) = log.deliveries.iter().position(|t| t.collaboration_only) {
            log.deliveries.remove(index);
        } else {
            log.deliveries.pop_front();
            log.deliveries_lossy = true;
        }
    }

    /// 这一轮有消费记录，而且消费的全是协作消息。
    fn turn_collaboration_only(&self, thread: &str, turn: &str) -> bool {
        self.runs
            .event_log
            .deliveries
            .iter()
            .any(|t| t.thread == thread && t.turn == turn && t.collaboration_only)
    }

    /// 暂存到期或关闭时清掉已明确判定不归该运行管理的候选：它们的摘要本来就不该写进该运行，
    /// 正常丢弃，不是覆盖缺口。
    ///
    /// 「明确」要先满足：此刻没有任何还可能把它归给本运行的证据（`event_log_evidence_in_flight`），
    /// 也没有等到期限才转未证明的证据（`proof_expired`，只说明归属没证明出来，不算明确排除）；
    /// 只记事实的未证明（上游 completed 通知）、定案不是自己的、明确被拒绝的（平级、向上）都不算未决。
    /// 再满足以下之一：
    /// - 它是别的运行的历史子代理、本运行对它没有任何管理证据
    ///   （[`historical_subagent_for_run`](Machine::historical_subagent_for_run)），典型是上一运行留下的兄弟；
    /// - 这一轮消费的全是平级或下级发来的协作消息（`turn_collaboration_only`），典型是本运行的
    ///   兄弟子代理之间互发、被平级拒绝纳管后唤醒的轮次。证据落在这一轮自己的消费记录上，
    ///   不拿别的轮、别的时刻的拒绝推断。
    ///
    /// 兄弟的轮次还在跑时分类会因活动而暂缓收尾，但那是收敛要等它，不是归属未定。
    /// 其余候选（血缘或历史不明、证据过期、没有消费记录的 v1 平级消息）照常记缺口。
    fn settle_pending_candidates(&self, pending: &mut Pending) {
        let thread = pending.thread.clone();
        let turn = pending.turn.clone();
        pending
            .candidates
            .retain(|(run, _)| !self.event_log_disowned(run, &thread, &turn));
    }

    fn event_log_disowned(&self, run: &str, thread: &str, turn: &str) -> bool {
        if self.event_log_evidence_in_flight(run, thread) {
            return false;
        }
        let expired = self.relations.operations.values().any(|op| {
            op.outcome == EvidenceOutcome::Unproven
                && op.proof_expired
                && (op.run_id == run || op.run_id.is_empty())
                && op.receivers.contains(thread)
                && !op.rejected_receivers.contains_key(thread)
        });
        !expired
            && (self.historical_subagent_for_run(run, thread)
                || self.turn_collaboration_only(thread, turn))
    }

    /// 还在进行、可能把这个线程的轮次归给该运行的证据：等配对或等发起方绑定的跟进、
    /// 等会话文件的跟进、等首轮的派生。多数有自己的证据期限，到期转成结果或未证明；
    /// 等 v2 配对的不受证据期限约束（`evidence_expirable`），靠父轮结束、收敛总期限和运行结束收口。
    fn event_log_evidence_in_flight(&self, run: &str, thread: &str) -> bool {
        self.relations.operations.values().any(|op| {
            op.outcome == EvidenceOutcome::Pending
                && (op.run_id == run || op.run_id.is_empty())
                && op.receivers.contains(thread)
                && !op.rejected_receivers.contains_key(thread)
        }) || self
            .relations
            .aux
            .rollout
            .waits
            .values()
            .any(|w| w.child == thread)
            || self.relations.aux.spawn_waits.contains_key(thread)
    }

    /// 到期的暂存：候选运行还在进行、且它的归属证据还在进行时续期，不按 30 秒丢
    /// （运行结束即停，收敛总期限与字节上限兜底）；否则清掉明确不归的候选，其余记缺口。
    /// 续期的条目留在原位，补写时仍按到达顺序。
    fn expire_event_log(&mut self, actions: &mut Vec<Action>) {
        let pending = std::mem::take(&mut self.runs.event_log.pending);
        for mut event in pending {
            if event.expires <= self.clock.mono_ms {
                if event.candidates.iter().any(|(run, _)| {
                    self.runs.runs.get(run).is_some_and(|r| {
                        matches!(
                            r.state,
                            super::RunState::Running | super::RunState::Stopping
                        )
                    }) && self.event_log_evidence_in_flight(run, &event.thread)
                }) {
                    event.expires = self.clock.mono_ms.saturating_add(PENDING_MS);
                    self.runs.event_log.pending.push_back(event);
                    continue;
                }
                self.runs.event_log.bytes -= event.bytes;
                self.settle_pending_candidates(&mut event);
                self.pending_gap(&event, "归属确认前摘要暂存过期", actions);
            } else {
                self.runs.event_log.pending.push_back(event);
            }
        }
    }

    fn schedule_event_log(&mut self, actions: &mut Vec<Action>) {
        if self.runs.event_log.timer.is_some() {
            return;
        }
        if let Some(deadline) = self.runs.event_log.pending.iter().map(|p| p.expires).min() {
            let generation = self.runs.generation();
            self.runs.event_log.timer = Some(generation);
            schedule(
                "",
                RunTimer::EventLogSweep { generation },
                deadline.saturating_sub(self.clock.mono_ms),
                actions,
            );
        }
    }

    pub(crate) fn sweep_event_log(&mut self, generation: u64, actions: &mut Vec<Action>) {
        if self.runs.event_log.timer != Some(generation) {
            return;
        }
        self.runs.event_log.timer = None;
        self.expire_event_log(actions);
        self.schedule_event_log(actions);
    }

    pub(crate) fn clear_pending_event_log(&mut self, actions: &mut Vec<Action>) {
        for mut pending in std::mem::take(&mut self.runs.event_log.pending) {
            self.settle_pending_candidates(&mut pending);
            self.pending_gap(&pending, "broker 关闭时归属尚未确认", actions);
        }
        self.runs.event_log.bytes = 0;
        self.cancel_event_log_timer(actions);
    }

    fn cancel_event_log_timer(&mut self, actions: &mut Vec<Action>) {
        if self.runs.event_log.timer.take().is_some() {
            actions.push(Action::Unschedule {
                key: crate::claim::TimerKey::EventLogSweep,
            });
        }
    }

    pub(crate) fn log_member_change(
        &mut self,
        run: &str,
        thread: &str,
        _change: MemberChange,
        actions: &mut Vec<Action>,
    ) {
        let Some(context) = self.relations.contexts.get(run) else {
            return;
        };
        if let Some(member) = context
            .members
            .get(thread)
            .filter(|m| !m.bindings.is_empty())
        {
            self.runs.log_source(
                run,
                thread,
                Some(&member.parent),
                "subagent",
                None,
                None,
                self.clock.now_ms,
                "成员状态变化",
                &format!("{:?}", member.activity),
                false,
                false,
                None,
                actions,
            );
        }
    }

    /// 只在运行定案时保存发现缺口；分类失效和重新发现期间的状态不代表覆盖丢失。
    pub(crate) fn log_final_discovery(
        &mut self,
        run: &str,
        complete: bool,
        actions: &mut Vec<Action>,
    ) {
        if complete {
            return;
        }
        let Some(record) = self.runs.runs.get(run) else {
            return;
        };
        let root = record.thread_id.clone();
        self.runs.log_source(
            run,
            &root,
            None,
            "run",
            None,
            None,
            self.clock.now_ms,
            "记录缺口",
            "运行定案时子代理发现范围仍不完整",
            false,
            false,
            Some("discoveryIncomplete"),
            actions,
        );
    }
}

#[cfg(test)]
mod tests;
