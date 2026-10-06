//! 结果补读的有界状态机：每次尝试只消费一次，队列和重试共用 256 个等待名额。
use super::*;
use crate::claim::{ClaimToken, Operation, RequestContext};
use crate::results::{full_items, ItemOutput, Snapshot};
use state::{schedule, Phase};

const PAGE_SIZE: usize = 64;
// 限制异常服务器返回无限游标链；每次尝试最多读取 262144 个条目。
const MAX_PAGES: usize = 4096;
const MAX_CURSOR_BYTES: usize = 4096;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Pages {
    legacy: bool,
    cursor: Option<String>,
    cursors: std::collections::HashSet<String>,
    count: usize,
    output: ItemOutput,
}

impl Pages {
    /// 完整遍历的末页才产生证据；中途失败不能把已读前缀冒充完整输出。
    fn accept(&mut self, value: &Value, turn: &str) -> Option<bool> {
        let data = value.get("data")?.as_array()?;
        if data.len() > PAGE_SIZE || self.count >= MAX_PAGES {
            return None;
        }
        let next = match value.get("nextCursor")? {
            Value::Null => None,
            Value::String(cursor)
                if !cursor.is_empty()
                    && cursor.len() <= MAX_CURSOR_BYTES
                    && !data.is_empty()
                    && self.count + 1 < MAX_PAGES
                    && self.cursors.insert(cursor.clone()) =>
            {
                Some(cursor.clone())
            }
            _ => return None,
        };
        for entry in data {
            if entry.get("turnId")?.as_str()? != turn {
                return None;
            }
            let item = entry.get("item")?;
            item.as_object()?;
            self.output.extend(std::slice::from_ref(item));
        }
        self.count += 1;
        self.cursor = next;
        Some(self.cursor.is_none())
    }
}

fn unsupported(error: &Value) -> bool {
    if error.get("code").and_then(Value::as_i64) == Some(-32601) {
        return true;
    }
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    // 有些兼容实现只回固定错误短语，不附方法名；不把存储失败等一般错误当作不支持。
    if matches!(
        message.trim(),
        "not implemented"
            | "method not implemented"
            | "unimplemented"
            | "method not found"
            | "unknown method"
            | "未实现"
            | "尚未实现"
            | "方法未实现"
    ) {
        return true;
    }
    message.contains("thread/items/list")
        && [
            "not supported",
            "not implemented",
            "unimplemented",
            "unknown method",
            "method not found",
        ]
        .iter()
        .any(|marker| message.contains(marker))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReadPhase {
    Queued,
    InFlight,
    RetryScheduled,
    // 轮次已截掉，但 RPC 还没回来：不存快照，保留名额直到回包或关闭封口。
    Done,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PendingRead {
    pub read: ResultRead,
    pub snapshot: Snapshot,
    pub phase: ReadPhase,
    pub once: bool,
    /// 已把本轮的条目证据交给空转刹车（每轮只交一次，见 `Runs.guard_evidence`）。
    pub guard_sent: bool,
    pages: Pages,
}

#[allow(dead_code)]
impl Runs {
    fn in_flight(&self) -> usize {
        self.reads
            .values()
            .filter(|r| matches!(r.phase, ReadPhase::InFlight | ReadPhase::Done))
            .count()
    }

    fn waiting(&self) -> usize {
        self.reads
            .values()
            .filter(|r| matches!(r.phase, ReadPhase::Queued | ReadPhase::RetryScheduled))
            .count()
    }

    pub(super) fn queue_read(
        &mut self,
        mut read: ResultRead,
        snapshot: Snapshot,
        once: bool,
        guard_sent: bool,
        actions: &mut Vec<Action>,
    ) {
        if self.phase != Phase::Open {
            return;
        }
        self.pump_reads(actions);
        if self.in_flight() >= MAX_RESULT_READS && self.waiting() >= MAX_RESULT_QUEUE {
            self.fill_output(&read, Ready::Unavailable, snapshot.output());
            if !guard_sent {
                self.guard_evidence.push((read.run_id, read.turn_id, None));
            }
            actions.push(Action::Warn(
                "结果补读等待队列已满，本轮使用降级快照".into(),
            ));
            return;
        }
        read.generation = self.generation();
        let generation = read.generation;
        self.reads.insert(
            generation,
            PendingRead {
                read,
                snapshot,
                phase: ReadPhase::Queued,
                once,
                guard_sent,
                pages: Pages::default(),
            },
        );
        self.read_queue.push_back(generation);
        self.pump_reads(actions);
    }

    pub(super) fn pump_reads(&mut self, actions: &mut Vec<Action>) {
        if self.phase != Phase::Open {
            return;
        }
        while self.in_flight() < MAX_RESULT_READS {
            let Some(generation) = self.read_queue.pop_front() else {
                break;
            };
            let Some(pending) = self.reads.get_mut(&generation) else {
                continue;
            };
            if pending.phase != ReadPhase::Queued {
                continue;
            }
            pending.phase = ReadPhase::InFlight;
            Self::request_page(pending, actions);
        }
    }

    fn request_page(pending: &PendingRead, actions: &mut Vec<Action>) {
        let read = pending.read.clone();
        let (method, params) = if pending.pages.legacy {
            (
                "thread/read",
                serde_json::json!({"threadId":read.thread_id,"includeTurns":true}),
            )
        } else {
            (
                "thread/items/list",
                serde_json::json!({"threadId":read.thread_id,"turnId":read.turn_id,
                "cursor":pending.pages.cursor,"limit":PAGE_SIZE,"sortDirection":"asc"}),
            )
        };
        actions.push(Action::Request(RequestContext {
            resume_retried: false,
            stream_id: None,
            review_id: None,
            client: None,
            token: Some(ClaimToken {
                thread_id: read.thread_id.clone(),
                claim_id: 0,
            }),
            method: method.into(),
            params: Some(params),
            operation: Operation::ResultRead(read),
        }));
    }

    fn next_page(&mut self, mut pending: PendingRead, actions: &mut Vec<Action>) {
        // 同一尝试的各页也换代，重复或迟到的上一页不能被当作当前页消费。
        pending.read.generation = self.generation();
        Self::request_page(&pending, actions);
        self.reads.insert(pending.read.generation, pending);
    }

    fn fill_output(&mut self, read: &ResultRead, ready: Ready, output: (String, OutputSource)) {
        if let Some(turn) = self
            .runs
            .get_mut(&read.run_id)
            .and_then(|run| run.turns.iter_mut().find(|t| t.turn_id == read.turn_id))
        {
            if turn.ready == Ready::Pending {
                turn.ready = ready;
                turn.output = output.0;
                turn.output_source = output.1;
                self.notice_turn(&read.run_id, read.turn_index as u64);
            }
        }
    }

    pub(super) fn read_reply(
        &mut self,
        read: ResultRead,
        result: Result<Value, Value>,
        now: u64,
        actions: &mut Vec<Action>,
    ) {
        if self.phase == Phase::Finalized {
            return;
        }
        let Some(pending) = self.reads.get(&read.generation) else {
            actions.push(Action::Debug("忽略已作废的结果补读回包".into()));
            return;
        };
        if pending.read != read || !matches!(pending.phase, ReadPhase::InFlight | ReadPhase::Done) {
            actions.push(Action::Debug(
                "忽略代号、尝试次数或状态不匹配的结果补读回包".into(),
            ));
            return;
        }
        let mut pending = self.reads.remove(&read.generation).expect("补读登记存在");
        if pending.phase == ReadPhase::Done {
            if !pending.guard_sent {
                self.guard_evidence
                    .push((read.run_id.clone(), read.turn_id.clone(), None));
            }
            self.pump_reads(actions);
            return;
        }
        if !pending.pages.legacy && result.as_ref().is_err_and(unsupported) {
            if !self.paging_fallback_logged {
                self.paging_fallback_logged = true;
                actions.push(Action::Warn(
                    "上游不支持条目分页，结果补读回退全历史读取；仍可能超过传输上限".into(),
                ));
            }
            pending.pages = Pages {
                legacy: true,
                ..Pages::default()
            };
            if self.phase == Phase::Open {
                self.next_page(pending, actions);
                return;
            }
        }
        let complete = result.as_ref().ok().and_then(|value| {
            if !pending.pages.legacy {
                return pending.pages.accept(value, &read.turn_id);
            }
            let thread = value.get("thread")?;
            // 缺失或不同的线程身份不是本轮的证据。
            if thread.get("id")?.as_str()? != read.thread_id {
                return None;
            }
            let turn = thread
                .get("turns")?
                .as_array()?
                .iter()
                .find(|t| t.get("id").and_then(Value::as_str) == Some(&read.turn_id))?;
            // 读回的轮次若仍是摘要或未加载，同样不是完整证据，按失败重试或降级。
            pending.pages.output.extend(full_items(turn)?);
            Some(true)
        });
        if complete == Some(false) && self.phase == Phase::Open {
            self.next_page(pending, actions);
            return;
        }
        // 完整条目一到就交给空转刹车，即使没有可用输出、还要重试。
        if complete == Some(true) && !pending.guard_sent {
            pending.guard_sent = true;
            self.guard_evidence.push((
                read.run_id.clone(),
                read.turn_id.clone(),
                Some(pending.pages.output.activity),
            ));
        }
        let output = complete
            .filter(|done| *done)
            .and_then(|_| pending.pages.output.finish());
        if let Some(output) = output {
            self.fill_output(&read, Ready::Ready, output);
            self.updated.insert(read.run_id.clone(), now);
            self.put(&read.run_id, actions);
        } else if self.phase == Phase::Open
            && !pending.once
            && usize::from(read.attempt) < RESULT_RETRY_MS.len()
            && self.waiting() < MAX_RESULT_QUEUE
        {
            let delay_ms = RESULT_RETRY_MS[usize::from(read.attempt)];
            pending.read.attempt += 1;
            pending.pages = Pages {
                legacy: pending.pages.legacy,
                ..Pages::default()
            };
            pending.phase = ReadPhase::RetryScheduled;
            schedule(
                &read.thread_id,
                RunTimer::ResultRetry(pending.read.clone()),
                delay_ms,
                actions,
            );
            self.reads.insert(read.generation, pending);
        } else {
            if self.phase == Phase::Open
                && !pending.once
                && usize::from(read.attempt) < RESULT_RETRY_MS.len()
            {
                actions.push(Action::Warn(
                    "结果补读重试队列已满，本轮使用降级快照".into(),
                ));
            }
            self.fill_output(&read, Ready::Unavailable, pending.snapshot.output());
            actions.push(Action::Warn(
                "结果补读失败或超过分页/传输上限，本轮已标记 unavailable 并使用降级快照".into(),
            ));
            if !pending.guard_sent {
                self.guard_evidence
                    .push((read.run_id.clone(), read.turn_id.clone(), None));
            }
            self.updated.insert(read.run_id.clone(), now);
            self.put(&read.run_id, actions);
        }
        self.pump_reads(actions);
        self.evict();
    }

    pub(super) fn retry_read(&mut self, read: ResultRead, actions: &mut Vec<Action>) {
        if self.phase != Phase::Open {
            return;
        }
        let Some(pending) = self.reads.get_mut(&read.generation) else {
            return;
        };
        if pending.read != read || pending.phase != ReadPhase::RetryScheduled {
            return;
        }
        pending.phase = ReadPhase::Queued;
        self.read_queue.push_back(read.generation);
        self.pump_reads(actions);
    }

    /// 保留最近 100 轮；被截掉的在途请求不提前归还名额，避免实际并发越过 16。
    pub(super) fn trim_turns(&mut self, run: &str) {
        let Some(record) = self.runs.get_mut(run) else {
            return;
        };
        if record.turns.len() <= MAX_TURNS {
            return;
        }
        let count = record.turns.len() - MAX_TURNS;
        let removed: Vec<_> = record.turns.drain(..count).map(|t| t.turn_id).collect();
        let mut dropped = Vec::new();
        self.reads.retain(|_, pending| {
            if pending.read.run_id != run || !removed.contains(&pending.read.turn_id) {
                return true;
            }
            if matches!(pending.phase, ReadPhase::InFlight | ReadPhase::Done) {
                pending.phase = ReadPhase::Done;
                pending.snapshot = Snapshot::default();
                pending.pages = Pages::default();
                true
            } else {
                if !pending.guard_sent {
                    dropped.push(pending.read.turn_id.clone());
                }
                false
            }
        });
        for turn in dropped {
            self.guard_evidence.push((run.to_owned(), turn, None));
        }
        self.read_queue
            .retain(|generation| self.reads.contains_key(generation));
    }

    pub(super) fn finalize_pending(&mut self, actions: &mut Vec<Action>) {
        let reads = std::mem::take(&mut self.reads);
        self.read_queue.clear();
        // 不依赖登记完整性；恢复数据中的每个 pending 都必须有终点。
        let mut changed = Vec::new();
        let mut notices = Vec::new();
        for record in self.runs.values_mut() {
            let mut dirty = false;
            let base = record.turns_total.saturating_sub(record.turns.len() as u64);
            for (i, turn) in record.turns.iter_mut().enumerate() {
                if turn.ready != Ready::Pending {
                    continue;
                }
                let output = reads
                    .values()
                    .find(|read| {
                        read.read.run_id == record.run_id && read.read.turn_id == turn.turn_id
                    })
                    .map(|read| read.snapshot.output())
                    .unwrap_or_else(|| (String::new(), OutputSource::None));
                turn.ready = Ready::Unavailable;
                turn.output = output.0;
                turn.output_source = output.1;
                notices.push((record.run_id.clone(), base + i as u64));
                dirty = true;
            }
            if dirty {
                changed.push(record.run_id.clone());
            }
        }
        for (run, index) in notices {
            self.notice_turn(&run, index);
        }
        for run in changed {
            self.put(&run, actions);
        }
        self.generation();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn request(actions: &[Action]) -> RequestContext {
        actions
            .iter()
            .find_map(|a| match a {
                Action::Request(c) if matches!(c.operation, Operation::ResultRead(_)) => {
                    Some(c.clone())
                }
                _ => None,
            })
            .expect("应有补读请求")
            .clone()
    }
    fn answer(m: &mut Machine, c: &RequestContext, value: Result<Value, Value>) -> Vec<Action> {
        let Operation::ResultRead(read) = &c.operation else {
            panic!("应为补读")
        };
        let mut actions = Vec::new();
        m.run_result_reply(read.clone(), value, &mut actions);
        actions
    }
    fn begin() -> (Machine, RequestContext) {
        let mut m = Machine::new(false);
        let mut actions = Vec::new();
        m.run_opened(
            RunOpen {
                thread_id: "线程".into(),
                run_id: "运行".into(),
                conn: None,
                kind: ReqKind::TurnStart,
                joined: false,
                input: String::new(),
                continued_from: None,
            },
            &mut actions,
        );
        actions.clear();
        m.run_turn_completed(
            "线程",
            Some("运行"),
            &json!({"id":"轮","status":"failed","error":{"message":"通知错误"},
            "itemsView":"summary","items":[msg("降级摘要")]}),
            &mut actions,
        );
        (m, request(&actions))
    }
    fn msg(text: &str) -> Value {
        json!({"type":"agentMessage","phase":"final_answer","text":text})
    }
    fn page(items: Vec<Value>, cursor: Option<&str>) -> Result<Value, Value> {
        Ok(
            json!({"data":items.into_iter().map(|item|json!({"turnId":"轮","item":item,"startedAtMs":null,"completedAtMs":null})).collect::<Vec<_>>(),
            "nextCursor":cursor,"backwardsCursor":"反向"}),
        )
    }
    fn retry(m: &mut Machine, actions: &[Action], delay: u64) -> RequestContext {
        let (timer, ms) = actions
            .iter()
            .find_map(|a| match a {
                Action::Schedule {
                    kind: crate::claim::TimerKind::Run(t),
                    delay_ms,
                    ..
                } => Some((t.clone(), *delay_ms)),
                _ => None,
            })
            .expect("应安排退避");
        assert_eq!(ms, delay);
        let mut actions = Vec::new();
        m.run_timer(timer, &mut actions);
        request(&actions)
    }

    #[test]
    fn 分页直到末页才发布完整输出与一次活动证据() {
        let (mut m, first) = begin();
        assert_eq!(first.method, "thread/items/list");
        assert_eq!(
            first.params,
            Some(
                json!({"threadId":"线程","turnId":"轮","cursor":null,"limit":64,"sortDirection":"asc"})
            )
        );
        let second = request(&answer(
            &mut m,
            &first,
            page(vec![msg("第一段")], Some("不透明游标")),
        ));
        assert!(m.runs.guard_evidence.is_empty());
        assert_eq!(m.runs.runs["运行"].turns[0].ready, Ready::Pending);
        assert_eq!(second.params.as_ref().unwrap()["cursor"], "不透明游标");
        answer(&mut m, &first, page(vec![msg("旧页不能重复")], None));
        assert!(m.runs.guard_evidence.is_empty());
        answer(
            &mut m,
            &second,
            page(
                vec![json!({"type":"commandExecution"}), msg("第二段")],
                None,
            ),
        );
        let turn = &m.runs.runs["运行"].turns[0];
        assert_eq!(turn.output, "第一段\n第二段");
        assert_eq!(turn.ready, Ready::Ready);
        assert_eq!(turn.status, TurnStatus::Failed);
        assert_eq!(turn.error, Some(json!({"message":"通知错误"})));
        assert_eq!(m.runs.guard_evidence.len(), 1);
        assert!(m.runs.guard_evidence[0].2.unwrap().action);
        answer(&mut m, &second, page(vec![msg("末页重放")], None));
        assert_eq!(m.runs.guard_evidence.len(), 1);
    }

    #[test]
    fn 中途失败丢弃前缀并从首页重试三次最后降级() {
        let (mut m, mut first) = begin();
        for delay in RESULT_RETRY_MS.into_iter().map(Some).chain([None]) {
            assert!(first.params.as_ref().unwrap()["cursor"].is_null());
            let second = request(&answer(
                &mut m,
                &first,
                page(vec![msg("不能作为完整结果")], Some("第二页")),
            ));
            let actions = answer(
                &mut m,
                &second,
                Err(json!({"code":-32000,"message":"磁盘暂不可用"})),
            );
            if let Some(delay) = delay {
                assert!(m.runs.guard_evidence.is_empty());
                first = retry(&mut m, &actions, delay);
            } else {
                assert!(!actions.iter().any(|a| matches!(a, Action::Request(_))));
            }
        }
        let turn = &m.runs.runs["运行"].turns[0];
        assert_eq!(
            (turn.ready, turn.output.as_str()),
            (Ready::Unavailable, "降级摘要")
        );
        assert_eq!(
            m.runs.guard_evidence,
            vec![("运行".into(), "轮".into(), None)]
        );
        assert!(!m.result_reads_pending());
    }

    #[test]
    fn 重试成功不重复累计前次已读文本() {
        let (mut m, first) = begin();
        let second = request(&answer(
            &mut m,
            &first,
            page(vec![msg("前次前缀")], Some("后续")),
        ));
        let actions = answer(&mut m, &second, Err(json!({"message":"失败"})));
        let first = retry(&mut m, &actions, 1000);
        answer(&mut m, &first, page(vec![msg("重读全文")], None));
        assert_eq!(m.runs.runs["运行"].turns[0].output, "重读全文");
    }

    #[test]
    fn 不支持才回退且摘要仍须重试告警仅一次() {
        for error in [
            json!({"code":-32601}),
            json!({"code":-32000,"message":"thread/items/list is not implemented"}),
            json!({"code":-32000,"message":"Not implemented"}),
            json!({"code":-32000,"message":"尚未实现"}),
        ] {
            for view in ["summary", "notLoaded", "未知", "full"] {
                let (mut m, first) = begin();
                let actions = answer(&mut m, &first, Err(error.clone()));
                assert_eq!(
                    actions
                        .iter()
                        .filter(|a| matches!(a, Action::Warn(_)))
                        .count(),
                    1
                );
                let legacy = request(&actions);
                assert_eq!(legacy.method, "thread/read");
                assert_eq!(
                    legacy.params,
                    Some(json!({"threadId":"线程","includeTurns":true}))
                );
                let actions = answer(
                    &mut m,
                    &legacy,
                    Ok(
                        json!({"thread":{"id":"线程","turns":[{"id":"别轮","items":[msg("不选")],"itemsView":"full"},
                    {"id":"轮","items":[msg("兼容结果")],"itemsView":view}]}}),
                    ),
                );
                if view == "full" {
                    assert_eq!(m.runs.runs["运行"].turns[0].output, "兼容结果");
                } else {
                    assert!(m.runs.guard_evidence.is_empty());
                    assert_eq!(retry(&mut m, &actions, 1000).method, "thread/read");
                }
                // 另一轮仍探测分页；旧式存储的不支持不能禁用新式线程的分页。
                let mut next = Vec::new();
                m.run_turn_completed(
                    "线程",
                    Some("运行"),
                    &json!({"id":"另一轮","status":"completed","items":[]}),
                    &mut next,
                );
                let next = request(&next);
                assert_eq!(next.method, "thread/items/list");
                let actions = answer(&mut m, &next, Err(error.clone()));
                assert_eq!(request(&actions).method, "thread/read");
                assert!(!actions.iter().any(|a| matches!(a, Action::Warn(_))));
            }
        }
    }

    #[test]
    fn 普通错误不冒充不支持分页() {
        for error in [
            json!({"code":-32600,"message":"invalid cursor"}),
            json!({"code":-32000,"message":"storage not supported"}),
            json!({"code":-32000,"message":"超时"}),
        ] {
            let (mut m, first) = begin();
            let actions = answer(&mut m, &first, Err(error));
            assert!(!actions.iter().any(|a| matches!(a, Action::Request(_))));
            assert_eq!(retry(&mut m, &actions, 1000).method, "thread/items/list");
        }
    }

    #[test]
    fn 错轮次畸形页重复游标与空续页不能当作完整证据() {
        let cases = [
            json!({}),
            json!({"data":[],"nextCursor":17}),
            json!({"data":[]}),
            json!({"data":[],"nextCursor":"新游标"}),
            json!({"data":[{"turnId":"别轮","item":msg("错误") }],"nextCursor":null}),
            json!({"data":[{"turnId":"轮","item":null}],"nextCursor":null}),
            page(vec![msg("循环")], Some("原游标")).unwrap(),
            page(vec![msg("过长")], Some(&"x".repeat(MAX_CURSOR_BYTES + 1))).unwrap(),
            page(vec![msg("超条数"); PAGE_SIZE + 1], None).unwrap(),
        ];
        for value in cases {
            let (mut m, first) = begin();
            let next = request(&answer(
                &mut m,
                &first,
                page(vec![msg("第一页")], Some("原游标")),
            ));
            let actions = answer(&mut m, &next, Ok(value));
            assert!(m.runs.guard_evidence.is_empty());
            assert_eq!(retry(&mut m, &actions, 1000).method, "thread/items/list");
        }
    }

    #[test]
    fn 长线程十万条目和单轮超过传输上限仍逐页生成有界输出() {
        let (mut m, mut c) = begin();
        let text = "中文".repeat(40);
        for i in 0..2000 {
            let mut items = vec![json!({"type":"commandExecution","aggregatedOutput":text}); 64];
            if i == 0 {
                items[0] = msg("开头");
            }
            if i == 1999 {
                items[63] = msg("结尾");
            }
            let cursor = format!("页-{i}");
            let actions = answer(
                &mut m,
                &c,
                page(items, (i < 1999).then_some(cursor.as_str())),
            );
            if i < 1999 {
                c = request(&actions);
            }
        }
        assert_eq!(m.runs.runs["运行"].turns[0].output, "开头\n结尾");
        assert_eq!(m.runs.guard_evidence.len(), 1);
        assert!(m.runs.guard_evidence[0].2.unwrap().action);
    }

    #[test]
    fn 页数达到有界上限会重试而非继续无穷读取() {
        let mut pages = Pages {
            count: MAX_PAGES - 1,
            ..Pages::default()
        };
        assert_eq!(
            pages.accept(&page(vec![msg("内容")], Some("还有")).unwrap(), "轮"),
            None
        );
        assert_eq!(
            pages.accept(&page(vec![msg("内容")], None).unwrap(), "轮"),
            Some(true)
        );
        assert_eq!(pages.accept(&page(vec![], None).unwrap(), "轮"), None);
    }

    #[test]
    fn 完整页输出格式错误时活动证据仍只交一次() {
        let (mut m, mut c) = begin();
        for delay in RESULT_RETRY_MS.into_iter().map(Some).chain([None]) {
            let actions = answer(
                &mut m,
                &c,
                page(
                    vec![
                        json!({"type":"commandExecution"}),
                        json!({"type":"agentMessage","text":5}),
                    ],
                    None,
                ),
            );
            assert_eq!(m.runs.guard_evidence.len(), 1);
            assert!(m.runs.guard_evidence[0].2.unwrap().action);
            if let Some(delay) = delay {
                c = retry(&mut m, &actions, delay);
            }
        }
        assert_eq!(m.runs.runs["运行"].turns[0].ready, Ready::Unavailable);
    }

    #[test]
    fn 关闭期间不发后续页而将已读前缀降级() {
        for value in [
            page(vec![msg("前缀")], Some("还有")).unwrap(),
            json!({"data":[],"nextCursor":null}),
        ] {
            let (mut m, c) = begin();
            m.shutdown_runs(&mut Vec::new());
            let actions = answer(&mut m, &c, Ok(value));
            assert!(!actions
                .iter()
                .any(|a| matches!(a, Action::Request(_) | Action::Schedule { .. })));
            assert_eq!(m.runs.runs["运行"].turns[0].ready, Ready::Unavailable);
            assert_eq!(m.runs.runs["运行"].turns[0].output, "降级摘要");
        }
    }

    #[test]
    fn 回退读拒绝错误线程轮次或畸形条目() {
        for value in [
            json!({"thread":{"id":"别线程","turns":[{"id":"轮","items":[msg("不选")]}]}}),
            json!({"thread":{"id":"线程","turns":[{"id":"别轮","items":[msg("不选")]}]}}),
            json!({"thread":{"id":"线程","turns":[{"id":"轮","items":null}]}}),
        ] {
            let (mut m, c) = begin();
            let fallback = request(&answer(&mut m, &c, Err(json!({"code":-32601}))));
            let actions = answer(&mut m, &fallback, Ok(value));
            assert!(m.runs.guard_evidence.is_empty());
            assert_eq!(retry(&mut m, &actions, 1000).method, "thread/read");
        }
    }

    #[test]
    fn 三个自动轮的完整分页证据驱动真实空转刹车() {
        use crate::claim::{step, ClientRequest, Event};
        fn apply(m: &mut Machine, event: Event) -> Vec<Action> {
            let (next, actions) = step(std::mem::take(m), event);
            *m = next;
            actions
        }
        fn notify(m: &mut Machine, method: &str, params: Value) -> Vec<Action> {
            apply(
                m,
                Event::Notification {
                    method: method.into(),
                    params: Some(params),
                },
            )
        }
        let mut m = Machine::new(false);
        m.record("线程").subscribed = true;
        let actions = apply(
            &mut m,
            Event::ClientRequest {
                request: ClientRequest {
                    conn: 1,
                    id: json!(1),
                    method: "turn/start".into(),
                    params: Some(
                        json!({"threadId":"线程","input":[{"type":"text","text":"要求"}]}),
                    ),
                },
                run_id: Some("运行".into()),
            },
        );
        let c = actions
            .into_iter()
            .find_map(|a| {
                if let Action::Request(c) = a {
                    Some(c)
                } else {
                    None
                }
            })
            .unwrap();
        apply(
            &mut m,
            Event::Reply {
                context: c,
                result: Ok(json!({"turn":{"id":"首轮"}})),
            },
        );
        notify(
            &mut m,
            "thread/goal/updated",
            json!({"threadId":"线程","goal":{"threadId":"线程","objective":"目标","status":"active","createdAt":1,"tokensUsed":0}}),
        );
        notify(
            &mut m,
            "turn/started",
            json!({"threadId":"线程","turn":{"id":"首轮"}}),
        );
        notify(
            &mut m,
            "turn/completed",
            json!({"threadId":"线程","turn":{"id":"首轮","status":"completed","itemsView":"full","items":[msg("首轮")]}}),
        );
        for i in 1..=3 {
            let turn = format!("续{i}");
            notify(
                &mut m,
                "turn/started",
                json!({"threadId":"线程","turn":{"id":turn}}),
            );
            let actions = notify(
                &mut m,
                "turn/completed",
                json!({"threadId":"线程","turn":{"id":turn,"status":"completed","itemsView":"summary","items":[msg("摘要")]}}),
            );
            let c = request(&actions);
            let page = |text: &str, cursor: Option<&str>| {
                Ok(json!({"data":[{"turnId":turn,"item":msg(text)}],"nextCursor":cursor}))
            };
            let actions = apply(
                &mut m,
                Event::Reply {
                    context: c.clone(),
                    result: page("前页", Some("后页")),
                },
            );
            let next = request(&actions);
            assert!(m.runs.runs["运行"].loop_guard.is_none(), "前页不应触发刹车");
            apply(
                &mut m,
                Event::Reply {
                    context: next.clone(),
                    result: page("末页", None),
                },
            );
            apply(
                &mut m,
                Event::Reply {
                    context: next,
                    result: page("重放", None),
                },
            );
            if i < 3 {
                assert!(
                    m.runs.runs["运行"].loop_guard.is_none(),
                    "重复回包不能重复计数"
                );
            }
        }
        let report = m.runs.runs["运行"]
            .loop_guard
            .as_ref()
            .expect("三个无动作自动轮应刹车");
        assert_eq!(report.kind, LoopGuardKind::NoAction);
        assert_eq!(report.turn_ids, vec!["续1", "续2", "续3"]);
        assert_eq!(
            m.runs.runs["运行"].interrupt_reason,
            Some(InterruptReason::GoalLoop)
        );
    }
}
