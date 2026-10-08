//! 直接构造 Machine 调钩子，覆盖边界、幂等、关闭与有界性，不依赖运行时或磁盘。
#[test]
fn n4_迁移通知仅向父所有者和扩展观察者发送一次() {
    let mut m = machine();
    open(&mut m, "审查", "父");
    m.live_conns.extend([1, 2, 3, 4]);
    m.cxb_conns.extend([1, 2, 4]);
    m.records.entry("父".into()).or_default().watchers = vec![1, 2, 3];
    m.records.entry("子".into()).or_default().watchers = vec![4];
    let mut a = Vec::new();
    m.run_review_moved("审查", "子", Some(1), &mut a);
    let mut recipients: Vec<_> = a.iter().filter_map(|a|match a {
        Action::Send {conn,message} if message["method"]=="broker/run/moved" => {
            assert_eq!(message["params"], json!({"threadId":"父","runId":"审查","reviewThreadId":"子","reviewShortId":m.runs.runs["审查"].review_short_id}));
            Some(*conn)
        }, _=>None,
    }).collect();
    recipients.sort_unstable();
    assert_eq!(recipients, vec![1, 2]);
    assert_eq!(m.latest_run("子").unwrap().run_id, "审查");
    for child in ["子", "冲突子"] {
        let mut a = Vec::new();
        m.run_review_moved("审查", child, Some(1), &mut a);
        assert!(!a.iter().any(|a| matches!(a, Action::Send { .. })));
        assert_eq!(m.runs.runs["审查"].review_thread_id.as_deref(), Some("子"));
    }
}

#[test]
fn n4_内联与非开放阶段及非活动运行不发迁移通知() {
    for (phase, child, active) in [
        (Phase::Open, "父", true),
        (Phase::Draining, "子", true),
        (Phase::Finalized, "子", true),
        (Phase::Open, "子", false),
    ] {
        let mut m = machine();
        open(&mut m, "审查", "父");
        m.live_conns.insert(1);
        m.cxb_conns.insert(1);
        m.runs.phase = phase;
        if !active {
            m.runs.active.clear();
        }
        let mut a = Vec::new();
        m.run_review_moved("审查", child, Some(1), &mut a);
        assert!(
            !a.iter().any(|a| matches!(a, Action::Send { .. })),
            "阶段 {phase:?}"
        );
        assert!(m.runs.runs["审查"].review_thread_id.is_none());
        assert!(m.runs.runs["审查"].review_short_id.is_none());
    }
}
use super::*;
use crate::claim::{Clock, Operation, RequestContext, TimerKind};
use crate::results::{Snapshot, MAX_TEXT_BYTES};
use crate::store::{StoreOp, RETENTION_MS};
use serde_json::json;

#[test]
fn n7_每类日志带运行编号且内容清洗与空内容符合四段格式() {
    let mut m = machine();
    let run = "run-abc-123abc";
    open(&mut m, run, "P");
    for event in [
        "运行开始",
        "运行结束",
        "轮次开始",
        "轮次完成",
        "条目开始",
        "条目完成",
        "停止发起",
        "停止确认",
        "空转刹车",
        "空转刹车暂停目标",
        "启动失败",
        "交接撤销",
    ] {
        for content in [String::new(), "甲\n乙\t丙\r丁\0".repeat(100)] {
            let mut a = vec![];
            m.runs.log(run, 1000, event, &content, &mut a);
            let line = a
                .iter()
                .find_map(|a| {
                    if let Action::Store(StoreOp::AppendLog { lines, .. }) = a {
                        Some(&lines[0])
                    } else {
                        None
                    }
                })
                .unwrap();
            let parts: Vec<_> = line.splitn(4, ' ').collect();
            assert_eq!(&parts[..3], &["1970-01-01T00:00:01.000Z", run, event]);
            assert_eq!(parts[3].chars().count(), content.chars().count().min(240));
            assert!(!parts[3].chars().any(char::is_control));
            assert_eq!(
                parts[3],
                content
                    .chars()
                    .take(240)
                    .map(|c| if c.is_control() { ' ' } else { c })
                    .collect::<String>()
            );
        }
    }
}

fn machine() -> Machine {
    let mut m = Machine::new(false);
    m.clock = Clock {
        now_ms: 1_000,
        entropy: 7,
        mono_ms: 0,
    };
    m
}
fn open(m: &mut Machine, run: &str, thread: &str) -> Vec<Action> {
    let mut actions = Vec::new();
    m.run_opened(
        RunOpen {
            run_id: run.into(),
            thread_id: thread.into(),
            conn: Some(1),
            kind: ReqKind::TurnStart,
            joined: false,
            input: "原始要求".into(),
            continued_from: None,
        },
        &mut actions,
    );
    actions
}
fn message(text: &str) -> Value {
    json!({"id":text,"type":"agentMessage","phase":"final_answer","text":text})
}
fn complete(m: &mut Machine, run: &str, thread: &str, turn: &str, items: Value) -> Vec<Action> {
    let mut actions = Vec::new();
    m.run_turn_completed(
        thread,
        Some(run),
        &json!({"id":turn,"status":"completed","error":null,"items":items}),
        &mut actions,
    );
    actions
}
fn item(m: &mut Machine, run: &str, turn: &str, method: &str, value: Value) -> Vec<Action> {
    let mut actions = Vec::new();
    m.run_item(
        "线程",
        Some(run),
        method,
        &Some(json!({"turnId":turn,"item":value})),
        &mut actions,
    );
    actions
}
fn reads(actions: &[Action]) -> Vec<ResultRead> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Request(RequestContext {
                operation: Operation::ResultRead(read),
                ..
            }) => Some(read.clone()),
            _ => None,
        })
        .collect()
}
fn timers(actions: &[Action]) -> Vec<(RunTimer, u64)> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Schedule {
                kind: TimerKind::Run(timer),
                delay_ms,
                ..
            } => Some((timer.clone(), *delay_ms)),
            _ => None,
        })
        .collect()
}
fn puts(actions: &[Action]) -> usize {
    actions
        .iter()
        .filter(|a| matches!(a, Action::Store(StoreOp::PutRun(_))))
        .count()
}
fn index_puts(actions: &[Action]) -> usize {
    actions
        .iter()
        .filter(|a| matches!(a, Action::Store(StoreOp::PutThreads(_))))
        .count()
}
fn reply(m: &mut Machine, read: ResultRead, result: Result<Value, Value>) -> Vec<Action> {
    let mut actions = Vec::new();
    m.run_result_reply(read, result, &mut actions);
    actions
}
fn success(read: &ResultRead, text: &str) -> Result<Value, Value> {
    Ok(
        json!({"data":[{"turnId":read.turn_id,"item":message(text),"startedAtMs":null,"completedAtMs":null}],"nextCursor":null,"backwardsCursor":null}),
    )
}
fn last<'a>(m: &'a Machine, run: &str) -> &'a TurnResult {
    m.runs.runs[run].turns.last().unwrap()
}
fn check() -> StopCheck {
    StopCheck {
        ownership: true,
        turn_ended: false,
        goal_braked: true,
        settled: false,
        verify_done: false,
    }
}

#[test]
fn output_precedence_and_empty_output() {
    let intermediate = json!({"type":"agentMessage","text":"中间说明"});
    assert_eq!(
        output_from_items(std::slice::from_ref(&intermediate)),
        Some(("中间说明".into(), OutputSource::Accumulated))
    );
    assert_eq!(
        output_from_items(&[intermediate.clone(), message("答案一"), message("答案二")]),
        Some(("答案一\n答案二".into(), OutputSource::FinalAnswer))
    );
    assert_eq!(
        output_from_items(&[
            message("收尾"),
            json!({"type":"exitedReviewMode","review":"审查报告"})
        ]),
        Some(("审查报告".into(), OutputSource::Review))
    );
    assert_eq!(
        output_from_items(&[intermediate, json!({"type":"exitedReviewMode","review":""})]),
        Some(("中间说明".into(), OutputSource::Accumulated))
    );
    assert_eq!(
        output_from_items(&[json!({"type":"commandExecution","command":"构建"})]),
        Some((String::new(), OutputSource::None))
    );
    for invalid in [
        json!([]),
        json!([null]),
        json!([{}]),
        json!([{"type":"agentMessage"}]),
        json!([{"type":"exitedReviewMode","review":10}]),
        json!([{"type":"agentMessage","text":"内容","phase":false}]),
    ] {
        assert_eq!(output_from_items(invalid.as_array().unwrap()), None);
    }
}

#[test]
fn snapshot_keeps_last_text_and_review_priority() {
    let mut s = Snapshot::default();
    s.observe(&message("旧答案"));
    s.observe(&message("新答案"));
    s.observe(&json!({"type":"agentMessage","phase":"commentary","text":"说明"}));
    assert_eq!(s.output(), ("新答案".into(), OutputSource::FinalAnswer));
    s.observe(&json!({"type":"exitedReviewMode","review":"正式审查"}));
    s.observe(&json!({"type":"exitedReviewMode","review":null}));
    assert_eq!(s.output(), ("正式审查".into(), OutputSource::Review));
}

#[test]
fn unicode_text_limits_apply_to_input_output_snapshot_and_summary() {
    let text = "中😀".repeat(MAX_TEXT_BYTES / 4);
    let bounded = limit_text(&text);
    assert!(bounded.len() <= MAX_TEXT_BYTES);
    assert!(bounded.ends_with("（输出过长，已截断）"));
    let output = output_from_items(&[message(&text), message("后续")])
        .unwrap()
        .0;
    assert!(output.len() <= MAX_TEXT_BYTES);
    let mut snapshot = Snapshot::default();
    snapshot.observe(&message(&text));
    assert_eq!(snapshot.output().0, bounded);
    let summary =
        summarize_item(&json!({"id":"命令","type":"commandExecution","command":text})).unwrap();
    assert_eq!(summary.summary.chars().count(), 80);
    let mut m = machine();
    let mut actions = Vec::new();
    m.run_opened(
        RunOpen {
            thread_id: "线程".into(),
            run_id: "运行".into(),
            conn: None,
            kind: ReqKind::TurnStart,
            joined: false,
            input: text,
            continued_from: None,
        },
        &mut actions,
    );
    assert_eq!(m.runs.runs["运行"].input, bounded);
}

#[test]
fn open_consumes_only_own_meta_and_uses_injected_clock() {
    let mut m = machine();
    m.records
        .entry("线程".into())
        .or_default()
        .pending_meta
        .insert(1, json!({"source":"run"}));
    m.records
        .get_mut("线程")
        .unwrap()
        .pending_meta
        .insert(2, json!({"source":"review"}));
    let actions = open(&mut m, "运行", "线程");
    let r = &m.runs.runs["运行"];
    assert_eq!(r.started_at, 1000);
    assert_eq!(r.short_id, "00000007");
    assert_eq!(r.meta, Some(json!({"source":"run"})));
    assert_eq!(m.records["线程"].pending_meta.len(), 1);
    assert_eq!(puts(&actions), 1);
    assert_eq!(index_puts(&actions), 1);
    assert_eq!(m.latest_run("线程").unwrap().run_id, "运行");
    assert_eq!(m.run_snapshot("运行").unwrap()["turnsTotal"], 0);
    assert!(actions.iter().any(|a| matches!(a, Action::Store(StoreOp::AppendLog { lines, .. }) if lines[0].starts_with("1970-01-01T00:00:01.000Z 运行 运行开始"))));
    assert!(open(&mut m, "运行", "线程").is_empty());
}

#[test]
fn short_id_collision_and_reply_field_locations() {
    let mut m = machine();
    open(&mut m, "甲", "父线程");
    open(&mut m, "乙", "另一线程");
    assert_ne!(m.runs.runs["甲"].short_id, m.runs.runs["乙"].short_id);
    assert_eq!(m.runs.runs["乙"].short_id.len(), 8);
    let mut actions = Vec::new();
    m.run_learn_thread("提示参数", &json!({"model":"模型","cwd":"错误位置","thread":{"id":"真线程","cwd":"真实目录","preview":"预览"}}), &mut actions);
    let entry = m
        .runs
        .index
        .threads
        .values()
        .find(|e| e.thread_id == "真线程")
        .unwrap();
    assert_eq!(entry.cwd.as_deref(), Some("真实目录"));
    assert_eq!(entry.model.as_deref(), Some("模型"));
    m.run_learn_thread("真线程", &json!({"thread":{"id":"真线程"}}), &mut actions);
    let entry = m
        .runs
        .index
        .threads
        .values()
        .find(|e| e.thread_id == "真线程")
        .unwrap();
    assert_eq!(
        (
            entry.cwd.clone(),
            entry.model.clone(),
            entry.preview.clone()
        ),
        (None, None, None)
    );
}

#[test]
fn completed_items_ready_and_notification_status_is_authoritative() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    let mut actions = Vec::new();
    m.run_turn_completed(
        "线程",
        Some("运行"),
        &json!({"id":"轮","status":"failed","error":{"message":"通知错误"},"items":[]}),
        &mut actions,
    );
    let read = reads(&actions).remove(0);
    assert_eq!(read.turn_index, 0);
    assert_eq!(read.attempt, 0);
    assert_eq!(m.runs.active["运行"].snapshot, Snapshot::default());
    let reply_actions = reply(&mut m, read.clone(), success(&read, "补读内容"));
    assert_eq!(puts(&reply_actions), 1);
    assert_eq!(last(&m, "运行").ready, Ready::Ready);
    assert_eq!(last(&m, "运行").status, TurnStatus::Failed);
    assert_eq!(last(&m, "运行").error, Some(json!({"message":"通知错误"})));
    assert!(!m.result_reads_pending());
    let duplicate = reply(&mut m, read.clone(), success(&read, "不应覆盖"));
    assert_eq!(puts(&duplicate), 0);
    assert_eq!(last(&m, "运行").output, "补读内容");
    let ready = complete(&mut m, "运行", "线程", "轮二", json!([message("自带输出")]));
    assert!(reads(&ready).is_empty());
    assert_eq!(last(&m, "运行").ready, Ready::Ready);
}

fn complete_view(m: &mut Machine, turn: &str, view: Option<Value>, items: Value) -> Vec<Action> {
    let mut value =
        json!({"id":turn,"status":"failed","error":{"message":"通知错误"},"items":items});
    if let Some(view) = view {
        value["itemsView"] = view;
    }
    let mut actions = Vec::new();
    m.run_turn_completed("线程", Some("运行"), &value, &mut actions);
    actions
}
fn read_with(read: &ResultRead, view: Option<&str>, items: Value) -> Result<Value, Value> {
    if view == Some("full") || view.is_none() {
        return Ok(
            json!({"data":items.as_array().unwrap().iter().map(|item| json!({"turnId":read.turn_id,"item":item})).collect::<Vec<_>>(),"nextCursor":null,"backwardsCursor":null}),
        );
    }
    let mut turn = json!({"id":read.turn_id,"items":items,"status":"completed","error":null});
    if let Some(view) = view {
        turn["itemsView"] = json!(view);
    }
    Ok(json!({"thread":{"id":read.thread_id,"turns":[turn]}}))
}

#[test]
fn only_absent_or_full_items_view_is_complete() {
    // M9：上游 completed 的 items 只是摘要；只有不带 itemsView 或标 full 的才立即就绪。
    for (view, ready) in [
        (None, true),
        (Some(json!("full")), true),
        (Some(json!("summary")), false),
        (Some(json!("notLoaded")), false),
        (Some(json!("未来取值")), false),
        (Some(Value::Null), false),
    ] {
        let mut m = machine();
        open(&mut m, "运行", "线程");
        let actions = complete_view(&mut m, "轮", view.clone(), json!([message("最后一条")]));
        assert_eq!(reads(&actions).is_empty(), ready, "itemsView={view:?}");
        assert_eq!(
            last(&m, "运行").ready,
            if ready { Ready::Ready } else { Ready::Pending },
            "itemsView={view:?}"
        );
        assert_eq!(last(&m, "运行").output, if ready { "最后一条" } else { "" });
    }
}

#[test]
fn summary_completion_reads_full_turn_and_keeps_notification_status() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    let actions = complete_view(
        &mut m,
        "轮",
        Some(json!("summary")),
        json!([message("答案二")]),
    );
    let read = reads(&actions).remove(0);
    assert_eq!((read.turn_id.as_str(), read.attempt), ("轮", 0));
    let full = json!([
        {"type":"agentMessage","phase":"commentary","text":"中间说明"},
        message("答案一"),
        {"type":"commandExecution","command":"构建"},
        message("答案二")
    ]);
    let done = reply(&mut m, read.clone(), read_with(&read, Some("full"), full));
    assert_eq!(puts(&done), 1);
    let turn = last(&m, "运行");
    assert_eq!(turn.ready, Ready::Ready);
    assert_eq!(turn.output, "答案一\n答案二");
    assert_eq!(turn.output_source, OutputSource::FinalAnswer);
    assert_eq!(turn.status, TurnStatus::Failed);
    assert_eq!(turn.error, Some(json!({"message":"通知错误"})));
    assert!(!m.result_reads_pending());
}

#[test]
fn summary_review_completion_reads_review_text() {
    // M9 p1：review 的 completed 只带收尾 agentMessage，完整条目里才有 exitedReviewMode。
    let mut m = machine();
    open(&mut m, "运行", "线程");
    let actions = complete_view(
        &mut m,
        "审查轮",
        Some(json!("summary")),
        json!([message("收尾")]),
    );
    let read = reads(&actions).remove(0);
    let full = json!([
        {"type":"enteredReviewMode","review":"当前改动"},
        {"type":"exitedReviewMode","review":"审查报告全文"},
        message("收尾")
    ]);
    reply(&mut m, read.clone(), read_with(&read, Some("full"), full));
    assert_eq!(last(&m, "运行").output, "审查报告全文");
    assert_eq!(last(&m, "运行").output_source, OutputSource::Review);
}

#[test]
fn read_reply_that_is_still_summary_is_not_evidence() {
    for view in ["summary", "notLoaded"] {
        let mut m = machine();
        open(&mut m, "运行", "线程");
        let read = reads(&complete_view(
            &mut m,
            "轮",
            Some(json!("summary")),
            json!([message("摘要")]),
        ))
        .remove(0);
        let fallback = reply(&mut m, read, Err(json!({"code":-32601})));
        let read = reads(&fallback).remove(0);
        let actions = reply(
            &mut m,
            read.clone(),
            read_with(&read, Some(view), json!([message("摘要")])),
        );
        assert_eq!(timers(&actions).len(), 1, "itemsView={view}");
        assert_eq!(puts(&actions), 0);
        assert_eq!(last(&m, "运行").ready, Ready::Pending);
    }
}

#[test]
fn failed_read_after_summary_falls_back_to_summary_output_last() {
    // 摘要排在流式快照之后：有冻结的 final_answer 时用快照，没有时才用摘要。
    for streamed in [false, true] {
        let mut m = machine();
        open(&mut m, "运行", "线程");
        if streamed {
            item(&mut m, "运行", "轮", "item/completed", message("流式答案"));
        }
        let summary = json!([{"type":"agentMessage","phase":"commentary","text":"摘要说明"}]);
        let mut read = reads(&complete_view(
            &mut m,
            "轮",
            Some(json!("summary")),
            summary,
        ))
        .remove(0);
        for _ in RESULT_RETRY_MS {
            let actions = reply(&mut m, read.clone(), Err(json!({"message":"读取失败"})));
            let mut next = Vec::new();
            m.run_timer(timers(&actions).remove(0).0, &mut next);
            read = reads(&next).remove(0);
        }
        reply(&mut m, read, Err(json!({"message":"读取失败"})));
        let turn = last(&m, "运行");
        assert_eq!(turn.ready, Ready::Unavailable);
        if streamed {
            assert_eq!(
                (turn.output.as_str(), turn.output_source),
                ("流式答案", OutputSource::FinalAnswer)
            );
        } else {
            assert_eq!(
                (turn.output.as_str(), turn.output_source),
                ("摘要说明", OutputSource::Accumulated)
            );
        }
    }
}

#[test]
fn snapshot_summary_ranks_below_streamed_text() {
    let mut s = Snapshot::default();
    s.observe_summary(&[json!({"type":"agentMessage","text":""})]);
    assert_eq!(s.summary, None);
    s.observe_summary(&[message("摘要答案")]);
    assert_eq!(s.output(), ("摘要答案".into(), OutputSource::FinalAnswer));
    s.observe(&message("流式答案"));
    assert_eq!(s.output(), ("流式答案".into(), OutputSource::FinalAnswer));
    s.observe(&json!({"type":"exitedReviewMode","review":"审查"}));
    assert_eq!(s.output(), ("审查".into(), OutputSource::Review));
}

#[test]
fn shutdown_seals_pending_summary_turn_as_unavailable_with_summary() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    let summary = json!([{"type":"agentMessage","phase":"commentary","text":"摘要说明"}]);
    assert_eq!(
        reads(&complete_view(
            &mut m,
            "轮",
            Some(json!("summary")),
            summary
        ))
        .len(),
        1
    );
    let mut actions = Vec::new();
    m.shutdown_runs(&mut actions);
    m.finalize_runs(&mut actions);
    let turn = last(&m, "运行");
    assert_eq!(turn.ready, Ready::Unavailable);
    assert_eq!(
        (turn.output.as_str(), turn.output_source),
        ("摘要说明", OutputSource::Accumulated)
    );
    assert!(!m.result_reads_pending());
}

#[test]
fn request_context_uses_actual_thread_and_no_claim_or_client() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    let actions = complete(&mut m, "运行", "线程", "轮", json!([]));
    let context = actions
        .iter()
        .find_map(|a| {
            if let Action::Request(c) = a {
                Some(c)
            } else {
                None
            }
        })
        .unwrap();
    assert!(context.client.is_none());
    assert!(context.stream_id.is_none());
    assert_eq!(context.token.as_ref().unwrap().claim_id, 0);
    assert_eq!(
        context.params,
        Some(
            json!({"threadId":"线程","turnId":"轮","cursor":null,"limit":64,"sortDirection":"asc"})
        )
    );
}

#[test]
fn three_retries_consume_each_attempt_once_then_fallback() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    item(&mut m, "运行", "轮", "item/completed", message("冻结答案"));
    let mut read = reads(&complete(&mut m, "运行", "线程", "轮", json!([]))).remove(0);
    for (attempt, delay) in RESULT_RETRY_MS.into_iter().enumerate() {
        assert_eq!(read.attempt, attempt as u8);
        let actions = reply(&mut m, read.clone(), Err(json!({"message":"读取失败"})));
        let scheduled = timers(&actions);
        assert_eq!(scheduled.len(), 1);
        assert_eq!(scheduled[0].1, delay);
        assert_eq!(
            puts(&reply(&mut m, read.clone(), success(&read, "迟到成功"))),
            0
        );
        let mut next = Vec::new();
        m.run_timer(scheduled[0].0.clone(), &mut next);
        let mut duplicate = Vec::new();
        m.run_timer(scheduled[0].0.clone(), &mut duplicate);
        assert!(duplicate.is_empty());
        read = reads(&next).remove(0);
    }
    assert_eq!(read.attempt, 3);
    reply(&mut m, read, Ok(json!({"thread":{"id":"线程","turns":[]}})));
    assert_eq!(last(&m, "运行").ready, Ready::Unavailable);
    assert_eq!(last(&m, "运行").output, "冻结答案");
    assert!(!m.result_reads_pending());
}

#[test]
fn malformed_result_is_unknown_and_wrong_identity_does_not_consume() {
    for value in [
        json!({}),
        json!({"thread":{"id":"线程"}}),
        json!({"thread":{"id":"别的线程","turns":[]}}),
        json!({"thread":{"id":"线程","turns":[{"id":"轮","items":null}]}}),
        json!({"thread":{"id":"线程","turns":[{"id":"轮","items":[{"type":"agentMessage","text":123}]}]}}),
    ] {
        let mut m = machine();
        open(&mut m, "运行", "线程");
        let read = reads(&complete(&mut m, "运行", "线程", "轮", json!([]))).remove(0);
        let mut wrong = read.clone();
        wrong.turn_index = 4;
        assert_eq!(puts(&reply(&mut m, wrong, success(&read, "错误身份"))), 0);
        assert_eq!(timers(&reply(&mut m, read, Ok(value))).len(), 1);
        assert_eq!(last(&m, "运行").ready, Ready::Pending);
    }
}

#[test]
fn inverse_replies_preserve_each_turn_and_frozen_snapshots() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    item(
        &mut m,
        "运行",
        "一",
        "item/completed",
        message("第一轮快照"),
    );
    let first = reads(&complete(&mut m, "运行", "线程", "一", json!([]))).remove(0);
    item(
        &mut m,
        "运行",
        "二",
        "item/completed",
        message("第二轮快照"),
    );
    let second = reads(&complete(&mut m, "运行", "线程", "二", json!([]))).remove(0);
    reply(&mut m, second.clone(), success(&second, "第二轮补读"));
    let mut actions = Vec::new();
    m.finalize_runs(&mut actions);
    assert_eq!(m.runs.runs["运行"].turns[0].output, "第一轮快照");
    assert_eq!(m.runs.runs["运行"].turns[1].output, "第二轮补读");
    assert!(reply(&mut m, first.clone(), success(&first, "迟到第一轮")).is_empty());
}

#[test]
fn inverse_successes_and_duplicate_completion_do_not_change_terminal_state() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    let first = reads(&complete(&mut m, "运行", "线程", "一", json!([]))).remove(0);
    let second = reads(&complete(&mut m, "运行", "线程", "二", json!([]))).remove(0);
    let mut actions = Vec::new();
    m.run_released("运行", ReleaseKind::Natural, &mut actions);
    let terminal = m.runs.runs["运行"].status;
    reply(&mut m, second.clone(), success(&second, "第二轮"));
    reply(&mut m, first.clone(), success(&first, "第一轮"));
    assert_eq!(m.runs.runs["运行"].turns[0].output, "第一轮");
    assert_eq!(last(&m, "运行").output, "第二轮");
    assert!(complete(&mut m, "运行", "线程", "一", json!([message("重复")])).is_empty());
    complete(&mut m, "运行", "线程", "三", json!([message("迟到轮次")]));
    assert_eq!(m.runs.runs["运行"].status, terminal);
    assert_eq!(m.runs.runs["运行"].turns_total, 3);
    assert!(!m.runs.active.contains_key("运行"));
}

#[test]
fn review_parent_identity_and_old_association_override_fallback() {
    let mut m = machine();
    open(&mut m, "旧运行", "父线程");
    let mut actions = Vec::new();
    m.run_review_moved("旧运行", "子线程", None, &mut actions);
    m.run_turn_started("旧运行", "子线程", "旧轮", &mut actions);
    open(&mut m, "新运行", "子线程");
    let actions = complete(&mut m, "新运行", "子线程", "旧轮", json!([]));
    let read = reads(&actions).remove(0);
    assert_eq!(read.run_id, "旧运行");
    assert_eq!(read.thread_id, "子线程");
    let r = &m.runs.runs["旧运行"];
    assert_eq!(r.thread_id, "父线程");
    assert_ne!(Some(&r.short_id), r.review_short_id.as_ref());
    assert!(m.runs.runs["新运行"].turns.is_empty());
}

#[test]
fn goal_multi_turns_and_settlement_remain_one_run() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    let mut actions = Vec::new();
    m.run_settled("运行", true, &mut actions);
    m.run_goal_status("线程", Some("active"));
    complete(&mut m, "运行", "线程", "一", json!([message("一")]));
    complete(&mut m, "运行", "线程", "二", json!([message("二")]));
    assert_eq!(m.runs.runs.len(), 1);
    assert_eq!(m.runs.runs["运行"].state, RunState::Running);
    m.run_goal_status("线程", Some("blocked"));
    m.run_released("运行", ReleaseKind::Natural, &mut actions);
    assert_eq!(m.runs.runs["运行"].status, Some(RunStatus::GoalBlocked));
    assert!(m.runs.runs["运行"].joined);
}

#[test]
fn progress_started_flush_ten_items_and_stale_generation() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    let cmd = json!({"id":"命令","type":"commandExecution","command":"测试构建"});
    let begin = item(&mut m, "运行", "轮", "item/started", cmd.clone());
    let timer = timers(&begin)[0].clone();
    assert_eq!(timer.1, PROGRESS_FLUSH_MS);
    assert_eq!(m.runs.runs["运行"].progress.unconfirmed.len(), 1);
    assert!(timers(&item(&mut m, "运行", "轮", "item/started", cmd.clone())).is_empty());
    item(&mut m, "运行", "轮", "item/completed", cmd);
    assert!(m.runs.runs["运行"].progress.unconfirmed.is_empty());
    for i in 1..10 {
        let actions = item(
            &mut m,
            "运行",
            "轮",
            "item/completed",
            message(&format!("条目{i}")),
        );
        assert_eq!(puts(&actions), usize::from(i == 9));
    }
    let mut old = Vec::new();
    m.run_timer(timer.0, &mut old);
    assert!(old.is_empty());
    let next = item(
        &mut m,
        "运行",
        "轮",
        "item/started",
        json!({"id":"新命令","type":"commandExecution","command":"运行"}),
    );
    let mut flush = Vec::new();
    m.run_timer(timers(&next)[0].0.clone(), &mut flush);
    assert_eq!(puts(&flush), 1);
    assert!(timers(&flush).is_empty());
    assert!(m.runs.active["运行"].progress_generation.is_none());
}

#[test]
fn progress_limits_files_status_and_deduplication() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    let mut warned = false;
    for i in 0..70 {
        warned |= item(
            &mut m,
            "运行",
            "轮",
            "item/started",
            json!({"id":format!("工具{i}"),"type":"dynamicToolCall","tool":"工具"}),
        )
        .iter()
        .any(|a| matches!(a, Action::Warn(_)));
    }
    assert!(warned);
    assert_eq!(m.runs.runs["运行"].progress.unconfirmed.len(), 64);
    for i in 0..510 {
        item(
            &mut m,
            "运行",
            "轮",
            "item/completed",
            json!({"id":format!("文件{i}"),"type":"fileChange","status":"completed","changes":[{"path":format!("路径{i}")},{"path":format!("路径{i}")}]}),
        );
    }
    item(
        &mut m,
        "运行",
        "轮",
        "item/completed",
        json!({"id":"失败文件","type":"fileChange","status":"failed","changes":[{"path":"不应记录"}]}),
    );
    let p = &m.runs.runs["运行"].progress;
    assert_eq!(p.done.len(), 200);
    assert_eq!(p.done_total, 511);
    assert_eq!(p.observed_files.len(), 500);
    assert!(!p.observed_files.contains(&"不应记录".into()));
}

#[test]
fn release_matrix_and_stop_race_uses_order_not_clock() {
    for (kind, status, expected) in [
        (ReleaseKind::Natural, None, RunStatus::Failed),
        (ReleaseKind::Joined, None, RunStatus::Interrupted),
        (
            ReleaseKind::StartFailed,
            Some("completed"),
            RunStatus::Failed,
        ),
        (
            ReleaseKind::Natural,
            Some("interrupted"),
            RunStatus::Interrupted,
        ),
        (ReleaseKind::Natural, Some("failed"), RunStatus::Failed),
        (ReleaseKind::Joined, Some("completed"), RunStatus::Completed),
    ] {
        let mut m = machine();
        open(&mut m, "运行", "线程");
        let mut actions = Vec::new();
        if let Some(status) = status {
            m.run_turn_completed(
                "线程",
                Some("运行"),
                &json!({"id":"轮","status":status,"items":[message("输出")]}),
                &mut actions,
            );
        }
        m.run_released("运行", kind, &mut actions);
        assert_eq!(m.runs.runs["运行"].status, Some(expected));
        let terminal = m.runs.runs["运行"].clone();
        m.clock.now_ms += 100;
        actions.clear();
        m.run_released("运行", kind, &mut actions);
        assert!(actions.is_empty());
        assert_eq!(m.runs.runs["运行"], terminal);
    }
    for (active, completed_before, expected) in [
        (false, false, RunStatus::Completed),
        (false, true, RunStatus::Interrupted),
        (true, false, RunStatus::Interrupted),
        (true, true, RunStatus::Interrupted),
    ] {
        let mut m = machine();
        open(&mut m, "运行", "线程");
        let mut actions = Vec::new();
        m.run_goal_status("线程", Some(if active { "active" } else { "paused" }));
        if completed_before {
            complete(&mut m, "运行", "线程", "轮", json!([message("答案")]));
        }
        m.run_stop_began("运行", StopReason::ClientDisconnected, &mut actions);
        if !completed_before {
            complete(&mut m, "运行", "线程", "轮", json!([message("答案")]));
        }
        m.run_stop_confirmed("运行", &mut actions);
        m.run_released("运行", ReleaseKind::StopConfirmed, &mut actions);
        let r = &m.runs.runs["运行"];
        assert_eq!(r.status, Some(expected));
        assert_eq!(
            r.interrupt_reason,
            if expected == RunStatus::Completed {
                None
            } else {
                Some(InterruptReason::ClientDisconnected)
            }
        );
    }
}

#[test]
fn stop_checks_idempotent_confirmed_is_only_explicit_and_lost_stays_lost() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    let mut actions = Vec::new();
    m.run_stop_began("运行", StopReason::Stop, &mut actions);
    assert_eq!(m.runs.runs["运行"].stop_outcome, None);
    actions.clear();
    m.run_stop_check("运行", check(), &mut actions);
    assert_eq!(puts(&actions), 1);
    actions.clear();
    m.run_stop_check("运行", check(), &mut actions);
    assert!(actions.is_empty());
    let mut record = m.runs.runs["运行"].clone();
    record.state = RunState::Lost;
    let mut restored = machine();
    restored.restore_runs(m.runs.index.clone(), vec![record], &mut Vec::new());
    restored.run_stop_confirmed("运行", &mut actions);
    assert_eq!(restored.runs.runs["运行"].state, RunState::Lost);
    assert_eq!(
        restored.runs.runs["运行"].stop_outcome,
        Some(StopOutcome::Confirmed)
    );
    assert_eq!(restored.runs.runs["运行"].stop_check, None);
}

#[test]
fn start_failed_records_rpc_error_once() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    let mut actions = Vec::new();
    let error = json!({"code":-32000,"message":"启动失败"});
    m.run_start_failed("运行", &error, &mut actions);
    m.run_start_failed("运行", &error, &mut actions);
    assert_eq!(m.runs.runs["运行"].turns_total, 1);
    assert_eq!(last(&m, "运行").error, Some(error));
    assert_eq!(last(&m, "运行").turn_id, "");
    assert_eq!(last(&m, "运行").ready, Ready::Ready);
    m.run_released("运行", ReleaseKind::StartFailed, &mut actions);
    assert_eq!(m.runs.runs["运行"].status, Some(RunStatus::Failed));
}

#[test]
fn restore_all_pending_and_single_attempt_even_for_lost_runs() {
    let mut old = machine();
    open(&mut old, "运行", "线程");
    complete(&mut old, "运行", "线程", "一", json!([]));
    complete(&mut old, "运行", "线程", "二", json!([]));
    let mut second = old.runs.runs["运行"].clone();
    second.run_id = "停止运行".into();
    second.thread_id = "停止线程".into();
    second.state = RunState::Stopping;
    second.interrupt_reason = Some(InterruptReason::Stop);
    let mut m = machine();
    let mut actions = Vec::new();
    m.restore_runs(
        old.runs.index.clone(),
        vec![old.runs.runs["运行"].clone(), second],
        &mut actions,
    );
    assert_eq!(reads(&actions).len(), 4);
    assert_eq!(timers(&actions).len(), 1);
    assert_eq!(m.runs.runs["运行"].state, RunState::Lost);
    assert_eq!(
        m.runs.runs["运行"].interrupt_reason,
        Some(InterruptReason::BrokerLost)
    );
    assert_eq!(
        m.runs.runs["停止运行"].interrupt_reason,
        Some(InterruptReason::Stop)
    );
    for read in reads(&actions) {
        let failed = reply(&mut m, read, Err(json!({})));
        assert!(timers(&failed).is_empty());
    }
    assert!(!m.result_reads_pending());
    assert!(m.runs.runs.values().all(|r| r
        .turns
        .iter()
        .all(|t| t.ready == Ready::Unavailable && t.output.is_empty())));
    actions.clear();
    m.restore_runs(ThreadIndexFile::default(), Vec::new(), &mut actions);
    assert!(actions.is_empty());
}

#[test]
fn shutdown_accepts_only_inflight_then_finalized_is_silent() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    item(&mut m, "运行", "一", "item/completed", message("快照一"));
    let first = reads(&complete(&mut m, "运行", "线程", "一", json!([]))).remove(0);
    item(&mut m, "运行", "二", "item/completed", message("快照二"));
    let second = reads(&complete(&mut m, "运行", "线程", "二", json!([]))).remove(0);
    let retry = timers(&reply(&mut m, first.clone(), Err(json!({}))))
        .remove(0)
        .0;
    let mut actions = Vec::new();
    m.shutdown_runs(&mut actions);
    assert_eq!(m.runs.runs["运行"].state, RunState::Lost);
    let index = m.runs.index.clone();
    actions.clear();
    m.run_timer(retry.clone(), &mut actions);
    m.run_timer(RunTimer::DailyCleanup, &mut actions);
    m.run_learn_thread("线程", &json!({"thread":{"id":"线程"}}), &mut actions);
    m.run_forget_thread("线程", &mut actions);
    m.run_released("运行", ReleaseKind::Shutdown, &mut actions);
    actions.extend(complete(&mut m, "运行", "线程", "迟到", json!([])));
    assert!(actions.is_empty());
    assert_eq!(m.runs.index, index);
    let actions = reply(&mut m, second.clone(), success(&second, "及时输出"));
    assert_eq!(puts(&actions), 1);
    assert!(timers(&actions).is_empty());
    let mut actions = Vec::new();
    m.finalize_runs(&mut actions);
    assert_eq!(m.runs.runs["运行"].turns[0].output, "快照一");
    assert_eq!(m.runs.runs["运行"].turns[1].output, "及时输出");
    assert!(!m.result_reads_pending());
    actions.clear();
    m.run_stop_confirmed("运行", &mut actions);
    m.run_timer(retry, &mut actions);
    m.finalize_runs(&mut actions);
    m.shutdown_runs(&mut actions);
    actions.extend(reply(&mut m, first.clone(), success(&first, "太晚")));
    assert!(actions.is_empty());
}

#[test]
fn read_budget_queue_overflow_and_retry_fairness() {
    let mut m = machine();
    let mut requests = Vec::new();
    for i in 0..(MAX_RESULT_READS + MAX_RESULT_QUEUE + 1) {
        let run = format!("运行{i}");
        let thread = format!("线程{i}");
        open(&mut m, &run, &thread);
        let actions = complete(&mut m, &run, &thread, "轮", json!([]));
        requests.extend(reads(&actions));
        if i == MAX_RESULT_READS + MAX_RESULT_QUEUE {
            assert_eq!(last(&m, &run).ready, Ready::Unavailable);
            assert!(actions.iter().any(|a| matches!(a, Action::Warn(_))));
        }
    }
    assert_eq!(requests.len(), 16);
    assert_eq!(m.runs.reads.len(), 272);
    assert_eq!(m.runs.read_queue.len(), 256);
    let first = requests.remove(0);
    let actions = reply(&mut m, first.clone(), success(&first, "内容"));
    assert_eq!(reads(&actions).len(), 1);
    assert_eq!(m.runs.read_queue.len(), 255);
    let second = requests.remove(0);
    let actions = reply(&mut m, second, Err(json!({})));
    assert_eq!(timers(&actions).len(), 1);
    assert_eq!(reads(&actions).len(), 1);
    let mut actions = Vec::new();
    m.shutdown_runs(&mut actions);
    m.finalize_runs(&mut actions);
    assert!(m.runs.reads.is_empty());
    assert!(m.runs.read_queue.is_empty());
}

#[test]
fn turns_cap_preserves_absolute_read_identity_and_physical_slots() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    let first = reads(&complete(&mut m, "运行", "线程", "轮0", json!([]))).remove(0);
    for i in 1..105 {
        complete(
            &mut m,
            "运行",
            "线程",
            &format!("轮{i}"),
            json!([message("输出")]),
        );
    }
    assert_eq!(m.runs.runs["运行"].turns.len(), 100);
    assert_eq!(m.runs.runs["运行"].turns_total, 105);
    assert_eq!(m.runs.turn_runs["线程"].len(), TURN_RUNS_PER_THREAD);
    assert_eq!(
        m.runs.reads[&first.generation].phase,
        reads::ReadPhase::Done
    );
    let latest = reads(&complete(&mut m, "运行", "线程", "末轮", json!([]))).remove(0);
    assert_eq!(latest.turn_index, 105);
    reply(&mut m, first.clone(), success(&first, "不应覆盖"));
    reply(&mut m, latest.clone(), success(&latest, "最后输出"));
    assert_eq!(last(&m, "运行").output, "最后输出");
    assert!(!m.result_reads_pending());
}

#[test]
fn index_activity_delete_markers_and_daily_retention() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    m.clock.now_ms = 2000;
    complete(&mut m, "运行", "线程", "轮", json!([message("输出")]));
    assert_eq!(
        m.runs.index.threads.values().next().unwrap().updated_at,
        2000
    );
    m.clock.now_ms = 3000;
    let mut actions = Vec::new();
    m.run_released("运行", ReleaseKind::Natural, &mut actions);
    assert_eq!(
        m.runs.index.threads.values().next().unwrap().updated_at,
        3000
    );
    m.run_forget_thread("线程", &mut actions);
    assert!(m.runs.index.threads.is_empty());
    assert_eq!(m.runs.index.removed["线程"], 3000);
    let mut restored = machine();
    restored.clock.now_ms = RETENTION_MS + 3001;
    restored.restore_runs(m.runs.index.clone(), Vec::new(), &mut actions);
    assert!(restored.runs.index.removed.is_empty());
    open(&mut restored, "新运行", "新线程");
    restored.clock.now_ms += RETENTION_MS + 1;
    actions.clear();
    restored.run_timer(RunTimer::DailyCleanup, &mut actions);
    assert!(restored.runs.index.threads.is_empty());
    assert_eq!(timers(&actions).len(), 1);
}

#[test]
fn eviction_keeps_active_pending_and_latest_unconfirmed_lost() {
    let mut m = machine();
    open(&mut m, "活动", "活动线程");
    open(&mut m, "停止", "停止线程");
    m.run_stop_began("停止", StopReason::Stop, &mut Vec::new());
    open(&mut m, "补读", "补读线程");
    let pending = reads(&complete(&mut m, "补读", "补读线程", "轮", json!([]))).remove(0);
    m.run_released("补读", ReleaseKind::Natural, &mut Vec::new());
    open(&mut m, "失联", "失联线程");
    m.runs.runs.get_mut("失联").unwrap().state = RunState::Lost;
    m.runs.runs.get_mut("失联").unwrap().stop_outcome = Some(StopOutcome::Unconfirmed);
    m.runs.active.remove("失联");
    for i in 0..220 {
        m.clock.now_ms += 1;
        let id = format!("历史{i}");
        open(&mut m, &id, &id);
        m.run_released(&id, ReleaseKind::Natural, &mut Vec::new());
        m.flush_run_notices(&mut Vec::new());
    }
    assert_eq!(m.runs.runs.len(), MAX_RECENT_RUNS + 4);
    for run in ["活动", "停止", "补读", "失联"] {
        assert!(m.runs.runs.contains_key(run));
    }
    assert_eq!(m.runs.updated.len(), m.runs.runs.len());
    reply(&mut m, pending.clone(), success(&pending, "补完"));
    m.flush_run_notices(&mut Vec::new());
    assert!(m.runs.runs.contains_key("补读"));
    assert_eq!(m.runs.runs.len(), MAX_RECENT_RUNS + 3);
    open(&mut m, "更新运行", "失联线程");
    assert!(!m.runs.runs.contains_key("失联"));
    assert!(m
        .runs
        .turn_runs
        .values()
        .all(|links| links.iter().all(|link| m.runs.runs.contains_key(&link.run))));
}

#[test]
fn release_invalidates_progress_timer_and_clock_formatter_handles_leap_days() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    let start = item(
        &mut m,
        "运行",
        "轮",
        "item/started",
        json!({"id":"命令","type":"commandExecution","command":"开始"}),
    );
    let timer = timers(&start).remove(0).0;
    m.run_released("运行", ReleaseKind::Joined, &mut Vec::new());
    let mut actions = Vec::new();
    m.run_timer(timer, &mut actions);
    assert!(actions.is_empty());
    assert_eq!(state::utc_timestamp(0), "1970-01-01T00:00:00.000Z");
    assert_eq!(
        state::utc_timestamp(951_782_400_123),
        "2000-02-29T00:00:00.123Z"
    );
    assert_eq!(
        state::utc_timestamp(4_107_542_400_000),
        "2100-03-01T00:00:00.000Z"
    );
}

#[test]
fn restore_ended_pending_is_bounded_and_single_read_can_succeed() {
    let mut old = machine();
    open(&mut old, "模板", "模板线程");
    complete(&mut old, "模板", "模板线程", "轮", json!([]));
    old.run_released("模板", ReleaseKind::Natural, &mut Vec::new());
    let template = old.runs.runs["模板"].clone();
    let records = (0..280)
        .map(|i| {
            let mut r = template.clone();
            r.run_id = format!("恢复{i:03}");
            r.thread_id = format!("线程{i}");
            r.short_id = format!("{i:08x}");
            r
        })
        .collect();
    let mut m = machine();
    let mut actions = Vec::new();
    m.restore_runs(ThreadIndexFile::default(), records, &mut actions);
    assert_eq!(reads(&actions).len(), MAX_RESULT_READS);
    assert_eq!(m.runs.reads.len(), MAX_RESULT_READS + MAX_RESULT_QUEUE);
    assert_eq!(
        m.runs
            .runs
            .values()
            .flat_map(|r| &r.turns)
            .filter(|t| t.ready == Ready::Unavailable)
            .count(),
        8
    );
    let read = reads(&actions).remove(0);
    let id = read.run_id.clone();
    let next = reply(&mut m, read.clone(), success(&read, "恢复成功"));
    assert_eq!(last(&m, &id).output, "恢复成功");
    assert_eq!(m.runs.runs[&id].state, RunState::Ended);
    assert_eq!(reads(&next).len(), 1);
    assert!(timers(&next).is_empty());
}

#[test]
fn all_finalized_hooks_produce_no_actions_and_keep_records() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    let read = reads(&complete(&mut m, "运行", "线程", "轮", json!([]))).remove(0);
    m.shutdown_runs(&mut Vec::new());
    m.finalize_runs(&mut Vec::new());
    let records = m.runs.runs.clone();
    let index = m.runs.index.clone();
    let mut actions = Vec::new();
    actions.extend(open(&mut m, "新运行", "新线程"));
    m.run_settled("运行", true, &mut actions);
    m.run_turn_started("运行", "线程", "新轮", &mut actions);
    m.run_item("线程",Some("运行"),"item/started",&Some(json!({"turnId":"新轮","item":{"id":"工具","type":"mcpToolCall","server":"服务","tool":"工具"}})),&mut actions);
    m.run_turn_completed(
        "线程",
        Some("运行"),
        &json!({"id":"新轮","status":"completed","items":[]}),
        &mut actions,
    );
    m.run_start_failed("运行", &json!({"message":"迟到失败"}), &mut actions);
    m.run_review_moved("运行", "子线程", None, &mut actions);
    m.run_goal_status("线程", Some("active"));
    m.run_stop_began("运行", StopReason::Stop, &mut actions);
    m.run_stop_check("运行", check(), &mut actions);
    m.run_stop_confirmed("运行", &mut actions);
    m.run_released("运行", ReleaseKind::Natural, &mut actions);
    m.run_learn_thread("线程", &json!({"thread":{"id":"线程"}}), &mut actions);
    m.run_forget_thread("线程", &mut actions);
    m.run_result_reply(read.clone(), success(&read, "迟到成功"), &mut actions);
    m.run_timer(RunTimer::ResultRetry(read), &mut actions);
    m.run_timer(RunTimer::DailyCleanup, &mut actions);
    m.run_timer(
        RunTimer::ProgressFlush {
            run_id: "运行".into(),
            generation: 1,
        },
        &mut actions,
    );
    m.restore_runs(ThreadIndexFile::default(), Vec::new(), &mut actions);
    m.shutdown_runs(&mut actions);
    m.finalize_runs(&mut actions);
    assert!(actions.is_empty());
    assert_eq!(m.runs.runs, records);
    assert_eq!(m.runs.index, index);
}

#[test]
fn truncated_pending_cannot_exceed_physical_rpc_limit() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    let mut sent = Vec::new();
    for i in 0..120 {
        sent.extend(reads(&complete(
            &mut m,
            "运行",
            "线程",
            &format!("轮{i}"),
            json!([]),
        )));
    }
    assert_eq!(sent.len(), 16);
    assert_eq!(
        m.runs
            .reads
            .values()
            .filter(|r| r.phase == reads::ReadPhase::Done)
            .count(),
        16
    );
    assert_eq!(m.runs.read_queue.len(), 100);
    for read in sent {
        let next = reply(&mut m, read.clone(), success(&read, "已截掉的轮"));
        assert_eq!(reads(&next).len(), 1);
    }
    assert_eq!(
        m.runs
            .reads
            .values()
            .filter(|r| r.phase == reads::ReadPhase::InFlight)
            .count(),
        16
    );
    assert_eq!(m.runs.runs["运行"].turns.len(), 100);
}

#[test]
fn sparse_tool_completion_removes_unconfirmed_and_flushes() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    item(
        &mut m,
        "运行",
        "轮",
        "item/started",
        json!({"id":"工具","type":"mcpToolCall","server":"服务","tool":"名称"}),
    );
    let generation = m.runs.active["运行"].progress_generation.unwrap();
    let mut actions = Vec::new();
    m.run_timer(
        RunTimer::ProgressFlush {
            run_id: "运行".into(),
            generation,
        },
        &mut actions,
    );
    let complete = item(
        &mut m,
        "运行",
        "轮",
        "item/completed",
        json!({"id":"工具","type":"mcpToolCall","status":"completed"}),
    );
    assert!(m.runs.runs["运行"].progress.unconfirmed.is_empty());
    assert_eq!(timers(&complete).len(), 1);
}

#[test]
fn ready_turn_outside_link_window_still_routes_to_old_run() {
    let mut m = machine();
    open(&mut m, "旧运行", "线程");
    let mut actions = Vec::new();
    m.run_turn_started("旧运行", "线程", "最旧轮", &mut actions);
    complete(
        &mut m,
        "旧运行",
        "线程",
        "最旧轮",
        json!([message("旧轮结果")]),
    );
    for i in 0..20 {
        complete(
            &mut m,
            "旧运行",
            "线程",
            &format!("轮{i}"),
            json!([message("输出")]),
        );
    }
    open(&mut m, "新运行", "线程");
    let actions = complete(
        &mut m,
        "新运行",
        "线程",
        "最旧轮",
        json!([message("重复通知")]),
    );
    assert!(actions.is_empty());
    assert!(m.runs.runs["新运行"].turns.is_empty());
    assert_eq!(m.runs.runs["旧运行"].turns_total, 21);
}

#[test]
fn draining_failed_inflight_does_not_retry_or_pump_queue() {
    let mut m = machine();
    let mut requests = Vec::new();
    for i in 0..20 {
        let id = format!("运行{i}");
        open(&mut m, &id, &id);
        requests.extend(reads(&complete(&mut m, &id, &id, "轮", json!([]))));
    }
    assert_eq!(m.runs.read_queue.len(), 4);
    m.shutdown_runs(&mut Vec::new());
    let read = requests.remove(0);
    let id = read.run_id.clone();
    let actions = reply(&mut m, read, Err(json!({"message":"服务器断开"})));
    assert_eq!(puts(&actions), 1);
    assert!(timers(&actions).is_empty());
    assert!(reads(&actions).is_empty());
    assert_eq!(last(&m, &id).ready, Ready::Unavailable);
    assert_eq!(m.runs.read_queue.len(), 4);
}

#[test]
fn pending_retry_survives_release_and_finishes_without_changing_ended_at() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    let read = reads(&complete(&mut m, "运行", "线程", "轮", json!([]))).remove(0);
    let timer = timers(&reply(&mut m, read, Err(json!({})))).remove(0).0;
    m.run_released("运行", ReleaseKind::Natural, &mut Vec::new());
    let ended = m.runs.runs["运行"].ended_at;
    m.clock.now_ms += 5000;
    let mut actions = Vec::new();
    m.run_timer(timer, &mut actions);
    let retry = reads(&actions).remove(0);
    reply(&mut m, retry.clone(), success(&retry, "释放后补完"));
    assert_eq!(m.runs.runs["运行"].ended_at, ended);
    assert_eq!(last(&m, "运行").ready, Ready::Ready);
    assert_eq!(m.runs.runs["运行"].status, Some(RunStatus::Completed));
}

#[test]
fn read_after_trimming_uses_original_ordinal() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    complete(&mut m, "运行", "线程", "将截掉", json!([message("完成")]));
    let read = reads(&complete(&mut m, "运行", "线程", "等待补读", json!([]))).remove(0);
    for i in 0..99 {
        complete(
            &mut m,
            "运行",
            "线程",
            &format!("后续{i}"),
            json!([message("内容")]),
        );
    }
    assert_eq!(m.runs.runs["运行"].turns[0].turn_id, "等待补读");
    assert_eq!(read.turn_index, 1);
    reply(&mut m, read.clone(), success(&read, "写入原轮"));
    assert_eq!(m.runs.runs["运行"].turns[0].output, "写入原轮");
    assert_eq!(last(&m, "运行").output, "内容");
}

#[test]
fn 父线程新运行不淘汰子线程索引引用的已结束审查() {
    let mut m = machine();
    open(&mut m, "审查运行", "父线程");
    m.run_review_moved("审查运行", "子线程", None, &mut Vec::new());
    complete(
        &mut m,
        "审查运行",
        "子线程",
        "审查轮",
        json!([message("审查结果")]),
    );
    m.run_released("审查运行", ReleaseKind::Natural, &mut Vec::new());
    m.clock.now_ms += 1;
    open(&mut m, "父线程新运行", "父线程");
    assert_eq!(m.latest_run("父线程").unwrap().run_id, "父线程新运行");
    let child = m.latest_run("子线程").expect("子线程最近运行必须保留");
    assert_eq!(child.run_id, "审查运行");
    assert_eq!(child.state, RunState::Ended);
    assert_eq!(child.turns[0].output, "审查结果");
    assert!(m.runs.turn_runs["子线程"]
        .iter()
        .any(|link| link.run == "审查运行"));
    m.flush_run_notices(&mut Vec::new());
    open(&mut m, "子线程新运行", "子线程");
    assert!(!m.runs.runs.contains_key("审查运行"));
}

#[test]
fn 父线程新运行后子线程失联审查仍受保护并进入实际线程核实() {
    use crate::claim::{self, ClientRequest, Event};
    use crate::stop::StopRequest;

    let mut previous = machine();
    open(&mut previous, "失联审查", "父线程");
    previous.run_review_moved("失联审查", "子线程", None, &mut Vec::new());
    let mut m = machine();
    m.restore_runs(
        previous.runs.index,
        previous.runs.runs.into_values().collect(),
        &mut Vec::new(),
    );
    m.clock.now_ms += 1;
    open(&mut m, "父线程新运行", "父线程");
    for i in 0..=MAX_RECENT_RUNS {
        m.clock.now_ms += 1;
        let id = format!("较新历史{i}");
        open(&mut m, &id, &id);
        m.run_released(&id, ReleaseKind::Natural, &mut Vec::new());
        m.flush_run_notices(&mut Vec::new());
    }
    assert_eq!(m.runs.runs.len(), MAX_RECENT_RUNS + 2);
    let lost = m
        .latest_run("子线程")
        .expect("子线程索引引用的失联证据必须保留");
    assert_eq!(lost.run_id, "失联审查");
    assert_eq!(lost.state, RunState::Lost);
    assert_eq!(lost.stop_outcome, Some(StopOutcome::Unconfirmed));
    assert_eq!(m.latest_run("父线程").unwrap().run_id, "父线程新运行");

    let mut replaced = m.clone();
    open(&mut replaced, "子线程新运行", "子线程");
    assert!(!replaced.runs.runs.contains_key("失联审查"));

    let (mut m, actions) = claim::step(
        m,
        Event::ClientRequest {
            request: ClientRequest {
                conn: 1,
                id: json!(1),
                method: "broker/threads/stop".into(),
                params: Some(json!({"threadId":"子线程"})),
            },
            run_id: None,
        },
    );
    let verification: Vec<_> = actions
        .iter()
        .filter_map(|action| match action {
            Action::Request(context)
                if matches!(
                    &context.operation,
                    Operation::Stop(_, StopRequest::VerifyTurn | StopRequest::VerifyGoal { .. })
                ) =>
            {
                Some(context.clone())
            }
            _ => None,
        })
        .collect();
    assert_eq!(verification.len(), 2);
    assert!(verification
        .iter()
        .all(|context| context.params.as_ref().unwrap()["threadId"] == "子线程"));
    let operation = m
        .stops
        .operations
        .values()
        .find(|operation| operation.run_id.as_deref() == Some("失联审查"))
        .unwrap();
    assert!(!operation.already_stopped);
    assert!(!operation.turn_absent);
    assert!(!actions.iter().any(
        |action| matches!(action, Action::Send { message, .. } if message.get("result").is_some())
    ));
    m.clock.now_ms += 1;
    for context in verification {
        let result = if context.method == "thread/read" {
            json!({"thread":{"id":"子线程","turns":[]}})
        } else {
            json!({"goal":null})
        };
        (m, _) = claim::step(
            m,
            Event::Reply {
                context,
                result: Ok(result),
            },
        );
    }
    let confirmed = m.latest_run("子线程").unwrap();
    assert_eq!(confirmed.state, RunState::Lost);
    assert_eq!(confirmed.stop_outcome, Some(StopOutcome::Confirmed));
    assert_eq!(m.latest_run("父线程").unwrap().state, RunState::Running);
}

#[test]
fn 索引预览按字符保留二百字符且回包不改变其他字段() {
    let mut m = machine();
    for length in [199, 200, 201] {
        let preview: String = ['中', '😀'].into_iter().cycle().take(length).collect();
        let mut actions = Vec::new();
        m.run_learn_thread(
            "线程",
            &json!({"thread":{"id":"线程","preview":preview,"cwd":"目录"},"model":"模型"}),
            &mut actions,
        );
        let entry = m.runs.index.threads.values().next().unwrap();
        assert_eq!(
            entry.preview.as_deref(),
            Some(preview.chars().take(200).collect::<String>().as_str())
        );
        assert_eq!(entry.model.as_deref(), Some("模型"));
        assert_eq!(entry.cwd.as_deref(), Some("目录"));
        assert_eq!(index_puts(&actions), 1);
        for action in &actions {
            if let Action::Store(StoreOp::PutThreads(index)) = action {
                assert!(index.threads.values().all(|entry| entry
                    .preview
                    .as_ref()
                    .unwrap()
                    .chars()
                    .count()
                    <= 200));
            }
        }
    }
}

#[test]
fn 索引元数据按序列化字节判断四千零九十六边界且超限置空告警() {
    let mut m = machine();
    // 字符串的 JSON 引号也计入预算；转义和中文按实际 UTF-8 序列化字节计。
    let exact = json!("x".repeat(4094));
    let cases = [
        (exact.clone(), true),
        (json!("x".repeat(4095)), false),
        (json!("\n".repeat(2048)), false),
        (json!("中".repeat(1365)), false),
        (json!({"source":"run","说明":"合法元数据"}), true),
    ];
    for (i, (meta, accepted)) in cases.into_iter().enumerate() {
        let serialized = serde_json::to_vec(&meta).unwrap();
        assert_eq!(serialized.len() <= 4096, accepted);
        m.records
            .entry("线程".into())
            .or_default()
            .pending_meta
            .insert(1, meta.clone());
        let id = format!("运行{i}");
        let actions = open(&mut m, &id, "线程");
        let entry = m.runs.index.threads.values().next().unwrap();
        assert_eq!(entry.meta, accepted.then(|| meta.clone()));
        assert_eq!(
            actions
                .iter()
                .filter(|action| matches!(action, Action::Warn(_)))
                .count(),
            usize::from(!accepted)
        );
        assert_eq!(m.runs.runs[&id].meta, Some(meta));
        assert!(m.records["线程"].pending_meta.is_empty());
        for action in &actions {
            if let Action::Store(StoreOp::PutThreads(index)) = action {
                let entry = index.threads.values().next().unwrap();
                assert!(entry
                    .meta
                    .as_ref()
                    .is_none_or(|meta| serde_json::to_vec(meta).unwrap().len() <= 4096));
                if !accepted {
                    assert!(serde_json::to_value(entry).unwrap()["meta"].is_null());
                }
            }
        }
    }
}

#[test]
fn 恢复索引规范化超长字段后只写回有界内容() {
    let mut previous = machine();
    open(&mut previous, "运行", "线程");
    let entry = previous.runs.index.threads.values_mut().next().unwrap();
    entry.preview = Some("😀".repeat(201));
    entry.meta = Some(json!({"内容":"x".repeat(4096)}));
    let mut m = machine();
    let mut actions = Vec::new();
    m.restore_runs(previous.runs.index, Vec::new(), &mut actions);
    let entry = m.runs.index.threads.values().next().unwrap();
    assert_eq!(entry.preview.as_deref(), Some("😀".repeat(200).as_str()));
    assert!(entry.meta.is_none());
    assert_eq!(entry.last_run_id.as_deref(), Some("运行"));
    assert_eq!(index_puts(&actions), 1);
    assert_eq!(
        actions
            .iter()
            .filter(|action| matches!(action, Action::Warn(_)))
            .count(),
        1
    );
    let index = actions
        .iter()
        .find_map(|action| {
            if let Action::Store(StoreOp::PutThreads(index)) = action {
                Some(index)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(index, &m.runs.index);
    m.clock.now_ms += RETENTION_MS + 1;
    let mut cleanup = Vec::new();
    m.run_timer(RunTimer::DailyCleanup, &mut cleanup);
    assert!(m.runs.index.threads.is_empty());
}

#[test]
fn 空通知不执行淘汰而通知消费后解除保护并留下墓碑() {
    let mut m = machine();
    open(&mut m, "旧运行", "线程");
    m.run_released("旧运行", ReleaseKind::Natural, &mut Vec::new());
    open(&mut m, "新运行", "线程");
    assert!(
        m.runs.runs.contains_key("旧运行"),
        "尚未消费的结束通知保护旧运行"
    );
    // 暂存通知以独立核对空队列的调用不会扫描、淘汰账本。
    let notices = m.runs.take_notices();
    assert!(!notices.is_empty());
    let before = m.runs.clone();
    let mut actions = Vec::new();
    m.flush_run_notices(&mut actions);
    assert_eq!(m.runs, before);
    assert!(actions.is_empty());
    m.runs.notices = notices;
    m.flush_run_notices(&mut actions);
    assert!(m.runs.notices.is_empty());
    assert!(!m.runs.runs.contains_key("旧运行"));
    assert_eq!(
        m.run_lookup("旧运行"),
        RunLookup::Retired {
            thread_id: "线程".into()
        }
    );
    assert!(m.runs.runs.contains_key("新运行"));
}

#[test]
fn 预算用量及受阻目标统一终态且失联保持未知() {
    for goal in [
        "blocked",
        "budgetLimited",
        "usageLimited",
        "complete",
        "paused",
    ] {
        for lost in [false, true] {
            let mut m = machine();
            open(&mut m, "r", "t");
            complete(&mut m, "r", "t", "turn", json!([message("最后一轮已完成")]));
            m.run_goal_status("t", Some(goal));
            if lost {
                m.shutdown_runs(&mut Vec::new());
            }
            m.run_released("r", ReleaseKind::Natural, &mut Vec::new());
            let record = &m.runs.runs["r"];
            if lost {
                assert_eq!(record.state, RunState::Lost);
                assert_eq!(record.status, None);
            } else {
                assert_eq!(
                    record.status,
                    Some(
                        if matches!(goal, "blocked" | "budgetLimited" | "usageLimited") {
                            RunStatus::GoalBlocked
                        } else {
                            RunStatus::Completed
                        }
                    ),
                    "目标状态：{goal}"
                );
            }
        }
    }
}

#[test]
fn 审查子线程继承父恢复覆盖且已有登记优先() {
    for child in ["parent", "child"] {
        for existing in [false, true] {
            let mut m = machine();
            open(&mut m, "review", "parent");
            let parent = m
                .runs
                .index
                .threads
                .values_mut()
                .find(|e| e.thread_id == "parent")
                .unwrap();
            parent.resume_overrides = Some(
                serde_json::from_value(
                    json!({"config":{"父键":true},"developerInstructions":"父指令"}),
                )
                .unwrap(),
            );
            if existing && child != "parent" {
                let short = m.runs.ensure_thread(child, 1, 42);
                m.runs
                    .index
                    .threads
                    .get_mut(&short)
                    .unwrap()
                    .resume_overrides = Some(
                    serde_json::from_value(json!({"config":{},"developerInstructions":"子指令"}))
                        .unwrap(),
                );
            }
            let mut actions = Vec::new();
            m.run_review_moved("review", child, None, &mut actions);
            let entry = m
                .runs
                .index
                .threads
                .values()
                .find(|e| e.thread_id == child)
                .unwrap();
            assert_eq!(
                entry
                    .resume_overrides
                    .as_ref()
                    .unwrap()
                    .developer_instructions
                    .as_deref(),
                Some(if existing && child != "parent" {
                    "子指令"
                } else {
                    "父指令"
                })
            );
            if child == "parent" {
                assert!(actions.is_empty());
                assert!(m.runs.runs["review"].review_thread_id.is_none());
            } else {
                assert!(index_puts(&actions) > 0);
            }
            let roundtrip: ThreadIndexFile =
                serde_json::from_slice(&serde_json::to_vec(&m.runs.index).unwrap()).unwrap();
            assert_eq!(roundtrip.threads, m.runs.index.threads);
        }
    }
}
