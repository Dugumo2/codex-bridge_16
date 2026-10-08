//! H2 状态读取层（接口第 2–5 节）：经单步驱动覆盖四种查询、合法性校验、版本、预算、期限与生命周期。

use super::*;
use crate::claim::{step, Event, Machine};
use crate::test_support::status_read::{
    agent_message, deadline_event, drive_status, error_reply, expect, expect_items, items_page,
    live_status_requests, meta, reply_status, status_requests, turn_event, turns_page,
    user_message, StatusPhase, StatusRequest, StatusStep,
};
use std::time::Instant;

const T: &str = "线程";

fn caller() -> StatusCaller {
    StatusCaller::ContinueCheck { round: 1 }
}

fn begin_with(m: &mut Machine, query: StatusQuery, caller: StatusCaller) -> (u64, Vec<Action>) {
    let mut actions = Vec::new();
    let id = m.begin_status_read(T, None, query, caller, &mut actions);
    assert_bounded(&actions);
    (id, actions)
}

fn begin(m: &mut Machine, query: StatusQuery) -> (u64, Vec<Action>) {
    begin_with(m, query, caller())
}

/// 本层发出的请求不得带 includeTurns，轮次页只用 notLoaded（接口第 5 节）。
fn assert_bounded(actions: &[Action]) {
    for request in status_requests(actions) {
        let params = request.params.as_ref().expect("状态读取请求带参数");
        assert!(
            params.get("includeTurns").is_none(),
            "不得请求全量历史：{params}"
        );
        if request.phase == StatusPhase::Turns {
            assert_eq!(params["itemsView"], "notLoaded");
        } else {
            assert!(params.get("itemsView").is_none());
        }
    }
}

fn drive(m: &mut Machine, script: Vec<StatusStep>) -> (Vec<Action>, Vec<StatusDone>) {
    let (actions, done) = drive_status(m, script);
    assert_bounded(&actions);
    (actions, done)
}

fn r(phase: StatusPhase, value: Value) -> StatusStep {
    StatusStep::Reply(expect(phase), Ok(value))
}

fn ri(candidate: &str, value: Value) -> StatusStep {
    StatusStep::Reply(expect_items(candidate), Ok(value))
}

fn fail(phase: StatusPhase, error: Value) -> StatusStep {
    StatusStep::Reply(expect(phase), Err(error))
}

fn ev(event: Event) -> StatusStep {
    StatusStep::Event(event)
}

fn only(done: &[StatusDone]) -> &StatusResult {
    assert_eq!(done.len(), 1, "每次读取只完成一次：{done:?}");
    &done[0].result
}

fn unknown(reason: StatusUnknown) -> StatusResult {
    StatusResult::Unknown(reason)
}

fn settlement(candidates: &[&str], input: &[&str]) -> StatusQuery {
    StatusQuery::Settlement {
        candidates: candidates.iter().map(|c| c.to_string()).collect(),
        input: input.iter().map(|c| c.to_string()).collect(),
    }
}

fn target(id: &str) -> StatusQuery {
    StatusQuery::Turn { target: id.into() }
}

fn debug_contains(actions: &[Action], needle: &str) -> bool {
    actions
        .iter()
        .any(|a| matches!(a, Action::Debug(text) if text.contains(needle)))
}

// ─── Busy ─────────────────────────────────────────────────────────────

#[test]
fn busy_只发一次不带历史的元数据读() {
    for (status, expected) in [
        ("idle", StatusResult::NotBusy),
        ("notLoaded", StatusResult::NotBusy),
        ("systemError", StatusResult::NotBusy),
        ("active", StatusResult::Busy),
    ] {
        let mut m = Machine::new(true);
        let (id, actions) = begin(&mut m, StatusQuery::Busy);
        assert!(id > 0);
        let requests = status_requests(&actions);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].phase, StatusPhase::Metadata);
        assert_eq!(requests[0].method, "thread/read");
        assert_eq!(requests[0].params, Some(json!({"threadId": T})));
        let (actions, done) = drive(&mut m, vec![r(StatusPhase::Metadata, meta(T, status))]);
        assert_eq!(only(&done), &expected, "{status}");
        assert!(status_requests(&actions).is_empty(), "Busy 不分页");
        assert!(m.status_reads.reads.is_empty());
    }
}

#[test]
fn busy_缺字段未知值与错误回包都是未知() {
    let cases = [
        (Ok(json!({})), StatusUnknown::Malformed),
        (Ok(json!({"thread": {}})), StatusUnknown::Malformed),
        (
            Ok(json!({"thread": {"status": {}}})),
            StatusUnknown::Malformed,
        ),
        (
            Ok(json!({"thread": {"status": "idle"}})),
            StatusUnknown::Malformed,
        ),
        (
            Ok(json!({"thread": {"status": {"type": "paused"}}})),
            StatusUnknown::Malformed,
        ),
        (
            Ok(json!({"thread": {"id": "别的线程", "status": {"type": "idle"}}})),
            StatusUnknown::Malformed,
        ),
        (
            Err(error_reply(-32000, "读取失败")),
            StatusUnknown::ErrorReply,
        ),
        (
            Err(error_reply(-32601, "方法不存在")),
            StatusUnknown::Unsupported,
        ),
    ];
    for (result, reason) in cases {
        let mut m = Machine::new(true);
        begin(&mut m, StatusQuery::Busy);
        let (_, done) = drive(
            &mut m,
            vec![StatusStep::Reply(
                expect(StatusPhase::Metadata),
                result.clone(),
            )],
        );
        assert_eq!(only(&done), &unknown(reason), "{result:?}");
    }
}

// ─── Activity ─────────────────────────────────────────────────────────

#[test]
fn activity_非活动直接空闲且不分页() {
    let mut m = Machine::new(true);
    begin(&mut m, StatusQuery::Activity);
    let (actions, done) = drive(&mut m, vec![r(StatusPhase::Metadata, meta(T, "idle"))]);
    assert_eq!(only(&done), &StatusResult::Idle);
    assert!(status_requests(&actions).is_empty());
}

#[test]
fn activity_遍历到末页并按升序返回全部活动轮次() {
    let mut m = Machine::new(true);
    begin(&mut m, StatusQuery::Activity);
    let (actions, done) = drive(
        &mut m,
        vec![
            r(StatusPhase::Metadata, meta(T, "active")),
            r(
                StatusPhase::Turns,
                turns_page(&[("轮5", "inProgress"), ("轮4", "completed")], Some("c1")),
            ),
            r(
                StatusPhase::Turns,
                turns_page(&[("轮3", "inProgress"), ("轮2", "interrupted")], Some("c2")),
            ),
            r(
                StatusPhase::Turns,
                turns_page(&[("轮1", "inProgress")], None),
            ),
        ],
    );
    assert_eq!(
        only(&done),
        &StatusResult::Active(vec!["轮1".into(), "轮3".into(), "轮5".into()])
    );
    let pages: Vec<_> = status_requests(&actions)
        .into_iter()
        .filter(|r| r.phase == StatusPhase::Turns)
        .map(|r| r.params.unwrap())
        .collect();
    assert_eq!(pages.len(), 3);
    for (page, cursor) in pages.iter().zip([Value::Null, json!("c1"), json!("c2")]) {
        assert_eq!(
            page,
            &json!({"threadId": T, "cursor": cursor, "limit": 100,
                "sortDirection": "desc", "itemsView": "notLoaded"})
        );
    }
}

#[test]
fn activity_活动但存储页里没有活动轮次时是空集合而不是空闲() {
    for page in [
        turns_page(&[], None),
        turns_page(&[("旧轮", "completed"), ("更旧", "failed")], None),
    ] {
        let mut m = Machine::new(true);
        begin(&mut m, StatusQuery::Activity);
        let (_, done) = drive(
            &mut m,
            vec![
                r(StatusPhase::Metadata, meta(T, "active")),
                r(StatusPhase::Turns, page),
            ],
        );
        assert_eq!(only(&done), &StatusResult::Active(Vec::new()));
        assert_ne!(only(&done), &StatusResult::Idle);
    }
}

#[test]
fn 合法空末页只结束分页流() {
    let mut m = Machine::new(true);
    begin(&mut m, StatusQuery::Activity);
    let (_, done) = drive(
        &mut m,
        vec![
            r(StatusPhase::Metadata, meta(T, "active")),
            r(
                StatusPhase::Turns,
                turns_page(&[("轮2", "inProgress")], Some("c1")),
            ),
            r(StatusPhase::Turns, turns_page(&[], None)),
        ],
    );
    assert_eq!(only(&done), &StatusResult::Active(vec!["轮2".into()]));
}

#[test]
fn 页的形状不合法一律畸形() {
    let turn = json!({"id": "轮", "status": "inProgress", "items": []});
    let many: Vec<Value> = (0..101)
        .map(|i| json!({"id": format!("轮{i}"), "status": "completed"}))
        .collect();
    let pages = [
        json!({"nextCursor": null}),
        json!({"data": {}, "nextCursor": null}),
        json!({"data": null, "nextCursor": null}),
        json!({"data": [], "nextCursor": "c1"}),
        json!({"data": [turn]}),
        json!({"data": [turn], "nextCursor": ""}),
        json!({"data": [turn], "nextCursor": 5}),
        json!({"data": [turn], "nextCursor": "c".repeat(4097)}),
        json!({"data": many, "nextCursor": null}),
    ];
    for page in pages {
        let mut m = Machine::new(true);
        begin(&mut m, StatusQuery::Activity);
        let (actions, done) = drive(
            &mut m,
            vec![
                r(StatusPhase::Metadata, meta(T, "active")),
                r(StatusPhase::Turns, page.clone()),
            ],
        );
        assert_eq!(only(&done), &unknown(StatusUnknown::Malformed), "{page}");
        assert_eq!(status_requests(&actions).len(), 1, "畸形页不再续读");
    }
}

#[test]
fn 游标上限四千零九十六字节以内可用() {
    let mut m = Machine::new(true);
    begin(&mut m, StatusQuery::Activity);
    let cursor = "c".repeat(4096);
    let (_, done) = drive(
        &mut m,
        vec![
            r(StatusPhase::Metadata, meta(T, "active")),
            r(
                StatusPhase::Turns,
                turns_page(&[("轮", "completed")], Some(&cursor)),
            ),
            r(StatusPhase::Turns, turns_page(&[], None)),
        ],
    );
    assert_eq!(only(&done), &StatusResult::Active(Vec::new()));
}

#[test]
fn 轮次的形状不合法一律畸形() {
    let turns = [
        json!({"id": "轮", "status": "running"}),
        json!({"id": "轮", "status": "pending"}),
        json!({"id": "轮"}),
        json!({"id": "", "status": "completed"}),
        json!({"status": "completed"}),
        json!({"id": 7, "status": "completed"}),
        json!("轮"),
    ];
    for turn in turns {
        // 未知状态即使排在活动轮次之后也不能被忽略。
        let page =
            json!({"data": [{"id": "好轮", "status": "inProgress"}, turn], "nextCursor": null});
        let mut m = Machine::new(true);
        begin(&mut m, StatusQuery::Activity);
        let (_, done) = drive(
            &mut m,
            vec![
                r(StatusPhase::Metadata, meta(T, "active")),
                r(StatusPhase::Turns, page.clone()),
            ],
        );
        assert_eq!(only(&done), &unknown(StatusUnknown::Malformed), "{page}");
    }
}

#[test]
fn 同一分页流内重复游标是未知() {
    let mut m = Machine::new(true);
    begin(&mut m, StatusQuery::Activity);
    let (_, done) = drive(
        &mut m,
        vec![
            r(StatusPhase::Metadata, meta(T, "active")),
            r(
                StatusPhase::Turns,
                turns_page(&[("轮3", "completed")], Some("c1")),
            ),
            r(
                StatusPhase::Turns,
                turns_page(&[("轮2", "completed")], Some("c1")),
            ),
        ],
    );
    assert_eq!(only(&done), &unknown(StatusUnknown::DuplicateCursor));
}

// ─── Turn ─────────────────────────────────────────────────────────────

#[test]
fn turn_第一页命中即停_不读元数据并返回轮次壳() {
    let mut m = Machine::new(true);
    let (_, actions) = begin(&mut m, target("轮2"));
    assert_eq!(status_requests(&actions)[0].phase, StatusPhase::Turns);
    let page = json!({"data": [
        {"id": "轮3", "status": "inProgress", "items": [], "itemsView": "notLoaded", "error": null},
        {"id": "轮2", "status": "failed", "items": [agent_message("不应保留")], "itemsView": "full",
         "error": {"message": "模拟失败"}, "startedAt": 5}
    ], "nextCursor": "c1"});
    let (actions, done) = drive(&mut m, vec![r(StatusPhase::Turns, page)]);
    assert_eq!(
        only(&done),
        &StatusResult::Found {
            status: StatusTurn::Failed,
            shell: json!({"id": "轮2", "status": "failed", "items": [], "itemsView": "notLoaded",
                "error": {"message": "模拟失败"}, "startedAt": 5}),
        }
    );
    assert!(status_requests(&actions).is_empty(), "找到就停");
}

#[test]
fn turn_目标在第二页以后() {
    let mut m = Machine::new(true);
    begin(&mut m, target("轮1"));
    let (actions, done) = drive(
        &mut m,
        vec![
            r(
                StatusPhase::Turns,
                turns_page(&[("轮9", "completed")], Some("c1")),
            ),
            r(
                StatusPhase::Turns,
                turns_page(&[("轮5", "inProgress")], Some("c2")),
            ),
            r(
                StatusPhase::Turns,
                turns_page(&[("轮1", "inProgress")], Some("c3")),
            ),
        ],
    );
    assert!(matches!(
        only(&done),
        StatusResult::Found {
            status: StatusTurn::InProgress,
            ..
        }
    ));
    assert!(status_requests(&actions)
        .iter()
        .all(|r| r.phase == StatusPhase::Turns));
}

#[test]
fn turn_遍历完未找到再读元数据_非活动为不存在_活动为不可见() {
    for (status, expected) in [
        ("idle", StatusResult::Absent),
        ("notLoaded", StatusResult::Absent),
        ("systemError", StatusResult::Absent),
        ("active", unknown(StatusUnknown::NotVisible)),
        ("weird", unknown(StatusUnknown::Malformed)),
    ] {
        let mut m = Machine::new(true);
        begin(&mut m, target("目标"));
        let (actions, done) = drive(
            &mut m,
            vec![
                r(
                    StatusPhase::Turns,
                    turns_page(&[("轮2", "completed")], Some("c1")),
                ),
                r(
                    StatusPhase::Turns,
                    turns_page(&[("轮1", "completed")], None),
                ),
                r(StatusPhase::Metadata, meta(T, status)),
            ],
        );
        assert_eq!(only(&done), &expected, "{status}");
        let phases: Vec<_> = status_requests(&actions)
            .into_iter()
            .map(|r| r.phase)
            .collect();
        assert_eq!(phases, vec![StatusPhase::Turns, StatusPhase::Metadata]);
    }
}

#[test]
fn turn_第一轮前的不支持不被缓存() {
    let mut m = Machine::new(true);
    begin(&mut m, target("首轮"));
    let (_, done) = drive(
        &mut m,
        vec![fail(
            StatusPhase::Turns,
            error_reply(-32601, "list_turns is not supported yet"),
        )],
    );
    assert_eq!(only(&done), &unknown(StatusUnknown::Unsupported));
    // 下一次读取照常分页，不因上次的 -32601 跳过或降级。
    let (_, actions) = begin(&mut m, target("首轮"));
    assert_eq!(status_requests(&actions)[0].phase, StatusPhase::Turns);
    let (_, done) = drive(
        &mut m,
        vec![r(
            StatusPhase::Turns,
            turns_page(&[("首轮", "inProgress")], None),
        )],
    );
    assert!(matches!(only(&done), StatusResult::Found { .. }));
}

// ─── Settlement ───────────────────────────────────────────────────────

#[test]
fn settlement_有活动轮次取升序第一个且不读条目() {
    let mut m = Machine::new(true);
    begin(&mut m, settlement(&["候选"], &["本次输入"]));
    let (actions, done) = drive(
        &mut m,
        vec![
            r(StatusPhase::Metadata, meta(T, "active")),
            r(
                StatusPhase::Turns,
                turns_page(&[("轮3", "inProgress"), ("轮2", "inProgress")], None),
            ),
        ],
    );
    assert_eq!(only(&done), &StatusResult::FirstActive("轮2".into()));
    assert!(!status_requests(&actions)
        .iter()
        .any(|r| r.phase == StatusPhase::Items));
}

#[test]
fn settlement_活动集合为空是不可见() {
    let mut m = Machine::new(true);
    begin(&mut m, settlement(&["候选"], &["本次输入"]));
    let (_, done) = drive(
        &mut m,
        vec![
            r(StatusPhase::Metadata, meta(T, "active")),
            r(
                StatusPhase::Turns,
                turns_page(&[("旧轮", "completed")], None),
            ),
        ],
    );
    assert_eq!(only(&done), &unknown(StatusUnknown::NotVisible));
}

#[test]
fn settlement_同轮第二条用户消息才匹配时仍认出载体() {
    let mut m = Machine::new(true);
    begin(&mut m, settlement(&["前轮", "后轮"], &["  本次输入  "]));
    let (actions, done) = drive(
        &mut m,
        vec![
            r(StatusPhase::Metadata, meta(T, "idle")),
            ri(
                "前轮",
                items_page(
                    "前轮",
                    &[user_message("别的输入"), agent_message("本次输入")],
                    None,
                ),
            ),
            ri(
                "后轮",
                items_page(
                    "后轮",
                    &[user_message("第一条输入"), agent_message("回答")],
                    Some("p1"),
                ),
            ),
            ri(
                "后轮",
                items_page(
                    "后轮",
                    &[user_message("本次输入 "), agent_message("回答")],
                    None,
                ),
            ),
        ],
    );
    assert_eq!(only(&done), &StatusResult::Carrier("后轮".into()));
    let items: Vec<_> = status_requests(&actions)
        .into_iter()
        .filter(|r| r.phase == StatusPhase::Items)
        .map(|r| r.params.unwrap())
        .collect();
    assert_eq!(
        items,
        vec![
            json!({"threadId": T, "turnId": "前轮", "cursor": null, "limit": 64, "sortDirection": "asc"}),
            json!({"threadId": T, "turnId": "后轮", "cursor": null, "limit": 64, "sortDirection": "asc"}),
            json!({"threadId": T, "turnId": "后轮", "cursor": "p1", "limit": 64, "sortDirection": "asc"}),
        ]
    );
}

#[test]
fn settlement_同页第二条用户消息才匹配时仍认出载体() {
    // summary 只给每轮第一条用户消息；被吸收的输入在同一页的第二条里。
    let mut m = Machine::new(true);
    begin(&mut m, settlement(&["本轮"], &["本次输入"]));
    let (_, done) = drive(
        &mut m,
        vec![
            r(StatusPhase::Metadata, meta(T, "idle")),
            ri(
                "本轮",
                items_page(
                    "本轮",
                    &[
                        user_message("第一条输入"),
                        agent_message("回答"),
                        user_message("本次输入"),
                    ],
                    None,
                ),
            ),
        ],
    );
    assert_eq!(only(&done), &StatusResult::Carrier("本轮".into()));
}

#[test]
fn settlement_匹配即停不读后续候选() {
    let mut m = Machine::new(true);
    begin(&mut m, settlement(&["甲", "乙"], &["本次输入"]));
    let (actions, done) = drive(
        &mut m,
        vec![
            r(StatusPhase::Metadata, meta(T, "idle")),
            ri(
                "甲",
                items_page("甲", &[user_message("本次输入")], Some("p1")),
            ),
        ],
    );
    assert_eq!(only(&done), &StatusResult::Carrier("甲".into()));
    assert!(status_requests(&actions)
        .iter()
        .all(|r| r.candidate.as_deref() != Some("乙")));
}

#[test]
fn settlement_全部候选确认不匹配才是无载体() {
    let mut m = Machine::new(true);
    begin(&mut m, settlement(&["甲", "乙"], &["本次输入", "  "]));
    let (_, done) = drive(
        &mut m,
        vec![
            r(StatusPhase::Metadata, meta(T, "notLoaded")),
            ri("甲", items_page("甲", &[user_message("别的")], None)),
            ri(
                "乙",
                items_page("乙", &[user_message(""), user_message("本次")], None),
            ),
        ],
    );
    assert_eq!(only(&done), &StatusResult::NoCarrier);
}

#[test]
fn settlement_没有候选时空闲即无载体() {
    let mut m = Machine::new(true);
    begin(&mut m, settlement(&[], &["本次输入"]));
    let (actions, done) = drive(&mut m, vec![r(StatusPhase::Metadata, meta(T, "idle"))]);
    assert_eq!(only(&done), &StatusResult::NoCarrier);
    assert!(status_requests(&actions).is_empty());
}

#[test]
fn settlement_候选超过十六个不截取() {
    let names: Vec<String> = (0..17).map(|i| format!("候选{i}")).collect();
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let mut m = Machine::new(true);
    begin(&mut m, settlement(&refs, &["本次输入"]));
    let (actions, done) = drive(&mut m, vec![r(StatusPhase::Metadata, meta(T, "idle"))]);
    assert_eq!(only(&done), &unknown(StatusUnknown::PageBudget));
    assert!(status_requests(&actions).is_empty());
    // 恰好十六个照常逐个核对。
    let mut m = Machine::new(true);
    begin(&mut m, settlement(&refs[..16], &["本次输入"]));
    let (actions, done) = drive(&mut m, vec![r(StatusPhase::Metadata, meta(T, "idle"))]);
    assert!(done.is_empty());
    assert_eq!(
        status_requests(&actions)[0].candidate.as_deref(),
        Some("候选0")
    );
}

#[test]
fn settlement_阶段间版本变化返回已变化且不沿用旧的空闲结果() {
    let mut m = Machine::new(true);
    begin(&mut m, settlement(&["前轮"], &["本次输入"]));
    let (actions, done) = drive(
        &mut m,
        vec![
            r(StatusPhase::Metadata, meta(T, "idle")),
            ev(turn_event("turn/started", T, "新轮")),
            // 即使条目匹配，也不能把阶段一的 Idle 与新版本下的条目拼成结论。
            ri(
                "前轮",
                items_page("前轮", &[user_message("本次输入")], None),
            ),
        ],
    );
    assert_eq!(only(&done), &unknown(StatusUnknown::Changed));
    assert_eq!(status_requests(&actions).len(), 1, "Settlement 不重读");
}

#[test]
fn settlement_元数据阶段版本变化同样直接返回已变化() {
    let mut m = Machine::new(true);
    begin(&mut m, settlement(&["前轮"], &["本次输入"]));
    let (actions, done) = drive(
        &mut m,
        vec![
            ev(turn_event("turn/completed", T, "前轮")),
            r(StatusPhase::Metadata, meta(T, "idle")),
        ],
    );
    assert_eq!(only(&done), &unknown(StatusUnknown::Changed));
    assert!(status_requests(&actions).is_empty());
}

#[test]
fn 条目的形状不合法一律畸形而不是不匹配() {
    let user = user_message("别的");
    let pages = [
        json!({"nextCursor": null}),
        json!({"data": [], "nextCursor": "p1"}),
        json!({"data": [{"turnId": "别的轮", "item": user}], "nextCursor": null}),
        json!({"data": [{"item": user}], "nextCursor": null}),
        json!({"data": [{"turnId": "甲", "item": "文本"}], "nextCursor": null}),
        json!({"data": [{"turnId": "甲"}], "nextCursor": null}),
        json!({"data": [{"turnId": "甲", "item": {"id": "x"}}], "nextCursor": null}),
        json!({"data": [{"turnId": "甲", "item": {"type": "userMessage", "content": "本次输入"}}], "nextCursor": null}),
        json!({"data": [{"turnId": "甲", "item": {"type": "userMessage"}}], "nextCursor": null}),
        json!({"data": [{"turnId": "甲", "item": {"type": "userMessage", "content": [{"type": "text"}]}}], "nextCursor": null}),
        json!({"data": [{"turnId": "甲", "item": {"type": "userMessage", "content": [{"type": "text", "text": 5}]}}], "nextCursor": null}),
        json!({"data": [{"turnId": "甲", "item": {"type": "userMessage", "content": ["本次输入"]}}], "nextCursor": null}),
    ];
    for page in pages {
        let mut m = Machine::new(true);
        begin(&mut m, settlement(&["甲", "乙"], &["本次输入"]));
        let (actions, done) = drive(
            &mut m,
            vec![
                r(StatusPhase::Metadata, meta(T, "idle")),
                ri("甲", page.clone()),
            ],
        );
        assert_eq!(only(&done), &unknown(StatusUnknown::Malformed), "{page}");
        assert!(
            !status_requests(&actions)
                .iter()
                .any(|r| r.candidate.as_deref() == Some("乙")),
            "畸形条目不能当作不匹配而换下一个候选"
        );
    }
}

#[test]
fn 条目里非文本部分与非用户消息不影响判定() {
    let mut m = Machine::new(true);
    begin(&mut m, settlement(&["甲"], &["本次输入"]));
    let image = json!({"type": "userMessage", "id": "u1", "content": [
        {"type": "image", "url": "data:"}, {"type": "text", "text": "本次输入"}]});
    let (_, done) = drive(
        &mut m,
        vec![
            r(StatusPhase::Metadata, meta(T, "idle")),
            ri(
                "甲",
                items_page(
                    "甲",
                    &[json!({"type": "reasoning", "id": "r1"}), image],
                    None,
                ),
            ),
        ],
    );
    assert_eq!(only(&done), &StatusResult::Carrier("甲".into()));
}

#[test]
fn 条目分页不支持是未知() {
    let mut m = Machine::new(true);
    begin(&mut m, settlement(&["甲"], &["本次输入"]));
    let (_, done) = drive(
        &mut m,
        vec![
            r(StatusPhase::Metadata, meta(T, "idle")),
            StatusStep::Reply(
                expect_items("甲"),
                Err(error_reply(-32601, "not supported")),
            ),
        ],
    );
    assert_eq!(only(&done), &unknown(StatusUnknown::Unsupported));
}

#[test]
fn 换候选时游标集合重置() {
    let mut m = Machine::new(true);
    begin(&mut m, settlement(&["甲", "乙"], &["本次输入"]));
    let (_, done) = drive(
        &mut m,
        vec![
            r(StatusPhase::Metadata, meta(T, "idle")),
            ri("甲", items_page("甲", &[user_message("a")], Some("p1"))),
            ri("甲", items_page("甲", &[user_message("b")], None)),
            // 新候选是新的分页流，同名游标合法。
            ri("乙", items_page("乙", &[user_message("c")], Some("p1"))),
            ri("乙", items_page("乙", &[user_message("d")], None)),
        ],
    );
    assert_eq!(only(&done), &StatusResult::NoCarrier);
}

// ─── 预算 ─────────────────────────────────────────────────────────────

/// 回 `pages` 个都带新游标的轮次页，返回累计请求数与完成。
fn feed_turn_pages(m: &mut Machine, pages: usize, prefix: &str) -> (usize, Vec<StatusDone>) {
    let script = (0..pages)
        .map(|i| {
            r(
                StatusPhase::Turns,
                turns_page(&[("旧轮", "completed")], Some(&format!("{prefix}{i}"))),
            )
        })
        .collect();
    let (actions, done) = drive(m, script);
    (status_requests(&actions).len(), done)
}

#[test]
fn 一次读取最多一千页_含元数据读() {
    let mut m = Machine::new(true);
    begin(&mut m, StatusQuery::Activity);
    let (_, done) = drive(&mut m, vec![r(StatusPhase::Metadata, meta(T, "active"))]);
    assert!(done.is_empty());
    // 元数据 1 次 + 轮次页 999 次 = 1000 次请求；第 999 页仍带游标时不能再发。
    let (sent, done) = feed_turn_pages(&mut m, 999, "c");
    assert_eq!(sent, 998);
    assert_eq!(only(&done), &unknown(StatusUnknown::PageBudget));
    assert!(m.status_reads.reads.is_empty());
}

#[test]
fn 换候选不清零页数预算() {
    let mut m = Machine::new(true);
    begin(&mut m, settlement(&["甲", "乙"], &["本次输入"]));
    let mut script = vec![r(StatusPhase::Metadata, meta(T, "idle"))];
    // 甲：997 页带游标 + 末页 = 998 次；累计 999 次。
    for i in 0..997 {
        script.push(ri(
            "甲",
            items_page("甲", &[agent_message("过渡")], Some(&format!("p{i}"))),
        ));
    }
    script.push(ri("甲", items_page("甲", &[user_message("别的")], None)));
    // 乙：第 1000 次请求，回包仍带游标时超出预算。
    script.push(ri(
        "乙",
        items_page("乙", &[user_message("别的")], Some("q0")),
    ));
    let (actions, done) = drive(&mut m, script);
    // 第 2–1000 次请求在本段发出。
    assert_eq!(status_requests(&actions).len(), 999);
    assert_eq!(only(&done), &unknown(StatusUnknown::PageBudget));
}

#[test]
fn 重读不清零页数预算() {
    let mut m = Machine::new(true);
    begin(&mut m, StatusQuery::Activity);
    drive(&mut m, vec![r(StatusPhase::Metadata, meta(T, "active"))]);
    let (sent, done) = feed_turn_pages(&mut m, 996, "c");
    // 累计已发 998 次（元数据 1 + 轮次页 997），第 998 次在途。
    assert_eq!((sent, done.len()), (996, 0));
    let (actions, done) = drive(
        &mut m,
        vec![
            ev(turn_event("turn/started", T, "新轮")),
            // 第 998 次的回包遇到版本变化：第 999 次重读元数据，第 1000 次重读第一页。
            r(
                StatusPhase::Turns,
                turns_page(&[("旧轮", "completed")], Some("x")),
            ),
            r(StatusPhase::Metadata, meta(T, "active")),
            // 第一页仍带游标：已满 1000 次，不能再发。
            r(
                StatusPhase::Turns,
                turns_page(&[("旧轮", "completed")], Some("y1")),
            ),
        ],
    );
    assert_eq!(status_requests(&actions).len(), 2);
    assert_eq!(only(&done), &unknown(StatusUnknown::PageBudget));
}

// ─── 版本 ─────────────────────────────────────────────────────────────

#[test]
fn 版本变化从第一步重读最多三次() {
    let mut m = Machine::new(true);
    begin(&mut m, StatusQuery::Activity);
    let mut script = Vec::new();
    for i in 0..3 {
        script.push(ev(turn_event("turn/started", T, &format!("轮{i}"))));
        script.push(r(StatusPhase::Metadata, meta(T, "idle")));
    }
    let (actions, done) = drive(&mut m, script);
    assert!(done.is_empty(), "三次重读内不完成");
    let phases: Vec<_> = status_requests(&actions)
        .into_iter()
        .map(|r| r.phase)
        .collect();
    assert_eq!(phases, vec![StatusPhase::Metadata; 3]);
    // 第四次变化：返回 Changed，不再重读。
    let (actions, done) = drive(
        &mut m,
        vec![
            ev(turn_event("turn/completed", T, "轮0")),
            r(StatusPhase::Metadata, meta(T, "idle")),
        ],
    );
    assert_eq!(only(&done), &unknown(StatusUnknown::Changed));
    assert!(status_requests(&actions).is_empty());
}

#[test]
fn 重读后版本稳定照常完成() {
    let mut m = Machine::new(true);
    begin(&mut m, StatusQuery::Activity);
    let (_, done) = drive(
        &mut m,
        vec![
            r(StatusPhase::Metadata, meta(T, "active")),
            r(
                StatusPhase::Turns,
                turns_page(&[("轮1", "inProgress")], Some("c1")),
            ),
            ev(turn_event("turn/started", T, "轮2")),
            // 页间换代：旧页作废，从元数据重读，游标集合重置（c1 再次出现合法）。
            r(
                StatusPhase::Turns,
                turns_page(&[("轮0", "inProgress")], None),
            ),
            r(StatusPhase::Metadata, meta(T, "active")),
            r(
                StatusPhase::Turns,
                turns_page(&[("轮2", "inProgress"), ("轮1", "inProgress")], Some("c1")),
            ),
            r(StatusPhase::Turns, turns_page(&[], None)),
        ],
    );
    assert_eq!(
        only(&done),
        &StatusResult::Active(vec!["轮1".into(), "轮2".into()])
    );
}

#[test]
fn turn_版本变化从第一页重读() {
    let mut m = Machine::new(true);
    begin(&mut m, target("目标"));
    let (actions, done) = drive(
        &mut m,
        vec![
            r(
                StatusPhase::Turns,
                turns_page(&[("轮1", "completed")], Some("c1")),
            ),
            ev(turn_event("turn/completed", T, "轮1")),
            r(
                StatusPhase::Turns,
                turns_page(&[("目标", "inProgress")], None),
            ),
            r(
                StatusPhase::Turns,
                turns_page(&[("目标", "completed")], None),
            ),
        ],
    );
    assert!(matches!(
        only(&done),
        StatusResult::Found {
            status: StatusTurn::Completed,
            ..
        }
    ));
    let cursors: Vec<_> = status_requests(&actions)
        .into_iter()
        .map(|r| r.params.unwrap()["cursor"].clone())
        .collect();
    assert_eq!(cursors, vec![json!("c1"), Value::Null]);
}

#[test]
fn 只有本线程的生命周期通知推进版本() {
    let mut m = Machine::new(true);
    begin(&mut m, StatusQuery::Activity);
    let (actions, done) = drive(
        &mut m,
        vec![
            ev(turn_event("turn/started", "别的线程", "轮")),
            ev(Event::Notification {
                method: "item/started".into(),
                params: Some(
                    json!({"threadId": T, "turnId": "轮", "item": {"type": "agentMessage", "id": "a"}}),
                ),
            }),
            r(StatusPhase::Metadata, meta(T, "idle")),
        ],
    );
    assert_eq!(only(&done), &StatusResult::Idle);
    assert!(status_requests(&actions).is_empty());
}

#[test]
fn 版本只为有在途读取的线程登记() {
    let mut m = Machine::new(true);
    drive(&mut m, vec![ev(turn_event("turn/started", T, "轮"))]);
    assert!(!m.status_reads.tracks(T));
    let (first, _) = begin(&mut m, StatusQuery::Busy);
    let (second, _) = begin(&mut m, StatusQuery::Busy);
    assert!(first > 0 && second > first);
    assert!(m.status_reads.tracks(T));
    drive(
        &mut m,
        vec![StatusStep::Reply(
            expect(StatusPhase::Metadata).of(first),
            Ok(meta(T, "idle")),
        )],
    );
    assert!(m.status_reads.tracks(T), "同线程还有在途读取");
    drive(
        &mut m,
        vec![StatusStep::Reply(
            expect(StatusPhase::Metadata).of(second),
            Ok(meta(T, "idle")),
        )],
    );
    assert!(!m.status_reads.tracks(T));
}

#[test]
fn 设置期间缓冲的通知在回放时不重复推进版本() {
    let mut m = Machine::new(true);
    m.record(T).subscribed = true;
    let (next, actions) = step(
        std::mem::take(&mut m),
        Event::ClientRequest {
            request: crate::claim::ClientRequest {
                conn: 1,
                id: json!(42),
                method: "turn/start".into(),
                params: Some(
                    json!({"threadId": T, "input": [{"type": "text", "text": "本次输入"}]}),
                ),
            },
            run_id: Some("run-测试-000001".into()),
        },
    );
    m = next;
    let stream = actions
        .iter()
        .find_map(|a| match a {
            Action::Request(c) if matches!(c.operation, Operation::Stream { .. }) => {
                Some(c.clone())
            }
            _ => None,
        })
        .expect("流式请求");
    let (next, actions) = step(
        std::mem::take(&mut m),
        Event::Reply {
            context: stream,
            result: Ok(json!({"turn": {"id": "本轮"}})),
        },
    );
    m = next;
    assert!(m.claims[T].settling(), "共享模式回包后进入 settling");
    let settlement_read = actions
        .iter()
        .find_map(|a| match a {
            Action::Request(c) if matches!(c.operation, Operation::SettlementRead { .. }) => {
                Some(c.clone())
            }
            _ => None,
        })
        .expect("吸收判定读");
    begin(&mut m, StatusQuery::Busy);
    let before = m.status_reads.version(T);
    drive(
        &mut m,
        vec![
            ev(turn_event("turn/completed", T, "前轮")),
            ev(turn_event("turn/started", T, "他轮")),
        ],
    );
    assert_eq!(m.status_reads.version(T), before + 2, "到达时各推进一次");
    let (next, _) = step(
        std::mem::take(&mut m),
        Event::Reply {
            context: settlement_read,
            result: Ok(json!({"thread": {"turns": [{"id": "本轮", "status": "inProgress"}]}})),
        },
    );
    m = next;
    assert!(!m.claims[T].settling(), "定案后回放缓冲");
    assert_eq!(m.status_reads.version(T), before + 2, "回放不再推进");
}

// ─── 期限、无响应、迟到、关闭 ───────────────────────────────────────

#[test]
fn 总期限二十秒从首次发起算起且重读不续期() {
    let mut m = Machine::new(true);
    let (id, actions) = begin(&mut m, StatusQuery::Activity);
    let schedules: Vec<_> = actions
        .iter()
        .filter_map(|a| match a {
            Action::Schedule {
                kind,
                delay_ms,
                token,
            } => Some((kind.clone(), *delay_ms, token.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(schedules.len(), 1);
    assert_eq!(
        schedules[0].0,
        TimerKind::StatusReadDeadline { read_id: id }
    );
    assert_eq!(schedules[0].1, 20_000);
    assert_eq!(
        schedules[0].0.key(&schedules[0].2),
        Some(TimerKey::StatusRead(id))
    );
    let (actions, _) = drive(
        &mut m,
        vec![
            ev(turn_event("turn/started", T, "轮")),
            r(StatusPhase::Metadata, meta(T, "active")),
            r(StatusPhase::Metadata, meta(T, "active")),
        ],
    );
    assert!(
        !actions.iter().any(|a| matches!(a, Action::Schedule { .. })),
        "重读与翻页都不续期"
    );
    let stale = live_status_requests(&m);
    let timer = deadline_event(&m, id);
    let (actions, done) = drive(&mut m, vec![ev(timer.clone())]);
    assert_eq!(only(&done), &unknown(StatusUnknown::Timeout));
    assert!(actions.contains(&Action::Unschedule {
        key: TimerKey::StatusRead(id)
    }));
    // 到期后的回包与重复到期都不再回调。
    let (actions, done) = drive(
        &mut m,
        vec![
            StatusStep::Reply(stale[0].clone(), Ok(turns_page(&[], None))),
            ev(timer),
        ],
    );
    assert!(done.is_empty());
    assert!(debug_contains(&actions, "已完成或已取消"));
}

#[test]
fn 正常完成时撤销期限计时() {
    let mut m = Machine::new(true);
    let (id, _) = begin(&mut m, StatusQuery::Busy);
    let (actions, _) = drive(&mut m, vec![r(StatusPhase::Metadata, meta(T, "idle"))]);
    assert!(actions.contains(&Action::Unschedule {
        key: TimerKey::StatusRead(id)
    }));
}

#[test]
fn 迟到与重复回包按代号丢弃() {
    let mut m = Machine::new(true);
    let (_, actions) = begin(&mut m, StatusQuery::Activity);
    let first = status_requests(&actions).remove(0);
    let actions = reply_status(&mut m, &first, Ok(meta(T, "active")));
    let page = status_requests(&actions).remove(0);
    assert_ne!(page.generation, first.generation);
    // 旧请求的重复回包：丢弃，不改变在途请求。
    let actions = reply_status(&mut m, &first, Ok(meta(T, "idle")));
    assert!(debug_contains(&actions, "代号不符"));
    assert_eq!(live_status_requests(&m), vec![page.clone()]);
    let actions = reply_status(&mut m, &page, Ok(turns_page(&[("轮", "inProgress")], None)));
    assert!(status_requests(&actions).is_empty());
    assert_eq!(m.status_reads.observed.len(), 1);
    // 同一页的重复回包：读取已完成，丢弃。
    let actions = reply_status(&mut m, &page, Ok(turns_page(&[], None)));
    assert!(debug_contains(&actions, "已完成或已取消"));
    assert_eq!(m.status_reads.observed.len(), 1);
    assert_eq!(
        m.status_reads.observed[0].result,
        StatusResult::Active(vec!["轮".into()])
    );
}

#[test]
fn 单次请求无响应以无响应完成且迟到回包丢弃() {
    let mut m = Machine::new(true);
    begin(&mut m, StatusQuery::Activity);
    let request = live_status_requests(&m).remove(0);
    let (actions, done) = drive(
        &mut m,
        vec![ev(Event::TimedOut {
            tag: 7,
            deadline: Instant::now(),
            context: request.context.clone(),
            error: error_reply(-32001, "请求超时"),
        })],
    );
    assert_eq!(only(&done), &unknown(StatusUnknown::Unresponsive));
    assert!(!m.app_server_unresponsive, "续跑预检的超时不置无响应");
    assert!(status_requests(&actions).is_empty());
    let (_, done) = drive(
        &mut m,
        vec![ev(Event::LateReply {
            context: request.context,
            result: Ok(meta(T, "idle")),
        })],
    );
    assert!(done.is_empty());
}

fn late_start(generation: u64, round: u64, late_settling: bool) -> crate::stop::LateStart {
    crate::stop::LateStart {
        stream_id: None,
        review_id: None,
        token: ClaimToken {
            thread_id: T.into(),
            claim_id: 1,
        },
        method: "turn/start".into(),
        generation,
        round,
        reconciling: true,
        turn: None,
        target_thread: T.into(),
        interrupting: false,
        wire_tag: 1,
        wire_available: true,
        deadline: Instant::now(),
        review_pending: false,
        ownership_proven: false,
        late_settling,
        reply_received: false,
        goal_round: 0,
        goal_busy: false,
        goal_done: false,
        goal_version: 0,
        goal_waiting_for_owner: false,
        goal_change_id: None,
        reconcile_delay_ms: 0,
        cleanup_due: false,
    }
}

/// 直接走 `timeout_reply`（与 step 的超时入口相同），不让伪造的登记进入停止驱动。
fn time_out(m: &mut Machine, caller: StatusCaller) -> (Vec<Action>, StatusResult) {
    let (id, actions) = begin_with(m, StatusQuery::Activity, caller);
    let request = status_requests(&actions).remove(0);
    let mut actions = Vec::new();
    m.timeout_reply(
        request.context,
        error_reply(-32001, "请求超时"),
        9,
        Instant::now(),
        &mut actions,
    );
    let done = m.status_reads.observed.pop().expect("超时完成读取");
    assert_eq!(done.read_id, id);
    (actions, done.result)
}

#[test]
fn 对账与迟到归属的无响应保护在状态读取下仍然生效() {
    let warned = |actions: &[Action]| {
        actions
            .iter()
            .any(|a| matches!(a, Action::Warn(text) if text.contains("Codex 无响应")))
    };
    // 当前的对账登记：置无响应。
    let mut m = Machine::new(true);
    m.stops.reconciliations.insert(3, late_start(3, 2, false));
    let (actions, result) = time_out(
        &mut m,
        StatusCaller::Reconcile {
            generation: 3,
            round: 2,
        },
    );
    assert_eq!(result, unknown(StatusUnknown::Unresponsive));
    assert!(m.app_server_unresponsive && warned(&actions));
    // 当前的迟到归属登记：置无响应。
    let mut m = Machine::new(true);
    m.stops.late.push_back(late_start(4, 1, true));
    let (actions, _) = time_out(
        &mut m,
        StatusCaller::LateSettlement {
            generation: 4,
            round: 1,
        },
    );
    assert!(m.app_server_unresponsive && warned(&actions));
    // 已换轮、未在迟到归属中或其他调用方：只完成读取，不置无响应。
    for (caller, reg) in [
        (
            StatusCaller::Reconcile {
                generation: 3,
                round: 1,
            },
            Some(late_start(3, 2, false)),
        ),
        (
            StatusCaller::LateSettlement {
                generation: 4,
                round: 1,
            },
            Some(late_start(4, 1, false)),
        ),
        (StatusCaller::JoinedCheck { attempt: 0 }, None),
    ] {
        let mut m = Machine::new(true);
        if let Some(reg) = reg {
            m.stops.reconciliations.insert(reg.generation, reg.clone());
            m.stops.late.push_back(reg);
        }
        let (actions, result) = time_out(&mut m, caller.clone());
        assert_eq!(result, unknown(StatusUnknown::Unresponsive));
        assert!(
            !m.app_server_unresponsive && !warned(&actions),
            "{caller:?}"
        );
    }
}

#[test]
fn 关闭时静默取消在途读取() {
    let mut m = Machine::new(true);
    let (id, _) = begin(&mut m, StatusQuery::Activity);
    let request = live_status_requests(&m).remove(0);
    let (actions, done) = drive(&mut m, vec![ev(Event::Shutdown)]);
    assert!(done.is_empty(), "关闭不回调");
    assert!(m.status_reads.reads.is_empty());
    assert!(!m.status_reads.tracks(T));
    assert!(actions.contains(&Action::Unschedule {
        key: TimerKey::StatusRead(id)
    }));
    let (_, done) = drive(
        &mut m,
        vec![StatusStep::Reply(request, Ok(meta(T, "idle")))],
    );
    assert!(done.is_empty());
    // 关闭后发起的读取不发请求、不回调，编号仍唯一非零。
    let (later, actions) = begin(&mut m, StatusQuery::Busy);
    assert!(later > id);
    assert!(status_requests(&actions).is_empty());
    assert!(m.status_reads.observed.is_empty());
}

#[test]
fn 完成按调用方分派到各处理函数() {
    for (caller, warning) in [
        (
            StatusCaller::ContinueCheck { round: 1 },
            "H2 调用点尚未迁移",
        ),
        (StatusCaller::Settlement { attempt: 0 }, "H2 调用点尚未迁移"),
    ] {
        let mut m = Machine::new(true);
        begin_with(&mut m, StatusQuery::Busy, caller.clone());
        let (actions, done) = drive(&mut m, vec![r(StatusPhase::Metadata, meta(T, "idle"))]);
        assert_eq!(done[0].caller, caller);
        assert_eq!(done[0].thread, T);
        assert!(actions
            .iter()
            .any(|a| matches!(a, Action::Warn(text) if text == warning)));
    }
}

#[test]
fn 单步驱动只回答本层请求() {
    let mut m = Machine::new(true);
    begin(&mut m, StatusQuery::Busy);
    let unrelated = StatusRequest {
        read_id: 99,
        generation: 99,
        ..expect(StatusPhase::Metadata)
    };
    // 生成号非零的旧快照原样回包：读取不存在，只记调试日志。
    let mut stale = unrelated.clone();
    stale.context.operation = Operation::StatusRead {
        read_id: 99,
        generation: 99,
    };
    let (actions, done) = drive(
        &mut m,
        vec![StatusStep::Reply(stale, Ok(meta(T, "active")))],
    );
    assert!(done.is_empty());
    assert!(debug_contains(&actions, "已完成或已取消"));
    assert_eq!(live_status_requests(&m).len(), 1);
}
