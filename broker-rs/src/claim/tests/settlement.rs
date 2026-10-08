//! 吸收判定的证据顺序、重试与提交回放测试（详细设计 3.4 节）。
//!
//! 服务器读取经 H2 状态读取层的 Settlement 组合查询（H2 接口第 3.4、6 节），用单步驱动逐页回包。

use super::*;
use crate::claim::status_read::{StatusCaller, StatusQuery};
use crate::test_support::status_read::{
    agent_message, drive_status, error_reply, expect, expect_items, items_page, meta,
    status_requests, turn_event, turns_page, user_message, StatusPhase, StatusRequest, StatusStep,
};

/// 吸收判定读取的第一个请求：只读元数据，不带 includeTurns（H2 接口第 5 节）。
pub(super) fn settlement_request(actions: &[Action]) -> StatusRequest {
    assert_bounded(actions);
    let requests = status_requests(actions);
    assert_eq!(requests.len(), 1, "只发一个吸收判定请求：{actions:?}");
    let request = requests.into_iter().next().unwrap();
    assert_eq!(request.phase, StatusPhase::Metadata);
    request
}

/// 本路发出的请求都不得带 includeTurns，轮次页只用 notLoaded。
pub(super) fn assert_bounded(actions: &[Action]) {
    for action in actions {
        if let Action::Request(c) = action {
            let Some(params) = &c.params else { continue };
            assert!(
                params.get("includeTurns").is_none(),
                "不得请求全量历史：{} {params}",
                c.method
            );
            if c.method == "thread/turns/list" {
                assert_eq!(params["itemsView"], "notLoaded");
            }
        }
    }
}

/// 回答唯一在途的吸收判定读取：线程活动，活动轮次按升序给出（服务器页按倒序）。
pub(super) fn settle_active(machine: &mut Machine, active: &[&str]) -> Vec<Action> {
    let page: Vec<(&str, &str)> = active.iter().rev().map(|id| (*id, "inProgress")).collect();
    let (actions, _) = drive_status(
        machine,
        vec![
            StatusStep::Reply(expect(StatusPhase::Metadata), Ok(meta("线程", "active"))),
            StatusStep::Reply(expect(StatusPhase::Turns), Ok(turns_page(&page, None))),
        ],
    );
    assert_bounded(&actions);
    actions
}

/// 回答唯一在途的吸收判定读取：线程空闲，按顺序给出各候选的单页条目。
fn settle_idle(machine: &mut Machine, candidates: &[(&str, Vec<Value>)]) -> Vec<Action> {
    let mut script = vec![StatusStep::Reply(
        expect(StatusPhase::Metadata),
        Ok(meta("线程", "idle")),
    )];
    for (turn, items) in candidates {
        script.push(StatusStep::Reply(
            expect_items(turn),
            Ok(items_page(turn, items, None)),
        ));
    }
    let (actions, _) = drive_status(machine, script);
    assert_bounded(&actions);
    actions
}

fn read_for(machine: &mut Machine, context: &RequestContext, turn_id: &str) -> StatusRequest {
    let actions = reply(machine, context, Ok(json!({"turn": {"id": turn_id}})));
    settlement_request(&actions)
}

fn settlement_query(machine: &Machine) -> (Vec<String>, Vec<String>) {
    let reads: Vec<_> = machine
        .status_reads
        .reads
        .values()
        .filter(|r| matches!(r.caller, StatusCaller::Settlement { .. }))
        .collect();
    assert_eq!(reads.len(), 1, "应只有一个吸收判定读取在途");
    match &reads[0].query {
        StatusQuery::Settlement { candidates, input } => (candidates.clone(), input.clone()),
        other => panic!("吸收判定应发 Settlement 查询：{other:?}"),
    }
}

fn retry_timer(actions: &[Action], expected: u8) -> Event {
    let (token, kind) = actions
        .iter()
        .find_map(|a| match a {
            Action::Schedule {
                token,
                kind: kind @ TimerKind::SettlementRetry { attempt },
                delay_ms,
            } => {
                assert_eq!(*delay_ms, 1000);
                assert_eq!(*attempt, expected);
                Some((token.clone(), kind.clone()))
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("应安排第 {expected} 次重试：{actions:?}"));
    Event::Timer { token, kind }
}

fn absorbed(actions: &[Action]) -> Option<Value> {
    sends(actions)
        .into_iter()
        .find(|(_, m)| m["id"] == 42)
        .and_then(|(_, m)| m["result"].get("absorbedBy").cloned())
}

fn user(text: &str) -> Value {
    user_message(text)
}

#[test]
fn 缓冲完成在定案回放时恰好记账一次() {
    let (mut machine, context) = start(true, "turn/start");
    apply(&mut machine, turn("turn/completed", "本轮"));
    apply(&mut machine, turn("turn/completed", "本轮"));
    assert!(machine.records["线程"].ended_turns.is_empty());
    read_for(&mut machine, &context, "本轮");
    assert!(settlement_query(&machine).0.is_empty(), "本轮不是候选");
    let actions = settle_active(&mut machine, &["本轮"]);
    assert!(machine.claims.is_empty());
    assert_eq!(
        machine.records["线程"].ended_turns,
        VecDeque::from(["本轮".into()])
    );
    assert_eq!(
        sends(&actions)
            .iter()
            .filter(|(_, m)| m["method"] == "turn/completed")
            .count(),
        1
    );
    assert!(sends(&actions)
        .iter()
        .any(|(_, m)| m["id"] == 42 && m["result"]["absorbedBy"].is_null()));
}

#[test]
fn 自有故事开始前的其他完成只转发不记账或释放() {
    let (mut machine, context) = start(true, "turn/start");
    apply(&mut machine, turn("turn/completed", "前轮"));
    apply(&mut machine, turn("turn/started", "本轮"));
    let actions = reply(&mut machine, &context, Ok(json!({"turn": {"id": "本轮"}})));
    assert!(machine.records["线程"].ended_turns.is_empty());
    assert!(machine.claims.contains_key("线程"));
    assert!(sends(&actions)
        .iter()
        .any(|(_, m)| m["method"] == "turn/completed" && m["params"]["turn"]["id"] == "前轮"));
    assert!(!actions.iter().any(|a| matches!(a, Action::Request(_))));
}

#[test]
fn 自有故事开始后的目标续跑按顺序提交() {
    let (mut machine, context) = start(true, "turn/start");
    apply(&mut machine, turn("turn/started", "本轮"));
    apply(
        &mut machine,
        notify(
            "thread/goal/updated",
            json!({"threadId": "线程", "goal": {"status": "active"}}),
        ),
    );
    apply(&mut machine, turn("turn/completed", "本轮"));
    apply(&mut machine, turn("turn/started", "续轮"));
    reply(&mut machine, &context, Ok(json!({"turn": {"id": "本轮"}})));
    assert!(machine.records["线程"].goal_active);
    assert_eq!(machine.records["线程"].ended_turns.len(), 1);
    assert_eq!(machine.claims["线程"].turn_id(), Some("续轮"));
}

#[test]
fn 加入定案后目标轮次的缓冲完成释放认领() {
    let (mut machine, context) = start(true, "turn/start");
    apply(&mut machine, turn("turn/completed", "外轮"));
    apply(&mut machine, turn("turn/completed", "其他轮"));
    read_for(&mut machine, &context, "本轮");
    let actions = settle_active(&mut machine, &["外轮"]);
    assert!(machine.claims.is_empty());
    assert_eq!(
        machine.records["线程"].ended_turns,
        VecDeque::from(["外轮".into()])
    );
    assert_eq!(absorbed(&actions), Some(json!("外轮")));
}

#[test]
fn 多个活动轮次取升序第一个() {
    for (active, joined) in [(vec!["本轮", "后轮"], false), (vec!["外轮", "本轮"], true)] {
        let (mut machine, context) = start(true, "turn/start");
        read_for(&mut machine, &context, "本轮");
        let actions = settle_active(&mut machine, &active);
        let expected = if joined { json!("外轮") } else { Value::Null };
        assert_eq!(absorbed(&actions), Some(expected));
        assert_eq!(machine.claims["线程"].joined(), joined);
        assert_eq!(machine.claims["线程"].turn_id(), Some(active[0]));
    }
}

#[test]
fn 读到本轮活跃优先于期间到达的前轮完成() {
    let (mut machine, context) = start(true, "turn/start");
    let read = read_for(&mut machine, &context, "本轮");
    apply(&mut machine, turn("turn/completed", "前轮"));
    // 期间到达的完成推进了活动版本：首个回包只得到「已变化」，立即用最新 held 重算候选重发。
    let actions = reply(&mut machine, &read.context, Ok(meta("线程", "active")));
    assert!(machine.claims["线程"].settling());
    let reissued = settlement_request(&actions);
    assert_ne!(reissued.read_id, read.read_id);
    assert_eq!(settlement_query(&machine).0, vec!["前轮".to_string()]);
    let actions = settle_active(&mut machine, &["本轮"]);
    assert!(!machine.claims["线程"].joined());
    assert!(machine.records["线程"].ended_turns.is_empty());
    assert_eq!(absorbed(&actions), Some(Value::Null));
}

#[test]
fn 用户输入载体与提交后完成候选相交才判加入() {
    for text in ["本次输入", "包含本次输入的其他内容"] {
        let (mut machine, context) = start(true, "turn/start");
        apply(&mut machine, turn("turn/completed", "外轮"));
        read_for(&mut machine, &context, "本轮");
        assert_eq!(
            settlement_query(&machine),
            (vec!["外轮".to_string()], vec!["  本次输入  ".to_string()])
        );
        let actions = settle_idle(&mut machine, &[("外轮", vec![user(text)])]);
        let expected = if text == "本次输入" {
            json!("外轮")
        } else {
            Value::Null
        };
        assert_eq!(absorbed(&actions), Some(expected));
        assert_eq!(machine.claims.is_empty(), text == "本次输入");
    }
}

#[test]
fn 被吸收的输入是同一轮的第二条用户消息且摘要看不到() {
    // 摘要只含第一条 userMessage 与最后一条 agentMessage；本次输入是第二条，必须读全部条目。
    for same_page in [false, true] {
        let (mut machine, context) = start(true, "turn/start");
        apply(&mut machine, turn("turn/completed", "外轮"));
        read_for(&mut machine, &context, "本轮");
        let first = vec![user("别人先发的输入"), agent_message("中间回答")];
        let second = vec![user("本次输入"), agent_message("最后回答")];
        let script = if same_page {
            vec![
                StatusStep::Reply(expect(StatusPhase::Metadata), Ok(meta("线程", "idle"))),
                StatusStep::Reply(
                    expect_items("外轮"),
                    Ok(items_page("外轮", &[first, second].concat(), None)),
                ),
            ]
        } else {
            vec![
                StatusStep::Reply(expect(StatusPhase::Metadata), Ok(meta("线程", "idle"))),
                StatusStep::Reply(
                    expect_items("外轮"),
                    Ok(items_page("外轮", &first, Some("第二页"))),
                ),
                StatusStep::Reply(expect_items("外轮"), Ok(items_page("外轮", &second, None))),
            ]
        };
        let (actions, _) = drive_status(&mut machine, script);
        assert_bounded(&actions);
        assert_eq!(absorbed(&actions), Some(json!("外轮")), "同页={same_page}");
        assert!(machine.claims.is_empty(), "外轮的缓冲完成释放加入的认领");
    }
}

#[test]
fn 候选按通知原顺序逐个核对且匹配即停() {
    let (mut machine, context) = start(true, "turn/start");
    for id in ["甲轮", "乙轮", "甲轮", "丙轮"] {
        apply(&mut machine, turn("turn/completed", id));
    }
    read_for(&mut machine, &context, "本轮");
    assert_eq!(
        settlement_query(&machine).0,
        vec!["甲轮".to_string(), "乙轮".into(), "丙轮".into()],
        "保持原顺序，重复完成只列一次"
    );
    let (actions, _) = drive_status(
        &mut machine,
        vec![
            StatusStep::Reply(expect(StatusPhase::Metadata), Ok(meta("线程", "idle"))),
            StatusStep::Reply(
                expect_items("甲轮"),
                Ok(items_page("甲轮", &[user("别的输入")], None)),
            ),
            StatusStep::Reply(
                expect_items("乙轮"),
                Ok(items_page("乙轮", &[user("本次输入")], None)),
            ),
        ],
    );
    assert_eq!(absorbed(&actions), Some(json!("乙轮")));
    assert!(status_requests(&actions)
        .iter()
        .all(|r| r.candidate.as_deref() != Some("丙轮")));
}

#[test]
fn 候选超过十六个不截取而走有界重试() {
    let (mut machine, context) = start(true, "turn/start");
    for i in 0..17 {
        apply(&mut machine, turn("turn/completed", &format!("外轮{i}")));
    }
    read_for(&mut machine, &context, "本轮");
    assert_eq!(settlement_query(&machine).0.len(), 17);
    // 第十七个候选才携带本次输入；截取前十六个会错判为自有。
    let (actions, done) = drive_status(
        &mut machine,
        vec![StatusStep::Reply(
            expect(StatusPhase::Metadata),
            Ok(meta("线程", "idle")),
        )],
    );
    assert!(status_requests(&actions).is_empty(), "不读任何条目");
    assert_eq!(done.len(), 1);
    assert!(machine.claims["线程"].settling(), "预算不足不是 NoCarrier");
    assert!(!machine.claims["线程"].joined());
    let response = sends(&actions);
    assert_eq!(response[0].1["id"], 42);
    assert!(response[0].1["result"].get("absorbedBy").is_none());
    retry_timer(&actions, 1);
}

#[test]
fn 十六个候选全部确认不匹配才按自有定案() {
    let (mut machine, context) = start(true, "turn/start");
    for i in 0..16 {
        apply(&mut machine, turn("turn/completed", &format!("外轮{i}")));
    }
    read_for(&mut machine, &context, "本轮");
    let candidates: Vec<(String, Vec<Value>)> = (0..16)
        .map(|i| (format!("外轮{i}"), vec![user("别人的输入")]))
        .collect();
    let refs: Vec<(&str, Vec<Value>)> = candidates
        .iter()
        .map(|(id, items)| (id.as_str(), items.clone()))
        .collect();
    let actions = settle_idle(&mut machine, &refs);
    assert_eq!(absorbed(&actions), Some(Value::Null));
    assert!(!machine.claims["线程"].joined());
}

#[test]
fn 阶段间来了其他轮次的启动则重算候选不沿用旧的空闲结果() {
    for later_active in [true, false] {
        let (mut machine, context) = start(true, "turn/start");
        apply(&mut machine, turn("turn/completed", "前轮"));
        read_for(&mut machine, &context, "本轮");
        // 第一步元数据空闲，进入条目阶段；条目在途时来了他轮的启动与新完成。
        let (actions, _) = drive_status(
            &mut machine,
            vec![StatusStep::Reply(
                expect(StatusPhase::Metadata),
                Ok(meta("线程", "idle")),
            )],
        );
        let items = status_requests(&actions);
        assert_eq!(items[0].candidate.as_deref(), Some("前轮"));
        apply(&mut machine, turn_event("turn/completed", "线程", "新完成"));
        apply(&mut machine, turn_event("turn/started", "线程", "他轮"));
        // 条目本身不匹配；旧的空闲结果不得被沿用成「无载体 → 自有」。
        let actions = reply(
            &mut machine,
            &items[0].context,
            Ok(items_page("前轮", &[user("别的输入")], None)),
        );
        assert!(machine.claims["线程"].settling(), "已变化不能定案");
        settlement_request(&actions);
        assert_eq!(
            settlement_query(&machine).0,
            vec!["前轮".to_string(), "新完成".into()],
            "用最新 held 重算候选"
        );
        let actions = if later_active {
            settle_active(&mut machine, &["他轮"])
        } else {
            settle_idle(
                &mut machine,
                &[
                    ("前轮", vec![user("别的输入")]),
                    ("新完成", vec![user("本次输入")]),
                ],
            )
        };
        let expected = if later_active { "他轮" } else { "新完成" };
        assert_eq!(absorbed(&actions), Some(json!(expected)));
        if later_active {
            assert!(machine.claims["线程"].joined());
            assert_eq!(machine.claims["线程"].turn_id(), Some("他轮"));
        } else {
            assert!(machine.claims.is_empty(), "新完成的缓冲完成释放加入的认领");
        }
    }
}

#[test]
fn 版本变化立即重发最多三次之后按普通失败计且不计入尝试次数() {
    let (mut machine, context) = start(true, "turn/start");
    let mut read = read_for(&mut machine, &context, "本轮");
    for change in 0..4 {
        apply(
            &mut machine,
            turn_event("turn/completed", "线程", &format!("他轮{change}")),
        );
        let actions = reply(&mut machine, &read.context, Ok(meta("线程", "idle")));
        if change < 3 {
            assert!(
                !actions.iter().any(|a| matches!(
                    a,
                    Action::Schedule {
                        kind: TimerKind::SettlementRetry { .. },
                        ..
                    }
                )),
                "前三次立即重发、不安排重试"
            );
            assert!(sends(&actions).iter().all(|(_, m)| m["id"] != 42));
            read = settlement_request(&actions);
            assert_eq!(
                machine.status_reads.reads[&read.read_id].caller,
                StatusCaller::Settlement { attempt: 0 },
                "不计入 attempt"
            );
        } else {
            assert!(status_requests(&actions).is_empty(), "第四次不再立即重发");
            assert_eq!(sends(&actions)[0].1["id"], 42, "按普通失败先交付回包");
            let timer = retry_timer(&actions, 1);
            // 计数按整次 settling 累计：重试后的已变化继续按普通失败计。
            let actions = apply(&mut machine, timer);
            let read = settlement_request(&actions);
            apply(&mut machine, turn_event("turn/completed", "线程", "再一轮"));
            let actions = reply(&mut machine, &read.context, Ok(meta("线程", "idle")));
            assert!(status_requests(&actions).is_empty());
            retry_timer(&actions, 2);
        }
    }
    assert!(machine.claims["线程"].settling());
}

#[test]
fn 活动但存储页里没有活动轮次走有界重试用尽后加入() {
    let (mut machine, context) = start(true, "turn/start");
    read_for(&mut machine, &context, "本轮");
    for attempt in 0..=10u8 {
        let actions = settle_active(&mut machine, &[]);
        if attempt == 0 {
            let response = sends(&actions);
            assert_eq!(response[0].1["id"], 42);
            assert!(response[0].1["result"].get("absorbedBy").is_none());
        }
        if attempt == 10 {
            assert!(machine.claims["线程"].joined());
            assert_eq!(machine.claims["线程"].turn_id(), Some("本轮"));
            assert!(actions
                .iter()
                .any(|a| matches!(a, Action::Warn(t) if t.contains("重试十次"))));
            break;
        }
        assert!(machine.claims["线程"].settling(), "Active([]) 不是空闲");
        let actions = apply(&mut machine, retry_timer(&actions, attempt + 1));
        settlement_request(&actions);
    }
}

#[test]
fn 畸形回包超时与无响应都不能定案为自有() {
    let malformed = [
        (StatusPhase::Metadata, json!({"thread": {"id": "线程"}})),
        (
            StatusPhase::Metadata,
            json!({"thread": {"id": "线程", "status": {"type": "未来状态"}}}),
        ),
        (
            StatusPhase::Metadata,
            json!({"thread": {"id": "别的线程", "status": {"type": "idle"}}}),
        ),
        (
            StatusPhase::Turns,
            json!({"data": [{"id": "本轮", "status": "未知"}], "nextCursor": null}),
        ),
        (
            StatusPhase::Turns,
            json!({"data": [{"status": "inProgress"}], "nextCursor": null}),
        ),
        (
            StatusPhase::Turns,
            json!({"data": [], "nextCursor": "空页带游标"}),
        ),
        (StatusPhase::Turns, json!({"nextCursor": null})),
        (
            StatusPhase::Items,
            json!({"data": [{"turnId": "别轮", "item": user("本次输入")}], "nextCursor": null}),
        ),
        (
            StatusPhase::Items,
            json!({"data": [{"turnId": "外轮", "item": {"type": "userMessage", "content": "本次输入"}}], "nextCursor": null}),
        ),
        (
            StatusPhase::Items,
            json!({"data": [{"turnId": "外轮", "item": user("别的")}]}),
        ),
    ];
    for (phase, value) in malformed {
        let (mut machine, context) = start(true, "turn/start");
        apply(&mut machine, turn("turn/completed", "外轮"));
        read_for(&mut machine, &context, "本轮");
        let mut script = Vec::new();
        match phase {
            StatusPhase::Metadata => {}
            StatusPhase::Turns => script.push(StatusStep::Reply(
                expect(StatusPhase::Metadata),
                Ok(meta("线程", "active")),
            )),
            StatusPhase::Items => script.push(StatusStep::Reply(
                expect(StatusPhase::Metadata),
                Ok(meta("线程", "idle")),
            )),
        }
        script.push(StatusStep::Reply(expect(phase.clone()), Ok(value.clone())));
        let (actions, done) = drive_status(&mut machine, script);
        assert_eq!(done.len(), 1, "{value}");
        assert!(machine.claims["线程"].settling(), "畸形不得定案：{value}");
        retry_timer(&actions, 1);
    }
    for kind in ["错误回包", "不支持", "无响应", "总期限"] {
        let (mut machine, context) = start(true, "turn/start");
        let read = read_for(&mut machine, &context, "本轮");
        let actions = match kind {
            "错误回包" => reply(
                &mut machine,
                &read.context,
                Err(error_reply(-32000, "读失败")),
            ),
            "不支持" => reply(
                &mut machine,
                &read.context,
                Err(error_reply(-32601, "list_turns is not supported yet")),
            ),
            "无响应" => apply(
                &mut machine,
                Event::TimedOut {
                    context: read.context.clone(),
                    error: json!({"message": "测试超时"}),
                    tag: 1,
                    deadline: std::time::Instant::now(),
                },
            ),
            _ => {
                let deadline =
                    crate::test_support::status_read::deadline_event(&machine, read.read_id);
                apply(&mut machine, deadline)
            }
        };
        assert!(machine.claims["线程"].settling(), "{kind}");
        retry_timer(&actions, 1);
    }
}

#[test]
fn 提交前完成不是吸收判定的证据() {
    let mut machine = Machine::new(true);
    let actions = apply(
        &mut machine,
        client(request(
            1,
            42,
            "turn/start",
            json!({"threadId": "线程", "input": [{"type": "text", "text": "本次输入"}]}),
        )),
    );
    let resume = first_request(&actions);
    apply(&mut machine, turn("turn/completed", "前轮"));
    let actions = reply(&mut machine, &resume, Ok(json!({"thread": {"id": "线程"}})));
    read_for(&mut machine, &first_request(&actions), "本轮");
    assert!(
        settlement_query(&machine).0.is_empty(),
        "提交前的完成不是候选"
    );
    let actions = settle_idle(&mut machine, &[]);
    assert!(status_requests(&actions).is_empty(), "无候选不读条目");
    assert!(!machine.claims["线程"].joined());
    assert!(machine.records["线程"].ended_turns.is_empty());
}

#[test]
fn 不明归属每秒重试十次后保守加入() {
    let (mut machine, context) = start(true, "turn/start");
    let mut read = read_for(&mut machine, &context, "本轮");
    for attempt in 0..=10 {
        let actions = reply(
            &mut machine,
            &read.context,
            Err(proto::error_object(-32000, "读失败", None)),
        );
        if attempt == 0 {
            let response = sends(&actions);
            assert_eq!(response[0].1["id"], 42);
            assert!(!response[0].1["result"]
                .as_object()
                .unwrap()
                .contains_key("absorbedBy"));
        }
        if attempt == 10 {
            assert!(machine.claims["线程"].joined());
            assert!(!machine.claims["线程"].settling());
            assert!(actions.iter().any(|a| matches!(a, Action::Warn(_))));
            break;
        }
        let timer = retry_timer(&actions, attempt + 1);
        read = settlement_request(&apply(&mut machine, timer));
        assert_eq!(
            machine.status_reads.reads[&read.read_id].caller,
            StatusCaller::Settlement {
                attempt: attempt + 1
            }
        );
    }
}

#[test]
fn 不明归属期间本轮启动立即定案且旧读与计时器无效() {
    let (mut machine, context) = start(true, "turn/start");
    let read = read_for(&mut machine, &context, "本轮");
    let actions = reply(&mut machine, &read.context, Err(json!({"code": -32000})));
    let timer = retry_timer(&actions, 1);
    let read = settlement_request(&apply(&mut machine, timer.clone()));
    apply(&mut machine, turn("turn/started", "本轮"));
    let claim = machine.claims["线程"].clone();
    assert!(!claim.settling());
    reply(&mut machine, &read.context, Ok(meta("线程", "active")));
    assert_eq!(machine.claims["线程"], claim);
    let actions = apply(&mut machine, timer);
    assert!(!actions.iter().any(|a| matches!(a, Action::Request(_))));
}

#[test]
fn 读取期间本轮启动优先于读取错误() {
    let (mut machine, context) = start(true, "turn/start");
    let read = read_for(&mut machine, &context, "本轮");
    apply(&mut machine, turn("turn/started", "本轮"));
    reply(&mut machine, &read.context, Err(json!({"code": -32000})));
    assert!(!machine.claims["线程"].settling());
    assert!(!machine.claims["线程"].joined());
}

#[test]
fn 读取期间本轮启动优先于他轮活动结果() {
    // 本轮启动推进版本，读取返回已变化；回调先查 own_started，按自有定案，不重发、不采纳外轮。
    let (mut machine, context) = start(true, "turn/start");
    read_for(&mut machine, &context, "本轮");
    let (actions, _) = drive_status(
        &mut machine,
        vec![StatusStep::Reply(
            expect(StatusPhase::Metadata),
            Ok(meta("线程", "active")),
        )],
    );
    let turns = status_requests(&actions).remove(0);
    apply(&mut machine, turn("turn/started", "本轮"));
    let actions = reply(
        &mut machine,
        &turns.context,
        Ok(turns_page(&[("外轮", "inProgress")], None)),
    );
    assert!(status_requests(&actions).is_empty());
    assert!(!machine.claims["线程"].joined());
    assert_eq!(absorbed(&actions), Some(Value::Null));
}

#[test]
fn 被取代的吸收判定读取结果丢弃() {
    let (mut machine, context) = start(true, "turn/start");
    let first = read_for(&mut machine, &context, "本轮");
    let mut actions = Vec::new();
    machine.begin_settlement("线程", 0, &mut actions);
    let second = settlement_request(&actions);
    // 先回答被取代的读取：活动轮次是外轮，若被采纳会错判为加入。
    let (stale, done) = drive_status(
        &mut machine,
        vec![
            StatusStep::Reply(
                expect(StatusPhase::Metadata).of(first.read_id),
                Ok(meta("线程", "active")),
            ),
            StatusStep::Reply(
                expect(StatusPhase::Turns).of(first.read_id),
                Ok(turns_page(&[("外轮", "inProgress")], None)),
            ),
        ],
    );
    assert_eq!(done.len(), 1);
    assert!(machine.claims["线程"].settling(), "被取代的结果不得定案");
    assert!(stale
        .iter()
        .any(|a| matches!(a, Action::Debug(t) if t.contains("取代"))));
    let (actions, _) = drive_status(
        &mut machine,
        vec![
            StatusStep::Reply(
                expect(StatusPhase::Metadata).of(second.read_id),
                Ok(meta("线程", "active")),
            ),
            StatusStep::Reply(
                expect(StatusPhase::Turns).of(second.read_id),
                Ok(turns_page(&[("本轮", "inProgress")], None)),
            ),
        ],
    );
    assert_eq!(absorbed(&actions), Some(Value::Null));
}

#[test]
fn 定案失败只转发缓冲不提交完成() {
    let (mut machine, context) = start(true, "turn/start");
    apply(&mut machine, turn("turn/completed", "外轮"));
    let actions = reply(&mut machine, &context, Err(json!({"code": -32000})));
    assert!(machine.claims.is_empty());
    assert!(machine.records["线程"].ended_turns.is_empty());
    assert!(sends(&actions)
        .iter()
        .any(|(_, m)| m["method"] == "turn/completed"));
}

#[test]
fn 缓冲仅对以斜线增量结尾的方法设二百条上限() {
    let (mut machine, _) = start(true, "turn/start");
    for i in 0..205 {
        apply(
            &mut machine,
            notify(
                "item/agentMessage/delta",
                json!({"threadId": "线程", "序号": i}),
            ),
        );
        apply(
            &mut machine,
            notify(
                "item/commandExecution/outputDelta",
                json!({"threadId": "线程", "序号": i}),
            ),
        );
    }
    apply(&mut machine, turn("turn/completed", "本轮"));
    let State::Starting { held, .. } = &machine.claims["线程"].state else {
        panic!("应保持缓冲")
    };
    assert_eq!(held.len(), 406);
}

#[test]
fn 客户端加入归属未定认领会回放目标完成() {
    let (mut machine, _) = start(true, "turn/start");
    apply(&mut machine, turn("turn/completed", "外轮"));
    apply(
        &mut machine,
        client(request(
            1,
            5,
            "broker/joined",
            json!({"threadId": "线程", "turnId": "外轮"}),
        )),
    );
    assert!(machine.claims.is_empty());
    assert_eq!(machine.records["线程"].ended_turns.len(), 1);
}

#[test]
fn 私有服务器响应不含吸收字段且转向请求也不含() {
    for (shared, method) in [
        (false, "turn/start"),
        (true, "turn/steer"),
        (true, "review/start"),
    ] {
        let (mut machine, context) = start(shared, method);
        let actions = reply(&mut machine, &context, Ok(json!({"turn": {"id": "本轮"}})));
        assert!(sends(&actions)
            .iter()
            .all(|(_, m)| m["result"].get("absorbedBy").is_none()));
    }
}

#[test]
fn 共享服务器无轮次响应按自有定案并明确空吸收字段() {
    let (mut machine, context) = start(true, "turn/start");
    let actions = reply(&mut machine, &context, Ok(json!({})));
    assert!(!machine.claims["线程"].settling());
    assert_eq!(sends(&actions)[0].1["result"]["absorbedBy"], Value::Null);
    assert!(sends(&actions)[0].1["result"]
        .as_object()
        .unwrap()
        .contains_key("absorbedBy"));
}

#[test]
fn 过期吸收读取不修改新认领且只产生告警() {
    let (mut machine, context) = start(true, "turn/start");
    let read = read_for(&mut machine, &context, "本轮");
    machine.release("线程", &mut Vec::new());
    apply(
        &mut machine,
        client(request(2, 99, "turn/start", json!({"threadId": "线程"}))),
    );
    let before = machine.claims["线程"].clone();
    let actions = reply(&mut machine, &read.context, Ok(meta("线程", "idle")));
    assert_eq!(machine.claims["线程"], before);
    assert!(actions.iter().any(|a| matches!(a, Action::Warn(_))));
}
