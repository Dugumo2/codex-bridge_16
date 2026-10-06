//! 吸收判定的证据顺序、重试与提交回放测试（详细设计 3.4 节）。

use super::*;

fn read_for(machine: &mut Machine, context: &RequestContext, turn_id: &str) -> RequestContext {
    let actions = reply(machine, context, Ok(json!({"turn": {"id": turn_id}})));
    first_request(&actions)
}

#[test]
fn 缓冲完成在定案回放时恰好记账一次() {
    let (mut machine, context) = start(true, "turn/start");
    apply(&mut machine, turn("turn/completed", "本轮"));
    apply(&mut machine, turn("turn/completed", "本轮"));
    assert!(machine.records["线程"].ended_turns.is_empty());
    let read = read_for(&mut machine, &context, "本轮");
    let actions = reply(
        &mut machine,
        &read,
        Ok(json!({"thread": {"turns": [{"id": "本轮", "status": "inProgress"}]}})),
    );
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
    let read = read_for(&mut machine, &context, "本轮");
    let actions = reply(
        &mut machine,
        &read,
        Ok(json!({"thread": {"turns": [{"id": "外轮", "status": "inProgress"}]}})),
    );
    assert!(machine.claims.is_empty());
    assert_eq!(
        machine.records["线程"].ended_turns,
        VecDeque::from(["外轮".into()])
    );
    assert!(sends(&actions)
        .iter()
        .any(|(_, m)| m["result"]["absorbedBy"] == "外轮"));
}

#[test]
fn 读到本轮活跃优先于期间到达的前轮完成() {
    let (mut machine, context) = start(true, "turn/start");
    let read = read_for(&mut machine, &context, "本轮");
    apply(&mut machine, turn("turn/completed", "前轮"));
    let actions = reply(
        &mut machine,
        &read,
        Ok(json!({"thread": {"turns": [{"id": "本轮", "status": "inProgress"}]}})),
    );
    assert!(!machine.claims["线程"].joined());
    assert!(machine.records["线程"].ended_turns.is_empty());
    assert!(sends(&actions)
        .iter()
        .any(|(_, m)| m["result"].get("absorbedBy") == Some(&Value::Null)));
}

#[test]
fn 用户输入载体与提交后完成候选相交才判加入() {
    for text in ["本次输入", "包含本次输入的其他内容"] {
        let (mut machine, context) = start(true, "turn/start");
        apply(&mut machine, turn("turn/completed", "外轮"));
        let read = read_for(&mut machine, &context, "本轮");
        let actions = reply(
            &mut machine,
            &read,
            Ok(json!({"thread": {"turns": [{
                "id": "外轮", "status": "completed",
                "items": [{"type": "userMessage", "content": [{"type": "text", "text": text}]}]
            }]}})),
        );
        let expected = if text == "本次输入" {
            json!("外轮")
        } else {
            Value::Null
        };
        assert!(sends(&actions)
            .iter()
            .any(|(_, m)| m["result"].get("absorbedBy") == Some(&expected)));
        assert_eq!(machine.claims.is_empty(), text == "本次输入");
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
    let read = read_for(&mut machine, &first_request(&actions), "本轮");
    reply(
        &mut machine,
        &read,
        Ok(json!({"thread": {"turns": [{
            "id": "前轮", "status": "completed",
            "items": [{"type": "userMessage", "content": [{"type": "text", "text": "本次输入"}]}]
        }]}})),
    );
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
            &read,
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
        let (token, kind) = actions
            .iter()
            .find_map(|a| match a {
                Action::Schedule {
                    token,
                    kind: TimerKind::SettlementRetry { attempt: next },
                    delay_ms,
                } => {
                    assert_eq!(*delay_ms, 1000);
                    assert_eq!(*next, attempt + 1);
                    Some((token.clone(), TimerKind::SettlementRetry { attempt: *next }))
                }
                _ => None,
            })
            .unwrap();
        read = first_request(&apply(&mut machine, Event::Timer { token, kind }));
    }
}

#[test]
fn 不明归属期间本轮启动立即定案且旧读与计时器无效() {
    let (mut machine, context) = start(true, "turn/start");
    let read = read_for(&mut machine, &context, "本轮");
    let actions = reply(&mut machine, &read, Err(json!({"code": -32000})));
    let (token, kind) = actions
        .iter()
        .find_map(|a| match a {
            Action::Schedule { token, kind, .. } => Some((token.clone(), kind.clone())),
            _ => None,
        })
        .unwrap();
    let read = first_request(&apply(
        &mut machine,
        Event::Timer {
            token: token.clone(),
            kind: kind.clone(),
        },
    ));
    apply(&mut machine, turn("turn/started", "本轮"));
    let claim = machine.claims["线程"].clone();
    reply(
        &mut machine,
        &read,
        Ok(json!({"thread": {"turns": [{"id": "外轮", "status": "inProgress"}]}})),
    );
    assert_eq!(machine.claims["线程"], claim);
    let actions = apply(&mut machine, Event::Timer { token, kind });
    assert!(!actions.iter().any(|a| matches!(a, Action::Request(_))));
}

#[test]
fn 读取期间本轮启动优先于读取错误() {
    let (mut machine, context) = start(true, "turn/start");
    let read = read_for(&mut machine, &context, "本轮");
    apply(&mut machine, turn("turn/started", "本轮"));
    reply(&mut machine, &read, Err(json!({"code": -32000})));
    assert!(!machine.claims["线程"].settling());
    assert!(!machine.claims["线程"].joined());
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
    let actions = reply(&mut machine, &read, Ok(json!({"thread": {"turns": []}})));
    assert_eq!(machine.claims["线程"], before);
    assert!(actions.iter().any(|a| matches!(a, Action::Warn(_))));
}
