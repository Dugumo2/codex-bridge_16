//! review 子线程的原子转移与早到通知测试（详细设计 3.5 节）。

use super::*;

#[test]
fn inline_缺省空值显式及非法模式不跟踪子线程且正常完成() {
    for delivery in [
        None,
        Some(Value::Null),
        Some(json!("inline")),
        Some(json!("其他值")),
        Some(json!(17)),
    ] {
        for same_thread in [false, true] {
            let mut params = json!({"threadId":"线程"});
            if let Some(value) = &delivery {
                params["delivery"] = value.clone();
            }
            let (mut machine, context) = start_params(false, "review/start", params.clone());
            assert_eq!(context.params, Some(params));
            assert_eq!(machine.pending_reviews, 0, "delivery={delivery:?}");
            assert!(context.review_id.is_none());
            apply(
                &mut machine,
                notify(
                    "turn/started",
                    json!({"threadId":"无关线程","turn":{"id":"无关轮"}}),
                ),
            );
            assert!(machine.unclaimed.is_empty());
            apply(&mut machine, Event::Connected { conn: 1 });
            apply(&mut machine, Event::Initialized { conn: 1, cxb: true });
            let mut response = json!({"turn":{"id":"审查轮"}});
            if same_thread {
                response["reviewThreadId"] = json!("线程");
            }
            let actions = reply(&mut machine, &context, Ok(response));
            assert!(sends(&actions)
                .iter()
                .all(|(_, message)| message["method"] != "broker/run/moved"));
            assert_eq!(machine.claims.len(), 1);
            assert!(machine.claims.contains_key("线程"));
            let run = &machine.runs.runs["run-测试-000001"];
            assert!(run.review_thread_id.is_none());
            assert!(run.review_short_id.is_none());
            let actions = apply(&mut machine, turn("turn/completed", "审查轮"));
            assert!(machine.claims.is_empty());
            assert_eq!(
                sends(&actions)
                    .iter()
                    .filter(|(_, message)| message["method"] == "broker/run/ended")
                    .count(),
                1
            );
            assert_eq!(machine.runs.runs["run-测试-000001"].turns.len(), 1);
        }
    }
}

#[test]
fn 子线程早到完成只提交一次且回放会释放认领() {
    let (mut machine, context) = start_legacy_review(false);
    let actions = apply(
        &mut machine,
        notify(
            "turn/completed",
            json!({"threadId": "子线程", "turn": {"id": "审查轮"}}),
        ),
    );
    assert!(sends(&actions).is_empty());
    assert!(machine.unclaimed["子线程"][0].committed);
    assert_eq!(machine.records["子线程"].ended_turns.len(), 1);
    let actions = reply(
        &mut machine,
        &context,
        Ok(json!({"turn": {"id": "审查轮"}, "reviewThreadId": "子线程"})),
    );
    assert!(machine.claims.is_empty());
    assert_eq!(machine.records["子线程"].ended_turns.len(), 1);
    assert_eq!(
        machine.records["子线程"].last_run,
        Some("run-测试-000001".into())
    );
    assert_eq!(
        sends(&actions)
            .iter()
            .filter(|(_, m)| m["method"] == "turn/completed")
            .count(),
        1
    );
    assert_eq!(machine.pending_reviews, 0);
}

#[test]
fn 父子转移不结束运行且子线程分配新认领代号() {
    let (mut machine, context) = start_legacy_review(false);
    let parent_id = machine.claims["线程"].claim_id;
    reply(
        &mut machine,
        &context,
        Ok(json!({"turn": {"id": "审查轮"}, "reviewThreadId": "子线程"})),
    );
    assert!(!machine.claims.contains_key("线程"));
    assert_eq!(
        machine.claims["子线程"].run_id,
        Some("run-测试-000001".into())
    );
    assert!(machine.claims["子线程"].claim_id > parent_id);
    assert!(machine.records["线程"].last_run.is_none());
    apply(
        &mut machine,
        client(request(2, 88, "turn/start", json!({"threadId": "线程"}))),
    );
    assert_eq!(machine.claims.len(), 2);
}

#[test]
fn 审查内层轮次不吞掉外层完成通知() {
    let (mut machine, context) = start_legacy_review(false);
    reply(
        &mut machine,
        &context,
        Ok(json!({"turn": {"id": "审查轮"}, "reviewThreadId": "子线程"})),
    );
    apply(
        &mut machine,
        notify(
            "turn/started",
            json!({"threadId": "子线程", "turn": {"id": "内层轮"}}),
        ),
    );
    let actions = apply(
        &mut machine,
        notify(
            "turn/completed",
            json!({"threadId": "子线程", "turn": {"id": "审查轮"}}),
        ),
    );
    assert!(machine.claims.is_empty());
    assert_eq!(sends(&actions)[0].1["params"]["turn"]["id"], "审查轮");
}

#[test]
fn 内联审查完成先于回包不会重建认领() {
    let (mut machine, context) = start(false, "review/start");
    apply(&mut machine, turn("turn/completed", "审查轮"));
    reply(
        &mut machine,
        &context,
        Ok(json!({"turn": {"id": "审查轮"}, "reviewThreadId": "线程"})),
    );
    assert!(machine.claims.is_empty());
    assert_eq!(machine.pending_reviews, 0);
}

#[test]
fn 断开后的审查回包停止子线程且完成前保留() {
    let (mut machine, context) = start_legacy_review(false);
    apply(&mut machine, Event::Disconnected { conn: 1 });
    let actions = reply(
        &mut machine,
        &context,
        Ok(json!({"turn": {"id": "审查轮"}, "reviewThreadId": "子线程"})),
    );
    assert!(machine.claims["子线程"].owner_gone);
    assert!(actions.iter().any(|a| matches!(a, Action::Request(c) if c.method == "turn/interrupt" && c.params.as_ref().unwrap()["threadId"] == "子线程" && c.params.as_ref().unwrap()["turnId"] == "审查轮")));
    assert!(!actions
        .iter()
        .any(|a| matches!(a, Action::Request(c) if c.method == "turn/interrupt" && c.params.as_ref().unwrap()["threadId"] == "线程")));
}

#[test]
fn 并行审查缓冲不泄漏给其他拥有者() {
    let (mut machine, first) = start_legacy_review(false);
    machine.record("父二").subscribed = true;
    let actions = apply(
        &mut machine,
        client(request(
            2,
            99,
            "review/start",
            json!({"threadId": "父二", "delivery": "detached"}),
        )),
    );
    let second = first_request(&actions);
    apply(
        &mut machine,
        notify("审查输出", json!({"threadId": "子一", "输出": "甲"})),
    );
    apply(
        &mut machine,
        notify("审查输出", json!({"threadId": "子二", "输出": "乙"})),
    );
    let actions = reply(
        &mut machine,
        &first,
        Ok(json!({"turn": {"id": "轮一"}, "reviewThreadId": "子一"})),
    );
    assert!(sends(&actions).iter().all(|(conn, _)| *conn == 1));
    assert_eq!(machine.pending_reviews, 1);
    assert!(machine.unclaimed.contains_key("子二"));
    let actions = reply(
        &mut machine,
        &second,
        Ok(json!({"turn": {"id": "轮二"}, "reviewThreadId": "子二"})),
    );
    assert!(sends(&actions).iter().all(|(conn, _)| *conn == 2));
    assert!(machine.unclaimed.is_empty());
}

#[test]
fn 审查缓冲总计五百条且计数归零清空() {
    let (mut machine, context) = start_legacy_review(false);
    for i in 0..510 {
        apply(
            &mut machine,
            notify("审查输出", json!({"threadId": format!("子{i}"), "序号": i})),
        );
    }
    assert_eq!(
        machine.unclaimed.values().map(Vec::len).sum::<usize>(),
        MAX_UNCLAIMED
    );
    reply(&mut machine, &context, Err(json!({"code": -32000})));
    assert_eq!(machine.pending_reviews, 0);
    assert!(machine.unclaimed.is_empty());
}

#[test]
fn 子线程记录不覆盖父线程元数据() {
    let (mut machine, context) = start_legacy_review(false);
    machine
        .record("线程")
        .pending_meta
        .insert(1, json!({"shortId": "父编号"}));
    machine
        .record("子线程")
        .pending_meta
        .insert(1, json!({"shortId": "子编号"}));
    reply(
        &mut machine,
        &context,
        Ok(json!({"turn": {"id": "审查轮"}, "reviewThreadId": "子线程"})),
    );
    assert_eq!(
        machine.records["线程"].pending_meta[&1]["shortId"],
        "父编号"
    );
    assert_eq!(
        machine.records["子线程"].pending_meta[&1]["shortId"],
        "子编号"
    );
}

#[test]
fn 早到审查完成后的同批输出仍转发且不进入目标续跑() {
    let (mut machine, context) = start_legacy_review(false);
    apply(
        &mut machine,
        notify(
            "thread/goal/updated",
            json!({"threadId": "子线程", "goal": {"status": "active"}}),
        ),
    );
    apply(
        &mut machine,
        notify(
            "turn/completed",
            json!({"threadId": "子线程", "turn": {"id": "审查轮"}}),
        ),
    );
    apply(
        &mut machine,
        notify(
            "审查尾部输出",
            json!({"threadId": "子线程", "内容": "尾部"}),
        ),
    );
    let actions = reply(
        &mut machine,
        &context,
        Ok(json!({"turn": {"id": "审查轮"}, "reviewThreadId": "子线程"})),
    );
    assert!(machine.claims.is_empty());
    assert_eq!(machine.records["子线程"].ended_turns.len(), 1);
    assert!(sends(&actions)
        .iter()
        .any(|(_, m)| m["method"] == "审查尾部输出"));
}

#[test]
fn 共享模式内联审查完成先于回包时恰好记账一次且不重建认领() {
    let (mut machine, context) = start(true, "review/start");
    apply(&mut machine, turn("turn/started", "审查轮"));
    apply(&mut machine, turn("turn/completed", "审查轮"));
    // settling 期间只缓冲，不记账。
    assert!(!machine
        .records
        .get("线程")
        .is_some_and(|r| r.ended_turns.iter().any(|t| t == "审查轮")));
    let actions = reply(
        &mut machine,
        &context,
        Ok(json!({"turn": {"id": "审查轮"}, "reviewThreadId": "线程"})),
    );
    assert!(machine.claims.is_empty());
    assert_eq!(
        machine.records["线程"]
            .ended_turns
            .iter()
            .filter(|t| *t == "审查轮")
            .count(),
        1
    );
    assert_eq!(
        machine.records["线程"].last_run,
        Some("run-测试-000001".into())
    );
    let sent = sends(&actions);
    assert_eq!(
        sent.iter()
            .filter(|(_, m)| m["method"] == "turn/completed")
            .count(),
        1
    );
    assert!(sent.iter().all(|(conn, _)| *conn == 1));
    // review 不做吸收判定：回包不带 absorbedBy。
    let answer = sent
        .iter()
        .find(|(_, m)| m["id"] == 42)
        .expect("review 回包");
    assert!(answer.1["result"].get("absorbedBy").is_none());
}

#[test]
fn 父认领换代后迟到的审查回包仍认领子线程且不动新父认领() {
    let (mut machine, review) = start_legacy_review(false);
    apply(&mut machine, Event::Connected { conn: 1 });
    // 审查请求还没回包时父线程的认领被一个完成通知释放，随后另一个连接在父线程上开了新一轮。
    apply(&mut machine, turn("turn/completed", "父轮"));
    assert!(machine.claims.is_empty());
    apply(
        &mut machine,
        client(request(2, 7, "turn/start", json!({"threadId": "线程"}))),
    );
    let new_parent = machine.claims["线程"].clone();
    apply(
        &mut machine,
        notify(
            "turn/started",
            json!({"threadId": "子线程", "turn": {"id": "审查轮"}}),
        ),
    );
    let actions = reply(
        &mut machine,
        &review,
        Ok(json!({"turn": {"id": "审查轮"}, "reviewThreadId": "子线程"})),
    );
    assert_eq!(machine.claims["线程"], new_parent);
    let child = &machine.claims["子线程"];
    assert_eq!(
        child.state,
        State::Running {
            owner: Owner::Client(1),
            turn_id: "审查轮".into(),
            joined: false,
        }
    );
    assert_eq!(child.run_id, Some("run-测试-000001".into()));
    assert!(!child.owner_gone);
    assert!(child.claim_id > new_parent.claim_id);
    // 早到的子线程通知交给原 review owner，回包也送回原连接。
    let sent = sends(&actions);
    assert!(sent
        .iter()
        .any(|(conn, m)| *conn == 1 && m["method"] == "turn/started"));
    assert!(sent.iter().any(|(conn, m)| *conn == 1 && m["id"] == 42));
    assert!(sent.iter().all(|(conn, _)| *conn == 1));
    assert_eq!(machine.pending_reviews, 0);
}

#[test]
fn 父认领过期且原连接已断开时子线程认领立即进入停止操作() {
    let (mut machine, review) = start_legacy_review(false);
    apply(&mut machine, Event::Connected { conn: 1 });
    apply(&mut machine, turn("turn/completed", "父轮"));
    apply(&mut machine, Event::Disconnected { conn: 1 });
    let actions = reply(
        &mut machine,
        &review,
        Ok(json!({"turn": {"id": "审查轮"}, "reviewThreadId": "子线程"})),
    );
    assert!(machine.claims["子线程"].owner_gone);
    assert!(actions.iter().any(|a| matches!(
        a,
        Action::Request(c) if c.method == "turn/interrupt" && c.params.as_ref().unwrap()["threadId"] == "子线程" && c.params.as_ref().unwrap()["turnId"] == "审查轮"
    )));
    assert!(sends(&actions).iter().all(|(conn, _)| *conn == 1));
}

#[test]
fn 关闭后迟到的审查回包不再认领子线程() {
    let (mut machine, review) = start_legacy_review(false);
    apply(&mut machine, Event::Connected { conn: 1 });
    apply(&mut machine, Event::Shutdown);
    assert!(machine.claims.is_empty());
    let actions = reply(
        &mut machine,
        &review,
        Ok(json!({"turn": {"id": "审查轮"}, "reviewThreadId": "子线程"})),
    );
    assert!(machine.claims.is_empty());
    assert!(!actions
        .iter()
        .any(|a| matches!(a, Action::Request(_) | Action::Schedule { .. })));
    // 原请求的回包照常交还客户端（连接若已断开，core 会丢弃），review 计数照常收尾。
    assert_eq!(
        sends(&actions),
        vec![(
            1,
            json!({"id": 42, "result": {"turn": {"id": "审查轮"}, "reviewThreadId": "子线程"}})
        )]
    );
    assert_eq!(machine.pending_reviews, 0);
}
