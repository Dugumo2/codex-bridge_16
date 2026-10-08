//! M7 预算与共享关闭边界；每个事件后核对字节守恒。
use super::*;
use crate::claim::status_read::StatusCaller;
use crate::test_support::status_read::{meta, status_requests, turns_page, StatusPhase};

fn invariant(m: &Machine) {
    m.assert_held_accounting();
    assert_eq!(
        m.settling_bytes,
        m.claims
            .values()
            .filter(|c| c.settling())
            .map(|c| c.held_bytes)
            .sum::<usize>()
    );
    for (thread, claim) in &m.claims {
        if !claim.settling() {
            assert_eq!(claim.held_bytes, 0, "{thread}");
        }
    }
}

#[test]
fn 字节不变式可以识别两个总计同时错误() {
    let mut m = Machine::new(true);
    begin(&mut m, "t", true);
    notify(&mut m, "t", "future/event", json!({"data":"中文"}));
    server(&mut m, "t", 1, "请求");
    m.assert_held_accounting();
    m.claims.get_mut("t").unwrap().held_bytes += 1;
    m.settling_bytes += 1;
    assert!(std::panic::catch_unwind(|| m.assert_held_accounting()).is_err());
}

#[test]
fn 首次溢出裁剪已经持有的生命周期也会触发核实() {
    let mut m = Machine::new(true);
    let start = begin(&mut m, "t", true);
    for _ in 0..SETTLING_HARD_ITEMS {
        notify(&mut m, "t", "turn/future", json!({}));
    }
    notify(
        &mut m,
        "t",
        "turn/completed",
        json!({"turn":{"id":"target","status":"completed","items":[]}}),
    );
    assert_eq!(held(&m, "t"), OVERFLOW_LIFECYCLE_ITEMS);
    let actions = reply(&mut m, &start, Ok(json!({"turn":{"id":"target"}})));
    let read = joined_read(&m, &actions, "t", 0);
    absent(&mut m, &read, "t");
    assert!(m.claims.is_empty());
}

#[test]
fn 溢出审查丢失生命周期后只核实实际子线程() {
    let mut m = Machine::new(true);
    let review = legacy_review(&mut m);
    overflow(&mut m, "parent");
    for _ in 0..OVERFLOW_LIFECYCLE_ITEMS {
        notify(&mut m, "parent", "turn/future", json!({}));
    }
    notify(
        &mut m,
        "parent",
        "turn/completed",
        json!({"turn":{"id":"旧父轮","status":"completed","items":[]}}),
    );
    let actions = reply(
        &mut m,
        &review,
        Ok(json!({"reviewThreadId":"child","turn":{"id":"review-turn"}})),
    );
    let read = joined_read(&m, &actions, "child", 0);
    assert!(!m.claims.contains_key("parent"));
    absent(&mut m, &read, "child");
    assert!(m.claims.is_empty());
    assert!(m.joined_checks.is_empty());
    assert_eq!(
        m.runs.runs["review-run"].state,
        crate::runs::RunState::Ended
    );
}

#[test]
fn 关闭时goal写入只允许明确的非激活状态及clear() {
    for closing in [false, true] {
        for status in [
            None,
            Some(json!("active")),
            Some(Value::Null),
            Some(json!(42)),
            Some(json!("未来状态")),
            Some(json!("paused")),
            Some(json!("blocked")),
            Some(json!("complete")),
            Some(json!("usageLimited")),
            Some(json!("budgetLimited")),
        ] {
            let mut m = Machine::new(true);
            m.quiescing = !closing;
            m.closing = closing;
            let mut params = json!({"threadId":"t","objective":"继续工作","tokenBudget":100});
            if let Some(status) = status {
                params["status"] = status;
            }
            let allowed = matches!(
                params["status"].as_str(),
                Some("paused" | "blocked" | "complete" | "usageLimited" | "budgetLimited")
            );
            let actions = apply(
                &mut m,
                Event::ClientRequest {
                    request: ClientRequest {
                        conn: 1,
                        id: json!(1),
                        method: "thread/goal/set".into(),
                        params: Some(params),
                    },
                    run_id: None,
                },
            );
            assert_eq!(
                actions.iter().any(|a| matches!(a, Action::Request(_))),
                allowed
            );
            assert_eq!(actions.iter().any(|a|matches!(a,Action::Send {message,..} if message["error"]["message"]==SHUTTING_DOWN)),!allowed);
            let actions = apply(
                &mut m,
                Event::ClientRequest {
                    request: ClientRequest {
                        conn: 1,
                        id: json!(2),
                        method: "thread/goal/clear".into(),
                        params: Some(json!({"threadId":"t"})),
                    },
                    run_id: None,
                },
            );
            request(&actions, "thread/goal/clear");
        }
    }
}

fn client_goal(m: &mut Machine, thread: &str, status: Option<&str>) -> RequestContext {
    let mut params = json!({"threadId":thread,"objective":"继续工作"});
    if let Some(status) = status {
        params["status"] = json!(status);
    }
    let actions = apply(
        m,
        Event::ClientRequest {
            request: ClientRequest {
                conn: 1,
                id: json!(9),
                method: "thread/goal/set".into(),
                params: Some(params),
            },
            run_id: None,
        },
    );
    request(&actions, "thread/goal/set")
}

#[test]
fn 普通客户端可能激活的在途及超时goal写入在关闭时先暂停() {
    for status in [
        None,
        Some("active"),
        Some("未知值"),
        Some("paused"),
        Some("blocked"),
    ] {
        for timeout in [false, true] {
            let mut m = Machine::new(false);
            let start = begin(&mut m, "t", true);
            reply(&mut m, &start, Ok(json!({"turn":{"id":"own"}})));
            m.shared = true;
            let goal = client_goal(&mut m, "t", status);
            let activates = !matches!(status, Some("paused" | "blocked"));
            if timeout {
                apply(
                    &mut m,
                    Event::TimedOut {
                        context: goal,
                        error: json!({"message":"测试超时"}),
                        tag: 88,
                        deadline: Instant::now(),
                    },
                );
            }
            assert!(!m.records["t"].goal_active);
            apply(&mut m, Event::ShutdownBegin);
            let actions = apply(&mut m, Event::ShutdownStopOwn);
            if activates {
                let pause = request(&actions, "thread/goal/set");
                assert_eq!(pause.params.as_ref().unwrap()["status"], "paused");
                assert!(!actions
                    .iter()
                    .any(|a| matches!(a,Action::Request(c) if c.method=="turn/interrupt")));
                let actions = reply(&mut m, &pause, Ok(json!({"goal":{"status":"paused"}})));
                request(&actions, "turn/interrupt");
            } else {
                request(&actions, "turn/interrupt");
            }
            apply(&mut m, Event::Shutdown);
            assert!(m.activating_goal_changes.is_empty());
        }
    }
}

#[test]
fn 已落定或其他线程的客户端goal写入不触发本线程额外暂停() {
    for settled in [false, true] {
        let mut m = Machine::new(false);
        let start = begin(&mut m, "t", true);
        reply(&mut m, &start, Ok(json!({"turn":{"id":"own"}})));
        m.shared = true;
        let goal = client_goal(&mut m, if settled { "t" } else { "other" }, None);
        if settled {
            reply(&mut m, &goal, Err(json!({"message":"未写入"})));
            assert!(m.activating_goal_changes.is_empty());
        }
        apply(&mut m, Event::ShutdownBegin);
        let actions = apply(&mut m, Event::ShutdownStopOwn);
        request(&actions, "turn/interrupt");
        assert!(!actions
            .iter()
            .any(|a| matches!(a,Action::Request(c) if c.method=="thread/goal/set")));
    }
}

fn review(m: &mut Machine) -> RequestContext {
    review_fixture(m, false)
}

/// 显式 legacy detached 服务器夹具，子线程回包由用例注入。
fn legacy_review(m: &mut Machine) -> RequestContext {
    review_fixture(m, true)
}

fn review_fixture(m: &mut Machine, legacy: bool) -> RequestContext {
    m.record("parent").subscribed = true;
    m.live_conns.insert(1);
    m.cxb_conns.insert(1);
    let actions = apply(
        m,
        Event::ClientRequest {
            request: ClientRequest {
                conn: 1,
                id: json!(7),
                method: "review/start".into(),
                params: Some(
                    json!({"threadId":"parent","delivery":if legacy { "detached" } else { "inline" }}),
                ),
            },
            run_id: Some("review-run".into()),
        },
    );
    request(&actions, "review/start")
}

fn completed_turn() -> Value {
    json!({"turn":{"id":"review-turn","status":"completed","items":[{"id":"result","type":"agentMessage","text":"审查完成","phase":"final_answer"}]}})
}

#[test]
fn 溢出审查迁移joined子线程且早到或晚到completed都结束运行() {
    for early in [false, true] {
        let mut m = Machine::new(true);
        let review = legacy_review(&mut m);
        overflow(&mut m, "parent");
        if early {
            notify(
                &mut m,
                "child",
                "turn/started",
                json!({"turn":{"id":"review-turn"}}),
            );
            notify(&mut m, "child", "turn/completed", completed_turn());
        }
        let actions = reply(
            &mut m,
            &review,
            Ok(json!({"reviewThreadId":"child","turn":{"id":"review-turn"}})),
        );
        assert!(!m.claims.contains_key("parent"));
        assert_eq!(
            m.runs.runs["review-run"].review_thread_id.as_deref(),
            Some("child")
        );
        assert!(m.runs.runs["review-run"].joined);
        assert!(!actions.iter().any(|a|matches!(a,Action::Request(c) if c.method=="turn/interrupt" || c.method=="thread/goal/set")));
        if !early {
            assert!(m.claims["child"].joined());
            assert!(m.claims["child"].owned_turns.is_empty());
        }
        let actions = apply(
            &mut m,
            Event::ClientRequest {
                request: ClientRequest {
                    conn: 1,
                    id: json!(8),
                    method: "turn/start".into(),
                    params: Some(json!({"threadId":"parent"})),
                },
                run_id: Some("next-parent".into()),
            },
        );
        request(&actions, "turn/start");
        if !early {
            notify(&mut m, "child", "turn/completed", completed_turn());
        }
        assert!(!m.claims.contains_key("child"));
        assert!(m.claims.contains_key("parent"));
        assert_eq!(
            m.runs.runs["review-run"].state,
            crate::runs::RunState::Ended
        );
        assert_eq!(
            m.runs.runs["review-run"].turns.last().unwrap().status,
            crate::runs::TurnStatus::Completed
        );
        assert_eq!(m.pending_reviews, 0);
        invariant(&m);
    }
}

#[test]
fn 溢出审查不抢占已有子线程且断开不停止joined审查() {
    for occupied in [false, true] {
        let mut m = Machine::new(false);
        let child = if occupied {
            let start = begin(&mut m, "child", true);
            reply(&mut m, &start, Ok(json!({"turn":{"id":"other"}})));
            Some(m.claims["child"].clone())
        } else {
            None
        };
        m.shared = true;
        let review = legacy_review(&mut m);
        overflow(&mut m, "parent");
        if !occupied {
            apply(&mut m, Event::Disconnected { conn: 1 });
        }
        let actions = reply(
            &mut m,
            &review,
            Ok(json!({"reviewThreadId":"child","turn":{"id":"review-turn"}})),
        );
        assert!(!m.claims.contains_key("parent"));
        if let Some(child) = child {
            assert_eq!(m.claims["child"], child);
        } else {
            assert!(m.claims.is_empty());
        }
        assert!(m.runs.runs["review-run"].joined);
        assert_eq!(
            m.runs.runs["review-run"].state,
            crate::runs::RunState::Ended
        );
        assert!(!actions
            .iter()
            .any(|a| matches!(a,Action::Request(c) if c.method=="turn/interrupt")));
        assert_eq!(m.settling_bytes, 0);
    }
}

#[test]
fn 溢出内联审查回放目标完成不重新认领() {
    let mut m = Machine::new(true);
    let review = review(&mut m);
    overflow(&mut m, "parent");
    notify(&mut m, "parent", "turn/completed", completed_turn());
    reply(
        &mut m,
        &review,
        Ok(json!({"reviewThreadId":"parent","turn":{"id":"review-turn"}})),
    );
    assert!(m.claims.is_empty());
    assert!(m.runs.runs["review-run"].joined);
    assert_eq!(
        m.runs.runs["review-run"].state,
        crate::runs::RunState::Ended
    );
}

fn lost_completed(m: &mut Machine, byte_limit: bool) -> RequestContext {
    let start = begin(m, "t", true);
    overflow(m, "t");
    if byte_limit {
        notify(
            m,
            "t",
            "turn/future",
            json!({"data":"x".repeat(SETTLING_HARD_BYTES-100)}),
        );
    } else {
        for _ in 0..OVERFLOW_LIFECYCLE_ITEMS {
            notify(m, "t", "turn/future", json!({}));
        }
    }
    let bytes = m.settling_bytes;
    notify(
        m,
        "t",
        "turn/completed",
        json!({"turn":{"id":"target","status":"completed","items":[]},"padding":"x".repeat(200)}),
    );
    assert_eq!(m.settling_bytes, bytes, "完成通知必须因预算未保存");
    let actions = reply(m, &start, Ok(json!({"turn":{"id":"target"}})));
    let read = joined_read(m, &actions, "t", 0);
    assert!(m.claims["t"].joined());
    assert_eq!(m.settling_bytes, 0);
    read
}

#[test]
fn 目标完成因条数或字节丢失后补读已结束或找不到均释放() {
    for byte_limit in [false, true] {
        for found in [false, true] {
            let mut m = Machine::new(true);
            let read = lost_completed(&mut m, byte_limit);
            let actions = if found {
                reply(
                    &mut m,
                    &read,
                    Ok(turns_page(&[("target", "completed")], None)),
                )
            } else {
                absent(&mut m, &read, "t")
            };
            // 释放不等结果补读：找到终态时补读刚发出、认领已经释放。
            let result_reads: Vec<_> = actions
                .iter()
                .filter_map(|a| match a {
                    Action::Request(c) if matches!(c.operation, Operation::ResultRead(_)) => {
                        Some(c.clone())
                    }
                    _ => None,
                })
                .collect();
            if found {
                assert_eq!(result_reads.len(), 1);
                assert_eq!(result_reads[0].method, "thread/items/list");
            } else {
                assert!(result_reads.is_empty());
            }
            assert_no_full_read(&actions);
            assert!(m.claims.is_empty());
            assert!(m.joined_checks.is_empty());
            assert!(m.stream_slots.is_empty());
            assert!(!m.stops_busy());
            assert_eq!(m.runs.runs["run-t"].state, crate::runs::RunState::Ended);
        }
    }
}

#[test]
fn 生命周期补读目标仍运行就保持joined并等待新completed() {
    let mut m = Machine::new(true);
    let read = lost_completed(&mut m, false);
    let actions = reply(
        &mut m,
        &read,
        Ok(turns_page(&[("target", "inProgress")], None)),
    );
    assert!(m.claims["t"].joined());
    assert!(m.joined_checks.is_empty());
    assert!(!actions.iter().any(|a| matches!(a, Action::Request(_))));
    notify(
        &mut m,
        "t",
        "turn/completed",
        json!({"turn":{"id":"target","status":"completed","items":[]}}),
    );
    assert!(m.claims.is_empty());
}

#[test]
fn 生命周期核实超时或坏快照重试十次后告警释放且重复事件不重发() {
    let mut m = Machine::new(true);
    let mut read = lost_completed(&mut m, false);
    for attempt in 0..=10 {
        let actions = if attempt % 2 == 0 {
            apply(
                &mut m,
                Event::TimedOut {
                    context: read.clone(),
                    error: json!({"message":"测试超时"}),
                    tag: 100 + attempt,
                    deadline: Instant::now(),
                },
            )
        } else {
            reply(&mut m, &read, Ok(turns_page(&[("target", "未知")], None)))
        };
        assert!(!reply(&mut m, &read, Err(json!({"message":"重复"})))
            .iter()
            .any(|a| matches!(a, Action::Request(_) | Action::Schedule { .. })));
        if attempt == 10 {
            assert!(m.claims.is_empty());
            assert!(m.joined_checks.is_empty());
            assert!(actions
                .iter()
                .any(|a| matches!(a,Action::Warn(text) if text.contains("重试十次"))));
        } else {
            let (token, kind) = actions
                .iter()
                .find_map(|a| match a {
                    Action::Schedule {
                        token,
                        kind: kind @ TimerKind::JoinedCheckRetry { .. },
                        delay_ms: 1000,
                    } => Some((token.clone(), kind.clone())),
                    _ => None,
                })
                .unwrap();
            let actions = apply(
                &mut m,
                Event::Timer {
                    token: token.clone(),
                    kind: kind.clone(),
                },
            );
            read = joined_read(&m, &actions, "t", attempt as u8 + 1);
            assert!(!apply(&mut m, Event::Timer { token, kind })
                .iter()
                .any(|a| matches!(a, Action::Request(_))));
        }
    }
}

#[test]
fn 生命周期核实期间完成释放或关闭后旧回包和期限不影响新认领() {
    for closing in [false, true] {
        let mut m = Machine::new(true);
        let read = lost_completed(&mut m, false);
        let actions = reply(&mut m, &read, Err(json!({"message":"暂不可读"})));
        let (token, kind) = actions
            .iter()
            .find_map(|a| match a {
                Action::Schedule {
                    token,
                    kind: kind @ TimerKind::JoinedCheckRetry { .. },
                    ..
                } => Some((token.clone(), kind.clone())),
                _ => None,
            })
            .unwrap();
        if closing {
            apply(&mut m, Event::Shutdown);
        } else {
            notify(
                &mut m,
                "t",
                "turn/completed",
                json!({"turn":{"id":"target","status":"completed","items":[]}}),
            );
            begin(&mut m, "t", true);
        }
        assert!(m.joined_checks.is_empty());
        let claims = m.claims.clone();
        reply(&mut m, &read, Ok(turns_page(&[], None)));
        let actions = apply(&mut m, Event::Timer { token, kind });
        assert!(!actions.iter().any(|a| matches!(a, Action::Request(_))));
        assert_eq!(m.claims, claims);
    }
}

fn apply(m: &mut Machine, event: Event) -> Vec<Action> {
    let (next, actions) = step(std::mem::take(m), event);
    *m = next;
    invariant(m);
    actions
}

fn request(actions: &[Action], method: &str) -> RequestContext {
    actions
        .iter()
        .find_map(|a| match a {
            Action::Request(c) if c.method == method => Some(c.clone()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("没有 {method} 请求：{actions:?}"))
}

/// joined 核实经 H2 状态读取层的 Turn 查询发出：第一步就是 notLoaded 轮次页，不发全量读。
fn joined_read(m: &Machine, actions: &[Action], thread: &str, attempt: u8) -> RequestContext {
    assert_no_full_read(actions);
    let requests = status_requests(actions);
    assert_eq!(requests.len(), 1, "只发一个核实请求：{actions:?}");
    let read = &requests[0];
    assert_eq!(read.phase, StatusPhase::Turns);
    let params = read.params.as_ref().unwrap();
    assert_eq!(params["threadId"], thread);
    assert_eq!(params["itemsView"], "notLoaded");
    assert_eq!(
        m.status_reads.reads[&read.read_id].caller,
        StatusCaller::JoinedCheck { attempt }
    );
    read.context.clone()
}

/// 遍历完未找到目标，再读元数据确认线程不活动：Turn 查询返回 Absent。
fn absent(m: &mut Machine, read: &RequestContext, thread: &str) -> Vec<Action> {
    let mut actions = reply(m, read, Ok(turns_page(&[], None)));
    let metadata = request(&actions, "thread/read");
    assert!(metadata
        .params
        .as_ref()
        .unwrap()
        .get("includeTurns")
        .is_none());
    actions.extend(reply(m, &metadata, Ok(meta(thread, "idle"))));
    actions
}

fn assert_no_full_read(actions: &[Action]) {
    assert!(
        !actions.iter().any(|a| matches!(a, Action::Request(c)
            if c.params.as_ref().is_some_and(|p| p.get("includeTurns").is_some()))),
        "不得发全量读取：{actions:?}"
    );
}

fn reply(m: &mut Machine, context: &RequestContext, result: Result<Value, Value>) -> Vec<Action> {
    apply(
        m,
        Event::Reply {
            context: context.clone(),
            result,
        },
    )
}

fn begin(m: &mut Machine, thread: &str, subscribed: bool) -> RequestContext {
    m.record(thread).subscribed = subscribed;
    m.live_conns.insert(1);
    m.cxb_conns.insert(1);
    let actions = apply(
        m,
        Event::ClientRequest {
            request: ClientRequest {
                conn: 1,
                id: json!(thread),
                method: "turn/start".into(),
                params: Some(json!({"threadId":thread})),
            },
            run_id: Some(format!("run-{thread}")),
        },
    );
    request(
        &actions,
        if subscribed {
            "turn/start"
        } else {
            "thread/resume"
        },
    )
}

fn notify(m: &mut Machine, thread: &str, method: &str, extra: Value) -> Vec<Action> {
    let mut params = extra;
    params["threadId"] = json!(thread);
    apply(
        m,
        Event::Notification {
            method: method.into(),
            params: Some(params),
        },
    )
}

fn held(m: &Machine, thread: &str) -> usize {
    match &m.claims[thread].state {
        State::Starting { held, .. } => held.len(),
        _ => 0,
    }
}

fn server(m: &mut Machine, thread: &str, id: usize, payload: &str) -> Vec<Action> {
    apply(
        m,
        Event::ServerRequest {
            id: json!(id),
            forward_id: format!("forward-{thread}-{id}"),
            method: "item/tool/requestUserInput".into(),
            params: Some(json!({"threadId":thread,"payload":payload})),
        },
    )
}

fn dismissed(actions: &[Action], id: usize) -> bool {
    actions
        .iter()
        .any(|a| matches!(a, Action::Dismiss { id: got } if *got == json!(id)))
}

fn overflow(m: &mut Machine, thread: &str) -> Vec<Action> {
    let mut actions = Vec::new();
    for _ in 0..=SETTLING_HARD_ITEMS {
        actions.extend(notify(m, thread, "future/event", json!({})));
    }
    actions
}

#[test]
fn 软上限保留必要通知且自有运行标缺项() {
    for joined in [false, true] {
        let mut m = Machine::new(true);
        let start = begin(&mut m, "t", true);
        for _ in 0..SETTLING_SOFT_ITEMS {
            notify(&mut m, "t", "future/event", json!({}));
        }
        let actions = notify(
            &mut m,
            "t",
            "item/completed",
            json!({"item":{"id":"i","type":"commandExecution"}}),
        );
        assert_eq!(
            actions
                .iter()
                .filter(|a| matches!(a, Action::Warn(_)))
                .count(),
            1
        );
        assert!(notify(&mut m, "t", "item/started", json!({})).is_empty());
        assert_eq!(held(&m, "t"), SETTLING_SOFT_ITEMS);
        for method in [
            "item/autoApprovalReview/completed",
            "turn/started",
            "future/event",
        ] {
            notify(&mut m, "t", method, json!({"turn":{"id":"own"}}));
        }
        assert_eq!(held(&m, "t"), SETTLING_SOFT_ITEMS + 3);
        if joined {
            let mut actions = Vec::new();
            m.settle("t", true, Some("other".into()), &mut actions);
        } else {
            reply(&mut m, &start, Ok(json!({"turn":{"id":"own"}})));
        }
        invariant(&m);
        assert_eq!(m.runs.runs["run-t"].progress.incomplete, !joined);
        assert_eq!(m.settling_bytes, 0);
        let mut actions = Vec::new();
        m.mark_progress_incomplete("run-t", &mut actions);
        actions.clear();
        m.mark_progress_incomplete("run-t", &mut actions);
        assert!(actions.is_empty());
    }
}

#[test]
fn delta二百条上限与软字节边界() {
    let mut m = Machine::new(true);
    begin(&mut m, "t", true);
    for _ in 0..250 {
        notify(&mut m, "t", "item/text/delta", json!({"delta":"字"}));
    }
    assert_eq!(held(&m, "t"), MAX_HELD_DELTAS);
    notify(
        &mut m,
        "t",
        "future/event",
        json!({"data":"x".repeat(SETTLING_SOFT_BYTES)}),
    );
    let before = m.settling_bytes;
    notify(&mut m, "t", "item/started", json!({}));
    assert_eq!(before, m.settling_bytes);
    assert_eq!(m.claims["t"].held_dropped_items, 1);
}

#[test]
fn 挂起请求数量字节与resolved撤销() {
    let mut m = Machine::new(true);
    begin(&mut m, "t", true);
    for id in 0..MAX_HELD_REQUESTS {
        assert!(!dismissed(&server(&mut m, "t", id, "字"), id));
    }
    let before = m.settling_bytes;
    let actions = server(&mut m, "t", 64, "");
    assert!(dismissed(&actions, 64));
    assert_eq!(before, m.settling_bytes);
    assert_eq!(
        actions
            .iter()
            .filter(|a| matches!(a, Action::Warn(_)))
            .count(),
        1
    );
    assert!(!server(&mut m, "t", 65, "")
        .iter()
        .any(|a| matches!(a, Action::Warn(_))));
    m.remove_held_request("t", &json!(0));
    invariant(&m);
    assert!(m.settling_bytes < before);
    assert!(!dismissed(&server(&mut m, "t", 66, ""), 66));
    apply(&mut m, Event::Shutdown);
    assert_eq!(m.settling_bytes, 0);
    assert!(m.settling_accounting.is_empty());

    let mut m = Machine::new(true);
    begin(&mut m, "t", true);
    let actions = server(&mut m, "t", 1, &"x".repeat(SETTLING_HARD_BYTES));
    assert!(dismissed(&actions, 1));
    assert_eq!(m.settling_bytes, 0);
    assert!(!m.claims["t"].overflowed);
}

#[test]
fn 回包后硬条数溢出按joined回放目标完成并只定案一次() {
    let mut m = Machine::new(true);
    let start = begin(&mut m, "t", true);
    let actions = reply(&mut m, &start, Ok(json!({"turn":{"id":"own"}})));
    let read = request(&actions, "thread/read");
    notify(
        &mut m,
        "t",
        "turn/completed",
        json!({"turn":{"id":"own","status":"completed","items":[]}}),
    );
    let actions = overflow(&mut m, "t");
    assert!(!m.claims.contains_key("t"));
    assert_eq!(m.settling_bytes, 0);
    assert!(m.runs.runs["run-t"].joined);
    assert_eq!(actions.iter().filter(|a| matches!(a,Action::Send { message, .. } if message["method"]=="broker/run/settled" && message["params"]["absorbedBy"]=="own")).count(),1);
    assert!(actions.iter().any(
        |a| matches!(a,Action::Send { message, .. } if message["result"]["absorbedBy"]=="own")
    ));
    assert!(!reply(&mut m, &read, Ok(json!({"thread":{"turns":[]}})))
        .iter()
        .any(
            |a| matches!(a,Action::Send { message, .. } if message["method"]=="broker/run/settled")
        ));
}

#[test]
fn 回包前溢出成功失败超时和缺失编号都结清() {
    for outcome in 0..4 {
        let mut m = Machine::new(true);
        let start = begin(&mut m, "t", true);
        server(&mut m, "t", 1, "");
        notify(
            &mut m,
            "t",
            "turn/completed",
            json!({"turn":{"id":"own","status":"completed","items":[]}}),
        );
        let actions = overflow(&mut m, "t");
        assert!(m.claims["t"].settling() && m.claims["t"].overflowed);
        assert_eq!(held(&m, "t"), 1);
        assert!(dismissed(&actions, 1));
        assert!(dismissed(&server(&mut m, "t", 2, ""), 2));
        let actions = match outcome {
            0 => reply(&mut m, &start, Ok(json!({"turn":{"id":"own"}}))),
            1 => reply(&mut m, &start, Err(json!({"message":"失败"}))),
            2 => apply(
                &mut m,
                Event::TimedOut {
                    context: start.clone(),
                    error: json!({"message":"超时"}),
                    tag: 9,
                    deadline: Instant::now(),
                },
            ),
            _ => reply(&mut m, &start, Ok(json!({}))),
        };
        assert!(!m.claims.contains_key("t"));
        assert_eq!(m.settling_bytes, 0);
        assert!(m.stream_slots.is_empty());
        assert!(m.stops.operations.is_empty());
        if outcome == 2 {
            assert!(actions
                .iter()
                .any(|a| matches!(a, Action::ForgetLate { tag: 9 })));
        }
    }
}

#[test]
fn 屏障前溢出仍等待原启动且生命周期缓冲有界() {
    let mut m = Machine::new(true);
    let barrier = begin(&mut m, "t", false);
    overflow(&mut m, "t");
    for _ in 0..300 {
        notify(&mut m, "t", "turn/started", json!({"turn":{"id":"other"}}));
    }
    assert_eq!(held(&m, "t"), OVERFLOW_LIFECYCLE_ITEMS);
    let bytes = m.settling_bytes;
    notify(&mut m, "t", "future/event", json!({"data":"x"}));
    assert_eq!(bytes, m.settling_bytes);
    let actions = reply(&mut m, &barrier, Ok(json!({})));
    let start = request(&actions, "turn/start");
    reply(&mut m, &start, Ok(json!({"turn":{"id":"own"}})));
    assert!(m.claims["t"].joined());
    assert_eq!(m.settling_bytes, 0);
}

#[test]
fn 失败进入stop_no_turn和未定案释放都清字节() {
    let mut m = Machine::new(true);
    let start = begin(&mut m, "t", true);
    notify(&mut m, "t", "future/event", json!({}));
    server(&mut m, "t", 1, "");
    apply(&mut m, Event::Disconnected { conn: 1 });
    reply(&mut m, &start, Err(json!({"message":"失败"})));
    invariant(&m);
    assert_eq!(m.settling_bytes, 0);
    assert!(m.claims.values().all(|c| c.held_requests.is_empty()));
    let mut m = Machine::new(true);
    begin(&mut m, "t", true);
    server(&mut m, "t", 1, "");
    let mut actions = Vec::new();
    m.release("t", &mut actions);
    invariant(&m);
    assert_eq!(m.settling_bytes, 0);
    assert!(dismissed(&actions, 1));
}

#[test]
fn 关闭先暂停后中断且重复与迟到回包无副作用() {
    for fail_pause in [false, true] {
        let mut m = Machine::new(false);
        let start = begin(&mut m, "t", true);
        reply(&mut m, &start, Ok(json!({"turn":{"id":"own"}})));
        m.shared = true;
        m.record("t").goal_active = true;
        apply(&mut m, Event::ShutdownBegin);
        let actions = apply(&mut m, Event::ShutdownStopOwn);
        let pause = request(&actions, "thread/goal/set");
        assert!(matches!(pause.operation, Operation::GoalChange { .. }));
        assert!(!actions
            .iter()
            .any(|a| matches!(a,Action::Request(c) if c.method=="turn/interrupt")));
        if let State::Running { turn_id, .. } = &mut m.claims.get_mut("t").unwrap().state {
            *turn_id = "next".into();
        }
        let actions = reply(
            &mut m,
            &pause,
            if fail_pause {
                Err(json!({"message":"失败"}))
            } else {
                Ok(json!({"goal":{"status":"paused"}}))
            },
        );
        let interrupt = request(&actions, "turn/interrupt");
        assert_eq!(interrupt.params.as_ref().unwrap()["turnId"], "next");
        assert!(!reply(&mut m, &pause, Ok(json!({})))
            .iter()
            .any(|a| matches!(a, Action::Request(_))));
        reply(&mut m, &interrupt, Ok(json!({})));
        assert_eq!(m.shutdown_stops_pending(), 0);
        assert!(!apply(&mut m, Event::ShutdownStopOwn)
            .iter()
            .any(|a| matches!(a, Action::Request(_))));
        apply(&mut m, Event::Shutdown);
        assert!(!reply(&mut m, &pause, Ok(json!({})))
            .iter()
            .any(|a| matches!(a, Action::Request(_))));
    }
}

#[test]
fn 硬字节超限回包后定案且溢出只发生一次() {
    let mut m = Machine::new(true);
    let start = begin(&mut m, "t", true);
    reply(&mut m, &start, Ok(json!({"turn":{"id":"own"}})));
    let actions = notify(
        &mut m,
        "t",
        "future/event",
        json!({"data":"x".repeat(SETTLING_HARD_BYTES)}),
    );
    assert!(m.claims["t"].joined());
    assert_eq!(m.settling_bytes, 0);
    assert_eq!(
        actions
            .iter()
            .filter(|a| matches!(a, Action::Warn(_)))
            .count(),
        1
    );
    let mut actions = Vec::new();
    m.settling_overflow("t", &mut actions);
    assert!(actions.is_empty());
}

#[test]
fn 全局预算选择最大认领并按线程名打破平局() {
    for tie in [false, true] {
        let mut m = Machine::new(true);
        for thread in ["b", "a", "c"] {
            begin(&mut m, thread, true);
        }
        let unit = 1024 * 1024;
        notify(
            &mut m,
            "b",
            "future/event",
            json!({"data":"x".repeat(24*unit)}),
        );
        notify(
            &mut m,
            "a",
            "future/event",
            json!({"data":"x".repeat(if tie {24*unit}else{25*unit})}),
        );
        notify(
            &mut m,
            "c",
            "future/event",
            json!({"data":"x".repeat(18*unit)}),
        );
        assert!(m.claims["a"].overflowed);
        assert!(!m.claims["b"].overflowed);
        assert!(!m.claims["c"].overflowed);
        assert_eq!(m.claims["a"].held_bytes, 0);
        assert!(m.settling_bytes <= SETTLING_GLOBAL_BYTES);
        apply(&mut m, Event::Shutdown);
        assert!(m.settling_accounting.is_empty());
    }
}

#[test]
fn 请求全局字节超限只dismiss不触发定案() {
    let mut m = Machine::new(true);
    for thread in ["a", "b", "c"] {
        begin(&mut m, thread, true);
    }
    for thread in ["a", "b"] {
        notify(
            &mut m,
            thread,
            "future/event",
            json!({"data":"x".repeat(30*1024*1024)}),
        );
    }
    let before = m.settling_bytes;
    assert!(dismissed(
        &server(&mut m, "c", 7, &"x".repeat(5 * 1024 * 1024)),
        7
    ));
    assert_eq!(m.settling_bytes, before);
    assert!(m.claims.values().all(|c| c.settling() && !c.overflowed));
}

#[test]
fn 取消订阅屏障与对账无turn均清缓冲() {
    for barrier in [true, false] {
        let mut m = Machine::new(true);
        let start = begin(&mut m, "t", !barrier);
        notify(&mut m, "t", "future/event", json!({}));
        server(&mut m, "t", 1, "");
        apply(&mut m, Event::Disconnected { conn: 1 });
        if !barrier {
            let actions = apply(
                &mut m,
                Event::TimedOut {
                    context: start,
                    error: json!({"message":"超时"}),
                    tag: 8,
                    deadline: Instant::now(),
                },
            );
            let read = request(&actions, "thread/read");
            reply(&mut m, &read, Ok(json!({"thread":{"turns":[]}})));
        }
        invariant(&m);
        assert_eq!(m.settling_bytes, 0);
        assert!(m.claims.values().all(|c| c.held_requests.is_empty()));
    }
}

#[test]
fn 第三步只处理自有认领并覆盖仅暂停仅中断与空档() {
    let mut m = Machine::new(false);
    for thread in ["own", "joined", "goal", "empty", "broker"] {
        let start = begin(&mut m, thread, true);
        reply(&mut m, &start, Ok(json!({"turn":{"id":thread}})));
    }
    if let State::Running { joined, .. } = &mut m.claims.get_mut("joined").unwrap().state {
        *joined = true;
    }
    for thread in ["goal", "empty"] {
        m.claims.get_mut(thread).unwrap().state = State::AwaitingContinuation {
            owner: Owner::Client(1),
        };
    }
    m.claims.get_mut("broker").unwrap().state = State::Running {
        owner: Owner::Broker {
            policy: crate::policy::Policy::Never,
            approvals_reviewer: None,
            owned_at: 0,
            last_watcher_at: 0,
        },
        turn_id: "broker".into(),
        joined: false,
    };
    m.record("goal").goal_active = true;
    m.record("joined").goal_active = true;
    m.shared = true;
    begin(&mut m, "settling", true);
    apply(&mut m, Event::ShutdownBegin);
    let actions = apply(&mut m, Event::ShutdownStopOwn);
    let methods: Vec<_> = actions
        .iter()
        .filter_map(|a| match a {
            Action::Request(c) => Some((
                c.params.as_ref().unwrap()["threadId"].as_str().unwrap(),
                c.method.as_str(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        methods,
        vec![
            ("broker", "turn/interrupt"),
            ("goal", "thread/goal/set"),
            ("own", "turn/interrupt")
        ]
    );
    let pause = request(&actions, "thread/goal/set");
    assert!(
        !reply(&mut m, &pause, Ok(json!({"goal":{"status":"paused"}})))
            .iter()
            .any(|a| matches!(a, Action::Request(_)))
    );
    assert_eq!(m.shutdown_stops_pending(), 2);
    apply(&mut m, Event::Shutdown);
    assert_eq!(m.shutdown_stops_pending(), 0);
}

#[test]
fn 暂停回包不干预换代认领且第四步后的回包完全静默() {
    let mut m = Machine::new(false);
    let start = begin(&mut m, "t", true);
    reply(&mut m, &start, Ok(json!({"turn":{"id":"old"}})));
    m.shared = true;
    m.record("t").goal_active = true;
    apply(&mut m, Event::ShutdownBegin);
    let actions = apply(&mut m, Event::ShutdownStopOwn);
    let pause = request(&actions, "thread/goal/set");
    m.claims.get_mut("t").unwrap().claim_id += 1;
    if let State::Running { turn_id, .. } = &mut m.claims.get_mut("t").unwrap().state {
        *turn_id = "foreign".into();
    }
    let actions = reply(&mut m, &pause, Ok(json!({"goal":{"status":"paused"}})));
    let interrupt = request(&actions, "turn/interrupt");
    assert_eq!(interrupt.params.as_ref().unwrap()["turnId"], "old");
    apply(&mut m, Event::Shutdown);
    let snapshot = m.clone();
    reply(&mut m, &pause, Ok(json!({"goal":{"status":"active"}})));
    reply(&mut m, &interrupt, Ok(json!({})));
    assert_eq!(m, snapshot);
}

fn continue_fixture(m: &mut Machine, thread: &str) -> Vec<Action> {
    let run = format!("old-{thread}");
    m.run_opened(
        RunOpen {
            thread_id: thread.into(),
            run_id: run.clone(),
            conn: None,
            kind: ReqKind::TurnStart,
            joined: false,
            input: String::new(),
            continued_from: None,
        },
        &mut Vec::new(),
    );
    m.run_released(&run, ReleaseKind::StopConfirmed, &mut Vec::new());
    let record = m.runs.runs.get_mut(&run).unwrap();
    record.state = crate::runs::RunState::Ended;
    record.interrupt_reason = Some(crate::runs::InterruptReason::Stop);
    m.record(thread).subscribed = true;
    apply(
        m,
        Event::ClientRequest {
            request: ClientRequest {
                conn: 1,
                id: json!(thread),
                method: "turn/start".into(),
                params: Some(json!({"threadId":thread,"cxbContinue":{"fromRunId":run}})),
            },
            run_id: Some(format!("continued-{thread}")),
        },
    )
}

#[test]
fn 关闭等待暂停时其他预检屏障和恢复读均不再开工() {
    let mut m = Machine::new(false);
    let own = begin(&mut m, "own", true);
    reply(&mut m, &own, Ok(json!({"turn":{"id":"own"}})));
    m.claims.get_mut("own").unwrap().resume_goal = true;
    let actions = notify(&mut m, "own", "turn/started", json!({"turn":{"id":"own"}}));
    let restore = request(&actions, "thread/goal/get");
    let checks = continue_fixture(&mut m, "continue");
    m.shared = true;
    let barrier = begin(&mut m, "barrier", false);
    m.record("own").goal_active = true;
    apply(&mut m, Event::ShutdownBegin);
    let actions = apply(&mut m, Event::ShutdownStopOwn);
    request(&actions, "thread/goal/set");
    for context in checks.iter().filter_map(|a| match a {
        Action::Request(c) => Some(c),
        _ => None,
    }) {
        let actions = reply(
            &mut m,
            context,
            Ok(if context.method == "thread/read" {
                json!({"thread":{"turns":[]}})
            } else {
                json!({"goal":{"status":"paused"}})
            }),
        );
        assert!(!actions.iter().any(|a| matches!(a, Action::Request(_))));
    }
    assert!(!m.runs.runs.contains_key("continued-continue"));
    assert!(!m.claims.contains_key("continue"));
    let actions = reply(&mut m, &barrier, Ok(json!({})));
    assert!(!actions
        .iter()
        .any(|a| matches!(a,Action::Request(c) if c.method=="turn/start")));
    assert!(actions.iter().any(
        |a| matches!(a,Action::Send { message,.. } if message["error"]["message"]==SHUTTING_DOWN)
    ));
    assert!(!m.claims.contains_key("barrier"));
    let actions = reply(&mut m, &restore, Ok(json!({"goal":{"status":"paused"}})));
    assert!(!actions.iter().any(|a| matches!(a, Action::Request(_))));
    assert_eq!(m.shutdown_stops_pending(), 1);
}

#[test]
fn active写入在途时关闭先暂停且新请求全部拒绝() {
    let mut m = Machine::new(false);
    let own = begin(&mut m, "own", true);
    reply(&mut m, &own, Ok(json!({"turn":{"id":"own"}})));
    m.claims.get_mut("own").unwrap().resume_goal = true;
    let actions = notify(&mut m, "own", "turn/started", json!({"turn":{"id":"own"}}));
    let get = request(&actions, "thread/goal/get");
    let actions = reply(&mut m, &get, Ok(json!({"goal":{"status":"paused"}})));
    let active = request(&actions, "thread/goal/set");
    assert_eq!(active.params.as_ref().unwrap()["status"], "active");
    assert!(!m.records["own"].goal_active);
    m.shared = true;
    apply(&mut m, Event::ShutdownBegin);
    let actions = apply(&mut m, Event::ShutdownStopOwn);
    let pause = request(&actions, "thread/goal/set");
    assert_eq!(pause.params.as_ref().unwrap()["status"], "paused");
    for (method, params) in [
        ("turn/start", json!({"threadId":"new"})),
        (
            "turn/start",
            json!({"threadId":"new","cxbContinue":{"fromRunId":"old"}}),
        ),
        (
            "thread/goal/set",
            json!({"threadId":"new","status":"active"}),
        ),
    ] {
        let actions = apply(
            &mut m,
            Event::ClientRequest {
                request: ClientRequest {
                    conn: 1,
                    id: json!(2),
                    method: method.into(),
                    params: Some(params),
                },
                run_id: Some("new".into()),
            },
        );
        assert!(actions
            .iter()
            .any(|a| matches!(a,Action::Send {message,..} if message.get("error").is_some())));
        assert!(!actions.iter().any(|a| matches!(a, Action::Request(_))));
    }
    reply(&mut m, &active, Ok(json!({"goal":{"status":"active"}})));
    let actions = reply(&mut m, &pause, Ok(json!({"goal":{"status":"paused"}})));
    request(&actions, "turn/interrupt");
}

#[test]
fn steer回包前溢出仍保持joined() {
    let method = "turn/steer";
    let mut m = Machine::new(true);
    m.record("t").subscribed = true;
    let actions = apply(
        &mut m,
        Event::ClientRequest {
            request: ClientRequest {
                conn: 1,
                id: json!(1),
                method: method.into(),
                params: Some(json!({"threadId":"t"})),
            },
            run_id: Some("run-t".into()),
        },
    );
    let context = request(&actions, method);
    overflow(&mut m, "t");
    reply(
        &mut m,
        &context,
        Ok(json!({"turn":{"id":"foreign"},"reviewThreadId":"child"})),
    );
    assert!(m.claims["t"].joined());
    assert!(!m.claims.contains_key("child"));
    assert_eq!(m.settling_bytes, 0);
    assert_eq!(m.pending_reviews, 0);
}

#[test]
fn 缺项标记沿进度计时器落盘且只调度一次() {
    let mut m = Machine::new(false);
    begin(&mut m, "t", true);
    let mut actions = Vec::new();
    m.mark_progress_incomplete("run-t", &mut actions);
    let (token, kind) = actions
        .iter()
        .find_map(|a| match a {
            Action::Schedule {
                token,
                kind: kind @ TimerKind::Run(crate::runs::RunTimer::ProgressFlush { .. }),
                ..
            } => Some((token.clone(), kind.clone())),
            _ => None,
        })
        .expect("缺项标记应按五秒进度规则调度落盘");
    actions.clear();
    m.mark_progress_incomplete("run-t", &mut actions);
    assert!(actions.is_empty());
    let actions = apply(&mut m, Event::Timer { token, kind });
    assert!(actions.iter().any(|a|matches!(a,Action::Store(crate::store::StoreOp::PutRun(record)) if record.progress.incomplete)));
}

#[test]
fn 暂停期间同代认领改为joined后不得中断外部轮次() {
    let mut m = Machine::new(false);
    let start = begin(&mut m, "t", true);
    reply(&mut m, &start, Ok(json!({"turn":{"id":"own"}})));
    m.shared = true;
    m.record("t").goal_active = true;
    apply(&mut m, Event::ShutdownBegin);
    let actions = apply(&mut m, Event::ShutdownStopOwn);
    let pause = request(&actions, "thread/goal/set");
    apply(
        &mut m,
        Event::ClientRequest {
            request: ClientRequest {
                conn: 1,
                id: json!(2),
                method: "broker/joined".into(),
                params: Some(json!({"threadId":"t","turnId":"foreign"})),
            },
            run_id: None,
        },
    );
    let actions = reply(&mut m, &pause, Ok(json!({"goal":{"status":"paused"}})));
    assert!(!actions
        .iter()
        .any(|a| matches!(a,Action::Request(c) if c.method=="turn/interrupt")));
    assert_eq!(m.shutdown_stops_pending(), 0);
}

// ─── H2-c：joined 核实经 Turn 查询 ───────────────────────────────

#[test]
fn joined核实目标在第二页以后照样找到() {
    for status in ["inProgress", "completed"] {
        let mut m = Machine::new(true);
        let read = lost_completed(&mut m, false);
        let first: Vec<(String, &str)> = (0..100)
            .map(|i| (format!("新轮{i}"), "completed"))
            .collect();
        let first: Vec<(&str, &str)> = first.iter().map(|(id, s)| (id.as_str(), *s)).collect();
        // 第一页未命中不等于目标不存在：继续翻页，不读元数据、不释放。
        let actions = reply(&mut m, &read, Ok(turns_page(&first, Some("第二页"))));
        assert!(m.claims["t"].joined());
        let next = request(&actions, "thread/turns/list");
        assert_eq!(next.params.as_ref().unwrap()["cursor"], "第二页");
        let actions = reply(
            &mut m,
            &next,
            Ok(turns_page(
                &[("更早", "completed"), ("target", status)],
                None,
            )),
        );
        assert_no_full_read(&actions);
        if status == "inProgress" {
            assert!(
                m.claims["t"].joined(),
                "目标仍在跑：保持 joined 等新的 completed"
            );
            assert!(m.joined_checks.is_empty());
        } else {
            assert!(m.claims.is_empty());
            assert!(m.joined_checks.is_empty());
        }
    }
}

#[test]
fn joined核实遍历完未找到但线程活动时不释放() {
    let mut m = Machine::new(true);
    let read = lost_completed(&mut m, false);
    let actions = reply(
        &mut m,
        &read,
        Ok(turns_page(&[("别轮", "completed")], Some("下一页"))),
    );
    let next = request(&actions, "thread/turns/list");
    let actions = reply(
        &mut m,
        &next,
        Ok(turns_page(&[("更早", "completed")], None)),
    );
    let metadata = request(&actions, "thread/read");
    // 分页线程的活动轮次可能还没持久化：线程 active 时不能按目标缺失释放。
    let actions = reply(&mut m, &metadata, Ok(meta("t", "active")));
    assert!(m.claims["t"].joined(), "不得释放");
    assert_eq!(m.joined_checks.len(), 1);
    let (token, kind) = actions
        .iter()
        .find_map(|a| match a {
            Action::Schedule {
                token,
                kind: kind @ TimerKind::JoinedCheckRetry { attempt: 1 },
                delay_ms: 1000,
            } => Some((token.clone(), kind.clone())),
            _ => None,
        })
        .expect("按有界重试再核实");
    let actions = apply(&mut m, Event::Timer { token, kind });
    let retry = joined_read(&m, &actions, "t", 1);
    // 重试时目标出现在存储页里且已完成，才释放。
    reply(
        &mut m,
        &retry,
        Ok(turns_page(&[("target", "completed")], None)),
    );
    assert!(m.claims.is_empty());
}

#[test]
fn joined核实找到终态时释放认领不等结果补读且旧服务器不发全量读() {
    let mut m = Machine::new(true);
    let read = lost_completed(&mut m, false);
    let mut actions = reply(
        &mut m,
        &read,
        Ok(
            json!({"data":[{"id":"target","status":"failed","items":[{"type":"agentMessage","text":"服务器忽略了 notLoaded"}],
            "itemsView":"full","error":{"message":"上游错误"}}],"nextCursor":null}),
        ),
    );
    assert!(m.claims.is_empty(), "释放认领不等结果补读");
    let turn = m.runs.runs["run-t"].turns.last().unwrap().clone();
    assert_eq!(turn.turn_id, "target");
    assert_eq!(turn.status, crate::runs::TurnStatus::Failed);
    assert_eq!(turn.error, Some(json!({"message":"上游错误"})));
    assert_eq!(
        turn.ready,
        crate::runs::Ready::Pending,
        "壳是 notLoaded，由补读接上"
    );
    let result_read = request(&actions, "thread/items/list");
    assert!(matches!(result_read.operation, Operation::ResultRead(_)));
    // legacy：条目分页不支持 → 标 unavailable，不发整线程读取。
    actions = reply(
        &mut m,
        &result_read,
        Err(json!({"code":-32601,"message":"Method not found"})),
    );
    assert_no_full_read(&actions);
    assert!(!actions.iter().any(|a| matches!(a, Action::Request(_))));
    let turn = m.runs.runs["run-t"].turns.last().unwrap().clone();
    assert_eq!(turn.ready, crate::runs::Ready::Unavailable);
}

#[test]
fn joined核实畸形回包与无法确认都不释放() {
    for (stage, value) in [
        (
            "turns",
            json!({"data":[{"id":"target","status":"未知"}],"nextCursor":null}),
        ),
        (
            "turns",
            json!({"data":[{"id":"","status":"completed"}],"nextCursor":null}),
        ),
        ("turns", json!({"data":{"id":"target"},"nextCursor":null})),
        ("turns", json!({"data":[],"nextCursor":"空页带游标"})),
        (
            "turns",
            json!({"data":[{"id":"别轮","status":"completed"}]}),
        ),
        ("metadata", json!({"thread":{"id":"t"}})),
        (
            "metadata",
            json!({"thread":{"id":"t","status":{"type":"未来状态"}}}),
        ),
        (
            "metadata",
            json!({"thread":{"id":"别的线程","status":{"type":"idle"}}}),
        ),
    ] {
        let mut m = Machine::new(true);
        let read = lost_completed(&mut m, false);
        let actions = if stage == "turns" {
            reply(&mut m, &read, Ok(value.clone()))
        } else {
            let actions = reply(&mut m, &read, Ok(turns_page(&[], None)));
            let metadata = request(&actions, "thread/read");
            reply(&mut m, &metadata, Ok(value.clone()))
        };
        assert!(m.claims["t"].joined(), "畸形不得释放：{value}");
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::Schedule {
                kind: TimerKind::JoinedCheckRetry { attempt: 1 },
                ..
            }
        )));
    }
}

#[test]
fn joined核实只认当前读取且版本变化不当作目标缺失() {
    let mut m = Machine::new(true);
    let read = lost_completed(&mut m, false);
    // 读取期间来了别的轮次的启动：读取层从第一页重读，旧页的回包按代号丢弃。
    notify(
        &mut m,
        "t",
        "turn/started",
        json!({"turn":{"id":"他轮","status":"inProgress","items":[]}}),
    );
    let actions = reply(&mut m, &read, Ok(turns_page(&[], None)));
    let reread = request(&actions, "thread/turns/list");
    assert!(m.claims["t"].joined());
    assert!(reply(&mut m, &read, Ok(turns_page(&[], None)))
        .iter()
        .all(|a| !matches!(a, Action::Request(_))));
    reply(
        &mut m,
        &reread,
        Ok(turns_page(&[("target", "inProgress")], None)),
    );
    assert!(m.claims["t"].joined());
    assert!(m.joined_checks.is_empty(), "目标仍在跑，取消核实");
}
