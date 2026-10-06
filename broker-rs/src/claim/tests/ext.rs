//! 丙路扩展协议与状态出口回归，不使用固定睡眠。
use super::*;
use crate::claim::ext::*;
use crate::runs::{Ready, RunLookup, SummaryOwner, SummaryState};

fn connected(machine: &mut Machine, conn: u64, cxb: bool) {
    apply(machine, Event::Connected { conn });
    apply(machine, Event::Initialized { conn, cxb });
}
fn call(machine: &mut Machine, conn: u64, method: &str, params: Value) -> Vec<Action> {
    apply(machine, client(request(conn, 99, method, params)))
}
fn response(actions: &[Action]) -> Value {
    sends(actions)
        .into_iter()
        .find(|(_, v)| v["id"] == 99)
        .expect("扩展请求应回包")
        .1
}
fn detach(machine: &mut Machine, conn: u64) -> Vec<Action> {
    call(
        machine,
        conn,
        "broker/threads/detach",
        json!({"threadId":"线程","runId":"run-测试-000001","policy":"auto"}),
    )
}
fn watch(machine: &mut Machine, conn: u64, thread: &str) -> Vec<Action> {
    call(
        machine,
        conn,
        "broker/threads/watch",
        json!({"threadId":thread}),
    )
}
fn timer(actions: &[Action], detach: bool) -> (ClaimToken, TimerKind, u64) {
    actions
        .iter()
        .find_map(|a| match a {
            Action::Schedule {
                token,
                kind,
                delay_ms,
            } if matches!(kind, TimerKind::DetachDeadline { .. }) == detach
                && matches!(
                    kind,
                    TimerKind::DetachDeadline { .. } | TimerKind::Reclaim { .. }
                ) =>
            {
                Some((token.clone(), kind.clone(), *delay_ms))
            }
            _ => None,
        })
        .expect("应登记扩展计时")
}
fn fire(machine: &mut Machine, timer: &(ClaimToken, TimerKind, u64)) -> Vec<Action> {
    apply(
        machine,
        Event::Timer {
            token: timer.0.clone(),
            kind: timer.1.clone(),
        },
    )
}
fn completed_ready() -> Event {
    notify(
        "turn/completed",
        json!({"threadId":"线程","turn":{"id":"本轮","status":"completed","items":[{"type":"agentMessage","text":"完成","phase":"final_answer"}]}}),
    )
}

#[test]
fn 门控与开工动作顺序覆盖三种流式请求及恢复屏障() {
    for method in ["turn/start", "turn/steer", "review/start"] {
        for cxb in [false, true] {
            let mut m = Machine::new(false);
            connected(&mut m, 1, cxb);
            let actions = apply(
                &mut m,
                client(request(1, 42, method, json!({"threadId":"线程"}))),
            );
            let notification = actions.iter().position(|a| matches!(a,Action::Send {message,..} if message["method"] == "broker/run/started"));
            assert_eq!(notification.is_some(), cxb);
            if let Some(n) = notification {
                assert!(
                    n < actions
                        .iter()
                        .position(|a| matches!(a, Action::Request(_)))
                        .unwrap()
                );
            }
            let resume = first_request(&actions);
            assert_eq!(resume.method, "thread/resume");
            let stream =
                first_request(&reply(&mut m, &resume, Ok(json!({"thread":{"id":"线程"}}))));
            let actions = reply(&mut m, &stream, Ok(json!({"turn":{"id":"本轮"}})));
            let value = sends(&actions)
                .into_iter()
                .find(|(_, v)| v["id"] == 42)
                .unwrap()
                .1;
            assert_eq!(value["result"].get("runId").is_some(), cxb);
            assert_eq!(value["result"].get("shortId").is_some(), cxb);
        }
    }
}
#[test]
fn 暂存定案回包及审查回包均附加运行编号() {
    let (mut m, context) = start(true, "turn/start");
    connected(&mut m, 1, true);
    let read = first_request(&reply(&mut m, &context, Ok(json!({"turn":{"id":"本轮"}}))));
    let actions = reply(
        &mut m,
        &read,
        Ok(json!({"thread":{"turns":[{"id":"本轮","status":"inProgress"}]}})),
    );
    let result = sends(&actions)
        .into_iter()
        .find(|(_, v)| v["id"] == 42)
        .unwrap()
        .1;
    assert_eq!(result["result"]["runId"], "run-测试-000001");
    assert!(result["result"].get("absorbedBy").is_some());
    let (mut m, context) = start(false, "review/start");
    connected(&mut m, 1, true);
    let actions = reply(
        &mut m,
        &context,
        Ok(json!({"reviewThreadId":"子线程","turn":{"id":"本轮"}})),
    );
    assert_eq!(
        sends(&actions)
            .into_iter()
            .find(|(_, v)| v["id"] == 42)
            .unwrap()
            .1["result"]["runId"],
        "run-测试-000001"
    );
}
#[test]
fn 元数据按连接消费校验大小并清理最旧及断开登记() {
    let mut m = Machine::new(false);
    m.record("线程").subscribed = true;
    let meta = json!({"source":"run","template":"中文模板"});
    assert!(response(&call(
        &mut m,
        1,
        "broker/threads/annotate",
        json!({"threadId":"线程","meta":meta})
    ))["result"]["shortId"]
        .is_string());
    call(
        &mut m,
        2,
        "broker/threads/annotate",
        json!({"threadId":"线程","meta":{"source":"review"}}),
    );
    for bad in [
        json!({}),
        json!({"source":"other"}),
        json!({"source":"run","approval":7}),
        json!({"source":"run","template":"中".repeat(MAX_META_BYTES)}),
    ] {
        assert_eq!(
            response(&call(
                &mut m,
                3,
                "broker/threads/annotate",
                json!({"threadId":"线程","meta":bad})
            ))["error"]["code"],
            -32602
        );
    }
    apply(
        &mut m,
        client(request(1, 42, "turn/start", json!({"threadId":"线程"}))),
    );
    assert_eq!(m.runs.runs["run-测试-000001"].meta, Some(meta));
    assert!(!m.records["线程"].pending_meta.contains_key(&1));
    assert!(m.records["线程"].pending_meta.contains_key(&2));
    for conn in 3..(MAX_PENDING_META as u64 + 3) {
        call(
            &mut m,
            conn,
            "broker/threads/annotate",
            json!({"threadId":"线程","meta":{"source":"run"}}),
        );
    }
    assert_eq!(m.records["线程"].pending_meta.len(), MAX_PENDING_META);
    assert!(!m.records["线程"].pending_meta.contains_key(&2));
    apply(&mut m, Event::Disconnected { conn: 3 });
    assert_eq!(m.records["线程"].pending_meta.len(), MAX_PENDING_META - 1);
}
#[test]
fn 交接覆盖参数归属幂等及运行结束结果() {
    let (mut m, _) = running();
    connected(&mut m, 1, true);
    assert_eq!(
        response(&detach(&mut m, 2))["error"]["data"]["reason"],
        "notOwner"
    );
    assert_eq!(
        response(&call(
            &mut m,
            1,
            "broker/threads/detach",
            json!({"threadId":"线程","runId":"无","policy":"auto"})
        ))["error"]["data"]["reason"],
        "unknownRun"
    );
    assert_eq!(
        response(&call(
            &mut m,
            1,
            "broker/threads/detach",
            json!({"threadId":"别处","runId":"run-测试-000001","policy":"auto"})
        ))["error"]["data"]["reason"],
        "runMismatch"
    );
    for params in [
        json!({"threadId":"线程","runId":"run-测试-000001","policy":"不合法"}),
        json!({"threadId":"线程","runId":"run-测试-000001","policy":"auto","approvalsReviewer":9}),
    ] {
        assert_eq!(
            response(&call(&mut m, 1, "broker/threads/detach", params))["error"]["code"],
            -32602
        );
    }
    assert_eq!(
        response(&detach(&mut m, 1))["result"]["outcome"],
        "detached"
    );
    assert_eq!(
        response(&detach(&mut m, 2))["result"]["outcome"],
        "alreadyDetached"
    );
    apply(&mut m, completed_ready());
    let result = response(&detach(&mut m, 1));
    assert_eq!(result["result"]["outcome"], "runEnded");
    assert_eq!(result["result"]["result"]["turns"][0]["ready"], "ready");
    let (mut joined, _) = start(false, "turn/steer");
    assert_eq!(
        response(&detach(&mut joined, 1))["error"]["data"]["reason"],
        "joined"
    );
}
#[test]
fn 排队交接自有加入超时断开与释放均只结算一次() {
    for outcome in [
        "own",
        "joined",
        "timeout",
        "disconnect",
        "release",
        "shutdown",
    ] {
        let (mut m, _) = start(true, "turn/start");
        connected(&mut m, 1, true);
        let queued = detach(&mut m, 1);
        assert!(sends(&queued).is_empty());
        let deadline = timer(&queued, true);
        assert_eq!(deadline.2, DETACH_WAIT_MS);
        assert_eq!(
            response(&detach(&mut m, 1))["error"]["data"]["reason"],
            "detachPending"
        );
        let mut actions = Vec::new();
        match outcome {
            "own" | "joined" => m.settle(
                "线程",
                outcome == "joined",
                Some("本轮".into()),
                &mut actions,
            ),
            "timeout" => actions = fire(&mut m, &deadline),
            "disconnect" => actions = apply(&mut m, Event::Disconnected { conn: 1 }),
            "release" => m.release("线程", &mut actions),
            _ => actions = apply(&mut m, Event::Shutdown),
        }
        if outcome == "disconnect" {
            assert!(!sends(&actions).iter().any(|(_, v)| v["id"] == 99));
            assert!(actions.iter().any(|a| matches!(a,Action::Store(crate::store::StoreOp::AppendLog {lines,..}) if lines.iter().any(|l| l.contains("交接撤销")))));
        } else {
            let value = response(&actions);
            let expected = match outcome {
                "own" => "detached",
                "release" => "runEnded",
                "timeout" => "settlingTimeout",
                "shutdown" => "shuttingDown",
                _ => "joined",
            };
            assert!(
                value["result"]["outcome"] == expected
                    || value["error"]["data"]["reason"] == expected
            );
            assert_eq!(
                sends(&actions)
                    .iter()
                    .filter(|(_, v)| v["id"] == 99)
                    .count(),
                1
            );
        }
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::Unschedule {
                key: TimerKey::DetachDeadline(_)
            }
        )));
        assert!(!sends(&fire(&mut m, &deadline))
            .iter()
            .any(|(_, v)| v["id"] == 99));
    }
}
#[test]
fn 排队交接重试不会被旧期限结算且定案回放完成走运行结束() {
    let (mut m, _) = start(true, "turn/start");
    let first = timer(&detach(&mut m, 1), true);
    fire(&mut m, &first);
    let second = timer(&detach(&mut m, 1), true);
    assert!(sends(&fire(&mut m, &first)).is_empty());
    assert!(m.claims["线程"].pending_detach.is_some());
    apply(&mut m, completed_ready());
    let mut actions = Vec::new();
    m.settle("线程", false, Some("本轮".into()), &mut actions);
    assert_eq!(response(&actions)["result"]["outcome"], "runEnded");
    assert!(sends(&fire(&mut m, &second)).is_empty());
}
#[test]
fn 原转发交接沿用截止并撤销计时且迟到回答失效() {
    let (mut m, _) = running();
    connected(&mut m, 1, true);
    m.clock.now_ms = 123;
    apply(
        &mut m,
        Event::ServerRequest {
            id: json!(7),
            forward_id: "转发".into(),
            method: "item/commandExecution/requestApproval".into(),
            params: Some(json!({"threadId":"线程","turnId":"本轮"})),
        },
    );
    let pending = m.requests.pending["转发"].clone();
    assert_eq!(
        pending.deadline_ms,
        123 + crate::requests::FORWARD_TIMEOUT_MS
    );
    assert_eq!(pending.method, "item/commandExecution/requestApproval");
    m.clock.now_ms += 500;
    let actions = detach(&mut m, 1);
    assert!(m.requests.pending.is_empty());
    assert!(actions.iter().any(|a| matches!(a,Action::CancelForwardTimer {forward_id,generation} if forward_id == "转发" && *generation == pending.generation)));
    assert!(m
        .inbox
        .hook_log
        .iter()
        .any(|s| s == "inbox_register_request"));
    let late = apply(
        &mut m,
        Event::ForwardResponse {
            conn: 1,
            id: json!("转发"),
            result: Some(json!({})),
            error: None,
        },
    );
    assert!(!late
        .iter()
        .any(|a| matches!(a, Action::ServerResult { .. } | Action::ServerError { .. })));
    assert!(apply(
        &mut m,
        Event::ForwardTimeout {
            forward_id: "转发".into(),
            generation: pending.generation
        }
    )
    .is_empty());
}
#[test]
fn 观察快照预算三层及重复订阅断开清理() {
    let (mut m, _) = running();
    connected(&mut m, 1, true);
    let snapshot = response(&watch(&mut m, 2, "线程"));
    assert_eq!(snapshot["result"]["run"]["runId"], "run-测试-000001");
    assert_eq!(snapshot["result"]["thread"]["owner"], "client");
    watch(&mut m, 2, "线程");
    assert_eq!(m.records["线程"].watchers, vec![2]);
    for c in 3..=(MAX_WATCHERS as u64 + 1) {
        watch(&mut m, c, "线程");
    }
    assert_eq!(
        response(&watch(&mut m, 999, "线程"))["error"]["data"]["reason"],
        "tooManyWatchers"
    );
    apply(&mut m, Event::Disconnected { conn: 2 });
    assert!(!m.records["线程"].watchers.contains(&2));
    assert!(m.claims.contains_key("线程"));
    let mut m = Machine::new(false);
    for i in 0..MAX_WATCHERS_PER_CONN {
        m.record(&format!("线程{i}")).watchers.push(1);
    }
    m.record("额外");
    assert_eq!(
        response(&watch(&mut m, 1, "额外"))["error"]["data"]["reason"],
        "tooManyWatchers"
    );
    let mut m = Machine::new(false);
    for i in 0..(MAX_WATCHERS_TOTAL / MAX_WATCHERS) {
        m.record(&format!("线程{i}")).watchers = (1..=MAX_WATCHERS as u64).collect();
    }
    m.record("额外");
    assert_eq!(
        response(&watch(&mut m, 999, "额外"))["error"]["data"]["reason"],
        "tooManyWatchers"
    );
    assert_eq!(
        response(&watch(&mut m, 1, "未知"))["error"]["data"]["reason"],
        "unknownThread"
    );
}
#[test]
fn 释放后结果通知仅发扩展观察者且结束通知包含原在线所有者() {
    let (mut m, _) = running();
    connected(&mut m, 1, true);
    connected(&mut m, 2, true);
    connected(&mut m, 3, false);
    watch(&mut m, 2, "线程");
    watch(&mut m, 3, "线程");
    let actions = apply(&mut m, turn("turn/completed", "本轮"));
    assert!(m.claims.is_empty());
    assert_eq!(
        sends(&actions)
            .iter()
            .filter(|(_, v)| v["method"] == "broker/run/ended")
            .count(),
        2
    );
    assert!(m.watched_results_pending());
    let read = first_request(&actions);
    assert_eq!(read.method, "thread/items/list");
    assert_eq!(read.params.as_ref().unwrap()["threadId"], "线程");
    assert_eq!(read.params.as_ref().unwrap()["turnId"], "本轮");
    assert!(read.params.as_ref().unwrap()["cursor"].is_null());
    let actions = reply(
        &mut m,
        &read,
        Ok(
            json!({"data":[{"turnId":"本轮","item":{"type":"agentMessage","text":"补全"},"startedAtMs":null,"completedAtMs":null}],"nextCursor":null,"backwardsCursor":null}),
        ),
    );
    let results: Vec<_> = sends(&actions)
        .into_iter()
        .filter(|(_, v)| v["method"] == "broker/thread/result")
        .collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, 2);
    assert_eq!(results[0].1["params"]["turn"], 0);
    assert!(!m.watched_results_pending());
    assert!(reply(&mut m, &read, Ok(json!({}))).iter().all(
        |a| !matches!(a,Action::Send {message,..} if message["method"] == "broker/thread/result")
    ));
    let actions = apply(
        &mut m,
        notify(
            "thread/status/changed",
            json!({"threadId":"线程","status":"idle"}),
        ),
    );
    assert_eq!(
        sends(&actions).iter().map(|(c, _)| *c).collect::<Vec<_>>(),
        vec![2, 3]
    );
}
#[test]
fn 立即就绪与启动失败通知恰好一次且所有者兼观察者去重() {
    for failed in [false, true] {
        let (mut m, context) = start(false, "turn/start");
        connected(&mut m, 1, true);
        watch(&mut m, 1, "线程");
        let actions = if failed {
            reply(
                &mut m,
                &context,
                Err(json!({"code":-1,"message":"启动失败"})),
            )
        } else {
            reply(&mut m, &context, Ok(json!({"turn":{"id":"本轮"}})));
            apply(&mut m, completed_ready())
        };
        for method in ["broker/thread/result", "broker/run/ended"] {
            assert_eq!(
                sends(&actions)
                    .iter()
                    .filter(|(_, v)| v["method"] == method)
                    .count(),
                1
            );
        }
        assert!(m.runs.notices.is_empty());
        assert_eq!(m.runs.runs["run-测试-000001"].turns[0].ready, Ready::Ready);
    }
}
#[test]
fn 回收边界观察者进出代号换代以及停止撤销() {
    let (mut m, _) = running();
    connected(&mut m, 1, true);
    m.clock.mono_ms = 100;
    let first = timer(&detach(&mut m, 1), false);
    assert_eq!(first.2, RECLAIM_MS);
    m.clock.mono_ms += 50;
    let watched = timer(&watch(&mut m, 2, "线程"), false);
    assert!(fire(&mut m, &first).is_empty());
    m.clock.mono_ms += RECLAIM_MS;
    let blocked = fire(&mut m, &watched);
    assert!(!blocked.iter().any(|a| matches!(
        a,
        Action::Schedule {
            kind: TimerKind::Reclaim { .. },
            ..
        }
    )));
    assert!(!m.stop_busy("线程"));
    let left = apply(&mut m, Event::Disconnected { conn: 2 });
    let last = timer(&left, false);
    assert_eq!(last.2, RECLAIM_MS);
    m.clock.mono_ms += RECLAIM_MS - 1;
    let early = fire(&mut m, &last);
    let one = timer(&early, false);
    assert_eq!(one.2, 1);
    m.clock.mono_ms += 1;
    fire(&mut m, &one);
    assert!(m.stop_busy("线程"));
    assert!(fire(&mut m, &one).is_empty());
    let (mut m, _) = running();
    let old = timer(&detach(&mut m, 1), false);
    m.claims.get_mut("线程").unwrap().claim_id += 1;
    assert!(fire(&mut m, &old).is_empty());
}
#[test]
fn 墓碑恢复与容量限制交接不会误报未知运行() {
    let (mut m, _) = running();
    apply(&mut m, completed_ready());
    let original = m.runs.runs["run-测试-000001"].clone();
    let mut newer = original.clone();
    newer.run_id = "新运行".into();
    newer.started_at += 1;
    let mut index = m.runs.index.clone();
    index.threads.values_mut().next().unwrap().last_run_id = Some("新运行".into());
    let mut restored = Machine::new(false);
    apply(
        &mut restored,
        Event::Restore {
            index,
            runs: vec![original, newer],
            inbox: None,
        },
    );
    assert_eq!(
        restored.run_lookup("run-测试-000001"),
        RunLookup::Retired {
            thread_id: "线程".into()
        }
    );
    let result = response(&detach(&mut restored, 1));
    assert_eq!(result["result"]["outcome"], "runEnded");
    assert!(result["result"].get("result").is_none());
    assert_eq!(
        response(&call(
            &mut restored,
            1,
            "broker/threads/detach",
            json!({"threadId":"别处","runId":"run-测试-000001","policy":"auto"})
        ))["error"]["data"]["reason"],
        "runMismatch"
    );
    for i in 0..crate::runs::MAX_RETIRED_RUNS {
        restored.runs.retire(format!("墓碑{i}"), "线程".into());
    }
    assert_eq!(restored.run_lookup("run-测试-000001"), RunLookup::Unknown);
    assert!(matches!(
        restored.run_lookup("墓碑0"),
        RunLookup::Retired { .. }
    ));
}
#[test]
fn 关闭拒绝变更但允许查询且停止中的在线所有者仍显示客户端() {
    let (mut m, _) = running();
    connected(&mut m, 1, true);
    call(&mut m, 1, "broker/threads/stop", json!({"threadId":"线程"}));
    let summary = m.thread_summary("线程").unwrap();
    assert_eq!(summary.state, SummaryState::Stopping);
    assert_eq!(summary.owner, Some(SummaryOwner::Client));
    m.closing = true;
    assert_eq!(
        response(&detach(&mut m, 1))["error"]["data"]["reason"],
        "shuttingDown"
    );
    assert_eq!(
        response(&call(
            &mut m,
            1,
            "broker/threads/annotate",
            json!({"threadId":"线程","meta":{"source":"run"}})
        ))["error"]["data"]["reason"],
        "shuttingDown"
    );
    assert!(response(&watch(&mut m, 2, "线程"))["result"].is_object());
    assert!(
        response(&call(&mut m, 2, "broker/threads/list", json!({"limit":1})))["result"]["threads"]
            .is_array()
    );
}
#[test]
fn 审查排队交接后子线程继承托管且父子观察者结果去重() {
    let (mut m, context) = start(true, "review/start");
    connected(&mut m, 1, true);
    connected(&mut m, 2, true);
    watch(&mut m, 2, "线程");
    m.record("子线程");
    watch(&mut m, 2, "子线程");
    detach(&mut m, 1);
    let actions = reply(
        &mut m,
        &context,
        Ok(json!({"reviewThreadId":"子线程","turn":{"id":"审查轮"}})),
    );
    assert_eq!(response(&actions)["result"]["outcome"], "detached");
    assert!(matches!(
        m.claims["子线程"].owner(),
        Some(Owner::Broker { .. })
    ));
    let actions = apply(
        &mut m,
        notify(
            "turn/completed",
            json!({"threadId":"子线程","turn":{"id":"审查轮","status":"completed","items":[]}}),
        ),
    );
    let read = first_request(&actions);
    assert_eq!(read.method, "thread/items/list");
    assert_eq!(read.params.as_ref().unwrap()["threadId"], "子线程");
    assert_eq!(read.params.as_ref().unwrap()["turnId"], "审查轮");
    assert!(read.params.as_ref().unwrap()["cursor"].is_null());
    assert!(m.watched_results_pending());
    let actions = reply(
        &mut m,
        &read,
        Ok(
            json!({"data":[{"turnId":"审查轮","item":{"type":"agentMessage","text":"审查结果"},"startedAtMs":null,"completedAtMs":null}],"nextCursor":null,"backwardsCursor":null}),
        ),
    );
    assert_eq!(
        sends(&actions)
            .iter()
            .filter(|(_, v)| v["method"] == "broker/thread/result")
            .count(),
        1
    );
}

#[test]
fn 元数据序列化字节上限边界与列表排序参数() {
    let mut m = Machine::new(false);
    m.record("旧线程");
    let base = serde_json::to_vec(&json!({"source":"run","template":""}))
        .unwrap()
        .len();
    let exact = json!({"source":"run","template":"a".repeat(MAX_META_BYTES-base)});
    let result = response(&call(
        &mut m,
        1,
        "broker/threads/annotate",
        json!({"threadId":"旧线程","meta":exact}),
    ));
    assert!(result.get("result").is_some());
    let overflow = json!({"source":"run","template":"a".repeat(MAX_META_BYTES-base+1)});
    assert_eq!(
        response(&call(
            &mut m,
            1,
            "broker/threads/annotate",
            json!({"threadId":"旧线程","meta":overflow})
        ))["error"]["code"],
        -32602
    );
    m.clock.now_ms = 20;
    m.record("新线程");
    call(
        &mut m,
        1,
        "broker/threads/annotate",
        json!({"threadId":"新线程","meta":{"source":"run"}}),
    );
    let result = response(&call(&mut m, 1, "broker/threads/list", json!({"limit":1})));
    assert_eq!(result["result"]["threads"].as_array().unwrap().len(), 1);
    assert_eq!(result["result"]["threads"][0]["threadId"], "新线程");
    for limit in [json!(-1), json!(1.5), json!("1"), Value::Null] {
        assert_eq!(
            response(&call(
                &mut m,
                1,
                "broker/threads/list",
                json!({"limit":limit})
            ))["error"]["code"],
            -32602
        );
    }
    assert_eq!(
        response(&call(
            &mut m,
            1,
            "broker/threads/annotate",
            json!({"threadId":"无","meta":{"source":"run"}})
        ))["error"]["data"]["reason"],
        "unknownThread"
    );
}
#[test]
fn 无观察者补读不算忙重试有观察者算忙最终不可用仅通知一次() {
    let (mut m, _) = running();
    let actions = apply(&mut m, turn("turn/completed", "本轮"));
    assert!(!m.watched_results_pending());
    connected(&mut m, 2, true);
    watch(&mut m, 2, "线程");
    assert!(m.watched_results_pending());
    let mut read = first_request(&actions);
    for attempt in 0..4 {
        let actions = reply(&mut m, &read, Err(json!({"message":"补读失败"})));
        if attempt == 3 {
            let notes: Vec<_> = sends(&actions)
                .into_iter()
                .filter(|(_, v)| v["method"] == "broker/thread/result")
                .collect();
            assert_eq!(notes.len(), 1);
            assert_eq!(notes[0].1["params"]["result"]["ready"], "unavailable");
            assert!(!m.watched_results_pending());
        } else {
            assert!(m.watched_results_pending());
            let (token, kind) = actions
                .iter()
                .find_map(|a| match a {
                    Action::Schedule {
                        token,
                        kind: kind @ TimerKind::Run(crate::runs::RunTimer::ResultRetry(_)),
                        ..
                    } => Some((token.clone(), kind.clone())),
                    _ => None,
                })
                .unwrap();
            read = first_request(&apply(&mut m, Event::Timer { token, kind }));
        }
    }
}
#[test]
fn 关闭封口将待补读结算不可用且迟到回包无新通知() {
    let (mut m, _) = running();
    connected(&mut m, 2, true);
    watch(&mut m, 2, "线程");
    let read = first_request(&apply(&mut m, turn("turn/completed", "本轮")));
    apply(&mut m, Event::Shutdown);
    let actions = apply(&mut m, Event::RunsFinalize);
    assert_eq!(
        sends(&actions)
            .iter()
            .filter(|(_, v)| v["method"] == "broker/thread/result")
            .count(),
        1
    );
    assert!(!m.watched_results_pending());
    assert!(sends(&reply(&mut m, &read, Ok(json!({})))).is_empty());
    assert!(sends(&apply(&mut m, Event::RunsFinalize)).is_empty());
}

#[test]
fn 审查回包后用父线程交接实际子线程且断开不会停止托管运行() {
    let (mut m, context) = start(false, "review/start");
    connected(&mut m, 1, true);
    reply(
        &mut m,
        &context,
        Ok(json!({"reviewThreadId":"子线程","turn":{"id":"审查轮"}})),
    );
    assert!(!m.claims.contains_key("线程"));
    assert_eq!(
        response(&detach(&mut m, 2))["error"]["data"]["reason"],
        "notOwner"
    );
    apply(
        &mut m,
        Event::ServerRequest {
            id: json!(7),
            forward_id: "审查转发".into(),
            method: "item/commandExecution/requestApproval".into(),
            params: Some(json!({"threadId":"子线程","turnId":"审查轮"})),
        },
    );
    let actions = detach(&mut m, 1);
    let result = response(&actions);
    assert_eq!(result["result"]["outcome"], "detached");
    assert_eq!(result["result"]["turnId"], "审查轮");
    assert_eq!(result["result"]["pending"].as_array().unwrap().len(), 1);
    assert_eq!(result["result"]["pending"][0]["threadId"], "子线程");
    assert_eq!(timer(&actions, false).0.thread_id, "子线程");
    assert!(m.requests.pending.is_empty());
    assert!(matches!(
        m.claims["子线程"].owner(),
        Some(Owner::Broker { .. })
    ));
    let actions = apply(&mut m, Event::Disconnected { conn: 1 });
    assert!(!m.claims["子线程"].owner_gone);
    assert!(!m.stop_busy("子线程"));
    assert!(!actions
        .iter()
        .any(|a| matches!(a, Action::Request(c) if c.method == "turn/interrupt")));
}

#[test]
fn 审查交接后以父线程重试返回子线程当前快照且不改变父线程新认领() {
    let (mut m, context) = start(false, "review/start");
    connected(&mut m, 1, true);
    connected(&mut m, 2, true);
    reply(
        &mut m,
        &context,
        Ok(json!({"reviewThreadId":"子线程","turn":{"id":"审查轮"}})),
    );
    let first = call(
        &mut m,
        1,
        "broker/threads/detach",
        json!({"threadId":"子线程","runId":"run-测试-000001","policy":"auto"}),
    );
    assert_eq!(response(&first)["result"]["outcome"], "detached");
    assert!(response(&first)["result"]["pending"]
        .as_array()
        .unwrap()
        .is_empty());
    apply(
        &mut m,
        Event::ClientRequest {
            request: request(2, 43, "turn/start", json!({"threadId":"线程"})),
            run_id: Some("父线程新运行".into()),
        },
    );
    apply(
        &mut m,
        Event::ServerRequest {
            id: json!(8),
            forward_id: "新审批".into(),
            method: "item/commandExecution/requestApproval".into(),
            params: Some(json!({"threadId":"子线程","turnId":"审查轮"})),
        },
    );
    let parent = m.claims["线程"].clone();
    let child = m.claims["子线程"].clone();
    let retried = detach(&mut m, 1);
    let result = response(&retried);
    assert_eq!(result["result"]["outcome"], "alreadyDetached");
    assert_eq!(result["result"]["turnId"], "审查轮");
    assert_eq!(result["result"]["pending"].as_array().unwrap().len(), 1);
    assert_eq!(result["result"]["pending"][0]["threadId"], "子线程");
    assert_eq!(m.claims["线程"], parent);
    assert_eq!(m.claims["子线程"], child);
    assert!(!retried.iter().any(|a| matches!(
        a,
        Action::Schedule {
            kind: TimerKind::Reclaim { .. },
            ..
        }
    )));
}

fn 声明切换后的暂存回包(initial: bool, final_cxb: bool) {
    // 定案、自有或加入、读取失败先交付、关闭排空都必须走同一个最终出口。
    for exit in ["own", "joined", "undecided", "shutdown"] {
        let (mut m, context) = start(true, "turn/start");
        connected(&mut m, 1, initial);
        let upstream = json!({"turn":{"id":"本轮"},"保留字段":{"值":1}});
        let received = reply(&mut m, &context, Ok(upstream.clone()));
        let read = first_request(&received);
        assert!(!sends(&received).iter().any(|(_, v)| v["id"] == 42));
        assert_eq!(
            m.claims["线程"].settlement_reply.as_ref().unwrap().1,
            upstream
        );
        apply(
            &mut m,
            Event::Initialized {
                conn: 1,
                cxb: final_cxb,
            },
        );
        let actions = match exit {
            "own" => reply(
                &mut m,
                &read,
                Ok(json!({"thread":{"turns":[{"id":"本轮","status":"inProgress"}]}})),
            ),
            "joined" => reply(
                &mut m,
                &read,
                Ok(json!({"thread":{"turns":[{"id":"外轮","status":"inProgress"}]}})),
            ),
            "undecided" => reply(&mut m, &read, Err(json!({"code":-1,"message":"核实失败"}))),
            _ => apply(&mut m, Event::Shutdown),
        };
        let replies: Vec<_> = sends(&actions)
            .into_iter()
            .filter(|(_, v)| v["id"] == 42)
            .collect();
        assert_eq!(replies.len(), 1, "出口：{exit}");
        let result = &replies[0].1["result"];
        assert_eq!(result["保留字段"], upstream["保留字段"]);
        if final_cxb {
            assert_eq!(result["runId"], "run-测试-000001");
            assert_eq!(result["shortId"], m.runs.runs["run-测试-000001"].short_id);
        } else {
            assert!(result.get("runId").is_none());
            assert!(result.get("shortId").is_none());
        }
        match exit {
            "own" => assert_eq!(result.get("absorbedBy"), Some(&Value::Null)),
            "joined" => assert_eq!(result["absorbedBy"], "外轮"),
            _ => assert!(result.get("absorbedBy").is_none()),
        }
    }
}

#[test]
fn 暂存回包后撤销扩展声明最终不附加运行字段() {
    声明切换后的暂存回包(true, false);
}

#[test]
fn 暂存回包后新增扩展声明最终附加运行字段() {
    声明切换后的暂存回包(false, true);
}

#[test]
fn 恢复屏障严格区分未知登记键与其他错误() {
    for case in 0..7 {
        let mut m = Machine::new(false);
        m.runs.ensure_thread("线程", 1, 1);
        call(
            &mut m,
            1,
            "broker/threads/resumeOverrides",
            json!({"threadId":"线程","overrides":{"config":{"bad.key":true,"keep.key":7},"developerInstructions":"保持指令"}}),
        );
        let actions = apply(
            &mut m,
            client(request(1, 42, "turn/start", json!({"threadId":"线程"}))),
        );
        let context = first_request(&actions);
        let message = match case { 1 => "failed to load configuration", 2 => "failed to load configuration: unknown configuration field `outside.key` in -c/--config override", 3 => "failed to load configuration: unknown configuration field `bad.key` in config.toml", _ => "failed to load configuration: unknown configuration field `bad.key` in -c/--config override" };
        if case == 5 {
            m.runs
                .index
                .threads
                .values_mut()
                .next()
                .unwrap()
                .resume_overrides
                .as_mut()
                .unwrap()
                .config
                .as_mut()
                .unwrap()
                .insert("bad.key".into(), json!(false));
        }
        if case == 6 {
            m.claims.clear();
        }
        let actions = reply(
            &mut m,
            &context,
            Err(json!({"code":if case==4 {-32602} else {-32600},"message":message})),
        );
        let resumes: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Request(r) if r.method == "thread/resume" => Some(r),
                _ => None,
            })
            .collect();
        assert_eq!(resumes.len(), usize::from(case == 0));
        if case == 0 {
            assert_eq!(
                resumes[0].params,
                Some(
                    json!({"threadId":"线程","config":{"keep.key":7},"developerInstructions":"保持指令"})
                )
            );
            assert!(actions
                .iter()
                .any(|a| matches!(a, Action::Store(crate::store::StoreOp::PutThreads(_)))));
            let retry = resumes[0].clone();
            let next = reply(&mut m, &retry, Ok(json!({"thread":{"id":"线程"}})));
            assert_eq!(first_request(&next).method, "turn/start");
        }
    }
}
