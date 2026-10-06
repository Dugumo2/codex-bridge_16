//! N4 第 5 节：父线程停止定位与迁移通知，不依赖真实时间或文件。
use super::*;

#[test]
fn 迁移登记保留未订阅父线程的真实所有者() {
    let (mut m, review) = started(false, "review/start");
    apply(&mut m, Event::Initialized { conn: 1, cxb: true });
    assert!(m.records["线程"].watchers.is_empty());
    let a = reply(
        &mut m,
        &review,
        Ok(json!({"reviewThreadId":"子线程","turn":{"id":"审查轮"}})),
    );
    assert_eq!(a.iter().filter(|a|matches!(a,Action::Send {conn:1,message} if message["method"]=="broker/run/moved")).count(),1);
}

#[test]
fn 父认领优先于仍指向审查的最近记录() {
    let mut m = moved();
    let mut claim = running().claims.remove("线程").unwrap();
    claim.run_id = Some("新认领".into());
    m.claims.insert("线程".into(), claim);
    let a = stop(&mut m, 90, false);
    assert_eq!(
        req(&a, "thread/goal/get").params.unwrap()["threadId"],
        "线程"
    );
    assert!(m.stops.operations.values().all(|op| op.thread == "线程"));
}

#[test]
fn 内联无认领不重定向且普通失联运行仍走核实() {
    let mut m = moved();
    m.claims.clear();
    m.runs.runs.get_mut("run-1").unwrap().review_thread_id = Some("线程".into());
    let a = stop(&mut m, 90, false);
    assert_eq!(
        req(&a, "thread/goal/get").params.unwrap()["threadId"],
        "线程"
    );
    assert!(m.stops.operations.values().next().unwrap().already_stopped);
    for state in [RunState::Stopping, RunState::Lost] {
        let mut m = running();
        m.claims.clear();
        let run = m.runs.runs.get_mut("run-1").unwrap();
        run.state = state;
        run.stop_outcome = Some(StopOutcome::Unconfirmed);
        let a = stop(&mut m, 90, false);
        assert_eq!(req(&a, "thread/read").params.unwrap()["threadId"], "线程");
        assert!(!m.stops.operations.values().next().unwrap().already_stopped);
    }
}

#[test]
fn 父停止四十秒到期后迟到完成收尾不重复回包() {
    let mut m = moved();
    let a = stop(&mut m, 90, false);
    reply(
        &mut m,
        &req(&a, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    let b = apply(
        &mut m,
        timer(&a, |k| {
            matches!(k, TimerKind::Stop(_, StopTimer::Waiter(_)))
        }),
    );
    assert_eq!(outcome(&b, 90)["stopOutcome"], "unconfirmed");
    assert!(m
        .stops
        .operations
        .values()
        .next()
        .unwrap()
        .waiters
        .is_empty());
    let b = child_completed(&mut m);
    assert!(!b
        .iter()
        .any(|a| matches!(a,Action::Send {message,..} if message["id"]==90)));
    assert!(m.stops.operations.is_empty());
    assert_eq!(
        m.runs.runs["run-1"].stop_outcome,
        Some(StopOutcome::Confirmed)
    );
}

fn moved() -> Machine {
    let (mut m, review) = started(false, "review/start");
    reply(
        &mut m,
        &review,
        Ok(json!({"reviewThreadId":"子线程","turn":{"id":"审查轮"}})),
    );
    m
}
fn child_completed(m: &mut Machine) -> Vec<Action> {
    apply(
        m,
        Event::Notification {
            method: "turn/completed".into(),
            params: Some(
                json!({"threadId":"子线程","turn":{"id":"审查轮","status":"interrupted","items":[]}}),
            ),
        },
    )
}
fn only_child(actions: &[Action]) {
    for action in actions {
        if let Action::Request(r) = action {
            if r.method.starts_with("thread/goal/")
                || r.method == "turn/interrupt"
                || r.method == "thread/read"
            {
                assert_eq!(
                    r.params.as_ref().unwrap()["threadId"],
                    "子线程",
                    "实际操作必须只作用于子线程"
                );
            }
        }
    }
}
fn rejected(actions: &[Action], reason: &str) {
    assert!(
        actions.iter().any(|a| matches!(a, Action::Send {message,..}
        if message["id"] == 90 && message["error"]["data"]["reason"] == reason)),
        "应明确拒绝：{actions:?}"
    );
    assert!(
        !actions.iter().any(|a| matches!(a, Action::Request(_))),
        "拒绝不得发起上游操作"
    );
}

#[test]
fn 父空闲停止客户端与托管审查仅操作子线程并等待唯一判定() {
    for managed in [false, true] {
        let mut m = moved();
        if managed {
            m.claims.get_mut("子线程").unwrap().state = State::Running {
                owner: Owner::Broker {
                    policy: crate::policy::Policy::Never,
                    approvals_reviewer: None,
                    owned_at: 0,
                    last_watcher_at: 0,
                },
                turn_id: "审查轮".into(),
                joined: false,
            };
        }
        let initial = stop(&mut m, 90, false);
        only_child(&initial);
        assert_eq!(m.stops.operations.len(), 1);
        assert!(!initial
            .iter()
            .any(|a| matches!(a,Action::Send {message,..} if message["id"]==90)));
        let a = reply(
            &mut m,
            &req(&initial, "thread/goal/get"),
            Ok(goal("active")),
        );
        only_child(&a);
        let a = reply(&mut m, &req(&a, "thread/goal/set"), Ok(goal("paused")));
        only_child(&a);
        assert_eq!(
            req(&a, "turn/interrupt").params.unwrap()["turnId"],
            "审查轮"
        );
        let a = child_completed(&mut m);
        assert_eq!(outcome(&a, 90)["stopOutcome"], "confirmed");
        assert_eq!(outcome(&a, 90)["alreadyStopped"], false);
        assert!(m.stops.operations.is_empty());
    }
}

#[test]
fn 迁移前后停止均选同一运行且未知子线程前不操作父线程() {
    for before in [false, true] {
        let (mut m, review) = started(false, "review/start");
        if before {
            let a = stop(&mut m, 90, false);
            assert!(requests(&a, "thread/goal/get").is_empty());
            assert!(requests(&a, "turn/interrupt").is_empty());
        }
        let mut a = reply(
            &mut m,
            &review,
            Ok(json!({"reviewThreadId":"子线程","turn":{"id":"审查轮"}})),
        );
        if !before {
            a.extend(stop(&mut m, 90, false));
        }
        only_child(&a);
        assert_eq!(m.stops.operations.len(), 1);
        let op = m.stops.operations.values().next().unwrap();
        assert_eq!(op.thread, "子线程");
        assert_eq!(op.run_id.as_deref(), Some("run-1"));
        assert_eq!(op.waiters.len(), 1);
    }
}

#[test]
fn 子停止合并父重试且清除只升每个等待者独立四十秒() {
    let mut m = moved();
    let first = client(
        &mut m,
        "broker/threads/stop",
        88,
        json!({"threadId":"子线程"}),
    );
    let second = client(
        &mut m,
        "broker/threads/stop",
        89,
        json!({"threadId":"子线程","clear":true}),
    );
    let third = stop(&mut m, 90, false);
    assert_eq!(m.stops.operations.len(), 1);
    let op = m.stops.operations.values().next().unwrap();
    assert!(op.clear);
    assert_eq!(op.waiters.len(), 3);
    for a in [&first, &second, &third] {
        assert!(a.iter().any(|a| matches!(
            a,
            Action::Schedule {
                kind: TimerKind::Stop(_, StopTimer::Waiter(_)),
                delay_ms: 40_000,
                ..
            }
        )));
        only_child(a);
    }
    for (a, id, left) in [(&first, 88, 2), (&second, 89, 1), (&third, 90, 0)] {
        let a = apply(
            &mut m,
            timer(a, |k| matches!(k, TimerKind::Stop(_, StopTimer::Waiter(_)))),
        );
        assert_eq!(outcome(&a, id)["stopOutcome"], "unconfirmed");
        assert_eq!(
            m.stops.operations.values().next().unwrap().waiters.len(),
            left
        );
    }
}

#[test]
fn 子线程冲突外来停止与归属未定均拒绝() {
    for case in ["别的认领", "别的停止", "外来停止", "joined", "settling"] {
        let mut m = moved();
        match case {
            "别的认领" => m.claims.get_mut("子线程").unwrap().run_id = Some("其他运行".into()),
            "别的停止" | "外来停止" => {
                client(
                    &mut m,
                    "broker/threads/stop",
                    88,
                    json!({"threadId":"子线程"}),
                );
                let op = m.stops.operations.values_mut().next().unwrap();
                if case == "别的停止" {
                    op.run_id = Some("其他运行".into());
                } else {
                    op.foreign_claim = true;
                }
            }
            "joined" => {
                if let State::Running { joined, .. } =
                    &mut m.claims.get_mut("子线程").unwrap().state
                {
                    *joined = true;
                }
            }
            "settling" => {
                m.claims.get_mut("子线程").unwrap().state = started(true, "review/start")
                    .0
                    .claims
                    .remove("线程")
                    .unwrap()
                    .state
            }
            _ => unreachable!(),
        }
        // 直接调用入口，避免既有停止操作的后台驱动混入本请求产生的动作。
        let mut a = Vec::new();
        m.client_stop(
            ClientRequest {
                conn: 1,
                id: json!(90),
                method: "broker/threads/stop".into(),
                params: Some(json!({"threadId":"线程"})),
            },
            &mut a,
        );
        let reason = if case == "joined" || case == "settling" {
            case
        } else {
            "stopping"
        };
        rejected(&a, reason);
        if reason == "stopping" {
            assert!(a.iter().any(
                |a| matches!(a,Action::Send {message,..} if message["error"]["code"] == -32001)
            ));
        }
    }
}

#[test]
fn 父新认领先处理父且空闲父只看最近运行() {
    for active in [true, false] {
        let mut m = moved();
        let a = client(&mut m, "turn/start", 2, json!({"threadId":"线程"}));
        reply(
            &mut m,
            &req(&a, "turn/start"),
            Ok(json!({"turn":{"id":"新轮"}})),
        );
        if !active {
            turn(&mut m, "turn/completed", "新轮");
        }
        let a = stop(&mut m, 90, true);
        assert_eq!(
            req(&a, "thread/goal/get").params.unwrap()["threadId"],
            "线程"
        );
        assert!(m.stops.operations.values().all(|op| op.thread == "线程"));
        assert_eq!(m.claims["子线程"].run_id.as_deref(), Some("run-1"));
    }
}

#[test]
fn 无认领审查停止与失联均核实子线程且读失败保持未确认() {
    for state in [RunState::Stopping, RunState::Lost] {
        let mut m = moved();
        m.claims.clear();
        let run = m.runs.runs.get_mut("run-1").unwrap();
        run.state = state;
        run.stop_outcome = Some(StopOutcome::Unconfirmed);
        let a = stop(&mut m, 90, false);
        only_child(&a);
        let get = req(&a, "thread/goal/get");
        let read = req(&a, "thread/read");
        let retry = stop(&mut m, 91, true);
        only_child(&retry);
        assert_eq!(m.stops.operations.len(), 1);
        assert_eq!(m.stops.operations.values().next().unwrap().waiters.len(), 2);
        reply(&mut m, &read, Err(err()));
        reply(&mut m, &get, Err(err()));
        let a = apply(
            &mut m,
            timer(&a, |k| {
                matches!(k, TimerKind::Stop(_, StopTimer::Waiter(_)))
            }),
        );
        assert_eq!(outcome(&a, 90)["stopOutcome"], "unconfirmed");
    }
}

#[test]
fn 无认领的运行中审查拒绝但已有同运行停止可并入() {
    for existing in [false, true] {
        let mut m = moved();
        if existing {
            client(
                &mut m,
                "broker/threads/stop",
                88,
                json!({"threadId":"子线程"}),
            );
        }
        m.claims.clear();
        m.runs.runs.get_mut("run-1").unwrap().state = RunState::Running;
        let a = stop(&mut m, 90, false);
        if existing {
            assert_eq!(m.stops.operations.len(), 1);
            assert_eq!(m.stops.operations.values().next().unwrap().waiters.len(), 2);
            only_child(&a);
        } else {
            rejected(&a, "stopping");
        }
    }
}

#[test]
fn 已结束与已确认或失联旁观审查不重定向并保留父清除() {
    for (state, joined, outcome) in [
        (RunState::Ended, false, None),
        (RunState::Lost, true, Some(StopOutcome::Unconfirmed)),
        (RunState::Lost, false, Some(StopOutcome::Confirmed)),
    ] {
        let mut m = moved();
        m.claims.clear();
        let run = m.runs.runs.get_mut("run-1").unwrap();
        run.state = state;
        run.joined = joined;
        run.stop_outcome = outcome;
        let a = stop(&mut m, 90, true);
        assert_eq!(
            req(&a, "thread/goal/get").params.unwrap()["threadId"],
            "线程"
        );
        let a = reply(&mut m, &req(&a, "thread/goal/get"), Ok(goal("active")));
        assert_eq!(
            req(&a, "thread/goal/clear").params.unwrap()["threadId"],
            "线程"
        );
        let a = reply(&mut m, &req(&a, "thread/goal/clear"), Ok(json!({})));
        assert_eq!(super::outcome(&a, 90)["alreadyStopped"], true);
    }
}

#[test]
fn 子线程制动失败迟到完成及关闭均不伪造确认() {
    let mut m = moved();
    let a = stop(&mut m, 90, false);
    let b = reply(&mut m, &req(&a, "thread/goal/get"), Ok(goal("active")));
    reply(&mut m, &req(&b, "thread/goal/set"), Err(err()));
    let b = apply(
        &mut m,
        timer(&a, |k| {
            matches!(k, TimerKind::Stop(_, StopTimer::Waiter(_)))
        }),
    );
    assert_eq!(outcome(&b, 90)["stopOutcome"], "unconfirmed");
    child_completed(&mut m);
    assert!(!m.confirmed(m.stops.operations.values().next().unwrap()));
    m.shutdown_stops(&mut Vec::new());
    assert!(m.stops.operations.is_empty());
}
