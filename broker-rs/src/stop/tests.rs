//! M4 任务说明第 4 节的停止证据、时序、超时与隔离测试。
#[path = "h2b_tests.rs"]
mod h2b;
#[path = "n4_review_tests.rs"]
mod n4_review;
use super::*;
use crate::claim::status_read::{StatusDone, StatusResult, StatusTurn};
use crate::claim::{self, Event, Owner};
use crate::test_support::status_read::{
    meta, reply_status, status_requests, turns_page, StatusPhase, StatusRequest,
};
use std::time::Instant;

fn apply(m: &mut Machine, event: Event) -> Vec<Action> {
    let (next, actions) = claim::step(std::mem::take(m), event);
    *m = next;
    crate::test_support::background::empty_background(m, actions)
}
fn client(m: &mut Machine, method: &str, id: u64, params: Value) -> Vec<Action> {
    apply(
        m,
        Event::ClientRequest {
            request: ClientRequest {
                conn: 1,
                id: json!(id),
                method: method.into(),
                params: Some(params),
            },
            run_id: Some(format!("run-{id}")),
        },
    )
}
fn req(actions: &[Action], method: &str) -> RequestContext {
    requests(actions, method)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("缺少请求 {method}：{actions:?}"))
}
fn requests(actions: &[Action], method: &str) -> Vec<RequestContext> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Request(c) if c.method == method => Some(c.clone()),
            _ => None,
        })
        .collect()
}
fn reply(m: &mut Machine, c: &RequestContext, result: Result<Value, Value>) -> Vec<Action> {
    apply(
        m,
        Event::Reply {
            context: c.clone(),
            result,
        },
    )
}
/// 经单步驱动回答一次状态读取，结果与旧全量快照 `{"thread":{"turns":[...]}}` 同义：`turns` 按旧回包
/// 的升序给出 (id, status)。元数据有 inProgress 时为 active、否则为 idle；轮次页按上游倒序一页给完。
/// `first` 是这次读取发出的第一个请求（Activity 为元数据读，Turn 为轮次页）。
fn snapshot(m: &mut Machine, first: &RequestContext, turns: &[(&str, &str)]) -> Vec<Action> {
    let mut request = status_request(first);
    let thread = request.params.as_ref().unwrap()["threadId"]
        .as_str()
        .unwrap()
        .to_owned();
    let active = turns.iter().any(|(_, status)| *status == "inProgress");
    let descending: Vec<_> = turns.iter().rev().copied().collect();
    let mut actions = Vec::new();
    loop {
        let result = match request.phase {
            StatusPhase::Metadata => meta(&thread, if active { "active" } else { "idle" }),
            StatusPhase::Turns => turns_page(&descending, None),
            StatusPhase::Items => panic!("停止与对账读取不读条目"),
        };
        let next = reply_status(m, &request, Ok(result));
        let follow = status_requests(&next)
            .into_iter()
            .find(|r| r.read_id == request.read_id);
        actions.extend(next);
        match follow {
            Some(follow) => request = follow,
            None => return actions,
        }
    }
}
/// 请求上下文对应的状态读取请求；不是状态读取则说明用例取错了请求。
fn status_request(context: &RequestContext) -> StatusRequest {
    status_requests(&[Action::Request(context.clone())])
        .pop()
        .unwrap_or_else(|| panic!("不是状态读取请求：{context:?}"))
}
fn err() -> Value {
    proto::error_object(-32000, "测试失败", None)
}
fn goal(status: &str) -> Value {
    json!({"goal":{"threadId":"线程","status":status,"objective":"目标"}})
}
fn turn(m: &mut Machine, method: &str, id: &str) -> Vec<Action> {
    apply(
        m,
        Event::Notification {
            method: method.into(),
            params: Some(json!({"threadId":"线程","turn":{"id":id}})),
        },
    )
}
fn started(shared: bool, method: &str) -> (Machine, RequestContext) {
    started_params(
        shared,
        method,
        json!({"threadId":"线程","input":[{"type":"text","text":"本次输入"}]}),
    )
}

/// 显式 legacy 服务器夹具：用例手工注入 detached 子线程回包。
fn started_legacy_review(shared: bool) -> (Machine, RequestContext) {
    started_params(
        shared,
        "review/start",
        json!({"threadId":"线程","delivery":"detached"}),
    )
}

fn started_params(shared: bool, method: &str, params: Value) -> (Machine, RequestContext) {
    let mut m = Machine::new(shared);
    apply(&mut m, Event::Connected { conn: 1 });
    m.records.entry("线程".into()).or_default().subscribed = true;
    let a = client(&mut m, method, 1, params);
    (m, req(&a, method))
}
fn running() -> Machine {
    let (mut m, c) = started(false, "turn/start");
    reply(&mut m, &c, Ok(json!({"turn":{"id":"本轮"}})));
    m
}
fn stop(m: &mut Machine, id: u64, clear: bool) -> Vec<Action> {
    client(
        m,
        "broker/threads/stop",
        id,
        json!({"threadId":"线程","clear":clear}),
    )
}
fn disconnected(m: &mut Machine) -> Vec<Action> {
    apply(m, Event::Disconnected { conn: 1 })
}
fn op(m: &Machine) -> &StopOp {
    m.stops
        .operations
        .values()
        .find(|o| o.thread == "线程")
        .unwrap()
}
fn timer(actions: &[Action], predicate: impl Fn(&TimerKind) -> bool) -> Event {
    actions
        .iter()
        .find_map(|a| match a {
            Action::Schedule { token, kind, .. } | Action::ScheduleAt { token, kind, .. }
                if predicate(kind) =>
            {
                Some(Event::Timer {
                    token: token.clone(),
                    kind: kind.clone(),
                })
            }
            _ => None,
        })
        .expect("缺少计时器")
}
fn outcome(actions: &[Action], id: u64) -> Value {
    actions
        .iter()
        .find_map(|a| match a {
            Action::Send { message, .. } if message["id"] == id => message.get("result").cloned(),
            _ => None,
        })
        .expect("缺少 stop 回包")
}
fn verification(m: &mut Machine, initial: &[Action]) -> Vec<Action> {
    let deadline = timer(initial, |k| {
        matches!(k, TimerKind::Stop(_, StopTimer::Deadline))
    });
    let a = apply(m, deadline);
    let verify = timer(&a, |k| matches!(k, TimerKind::Stop(_, StopTimer::Verify)));
    apply(m, verify)
}

#[test]
fn 迁移表停止入口逐行覆盖() {
    for row in [
        "11",
        "19",
        "20",
        "21",
        "22",
        "24",
        "28",
        "28b-settling",
        "28b-joined",
        "28c",
    ] {
        let mut m = running();
        match row {
            "11" => {
                let (mut state, c) = started(false, "turn/start");
                turn(&mut state, "turn/started", "本轮");
                let a = reply(&mut state, &c, Err(err()));
                assert_eq!(op(&state).reason, StopReason::Stop);
                assert!(!requests(&a, "thread/goal/get").is_empty());
            }
            "19" => {
                let a = disconnected(&mut m);
                assert_eq!(op(&m).reason, StopReason::ClientDisconnected);
                assert!(!requests(&a, "thread/goal/get").is_empty());
            }
            "20" => {
                let (mut m, _) = started(false, "turn/start");
                disconnected(&mut m);
                assert!(m.claims["线程"].request_pending);
            }
            "21" => {
                let a = stop(&mut m, 2, false);
                reply(
                    &mut m,
                    &req(&a, "thread/goal/get"),
                    Ok(json!({"goal":null})),
                );
                let a = turn(&mut m, "turn/completed", "本轮");
                assert_eq!(outcome(&a, 2)["stopOutcome"], "confirmed");
            }
            "22" => {
                let a = stop(&mut m, 2, false);
                let a = apply(
                    &mut m,
                    timer(&a, |k| matches!(k, TimerKind::Stop(_, StopTimer::Deadline))),
                );
                assert_eq!(op(&m).outcome, Outcome::Unconfirmed);
                assert!(m.claims.contains_key("线程"));
                assert!(a.iter().any(|a| matches!(
                    a,
                    Action::Schedule {
                        delay_ms: VERIFY_MS,
                        ..
                    }
                )));
            }
            "24" => {
                let a = stop(&mut m, 2, false);
                let a = reply(
                    &mut m,
                    &req(&a, "thread/goal/get"),
                    Ok(json!({"goal":null})),
                );
                let old = req(&a, "turn/interrupt");
                let a = turn(&mut m, "turn/started", "新轮");
                assert_eq!(req(&a, "turn/interrupt").params.unwrap()["turnId"], "新轮");
                let a = reply(
                    &mut m,
                    &old,
                    Err(proto::error_object(
                        -32600,
                        "expected active turn id 本轮 but found 其他轮",
                        None,
                    )),
                );
                assert!(requests(&a, "turn/interrupt").is_empty());
            }
            "28" => {
                let a = stop(&mut m, 2, false);
                assert_eq!(op(&m).reason, StopReason::Stop);
                assert!(!requests(&a, "thread/goal/get").is_empty());
            }
            "28b-settling" => {
                let (mut m, _) = started(true, "turn/start");
                let a = stop(&mut m, 2, false);
                assert!(m.stops.operations.is_empty());
                assert!(a.iter().any(|a|matches!(a,Action::Send{message,..} if message["error"]["data"]["reason"]=="settling")));
            }
            "28b-joined" => {
                if let State::Running { joined, .. } = &mut m.claims.get_mut("线程").unwrap().state
                {
                    *joined = true;
                }
                let a = stop(&mut m, 2, false);
                assert!(m.stops.operations.is_empty());
                assert!(a.iter().any(|a|matches!(a,Action::Send{message,..} if message["error"]["data"]["reason"]=="joined")));
            }
            "28c" => {
                let mut m = Machine::default();
                apply(&mut m, Event::Connected { conn: 1 });
                let a = stop(&mut m, 2, false);
                let a = reply(
                    &mut m,
                    &req(&a, "thread/goal/get"),
                    Ok(json!({"goal":null})),
                );
                assert_eq!(outcome(&a, 2)["alreadyStopped"], true);
                assert_eq!(outcome(&a, 2)["runId"], Value::Null);
            }
            _ => unreachable!(),
        }
    }
}
#[test]
fn 唯一判定四项分别缺一均不放行() {
    let mut m = running();
    stop(&mut m, 2, false);
    let id = m.stop_key("线程").unwrap();
    let mut base = op(&m).clone();
    base.background.finish();
    base.turn_absent = true;
    base.brake_version = Some(m.stops.version("线程"));
    assert!(m.confirmed(&base));
    for missing in ["归属", "轮次", "制动", "启动请求", "goal变更"] {
        let mut m = m.clone();
        let mut o = base.clone();
        match missing {
            "归属" => {
                let c = m.claims.get_mut("线程").unwrap();
                c.state = State::Starting {
                    owner: Owner::Client(1),
                    req: crate::claim::ReqKind::TurnStart,
                    turn_id: Some("本轮".into()),
                    cont: None,
                    settling: true,
                    undecided: None,
                    submitted_at: 0,
                    held: vec![],
                };
            }
            "轮次" => o.turn_absent = false,
            "制动" => o.brake_version = None,
            "启动请求" => m.claims.get_mut("线程").unwrap().request_pending = true,
            "goal变更" => {
                m.stops.changes.insert(
                    999,
                    GoalChange {
                        thread: "线程".into(),
                        unknown: true,
                        wire_tag: None,
                        late_brake: false,
                        control_epoch: None,
                        grant_objective: None,
                    },
                );
            }
            _ => unreachable!(),
        }
        m.stops.operations.insert(id.clone(), o.clone());
        assert!(!m.confirmed(&o), "{missing}");
    }
}
#[test]
fn 中断成功仍占用且重复停止真正重发() {
    let mut m = running();
    let a = stop(&mut m, 2, false);
    let a = reply(
        &mut m,
        &req(&a, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    let c = req(&a, "turn/interrupt");
    reply(&mut m, &c, Ok(json!({})));
    assert!(m.claims.contains_key("线程"));
    let a = stop(&mut m, 3, false);
    assert_eq!(requests(&a, "turn/interrupt").len(), 1);
    assert!(op(&m).interrupt_sent);
}
#[test]
fn 已结束轮次不再中断且中断不回也能确认() {
    let mut m = running();
    let a = stop(&mut m, 2, false);
    let read = req(&a, "thread/goal/get");
    let a = reply(&mut m, &read, Ok(goal("active")));
    let pause = req(&a, "thread/goal/set");
    turn(&mut m, "turn/completed", "本轮");
    assert!(m.claims.contains_key("线程"));
    let a = stop(&mut m, 3, false);
    assert!(requests(&a, "turn/interrupt").is_empty());
    let a = reply(&mut m, &pause, Ok(goal("paused")));
    assert!(m.stops.operations.is_empty());
    assert!(m.claims.is_empty());
    assert!(requests(&a, "turn/interrupt").is_empty());
    assert_eq!(outcome(&a, 2)["stopOutcome"], "confirmed");
    let mut m = running();
    let a = stop(&mut m, 2, false);
    let a = reply(
        &mut m,
        &req(&a, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    let interrupt = req(&a, "turn/interrupt");
    turn(&mut m, "turn/completed", "本轮");
    let before = m.clone();
    reply(&mut m, &interrupt, Ok(json!({})));
    assert_eq!(m, before);
}
#[test]
fn 暂停失败保留占用并中断目标续跑新轮次() {
    let mut m = running();
    let a = stop(&mut m, 2, false);
    let a = reply(&mut m, &req(&a, "thread/goal/get"), Ok(goal("active")));
    let a = reply(&mut m, &req(&a, "thread/goal/set"), Err(err()));
    assert_eq!(requests(&a, "turn/interrupt").len(), 1);
    let completed = turn(&mut m, "turn/completed", "本轮");
    assert!(m.claims.contains_key("线程"));
    let a = turn(&mut m, "turn/started", "续跑");
    // completed 已触发新的 goal 读取，先等制动流程，随后中断新轮次。
    assert!(requests(&a, "turn/interrupt").is_empty());
    assert_eq!(op(&m).target.as_deref(), Some("续跑"));
    let a = reply(
        &mut m,
        &req(&completed, "thread/goal/get"),
        Ok(goal("active")),
    );
    let a = reply(&mut m, &req(&a, "thread/goal/set"), Err(err()));
    assert_eq!(req(&a, "turn/interrupt").params.unwrap()["turnId"], "续跑");
}
#[test]
fn 暂停回包前换代只中断最新轮次() {
    let mut m = running();
    let a = stop(&mut m, 2, false);
    let a = reply(&mut m, &req(&a, "thread/goal/get"), Ok(goal("active")));
    let pause = req(&a, "thread/goal/set");
    turn(&mut m, "turn/started", "新轮");
    let a = reply(&mut m, &pause, Ok(goal("paused")));
    assert_eq!(req(&a, "turn/interrupt").params.unwrap()["turnId"], "新轮");
}
#[test]
fn 共享归属未定先不动目标且自有定案后处理() {
    let (mut m, c) = started(true, "turn/start");
    let a = disconnected(&mut m);
    assert!(requests(&a, "thread/goal/get").is_empty());
    turn(&mut m, "turn/started", "本轮");
    let a = reply(&mut m, &c, Ok(json!({"turn":{"id":"本轮"}})));
    assert!(!requests(&a, "thread/goal/get").is_empty());
}
#[test]
fn 共享加入定案与加入认领断开只释放() {
    let (mut m, c) = started(true, "turn/start");
    disconnected(&mut m);
    reply(&mut m, &c, Ok(json!({"turn":{"id":"本轮"}})));
    // 吸收判定经 H2 状态读取层：线程活动，第一个活动轮次是其他轮 → 加入。
    let (a, _) = crate::test_support::status_read::drive_status(
        &mut m,
        vec![
            crate::test_support::status_read::StatusStep::Reply(
                crate::test_support::status_read::expect(
                    crate::test_support::status_read::StatusPhase::Metadata,
                ),
                Ok(crate::test_support::status_read::meta("线程", "active")),
            ),
            crate::test_support::status_read::StatusStep::Reply(
                crate::test_support::status_read::expect(
                    crate::test_support::status_read::StatusPhase::Turns,
                ),
                Ok(crate::test_support::status_read::turns_page(
                    &[("其他轮", "inProgress")],
                    None,
                )),
            ),
        ],
    );
    assert!(m.claims.is_empty());
    assert!(m.stops.operations.is_empty());
    assert!(requests(&a, "thread/goal/get").is_empty());
    assert!(requests(&a, "turn/interrupt").is_empty());
    let mut m = running();
    if let State::Running { joined, .. } = &mut m.claims.get_mut("线程").unwrap().state {
        *joined = true;
    }
    let a = disconnected(&mut m);
    assert!(m.claims.is_empty());
    assert!(m.stops.operations.is_empty());
    assert!(requests(&a, "turn/interrupt").is_empty());
}
#[test]
fn 客户端目标变更断开后才回来必须等最新事实() {
    for method in ["thread/goal/set", "thread/goal/clear"] {
        let mut m = running();
        let a = client(
            &mut m,
            method,
            8,
            json!({"threadId":"线程","status":"active"}),
        );
        let change = req(&a, method);
        let a = disconnected(&mut m);
        assert!(requests(&a, "thread/goal/get").is_empty());
        assert!(requests(&a, "turn/interrupt").is_empty());
        let a = reply(&mut m, &change, Ok(goal("active")));
        assert!(!requests(&a, "thread/goal/get").is_empty());
        let a = reply(&mut m, &req(&a, "thread/goal/get"), Ok(goal("active")));
        assert!(!requests(&a, "thread/goal/set").is_empty());
    }
}
#[test]
fn 屏障中启动取消仍须确认目标且失败保留() {
    for status in ["无目标", "active", "失败"] {
        let mut m = Machine::default();
        apply(&mut m, Event::Connected { conn: 1 });
        let a = client(&mut m, "turn/start", 1, json!({"threadId":"线程"}));
        let resume = req(&a, "thread/resume");
        let a = disconnected(&mut m);
        let get = req(&a, "thread/goal/get");
        assert!(m.claims["线程"].pending_start.is_none());
        let a = reply(&mut m, &resume, Ok(json!({})));
        assert!(requests(&a, "turn/start").is_empty());
        if status == "无目标" {
            reply(&mut m, &get, Ok(json!({"goal":null})));
            assert!(m.claims.is_empty());
        } else {
            let a = reply(&mut m, &get, Ok(goal("active")));
            assert!(m.claims.contains_key("线程"));
            reply(
                &mut m,
                &req(&a, "thread/goal/set"),
                if status == "失败" {
                    Err(err())
                } else {
                    Ok(goal("paused"))
                },
            );
            assert_eq!(m.claims.contains_key("线程"), status == "失败");
        }
    }
}
#[test]
fn 显式取消待发启动回复中文错误且不再发出() {
    let mut m = Machine::default();
    apply(&mut m, Event::Connected { conn: 1 });
    client(&mut m, "turn/start", 1, json!({"threadId":"线程"}));
    let a = stop(&mut m, 2, false);
    assert!(a.iter().any(|a|matches!(a,Action::Send{message,..} if message["id"]==1 && message["error"]["code"] == -32000)));
    assert!(m.claims["线程"].pending_start.is_none());
}
#[test]
fn 两轮之间断开立即制动成功释放失败保留() {
    for fail in [false, true] {
        let mut m = running();
        apply(
            &mut m,
            Event::Notification {
                method: "thread/goal/updated".into(),
                params: Some(json!({"threadId":"线程","goal":{"status":"active"}})),
            },
        );
        turn(&mut m, "turn/completed", "本轮");
        assert!(matches!(
            m.claims["线程"].state,
            State::AwaitingContinuation { .. }
        ));
        let a = disconnected(&mut m);
        let a = reply(&mut m, &req(&a, "thread/goal/get"), Ok(goal("active")));
        let a = reply(
            &mut m,
            &req(&a, "thread/goal/set"),
            if fail { Err(err()) } else { Ok(goal("paused")) },
        );
        assert!(requests(&a, "turn/interrupt").is_empty());
        assert_eq!(m.claims.contains_key("线程"), fail);
    }
}

#[test]
fn 清除覆盖暂停和阻塞目标且失败报告未改变() {
    for status in ["active", "paused", "blocked"] {
        for fail in [false, true] {
            let mut m = Machine::default();
            apply(&mut m, Event::Connected { conn: 1 });
            m.records.entry("线程".into()).or_default().last_run = Some("最近运行".into());
            let a = stop(&mut m, 2, true);
            let a = reply(&mut m, &req(&a, "thread/goal/get"), Ok(goal(status)));
            let a = reply(
                &mut m,
                &req(&a, "thread/goal/clear"),
                if fail { Err(err()) } else { Ok(json!({})) },
            );
            if fail && status == "active" {
                assert!(m.stops_busy());
            } else {
                let result = outcome(&a, 2);
                assert_eq!(result["goal"], if fail { "unchanged" } else { "cleared" });
                assert_eq!(result["runId"], "最近运行");
                assert_eq!(result["alreadyStopped"], true);
            }
        }
    }
}
#[test]
fn 合并清除意图只升不降并在暂停后补清除() {
    let mut m = running();
    let a = stop(&mut m, 2, false);
    let a = reply(&mut m, &req(&a, "thread/goal/get"), Ok(goal("active")));
    let pause = req(&a, "thread/goal/set");
    stop(&mut m, 3, true);
    stop(&mut m, 4, false);
    assert!(op(&m).clear);
    let a = reply(&mut m, &pause, Ok(goal("paused")));
    let a = reply(&mut m, &req(&a, "thread/goal/get"), Ok(goal("paused")));
    assert!(!requests(&a, "thread/goal/clear").is_empty());
    assert!(op(&m).clear);
}
#[test]
fn 目标功能不可用等同没有目标其他读取失败不放行() {
    for message in [
        "GOALS FEATURE IS DISABLED",
        "Method not found",
        "unknown method",
        "sqlite state db unavailable",
        "暂时不可读",
    ] {
        let mut m = Machine::default();
        apply(&mut m, Event::Connected { conn: 1 });
        let a = stop(&mut m, 2, false);
        let a = reply(
            &mut m,
            &req(&a, "thread/goal/get"),
            Err(proto::error_object(-32601, message, None)),
        );
        if message == "暂时不可读" {
            assert!(m.stops_busy());
        } else {
            assert_eq!(outcome(&a, 2)["goal"], "none");
            assert!(!m.stops_busy());
        }
    }
}
#[test]
fn 清除读取失败仍尝试清除且成功后确认() {
    let mut m = Machine::default();
    apply(&mut m, Event::Connected { conn: 1 });
    let a = stop(&mut m, 2, true);
    let a = reply(&mut m, &req(&a, "thread/goal/get"), Err(err()));
    let a = reply(&mut m, &req(&a, "thread/goal/clear"), Ok(json!({})));
    assert_eq!(outcome(&a, 2)["goal"], "cleared");
}
#[test]
fn 目标事实版本使旧暂停读取和旧暂停回包失效() {
    for write in [false, true] {
        let mut m = running();
        let a = stop(&mut m, 2, false);
        let get = req(&a, "thread/goal/get");
        let c = if write {
            let a = reply(&mut m, &get, Ok(goal("active")));
            req(&a, "thread/goal/set")
        } else {
            get
        };
        apply(
            &mut m,
            Event::Notification {
                method: "thread/goal/updated".into(),
                params: Some(json!({"threadId":"线程","goal":{"status":"active"}})),
            },
        );
        let a = reply(&mut m, &c, Ok(goal("paused")));
        assert!(op(&m).brake_version.is_none());
        assert!(!requests(&a, "thread/goal/get").is_empty());
    }
}
#[test]
fn 目标设置超时结果未知直到迟到生效后重新制动() {
    let mut m = running();
    let a = client(
        &mut m,
        "thread/goal/set",
        8,
        json!({"threadId":"线程","status":"active"}),
    );
    let change = req(&a, "thread/goal/set");
    disconnected(&mut m);
    let a = apply(
        &mut m,
        Event::TimedOut {
            tag: 0,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: change.clone(),
            error: err(),
        },
    );
    assert!(m.stops.changes.values().any(|c| c.unknown));
    let get = req(&a, "thread/goal/get");
    reply(&mut m, &get, Ok(json!({"goal":null})));
    turn(&mut m, "turn/completed", "本轮");
    assert!(m.claims.contains_key("线程"));
    let a = apply(
        &mut m,
        Event::LateReply {
            context: change,
            result: Ok(goal("active")),
        },
    );
    assert!(m.stops.changes.is_empty());
    let a = reply(&mut m, &req(&a, "thread/goal/get"), Ok(goal("active")));
    let a = reply(&mut m, &req(&a, "thread/goal/set"), Ok(goal("paused")));
    assert!(m.stops.operations.is_empty());
    assert!(requests(&a, "turn/interrupt").is_empty());
}
#[test]
fn 四十秒到期请求各自回未确认且认领保留() {
    let mut m = running();
    let a = stop(&mut m, 2, false);
    let original = op(&m).generation;
    let deadline = timer(&a, |k| matches!(k, TimerKind::Stop(_, StopTimer::Deadline)));
    let waiter1 = timer(&a, |k| {
        matches!(k, TimerKind::Stop(_, StopTimer::Waiter(_)))
    });
    reply(
        &mut m,
        &req(&a, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    let second = stop(&mut m, 3, false);
    assert_eq!(op(&m).generation, original);
    assert!(!second.iter().any(|a| matches!(
        a,
        Action::Schedule {
            kind: TimerKind::Stop(_, StopTimer::Deadline),
            ..
        }
    )));
    apply(&mut m, deadline);
    let a = apply(&mut m, waiter1);
    assert_eq!(outcome(&a, 2)["stopOutcome"], "unconfirmed");
    assert_eq!(op(&m).waiters.len(), 1);
    assert!(m.claims.contains_key("线程"));
    let a = apply(
        &mut m,
        timer(&second, |k| {
            matches!(k, TimerKind::Stop(_, StopTimer::Waiter(_)))
        }),
    );
    assert_eq!(outcome(&a, 3)["stopOutcome"], "unconfirmed");
}
#[test]
fn 十五秒核实未知不放行完整成功解除() {
    // (元数据回包, 活动线程的轮次页, 是否确认)。只有元数据非 active（Idle）才证明没有活动轮次；
    // 畸形回包、Active([])（状态 active 但轮次页暂时为空）都不放行。
    let empty = turns_page(&[], None);
    let cases = [
        (json!({}), None, false),
        (json!({"thread":{"id":"线程"}}), None, false),
        (
            json!({"thread":{"id":"线程","status":{"type":"未知"}}}),
            None,
            false,
        ),
        (
            meta("线程", "active"),
            Some(json!({"data":[{"status":"inProgress"}],"nextCursor":null})),
            false,
        ),
        (meta("线程", "active"), Some(empty.clone()), false),
        (
            meta("线程", "active"),
            Some(turns_page(&[("本轮", "completed")], None)),
            false,
        ),
        (meta("线程", "idle"), None, true),
        (meta("线程", "notLoaded"), None, true),
        (meta("线程", "systemError"), None, true),
    ];
    for (metadata, page, confirmed) in cases {
        let mut m = running();
        let initial = stop(&mut m, 2, false);
        reply(
            &mut m,
            &req(&initial, "thread/goal/get"),
            Ok(json!({"goal":null})),
        );
        let verify = verification(&mut m, &initial);
        let read = status_request(&req(&verify, "thread/read"));
        let get = req(&verify, "thread/goal/get");
        let a = reply_status(&mut m, &read, Ok(metadata.clone()));
        if let Some(page) = page {
            let turns = status_requests(&a).pop().expect("活动线程继续分页");
            assert_eq!(turns.phase, StatusPhase::Turns);
            reply_status(&mut m, &turns, Ok(page));
        }
        reply(&mut m, &get, Ok(json!({"goal":null})));
        assert_eq!(m.claims.is_empty(), confirmed, "{metadata}");
        if !confirmed {
            assert!(
                !op(&m).turn_absent,
                "未证明空闲时缺轮证据不成立：{metadata}"
            );
        }
    }
}
#[test]
fn 核实失败后登记下一轮且活动目标再次制动不周期重发中断() {
    let mut m = running();
    let initial = stop(&mut m, 2, false);
    let a = reply(
        &mut m,
        &req(&initial, "thread/goal/get"),
        Ok(goal("active")),
    );
    reply(&mut m, &req(&a, "thread/goal/set"), Err(err()));
    let verify = verification(&mut m, &initial);
    reply(&mut m, &req(&verify, "thread/read"), Err(err()));
    let a = reply(&mut m, &req(&verify, "thread/goal/get"), Ok(goal("active")));
    assert!(!requests(&a, "thread/goal/set").is_empty());
    assert!(requests(&a, "turn/interrupt").is_empty());
    let pause = req(&a, "thread/goal/set");
    let retry = timer(&a, |k| matches!(k, TimerKind::Stop(_, StopTimer::Verify)));
    reply(&mut m, &pause, Err(err()));
    let a = apply(&mut m, retry);
    assert!(!requests(&a, "thread/read").is_empty());
}
#[test]
fn 核实同轮活动快照不重复中断() {
    let mut m = running();
    let initial = stop(&mut m, 2, false);
    reply(
        &mut m,
        &req(&initial, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    let verify = verification(&mut m, &initial);
    snapshot(
        &mut m,
        &req(&verify, "thread/read"),
        &[("本轮", "inProgress")],
    );
    let a = reply(
        &mut m,
        &req(&verify, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    assert!(requests(&a, "turn/interrupt").is_empty());
    assert!(m.claims.contains_key("线程"));
}
#[test]
fn 真完成证据不被陈旧活动快照撤销() {
    let mut m = running();
    let initial = stop(&mut m, 2, false);
    let get = req(&initial, "thread/goal/get");
    reply(&mut m, &get, Err(err()));
    let verify = verification(&mut m, &initial);
    let completed = turn(&mut m, "turn/completed", "本轮");
    snapshot(
        &mut m,
        &req(&verify, "thread/read"),
        &[("本轮", "inProgress")],
    );
    assert!(op(&m).ended.contains("本轮"));
    reply(
        &mut m,
        &req(&completed, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    assert!(m.claims.is_empty());
}
#[test]
fn 核实期间启动目标变活与清除升级分别使旧轮次失效() {
    for event in ["started", "active", "clear"] {
        let mut m = running();
        let initial = stop(&mut m, 2, false);
        reply(
            &mut m,
            &req(&initial, "thread/goal/get"),
            Ok(json!({"goal":null})),
        );
        let verify = verification(&mut m, &initial);
        let trigger = match event {
            "started" => turn(&mut m, "turn/started", "新轮"),
            "active" => apply(
                &mut m,
                Event::Notification {
                    method: "thread/goal/updated".into(),
                    params: Some(json!({"threadId":"线程","goal":{"status":"active"}})),
                },
            ),
            _ => stop(&mut m, 3, true),
        };
        snapshot(&mut m, &req(&verify, "thread/read"), &[]);
        reply(
            &mut m,
            &req(&verify, "thread/goal/get"),
            Ok(json!({"goal":null})),
        );
        assert!(m.claims.contains_key("线程"), "{event}");
        assert!(m.stops_busy());
        let successor = timer(&trigger, |k| {
            matches!(k, TimerKind::Stop(_, StopTimer::Verify))
        });
        for get in requests(&trigger, "thread/goal/get") {
            reply(&mut m, &get, Ok(json!({"goal":null})));
        }
        let verify = apply(&mut m, successor);
        snapshot(&mut m, &req(&verify, "thread/read"), &[]);
        reply(
            &mut m,
            &req(&verify, "thread/goal/get"),
            Ok(json!({"goal":null})),
        );
        assert!(m.claims.is_empty(), "{event} 后继核实未确认");
        assert!(!m.stops_busy());
    }
}
#[test]
fn 过期代号计时器与旧轮次回包无效() {
    let mut m = running();
    let initial = stop(&mut m, 2, false);
    reply(
        &mut m,
        &req(&initial, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    let verify = verification(&mut m, &initial);
    let before = m.clone();
    // 旧代号的回包由读取层丢弃；旧轮次的完成结果由处理函数按 round 丢弃。
    let read = status_request(&req(&verify, "thread/read"));
    let mut stale = read.clone();
    stale.context.operation = Operation::StatusRead {
        read_id: read.read_id,
        generation: read.generation - 1,
    };
    reply_status(&mut m, &stale, Ok(meta("线程", "idle")));
    assert_eq!(m.status_reads, before.status_reads);
    assert_eq!(m.stops, before.stops);
    let mut done = m.clone();
    let StatusCaller::StopVerify { mut tag } =
        done.status_reads.reads[&read.read_id].caller.clone()
    else {
        panic!("核实读取的调用方")
    };
    tag.round -= 1;
    done.stop_verify_done(
        StatusDone {
            read_id: read.read_id,
            thread: "线程".into(),
            token: None,
            caller: StatusCaller::StopVerify { tag },
            result: StatusResult::Idle,
        },
        &mut Vec::new(),
    );
    assert_eq!(done, m);
    let mut stale = timer(&initial, |k| {
        matches!(k, TimerKind::Stop(_, StopTimer::Deadline))
    });
    if let Event::Timer {
        kind: TimerKind::Stop(tag, ..),
        ..
    } = &mut stale
    {
        tag.generation += 1;
    }
    apply(&mut m, stale);
    assert_eq!(m, before);
}
#[test]
fn 审查断开随子线程迁移且不碰父线程() {
    let (mut m, c) = started_legacy_review(false);
    let a = disconnected(&mut m);
    assert!(requests(&a, "thread/goal/get").is_empty());
    let a = reply(
        &mut m,
        &c,
        Ok(json!({"turn":{"id":"审查轮"},"reviewThreadId":"子线程"})),
    );
    let get = req(&a, "thread/goal/get");
    assert_eq!(get.params.as_ref().unwrap()["threadId"], "子线程");
    let a = reply(&mut m, &get, Ok(json!({"goal":null})));
    assert_eq!(
        req(&a, "turn/interrupt").params.unwrap(),
        json!({"threadId":"子线程","turnId":"审查轮"})
    );
    assert!(!m.claims.contains_key("线程"));
}
#[test]
fn 托管所有者不因原连接或观察者断开停止但显式停止有效() {
    let mut m = running();
    m.claims.get_mut("线程").unwrap().state = State::Running {
        owner: Owner::Broker {
            policy: crate::policy::Policy::Never,
            approvals_reviewer: None,
            owned_at: 0,
            last_watcher_at: 0,
        },
        turn_id: "本轮".into(),
        joined: false,
    };
    m.records.get_mut("线程").unwrap().watchers.push(2);
    disconnected(&mut m);
    apply(&mut m, Event::Disconnected { conn: 2 });
    assert!(m.stops.operations.is_empty());
    assert!(m.records["线程"].watchers.is_empty());
    apply(&mut m, Event::Connected { conn: 1 });
    let a = stop(&mut m, 2, false);
    let a = reply(
        &mut m,
        &req(&a, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    assert!(!requests(&a, "turn/interrupt").is_empty());
}
#[test]
fn 关闭等待请求回未确认并作废全部停止回调() {
    let mut m = running();
    let initial = stop(&mut m, 2, false);
    let get = req(&initial, "thread/goal/get");
    let verify = verification(&mut m, &initial);
    let a = apply(&mut m, Event::Shutdown);
    assert_eq!(outcome(&a, 2)["stopOutcome"], "unconfirmed");
    let before = m.clone();
    reply(&mut m, &get, Ok(goal("active")));
    for c in requests(&verify, "thread/read")
        .into_iter()
        .chain(requests(&verify, "thread/goal/get"))
    {
        reply(&mut m, &c, Ok(json!({})));
    }
    apply(
        &mut m,
        timer(&initial, |k| {
            matches!(k, TimerKind::Stop(_, StopTimer::Deadline))
        }),
    );
    assert_eq!(m, before);
}
#[test]
fn 已确认停止不重开旧完成不驱动新认领() {
    let mut m = running();
    let a = stop(&mut m, 2, false);
    reply(
        &mut m,
        &req(&a, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    turn(&mut m, "turn/completed", "本轮");
    assert!(m.stops.operations.is_empty());
    client(&mut m, "turn/start", 9, json!({"threadId":"线程"}));
    let claim = m.claims["线程"].clone();
    turn(&mut m, "turn/completed", "本轮");
    assert_eq!(m.claims["线程"], claim);
    assert!(m.stops.operations.is_empty());
}
#[test]
fn 启动错误隔离本认领审批关闭工具静默原连接仍在线() {
    let (mut m, c) = started(false, "turn/start");
    turn(&mut m, "turn/started", "本轮");
    let a = reply(&mut m, &c, Err(err()));
    assert!(m.live_conns.contains(&1));
    assert!(m.claims["线程"].owner_gone);
    assert!(a.iter().any(|a|matches!(a,Action::Send{message,..} if message["id"]==1 && message.get("error").is_some())));
    for method in [
        "item/commandExecution/requestApproval",
        "item/fileChange/requestApproval",
        "item/tool/call",
        "item/tool/requestUserInput",
    ] {
        let a = apply(
            &mut m,
            Event::ServerRequest {
                id: json!(88),
                forward_id: "broker-88".into(),
                method: method.into(),
                params: Some(json!({"threadId":"线程"})),
            },
        );
        assert!(!a.iter().any(|a| matches!(a, Action::Send { .. })));
        assert!(a
            .iter()
            .any(|a| matches!(a, Action::Dismiss { .. } | Action::ServerError { .. })));
    }
}
#[test]
fn 停止参数形状错误回负三二六零二() {
    let mut m = Machine::default();
    for params in [
        json!({}),
        json!({"threadId":1}),
        json!({"threadId":"线程","clear":"true"}),
    ] {
        let a = client(&mut m, "broker/threads/stop", 2, params);
        assert!(a
            .iter()
            .any(|a| matches!(a,Action::Send{message,..} if message["error"]["code"] == -32602)));
    }
    assert!(m.stops.operations.is_empty());
}
#[test]
fn 等待停止的连接断开不再回包() {
    let mut m = running();
    let a = stop(&mut m, 2, false);
    disconnected(&mut m);
    assert!(op(&m).waiters.is_empty());
    let a = reply(
        &mut m,
        &req(&a, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    assert!(!a
        .iter()
        .any(|a| matches!(a,Action::Send{message,..} if message["id"]==2)));
}

#[test]
fn 启动迟到回包在断开后仍先处理目标再中断() {
    let (mut m, c) = started(false, "turn/start");
    let a = disconnected(&mut m);
    let get = req(&a, "thread/goal/get");
    let a = reply(&mut m, &c, Ok(json!({"turn":{"id":"本轮"}})));
    assert!(requests(&a, "turn/interrupt").is_empty());
    let a = reply(&mut m, &get, Ok(json!({"goal":null})));
    assert_eq!(req(&a, "turn/interrupt").params.unwrap()["turnId"], "本轮");
}
#[test]
fn 启动确定失败没有轮次时按目标证据确认() {
    let (mut m, c) = started(false, "turn/start");
    let a = disconnected(&mut m);
    let get = req(&a, "thread/goal/get");
    reply(&mut m, &c, Err(err()));
    assert!(m.claims.contains_key("线程"));
    reply(&mut m, &get, Ok(json!({"goal":null})));
    assert!(m.claims.is_empty());
}
#[test]
fn 启动超时对账找到活动轮次按失败停止并正常确认() {
    {
        let method = "turn/start";
        let (mut m, c) = started(false, method);
        let a = apply(
            &mut m,
            Event::TimedOut {
                tag: 0,
                deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
                context: c,
                error: err(),
            },
        );
        let read = req(&a, "thread/read");
        assert!(m.claims["线程"].request_pending);
        let a = snapshot(&mut m, &read, &[("本轮", "inProgress")]);
        let a = reply(
            &mut m,
            &req(&a, "thread/goal/get"),
            Ok(json!({"goal":null})),
        );
        assert_eq!(req(&a, "turn/interrupt").params.unwrap()["turnId"], "本轮");
        assert_eq!(op(&m).reason, StopReason::Stop);
        turn(&mut m, "turn/completed", "本轮");
        assert!(m.claims.is_empty());
        assert!(m.stops.operations.is_empty());
    }
}
#[test]
fn 启动已宣布轮次的超时走正常失败停止() {
    let (mut m, c) = started(false, "turn/start");
    turn(&mut m, "turn/started", "本轮");
    let a = apply(
        &mut m,
        Event::TimedOut {
            tag: 0,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: c,
            error: err(),
        },
    );
    assert!(!requests(&a, "thread/goal/get").is_empty());
    assert!(requests(&a, "thread/read").is_empty());
    assert!(m.claims["线程"].owner_gone);
}
#[test]
fn 超时对账没有活动释放登记迟到按编号中断不碰新认领() {
    let (mut m, c) = started(false, "turn/start");
    let a = apply(
        &mut m,
        Event::TimedOut {
            tag: 0,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: c.clone(),
            error: err(),
        },
    );
    snapshot(&mut m, &req(&a, "thread/read"), &[]);
    assert!(m.claims.is_empty());
    assert_eq!(m.stops.late.len(), 1);
    client(&mut m, "turn/start", 9, json!({"threadId":"线程"}));
    let new = m.claims["线程"].clone();
    let a = apply(
        &mut m,
        Event::LateReply {
            context: c,
            result: Ok(json!({"turn":{"id":"旧轮"}})),
        },
    );
    let a = reply(
        &mut m,
        &req(&a, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    let interrupt = req(&a, "turn/interrupt");
    assert_eq!(interrupt.params.unwrap()["turnId"], "旧轮");
    assert_eq!(m.claims["线程"], new);
    reply(&mut m, &req(&a, "turn/interrupt"), Ok(json!({})));
    assert!(m.stops.late.is_empty());
    assert_eq!(m.claims["线程"], new);
}
#[test]
fn 迟到登记期间旧轮次启动只中断旧轮次且完成清除登记() {
    let (mut m, c) = started(false, "turn/start");
    let a = apply(
        &mut m,
        Event::TimedOut {
            tag: 0,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: c,
            error: err(),
        },
    );
    snapshot(&mut m, &req(&a, "thread/read"), &[]);
    m.stops.late[0].turn = Some("旧轮".into());
    let queued = client(&mut m, "turn/start", 9, json!({"threadId":"线程"}));
    let new = m.claims["线程"].clone();
    let a = turn(&mut m, "turn/started", "旧轮");
    assert!(requests(&a, "turn/interrupt").is_empty());
    let a = reply(
        &mut m,
        &req(&queued, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    assert_eq!(req(&a, "turn/interrupt").params.unwrap()["turnId"], "旧轮");
    assert_eq!(m.claims["线程"], new);
    turn(&mut m, "turn/completed", "旧轮");
    assert!(m.stops.late.is_empty());
    assert_eq!(m.claims["线程"], new);
}
#[test]
fn 超时对账未知保留每十五秒重读且旧轮次无效() {
    let (mut m, c) = started(false, "turn/start");
    let a = apply(
        &mut m,
        Event::TimedOut {
            tag: 0,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: c,
            error: err(),
        },
    );
    let read = req(&a, "thread/read");
    let a = reply(&mut m, &read, Err(err()));
    assert!(m.claims.contains_key("线程"));
    assert!(a.iter().any(|a| matches!(
        a,
        Action::Schedule {
            delay_ms: VERIFY_MS,
            ..
        }
    )));
    let event = timer(&a, |k| matches!(k, TimerKind::Reconcile { .. }));
    let a = apply(&mut m, event);
    assert_eq!(requests(&a, "thread/read").len(), 1);
    let before = m.clone();
    reply(&mut m, &read, Ok(json!({"thread":{"turns":[]}})));
    assert_eq!(m, before);
    let a = client(&mut m, "turn/start", 9, json!({"threadId":"线程"}));
    assert!(a
        .iter()
        .any(|a| matches!(a,Action::Send{message,..} if message["error"]["code"] == -32001)));
}
#[test]
fn 超时尚在对账时迟到回包落定并正常停止() {
    let (mut m, c) = started(false, "turn/start");
    apply(
        &mut m,
        Event::TimedOut {
            tag: 0,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: c.clone(),
            error: err(),
        },
    );
    let a = apply(
        &mut m,
        Event::LateReply {
            context: c,
            result: Ok(json!({"turn":{"id":"本轮"}})),
        },
    );
    assert!(m.stops.late.is_empty());
    let a = reply(
        &mut m,
        &req(&a, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    assert_eq!(req(&a, "turn/interrupt").params.unwrap()["turnId"], "本轮");
}
#[test]
fn 迟到登记十分钟过期六十四条上限与关闭清理() {
    let mut m = Machine::default();
    apply(&mut m, Event::Connected { conn: 1 });
    let mut contexts = Vec::new();
    for i in 1..=65 {
        let thread = format!("线程{i}");
        m.records.entry(thread.clone()).or_default().subscribed = true;
        let a = client(&mut m, "turn/start", i, json!({"threadId":thread}));
        let c = req(&a, "turn/start");
        let a = apply(
            &mut m,
            Event::TimedOut {
                tag: i,
                deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
                context: c.clone(),
                error: err(),
            },
        );
        assert!(!a.iter().any(|a| matches!(
            a,
            Action::ScheduleAt {
                kind: TimerKind::LateExpiry { .. },
                ..
            }
        )));
        snapshot(&mut m, &req(&a, "thread/read"), &[]);
        contexts.push(c);
    }
    assert_eq!(m.stops.late.len(), MAX_LATE);
    assert_eq!(m.stops.late.front().unwrap().wire_tag, 2);
    apply(
        &mut m,
        Event::LateDiscarded {
            context: contexts[0].clone(),
        },
    );
    assert_eq!(m.stops.late.len(), MAX_LATE);
    apply(
        &mut m,
        Event::LateDiscarded {
            context: contexts[1].clone(),
        },
    );
    assert_eq!(m.stops.late.len(), 63);
    apply(&mut m, Event::Shutdown);
    assert!(m.stops.late.is_empty());
}

#[test]
fn 关闭后超时迟到回包和对账计时器不产生动作() {
    let (mut m, c) = started(false, "turn/start");
    let a = apply(
        &mut m,
        Event::TimedOut {
            tag: 0,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: c.clone(),
            error: err(),
        },
    );
    let read = req(&a, "thread/read");
    let a = reply(&mut m, &read, Err(err()));
    let retry = timer(&a, |k| matches!(k, TimerKind::Reconcile { .. }));
    apply(&mut m, Event::Shutdown);
    let before = m.clone();
    let a = apply(
        &mut m,
        Event::LateReply {
            context: c,
            result: Ok(json!({"turn":{"id":"旧轮"}})),
        },
    );
    assert!(a.is_empty());
    apply(&mut m, retry);
    reply(&mut m, &read, Ok(json!({"thread":{"turns":[]}})));
    assert_eq!(m, before);
}

#[test]
fn 共享待定四十秒后核实仍不读写目标或中断() {
    let (mut m, _) = started(true, "turn/start");
    let initial = disconnected(&mut m);
    let a = verification(&mut m, &initial);
    assert!(requests(&a, "thread/goal/get").is_empty());
    assert!(requests(&a, "thread/goal/set").is_empty());
    assert!(requests(&a, "turn/interrupt").is_empty());
    assert!(m.claims["线程"].settling());
}
#[test]
fn 在途目标变更超过确认期限核实仍先等待() {
    let mut m = running();
    client(
        &mut m,
        "thread/goal/set",
        8,
        json!({"threadId":"线程","status":"active"}),
    );
    let initial = stop(&mut m, 2, false);
    let a = verification(&mut m, &initial);
    assert!(requests(&a, "thread/goal/get").is_empty());
    assert!(requests(&a, "turn/interrupt").is_empty());
    assert!(m.claims.contains_key("线程"));
}
#[test]
fn 共享启动超时活动快照不能直接当成自有轮次() {
    let (mut m, c) = started(true, "turn/start");
    let a = apply(
        &mut m,
        Event::TimedOut {
            tag: 0,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: c,
            error: err(),
        },
    );
    let a = snapshot(&mut m, &req(&a, "thread/read"), &[("其他轮", "inProgress")]);
    assert!(requests(&a, "thread/goal/get").is_empty());
    assert!(requests(&a, "turn/interrupt").is_empty());
    assert!(m.claims["线程"].settling());
    assert!(m.claims["线程"].request_pending);
    assert_eq!(m.stops.reconciliations.len(), 1);
}
#[test]
fn 内联审查完成早于断开回包仍保留制动与等待请求() {
    let (mut m, c) = started(false, "review/start");
    stop(&mut m, 2, false);
    turn(&mut m, "turn/started", "审查轮");
    turn(&mut m, "turn/completed", "审查轮");
    let a = reply(
        &mut m,
        &c,
        Ok(json!({"turn":{"id":"审查轮"},"reviewThreadId":"线程"})),
    );
    let get = req(&a, "thread/goal/get");
    assert!(m.claims.contains_key("线程"));
    let a = reply(&mut m, &get, Ok(json!({"goal":null})));
    assert_eq!(outcome(&a, 2)["stopOutcome"], "confirmed");
    assert!(m.claims.is_empty());
    assert!(requests(&a, "turn/interrupt").is_empty());
}
#[test]
fn 非本轮完成不能给当前目标制造结束证据() {
    let mut m = running();
    let a = stop(&mut m, 2, false);
    reply(
        &mut m,
        &req(&a, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    turn(&mut m, "turn/completed", "其他轮");
    assert!(m.claims.contains_key("线程"));
    assert!(!op(&m).ended.contains("本轮"));
}
#[test]
fn 已中断迟到轮次的后续通知不改变新认领() {
    let (mut m, c) = started(false, "turn/start");
    let a = apply(
        &mut m,
        Event::TimedOut {
            tag: 0,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: c.clone(),
            error: err(),
        },
    );
    snapshot(&mut m, &req(&a, "thread/read"), &[]);
    client(&mut m, "turn/start", 9, json!({"threadId":"线程"}));
    let new = m.claims["线程"].clone();
    let a = apply(
        &mut m,
        Event::LateReply {
            context: c,
            result: Ok(json!({"turn":{"id":"旧轮"}})),
        },
    );
    let a = reply(
        &mut m,
        &req(&a, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    reply(&mut m, &req(&a, "turn/interrupt"), Ok(json!({})));
    assert!(m.stops.late.is_empty());
    turn(&mut m, "turn/started", "旧轮");
    turn(&mut m, "turn/completed", "旧轮");
    assert_eq!(m.claims["线程"], new);
    assert!(m.records["线程"].ended_turns.contains(&"旧轮".into()));
}
#[test]
fn 客户端迟到暂停回包不能覆盖新活动事实() {
    let mut m = running();
    let a = client(
        &mut m,
        "thread/goal/set",
        8,
        json!({"threadId":"线程","status":"paused"}),
    );
    let pause = req(&a, "thread/goal/set");
    apply(
        &mut m,
        Event::Notification {
            method: "thread/goal/updated".into(),
            params: Some(json!({"threadId":"线程","goal":{"status":"active"}})),
        },
    );
    reply(&mut m, &pause, Ok(goal("paused")));
    assert!(m.records["线程"].goal_active);
}
#[test]
fn 中断一直不回但完成加制动照样确认() {
    let mut m = running();
    let a = stop(&mut m, 2, false);
    let a = reply(
        &mut m,
        &req(&a, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    assert_eq!(requests(&a, "turn/interrupt").len(), 1);
    let a = turn(&mut m, "turn/completed", "本轮");
    assert_eq!(outcome(&a, 2)["stopOutcome"], "confirmed");
    assert!(m.claims.is_empty());
}
#[test]
fn 停止核实观察私有新轮次自动中断一次() {
    let mut m = running();
    let initial = stop(&mut m, 2, false);
    reply(
        &mut m,
        &req(&initial, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    let verify = verification(&mut m, &initial);
    let mut read_actions = snapshot(
        &mut m,
        &req(&verify, "thread/read"),
        &[("新轮", "inProgress")],
    );
    let a = reply(
        &mut m,
        &req(&verify, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    read_actions.extend(a);
    assert_eq!(
        req(&read_actions, "turn/interrupt").params.unwrap()["turnId"],
        "新轮"
    );
    assert_eq!(m.claims["线程"].turn_id(), Some("新轮"));
}

#[test]
fn 核实空快照先到后清除升级不能复用旧轮次放行() {
    let mut m = running();
    let initial = stop(&mut m, 2, false);
    reply(
        &mut m,
        &req(&initial, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    let verify = verification(&mut m, &initial);
    snapshot(&mut m, &req(&verify, "thread/read"), &[]);
    assert!(op(&m).turn_absent);
    let a = stop(&mut m, 3, true);
    let a = reply(&mut m, &req(&a, "thread/goal/get"), Ok(goal("paused")));
    reply(&mut m, &req(&a, "thread/goal/clear"), Ok(json!({})));
    assert!(m.claims.contains_key("线程"));
    assert!(!op(&m).turn_absent);
    reply(
        &mut m,
        &req(&verify, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    assert!(m.claims.contains_key("线程"));
}
#[test]
fn 停止合法改投仅命中已观察自有轮次且未知换代不改投() {
    for found in ["新轮", "其他轮"] {
        let mut m = running();
        let a = stop(&mut m, 2, false);
        let a = reply(
            &mut m,
            &req(&a, "thread/goal/get"),
            Ok(json!({"goal":null})),
        );
        let old = req(&a, "turn/interrupt");
        turn(&mut m, "turn/started", "新轮");
        // 模拟新轮次尚未实际发出中断的合法改投窗口。
        let key = m.stop_key("线程").unwrap();
        m.stops
            .operations
            .get_mut(&key)
            .unwrap()
            .interrupted
            .remove("新轮");
        let a = reply(
            &mut m,
            &old,
            Err(proto::error_object(
                -32600,
                format!("expected active turn id 本轮 but found {found}"),
                None,
            )),
        );
        let interrupts = requests(&a, "turn/interrupt");
        // 后续驱动也会自动中断新观察轮次，但绝不使用未观察到的服务器编号。
        assert!(interrupts
            .iter()
            .all(|c| c.params.as_ref().unwrap()["turnId"] == "新轮"));
        assert_eq!(interrupts.len(), 1);
    }
}

#[test]
fn 中断成功错误和超时都不是停止证据() {
    for result in [
        Ok(json!({})),
        Err(proto::error_object(-32600, "INVALID_REQUEST", None)),
        Err(proto::error_object(-32000, "中断请求超时", None)),
    ] {
        let mut m = running();
        let a = stop(&mut m, 2, false);
        let a = reply(
            &mut m,
            &req(&a, "thread/goal/get"),
            Ok(json!({"goal":null})),
        );
        let a = reply(&mut m, &req(&a, "turn/interrupt"), result);
        assert!(m.claims.contains_key("线程"));
        assert!(a.iter().all(|a|!matches!(a,Action::Send{message,..} if message["result"]["stopOutcome"] == "confirmed")));
    }
}
#[test]
fn 完成证据保存在操作中不依赖十六项历史缓存() {
    let mut m = running();
    let a = stop(&mut m, 2, false);
    let a = reply(&mut m, &req(&a, "thread/goal/get"), Ok(goal("active")));
    let pause = req(&a, "thread/goal/set");
    turn(&mut m, "turn/completed", "本轮");
    for i in 0..20 {
        turn(&mut m, "turn/completed", &format!("旧轮{i}"));
    }
    assert!(!m.records["线程"].ended_turns.contains(&"本轮".into()));
    assert!(op(&m).ended.contains("本轮"));
    let a = reply(&mut m, &pause, Ok(goal("paused")));
    assert_eq!(outcome(&a, 2)["stopOutcome"], "confirmed");
    assert!(requests(&a, "turn/interrupt").is_empty());
}
#[test]
fn 关闭后暂停与清除成功迟到都不改变状态() {
    for clear in [false, true] {
        let mut m = running();
        let a = stop(&mut m, 2, clear);
        let a = reply(&mut m, &req(&a, "thread/goal/get"), Ok(goal("active")));
        let c = req(
            &a,
            if clear {
                "thread/goal/clear"
            } else {
                "thread/goal/set"
            },
        );
        apply(&mut m, Event::Shutdown);
        let before = m.clone();
        reply(&mut m, &c, Ok(goal("paused")));
        apply(
            &mut m,
            Event::LateReply {
                context: c,
                result: Ok(goal("paused")),
            },
        );
        assert_eq!(m, before);
    }
}

#[test]
fn 审查子线程冲突保留停止等待期限且不触碰别的认领() {
    let (mut m, c) = started_legacy_review(false);
    let initial = stop(&mut m, 2, false);
    let mut occupied = running().claims.remove("线程").unwrap();
    occupied.claim_id = 99;
    occupied.run_id = Some("其他运行".into());
    m.claims.insert("子线程".into(), occupied.clone());
    let a = reply(
        &mut m,
        &c,
        Ok(json!({"turn":{"id":"审查轮"},"reviewThreadId":"子线程"})),
    );
    assert_eq!(m.claims["子线程"], occupied);
    assert!(requests(&a, "thread/goal/get").is_empty());
    assert!(requests(&a, "turn/interrupt").is_empty());
    let a = apply(
        &mut m,
        timer(&initial, |k| {
            matches!(k, TimerKind::Stop(_, StopTimer::Waiter(_)))
        }),
    );
    assert_eq!(outcome(&a, 2)["stopOutcome"], "unconfirmed");
    assert!(m.stops.operations.is_empty());
    apply(
        &mut m,
        Event::Notification {
            method: "turn/completed".into(),
            params: Some(json!({"threadId":"子线程","turn":{"id":"本轮"}})),
        },
    );
    assert!(!m.claims.contains_key("子线程"));
    assert!(!m.stop_busy("子线程"));
    assert!(!m.stops_busy());
    let a = client(&mut m, "turn/start", 9, json!({"threadId":"子线程"}));
    assert!(requests(&a, "thread/resume").len() + requests(&a, "turn/start").len() > 0);
}

#[test]
fn 旧审查父认领回包不迁移新认领的停止操作() {
    let (mut m, c) = started_legacy_review(false);
    turn(&mut m, "turn/completed", "父轮");
    let a = client(&mut m, "turn/start", 9, json!({"threadId":"线程"}));
    reply(
        &mut m,
        &req(&a, "turn/start"),
        Ok(json!({"turn":{"id":"新轮"}})),
    );
    stop(&mut m, 10, false);
    let current = m.stops.operations["run-9"].clone();
    reply(
        &mut m,
        &c,
        Ok(json!({"turn":{"id":"审查轮"},"reviewThreadId":"子线程"})),
    );
    assert_eq!(m.stops.operations["run-9"], current);
    assert_eq!(m.claims["线程"].turn_id(), Some("新轮"));
}

#[test]
fn 多次停止确认后操作表为空且迟到回调没有动作() {
    let mut m = Machine::default();
    apply(&mut m, Event::Connected { conn: 1 });
    m.records.entry("线程".into()).or_default().subscribed = true;
    for i in 0..64 {
        let id = i * 3 + 1;
        let turn_id = format!("本轮{i}");
        let a = client(&mut m, "turn/start", id, json!({"threadId":"线程"}));
        reply(
            &mut m,
            &req(&a, "turn/start"),
            Ok(json!({"turn":{"id":turn_id}})),
        );
        let initial = stop(&mut m, id + 1, false);
        let get = req(&initial, "thread/goal/get");
        let a = reply(&mut m, &get, Ok(goal("active")));
        let pause = req(&a, "thread/goal/set");
        let a = reply(&mut m, &pause, Ok(goal("paused")));
        let interrupt = req(&a, "turn/interrupt");
        let a = turn(&mut m, "turn/completed", &turn_id);
        assert_eq!(outcome(&a, id + 1)["stopOutcome"], "confirmed");
        assert!(m.stops.operations.is_empty());
        assert!(!m.stops_busy());
        let before = m.clone();
        for context in [get, pause, interrupt] {
            assert!(reply(&mut m, &context, Ok(goal("active"))).is_empty());
        }
        for action in &initial {
            if let Action::Schedule { token, kind, .. } = action {
                assert!(apply(
                    &mut m,
                    Event::Timer {
                        token: token.clone(),
                        kind: kind.clone()
                    }
                )
                .is_empty());
            }
        }
        assert_eq!(m, before);
    }
}

#[test]
fn 关闭期间客户端目标回包正常交付且不学习事实() {
    for method in ["thread/goal/get", "thread/goal/set", "thread/goal/clear"] {
        for result in [Ok(goal("active")), Err(err())] {
            let mut m = running();
            let a = client(
                &mut m,
                method,
                8,
                json!({"threadId":"线程","status":"active"}),
            );
            let context = req(&a, method);
            apply(&mut m, Event::Shutdown);
            let before = m.clone();
            let a = reply(&mut m, &context, result.clone());
            let expected = match result {
                Ok(value) => proto::response_ok(json!(8), value),
                Err(error) => proto::response_err(json!(8), error),
            };
            assert_eq!(
                a,
                vec![Action::Send {
                    conn: 1,
                    message: expected
                }]
            );
            assert_eq!(m, before);
        }
    }
}

#[test]
fn 未知目标变更到期前不放行到期后重新核实制动才确认() {
    for method in ["thread/goal/set", "thread/goal/clear"] {
        let mut m = running();
        let a = client(
            &mut m,
            method,
            8,
            json!({"threadId":"线程","status":"active"}),
        );
        let change = req(&a, method);
        let initial = stop(&mut m, 2, false);
        let a = apply(
            &mut m,
            Event::TimedOut {
                tag: 0,
                deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
                context: change.clone(),
                error: err(),
            },
        );
        let expiry = Event::LateDiscarded {
            context: change.clone(),
        };
        assert!(!a.iter().any(|action| matches!(
            action,
            Action::Schedule {
                kind: TimerKind::GoalChangeExpiry { .. },
                ..
            } | Action::ScheduleAt {
                kind: TimerKind::GoalChangeExpiry { .. },
                ..
            }
        )));
        reply(
            &mut m,
            &req(&a, "thread/goal/get"),
            Ok(json!({"goal":null})),
        );
        turn(&mut m, "turn/completed", "本轮");
        let verify = verification(&mut m, &initial);
        snapshot(&mut m, &req(&verify, "thread/read"), &[]);
        let a = reply(
            &mut m,
            &req(&verify, "thread/goal/get"),
            Ok(json!({"goal":null})),
        );
        let next_verify = timer(&a, |kind| {
            matches!(kind, TimerKind::Stop(_, StopTimer::Verify))
        });
        assert!(m.claims.contains_key("线程"));
        assert!(m.stops_busy());
        assert!(!m.confirmed(op(&m)));
        let old_version = m.stops.version("线程");
        let a = apply(&mut m, expiry);
        assert!(m.stops.changes.is_empty());
        assert!(a.iter().any(|action| matches!(action, Action::Warn(_))));
        assert!(m.stops.version("线程") > old_version);
        assert!(op(&m).brake_version.is_none());
        assert!(m.claims.contains_key("线程"));
        assert!(requests(&a, "thread/goal/get").is_empty());
        let verify = apply(&mut m, next_verify);
        snapshot(&mut m, &req(&verify, "thread/read"), &[]);
        let a = reply(&mut m, &req(&verify, "thread/goal/get"), Ok(goal("active")));
        let pause = req(&a, "thread/goal/set");
        assert!(m.claims.contains_key("线程"));
        let a = reply(&mut m, &pause, Ok(goal("paused")));
        assert_eq!(outcome(&a, 2)["stopOutcome"], "confirmed");
        assert!(m.claims.is_empty());
        assert!(m.stops.operations.is_empty());
        assert!(!m.stops_busy());
    }
}

#[test]
fn 未知目标变更迟到先落定则过期计时器没有动作() {
    let mut m = running();
    let a = client(
        &mut m,
        "thread/goal/set",
        8,
        json!({"threadId":"线程","status":"active"}),
    );
    let change = req(&a, "thread/goal/set");
    let a = apply(
        &mut m,
        Event::TimedOut {
            tag: 0,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: change.clone(),
            error: err(),
        },
    );
    let expiry = Event::LateDiscarded {
        context: change.clone(),
    };
    assert!(!a.iter().any(|action| matches!(
        action,
        Action::Schedule {
            kind: TimerKind::GoalChangeExpiry { .. },
            ..
        } | Action::ScheduleAt {
            kind: TimerKind::GoalChangeExpiry { .. },
            ..
        }
    )));
    apply(
        &mut m,
        Event::LateReply {
            context: change,
            result: Ok(goal("paused")),
        },
    );
    assert!(m.stops.changes.is_empty());
    let before = m.clone();
    assert!(apply(&mut m, expiry.clone()).is_empty());
    assert_eq!(m, before);
    let a = client(
        &mut m,
        "thread/goal/set",
        9,
        json!({"threadId":"线程","status":"active"}),
    );
    apply(
        &mut m,
        Event::TimedOut {
            tag: 0,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: req(&a, "thread/goal/set"),
            error: err(),
        },
    );
    let before = m.clone();
    assert!(apply(&mut m, expiry).is_empty());
    assert_eq!(m, before);
    assert_eq!(m.stops.changes.len(), 1);
}

#[test]
fn 目标版本随历史淘汰清理且重新建立后旧读数失效() {
    let mut m = Machine::default();
    apply(&mut m, Event::Connected { conn: 1 });
    let a = client(&mut m, "thread/goal/get", 8, json!({"threadId":"线程"}));
    let old = req(&a, "thread/goal/get");
    let old_version = m.stops.version("线程");
    assert!(old_version > 0);
    for i in 0..=crate::claim::MAX_HISTORY {
        apply(
            &mut m,
            Event::Notification {
                method: "thread/goal/cleared".into(),
                params: Some(json!({"threadId":format!("历史{i}")})),
            },
        );
    }
    assert!(!m.records.contains_key("线程"));
    assert!(!m.stops.versions.contains_key("线程"));
    assert_eq!(m.stops.versions.len(), crate::claim::MAX_HISTORY);
    let a = client(&mut m, "thread/goal/get", 9, json!({"threadId":"线程"}));
    let fresh = req(&a, "thread/goal/get");
    let fresh_version = m.stops.version("线程");
    assert!(fresh_version > old_version);
    let a = reply(&mut m, &old, Ok(goal("active")));
    assert_eq!(outcome(&a, 8), goal("active"));
    assert_eq!(m.stops.version("线程"), fresh_version);
    assert!(!m.records["线程"].goal_active);
    reply(&mut m, &fresh, Ok(goal("active")));
    assert!(m.records["线程"].goal_active);
    assert_eq!(m.stops.versions.len(), crate::claim::MAX_HISTORY);
}

#[test]
fn 记录淘汰后停止和在途或未知目标变更仍保留版本() {
    for unknown in [false, true] {
        let mut m = Machine::default();
        apply(&mut m, Event::Connected { conn: 1 });
        let stop_actions = stop(&mut m, 2, false);
        let a = client(
            &mut m,
            "thread/goal/set",
            8,
            json!({"threadId":"变更线程","status":"active"}),
        );
        let change = req(&a, "thread/goal/set");
        if unknown {
            apply(
                &mut m,
                Event::TimedOut {
                    tag: 0,
                    deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
                    context: change.clone(),
                    error: err(),
                },
            );
        }
        let stop_version = m.stops.version("线程");
        let change_version = m.stops.version("变更线程");
        for i in 0..=crate::claim::MAX_HISTORY {
            apply(
                &mut m,
                Event::Notification {
                    method: "thread/goal/cleared".into(),
                    params: Some(json!({"threadId":format!("历史{i}")})),
                },
            );
        }
        assert!(!m.records.contains_key("线程"));
        assert!(!m.records.contains_key("变更线程"));
        assert_eq!(m.stops.version("线程"), stop_version);
        assert_eq!(m.stops.version("变更线程"), change_version);
        assert_eq!(m.stops.versions.len(), crate::claim::MAX_HISTORY + 2);
        if unknown {
            apply(
                &mut m,
                Event::LateReply {
                    context: change,
                    result: Err(err()),
                },
            );
        } else {
            reply(&mut m, &change, Err(err()));
        }
        assert!(!m.stops.versions.contains_key("变更线程"));
        reply(
            &mut m,
            &req(&stop_actions, "thread/goal/get"),
            Ok(json!({"goal":null})),
        );
        assert!(m.stops.operations.is_empty());
        for i in 0..=crate::claim::MAX_HISTORY {
            apply(
                &mut m,
                Event::Notification {
                    method: "thread/goal/cleared".into(),
                    params: Some(json!({"threadId":format!("新历史{i}")})),
                },
            );
        }
        assert!(!m.stops.versions.contains_key("线程"));
        assert_eq!(m.stops.versions.len(), crate::claim::MAX_HISTORY);
    }
}

#[test]
fn 全局版本跨线程递增时清除失败仍保留有效制动证据() {
    let mut m = Machine::default();
    apply(&mut m, Event::Connected { conn: 1 });
    let a = stop(&mut m, 2, true);
    let get = req(&a, "thread/goal/get");
    apply(
        &mut m,
        Event::Notification {
            method: "thread/goal/updated".into(),
            params: Some(json!({"threadId":"其他线程","goal":{"status":"active"}})),
        },
    );
    let a = reply(&mut m, &get, Ok(goal("paused")));
    let a = reply(&mut m, &req(&a, "thread/goal/clear"), Err(err()));
    assert_eq!(outcome(&a, 2)["stopOutcome"], "confirmed");
    assert_eq!(outcome(&a, 2)["goal"], "unchanged");
    assert!(m.stops.operations.is_empty());
}

#[test]
fn 转向请求超时只释放且迟到成功也不能取得中断权限() {
    for shared in [false, true] {
        let (mut m, c) = started(shared, "turn/steer");
        let a = apply(
            &mut m,
            Event::TimedOut {
                tag: 17,
                deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
                context: c.clone(),
                error: err(),
            },
        );
        assert!(m.claims.is_empty());
        assert!(m.stops.operations.is_empty());
        assert!(m.stops.late.is_empty());
        assert!(m.stops.reconciliations.is_empty());
        assert!(requests(&a, "turn/interrupt").is_empty());
        assert!(requests(&a, "thread/read").is_empty());
        let a = apply(
            &mut m,
            Event::LateReply {
                context: c,
                result: Ok(json!({"turn":{"id":"别人轮次"}})),
            },
        );
        assert!(a.is_empty());
    }
}
#[test]
fn 审查超时活动父轮次不授予停止权限迟到回包确定子线程才停() {
    let (mut m, c) = started_legacy_review(false);
    let a = apply(
        &mut m,
        Event::TimedOut {
            tag: 18,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: c.clone(),
            error: err(),
        },
    );
    let a = snapshot(&mut m, &req(&a, "thread/read"), &[("父轮", "inProgress")]);
    assert!(requests(&a, "thread/goal/get").is_empty());
    assert!(requests(&a, "turn/interrupt").is_empty());
    assert_eq!(m.pending_reviews, 1);
    apply(
        &mut m,
        Event::Notification {
            method: "turn/started".into(),
            params: Some(json!({"threadId":"子线程","turn":{"id":"子轮"}})),
        },
    );
    assert!(!m.unclaimed.is_empty());
    let a = apply(
        &mut m,
        Event::LateReply {
            context: c,
            result: Ok(json!({"reviewThreadId":"子线程","turn":{"id":"子轮"}})),
        },
    );
    assert_eq!(m.pending_reviews, 0);
    assert!(m.unclaimed.is_empty());
    assert!(!m.claims.contains_key("线程"));
    let get = req(&a, "thread/goal/get");
    assert_eq!(get.params.as_ref().unwrap()["threadId"], "子线程");
    let a = reply(&mut m, &get, Ok(json!({"goal":null})));
    assert_eq!(
        req(&a, "turn/interrupt").params.unwrap()["threadId"],
        "子线程"
    );
}
#[test]
fn 共享释放后迟到启动先按回包编号核实归属且不碰新认领() {
    for own in [true, false] {
        let (mut m, c) = started(true, "turn/start");
        let a = apply(
            &mut m,
            Event::TimedOut {
                tag: 19,
                deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
                context: c.clone(),
                error: err(),
            },
        );
        snapshot(&mut m, &req(&a, "thread/read"), &[]);
        assert!(m.claims.is_empty());
        let a = client(&mut m, "turn/start", 9, json!({"threadId":"线程"}));
        let new_context = req(&a, "turn/start");
        let newer = m.claims["线程"].clone();
        let a = apply(
            &mut m,
            Event::LateReply {
                context: c,
                result: Ok(json!({"turn":{"id":"旧轮"}})),
            },
        );
        assert!(requests(&a, "turn/interrupt").is_empty());
        assert_eq!(m.claims["线程"], newer);
        // 迟到归属在 target_thread 上按轮次查找旧轮：命中且在活动才取得自有证据；
        // 遍历完未命中、元数据非 active 则不存在，结束跟踪。
        let a = snapshot(
            &mut m,
            &req(&a, "thread/turns/list"),
            &[(if own { "旧轮" } else { "别人轮" }, "inProgress")],
        );
        let a = if own {
            reply(
                &mut m,
                &req(&a, "thread/goal/get"),
                Ok(json!({"goal":null})),
            )
        } else {
            a
        };
        assert_eq!(requests(&a, "turn/interrupt").len(), usize::from(own));
        assert_eq!(m.claims["线程"], newer);
        let _ = new_context;
    }
}
#[test]
fn 旧超时启动加入释放新连接认领后旧成功回包不改变新认领() {
    let (mut m, c) = started(true, "turn/start");
    let a = apply(
        &mut m,
        Event::TimedOut {
            tag: 20,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: c.clone(),
            error: err(),
        },
    );
    let old_read = req(&a, "thread/read");
    client(
        &mut m,
        "broker/joined",
        8,
        json!({"threadId":"线程","turnId":"别人轮"}),
    );
    assert!(m.claims.is_empty());
    assert!(m.stops.reconciliations.is_empty());
    assert!(m.stops.late.is_empty());
    apply(&mut m, Event::Connected { conn: 2 });
    let a = apply(
        &mut m,
        Event::ClientRequest {
            request: ClientRequest {
                conn: 2,
                id: json!(9),
                method: "turn/start".into(),
                params: Some(json!({"threadId":"线程"})),
            },
            run_id: Some("新运行".into()),
        },
    );
    assert!(!requests(&a, "turn/start").is_empty());
    let newer = m.claims["线程"].clone();
    let a = snapshot(&mut m, &old_read, &[("别人轮", "inProgress")]);
    assert!(requests(&a, "turn/interrupt").is_empty());
    let a = apply(
        &mut m,
        Event::LateReply {
            context: c,
            result: Ok(json!({"turn":{"id":"别人轮"}})),
        },
    );
    assert!(a.is_empty());
    assert_eq!(m.claims["线程"], newer);
    assert!(!m.stop_busy("线程"));
}
#[test]
fn 对账认领过期转为旧请求跟踪迟到只按旧编号中断() {
    let (mut m, c) = started(false, "turn/start");
    let a = apply(
        &mut m,
        Event::TimedOut {
            tag: 21,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: c.clone(),
            error: err(),
        },
    );
    let old = req(&a, "thread/read");
    m.release("线程", &mut Vec::new());
    assert!(m.stops.reconciliations.is_empty());
    let a = client(&mut m, "turn/start", 9, json!({"threadId":"线程"}));
    let _ = req(&a, "turn/start");
    let newer = m.claims["线程"].clone();
    snapshot(&mut m, &old, &[]);
    let a = apply(
        &mut m,
        Event::LateReply {
            context: c,
            result: Ok(json!({"turn":{"id":"旧轮"}})),
        },
    );
    let a = reply(
        &mut m,
        &req(&a, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    assert_eq!(req(&a, "turn/interrupt").params.unwrap()["turnId"], "旧轮");
    assert_eq!(m.claims["线程"], newer);
    assert!(!m.stop_busy("线程"));
}
#[test]
fn 核实期间客户端目标写失败无通知旧双读失效后下一轮可确认() {
    let mut m = running();
    let initial = stop(&mut m, 2, false);
    reply(
        &mut m,
        &req(&initial, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    let verify = verification(&mut m, &initial);
    let changed = client(
        &mut m,
        "thread/goal/set",
        8,
        json!({"threadId":"线程","status":"active"}),
    );
    let successor = timer(&changed, |k| {
        matches!(k, TimerKind::Stop(_, StopTimer::Verify))
    });
    reply(&mut m, &req(&verify, "thread/goal/get"), Ok(goal("paused")));
    reply(&mut m, &req(&changed, "thread/goal/set"), Err(err()));
    snapshot(&mut m, &req(&verify, "thread/read"), &[]);
    assert!(!op(&m).verifying);
    assert!(m.claims.contains_key("线程"));
    let verify = apply(&mut m, successor);
    snapshot(&mut m, &req(&verify, "thread/read"), &[]);
    let a = reply(
        &mut m,
        &req(&verify, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    assert_eq!(outcome(&a, 2)["stopOutcome"], "confirmed");
    assert!(m.claims.is_empty());
}
#[test]
fn 核实目标读版本拒用时结束轮次并安排后继() {
    let mut m = running();
    let initial = stop(&mut m, 2, false);
    reply(
        &mut m,
        &req(&initial, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    let verify = verification(&mut m, &initial);
    // 模拟已有版本推进但当前轮次尚未被主动作废的回包拒用入口。
    m.stops.bump("线程");
    let a = reply(&mut m, &req(&verify, "thread/goal/get"), Ok(goal("paused")));
    assert!(!op(&m).verifying);
    let successor = timer(&a, |k| matches!(k, TimerKind::Stop(_, StopTimer::Verify)));
    for get in requests(&a, "thread/goal/get") {
        reply(&mut m, &get, Ok(json!({"goal":null})));
    }
    let verify = apply(&mut m, successor);
    snapshot(&mut m, &req(&verify, "thread/read"), &[]);
    reply(
        &mut m,
        &req(&verify, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    assert!(m.claims.is_empty());
}
#[test]
fn 已受理停止转加入统一回复全部等待者并清理不报告确认() {
    for settling in [false, true] {
        let (mut m, c) = started(false, "turn/start");
        let initial = stop(&mut m, 2, false);
        stop(&mut m, 3, true);
        let a = if settling {
            m.shared = true;
            if let State::Starting { settling, .. } = &mut m.claims.get_mut("线程").unwrap().state
            {
                *settling = true;
            }
            let mut a = reply(&mut m, &c, Ok(json!({"turn":{"id":"本轮"}})));
            // 吸收判定经 H2 状态读取层：线程活动，第一个活动轮次是别人轮 → 加入。
            let (driven, _) = crate::test_support::status_read::drive_status(
                &mut m,
                vec![
                    crate::test_support::status_read::StatusStep::Reply(
                        crate::test_support::status_read::expect(
                            crate::test_support::status_read::StatusPhase::Metadata,
                        ),
                        Ok(crate::test_support::status_read::meta("线程", "active")),
                    ),
                    crate::test_support::status_read::StatusStep::Reply(
                        crate::test_support::status_read::expect(
                            crate::test_support::status_read::StatusPhase::Turns,
                        ),
                        Ok(crate::test_support::status_read::turns_page(
                            &[("别人轮", "inProgress")],
                            None,
                        )),
                    ),
                ],
            );
            a.extend(driven);
            a
        } else {
            client(
                &mut m,
                "broker/joined",
                8,
                json!({"threadId":"线程","turnId":"别人轮"}),
            )
        };
        for id in [2, 3] {
            assert!(a.iter().any(|a|matches!(a,Action::Send {message,..} if message["id"]==id && message["error"]["code"] == -32000 && message["error"]["data"]["reason"]=="joined" && message["error"]["message"]==STOP_NOT_OWNED)));
        }
        assert!(m.stops.operations.is_empty());
        assert!(requests(&a, "turn/interrupt").is_empty());
        let a = reply(
            &mut m,
            &req(&initial, "thread/goal/get"),
            Ok(json!({"goal":null})),
        );
        assert!(requests(&a, "turn/interrupt").is_empty());
    }
}
#[test]
fn 审查超时跟踪迟到失败过期淘汰与关闭各结算一次计数() {
    for exit in ["失败", "过期", "淘汰", "关闭"] {
        let (mut m, c) = started_legacy_review(false);
        let a = apply(
            &mut m,
            Event::TimedOut {
                tag: 22,
                deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
                context: c.clone(),
                error: err(),
            },
        );
        let expiry = Event::LateDiscarded { context: c.clone() };
        assert!(!a.iter().any(|action| matches!(
            action,
            Action::ScheduleAt {
                kind: TimerKind::LateExpiry { .. },
                ..
            }
        )));
        snapshot(&mut m, &req(&a, "thread/read"), &[("父轮", "inProgress")]);
        apply(
            &mut m,
            Event::Notification {
                method: "turn/started".into(),
                params: Some(json!({"threadId":"未知子线程","turn":{"id":"子轮"}})),
            },
        );
        assert_eq!(m.pending_reviews, 1);
        assert!(!m.unclaimed.is_empty());
        match exit {
            "失败" => {
                apply(
                    &mut m,
                    Event::LateReply {
                        context: c.clone(),
                        result: Err(err()),
                    },
                );
            }
            "过期" => {
                apply(&mut m, expiry.clone());
            }
            "淘汰" => {
                apply(&mut m, Event::LateDiscarded { context: c.clone() });
            }
            _ => {
                apply(&mut m, Event::Shutdown);
            }
        }
        assert_eq!(m.pending_reviews, 0, "{exit}");
        assert!(m.unclaimed.is_empty(), "{exit}");
        apply(&mut m, expiry);
        apply(
            &mut m,
            Event::LateReply {
                context: c,
                result: Err(err()),
            },
        );
        assert_eq!(m.pending_reviews, 0);
    }
}
#[test]
fn 线上等待截止不延长回包消费后另给收尾期() {
    let (mut m, c) = started(false, "turn/start");
    let deadline = Instant::now() + std::time::Duration::from_millis(LATE_MS);
    let a = apply(
        &mut m,
        Event::TimedOut {
            tag: 23,
            deadline,
            context: c.clone(),
            error: err(),
        },
    );
    assert!(!a.iter().any(|action| matches!(
        action,
        Action::ScheduleAt {
            kind: TimerKind::LateExpiry { .. },
            ..
        }
    )));
    snapshot(&mut m, &req(&a, "thread/read"), &[]);
    assert_eq!(m.stops.late[0].deadline, deadline);
    let a = apply(
        &mut m,
        Event::LateReply {
            context: c,
            result: Ok(json!({"turn":{"id":"迟到轮"}})),
        },
    );
    assert!(a.iter().any(|action| matches!(
        action,
        Action::Schedule {
            kind: TimerKind::LateExpiry { .. },
            delay_ms: LATE_MS,
            ..
        }
    )));
    let expiry = timer(&a, |k| matches!(k, TimerKind::LateExpiry { .. }));
    apply(&mut m, expiry);
    assert!(m.stops.late[0].cleanup_due);
    assert!(m.stops.late[0].goal_busy);
}

#[test]
fn 审查跟踪重复清理不减少另一在途审查计数() {
    let (mut m, c) = started_legacy_review(false);
    m.records.entry("其他线程".into()).or_default().subscribed = true;
    let a = client(
        &mut m,
        "review/start",
        9,
        json!({"threadId":"其他线程","delivery":"detached"}),
    );
    let other = req(&a, "review/start");
    assert_eq!(m.pending_reviews, 2);
    let a = apply(
        &mut m,
        Event::TimedOut {
            tag: 24,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: c.clone(),
            error: err(),
        },
    );
    let expiry = Event::LateDiscarded { context: c.clone() };
    assert!(!a.iter().any(|action| matches!(
        action,
        Action::ScheduleAt {
            kind: TimerKind::LateExpiry { .. },
            ..
        }
    )));
    apply(&mut m, expiry.clone());
    assert_eq!(m.pending_reviews, 1);
    apply(&mut m, expiry);
    apply(
        &mut m,
        Event::LateReply {
            context: c,
            result: Err(err()),
        },
    );
    assert_eq!(m.pending_reviews, 1);
    reply(&mut m, &other, Err(err()));
    assert_eq!(m.pending_reviews, 0);
    assert!(m.unclaimed.is_empty());
}

#[test]
fn 审查超时父线程没有活动也继续等子线程权威回包() {
    let (mut m, c) = started_legacy_review(false);
    let a = apply(
        &mut m,
        Event::TimedOut {
            tag: 25,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: c.clone(),
            error: err(),
        },
    );
    let a = snapshot(&mut m, &req(&a, "thread/read"), &[]);
    assert!(m.claims.contains_key("线程"));
    assert_eq!(m.pending_reviews, 1);
    assert!(requests(&a, "turn/interrupt").is_empty());
    let a = apply(
        &mut m,
        Event::LateReply {
            context: c,
            result: Ok(json!({"reviewThreadId":"子线程","turn":{"id":"子轮"}})),
        },
    );
    assert_eq!(m.pending_reviews, 0);
    assert!(m.unclaimed.is_empty());
    assert_eq!(
        req(&a, "thread/goal/get").params.unwrap()["threadId"],
        "子线程"
    );
}
#[test]
fn 共享迟到轮次与新认领重合时不按旧请求停止新任务() {
    let (mut m, c) = started(true, "turn/start");
    let a = apply(
        &mut m,
        Event::TimedOut {
            tag: 26,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: c.clone(),
            error: err(),
        },
    );
    snapshot(&mut m, &req(&a, "thread/read"), &[]);
    let a = client(&mut m, "turn/start", 9, json!({"threadId":"线程"}));
    let a = reply(
        &mut m,
        &req(&a, "turn/start"),
        Ok(json!({"turn":{"id":"新轮"}})),
    );
    // 新认领的共享吸收判定（settling 读取，不属于本路迁移）仍按全量快照回答。
    reply(
        &mut m,
        &req(&a, "thread/read"),
        Ok(json!({"thread":{"turns":[{"id":"新轮","status":"inProgress"}]}})),
    );
    let newer = m.claims["线程"].clone();
    let a = apply(
        &mut m,
        Event::LateReply {
            context: c,
            result: Ok(json!({"turn":{"id":"新轮"}})),
        },
    );
    // 迟到归属按轮次分页查找（Turn），第一页命中即停。
    let a = snapshot(
        &mut m,
        &req(&a, "thread/turns/list"),
        &[("新轮", "inProgress")],
    );
    assert!(requests(&a, "turn/interrupt").is_empty());
    assert_eq!(m.claims["线程"], newer);
    assert!(m.stops.late.is_empty());
}

#[test]
fn 审查未知子线程的旧父轮通知也不能触发迟到中断() {
    let (mut m, c) = started_legacy_review(false);
    turn(&mut m, "turn/started", "父轮");
    apply(
        &mut m,
        Event::TimedOut {
            tag: 27,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: c,
            error: err(),
        },
    );
    m.release("线程", &mut Vec::new());
    assert_eq!(m.pending_reviews, 1);
    assert!(m.stops.late[0].turn.is_none());
    let a = turn(&mut m, "turn/started", "父轮");
    assert!(requests(&a, "turn/interrupt").is_empty());
    assert!(requests(&a, "thread/goal/get").is_empty());
}
#[test]
fn 已对账自有轮次的迟到回包仍等待目标制动再中断() {
    let (mut m, c) = started(false, "turn/start");
    let a = apply(
        &mut m,
        Event::TimedOut {
            tag: 28,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: c.clone(),
            error: err(),
        },
    );
    let a = snapshot(&mut m, &req(&a, "thread/read"), &[("本轮", "inProgress")]);
    let get = req(&a, "thread/goal/get");
    let a = apply(
        &mut m,
        Event::LateReply {
            context: c,
            result: Ok(json!({"turn":{"id":"本轮"}})),
        },
    );
    assert!(requests(&a, "turn/interrupt").is_empty());
    let a = reply(&mut m, &get, Ok(goal("active")));
    let a = reply(&mut m, &req(&a, "thread/goal/set"), Ok(goal("paused")));
    assert_eq!(requests(&a, "turn/interrupt").len(), 1);
}

#[test]
fn 已对账自有轮次的迟到启动通知先制动且只中断一次() {
    let (mut m, c) = started(false, "turn/start");
    let a = apply(
        &mut m,
        Event::TimedOut {
            tag: 29,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: c,
            error: err(),
        },
    );
    let a = snapshot(&mut m, &req(&a, "thread/read"), &[("本轮", "inProgress")]);
    let get = req(&a, "thread/goal/get");
    let a = turn(&mut m, "turn/started", "本轮");
    assert!(requests(&a, "turn/interrupt").is_empty());
    let a = reply(&mut m, &get, Ok(goal("active")));
    let pause = req(&a, "thread/goal/set");
    assert!(requests(&a, "turn/interrupt").is_empty());
    let a = turn(&mut m, "turn/started", "本轮");
    assert!(requests(&a, "turn/interrupt").is_empty());
    let a = reply(&mut m, &pause, Ok(goal("paused")));
    assert_eq!(requests(&a, "turn/interrupt").len(), 1);
    let interrupt = req(&a, "turn/interrupt");
    let a = turn(&mut m, "turn/started", "本轮");
    assert!(requests(&a, "turn/interrupt").is_empty());
    let a = reply(&mut m, &interrupt, Ok(json!({})));
    assert!(requests(&a, "turn/interrupt").is_empty());
}
#[test]
fn 纯迟到启动通知读取并暂停目标后才按编号中断一次() {
    let (mut m, c) = started(false, "turn/start");
    let a = apply(
        &mut m,
        Event::TimedOut {
            tag: 30,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: c,
            error: err(),
        },
    );
    snapshot(&mut m, &req(&a, "thread/read"), &[]);
    m.stops.late[0].turn = Some("旧轮".into());
    let a = turn(&mut m, "turn/started", "旧轮");
    let get = req(&a, "thread/goal/get");
    assert!(requests(&a, "turn/interrupt").is_empty());
    let a = reply(&mut m, &get, Ok(goal("active")));
    let pause = req(&a, "thread/goal/set");
    assert!(requests(&a, "turn/interrupt").is_empty());
    let a = turn(&mut m, "turn/started", "旧轮");
    assert!(requests(&a, "turn/interrupt").is_empty());
    let a = reply(&mut m, &pause, Ok(goal("paused")));
    assert_eq!(requests(&a, "turn/interrupt").len(), 1);
    let a = turn(&mut m, "turn/started", "旧轮");
    assert!(requests(&a, "turn/interrupt").is_empty());
    assert!(m.claims.is_empty());
}
#[test]
fn 后来认领目标活跃时纯迟到制动等待其释放() {
    let (mut m, c) = started(false, "turn/start");
    let a = apply(
        &mut m,
        Event::TimedOut {
            tag: 31,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: c.clone(),
            error: err(),
        },
    );
    snapshot(&mut m, &req(&a, "thread/read"), &[]);
    let a = client(&mut m, "turn/start", 9, json!({"threadId":"线程"}));
    reply(
        &mut m,
        &req(&a, "turn/start"),
        Ok(json!({"turn":{"id":"新轮"}})),
    );
    let newer = m.claims["线程"].clone();
    let a = apply(
        &mut m,
        Event::LateReply {
            context: c,
            result: Ok(json!({"turn":{"id":"旧轮"}})),
        },
    );
    let a = reply(&mut m, &req(&a, "thread/goal/get"), Ok(goal("active")));
    assert!(requests(&a, "thread/goal/set").is_empty());
    assert!(requests(&a, "turn/interrupt").is_empty());
    assert_eq!(m.claims["线程"], newer);
    m.release("线程", &mut Vec::new());
    let a = apply(&mut m, Event::Connected { conn: 1 });
    let a = reply(&mut m, &req(&a, "thread/goal/get"), Ok(goal("active")));
    let a = reply(&mut m, &req(&a, "thread/goal/set"), Ok(goal("paused")));
    assert_eq!(req(&a, "turn/interrupt").params.unwrap()["turnId"], "旧轮");
}
#[test]
fn 等待线回包的旧本地截止不清理登记或未知目标变更() {
    let (mut m, c) = started(false, "turn/start");
    let a = apply(
        &mut m,
        Event::TimedOut {
            tag: 32,
            deadline: Instant::now(),
            context: c.clone(),
            error: err(),
        },
    );
    assert!(!a.iter().any(|a| matches!(
        a,
        Action::ScheduleAt {
            kind: TimerKind::LateExpiry { .. },
            ..
        }
    )));
    let reg = m.stops.reconciliations.values().next().unwrap().clone();
    let before = m.clone();
    let a = apply(
        &mut m,
        Event::Timer {
            token: reg.token,
            kind: TimerKind::LateExpiry {
                generation: reg.generation,
            },
        },
    );
    assert!(a.is_empty());
    assert_eq!(m, before);
    let mut m = running();
    let a = client(
        &mut m,
        "thread/goal/set",
        8,
        json!({"threadId":"线程","status":"active"}),
    );
    let c = req(&a, "thread/goal/set");
    let id = match c.operation {
        Operation::GoalChange { id, .. } => id,
        _ => unreachable!(),
    };
    let a = apply(
        &mut m,
        Event::TimedOut {
            tag: 33,
            deadline: Instant::now(),
            context: c,
            error: err(),
        },
    );
    assert!(!a.iter().any(|a| matches!(
        a,
        Action::ScheduleAt {
            kind: TimerKind::GoalChangeExpiry { .. },
            ..
        }
    )));
    let before = m.clone();
    let a = apply(
        &mut m,
        Event::Timer {
            token: ClaimToken {
                thread_id: "线程".into(),
                claim_id: 1,
            },
            kind: TimerKind::GoalChangeExpiry { id },
        },
    );
    assert!(a.is_empty());
    assert_eq!(m, before);
}
#[test]
fn 迟到回包已到但定案失败超过六十四条仍限额并结算审查计数() {
    let (mut m, review) = started_legacy_review(true);
    apply(
        &mut m,
        Event::TimedOut {
            tag: 34,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: review.clone(),
            error: err(),
        },
    );
    m.release("线程", &mut Vec::new());
    apply(
        &mut m,
        Event::Notification {
            method: "turn/started".into(),
            params: Some(json!({"threadId":"未认领子线程","turn":{"id":"子轮"}})),
        },
    );
    assert_eq!(m.pending_reviews, 1);
    assert!(!m.unclaimed.is_empty());
    let mut evicted = false;
    for i in 0..65 {
        let thread = format!("待定线程{i}");
        m.records.entry(thread.clone()).or_default().subscribed = true;
        let a = client(&mut m, "turn/start", 100 + i, json!({"threadId":thread}));
        let c = req(&a, "turn/start");
        begin_unresolved(&mut m, &c, 100 + i);
        assert!(m.stops.reconciliations.len() <= MAX_UNRESOLVED_STARTS);
        let token = c.token.as_ref().unwrap();
        m.claims.get_mut(&token.thread_id).unwrap().claim_id += 10_000;
        let a = apply(
            &mut m,
            Event::LateReply {
                context: c,
                result: Ok(json!({"turn":{"id":format!("旧轮{i}")}})),
            },
        );
        evicted |= a
            .iter()
            .any(|action| matches!(action, Action::ForgetLate { tag: 34 }));
        let read = req(&a, "thread/turns/list");
        reply(&mut m, &read, Err(err()));
        assert!(m.stops.late.len() <= MAX_LATE);
        assert!(m.stream_slots.len() <= MAX_UNRESOLVED_STARTS);
        assert!(m
            .stops
            .late
            .iter()
            .filter(|reg| reg.reply_received)
            .all(|reg| reg.late_settling));
    }
    assert!(evicted);
    assert_eq!(m.stops.late.len(), MAX_LATE);
    assert!(m.stops.reconciliations.is_empty());
    assert_eq!(m.pending_reviews, 0);
    assert!(m.unclaimed.is_empty());
    let before = m.clone();
    let a = apply(
        &mut m,
        Event::LateReply {
            context: review,
            result: Err(err()),
        },
    );
    assert!(a.is_empty());
    assert_eq!(m, before);
}

#[test]
fn 普通审查超时迟到错误不清掉另一审查早到完成缓冲() {
    for params in [
        json!({"delivery":"detached"}),
        json!({"threadId":17,"delivery":"detached"}),
    ] {
        let (mut m, normal) = started_legacy_review(false);
        let a = client(&mut m, "review/start", 8, params);
        let forwarded = req(&a, "review/start");
        assert_eq!(forwarded.operation, Operation::Forward);
        assert_ne!(forwarded.review_id, normal.review_id);
        assert_eq!(m.pending_reviews, 2);
        apply(
            &mut m,
            Event::TimedOut {
                tag: 40,
                deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
                context: forwarded.clone(),
                error: err(),
            },
        );
        assert_eq!(m.pending_reviews, 1);
        apply(
            &mut m,
            Event::Notification {
                method: "turn/completed".into(),
                params: Some(json!({"threadId":"子线程","turn":{"id":"子轮"}})),
            },
        );
        let held = m.unclaimed["子线程"].clone();
        let a = apply(
            &mut m,
            Event::LateReply {
                context: forwarded,
                result: Err(err()),
            },
        );
        assert!(a.is_empty());
        assert_eq!(m.pending_reviews, 1);
        assert_eq!(m.unclaimed["子线程"], held);
        reply(
            &mut m,
            &normal,
            Ok(json!({"reviewThreadId":"子线程","turn":{"id":"子轮"}})),
        );
        assert_eq!(m.pending_reviews, 0);
        assert!(m.unclaimed.is_empty());
        assert!(m.claims.is_empty());
        assert!(!m.stop_busy("子线程"));
    }
}
#[test]
fn 普通审查复用客户端编号时旧回包不结算新请求() {
    let mut m = Machine::default();
    apply(&mut m, Event::Connected { conn: 1 });
    let a = client(&mut m, "review/start", 8, json!({"delivery":"detached"}));
    let old = req(&a, "review/start");
    apply(
        &mut m,
        Event::TimedOut {
            tag: 41,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: old.clone(),
            error: err(),
        },
    );
    assert_eq!(m.pending_reviews, 0);
    let a = client(&mut m, "review/start", 8, json!({"delivery":"detached"}));
    let new = req(&a, "review/start");
    assert_ne!(old.review_id, new.review_id);
    assert_eq!(m.pending_reviews, 1);
    apply(
        &mut m,
        Event::LateReply {
            context: old.clone(),
            result: Err(err()),
        },
    );
    assert_eq!(m.pending_reviews, 1);
    reply(&mut m, &old, Err(err()));
    assert_eq!(m.pending_reviews, 1);
    reply(&mut m, &new, Err(err()));
    assert_eq!(m.pending_reviews, 0);
    reply(&mut m, &new, Err(err()));
    assert_eq!(m.pending_reviews, 0);
}
fn begin_unresolved(m: &mut Machine, c: &RequestContext, tag: u64) -> Vec<Action> {
    apply(
        m,
        Event::TimedOut {
            tag,
            deadline: Instant::now() + std::time::Duration::from_millis(LATE_MS),
            context: c.clone(),
            error: err(),
        },
    )
}
fn scheduled_delay(actions: &[Action], predicate: impl Fn(&TimerKind) -> bool) -> u64 {
    actions
        .iter()
        .find_map(|action| match action {
            Action::Schedule { kind, delay_ms, .. } if predicate(kind) => Some(*delay_ms),
            _ => None,
        })
        .expect("缺少预期重读计划")
}
#[test]
fn 启动超时后只有对账也超时才断开熔断错误回包不触发() {
    let (mut m, c) = started(false, "turn/start");
    let a = begin_unresolved(&mut m, &c, 42);
    assert!(!m.app_server_unresponsive);
    let read = req(&a, "thread/read");
    let a = reply(&mut m, &read, Err(err()));
    assert!(!m.app_server_unresponsive);
    let retry = timer(&a, |k| matches!(k, TimerKind::Reconcile { .. }));
    let a = apply(&mut m, retry);
    let read = req(&a, "thread/read");
    let a = begin_unresolved(&mut m, &read, 43);
    assert!(m.app_server_unresponsive);
    assert_eq!(m.stops.reconciliations.len(), 1);
    assert!(m.claims.contains_key("线程"));
    assert_eq!(
        scheduled_delay(&a, |k| matches!(k, TimerKind::Reconcile { .. })),
        30_000
    );
    let (mut m, c) = started(false, "turn/start");
    begin_unresolved(&mut m, &c, 44);
    let a = client(&mut m, "普通请求", 8, json!({}));
    begin_unresolved(&mut m, &req(&a, "普通请求"), 45);
    assert!(!m.app_server_unresponsive);
}
#[test]
fn 熔断期间新流式拒绝普通请求停止和已有对账继续() {
    let (mut m, c) = started(false, "turn/start");
    let a = begin_unresolved(&mut m, &c, 46);
    let a = begin_unresolved(&mut m, &req(&a, "thread/read"), 47);
    let retry = timer(&a, |k| matches!(k, TimerKind::Reconcile { .. }));
    assert!(m.app_server_unresponsive);
    for method in ["turn/start", "review/start", "turn/steer"] {
        for params in [json!({"threadId":"新线程"}), json!({})] {
            let a = client(&mut m, method, 9, params);
            assert!(requests(&a, method).is_empty());
            assert!(a.iter().any(|a|matches!(a,Action::Send {message,..} if message["error"]["code"] == -32000 && message["error"]["data"]["reason"]=="appServerUnresponsive")));
        }
    }
    assert_eq!(m.claims.len(), 1);
    assert_eq!(m.pending_reviews, 0);
    let a = client(&mut m, "普通请求", 10, json!({"threadId":"新线程"}));
    assert_eq!(requests(&a, "普通请求").len(), 1);
    let a = stop(&mut m, 11, false);
    assert_eq!(requests(&a, "thread/goal/get").len(), 1);
    assert!(m.app_server_unresponsive);
    let a = apply(&mut m, retry);
    assert_eq!(requests(&a, "thread/read").len(), 1);
    assert!(m.app_server_unresponsive);
    assert_eq!(m.stops.reconciliations.len(), 1);
}
#[test]
fn 任意请求成功错误和迟到回包都恢复接单本地传输失败不恢复() {
    for late in [false, true] {
        for result in [Ok(json!({"事实":"已回应"})), Err(err())] {
            let (mut m, c) = started(false, "turn/start");
            let a = begin_unresolved(&mut m, &c, 48);
            begin_unresolved(&mut m, &req(&a, "thread/read"), 49);
            assert!(m.app_server_unresponsive);
            let a = client(&mut m, "普通请求", 8, json!({}));
            let ordinary = req(&a, "普通请求");
            apply(
                &mut m,
                Event::TransportFailed {
                    context: ordinary.clone(),
                    error: err(),
                },
            );
            assert!(m.app_server_unresponsive);
            if late {
                apply(
                    &mut m,
                    Event::LateReply {
                        context: ordinary,
                        result,
                    },
                );
            } else {
                reply(&mut m, &ordinary, result);
            }
            assert!(!m.app_server_unresponsive);
            m.records.entry("新线程".into()).or_default().subscribed = true;
            let a = client(&mut m, "turn/start", 9, json!({"threadId":"新线程"}));
            assert_eq!(requests(&a, "turn/start").len(), 1);
        }
    }
}
#[test]
fn 对账失败指数退避到五分钟完整结果后清零() {
    let (mut m, c) = started(true, "turn/start");
    let a = begin_unresolved(&mut m, &c, 50);
    let mut read = req(&a, "thread/read");
    for expected in [15_000, 30_000, 60_000, 120_000, 240_000, 300_000, 300_000] {
        let a = reply(&mut m, &read, Err(err()));
        assert_eq!(
            scheduled_delay(&a, |k| matches!(k, TimerKind::Reconcile { .. })),
            expected
        );
        let a = apply(
            &mut m,
            timer(&a, |k| matches!(k, TimerKind::Reconcile { .. })),
        );
        read = req(&a, "thread/read");
    }
    let a = snapshot(&mut m, &read, &[("别人的轮", "inProgress")]);
    assert_eq!(
        scheduled_delay(&a, |k| matches!(k, TimerKind::Reconcile { .. })),
        VERIFY_MS
    );
    assert_eq!(
        m.stops
            .reconciliations
            .values()
            .next()
            .unwrap()
            .reconcile_delay_ms,
        0
    );
    let a = apply(
        &mut m,
        timer(&a, |k| matches!(k, TimerKind::Reconcile { .. })),
    );
    let a = reply(&mut m, &req(&a, "thread/read"), Err(err()));
    assert_eq!(
        scheduled_delay(&a, |k| matches!(k, TimerKind::Reconcile { .. })),
        15_000
    );
}
#[test]
fn 对账退避按请求独立并且停止核实仍固定十五秒() {
    let (mut m, c) = started(false, "turn/start");
    let a = begin_unresolved(&mut m, &c, 51);
    let mut read = req(&a, "thread/read");
    for _ in 0..4 {
        let a = reply(&mut m, &read, Err(err()));
        let a = apply(
            &mut m,
            timer(&a, |k| matches!(k, TimerKind::Reconcile { .. })),
        );
        read = req(&a, "thread/read");
    }
    m.records.entry("另一线程".into()).or_default().subscribed = true;
    let a = client(&mut m, "turn/start", 9, json!({"threadId":"另一线程"}));
    let a = begin_unresolved(&mut m, &req(&a, "turn/start"), 52);
    let a = reply(&mut m, &req(&a, "thread/read"), Err(err()));
    assert_eq!(
        scheduled_delay(&a, |k| matches!(k, TimerKind::Reconcile { .. })),
        15_000
    );
    let mut independent = running().claims.remove("线程").unwrap();
    independent.claim_id = 999;
    independent.run_id = Some("停止运行".into());
    m.claims.insert("停止线程".into(), independent);
    let initial = client(
        &mut m,
        "broker/threads/stop",
        10,
        json!({"threadId":"停止线程"}),
    );
    reply(
        &mut m,
        &req(&initial, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    let verify = verification(&mut m, &initial);
    reply(&mut m, &req(&verify, "thread/read"), Err(err()));
    let a = reply(&mut m, &req(&verify, "thread/goal/get"), Err(err()));
    assert_eq!(
        scheduled_delay(&a, |k| matches!(k, TimerKind::Stop(_, StopTimer::Verify))),
        15_000
    );
}
#[test]
fn 六十四项未结清对账拒绝新开工结清一项后放行() {
    let mut m = Machine::default();
    apply(&mut m, Event::Connected { conn: 1 });
    let mut reads = Vec::new();
    for i in 0..64 {
        let thread = format!("线程{i}");
        m.records.entry(thread.clone()).or_default().subscribed = true;
        let a = client(&mut m, "turn/start", 100 + i, json!({"threadId":thread}));
        let c = req(&a, "turn/start");
        let a = begin_unresolved(&mut m, &c, 100 + i);
        reads.push(req(&a, "thread/read"));
    }
    assert_eq!(m.stops.reconciliations.len(), MAX_UNRESOLVED_STARTS);
    assert_eq!(m.claims.len(), 64);
    for method in ["turn/start", "review/start", "turn/steer"] {
        let a = client(&mut m, method, 1000, json!({"threadId":"新线程"}));
        assert!(requests(&a, method).is_empty());
        assert!(a.iter().any(|a|matches!(a,Action::Send {message,..} if message["error"]["code"] == -32000 && message["error"]["data"]["reason"]=="tooManyUnresolvedStarts")));
    }
    assert_eq!(m.claims.len(), 64);
    assert_eq!(m.stops.reconciliations.len(), 64);
    assert_eq!(m.pending_reviews, 0);
    let a = client(&mut m, "普通请求", 1001, json!({}));
    assert_eq!(requests(&a, "普通请求").len(), 1);
    let a = client(
        &mut m,
        "broker/threads/stop",
        1002,
        json!({"threadId":"线程0"}),
    );
    assert_eq!(requests(&a, "thread/goal/get").len(), 1);
    snapshot(&mut m, &reads[0], &[]);
    assert_eq!(m.stops.reconciliations.len(), 63);
    assert!(m.claims.contains_key("线程1"));
    m.records.entry("新线程".into()).or_default().subscribed = true;
    let a = client(&mut m, "turn/start", 1003, json!({"threadId":"新线程"}));
    assert_eq!(requests(&a, "turn/start").len(), 1);
}
#[test]
fn 阻塞期间重复停止只合并一次显式重发且全部等待者结算() {
    let mut m = running();
    let a = client(
        &mut m,
        "thread/goal/set",
        8,
        json!({"threadId":"线程","status":"active"}),
    );
    let change = req(&a, "thread/goal/set");
    for i in 100..132 {
        let a = stop(&mut m, i, false);
        assert!(requests(&a, "turn/interrupt").is_empty());
    }
    assert!(op(&m).explicit_interrupts);
    assert_eq!(op(&m).waiters.len(), 32);
    let a = reply(&mut m, &change, Ok(goal("active")));
    let a = reply(&mut m, &req(&a, "thread/goal/get"), Ok(goal("active")));
    let pause = req(&a, "thread/goal/set");
    for i in 132..164 {
        let a = stop(&mut m, i, false);
        assert!(requests(&a, "turn/interrupt").is_empty());
    }
    let a = reply(&mut m, &pause, Ok(goal("paused")));
    assert_eq!(requests(&a, "turn/interrupt").len(), 1);
    assert!(!op(&m).explicit_interrupts);
    let a = turn(&mut m, "turn/completed", "本轮");
    for i in 100..164 {
        assert_eq!(outcome(&a, i)["stopOutcome"], "confirmed");
    }
    assert!(m.stops.operations.is_empty());
}
#[test]
fn 停止证据只记目标及自有轮次无关完成不使集合增长() {
    let mut m = running();
    m.record("线程").ended_turns.push_back("历史外部轮".into());
    let initial = stop(&mut m, 2, false);
    let a = reply(
        &mut m,
        &req(&initial, "thread/goal/get"),
        Ok(goal("active")),
    );
    let pause = req(&a, "thread/goal/set");
    assert!(op(&m).ended.is_empty());
    turn(&mut m, "turn/started", "新轮");
    turn(&mut m, "turn/completed", "本轮");
    assert!(op(&m).ended.contains("本轮"));
    assert!(!op(&m).ended.contains("新轮"));
    for i in 0..256 {
        turn(&mut m, "turn/completed", &format!("无关轮{i}"));
    }
    assert_eq!(op(&m).ended.len(), 1);
    assert!(op(&m).ended.contains("本轮"));
    assert!(!m.records["线程"].ended_turns.contains(&"本轮".into()));
    let a = reply(&mut m, &pause, Ok(goal("paused")));
    assert_eq!(req(&a, "turn/interrupt").params.unwrap()["turnId"], "新轮");
    let verify = verification(&mut m, &initial);
    snapshot(
        &mut m,
        &req(&verify, "thread/read"),
        &[("本轮", "inProgress"), ("新轮", "inProgress")],
    );
    reply(&mut m, &req(&verify, "thread/goal/get"), Ok(goal("paused")));
    assert!(m.claims.contains_key("线程"));
    assert!(op(&m).ended.contains("本轮"));
    let a = turn(&mut m, "turn/completed", "新轮");
    assert_eq!(outcome(&a, 2)["stopOutcome"], "confirmed");
    assert!(m.claims.is_empty());
}

#[test]
fn 停止操作保留发起前本认领自有完成证据并排除历史外部轮() {
    let mut m = running();
    apply(
        &mut m,
        Event::Notification {
            method: "thread/goal/updated".into(),
            params: Some(json!({"threadId":"线程","goal":{"status":"active"}})),
        },
    );
    turn(&mut m, "turn/completed", "本轮");
    turn(&mut m, "turn/started", "续轮");
    m.record("线程").ended_turns.push_back("历史外部轮".into());
    let a = stop(&mut m, 2, false);
    let get = req(&a, "thread/goal/get");
    assert!(op(&m).ended.contains("本轮"));
    assert!(!op(&m).ended.contains("历史外部轮"));
    assert_eq!(op(&m).ended.len(), 1);
    let a = reply(&mut m, &get, Ok(goal("active")));
    let pause = req(&a, "thread/goal/set");
    turn(&mut m, "turn/completed", "续轮");
    assert_eq!(op(&m).ended.len(), 2);
    let a = reply(&mut m, &pause, Ok(goal("paused")));
    assert_eq!(outcome(&a, 2)["stopOutcome"], "confirmed");
    assert!(requests(&a, "turn/interrupt").is_empty());
}

fn orphan_pause() -> (Machine, RequestContext) {
    let (mut m, c) = started(false, "turn/start");
    let a = begin_unresolved(&mut m, &c, 60);
    snapshot(&mut m, &req(&a, "thread/read"), &[]);
    let a = apply(
        &mut m,
        Event::LateReply {
            context: c,
            result: Ok(json!({"turn":{"id":"旧轮"}})),
        },
    );
    let a = reply(&mut m, &req(&a, "thread/goal/get"), Ok(goal("active")));
    let pause = req(&a, "thread/goal/set");
    (m, pause)
}
fn probe_timer_event(actions: &[Action]) -> Event {
    actions
        .iter()
        .find_map(|a| match a {
            Action::ScheduleBreakerProbe { generation, .. } => Some(Event::BreakerProbeTimer {
                generation: *generation,
            }),
            _ => None,
        })
        .expect("缺少独立试探计时器")
}
#[test]
fn 纯迟到暂停在途及旧轮已完成时同线程新开工仍被挡住() {
    let (mut m, pause) = orphan_pause();
    assert!(m.claims.is_empty());
    for method in ["turn/start", "review/start", "turn/steer"] {
        let a = client(&mut m, method, 70, json!({"threadId":"线程"}));
        assert!(a.iter().any(|a|matches!(a,Action::Send {message,..} if message["error"]["code"] == -32001 && message["error"]["message"]=="A turn is already running on this thread.")));
        assert!(requests(&a, method).is_empty());
    }
    turn(&mut m, "turn/completed", "旧轮");
    assert!(m.stops.late.is_empty());
    assert_eq!(m.stops.changes.len(), 1);
    let a = client(&mut m, "turn/start", 71, json!({"threadId":"线程"}));
    assert!(requests(&a, "turn/start").is_empty());
    assert!(m.stream_slots.is_empty());
    reply(&mut m, &pause, Ok(goal("paused")));
    let a = client(&mut m, "turn/start", 72, json!({"threadId":"线程"}));
    assert_eq!(requests(&a, "turn/start").len(), 1);
}
#[test]
fn 迟到登记淘汰后未知暂停屏障保留至结果落定或十分钟截止() {
    for late_reply in [false, true] {
        let (mut m, pause) = orphan_pause();
        let original = m.stops.late[0].clone();
        for i in 0..64 {
            let mut fake = original.clone();
            fake.generation = 10_000 + i;
            fake.target_thread = format!("其他线程{i}");
            fake.turn = None;
            fake.stream_id = None;
            m.push_late(fake, &mut Vec::new());
        }
        assert!(m
            .stops
            .late
            .iter()
            .all(|reg| reg.generation != original.generation));
        assert!(m.stops.changes.values().any(|change| change.late_brake));
        begin_unresolved(&mut m, &pause, 61);
        assert!(m
            .stops
            .changes
            .values()
            .any(|change| change.unknown && change.late_brake));
        let a = client(&mut m, "turn/start", 73, json!({"threadId":"线程"}));
        assert!(requests(&a, "turn/start").is_empty());
        if late_reply {
            apply(
                &mut m,
                Event::LateReply {
                    context: pause.clone(),
                    result: Ok(goal("paused")),
                },
            );
        } else {
            apply(
                &mut m,
                Event::LateDiscarded {
                    context: pause.clone(),
                },
            );
        }
        assert!(m.stops.changes.is_empty());
        let a = client(&mut m, "turn/start", 74, json!({"threadId":"线程"}));
        assert_eq!(requests(&a, "turn/start").len(), 1);
    }
}
#[test]
fn 最后审查对账清理后熔断仍有独立试探且恢复接单() {
    for result in [Ok(json!({"data":[]})), Err(err())] {
        let (mut m, c) = started_legacy_review(false);
        let a = begin_unresolved(&mut m, &c, 62);
        let a = begin_unresolved(&mut m, &req(&a, "thread/read"), 63);
        let old_timer = timer(&a, |kind| matches!(kind, TimerKind::Reconcile { .. }));
        assert!(m.app_server_unresponsive);
        assert!(!m.stops.probe.scheduled);
        let a = apply(&mut m, Event::LateDiscarded { context: c });
        assert!(m.stops.reconciliations.is_empty());
        assert!(m.stream_slots.is_empty());
        assert!(m.stops.probe.scheduled);
        assert_eq!(m.pending_reviews, 0);
        let probe_timer = probe_timer_event(&a);
        let a = apply(&mut m, old_timer);
        assert!(requests(&a, "thread/read").is_empty());
        assert!(!a
            .iter()
            .any(|a| matches!(a, Action::ScheduleBreakerProbe { .. })));
        let a = client(&mut m, "turn/start", 75, json!({"threadId":"线程"}));
        assert!(requests(&a, "turn/start").is_empty());
        let a = apply(&mut m, probe_timer.clone());
        let probe = req(&a, "thread/list");
        assert_eq!(probe.params, Some(json!({"limit":1})));
        assert_eq!(m.stops.probe.pending, Some(m.stops.probe.generation));
        assert!(!m.stops.probe.scheduled);
        assert!(requests(&apply(&mut m, probe_timer), "thread/list").is_empty());
        reply(&mut m, &probe, result);
        assert!(!m.app_server_unresponsive);
        assert!(m.stops.probe.pending.is_none());
        let a = client(&mut m, "turn/start", 76, json!({"threadId":"线程"}));
        assert_eq!(requests(&a, "turn/start").len(), 1);
    }
}
#[test]
fn 独立试探去重指数退避到五分钟合上取消后继() {
    let mut m = Machine::default();
    m.app_server_unresponsive = true;
    let mut actions = apply(&mut m, Event::Connected { conn: 1 });
    for expected in [15_000, 30_000, 60_000, 120_000, 240_000, 300_000, 300_000] {
        assert!(actions.iter().any(
            |a| matches!(a,Action::ScheduleBreakerProbe {delay_ms,..} if *delay_ms==expected)
        ));
        assert!(!apply(&mut m, Event::Connected { conn: 1 })
            .iter()
            .any(|a| matches!(a, Action::ScheduleBreakerProbe { .. })));
        let timer = probe_timer_event(&actions);
        let a = apply(&mut m, timer.clone());
        let probe = req(&a, "thread/list");
        assert!(m.stops.probe.pending.is_some());
        assert!(requests(&apply(&mut m, timer), "thread/list").is_empty());
        actions = begin_unresolved(&mut m, &probe, 64);
    }
    let timer = probe_timer_event(&actions);
    let a = apply(&mut m, Event::AppResponsive);
    assert!(a
        .iter()
        .any(|a| matches!(a, Action::CancelBreakerProbe { .. })));
    assert!(!m.stops.probe.scheduled);
    assert_eq!(m.stops.probe.delay_ms, 0);
    assert!(requests(&apply(&mut m, timer), "thread/list").is_empty());
}
#[test]
fn 集中受理含恢复屏障的启动最多六十四个超时沿用名额() {
    for method in ["turn/start", "review/start", "turn/steer"] {
        let mut m = Machine::default();
        apply(&mut m, Event::Connected { conn: 1 });
        let mut sent = Vec::new();
        let mut resumes = Vec::new();
        for i in 0..70 {
            let thread = format!("批次线程{i}");
            if i % 2 == 0 {
                m.records.entry(thread.clone()).or_default().subscribed = true;
            }
            let a = client(&mut m, method, 1000 + i, json!({"threadId":thread}));
            if i < 64 {
                if i % 2 == 0 {
                    sent.push(req(&a, method));
                } else {
                    resumes.push(req(&a, "thread/resume"));
                }
            } else {
                assert!(requests(&a, method).is_empty());
                assert!(requests(&a, "thread/resume").is_empty());
                assert!(a.iter().any(|a|matches!(a,Action::Send {message,..} if message["error"]["data"]["reason"]=="tooManyUnresolvedStarts")));
            }
            assert!(m.stream_slots.len() <= 64);
        }
        assert_eq!(m.claims.len(), 64);
        assert_eq!(m.stream_slots.len(), 64);
        for resume in resumes {
            let a = reply(&mut m, &resume, Ok(json!({})));
            sent.push(req(&a, method));
            assert_eq!(m.stream_slots.len(), 64);
        }
        let mut reads = Vec::new();
        for c in &sent {
            let a = begin_unresolved(&mut m, c, 1000);
            reads.extend(requests(&a, "thread/read"));
            assert!(m.stream_slots.len() <= 64);
            assert!(m.stops.reconciliations.len() <= 64);
        }
        if method == "turn/steer" {
            assert!(m.stream_slots.is_empty());
            assert!(m.stops.reconciliations.is_empty());
        } else {
            assert_eq!(m.stream_slots.len(), 64);
            assert_eq!(m.stops.reconciliations.len(), 64);
            if method == "turn/start" {
                snapshot(&mut m, &reads[0], &[]);
            } else {
                apply(
                    &mut m,
                    Event::LateReply {
                        context: sent[0].clone(),
                        result: Err(err()),
                    },
                );
            }
            assert_eq!(m.stream_slots.len(), 63);
            assert_eq!(m.stops.reconciliations.len(), 63);
        }
        m.records.entry("补入线程".into()).or_default().subscribed = true;
        let a = client(&mut m, "turn/start", 2000, json!({"threadId":"补入线程"}));
        assert_eq!(requests(&a, "turn/start").len(), 1);
    }
}
#[test]
fn 恢复屏障取消及启动回包落定释放名额完成早于回包不提前释放() {
    let mut m = Machine::default();
    apply(&mut m, Event::Connected { conn: 1 });
    let a = client(&mut m, "turn/start", 1, json!({"threadId":"线程"}));
    let resume = req(&a, "thread/resume");
    assert_eq!(m.stream_slots.len(), 1);
    let a = stop(&mut m, 2, false);
    assert!(m.stream_slots.is_empty());
    reply(
        &mut m,
        &req(&a, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    let a = reply(&mut m, &resume, Ok(json!({})));
    assert!(requests(&a, "turn/start").is_empty());
    assert!(m.stream_slots.is_empty());
    let (mut m, c) = started(false, "turn/start");
    turn(&mut m, "turn/started", "本轮");
    turn(&mut m, "turn/completed", "本轮");
    assert!(m.claims.is_empty());
    assert_eq!(m.stream_slots.len(), 1);
    reply(&mut m, &c, Ok(json!({"turn":{"id":"本轮"}})));
    assert!(m.stream_slots.is_empty());
}
#[test]
fn 连续目标轮次只保留未结束轮及最近十六个完成轮() {
    let mut m = running();
    apply(
        &mut m,
        Event::Notification {
            method: "thread/goal/updated".into(),
            params: Some(json!({"threadId":"线程","goal":{"status":"active"}})),
        },
    );
    let mut current = "本轮".to_owned();
    for i in 0..600 {
        turn(&mut m, "turn/completed", &current);
        assert!(m.claims["线程"].owned_turns.len() <= crate::claim::ENDED_PER_THREAD);
        assert!(m.claims["线程"].owned_ended.len() <= crate::claim::ENDED_PER_THREAD);
        current = format!("续轮{i}");
        turn(&mut m, "turn/started", &current);
        assert!(m.claims["线程"].owned_turns.len() <= crate::claim::ENDED_PER_THREAD + 1);
    }
    turn(&mut m, "turn/started", "未结束甲");
    turn(&mut m, "turn/started", "未结束乙");
    assert_eq!(
        m.claims["线程"].owned_turns.len(),
        crate::claim::ENDED_PER_THREAD + 3
    );
    assert!(m.claims["线程"].owned_turns.contains(&current));
    assert!(m.claims["线程"].owned_turns.contains("未结束甲"));
    assert!(m.claims["线程"].owned_turns.contains("未结束乙"));
}
#[test]
fn 自有历史淘汰不删除停止操作增量保存的结束证据() {
    let mut m = running();
    let a = stop(&mut m, 2, false);
    let a = reply(&mut m, &req(&a, "thread/goal/get"), Ok(goal("active")));
    let pause = req(&a, "thread/goal/set");
    turn(&mut m, "turn/completed", "本轮");
    for i in 0..80 {
        let id = format!("停止续轮{i}");
        turn(&mut m, "turn/started", &id);
        turn(&mut m, "turn/completed", &id);
        assert!(m.claims["线程"].owned_turns.len() <= crate::claim::ENDED_PER_THREAD);
    }
    assert!(!m.claims["线程"].owned_turns.contains("本轮"));
    assert!(!m.records["线程"].ended_turns.contains(&"本轮".into()));
    assert!(op(&m).owned_turns.contains("本轮"));
    assert!(op(&m).ended.contains("本轮"));
    let a = reply(&mut m, &pause, Ok(goal("paused")));
    assert_eq!(outcome(&a, 2)["stopOutcome"], "confirmed");
    assert!(requests(&a, "turn/interrupt").is_empty());
}
#[test]
fn 停止等待者最多六十四个超出立即回当前未确认结果() {
    let mut m = running();
    let initial = stop(&mut m, 100, false);
    let get = req(&initial, "thread/goal/get");
    for id in 101..164 {
        stop(&mut m, id, false);
    }
    assert_eq!(op(&m).waiters.len(), MAX_STOP_WAITERS);
    for id in 164..300 {
        let a = stop(&mut m, id, true);
        assert_eq!(outcome(&a, id)["stopOutcome"], "unconfirmed");
        assert_eq!(op(&m).waiters.len(), 64);
        assert!(!a.iter().any(|a| matches!(
            a,
            Action::Schedule {
                kind: TimerKind::Stop(_, StopTimer::Waiter(_)),
                ..
            }
        )));
    }
    let a = reply(&mut m, &get, Ok(json!({"goal":null})));
    assert_eq!(requests(&a, "turn/interrupt").len(), 1);
    let a = turn(&mut m, "turn/completed", "本轮");
    for id in 100..164 {
        assert_eq!(outcome(&a, id)["stopOutcome"], "confirmed");
    }
    assert_eq!(
        a.iter()
            .filter(|a| matches!(a,Action::Send {message,..} if message.get("result").is_some()))
            .count(),
        64
    );
    assert!(m.stops.operations.is_empty());
}

#[test]
fn 收尾截止先到随后暂停超时仍等待本次写入落定再中断一次() {
    for success in [false, true] {
        let (mut m, pause) = orphan_pause();
        let reg = m.stops.late[0].clone();
        apply(
            &mut m,
            Event::Timer {
                token: reg.token,
                kind: TimerKind::LateExpiry {
                    generation: reg.generation,
                },
            },
        );
        assert!(m.stops.late[0].cleanup_due);
        begin_unresolved(&mut m, &pause, 65);
        assert_eq!(m.stops.late.len(), 1);
        assert!(m.stops.changes.values().any(|change| change.unknown));
        let a = if success {
            apply(
                &mut m,
                Event::LateReply {
                    context: pause,
                    result: Ok(goal("paused")),
                },
            )
        } else {
            apply(&mut m, Event::LateDiscarded { context: pause })
        };
        assert_eq!(requests(&a, "turn/interrupt").len(), 1);
        assert!(requests(&a, "thread/goal/get").is_empty());
        assert!(m.stops.late.is_empty());
        assert!(m.stops.changes.is_empty());
    }
}

#[test]
fn 关闭后的有效或无效线程开工都不再预留名额或审查计数() {
    let mut m = Machine::default();
    apply(&mut m, Event::Connected { conn: 1 });
    apply(&mut m, Event::Shutdown);
    for method in ["turn/start", "review/start", "turn/steer"] {
        for params in [json!({}), json!({"threadId":"线程"})] {
            let a = client(&mut m, method, 1, params);
            assert!(requests(&a, method).is_empty());
            assert!(a.iter().any(|a|matches!(a,Action::Send {message,..} if message["error"]["message"]==crate::claim::SHUTTING_DOWN)));
        }
    }
    assert!(m.stream_slots.is_empty());
    assert_eq!(m.pending_reviews, 0);
}
#[test]
fn 未知暂停被迟到登记上限挤出后屏障保留到原截止再解除() {
    let (mut m, pause) = orphan_pause();
    let deadline = Instant::now() + std::time::Duration::from_millis(LATE_MS);
    apply(
        &mut m,
        Event::TimedOut {
            tag: 61,
            deadline,
            context: pause.clone(),
            error: err(),
        },
    );
    let id = *m.stops.changes.keys().next().unwrap();
    assert!(m.stops.changes[&id].unknown && m.stops.changes[&id].late_brake);
    // 被挤出不等于到期：屏障保留，改由本地计时器在原截止清理。
    let a = apply(
        &mut m,
        Event::LateEvicted {
            context: pause.clone(),
            deadline,
        },
    );
    assert!(m.stops.changes[&id].unknown && m.stops.changes[&id].wire_tag.is_none());
    let (token, kind) = a
        .iter()
        .find_map(|action| match action {
            Action::ScheduleAt {
                token,
                kind: kind @ TimerKind::GoalChangeExpiry { id: got },
                deadline: at,
            } if *got == id && *at == deadline => Some((token.clone(), kind.clone())),
            _ => None,
        })
        .expect("缺少原截止的本地清理计时器");
    let a = client(&mut m, "turn/start", 75, json!({"threadId":"线程"}));
    assert!(requests(&a, "turn/start").is_empty());
    // 原截止到了才清理，屏障随之解除。
    apply(&mut m, Event::Timer { token, kind });
    assert!(m.stops.changes.is_empty());
    let a = client(&mut m, "turn/start", 76, json!({"threadId":"线程"}));
    assert_eq!(requests(&a, "turn/start").len(), 1);
}
#[test]
fn 收尾期到后读回目标活跃且线程已有后来认领时结束旧跟踪() {
    let (mut m, c) = started(false, "turn/start");
    let a = begin_unresolved(&mut m, &c, 32);
    snapshot(&mut m, &req(&a, "thread/read"), &[]);
    let a = client(&mut m, "turn/start", 9, json!({"threadId":"线程"}));
    reply(
        &mut m,
        &req(&a, "turn/start"),
        Ok(json!({"turn":{"id":"新轮"}})),
    );
    let newer = m.claims["线程"].clone();
    let a = apply(
        &mut m,
        Event::LateReply {
            context: c,
            result: Ok(json!({"turn":{"id":"旧轮"}})),
        },
    );
    let get = req(&a, "thread/goal/get");
    // 读回之前收尾期到：制动在途，只记下到期。
    let reg = m.stops.late[0].clone();
    apply(
        &mut m,
        Event::Timer {
            token: reg.token.clone(),
            kind: TimerKind::LateExpiry {
                generation: reg.generation,
            },
        },
    );
    assert!(m.stops.late[0].cleanup_due);
    // 读回目标活跃、线程已由后来认领占用：不暂停、不中断，结束旧跟踪，也不无限等待。
    let a = reply(&mut m, &get, Ok(goal("active")));
    assert!(requests(&a, "thread/goal/set").is_empty());
    assert!(requests(&a, "turn/interrupt").is_empty());
    assert!(m.stops.late.is_empty());
    assert_eq!(m.claims["线程"], newer);
}

#[test]
fn 五项展示与原停止判定逐项等价() {
    let mut base = running();
    stop(&mut base, 2, false);
    let original = op(&base).clone();
    for flags in 0..2048 {
        let mut m = base.clone();
        let mut candidate = original.clone();
        let bit = |n| flags & (1 << n) != 0;
        candidate.background.confirmed = bit(10);
        candidate.foreign_claim = bit(0);
        candidate.turn_absent = bit(1);
        if bit(2) {
            candidate.ended.insert("本轮".into());
        }
        candidate.brake_version = bit(3).then_some(m.stops.version("线程"));
        m.claims.get_mut("线程").unwrap().request_pending = bit(4);
        candidate.verifying = bit(5);
        candidate.read_done = bit(6);
        candidate.goal_read_done = bit(7);
        if bit(8) {
            m.stops.changes.insert(
                999,
                GoalChange {
                    thread: "线程".into(),
                    unknown: true,
                    wire_tag: None,
                    late_brake: false,
                    control_epoch: None,
                    grant_objective: None,
                },
            );
        }
        if let State::Running { joined, .. } = &mut m.claims.get_mut("线程").unwrap().state {
            *joined = bit(9);
        }
        let claim = candidate
            .claim_id
            .and_then(|id| m.claims.get(&candidate.thread).filter(|c| c.claim_id == id));
        let before = candidate.background.confirmed
            && !candidate.foreign_claim
            && claim.is_none_or(|c| !c.settling() && !c.joined())
            && (candidate.turn_absent
                || candidate
                    .target
                    .as_ref()
                    .is_some_and(|t| candidate.ended.contains(t)))
            && candidate.brake_version == Some(m.stops.version(&candidate.thread))
            && claim.is_none_or(|c| !c.request_pending)
            && !m
                .stops
                .changes
                .values()
                .any(|c| c.thread == candidate.thread)
            && (!candidate.verifying || (candidate.read_done && candidate.goal_read_done));
        assert_eq!(m.confirmed(&candidate), before, "组合 {flags}");
        let snapshot = m.clone();
        m.stop_check(&candidate);
        assert_eq!(m, snapshot);
    }
}

#[test]
fn 停止钩子只在唯一判定通过后确认并先于释放() {
    let mut m = running();
    let a = stop(&mut m, 2, false);
    assert_eq!(m.runs.hook_log.last().unwrap(), "run_stop_began");
    let a = reply(
        &mut m,
        &req(&a, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    reply(&mut m, &req(&a, "turn/interrupt"), Ok(json!({})));
    assert!(!m.runs.hook_log.iter().any(|h| h == "run_stop_confirmed"));
    let a = turn(&mut m, "turn/completed", "本轮");
    assert_eq!(
        &m.runs.hook_log[m.runs.hook_log.len() - 2..],
        ["run_stop_confirmed", "run_released"]
    );
    // 乙路账本合入后，回包的 run 是更新之后的运行快照（任务说明 8.1 第 13 条）。
    let reply = outcome(&a, 2);
    assert_eq!(reply["run"]["runId"], reply["runId"]);
    assert_eq!(reply["run"]["stopOutcome"], "confirmed");
}

#[test]
fn 期限与核实不通过记录展示() {
    let mut m = running();
    let a = stop(&mut m, 2, false);
    let deadline = timer(&a, |kind| {
        matches!(kind, TimerKind::Stop(_, StopTimer::Deadline))
    });
    apply(&mut m, deadline);
    assert!(m.runs.hook_log.iter().any(|h| h == "run_stop_check"));
}

#[test]
fn 丢失运行核实不采用历史完成且实际线程立即双读() {
    let mut m = Machine::new(true);
    m.record("子线程").ended_turns.push_back("历史轮".into());
    let mut actions = Vec::new();
    let id = m.begin_lost_stop("子线程", "丢失运行", false, &mut actions);
    let operation = &m.stops.operations[&id];
    assert_eq!(operation.run_id.as_deref(), Some("丢失运行"));
    assert!(operation.target.is_none());
    assert!(!operation.turn_absent);
    assert!(!operation.already_stopped);
    assert!(operation.ended.is_empty());
    assert!(!m.runs.hook_log.iter().any(|h| h == "run_stop_began"));
    let read = req(&actions, "thread/read");
    assert_eq!(read.params.as_ref().unwrap()["threadId"], "子线程");
    // H2：只读元数据，不再带 includeTurns。
    assert!(read.params.as_ref().unwrap().get("includeTurns").is_none());
    snapshot(&mut m, &read, &[("当前轮", "inProgress")]);
    let a = reply(
        &mut m,
        &req(&actions, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    assert_eq!(
        req(&a, "turn/interrupt").params.as_ref().unwrap()["turnId"],
        "当前轮"
    );
    assert!(!m.runs.hook_log.iter().any(|h| h == "run_stop_confirmed"));
}

#[test]
fn 丢失运行未知读取不放行且确认不释放认领() {
    for result in [Err(err()), Ok(json!({})), Ok(meta("线程", "idle"))] {
        let mut m = Machine::default();
        let mut a = Vec::new();
        m.begin_lost_stop("线程", "丢失运行", false, &mut a);
        reply(&mut m, &req(&a, "thread/read"), result.clone());
        reply(
            &mut m,
            &req(&a, "thread/goal/get"),
            Ok(json!({"goal":null})),
        );
        let confirmed = result.as_ref().ok() == Some(&meta("线程", "idle"));
        assert_eq!(
            m.runs.hook_log.iter().any(|h| h == "run_stop_confirmed"),
            confirmed
        );
        assert!(!m.runs.hook_log.iter().any(|h| h == "run_released"));
    }
}

fn goal_notice(m: &mut Machine, status: &str) -> Vec<Action> {
    apply(
        m,
        Event::Notification {
            method: "thread/goal/updated".into(),
            params: Some(json!({"threadId":"线程","goal":{"status":status}})),
        },
    )
}
/// 只回答本批动作里在途的 turn/interrupt，返回全部后续动作。
fn answer_interrupts(m: &mut Machine, actions: &[Action]) -> Vec<Action> {
    let mut out = Vec::new();
    for c in requests(actions, "turn/interrupt") {
        out.extend(reply(m, &c, Ok(json!({}))));
    }
    out
}
fn has_outcome(actions: &[Action], id: u64) -> bool {
    actions.iter().any(
        |a| matches!(a, Action::Send { message, .. } if message["id"] == id && message.get("result").is_some()),
    )
}

/// 依次回答新出现的 goal 读取（一律 paused）与中断请求，直到不再产生；返回全部动作与读取次数。
fn settle_paused(m: &mut Machine, actions: Vec<Action>) -> (Vec<Action>, usize) {
    let mut all = actions;
    let mut done = 0;
    let mut reads_answered = 0;
    for _ in 0..8 {
        let pending: Vec<_> = all[done..].to_vec();
        done = all.len();
        if pending.is_empty() {
            break;
        }
        assert!(
            requests(&pending, "thread/goal/set").is_empty(),
            "goal 已暂停时不应再次暂停：{pending:?}"
        );
        for c in requests(&pending, "thread/goal/get") {
            reads_answered += 1;
            let next = reply(m, &c, Ok(goal("paused")));
            all.extend(next);
        }
        let next = answer_interrupts(m, &pending);
        all.extend(next);
    }
    (all, reads_answered)
}

#[test]
fn 暂停成功后核实读到已暂停仍报告暂停() {
    // M9 p3：暂停回包与服务器 paused 通知交错，版本变化后的核实读到 paused，不得把报告改回 unchanged。
    for notice_first in [true, false] {
        let mut m = running();
        let a = stop(&mut m, 2, false);
        let a = reply(&mut m, &req(&a, "thread/goal/get"), Ok(goal("active")));
        let pause = req(&a, "thread/goal/set");
        let mut all = Vec::new();
        if notice_first {
            all.extend(goal_notice(&mut m, "paused"));
        }
        all.extend(reply(&mut m, &pause, Ok(goal("paused"))));
        if !notice_first {
            all.extend(goal_notice(&mut m, "paused"));
        }
        assert_eq!(op(&m).goal, "paused");
        let (mut all, before) = settle_paused(&mut m, all);
        assert!(!has_outcome(&all, 2), "轮次未结束前不得确认");
        let completed = turn(&mut m, "turn/completed", "本轮");
        let (rest, after) = settle_paused(&mut m, completed);
        all.extend(rest);
        // 暂停证据已被 paused 通知作废，确认前必须重新读过 goal。
        assert!(
            before + after >= 1,
            "notice_first={notice_first} 未重新核实 goal"
        );
        let result = outcome(&all, 2);
        assert_eq!(result["stopOutcome"], "confirmed");
        assert_eq!(result["goal"], "paused", "notice_first={notice_first}");
    }
}

#[test]
fn 暂停后服务器又改回进行中则重新暂停且失败不确认() {
    let mut m = running();
    let a = stop(&mut m, 2, false);
    let a = reply(&mut m, &req(&a, "thread/goal/get"), Ok(goal("active")));
    let mut all = reply(&mut m, &req(&a, "thread/goal/set"), Ok(goal("paused")));
    all.extend(goal_notice(&mut m, "active"));
    let read = req(&all, "thread/goal/get");
    let again = reply(&mut m, &read, Ok(goal("active")));
    let pause = req(&again, "thread/goal/set");
    all.extend(again);
    let interrupts = answer_interrupts(&mut m, &all);
    all.extend(interrupts);
    all.extend(turn(&mut m, "turn/completed", "本轮"));
    // 第二次暂停尚未回包：制动证据不成立，不得确认。
    assert!(m.stops_busy());
    all.extend(reply(&mut m, &pause, Err(err())));
    assert!(m.stops_busy(), "暂停失败且 goal 仍为 active 时不得确认");
    assert!(!has_outcome(&all, 2));
    // 已成功的第一次暂停保留在报告里。
    assert_eq!(op(&m).goal, "paused");
}

#[test]
fn 暂停成功后合并清除按清除结果报告() {
    for fail in [false, true] {
        let mut m = running();
        let a = stop(&mut m, 2, false);
        let a = reply(&mut m, &req(&a, "thread/goal/get"), Ok(goal("active")));
        let mut all = reply(&mut m, &req(&a, "thread/goal/set"), Ok(goal("paused")));
        all.extend(stop(&mut m, 3, true));
        let read = req(&all, "thread/goal/get");
        let a = reply(&mut m, &read, Ok(goal("paused")));
        let clear = req(&a, "thread/goal/clear");
        all.extend(a);
        all.extend(reply(
            &mut m,
            &clear,
            if fail { Err(err()) } else { Ok(json!({})) },
        ));
        let interrupts = answer_interrupts(&mut m, &all);
        all.extend(interrupts);
        all.extend(turn(&mut m, "turn/completed", "本轮"));
        let expected = if fail { "paused" } else { "cleared" };
        for id in [2, 3] {
            let result = outcome(&all, id);
            assert_eq!(result["stopOutcome"], "confirmed");
            assert_eq!(result["goal"], expected, "fail={fail} id={id}");
        }
    }
}

#[test]
fn 未做成任何动作时按观察与失败报告() {
    // 暂停失败、goal 不是 active、没有 goal，分别报告 unchanged、unchanged、none。
    for (status, write_fails, expected) in [
        (Some("active"), true, "unchanged"),
        (Some("paused"), false, "unchanged"),
        (None, false, "none"),
    ] {
        let mut m = running();
        let a = stop(&mut m, 2, false);
        let read = req(&a, "thread/goal/get");
        let a = reply(&mut m, &read, Ok(status.map_or(json!({"goal":null}), goal)));
        if write_fails {
            reply(&mut m, &req(&a, "thread/goal/set"), Err(err()));
        }
        assert_eq!(op(&m).goal, expected, "status={status:?}");
    }
}

#[test]
fn 按运行停止拒绝换代和未知认领且旧终态不影响新运行() {
    let mut m = running();
    for run in [json!("other"), json!(""), json!(null), json!(42)] {
        let mut actions = client(
            &mut m,
            "broker/threads/stop",
            90,
            json!({"threadId":"线程","runId":run}),
        );
        if run == "other" {
            for action in &actions {
                if let Action::Store(crate::store::StoreOp::Lifecycle(work)) = action {
                    work.ticket
                        .finish(crate::store::lifecycle::ResultValue::Run(None));
                }
            }
            actions = apply(
                &mut m,
                Event::Timer {
                    token: ClaimToken {
                        thread_id: String::new(),
                        claim_id: 0,
                    },
                    kind: TimerKind::Lifecycle,
                },
            );
        }
        let message = actions
            .iter()
            .find_map(|a| match a {
                Action::Send { message, .. } if message["id"] == 90 => Some(message),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            message["error"]["code"],
            if run == "other" { -32000 } else { -32602 }
        );
        if run == "other" {
            assert_eq!(message["error"]["data"]["reason"], "runMismatch");
        }
        assert!(m.stops.operations.is_empty());
        assert!(!actions.iter().any(|a| matches!(a, Action::Request(_))));
    }
    m.runs
        .runs
        .insert("old".into(), crate::store::tests::run("old", "线程", 1));
    let actions = client(
        &mut m,
        "broker/threads/stop",
        91,
        json!({"threadId":"线程","runId":"old","clear":true}),
    );
    assert!(actions.iter().any(|a|matches!(a,Action::Send {message,..} if message["error"]["data"]["reason"]=="runMismatch")));
    assert!(m.stops.operations.is_empty());
    assert_eq!(m.claims["线程"].run_id.as_deref(), Some("run-1"));
    m.claims.clear();
    let actions = client(
        &mut m,
        "broker/threads/stop",
        92,
        json!({"threadId":"线程","runId":"run-1"}),
    );
    assert!(actions.iter().any(|a|matches!(a,Action::Send {message,..} if message["error"]["data"]["reason"]=="runMismatch")));
}

#[test]
fn 按运行停止审查由父标识定位子认领() {
    let (mut m, review) = started_legacy_review(false);
    reply(
        &mut m,
        &review,
        Ok(json!({"reviewThreadId":"child","turn":{"id":"review-turn"}})),
    );
    let _ = client(
        &mut m,
        "turn/start",
        2,
        json!({"threadId":"线程","input":[{"type":"text","text":"父线程的新运行"}]}),
    );
    let actions = client(
        &mut m,
        "broker/threads/stop",
        93,
        json!({"threadId":"线程","runId":"run-1"}),
    );
    assert!(m
        .stops
        .operations
        .values()
        .any(|op| op.thread == "child" && op.run_id.as_deref() == Some("run-1")));
    assert!(!m.stops.operations.values().any(|op| op.thread == "线程"));
    assert!(actions.iter().all(|a| !matches!(a,Action::Request(r) if r.params.as_ref().is_some_and(|p|p["threadId"]=="线程"))));
}
