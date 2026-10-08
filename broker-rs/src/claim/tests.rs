//! 逐行迁移与乱序回归测试（详细设计第 9 节、S3 计划 M2）。

use super::*;

mod ext;
mod loop_guard;
mod resume;
mod review;
mod runs;
mod settlement;

fn apply(machine: &mut Machine, event: Event) -> Vec<Action> {
    let (next, mut actions) = step(std::mem::take(machine), event);
    *machine = next;
    // 认领回归 fixture 的服务器默认没有 goal；完整停止证据在 stop/tests.rs 单独验证。
    let reads: Vec<_> = actions
        .iter()
        .filter_map(|a| match a {
            Action::Request(c)
                if c.method == "thread/goal/get" && matches!(c.operation, Operation::Stop(..)) =>
            {
                Some(c.clone())
            }
            _ => None,
        })
        .collect();
    for context in reads {
        let (next, extra) = step(
            std::mem::take(machine),
            Event::Reply {
                context,
                result: Ok(json!({"goal":null})),
            },
        );
        *machine = next;
        actions.extend(extra);
    }
    crate::test_support::background::empty_background(machine, actions)
}

fn request(conn: u64, id: u64, method: &str, params: Value) -> ClientRequest {
    ClientRequest {
        conn,
        id: json!(id),
        method: method.into(),
        params: Some(params),
    }
}

fn client(request: ClientRequest) -> Event {
    Event::ClientRequest {
        request,
        run_id: Some("run-测试-000001".into()),
    }
}

fn notify(method: &str, params: Value) -> Event {
    Event::Notification {
        method: method.into(),
        params: Some(params),
    }
}

fn turn(method: &str, id: &str) -> Event {
    notify(
        method,
        json!({"threadId": "线程", "turn": {"id": id, "items": [], "status": "completed"}}),
    )
}

fn first_request(actions: &[Action]) -> RequestContext {
    actions
        .iter()
        .find_map(|a| match a {
            Action::Request(c) => Some(c.clone()),
            _ => None,
        })
        .expect("应产生请求动作")
}

fn reply(
    machine: &mut Machine,
    context: &RequestContext,
    result: Result<Value, Value>,
) -> Vec<Action> {
    apply(
        machine,
        Event::Reply {
            context: context.clone(),
            result,
        },
    )
}

fn start(shared: bool, method: &str) -> (Machine, RequestContext) {
    start_params(
        shared,
        method,
        json!({"threadId": "线程", "input": [{"type": "text", "text": "  本次输入  "}]}),
    )
}

/// 显式 legacy 服务器夹具：调用方注入 detached 子线程通知与回包。
fn start_legacy_review(shared: bool) -> (Machine, RequestContext) {
    start_params(
        shared,
        "review/start",
        json!({"threadId": "线程", "delivery": "detached"}),
    )
}

fn start_params(shared: bool, method: &str, params: Value) -> (Machine, RequestContext) {
    let mut machine = Machine::new(shared);
    machine.record("线程").subscribed = true;
    let actions = apply(&mut machine, client(request(1, 42, method, params)));
    (machine, first_request(&actions))
}

fn running() -> (Machine, RequestContext) {
    let (mut machine, context) = start(false, "turn/start");
    reply(&mut machine, &context, Ok(json!({"turn": {"id": "本轮"}})));
    (machine, context)
}

fn sends(actions: &[Action]) -> Vec<(u64, Value)> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Send { conn, message } => Some((*conn, message.clone())),
            _ => None,
        })
        .collect()
}

#[derive(Clone, Copy)]
enum Fixture {
    Idle,
    Closing,
    Starting,
    Settling,
    Running,
    Goal,
    Awaiting,
    Joined,
    Ended,
}
#[derive(Clone, Copy)]
enum Input {
    Start,
    Started,
    Completed,
    Failed,
    Succeeded,
    SharedSucceeded,
    ForeignCompleted,
    Paused,
    Disconnect,
    Interrupt,
    Join,
    ServerRequest,
    Resolved,
    Retarget,
    Stale,
}
#[derive(Clone, Copy)]
enum Expected {
    Idle,
    Starting,
    Held,
    HeldRequest,
    Running,
    Awaiting,
    Joined,
    Unchanged,
    Resolved,
}
#[derive(Clone, Copy)]
enum Effect {
    Error(i64),
    Resume,
    Send,
    Buffer,
    Release,
    Drop,
    Retarget,
    Dismiss,
}

struct Case {
    row: &'static str,
    fixture: Fixture,
    input: Input,
    expected: Expected,
    effect: Effect,
}

#[test]
fn 迁移表逐行断言状态与动作() {
    use Effect as E;
    use Expected as N;
    use Fixture as F;
    use Input as I;
    let cases = [
        Case {
            row: "#1",
            fixture: F::Closing,
            input: I::Start,
            expected: N::Idle,
            effect: E::Error(-32000),
        },
        Case {
            row: "#2",
            fixture: F::Idle,
            input: I::Start,
            expected: N::Starting,
            effect: E::Resume,
        },
        Case {
            row: "#3",
            fixture: F::Running,
            input: I::Start,
            expected: N::Unchanged,
            effect: E::Error(-32001),
        },
        Case {
            row: "#4",
            fixture: F::Starting,
            input: I::Started,
            expected: N::Starting,
            effect: E::Send,
        },
        Case {
            row: "#5",
            fixture: F::Settling,
            input: I::Started,
            expected: N::Held,
            effect: E::Buffer,
        },
        Case {
            row: "#6",
            fixture: F::Settling,
            input: I::ServerRequest,
            expected: N::HeldRequest,
            effect: E::Buffer,
        },
        Case {
            row: "#7",
            fixture: F::Starting,
            input: I::Succeeded,
            expected: N::Running,
            effect: E::Send,
        },
        Case {
            row: "#8",
            fixture: F::Settling,
            input: I::SharedSucceeded,
            expected: N::Running,
            effect: E::Send,
        },
        Case {
            row: "#9",
            fixture: F::Starting,
            input: I::Completed,
            expected: N::Idle,
            effect: E::Release,
        },
        Case {
            row: "#10",
            fixture: F::Starting,
            input: I::Failed,
            expected: N::Idle,
            effect: E::Release,
        },
        Case {
            row: "#12",
            fixture: F::Running,
            input: I::Started,
            expected: N::Running,
            effect: E::Send,
        },
        Case {
            row: "#13",
            fixture: F::Ended,
            input: I::ForeignCompleted,
            expected: N::Unchanged,
            effect: E::Drop,
        },
        Case {
            row: "#14",
            fixture: F::Goal,
            input: I::Completed,
            expected: N::Awaiting,
            effect: E::Send,
        },
        Case {
            row: "#15",
            fixture: F::Running,
            input: I::Completed,
            expected: N::Idle,
            effect: E::Release,
        },
        Case {
            row: "#16",
            fixture: F::Awaiting,
            input: I::Started,
            expected: N::Running,
            effect: E::Send,
        },
        Case {
            row: "#17",
            fixture: F::Awaiting,
            input: I::Paused,
            expected: N::Idle,
            effect: E::Release,
        },
        Case {
            row: "#18",
            fixture: F::Joined,
            input: I::Disconnect,
            expected: N::Idle,
            effect: E::Release,
        },
        Case {
            row: "#23",
            fixture: F::Joined,
            input: I::Interrupt,
            expected: N::Unchanged,
            effect: E::Error(-32000),
        },
        Case {
            row: "#23归属未定",
            fixture: F::Settling,
            input: I::Interrupt,
            expected: N::Unchanged,
            effect: E::Error(-32000),
        },
        Case {
            row: "#24",
            fixture: F::Running,
            input: I::Retarget,
            expected: N::Unchanged,
            effect: E::Retarget,
        },
        Case {
            row: "#25",
            fixture: F::Running,
            input: I::Join,
            expected: N::Joined,
            effect: E::Send,
        },
        Case {
            row: "#26",
            fixture: F::Settling,
            input: I::Resolved,
            expected: N::Resolved,
            effect: E::Dismiss,
        },
        Case {
            row: "#27",
            fixture: F::Running,
            input: I::Stale,
            expected: N::Unchanged,
            effect: E::Send,
        },
    ];
    for case in cases {
        let (mut machine, context) = match case.fixture {
            F::Idle | F::Closing => (
                Machine::new(false),
                RequestContext {
                    resume_retried: false,
                    stream_id: None,
                    review_id: None,
                    client: None,
                    token: None,
                    operation: Operation::Forward,
                    method: String::new(),
                    params: None,
                },
            ),
            F::Starting => start(false, "turn/start"),
            F::Settling => start(true, "turn/start"),
            _ => running(),
        };
        match case.fixture {
            F::Closing => machine.closing = true,
            F::Goal => machine.record("线程").goal_active = true,
            F::Awaiting => {
                machine.record("线程").goal_active = true;
                apply(&mut machine, turn("turn/completed", "前轮"));
            }
            F::Joined => {
                if let State::Running { joined, .. } =
                    &mut machine.claims.get_mut("线程").unwrap().state
                {
                    *joined = true;
                }
            }
            F::Ended => {
                machine.mark_ended("线程", "前轮");
            }
            _ => {}
        }
        if matches!(case.input, I::Resolved) {
            apply(
                &mut machine,
                Event::ServerRequest {
                    id: json!(7),
                    forward_id: "broker-1-000001".into(),
                    method: "item/commandExecution/requestApproval".into(),
                    params: Some(json!({"threadId": "线程"})),
                },
            );
        }
        let before = machine.claims.get("线程").cloned();
        let event = match case.input {
            I::Start => client(request(2, 77, "turn/start", json!({"threadId": "线程"}))),
            I::Started => turn("turn/started", "新轮"),
            I::Completed => turn("turn/completed", "本轮"),
            I::ForeignCompleted => turn("turn/completed", "前轮"),
            I::Paused => notify(
                "thread/goal/updated",
                json!({"threadId": "线程", "goal": {"status": "paused"}}),
            ),
            I::Disconnect => Event::Disconnected { conn: 1 },
            I::Interrupt => client(request(
                1,
                77,
                "turn/interrupt",
                json!({"threadId": "线程", "turnId": "本轮"}),
            )),
            I::Join => client(request(
                1,
                77,
                "broker/joined",
                json!({"threadId": "线程", "turnId": "外轮"}),
            )),
            I::ServerRequest => Event::ServerRequest {
                id: json!(7),
                forward_id: "broker-1-000001".into(),
                method: "审批".into(),
                params: Some(json!({"threadId": "线程"})),
            },
            I::Resolved => notify(
                "serverRequest/resolved",
                json!({"threadId": "线程", "requestId": "7"}),
            ),
            I::Failed => Event::Reply {
                context: context.clone(),
                result: Err(proto::error_object(-32000, "启动失败", None)),
            },
            I::Succeeded => Event::Reply {
                context: context.clone(),
                result: Ok(json!({"turn": {"id": "本轮"}})),
            },
            I::SharedSucceeded => Event::Reply {
                context: context.clone(),
                result: Ok(json!({})),
            },
            I::Retarget => Event::Reply {
                context: RequestContext {
                    resume_retried: false,
                    stream_id: None,
                    review_id: None,
                    client: Some(request(
                        1,
                        77,
                        "turn/interrupt",
                        json!({"threadId": "线程", "turnId": "旧轮"}),
                    )),
                    token: machine.token("线程"),
                    operation: Operation::Forward,
                    method: "turn/interrupt".into(),
                    params: Some(json!({"threadId": "线程", "turnId": "旧轮"})),
                },
                result: Err(proto::error_object(
                    -32000,
                    "expected active turn id 旧轮 but found 新轮",
                    None,
                )),
            },
            I::Stale => {
                let mut context = context.clone();
                context.token.as_mut().unwrap().claim_id += 1;
                Event::Reply {
                    context,
                    result: Ok(json!({"turn": {"id": "旧轮"}})),
                }
            }
        };
        let actions = apply(&mut machine, event);
        let claim = machine.claims.get("线程");
        match case.expected {
            N::Idle => assert!(claim.is_none(), "{}", case.row),
            N::Starting => assert!(
                matches!(claim.unwrap().state, State::Starting { .. }),
                "{}",
                case.row
            ),
            N::Held => assert!(
                matches!(&claim.unwrap().state, State::Starting { held, .. } if held.len() == 1),
                "{}",
                case.row
            ),
            N::HeldRequest => assert_eq!(claim.unwrap().held_requests.len(), 1, "{}", case.row),
            N::Resolved => assert!(claim.unwrap().held_requests.is_empty(), "{}", case.row),
            N::Running => assert!(
                matches!(claim.unwrap().state, State::Running { joined: false, .. }),
                "{}",
                case.row
            ),
            N::Joined => assert!(claim.unwrap().joined(), "{}", case.row),
            N::Awaiting => assert!(
                matches!(claim.unwrap().state, State::AwaitingContinuation { .. }),
                "{}",
                case.row
            ),
            N::Unchanged => assert_eq!(claim, before.as_ref(), "{}", case.row),
        }
        match case.effect {
            E::Error(code) => assert!(
                sends(&actions)
                    .iter()
                    .any(|(_, m)| m["error"]["code"] == code),
                "{}",
                case.row
            ),
            E::Resume => assert_eq!(
                first_request(&actions).operation,
                Operation::ResumeBarrier,
                "{}",
                case.row
            ),
            // 夹具里的 owner 和发起请求的连接都是 1：消息必须发到它，不能发给别的连接。
            E::Send => {
                let sent = sends(&actions);
                assert!(!sent.is_empty(), "{}", case.row);
                assert!(sent.iter().all(|(conn, _)| *conn == 1), "{}", case.row);
            }
            E::Buffer => assert!(actions.is_empty(), "{}", case.row),
            E::Release => assert!(
                actions.iter().any(|a| matches!(
                    a,
                    Action::Schedule {
                        kind: TimerKind::Unsubscribe,
                        ..
                    }
                )),
                "{}",
                case.row
            ),
            E::Drop => assert!(
                actions.iter().any(|a| matches!(a, Action::Debug(_))) && sends(&actions).is_empty(),
                "{}",
                case.row
            ),
            E::Retarget => assert!(
                matches!(
                    first_request(&actions).operation,
                    Operation::InterruptRetry { .. }
                ),
                "{}",
                case.row
            ),
            E::Dismiss => assert!(
                actions
                    .iter()
                    .any(|a| matches!(a, Action::Dismiss { id } if *id == json!("7"))),
                "{}",
                case.row
            ),
        }
    }
}

#[test]
fn 订阅屏障等待回包且恢复失败仍转发() {
    for ok in [true, false] {
        let mut machine = Machine::new(false);
        let actions = apply(
            &mut machine,
            client(request(1, 42, "turn/start", json!({"threadId": "线程"}))),
        );
        let resume = first_request(&actions);
        assert_eq!(
            resume.params,
            Some(json!({"threadId": "线程", "excludeTurns": true}))
        );
        assert!(machine.claims["线程"].pending_start.is_some());
        let result = if ok {
            Ok(json!({}))
        } else {
            Err(json!({"code": -32000}))
        };
        let actions = reply(&mut machine, &resume, result);
        assert_eq!(first_request(&actions).method, "turn/start");
        assert_eq!(machine.records["线程"].subscribed, ok);
        assert!(machine.claims["线程"].pending_start.is_none());
    }
}

#[test]
fn 同连接连续启动第二个请求立即返回忙() {
    let (mut machine, _) = start(false, "turn/start");
    let actions = apply(
        &mut machine,
        client(request(1, 99, "turn/start", json!({"threadId": "线程"}))),
    );
    assert_eq!(sends(&actions)[0].1["error"]["code"], -32001);
    assert!(machine.claims["线程"].request_pending);
}

#[test]
fn 完成先于响应且旧回包不改新认领() {
    let (mut machine, old) = start(false, "turn/start");
    apply(&mut machine, turn("turn/started", "旧轮"));
    apply(&mut machine, turn("turn/completed", "旧轮"));
    apply(
        &mut machine,
        client(request(2, 88, "turn/start", json!({"threadId": "线程"}))),
    );
    let new = machine.claims["线程"].clone();
    let actions = reply(&mut machine, &old, Ok(json!({"turn": {"id": "旧轮"}})));
    assert_eq!(machine.claims["线程"], new);
    assert!(sends(&actions)
        .iter()
        .any(|(conn, m)| *conn == 1 && m["id"] == 42));
}

#[test]
fn 启动先于响应时保留已学到的轮次() {
    let (mut machine, context) = start(false, "turn/start");
    apply(&mut machine, turn("turn/started", "当前轮"));
    reply(&mut machine, &context, Ok(json!({})));
    assert_eq!(machine.claims["线程"].turn_id(), Some("当前轮"));
    assert!(!machine.claims["线程"].request_pending);
}

#[test]
fn 首轮完成早于响应不会退出目标续跑空窗() {
    let (mut machine, context) = start(false, "turn/start");
    machine.record("线程").goal_active = true;
    apply(&mut machine, turn("turn/completed", "首轮"));
    reply(&mut machine, &context, Ok(json!({"turn": {"id": "首轮"}})));
    assert!(matches!(
        machine.claims["线程"].state,
        State::AwaitingContinuation { .. }
    ));
    assert!(!machine.claims["线程"].request_pending);
    apply(&mut machine, turn("turn/started", "续轮"));
    apply(&mut machine, turn("turn/completed", "首轮"));
    assert_eq!(machine.claims["线程"].turn_id(), Some("续轮"));
}

#[test]
fn 续跑空窗暂停目标后释放且旧回包不重建() {
    let (mut machine, context) = start(false, "turn/start");
    machine.record("线程").goal_active = true;
    apply(&mut machine, turn("turn/completed", "首轮"));
    apply(
        &mut machine,
        notify(
            "thread/goal/updated",
            json!({"threadId": "线程", "goal": {"status": "paused"}}),
        ),
    );
    reply(&mut machine, &context, Ok(json!({"turn": {"id": "首轮"}})));
    assert!(machine.claims.is_empty());
    assert!(!machine.records["线程"].goal_active);
}

#[test]
fn 无认领也从目标读回包学习状态() {
    let mut machine = Machine::new(false);
    let actions = apply(
        &mut machine,
        client(request(
            1,
            1,
            "thread/goal/get",
            json!({"threadId": "线程"}),
        )),
    );
    let context = first_request(&actions);
    reply(
        &mut machine,
        &context,
        Ok(json!({"goal": {"threadId": "线程", "status": "active"}})),
    );
    assert!(machine.records["线程"].goal_active);
    assert!(machine.claims.is_empty());
    let actions = apply(
        &mut machine,
        client(request(
            2,
            2,
            "thread/goal/clear",
            json!({"threadId": "线程"}),
        )),
    );
    // 请求发出时不学，回包成功后才学（上游 891–904 行）。
    assert!(machine.records["线程"].goal_active);
    reply(&mut machine, &first_request(&actions), Ok(json!({})));
    assert!(!machine.records["线程"].goal_active);
}

#[test]
fn 清除目标失败时续跑空窗保留认领() {
    let (mut machine, _) = running();
    machine.record("线程").goal_active = true;
    apply(&mut machine, turn("turn/completed", "本轮"));
    assert!(matches!(
        machine.claims["线程"].state,
        State::AwaitingContinuation { .. }
    ));
    let actions = apply(
        &mut machine,
        client(request(
            2,
            7,
            "thread/goal/clear",
            json!({"threadId": "线程"}),
        )),
    );
    assert!(machine.claims.contains_key("线程"));
    reply(
        &mut machine,
        &first_request(&actions),
        Err(json!({"code": -32000, "message": "清除失败"})),
    );
    // goal 仍是 active，下一轮续跑还要有人接收。
    assert!(machine.records["线程"].goal_active);
    assert!(matches!(
        machine.claims["线程"].state,
        State::AwaitingContinuation { .. }
    ));
}

#[test]
fn 恢复订阅期间断开则取消尚未发出的启动() {
    let mut machine = Machine::new(false);
    let actions = apply(
        &mut machine,
        client(request(1, 42, "turn/start", json!({"threadId": "线程"}))),
    );
    let resume = first_request(&actions);
    assert_eq!(resume.operation, Operation::ResumeBarrier);
    let mut actions = apply(&mut machine, Event::Disconnected { conn: 1 });
    assert!(machine.claims.is_empty());
    actions.extend(reply(&mut machine, &resume, Ok(json!({}))));
    // 不发出 turn/start，不给已断开的连接回包，认领释放。
    assert!(!actions
        .iter()
        .any(|a| matches!(a, Action::Request(c) if c.method == "turn/start")));
    assert!(sends(&actions).is_empty());
    assert!(machine.claims.is_empty());
    assert!(actions.iter().any(|a| matches!(
        a,
        Action::Schedule {
            kind: TimerKind::Unsubscribe,
            ..
        }
    )));
}

#[test]
fn 退订检查遇到新认领及再次释放都取消() {
    let (mut machine, _) = running();
    let old_token = machine.token("线程").unwrap();
    apply(&mut machine, turn("turn/completed", "本轮"));
    apply(
        &mut machine,
        client(request(2, 2, "turn/start", json!({"threadId": "线程"}))),
    );
    let actions = apply(
        &mut machine,
        Event::Timer {
            token: old_token.clone(),
            kind: TimerKind::Unsubscribe,
        },
    );
    assert!(actions.is_empty());
    apply(&mut machine, turn("turn/completed", "新轮"));
    let actions = apply(
        &mut machine,
        Event::Timer {
            token: old_token,
            kind: TimerKind::Unsubscribe,
        },
    );
    assert!(actions.is_empty());
    assert!(machine.records["线程"].subscribed);
}

#[test]
fn 退订只在无认领无活跃目标且代号匹配时发出() {
    for active in [true, false] {
        let (mut machine, _) = running();
        let token = machine.token("线程").unwrap();
        apply(&mut machine, turn("turn/completed", "本轮"));
        machine.record("线程").goal_active = active;
        let actions = apply(
            &mut machine,
            Event::Timer {
                token,
                kind: TimerKind::Unsubscribe,
            },
        );
        assert_eq!(
            actions
                .iter()
                .any(|a| matches!(a, Action::Request(c) if c.method == "thread/unsubscribe")),
            !active
        );
        assert_eq!(machine.records["线程"].subscribed, active);
    }
}

#[test]
fn 迟到退订回包不覆盖新认领的订阅事实() {
    let (mut machine, _) = running();
    let token = machine.token("线程").unwrap();
    apply(&mut machine, turn("turn/completed", "本轮"));
    let actions = apply(
        &mut machine,
        Event::Timer {
            token,
            kind: TimerKind::Unsubscribe,
        },
    );
    let old = first_request(&actions);
    machine.record("线程").subscribed = true;
    apply(
        &mut machine,
        client(request(1, 2, "turn/start", json!({"threadId": "线程"}))),
    );
    reply(&mut machine, &old, Ok(json!({})));
    assert!(machine.records["线程"].subscribed);
}

#[test]
fn 普通启动恢复成功才订阅且派生线程不订阅() {
    for method in ["thread/start", "thread/resume", "thread/fork"] {
        let mut machine = Machine::new(false);
        let actions = apply(
            &mut machine,
            client(request(1, 1, method, json!({"threadId": "线程"}))),
        );
        reply(
            &mut machine,
            &first_request(&actions),
            Ok(json!({"thread": {"id": "线程"}})),
        );
        assert_eq!(
            machine.records.get("线程").is_some_and(|r| r.subscribed),
            method != "thread/fork"
        );
    }
}

#[test]
fn 无认领的线程通知记账而不发送且全局通知广播() {
    let mut machine = Machine::new(false);
    let actions = apply(&mut machine, turn("turn/completed", "外轮"));
    assert!(sends(&actions).is_empty());
    assert_eq!(machine.records["线程"].ended_turns.len(), 1);
    let actions = apply(&mut machine, notify("未来全局通知", json!({"字段": 1})));
    assert!(sends(&actions).is_empty());
    assert!(matches!(
        actions.as_slice(),
        [Action::Broadcast { message }] if message["method"] == "未来全局通知"
    ));
}

#[test]
fn 非流式失败保留认领及原始错误内容() {
    let (mut machine, _) = running();
    let before = machine.claims["线程"].clone();
    let actions = apply(
        &mut machine,
        client(request(1, 3, "未知方法", json!({"threadId": "线程"}))),
    );
    let error = proto::error_object(-32601, "未知方法失败", Some(json!({"明细": 1})));
    let actions = reply(&mut machine, &first_request(&actions), Err(error.clone()));
    assert_eq!(machine.claims["线程"], before);
    assert_eq!(sends(&actions)[0].1["error"], error);
}

#[test]
fn 客户端加入只能修改自己的认领() {
    let (mut machine, _) = running();
    let actions = apply(
        &mut machine,
        client(request(
            2,
            3,
            "broker/joined",
            json!({"threadId": "线程", "turnId": "外轮"}),
        )),
    );
    assert_eq!(sends(&actions)[0].1["result"]["joined"], false);
    assert!(!machine.claims["线程"].joined());
}

#[test]
fn 非拥有者的换代中断失败不改投() {
    let (mut machine, _) = running();
    let actions = apply(
        &mut machine,
        client(request(
            2,
            3,
            "turn/interrupt",
            json!({"threadId": "线程", "turnId": "旧轮"}),
        )),
    );
    let actions = reply(
        &mut machine,
        &first_request(&actions),
        Err(proto::error_object(
            -32000,
            "expected active turn id 旧轮 but found 新轮",
            None,
        )),
    );
    assert!(!actions.iter().any(|a| matches!(a, Action::Request(_))));
    assert_eq!(sends(&actions)[0].1["error"]["code"], -32000);
}

#[test]
fn 拥有者改投失败仍返回原始换代错误() {
    let (mut machine, _) = running();
    let actions = apply(
        &mut machine,
        client(request(
            1,
            3,
            "turn/interrupt",
            json!({"threadId": "线程", "turnId": "旧轮"}),
        )),
    );
    let original = proto::error_object(-32000, "expected active turn id 旧轮 but found 新轮", None);
    let actions = reply(
        &mut machine,
        &first_request(&actions),
        Err(original.clone()),
    );
    let retry = first_request(&actions);
    let actions = reply(
        &mut machine,
        &retry,
        Err(proto::error_object(-32600, "改投失败", None)),
    );
    assert_eq!(sends(&actions)[0].1["error"], original);
}

#[test]
fn 断开产生停止操作且完成前保留认领() {
    let (mut machine, _) = running();
    let actions = apply(&mut machine, Event::Disconnected { conn: 1 });
    assert!(machine.claims["线程"].owner_gone);
    assert!(actions.iter().any(|a| matches!(a, Action::Request(c) if c.method == "turn/interrupt" && c.params.as_ref().unwrap()["turnId"] == "本轮")));
    machine.record("线程").goal_active = true;
    apply(&mut machine, turn("turn/completed", "本轮"));
    assert!(machine.claims.is_empty());
}

#[test]
fn 断开时轮次未知由迟到响应补发停止操作() {
    let (mut machine, context) = start(false, "turn/start");
    let actions = apply(&mut machine, Event::Disconnected { conn: 1 });
    assert!(actions
        .iter()
        .any(|a| matches!(a, Action::Request(c) if c.method == "thread/goal/get")));
    let actions = reply(&mut machine, &context, Ok(json!({"turn": {"id": "本轮"}})));
    assert!(actions
        .iter()
        .any(|a| matches!(a, Action::Request(c) if c.method == "turn/interrupt" && c.params.as_ref().unwrap()["turnId"] == "本轮")));
    assert!(machine.claims.contains_key("线程"));
}

#[test]
fn 启动失败但已宣布轮次时保留认领并发停止操作() {
    let (mut machine, context) = start(false, "turn/start");
    apply(&mut machine, turn("turn/started", "本轮"));
    let actions = reply(
        &mut machine,
        &context,
        Err(proto::error_object(-32000, "启动失败", None)),
    );
    assert!(machine.claims["线程"].owner_gone);
    assert!(actions.iter().any(|a| matches!(
        a,
        Action::Request(c) if c.method == "turn/interrupt"
    )));
}

#[test]
fn 托管拥有者不会因普通连接断开而停止且目标可续跑() {
    let (mut machine, _) = running();
    machine.claims.get_mut("线程").unwrap().state = State::Running {
        owner: Owner::Broker {
            policy: crate::policy::Policy::Never,
            approvals_reviewer: None,
            owned_at: 0,
            last_watcher_at: 0,
        },
        turn_id: "本轮".into(),
        joined: false,
    };
    machine.record("线程").goal_active = true;
    let actions = apply(&mut machine, Event::Disconnected { conn: 1 });
    assert!(actions.is_empty());
    apply(&mut machine, turn("turn/completed", "本轮"));
    assert!(matches!(
        machine.claims["线程"].state,
        State::AwaitingContinuation { .. }
    ));
}

#[test]
fn 关闭释放所有认领并清除缓冲() {
    let (mut machine, _) = start(true, "turn/start");
    apply(&mut machine, turn("turn/completed", "外轮"));
    apply(&mut machine, Event::Shutdown);
    assert!(machine.claims.is_empty());
    assert!(machine.closing);
}

#[test]
fn 每线程完成记录保留十六项() {
    let mut machine = Machine::new(false);
    for i in 0..20 {
        apply(&mut machine, turn("turn/completed", &format!("轮{i}")));
    }
    let turns = &machine.records["线程"].ended_turns;
    assert_eq!(turns.len(), 16);
    assert_eq!(turns.front().unwrap(), "轮4");
}

#[test]
fn 可淘汰历史上限不包含活动认领与待补全结果() {
    let (mut machine, _) = running();
    machine.record("待补全").result_pending = true;
    for i in 0..210 {
        apply(
            &mut machine,
            notify(
                "thread/goal/cleared",
                json!({"threadId": format!("历史{i}")}),
            ),
        );
    }
    assert!(machine.records.contains_key("线程"));
    assert!(machine.records.contains_key("待补全"));
    assert_eq!(machine.records.len(), 202);
    assert!(!machine.records.contains_key("历史0"));
}

#[test]
fn 完成记录的线程上限独立且按最近使用淘汰() {
    let (mut machine, _) = running();
    machine.mark_ended("线程", "首轮");
    for i in 0..200 {
        machine.mark_ended(&format!("线程{i}"), "轮");
    }
    assert!(machine.records["线程"].ended_turns.is_empty());
    machine.mark_ended("线程0", "新轮");
    machine.mark_ended("新线程", "轮");
    assert!(!machine.records["线程0"].ended_turns.is_empty());
    assert!(machine.records["线程1"].ended_turns.is_empty());
}

#[test]
fn 已宣布轮次后失败的加入请求只释放不中断() {
    let (mut machine, context) = start(false, "turn/steer");
    apply(&mut machine, turn("turn/started", "别人的轮"));
    let actions = reply(
        &mut machine,
        &context,
        Err(json!({"code": -32000, "message": "加入失败"})),
    );
    assert!(machine.claims.is_empty());
    assert!(!actions
        .iter()
        .any(|a| matches!(a, Action::Request(c) if c.method == "turn/interrupt")));
    assert_eq!(
        sends(&actions),
        vec![(
            1,
            json!({"id": 42, "error": {"code": -32000, "message": "加入失败"}})
        )]
    );
}

#[test]
fn 旧认领发出的清除目标成功后释放新认领的续跑空窗() {
    let (mut machine, _) = running();
    let clear = first_request(&apply(
        &mut machine,
        client(request(
            1,
            5,
            "thread/goal/clear",
            json!({"threadId": "线程"}),
        )),
    ));
    assert!(clear.token.is_some());
    apply(&mut machine, turn("turn/completed", "本轮"));
    assert!(machine.claims.is_empty());
    let second = first_request(&apply(
        &mut machine,
        client(request(2, 6, "turn/start", json!({"threadId": "线程"}))),
    ));
    machine.record("线程").goal_active = true;
    reply(&mut machine, &second, Ok(json!({"turn": {"id": "第二轮"}})));
    apply(&mut machine, turn("turn/completed", "第二轮"));
    assert!(matches!(
        machine.claims["线程"].state,
        State::AwaitingContinuation { .. }
    ));
    // 清除请求属于已释放的旧认领，但 goal 是线程级事实：接受后按 #17 释放空窗。
    let actions = reply(&mut machine, &clear, Ok(json!({})));
    assert!(!machine.records["线程"].goal_active);
    assert!(machine.claims.is_empty());
    assert_eq!(sends(&actions)[0], (1, json!({"id": 5, "result": {}})));
}

#[test]
fn 断开后同一轮次只发一次中断() {
    fn stops(actions: Vec<Action>) -> Vec<Option<String>> {
        actions
            .into_iter()
            .filter_map(|action| match action {
                Action::Request(c) if c.method == "turn/interrupt" => Some(
                    c.params.as_ref().unwrap()["turnId"]
                        .as_str()
                        .map(str::to_owned),
                ),
                _ => None,
            })
            .collect()
    }
    let (mut machine, context) = start(false, "turn/start");
    let mut seen = stops(apply(&mut machine, Event::Disconnected { conn: 1 }));
    seen.extend(stops(apply(&mut machine, turn("turn/started", "本轮"))));
    seen.extend(stops(reply(
        &mut machine,
        &context,
        Ok(json!({"turn": {"id": "本轮"}})),
    )));
    // 断开时还不知道轮次，先完成无目标的读取；学到「本轮」后只中断一次。
    assert_eq!(seen, vec![Some("本轮".to_string())]);
    // 换成新的一轮（例如上下文压缩换 turn）时要再中断一次。
    assert_eq!(
        stops(apply(&mut machine, turn("turn/started", "新一轮"))),
        vec![Some("新一轮".to_string())]
    );
}
