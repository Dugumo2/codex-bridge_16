//! 路由、回包、定案和撤销的纯逻辑验证（详细设计 1.2、3.3、3.4、4.1、4.2 节）。

use super::*;
use crate::claim::{self, Claim, ClientRequest, Event, ReqKind, State};
use serde_json::json;

const APPROVAL: &str = "item/commandExecution/requestApproval";
const METHODS: [&str; 7] = [
    APPROVAL,
    "item/fileChange/requestApproval",
    "item/tool/call",
    "item/tool/requestUserInput",
    "item/permissions/requestApproval",
    "mcpServer/elicitation/request",
    "未来/请求",
];

fn apply(machine: &mut Machine, event: Event) -> Vec<Action> {
    let (next, actions) = claim::step(std::mem::take(machine), event);
    *machine = next;
    actions
}

fn machine() -> Machine {
    let mut machine = Machine::new(false);
    machine.live_conns.extend([1, 2]);
    add_claim(&mut machine, "线程", 1, 1);
    machine
}

fn add_claim(machine: &mut Machine, thread: &str, conn: ConnId, claim_id: u64) {
    machine.claims.insert(
        thread.into(),
        Claim {
            stream_id: None,
            owned_ended: Default::default(),
            owned_turns: Default::default(),
            claim_id,
            state: State::Running {
                owner: Owner::Client(conn),
                turn_id: "本轮".into(),
                joined: false,
            },
            run_id: Some("运行".into()),
            resume_goal: false,
            request_pending: false,
            owner_gone: false,
            pending_start: None,
            submitted_input: Vec::new(),
            held_requests: Vec::new(),
            held_deltas: 0,
            held_bytes: 0,
            held_dropped_items: 0,
            overflowed: false,
            settlement_reply: None,
            pending_detach: None,
        },
    );
}

fn request(id: Value, forward_id: &str, method: &str, params: Option<Value>) -> Event {
    Event::ServerRequest {
        id,
        forward_id: forward_id.into(),
        method: method.into(),
        params,
    }
}

fn forward(machine: &mut Machine, id: u64, thread: &str) -> String {
    let forward_id = format!("broker-1-{id:06}");
    let actions = apply(
        machine,
        request(
            json!(id),
            &forward_id,
            APPROVAL,
            Some(json!({"threadId": thread})),
        ),
    );
    assert!(actions.iter().any(|a| matches!(
        a,
        Action::ScheduleForward {
            delay_ms: FORWARD_TIMEOUT_MS,
            ..
        }
    )));
    forward_id
}

fn response(id: &str, conn: ConnId, result: Option<Value>, error: Option<Value>) -> Event {
    Event::ForwardResponse {
        conn,
        id: json!(id),
        result,
        error,
    }
}

fn resolved(id: Value, thread: &str) -> Event {
    Event::Notification {
        method: "serverRequest/resolved".into(),
        params: Some(json!({"threadId": thread, "requestId": id})),
    }
}

fn upstream(actions: &[Action]) -> Vec<&Action> {
    actions
        .iter()
        .filter(|a| {
            matches!(
                a,
                Action::Dismiss { .. } | Action::ServerResult { .. } | Action::ServerError { .. }
            )
        })
        .collect()
}

fn assert_error(actions: &[Action], id: Value, message: &str) {
    assert_eq!(
        upstream(actions),
        vec![&Action::ServerError {
            id,
            error: handler_error(message)
        }]
    );
}

#[test]
fn 所有请求类按归属规则表驱动路由() {
    for method in METHODS {
        // 六种归属覆盖 4.2 第 1、3、5 条；每类都经过同一判定。
        for case in 0..6 {
            let mut machine = machine();
            match case {
                0 => {
                    machine.claims.clear();
                }
                1 => {
                    if let State::Running { joined, .. } =
                        &mut machine.claims.get_mut("线程").unwrap().state
                    {
                        *joined = true;
                    }
                }
                3 => machine.claims.get_mut("线程").unwrap().owner_gone = true,
                4 => {
                    machine.live_conns.remove(&1);
                }
                5 => machine.claims.get_mut("线程").unwrap().state = State::Idle,
                _ => {}
            }
            let params = Some(json!({"threadId": "线程", "turnId": "本轮", "保留": [null, 7]}));
            let actions = apply(
                &mut machine,
                request(json!(9), "broker-1-000001", method, params.clone()),
            );
            match case {
                0 | 1 => assert_eq!(
                    upstream(&actions),
                    vec![&Action::Dismiss { id: json!(9) }],
                    "{method}，归属 {case}"
                ),
                2 => {
                    assert_eq!(
                        actions[0],
                        Action::Send {
                            conn: 1,
                            message: proto::request(json!("broker-1-000001"), method, params)
                        }
                    );
                    assert_eq!(machine.requests.pending["broker-1-000001"].claim_id, 1);
                    assert_eq!(
                        actions[1],
                        Action::ScheduleForward {
                            forward_id: "broker-1-000001".into(),
                            generation: 1,
                            delay_ms: FORWARD_TIMEOUT_MS
                        }
                    );
                }
                _ if matches!(method, APPROVAL | "item/fileChange/requestApproval") => {
                    assert_error(
                        &actions,
                        json!(9),
                        "No active client to forward approval request",
                    )
                }
                // 4.1 动态工具：owner 不在线时上游不答（NO_RESPONSE），不是 -32601。
                _ if method == "item/tool/call" => assert_eq!(
                    upstream(&actions),
                    vec![&Action::Dismiss { id: json!(9) }],
                    "{method}，归属 {case}"
                ),
                _ => assert_eq!(
                    upstream(&actions),
                    vec![&Action::ServerError {
                        id: json!(9),
                        error: proto::error_object(
                            -32601,
                            format!("Method not found: {method}"),
                            None
                        )
                    }]
                ),
            }
        }
    }
}

#[test]
fn 全局请求保持共享静默和私有方法不存在() {
    for shared in [false, true] {
        for params in [None, Some(json!({})), Some(json!({"threadId": 7}))] {
            let mut machine = Machine::new(shared);
            let actions = apply(
                &mut machine,
                request(Value::Null, "broker-1-000001", "全局/请求", params),
            );
            let expected = if shared {
                Action::Dismiss { id: Value::Null }
            } else {
                Action::ServerError {
                    id: Value::Null,
                    error: proto::error_object(-32601, "Method not found: 全局/请求", None),
                }
            };
            assert_eq!(actions, vec![expected]);
            assert!(machine.requests.pending.is_empty());
        }
    }
}

#[test]
fn 托管动态工具回方法不存在而其他请求交给策略或收件箱() {
    for method in METHODS {
        let mut machine = machine();
        if let State::Running { owner, .. } = &mut machine.claims.get_mut("线程").unwrap().state {
            *owner = Owner::Broker {
                policy: crate::policy::Policy::Never,
                approvals_reviewer: None,
                owned_at: 0,
                last_watcher_at: 0,
            };
        }
        let actions = apply(
            &mut machine,
            request(
                json!(1),
                "broker-1-000001",
                method,
                Some(json!({"threadId": "线程"})),
            ),
        );
        if method == "item/tool/call" {
            assert_eq!(
                actions,
                vec![Action::ServerError {
                    id: json!(1),
                    error: proto::error_object(-32601, "Method not found: item/tool/call", None)
                }]
            );
        } else {
            match crate::policy::decide(
                crate::policy::Policy::Never,
                method,
                &json!({"threadId":"线程"}),
            ) {
                crate::policy::Decision::Answer(result) => {
                    assert!(actions.contains(&Action::ServerResult {
                        id: json!(1),
                        result
                    }))
                }
                crate::policy::Decision::Inbox => assert_eq!(
                    machine.inbox.hook_log.last().map(String::as_str),
                    Some("inbox_register_request")
                ),
                crate::policy::Decision::MethodNotFound => assert!(actions
                    .iter()
                    .any(|a| matches!(a, Action::ServerError { .. }))),
            }
        }
    }
}

#[test]
fn 非原连接回包不消费转发且原连接仍能作答() {
    let mut machine = machine();
    let id = forward(&mut machine, 7, "线程");
    let before = machine.requests.clone();
    let actions = apply(
        &mut machine,
        response(&id, 2, Some(json!({"越权": true})), None),
    );
    assert_eq!(before, machine.requests);
    assert!(matches!(actions.as_slice(), [Action::Warn(_)]));
    let actions = apply(
        &mut machine,
        response(&id, 1, Some(json!({"同意": true})), None),
    );
    assert_eq!(
        upstream(&actions),
        vec![&Action::ServerResult {
            id: json!(7),
            result: json!({"同意": true})
        }]
    );
    assert!(machine.requests.pending.is_empty());
}

#[test]
fn 回包字段表驱动覆盖结果优先空值和错误数据透传() {
    let error = proto::error_object(-12345, "原始错误", Some(json!({"详情": [1, null]})));
    for (result, err, expected) in [
        (
            Some(json!({"通过": true})),
            None,
            Action::ServerResult {
                id: json!(7),
                result: json!({"通过": true}),
            },
        ),
        (
            Some(Value::Null),
            Some(error.clone()),
            Action::ServerResult {
                id: json!(7),
                result: Value::Null,
            },
        ),
        (
            None,
            Some(error.clone()),
            Action::ServerError {
                id: json!(7),
                error,
            },
        ),
        (
            None,
            None,
            Action::ServerError {
                id: json!(7),
                error: handler_error(
                    "Malformed forwarded response: missing both 'result' and 'error'",
                ),
            },
        ),
    ] {
        let mut machine = machine();
        let id = forward(&mut machine, 7, "线程");
        let actions = apply(&mut machine, response(&id, 1, result, err));
        assert_eq!(upstream(&actions), vec![&expected]);
        assert!(machine.requests.pending.is_empty());
        assert!(
            matches!(&actions[0], Action::CancelForwardTimer { forward_id, generation: 1 } if forward_id == &id)
        );
    }
}

#[test]
fn 未知和重复回包只告警不回答服务器() {
    let mut machine = machine();
    let id = forward(&mut machine, 7, "线程");
    apply(&mut machine, response(&id, 1, Some(json!({})), None));
    for id in [id.as_str(), "未知编号"] {
        assert!(matches!(
            apply(&mut machine, response(id, 1, Some(json!({})), None)).as_slice(),
            [Action::Warn(_)]
        ));
    }
}

#[test]
fn 断开只撤销目标连接的所有线程转发() {
    let mut machine = machine();
    add_claim(&mut machine, "同连接线程", 1, 2);
    add_claim(&mut machine, "其他连接线程", 2, 3);
    forward(&mut machine, 7, "线程");
    forward(&mut machine, 8, "同连接线程");
    let other = forward(&mut machine, 9, "其他连接线程");
    let actions = apply(&mut machine, Event::Disconnected { conn: 1 });
    assert_eq!(machine.requests.pending.len(), 1);
    assert!(machine.requests.pending.contains_key(&other));
    for id in [7, 8] {
        assert!(upstream(&actions).contains(&&Action::ServerError {
            id: json!(id),
            error: handler_error(DISCONNECTED)
        }));
    }
    assert!(machine.claims["线程"].owner_gone);
    assert!(!machine.claims["其他连接线程"].owner_gone);
}

#[test]
fn 断开时已旁观的挂起转发静默撤销() {
    let mut machine = machine();
    forward(&mut machine, 7, "线程");
    if let State::Running { joined, .. } = &mut machine.claims.get_mut("线程").unwrap().state {
        *joined = true;
    }
    let actions = apply(&mut machine, Event::Disconnected { conn: 1 });
    assert_eq!(upstream(&actions), vec![&Action::Dismiss { id: json!(7) }]);
    assert!(machine.requests.pending.is_empty());
    assert!(machine.claims.is_empty());
}

#[test]
fn 超时仅结算匹配代号且重复超时没有副作用() {
    let mut machine = machine();
    let id = forward(&mut machine, 7, "线程");
    assert!(apply(
        &mut machine,
        Event::ForwardTimeout {
            forward_id: id.clone(),
            generation: 0
        }
    )
    .is_empty());
    let actions = apply(
        &mut machine,
        Event::ForwardTimeout {
            forward_id: id.clone(),
            generation: 1,
        },
    );
    assert_error(&actions, json!(7), TIMEOUT);
    assert!(apply(
        &mut machine,
        Event::ForwardTimeout {
            forward_id: id.clone(),
            generation: 1
        }
    )
    .is_empty());
    assert!(matches!(
        apply(&mut machine, response(&id, 1, Some(json!({})), None)).as_slice(),
        [Action::Warn(_)]
    ));
}

#[test]
fn 旧计时器不能结算复用转发编号的新请求() {
    let mut machine = machine();
    let id = forward(&mut machine, 7, "线程");
    apply(&mut machine, response(&id, 1, Some(json!({})), None));
    apply(
        &mut machine,
        request(json!(8), &id, APPROVAL, Some(json!({"threadId": "线程"}))),
    );
    assert!(apply(
        &mut machine,
        Event::ForwardTimeout {
            forward_id: id.clone(),
            generation: 1
        }
    )
    .is_empty());
    assert_eq!(machine.requests.pending[&id].generation, 2);
    let actions = apply(
        &mut machine,
        Event::ForwardTimeout {
            forward_id: id,
            generation: 2,
        },
    );
    assert_error(&actions, json!(8), TIMEOUT);
}

#[test]
fn 关闭拒绝所有连接转发且清理计时器与认领() {
    let mut machine = machine();
    add_claim(&mut machine, "其他线程", 2, 2);
    forward(&mut machine, 7, "线程");
    forward(&mut machine, 8, "其他线程");
    let actions = apply(&mut machine, Event::Shutdown);
    assert_eq!(upstream(&actions).len(), 2);
    for id in [7, 8] {
        assert!(upstream(&actions).contains(&&Action::ServerError {
            id: json!(id),
            error: handler_error(SHUTDOWN)
        }));
    }
    assert!(machine.requests.pending.is_empty());
    assert!(machine.claims.is_empty());
    assert!(apply(&mut machine, Event::Shutdown).is_empty());
}

#[test]
fn 先外部解决后本地回包只静默退役一次() {
    let mut machine = machine();
    let id = forward(&mut machine, 7, "线程");
    let actions = apply(&mut machine, resolved(json!("7"), "线程"));
    assert_eq!(
        upstream(&actions),
        vec![&Action::Dismiss { id: json!("7") }]
    );
    let actions = apply(&mut machine, response(&id, 1, Some(json!({})), None));
    assert!(matches!(actions.as_slice(), [Action::Warn(_)]));
    assert!(machine.requests.pending.is_empty());
}

#[test]
fn 先本地回包后外部解决不会再次回答() {
    let mut machine = machine();
    let id = forward(&mut machine, 7, "线程");
    let actions = apply(&mut machine, response(&id, 1, Some(json!({})), None));
    assert_eq!(
        upstream(&actions),
        vec![&Action::ServerResult {
            id: json!(7),
            result: json!({})
        }]
    );
    let actions = apply(&mut machine, resolved(json!(7), "线程"));
    assert_eq!(upstream(&actions), vec![&Action::Dismiss { id: json!(7) }]);
}

#[test]
fn 外部解决按线程和原请求编号选择且不碰其他转发() {
    let mut machine = machine();
    let one = forward(&mut machine, 7, "线程");
    let two = forward(&mut machine, 8, "线程");
    apply(&mut machine, resolved(json!(7), "其他线程"));
    assert!(machine.requests.pending.contains_key(&one));
    apply(&mut machine, resolved(json!(7), "线程"));
    assert!(!machine.requests.pending.contains_key(&one));
    assert!(machine.requests.pending.contains_key(&two));
}

#[test]
fn 旁观定案只退役本连接本线程的转发() {
    let mut machine = machine();
    add_claim(&mut machine, "其他线程", 1, 2);
    let one = forward(&mut machine, 7, "线程");
    let two = forward(&mut machine, 8, "其他线程");
    let actions = apply(
        &mut machine,
        Event::ClientRequest {
            request: ClientRequest {
                conn: 1,
                id: json!(99),
                method: "broker/joined".into(),
                params: Some(json!({"threadId": "线程", "turnId": "外轮"})),
            },
            run_id: None,
        },
    );
    assert_eq!(upstream(&actions), vec![&Action::Dismiss { id: json!(7) }]);
    assert!(!machine.requests.pending.contains_key(&one));
    assert!(machine.requests.pending.contains_key(&two));
    assert!(machine.claims["线程"].joined());
}

#[test]
fn 非原连接不能宣告旁观或撤销转发() {
    let mut machine = machine();
    let id = forward(&mut machine, 7, "线程");
    let actions = apply(
        &mut machine,
        Event::ClientRequest {
            request: ClientRequest {
                conn: 2,
                id: json!(99),
                method: "broker/joined".into(),
                params: Some(json!({"threadId": "线程", "turnId": "外轮"})),
            },
            run_id: None,
        },
    );
    assert!(upstream(&actions).is_empty());
    assert!(machine.requests.pending.contains_key(&id));
    assert!(!machine.claims["线程"].joined());
}

#[test]
fn 释放后转发仍保留且原连接仍可作答() {
    // 上游 releaseThread 不撤销挂起的转发（broker-server.ts 788–799 行）：app-server 还在等，
    // owner 迟到的回答照常送达，计时器也保留作兜底。
    let mut machine = machine();
    let id = forward(&mut machine, 7, "线程");
    let actions = apply(
        &mut machine,
        Event::Notification {
            method: "turn/completed".into(),
            params: Some(json!({"threadId": "线程", "turn": {"id": "本轮"}})),
        },
    );
    assert!(upstream(&actions).is_empty());
    assert!(!actions
        .iter()
        .any(|a| matches!(a, Action::CancelForwardTimer { .. })));
    assert!(machine.claims.is_empty());
    assert!(machine.requests.pending.contains_key(&id));
    let actions = apply(
        &mut machine,
        response(&id, 1, Some(json!({"同意": true})), None),
    );
    assert_eq!(
        upstream(&actions),
        vec![&Action::ServerResult {
            id: json!(7),
            result: json!({"同意": true}),
        }]
    );
    assert!(machine.requests.pending.is_empty());
}

#[test]
fn 启动失败释放认领后人工请求仍等待并可回答() {
    // astra 指出的路径：start 尚未宣布 turn 就先来了人工请求，随后 start 失败（#10）释放认领。
    let mut machine = Machine::new(false);
    machine.live_conns.insert(1);
    machine.records.entry("线程".into()).or_default().subscribed = true;
    let start = apply(
        &mut machine,
        Event::ClientRequest {
            request: ClientRequest {
                conn: 1,
                id: json!(42),
                method: "turn/start".into(),
                params: Some(json!({"threadId": "线程"})),
            },
            run_id: None,
        },
    );
    let context = start
        .iter()
        .find_map(|a| match a {
            Action::Request(c) => Some(c.clone()),
            _ => None,
        })
        .expect("流式请求");
    let forwarded = apply(
        &mut machine,
        request(
            json!(7),
            "broker-1-000007",
            APPROVAL,
            Some(json!({"threadId": "线程"})),
        ),
    );
    assert!(matches!(forwarded[0], Action::Send { conn: 1, .. }));
    let failed = apply(
        &mut machine,
        Event::Reply {
            context,
            result: Err(json!({"code": -32000, "message": "启动失败"})),
        },
    );
    assert!(machine.claims.is_empty());
    assert!(upstream(&failed).is_empty(), "释放不应替 owner 结算请求");
    assert!(machine.requests.pending.contains_key("broker-1-000007"));
    let answered = apply(
        &mut machine,
        response(
            "broker-1-000007",
            1,
            Some(json!({"decision": "accept"})),
            None,
        ),
    );
    assert_eq!(
        upstream(&answered),
        vec![&Action::ServerResult {
            id: json!(7),
            result: json!({"decision": "accept"}),
        }]
    );
}

/// 用原始 JSON id（可能是浮点数字）登记一个审批转发，返回转发 id。
fn forward_raw(machine: &mut Machine, id: Value, thread: &str) -> String {
    let forward_id = "broker-1-0000ff".to_owned();
    apply(
        machine,
        request(id, &forward_id, APPROVAL, Some(json!({"threadId": thread}))),
    );
    assert!(machine.requests.pending.contains_key(&forward_id));
    forward_id
}

#[test]
fn 外部解决按上游数字编号规则匹配挂起转发与挂起请求() {
    // 与 rpc 层去重同一规则（JS String(id)）：7.0 与 "7"、1e21 与 "1e+21"、-0 与 "0" 都是同一请求。
    for (raw, resolved) in [
        (json!(7.0), json!("7")),
        (json!(1e21), json!("1e+21")),
        (json!(-0.0), json!(0)),
    ] {
        let mut machine = machine();
        let id = forward_raw(&mut machine, raw.clone(), "线程");
        let actions = apply(
            &mut machine,
            Event::Notification {
                method: "serverRequest/resolved".into(),
                params: Some(json!({"threadId": "线程", "requestId": resolved})),
            },
        );
        assert!(!machine.requests.pending.contains_key(&id), "{raw}");
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::CancelForwardTimer { forward_id, .. } if *forward_id == id
        )));

        let mut machine = settling();
        apply(
            &mut machine,
            request(
                raw.clone(),
                "broker-1-000009",
                APPROVAL,
                Some(json!({"threadId": "线程"})),
            ),
        );
        assert_eq!(machine.claims["线程"].held_requests.len(), 1);
        apply(
            &mut machine,
            Event::Notification {
                method: "serverRequest/resolved".into(),
                params: Some(json!({"threadId": "线程", "requestId": resolved})),
            },
        );
        assert!(machine.claims["线程"].held_requests.is_empty(), "{raw}");
    }
}

#[test]
fn 动态工具按上游专门规则路由() {
    let call = |id: u64| {
        request(
            json!(id),
            &format!("broker-1-{id:06}"),
            "item/tool/call",
            Some(json!({"threadId": "线程", "tool": "工具"})),
        )
    };
    // 私有服务器、owner 在线：转发。
    let mut private = machine();
    assert!(matches!(
        apply(&mut private, call(1))[0],
        Action::Send { conn: 1, .. }
    ));
    // 共享服务器上已定案的认领：不答（上游 721 行）。
    let mut shared = machine();
    shared.shared = true;
    assert_eq!(
        upstream(&apply(&mut shared, call(2))),
        vec![&Action::Dismiss { id: json!(2) }]
    );
    // 共享服务器 settling 期间挂起，定案为自己的之后回放：owner 在线就转发（上游 713–719 行）。
    let mut held = settling();
    apply(&mut held, call(3));
    let actions = settle(&mut held, false);
    assert!(actions.iter().any(
        |a| matches!(a, Action::Send { conn: 1, message } if message["method"] == "item/tool/call")
    ));
    assert!(held.requests.pending.contains_key("broker-1-000003"));
}

fn settling() -> Machine {
    let mut machine = machine();
    machine.shared = true;
    machine.claims.get_mut("线程").unwrap().state = State::Starting {
        owner: Owner::Client(1),
        req: ReqKind::TurnStart,
        turn_id: Some("本轮".into()),
        cont: None,
        settling: true,
        undecided: None,
        submitted_at: 0,
        held: Vec::new(),
    };
    machine
}

/// 经 H2 状态读取层驱动吸收判定：线程活动、第一个活动轮次是本轮（自有）或外轮（加入）。
fn settle(machine: &mut Machine, joined: bool) -> Vec<Action> {
    use crate::test_support::status_read::{
        drive_status, expect, meta, turns_page, StatusPhase, StatusStep,
    };
    let mut actions = Vec::new();
    machine.begin_settlement("线程", 0, &mut actions);
    let active = if joined { "外轮" } else { "本轮" };
    let (driven, _) = drive_status(
        machine,
        vec![
            StatusStep::Reply(expect(StatusPhase::Metadata), Ok(meta("线程", "active"))),
            StatusStep::Reply(
                expect(StatusPhase::Turns),
                Ok(turns_page(&[(active, "inProgress")], None)),
            ),
        ],
    );
    actions.extend(driven);
    actions
}

#[test]
fn 待定请求按自有和旁观定案分别转发或静默() {
    for joined in [false, true] {
        for method in METHODS {
            let mut machine = settling();
            let actions = apply(
                &mut machine,
                request(
                    json!(7),
                    "broker-1-000007",
                    method,
                    Some(json!({"threadId": "线程", "turnId": "本轮"})),
                ),
            );
            assert!(actions.is_empty());
            assert_eq!(machine.claims["线程"].held_requests.len(), 1);
            let actions = settle(&mut machine, joined);
            assert!(machine.claims["线程"].held_requests.is_empty());
            if joined {
                assert_eq!(upstream(&actions), vec![&Action::Dismiss { id: json!(7) }]);
                assert!(machine.requests.pending.is_empty());
            } else {
                assert!(actions.iter().any(|a| matches!(a, Action::Send { conn: 1, message } if message["method"] == method)));
                assert_eq!(machine.requests.pending.len(), 1);
            }
        }
    }
}

#[test]
fn 自有定案仅转发本轮和没有轮次的挂起请求() {
    let mut machine = settling();
    for (id, turn) in [(7, Some("本轮")), (8, None), (9, Some("前轮"))] {
        let mut params = json!({"threadId": "线程"});
        if let Some(turn) = turn {
            params["turnId"] = json!(turn);
        }
        apply(
            &mut machine,
            request(
                json!(id),
                &format!("broker-1-{id:06}"),
                APPROVAL,
                Some(params),
            ),
        );
    }
    let actions = settle(&mut machine, false);
    assert_eq!(machine.requests.pending.len(), 2);
    assert_eq!(upstream(&actions), vec![&Action::Dismiss { id: json!(9) }]);
}

#[test]
fn 待定时已被外部解决的请求不再回放() {
    let mut machine = settling();
    apply(
        &mut machine,
        request(
            json!(7),
            "broker-1-000007",
            APPROVAL,
            Some(json!({"threadId": "线程"})),
        ),
    );
    apply(&mut machine, resolved(json!("7"), "线程"));
    assert!(machine.claims["线程"].held_requests.is_empty());
    let actions = settle(&mut machine, false);
    assert!(machine.requests.pending.is_empty());
    assert!(!actions
        .iter()
        .any(|a| matches!(a, Action::ScheduleForward { .. })));
}

#[test]
fn 断开后定案自有按类别拒绝而旁观静默() {
    for joined in [false, true] {
        for method in METHODS {
            let mut machine = settling();
            apply(
                &mut machine,
                request(
                    json!(7),
                    "broker-1-000007",
                    method,
                    Some(json!({"threadId": "线程"})),
                ),
            );
            apply(&mut machine, Event::Disconnected { conn: 1 });
            let actions = settle(&mut machine, joined);
            assert!(machine.requests.pending.is_empty());
            if joined {
                assert_eq!(upstream(&actions), vec![&Action::Dismiss { id: json!(7) }]);
                assert!(machine.claims.is_empty());
            } else if matches!(method, APPROVAL | "item/fileChange/requestApproval") {
                assert_error(
                    &actions,
                    json!(7),
                    "No active client to forward approval request",
                );
            } else if method == "item/tool/call" {
                assert_eq!(upstream(&actions), vec![&Action::Dismiss { id: json!(7) }]);
            } else {
                assert_eq!(
                    upstream(&actions),
                    vec![&Action::ServerError {
                        id: json!(7),
                        error: proto::error_object(
                            -32601,
                            format!("Method not found: {method}"),
                            None
                        )
                    }]
                );
            }
        }
    }
}

#[test]
fn 关闭时尚未定案的请求静默清理且迟到定案不转发() {
    use crate::test_support::status_read::{meta, reply_status, status_requests};
    let mut machine = settling();
    let mut started = Vec::new();
    machine.begin_settlement("线程", 0, &mut started);
    let read = status_requests(&started).remove(0);
    apply(
        &mut machine,
        request(
            json!(7),
            "broker-1-000007",
            APPROVAL,
            Some(json!({"threadId": "线程"})),
        ),
    );
    let actions = apply(&mut machine, Event::Shutdown);
    assert_eq!(upstream(&actions), vec![&Action::Dismiss { id: json!(7) }]);
    assert!(machine.claims.is_empty());
    // 关闭时在途的吸收判定读取被静默取消；迟到回包不得定案或转发。
    assert!(
        reply_status(&mut machine, &read, Ok(meta("线程", "active")))
            .iter()
            .all(|a| !matches!(a, Action::Send { .. } | Action::ScheduleForward { .. }))
    );
}

#[test]
fn 定案回放完成时清理挂起请求而不发送过时对话() {
    let mut machine = settling();
    apply(
        &mut machine,
        request(
            json!(7),
            "broker-1-000007",
            APPROVAL,
            Some(json!({"threadId": "线程", "turnId": "本轮"})),
        ),
    );
    apply(
        &mut machine,
        Event::Notification {
            method: "turn/completed".into(),
            params: Some(json!({"threadId": "线程", "turn": {"id": "本轮"}})),
        },
    );
    let actions = settle(&mut machine, false);
    assert_eq!(upstream(&actions), vec![&Action::Dismiss { id: json!(7) }]);
    assert!(machine.requests.pending.is_empty());
    assert!(machine.claims.is_empty());
    assert!(!actions
        .iter()
        .any(|a| matches!(a, Action::ScheduleForward { .. })));
}

#[test]
fn 客户端错误按上游规范化后转给服务器() {
    for (raw, expected) in [
        // 完整的错误原样透传，data 保留。
        (
            json!({"code": -32000, "message": "拒绝", "data": {"原因": 1}}),
            json!({"code": -32000, "message": "拒绝", "data": {"原因": 1}}),
        ),
        // 缺 code 用 -32000，缺 message 用 Client error。
        (
            json!({}),
            json!({"code": -32000, "message": "Client error"}),
        ),
        // message 只在缺失或为 null 时用 Client error，其他类型按 JS String() 转换。
        (
            json!({"code": "坏", "message": 5}),
            json!({"code": -32000, "message": "5"}),
        ),
        (
            json!({"code": -32001, "message": null}),
            json!({"code": -32001, "message": "Client error"}),
        ),
        (
            json!({"code": -32001, "message": [1, null, "a"]}),
            json!({"code": -32001, "message": "1,,a"}),
        ),
        (
            json!({"code": -32001, "message": {"a": 1}}),
            json!({"code": -32001, "message": "[object Object]"}),
        ),
        // 非整数、越界（2^63 本身已越界）的 code 按 -32000；布尔 message 按 JS String()。
        (
            json!({"code": -32000.5, "message": true}),
            json!({"code": -32000, "message": "true"}),
        ),
        (
            json!({"code": 9.223_372_036_854_776e18, "message": "越界"}),
            json!({"code": -32000, "message": "越界"}),
        ),
        // 整数值的浮点 code 照常使用，-32603.0 同样加前缀；data 为 null 也保留。
        (
            json!({"code": -32603.0, "message": "内部", "data": null}),
            json!({"code": -32603, "message": "Handler error: 内部", "data": null}),
        ),
        // -32603 与上游一样加 Handler error 前缀。
        (
            json!({"code": -32603, "message": "内部错误"}),
            json!({"code": -32603, "message": "Handler error: 内部错误"}),
        ),
    ] {
        let mut machine = machine();
        let forward_id = forward(&mut machine, 7, "线程");
        let actions = apply(
            &mut machine,
            Event::ForwardResponse {
                conn: 1,
                id: json!(forward_id),
                result: None,
                error: Some(raw),
            },
        );
        assert_eq!(
            upstream(&actions),
            vec![&Action::ServerError {
                id: json!(7),
                error: expected,
            }]
        );
    }
}

#[test]
fn 线程换给别的连接旁观后旧转发在原连接断开时照常拒绝() {
    // A（连接 1）认领时转发的审批，A 的认领释放后，B（连接 2）在同线程以 joined 认领。
    // A 断开时不能拿 B 的 joined 来静默处置 A 的请求，app-server 还在等它。
    let mut machine = machine();
    forward(&mut machine, 7, "线程");
    apply(
        &mut machine,
        Event::Notification {
            method: "turn/completed".into(),
            params: Some(json!({"threadId": "线程", "turn": {"id": "本轮"}})),
        },
    );
    add_claim(&mut machine, "线程", 2, 9);
    if let State::Running { joined, .. } = &mut machine.claims.get_mut("线程").unwrap().state {
        *joined = true;
    }
    let actions = apply(&mut machine, Event::Disconnected { conn: 1 });
    assert_eq!(
        upstream(&actions),
        vec![&Action::ServerError {
            id: json!(7),
            error: handler_error(DISCONNECTED),
        }]
    );
}

#[test]
fn 同一连接换代后定案旁观不撤销旧认领留下的转发() {
    let mut machine = machine();
    let old = forward(&mut machine, 7, "线程");
    apply(
        &mut machine,
        Event::Notification {
            method: "turn/completed".into(),
            params: Some(json!({"threadId": "线程", "turn": {"id": "本轮"}})),
        },
    );
    // 同一连接重新认领（新代号），随后声明只是旁观。
    add_claim(&mut machine, "线程", 1, 9);
    let actions = apply(
        &mut machine,
        Event::ClientRequest {
            request: ClientRequest {
                conn: 1,
                id: json!(50),
                method: "broker/joined".into(),
                params: Some(json!({"threadId": "线程", "turnId": "别人的轮"})),
            },
            run_id: None,
        },
    );
    assert!(machine.claims["线程"].joined());
    assert!(upstream(&actions).is_empty(), "旧认领的转发不能被静默撤销");
    assert!(machine.requests.pending.contains_key(&old));
    // owner 仍可回答旧请求。
    let answered = apply(
        &mut machine,
        response(&old, 1, Some(json!({"同意": true})), None),
    );
    assert_eq!(
        upstream(&answered),
        vec![&Action::ServerResult {
            id: json!(7),
            result: json!({"同意": true}),
        }]
    );
}
