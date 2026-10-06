//! 生命周期、边界、配额与重启契约；只推进注入的时钟，不使用固定睡眠。
use super::*;
use crate::claim::{Claim, Owner, State, ThreadRecord};
use std::collections::{HashSet, VecDeque};

fn machine() -> Machine {
    let mut m = Machine::new(false);
    m.clock.now_ms = 100;
    m.clock.entropy = 42;
    m.workspace_key = "工作区".into();
    m
}
fn own(m: &mut Machine, thread: &str) {
    m.claims.insert(
        thread.into(),
        Claim {
            stream_id: None,
            owned_ended: VecDeque::new(),
            owned_turns: HashSet::new(),
            claim_id: 7,
            state: State::Running {
                owner: Owner::Client(1),
                turn_id: "turn".into(),
                joined: false,
            },
            run_id: Some("run".into()),
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
    m.live_conns.insert(1);
}
fn register(
    m: &mut Machine,
    method: &str,
    params: Value,
    app_id: Value,
    actions: &mut Vec<Action>,
) -> String {
    m.inbox_register_request(
        InboxOrigin {
            thread_id: "t".into(),
            claim_id: 7,
            run_id: Some("run".into()),
            turn_id: Some("turn".into()),
        },
        HeldRequest {
            id: app_id,
            forward_id: "旧转发".into(),
            method: method.into(),
            params: Some(params),
        },
        None,
        actions,
    );
    m.inbox
        .entries
        .values()
        .max_by_key(|e| e.generation)
        .unwrap()
        .item
        .id
        .clone()
}
fn request(m: &mut Machine, conn: u64, rid: u64, method: &str, params: Value) -> Vec<Action> {
    let mut actions = Vec::new();
    m.inbox_request(
        ClientRequest {
            conn,
            id: json!(rid),
            method: format!("broker/inbox/{method}"),
            params: Some(params),
        },
        &mut actions,
    );
    actions
}
fn resolve(m: &mut Machine, rid: u64, id: &str, answer: Value) -> Vec<Action> {
    request(m, 1, rid, "resolve", json!({"id":id,"answer":answer}))
}
fn response(actions: &[Action], rid: u64) -> Value {
    actions
        .iter()
        .find_map(|a| match a {
            Action::Send { message, .. } if message["id"] == rid => Some(message.clone()),
            _ => None,
        })
        .unwrap()
}
fn op(actions: &[Action]) -> u64 {
    actions
        .iter()
        .find_map(|a| match a {
            Action::AppReplyAcked { op, .. } => Some(*op),
            Action::Request(r) => match r.operation {
                Operation::Inbox(
                    InboxRpc::GuardianResume { op }
                    | InboxRpc::GuardianResumeFor { op, .. }
                    | InboxRpc::GuardianOverride { op },
                ) => Some(op),
                _ => None,
            },
            _ => None,
        })
        .unwrap()
}
fn expire(m: &mut Machine, id: &str) -> Vec<Action> {
    let e = &m.inbox.entries[id];
    let (token, generation) = (e.token(), e.generation);
    let mut actions = Vec::new();
    m.inbox_timer(
        token,
        InboxTimer::Expiry {
            id: id.into(),
            generation,
        },
        &mut actions,
    );
    actions
}
fn guardian(m: &mut Machine, review: &str, status: Value, actions: &mut Vec<Action>) -> String {
    m.inbox_guardian_review(
        "t",
        &Some(json!({"reviewId":review,"review":{"status":status},"action":{"type":"command"}})),
        actions,
    );
    format!("gd_{}", digest(&format!("{}{}", m.workspace_key, review)))
}

#[test]
fn 登记类型编号门控原始请求与截止时刻() {
    let mut m = machine();
    own(&mut m, "t");
    m.cxb_conns.extend([1, 2]);
    m.records.insert(
        "t".into(),
        ThreadRecord {
            watchers: vec![1, 2, 3],
            ..Default::default()
        },
    );
    for (i, (method, prefix, kind)) in [
        (
            "item/commandExecution/requestApproval",
            "ap",
            InboxKind::Approval,
        ),
        ("item/fileChange/requestApproval", "ap", InboxKind::Approval),
        (
            "item/permissions/requestApproval",
            "ap",
            InboxKind::Approval,
        ),
        ("item/tool/requestUserInput", "qu", InboxKind::Question),
        ("未来方法", "qu", InboxKind::Question),
        ("mcpServer/elicitation/request", "mc", InboxKind::Mcp),
    ]
    .into_iter()
    .enumerate()
    {
        let mut actions = Vec::new();
        let id = register(&mut m, method, json!({"原始":true}), json!(i), &mut actions);
        let entry = &m.inbox.entries[&id];
        assert!(id.starts_with(&format!("{prefix}_")));
        assert_eq!(entry.item.kind, kind);
        assert!((8..=64).contains(&id[3..].len()));
        assert!(id[3..]
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-'));
        assert_eq!(entry.item.expires_at, 100 + REQUEST_EXPIRY_MS);
        assert_eq!(entry.app_id, Some(json!(i)));
        assert!(actions
            .iter()
            .any(|a| matches!(a, Action::Store(StoreOp::PutInbox(_)))));
        let mut conns: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Send { conn, .. } => Some(*conn),
                _ => None,
            })
            .collect();
        conns.sort();
        assert_eq!(conns, vec![1, 2]);
    }
    let id = register(
        &mut m,
        "mcpServer/elicitation/request",
        json!({"mode":"url","elicitationId":"e1"}),
        json!(99),
        &mut Vec::new(),
    );
    assert_eq!(id, format!("mc_{}", digest("e1")));
    let count = m.inbox.entries.len();
    register(&mut m, "未来方法", json!({}), json!(99.0), &mut Vec::new());
    assert_eq!(m.inbox.entries.len(), count);
    let mut actions = Vec::new();
    m.inbox_register_request(
        InboxOrigin {
            thread_id: "t".into(),
            claim_id: 7,
            run_id: None,
            turn_id: None,
        },
        HeldRequest {
            id: json!(100),
            forward_id: "f".into(),
            method: "未来方法".into(),
            params: None,
        },
        Some(123),
        &mut actions,
    );
    assert!(actions
        .iter()
        .any(|a| matches!(a, Action::Schedule { delay_ms: 23, .. })));
}

#[test]
fn 审批与权限编码及原样答案() {
    for method in [
        "item/commandExecution/requestApproval",
        "item/fileChange/requestApproval",
        "item/permissions/requestApproval",
    ] {
        for decision in ["accept", "acceptForSession", "decline", "cancel"] {
            let mut m = machine();
            let id = register(
                &mut m,
                method,
                json!({"permissions":{"read":true}}),
                json!(8),
                &mut Vec::new(),
            );
            let bad = resolve(&mut m, 1, &id, json!({"decision":"错误"}));
            assert_eq!(response(&bad, 1)["error"]["code"], -32602);
            let actions = resolve(&mut m, 2, &id, json!({"decision":decision}));
            let result = actions
                .iter()
                .find_map(|a| match a {
                    Action::AppReplyAcked {
                        id, payload: Ok(p), ..
                    } => {
                        assert_eq!(id, &json!(8));
                        Some(p)
                    }
                    _ => None,
                })
                .unwrap();
            let expected = if method == "item/permissions/requestApproval" {
                json!({"scope":if decision=="acceptForSession" {"session"} else {"turn"},"permissions":if ["accept","acceptForSession"].contains(&decision) {json!({"read":true})} else {json!({})}})
            } else {
                json!({"decision":decision})
            };
            assert_eq!(result, &expected);
        }
    }
    for method in [
        "item/tool/requestUserInput",
        "mcpServer/elicitation/request",
        "未来方法",
    ] {
        let mut m = machine();
        let id = register(&mut m, method, json!({}), json!(1), &mut Vec::new());
        let answer = json!([null,{"任意":"原样"}]);
        let actions = resolve(&mut m, 1, &id, answer.clone());
        assert!(actions
            .iter()
            .any(|a| matches!(a,Action::AppReplyAcked { payload:Ok(v),.. } if v==&answer)));
    }
}

#[test]
fn resolve_合并恰好一次与全部终态错误() {
    let mut m = machine();
    let id = register(&mut m, "未来方法", json!({}), json!(7), &mut Vec::new());
    let a = resolve(&mut m, 1, &id, json!({"a":1}));
    let operation = op(&a);
    assert_eq!(m.inbox.entries[&id].item.status, InboxStatus::Resolving);
    assert!(!a.iter().any(|a| matches!(a, Action::Send { .. })));
    assert!(resolve(&mut m, 2, &id, json!({"a":1})).is_empty());
    assert_eq!(
        response(&resolve(&mut m, 3, &id, json!(2)), 3)["error"]["data"]["reason"],
        "alreadyResolved"
    );
    let mut done = Vec::new();
    m.inbox_reply_written(operation, ReplyOutcome::Written, &mut done);
    assert_eq!(
        response(&done, 1)["result"],
        json!({"status":"resolved","duplicate":false})
    );
    assert_eq!(response(&done, 2)["result"]["duplicate"], true);
    assert!(m.inbox.operations.is_empty());
    assert!(m.inbox.app_ids.is_empty());
    assert_eq!(
        response(&resolve(&mut m, 4, &id, json!({"a":1})), 4)["result"]["duplicate"],
        true
    );
    assert_eq!(
        response(&resolve(&mut m, 5, &id, json!(2)), 5)["error"]["data"]["reason"],
        "alreadyResolved"
    );
    let mut late = Vec::new();
    m.inbox_reply_written(operation, ReplyOutcome::Failed("失败".into()), &mut late);
    assert!(!late.iter().any(|a| matches!(a, Action::Send { .. })));
    assert_eq!(
        response(&resolve(&mut m, 6, "qu_不存在", json!(2)), 6)["error"]["data"]["reason"],
        "notFound"
    );
    for (n, status) in [(8, InboxStatus::Expired), (9, InboxStatus::Cancelled)] {
        let id = register(&mut m, "未来方法", json!({}), json!(n), &mut Vec::new());
        if status == InboxStatus::Expired {
            expire(&mut m, &id);
        } else {
            m.inbox_server_resolved("t", &json!(n), &mut Vec::new());
        }
        assert_eq!(
            response(&resolve(&mut m, n, &id, json!(2)), n)["error"]["data"]["reason"],
            status_event(status)
        );
    }
}

#[test]
fn 写失败回_pending_可重试且断开不改变重复标志() {
    let mut m = machine();
    let id = register(&mut m, "未来方法", json!({}), json!(1), &mut Vec::new());
    let first = op(&resolve(&mut m, 1, &id, json!(1)));
    request(&mut m, 2, 2, "resolve", json!({"id":id,"answer":1}));
    m.inbox_disconnect(1);
    let mut failed = Vec::new();
    m.inbox_reply_written(first, ReplyOutcome::Failed("写失败".into()), &mut failed);
    assert_eq!(
        failed
            .iter()
            .filter(|a| matches!(a, Action::Send { .. }))
            .count(),
        1
    );
    assert_eq!(
        response(&failed, 2)["error"]["data"]["reason"],
        "writeFailed"
    );
    assert_eq!(m.inbox.entries[&id].item.status, InboxStatus::Pending);
    let second = op(&resolve(&mut m, 3, &id, json!(2)));
    assert!(second > first);
    request(&mut m, 2, 4, "resolve", json!({"id":id,"answer":2}));
    m.inbox_disconnect(1);
    let mut done = Vec::new();
    m.inbox_reply_written(second, ReplyOutcome::Written, &mut done);
    assert_eq!(response(&done, 4)["result"]["duplicate"], true);
}

#[test]
fn b2_待处理排队写出与迟到确认均不复活() {
    for mode in 0..4 {
        let mut m = machine();
        let id = register(&mut m, "未来方法", json!({}), json!(7), &mut Vec::new());
        let operation = if mode > 0 {
            Some(op(&resolve(&mut m, 1, &id, json!(true))))
        } else {
            None
        };
        if mode == 3 {
            m.inbox_reply_written(operation.unwrap(), ReplyOutcome::Written, &mut Vec::new());
        }
        let mut actions = Vec::new();
        m.inbox_server_resolved("t", &json!("7"), &mut actions);
        assert_eq!(
            m.inbox.entries[&id].item.status,
            if mode == 3 {
                InboxStatus::Resolved
            } else {
                InboxStatus::Cancelled
            }
        );
        if let Some(operation) = operation {
            if mode != 3 {
                assert!(actions
                    .iter()
                    .any(|a| matches!(a,Action::CancelAppReply {op} if *op==operation)));
            }
            let mut late = Vec::new();
            m.inbox_reply_written(
                operation,
                if mode == 2 {
                    ReplyOutcome::Written
                } else {
                    ReplyOutcome::Failed("失败".into())
                },
                &mut late,
            );
            assert!(!late.iter().any(|a| matches!(a, Action::Send { .. })));
        }
        assert_eq!(
            m.inbox.entries[&id].item.status,
            if mode == 3 {
                InboxStatus::Resolved
            } else {
                InboxStatus::Cancelled
            }
        );
    }
}

#[test]
fn 过期排队先撤回_写出中等待结果_关闭计时清理() {
    for outcome in [
        ReplyOutcome::Cancelled,
        ReplyOutcome::Failed("失败".into()),
        ReplyOutcome::Written,
    ] {
        let mut m = machine();
        let id = register(&mut m, "未来方法", json!({}), json!(1), &mut Vec::new());
        let generation = m.inbox.entries[&id].generation;
        let operation = op(&resolve(&mut m, 1, &id, json!(true)));
        m.clock.now_ms += REQUEST_EXPIRY_MS;
        let expired = expire(&mut m, &id);
        assert!(expired
            .iter()
            .any(|a| matches!(a, Action::CancelAppReply { .. })));
        assert!(!expired
            .iter()
            .any(|a| matches!(a, Action::ServerError { .. })));
        assert_eq!(m.inbox.entries[&id].item.status, InboxStatus::Resolving);
        let mut done = Vec::new();
        m.inbox_reply_written(operation, outcome.clone(), &mut done);
        let expected = if outcome == ReplyOutcome::Written {
            InboxStatus::Resolved
        } else {
            InboxStatus::Expired
        };
        assert_eq!(m.inbox.entries[&id].item.status, expected);
        assert_eq!(done.iter().filter(|a| matches!(a,Action::ServerError {error,..} if error["message"]=="Handler error: Request forwarding timed out")).count(),usize::from(expected==InboxStatus::Expired));
        m.inbox_timer(
            ClaimToken {
                thread_id: "t".into(),
                claim_id: 7,
            },
            InboxTimer::Expiry {
                id: id.clone(),
                generation,
            },
            &mut Vec::new(),
        );
        assert!(m.inbox.entries.contains_key(&id));
        m.clock.now_ms += CLOSED_RETENTION_MS;
        expire(&mut m, &id);
        assert!(m.inbox.entries.is_empty());
    }
}

#[test]
fn guardian_状态缺字段隔离去重与释放后过期() {
    let mut m = machine();
    own(&mut m, "t");
    for status in [json!("denied"), json!({"type":"denied"})] {
        let mut actions = Vec::new();
        let id = guardian(&mut m, "r", status, &mut actions);
        assert_eq!(m.inbox.entries.len(), 1);
        let item = &m.inbox.entries[&id].item;
        assert_eq!(item.run_id, Some("run".into()));
        assert_eq!(item.turn_id, Some("turn".into()));
        assert_eq!(item.expires_at - item.created_at, GUARDIAN_EXPIRY_MS);
    }
    for params in [
        json!({"review":{"status":"denied"},"action":{"type":"command"}}),
        json!({"reviewId":"x","review":{"status":"denied"}}),
        json!({"reviewId":"x","review":{"status":"denied"},"action":{}}),
    ] {
        let mut actions = Vec::new();
        m.inbox_guardian_review("t", &Some(params), &mut actions);
        assert!(actions.iter().any(|a| matches!(a, Action::Warn(_))));
        assert_eq!(m.inbox.entries.len(), 1);
    }
    guardian(&mut m, "accepted", json!("approved"), &mut Vec::new());
    assert_eq!(m.inbox.entries.len(), 1);
    if let State::Running { joined, .. } = &mut m.claims.get_mut("t").unwrap().state {
        *joined = true;
    }
    guardian(&mut m, "joined", json!("denied"), &mut Vec::new());
    assert_eq!(m.inbox.entries.len(), 1);
    m.inbox_claim_released("t", 7, &mut Vec::new());
    m.claims.clear();
    assert!(m.inbox_busy());
    let id = m.inbox.entries.keys().next().unwrap().clone();
    m.clock.now_ms += GUARDIAN_EXPIRY_MS;
    let actions = expire(&mut m, &id);
    assert!(!m.inbox_busy());
    assert!(!actions
        .iter()
        .any(|a| matches!(a, Action::ServerError { .. } | Action::AppReplyAcked { .. })));
    m.clock.now_ms += CLOSED_RETENTION_MS;
    expire(&mut m, &id);
    own(&mut m, "t");
    guardian(&mut m, "r", json!("denied"), &mut Vec::new());
    assert_eq!(m.inbox.entries.len(), 1);
}

#[test]
fn guardian_override_阶段代号失败重试关闭与退订() {
    let mut m = machine();
    own(&mut m, "t");
    m.shared = true;
    let id = guardian(&mut m, "r", json!("denied"), &mut Vec::new());
    m.claims.clear();
    m.records.insert(
        "t".into(),
        ThreadRecord {
            last_claim_id: 77,
            subscribed: true,
            ..Default::default()
        },
    );
    for answer in [
        json!({}),
        json!({"override":true}),
        json!({"override":"true"}),
    ] {
        assert_eq!(
            response(&resolve(&mut m, 9, &id, answer), 9)["error"]["code"],
            -32602
        );
    }
    let answer = json!({"override":true,"event":{"review_id":"r"}});
    let actions = resolve(&mut m, 1, &id, answer.clone());
    let first = op(&actions);
    assert!(actions.iter().any(|a| matches!(a,Action::Request(r) if r.method=="thread/resume" && r.params.as_ref().unwrap()["persistExtendedHistory"]==false)));
    let mut failed = Vec::new();
    m.inbox_rpc_reply(
        InboxRpc::GuardianResume { op: first },
        Err(json!({"code":-32000,"message":"上游错误"})),
        &mut failed,
    );
    assert_eq!(
        response(&failed, 1)["error"]["data"]["cause"]["message"],
        "上游错误"
    );
    let second = op(&resolve(&mut m, 2, &id, answer));
    assert!(second > first);
    let mut late = Vec::new();
    m.inbox_rpc_reply(
        InboxRpc::GuardianResume { op: first },
        Ok(json!({})),
        &mut late,
    );
    assert!(!late.iter().any(|a| matches!(a, Action::Request(_))));
    let mut wrong = Vec::new();
    m.inbox_rpc_reply(
        InboxRpc::GuardianOverride { op: second },
        Ok(json!({})),
        &mut wrong,
    );
    assert_eq!(m.inbox.entries[&id].item.status, InboxStatus::Resolving);
    let mut next = Vec::new();
    m.inbox_rpc_reply(
        InboxRpc::GuardianResume { op: second },
        Ok(json!({})),
        &mut next,
    );
    assert!(next.iter().any(|a| matches!(a,Action::Request(r) if r.method=="thread/approveGuardianDeniedAction" && r.params.as_ref().unwrap()["event"]==json!({"review_id":"r"}))));
    let mut duplicated = Vec::new();
    m.inbox_rpc_reply(
        InboxRpc::GuardianResume { op: second },
        Ok(json!({})),
        &mut duplicated,
    );
    assert!(!duplicated.iter().any(|a| matches!(a, Action::Request(_))));
    let mut done = Vec::new();
    m.inbox_rpc_reply(
        InboxRpc::GuardianOverride { op: second },
        Ok(json!({})),
        &mut done,
    );
    assert_eq!(response(&done, 2)["result"]["duplicate"], false);
    assert!(done.iter().any(|a| matches!(a,Action::Schedule {token,kind:TimerKind::Unsubscribe,..} if token.claim_id==77)));
    own(&mut m, "t");
    let id2 = guardian(&mut m, "r2", json!("denied"), &mut Vec::new());
    let keep = resolve(&mut m, 3, &id2, json!({"override":false}));
    assert_eq!(response(&keep, 3)["result"]["status"], "resolved");
    assert!(!keep
        .iter()
        .any(|a| matches!(a, Action::Request(_) | Action::AppReplyAcked { .. })));
}

#[test]
fn wait_七种事件订阅快照与恰好一次() {
    let mut m = machine();
    assert_eq!(
        response(&request(&mut m, 1, 1, "wait", json!({"timeoutMs":5})), 1)["result"]["event"],
        "idle"
    );
    own(&mut m, "t");
    for (rid, params) in [
        (2, json!({"timeoutMs":5})),
        (3, json!({"threadId":"t","timeoutMs":5})),
        (4, json!({"threadId":"other","timeoutMs":5})),
    ] {
        let a = request(&mut m, 1, rid, "wait", params);
        if rid == 4 {
            assert_eq!(response(&a, 4)["result"]["event"], "idle");
        }
    }
    let mut added = Vec::new();
    let id = register(&mut m, "未来方法", json!({}), json!(1), &mut added);
    for rid in [2, 3] {
        assert_eq!(response(&added, rid)["result"]["event"], "added");
    }
    assert!(m.inbox.waiters.is_empty());
    assert_eq!(
        added
            .iter()
            .filter(|a| matches!(
                a,
                Action::Unschedule {
                    key: TimerKey::InboxWaiter(_)
                }
            ))
            .count(),
        2
    );
    assert_eq!(
        response(
            &request(
                &mut m,
                1,
                5,
                "wait",
                json!({"threadId":"t","timeoutMs":100})
            ),
            5
        )["result"]["item"]["id"],
        id
    );
    let a = request(&mut m, 1, 6, "wait", json!({"id":id,"timeoutMs":5}));
    assert!(!a.iter().any(|a| matches!(a, Action::Send { .. })));
    let waiter = *m.inbox.waiters.keys().next().unwrap();
    let mut timeout = Vec::new();
    m.inbox_timer(
        ClaimToken {
            thread_id: "".into(),
            claim_id: 0,
        },
        InboxTimer::WaitTimeout { waiter },
        &mut timeout,
    );
    assert_eq!(response(&timeout, 6)["result"]["event"], "timeout");
    for (n, status) in [
        (10, InboxStatus::Resolved),
        (20, InboxStatus::Expired),
        (30, InboxStatus::Cancelled),
    ] {
        let id = register(&mut m, "未来方法", json!({}), json!(n), &mut Vec::new());
        request(&mut m, 1, n, "wait", json!({"id":id,"timeoutMs":100}));
        let actions = match status {
            InboxStatus::Resolved => {
                let operation = op(&resolve(&mut m, n + 1, &id, json!(true)));
                let mut a = Vec::new();
                m.inbox_reply_written(operation, ReplyOutcome::Written, &mut a);
                a
            }
            InboxStatus::Expired => expire(&mut m, &id),
            _ => {
                let mut a = Vec::new();
                m.inbox_server_resolved("t", &json!(n), &mut a);
                a
            }
        };
        assert_eq!(
            response(&actions, n)["result"]["event"],
            status_event(status)
        );
        let immediate = request(&mut m, 1, n + 2, "wait", json!({"id":id,"timeoutMs":100}));
        assert_eq!(
            response(&immediate, n + 2)["result"]["event"],
            status_event(status)
        );
    }
    m.inbox_claim_released("t", 7, &mut Vec::new());
    request(
        &mut m,
        1,
        50,
        "wait",
        json!({"threadId":"t","timeoutMs":100}),
    );
    request(&mut m, 2, 51, "wait", json!({"timeoutMs":100}));
    let mut ended = Vec::new();
    m.inbox_run_ended("other", json!({"runId":"x"}), &mut ended);
    assert_eq!(response(&ended, 51)["result"]["run"]["runId"], "x");
    assert_eq!(m.inbox.waiters.len(), 1);
    m.inbox_run_ended("t", json!({"runId":"r"}), &mut ended);
    assert_eq!(response(&ended, 50)["result"]["event"], "runEnded");
    assert!(m.inbox.waiters.is_empty());
}

#[test]
fn wait_参数关闭上限零超时断开撤销() {
    let mut m = machine();
    own(&mut m, "t");
    for timeout in [
        json!(-1),
        json!(1.5),
        json!(MAX_WAIT_MS + 1),
        json!("3"),
        Value::Null,
    ] {
        assert_eq!(
            response(
                &request(&mut m, 1, 1, "wait", json!({"timeoutMs":timeout})),
                1
            )["error"]["code"],
            -32602
        );
    }
    assert_eq!(
        response(&request(&mut m, 1, 1, "wait", json!({"timeoutMs":0})), 1)["result"]["event"],
        "timeout"
    );
    assert_eq!(
        response(
            &request(
                &mut m,
                1,
                2,
                "wait",
                json!({"id":"qu_missing","timeoutMs":100})
            ),
            2
        )["error"]["data"]["reason"],
        "notFound"
    );
    for i in 0..MAX_WAITERS {
        request(
            &mut m,
            1,
            i as u64,
            "wait",
            json!({"threadId":"t","timeoutMs":MAX_WAIT_MS}),
        );
    }
    assert_eq!(m.inbox.waiters.len(), MAX_WAITERS);
    assert!(m.inbox_busy());
    assert_eq!(
        response(
            &request(&mut m, 1, 999, "wait", json!({"timeoutMs":5})),
            999
        )["error"]["data"]["reason"],
        "tooManyWaiters"
    );
    m.inbox_disconnect(1);
    assert!(m.inbox.waiters.is_empty());
    let mut actions = Vec::new();
    m.inbox_flush_actions(&mut actions);
    assert_eq!(actions.len(), MAX_WAITERS);
    assert!(actions
        .iter()
        .all(|a| matches!(a, Action::Unschedule { .. })));
    m.closing = true;
    for method in ["wait", "resolve"] {
        assert_eq!(
            response(&request(&mut m, 1, 999, method, json!({})), 999)["error"]["data"]["reason"],
            "shuttingDown"
        );
    }
    assert!(response(&request(&mut m, 1, 999, "list", json!({})), 999)
        .get("result")
        .is_some());
}

#[test]
fn resolve_等待者上限且只存一份答案() {
    let mut m = machine();
    let id = register(&mut m, "未来方法", json!({}), json!(1), &mut Vec::new());
    let operation = op(&resolve(&mut m, 1, &id, json!({"文本":"回答"})));
    for i in 2..=64 {
        resolve(&mut m, i, &id, json!({"文本":"回答"}));
    }
    assert_eq!(m.inbox.entries[&id].waiters.len(), 64);
    assert_eq!(m.inbox.operations.len(), 1);
    assert_eq!(
        response(&resolve(&mut m, 65, &id, json!({"文本":"回答"})), 65)["error"]["data"]["reason"],
        "tooManyWaiters"
    );
    let mut actions = Vec::new();
    m.inbox_reply_written(operation, ReplyOutcome::Written, &mut actions);
    assert_eq!(
        actions
            .iter()
            .filter(|a| matches!(a, Action::Send { .. }))
            .count(),
        64
    );
}

#[test]
fn 数量上限先淘汰最早闭项_容量拒绝审批及其他类() {
    let mut m = machine();
    for i in 0..MAX_ITEMS {
        register(&mut m, "未来方法", json!({}), json!(i), &mut Vec::new());
    }
    for (method, code) in [
        ("item/commandExecution/requestApproval", -32603),
        ("未来方法", -32601),
    ] {
        let mut a = Vec::new();
        register(&mut m, method, json!({}), json!(10000), &mut a);
        assert_eq!(m.inbox.entries.len(), MAX_ITEMS);
        assert!(a
            .iter()
            .any(|a| matches!(a,Action::ServerError {error,..} if error["code"]==code)));
    }
    own(&mut m, "t");
    guardian(&mut m, "blocked", json!("denied"), &mut Vec::new());
    assert_eq!(m.inbox.entries.len(), MAX_ITEMS);
    let oldest = m.inbox.app_ids[&("t".into(), "0".into())].clone();
    m.inbox_server_resolved("t", &json!(0), &mut Vec::new());
    m.clock.now_ms += 1;
    m.inbox_server_resolved("t", &json!(1), &mut Vec::new());
    register(&mut m, "未来方法", json!({}), json!(10001), &mut Vec::new());
    assert!(!m.inbox.entries.contains_key(&oldest));
    assert_eq!(m.inbox.entries.len(), MAX_ITEMS);
}

#[test]
fn 字节预算包括答案与内部字段_重启裁剪() {
    let mut m = machine();
    let id = register(&mut m, "未来方法", json!({}), json!(1), &mut Vec::new());
    let large = Value::String("x".repeat(MAX_INBOX_BYTES));
    let denied = resolve(&mut m, 1, &id, large.clone());
    assert_eq!(
        response(&denied, 1)["error"]["data"]["reason"],
        "capacityFull"
    );
    assert_eq!(m.inbox.entries[&id].item.status, InboxStatus::Pending);
    let mut actions = Vec::new();
    register(&mut m, "未来方法", large.clone(), json!(2), &mut actions);
    assert_eq!(m.inbox.entries.len(), 1);
    assert!(actions
        .iter()
        .any(|a| matches!(a, Action::ServerError { .. })));
    let mut item = m.inbox.entries[&id].item.clone();
    item.params = large;
    item.id = "qu_oversized".into();
    let mut restored = machine();
    let mut actions = Vec::new();
    restored.inbox_restore(
        Some(InboxFile {
            items: vec![item, m.inbox.entries[&id].item.clone()],
        }),
        &mut actions,
    );
    assert_eq!(restored.inbox.entries.len(), 1);
    assert!(restored.inbox.bytes() <= MAX_INBOX_BYTES);
    assert!(actions.iter().any(|a| matches!(a, Action::Warn(_))));
}

#[test]
fn 关闭一次落盘_等待者全部结算_重启不恢复回答() {
    let mut m = machine();
    own(&mut m, "t");
    let id = register(&mut m, "未来方法", json!({}), json!(1), &mut Vec::new());
    let operation = op(&resolve(&mut m, 1, &id, json!(true)));
    request(&mut m, 2, 2, "wait", json!({"id":id,"timeoutMs":100}));
    let gd = guardian(&mut m, "g", json!("denied"), &mut Vec::new());
    let guardian_op = op(&resolve(
        &mut m,
        3,
        &gd,
        json!({"override":true,"event":{}}),
    ));
    let mut actions = Vec::new();
    m.closing = true;
    m.inbox_shutdown(&mut actions);
    for rid in [1, 2, 3] {
        assert_eq!(
            response(&actions, rid)["error"]["data"]["reason"],
            "shuttingDown"
        );
    }
    assert_eq!(m.inbox.operations.len(), 1);
    assert!(m.inbox.waiters.is_empty());
    assert_eq!(m.inbox.entries[&id].item.status, InboxStatus::Resolving);
    assert_eq!(m.inbox.entries[&gd].item.status, InboxStatus::Pending);
    let snapshots: Vec<_> = actions
        .iter()
        .filter_map(|a| match a {
            Action::Store(StoreOp::PutInbox(f)) => Some(f.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(snapshots.len(), 1);
    assert!(!actions
        .iter()
        .any(|a| matches!(a, Action::ServerError { .. })));
    let mut late = Vec::new();
    m.inbox_reply_written(operation, ReplyOutcome::Written, &mut late);
    assert_eq!(m.inbox.entries[&id].item.status, InboxStatus::Resolved);
    assert!(m.inbox.operations.is_empty());
    assert!(!late.iter().any(|a| matches!(a, Action::ServerError { .. })));
    m.inbox_rpc_reply(
        InboxRpc::GuardianResume { op: guardian_op },
        Ok(json!({})),
        &mut late,
    );
    assert!(!late
        .iter()
        .any(|a| matches!(a, Action::Send { .. } | Action::Request(_))));
    let mut restored = machine();
    let mut actions = Vec::new();
    restored.inbox_restore(Some(snapshots[0].clone()), &mut actions);
    assert!(restored
        .inbox
        .entries
        .values()
        .all(|e| e.item.status == InboxStatus::Expired));
    assert!(!restored.inbox_busy());
    assert_eq!(
        actions
            .iter()
            .filter(|a| matches!(a, Action::Store(StoreOp::PutInbox(_))))
            .count(),
        1
    );
    assert!(!actions.iter().any(|a| matches!(
        a,
        Action::AppReplyAcked { .. } | Action::Request(_) | Action::ServerError { .. }
    )));
    let base = restored.inbox.entries[&id].item.clone();
    let mut items = Vec::new();
    for i in 0..MAX_ITEMS + 10 {
        let mut item = base.clone();
        item.id = format!("qu_{i:08}");
        item.created_at = i as u64;
        items.push(item);
    }
    let mut cropped = machine();
    cropped.inbox_restore(Some(InboxFile { items }), &mut Vec::new());
    assert_eq!(cropped.inbox.entries.len(), MAX_ITEMS);
    assert!(!cropped.inbox.entries.contains_key("qu_00000000"));
}

#[test]
fn 查询过滤与认领换代释放隔离() {
    let mut m = machine();
    let a = register(
        &mut m,
        "item/fileChange/requestApproval",
        json!({}),
        json!(1),
        &mut Vec::new(),
    );
    m.clock.now_ms += 1;
    let b = register(&mut m, "未来方法", json!({}), json!(2), &mut Vec::new());
    let operation = op(&resolve(&mut m, 1, &b, json!(true)));
    assert_eq!(
        m.inbox_pending("t")
            .iter()
            .map(|i| i.id.clone())
            .collect::<Vec<_>>(),
        vec![a.clone(), b.clone()]
    );
    assert_eq!(
        response(
            &request(&mut m, 1, 2, "list", json!({"kind":"approval"})),
            2
        )["result"]["items"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    m.inbox_claim_released("t", 8, &mut Vec::new());
    assert_eq!(m.inbox_pending_count("t"), 2);
    let mut release = Vec::new();
    m.inbox_claim_released("t", 7, &mut release);
    assert_eq!(m.inbox_pending_count("t"), 0);
    assert!(release
        .iter()
        .any(|a| matches!(a,Action::CancelAppReply {op} if *op==operation)));
    assert_eq!(
        response(&request(&mut m, 1, 3, "list", json!({})), 3)["result"]["items"],
        json!([])
    );
    assert_eq!(
        response(
            &request(&mut m, 1, 4, "list", json!({"includeClosed":true})),
            4
        )["result"]["items"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        response(&request(&mut m, 1, 5, "list", json!({"kind":"坏类型"})), 5)["error"]["code"],
        -32602
    );
}

#[test]
fn guardian_未定案预检与无认领不登记() {
    let mut m = machine();
    guardian(&mut m, "无认领", json!("denied"), &mut Vec::new());
    assert!(m.inbox.entries.is_empty());
    own(&mut m, "t");
    for precheck in [false, true] {
        m.claims.get_mut("t").unwrap().state = State::Starting {
            owner: Owner::Client(1),
            req: crate::claim::ReqKind::TurnStart,
            turn_id: None,
            cont: precheck.then(|| crate::claim::ContinueCheck {
                from_run: "old".into(),
                resume_goal: true,
                run_id: "new".into(),
            }),
            settling: !precheck,
            undecided: None,
            submitted_at: 0,
            held: Vec::new(),
        };
        guardian(&mut m, "待定案", json!("denied"), &mut Vec::new());
        assert!(m.inbox.entries.is_empty());
    }
}

#[test]
fn guardian_覆盖失败可重试_过期忽略迟到成功() {
    let mut m = machine();
    own(&mut m, "t");
    let id = guardian(&mut m, "r", json!("denied"), &mut Vec::new());
    let answer = json!({"override":true,"event":{}});
    let operation = op(&resolve(&mut m, 1, &id, answer.clone()));
    m.inbox_rpc_reply(
        InboxRpc::GuardianResume { op: operation },
        Ok(json!({})),
        &mut Vec::new(),
    );
    let mut failed = Vec::new();
    m.inbox_rpc_reply(
        InboxRpc::GuardianOverride { op: operation },
        Err(json!({"code":-32000,"message":"上游拒绝"})),
        &mut failed,
    );
    assert_eq!(m.inbox.entries[&id].item.status, InboxStatus::Pending);
    assert_eq!(response(&failed, 1)["error"]["data"]["reason"], "rpcFailed");
    let newer = op(&resolve(&mut m, 2, &id, answer));
    m.clock.now_ms += GUARDIAN_EXPIRY_MS;
    let actions = expire(&mut m, &id);
    assert_eq!(response(&actions, 2)["error"]["data"]["reason"], "expired");
    let mut late = Vec::new();
    m.inbox_rpc_reply(
        InboxRpc::GuardianResume { op: newer },
        Ok(json!({})),
        &mut late,
    );
    assert!(!late
        .iter()
        .any(|a| matches!(a, Action::Request(_) | Action::Send { .. })));
    assert_eq!(m.inbox.entries[&id].item.status, InboxStatus::Expired);
}

#[test]
fn wait_重放已处理计时器不重复回包_快照选择最早事项() {
    let mut m = machine();
    own(&mut m, "t");
    let first = register(&mut m, "未来方法", json!({}), json!(1), &mut Vec::new());
    m.clock.now_ms += 1;
    register(&mut m, "未来方法", json!({}), json!(2), &mut Vec::new());
    let operation = op(&resolve(&mut m, 1, &first, json!(true)));
    let immediate = request(&mut m, 2, 2, "wait", json!({"timeoutMs":100}));
    assert_eq!(response(&immediate, 2)["result"]["item"]["id"], first);
    assert_eq!(
        response(&immediate, 2)["result"]["item"]["status"],
        "resolving"
    );
    request(&mut m, 2, 3, "wait", json!({"id":first,"timeoutMs":100}));
    let waiter = *m.inbox.waiters.keys().next().unwrap();
    m.inbox_reply_written(operation, ReplyOutcome::Written, &mut Vec::new());
    let mut late = Vec::new();
    m.inbox_timer(
        ClaimToken {
            thread_id: "".into(),
            claim_id: 0,
        },
        InboxTimer::WaitTimeout { waiter },
        &mut late,
    );
    assert!(!late.iter().any(|a| matches!(a, Action::Send { .. })));
}

mod writer;

fn assert_byte_accounting(m: &Machine) {
    let expected = 32 + m.inbox.entries.values().map(Entry::bytes).sum::<usize>();
    assert_eq!(m.inbox.bytes(), expected, "增量字节账本必须与独立重算一致");
    for entry in m.inbox.entries.values() {
        assert_eq!(entry.byte_size, entry.bytes());
    }
}

#[test]
fn 批量关闭与认领释放只构造一次完整快照() {
    let mut baseline = machine();
    let id = register(
        &mut baseline,
        "未来方法",
        json!({"内容":"x".repeat(60 * 1024)}),
        json!(1),
        &mut Vec::new(),
    );
    let template = baseline.inbox.entries[&id].clone();
    baseline.remove_inbox(&id, &mut Vec::new());
    for n in 0..MAX_ITEMS {
        let mut entry = template.clone();
        entry.item.id = format!("qu_{n:016x}");
        entry.app_id = Some(json!(n));
        baseline.inbox.insert_entry(entry);
    }
    assert!(baseline.inbox.bytes() < MAX_INBOX_BYTES);
    for shutdown in [false, true] {
        let mut m = baseline.clone();
        let before = m.inbox.save_count;
        let mut actions = Vec::new();
        if shutdown {
            m.closing = true;
            m.inbox_shutdown(&mut actions);
        } else {
            m.inbox_claim_released("t", 7, &mut actions);
        }
        assert_eq!(
            m.inbox.save_count - before,
            1,
            "直接统计构造次数，不能先创建再过滤"
        );
        assert_eq!(
            actions
                .iter()
                .filter(|a| matches!(a, Action::Store(StoreOp::PutInbox(_))))
                .count(),
            1
        );
        assert_eq!(m.inbox_pending_total(), 0);
        assert_byte_accounting(&m);
        let before = m.inbox.save_count;
        m.inbox_claim_released("t", 7, &mut Vec::new());
        assert_eq!(
            m.inbox.save_count, before,
            "关闭之后的逐认领释放不能重复保存"
        );
    }
}

#[test]
fn 字节缓存覆盖答案合并重试断开终态淘汰及恢复() {
    let mut m = machine();
    let id = register(
        &mut m,
        "未来方法",
        json!({"内容":"原文"}),
        json!(1),
        &mut Vec::new(),
    );
    assert_byte_accounting(&m);
    let operation = op(&resolve(&mut m, 1, &id, json!("答案")));
    assert_byte_accounting(&m);
    request(&mut m, 2, 2, "resolve", json!({"id":id,"answer":"答案"}));
    assert_byte_accounting(&m);
    m.inbox_disconnect(1);
    assert_byte_accounting(&m);
    m.inbox_reply_written(
        operation,
        ReplyOutcome::Failed("失败".into()),
        &mut Vec::new(),
    );
    assert_byte_accounting(&m);
    let operation = op(&resolve(&mut m, 3, &id, json!("新答案")));
    expire(&mut m, &id);
    assert_byte_accounting(&m);
    m.inbox_reply_written(operation, ReplyOutcome::Written, &mut Vec::new());
    assert_byte_accounting(&m);
    let snapshot = InboxFile {
        items: m.inbox.entries.values().map(|e| e.item.clone()).collect(),
    };
    expire(&mut m, &id);
    assert_byte_accounting(&m);
    assert_eq!(m.inbox.bytes(), 32);
    m.inbox_restore(Some(snapshot), &mut Vec::new());
    assert_byte_accounting(&m);
    own(&mut m, "t");
    let gd = guardian(&mut m, "g", json!("denied"), &mut Vec::new());
    let operation = op(&resolve(
        &mut m,
        4,
        &gd,
        json!({"override":true,"event":{}}),
    ));
    assert_byte_accounting(&m);
    m.inbox_rpc_reply(
        InboxRpc::GuardianResume { op: operation },
        Ok(json!({})),
        &mut Vec::new(),
    );
    assert_byte_accounting(&m);
    m.closing = true;
    m.inbox_shutdown(&mut Vec::new());
    assert_byte_accounting(&m);
}

#[test]
fn 容量满时与无客户端的拒绝分类完全一致() {
    let mut m = machine();
    let id = register(&mut m, "未来方法", json!({}), json!(1), &mut Vec::new());
    let template = m.inbox.entries[&id].clone();
    for n in 1..MAX_ITEMS {
        let mut entry = template.clone();
        entry.item.id = format!("qu_{n:016x}");
        m.inbox.insert_entry(entry);
    }
    for method in [
        "item/commandExecution/requestApproval",
        "item/fileChange/requestApproval",
        "item/permissions/requestApproval",
        "item/tool/requestUserInput",
        "mcpServer/elicitation/request",
        "未来方法",
    ] {
        let mut full = Vec::new();
        register(&mut m, method, json!({}), json!("容量拒绝"), &mut full);
        let mut direct = Vec::new();
        requests::reject_no_client(
            HeldRequest {
                id: json!("容量拒绝"),
                forward_id: "".into(),
                method: method.into(),
                params: None,
            },
            &mut direct,
        );
        assert_eq!(
            full.iter()
                .find(|a| matches!(a, Action::ServerError { .. })),
            direct.first(),
            "方法 {method}"
        );
    }
}

#[test]
fn guardian_退役_resume_经事实学习后使用当前代号真正退订() {
    use crate::claim::{step, Event};
    for retired in ["超时", "过期", "事项已淘汰"] {
        for takeover in ["换代后已释放", "记录已淘汰", "新认领仍在", "计时期间新认领"]
        {
            let mut m = machine();
            m.shared = true;
            own(&mut m, "t");
            let id = guardian(&mut m, "g", json!("denied"), &mut Vec::new());
            m.claims.clear();
            let actions = resolve(&mut m, 1, &id, json!({"override":true,"event":{}}));
            let context = actions
                .into_iter()
                .find_map(|a| match a {
                    Action::Request(c) => Some(c),
                    _ => None,
                })
                .unwrap();
            if retired == "超时" {
                // 包含 apply_reply 的失败分派，不直接调用 inbox 钩子。
                (m, _) = step(
                    m,
                    Event::TransportFailed {
                        context: context.clone(),
                        error: proto::error_object(-32000, "恢复订阅超时", None),
                    },
                );
            } else {
                expire(&mut m, &id);
                if retired == "事项已淘汰" {
                    expire(&mut m, &id);
                }
            }
            m.records.insert(
                "t".into(),
                ThreadRecord {
                    last_claim_id: 99,
                    ..Default::default()
                },
            );
            if takeover == "记录已淘汰" {
                m.records.clear();
            }
            if takeover == "新认领仍在" {
                own(&mut m, "t");
            }
            let expected_claim = m.records.get("t").map_or(0, |r| r.last_claim_id);
            // 完整 apply_reply 先学习事实，再分派旧 op。
            let (mut m, actions) = step(
                m,
                Event::Reply {
                    context,
                    result: Ok(json!({"thread":{"id":"t"}})),
                },
            );
            assert!(m.records["t"].subscribed);
            assert!(!actions.iter().any(
                |a| matches!(a,Action::Request(r) if r.method=="thread/approveGuardianDeniedAction")
            ));
            let token = actions.iter().find_map(|a| match a {
                Action::Schedule {
                    token,
                    kind: TimerKind::Unsubscribe,
                    ..
                } => Some(token.clone()),
                _ => None,
            });
            if takeover == "新认领仍在" {
                assert!(token.is_none());
                continue;
            }
            let token = token.expect("迟到恢复订阅必须安排退订");
            assert_eq!(token.claim_id, expected_claim);
            if takeover == "计时期间新认领" {
                own(&mut m, "t");
            }
            let (_, actions) = step(
                m,
                Event::Timer {
                    token,
                    kind: TimerKind::Unsubscribe,
                },
            );
            assert_eq!(
                actions
                    .iter()
                    .filter(|a| matches!(a,Action::Request(r) if r.method=="thread/unsubscribe"))
                    .count(),
                usize::from(takeover != "计时期间新认领")
            );
        }
    }
}

#[test]
fn 守护审批恢复带覆盖并保留历史选项() {
    let mut m = machine();
    own(&mut m, "t");
    let short = m.runs.ensure_thread("t", 1, 1);
    m.runs.index.threads.get_mut(&short).unwrap().resume_overrides = Some(serde_json::from_value(json!({"config":{"features.default_mode_request_user_input":true},"developerInstructions":"提问指令"})).unwrap());
    let id = guardian(&mut m, "r", json!("denied"), &mut Vec::new());
    let actions = resolve(
        &mut m,
        1,
        &id,
        json!({"override":true,"event":{"review_id":"r"}}),
    );
    let resume = actions
        .iter()
        .find_map(|a| match a {
            Action::Request(c) if c.method == "thread/resume" => Some(c),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        resume.params,
        Some(
            json!({"threadId":"t","persistExtendedHistory":false,"config":{"features.default_mode_request_user_input":true},"developerInstructions":"提问指令"})
        )
    );
}

#[test]
fn 守护恢复未知键降级一次并保留历史参数() {
    let mut m = machine();
    own(&mut m, "t");
    let short = m.runs.ensure_thread("t", 1, 1);
    m.runs
        .index
        .threads
        .get_mut(&short)
        .unwrap()
        .resume_overrides = Some(
        serde_json::from_value(
            json!({"config":{"bad.key":true,"keep.key":1},"developerInstructions":"指令"}),
        )
        .unwrap(),
    );
    let id = guardian(&mut m, "r", json!("denied"), &mut Vec::new());
    let actions = resolve(
        &mut m,
        1,
        &id,
        json!({"override":true,"event":{"review_id":"r"}}),
    );
    let context = actions
        .iter()
        .find_map(|a| match a {
            Action::Request(r) if r.method == "thread/resume" => Some(r.clone()),
            _ => None,
        })
        .unwrap();
    let mut actions = Vec::new();
    m.apply_reply(context,Err(json!({"code":-32600,"message":"failed to load configuration: unknown configuration field `bad.key` in -c/--config override"})),&mut actions);
    let retry = actions
        .iter()
        .find_map(|a| match a {
            Action::Request(r) if r.method == "thread/resume" => Some(r.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        retry.params,
        Some(
            json!({"threadId":"t","persistExtendedHistory":false,"config":{"keep.key":1},"developerInstructions":"指令"})
        )
    );
    actions.clear();
    m.apply_reply(retry, Ok(json!({"thread":{"id":"t"}})), &mut actions);
    assert!(actions
        .iter()
        .any(|a| matches!(a,Action::Request(r) if r.method=="thread/approveGuardianDeniedAction")));
}

#[test]
fn 回复按事项真实线程检查删除锁且遗留守护事项阻止删除() {
    let mut m = machine();
    own(&mut m, "t");
    let id = guardian(&mut m, "r", json!("denied"), &mut Vec::new());
    m.claims.clear();
    assert!(m.purge_guardian_pending("t"));
    let purge = ClientRequest {
        conn: 1,
        id: json!(99),
        method: "broker/threads/purge".into(),
        params: Some(json!({"threadId":"t"})),
    };
    let mut actions = Vec::new();
    m.run_purge_request(purge.clone(), "t", &mut actions);
    assert_eq!(
        response(&actions, 99)["error"]["data"]["reason"],
        "purgePending"
    );
    // 先锁住线程，再到达一个 Guardian 事项，验证无顶层 threadId 的回复也受锁保护。
    m.finish_inbox(&id, InboxStatus::Cancelled, None, &mut Vec::new());
    m.run_purge_request(purge, "t", &mut Vec::new());
    own(&mut m, "t");
    let new = guardian(&mut m, "r2", json!("denied"), &mut Vec::new());
    m.claims.clear();
    let actions = resolve(
        &mut m,
        11,
        &new,
        json!({"override":true,"event":{"review_id":"r2"}}),
    );
    assert_eq!(response(&actions, 11)["error"]["code"], -32001);
    assert_eq!(
        response(&actions, 11)["error"]["data"]["reason"],
        "purgePending"
    );
    assert!(!actions.iter().any(|a| matches!(a, Action::Request(_))));
}

#[test]
fn 删除前守护恢复的迟到成功不重建索引或清除标记() {
    let mut m = machine();
    own(&mut m, "t");
    m.runs.ensure_thread("t", 1, 1);
    let id = guardian(&mut m, "r", json!("denied"), &mut Vec::new());
    let actions = resolve(
        &mut m,
        1,
        &id,
        json!({"override":true,"event":{"review_id":"r"}}),
    );
    let context = actions
        .iter()
        .find_map(|a| match a {
            Action::Request(r) => Some(r.clone()),
            _ => None,
        })
        .unwrap();
    m.finish_inbox(&id, InboxStatus::Cancelled, None, &mut Vec::new());
    m.claims.clear();
    m.run_forget_thread("t", &mut Vec::new());
    let mut actions = Vec::new();
    m.apply_reply(context, Ok(json!({"thread":{"id":"t"}})), &mut actions);
    assert!(m.runs.index.removed.contains_key("t"));
    assert!(m.runs.index.threads.is_empty());
    assert!(!actions
        .iter()
        .any(|a| matches!(a,Action::Request(r) if r.method=="thread/approveGuardianDeniedAction")));
}
