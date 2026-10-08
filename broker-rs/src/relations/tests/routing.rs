//! 子线程请求：路由全序每一行、回查队列、撤销与墓碑、detach 接管、收尾撤销、动态工具、Guardian、cxbOrigin、聚合。
use super::*;
use crate::claim::{ClientRequest, TimerKind};
use crate::policy::Policy;

fn open_run(m: &mut Machine) {
    let mut actions = Vec::new();
    m.run_opened(
        crate::runs::RunOpen {
            thread_id: P.into(),
            run_id: R.into(),
            conn: Some(1),
            kind: crate::claim::ReqKind::TurnStart,
            joined: false,
            input: "任务".into(),
            continued_from: None,
        },
        &mut actions,
    );
}

fn client(conn: u64, id: u64, method: &str, params: Value) -> Event {
    Event::ClientRequest {
        request: ClientRequest {
            conn,
            id: json!(id),
            method: method.into(),
            params: Some(params),
        },
        run_id: None,
    }
}

fn reply_to(actions: &[Action], conn: u64, id: u64) -> Value {
    sent(actions, conn)
        .into_iter()
        .find(|m| m["id"] == id && m.get("method").is_none())
        .expect("回包")
}

fn set_broker(m: &mut Machine, policy: Policy) {
    if let State::Running { owner, .. } = &mut m.claims.get_mut(P).unwrap().state {
        *owner = Owner::Broker {
            policy,
            approvals_reviewer: None,
            owned_at: 0,
            last_watcher_at: 0,
        };
    }
}

fn lookup_requests(actions: &[Action]) -> Vec<String> {
    app_requests(actions, "thread/read")
        .into_iter()
        .filter(|c| matches!(c.operation, crate::claim::Operation::RelationLookup { .. }))
        .map(|c| {
            c.params.as_ref().unwrap()["threadId"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect()
}

#[test]
fn 已绑定子线程请求转给父运行owner_带来源标注且回包用原始请求号() {
    let (mut m, mut mock) = setup(false);
    spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    let (id, event) = mock.request(C, Some("子轮1"), Some(json!(41)), APPROVAL);
    let actions = mock.inject(&mut m, event);
    assert!(upstream(&actions).is_empty(), "不 Dismiss、不回答");
    let forwarded = forwarded(&actions, 1);
    assert_eq!(forwarded.len(), 1);
    let params = &forwarded[0]["params"];
    assert_eq!(params["threadId"], C);
    assert_eq!(
        params["cxbOrigin"],
        json!({"rootThreadId": P, "runId": R, "parentThreadId": P, "relationKind": "subagent", "sourceThreadId": C})
    );
    // broker 内部保留原始 params。
    let forward_id = forwarded[0]["id"].as_str().unwrap().to_owned();
    assert!(m.requests.pending[&forward_id]
        .params
        .as_ref()
        .unwrap()
        .get("cxbOrigin")
        .is_none());
    assert_eq!(m.manage_context(R).unwrap().members[C].pending_requests, 1);
    let actions = mock.inject(
        &mut m,
        Event::ForwardResponse {
            conn: 1,
            id: json!(forward_id),
            result: Some(json!({"decision": "accept"})),
            error: None,
        },
    );
    assert_eq!(
        upstream(&actions),
        vec![&Action::ServerResult {
            id: id.clone(),
            result: json!({"decision": "accept"})
        }]
    );
    assert_eq!(mock.answer_count(C, &id), 1);
    assert!(m.relations.requests.is_empty());
    assert_eq!(m.manage_context(R).unwrap().members[C].pending_requests, 0);
}

#[test]
fn 来源标注只发给cxb连接且覆盖上游同名字段() {
    let (mut m, mut mock) = setup(false);
    // owner 换成非 cxb 连接 2。
    if let State::Running { owner, .. } = &mut m.claims.get_mut(P).unwrap().state {
        *owner = Owner::Client(2);
    }
    spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    let (_, event) = mock.request(C, Some("子轮1"), None, APPROVAL);
    let actions = mock.inject(&mut m, event);
    let forwarded2 = forwarded(&actions, 2);
    assert_eq!(forwarded2.len(), 1);
    assert!(forwarded2[0]["params"].get("cxbOrigin").is_none());
    // cxb 连接上，上游参数里碰巧带的同名字段被覆盖。
    let (mut m, mut mock) = setup(false);
    spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    let (_, event) = mock.request(C, Some("子轮1"), None, APPROVAL);
    let Event::ServerRequest {
        id,
        forward_id,
        method,
        mut params,
    } = event
    else {
        unreachable!()
    };
    params.as_mut().unwrap()["cxbOrigin"] = json!({"rootThreadId": "伪造"});
    let actions = mock.inject(
        &mut m,
        Event::ServerRequest {
            id,
            forward_id,
            method,
            params,
        },
    );
    assert_eq!(
        forwarded(&actions, 1)[0]["params"]["cxbOrigin"]["rootThreadId"],
        P
    );
}

#[test]
fn 子线程审批先于派生证据到达_回查后随证据转给父运行owner() {
    let (mut m, mut mock) = setup(false);
    mock.register(C, Some(P), "v1", false, true);
    let (started, completed) = SubagentMock::v1_spawn(P, T, "派生1", C, false);
    mock.inject(&mut m, started);
    let (id, event) = mock.request(C, Some("子轮1"), None, APPROVAL);
    let actions = mock.inject(&mut m, event);
    assert!(upstream(&actions).is_empty());
    assert_eq!(lookup_requests(&actions), vec![C.to_owned()]);
    assert_eq!(
        m.relations.requests.values().next().unwrap().stage,
        ChildRequestStage::Lookup
    );
    // 回查只给出血缘；还没有派生证据，继续等。
    let after = mock.answer_reads(&mut m, &actions);
    assert!(forwarded(&after, 1).is_empty());
    assert_eq!(
        m.thread_relation(C).unwrap().discovered_by,
        DiscoveredBy::Lookup
    );
    let actions = mock.inject(&mut m, completed);
    let forwarded = forwarded(&actions, 1);
    assert_eq!(forwarded.len(), 1, "证据到达后从第 1 条重新判定");
    assert_eq!(forwarded[0]["params"]["cxbOrigin"]["runId"], R);
    assert_eq!(mock.answer_count(C, &id), 0);
    // 迟到的子线程首轮通知不重复转发。
    let first = mock.turn_started(C, "子轮1");
    let actions = mock.inject(&mut m, first);
    assert!(forwarded_none(&actions));
}

fn forwarded_none(actions: &[Action]) -> bool {
    forwarded(actions, 1).is_empty() && upstream(actions).is_empty()
}

#[test]
fn 外部_unbound_conflict_撤权都不答也不dismiss() {
    // 外部：回查证明它是另一个根线程。
    let (mut m, mut mock) = setup(false);
    mock.register("外部", None, "v1", false, true);
    let (id, event) = mock.request("外部", Some("轮"), None, APPROVAL);
    let actions = mock.inject(&mut m, event);
    let after = mock.answer_reads(&mut m, &actions);
    assert!(upstream(&actions).is_empty() && upstream(&after).is_empty());
    assert!(m.relations.requests.is_empty(), "已证明外部，静默丢弃");
    assert_eq!(mock.answer_count("外部", &id), 0);
    // unbound：成员的后续轮次没有证据。
    let (mut m, mut mock) = setup(false);
    spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    let done = mock.turn_completed(C, "子轮1", "completed");
    mock.inject(&mut m, done);
    let next = mock.turn_started(C, "子轮2");
    mock.inject(&mut m, next);
    let (_, event) = mock.request(C, Some("子轮2"), None, APPROVAL);
    let actions = mock.inject(&mut m, event);
    assert!(forwarded_none(&actions));
    assert!(m.relations.requests.is_empty());
    // 撤权（joined/外部接管）：已绑定也不答。
    let (mut m, mut mock) = setup(false);
    spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    let mut actions = Vec::new();
    m.on_handoff(R, HandoffKind::External, &mut actions);
    let (_, event) = mock.request(C, Some("子轮1"), None, APPROVAL);
    let actions = mock.inject(&mut m, event);
    assert!(forwarded_none(&actions));
}

#[test]
fn 来源线程自己有认领时走现有逻辑() {
    let (mut m, mut mock) = setup(false);
    m.claims
        .insert(C.into(), claim(Owner::Client(2), "子轮1", "独立运行", 7));
    let (_, event) = mock.request(C, Some("子轮1"), None, APPROVAL);
    let actions = mock.inject(&mut m, event);
    let forwarded = forwarded(&actions, 2);
    assert_eq!(forwarded.len(), 1);
    assert!(forwarded[0]["params"].get("cxbOrigin").is_none());
    assert!(m.relations.requests.is_empty());
}

#[test]
fn 没有任何自有运行时保持原有交还规则() {
    let mut m = Machine::new(false);
    let mut mock = SubagentMock::default();
    let (id, event) = mock.request("无关", Some("轮"), None, APPROVAL);
    let actions = mock.inject(&mut m, event);
    assert_eq!(upstream(&actions), vec![&Action::Dismiss { id }]);
}

#[test]
fn 关闭中已证明自有的回关闭错误_未知的不答() {
    let (mut m, mut mock) = setup(false);
    spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    m.closing = true;
    let (id, event) = mock.request(C, Some("子轮1"), None, APPROVAL);
    let actions = mock.inject(&mut m, event);
    assert_eq!(
        upstream(&actions),
        vec![&Action::ServerError {
            id,
            error: crate::requests::handler_error("Broker shutting down")
        }]
    );
    let (_, event) = mock.request("未知", Some("轮"), None, APPROVAL);
    let actions = mock.inject(&mut m, event);
    assert!(upstream(&actions).is_empty());
}

#[test]
fn 父认领settling时暂存_定案后重新判定() {
    let (mut m, mut mock) = setup(true);
    spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    m.claims.get_mut(P).unwrap().state = State::Starting {
        owner: Owner::Client(1),
        req: crate::claim::ReqKind::TurnStart,
        turn_id: Some("父轮2".into()),
        cont: None,
        settling: true,
        undecided: None,
        submitted_at: 0,
        held: Vec::new(),
    };
    let (_, event) = mock.request(C, Some("子轮1"), None, APPROVAL);
    let actions = mock.inject(&mut m, event);
    assert!(forwarded_none(&actions));
    assert_eq!(
        m.relations.requests.values().next().unwrap().stage,
        ChildRequestStage::Held
    );
    m.claims.get_mut(P).unwrap().state = State::Running {
        owner: Owner::Client(1),
        turn_id: "父轮2".into(),
        joined: false,
    };
    let mut actions = Vec::new();
    m.on_claim_settled(P, 1, true, &mut actions);
    assert_eq!(forwarded(&actions, 1).len(), 1);
}

#[test]
fn 托管运行按策略应答或进收件箱_保留来源与原期限() {
    for (policy, inbox) in [(Policy::Never, false), (Policy::OnRequest, true)] {
        let (mut m, mut mock) = setup(false);
        open_run(&mut m);
        spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
        set_broker(&mut m, policy);
        let mut actions = Vec::new();
        m.on_handoff(R, HandoffKind::Detach, &mut actions);
        let received = m.clock.now_ms;
        let (id, event) = mock.request(C, Some("子轮1"), None, APPROVAL);
        let actions = mock.inject(&mut m, event);
        if inbox {
            assert!(upstream(&actions).is_empty());
            let items = m.inbox_pending_run_items(R);
            assert_eq!(items.len(), 1);
            let item = &items[0];
            assert_eq!(item.thread_id, C);
            assert_eq!(item.root_thread_id.as_deref(), Some(P));
            assert_eq!(item.parent_thread_id.as_deref(), Some(P));
            assert_eq!(item.run_id.as_deref(), Some(R));
            assert_eq!(item.turn_id.as_deref(), Some("子轮1"));
            assert_eq!(item.expires_at, received + crate::inbox::REQUEST_EXPIRY_MS);
            assert_eq!(m.inbox_pending_count(P), 1, "父线程计数按聚合值");
            assert_eq!(m.inbox_pending_run(R), 1);
        } else {
            assert_eq!(
                upstream(&actions),
                vec![&Action::ServerResult {
                    id,
                    result: json!({"decision": "accept"})
                }]
            );
        }
    }
}

#[test]
fn detach原子接管已转发子请求_保留来源和原期限_旧客户端迟到回答被忽略() {
    let (mut m, mut mock) = setup(false);
    open_run(&mut m);
    spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    let received = m.clock.now_ms;
    let (id, event) = mock.request(C, Some("子轮1"), None, "item/tool/requestUserInput");
    let actions = mock.inject(&mut m, event);
    let forward_id = forwarded(&actions, 1)[0]["id"].clone();
    // 回查中的请求（另一个尚无证据的子线程）。
    let (_, waiting) = mock.request("新子", Some("新轮"), None, APPROVAL);
    mock.inject(&mut m, waiting);
    m.clock.now_ms += 5_000;
    let actions = mock.inject(
        &mut m,
        client(
            1,
            9,
            "broker/threads/detach",
            json!({"threadId": P, "runId": R, "policy": "on-request"}),
        ),
    );
    let reply = reply_to(&actions, 1, 9);
    assert_eq!(reply["result"]["outcome"], "detached");
    let pending = reply["result"]["pending"].as_array().unwrap();
    assert_eq!(pending.len(), 1, "detach 回包按运行汇总");
    assert_eq!(pending[0]["threadId"], C);
    assert_eq!(pending[0]["rootThreadId"], P);
    assert_eq!(
        pending[0]["expiresAt"],
        received + crate::inbox::REQUEST_EXPIRY_MS
    );
    assert!(m.requests.pending.is_empty(), "旧转发已撤回");
    assert!(matches!(
        m.relations
            .requests
            .values()
            .find(|r| r.source_thread_id == C)
            .unwrap()
            .stage,
        ChildRequestStage::Inbox { .. }
    ));
    let late = mock.inject(
        &mut m,
        Event::ForwardResponse {
            conn: 1,
            id: forward_id,
            result: Some(json!({"answers": {}})),
            error: None,
        },
    );
    assert!(upstream(&late).is_empty());
    assert_eq!(mock.answer_count(C, &id), 0);
    // 收件箱回答：用原始 requestId 带确认写出。
    let item = pending[0]["id"].as_str().unwrap().to_owned();
    let actions = mock.inject(
        &mut m,
        client(
            1,
            10,
            "broker/inbox/resolve",
            json!({"id": item, "answer": {"answers": {}}}),
        ),
    );
    let op = actions
        .iter()
        .find_map(|a| match a {
            Action::AppReplyAcked { op, id: got, .. } if *got == id => Some(*op),
            _ => None,
        })
        .expect("带确认写出");
    assert!(matches!(
        m.relations
            .requests
            .values()
            .find(|r| r.source_thread_id == C)
            .unwrap()
            .stage,
        ChildRequestStage::Writing { .. }
    ));
    mock.inject(
        &mut m,
        Event::AppReplyWritten {
            op,
            outcome: crate::inbox::ReplyOutcome::Written,
        },
    );
    assert!(m
        .relations
        .requests
        .values()
        .all(|r| r.source_thread_id != C));
    assert_eq!(mock.answer_count(C, &id), 1);
}

#[test]
fn 子事项通知发给运行owner与逻辑根watcher_wait被子事项唤醒() {
    let (mut m, mut mock) = setup(false);
    open_run(&mut m);
    spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    // 连接 3 观察父线程，并在父线程上等待事项。
    mock.inject(
        &mut m,
        client(3, 1, "broker/threads/watch", json!({"threadId": P})),
    );
    let actions = mock.inject(
        &mut m,
        client(
            3,
            2,
            "broker/inbox/wait",
            json!({"threadId": P, "timeoutMs": 60_000}),
        ),
    );
    assert!(
        sent(&actions, 3).iter().all(|m| m["id"] != 2),
        "尚无事项，等待"
    );
    set_broker(&mut m, Policy::OnRequest);
    let mut actions = Vec::new();
    m.on_handoff(R, HandoffKind::Detach, &mut actions);
    let (_, event) = mock.request(C, Some("子轮1"), None, APPROVAL);
    let actions = mock.inject(&mut m, event);
    let added = notifications(&actions, 3, "broker/inbox/added");
    assert_eq!(added.len(), 1);
    assert_eq!(added[0]["item"]["threadId"], C);
    let woke = reply_to(&actions, 3, 2);
    assert_eq!(woke["result"]["event"], "added");
    assert_eq!(woke["result"]["item"]["rootThreadId"], P);
}

#[test]
fn 回查期限三十秒_未知不答并记诊断() {
    let (mut m, mut mock) = setup(false);
    let (id, event) = mock.request("未知子", Some("轮"), None, APPROVAL);
    let actions = mock.inject(&mut m, event);
    assert_eq!(lookup_requests(&actions), vec!["未知子".to_owned()]);
    let actions = advance(&mut m, &mut mock, LOOKUP_DEADLINE_MS - 1);
    assert!(upstream(&actions).is_empty());
    assert_eq!(m.relations.requests.len(), 1);
    let actions = advance(&mut m, &mut mock, 1);
    assert!(upstream(&actions).is_empty());
    assert!(m.relations.requests.is_empty());
    assert!(actions
        .iter()
        .any(|a| matches!(a, Action::Warn(w) if w.contains("回查"))));
    assert_eq!(mock.answer_count("未知子", &id), 0);
}

#[test]
fn 回查并发八个_同线程合并一个在途() {
    let (mut m, mut mock) = setup(false);
    let mut all = Vec::new();
    for i in 0..10 {
        for j in 0..2 {
            let (_, event) =
                mock.request(&format!("子{i}"), Some(&format!("轮{j}")), None, APPROVAL);
            all.extend(mock.inject(&mut m, event));
        }
    }
    let first = lookup_requests(&all);
    assert_eq!(first.len(), LOOKUP_CONCURRENCY);
    let in_flight = m
        .relations
        .aux
        .lookups
        .values()
        .filter(|l| l.in_flight)
        .count();
    assert_eq!(in_flight, LOOKUP_CONCURRENCY);
    // 回一个，排队的下一个才发出。
    let context = app_requests(&all, "thread/read")[0].clone();
    let actions = mock.inject(
        &mut m,
        Event::Reply {
            context,
            result: Ok(
                json!({"thread": {"id": first[0], "parentThreadId": null, "source": "vscode"}}),
            ),
        },
    );
    assert_eq!(lookup_requests(&actions).len(), 1);
}

#[test]
fn 回查队列全局256每线程16总载荷8兆() {
    // 每线程 16。
    let (mut m, mut mock) = setup(false);
    for i in 0..LOOKUP_PER_THREAD + 1 {
        let (_, event) = mock.request("同一子", Some(&format!("轮{i}")), None, APPROVAL);
        let actions = mock.inject(&mut m, event);
        if i == LOOKUP_PER_THREAD {
            assert!(actions
                .iter()
                .any(|a| matches!(a, Action::Warn(w) if w.contains("回查队列已满"))));
            assert!(upstream(&actions).is_empty());
        }
    }
    assert_eq!(m.relations.requests.len(), LOOKUP_PER_THREAD);
    // 全局 256。
    let (mut m, mut mock) = setup(false);
    for i in 0..LOOKUP_MAX_REQUESTS + 1 {
        let (_, event) = mock.request(&format!("子{i}"), Some("轮"), None, APPROVAL);
        mock.inject(&mut m, event);
    }
    assert_eq!(m.relations.requests.len(), LOOKUP_MAX_REQUESTS);
    // 总载荷 8 MiB。
    let (mut m, mut mock) = setup(false);
    let big = "x".repeat(3 * 1024 * 1024);
    for i in 0..3 {
        let (id, event) = mock.request(&format!("大{i}"), Some("轮"), None, APPROVAL);
        let Event::ServerRequest {
            forward_id,
            method,
            mut params,
            ..
        } = event
        else {
            unreachable!()
        };
        params.as_mut().unwrap()["command"] = json!(big);
        mock.inject(
            &mut m,
            Event::ServerRequest {
                id,
                forward_id,
                method,
                params,
            },
        );
    }
    assert_eq!(m.relations.requests.len(), 2);
}

#[test]
fn 回查失败退避重试最多三次_迟到回包按代号丢弃() {
    let (mut m, mut mock) = setup(false);
    let (_, event) = mock.request("未知子", Some("轮"), None, APPROVAL);
    let actions = mock.inject(&mut m, event);
    let mut context = app_requests(&actions, "thread/read")[0].clone();
    let stale = context.clone();
    for attempt in 1..=LOOKUP_RETRIES {
        let actions = mock.inject(
            &mut m,
            Event::Reply {
                context: context.clone(),
                result: Err(json!({"code": -32603, "message": "暂时失败"})),
            },
        );
        assert!(lookup_requests(&actions).is_empty(), "失败后不立即重发");
        if attempt == LOOKUP_RETRIES {
            assert!(m.relations.aux.lookups["未知子"].exhausted);
            break;
        }
        let backoff = LOOKUP_BACKOFF_MS << (attempt - 1);
        let early = advance(&mut m, &mut mock, backoff - 1);
        assert!(lookup_requests(&early).is_empty());
        let retry = advance(&mut m, &mut mock, 1);
        let reads = app_requests(&retry, "thread/read");
        assert_eq!(reads.len(), 1, "第 {attempt} 次退避后重试");
        context = reads[0].clone();
    }
    // 第一次的旧回包迟到：代号不符，丢弃。
    let actions = mock.inject(
        &mut m,
        Event::LateReply {
            context: stale,
            result: Ok(json!({"thread": {"id": "未知子", "parentThreadId": P}})),
        },
    );
    assert!(actions
        .iter()
        .any(|a| matches!(a, Action::Debug(d) if d.contains("迟到"))));
    assert!(m.thread_relation("未知子").is_none());
}

#[test]
fn resolved先于请求到达写墓碑_不复活_过期后按普通请求处理() {
    let (mut m, mut mock) = setup(false);
    spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    let resolved = SubagentMock::resolved(C, &json!(77));
    let actions = mock.inject(&mut m, resolved);
    assert!(
        upstream(&actions).is_empty(),
        "无认领线程的 resolved 不 Dismiss"
    );
    assert!(m.relations.tombstones.contains_key(&(0, "77".into())));
    let (_, event) = mock.request(C, Some("子轮1"), Some(json!(77)), APPROVAL);
    let actions = mock.inject(&mut m, event);
    assert!(forwarded_none(&actions), "墓碑命中，不复活");
    advance(&mut m, &mut mock, TOMBSTONE_MS);
    assert!(m.relations.tombstones.is_empty());
    let (_, event) = mock.request(C, Some("子轮1"), Some(json!(77)), APPROVAL);
    let actions = mock.inject(&mut m, event);
    assert_eq!(forwarded(&actions, 1).len(), 1, "保留期之后按普通请求处理");
}

#[test]
fn 墓碑容量4096满时淘汰最旧并告警() {
    let (mut m, mut mock) = setup(false);
    for i in 0..MAX_TOMBSTONES {
        m.clock.mono_ms += 1;
        mock.inject(&mut m, SubagentMock::resolved(C, &json!(i)));
    }
    assert_eq!(m.relations.tombstones.len(), MAX_TOMBSTONES);
    let actions = mock.inject(&mut m, SubagentMock::resolved(C, &json!("新")));
    assert_eq!(m.relations.tombstones.len(), MAX_TOMBSTONES);
    assert!(!m.relations.tombstones.contains_key(&(0, "0".into())));
    assert!(actions
        .iter()
        .any(|a| matches!(a, Action::Warn(w) if w.contains("墓碑"))));
}

#[test]
fn resolved撤销各阶段_重复请求丢弃() {
    // Forwarded：撤掉转发，旧 CLI 的迟到回答不再回给上游。
    let (mut m, mut mock) = setup(false);
    spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    let (id, event) = mock.request(C, Some("子轮1"), None, APPROVAL);
    let actions = mock.inject(&mut m, event);
    let forward_id = forwarded(&actions, 1)[0]["id"].clone();
    let (_, duplicate) = mock.request(C, Some("子轮1"), Some(id.clone()), APPROVAL);
    let actions = mock.inject(&mut m, duplicate);
    assert!(forwarded_none(&actions), "同 ID 重复到达不重复转发");
    let actions = mock.inject(&mut m, SubagentMock::resolved(C, &id));
    assert!(upstream(&actions).is_empty());
    assert!(m.requests.pending.is_empty() && m.relations.requests.is_empty());
    let late = mock.inject(
        &mut m,
        Event::ForwardResponse {
            conn: 1,
            id: forward_id,
            result: Some(json!({})),
            error: None,
        },
    );
    assert!(upstream(&late).is_empty());
    assert_eq!(mock.answer_count(C, &id), 0);
    // Lookup：直接撤销。
    let (mut m, mut mock) = setup(false);
    let (id, event) = mock.request("未知子", Some("轮"), None, APPROVAL);
    mock.inject(&mut m, event);
    mock.inject(&mut m, SubagentMock::resolved("未知子", &id));
    assert!(m.relations.requests.is_empty());
    // Inbox：事项关闭为 cancelled。
    let (mut m, mut mock) = setup(false);
    open_run(&mut m);
    spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    set_broker(&mut m, Policy::OnRequest);
    let mut a = Vec::new();
    m.on_handoff(R, HandoffKind::Detach, &mut a);
    let (id, event) = mock.request(C, Some("子轮1"), None, APPROVAL);
    mock.inject(&mut m, event);
    assert_eq!(m.inbox_pending_run(R), 1);
    let actions = mock.inject(&mut m, SubagentMock::resolved(C, &id));
    assert!(upstream(&actions).is_empty());
    assert_eq!(m.inbox_pending_run(R), 0);
    assert!(m.relations.requests.is_empty());
}

#[test]
fn 运行收尾按run撤销子请求与收件箱事项_不答不dismiss() {
    let (mut m, mut mock) = setup(false);
    open_run(&mut m);
    spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    let (forwarded_id, event) = mock.request(C, Some("子轮1"), None, APPROVAL);
    let actions = mock.inject(&mut m, event);
    assert_eq!(forwarded(&actions, 1).len(), 1);
    // 父轮结束：子请求待答算受管活动，SA-b 门禁暂缓释放并发起整树停止；停止开始即撤回已转发的子请求，
    // 按清理策略进收件箱（不答、不 Dismiss），运行提交 ended 时再按 run 撤销（见 SA-b 的门禁用例）。
    let done = mock.turn_completed(P, T, "completed");
    let actions = mock.inject(&mut m, done);
    assert!(matches!(
        m.claims[P].state,
        crate::claim::State::FinishingTree { .. }
    ));
    assert!(upstream(&actions).is_empty());
    assert!(
        m.requests.pending.is_empty(),
        "已转发的子请求随运行收尾撤回"
    );
    assert!(m
        .relations
        .requests
        .values()
        .all(|r| !matches!(r.stage, ChildRequestStage::Forwarded { .. })));
    assert_eq!(mock.answer_count(C, &forwarded_id), 0);
    // 收尾之后的新请求不再转给旧 owner。
    let (_, event) = mock.request(C, Some("子轮1"), None, APPROVAL);
    let actions = mock.inject(&mut m, event);
    assert!(forwarded_none(&actions));
}

#[test]
fn 修复2_撤权取消排队回答且失败不得重试() {
    for outcome in [
        crate::inbox::ReplyOutcome::Written,
        crate::inbox::ReplyOutcome::Cancelled,
        crate::inbox::ReplyOutcome::Failed("写失败".into()),
    ] {
        for cleanup in [false, true] {
            let (mut m, mut mock) = setup(false);
            open_run(&mut m);
            spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
            set_broker(&mut m, Policy::OnRequest);
            let mut a = Vec::new();
            m.on_handoff(R, HandoffKind::Detach, &mut a);
            let (id, event) = mock.request(C, Some("子轮1"), None, APPROVAL);
            mock.inject(&mut m, event);
            let item = m.inbox_pending_run_items(R)[0].id.clone();
            let actions = mock.inject(
                &mut m,
                client(
                    1,
                    3,
                    "broker/inbox/resolve",
                    json!({"id": item, "answer": {"decision": "accept"}}),
                ),
            );
            let op = actions
                .iter()
                .find_map(|a| match a {
                    Action::AppReplyAcked { op, .. } => Some(*op),
                    _ => None,
                })
                .unwrap();
            let mut actions = Vec::new();
            if cleanup {
                m.relations_end_cleanup(R, &mut actions);
            } else {
                m.on_handoff(R, HandoffKind::External, &mut actions);
            }
            assert!(actions
                .iter()
                .any(|a| matches!(a, Action::CancelAppReply { .. })));
            assert!(matches!(
                m.relations.requests.values().next().unwrap().stage,
                ChildRequestStage::Writing { .. }
            ));
            mock.inject(
                &mut m,
                Event::AppReplyWritten {
                    op,
                    outcome: outcome.clone(),
                },
            );
            assert!(m.relations.requests.is_empty());
            let committed = outcome == crate::inbox::ReplyOutcome::Written;
            assert_eq!(mock.answer_count(C, &id), 1);
            let retry = mock.inject(
                &mut m,
                client(
                    1,
                    4,
                    "broker/inbox/resolve",
                    json!({"id": item, "answer": {"decision": "accept"}}),
                ),
            );
            assert!(!retry
                .iter()
                .any(|a| matches!(a, Action::AppReplyAcked { .. })));
            assert_eq!(reply_to(&retry, 1, 4).get("error").is_some(), !committed);
            let duplicate = mock.inject(
                &mut m,
                Event::AppReplyWritten {
                    op,
                    outcome: outcome.clone(),
                },
            );
            assert!(upstream(&duplicate).is_empty());
        }
    }
}

#[test]
fn 动态工具沿用现有规则_用运行认领判断() {
    // 私有、owner 在线：转发。
    let (mut m, mut mock) = setup(false);
    spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    let (_, event) = mock.request(C, Some("子轮1"), None, "item/tool/call");
    let actions = mock.inject(&mut m, event);
    assert_eq!(forwarded(&actions, 1).len(), 1);
    // 共享、非定案回放：不答（不 Dismiss）。
    let (mut m, mut mock) = setup(true);
    spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    let (_, event) = mock.request(C, Some("子轮1"), None, "item/tool/call");
    let actions = mock.inject(&mut m, event);
    assert!(forwarded_none(&actions));
    // 托管：-32601。
    let (mut m, mut mock) = setup(false);
    spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    set_broker(&mut m, Policy::Never);
    let mut a = Vec::new();
    m.on_handoff(R, HandoffKind::Detach, &mut a);
    let (id, event) = mock.request(C, Some("子轮1"), None, "item/tool/call");
    let actions = mock.inject(&mut m, event);
    assert!(
        matches!(upstream(&actions)[..], [Action::ServerError { id: ref got, ref error }] if *got == id && error["code"] == -32601)
    );
}

#[test]
fn 清理路由按冻结策略_不转回旧客户端() {
    for shared in [false, true] {
        let (mut m, mut mock) = setup(shared);
        open_run(&mut m);
        spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
        let (_, event) = mock.request(C, Some("子轮1"), None, APPROVAL);
        let actions = mock.inject(&mut m, event);
        assert_eq!(forwarded(&actions, 1).len(), 1);
        // 停止开始：Forwarded 撤回、按清理策略（缺省按需人工）进收件箱。
        let mut actions = Vec::new();
        m.relations_enter_cleanup(R, 5, &mut actions);
        assert!(m.requests.pending.is_empty());
        assert_eq!(m.inbox_pending_run(R), 1);
        // 新的普通请求进收件箱，不新转发。
        let (_, event) = mock.request(C, Some("子轮1"), None, APPROVAL);
        let actions = mock.inject(&mut m, event);
        assert!(forwarded(&actions, 1).is_empty());
        assert_eq!(m.inbox_pending_run(R), 2);
        // 动态工具：私有 -32601，共享静默。
        let (id, event) = mock.request(C, Some("子轮1"), None, "item/tool/call");
        let actions = mock.inject(&mut m, event);
        if shared {
            assert!(forwarded_none(&actions));
        } else {
            assert!(
                matches!(upstream(&actions)[..], [Action::ServerError { id: ref got, .. }] if *got == id)
            );
        }
        // 树结束提交：撤销旧 owner 回答权，清理期间新请求仍进收件箱。
        let mut actions = Vec::new();
        m.relations_commit_cleanup(R, 5, &mut actions);
        assert_eq!(
            m.manage_context(R).unwrap().authority,
            Authority::Cleanup { tree_op: 5 }
        );
        let (_, event) = mock.request(C, Some("子轮1"), None, APPROVAL);
        let actions = mock.inject(&mut m, event);
        assert!(forwarded(&actions, 1).is_empty());
        assert_eq!(m.inbox_pending_run(R), 1);
    }
}

#[test]
fn guardian拒绝走同一解析链进gd() {
    let (mut m, mut mock) = setup(false);
    open_run(&mut m);
    spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    let denied = Event::Notification {
        method: "item/autoApprovalReview/completed".into(),
        params: Some(
            json!({"threadId": C, "turnId": "子轮1", "reviewId": "审查1",
            "review": {"status": "denied"}, "action": {"type": "command"}}),
        ),
    };
    mock.inject(&mut m, denied.clone());
    let items = m.inbox_pending_run_items(R);
    assert_eq!(items.len(), 1);
    assert!(items[0].id.starts_with("gd_"));
    assert_eq!(items[0].thread_id, C);
    assert_eq!(items[0].root_thread_id.as_deref(), Some(P));
    assert_eq!(m.manage_context(R).unwrap().members[C].pending_requests, 1);
    // 外部线程的拒绝不进收件箱。
    mock.register("外部", None, "v1", false, true);
    let foreign = Event::Notification {
        method: "item/autoApprovalReview/completed".into(),
        params: Some(
            json!({"threadId": "外部", "turnId": "轮", "reviewId": "审查2",
            "review": {"status": "denied"}, "action": {"type": "command"}}),
        ),
    };
    let actions = mock.inject(&mut m, foreign);
    mock.answer_reads(&mut m, &actions);
    assert_eq!(m.inbox_pending_run_items(R).len(), 1);
}

#[test]
fn watch快照带children与revision_子成员变化只发cxb连接() {
    let (mut m, mut mock) = setup(false);
    open_run(&mut m);
    mock.inject(
        &mut m,
        client(2, 1, "broker/threads/watch", json!({"threadId": P})),
    );
    let actions = mock.inject(
        &mut m,
        client(3, 1, "broker/threads/watch", json!({"threadId": P})),
    );
    let snapshot = reply_to(&actions, 3, 1)["result"].clone();
    assert_eq!(snapshot["revision"], 0);
    assert_eq!(snapshot["children"]["members"], json!([]));
    let actions = spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    let changes = notifications(&actions, 3, "broker/run/childChanged");
    assert!(!changes.is_empty());
    assert!(
        notifications(&actions, 2, "broker/run/childChanged").is_empty(),
        "非 cxb 连接不收"
    );
    let revisions: Vec<u64> = changes
        .iter()
        .map(|c| c["revision"].as_u64().unwrap())
        .collect();
    assert!(
        revisions.windows(2).all(|w| w[0] < w[1]),
        "revision 单调递增"
    );
    let last = changes.last().unwrap();
    assert_eq!(last["runId"], R);
    assert_eq!(last["rootThreadId"], P);
    assert_eq!(last["threadId"], C);
    assert_eq!(last["activity"], "running");
    let actions = mock.inject(
        &mut m,
        client(3, 2, "broker/threads/watch", json!({"threadId": P})),
    );
    let snapshot = reply_to(&actions, 3, 2)["result"].clone();
    assert_eq!(snapshot["revision"], *revisions.last().unwrap());
    assert_eq!(snapshot["children"]["members"][0]["threadId"], C);
    assert_eq!(
        snapshot["children"]["members"][0]["turnIds"],
        json!(["子轮1"])
    );
    // 子线程原始的 turn/completed 不转发给父运行的 watcher。
    let done = mock.turn_completed(C, "子轮1", "completed");
    let actions = mock.inject(&mut m, done);
    assert!(sent(&actions, 3)
        .iter()
        .all(|m| m["method"] != "turn/completed"));
}

#[test]
fn review迁移后review线程派生的子代理请求到达客户端() {
    let (mut m, mut mock) = setup(false);
    open_run(&mut m);
    // 运行迁到 review 线程：执行线程变化，逻辑根仍是 P。
    let mut review = m.claims.remove(P).unwrap();
    review.owned_turns = ["审查轮".to_owned()].into_iter().collect();
    review.state = State::Running {
        owner: Owner::Client(1),
        turn_id: "审查轮".into(),
        joined: false,
    };
    m.claims.insert("审查线程".into(), review);
    m.runs.runs.get_mut(R).unwrap().review_thread_id = Some("审查线程".into());
    mock.register(C, Some("审查线程"), "v1", false, true);
    let (s, c) = SubagentMock::v1_spawn("审查线程", "审查轮", "派生1", C, false);
    let first = mock.turn_started(C, "子轮1");
    run(&mut m, &mut mock, vec![s, c, first]);
    let (_, event) = mock.request(C, Some("子轮1"), None, APPROVAL);
    let actions = mock.inject(&mut m, event);
    let forwarded = forwarded(&actions, 1);
    assert_eq!(forwarded.len(), 1);
    assert_eq!(forwarded[0]["params"]["cxbOrigin"]["rootThreadId"], P);
    assert_eq!(
        forwarded[0]["params"]["cxbOrigin"]["parentThreadId"],
        "审查线程"
    );
    let context = m.manage_context(R).unwrap();
    assert_eq!(context.identity.logical_root_thread_id, P);
    assert_eq!(context.identity.execution_thread_id, "审查线程");
}

#[test]
fn 无轮次请求只认唯一已绑定活动轮次() {
    let (mut m, mut mock) = setup(false);
    spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    let (_, event) = mock.request(C, None, None, APPROVAL);
    let actions = mock.inject(&mut m, event);
    assert_eq!(forwarded(&actions, 1).len(), 1);
    // 第二个活动轮次出现后，不带 turnId 的请求视为未知。
    let other = mock.turn_started(C, "子轮2");
    mock.inject(&mut m, other);
    let (_, event) = mock.request(C, None, None, APPROVAL);
    let actions = mock.inject(&mut m, event);
    assert!(forwarded_none(&actions));
    assert_eq!(
        m.relations
            .requests
            .values()
            .filter(|r| r.stage == ChildRequestStage::Lookup)
            .count(),
        1
    );
    let _ = TimerKind::Lifecycle;
}

#[test]
fn 修复1_转发与收件箱超三十秒不反复扫描() {
    for inbox in [false, true] {
        let (mut m, mut mock) = setup(false);
        open_run(&mut m);
        spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
        if inbox {
            set_broker(&mut m, Policy::OnRequest);
            m.on_handoff(R, HandoffKind::Detach, &mut Vec::new());
        }
        let (_, event) = mock.request(C, Some("子轮1"), None, APPROVAL);
        mock.inject(&mut m, event);
        let actions = advance(&mut m, &mut mock, LOOKUP_DEADLINE_MS + 1);
        assert_eq!(m.relations.requests.len(), 1);
        assert!(!actions.iter().any(|a| matches!(
            a,
            Action::Schedule {
                kind: TimerKind::LookupDeadline { .. },
                delay_ms: 0,
                ..
            }
        )));
        assert!(m.relations.aux.sweep.is_none());
        assert!(advance(&mut m, &mut mock, 1_000).is_empty());
    }
}

#[test]
fn 修复3_guardian处理后保留受管订阅_真实退订使附着失效() {
    let (mut m, mut mock) = setup(false);
    open_run(&mut m);
    spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    mock.inject(&mut m, Event::Notification {
        method: "item/autoApprovalReview/completed".into(),
        params: Some(json!({"threadId": C, "turnId": "子轮1", "reviewId": "审查1", "review": {"status": "denied"}, "action": {"type": "command"}})),
    });
    let item = m.inbox_pending_run_items(R)[0].id.clone();
    let actions = mock.inject(
        &mut m,
        client(
            1,
            10,
            "broker/inbox/resolve",
            json!({"id":item,"answer":{"override":true,"event":{}}}),
        ),
    );
    let resume = app_requests(&actions, "thread/resume")[0].clone();
    let actions = mock.inject(
        &mut m,
        Event::Reply {
            context: resume,
            result: Ok(json!({"thread":{"id":C}})),
        },
    );
    let approve = app_requests(&actions, "thread/approveGuardianDeniedAction")[0].clone();
    let actions = mock.inject(
        &mut m,
        Event::Reply {
            context: approve,
            result: Ok(json!({})),
        },
    );
    assert!(!actions.iter().any(|a| matches!(
        a,
        Action::Schedule {
            kind: TimerKind::Unsubscribe,
            ..
        }
    )));
    let timer = Event::Timer {
        token: crate::claim::ClaimToken {
            thread_id: C.into(),
            claim_id: 0,
        },
        kind: TimerKind::Unsubscribe,
    };
    // 即使旧计时已排队，执行时也重查管理关系。
    assert!(app_requests(&mock.inject(&mut m, timer.clone()), "thread/unsubscribe").is_empty());
    assert!(m.relations.attachments[C].valid);
    assert!(m.records[C].subscribed);
    // 单独验证活动绑定和清理范围，避免只靠成员快照挡住退订。
    let context = m.relations.contexts.remove(R).unwrap();
    assert!(m.relation_needs_subscription(C));
    m.relations.contexts.insert(R.into(), context);
    m.relations_enter_cleanup(R, 77, &mut Vec::new());
    m.relations.contexts.get_mut(R).unwrap().authority = Authority::Revoked;
    m.relations.bindings.clear();
    m.relations.contexts.get_mut(R).unwrap().members.clear();
    assert!(app_requests(&mock.inject(&mut m, timer.clone()), "thread/unsubscribe").is_empty());
    m.relations.contexts.get_mut(R).unwrap().cleanup = None;
    let actions = mock.inject(&mut m, timer);
    assert_eq!(app_requests(&actions, "thread/unsubscribe").len(), 1);
    assert!(!m.relations.attachments[C].valid);
    assert!(!m.records[C].subscribed);
}

#[test]
fn 修复5_回查超时静默丢弃仍退役rpc登记() {
    let (mut m, mut mock) = setup(true);
    let (id, event) = mock.request("未知外部", Some("轮"), None, APPROVAL);
    mock.inject(&mut m, event);
    let actions = advance(&mut m, &mut mock, LOOKUP_DEADLINE_MS + 1);
    assert!(upstream(&actions).is_empty());
    let key = crate::proto::request_id_key(&id);
    assert!(m.relations.tombstones.contains_key(&(0, key.clone())));
    assert!(m.relations.requests.is_empty());
    let (_, duplicate) = mock.request("未知外部", Some("轮"), Some(id), APPROVAL);
    let actions = mock.inject(&mut m, duplicate);
    assert!(app_requests(&actions, "thread/read").is_empty());
    let actions = advance(&mut m, &mut mock, TOMBSTONE_MS);
    assert!(actions
        .iter()
        .any(|a| matches!(a, Action::ForgetServerRequest { request_key } if request_key == &key)));
}

#[test]
fn 修复6_历史运行记录保留期间回收关系运行态() {
    let (mut m, mut mock) = setup(false);
    open_run(&mut m);
    spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    m.claims.clear();
    m.relations.contexts.get_mut(R).unwrap().authority = Authority::Revoked;
    m.prune_contexts();
    assert!(m.manage_context(R).is_some(), "运行未结束不能回收");
    m.runs.runs.get_mut(R).unwrap().ended_at = Some(m.clock.now_ms);
    m.runs.tree_retained.insert(R.into());
    m.prune_contexts();
    assert!(m.manage_context(R).is_some(), "清理保留屏障未结算不能回收");
    m.runs.tree_retained.remove(R);
    mock.inject(&mut m, Event::Initialized { conn: 1, cxb: true });
    assert!(m.runs.runs.contains_key(R));
    assert!(m.manage_context(R).is_none());
    assert!(m.relations.bindings.is_empty());
    assert!(m.relations.aux.member_evidence.is_empty());
}
