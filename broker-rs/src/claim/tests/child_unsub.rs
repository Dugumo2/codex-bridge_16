//! 子线程订阅释放：app-server 把子代理线程自动挂到本连接，根线程释放退订时要一并退订已无活动的子线程（E4 实测）。
//! 绑定由真实 v1 派生建立；未启用 SA1b，父轮完成即释放认领。
use super::*;
use crate::relations::{AgentVersion, BindingStatus, DiscoveredBy, RelationEdge, RelationKind};
use crate::test_support::subagent::SubagentMock;

const P: &str = "父线程";
const T: &str = "父轮";
const R: &str = "父运行";
const C: &str = "子线程";
const CT: &str = "子轮";

/// 父线程上运行 R 已开轮，owner 是 cxb 连接 1。
fn setup() -> (Machine, SubagentMock) {
    let mut m = Machine::default();
    m.clock.mono_ms = 1_000;
    m.clock.now_ms = 100_000;
    apply(&mut m, Event::Connected { conn: 1 });
    apply(&mut m, Event::Initialized { conn: 1, cxb: true });
    m.record(P).subscribed = true;
    let out = apply(
        &mut m,
        Event::ClientRequest {
            request: request(
                1,
                1,
                "turn/start",
                json!({"threadId":P,"input":[{"type":"text","text":"任务"}]}),
            ),
            run_id: Some(R.into()),
        },
    );
    let context = out
        .iter()
        .find_map(|a| match a {
            Action::Request(c) if c.method == "turn/start" => Some(c.clone()),
            _ => None,
        })
        .expect("开轮请求");
    reply(&mut m, &context, Ok(json!({"turn":{"id":T}})));
    apply(
        &mut m,
        notify(
            "turn/started",
            json!({"threadId":P,"turn":{"id":T,"status":"inProgress","items":[]}}),
        ),
    );
    let mut mock = SubagentMock::default();
    mock.register(P, None, "v1", false, true);
    (m, mock)
}

/// `parent` 在 `parent_turn` 里 v1 派生 `child` 并开始首轮。
fn spawn(
    m: &mut Machine,
    mock: &mut SubagentMock,
    parent: &str,
    parent_turn: &str,
    child: &str,
    item: &str,
    turn: &str,
) {
    mock.register(child, Some(parent), "v1", false, true);
    let (started, completed) = SubagentMock::v1_spawn(parent, parent_turn, item, child, false);
    apply(m, started);
    apply(m, completed);
    apply(m, mock.turn_started(child, turn));
}

fn parent_completed(m: &mut Machine) -> Vec<Action> {
    apply(
        m,
        notify(
            "turn/completed",
            json!({"threadId":P,"turn":{"id":T,"status":"completed","items":[]}}),
        ),
    )
}

/// 父轮完成、释放认领，再触发根线程的退订计时；返回计时产生的动作。
fn release_root(m: &mut Machine) -> Vec<Action> {
    let token = m.token(P).expect("父线程认领");
    let released = parent_completed(m);
    assert!(
        released.iter().any(|a| matches!(
            a,
            Action::Schedule {
                kind: TimerKind::Unsubscribe,
                ..
            }
        )),
        "释放认领应排根线程退订"
    );
    assert!(!m.claims.contains_key(P), "父轮完成即释放认领");
    apply(
        m,
        Event::Timer {
            token,
            kind: TimerKind::Unsubscribe,
        },
    )
}

fn unsubscribed(actions: &[Action]) -> Vec<String> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Request(c) if c.method == "thread/unsubscribe" => c
                .params
                .as_ref()
                .and_then(|p| p["threadId"].as_str())
                .map(str::to_owned),
            _ => None,
        })
        .collect()
}

#[test]
fn 根释放时已结束的子线程随根退订且附着证据失效() {
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, C, "派生1", CT);
    assert_eq!(
        m.binding(C, CT).expect("首轮绑定").status,
        BindingStatus::Bound
    );
    apply(&mut m, mock.turn_completed(C, CT, "completed"));
    let a = release_root(&mut m);
    assert_eq!(unsubscribed(&a), vec![P.to_owned(), C.to_owned()]);
    assert!(!m.relations.attachments[C].valid);
    // 子线程的退订回包走现成的退订处理，不改任何事实、不报错。
    let request = a
        .iter()
        .find_map(|x| match x {
            Action::Request(c) if c.params.as_ref().is_some_and(|p| p["threadId"] == C) => {
                Some(c.clone())
            }
            _ => None,
        })
        .unwrap();
    let after = reply(&mut m, &request, Ok(json!({"status":"unsubscribed"})));
    assert!(!after.iter().any(|x| matches!(x, Action::Warn(_))));
    // 证据已失效：之后再有扫描也不重复退订。
    let mut again = Vec::new();
    m.release_idle_children(None, &mut again);
    assert!(unsubscribed(&again).is_empty());
}

#[test]
fn 根仍被认领时子轮结束不退订() {
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, C, "派生1", CT);
    let done = apply(&mut m, mock.turn_completed(C, CT, "completed"));
    assert!(unsubscribed(&done).is_empty());
    assert!(m.relations.attachments[C].valid);
}

#[test]
fn 根有活跃目标不退订时子线程也保留() {
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, C, "派生1", CT);
    apply(&mut m, mock.turn_completed(C, CT, "completed"));
    let token = m.token(P).unwrap();
    parent_completed(&mut m);
    m.record(P).goal_active = true;
    let a = apply(
        &mut m,
        Event::Timer {
            token,
            kind: TimerKind::Unsubscribe,
        },
    );
    assert!(unsubscribed(&a).is_empty());
    assert!(m.relations.attachments[C].valid);
    // 根仍订阅（未卸载）：任何扫描都不退子线程。
    let mut scan = Vec::new();
    m.release_idle_children(None, &mut scan);
    assert!(unsubscribed(&scan).is_empty());
}

#[test]
fn 父轮先结束触发整树停止期间不退订子线程() {
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, C, "派生1", CT);
    let a = parent_completed(&mut m);
    assert!(
        matches!(m.claims[P].state, State::FinishingTree { .. }),
        "子代理仍在跑：父认领转入整树收尾，不直接释放"
    );
    assert!(m.tree_busy(Some(C)));
    assert!(unsubscribed(&a).is_empty());
    let mut scan = Vec::new();
    m.release_idle_children(None, &mut scan);
    assert!(unsubscribed(&scan).is_empty());
    assert!(m.relations.attachments[C].valid);
}

#[test]
fn 退订后旧轮次通知不恢复证据也不重复退订() {
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, C, "派生1", CT);
    apply(&mut m, mock.turn_completed(C, CT, "completed"));
    assert_eq!(
        unsubscribed(&release_root(&mut m)),
        vec![P.to_owned(), C.to_owned()]
    );
    // 退订后到达的事件可能使用旧快照，不能证明已经重新附着。
    let started = apply(&mut m, mock.turn_started(C, "子轮2"));
    assert!(unsubscribed(&started).is_empty());
    assert!(!m.relations.attachments[C].valid);
    // 新轮未完成：此时再扫也不退。
    let mut scan = Vec::new();
    m.release_idle_children(None, &mut scan);
    assert!(unsubscribed(&scan).is_empty());
    let done = apply(&mut m, mock.turn_completed(C, "子轮2", "completed"));
    assert!(unsubscribed(&done).is_empty());
    let dup = apply(&mut m, mock.turn_completed(C, "子轮2", "completed"));
    assert!(unsubscribed(&dup).is_empty(), "重复的完成通知不再退订");
}

#[test]
fn 子线程自身有活跃目标时保留_清理结束后再扫才退订() {
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, C, "派生1", CT);
    apply(&mut m, mock.turn_completed(C, CT, "completed"));
    m.record(C).goal_active = true;
    let a = release_root(&mut m);
    assert_eq!(unsubscribed(&a), vec![P.to_owned()]);
    m.record(C).goal_active = false;
    let mut cleanup = Vec::new();
    m.relations_end_cleanup(R, &mut cleanup);
    assert_eq!(unsubscribed(&cleanup), vec![C.to_owned()]);
}

#[test]
fn 孙线程随根一并退订() {
    const G: &str = "孙线程";
    const GT: &str = "孙轮";
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, C, "派生1", CT);
    spawn(&mut m, &mut mock, C, CT, G, "派生2", GT);
    assert_eq!(
        m.thread_relation(G).expect("孙线程血缘").parent_thread_id,
        C
    );
    apply(&mut m, mock.turn_completed(G, GT, "completed"));
    apply(&mut m, mock.turn_completed(C, CT, "completed"));
    let mut got = unsubscribed(&release_root(&mut m));
    got.sort();
    let mut want = vec![P.to_owned(), C.to_owned(), G.to_owned()];
    want.sort();
    assert_eq!(got, want);
}

#[test]
fn 不属于本_broker_运行的线程不退订() {
    const LONE: &str = "无血缘线程";
    const FOREIGN_PARENT: &str = "外部父线程";
    const FOREIGN_CHILD: &str = "外部子线程";
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, C, "派生1", CT);
    apply(&mut m, mock.turn_completed(C, CT, "completed"));
    // 无父边的线程与父线程从未跑过本 broker 运行的子线程：都有有效附着证据，但不归本 broker 管。
    m.relations.edges.insert(
        FOREIGN_CHILD.into(),
        RelationEdge {
            child_thread_id: FOREIGN_CHILD.into(),
            parent_thread_id: FOREIGN_PARENT.into(),
            kind: RelationKind::Subagent,
            source_summary: json!({}),
            can_accept_direct_input: None,
            agent_version: AgentVersion::Unknown,
            discovered_by: DiscoveredBy::Started,
        },
    );
    for (thread, turn) in [(LONE, "甲轮"), (FOREIGN_CHILD, "乙轮")] {
        apply(
            &mut m,
            notify(
                "turn/started",
                json!({"threadId":thread,"turn":{"id":turn,"status":"inProgress","items":[]}}),
            ),
        );
        let done = apply(
            &mut m,
            notify(
                "turn/completed",
                json!({"threadId":thread,"turn":{"id":turn,"status":"completed","items":[]}}),
            ),
        );
        assert!(unsubscribed(&done).is_empty());
        assert!(m.relations.attachments[thread].valid);
    }
    assert_eq!(
        unsubscribed(&release_root(&mut m)),
        vec![P.to_owned(), C.to_owned()]
    );
    assert!(m.relations.attachments[LONE].valid);
    assert!(m.relations.attachments[FOREIGN_CHILD].valid);
}

#[test]
fn 关闭中不退订子线程() {
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, C, "派生1", CT);
    apply(&mut m, mock.turn_completed(C, CT, "completed"));
    parent_completed(&mut m);
    // 根已退订、子线程本可释放：只剩关闭这一条挡住。
    m.record(P).subscribed = false;
    let mut open = Vec::new();
    m.clone().release_idle_children(None, &mut open);
    assert_eq!(unsubscribed(&open), vec![C.to_owned()]);
    m.closing = true;
    let mut a = Vec::new();
    m.release_idle_children(None, &mut a);
    assert!(unsubscribed(&a).is_empty());
}

// ─── 退订后再开运行：重新订阅仍加载的子线程（E6 实测：未订阅时父代理派活，子线程的轮次与输出都收不到）───

/// 在 P 上用连接 1 再开一个运行，返回动作。
fn start_again(m: &mut Machine, run: &str, id: u64) -> Vec<Action> {
    apply(
        m,
        Event::ClientRequest {
            request: request(
                1,
                id,
                "turn/start",
                json!({"threadId":P,"input":[{"type":"text","text":"续做"}]}),
            ),
            run_id: Some(run.into()),
        },
    )
}

fn rpc(actions: &[Action], method: &str, thread: &str) -> RequestContext {
    actions
        .iter()
        .find_map(|a| match a {
            Action::Request(c)
                if c.method == method
                    && c.params.as_ref().is_some_and(|p| p["threadId"] == thread) =>
            {
                Some(c.clone())
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("应发出 {method}：{thread}，实际 {actions:?}"))
}

fn has_rpc(actions: &[Action], method: &str, thread: &str) -> bool {
    actions.iter().any(|a| matches!(a, Action::Request(c) if c.method == method && c.params.as_ref().is_some_and(|p| p["threadId"] == thread)))
}

fn pending_release() -> (Machine, SubagentMock, RequestContext) {
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, C, "派生1", CT);
    apply(&mut m, mock.turn_completed(C, CT, "completed"));
    let a = release_root(&mut m);
    assert_eq!(unsubscribed(&a), vec![P.to_owned(), C.to_owned()]);
    (m, mock, rpc(&a, "thread/unsubscribe", C))
}

fn released() -> (Machine, SubagentMock) {
    let (mut m, mock, context) = pending_release();
    reply(&mut m, &context, Ok(json!({"status":"unsubscribed"})));
    assert!(m.relations.aux.released.contains_key(C));
    (m, mock)
}

fn loaded(m: &mut Machine, actions: &[Action]) -> Vec<Action> {
    let read = rpc(actions, "thread/read", C);
    reply(
        m,
        &read,
        Ok(json!({"thread":{"id":C,"status":{"type":"idle"}}})),
    )
}

fn resume_ok(m: &mut Machine, context: &RequestContext) -> Vec<Action> {
    reply(
        m,
        context,
        Ok(json!({"thread":{"id":C,"status":{"type":"idle"}}})),
    )
}

#[test]
fn 根再开运行先确认子线程挂回_两道屏障两种完成顺序() {
    for parent_first in [false, true] {
        let (mut m, _) = released();
        let a = start_again(&mut m, "第二运行", 2);
        assert!(!has_rpc(&a, "turn/start", P));
        let parent = rpc(&a, "thread/resume", P);
        let a = loaded(&mut m, &a);
        let child = rpc(&a, "thread/resume", C);
        if parent_first {
            let a = reply(&mut m, &parent, Ok(json!({"thread":{"id":P}})));
            assert!(!has_rpc(&a, "turn/start", P));
            assert!(has_rpc(&resume_ok(&mut m, &child), "turn/start", P));
        } else {
            let a = resume_ok(&mut m, &child);
            assert!(!has_rpc(&a, "turn/start", P));
            assert!(has_rpc(
                &reply(&mut m, &parent, Ok(json!({"thread":{"id":P}}))),
                "turn/start",
                P
            ));
        }
        assert!(m.relations.attachments[C].valid);
        assert!(
            m.relations.aux.released.contains_key(C),
            "挂回后仍保留释放责任，只有确认卸载才删除"
        );
        assert!(m.relations.aux.child_barriers.is_empty());
        assert!(!m.records.get(C).is_some_and(|r| r.subscribed));
    }
}

#[test]
fn 已卸载的子线程不再重新订阅() {
    let (mut m, _) = released();
    apply(&mut m, notify("thread/closed", json!({"threadId":C})));
    assert!(!m.relations.aux.released.contains_key(C));
    let a = start_again(&mut m, "第二运行", 2);
    assert!(!has_rpc(&a, "thread/read", C));
    assert!(!has_rpc(&a, "thread/resume", C));
}

#[test]
fn 三态下广播和旧通知旧服务端请求都不能改变订阅责任() {
    for phase in 0..3 {
        let (mut m, _, unsub) = pending_release();
        if phase > 0 {
            reply(&mut m, &unsub, Ok(json!({"status":"unsubscribed"})));
        }
        if phase == 2 {
            let a = start_again(&mut m, "第二运行", 2);
            loaded(&mut m, &a);
        }
        let before = m.relations.aux.released[C].clone();
        for (method, params) in [
            (
                "thread/status/changed",
                json!({"threadId":C,"status":{"type":"active"}}),
            ),
            (
                "thread/goal/updated",
                json!({"threadId":C,"goal":{"status":"active"}}),
            ),
            ("thread/goal/cleared", json!({"threadId":C})),
            (
                "thread/started",
                json!({"thread":{"id":C,"status":{"type":"idle"}}}),
            ),
            (
                "thread/name/updated",
                json!({"threadId":C,"threadName":"迟到"}),
            ),
            ("turn/started", json!({"threadId":C,"turn":{"id":"旧轮"}})),
        ] {
            apply(&mut m, notify(method, params));
        }
        apply(
            &mut m,
            Event::ServerRequest {
                id: json!(99),
                forward_id: "旧请求".into(),
                method: "item/commandExecution/requestApproval".into(),
                params: Some(json!({"threadId":C,"turnId":"旧轮","itemId":"旧审批"})),
            },
        );
        assert_eq!(m.relations.aux.released[C], before);
        assert!(!m.relations.attachments[C].valid);
    }
}

#[test]
fn 卸载状态广播不能划掉名单_只读确认未加载才清除() {
    let (mut m, _) = released();
    apply(
        &mut m,
        notify(
            "thread/status/changed",
            json!({"threadId":C,"status":{"type":"notLoaded"}}),
        ),
    );
    assert!(m.relations.aux.released.contains_key(C));
    let a = start_again(&mut m, "第二运行", 2);
    let read = rpc(&a, "thread/read", C);
    let a = reply(
        &mut m,
        &read,
        Ok(json!({"thread":{"id":C,"status":{"type":"notLoaded"}}})),
    );
    assert!(!m.relations.aux.released.contains_key(C));
    assert!(!has_rpc(&a, "thread/resume", C));
}

#[test]
fn 子线程又收到本连接轮次事件仍保留责任并重新核实() {
    let (mut m, mut mock) = released();
    apply(&mut m, mock.turn_started(C, "子轮2"));
    assert!(m.relations.aux.released.contains_key(C));
    assert!(!m.relations.attachments[C].valid);
    assert!(has_rpc(
        &start_again(&mut m, "第二运行", 2),
        "thread/read",
        C
    ));
}

#[test]
fn 别的线程开运行不挂回本根的子线程() {
    let (mut m, _) = released();
    let a = apply(
        &mut m,
        Event::ClientRequest {
            request: request(
                1,
                2,
                "turn/start",
                json!({"threadId":"别的线程","input":[]}),
            ),
            run_id: Some("别的运行".into()),
        },
    );
    assert!(!has_rpc(&a, "thread/resume", C));
    assert!(!has_rpc(&a, "thread/read", C));
    assert!(m.relations.aux.released.contains_key(C));
}

#[test]
fn 重新订阅报错本次开工失败_责任保留且下次可重试() {
    for message in [
        "临时失败",
        "thread 子线程 is closing; retry thread/resume after the thread is closed",
        "no rollout found for thread id 子线程",
        "已附着后构造响应失败",
    ] {
        let (mut m, _) = released();
        let a = start_again(&mut m, "第二运行", 2);
        let parent = rpc(&a, "thread/resume", P);
        reply(&mut m, &parent, Ok(json!({"thread":{"id":P}})));
        let a = loaded(&mut m, &a);
        let child = rpc(&a, "thread/resume", C);
        let a = reply(
            &mut m,
            &child,
            Err(json!({"code":-32603,"message":message})),
        );
        assert!(!has_rpc(&a, "turn/start", P));
        assert!(!m.claims.contains_key(P));
        assert!(m.stream_slots.is_empty());
        assert!(m.relations.aux.child_barriers.is_empty());
        assert!(m.relations.aux.released.contains_key(C));
        assert!(a.iter().any(|a| matches!(a,Action::Send {message,..} if message["error"]["message"].as_str().is_some_and(|s|s.contains("子代理线程 子线程 恢复订阅失败")))));
        let unsub = rpc(&a, "thread/unsubscribe", C);
        reply(&mut m, &unsub, Ok(json!({"status":"notSubscribed"})));
        let a = start_again(&mut m, "第三运行", 3);
        let a = loaded(&mut m, &a);
        let child = rpc(&a, "thread/resume", C);
        let a = resume_ok(&mut m, &child);
        assert!(has_rpc(&a, "turn/start", P));
    }
}

#[test]
fn 子线程重新加载广播仍不能划掉名单() {
    let (mut m, _) = released();
    apply(
        &mut m,
        notify(
            "thread/started",
            json!({"thread":{"id":C,"status":{"type":"idle"}}}),
        ),
    );
    assert!(m.relations.aux.released.contains_key(C));
    assert!(!m.relations.attachments[C].valid);
}

fn discover(m: &mut Machine) -> (u64, Vec<Action>) {
    let mut a = Vec::new();
    let id = m.begin_discovery(
        P,
        crate::relations::DiscoveryScope::Subtree,
        crate::relations::DiscoveryCaller::TreeStop {
            tree_op: 1,
            round: 1,
        },
        &mut a,
    );
    (id, a)
}

#[test]
fn 发现同步登记_挂回成功后才发送来源查询且不重新起算期限() {
    let (mut m, _) = released();
    let (id, a) = discover(&mut m);
    let deadline = m.relations.discoveries[&id].deadline_ms;
    assert!(!a.iter().any(
        |a| matches!(a,Action::Request(c) if matches!(c.operation,Operation::Discovery {..}))
    ));
    let a = loaded(&mut m, &a);
    let child = rpc(&a, "thread/resume", C);
    let a = resume_ok(&mut m, &child);
    assert!(a.iter().any(|a|matches!(a,Action::Request(c) if matches!(c.operation,Operation::Discovery {op_id,..} if op_id==id))));
    assert_eq!(m.relations.discoveries[&id].deadline_ms, deadline);
    assert!(!has_rpc(&a, "thread/unsubscribe", C), "发现需要它时不补退");
}

#[test]
fn 开工总期限覆盖旧退订_超时不开轮不丢责任() {
    let (mut m, _, _) = pending_release();
    let a = start_again(&mut m, "第二运行", 2);
    let parent = rpc(&a, "thread/resume", P);
    reply(&mut m, &parent, Ok(json!({"thread":{"id":P}})));
    let waiter = m
        .relations
        .aux
        .child_barriers
        .keys()
        .next()
        .unwrap()
        .clone();
    let a = apply(
        &mut m,
        Event::Timer {
            token: ClaimToken {
                thread_id: P.into(),
                claim_id: 0,
            },
            kind: TimerKind::ChildBarrier { waiter },
        },
    );
    assert!(!has_rpc(&a, "turn/start", P));
    assert!(!m.claims.contains_key(P));
    assert!(m.relations.aux.released.contains_key(C));
    assert!(m.relations.aux.child_barriers.is_empty());
}

#[test]
fn 退订结果未知不能恢复附着_后续恢复成功才开工() {
    let (mut m, _, unsub) = pending_release();
    let a = start_again(&mut m, "第二运行", 2);
    let parent = rpc(&a, "thread/resume", P);
    reply(&mut m, &parent, Ok(json!({"thread":{"id":P}})));
    let a = apply(
        &mut m,
        Event::TransportFailed {
            context: unsub,
            error: json!({"message":"本地超时"}),
        },
    );
    assert!(!m.relations.attachments[C].valid);
    assert!(!has_rpc(&a, "turn/start", P));
    let a = loaded(&mut m, &a);
    let child = rpc(&a, "thread/resume", C);
    assert!(has_rpc(&resume_ok(&mut m, &child), "turn/start", P));
}

#[test]
fn 迟到成功和报错都不覆盖后续退订_淘汰回包登记仍补偿() {
    for outcome in [
        Ok(json!({"thread":{"id":C}})),
        Err(json!({"message":"附着后失败"})),
    ] {
        let (mut m, _) = released();
        let a = start_again(&mut m, "第二运行", 2);
        let a = loaded(&mut m, &a);
        let child = rpc(&a, "thread/resume", C);
        let waiter = m
            .relations
            .aux
            .child_barriers
            .keys()
            .next()
            .unwrap()
            .clone();
        let a = apply(
            &mut m,
            Event::Timer {
                token: ClaimToken {
                    thread_id: P.into(),
                    claim_id: 0,
                },
                kind: TimerKind::ChildBarrier { waiter },
            },
        );
        let compensation = rpc(&a, "thread/unsubscribe", C);
        reply(&mut m, &compensation, Ok(json!({"status":"unsubscribed"})));
        let latest = m.relations.aux.released[C].clone();
        apply(
            &mut m,
            Event::LateReply {
                context: child.clone(),
                result: outcome,
            },
        );
        apply(
            &mut m,
            Event::LateEvicted {
                context: child,
                deadline: Instant::now(),
            },
        );
        assert_eq!(
            m.relations.aux.released[C], latest,
            "旧回包不得把后续已确认退订改为未知，也不得重新安排补偿"
        );
        assert!(!m.relations.attachments[C].valid);
    }
}

#[test]
fn 两个等待者共享挂回_取消其中一个不影响另一个() {
    let (mut m, _) = released();
    let a = start_again(&mut m, "第二运行", 2);
    let (discovery, more) = discover(&mut m);
    assert!(!has_rpc(&more, "thread/read", C));
    let a = loaded(&mut m, &a);
    let child = rpc(&a, "thread/resume", C);
    let a = apply(&mut m, Event::Disconnected { conn: 1 });
    assert!(!has_rpc(&a, "thread/unsubscribe", C));
    assert!(m
        .relations
        .aux
        .child_barriers
        .contains_key(&crate::relations::ChildWaiter::Discovery(discovery)));
    let a = resume_ok(&mut m, &child);
    assert!(!has_rpc(&a, "turn/start", P));
    assert!(!has_rpc(&a, "thread/unsubscribe", C));
    assert!(a.iter().any(
        |a| matches!(a,Action::Request(c) if matches!(c.operation,Operation::Discovery {..}))
    ));
}

#[test]
fn 三种退订成功状态_not_loaded才清除责任() {
    for status in ["unsubscribed", "notSubscribed", "notLoaded"] {
        let (mut m, _, unsub) = pending_release();
        reply(&mut m, &unsub, Ok(json!({"status":status})));
        assert_eq!(
            m.relations.aux.released.contains_key(C),
            status != "notLoaded"
        );
    }
}

#[test]
fn closed只在已确认时放行_未确认订阅操作先保留责任() {
    for phase in 0..3 {
        let (mut m, _, unsub) = pending_release();
        if phase > 0 {
            reply(&mut m, &unsub, Ok(json!({"status":"unsubscribed"})));
        }
        let a = start_again(&mut m, "第二运行", 2);
        let child = if phase == 2 {
            Some(rpc(&loaded(&mut m, &a), "thread/resume", C))
        } else {
            None
        };
        let closed = apply(&mut m, notify("thread/closed", json!({"threadId":C})));
        assert_eq!(m.relations.aux.released.contains_key(C), phase != 1);
        if phase == 0 {
            let probe = rpc(&closed, "thread/read", C);
            reply(
                &mut m,
                &probe,
                Ok(json!({"thread":{"id":C,"status":{"type":"notLoaded"}}})),
            );
            assert!(!m.relations.aux.released.contains_key(C));
        }
        if let Some(child) = child {
            assert!(!m.relations.aux.child_barriers.is_empty());
            resume_ok(&mut m, &child);
            assert!(m.relations.attachments[C].valid);
            assert!(m.relations.aux.child_barriers.is_empty());
        }
    }
}

#[test]
fn 名单满不淘汰旧责任也不新增退订() {
    let (mut m, _) = released();
    let entry = m.relations.aux.released[C].clone();
    for i in 1..crate::relations::MAX_ATTACHMENTS {
        m.relations
            .aux
            .released
            .insert(format!("保留{i}"), entry.clone());
    }
    let child = "新的子线程";
    m.relations.edges.insert(
        child.into(),
        RelationEdge {
            child_thread_id: child.into(),
            parent_thread_id: P.into(),
            kind: RelationKind::Subagent,
            source_summary: Value::Null,
            can_accept_direct_input: Some(true),
            agent_version: AgentVersion::V1,
            discovered_by: DiscoveredBy::Started,
        },
    );
    m.attach(child, crate::relations::AttachmentSource::ThreadStarted);
    let mut a = Vec::new();
    m.release_idle_children(Some(child), &mut a);
    assert!(!has_rpc(&a, "thread/unsubscribe", child));
    assert_eq!(
        m.relations.aux.released.len(),
        crate::relations::MAX_ATTACHMENTS
    );
    assert_eq!(m.relations.aux.released[C], entry);
    assert!(m.relations.attachments[child].valid);
}

#[test]
fn 关闭清除等待集合与计时器() {
    let (mut m, _) = released();
    start_again(&mut m, "第二运行", 2);
    discover(&mut m);
    let a = apply(&mut m, Event::Shutdown);
    assert!(m.relations.aux.child_barriers.is_empty());
    assert!(m.claims.is_empty());
    assert!(a.iter().any(|a| matches!(
        a,
        Action::Unschedule {
            key: TimerKey::ChildBarrier(_)
        }
    )));
    assert!(a.iter().any(|a| matches!(
        a,
        Action::Unschedule {
            key: TimerKey::ChildSubscription(_)
        }
    )));
}

#[test]
fn 普通独立子线程退订同样登记_自身恢复成功与原父复用一致() {
    let (mut m, _) = released();
    // 独立运行成功恢复后，普通退订计时器必须走同一责任表。
    let a = apply(
        &mut m,
        Event::ClientRequest {
            request: request(1, 2, "turn/start", json!({"threadId":C,"input":[]})),
            run_id: Some("子独立运行".into()),
        },
    );
    let own = rpc(&a, "thread/resume", C);
    let a = resume_ok(&mut m, &own);
    assert!(has_rpc(&a, "turn/start", C));
    let start = rpc(&a, "turn/start", C);
    reply(&mut m, &start, Ok(json!({"turn":{"id":"独立轮"}})));
    let token = m.token(C).unwrap();
    apply(
        &mut m,
        notify(
            "turn/completed",
            json!({"threadId":C,"turn":{"id":"独立轮","status":"completed"}}),
        ),
    );
    let a = apply(
        &mut m,
        Event::Timer {
            token,
            kind: TimerKind::Unsubscribe,
        },
    );
    let unsub = rpc(&a, "thread/unsubscribe", C);
    reply(&mut m, &unsub, Ok(json!({"status":"unsubscribed"})));
    assert!(m.relations.aux.released.contains_key(C));
    let a = start_again(&mut m, "父再复用", 3);
    assert!(has_rpc(&a, "thread/read", C));
}

#[test]
fn 未加载读取的精确错误可以清除责任_其他错误不可以() {
    for message in [
        "thread not loaded: 子线程",
        "no rollout found for thread id 子线程",
        "读取暂时失败",
    ] {
        let (mut m, _) = released();
        let a = start_again(&mut m, "第二运行", 2);
        let read = rpc(&a, "thread/read", C);
        let a = reply(&mut m, &read, Err(json!({"message":message})));
        assert_eq!(
            m.relations.aux.released.contains_key(C),
            message != "thread not loaded: 子线程"
        );
        assert!(!has_rpc(&a, "thread/resume", C));
    }
}

#[test]
fn 状态变化广播不算重新附着_仍在名单里并在再开运行时挂回() {
    let (mut m, _) = released();
    apply(
        &mut m,
        notify(
            "thread/status/changed",
            json!({"threadId":C,"status":{"type":"active","activeFlags":[]}}),
        ),
    );
    assert!(!m.relations.attachments[C].valid);
    assert!(m.relations.aux.released.contains_key(C));
    let a = start_again(&mut m, "第二运行", 2);
    assert!(has_rpc(&loaded(&mut m, &a), "thread/resume", C));
}

#[test]
fn 目标非活跃通知直接触发补退_不需要人工扫描() {
    for method in ["thread/goal/cleared", "thread/goal/updated"] {
        let (mut m, mut mock) = setup();
        spawn(&mut m, &mut mock, P, T, C, "派生1", CT);
        apply(&mut m, mock.turn_completed(C, CT, "completed"));
        m.record(C).goal_active = true;
        assert!(!unsubscribed(&release_root(&mut m)).contains(&C.to_owned()));
        let a = apply(
            &mut m,
            notify(method, json!({"threadId":C,"goal":{"status":"completed"}})),
        );
        assert!(has_rpc(&a, "thread/unsubscribe", C));
    }
}

#[test]
fn 最后待答请求消失后补退() {
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, C, "派生1", CT);
    apply(&mut m, mock.turn_completed(C, CT, "completed"));
    m.record(C).goal_active = true;
    release_root(&mut m);
    apply(
        &mut m,
        Event::ServerRequest {
            id: json!(99),
            forward_id: "待答".into(),
            method: "item/commandExecution/requestApproval".into(),
            params: Some(json!({"threadId":C,"turnId":"待答轮","itemId":"审批"})),
        },
    );
    assert!(!m.relations.requests.is_empty());
    let a = apply(&mut m, notify("thread/goal/cleared", json!({"threadId":C})));
    assert!(!has_rpc(&a, "thread/unsubscribe", C));
    let a = apply(
        &mut m,
        notify(
            "serverRequest/resolved",
            json!({"threadId":C,"requestId":99}),
        ),
    );
    assert!(m.relations.requests.is_empty());
    assert!(has_rpc(&a, "thread/unsubscribe", C));
}

#[test]
fn 续跑通过预检后仍受子线程开工屏障保护() {
    let (mut m, _) = released();
    let old = m.runs.runs.get_mut(R).unwrap();
    old.state = crate::runs::RunState::Ended;
    old.interrupt_reason = Some(crate::runs::InterruptReason::Stop);
    old.stop_outcome = Some(crate::runs::StopOutcome::Confirmed);
    let mut a = apply(
        &mut m,
        Event::ClientRequest {
            request: request(
                1,
                2,
                "turn/start",
                json!({"threadId":P,"input":[],"cxbContinue":{"fromRunId":R}}),
            ),
            run_id: Some("续跑运行".into()),
        },
    );
    let mut all = a.clone();
    for _ in 0..15 {
        let requests: Vec<_> = a
            .iter()
            .filter_map(|a| match a {
                Action::Request(c)
                    if matches!(
                        c.operation,
                        Operation::ContinueResume
                            | Operation::ContinueRead(_)
                            | Operation::StatusRead { .. }
                    ) =>
                {
                    Some(c.clone())
                }
                _ => None,
            })
            .collect();
        if requests.is_empty() {
            break;
        }
        a = Vec::new();
        for context in requests {
            let result = if context.method == "thread/goal/get" {
                json!({"goal":null})
            } else {
                json!({"thread":{"id":P,"status":{"type":"idle"}}})
            };
            a.extend(reply(&mut m, &context, Ok(result)));
        }
        all.extend(a.clone());
    }
    assert!(!has_rpc(&all, "turn/start", P));
    assert!(!m.claims[P].precheck(), "父预检已完成，子挂回仍保护开工");
    let a = loaded(&mut m, &all);
    let child = rpc(&a, "thread/resume", C);
    assert!(has_rpc(&resume_ok(&mut m, &child), "turn/start", P));
}

#[test]
fn 实际转发时确定提交位置_屏障前外部轮次及溢出不能取得归属() {
    for overflow in [false, true] {
        let (mut m, _) = released();
        m.shared = true;
        m.record(P).subscribed = true;
        let a = start_again(&mut m, "第二运行", 2);
        apply(
            &mut m,
            notify(
                "turn/started",
                json!({"threadId":P,"turn":{"id":"外部轮","status":"inProgress"}}),
            ),
        );
        if overflow {
            m.settling_overflow(P, &mut Vec::new());
        }
        assert!(matches!(
            &m.claims[P].state,
            State::Starting {
                submitted_at: 0,
                ..
            }
        ));
        let a = loaded(&mut m, &a);
        let child = rpc(&a, "thread/resume", C);
        let a = resume_ok(&mut m, &child);
        let start = rpc(&a, "turn/start", P);
        match &m.claims[P].state {
            State::Starting {
                submitted_at, held, ..
            } => assert_eq!(*submitted_at, held.len()),
            _ => panic!("仍应等待开轮回包"),
        }
        reply(&mut m, &start, Ok(json!({"turn":{"id":"外部轮"}})));
        assert!(!m.claims[P].owned_turns.contains("外部轮"));
        assert!(
            m.claims[P].joined() || m.claims[P].settling(),
            "可以等待核实，但不能授予本次运行归属"
        );
    }
}

#[test]
fn 显式停止取消屏障并完成对账_迟到回包不能开轮() {
    let (mut m, _) = released();
    let a = start_again(&mut m, "第二运行", 2);
    let parent = rpc(&a, "thread/resume", P);
    let a = loaded(&mut m, &a);
    let child = rpc(&a, "thread/resume", C);
    let mut all = apply(
        &mut m,
        Event::ClientRequest {
            request: request(1, 3, "broker/threads/stop", json!({"threadId":P})),
            run_id: None,
        },
    );
    assert!(m.relations.aux.child_barriers.is_empty());
    assert!(m.claims.get(P).is_none_or(|c| c.pending_start.is_none()));
    all.extend(reply(&mut m, &parent, Ok(json!({"thread":{"id":P}}))));
    all.extend(resume_ok(&mut m, &child));
    assert!(!has_rpc(&all, "turn/start", P));
    assert!(all.iter().any(|a| matches!(
        a,
        Action::Unschedule {
            key: TimerKey::ChildBarrier(_)
        }
    )));
}

#[test]
fn 发现挂回失败保持缺附着证据且总期限后结束() {
    let (mut m, _) = released();
    let (id, a) = discover(&mut m);
    let generation = m.relations.discoveries[&id].generation;
    let a = loaded(&mut m, &a);
    let child = rpc(&a, "thread/resume", C);
    reply(&mut m, &child, Err(json!({"message":"挂回失败"})));
    apply(
        &mut m,
        Event::Timer {
            token: ClaimToken {
                thread_id: P.into(),
                claim_id: 0,
            },
            kind: TimerKind::DiscoveryDeadline {
                op_id: id,
                generation,
            },
        },
    );
    assert!(!m.relations.discoveries.contains_key(&id));
    assert!(m.relations.aux.child_barriers.is_empty());
    let result = m.discovery_outcome(id).unwrap();
    assert!(result
        .state
        .incomplete_reasons
        .iter()
        .any(|r| r == "attachmentMissing"));
    assert!(result.state.missing[C]
        .iter()
        .any(|r| r == "attachmentMissing"));
}

#[test]
fn 补偿失败有界退避_后续新需要保留重试责任() {
    let (mut m, _) = released();
    let a = start_again(&mut m, "第二运行", 2);
    let a = loaded(&mut m, &a);
    let child = rpc(&a, "thread/resume", C);
    let mut a = reply(&mut m, &child, Err(json!({"message":"已附着后失败"})));
    for _ in 0..3 {
        if !has_rpc(&a, "thread/unsubscribe", C) {
            let r = m.relations.aux.released[C].clone();
            m.clock.mono_ms = r.retry_at.max(r.compensation_at);
            a = apply(
                &mut m,
                Event::Timer {
                    token: ClaimToken {
                        thread_id: C.into(),
                        claim_id: 0,
                    },
                    kind: TimerKind::ChildRetry {
                        thread: C.into(),
                        op: r.op,
                    },
                },
            );
        }
        let unsub = rpc(&a, "thread/unsubscribe", C);
        a = reply(&mut m, &unsub, Err(json!({"message":"退订响应未知"})));
    }
    assert!(m.relations.aux.released.contains_key(C));
    assert!(!has_rpc(&a, "thread/unsubscribe", C));
    assert_eq!(
        m.relations.aux.released[C].failures, 1,
        "补偿退订错误不增加连续挂回失败次数"
    );
    let a = start_again(&mut m, "重新需要", 3);
    assert!(!has_rpc(&a, "turn/start", P));
    assert!(has_rpc(&a, "thread/read", C));
}

#[test]
fn 归档删除无关闭通知时用只读核实退役() {
    for method in ["thread/archived", "thread/deleted"] {
        let (mut m, _) = released();
        let a = apply(&mut m, notify(method, json!({"threadId":C})));
        let read = rpc(&a, "thread/read", C);
        reply(
            &mut m,
            &read,
            Err(json!({"message":"thread not loaded: 子线程"})),
        );
        assert!(!m.relations.aux.released.contains_key(C));
    }
}

#[test]
fn 旧自身恢复成功不能覆盖后续退订和未加载确认() {
    for unloaded in [false, true] {
        let (mut m, _) = released();
        let a = apply(
            &mut m,
            Event::ClientRequest {
                request: request(1, 2, "turn/start", json!({"threadId":C,"input":[]})),
                run_id: Some("独立".into()),
            },
        );
        let own = rpc(&a, "thread/resume", C);
        let a = apply(&mut m, Event::Disconnected { conn: 1 });
        let unsub = rpc(&a, "thread/unsubscribe", C);
        reply(
            &mut m,
            &unsub,
            Ok(json!({"status":if unloaded {"notLoaded"} else {"unsubscribed"}})),
        );
        apply(
            &mut m,
            Event::LateReply {
                context: own,
                result: Ok(json!({"thread":{"id":C}})),
            },
        );
        assert!(!m.records.get(C).is_some_and(|r| r.subscribed));
        assert!(!m.relations.attachments[C].valid);
    }
}

#[test]
fn 一名等待者到期不取消另一名的独立期限() {
    let (mut m, _) = released();
    let a = start_again(&mut m, "第二运行", 2);
    let a = loaded(&mut m, &a);
    let child = rpc(&a, "thread/resume", C);
    let run_waiter = m
        .relations
        .aux
        .child_barriers
        .keys()
        .next()
        .unwrap()
        .clone();
    m.clock.mono_ms += 5_000;
    let (discovery, _) = discover(&mut m);
    let op = m.relations.aux.released[C].op;
    m.clock.mono_ms += 10_000;
    apply(
        &mut m,
        Event::Timer {
            token: ClaimToken {
                thread_id: P.into(),
                claim_id: 0,
            },
            kind: TimerKind::ChildBarrier { waiter: run_waiter },
        },
    );
    let a = apply(
        &mut m,
        Event::Timer {
            token: ClaimToken {
                thread_id: C.into(),
                claim_id: 0,
            },
            kind: TimerKind::ChildSubscription {
                thread: C.into(),
                op,
            },
        },
    );
    assert!(!has_rpc(&a, "thread/unsubscribe", C));
    assert!(m
        .relations
        .aux
        .child_barriers
        .contains_key(&crate::relations::ChildWaiter::Discovery(discovery)));
    let a = resume_ok(&mut m, &child);
    assert!(!has_rpc(&a, "turn/start", P));
    assert!(a.iter().any(|a|matches!(a,Action::Request(c) if matches!(c.operation,Operation::Discovery {op_id,..} if op_id==discovery))));
}

#[test]
fn 挂回超时且迟到登记淘汰后没有后续运行也能最终撒手() {
    let (mut m, _) = released();
    let a = start_again(&mut m, "第二运行", 2);
    let a = loaded(&mut m, &a);
    let child = rpc(&a, "thread/resume", C);
    let waiter = m
        .relations
        .aux
        .child_barriers
        .keys()
        .next()
        .unwrap()
        .clone();
    let a = apply(
        &mut m,
        Event::Timer {
            token: ClaimToken {
                thread_id: P.into(),
                claim_id: 0,
            },
            kind: TimerKind::ChildBarrier { waiter },
        },
    );
    let unsub = rpc(&a, "thread/unsubscribe", C);
    apply(
        &mut m,
        Event::LateEvicted {
            context: child,
            deadline: Instant::now(),
        },
    );
    reply(&mut m, &unsub, Ok(json!({"status":"notSubscribed"})));
    assert!(!m.claims.contains_key(P));
    assert!(!m.relations.attachments[C].valid);
    assert_eq!(
        m.relations.aux.released[C].context.method,
        "thread/unsubscribe"
    );
}

#[test]
fn 连续挂回失败三次后按五十二十秒退避_成功清零() {
    let (mut m, _) = released();
    for failure in 1..=5 {
        let a = start_again(&mut m, &format!("失败运行{failure}"), failure + 1);
        let a = loaded(&mut m, &a);
        let child = rpc(&a, "thread/resume", C);
        let a = reply(&mut m, &child, Err(json!({"message":"暂时失败"})));
        assert!(!m.claims.contains_key(P));
        if failure < 3 {
            let unsub = rpc(&a, "thread/unsubscribe", C);
            reply(&mut m, &unsub, Ok(json!({"status":"unsubscribed"})));
        } else {
            let r = m.relations.aux.released[C].clone();
            assert_eq!(r.retry_at - m.clock.mono_ms, 5_000 << (failure - 3));
            let a = start_again(&mut m, &format!("过早重试{failure}"), failure + 10);
            assert!(!has_rpc(&a, "thread/read", C));
            assert!(!has_rpc(&a, "turn/start", P));
            assert!(!m.claims.contains_key(P));
            m.clock.mono_ms = r.retry_at;
        }
    }
    let a = start_again(&mut m, "恢复运行", 30);
    let a = loaded(&mut m, &a);
    let child = rpc(&a, "thread/resume", C);
    resume_ok(&mut m, &child);
    assert_eq!(m.relations.aux.released[C].failures, 0);
    assert_eq!(m.relations.aux.released[C].retry_at, 0);
}

/// 子线程独立运行结束，但其普通退订计时器尚未进入邮箱。
fn independent_child_before_unsubscribe() -> (Machine, SubagentMock, ClaimToken) {
    let (mut m, mock) = released();
    let a = apply(
        &mut m,
        Event::ClientRequest {
            request: request(1, 20, "turn/start", json!({"threadId":C,"input":[]})),
            run_id: Some("子线程独立运行".into()),
        },
    );
    let resumed = resume_ok(&mut m, &rpc(&a, "thread/resume", C));
    let start = rpc(&resumed, "turn/start", C);
    reply(&mut m, &start, Ok(json!({"turn":{"id":"独立轮"}})));
    let token = m.token(C).unwrap();
    let a = apply(
        &mut m,
        notify(
            "turn/completed",
            json!({"threadId":C,"turn":{"id":"独立轮","status":"completed"}}),
        ),
    );
    assert!(a.iter().any(|a|matches!(a,Action::Schedule {token:scheduled,kind:TimerKind::Unsubscribe,..} if *scheduled == token)));
    assert!(!m.claims.contains_key(C));
    assert!(m.records[C].subscribed);
    (m, mock, token)
}

#[test]
fn 审查一_父运行先登记后旧子退订计时器不得摘订_复用事件可达且结束后补退() {
    let (mut m, mut mock, old) = independent_child_before_unsubscribe();
    let a = start_again(&mut m, "父复用运行", 21);
    assert!(
        m.relations.aux.child_barriers.is_empty(),
        "子线程尚未撒手，无需挂回屏障"
    );
    let parent = rpc(&a, "thread/resume", P);
    let a = reply(&mut m, &parent, Ok(json!({"thread":{"id":P}})));
    let start = rpc(&a, "turn/start", P);
    reply(&mut m, &start, Ok(json!({"turn":{"id":"复用父轮"}})));
    apply(&mut m, mock.turn_started(P, "复用父轮"));
    assert!(
        !m.relation_needs_subscription(C),
        "尚未建立新运行对子线程的成员需求"
    );
    let a = apply(
        &mut m,
        Event::Timer {
            token: old,
            kind: TimerKind::Unsubscribe,
        },
    );
    assert!(
        !has_rpc(&a, "thread/unsubscribe", C),
        "祖先运行必须保护普通退订入口"
    );
    assert!(m.records[C].subscribed);
    assert!(m.relations.attachments[C].valid);

    // 模拟订阅传输边界：一旦发出退订，后续子事件就不再投递给 broker。
    let subscribed = !has_rpc(&a, "thread/unsubscribe", C);
    let (started, completed) =
        SubagentMock::v1_message("sendInput", P, "复用父轮", "复用", C, false);
    apply(&mut m, started);
    apply(&mut m, completed);
    if subscribed {
        apply(&mut m, mock.turn_started(C, "复用子轮"));
    }
    assert!(m.relations.aux.observed[C]
        .iter()
        .any(|t| t.turn == "复用子轮" && t.started));
    // v1 跟进不单凭线程血缘授予新轮归属；这里核对子事件确实到达 broker 的观察层。
    apply(&mut m, mock.turn_completed(C, "复用子轮", "completed"));
    let a = apply(&mut m, mock.turn_completed(P, "复用父轮", "completed"));
    assert!(
        has_rpc(&a, "thread/unsubscribe", C),
        "祖先运行结束后自动补退，不依赖子线程再来事件"
    );
    assert!(!m.records[C].subscribed);
    assert!(m.relations.aux.deferred_child_unsubscribes.is_empty());
    reply(
        &mut m,
        &rpc(&a, "thread/unsubscribe", C),
        Ok(json!({"status":"unsubscribed"})),
    );
}

#[test]
fn 审查一_未完成发现保护普通子退订_发现结束后补退() {
    let (mut m, _, old) = independent_child_before_unsubscribe();
    let (discovery, _) = discover(&mut m);
    let generation = m.relations.discoveries[&discovery].generation;
    let a = apply(
        &mut m,
        Event::Timer {
            token: old,
            kind: TimerKind::Unsubscribe,
        },
    );
    assert!(!has_rpc(&a, "thread/unsubscribe", C));
    assert!(m.records[C].subscribed);
    let a = apply(
        &mut m,
        Event::Timer {
            token: ClaimToken {
                thread_id: P.into(),
                claim_id: 0,
            },
            kind: TimerKind::DiscoveryDeadline {
                op_id: discovery,
                generation,
            },
        },
    );
    assert!(!m.relations.discoveries.contains_key(&discovery));
    assert!(has_rpc(&a, "thread/unsubscribe", C));
    assert!(m.relations.aux.deferred_child_unsubscribes.is_empty());
}

#[test]
fn 审查一_延后退订遇到新认领或关闭不遗留也不作用于新运行() {
    for shutdown in [false, true] {
        let (mut m, _, old) = independent_child_before_unsubscribe();
        start_again(&mut m, "父复用运行", 21);
        apply(
            &mut m,
            Event::Timer {
                token: old,
                kind: TimerKind::Unsubscribe,
            },
        );
        assert!(m.relations.aux.deferred_child_unsubscribes.contains_key(C));
        let a = if shutdown {
            apply(&mut m, Event::Shutdown)
        } else {
            apply(
                &mut m,
                Event::ClientRequest {
                    request: request(1, 22, "turn/start", json!({"threadId":C,"input":[]})),
                    run_id: Some("新的子独立运行".into()),
                },
            )
        };
        assert!(!has_rpc(&a, "thread/unsubscribe", C));
        assert!(m.relations.aux.deferred_child_unsubscribes.is_empty());
        if !shutdown {
            assert!(m.claims.contains_key(C));
        }
    }
}

/// 恢复尚未确认时撤销等待者，得到排在恢复之后的补偿退订。
fn cancelled_resume_with_compensation() -> (Machine, RequestContext, RequestContext) {
    let (mut m, _) = released();
    let a = start_again(&mut m, "取消的运行", 30);
    let a = loaded(&mut m, &a);
    let resume = rpc(&a, "thread/resume", C);
    let a = apply(&mut m, Event::Disconnected { conn: 1 });
    let unsubscribe = rpc(&a, "thread/unsubscribe", C);
    assert!(!m.claims.contains_key(P));
    assert!(m.relations.aux.child_barriers.is_empty());
    (m, resume, unsubscribe)
}

#[test]
fn 审查二_补偿退订未确认时closed保留责任_冷恢复后再次开工仍等屏障() {
    for unknown in [false, true] {
        let (mut m, resume, unsubscribe) = cancelled_resume_with_compensation();
        if unknown {
            apply(
                &mut m,
                Event::TransportFailed {
                    context: unsubscribe.clone(),
                    error: json!({"message":"补偿结果未知"}),
                },
            );
        }
        let op = m.relations.aux.released[C].op;
        let a = apply(&mut m, notify("thread/closed", json!({"threadId":C})));
        assert!(
            m.relations.aux.released.contains_key(C),
            "最后操作为补偿退订也不能提前结束责任"
        );
        assert_eq!(m.relations.aux.released[C].op, op);
        let probe = rpc(&a, "thread/read", C);
        apply(&mut m, Event::Connected { conn: 1 });
        apply(&mut m, Event::Initialized { conn: 1, cxb: true });
        let a = start_again(&mut m, "再次开工", 31);
        let parent = rpc(&a, "thread/resume", P);
        let a = reply(&mut m, &parent, Ok(json!({"thread":{"id":P}})));
        assert!(!has_rpc(&a, "turn/start", P));
        assert!(!m.relations.aux.child_barriers.is_empty());

        // 旧运行时关闭后，先前排队的恢复冷加载成功；它仍早于补偿退订，不能越过屏障。
        let a = apply(
            &mut m,
            Event::LateReply {
                context: resume,
                result: Ok(json!({"thread":{"id":C}})),
            },
        );
        assert!(!has_rpc(&a, "turn/start", P));
        assert_eq!(m.relations.aux.released[C].op, op);
        let a = reply(&mut m, &unsubscribe, Ok(json!({"status":"unsubscribed"})));
        assert!(!has_rpc(&a, "turn/start", P));
        let a = reply(
            &mut m,
            &probe,
            Ok(json!({"thread":{"id":C,"status":{"type":"idle"}}})),
        );
        let current_resume = rpc(&a, "thread/resume", C);
        assert!(!has_rpc(&a, "turn/start", P));
        let a = resume_ok(&mut m, &current_resume);
        let start = rpc(&a, "turn/start", P);
        assert!(m.relations.attachments[C].valid);
        assert!(m.relations.aux.child_barriers.is_empty());
        reply(&mut m, &start, Ok(json!({"turn":{"id":"再次父轮"}})));
        let a = apply(
            &mut m,
            notify(
                "turn/completed",
                json!({"threadId":P,"turn":{"id":"再次父轮","status":"completed"}}),
            ),
        );
        let final_unsubscribe = rpc(&a, "thread/unsubscribe", C);
        reply(
            &mut m,
            &final_unsubscribe,
            Ok(json!({"status":"notLoaded"})),
        );
        assert!(!m.relations.aux.released.contains_key(C));
        assert!(!m.relations.attachments[C].valid);
    }
}

#[test]
fn 审查二_closed后的有序只读可确认未加载_旧读不能越过新恢复() {
    for loaded_after_close in [false, true] {
        let (mut m, resume, _) = cancelled_resume_with_compensation();
        let a = apply(&mut m, notify("thread/closed", json!({"threadId":C})));
        assert!(m.relations.aux.released.contains_key(C));
        let probe = rpc(&a, "thread/read", C);
        if !loaded_after_close {
            // 即使补偿的回包未送达，排在它之后的读取也能核实实际卸载，安全退役。
            reply(
                &mut m,
                &probe,
                Ok(json!({"thread":{"id":C,"status":{"type":"notLoaded"}}})),
            );
            assert!(!m.relations.aux.released.contains_key(C));
            apply(
                &mut m,
                Event::LateReply {
                    context: resume,
                    result: Ok(json!({"thread":{"id":C}})),
                },
            );
            assert!(!m.relations.attachments[C].valid);
        } else {
            // 新恢复发出会作废更早的读取身份，迟到读取不能清除新操作责任。
            apply(&mut m, Event::Connected { conn: 1 });
            let a = apply(
                &mut m,
                Event::ClientRequest {
                    request: request(1, 40, "turn/start", json!({"threadId":C,"input":[]})),
                    run_id: Some("独立冷恢复".into()),
                },
            );
            let current_resume = rpc(&a, "thread/resume", C);
            let current_op = m.relations.aux.released[C].op;
            let a = reply(
                &mut m,
                &probe,
                Ok(json!({"thread":{"id":C,"status":{"type":"notLoaded"}}})),
            );
            assert_eq!(m.relations.aux.released[C].op, current_op);
            assert!(!has_rpc(&a, "turn/start", C));
            assert!(has_rpc(
                &resume_ok(&mut m, &current_resume),
                "turn/start",
                C
            ));
        }
    }
}

/// 父运行已经实际开轮，子线程仍沿用独立运行留下的订阅。
fn reuse_parent(m: &mut Machine, mock: &mut SubagentMock) {
    let a = start_again(m, "父复用运行", 51);
    let a = resume_ok(m, &rpc(&a, "thread/resume", P));
    reply(
        m,
        &rpc(&a, "turn/start", P),
        Ok(json!({"turn":{"id":"复用父轮"}})),
    );
    apply(m, mock.turn_started(P, "复用父轮"));
}

#[test]
fn 验收1b_父先结束而无绑定子轮仍活动_初次退订与补退都等子轮完成() {
    for already_deferred in [true, false] {
        let (mut m, mut mock, old) = independent_child_before_unsubscribe();
        reuse_parent(&mut m, &mut mock);
        if already_deferred {
            let a = apply(
                &mut m,
                Event::Timer {
                    token: old.clone(),
                    kind: TimerKind::Unsubscribe,
                },
            );
            assert!(!has_rpc(&a, "thread/unsubscribe", C));
            assert!(m.relations.aux.deferred_child_unsubscribes.contains_key(C));
        }
        // 不给新子轮授予运行绑定，关系层成员需求不能替代观察轮次保护。
        apply(&mut m, mock.turn_started(C, "无绑定复用子轮"));
        assert!(!m.relation_needs_subscription(C));
        let a = apply(&mut m, mock.turn_completed(P, "复用父轮", "completed"));
        assert!(!m.claims.contains_key(P));
        assert!(
            !has_rpc(&a, "thread/unsubscribe", C),
            "父先结束不能摘掉仍活动的无绑定子轮"
        );
        if !already_deferred {
            let a = apply(
                &mut m,
                Event::Timer {
                    token: old,
                    kind: TimerKind::Unsubscribe,
                },
            );
            assert!(
                !has_rpc(&a, "thread/unsubscribe", C),
                "仅剩观察轮次也要保护初次退订"
            );
        }
        assert!(m.records[C].subscribed);
        assert!(m.relations.attachments[C].valid);
        assert!(m.relations.aux.deferred_child_unsubscribes.contains_key(C));
        let a = apply(&mut m, Event::Connected { conn: 99 });
        assert!(
            !has_rpc(&a, "thread/unsubscribe", C),
            "无关事件复查不能重复或提前退订"
        );
        let a = apply(
            &mut m,
            mock.turn_completed(C, "无绑定复用子轮", "completed"),
        );
        assert_eq!(
            unsubscribed(&a),
            vec![C.to_owned()],
            "子轮完成后恰好补退一次"
        );
        assert!(!m.records[C].subscribed);
        assert!(m.relations.aux.deferred_child_unsubscribes.is_empty());
    }
}

#[test]
fn 验收2b_两个延后项同一步解除保护_仅剩一个名额时逐个补退并保留责任() {
    let (mut m, mut mock, old) = independent_child_before_unsubscribe();
    reuse_parent(&mut m, &mut mock);
    let other = "另一个子线程";
    let mut edge = m.thread_relation(C).unwrap();
    edge.child_thread_id = other.into();
    m.relations.edges.insert(other.into(), edge);
    let record = m.records[C].clone();
    *m.record(other) = record;
    m.attach(other, crate::relations::AttachmentSource::ThreadStarted);
    for child in [C, other] {
        let a = apply(
            &mut m,
            Event::Timer {
                token: ClaimToken {
                    thread_id: child.into(),
                    claim_id: old.claim_id,
                },
                kind: TimerKind::Unsubscribe,
            },
        );
        assert!(!has_rpc(&a, "thread/unsubscribe", child));
    }
    assert_eq!(m.relations.aux.deferred_child_unsubscribes.len(), 2);
    // 两个候选都还没有退订责任，构造已有责任占用 1023 个名额的边界。
    m.relations.aux.released.remove(C).unwrap();
    let (seed, _) = released();
    let entry = seed.relations.aux.released[C].clone();
    for i in 0..crate::relations::MAX_ATTACHMENTS - 1 {
        m.relations
            .aux
            .released
            .insert(format!("保留{i}"), entry.clone());
    }
    let a = apply(&mut m, mock.turn_completed(P, "复用父轮", "completed"));
    let sent = unsubscribed(&a);
    assert_eq!(sent.len(), 1, "仅剩一个名额，同一步只能发一个退订");
    let first = &sent[0];
    let second = if first == C { other } else { C };
    assert!(m.relations.aux.released.contains_key(first));
    assert_eq!(
        m.relations.aux.released.len(),
        crate::relations::MAX_ATTACHMENTS
    );
    assert_eq!(m.relations.aux.deferred_child_unsubscribes.len(), 1);
    assert!(m
        .relations
        .aux
        .deferred_child_unsubscribes
        .contains_key(second));
    assert!(m.records[second].subscribed);
    let first_request = rpc(&a, "thread/unsubscribe", first);
    let a = apply(&mut m, Event::Connected { conn: 99 });
    assert!(
        unsubscribed(&a).is_empty(),
        "满表时重复复查不能先发请求再丢责任"
    );
    assert_eq!(
        m.relations.aux.released.len(),
        crate::relations::MAX_ATTACHMENTS
    );
    let a = reply(&mut m, &first_request, Ok(json!({"status":"notLoaded"})));
    assert_eq!(
        unsubscribed(&a),
        vec![second.to_owned()],
        "确认卸载释放名额后自动补退剩余项"
    );
    assert_eq!(
        m.relations.aux.released.len(),
        crate::relations::MAX_ATTACHMENTS
    );
    assert!(m.relations.aux.released.contains_key(second));
    assert!(m.relations.aux.deferred_child_unsubscribes.is_empty());
    for i in 0..crate::relations::MAX_ATTACHMENTS - 1 {
        assert_eq!(m.relations.aux.released[&format!("保留{i}")], entry);
    }
}

#[test]
fn 延后退订在失去订阅记录缺失或记录淘汰时立即清理() {
    for cause in 0..3 {
        let (mut m, _, old) = independent_child_before_unsubscribe();
        start_again(&mut m, "父保护运行", 60);
        apply(
            &mut m,
            Event::Timer {
                token: old,
                kind: TimerKind::Unsubscribe,
            },
        );
        assert!(m.relations.aux.deferred_child_unsubscribes.contains_key(C));
        match cause {
            0 => m.records.get_mut(C).unwrap().subscribed = false,
            1 => {
                m.records.remove(C);
            }
            _ => {
                for i in 0..MAX_HISTORY {
                    m.record(&format!("待淘汰历史{i}"));
                }
            }
        }
        let a = apply(&mut m, Event::Connected { conn: 99 });
        assert!(!has_rpc(&a, "thread/unsubscribe", C));
        assert!(
            m.relations.aux.deferred_child_unsubscribes.is_empty(),
            "清理不等待下一步事件"
        );
        if cause == 2 {
            assert!(!m.records.contains_key(C));
        }
    }
}

#[test]
fn 延后退订的旧代号不能退掉已结束的新认领订阅() {
    let (mut m, _, old) = independent_child_before_unsubscribe();
    start_again(&mut m, "父保护运行", 61);
    apply(
        &mut m,
        Event::Timer {
            token: old.clone(),
            kind: TimerKind::Unsubscribe,
        },
    );
    // 模拟新认领已经结束：此时没有活动认领可兜底，必须核对记录中的代号。
    m.records.get_mut(C).unwrap().last_claim_id = old.claim_id + 1;
    let a = apply(&mut m, Event::Connected { conn: 99 });
    assert!(!has_rpc(&a, "thread/unsubscribe", C));
    assert!(m.relations.aux.deferred_child_unsubscribes.is_empty());
    let a = apply(
        &mut m,
        Event::Timer {
            token: old,
            kind: TimerKind::Unsubscribe,
        },
    );
    assert!(!has_rpc(&a, "thread/unsubscribe", C));
    assert!(m.records[C].subscribed);
    assert!(
        m.relations.aux.deferred_child_unsubscribes.is_empty(),
        "迟到旧计时器不能重新登记延后项"
    );
}
