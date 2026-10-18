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
fn 退订后子线程又有新轮_新轮结束时再次退订一次() {
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, C, "派生1", CT);
    apply(&mut m, mock.turn_completed(C, CT, "completed"));
    assert_eq!(
        unsubscribed(&release_root(&mut m)),
        vec![P.to_owned(), C.to_owned()]
    );
    // 子线程上又来了事件（例如被重新附着）：附着证据重新有效，跑着时不退。
    let started = apply(&mut m, mock.turn_started(C, "子轮2"));
    assert!(unsubscribed(&started).is_empty());
    assert!(m.relations.attachments[C].valid);
    // 新轮未完成：此时再扫也不退。
    let mut scan = Vec::new();
    m.release_idle_children(None, &mut scan);
    assert!(unsubscribed(&scan).is_empty());
    let done = apply(&mut m, mock.turn_completed(C, "子轮2", "completed"));
    assert_eq!(unsubscribed(&done), vec![C.to_owned()]);
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

fn reattaches(actions: &[Action]) -> Vec<RequestContext> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Request(c) if matches!(c.operation, Operation::ChildReattach { .. }) => {
                Some(c.clone())
            }
            _ => None,
        })
        .collect()
}

/// 派生、子轮完成、根释放并随根退订子线程。
fn released() -> (Machine, SubagentMock) {
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, C, "派生1", CT);
    apply(&mut m, mock.turn_completed(C, CT, "completed"));
    assert_eq!(
        unsubscribed(&release_root(&mut m)),
        vec![P.to_owned(), C.to_owned()]
    );
    assert!(m.relations.aux.released_children.contains(C));
    (m, mock)
}

#[test]
fn 根再开运行先重新订阅退订过的子线程_回包只恢复附着证据() {
    let (mut m, _) = released();
    let a = start_again(&mut m, "第二运行", 2);
    let re = reattaches(&a);
    assert_eq!(re.len(), 1);
    assert_eq!(re[0].method, "thread/resume");
    assert_eq!(re[0].params.as_ref().unwrap()["threadId"], C);
    let pos_reattach = a
        .iter()
        .position(|x| matches!(x, Action::Request(c) if matches!(c.operation, Operation::ChildReattach { .. })))
        .unwrap();
    let pos_parent = a
        .iter()
        .position(|x| matches!(x, Action::Request(c) if c.params.as_ref().is_some_and(|p| p["threadId"] == P)))
        .expect("父线程的恢复或开轮请求");
    assert!(pos_reattach < pos_parent, "子线程先挂回，再处理父线程开工");
    assert!(!m.relations.aux.released_children.contains(C));
    let out = reply(
        &mut m,
        &re[0],
        Ok(json!({"thread":{"id":C,"status":{"type":"idle"}}})),
    );
    assert!(!out.iter().any(|x| matches!(x, Action::Warn(_))));
    assert!(
        m.relations.attachments[C].valid,
        "恢复附着证据，之后可再随根退订"
    );
    assert!(
        !m.records.get(C).is_some_and(|r| r.subscribed),
        "子线程不记为 broker 自己订阅的线程"
    );
}

#[test]
fn 已卸载的子线程不再重新订阅() {
    let (mut m, _) = released();
    apply(&mut m, notify("thread/closed", json!({"threadId":C})));
    assert!(!m.relations.aux.released_children.contains(C));
    assert!(reattaches(&start_again(&mut m, "第二运行", 2)).is_empty());
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
    assert!(m.relations.aux.released_children.contains(C));
    assert_eq!(reattaches(&start_again(&mut m, "第二运行", 2)).len(), 1);
}

#[test]
fn 卸载状态也划掉名单() {
    let (mut m, _) = released();
    apply(
        &mut m,
        notify(
            "thread/status/changed",
            json!({"threadId":C,"status":{"type":"notLoaded"}}),
        ),
    );
    assert!(!m.relations.aux.released_children.contains(C));
}

#[test]
fn 子线程又收到本连接的轮次事件说明已挂上_不再重新订阅() {
    let (mut m, mut mock) = released();
    apply(&mut m, mock.turn_started(C, "子轮2"));
    assert!(!m.relations.aux.released_children.contains(C));
    assert!(reattaches(&start_again(&mut m, "第二运行", 2)).is_empty());
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
                json!({"threadId":"别的线程","input":[{"type":"text","text":"无关"}]}),
            ),
            run_id: Some("别的运行".into()),
        },
    );
    assert!(reattaches(&a).is_empty());
    assert!(m.relations.aux.released_children.contains(C));
}

#[test]
fn 重新订阅失败只告警_附着证据保持失效() {
    let (mut m, _) = released();
    let re = reattaches(&start_again(&mut m, "第二运行", 2));
    let out = reply(
        &mut m,
        &re[0],
        Err(json!({"code":-32600,"message":"thread not found"})),
    );
    assert!(out
        .iter()
        .any(|x| matches!(x, Action::Warn(w) if w.contains("重新订阅失败"))));
    assert!(!m.relations.attachments[C].valid);
}

#[test]
fn 子线程被重新加载并自动挂上时划掉名单() {
    let (mut m, _) = released();
    // 父代理 resume_agent 重新加载子线程：Codex 自动挂上所有连接，并发 thread/started。
    apply(
        &mut m,
        notify(
            "thread/started",
            json!({"thread":{"id":C,"status":{"type":"idle"}}}),
        ),
    );
    assert!(!m.relations.aux.released_children.contains(C));
    assert!(m.relations.attachments[C].valid);
}

#[test]
fn 对已结束运行再开子树发现时先重新订阅退订过的子线程() {
    use crate::relations::{DiscoveryCaller, DiscoveryScope};
    let (mut m, _) = released();
    // 例如对已结束运行再次 cxb stop：整树停止从子树发现开始。
    let mut a = Vec::new();
    m.begin_discovery(
        P,
        DiscoveryScope::Run { run_id: R.into() },
        DiscoveryCaller::TreeStop {
            tree_op: 1,
            round: 1,
        },
        &mut a,
    );
    let re = reattaches(&a);
    assert_eq!(re.len(), 1);
    assert_eq!(re[0].params.as_ref().unwrap()["threadId"], C);
    let pos_reattach = a
        .iter()
        .position(|x| matches!(x, Action::Request(c) if matches!(c.operation, Operation::ChildReattach { .. })))
        .unwrap();
    let pos_discovery = a
        .iter()
        .position(|x| matches!(x, Action::Request(c) if matches!(c.operation, Operation::Discovery { .. })))
        .expect("发现请求");
    assert!(pos_reattach < pos_discovery, "先挂回子线程，再发发现请求");
}
