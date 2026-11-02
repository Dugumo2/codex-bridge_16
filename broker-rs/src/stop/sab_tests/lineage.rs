//! 缺陷 C：停止与删除读取独立的血缘、活动和未决候选集合。
use super::*;
use crate::relations::{DiscoveryCaller, DiscoveryScope};
use crate::stop::background::TERMINATE;

fn discover(s: &mut Sim) {
    let mut actions = Vec::new();
    let op = s.m.begin_discovery(
        P,
        DiscoveryScope::Run { run_id: R.into() },
        DiscoveryCaller::FinishGate {
            run_id: R.into(),
            claim_id: s.m.claims[P].claim_id,
        },
        &mut actions,
    );
    s.absorb(actions);
    assert!(s.m.discovery_outcome(op).is_some(), "实际发现请求已返回");
}

fn candidate(s: &mut Sim, thread: &str) {
    s.mock.register(thread, Some(P), "v1", false, true);
    s.children.entry(P.into()).or_default().push(thread.into());
    s.learn_thread(thread);
}

#[test]
fn lineage_停止范围包含三个未受管后代与两个活动成员且不越权中断() {
    let mut s = Sim::new();
    let historical: Vec<_> = ["旧甲", "旧乙", "旧丙"]
        .into_iter()
        .map(|thread| {
            let mut child = crate::runs::child_history::test_child(thread);
            child.parent_thread_id = P.into();
            child
        })
        .collect();
    s.m.runs.index.child_history.remember(
        "previous-run",
        P,
        &historical,
        s.m.clock.now_ms,
        &BTreeMap::new(),
    );
    for thread in ["旧甲", "旧乙", "旧丙"] {
        candidate(&mut s, thread);
        assert!(
            s.m.historical_subagent_for_run(R, thread),
            "旧后代有独立历史归属证据"
        );
    }
    s.turn_started("旧甲", "外部轮");
    s.goals.insert("旧乙".into(), json!({"status": "active"}));
    s.spawn_v1("新甲", "派生甲", "新轮甲");
    s.spawn_v1("新乙", "派生乙", "新轮乙");
    discover(&mut s);
    let activity: BTreeSet<_> =
        s.m.manage_context(R)
            .unwrap()
            .members
            .keys()
            .cloned()
            .collect();
    assert_eq!(activity, BTreeSet::from(["新甲".into(), "新乙".into()]));

    s.call(
        OWNER,
        "broker/threads/stop",
        json!({"threadId": P, "runId": R}),
    );
    let report = s.record().children_stop.as_ref().expect("已生成树停止报告");
    let scope: BTreeSet<_> = report.members.keys().cloned().collect();
    assert_eq!(
        scope,
        BTreeSet::from([
            "旧甲".into(),
            "旧乙".into(),
            "旧丙".into(),
            "新甲".into(),
            "新乙".into(),
        ]),
        "清理核实集合必须保留全部五个后代"
    );
    assert_eq!(s.interrupts(&s.log, "旧甲"), 0, "外部轮没有本次绑定");
    assert_eq!(s.interrupts(&s.log, "新甲"), 1);
    assert_eq!(s.interrupts(&s.log, "新乙"), 1);
    assert_eq!(s.goals["旧乙"]["status"], "active", "候选不获 goal 修改权");
    assert!(report.conflicts.iter().any(|c| c.thread == "旧甲"));
}

#[test]
fn lineage_只有候选的运行停止仍采用完整血缘且不清理候选后台命令() {
    let mut s = Sim::new();
    candidate(&mut s, "未知子");
    s.background.insert(
        "未知子".into(),
        vec![json!({"itemId": "外部命令", "processId": "进程1"})],
    );
    discover(&mut s);
    assert!(s.m.run_has_candidate(R, "未知子"));
    assert!(s.m.manage_context(R).unwrap().members.is_empty());
    s.call(OWNER, "broker/threads/stop", json!({"threadId": P}));
    let report = s
        .record()
        .children_stop
        .as_ref()
        .expect("候选也进入整树核实");
    assert!(report.members.contains_key("未知子"));
    assert_eq!(s.background["未知子"].len(), 1);
    assert!(
        !s.log.iter().any(|a| matches!(a, Action::Request(c)
        if c.method == TERMINATE && c.params.as_ref().unwrap()["threadId"] == "未知子")),
        "发现候选不获后台命令终止权"
    );
}

#[test]
fn lineage_未决候选不在活动成员表仍阻塞删除并说明发现不完整() {
    for method in [
        "broker/threads/archive",
        "thread/delete",
        "broker/threads/purge",
    ] {
        let mut s = Sim::new();
        candidate(&mut s, "未知子");
        discover(&mut s);
        assert!(s.m.run_has_candidate(R, "未知子"));
        assert!(!s
            .m
            .manage_context(R)
            .unwrap()
            .members
            .contains_key("未知子"));
        let (id, actions) = s.call(OWNER, method, json!({"threadId": "未知子"}));
        let reply = s.response(&actions, id).expect("门禁明确拒绝");
        let blockers = reply["error"]["data"]["blockers"]
            .as_array()
            .expect("列出阻塞原因");
        assert!(blockers
            .iter()
            .any(|b| b["reason"] == "subagentInUse" && b["runId"] == R));
        assert!(blockers
            .iter()
            .any(|b| b["reason"] == "discoveryIncomplete"));
        assert!(
            !actions.iter().any(|a| matches!(a, Action::Request(c)
            if matches!(c.method.as_str(), "thread/archive" | "thread/delete"))),
            "核实期间不得向上游执行删除或归档"
        );
    }
}

#[test]
fn lineage_显式历史停止只恢复清理范围不恢复活动成员() {
    let mut s = Sim::new();
    s.spawn_v1("历史子", "派生", "历史轮");
    s.turn_completed("历史子", "历史轮", "completed");
    s.turn_completed(P, T, "completed");
    assert!(!s.m.claims.contains_key(P));
    let children = s.record().children.clone();
    s.hold
        .push(("thread/backgroundTerminals/list".into(), P.into()));
    s.call(
        OWNER,
        "broker/threads/stop",
        json!({"threadId": P, "runId": R}),
    );
    let context = s.m.manage_context(R).expect("显式历史停止建立清理上下文");
    assert!(
        context.members.is_empty(),
        "历史清理不能污染 wait --tree 活动成员"
    );
    assert!(context.cleanup.as_ref().unwrap().scope.contains("历史子"));
    assert!(s.m.binding("历史子", "历史轮").is_none());
    assert_eq!(s.record().children, children);
    s.release("thread/backgroundTerminals/list", P);
    assert!(s.tree_op().is_none(), "新鲜核实结束后释放清理范围");
}
