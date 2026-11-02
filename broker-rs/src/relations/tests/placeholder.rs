//! 失效占位只按操作证据撤销，未知与真实受管轮不得被顺带消账。
use super::*;
use crate::relations::convergence::settled;

fn prepared() -> (Machine, SubagentMock) {
    let (mut m, mut mock) = super::lineage_safety::machine();
    spawn_v1(&mut m, &mut mock, C, "派生", "t1");
    let event = mock.turn_completed(C, "t1", "completed");
    mock.inject(&mut m, event);
    m.convergence_observe_goal(C, false, &mut vec![]);
    m.convergence_sync_facts(R);
    (m, mock)
}

fn key(item: &str) -> EvidenceKey {
    EvidenceKey {
        parent_thread_id: P.into(),
        parent_turn_id: T.into(),
        item_id: item.into(),
    }
}

fn placeholder(m: &Machine) -> bool {
    m.convergences.ledgers[R]
        .activities
        .iter()
        .any(|a| a.key.thread_id == C && a.key.turn_id.is_none())
}

fn original(m: &Machine) -> (crate::sa1b::ActivityFact, crate::sa1b::ResultRef) {
    let ledger = &m.convergences.ledgers[R];
    (
        ledger
            .activities
            .iter()
            .find(|a| a.key.turn_id.as_deref() == Some("t1"))
            .unwrap()
            .clone(),
        ledger
            .results
            .iter()
            .find(|r| r.key.turn_id.as_deref() == Some("t1"))
            .unwrap()
            .clone(),
    )
}

#[test]
fn 失败跟进撤销占位保留原轮与结果且同步幂等() {
    for tool in ["sendInput", "resumeAgent"] {
        let (mut m, mut mock) = prepared();
        let saved = original(&m);
        let (started, failed) = SubagentMock::v1_message(tool, P, T, "失败跟进", C, true);
        mock.inject(&mut m, started);
        assert!(placeholder(&m), "必须先真实建立占位");
        assert!(m.convergences.ledgers[R].pending.contains(&key("失败跟进")));
        let epoch = m.convergences.ledgers[R].work_epoch;
        m.convergences.ledgers.get_mut(R).unwrap().covered_epoch = Some(epoch);
        mock.inject(&mut m, failed);
        assert!(!placeholder(&m), "明确失败后不得留下失效占位");
        assert_eq!(original(&m), saved);
        assert!(m.convergences.ledgers[R].work_epoch > epoch);
        assert_eq!(m.convergences.ledgers[R].covered_epoch, None);
        let after = m.convergences.ledgers[R].clone();
        m.convergence_sync_facts(R);
        m.convergence_sync_facts(R);
        assert_eq!(m.convergences.ledgers[R], after);
        assert!(settled(&m.convergence_facts(R).unwrap()));
    }
}

#[test]
fn 并发只失败一个仍保留另一操作及真实活动直到新轮完成() {
    let (mut m, mut mock) = prepared();
    let saved = original(&m);
    let (first, failed) = SubagentMock::v1_message("sendInput", P, T, "失败", C, true);
    let (second, success) = SubagentMock::v1_message("sendInput", P, T, "剩余", C, false);
    run(&mut m, &mut mock, vec![first, second]);
    assert!(placeholder(&m));
    mock.inject(&mut m, failed);
    assert!(placeholder(&m), "另一个操作仍需要占位");
    assert_eq!(original(&m), saved, "真实轮身份及结果不得按线程整批删除");
    assert!(m.convergences.ledgers[R].pending.contains(&key("剩余")));
    assert_eq!(m.convergences.ledgers[R].placeholder_operations[C].len(), 1);
    assert!(m.relations.operations.contains_key(&key("剩余")));
    assert!(!settled(&m.convergence_facts(R).unwrap()));
    mock.inject(&mut m, success);
    let event = mock.turn_started(C, "t2");
    mock.inject(&mut m, event);
    assert!(!placeholder(&m));
    assert_eq!(original(&m), saved);
    assert!(
        !settled(&m.convergence_facts(R).unwrap()),
        "真实新轮未结束仍阻塞"
    );
    let event = mock.turn_completed(C, "t2", "completed");
    mock.inject(&mut m, event);
    assert!(settled(&m.convergence_facts(R).unwrap()));
}

#[test]
fn 跟进成功并入已有活动轮仍等该轮完成() {
    let (mut m, mut mock) = prepared();
    let saved = original(&m);
    let (started, completed) = SubagentMock::v1_message("sendInput", P, T, "启动t2", C, false);
    run(&mut m, &mut mock, vec![started, completed]);
    assert!(placeholder(&m));
    let event = mock.turn_started(C, "t2");
    mock.inject(&mut m, event);
    let t2 = m.convergences.ledgers[R]
        .activities
        .iter()
        .find(|a| a.key.turn_id.as_deref() == Some("t2"))
        .unwrap()
        .key
        .clone();
    let (started, completed) = SubagentMock::v1_message("sendInput", P, T, "并入t2", C, false);
    run(&mut m, &mut mock, vec![started, completed]);
    assert!(!placeholder(&m));
    assert!(m.relations.operations[&key("并入t2")].proven_turns[C].contains("t2"));
    assert!(m.convergences.ledgers[R]
        .activities
        .iter()
        .any(|a| a.key == t2));
    assert_eq!(original(&m), saved);
    assert!(!settled(&m.convergence_facts(R).unwrap()));
    let event = mock.turn_completed(C, "t2", "completed");
    mock.inject(&mut m, event);
    assert!(settled(&m.convergence_facts(R).unwrap()));
}

#[test]
fn 跟进被拒绝只撤销该操作占位并保留管理约束() {
    let (mut m, mut mock) = prepared();
    let saved = original(&m);
    let (started, completed) = SubagentMock::v1_message("sendInput", P, T, "拒绝", C, false);
    mock.inject(&mut m, started);
    assert!(placeholder(&m));
    // 用其他运行绑定触发生产拒绝路径；独立认领会让 observe_turn 提前返回，无法驱动配对。
    let mut foreign = m.relations.bindings[&(C.into(), "t1".into())].clone();
    foreign.run_id = "独立运行".into();
    foreign.turn_id = "其他运行轮".into();
    m.relations
        .bindings
        .insert((C.into(), foreign.turn_id.clone()), foreign);
    let event = mock.turn_started(C, "外来新轮");
    mock.inject(&mut m, event);
    mock.inject(&mut m, completed);
    m.convergence_sync_facts(R);
    assert!(m.relations.operations[&key("拒绝")]
        .rejected_receivers
        .contains_key(C));
    assert!(!placeholder(&m));
    let (activity, result) = original(&m);
    assert_eq!(activity.key, saved.0.key);
    assert_eq!(result, saved.1);
    assert!(m.current_management_evidence(R, C));
    assert!(m.manage_context(R).unwrap().members.contains_key(C));
    assert!(
        !settled(&m.convergence_facts(R).unwrap()),
        "其他运行绑定与未知新轮仍然阻塞"
    );
}

#[test]
fn v2无配对轮次到期与操作回收都继续阻塞() {
    let (mut m, mut mock) = prepared();
    let saved = original(&m);
    let (started, completed) = SubagentMock::v2_activity("interacted", P, T, "v2无轮次", C);
    run(&mut m, &mut mock, vec![started, completed]);
    assert!(placeholder(&m));
    let expiry = m.relations.operations[&key("v2无轮次")].expires_at_ms;
    advance(&mut m, &mut mock, expiry + 1);
    m.convergence_sync_facts(R);
    assert_eq!(
        m.relations.operations[&key("v2无轮次")].outcome,
        EvidenceOutcome::Unproven
    );
    assert!(
        m.convergences.ledgers[R].pending.contains(&key("v2无轮次")),
        "到期不构成结束证明"
    );
    assert!(placeholder(&m));
    assert_eq!(original(&m), saved);
    assert!(!settled(&m.convergence_facts(R).unwrap()));
    let expired = m
        .convergence_diagnostics(R)
        .expect("未配对 v2 操作必须可在线诊断");
    let pending = expired["pendingOperations"].as_array().unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0]["expired"], true);
    assert_eq!(pending[0]["missing"], false);
    assert_eq!(pending[0]["outcome"], "Unproven");
    assert_eq!(pending[0]["receivers"], json!([C]));
    assert!(expired["activities"]
        .as_array()
        .unwrap()
        .iter()
        .any(|a| a["turnId"].is_null()));
    // 模拟 reserve_operation 只回收操作缓存；保守账本不能随之失去未知原因。
    m.relations.operations.remove(&key("v2无轮次"));
    m.convergence_sync_facts(R);
    assert!(m.convergences.ledgers[R].pending.contains(&key("v2无轮次")));
    assert!(placeholder(&m));
    assert!(m.convergences.ledgers[R].placeholder_operations[C].contains(&key("v2无轮次")));
    assert!(!settled(&m.convergence_facts(R).unwrap()));
    let missing = m
        .convergence_diagnostics(R)
        .expect("操作缓存回收后仍可诊断");
    let pending = missing["pendingOperations"].as_array().unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0]["missing"], true);
    assert_eq!(pending[0]["outcome"], "unknown");
    assert_eq!(pending[0]["receivers"], json!([C]));
    assert!(missing["activities"]
        .as_array()
        .unwrap()
        .iter()
        .any(|a| a["turnId"].is_null()));
}

#[test]
fn 观察缺口后遗漏旧轮保持未核实并保留原结果() {
    let (mut m, mut mock) = prepared();
    let saved = original(&m);
    m.convergence_observation_gap(C, &mut vec![]);
    m.convergence_observe_goal(C, false, &mut vec![]);
    // 回查只有另一个轮次，不补回 t1 的观察。
    let event = mock.turn_completed(C, "其他轮", "completed");
    mock.inject(&mut m, event);
    m.convergence_sync_facts(R);
    let (activity, result) = original(&m);
    assert_eq!(activity.key, saved.0.key);
    assert_eq!(result, saved.1);
    assert!(!activity.verified);
    assert_eq!(activity.activity, MemberActivity::Unknown);
    assert!(!settled(&m.convergence_facts(R).unwrap()));
}
