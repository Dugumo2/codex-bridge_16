//! 真实 v2 双通知晚于执行；只有当前代际中的唯一可核实承载轮才能消账。
use super::*;

fn fixture() -> (Machine, SubagentMock) {
    let (mut m, mut mock) = super::lineage_safety::machine();
    mock.register(C, Some(P), "v2", false, true);
    let event = mock.thread_started(C);
    mock.inject(&mut m, event);
    let event = mock.turn_completed(C, "历史轮", "completed");
    mock.inject(&mut m, event);
    (m, mock)
}

fn begin(m: &mut Machine, mock: &mut SubagentMock) {
    mock.inject(m, Event::Notification { method: "rawResponseItem/completed".into(),
        params: Some(json!({"threadId":P,"turnId":T,"item":{"type":"function_call", "name":"followup_task","call_id":"v2操作"}})) });
}

fn interact(m: &mut Machine, mock: &mut SubagentMock) {
    let (a, b) = SubagentMock::v2_activity("interacted", P, T, "v2操作", C);
    run(m, mock, vec![a, b]);
    m.convergence_sync_facts(R);
}

#[test]
fn v2配对无唯一轮不以任意completed轮消账() {
    for count in [0, 2] {
        let (mut m, mut mock) = fixture();
        begin(&mut m, &mut mock);
        for index in 0..count {
            let turn = format!("候选{index}");
            let event = mock.turn_started(C, &turn);
            mock.inject(&mut m, event);
            let event = mock.turn_completed(C, &turn, "completed");
            mock.inject(&mut m, event);
        }
        interact(&mut m, &mut mock);
        assert_eq!(
            m.convergences.ledgers[R].pending.len(),
            1,
            "历史完成或多候选不能证明唯一配对"
        );
        assert!(m
            .relations
            .operations
            .values()
            .all(|op| op.proven_turns.is_empty()));
        assert!(!m
            .relations
            .bindings
            .values()
            .any(|binding| binding.status == BindingStatus::Bound));
    }
}

#[test]
fn v2配对缺失开始证据不把先完成轮当作新轮() {
    let (mut m, mut mock) = fixture();
    let event = mock.turn_completed(C, "无法判明因果的轮", "completed");
    mock.inject(&mut m, event);
    interact(&mut m, &mut mock);
    assert_eq!(m.convergences.ledgers[R].pending.len(), 1);
}

#[test]
fn v2配对观察缺口和not_loaded保持pending() {
    for not_loaded in [false, true] {
        let (mut m, mut mock) = fixture();
        begin(&mut m, &mut mock);
        let event = mock.turn_completed(C, "真实轮", "completed");
        mock.inject(&mut m, event);
        if not_loaded {
            mock.inject(
                &mut m,
                Event::Notification {
                    method: "thread/status/changed".into(),
                    params: Some(json!({
                "threadId":C,"status":{"type":"notLoaded"}})),
                },
            );
        } else {
            m.convergence_observation_gap(C, &mut vec![]);
        }
        interact(&mut m, &mut mock);
        assert_eq!(m.convergences.ledgers[R].pending.len(), 1);
        assert!(m
            .relations
            .operations
            .values()
            .all(|op| op.proven_turns.is_empty()));
    }
}

#[test]
fn v2配对连接换代和授权换代不能沿用旧操作() {
    for connection in [false, true] {
        let (mut m, mut mock) = fixture();
        begin(&mut m, &mut mock);
        interact(&mut m, &mut mock);
        if connection {
            m.relations.conn_epoch += 1;
        } else {
            m.relations.contexts.get_mut(R).unwrap().gen += 1;
        }
        let event = mock.turn_completed(C, "换代后轮", "completed");
        mock.inject(&mut m, event);
        m.convergence_sync_facts(R);
        assert_eq!(m.convergences.ledgers[R].pending.len(), 1);
        assert!(m
            .relations
            .operations
            .values()
            .all(|op| op.proven_turns.is_empty()));
    }
}

#[test]
fn v2配对运行中重启保留失联终态而不恢复授权或消账为成功() {
    let (mut m, mut mock) = fixture();
    begin(&mut m, &mut mock);
    interact(&mut m, &mut mock);
    assert_eq!(m.convergences.ledgers[R].pending.len(), 1);
    let record = m.runs.runs[R].clone();
    let mut restored = Machine::default();
    mock.inject(
        &mut restored,
        Event::Restore {
            index: m.runs.index.clone(),
            runs: vec![record],
            inbox: Default::default(),
        },
    );
    assert_eq!(restored.runs.runs[R].state, crate::runs::RunState::Lost);
    assert!(
        restored.runs.runs[R]
            .subagent_join
            .as_ref()
            .unwrap()
            .automatic_control_revoked
    );
    assert!(restored.convergence_diagnostics(R).is_none());
    assert!(restored.convergences.runs.is_empty());
    let event = mock.turn_completed(C, "重启后迟到轮", "completed");
    mock.inject(&mut restored, event);
    interact(&mut restored, &mut mock);
    assert!(!restored
        .relations
        .bindings
        .values()
        .any(|binding| binding.status == BindingStatus::Bound));
    assert_eq!(restored.runs.runs[R].state, crate::runs::RunState::Lost);
}
