//! 真实 v2 双通知晚于执行；只有当前代际中的唯一可核实承载轮才能消账。
use super::*;
use crate::store::StoreOp;

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
fn v2无raw回退与已选严格模式互不改写() {
    for mode in ["无开始", "晚于轮开始", "晚于Interacted", "换代丢失"] {
        let (mut m, mut mock) = fixture();
        if mode == "换代丢失" {
            begin(&mut m, &mut mock);
            m.relations.conn_epoch += 1;
        }
        let event = mock.turn_started(C, "活动轮");
        mock.inject(&mut m, event);
        if mode == "晚于轮开始" {
            begin(&mut m, &mut mock);
        }
        interact(&mut m, &mut mock);
        if mode == "晚于Interacted" {
            begin(&mut m, &mut mock);
        }
        let strict = mode == "晚于轮开始";
        assert_eq!(
            m.convergences.ledgers[R].pending.len(),
            usize::from(strict),
            "{mode}"
        );
        let event = mock.turn_completed(C, "活动轮", "completed");
        mock.inject(&mut m, event);
        assert_eq!(
            m.convergences.ledgers[R].pending.len(),
            usize::from(strict),
            "{mode}"
        );
        assert_eq!(
            m.relations
                .operations
                .values()
                .find(|op| op.key.item_id == "v2操作")
                .unwrap()
                .v2_strict,
            strict
        );
    }
}

#[test]
fn v2因果退役旧运行迟到轮不被新运行吸收() {
    let (mut m, mut mock) = super::lineage_safety::machine();
    spawn_v1(&mut m, &mut mock, C, "旧派生", "旧活动轮");
    assert!(m.binding(C, "旧活动轮").is_some());
    // 用正常退役判定回收旧绑定；首个实际开始观察仍属于旧轮，不能因回收而重新计时。
    m.claims.remove(P);
    let record = m.runs.runs.get_mut(R).unwrap();
    record.state = crate::runs::RunState::Ended;
    record.ended_at = Some(m.clock.now_ms);
    m.relations.contexts.get_mut(R).unwrap().authority = Authority::Revoked;
    m.prune_contexts();
    assert!(m.binding(C, "旧活动轮").is_none());
    m.claims
        .insert(P.into(), claim(Owner::Client(1), "新父轮", "新运行", 20));
    m.run_opened(
        crate::runs::RunOpen {
            thread_id: P.into(),
            run_id: "新运行".into(),
            conn: Some(1),
            kind: crate::claim::ReqKind::TurnStart,
            joined: false,
            input: "新任务".into(),
            continued_from: None,
        },
        &mut vec![],
    );
    m.convergence_begin(P, &mut vec![]);
    mock.inject(&mut m,Event::Notification {method:"rawResponseItem/completed".into(),params:Some(json!({
        "threadId":P,"turnId":"新父轮","item":{"type":"function_call","name":"followup_task","call_id":"新跟进"}}))});
    let event = mock.turn_started(C, "旧活动轮");
    mock.inject(&mut m, event);
    let event = mock.turn_completed(C, "旧活动轮", "completed");
    mock.inject(&mut m, event);
    let (a, b) = SubagentMock::v2_activity("interacted", P, "新父轮", "新跟进", C);
    run(&mut m, &mut mock, vec![a, b]);
    assert_eq!(m.convergences.ledgers["新运行"].pending.len(), 1);
    assert!(!m
        .relations
        .bindings
        .values()
        .any(|b| b.status == BindingStatus::Bound));
}

#[test]
fn v2配对观察缺口和not_loaded保持pending() {
    for not_loaded in [false, true] {
        let (mut m, mut mock) = fixture();
        begin(&mut m, &mut mock);
        let event = mock.turn_started(C, "真实轮");
        mock.inject(&mut m, event);
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

fn log_lines_with<'a>(actions: &'a [Action], text: &str) -> Vec<&'a String> {
    actions
        .iter()
        .flat_map(|a| match a {
            Action::Store(StoreOp::AppendLog { lines, .. }) => lines.iter().collect::<Vec<_>>(),
            _ => vec![],
        })
        .filter(|line| line.contains(text))
        .collect()
}

#[test]
fn v2子代理向上发消息不记归属冲突而向下跟进非直接子仍拒绝() {
    const GRANDCHILD: &str = "孙线程";
    let (mut m, mut mock) = fixture();
    begin(&mut m, &mut mock);
    interact(&mut m, &mut mock);
    let event = mock.turn_started(C, "子轮");
    mock.inject(&mut m, event);
    assert!(m.binding(C, "子轮").is_some(), "前置：子轮已受本运行管理");
    mock.register(GRANDCHILD, Some(C), "v2", false, true);
    let event = mock.thread_started(GRANDCHILD);
    mock.inject(&mut m, event);
    let (a, b) = SubagentMock::v2_activity("started", C, "子轮", "派生孙", GRANDCHILD);
    let event = mock.turn_started(GRANDCHILD, "孙轮");
    run(&mut m, &mut mock, vec![a, b, event]);
    assert!(
        m.binding(GRANDCHILD, "孙轮").is_some(),
        "前置：孙轮已受本运行管理"
    );

    // 子向父、孙隔代向根：都是正常向上消息。
    for (sender, turn, item) in [(C, "子轮", "子向上"), (GRANDCHILD, "孙轮", "孙向上")] {
        let (a, b) = SubagentMock::v2_activity("interacted", sender, turn, item, P);
        let actions = run(&mut m, &mut mock, vec![a, b]);
        m.convergence_sync_facts(R);
        let op = m
            .relations
            .operations
            .values()
            .find(|op| op.key.item_id == item)
            .expect("前置：向上消息确实进入跟进判定");
        assert!(op.rejected_receivers.contains_key(P), "{item} 不授予管理权");
        assert!(log_lines_with(&actions, "归属冲突").is_empty(), "{item}");
        assert!(
            log_lines_with(&actions, "discoveryIncomplete").is_empty(),
            "{item}"
        );
        assert!(!m.manage_context(R).unwrap().members.contains_key(P));
    }
    assert!(
        m.convergences.ledgers[R].pending.is_empty(),
        "向上消息不留待定操作"
    );

    // 父线程直接跟进孙代理不是向上，C 的直接父校验仍要拒绝并记冲突。
    let (a, b) = SubagentMock::v2_activity("interacted", P, T, "越级跟进", GRANDCHILD);
    let actions = run(&mut m, &mut mock, vec![a, b]);
    let conflicts = log_lines_with(&actions, "归属冲突");
    assert_eq!(conflicts.len(), 1);
    assert!(conflicts[0].contains("未核实为本父线程的子代理"));
}

#[test]
fn v2兄弟与隔房子代理互发不记归属冲突且不授管理权() {
    const SIBLING: &str = "兄弟线程";
    const NEPHEW: &str = "侄线程";
    let (mut m, mut mock) = fixture();
    begin(&mut m, &mut mock);
    interact(&mut m, &mut mock);
    let event = mock.turn_started(C, "子轮");
    mock.inject(&mut m, event);
    for (thread, parent, item, turn) in [
        (SIBLING, P, "派生兄弟", "兄弟轮"),
        (NEPHEW, SIBLING, "派生侄", "侄轮"),
    ] {
        mock.register(thread, Some(parent), "v2", false, true);
        let event = mock.thread_started(thread);
        mock.inject(&mut m, event);
        let parent_turn = if parent == P { T } else { "兄弟轮" };
        let (a, b) = SubagentMock::v2_activity("started", parent, parent_turn, item, thread);
        let event = mock.turn_started(thread, turn);
        run(&mut m, &mut mock, vec![a, b, event]);
    }
    for (item, target) in [("互发兄弟", SIBLING), ("互发侄", NEPHEW)] {
        let (a, b) = SubagentMock::v2_activity("interacted", C, "子轮", item, target);
        let actions = run(&mut m, &mut mock, vec![a, b]);
        m.convergence_sync_facts(R);
        let op = m
            .relations
            .operations
            .values()
            .find(|op| op.key.item_id == item)
            .unwrap();
        assert!(op.rejected_receivers[target].contains("平级"), "{item}");
        assert!(log_lines_with(&actions, "归属冲突").is_empty(), "{item}");
        assert!(
            log_lines_with(&actions, "discoveryIncomplete").is_empty(),
            "{item}"
        );
    }
    assert!(m.convergences.ledgers[R].pending.is_empty());

    // 关系不明的线程不是平级，仍要记冲突。
    let (a, b) = SubagentMock::v2_activity("interacted", C, "子轮", "互发未知", "未知线程");
    let actions = run(&mut m, &mut mock, vec![a, b]);
    assert_eq!(log_lines_with(&actions, "归属冲突").len(), 1);

    // 隔代下级也不是平级：跨过直接子代理跟进仍要拒绝并记冲突。
    const GRAND_NEPHEW: &str = "侄孙线程";
    mock.register(GRAND_NEPHEW, Some(NEPHEW), "v2", false, true);
    let event = mock.thread_started(GRAND_NEPHEW);
    mock.inject(&mut m, event);
    let (a, b) = SubagentMock::v2_activity("started", NEPHEW, "侄轮", "派生侄孙", GRAND_NEPHEW);
    let event = mock.turn_started(GRAND_NEPHEW, "侄孙轮");
    run(&mut m, &mut mock, vec![a, b, event]);
    let (a, b) = SubagentMock::v2_activity(
        "interacted",
        SIBLING,
        "兄弟轮",
        "越级跟进侄孙",
        GRAND_NEPHEW,
    );
    let actions = run(&mut m, &mut mock, vec![a, b]);
    let conflicts = log_lines_with(&actions, "归属冲突");
    assert_eq!(conflicts.len(), 1);
    assert!(conflicts[0].contains("未核实为本父线程的子代理"));
}

#[test]
fn v2乱序消费暂存有界并按ttl清理() {
    let (mut m, _) = fixture();
    for index in 0..=MAX_OPERATIONS {
        let item = json!({"type":"agent_message","id":format!("input-{index}"),"author":"/root","recipient":format!("/root/{C}"),
            "content":[{"type":"input_text","text":format!("Message Type: NEW_TASK\nTask name: /root/{C}\nSender: /root\nPayload:\n输入{index}")}]});
        m.v2_observe_raw(C, "先到轮", &item, &mut vec![]);
    }
    assert_eq!(m.relations.aux.v2_deliveries.len(), MAX_OPERATIONS);
    m.clock.mono_ms += EVIDENCE_TTL_MS;
    m.prune_v2_deliveries();
    assert!(m.relations.aux.v2_deliveries.is_empty());
}

#[test]
fn v2配对索引与原表始终一致且挤出后同号可再登记() {
    let (mut m, mut mock) = fixture();
    let delivery = |index: usize| {
        json!({"type":"agent_message","id":format!("input-{index}"),"author":"/root","recipient":format!("/root/{C}"),
            "content":[{"type":"input_text","text":format!("Message Type: NEW_TASK\nTask name: /root/{C}\nSender: /root\nPayload:\n输入{}", index % 3)}]})
    };
    begin(&mut m, &mut mock);
    for index in 0..=MAX_OPERATIONS {
        m.v2_observe_raw(C, "先到轮", &delivery(index), &mut vec![]);
        assert!(m.v2_index_consistent(), "第 {index} 条后索引失配");
    }
    // 最早一条已被挤出：同号再到是新事实，不能被旧索引当成重复丢弃。
    assert!(!m
        .relations
        .aux
        .v2_deliveries
        .iter()
        .any(|d| d.id == "input-0"));
    m.v2_observe_raw(C, "先到轮", &delivery(0), &mut vec![]);
    assert!(m
        .relations
        .aux
        .v2_deliveries
        .iter()
        .any(|d| d.id == "input-0"));
    // 重复到达的同号输入仍只登记一次。
    let before = m.relations.aux.v2_deliveries.len();
    m.v2_observe_raw(C, "先到轮", &delivery(0), &mut vec![]);
    assert_eq!(m.relations.aux.v2_deliveries.len(), before);
    assert!(m.v2_index_consistent());
    m.clock.mono_ms += EVIDENCE_TTL_MS;
    m.prune_v2_deliveries();
    m.prune_followup_starts();
    assert!(m.relations.aux.v2_deliveries.is_empty());
    assert!(m.relations.aux.followup_starts.is_empty());
    assert!(m.v2_index_consistent());
    assert_eq!(m.relations.aux.v2_index, Default::default());
}
