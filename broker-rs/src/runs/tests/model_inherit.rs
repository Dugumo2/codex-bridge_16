use super::*;

#[test]
fn mi_顺序键不受同毫秒随机编号和墙钟回拨影响() {
    let mut m = machine();
    m.runs.epoch = 42;
    open(&mut m, "run-z", "线程");
    open(&mut m, "run-a", "线程");
    m.clock.now_ms = 1;
    open(&mut m, "run-b", "线程");
    let keys: Vec<_> = ["run-z", "run-a", "run-b"]
        .map(|id| m.runs.runs[id].creation_order.unwrap())
        .into();
    assert_eq!(
        keys,
        vec![
            CreationOrder {
                epoch: 42,
                sequence: 1
            },
            CreationOrder {
                epoch: 42,
                sequence: 2
            },
            CreationOrder {
                epoch: 42,
                sequence: 3
            }
        ]
    );
    assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
    let before = m.runs.runs["run-z"].creation_order;
    m.run_start_evidence("run-z", "线程", "首轮", &mut vec![]);
    complete(&mut m, "run-z", "线程", "首轮", json!([message("完成")]));
    assert_eq!(m.runs.runs["run-z"].creation_order, before);
}

#[test]
fn mi_重启只认持久化确认不能由结果或joined反推() {
    for (state, confirm, joined) in [
        (RunState::Running, false, false),
        (RunState::Stopping, false, false),
        (RunState::Lost, false, false),
        (RunState::Ended, false, false),
        (RunState::Ended, false, true),
        (RunState::Running, true, false),
        (RunState::Ended, true, false),
    ] {
        let mut original = machine();
        open(&mut original, "run-old", "线程");
        if confirm {
            original.run_start_evidence("run-old", "线程", "轮", &mut vec![]);
        }
        complete(
            &mut original,
            "run-old",
            "线程",
            "轮",
            json!([message("完成")]),
        );
        let mut record = original.runs.runs["run-old"].clone();
        record.state = state;
        record.joined = joined;
        let expected = record.start_confirmation.clone();
        let record = serde_json::from_value(serde_json::to_value(&record).unwrap()).unwrap();
        let mut restored = machine();
        restored.restore_runs(original.runs.index, vec![record], &mut vec![]);
        assert_eq!(
            restored.runs.runs["run-old"].start_confirmation, expected,
            "{state:?}, confirmed={confirm}, joined={joined}"
        );
        if matches!(
            state,
            RunState::Running | RunState::Stopping | RunState::Lost
        ) {
            restored.run_start_evidence("run-old", "线程", "迟到轮", &mut vec![]);
            restored.run_settled("run-old", false, &mut vec![]);
            assert_eq!(restored.runs.runs["run-old"].start_confirmation, expected);
        }
    }
}

#[test]
fn mi_旧记录缺新增字段读回不补造顺序与确认() {
    let mut m = machine();
    open(&mut m, "run-old", "线程");
    complete(&mut m, "run-old", "线程", "轮", json!([message("完成")]));
    let mut value = serde_json::to_value(&m.runs.runs["run-old"]).unwrap();
    for field in [
        "creationOrder",
        "startConfirmation",
        "pendingReroute",
        "currentTurnId",
    ] {
        value.as_object_mut().unwrap().remove(field);
    }
    value["meta"] = json!({"source":"run","template":"旧模板"});
    let record: RunRecord = serde_json::from_value(value).unwrap();
    assert!(record.creation_order.is_none());
    assert!(record.current_turn_id.is_none());
    assert!(serde_json::to_value(&record)
        .unwrap()
        .get("currentTurnId")
        .is_none());
    assert_eq!(record.start_confirmation, StartConfirmation::default());
    assert!(record.turns[0].reroute.is_none());
    let meta: RunMeta = serde_json::from_value(record.meta.clone().unwrap()).unwrap();
    assert!(meta.model.is_none());
    assert!(meta.effort.is_none());
    let mut restored = machine();
    restored.restore_runs(m.runs.index, vec![record], &mut vec![]);
    assert!(!restored.runs.runs["run-old"].start_confirmation.confirmed);
    assert!(restored.runs.runs["run-old"].creation_order.is_none());
}

#[test]
fn mi_确认前lost即使曾有未定案开轮证据也不能确认() {
    let mut m = machine();
    m.shared = true;
    open(&mut m, "run", "线程");
    m.run_start_evidence("run", "线程", "轮", &mut vec![]);
    assert!(!m.runs.runs["run"].start_confirmation.confirmed);
    m.runs.runs.get_mut("run").unwrap().state = RunState::Lost;
    m.run_settled("run", false, &mut vec![]);
    m.run_start_evidence("run", "线程", "轮", &mut vec![]);
    assert!(!m.runs.runs["run"].start_confirmation.confirmed);
}

#[test]
fn mi_缺失旧轮完成时新轮清理改道且迟到完成不消费新轮() {
    let mut m = machine();
    open(&mut m, "run", "线程");
    m.run_turn_started("run", "线程", "旧轮", &mut vec![]);
    let params = |turn: &str| {
        Some(json!({"threadId":"线程","turnId":turn,"fromModel":"A","toModel":"B","reason":"test"}))
    };
    m.run_rerouted(&params("旧轮"), &mut vec![]);
    let mut actions = vec![];
    m.run_turn_started("run", "线程", "新轮", &mut actions);
    assert!(m.runs.runs["run"].pending_reroute.is_none());
    assert!(puts(&actions) > 0);
    m.run_rerouted(&params("新轮"), &mut vec![]);
    complete(&mut m, "run", "线程", "旧轮", json!([message("旧输出")]));
    assert!(m.runs.runs["run"].turns[0].reroute.is_none());
    assert_eq!(
        m.runs.runs["run"].pending_reroute.as_ref().unwrap().turn_id,
        "新轮"
    );
    complete(&mut m, "run", "线程", "新轮", json!([message("新输出")]));
    assert!(m.runs.runs["run"].pending_reroute.is_none());
    assert_eq!(last(&m, "run").reroute.as_ref().unwrap().to_model, "B");
}
