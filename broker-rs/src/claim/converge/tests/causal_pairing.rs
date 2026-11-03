//! 因果隔离和观察恢复须通过真实发现、汇总及最终释放，不能只检查账本计数。
use super::placeholder::replies;
use super::*;

const C: &str = "子";

fn base() -> (Machine, SubagentMock) {
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, C, "原派生", "旧轮");
    (m, mock)
}

fn raw(m: &mut Machine, id: &str, message: &str) {
    notify(
        m,
        "rawResponseItem/completed",
        json!({"threadId":P,"turnId":T,"item":{
        "type":"function_call","name":"followup_task","call_id":id,
        "arguments":json!({"target":C,"message":message}).to_string()}}),
    );
}

fn interacted(m: &mut Machine, id: &str) {
    let (a, b) = SubagentMock::v2_activity("interacted", P, T, id, C);
    apply(m, a);
    apply(m, b);
}

fn delivery(m: &mut Machine, turn: &str, id: &str, message: &str, encrypted: bool) {
    let prefix = format!("Message Type: NEW_TASK\nTask name: /root/{C}\nSender: /root\nPayload:\n");
    let content = if encrypted {
        json!([{"type":"input_text","text":prefix}, {"type":"encrypted_content","encrypted_content":message}])
    } else {
        json!([{"type":"input_text","text":format!("{prefix}{message}")}])
    };
    notify(
        m,
        "rawResponseItem/completed",
        json!({"threadId":C,"turnId":turn,"item":{
        "type":"agent_message","id":id,"author":"/root","recipient":format!("/root/{C}"),"content":content}}),
    );
}

fn pending(m: &Machine, count: usize) {
    assert_eq!(
        m.convergences.ledgers[R].pending.len(),
        count,
        "尚未证明消费本次跟进，必须保留 pending"
    );
}

fn finish(m: &mut Machine, mock: &mut SubagentMock) {
    if !matches!(m.claims[P].state, State::Converging { .. }) {
        let actions = complete(m, mock, P, T, "completed");
        replies(m, mock, actions);
    }
    for _ in 0..8 {
        if m.convergences.progress[R].reading.is_some() {
            break;
        }
        if m.convergences.progress[R].retry_at.is_none() {
            break;
        }
        let actions = retry(m);
        replies(m, mock, actions);
    }
    if m.convergences.progress[R].reading.is_some() {
        confirm_parent(m);
        let actions = complete(m, mock, P, "汇总轮", "completed");
        replies(m, mock, actions);
    }
    assert_eq!(
        m.runs.runs[R].state,
        RunState::Ended,
        "因果已核实的跟进必须最终收敛"
    );
    assert_eq!(m.runs.runs[R].status, Some(RunStatus::Completed));
    assert!(m.convergence_diagnostics(R).is_none());
}

#[test]
fn 因果隔离_旧轮跨基线完成仍等真正新轮并最终收敛() {
    let (mut m, mut mock) = base();
    raw(&mut m, "B", "新工作");
    complete(&mut m, &mut mock, C, "旧轮", "completed");
    interacted(&mut m, "B");
    pending(&m, 1);
    assert!(m
        .relations
        .operations
        .values()
        .find(|op| op.key.item_id == "B")
        .unwrap()
        .proven_turns
        .is_empty());
    let actions = complete(&mut m, &mut mock, P, T, "completed");
    replies(&mut m, &mock, actions);
    assert_eq!(m.runs.runs[R].state, RunState::Running);
    let actions = apply(&mut m, mock.turn_started(C, "新轮"));
    replies(&mut m, &mock, actions);
    let actions = complete(&mut m, &mut mock, C, "新轮", "completed");
    replies(&mut m, &mock, actions);
    pending(&m, 0);
    finish(&mut m, &mut mock);
    assert_eq!(
        m.runs.runs[R].children[0]
            .turn_ids
            .iter()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from(["新轮", "旧轮"])
    );
}

#[test]
fn 因果隔离_连续跟进第二个不能被第一个轮消账() {
    let (mut m, mut mock) = base();
    complete(&mut m, &mut mock, C, "旧轮", "completed");
    raw(&mut m, "A", "第一任务");
    interacted(&mut m, "A");
    apply(&mut m, mock.turn_started(C, "第一轮"));
    pending(&m, 0);
    raw(&mut m, "B", "第二任务");
    interacted(&mut m, "B");
    complete(&mut m, &mut mock, C, "第一轮", "completed");
    pending(&m, 1);
    apply(&mut m, mock.turn_started(C, "第二轮"));
    complete(&mut m, &mut mock, C, "第二轮", "completed");
    pending(&m, 0);
    finish(&mut m, &mut mock);
}

#[test]
fn 因果隔离_重叠操作须各有输入消费证据才可共享同轮() {
    let (mut m, mut mock) = base();
    complete(&mut m, &mut mock, C, "旧轮", "completed");
    raw(&mut m, "A", "第一任务");
    raw(&mut m, "B", "第二任务");
    interacted(&mut m, "A");
    interacted(&mut m, "B");
    apply(&mut m, mock.turn_started(C, "同一轮"));
    complete(&mut m, &mut mock, C, "同一轮", "completed");
    pending(&m, 2);
    delivery(&mut m, "同一轮", "入站A", "第一任务", false);
    pending(&m, 1);
    delivery(&mut m, "同一轮", "入站B", "第二任务", false);
    pending(&m, 0);
    finish(&mut m, &mut mock);
}

#[test]
fn 宽松操作先配对不替严格操作消账() {
    let (mut m, mut mock) = base();
    complete(&mut m, &mut mock, C, "旧轮", "completed");
    interacted(&mut m, "没有开始的A");
    raw(&mut m, "B", "第二任务");
    interacted(&mut m, "B");
    apply(&mut m, mock.turn_started(C, "不明承载轮"));
    pending(&m, 1);
    delivery(&mut m, "不明承载轮", "B输入", "第二任务", false);
    pending(&m, 0);
    complete(&mut m, &mut mock, C, "不明承载轮", "completed");
    finish(&mut m, &mut mock);
}

#[test]
fn 因果隔离_忙碌旧轮有匹配输入才允许吸收并最终收敛() {
    for encrypted in [false, true] {
        let (mut m, mut mock) = base();
        let original = m.binding(C, "旧轮").unwrap();
        let identity = m.convergences.ledgers[R].activities[0].key.clone();
        raw(&mut m, "B", "唯一跟进输入");
        interacted(&mut m, "B");
        pending(&m, 1);
        delivery(&mut m, "旧轮", "外来输入", "别的内容", encrypted);
        pending(&m, 1);
        delivery(&mut m, "旧轮", "实际输入", "唯一跟进输入", encrypted);
        pending(&m, 0);
        assert_eq!(m.binding(C, "旧轮").unwrap(), original);
        assert_eq!(m.convergences.ledgers[R].activities[0].key, identity);
        complete(&mut m, &mut mock, C, "旧轮", "completed");
        finish(&mut m, &mut mock);
    }
}

#[test]
fn 观察恢复_缺口及未加载回查同轮后配对并最终收敛() {
    for not_loaded in [false, true] {
        let (mut m, mut mock) = base();
        complete(&mut m, &mut mock, C, "旧轮", "completed");
        raw(&mut m, "B", "新工作");
        apply(&mut m, mock.turn_started(C, "恢复轮"));
        complete(&mut m, &mut mock, C, "恢复轮", "completed");
        let start = m.relations.aux.live_turn_starts[&(C.into(), "恢复轮".into())];
        if not_loaded {
            notify(
                &mut m,
                "thread/status/changed",
                json!({"threadId":C,"status":{"type":"notLoaded"}}),
            );
        } else {
            m.convergence_observation_gap(C, &mut vec![]);
        }
        interacted(&mut m, "B");
        pending(&m, 1);
        let actions = complete(&mut m, &mut mock, P, T, "completed");
        // 真实 thread/read 回包恢复同一轮 ActivityObservation；TurnObservation 标志保持不变。
        replies(&mut m, &mock, actions);
        pending(&m, 0);
        assert_eq!(
            m.relations.aux.live_turn_starts[&(C.into(), "恢复轮".into())],
            start
        );
        let ledger = m.convergences.ledgers[R].clone();
        let seq = m.relations.aux.observation_seq;
        // 再走真实发现，把相同轮次再次读回，不能重新分配身份或结果版本。
        let mut actions = vec![];
        let op = m.begin_discovery(
            P,
            DiscoveryScope::Run { run_id: R.into() },
            DiscoveryCaller::Converge {
                token: m.convergences.runs[R].token.clone(),
                work_epoch: ledger.work_epoch,
            },
            &mut actions,
        );
        let progress = m.convergences.progress.get_mut(R).unwrap();
        progress.discovery = Some((op, ledger.work_epoch));
        progress.discovery_generation = m.relations.discoveries[&op].generation;
        replies(&mut m, &mock, actions);
        assert_eq!(m.convergences.ledgers[R], ledger);
        assert_eq!(m.relations.aux.observation_seq, seq);
        finish(&mut m, &mut mock);
    }
}
