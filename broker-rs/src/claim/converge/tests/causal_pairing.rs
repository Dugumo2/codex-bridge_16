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

fn finish(m: &mut Machine, mock: &mut SubagentMock) -> Vec<Action> {
    let mut all = vec![];
    if !matches!(m.claims[P].state, State::Converging { .. }) {
        let actions = complete(m, mock, P, T, "completed");
        all.extend(replies(m, mock, actions));
    }
    for _ in 0..8 {
        if m.convergences.progress[R].reading.is_some() {
            break;
        }
        if m.convergences.progress[R].retry_at.is_none() {
            break;
        }
        let actions = retry(m);
        all.extend(replies(m, mock, actions));
    }
    if m.convergences.progress[R].reading.is_some() {
        confirm_parent(m);
        let actions = complete(m, mock, P, "汇总轮", "completed");
        all.extend(replies(m, mock, actions));
    }
    assert_eq!(
        m.runs.runs[R].state,
        RunState::Ended,
        "因果已核实的跟进必须最终收敛"
    );
    assert_eq!(m.runs.runs[R].status, Some(RunStatus::Completed));
    assert!(m.convergence_diagnostics(R).is_none());
    all
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

/// 按真实调度走过证据期限：期限内应发生的到期扫描都要真正触发，否则测不到被判过期的路径。
fn past_evidence_ttl(m: &mut Machine, mock: &mut SubagentMock) -> Vec<Action> {
    m.clock.mono_ms += EVIDENCE_TTL_MS + 1;
    m.clock.now_ms += EVIDENCE_TTL_MS + 1;
    let mut actions = vec![];
    m.facts_expire(&mut actions);
    m.convergence_sync_facts(R);
    replies(m, mock, actions)
}

fn op<'a>(m: &'a Machine, id: &str) -> &'a EvidenceOperation {
    m.relations
        .operations
        .values()
        .find(|op| op.key.item_id == id)
        .unwrap()
}

#[test]
fn 忙碌子代理超过证据期限才读到跟进仍配对并最终收敛() {
    for encrypted in [false, true] {
        let (mut m, mut mock) = base();
        raw(&mut m, "B", "唯一跟进输入");
        interacted(&mut m, "B");
        past_evidence_ttl(&mut m, &mut mock);
        pending(&m, 1);
        assert_eq!(op(&m, "B").outcome, EvidenceOutcome::Pending);
        let diagnostic = m.convergence_diagnostics(R).unwrap();
        let listed = &diagnostic["pendingOperations"][0];
        assert_eq!(listed["pairingMode"], "strict");
        assert_eq!(listed["expired"], false, "等待配对不算过期");
        assert!(listed["expiresAt"].is_null());
        delivery(&mut m, "旧轮", "实际输入", "唯一跟进输入", encrypted);
        pending(&m, 0);
        complete(&mut m, &mut mock, C, "旧轮", "completed");
        finish(&mut m, &mut mock);
    }
}

#[test]
fn 旧轮结束超过证据期限才开新轮仍配对并最终收敛() {
    let (mut m, mut mock) = base();
    raw(&mut m, "B", "新工作");
    interacted(&mut m, "B");
    complete(&mut m, &mut mock, C, "旧轮", "completed");
    past_evidence_ttl(&mut m, &mut mock);
    pending(&m, 1);
    assert_eq!(
        m.has_managed_activity(R),
        crate::relations::Activity::Some,
        "跟进还在子代理队列里，自然结束门禁不能当成没有活动"
    );
    let actions = apply(&mut m, mock.turn_started(C, "新轮"));
    replies(&mut m, &mock, actions);
    pending(&m, 0);
    let actions = complete(&mut m, &mut mock, C, "新轮", "completed");
    replies(&mut m, &mock, actions);
    finish(&mut m, &mut mock);
}

#[test]
fn 没有接收方的跟进事件不算等待配对() {
    let (mut m, _mock) = base();
    let (a, b) = SubagentMock::v2_activity("interacted", P, T, "无接收方", "");
    apply(&mut m, a);
    apply(&mut m, b);
    assert!(op(&m, "无接收方").receivers.is_empty());
    assert!(!m.v2_awaits_pairing(op(&m, "无接收方")));
    assert!(m.evidence_expirable(op(&m, "无接收方")));
}

#[test]
fn 等待配对的跟进在连接换代后恢复按期限失效() {
    let (mut m, mut mock) = base();
    raw(&mut m, "B", "唯一跟进输入");
    interacted(&mut m, "B");
    assert!(m.v2_awaits_pairing(op(&m, "B")));
    m.relations.conn_epoch += 1;
    assert!(!m.v2_awaits_pairing(op(&m, "B")));
    assert!(m.evidence_expirable(op(&m, "B")));
    past_evidence_ttl(&mut m, &mut mock);
    assert_eq!(op(&m, "B").outcome, EvidenceOutcome::Unproven);
    assert!(
        !m.relations
            .aux
            .followup_starts
            .contains_key(&op(&m, "B").key),
        "换代后不再保留开始记录"
    );
}

#[test]
fn 已配对跟进的开始记录仍按期限清理() {
    let (mut m, mut mock) = base();
    raw(&mut m, "B", "唯一跟进输入");
    interacted(&mut m, "B");
    delivery(&mut m, "旧轮", "实际输入", "唯一跟进输入", false);
    pending(&m, 0);
    past_evidence_ttl(&mut m, &mut mock);
    assert!(m.relations.aux.followup_starts.is_empty());
    assert_eq!(op(&m, "B").outcome, EvidenceOutcome::Proven);
}

fn raw_send_message(m: &mut Machine, id: &str, message: &str) {
    notify(
        m,
        "rawResponseItem/completed",
        json!({"threadId":P,"turnId":T,"item":{
        "type":"function_call","name":"send_message","call_id":id,
        "arguments":json!({"target":C,"message":message}).to_string()}}),
    );
}

fn send_message(m: &mut Machine, id: &str, message: &str) {
    raw_send_message(m, id, message);
    interacted(m, id);
}

#[test]
fn 只入队消息不占待定账本且空闲子代理也能收敛() {
    let (mut m, mut mock) = base();
    complete(&mut m, &mut mock, C, "旧轮", "completed");
    let epoch = m.convergences.ledgers[R].work_epoch;
    send_message(&mut m, "S", "顺带告知");
    m.convergence_sync_facts(R);
    pending(&m, 0);
    assert_eq!(m.convergences.ledgers[R].work_epoch, epoch);
    assert!(op(&m, "S").v2_queue_only && op(&m, "S").receivers.is_empty());
    assert!(!m.v2_awaits_pairing(op(&m, "S")));
    past_evidence_ttl(&mut m, &mut mock);
    assert_eq!(op(&m, "S").outcome, EvidenceOutcome::Unproven);
    finish(&mut m, &mut mock);
}

#[test]
fn 只入队消息不干扰之后的跟进配对新轮() {
    let (mut m, mut mock) = base();
    complete(&mut m, &mut mock, C, "旧轮", "completed");
    send_message(&mut m, "S", "顺带告知");
    raw(&mut m, "B", "新工作");
    interacted(&mut m, "B");
    // 另一条只入队消息才到 raw、还没到 Interacted：它也不可能是新轮的原因。
    raw_send_message(&mut m, "S2", "再告知一次");
    pending(&m, 1);
    let actions = apply(&mut m, mock.turn_started(C, "新轮"));
    replies(&mut m, &mock, actions);
    pending(&m, 0);
    let actions = complete(&mut m, &mut mock, C, "新轮", "completed");
    replies(&mut m, &mock, actions);
    finish(&mut m, &mut mock);
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

fn pairing_rows(m: &Machine) -> std::collections::BTreeSet<(String, String)> {
    m.convergence_diagnostics(R).unwrap()["pendingOperations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["pairingMode"].to_string(),
                row["fallbackReason"].to_string(),
            )
        })
        .collect()
}

#[test]
fn 宽松配对诊断标明回退原因且严格操作不带原因() {
    // 父线程从没送来 raw：典型是跨重启 --resume 恢复的线程。
    // 子代理先空闲：宽松模式才会真正等新轮，而不是吸收旧活动轮。
    let (mut m, mut mock) = base();
    complete(&mut m, &mut mock, C, "旧轮", "completed");
    interacted(&mut m, "无raw父线程");
    pending(&m, 1);
    assert_eq!(
        pairing_rows(&m),
        [("\"fallback\"".into(), "\"parentWithoutRaw\"".into())].into()
    );

    // 父线程送过 raw，只是这次调用缺开始记录；同一账本里的严格操作不带原因。
    let (mut m, mut mock) = base();
    complete(&mut m, &mut mock, C, "旧轮", "completed");
    raw(&mut m, "A", "第一任务");
    interacted(&mut m, "A");
    interacted(&mut m, "缺开始");
    pending(&m, 2);
    assert_eq!(op(&m, "A").v2_fallback, None);
    assert_eq!(
        pairing_rows(&m),
        [
            ("\"fallback\"".into(), "\"startMissing\"".into()),
            ("\"strict\"".into(), "null".into()),
        ]
        .into()
    );

    // 有开始记录但授权已换代：不能沿用，原因写明失效而不是缺失。
    let (mut m, _mock) = base();
    raw(&mut m, "B", "唯一跟进输入");
    m.relations.contexts.get_mut(R).unwrap().gen += 1;
    interacted(&mut m, "B");
    assert!(!op(&m, "B").v2_strict);
    assert_eq!(op(&m, "B").v2_fallback, Some(V2FallbackReason::StartStale));
}

/// 正常成功路径不得出现缺口、陈旧、不完整、冲突、过期类提示，也不得发警告。
fn assert_no_alarm(actions: &[Action]) {
    for action in actions {
        match action {
            Action::Warn(warning) => panic!("正常路径不应告警：{warning}"),
            Action::Store(crate::store::StoreOp::AppendLog { lines, .. }) => {
                for line in lines {
                    for word in [
                        "缺口",
                        "冲突",
                        "陈旧",
                        "不完整",
                        "过期",
                        "coverageIssue",
                        "Incomplete",
                        "stale",
                    ] {
                        assert!(!line.contains(word), "正常路径不应出现「{word}」：{line}");
                    }
                }
            }
            _ => {}
        }
    }
}

fn awaiting_row(m: &Machine) -> serde_json::Value {
    let diagnostic = m.convergence_diagnostics(R).unwrap();
    let rows = diagnostic["pendingOperations"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    rows[0].clone()
}

#[test]
fn 跟进久未读到时诊断给出等待起点与接收方忙闲且正常路径零误报() {
    let (mut m, mut mock) = base();
    let since = m.clock.now_ms;
    let mut all = notify(
        &mut m,
        "rawResponseItem/completed",
        json!({"threadId":P,"turnId":T,"item":{
        "type":"function_call","name":"followup_task","call_id":"B",
        "arguments":json!({"target":C,"message":"新工作"}).to_string()}}),
    );
    let (a, b) = SubagentMock::v2_activity("interacted", P, T, "B", C);
    all.extend(apply(&mut m, a));
    all.extend(apply(&mut m, b));
    pending(&m, 1);
    // 子代理还在旧轮里忙：跟进在队列里，诊断如实给出等待起点。
    let row = awaiting_row(&m);
    assert_eq!(row["pairingMode"], "strict");
    assert_eq!(row["awaitingPairingSince"], since);
    assert_eq!(
        row["receiverStates"],
        json!([{"threadId":C,"state":"running"}])
    );
    // 超过证据期限后旧轮结束：子代理空闲而跟进仍未读到，起点不变、只是状态变成 idle。
    all.extend(past_evidence_ttl(&mut m, &mut mock));
    all.extend(complete(&mut m, &mut mock, C, "旧轮", "completed"));
    let row = awaiting_row(&m);
    assert_eq!(row["awaitingPairingSince"], since);
    assert_eq!(row["expired"], false, "等待配对不是过期");
    assert_eq!(
        row["receiverStates"],
        json!([{"threadId":C,"state":"idle"}])
    );
    // 子代理读到跟进开新轮：配对后不再等待，运行正常收敛，全程零误报。
    let actions = apply(&mut m, mock.turn_started(C, "新轮"));
    all.extend(replies(&mut m, &mock, actions));
    pending(&m, 0);
    let actions = complete(&mut m, &mut mock, C, "新轮", "completed");
    all.extend(replies(&mut m, &mock, actions));
    all.extend(finish(&mut m, &mut mock));
    assert_no_alarm(&all);
}

#[test]
fn 观察有缺口时诊断不猜接收方空闲() {
    let (mut m, mut mock) = base();
    complete(&mut m, &mut mock, C, "旧轮", "completed");
    raw(&mut m, "B", "新工作");
    interacted(&mut m, "B");
    assert_eq!(
        awaiting_row(&m)["receiverStates"],
        json!([{"threadId":C,"state":"idle"}])
    );
    m.convergence_observation_gap(C, &mut vec![]);
    assert_eq!(
        awaiting_row(&m)["receiverStates"],
        json!([{"threadId":C,"state":"unknown"}])
    );
}
