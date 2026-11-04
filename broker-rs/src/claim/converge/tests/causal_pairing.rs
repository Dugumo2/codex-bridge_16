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
    // 本夹具的 app-server 不给会话文件路径：原因写明文件不可用，来源为无。
    let (mut m, mut mock) = base();
    complete(&mut m, &mut mock, C, "旧轮", "completed");
    interacted(&mut m, "无raw父线程");
    pending(&m, 1);
    assert_eq!(
        pairing_rows(&m),
        [("\"fallback\"".into(), "\"rolloutPathUnavailable\"".into())].into()
    );
    assert_eq!(
        m.convergence_diagnostics(R).unwrap()["pendingOperations"][0]["pairingSource"],
        "none"
    );

    // 父线程送过 raw，只是这次调用缺开始记录；同一账本里的严格操作不带原因。
    let (mut m, mut mock) = base();
    complete(&mut m, &mut mock, C, "旧轮", "completed");
    raw(&mut m, "A", "第一任务");
    interacted(&mut m, "A");
    interacted(&mut m, "缺开始");
    pending(&m, 2);
    assert_eq!(op(&m, "A").v2_fallback, None);
    assert_eq!(op(&m, "A").v2_source, V2EvidenceSource::Raw);
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

/// 在同一父线程上开新运行：与 `setup` 相同的真实 turn/start 请求、回包与开始通知。
fn start_run(m: &mut Machine, run: &str, turn: &str, id: u64) -> Vec<Action> {
    let out = apply(
        m,
        Event::ClientRequest {
            request: crate::claim::ClientRequest {
                conn: 1,
                id: json!(id),
                method: "turn/start".into(),
                params: Some(json!({"threadId":P,"input":[{"type":"text","text":"新任务"}]})),
            },
            run_id: Some(run.into()),
        },
    );
    let context = out
        .into_iter()
        .find_map(|a| match a {
            Action::Request(c) if c.method == "turn/start" => Some(c),
            _ => None,
        })
        .expect("新运行发出 turn/start");
    let mut all = apply(
        m,
        Event::Reply {
            context,
            result: Ok(json!({"turn":{"id":turn}})),
        },
    );
    all.extend(notify(
        m,
        "turn/started",
        json!({"threadId":P,"turn":{"id":turn,"status":"inProgress","items":[]}}),
    ));
    all
}

#[test]
fn 根认领释放后再次发现同一向上候选仍按向上处理且新运行正常收敛() {
    const R2: &str = "第二运行";
    const T2: &str = "第二父轮";
    let (mut m, mut mock) = base();
    let (a, b) = SubagentMock::v2_activity("interacted", C, "旧轮", "首次向上", P);
    let mut all = apply(&mut m, a);
    all.extend(apply(&mut m, b));
    assert!(op(&m, "首次向上").rejected_receivers[P]
        .reason()
        .contains("上级"));
    all.extend(complete(&mut m, &mut mock, C, "旧轮", "completed"));
    all.extend(finish(&mut m, &mut mock));
    assert!(!m.claims.contains_key(P), "前置：根认领已释放");

    // 同一父线程开新运行，父线程用 followup_task 再次接手 C：C 经新配对重新被发现为本运行成员。
    all.extend(start_run(&mut m, R2, T2, 2));
    all.extend(notify(
        &mut m,
        "rawResponseItem/completed",
        json!({"threadId":P,"turnId":T2,"item":{
        "type":"function_call","name":"followup_task","call_id":"再次跟进",
        "arguments":json!({"target":C,"message":"继续"}).to_string()}}),
    ));
    let (a, b) = SubagentMock::v2_activity("interacted", P, T2, "再次跟进", C);
    all.extend(apply(&mut m, a));
    all.extend(apply(&mut m, b));
    let actions = apply(&mut m, mock.turn_started(C, "新运行轮"));
    all.extend(replies(&mut m, &mock, actions));
    assert!(
        m.binding(C, "新运行轮")
            .is_some_and(|b| b.run_id == R2 && b.status == BindingStatus::Bound),
        "前置：C 的新轮归新运行管理"
    );

    // 同一个向上候选再次出现：仍是正常协作，不授管理权、不记冲突或缺口，也不留待定。
    let (a, b) = SubagentMock::v2_activity("interacted", C, "新运行轮", "再次向上", P);
    all.extend(apply(&mut m, a));
    all.extend(apply(&mut m, b));
    m.convergence_sync_facts(R2);
    let again = op(&m, "再次向上");
    assert_eq!(again.run_id, R2);
    assert!(again.rejected_receivers[P].reason().contains("上级"));
    assert!(!m.manage_context(R2).unwrap().members.contains_key(P));
    assert!(m.convergences.ledgers[R2].pending.is_empty());

    // 新运行照常收敛，全程零误报。
    let actions = complete(&mut m, &mut mock, C, "新运行轮", "completed");
    all.extend(replies(&mut m, &mock, actions));
    let actions = complete(&mut m, &mut mock, P, T2, "completed");
    all.extend(replies(&mut m, &mock, actions));
    for _ in 0..8 {
        let progress = &m.convergences.progress[R2];
        if progress.reading.is_some() || progress.retry_at.is_none() {
            break;
        }
        let runtime = m.convergences.runs[R2].clone();
        m.clock.mono_ms = progress.retry_at.unwrap();
        let mut out = vec![];
        m.convergence_timer(
            TimerKind::ConvergeRetry {
                token: runtime.token,
                attempt: runtime.discovery_retry,
            },
            &mut out,
        );
        all.extend(replies(&mut m, &mock, out));
    }
    assert!(
        m.convergences.progress[R2].reading.is_some(),
        "新运行应进入汇总，不被向上候选挡住"
    );
    assert!(m.convergence_facts(R2).unwrap().conflicts.is_empty());
    assert_no_alarm(&all);
}

#[test]
fn 同正文两条跟进被同一忙碌轮吸收后各认一条消费记录并最终收敛() {
    for encrypted in [false, true] {
        let (mut m, mut mock) = base();
        let mut all = vec![];
        raw(&mut m, "A", "同一正文");
        interacted(&mut m, "A");
        raw(&mut m, "B", "同一正文");
        interacted(&mut m, "B");
        pending(&m, 2);
        delivery(&mut m, "旧轮", "入站1", "同一正文", encrypted);
        pending(&m, 1);
        delivery(&mut m, "旧轮", "入站2", "同一正文", encrypted);
        pending(&m, 0);
        all.extend(complete(&mut m, &mut mock, C, "旧轮", "completed"));
        all.extend(finish(&mut m, &mut mock));
        assert_no_alarm(&all);
    }
}

#[test]
fn 同正文两条只被吸收一条时另一条仍待定直到自己的承载轮() {
    let (mut m, mut mock) = base();
    raw(&mut m, "A", "同一正文");
    interacted(&mut m, "A");
    raw(&mut m, "B", "同一正文");
    interacted(&mut m, "B");
    delivery(&mut m, "旧轮", "入站1", "同一正文", false);
    pending(&m, 1);
    // 另一条在旧轮回答之后才到，留在队列里等下一轮：旧轮结束不能把它一起核销。
    complete(&mut m, &mut mock, C, "旧轮", "completed");
    pending(&m, 1);
    let actions = apply(&mut m, mock.turn_started(C, "新轮"));
    replies(&mut m, &mock, actions);
    delivery(&mut m, "新轮", "入站2", "同一正文", false);
    pending(&m, 0);
    let actions = complete(&mut m, &mut mock, C, "新轮", "completed");
    replies(&mut m, &mock, actions);
    let all = finish(&mut m, &mut mock);
    assert_no_alarm(&all);
}

#[test]
fn 前一条超过证据期限才被读到时同正文后来者不冒领其消费记录() {
    for absorbed in [false, true] {
        let (mut m, mut mock) = base();
        raw(&mut m, "A", "同一正文");
        interacted(&mut m, "A");
        past_evidence_ttl(&mut m, &mut mock);
        complete(&mut m, &mut mock, C, "旧轮", "completed");
        let actions = apply(&mut m, mock.turn_started(C, "第一轮"));
        replies(&mut m, &mock, actions);
        delivery(&mut m, "第一轮", "入站A", "同一正文", false);
        pending(&m, 0);
        // 第一轮进行中父线程再发同正文：A 的消费记录不能被它认领。
        raw(&mut m, "B", "同一正文");
        interacted(&mut m, "B");
        pending(&m, 1);
        if absorbed {
            // 第一轮把它也吸收了：留下自己的消费记录，立即配对。
            delivery(&mut m, "第一轮", "入站B", "同一正文", false);
            pending(&m, 0);
            let actions = complete(&mut m, &mut mock, C, "第一轮", "completed");
            replies(&mut m, &mock, actions);
        } else {
            let actions = complete(&mut m, &mut mock, C, "第一轮", "completed");
            replies(&mut m, &mock, actions);
            pending(&m, 1);
            let actions = apply(&mut m, mock.turn_started(C, "第二轮"));
            replies(&mut m, &mock, actions);
            delivery(&mut m, "第二轮", "入站B", "同一正文", false);
            pending(&m, 0);
            let actions = complete(&mut m, &mut mock, C, "第二轮", "completed");
            replies(&mut m, &mock, actions);
        }
        let all = finish(&mut m, &mut mock);
        assert_no_alarm(&all);
    }
}

/// 一条：来历不明的记录早于 B 发出，按先后不可能是 B 的。
/// 两条：B 发出后到了两条同正文记录而 B 只有一条，多出的一条说明有没看到开始的同内容发送，整组不认。
/// （同正文只到一条且晚于 B 发出时无从区分，属于残余风险，见交付说明。）
#[test]
fn 没有开始记录的同正文先被消费时严格跟进不冒领() {
    for untracked in [1, 2] {
        let (mut m, mut mock) = base();
        complete(&mut m, &mut mock, C, "旧轮", "completed");
        // raw 迟到于 Interacted：这些操作只能宽松配对，broker 不知道它们的正文。
        let late: Vec<String> = (0..untracked).map(|i| format!("无开始{i}")).collect();
        for id in &late {
            interacted(&mut m, id);
            raw(&mut m, id, "同一正文");
            assert!(!op(&m, id).v2_strict);
        }
        if untracked == 2 {
            // 严格跟进 B 先于第一轮发出，但没赶上第一轮取输入；它的 Interacted 晚于两条消费记录才到。
            raw(&mut m, "B", "同一正文");
        }
        let actions = apply(&mut m, mock.turn_started(C, "第一轮"));
        replies(&mut m, &mock, actions);
        for i in 0..untracked {
            delivery(&mut m, "第一轮", &format!("入站{i}"), "同一正文", false);
        }
        if untracked == 1 {
            raw(&mut m, "B", "同一正文");
        }
        interacted(&mut m, "B");
        assert!(op(&m, "B").v2_strict);
        assert!(
            op(&m, "B").proven_turns.is_empty(),
            "来历不明的同正文消费记录不能核销 B"
        );
        let actions = complete(&mut m, &mut mock, C, "第一轮", "completed");
        replies(&mut m, &mock, actions);
        assert!(op(&m, "B").proven_turns.is_empty());
        let actions = apply(&mut m, mock.turn_started(C, "第二轮"));
        replies(&mut m, &mock, actions);
        delivery(&mut m, "第二轮", "入站B", "同一正文", false);
        pending(&m, 0);
        let actions = complete(&mut m, &mut mock, C, "第二轮", "completed");
        replies(&mut m, &mock, actions);
        finish(&mut m, &mut mock);
    }
}

#[test]
fn 同正文跟进发给不同子代理时各凭自己的消费记录配对() {
    const C2: &str = "子二";
    let (mut m, mut mock) = base();
    spawn(&mut m, &mut mock, P, T, C2, "派生子二", "子二旧轮");
    raw(&mut m, "A", "同一正文");
    interacted(&mut m, "A");
    notify(
        &mut m,
        "rawResponseItem/completed",
        json!({"threadId":P,"turnId":T,"item":{
        "type":"function_call","name":"followup_task","call_id":"B",
        "arguments":json!({"target":C2,"message":"同一正文"}).to_string()}}),
    );
    let (a, b) = SubagentMock::v2_activity("interacted", P, T, "B", C2);
    apply(&mut m, a);
    apply(&mut m, b);
    pending(&m, 2);
    delivery(&mut m, "旧轮", "入站A", "同一正文", false);
    pending(&m, 1);
    notify(
        &mut m,
        "rawResponseItem/completed",
        json!({"threadId":C2,"turnId":"子二旧轮","item":{
        "type":"agent_message","id":"入站B","author":"/root","recipient":format!("/root/{C2}"),
        "content":[{"type":"input_text","text":format!("Message Type: NEW_TASK\nTask name: /root/{C2}\nSender: /root\nPayload:\n同一正文")}]}}),
    );
    pending(&m, 0);
    complete(&mut m, &mut mock, C, "旧轮", "completed");
    complete(&mut m, &mut mock, C2, "子二旧轮", "completed");
    let all = finish(&mut m, &mut mock);
    assert_no_alarm(&all);
}

/// 新运行的收尾驱动：父轮结束后按重试推进发现，直到进入汇总或不再重试。
fn drive_run(m: &mut Machine, mock: &SubagentMock, run: &str) -> Vec<Action> {
    let mut all = vec![];
    for _ in 0..8 {
        let progress = &m.convergences.progress[run];
        if progress.reading.is_some() || progress.retry_at.is_none() {
            break;
        }
        let runtime = m.convergences.runs[run].clone();
        m.clock.mono_ms = progress.retry_at.unwrap();
        let mut out = vec![];
        m.convergence_timer(
            TimerKind::ConvergeRetry {
                token: runtime.token,
                attempt: runtime.discovery_retry,
            },
            &mut out,
        );
        all.extend(replies(m, mock, out));
    }
    all
}

/// 上一运行派生的兄弟：本运行的子代理给它发平级消息（或它向上回消息）不是跟进被拒，
/// 不能记 rejectedFollowup 冲突把运行卡到收敛期限；兄弟被唤醒的新轮已开始时仍按活动轮阻塞收敛。
#[test]
fn 历史兄弟收到平级消息不算冲突_兄弟新轮在跑时仍阻塞收敛() {
    const S: &str = "历史兄弟";
    const R2: &str = "第二运行";
    const T2: &str = "第二父轮";
    for upward in [false, true] {
        let (mut m, mut mock) = base();
        spawn(&mut m, &mut mock, P, T, S, "派生兄弟", "兄弟旧轮");
        let mut all = complete(&mut m, &mut mock, S, "兄弟旧轮", "completed");
        all.extend(complete(&mut m, &mut mock, C, "旧轮", "completed"));
        all.extend(finish(&mut m, &mut mock));
        assert!(!m.claims.contains_key(P), "前置：上一运行已结束");

        // 新运行只跟进 C；S 是上一运行留下的兄弟。
        all.extend(start_run(&mut m, R2, T2, 2));
        all.extend(notify(
            &mut m,
            "rawResponseItem/completed",
            json!({"threadId":P,"turnId":T2,"item":{
            "type":"function_call","name":"followup_task","call_id":"跟进C",
            "arguments":json!({"target":C,"message":"继续"}).to_string()}}),
        ));
        let (a, b) = SubagentMock::v2_activity("interacted", P, T2, "跟进C", C);
        all.extend(apply(&mut m, a));
        all.extend(apply(&mut m, b));
        let actions = apply(&mut m, mock.turn_started(C, "C新轮"));
        all.extend(replies(&mut m, &mock, actions));
        assert!(m
            .binding(C, "C新轮")
            .is_some_and(|b| b.run_id == R2 && b.status == BindingStatus::Bound));

        // C 给历史兄弟 S 发平级消息，S 被唤醒开新轮；upward 时 C 另向上给 P 发消息。
        let (a, b) = SubagentMock::v2_activity("interacted", C, "C新轮", "平级消息", S);
        all.extend(apply(&mut m, a));
        all.extend(apply(&mut m, b));
        assert_eq!(
            op(&m, "平级消息").rejected_receivers[S],
            FollowupRejection::Peer
        );
        let actions = apply(&mut m, mock.turn_started(S, "兄弟新轮"));
        all.extend(replies(&mut m, &mock, actions));
        if upward {
            let (a, b) = SubagentMock::v2_activity("interacted", C, "C新轮", "向上消息", P);
            all.extend(apply(&mut m, a));
            all.extend(apply(&mut m, b));
            assert_eq!(
                op(&m, "向上消息").rejected_receivers[P],
                FollowupRejection::Upward
            );
            assert!(!m.rejected_followup_for_run(R2, P), "向上不算跟进被拒");
        }
        assert!(!m.rejected_followup_for_run(R2, S), "平级不算跟进被拒");

        let actions = complete(&mut m, &mut mock, C, "C新轮", "completed");
        all.extend(replies(&mut m, &mock, actions));
        let actions = complete(&mut m, &mut mock, P, T2, "completed");
        all.extend(replies(&mut m, &mock, actions));
        all.extend(drive_run(&mut m, &mock, R2));
        // 维护者接受的代价：兄弟新轮已经开始，按活动候选阻塞收尾，但不是冲突。
        assert!(
            m.convergences.progress[R2].reading.is_none(),
            "兄弟新轮在跑时不能收尾"
        );
        let facts = m.convergence_facts(R2).unwrap();
        assert!(facts.conflicts.is_empty(), "{:?}", facts.conflicts);
        assert!(
            m.relations
                .aux
                .classifications
                .get(&(R2.into(), S.into()))
                .is_some_and(
                    |c| c.membership == Membership::Pending("candidateActivityActive".into())
                ),
            "兄弟按活动候选阻塞"
        );

        // 兄弟新轮结束后运行正常收尾，全程零误报。
        let actions = complete(&mut m, &mut mock, S, "兄弟新轮", "completed");
        all.extend(replies(&mut m, &mock, actions));
        all.extend(drive_run(&mut m, &mock, R2));
        assert!(
            m.convergences.progress[R2].reading.is_some(),
            "兄弟新轮结束后应进入汇总"
        );
        assert!(m.convergence_facts(R2).unwrap().conflicts.is_empty());
        // 兄弟不归本运行管理，它的轮次摘要暂存到期会记「归属确认前摘要暂存过期」；
        // 这条与本修复无关（事件日志按血缘暂存，修复前同样会记），单独列出，其余仍须零误报。
        let (sibling_gaps, rest): (Vec<_>, Vec<_>) = all.into_iter().partition(|action| {
            matches!(action, Action::Store(crate::store::StoreOp::AppendLog { lines, .. })
                if lines.iter().all(|line| line.contains("beforeBindingUnknown") && line.contains(S)))
        });
        assert!(sibling_gaps.len() <= 1);
        assert_no_alarm(&rest);
    }
}
