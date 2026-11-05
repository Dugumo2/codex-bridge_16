//! 因果隔离和观察恢复须通过真实发现、汇总及最终释放，不能只检查账本计数。
use super::placeholder::replies;
use super::*;

pub(super) const C: &str = "子";

pub(super) fn base() -> (Machine, SubagentMock) {
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, C, "原派生", "旧轮");
    (m, mock)
}

pub(super) fn raw(m: &mut Machine, id: &str, message: &str) {
    notify(
        m,
        "rawResponseItem/completed",
        json!({"threadId":P,"turnId":T,"item":{
        "type":"function_call","name":"followup_task","call_id":id,
        "arguments":json!({"target":C,"message":message}).to_string()}}),
    );
}

pub(super) fn interacted(m: &mut Machine, id: &str) {
    let (a, b) = SubagentMock::v2_activity("interacted", P, T, id, C);
    apply(m, a);
    apply(m, b);
}

pub(super) fn delivery(m: &mut Machine, turn: &str, id: &str, message: &str, encrypted: bool) {
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

pub(super) fn pending(m: &Machine, count: usize) {
    assert_eq!(
        m.convergences.ledgers[R].pending.len(),
        count,
        "尚未证明消费本次跟进，必须保留 pending"
    );
}

pub(super) fn finish(m: &mut Machine, mock: &mut SubagentMock) -> Vec<Action> {
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

pub(super) fn op<'a>(m: &'a Machine, id: &str) -> &'a EvidenceOperation {
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
pub(super) fn assert_no_alarm(actions: &[Action]) {
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
pub(super) fn start_run(m: &mut Machine, run: &str, turn: &str, id: u64) -> Vec<Action> {
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
fn 父线程事件晚到_消费记录先于开始记录被看到仍配对并最终收敛() {
    let (mut m, mut mock) = base();
    complete(&mut m, &mut mock, C, "旧轮", "completed");
    // 子线程的事件先送达：新轮和消费记录都在父线程的调用记录之前被看到。
    let actions = apply(&mut m, mock.turn_started(C, "新轮"));
    replies(&mut m, &mock, actions);
    delivery(&mut m, "新轮", "入站", "先到的正文", false);
    raw(&mut m, "跟进", "先到的正文");
    interacted(&mut m, "跟进");
    assert!(op(&m, "跟进").v2_strict);
    pending(&m, 0);
    let actions = complete(&mut m, &mut mock, C, "新轮", "completed");
    replies(&mut m, &mock, actions);
    finish(&mut m, &mut mock);
}

#[test]
fn 子代理归属未定时转手跟进孙代理_定案后接回孙代理并最终收敛() {
    const G: &str = "孙";
    // 孙代理的新轮可能晚于子代理绑定，也可能早于它；后一种只能靠绑定时接回。
    for grandchild_first in [false, true] {
        let (mut m, mut mock) = base();
        spawn(&mut m, &mut mock, C, "旧轮", G, "派生孙", "孙旧轮");
        complete(&mut m, &mut mock, G, "孙旧轮", "completed");
        complete(&mut m, &mut mock, C, "旧轮", "completed");
        raw(&mut m, "跟进子", "交给你");
        interacted(&mut m, "跟进子");
        // 子代理的新轮还没绑定到运行，它发出的跟进先到。
        let (started, done) = SubagentMock::v2_activity("interacted", C, "新轮", "跟进孙", G);
        apply(&mut m, started);
        apply(&mut m, done);
        if grandchild_first {
            let actions = apply(&mut m, mock.turn_started(G, "孙新轮"));
            replies(&mut m, &mock, actions);
        }
        let actions = apply(&mut m, mock.turn_started(C, "新轮"));
        replies(&mut m, &mock, actions);
        delivery(&mut m, "新轮", "入站", "交给你", false);
        if !grandchild_first {
            let actions = apply(&mut m, mock.turn_started(G, "孙新轮"));
            replies(&mut m, &mock, actions);
        }
        assert!(
            m.relations
                .bindings
                .get(&(G.to_string(), "孙新轮".to_string()))
                .is_some_and(|b| b.run_id == R && b.status == BindingStatus::Bound),
            "孙代理的新轮必须绑定到本运行（孙先开轮：{grandchild_first}）"
        );
        pending(&m, 0);
        let actions = complete(&mut m, &mut mock, G, "孙新轮", "completed");
        replies(&mut m, &mock, actions);
        let actions = complete(&mut m, &mut mock, C, "新轮", "completed");
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
pub(super) fn drive_run(m: &mut Machine, mock: &SubagentMock, run: &str) -> Vec<Action> {
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
    for (upward, close) in [(false, false), (true, false), (false, true)] {
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
        // 兄弟已明确判定不归本运行管理（历史子代理、已结束）：它按血缘暂存的轮次摘要
        // 到期或关闭清理时都正常丢弃，不写「记录缺口」。
        assert!(
            m.relations
                .aux
                .classifications
                .get(&(R2.into(), S.into()))
                .is_some_and(|c| c.membership == Membership::Historical),
            "前置：兄弟已判定为历史子代理"
        );
        if close {
            all.extend(apply(&mut m, Event::Shutdown));
        } else {
            all.extend(sweep_pending_event_log(&mut m, &all));
        }
        assert_no_alarm(&all);
    }
}

/// 走过归属确认前的暂存期限，按最后一次排下的真实计时触发暂存清理。
fn sweep_pending_event_log(m: &mut Machine, actions: &[Action]) -> Vec<Action> {
    let (token, kind) = actions
        .iter()
        .rev()
        .find_map(|a| match a {
            Action::Schedule {
                token,
                kind: kind @ TimerKind::Run(crate::runs::RunTimer::EventLogSweep { .. }),
                ..
            } => Some((token.clone(), kind.clone())),
            _ => None,
        })
        .expect("前置：有摘要在等归属确认");
    m.clock.mono_ms += 30_001;
    m.clock.now_ms += 30_001;
    apply(m, Event::Timer { token, kind })
}

/// 任意线程上的协作工具调用（raw 的 function_call）。
pub(super) fn raw_call(
    m: &mut Machine,
    thread: &str,
    turn: &str,
    name: &str,
    id: &str,
    target: &str,
    message: &str,
) -> Vec<Action> {
    notify(
        m,
        "rawResponseItem/completed",
        json!({"threadId":thread,"turnId":turn,"item":{
        "type":"function_call","name":name,"call_id":id,
        "arguments":json!({"target":target,"message":message}).to_string()}}),
    )
}

/// 子代理 C 刚被跟进、它的新轮还没绑定到运行，就对自己的空闲孙代理 G 留言（send_message）。
/// 调用证据（raw）须先存下，C 绑定后恢复工具类型：只入队，不让运行等 G 开新轮。
#[test]
fn 子代理归属未定时留言给空闲孙代理_有raw_绑定后按只入队处理且运行不卡() {
    const G: &str = "孙";
    let (mut m, mut mock) = base();
    spawn(&mut m, &mut mock, C, "旧轮", G, "派生孙", "孙旧轮");
    let mut all = complete(&mut m, &mut mock, G, "孙旧轮", "completed");
    all.extend(complete(&mut m, &mut mock, C, "旧轮", "completed"));
    raw(&mut m, "跟进子", "交给你");
    interacted(&mut m, "跟进子");
    // C 的事件先于它的 turn/started 送达：调用与 Interacted 到达时 C 的新轮归属未定。
    all.extend(raw_call(
        &mut m,
        C,
        "新轮",
        "send_message",
        "留言孙",
        G,
        "顺便说",
    ));
    let (a, b) = SubagentMock::v2_activity("interacted", C, "新轮", "留言孙", G);
    all.extend(apply(&mut m, a));
    all.extend(apply(&mut m, b));
    let actions = apply(&mut m, mock.turn_started(C, "新轮"));
    all.extend(replies(&mut m, &mock, actions));
    delivery(&mut m, "新轮", "入站", "交给你", false);
    assert!(m
        .binding(C, "新轮")
        .is_some_and(|b| b.run_id == R && b.status == BindingStatus::Bound));
    let o = op(&m, "留言孙");
    assert_eq!(o.run_id, R, "C 绑定后接回运行");
    assert!(o.v2_strict && o.v2_queue_only, "恢复工具类型：只入队");
    assert_eq!(o.v2_source, V2EvidenceSource::Raw);
    assert!(o.receivers.is_empty(), "只入队不授予管理权");
    pending(&m, 0);
    let actions = complete(&mut m, &mut mock, C, "新轮", "completed");
    all.extend(replies(&mut m, &mock, actions));
    all.extend(finish(&mut m, &mut mock));
    assert_no_alarm(&all);
}

/// 同一时序下 C 用 followup_task 跟进 G：C 绑定后按 raw 严格配对接纳 G，
/// G 的新轮早于或晚于 C 绑定都能配上并最终收敛。
#[test]
fn 子代理归属未定时跟进孙代理_有raw_绑定后严格配对且孙新轮早晚都收敛() {
    const G: &str = "孙";
    for grandchild_first in [false, true] {
        let (mut m, mut mock) = base();
        spawn(&mut m, &mut mock, C, "旧轮", G, "派生孙", "孙旧轮");
        let mut all = complete(&mut m, &mut mock, G, "孙旧轮", "completed");
        all.extend(complete(&mut m, &mut mock, C, "旧轮", "completed"));
        raw(&mut m, "跟进子", "交给你");
        interacted(&mut m, "跟进子");
        all.extend(raw_call(
            &mut m,
            C,
            "新轮",
            "followup_task",
            "跟进孙",
            G,
            "孙任务",
        ));
        let (a, b) = SubagentMock::v2_activity("interacted", C, "新轮", "跟进孙", G);
        all.extend(apply(&mut m, a));
        all.extend(apply(&mut m, b));
        if grandchild_first {
            let actions = apply(&mut m, mock.turn_started(G, "孙新轮"));
            all.extend(replies(&mut m, &mock, actions));
        }
        let actions = apply(&mut m, mock.turn_started(C, "新轮"));
        all.extend(replies(&mut m, &mock, actions));
        delivery(&mut m, "新轮", "入站", "交给你", false);
        if !grandchild_first {
            let actions = apply(&mut m, mock.turn_started(G, "孙新轮"));
            all.extend(replies(&mut m, &mock, actions));
        }
        let o = op(&m, "跟进孙");
        assert!(
            o.v2_strict && !o.v2_queue_only,
            "恢复严格证据（孙先开轮：{grandchild_first}）"
        );
        assert_eq!(o.v2_source, V2EvidenceSource::Raw);
        assert_eq!(o.v2_fallback, None);
        assert!(
            m.binding(G, "孙新轮")
                .is_some_and(|b| b.run_id == R && b.status == BindingStatus::Bound),
            "孙代理的新轮绑定到本运行（孙先开轮：{grandchild_first}）"
        );
        pending(&m, 0);
        let actions = complete(&mut m, &mut mock, G, "孙新轮", "completed");
        all.extend(replies(&mut m, &mock, actions));
        let actions = complete(&mut m, &mut mock, C, "新轮", "completed");
        all.extend(replies(&mut m, &mock, actions));
        all.extend(finish(&mut m, &mut mock));
        assert_no_alarm(&all);
    }
}

/// 运行转交 broker 托管：走公开的 detach 请求（与 CLI 遇到要人回答的问题时相同）。
pub(super) fn detach(m: &mut Machine) -> Vec<Action> {
    let out = apply(
        m,
        Event::ClientRequest {
            request: crate::claim::ClientRequest {
                conn: 1,
                id: json!(77),
                method: "broker/threads/detach".into(),
                params: Some(json!({"threadId":P,"runId":R,"policy":"on-request"})),
            },
            run_id: None,
        },
    );
    assert!(
        matches!(
            m.claims[P].owner(),
            Some(crate::claim::Owner::Broker { .. })
        ),
        "{out:?}"
    );
    out
}

/// 严格跟进还没被读到时运行合法交接（detach）：开始记录随操作一起迁到新代际，
/// 之后的新轮或消费记录照常配对，运行不卡到收敛期限。
#[test]
fn 严格跟进未读到时运行转交托管_开始记录随交接迁移仍配对并最终收敛() {
    for by_delivery in [false, true] {
        let (mut m, mut mock) = base();
        let mut all = complete(&mut m, &mut mock, C, "旧轮", "completed");
        raw(&mut m, "B", "新工作");
        interacted(&mut m, "B");
        pending(&m, 1);
        let gen = m.manage_context(R).unwrap().gen;
        all.extend(detach(&mut m));
        assert_eq!(m.manage_context(R).unwrap().gen, gen + 1, "前置：交接换代");
        let actions = apply(&mut m, mock.turn_started(C, "新轮"));
        all.extend(replies(&mut m, &mock, actions));
        if by_delivery {
            delivery(&mut m, "新轮", "入站", "新工作", false);
        }
        pending(&m, 0);
        assert_eq!(op(&m, "B").proven_turns[C], ["新轮".to_owned()].into());
        let actions = complete(&mut m, &mut mock, C, "新轮", "completed");
        all.extend(replies(&mut m, &mock, actions));
        all.extend(finish(&mut m, &mut mock));
        assert_no_alarm(&all);
    }
}

/// owner 断开（OwnerGone）同样是合法交接：开始记录迁移，跟进的新轮仍配给本运行。
#[test]
fn 严格跟进未读到时owner断开_开始记录随交接迁移仍配对() {
    let (mut m, mut mock) = base();
    complete(&mut m, &mut mock, C, "旧轮", "completed");
    raw(&mut m, "B", "新工作");
    interacted(&mut m, "B");
    let gen = m.manage_context(R).unwrap().gen;
    apply(&mut m, Event::Disconnected { conn: 1 });
    assert_eq!(m.manage_context(R).unwrap().gen, gen + 1, "前置：交接换代");
    assert!(matches!(
        m.manage_context(R).unwrap().authority,
        Authority::Broker { .. }
    ));
    let actions = apply(&mut m, mock.turn_started(C, "新轮"));
    replies(&mut m, &mock, actions);
    assert_eq!(
        op(&m, "B").proven_turns.get(C),
        Some(&["新轮".to_owned()].into()),
        "交接后跟进的新轮仍配对"
    );
    assert!(m
        .binding(C, "新轮")
        .is_some_and(|b| b.run_id == R && b.status == BindingStatus::Bound));
}

/// 撤权（broker/joined 使运行改由外部接管）与换连接后，开始记录不迁移、不再能配对。
#[test]
fn 撤权与换连接后开始记录不随之迁移_跟进不再配对() {
    for case in ["撤权", "换连接"] {
        let (mut m, mut mock) = base();
        complete(&mut m, &mut mock, C, "旧轮", "completed");
        raw(&mut m, "B", "新工作");
        interacted(&mut m, "B");
        let key = op(&m, "B").key.clone();
        let gen = m.manage_context(R).unwrap().gen;
        match case {
            "撤权" => {
                apply(
                    &mut m,
                    Event::ClientRequest {
                        request: crate::claim::ClientRequest {
                            conn: 1,
                            id: json!(78),
                            method: "broker/joined".into(),
                            params: Some(json!({"threadId":P,"turnId":T})),
                        },
                        run_id: None,
                    },
                );
                assert_eq!(
                    m.manage_context(R).unwrap().authority,
                    Authority::Revoked,
                    "前置：撤权"
                );
            }
            // 生产里没有让连接代际递增的入口（推断：换连接即 broker 重启），与既有用例一样直接换代。
            _ => {
                m.relations.conn_epoch += 1;
                detach(&mut m);
            }
        }
        assert_eq!(
            m.relations
                .aux
                .followup_starts
                .get(&key)
                .map(|s| s.generation),
            Some(gen),
            "{case}：开始记录不迁移"
        );
        let actions = apply(&mut m, mock.turn_started(C, "新轮"));
        replies(&mut m, &mock, actions);
        delivery(&mut m, "新轮", "入站", "新工作", false);
        assert!(op(&m, "B").proven_turns.is_empty(), "{case}");
        assert!(
            !m.binding(C, "新轮")
                .is_some_and(|b| b.run_id == R && b.status == BindingStatus::Bound),
            "{case}"
        );
    }
}

/// 任意接收方线程上收到的消息（raw 的 agent_message），发送方与接收方都给出代理路径。
fn delivery_to(m: &mut Machine, thread: &str, turn: &str, id: &str, sender: &str, message: &str) {
    let text = format!(
        "Message Type: NEW_TASK\nTask name: /root/{thread}\nSender: {sender}\nPayload:\n{message}"
    );
    notify(
        m,
        "rawResponseItem/completed",
        json!({"threadId":thread,"turnId":turn,"item":{
        "type":"agent_message","id":id,"author":sender,"recipient":format!("/root/{thread}"),
        "content":[{"type":"input_text","text":text}]}}),
    );
}

/// 共享服务器上这一轮还没定案是不是自己的（settling）。raw 不进 settling 缓冲，当场交给配对：
/// 这时发出的跟进也要留下开始记录。定案是自己的之后按严格配对，子代理的旧轮结束不算读到跟进，
/// 否则运行会在子代理还没处理跟进时收尾。Interacted 早于或晚于定案到达都一样。
#[test]
fn 共享服务器定案前发出的跟进_定案后严格配对_旧轮结束不算读到() {
    for interacted_after_settle in [false, true] {
        let (mut m, mut mock, context) = setup_pending_mode(true);
        assert!(m.claims[P].settling(), "前置：共享服务器上定案之前");
        notify(
            &mut m,
            "turn/started",
            json!({"threadId":P,"turn":{"id":T,"status":"inProgress","items":[]}}),
        );
        mock.register(C, Some(P), "v2", false, true);
        let (a, b) = SubagentMock::v2_activity("started", P, T, "派生", C);
        apply(&mut m, a);
        apply(&mut m, b);
        let actions = apply(&mut m, mock.turn_started(C, "旧轮"));
        replies(&mut m, &mock, actions);
        raw(&mut m, "跟进", "新工作");
        if !interacted_after_settle {
            interacted(&mut m, "跟进");
        }
        assert!(m.claims[P].settling(), "前置：这些事件都在定案之前到达");
        let actions = apply(
            &mut m,
            Event::Reply {
                context,
                result: Ok(json!({"turn":{"id":T}})),
            },
        );
        replies(&mut m, &mock, actions);
        assert!(!m.claims[P].settling(), "前置：回包后定案为自己的轮");
        if interacted_after_settle {
            interacted(&mut m, "跟进");
        }
        let o = op(&m, "跟进");
        assert!(
            o.v2_strict,
            "Interacted 晚于定案={interacted_after_settle}：开始记录没丢，走严格配对；回退原因 {:?}",
            o.v2_fallback
        );
        assert_eq!(o.v2_fallback, None);
        // 跟进还在队列里，旧轮就结束了：不能算子代理读到了跟进。
        let actions = complete(&mut m, &mut mock, C, "旧轮", "completed");
        replies(&mut m, &mock, actions);
        pending(&m, 1);
        let actions = apply(&mut m, mock.turn_started(C, "新轮"));
        replies(&mut m, &mock, actions);
        pending(&m, 0);
        let actions = complete(&mut m, &mut mock, C, "新轮", "completed");
        replies(&mut m, &mock, actions);
        finish(&mut m, &mut mock);
    }
}

/// 定案为不是自己的（被别的客户端的轮吸收）：settling 期间存下的开始记录不能让跟进变成本运行的严格配对。
#[test]
fn 共享服务器定案为别人的轮_定案前存下的开始记录不把跟进归给本运行() {
    let (mut m, mut mock, _context) = setup_pending_mode(true);
    mock.register(C, Some(P), "v2", false, true);
    notify(
        &mut m,
        "turn/started",
        json!({"threadId":P,"turn":{"id":"别人的轮","status":"inProgress","items":[]}}),
    );
    notify(
        &mut m,
        "rawResponseItem/completed",
        json!({"threadId":P,"turnId":"别人的轮","item":{
        "type":"function_call","name":"followup_task","call_id":"别人的跟进",
        "arguments":json!({"target":C,"message":"别人的工作"}).to_string()}}),
    );
    let (a, b) = SubagentMock::v2_activity("interacted", P, "别人的轮", "别人的跟进", C);
    apply(&mut m, a);
    apply(&mut m, b);
    // 吸收判定的结果：这一轮是别的客户端的。
    m.settle(P, true, Some("别人的轮".into()), &mut Vec::new());
    assert!(m.claims[P].joined(), "前置：定案为别人的轮");
    let o = op(&m, "别人的跟进");
    assert_eq!(o.outcome, EvidenceOutcome::Unproven);
    assert!(o.proven_turns.is_empty());
    assert!(
        m.manage_context(R)
            .is_none_or(|c| !c.members.contains_key(C)),
        "接收方不归本运行管理"
    );
    assert!(m.relations.aux.followup_starts.is_empty(), "开始记录已清掉");
    assert!(m.v2_index_consistent());
}

/// 共享服务器上的 detached review 溢出、回包给出别的子线程：父认领被放弃后直接移走，不经过定案回调，
/// 也不走普通释放。等它定案的证据同样要结算，否则永不到期、占着操作表。
#[test]
fn 共享服务器review溢出迁移到子线程_等定案的跟进要结算不残留() {
    let mut m = Machine {
        shared: true,
        ..Machine::default()
    };
    m.clock.mono_ms = 1_000;
    m.clock.now_ms = 100_000;
    assert!(m.enable_convergence(ready()));
    apply(&mut m, Event::Connected { conn: 1 });
    apply(&mut m, Event::Initialized { conn: 1, cxb: true });
    m.record(P).subscribed = true;
    let out = apply(
        &mut m,
        Event::ClientRequest {
            request: crate::claim::ClientRequest {
                conn: 1,
                id: json!(1),
                method: "review/start".into(),
                params: Some(json!({"threadId":P,"delivery":"detached"})),
            },
            run_id: Some(R.into()),
        },
    );
    let context = out
        .into_iter()
        .find_map(|a| match a {
            Action::Request(c) if c.method == "review/start" => Some(c),
            _ => None,
        })
        .expect("发出 review/start");
    let mut mock = SubagentMock::default();
    mock.register(P, None, "v2", false, true);
    mock.register(C, Some(P), "v2", false, true);
    raw(&mut m, "跟进", "新工作");
    interacted(&mut m, "跟进");
    assert!(m.claims[P].settling(), "前置：定案之前");
    let key = op(&m, "跟进").key.clone();
    assert!(m.relations.aux.parent_waits.contains_key(&key), "前置");
    m.settling_overflow(P, &mut Vec::new());
    let actions = apply(
        &mut m,
        Event::Reply {
            context,
            result: Ok(
                json!({"reviewThreadId":"审查子线程","turn":{"id":"审查轮","status":"inProgress","items":[]}}),
            ),
        },
    );
    replies(&mut m, &mock, actions);
    assert!(!m.claims.contains_key(P), "前置：父认领已移走");
    assert!(
        !m.relations.aux.parent_waits.contains_key(&key),
        "等定案的记录要清掉"
    );
    assert_eq!(op(&m, "跟进").outcome, EvidenceOutcome::Unproven);
}

/// 只有等证据等到期限才算「证明超时」（日志要留缺口）；上游 completed 这种只记事实的条目不算。
#[test]
fn 等归属的跟进到期才标证明超时_只记事实的条目不标() {
    const G: &str = "孙";
    let (mut m, mut mock) = base();
    mock.register(G, Some(C), "v2", false, true);
    // C 的这一轮还没绑定：转手跟进孙代理的操作在等绑定。
    let (a, b) = SubagentMock::v2_activity("interacted", C, "未绑定轮", "转手", G);
    apply(&mut m, a);
    apply(&mut m, b);
    let (a, b) = SubagentMock::v2_activity("completed", P, T, "已完成", C);
    apply(&mut m, a);
    apply(&mut m, b);
    // 普通到期：派生了子代理，一直没等到它的首轮。
    mock.register("迟子", Some(P), "v1", false, true);
    let (a, b) = SubagentMock::v1_spawn(P, T, "迟派生", "迟子", false);
    apply(&mut m, a);
    apply(&mut m, b);
    assert_eq!(op(&m, "转手").outcome, EvidenceOutcome::Pending, "前置");
    assert_eq!(op(&m, "迟派生").outcome, EvidenceOutcome::Pending, "前置");
    assert_eq!(op(&m, "已完成").outcome, EvidenceOutcome::Unproven, "前置");
    m.clock.mono_ms += crate::relations::EVIDENCE_TTL_MS + 1;
    m.facts_expire(&mut Vec::new());
    assert_eq!(op(&m, "转手").outcome, EvidenceOutcome::Unproven);
    assert!(op(&m, "转手").proof_expired, "等到期限才转未证明");
    assert_eq!(op(&m, "迟派生").outcome, EvidenceOutcome::Unproven);
    assert!(op(&m, "迟派生").proof_expired, "普通到期同样算证明超时");
    assert!(
        !op(&m, "已完成").proof_expired,
        "只记事实的条目不算证明超时"
    );
}

/// 定案是别人的轮时，这段时间的开始记录和它对应的消费记录一起清掉。同一连接上新运行发出同内容的跟进时，
/// 既不会认定旧轮（否则运行可能提前收尾），也不会被旧记录挡住（否则卡到期限）；过了开始记录的期限也一样。
#[test]
fn 共享服务器定案为别人的轮后_同内容新跟进不认旧消费记录也不被挡住() {
    const OTHER: &str = "别人的轮";
    const R2: &str = "第二运行";
    const T2: &str = "第二父轮";
    for late in [false, true] {
        let (mut m, mut mock, _context) = setup_pending_mode(true);
        mock.register(C, Some(P), "v2", false, true);
        notify(
            &mut m,
            "turn/started",
            json!({"threadId":P,"turn":{"id":OTHER,"status":"inProgress","items":[]}}),
        );
        // 别人的轮派生了这个子代理：血缘属于这个父线程，运行归属待定。
        let (a, b) = SubagentMock::v2_activity("started", P, OTHER, "别人的派生", C);
        apply(&mut m, a);
        apply(&mut m, b);
        let actions = apply(&mut m, mock.turn_started(C, "旧轮"));
        replies(&mut m, &mock, actions);
        notify(
            &mut m,
            "rawResponseItem/completed",
            json!({"threadId":P,"turnId":OTHER,"item":{
        "type":"function_call","name":"followup_task","call_id":"别人的跟进",
        "arguments":json!({"target":C,"message":"同一句"}).to_string()}}),
        );
        let (a, b) = SubagentMock::v2_activity("interacted", P, OTHER, "别人的跟进", C);
        apply(&mut m, a);
        apply(&mut m, b);
        if late {
            // 消费记录比开始记录晚 10 秒：开始记录过期时，消费记录还在。
            m.clock.mono_ms += 10_000;
        }
        delivery(&mut m, "旧轮", "入站旧", "同一句", false);
        // 同样内容发给别的子代理的记录，不属于这条跟进，不能被一起清掉。
        delivery_to(&mut m, "别的子", "别的轮", "入站别", "/root", "同一句");
        m.settle(P, true, Some(OTHER.into()), &mut Vec::new());
        assert!(m.claims[P].joined(), "前置：定案为别人的轮");
        let ids: Vec<String> = m
            .relations
            .aux
            .v2_deliveries
            .iter()
            .map(|d| d.id.clone())
            .collect();
        let starts_left = m.relations.aux.followup_starts.len();
        let actions = complete(&mut m, &mut mock, P, OTHER, "completed");
        replies(&mut m, &mock, actions);
        assert!(!m.claims.contains_key(P), "前置：认领已释放");

        let out = apply(
            &mut m,
            Event::ClientRequest {
                request: crate::claim::ClientRequest {
                    conn: 1,
                    id: json!(2),
                    method: "turn/start".into(),
                    params: Some(json!({"threadId":P,"input":[{"type":"text","text":"新任务"}]})),
                },
                run_id: Some(R2.into()),
            },
        );
        let context = out
            .into_iter()
            .find_map(|a| match a {
                Action::Request(c) if c.method == "turn/start" => Some(c),
                _ => None,
            })
            .expect("新运行发出 turn/start");
        notify(
            &mut m,
            "turn/started",
            json!({"threadId":P,"turn":{"id":T2,"status":"inProgress","items":[]}}),
        );
        let actions = apply(
            &mut m,
            Event::Reply {
                context,
                result: Ok(json!({"turn":{"id":T2}})),
            },
        );
        replies(&mut m, &mock, actions);
        assert!(!m.claims[P].settling(), "前置：新运行已定案");
        if late {
            m.clock.mono_ms += crate::relations::EVIDENCE_TTL_MS - 9_000;
        }

        notify(
            &mut m,
            "rawResponseItem/completed",
            json!({"threadId":P,"turnId":T2,"item":{
        "type":"function_call","name":"followup_task","call_id":"新跟进",
        "arguments":json!({"target":C,"message":"同一句"}).to_string()}}),
        );
        let (a, b) = SubagentMock::v2_activity("interacted", P, T2, "新跟进", C);
        let actions = apply(&mut m, a);
        replies(&mut m, &mock, actions);
        let actions = apply(&mut m, b);
        replies(&mut m, &mock, actions);
        let o = op(&m, "新跟进");
        assert!(o.v2_strict, "前置：新跟进走严格配对");
        assert!(
            o.rejected_receivers.is_empty(),
            "前置：接收方是这个父线程的子代理：{:?}",
            o.rejected_receivers
        );
        assert_eq!(
            o.proven_turns.get(C),
            None,
            "晚到={late}：不能认定案前的旧消费记录"
        );
        let actions = apply(&mut m, mock.turn_started(C, "新轮"));
        replies(&mut m, &mock, actions);
        delivery(&mut m, "新轮", "入站新", "同一句", false);
        assert_eq!(
            op(&m, "新跟进").proven_turns.get(C),
            Some(&["新轮".to_owned()].into()),
            "晚到={late}：不能被旧记录挡住，配上真正的新轮"
        );
        // 定案后立刻清的范围：只清这条跟进对应的消费记录和开始记录。
        assert_eq!(ids, ["入站别"], "只清这条跟进对应的消费记录");
        assert_eq!(starts_left, 0, "开始记录也清掉");
        assert!(m.v2_index_consistent());
    }
}

/// 清理未归属的证据时，操作还有效的不动：它的开始记录还要用来配对。
#[test]
fn 清理未归属证据时操作还有效的开始记录不动() {
    let (mut m, mut mock) = base();
    let actions = complete(&mut m, &mut mock, C, "旧轮", "completed");
    replies(&mut m, &mock, actions);
    raw(&mut m, "跟进", "新工作");
    interacted(&mut m, "跟进");
    let key = op(&m, "跟进").key.clone();
    assert_eq!(
        op(&m, "跟进").outcome,
        EvidenceOutcome::Pending,
        "前置：操作还在等配对"
    );
    assert!(m.relations.aux.followup_starts.contains_key(&key), "前置");
    m.v2_forget_unowned(P, R);
    assert!(
        m.relations.aux.followup_starts.contains_key(&key),
        "操作还有效，开始记录不能清"
    );
    assert!(m.v2_index_consistent());
}

/// 启动失败时认领走释放路径，不经过定案回调：等定案的操作也要结算，否则永不到期、占着操作表，
/// 开始记录也跟着留下。先断开（有停止操作）再失败时，停止路径会先清掉 settling 标记，同样要结算。
#[test]
fn 共享服务器定案前启动失败_等定案的跟进和开始记录都结算不残留() {
    for disconnect in [false, true] {
        let (mut m, mut mock, context) = setup_pending_mode(true);
        mock.register(C, Some(P), "v2", false, true);
        raw(&mut m, "跟进", "新工作");
        raw(&mut m, "只有开始记录", "还没等到 Interacted");
        interacted(&mut m, "跟进");
        assert!(m.claims[P].settling(), "前置：定案之前");
        assert_eq!(
            m.relations.aux.followup_starts.len(),
            2,
            "前置：开始记录已存下"
        );
        assert_eq!(op(&m, "跟进").outcome, EvidenceOutcome::Pending, "前置");
        let key = op(&m, "跟进").key.clone();
        if disconnect {
            let actions = apply(&mut m, Event::Disconnected { conn: 1 });
            replies(&mut m, &mock, actions);
        }
        let actions = apply(
            &mut m,
            Event::Reply {
                context,
                result: Err(json!({"code":-32000,"message":"启动失败"})),
            },
        );
        replies(&mut m, &mock, actions);
        if disconnect {
            // 断开时有停止操作：启动失败只把 settling 标记清掉，认领等停止确认后才释放。
            assert!(m.stop_busy(P), "前置：有停止操作在进行");
            assert!(!m.claims[P].settling(), "前置：settling 标记已被清掉");
            m.release_with_kind(P, crate::runs::ReleaseKind::StopConfirmed, &mut Vec::new());
        }
        assert!(
            !m.claims.contains_key(P),
            "断开：{disconnect}：前置：认领已释放"
        );
        assert!(
            !m.relations.aux.parent_waits.contains_key(&key),
            "断开：{disconnect}：等定案的记录要清掉"
        );
        assert_eq!(op(&m, "跟进").outcome, EvidenceOutcome::Unproven);
        assert!(!m.op_confirmed(&key), "不再占着待定");
        assert!(
            m.relations.aux.followup_starts.is_empty(),
            "断开：{disconnect}：开始记录要清掉：{:?}",
            m.relations.aux.followup_starts.keys()
        );
        assert!(m.v2_index_consistent());
    }
}

/// raw 下非根发起方（子代理 C）跟进它的子代理 G，跟进被 G 的忙碌轮吸收、G 不再开新轮：
/// 只能靠消费记录配对，而消费记录按发起方的代理路径比对。C 的血缘边没有代理路径时，
/// 从上游 subAgentActivity 条目给出的 agentPath（该子代理的规范路径）得到，不让运行卡到期限。
#[test]
fn raw下非根发起方的代理路径来自活动条目_孙代理忙碌轮吸收的跟进按消费记录配对并收敛() {
    const G: &str = "孙";
    for source in ["父线程跟进", "派生"] {
        let (mut m, mut mock) = base();
        let mut all = vec![];
        let sender = if source == "派生" {
            // C 由 v2 派生：started 条目给出它的代理路径；C 的首轮由派生绑定。
            const C2: &str = "子二";
            mock.register(C2, Some(P), "v2", false, true);
            let (a, b) = SubagentMock::v2_activity("started", P, T, "派生二", C2);
            all.extend(apply(&mut m, a));
            all.extend(apply(&mut m, b));
            let actions = apply(&mut m, mock.turn_started(C2, "二轮"));
            all.extend(replies(&mut m, &mock, actions));
            (C2, "二轮")
        } else {
            // C 是冷恢复或 v1 派生的旧子代理，父线程跟进它：Interacted 条目给出它的代理路径。
            all.extend(complete(&mut m, &mut mock, C, "旧轮", "completed"));
            raw(&mut m, "跟进子", "交给你");
            interacted(&mut m, "跟进子");
            let actions = apply(&mut m, mock.turn_started(C, "新轮"));
            all.extend(replies(&mut m, &mock, actions));
            delivery(&mut m, "新轮", "入站", "交给你", false);
            (C, "新轮")
        };
        let (c, c_turn) = sender;
        assert!(
            m.thread_relation(c)
                .is_none_or(|e| e.source_summary["agentPath"].is_null()),
            "前置：血缘边上没有 C 的代理路径"
        );
        assert!(m
            .binding(c, c_turn)
            .is_some_and(|b| b.run_id == R && b.status == BindingStatus::Bound));
        // G 是 C 的子代理，正在跑一轮。
        spawn(&mut m, &mut mock, c, c_turn, G, "派生孙", "孙忙碌轮");
        all.extend(raw_call(
            &mut m,
            c,
            c_turn,
            "followup_task",
            "跟进孙",
            G,
            "补充要求",
        ));
        let (a, b) = SubagentMock::v2_activity("interacted", c, c_turn, "跟进孙", G);
        all.extend(apply(&mut m, a));
        all.extend(apply(&mut m, b));
        assert!(op(&m, "跟进孙").v2_strict);
        // G 的忙碌轮吸收了跟进：留下消费记录，不开新轮。
        delivery_to(
            &mut m,
            G,
            "孙忙碌轮",
            "入站孙",
            &format!("/root/{c}"),
            "补充要求",
        );
        assert_eq!(
            op(&m, "跟进孙").proven_turns.get(G),
            Some(&["孙忙碌轮".to_owned()].into()),
            "{source}：按消费记录配到吸收它的忙碌轮"
        );
        pending(&m, 0);
        let actions = complete(&mut m, &mut mock, G, "孙忙碌轮", "completed");
        all.extend(replies(&mut m, &mock, actions));
        let actions = complete(&mut m, &mut mock, c, c_turn, "completed");
        all.extend(replies(&mut m, &mock, actions));
        if source == "派生" {
            all.extend(complete(&mut m, &mut mock, C, "旧轮", "completed"));
        }
        all.extend(finish(&mut m, &mut mock));
        assert_no_alarm(&all);
    }
}
