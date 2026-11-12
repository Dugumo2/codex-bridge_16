use super::*;
use crate::claim::ReqKind;
use crate::relations::*;
use crate::runs::{RunOpen, RunState};

fn setup() -> Machine {
    let mut m = Machine::new(false);
    m.clock.now_ms = 1000;
    m.runs.epoch = 42;
    open(&mut m, "run-a", "root");
    m
}

fn open(m: &mut Machine, run: &str, root: &str) {
    m.run_opened(
        RunOpen {
            run_id: run.into(),
            thread_id: root.into(),
            conn: None,
            kind: ReqKind::TurnStart,
            joined: false,
            input: String::new(),
            continued_from: None,
        },
        &mut Vec::new(),
    );
    m.relations.contexts.insert(
        run.into(),
        ManageContext {
            identity: RunIdentity {
                run_id: run.into(),
                logical_root_thread_id: root.into(),
                execution_thread_id: root.into(),
            },
            gen: 1,
            authority: Authority::Owner(1),
            policy: crate::policy::Policy::OnRequest,
            members: Default::default(),
            in_flight_spawns: Default::default(),
            revision: 0,
            discovery: DiscoveryState {
                missing: Default::default(),
                complete: true,
                version: 0,
                incomplete_reasons: vec![],
            },
            cleanup: None,
        },
    );
}

fn candidate(m: &mut Machine, thread: &str, parent: &str) {
    m.relations.edges.insert(
        thread.into(),
        RelationEdge {
            child_thread_id: thread.into(),
            parent_thread_id: parent.into(),
            kind: RelationKind::Subagent,
            source_summary: Value::Null,
            can_accept_direct_input: Some(false),
            agent_version: AgentVersion::V1,
            discovered_by: DiscoveredBy::Started,
        },
    );
}

fn bind(m: &mut Machine, run: &str, thread: &str, turn: &str, parent: &str) {
    candidate(m, thread, parent);
    m.relations.bindings.insert(
        (thread.into(), turn.into()),
        ActivityBinding {
            thread_id: thread.into(),
            turn_id: turn.into(),
            run_id: run.into(),
            parent_thread_id: parent.into(),
            evidence: BindingEvidence::V1Spawn,
            operation: EvidenceKey {
                parent_thread_id: parent.into(),
                parent_turn_id: "parent-turn".into(),
                item_id: "spawn".into(),
            },
            status: BindingStatus::Bound,
        },
    );
    m.relations
        .aux
        .observed
        .entry(thread.into())
        .or_default()
        .push_back(TurnObservation {
            turn: turn.into(),
            started: true,
            completed: false,
            seen_ms: m.clock.mono_ms,
        });
}

fn logs(actions: &[Action]) -> Vec<(String, String, String, Value)> {
    actions
        .iter()
        .flat_map(|a| match a {
            Action::Store(StoreOp::AppendLog { short_id, lines }) => lines
                .iter()
                .map(|line| {
                    let fields: Vec<_> = line.splitn(4, ' ').collect();
                    (
                        short_id.clone(),
                        fields[1].into(),
                        fields[2].into(),
                        serde_json::from_str(
                            fields[3].strip_prefix("\t@cxb2 ").expect("结构化标记"),
                        )
                        .expect("完整 JSON"),
                    )
                })
                .collect(),
            _ => vec![],
        })
        .collect()
}

fn item(thread: &str, turn: &str) -> Option<Value> {
    Some(
        json!({"threadId":thread,"turnId":turn,"item":{"id":"same-item","type":"agentMessage","text":"同毫秒同正文"}}),
    )
}

/// 上游格式（UUIDv7）的轮号，前 48 位是开始毫秒。
fn turn_at(ms: u64) -> String {
    format!(
        "{:08x}-{:04x}-7000-8000-000000000000",
        ms >> 16,
        ms & 0xffff
    )
}

/// 给这一轮暂存一条摘要并等到到期，返回有没有写缺口。
fn expires_with_gap(m: &mut Machine, thread: &str, turn: &str) -> bool {
    m.coverage_complete(thread, turn, &json!({"status":"completed"}), 3, true);
    let mut a = vec![];
    m.buffer_child_event_log(thread, "item/completed", &item(thread, turn), false, &mut a);
    assert_eq!(m.runs.event_log.pending.len(), 1, "前置：已暂存");
    let generation = m.runs.event_log.timer.unwrap();
    m.clock.mono_ms += PENDING_MS;
    m.clock.now_ms += PENDING_MS;
    a.clear();
    m.sweep_event_log(generation, &mut a);
    assert!(m.runs.event_log.pending.is_empty());
    logs(&a).iter().any(|l| l.2 == "记录缺口")
}

#[test]
fn tree_log_实际tab边界与正文标志不会混淆() {
    let mut m = setup();
    let mut actions = vec![];
    m.runs.log(
        "run-a",
        1000,
        "轮次开始",
        "@cxb2 正文\t控制字符\n仍是摘要",
        &mut actions,
    );
    let lines = logs(&actions);
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0].3["summary"], "@cxb2 正文 控制字符 仍是摘要");
    let Action::Store(StoreOp::AppendLog { lines, .. }) = &actions[0] else {
        panic!("应写入日志");
    };
    assert_eq!(lines[0].chars().filter(|c| *c == '\t').count(), 1);
    assert!(lines[0].contains(" 轮次开始 \t@cxb2 {"));
}

#[test]
fn tree_log_发现中间态与候选分类不写缺口_恢复完整后结束零误报() {
    let mut m = setup();
    let mut actions = vec![];
    candidate(&mut m, "child", "root");
    m.refresh_classification("run-a", "child", "Discovery", &mut actions);
    assert!(!m.manage_context("run-a").unwrap().discovery.complete);
    m.log_member_change("run-a", "child", MemberChange::Discovery, &mut actions);
    assert!(logs(&actions).iter().any(|l| l.2 == "候选分类"));
    assert!(logs(&actions)
        .iter()
        .all(|l| l.3.get("coverageIssue").is_none()));
    m.relations
        .contexts
        .get_mut("run-a")
        .unwrap()
        .discovery
        .complete = true;
    m.run_released("run-a", crate::runs::ReleaseKind::Natural, &mut actions);
    assert_eq!(m.runs.runs["run-a"].state, RunState::Ended);
    assert!(logs(&actions).iter().all(|l| l.2 != "记录缺口"));
}

#[test]
fn tree_log_定案时发现仍不完整必须记录真缺口() {
    for shutdown in [false, true] {
        let mut m = setup();
        let state = &mut m.relations.contexts.get_mut("run-a").unwrap().discovery;
        state.complete = false;
        state.version = 1;
        state.incomplete_reasons.push("listFailed".into());
        let mut actions = vec![];
        if shutdown {
            m.shutdown_runs(&mut actions);
            m.shutdown_runs(&mut actions);
        } else {
            m.run_released("run-a", crate::runs::ReleaseKind::Natural, &mut actions);
            m.run_released("run-a", crate::runs::ReleaseKind::Natural, &mut actions);
        }
        let gaps: Vec<_> = logs(&actions)
            .into_iter()
            .filter(|l| l.3["coverageIssue"] == "discoveryIncomplete")
            .collect();
        assert_eq!(gaps.len(), 1, "每次定案只记录一次真实缺口，关闭={shutdown}");
        assert_eq!(gaps[0].2, "记录缺口");
        assert_eq!(gaps[0].3["threadId"], "root");
    }
}

#[test]
fn tree_log_根子孙同文件身份与同正文独立事件以及review来源() {
    let mut m = setup();
    bind(&mut m, "run-a", "child", "ct", "root");
    bind(&mut m, "run-a", "grandchild", "gt", "child");
    let mut a = vec![];
    m.runs.log("run-a", 1000, "运行开始", "开始", &mut a);
    m.log_bound_child_event(
        "run-a",
        "child",
        "ct",
        "item/completed",
        &item("child", "ct"),
        &mut a,
    );
    m.log_bound_child_event(
        "run-a",
        "grandchild",
        "gt",
        "item/completed",
        &item("grandchild", "gt"),
        &mut a,
    );
    m.runs.runs.get_mut("run-a").unwrap().review_thread_id = Some("review".into());
    m.runs.log_owned(
        "run-a",
        "review",
        Some("rt"),
        Some("ri"),
        1000,
        "条目完成",
        "审查",
        false,
        &mut a,
    );
    let lines = logs(&a);
    assert_eq!(lines.len(), 4);
    assert!(lines.iter().all(|l| l.0 == lines[0].0 && l.1 == "run-a"));
    assert_eq!(
        lines
            .iter()
            .map(|l| l.3["threadId"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["root", "child", "grandchild", "review"]
    );
    assert_eq!(lines[2].3["parentThreadId"], "child");
    assert_eq!(lines[3].3["sourceRole"], "execution");
    assert_eq!(lines[1].3["summary"], lines[2].3["summary"]);
    assert_ne!(lines[1].3["eventId"], lines[2].3["eventId"]);
}

#[test]
fn tree_log_逐轮逐运行校验不跨账也不接纳冲突() {
    let mut m = setup();
    open(&mut m, "run-b", "other-root");
    bind(&mut m, "run-a", "child", "old-turn", "root");
    bind(&mut m, "run-b", "child", "new-turn", "other-root");
    let mut a = vec![];
    m.log_bound_child_event(
        "run-a",
        "child",
        "new-turn",
        "item/completed",
        &item("child", "new-turn"),
        &mut a,
    );
    m.log_bound_child_event(
        "run-a",
        "child",
        "unknown-turn",
        "item/completed",
        &item("child", "unknown-turn"),
        &mut a,
    );
    assert!(logs(&a).is_empty());
    m.log_bound_child_event(
        "run-a",
        "child",
        "old-turn",
        "item/completed",
        &item("child", "old-turn"),
        &mut a,
    );
    m.log_bound_child_event(
        "run-b",
        "child",
        "new-turn",
        "item/completed",
        &item("child", "new-turn"),
        &mut a,
    );
    assert_eq!(
        logs(&a).iter().map(|l| l.1.as_str()).collect::<Vec<_>>(),
        ["run-a", "run-b"]
    );
    m.relations
        .bindings
        .get_mut(&("child".into(), "old-turn".into()))
        .unwrap()
        .status = BindingStatus::Conflict("独立认领".into());
    a.clear();
    m.log_bound_child_event(
        "run-a",
        "child",
        "old-turn",
        "item/completed",
        &item("child", "old-turn"),
        &mut a,
    );
    assert!(logs(&a).is_empty());
}

#[test]
fn tree_log_完成首次接纳且父轮预算空转进度不因日志改变() {
    let mut m = setup();
    bind(&mut m, "run-a", "child", "ct", "root");
    let mut a = vec![];
    let before = m.runs.runs["run-a"].clone();
    let guard = m.loop_guards.clone();
    let params =
        Some(json!({"threadId":"child","turn":{"id":"ct","status":"completed","items":[]}}));
    assert!(m.dispatch_child_diagnostic("child", "turn/completed", &params, false, &mut a));
    assert!(m.dispatch_child_diagnostic("child", "turn/completed", &params, false, &mut a));
    assert_eq!(logs(&a).iter().filter(|l| l.2 == "轮次完成").count(), 1);
    let after = &m.runs.runs["run-a"];
    assert_eq!(after.state, RunState::Running);
    assert_eq!(after.turns, before.turns);
    assert_eq!(after.turns_total, before.turns_total);
    assert_eq!(after.progress, before.progress);
    assert_eq!(after.run_budget, before.run_budget);
    assert_eq!(m.loop_guards, guard);
}

#[test]
fn tree_log_绑定前完成只补摘要不重放诊断与完成状态() {
    let mut m = setup();
    candidate(&mut m, "child", "root");
    let mut a = vec![];
    m.buffer_child_event_log(
        "child",
        "item/completed",
        &item("child", "ct"),
        false,
        &mut a,
    );
    let completed = Some(
        json!({"threadId":"child","turn":{"id":"ct","status":"failed","error":{"message":"Authorization: secret-value"}}}),
    );
    m.buffer_child_event_log("child", "turn/completed", &completed, false, &mut a);
    assert!(logs(&a).is_empty());
    m.records
        .entry("child".into())
        .or_default()
        .ended_turns
        .push_back("ct".into());
    bind(&mut m, "run-a", "child", "ct", "root");
    let before = m.runs.runs["run-a"].clone();
    let active = m.runs.active["run-a"].clone();
    m.clock.now_ms = 2000;
    m.flush_bound_event_log("run-a", "child", "ct", "root", &mut a);
    assert_eq!(m.runs.runs["run-a"], before);
    assert_eq!(m.runs.active["run-a"], active);
    assert_eq!(logs(&a).len(), 3);
    assert!(logs(&a).iter().all(|l| l.3["late"] == true));
    assert!(!format!("{a:?}").contains("secret-value"));
    assert!(m.runs.event_log.pending.is_empty());
    assert_eq!(m.runs.event_log.bytes, 0);
    a.clear();
    m.flush_bound_event_log("run-a", "child", "ct", "root", &mut a);
    assert!(logs(&a).is_empty());
}

#[test]
fn tree_log_暂存过期超限与关闭清理() {
    let mut m = setup();
    candidate(&mut m, "child", "root");
    let mut a = vec![];
    for _ in 0..1000 {
        m.buffer_child_event_log(
            "child",
            "item/completed",
            &item("child", "ct"),
            false,
            &mut a,
        );
    }
    assert!(m.runs.event_log.bytes <= PER_THREAD_BYTES);
    assert!(logs(&a)
        .iter()
        .any(|l| l.3["coverageIssue"] == "beforeBindingUnknown"));
    let generation = m.runs.event_log.timer.unwrap();
    m.clock.mono_ms = PENDING_MS;
    a.clear();
    m.sweep_event_log(generation, &mut a);
    assert!(m.runs.event_log.pending.is_empty());
    assert_eq!(m.runs.event_log.bytes, 0);
    assert!(m.runs.event_log.timer.is_none());
    // 归属仍未决（没有任何历史或管理证据）的暂存到期必须照常告警。
    assert!(!logs(&a).is_empty());
    assert!(logs(&a).iter().all(|l| l.2 == "记录缺口"
        && l.3["coverageIssue"] == "beforeBindingUnknown"
        && l.3["summary"] == "归属确认前摘要暂存过期"));
    m.buffer_child_event_log(
        "child",
        "item/completed",
        &item("child", "ct"),
        false,
        &mut a,
    );
    a.clear();
    m.shutdown_runs(&mut a);
    assert!(logs(&a)
        .iter()
        .any(|l| l.2 == "记录缺口" && l.3["summary"] == "broker 关闭时归属尚未确认"));
    assert!(a.iter().any(|a| matches!(
        a,
        Action::Unschedule {
            key: crate::claim::TimerKey::EventLogSweep
        }
    )));
    assert!(m.runs.event_log.pending.is_empty());
    assert!(m.runs.event_log.timer.is_none());
    assert_eq!(m.runs.event_log.bytes, 0);
}

#[test]
fn tree_log_全局暂存上限与未知线程不暂存() {
    let mut m = setup();
    let mut a = vec![];
    m.buffer_child_event_log(
        "unknown",
        "item/completed",
        &item("unknown", "t"),
        false,
        &mut a,
    );
    assert!(m.runs.event_log.pending.is_empty());
    for n in 0..25 {
        let thread = format!("child-{n}");
        candidate(&mut m, &thread, "root");
        for _ in 0..300 {
            m.buffer_child_event_log(
                &thread,
                "item/completed",
                &item(&thread, "t"),
                false,
                &mut a,
            );
        }
    }
    assert!(m.runs.event_log.bytes <= GLOBAL_BYTES);
    assert!(m.runs.event_log.bytes > GLOBAL_BYTES - 1024);
    assert!(logs(&a)
        .iter()
        .any(|l| l.3["coverageIssue"] == "beforeBindingUnknown"));
}

#[test]
fn tree_log_摘要截断标记与长身份的json保持完整() {
    let mut m = setup();
    let thread = "子线程".repeat(200);
    let turn = "轮".repeat(300);
    bind(&mut m, "run-a", &thread, &turn, "root");
    for (text, truncated) in [("甲".repeat(80), false), ("乙".repeat(81), true)] {
        let mut a = vec![];
        let params = Some(
            json!({"threadId":thread,"turnId":turn,"item":{"id":"条目","type":"agentMessage","text":text}}),
        );
        m.log_bound_child_event("run-a", &thread, &turn, "item/completed", &params, &mut a);
        let lines = logs(&a);
        assert_eq!(lines[0].3["threadId"], thread);
        assert_eq!(lines[0].3["turnId"], turn);
        assert_eq!(lines[0].3["truncated"], truncated);
        assert_eq!(lines[0].3["summary"].as_str().unwrap().chars().count(), 80);
    }
}

fn evidence_to(
    m: &mut Machine,
    receiver: &str,
    kind: EvidenceKind,
    outcome: EvidenceOutcome,
    proof_expired: bool,
    rejected: bool,
) {
    let id = "条目";
    let key = EvidenceKey {
        parent_thread_id: "发起方".into(),
        parent_turn_id: "发起轮".into(),
        item_id: id.into(),
    };
    m.relations.operations.insert(
        key.clone(),
        EvidenceOperation {
            key,
            run_id: String::new(),
            conn_epoch: m.relations.conn_epoch,
            authority_gen: 0,
            kind,
            started_seen: true,
            completed_seen: true,
            outcome,
            receivers: [receiver.to_owned()].into(),
            rejected_receivers: if rejected {
                [(receiver.to_owned(), FollowupRejection::Peer)].into()
            } else {
                Default::default()
            },
            proven_turns: Default::default(),
            baseline_turns: Default::default(),
            v2_strict: false,
            v2_queue_only: false,
            v2_source: V2EvidenceSource::None,
            v2_fallback: None,
            rollout_arrival: None,
            proof_expired,
            lenient_floor: None,
            first_seen_ms: 0,
            expires_at_ms: 1,
            parent_ended: false,
            failure: None,
        },
    );
}

/// 有历史记录的线程收到过跟进，但证据过期转成了未证明：这只说明归属没证明出来，不等于确定不归本运行，
/// 暂存到期时要留缺口。只记事实的未证明（上游 completed 通知）、定案不是自己的、明确被拒绝的（平级、向上）
/// 都不算未决，否则历史子代理正常结束也会误报。证据超时后又收到明确失败的，同样不再未决。
#[test]
fn tree_log_跟进证据过期未证明的历史线程仍写缺口_其余未证明不写() {
    use EvidenceOutcome::{Failed, Unproven};
    for (kind, outcome, expired, rejected, expect_gap) in [
        (EvidenceKind::V2Interacted, Unproven, true, false, true),
        (EvidenceKind::V2Interacted, Unproven, true, true, false),
        (EvidenceKind::V2Interacted, Unproven, false, false, false),
        (EvidenceKind::V2Completed, Unproven, false, false, false),
        (EvidenceKind::V1ResumeAgent, Failed, true, false, false),
    ] {
        let mut m = setup();
        candidate(&mut m, "历史线程", "root");
        m.runs.index.child_history = serde_json::from_value(json!({
            "coverage":"verified","retainedAfter":0,
            "entries":{"历史线程":{"run-old":{"rootThreadId":"root","observedAt":1}}}
        }))
        .unwrap();
        evidence_to(&mut m, "历史线程", kind, outcome, expired, rejected);
        assert!(m.historical_subagent_for_run("run-a", "历史线程"), "前置");
        let mut a = vec![];
        m.buffer_child_event_log(
            "历史线程",
            "item/completed",
            &item("历史线程", "t"),
            false,
            &mut a,
        );
        let generation = m.runs.event_log.timer.unwrap();
        m.clock.mono_ms = PENDING_MS;
        a.clear();
        m.sweep_event_log(generation, &mut a);
        let gap = logs(&a).iter().any(|l| l.2 == "记录缺口");
        assert_eq!(
            gap, expect_gap,
            "{kind:?} {outcome:?} 证据过期：{expired}，明确被拒绝：{rejected}"
        );
    }
}

/// 暂存超限的出口也用同一判定：明确不归本运行的历史兄弟输出再多，也不写缺口；归属未决的照常告警。
#[test]
fn tree_log_明确不归本运行的历史兄弟暂存超限不写缺口() {
    let mut m = setup();
    candidate(&mut m, "sibling", "root");
    candidate(&mut m, "unknown", "root");
    m.runs.index.child_history = serde_json::from_value(json!({
        "coverage":"verified","retainedAfter":0,
        "entries":{"sibling":{"run-old":{"rootThreadId":"root","observedAt":1}}}
    }))
    .unwrap();
    let mut a = vec![];
    for _ in 0..1000 {
        for thread in ["sibling", "unknown"] {
            m.buffer_child_event_log(thread, "item/completed", &item(thread, "t"), false, &mut a);
        }
    }
    let gaps: std::collections::BTreeSet<String> = logs(&a)
        .into_iter()
        .filter(|l| l.2 == "记录缺口")
        .map(|l| l.3["threadId"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(gaps, ["unknown".to_owned()].into());
}

/// 本运行的子代理某一轮只消费了协作消息（平级、跨分支、下级向上，如兄弟子代理之间互发）：到期正常丢弃，不写缺口。
/// 消费了直接上级的消息、上下混合、跨层下发、只有别的轮有协作记录、没有任何消费记录的，照常写缺口。
#[test]
fn tree_log_只被协作消息唤醒的轮次到期不写缺口_其余照常() {
    /// （用例名，消费记录（轮，发件人，收件人），是否写缺口）
    type Case<'a> = (&'a str, &'a [(&'a str, &'a str, &'a str)], bool);
    let cases: &[Case] = &[
        ("平级", &[("t", "/root/甲", "/root/乙")], false),
        ("下级向上", &[("t", "/root/乙/丙", "/root/乙")], false),
        (
            "两条平级",
            &[("t", "/root/甲", "/root/乙"), ("t", "/root/丁", "/root/乙")],
            false,
        ),
        ("直接上级", &[("t", "/root", "/root/乙")], true),
        (
            "上级加平级",
            &[("t", "/root/甲", "/root/乙"), ("t", "/root", "/root/乙")],
            true,
        ),
        ("跨分支", &[("t", "/root/甲/丙", "/root/乙")], false),
        ("跨层下发", &[("t", "/root", "/root/甲/乙")], true),
        (
            "协作记录在别的轮",
            &[("另一轮", "/root/甲", "/root/乙")],
            true,
        ),
        ("没有消费记录", &[], true),
    ];
    let id = |label: &str| turn_at(if label == "t" { 5000 } else { 5001 });
    for (name, notes, expect_gap) in cases {
        let mut m = setup();
        candidate(&mut m, "乙", "root");
        for (turn, author, recipient) in *notes {
            m.note_event_log_delivery("乙", &id(turn), author, recipient);
        }
        assert!(
            !m.historical_subagent_for_run("run-a", "乙"),
            "前置：不是历史子代理"
        );
        let mut a = vec![];
        m.buffer_child_event_log("乙", "item/completed", &item("乙", &id("t")), false, &mut a);
        assert_eq!(m.runs.event_log.pending.len(), 1, "前置：{name} 已暂存");
        m.coverage_complete("乙", &id("t"), &json!({"status":"completed"}), 3, true);
        let generation = m.runs.event_log.timer.unwrap();
        m.clock.mono_ms = PENDING_MS;
        a.clear();
        m.sweep_event_log(generation, &mut a);
        assert!(m.runs.event_log.pending.is_empty(), "{name}");
        let gap = logs(&a).iter().any(|l| l.2 == "记录缺口");
        assert_eq!(gap, *expect_gap, "{name}");
    }
}

/// 归属证据还在进行（跟进等配对）时，暂存到期续期、不写缺口；证据之后转成过期未证明，再到期才写缺口。
/// 续期不打乱顺序；协作消费记录也不能越过过期证据把它吞掉。
#[test]
fn tree_log_归属证据进行中到期续期_转过期后写缺口() {
    for collaboration in [false, true] {
        let mut m = setup();
        candidate(&mut m, "乙", "root");
        let t = turn_at(5000);
        if collaboration {
            m.note_event_log_delivery("乙", &t, "/root/甲", "/root/乙");
            assert!(m.turn_collaboration_only("乙", &t), "前置：协作记录已登记");
            m.coverage_complete("乙", &t, &json!({"status":"completed"}), 3, true);
        }
        evidence_to(
            &mut m,
            "乙",
            EvidenceKind::V2Interacted,
            EvidenceOutcome::Pending,
            false,
            false,
        );
        let mut a = vec![];
        let first = Some(
            json!({"threadId":"乙","turnId":&t,"item":{"id":"一","type":"agentMessage","text":"第一条"}}),
        );
        m.buffer_child_event_log("乙", "item/completed", &first, false, &mut a);
        m.clock.mono_ms = PENDING_MS / 2;
        let second = Some(
            json!({"threadId":"乙","turnId":&t,"item":{"id":"二","type":"agentMessage","text":"第二条"}}),
        );
        m.buffer_child_event_log("乙", "item/completed", &second, false, &mut a);
        let generation = m.runs.event_log.timer.unwrap();
        m.clock.mono_ms = PENDING_MS;
        a.clear();
        m.sweep_event_log(generation, &mut a);
        assert!(logs(&a).is_empty(), "证据进行中不写缺口");
        let kept: Vec<_> = m
            .runs
            .event_log
            .pending
            .iter()
            .map(|p| p.summary.item.clone())
            .collect();
        assert_eq!(
            kept,
            [Some("一".to_owned()), Some("二".to_owned())],
            "续期后仍按到达顺序"
        );
        let generation = m.runs.event_log.timer.expect("续期后重新计时");
        let op = m.relations.operations.values_mut().next().unwrap();
        op.outcome = EvidenceOutcome::Unproven;
        op.proof_expired = true;
        m.clock.mono_ms = PENDING_MS * 2;
        a.clear();
        m.sweep_event_log(generation, &mut a);
        assert!(m.runs.event_log.pending.is_empty());
        assert_eq!(m.runs.event_log.bytes, 0);
        let gaps = logs(&a).iter().filter(|l| l.2 == "记录缺口").count();
        assert_eq!(gaps, 2, "证据过期后写缺口（协作记录：{collaboration}）");
    }
}

/// 候选运行已经结束：即使证据仍标为进行中也不再续期，到期照常写缺口，不会无限续期。
#[test]
fn tree_log_候选运行结束后证据进行中也不续期() {
    let mut m = setup();
    candidate(&mut m, "乙", "root");
    evidence_to(
        &mut m,
        "乙",
        EvidenceKind::V2Interacted,
        EvidenceOutcome::Pending,
        false,
        false,
    );
    let mut a = vec![];
    m.buffer_child_event_log("乙", "item/completed", &item("乙", "t"), false, &mut a);
    assert_eq!(m.runs.event_log.pending.len(), 1, "前置：已暂存");
    m.runs.runs.get_mut("run-a").unwrap().state = RunState::Ended;
    let generation = m.runs.event_log.timer.unwrap();
    m.clock.mono_ms = PENDING_MS;
    a.clear();
    m.sweep_event_log(generation, &mut a);
    assert!(m.runs.event_log.pending.is_empty());
    assert!(logs(&a).iter().any(|l| l.2 == "记录缺口"));
}

/// 发件方向的判定与线程关系上的平级、向上拒绝同义：同一棵树里发件人不是收件人的上级就是协作。
#[test]
fn tree_log_协作消息方向判定() {
    for (author, recipient, expect, why) in [
        ("/root/甲", "/root/乙", true, "平级"),
        ("/root/乙/丙", "/root/乙", true, "下级向上"),
        ("/root/乙/丙/丁", "/root/乙", true, "隔层下级向上"),
        ("/root/a/x", "/root/a/y", true, "深层平级"),
        ("/root/甲/丙", "/root/乙", true, "跨分支"),
        ("/root/a/x", "/root/b/y", true, "跨分支到别的分支的下级"),
        ("/root/乙x", "/root/乙", true, "名字前缀相同的平级"),
        (
            "/root/乙x/丙",
            "/root/乙",
            true,
            "名字前缀相同的平级的下级也是跨分支",
        ),
        ("/root", "/root/乙", false, "直接上级"),
        ("/root/甲", "/root/甲/乙", false, "直接上级"),
        ("/root", "/root/甲/乙", false, "跨层的祖先"),
        ("/root/乙", "/root/乙", false, "自己"),
        ("/other/甲", "/root/乙", false, "不同的树"),
        ("", "/root/乙", false, "空路径"),
    ] {
        assert_eq!(
            collaboration_delivery(author, recipient),
            expect,
            "{why}：{author} → {recipient}"
        );
    }
}

/// 轮号里的开始时间只认上游的 UUIDv7；别的格式没有开始时间，这样的轮不登记消费方向，协作消息也不免告警。
#[test]
fn tree_log_轮号开始时间只认uuid7() {
    assert_eq!(
        turn_started_ms("01a1ba72-3987-76a0-a329-b91bc39b3682"),
        Some(0x01a1_ba72_3987)
    );
    assert_eq!(turn_started_ms(&turn_at(5000)), Some(5000));
    for (turn, why) in [
        ("01a1ba72-3987-46a0-a329-b91bc39b3682", "版本 4"),
        ("01a1ba72-3987-76a0-a329-b91bc39b368", "少一位"),
        ("01a1ba7203987-76a0-a329-b91bc39b3682", "分隔符位置不对"),
        ("+1a1ba72-3987-76a0-a329-b91bc39b3682", "带符号"),
        ("01a1ba72-39g7-76a0-a329-b91bc39b3682", "不是十六进制"),
        ("01a1ba72-3987-76a0-c329-b91bc39b3682", "变体不是 RFC 4122"),
        ("01a1ba72-3987-76a0-a329-b91bc39b36zz", "后半段不是十六进制"),
        ("01a1ba72-3987-76a0-a329+b91bc39b3682", "后半段分隔符不对"),
        ("01a1ba72-3987-76-0-a329-b91bc39b3682", "版本段里混进分隔符"),
        ("消息轮消息轮消息轮消息轮", "36 字节但不是编号"),
        ("消息轮", "不是编号"),
        ("", "空"),
    ] {
        assert_eq!(turn_started_ms(turn), None, "{why}");
    }
    let mut m = setup();
    candidate(&mut m, "乙", "root");
    m.note_event_log_delivery("乙", "消息轮", "/root/甲", "/root/乙");
    assert!(
        m.runs.event_log.coverage.is_empty(),
        "前置：没有开始时间的轮不登记"
    );
    assert!(expires_with_gap(&mut m, "乙", "消息轮"));
}

/// 同一轮先后消费了上级和平级的消息：只要出现过一次非协作消息就不算纯协作。
/// 之后更晚开始的轮把它挤出记录，它再收到平级消息也登记不回来，照常写缺口。
#[test]
fn tree_log_混合消费的轮被挤出记录后仍写缺口() {
    for (order, fill, filler_author) in [
        (["/root", "/root/甲"], 1023_u64, "/root/甲"),
        (["/root/甲", "/root"], 1023, "/root/甲"),
        (["/root", "/root/甲"], 1024, "/root/甲"),
        (["/root/甲", "/root"], 1024, "/root"),
        (["/root", "/root/甲"], 3000, "/root/甲"),
        (["/root", "/root/甲"], 3000, "/root"),
    ] {
        let mut m = setup();
        candidate(&mut m, "乙", "root");
        let t = turn_at(5000);
        for author in order {
            m.note_event_log_delivery("乙", &t, author, "/root/乙");
        }
        for n in 0..fill {
            m.note_event_log_delivery("别的线程", &turn_at(6000 + n), filler_author, "/root/丁");
        }
        let kept = m.runs.event_log.coverage.keys().any(|k| k.1 == "乙");
        assert_eq!(
            kept,
            fill < 1024,
            "前置：挤入 {fill} 轮后乙这一轮还在不在表里"
        );
        m.note_event_log_delivery("乙", &t, "/root/丙", "/root/乙");
        assert!(
            !m.turn_collaboration_only("乙", &t),
            "{order:?} 挤入 {fill} 轮（{filler_author}）再收平级消息后不是纯协作"
        );
        assert_eq!(
            m.runs.event_log.coverage.len() as u64,
            (fill + 1).min(1024),
            "迟到记录不占位、不挤别的轮"
        );
        assert!(
            expires_with_gap(&mut m, "乙", &t),
            "{order:?} 挤入 {fill} 轮（{filler_author}）"
        );
    }
}

/// 表满时只丢开始最早的一轮，留下的是开始最晚的那些：被丢的轮登记不回来，留下的轮结论不变。
/// 同一毫秒开始的两轮，一轮被丢、另一轮还在时也一样。
#[test]
fn tree_log_消费方向表只丢开始最早的轮() {
    let mut m = setup();
    candidate(&mut m, "子一", "root");
    candidate(&mut m, "子二", "root");
    // 两轮同一毫秒开始：子一消费过上级的消息，子二只消费过平级的。
    let t = turn_at(5000);
    m.note_event_log_delivery("子一", &t, "/root", "/root/子一");
    m.note_event_log_delivery("子二", &t, "/root/甲", "/root/子二");
    // 再登记 1023 个更晚开始、含上级消息的轮，表里多出一轮。
    for n in 0..1023 {
        m.note_event_log_delivery("别的线程", &turn_at(6000 + n), "/root", "/root/丁");
    }
    let has = |m: &Machine, thread: &str| m.runs.event_log.coverage.keys().any(|k| k.1 == thread);
    assert!(
        !has(&m, "子一") && has(&m, "子二"),
        "前置：同毫秒的两轮只挤掉了子一"
    );
    assert_eq!(m.runs.event_log.coverage.len(), 1024);
    // 被丢的子一再收到平级消息：和表里最早的一轮同时开始，登记不回来，也不挤别的轮。
    m.note_event_log_delivery("子一", &t, "/root/甲", "/root/子一");
    assert!(!m.turn_collaboration_only("子一", &t));
    assert!(has(&m, "子二"), "迟到记录不挤掉别的轮");
    // 开始最晚的一轮一直在表里：再收到平级消息仍是「消费过上级消息」，没有被当成新轮。
    let newest = turn_at(6000 + 1022);
    m.note_event_log_delivery("别的线程", &newest, "/root/甲", "/root/丁");
    assert!(
        !m.turn_collaboration_only("别的线程", &newest),
        "留下的是开始最晚的轮"
    );
    assert!(expires_with_gap(&mut m, "子一", &t));
    assert!(
        !expires_with_gap(&mut m, "子二", &t),
        "还在表里的纯协作轮不受影响"
    );
}

/// 长期运行攒下很多用不到的旧轮（都消费过上级消息）：从开始最早的丢起，之后新开始的轮照常登记，
/// 兄弟互发唤醒的轮仍不写缺口。
#[test]
fn tree_log_长期累计旧轮后新协作轮仍不写缺口() {
    let mut m = setup();
    candidate(&mut m, "乙", "root");
    for n in 0..5000 {
        m.note_event_log_delivery(
            &format!("旧线程{n}"),
            &turn_at(5000 + n),
            "/root",
            "/root/丁",
        );
    }
    assert_eq!(
        m.runs.event_log.coverage.len(),
        1024,
        "前置：表已写满并腾过位"
    );
    let t = turn_at(20_000);
    m.note_event_log_delivery("乙", &t, "/root/甲", "/root/乙");
    assert!(m.turn_collaboration_only("乙", &t));
    assert!(!expires_with_gap(&mut m, "乙", &t));
}

/// 表满之后才第一次见到的更早开始的轮：登记不进来，也不挤别的轮；它的记录不全，照常写缺口。
#[test]
fn tree_log_表满后更早开始的轮不登记() {
    let mut m = setup();
    candidate(&mut m, "乙", "root");
    for n in 0..1024 {
        m.note_event_log_delivery("别的线程", &turn_at(6000 + n), "/root/甲", "/root/丁");
    }
    let t = turn_at(5000);
    m.note_event_log_delivery("乙", &t, "/root", "/root/乙");
    m.note_event_log_delivery("乙", &t, "/root/甲", "/root/乙");
    let oldest = m.runs.event_log.coverage.keys().next().unwrap();
    assert_eq!(
        (oldest.0, oldest.1.as_str()),
        (6000, "别的线程"),
        "前置：乙这一轮没有登记，也没有挤掉别的轮"
    );
    assert_eq!(m.runs.event_log.coverage.len(), 1024);
    assert!(expires_with_gap(&mut m, "乙", &t));
}

/// 墙钟不能建立覆盖：没有真实开始时，无论轮号早于或晚于当前时间都保守拒绝。
#[test]
fn tree_log_墙钟前跳与回拨均不能替代真实开始() {
    for now in [1, 50_000, 150_000] {
        let mut m = setup();
        m.clock.now_ms = now;
        candidate(&mut m, "乙", "root");
        let turn = turn_at(49_000);
        m.coverage_consume("乙", &turn, "消息", &json!({"author":"/root/甲"}), true);
        assert!(matches!(
            m.runs.event_log.coverage.values().next(),
            Some(TurnCoverage::Veto { .. })
        ));
        assert!(expires_with_gap(&mut m, "乙", &turn));
    }
}
/// 上一运行留下的历史兄弟：本运行对它没有任何管理证据，它被唤醒的轮次摘要按血缘暂存后，
/// 到期或关闭清理时正常丢弃，不写「记录缺口」；同一时刻血缘不明的候选照常告警。
#[test]
fn tree_log_明确不归本运行的历史兄弟暂存到期与关闭清理都不写缺口() {
    for close in [false, true] {
        let mut m = setup();
        candidate(&mut m, "sibling", "root");
        candidate(&mut m, "unknown", "root");
        m.runs.index.child_history = serde_json::from_value(json!({
            "coverage":"verified","retainedAfter":0,
            "entries":{"sibling":{"run-old":{"rootThreadId":"root","observedAt":1}}}
        }))
        .unwrap();
        assert!(m.historical_subagent_for_run("run-a", "sibling"), "前置");
        assert!(!m.historical_subagent_for_run("run-a", "unknown"), "前置");
        let mut a = vec![];
        for thread in ["sibling", "unknown"] {
            m.buffer_child_event_log(thread, "item/completed", &item(thread, "t"), false, &mut a);
        }
        assert_eq!(m.runs.event_log.pending.len(), 2, "前置：两条都按血缘暂存");
        assert!(logs(&a).is_empty());
        a.clear();
        if close {
            m.shutdown_runs(&mut a);
        } else {
            let generation = m.runs.event_log.timer.unwrap();
            m.clock.mono_ms = PENDING_MS;
            m.sweep_event_log(generation, &mut a);
        }
        assert!(m.runs.event_log.pending.is_empty());
        assert_eq!(m.runs.event_log.bytes, 0);
        let gaps: Vec<_> = logs(&a)
            .into_iter()
            .filter(|l| l.2 == "记录缺口")
            .map(|l| l.3["threadId"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(gaps, ["unknown"], "关闭：{close}");
    }
}

// 方案 B 复现：通知、消费和计时器都经真实 claim::step 入口，不直接填方向表。
fn gap_b_apply(m: &mut Machine, event: crate::claim::Event) -> Vec<Action> {
    let (next, actions) = crate::claim::step(std::mem::take(m), event);
    *m = next;
    actions
}

fn gap_b_notify(m: &mut Machine, method: &str, params: Value) -> Vec<Action> {
    gap_b_apply(
        m,
        crate::claim::Event::Notification {
            method: method.into(),
            params: Some(params),
        },
    )
}

fn gap_b_started(m: &mut Machine, turn: &str) -> Vec<Action> {
    gap_b_notify(
        m,
        "turn/started",
        json!({"threadId":"乙", "turn":{"id":turn,"status":"inProgress"}}),
    )
}

fn gap_b_completed(m: &mut Machine, turn: &str) -> Vec<Action> {
    gap_b_notify(
        m,
        "turn/completed",
        json!({"threadId":"乙", "turn":{"id":turn,"status":"completed","error":null}}),
    )
}

fn gap_b_delivery(m: &mut Machine, turn: &str, id: &str, author: &str) -> Vec<Action> {
    let recipient = "/root/乙";
    let text = format!(
        "Message Type: MESSAGE\nTask name: {recipient}\nSender: {author}\nPayload:\n核对结果"
    );
    gap_b_notify(
        m,
        "rawResponseItem/completed",
        json!({
            "threadId":"乙","turnId":turn,"item":{
                "type":"agent_message","id":id,"author":author,"recipient":recipient,
                "content":[{"type":"input_text","text":text}]
            }
        }),
    )
}

/// 先验见证来自同线程前一真实 v7 轮：开始后收到合法 raw，结束后再登记候选。
/// 此时没有血缘候选，所以种子轮不会污染被测暂存；没有用内部表伪造 raw 证据。
fn gap_b_seeded() -> Machine {
    let mut m = setup();
    let seed = turn_at(4000);
    gap_b_started(&mut m, &seed);
    gap_b_delivery(&mut m, &seed, "种子消费", "/root/甲");
    assert_eq!(
        m.relations.aux.v2_raw_threads.get("乙"),
        Some(&m.relations.conn_epoch),
        "前置：种子真实 raw 已观察"
    );
    assert!(
        m.relations
            .aux
            .live_turn_starts
            .contains_key(&("乙".into(), seed.clone())),
        "前置：亲眼观察种子开始"
    );
    gap_b_completed(&mut m, &seed);
    assert!(
        m.runs.event_log.pending.is_empty(),
        "前置：种子不产生候选摘要"
    );
    assert!(
        m.relations.attachments.get("乙").is_some_and(|a| a.valid),
        "前置：种子结束没有退订"
    );
    candidate(&mut m, "乙", "root");
    m
}

fn gap_b_summary(m: &mut Machine, turn: &str) {
    gap_b_notify(m, "item/completed", item("乙", turn).unwrap());
    assert!(
        !m.historical_subagent_for_run("run-a", "乙"),
        "前置：不是其他运行历史成员"
    );
    assert!(m.binding("乙", turn).is_none(), "前置：目标轮尚未绑定");
    assert!(
        !m.event_log_evidence_in_flight("run-a", "乙"),
        "前置：没有归属证据遮挡到期判断"
    );
    assert!(
        m.relations.operations.values().all(|o| !o.proof_expired),
        "前置：没有过期证据掩盖免报分支"
    );
    assert!(
        m.runs
            .event_log
            .pending
            .iter()
            .any(|p| p.turn == turn && p.summary.item.as_deref() == Some("same-item")),
        "前置：目标摘要已入队"
    );
    assert!(m.runs.event_log.bytes > 0, "前置：摘要实际占用字节");
}

fn gap_b_sweep(m: &mut Machine) -> Vec<Action> {
    let generation = m.runs.event_log.timer.expect("前置：有实际暂存计时器");
    m.clock.mono_ms += PENDING_MS + 1;
    m.clock.now_ms += PENDING_MS + 1;
    gap_b_apply(
        m,
        crate::claim::Event::Timer {
            token: crate::claim::ClaimToken {
                thread_id: String::new(),
                claim_id: 0,
            },
            kind: crate::claim::TimerKind::Run(crate::runs::RunTimer::EventLogSweep { generation }),
        },
    )
}

fn gap_b_has_gap(actions: &[Action], turn: &str) -> bool {
    logs(actions)
        .iter()
        .any(|l| l.2 == "记录缺口" && l.3["turnId"] == turn)
}

#[test]
fn gap_b_red_有先验raw的未结束协作轮到期仍保留摘要() {
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "兄弟消费", "/root/甲");
    gap_b_summary(&mut m, &turn);
    let bytes = m.runs.event_log.bytes;
    let actions = gap_b_sweep(&mut m);
    assert!(!gap_b_has_gap(&actions, &turn), "完整开放轮暂不报缺口");
    assert!(
        m.runs.event_log.pending.iter().any(|p| p.turn == turn),
        "未结束轮必须继续暂存，不能提前丢摘要"
    );
    assert_eq!(m.runs.event_log.bytes, bytes, "续期不能扣掉暂存字节");
}

#[test]
fn gap_b_red_到期之后同轮上级消息仍可否决全部摘要() {
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "兄弟消费", "/root/甲");
    gap_b_summary(&mut m, &turn);
    let original_count = m
        .runs
        .event_log
        .pending
        .iter()
        .filter(|p| p.turn == turn)
        .count();
    assert!(original_count >= 2, "前置：开始与条目都已暂存");
    let mut all = gap_b_sweep(&mut m);
    all.extend(gap_b_delivery(&mut m, &turn, "迟到上级", "/root"));
    all.extend(gap_b_completed(&mut m, &turn));
    if m.runs.event_log.timer.is_some() {
        all.extend(gap_b_sweep(&mut m));
    }
    let gaps = logs(&all)
        .iter()
        .filter(|l| l.2 == "记录缺口" && l.3["turnId"] == turn)
        .count();
    assert!(
        gaps >= original_count,
        "上级消息不能只保住后来的完成摘要，原先 {original_count} 条摘要必须写缺口，实际 {gaps}"
    );
}

#[test]
fn gap_b_red_晚订阅缺真实started的协作轮仍写缺口() {
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_delivery(&mut m, &turn, "晚订阅后首条", "/root/甲");
    assert!(
        !m.relations
            .aux
            .live_turn_starts
            .contains_key(&("乙".into(), turn.clone())),
        "前置：目标轮真实开始未观察"
    );
    gap_b_summary(&mut m, &turn);
    gap_b_completed(&mut m, &turn);
    assert!(
        gap_b_has_gap(&gap_b_sweep(&mut m), &turn),
        "既往 raw 能力不能替代本轮完整开始"
    );
}

#[test]
fn gap_b_red_本轮首条raw不能追认开始时的资格() {
    let mut m = setup();
    candidate(&mut m, "乙", "root");
    let turn = turn_at(5000);
    assert!(!m.relations.aux.v2_raw_threads.contains_key("乙"));
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "首次raw", "/root/甲");
    gap_b_summary(&mut m, &turn);
    gap_b_completed(&mut m, &turn);
    assert!(
        gap_b_has_gap(&gap_b_sweep(&mut m), &turn),
        "开始以后见到 raw 不能追认先验凭据"
    );
}

#[test]
fn gap_b_red_未结束协作轮在关闭时必须报缺口() {
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "兄弟消费", "/root/甲");
    gap_b_summary(&mut m, &turn);
    let actions = gap_b_apply(&mut m, crate::claim::Event::Shutdown);
    assert!(
        gap_b_has_gap(&actions, &turn),
        "关闭时尚未完成的纯协作外观不能免报"
    );
    assert!(m.runs.event_log.pending.is_empty());
    assert_eq!(m.runs.event_log.bytes, 0);
}

#[test]
fn gap_b_red_消费去重不能跳过相同id的非协作反证() {
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "重复id", "/root/甲");
    gap_b_delivery(&mut m, &turn, "重复id", "/root");
    gap_b_delivery(&mut m, &turn, "后续兄弟", "/root/丙");
    gap_b_summary(&mut m, &turn);
    gap_b_completed(&mut m, &turn);
    assert!(
        gap_b_has_gap(&gap_b_sweep(&mut m), &turn),
        "去重之前必须保留冲突消费，之后的协作不能覆盖它"
    );
}

#[test]
fn gap_b_red_跨轮复用id不能跳过本轮上级消费() {
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "种子消费", "/root");
    gap_b_delivery(&mut m, &turn, "本轮兄弟", "/root/甲");
    gap_b_summary(&mut m, &turn);
    gap_b_completed(&mut m, &turn);
    assert!(
        gap_b_has_gap(&gap_b_sweep(&mut m), &turn),
        "跨轮复用 id 不能令上级先于兄弟的轮变成全协作"
    );
}

#[test]
fn gap_b_red_上级后兄弟完成仍为混合消费() {
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "上级", "/root");
    gap_b_delivery(&mut m, &turn, "兄弟", "/root/甲");
    gap_b_summary(&mut m, &turn);
    gap_b_completed(&mut m, &turn);
    assert!(
        gap_b_has_gap(&gap_b_sweep(&mut m), &turn),
        "含其他是吸收态，不能用最后一条覆盖"
    );
}

#[test]
fn gap_b_red_重复completed不破坏完整协作轮() {
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "兄弟消费", "/root/甲");
    gap_b_summary(&mut m, &turn);
    let mut all = gap_b_completed(&mut m, &turn);
    all.extend(gap_b_completed(&mut m, &turn));
    all.extend(gap_b_sweep(&mut m));
    assert!(!gap_b_has_gap(&all, &turn), "一致重复完成须幂等");
    assert!(
        m.runs.event_log.pending.is_empty(),
        "完整结束后不应永久暂存"
    );
}

#[test]
fn gap_b_red_未知生命周期通知撤销协作免报() {
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "兄弟消费", "/root/甲");
    gap_b_notify(&mut m, "thread/runtime/replaced", json!({"threadId":"乙"}));
    gap_b_summary(&mut m, &turn);
    gap_b_completed(&mut m, &turn);
    assert!(
        gap_b_has_gap(&gap_b_sweep(&mut m), &turn),
        "未知生命周期不能被当作无关通知吞掉"
    );
}

#[test]
fn gap_b_red_无法解释的raw消费必须撤销资格() {
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "兄弟消费", "/root/甲");
    gap_b_notify(
        &mut m,
        "rawResponseItem/completed",
        json!({"threadId":"乙","turnId":turn,"item":{
            "type":"agent_message","id":"坏记录","author":"/root","recipient":"/root/乙","content":false
        }}),
    );
    gap_b_summary(&mut m, &turn);
    gap_b_completed(&mut m, &turn);
    assert!(
        gap_b_has_gap(&gap_b_sweep(&mut m), &turn),
        "合法 JSON 但消费结构无法解释时必须保守报缺口"
    );
}

#[test]
fn gap_b_red_冷恢复无raw的轮保持保守缺口() {
    let mut m = setup();
    candidate(&mut m, "乙", "root");
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_summary(&mut m, &turn);
    assert!(
        !m.relations.aux.v2_raw_threads.contains_key("乙"),
        "前置：无 raw 见证"
    );
    gap_b_completed(&mut m, &turn);
    assert!(gap_b_has_gap(&gap_b_sweep(&mut m), &turn));
}

/// 仅用于覆盖表的容量和方向单元测试；完整入口授信另由 gap_b_* 测试验证。
impl Machine {
    fn note_event_log_delivery(&mut self, thread: &str, turn: &str, author: &str, recipient: &str) {
        self.coverage_start(
            thread,
            turn,
            Some(StartProof {
                conn_epoch: self.relations.conn_epoch,
                observation_gen: 1,
                start_seq: 2,
                raw_seq_at_start: 1,
            }),
        );
        let raw = json!({"author":author,"recipient":recipient});
        let item = format!("{author}:{recipient}");
        self.coverage_consume(
            thread,
            turn,
            &item,
            &raw,
            collaboration_delivery(author, recipient),
        );
    }

    fn turn_collaboration_only(&self, thread: &str, turn: &str) -> bool {
        let Some(ms) = turn_started_ms(turn) else {
            return false;
        };
        matches!(
            self.runs
                .event_log
                .coverage
                .get(&(ms, thread.into(), turn.into())),
            Some(TurnCoverage::Open {
                consumption: coverage::Consumption::CollaborationOnly,
                ..
            }) | Some(TurnCoverage::Complete {
                consumption: coverage::Consumption::CollaborationOnly,
                ..
            })
        )
    }
} // 此组入口复现只依赖旧有测试助手，可移植到修改前快照运行。
#[test]
fn gap_b_red_v4_先完成后开始不能制造覆盖() {
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_completed(&mut m, &turn);
    assert!(
        matches!(gap_b_coverage(&m, &turn), TurnCoverage::Veto { .. }),
        "没有真实开始的完成必须立即否决，不能暂时封存"
    );
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "兄弟", "/root/甲");
    gap_b_summary(&mut m, &turn);
    assert!(
        gap_b_has_gap(&gap_b_sweep(&mut m), &turn),
        "结束先到不能被迟来的开始复活"
    );
}

#[test]
fn gap_b_red_v4_完成后矛盾终态不能免报() {
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "兄弟", "/root/甲");
    gap_b_summary(&mut m, &turn);
    gap_b_completed(&mut m, &turn);
    gap_b_notify(
        &mut m,
        "turn/completed",
        json!({"threadId":"乙","turn":{"id":turn,"status":"failed","error":{"message":"矛盾"}}}),
    );
    assert!(
        gap_b_has_gap(&gap_b_sweep(&mut m), &turn),
        "已封存终态不可被新终态覆盖"
    );
}

#[test]
fn gap_b_red_v4_完成后新增同轮消费不能免报() {
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "兄弟", "/root/甲");
    gap_b_summary(&mut m, &turn);
    gap_b_completed(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "迟到兄弟", "/root/丙");
    assert!(
        gap_b_has_gap(&gap_b_sweep(&mut m), &turn),
        "完成后新增消费意味着结束证明不完整"
    );
}

#[test]
fn gap_b_red_v4_未完成纯协作轮容量超限仍报告() {
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "兄弟", "/root/甲");
    gap_b_summary(&mut m, &turn);
    let mut actions = Vec::new();
    for n in 0..250 {
        actions.extend(gap_b_notify(&mut m, "item/completed", json!({"threadId":"乙","turnId":turn,"item":{"id":format!("容量-{n}"),"type":"agentMessage","text":"完整摘要".repeat(100)}})));
    }
    assert!(
        m.runs.event_log.bytes <= PER_THREAD_BYTES,
        "前置：容量上限确实生效"
    );
    assert!(
        gap_b_has_gap(&actions, &turn),
        "强制容量处置不能启用协作排除"
    );
}

fn gap_b_coverage(m: &Machine, turn: &str) -> TurnCoverage {
    m.runs.event_log.coverage[&(turn_started_ms(turn).unwrap(), "乙".into(), turn.into())].clone()
}

#[test]
fn gap_b_完成退订与重复边界保持封存序号和消费指纹() {
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "兄弟", "/root/甲");
    gap_b_summary(&mut m, &turn);
    gap_b_completed(&mut m, &turn);
    let sealed = gap_b_coverage(&m, &turn);
    assert!(
        matches!(sealed, TurnCoverage::Complete { .. }),
        "前置：真实通知已封存"
    );
    m.detach_evidence("乙");
    assert!(!m.relations.attachments["乙"].valid, "前置：当前附着已失效");
    assert!(
        m.coverage_observation_gen("乙").is_none(),
        "前置：raw 未来授信已封锁"
    );
    assert_eq!(
        m.coverage_disposition("乙", &turn),
        CoverageDisposition::AllowCollaborationExclusion
    );
    gap_b_completed(&mut m, &turn);
    assert_eq!(
        gap_b_coverage(&m, &turn),
        sealed,
        "一致完成保留原 end_seq 与消费汇总"
    );
    gap_b_started(&mut m, &turn);
    assert_eq!(
        gap_b_coverage(&m, &turn),
        sealed,
        "重复开始不能重开或重拍快照"
    );
    gap_b_delivery(&mut m, &turn, "兄弟", "/root/甲");
    assert_eq!(
        gap_b_coverage(&m, &turn),
        sealed,
        "精确重复消费不改变封存证明"
    );
    assert!(!gap_b_has_gap(&gap_b_sweep(&mut m), &turn));
    assert!(m.runs.event_log.pending.is_empty());
    assert_eq!(m.runs.event_log.bytes, 0);
}

#[test]
fn gap_b_开放轮退订立即否决而完成不需要当前附着() {
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "兄弟", "/root/甲");
    gap_b_summary(&mut m, &turn);
    assert_eq!(
        m.coverage_disposition("乙", &turn),
        CoverageDisposition::Wait
    );
    m.detach_evidence("乙");
    assert!(matches!(
        gap_b_coverage(&m, &turn),
        TurnCoverage::Veto { .. }
    ));
    gap_b_completed(&mut m, &turn);
    assert!(gap_b_has_gap(&gap_b_sweep(&mut m), &turn));
}

#[test]
fn gap_b_嵌套扫描不能在事件处理结束之前协作丢弃() {
    for counterevidence in [false, true] {
        let mut m = gap_b_seeded();
        let turn = turn_at(5000);
        gap_b_started(&mut m, &turn);
        gap_b_delivery(&mut m, &turn, "兄弟", "/root/甲");
        gap_b_summary(&mut m, &turn);
        gap_b_completed(&mut m, &turn);
        let count = m.runs.event_log.pending.len();
        let bytes = m.runs.event_log.bytes;
        m.clock.mono_ms += PENDING_MS + 1;
        m.begin_event_log_step();
        let mut actions = Vec::new();
        m.expire_event_log(&mut actions);
        assert_eq!(
            m.runs.event_log.pending.len(),
            count,
            "处理中嵌套扫描不能删除摘要"
        );
        assert_eq!(m.runs.event_log.bytes, bytes, "处理中不可扣除字节");
        assert!(logs(&actions).is_empty());
        if counterevidence {
            m.coverage_veto(Some("乙"), Some(&turn), "同一步文件反证", true);
        }
        m.settle_event_log_step(&mut actions);
        assert!(m.runs.event_log.pending.is_empty());
        assert_eq!(m.runs.event_log.bytes, 0);
        assert_eq!(
            gap_b_has_gap(&actions, &turn),
            counterevidence,
            "尾部使用本步最终事实"
        );
    }
}

#[test]
fn gap_b_开放续期后结束在尾部立即释放原摘要() {
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "兄弟", "/root/甲");
    gap_b_summary(&mut m, &turn);
    let count = m.runs.event_log.pending.len();
    let bytes = m.runs.event_log.bytes;
    let mut actions = gap_b_sweep(&mut m);
    assert_eq!(m.runs.event_log.pending.len(), count);
    assert_eq!(m.runs.event_log.bytes, bytes);
    actions.extend(gap_b_completed(&mut m, &turn));
    assert!(
        !m.runs.event_log.pending.iter().any(|p| p.awaiting_coverage),
        "已经到期暂留的摘要不再等额外用户事件"
    );
    if m.runs.event_log.timer.is_some() {
        actions.extend(gap_b_sweep(&mut m));
    }
    assert!(!gap_b_has_gap(&actions, &turn));
    assert!(m.runs.event_log.pending.is_empty());
    assert_eq!(m.runs.event_log.bytes, 0);
}

#[test]
fn gap_b_零消费完成不能协作免报() {
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_summary(&mut m, &turn);
    gap_b_completed(&mut m, &turn);
    assert!(matches!(
        gap_b_coverage(&m, &turn),
        TurnCoverage::Complete {
            consumption: coverage::Consumption::Unseen,
            ..
        }
    ));
    assert!(gap_b_has_gap(&gap_b_sweep(&mut m), &turn));
}

#[test]
fn gap_b_配对ttl淘汰不清除上级消费汇总() {
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "上级", "/root");
    assert!(m.relations.aux.v2_deliveries.iter().any(|d| d.id == "上级"));
    m.clock.mono_ms += crate::relations::EVIDENCE_TTL_MS + 1;
    m.facts_expire(&mut Vec::new());
    assert!(
        !m.relations.aux.v2_deliveries.iter().any(|d| d.id == "上级"),
        "前置：配对证据已实际淘汰"
    );
    assert!(matches!(
        gap_b_coverage(&m, &turn),
        TurnCoverage::Open {
            consumption: coverage::Consumption::ContainsOther,
            ..
        }
    ));
    gap_b_delivery(&mut m, &turn, "兄弟", "/root/甲");
    gap_b_summary(&mut m, &turn);
    gap_b_completed(&mut m, &turn);
    assert!(matches!(
        gap_b_coverage(&m, &turn),
        TurnCoverage::Complete {
            consumption: coverage::Consumption::ContainsOther,
            ..
        }
    ));
    assert!(gap_b_has_gap(&gap_b_sweep(&mut m), &turn));
}

#[test]
fn gap_b_完成轮等待真实结果回包后尾部解除票据屏障() {
    use crate::relations::rollout_pairing::{ReadTicket, ReadTicketState};
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "兄弟", "/root/甲");
    gap_b_summary(&mut m, &turn);
    gap_b_completed(&mut m, &turn);
    m.clock.mono_ms = 15_000;
    let ticket = ReadTicket {
        id: 777,
        thread: "乙".into(),
        conn_epoch: m.relations.conn_epoch,
        observation_gen: m.coverage_read_observation_gen("乙"),
        issued_seq: m.coverage_next_seq().unwrap(),
        deadline: 45_000,
        state: ReadTicketState::Active,
    };
    m.relations
        .aux
        .rollout
        .coverage_reads
        .insert(ticket.id, ticket.clone());
    assert_eq!(m.coverage_read_deadline("乙"), Some(45_000));
    assert_eq!(
        m.coverage_disposition("乙", &turn),
        CoverageDisposition::Wait
    );
    let bytes = m.runs.event_log.bytes;
    let count = m.runs.event_log.pending.len();
    m.clock.mono_ms = 30_001;
    let generation = m.runs.event_log.timer.unwrap();
    let mut actions = gap_b_apply(
        &mut m,
        crate::claim::Event::Timer {
            token: crate::claim::ClaimToken {
                thread_id: String::new(),
                claim_id: 0,
            },
            kind: crate::claim::TimerKind::Run(crate::runs::RunTimer::EventLogSweep { generation }),
        },
    );
    assert_eq!(
        m.runs.event_log.pending.len(),
        count,
        "读取未决时已完成轮仍保留"
    );
    assert_eq!(m.runs.event_log.bytes, bytes);
    assert_eq!(m.runs.event_log.pending.front().unwrap().expires, 45_000);
    actions.extend(gap_b_apply(
        &mut m,
        crate::claim::Event::RolloutRead {
            id: ticket.id,
            thread: ticket.thread,
            conn_epoch: ticket.conn_epoch,
            observation_gen: ticket.observation_gen,
            issued_seq: ticket.issued_seq,
            result: Ok(crate::rollout::RolloutChunk::default()),
        },
    ));
    assert!(
        m.relations.aux.rollout.coverage_reads.is_empty(),
        "实际回包释放物理票据"
    );
    assert!(
        m.runs.event_log.pending.is_empty(),
        "空结果也在同一步尾部重评暂留摘要"
    );
    assert_eq!(m.runs.event_log.bytes, 0);
    assert!(!gap_b_has_gap(&actions, &turn));
}

#[test]
fn gap_b_水位单调且同毫秒否决轮淘汰不能复活() {
    let mut m = setup();
    let oldest = turn_at(5000);
    m.coverage_veto(Some("乙"), Some(&oldest), "原始反证", true);
    for n in 0..COVERAGE_TURNS as u64 {
        m.coverage_start("填充", &turn_at(6000 + n), None);
    }
    assert_eq!(m.runs.event_log.coverage.len(), COVERAGE_TURNS);
    assert_eq!(m.runs.event_log.evicted_through_ms, Some(5000));
    assert!(!m
        .runs
        .event_log
        .coverage
        .contains_key(&(5000, "乙".into(), oldest.clone())));
    m.coverage_start(
        "乙",
        &oldest,
        Some(StartProof {
            conn_epoch: 0,
            observation_gen: 0,
            start_seq: 2,
            raw_seq_at_start: 1,
        }),
    );
    assert!(m.coverage_turn_seen("乙", &oldest), "即便缺失也记得已淘汰");
    assert!(!m
        .runs
        .event_log
        .coverage
        .contains_key(&(5000, "乙".into(), oldest.clone())));
    m.coverage_start("回拨", &turn_at(4000), None);
    assert_eq!(
        m.runs.event_log.evicted_through_ms,
        Some(5000),
        "迟到更早轮不能降低水位"
    );
    let same_ms = "00000000-1388-7000-8000-000000000001";
    m.coverage_start(
        "同毫秒",
        same_ms,
        Some(StartProof {
            conn_epoch: 0,
            observation_gen: 0,
            start_seq: 2,
            raw_seq_at_start: 1,
        }),
    );
    assert!(!m.runs.event_log.coverage.keys().any(|k| k.1 == "同毫秒"));
}

#[test]
fn gap_b_完成指纹忽略展示字段但规范化错误对象键顺序() {
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "兄弟", "/root/甲");
    gap_b_summary(&mut m, &turn);
    gap_b_notify(
        &mut m,
        "turn/completed",
        json!({"threadId":"乙","turn":{"id":turn,"status":"failed","error":{"a":1,"b":{"c":2,"d":3}},"usage":10}}),
    );
    let sealed = gap_b_coverage(&m, &turn);
    assert!(matches!(sealed, TurnCoverage::Complete { .. }));
    m.detach_evidence("乙");
    let duplicate: Value = serde_json::from_str(&format!(r#"{{"threadId":"乙","turn":{{"id":"{turn}","status":"failed","error":{{"b":{{"d":3,"c":2}},"a":1}},"usage":99}}}}"#)).unwrap();
    gap_b_notify(&mut m, "turn/completed", duplicate);
    assert_eq!(gap_b_coverage(&m, &turn), sealed);
    assert!(!gap_b_has_gap(&gap_b_sweep(&mut m), &turn));
}

#[test]
fn gap_b_全局容量强制处置不能使用完成协作豁免() {
    let mut m = setup();
    let mut actions = Vec::new();
    for n in 0..80 {
        let thread = format!("容量线程-{n}");
        let turn = turn_at(5000 + n);
        candidate(&mut m, &thread, "root");
        m.note_event_log_delivery(&thread, &turn, "/root/甲", "/root/乙");
        m.coverage_complete(&thread, &turn, &json!({"status":"completed"}), 3, true);
        assert_eq!(
            m.coverage_disposition(&thread, &turn),
            CoverageDisposition::AllowCollaborationExclusion
        );
        for item in 0..60 {
            m.buffer_child_event_log(&thread, "item/completed", &Some(json!({
                "threadId":thread,"turnId":turn,"item":{"id":format!("摘要-{item}"),"type":"agentMessage","text":"x".repeat(240)}
            })), false, &mut actions);
        }
        let per_thread: usize = m
            .runs
            .event_log
            .pending
            .iter()
            .filter(|p| p.thread == thread)
            .map(|p| p.bytes)
            .sum();
        assert!(
            per_thread < PER_THREAD_BYTES,
            "前置：每线程尚未达到本地上限"
        );
    }
    assert!(m.runs.event_log.bytes <= GLOBAL_BYTES);
    assert!(
        m.runs.event_log.bytes > GLOBAL_BYTES - PER_THREAD_BYTES,
        "前置：全局容量已真实吃满"
    );
    assert!(logs(&actions)
        .iter()
        .any(|l| l.2 == "记录缺口" && l.3["summary"] == "归属确认前摘要暂存超限"));
    actions.clear();
    m.clear_pending_event_log(&mut actions);
    assert_eq!(m.runs.event_log.bytes, 0);
    assert!(m.runs.event_log.pending.is_empty());
    assert!(
        logs(&actions).iter().any(|l| l.2 == "记录缺口"),
        "关闭也不使用已完成协作豁免"
    );
}

#[test]
fn gap_b_配对容量淘汰不能抹去上级消费否决() {
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "上级", "/root");
    assert!(m
        .relations
        .aux
        .v2_deliveries
        .iter()
        .any(|d| d.thread == "乙" && d.id == "上级"));
    for n in 0..crate::relations::MAX_OPERATIONS {
        gap_b_notify(
            &mut m,
            "rawResponseItem/completed",
            json!({
                "threadId":"填充","turnId":"只用于配对容量的非规范轮","item":{
                    "type":"agent_message","id":format!("填充-{n}"),"author":"/root/甲","recipient":"/root/填充",
                    "content":[{"type":"input_text","text":"Message Type: MESSAGE\nTask name: /root/填充\nSender: /root/甲\nPayload:\n核对结果"}]
                }
            }),
        );
    }
    assert_eq!(
        m.relations.aux.v2_deliveries.len(),
        crate::relations::MAX_OPERATIONS
    );
    assert!(
        !m.relations
            .aux
            .v2_deliveries
            .iter()
            .any(|d| d.thread == "乙" && d.id == "上级"),
        "前置：上级消费已被配对容量实际淘汰"
    );
    assert!(
        matches!(
            gap_b_coverage(&m, &turn),
            TurnCoverage::Open {
                consumption: coverage::Consumption::ContainsOther,
                ..
            }
        ),
        "覆盖事实仍独立保留"
    );
    gap_b_delivery(&mut m, &turn, "兄弟", "/root/甲");
    gap_b_summary(&mut m, &turn);
    gap_b_completed(&mut m, &turn);
    assert!(matches!(
        gap_b_coverage(&m, &turn),
        TurnCoverage::Complete {
            consumption: coverage::Consumption::ContainsOther,
            ..
        }
    ));
    assert!(gap_b_has_gap(&gap_b_sweep(&mut m), &turn));
}

#[test]
fn gap_b_续期后的迟到绑定按原顺序补写所有摘要() {
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "兄弟", "/root/甲");
    gap_b_summary(&mut m, &turn);
    let count = m.runs.event_log.pending.len();
    gap_b_sweep(&mut m);
    assert_eq!(
        m.runs.event_log.pending.len(),
        count,
        "前置：开放轮到期摘要全部保留"
    );
    bind(&mut m, "run-a", "乙", &turn, "root");
    let mut actions = Vec::new();
    m.flush_bound_event_log("run-a", "乙", &turn, "root", &mut actions);
    let rows = logs(&actions);
    assert_eq!(rows.len(), count);
    assert_eq!(rows.first().unwrap().2, "轮次开始");
    assert!(rows.iter().all(|r| r.3["late"] == true));
    assert!(!gap_b_has_gap(&actions, &turn));
    assert!(m.runs.event_log.pending.is_empty());
    assert_eq!(m.runs.event_log.bytes, 0);
    actions.clear();
    m.flush_bound_event_log("run-a", "乙", &turn, "root", &mut actions);
    assert!(logs(&actions).is_empty(), "绑定补写恰好一次");
}

#[test]
fn gap_b_完成轮读取定时到期在同一步重评全部摘要() {
    use crate::relations::rollout_pairing::{ReadTicket, ReadTicketState};
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "兄弟", "/root/甲");
    gap_b_summary(&mut m, &turn);
    gap_b_completed(&mut m, &turn);
    m.clock.mono_ms = 15_000;
    let ticket = ReadTicket {
        id: 777,
        thread: "乙".into(),
        conn_epoch: m.relations.conn_epoch,
        observation_gen: m.coverage_read_observation_gen("乙"),
        issued_seq: m.coverage_next_seq().unwrap(),
        deadline: 45_000,
        state: ReadTicketState::Active,
    };
    m.relations
        .aux
        .rollout
        .coverage_reads
        .insert(ticket.id, ticket.clone());
    assert_eq!(m.coverage_read_deadline("乙"), Some(45_000));
    assert_eq!(
        m.coverage_disposition("乙", &turn),
        CoverageDisposition::Wait
    );
    let bytes = m.runs.event_log.bytes;
    let count = m.runs.event_log.pending.len();
    m.clock.mono_ms = 30_001;
    let generation = m.runs.event_log.timer.unwrap();
    let mut actions = gap_b_apply(
        &mut m,
        crate::claim::Event::Timer {
            token: crate::claim::ClaimToken {
                thread_id: String::new(),
                claim_id: 0,
            },
            kind: crate::claim::TimerKind::Run(crate::runs::RunTimer::EventLogSweep { generation }),
        },
    );
    assert_eq!(
        m.runs.event_log.pending.len(),
        count,
        "读取未决时已完成轮仍保留"
    );
    assert_eq!(m.runs.event_log.bytes, bytes);
    assert_eq!(m.runs.event_log.pending.front().unwrap().expires, 45_000);
    let (deadline, generation) = m
        .relations
        .aux
        .rollout
        .timer
        .expect("Active 票据拥有独立定时器");
    assert_eq!(deadline, ticket.deadline);
    m.clock.mono_ms = deadline;
    actions.extend(gap_b_apply(
        &mut m,
        crate::claim::Event::Timer {
            token: crate::claim::ClaimToken {
                thread_id: String::new(),
                claim_id: 0,
            },
            kind: crate::claim::TimerKind::RolloutPoll { generation },
        },
    ));
    assert_eq!(
        m.relations.aux.rollout.coverage_reads[&ticket.id].state,
        ReadTicketState::Retired
    );
    assert!(
        m.relations.aux.rollout.timer.is_none(),
        "退休占位不反复调度"
    );
    assert!(matches!(
        gap_b_coverage(&m, &turn),
        TurnCoverage::Veto { .. }
    ));
    assert!(m.coverage_observation_gen("乙").is_none());
    assert!(
        m.runs.event_log.pending.is_empty(),
        "到期不依赖新的用户事件便重评摘要"
    );
    assert_eq!(m.runs.event_log.bytes, 0);
    assert_eq!(
        logs(&actions)
            .iter()
            .filter(|l| l.2 == "记录缺口" && l.3["turnId"] == turn)
            .count(),
        count
    );
}

// 附加到 runs/event_log/tests.rs：直接验证原有摘要及字节未被提前丢失。
#[test]
fn gap_b_review1_raw系统和开发者输入令完成前摘要最终写缺口() {
    for role in ["system", "developer", "assistant"] {
        let mut m = gap_b_seeded();
        let turn = turn_at(5000);
        gap_b_started(&mut m, &turn);
        gap_b_delivery(&mut m, &turn, "兄弟消费", "/root/甲");
        gap_b_summary(&mut m, &turn);
        assert!(m.coverage_open_proof("乙", &turn).is_some());
        let bytes = m.runs.event_log.bytes;
        let count = m.runs.event_log.pending.len();
        assert!(bytes > 0 && count >= 2, "前置：原始摘要及开始记录实际暂存");
        let mut all = gap_b_notify(
            &mut m,
            "rawResponseItem/completed",
            json!({"threadId":"乙","turnId":turn,
            "item":{"type":"message","role":role,"content":[{"type":if role=="assistant" {"output_text"} else {"input_text"},"text":"新内容"}]}}),
        );
        assert_eq!(
            m.runs.event_log.bytes, bytes,
            "raw不进入摘要，也不得删除既有摘要"
        );
        assert_eq!(m.runs.event_log.pending.len(), count);
        if role == "assistant" {
            assert!(m.coverage_open_proof("乙", &turn).is_some());
        } else {
            assert!(
                m.coverage_open_proof("乙", &turn).is_none(),
                "{role}输入必须否决"
            );
        }
        all.extend(gap_b_completed(&mut m, &turn));
        all.extend(gap_b_sweep(&mut m));
        if role == "assistant" {
            assert!(!gap_b_has_gap(&all, &turn), "合法输出对照最终免报");
        } else {
            let gaps = logs(&all)
                .iter()
                .filter(|line| line.2 == "记录缺口" && line.3["turnId"] == turn)
                .count();
            assert!(
                gaps >= count,
                "{role}输入必须保住全部{count}条原始摘要，实际缺口{gaps}"
            );
        }
        assert_eq!(m.runs.event_log.bytes, 0);
        assert!(m.runs.event_log.pending.is_empty());
    }
}

impl Machine {
    /// 其他入口测试只读检查指定轮暂存与全局字节，不暴露或修改内部覆盖状态。
    pub(crate) fn event_log_pending_stats(
        &self,
        thread: &str,
        turn: &str,
    ) -> (usize, usize, usize) {
        let selected: Vec<_> = self
            .runs
            .event_log
            .pending
            .iter()
            .filter(|p| p.thread == thread && p.turn == turn)
            .collect();
        (
            selected.len(),
            selected.iter().map(|p| p.bytes).sum(),
            self.runs.event_log.bytes,
        )
    }
}

#[test]
fn gap_b_review1_新增合法展示与工具输出完整日志零缺口() {
    let mut cases: Vec<(&str, Value)> = Vec::new();
    for tool in [
        "sendMessage",
        "followupTask",
        "interruptAgent",
        "listAgents",
    ] {
        for status in ["completed", "interrupted"] {
            cases.push(("item/completed", json!({"type":"collabAgentToolCall","id":format!("合法{tool}-{status}"),
                "tool":tool,"status":status,"senderThreadId":"乙","receiverThreadIds":[],"agentsStates":{},
                "prompt":null,"model":null,"reasoningEffort":null})));
        }
    }
    cases.push((
        "item/completed",
        json!({"type":"webSearch","id":"搜索","query":"问题","action":null,"results":null}),
    ));
    cases.push((
        "item/completed",
        json!({"type":"sleep","id":"等待","durationMs":10}),
    ));
    for kind in ["function_call_output", "custom_tool_call_output"] {
        cases.push(("rawResponseItem/completed", json!({"type":kind,"call_id":"工具调用","output":[{"type":"encrypted_content","encrypted_content":"密文"}]})));
    }
    cases.push(("item/completed", json!({"type":"functionCallOutput","id":"展示加密结果","name":"工具","output":[{"type":"encrypted_content","encrypted_content":"密文"}]})));
    for (method, output) in cases {
        let mut m = gap_b_seeded();
        let turn = turn_at(5000);
        gap_b_started(&mut m, &turn);
        gap_b_delivery(&mut m, &turn, "兄弟消费", "/root/甲");
        gap_b_summary(&mut m, &turn);
        let proof = m
            .coverage_open_proof("乙", &turn)
            .cloned()
            .expect("前置：真实先验raw和开始已授予Open");
        let (count, bytes, _) = m.event_log_pending_stats("乙", &turn);
        assert!(count >= 2 && bytes > 0);
        let mut actions = gap_b_notify(
            &mut m,
            method,
            json!({"threadId":"乙","turnId":turn,"item":output}),
        );
        assert_eq!(
            m.coverage_open_proof("乙", &turn),
            Some(&proof),
            "合法输出不能破坏真实Open：{output}"
        );
        assert_eq!(
            m.coverage_observation_gen("乙"),
            Some(proof.observation_gen)
        );
        let (after_count, after_bytes, _) = m.event_log_pending_stats("乙", &turn);
        assert!(
            after_count >= count && after_bytes >= bytes,
            "输出不能提前丢弃原摘要"
        );
        actions.extend(gap_b_completed(&mut m, &turn));
        assert!(matches!(
            gap_b_coverage(&m, &turn),
            TurnCoverage::Complete { .. }
        ));
        actions.extend(gap_b_sweep(&mut m));
        assert!(
            !gap_b_has_gap(&actions, &turn),
            "合法输出全流程零误报：{output}"
        );
        assert_eq!(m.event_log_pending_stats("乙", &turn), (0, 0, 0));
    }
}

#[derive(Debug)]
struct GapStepCheckpoint {
    processing: bool,
    count: usize,
    bytes: usize,
    total_bytes: usize,
    coverage: Option<TurnCoverage>,
}

struct GapStepTrace {
    thread: String,
    turn: String,
    checkpoints: Vec<GapStepCheckpoint>,
}

thread_local! {
    static GAP_STEP_TRACE: std::cell::RefCell<Option<GapStepTrace>> = const { std::cell::RefCell::new(None) };
}

struct GapStepTraceGuard;

impl GapStepTraceGuard {
    fn start(thread: &str, turn: &str) -> Self {
        GAP_STEP_TRACE.with(|slot| {
            assert!(slot.borrow().is_none(), "同线程不得嵌套安装采集器");
            *slot.borrow_mut() = Some(GapStepTrace {
                thread: thread.into(),
                turn: turn.into(),
                checkpoints: Vec::new(),
            });
        });
        Self
    }

    fn finish(self) -> Vec<GapStepCheckpoint> {
        GAP_STEP_TRACE.with(|slot| slot.borrow_mut().take().expect("采集器已安装").checkpoints)
    }
}

impl Drop for GapStepTraceGuard {
    fn drop(&mut self) {
        GAP_STEP_TRACE.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}

impl Machine {
    /// 发布构建没有该钩子；只读采集实际 step 内生产 expire 完成时的状态。
    pub(super) fn capture_event_log_checkpoint(&self) {
        GAP_STEP_TRACE.with(|slot| {
            if let Some(trace) = slot.borrow_mut().as_mut() {
                let (count, bytes, total_bytes) =
                    self.event_log_pending_stats(&trace.thread, &trace.turn);
                let coverage = turn_started_ms(&trace.turn)
                    .and_then(|ms| {
                        self.runs.event_log.coverage.get(&(
                            ms,
                            trace.thread.clone(),
                            trace.turn.clone(),
                        ))
                    })
                    .cloned();
                trace.checkpoints.push(GapStepCheckpoint {
                    processing: self.runs.event_log.processing_step,
                    count,
                    bytes,
                    total_bytes,
                    coverage,
                });
            }
        });
    }
}

#[test]
fn gap_b_review1_真实完成自动退订与同一步读取反证均经过嵌套结算屏障() {
    use crate::relations::rollout_pairing::{
        ReadTicket, ReadTicketState, RolloutFile, RolloutPathState,
    };
    for counterevidence in [false, true] {
        let mut m = gap_b_seeded();
        let turn = turn_at(5000);
        gap_b_started(&mut m, &turn);
        gap_b_delivery(&mut m, &turn, "兄弟消费", "/root/甲");
        gap_b_summary(&mut m, &turn);
        let start = m
            .coverage_open_proof("乙", &turn)
            .cloned()
            .expect("前置：真实通知已建立Open");
        assert_eq!(
            m.coverage_observation_gen("乙"),
            Some(start.observation_gen)
        );
        let (count, bytes, total_bytes) = m.event_log_pending_stats("乙", &turn);
        assert!(count >= 2 && bytes > 0);
        assert!(
            !m.event_log_evidence_in_flight("run-a", "乙"),
            "前置：没有独立归属续期遮挡"
        );
        if counterevidence {
            // 文件读取在此前已经派出；与摘要同一时刻到期，但只会在完成通知的后处理退休。
            let ticket = ReadTicket {
                id: 880,
                thread: "乙".into(),
                conn_epoch: m.relations.conn_epoch,
                observation_gen: start.observation_gen,
                issued_seq: m.coverage_next_seq().unwrap(),
                deadline: PENDING_MS,
                state: ReadTicketState::Active,
            };
            m.relations
                .aux
                .rollout
                .coverage_reads
                .insert(ticket.id, ticket);
            assert_eq!(m.coverage_read_deadline("乙"), Some(PENDING_MS));
        } else {
            // 根已释放的持久快照：本轮仍在活动，自动退订须等真实 completed 处理。
            m.records.entry("root".into()).or_default().last_run = Some("run-a".into());
            m.relations.contexts.get_mut("run-a").unwrap().authority = Authority::Revoked;
            assert!(!m.claims.contains_key("root"));
            assert!(!m.records["root"].subscribed);
            assert!(m.child_activity_protected("乙"));
            assert!(!m.relation_needs_subscription("乙"));
            assert!(m.relations.attachments["乙"].valid);
            assert!(!m.relations.aux.released.contains_key("乙"));
            assert!(
                m.coverage_read_deadline("乙").is_none(),
                "前置：没有读取屏障掩盖处理中屏障"
            );
            // 已停止跟随的文件游标会在同一次 step 后处理清掉；没有票据或文件消费反证。
            m.relations.aux.rollout.files.insert(
                "乙".into(),
                RolloutFile {
                    epoch: m.relations.conn_epoch,
                    path: RolloutPathState::Ready("D:\\unused-乙.jsonl".into()),
                    cursor: Some(crate::rollout::RolloutCursor {
                        offset: 128,
                        ..Default::default()
                    }),
                    reading: None,
                    reads: 2,
                    next_at: 0,
                },
            );
        }
        m.clock.mono_ms = PENDING_MS + 1;
        m.clock.now_ms += PENDING_MS + 1;
        let capture = GapStepTraceGuard::start("乙", &turn);
        let mut actions = gap_b_completed(&mut m, &turn);
        let checkpoints = capture.finish();
        let nested = checkpoints
            .iter()
            .find(|point| {
                point.processing && matches!(point.coverage, Some(TurnCoverage::Complete { .. }))
            })
            .expect("真实completed必须在封存后经commit_notification触发嵌套expire");
        assert_eq!(
            (nested.count, nested.bytes, nested.total_bytes),
            (count, bytes, total_bytes),
            "真实step中途原摘要与字节必须全部保留"
        );
        assert!(
            checkpoints.iter().any(|point| !point.processing),
            "同一次真实step确实进入统一尾部"
        );
        if counterevidence {
            assert_eq!(
                m.relations.aux.rollout.coverage_reads[&880].state,
                ReadTicketState::Retired
            );
            assert!(matches!(
                gap_b_coverage(&m, &turn),
                TurnCoverage::Veto { .. }
            ));
            assert!(m.coverage_observation_gen("乙").is_none());
            assert_eq!(
                logs(&actions)
                    .iter()
                    .filter(|row| row.2 == "记录缺口" && row.3["turnId"] == turn)
                    .count(),
                count,
                "完成后的同一步票据反证必须保住全部原摘要"
            );
        } else {
            assert!(actions.iter().any(|action| matches!(action, Action::Request(context) if context.method == "thread/unsubscribe" && context.params.as_ref().is_some_and(|p| p["threadId"] == "乙"))), "必须真正走完成触发的自动退订请求");
            assert!(m.relations.aux.released.contains_key("乙"));
            assert!(!m.relations.attachments["乙"].valid);
            assert!(m.coverage_observation_gen("乙").is_none());
            assert!(
                matches!(gap_b_coverage(&m, &turn), TurnCoverage::Complete { .. }),
                "自动退订不可污染之前封存的完成"
            );
            assert_eq!(
                m.coverage_disposition("乙", &turn),
                CoverageDisposition::AllowCollaborationExclusion
            );
            assert!(m.relations.aux.rollout.files["乙"].cursor.is_none());
            assert_eq!(
                m.relations.aux.rollout.files["乙"].reads, 0,
                "同一步实际清理停止跟随文件"
            );
            assert!(!gap_b_has_gap(&actions, &turn));
            let sealed = gap_b_coverage(&m, &turn);
            let unsubscribe = actions
                .iter()
                .find_map(|action| match action {
                    Action::Request(context) if context.method == "thread/unsubscribe" => {
                        Some(context.clone())
                    }
                    _ => None,
                })
                .expect("自动退订请求确已发出");
            actions.extend(gap_b_apply(
                &mut m,
                crate::claim::Event::Reply {
                    context: unsubscribe,
                    result: Ok(json!({"status":"unsubscribed"})),
                },
            ));
            actions.extend(gap_b_completed(&mut m, &turn));
            assert_eq!(
                gap_b_coverage(&m, &turn),
                sealed,
                "实际退订回包后重复完成保持原封存证明"
            );
        }
        if m.runs.event_log.timer.is_some() {
            actions.extend(gap_b_sweep(&mut m));
        }
        assert_eq!(m.event_log_pending_stats("乙", &turn), (0, 0, 0));
        assert_eq!(gap_b_has_gap(&actions, &turn), counterevidence);
    }
}

// 第二轮实现审查日志入口红测：仅追加到原有 event_log 测试模块，不改生产状态。
fn gap_b_review2_log_sequence(label: &str, method: &str, output: Value, expect_gap: bool) {
    let mut m = gap_b_seeded();
    let turn = turn_at(5000);
    gap_b_started(&mut m, &turn);
    gap_b_delivery(&mut m, &turn, "兄弟消费", "/root/甲");
    gap_b_summary(&mut m, &turn);
    let proof = m
        .coverage_open_proof("乙", &turn)
        .cloned()
        .expect("前置：真实开始和先验raw建立Open");
    assert_eq!(
        m.coverage_observation_gen("乙"),
        Some(proof.observation_gen)
    );
    assert_eq!(
        m.coverage_disposition("乙", &turn),
        CoverageDisposition::Wait
    );
    assert!(!m.event_log_evidence_in_flight("run-a", "乙"));
    assert!(!m.historical_subagent_for_run("run-a", "乙"));
    assert!(m.relations.operations.values().all(|op| !op.proof_expired));
    assert!(
        m.coverage_read_deadline("乙").is_none(),
        "前置：无读取屏障掩盖结构检查"
    );
    let original = m.runs.event_log.pending.clone();
    let (count, bytes, _) = m.event_log_pending_stats("乙", &turn);
    assert!(count >= 2 && bytes > 0, "前置：开始与摘要真实占用暂存字节");
    let mut actions = gap_b_notify(
        &mut m,
        method,
        json!({"threadId":"乙","turnId":turn,"item":output}),
    );
    let (after_count, after_bytes, _) = m.event_log_pending_stats("乙", &turn);
    assert!(
        after_count >= count && after_bytes >= bytes,
        "{label}不能提前删除原摘要"
    );
    for pending in &original {
        assert!(
            m.runs.event_log.pending.contains(pending),
            "{label}必须逐条保留原始摘要，不只比较总数"
        );
    }
    if !expect_gap {
        assert_eq!(
            m.coverage_open_proof("乙", &turn),
            Some(&proof),
            "合法结构必须保持原Open：{label}"
        );
        assert_eq!(
            m.coverage_observation_gen("乙"),
            Some(proof.observation_gen)
        );
    }
    actions.extend(gap_b_completed(&mut m, &turn));
    actions.extend(gap_b_sweep(&mut m));
    let gaps = logs(&actions)
        .iter()
        .filter(|row| row.2 == "记录缺口" && row.3["turnId"] == turn)
        .count();
    if expect_gap {
        assert!(
            gaps >= count,
            "{label}的已声明坏结构必须保住全部{count}条原始摘要并写缺口，实际{gaps}"
        );
        assert!(matches!(
            gap_b_coverage(&m, &turn),
            TurnCoverage::Veto { .. }
        ));
        assert!(m.coverage_observation_gen("乙").is_none());
    } else {
        assert_eq!(gaps, 0, "合法结构或不透明载荷最终不得误报：{label}");
        assert!(matches!(
            gap_b_coverage(&m, &turn),
            TurnCoverage::Complete { .. }
        ));
    }
    assert_eq!(
        m.event_log_pending_stats("乙", &turn),
        (0, 0, 0),
        "最终暂存及字节必须清零：{label}"
    );
}

#[test]
fn gap_b_review2_log_command_actions坏结构最终全部写缺口() {
    for actions in [
        json!([7]),
        json!([{"type":"read","command":"cat f","name":"f"}]),
        json!([{"type":"search","command":"rg q","query":7}]),
    ] {
        gap_b_review2_log_sequence(
            "commandActions",
            "item/completed",
            json!({"type":"commandExecution","id":"坏命令","command":"echo x","cwd":"D:\\w","status":"completed","commandActions":actions}),
            true,
        );
    }
}

#[test]
fn gap_b_review2_log_changes坏结构最终全部写缺口() {
    for changes in [
        json!([7]),
        json!([{"path":"f","diff":"补丁","kind":{"type":"update","move_path":7}}]),
        json!([{"path":"f","kind":{"type":"add"}}]),
    ] {
        gap_b_review2_log_sequence(
            "changes",
            "item/completed",
            json!({"type":"fileChange","id":"坏补丁","status":"completed","changes":changes}),
            true,
        );
    }
}

#[test]
fn gap_b_review2_log_agents_states坏结构最终全部写缺口() {
    for states in [
        json!({"child":7}),
        json!({"child":{"status":"futureState"}}),
        json!({"child":{"status":"completed","message":7}}),
    ] {
        gap_b_review2_log_sequence(
            "agentsStates",
            "item/completed",
            json!({"type":"collabAgentToolCall","id":"坏代理状态","tool":"wait","status":"completed","senderThreadId":"乙","receiverThreadIds":[],"agentsStates":states}),
            true,
        );
    }
}

#[test]
fn gap_b_review2_log_mcp已声明嵌套坏结构最终全部写缺口() {
    for (field, value) in [
        ("appContext", json!({"connectorId":7})),
        (
            "mcpAppUi",
            json!({"resourceUri":"ui://结果","preferredModelDisplayMode":"futureMode"}),
        ),
        ("result", json!({"content":7})),
        ("error", json!({"message":7})),
        ("readOnlyHint", json!(7)),
    ] {
        let mut item = json!({"type":"mcpToolCall","id":"坏MCP","server":"测试","tool":"读取","status":"completed","arguments":{"任意":[7,null]}});
        item[field] = value;
        gap_b_review2_log_sequence(field, "item/completed", item, true);
    }
}

#[test]
fn gap_b_review2_log_dynamic已声明内容坏结构最终全部写缺口() {
    for (field, value) in [
        ("contentItems", json!([7])),
        ("contentItems", json!([{"type":"inputImage","imageUrl":7}])),
        ("success", json!(7)),
        ("namespace", json!(["错误"])),
    ] {
        let mut item = json!({"type":"dynamicToolCall","id":"坏动态工具","tool":"读取","status":"completed","arguments":7});
        item[field] = value;
        gap_b_review2_log_sequence(field, "item/completed", item, true);
    }
}

#[test]
fn gap_b_review2_log_agent_message元数据坏结构最终全部写缺口() {
    for (field, value) in [
        ("phase", json!(7)),
        ("phase", json!("futurePhase")),
        ("memoryCitation", json!(7)),
        (
            "memoryCitation",
            json!({"entries":[{"path":"f","lineStart":4294967296_u64,"lineEnd":1,"note":"备注"}],"threadIds":[]}),
        ),
        ("delivery", json!("futureDelivery")),
        ("questions", json!([{"title":"问题","options":[7]}])),
    ] {
        let mut item = json!({"type":"agentMessage","id":"坏展示元数据","text":"输出"});
        item[field] = value;
        gap_b_review2_log_sequence(field, "item/completed", item, true);
    }
}

#[test]
fn gap_b_review2_log_command可选类型与数值边界最终全部写缺口() {
    for (field, value) in [
        ("pluginId", json!(7)),
        ("source", json!("futureSource")),
        ("exitCode", json!(2147483648_u64)),
        ("durationMs", json!(18446744073709551615_u64)),
    ] {
        let mut item = json!({"type":"commandExecution","id":"坏命令元数据","command":"echo x","cwd":"D:\\w","status":"completed","commandActions":[]});
        item[field] = value;
        gap_b_review2_log_sequence(field, "item/completed", item, true);
    }
}

#[test]
fn gap_b_review2_log_raw可选坏结构最终全部写缺口() {
    for (label, item) in [
        (
            "加密参数元素",
            json!({"type":"function_call","call_id":"普通调用","name":"ordinary_tool","arguments":"不要求JSON","encrypted_function_args":[7]}),
        ),
        (
            "raw命名空间",
            json!({"type":"function_call","call_id":"普通调用","name":"ordinary_tool","arguments":"{}","namespace":7}),
        ),
        (
            "公开透传元数据",
            json!({"type":"reasoning","summary":[],"internal_chat_message_metadata_passthrough":{"turn_id":7}}),
        ),
        (
            "raw阶段",
            json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":"输出"}],"phase":7}),
        ),
        (
            "图片detail",
            json!({"type":"function_call_output","call_id":"普通调用","output":[{"type":"input_image","image_url":"data:image/png;base64,AA==","detail":"futureDetail"}]}),
        ),
        (
            "shell环境变量",
            json!({"type":"local_shell_call","status":"completed","action":{"type":"exec","command":["echo","ok"],"env":{"X":7}}}),
        ),
        (
            "shell负超时",
            json!({"type":"local_shell_call","status":"completed","action":{"type":"exec","command":[],"timeout_ms":-1}}),
        ),
    ] {
        gap_b_review2_log_sequence(label, "rawResponseItem/completed", item, true);
    }
}

#[test]
fn gap_b_review2_log_image_generation可选坏结构最终全部写缺口() {
    for (field, value) in [
        ("transparentBackground", json!(7)),
        ("savedPath", json!(7)),
        ("failure", json!({"type":"usageLimitExceeded","limitId":7})),
    ] {
        let mut item = json!({"type":"imageGeneration","id":"坏图片元数据","status":"completed","result":"图像"});
        item[field] = value;
        gap_b_review2_log_sequence(field, "item/completed", item, true);
    }
}

#[test]
fn gap_b_review2_log_合法结构缺省null与opaque对照最终零缺口() {
    let public = vec![
        json!({"type":"commandExecution","id":"命令","command":"echo x","cwd":"D:\\w","status":"completed","commandActions":[
            {"type":"read","command":"cat f","name":"f","path":"D:\\w\\f"},
            {"type":"listFiles","command":"ls","path":null},
            {"type":"search","command":"rg q","query":null},
            {"type":"unknown","command":"echo x"}],"source":"unifiedExecInteraction","exitCode":-2147483648_i64,"durationMs":-1,"pluginId":null}),
        json!({"type":"commandExecution","id":"缺省来源","command":"echo x","cwd":"D:\\w","status":"completed","commandActions":[]}),
        json!({"type":"fileChange","id":"补丁","status":"completed","changes":[
            {"path":"f","diff":"补丁","kind":{"type":"add"}},
            {"path":"g","diff":"补丁","kind":{"type":"delete"}},
            {"path":"h","diff":"补丁","kind":{"type":"update","move_path":null}}]}),
        json!({"type":"collabAgentToolCall","id":"代理状态","tool":"wait","status":"completed","senderThreadId":"乙","receiverThreadIds":[],"agentsStates":{"unused":{"status":"completed","message":null}},"prompt":null,"reasoningEffort":"未来有效强度"}),
        json!({"type":"agentMessage","id":"消息","text":"输出","phase":"final_answer","delivery":"async","memoryCitation":{"entries":[{"path":"f","lineStart":0,"lineEnd":4294967295_u64,"note":"备注"}],"threadIds":["旧线程"]},"questions":[{"title":"问题","options":["甲","乙"]}]}),
        json!({"type":"agentMessage","id":"空可选","text":"输出","phase":null,"delivery":null,"memoryCitation":null,"questions":null,"未知附加字段":{"可兼容":7}}),
        json!({"type":"mcpToolCall","id":"MCP","server":"测试","tool":"读取","status":"completed","arguments":[7,null,{"未知":false}],
            "appContext":{"connectorId":"连接器","appName":null},"mcpAppUi":{"resourceUri":"ui://结果","preferredModelDisplayMode":"inline"},"readOnlyHint":false,
            "result":{"content":[7,null,{"未知内容":{"任意":false}}],"structuredContent":7,"_meta":[7]},"error":null}),
        json!({"type":"dynamicToolCall","id":"动态","tool":"读取","status":"completed","arguments":7,"namespace":null,"success":true,"durationMs":-1,
            "contentItems":[{"type":"inputText","text":"输出"},{"type":"inputImage","imageUrl":"image://结果"},{"type":"inputAudio","audioUrl":"audio://结果"}]}),
        json!({"type":"dynamicToolCall","id":"动态空可选","tool":"读取","status":"completed","arguments":null,"contentItems":null,"success":null}),
        json!({"type":"reasoning","id":"默认空数组"}),
    ];
    for item in public {
        gap_b_review2_log_sequence("合法public结构", "item/completed", item, false);
    }
    for item in [
        json!({"type":"function_call","call_id":"普通调用","name":"ordinary_tool","arguments":"不要求JSON","namespace":null,"encrypted_function_args":["密文"]}),
        json!({"type":"message","role":"assistant","phase":"commentary","content":[{"type":"output_text","text":"输出"}],"internal_chat_message_metadata_passthrough":{"turn_id":"原样轮号"}}),
        json!({"type":"function_call_output","call_id":"普通调用","output":[{"type":"input_image","image_url":"data:image/png;base64,AA==","detail":"low"}]}),
        json!({"type":"local_shell_call","status":"completed","call_id":null,"action":{"type":"exec","command":["echo","ok"],"timeout_ms":0,"working_directory":null,"user":"用户","env":{"X":"值"}}}),
        json!({"type":"tool_search_call","execution":"server","arguments":[7,null,{"任意":"值"}]}),
        json!({"type":"tool_search_output","execution":"server","status":"completed","tools":[7,null,{"任意":"值"}]}),
    ] {
        gap_b_review2_log_sequence("合法raw或opaque", "rawResponseItem/completed", item, false);
    }
}
