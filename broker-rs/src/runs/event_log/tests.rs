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

/// 本运行的子代理某一轮只消费了平级或下级的协作消息（如兄弟子代理之间互发）：到期正常丢弃，不写缺口。
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
        ("跨层下发", &[("t", "/root", "/root/甲/乙")], true),
        (
            "协作记录在别的轮",
            &[("另一轮", "/root/甲", "/root/乙")],
            true,
        ),
        ("没有消费记录", &[], true),
    ];
    for (name, notes, expect_gap) in cases {
        let mut m = setup();
        candidate(&mut m, "乙", "root");
        for (turn, author, recipient) in *notes {
            m.note_event_log_delivery("乙", turn, author, recipient);
        }
        assert!(
            !m.historical_subagent_for_run("run-a", "乙"),
            "前置：不是历史子代理"
        );
        let mut a = vec![];
        m.buffer_child_event_log("乙", "item/completed", &item("乙", "t"), false, &mut a);
        assert_eq!(m.runs.event_log.pending.len(), 1, "前置：{name} 已暂存");
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
        if collaboration {
            m.note_event_log_delivery("乙", "t", "/root/甲", "/root/乙");
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
            json!({"threadId":"乙","turnId":"t","item":{"id":"一","type":"agentMessage","text":"第一条"}}),
        );
        m.buffer_child_event_log("乙", "item/completed", &first, false, &mut a);
        m.clock.mono_ms = PENDING_MS / 2;
        let second = Some(
            json!({"threadId":"乙","turnId":"t","item":{"id":"二","type":"agentMessage","text":"第二条"}}),
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

/// 发件方向的判定只看路径层级：平级与下级是协作，直接上级和跨层下发不是。
#[test]
fn tree_log_协作消息方向判定() {
    assert!(collaboration_delivery("/root/甲", "/root/乙"));
    assert!(collaboration_delivery("/root/乙/丙", "/root/乙"));
    assert!(collaboration_delivery("/root/乙/丙/丁", "/root/乙"));
    assert!(collaboration_delivery("/root/a/x", "/root/a/y"));
    assert!(!collaboration_delivery("/root", "/root/乙"));
    assert!(!collaboration_delivery("/root/甲", "/root/甲/乙"));
    assert!(!collaboration_delivery("/root", "/root/甲/乙"));
    assert!(!collaboration_delivery("/root/乙", "/root/乙"));
    assert!(
        collaboration_delivery("/root/乙x", "/root/乙"),
        "名字前缀相同的是平级"
    );
    assert!(
        !collaboration_delivery("/root/乙x/丙", "/root/乙"),
        "名字前缀相同的平级的下级，不是乙的下级"
    );
    assert!(
        !collaboration_delivery("/root/甲/丙", "/root/乙"),
        "别的分支的下级"
    );
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
