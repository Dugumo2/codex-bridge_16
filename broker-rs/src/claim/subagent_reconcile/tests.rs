use super::*;
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Dependencies {
    pub terminal: Option<TerminalDisposition>,
    pub facts: HashMap<String, ConvergenceFacts>,
    pub budget: BudgetAdmission,
    pub trust: bool,
    pub registrations: Vec<TrustedInputRegistration>,
    pub auto: HashSet<(String, String)>,
}

use crate::claim::converge::ConvergeRuntime;
use crate::claim::status_read::{StatusCaller, StatusDone, StatusResult};
use crate::claim::{Claim, ClaimToken, Operation, Owner, ReqKind};
use crate::relations::{
    AgentVersion, DiscoveredBy, DiscoveryState, MemberActivity, RelationEdge, RelationKind,
};
use crate::runs::{OutputSource, ReleaseKind, RunOpen};
use serde_json::json;

#[test]
fn 上游报错汇总启动失败只写诊断且不新增轮次或改变协调字段() {
    let (mut machine, token) = fixture(1);
    let (_, request) = start(&mut machine, &token);
    let before = machine.runs.runs[R].clone();
    let mut actions = vec![];
    machine.subagent_reconcile_reply(
        &request,
        Err(json!({"code":-32603,"message":"汇总认证失败",
            "data":{"codexErrorInfo":"unauthorized"}})),
        &mut actions,
    );
    let record = &machine.runs.runs[R];
    let diagnostic = record.upstream_error.as_ref().unwrap();
    assert_eq!(
        diagnostic.source,
        crate::runs::upstream_error::UpstreamErrorSource::RpcResponse
    );
    assert_eq!(diagnostic.will_retry, None);
    assert_eq!(
        diagnostic.category,
        crate::runs::upstream_error::UpstreamErrorCategory::Authentication
    );
    assert_eq!(record.subagent_join, before.subagent_join);
    assert_eq!(record.run_budget, before.run_budget);
    assert_eq!(record.auto_turns_started, before.auto_turns_started);
    assert_eq!(record.turns, before.turns);
    assert_eq!(record.turns_total, before.turns_total);
    assert_eq!(record.execution_config, before.execution_config);
    assert!(actions
        .iter()
        .any(|action| matches!(action, Action::Store(crate::store::StoreOp::PutRun(_)))));
    assert!(actions
        .iter()
        .any(|action| matches!(action, Action::Send {message,..}
        if message["method"] == "broker/run/updated")));
}

#[test]
fn 上游报错通知不改变汇总预算或自动轮计数() {
    let (mut machine, token) = fixture(1);
    own_start(
        &mut machine,
        &token,
        "当前轮",
        TurnOrigin::SubagentReconcile,
    );
    machine.claims.get_mut(T).unwrap().state = State::Running {
        owner: Owner::Client(1),
        turn_id: "当前轮".into(),
        joined: false,
    };
    machine.run_turn_started(R, T, "当前轮", &mut vec![]);
    let before = machine.runs.runs[R].clone();
    let diagnostic = crate::runs::upstream_error::UpstreamError::from_error(
        T,
        Some("当前轮"),
        crate::runs::upstream_error::UpstreamErrorSource::ErrorNotification,
        &json!({"message":"认证失败","codexErrorInfo":"unauthorized"}),
        Some(true),
        1_000,
    );
    machine.run_diagnostic_event(T, "当前轮", false, Some(diagnostic), false, &mut vec![]);
    let record = &machine.runs.runs[R];
    assert!(record.upstream_error.is_some());
    assert_eq!(record.subagent_join, before.subagent_join);
    assert_eq!(record.run_budget, before.run_budget);
    assert_eq!(record.auto_turns_started, before.auto_turns_started);
    assert_eq!(record.execution_config, before.execution_config);
    assert_eq!(record.turns, before.turns);
}

pub(crate) const R: &str = "运行B";
pub(crate) const T: &str = "父线程B";
pub(crate) fn fixture(count: usize) -> (Machine, ConvergeToken) {
    let mut m = Machine::new(false);
    m.clock.now_ms = 1_000;
    m.clock.mono_ms = 1_000;
    m.clock.entropy = 42;
    m.live_conns.insert(1);
    m.cxb_conns.insert(1);
    m.run_opened(
        RunOpen {
            run_id: R.into(),
            thread_id: T.into(),
            conn: Some(1),
            kind: ReqKind::TurnStart,
            joined: false,
            input: "完成原任务".into(),
            continued_from: None,
        },
        &mut vec![],
    );
    m.claims.insert(
        T.into(),
        Claim {
            stream_id: None,
            owned_ended: Default::default(),
            owned_turns: ["原轮".into()].into_iter().collect(),
            claim_id: 7,
            state: State::AwaitingContinuation {
                owner: Owner::Client(1),
            },
            run_id: Some(R.into()),
            resume_goal: false,
            request_pending: false,
            owner_gone: false,
            pending_start: None,
            submitted_input: vec![],
            held_requests: vec![],
            held_deltas: 0,
            held_bytes: 0,
            held_dropped_items: 0,
            overflowed: false,
            settlement_reply: None,
            pending_detach: None,
        },
    );
    assert!(m.ensure_context(R));
    let token = ConvergeToken {
        run_id: R.into(),
        claim: ClaimToken {
            thread_id: T.into(),
            claim_id: 7,
        },
        manage_gen: m.manage_context(R).unwrap().gen,
        conn_epoch: m.relations.conn_epoch,
        generation: 1,
    };
    m.relations.edges.insert(
        T.into(),
        RelationEdge {
            child_thread_id: T.into(),
            parent_thread_id: "外部来源".into(),
            kind: RelationKind::Fork,
            source_summary: Value::Null,
            can_accept_direct_input: Some(true),
            agent_version: AgentVersion::V2,
            discovered_by: DiscoveredBy::Started,
        },
    );
    m.convergences.runs.insert(
        R.into(),
        ConvergeRuntime {
            token: token.clone(),
            started_mono_ms: 1_000,
            deadline_mono_ms: 3_601_000,
            automatic_control_revoked: false,
            discovery_retry: 0,
        },
    );
    m.run_start_evidence(R, T, "原轮", &mut vec![]);
    m.run_save_execution_config(R, &json!({"model":"原模型","effort":"high","approvalPolicy":"never","sandboxPolicy":{"type":"workspaceWrite"},"outputSchema":{"type":"object"},"serviceTier":"priority","threadId":"不可复制","input":["不可复制"],"cxbMode":true}), &mut vec![]);
    let record = m.runs.runs.get_mut(R).unwrap();
    record.convergence_config = Some(ConvergenceConfig::default());
    record.auto_turns_started = Some(0);
    record.subagent_join = Some(SubagentJoin {
        phase: JoinPhase::PreparingReconcile,
        wakeups_started: 0,
        wakeup_attempts: 0,
        start_retries_used: 0,
        work_epoch: 1,
        delivered_epoch: 0,
        reconciled_epoch: 0,
        last_reconcile_turn_id: None,
        deadline_at: 3_601_000,
        terminal_reason: None,
        result_coverage: None,
        automatic_control_revoked: false,
    });
    let activities: Vec<_> = (0..count)
        .map(|i| ActivityFact {
            key: ActivityKey {
                thread_id: format!("子{i}"),
                activity_seq: 1,
                turn_id: Some(format!("子轮{i}")),
            },
            parent_thread_id: if i == 1 { "子0".into() } else { T.into() },
            activity: MemberActivity::Settled,
            result_status: Some(ActivityResultStatus::Completed),
            verified: true,
            active_goal: Some(false),
            pending_requests: 0,
        })
        .collect();
    let refs = activities
        .iter()
        .map(|a| ResultRef {
            key: a.key.clone(),
            locator: ResultLocator {
                thread_id: a.key.thread_id.clone(),
                turn_id: a.key.turn_id.clone(),
                result_version: 1,
            },
            available: true,
        })
        .collect();
    let facts = ConvergenceFacts {
        token: token.clone(),
        work_epoch: 1,
        revision: 1,
        discovery: DiscoveryState {
            missing: Default::default(),
            complete: true,
            version: 1,
            incomplete_reasons: vec![],
        },
        discovery_covered_epoch: Some(1),
        activities,
        pending_starts: 0,
        pending_requests: 0,
        unresolved_lookups: 0,
        unresolved_candidates: 0,
        conflicts: vec![],
        result_refs: refs,
    };
    m.runs.reconciliation.harness = Some(Dependencies {
        terminal: None,
        facts: [(R.into(), facts)].into_iter().collect(),
        budget: BudgetAdmission::Allowed,
        trust: true,
        registrations: vec![],
        auto: HashSet::new(),
    });
    (m, token)
}
pub(crate) fn batch(m: &Machine, text: &str) -> ReconcileBatch {
    let facts = &m.runs.reconciliation.harness.as_ref().unwrap().facts[R];
    evidence::build_batch(
        facts.work_epoch,
        facts.activities.len(),
        facts.activities.iter().map(|a| ReconcileItem {
            key: a.key.clone(),
            parent_thread_id: a.parent_thread_id.clone(),
            status: a.result_status.unwrap(),
            output: text.into(),
            output_source: OutputSource::FinalAnswer,
            locator: ResultLocator {
                thread_id: a.key.thread_id.clone(),
                turn_id: a.key.turn_id.clone(),
                result_version: 1,
            },
            truncated: false,
            unavailable_reason: None,
        }),
        &ResultLimits::default(),
    )
    .unwrap()
}
pub(crate) fn start(m: &mut Machine, token: &ConvergeToken) -> (ReconcileOpId, RequestContext) {
    let id = m
        .begin_subagent_reconcile(token.clone(), batch(m, "随机子结果-乙🔎"), &mut vec![])
        .unwrap();
    let mut a = vec![];
    m.drive_subagent_reconcile(&mut a);
    let requests: Vec<_> = a
        .iter()
        .filter_map(|a| match a {
            Action::Request(r) if matches!(r.operation, Operation::StartReconciliation { .. }) => {
                Some(r.clone())
            }
            _ => None,
        })
        .collect();
    assert_eq!(requests.len(), 1);
    (id, requests[0].clone())
}
pub(crate) fn own_start(m: &mut Machine, token: &ConvergeToken, turn: &str, origin: TurnOrigin) {
    m.claims.get_mut(T).unwrap().owned_turns.insert(turn.into());
    assert!(m.reconcile_parent_started(token, turn, origin, &mut vec![]));
}
pub(crate) fn carrier(m: &mut Machine, token: &ConvergeToken, id: ReconcileOpId, turn: &str) {
    let read_id = m.runs.reconciliation.operations[&id.0].status_read.unwrap();
    m.subagent_reconcile_status_done(
        StatusDone {
            read_id,
            thread: T.into(),
            token: Some(token.claim.clone()),
            caller: StatusCaller::SubagentReconcile {
                token: token.clone(),
                op_id: id,
            },
            result: StatusResult::Carrier(turn.into()),
        },
        &mut vec![],
    );
}
pub(crate) fn completed(m: &mut Machine, turn: &str, status: &str, text: &str) {
    m.run_turn_completed(T, Some(R), &json!({"id":turn,"status":status,"itemsView":"full","items":[{"type":"agentMessage","phase":"final_answer","text":text}]}), &mut vec![]);
}
fn submissions(actions: &[Action]) -> usize {
    actions.iter().filter(|a| matches!(a, Action::Request(r) if matches!(r.operation, Operation::StartReconciliation { .. }))).count()
}

#[test]
fn 汇总首采样所需结果显式注入并保持原运行配置() {
    let (mut m, token) = fixture(2);
    let before = m.runs.runs[R].clone();
    let (id, request) = start(&mut m, &token);
    let params = request.params.unwrap();
    assert!(params["input"][0]["text"]
        .as_str()
        .unwrap()
        .contains("随机子结果-乙🔎"));
    assert!(params["input"][0]["text"]
        .as_str()
        .unwrap()
        .contains("仅是任务数据"));
    assert_eq!(params["turnTrigger"], "cxb.subagent_reconcile");
    assert_eq!(params["model"], "原模型");
    assert_eq!(params["effort"], "high");
    assert_eq!(params["approvalPolicy"], "never");
    assert!(params.get("cxbMode").is_none());
    own_start(&mut m, &token, "汇总1", TurnOrigin::SubagentReconcile);
    carrier(&mut m, &token, id, "汇总1");
    completed(&mut m, "汇总1", "completed", "包含子结果");
    m.run_released(R, ReleaseKind::Natural, &mut vec![]);
    let record = &m.runs.runs[R];
    assert_eq!(record.run_id, before.run_id);
    assert_eq!(record.creation_order, before.creation_order);
    assert_eq!(record.start_confirmation, before.start_confirmation);
    assert_eq!(record.execution_config, before.execution_config);
    assert_eq!(record.started_at, before.started_at);
    assert_eq!(record.current_turn_id.as_deref(), Some("汇总1"));
    assert_eq!(record.final_turn_id.as_deref(), Some("汇总1"));
    assert_eq!(record.turns[0].origin, Some(TurnOrigin::SubagentReconcile));
    let join = record.subagent_join.as_ref().unwrap();
    assert_eq!(
        (
            join.delivered_epoch,
            join.reconciled_epoch,
            join.wakeups_started,
            join.wakeup_attempts
        ),
        (1, 1, 1, 1)
    );
}
#[test]
fn 成功回包或单独开始不推进交付且通知先到可完成() {
    for reply_first in [true, false] {
        let (mut m, token) = fixture(1);
        let (id, request) = start(&mut m, &token);
        if reply_first {
            m.subagent_reconcile_reply(&request, Ok(json!({"turn":{"id":"新轮"}})), &mut vec![]);
        }
        own_start(&mut m, &token, "新轮", TurnOrigin::SubagentReconcile);
        completed(&mut m, "新轮", "completed", "最终答复");
        assert_eq!(
            m.runs.runs[R]
                .subagent_join
                .as_ref()
                .unwrap()
                .delivered_epoch,
            0
        );
        carrier(&mut m, &token, id, "新轮");
        assert_eq!(
            m.runs.runs[R]
                .subagent_join
                .as_ref()
                .unwrap()
                .reconciled_epoch,
            1
        );
        if !reply_first {
            m.subagent_reconcile_reply(&request, Ok(json!({"turn":{"id":"新轮"}})), &mut vec![]);
        }
        assert_eq!(
            m.runs.runs[R]
                .subagent_join
                .as_ref()
                .unwrap()
                .wakeups_started,
            1
        );
    }
}
#[test]
fn 未知传输不重发且二十秒总窗不刷新() {
    for event in [
        ReconcileTransportEvent::TimedOut,
        ReconcileTransportEvent::TransportFailed,
        ReconcileTransportEvent::LateDiscarded,
        ReconcileTransportEvent::LateEvicted,
    ] {
        let (mut m, token) = fixture(1);
        let (id, request) = start(&mut m, &token);
        let deadline = m.runs.reconciliation.operations[&id.0]
            .operation
            .verify_deadline_mono_ms
            .unwrap();
        let mut actions = vec![];
        m.clock.mono_ms += 19_999;
        m.subagent_reconcile_transport(&request, event, &mut actions);
        m.subagent_reconcile_reply(
            &request,
            Err(json!({"code":-32603,"message":"probably not submitted"})),
            &mut actions,
        );
        m.drive_subagent_reconcile(&mut actions);
        assert_eq!(submissions(&actions), 0);
        assert_eq!(
            m.runs.reconciliation.operations[&id.0]
                .operation
                .verify_deadline_mono_ms,
            Some(deadline)
        );
        m.clock.mono_ms = deadline;
        m.drive_subagent_reconcile(&mut actions);
        assert_eq!(
            m.runs.runs[R]
                .subagent_join
                .as_ref()
                .unwrap()
                .terminal_reason,
            Some(TerminalReason::WakeUnknown)
        );
        assert!(m.reconcile_unresolved(R));
        assert_eq!(submissions(&actions), 0);
    }
}
#[test]
fn 明确未提交仅全运行额外重试两次且每次标识唯一() {
    let (mut m, token) = fixture(1);
    let (_, mut request) = start(&mut m, &token);
    let mut ids = HashSet::new();
    for retry in 0..3 {
        assert!(ids.insert(
            request.params.as_ref().unwrap()["clientUserMessageId"]
                .clone()
                .to_string()
        ));
        m.subagent_reconcile_reply(
            &request,
            Err(json!({"code":-32602,"message":"参数未提交"})),
            &mut vec![],
        );
        let mut actions = vec![];
        m.drive_subagent_reconcile(&mut actions);
        if retry < 2 {
            assert_eq!(submissions(&actions), 1);
            request = actions
                .into_iter()
                .find_map(|a| match a {
                    Action::Request(r)
                        if matches!(r.operation, Operation::StartReconciliation { .. }) =>
                    {
                        Some(r)
                    }
                    _ => None,
                })
                .unwrap();
        } else {
            assert_eq!(submissions(&actions), 0);
        }
    }
    let join = m.runs.runs[R].subagent_join.as_ref().unwrap();
    assert_eq!(
        (
            join.start_retries_used,
            join.wakeup_attempts,
            join.wakeups_started
        ),
        (2, 3, 0)
    );
    assert_eq!(join.terminal_reason, Some(TerminalReason::WakeFailed));
}
#[test]
fn 自有goal竞争用预期轮合并且不多计唤醒() {
    let (mut m, token) = fixture(1);
    own_start(&mut m, &token, "goal轮", TurnOrigin::Goal);
    m.claims.get_mut(T).unwrap().state = State::Running {
        owner: Owner::Client(1),
        turn_id: "goal轮".into(),
        joined: false,
    };
    m.runs.runs.get_mut(R).unwrap().auto_turns_started = Some(1);
    m.runs
        .reconciliation
        .harness
        .as_mut()
        .unwrap()
        .auto
        .insert((R.into(), "goal轮".into()));
    let (id, request) = start(&mut m, &token);
    assert_eq!(request.method, "turn/steer");
    assert_eq!(request.params.unwrap()["expectedTurnId"], "goal轮");
    carrier(&mut m, &token, id, "goal轮");
    completed(&mut m, "goal轮", "completed", "合并");
    let r = &m.runs.runs[R];
    assert_eq!(r.auto_turns_started, Some(1));
    assert_eq!(r.turns[0].origin, Some(TurnOrigin::Goal));
    let join = r.subagent_join.as_ref().unwrap();
    assert_eq!(
        (
            join.wakeups_started,
            join.wakeup_attempts,
            join.reconciled_epoch
        ),
        (0, 1, 1)
    );
}
#[test]
fn 外部未知轮及合并旧轮不授予自有权() {
    for turn in ["外部轮", "原轮"] {
        let (mut m, token) = fixture(1);
        let (id, request) = start(&mut m, &token);
        m.subagent_reconcile_reply(&request, Ok(json!({"turn":{"id":turn}})), &mut vec![]);
        carrier(&mut m, &token, id, turn);
        assert_eq!(m.runs.runs[R].current_turn_id.as_deref(), Some("原轮"));
        assert!(m.runs.runs[R].turns.is_empty());
        let j = m.runs.runs[R].subagent_join.as_ref().unwrap();
        assert!(j.automatic_control_revoked);
        assert_eq!(j.delivered_epoch, 0);
        assert_eq!(j.terminal_reason, Some(TerminalReason::OwnershipConflict));
    }
}
#[test]
fn 取消未发与已发未知分开且迟到不复活() {
    for reason in [
        ReconcileCancel::Stop,
        ReconcileCancel::OwnerGone,
        ReconcileCancel::Shutdown,
        ReconcileCancel::Abandon,
        ReconcileCancel::UserInput,
        ReconcileCancel::GoalInactive,
        ReconcileCancel::Deadline,
        ReconcileCancel::AuthorityChanged,
    ] {
        for sent in [false, true] {
            let (mut m, token) = fixture(1);
            let id = if sent {
                start(&mut m, &token).0
            } else {
                m.begin_subagent_reconcile(token.clone(), batch(&m, "内容"), &mut vec![])
                    .unwrap()
            };
            let deadline = m.runs.reconciliation.operations[&id.0]
                .operation
                .verify_deadline_mono_ms;
            m.cancel_subagent_reconcile(&token, reason, &mut vec![]);
            let mut actions = vec![];
            m.drive_subagent_reconcile(&mut actions);
            assert_eq!(submissions(&actions), 0);
            assert_eq!(m.reconcile_unresolved(R), sent);
            assert_eq!(
                m.runs.reconciliation.operations[&id.0]
                    .operation
                    .verify_deadline_mono_ms,
                deadline
            );
            assert!(!m.reconcile_parent_started(
                &token,
                "迟到轮",
                TurnOrigin::SubagentReconcile,
                &mut vec![]
            ));
        }
    }
}

mod child_reads;

#[test]
fn 第五次汇总允许完成但新工作不产生第六轮() {
    let (mut m, token) = fixture(1);
    m.runs
        .runs
        .get_mut(R)
        .unwrap()
        .subagent_join
        .as_mut()
        .unwrap()
        .wakeups_started = 4;
    let (id, _) = start(&mut m, &token);
    own_start(&mut m, &token, "第五轮", TurnOrigin::SubagentReconcile);
    carrier(&mut m, &token, id, "第五轮");
    completed(&mut m, "第五轮", "completed", "完成");
    assert_eq!(
        m.runs.runs[R]
            .subagent_join
            .as_ref()
            .unwrap()
            .reconciled_epoch,
        1
    );
    assert!(
        !m.runs.runs[R]
            .subagent_join
            .as_ref()
            .unwrap()
            .automatic_control_revoked
    );
    let mut no_more = m.clone();
    no_more.run_released(R, ReleaseKind::Natural, &mut vec![]);
    assert_eq!(
        no_more.runs.runs[R].status,
        Some(crate::runs::RunStatus::Completed)
    );
    let facts = m
        .runs
        .reconciliation
        .harness
        .as_mut()
        .unwrap()
        .facts
        .get_mut(R)
        .unwrap();
    facts.work_epoch = 2;
    facts.discovery_covered_epoch = Some(2);
    m.runs
        .runs
        .get_mut(R)
        .unwrap()
        .subagent_join
        .as_mut()
        .unwrap()
        .work_epoch = 2;
    assert_eq!(
        m.begin_subagent_reconcile(token, batch(&m, "又有结果"), &mut vec![]),
        Err(ReconcileRefusal::WakeLimit)
    );
    assert_eq!(
        m.runs.runs[R]
            .subagent_join
            .as_ref()
            .unwrap()
            .wakeup_attempts,
        1
    );
}
#[test]
fn 准入保守检查模型能力预算版本归属和自动轮额度() {
    for case in 0..12 {
        let (mut m, token) = fixture(1);
        let b = batch(&m, "结果");
        let expected = match case {
            0 => {
                m.runs.reconciliation.harness.as_mut().unwrap().budget = BudgetAdmission::Exhausted;
                ReconcileRefusal::BudgetExhausted
            }
            1 => {
                m.runs.reconciliation.harness.as_mut().unwrap().budget =
                    BudgetAdmission::NeedsVerification;
                ReconcileRefusal::BudgetIncomplete
            }
            2 => {
                m.relations
                    .edges
                    .get_mut(T)
                    .unwrap()
                    .can_accept_direct_input = None;
                ReconcileRefusal::CapabilityUnknown
            }
            3 => {
                m.relations
                    .edges
                    .get_mut(T)
                    .unwrap()
                    .can_accept_direct_input = Some(false);
                ReconcileRefusal::RootNoDirectInput
            }
            4 => {
                m.runs.runs.get_mut(R).unwrap().execution_config = None;
                ReconcileRefusal::InvalidBatch
            }
            5 => {
                m.runs.runs.get_mut(R).unwrap().auto_turns_started = Some(50);
                ReconcileRefusal::AutoTurnLimit
            }
            6 => {
                m.runs
                    .reconciliation
                    .harness
                    .as_mut()
                    .unwrap()
                    .facts
                    .get_mut(R)
                    .unwrap()
                    .work_epoch += 1;
                ReconcileRefusal::WorkChanged
            }
            7 => {
                m.runs
                    .reconciliation
                    .harness
                    .as_mut()
                    .unwrap()
                    .facts
                    .get_mut(R)
                    .unwrap()
                    .discovery_covered_epoch = None;
                ReconcileRefusal::DiscoveryIncomplete
            }
            8 => {
                m.runs
                    .reconciliation
                    .harness
                    .as_mut()
                    .unwrap()
                    .facts
                    .get_mut(R)
                    .unwrap()
                    .pending_requests = 1;
                ReconcileRefusal::DiscoveryIncomplete
            }
            9 => {
                m.runs.runs.get_mut(R).unwrap().start_confirmation.owned = Some(false);
                ReconcileRefusal::OwnershipConflict
            }
            10 => {
                m.convergences.runs.get_mut(R).unwrap().deadline_mono_ms = m.clock.mono_ms;
                ReconcileRefusal::Deadline
            }
            _ => {
                m.records.entry(T.into()).or_default().goal_status = Some("paused".into());
                ReconcileRefusal::GoalInactive
            }
        };
        let mut a = vec![];
        assert_eq!(
            m.begin_subagent_reconcile(token, b, &mut a),
            Err(expected),
            "情况{case}"
        );
        assert_eq!(submissions(&a), 0);
    }
}
#[test]
fn 同批重复启动与重复核实不增加计数() {
    let (mut m, token) = fixture(1);
    let (id, _) = start(&mut m, &token);
    assert_eq!(
        m.begin_subagent_reconcile(token.clone(), batch(&m, "结果"), &mut vec![]),
        Err(ReconcileRefusal::AlreadyInFlight)
    );
    own_start(&mut m, &token, "汇总轮", TurnOrigin::SubagentReconcile);
    carrier(&mut m, &token, id, "汇总轮");
    m.reconcile_turn_confirmed(
        &token,
        id,
        "汇总轮",
        TurnOrigin::SubagentReconcile,
        &mut vec![],
    );
    assert_eq!(
        m.begin_subagent_reconcile(token, batch(&m, "结果"), &mut vec![]),
        Err(ReconcileRefusal::AlreadyDelivered)
    );
    assert_eq!(m.runs.runs[R].auto_turns_started, Some(1));
}
#[test]
fn 失败和中断父轮不推进已汇总水位() {
    for status in ["failed", "interrupted"] {
        let (mut m, token) = fixture(1);
        let (id, _) = start(&mut m, &token);
        own_start(&mut m, &token, "汇总轮", TurnOrigin::SubagentReconcile);
        carrier(&mut m, &token, id, "汇总轮");
        completed(&mut m, "汇总轮", status, "失败结果");
        let j = m.runs.runs[R].subagent_join.as_ref().unwrap();
        assert_eq!(j.delivered_epoch, 1);
        assert_eq!(j.reconciled_epoch, 0);
    }
}
#[test]
fn 未登记可信输入不能发送且等待期间真实输入能取消() {
    for cancel in [false, true] {
        let (mut m, token) = fixture(1);
        let id = m
            .begin_subagent_reconcile(token.clone(), batch(&m, "结果"), &mut vec![])
            .unwrap();
        if cancel {
            m.cancel_subagent_reconcile(&token, ReconcileCancel::UserInput, &mut vec![]);
        } else {
            m.runs.reconciliation.harness.as_mut().unwrap().trust = false;
        }
        let mut a = vec![];
        m.drive_subagent_reconcile(&mut a);
        assert_eq!(submissions(&a), 0);
        assert_eq!(
            m.runs.reconciliation.operations[&id.0].operation.submission,
            ReconcileSubmission::CancelledBeforeSubmit
        );
        assert_eq!(
            m.runs.runs[R]
                .subagent_join
                .as_ref()
                .unwrap()
                .wakeup_attempts,
            0
        );
    }
}
#[test]
fn 旧代号回调不能给新授权推进覆盖或撤权() {
    let (mut m, token) = fixture(1);
    let (id, _) = start(&mut m, &token);
    m.convergences.runs.get_mut(R).unwrap().token.generation += 1;
    carrier(&mut m, &token, id, "未知承载");
    assert_eq!(
        m.runs.runs[R]
            .subagent_join
            .as_ref()
            .unwrap()
            .delivered_epoch,
        0
    );
    m.cancel_subagent_reconcile(&token, ReconcileCancel::AuthorityChanged, &mut vec![]);
    assert!(!m.convergences.runs[R].automatic_control_revoked);
    assert!(
        !m.runs.runs[R]
            .subagent_join
            .as_ref()
            .unwrap()
            .automatic_control_revoked
    );
}

#[test]
fn 总窗后迟到成功只补原操作信息且明确拒绝可结算但不重发() {
    for success in [true, false] {
        let (mut m, token) = fixture(1);
        let (id, request) = start(&mut m, &token);
        m.clock.mono_ms += 20_000;
        m.drive_subagent_reconcile(&mut vec![]);
        let before = m.runs.runs[R].clone();
        let mut a = vec![];
        let reply = if success {
            Ok(json!({"turn":{"id":"迟到承载候选"}}))
        } else {
            Err(json!({"code":-32602}))
        };
        m.subagent_reconcile_reply(&request, reply, &mut a);
        m.drive_subagent_reconcile(&mut a);
        assert_eq!(m.reconcile_unresolved(R), success);
        if success {
            assert_eq!(
                m.runs.reconciliation.operations[&id.0]
                    .reply_turn
                    .as_deref(),
                Some("迟到承载候选")
            );
        }
        assert_eq!(submissions(&a), 0);
        assert_eq!(m.runs.runs[R], before);
    }
}
#[test]
fn 重复提交回包不能覆盖首次响应的承载候选() {
    let (mut m, token) = fixture(1);
    let (id, request) = start(&mut m, &token);
    m.subagent_reconcile_reply(&request, Ok(json!({"turn":{"id":"新轮"}})), &mut vec![]);
    m.subagent_reconcile_reply(&request, Ok(json!({"turn":{"id":"伪重复"}})), &mut vec![]);
    assert_eq!(
        m.runs.reconciliation.operations[&id.0]
            .reply_turn
            .as_deref(),
        Some("新轮")
    );
    own_start(&mut m, &token, "新轮", TurnOrigin::SubagentReconcile);
    carrier(&mut m, &token, id, "新轮");
    assert_eq!(
        m.runs.runs[R]
            .subagent_join
            .as_ref()
            .unwrap()
            .delivered_epoch,
        1
    );
}

#[test]
fn 原始started加client_id及前轮证明才授予新父轮() {
    for shared in [false, true] {
        let (mut m, token) = fixture(1);
        m.shared = shared;
        let (id, _) = start(&mut m, &token);
        m.reconcile_observe_parent_started(&token, "候选新轮");
        m.reconcile_observe_parent_started(&token, "原轮");
        assert!(!m.claims[T].owned_turns.contains("候选新轮"));
        assert_eq!(m.runs.runs[R].current_turn_id.as_deref(), Some("原轮"));
        carrier(&mut m, &token, id, "候选新轮");
        assert!(m.claims[T].owned_turns.contains("候选新轮"));
        assert_eq!(m.runs.runs[R].current_turn_id.as_deref(), Some("候选新轮"));
        assert_eq!(
            m.runs.runs[R]
                .subagent_join
                .as_ref()
                .unwrap()
                .delivered_epoch,
            1
        );
    }
}
#[test]
fn 竞争开始与真实输入都阻止候选轮获得自动归属() {
    for user_input in [false, true] {
        let (mut m, token) = fixture(1);
        let (id, _) = start(&mut m, &token);
        m.reconcile_observe_parent_started(&token, "候选新轮");
        if user_input {
            m.cancel_subagent_reconcile(&token, ReconcileCancel::UserInput, &mut vec![]);
        } else {
            m.reconcile_observe_parent_started(&token, "竞争轮");
        }
        carrier(&mut m, &token, id, "候选新轮");
        assert!(!m.claims[T].owned_turns.contains("候选新轮"));
        assert_eq!(m.runs.runs[R].current_turn_id.as_deref(), Some("原轮"));
        assert_eq!(
            m.runs.runs[R]
                .subagent_join
                .as_ref()
                .unwrap()
                .delivered_epoch,
            0
        );
    }
}

#[test]
fn 承载核实页里的真实用户输入先于归属定案否决() {
    use crate::test_support::status_read::{items_page, turns_page};
    for real in [false, true] {
        let (mut m, token) = fixture(1);
        let (id, request) = start(&mut m, &token);
        m.reconcile_observe_parent_started(&token, "新轮");
        let read_id = m.runs.reconciliation.operations[&id.0].status_read.unwrap();
        let ctx = m.status_request_context(read_id).unwrap();
        let Operation::StatusRead { generation, .. } = ctx.operation else {
            unreachable!()
        };
        m.status_read_reply(
            read_id,
            generation,
            Ok(turns_page(
                &[("新轮", "inProgress"), ("原轮", "completed")],
                None,
            )),
            &mut vec![],
        );
        let mut items = vec![
            json!({"type":"userMessage","clientId":request.params.unwrap()["clientUserMessageId"],"content":[]}),
        ];
        if real {
            items.push(json!({"type":"userMessage","clientId":"外部输入","content":[]}));
        }
        let ctx = m.status_request_context(read_id).unwrap();
        let Operation::StatusRead { generation, .. } = ctx.operation else {
            unreachable!()
        };
        m.status_read_reply(
            read_id,
            generation,
            Ok(items_page("新轮", &items, None)),
            &mut vec![],
        );
        assert_eq!(m.claims[T].owned_turns.contains("新轮"), !real);
        assert_eq!(
            m.runs.runs[R]
                .subagent_join
                .as_ref()
                .unwrap()
                .delivered_epoch,
            u64::from(!real)
        );
    }
}

#[test]
fn 核实回看前轮历史用户输入不误撤销当前操作() {
    use crate::test_support::status_read::{items_page, turns_page};
    let (mut m, token) = fixture(1);
    let (id, _) = start(&mut m, &token);
    let read_id = m.runs.reconciliation.operations[&id.0].status_read.unwrap();
    let ctx = m.status_request_context(read_id).unwrap();
    let Operation::StatusRead { generation, .. } = ctx.operation else {
        unreachable!()
    };
    m.status_read_reply(
        read_id,
        generation,
        Ok(turns_page(&[("原轮", "completed")], None)),
        &mut vec![],
    );
    let ctx = m.status_request_context(read_id).unwrap();
    let Operation::StatusRead { generation, .. } = ctx.operation else {
        unreachable!()
    };
    m.status_read_reply(
        read_id,
        generation,
        Ok(items_page(
            "原轮",
            &[json!({"type":"userMessage","clientId":"原用户输入","content":[]})],
            None,
        )),
        &mut vec![],
    );
    assert!(m.reconcile_unresolved(R));
    assert!(
        !m.runs.runs[R]
            .subagent_join
            .as_ref()
            .unwrap()
            .automatic_control_revoked
    );
}

#[test]
fn 授权换代后的已承载旧批次不可重发() {
    let (mut m, token) = fixture(1);
    let (id, _) = start(&mut m, &token);
    own_start(&mut m, &token, "已自有轮", TurnOrigin::SubagentReconcile);
    m.convergences.runs.get_mut(R).unwrap().token.generation += 1;
    let current = m.convergences.runs[R].token.clone();
    m.runs
        .reconciliation
        .harness
        .as_mut()
        .unwrap()
        .facts
        .get_mut(R)
        .unwrap()
        .token = current.clone();
    carrier(&mut m, &token, id, "已自有轮");
    assert_eq!(
        m.runs.runs[R]
            .subagent_join
            .as_ref()
            .unwrap()
            .delivered_epoch,
        0
    );
    assert_eq!(
        m.begin_subagent_reconcile(current, batch(&m, "相同批次"), &mut vec![]),
        Err(ReconcileRefusal::AlreadyDelivered)
    );
    assert_eq!(
        m.runs.runs[R]
            .subagent_join
            .as_ref()
            .unwrap()
            .wakeup_attempts,
        1
    );
}

#[test]
fn 已用启动重试额度跨结果批次保留() {
    fn reject_and_drive(m: &mut Machine, request: &RequestContext) -> Option<RequestContext> {
        m.subagent_reconcile_reply(request, Err(json!({"code":-32602})), &mut vec![]);
        let mut a = vec![];
        m.drive_subagent_reconcile(&mut a);
        a.into_iter().find_map(|a| match a {
            Action::Request(r) if matches!(r.operation, Operation::StartReconciliation { .. }) => {
                Some(r)
            }
            _ => None,
        })
    }
    let (mut m, token) = fixture(1);
    let (_, first) = start(&mut m, &token);
    let retry = reject_and_drive(&mut m, &first).unwrap();
    let Operation::StartReconciliation { op_id, .. } = retry.operation else {
        unreachable!()
    };
    own_start(&mut m, &token, "第一批汇总", TurnOrigin::SubagentReconcile);
    carrier(&mut m, &token, op_id, "第一批汇总");
    completed(&mut m, "第一批汇总", "completed", "第一批完成");
    let facts = m
        .runs
        .reconciliation
        .harness
        .as_mut()
        .unwrap()
        .facts
        .get_mut(R)
        .unwrap();
    facts.work_epoch = 2;
    facts.discovery_covered_epoch = Some(2);
    facts.activities[0].key.activity_seq = 2;
    facts.result_refs[0].key.activity_seq = 2;
    facts.activities[0].key.turn_id = Some("复用子轮".into());
    facts.result_refs[0].key.turn_id = Some("复用子轮".into());
    facts.result_refs[0].locator.turn_id = Some("复用子轮".into());
    m.runs
        .runs
        .get_mut(R)
        .unwrap()
        .subagent_join
        .as_mut()
        .unwrap()
        .work_epoch = 2;
    let (_, second) = start(&mut m, &token);
    let last = reject_and_drive(&mut m, &second).unwrap();
    assert!(reject_and_drive(&mut m, &last).is_none());
    let j = m.runs.runs[R].subagent_join.as_ref().unwrap();
    assert_eq!(j.start_retries_used, 2);
    assert_eq!(j.wakeup_attempts, 4);
    assert_eq!(j.wakeups_started, 1);
    assert_eq!(j.terminal_reason, Some(TerminalReason::WakeFailed));
}

#[test]
fn 交接后用新token重查同一输入且原提交和总窗不变() {
    for finish in [false, true] {
        let (mut m, old) = fixture(1);
        let (id, request) = start(&mut m, &old);
        m.reconcile_observe_parent_started(&old, "交接中的新轮");
        let original = m.reconcile_operation(id).unwrap().clone();
        let old_read = m.runs.reconciliation.operations[&id.0].status_read.unwrap();
        m.clock.mono_ms += 5_000;
        m.relations.contexts.get_mut(R).unwrap().gen += 1;
        let context = m.convergences.runs.get_mut(R).unwrap();
        context.token.manage_gen += 1;
        context.token.generation += 1;
        let current = context.token.clone();
        m.runs
            .reconciliation
            .harness
            .as_mut()
            .unwrap()
            .facts
            .get_mut(R)
            .unwrap()
            .token = current.clone();
        let mut a = vec![];
        assert_eq!(m.reverify_subagent_reconcile(&current, id, &mut a), Ok(()));
        assert_eq!(submissions(&a), 0);
        let p = &m.runs.reconciliation.operations[&id.0];
        assert_eq!(p.operation.token, original.token);
        assert_eq!(
            p.operation.client_user_message_id,
            original.client_user_message_id
        );
        assert_eq!(
            p.operation.verify_deadline_mono_ms,
            original.verify_deadline_mono_ms
        );
        assert_ne!(p.status_read, Some(old_read));
        assert!(a.iter().any(|a| matches!(a, Action::Schedule { kind: crate::claim::TimerKind::ReconcileVerifyDeadline { .. }, delay_ms, .. } if *delay_ms == 15_000)));
        m.subagent_reconcile_status_done(
            StatusDone {
                read_id: old_read,
                thread: T.into(),
                token: Some(old.claim.clone()),
                caller: StatusCaller::SubagentReconcile {
                    token: old.clone(),
                    op_id: id,
                },
                result: StatusResult::Carrier("交接中的新轮".into()),
            },
            &mut a,
        );
        assert!(!m.claims[T].owned_turns.contains("交接中的新轮"));
        m.subagent_reconcile_reply(&request, Ok(json!({"turn":{"id":"交接中的新轮"}})), &mut a);
        if finish {
            carrier(&mut m, &current, id, "交接中的新轮");
            assert!(m.claims[T].owned_turns.contains("交接中的新轮"));
            assert_eq!(
                m.runs.runs[R]
                    .subagent_join
                    .as_ref()
                    .unwrap()
                    .delivered_epoch,
                1
            );
            assert_eq!(
                m.runs.runs[R]
                    .subagent_join
                    .as_ref()
                    .unwrap()
                    .wakeup_attempts,
                1
            );
        } else {
            m.clock.mono_ms = original.verify_deadline_mono_ms.unwrap();
            m.drive_subagent_reconcile(&mut a);
            assert_eq!(
                m.runs.runs[R]
                    .subagent_join
                    .as_ref()
                    .unwrap()
                    .terminal_reason,
                Some(TerminalReason::WakeUnknown)
            );
            assert_eq!(
                m.reverify_subagent_reconcile(&current, id, &mut a),
                Err(ReconcileRefusal::StaleToken)
            );
        }
        assert_eq!(submissions(&a), 0);
    }
}

#[test]
fn 集成_真实开始后空条目页只等待输入落盘不判外部轮() {
    let (mut m, token) = fixture(1);
    let (id, request) = start(&mut m, &token);
    m.subagent_reconcile_reply(&request, Ok(json!({"turn":{"id":"快轮"}})), &mut vec![]);
    m.reconcile_observe_parent_started(&token, "快轮");
    let read_id = m.runs.reconciliation.operations[&id.0].status_read.unwrap();
    m.subagent_reconcile_status_done(
        StatusDone {
            read_id,
            thread: T.into(),
            token: Some(token.claim.clone()),
            caller: StatusCaller::SubagentReconcile {
                token: token.clone(),
                op_id: id,
            },
            result: StatusResult::NoCarrier,
        },
        &mut vec![],
    );
    assert!(m.reconcile_operation(id).unwrap().outcome_unknown);
    assert!(!m.runs.reconciliation.operations[&id.0]
        .known_turns
        .contains("快轮"));
    m.clock.mono_ms += 500;
    let mut actions = vec![];
    m.drive_subagent_reconcile(&mut actions);
    carrier(&mut m, &token, id, "快轮");
    assert_eq!(
        m.reconcile_operation(id).unwrap().submission,
        ReconcileSubmission::ConfirmedOwnTurn
    );
    assert_eq!(submissions(&actions), 0);
}

#[test]
fn 二审_r3_核实截止已到但到期事件未处理时先detach立即wakeunknown() {
    for late_by in [0, 1] {
        let (mut m, old) = fixture(1);
        let (id, request) = start(&mut m, &old);
        let original = m.runs.reconciliation.operations[&id.0].clone();
        let old_read = original.status_read.expect("原核实读取在途");
        m.enable_convergence(crate::claim::converge::ConvergenceReadiness {
            trusted_inputs: true,
            budget_admission: true,
            asynchronous_results: true,
            terminal_mapping: true,
            cancellation_verified: true,
        });
        m.clock.mono_ms = original.operation.verify_deadline_mono_ms.unwrap() + late_by;
        // 只推进时钟，不投递到期事件。按实际 detach 顺序先交接管理权，再进入 G 路调用方。
        m.claims.get_mut(T).unwrap().state = State::AwaitingContinuation {
            owner: Owner::Broker {
                policy: crate::policy::Policy::Never,
                approvals_reviewer: None,
                owned_at: m.clock.now_ms,
                last_watcher_at: m.clock.now_ms,
            },
        };
        let mut actions = vec![];
        m.on_handoff(R, crate::relations::HandoffKind::Detach, &mut actions);
        m.convergence_event(
            T,
            crate::claim::converge::ConvergeEvent::Detach,
            &mut actions,
        );
        let current = m.convergences.runs[R].token.clone();
        assert_ne!(current, old);
        let pending = &m.runs.reconciliation.operations[&id.0];
        assert_eq!(pending.verify_token, current);
        assert!(pending.expired);
        assert!(pending.status_read.is_none());
        assert!(pending.operation.outcome_unknown, "截止不能伪造明确未提交");
        assert_eq!(
            pending.operation.submission,
            ReconcileSubmission::OutcomeUnknown
        );
        assert!(pending.operation.automatic_control_revoked);
        assert_eq!(pending.operation.token, original.operation.token);
        assert_eq!(
            pending.operation.client_user_message_id,
            original.operation.client_user_message_id
        );
        assert_eq!(
            pending.operation.verify_deadline_mono_ms,
            original.operation.verify_deadline_mono_ms
        );
        assert_eq!(pending.batch, original.batch);
        assert_eq!(
            m.runs.runs[R]
                .subagent_join
                .as_ref()
                .unwrap()
                .terminal_reason,
            Some(TerminalReason::WakeUnknown)
        );
        assert!(m.reconcile_unresolved(R));
        assert!(m.tree_for_run(R).is_some(), "不等总期限，立即进入停树收尾");
        assert_eq!(submissions(&actions), 0);
        // 旧代读取、启动回包及后续驱动都不能复活自动权或重发。
        m.subagent_reconcile_status_done(
            StatusDone {
                read_id: old_read,
                thread: T.into(),
                token: Some(old.claim.clone()),
                caller: StatusCaller::SubagentReconcile {
                    token: old.clone(),
                    op_id: id,
                },
                result: StatusResult::Carrier("迟到轮".into()),
            },
            &mut actions,
        );
        m.subagent_reconcile_reply(&request, Ok(json!({"turn":{"id":"迟到轮"}})), &mut actions);
        m.drive_subagent_reconcile(&mut actions);
        assert!(!m.claims[T].owned_turns.contains("迟到轮"));
        assert_eq!(submissions(&actions), 0);
        assert_eq!(
            m.runs.runs[R]
                .subagent_join
                .as_ref()
                .unwrap()
                .terminal_reason,
            Some(TerminalReason::WakeUnknown)
        );
    }
}
