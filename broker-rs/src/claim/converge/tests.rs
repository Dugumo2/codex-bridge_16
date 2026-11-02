//! A 路组件测试使用真实 step；B/C 只在冻结边界提供确定性桩。
use super::*;
use crate::claim::{step, ClientRequest, Event};
use crate::relations::*;
use crate::runs::{RunState, RunStatus};
use crate::test_support::subagent::SubagentMock;
use serde_json::json;

const P: &str = "父";
const T: &str = "父轮";
const R: &str = "run-convergence";

mod lineage;

fn ready() -> ConvergenceReadiness {
    ConvergenceReadiness {
        trusted_inputs: true,
        budget_admission: true,
        asynchronous_results: true,
        terminal_mapping: true,
        cancellation_verified: true,
    }
}
fn apply(m: &mut Machine, event: Event) -> Vec<Action> {
    let (next, out) = step(std::mem::take(m), event);
    *m = next;
    out
}
fn notify(m: &mut Machine, method: &str, params: serde_json::Value) -> Vec<Action> {
    apply(
        m,
        Event::Notification {
            method: method.into(),
            params: Some(params),
        },
    )
}
fn setup_pending() -> (Machine, SubagentMock, crate::claim::RequestContext) {
    setup_pending_mode(false)
}
fn setup_pending_mode(shared: bool) -> (Machine, SubagentMock, crate::claim::RequestContext) {
    let mut m = Machine {
        shared,
        ..Machine::default()
    };
    m.clock.mono_ms = 1_000;
    m.clock.now_ms = 100_000;
    // 与生产 Core 相同：broker 启动即打开门禁，运行按需启用（不再绕过启用前的初始化路径）。
    assert!(m.enable_convergence(ready()));
    apply(&mut m, Event::Connected { conn: 1 });
    apply(&mut m, Event::Initialized { conn: 1, cxb: true });
    m.record(P).subscribed = true;
    let out = apply(
        &mut m,
        Event::ClientRequest {
            request: ClientRequest {
                conn: 1,
                id: json!(1),
                method: "turn/start".into(),
                params: Some(json!({"threadId":P,"input":[{"type":"text","text":"任务"}]})),
            },
            run_id: Some(R.into()),
        },
    );
    let context = out
        .into_iter()
        .find_map(|a| match a {
            Action::Request(c) if c.method == "turn/start" => Some(c),
            _ => None,
        })
        .unwrap();
    m.convergences
        .capabilities
        .insert(P.into(), (m.relations.conn_epoch, true));
    m.convergences.test_budget = Some(BudgetAdmission::Allowed);
    m.convergences.test_read = Some(BatchReadId(9));
    let mut mock = SubagentMock::default();
    mock.register(P, None, "v1", false, true);
    (m, mock, context)
}
fn setup() -> (Machine, SubagentMock) {
    let (mut m, mock, context) = setup_pending();
    apply(
        &mut m,
        Event::Reply {
            context,
            result: Ok(json!({"turn":{"id":T}})),
        },
    );
    notify(
        &mut m,
        "turn/started",
        json!({"threadId":P,"turn":{"id":T,"status":"inProgress","items":[]}}),
    );
    (m, mock)
}
/// 已纳管一个先完成的子代理：只有确有子代理的运行才进入收敛（用户 2026-10-11 定）。
fn setup_with_child() -> (Machine, SubagentMock) {
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, "C0", "spawn-c0", "c0-t1");
    complete(&mut m, &mut mock, "C0", "c0-t1", "completed");
    (m, mock)
}
fn spawn(
    m: &mut Machine,
    mock: &mut SubagentMock,
    parent: &str,
    parent_turn: &str,
    child: &str,
    item: &str,
    turn: &str,
) {
    mock.register(child, Some(parent), "v1", false, true);
    let (a, b) = SubagentMock::v1_spawn(parent, parent_turn, item, child, false);
    apply(m, a);
    apply(m, b);
    apply(m, mock.turn_started(child, turn));
    let out = m.relations.aux.lookups.keys().cloned().collect::<Vec<_>>();
    let _ = out;
    // 明确的 goal 空值是事实；默认 ThreadRecord 不是该证据。
    notify(m, "thread/goal/cleared", json!({"threadId":child}));
}
fn complete(
    m: &mut Machine,
    mock: &mut SubagentMock,
    thread: &str,
    turn: &str,
    status: &str,
) -> Vec<Action> {
    apply(m, mock.turn_completed(thread, turn, status))
}
fn discover(m: &mut Machine, complete: bool) -> Vec<Action> {
    let runtime = m.convergences.runs[R].clone();
    let (op, epoch) = m.convergences.progress[R].discovery.expect("有在途发现");
    let generation = m.relations.discoveries[&op].generation;
    m.relations.discoveries.remove(&op);
    m.relations.aux.work.remove(&op);
    let result = DiscoveryResult {
        root: P.into(),
        scope: DiscoveryScope::Run { run_id: R.into() },
        generation,
        members: m.relations.edges.clone(),
        state: DiscoveryState {
            complete,
            version: m.relations.aux.version,
            missing: Default::default(),
            incomplete_reasons: if complete {
                vec![]
            } else {
                vec!["listError".into()]
            },
        },
        attachments: Default::default(),
        cancelled: false,
    };
    let mut out = vec![];
    m.convergence_discovery_done(runtime.token, epoch, result, &mut out);
    out
}
fn retry(m: &mut Machine) -> Vec<Action> {
    let runtime = m.convergences.runs[R].clone();
    m.clock.mono_ms = m.convergences.progress[R].retry_at.expect("重试截止");
    let mut out = vec![];
    m.convergence_timer(
        TimerKind::ConvergeRetry {
            token: runtime.token,
            attempt: runtime.discovery_retry,
        },
        &mut out,
    );
    out
}
fn join(m: &Machine) -> &SubagentJoin {
    m.runs.runs[R].subagent_join.as_ref().unwrap()
}
fn operation(m: &Machine, submission: ReconcileSubmission) -> ReconcileOperation {
    ReconcileOperation {
        token: m.convergences.runs[R].token.clone(),
        work_epoch: join(m).work_epoch,
        op_id: ReconcileOpId(7),
        client_user_message_id: "唯一输入".into(),
        expected_previous_turn_id: Some(T.into()),
        target_turn_id: Some("汇总轮".into()),
        submission,
        outcome_unknown: submission == ReconcileSubmission::OutcomeUnknown,
        automatic_control_revoked: false,
        verify_deadline_mono_ms: Some(m.clock.mono_ms + 20_000),
    }
}

#[test]
fn 默认关闭且启用必须确认全部组件() {
    let mut m = Machine::default();
    for missing in 0..5 {
        let mut r = ready();
        match missing {
            0 => r.trusted_inputs = false,
            1 => r.budget_admission = false,
            2 => r.asynchronous_results = false,
            3 => r.terminal_mapping = false,
            _ => r.cancellation_verified = false,
        }
        assert!(!m.enable_convergence(r));
        assert!(!m.convergences.enabled);
    }
    assert_eq!(m.convergence_release_gate(P, None, None, &mut vec![]), None);
}
#[test]
fn 缺管理上下文不是空树成功() {
    let m = Machine::default();
    assert_eq!(m.has_managed_activity(R), Activity::Unknown);
    assert!(m.convergence_facts(R).is_none());
}
#[test]
fn 父不等部分等子先完成均保持同运行直到汇总() {
    for completed in 0..=2 {
        let (mut m, mut mock) = setup();
        spawn(&mut m, &mut mock, P, T, "子甲", "派生甲", "子轮甲");
        spawn(&mut m, &mut mock, P, T, "子乙", "派生乙", "子轮乙");
        for (i, (c, t)) in [("子甲", "子轮甲"), ("子乙", "子轮乙")].iter().enumerate() {
            if i < completed {
                complete(&mut m, &mut mock, c, t, "completed");
            }
        }
        complete(&mut m, &mut mock, P, T, "completed");
        assert!(matches!(m.claims[P].state, State::Converging { .. }));
        assert_eq!(m.runs.runs[R].state, RunState::Running);
        assert!(m.tree_for_run(R).is_none());
        discover(&mut m, true);
        assert!(m.claims.contains_key(P));
        assert_eq!(join(&m).reconciled_epoch, 0);
        if completed == 2 {
            assert_eq!(
                m.convergences.progress[R].reading,
                Some((BatchReadId(9), join(&m).work_epoch))
            );
        }
    }
}
#[test]
fn 孙代理未完成不能被已完成直接子抵消() {
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, "子", "派生", "子轮");
    spawn(&mut m, &mut mock, "子", "子轮", "孙", "孙派生", "孙轮");
    complete(&mut m, &mut mock, "子", "子轮", "completed");
    complete(&mut m, &mut mock, P, T, "completed");
    discover(&mut m, true);
    let facts = m.convergence_facts(R).unwrap();
    assert!(facts
        .activities
        .iter()
        .any(|a| a.key.thread_id == "孙" && a.activity == MemberActivity::Running));
    assert!(!crate::relations::convergence::settled(&facts));
}
#[test]
fn 重复事件与请求计数不制造新批次() {
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, "子", "派生", "子轮");
    complete(&mut m, &mut mock, "子", "子轮", "completed");
    complete(&mut m, &mut mock, P, T, "completed");
    let epoch = join(&m).work_epoch;
    complete(&mut m, &mut mock, "子", "子轮", "completed");
    m.relations
        .contexts
        .get_mut(R)
        .unwrap()
        .members
        .get_mut("子")
        .unwrap()
        .pending_requests = 3;
    m.touch_context(R, MemberChange::Request, &mut vec![]);
    assert_eq!(m.convergence_facts(R).unwrap().work_epoch, epoch);
    let revision = m.relations.contexts[R].revision;
    m.convergence_phase(R, JoinPhase::FinalCheck, &mut vec![]);
    assert!(m.relations.contexts[R].revision > revision);
    assert_eq!(join(&m).work_epoch, epoch);
}
#[test]
fn 首轮定位后补不改变活动身份_线程复用新代际() {
    let (mut m, mut mock) = setup();
    mock.register("子", Some(P), "v1", false, true);
    let (a, b) = SubagentMock::v1_spawn(P, T, "派生", "子", false);
    apply(&mut m, a);
    apply(&mut m, b);
    let seq = m.convergences.ledgers[R].activities[0].key.activity_seq;
    apply(&mut m, mock.turn_started("子", "第一轮"));
    assert_eq!(
        m.convergences.ledgers[R].activities[0].key.activity_seq,
        seq
    );
    complete(&mut m, &mut mock, "子", "第一轮", "completed");
    apply(&mut m, mock.turn_started("子", "第二轮"));
    let ledger = &m.convergences.ledgers[R];
    assert_eq!(ledger.activities.len(), 2);
    assert!(ledger.activities[1].key.activity_seq > seq);
    assert_eq!(ledger.activities[1].activity, MemberActivity::Running);
    assert!(!ledger.activities[1].verified, "新轮未授权不能借用旧绑定");
}
#[test]
fn 状态归一完整且未知不等于缺输出() {
    use crate::relations::convergence::normalize_activity as normalize;
    for (status, result) in [
        ("completed", ActivityResultStatus::Completed),
        ("failed", ActivityResultStatus::Failed),
        ("errored", ActivityResultStatus::Failed),
        ("interrupted", ActivityResultStatus::Interrupted),
    ] {
        assert_eq!(
            normalize(status, false),
            (MemberActivity::Settled, Some(result))
        );
    }
    assert_eq!(
        normalize("shutdown", true),
        (
            MemberActivity::Settled,
            Some(ActivityResultStatus::Shutdown)
        )
    );
    for status in ["shutdown", "notFound", "notLoaded", ""] {
        assert_eq!(normalize(status, false), (MemberActivity::Unknown, None));
    }
    assert_eq!(normalize("pendingInit", false).0, MemberActivity::Starting);
    assert_eq!(normalize("running", false).0, MemberActivity::Running);
}
#[test]
fn pending_init父结束或证据过期仍阻塞() {
    let (mut m, mut mock) = setup_with_child();
    let (a, _) = SubagentMock::v1_spawn(P, T, "无首轮", "子", false);
    apply(&mut m, a);
    complete(&mut m, &mut mock, P, T, "completed");
    assert!(m.relations.contexts[R].in_flight_spawns.is_empty());
    assert_eq!(m.convergence_facts(R).unwrap().pending_starts, 1);
    m.clock.mono_ms += 100_000;
    m.convergence_sync_facts(R);
    assert_eq!(m.convergence_facts(R).unwrap().pending_starts, 1);
    discover(&mut m, true);
    assert_eq!(
        m.convergence_facts(R).unwrap().discovery_covered_epoch,
        None
    );
}
#[test]
fn 发现失败按五十五三十六十退避且总期限不刷新() {
    let (mut m, mut mock) = setup_with_child();
    complete(&mut m, &mut mock, P, T, "completed");
    let deadline = m.convergences.runs[R].deadline_mono_ms;
    for delay in [5_000, 15_000, 30_000, 60_000, 60_000] {
        let now = m.clock.mono_ms;
        discover(&mut m, false);
        assert_eq!(m.convergences.progress[R].retry_at, Some(now + delay));
        assert_eq!(m.convergences.runs[R].deadline_mono_ms, deadline);
        retry(&mut m);
    }
}
#[test]
fn 发现期间新工作使旧覆盖无效() {
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, "子", "派生", "子轮");
    complete(&mut m, &mut mock, P, T, "completed");
    let original = m.convergences.progress[R].discovery.unwrap().1;
    complete(&mut m, &mut mock, "子", "子轮", "completed");
    assert!(m.convergence_facts(R).unwrap().work_epoch > original);
    discover(&mut m, true);
    assert_eq!(
        m.convergence_facts(R).unwrap().discovery_covered_epoch,
        None
    );
}
#[test]
fn 无子代理运行不进入收敛_按原流程直接完成() {
    // 用户 2026-10-11 定：没有纳管子代理的运行不做收尾发现、不开汇总，保持原释放语义。
    let (mut m, mut mock) = setup();
    complete(&mut m, &mut mock, P, T, "completed");
    assert!(!m.claims.contains_key(P));
    assert!(m
        .convergences
        .progress
        .get(R)
        .is_none_or(|p| p.discovery.is_none()));
    assert_eq!(m.runs.runs[R].status, Some(RunStatus::Completed));
}
#[test]
fn goal清除覆盖第二条自然释放路径() {
    let (mut m, mut mock) = setup_with_child();
    notify(
        &mut m,
        "thread/goal/updated",
        json!({"threadId":P,"goal":{"status":"active"}}),
    );
    complete(&mut m, &mut mock, P, T, "completed");
    assert!(matches!(
        m.claims[P].state,
        State::AwaitingContinuation { .. }
    ));
    notify(&mut m, "thread/goal/cleared", json!({"threadId":P}));
    assert!(matches!(m.claims[P].state, State::Converging { .. }));
    discover(&mut m, true);
    // 已有子代理结果：发现完成后仍须汇总，不能直接沿用父 completed 释放。
    assert!(m.claims.contains_key(P));
    assert_eq!(m.runs.runs[R].state, RunState::Running);
}
#[test]
fn detach保留待答期限工作版本并拒绝旧token() {
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, "子", "派生", "子轮");
    complete(&mut m, &mut mock, P, T, "completed");
    let old = m.convergences.runs[R].clone();
    let summary = join(&m).clone();
    let out = apply(
        &mut m,
        Event::ClientRequest {
            request: ClientRequest {
                conn: 1,
                id: json!(99),
                method: "broker/threads/detach".into(),
                params: Some(json!({"threadId":P,"runId":R,"policy":"on-request"})),
            },
            run_id: None,
        },
    );
    // 若公开参数检查拒绝，测试必须明确失败，不能手改 owner 充当 detach。
    assert!(
        matches!(m.claims[P].owner(), Some(Owner::Broker { .. })),
        "{out:?}"
    );
    assert_eq!(
        m.convergences.runs[R].deadline_mono_ms,
        old.deadline_mono_ms
    );
    assert_eq!(join(&m).deadline_at, summary.deadline_at);
    assert_eq!(join(&m).work_epoch, summary.work_epoch);
    assert!(!m.convergence_token_current(&old.token));
    assert!(m.convergence_token_current(&m.convergences.runs[R].token));
    apply(&mut m, Event::Disconnected { conn: 1 });
    assert!(!join(&m).automatic_control_revoked);
}
#[test]
fn 单调期限不受墙钟前后跳影响() {
    let (mut m, mut mock) = setup_with_child();
    complete(&mut m, &mut mock, P, T, "completed");
    let token = m.convergences.runs[R].token.clone();
    let deadline = m.convergences.runs[R].deadline_mono_ms;
    m.clock.now_ms = u64::MAX;
    m.convergence_timer(
        TimerKind::ConvergeDeadline {
            token: token.clone(),
        },
        &mut vec![],
    );
    assert!(!join(&m).automatic_control_revoked);
    m.clock.now_ms = 0;
    m.clock.mono_ms = deadline;
    m.convergence_timer(TimerKind::ConvergeDeadline { token }, &mut vec![]);
    assert_eq!(
        join(&m).terminal_reason,
        Some(TerminalReason::DiscoveryIncomplete)
    );
    assert!(join(&m).automatic_control_revoked);
}
#[test]
fn 迟到started不授予外部轮所有权() {
    let (mut m, mut mock) = setup_with_child();
    complete(&mut m, &mut mock, P, T, "completed");
    apply(&mut m, mock.turn_started(P, "外部轮"));
    assert!(!m.claims[P].owned_turns.contains("外部轮"));
    assert!(matches!(m.claims[P].state, State::FinishingTree { .. }));
    assert_eq!(
        join(&m).terminal_reason,
        Some(TerminalReason::OwnershipConflict)
    );
    assert!(!m.runs.runs[R].turns.iter().any(|t| t.turn_id == "外部轮"));
}
#[test]
fn 停止与期限交错保留首个原因且不复活() {
    for first_stop in [true, false] {
        let (mut m, mut mock) = setup_with_child();
        complete(&mut m, &mut mock, P, T, "completed");
        let token = m.convergences.runs[R].token.clone();
        if first_stop {
            m.convergence_cancel_for_thread(P, ReconcileCancel::Stop, &mut vec![]);
        }
        m.clock.mono_ms = m.convergences.runs[R].deadline_mono_ms;
        m.convergence_timer(TimerKind::ConvergeDeadline { token }, &mut vec![]);
        m.convergence_cancel_for_thread(P, ReconcileCancel::Stop, &mut vec![]);
        assert_eq!(
            join(&m).terminal_reason,
            Some(if first_stop {
                TerminalReason::UserStop
            } else {
                TerminalReason::DiscoveryIncomplete
            })
        );
        notify(
            &mut m,
            "thread/goal/updated",
            json!({"threadId":P,"goal":{"status":"active"}}),
        );
        assert!(join(&m).automatic_control_revoked);
    }
}
#[test]
fn owner断开锁定原因_detach后不响应旧owner() {
    let (mut m, mut mock) = setup_with_child();
    complete(&mut m, &mut mock, P, T, "completed");
    apply(&mut m, Event::Disconnected { conn: 1 });
    assert_eq!(join(&m).terminal_reason, Some(TerminalReason::OwnerGone));
    assert!(join(&m).automatic_control_revoked);
}
#[test]
fn 未知已发操作在取消和abandon后仍未落定() {
    let (mut m, mut mock) = setup_with_child();
    complete(&mut m, &mut mock, P, T, "completed");
    let op = operation(&m, ReconcileSubmission::OutcomeUnknown);
    m.convergence_operation_changed(op.clone());
    for reason in [
        ReconcileCancel::Stop,
        ReconcileCancel::Abandon,
        ReconcileCancel::Shutdown,
    ] {
        m.convergence_cancel_for_thread(P, reason, &mut vec![]);
        assert!(m.convergence_root_unsettled(R));
    }
    let mut confirmed = op;
    confirmed.outcome_unknown = false;
    confirmed.submission = ReconcileSubmission::DefinitelyNotSubmitted;
    m.convergence_operation_changed(confirmed);
    assert!(!m.convergence_root_unsettled(R));
    assert!(join(&m).automatic_control_revoked);
}
#[test]
fn phase通知与watch共享revision且只给扩展连接() {
    let (mut m, mut mock) = setup_with_child();
    complete(&mut m, &mut mock, P, T, "completed");
    m.records.get_mut(P).unwrap().watchers.extend([2, 3]);
    m.live_conns.extend([2, 3]);
    m.cxb_conns.insert(3);
    let mut actions = vec![];
    m.convergence_phase(R, JoinPhase::FinalCheck, &mut actions);
    let sent: Vec<_> = actions
        .iter()
        .filter_map(|a| match a {
            Action::Send { conn, message }
                if message["method"] == "broker/run/convergenceChanged" =>
            {
                Some((*conn, message))
            }
            _ => None,
        })
        .collect();
    assert!(sent.iter().any(|(c, _)| *c == 1));
    assert!(sent.iter().any(|(c, _)| *c == 3));
    assert!(!sent.iter().any(|(c, _)| *c == 2));
    let snapshot = m.child_watch_snapshot(P, Some(R), json!({}));
    for (_, message) in sent {
        assert_eq!(message["params"]["revision"], snapshot["revision"]);
        assert_eq!(message["params"]["subagentJoin"], snapshot["subagentJoin"]);
    }
}
#[test]
fn 终态全表与树提交一致_父失败优先() {
    use InterruptReason as I;
    use RunStatus as S;
    use TerminalReason as Rn;
    let table = [
        (Rn::ParentFailed, S::Failed, None, 7),
        (Rn::ParentInterrupted, S::Interrupted, None, 4),
        (Rn::UserStop, S::Interrupted, Some(I::Stop), 4),
        (
            Rn::OwnerGone,
            S::Interrupted,
            Some(I::ClientDisconnected),
            4,
        ),
        (
            Rn::BudgetExhausted,
            S::GoalBlocked,
            Some(I::BudgetExhausted),
            9,
        ),
        (
            Rn::BudgetIncomplete,
            S::GoalBlocked,
            Some(I::BudgetIncomplete),
            9,
        ),
        (
            Rn::ConvergeDeadline,
            S::Interrupted,
            Some(I::ConvergeDeadline),
            4,
        ),
        (Rn::WakeLimit, S::GoalBlocked, Some(I::WakeLimit), 9),
        (Rn::AutoTurnLimit, S::GoalBlocked, Some(I::AutoTurnLimit), 9),
        (Rn::NoAction, S::Interrupted, Some(I::GoalLoop), 9),
        (
            Rn::DiscoveryIncomplete,
            S::Interrupted,
            Some(I::DiscoveryIncomplete),
            4,
        ),
        (
            Rn::OwnershipConflict,
            S::Interrupted,
            Some(I::OwnershipConflict),
            4,
        ),
        (Rn::WakeUnknown, S::Interrupted, Some(I::WakeUnknown), 4),
        (Rn::WakeFailed, S::Failed, Some(I::WakeFailed), 7),
        (Rn::GoalPaused, S::Interrupted, Some(I::GoalInactive), 4),
        (Rn::GoalBlocked, S::GoalBlocked, Some(I::GoalInactive), 9),
        (
            Rn::GoalBudgetLimited,
            S::GoalBlocked,
            Some(I::BudgetExhausted),
            9,
        ),
        (
            Rn::GoalUsageLimited,
            S::GoalBlocked,
            Some(I::GoalInactive),
            9,
        ),
        (
            Rn::RootNoDirectInput,
            S::Interrupted,
            Some(I::RootNoDirectInput),
            4,
        ),
        (Rn::Abandoned, S::Interrupted, Some(I::Abandoned), 4),
    ];
    let m = Machine::default();
    for (reason, status, interrupt, exit) in table {
        let result = m
            .convergence_terminal(&TerminalInput {
                reason,
                parent_status: Some(TurnStatus::Completed),
                existing_interrupt: None,
            })
            .unwrap();
        assert_eq!(
            (result.status, result.interrupt_reason, result.exit_code),
            (status, interrupt, exit),
            "{reason:?}"
        );
        if let Some(trigger) = result.tree_trigger {
            let snapshot = crate::stop::tree::ReleaseSnapshot {
                identity: RunIdentity {
                    run_id: R.into(),
                    logical_root_thread_id: P.into(),
                    execution_thread_id: P.into(),
                },
                token: None,
                owner: None,
                gen: 1,
                parent_turn_id: Some(T.into()),
                parent_status: Some(TurnStatus::Completed),
                goal_status: None,
                loop_guard: false,
            };
            assert_eq!(
                crate::stop::tree::commit_status(&snapshot, trigger, None),
                (status, interrupt)
            );
        }
        let failed = m
            .convergence_terminal(&TerminalInput {
                reason,
                parent_status: Some(TurnStatus::Failed),
                existing_interrupt: Some(I::Stop),
            })
            .unwrap();
        assert_eq!(
            (
                failed.status,
                failed.terminal_reason,
                failed.interrupt_reason
            ),
            (S::Failed, Rn::ParentFailed, Some(I::Stop))
        );
    }
    assert!(m
        .convergence_terminal(&TerminalInput {
            reason: Rn::BrokerShutdown,
            parent_status: None,
            existing_interrupt: None
        })
        .is_none());
}

fn staged() -> (Machine, SubagentMock) {
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, "子", "派生", "子轮");
    complete(&mut m, &mut mock, "子", "子轮", "completed");
    complete(&mut m, &mut mock, P, T, "completed");
    discover(&mut m, true);
    assert!(
        m.convergences.progress[R].reading.is_some(),
        "facts={:?} progress={:?} goals={:?}",
        m.convergence_facts(R),
        m.convergences.progress[R],
        m.convergences.goals
    );
    (m, mock)
}
fn batch(m: &Machine) -> ReconcileBatch {
    let facts = m.convergence_facts(R).unwrap();
    let items = facts
        .activities
        .iter()
        .map(|f| ReconcileItem {
            key: f.key.clone(),
            parent_thread_id: f.parent_thread_id.clone(),
            status: f.result_status.unwrap(),
            output: String::new(),
            output_source: crate::runs::OutputSource::None,
            locator: facts
                .result_refs
                .iter()
                .find(|r| r.key == f.key)
                .unwrap()
                .locator
                .clone(),
            truncated: false,
            unavailable_reason: Some("未取得输出".into()),
        })
        .collect();
    ReconcileBatch {
        work_epoch: facts.work_epoch,
        items,
        input: "证据包".into(),
        total_bytes: "证据包".len(),
        omitted: 0,
        coverage: ResultCoverage {
            epoch: facts.work_epoch,
            total: facts.activities.len(),
            available: 0,
            unavailable: facts.activities.len(),
            truncated: 0,
            omitted: 0,
        },
    }
}
fn confirm_parent(m: &mut Machine) -> ConvergeToken {
    let mut op = operation(m, ReconcileSubmission::ConfirmedOwnTurn);
    let token = op.token.clone();
    m.convergences.progress.get_mut(R).unwrap().reading = None;
    m.remember_owned_turn(P, "汇总轮");
    op.outcome_unknown = false;
    m.convergence_operation_changed(op.clone());
    assert!(m.convergence_delivered(&token, op.op_id, "汇总轮", op.work_epoch, &mut vec![]));
    m.convergence_event(
        P,
        ConvergeEvent::ParentTurn {
            turn: "汇总轮",
            started: true,
        },
        &mut vec![],
    );
    // B 的确定性桩：确认后在原账本推进当前父轮；不创建第二个 RunRecord。
    m.runs.runs.get_mut(R).unwrap().current_turn_id = Some("汇总轮".into());
    m.run_turn_started(R, P, "汇总轮", &mut vec![]);
    token
}
#[test]
fn 完整投递完成最终发现才释放_最后额度允许当前轮完成() {
    for last in [false, true] {
        let (mut m, mut mock) = staged();
        let started = m.runs.runs[R].started_at;
        let deadline = m.convergences.runs[R].deadline_mono_ms;
        let token = confirm_parent(&mut m);
        if last {
            let record = m.runs.runs.get_mut(R).unwrap();
            record.auto_turns_started = Some(50);
            record.subagent_join.as_mut().unwrap().wakeups_started = 5;
        }
        assert_eq!(join(&m).reconciled_epoch, 0);
        complete(&mut m, &mut mock, P, "汇总轮", "completed");
        assert_eq!(join(&m).reconciled_epoch, join(&m).work_epoch);
        assert_eq!(join(&m).phase, JoinPhase::FinalCheck);
        assert!(m.claims.contains_key(P));
        assert_eq!(m.convergences.runs[R].deadline_mono_ms, deadline);
        discover(&mut m, true);
        assert_eq!(m.runs.runs[R].status, Some(RunStatus::Completed));
        assert_eq!(m.runs.runs[R].started_at, started);
        assert!(!m.convergence_token_current(&token));
    }
}
#[test]
fn 最后额度后新工作必须异常不能第六次汇总() {
    for wake in [true, false] {
        let (mut m, mut mock) = staged();
        confirm_parent(&mut m);
        if wake {
            m.runs
                .runs
                .get_mut(R)
                .unwrap()
                .subagent_join
                .as_mut()
                .unwrap()
                .wakeups_started = 5;
        } else {
            m.runs.runs.get_mut(R).unwrap().auto_turns_started = Some(50);
        }
        spawn(&mut m, &mut mock, P, "汇总轮", "新子", "新派生", "新子轮");
        complete(&mut m, &mut mock, "新子", "新子轮", "completed");
        complete(&mut m, &mut mock, P, "汇总轮", "completed");
        discover(&mut m, true);
        assert_eq!(
            join(&m).terminal_reason,
            Some(if wake {
                TerminalReason::WakeLimit
            } else {
                TerminalReason::AutoTurnLimit
            })
        );
        assert!(join(&m).automatic_control_revoked);
    }
}
#[test]
fn 完成前回包或独立started不能推进投递版本() {
    let (mut m, _) = staged();
    let op = operation(&m, ReconcileSubmission::OutcomeUnknown);
    let token = op.token.clone();
    let epoch = op.work_epoch;
    m.convergence_operation_changed(op.clone());
    assert!(!m.convergence_delivered(&token, op.op_id, "汇总轮", epoch, &mut vec![]));
    m.remember_owned_turn(P, "汇总轮");
    assert!(!m.convergence_delivered(&token, op.op_id, "汇总轮", epoch, &mut vec![]));
    assert_eq!(join(&m).delivered_epoch, 0);
}
#[test]
fn 父汇总失败不推进reconciled并覆盖后到goalblocked() {
    let (mut m, mut mock) = staged();
    confirm_parent(&mut m);
    complete(&mut m, &mut mock, P, "汇总轮", "failed");
    notify(
        &mut m,
        "thread/goal/updated",
        json!({"threadId":P,"goal":{"status":"blocked"}}),
    );
    assert_eq!(join(&m).reconciled_epoch, 0);
    let terminal = m
        .convergence_run_terminal(R, Some(TurnStatus::Failed))
        .unwrap();
    assert_eq!(terminal.status, RunStatus::Failed);
    assert_eq!(terminal.terminal_reason, TerminalReason::ParentFailed);
}
#[test]
fn 旧读取回调零号重复回调与新批次隔离() {
    let (mut m, _) = staged();
    let token = m.convergences.runs[R].token.clone();
    let batch = batch(&m);
    m.convergences.test_submission = Some(Err(ReconcileRefusal::WakeLimit));
    m.convergence_results_ready(
        token.clone(),
        BatchReadId::DISABLED,
        batch.clone(),
        &mut vec![],
    );
    assert!(!join(&m).automatic_control_revoked);
    let mut stale = token.clone();
    stale.generation += 1;
    m.convergence_results_ready(stale, BatchReadId(9), batch.clone(), &mut vec![]);
    assert!(!join(&m).automatic_control_revoked);
    m.convergence_results_ready(token.clone(), BatchReadId(9), batch.clone(), &mut vec![]);
    assert_eq!(join(&m).terminal_reason, Some(TerminalReason::WakeLimit));
    let before = m.clone();
    m.convergence_results_ready(token, BatchReadId(9), batch, &mut vec![]);
    assert_eq!(m, before);
}
#[test]
fn 预算未核实不把零读号当成功() {
    for admission in [
        BudgetAdmission::Disabled,
        BudgetAdmission::NeedsVerification,
        BudgetAdmission::Exhausted,
    ] {
        let (mut m, mut mock) = setup();
        m.convergences.test_budget = Some(admission);
        spawn(&mut m, &mut mock, P, T, "子", "派生", "子轮");
        complete(&mut m, &mut mock, "子", "子轮", "completed");
        complete(&mut m, &mut mock, P, T, "completed");
        discover(&mut m, true);
        assert!(m.convergences.progress[R].reading.is_none());
        assert_eq!(join(&m).delivered_epoch, 0);
        if admission == BudgetAdmission::Exhausted {
            assert_eq!(
                join(&m).terminal_reason,
                Some(TerminalReason::BudgetExhausted)
            );
        }
    }
    let (mut m, mut mock) = setup();
    m.convergences.test_read = Some(BatchReadId::DISABLED);
    spawn(&mut m, &mut mock, P, T, "子", "派生", "子轮");
    complete(&mut m, &mut mock, "子", "子轮", "completed");
    complete(&mut m, &mut mock, P, T, "completed");
    discover(&mut m, true);
    assert!(m.convergences.progress[R].reading.is_none());
    assert!(m.convergences.progress[R].retry_at.is_some());
}
#[test]
fn goal暂停阻断运行中及收敛中自动启动() {
    for waiting in [true, false] {
        for status in ["paused", "blocked", "budgetLimited", "usageLimited"] {
            let (mut m, mut mock) = setup_with_child();
            if waiting {
                complete(&mut m, &mut mock, P, T, "completed");
            }
            notify(
                &mut m,
                "thread/goal/updated",
                json!({"threadId":P,"goal":{"status":status}}),
            );
            assert!(join(&m).automatic_control_revoked);
            assert!(m.tree_for_run(R).is_some());
        }
    }
}
#[test]
fn notloaded通知撤销旧观察但不制造新结果版本() {
    let (mut m, _) = staged();
    let epoch = join(&m).work_epoch;
    notify(
        &mut m,
        "thread/status/changed",
        json!({"threadId":"子","status":{"type":"notLoaded"}}),
    );
    let facts = m.convergence_facts(R).unwrap();
    assert_eq!(facts.work_epoch, epoch);
    assert!(!facts.activities[0].verified);
    assert_eq!(facts.activities[0].activity, MemberActivity::Unknown);
    assert_eq!(facts.discovery_covered_epoch, None);
}
#[test]
fn 关闭失去自动权_重启仅保留历史摘要() {
    let (mut m, mut mock) = setup_with_child();
    complete(&mut m, &mut mock, P, T, "completed");
    let token = m.convergences.runs[R].token.clone();
    apply(&mut m, Event::Shutdown);
    apply(&mut m, Event::RunsFinalize);
    let record = m.runs.runs[R].clone();
    assert_eq!(record.state, RunState::Lost);
    assert_eq!(record.status, None);
    assert_eq!(record.interrupt_reason, Some(InterruptReason::BrokerLost));
    assert!(
        record
            .subagent_join
            .as_ref()
            .unwrap()
            .automatic_control_revoked
    );
    let mut restored = Machine::default();
    restored.runs.runs.insert(
        R.into(),
        serde_json::from_value(serde_json::to_value(record).unwrap()).unwrap(),
    );
    assert!(restored.convergences.runs.is_empty());
    assert!(!restored.convergence_token_current(&token));
    assert!(restored.convergence_facts(R).is_none());
    assert!(restored.convergence_summary(R).is_some());
}
#[test]
fn 发现实际_rpc路径补查活动与goal并不学习旧代回包() {
    let (mut m, mut mock) = setup();
    m.convergences.capabilities.remove(P);
    spawn(&mut m, &mut mock, P, T, "子", "派生", "子轮");
    complete(&mut m, &mut mock, "子", "子轮", "completed");
    let actions = complete(&mut m, &mut mock, P, T, "completed");
    let mut queue: std::collections::VecDeque<_> = actions.into_iter().collect();
    let mut checks = 0;
    let mut steps = 0;
    while let Some(action) = queue.pop_front() {
        steps += 1;
        assert!(steps < 1000, "发现必须有界");
        let Action::Request(context) = action else {
            continue;
        };
        if !matches!(context.operation, crate::claim::Operation::Discovery { .. }) {
            continue;
        }
        let value = match context.method.as_str() {
            "thread/list" | "thread/loaded/list" => json!({"data":[],"nextCursor":null}),
            "thread/goal/get" => {
                checks += 1;
                json!({"goal":null})
            }
            "thread/read" => {
                checks += 1;
                if context.params.as_ref().unwrap()["threadId"] == P {
                    assert_eq!(context.params.as_ref().unwrap()["includeTurns"], false);
                    json!({"thread":{"id":P,"canAcceptDirectInput":true}})
                } else {
                    json!({"thread":{"id":"子","status":{"type":"idle"},"turns":[{"id":"子轮","status":"completed"}]}})
                }
            }
            method => panic!("未预期方法 {method}"),
        };
        queue.extend(apply(
            &mut m,
            Event::Reply {
                context,
                result: Ok(value),
            },
        ));
    }
    assert_eq!(
        checks,
        4,
        "facts={:?} progress={:?} discoveries={:?} work={:?}",
        m.convergence_facts(R),
        m.convergences.progress[R],
        m.relations.discoveries,
        m.relations.aux.work
    );
    assert!(
        m.convergences.progress[R].reading.is_some(),
        "facts={:?} progress={:?} goals={:?}",
        m.convergence_facts(R),
        m.convergences.progress[R],
        m.convergences.goals
    );
}
#[test]
fn 独立配置保存但不发送上游也不启用门禁() {
    let mut m = Machine::default();
    m.record(P).subscribed = true;
    let actions = apply(
        &mut m,
        Event::ClientRequest {
            request: ClientRequest {
                conn: 1,
                id: json!(1),
                method: "turn/start".into(),
                params: Some(
                    json!({"threadId":P,"input":[],"cxbConvergence":{"timeoutMs":120000}}),
                ),
            },
            run_id: Some(R.into()),
        },
    );
    assert_eq!(
        m.runs.runs[R]
            .convergence_config
            .as_ref()
            .unwrap()
            .timeout_ms,
        120000
    );
    assert!(!m.convergences.enabled);
    assert!(actions.iter().any(|a|matches!(a,Action::Request(c) if c.method=="turn/start" && c.params.as_ref().unwrap().get("cxbConvergence").is_none())));
}

#[test]
fn 所有身份字段与发现代号都拒绝旧回调() {
    let (mut m, mut mock) = setup_with_child();
    complete(&mut m, &mut mock, P, T, "completed");
    let token = m.convergences.runs[R].token.clone();
    for i in 0..6 {
        let mut old = token.clone();
        match i {
            0 => old.run_id = "旧运行".into(),
            1 => old.claim.claim_id += 1,
            2 => old.claim.thread_id = "旧线程".into(),
            3 => old.manage_gen += 1,
            4 => old.conn_epoch += 1,
            _ => old.generation += 1,
        }
        assert!(!m.convergence_token_current(&old));
        let before = m.clone();
        m.convergence_timer(TimerKind::ConvergeDeadline { token: old }, &mut vec![]);
        assert_eq!(m, before);
    }
    let progress = m.convergences.progress[R].clone();
    let epoch = progress.discovery.unwrap().1;
    let result = DiscoveryResult {
        root: P.into(),
        scope: DiscoveryScope::Run { run_id: R.into() },
        generation: progress.discovery_generation + 1,
        members: Default::default(),
        state: DiscoveryState {
            complete: true,
            version: 0,
            missing: Default::default(),
            incomplete_reasons: vec![],
        },
        attachments: Default::default(),
        cancelled: false,
    };
    let before = m.clone();
    m.convergence_discovery_done(token, epoch, result, &mut vec![]);
    assert_eq!(m, before);
}
#[test]
fn 默认线程记录不能当作goal已核实() {
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, "子", "派生", "子轮");
    m.convergences.goals.remove("子");
    complete(&mut m, &mut mock, "子", "子轮", "completed");
    complete(&mut m, &mut mock, P, T, "completed");
    discover(&mut m, true);
    let facts = m.convergence_facts(R).unwrap();
    assert_eq!(facts.activities[0].active_goal, None);
    assert!(m.convergences.progress[R].reading.is_none());
}
#[test]
fn 有界批次形状与活动范围二次核实() {
    for bad in 0..4 {
        let (mut m, _) = staged();
        let token = m.convergences.runs[R].token.clone();
        let mut b = batch(&m);
        match bad {
            0 => b.total_bytes = 1,
            1 => b.coverage.total += 1,
            2 => b.items[0].key.activity_seq += 1,
            _ => b.items[0].status = ActivityResultStatus::Failed,
        }
        m.convergence_results_ready(token, BatchReadId(9), b, &mut vec![]);
        assert_eq!(join(&m).terminal_reason, Some(TerminalReason::WakeFailed));
    }
}
#[test]
fn 预算补核实有总窗_不重复发请求且失败不准入() {
    let (mut m, mut mock, verification) = setup_missing_budget();
    complete(&mut m, &mut mock, "预算子", "预算子轮", "completed");
    notify(&mut m, "thread/goal/cleared", json!({"threadId":P}));
    complete(&mut m, &mut mock, P, T, "completed");
    discover(&mut m, true);
    retry(&mut m);
    discover(&mut m, true);
    assert!(!join(&m).automatic_control_revoked);
    retry(&mut m);
    assert!(m.clock.mono_ms >= verification.deadline_mono_ms);
    assert!(m.take_budget_verification(&verification.token).is_none());
    assert_eq!(
        join(&m).terminal_reason,
        Some(TerminalReason::BudgetIncomplete)
    );
}
#[test]
fn 真实输入优先取消后goal变化不恢复自动控制() {
    let (mut m, _) = staged();
    let token = m.convergences.runs[R].token.clone();
    let op = operation(&m, ReconcileSubmission::OutcomeUnknown);
    m.convergence_operation_changed(op.clone());
    m.convergence_cancel_for_thread(P, ReconcileCancel::UserInput, &mut vec![]);
    assert!(join(&m).automatic_control_revoked);
    assert!(m.convergence_root_unsettled(R));
    assert!(!m.convergence_delivered(&token, op.op_id, "汇总轮", op.work_epoch, &mut vec![]));
    notify(
        &mut m,
        "thread/goal/updated",
        json!({"threadId":P,"goal":{"status":"active"}}),
    );
    assert!(join(&m).automatic_control_revoked);
}
#[test]
fn 旧投递不得回退最终汇总轮定位() {
    let (mut m, _) = staged();
    let token = confirm_parent(&mut m);
    let epoch = join(&m).delivered_epoch;
    let mut old = operation(&m, ReconcileSubmission::ConfirmedOwnTurn);
    old.op_id = ReconcileOpId(8);
    old.work_epoch = epoch - 1;
    old.target_turn_id = Some(T.into());
    m.convergence_operation_changed(old.clone());
    assert!(!m.convergence_delivered(&token, old.op_id, T, old.work_epoch, &mut vec![]));
    assert_eq!(join(&m).last_reconcile_turn_id.as_deref(), Some("汇总轮"));
    assert_eq!(join(&m).delivered_epoch, epoch);
}
#[test]
fn 重启沿用原中断原因但撤销历史自动权() {
    let (mut m, mut mock) = setup_with_child();
    complete(&mut m, &mut mock, P, T, "completed");
    let mut record = m.runs.runs[R].clone();
    record.interrupt_reason = Some(InterruptReason::Stop);
    let index = m.runs.index.clone();
    let mut restored = Machine::default();
    apply(
        &mut restored,
        Event::Restore {
            index,
            runs: vec![record],
            inbox: Default::default(),
        },
    );
    assert_eq!(restored.runs.runs[R].state, RunState::Lost);
    assert_eq!(
        restored.runs.runs[R].interrupt_reason,
        Some(InterruptReason::Stop)
    );
    assert_eq!(
        join(&restored).terminal_reason,
        Some(TerminalReason::BrokerShutdown)
    );
    assert!(join(&restored).automatic_control_revoked);
    assert!(restored.convergences.runs.is_empty());
}

#[test]
fn 旧父轮迟到started不把收敛状态恢复为running() {
    let (mut m, mut mock) = setup_with_child();
    complete(&mut m, &mut mock, P, T, "completed");
    let deadline = m.convergences.runs[R].deadline_mono_ms;
    apply(&mut m, mock.turn_started(P, T));
    assert!(matches!(m.claims[P].state, State::Converging { .. }));
    assert_eq!(m.convergences.runs[R].deadline_mono_ms, deadline);
}
#[test]
fn 代理工具状态errored和shutdown接入活动结果而非永久线程终态() {
    for (status, tool, expected) in [
        ("errored", "wait", ActivityResultStatus::Failed),
        ("shutdown", "closeAgent", ActivityResultStatus::Shutdown),
        (
            "interrupted",
            "interruptAgent",
            ActivityResultStatus::Interrupted,
        ),
    ] {
        let (mut m, mut mock) = setup();
        spawn(&mut m, &mut mock, P, T, "子", "派生", "子轮");
        notify(
            &mut m,
            "item/completed",
            json!({"threadId":P,"turnId":T,"item":{"id":"状态工具","type":"collabAgentToolCall","tool":tool,"status":"completed","agentsStates":{"子":{"status":status}}}}),
        );
        complete(&mut m, &mut mock, P, T, "completed");
        let facts = m.convergence_facts(R).unwrap();
        assert_eq!(facts.activities[0].result_status, Some(expected));
        assert_eq!(facts.activities[0].activity, MemberActivity::Settled);
        apply(&mut m, mock.turn_started("子", "复用新轮"));
        assert!(m.convergence_facts(R).unwrap().activities.iter().any(|a| a
            .key
            .turn_id
            .as_deref()
            == Some("复用新轮")
            && a.activity == MemberActivity::Running));
    }
}

#[test]
fn 关闭门禁时新增代理状态归一不改变sa1a活动事实() {
    let (mut m, mut mock) = setup();
    m.convergences.enabled = false;
    spawn(&mut m, &mut mock, P, T, "子", "派生", "子轮");
    notify(
        &mut m,
        "item/completed",
        json!({"threadId":P,"turnId":T,"item":{"id":"等待","type":"collabAgentToolCall","tool":"wait","status":"completed","agentsStates":{"子":{"status":"errored"}}}}),
    );
    assert_eq!(
        m.relations.contexts[R].members["子"].activity,
        MemberActivity::Running
    );
    assert!(
        !m.relations.aux.observed["子"]
            .iter()
            .find(|t| t.turn == "子轮")
            .unwrap()
            .completed
    );
    assert!(m.convergences.runs.is_empty());
}

#[test]
fn 根能力未知先补查_明确false异常且不启动汇总() {
    for capability in [None, Some(false)] {
        let (mut m, mut mock) = setup();
        m.convergences.capabilities.remove(P);
        if let Some(value) = capability {
            m.convergences
                .capabilities
                .insert(P.into(), (m.relations.conn_epoch, value));
        }
        spawn(&mut m, &mut mock, P, T, "子", "派生", "子轮");
        complete(&mut m, &mut mock, "子", "子轮", "completed");
        complete(&mut m, &mut mock, P, T, "completed");
        discover(&mut m, true);
        assert!(m.convergences.progress[R].reading.is_none());
        if capability == Some(false) {
            assert_eq!(
                join(&m).terminal_reason,
                Some(TerminalReason::RootNoDirectInput)
            );
        } else {
            assert!(m.convergences.progress[R].retry_at.is_some());
            assert_eq!(join(&m).terminal_reason, None);
        }
    }
}

#[test]
fn 集成_跟进复用只绑定明确操作后的新轮() {
    for version in ["v1", "v2"] {
        let (mut m, mut mock) = setup();
        spawn(&mut m, &mut mock, P, T, "子", "派生", "旧轮");
        complete(&mut m, &mut mock, "子", "旧轮", "completed");
        let item = if version == "v1" {
            json!({"type":"collabAgentToolCall","id":"跟进","tool":"sendInput","status":"completed","senderThreadId":P,"receiverThreadIds":["子"]})
        } else {
            json!({"type":"subAgentActivity","id":"跟进","kind":"interacted","agentThreadId":"子"})
        };
        notify(
            &mut m,
            "item/started",
            json!({"threadId":P,"turnId":T,"item":item}),
        );
        notify(
            &mut m,
            "item/completed",
            json!({"threadId":P,"turnId":T,"item":item}),
        );
        apply(&mut m, mock.turn_started("子", "新轮"));
        complete(&mut m, &mut mock, "子", "新轮", "completed");
        complete(&mut m, &mut mock, P, T, "completed");
        discover(&mut m, true);
        let facts = m.convergence_facts(R).unwrap();
        assert_eq!(facts.activities.len(), 2);
        assert!(facts
            .activities
            .iter()
            .all(|a| a.verified && a.activity == MemberActivity::Settled));
        assert_eq!(facts.pending_starts, 0);
        assert!(m.convergences.progress[R].reading.is_some());
    }
}

#[test]
fn 集成_完成后关闭保留shutdown结果和关闭证明() {
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, "子", "派生", "子轮");
    complete(&mut m, &mut mock, "子", "子轮", "interrupted");
    notify(&mut m, "thread/closed", json!({"threadId":"子"}));
    notify(
        &mut m,
        "item/completed",
        json!({"threadId":P,"turnId":T,"item":{"id":"关闭","type":"collabAgentToolCall","tool":"closeAgent","status":"completed","agentsStates":{"子":{"status":"shutdown"}}}}),
    );
    complete(&mut m, &mut mock, P, T, "completed");
    assert_eq!(
        m.convergence_facts(R).unwrap().activities[0].result_status,
        Some(ActivityResultStatus::Shutdown)
    );
    assert!(m.convergences.closed_threads.contains("子"));
}

#[test]
fn 集成_授权交接新token重核实原提交不重发不刷新截止() {
    use crate::claim::status_read::{StatusCaller, StatusDone, StatusResult};
    use crate::claim::Operation;
    let (mut m, mut mock) = setup();
    m.convergences.test_budget = None;
    m.convergences.test_read = None;
    spawn(&mut m, &mut mock, P, T, "子", "派生", "子轮");
    complete(&mut m, &mut mock, "子", "子轮", "completed");
    complete(&mut m, &mut mock, P, T, "completed");
    let actions = discover(&mut m, true);
    let read = actions
        .into_iter()
        .find_map(|a| match a {
            Action::Request(c) if matches!(c.operation, Operation::ChildResultRead { .. }) => {
                Some(c)
            }
            _ => None,
        })
        .unwrap();
    let actions = apply(
        &mut m,
        Event::Reply {
            context: read,
            result: Ok(crate::test_support::status_read::items_page(
                "子轮",
                &[json!({"type":"agentMessage","text":"随机子结果"})],
                None,
            )),
        },
    );
    let request = actions
        .iter()
        .find_map(|a| match a {
            Action::Request(c) if matches!(c.operation, Operation::StartReconciliation { .. }) => {
                Some(c.clone())
            }
            _ => None,
        })
        .unwrap();
    let Operation::StartReconciliation { token: old, op_id } = &request.operation else {
        unreachable!()
    };
    let original = m.reconcile_operation(*op_id).unwrap().clone();
    let old_read = m.runs.reconciliation.operations[&op_id.0]
        .status_read
        .unwrap();
    notify(
        &mut m,
        "turn/started",
        json!({"threadId":P,"turn":{"id":"汇总轮","status":"inProgress"}}),
    );
    m.clock.mono_ms += 5_000;
    let actions = apply(
        &mut m,
        Event::ClientRequest {
            request: ClientRequest {
                conn: 1,
                id: json!(999),
                method: "broker/threads/detach".into(),
                params: Some(json!({"threadId":P,"runId":R,"policy":"on-request"})),
            },
            run_id: None,
        },
    );
    let current = m.convergences.runs[R].token.clone();
    assert_ne!(current, *old);
    let pending = &m.runs.reconciliation.operations[&op_id.0];
    assert_eq!(pending.verify_token, current);
    assert_ne!(pending.status_read, Some(old_read));
    assert_eq!(pending.operation, original);
    assert!(!actions.iter().any(|a| matches!(a,Action::Request(c) if matches!(c.operation,Operation::StartReconciliation { .. }))));
    m.subagent_reconcile_status_done(
        StatusDone {
            read_id: old_read,
            thread: P.into(),
            token: Some(old.claim.clone()),
            caller: StatusCaller::SubagentReconcile {
                token: old.clone(),
                op_id: *op_id,
            },
            result: StatusResult::Carrier("汇总轮".into()),
        },
        &mut vec![],
    );
    assert_eq!(join(&m).delivered_epoch, 0);
    let read_id = m.runs.reconciliation.operations[&op_id.0]
        .status_read
        .unwrap();
    let mut actions = vec![];
    m.subagent_reconcile_status_done(
        StatusDone {
            read_id,
            thread: P.into(),
            token: Some(current.claim.clone()),
            caller: StatusCaller::SubagentReconcile {
                token: current.clone(),
                op_id: *op_id,
            },
            result: StatusResult::Carrier("汇总轮".into()),
        },
        &mut actions,
    );
    assert_eq!(join(&m).delivered_epoch, original.work_epoch);
    assert_eq!(join(&m).wakeup_attempts, 1);
    assert_eq!(m.runs.runs[R].auto_turns_started, Some(1));
    let final_op = m.reconcile_operation(*op_id).unwrap();
    assert_eq!(final_op.token, *old);
    assert_eq!(
        final_op.client_user_message_id,
        original.client_user_message_id
    );
    assert_eq!(
        final_op.verify_deadline_mono_ms,
        original.verify_deadline_mono_ms
    );
    assert!(!actions.iter().any(|a| matches!(a,Action::Request(c) if matches!(c.operation,Operation::StartReconciliation { .. }))));
}

#[test]
fn 集成_托管交接保留动态工具待答及原截止() {
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, "子", "派生", "子轮");
    apply(
        &mut m,
        Event::ServerRequest {
            id: json!(91),
            forward_id: "动态转发".into(),
            method: "item/tool/call".into(),
            params: Some(
                json!({"threadId":"子","turnId":"子轮","callId":"调用","tool":"测试工具","arguments":{}}),
            ),
        },
    );
    let deadline = m.convergences.runs[R].deadline_mono_ms;
    let actions = apply(
        &mut m,
        Event::ClientRequest {
            request: ClientRequest {
                conn: 1,
                id: json!(92),
                method: "broker/threads/detach".into(),
                params: Some(json!({"threadId":P,"runId":R,"policy":"on-request"})),
            },
            run_id: None,
        },
    );
    assert!(!actions.iter().any(
        |a| matches!(a,Action::ServerResult {id,..}|Action::ServerError {id,..} if *id==json!(91))
    ));
    assert_eq!(m.convergences.runs[R].deadline_mono_ms, deadline);
    assert_eq!(m.convergence_facts(R).unwrap().pending_requests, 1);
    assert!(m
        .inbox_pending_run_items(R)
        .iter()
        .any(|i| i.method == "item/tool/call"));
}

#[test]
fn 集成_停止期间非活动goal换版本会重新核实() {
    let (mut m, mut mock) = setup_with_child();
    complete(&mut m, &mut mock, P, T, "interrupted");
    let id = m.stops.operations.keys().next().unwrap().clone();
    let op = m.stops.operations.get_mut(&id).unwrap();
    op.goal_busy = false;
    op.goal_attempted = true;
    op.goal_done = true;
    op.brake_version = Some(m.stops.versions[P]);
    let actions = notify(
        &mut m,
        "thread/goal/updated",
        json!({"threadId":P,"goal":{"status":"blocked"}}),
    );
    assert!(actions
        .iter()
        .any(|a| matches!(a,Action::Request(c) if c.method=="thread/goal/get")));
    assert_eq!(
        join(&m).terminal_reason,
        Some(TerminalReason::ParentInterrupted)
    );
}

#[test]
fn 集成_首采样完成通知早于承载核实时仍回放原运行结果() {
    use crate::claim::status_read::{StatusCaller, StatusDone, StatusResult};
    use crate::claim::Operation;
    let (mut m, mut mock) = setup();
    m.convergences.test_budget = None;
    m.convergences.test_read = None;
    spawn(&mut m, &mut mock, P, T, "子", "派生", "子轮");
    complete(&mut m, &mut mock, "子", "子轮", "completed");
    complete(&mut m, &mut mock, P, T, "completed");
    let actions = discover(&mut m, true);
    let read = actions
        .into_iter()
        .find_map(|a| match a {
            Action::Request(c) if matches!(c.operation, Operation::ChildResultRead { .. }) => {
                Some(c)
            }
            _ => None,
        })
        .unwrap();
    let actions = apply(
        &mut m,
        Event::Reply {
            context: read,
            result: Ok(crate::test_support::status_read::items_page(
                "子轮",
                &[json!({"type":"agentMessage","text":"结果"})],
                None,
            )),
        },
    );
    let (token, op_id) = actions
        .into_iter()
        .find_map(|a| match a {
            Action::Request(c) => match c.operation {
                Operation::StartReconciliation { token, op_id } => Some((token, op_id)),
                _ => None,
            },
            _ => None,
        })
        .unwrap();
    notify(
        &mut m,
        "turn/started",
        json!({"threadId":P,"turn":{"id":"快汇总","status":"inProgress"}}),
    );
    notify(
        &mut m,
        "turn/completed",
        json!({"threadId":P,"turn":{"id":"快汇总","status":"completed","itemsView":"summary","items":[]}}),
    );
    assert!(m.runs.reconciliation.operations[&op_id.0]
        .completions
        .contains_key("快汇总"));
    assert!(!m.runs.runs[R].turns.iter().any(|t| t.turn_id == "快汇总"));
    let read_id = m.runs.reconciliation.operations[&op_id.0]
        .status_read
        .unwrap();
    m.subagent_reconcile_status_done(
        StatusDone {
            read_id,
            thread: P.into(),
            token: Some(token.claim.clone()),
            caller: StatusCaller::SubagentReconcile { token, op_id },
            result: StatusResult::Carrier("快汇总".into()),
        },
        &mut vec![],
    );
    assert!(m.runs.runs[R]
        .turns
        .iter()
        .any(|t| t.turn_id == "快汇总" && t.status == TurnStatus::Completed));
    assert_eq!(join(&m).reconciled_epoch, join(&m).work_epoch);
    assert!(matches!(m.claims[P].state, State::Converging { .. }));
}
#[test]
fn 无子代理时不建协调器_detach后再派生用当前授权代际启用() {
    let (mut m, mut mock) = setup();
    assert!(!m.sa1b_active(R), "没有子代理证据不启用");
    assert!(m.runs.runs[R].subagent_join.is_none());
    assert!(m.runs.runs[R].auto_turns_started.is_none());
    let out = apply(
        &mut m,
        Event::ClientRequest {
            request: ClientRequest {
                conn: 1,
                id: json!(98),
                method: "broker/threads/detach".into(),
                params: Some(json!({"threadId":P,"runId":R,"policy":"on-request"})),
            },
            run_id: None,
        },
    );
    assert!(
        matches!(m.claims[P].owner(), Some(Owner::Broker { .. })),
        "{out:?}"
    );
    assert!(!m.sa1b_active(R), "detach 本身不启用");
    spawn(&mut m, &mut mock, P, T, "子", "派生", "子轮");
    let token = m.convergences.runs[R].token.clone();
    assert_eq!(token.manage_gen, m.manage_context(R).unwrap().gen);
    assert!(m.convergence_token_current(&token), "启用时取当前代际");
    complete(&mut m, &mut mock, "子", "子轮", "completed");
    complete(&mut m, &mut mock, P, T, "completed");
    assert!(matches!(m.claims[P].state, State::Converging { .. }));
    discover(&mut m, true);
    assert_eq!(
        m.convergences.progress[R].reading,
        Some((BatchReadId(9), join(&m).work_epoch)),
        "收敛继续推进到读取子结果"
    );
}
#[test]
fn 按需启用时只发布运行中且未计时_不排期也不公布收敛期限() {
    let (mut m, mut mock) = setup();
    mock.register("子", Some(P), "v1", false, true);
    let (a, b) = SubagentMock::v1_spawn(P, T, "派生", "子", false);
    let mut out = apply(&mut m, a);
    out.extend(apply(&mut m, b));
    assert!(m.sa1b_active(R));
    assert_eq!(join(&m).phase, JoinPhase::Running);
    assert_eq!(join(&m).deadline_at, 0);
    assert_eq!(m.convergences.runs[R].deadline_mono_ms, u64::MAX);
    assert!(!out.iter().any(|a| matches!(
        a,
        Action::Schedule {
            kind: TimerKind::ConvergeDeadline { .. },
            ..
        }
    )));
    // 真实实测发现：启用中途曾广播「等待子代理＋期限」再改回运行中，CLI 显示了不存在的期限。
    for action in &out {
        if let Action::Send { message, .. } = action {
            if message["method"] == "broker/run/convergenceChanged" {
                assert_eq!(message["params"]["subagentJoin"]["phase"], "running");
                assert_eq!(message["params"]["subagentJoin"]["deadlineAt"], 0);
            }
        }
    }
    complete(&mut m, &mut mock, P, T, "completed");
    assert!(matches!(m.claims[P].state, State::Converging { .. }));
    assert!(join(&m).deadline_at > 0, "首次收敛门禁才开始计时");
    assert!(m.convergences.runs[R].deadline_mono_ms < u64::MAX);
}

#[test]
fn 启动回包最后到仍启用收敛且不复活父轮() {
    let (mut m, mut mock, context) = setup_pending();
    notify(
        &mut m,
        "turn/started",
        json!({"threadId":P,"turn":{"id":T,"status":"inProgress","items":[]}}),
    );
    assert!(matches!(
        m.claims[P].state,
        State::Starting {
            settling: false,
            ..
        }
    ));
    assert!(m.claims[P].owned_turns.contains(T));
    spawn(&mut m, &mut mock, P, T, "子", "派生", "子轮");
    assert!(m.sa1b_active(R));
    complete(&mut m, &mut mock, P, T, "completed");
    assert!(matches!(m.claims[P].state, State::Converging { .. }));
    assert!(m.tree_for_run(R).is_none());
    let deadline = m.convergences.runs[R].deadline_mono_ms;
    // 迟到的旧完成不能覆盖当前完成目标，启动回包只能清 pending。
    complete(&mut m, &mut mock, P, "旧父轮", "failed");
    apply(
        &mut m,
        Event::Reply {
            context,
            result: Ok(json!({"turn":{"id":T}})),
        },
    );
    assert!(matches!(m.claims[P].state, State::Converging { .. }));
    assert!(!m.claims[P].request_pending);
    assert_eq!(m.claims[P].turn_id(), Some(T));
    assert_eq!(m.convergences.runs[R].deadline_mono_ms, deadline);
    assert!(m.tree_for_run(R).is_none());
    complete(&mut m, &mut mock, "子", "子轮", "completed");
    discover(&mut m, true);
    assert_eq!(
        m.convergences.progress[R].reading,
        Some((BatchReadId(9), join(&m).work_epoch))
    );
}

#[test]
fn settling外部派生不启用而自有派生定案后启用() {
    for own_spawn in [false, true] {
        for spawn_completed in [false, true] {
            let (mut m, mut mock, context) = setup_pending_mode(true);
            let parent = if own_spawn { T } else { "外部X" };
            notify(
                &mut m,
                "turn/started",
                json!({"threadId":P,"turn":{"id":parent,"status":"inProgress","items":[]}}),
            );
            let (started, completed) = SubagentMock::v1_spawn(P, parent, "派生", "子", false);
            apply(&mut m, started);
            if spawn_completed {
                apply(&mut m, completed);
            }
            assert!(m.claims[P].settling());
            let key = m.relations.operations.keys().next().unwrap().clone();
            assert_eq!(m.relations.operations[&key].run_id, R);
            assert!(!m.op_confirmed(&key));
            assert!(!m.sa1b_active(R));
            if !own_spawn {
                notify(
                    &mut m,
                    "turn/started",
                    json!({"threadId":P,"turn":{"id":T,"status":"inProgress","items":[]}}),
                );
            }
            // started 已有证明，启动回包经 begin_settlement 同步定案，触发 confirm_waiting。
            apply(
                &mut m,
                Event::Reply {
                    context,
                    result: Ok(json!({"turn":{"id":T}})),
                },
            );
            assert!(!m.claims[P].settling());
            assert!(m.claims[P].owned_turns.contains(T));
            assert_eq!(m.run_subagent_evidence(R), own_spawn);
            assert_eq!(m.sa1b_active(R), own_spawn);
            if own_spawn {
                assert!(m.op_confirmed(&key));
                if !spawn_completed {
                    assert!(m.relations.contexts[R].in_flight_spawns.contains(&key));
                }
            } else {
                assert_eq!(
                    m.relations.operations[&key].outcome,
                    EvidenceOutcome::Unproven
                );
                assert_eq!(m.relations.operations[&key].run_id, R);
                complete(&mut m, &mut mock, P, T, "completed");
                assert!(!m.sa1b_active(R));
            }
        }
    }
}

#[test]
fn 自有v1在途派生立即启用() {
    let (mut m, _) = setup();
    let (started, _) = SubagentMock::v1_spawn(P, T, "派生", "子", false);
    apply(&mut m, started);
    let key = m.relations.operations.keys().next().unwrap();
    assert!(m.op_confirmed(key));
    assert!(m.relations.contexts[R].in_flight_spawns.contains(key));
    assert!(m.sa1b_active(R));
}

fn setup_initial_input(shared: bool) -> (Machine, SubagentMock, serde_json::Value) {
    let (mut m, mock, context) = setup_pending_mode(shared);
    let initial = json!({"id":"初始条目","type":"userMessage","clientId":context.params.as_ref().unwrap()["clientUserMessageId"],"content":[{"type":"text","text":"任务"}]});
    assert!(initial["clientId"].is_string());
    notify(
        &mut m,
        "turn/started",
        json!({"threadId":P,"turn":{"id":T,"status":"inProgress"}}),
    );
    apply(
        &mut m,
        Event::Reply {
            context,
            result: Ok(json!({"turn":{"id":T}})),
        },
    );
    (m, mock, initial)
}

fn first_turn_input_veto(entrance: usize) {
    for shared in [false, true] {
        let (mut m, mut mock, initial) = setup_initial_input(shared);
        assert!(!m.sa1b_active(R));
        assert!(m.loop_guards.contains_key(R));
        notify(
            &mut m,
            "thread/goal/updated",
            json!({"threadId":P,"goal":{"status":"active","objective":"任务","createdAt":1}}),
        );
        m.record(P).goal_controlled = true;
        let control_epoch = m.records[P].goal_control_epoch;
        // 文本与初始请求相同也不能豁免第二条输入；只信任原请求的 clientId。
        let extra = json!({"id":"追加条目","type":"userMessage","clientId":"外部输入","content":[{"type":"text","text":"任务"}]});
        if entrance == 0 {
            for method in ["item/started", "item/completed"] {
                notify(
                    &mut m,
                    method,
                    json!({"threadId":P,"turnId":T,"item":initial}),
                );
            }
            assert!(!m.loop_guards[R].user_input_seen());
            notify(
                &mut m,
                "item/started",
                json!({"threadId":P,"turnId":T,"item":extra}),
            );
        } else if entrance == 1 {
            notify(
                &mut m,
                "turn/completed",
                json!({"threadId":P,"turn":{"id":T,"status":"completed","itemsView":"full","items":[initial,extra]}}),
            );
        } else {
            let out = notify(
                &mut m,
                "turn/completed",
                json!({"threadId":P,"turn":{"id":T,"status":"completed","itemsView":"notLoaded","items":[]}}),
            );
            let context = out
                .into_iter()
                .find_map(|a| match a {
                    Action::Request(c)
                        if matches!(c.operation, crate::claim::Operation::ResultRead(_)) =>
                    {
                        Some(c)
                    }
                    _ => None,
                })
                .expect("首轮输出触发真实条目分页");
            apply(
                &mut m,
                Event::Reply {
                    context,
                    result: Ok(crate::test_support::status_read::items_page(
                        T,
                        &[initial, extra],
                        Some("下一页"),
                    )),
                },
            );
        }
        assert!(m.loop_guards[R].user_input_seen(), "入口 {entrance}");
        assert!(
            m.records[P].goal_controlled,
            "被动记录不能撤销旧共享控制资格"
        );
        assert_eq!(m.records[P].goal_control_epoch, control_epoch);
        assert_eq!(m.loop_guards[R].continuations, 0);
        spawn(&mut m, &mut mock, P, T, "子", "派生", "子轮");
        assert!(m.run_subagent_evidence(R));
        assert!(!m.sa1b_active(R));
        if entrance == 0 {
            complete(&mut m, &mut mock, P, T, "completed");
        }
        notify(&mut m, "thread/goal/cleared", json!({"threadId":P}));
        assert!(m.tree_for_run(R).is_some(), "自然结束交 SA1a 停树");
        assert!(m.runs.runs[R].subagent_join.is_none());
        assert_eq!(
            m.trees.operations[&m.tree_for_run(R).unwrap()]
                .request
                .trigger,
            crate::stop::tree::TreeTrigger::ParentFinishedEarly
        );
    }
}

#[test]
fn 首轮真实插话通知阻止派生启用并交sa1a() {
    first_turn_input_veto(0);
}
#[test]
fn 首轮真实插话全文阻止派生启用并交sa1a() {
    first_turn_input_veto(1);
}
#[test]
fn 首轮真实插话分页前缀阻止派生启用并交sa1a() {
    first_turn_input_veto(2);
}

#[test]
fn 首轮仅初始输入重复或全文回放仍可按需启用() {
    for shared in [false, true] {
        let (mut m, mut mock, initial) = setup_initial_input(shared);
        for method in ["item/started", "item/completed"] {
            notify(
                &mut m,
                method,
                json!({"threadId":P,"turnId":T,"item":initial}),
            );
        }
        assert!(!m.loop_guards[R].user_input_seen());
        spawn(&mut m, &mut mock, P, T, "子", "派生", "子轮");
        assert!(m.sa1b_active(R));
        notify(
            &mut m,
            "turn/completed",
            json!({"threadId":P,"turn":{"id":T,"status":"completed","itemsView":"full","items":[initial]}}),
        );
        assert!(matches!(m.claims[P].state, State::Converging { .. }));
        assert!(!join(&m).automatic_control_revoked);
        assert!(!m.loop_guards[R].user_input_seen());
    }
}

fn setup_missing_budget() -> (
    Machine,
    SubagentMock,
    crate::runs::budget::BudgetVerification,
) {
    let (mut m, mut mock) = setup();
    m.convergences.test_budget = None;
    notify(
        &mut m,
        "thread/goal/updated",
        json!({"threadId":P,"goal":{"id":"预算goal","status":"active","tokenBudget":100000,"tokensUsed":10}}),
    );
    mock.register("预算子", Some(P), "v1", false, true);
    let (started, completed) = SubagentMock::v1_spawn(P, T, "预算派生", "预算子", false);
    let out = apply(&mut m, started);
    let verification = out
        .iter()
        .find_map(|a| match a {
            Action::Request(c) => match &c.operation {
                crate::claim::Operation::GoalRead { inner, .. } => match inner.as_ref() {
                    crate::claim::Operation::BudgetVerify { verification } => {
                        Some(verification.clone())
                    }
                    _ => None,
                },
                crate::claim::Operation::BudgetVerify { verification } => {
                    Some(verification.clone())
                }
                _ => None,
            },
            _ => None,
        })
        .expect("真实预算缺口登记并领取一次核实请求");
    assert!(out.iter().any(|a| matches!(
        a,
        Action::Schedule {
            kind: TimerKind::BudgetVerifyDeadline { .. },
            delay_ms: 20_000,
            ..
        }
    )));
    apply(&mut m, completed);
    apply(&mut m, mock.turn_started("预算子", "预算子轮"));
    notify(&mut m, "thread/goal/cleared", json!({"threadId":"预算子"}));
    assert_eq!(
        m.reconcile_budget_admission(&verification.token),
        BudgetAdmission::NeedsVerification
    );
    assert!(m.take_budget_verification(&verification.token).is_none());
    (m, mock, verification)
}

#[test]
fn 预算到期不经合并goal准入中断工作父轮() {
    let (mut m, mut mock, verification) = setup_missing_budget();
    complete(&mut m, &mut mock, "预算子", "预算子轮", "completed");
    complete(&mut m, &mut mock, P, T, "completed");
    notify(
        &mut m,
        "turn/started",
        json!({"threadId":P,"turn":{"id":"工作goal轮","status":"inProgress","items":[]}}),
    );
    assert_eq!(
        m.reconcile_goal_target(&verification.token).as_deref(),
        Some("工作goal轮")
    );
    discover(&mut m, true);
    assert!(crate::relations::convergence::settled(
        &m.convergence_facts(R).unwrap()
    ));
    for _ in 0..2 {
        retry(&mut m);
        discover(&mut m, true);
    }
    assert!(m.clock.mono_ms >= verification.deadline_mono_ms);
    assert!(m.budget_verification_expired(&verification.token));
    assert!(matches!(m.claims[P].state, State::Running { .. }));
    assert_eq!(join(&m).terminal_reason, None);
    assert!(m.tree_for_run(R).is_none());
    assert!(m.convergences.progress[R].reading.is_none());
    assert!(m.convergences.progress[R].operations.is_empty());
    m.convergence_timer(
        TimerKind::BudgetVerifyDeadline {
            token: verification.token,
        },
        &mut vec![],
    );
    assert!(matches!(m.claims[P].state, State::Running { .. }));
    assert!(!join(&m).automatic_control_revoked);
}

#[test]
fn 工作期间预算到期在进入收敛时补触发不等活动子树或发现() {
    for child_running in [true, false] {
        let (mut m, mut mock, verification) = setup_missing_budget();
        if !child_running {
            complete(&mut m, &mut mock, "预算子", "预算子轮", "completed");
        }
        notify(&mut m, "thread/goal/cleared", json!({"threadId":P}));
        assert!(matches!(m.claims[P].state, State::Running { .. }));
        m.clock.mono_ms = verification.deadline_mono_ms;
        m.convergence_timer(
            TimerKind::BudgetVerifyDeadline {
                token: verification.token.clone(),
            },
            &mut vec![],
        );
        assert_eq!(join(&m).terminal_reason, None);
        assert!(m.tree_for_run(R).is_none());
        assert!(m.take_budget_verification(&verification.token).is_none());
        // 定时器已消费且不会重排；父轮稍后完成时，发现尚未覆盖，子轮也可以仍在工作。
        m.clock.mono_ms += 123;
        let facts = m.convergence_facts(R).unwrap();
        assert_eq!(
            crate::relations::convergence::settled(&facts),
            !child_running
        );
        assert_ne!(facts.discovery_covered_epoch, Some(facts.work_epoch));
        complete(&mut m, &mut mock, P, T, "completed");
        assert_eq!(
            join(&m).terminal_reason,
            Some(TerminalReason::BudgetIncomplete)
        );
        assert!(m.clock.mono_ms < m.convergences.runs[R].deadline_mono_ms);
        assert!(join(&m).automatic_control_revoked);
        assert!(m.tree_for_run(R).is_some());
        assert!(m.convergences.progress[R].reading.is_none());
        assert!(m.convergences.progress[R].operations.is_empty());
    }
}

#[test]
fn 收敛等待未到预算原截止不终止且到期不依赖发现完成() {
    let (mut m, mut mock, verification) = setup_missing_budget();
    notify(&mut m, "thread/goal/cleared", json!({"threadId":P}));
    m.clock.mono_ms = verification.deadline_mono_ms - 1;
    complete(&mut m, &mut mock, P, T, "completed");
    assert!(matches!(m.claims[P].state, State::Converging { .. }));
    assert!(m.convergences.progress[R].discovery.is_some());
    assert_eq!(join(&m).terminal_reason, None);
    assert!(m.tree_for_run(R).is_none());
    m.clock.mono_ms = verification.deadline_mono_ms;
    m.convergence_timer(
        TimerKind::BudgetVerifyDeadline {
            token: verification.token,
        },
        &mut vec![],
    );
    assert_eq!(
        join(&m).terminal_reason,
        Some(TerminalReason::BudgetIncomplete)
    );
    assert!(m.tree_for_run(R).is_some());
}
#[test]
fn 无进展到期_收敛中父轮已结束按absent走收敛失败且不无限重排() {
    let (mut m, mut mock) = setup();
    spawn(&mut m, &mut mock, P, T, "C1", "spawn-c1", "c1-t1");
    complete(&mut m, &mut mock, P, T, "completed");
    assert!(matches!(m.claims[P].state, State::Converging { .. }));
    m.runs.runs.get_mut(R).unwrap().run_policy = Some(crate::runs::RunPolicy {
        stall_timeout_ms: 3_000,
        upstream_error_policy: crate::runs::UpstreamErrorPolicy::Observe,
    });
    m.stall_arm(R, &mut vec![]);
    m.clock.mono_ms += 3_000;
    let generation = m.runs.stall_states[R].generation;
    let mut actions = vec![];
    m.stall_timer(R, generation, &mut actions);
    // 历史 currentTurnId 是已完成父轮：不读取、直接 absent 并停止，不重排。
    assert!(!actions
        .iter()
        .any(|a| matches!(a, Action::Request(c) if c.method == "thread/turns/list")));
    let report = m.runs.runs[R].stall.clone().unwrap();
    assert_eq!(report.verify, Some(crate::runs::StallVerify::Absent));
    assert_eq!(report.phase, crate::runs::StallPhase::Stopping);
    assert_eq!(join(&m).terminal_reason, Some(TerminalReason::StallTimeout));
}
