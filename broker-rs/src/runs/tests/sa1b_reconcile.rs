//! 最终父轮选择与旧结果补读隔离；复用本路事实及 C 接口桩。
use super::*;
use crate::claim::subagent_reconcile::tests::{
    carrier, completed, fixture, own_start, start, R, T,
};
use crate::sa1b::*;

#[test]
fn 旧完成晚于新汇总仍选择新父轮() {
    let (mut m, token) = fixture(1);
    let (id, _) = start(&mut m, &token);
    own_start(&mut m, &token, "新汇总", TurnOrigin::SubagentReconcile);
    carrier(&mut m, &token, id, "新汇总");
    completed(&mut m, "新汇总", "completed", "真正最终答复");
    completed(&mut m, "原轮", "failed", "迟到旧答复");
    assert_eq!(m.runs.runs[R].turns.last().unwrap().turn_id, "原轮");
    m.run_released(R, ReleaseKind::Natural, &mut vec![]);
    let r = &m.runs.runs[R];
    assert_eq!(r.status, Some(RunStatus::Completed));
    assert_eq!(r.final_turn_id.as_deref(), Some("新汇总"));
    assert_eq!(r.selected_parent_result().unwrap().output, "真正最终答复");
}
#[test]
fn 旧分页补读在新汇总结束后只更新旧轮() {
    let (mut m, token) = fixture(1);
    let mut a = vec![];
    m.run_turn_completed(
        T,
        Some(R),
        &json!({"id":"原轮","status":"completed","items":[],"itemsView":"notLoaded"}),
        &mut a,
    );
    let read = a
        .iter()
        .find_map(|a| match a {
            Action::Request(r) => {
                if let Operation::ResultRead(read) = &r.operation {
                    Some(read.clone())
                } else {
                    None
                }
            }
            _ => None,
        })
        .unwrap();
    let (id, _) = start(&mut m, &token);
    own_start(&mut m, &token, "新汇总", TurnOrigin::SubagentReconcile);
    carrier(&mut m, &token, id, "新汇总");
    completed(&mut m, "新汇总", "completed", "最新输出");
    m.run_released(R, ReleaseKind::Natural, &mut vec![]);
    m.run_result_reply(read, Ok(json!({"data":[{"turnId":"原轮","item":{"type":"agentMessage","text":"补读旧文本","phase":"final_answer"}}],"nextCursor":null})), &mut vec![]);
    let r = &m.runs.runs[R];
    assert_eq!(
        r.turns.iter().find(|t| t.turn_id == "原轮").unwrap().output,
        "补读旧文本"
    );
    assert_eq!(r.final_turn_id.as_deref(), Some("新汇总"));
    assert_eq!(r.selected_parent_result().unwrap().output, "最新输出");
}
#[test]
fn 最终指针缺失对应结果不得回退旧completed() {
    let (mut m, _) = fixture(1);
    completed(&mut m, "原轮", "completed", "旧文本");
    let r = m.runs.runs.get_mut(R).unwrap();
    r.final_turn_id = Some("待补读新轮".into());
    r.state = RunState::Ended;
    assert!(r.selected_parent_result().is_none());
    r.final_turn_id = None;
    assert!(r.selected_parent_result().is_none());
    r.subagent_join = None;
    assert_eq!(r.selected_parent_result().unwrap().output, "旧文本");
}
#[test]
fn full与分页都先分类且同轮真实输入仍保留() {
    for paged in [false, true] {
        for real in [false, true] {
            let (mut m, token) = fixture(1);
            let (_, request) = start(&mut m, &token);
            own_start(&mut m, &token, "汇总轮", TurnOrigin::SubagentReconcile);
            let client_id = request.params.unwrap()["clientUserMessageId"].clone();
            let mut items = vec![
                json!({"type":"userMessage","clientId":client_id,"content":[]}),
                json!({"type":"agentMessage","phase":"final_answer","text":"答案"}),
            ];
            if real {
                items.push(json!({"type":"userMessage","clientId":"真实用户","content":[]}));
            }
            if paged {
                let mut a = vec![];
                m.run_turn_completed(
                    T,
                    Some(R),
                    &json!({"id":"汇总轮","status":"completed","itemsView":"notLoaded","items":[]}),
                    &mut a,
                );
                let read = a
                    .iter()
                    .find_map(|a| match a {
                        Action::Request(r) => {
                            if let Operation::ResultRead(read) = &r.operation {
                                Some(read.clone())
                            } else {
                                None
                            }
                        }
                        _ => None,
                    })
                    .unwrap();
                m.run_result_reply(
                    read,
                    Ok(crate::test_support::status_read::items_page(
                        "汇总轮",
                        &items,
                        None,
                    )),
                    &mut vec![],
                );
            } else {
                m.run_turn_completed(
                    T,
                    Some(R),
                    &json!({"id":"汇总轮","status":"completed","itemsView":"full","items":items}),
                    &mut vec![],
                );
            }
            let activity = m.runs.guard_evidence.last().unwrap().2.unwrap();
            assert_eq!(activity.user_input, real);
            assert!(!activity.action);
        }
    }
}

#[test]
fn 普通释放与树提交都使用相同终态决定且保留真实报告() {
    use crate::stop::tree::{
        ChildrenStopReport, ReleaseSnapshot, TreeFinishCommit, TreeStopReport,
    };
    for (reason, status, interrupt) in [
        (
            TerminalReason::WakeUnknown,
            RunStatus::Interrupted,
            Some(InterruptReason::WakeUnknown),
        ),
        (
            TerminalReason::BudgetExhausted,
            RunStatus::GoalBlocked,
            Some(InterruptReason::BudgetExhausted),
        ),
        (TerminalReason::ParentFailed, RunStatus::Failed, None),
    ] {
        let (mut original, _) = fixture(1);
        original
            .runs
            .runs
            .get_mut(R)
            .unwrap()
            .subagent_join
            .as_mut()
            .unwrap()
            .terminal_reason = Some(reason);
        original
            .runs
            .reconciliation
            .harness
            .as_mut()
            .unwrap()
            .terminal = Some(TerminalDisposition {
            status,
            interrupt_reason: interrupt,
            terminal_reason: reason,
            tree_trigger: None,
            exit_code: 4,
        });
        let mut ordinary = original.clone();
        ordinary.run_released(R, ReleaseKind::Natural, &mut vec![]);
        let snapshot = ReleaseSnapshot {
            identity: original.manage_context(R).unwrap().identity.clone(),
            token: None,
            owner: None,
            gen: 1,
            parent_turn_id: Some("原轮".into()),
            parent_status: Some(TurnStatus::Completed),
            goal_status: None,
            loop_guard: false,
        };
        let children: ChildrenStopReport = serde_json::from_value(json!({
            "trigger":"userStop","phase":"unconfirmed","version":1,
            "root":{"thread":T,"phase":"unconfirmed","check":{"ownership":true,"turnEnded":false,"goalBraked":true,"settled":false,"verifyDone":false},"clearRequested":false,"clearConfirmed":false,"error":null},
            "stopped":[],"unconfirmed":[T],"failed":[],"conflicts":[],"capabilityLimited":[],
            "discovery":{"missing":{},"complete":false,"version":1,"incompleteReasons":["待核实"]},"unsettledSpawns":0
        })).unwrap();
        original.commit_tree_finish(
            TreeFinishCommit {
                snapshot,
                report: TreeStopReport {
                    tree_op: 1,
                    run_id: R.into(),
                    generation: 1,
                    round: 1,
                    children,
                    first_delivery_deadline_ms: 20_000,
                    committed: false,
                    cancelled: false,
                    barrier: true,
                },
                status: RunStatus::Completed,
                interrupt_reason: None,
            },
            &mut vec![],
        );
        assert_eq!(original.runs.runs[R].status, ordinary.runs.runs[R].status);
        assert_eq!(original.runs.runs[R].status, Some(status));
        assert_eq!(original.runs.runs[R].interrupt_reason, interrupt);
        assert_eq!(
            original.runs.runs[R].stop_outcome,
            Some(StopOutcome::Unconfirmed)
        );
        assert!(ordinary.runs.runs[R].children_stop.is_none());
    }
}

#[test]
fn 父轮未结束停树到期提交不建立缺失最终目标() {
    use crate::stop::tree::{
        ChildrenStopReport, ReleaseSnapshot, TreeFinishCommit, TreeStopReport,
    };
    let (mut m, token) = fixture(1);
    own_start(&mut m, &token, "未完成父轮", TurnOrigin::Goal);
    m.runs
        .runs
        .get_mut(R)
        .unwrap()
        .subagent_join
        .as_mut()
        .unwrap()
        .terminal_reason = Some(TerminalReason::ConvergeDeadline);
    let snapshot = ReleaseSnapshot {
        identity: m.manage_context(R).unwrap().identity.clone(),
        token: None,
        owner: None,
        gen: 1,
        parent_turn_id: Some("未完成父轮".into()),
        parent_status: None,
        goal_status: None,
        loop_guard: false,
    };
    let children: ChildrenStopReport = serde_json::from_value(json!({
        "trigger":"convergeDeadline","phase":"unconfirmed","version":1,
        "root":{"thread":T,"phase":"unconfirmed","check":{"ownership":true,"turnEnded":false,"goalBraked":true,"settled":false,"verifyDone":false},"clearRequested":false,"clearConfirmed":false,"error":null},
        "stopped":[],"unconfirmed":[T],"failed":[],"conflicts":[],"capabilityLimited":[],
        "discovery":{"missing":{},"complete":true,"version":1,"incompleteReasons":[]},"unsettledSpawns":0
    })).unwrap();
    m.commit_tree_finish(
        TreeFinishCommit {
            snapshot,
            report: TreeStopReport {
                tree_op: 1,
                run_id: R.into(),
                generation: 1,
                round: 1,
                children,
                first_delivery_deadline_ms: 20_000,
                committed: false,
                cancelled: false,
                barrier: true,
            },
            status: RunStatus::Interrupted,
            interrupt_reason: Some(InterruptReason::ConvergeDeadline),
        },
        &mut vec![],
    );
    let run = &m.runs.runs[R];
    assert_eq!(run.state, RunState::Ended);
    assert_eq!(run.status, Some(RunStatus::Interrupted));
    assert_eq!(run.stop_outcome, Some(StopOutcome::Unconfirmed));
    assert_eq!(run.current_turn_id.as_deref(), Some("未完成父轮"));
    assert!(run.turns.is_empty(), "不能伪造完成轮、状态或轮次计数");
    assert_eq!(run.turns_total, 0);
    assert!(
        run.final_turn_id.is_none(),
        "没有完成证据时不能承诺未来会出现最终目标"
    );
    assert!(run.selected_parent_result().is_none());
    m.finalize_runs(&mut vec![]);
    assert!(m.runs.runs[R].final_turn_id.is_none());
}

#[test]
fn 未知外部轮完成不能通过fallback进入父运行() {
    let (mut m, token) = fixture(1);
    let (id, _) = start(&mut m, &token);
    completed(&mut m, "外部轮", "completed", "外部答复");
    assert!(m.runs.runs[R].turns.is_empty());
    carrier(&mut m, &token, id, "外部轮");
    assert!(m.runs.runs[R].turns.is_empty());
    assert_eq!(m.runs.runs[R].current_turn_id.as_deref(), Some("原轮"));
}

#[test]
fn 已证明自有但完成先到仍推进当前轮与最终指针() {
    let (mut m, token) = fixture(1);
    let (id, _) = start(&mut m, &token);
    m.claims
        .get_mut(T)
        .unwrap()
        .owned_turns
        .insert("先完成轮".into());
    completed(&mut m, "先完成轮", "completed", "输出先到");
    own_start(&mut m, &token, "先完成轮", TurnOrigin::SubagentReconcile);
    carrier(&mut m, &token, id, "先完成轮");
    m.run_released(R, ReleaseKind::Natural, &mut vec![]);
    assert_eq!(m.runs.runs[R].current_turn_id.as_deref(), Some("先完成轮"));
    assert_eq!(m.runs.runs[R].final_turn_id.as_deref(), Some("先完成轮"));
}
#[test]
fn 汇总后自有goal轮是最终父轮且旧开始通知不回退顺序() {
    let (mut m, token) = fixture(1);
    let (id, _) = start(&mut m, &token);
    own_start(&mut m, &token, "汇总", TurnOrigin::SubagentReconcile);
    carrier(&mut m, &token, id, "汇总");
    completed(&mut m, "汇总", "completed", "阶段答复");
    own_start(&mut m, &token, "后续goal", TurnOrigin::Goal);
    completed(&mut m, "后续goal", "completed", "最终答复");
    own_start(&mut m, &token, "原轮", TurnOrigin::User);
    m.run_released(R, ReleaseKind::Natural, &mut vec![]);
    assert_eq!(m.runs.runs[R].current_turn_id.as_deref(), Some("后续goal"));
    assert_eq!(m.runs.runs[R].final_turn_id.as_deref(), Some("后续goal"));
}

#[test]
fn 首采样final早于client_id核实时完成壳隔离后补读() {
    use crate::claim::status_read::{StatusCaller, StatusDone, StatusResult};
    for real_user in [false, true] {
        let (mut m, token) = fixture(1);
        let (id, request) = start(&mut m, &token);
        m.reconcile_observe_parent_started(&token, "快完成轮");
        let mut items = vec![
            json!({"type":"userMessage","clientId":request.params.unwrap()["clientUserMessageId"],"content":[]}),
            json!({"type":"agentMessage","phase":"final_answer","text":"最终快答复"}),
        ];
        if real_user {
            items.push(json!({"type":"userMessage","clientId":"真实输入","content":[]}));
        }
        m.run_turn_completed(
            T,
            Some(R),
            &json!({"id":"快完成轮","status":"completed","itemsView":"full","items":items}),
            &mut vec![],
        );
        assert!(m.runs.runs[R].turns.is_empty());
        assert!(!m.claims[T].owned_turns.contains("快完成轮"));
        let read_id = m.runs.reconciliation.operations[&id.0].status_read.unwrap();
        let mut a = vec![];
        m.subagent_reconcile_status_done(
            StatusDone {
                read_id,
                thread: T.into(),
                token: Some(token.claim.clone()),
                caller: StatusCaller::SubagentReconcile {
                    token: token.clone(),
                    op_id: id,
                },
                result: StatusResult::Carrier("快完成轮".into()),
            },
            &mut a,
        );
        if real_user {
            assert!(m.runs.runs[R].turns.is_empty());
            assert!(!m.claims[T].owned_turns.contains("快完成轮"));
            continue;
        }
        assert!(m.claims[T].owned_turns.contains("快完成轮"));
        assert_eq!(
            m.runs.runs[R]
                .subagent_join
                .as_ref()
                .unwrap()
                .reconciled_epoch,
            1
        );
        let read = a
            .iter()
            .find_map(|a| match a {
                Action::Request(r) => {
                    if let Operation::ResultRead(read) = &r.operation {
                        Some(read.clone())
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .unwrap();
        m.run_result_reply(
            read,
            Ok(crate::test_support::status_read::items_page(
                "快完成轮",
                &items,
                None,
            )),
            &mut vec![],
        );
        m.run_released(R, ReleaseKind::Natural, &mut vec![]);
        assert_eq!(m.runs.runs[R].final_turn_id.as_deref(), Some("快完成轮"));
        assert_eq!(
            m.runs.runs[R].selected_parent_result().unwrap().output,
            "最终快答复"
        );
    }
}
