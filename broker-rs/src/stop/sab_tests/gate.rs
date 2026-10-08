//! 结束门禁与专用提交：Hold、停树、状态与退出码依据、报告先于 ended、提交后只更新报告。
use super::*;
use crate::relations::RunIdentity;
use crate::runs::TurnStatus;
use crate::runs::{InterruptReason, RunState, RunStatus, StopOutcome};
use crate::stop::tree::{commit_status, ReleaseSnapshot, TreePhase, TreeTrigger};

#[test]
fn 父线程完全不等子代理_门禁暂缓并停树_运行标子代理未完成() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮1");
    assert!(s.bound("子甲", "子轮1"), "子线程首轮已绑定到运行");
    let start = s.log.len();
    s.hold.push(("turn/interrupt".into(), "子甲".into()));
    s.turn_completed(P, T, "completed");
    // 父轮已结束但子代理仍在跑：认领转入结束快照，运行保持 running，未发 run/ended。
    assert!(s.finishing(), "门禁 Hold 后认领处于 FinishingTree");
    assert_eq!(s.record().state, RunState::Running);
    assert_eq!(s.run_ended(&s.log[start..]), 0);
    assert!(s.tree_op().is_some(), "以 parentFinishedEarly 发起整树停止");
    assert_eq!(
        s.m.trees.operations[&s.tree_op().unwrap()].request.trigger,
        TreeTrigger::ParentFinishedEarly
    );
    // 放行中断：子线程停下后树确认、唯一一次提交。
    s.release("turn/interrupt", "子甲");
    assert!(s.tree_op().is_none(), "确认后树操作移除");
    assert!(!s.m.claims.contains_key(P), "提交后释放根认领");
    let record = s.record();
    assert_eq!(record.state, RunState::Ended);
    assert_eq!(record.status, Some(RunStatus::Interrupted));
    assert_eq!(
        record.interrupt_reason,
        Some(InterruptReason::SubagentsUnfinished)
    );
    let report = record.children_stop.as_ref().expect("有子代理停止报告");
    assert_eq!(report.phase, TreePhase::Confirmed);
    assert_eq!(report.stopped, vec!["子甲".to_owned()]);
    assert_eq!(record.stop_outcome, Some(StopOutcome::Confirmed));
    assert_eq!(s.run_ended(&s.log[start..]), 1, "只发一次 run/ended");
    // 父轮输出保留：父轮结果仍在运行记录里。
    assert!(record.turns.iter().any(|t| t.turn_id == T));
}

#[test]
fn 报告先于ended_写盘里没有已结束而无报告的记录() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮1");
    let start = s.log.len();
    s.turn_completed(P, T, "completed");
    let writes = s.run_writes(&s.log[start..]);
    let first_ended = writes
        .iter()
        .position(|r| r.state == RunState::Ended)
        .expect("有结束写盘");
    assert!(
        writes[first_ended].children_stop.is_some(),
        "首次 ended 写盘已带 childrenStop"
    );
    assert!(writes[first_ended].stop_outcome.is_some());
}

#[test]
fn 父线程只等一个子代理_只停仍在跑的那个() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.spawn_v1("子乙", "派生2", "子轮乙");
    s.turn_completed("子甲", "子轮甲", "completed");
    let start = s.log.len();
    s.turn_completed(P, T, "completed");
    let actions = s.log[start..].to_vec();
    assert_eq!(s.interrupts(&actions, "子甲"), 0, "已完成的子代理不中断");
    assert_eq!(s.interrupts(&actions, "子乙"), 1);
    let record = s.record();
    assert_eq!(
        record.interrupt_reason,
        Some(InterruptReason::SubagentsUnfinished)
    );
    let report = record.children_stop.as_ref().unwrap();
    assert_eq!(report.stopped, vec!["子乙".to_owned(), "子甲".to_owned()]);
}

#[test]
fn 子代理先全部完成_父线程结束不进门禁() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮1");
    s.turn_completed("子甲", "子轮1", "completed");
    let start = s.log.len();
    s.turn_completed(P, T, "completed");
    assert!(s.tree_op().is_none(), "没有受管活动不发起树停止");
    assert!(!s.m.claims.contains_key(P));
    let record = s.record();
    assert_eq!(record.status, Some(RunStatus::Completed));
    assert!(record.children_stop.is_none());
    assert_eq!(s.run_ended(&s.log[start..]), 1);
}

#[test]
fn goal自动续跑的中间轮不进门禁() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮1");
    s.hold.push(("turn/interrupt".into(), "子甲".into()));
    s.goals.insert(P.into(), goal(P, "active"));
    s.notify(json!({"method": "thread/goal/updated", "params": {"threadId": P, "goal": goal(P, "active")}}));
    s.turn_completed(P, T, "completed");
    assert!(s.tree_op().is_none(), "goal active 的正常父轮走自动续跑");
    assert!(matches!(
        s.m.claims[P].state,
        State::AwaitingContinuation { .. }
    ));
    // 续跑空窗里 goal 不再 active：此时才经过门禁并暂缓。
    s.notify(json!({"method": "thread/goal/updated", "params": {"threadId": P, "goal": goal(P, "complete")}}));
    assert!(
        s.finishing(),
        "两轮之间 goal 变为非 active 的释放同样过门禁"
    );
}

#[test]
fn 父轮失败带残留子树_先于goal分支暂缓且终态为failed() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮1");
    s.goals.insert(P.into(), goal(P, "active"));
    s.notify(json!({"method": "thread/goal/updated", "params": {"threadId": P, "goal": goal(P, "active")}}));
    s.hold.push(("turn/interrupt".into(), "子甲".into()));
    s.turn_completed(P, T, "failed");
    assert!(s.finishing(), "父轮 failed 的门禁先于 goal active 分支");
    let tree = s.tree_op().unwrap();
    assert_eq!(
        s.m.trees.operations[&tree].request.trigger,
        TreeTrigger::ParentFailed
    );
    // 根 goal 仍 active：Hold 阻断异常自动续跑，根上的 goal 被暂停。
    assert_eq!(s.goals[P]["status"], "paused");
    s.release("turn/interrupt", "子甲");
    let record = s.record();
    assert_eq!(record.status, Some(RunStatus::Failed));
    assert_eq!(record.state, RunState::Ended);
}

#[test]
fn 结束快照期间_迟到started与重复completed交协调器_不恢复running不重复释放() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮1");
    s.never.insert("子甲".into());
    s.turn_completed(P, T, "completed");
    assert!(s.finishing());
    // 重复 completed：不释放、不发 run/ended。
    let start = s.log.len();
    s.turn_completed(P, T, "completed");
    assert!(s.finishing());
    assert_eq!(s.run_ended(&s.log[start..]), 0);
    // 根上迟到开始的新轮次：不覆盖父 turn_id、不恢复 Running，被当作本运行的轮次中断。
    let start = s.log.len();
    s.turn_started(P, "根新轮");
    let State::FinishingTree { turn_id, .. } = &s.m.claims[P].state else {
        panic!("仍是结束快照");
    };
    assert_eq!(turn_id.as_deref(), Some(T));
    assert_eq!(s.interrupts(&s.log[start..], P), 1, "根上迟到轮次被中断");
}

#[test]
fn 结束快照期间detach与断开_仍由协调器提交() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮1");
    s.hold.push(("turn/interrupt".into(), "子甲".into()));
    s.turn_completed(P, T, "completed");
    assert!(s.finishing());
    // owner 断开：保留清理权，合并进同一次整树停止，不另起停止。
    let tree = s.tree_op().unwrap();
    s.apply(Event::Disconnected { conn: OWNER });
    assert_eq!(s.tree_op(), Some(tree));
    assert!(s.finishing());
    assert!(
        s.m.stops.operations.is_empty(),
        "不为结束快照另建单线程停止"
    );
    s.release("turn/interrupt", "子甲");
    assert_eq!(s.record().state, RunState::Ended);
    assert_eq!(
        s.record().interrupt_reason,
        Some(InterruptReason::SubagentsUnfinished)
    );
}

#[test]
fn 结束快照期间detach_owner转为托管仍等树提交() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮1");
    s.hold.push(("turn/interrupt".into(), "子甲".into()));
    s.turn_completed(P, T, "completed");
    let (id, actions) = s.call(
        OWNER,
        "broker/threads/detach",
        json!({"threadId": P, "runId": R, "policy": "never"}),
    );
    assert!(s.response(&actions, id).is_some(), "detach 有回包");
    assert!(s.finishing(), "detach 只改 owner");
    assert!(matches!(
        s.m.claims[P].owner(),
        Some(crate::claim::Owner::Broker { .. })
    ));
    s.release("turn/interrupt", "子甲");
    assert_eq!(s.record().state, RunState::Ended);
}

#[test]
fn 首次期限到期以unconfirmed提交_之后核实只更新报告不重复ended() {
    let mut s = Sim::new();
    s.call(2, "broker/threads/watch", json!({"threadId": P}));
    s.spawn_v1("子甲", "派生1", "子轮1");
    s.never.insert("子甲".into());
    let start = s.log.len();
    s.turn_completed(P, T, "completed");
    assert_eq!(s.record().state, RunState::Running);
    s.advance(40_000);
    let record = s.record().clone();
    assert_eq!(record.state, RunState::Ended, "独立首次期限到期即提交");
    assert_eq!(record.stop_outcome, Some(StopOutcome::Unconfirmed));
    let report = record.children_stop.clone().unwrap();
    assert_eq!(report.phase, TreePhase::Unconfirmed);
    assert_eq!(report.unconfirmed, vec!["子甲".to_owned()]);
    assert!(record.stop_check.is_some_and(|c| !c.turn_ended));
    assert_eq!(s.run_ended(&s.log[start..]), 1);
    assert!(s.tree_op().is_some(), "清理与屏障继续");
    // 子线程终于停下：只更新报告与 childChanged，不再发 run/ended。
    s.never.clear();
    let before = s.log.len();
    s.advance(15_000);
    let record = s.record();
    assert_eq!(
        record.children_stop.as_ref().unwrap().phase,
        TreePhase::Confirmed
    );
    assert!(record.children_stop.as_ref().unwrap().version > report.version);
    assert_eq!(record.stop_outcome, Some(StopOutcome::Confirmed));
    assert_eq!(record.status, Some(RunStatus::Interrupted));
    assert_eq!(s.run_ended(&s.log[before..]), 0);
    assert!(s.log[before..].iter().any(|a| matches!(a, Action::Send { message, .. }
        if message["method"] == "broker/run/childChanged" && message["params"]["stopPhase"] == "confirmed")));
    assert!(s.tree_op().is_none());
}

#[test]
fn 终态优先级_父失败_父中断_循环刹车_树原因() {
    let snapshot = |status: Option<TurnStatus>, guard: bool| ReleaseSnapshot {
        identity: RunIdentity {
            run_id: R.into(),
            logical_root_thread_id: P.into(),
            execution_thread_id: P.into(),
        },
        token: None,
        owner: None,
        gen: 0,
        parent_turn_id: None,
        parent_status: status,
        goal_status: None,
        loop_guard: guard,
    };
    let early = TreeTrigger::ParentFinishedEarly;
    assert_eq!(
        commit_status(&snapshot(Some(TurnStatus::Failed), true), early, None),
        (RunStatus::Failed, None)
    );
    assert_eq!(
        commit_status(
            &snapshot(Some(TurnStatus::Interrupted), true),
            TreeTrigger::UserStop,
            Some(InterruptReason::Stop)
        ),
        (RunStatus::Interrupted, Some(InterruptReason::Stop))
    );
    assert_eq!(
        commit_status(&snapshot(Some(TurnStatus::Interrupted), false), early, None),
        (RunStatus::Interrupted, None),
        "父 interrupted 无原因则 null"
    );
    assert_eq!(
        commit_status(&snapshot(Some(TurnStatus::Completed), true), early, None),
        (RunStatus::Interrupted, Some(InterruptReason::GoalLoop)),
        "有 loopGuard 优先于树原因"
    );
    assert_eq!(
        commit_status(&snapshot(Some(TurnStatus::Completed), false), early, None),
        (
            RunStatus::Interrupted,
            Some(InterruptReason::SubagentsUnfinished)
        )
    );
    assert_eq!(
        commit_status(&snapshot(None, false), TreeTrigger::OwnerGone, None),
        (
            RunStatus::Interrupted,
            Some(InterruptReason::ClientDisconnected)
        )
    );
    assert_eq!(
        commit_status(&snapshot(None, false), TreeTrigger::UserStop, None),
        (RunStatus::Interrupted, Some(InterruptReason::Stop))
    );
}

#[test]
fn 门禁对未知活动同样暂缓() {
    let mut s = Sim::new();
    // 运行已有子代理上下文（子线程已完成），之后来路不明的线程先发审批：回查未决、可能属于本运行 → 活动未知。
    s.spawn_v1("子甲", "派生1", "子轮1");
    s.turn_completed("子甲", "子轮1", "completed");
    s.hold.push(("thread/read".into(), "陌生线程".into()));
    let (_, request) = s.mock.request(
        "陌生线程",
        Some("陌生轮"),
        None,
        "item/commandExecution/requestApproval",
    );
    s.apply(request);
    assert_eq!(
        s.m.has_managed_activity(R),
        crate::relations::Activity::Unknown
    );
    s.turn_completed(P, T, "completed");
    assert!(s.finishing(), "unknown 与 some 一样 Hold");
}

#[test]
fn joined运行结束只释放不暂缓() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮1");
    let (id, actions) = s.call(OWNER, "broker/joined", json!({"threadId": P, "turnId": T}));
    assert_eq!(s.response(&actions, id).unwrap()["result"]["joined"], true);
    s.turn_completed(P, T, "completed");
    assert!(s.tree_op().is_none(), "joined 只撤权，不停树");
    assert!(!s.m.claims.contains_key(P));
}

#[test]
fn 树提交ended时按run撤销子请求与收件箱事项_不答不dismiss() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮1");
    s.hold.push(("turn/interrupt".into(), "子甲".into()));
    let (request, event) = s.mock.request(
        "子甲",
        Some("子轮1"),
        None,
        "item/commandExecution/requestApproval",
    );
    let actions = s.apply(event);
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::Send { conn, message }
            if *conn == OWNER && message["method"] == "item/commandExecution/requestApproval")),
        "运行中的子线程审批转给 owner"
    );
    s.turn_completed(P, T, "completed");
    assert!(s.finishing(), "待答的子请求算受管活动");
    // 停止开始：已转发的子请求撤回，按清理策略进收件箱，不再转给旧 owner。
    assert!(s.m.requests.pending.is_empty());
    assert_eq!(s.m.inbox_pending_run(R), 1);
    s.release("turn/interrupt", "子甲");
    assert_eq!(s.record().state, RunState::Ended);
    assert!(
        s.m.relations.requests.is_empty(),
        "提交 ended 时按 run 撤销"
    );
    assert_eq!(s.m.inbox_pending_run(R), 0);
    assert_eq!(s.mock.answer_count("子甲", &request), 0, "不答、不 Dismiss");
}
