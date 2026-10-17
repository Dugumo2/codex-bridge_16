use super::*;

fn reasons() -> [(StopReason, InterruptReason); 2] {
    [
        (StopReason::StallTimeout, InterruptReason::StallTimeout),
        (
            StopReason::UpstreamAccessError,
            InterruptReason::UpstreamAccessError,
        ),
    ]
}

fn finish_parent(m: &mut Machine, run: &str, thread: &str, status: &str) {
    m.run_turn_completed(
        thread,
        Some(run),
        &json!({"id":"父轮","status":status,"items":[message("输出")]}),
        &mut Vec::new(),
    );
}

fn children_stop() -> crate::stop::tree::ChildrenStopReport {
    serde_json::from_value(json!({
        "trigger":"stallTimeout","phase":"confirmed","version":1,
        "root":{"thread":"线程","phase":"confirmed",
            "check":{"ownership":true,"turnEnded":true,"goalBraked":true,"settled":true,"verifyDone":true},
            "clearRequested":false,"clearConfirmed":false,"error":null},
        "stopped":[],"unconfirmed":[],"failed":[],"conflicts":[],"capabilityLimited":[],
        "discovery":{"missing":{},"complete":true,"version":1,"incompleteReasons":[]},
        "unsettledSpawns":0
    })).unwrap()
}

#[test]
fn 新停止原因在目标受阻前采用真实父轮失败或中断() {
    for (reason, interrupt) in reasons() {
        for (status, expected) in [
            ("failed", RunStatus::Failed),
            ("interrupted", RunStatus::Interrupted),
        ] {
            for goal in [
                None,
                Some("blocked"),
                Some("budgetLimited"),
                Some("usageLimited"),
            ] {
                let mut m = machine();
                open(&mut m, "运行", "线程");
                if let Some(goal) = goal {
                    m.run_goal_status("线程", Some(goal));
                }
                m.run_stop_began("运行", reason, &mut Vec::new());
                finish_parent(&mut m, "运行", "线程", status);
                m.run_stop_confirmed("运行", &mut Vec::new());
                m.run_released("运行", ReleaseKind::StopConfirmed, &mut Vec::new());
                let run = &m.runs.runs["运行"];
                assert_eq!(run.status, Some(expected), "{reason:?}/{status}/{goal:?}");
                assert_eq!(run.interrupt_reason, Some(interrupt));
            }
        }
    }
}

#[test]
fn 新停止原因正常完成例外只适用于未启用收敛的单线程确认停止() {
    for (reason, interrupt) in reasons() {
        for goal_active in [false, true] {
            for completed_before in [false, true] {
                for tree in [false, true] {
                    for kind in [ReleaseKind::Natural, ReleaseKind::StopConfirmed] {
                        let mut m = machine();
                        open(&mut m, "运行", "线程");
                        m.run_goal_status(
                            "线程",
                            Some(if goal_active { "active" } else { "paused" }),
                        );
                        if completed_before {
                            finish_parent(&mut m, "运行", "线程", "completed");
                        }
                        m.run_stop_began("运行", reason, &mut Vec::new());
                        if !completed_before {
                            finish_parent(&mut m, "运行", "线程", "completed");
                        }
                        if tree {
                            m.runs.runs.get_mut("运行").unwrap().children_stop =
                                Some(children_stop());
                        }
                        if kind == ReleaseKind::StopConfirmed {
                            m.run_stop_confirmed("运行", &mut Vec::new());
                        }
                        m.run_released("运行", kind, &mut Vec::new());
                        let expected_completed = !goal_active
                            && !completed_before
                            && !tree
                            && kind == ReleaseKind::StopConfirmed;
                        let run = &m.runs.runs["运行"];
                        assert_eq!(run.status, Some(if expected_completed { RunStatus::Completed } else { RunStatus::Interrupted }),
                            "{reason:?}/goal={goal_active}/before={completed_before}/tree={tree}/{kind:?}");
                        assert_eq!(
                            run.interrupt_reason,
                            if expected_completed {
                                None
                            } else {
                                Some(interrupt)
                            }
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn 已启用收敛的新原因停止不能采用父轮正常完成例外() {
    use crate::claim::subagent_reconcile::tests::{fixture, R, T};
    for (reason, interrupt) in reasons() {
        let (mut m, _) = fixture(0);
        assert!(m.sa1b_active(R));
        m.run_goal_status(T, Some("paused"));
        m.run_stop_began(R, reason, &mut Vec::new());
        finish_parent(&mut m, R, T, "completed");
        m.run_stop_confirmed(R, &mut Vec::new());
        m.run_released(R, ReleaseKind::StopConfirmed, &mut Vec::new());
        assert_eq!(m.runs.runs[R].status, Some(RunStatus::Interrupted));
        assert_eq!(m.runs.runs[R].interrupt_reason, Some(interrupt));
    }
}

#[test]
fn 旧停止原因仍保留目标受阻的原始优先级() {
    for reason in [
        StopReason::Stop,
        StopReason::ClientDisconnected,
        StopReason::GoalLoop,
    ] {
        let mut m = machine();
        open(&mut m, "运行", "线程");
        m.run_goal_status("线程", Some("blocked"));
        m.run_stop_began("运行", reason, &mut Vec::new());
        finish_parent(&mut m, "运行", "线程", "failed");
        m.run_stop_confirmed("运行", &mut Vec::new());
        m.run_released("运行", ReleaseKind::StopConfirmed, &mut Vec::new());
        assert_eq!(m.runs.runs["运行"].status, Some(RunStatus::GoalBlocked));
    }
}
