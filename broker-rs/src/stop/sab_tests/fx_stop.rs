//! fx-stop：清理证据、授权范围和显式放弃的状态机回归。
use super::*;
use crate::runs::{RunState, StopOutcome};
use crate::stop::background::{Background, LIST, TERMINATE};

fn command(s: &mut Sim, thread: &str, turn: &str, id: &str) {
    s.background
        .entry(thread.into())
        .or_default()
        .push(json!({"itemId":id,"processId":id}));
    s.notify(
        json!({"method":"item/started","params":{"threadId":thread,"turnId":turn,
        "item":{"id":id,"type":"commandExecution","command":"测试后台命令"}}}),
    );
}
fn stop(s: &mut Sim, run: bool) -> u64 {
    let mut p = json!({"threadId":P});
    if run {
        p["runId"] = json!(R);
    }
    s.call(OWNER, "broker/threads/stop", p).0
}
fn abandon(s: &mut Sim) -> Value {
    let (id, a) = s.call(
        OWNER,
        "broker/threads/stop",
        json!({"threadId":P,"runId":R,"abandon":true}),
    );
    s.response(&a, id).expect("放弃应立即回答")["result"].clone()
}

#[test]
fn fx_单线程多个命令逐项终止并重新列举() {
    let mut s = Sim::new();
    command(&mut s, P, T, "11");
    command(&mut s, P, T, "12");
    let id = stop(&mut s, false);
    assert_eq!(
        s.response(&s.log, id).unwrap()["result"]["stopOutcome"],
        "confirmed"
    );
    assert!(s.background[P].is_empty());
    let methods: Vec<_> = s
        .log
        .iter()
        .filter_map(|a| match a {
            Action::Request(c) if c.method.starts_with("thread/backgroundTerminals/") => {
                Some(c.method.as_str())
            }
            _ => None,
        })
        .collect();
    assert_eq!(methods, [LIST, TERMINATE, TERMINATE, LIST]);
}

#[test]
fn fx_派生v1与v2命令都要清理() {
    for version in ["v1", "v2"] {
        let mut s = Sim::new();
        s.spawn("子", "派生", "子轮", version);
        command(&mut s, "子", "子轮", "13");
        let id = stop(&mut s, true);
        let result = &s.response(&s.log, id).unwrap()["result"];
        assert_eq!(result["stopOutcome"], "confirmed", "{version}: {result}");
        assert!(s.background["子"].is_empty());
        assert_eq!(
            result["childrenStop"]["members"]["子"]["backgroundCleanup"]["state"],
            "confirmed"
        );
    }
}

#[test]
fn fx_接口不支持绝不确认() {
    let mut s = Sim::new();
    s.background_supported = false;
    let id = stop(&mut s, false);
    s.advance(40_000);
    let result = &s.response(&s.log, id).unwrap()["result"];
    assert_eq!(result["stopOutcome"], "unconfirmed");
    assert_eq!(result["backgroundCleanup"]["state"], "capabilityLimited");
}

#[test]
fn fx_run未知归属不能终止而线程范围可以() {
    for narrow in [true, false] {
        let mut s = Sim::new();
        s.background
            .insert(P.into(), vec![json!({"itemId":"未知","processId":"14"})]);
        let id = stop(&mut s, narrow);
        s.advance(40_000);
        let result = &s.response(&s.log, id).unwrap()["result"];
        assert_eq!(
            result["stopOutcome"],
            if narrow { "unconfirmed" } else { "confirmed" }
        );
        assert_eq!(s.background[P].is_empty(), !narrow);
    }
}

#[test]
fn fx_terminate_false自然退出后复查可以确认() {
    let mut s = Sim::new();
    s.terminate_false = true;
    command(&mut s, P, T, "15");
    let id = stop(&mut s, true);
    let result = &s.response(&s.log, id).unwrap()["result"];
    assert_eq!(result["stopOutcome"], "confirmed");
    assert_eq!(
        result["backgroundCleanup"]["terminationResults"]["15"],
        false
    );
}

#[test]
fn fx_已结束运行重复停止重新核验() {
    let mut s = Sim::new();
    stop(&mut s, false);
    command(&mut s, P, T, "16");
    let id = stop(&mut s, true);
    assert_eq!(
        s.response(&s.log, id).unwrap()["result"]["stopOutcome"],
        "confirmed"
    );
    assert!(s.background[P].is_empty());
}

#[test]
fn fx_清理总期限不会按命令续期() {
    let mut b = Background::new(40_000);
    b.next(P, 1);
    b.reply(Ok(json!({"data":[{"itemId":"a","processId":"1"},{"itemId":"b","processId":"2"}],"nextCursor":null})), 2, |_| true);
    assert_eq!(b.next(P, 39_999).unwrap().0, TERMINATE);
    b.reply(Ok(json!({"terminated":true})), 40_001, |_| true);
    assert!(b.next(P, 40_002).is_none());
    assert!(!b.confirmed);
    assert_eq!(b.deadline, 40_000);
}

#[test]
fn fx_分页后必须从头复查() {
    let mut b = Background::new(40_000);
    b.next(P, 1);
    b.reply(
        Ok(json!({"data":[{"itemId":"a","processId":"1"}],"nextCursor":"页2"})),
        2,
        |_| true,
    );
    assert_eq!(b.next(P, 3).unwrap().1["cursor"], "页2");
    b.reply(Ok(json!({"data":[],"nextCursor":null})), 4, |_| true);
    assert_eq!(b.next(P, 5).unwrap().0, TERMINATE);
    b.reply(Ok(json!({"terminated":true})), 6, |_| true);
    assert!(b.next(P, 7).unwrap().1["cursor"].is_null());
    assert!(!b.confirmed);
}

#[test]
fn fx_能力卸载断连均保留不同失败原因() {
    for (error, state) in [
        (json!({"code":-32601}), "capabilityLimited"),
        (json!({"message":"experimental API disabled"}), "unknown"),
        (json!({"message":"thread not found"}), "notLoaded"),
        (json!({"message":"connection closed"}), "unknown"),
    ] {
        let mut b = Background::new(40_000);
        b.next(P, 1);
        b.reply(Err(error), 2, |_| true);
        assert_eq!(b.report.state, state);
        assert!(!b.confirmed);
    }
}

#[test]
fn fx_终止失败仍有编号时不能确认() {
    let mut b = Background::new(40_000);
    let page = json!({"data":[{"itemId":"a","processId":"1"}]});
    b.next(P, 1);
    b.reply(Ok(page.clone()), 2, |_| true);
    b.next(P, 3);
    b.reply(Ok(json!({"terminated":false})), 4, |_| true);
    b.next(P, 5);
    b.reply(Ok(page), 6, |_| true);
    assert!(b.failed);
    assert_eq!(b.report.remaining, ["1"]);
    assert!(!b.confirmed);
}

#[test]
fn fx_abandon根活动不提交结束且解除树屏障() {
    let mut s = Sim::new();
    s.spawn_v1("子", "派生", "子轮");
    s.never.insert(P.into());
    stop(&mut s, true);
    let before = s.run_ended(&s.log);
    let result = abandon(&mut s);
    assert_eq!(result["actionApplied"], true);
    assert_eq!(result["stopOutcome"], "unconfirmed");
    assert_eq!(result["childrenStop"]["cleanupState"], "abandoned");
    assert_eq!(result["childrenStop"]["rootRelease"]["released"], false);
    assert!(result["childrenStop"]["rootRelease"]["reason"]
        .as_str()
        .unwrap()
        .contains("轮次"));
    assert!(s.m.claims.contains_key(P));
    assert_eq!(s.run_ended(&s.log), before);
    assert_ne!(s.record().state, RunState::Ended);
    assert!(!s.m.tree_busy(Some(P)));
    assert!(s.m.stops.operations.is_empty());
}

fn lim_b_根后台受限() -> Sim {
    let mut s = Sim::new();
    s.spawn_v1("子", "派生", "子轮");
    s.background_supported = false;
    stop(&mut s, true);
    s.advance(40_000);
    assert_ne!(s.record().state, RunState::Ended);
    assert!(s.m.claims.contains_key(P));
    s
}

#[test]
fn lim_b_abandon根已停但后台受限应唯一提交并可同线程派单() {
    for no_claim in [false, true] {
        let mut s = lim_b_根后台受限();
        if no_claim {
            s.m.claims.remove(P);
        }
        let result = abandon(&mut s);
        assert_eq!(result["childrenStop"]["rootRelease"]["released"], true);
        assert_eq!(result["run"]["state"], "ended");
        assert_eq!(result["run"]["status"], "interrupted");
        assert_eq!(result["run"]["stopOutcome"], "unconfirmed");
        assert_eq!(result["run"]["childrenStop"]["cleanupState"], "abandoned");
        assert_eq!(s.run_ended(&s.log), 1);
        assert_eq!(abandon(&mut s)["reason"], "alreadyAbandoned");
        s.advance(60_000);
        assert_eq!(s.run_ended(&s.log), 1);
        assert!(s.m.claims.is_empty());
        assert!(!s.m.stops_busy());
        assert!(!s.m.tree_busy(None));
        let a = s.client(
            OWNER,
            "turn/start",
            json!({"threadId":P,"input":[]}),
            Some("新运行"),
        );
        assert_eq!(s.m.claims[P].run_id.as_deref(), Some("新运行"));
        assert!(
            a.iter()
                .any(|a| matches!(a, Action::Request(c) if c.method == "thread/resume")),
            "新运行通过门禁并开始恢复订阅"
        );
    }
}

#[test]
fn sa1b_弃管后同线程新运行不继承旧子代理且可完成() {
    for (root_limited, new_child) in [(false, false), (true, false), (false, true)] {
        let mut s = Sim::new();
        assert!(s
            .m
            .enable_convergence(crate::claim::converge::ConvergenceReadiness {
                trusted_inputs: true,
                budget_admission: true,
                asynchronous_results: true,
                terminal_mapping: true,
                cancellation_verified: true,
            }));
        s.spawn_v1("旧子", "旧派生", "旧子轮");
        s.never.insert("旧子".into());
        s.background_supported = !root_limited;
        stop(&mut s, true);
        s.advance(40_000);
        assert_eq!(
            abandon(&mut s)["childrenStop"]["rootRelease"]["released"],
            true
        );
        assert!(s.m.thread_relation("旧子").is_some(), "保留血缘诊断");
        let actions = s.client(
            OWNER,
            "turn/start",
            json!({"threadId":P,"input":[]}),
            Some("新运行"),
        );
        let start = actions
            .iter()
            .find_map(|a| match a {
                Action::Request(c) if c.method == "turn/start" => Some(c.clone()),
                _ => None,
            })
            .expect("新运行开轮");
        s.apply(Event::Reply {
            context: start,
            result: Ok(json!({"turn":{"id":"新父轮"}})),
        });
        s.turn_started(P, "新父轮");
        assert!(
            !s.m.run_subagent_evidence("新运行"),
            "旧血缘不能授予新运行收敛责任"
        );
        if new_child {
            // 新运行确有派生时正常进入收敛，但发现同一父线程的历史成员不能扩大本次工作集。
            s.mock.register("新子", Some(P), "v1", false, true);
            s.children.entry(P.into()).or_default().push("新子".into());
            let (started, completed) = SubagentMock::v1_spawn(P, "新父轮", "新派生", "新子", false);
            s.apply(started);
            s.apply(completed);
            s.turn_started("新子", "新子轮");
            assert!(s.m.run_subagent_evidence("新运行"));
            s.turn_completed(P, "新父轮", "completed");
            let members = &s.m.manage_context("新运行").unwrap().members;
            assert!(members.contains_key("新子"));
            assert!(
                !members.contains_key("旧子"),
                "收敛发现不能重新纳管已弃管旧子"
            );
            assert!(s.m.claims.contains_key(P), "新子仍活动，保留认领");
            continue;
        }
        s.turn_completed(P, "新父轮", "completed");
        assert!(!s.m.claims.contains_key(P));
        assert_eq!(
            s.m.runs.runs["新运行"].status,
            Some(crate::runs::RunStatus::Completed)
        );
        assert_eq!(
            s.m.runs.runs[R]
                .children_stop
                .as_ref()
                .unwrap()
                .cleanup_state,
            "abandoned"
        );
    }
}

#[test]
fn lim_b_abandon证据不足或认领变更不释放且说明原因() {
    for fault in [
        "ownership",
        "goal",
        "pending",
        "joined",
        "claim",
        "run",
        "evidence",
        "epoch",
    ] {
        let mut s = lim_b_根后台受限();
        match fault {
            "ownership" => {
                s.m.stops
                    .operations
                    .values_mut()
                    .find(|o| o.thread == P)
                    .unwrap()
                    .foreign_claim = true
            }
            "goal" => s.m.records.get_mut(P).unwrap().goal_active = true,
            "pending" => s.m.claims.get_mut(P).unwrap().request_pending = true,
            "joined" => {
                s.m.claims.get_mut(P).unwrap().state = State::Running {
                    owner: crate::claim::Owner::Client(OWNER),
                    turn_id: T.into(),
                    joined: true,
                }
            }
            "claim" => s.m.claims.get_mut(P).unwrap().claim_id += 1,
            "run" => s.m.claims.get_mut(P).unwrap().run_id = Some("外部运行".into()),
            "evidence" => s.m.stops.operations.clear(),
            "epoch" => s.m.relations.conn_epoch += 1,
            _ => unreachable!(),
        }
        let claim = s.m.claims[P].clone();
        let result = abandon(&mut s);
        assert_eq!(result["actionApplied"], true, "{fault}");
        assert_eq!(
            result["childrenStop"]["rootRelease"]["released"], false,
            "{fault}"
        );
        assert!(!result["childrenStop"]["rootRelease"]["reason"]
            .as_str()
            .unwrap()
            .is_empty());
        assert_eq!(s.m.claims[P], claim, "{fault}");
        assert_ne!(s.record().state, RunState::Ended, "{fault}");
        assert_eq!(s.run_ended(&s.log), 0, "{fault}");
    }
}

#[test]
fn lim_b_abandon结束快照提交中断且不沿用展示报告() {
    for active_goal in [false, true] {
        let mut s = Sim::new();
        s.spawn_v1("子", "派生", "子轮");
        s.never.insert("子".into());
        stop(&mut s, true);
        assert!(matches!(s.m.claims[P].state, State::FinishingTree { .. }));
        if active_goal {
            s.m.records.get_mut(P).unwrap().goal_active = true;
        }
        let result = abandon(&mut s);
        assert_eq!(
            result["childrenStop"]["rootRelease"]["released"],
            !active_goal
        );
        assert_eq!(s.run_ended(&s.log), usize::from(!active_goal));
    }
}

fn lim_b_review_结束快照() -> Sim {
    let mut s = Sim::new();
    s.spawn_v1("子", "派生", "子轮");
    s.never.insert("子".into());
    stop(&mut s, true);
    assert!(matches!(s.m.claims[P].state, State::FinishingTree { .. }));
    s
}

fn lim_b_review_goal(s: &mut Sim, status: &str) {
    let g = goal(P, status);
    s.goals.insert(P.into(), g.clone());
    s.notify(json!({"method":"thread/goal/updated","params":{"threadId":P,"goal":g}}));
}

fn lim_b_review_可再派单(s: &mut Sim) {
    assert!(!s.m.claims.contains_key(P));
    assert_eq!(s.record().state, RunState::Ended);
    assert_eq!(s.run_ended(&s.log), 1);
    assert_eq!(
        s.record().children_stop.as_ref().unwrap().cleanup_state,
        "abandoned"
    );
    assert_eq!(s.record().stop_outcome, Some(StopOutcome::Unconfirmed));
    let a = s.client(
        OWNER,
        "turn/start",
        json!({"threadId":P,"input":[]}),
        Some("审查修复新运行"),
    );
    assert_eq!(s.m.claims[P].run_id.as_deref(), Some("审查修复新运行"));
    assert!(a.iter().any(|a| matches!(a, Action::Request(c) if c.method == "turn/start" || c.method == "thread/resume")));
}

#[test]
fn lim_b_review_弃管未释放快照后轮次事件可收敛并再派单() {
    for started_before in [false, true] {
        let mut s = lim_b_review_结束快照();
        s.never.insert(P.into());
        s.hold.push(("thread/goal/get".into(), P.into()));
        if started_before {
            s.turn_started(P, "恢复轮");
        } else {
            lim_b_review_goal(&mut s, "active");
        }
        assert_eq!(
            abandon(&mut s)["childrenStop"]["rootRelease"]["released"],
            false
        );
        assert!(!s.m.tree_busy(Some(P)));
        assert!(s.tree_op().is_none());
        assert!(s.m.claims.contains_key(P), "弃管当时不得强行释放");
        if !started_before {
            s.turn_started(P, "恢复轮");
        }
        lim_b_review_goal(&mut s, "paused");
        s.turn_completed(P, "恢复轮", "completed");
        lim_b_review_可再派单(&mut s);
    }
}

#[test]
fn lim_b_review_派生根goal写在途弃管不释放且回包落定后收敛() {
    for status in ["active", "paused"] {
        let mut s = lim_b_review_结束快照();
        s.goals.insert(P.into(), goal(P, "paused"));
        s.hold.push(("thread/goal/set".into(), P.into()));
        let a = s.client(
            OWNER,
            "thread/goal/set",
            json!({"threadId":P,"status":status}),
            None,
        );
        let write = a
            .iter()
            .find_map(|a| match a {
                Action::Request(c) if c.method == "thread/goal/set" => Some(c.clone()),
                _ => None,
            })
            .unwrap();
        assert!(s.m.stops.changes.values().any(|c| c.thread == P));
        let result = abandon(&mut s);
        assert_eq!(result["childrenStop"]["rootRelease"]["released"], false);
        assert!(result["childrenStop"]["rootRelease"]["reason"]
            .as_str()
            .unwrap()
            .contains("请求未落定"));
        assert_eq!(s.run_ended(&s.log), 0);
        // 即使其他通知先报告根停下，也必须等写请求落定。
        lim_b_review_goal(&mut s, "paused");
        assert!(s.m.claims.contains_key(P));
        assert_eq!(s.run_ended(&s.log), 0);
        s.turn_started(P, "写回包前的新轮");
        s.turn_completed(P, "写回包前的新轮", "completed");
        assert!(
            s.m.claims.contains_key(P),
            "轮次完成也不能越过尚未落定的 goal 写"
        );
        assert_eq!(s.run_ended(&s.log), 0);
        s.apply(Event::Reply {
            context: write,
            result: Ok(json!({"goal":goal(P,status)})),
        });
        if status == "active" {
            assert!(s.m.records[P].goal_active);
            assert!(s.m.claims.contains_key(P));
            lim_b_review_goal(&mut s, "paused");
        }
        lim_b_review_可再派单(&mut s);
    }
}

#[test]
fn lim_b_review_弃管根读取迟到不污染已结束运行且不重复结束() {
    let mut s = lim_b_review_结束快照();
    s.hold.push(("thread/goal/get".into(), P.into()));
    lim_b_review_goal(&mut s, "active");
    let old = s
        .held
        .iter()
        .find(|c| c.method == "thread/goal/get" && c.params.as_ref().unwrap()["threadId"] == P)
        .unwrap()
        .clone();
    assert_eq!(
        abandon(&mut s)["childrenStop"]["rootRelease"]["released"],
        false
    );
    lim_b_review_goal(&mut s, "paused");
    assert_eq!(s.record().state, RunState::Ended);
    let record = s.record().clone();
    s.apply(Event::Reply {
        context: old,
        result: Ok(json!({"goal":goal(P,"active")})),
    });
    assert!(!s.m.records[P].goal_active);
    assert_eq!(s.record(), &record);
    assert_eq!(s.run_ended(&s.log), 1);
    lim_b_review_可再派单(&mut s);
}

#[test]
fn lim_b_abandon自然收尾根后台受限也提交中断() {
    for status in ["completed", "failed", "interrupted"] {
        let mut s = Sim::new();
        s.spawn_v1("子", "派生", "子轮");
        s.background_supported = false;
        s.turn_completed(P, T, status);
        s.advance(40_000);
        assert_ne!(s.record().state, RunState::Ended);
        let result = abandon(&mut s);
        assert_eq!(
            result["childrenStop"]["rootRelease"]["released"], true,
            "{status}: {result}"
        );
        assert_eq!(result["run"]["status"], "interrupted", "{status}");
        assert_eq!(s.run_ended(&s.log), 1);
        assert!(!s.m.claims.contains_key(P));
    }
}

#[test]
fn lim_b_abandon已提交根或外部新认领均不重复结束() {
    for foreign in [false, true] {
        let mut s = Sim::new();
        s.spawn_v1("子", "派生", "子轮");
        s.never.insert("子".into());
        let mut old_claim = s.m.claims[P].clone();
        stop(&mut s, true);
        s.advance(40_000);
        assert_eq!(s.record().state, RunState::Ended);
        if foreign {
            old_claim.run_id = Some("外部新运行".into());
            s.m.claims.insert(P.into(), old_claim.clone());
        }
        let result = abandon(&mut s);
        assert_eq!(result["childrenStop"]["rootRelease"]["released"], !foreign);
        assert_eq!(s.run_ended(&s.log), 1);
        if foreign {
            assert_eq!(s.m.claims[P], old_claim);
        }
    }
}

#[test]
fn fx_abandon多个等待者都获未确认且不续期() {
    let mut s = Sim::new();
    s.spawn_v1("子", "派生", "子轮");
    s.never.insert("子".into());
    let first = stop(&mut s, true);
    let tree = s.tree_op().unwrap();
    let deadline = s.m.trees.operations[&tree]
        .request
        .first_delivery_deadline_ms;
    s.advance(10_000);
    let second = stop(&mut s, true);
    assert_eq!(
        s.m.trees.operations[&tree]
            .request
            .first_delivery_deadline_ms,
        deadline
    );
    let result = abandon(&mut s);
    assert_eq!(result["childrenStop"]["rootRelease"]["released"], true);
    for id in [first, second] {
        assert_eq!(
            s.response(&s.log, id).unwrap()["result"]["stopOutcome"],
            "unconfirmed"
        );
    }
    s.advance(90_000);
    assert!(s.tree_op().is_none());
}

#[test]
fn fx_abandon已提交运行只结束一次重复停止不重启() {
    let mut s = Sim::new();
    s.spawn_v1("子", "派生", "子轮");
    s.never.insert("子".into());
    stop(&mut s, true);
    s.advance(40_000);
    assert_eq!(s.run_ended(&s.log), 1);
    abandon(&mut s);
    let start = s.log.len();
    let id = stop(&mut s, true);
    assert_eq!(
        s.response(&s.log, id).unwrap()["result"]["reason"],
        "alreadyAbandoned"
    );
    assert!(!s.log[start..]
        .iter()
        .any(|a| matches!(a, Action::Request(_))));
    assert_eq!(s.run_ended(&s.log), 1);
    assert_eq!(s.record().stop_outcome, Some(StopOutcome::Unconfirmed));
}

#[test]
fn fx_abandon迟到清理回包无副作用() {
    let mut s = Sim::new();
    s.spawn_v1("子", "派生", "子轮");
    s.hold.push((LIST.into(), "子".into()));
    stop(&mut s, true);
    assert!(s.held.iter().any(|c| c.method == LIST));
    abandon(&mut s);
    let start = s.log.len();
    s.release(LIST, "子");
    assert!(!s.log[start..]
        .iter()
        .any(|a| matches!(a, Action::Request(_))));
    assert!(s.tree_op().is_none());
}

#[test]
fn fx_abandon不存在已确认缺run均明确返回() {
    let mut s = Sim::new();
    assert_eq!(abandon(&mut s)["reason"], "treeStopNotFound");
    let (id, a) = s.call(
        OWNER,
        "broker/threads/stop",
        json!({"threadId":P,"abandon":true}),
    );
    assert_eq!(s.response(&a, id).unwrap()["error"]["code"], -32602);
    s.spawn_v1("子", "派生", "子轮");
    stop(&mut s, true);
    assert_eq!(abandon(&mut s)["reason"], "alreadyConfirmed");
}

#[test]
fn fx_树重复停止仍清理子线程且不重复结束() {
    for narrow in [true, false] {
        let mut s = Sim::new();
        s.spawn_v1("子", "派生", "子轮");
        command(&mut s, "子", "子轮", "21");
        stop(&mut s, narrow);
        s.background
            .insert("子".into(), vec![json!({"itemId":"21","processId":"21"})]);
        let id = stop(&mut s, narrow);
        assert_eq!(
            s.response(&s.log, id).unwrap()["result"]["stopOutcome"],
            "confirmed"
        );
        assert!(s.background["子"].is_empty());
        assert_eq!(s.run_ended(&s.log), 1);
    }
}

#[test]
fn fx_空列表后goal读取失败不能确认() {
    let mut s = Sim::new();
    s.hold.push((LIST.into(), P.into()));
    let id = stop(&mut s, false);
    s.hold.push(("thread/goal/get".into(), P.into()));
    s.release(LIST, P);
    let index = s
        .held
        .iter()
        .position(|c| c.method == "thread/goal/get")
        .unwrap();
    let context = s.held.remove(index);
    s.apply(Event::Reply {
        context,
        result: Err(json!({"code":-32000,"message":"断连"})),
    });
    s.advance(40_000);
    assert_eq!(
        s.response(&s.log, id).unwrap()["result"]["stopOutcome"],
        "unconfirmed"
    );
}

#[test]
fn fx_空列表后活动未知不能确认() {
    let mut s = Sim::new();
    s.hold.push((LIST.into(), P.into()));
    let id = stop(&mut s, false);
    s.hold.push(("thread/read".into(), P.into()));
    s.release(LIST, P);
    s.reply_status_read(
        "thread/read",
        P,
        Err(json!({"code":-32000,"message":"读取失败"})),
    );
    s.advance(40_000);
    assert_eq!(
        s.response(&s.log, id).unwrap()["result"]["stopOutcome"],
        "unconfirmed"
    );
}

#[test]
fn fx_后台清理超时后迟到空列表不能确认() {
    let mut s = Sim::new();
    s.hold.push((LIST.into(), P.into()));
    let id = stop(&mut s, false);
    s.advance(40_000);
    s.release(LIST, P);
    assert_eq!(
        s.response(&s.log, id).unwrap()["result"]["stopOutcome"],
        "unconfirmed"
    );
    assert!(s.m.stops_busy());
    assert_ne!(s.record().stop_outcome, Some(StopOutcome::Confirmed));
}

#[test]
fn fx_连接换代使旧空列表失效() {
    let mut s = Sim::new();
    s.hold.push((LIST.into(), P.into()));
    let id = stop(&mut s, false);
    s.m.relations.conn_epoch += 1;
    let ctx = s.held.iter().find(|c| c.method == LIST).unwrap().clone();
    s.apply(Event::Reply {
        context: ctx,
        result: Ok(json!({"data":[],"nextCursor":null})),
    });
    assert!(s.response(&s.log, id).is_none());
    assert!(s.m.stops_busy());
}

#[test]
fn fx_新轮次出现必须废弃清理证据() {
    let mut s = Sim::new();
    s.hold.push((LIST.into(), P.into()));
    let id = stop(&mut s, false);
    let old = s.held.iter().find(|c| c.method == LIST).unwrap().clone();
    s.never.insert(P.into());
    s.turn_started(P, "新轮");
    s.apply(Event::Reply {
        context: old,
        result: Ok(json!({"data":[],"nextCursor":null})),
    });
    assert!(s.response(&s.log, id).is_none());
    s.advance(40_000);
    assert_eq!(
        s.response(&s.log, id).unwrap()["result"]["stopOutcome"],
        "unconfirmed"
    );
}

#[test]
fn fx_缺附着证据报告包含具体子线程() {
    let mut s = Sim::new();
    s.spawn_v1("缺证据子", "派生", "子轮");
    s.turn_completed("缺证据子", "子轮", "completed");
    s.m.relations.attachments.remove("缺证据子");
    let id = stop(&mut s, true);
    s.advance(40_000);
    let result = &s.response(&s.log, id).unwrap()["result"];
    assert_eq!(
        result["childrenStop"]["discovery"]["missing"]["缺证据子"],
        json!(["attachmentMissing"])
    );
}

#[test]
fn fx_放弃后旧goal回包不改变新运行事实() {
    let mut s = Sim::new();
    s.spawn_v1("子", "派生", "子轮");
    s.hold.push(("thread/goal/get".into(), "子".into()));
    stop(&mut s, true);
    let context = s
        .held
        .iter()
        .find(|c| c.method == "thread/goal/get" && c.params.as_ref().unwrap()["threadId"] == "子")
        .unwrap()
        .clone();
    abandon(&mut s);
    let before = s.m.stops.version("子");
    let actions = s.apply(Event::Reply {
        context,
        result: Ok(json!({"goal":{"threadId":"子","status":"active"}})),
    });
    assert_eq!(s.m.stops.version("子"), before);
    assert!(!actions.iter().any(|a| matches!(a, Action::Request(_))));
}

#[test]
fn fx_放弃旧运行后迟到操作不影响新运行() {
    let mut s = Sim::new();
    s.spawn_v1("子", "派生", "子轮");
    s.never.insert("子".into());
    stop(&mut s, true);
    s.advance(40_000);
    abandon(&mut s);
    s.client(
        OWNER,
        "turn/start",
        json!({"threadId":P,"input":[]}),
        Some("新运行"),
    );
    let claim = s.m.claims[P].clone();
    let actions_start = s.log.len();
    let result = abandon(&mut s);
    assert_eq!(result["reason"], "alreadyAbandoned");
    assert_eq!(s.m.claims[P], claim);
    assert!(!s.log[actions_start..]
        .iter()
        .any(|a| matches!(a, Action::Request(_))));
}

#[test]
fn fx_放弃同时取消在途读取发现与全部定时器() {
    let mut s = Sim::new();
    s.spawn_v1("子", "派生", "子轮");
    s.hold.push(("thread/read".into(), "子".into()));
    s.hold.push(("thread/list".into(), "".into()));
    stop(&mut s, true);
    let tree = s.tree_op().unwrap();
    abandon(&mut s);
    assert!(!s.m.status_reads.reads.values().any(|r| matches!(r.caller, crate::claim::status_read::StatusCaller::DerivedStopVerify { tree_op, .. } if tree_op == tree)));
    assert!(!s.m.relations.discoveries.values().any(|d| matches!(d.caller, crate::relations::DiscoveryCaller::TreeStop { tree_op, .. } if tree_op == tree)));
    assert!(!s.timers.iter().any(|(_, _, _, k)| matches!(k, TimerKind::TreeStopWait { tree_op, .. } | TimerKind::TreeStopVerify { tree_op, .. } if *tree_op == tree)));
}

#[test]
fn fx_空分页也必须重新从第一页核验() {
    let mut b = Background::new(40_000);
    b.next(P, 1);
    b.reply(Ok(json!({"data":[],"nextCursor":"页2"})), 2, |_| true);
    b.next(P, 3);
    b.reply(Ok(json!({"data":[],"nextCursor":null})), 4, |_| true);
    assert!(!b.verifying);
    assert!(b.next(P, 5).unwrap().1["cursor"].is_null());
    b.reply(Ok(json!({"data":[],"nextCursor":null})), 6, |_| true);
    assert!(b.verifying);
}

#[test]
fn fx_派生后台核验期间新轮次不会沿用迟到空列表() {
    let mut s = Sim::new();
    s.spawn_v1("子", "派生", "子轮");
    s.hold.push((LIST.into(), "子".into()));
    let id = stop(&mut s, true);
    let old = s.held.iter().find(|c| c.method == LIST).unwrap().clone();
    s.never.insert("子".into());
    s.turn_started("子", "新子轮");
    s.apply(Event::Reply {
        context: old,
        result: Ok(json!({"data":[],"nextCursor":null})),
    });
    s.advance(40_000);
    assert_eq!(
        s.response(&s.log, id).unwrap()["result"]["stopOutcome"],
        "unconfirmed"
    );
}

#[test]
fn fx_旧清理代号的终止回包不能推进新操作() {
    let mut s = Sim::new();
    command(&mut s, P, T, "31");
    s.hold.push((TERMINATE.into(), P.into()));
    stop(&mut s, false);
    let old = s
        .held
        .iter()
        .find(|c| c.method == TERMINATE)
        .unwrap()
        .clone();
    let op = s.m.stops.operations.values_mut().next().unwrap();
    op.generation += 1;
    let before = op.background.clone();
    s.apply(Event::Reply {
        context: old,
        result: Ok(json!({"terminated":true})),
    });
    assert_eq!(
        s.m.stops.operations.values().next().unwrap().background,
        before
    );
}

#[test]
fn fx_收尾复核保留根和成员的发现数及终止结果() {
    let mut s = Sim::new();
    s.spawn_v1("子", "派生", "子轮");
    command(&mut s, P, T, "41");
    command(&mut s, "子", "子轮", "42");
    let id = stop(&mut s, true);
    let report = &s.response(&s.log, id).unwrap()["result"]["childrenStop"];
    assert_eq!(report["root"]["backgroundCleanup"]["discovered"], 1);
    assert_eq!(
        report["root"]["backgroundCleanup"]["terminationResults"]["41"],
        true
    );
    assert_eq!(
        report["members"]["子"]["backgroundCleanup"]["discovered"],
        1
    );
    assert_eq!(
        report["members"]["子"]["backgroundCleanup"]["terminationResults"]["42"],
        true
    );
}

#[test]
fn fx_清理中所有权撤销不能继续终止() {
    let mut s = Sim::new();
    s.spawn_v1("子", "派生", "子轮");
    command(&mut s, "子", "子轮", "43");
    s.hold.push((LIST.into(), "子".into()));
    stop(&mut s, true);
    s.m.relations.contexts.get_mut(R).unwrap().authority = crate::relations::Authority::Revoked;
    let start = s.log.len();
    s.release(LIST, "子");
    assert!(!s.log[start..]
        .iter()
        .any(|a| matches!(a, Action::Request(c) if c.method == TERMINATE)));
    assert_eq!(s.background["子"].len(), 1);
}

#[test]
fn fx_后台终止沿用树名额最多八个() {
    let mut s = Sim::new();
    for i in 0..10 {
        let thread = format!("子{i}");
        let turn = format!("子轮{i}");
        s.spawn_v1(&thread, &format!("派生{i}"), &turn);
        command(&mut s, &thread, &turn, &format!("命令{i}"));
        s.hold.push((TERMINATE.into(), thread));
    }
    stop(&mut s, true);
    let tree = s.tree_op().unwrap();
    assert_eq!(s.held.iter().filter(|c| c.method == TERMINATE).count(), 8);
    assert_eq!(s.m.trees.operations[&tree].in_flight, 8);
    s.release(TERMINATE, "子0");
    assert_eq!(s.held.iter().filter(|c| c.method == TERMINATE).count(), 8);
    assert_eq!(s.m.trees.operations[&tree].in_flight, 8);
}

#[test]
fn fx_父轮晚于子线程结束时根交接不能覆盖已经启动的收尾复核() {
    let mut s = Sim::new();
    s.spawn_v1("子", "派生", "子轮");
    // 与黑盒一致：中断父轮仅回包，先让子线程完成清理，再单独送达父轮结束通知。
    s.never.insert(P.into());
    let id = stop(&mut s, false);
    let tree = s.tree_op().unwrap();
    let deadline = s.m.trees.operations[&tree]
        .request
        .first_delivery_deadline_ms;
    assert!(s.m.trees.aux.derived[&(tree, "子".into())].stopped);
    assert!(!s.log.iter().any(|a| matches!(a, Action::Request(c)
        if c.method == LIST && c.params.as_ref().unwrap()["threadId"] == P)));
    assert!(s.response(&s.log, id).is_none());

    s.turn_completed(P, T, "interrupted");
    let reply = s.response(&s.log, id).unwrap_or_else(|| {
        panic!(
            "未推进首次期限内的确认：{:?}",
            s.m.trees.aux.derived.get(&(tree, P.into()))
        )
    });
    assert_eq!(reply["result"]["stopOutcome"], "confirmed");
    assert_eq!(
        reply["result"]["childrenStop"]["root"]["check"]["settled"],
        true
    );
    assert_eq!(
        reply["result"]["childrenStop"]["root"]["check"]["verifyDone"],
        true
    );
    assert!(
        s.m.clock.mono_ms < deadline,
        "不推进时钟，不依赖 40 秒或后续 15 秒计时器"
    );
    assert!(s.tree_op().is_none());
    assert_eq!(s.run_ended(&s.log), 1);
    let lists = s
        .log
        .iter()
        .filter(|a| {
            matches!(a, Action::Request(c)
        if c.method == LIST && c.params.as_ref().unwrap()["threadId"] == P)
        })
        .count();
    assert_eq!(lists, 2, "根单线程清理和树收尾各自从头核验一次");
}

#[test]
fn fx_单线程结束事件立即推进后台清理与后置复核无需等期限() {
    let mut s = Sim::new();
    s.never.insert(P.into());
    command(&mut s, P, T, "晚结束命令");
    let id = stop(&mut s, false);
    assert!(s.response(&s.log, id).is_none());
    assert!(!s
        .log
        .iter()
        .any(|a| matches!(a, Action::Request(c) if c.method == LIST)));
    let now = s.m.clock.mono_ms;
    s.turn_completed(P, T, "interrupted");
    let reply = s.response(&s.log, id).expect("结束通知应立即驱动清理");
    assert_eq!(reply["result"]["stopOutcome"], "confirmed");
    assert_eq!(reply["result"]["backgroundCleanup"]["state"], "confirmed");
    assert!(s.background[P].is_empty());
    assert_eq!(s.m.clock.mono_ms, now);
    assert!(s.m.stops.operations.is_empty());
}

#[test]
fn fx_review1_连续三次停止保留历史成员并逐次清理子命令() {
    for narrow in [false, true] {
        let mut s = Sim::new();
        s.spawn_v1("子", "派生", "子轮");
        command(&mut s, "子", "子轮", "71");
        let history = s.record().children.clone();
        for attempt in 1..=3 {
            s.background
                .insert("子".into(), vec![json!({"itemId":"71","processId":"71"})]);
            let start = s.log.len();
            let id = stop(&mut s, narrow);
            assert_eq!(
                s.response(&s.log, id).unwrap()["result"]["stopOutcome"],
                "confirmed"
            );
            assert_eq!(
                s.record().children.len(),
                history.len(),
                "第 {attempt} 次不得删除历史成员"
            );
            assert_eq!(s.record().children[0].thread_id, history[0].thread_id);
            assert_eq!(s.record().children[0].evidence, history[0].evidence);
            assert_eq!(s.record().children[0].turn_ids, history[0].turn_ids);
            for method in [LIST, TERMINATE] {
                assert!(
                    s.log[start..].iter().any(|a| matches!(a, Action::Request(c)
                    if c.method == method && c.params.as_ref().unwrap()["threadId"] == "子")),
                    "第 {attempt} 次缺少子线程 {method}"
                );
            }
            assert!(s.background["子"].is_empty());
        }
        assert_eq!(s.run_ended(&s.log), 1);
    }
}

#[test]
fn fx_review2_自然完成且没有树停止报告仍清理历史子命令() {
    let mut s = Sim::new();
    s.spawn_v1("子", "派生", "子轮");
    command(&mut s, "子", "子轮", "72");
    s.turn_completed("子", "子轮", "completed");
    s.turn_completed(P, T, "completed");
    assert_eq!(s.record().state, RunState::Ended);
    assert!(s.record().children_stop.is_none());
    assert_eq!(s.record().children.len(), 1);
    let start = s.log.len();
    let id = stop(&mut s, false);
    assert_eq!(
        s.response(&s.log, id).unwrap()["result"]["stopOutcome"],
        "confirmed"
    );
    assert!(s.log[start..].iter().any(|a| matches!(a, Action::Request(c)
        if c.method == TERMINATE && c.params.as_ref().unwrap()["threadId"] == "子")));
    assert!(s.background["子"].is_empty());
    assert_eq!(s.run_ended(&s.log), 1);
}

#[test]
fn fx_review3_无认领根收尾后新活动撤销旧证据且不认领外部轮次() {
    let mut s = Sim::new();
    s.m.shared = true;
    s.spawn_v1("子", "派生", "子轮");
    stop(&mut s, true);
    assert!(!s.m.claims.contains_key(P));
    s.hold.push((LIST.into(), P.into()));
    let id = stop(&mut s, true);
    s.hold.push(("thread/list".into(), "".into()));
    s.release(LIST, P);
    let tree = s.tree_op().unwrap();
    assert!(s.m.trees.aux.work[&tree].final_pending.is_empty());
    assert!(s.m.trees.aux.work[&tree].final_discovery.is_some());
    assert!(
        s.m.trees.aux.derived[&(tree, P.into())]
            .background
            .confirmed
    );
    let start = s.log.len();
    s.turn_started(P, "外部轮");
    command(&mut s, P, "外部轮", "73");
    s.release("thread/list", "");
    assert!(
        s.response(&s.log, id).is_none(),
        "收到新活动后不得沿用根的旧空闲证据确认"
    );
    assert_eq!(s.tree_op(), Some(tree));
    assert!(!s.m.trees.aux.work[&tree].root_done);
    assert!(
        !s.m.trees.aux.derived[&(tree, P.into())]
            .background
            .confirmed
    );
    assert!(!s.m.claims.contains_key(P));
    assert_eq!(s.interrupts(&s.log[start..], P), 0);
    assert!(!s.log[start..]
        .iter()
        .any(|a| matches!(a, Action::Request(c) if c.method == TERMINATE)));
    // 外部运行自行结束并清掉命令后，可以重新核验收敛，不借用旧运行中断它。
    s.background.get_mut(P).unwrap().clear();
    s.turn_completed(P, "外部轮", "completed");
    assert_eq!(
        s.response(&s.log, id).unwrap()["result"]["stopOutcome"],
        "confirmed"
    );
    assert_eq!(s.interrupts(&s.log[start..], P), 0);
    assert_eq!(s.run_ended(&s.log), 1);
}

#[test]
fn fx_review4_重启恢复为lost且全空闲可确认历史树停止() {
    let mut s = Sim::new();
    s.spawn_v1("子", "派生", "子轮");
    let record = s.record().clone();
    let index = s.m.runs.index.clone();
    s.m = Machine::new(true);
    s.m.clock.mono_ms = 1000;
    s.m.clock.now_ms = record.started_at + 1000;
    s.apply(Event::Connected { conn: OWNER });
    s.apply(Event::Initialized {
        conn: OWNER,
        cxb: true,
    });
    let mut actions = Vec::new();
    s.m.restore_runs(index, vec![record], &mut actions);
    s.absorb(actions);
    s.turns.clear();
    s.learn_thread(P);
    s.learn_thread("子");
    assert_eq!(s.record().state, RunState::Lost);
    assert!(s.m.claims.is_empty());
    assert!(s.m.relations.contexts.is_empty());
    for _ in 0..3 {
        let start = s.log.len();
        let id = stop(&mut s, true);
        let reply = s
            .response(&s.log, id)
            .expect("全空闲、接口正常应在首轮收敛");
        assert_eq!(reply["result"]["stopOutcome"], "confirmed");
        assert!(s.tree_op().is_none());
        assert!(!s.m.tree_busy(Some(P)));
        assert_eq!(s.record().children.len(), 1);
        assert_eq!(s.record().state, RunState::Lost);
        assert!(s.m.claims.is_empty());
        assert!(
            !s.log[start..].iter().any(|a| matches!(a, Action::Request(c)
            if matches!(c.method.as_str(), "thread/resume" | "turn/start" | "turn/interrupt")))
        );
    }
    assert_eq!(
        s.run_ended(&s.log),
        0,
        "Lost 只更新核验报告，不补发历史结束事件"
    );
}

#[test]
fn fx_review3_无认领根的active_goal也撤销收尾证据() {
    let mut s = Sim::new();
    s.spawn_v1("子", "派生", "子轮");
    stop(&mut s, true);
    s.hold.push((LIST.into(), P.into()));
    let id = stop(&mut s, true);
    s.hold.push(("thread/list".into(), "".into()));
    s.release(LIST, P);
    let tree = s.tree_op().unwrap();
    assert!(s.m.trees.aux.work[&tree].final_pending.is_empty());
    let start = s.log.len();
    let goal = json!({"threadId":P,"status":"active","objective":"外部目标"});
    s.goals.insert(P.into(), goal.clone());
    s.notify(json!({"method":"thread/goal/updated","params":{"threadId":P,"goal":goal}}));
    s.release("thread/list", "");
    assert!(s.response(&s.log, id).is_none());
    assert!(!s.m.trees.aux.work[&tree].root_done);
    assert!(
        !s.m.trees.aux.derived[&(tree, P.into())]
            .background
            .confirmed
    );
    assert!(
        !s.log[start..].iter().any(|a| matches!(a, Action::Request(c)
        if c.method == "thread/goal/set" || c.method == "thread/goal/clear"))
    );
}

#[test]
fn fx_review2_自然完成但已撤权上下文尚未回收仍按历史成员停树() {
    let mut s = Sim::new();
    s.spawn_v1("子", "派生", "子轮");
    command(&mut s, "子", "子轮", "74");
    s.turn_completed("子", "子轮", "completed");
    // 控制回收边界：保留上下文直到显式 stop，模拟清理/淘汰保护尚未解除。
    s.m.runs.tree_retained.insert(R.into());
    s.turn_completed(P, T, "completed");
    assert!(s.record().children_stop.is_none());
    assert_eq!(
        s.m.relations.contexts[R].authority,
        crate::relations::Authority::Revoked
    );
    let id = stop(&mut s, false);
    assert_eq!(
        s.response(&s.log, id).unwrap()["result"]["stopOutcome"],
        "confirmed"
    );
    assert!(s.background["子"].is_empty());
    assert_eq!(s.record().children.len(), 1);
}

#[test]
fn fx_unloaded_重复停止有关闭通知应确认且立即释放() {
    let mut s = Sim::new();
    stop(&mut s, false);
    s.background_missing.insert(P.into());
    s.metadata_unloaded.insert(P.into());
    s.notify(json!({"method":"thread/closed","params":{"threadId":P}}));
    let id = stop(&mut s, false);
    let result = &s.response(&s.log, id).expect("关闭证据成立应立即回答")["result"];
    assert_eq!(result["stopOutcome"], "confirmed");
    assert_eq!(result["backgroundCleanup"]["state"], "notLoaded");
    assert_eq!(
        result["backgroundCleanup"]["verificationBasis"]["source"],
        "thread/closed"
    );
    assert!(!s.m.stop_busy(P));
    let a = s.client(
        OWNER,
        "turn/start",
        json!({"threadId":P,"input":[]}),
        Some("新运行"),
    );
    assert!(a
        .iter()
        .any(|a| matches!(a, Action::Request(c) if c.method == "turn/start")));
}

#[test]
fn fx_unloaded_重复停止缺关闭证据未确认也不留屏障或周期重试() {
    let mut s = Sim::new();
    stop(&mut s, false);
    s.background_missing.insert(P.into());
    let id = stop(&mut s, false);
    assert_eq!(
        s.response(&s.log, id).expect("未加载且缺证据应立即返回")["result"]["stopOutcome"],
        "unconfirmed"
    );
    assert!(!s.m.stop_busy(P));
    let from = s.log.len();
    s.advance(60_000);
    assert!(!s.log[from..].iter().any(
        |a| matches!(a, Action::Request(c) if c.method.starts_with("thread/backgroundTerminals/"))
    ));
    assert!(s.m.stops.operations.is_empty());
}

#[test]
fn fx_unloaded_重复停树未知卸载成员应立即解除树屏障() {
    let mut s = Sim::new();
    s.spawn_v1("子", "派生", "子轮");
    stop(&mut s, true);
    s.background_missing.insert("子".into());
    let id = stop(&mut s, true);
    let reply = s
        .response(&s.log, id)
        .expect("历史树遇到未知卸载不留下无限核验");
    assert_eq!(reply["result"]["stopOutcome"], "unconfirmed");
    assert_eq!(reply["result"]["childrenStop"]["cleanupState"], "released");
    assert!(!s.m.tree_busy(Some(P)));
    assert!(s.tree_op().is_none());
}

#[test]
fn fx_unloaded_新鲜notloaded元数据可单独确认不需要resume() {
    for narrow in [false, true] {
        let mut s = Sim::new();
        stop(&mut s, false);
        s.background_missing.insert(P.into());
        s.metadata_unloaded.insert(P.into());
        let start = s.log.len();
        let id = stop(&mut s, narrow);
        let result = &s.response(&s.log, id).unwrap()["result"];
        assert_eq!(result["stopOutcome"], "confirmed");
        assert_eq!(
            result["backgroundCleanup"]["verificationBasis"]["source"],
            "thread/read:notLoaded"
        );
        assert!(!s.log[start..].iter().any(|a| matches!(a, Action::Request(c)
            if matches!(c.method.as_str(), "thread/resume" | "thread/goal/set" | "thread/goal/clear"))));
        assert!(!s.m.stop_busy(P));
        assert!(!s.m.runs.verifying_history.contains(R));
    }
}

#[test]
fn fx_unloaded_已卸载父子都按新鲜关闭证据确认历史树() {
    let mut s = Sim::new();
    s.spawn_v1("子", "派生", "子轮");
    stop(&mut s, true);
    for thread in [P, "子"] {
        s.background_missing.insert(thread.into());
        s.metadata_unloaded.insert(thread.into());
    }
    let id = stop(&mut s, true);
    let result = &s.response(&s.log, id).unwrap()["result"];
    assert_eq!(result["stopOutcome"], "confirmed");
    assert_eq!(
        result["childrenStop"]["root"]["backgroundCleanup"]["state"],
        "notLoaded"
    );
    assert_eq!(
        result["childrenStop"]["members"]["子"]["backgroundCleanup"]["state"],
        "notLoaded"
    );
    assert!(!s.m.tree_busy(Some(P)));
    assert_eq!(s.record().children.len(), 1);
    assert_eq!(s.run_ended(&s.log), 1);
}

#[test]
fn fx_unloaded_首次活动停止缺关闭证据仍保留严格屏障() {
    for tree in [false, true] {
        let mut s = Sim::new();
        let target = if tree {
            s.spawn_v1("子", "派生", "子轮");
            "子"
        } else {
            P
        };
        s.background_missing.insert(target.into());
        let id = stop(&mut s, false);
        assert!(s.response(&s.log, id).is_none());
        s.advance(40_000);
        assert_eq!(
            s.response(&s.log, id).unwrap()["result"]["stopOutcome"],
            "unconfirmed"
        );
        s.advance(15_000);
        assert!(
            s.m.stop_busy(P) || s.m.tree_busy(Some(P)),
            "首次停止不适用历史释放例外"
        );
    }
}

#[test]
fn fx_unloaded_初次停树已关闭子成员有强证据仍可确认() {
    let mut s = Sim::new();
    s.spawn_v1("子", "派生", "子轮");
    s.turn_completed("子", "子轮", "completed");
    s.background_missing.insert("子".into());
    s.metadata_unloaded.insert("子".into());
    s.notify(json!({"method":"thread/closed","params":{"threadId":"子"}}));
    let id = stop(&mut s, false);
    assert_eq!(
        s.response(&s.log, id).unwrap()["result"]["stopOutcome"],
        "confirmed"
    );
}

#[test]
fn fx_unloaded_连接换代和重新加载使旧关闭证据失效() {
    for epoch in [true, false] {
        let mut s = Sim::new();
        stop(&mut s, false);
        s.notify(json!({"method":"thread/closed","params":{"threadId":P}}));
        assert!(s.m.closed_runtime(P).is_some());
        if epoch {
            s.m.relations.conn_epoch += 1;
        } else {
            s.learn_thread(P);
        }
        assert!(s.m.closed_runtime(P).is_none());
        s.background_missing.insert(P.into());
        let id = stop(&mut s, false);
        assert_eq!(
            s.response(&s.log, id).unwrap()["result"]["stopOutcome"],
            "unconfirmed"
        );
        assert!(!s.m.stop_busy(P));
    }
}

#[test]
fn fx_unloaded_旧closed不能覆盖新鲜的已加载元数据() {
    let mut s = Sim::new();
    stop(&mut s, false);
    s.notify(json!({"method":"thread/closed","params":{"threadId":P}}));
    s.background
        .insert(P.into(), vec![json!({"itemId":"75","processId":"75"})]);
    let id = stop(&mut s, false);
    let result = &s.response(&s.log, id).unwrap()["result"];
    assert_eq!(result["backgroundCleanup"]["state"], "confirmed");
    assert_eq!(
        result["backgroundCleanup"]["terminationResults"]["75"],
        true
    );
    assert!(s.background[P].is_empty());
    assert!(s.m.closed_runtime(P).is_none());
}

#[test]
fn fx_unloaded_元数据未知历史核验立即释放且迟到回包不影响新运行() {
    let mut s = Sim::new();
    stop(&mut s, false);
    s.hold.push(("thread/read".into(), P.into()));
    let id = stop(&mut s, false);
    let old = s
        .held
        .iter()
        .find(|c| c.method == "thread/read")
        .unwrap()
        .clone();
    s.reply_status_read(
        "thread/read",
        P,
        Err(json!({"code":-32000,"message":"无法读取"})),
    );
    assert_eq!(
        s.response(&s.log, id).unwrap()["result"]["stopOutcome"],
        "unconfirmed"
    );
    assert!(!s.m.stop_busy(P));
    s.client(
        OWNER,
        "turn/start",
        json!({"threadId":P,"input":[]}),
        Some("新运行"),
    );
    let claim = s.m.claims[P].clone();
    s.apply(Event::Reply {
        context: old,
        result: Ok(meta(P, "notLoaded")),
    });
    assert_eq!(s.m.claims[P], claim);
    assert!(s.m.closed_runtime(P).is_none());
    assert!(s.m.stops.operations.is_empty());
}

#[test]
fn fx_unloaded_关闭不能冒充已完成clear且同样不占用历史线程() {
    let mut s = Sim::new();
    stop(&mut s, false);
    s.metadata_unloaded.insert(P.into());
    let (id, _) = s.call(
        OWNER,
        "broker/threads/stop",
        json!({"threadId":P,"clear":true}),
    );
    let result = &s.response(&s.log, id).unwrap()["result"];
    assert_eq!(result["stopOutcome"], "unconfirmed");
    assert_ne!(result["goal"], "cleared");
    assert!(!s.m.stop_busy(P));
}

#[test]
fn fx_unloaded_重复停止接口不支持仍沿用原来的未确认策略() {
    let mut s = Sim::new();
    stop(&mut s, false);
    s.background_supported = false;
    let id = stop(&mut s, false);
    assert!(s.response(&s.log, id).is_none());
    s.advance(40_000);
    let result = &s.response(&s.log, id).unwrap()["result"];
    assert_eq!(result["stopOutcome"], "unconfirmed");
    assert_eq!(result["backgroundCleanup"]["state"], "capabilityLimited");
    assert!(s.m.stop_busy(P));
}

#[test]
fn fx_unloaded_只读历史预检不挡新派单并取消旧核验() {
    let mut s = Sim::new();
    stop(&mut s, false);
    s.hold.push(("thread/read".into(), P.into()));
    let id = stop(&mut s, false);
    let old = s
        .held
        .iter()
        .find(|c| c.method == "thread/read")
        .unwrap()
        .clone();
    assert!(!s.m.stop_busy(P), "只读预检不取得运行占用");
    let a = s.client(
        OWNER,
        "turn/start",
        json!({"threadId":P,"input":[]}),
        Some("新运行"),
    );
    assert!(a
        .iter()
        .any(|a| matches!(a, Action::Request(c) if c.method == "turn/start")));
    assert_eq!(
        s.response(&s.log, id).unwrap()["result"]["stopOutcome"],
        "unconfirmed"
    );
    let current = s.m.claims[P].clone();
    s.apply(Event::Reply {
        context: old,
        result: Ok(meta(P, "notLoaded")),
    });
    assert_eq!(s.m.claims[P], current);
    assert!(s.m.stops.operations.is_empty());
    assert!(s.m.closed_runtime(P).is_none());
}

#[test]
fn fx_unloaded_身份缺失或不匹配的元数据不能作为关闭依据() {
    for thread in [
        json!({"status":{"type":"notLoaded"}}),
        json!({"id":"其他线程","status":{"type":"notLoaded"}}),
    ] {
        let mut s = Sim::new();
        stop(&mut s, false);
        s.background_missing.insert(P.into());
        s.hold.push(("thread/read".into(), P.into()));
        let id = stop(&mut s, false);
        s.reply_status_read("thread/read", P, Ok(json!({"thread":thread})));
        assert_eq!(
            s.response(&s.log, id).unwrap()["result"]["stopOutcome"],
            "unconfirmed"
        );
        assert!(!s.m.stop_busy(P));
        assert!(s.m.closed_runtime(P).is_none());
    }
}

#[test]
fn fx_unloaded_旧连接的在途元数据不得生成新连接关闭证据() {
    let mut s = Sim::new();
    stop(&mut s, false);
    s.hold.push(("thread/read".into(), P.into()));
    let id = stop(&mut s, false);
    s.m.relations.conn_epoch += 1;
    s.reply_status_read("thread/read", P, Ok(meta(P, "notLoaded")));
    assert_eq!(
        s.response(&s.log, id).unwrap()["result"]["stopOutcome"],
        "unconfirmed"
    );
    assert!(s.m.closed_runtime(P).is_none());
    assert!(!s.m.stop_busy(P));
}

#[test]
fn fx_unloaded_历史预检发现实际活动仍走严格停止流程() {
    let mut s = Sim::new();
    stop(&mut s, false);
    s.turn_started(P, "后续活动");
    s.background_missing.insert(P.into());
    let id = stop(&mut s, false);
    assert!(
        s.response(&s.log, id).is_none(),
        "发现活动不能套用空闲历史核验例外"
    );
    s.advance(40_000);
    assert_eq!(
        s.response(&s.log, id).unwrap()["result"]["stopOutcome"],
        "unconfirmed"
    );
    assert!(s.m.stop_busy(P));
}
