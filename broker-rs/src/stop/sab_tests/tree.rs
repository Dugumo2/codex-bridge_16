//! 整树停止：用户停止、名额让出、40 秒期限与 15 秒核实、多调用者、新成员纳入、--run 限定与冲突、
//! 同步修改的忙判定与淘汰保护、所有内部停止来源升级为整树停止。
use super::*;
use crate::runs::{InterruptReason, RunState, RunStatus, StopOutcome};
use crate::stop::tree::{TreePhase, TreeTrigger, FIRST_DELIVERY_MS, MAX_TREE_REQUESTS};

fn stop(s: &mut Sim, conn: u64, params: Value) -> (u64, Vec<Action>) {
    s.call(conn, "broker/threads/stop", params)
}

#[test]
fn 整树停止回包携带根停止诊断() {
    for diagnostics in [None, Some(crate::stop::tests::stop_diagnostics_fixture())] {
        let mut s = Sim::new();
        s.spawn_v1("子甲", "派生1", "子轮甲");
        s.m.runs.runs.get_mut(R).unwrap().stop_diagnostics = diagnostics.clone();
        let (id, _) = stop(&mut s, OWNER, json!({"threadId": P}));
        let result = &s.response(&s.log, id).unwrap()["result"];
        assert_eq!(result["stopDiagnostics"], json!(diagnostics));
        assert_eq!(result["stopOutcome"], "confirmed");
    }
}

#[test]
fn 派生根核实目标沿用最近一次同代际发送轮次() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.hold.push(("turn/interrupt".into(), "子甲".into()));
    stop(&mut s, OWNER, json!({"threadId": P}));
    let tree_op = s.tree_op().unwrap();
    let key = (tree_op, P.to_owned());
    assert!(s.m.trees.aux.derived[&key].root);
    let generation = s.m.trees.aux.work[&tree_op].root_diag_generation.unwrap();
    let original = s.m.derived_diagnostic_send(&key, None).unwrap().1.turn;
    s.m.runs.stop_diag_sends.insert(
        R.into(),
        crate::runs::StopDiagSend {
            stop_generation: generation,
            thread: P.into(),
            turn: Some("后续根轮".into()),
        },
    );
    assert_eq!(
        s.m.derived_diagnostic_send(&key, None)
            .unwrap()
            .1
            .turn
            .as_deref(),
        Some("后续根轮")
    );
    assert_eq!(
        s.m.derived_diagnostic_send(&key, Some("明确轮"))
            .unwrap()
            .1
            .turn
            .as_deref(),
        Some("明确轮")
    );
    for mismatched_thread in [false, true] {
        let send = s.m.runs.stop_diag_sends.get_mut(R).unwrap();
        if mismatched_thread {
            send.thread = "旧执行线程".into();
        } else {
            send.stop_generation = generation + 1;
        }
        assert_eq!(
            s.m.derived_diagnostic_send(&key, None).unwrap().1.turn,
            original
        );
    }
}

#[test]
fn 派生根核实总期限只归原根停止代际且不接纳子成员() {
    use crate::claim::status_read::{StatusCaller, StatusDone, StatusResult, StatusUnknown};
    use crate::stop::derived::DerivedStage;
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.hold.push(("turn/interrupt".into(), "子甲".into()));
    stop(&mut s, OWNER, json!({"threadId": P}));
    let tree_op = s.tree_op().unwrap();
    let key = (tree_op, P.to_owned());
    let derived = s.m.trees.aux.derived.get_mut(&key).unwrap();
    derived.stage = DerivedStage::Verify;
    derived.request_round = 23;
    derived.read_id = Some(123);
    let (run, send) = s.m.derived_diagnostic_send(&key, None).unwrap();
    assert_eq!(
        Some(send.stop_generation),
        s.m.trees.aux.work[&tree_op].root_diag_generation
    );
    let rpc_key = crate::stop::StopDiagRpcKey {
        source: "read:123".into(),
        generation: 7,
        round: 0,
        method: "thread/turns/list".into(),
        thread: P.into(),
        turn: None,
    };
    s.m.stops
        .diagnostic_rpc_sends
        .insert(rpc_key.clone(), (run.clone(), send.clone()));
    let done = StatusDone {
        read_id: 123,
        thread: P.into(),
        token: None,
        caller: StatusCaller::DerivedStopVerify {
            tree_op,
            thread: P.into(),
            round: 23,
        },
        result: StatusResult::Unknown(StatusUnknown::Timeout),
    };
    let (actual_run, actual_send, method) = s.m.take_stop_diag_status_issue(&done).unwrap();
    assert_eq!(actual_run, R);
    assert_eq!(actual_send, send);
    assert_eq!(method, "thread/turns/list");
    assert!(s.m.take_stop_diag_status_issue(&done).is_none());
    s.m.trees.aux.derived.get_mut(&key).unwrap().root = false;
    s.m.stops.diagnostic_rpc_sends.insert(rpc_key, (run, send));
    assert!(s.m.take_stop_diag_status_issue(&done).is_none());
}

#[test]
fn 收尾发现一次失败后周期重查恢复且保留调用者期限() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.hold.push(("turn/interrupt".into(), "子甲".into()));
    let (id, _) = stop(&mut s, OWNER, json!({"threadId": P}));
    let tree = s.tree_op().unwrap();
    let deadline = s.m.trees.operations[&tree]
        .request
        .first_delivery_deadline_ms;
    let waiters = s.m.trees.operations[&tree].waiters.clone();
    s.list_fault = true;
    s.release("turn/interrupt", "子甲");
    assert_eq!(s.m.trees.aux.work[&tree].final_complete, Some(false));
    assert!(!s.m.trees.aux.work[&tree].final_started);
    assert_eq!(
        s.m.trees.operations[&tree]
            .request
            .first_delivery_deadline_ms,
        deadline
    );
    assert_eq!(s.m.trees.operations[&tree].waiters, waiters);
    assert!(s.response(&s.log, id).is_none());
    s.list_fault = false;
    s.advance(15_000);
    assert!(s.tree_op().is_none(), "恢复后重新发现和核实，解除屏障");
    assert_eq!(
        s.response(&s.log, id).unwrap()["result"]["stopOutcome"],
        "confirmed"
    );
}

#[test]
fn 用户停止父线程_停整棵受管树并以根与子代理报告确认() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.spawn_v1("子乙", "派生2", "子轮乙");
    let start = s.log.len();
    let (id, _) = stop(&mut s, OWNER, json!({"threadId": P}));
    let actions = s.log[start..].to_vec();
    assert_eq!(s.interrupts(&actions, P), 1, "根走现有单线程停止");
    assert_eq!(s.interrupts(&actions, "子甲"), 1);
    assert_eq!(s.interrupts(&actions, "子乙"), 1);
    let reply = s.response(&actions, id).expect("stop 有回包")["result"].clone();
    assert_eq!(reply["stopOutcome"], "confirmed");
    assert_eq!(reply["alreadyStopped"], false);
    assert_eq!(reply["childrenStop"]["phase"], "confirmed");
    assert_eq!(reply["childrenStop"]["stopped"], json!(["子乙", "子甲"]));
    let record = s.record();
    assert_eq!(record.state, RunState::Ended);
    assert_eq!(record.status, Some(RunStatus::Interrupted));
    assert_eq!(record.interrupt_reason, Some(InterruptReason::Stop));
    assert_eq!(record.stop_outcome, Some(StopOutcome::Confirmed));
    assert_eq!(s.run_ended(&actions), 1);
}

#[test]
fn 根单线程停止确认后不直接释放_等子代理报告后由协调器提交() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.hold.push(("turn/interrupt".into(), "子甲".into()));
    let start = s.log.len();
    stop(&mut s, OWNER, json!({"threadId": P}));
    // 根已停，但子代理未确认：不释放运行、不写父 stopOutcome=confirmed、不发 run/ended。
    assert!(s.finishing(), "根停止成立后认领转入结束快照");
    assert_eq!(s.record().state, RunState::Stopping);
    assert_ne!(s.record().stop_outcome, Some(StopOutcome::Confirmed));
    assert_eq!(s.run_ended(&s.log[start..]), 0);
    s.release("turn/interrupt", "子甲");
    assert_eq!(s.record().state, RunState::Ended);
    assert!(s.record().children_stop.is_some());
    assert_eq!(s.run_ended(&s.log[start..]), 1);
}

#[test]
fn 并发名额最多八个_等确认的成员让出名额() {
    let mut s = Sim::new();
    for i in 0..10 {
        let child = format!("子{i:02}");
        s.spawn_v1(&child, &format!("派生{i}"), &format!("子轮{i}"));
        s.never.insert(child);
    }
    // 扣住所有子线程的 goal 读：名额满时其余排队。
    for i in 0..10 {
        s.hold.push(("thread/goal/get".into(), format!("子{i:02}")));
    }
    stop(&mut s, OWNER, json!({"threadId": P}));
    let tree = s.tree_op().unwrap();
    assert_eq!(s.m.trees.operations[&tree].in_flight, MAX_TREE_REQUESTS);
    let goal_reads = s
        .held
        .iter()
        .filter(|c| c.method == "thread/goal/get")
        .count();
    assert_eq!(goal_reads, MAX_TREE_REQUESTS, "最多 8 个在途请求");
    // 放行：前 8 个发出中断、等确认，让出名额，余下 2 个才开始。
    for i in 0..10 {
        s.release("thread/goal/get", &format!("子{i:02}"));
    }
    let interrupted = (0..10)
        .filter(|i| s.interrupts(&s.log, &format!("子{i:02}")) > 0)
        .count();
    assert_eq!(interrupted, 10, "让出名额后全部成员都发出中断");
    assert_eq!(s.m.trees.operations[&tree].in_flight, 0);
}

#[test]
fn 子线程永不回应_四十秒返回unconfirmed_之后每十五秒继续核实() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.never.insert("子甲".into());
    let (id, _) = stop(&mut s, OWNER, json!({"threadId": P}));
    let actions = s.advance(FIRST_DELIVERY_MS - 1);
    assert!(s.response(&actions, id).is_none(), "未到 40 秒不交付");
    let actions = s.advance(1);
    let reply = s.response(&actions, id).expect("40 秒交付")["result"].clone();
    assert_eq!(reply["stopOutcome"], "unconfirmed");
    assert_eq!(reply["childrenStop"]["phase"], "unconfirmed");
    assert_eq!(reply["childrenStop"]["unconfirmed"], json!(["子甲"]));
    // 清理与屏障继续：之后每 15 秒再核实（读取层的元数据读）并重发中断。
    let before = s.log.len();
    s.advance(15_000);
    let reads = s.requests(&s.log[before..], "thread/read", "子甲").len();
    assert!(reads >= 1, "15 秒后继续核实");
    assert!(s.interrupts(&s.log[before..], "子甲") >= 1);
    assert!(s.tree_op().is_some());
    // 子线程终于停下：确认并解除屏障。
    s.never.clear();
    s.advance(15_000);
    assert!(s.tree_op().is_none());
    assert_eq!(s.record().stop_outcome, Some(StopOutcome::Confirmed));
}

#[test]
fn 停止期间派生的新子线程纳入同一次操作_期限不因新成员重置() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.never.insert("子甲".into());
    s.never.insert(P.into());
    let (id, _) = stop(&mut s, OWNER, json!({"threadId": P}));
    let tree = s.tree_op().unwrap();
    let deadline = s.m.trees.operations[&tree]
        .request
        .first_delivery_deadline_ms;
    s.advance(20_000);
    // 父轮还没停下，又派生了一个子线程：纳入同一次停止并被中断。
    let start = s.log.len();
    s.spawn_v1("子乙", "派生2", "子轮乙");
    assert_eq!(s.tree_op(), Some(tree), "合并进同一次操作");
    assert_eq!(s.interrupts(&s.log[start..], "子乙"), 1, "新成员被中断");
    assert_eq!(
        s.m.trees.operations[&tree]
            .request
            .first_delivery_deadline_ms,
        deadline,
        "新增成员不重置首次期限"
    );
    let waits: Vec<u64> = s
        .timers
        .iter()
        .filter(
            |(_, _, _, k)| matches!(k, TimerKind::TreeStopWait { tree_op, .. } if *tree_op == tree),
        )
        .map(|(at, ..)| *at)
        .collect();
    assert!(waits.iter().all(|at| *at <= deadline), "等待计时没有续期");
    let actions = s.advance(20_000);
    let reply = s.response(&actions, id).expect("从进入起 40 秒交付");
    assert_eq!(reply["result"]["stopOutcome"], "unconfirmed");
}

#[test]
fn 多个调用者合并进同一次操作且各自计时() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.never.insert("子甲".into());
    let (first, _) = stop(&mut s, OWNER, json!({"threadId": P}));
    s.advance(20_000);
    let (second, _) = stop(&mut s, 2, json!({"threadId": P}));
    assert_eq!(s.m.trees.operations.len(), 1, "多调用者合并");
    let actions = s.advance(20_000);
    assert!(
        s.response(&actions, first).is_some(),
        "第一个调用者 40 秒到期"
    );
    assert!(
        s.response(&actions, second).is_none(),
        "第二个调用者从自己进入起计时"
    );
    let actions = s.advance(20_000);
    assert!(s.response(&actions, second).is_some());
}

#[test]
fn 指定run只停该运行范围_其他认领的成员记冲突不中断() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    // 子线程上另有独立认领的新轮次（来自另一个客户端）：不属于运行 R。
    s.turn_completed("子甲", "子轮甲", "completed");
    s.m.records.entry("子甲".into()).or_default().subscribed = true;
    let start_actions = s.client(
        2,
        "turn/start",
        json!({"threadId": "子甲", "input": [{"type": "text", "text": "直接输入"}]}),
        Some("运行乙"),
    );
    let start = start_actions.iter().find_map(|a| match a {
        Action::Request(c) if c.method == "turn/start" => Some(c.clone()),
        _ => None,
    });
    let start = start.expect("能力未知的子线程交给上游校验，外部认领照常开工");
    {
        s.turns
            .entry("子甲".into())
            .or_default()
            .push(("外部轮".into(), "inProgress".into()));
        s.apply(Event::Reply {
            context: start,
            result: Ok(json!({"turn": {"id": "外部轮"}})),
        });
    }
    let before = s.log.len();
    let (id, _) = stop(&mut s, OWNER, json!({"threadId": P, "runId": R}));
    let actions = s.log[before..].to_vec();
    assert_eq!(
        s.interrupts(&actions, "子甲"),
        0,
        "外部轮次不属于 R，不中断"
    );
    let actions = s.advance(FIRST_DELIVERY_MS);
    let reply = s.response(&actions, id).expect("40 秒交付")["result"].clone();
    assert_eq!(reply["stopOutcome"], "unconfirmed");
    let conflicts = reply["childrenStop"]["conflicts"].as_array().unwrap();
    assert!(
        conflicts.iter().any(|c| c["thread"] == "子甲"),
        "其他运行的活动成员列为冲突：{reply}"
    );
}

#[test]
fn 结束快照期间客户端停止不误报已停止() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.never.insert("子甲".into());
    s.turn_completed(P, T, "completed");
    assert!(s.finishing());
    let (id, _) = stop(&mut s, 2, json!({"threadId": P}));
    let actions = s.advance(FIRST_DELIVERY_MS);
    let reply = s.response(&actions, id).expect("交付")["result"].clone();
    assert_eq!(reply["alreadyStopped"], false);
    assert_eq!(reply["stopOutcome"], "unconfirmed");
    // 已提交但未确认：带 runId 的停止也不走「已结束即 alreadyStopped」的快捷出口。
    assert_eq!(s.record().state, RunState::Ended);
    let (id, actions) = stop(&mut s, 2, json!({"threadId": P, "runId": R}));
    assert!(
        s.response(&actions, id).is_none(),
        "合并进未确认的树停止，等待核实"
    );
    let actions = s.advance(FIRST_DELIVERY_MS);
    assert_eq!(
        s.response(&actions, id).unwrap()["result"]["alreadyStopped"],
        false
    );
}

#[test]
fn 树停止未确认时算忙_不退订_不淘汰_拒绝新工作() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.never.insert("子甲".into());
    s.turn_completed(P, T, "completed");
    s.advance(FIRST_DELIVERY_MS);
    assert_eq!(s.record().state, RunState::Ended, "已提交 unconfirmed");
    assert!(!s.m.claims.contains_key(P));
    assert!(s.m.tree_busy(None));
    assert!(s.m.tree_busy(Some(P)));
    assert!(s.m.tree_busy(Some("子甲")));
    assert!(s.m.inbox_busy(), "收件箱空闲判定算忙");
    assert!(s.m.runs.tree_retention_required(R), "未确认运行不淘汰");
    s.m.runs.evict();
    assert!(s.m.runs.runs.contains_key(R));
    // 自动退订：屏障期间不发 thread/unsubscribe。
    let unsubscribes = s.requests(&s.log, "thread/unsubscribe", P).len();
    assert_eq!(unsubscribes, 0);
    // 新工作准入：根线程与成员上的新运行、续跑、开工都被屏障拒绝。
    let (id, actions) = s.call(
        OWNER,
        "turn/start",
        json!({"threadId": P, "input": [{"type": "text", "text": "新任务"}]}),
    );
    let error = s.response(&actions, id).unwrap()["error"].clone();
    assert_eq!(error["data"]["reason"], "treeStopping");
    assert_eq!(error["code"], crate::proto::code::THREAD_BUSY);
    let (id, actions) = s.call(
        OWNER,
        "turn/start",
        json!({"threadId": "子甲", "input": [{"type": "text", "text": "直接输入"}]}),
    );
    assert_eq!(
        s.response(&actions, id).unwrap()["error"]["data"]["reason"],
        "treeStopping"
    );
    // 确认后解除。
    s.never.clear();
    s.advance(15_000);
    assert!(!s.m.tree_busy(None));
    assert!(!s.m.runs.tree_retention_required(R));
}

#[test]
fn 空转刹车走整树停止() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.hold.push(("turn/interrupt".into(), "子甲".into()));
    // 直接用刹车的停止入口（GoalLoop）：有受管子树时改走整树停止。
    let mut actions = Vec::new();
    s.m.run_loop_guard(
        R,
        crate::runs::LoopGuardReport {
            kind: crate::runs::LoopGuardKind::NoAction,
            count: 3,
            threshold: 3,
            turn_ids: vec![T.into()],
            action: crate::runs::LoopGuardAction::Stop,
            pause: None,
        },
        &mut actions,
    );
    let mut m = std::mem::take(&mut s.m);
    m.begin_stop(P, crate::claim::StopReason::GoalLoop, false, &mut actions);
    s.m = m;
    let tree = s.tree_op().expect("刹车升级为整树停止");
    assert_eq!(
        s.m.trees.operations[&tree].request.trigger,
        TreeTrigger::LoopGuard
    );
    s.absorb(actions);
    s.release("turn/interrupt", "子甲");
    let record = s.record();
    assert_eq!(record.state, RunState::Ended);
    assert_eq!(record.interrupt_reason, Some(InterruptReason::GoalLoop));
    assert!(
        record.loop_guard.is_some(),
        "保留 loopGuard 报告，运行消费者沿用退出 9"
    );
}

#[test]
fn owner断开发起整树停止_原因为客户端断开() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.apply(Event::Disconnected { conn: OWNER });
    assert!(s.tree_op().is_none(), "断开后的停止已确认");
    let record = s.record();
    assert_eq!(record.state, RunState::Ended);
    assert_eq!(
        record.interrupt_reason,
        Some(InterruptReason::ClientDisconnected)
    );
    assert_eq!(
        record.children_stop.as_ref().unwrap().trigger,
        TreeTrigger::OwnerGone
    );
}

#[test]
fn 无受管子树时停止保持原单线程行为() {
    let mut s = Sim::new();
    let (id, actions) = stop(&mut s, OWNER, json!({"threadId": P}));
    assert!(s.tree_op().is_none());
    assert!(s.m.trees.operations.is_empty());
    let reply = s.response(&actions, id).unwrap()["result"].clone();
    assert_eq!(reply["stopOutcome"], "confirmed");
    assert!(reply.get("childrenStop").is_none());
    assert!(s.record().children_stop.is_none());
}

#[test]
fn 报告随核实推进_停止报告与停止结论和检查项一致() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.never.insert("子甲".into());
    stop(&mut s, OWNER, json!({"threadId": P}));
    // 提交前 pending：只保留当前检查，不宣告结论。
    let record = s.record().clone();
    let report = record.children_stop.clone().unwrap();
    assert_eq!(report.phase, TreePhase::Pending);
    assert_ne!(record.stop_outcome, Some(StopOutcome::Confirmed));
    assert!(record.stop_check.is_some_and(|c| !c.turn_ended));
    s.advance(FIRST_DELIVERY_MS);
    let record = s.record().clone();
    assert_eq!(
        record.children_stop.as_ref().unwrap().phase,
        TreePhase::Unconfirmed
    );
    assert_eq!(record.stop_outcome, Some(StopOutcome::Unconfirmed));
    let check = record.stop_check.unwrap();
    assert!(!check.turn_ended, "子线程没有停止证据");
    assert!(check.ownership);
}

#[test]
fn 直接停止树停止范围内的子线程_合并进树停止不误报已停止() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.never.insert("子甲".into());
    s.turn_completed(P, T, "completed");
    let tree = s.tree_op().unwrap();
    let (id, actions) = stop(&mut s, 2, json!({"threadId": "子甲"}));
    assert!(s.response(&actions, id).is_none(), "合并进树停止，等待核实");
    assert_eq!(s.tree_op(), Some(tree));
    let actions = s.advance(FIRST_DELIVERY_MS);
    let reply = s.response(&actions, id).unwrap()["result"].clone();
    assert_eq!(reply["alreadyStopped"], false);
    assert_eq!(reply["stopOutcome"], "unconfirmed");
    assert_eq!(reply["threadId"], "子甲");
}
