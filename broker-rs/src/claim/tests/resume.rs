//! 固定先核实再恢复订阅，覆盖预检、目标事实与迟到收尾，不依赖固定睡眠。
use super::*;
use crate::runs::{InterruptReason, RunState, StopOutcome};

const OLD: &str = "run-原运行";
const NEW: &str = "run-续跑";
fn step_raw(machine: &mut Machine, event: Event) -> Vec<Action> {
    let (next, actions) = step(std::mem::take(machine), event);
    *machine = next;
    assert!(
        machine.continue_prechecks_valid(),
        "每个事件之后预检登记必须保持不变量"
    );
    actions
}

fn response(
    machine: &mut Machine,
    context: &RequestContext,
    result: Result<Value, Value>,
) -> Vec<Action> {
    step_raw(
        machine,
        Event::Reply {
            context: context.clone(),
            result,
        },
    )
}

fn fixture(subscribed: bool) -> Machine {
    let mut machine = Machine::new(false);
    machine.live_conns.extend([1, 2]);
    machine.cxb_conns.insert(1);
    machine.run_opened(
        RunOpen {
            thread_id: "线程".into(),
            run_id: OLD.into(),
            conn: None,
            kind: ReqKind::TurnStart,
            joined: false,
            input: "旧输入".into(),
            continued_from: None,
        },
        &mut Vec::new(),
    );
    machine.run_released(OLD, ReleaseKind::StopConfirmed, &mut Vec::new());
    let run = machine.runs.runs.get_mut(OLD).unwrap();
    run.state = RunState::Ended;
    run.interrupt_reason = Some(InterruptReason::Stop);
    run.stop_outcome = Some(StopOutcome::Confirmed);
    machine.record("线程").subscribed = subscribed;
    machine.record("线程").last_run = Some(OLD.into());
    machine
        .record("线程")
        .pending_meta
        .insert(1, json!({"source":"run","template":"测试"}));
    machine.runs.hook_log.clear();
    machine.runs.notices.clear();
    machine
}

fn begin(machine: &mut Machine) -> Vec<Action> {
    step_raw(
        machine,
        Event::ClientRequest {
            request: request(
                1,
                42,
                "turn/start",
                json!({"threadId":"线程","input":[{"type":"text","text":"接着做"}],"cxbContinue":{"fromRunId":OLD}}),
            ),
            run_id: Some(NEW.into()),
        },
    )
}

fn requests(actions: &[Action]) -> Vec<RequestContext> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::Request(context) => Some(context.clone()),
            _ => None,
        })
        .collect()
}

fn normal_result(context: &RequestContext) -> Value {
    match context.method.as_str() {
        "thread/resume" => json!({"thread":{"id":"线程"}}),
        "thread/read" => {
            json!({"thread":{"id":"线程","turns":[{"id":"历史轮","status":"completed"}]}})
        }
        "thread/goal/get" => json!({"goal":{"status":"paused"}}),
        _ => panic!("不应自动回答开工请求"),
    }
}

/// 一批并发读算一个往返；只统计转发 turn/start 之前的请求。
fn pump(machine: &mut Machine, initial: Vec<Action>) -> (Vec<Action>, usize, usize, Vec<String>) {
    let mut all = initial.clone();
    let mut batch = requests(&initial);
    let mut reads = 0;
    let mut sequence = Vec::new();
    for trips in 0..15 {
        let controls: Vec<_> = batch
            .into_iter()
            .filter(|c| {
                matches!(
                    c.operation,
                    Operation::ContinueResume | Operation::ContinueRead(_)
                )
            })
            .collect();
        if controls.is_empty() {
            return (all, reads, trips, sequence);
        }
        batch = Vec::new();
        for context in controls {
            reads += usize::from(context.method != "thread/resume");
            sequence.push(context.method.clone());
            let next = response(machine, &context, Ok(normal_result(&context)));
            batch.extend(requests(&next));
            all.extend(next);
        }
    }
    panic!("预检未在有界轮次内结束")
}

fn reason(actions: &[Action]) -> Option<String> {
    sends(actions).into_iter().find_map(|(_, message)| {
        message
            .pointer("/error/data/reason")
            .and_then(Value::as_str)
            .map(str::to_owned)
    })
}

fn n7_gate(machine: &mut Machine) {
    use crate::store::lifecycle::{File, Kind, Phase, Record};
    machine.lifecycle_restore(Ok(File {
        records: std::collections::BTreeMap::from([(
            "线程".into(),
            Record {
                operation_id: "恢复操作".into(),
                kind: Kind::Archive,
                thread_id: "线程".into(),
                short_id: "12345678".into(),
                preview: None,
                phase: Phase::Forwarded,
                attempts: vec![],
                created_at: 0,
                updated_at: 0,
            },
        )]),
    }));
}

#[test]
fn n7_续跑预检已受理但提交时遇门禁不能开工() {
    let mut machine = fixture(true);
    let a = begin(&mut machine);
    n7_gate(&mut machine);
    let expected = machine.lifecycle_error("线程").unwrap();
    let (all, _, _, _) = pump(&mut machine, a);
    assert!(!requests(&all).iter().any(|r| r.method == "turn/start"));
    assert_eq!(reason(&all).as_deref(), Some("lifecyclePending"));
    let errors: Vec<_> = sends(&all)
        .into_iter()
        .filter_map(|(_, message)| message.get("error").cloned())
        .collect();
    assert_eq!(
        errors,
        vec![expected],
        "提交点必须保留操作编号、种类、阶段和实际线程"
    );
    no_run(&machine);
}

#[test]
fn n7_恢复订阅屏障已受理但回包时遇门禁按失败出口结算() {
    let mut machine = Machine::new(false);
    machine.live_conns.insert(1);
    let a = step_raw(
        &mut machine,
        client(request(1, 71, "turn/start", json!({"threadId":"线程"}))),
    );
    let resume = first_request(&a);
    assert_eq!(resume.method, "thread/resume");
    n7_gate(&mut machine);
    let a = response(&mut machine, &resume, Ok(json!({"thread":{"id":"线程"}})));
    assert_eq!(reason(&a).as_deref(), Some("lifecyclePending"));
    assert!(!requests(&a).iter().any(|r| r.method == "turn/start"));
    assert!(!machine.claims.contains_key("线程"));
}

#[test]
fn n7_目标恢复读回包遇门禁不再写入活动目标() {
    let (mut machine, read) = goal_read();
    n7_gate(&mut machine);
    let a = response(
        &mut machine,
        &read,
        Ok(json!({"goal":{"status":"paused","objective":"目标"}})),
    );
    assert!(!requests(&a).iter().any(|r| r.method == "thread/goal/set"));
    assert_eq!(machine.goal_restores, Default::default());
}

fn no_run(machine: &Machine) {
    assert!(!machine.claims.contains_key("线程"));
    assert!(!machine.runs.runs.contains_key(NEW));
    assert!(machine.stream_slots.is_empty());
    assert_eq!(machine.continues, Default::default());
    assert!(!machine
        .runs
        .hook_log
        .iter()
        .any(|name| name == "run_opened" || name == "run_released"));
}

#[test]
fn 合法续跑固定请求顺序及控制请求名额隔离() {
    for subscribed in [false, true] {
        let mut machine = fixture(subscribed);
        let initial = begin(&mut machine);
        assert!(machine.claims["线程"].precheck());
        assert_eq!(machine.claims["线程"].run_id, None);
        assert_eq!(machine.stream_slots.len(), 1);
        assert!(requests(&initial)
            .iter()
            .all(|context| context.stream_id.is_none()));
        let (actions, reads, trips, sequence) = pump(&mut machine, initial);
        let stream = requests(&actions)
            .into_iter()
            .find(|context| context.method == "turn/start")
            .unwrap();
        assert!(stream.params.as_ref().unwrap().get("cxbContinue").is_none());
        assert_eq!(machine.stream_slots.len(), 1);
        assert_eq!(machine.runs.runs[NEW].continued_from.as_deref(), Some(OLD));
        assert_eq!(machine.runs.runs[NEW].input, "接着做");
        assert_eq!(
            machine.runs.runs[NEW].meta.as_ref().unwrap()["template"],
            "测试"
        );
        assert!(!machine.records["线程"].pending_meta.contains_key(&1));
        let start_notice = actions.iter().position(|action| matches!(action, Action::Send { message, .. } if message["method"] == "broker/run/started")).unwrap();
        let forwarding = actions
            .iter()
            .position(|action| matches!(action, Action::Request(c) if c.method == "turn/start"))
            .unwrap();
        assert!(start_notice < forwarding);
        assert_eq!((reads, trips), if subscribed { (2, 1) } else { (4, 3) });
        let expected = if subscribed {
            vec!["thread/read", "thread/goal/get"]
        } else {
            vec![
                "thread/read",
                "thread/goal/get",
                "thread/resume",
                "thread/read",
                "thread/goal/get",
            ]
        };
        assert_eq!(sequence, expected);
        response(&mut machine, &stream, Ok(json!({"turn":{"id":"新轮"}})));
        assert!(machine.stream_slots.is_empty());
    }
}

#[test]
fn 内存拒绝矩阵与失败保留元数据() {
    for case in 0..9 {
        let mut machine = fixture(false);
        let expected = match case {
            0 => {
                machine.runs.runs.remove(OLD);
                "unknownRun"
            }
            1 => {
                machine.runs.runs.get_mut(OLD).unwrap().thread_id = "别的线程".into();
                "runMismatch"
            }
            2 => {
                machine
                    .runs
                    .index
                    .threads
                    .values_mut()
                    .next()
                    .unwrap()
                    .last_run_id = None;
                "notLatestRun"
            }
            3 => {
                machine.runs.runs.get_mut(OLD).unwrap().interrupt_reason = None;
                "notResumable"
            }
            4 => {
                machine.runs.runs.get_mut(OLD).unwrap().joined = true;
                "notResumable"
            }
            5 => {
                machine.runs.runs.get_mut(OLD).unwrap().review_thread_id = Some("审查".into());
                "notResumable"
            }
            6 => {
                machine.runs.runs.get_mut(OLD).unwrap().meta = Some(json!({"source":"review"}));
                "notResumable"
            }
            7 => {
                machine.runs.runs.get_mut(OLD).unwrap().state = RunState::Stopping;
                "stopping"
            }
            _ => {
                machine.closing = true;
                "shuttingDown"
            }
        };
        let initial = begin(&mut machine);
        assert!(requests(&initial).is_empty(), "内存拒绝不得发出 RPC");
        let (actions, _, _, _) = pump(&mut machine, initial);
        assert_eq!(reason(&actions).as_deref(), Some(expected), "用例 {case}");
        no_run(&machine);
        assert!(machine.records["线程"].pending_meta.contains_key(&1));
        assert_eq!(machine.records["线程"].last_run.as_deref(), Some(OLD));
    }
}

#[test]
fn 参数校验不占名额且普通开工不走预检() {
    for params in [
        json!({"cxbContinue":{"fromRunId":OLD}}),
        json!({"threadId":1,"cxbContinue":{"fromRunId":OLD}}),
        json!({"threadId":"线程","cxbContinue":null}),
        json!({"threadId":"线程","cxbContinue":{"fromRunId":3}}),
        json!({"threadId":"线程","cxbContinue":{"fromRunId":OLD,"resumeGoal":"否"}}),
    ] {
        let mut machine = fixture(true);
        let actions = step_raw(
            &mut machine,
            Event::ClientRequest {
                request: request(1, 42, "turn/start", params),
                run_id: Some(NEW.into()),
            },
        );
        assert_eq!(sends(&actions)[0].1["error"]["code"], -32602);
        no_run(&machine);
    }
    let mut machine = fixture(true);
    let actions = step_raw(
        &mut machine,
        client(request(1, 42, "turn/start", json!({"threadId":"线程"}))),
    );
    assert_eq!(first_request(&actions).method, "turn/start");
    assert_eq!(machine.continues, Default::default());
}

fn read_batch(machine: &mut Machine) -> Vec<RequestContext> {
    let controls = requests(&begin(machine));
    assert_eq!(
        controls
            .iter()
            .map(|context| context.method.as_str())
            .collect::<Vec<_>>(),
        ["thread/read", "thread/goal/get"]
    );
    controls
}

/// 首次核实通过后停在恢复订阅回包屏障。
fn resume_barrier(machine: &mut Machine) -> RequestContext {
    let reads = read_batch(machine);
    let mut actions = Vec::new();
    for context in reads {
        actions.extend(response(machine, &context, Ok(normal_result(&context))));
    }
    let controls = requests(&actions);
    assert_eq!(controls.len(), 1);
    assert_eq!(controls[0].method, "thread/resume");
    assert!(machine.claims["线程"].precheck());
    controls[0].clone()
}

#[test]
fn 服务器拒绝与从严解析矩阵() {
    for case in 0..8 {
        let mut machine = fixture(false);
        if case == 1 {
            machine.runs.runs.get_mut(OLD).unwrap().state = RunState::Lost;
        }
        let controls = read_batch(&mut machine);
        let (method, result, expected) = match case {
            0 | 1 => (
                "thread/read",
                Ok(json!({"thread":{"turns":[{"id":"活动轮","status":"inProgress"}]}})),
                if case == 0 { "threadBusy" } else { "stopping" },
            ),
            2 => (
                "thread/goal/get",
                Ok(json!({"goal":{"status":"active"}})),
                "stopping",
            ),
            3 => (
                "thread/read",
                Err(json!({"message":"读失败"})),
                "resumeCheckFailed",
            ),
            4 => ("thread/read", Ok(json!({"thread":{}})), "resumeCheckFailed"),
            5 => ("thread/goal/get", Ok(json!({})), "resumeCheckFailed"),
            6 => (
                "thread/goal/get",
                Ok(json!({"goal":{"status":32}})),
                "resumeCheckFailed",
            ),
            _ => (
                "thread/goal/get",
                Err(json!({"message":"读取目标失败"})),
                "resumeCheckFailed",
            ),
        };
        let context = controls.iter().find(|c| c.method == method).unwrap();
        let actions = response(&mut machine, context, result);
        assert_eq!(reason(&actions).as_deref(), Some(expected), "用例 {case}");
        for context in controls {
            response(&mut machine, &context, Ok(normal_result(&context)));
        }
        no_run(&machine);
    }
}

#[test]
fn 并发续跑与预检名额上限() {
    let mut machine = fixture(false);
    begin(&mut machine);
    let actions = step_raw(
        &mut machine,
        Event::ClientRequest {
            request: request(
                2,
                43,
                "turn/start",
                json!({"threadId":"线程","cxbContinue":{"fromRunId":OLD}}),
            ),
            run_id: Some("第二运行".into()),
        },
    );
    assert_eq!(reason(&actions).as_deref(), Some("threadBusy"));
    assert_eq!(machine.stream_slots.len(), 1);
    machine.stream_slots.extend(100..163);
    let actions = step_raw(
        &mut machine,
        Event::ClientRequest {
            request: request(
                2,
                44,
                "turn/start",
                json!({"threadId":"其他线程","cxbContinue":{"fromRunId":OLD}}),
            ),
            run_id: Some("第三运行".into()),
        },
    );
    assert_eq!(reason(&actions).as_deref(), Some("tooManyUnresolvedStarts"));
}

#[test]
fn 取消出口迟到回包与通知隔离() {
    for case in 0..4 {
        let mut machine = fixture(false);
        let initial = begin(&mut machine);
        let controls = requests(&initial);
        let ignored = step_raw(&mut machine, turn("turn/completed", "历史轮"));
        assert!(!requests(&ignored).iter().any(|c| c.method == "turn/start"));
        assert!(machine.claims["线程"].precheck());
        let event = match case {
            0 => Event::Disconnected { conn: 1 },
            1 => Event::Shutdown,
            2 => turn("turn/started", "外部轮"),
            _ => Event::ClientRequest {
                request: request(2, 88, "broker/threads/stop", json!({"threadId":"线程"})),
                run_id: None,
            },
        };
        let actions = step_raw(&mut machine, event);
        let expected = match case {
            0 => None,
            1 => Some("shuttingDown"),
            2 => Some("threadBusy"),
            _ => Some("continueCancelled"),
        };
        assert_eq!(reason(&actions).as_deref(), expected);
        for context in controls {
            let late = response(&mut machine, &context, Ok(normal_result(&context)));
            assert!(!requests(&late).iter().any(|c| c.method == "turn/start"));
            if case != 1 && context.method == "thread/resume" {
                assert!(late.iter().any(|a| matches!(
                    a,
                    Action::Schedule {
                        kind: TimerKind::Unsubscribe,
                        ..
                    }
                )));
            }
        }
        assert!(!machine.runs.runs.contains_key(NEW));
        assert!(machine.stream_slots.is_empty());
        assert_eq!(machine.continues, Default::default());
        if case == 0 {
            assert!(!machine.records["线程"].pending_meta.contains_key(&1));
        }
    }
}

#[test]
fn 恢复订阅失败不能用成功读取放行() {
    let mut machine = fixture(false);
    let context = &resume_barrier(&mut machine);
    let actions = response(
        &mut machine,
        context,
        Err(json!({"message":"恢复订阅失败"})),
    );
    assert_eq!(reason(&actions).as_deref(), Some("resumeCheckFailed"));
    no_run(&machine);
    response(&mut machine, context, Ok(normal_result(context)));
    no_run(&machine);
}

#[test]
fn 核实版本重读上限和乱序重复回包() {
    let mut machine = fixture(true);
    let mut batch = read_batch(&mut machine);
    for round in 0..3 {
        step_raw(
            &mut machine,
            notify(
                "thread/goal/updated",
                json!({"threadId":"线程","goal":{"status":"paused"}}),
            ),
        );
        let actions = response(&mut machine, &batch[0], Ok(normal_result(&batch[0])));
        let late = response(&mut machine, &batch[1], Ok(normal_result(&batch[1])));
        assert!(requests(&late).is_empty());
        if round == 2 {
            assert_eq!(reason(&actions).as_deref(), Some("resumeCheckFailed"));
            no_run(&machine);
        } else {
            batch = requests(&actions);
            assert_eq!(batch.len(), 2);
        }
    }
    let mut machine = fixture(true);
    let batch = read_batch(&mut machine);
    let goal = batch
        .iter()
        .find(|c| c.method == "thread/goal/get")
        .unwrap();
    let thread = batch.iter().find(|c| c.method == "thread/read").unwrap();
    response(&mut machine, goal, Ok(normal_result(goal)));
    assert!(requests(&response(&mut machine, goal, Ok(normal_result(goal)))).is_empty());
    let actions = response(&mut machine, thread, Ok(normal_result(thread)));
    assert_eq!(
        requests(&actions)
            .iter()
            .filter(|c| c.method == "turn/start")
            .count(),
        1
    );
    assert!(requests(&response(&mut machine, thread, Ok(normal_result(thread)))).is_empty());
}

#[test]
fn 最后复查拦截最新运行与在途目标变更() {
    for change in [false, true] {
        let mut machine = fixture(true);
        let batch = read_batch(&mut machine);
        response(&mut machine, &batch[0], Ok(normal_result(&batch[0])));
        if change {
            step_raw(
                &mut machine,
                Event::ClientRequest {
                    request: request(
                        2,
                        99,
                        "thread/goal/set",
                        json!({"threadId":"线程","status":"paused"}),
                    ),
                    run_id: None,
                },
            );
        } else {
            machine
                .runs
                .index
                .threads
                .values_mut()
                .next()
                .unwrap()
                .last_run_id = None;
        }
        let actions = response(&mut machine, &batch[1], Ok(normal_result(&batch[1])));
        assert_eq!(
            reason(&actions).as_deref(),
            Some(if change { "stopping" } else { "notLatestRun" })
        );
        no_run(&machine);
    }
}

fn opened(shared: bool) -> (Machine, RequestContext) {
    let mut machine = fixture(true);
    machine.shared = shared;
    let initial = begin(&mut machine);
    let (actions, _, _, _) = pump(&mut machine, initial);
    let stream = requests(&actions)
        .into_iter()
        .find(|c| c.method == "turn/start")
        .unwrap();
    (machine, stream)
}

fn goal_read() -> (Machine, RequestContext) {
    let (mut machine, stream) = opened(false);
    response(&mut machine, &stream, Ok(json!({"turn":{"id":"新轮"}})));
    let actions = step_raw(&mut machine, turn("turn/started", "新轮"));
    let read = requests(&actions)
        .into_iter()
        .find(|c| matches!(c.operation, Operation::GoalRestore(_)))
        .unwrap();
    (machine, read)
}

fn active_write() -> (Machine, RequestContext) {
    let (mut machine, read) = goal_read();
    let actions = response(
        &mut machine,
        &read,
        Ok(json!({"goal":{"status":"paused","objective":"保持","tokenBudget":123}})),
    );
    let write = first_request(&actions);
    assert_eq!(
        write.params,
        Some(json!({"threadId":"线程","status":"active"}))
    );
    assert!(matches!(write.operation, Operation::GoalChange { .. }));
    (machine, write)
}

#[test]
fn 恢复写入回包迟到_外部已换目标_自有轮里也不经通知分支授予资格() {
    // 自有客户端轮里，恢复写入还没回包；外部把目标换成别的、暂停再恢复（资格已撤销），旧回包才到。
    let (mut machine, write) = active_write();
    for status in ["paused", "active"] {
        step_raw(
            &mut machine,
            notify(
                "thread/goal/updated",
                json!({"threadId":"线程","goal":{"threadId":"线程","objective":"别人的目标","status":status,"createdAt":5}}),
            ),
        );
    }
    assert!(!machine.records["线程"].goal_controlled);
    response(
        &mut machine,
        &write,
        Ok(
            json!({"goal":{"threadId":"线程","status":"active","objective":"保持","tokenBudget":123}}),
        ),
    );
    assert_eq!(
        machine.records["线程"].goal_objective.as_deref(),
        Some("保持"),
        "回包确实被学习"
    );
    assert!(!machine.records["线程"].goal_controlled);
}

#[test]
fn 恢复写入超时后迟到成功仍授予空转刹车控制资格() {
    // 迟到回包会先在 late_reply、再在 apply_reply 各进一次解包：资格代号不能在第二次被清掉。
    let (mut machine, write) = active_write();
    step_raw(
        &mut machine,
        Event::TimedOut {
            tag: 7,
            deadline: std::time::Instant::now(),
            context: write.clone(),
            error: json!({"code": -32000, "message": "超时"}),
        },
    );
    assert!(!machine.records["线程"].goal_controlled);
    step_raw(
        &mut machine,
        Event::LateReply {
            context: write,
            result: Ok(
                json!({"goal":{"threadId":"线程","status":"active","objective":"保持","tokenBudget":123}}),
            ),
        },
    );
    assert_eq!(
        machine.records["线程"].goal_status.as_deref(),
        Some("active")
    );
    assert!(machine.records["线程"].goal_controlled);
}

#[test]
fn 目标恢复首次触发与关闭清表() {
    let (mut machine, write) = active_write();
    assert!(requests(&step_raw(&mut machine, turn("turn/started", "新轮"))).is_empty());
    response(
        &mut machine,
        &write,
        Ok(json!({"goal":{"status":"active"}})),
    );
    assert!(machine.records["线程"].goal_active);
    assert!(!machine.claims["线程"].resume_goal);
    let (mut machine, _) = goal_read();
    step_raw(&mut machine, Event::Shutdown);
    assert_eq!(machine.goal_restores, Default::default());
}

#[test]
fn 目标读取结束换代禁用以及变化重读() {
    for ended in [true, false] {
        let (mut machine, read) = goal_read();
        if ended {
            step_raw(&mut machine, turn("turn/completed", "新轮"));
        } else {
            machine.claims.get_mut("线程").unwrap().claim_id += 1;
        }
        let actions = response(&mut machine, &read, Ok(json!({"goal":{"status":"paused"}})));
        assert!(!requests(&actions)
            .iter()
            .any(|c| c.method == "thread/goal/set"));
    }
    let (mut machine, mut read) = goal_read();
    for round in 0..3 {
        step_raw(
            &mut machine,
            notify(
                "thread/goal/updated",
                json!({"threadId":"线程","goal":{"status":"paused"}}),
            ),
        );
        let actions = response(&mut machine, &read, Ok(json!({"goal":{"status":"paused"}})));
        if round < 2 {
            let next = first_request(&actions);
            assert_ne!(next.operation, read.operation);
            assert!(requests(&response(
                &mut machine,
                &read,
                Ok(json!({"goal":{"status":"paused"}}))
            ))
            .is_empty());
            read = next;
        } else {
            assert!(requests(&actions).is_empty());
            assert!(actions.iter().any(|a| matches!(a, Action::Warn(_))));
        }
    }
    let mut machine = fixture(true);
    let initial = step_raw(
        &mut machine,
        Event::ClientRequest {
            request: request(
                1,
                42,
                "turn/start",
                json!({"threadId":"线程","cxbContinue":{"fromRunId":OLD,"resumeGoal":false}}),
            ),
            run_id: Some(NEW.into()),
        },
    );
    let (actions, _, _, _) = pump(&mut machine, initial);
    let stream = requests(&actions)
        .into_iter()
        .find(|c| c.method == "turn/start")
        .unwrap();
    response(&mut machine, &stream, Ok(json!({"turn":{"id":"新轮"}})));
    assert!(requests(&step_raw(&mut machine, turn("turn/started", "新轮"))).is_empty());
}

#[test]
fn 写入晚于自然结束补暂停且不动新认领() {
    for replaced in [false, true] {
        let (mut machine, write) = active_write();
        if replaced {
            machine.claims.get_mut("线程").unwrap().claim_id += 1;
        } else {
            step_raw(&mut machine, turn("turn/completed", "新轮"));
        }
        let actions = response(
            &mut machine,
            &write,
            Ok(json!({"goal":{"status":"active"}})),
        );
        let pauses: Vec<_> = requests(&actions)
            .into_iter()
            .filter(|c| c.params.as_ref().is_some_and(|p| p["status"] == "paused"))
            .collect();
        assert_eq!(pauses.len(), usize::from(!replaced));
        if let Some(pause) = pauses.first() {
            response(&mut machine, pause, Ok(json!({"goal":{"status":"paused"}})));
            assert!(!machine.records["线程"].goal_active);
        }
    }
}

#[test]
fn 目标写入在途停止等待与未知到期补制动() {
    let (mut machine, write) = active_write();
    let actions = step_raw(
        &mut machine,
        Event::ClientRequest {
            request: request(2, 88, "broker/threads/stop", json!({"threadId":"线程"})),
            run_id: None,
        },
    );
    assert!(!requests(&actions)
        .iter()
        .any(|c| c.method.starts_with("thread/goal/")));
    let actions = response(
        &mut machine,
        &write,
        Ok(json!({"goal":{"status":"active"}})),
    );
    assert!(requests(&actions)
        .iter()
        .any(|c| c.method == "thread/goal/get"));
    let (mut machine, write) = active_write();
    let deadline = std::time::Instant::now();
    step_raw(
        &mut machine,
        Event::TimedOut {
            tag: 17,
            deadline,
            context: write.clone(),
            error: json!({"message":"目标写入超时"}),
        },
    );
    step_raw(&mut machine, turn("turn/completed", "新轮"));
    let actions = step_raw(&mut machine, Event::LateDiscarded { context: write });
    assert!(requests(&actions)
        .iter()
        .any(|c| c.params.as_ref().is_some_and(|p| p["status"] == "paused")));
}

#[test]
fn 预检停止必须重查原丢失或停止中运行() {
    for state in [RunState::Lost, RunState::Stopping] {
        let mut machine = fixture(false);
        // 先占座，再模拟原运行状态变化，覆盖 #28d 的停止入口。
        begin(&mut machine);
        let run = machine.runs.runs.get_mut(OLD).unwrap();
        run.state = state;
        run.stop_outcome = Some(StopOutcome::Unconfirmed);
        let actions = step_raw(
            &mut machine,
            Event::ClientRequest {
                request: request(
                    2,
                    88,
                    "broker/threads/stop",
                    json!({"threadId":"线程","clear":true}),
                ),
                run_id: None,
            },
        );
        assert_eq!(reason(&actions).as_deref(), Some("continueCancelled"));
        let reads = requests(&actions);
        assert!(reads.iter().any(|c| c.method == "thread/read"));
        assert!(!sends(&actions)
            .iter()
            .any(|(_, v)| v["id"] == 88 && v.get("result").is_some()));
        let read = reads.iter().find(|c| c.method == "thread/read").unwrap();
        let mut next = response(
            &mut machine,
            read,
            Ok(json!({"thread":{"turns":[{"id":"仍在跑","status":"inProgress"}]}})),
        );
        for goal in reads.iter().filter(|c| c.method == "thread/goal/get") {
            next.extend(response(&mut machine, goal, Ok(json!({"goal":null}))));
        }
        assert!(requests(&next).iter().any(|c| c.method == "turn/interrupt"));
    }
}

#[test]
fn 共享定案先于回放且恰好一次并保留来源() {
    for joined in [false, true] {
        for undecided in [false, true] {
            let (mut machine, stream) = opened(true);
            machine.record("线程").watchers = vec![1, 2, 3];
            machine.cxb_conns.insert(2);
            let mut actions = response(&mut machine, &stream, Ok(json!({"turn":{"id":"新轮"}})));
            let settlement = super::settlement::settlement_request(&actions).context;
            if undecided {
                actions = response(
                    &mut machine,
                    &settlement,
                    Err(json!({"message":"归属暂不明确"})),
                );
                assert!(!sends(&actions)
                    .iter()
                    .any(|(_, v)| v["method"] == "broker/run/settled"));
            }
            if !joined {
                actions = step_raw(&mut machine, turn("turn/started", "新轮"));
                if !undecided {
                    // 本轮启动推进了活动版本：读取返回已变化，回调先查 own_started，按自有定案。
                    actions.extend(response(
                        &mut machine,
                        &settlement,
                        Ok(json!({"thread":{"id":"线程","status":{"type":"active"}}})),
                    ));
                }
            } else {
                if undecided {
                    // 待定后的下一次读取由一秒重试计时发起。
                    let (token, kind) = actions
                        .iter()
                        .find_map(|a| match a {
                            Action::Schedule {
                                token,
                                kind: kind @ TimerKind::SettlementRetry { .. },
                                ..
                            } => Some((token.clone(), kind.clone())),
                            _ => None,
                        })
                        .expect("待定后安排重试");
                    step_raw(&mut machine, Event::Timer { token, kind });
                }
                actions = super::settlement::settle_active(&mut machine, &["别人的轮"]);
            }
            let notices: Vec<_> = sends(&actions)
                .into_iter()
                .filter(|(_, v)| v["method"] == "broker/run/settled")
                .collect();
            assert_eq!(notices.len(), 2, "加入={joined} 待定={undecided}");
            assert_eq!(
                notices[0].1["params"]["absorbedBy"],
                if joined {
                    json!("别人的轮")
                } else {
                    Value::Null
                }
            );
            assert_eq!(machine.runs.runs[NEW].continued_from.as_deref(), Some(OLD));
            assert_eq!(machine.runs.runs[NEW].joined, joined);
            if joined {
                assert!(!machine.claims["线程"].resume_goal);
            }
            if !joined {
                let notice=actions.iter().position(|a|matches!(a,Action::Send{message,..} if message["method"]=="broker/run/settled")).unwrap();
                let replay=actions.iter().position(|a|matches!(a,Action::Send{message,..} if message["method"]=="turn/started")).unwrap();
                assert!(notice < replay);
            }
            let duplicate = step_raw(
                &mut machine,
                Event::ClientRequest {
                    request: request(
                        1,
                        80,
                        "broker/joined",
                        json!({"threadId":"线程","turnId":"别人的轮"}),
                    ),
                    run_id: None,
                },
            );
            assert!(!sends(&duplicate)
                .iter()
                .any(|(_, v)| v["method"] == "broker/run/settled"));
        }
    }
}

#[test]
fn 墓碑分类与目标功能禁用() {
    use crate::runs::RunLookup;
    assert_eq!(
        crate::claim::resume::lookup_error(
            &RunLookup::Retired {
                thread_id: "线程".into()
            },
            "线程"
        ),
        Some("notLatestRun")
    );
    assert_eq!(
        crate::claim::resume::lookup_error(
            &RunLookup::Retired {
                thread_id: "其他".into()
            },
            "线程"
        ),
        Some("runMismatch")
    );
    assert_eq!(
        crate::claim::resume::lookup_error(&RunLookup::Unknown, "线程"),
        Some("unknownRun")
    );
    for unavailable in [
        "goals feature is disabled",
        "method not found",
        "unknown method",
        "sqlite state db unavailable",
    ] {
        let mut machine = fixture(true);
        let batch = read_batch(&mut machine);
        let mut actions = Vec::new();
        for context in batch {
            actions.extend(response(
                &mut machine,
                &context,
                if context.method == "thread/goal/get" {
                    Err(json!({"message":unavailable}))
                } else {
                    Ok(normal_result(&context))
                },
            ));
        }
        assert!(requests(&actions).iter().any(|c| c.method == "turn/start"));
    }
}

#[test]
fn 预检控制请求超时均有失败出口() {
    for method in ["thread/resume", "thread/read", "thread/goal/get"] {
        let mut machine = fixture(method != "thread/resume");
        let initial = begin(&mut machine);
        let mut controls = requests(&initial);
        if method == "thread/resume" {
            let mut actions = Vec::new();
            for context in controls {
                actions.extend(response(
                    &mut machine,
                    &context,
                    Ok(normal_result(&context)),
                ));
            }
            controls = requests(&actions);
        }
        let context = controls
            .iter()
            .find(|c| c.method == method)
            .unwrap()
            .clone();
        let actions = step_raw(
            &mut machine,
            Event::TimedOut {
                tag: 19,
                deadline: std::time::Instant::now(),
                context: context.clone(),
                error: json!({"message":"核实请求超时"}),
            },
        );
        assert_eq!(reason(&actions).as_deref(), Some("resumeCheckFailed"));
        no_run(&machine);
        let late = step_raw(
            &mut machine,
            Event::LateReply {
                context: context.clone(),
                result: Ok(normal_result(&context)),
            },
        );
        assert!(!requests(&late).iter().any(|c| c.method == "turn/start"));
    }
}

#[test]
fn 未知写入按通知补暂停且迟到写入仍受制动() {
    for active_first in [false, true] {
        let (mut machine, write) = active_write();
        step_raw(
            &mut machine,
            Event::TimedOut {
                tag: 25,
                deadline: std::time::Instant::now(),
                context: write.clone(),
                error: json!({"message":"恢复超时"}),
            },
        );
        step_raw(&mut machine, turn("turn/completed", "新轮"));
        let actions = step_raw(
            &mut machine,
            notify(
                "thread/goal/updated",
                json!({"threadId":"线程","goal":{"status":"active"}}),
            ),
        );
        let pause = first_request(&actions);
        assert_eq!(pause.params.as_ref().unwrap()["status"], "paused");
        let mut extra = Vec::new();
        if active_first {
            extra.extend(step_raw(
                &mut machine,
                Event::LateReply {
                    context: write.clone(),
                    result: Ok(json!({"goal":{"status":"active"}})),
                },
            ));
            extra.extend(response(
                &mut machine,
                &pause,
                Ok(json!({"goal":{"status":"paused"}})),
            ));
        } else {
            extra.extend(response(
                &mut machine,
                &pause,
                Ok(json!({"goal":{"status":"paused"}})),
            ));
            extra.extend(step_raw(
                &mut machine,
                Event::LateReply {
                    context: write.clone(),
                    result: Ok(json!({"goal":{"status":"active"}})),
                },
            ));
        }
        let brake = requests(&extra)
            .into_iter()
            .find(|c| c.params.as_ref().is_some_and(|p| p["status"] == "paused"))
            .unwrap();
        response(
            &mut machine,
            &brake,
            Ok(json!({"goal":{"status":"paused"}})),
        );
        assert_eq!(machine.goal_restores, Default::default());
        assert!(machine.stops.changes.is_empty());
        assert!(!machine.records["线程"].goal_active);
    }
}

#[test]
fn 补暂停超时保留到迟到到期且关闭清理各阶段() {
    for phase in 0..4 {
        let (mut machine, write) = active_write();
        let mut context = write;
        if phase >= 1 {
            step_raw(
                &mut machine,
                Event::TimedOut {
                    tag: 27,
                    deadline: std::time::Instant::now(),
                    context: context.clone(),
                    error: json!({"message":"超时"}),
                },
            );
        }
        if phase >= 2 {
            step_raw(&mut machine, turn("turn/completed", "新轮"));
            let actions = step_raw(
                &mut machine,
                Event::LateDiscarded {
                    context: context.clone(),
                },
            );
            context = first_request(&actions);
        }
        if phase == 3 {
            step_raw(
                &mut machine,
                Event::TimedOut {
                    tag: 28,
                    deadline: std::time::Instant::now(),
                    context: context.clone(),
                    error: json!({"message":"补暂停超时"}),
                },
            );
            assert_ne!(machine.goal_restores, Default::default());
            step_raw(
                &mut machine,
                Event::LateDiscarded {
                    context: context.clone(),
                },
            );
            assert_eq!(machine.goal_restores, Default::default());
        }
        step_raw(&mut machine, Event::Shutdown);
        assert_eq!(machine.goal_restores, Default::default());
        assert!(requests(&response(
            &mut machine,
            &context,
            Ok(json!({"goal":{"status":"active"}}))
        ))
        .is_empty());
    }
}

#[test]
fn 共享回放完成不提前恢复目标且定案只发一次() {
    let (mut machine, stream) = opened(true);
    step_raw(&mut machine, turn("turn/started", "新轮"));
    step_raw(&mut machine, turn("turn/completed", "新轮"));
    let actions = response(&mut machine, &stream, Ok(json!({"turn":{"id":"新轮"}})));
    assert_eq!(
        sends(&actions)
            .iter()
            .filter(|(_, v)| v["method"] == "broker/run/settled")
            .count(),
        1
    );
    assert_eq!(
        sends(&actions)
            .iter()
            .filter(|(_, v)| v["method"] == "turn/completed")
            .count(),
        1
    );
    assert!(!machine.claims.contains_key("线程"));
    let get = requests(&actions)
        .into_iter()
        .find(|c| c.method == "thread/goal/get")
        .unwrap();
    let late = response(&mut machine, &get, Ok(json!({"goal":{"status":"paused"}})));
    assert!(!requests(&late)
        .iter()
        .any(|c| c.method == "thread/goal/set"));
    assert_eq!(machine.goal_restores, Default::default());
    assert_eq!(machine.runs.runs[NEW].turns.len(), 1);
}

#[test]
fn 内存失败直接拒绝而不触发服务器副作用() {
    let mut machine = fixture(false);
    machine.runs.runs.remove(OLD);
    let actions = begin(&mut machine);
    assert_eq!(reason(&actions).as_deref(), Some("unknownRun"));
    assert!(requests(&actions).is_empty());
    no_run(&machine);
}

#[test]
fn 预检期间_joined_回_false_且不改变预检() {
    let mut machine = fixture(false);
    let initial = begin(&mut machine);
    let actions = step_raw(
        &mut machine,
        Event::ClientRequest {
            request: request(
                1,
                92,
                "broker/joined",
                json!({"threadId":"线程","turnId":"其他轮"}),
            ),
            run_id: None,
        },
    );
    let joined = actions.iter().find_map(|action| match action {
        Action::Send { message, .. } if message["id"] == json!(92) => {
            Some(message["result"]["joined"].clone())
        }
        _ => None,
    });
    assert_eq!(joined, Some(json!(false)));
    assert!(machine.claims["线程"].precheck());
    assert_ne!(machine.continues, Default::default());
    assert_eq!(machine.stream_slots.len(), 1);
    assert!(!machine.runs.runs.contains_key(NEW));
    let _ = initial;
}

#[test]
fn 活动目标在恢复订阅之前拒绝且迟到核实不能开工() {
    for lost in [false, true] {
        for goal_first in [false, true] {
            let mut machine = fixture(false);
            if lost {
                machine.runs.runs.get_mut(OLD).unwrap().state = RunState::Lost;
            }
            let mut reads = read_batch(&mut machine);
            if goal_first {
                reads.reverse();
            }
            let mut actions = Vec::new();
            for context in reads {
                let result = if context.method == "thread/goal/get" {
                    json!({"goal":{"status":"active"}})
                } else {
                    normal_result(&context)
                };
                actions.extend(response(&mut machine, &context, Ok(result)));
            }
            assert_eq!(reason(&actions).as_deref(), Some("stopping"));
            assert!(
                requests(&actions).is_empty(),
                "活动目标不得触发 resume 或开工"
            );
            assert_eq!(
                sends(&actions)
                    .iter()
                    .filter(|(_, message)| message["id"] == 42)
                    .count(),
                1
            );
            no_run(&machine);
        }
    }
}

#[test]
fn 恢复订阅之后必须复核并拒绝期间变化() {
    for case in 0..4 {
        let mut machine = fixture(false);
        let resume = resume_barrier(&mut machine);
        if case == 3 {
            machine.runs.runs.get_mut(OLD).unwrap().interrupt_reason = None;
        }
        let actions = response(&mut machine, &resume, Ok(normal_result(&resume)));
        let mut all = actions.clone();
        let reads = requests(&actions);
        if case == 3 {
            assert!(reads.is_empty(), "复核内存前提已失败，不再读服务器");
        } else {
            assert_eq!(
                reads
                    .iter()
                    .map(|context| context.method.as_str())
                    .collect::<Vec<_>>(),
                ["thread/read", "thread/goal/get"]
            );
            for context in reads {
                let result = match (case, context.method.as_str()) {
                    (0, "thread/goal/get") => json!({"goal":{"status":"active"}}),
                    (1, "thread/read") => {
                        json!({"thread":{"turns":[{"id":"外部轮","status":"inProgress"}]}})
                    }
                    (2, "thread/read") => json!({"thread":{}}),
                    _ => normal_result(&context),
                };
                all.extend(response(&mut machine, &context, Ok(result)));
            }
        }
        let expected = [
            "stopping",
            "threadBusy",
            "resumeCheckFailed",
            "notResumable",
        ][case];
        assert_eq!(reason(&all).as_deref(), Some(expected));
        assert!(!requests(&all)
            .iter()
            .any(|context| context.method == "turn/start"));
        no_run(&machine);
    }
}

/// 经真实请求、回包和 started 建立后继认领，不直接修改认领代号。
fn ordinary_run(machine: &mut Machine, run: &str, turn_id: &str) {
    let actions = step_raw(
        machine,
        Event::ClientRequest {
            request: request(2, 103, "turn/start", json!({"threadId":"线程","input":[]})),
            run_id: Some(run.into()),
        },
    );
    let mut context = first_request(&actions);
    if context.method == "thread/resume" {
        context = first_request(&response(machine, &context, Ok(normal_result(&context))));
    }
    assert_eq!(context.method, "turn/start");
    response(machine, &context, Ok(json!({"turn":{"id":turn_id}})));
    step_raw(machine, turn("turn/started", turn_id));
    assert_eq!(machine.claims["线程"].run_id.as_deref(), Some(run));
}

#[test]
fn 换代后的目标写入回包按版本更新事实并驱动新认领完成() {
    for timed_out in [false, true] {
        for case in 0..6 {
            let (mut machine, write) = active_write();
            if timed_out {
                step_raw(
                    &mut machine,
                    Event::TimedOut {
                        tag: 301,
                        deadline: std::time::Instant::now(),
                        context: write.clone(),
                        error: json!({"message":"目标恢复写入超时"}),
                    },
                );
            }
            step_raw(&mut machine, turn("turn/completed", "新轮"));
            ordinary_run(&mut machine, "run-后继", "后继轮");
            let token = machine.token("线程").unwrap();
            if (1..=4).contains(&case) {
                let status = if case == 1 { "paused" } else { "active" };
                step_raw(
                    &mut machine,
                    notify(
                        "thread/goal/updated",
                        json!({"threadId":"线程","goal":{"status":status}}),
                    ),
                );
            }
            let result = match case {
                0 | 1 => Ok(json!({"goal":{"status":"active"}})),
                2 | 5 => Ok(json!({"goal":{"status":"paused"}})),
                3 => Ok(json!({"goal":null})),
                _ => Err(json!({"message":"恢复目标失败"})),
            };
            let before_version = machine.stops.versions["线程"];
            let actions = if timed_out {
                step_raw(
                    &mut machine,
                    Event::LateReply {
                        context: write.clone(),
                        result,
                    },
                )
            } else {
                response(&mut machine, &write, result)
            };
            assert!(machine.current(&token));
            assert!(actions
                .iter()
                .any(|action| matches!(action, Action::Warn(_))));
            assert!(!requests(&actions)
                .iter()
                .any(|context| context.method == "thread/goal/set"));
            let active = case != 5;
            assert_eq!(
                machine.records["线程"].goal_active, active,
                "超时={timed_out} 用例={case}"
            );
            if (2..=4).contains(&case) {
                assert_eq!(
                    machine.stops.versions["线程"], before_version,
                    "旧 inactive 或失败不覆盖新事实"
                );
            } else {
                assert!(machine.stops.versions["线程"] > before_version);
            }
            step_raw(&mut machine, turn("turn/completed", "后继轮"));
            if active {
                assert!(machine.current(&token));
                assert!(matches!(
                    machine.claims["线程"].state,
                    State::AwaitingContinuation { .. }
                ));
                assert_eq!(machine.runs.runs["run-后继"].state, RunState::Running);
            } else {
                assert!(!machine.claims.contains_key("线程"));
            }
            assert!(machine.continue_prechecks_valid());
        }
    }
}

fn unsubscribe_timer(actions: &[Action]) -> Option<Event> {
    actions.iter().find_map(|action| match action {
        Action::Schedule {
            token,
            kind: TimerKind::Unsubscribe,
            ..
        } => Some(Event::Timer {
            token: token.clone(),
            kind: TimerKind::Unsubscribe,
        }),
        _ => None,
    })
}

#[test]
fn 迟到恢复订阅在认领换代或记录重建后真正退订() {
    for scenario in 0..4 {
        let mut machine = fixture(false);
        let resume = resume_barrier(&mut machine);
        let old_token = resume.token.clone().unwrap();
        let cancelled = step_raw(
            &mut machine,
            Event::TimedOut {
                tag: 302,
                deadline: std::time::Instant::now(),
                context: resume.clone(),
                error: json!({"message":"恢复订阅超时"}),
            },
        );
        if let Some(timer) = unsubscribe_timer(&cancelled) {
            step_raw(&mut machine, timer);
        }
        ordinary_run(&mut machine, "run-后继", "后继轮");
        if scenario != 2 {
            let ended = step_raw(&mut machine, turn("turn/completed", "后继轮"));
            let timer = unsubscribe_timer(&ended).expect("后继认领释放后安排退订");
            let actions = step_raw(&mut machine, timer);
            let unsubscribe = requests(&actions)
                .into_iter()
                .find(|context| context.method == "thread/unsubscribe")
                .unwrap();
            response(&mut machine, &unsubscribe, Ok(json!({})));
            assert!(!machine.records["线程"].subscribed);
        }
        if scenario == 1 {
            machine.records.remove("线程");
        }
        let actions = step_raw(
            &mut machine,
            Event::LateReply {
                context: resume,
                result: Ok(json!({"thread":{"id":"线程"}})),
            },
        );
        assert!(machine.records["线程"].subscribed);
        assert!(!requests(&actions)
            .iter()
            .any(|context| context.method == "turn/start"));
        if scenario == 2 {
            assert!(
                unsubscribe_timer(&actions).is_none(),
                "新认领存在时不安排退订"
            );
            assert!(machine.claims.contains_key("线程"));
            continue;
        }
        let timer = unsubscribe_timer(&actions).expect("迟到成功仍应收尾服务器订阅");
        let Event::Timer { token, .. } = &timer else {
            unreachable!()
        };
        assert_eq!(token.claim_id, machine.records["线程"].last_claim_id);
        assert_ne!(token.claim_id, old_token.claim_id);
        if scenario == 3 {
            ordinary_run(&mut machine, "run-再后继", "再后继轮");
        }
        let effects = step_raw(&mut machine, timer);
        assert_eq!(
            requests(&effects)
                .iter()
                .filter(|context| context.method == "thread/unsubscribe")
                .count(),
            usize::from(scenario != 3)
        );
        assert_eq!(machine.records["线程"].subscribed, scenario == 3);
    }
}

#[test]
fn 预检登记不变量覆盖读恢复复核和所有取消入口() {
    for phase in 0..3 {
        for exit in 0..5 {
            let mut machine = fixture(false);
            let contexts = if phase == 0 {
                read_batch(&mut machine)
            } else {
                let resume = resume_barrier(&mut machine);
                if phase == 1 {
                    vec![resume]
                } else {
                    requests(&response(&mut machine, &resume, Ok(normal_result(&resume))))
                }
            };
            assert!(machine.continue_prechecks_valid());
            let mut broken = machine.clone();
            broken.claims.get_mut("线程").unwrap().claim_id += 1;
            assert!(!broken.continue_prechecks_valid());
            broken = machine.clone();
            broken.stream_slots.clear();
            assert!(!broken.continue_prechecks_valid());
            broken = machine.clone();
            broken.continues = Default::default();
            assert!(!broken.continue_prechecks_valid());
            let event = match exit {
                0 => Event::Disconnected { conn: 1 },
                1 => Event::Shutdown,
                2 => turn("turn/started", "外部轮"),
                3 => Event::ClientRequest {
                    request: request(2, 105, "broker/threads/stop", json!({"threadId":"线程"})),
                    run_id: None,
                },
                _ => Event::Reply {
                    context: contexts[0].clone(),
                    result: Err(json!({"message":"预检 RPC 失败"})),
                },
            };
            step_raw(&mut machine, event);
            assert!(machine.continue_prechecks_valid());
            assert_eq!(machine.continues, Default::default());
            assert!(machine.stream_slots.is_empty());
            for context in contexts {
                response(&mut machine, &context, Ok(normal_result(&context)));
            }
            assert!(machine.continue_prechecks_valid());
            assert_eq!(machine.continues, Default::default());
            assert!(!machine.runs.runs.contains_key(NEW));
        }
    }
}

#[test]
fn 续跑恢复补传已登记覆盖() {
    let mut machine = fixture(false);
    machine.runs.index.threads.values_mut().next().unwrap().resume_overrides = Some(serde_json::from_value(json!({"config":{"features.default_mode_request_user_input":true},"developerInstructions":"需要用户决定时提问"})).unwrap());
    let initial = begin(&mut machine);
    let (actions, _, _, _) = pump(&mut machine, initial);
    let resume = requests(&actions)
        .into_iter()
        .find(|c| matches!(c.operation, Operation::ContinueResume))
        .unwrap();
    assert_eq!(
        resume.params,
        Some(
            json!({"threadId":"线程","excludeTurns":true,"config":{"features.default_mode_request_user_input":true},"developerInstructions":"需要用户决定时提问"})
        )
    );
}

#[test]
fn 条件续跑未知配置只删命中键且最多重试一次() {
    let mut m = fixture(false);
    m.runs
        .index
        .threads
        .values_mut()
        .next()
        .unwrap()
        .resume_overrides = Some(
        serde_json::from_value(
            json!({"config":{"bad.key":true,"keep.key":7},"developerInstructions":"保持指令"}),
        )
        .unwrap(),
    );
    let context = resume_barrier(&mut m);
    assert_eq!(
        context.params,
        Some(
            json!({"threadId":"线程","excludeTurns":true,"config":{"bad.key":true,"keep.key":7},"developerInstructions":"保持指令"})
        )
    );
    let error = json!({"code":-32600,"message":"failed to load configuration: unknown configuration field `bad.key` in -c/--config override"});
    let actions = response(&mut m, &context, Err(error));
    let retry = requests(&actions)
        .into_iter()
        .find(|r| r.method == "thread/resume")
        .expect("必须重试恢复");
    assert_eq!(
        retry.params,
        Some(
            json!({"threadId":"线程","excludeTurns":true,"config":{"keep.key":7},"developerInstructions":"保持指令"})
        )
    );
    assert!(retry.resume_retried);
    let actions = response(
        &mut m,
        &retry,
        Err(
            json!({"code":-32600,"message":"failed to load configuration: unknown configuration field `keep.key` in -c/--config override"}),
        ),
    );
    assert_eq!(reason(&actions).as_deref(), Some("resumeCheckFailed"));
    assert!(requests(&actions)
        .iter()
        .all(|c| c.method != "thread/resume"));
    assert!(m
        .runs
        .index
        .threads
        .values()
        .next()
        .unwrap()
        .resume_overrides
        .as_ref()
        .unwrap()
        .config
        .as_ref()
        .unwrap()
        .contains_key("keep.key"));
}

#[test]
fn 条件续跑无覆盖仍排除历史() {
    let mut m = fixture(false);
    let context = resume_barrier(&mut m);
    assert_eq!(context.method, "thread/resume");
    assert_eq!(
        context.params,
        Some(json!({"threadId":"线程","excludeTurns":true}))
    );
}
