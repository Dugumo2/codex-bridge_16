//! 协调器的发现按「多久没有回包」判超时：慢但一直有回包能走完，真卡住仍按期失败，总期限仍是上限。
use super::lineage_safety::machine;
use super::*;
use crate::claim::{Operation, RequestContext, TimerKind};
use serde_json::Value;

fn begin_converge(m: &mut Machine) -> (u64, Vec<Action>) {
    let token = m.convergences.runs[R].token.clone();
    let epoch = m.convergences.ledgers[R].work_epoch;
    let mut actions = Vec::new();
    let op = m.begin_discovery(
        P,
        DiscoveryScope::Run { run_id: R.into() },
        DiscoveryCaller::Converge {
            token,
            work_epoch: epoch,
        },
        &mut actions,
    );
    let progress = m.convergences.progress.get_mut(R).unwrap();
    progress.discovery = Some((op, epoch));
    progress.discovery_generation = m.relations.discoveries[&op].generation;
    (op, actions)
}

fn request(actions: &[Action], method: &str, archived: Option<bool>) -> RequestContext {
    actions
        .iter()
        .find_map(|a| match a {
            Action::Request(c)
                if c.method == method
                    && archived.is_none_or(|x| c.params.as_ref().unwrap()["archived"] == x) =>
            {
                Some(c.clone())
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("缺少请求 {method}"))
}

fn empty_page() -> Result<Value, Value> {
    Ok(json!({"data": [], "nextCursor": null}))
}

fn answer(
    m: &mut Machine,
    mock: &mut SubagentMock,
    context: RequestContext,
    result: Result<Value, Value>,
) -> Vec<Action> {
    mock.inject(m, Event::Reply { context, result })
}

fn fire(m: &mut Machine, mock: &mut SubagentMock, op: u64) -> Vec<Action> {
    let generation = m.relations.discoveries[&op].generation;
    mock.inject(
        m,
        Event::Timer {
            token: Machine::relation_token(),
            kind: TimerKind::DiscoveryDeadline {
                op_id: op,
                generation,
            },
        },
    )
}

fn rescheduled(actions: &[Action], op: u64) -> Option<u64> {
    actions.iter().find_map(|a| match a {
        Action::Schedule {
            kind: TimerKind::DiscoveryDeadline { op_id, .. },
            delay_ms,
            ..
        } if *op_id == op => Some(*delay_ms),
        _ => None,
    })
}

fn timed_out(m: &Machine, op: u64) -> bool {
    m.discovery_outcome(op)
        .expect("发现已结束")
        .state
        .incomplete_reasons
        .contains(&"timeout".to_owned())
}

#[test]
fn 一直有回包时超过二十秒也不算超时并能完成() {
    let (mut m, mut mock) = machine();
    let t0 = m.clock.mono_ms;
    let (op, actions) = begin_converge(&mut m);
    assert_eq!(m.relations.discoveries[&op].deadline_ms, t0 + DISCOVERY_MS);

    m.clock.mono_ms = t0 + 15_000;
    answer(
        &mut m,
        &mut mock,
        request(&actions, "thread/list", Some(false)),
        empty_page(),
    );
    // 最初的定时器到点：已收到回包，不结束，只按剩余时间重新定时。
    m.clock.mono_ms = t0 + 20_000;
    let timer = fire(&mut m, &mut mock, op);
    assert!(m.discovery_outcome(op).is_none(), "还有进展，不能判超时");
    assert_eq!(rescheduled(&timer, op), Some(15_000));

    m.clock.mono_ms = t0 + 30_000;
    answer(
        &mut m,
        &mut mock,
        request(&actions, "thread/list", Some(true)),
        empty_page(),
    );
    m.clock.mono_ms = t0 + 35_000;
    let timer = fire(&mut m, &mut mock, op);
    assert_eq!(rescheduled(&timer, op), Some(15_000));

    m.clock.mono_ms = t0 + 45_000;
    let mut queue: VecDeque<_> = answer(
        &mut m,
        &mut mock,
        request(&actions, "thread/loaded/list", None),
        empty_page(),
    )
    .into_iter()
    .collect();
    // 后面的核对请求也慢：每个隔 15 秒回。
    let mut calls = 0;
    while let Some(action) = queue.pop_front() {
        let Action::Request(context) = action else {
            continue;
        };
        let thread = context.params.as_ref().unwrap()["threadId"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let result = match context.method.as_str() {
            "thread/read" => {
                let mut info = mock.thread_info(&thread).expect("线程已登记");
                info["status"] = json!({"type": "idle"});
                info["turns"] = json!([]);
                json!({"thread": info})
            }
            "thread/goal/get" => json!({"goal": null}),
            _ => continue,
        };
        calls += 1;
        assert!(calls < 64, "发现请求必须有界完成");
        m.clock.mono_ms += 15_000;
        queue.extend(answer(&mut m, &mut mock, context, Ok(result)));
    }
    assert!(calls > 0, "应走到核对请求");
    assert!(
        m.clock.mono_ms - t0 > 2 * DISCOVERY_MS,
        "总用时要明显超过单次期限"
    );
    assert!(!timed_out(&m, op), "慢但一直有回包不算超时");
    assert!(
        m.discovery_outcome(op).unwrap().state.complete,
        "走完的发现必须是完整的"
    );
}

#[test]
fn 本地超时和连接失败不算进展() {
    for local in ["timeout", "transport"] {
        let (mut m, mut mock) = machine();
        let t0 = m.clock.mono_ms;
        let (op, actions) = begin_converge(&mut m);
        let context = request(&actions, "thread/list", Some(false));
        let error = json!({"code": -32603, "message": "本地失败"});
        m.clock.mono_ms = t0 + 15_000;
        let event = if local == "timeout" {
            Event::TimedOut {
                tag: 1,
                deadline: std::time::Instant::now(),
                context,
                error,
            }
        } else {
            Event::TransportFailed { context, error }
        };
        mock.inject(&mut m, event);
        assert_eq!(
            m.relations.discoveries[&op].deadline_ms,
            t0 + DISCOVERY_MS,
            "{local} 不能延后期限"
        );
        m.clock.mono_ms = t0 + DISCOVERY_MS;
        let timer = fire(&mut m, &mut mock, op);
        assert_eq!(rescheduled(&timer, op), None);
        assert!(timed_out(&m, op), "{local} 之后上游仍没有回话，按期超时");
    }
}

#[test]
fn 重查换代后旧定时器不影响新期限() {
    let (mut m, mut mock) = machine();
    let t0 = m.clock.mono_ms;
    let (op, actions) = begin_converge(&mut m);
    let old_generation = m.relations.discoveries[&op].generation;
    m.relations.aux.version += 1;
    m.clock.mono_ms = t0 + 5_000;
    let mut queue: VecDeque<Action> = actions.into_iter().collect();
    let mut calls = 0;
    while m.relations.discoveries[&op].generation == old_generation {
        let action = queue.pop_front().expect("应走到重查");
        let Action::Request(context) = action else {
            continue;
        };
        let thread = context
            .params
            .as_ref()
            .and_then(|p| p["threadId"].as_str())
            .unwrap_or_default()
            .to_owned();
        let result = match context.method.as_str() {
            "thread/list" | "thread/loaded/list" => json!({"data": [], "nextCursor": null}),
            "thread/read" => {
                let mut info = mock.thread_info(&thread).expect("线程已登记");
                info["status"] = json!({"type": "idle"});
                info["turns"] = json!([]);
                json!({"thread": info})
            }
            "thread/goal/get" => json!({"goal": null}),
            _ => continue,
        };
        calls += 1;
        assert!(calls < 64, "发现请求必须有界完成");
        queue.extend(answer(&mut m, &mut mock, context, Ok(result)));
    }
    let deadline = m.relations.discoveries[&op].deadline_ms;
    assert_eq!(deadline, t0 + 5_000 + DISCOVERY_MS, "重查不重置期限");

    // 旧代际的定时器到点：不结束，也不重新定时。
    m.clock.mono_ms = t0 + DISCOVERY_MS;
    let stale = mock.inject(
        &mut m,
        Event::Timer {
            token: Machine::relation_token(),
            kind: TimerKind::DiscoveryDeadline {
                op_id: op,
                generation: old_generation,
            },
        },
    );
    assert_eq!(rescheduled(&stale, op), None);
    assert!(m.discovery_outcome(op).is_none());
    assert_eq!(m.relations.discoveries[&op].deadline_ms, deadline);

    // 新一代的回包继续算进展；之后不再回话，按新期限超时。
    let requests: Vec<_> = queue
        .into_iter()
        .filter_map(|a| match a {
            Action::Request(c) if c.method == "thread/list" => Some(c),
            _ => None,
        })
        .collect();
    assert!(!requests.is_empty(), "重查应重新遍历来源");
    m.clock.mono_ms = t0 + 21_000;
    answer(&mut m, &mut mock, requests[0].clone(), empty_page());
    assert_eq!(
        m.relations.discoveries[&op].deadline_ms,
        t0 + 21_000 + DISCOVERY_MS
    );
    m.clock.mono_ms = t0 + 21_000 + DISCOVERY_MS;
    fire(&mut m, &mut mock, op);
    assert!(timed_out(&m, op));
}

#[test]
fn 旧连接的回包不算进展() {
    let (mut m, mut mock) = machine();
    let t0 = m.clock.mono_ms;
    let (op, actions) = begin_converge(&mut m);
    m.relations.conn_epoch += 1;
    m.clock.mono_ms = t0 + 15_000;
    answer(
        &mut m,
        &mut mock,
        request(&actions, "thread/list", Some(false)),
        empty_page(),
    );
    assert_eq!(m.relations.discoveries[&op].deadline_ms, t0 + DISCOVERY_MS);
}

#[test]
fn 续期后关闭时静默取消() {
    let (mut m, mut mock) = machine();
    let t0 = m.clock.mono_ms;
    let (op, actions) = begin_converge(&mut m);
    let generation = m.relations.discoveries[&op].generation;
    m.clock.mono_ms = t0 + 15_000;
    answer(
        &mut m,
        &mut mock,
        request(&actions, "thread/list", Some(false)),
        empty_page(),
    );
    mock.inject(&mut m, Event::Shutdown);
    assert!(m.discovery_outcome(op).is_none(), "关闭不产生结果");
    assert!(m.relations.aux.work.is_empty());
    // 已登记的定时器到点也不会把它救活。
    m.clock.mono_ms = t0 + 20_000;
    let late = mock.inject(
        &mut m,
        Event::Timer {
            token: Machine::relation_token(),
            kind: TimerKind::DiscoveryDeadline {
                op_id: op,
                generation,
            },
        },
    );
    assert_eq!(rescheduled(&late, op), None);
    assert!(m.discovery_outcome(op).is_none());
}

#[test]
fn 发现在途时总期限到达如实异常收尾() {
    let (mut m, mut mock) = machine();
    let t0 = m.clock.mono_ms;
    m.convergences.runs.get_mut(R).unwrap().deadline_mono_ms = t0 + 25_000;
    let (_op, actions) = begin_converge(&mut m);
    m.clock.mono_ms = t0 + 15_000;
    answer(
        &mut m,
        &mut mock,
        request(&actions, "thread/list", Some(false)),
        empty_page(),
    );
    m.clock.mono_ms = t0 + 25_000;
    let token = m.convergences.runs[R].token.clone();
    mock.inject(
        &mut m,
        Event::Timer {
            token: token.claim.clone(),
            kind: TimerKind::ConvergeDeadline { token },
        },
    );
    let join = m.runs.runs[R].subagent_join.as_ref().unwrap();
    assert_eq!(
        join.terminal_reason,
        Some(crate::sa1b::TerminalReason::DiscoveryIncomplete),
        "发现没走完就到总期限，如实报发现不完整"
    );
}

#[test]
fn 完全没有回包时二十秒判超时() {
    let (mut m, mut mock) = machine();
    let t0 = m.clock.mono_ms;
    let (op, _) = begin_converge(&mut m);
    m.clock.mono_ms = t0 + DISCOVERY_MS;
    fire(&mut m, &mut mock, op);
    assert!(timed_out(&m, op));
}

#[test]
fn 中途停止回包时从最后一个回包算起二十秒判超时() {
    let (mut m, mut mock) = machine();
    let t0 = m.clock.mono_ms;
    let (op, actions) = begin_converge(&mut m);
    m.clock.mono_ms = t0 + 15_000;
    answer(
        &mut m,
        &mut mock,
        request(&actions, "thread/list", Some(false)),
        empty_page(),
    );
    m.clock.mono_ms = t0 + 20_000;
    fire(&mut m, &mut mock, op);
    assert!(m.discovery_outcome(op).is_none());
    m.clock.mono_ms = t0 + 15_000 + DISCOVERY_MS;
    fire(&mut m, &mut mock, op);
    assert!(timed_out(&m, op));
}

#[test]
fn 旧代际的迟到回包不算进展() {
    let (mut m, mut mock) = machine();
    let t0 = m.clock.mono_ms;
    let (op, actions) = begin_converge(&mut m);
    let generation = m.relations.discoveries[&op].generation;
    let mut late = request(&actions, "thread/list", Some(false));
    late.operation = Operation::Discovery {
        op_id: op,
        generation: generation + 1000,
    };
    m.clock.mono_ms = t0 + 15_000;
    answer(&mut m, &mut mock, late, empty_page());
    assert_eq!(m.relations.discoveries[&op].deadline_ms, t0 + DISCOVERY_MS);
    m.clock.mono_ms = t0 + DISCOVERY_MS;
    fire(&mut m, &mut mock, op);
    assert!(timed_out(&m, op), "旧回包不能让卡住的发现继续等");
}

#[test]
fn 期限不超过总期限() {
    let (mut m, mut mock) = machine();
    let t0 = m.clock.mono_ms;
    m.convergences.runs.get_mut(R).unwrap().deadline_mono_ms = t0 + 25_000;
    let (op, actions) = begin_converge(&mut m);
    m.clock.mono_ms = t0 + 15_000;
    answer(
        &mut m,
        &mut mock,
        request(&actions, "thread/list", Some(false)),
        empty_page(),
    );
    assert_eq!(
        m.relations.discoveries[&op].deadline_ms,
        t0 + 25_000,
        "回包只能推到总期限"
    );
    m.clock.mono_ms = t0 + 25_000;
    fire(&mut m, &mut mock, op);
    assert!(timed_out(&m, op));

    // 开始时总期限已不足一个间隔：定时也不超过它。
    let (mut m, mut mock) = machine();
    let t0 = m.clock.mono_ms;
    m.convergences.runs.get_mut(R).unwrap().deadline_mono_ms = t0 + 5_000;
    let (op, _) = begin_converge(&mut m);
    assert_eq!(m.relations.discoveries[&op].deadline_ms, t0 + 5_000);
    m.clock.mono_ms = t0 + 5_000;
    fire(&mut m, &mut mock, op);
    assert!(timed_out(&m, op));
}

#[test]
fn 其他调用方仍是固定期限() {
    let (mut m, mut mock) = setup(false);
    let t0 = m.clock.mono_ms;
    let mut actions = Vec::new();
    let op = m.begin_discovery(
        P,
        DiscoveryScope::Run { run_id: R.into() },
        DiscoveryCaller::TreeStop {
            tree_op: 1,
            round: 1,
        },
        &mut actions,
    );
    assert_eq!(m.relations.discoveries[&op].idle_timeout_ms, None);
    m.clock.mono_ms = t0 + 15_000;
    answer(
        &mut m,
        &mut mock,
        request(&actions, "thread/list", Some(false)),
        empty_page(),
    );
    assert_eq!(m.relations.discoveries[&op].deadline_ms, t0 + DISCOVERY_MS);
    m.clock.mono_ms = t0 + DISCOVERY_MS;
    let timer = fire(&mut m, &mut mock, op);
    assert_eq!(rescheduled(&timer, op), None);
    assert!(timed_out(&m, op), "收到过回包也按原期限结束");
}

#[test]
fn 错误回包算进展且错误原因照样记录() {
    let (mut m, mut mock) = machine();
    let t0 = m.clock.mono_ms;
    let (op, actions) = begin_converge(&mut m);
    m.clock.mono_ms = t0 + 15_000;
    answer(
        &mut m,
        &mut mock,
        request(&actions, "thread/list", Some(false)),
        Err(json!({"code": -32000, "message": "读取失败"})),
    );
    assert_eq!(
        m.relations.discoveries[&op].deadline_ms,
        t0 + 15_000 + DISCOVERY_MS
    );
    m.clock.mono_ms = t0 + 15_000 + DISCOVERY_MS;
    fire(&mut m, &mut mock, op);
    let reasons = &m.discovery_outcome(op).unwrap().state.incomplete_reasons;
    assert!(reasons.contains(&"pageError:unarchived".to_owned()));
    assert!(reasons.contains(&"timeout".to_owned()));
}
