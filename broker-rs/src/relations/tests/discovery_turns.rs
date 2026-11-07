//! 发现核对子代理时不读全部历史：先读线程状态，再分页读轮次的编号和状态。
//! 历史超过 broker 的 10 MiB 单条上限时，全量读会让 broker 退出，所以这里不能退回 includeTurns。
use super::lineage_safety::machine;
use super::*;
use crate::claim::RequestContext;
use serde_json::Value;

fn begin(m: &mut Machine) -> (u64, VecDeque<Action>) {
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
    (op, actions.into_iter().collect())
}

fn add_child(m: &mut Machine, mock: &mut SubagentMock) {
    mock.register(C, Some(P), "v1", false, true);
    let started = mock.thread_started(C);
    mock.inject(m, started);
}

fn params(context: &RequestContext) -> &Value {
    context.params.as_ref().unwrap()
}

/// 代答发现请求，`turns` 决定子代理的轮次分页。返回发出过的子代理请求。
fn run_with(
    m: &mut Machine,
    mock: &mut SubagentMock,
    op: u64,
    mut queue: VecDeque<Action>,
    status: &str,
    mut turns: impl FnMut(&RequestContext) -> Result<Value, Value>,
) -> Vec<RequestContext> {
    let mut seen = Vec::new();
    let mut calls = 0;
    while let Some(action) = queue.pop_front() {
        if m.discovery_outcome(op).is_some() {
            break;
        }
        let Action::Request(context) = action else {
            continue;
        };
        if !matches!(context.operation, crate::claim::Operation::Discovery { .. }) {
            continue;
        }
        calls += 1;
        assert!(calls < 2048, "发现请求必须有界完成");
        let thread = params(&context)["threadId"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        if thread == C {
            seen.push(context.clone());
        }
        let result = match context.method.as_str() {
            "thread/list" => {
                let data = if params(&context)["archived"] == true {
                    json!([])
                } else {
                    json!([mock.thread_info(C).unwrap()])
                };
                Ok(json!({"data": data, "nextCursor": null}))
            }
            "thread/loaded/list" => Ok(json!({"data": [], "nextCursor": null})),
            "thread/goal/get" => Ok(json!({"goal": null})),
            "thread/read" => {
                let mut info = mock.thread_info(&thread).unwrap();
                info["status"] = json!({"type": status});
                Ok(json!({"thread": info}))
            }
            "thread/turns/list" => turns(&context),
            other => panic!("未预期的发现请求 {other}"),
        };
        queue.extend(mock.inject(m, Event::Reply { context, result }));
    }
    seen
}

fn page(turns: Value, next: Option<&str>) -> Result<Value, Value> {
    Ok(json!({"data": turns, "nextCursor": next}))
}

#[test]
fn 核对子代理不读全部历史且只要轮次状态() {
    let (mut m, mut mock) = machine();
    add_child(&mut m, &mut mock);
    let (op, queue) = begin(&mut m);
    let seen = run_with(&mut m, &mut mock, op, queue, "idle", |_| {
        page(json!([{"id": "子轮", "status": "completed"}]), None)
    });
    let read = seen.iter().find(|c| c.method == "thread/read").unwrap();
    assert_eq!(params(read)["includeTurns"], false, "不能读全部历史");
    let list = seen
        .iter()
        .find(|c| c.method == "thread/turns/list")
        .expect("轮次要分页读");
    assert_eq!(params(list)["itemsView"], "notLoaded", "不要轮次内容");
    assert_eq!(params(list)["sortDirection"], "asc");
    let state = &m.discovery_outcome(op).expect("发现已结束").state;
    assert!(state.complete, "{:?}", state.incomplete_reasons);
}

/// 两页轮次走完后，这个子代理是否被判为静止。
fn inactive_after_pages(first: &str, second: &str) -> bool {
    let (mut m, mut mock) = machine();
    add_child(&mut m, &mut mock);
    let (op, queue) = begin(&mut m);
    let mut cursors = Vec::new();
    let seen = run_with(&mut m, &mut mock, op, queue, "idle", |context| {
        let cursor = params(context)["cursor"].as_str().map(str::to_owned);
        cursors.push(cursor.clone());
        match cursor.as_deref() {
            None => page(json!([{"id": "轮1", "status": first}]), Some("第二页")),
            Some("第二页") => page(json!([{"id": "轮2", "status": second}]), None),
            other => panic!("未预期的游标 {other:?}"),
        }
    });
    assert_eq!(cursors, vec![None, Some("第二页".to_owned())]);
    assert_eq!(
        seen.iter()
            .filter(|c| c.method == "thread/turns/list")
            .count(),
        2
    );
    assert!(m.discovery_outcome(op).is_some());
    m.relations
        .aux
        .candidate_observations
        .get(C)
        .expect("读完轮次后应有观察记录")
        .inactive
}

#[test]
fn 多页轮次要读完_任何一页有在跑的轮次都不算静止() {
    assert!(
        inactive_after_pages("completed", "failed"),
        "都结束了才算静止"
    );
    assert!(
        !inactive_after_pages("inProgress", "completed"),
        "在跑的轮次在第一页"
    );
    assert!(
        !inactive_after_pages("completed", "inProgress"),
        "在跑的轮次在最后一页"
    );
}

#[test]
fn 轮次读取失败时这个子代理未核实() {
    let (mut m, mut mock) = machine();
    add_child(&mut m, &mut mock);
    let (op, queue) = begin(&mut m);
    run_with(&mut m, &mut mock, op, queue, "idle", |_| {
        Err(json!({"code": -32000, "message": "读取失败"}))
    });
    let state = &m.discovery_outcome(op).unwrap().state;
    assert!(!state.complete);
    assert!(state
        .incomplete_reasons
        .contains(&"activityUnknown".to_owned()));
}

#[test]
fn 回包不是列表时这个子代理未核实() {
    let (mut m, mut mock) = machine();
    add_child(&mut m, &mut mock);
    let (op, queue) = begin(&mut m);
    run_with(&mut m, &mut mock, op, queue, "idle", |_| {
        Ok(json!({"data": null}))
    });
    let state = &m.discovery_outcome(op).unwrap().state;
    assert!(!state.complete);
    assert!(state
        .incomplete_reasons
        .contains(&"activityUnknown".to_owned()));
}

#[test]
fn 重复游标时停止翻页并判未核实() {
    let (mut m, mut mock) = machine();
    add_child(&mut m, &mut mock);
    let (op, queue) = begin(&mut m);
    let seen = run_with(&mut m, &mut mock, op, queue, "idle", |_| {
        page(json!([{"id": "轮", "status": "completed"}]), Some("同一页"))
    });
    assert_eq!(
        seen.iter()
            .filter(|c| c.method == "thread/turns/list")
            .count(),
        2,
        "看到重复游标就停"
    );
    let state = &m.discovery_outcome(op).unwrap().state;
    assert!(!state.complete);
    assert!(state.incomplete_reasons.contains(&format!("turnPages:{C}")));
}

#[test]
fn 翻页有上限() {
    let (mut m, mut mock) = machine();
    add_child(&mut m, &mut mock);
    let (op, queue) = begin(&mut m);
    let mut next = 0;
    let seen = run_with(&mut m, &mut mock, op, queue, "idle", |_| {
        next += 1;
        page(
            json!([{"id": format!("轮{next}"), "status": "completed"}]),
            Some(format!("页{next}").as_str()),
        )
    });
    assert_eq!(
        seen.iter()
            .filter(|c| c.method == "thread/turns/list")
            .count(),
        crate::claim::status_read::STATUS_MAX_PAGES
    );
    let state = &m.discovery_outcome(op).unwrap().state;
    assert!(!state.complete);
    assert!(state.incomplete_reasons.contains(&format!("turnPages:{C}")));
}

#[test]
fn 翻页途中候选变化时按候选变化处理() {
    let (mut m, mut mock) = machine();
    add_child(&mut m, &mut mock);
    let (op, mut queue) = begin(&mut m);
    let mut list = None;
    while let Some(action) = queue.pop_front() {
        let Action::Request(context) = action else {
            continue;
        };
        if !matches!(context.operation, crate::claim::Operation::Discovery { .. }) {
            continue;
        }
        let result = match context.method.as_str() {
            "thread/list" => {
                let data = if params(&context)["archived"] == true {
                    json!([])
                } else {
                    json!([mock.thread_info(C).unwrap()])
                };
                json!({"data": data, "nextCursor": null})
            }
            "thread/loaded/list" => json!({"data": [], "nextCursor": null}),
            "thread/goal/get" => json!({"goal": null}),
            "thread/read" => {
                let thread = params(&context)["threadId"].as_str().unwrap().to_owned();
                let mut info = mock.thread_info(&thread).unwrap();
                info["status"] = json!({"type": "idle"});
                json!({"thread": info})
            }
            "thread/turns/list" => {
                list = Some(context);
                continue;
            }
            other => panic!("未预期的发现请求 {other}"),
        };
        queue.extend(mock.inject(
            &mut m,
            Event::Reply {
                context,
                result: Ok(result),
            },
        ));
    }
    let list = list.expect("应走到轮次分页");
    m.candidate_changed(C);
    let next = mock.inject(
        &mut m,
        Event::Reply {
            context: list,
            result: page(
                json!([{"id": "轮1", "status": "completed"}]),
                Some("第二页"),
            ),
        },
    );
    assert!(
        !next
            .iter()
            .any(|a| matches!(a, Action::Request(c) if c.method == "thread/turns/list")),
        "候选已变，不再翻下一页"
    );
    let reasons = &m.relations.aux.work.get(&op).map(|w| w.reasons.clone());
    let outcome = m
        .discovery_outcome(op)
        .map(|r| r.state.incomplete_reasons.clone());
    assert!(
        reasons
            .as_ref()
            .is_some_and(|r| r.contains("candidateChanged"))
            || outcome.is_some_and(|r| r.contains(&"candidateChanged".to_owned())),
        "要记成候选变化"
    );
}
