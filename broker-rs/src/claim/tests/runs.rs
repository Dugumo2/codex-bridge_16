//! M5 接线：核对钩子顺序、真实运行记录与跨运行隔离。
use super::*;
use crate::runs::{OutputSource, Ready, RunState, RunStatus, TurnStatus};

fn count(m: &Machine, hook: &str) -> usize {
    m.runs
        .hook_log
        .iter()
        .filter(|name| name.as_str() == hook)
        .count()
}

const RUN: &str = "run-测试-000001";

fn message_item(id: &str, text: &str) -> Value {
    json!({"id":id,"type":"agentMessage","phase":"final_answer","text":text})
}

fn full_turn(id: &str, status: &str, items: Vec<Value>) -> Value {
    json!({"id":id,"status":status,"items":items,"error":null})
}

fn complete(thread: &str, id: &str, status: &str, items: Vec<Value>) -> Event {
    notify(
        "turn/completed",
        json!({"threadId":thread,"turn":full_turn(id, status, items)}),
    )
}

fn result_read(actions: &[Action]) -> RequestContext {
    actions
        .iter()
        .find_map(|action| match action {
            Action::Request(context) if matches!(context.operation, Operation::ResultRead(_)) => {
                Some(context.clone())
            }
            _ => None,
        })
        .expect("应生成结果补读")
}

#[test]
fn 建立启动条目完成释放与迟到完成分别交付() {
    let (mut m, context) = start(false, "turn/start");
    assert_eq!(m.runs.hook_log, ["run_opened"]);
    reply(
        &mut m,
        &context,
        Ok(json!({"turn":full_turn("本轮", "inProgress", vec![])})),
    );
    assert_eq!(count(&m, "run_turn_started"), 1);
    for method in ["item/started", "item/completed"] {
        apply(
            &mut m,
            notify(
                method,
                json!({"threadId":"线程","turnId":"本轮","item":message_item("条目", "正式输出")}),
            ),
        );
    }
    assert_eq!(count(&m, "run_item"), 2);
    apply(
        &mut m,
        complete(
            "线程",
            "本轮",
            "completed",
            vec![message_item("条目", "正式输出")],
        ),
    );
    assert_eq!(
        &m.runs.hook_log[m.runs.hook_log.len() - 2..],
        ["run_turn_completed", "run_released"]
    );
    let record = m.runs.runs[RUN].clone();
    assert_eq!(record.run_id, RUN);
    assert_eq!(record.thread_id, "线程");
    assert_eq!(record.state, RunState::Ended);
    assert_eq!(record.status, Some(RunStatus::Completed));
    assert_eq!(record.turns.len(), 1);
    assert_eq!(record.turns[0].turn_id, "本轮");
    assert_eq!(record.turns[0].status, TurnStatus::Completed);
    assert_eq!(record.turns[0].ready, Ready::Ready);
    assert_eq!(record.turns[0].output, "正式输出");
    assert_eq!(record.turns[0].output_source, OutputSource::FinalAnswer);
    assert_eq!(record.progress.done_total, 1);
    assert_eq!(record.progress.done[0].item_id, "条目");
    apply(
        &mut m,
        complete(
            "线程",
            "本轮",
            "completed",
            vec![message_item("条目", "迟到输出")],
        ),
    );
    assert_eq!(m.runs.runs[RUN], record);
    assert_eq!(count(&m, "run_turn_completed"), 2);
    assert_eq!(count(&m, "run_released"), 1);
}

#[test]
fn 不带运行编号不创建运行或结算运行() {
    let mut m = Machine::default();
    m.record("线程").subscribed = true;
    let actions = apply(
        &mut m,
        Event::ClientRequest {
            request: request(1, 1, "turn/start", json!({"threadId":"线程"})),
            run_id: None,
        },
    );
    reply(
        &mut m,
        &first_request(&actions),
        Ok(json!({"turn":full_turn("本轮", "inProgress", vec![])})),
    );
    apply(&mut m, complete("线程", "本轮", "completed", vec![]));
    assert_eq!(count(&m, "run_opened"), 0);
    assert_eq!(count(&m, "run_turn_started"), 0);
    assert_eq!(count(&m, "run_released"), 0);
    assert_eq!(count(&m, "run_turn_completed"), 0);
    assert!(m.runs.runs.is_empty());
}

#[test]
fn 共享定案先于缓冲条目和完成回放() {
    let (mut m, context) = start(true, "turn/start");
    apply(
        &mut m,
        notify(
            "turn/started",
            json!({"threadId":"线程","turn":full_turn("本轮", "inProgress", vec![])}),
        ),
    );
    apply(
        &mut m,
        notify(
            "item/completed",
            json!({"threadId":"线程","turnId":"本轮","item":message_item("条目", "共享输出")}),
        ),
    );
    apply(
        &mut m,
        complete(
            "线程",
            "本轮",
            "completed",
            vec![message_item("条目", "共享输出")],
        ),
    );
    assert_eq!(count(&m, "run_item"), 0);
    reply(
        &mut m,
        &context,
        Ok(json!({"turn":full_turn("本轮", "inProgress", vec![])})),
    );
    let position = |hook| m.runs.hook_log.iter().position(|s| s == hook).unwrap();
    assert!(position("run_settled") < position("run_item"));
    assert!(position("run_settled") < position("run_turn_completed"));
    assert_eq!(count(&m, "run_settled"), 1);
    let record = &m.runs.runs[RUN];
    assert_eq!(record.run_id, RUN);
    assert_eq!(record.status, Some(RunStatus::Completed));
    assert_eq!(record.turns[0].ready, Ready::Ready);
    assert_eq!(record.turns[0].output, "共享输出");
}

#[test]
fn 客户端加入与启动失败各记一次() {
    let (mut m, context) = start(false, "turn/start");
    reply(
        &mut m,
        &context,
        Err(json!({"code":-32000,"message":"启动失败"})),
    );
    assert_eq!(
        &m.runs.hook_log[m.runs.hook_log.len() - 2..],
        ["run_start_failed", "run_released"]
    );
    let failed = &m.runs.runs[RUN];
    assert_eq!(failed.run_id, RUN);
    assert_eq!(failed.status, Some(RunStatus::Failed));
    assert_eq!(failed.turns[0].status, TurnStatus::Failed);
    assert_eq!(failed.turns[0].ready, Ready::Ready);
    assert!(failed.turns[0].output.is_empty());
    assert_eq!(
        failed.turns[0].error,
        Some(json!({"code":-32000,"message":"启动失败"}))
    );
    let (mut m, context) = start(false, "turn/start");
    reply(
        &mut m,
        &context,
        Ok(json!({"turn":full_turn("本轮", "inProgress", vec![])})),
    );
    apply(
        &mut m,
        client(request(
            1,
            2,
            "broker/joined",
            json!({"threadId":"线程","turnId":"本轮"}),
        )),
    );
    assert_eq!(count(&m, "run_settled"), 1);
    assert!(m.runs.runs[RUN].joined);
}

#[test]
fn 审查子线程每条缓冲条目和完成只交付一次() {
    let (mut m, context) = start_legacy_review(false);
    for method in [
        "turn/started",
        "item/started",
        "item/completed",
        "turn/completed",
    ] {
        apply(
            &mut m,
            notify(
                method,
                json!({"threadId":"子线程","turnId":"子轮",
                    "turn":full_turn("子轮", if method == "turn/completed" { "completed" } else { "inProgress" }, vec![]),
                    "item":message_item("条目", "审查快照")}),
            ),
        );
    }
    assert_eq!(count(&m, "run_item"), 0);
    let actions = reply(
        &mut m,
        &context,
        Ok(json!({"reviewThreadId":"子线程","turn":full_turn("子轮", "inProgress", vec![])})),
    );
    assert_eq!(count(&m, "run_review_moved"), 1);
    assert_eq!(count(&m, "run_item"), 2);
    assert_eq!(count(&m, "run_turn_completed"), 1);
    assert_eq!(count(&m, "run_released"), 1);
    let read = result_read(&actions);
    assert_eq!(read.method, "thread/items/list");
    let Operation::ResultRead(identity) = &read.operation else {
        unreachable!()
    };
    assert_eq!(identity.run_id, RUN);
    assert_eq!(identity.thread_id, "子线程");
    assert_eq!(identity.turn_id, "子轮");
    assert_eq!(
        read.params.as_ref().unwrap(),
        &json!({"threadId":"子线程","turnId":"子轮","cursor":null,"limit":64,"sortDirection":"asc"})
    );
    let record = &m.runs.runs[RUN];
    assert_eq!(record.run_id, RUN);
    assert_eq!(record.thread_id, "线程");
    assert_eq!(record.review_thread_id.as_deref(), Some("子线程"));
    assert_eq!(record.status, Some(RunStatus::Completed));
    assert_eq!(record.turns[0].status, TurnStatus::Completed);
    assert_eq!(record.turns[0].ready, Ready::Pending);
    assert!(record.turns[0].output.is_empty());
    let review_item = json!({"id":"审查结果","type":"exitedReviewMode","review":"正式审查结果"});
    reply(
        &mut m,
        &read,
        Ok(
            json!({"data":[{"turnId":"子轮","item":review_item,"startedAtMs":null,"completedAtMs":null}],"nextCursor":null,"backwardsCursor":null}),
        ),
    );
    let record = &m.runs.runs[RUN];
    assert_eq!(record.turns[0].ready, Ready::Ready);
    assert_eq!(record.turns[0].status, TurnStatus::Completed);
    assert_eq!(record.turns[0].output, "正式审查结果");
    assert_eq!(record.turns[0].output_source, OutputSource::Review);
    assert_eq!(record.progress.done_total, 1);
}

#[test]
fn 线程流量学习删除和目标状态均接入() {
    let mut m = Machine::default();
    for method in ["thread/start", "thread/resume", "thread/fork"] {
        let a = apply(
            &mut m,
            client(request(1, 1, method, json!({"threadId":"线程"}))),
        );
        reply(
            &mut m,
            &first_request(&a),
            Ok(json!({"thread":{"id":"线程"}})),
        );
    }
    assert_eq!(count(&m, "run_learn_thread"), 3);
    for method in ["thread/archive", "thread/delete"] {
        let a = apply(
            &mut m,
            client(request(1, 2, method, json!({"threadId":"线程"}))),
        );
        let mut a = a;
        for _ in 0..2 {
            for action in &a {
                if let Action::Store(crate::store::StoreOp::Lifecycle(work)) = action {
                    work.ticket.finish(match work.kind {
                        crate::store::lifecycle::WorkKind::Scan { .. } => {
                            crate::store::lifecycle::ResultValue::Scanned(vec![])
                        }
                        _ => crate::store::lifecycle::ResultValue::Written(1),
                    });
                }
            }
            a = apply(
                &mut m,
                Event::Timer {
                    token: ClaimToken {
                        thread_id: String::new(),
                        claim_id: 0,
                    },
                    kind: TimerKind::Lifecycle,
                },
            );
        }
        let a = reply(&mut m, &first_request(&a), Ok(json!({})));
        for action in &a {
            if let Action::Store(crate::store::StoreOp::Lifecycle(work)) = action {
                work.ticket
                    .finish(crate::store::lifecycle::ResultValue::Written(2));
            }
        }
        apply(
            &mut m,
            Event::Timer {
                token: ClaimToken {
                    thread_id: String::new(),
                    claim_id: 0,
                },
                kind: TimerKind::Lifecycle,
            },
        );
        m.threads_written(m.runs.index.version, None);
        let a = apply(
            &mut m,
            Event::Timer {
                token: ClaimToken {
                    thread_id: String::new(),
                    claim_id: 0,
                },
                kind: TimerKind::Lifecycle,
            },
        );
        for action in &a {
            if let Action::Store(crate::store::StoreOp::Lifecycle(work)) = action {
                work.ticket
                    .finish(crate::store::lifecycle::ResultValue::Written(3));
            }
        }
        apply(
            &mut m,
            Event::Timer {
                token: ClaimToken {
                    thread_id: String::new(),
                    claim_id: 0,
                },
                kind: TimerKind::Lifecycle,
            },
        );
    }
    apply(&mut m, notify("thread/deleted", json!({"threadId":"线程"})));
    assert_eq!(count(&m, "run_forget_thread"), 3);
    apply(
        &mut m,
        notify(
            "thread/goal/updated",
            json!({"threadId":"线程","goal":{"status":"blocked"}}),
        ),
    );
    apply(
        &mut m,
        notify("thread/goal/cleared", json!({"threadId":"线程"})),
    );
    assert_eq!(count(&m, "run_goal_status"), 2);
}

#[test]
fn 关闭先标丢失再释放且迟到流量不写账本() {
    let (mut m, context) = start(false, "turn/start");
    apply(&mut m, Event::Shutdown);
    assert_eq!(
        &m.runs.hook_log[m.runs.hook_log.len() - 2..],
        ["shutdown_runs", "run_released"]
    );
    let before = m.runs.hook_log.clone();
    apply(&mut m, complete("线程", "本轮", "completed", vec![]));
    reply(
        &mut m,
        &context,
        Ok(json!({"turn":full_turn("本轮", "inProgress", vec![])})),
    );
    apply(&mut m, Event::Shutdown);
    assert_eq!(m.runs.hook_log, before);
    apply(&mut m, Event::RunsFinalize);
    apply(&mut m, Event::RunsFinalize);
    assert_eq!(count(&m, "finalize_runs"), 1);
}

#[test]
fn 旧运行淘汰后迟到完成与条目不污染新运行() {
    for ended in [false, true] {
        for known in [false, true] {
            let (mut m, context) = start(false, "turn/start");
            reply(
                &mut m,
                &context,
                Ok(json!({"turn":full_turn("旧轮", "inProgress", vec![])})),
            );
            if ended {
                apply(
                    &mut m,
                    complete(
                        "线程",
                        "旧轮",
                        "completed",
                        vec![message_item("旧条目", "旧结果")],
                    ),
                );
                assert!(m.records["线程"].ended_turns.iter().any(|id| id == "旧轮"));
            } else {
                // joined 的断开只释放认领，不制造 completed；覆盖只有 released_turns 的旧轮。
                apply(
                    &mut m,
                    client(request(
                        1,
                        2,
                        "broker/joined",
                        json!({"threadId":"线程","turnId":"旧轮"}),
                    )),
                );
                apply(&mut m, Event::Disconnected { conn: 1 });
                assert!(m.records["线程"].ended_turns.is_empty());
            }
            assert!(m.records["线程"]
                .released_turns
                .iter()
                .any(|id| id == "旧轮"));
            m.clock.now_ms = 2;
            m.record("线程").subscribed = true;
            let actions = apply(
                &mut m,
                Event::ClientRequest {
                    request: request(2, 3, "turn/start", json!({"threadId":"线程"})),
                    run_id: Some("run-new".into()),
                },
            );
            let start = first_request(&actions);
            assert!(
                !m.runs.runs.contains_key(RUN),
                "旧运行必须真正被淘汰，不能只移除认领"
            );
            if known {
                reply(
                    &mut m,
                    &start,
                    Ok(json!({"turn":full_turn("新轮", "inProgress", vec![])})),
                );
            }
            let before = m.runs.runs["run-new"].clone();
            let claim = m.claims["线程"].clone();
            for method in ["item/started", "item/completed"] {
                apply(
                    &mut m,
                    notify(
                        method,
                        json!({"threadId":"线程","turnId":"旧轮",
                    "item":{"id":"旧命令","type":"commandExecution","command":"旧命令"}}),
                    ),
                );
                assert_eq!(m.runs.runs["run-new"], before, "旧条目不得改变进度或结果");
            }
            apply(
                &mut m,
                complete(
                    "线程",
                    "旧轮",
                    "completed",
                    vec![message_item("旧条目", "迟到旧结果")],
                ),
            );
            assert_eq!(m.claims["线程"], claim);
            assert_eq!(m.runs.runs["run-new"].turns, before.turns);
            assert_eq!(m.runs.runs["run-new"].progress, before.progress);
            assert_eq!(m.runs.runs["run-new"].status, before.status);
            assert_eq!(m.runs.runs["run-new"], before);
            if !known {
                reply(
                    &mut m,
                    &start,
                    Ok(json!({"turn":full_turn("新轮", "inProgress", vec![])})),
                );
            }
            apply(
                &mut m,
                complete(
                    "线程",
                    "新轮",
                    "failed",
                    vec![message_item("新条目", "新结果")],
                ),
            );
            let record = &m.runs.runs["run-new"];
            assert_eq!(record.state, RunState::Ended);
            assert_eq!(record.status, Some(RunStatus::Failed));
            assert_eq!(record.turns.len(), 1);
            assert_eq!(record.turns[0].turn_id, "新轮");
            assert_eq!(record.turns[0].output, "新结果");
            assert_eq!(record.turns[0].ready, Ready::Ready);
        }
    }
}

#[test]
fn 无归属的其他轮次条目不能回退到当前运行() {
    let (mut m, context) = start(false, "turn/start");
    reply(
        &mut m,
        &context,
        Ok(json!({"turn":full_turn("本轮", "inProgress", vec![])})),
    );
    let before = m.runs.runs[RUN].clone();
    apply(
        &mut m,
        notify(
            "item/completed",
            json!({"threadId":"线程","turnId":"其他轮",
        "item":message_item("其他条目", "不属于本轮")}),
        ),
    );
    assert_eq!(m.runs.runs[RUN], before);
}

#[test]
fn 尚未知轮次的认领允许本轮完成回退并生成结果() {
    let (mut m, _context) = start(false, "turn/start");
    let actions = apply(&mut m, complete("线程", "本轮", "interrupted", vec![]));
    let read = result_read(&actions);
    assert_eq!(read.method, "thread/items/list");
    let Operation::ResultRead(identity) = &read.operation else {
        unreachable!()
    };
    assert_eq!(identity.run_id, RUN);
    assert_eq!(identity.thread_id, "线程");
    assert_eq!(m.runs.runs[RUN].turns[0].ready, Ready::Pending);
    assert_eq!(m.runs.runs[RUN].status, Some(RunStatus::Interrupted));
    reply(
        &mut m,
        &read,
        Ok(
            json!({"data":[{"turnId":"本轮","item":message_item("条目", "补全输出"),"startedAtMs":null,"completedAtMs":null}],"nextCursor":null,"backwardsCursor":null}),
        ),
    );
    let record = &m.runs.runs[RUN];
    assert_eq!(record.run_id, RUN);
    assert_eq!(record.status, Some(RunStatus::Interrupted));
    assert_eq!(record.turns[0].status, TurnStatus::Interrupted);
    assert_eq!(record.turns[0].ready, Ready::Ready);
    assert_eq!(record.turns[0].output, "补全输出");
}
