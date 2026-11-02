//! 缺陷 C：真实落盘恢复、缓存淘汰和分页发现链。
use super::*;
use std::collections::VecDeque;

const R2: &str = "run-resumed";
const OLD: [&str; 3] = ["旧甲", "旧乙", "旧丙"];
const NEW: [&str; 2] = ["新甲", "新乙"];

fn replies(m: &mut Machine, mock: &SubagentMock, actions: Vec<Action>, history: bool) {
    let mut queue: VecDeque<_> = actions.into();
    let mut steps = 0;
    while let Some(action) = queue.pop_front() {
        steps += 1;
        assert!(steps < 2000, "发现回包链必须有界");
        let Action::Request(context) = action else {
            continue;
        };
        if let crate::claim::Operation::ResultRead(read) = &context.operation {
            let value = json!({"data":[{"turnId":read.turn_id,"item":{"id":"answer","type":"agentMessage","text":"任务完成","phase":"final_answer"}}],"nextCursor":null});
            queue.extend(apply(
                m,
                Event::Reply {
                    context,
                    result: Ok(value),
                },
            ));
            continue;
        }
        if !matches!(context.operation, crate::claim::Operation::Discovery { .. }) {
            continue;
        }
        let params = context.params.as_ref().unwrap();
        let value = match context.method.as_str() {
            "thread/list" => {
                let archived = params["archived"] == true;
                let next = params["cursor"].as_str().is_some();
                let children: Vec<&str> = if history {
                    match (archived, next) {
                        (true, _) => vec![OLD[2]],
                        (false, false) => vec![OLD[0], OLD[1], NEW[0]],
                        (false, true) => vec![OLD[1], NEW[0], NEW[1]],
                    }
                } else {
                    OLD.to_vec()
                };
                let data: Vec<_> = children
                    .iter()
                    .map(|t| mock.thread_info(t).unwrap())
                    .collect();
                json!({"data":data,"nextCursor": if history && !archived && !next { Some("第二页") } else { None }})
            }
            "thread/loaded/list" => json!({"data":[],"nextCursor":null}),
            "thread/goal/get" => json!({"goal":null}),
            "thread/read" => {
                let thread = params["threadId"].as_str().unwrap();
                if thread == P {
                    json!({"thread":{"id":P,"canAcceptDirectInput":true}})
                } else {
                    let mut info = mock.thread_info(thread).unwrap();
                    info["status"] = json!({"type": if history && OLD.contains(&thread) { "notLoaded" } else { "idle" }});
                    info["turns"] = json!([{"id":format!("{thread}轮"),"status":"completed"}]);
                    json!({"thread":info})
                }
            }
            method => panic!("未预期发现方法 {method}"),
        };
        queue.extend(apply(
            m,
            Event::Reply {
                context,
                result: Ok(value),
            },
        ));
    }
}

#[tokio::test]
async fn 恢复并真实淘汰后旧三子不污染新二子账本() {
    let temp = crate::store::tests::Temp::new();
    let (mut first, mut mock) = setup();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    first.clock.now_ms = now;
    first.runs.runs.get_mut(R).unwrap().started_at = now;
    for child in OLD {
        spawn(
            &mut first,
            &mut mock,
            P,
            T,
            child,
            child,
            &format!("{child}轮"),
        );
        complete(
            &mut first,
            &mut mock,
            child,
            &format!("{child}轮"),
            "completed",
        );
    }
    let actions = complete(&mut first, &mut mock, P, T, "completed");
    replies(&mut first, &mock, actions, false);
    assert!(first.convergences.progress[R].reading.is_some());
    confirm_parent(&mut first);
    let actions = complete(&mut first, &mut mock, P, "汇总轮", "completed");
    replies(&mut first, &mock, actions, false);
    assert_eq!(first.runs.runs[R].state, RunState::Ended);
    assert_eq!(first.runs.runs[R].children.len(), 3);
    let (store, _) = crate::store::open(temp.config()).await.unwrap();
    store.submit(crate::store::StoreOp::PutRun(Box::new(
        first.runs.runs[R].clone(),
    )));
    store.submit(crate::store::StoreOp::PutThreads(first.runs.index.clone()));
    assert!(store
        .drain(std::time::Duration::from_secs(5))
        .await
        .abandoned
        .is_empty());
    drop(store);
    let saved: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.dir().join("runs").join(format!("{R}.json"))).unwrap(),
    )
    .unwrap();
    assert_eq!(saved["payload"]["state"], "ended");
    assert_eq!(saved["payload"]["children"].as_array().unwrap().len(), 3);
    let (store, loaded) = crate::store::open(temp.config()).await.unwrap();
    assert!(store
        .drain(std::time::Duration::from_secs(5))
        .await
        .abandoned
        .is_empty());
    drop(store);
    assert_eq!(loaded.runs.len(), 1);
    let mut m = Machine::default();
    apply(
        &mut m,
        Event::Restore {
            index: loaded.index,
            runs: loaded.runs,
            inbox: loaded.inbox,
        },
    );
    assert!(m.relations.operations.is_empty());
    assert!(m.relations.bindings.is_empty());
    m.clock.mono_ms = 2000;
    m.clock.now_ms = now + 1000;
    assert!(m.enable_convergence(ready()));
    apply(&mut m, Event::Connected { conn: 1 });
    apply(&mut m, Event::Initialized { conn: 1, cxb: true });
    m.record(P).subscribed = true;
    let actions = apply(
        &mut m,
        Event::ClientRequest {
            request: ClientRequest {
                conn: 1,
                id: json!(2),
                method: "turn/start".into(),
                params: Some(json!({"threadId":P,"input":[{"type":"text","text":"续做"}]})),
            },
            run_id: Some(R2.into()),
        },
    );
    let context = actions
        .into_iter()
        .find_map(|a| match a {
            Action::Request(c) if c.method == "turn/start" => Some(c),
            _ => None,
        })
        .unwrap();
    assert!(!m.runs.runs.contains_key(R), "run_opened 必须真实淘汰旧完整运行；runs={:?} index={:?} notices={:?} records={:?} dirty={:?}", m.runs.runs.keys(), m.runs.index, m.runs.notices, m.runs.runs, m.runs.diagnostic_dirty);
    apply(
        &mut m,
        Event::Reply {
            context,
            result: Ok(json!({"turn":{"id":"续跑父轮"}})),
        },
    );
    notify(
        &mut m,
        "turn/started",
        json!({"threadId":P,"turn":{"id":"续跑父轮","status":"inProgress","items":[]}}),
    );
    m.convergences.test_budget = Some(BudgetAdmission::Allowed);
    m.convergences.test_read = Some(BatchReadId(9));
    for child in NEW {
        spawn(
            &mut m,
            &mut mock,
            P,
            "续跑父轮",
            child,
            child,
            &format!("{child}轮"),
        );
        complete(&mut m, &mut mock, child, &format!("{child}轮"), "completed");
    }
    let actions = complete(&mut m, &mut mock, P, "续跑父轮", "completed");
    replies(&mut m, &mock, actions, true);
    let facts = m.convergence_facts(R2).unwrap();
    assert!(
        facts
            .activities
            .iter()
            .all(|a| !OLD.contains(&a.key.thread_id.as_str())),
        "旧子不得进入本次活动账：{:?}",
        facts.activities
    );
    assert_eq!(facts.activities.len(), 2);
    assert!(facts.activities.iter().all(|a| a.key.turn_id.is_some()));
    assert_eq!(facts.result_refs.len(), 2);
    assert!(
        m.convergences.progress[R2].reading.is_some(),
        "新子应正常进入汇总：{facts:?}"
    );
    assert_eq!(m.runs.runs[R2].children.len(), 2);
    let token = m.convergences.runs[R2].token.clone();
    let epoch = facts.work_epoch;
    m.convergences.progress.get_mut(R2).unwrap().reading = None;
    m.remember_owned_turn(P, "续跑汇总轮");
    let op = ReconcileOperation {
        token: token.clone(),
        work_epoch: epoch,
        op_id: ReconcileOpId(17),
        client_user_message_id: "续跑汇总输入".into(),
        expected_previous_turn_id: Some("续跑父轮".into()),
        target_turn_id: Some("续跑汇总轮".into()),
        submission: ReconcileSubmission::ConfirmedOwnTurn,
        outcome_unknown: false,
        automatic_control_revoked: false,
        verify_deadline_mono_ms: None,
    };
    m.convergence_operation_changed(op);
    assert!(m.convergence_delivered(&token, ReconcileOpId(17), "续跑汇总轮", epoch, &mut vec![]));
    m.convergence_event(
        P,
        ConvergeEvent::ParentTurn {
            turn: "续跑汇总轮",
            started: true,
        },
        &mut vec![],
    );
    m.runs.runs.get_mut(R2).unwrap().current_turn_id = Some("续跑汇总轮".into());
    m.run_turn_started(R2, P, "续跑汇总轮", &mut vec![]);
    let actions = complete(&mut m, &mut mock, P, "续跑汇总轮", "completed");
    replies(&mut m, &mock, actions, true);
    assert_eq!(m.runs.runs[R2].status, Some(RunStatus::Completed));
    assert!(!m.convergence_token_current(&token));
}
