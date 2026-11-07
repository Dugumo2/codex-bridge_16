//! 缺陷 C：真实落盘恢复、缓存淘汰和分页发现链。
use super::*;
use std::collections::VecDeque;

const R2: &str = "run-resumed";
const OLD: [&str; 3] = ["旧甲", "旧乙", "旧丙"];
const NEW: [&str; 2] = ["新甲", "新乙"];

fn replies(m: &mut Machine, mock: &SubagentMock, actions: Vec<Action>, history: bool) {
    assert!(replies_with_hold(m, mock, actions, history, None).is_empty());
}

fn replies_with_hold(
    m: &mut Machine,
    mock: &SubagentMock,
    actions: Vec<Action>,
    history: bool,
    held_thread: Option<&str>,
) -> Vec<crate::claim::RequestContext> {
    let mut held = Vec::new();
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
        if params["threadId"]
            .as_str()
            .is_some_and(|thread| Some(thread) == held_thread)
            && matches!(context.method.as_str(), "thread/read" | "thread/goal/get")
        {
            held.push(context);
            continue;
        }
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
                    json!({"thread":info})
                }
            }
            "thread/turns/list" => {
                let thread = params["threadId"].as_str().unwrap();
                json!({"data":[{"id":format!("{thread}轮"),"status":"completed"}],"nextCursor":null})
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
    held
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

#[test]
fn 结果读取期间历史子无轮次待答阻止汇总且不越权回答() {
    let (mut m, mut mock) = staged_with_history();
    let (read_id, epoch) = m.convergences.progress[R]
        .reading
        .expect("真实发现后结果读取已启动");
    assert!(!m.run_has_candidate(R, OLD[0]));
    assert!(crate::relations::convergence::settled(
        &m.convergence_facts(R).unwrap()
    ));
    let token = m.convergences.runs[R].token.clone();
    let result = batch(&m);
    assert_eq!(result.work_epoch, epoch);
    // 若错误走到提交边界，明确的测试拒绝会留下 WakeLimit，而不是靠计时断言。
    m.convergences.test_submission = Some(Err(ReconcileRefusal::WakeLimit));
    let (_, request) = mock.request(
        OLD[0],
        None,
        Some(json!(907)),
        "item/commandExecution/requestApproval",
    );
    let mut actions = apply(&mut m, request);
    assert!(
        m.run_has_candidate(R, OLD[0]),
        "读取期间无轮次请求必须立即使历史分类失效"
    );
    assert!(m.convergence_facts(R).unwrap().pending_requests > 0);
    m.convergence_results_ready(token, read_id, result, &mut actions);
    assert!(
        !join(&m).automatic_control_revoked,
        "待答期间不得走到汇总提交边界"
    );
    assert_eq!(join(&m).terminal_reason, None);
    assert_eq!(join(&m).delivered_epoch, 0);
    assert!(m.claims.contains_key(P), "待答候选仍阻止释放");
    assert!(
        !actions.iter().any(|a| matches!(a,
        Action::Request(context) if context.method == "turn/start")),
        "结果回包不得提交汇总轮"
    );
    assert!(
        !actions.iter().any(|a| matches!(a,
        Action::ServerResult { id, .. } | Action::ServerError { id, .. } if id == &json!(907))),
        "历史候选不能由本运行越权回答"
    );
    let pending = m
        .relations
        .requests
        .values()
        .find(|r| r.upstream_request_id == json!(907))
        .unwrap();
    assert_eq!(pending.stage, ChildRequestStage::Lookup);
    assert!(pending.run_id.is_none());
    assert!(m.relations.bindings.values().all(|b| b.thread_id != OLD[0]));
}

/// 完整发现链已经结束、两项本次结果正在读取；三个旧子已核实排除。
fn staged_with_history() -> (Machine, SubagentMock) {
    let (mut m, mut mock) = setup();
    for child in OLD {
        mock.register(child, Some(P), "v1", false, true);
        m.runs.index.child_history.remember(
            "旧运行",
            P,
            &[ChildRecord {
                thread_id: child.into(),
                parent_thread_id: P.into(),
                relation_kind: RelationKind::Subagent,
                turn_ids: vec![format!("{child}轮")],
                evidence: BindingEvidence::V1Spawn,
                final_state: MemberActivity::Settled,
            }],
            1,
            &Default::default(),
        );
    }
    for child in NEW {
        spawn(&mut m, &mut mock, P, T, child, child, &format!("{child}轮"));
        complete(&mut m, &mut mock, child, &format!("{child}轮"), "completed");
    }
    let actions = complete(&mut m, &mut mock, P, T, "completed");
    replies(&mut m, &mock, actions, true);
    assert!(m.convergences.progress[R].reading.is_some());
    (m, mock)
}

#[test]
fn 最终核实已完成历史候选收到活动通知后不得释放() {
    for status in ["active", "pendingInit"] {
        let (mut m, mut mock) = staged_with_history();
        let token = confirm_parent(&mut m);
        let actions = complete(&mut m, &mut mock, P, "汇总轮", "completed");
        assert_eq!(join(&m).phase, JoinPhase::FinalCheck);
        let (op, _) = m.convergences.progress[R]
            .discovery
            .expect("最终核实发现已启动");
        let held = replies_with_hold(&mut m, &mock, actions, true, Some(OLD[2]));
        assert!(!held.is_empty(), "另一个历史候选核实回包实际在途");
        let work = &m.relations.aux.work[&op];
        for method in ["thread/read", "thread/goal/get"] {
            assert!(
                work.checks_done.contains(&(OLD[0].into(), method.into())),
                "目标历史候选已经完成当前批次核实：{method}"
            );
        }
        assert!(!m.run_has_candidate(R, OLD[0]));
        notify(
            &mut m,
            "thread/status/changed",
            json!({"threadId":OLD[0],"status":{"type":status}}),
        );
        assert!(
            m.run_has_candidate(R, OLD[0]),
            "最终核实期间活动通知立即撤销排除：{status}"
        );
        let mut pending: std::collections::VecDeque<_> = held.into_iter().collect();
        while let Some(context) = pending.pop_front() {
            let value = if context.method == "thread/goal/get" {
                json!({"goal":null})
            } else if context.method == "thread/turns/list" {
                json!({"data":[{"id":format!("{}轮", OLD[2]),"status":"completed"}],"nextCursor":null})
            } else {
                let mut info = mock.thread_info(OLD[2]).unwrap();
                info["status"] = json!({"type":"notLoaded"});
                json!({"thread":info})
            };
            // 仅放行原最终核实批次；新一轮核实不得被夹具凭空回答成静止。
            let next = apply(
                &mut m,
                Event::Reply {
                    context,
                    result: Ok(value),
                },
            );
            pending.extend(next.into_iter().filter_map(|action| match action {
                Action::Request(next) if next.method == "thread/turns/list" => Some(next),
                _ => None,
            }));
        }
        assert!(
            !m.relations.discoveries.contains_key(&op),
            "原最终核实批次所有回包已经放行"
        );
        assert!(
            m.run_has_candidate(R, OLD[0]),
            "最终发布不能排除已重新活动的历史子：{status}"
        );
        assert_eq!(
            m.runs.runs[R].state,
            RunState::Running,
            "最终核实活动反证阻止释放：{status}"
        );
        assert!(m.claims.contains_key(P));
        assert!(m.convergence_token_current(&token));
        assert!(m
            .convergence_facts(R)
            .unwrap()
            .discovery_covered_epoch
            .is_none());
        assert_eq!(join(&m).terminal_reason, None);
    }
}
