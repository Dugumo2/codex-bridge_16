//! 通过真实发现回包、汇总投递和最终核实验证释放，不以 pending 清空替代终态断言。
use super::*;
use std::collections::VecDeque;

/// 按真实回包推进发现与结果读取；返回途中产生的全部动作，供零误报断言检查。
pub(super) fn replies(m: &mut Machine, mock: &SubagentMock, actions: Vec<Action>) -> Vec<Action> {
    let mut all = actions.clone();
    let mut queue: VecDeque<_> = actions.into();
    let mut count = 0;
    while let Some(action) = queue.pop_front() {
        count += 1;
        assert!(count < 512, "发现与结果读取回包链必须有界");
        let Action::Request(context) = action else {
            continue;
        };
        let value = if let crate::claim::Operation::ResultRead(read) = &context.operation {
            json!({"data":[{"turnId":read.turn_id,"item":{"id":"结果","type":"agentMessage","text":"子任务完成","phase":"final_answer"}}],"nextCursor":null})
        } else if matches!(context.operation, crate::claim::Operation::Discovery { .. }) {
            match context.method.as_str() {
                "thread/list" => {
                    let data: Vec<_> = mock
                        .threads
                        .keys()
                        .filter(|thread| thread.as_str() != P)
                        .filter_map(|thread| mock.thread_info(thread))
                        .collect();
                    json!({"data":data,"nextCursor":null})
                }
                "thread/loaded/list" => json!({"data":[],"nextCursor":null}),
                "thread/goal/get" => json!({"goal":null}),
                "thread/read" => {
                    let thread = context.params.as_ref().unwrap()["threadId"]
                        .as_str()
                        .unwrap();
                    if thread == P {
                        json!({"thread":{"id":P,"canAcceptDirectInput":true}})
                    } else {
                        let mut info = mock.thread_info(thread).unwrap();
                        info["status"] = json!({"type":"idle"});
                        json!({"thread":info})
                    }
                }
                // thread_info 只有元数据；轮次由分页读给出，用本夹具已观察到的完整轮次。
                "thread/turns/list" => {
                    let thread = context.params.as_ref().unwrap()["threadId"]
                        .as_str()
                        .unwrap();
                    json!({"data":mock.threads[thread]["turns"].clone(),"nextCursor":null})
                }
                method => panic!("未预期发现请求 {method}"),
            }
        } else {
            continue;
        };
        let next = apply(
            m,
            Event::Reply {
                context,
                result: Ok(value),
            },
        );
        all.extend(next.iter().cloned());
        queue.extend(next);
    }
    all
}

#[test]
fn 失败跟进占位撤销后完整汇总并最终收敛() {
    for tool in ["sendInput", "resumeAgent"] {
        let (mut m, mut mock) = setup();
        spawn(&mut m, &mut mock, P, T, "子", "派生", "t1");
        complete(&mut m, &mut mock, "子", "t1", "completed");
        let original = m.convergences.ledgers[R].activities[0].key.clone();
        let result_ref = m.convergences.ledgers[R].results[0].clone();
        let (started, failed) = SubagentMock::v1_message(tool, P, T, "失败跟进", "子", true);
        apply(&mut m, started);
        assert!(
            m.convergences.ledgers[R]
                .activities
                .iter()
                .any(|a| a.key.turn_id.is_none()),
            "先确认故障所需占位已建立"
        );
        apply(&mut m, failed);
        let actions = complete(&mut m, &mut mock, P, T, "completed");
        replies(&mut m, &mock, actions);
        // 只有协调器实际越过 settled 门才能投递汇总；故障变异必须落在下面的最终收敛断言。
        if m.convergences.progress[R].reading.is_some() {
            confirm_parent(&mut m);
            let actions = complete(&mut m, &mut mock, P, "汇总轮", "completed");
            // 最终发现前读取当前账本；正常释放钩子随后会主动删除账本。
            let ledger = &m.convergences.ledgers[R];
            assert_eq!(ledger.activities.len(), 1);
            assert_eq!(ledger.activities[0].key, original);
            assert_eq!(ledger.results, vec![result_ref]);
            replies(&mut m, &mock, actions);
        }
        assert_eq!(
            m.runs.runs[R].state,
            RunState::Ended,
            "失败跟进必须通过汇总及最终核实正常收敛"
        );
        assert_eq!(m.runs.runs[R].status, Some(RunStatus::Completed));
        assert!(!m.claims.contains_key(P));
        assert_eq!(m.runs.runs[R].children.len(), 1);
        assert_eq!(m.runs.runs[R].children[0].turn_ids, vec!["t1".to_owned()]);
        assert!(
            !m.convergences.ledgers.contains_key(R),
            "最终释放已回收账本，不能对旧克隆断言"
        );
    }
}

fn v2_converges(children: usize, child_before_interacted: bool) {
    let (mut m, mut mock) = setup();
    for index in 0..children {
        let child = format!("子{index}");
        spawn(
            &mut m,
            &mut mock,
            P,
            T,
            &child,
            &format!("派生{index}"),
            "t1",
        );
        complete(&mut m, &mut mock, &child, "t1", "completed");
        let item = format!("v2跟进{index}");
        notify(
            &mut m,
            "rawResponseItem/completed",
            json!({"threadId":P,"turnId":T,
            "item":{"type":"function_call","name":"followup_task","call_id":item}}),
        );
        if child_before_interacted {
            apply(&mut m, mock.turn_started(&child, "t2"));
            complete(&mut m, &mut mock, &child, "t2", "completed");
        }
        let (started, completed) = SubagentMock::v2_activity("interacted", P, T, &item, &child);
        apply(&mut m, started);
        apply(&mut m, completed);
    }
    let actions = complete(&mut m, &mut mock, P, T, "completed");
    replies(&mut m, &mock, actions);
    if !child_before_interacted {
        for index in 0..children {
            let child = format!("子{index}");
            let actions = apply(&mut m, mock.turn_started(&child, "t2"));
            replies(&mut m, &mock, actions);
            let actions = complete(&mut m, &mut mock, &child, "t2", "completed");
            replies(&mut m, &mock, actions);
        }
    }
    for _ in 0..8 {
        if m.convergences.progress[R].reading.is_some() {
            break;
        }
        if m.convergences.progress[R].retry_at.is_none() {
            break;
        }
        let actions = retry(&mut m);
        replies(&mut m, &mock, actions);
    }
    if m.convergences.progress[R].reading.is_some() {
        confirm_parent(&mut m);
        let actions = complete(&mut m, &mut mock, P, "汇总轮", "completed");
        let ledger = &m.convergences.ledgers[R];
        assert!(ledger.pending.is_empty());
        assert_eq!(ledger.activities.len(), children * 2);
        assert!(ledger
            .activities
            .iter()
            .all(|activity| activity.verified && activity.key.turn_id.is_some()));
        assert_eq!(ledger.results.len(), children * 2);
        replies(&mut m, &mock, actions);
    }
    assert_eq!(
        m.runs.runs[R].state,
        RunState::Ended,
        "真实 v2 跟进须经汇总和最终核实正常收敛"
    );
    assert_eq!(m.runs.runs[R].status, Some(RunStatus::Completed));
    assert!(!m.convergences.ledgers.contains_key(R));
}

#[test]
fn v2父轮先结束再开始子轮最终收敛() {
    v2_converges(1, false);
}

#[test]
fn v2子轮先完成再收到双interacted最终收敛() {
    v2_converges(1, true);
}

#[test]
fn v2三个跟进父轮先结束最终收敛() {
    v2_converges(3, false);
}
