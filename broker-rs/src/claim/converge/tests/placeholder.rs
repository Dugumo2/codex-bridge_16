//! 通过真实发现回包、汇总投递和最终核实验证释放，不以 pending 清空替代终态断言。
use super::*;
use std::collections::VecDeque;

fn replies(m: &mut Machine, mock: &SubagentMock, actions: Vec<Action>) {
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
                    json!({"data":[mock.thread_info("子").unwrap()],"nextCursor":null})
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
                        // thread_info 只有元数据；真实发现还需要本夹具已观察到的完整轮次。
                        info["turns"] = mock.threads[thread]["turns"].clone();
                        json!({"thread":info})
                    }
                }
                method => panic!("未预期发现请求 {method}"),
            }
        } else {
            continue;
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
        let ledger = m.convergences.ledgers[R].clone();
        if m.convergences.progress[R].reading.is_some() {
            confirm_parent(&mut m);
            let actions = complete(&mut m, &mut mock, P, "汇总轮", "completed");
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
        assert_eq!(ledger.activities.len(), 1);
        assert_eq!(ledger.activities[0].key, original);
        assert_eq!(ledger.results, vec![result_ref]);
    }
}
