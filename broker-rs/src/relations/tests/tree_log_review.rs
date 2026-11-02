//! 聚合日志审查回归：通过真实通知入口覆盖未绑定祖先和跟进拒绝。
use super::*;
use crate::claim::{ReqKind, TimerKind};
use crate::runs::{RunOpen, RunTimer};
use crate::store::StoreOp;

const G: &str = "孙线程";

fn open_run(m: &mut Machine, run: &str, root: &str) {
    m.run_opened(
        RunOpen {
            thread_id: root.into(),
            run_id: run.into(),
            conn: Some(1),
            kind: ReqKind::TurnStart,
            joined: false,
            input: "日志回归".into(),
            continued_from: None,
        },
        &mut Vec::new(),
    );
}

fn log_rows(actions: &[Action]) -> Vec<Value> {
    actions
        .iter()
        .flat_map(|a| match a {
            Action::Store(StoreOp::AppendLog { short_id, lines }) => lines
                .iter()
                .map(|line| {
                    let parts: Vec<_> = line.splitn(4, ' ').collect();
                    let mut payload: Value =
                        serde_json::from_str(parts[3].strip_prefix("@cxb2 ").unwrap()).unwrap();
                    payload["ts"] = json!(parts[0]);
                    payload["runId"] = json!(parts[1]);
                    payload["kind"] = json!(parts[2]);
                    payload["fileShortId"] = json!(short_id);
                    payload
                })
                .collect(),
            _ => vec![],
        })
        .collect()
}

/// P 的派生确认未到，C 只有血缘；C 的派生操作已发现 G，但运行编号仍为空。
fn delayed_ancestor() -> (Machine, SubagentMock, Event, Vec<Action>) {
    let (mut m, mut mock) = setup(false);
    open_run(&mut m, R, P);
    mock.register(C, Some(P), "v1", false, true);
    mock.register(G, Some(C), "v1", false, true);
    let (parent_started, parent_completed) = SubagentMock::v1_spawn(P, T, "父派生", C, false);
    let child_started = mock.turn_started(C, "子轮");
    let (grand_started, grand_completed) = SubagentMock::v1_spawn(C, "子轮", "子派生", G, false);
    let events = vec![
        parent_started,
        mock.thread_started(C),
        child_started,
        grand_started,
        grand_completed,
    ];
    let actions = run(&mut m, &mut mock, events);
    assert!(m.binding(C, "子轮").is_none());
    assert!(!m.manage_context(R).unwrap().members.contains_key(C));
    assert!(m
        .relations
        .operations
        .values()
        .any(|op| op.receivers.contains(G) && op.run_id.is_empty()));
    (m, mock, parent_completed, actions)
}

fn grand_item(method: &str, id: &str) -> Event {
    Event::Notification {
        method: method.into(),
        params: Some(
            json!({"threadId":G,"turnId":"孙轮","item":{"id":id,"type":"agentMessage","text":"孙线程已观察摘要"}}),
        ),
    }
}

#[test]
fn tree_log_review_祖先晚绑定补写孙事件保留原时间且不重放完成() {
    for operation_only in [false, true] {
        let (mut m, mut mock, release_parent, mut actions) = delayed_ancestor();
        // 只有空 run_id 操作仍能找到候选；不能只靠完整的直接血缘边。
        let removed_edge = operation_only.then(|| m.relations.edges.remove(G).unwrap());
        let saved_index = m.runs.index.clone();
        if operation_only {
            for entry in m
                .runs
                .index
                .threads
                .values_mut()
                .filter(|entry| entry.thread_id == G)
            {
                entry.parent_thread_id = None;
                entry.relation_kind = None;
            }
        }
        let started = mock.turn_started(G, "孙轮");
        let error = Event::Notification {
            method: "error".into(),
            params: Some(
                json!({"threadId":G,"turnId":"孙轮","willRetry":true,"error":{"message":"Authorization: secret-grand"}}),
            ),
        };
        let completed = mock.turn_completed(G, "孙轮", "completed");
        let events = [
            started,
            grand_item("item/started", "孙条目"),
            grand_item("item/completed", "孙条目"),
            error,
            completed,
        ];
        for (offset, event) in events.into_iter().enumerate() {
            m.clock.now_ms = 1_700_000_010_000 + offset as u64;
            actions.extend(mock.inject(&mut m, event));
        }
        assert!(m.binding(G, "孙轮").is_none());
        assert!(!log_rows(&actions).iter().any(|row| row["threadId"] == G));
        assert!(m.records[G].ended_turns.iter().any(|turn| turn == "孙轮"));
        let before_turns = m.runs.runs[R].turns.clone();
        let before_errors = m.runs.runs[R].child_upstream_errors.clone();
        if let Some(edge) = removed_edge {
            m.relations.edges.insert(G.into(), edge);
            m.runs.index = saved_index;
        }
        m.clock.now_ms = 1_700_000_020_000;
        let flushed = mock.inject(&mut m, release_parent);
        assert_eq!(m.binding(G, "孙轮").unwrap().run_id, R);
        let rows: Vec<_> = log_rows(&flushed)
            .into_iter()
            .filter(|row| row["threadId"] == G && row["late"] == true)
            .collect();
        assert_eq!(rows.len(), 5, "空操作上溯={operation_only}");
        assert_eq!(
            rows.iter()
                .map(|r| r["kind"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["轮次开始", "条目开始", "条目完成", "子线程错误", "轮次完成"]
        );
        for (offset, row) in rows.iter().enumerate() {
            assert_eq!(
                row["ts"],
                crate::runs::utc_timestamp(1_700_000_010_000 + offset as u64)
            );
            assert_eq!(row["runId"], R);
            assert_eq!(row["parentThreadId"], C);
            assert_eq!(row["fileShortId"], m.runs.runs[R].short_id);
        }
        assert!(!format!("{rows:?}").contains("secret-grand"));
        assert_eq!(m.runs.runs[R].turns, before_turns);
        assert_eq!(
            m.runs.runs[R].child_upstream_errors, before_errors,
            "补写不重新驱动错误诊断"
        );
        assert!(m.runs.child_event_log.iter().all(|(thread, _)| thread != G));
    }
}

#[test]
fn tree_log_review_祖先未绑定的孙事件超限与过期留下缺口() {
    let (mut m, mut mock, _, mut actions) = delayed_ancestor();
    let event = mock.turn_started(G, "孙轮");
    actions.extend(mock.inject(&mut m, event));
    for n in 0..500 {
        actions.extend(mock.inject(&mut m, grand_item("item/completed", &format!("孙条目-{n}"))));
    }
    let overflow: Vec<_> = log_rows(&actions)
        .into_iter()
        .filter(|r| r["threadId"] == G && r["coverageIssue"] == "beforeBindingUnknown")
        .collect();
    assert!(!overflow.is_empty(), "未绑定祖先不能让孙事件静默丢失");
    assert!(overflow
        .iter()
        .all(|r| r["runId"] == R && r["parentThreadId"] == C));
    let timer = actions
        .iter()
        .find_map(|a| match a {
            Action::Schedule {
                token,
                kind: kind @ TimerKind::Run(RunTimer::EventLogSweep { .. }),
                ..
            } => Some((token.clone(), kind.clone())),
            _ => None,
        })
        .expect("孙候选摘要有到期计时");
    m.clock.mono_ms += 30_000;
    m.clock.now_ms += 30_000;
    let expired = mock.inject(
        &mut m,
        Event::Timer {
            token: timer.0,
            kind: timer.1,
        },
    );
    let gaps: Vec<_> = log_rows(&expired)
        .into_iter()
        .filter(|r| r["threadId"] == G)
        .collect();
    assert!(!gaps.is_empty());
    assert!(gaps.iter().all(|r| r["kind"] == "记录缺口"
        && r["coverageIssue"] == "beforeBindingUnknown"
        && r["runId"] == R
        && r["parentThreadId"] == C));
    assert!(m.binding(G, "孙轮").is_none());
}

#[test]
fn tree_log_review_跟进通知拒绝按候选运行记冲突且不接纳活动() {
    for tool in ["sendInput", "resumeAgent", "interacted"] {
        for independent in [true, false] {
            let (mut m, mut mock) = setup(false);
            open_run(&mut m, R, P);
            open_run(&mut m, "旧运行", if independent { C } else { "旧根" });
            assert!(
                m.enable_convergence(crate::claim::converge::ConvergenceReadiness {
                    trusted_inputs: true,
                    budget_admission: true,
                    asynchronous_results: true,
                    terminal_mapping: true,
                    cancellation_verified: true,
                })
            );
            m.ensure_context(R);
            mock.register(C, Some(P), "v1", false, true);
            let edge = mock.thread_started(C);
            mock.inject(&mut m, edge);
            if independent {
                m.claims
                    .insert(C.into(), claim(Owner::Client(2), "独立轮", "旧运行", 9));
            } else {
                m.relations.bindings.insert(
                    (C.into(), "旧子轮".into()),
                    ActivityBinding {
                        thread_id: C.into(),
                        turn_id: "旧子轮".into(),
                        run_id: "旧运行".into(),
                        parent_thread_id: "旧根".into(),
                        evidence: BindingEvidence::V1Spawn,
                        operation: EvidenceKey {
                            parent_thread_id: "旧根".into(),
                            parent_turn_id: "旧父轮".into(),
                            item_id: "旧派生".into(),
                        },
                        status: BindingStatus::Bound,
                    },
                );
            }
            let before_bindings = m.relations.bindings.clone();
            let (started, completed) = if tool == "interacted" {
                SubagentMock::v2_activity(tool, P, T, "跟进拒绝", C)
            } else {
                SubagentMock::v1_message(tool, P, T, "跟进拒绝", C, false)
            };
            let actions = run(
                &mut m,
                &mut mock,
                vec![started.clone(), completed.clone(), started, completed],
            );
            let conflicts: Vec<_> = log_rows(&actions)
                .into_iter()
                .filter(|r| r["kind"] == "归属冲突")
                .collect();
            assert_eq!(conflicts.len(), 1, "{tool}，独立认领={independent}");
            let row = &conflicts[0];
            assert_eq!(row["runId"], R);
            assert_eq!(row["fileShortId"], m.runs.runs[R].short_id);
            assert_eq!(row["threadId"], C);
            assert_eq!(row["parentThreadId"], P);
            assert_eq!(row["itemId"], "跟进拒绝");
            assert!(row["turnId"].is_null(), "不能把旧绑定轮次当成当前子轮");
            assert_eq!(row["coverageIssue"], "discoveryIncomplete");
            assert!(row["summary"].as_str().unwrap().contains(if independent {
                "独立认领"
            } else {
                "其他运行"
            }));
            assert_eq!(m.relations.bindings, before_bindings);
            assert!(!m.manage_context(R).unwrap().members.contains_key(C));
            let new_turn = mock.turn_started(C, "被拒绝的新轮");
            let mut late = mock.inject(&mut m, new_turn);
            late.extend(mock.inject(&mut m, Event::Notification { method: "item/completed".into(), params: Some(json!({"threadId":C,"turnId":"被拒绝的新轮","item":{"id":"拒绝条目","type":"agentMessage","text":"不入候选运行"}})) }));
            assert!(!log_rows(&late)
                .iter()
                .any(|r| r["runId"] == R && r["threadId"] == C && r["kind"] != "记录缺口"));
            assert!(m.binding(C, "被拒绝的新轮").is_none());
        }
    }
}
