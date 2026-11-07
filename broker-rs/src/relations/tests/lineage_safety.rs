//! 缺陷 C 的反向安全：未知候选阻塞收敛，但不能借发现或旧成员身份取得管理权。
use super::*;
use crate::claim::converge::ConvergenceReadiness;
use crate::relations::convergence::settled;

pub(super) fn machine() -> (Machine, SubagentMock) {
    let (mut m, mock) = setup(false);
    assert!(m.enable_convergence(ConvergenceReadiness {
        trusted_inputs: true,
        budget_admission: true,
        asynchronous_results: true,
        terminal_mapping: true,
        cancellation_verified: true,
    }));
    let mut actions = Vec::new();
    m.run_opened(
        crate::runs::RunOpen {
            thread_id: P.into(),
            run_id: R.into(),
            conn: Some(1),
            kind: crate::claim::ReqKind::TurnStart,
            joined: false,
            input: "安全回归".into(),
            continued_from: None,
        },
        &mut actions,
    );
    assert_eq!(m.convergence_begin(P, &mut actions).as_deref(), Some(R));
    (m, mock)
}

fn candidate(m: &mut Machine, mock: &mut SubagentMock, thread: &str) {
    mock.register(thread, Some(P), "v1", false, true);
    let started = mock.thread_started(thread);
    mock.inject(m, started);
}

fn history(m: &mut Machine, thread: &str) {
    m.runs.index.child_history.remember(
        "历史运行",
        P,
        &[ChildRecord {
            thread_id: thread.into(),
            parent_thread_id: P.into(),
            relation_kind: RelationKind::Subagent,
            turn_ids: vec!["历史轮".into()],
            evidence: BindingEvidence::V1Spawn,
            final_state: MemberActivity::Settled,
        }],
        1,
        &BTreeMap::new(),
    );
}

/// 用生产发现请求及回包核实快照；不写成员表、不强设 complete。
fn discover(m: &mut Machine, mock: &mut SubagentMock, snapshots: &[(&str, &str, Value, bool)]) {
    let (op, held) = discover_with_hold(m, mock, snapshots, None);
    assert!(held.is_empty());
    assert!(m.discovery_outcome(op).is_some(), "生产发现回调已完成");
}

fn discover_with_hold(
    m: &mut Machine,
    mock: &mut SubagentMock,
    snapshots: &[(&str, &str, Value, bool)],
    held_thread: Option<&str>,
) -> (u64, Vec<RequestContext>) {
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
    let mut queue: VecDeque<_> = actions.into_iter().collect();
    let mut held = Vec::new();
    let mut calls = 0;
    while let Some(action) = queue.pop_front() {
        let Action::Request(context) = action else {
            continue;
        };
        calls += 1;
        assert!(calls < 256, "发现请求必须有界完成");
        let params = context.params.clone().unwrap_or(Value::Null);
        let thread = params["threadId"].as_str().unwrap_or_default();
        if held_thread == Some(thread)
            && matches!(context.method.as_str(), "thread/read" | "thread/goal/get")
        {
            held.push(context);
            continue;
        }
        let snapshot = snapshots.iter().find(|(id, ..)| *id == thread);
        let response = match context.method.as_str() {
            "thread/list" => {
                let data: Vec<_> = if params["archived"] == true {
                    Vec::new()
                } else {
                    snapshots
                        .iter()
                        .filter_map(|(id, ..)| mock.thread_info(id))
                        .collect()
                };
                json!({"data": data, "nextCursor": null})
            }
            "thread/loaded/list" => json!({"data": [], "nextCursor": null}),
            "thread/read" => {
                let mut info = mock.thread_info(thread).expect("快照线程已登记");
                info["status"] = json!({"type": snapshot.map_or("idle", |s| s.1)});
                json!({"thread": info})
            }
            "thread/turns/list" => {
                json!({"data": snapshot.map_or_else(|| json!([]), |s| s.2.clone()), "nextCursor": null})
            }
            "thread/goal/get" => {
                json!({"goal": snapshot.filter(|s| s.3).map(|_| json!({"status":"active"}))})
            }
            _ => continue,
        };
        queue.extend(mock.inject(
            m,
            Event::Reply {
                context,
                result: Ok(response),
            },
        ));
    }
    (op, held)
}

#[test]
fn lineage_safety_未知候选独立阻塞且无活动无绑定无审批回答权并保留订阅() {
    let (mut m, mut mock) = machine();
    candidate(&mut m, &mut mock, C);
    discover(&mut m, &mut mock, &[(C, "idle", json!([]), false)]);
    assert!(m.run_has_candidate(R, C));
    assert!(!m.manage_context(R).unwrap().members.contains_key(C));
    let facts = m.convergence_facts(R).unwrap();
    assert!(
        facts.activities.is_empty(),
        "候选不能制造无来源 turnId=None 活动"
    );
    assert!(facts.result_refs.is_empty());
    assert!(facts.unresolved_candidates > 0);
    assert!(!settled(&facts));
    assert!(m.relation_needs_subscription(C), "分类核实期间不得提前退订");
    let mut actions = Vec::new();
    m.release_idle_children(Some(C), &mut actions);
    assert!(app_requests(&actions, "thread/unsubscribe").is_empty());
    let (_, request) = mock.request(C, Some("来源未知轮"), Some(json!(601)), APPROVAL);
    let routed = mock.inject(&mut m, request);
    assert!(
        forwarded(&routed, 1).is_empty(),
        "父 owner 不获候选审批回答权"
    );
    assert!(m.binding(C, "来源未知轮").is_none());
    assert!(!routed.iter().any(|a| matches!(a,
        Action::ServerResult { result, .. } if result["decision"] == "accept")));
}

#[test]
fn lineage_safety_历史线程有活动goal或pendinginit仍阻塞且不成为活动成员() {
    for (status, turns, goal) in [
        (
            "idle",
            json!([{ "id": "历史轮", "status": "completed" }]),
            true,
        ),
        ("pendingInit", json!([]), false),
        (
            "active",
            json!([{ "id": "初始化轮", "status": "pendingInit" }]),
            false,
        ),
    ] {
        let (mut m, mut mock) = machine();
        history(&mut m, C);
        candidate(&mut m, &mut mock, C);
        discover(&mut m, &mut mock, &[(C, status, turns, goal)]);
        assert!(m.run_has_candidate(R, C), "{status} / goal={goal}");
        assert!(!m.manage_context(R).unwrap().members.contains_key(C));
        let facts = m.convergence_facts(R).unwrap();
        assert!(!settled(&facts), "历史归属不能替代本次活动核实");
        assert!(facts.activities.is_empty());
    }
}

#[test]
fn lineage_safety_历史核实完成后退出候选且不再永久占用订阅() {
    let (mut m, mut mock) = machine();
    history(&mut m, C);
    candidate(&mut m, &mut mock, C);
    discover(&mut m, &mut mock, &[(C, "pendingInit", json!([]), false)]);
    assert!(m.run_has_candidate(R, C));
    discover(
        &mut m,
        &mut mock,
        &[(
            C,
            "notLoaded",
            json!([{ "id": "历史轮", "status": "completed" }]),
            false,
        )],
    );
    assert!(!m.run_has_candidate(R, C));
    assert!(!m.run_observes_thread(R, C));
    assert!(!m.relation_needs_subscription(C));
    assert!(m.convergence_facts(R).unwrap().activities.is_empty());
}

#[test]
fn lineage_safety_旧成员不能让失败撤销拒绝与旧代操作重新获得资格() {
    for invalid in ["failed", "revoked", "rejected", "connection", "generation"] {
        let (mut m, mut mock) = machine();
        history(&mut m, C);
        mock.register(C, Some(P), "v1", false, true);
        let (started, completed) = SubagentMock::v1_spawn(P, T, "有效启动", C, false);
        run(&mut m, &mut mock, vec![started, completed]);
        assert!(m.manage_context(R).unwrap().members.contains_key(C));
        assert!(
            m.relations.bindings.is_empty(),
            "尚无轮次，不以绑定掩盖失效操作"
        );
        let op = m
            .relations
            .operations
            .values_mut()
            .find(|o| o.key.item_id == "有效启动")
            .unwrap();
        match invalid {
            "failed" => op.outcome = EvidenceOutcome::Failed,
            "revoked" => op.outcome = EvidenceOutcome::Revoked,
            "rejected" => {
                op.rejected_receivers
                    .insert(C.into(), FollowupRejection::IndependentClaim);
            }
            "connection" => op.conn_epoch = op.conn_epoch.saturating_add(1),
            "generation" => op.authority_gen = op.authority_gen.saturating_add(1),
            _ => unreachable!(),
        }
        let mut actions = Vec::new();
        m.refresh_member(R, C, MemberChange::Activity, &mut actions);
        m.convergence_sync_facts(R);
        assert!(!m.current_management_evidence(R, C), "{invalid}");
        assert!(
            !m.manage_context(R).unwrap().members.contains_key(C),
            "旧成员不能自证：{invalid}"
        );
        assert!(m.historical_subagent_for_run(R, C), "{invalid}");
        assert!(
            m.convergences.ledgers[R].activities.is_empty(),
            "撤掉无来源活动：{invalid}"
        );
    }
}

#[test]
fn lineage_safety_本次有效启动尚无轮次仍是活动且阻止收敛() {
    let (mut m, mut mock) = machine();
    mock.register(C, Some(P), "v1", false, true);
    let (started, completed) = SubagentMock::v1_spawn(P, T, "有效启动", C, false);
    run(&mut m, &mut mock, vec![started, completed]);
    m.convergence_sync_facts(R);
    assert!(m.current_management_evidence(R, C));
    assert!(!m.run_has_candidate(R, C));
    assert_eq!(
        m.manage_context(R).unwrap().members[C].activity,
        MemberActivity::Starting
    );
    let facts = m.convergence_facts(R).unwrap();
    assert_eq!(facts.activities.len(), 1);
    assert!(facts.activities[0].key.turn_id.is_none());
    assert!(!facts.activities[0].verified);
    assert!(facts.pending_starts > 0);
    assert!(!settled(&facts));
}

#[test]
fn lineage_safety_候选转活动后真实绑定结果跨重新分类保留() {
    let (mut m, mut mock) = machine();
    candidate(&mut m, &mut mock, C);
    discover(&mut m, &mut mock, &[(C, "idle", json!([]), false)]);
    assert!(m.run_has_candidate(R, C));
    spawn_v1(&mut m, &mut mock, C, "本次启动", "本次轮");
    let ended = mock.turn_completed(C, "本次轮", "completed");
    mock.inject(&mut m, ended);
    m.convergence_sync_facts(R);
    assert_eq!(bound(&m, C, "本次轮").as_deref(), Some(R));
    assert!(!m.run_has_candidate(R, C));
    let results = m.convergences.ledgers[R].results.clone();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].locator.turn_id.as_deref(), Some("本次轮"));
    // 操作失效后，真正发生过的受管轮与结果仍以绑定证明，不被分类清理误删。
    m.relations
        .operations
        .values_mut()
        .find(|o| o.key.item_id == "本次启动")
        .unwrap()
        .outcome = EvidenceOutcome::Revoked;
    let mut actions = Vec::new();
    m.refresh_member(R, C, MemberChange::Activity, &mut actions);
    m.convergence_observation_gap(C, &mut actions);
    m.convergence_sync_facts(R);
    assert!(m.current_management_evidence(R, C));
    assert_eq!(m.convergences.ledgers[R].results, results);
    assert_eq!(m.convergences.ledgers[R].activities.len(), 1);
    assert_eq!(
        m.convergences.ledgers[R].activities[0]
            .key
            .turn_id
            .as_deref(),
        Some("本次轮")
    );
    assert!(m.convergences.ledgers[R].covered_epoch.is_none());
}

fn stale_check_reply(mock: &SubagentMock, context: &RequestContext) -> Value {
    if context.method == "thread/goal/get" {
        json!({"goal": null})
    } else if context.method == "thread/turns/list" {
        json!({"data": [{ "id": "历史轮", "status": "completed" }], "nextCursor": null})
    } else {
        let thread = context.params.as_ref().unwrap()["threadId"]
            .as_str()
            .unwrap();
        let mut info = mock.thread_info(thread).unwrap();
        info["status"] = json!({"type": "notLoaded"});
        json!({"thread": info})
    }
}

#[test]
fn lineage_safety_发现旧工作回包不能覆盖本次启动复用与活动通知() {
    for change in ["spawn", "reuse", "activity", "goal"] {
        let (mut m, mut mock) = machine();
        history(&mut m, C);
        candidate(&mut m, &mut mock, C);
        let (op, held) = discover_with_hold(
            &mut m,
            &mut mock,
            &[(C, "notLoaded", json!([]), false)],
            Some(C),
        );
        assert!(
            held.iter().any(|c| c.method == "thread/read"),
            "扣住实际核实读取"
        );
        assert!(
            held.iter().any(|c| c.method == "thread/goal/get"),
            "扣住实际 goal 核实"
        );
        let old_version = m.relations.aux.work[&op].candidate_versions[C];
        match change {
            "spawn" => {
                let (started, completed) = SubagentMock::v1_spawn(P, T, "新启动", C, false);
                run(&mut m, &mut mock, vec![started, completed]);
            }
            "reuse" => {
                let (started, completed) =
                    SubagentMock::v1_message("sendInput", P, T, "新复用", C, false);
                run(&mut m, &mut mock, vec![started, completed]);
            }
            "activity" => {
                let started = mock.turn_started(C, "新活动轮");
                mock.inject(&mut m, started);
            }
            "goal" => {
                mock.inject(&mut m, Event::Notification {
                    method: "thread/goal/updated".into(),
                    params: Some(json!({"threadId": C, "goal": {"status": "active", "objective": "新目标"}})),
                });
            }
            _ => unreachable!(),
        }
        assert!(
            m.relations.aux.candidate_versions[C] > old_version,
            "{change}"
        );
        let classification = m.classify_candidate(R, C);
        let observations = m.convergences.observations.clone();
        let candidate_observation = m.relations.aux.candidate_observations.get(C).cloned();
        let goal = m.convergences.goals.get(C).cloned();
        for context in held {
            let response = stale_check_reply(&mock, &context);
            mock.inject(
                &mut m,
                Event::Reply {
                    context,
                    result: Ok(response),
                },
            );
        }
        assert_eq!(m.classify_candidate(R, C), classification, "{change}");
        assert_eq!(
            m.convergences.observations, observations,
            "旧快照不能写活动事实：{change}"
        );
        assert_eq!(
            m.relations.aux.candidate_observations.get(C).cloned(),
            candidate_observation,
            "旧快照不能证明历史空闲：{change}"
        );
        assert_eq!(
            m.convergences.goals.get(C).cloned(),
            goal,
            "旧goal不能覆盖最新观察：{change}"
        );
        if m.discovery_outcome(op).is_none() {
            // 启动/复用可改变血缘版本并触发一次重查；扣住重查回包，沿原发现期限定案。
            let work = &m.relations.aux.work[&op];
            assert!(
                work.reasons.contains("candidateChanged"),
                "旧回包已被拒绝：{change}"
            );
            let discovery = m.relations.discoveries[&op].clone();
            m.clock.mono_ms = discovery.deadline_ms;
            mock.inject(
                &mut m,
                Event::Timer {
                    token: Machine::relation_token(),
                    kind: crate::claim::TimerKind::DiscoveryDeadline {
                        op_id: op,
                        generation: discovery.generation,
                    },
                },
            );
        }
        let result = m
            .discovery_outcome(op)
            .expect("旧批次在原期限内以不完整结束");
        assert!(result
            .state
            .incomplete_reasons
            .iter()
            .any(|r| r == "candidateChanged"));
        assert!(!result.state.complete);
        assert!(m.convergences.ledgers[R].covered_epoch.is_none());
    }
}

#[test]
fn lineage_safety_旧连接及旧发现操作回包不得写候选事实() {
    for stale in ["connection", "operation"] {
        let (mut m, mut mock) = machine();
        candidate(&mut m, &mut mock, C);
        let (op, held) =
            discover_with_hold(&mut m, &mut mock, &[(C, "idle", json!([]), false)], Some(C));
        assert!(held.iter().any(|c| c.method == "thread/read"));
        assert!(held.iter().any(|c| c.method == "thread/goal/get"));
        if stale == "connection" {
            m.relations.conn_epoch += 1;
        } else {
            m.relations.discoveries.get_mut(&op).unwrap().generation += 1;
        }
        let classifications = m.relations.aux.classifications.clone();
        let observations = m.convergences.observations.clone();
        let goals = m.convergences.goals.clone();
        let candidate_observations = m.relations.aux.candidate_observations.clone();
        for context in held {
            let response = stale_check_reply(&mock, &context);
            mock.inject(
                &mut m,
                Event::Reply {
                    context,
                    result: Ok(response),
                },
            );
        }
        assert_eq!(m.relations.aux.classifications, classifications, "{stale}");
        assert_eq!(m.convergences.observations, observations, "{stale}");
        assert_eq!(m.convergences.goals, goals, "{stale}");
        assert_eq!(
            m.relations.aux.candidate_observations, candidate_observations,
            "{stale}"
        );
        assert!(
            m.discovery_outcome(op).is_none(),
            "旧批次不能发布新发现结论：{stale}"
        );
    }
}

#[test]
fn lineage_safety_本次受管子notloaded夹带completed不得解锁汇总() {
    let (mut m, mut mock) = machine();
    spawn_v1(&mut m, &mut mock, C, "本次启动", "本次轮");
    discover(
        &mut m,
        &mut mock,
        &[(
            C,
            "notLoaded",
            json!([{ "id": "本次轮", "status": "completed" }]),
            false,
        )],
    );
    let facts = m.convergence_facts(R).unwrap();
    assert_eq!(facts.activities.len(), 1);
    assert_eq!(facts.activities[0].key.turn_id.as_deref(), Some("本次轮"));
    assert_ne!(
        facts.activities[0].activity,
        MemberActivity::Settled,
        "notLoaded不能让本次受管轮借历史completed页被判完成"
    );
    assert!(!settled(&facts));
    assert!(facts.result_refs.is_empty());
    assert!(!facts.discovery.complete);
    assert!(facts
        .discovery
        .incomplete_reasons
        .iter()
        .any(|r| r == "activityUnknown"));
    assert!(
        m.convergences.progress[R].reading.is_none(),
        "不得启动结果汇总"
    );
    assert!(m.claims.contains_key(P));
}

#[test]
fn lineage_safety_未知候选空活动账仍在原期限异常终止并保留原因() {
    use crate::claim::TimerKind;
    use crate::sa1b::TerminalReason;
    let (mut m, mut mock) = machine();
    candidate(&mut m, &mut mock, C);
    discover(&mut m, &mut mock, &[(C, "idle", json!([]), false)]);
    let before = m.convergence_facts(R).unwrap();
    assert!(before.activities.is_empty());
    assert!(before.unresolved_candidates > 0);
    let ended = mock.turn_completed(P, T, "completed");
    mock.inject(&mut m, ended);
    assert!(m.claims.contains_key(P), "候选未定案不能正常释放");
    let runtime = m.convergences.runs[R].clone();
    assert!(runtime.deadline_mono_ms < u64::MAX, "使用生产首次收敛期限");
    let timeout = m.runs.runs[R]
        .convergence_config
        .as_ref()
        .unwrap()
        .timeout_ms;
    assert_eq!(runtime.deadline_mono_ms - runtime.started_mono_ms, timeout);
    assert!(m
        .manage_context(R)
        .unwrap()
        .discovery
        .missing
        .get(C)
        .is_some_and(|reasons| !reasons.is_empty()));
    m.clock.mono_ms = runtime.deadline_mono_ms;
    mock.inject(
        &mut m,
        Event::Timer {
            token: runtime.token.claim.clone(),
            kind: TimerKind::ConvergeDeadline {
                token: runtime.token,
            },
        },
    );
    assert_eq!(
        m.runs.runs[R]
            .subagent_join
            .as_ref()
            .unwrap()
            .terminal_reason,
        Some(TerminalReason::DiscoveryIncomplete)
    );
    assert!(
        m.runs.runs[R]
            .subagent_join
            .as_ref()
            .unwrap()
            .automatic_control_revoked
    );
    let tree = m.tree_for_run(R).expect("期限进入异常停止收尾");
    assert_eq!(
        m.trees.operations[&tree].request.trigger,
        crate::stop::tree::TreeTrigger::DiscoveryIncomplete
    );
    let generation = m.trees.operations[&tree].generation;
    m.clock.mono_ms = m.trees.operations[&tree].request.first_delivery_deadline_ms;
    let final_actions = mock.inject(
        &mut m,
        Event::Timer {
            token: ClaimToken {
                thread_id: P.into(),
                claim_id: 0,
            },
            kind: TimerKind::TreeStopWait {
                tree_op: tree,
                waiter: None,
                generation,
            },
        },
    );
    assert_eq!(m.runs.runs[R].state, crate::runs::RunState::Ended);
    assert!(
        final_actions.iter().any(|action| matches!(action,
            Action::Store(crate::store::StoreOp::AppendLog { lines, .. })
                if lines.iter().any(|line| line.contains("记录缺口")
                    && line.contains("\"coverageIssue\":\"discoveryIncomplete\""))
        )),
        "发现无法核实时，树停止定案必须保存真实日志缺口"
    );
    assert_eq!(
        m.runs.runs[R].status,
        Some(crate::runs::RunStatus::Interrupted)
    );
    assert_eq!(
        m.runs.runs[R].interrupt_reason,
        Some(crate::runs::InterruptReason::DiscoveryIncomplete)
    );
    assert!(
        m.runs.runs[R].children.is_empty(),
        "异常收尾也不把候选写成受管children"
    );
}

#[test]
fn lineage_safety_真实超过操作容量后旧运行归属仍可识别() {
    const NEXT: &str = "容量淘汰后新运行";
    const NEXT_TURN: &str = "容量淘汰后父轮";
    let (mut m, mut mock) = setup(false);
    let mut actions = Vec::new();
    m.run_opened(
        crate::runs::RunOpen {
            thread_id: P.into(),
            run_id: R.into(),
            conn: Some(1),
            kind: crate::claim::ReqKind::TurnStart,
            joined: false,
            input: "旧运行".into(),
            continued_from: None,
        },
        &mut actions,
    );
    spawn_v1(&mut m, &mut mock, C, "旧派生", "旧子轮");
    let old_operation = m
        .relations
        .operations
        .keys()
        .find(|key| key.item_id == "旧派生")
        .unwrap()
        .clone();
    let child_done = mock.turn_completed(C, "旧子轮", "completed");
    mock.inject(&mut m, child_done);
    let parent_done = mock.turn_completed(P, T, "completed");
    let mut replies: VecDeque<_> = mock.inject(&mut m, parent_done).into_iter().collect();
    let mut reads = 0;
    while let Some(action) = replies.pop_front() {
        let Action::Request(context) = action else {
            continue;
        };
        let crate::claim::Operation::ResultRead(read) = &context.operation else {
            continue;
        };
        reads += 1;
        assert!(reads <= 4, "旧运行结果补读必须有界");
        let response = json!({"data": [{"turnId": read.turn_id, "item": {
            "id": "旧运行结果", "type": "agentMessage", "text": "旧任务完成", "phase": "final_answer"
        }}], "nextCursor": null});
        replies.extend(mock.inject(
            &mut m,
            Event::Reply {
                context,
                result: Ok(response),
            },
        ));
    }
    assert_eq!(m.runs.runs[R].state, crate::runs::RunState::Ended);
    assert_eq!(m.runs.runs[R].children.len(), 1);
    m.clock.mono_ms += 1;
    m.clock.now_ms += 1;
    m.claims
        .insert(P.into(), claim(Owner::Client(1), NEXT_TURN, NEXT, 2));
    m.run_opened(
        crate::runs::RunOpen {
            thread_id: P.into(),
            run_id: NEXT.into(),
            conn: Some(1),
            kind: crate::claim::ReqKind::TurnStart,
            joined: false,
            input: "新运行".into(),
            continued_from: None,
        },
        &mut actions,
    );
    m.ensure_context(NEXT);
    assert!(
        !m.runs.runs.contains_key(R),
        "run_opened真实淘汰旧完整运行：runs={:?} notices={:?} dirty={:?}",
        m.runs.runs,
        m.runs.notices,
        m.runs.diagnostic_dirty
    );
    assert!(
        m.relations.operations.contains_key(&old_operation),
        "操作缓存尚有旧记录"
    );
    for index in 0..MAX_OPERATIONS {
        m.clock.mono_ms += 1;
        let (started, failed) =
            SubagentMock::v1_spawn(P, NEXT_TURN, &format!("容量操作{index}"), "未创建", true);
        run(&mut m, &mut mock, vec![started, failed]);
    }
    assert_eq!(m.relations.operations.len(), MAX_OPERATIONS);
    assert!(
        !m.relations.operations.contains_key(&old_operation),
        "reserve_operation真实淘汰最旧记录"
    );
    assert!(m.relations.bindings.values().all(|b| b.run_id != R));
    assert!(
        m.historical_subagent_for_run(NEXT, C),
        "历史判断不依赖已淘汰操作或完整RunRecord"
    );
    assert!(!m.current_management_evidence(NEXT, C));
    assert!(!m.manage_context(NEXT).unwrap().members.contains_key(C));
}

#[test]
fn lineage_safety_历史线程合法复用绑定新轮且只汇总本次结果() {
    let (mut m, mut mock) = machine();
    history(&mut m, C);
    candidate(&mut m, &mut mock, C);
    discover(
        &mut m,
        &mut mock,
        &[(
            C,
            "idle",
            json!([{ "id": "历史轮", "status": "completed" }]),
            false,
        )],
    );
    assert!(m.runs.index.child_history.has_other_run(C, R));
    assert!(m.historical_subagent_for_run(R, C));
    assert!(!m.run_has_candidate(R, C));
    assert!(m.convergences.ledgers[R].activities.is_empty());
    let (started, completed) = SubagentMock::v1_message("sendInput", P, T, "合法复用", C, false);
    run(&mut m, &mut mock, vec![started, completed]);
    let next = mock.turn_started(C, "复用新轮");
    mock.inject(&mut m, next);
    assert!(
        m.runs.index.child_history.has_other_run(C, R),
        "历史事实仍保留"
    );
    assert!(
        m.current_management_evidence(R, C),
        "本次合法复用优先于历史归属"
    );
    assert!(!m.historical_subagent_for_run(R, C));
    assert_eq!(bound(&m, C, "复用新轮").as_deref(), Some(R));
    assert!(m.binding(C, "历史轮").is_none());
    let ended = mock.turn_completed(C, "复用新轮", "completed");
    mock.inject(&mut m, ended);
    m.convergence_sync_facts(R);
    let ledger = &m.convergences.ledgers[R];
    assert_eq!(ledger.activities.len(), 1);
    assert_eq!(ledger.results.len(), 1);
    assert_eq!(
        ledger.results[0].locator.turn_id.as_deref(),
        Some("复用新轮")
    );
    assert_eq!(
        m.runs.runs[R].children[0].turn_ids,
        vec!["复用新轮".to_owned()]
    );
}

#[test]
fn lineage_safety_byte_budget_evicted_child_still_blocks_without_management() {
    use crate::runs::child_history::{test_budget_children, MAX_CHILD_HISTORY_BYTES};
    let (mut m, mut mock) = machine();
    m.runs.index.child_history.verify_empty_store();
    history(&mut m, C);
    assert!(m.runs.index.child_history.has_other_run(C, R));
    m.runs.index.child_history.remember(
        "较新运行",
        &"根".repeat(300),
        &test_budget_children(),
        2,
        &BTreeMap::new(),
    );
    assert!(
        !m.runs.index.child_history.has_other_run(C, R),
        "旧子证据确实被预算裁掉"
    );
    assert!(
        serde_json::to_vec(&m.runs.index.child_history)
            .unwrap()
            .len()
            <= MAX_CHILD_HISTORY_BYTES
    );
    candidate(&mut m, &mut mock, C);
    discover(
        &mut m,
        &mut mock,
        &[(
            C,
            "notLoaded",
            json!([{"id":"旧轮","status":"completed"}]),
            false,
        )],
    );
    assert!(m.run_has_candidate(R, C));
    let facts = m.convergence_facts(R).unwrap();
    assert_eq!(facts.unresolved_candidates, 1);
    assert!(facts.activities.is_empty());
    assert!(facts.result_refs.is_empty());
    assert!(facts.discovery.missing[C]
        .iter()
        .any(|r| r == "historyTrimmed"));
    assert!(!settled(&facts), "证据缺失必须阻塞收敛");
    assert!(m.binding(C, "旧轮").is_none());
    assert!(!m.current_management_evidence(R, C));
}

#[test]
fn lineage_safety_缺失损坏裁剪都不能把未知已完成候选当历史排除() {
    for reason in ["historyMissing", "historyCorrupt", "historyTrimmed"] {
        let (mut m, mut mock) = machine();
        match reason {
            "historyCorrupt" => m.runs.index.child_history.mark_corrupt(),
            "historyTrimmed" => m.runs.index.child_history.mark_trimmed(),
            _ => {}
        }
        candidate(&mut m, &mut mock, C);
        discover(
            &mut m,
            &mut mock,
            &[(
                C,
                "notLoaded",
                json!([{"id":"旧轮","status":"completed"}]),
                false,
            )],
        );
        assert!(m.run_has_candidate(R, C), "{reason}");
        let facts = m.convergence_facts(R).unwrap();
        assert_eq!(facts.unresolved_candidates, 1);
        assert!(facts.activities.is_empty());
        assert!(facts.result_refs.is_empty());
        assert!(facts.discovery.missing[C].iter().any(|r| r == reason));
        assert!(!settled(&facts));
    }
}

#[test]
fn lineage_safety_独立认领候选产生归属冲突而不获取管理权() {
    let (mut m, mut mock) = machine();
    candidate(&mut m, &mut mock, C);
    m.claims
        .insert(C.into(), claim(Owner::Client(2), "独立轮", "独立运行", 9));
    discover(
        &mut m,
        &mut mock,
        &[(
            C,
            "active",
            json!([{"id":"独立轮","status":"inProgress"}]),
            false,
        )],
    );
    let facts = m.convergence_facts(R).unwrap();
    assert!(facts
        .conflicts
        .iter()
        .any(|r| r.contains("independentClaim")));
    assert!(!settled(&facts));
    assert!(facts.activities.is_empty());
    assert!(m.binding(C, "独立轮").is_none());
    assert!(!m.current_management_evidence(R, C));
}

#[test]
fn lineage_safety_历史核实完成后活动通知立即撤证且发布前复核() {
    for status in ["active", "pendingInit"] {
        const OTHER: &str = "扣包的另一历史子";
        let (mut m, mut mock) = machine();
        for thread in [C, OTHER] {
            history(&mut m, thread);
            candidate(&mut m, &mut mock, thread);
        }
        let (op, held) = discover_with_hold(
            &mut m,
            &mut mock,
            &[
                (
                    C,
                    "notLoaded",
                    json!([{"id":"历史轮","status":"completed"}]),
                    false,
                ),
                (OTHER, "notLoaded", json!([]), false),
            ],
            Some(OTHER),
        );
        assert!(!held.is_empty(), "其他候选核实回包实际在途");
        assert_eq!(m.classify_candidate(R, C), Membership::Historical);
        assert!(m.relations.aux.work[&op]
            .checks_done
            .contains(&(C.into(), "thread/read".into())));
        assert!(m.relations.aux.work[&op]
            .checks_done
            .contains(&(C.into(), "thread/goal/get".into())));
        assert!(m.discovery_outcome(op).is_none());
        mock.inject(
            &mut m,
            Event::Notification {
                method: "thread/status/changed".into(),
                params: Some(json!({"threadId":C,"status":{"type":status}})),
            },
        );
        assert!(
            m.run_has_candidate(R, C),
            "活动通知必须立即撤销历史排除：{status}"
        );
        assert!(
            !m.relations
                .aux
                .candidate_observations
                .get(C)
                .is_some_and(|o| o.inactive),
            "活动通知不能保留旧静止证明：{status}"
        );
        assert!(m.convergences.ledgers[R].covered_epoch.is_none());
        let mut pending: VecDeque<_> = held.into_iter().collect();
        while let Some(context) = pending.pop_front() {
            let response = stale_check_reply(&mock, &context);
            let next = mock.inject(
                &mut m,
                Event::Reply {
                    context,
                    result: Ok(response),
                },
            );
            pending.extend(next.into_iter().filter_map(|action| match action {
                Action::Request(next) if next.method == "thread/turns/list" => Some(next),
                _ => None,
            }));
        }
        let result = m.discovery_outcome(op).expect("其他回包放行后发现已结束");
        assert!(
            result
                .state
                .incomplete_reasons
                .iter()
                .any(|r| r == "candidateChanged"),
            "发布必须复核已完成候选版本：{status}"
        );
        assert!(
            m.run_has_candidate(R, C),
            "活动候选不能被最后发布排除：{status}"
        );
        assert!(!settled(&m.convergence_facts(R).unwrap()));
        let done = mock.turn_completed(P, T, "completed");
        mock.inject(&mut m, done);
        assert!(
            m.claims.contains_key(P),
            "候选重新活动后父轮完成也不得释放：{status}"
        );
    }
}

#[test]
fn lineage_safety_历史无轮次请求插入及结束立即刷新分类() {
    let (mut m, mut mock) = machine();
    history(&mut m, C);
    candidate(&mut m, &mut mock, C);
    discover(&mut m, &mut mock, &[(C, "notLoaded", json!([]), false)]);
    assert_eq!(m.classify_candidate(R, C), Membership::Historical);
    let (_, request) = mock.request(C, None, Some(json!(906)), APPROVAL);
    let routed = mock.inject(&mut m, request);
    assert!(
        m.run_has_candidate(R, C),
        "无轮次 Lookup 插入必须立即刷新历史分类"
    );
    assert!(m.convergences.ledgers[R].covered_epoch.is_none());
    assert!(
        m.relations.aux.lookups.is_empty(),
        "已知血缘不需要额外回查任务"
    );
    let pending = m
        .relations
        .requests
        .values()
        .find(|r| r.upstream_request_id == json!(906))
        .unwrap();
    assert_eq!(pending.stage, ChildRequestStage::Lookup);
    assert!(pending.run_id.is_none());
    assert!(
        m.convergence_facts(R).unwrap().pending_requests > 0,
        "收敛事实直接考虑可能属于本运行的 Lookup 请求"
    );
    assert!(forwarded(&routed, 1).is_empty());
    assert!(
        !routed
            .iter()
            .any(|a| matches!(a, Action::ServerResult { .. } | Action::ServerError { .. })),
        "候选无管理权，不越权作答"
    );
    mock.inject(
        &mut m,
        Event::Notification {
            method: "serverRequest/resolved".into(),
            params: Some(json!({"threadId": C, "requestId":906})),
        },
    );
    assert!(
        !m.run_has_candidate(R, C),
        "无轮次请求结束必须撤销待答候选分类"
    );
    assert_eq!(m.convergence_facts(R).unwrap().pending_requests, 0);
}

#[test]
fn lineage_safety_合法交接保留未绑定资格且首轮晚到仍正确绑定() {
    for kind in [HandoffKind::Detach, HandoffKind::OwnerGone] {
        let (mut m, mut mock) = machine();
        history(&mut m, C);
        mock.register(C, Some(P), "v1", false, true);
        let (started, completed) = SubagentMock::v1_spawn(P, T, "交接前派生", C, false);
        run(&mut m, &mut mock, vec![started, completed]);
        assert!(m.relations.bindings.is_empty());
        let before = m.manage_context(R).unwrap().gen;
        if kind == HandoffKind::Detach {
            // 经真实 detach 入口同步管理代际与协调器令牌。
            mock.inject(
                &mut m,
                Event::ClientRequest {
                    request: crate::claim::ClientRequest {
                        conn: 1,
                        id: json!(908),
                        method: "broker/threads/detach".into(),
                        params: Some(json!({"threadId":P,"runId":R,"policy":"on-request"})),
                    },
                    run_id: None,
                },
            );
        } else {
            // OwnerGone 的生产后续进入停止；这里仅核实关系交接仍保留首轮资格。
            m.on_handoff(R, kind, &mut vec![]);
        }
        assert_eq!(m.manage_context(R).unwrap().gen, before + 1);
        assert!(
            m.current_management_evidence(R, C),
            "合法交接不得丢失未绑定资格：{kind:?}"
        );
        assert!(m.manage_context(R).unwrap().members.contains_key(C));
        assert!(!m.run_has_candidate(R, C));
        if kind == HandoffKind::Detach {
            assert!(!settled(&m.convergence_facts(R).unwrap()));
        }
        let late = mock.turn_started(C, "交接后晚到首轮");
        mock.inject(&mut m, late);
        assert_eq!(bound(&m, C, "交接后晚到首轮").as_deref(), Some(R));
        assert!(m.current_management_evidence(R, C));
        let done = mock.turn_completed(C, "交接后晚到首轮", "completed");
        mock.inject(&mut m, done);
        assert!(m.convergences.ledgers[R]
            .results
            .iter()
            .any(|r| r.locator.turn_id.as_deref() == Some("交接后晚到首轮")));
    }
}
