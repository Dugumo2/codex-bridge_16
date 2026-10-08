//! 事件矩阵、首轮配对与绑定、乱序、过期、复用、继承、冲突、settling 定案、代际迁移、持久化。
use super::*;

#[test]
fn v1派生接收方到completed才出现_子线程首轮绑定到父运行() {
    let (mut m, mut mock) = setup(false);
    mock.register(C, Some(P), "v1", false, true);
    let (started, completed) = SubagentMock::v1_spawn(P, T, "派生1", C, false);
    mock.inject(&mut m, started);
    // started 不带接收方：只登记在途派生，没有成员、没有边。
    let context = m.manage_context(R).expect("上下文");
    assert_eq!(context.in_flight_spawns.len(), 1);
    assert!(context.members.is_empty());
    assert!(m.thread_relation(C).is_none());
    assert_eq!(m.has_managed_activity(R), Activity::Some);
    mock.inject(&mut m, completed);
    let edge = m.thread_relation(C).expect("completed 才给出 C");
    assert_eq!(edge.parent_thread_id, P);
    assert_eq!(edge.agent_version, AgentVersion::V1);
    assert!(m.manage_context(R).unwrap().in_flight_spawns.is_empty());
    assert_eq!(
        m.manage_context(R).unwrap().members[C].activity,
        MemberActivity::Starting
    );
    let first = mock.turn_started(C, "子轮1");
    mock.inject(&mut m, first);
    let binding = m.binding(C, "子轮1").expect("首轮绑定");
    assert_eq!(binding.status, BindingStatus::Bound);
    assert_eq!(binding.run_id, R);
    assert_eq!(binding.evidence, BindingEvidence::V1Spawn);
    assert_eq!(
        binding.operation,
        EvidenceKey {
            parent_thread_id: P.into(),
            parent_turn_id: T.into(),
            item_id: "派生1".into()
        }
    );
    assert_eq!(
        m.manage_context(R).unwrap().members[C].activity,
        MemberActivity::Running
    );
    assert_eq!(m.has_managed_activity(R), Activity::Some);
    let done = mock.turn_completed(C, "子轮1", "completed");
    mock.inject(&mut m, done);
    assert_eq!(
        m.manage_context(R).unwrap().members[C].activity,
        MemberActivity::Settled
    );
    assert_eq!(m.has_managed_activity(R), Activity::None);
}

#[test]
fn v1子线程首轮先于派生completed到达时回填() {
    let (mut m, mut mock) = setup(false);
    mock.register(C, Some(P), "v1", false, true);
    let (started, completed) = SubagentMock::v1_spawn(P, T, "派生1", C, false);
    let first = mock.turn_started(C, "子轮1");
    run(&mut m, &mut mock, vec![started, first]);
    assert!(m.binding(C, "子轮1").is_none(), "接收方未出现前不能绑定");
    mock.inject(&mut m, completed);
    assert_eq!(bound(&m, C, "子轮1").as_deref(), Some(R));
}

#[test]
fn v1发送输入与恢复不占下一轮_已绑定轮次不因steer改归属() {
    for tool in ["sendInput", "resumeAgent"] {
        let (mut m, mut mock) = setup(false);
        spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
        assert_eq!(bound(&m, C, "子轮1").as_deref(), Some(R));
        // steer 进当前轮：接收方已在 started 出现，但不改变已有绑定。
        let (s, c) = SubagentMock::v1_message(tool, P, T, "消息1", C, false);
        run(&mut m, &mut mock, vec![s, c]);
        assert_eq!(bound(&m, C, "子轮1").as_deref(), Some(R));
        let done = mock.turn_completed(C, "子轮1", "completed");
        mock.inject(&mut m, done);
        // 之后子线程新开一轮（无论是 sendInput 触发还是别的来源），都没有轮次因果证据。
        let (s, c) = SubagentMock::v1_message(tool, P, T, "消息2", C, false);
        let next = mock.turn_started(C, "子轮2");
        run(&mut m, &mut mock, vec![s, next, c]);
        assert_eq!(bound(&m, C, "子轮2"), None, "{tool} 不能预约下一轮");
        assert_eq!(
            m.manage_context(R).unwrap().members[C].activity,
            MemberActivity::Unknown,
            "成员上有未归属的活动轮次"
        );
        assert_eq!(m.has_managed_activity(R), Activity::Unknown);
    }
}

#[test]
fn v2双通知只登记一次_started已带接收方_interacted不产生绑定() {
    let (mut m, mut mock) = setup(false);
    mock.register(C, Some(P), "v2", false, true);
    let (s, c) = SubagentMock::v2_activity("started", P, T, "活动1", C);
    mock.inject(&mut m, s.clone());
    assert_eq!(
        m.thread_relation(C).unwrap().agent_version,
        AgentVersion::V2
    );
    let revision = m.manage_context(R).unwrap().revision;
    // 同一条目的第二条通知（completed）与重复的 started 都不再改变任何事实。
    mock.inject(&mut m, c);
    mock.inject(&mut m, s);
    assert_eq!(m.manage_context(R).unwrap().revision, revision);
    assert_eq!(m.relations.operations.len(), 1);
    let op = m.relations.operations.values().next().unwrap();
    assert!(op.started_seen && op.completed_seen);
    let first = mock.turn_started(C, "子轮1");
    mock.inject(&mut m, first);
    let binding = m.binding(C, "子轮1").unwrap();
    assert_eq!(binding.evidence, BindingEvidence::V2Started);
    assert_eq!(binding.status, BindingStatus::Bound);
    let done = mock.turn_completed(C, "子轮1", "completed");
    mock.inject(&mut m, done);
    // v2 interacted：只记事实，不授予下一轮。
    let (s, c) = SubagentMock::v2_activity("interacted", P, T, "活动2", C);
    let next = mock.turn_started(C, "子轮2");
    run(&mut m, &mut mock, vec![s, c, next]);
    assert_eq!(bound(&m, C, "子轮2"), None);
}

#[test]
fn completed先到时迟到started不回退也不增加在途派生() {
    let (mut m, mut mock) = setup(false);
    let (started, completed) = SubagentMock::v1_spawn(P, T, "派生1", C, false);
    mock.inject(&mut m, completed);
    mock.inject(&mut m, started);
    assert!(m.manage_context(R).unwrap().in_flight_spawns.is_empty());
    let op = m.relations.operations.values().next().unwrap();
    assert!(op.completed_seen && op.started_seen);
    assert!(op.receivers.contains(C));
}

#[test]
fn 派生失败撤销操作且不产生成员() {
    let (mut m, mut mock) = setup(false);
    let (started, failed) = SubagentMock::v1_spawn(P, T, "派生1", C, true);
    run(&mut m, &mut mock, vec![started, failed]);
    let op = m.relations.operations.values().next().unwrap();
    assert_eq!(op.outcome, EvidenceOutcome::Failed);
    assert!(op.failure.is_some());
    assert!(m.manage_context(R).unwrap().in_flight_spawns.is_empty());
    assert!(m.manage_context(R).unwrap().members.is_empty());
    assert_eq!(m.has_managed_activity(R), Activity::None);
    // 失败后才出现的同名线程首轮不能借这次派生绑定。
    let first = mock.turn_started(C, "子轮1");
    mock.inject(&mut m, first);
    assert!(m.binding(C, "子轮1").is_none());
}

#[test]
fn 证据过期或父轮结束后不再授予绑定() {
    // 过期：首次观察起 30 秒。
    let (mut m, mut mock) = setup(false);
    let (s, c) = SubagentMock::v1_spawn(P, T, "派生1", C, false);
    run(&mut m, &mut mock, vec![s, c]);
    assert_eq!(m.has_managed_activity(R), Activity::Some);
    advance(&mut m, &mut mock, EVIDENCE_TTL_MS);
    let first = mock.turn_started(C, "子轮1");
    mock.inject(&mut m, first);
    assert_eq!(bound(&m, C, "子轮1"), None);
    assert_eq!(
        m.binding(C, "子轮1").map(|b| b.status),
        None,
        "过期证据只保留诊断，不会把后来的轮次记成本运行"
    );
    // 父轮结束：未配对候选转 unbound。
    let (mut m, mut mock) = setup(false);
    let (s, c) = SubagentMock::v1_spawn(P, T, "派生1", C, false);
    run(&mut m, &mut mock, vec![s, c]);
    let parent_done = mock.turn_completed(P, T, "completed");
    mock.inject(&mut m, parent_done);
    let first = mock.turn_started(C, "子轮1");
    mock.inject(&mut m, first);
    assert_eq!(bound(&m, C, "子轮1"), None);
}

#[test]
fn 首轮不唯一时保持unbound() {
    let (mut m, mut mock) = setup(false);
    let (s, c) = SubagentMock::v1_spawn(P, T, "派生1", C, false);
    let one = mock.turn_started(C, "子轮1");
    let two = mock.turn_started(C, "子轮2");
    run(&mut m, &mut mock, vec![s, one, two, c]);
    assert_eq!(bound(&m, C, "子轮1"), None);
    assert_eq!(bound(&m, C, "子轮2"), None);
    assert_eq!(
        m.binding(C, "子轮1").map(|b| b.status),
        Some(BindingStatus::Unbound)
    );
}

#[test]
fn 旧子线程在父新运行开始后才发事件不误绑_同一子代理被两个运行复用() {
    let (mut m, mut mock) = setup(false);
    spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    assert_eq!(bound(&m, C, "子轮1").as_deref(), Some(R));
    // 子代理先完成（否则 SA-b 的自然结束门禁会暂缓释放并停树）。
    let child_done = mock.turn_completed(C, "子轮1", "completed");
    mock.inject(&mut m, child_done);
    // R 结束：父认领释放，运行上下文撤权。
    let parent_done = mock.turn_completed(P, T, "completed");
    mock.inject(&mut m, parent_done);
    assert!(!m.claims.contains_key(P));
    assert_eq!(m.manage_context(R).unwrap().authority, Authority::Revoked);
    // 新运行 R2 在 P 上开始。
    m.claims
        .insert(P.into(), claim(Owner::Client(1), "父轮2", "运行乙", 2));
    // 旧子线程迟到的新轮：不能归 R2，也不能再归已撤权的 R。
    let late = mock.turn_started(C, "子轮9");
    mock.inject(&mut m, late);
    assert_eq!(bound(&m, C, "子轮9"), None);
    // R2 用 sendInput 复用同一子代理：仍然没有轮次证据。
    let (s, c) = SubagentMock::v1_message("sendInput", P, "父轮2", "消息1", C, false);
    let reuse = mock.turn_started(C, "子轮10");
    run(&mut m, &mut mock, vec![s, reuse, c]);
    assert_eq!(bound(&m, C, "子轮10"), None);
    assert_eq!(bound(&m, C, "子轮1").as_deref(), Some(R), "历史绑定保留");
}

#[test]
fn 已绑定子线程派生的孙线程继承运行并保留直接父边() {
    let (mut m, mut mock) = setup(false);
    spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    mock.register("孙线程", Some(C), "v1", false, true);
    let (s, c) = SubagentMock::v1_spawn(C, "子轮1", "派生2", "孙线程", false);
    let first = mock.turn_started("孙线程", "孙轮1");
    run(&mut m, &mut mock, vec![s, c, first]);
    let binding = m.binding("孙线程", "孙轮1").expect("继承绑定");
    assert_eq!(binding.status, BindingStatus::Bound);
    assert_eq!(binding.run_id, R);
    assert_eq!(binding.evidence, BindingEvidence::Inherited);
    assert_eq!(binding.parent_thread_id, C);
    assert_eq!(m.thread_relation("孙线程").unwrap().parent_thread_id, C);
    assert_eq!(m.manage_context(R).unwrap().members["孙线程"].parent, C);
}

#[test]
fn 孙线程派生先于子线程绑定到达时随后回填() {
    let (mut m, mut mock) = setup(false);
    // C 的首轮先跑起来并派生 G，P 的派生 completed 最后才到。
    let (ps, pc) = SubagentMock::v1_spawn(P, T, "派生1", C, false);
    let c_first = mock.turn_started(C, "子轮1");
    let (gs, gc) = SubagentMock::v1_spawn(C, "子轮1", "派生2", "孙线程", false);
    let g_first = mock.turn_started("孙线程", "孙轮1");
    run(&mut m, &mut mock, vec![ps, c_first, gs, gc, g_first]);
    assert_eq!(bound(&m, "孙线程", "孙轮1"), None);
    mock.inject(&mut m, pc);
    assert_eq!(bound(&m, C, "子轮1").as_deref(), Some(R));
    assert_eq!(bound(&m, "孙线程", "孙轮1").as_deref(), Some(R));
}

#[test]
fn 独立认领或已归属其他运行构成冲突且不覆盖() {
    // 子线程有自己的 cxb 认领。
    let (mut m, mut mock) = setup(false);
    m.claims
        .insert(C.into(), claim(Owner::Client(2), "子轮1", "独立运行", 7));
    let (s, c) = SubagentMock::v1_spawn(P, T, "派生1", C, false);
    run(&mut m, &mut mock, vec![s, c]);
    // 有认领的线程轮次不经无认领观察；用请求观察也不改变：首轮由认领自己管理。
    assert_eq!(bound(&m, C, "子轮1"), None);
    // 已归属其他运行：同一 (线程, 轮次) 不覆盖，记冲突。
    let (mut m, mut mock) = setup(false);
    spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    m.claims.insert(
        "另一父".into(),
        claim(Owner::Client(1), "另一轮", "运行乙", 9),
    );
    let (s, c) = SubagentMock::v1_spawn("另一父", "另一轮", "派生9", C, false);
    run(&mut m, &mut mock, vec![s, c]);
    // C 已观察到一轮（子轮1）：唯一首轮配对给了运行乙，但该轮已归属 R → 冲突。
    assert_eq!(bound(&m, C, "子轮1").as_deref(), Some(R));
    assert!(m.manage_context("运行乙").unwrap().members[C]
        .conflict
        .as_deref()
        .is_some_and(|c| c.contains(R)));
}

#[test]
fn 父认领settling时等定案_自有才落实_joined不绑() {
    for own in [true, false] {
        let (mut m, mut mock) = setup(true);
        let claim = m.claims.get_mut(P).unwrap();
        claim.owned_turns.clear();
        claim.state = State::Starting {
            owner: Owner::Client(1),
            req: crate::claim::ReqKind::TurnStart,
            turn_id: Some(T.into()),
            cont: None,
            settling: true,
            undecided: None,
            submitted_at: 0,
            held: Vec::new(),
        };
        let (s, c) = SubagentMock::v1_spawn(P, T, "派生1", C, false);
        let first = mock.turn_started(C, "子轮1");
        run(&mut m, &mut mock, vec![s, c, first]);
        assert_eq!(bound(&m, C, "子轮1"), None, "定案前不绑定");
        // 模拟 settle 的归属更新与钩子位置。
        let claim = m.claims.get_mut(P).unwrap();
        claim.state = State::Running {
            owner: Owner::Client(1),
            turn_id: T.into(),
            joined: !own,
        };
        if own {
            claim.owned_turns.insert(T.into());
        }
        let mut actions = Vec::new();
        m.on_claim_settled(P, 1, own, &mut actions);
        if own {
            assert_eq!(bound(&m, C, "子轮1").as_deref(), Some(R));
        } else {
            assert_eq!(bound(&m, C, "子轮1"), None);
            assert_eq!(
                m.relations.operations.values().next().unwrap().outcome,
                EvidenceOutcome::Unproven
            );
        }
    }
}

#[test]
fn 代际迁移_detach断开joined外部接管() {
    use crate::policy::Policy;
    for kind in [
        HandoffKind::Detach,
        HandoffKind::OwnerGone,
        HandoffKind::Joined,
        HandoffKind::External,
    ] {
        let (mut m, mut mock) = setup(false);
        spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
        let gen = m.manage_context(R).unwrap().gen;
        if kind == HandoffKind::Detach {
            if let State::Running { owner, .. } = &mut m.claims.get_mut(P).unwrap().state {
                *owner = Owner::Broker {
                    policy: Policy::Never,
                    approvals_reviewer: None,
                    owned_at: 0,
                    last_watcher_at: 0,
                };
            }
        }
        let mut actions = Vec::new();
        m.on_handoff(R, kind, &mut actions);
        let context = m.manage_context(R).unwrap();
        assert_eq!(context.gen, gen + 1, "{kind:?}");
        match kind {
            HandoffKind::Detach => assert_eq!(
                context.authority,
                Authority::Broker {
                    policy: Policy::Never
                }
            ),
            // 断开前（运行登记）的策略快照缺失时按需人工，不能默认为自动批准。
            HandoffKind::OwnerGone => assert_eq!(
                context.authority,
                Authority::Broker {
                    policy: Policy::OnRequest
                }
            ),
            _ => assert_eq!(context.authority, Authority::Revoked),
        }
        // 绑定挂在 run_id 上，不随代际改变。
        assert_eq!(bound(&m, C, "子轮1").as_deref(), Some(R));
    }
}

#[test]
fn 线程索引与运行记录持久化_重启只恢复关系不恢复绑定() {
    let (mut m, mut mock) = setup(false);
    let mut actions = Vec::new();
    m.run_opened(
        crate::runs::RunOpen {
            thread_id: P.into(),
            run_id: R.into(),
            conn: Some(1),
            kind: crate::claim::ReqKind::TurnStart,
            joined: false,
            input: "任务".into(),
            continued_from: None,
        },
        &mut actions,
    );
    let actions = spawn_v1(&mut m, &mut mock, C, "派生1", "子轮1");
    // threads.json：子线程登记父线程、关系类型、版本。
    let index = actions
        .iter()
        .rev()
        .find_map(|a| match a {
            Action::Store(crate::store::StoreOp::PutThreads(index)) => Some(index.clone()),
            _ => None,
        })
        .expect("写线程索引");
    let entry = index.threads.values().find(|e| e.thread_id == C).unwrap();
    assert_eq!(entry.parent_thread_id.as_deref(), Some(P));
    assert_eq!(entry.relation_kind, Some(RelationKind::Subagent));
    assert_eq!(entry.agent_version, Some(AgentVersion::V1));
    // RunRecord.children：子线程、已绑定轮次与证据。
    let record = &m.runs.runs[R];
    assert_eq!(record.children.len(), 1);
    assert_eq!(record.children[0].thread_id, C);
    assert_eq!(record.children[0].turn_ids, vec!["子轮1".to_owned()]);
    assert_eq!(record.children[0].evidence, BindingEvidence::V1Spawn);
    assert!(actions.iter().any(
        |a| matches!(a, Action::Store(crate::store::StoreOp::PutRun(r)) if !r.children.is_empty())
    ));
    // 序列化：新字段 camelCase，旧文件缺字段可读。
    let text = serde_json::to_value(entry).unwrap();
    assert_eq!(text["parentThreadId"], P);
    assert_eq!(text["relationKind"], "subagent");
    // 重启：新状态机只从索引恢复关系历史，没有绑定、没有上下文与回答权。
    let mut fresh = Machine::new(false);
    fresh.runs.index = index;
    assert_eq!(fresh.thread_relation(C).unwrap().parent_thread_id, P);
    assert!(fresh.binding(C, "子轮1").is_none());
    assert!(fresh.manage_context(R).is_none());
}
