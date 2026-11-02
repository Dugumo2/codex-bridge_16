//! 发现：分页、已加载补读、完整性条件、版本变化重查、上限、附着证据、期限与迟到回包。
use super::*;
use crate::claim::RequestContext;

fn caller() -> DiscoveryCaller {
    DiscoveryCaller::TreeStop {
        tree_op: 1,
        round: 1,
    }
}

fn begin(m: &mut Machine) -> (u64, Vec<Action>) {
    let mut actions = Vec::new();
    let op = m.begin_discovery(
        P,
        DiscoveryScope::Run { run_id: R.into() },
        caller(),
        &mut actions,
    );
    (op, actions)
}

fn find(actions: &[Action], method: &str, archived: Option<bool>) -> RequestContext {
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

fn thread(id: &str, parent: &str) -> Value {
    json!({"id": id, "parentThreadId": parent,
        "source": {"subAgent": {"thread_spawn": {"parent_thread_id": parent, "depth": 1, "agent_path": null}}},
        "canAcceptDirectInput": true})
}

fn reply(
    m: &mut Machine,
    mock: &mut SubagentMock,
    context: RequestContext,
    result: Result<Value, Value>,
) -> Vec<Action> {
    mock.inject(m, Event::Reply { context, result })
}

fn observe(m: &mut Machine, mock: &mut SubagentMock, thread: &str) {
    mock.inject(
        m,
        Event::Notification {
            method: "thread/status/changed".into(),
            params: Some(json!({"threadId": thread, "status": {"type": "idle"}})),
        },
    );
}

/// 三个来源各一页即完；返回最后一步的动作。
fn finish_simple(
    m: &mut Machine,
    mock: &mut SubagentMock,
    actions: &[Action],
    unarchived: Vec<Value>,
    loaded: Vec<&str>,
) -> Vec<Action> {
    let u = find(actions, "thread/list", Some(false));
    let a = find(actions, "thread/list", Some(true));
    let l = find(actions, "thread/loaded/list", None);
    let mut out = reply(
        m,
        mock,
        u,
        Ok(json!({"data": unarchived, "nextCursor": null})),
    );
    out.extend(reply(
        m,
        mock,
        a,
        Ok(json!({"data": [], "nextCursor": null})),
    ));
    out.extend(reply(
        m,
        mock,
        l,
        Ok(json!({"data": loaded, "nextCursor": null})),
    ));
    out
}

#[test]
fn 分页与已加载补读合并成员_附着齐全为完整() {
    let (mut m, mut mock) = setup(false);
    m.ensure_context(R);
    observe(&mut m, &mut mock, C);
    observe(&mut m, &mut mock, "已加载");
    let (op, actions) = begin(&mut m);
    assert!(op > 0, "正式编号非零");
    // 未归档分页参数：ancestorThreadId、不加 cwd、显式 sourceKinds。
    let u = find(&actions, "thread/list", Some(false));
    let params = u.params.clone().unwrap();
    assert_eq!(params["ancestorThreadId"], P);
    assert!(params.get("cwd").is_none());
    assert_eq!(
        params["sourceKinds"],
        json!(["subAgentThreadSpawn", "subAgent", "subAgentOther"])
    );
    let next = reply(
        &mut m,
        &mut mock,
        u,
        Ok(json!({"data": [thread(C, P)], "nextCursor": "游标1"})),
    );
    let page2 = find(&next, "thread/list", Some(false));
    assert_eq!(page2.params.as_ref().unwrap()["cursor"], "游标1");
    reply(
        &mut m,
        &mut mock,
        page2,
        Ok(json!({"data": [thread("孙", C)], "nextCursor": null})),
    );
    let a = find(&actions, "thread/list", Some(true));
    reply(
        &mut m,
        &mut mock,
        a,
        Ok(json!({"data": [], "nextCursor": null})),
    );
    // 已加载但未持久化的成员：定向 thread/read 补齐血缘。
    let l = find(&actions, "thread/loaded/list", None);
    let reads = reply(
        &mut m,
        &mut mock,
        l,
        Ok(json!({"data": [P, C, "已加载", "无关"], "nextCursor": null})),
    );
    let read_targets: Vec<String> = app_requests(&reads, "thread/read")
        .iter()
        .map(|c| {
            c.params.as_ref().unwrap()["threadId"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(read_targets, vec!["已加载".to_owned(), "无关".to_owned()]);
    assert!(m.discovery_outcome(op).is_none());
    let r1 = app_requests(&reads, "thread/read")[0].clone();
    let r2 = app_requests(&reads, "thread/read")[1].clone();
    reply(
        &mut m,
        &mut mock,
        r1,
        Ok(json!({"thread": thread("已加载", P)})),
    );
    reply(
        &mut m,
        &mut mock,
        r2,
        Ok(json!({"thread": {"id": "无关", "parentThreadId": null, "source": "vscode"}})),
    );
    let result = m.discovery_outcome(op).expect("发现完成").clone();
    assert!(
        result.state.complete,
        "{:?}",
        result.state.incomplete_reasons
    );
    let mut expected = vec!["孙".to_owned(), "已加载".to_owned(), C.to_owned()];
    expected.sort();
    assert_eq!(result.members.keys().cloned().collect::<Vec<_>>(), expected);
    assert!(result.attachments.contains_key(C));
    let context = m.manage_context(R).unwrap();
    assert!(!context.discovery.complete, "血缘发现完成不证明候选归属");
    assert!(!context.members.contains_key("孙"));
    assert!(m.run_has_candidate(R, "孙"));
    assert_eq!(m.thread_relation("孙").unwrap().parent_thread_id, C);
}

#[test]
fn 分页失败重复游标页数超限判不完整并保留已知成员() {
    // 分页失败。
    let (mut m, mut mock) = setup(false);
    let (op, actions) = begin(&mut m);
    let u = find(&actions, "thread/list", Some(false));
    let next = reply(
        &mut m,
        &mut mock,
        u,
        Ok(json!({"data": [thread(C, P)], "nextCursor": "游标1"})),
    );
    let page2 = find(&next, "thread/list", Some(false));
    reply(
        &mut m,
        &mut mock,
        page2,
        Err(json!({"code": -32603, "message": "分页失败"})),
    );
    finish_rest(&mut m, &mut mock, &actions);
    let result = m.discovery_outcome(op).unwrap();
    assert!(!result.state.complete);
    assert!(result
        .state
        .incomplete_reasons
        .iter()
        .any(|r| r.starts_with("pageError")));
    assert!(result.members.contains_key(C));
    // 重复游标。
    let (mut m, mut mock) = setup(false);
    let (op, actions) = begin(&mut m);
    let u = find(&actions, "thread/list", Some(false));
    let next = reply(
        &mut m,
        &mut mock,
        u,
        Ok(json!({"data": [], "nextCursor": "同一"})),
    );
    let page2 = find(&next, "thread/list", Some(false));
    let again = reply(
        &mut m,
        &mut mock,
        page2,
        Ok(json!({"data": [], "nextCursor": "同一"})),
    );
    assert!(app_requests(&again, "thread/list").is_empty());
    finish_rest(&mut m, &mut mock, &actions);
    assert!(m
        .discovery_outcome(op)
        .unwrap()
        .state
        .incomplete_reasons
        .iter()
        .any(|r| r.starts_with("duplicateCursor")));
    // 页数超限。
    let (mut m, mut mock) = setup(false);
    let (op, actions) = begin(&mut m);
    let mut context = find(&actions, "thread/list", Some(false));
    for page in 0..MAX_PAGES {
        let next = reply(
            &mut m,
            &mut mock,
            context.clone(),
            Ok(json!({"data": [], "nextCursor": format!("游标{page}")})),
        );
        match app_requests(&next, "thread/list").first() {
            Some(c) => context = (*c).clone(),
            None => break,
        }
    }
    finish_rest(&mut m, &mut mock, &actions);
    assert!(m
        .discovery_outcome(op)
        .unwrap()
        .state
        .incomplete_reasons
        .iter()
        .any(|r| r.starts_with("tooManyPages")));
}

fn finish_rest(m: &mut Machine, mock: &mut SubagentMock, actions: &[Action]) {
    let a = find(actions, "thread/list", Some(true));
    reply(m, mock, a, Ok(json!({"data": [], "nextCursor": null})));
    let l = find(actions, "thread/loaded/list", None);
    reply(m, mock, l, Ok(json!({"data": [], "nextCursor": null})));
}

#[test]
fn 期间关系版本变化重查一次_再变判不完整_旧代号回包丢弃() {
    let (mut m, mut mock) = setup(false);
    let (op, actions) = begin(&mut m);
    // 发现进行中出现新派生（实时关系变化）。
    let (s, c) = SubagentMock::v1_spawn(P, T, "派生1", "新子", false);
    run(&mut m, &mut mock, vec![s, c]);
    let generation = m.relations.discoveries[&op].generation;
    let rerun = finish_simple(&mut m, &mut mock, &actions, vec![], vec![]);
    assert_ne!(
        m.relations.discoveries[&op].generation, generation,
        "重查换代"
    );
    assert!(m.discovery_outcome(op).is_none());
    // 旧代号的回包迟到：丢弃。
    let stale = find(&actions, "thread/list", Some(false));
    let dropped = reply(
        &mut m,
        &mut mock,
        stale,
        Ok(json!({"data": [], "nextCursor": null})),
    );
    assert!(dropped
        .iter()
        .any(|a| matches!(a, Action::Debug(d) if d.contains("过期发现"))));
    // 重查期间再变一次：判不完整，但保留成员。
    let (s, c) = SubagentMock::v1_spawn(P, T, "派生2", "又一子", false);
    run(&mut m, &mut mock, vec![s, c]);
    finish_simple(&mut m, &mut mock, &rerun, vec![], vec![]);
    let result = m.discovery_outcome(op).expect("完成");
    assert!(result
        .state
        .incomplete_reasons
        .contains(&"versionChanged".to_owned()));
    assert!(result.members.contains_key("新子") && result.members.contains_key("又一子"));
}

#[test]
fn 节点超过256_深度超过8_未落定派生_无附着证据都判不完整() {
    let (mut m, mut mock) = setup(false);
    let (op, actions) = begin(&mut m);
    let many: Vec<Value> = (0..MAX_MEMBERS + 1)
        .map(|i| thread(&format!("子{i:03}"), P))
        .collect();
    finish_simple(&mut m, &mut mock, &actions, many, vec![]);
    let result = m.discovery_outcome(op).unwrap();
    assert_eq!(result.members.len(), MAX_MEMBERS);
    assert!(result
        .state
        .incomplete_reasons
        .contains(&"tooManyMembers".to_owned()));
    // 深度：根 0，最深 8。
    let (mut m, mut mock) = setup(false);
    let (op, actions) = begin(&mut m);
    let mut chain = Vec::new();
    let mut parent = P.to_owned();
    for depth in 1..=MAX_DEPTH + 1 {
        let id = format!("层{depth}");
        chain.push(thread(&id, &parent));
        parent = id;
    }
    finish_simple(&mut m, &mut mock, &actions, chain, vec![]);
    let result = m.discovery_outcome(op).unwrap();
    assert!(result.members.contains_key(&format!("层{MAX_DEPTH}")));
    assert!(!result.members.contains_key(&format!("层{}", MAX_DEPTH + 1)));
    assert!(result
        .state
        .incomplete_reasons
        .contains(&"depthExceeded".to_owned()));
    // 在途派生（v1 started 未 completed）。
    let (mut m, mut mock) = setup(false);
    let (s, _) = SubagentMock::v1_spawn(P, T, "派生1", C, false);
    mock.inject(&mut m, s);
    let (op, actions) = begin(&mut m);
    finish_simple(&mut m, &mut mock, &actions, vec![], vec![]);
    assert!(m
        .discovery_outcome(op)
        .unwrap()
        .state
        .incomplete_reasons
        .contains(&"unsettledSpawns".to_owned()));
    // 已加载但本连接没有附着证据（不主动 resume 补附着）。
    let (mut m, mut mock) = setup(false);
    m.ensure_context(R);
    let (op, actions) = begin(&mut m);
    let out = finish_simple(&mut m, &mut mock, &actions, vec![thread(C, P)], vec![C]);
    assert!(
        app_requests(&out, "thread/resume").is_empty(),
        "SA1a 不为监听发 resume"
    );
    let result = m.discovery_outcome(op).unwrap();
    assert!(result
        .state
        .incomplete_reasons
        .contains(&"attachmentMissing".to_owned()));
    assert!(!m.manage_context(R).unwrap().members.contains_key(C));
    assert!(
        m.run_has_candidate(R, C),
        "无归属和附着的已加载后代仍是未决候选"
    );
}

#[test]
fn 发现期限二十秒_迟到回包丢弃_关闭静默取消() {
    let (mut m, mut mock) = setup(false);
    let (op, actions) = begin(&mut m);
    let generation = m.relations.discoveries[&op].generation;
    m.clock.mono_ms += DISCOVERY_MS;
    mock.inject(
        &mut m,
        Event::Timer {
            token: Machine::relation_token(),
            kind: crate::claim::TimerKind::DiscoveryDeadline {
                op_id: op,
                generation,
            },
        },
    );
    let result = m.discovery_outcome(op).expect("到期结束").clone();
    assert!(result
        .state
        .incomplete_reasons
        .contains(&"timeout".to_owned()));
    let late = find(&actions, "thread/list", Some(false));
    reply(
        &mut m,
        &mut mock,
        late,
        Ok(json!({"data": [thread(C, P)], "nextCursor": null})),
    );
    assert_eq!(
        m.discovery_outcome(op).unwrap(),
        &result,
        "迟到回包不改结果"
    );
    // 关闭：在途发现静默取消，不产生结果。
    let (mut m, mut mock) = setup(false);
    let (op, _) = begin(&mut m);
    mock.inject(&mut m, Event::Shutdown);
    assert!(m.discovery_outcome(op).is_none());
    assert!(m.relations.aux.work.is_empty());
}

#[test]
fn 修复6_发现完成退役登记且迟到回包无效() {
    let (mut m, mut mock) = setup(false);
    for _ in 0..MAX_RESULTS + 10 {
        let (op, actions) = begin(&mut m);
        let late = find(&actions, "thread/list", Some(false));
        finish_simple(&mut m, &mut mock, &actions, vec![], vec![]);
        assert!(!m.relations.discoveries.contains_key(&op));
        let result = m.discovery_outcome(op).unwrap().clone();
        reply(
            &mut m,
            &mut mock,
            late,
            Ok(json!({"data":[thread(C,P)],"nextCursor":null})),
        );
        assert_eq!(m.discovery_outcome(op), Some(&result));
    }
    assert!(m.relations.discoveries.is_empty());
    assert!(m.relations.aux.work.is_empty());
    assert_eq!(m.relations.aux.results.len(), MAX_RESULTS);
    begin(&mut m);
    m.relations_shutdown(&mut Vec::new());
    assert!(m.relations.discoveries.is_empty());
}

#[test]
fn 二审_r1_已拒绝跟进不会经运行发现重新纳管() {
    let (mut m, mut mock) = setup(false);
    m.enable_convergence(crate::claim::converge::ConvergenceReadiness {
        trusted_inputs: true,
        budget_admission: true,
        asynchronous_results: true,
        terminal_mapping: true,
        cancellation_verified: true,
    });
    m.ensure_context(R);
    mock.register(C, Some(P), "v1", false, true);
    let event = mock.thread_started(C);
    mock.inject(&mut m, event);
    m.claims
        .insert(C.into(), claim(Owner::Client(2), "独立轮", "独立运行", 9));
    let (started, completed) = SubagentMock::v1_message("sendInput", P, T, "跟进", C, false);
    run(&mut m, &mut mock, vec![started, completed]);
    assert!(!m.manage_context(R).unwrap().members.contains_key(C));
    let (id, actions) = begin(&mut m);
    finish_simple(&mut m, &mut mock, &actions, vec![thread(C, P)], vec![]);
    let result = m.relations.aux.results.get(&id).expect("发现完成");
    assert!(
        result.members.contains_key(C),
        "拒绝纳管不能抹掉血缘清理范围"
    );
    assert!(!m.manage_context(R).unwrap().members.contains_key(C));
    assert!(m.run_has_candidate(R, C));
}
