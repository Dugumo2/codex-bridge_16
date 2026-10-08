use super::*;

fn restored_review_fixture(lifecycle_first: bool, phase: DiskPhase) -> Machine {
    let mut source = Machine::new(true);
    sent(&mut source);
    let mut file = source.lifecycle.file.clone();
    file.records.get_mut("P").unwrap().phase = phase;
    let mut run = crate::store::tests::run("R", "P", 1);
    run.review_thread_id = Some("C".into());
    let mut m = Machine::new(true);
    if lifecycle_first {
        m.lifecycle_restore(Ok(file.clone()));
    }
    m.restore_runs(Default::default(), vec![run], &mut vec![]);
    if !lifecycle_first {
        m.lifecycle_restore(Ok(file));
    }
    m
}

#[test]
fn 续修_先生命周期后运行恢复必须封住审查子线程() {
    let m = restored_review_fixture(true, DiskPhase::Forwarded);
    assert!(
        m.runs.lifecycle_gates.contains_key("C"),
        "加载运行后应立即同步关联门禁"
    );
    let mut req = request("turn/start", 91);
    req.params = Some(json!({"threadId":"C"}));
    let (_, a) = step(
        m,
        Event::ClientRequest {
            request: req,
            run_id: Some("new".into()),
        },
    );
    assert!(reason(&a, "lifecyclePending"));
    assert!(!a.iter().any(|a| matches!(a, Action::Request(_))));
}

#[test]
fn 续修_先运行后生命周期恢复必须封住审查子线程() {
    let m = restored_review_fixture(false, DiskPhase::LocalPurging);
    assert!(
        m.runs.lifecycle_gates.contains_key("C"),
        "加载生命周期后应立即同步关联门禁"
    );
    let mut req = request("turn/start", 92);
    req.params = Some(json!({"threadId":"C"}));
    let (_, a) = step(
        m,
        Event::ClientRequest {
            request: req,
            run_id: Some("new".into()),
        },
    );
    assert!(reason(&a, "lifecyclePending"));
    assert!(!a.iter().any(|a| matches!(a, Action::Request(_))));
}

fn lifecycle_scheduled(a: &[Action]) -> bool {
    a.iter().any(|a| {
        matches!(
            a,
            Action::Schedule {
                kind: TimerKind::Lifecycle,
                ..
            }
        )
    })
}

#[test]
fn 续修_共享结果未知和排空无票据时停止自调度() {
    let mut m = Machine::new(true);
    sent(&mut m);
    timeout(&mut m, 1);
    assert!(
        !lifecycle_scheduled(&tick(&mut m)),
        "只有未知结果时应等待外部事件"
    );
    retry(&mut m);
    upstream(&mut m, 2, Ok(json!({})));
    write(&mut m);
    m.threads_written(m.runs.index.version, None);
    assert!(
        !lifecycle_scheduled(&tick(&mut m)),
        "排空仅剩旧尝试时不应轮询"
    );
    assert_eq!(m.lifecycle.operations["P"].phase, Phase::Draining);
    let a = upstream(&mut m, 1, Ok(json!({})));
    assert!(
        lifecycle_scheduled(&a),
        "迟到回包应主动开始清记录写入并轮询票据"
    );
    write(&mut m);
    assert!(m.lifecycle.operations.is_empty());
}

#[test]
fn 续修_扫描写入历史读取和提交期限仍轮询并推进() {
    let mut m = Machine::default();
    assert!(lifecycle_scheduled(&begin(&mut m)));
    assert!(lifecycle_scheduled(&scan(&mut m, vec![])));
    let a = write(&mut m);
    assert!(a.iter().any(|a| matches!(a, Action::Request(_))));
    assert!(!lifecycle_scheduled(&a), "已发送请求由 RPC 回包或超时推进");
    let a = upstream(&mut m, 1, Ok(json!({})));
    assert!(lifecycle_scheduled(&a));
    assert!(lifecycle_scheduled(&write(&mut m)));
    m.clock.mono_ms = STORAGE_MS;
    let a = tick(&mut m);
    assert!(reason(&a, "storageTimeout"));
    assert!(lifecycle_scheduled(&a), "提交阶段仍需检查期限");
    m.threads_written(m.runs.index.version, None);
    let mut a = vec![];
    m.lifecycle_drive(&mut a);
    assert!(
        !lifecycle_scheduled(&a),
        "版本确认后排空写入由票据完成事件推进"
    );
    write(&mut m);
    assert!(m.lifecycle.operations.is_empty());
    m.lifecycle_history_stop(
        request("broker/threads/stop", 2),
        "missing".into(),
        &mut vec![],
    );
    assert!(lifecycle_scheduled(&tick(&mut m)));
    m.lifecycle.history[0].ticket.finish(ResultValue::Run(None));
    let a = tick(&mut m);
    assert!(reason(&a, "runMismatch"));
    assert!(!lifecycle_scheduled(&a));
}

fn request(method: &str, id: u64) -> ClientRequest {
    ClientRequest {
        conn: 1,
        id: json!(id),
        method: method.into(),
        params: Some(json!({"threadId":"P"})),
    }
}
fn tick(m: &mut Machine) -> Vec<Action> {
    let mut a = vec![];
    m.lifecycle_tick();
    m.lifecycle_drive(&mut a);
    a
}
fn begin(m: &mut Machine) -> Vec<Action> {
    let mut a = vec![];
    m.lifecycle_request(
        request("broker/threads/archive", 1),
        Kind::Archive,
        false,
        &mut a,
    );
    m.lifecycle_drive(&mut a);
    a
}
fn scan(m: &mut Machine, headers: Vec<RunHeader>) -> Vec<Action> {
    m.lifecycle.operations["P"]
        .scan
        .as_ref()
        .unwrap()
        .finish(ResultValue::Scanned(headers));
    tick(m)
}
fn write(m: &mut Machine) -> Vec<Action> {
    m.lifecycle
        .writing
        .as_ref()
        .expect("必须有待确认写入")
        .ticket
        .finish(ResultValue::Written(1));
    tick(m)
}
fn sent(m: &mut Machine) {
    begin(m);
    scan(m, vec![]);
    let a = write(m);
    assert!(a
        .iter()
        .any(|a| matches!(a,Action::Request(r) if r.method=="thread/archive")));
}
fn upstream(m: &mut Machine, attempt: u64, result: Result<Value, Value>) -> Vec<Action> {
    let id = m.lifecycle.operations["P"].record.operation_id.clone();
    let mut a = vec![];
    m.lifecycle_reply("P", &id, attempt, result, true, &mut a);
    m.lifecycle_drive(&mut a);
    a
}
fn timeout(m: &mut Machine, attempt: u64) {
    let id = m.lifecycle.operations["P"].record.operation_id.clone();
    m.lifecycle_reply("P", &id, attempt, Err(json!({})), false, &mut vec![]);
}
fn retry(m: &mut Machine) {
    m.lifecycle_request(
        request("broker/threads/archive", 2),
        Kind::Archive,
        false,
        &mut vec![],
    );
    scan(m, vec![]);
    write(m);
}
fn header(id: &str, parent: &str, child: Option<&str>, state: RunState) -> RunHeader {
    RunHeader {
        run_id: id.into(),
        thread_id: parent.into(),
        short_id: "12345678".into(),
        review_thread_id: child.map(str::to_owned),
        review_short_id: None,
        state: Some(state),
        stop_outcome: None,
    }
}
fn reason(a: &[Action], reason: &str) -> bool {
    a.iter()
        .any(|a| matches!(a,Action::Send{message,..} if message["error"]["data"]["reason"]==reason))
}

#[test]
#[allow(non_snake_case)]
fn 续修四_上游成功但票据未完成时对外阶段为_upstreamDone() {
    let mut m = Machine::default();
    sent(&mut m);
    upstream(&mut m, 1, Ok(json!({})));
    assert_eq!(
        m.lifecycle_projection("P").unwrap()["phase"],
        "upstreamDone"
    );
    assert_eq!(
        m.lifecycle_error("P").unwrap()["data"]["phase"],
        "upstreamDone"
    );
}

#[test]
#[allow(non_snake_case)]
fn 续修四_重启恢复_upstreamDone_立即投影为提交中阶段() {
    let mut source = Machine::default();
    sent(&mut source);
    let mut file = source.lifecycle.file.clone();
    file.records.get_mut("P").unwrap().phase = DiskPhase::UpstreamDone;
    let mut restored = Machine::default();
    restored.lifecycle_restore(Ok(file));
    assert_eq!(
        restored.lifecycle_projection("P").unwrap()["phase"],
        "upstreamDone"
    );
    assert_eq!(
        restored.lifecycle_error("P").unwrap()["data"]["phase"],
        "upstreamDone"
    );
}

#[test]
fn 续修四_意图写入期间仍对外报告_persisting() {
    let mut m = Machine::default();
    begin(&mut m);
    scan(&mut m, vec![]);
    assert_eq!(m.lifecycle_projection("P").unwrap()["phase"], "persisting");
    assert_eq!(
        m.lifecycle_error("P").unwrap()["data"]["phase"],
        "persisting"
    );
}

fn private_draining(old: &disk::Instance, new: &disk::Instance) -> Machine {
    let mut m = Machine::new(false);
    m.lifecycle.instance = Some(old.clone());
    sent(&mut m);
    timeout(&mut m, 1);
    m.lifecycle.instance = Some(new.clone());
    retry(&mut m);
    upstream(&mut m, 2, Ok(json!({})));
    write(&mut m);
    m.threads_written(m.runs.index.version, None);
    let a = tick(&mut m);
    assert!(a.iter().any(
        |a| matches!(a, Action::Send { message, .. } if message["result"]["archived"] == true)
    ));
    assert_eq!(m.lifecycle.operations["P"].phase, Phase::Draining);
    m
}

#[test]
fn 续修二_传输失败先到后实例退出事件推进排空并解除门禁() {
    let old = disk::Instance {
        pid: 101,
        created_at: 1,
    };
    let new = disk::Instance {
        pid: 202,
        created_at: 2,
    };
    let mut m = private_draining(&old, &new);
    let id = m.lifecycle.operations["P"].record.operation_id.clone();
    m.lifecycle_reply(
        "P",
        &id,
        1,
        Err(json!({"code":-32000,"message":"断开"})),
        false,
        &mut vec![],
    );
    let (mut m, actions) = step(m, Event::PrivateInstanceExited { instance: old });
    assert!(!actions.iter().any(|a| matches!(a, Action::Send { .. })));
    write(&mut m);
    assert!(m.lifecycle.operations.is_empty());
    assert!(m.lifecycle_error("P").is_none());
}

#[test]
fn 续修二_旧实例退出事件不结算新实例上的尝试() {
    let old = disk::Instance {
        pid: 301,
        created_at: 1,
    };
    let new = disk::Instance {
        pid: 302,
        created_at: 2,
    };
    let mut m = Machine::new(false);
    m.lifecycle.instance = Some(old.clone());
    sent(&mut m);
    timeout(&mut m, 1);
    m.lifecycle.instance = Some(new.clone());
    retry(&mut m);
    let (m, _) = step(m, Event::PrivateInstanceExited { instance: old });
    let attempts = &m.lifecycle.operations["P"].record.attempts;
    assert!(attempts[0].settled);
    assert!(!attempts[1].settled);
    assert!(m.lifecycle_error("P").is_some());
}

#[test]
fn 续修二_重复实例退出事件不重复回包() {
    let old = disk::Instance {
        pid: 401,
        created_at: 1,
    };
    let new = disk::Instance {
        pid: 402,
        created_at: 2,
    };
    let mut m = private_draining(&old, &new);
    let id = m.lifecycle.operations["P"].record.operation_id.clone();
    m.lifecycle_reply(
        "P",
        &id,
        1,
        Err(json!({"code":-32000,"message":"断开"})),
        false,
        &mut vec![],
    );
    let (mut m, _) = step(
        m,
        Event::PrivateInstanceExited {
            instance: old.clone(),
        },
    );
    write(&mut m);
    assert!(m.lifecycle.operations.is_empty());
    let (_, actions) = step(m, Event::PrivateInstanceExited { instance: old });
    assert!(!actions.iter().any(|a| matches!(a, Action::Send { .. })));
}

#[test]
fn 旧审查仍运行时列出真实子线程并解除门禁() {
    let mut m = Machine::default();
    let mut old = crate::store::tests::run("R", "P", 1);
    old.review_thread_id = Some("C".into());
    old.state = RunState::Running;
    m.runs.runs.insert("R".into(), old);
    m.runs
        .runs
        .insert("new".into(), crate::store::tests::run("new", "P", 2));
    let a = begin(&mut m);
    assert!(reason(&a, "threadBusy"));
    assert!(a.iter().any(|a|matches!(a,Action::Send{message,..} if message["error"]["data"]["blockers"].as_array().unwrap().contains(&json!({"threadId":"C","runId":"R","reason":"running"})))));
    assert!(m.lifecycle.operations.is_empty());
    assert!(!a.iter().any(|a| matches!(a, Action::Request(_))));
}
#[test]
fn 磁盘旧失联与多层双向闭包均阻塞() {
    for target in ["P", "C2"] {
        let mut m = Machine::default();
        let mut req = request("broker/threads/archive", 1);
        req.params = Some(json!({"threadId":target}));
        m.lifecycle_request(req, Kind::Archive, false, &mut vec![]);
        let headers = vec![
            header("R", "P", Some("C"), RunState::Ended),
            header("R2", "C", Some("C2"), RunState::Lost),
        ];
        m.lifecycle.operations[target]
            .scan
            .as_ref()
            .unwrap()
            .finish(ResultValue::Scanned(headers));
        let a = tick(&mut m);
        assert!(reason(&a, "threadBusy"));
        assert!(a.iter().any(|a|matches!(a,Action::Send{message,..} if message["error"]["data"]["blockers"][0]["reason"]=="lostUnconfirmed")));
        assert!(m.lifecycle.operations.is_empty());
    }
}

#[test]
fn 双向闭包与顺序无关覆盖祖先兄弟和孙线程() {
    let mut headers = vec![
        header("R", "P", Some("C"), RunState::Ended),
        header("R2", "C", Some("C2"), RunState::Ended),
        header("R3", "P", Some("S"), RunState::Ended),
    ];
    for _ in 0..2 {
        for thread in ["P", "C", "C2", "S"] {
            assert_eq!(
                closure(thread, &headers),
                BTreeSet::from(["P".into(), "C".into(), "C2".into(), "S".into()])
            );
        }
        headers.reverse();
    }
}
#[test]
fn 扫描失败和超时拒绝而不当作空集() {
    for timeout in [false, true] {
        let mut m = Machine::default();
        begin(&mut m);
        if timeout {
            m.clock.mono_ms = STORAGE_MS;
        } else {
            m.lifecycle.operations["P"]
                .scan
                .as_ref()
                .unwrap()
                .finish(ResultValue::Failed("测试失败".into()));
        }
        let a = tick(&mut m);
        assert!(reason(
            &a,
            if timeout {
                "storageTimeout"
            } else {
                "storageError"
            }
        ));
        assert!(m.lifecycle.operations.is_empty());
        assert!(!a.iter().any(|a| matches!(a, Action::Request(_))));
    }
}
#[test]
fn 意图超时保持门禁迟到成功仍发送而迟到失败放行() {
    for success in [false, true] {
        let mut m = Machine::default();
        begin(&mut m);
        scan(&mut m, vec![]);
        m.clock.mono_ms = STORAGE_MS;
        let a = tick(&mut m);
        assert!(reason(&a, "storageTimeout"));
        assert!(m.lifecycle_error("P").is_some());
        m.lifecycle
            .writing
            .as_ref()
            .unwrap()
            .ticket
            .finish(if success {
                ResultValue::Written(1)
            } else {
                ResultValue::Failed("测试失败".into())
            });
        let a = tick(&mut m);
        assert_eq!(a.iter().any(|a| matches!(a, Action::Request(_))), success);
        assert_eq!(m.lifecycle.operations.contains_key("P"), success);
        assert!(!a.iter().any(|a| matches!(a, Action::Send { .. })));
    }
}
#[test]
fn 必须等包含删除的索引版本确认才回成功() {
    let mut m = Machine::default();
    sent(&mut m);
    upstream(&mut m, 1, Ok(json!({})));
    let a = write(&mut m);
    assert!(!a.iter().any(|a| matches!(a, Action::Send { .. })));
    let version = m.runs.index.version;
    assert!(version > 0);
    m.threads_written(version - 1, None);
    assert!(!tick(&mut m)
        .iter()
        .any(|a| matches!(a, Action::Send { .. })));
    m.threads_written(version + 3, None);
    let a = tick(&mut m);
    assert!(a
        .iter()
        .any(|a| matches!(a,Action::Send{message,..} if message["result"]["archived"]==true)));
    assert!(m.lifecycle_error("P").is_some());
    write(&mut m);
    assert!(m.lifecycle_error("P").is_none());
}
#[test]
fn 第二代拒绝不能释放第一代在途请求() {
    let mut m = Machine::default();
    sent(&mut m);
    timeout(&mut m, 1);
    retry(&mut m);
    let a = upstream(&mut m, 2, Err(json!({"code":-32601,"message":"不支持"})));
    assert!(reason(&a, "outcomeUnknown"));
    assert!(m.lifecycle_error("P").is_some());
    write(&mut m); // 先确认第二代的拒绝事实，第一代仍在途。
    upstream(&mut m, 1, Ok(json!({})));
    write(&mut m);
    m.threads_written(m.runs.index.version, None);
    tick(&mut m);
    write(&mut m);
    assert!(m.lifecycle.operations.is_empty());
}
#[test]
fn 第二代成功先回包但第一代未终结则持续排空门禁() {
    let mut m = Machine::default();
    sent(&mut m);
    timeout(&mut m, 1);
    retry(&mut m);
    upstream(&mut m, 2, Ok(json!({})));
    write(&mut m);
    m.threads_written(m.runs.index.version, None);
    let a = tick(&mut m);
    assert!(a
        .iter()
        .any(|a| matches!(a,Action::Send{message,..} if message["result"]["archived"]==true)));
    assert_eq!(m.lifecycle.operations["P"].phase, Phase::Draining);
    assert!(m.lifecycle.writing.is_none());
    upstream(&mut m, 1, Err(json!({"code":-32601,"message":"不支持"})));
    write(&mut m);
    assert!(m.lifecycle.operations.is_empty());
}
#[test]
fn 重试扫描失败和新票据失败均保留旧请求与目标门禁() {
    for failed_write in [false, true] {
        let mut m = Machine::default();
        sent(&mut m);
        timeout(&mut m, 1);
        m.lifecycle_request(
            request("broker/threads/archive", 2),
            Kind::Archive,
            false,
            &mut vec![],
        );
        if failed_write {
            scan(&mut m, vec![]);
            m.lifecycle
                .writing
                .as_ref()
                .unwrap()
                .ticket
                .finish(ResultValue::Failed("失败".into()));
        } else {
            m.lifecycle.operations["P"]
                .scan
                .as_ref()
                .unwrap()
                .finish(ResultValue::Failed("失败".into()));
        }
        let a = tick(&mut m);
        assert!(reason(&a, "storageError"));
        let op = &m.lifecycle.operations["P"];
        assert_eq!(op.phase, Phase::OutcomeUnknown);
        assert!(!op.record.attempts[0].settled);
        assert!(m.lifecycle_error("P").is_some());
    }
}
#[test]
fn 精确错误分类不混淆归档删除与线程身份() {
    for kind in [Kind::Archive, Kind::Delete] {
        for message in [
            "thread not found: P",
            "no rollout found for thread id P",
            "no rollout found for conversation id P",
        ] {
            assert!(already_done(
                kind,
                "P",
                &json!({"code":-32600,"message":message})
            ));
        }
        for message in [
            "not found",
            "thread not found: C",
            "thread not found: P extra",
        ] {
            assert!(!already_done(
                kind,
                "P",
                &json!({"code":-32600,"message":message})
            ));
        }
        assert!(!already_done(
            kind,
            "P",
            &json!({"code":-32601,"message":"thread not found: P"})
        ));
        assert_eq!(
            already_done(
                kind,
                "P",
                &json!({"code":-32600,"message":"thread P is archived"})
            ),
            kind == Kind::Archive
        );
        assert_eq!(
            already_done(
                kind,
                "P",
                &json!({"code":-32603,"message":"failed to archive session: thread P not found"})
            ),
            kind == Kind::Archive
        );
    }
}
#[test]
fn 损坏生命周期拒绝写入只读仍放行() {
    let mut m = Machine::default();
    m.lifecycle_restore(Err("损坏".into()));
    assert!(reason(&begin(&mut m), "storageError"));
    for method in [
        "turn/start",
        "turn/steer",
        "review/start",
        "thread/resume",
        "thread/goal/set",
        "thread/unarchive",
        "broker/threads/purge",
    ] {
        let (_, a) = step(
            m.clone(),
            Event::ClientRequest {
                request: request(method, 1),
                run_id: None,
            },
        );
        assert!(reason(&a, "storageError"), "{method}");
    }
    let (_, a) = step(
        m,
        Event::ClientRequest {
            request: request("thread/read", 1),
            run_id: None,
        },
    );
    assert!(a.iter().any(|a| matches!(a, Action::Request(_))));
}
#[test]
fn 每个写入口门禁与只读放行及禁止重建索引() {
    let mut m = Machine::default();
    begin(&mut m);
    for method in [
        "turn/start",
        "turn/steer",
        "review/start",
        "thread/resume",
        "thread/goal/set",
        "thread/unarchive",
        "broker/threads/stop",
    ] {
        let (_, a) = step(
            m.clone(),
            Event::ClientRequest {
                request: request(method, 2),
                run_id: None,
            },
        );
        assert!(reason(&a, "lifecyclePending"), "{method}");
        assert!(!a.iter().any(|a| matches!(a, Action::Request(_))));
    }
    m.runs.ensure_thread("P", 1, 1);
    assert!(m.runs.index.threads.is_empty());
    for method in [
        "thread/read",
        "thread/turns/list",
        "thread/items/list",
        "thread/goal/get",
    ] {
        let (_, a) = step(
            m.clone(),
            Event::ClientRequest {
                request: request(method, 3),
                run_id: None,
            },
        );
        assert!(
            a.iter().any(|a| matches!(a, Action::Request(_))),
            "{method}"
        );
    }
}
#[test]
fn 删除通知不推进当前阶段也不结算尝试() {
    let mut m = Machine::default();
    begin(&mut m);
    m.lifecycle_observe("P", true, &mut vec![]);
    assert_eq!(m.lifecycle.operations["P"].phase, Phase::Scanning);
    scan(&mut m, vec![]);
    write(&mut m);
    m.lifecycle_observe("P", true, &mut vec![]);
    assert!(!m.lifecycle.operations["P"].record.attempts[0].settled);
    upstream(&mut m, 1, Err(json!({"code":-32601,"message":"错误"})));
    assert_eq!(
        m.lifecycle.operations["P"].record.phase,
        DiskPhase::UpstreamDone
    );
}
#[test]
fn 首次拒绝撤销失败保持门禁成功才放行() {
    let mut m = Machine::default();
    sent(&mut m);
    upstream(&mut m, 1, Err(json!({"code":-32601,"message":"不支持"})));
    m.lifecycle
        .writing
        .as_ref()
        .unwrap()
        .ticket
        .finish(ResultValue::Failed("失败".into()));
    assert!(reason(&tick(&mut m), "storageError"));
    assert!(m.lifecycle_error("P").is_some());
    write(&mut m);
    assert!(m.lifecycle.operations.is_empty());
}
#[test]
fn 关闭各阶段只回一次且不再发送() {
    for phase in [
        Phase::Scanning,
        Phase::Persisting,
        Phase::LocalPurging,
        Phase::Forwarded,
        Phase::OutcomeUnknown,
        Phase::Committing,
        Phase::Draining,
        Phase::Revoking,
    ] {
        let mut m = Machine::default();
        begin(&mut m);
        if phase != Phase::Scanning {
            scan(&mut m, vec![]);
        }
        if !matches!(phase, Phase::Scanning | Phase::Persisting) {
            write(&mut m);
        }
        if matches!(phase, Phase::Committing | Phase::Draining) {
            upstream(&mut m, 1, Ok(json!({})));
            write(&mut m);
        }
        m.lifecycle.operations.get_mut("P").unwrap().phase = phase;
        let persisted = m.lifecycle.file.clone();
        let (mut m, a) = step(m, Event::Shutdown);
        assert!(reason(&a, "shuttingDown"));
        assert_eq!(
            a.iter()
                .filter(|a| matches!(a,Action::Send{message,..} if message["id"]==1))
                .count(),
            1,
            "阶段 {phase:?}"
        );
        assert!(!a
            .iter()
            .any(|a| matches!(a, Action::Request(_) | Action::Store(StoreOp::Lifecycle(_)))));
        assert!(tick(&mut m).is_empty());
        assert_eq!(m.lifecycle.file, persisted);
    }
}

#[test]
fn 父子集合重叠的归档与复合删除先到者独占且后到者不等待() {
    for (kind, purge) in [(Kind::Archive, false), (Kind::Delete, true)] {
        let mut m = Machine::default();
        let mut old = crate::store::tests::run("R", "P", 1);
        old.review_thread_id = Some("C".into());
        m.runs.runs.insert("R".into(), old);
        m.lifecycle_request(
            request(
                if purge {
                    "broker/threads/purge"
                } else {
                    "broker/threads/archive"
                },
                1,
            ),
            kind,
            purge,
            &mut vec![],
        );
        let mut req = request(
            if purge {
                "broker/threads/archive"
            } else {
                "broker/threads/purge"
            },
            2,
        );
        req.params = Some(json!({"threadId":"C"}));
        let mut a = vec![];
        m.lifecycle_request(
            req,
            if purge { Kind::Archive } else { Kind::Delete },
            !purge,
            &mut a,
        );
        assert!(reason(&a, "lifecyclePending"));
        assert_eq!(m.lifecycle.operations.len(), 1);
        assert!(!a
            .iter()
            .any(|a| matches!(a, Action::Store(_) | Action::Request(_))));
        m.lifecycle.operations["P"]
            .scan
            .as_ref()
            .unwrap()
            .finish(ResultValue::Failed("首操作扫描失败".into()));
        tick(&mut m);
        assert!(m.lifecycle.operations.is_empty());
    }
}

#[test]
fn 本地删除扫描与归档交错发现集合重叠立即拒绝无相互等待() {
    use crate::runs::{PurgePlan, PurgeRun, RunTimer};
    use crate::store::purge::PurgeResult;
    for purge_first in [true, false] {
        let mut m = Machine::default();
        let mut actions = vec![];
        let mut local = request("broker/threads/purge", 10);
        local.params = Some(json!({"threadId":"C"}));
        if purge_first {
            m.run_purge_request(local.clone(), "C", &mut actions);
        }
        actions.extend(begin(&mut m));
        if !purge_first {
            scan(&mut m, vec![header("R", "P", Some("C"), RunState::Ended)]);
            let mut rejected = vec![];
            m.run_purge_request(local, "C", &mut rejected);
            assert!(reason(&rejected, "lifecyclePending"));
            assert!(!m.run_purge_busy("C"));
            continue;
        }
        let work = actions
            .iter()
            .find_map(|a| {
                if let Action::Store(StoreOp::Purge(w)) = a {
                    Some(w.clone())
                } else {
                    None
                }
            })
            .unwrap();
        let timer = actions
            .iter()
            .find_map(|a| {
                if let Action::Schedule {
                    kind: TimerKind::Run(RunTimer::Purge { generation }),
                    ..
                } = a
                {
                    Some(*generation)
                } else {
                    None
                }
            })
            .unwrap();
        let headers = vec![header("R", "P", Some("C"), RunState::Ended)];
        if purge_first {
            assert!(reason(&scan(&mut m, headers), "threadBusy"));
            assert!(m.lifecycle.operations.is_empty());
        } else {
            scan(&mut m, headers);
            assert!(m.lifecycle_error("C").is_some());
        }
        work.ticket.finish(PurgeResult::Prepared(PurgePlan {
            threads: BTreeSet::from(["C".into()]),
            runs: BTreeMap::from([(
                "R".into(),
                PurgeRun {
                    thread_id: "P".into(),
                    review_thread_id: Some("C".into()),
                    state: Some(RunState::Ended),
                    stop_outcome: None,
                },
            )]),
            ..Default::default()
        }));
        let mut a = vec![];
        m.run_purge_poll(timer, &mut a);
        // 先到的本地 purge 可以完成；先到的归档让本地 purge 在复查时立即失败。
        if !purge_first {
            assert!(reason(&a, "lifecyclePending"));
            assert!(!m.run_purge_busy("C"));
        } else {
            assert!(a
                .iter()
                .any(|a| matches!(a, Action::Store(StoreOp::Purge(_)))));
        }
    }
}

#[test]
fn 门禁期间审查回包照常迁移认领并撤销生命周期意图() {
    let mut m = Machine::default();
    m.live_conns.insert(1);
    m.records.entry("Q".into()).or_default().subscribed = true;
    let mut req = request("review/start", 8);
    // 显式 legacy 服务器夹具，后续注入独立审查子线程。
    req.params = Some(json!({"threadId":"Q","delivery":"detached"}));
    let (mut m, a) = step(
        m,
        Event::ClientRequest {
            request: req,
            run_id: Some("R".into()),
        },
    );
    let context = a
        .iter()
        .find_map(|a| {
            if let Action::Request(c) = a {
                Some(c.clone())
            } else {
                None
            }
        })
        .unwrap();
    begin(&mut m);
    scan(&mut m, vec![]);
    let (mut m, a) = step(
        m,
        Event::Reply {
            context,
            result: Ok(json!({"reviewThreadId":"P","turn":{"id":"审查轮"}})),
        },
    );
    assert_eq!(m.claims["P"].run_id.as_deref(), Some("R"));
    assert!(!m.claims.contains_key("Q"));
    assert_eq!(m.runs.runs["R"].review_thread_id.as_deref(), Some("P"));
    assert!(!a
        .iter()
        .any(|a| matches!(a,Action::Request(c) if c.method=="thread/archive")));
    write(&mut m);
    assert_eq!(m.lifecycle.operations["P"].phase, Phase::Revoking);
    let a = write(&mut m);
    assert!(reason(&a, "threadBusy"));
    assert!(a.iter().any(|a|matches!(a,Action::Send{message,..} if message["error"]["data"]["blockers"][0]["runId"]=="R")));
    assert!(m.lifecycle.file.records.is_empty());
    assert!(m.lifecycle_error("P").is_none());
    assert!(m.claims.contains_key("P"));
}

#[test]
fn 撤销期限到仍持门禁迟到确认后只释放不重复回包() {
    let mut m = Machine::default();
    sent(&mut m);
    upstream(&mut m, 1, Err(json!({"code":-32601,"message":"拒绝"})));
    m.clock.mono_ms = STORAGE_MS;
    assert!(reason(&tick(&mut m), "storageTimeout"));
    assert!(m.lifecycle_error("P").is_some());
    let a = write(&mut m);
    assert!(!a.iter().any(|a| matches!(a, Action::Send { .. })));
    assert!(m.lifecycle.operations.is_empty());
}

#[test]
fn 重试发现新阻塞或发送前新活动都保留旧尝试() {
    for late in [false, true] {
        let mut m = Machine::default();
        sent(&mut m);
        timeout(&mut m, 1);
        m.lifecycle_request(
            request("broker/threads/archive", 2),
            Kind::Archive,
            false,
            &mut vec![],
        );
        let mut a = vec![];
        if late {
            scan(&mut m, vec![]);
        } else {
            m.lifecycle.operations["P"]
                .scan
                .as_ref()
                .unwrap()
                .finish(ResultValue::Scanned(vec![header(
                    "R",
                    "P",
                    Some("C"),
                    RunState::Running,
                )]));
        }
        if late {
            let mut run = crate::store::tests::run("R", "P", 1);
            run.review_thread_id = Some("C".into());
            run.state = RunState::Running;
            m.runs.runs.insert("R".into(), run);
            a.extend(write(&mut m));
        } else {
            a.extend(tick(&mut m));
        }
        assert!(reason(&a, "threadBusy"));
        assert_eq!(m.lifecycle.operations["P"].phase, Phase::OutcomeUnknown);
        assert!(!m.lifecycle.operations["P"].record.attempts[0].settled);
        assert!(m.lifecycle_error("P").is_some());
        assert!(m.lifecycle_error("C").is_none());
    }
}

#[test]
fn 删除通知各阶段和操作完成后都清索引而不结算在途尝试() {
    for phase in [Phase::Scanning, Phase::LocalPurging, Phase::Forwarded] {
        let mut m = Machine::default();
        m.runs.ensure_thread("P", 1, 1);
        begin(&mut m);
        if phase != Phase::Scanning {
            scan(&mut m, vec![]);
            write(&mut m);
        }
        m.lifecycle.operations.get_mut("P").unwrap().phase = phase;
        let attempts = m.lifecycle.operations["P"].record.attempts.clone();
        let (mut m, a) = step(
            m,
            Event::Notification {
                method: "thread/deleted".into(),
                params: Some(json!({"threadId":"P"})),
            },
        );
        assert_eq!(m.lifecycle.operations["P"].phase, phase);
        assert_eq!(m.lifecycle.operations["P"].record.attempts, attempts);
        assert!(m.runs.index.threads.is_empty());
        assert!(a
            .iter()
            .any(|a| matches!(a, Action::Store(StoreOp::PutThreads(_)))));
        m.lifecycle.operations.clear();
        m.runs.lifecycle_gates.clear();
        m.runs.ensure_thread("P", 2, 2);
        let (m, _) = step(
            m,
            Event::Notification {
                method: "thread/deleted".into(),
                params: Some(json!({"threadId":"P"})),
            },
        );
        assert!(m.runs.index.threads.is_empty());
    }
}
#[test]
fn 恢复仅有记录的线程保留短编号且不建索引() {
    let mut m = Machine::default();
    sent(&mut m);
    let file = m.lifecycle.file.clone();
    let mut restored = Machine::default();
    restored.lifecycle_restore(Ok(file));
    restored.records.entry("P".into()).or_default().goal_active = true;
    restored
        .runs
        .runs
        .insert("旧运行".into(), crate::store::tests::run("旧运行", "P", 1));
    assert_eq!(
        restored.lifecycle.operations["P"].phase,
        Phase::OutcomeUnknown
    );
    let summary = restored.thread_summary("P").unwrap();
    assert_eq!(summary.state, crate::runs::SummaryState::Idle);
    assert!(!summary.short_id.is_empty());
    assert!(summary.lifecycle.is_some());
    assert!(!summary.goal_active);
    assert_eq!(summary.pending_count, 0);
    assert!(summary.last_run_id.is_none());
    let (_, a) = step(
        restored.clone(),
        Event::ClientRequest {
            request: request("broker/threads/watch", 7),
            run_id: None,
        },
    );
    assert!(a.iter().any(|a|matches!(a,Action::Send{message,..} if message["id"]==7 && message["result"]["run"].is_null())));
    assert!(restored.runs.index.threads.is_empty());
}

#[test]
fn 提交写盘失败超时重试仅继续提交且合并版本可完成() {
    for timeout in [false, true] {
        let mut m = Machine::default();
        sent(&mut m);
        upstream(&mut m, 1, Ok(json!({})));
        write(&mut m);
        if timeout {
            m.clock.mono_ms = STORAGE_MS;
        } else {
            m.threads_written(m.runs.index.version, Some("失败".into()));
        }
        let a = tick(&mut m);
        assert!(reason(
            &a,
            if timeout {
                "storageTimeout"
            } else {
                "storageError"
            }
        ));
        assert!(a.iter().any(|a|matches!(a,Action::Send{message,..} if message["error"]["data"]["upstreamDone"]==true)));
        m.lifecycle_request(
            request("broker/threads/archive", 2),
            Kind::Archive,
            false,
            &mut vec![],
        );
        assert_eq!(m.lifecycle.operations["P"].record.attempts.len(), 1);
        m.threads_written(m.runs.index.version + 1, None);
        let a = tick(&mut m);
        assert!(!a.iter().any(|a| matches!(a, Action::Request(_))));
        assert!(a.iter().any(|a|matches!(a,Action::Send{message,..} if message["id"]==2 && message["result"]["archived"]==true)));
    }
}
#[test]
fn 结果未知只保留父门禁重试重新封住子线程() {
    let mut m = Machine::default();
    begin(&mut m);
    scan(&mut m, vec![header("R", "P", Some("C"), RunState::Ended)]);
    write(&mut m);
    assert!(m.lifecycle_error("C").is_some());
    timeout(&mut m, 1);
    assert!(m.lifecycle_error("P").is_some());
    assert!(m.lifecycle_error("C").is_none());
    m.lifecycle_request(
        request("broker/threads/archive", 2),
        Kind::Archive,
        false,
        &mut vec![],
    );
    scan(&mut m, vec![header("R", "P", Some("C"), RunState::Ended)]);
    assert!(m.lifecycle_error("C").is_some());
    m.clock.mono_ms = STORAGE_MS;
    assert!(reason(&tick(&mut m), "storageTimeout"));
    assert_eq!(m.lifecycle.operations["P"].phase, Phase::OutcomeUnknown);
    assert!(m.lifecycle_error("C").is_none());
    let a = write(&mut m);
    assert!(!a.iter().any(|a| matches!(a, Action::Request(_))));
    assert!(m.lifecycle.operations["P"].record.attempts[1].settled);
    assert!(!m.lifecycle.operations["P"].record.attempts[0].settled);
}
#[test]
fn 发送前复查遇新活动必须撤销意图才放行() {
    let mut m = Machine::default();
    begin(&mut m);
    scan(&mut m, vec![]);
    let mut run = crate::store::tests::run("R", "P", 1);
    run.state = RunState::Running;
    m.runs.runs.insert("R".into(), run);
    let a = write(&mut m);
    assert!(!a.iter().any(|a| matches!(a, Action::Request(_))));
    assert_eq!(m.lifecycle.operations["P"].phase, Phase::Revoking);
    assert!(m.lifecycle_error("P").is_some());
    assert!(reason(&write(&mut m), "threadBusy"));
    assert!(m.lifecycle.operations.is_empty());
}
#[test]
fn 外部开轮在扫描中止而发送后仅告警() {
    let mut m = Machine::default();
    begin(&mut m);
    let mut a = vec![];
    m.lifecycle_observe("P", false, &mut a);
    assert!(reason(&a, "threadBusy"));
    assert!(m.lifecycle.operations.is_empty());
    sent(&mut m);
    let mut a = vec![];
    m.lifecycle_observe("P", false, &mut a);
    assert!(a.iter().any(|a| matches!(a, Action::Warn(_))));
    assert_eq!(m.lifecycle.operations["P"].phase, Phase::Forwarded);
}
#[test]
fn 恢复上游已完成只提交共享旧请求仍保持排空() {
    let mut m = Machine::new(true);
    sent(&mut m);
    timeout(&mut m, 1);
    retry(&mut m);
    upstream(&mut m, 2, Ok(json!({})));
    write(&mut m);
    let mut restored = Machine::new(true);
    restored.lifecycle_restore(Ok(m.lifecycle.file.clone()));
    let a = tick(&mut restored);
    assert!(!a.iter().any(|a| matches!(a, Action::Request(_))));
    write(&mut restored);
    restored.threads_written(restored.runs.index.version, None);
    tick(&mut restored);
    assert_eq!(restored.lifecycle.operations["P"].phase, Phase::Draining);
    assert!(restored.lifecycle.writing.is_none());
}
#[test]
fn 私有实例只有确切退出身份才能结算旧尝试() {
    let mut m = Machine::default();
    let instance = disk::Instance {
        pid: 42,
        created_at: 12,
    };
    m.lifecycle.instance = Some(instance.clone());
    sent(&mut m);
    timeout(&mut m, 1);
    assert!(!m.lifecycle.operations["P"].record.attempts[0].settled);
    m.lifecycle_instance_exited(&disk::Instance {
        pid: 42,
        created_at: 13,
    });
    assert!(!m.lifecycle.operations["P"].record.attempts[0].settled);
    m.lifecycle_instance_exited(&instance);
    assert!(m.lifecycle.operations["P"].record.attempts[0].settled);
}
#[test]
fn 本地删除前先保存记录而删除通知不能跳过本地阶段() {
    let mut m = Machine::default();
    m.lifecycle_request(
        request("broker/threads/purge", 1),
        Kind::Delete,
        true,
        &mut vec![],
    );
    scan(&mut m, vec![]);
    assert_eq!(
        m.lifecycle.writing.as_ref().unwrap().file.records["P"].phase,
        DiskPhase::LocalPurging
    );
    assert!(m.lifecycle.operations["P"].record.attempts.is_empty());
    let a = write(&mut m);
    assert!(a
        .iter()
        .any(|a| matches!(a, Action::Store(StoreOp::Purge(_)))));
    m.lifecycle_observe("P", true, &mut vec![]);
    assert_eq!(m.lifecycle.operations["P"].phase, Phase::LocalPurging);
    assert!(m.lifecycle.operations["P"].record.attempts.is_empty());
}
#[test]
fn 仅有本地删除记录恢复后重试先本地删除() {
    let mut m = Machine::default();
    m.lifecycle_request(
        request("broker/threads/purge", 1),
        Kind::Delete,
        true,
        &mut vec![],
    );
    scan(&mut m, vec![]);
    write(&mut m);
    let mut restored = Machine::default();
    restored.lifecycle_restore(Ok(m.lifecycle.file.clone()));
    assert_eq!(
        restored.thread_summary("P").unwrap().state,
        crate::runs::SummaryState::Idle
    );
    restored.lifecycle_request(
        request("broker/threads/purge", 2),
        Kind::Delete,
        true,
        &mut vec![],
    );
    scan(&mut restored, vec![]);
    let a = write(&mut restored);
    assert!(a
        .iter()
        .any(|a| matches!(a, Action::Store(StoreOp::Purge(_)))));
    assert!(!a.iter().any(|a| matches!(a, Action::Request(_))));
}
#[test]
fn 全部尝试拒绝才撤销且不同种类不能重试() {
    let mut m = Machine::default();
    sent(&mut m);
    timeout(&mut m, 1);
    let mut a = vec![];
    m.lifecycle_request(request("thread/delete", 3), Kind::Delete, false, &mut a);
    assert!(reason(&a, "lifecyclePending"));
    retry(&mut m);
    upstream(&mut m, 2, Err(json!({"code":-32601,"message":"拒绝"})));
    write(&mut m);
    upstream(&mut m, 1, Err(json!({"code":-32601,"message":"拒绝"})));
    assert_eq!(m.lifecycle.operations["P"].phase, Phase::Revoking);
    write(&mut m);
    assert!(m.lifecycle_error("P").is_none());
}

#[test]
fn 事项编号解析后按实际线程拒绝含守护事项() {
    use crate::inbox::{InboxFile, InboxItem, InboxKind, InboxStatus};
    for (id, kind) in [
        ("ap_test", InboxKind::Approval),
        ("gd_test", InboxKind::Guardian),
    ] {
        let mut m = Machine::default();
        begin(&mut m);
        m.inbox_restore(
            Some(InboxFile {
                items: vec![InboxItem {
                    root_thread_id: None,
                    parent_thread_id: None,

                    id: id.into(),
                    kind,
                    thread_id: "P".into(),
                    run_id: None,
                    turn_id: None,
                    method: "审批".into(),
                    params: json!({}),
                    created_at: 0,
                    expires_at: 999999,
                    status: InboxStatus::Pending,
                }],
            }),
            &mut vec![],
        );
        let mut req = request("broker/inbox/resolve", 2);
        req.params = Some(json!({"id":id,"answer":{"override":false}}));
        let (_, a) = step(
            m,
            Event::ClientRequest {
                request: req,
                run_id: None,
            },
        );
        assert!(reason(&a, "lifecyclePending"));
        assert!(!a
            .iter()
            .any(|a| matches!(a, Action::Request(_) | Action::AppReplyAcked { .. })));
    }
}
#[test]
fn 停止改投审查子线程后再次检查门禁() {
    let mut m = Machine::default();
    let mut old = crate::store::tests::run("R", "P", 1);
    old.state = RunState::Lost;
    old.stop_outcome = Some(StopOutcome::Unconfirmed);
    old.review_thread_id = Some("C".into());
    m.runs.runs.insert("R".into(), old);
    let mut saved = Machine::default();
    sent(&mut saved);
    let mut record = saved.lifecycle.file.records.remove("P").unwrap();
    record.thread_id = "C".into();
    record.kind = Kind::Delete;
    m.lifecycle_restore(Ok(File {
        records: BTreeMap::from([("C".into(), record)]),
    }));
    let mut req = request("broker/threads/stop", 2);
    req.params = Some(json!({"threadId":"P","runId":"R"}));
    let (_, a) = step(
        m,
        Event::ClientRequest {
            request: req,
            run_id: None,
        },
    );
    assert!(reason(&a, "lifecyclePending"));
    assert!(!a.iter().any(|a| matches!(a, Action::Request(_))));
}
#[test]
fn 历史失联按编号读取核实后写回且较新运行保持不变() {
    let mut m = Machine::default();
    let newer = crate::store::tests::run("new", "P", 5);
    m.runs.runs.insert("new".into(), newer.clone());
    let mut old = crate::store::tests::run("old", "P", 1);
    old.state = RunState::Lost;
    old.stop_outcome = Some(StopOutcome::Unconfirmed);
    old.review_thread_id = Some("C".into());
    let mut req = request("broker/threads/stop", 2);
    req.params = Some(json!({"threadId":"P","runId":"old"}));
    m.client_stop(req, &mut vec![]);
    m.lifecycle.history[0]
        .ticket
        .finish(ResultValue::Run(Some(Box::new(old))));
    let a = tick(&mut m);
    assert_eq!(
        a.iter().filter(|a| matches!(a, Action::Request(_))).count(),
        2
    );
    let contexts: Vec<_> = a
        .iter()
        .filter_map(|a| {
            if let Action::Request(c) = a {
                Some(c.clone())
            } else {
                None
            }
        })
        .collect();
    let mut all = vec![];
    for c in contexts {
        assert_eq!(c.params.as_ref().unwrap()["threadId"], "C");
        // 丢失运行的停止核实经状态读取层：元数据非 active 即 Idle。
        let result = if c.method == "thread/read" {
            assert!(c.params.as_ref().unwrap().get("includeTurns").is_none());
            json!({"thread":{"id":"C","status":{"type":"idle"}}})
        } else {
            json!({"goal":null})
        };
        let (next, a) = step(
            m,
            Event::Reply {
                context: c,
                result: Ok(result),
            },
        );
        m = next;
        all.extend(crate::test_support::background::empty_background(&mut m, a));
    }
    assert!(all.iter().any(|a|matches!(a,Action::Store(StoreOp::PutRun(r)) if r.run_id=="old" && r.stop_outcome==Some(StopOutcome::Confirmed))));
    assert_eq!(m.runs.runs["new"], newer);
    let a = begin(&mut m);
    assert!(!reason(&a, "threadBusy"));
}
#[test]
fn 客户端取消和断开仅移除等待者() {
    let mut m = Machine::default();
    sent(&mut m);
    m.lifecycle_cancel(1, &json!(1));
    assert!(m.lifecycle.operations["P"].waiters.is_empty());
    assert!(m.lifecycle_error("P").is_some());
    timeout(&mut m, 1);
    retry(&mut m);
    m.lifecycle_disconnect(1);
    assert!(m.lifecycle.operations["P"].waiters.is_empty());
    assert_eq!(m.lifecycle.operations["P"].record.attempts.len(), 2);
}

fn local_finished(m: &mut Machine, failed: bool) -> Vec<Action> {
    let operation = m.lifecycle.operations["P"].record.operation_id.clone();
    let mut a = vec![Action::Send {
        conn: u64::MAX,
        message: proto::response_ok(
            json!(operation),
            json!({"threadId":"P","deleted":["runs/old.json"],"alreadyDeleted":[],"failed":if failed{json!([{"path":"runs/locked.json","reason":"文件被占用"}])}else{json!([])}}),
        ),
    }];
    m.lifecycle_drive(&mut a);
    a
}

#[test]
fn 复合删除本地失败不发服务器而成功后各结局并入报告() {
    for outcome in [
        "localFailed",
        "success",
        "missing",
        "unsupported",
        "unknown",
    ] {
        let mut m = Machine::default();
        m.lifecycle_request(
            request("broker/threads/purge", 1),
            Kind::Delete,
            true,
            &mut vec![],
        );
        scan(&mut m, vec![]);
        // 模拟本地存储已确认阶段，逐项结果从同一内部回包入口交付。
        let file = m.lifecycle.writing.take().unwrap().file;
        m.lifecycle.file = file;
        m.lifecycle.operations.get_mut("P").unwrap().phase = Phase::LocalPurging;
        let a = local_finished(&mut m, outcome == "localFailed");
        if outcome == "localFailed" {
            assert!(a.iter().any(|a|matches!(a,Action::Send{message,..} if message["result"]["failed"].as_array().unwrap().len()==2)));
            assert!(m.lifecycle.writing.is_none());
            continue;
        }
        let saved = &m.lifecycle.writing.as_ref().unwrap().file.records["P"];
        assert_eq!(saved.phase, DiskPhase::Forwarded);
        assert_eq!(saved.attempts.len(), 1);
        assert!(!saved.attempts[0].settled);
        assert!(m.lifecycle.operations["P"].sent.is_empty());
        let a = write(&mut m);
        assert!(a
            .iter()
            .any(|a| matches!(a,Action::Request(c) if c.method=="thread/delete")));
        let a = if outcome == "unknown" {
            let id = m.lifecycle.operations["P"].record.operation_id.clone();
            let mut a = vec![];
            m.lifecycle_reply("P", &id, 1, Err(json!({})), false, &mut a);
            a
        } else {
            let result = match outcome {
                "success" => Ok(json!({})),
                "missing" => Err(json!({"code":-32600,"message":"thread not found: P"})),
                _ => Err(json!({"code":-32601,"message":"不支持"})),
            };
            upstream(&mut m, 1, result);
            let mut a = write(&mut m);
            if outcome != "unsupported" {
                m.threads_written(m.runs.index.version, None);
                a.extend(tick(&mut m));
            }
            a
        };
        let result = a
            .iter()
            .find_map(|a| {
                if let Action::Send { message, .. } = a {
                    message.get("result")
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(result["deleted"][0], "runs/old.json");
        assert!(result["operationId"].is_string());
        match outcome {
            "success" => assert!(result["deleted"]
                .as_array()
                .unwrap()
                .contains(&json!("server-thread"))),
            "missing" => assert!(result["alreadyDeleted"]
                .as_array()
                .unwrap()
                .contains(&json!("server-thread"))),
            "unknown" => {
                assert_eq!(result["failed"][0]["reason"], "outcomeUnknown");
                assert!(m.lifecycle_error("P").is_some());
            }
            _ => assert_eq!(
                result["failed"][0]["reason"],
                "此 Codex 版本不支持 thread/delete，服务器线程保留"
            ),
        }
    }
}

#[test]
fn 原始方法所有确切错误清索引但错误回包保持形状其他错误保留索引() {
    for kind in [Kind::Archive, Kind::Delete] {
        for (code, message) in [
            (-32600, "thread not found: P"),
            (-32600, "no rollout found for thread id P"),
            (-32600, "no rollout found for conversation id P"),
            (-32603, "failed to archive session: thread P not found"),
            (-32600, "thread P is archived"),
            (-32600, "not found"),
            (-32600, "thread not found: Q"),
            (-32601, "不支持"),
        ] {
            let error = json!({"code":code,"message":message});
            let done = already_done(kind, "P", &error);
            let mut m = Machine::default();
            m.runs.ensure_thread("P", 1, 1);
            let mut a = vec![];
            m.lifecycle_request(
                request(
                    if kind == Kind::Archive {
                        "thread/archive"
                    } else {
                        "thread/delete"
                    },
                    1,
                ),
                kind,
                false,
                &mut a,
            );
            scan(&mut m, vec![]);
            write(&mut m);
            upstream(&mut m, 1, Err(error.clone()));
            let mut a = write(&mut m);
            if done {
                assert!(m.runs.index.threads.is_empty());
                assert!(!a.iter().any(|a| matches!(a, Action::Send { .. })));
                m.threads_written(m.runs.index.version, None);
                a.extend(tick(&mut m));
            } else {
                assert_eq!(m.runs.index.threads.len(), 1);
            }
            let reply = a
                .iter()
                .find_map(|a| {
                    if let Action::Send { message, .. } = a {
                        Some(message)
                    } else {
                        None
                    }
                })
                .unwrap();
            if done {
                assert_eq!(reply["error"], error);
            } else {
                assert_eq!(reply["error"]["data"]["reason"], "upstreamRejected");
            }
        }
    }
}

#[test]
fn 私有和共享旧转发记录恢复后拒绝是否能放行取决于旧实例证据() {
    for private in [false, true] {
        let mut before = Machine::new(!private);
        sent(&mut before);
        let mut file = before.lifecycle.file.clone();
        if private {
            file.records.get_mut("P").unwrap().attempts[0].settled = true;
        }
        let mut m = Machine::new(!private);
        m.lifecycle_restore(Ok(file));
        assert_eq!(
            m.lifecycle_projection("P").unwrap()["phase"],
            "outcomeUnknown"
        );
        retry(&mut m);
        let a = upstream(&mut m, 2, Err(json!({"code":-32601,"message":"拒绝"})));
        if private {
            let a = write(&mut m);
            assert!(reason(&a, "upstreamRejected"));
            assert!(m.lifecycle.operations.is_empty());
        } else {
            assert!(reason(&a, "outcomeUnknown"));
            assert!(m.lifecycle_error("P").is_some());
        }
    }
}

#[test]
fn 删除提交恢复时重试仍返回删除报告且不重发上游() {
    let mut before = Machine::default();
    sent(&mut before);
    upstream(&mut before, 1, Ok(json!({})));
    write(&mut before);
    let mut file = before.lifecycle.file.clone();
    file.records.get_mut("P").unwrap().kind = Kind::Delete;
    let mut m = Machine::default();
    m.lifecycle_restore(Ok(file));
    let mut a = vec![];
    m.lifecycle_request(
        request("broker/threads/purge", 2),
        Kind::Delete,
        true,
        &mut a,
    );
    assert!(!reason(&a, "lifecyclePending"));
    tick(&mut m);
    write(&mut m);
    m.threads_written(m.runs.index.version, None);
    let a = tick(&mut m);
    assert!(a.iter().any(|a|matches!(a,Action::Send{message,..} if message["result"]["deleted"].as_array().is_some_and(|items|items.contains(&json!("server-thread"))))));
    assert!(!a.iter().any(|a| matches!(a, Action::Request(_))));
}
