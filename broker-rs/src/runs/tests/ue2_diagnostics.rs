use super::*;

#[test]
fn 旧记录可读且缺省新增字段序列化逐字一致() {
    let legacy = r#"{"startConfirmation":{"confirmed":false,"owned":null,"threadId":null,"turnId":null},"runId":"旧运行","threadId":"线程","shortId":"12345678","joined":false,"state":"running","status":null,"startedAt":1,"endedAt":null,"meta":null,"reviewThreadId":null,"reviewShortId":null,"input":"输入","continuedFrom":null,"interruptReason":null,"stopOutcome":null,"stopCheck":null,"progress":{"done":[],"doneTotal":0,"unconfirmed":[],"observedFiles":[]},"turns":[],"turnsTotal":0}"#;
    let record: RunRecord = serde_json::from_str(legacy).unwrap();
    assert!(
        record.run_policy.is_none() && record.stall.is_none() && record.stop_diagnostics.is_none()
    );
    assert!(record.child_upstream_errors.is_empty());
    assert_eq!(serde_json::to_string(&record).unwrap(), legacy);
}

fn new_diagnostics() -> StopDiagnostics {
    StopDiagnostics {
        reason: InterruptReason::StallTimeout,
        started_at: 1,
        goal_started_at: None,
        goal_settled_at: Some(2),
        interrupt_sent: true,
        interrupt_sent_at: Some(3),
        interrupt_reply: Some(StopRpcOutcome::Timeout),
        interrupt_replied_at: Some(4),
        turn_ended_at: None,
        unconfirmed_at: Some(5),
        confirmed_at: None,
        rpc_issues: vec![StopRpcIssue {
            method: "turn/interrupt".into(),
            kind: StopRpcOutcome::Timeout,
            at: 4,
            message: "请求超时".into(),
        }],
    }
}

#[test]
fn 新字段与全部枚举往返且结构体内部空值是null() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    let r = m.runs.runs.get_mut("运行").unwrap();
    r.run_policy = Some(RunPolicy {
        stall_timeout_ms: 0,
        upstream_error_policy: UpstreamErrorPolicy::StopAccess,
    });
    r.stall = Some(StallReport {
        timeout_ms: 600_000,
        phase: StallPhase::Verifying,
        expired_at: 1000,
        idle_ms: 600_000,
        last_progress_at: None,
        verify: None,
        expiries: 1,
    });
    r.stop_diagnostics = Some(new_diagnostics());
    let error = upstream_error::UpstreamError::from_error(
        "子",
        Some("子轮"),
        upstream_error::UpstreamErrorSource::ErrorNotification,
        &json!({"message":"认证失败","codexErrorInfo":"unauthorized"}),
        Some(true),
        1,
    );
    r.child_upstream_errors.push(ChildUpstreamError {
        thread_id: "子".into(),
        parent_thread_id: "线程".into(),
        upstream_error: error,
    });
    let wire = serde_json::to_value(&*r).unwrap();
    assert_eq!(
        wire["runPolicy"],
        json!({"stallTimeoutMs":0,"upstreamErrorPolicy":"stopAccess"})
    );
    assert!(wire["stall"].get("verify").unwrap().is_null());
    assert!(wire["stopDiagnostics"]
        .get("confirmedAt")
        .unwrap()
        .is_null());
    assert_eq!(serde_json::from_value::<RunRecord>(wire).unwrap(), *r);
    for phase in [
        StallPhase::Verifying,
        StallPhase::Resumed,
        StallPhase::Stopping,
        StallPhase::Deferred,
    ] {
        assert_eq!(
            serde_json::from_value::<StallPhase>(serde_json::to_value(phase).unwrap()).unwrap(),
            phase
        );
    }
    for verify in [
        StallVerify::Progressed,
        StallVerify::TurnEnded,
        StallVerify::InProgress,
        StallVerify::Absent,
        StallVerify::Unknown,
    ] {
        assert_eq!(
            serde_json::from_value::<StallVerify>(serde_json::to_value(verify).unwrap()).unwrap(),
            verify
        );
    }
}

#[test]
fn 同事件多个标脏只增版一次写盘一次且通知字段完整() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    m.live_conns.insert(1);
    m.cxb_conns.insert(1);
    m.records.entry("线程".into()).or_default().watchers.push(1);
    m.runs.runs.get_mut("运行").unwrap().stop_diagnostics = Some(new_diagnostics());
    m.mark_run_diagnostic("运行", DiagnosticField::StopDiagnostics, None);
    m.mark_run_diagnostic("运行", DiagnosticField::Stall, None);
    let mut actions = vec![];
    m.runs.put("运行", &mut actions);
    m.flush_run_diagnostics(&mut actions);
    assert_eq!(m.runs.runs["运行"].diagnostic_revision, 1);
    assert_eq!(puts(&actions), 1);
    let notices: Vec<_> = actions
        .iter()
        .filter_map(|a| match a {
            Action::Send { message, .. } if message["method"] == "broker/run/updated" => {
                Some(&message["params"])
            }
            _ => None,
        })
        .collect();
    assert_eq!(notices.len(), 1);
    for field in [
        "runId",
        "threadId",
        "diagnosticRevision",
        "upstreamError",
        "lastProgressAt",
        "state",
        "status",
        "interruptReason",
        "stopOutcome",
        "stopCheck",
        "stall",
        "stopDiagnostics",
        "childUpstreamErrors",
        "childUpstreamError",
    ] {
        assert!(notices[0].get(field).is_some(), "缺少 {field}");
    }
    assert_eq!(notices[0]["childUpstreamErrors"], json!([]));
    assert_eq!(notices[0]["stopDiagnostics"]["interruptReply"], "timeout");
    m.flush_run_diagnostics(&mut actions);
    assert_eq!(m.runs.runs["运行"].diagnostic_revision, 1);
    let child = ChildUpstreamError {
        thread_id: "子".into(),
        parent_thread_id: "线程".into(),
        upstream_error: upstream_error::UpstreamError::from_error(
            "子",
            Some("子轮"),
            upstream_error::UpstreamErrorSource::ErrorNotification,
            &json!({"message":"权限错误","codexErrorInfo":{"httpConnectionFailed":{"httpStatusCode":403}}}),
            Some(true),
            10,
        ),
    };
    m.runs
        .runs
        .get_mut("运行")
        .unwrap()
        .child_upstream_errors
        .push(child.clone());
    m.mark_run_diagnostic(
        "运行",
        DiagnosticField::ChildUpstreamErrors,
        Some(child.clone()),
    );
    actions.clear();
    m.flush_run_diagnostics(&mut actions);
    let snapshot = actions
        .iter()
        .find_map(|a| match a {
            Action::Send { message, .. } => Some(&message["params"]),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        snapshot["childUpstreamErrors"],
        serde_json::to_value(vec![child.clone()]).unwrap()
    );
    assert_eq!(
        snapshot["childUpstreamError"],
        serde_json::to_value(child).unwrap()
    );
}

#[test]
fn 仅最近进展增版节流不通知且收件人释放前捕获() {
    let mut m = machine();
    open(&mut m, "运行", "线程");
    m.cxb_conns.insert(2);
    m.records.entry("线程".into()).or_default().watchers.push(2);
    m.runs.runs.get_mut("运行").unwrap().last_progress_at = Some(1000);
    m.mark_run_diagnostic("运行", DiagnosticField::LastProgressAt, None);
    let mut a = vec![];
    m.flush_run_diagnostics(&mut a);
    assert_eq!(m.runs.runs["运行"].diagnostic_revision, 1);
    assert_eq!(puts(&a), 0);
    assert!(!a.iter().any(|a| matches!(a, Action::Send { .. })));
    m.mark_run_diagnostic("运行", DiagnosticField::StopCheck, None);
    m.records.get_mut("线程").unwrap().watchers.clear();
    a.clear();
    m.flush_run_diagnostics(&mut a);
    assert!(a.iter().any(|a| matches!(a, Action::Send { conn: 2, .. })));
}
