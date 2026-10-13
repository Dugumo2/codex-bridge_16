use super::*;
use crate::runs::{ChildRunRef, RunState, UpstreamErrorPolicy};

#[test]
fn 统一诊断在启动rpc错误回包前发送() {
    let (mut m, context) = start(false, "turn/start");
    m.live_conns.insert(1);
    m.cxb_conns.insert(1);
    let actions = reply(
        &mut m,
        &context,
        Err(json!({"code":-32602,"message":"不支持审查模式"})),
    );
    let diagnostic = actions.iter().position(|a| matches!(a, Action::Send {conn:1,message} if message["method"] == "broker/run/updated")).unwrap();
    let rpc_error = actions.iter().position(|a| matches!(a, Action::Send {conn:1,message} if message["id"] == 42 && message.get("error").is_some())).unwrap();
    assert!(diagnostic < rpc_error);
}

#[test]
fn 核实得到父轮真实终态只收尾一次且不伪造上游通知() {
    for (status, wire, expected) in [
        (
            status_read::StatusTurn::Completed,
            "completed",
            crate::runs::RunStatus::Completed,
        ),
        (
            status_read::StatusTurn::Failed,
            "failed",
            crate::runs::RunStatus::Failed,
        ),
        (
            status_read::StatusTurn::Interrupted,
            "interrupted",
            crate::runs::RunStatus::Interrupted,
        ),
    ] {
        let (mut m, _) = running();
        let token = m.token("线程").unwrap();
        let shell = json!({"id":"本轮","status":wire,"items":[]});
        let mut a = vec![];
        let mut stale = token.clone();
        stale.claim_id += 1;
        assert!(!m.commit_verified_turn_end("run-测试-000001", &stale, status, &shell, &mut a));
        assert!(!m.commit_verified_turn_end("其他运行", &token, status, &shell, &mut a));
        assert!(!m.commit_verified_turn_end(
            "run-测试-000001",
            &token,
            status,
            &json!({"id":"旧轮","status":wire}),
            &mut a
        ));
        assert!(m.commit_verified_turn_end("run-测试-000001", &token, status, &shell, &mut a));
        assert_eq!(m.runs.runs["run-测试-000001"].state, RunState::Ended);
        assert_eq!(m.runs.runs["run-测试-000001"].status, Some(expected));
        assert_eq!(m.runs.runs["run-测试-000001"].turns.len(), 1);
        assert!(!a.iter().any(
            |a| matches!(a,Action::Send {message,..} if message["method"] == "turn/completed")
        ));
        a.clear();
        assert!(!m.commit_verified_turn_end("run-测试-000001", &token, status, &shell, &mut a));
        assert!(a.is_empty());
    }
}

#[test]
fn 进展查询严格区分自有轮和子轮且显式策略留给去重钩子() {
    let (mut m, _) = running();
    assert!(
        matches!(m.progress_run_for("线程", "本轮", false), Some(crate::runs::ProgressTarget::Own {run_id}) if run_id == "run-测试-000001")
    );
    assert!(m.progress_run_for("线程", "旧轮", false).is_none());
    assert!(m.progress_run_for("其他线程", "本轮", false).is_none());
    m.runs.child_activity_override = Some(ChildRunRef {
        run_id: "子归属运行".into(),
        parent_thread_id: "父线程".into(),
    });
    assert!(
        matches!(m.progress_run_for("子线程", "子轮", false), Some(crate::runs::ProgressTarget::Child(child)) if child.run_id == "子归属运行")
    );
    m.runs.child_activity_override = None;
    let record = m.runs.runs.get_mut("run-测试-000001").unwrap();
    record.run_policy = Some(crate::runs::RunPolicy {
        stall_timeout_ms: 600_000,
        upstream_error_policy: UpstreamErrorPolicy::Observe,
    });
    let before = record.last_progress_at;
    m.clock.now_ms = 1000;
    m.run_diagnostic_event("线程", "本轮", false, None, true, &mut vec![]);
    assert_eq!(m.runs.runs["run-测试-000001"].last_progress_at, before);
}

#[test]
fn 运行策略合法参数落盘且删除后不发上游() {
    for (method, timeout, policy) in [
        ("turn/start", 0, "observe"),
        ("review/start", 600_000, "stopAccess"),
    ] {
        let params = json!({"threadId":"线程", "cxbRunPolicy":{"stallTimeoutMs":timeout,"upstreamErrorPolicy":policy}});
        let (m, context) = start_params(false, method, params);
        assert!(context
            .params
            .as_ref()
            .unwrap()
            .get("cxbRunPolicy")
            .is_none());
        let r = &m.runs.runs["run-测试-000001"];
        assert_eq!(r.run_policy.as_ref().unwrap().stall_timeout_ms, timeout);
        assert_eq!(
            r.run_policy.as_ref().unwrap().upstream_error_policy,
            if policy == "observe" {
                UpstreamErrorPolicy::Observe
            } else {
                UpstreamErrorPolicy::StopAccess
            }
        );
        assert!(r
            .execution_config
            .as_ref()
            .and_then(|c| c.turn_params.as_ref())
            .is_none_or(|p| p.get("cxbRunPolicy").is_none()));
    }
    let (m, _) = start(false, "turn/start");
    assert!(m.runs.runs["run-测试-000001"].run_policy.is_none());
}

#[test]
fn 运行策略缺字段类型错误负数未知策略和非安全整数都拒绝() {
    for value in [
        Value::Null,
        json!({}),
        json!({"stallTimeoutMs":0}),
        json!({"upstreamErrorPolicy":"observe"}),
        json!({"stallTimeoutMs":"600","upstreamErrorPolicy":"observe"}),
        json!({"stallTimeoutMs":-1,"upstreamErrorPolicy":"observe"}),
        json!({"stallTimeoutMs":1.5,"upstreamErrorPolicy":"observe"}),
        json!({"stallTimeoutMs":9_007_199_254_740_992u64,"upstreamErrorPolicy":"observe"}),
        json!({"stallTimeoutMs":0,"upstreamErrorPolicy":"unknown"}),
    ] {
        let mut m = Machine::new(false);
        m.record("线程").subscribed = true;
        let a = apply(
            &mut m,
            client(request(
                1,
                42,
                "turn/start",
                json!({"threadId":"线程","cxbRunPolicy":value}),
            )),
        );
        assert!(!a
            .iter()
            .any(|a| matches!(a,Action::Request(c) if c.method=="turn/start")));
        let error = sends(&a)
            .into_iter()
            .find(|(_, v)| v["id"] == 42)
            .unwrap()
            .1;
        assert_eq!(error["error"]["code"], -32602);
        assert_eq!(
            error["error"]["message"],
            "cxbRunPolicy 必须是有效的运行策略"
        );
        assert_eq!(m.runs.runs["run-测试-000001"].state, RunState::Ended);
    }
}

#[test]
fn review未归属时已绑定子线程先分流且回放不重复() {
    let (mut m, _) = start_legacy_review(false);
    m.runs.child_activity_override = Some(ChildRunRef {
        run_id: "父运行".into(),
        parent_thread_id: "父线程".into(),
    });
    let params = json!({"threadId":"已绑定子","turnId":"子轮","willRetry":true,"error":{"message":"认证失败","codexErrorInfo":"unauthorized"}});
    apply(&mut m, notify("error", params.clone()));
    assert!(!m.unclaimed.contains_key("已绑定子"));
    assert_eq!(m.runs.child_event_log.len(), 1);
    m.commit_notification(
        "已绑定子",
        HeldNotification {
            method: "error".into(),
            params: Some(params),
            committed: true,
        },
        &mut vec![],
    );
    assert_eq!(m.runs.child_event_log.len(), 1);
    m.runs.child_activity_override = None;
    apply(
        &mut m,
        notify(
            "error",
            json!({"threadId":"未知review","turnId":"轮","willRetry":true,"error":{"message":"待归属"}}),
        ),
    );
    assert!(m.unclaimed.contains_key("未知review"));
}
