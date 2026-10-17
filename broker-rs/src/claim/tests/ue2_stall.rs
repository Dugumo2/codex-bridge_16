//! 上游报错第二批 C1：无进展期限、访问报错即停与停止诊断（契约第 2、3、4.3 节）。
use super::*;
use crate::runs::upstream_error::{UpstreamError, UpstreamErrorSource};
use crate::runs::{
    ChildRunRef, InterruptReason, RunState, RunStatus, StallPhase, StallVerify, StopDiagSend,
    StopRpcOutcome,
};
use crate::test_support::status_read::{status_requests, turns_page, StatusRequest};

const RUN: &str = "run-测试-000001";
const TIMEOUT: u64 = 3_000;

fn policy_params(timeout_ms: u64, policy: &str) -> Value {
    json!({"threadId":"线程","input":[{"type":"text","text":"本次输入"}],
        "cxbRunPolicy":{"stallTimeoutMs":timeout_ms,"upstreamErrorPolicy":policy}})
}

/// 带策略开轮并拿到上游轮号；返回开轮期间的全部动作。
fn running_with(shared: bool, timeout_ms: u64, policy: &str) -> (Machine, Vec<Action>) {
    let (mut m, context) = start_params(shared, "turn/start", policy_params(timeout_ms, policy));
    m.live_conns.insert(1);
    let actions = reply(&mut m, &context, Ok(json!({"turn": {"id": "本轮"}})));
    (m, actions)
}

fn stall_running() -> Machine {
    running_with(false, TIMEOUT, "observe").0
}

fn advance(m: &mut Machine, ms: u64) {
    m.clock.mono_ms += ms;
    m.clock.now_ms += ms;
}

/// 让当前代际的无进展计时到点。
fn fire(m: &mut Machine) -> Vec<Action> {
    let generation = m.runs.stall_states[RUN].generation;
    apply(
        m,
        Event::Timer {
            token: ClaimToken {
                thread_id: "线程".into(),
                claim_id: 0,
            },
            kind: TimerKind::Stall {
                run_id: RUN.into(),
                generation,
            },
        },
    )
}

fn stall_delays(actions: &[Action]) -> Vec<u64> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Schedule {
                kind: TimerKind::Stall { .. },
                delay_ms,
                ..
            } => Some(*delay_ms),
            _ => None,
        })
        .collect()
}

fn verify_read(actions: &[Action]) -> StatusRequest {
    status_requests(actions)
        .into_iter()
        .find(|r| r.method == "thread/turns/list")
        .unwrap_or_else(|| panic!("应发出核实读取：{actions:?}"))
}

fn answer(m: &mut Machine, request: &StatusRequest, turns: &[(&str, &str)]) -> Vec<Action> {
    reply(m, &request.context, Ok(turns_page(turns, None)))
}

fn delta(text: &str) -> Event {
    notify(
        "item/agentMessage/delta",
        json!({"threadId":"线程","turnId":"本轮","itemId":"消息","delta":text}),
    )
}

fn stall(m: &Machine) -> crate::runs::StallReport {
    m.runs.runs[RUN].stall.clone().expect("应有无进展诊断")
}

fn interrupts(actions: &[Action]) -> usize {
    actions
        .iter()
        .filter(|a| matches!(a, Action::Request(c) if c.method == "turn/interrupt"))
        .count()
}

/// 到期并让核实读到「轮次仍在进行」，返回停止阶段的动作。
fn expire_in_progress(m: &mut Machine) -> Vec<Action> {
    advance(m, TIMEOUT);
    let a = fire(m);
    let read = verify_read(&a);
    answer(m, &read, &[("本轮", "inProgress")])
}

#[test]
fn 基准未前移则到期核实_有进展则按新基准重排() {
    let mut m = stall_running();
    let state = &m.runs.stall_states[RUN];
    assert!(state.timer_scheduled && state.timeout_ms == TIMEOUT);
    advance(&mut m, 1_000);
    let a = apply(&mut m, delta("片段"));
    assert!(stall_delays(&a).is_empty(), "进展只前移基准，不重排计时");
    assert_eq!(m.runs.runs[RUN].last_progress_at, Some(m.clock.now_ms));
    advance(&mut m, 2_000);
    let a = fire(&mut m);
    assert_eq!(stall_delays(&a), vec![1_000]);
    assert!(status_requests(&a).is_empty());
    assert!(m.runs.runs[RUN].stall.is_none());
    advance(&mut m, 1_000);
    let a = fire(&mut m);
    let read = verify_read(&a);
    assert_eq!(read.params.as_ref().unwrap()["threadId"], "线程");
    let report = stall(&m);
    assert_eq!(report.phase, StallPhase::Verifying);
    assert_eq!((report.idle_ms, report.expiries), (TIMEOUT, 1));
    assert_eq!(report.verify, None);
}

#[test]
fn 过期代际的到点直接忽略() {
    let mut m = stall_running();
    advance(&mut m, TIMEOUT);
    let generation = m.runs.stall_states[RUN].generation;
    let token = m.token("线程").unwrap();
    let a = apply(
        &mut m,
        Event::Timer {
            token,
            kind: TimerKind::Stall {
                run_id: RUN.into(),
                generation: generation - 1,
            },
        },
    );
    assert!(status_requests(&a).is_empty());
    assert!(m.runs.runs[RUN].stall.is_none());
    assert!(m.runs.stall_states[RUN].timer_scheduled);
}

#[test]
fn 期限为零或没有策略都不计时() {
    let (m, _) = running_with(false, 0, "observe");
    assert!(!m.runs.stall_states.contains_key(RUN));
    let (mut m, _) = running();
    assert!(!m.runs.stall_states.contains_key(RUN));
    // 旧客户端仍走第一批进展语义，C1 去重不接管。
    m.clock.now_ms = 77;
    apply(
        &mut m,
        notify(
            "item/started",
            json!({"threadId":"线程","turnId":"本轮","item":{"id":"条目","type":"agentMessage","text":""}}),
        ),
    );
    assert_eq!(m.runs.runs[RUN].last_progress_at, Some(77));
    assert!(!m.runs.progress_dedup.contains_key(RUN));
}

#[test]
fn joined定案撤销计时_owned定案重置基准() {
    let mut m = stall_running();
    let mut a = vec![];
    m.run_settled(RUN, true, &mut a);
    assert!(!m.runs.stall_states.contains_key(RUN));
    assert!(a
        .iter()
        .any(|a| matches!(a, Action::Unschedule { key: TimerKey::Stall(run) } if run == RUN)));
    let mut m = stall_running();
    advance(&mut m, 2_500);
    let mut a = vec![];
    m.stall_owned_settled(RUN, &mut a);
    assert_eq!(m.runs.stall_states[RUN].baseline_mono, m.clock.mono_ms);
    assert_eq!(stall_delays(&a), vec![TIMEOUT]);
}

#[test]
fn settling中到点推迟五秒再判且同一次到期不重复计数() {
    let mut m = stall_running();
    let pending = m.claims.get_mut("线程").unwrap();
    pending.state = State::Starting {
        owner: pending.owner().unwrap().clone(),
        req: ReqKind::TurnStart,
        turn_id: Some("本轮".into()),
        cont: None,
        settling: true,
        undecided: None,
        submitted_at: 0,
        held: vec![],
    };
    advance(&mut m, TIMEOUT);
    let a = fire(&mut m);
    assert_eq!(stall_delays(&a), vec![crate::runs::stall::STALL_DEFER_MS]);
    assert!(status_requests(&a).is_empty());
    assert_eq!(
        (stall(&m).phase, stall(&m).expiries),
        (StallPhase::Deferred, 1)
    );
    advance(&mut m, 5_000);
    let a = fire(&mut m);
    assert_eq!(stall_delays(&a), vec![5_000]);
    assert_eq!(
        (stall(&m).phase, stall(&m).expiries),
        (StallPhase::Deferred, 1)
    );
}

#[test]
fn 已有停止在途时到点不发起也不重排() {
    let mut m = stall_running();
    let mut a = vec![];
    m.begin_stop("线程", StopReason::Stop, false, &mut a);
    advance(&mut m, TIMEOUT);
    let a = fire(&mut m);
    assert!(status_requests(&a).is_empty());
    assert!(stall_delays(&a).is_empty());
    assert_eq!(
        m.runs.runs[RUN].interrupt_reason,
        Some(InterruptReason::Stop)
    );
}

fn approval() -> Event {
    Event::ServerRequest {
        id: json!(7),
        forward_id: "broker-1-000001".into(),
        method: "item/commandExecution/requestApproval".into(),
        params: Some(json!({"threadId":"线程","turnId":"本轮","itemId":"命令"})),
    }
}

fn resolved() -> Event {
    notify(
        "serverRequest/resolved",
        json!({"threadId":"线程","requestId":7}),
    )
}

#[test]
fn 待答在场不到期_真实解除后从解除时刻重新计时() {
    let mut m = stall_running();
    advance(&mut m, 1_000);
    apply(&mut m, approval());
    assert_eq!(m.runs.stall_states[RUN].baseline_mono, 1_000);
    advance(&mut m, 10_000);
    let a = fire(&mut m);
    assert!(status_requests(&a).is_empty(), "待答时不判到期");
    assert_eq!(m.runs.stall_states[RUN].baseline_mono, 11_000);
    advance(&mut m, 500);
    apply(&mut m, resolved());
    assert_eq!(m.runs.stall_states[RUN].baseline_mono, 11_500);
    // 重复的 resolved 不再重置。
    advance(&mut m, 500);
    apply(&mut m, resolved());
    assert_eq!(m.runs.stall_states[RUN].baseline_mono, 11_500);
    advance(&mut m, TIMEOUT - 500);
    let a = fire(&mut m);
    verify_read(&a);
}

#[test]
fn 核实期间出现进展或待答都撤销核实且迟到回包被丢弃() {
    for interject in [delta("片段"), approval()] {
        let mut m = stall_running();
        advance(&mut m, TIMEOUT);
        let a = fire(&mut m);
        let read = verify_read(&a);
        advance(&mut m, 100);
        let a = apply(&mut m, interject);
        assert!(m.runs.stall_states[RUN].verify.is_none());
        assert_eq!(
            (stall(&m).phase, stall(&m).verify),
            (StallPhase::Resumed, Some(StallVerify::Progressed))
        );
        assert!(!stall_delays(&a).is_empty() || m.runs.stall_states[RUN].timer_scheduled);
        let a = answer(&mut m, &read, &[("本轮", "inProgress")]);
        assert_eq!(interrupts(&a), 0);
        assert_eq!(m.runs.runs[RUN].state, RunState::Running);
    }
}

#[test]
fn 核实读到新终态交既有收尾且不重复重置() {
    let mut m = stall_running();
    advance(&mut m, TIMEOUT);
    let a = fire(&mut m);
    let read = verify_read(&a);
    let a = answer(&mut m, &read, &[("本轮", "completed")]);
    assert_eq!(interrupts(&a), 0);
    let record = &m.runs.runs[RUN];
    assert_eq!(record.state, RunState::Ended);
    assert_eq!(record.status, Some(RunStatus::Completed));
    assert_eq!(record.interrupt_reason, None);
    assert!(
        !m.runs.stall_states.contains_key(RUN),
        "收尾释放后不重建计时"
    );
    // 同一结束证据再次送达不改动运行。
    let a = answer(&mut m, &read, &[("本轮", "completed")]);
    assert!(stall_delays(&a).is_empty());
}

#[test]
fn 四轮_核实终态立即释放仍提交正确诊断且不重排() {
    for status in ["completed", "failed", "interrupted"] {
        let mut m = stall_running();
        m.cxb_conns.insert(1);
        advance(&mut m, TIMEOUT);
        let read = verify_read(&fire(&mut m));
        let actions = answer(&mut m, &read, &[("本轮", status)]);
        assert_eq!(m.runs.runs[RUN].state, RunState::Ended);
        assert_eq!(stall(&m).phase, StallPhase::Resumed);
        assert_eq!(stall(&m).verify, Some(StallVerify::TurnEnded));
        assert!(!m.runs.stall_states.contains_key(RUN));
        // 收尾内的进展可能先排程、随后撤销；提交后最后一条计时动作必须是撤销。
        let last_timer = actions.iter().rev().find(|a| {
            matches!(a,
            Action::Schedule { kind: TimerKind::Stall { run_id, .. }, .. } if run_id == RUN)
                || matches!(a, Action::Unschedule { key: TimerKey::Stall(run) } if run == RUN)
        });
        assert!(matches!(last_timer, Some(Action::Unschedule { .. })));
        assert!(actions.iter().any(|a| matches!(a, Action::Store(crate::store::StoreOp::PutRun(r))
            if r.run_id == RUN && r.stall.as_ref().is_some_and(|s| s.phase == StallPhase::Resumed && s.verify == Some(StallVerify::TurnEnded)))));
        assert!(actions.iter().any(|a| matches!(a, Action::Send { message, .. }
            if message["method"] == "broker/run/updated" && message["params"]["stall"]["phase"] == "resumed"
            && message["params"]["stall"]["verify"] == "turnEnded")));
    }
}

#[test]
fn 轮次仍在进行或读取失败都按无进展停止() {
    let mut m = stall_running();
    let a = expire_in_progress(&mut m);
    assert_eq!(interrupts(&a), 1);
    let record = &m.runs.runs[RUN];
    assert_eq!(record.interrupt_reason, Some(InterruptReason::StallTimeout));
    assert_eq!(record.state, RunState::Stopping);
    let report = stall(&m);
    assert_eq!(
        (report.phase, report.verify),
        (StallPhase::Stopping, Some(StallVerify::InProgress))
    );
    let diag = record.stop_diagnostics.clone().unwrap();
    assert_eq!(diag.reason, InterruptReason::StallTimeout);
    assert!(diag.interrupt_sent && diag.interrupt_sent_at.is_some());

    let mut m = stall_running();
    advance(&mut m, TIMEOUT);
    let a = fire(&mut m);
    let read = verify_read(&a);
    let a = reply(
        &mut m,
        &read.context,
        Err(json!({"code":-32000,"message":"读取失败"})),
    );
    assert_eq!(interrupts(&a), 1);
    assert_eq!(stall(&m).verify, Some(StallVerify::Unknown));
}

#[test]
fn 没有活动父轮时直接absent停止且不用历史轮号() {
    let mut m = stall_running();
    // 父轮已结束但认领仍保留旧轮号（同 Converging 保留已完成父轮的编号）。
    m.records
        .get_mut("线程")
        .unwrap()
        .ended_turns
        .push_back("本轮".into());
    advance(&mut m, TIMEOUT);
    fire(&mut m);
    assert_eq!(stall(&m).verify, Some(StallVerify::Absent));
    assert_eq!(stall(&m).phase, StallPhase::Stopping);
    assert_eq!(
        m.runs.runs[RUN].interrupt_reason,
        Some(InterruptReason::StallTimeout)
    );
    assert!(m
        .runs
        .stall_states
        .get(RUN)
        .is_none_or(|s| s.verify.is_none()));
}

#[test]
fn 停止前复核不通过只记日志() {
    let mut m = stall_running();
    advance(&mut m, TIMEOUT);
    let a = fire(&mut m);
    let read = verify_read(&a);
    let mut b = vec![];
    m.begin_stop("线程", StopReason::Stop, false, &mut b);
    let a = answer(&mut m, &read, &[("本轮", "inProgress")]);
    assert!(a
        .iter()
        .any(|a| matches!(a, Action::Debug(t) if t.contains("不发停止"))));
    assert_eq!(
        m.runs.runs[RUN].interrupt_reason,
        Some(InterruptReason::Stop)
    );
}

#[test]
fn 用户停止先于到期时原因保持stop() {
    let mut m = stall_running();
    let mut a = vec![];
    m.begin_stop("线程", StopReason::Stop, false, &mut a);
    expire_attempt(&mut m);
    // 停止已在途：不发起无进展核实，也不写到期诊断。
    assert!(m.runs.stall_states[RUN].verify.is_none());
    assert!(m.runs.runs[RUN].stall.is_none());
    assert_eq!(
        m.runs.runs[RUN].stop_diagnostics.as_ref().unwrap().reason,
        InterruptReason::Stop
    );
}

fn expire_attempt(m: &mut Machine) -> Vec<Action> {
    advance(m, TIMEOUT);
    fire(m)
}

#[test]
fn 共享模式的owned运行同样计时并停止() {
    let (mut m, _) = running_with(true, TIMEOUT, "observe");
    if m.claims["线程"].settling() {
        let mut a = vec![];
        let claim = m.claims.get_mut("线程").unwrap();
        claim.state = State::Running {
            owner: claim.owner().unwrap().clone(),
            turn_id: "本轮".into(),
            joined: false,
        };
        m.run_settled(RUN, false, &mut a);
        m.stall_owned_settled(RUN, &mut a);
    }
    // 夹具显式定案为 owned 后，也要补齐真实定案路径登记的自有轮，才能核实活动目标。
    m.claims.get_mut("线程").unwrap().pending_start = None;
    m.remember_owned_turn("线程", "本轮");
    m.run_turn_started(RUN, "线程", "本轮", &mut vec![]);
    assert_eq!(
        m.diagnostic_run("线程", "本轮", false).as_deref(),
        Some(RUN)
    );
    assert!(m.runs.stall_states.contains_key(RUN));
    let actions = expire_in_progress(&mut m);
    assert_eq!(interrupts(&actions), 1);
    assert_eq!(m.runs.runs[RUN].state, RunState::Stopping);
    assert_eq!(
        m.runs.runs[RUN].interrupt_reason,
        Some(InterruptReason::StallTimeout)
    );
}

#[test]
fn 释放与关闭都撤销计时并清除状态() {
    let mut m = stall_running();
    let a = apply(&mut m, turn("turn/completed", "本轮"));
    assert_eq!(m.runs.runs[RUN].state, RunState::Ended);
    assert!(!m.runs.stall_states.contains_key(RUN));
    assert!(!m.runs.progress_dedup.contains_key(RUN));
    assert!(a
        .iter()
        .any(|a| matches!(a, Action::Unschedule { key: TimerKey::Stall(run) } if run == RUN)));
    let mut m = stall_running();
    let mut a = vec![];
    m.stall_shutdown(&mut a);
    assert!(m.runs.stall_states.is_empty());
}

#[test]
fn review迁移使在途核实失效且基准保留() {
    let mut m = stall_running();
    advance(&mut m, TIMEOUT);
    let a = fire(&mut m);
    let read = verify_read(&a);
    let baseline = m.runs.stall_states[RUN].baseline_mono;
    let generation = m.runs.stall_states[RUN].generation;
    let mut a = vec![];
    m.stall_review_moved(RUN, &mut a);
    let state = &m.runs.stall_states[RUN];
    assert!(state.verify.is_none());
    assert!(state.generation > generation);
    assert_eq!(state.baseline_mono, baseline);
    assert!(!m.status_reads.reads.contains_key(&read.read_id));
    assert_eq!(stall_delays(&a), vec![0], "已超期按新执行线程立即重新到点");
}

/// 进展是否计入：比较前后的 lastProgressAt（每次推进时钟）。
fn counts(m: &mut Machine, method: &str, params: Value) -> bool {
    advance(m, 10);
    let before = m.runs.runs[RUN].last_progress_at;
    apply(m, notify(method, params));
    m.runs.runs[RUN].last_progress_at != before
}

#[test]
fn 各类进展按类别去重() {
    let mut m = stall_running();
    let item = json!({"threadId":"线程","turnId":"本轮","item":{"id":"条目","type":"agentMessage","text":""}});
    assert!(counts(&mut m, "item/started", item.clone()));
    assert!(!counts(&mut m, "item/started", item.clone()));
    assert!(counts(&mut m, "item/completed", item));
    let empty = json!({"threadId":"线程","turnId":"本轮","itemId":"条目","delta":""});
    assert!(!counts(&mut m, "item/agentMessage/delta", empty));
    let diff = json!({"threadId":"线程","turnId":"本轮","diff":"甲"});
    assert!(counts(&mut m, "turn/diff/updated", diff.clone()));
    assert!(!counts(&mut m, "turn/diff/updated", diff));
    assert!(counts(
        &mut m,
        "turn/diff/updated",
        json!({"threadId":"线程","turnId":"本轮","diff":"乙"})
    ));
    let usage = |total: u64| json!({"threadId":"线程","turnId":"本轮","tokenUsage":{"total":{"totalTokens":total},"last":{"totalTokens":1}}});
    assert!(counts(&mut m, "thread/tokenUsage/updated", usage(10)));
    assert!(!counts(&mut m, "thread/tokenUsage/updated", usage(10)));
    assert!(!counts(&mut m, "thread/tokenUsage/updated", usage(9)));
    assert!(counts(&mut m, "thread/tokenUsage/updated", usage(11)));
    let hook = json!({"threadId":"线程","turnId":"本轮","run":{"id":"钩子1"}});
    assert!(counts(&mut m, "hook/started", hook.clone()));
    assert!(!counts(&mut m, "hook/started", hook));
    assert!(!counts(
        &mut m,
        "hook/started",
        json!({"threadId":"线程","turnId":null,"run":{"id":"钩子2"}})
    ));
}

#[test]
fn 报错警告与认证恢复都不算进展() {
    let mut m = stall_running();
    for (method, params) in [
        (
            "error",
            json!({"threadId":"线程","turnId":"本轮","willRetry":true,"error":{"message":"重试中"}}),
        ),
        (
            "warning",
            json!({"threadId":"线程","turnId":"本轮","message":"警告"}),
        ),
        (
            "modelProvider/authRecoveryStarted",
            json!({"threadId":"线程","turnId":"本轮"}),
        ),
    ] {
        assert!(!counts(&mut m, method, params), "{method} 不应算进展");
    }
    assert_eq!(m.runs.stall_states[RUN].baseline_mono, 0);
}

#[test]
fn 子线程进展经归属查询计入父运行() {
    let mut m = stall_running();
    m.runs.child_activity_override = Some(ChildRunRef {
        run_id: RUN.into(),
        parent_thread_id: "线程".into(),
    });
    advance(&mut m, 1_000);
    apply(
        &mut m,
        notify(
            "item/agentMessage/delta",
            json!({"threadId":"子线程","turnId":"子轮","itemId":"子消息","delta":"子输出"}),
        ),
    );
    assert_eq!(m.runs.stall_states[RUN].baseline_mono, 1_000);
    assert_eq!(m.runs.runs[RUN].last_progress_at, Some(m.clock.now_ms));
}

fn access_error(info: &str) -> Event {
    notify(
        "error",
        json!({"threadId":"线程","turnId":"本轮","willRetry":true,
            "error":{"message":"访问被拒","codexErrorInfo":info}}),
    )
}

#[test]
fn 访问报错即停只对三类结构化错误触发一次() {
    for info in [
        json!("unauthorized"),
        json!({"httpConnectionFailed":{"httpStatusCode":402}}),
        json!({"responseStreamDisconnected":{"httpStatusCode":403}}),
    ] {
        let (mut m, _) = running_with(false, 0, "stopAccess");
        let event = notify(
            "error",
            json!({"threadId":"线程","turnId":"本轮","willRetry":true,
                "error":{"message":"访问被拒","codexErrorInfo":info}}),
        );
        let a = apply(&mut m, event.clone());
        assert_eq!(interrupts(&a), 1, "{info}");
        assert_eq!(
            m.runs.runs[RUN].interrupt_reason,
            Some(InterruptReason::UpstreamAccessError)
        );
        let a = apply(&mut m, event);
        assert_eq!(interrupts(&a), 0, "同一运行只触发一次");
    }
    // 其他类别、observe 均不触发。
    let (mut m, _) = running_with(false, 0, "stopAccess");
    let a = apply(&mut m, access_error("usageLimitExceeded"));
    assert_eq!(interrupts(&a), 0);
    let (mut m, _) = running_with(false, 0, "observe");
    let a = apply(&mut m, access_error("unauthorized"));
    assert_eq!(interrupts(&a), 0);
    assert_eq!(m.runs.runs[RUN].state, RunState::Running);
}

#[test]
fn 访问报错即停不接受rpc回包来源() {
    let (mut m, _) = running_with(false, 0, "stopAccess");
    let error = UpstreamError::from_error(
        "线程",
        Some("本轮"),
        UpstreamErrorSource::RpcResponse,
        &json!({"message":"未授权","codexErrorInfo":"unauthorized"}),
        None,
        1,
    );
    let mut a = vec![];
    m.upstream_access_check(RUN, &error, &mut a);
    assert_eq!(m.runs.runs[RUN].state, RunState::Running);
    assert!(!m.runs.access_stopped.contains(RUN));
}

fn send(m: &Machine, turn: Option<&str>) -> StopDiagSend {
    StopDiagSend {
        stop_generation: m.runs.stop_diag_sends[RUN].stop_generation,
        turn: turn.map(str::to_owned),
        thread: "线程".into(),
    }
}

#[test]
fn 停止诊断记录各阶段且新发送清空回包() {
    let mut m = stall_running();
    m.clock.now_ms = 100;
    let mut a = vec![];
    m.begin_stop("线程", StopReason::Stop, false, &mut a);
    let s = send(&m, Some("本轮"));
    m.clock.now_ms = 200;
    m.stop_diag_goal_started(RUN, &s);
    m.stop_diag_goal_settled(RUN, &s);
    m.stop_diag_interrupt_sent(RUN, &s);
    m.clock.now_ms = 300;
    m.stop_diag_interrupt_reply(RUN, &s, StopRpcOutcome::Timeout);
    let d = m.runs.runs[RUN].stop_diagnostics.clone().unwrap();
    assert_eq!(d.started_at, 100);
    assert_eq!(
        (d.goal_started_at, d.goal_settled_at),
        (Some(200), Some(200))
    );
    assert_eq!(
        (d.interrupt_reply, d.interrupt_replied_at),
        (Some(StopRpcOutcome::Timeout), Some(300))
    );
    m.clock.now_ms = 400;
    m.stop_diag_interrupt_sent(RUN, &s);
    let d = m.runs.runs[RUN].stop_diagnostics.clone().unwrap();
    assert_eq!(d.interrupt_sent_at, Some(400));
    assert_eq!((d.interrupt_reply, d.interrupt_replied_at), (None, None));
    // 别的停止代际与迁移前线程的回包都不写。
    let mut old = s.clone();
    old.stop_generation += 99;
    m.stop_diag_interrupt_reply(RUN, &old, StopRpcOutcome::Ok);
    let mut moved = s.clone();
    moved.thread = "旧执行线程".into();
    m.stop_diag_interrupt_reply(RUN, &moved, StopRpcOutcome::Ok);
    assert_eq!(
        m.runs.runs[RUN]
            .stop_diagnostics
            .as_ref()
            .unwrap()
            .interrupt_reply,
        None
    );
}

#[test]
fn 未发interrupt与已发无完成两种未确认可区分() {
    // 未发 interrupt：goal 读取一直不回。
    let (mut m, context) = start_params(false, "turn/start", policy_params(0, "observe"));
    let mut a = vec![];
    reply(&mut m, &context, Ok(json!({"turn": {"id": "本轮"}})));
    m.begin_stop("线程", StopReason::Stop, false, &mut a);
    m.stop_diag_unconfirmed(RUN);
    let d = m.runs.runs[RUN].stop_diagnostics.clone().unwrap();
    assert!(!d.interrupt_sent && d.unconfirmed_at.is_some());
    // 已发 interrupt 而没有轮次结束证据。
    let mut m = stall_running();
    let a = expire_in_progress(&mut m);
    assert_eq!(interrupts(&a), 1);
    m.stop_diag_unconfirmed(RUN);
    let d = m.runs.runs[RUN].stop_diagnostics.clone().unwrap();
    assert!(d.interrupt_sent && d.turn_ended_at.is_none() && d.unconfirmed_at.is_some());
    assert!(d.confirmed_at.is_none());
}

#[test]
fn 四轮_停止代际或执行线程迁移清除目标证据但保留运行历史() {
    for migrate in [false, true] {
        let mut m = stall_running();
        m.clock.now_ms = 100;
        m.begin_stop("线程", StopReason::Stop, false, &mut vec![]);
        let old = send(&m, Some("本轮"));
        m.stop_diag_interrupt_sent(RUN, &old);
        m.stop_diag_interrupt_reply(RUN, &old, StopRpcOutcome::Ok);
        m.stop_diag_rpc_issue(
            RUN,
            &old,
            "thread/goal/get",
            StopRpcOutcome::Error,
            "旧问题",
        );
        m.stop_diag_turn_ended(RUN, &old);
        let before = m.runs.runs[RUN].stop_diagnostics.clone().unwrap();
        let mut new = old.clone();
        if migrate {
            new.thread = "审查执行线程".into();
            m.runs.runs.get_mut(RUN).unwrap().review_thread_id = Some(new.thread.clone());
        } else {
            new.stop_generation += 1;
        }
        m.stop_diag_began(RUN, InterruptReason::StallTimeout, new.stop_generation);
        let changed = m.runs.runs[RUN].stop_diagnostics.as_ref().unwrap();
        assert_eq!(
            (
                changed.turn_ended_at,
                changed.interrupt_reply,
                changed.interrupt_replied_at
            ),
            (None, None, None)
        );
        assert_eq!(
            (changed.started_at, changed.reason),
            (before.started_at, before.reason)
        );
        assert_eq!(changed.rpc_issues, before.rpc_issues);
        m.clock.now_ms = 200;
        m.stop_diag_interrupt_sent(RUN, &new);
        m.stop_diag_turn_ended(RUN, &old);
        assert_eq!(
            m.runs.runs[RUN]
                .stop_diagnostics
                .as_ref()
                .unwrap()
                .turn_ended_at,
            None
        );
        m.stop_diag_turn_ended(RUN, &new);
        assert_eq!(
            m.runs.runs[RUN]
                .stop_diagnostics
                .as_ref()
                .unwrap()
                .turn_ended_at,
            Some(200)
        );
    }
}

#[test]
fn 停止确认后不再写未确认且rpc问题最多保留八条() {
    let mut m = stall_running();
    let mut a = vec![];
    m.begin_stop("线程", StopReason::Stop, false, &mut a);
    let s = send(&m, Some("本轮"));
    for i in 0..10 {
        m.clock.now_ms = 1_000 + i;
        m.stop_diag_rpc_issue(
            RUN,
            &s,
            "turn/interrupt",
            StopRpcOutcome::Error,
            &format!("第{i}次\n失败 Authorization: Bearer abcdefghijklmnop"),
        );
    }
    m.stop_diag_rpc_issue(RUN, &s, "turn/interrupt", StopRpcOutcome::Ok, "成功");
    let d = m.runs.runs[RUN].stop_diagnostics.clone().unwrap();
    assert_eq!(d.rpc_issues.len(), 8);
    assert_eq!(d.rpc_issues[0].at, 1_002);
    assert!(d.rpc_issues.iter().all(|i| !i.message.contains('\n')
        && !i.message.contains("abcdefghijklmnop")
        && i.message.len() <= 300));
    m.stop_diag_confirmed(RUN);
    m.stop_diag_unconfirmed(RUN);
    let d = m.runs.runs[RUN].stop_diagnostics.clone().unwrap();
    assert!(d.confirmed_at.is_some() && d.unconfirmed_at.is_none());
}

#[test]
fn 历史停止不新建诊断() {
    let mut m = stall_running();
    apply(&mut m, turn("turn/completed", "本轮"));
    assert_eq!(m.runs.runs[RUN].state, RunState::Ended);
    m.stop_diag_began(RUN, InterruptReason::Stop, 9);
    m.stop_diag_confirmed(RUN);
    assert!(m.runs.runs[RUN].stop_diagnostics.is_none());
    let mut a = vec![];
    m.begin_stop("线程", StopReason::Stop, false, &mut a);
    assert!(m.runs.runs[RUN].stop_diagnostics.is_none());
}

#[test]
fn 停止诊断变化在同一事件只增版一次() {
    let mut m = stall_running();
    m.records.entry("线程".into()).or_default().watchers.push(1);
    m.cxb_conns.insert(1);
    let before = m.runs.runs[RUN].diagnostic_revision;
    advance(&mut m, TIMEOUT);
    let a = fire(&mut m);
    let read = verify_read(&a);
    let notices = |a: &[Action]| {
        a.iter()
            .filter(|a| matches!(a, Action::Send { message, .. } if message["method"] == "broker/run/updated"))
            .count()
    };
    assert_eq!(notices(&a), 1);
    assert_eq!(m.runs.runs[RUN].diagnostic_revision, before + 1);
    // 核实结论、停止发起、停止诊断在同一事件里变化，仍只增版一次、通知一次。
    let (next, a) = step(
        std::mem::take(&mut m),
        Event::Reply {
            context: read.context.clone(),
            result: Ok(turns_page(&[("本轮", "inProgress")], None)),
        },
    );
    m = next;
    assert_eq!(notices(&a), 1);
    assert_eq!(m.runs.runs[RUN].diagnostic_revision, before + 2);
    assert!(m.runs.runs[RUN].stop_diagnostics.is_some());
}
