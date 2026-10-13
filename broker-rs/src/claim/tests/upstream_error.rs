//! 上游诊断只记当前自有执行轮，覆盖早到回放、持久化和扩展接收者。

use super::*;
use crate::runs::upstream_error::{UpstreamErrorCategory, UpstreamErrorSource};
use crate::runs::{RunRecord, RunState};
use crate::store::StoreOp;

const RUN: &str = "run-测试-000001";

fn connected(machine: &mut Machine, conn: ConnId, cxb: bool) {
    apply(machine, Event::Connected { conn });
    apply(machine, Event::Initialized { conn, cxb });
}

fn upstream(thread: &str, turn: &str, retry: bool) -> Event {
    notify(
        "error",
        json!({
            "threadId":thread,"turnId":turn,"willRetry":retry,
            "error":{"message":"Reconnecting... 2/5","additionalDetails":"认证失败 Bearer secret-token",
                "codexErrorInfo":{"responseStreamDisconnected":{"httpStatusCode":401}}}
        }),
    )
}

fn updates(actions: &[Action]) -> Vec<(ConnId, Value)> {
    sends(actions)
        .into_iter()
        .filter(|(_, message)| message["method"] == "broker/run/updated")
        .collect()
}

fn puts(actions: &[Action]) -> Vec<&RunRecord> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::Store(StoreOp::PutRun(record)) => Some(record.as_ref()),
            _ => None,
        })
        .collect()
}

#[test]
fn 上游报错当前轮立即写盘增版且终止错误仍保持认领() {
    let (mut machine, _) = running();
    connected(&mut machine, 1, true);
    let before = machine.runs.runs[RUN].clone();
    machine.clock.now_ms = 10;
    let actions = apply(&mut machine, upstream("线程", "本轮", true));
    let record = &machine.runs.runs[RUN];
    assert_eq!(record.diagnostic_revision, before.diagnostic_revision + 1);
    assert_eq!(record.last_progress_at, before.last_progress_at);
    assert_eq!(record.upstream_error.as_ref().unwrap().count, 1);
    assert_eq!(
        record.upstream_error.as_ref().unwrap().category,
        UpstreamErrorCategory::Authentication
    );
    assert_eq!(puts(&actions).len(), 1);
    assert_eq!(puts(&actions)[0], record);
    assert_eq!(updates(&actions).len(), 1);
    let actions = apply(&mut machine, upstream("线程", "本轮", false));
    assert_eq!(
        machine.runs.runs[RUN]
            .upstream_error
            .as_ref()
            .unwrap()
            .count,
        2
    );
    assert_eq!(
        machine.runs.runs[RUN].diagnostic_revision,
        before.diagnostic_revision + 2
    );
    assert_eq!(machine.runs.runs[RUN].state, RunState::Running);
    assert_eq!(machine.claims["线程"].turn_id(), Some("本轮"));
    assert_eq!(puts(&actions).len(), 1);
    assert_eq!(
        updates(&actions)[0].1["params"]["upstreamError"]["willRetry"],
        false
    );
}

#[test]
fn 上游报错未知当前轮不接受自报轮次且子线程无认领旧轮均不记账() {
    let (mut unknown, _) = start(false, "turn/start");
    let before = unknown.runs.runs[RUN].clone();
    let actions = apply(&mut unknown, upstream("线程", "自报轮", true));
    assert_eq!(unknown.runs.runs[RUN], before);
    assert!(puts(&actions).is_empty());

    let (mut machine, _) = running();
    let before = machine.runs.runs[RUN].clone();
    for event in [
        upstream("子线程", "本轮", true),
        upstream("未认领", "本轮", true),
        upstream("线程", "旧轮", true),
    ] {
        let actions = apply(&mut machine, event);
        assert_eq!(machine.runs.runs[RUN], before);
        assert!(puts(&actions).is_empty());
    }
    for ended in [true, false] {
        let (mut machine, _) = running();
        let record = machine.records.get_mut("线程").unwrap();
        if ended {
            record.ended_turns.push_back("本轮".into());
        } else {
            record.released_turns.push_back("本轮".into());
        }
        assert!(puts(&apply(&mut machine, upstream("线程", "本轮", true))).is_empty());
        assert!(machine.runs.runs[RUN].upstream_error.is_none());
    }
}

#[test]
fn 上游报错旧轮迟到不会污染续轮或新认领() {
    let (mut machine, _) = running();
    apply(&mut machine, turn("turn/started", "新轮"));
    let before = machine.runs.runs[RUN].clone();
    apply(&mut machine, upstream("线程", "本轮", true));
    assert_eq!(machine.runs.runs[RUN], before);
    apply(&mut machine, upstream("线程", "新轮", true));
    assert_eq!(
        machine.runs.runs[RUN]
            .upstream_error
            .as_ref()
            .unwrap()
            .turn_id
            .as_deref(),
        Some("新轮")
    );
    apply(&mut machine, turn("turn/completed", "新轮"));
    let completed = machine.runs.runs[RUN].clone();
    apply(&mut machine, upstream("线程", "新轮", false));
    assert_eq!(machine.runs.runs[RUN], completed);
}

#[test]
fn 上游报错完成同组不加计数且进展与错误合并只增版一次() {
    let (mut machine, _) = running();
    connected(&mut machine, 1, true);
    apply(&mut machine, upstream("线程", "本轮", true));
    let revision = machine.runs.runs[RUN].diagnostic_revision;
    machine.clock.now_ms = 25;
    let error = match upstream("线程", "本轮", true) {
        Event::Notification {
            params: Some(params),
            ..
        } => params["error"].clone(),
        _ => unreachable!(),
    };
    let actions = apply(
        &mut machine,
        notify(
            "turn/completed",
            json!({"threadId":"线程",
        "turn":{"id":"本轮","status":"failed","items":[],"error":error}}),
        ),
    );
    let record = &machine.runs.runs[RUN];
    assert_eq!(record.upstream_error.as_ref().unwrap().count, 1);
    assert_eq!(
        record.upstream_error.as_ref().unwrap().will_retry,
        Some(false)
    );
    assert_eq!(
        record.upstream_error.as_ref().unwrap().source,
        UpstreamErrorSource::ErrorNotification
    );
    assert_eq!(record.diagnostic_revision, revision + 1);
    assert_eq!(record.last_progress_at, Some(25));
    assert_eq!(updates(&actions).len(), 1);
    let before = record.clone();
    apply(
        &mut machine,
        notify(
            "turn/completed",
            json!({"threadId":"线程",
        "turn":{"id":"本轮","status":"failed","items":[],"error":error}}),
        ),
    );
    assert_eq!(machine.runs.runs[RUN], before);
}

#[test]
fn 上游报错进展单独增版只沿用五秒写盘且查询不增版() {
    let (mut machine, _) = running();
    connected(&mut machine, 1, true);
    for (now, event) in [
        (10, turn("turn/started", "本轮")),
        (
            20,
            notify(
                "item/started",
                json!({"threadId":"线程","turnId":"本轮","item":{"id":"条目","type":"agentMessage"}}),
            ),
        ),
        (
            30,
            notify(
                "item/completed",
                json!({"threadId":"线程","turnId":"本轮",
            "item":{"id":"条目","type":"agentMessage","text":"输出"}}),
            ),
        ),
    ] {
        machine.clock.now_ms = now;
        let revision = machine.runs.runs[RUN].diagnostic_revision;
        let actions = apply(&mut machine, event);
        assert_eq!(machine.runs.runs[RUN].last_progress_at, Some(now));
        assert_eq!(machine.runs.runs[RUN].diagnostic_revision, revision + 1);
        assert!(updates(&actions).is_empty());
    }
    let before = machine.runs.runs[RUN].clone();
    apply(
        &mut machine,
        client(request(
            1,
            80,
            "broker/threads/watch",
            json!({"threadId":"线程"}),
        )),
    );
    assert_eq!(machine.runs.runs[RUN], before);
    assert_eq!(machine.runs.runs[RUN].last_progress_at, Some(30));
}

#[test]
fn 上游报错接收者去重且普通连接收不到扩展() {
    let (mut machine, _) = running();
    for (conn, cxb) in [(1, true), (2, true), (3, false), (4, true)] {
        connected(&mut machine, conn, cxb);
    }
    machine.record("线程").watchers = vec![1, 2, 3, 2];
    let actions = apply(&mut machine, upstream("线程", "本轮", true));
    let mut recipients: Vec<_> = updates(&actions)
        .into_iter()
        .map(|(conn, _)| conn)
        .collect();
    recipients.sort_unstable();
    assert_eq!(recipients, vec![1, 2]);
    assert!(sends(&actions)
        .iter()
        .any(|(conn, message)| *conn == 3 && message["method"] == "error"));
    machine.claims.get_mut("线程").unwrap().state = State::Running {
        owner: Owner::Broker {
            policy: crate::policy::Policy::Never,
            approvals_reviewer: None,
            owned_at: 0,
            last_watcher_at: 0,
        },
        turn_id: "本轮".into(),
        joined: false,
    };
    machine.record("线程").watchers = vec![2, 3];
    let actions = apply(&mut machine, upstream("线程", "本轮", true));
    assert_eq!(
        updates(&actions)
            .into_iter()
            .map(|(conn, _)| conn)
            .collect::<Vec<_>>(),
        vec![2]
    );
}

#[test]
fn 上游报错审查早到与完成同批回放恰好记一次并查询执行线程去重() {
    let (mut machine, context) = start_legacy_review(false);
    for (conn, cxb) in [(1, true), (2, true), (3, true), (4, false)] {
        connected(&mut machine, conn, cxb);
    }
    machine.record("线程").watchers = vec![2, 4];
    machine.record("审查线程").watchers = vec![3, 4];
    apply(&mut machine, upstream("审查线程", "审查轮", true));
    apply(
        &mut machine,
        notify(
            "turn/completed",
            json!({"threadId":"审查线程",
        "turn":{"id":"审查轮","status":"completed","items":[]}}),
        ),
    );
    assert!(machine.runs.runs[RUN].upstream_error.is_none());
    assert!(machine.records["审查线程"]
        .ended_turns
        .contains(&"审查轮".into()));
    let actions = reply(
        &mut machine,
        &context,
        Ok(json!({"reviewThreadId":"审查线程","turn":{"id":"审查轮"}})),
    );
    let error = machine.runs.runs[RUN].upstream_error.as_ref().unwrap();
    assert_eq!(error.thread_id, "审查线程");
    assert_eq!(error.count, 1);
    let mut recipients: Vec<_> = updates(&actions)
        .into_iter()
        .map(|(conn, _)| conn)
        .collect();
    recipients.sort_unstable();
    assert_eq!(recipients, vec![1, 2, 3]);
    assert_eq!(machine.runs.runs[RUN].state, RunState::Ended);
    let actions = apply(
        &mut machine,
        client(request(
            1,
            82,
            "broker/runs/modelSummaries",
            json!({"threadIds":["线程","审查线程","审查线程"]}),
        )),
    );
    let summary = sends(&actions)
        .into_iter()
        .find(|(_, message)| message["id"] == 82)
        .unwrap()
        .1;
    assert_eq!(summary["result"]["runs"].as_array().unwrap().len(), 1);
    assert_eq!(summary["result"]["runs"][0]["upstreamError"]["count"], 1);
    let actions = apply(
        &mut machine,
        client(request(
            1,
            83,
            "broker/runs/modelSummaries",
            json!({"threadIds":["审查线程"]}),
        )),
    );
    assert_eq!(
        sends(&actions)[0].1["result"]["runs"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn 上游报错暂存定案只记一次且先于完成() {
    let (mut machine, context) = start(true, "turn/start");
    connected(&mut machine, 1, true);
    apply(&mut machine, turn("turn/started", "本轮"));
    apply(&mut machine, upstream("线程", "本轮", true));
    apply(&mut machine, turn("turn/completed", "本轮"));
    assert!(machine.runs.runs[RUN].upstream_error.is_none());
    let actions = reply(&mut machine, &context, Ok(json!({"turn":{"id":"本轮"}})));
    let error = machine.runs.runs[RUN].upstream_error.as_ref().unwrap();
    assert_eq!(error.count, 1);
    assert_eq!(updates(&actions).len(), 1);
}

#[test]
fn 上游报错启动失败诊断在释放前通知所有者并保留原错误() {
    let (mut machine, context) = start(false, "turn/start");
    connected(&mut machine, 1, true);
    let input = json!({"code":-32602,"message":"启动失败 Bearer secret",
        "data":{"codexErrorInfo":"unauthorized"}});
    let actions = reply(&mut machine, &context, Err(input.clone()));
    let record = &machine.runs.runs[RUN];
    assert_eq!(
        record.upstream_error.as_ref().unwrap().source,
        UpstreamErrorSource::RpcResponse
    );
    assert_eq!(record.upstream_error.as_ref().unwrap().will_retry, None);
    assert_eq!(record.turns[0].error, Some(input));
    assert_eq!(record.diagnostic_revision, 1);
    assert_eq!(updates(&actions).len(), 1);
    assert!(machine.claims.is_empty());
}

#[test]
fn 上游报错序列化重读保留字段且旧记录缺省省略() {
    let (mut machine, _) = running();
    let original = serde_json::to_string(&machine.runs.runs[RUN]).unwrap();
    assert!(!original.contains("upstreamError"));
    assert!(!original.contains("diagnosticRevision"));
    assert!(!original.contains("lastProgressAt"));
    let old: RunRecord = serde_json::from_str(&original).unwrap();
    assert_eq!(serde_json::to_string(&old).unwrap(), original);
    apply(&mut machine, upstream("线程", "本轮", true));
    let record = &machine.runs.runs[RUN];
    let saved = serde_json::to_string(record).unwrap();
    let restored: RunRecord = serde_json::from_str(&saved).unwrap();
    assert_eq!(&restored, record);
}
