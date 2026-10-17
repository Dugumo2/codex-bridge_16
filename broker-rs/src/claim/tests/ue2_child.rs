//! 上游报错第二批 C2：子线程报错与进展归入父运行（契约第 5、6 节）。
//! 首轮绑定由真实 v1 派生建立；缓存淘汰测试另构造归属层已确认的新轮绑定，不绕过诊断入口。
use super::*;
use crate::claim::converge::ConvergenceReadiness;
use crate::relations::BindingStatus;
use crate::runs::child_diag::MAX_CHILD_UPSTREAM_ERRORS;
use crate::runs::upstream_error::{UpstreamError, UpstreamErrorCategory, UpstreamErrorSource};
use crate::runs::{ChildUpstreamError, RunState};
use crate::sa1b::{BatchReadId, BudgetAdmission};
use crate::store::StoreOp;
use crate::test_support::subagent::SubagentMock;

const P: &str = "父线程";
const T: &str = "父轮";
const R: &str = "父运行";
const C: &str = "子线程";
const CT: &str = "子轮";

/// 父线程上运行 R 已开轮，owner 是 cxb 连接 1；`sa1b` 决定是否打开收敛门禁。
fn setup(sa1b: bool) -> (Machine, SubagentMock) {
    let mut m = Machine::default();
    m.clock.mono_ms = 1_000;
    m.clock.now_ms = 100_000;
    if sa1b {
        assert!(m.enable_convergence(ConvergenceReadiness {
            trusted_inputs: true,
            budget_admission: true,
            asynchronous_results: true,
            terminal_mapping: true,
            cancellation_verified: true,
        }));
    }
    apply(&mut m, Event::Connected { conn: 1 });
    apply(&mut m, Event::Initialized { conn: 1, cxb: true });
    m.record(P).subscribed = true;
    let out = apply(
        &mut m,
        Event::ClientRequest {
            request: request(
                1,
                1,
                "turn/start",
                json!({"threadId":P,"input":[{"type":"text","text":"任务"}]}),
            ),
            run_id: Some(R.into()),
        },
    );
    let context = out
        .iter()
        .find_map(|a| match a {
            Action::Request(c) if c.method == "turn/start" => Some(c.clone()),
            _ => None,
        })
        .expect("开轮请求");
    if sa1b {
        m.convergences
            .capabilities
            .insert(P.into(), (m.relations.conn_epoch, true));
        m.convergences.test_budget = Some(BudgetAdmission::Allowed);
        m.convergences.test_read = Some(BatchReadId(9));
    }
    reply(&mut m, &context, Ok(json!({"turn":{"id":T}})));
    apply(
        &mut m,
        notify(
            "turn/started",
            json!({"threadId":P,"turn":{"id":T,"status":"inProgress","items":[]}}),
        ),
    );
    let mut mock = SubagentMock::default();
    mock.register(P, None, "v1", false, true);
    (m, mock)
}

/// 父轮 v1 派生子线程并开始首轮，确认绑定到 R。
fn spawn(m: &mut Machine, mock: &mut SubagentMock, child: &str, item: &str, turn: &str) {
    mock.register(child, Some(P), "v1", false, true);
    let (started, completed) = SubagentMock::v1_spawn(P, T, item, child, false);
    apply(m, started);
    apply(m, completed);
    apply(m, mock.turn_started(child, turn));
    let binding = m.binding(child, turn).expect("首轮绑定");
    assert_eq!(binding.status, BindingStatus::Bound);
    assert_eq!(binding.run_id, R);
}

fn advance(m: &mut Machine, ms: u64) {
    m.clock.mono_ms += ms;
    m.clock.now_ms += ms;
}

fn error_event(thread: &str, turn: &str, message: &str, info: Value) -> Event {
    notify(
        "error",
        json!({"threadId":thread,"turnId":turn,"willRetry":true,
            "error":{"message":message,"codexErrorInfo":info}}),
    )
}

fn unauthorized(thread: &str, turn: &str) -> Event {
    error_event(thread, turn, "认证失败", json!("unauthorized"))
}

fn reconnect(thread: &str, turn: &str, attempt: u32) -> Event {
    error_event(
        thread,
        turn,
        &format!("Reconnecting... {attempt}/5"),
        json!({"responseStreamDisconnected":{"httpStatusCode":null}}),
    )
}

fn failed(thread: &str, turn: &str) -> Event {
    notify(
        "turn/completed",
        json!({"threadId":thread,"turn":{"id":turn,"items":[],"status":"failed",
            "error":{"message":"Reconnecting... 5/5",
                "codexErrorInfo":{"responseStreamDisconnected":{"httpStatusCode":null}}}}}),
    )
}

fn delta(thread: &str, turn: &str) -> Event {
    notify(
        "item/agentMessage/delta",
        json!({"threadId":thread,"turnId":turn,"itemId":"子消息","delta":"子输出"}),
    )
}

fn run_updates(actions: &[Action]) -> Vec<Value> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Send { conn: 1, message } if message["method"] == "broker/run/updated" => {
                Some(message["params"].clone())
            }
            _ => None,
        })
        .collect()
}

fn child_errors(m: &Machine) -> &Vec<ChildUpstreamError> {
    &m.runs.runs[R].child_upstream_errors
}

fn entry<'a>(m: &'a Machine, thread: &str) -> &'a UpstreamError {
    &child_errors(m)
        .iter()
        .find(|e| e.thread_id == thread)
        .unwrap_or_else(|| panic!("应有 {thread} 的子线程项"))
        .upstream_error
}

#[test]
fn 已绑定未结束子轮的报错归入父运行且父报错不变_统一提交只增版一次() {
    let (mut m, mut mock) = setup(false);
    spawn(&mut m, &mut mock, C, "派生1", CT);
    // 只有 SA1a 管理上下文、未启用 SA1b 的运行也归入。
    assert!(m.runs.runs[R].subagent_join.is_none());
    assert!(m.manage_context(R).is_some());
    let revision = m.runs.runs[R].diagnostic_revision;
    let a = apply(&mut m, unauthorized(C, CT));
    let record = &m.runs.runs[R];
    assert!(record.upstream_error.is_none(), "父运行自己的报错不变");
    assert_eq!(record.child_upstream_errors.len(), 1);
    let item = &record.child_upstream_errors[0];
    assert_eq!(item.thread_id, C);
    assert_eq!(item.parent_thread_id, P);
    assert_eq!(item.upstream_error.turn_id.as_deref(), Some(CT));
    assert_eq!(
        item.upstream_error.category,
        UpstreamErrorCategory::Authentication
    );
    assert_eq!(item.upstream_error.count, 1);
    assert_eq!(record.diagnostic_revision, revision + 1);
    let updates = run_updates(&a);
    assert_eq!(updates.len(), 1, "一次事件只发一条通知：{updates:?}");
    let update = &updates[0];
    assert_eq!(update["diagnosticRevision"], revision + 1);
    assert!(update["upstreamError"].is_null());
    assert_eq!(update["childUpstreamError"]["threadId"], C);
    assert_eq!(update["childUpstreamError"]["parentThreadId"], P);
    assert_eq!(update["childUpstreamError"]["upstreamError"]["count"], 1);
    assert_eq!(update["childUpstreamErrors"].as_array().unwrap().len(), 1);
    let puts = a
        .iter()
        .filter(|x| matches!(x, Action::Store(StoreOp::PutRun(r)) if r.run_id == R))
        .count();
    assert_eq!(puts, 1, "统一提交只落盘一次");
    assert!(a
        .iter()
        .any(|x| matches!(x, Action::Store(StoreOp::PutRun(r))
        if r.run_id == R && r.child_upstream_errors.len() == 1)));
}

#[test]
fn 同一子线程重连同组计数_类别变化换组_失败完成同组不加计数且只接纳一次() {
    let (mut m, mut mock) = setup(false);
    spawn(&mut m, &mut mock, C, "派生1", CT);
    apply(&mut m, reconnect(C, CT, 2));
    let first_seen = entry(&m, C).first_seen_at;
    advance(&mut m, 1_000);
    let a = apply(&mut m, reconnect(C, CT, 3));
    assert_eq!(entry(&m, C).count, 2);
    assert_eq!(entry(&m, C).first_seen_at, first_seen);
    assert_eq!(
        run_updates(&a)[0]["childUpstreamError"]["upstreamError"]["count"],
        2
    );
    advance(&mut m, 1_000);
    apply(&mut m, unauthorized(C, CT));
    assert_eq!(entry(&m, C).count, 1, "类别变化另起一组");
    assert_eq!(entry(&m, C).category, UpstreamErrorCategory::Authentication);
    advance(&mut m, 1_000);
    apply(&mut m, reconnect(C, CT, 4));
    assert_ne!(entry(&m, C).category, UpstreamErrorCategory::Authentication);
    assert_eq!(entry(&m, C).count, 1);
    advance(&mut m, 1_000);
    let revision = m.runs.runs[R].diagnostic_revision;
    let a = apply(&mut m, failed(C, CT));
    assert_eq!(entry(&m, C).count, 1, "失败完成同组不加计数");
    assert_eq!(entry(&m, C).will_retry, Some(false));
    assert_eq!(entry(&m, C).last_seen_at, m.clock.now_ms);
    assert_eq!(m.runs.runs[R].diagnostic_revision, revision + 1);
    assert_eq!(run_updates(&a).len(), 1);
    advance(&mut m, 1_000);
    let before = child_errors(&m).clone();
    let a = apply(&mut m, failed(C, CT));
    assert_eq!(*child_errors(&m), before, "重复完成不再接纳");
    assert_eq!(m.runs.runs[R].diagnostic_revision, revision + 1);
    assert!(run_updates(&a).is_empty());
    assert!(m.runs.runs[R].upstream_error.is_none());
}

#[test]
fn 子轮完成后迟到的报错与增量不归入() {
    let (mut m, mut mock) = setup(false);
    spawn(&mut m, &mut mock, C, "派生1", CT);
    apply(&mut m, delta(C, CT));
    assert!(
        m.runs
            .child_event_log
            .iter()
            .any(|(t, method)| t == C && method == "item/agentMessage/delta"),
        "子轮进行中的增量归入父运行"
    );
    apply(&mut m, mock.turn_completed(C, CT, "completed"));
    let log = m.runs.child_event_log.len();
    let revision = m.runs.runs[R].diagnostic_revision;
    advance(&mut m, 1_000);
    let a = apply(&mut m, unauthorized(C, CT));
    apply(&mut m, delta(C, CT));
    assert!(child_errors(&m).is_empty());
    assert_eq!(m.runs.child_event_log.len(), log, "迟到事件不经子线程分流");
    assert_eq!(m.runs.runs[R].diagnostic_revision, revision);
    assert!(run_updates(&a).is_empty());
    assert!(m.child_activity_run(C, CT, false).is_none());
}

#[test]
fn 四轮_完成缓存淘汰后旧子轮不覆盖新诊断且不增版() {
    let (mut m, mut mock) = setup(false);
    spawn(&mut m, &mut mock, C, "派生1", CT);
    apply(&mut m, failed(C, CT));
    assert_eq!(entry(&m, C).turn_id.as_deref(), Some(CT));
    // 本测试聚焦诊断缓存退化；显式提供归属层已确认的新轮因果绑定。
    let mut binding = m.binding(C, CT).unwrap();
    binding.turn_id = "新子轮".into();
    binding.operation.item_id = "已确认跟进".into();
    m.relations
        .bindings
        .insert((C.into(), "新子轮".into()), binding);
    apply(&mut m, mock.turn_started(C, "新子轮"));
    apply(&mut m, unauthorized(C, "新子轮"));
    assert_eq!(entry(&m, C).turn_id.as_deref(), Some("新子轮"));
    let before = child_errors(&m).clone();
    let revision = m.runs.runs[R].diagnostic_revision;
    // 精确模拟完成去重缓存被淘汰；有效绑定与关系观察仍存在。
    m.records.get_mut(C).unwrap().ended_turns.clear();
    let actions = apply(&mut m, failed(C, CT));
    assert_eq!(*child_errors(&m), before);
    assert_eq!(m.runs.runs[R].diagnostic_revision, revision);
    assert!(run_updates(&actions).is_empty());
    // 数组项被淘汰后，重复完成也不能重新插入。
    m.runs
        .runs
        .get_mut(R)
        .unwrap()
        .child_upstream_errors
        .clear();
    m.records.get_mut(C).unwrap().ended_turns.clear();
    let actions = apply(&mut m, failed(C, CT));
    assert!(child_errors(&m).is_empty());
    assert_eq!(m.runs.runs[R].diagnostic_revision, revision);
    assert!(run_updates(&actions).is_empty());
    m.run_released(R, crate::runs::ReleaseKind::Natural, &mut vec![]);
    assert!(!m.accept_child_completion(
        &crate::runs::ChildRunRef {
            run_id: R.into(),
            parent_thread_id: P.into()
        },
        C,
        "释放后新键"
    ));
}

#[test]
fn 两个子线程各自一项() {
    let (mut m, mut mock) = setup(false);
    spawn(&mut m, &mut mock, "子甲", "派生甲", "甲轮");
    spawn(&mut m, &mut mock, "子乙", "派生乙", "乙轮");
    apply(&mut m, reconnect("子甲", "甲轮", 2));
    advance(&mut m, 1_000);
    apply(&mut m, reconnect("子乙", "乙轮", 2));
    advance(&mut m, 1_000);
    let a = apply(&mut m, reconnect("子甲", "甲轮", 3));
    assert_eq!(child_errors(&m).len(), 2);
    assert_eq!(entry(&m, "子甲").count, 2);
    assert_eq!(entry(&m, "子乙").count, 1);
    assert_eq!(entry(&m, "子乙").thread_id, "子乙");
    let update = &run_updates(&a)[0];
    assert_eq!(update["childUpstreamError"]["threadId"], "子甲");
    assert_eq!(update["childUpstreamErrors"].as_array().unwrap().len(), 2);
}

#[test]
fn 第33个子线程淘汰最后出现时间最旧的一项_已有项更新不淘汰() {
    let (mut m, mut mock) = setup(false);
    spawn(&mut m, &mut mock, C, "派生1", CT);
    let now = m.clock.now_ms;
    // 先填满 32 项；最旧的放在中间，确认按 lastSeenAt 而不是位置淘汰。
    let filler: Vec<ChildUpstreamError> = (0..MAX_CHILD_UPSTREAM_ERRORS as u64)
        .map(|i| {
            let thread = format!("旧子{i:02}");
            let seen = if i == 5 { now - 100 } else { now - 50 + i };
            ChildUpstreamError {
                thread_id: thread.clone(),
                parent_thread_id: P.into(),
                upstream_error: UpstreamError::from_error(
                    &thread,
                    Some("旧轮"),
                    UpstreamErrorSource::ErrorNotification,
                    &json!({"message":"旧报错","codexErrorInfo":"unauthorized"}),
                    Some(true),
                    seen,
                ),
            }
        })
        .collect();
    m.runs.runs.get_mut(R).unwrap().child_upstream_errors = filler;
    let a = apply(&mut m, unauthorized(C, CT));
    assert_eq!(child_errors(&m).len(), MAX_CHILD_UPSTREAM_ERRORS);
    assert!(!child_errors(&m).iter().any(|e| e.thread_id == "旧子05"));
    assert_eq!(child_errors(&m).last().unwrap().thread_id, C);
    assert_eq!(
        run_updates(&a)[0]["childUpstreamErrors"]
            .as_array()
            .unwrap()
            .len(),
        MAX_CHILD_UPSTREAM_ERRORS,
        "通知带淘汰后的完整数组"
    );
    let survivors: Vec<String> = child_errors(&m)
        .iter()
        .map(|e| e.thread_id.clone())
        .collect();
    advance(&mut m, 1_000);
    apply(&mut m, unauthorized(C, CT));
    let after: Vec<String> = child_errors(&m)
        .iter()
        .map(|e| e.thread_id.clone())
        .collect();
    assert_eq!(after, survivors, "已有项更新不淘汰别的子线程");
    assert_eq!(entry(&m, C).count, 2);
}

#[test]
fn 绑定不存在_未绑定_冲突都不归入() {
    let (mut m, mut mock) = setup(false);
    apply(&mut m, unauthorized("陌生线程", "陌生轮"));
    assert!(m.child_activity_run("陌生线程", "陌生轮", false).is_none());
    spawn(&mut m, &mut mock, C, "派生1", CT);
    for status in [
        BindingStatus::Unbound,
        BindingStatus::Conflict("其他运行".into()),
    ] {
        m.relations
            .bindings
            .get_mut(&(C.to_owned(), CT.to_owned()))
            .unwrap()
            .status = status.clone();
        assert!(m.child_activity_run(C, CT, false).is_none(), "{status:?}");
        advance(&mut m, 1_000);
        apply(&mut m, unauthorized(C, CT));
        assert!(child_errors(&m).is_empty(), "{status:?}");
    }
}

#[test]
fn 另一运行的子线程与已结束运行都不归入() {
    let (mut m, mut mock) = setup(false);
    spawn(&mut m, &mut mock, C, "派生1", CT);
    let binding = (C.to_owned(), CT.to_owned());
    m.relations.bindings.get_mut(&binding).unwrap().run_id = "其他运行".into();
    assert!(m.child_activity_run(C, CT, false).is_none());
    apply(&mut m, unauthorized(C, CT));
    assert!(child_errors(&m).is_empty());
    m.relations.bindings.get_mut(&binding).unwrap().run_id = R.into();
    assert!(m.child_activity_run(C, CT, false).is_some());
    for state in [RunState::Ended, RunState::Lost] {
        m.runs.runs.get_mut(R).unwrap().state = state;
        assert!(m.child_activity_run(C, CT, false).is_none(), "{state:?}");
        advance(&mut m, 1_000);
        apply(&mut m, unauthorized(C, CT));
        assert!(child_errors(&m).is_empty(), "{state:?}");
    }
    m.runs.runs.get_mut(R).unwrap().state = RunState::Stopping;
    assert!(
        m.child_activity_run(C, CT, false).is_some(),
        "Stopping 仍归入"
    );
}

#[test]
fn 认领执行线程与review执行线程不归入() {
    let (mut m, mut mock) = setup(false);
    spawn(&mut m, &mut mock, C, "派生1", CT);
    let mut own = m.binding(C, CT).unwrap();
    own.thread_id = P.into();
    own.turn_id = T.into();
    m.relations
        .bindings
        .insert((P.to_owned(), T.to_owned()), own);
    assert!(m.child_activity_run(P, T, false).is_none(), "认领执行线程");
    m.runs.runs.get_mut(R).unwrap().review_thread_id = Some(C.into());
    assert!(
        m.child_activity_run(C, CT, false).is_none(),
        "review 执行线程"
    );
    apply(&mut m, unauthorized(C, CT));
    assert!(child_errors(&m).is_empty());
}

#[test]
fn 归属用绑定上的运行号不转投同线程上的新运行() {
    let (mut m, mut mock) = setup(false);
    spawn(&mut m, &mut mock, C, "派生1", CT);
    // 父线程的认领改记为另一运行：子线程项仍按绑定记到 R。
    m.claims.get_mut(P).unwrap().run_id = Some("新运行".into());
    let child = m.child_activity_run(C, CT, false).expect("仍归入");
    assert_eq!(child.run_id, R);
    assert_eq!(child.parent_thread_id, P);
    apply(&mut m, unauthorized(C, CT));
    assert_eq!(child_errors(&m).len(), 1);
    assert!(!m.runs.runs.contains_key("新运行"));
}

#[test]
fn 启用sa1b的运行收到子线程报错不改收敛状态与预算() {
    let (mut m, mut mock) = setup(true);
    spawn(&mut m, &mut mock, C, "派生1", CT);
    let record = m.runs.runs[R].clone();
    let runtime = m.convergences.runs.get(R).cloned();
    let progress = m.convergences.progress.get(R).cloned();
    // 只发报错：子轮真实结束会按 SA1b 现有逻辑推进收敛批次，与诊断记账无关。
    apply(&mut m, reconnect(C, CT, 2));
    advance(&mut m, 1_000);
    apply(&mut m, unauthorized(C, CT));
    let after = &m.runs.runs[R];
    assert_eq!(after.child_upstream_errors.len(), 1);
    assert_eq!(after.diagnostic_revision, record.diagnostic_revision + 2);
    assert!(after.upstream_error.is_none());
    assert_eq!(
        after.subagent_join, record.subagent_join,
        "subagentJoin 不变"
    );
    assert_eq!(after.run_budget, record.run_budget, "预算不变");
    assert_eq!(after.turns, record.turns, "不新增轮次结果");
    assert_eq!(m.convergences.runs.get(R).cloned(), runtime);
    assert_eq!(m.convergences.progress.get(R).cloned(), progress);
}

#[test]
fn 重启后子线程报错数组可读() {
    let (mut m, mut mock) = setup(false);
    spawn(&mut m, &mut mock, C, "派生1", CT);
    apply(&mut m, unauthorized(C, CT));
    let record = m.runs.runs[R].clone();
    let mut restored = Machine::default();
    apply(
        &mut restored,
        Event::Restore {
            index: m.runs.index.clone(),
            runs: vec![record.clone()],
            inbox: Default::default(),
        },
    );
    assert_eq!(
        restored.runs.runs[R].child_upstream_errors,
        record.child_upstream_errors
    );
    let wire = serde_json::to_value(&restored.runs.runs[R]).unwrap();
    assert_eq!(wire["childUpstreamErrors"][0]["threadId"], C);
    assert_eq!(wire["childUpstreamErrors"][0]["parentThreadId"], P);
}
