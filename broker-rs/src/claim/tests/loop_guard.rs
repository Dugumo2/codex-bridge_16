//! 自动续跑空转刹车（S1-评审第 26 节）：计数、清零、结算顺序、两种刹车与私有模式的停止。

use super::*;
use crate::claim::loop_guard::{CONTINUATION_LIMIT, MAX_AUTO_TURNS, MAX_PENDING_TURNS};
use crate::runs::{
    InterruptReason, LoopGuardAction, LoopGuardKind, PauseOutcome, RunRecord, RunStatus, RunTimer,
};

const RUN: &str = "run-测试-000001";

fn goal_event(objective: &str, status: &str, tokens: u64) -> Event {
    notify(
        "thread/goal/updated",
        json!({"threadId": "线程", "goal": {"threadId": "线程", "objective": objective,
            "status": status, "createdAt": 1, "tokensUsed": tokens}}),
    )
}

fn agent(id: &str) -> Value {
    json!({"type": "agentMessage", "id": id, "text": "还在想"})
}

fn command(id: &str) -> Value {
    json!({"type": "commandExecution", "id": id, "command": "ls", "status": "completed"})
}

fn done(id: &str, items: Value, view: &str) -> Event {
    notify(
        "turn/completed",
        json!({"threadId": "线程", "turn": {"id": id, "items": items, "itemsView": view,
            "status": "completed"}}),
    )
}

/// 首轮由客户端开、goal 进行中，完成后认领等服务器续跑。
fn awaiting() -> Machine {
    let (mut machine, context) = start(false, "turn/start");
    apply(&mut machine, goal_event("目标", "active", 0));
    reply(&mut machine, &context, Ok(json!({"turn": {"id": "首轮"}})));
    apply(&mut machine, turn("turn/started", "首轮"));
    apply(
        &mut machine,
        done("首轮", json!([agent("首轮回答")]), "full"),
    );
    assert!(matches!(
        machine.claims["线程"].state,
        State::AwaitingContinuation { .. }
    ));
    machine
}

/// 服务器自开一轮，完成时带完整条目。
fn auto(machine: &mut Machine, id: &str, items: Value) -> Vec<Action> {
    let mut actions = apply(machine, turn("turn/started", id));
    actions.extend(apply(machine, done(id, items, "full")));
    actions
}

fn idle(machine: &mut Machine, id: &str) -> Vec<Action> {
    auto(machine, id, json!([agent(&format!("{id}-话"))]))
}

/// 有外部动作的自动轮（带最后回答，不触发补读）。
fn busy(machine: &mut Machine, id: &str) -> Vec<Action> {
    auto(
        machine,
        id,
        json!([command(&format!("{id}-命令")), agent("做完了")]),
    )
}

fn record(machine: &Machine) -> &RunRecord {
    &machine.runs.runs[RUN]
}

/// 夹具对停止的 goal 读一律回「没有 goal」，停止可能在同一步就确认并释放，所以按运行记录判断。
fn braked(machine: &Machine) -> bool {
    machine
        .stops
        .operations
        .values()
        .any(|op| op.reason == StopReason::GoalLoop)
        || record(machine).interrupt_reason == Some(InterruptReason::GoalLoop)
}

fn assert_report(machine: &Machine, kind: LoopGuardKind, turns: &[&str]) {
    assert!(braked(machine), "应发起空转刹车的停止");
    let report = record(machine).loop_guard.as_ref().expect("应记下刹车详情");
    assert_eq!(report.kind, kind);
    assert_eq!(report.turn_ids, turns);
    assert_eq!(report.count as usize, turns.len());
    assert_eq!(
        record(machine).interrupt_reason,
        Some(InterruptReason::GoalLoop)
    );
}

fn result_read(actions: &[Action], turn: &str) -> RequestContext {
    let context = actions
        .iter()
        .find_map(|a| match a {
            Action::Request(c)
                if matches!(&c.operation, Operation::ResultRead(r) if r.turn_id == turn) =>
            {
                Some(c.clone())
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("缺少轮次 {turn} 的补读：{actions:?}"));
    assert_eq!(context.method, "thread/items/list");
    assert_eq!(context.params.as_ref().unwrap()["turnId"], turn);
    context
}

/// 条目页必须读到 nextCursor=null 才代表完整轮次；响应中的 turnId 保留归属校验。
fn read_reply(turn: &str, items: Value, next_cursor: Option<&str>) -> Value {
    json!({"data": items.as_array().unwrap().iter().map(|item| json!({
        "turnId":turn,"item":item,"startedAtMs":null,"completedAtMs":null
    })).collect::<Vec<_>>(), "nextCursor":next_cursor,"backwardsCursor":null})
}

/// 补读一直失败直到放弃：每次失败后触发重试计时，返回最后一次的动作。
fn exhaust_read(machine: &mut Machine, first: RequestContext) -> Vec<Action> {
    let mut context = first;
    // 每次失败只安排下一次尝试；首次加三次重试共四个分页 RPC。
    for (attempt, delay) in [Some(1_000), Some(2_000), Some(4_000), None]
        .into_iter()
        .enumerate()
    {
        assert_eq!(context.method, "thread/items/list");
        let Operation::ResultRead(identity) = &context.operation else {
            panic!("应为结果补读")
        };
        assert_eq!(usize::from(identity.attempt), attempt);
        let params = context.params.as_ref().unwrap();
        assert_eq!(params["threadId"], identity.thread_id);
        assert_eq!(params["turnId"], identity.turn_id);
        assert_eq!(params["sortDirection"], "asc");
        if attempt > 0 {
            assert!(params["cursor"].is_null(), "重试必须从首页开始");
        }
        let actions = reply(
            machine,
            &context,
            Err(json!({"code":-32000,"message":"分页读取失败"})),
        );
        let retries: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Schedule {
                    token,
                    kind: kind @ TimerKind::Run(RunTimer::ResultRetry(_)),
                    delay_ms,
                } => Some((
                    Event::Timer {
                        token: token.clone(),
                        kind: kind.clone(),
                    },
                    *delay_ms,
                )),
                _ => None,
            })
            .collect();
        let Some(delay) = delay else {
            assert!(retries.is_empty(), "三次重试耗尽后不得再重试");
            return actions;
        };
        assert_eq!(retries.len(), 1);
        assert_eq!(retries[0].1, delay);
        let actions = apply(machine, retries[0].0.clone());
        let requests: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Request(c) if matches!(c.operation, Operation::ResultRead(_)) => {
                    Some(c.clone())
                }
                _ => None,
            })
            .collect();
        assert_eq!(requests.len(), 1, "每次重试只发一个首页请求");
        context = requests[0].clone();
    }
    unreachable!("四次失败应已收束")
}

#[test]
fn 连续三个无动作的自动轮触发刹车一并走停止流程() {
    let mut m = awaiting();
    idle(&mut m, "续1");
    idle(&mut m, "续2");
    assert!(!braked(&m), "两轮不应触发");
    assert!(record(&m).loop_guard.is_none());
    idle(&mut m, "续3");
    assert!(braked(&m));
    assert_report(&m, LoopGuardKind::NoAction, &["续1", "续2", "续3"]);
    assert_eq!(record(&m).loop_guard.as_ref().unwrap().threshold, 3);
}

#[test]
fn 触发后停止流程读到_goal_进行中就发暂停写入() {
    let mut m = awaiting();
    idle(&mut m, "续1");
    idle(&mut m, "续2");
    apply(&mut m, turn("turn/started", "续3"));
    // 绕过夹具对 goal 读的自动回答，看停止流程实际发出的请求。
    let (next, actions) = step(
        std::mem::take(&mut m),
        done("续3", json!([agent("说")]), "full"),
    );
    m = next;
    let read = actions
        .iter()
        .find_map(|a| match a {
            Action::Request(c)
                if c.method == "thread/goal/get" && matches!(c.operation, Operation::Stop(..)) =>
            {
                Some(c.clone())
            }
            _ => None,
        })
        .expect("刹车应发起停止并先读 goal");
    let active = json!({"goal": {"threadId": "线程", "objective": "目标", "status": "active", "createdAt": 1}});
    let (next, actions) = step(
        m,
        Event::Reply {
            context: read,
            result: Ok(active),
        },
    );
    m = next;
    let write = actions
        .iter()
        .find_map(|a| match a {
            Action::Request(c) if c.method == "thread/goal/set" => Some(c.clone()),
            _ => None,
        })
        .expect("goal 仍在进行时应暂停");
    assert_eq!(write.params.as_ref().unwrap()["status"], "paused");
    assert!(m
        .stops
        .operations
        .values()
        .any(|op| op.reason == StopReason::GoalLoop && !op.clear));
}

#[test]
fn 客户端开的首轮不计入() {
    let (mut m, context) = start(false, "turn/start");
    apply(&mut m, goal_event("目标", "active", 0));
    reply(&mut m, &context, Ok(json!({"turn": {"id": "首轮"}})));
    apply(&mut m, turn("turn/started", "首轮"));
    apply(&mut m, done("首轮", json!([agent("只说话")]), "full"));
    idle(&mut m, "续1");
    idle(&mut m, "续2");
    assert!(!braked(&m), "首轮是客户端开的，不算自动轮");
    idle(&mut m, "续3");
    assert!(braked(&m));
}

#[test]
fn 有外部动作的自动轮清零() {
    let mut m = awaiting();
    idle(&mut m, "续1");
    idle(&mut m, "续2");
    auto(&mut m, "续3", json!([command("命令"), agent("说")]));
    idle(&mut m, "续4");
    idle(&mut m, "续5");
    assert!(!braked(&m));
    idle(&mut m, "续6");
    assert_report(&m, LoopGuardKind::NoAction, &["续4", "续5", "续6"]);
}

#[test]
fn 各类动作都算_用户输入与维护条目清零() {
    for (name, item) in [
        (
            "文件",
            json!({"type": "fileChange", "id": "x", "changes": []}),
        ),
        ("MCP", json!({"type": "mcpToolCall", "id": "x"})),
        ("动态工具", json!({"type": "dynamicToolCall", "id": "x"})),
        ("搜索", json!({"type": "webSearch", "id": "x"})),
        ("看图", json!({"type": "imageView", "id": "x"})),
        ("生图", json!({"type": "imageGeneration", "id": "x"})),
        (
            "其他工具",
            json!({"type": "collabAgentToolCall", "id": "x"}),
        ),
        (
            "用户输入",
            json!({"type": "userMessage", "id": "x", "content": []}),
        ),
        ("审查", json!({"type": "exitedReviewMode", "id": "x"})),
        ("压缩", json!({"type": "contextCompaction", "id": "x"})),
    ] {
        let mut m = awaiting();
        idle(&mut m, "续1");
        idle(&mut m, "续2");
        auto(&mut m, "续3", json!([item, agent("说")]));
        idle(&mut m, "续4");
        assert!(!braked(&m), "{name} 应清零");
    }
}

#[test]
fn 推理与计划不算动作() {
    let mut m = awaiting();
    auto(
        &mut m,
        "续1",
        json!([{"type": "reasoning", "id": "r", "summary": []}, agent("a")]),
    );
    auto(
        &mut m,
        "续2",
        json!([{"type": "plan", "id": "p", "text": "计划"}]),
    );
    auto(&mut m, "续3", json!([]));
    assert_report(&m, LoopGuardKind::NoAction, &["续1", "续2", "续3"]);
}

#[test]
fn 未知类型既不加一也不清零() {
    let mut m = awaiting();
    idle(&mut m, "续1");
    auto(
        &mut m,
        "续2",
        json!([{"type": "未来的新类型", "id": "n"}, agent("说")]),
    );
    idle(&mut m, "续3");
    assert!(!braked(&m));
    idle(&mut m, "续4");
    assert_report(&m, LoopGuardKind::NoAction, &["续1", "续3", "续4"]);
}

#[test]
fn 摘要完成等补读结算_按开始顺序() {
    let mut m = awaiting();
    apply(&mut m, turn("turn/started", "续1"));
    let a = apply(&mut m, done("续1", json!([agent("摘要")]), "summary"));
    let read = result_read(&a, "续1");
    idle(&mut m, "续2");
    idle(&mut m, "续3");
    assert!(!braked(&m), "续1 的证据未到，后面的轮只能排队");
    let pages = reply(
        &mut m,
        &read,
        Ok(read_reply("续1", json!([agent("前页")]), Some("末页"))),
    );
    assert!(!braked(&m), "前页不能提前结算续1");
    let last_page = result_read(&pages, "续1");
    assert_eq!(last_page.params.as_ref().unwrap()["cursor"], "末页");
    // 上一页重放不能被当成末页，真正末页到齐后仍只算续1一次。
    reply(
        &mut m,
        &read,
        Ok(read_reply("续1", json!([agent("重放")]), None)),
    );
    assert!(!braked(&m));
    reply(
        &mut m,
        &last_page,
        Ok(read_reply("续1", json!([agent("全文")]), None)),
    );
    assert_report(&m, LoopGuardKind::NoAction, &["续1", "续2", "续3"]);
}

#[test]
fn 分页补读只有前页不算完整证据() {
    let mut m = awaiting();
    apply(&mut m, turn("turn/started", "续1"));
    let a = apply(&mut m, done("续1", json!([agent("摘要")]), "summary"));
    let read = result_read(&a, "续1");
    let a = reply(
        &mut m,
        &read,
        Ok(read_reply(
            "续1",
            json!([agent("摘要")]),
            Some("后续条目页"),
        )),
    );
    assert!(!braked(&m), "前页不是完整证据");
    let next = result_read(&a, "续1");
    assert_eq!(next.params.as_ref().unwrap()["cursor"], "后续条目页");
    exhaust_read(&mut m, next);
    idle(&mut m, "续2");
    idle(&mut m, "续3");
    assert!(!braked(&m), "续1 没有完整证据，不计入");
    idle(&mut m, "续4");
    assert_report(&m, LoopGuardKind::NoAction, &["续2", "续3", "续4"]);
}

#[test]
fn 补读失败但通知证明有动作时清零() {
    let mut m = awaiting();
    idle(&mut m, "续1");
    idle(&mut m, "续2");
    apply(&mut m, turn("turn/started", "续3"));
    apply(
        &mut m,
        notify(
            "item/completed",
            json!({"threadId": "线程", "turnId": "续3", "item": command("命令")}),
        ),
    );
    let a = apply(&mut m, done("续3", json!([agent("摘要")]), "summary"));
    exhaust_read(&mut m, result_read(&a, "续3"));
    idle(&mut m, "续4");
    idle(&mut m, "续5");
    assert!(!braked(&m), "通知已证明续3 有动作，计数应清零");
    idle(&mut m, "续6");
    assert_report(&m, LoopGuardKind::NoAction, &["续4", "续5", "续6"]);
}

#[test]
fn 补读失败且无通知证据时不计也不清零() {
    let mut m = awaiting();
    idle(&mut m, "续1");
    idle(&mut m, "续2");
    apply(&mut m, turn("turn/started", "续3"));
    let a = apply(&mut m, done("续3", json!([agent("摘要")]), "summary"));
    let a = exhaust_read(&mut m, result_read(&a, "续3"));
    assert!(!a
        .iter()
        .any(|a| matches!(a, Action::Request(c) if c.method == "turn/interrupt")));
    assert!(!braked(&m));
    idle(&mut m, "续4");
    assert_report(&m, LoopGuardKind::NoAction, &["续1", "续2", "续4"]);
}

#[test]
fn goal_目标或状态真正变化才清零() {
    let mut m = awaiting();
    idle(&mut m, "续1");
    idle(&mut m, "续2");
    apply(&mut m, goal_event("新目标", "active", 10));
    idle(&mut m, "续3");
    idle(&mut m, "续4");
    assert!(!braked(&m), "目标变了应清零");
    // 只更新用量的通知不清零。
    apply(&mut m, goal_event("新目标", "active", 99));
    idle(&mut m, "续5");
    assert_report(&m, LoopGuardKind::NoAction, &["续3", "续4", "续5"]);
}

#[test]
fn 刹车二_第五十个自动轮结束时触发() {
    let mut m = awaiting();
    let limit = CONTINUATION_LIMIT as usize;
    for i in 1..limit {
        busy(&mut m, &format!("续{i}"));
    }
    assert!(!braked(&m), "{} 轮不应触发", limit - 1);
    let last = format!("续{limit}");
    apply(&mut m, turn("turn/started", &last));
    assert!(!braked(&m), "第上限轮开始时不触发，等它结束");
    apply(
        &mut m,
        done(&last, json!([command("最后"), agent("说")]), "full"),
    );
    assert!(braked(&m));
    let report = record(&m).loop_guard.clone().unwrap();
    assert_eq!(report.kind, LoopGuardKind::ContinuationLimit);
    assert_eq!(report.count, CONTINUATION_LIMIT);
    assert_eq!(report.threshold, CONTINUATION_LIMIT);
    assert_eq!(report.turn_ids, vec![last]);
}

#[test]
fn 刹车二随目标变化清零_用量记账不清零() {
    let mut m = awaiting();
    let limit = CONTINUATION_LIMIT as usize;
    for i in 1..limit {
        busy(&mut m, &format!("续{i}"));
    }
    apply(&mut m, goal_event("改过的目标", "active", 0));
    for i in limit..limit * 2 - 1 {
        busy(&mut m, &format!("续{i}"));
    }
    assert!(!braked(&m), "目标变化后重新计数");
    let last = format!("续{}", limit * 2 - 1);
    busy(&mut m, &last);
    assert_eq!(record(&m).loop_guard.as_ref().unwrap().turn_ids, vec![last]);
}

#[test]
fn 已在停止中不再记刹车() {
    let mut m = awaiting();
    idle(&mut m, "续1");
    idle(&mut m, "续2");
    apply(&mut m, turn("turn/started", "续3"));
    apply(
        &mut m,
        client(request(
            1,
            7,
            "broker/threads/stop",
            json!({"threadId": "线程"}),
        )),
    );
    apply(&mut m, done("续3", json!([agent("说")]), "full"));
    assert!(!braked(&m));
    assert!(record(&m).loop_guard.is_none());
    assert_ne!(record(&m).interrupt_reason, Some(InterruptReason::GoalLoop));
}

#[test]
fn 运行结束后的迟到证据不刹车且刹车状态随认领删除() {
    let mut m = awaiting();
    idle(&mut m, "续1");
    idle(&mut m, "续2");
    apply(&mut m, turn("turn/started", "续3"));
    let a = apply(&mut m, done("续3", json!([agent("摘要")]), "summary"));
    let read = result_read(&a, "续3");
    // goal 完成：认领在等待续跑时释放，运行结束。
    apply(&mut m, goal_event("目标", "complete", 5));
    assert!(m.claims.is_empty());
    assert!(m.loop_guards.is_empty());
    reply(
        &mut m,
        &read,
        Ok(read_reply("续3", json!([agent("全文")]), None)),
    );
    assert!(!braked(&m));
    assert!(record(&m).loop_guard.is_none());
}

#[test]
fn 等证据的轮超过上限时最早的不计入() {
    let mut m = awaiting();
    let mut reads = Vec::new();
    for i in 1..=MAX_PENDING_TURNS + 1 {
        let id = format!("续{i}");
        apply(&mut m, turn("turn/started", &id));
        let a = apply(&mut m, done(&id, json!([agent("摘要")]), "summary"));
        reads.push((id.clone(), result_read(&a, &id)));
    }
    // 续1 已被挤出：它的证据回来也不参与判断；续2–续4 依次无动作，满三轮才刹。
    for (id, read) in reads.iter().take(3) {
        reply(
            &mut m,
            read,
            Ok(read_reply(id, json!([agent("全文")]), None)),
        );
    }
    assert!(!braked(&m), "续1 被挤出，只结算了两轮");
    let (id, read) = &reads[3];
    reply(
        &mut m,
        read,
        Ok(read_reply(id, json!([agent("全文")]), None)),
    );
    assert_report(&m, LoopGuardKind::NoAction, &["续2", "续3", "续4"]);
}

fn goal_reply(objective: &str, status: &str) -> Value {
    json!({"goal": {"threadId": "线程", "objective": objective, "status": status, "createdAt": 1,
        "tokensUsed": 0}})
}

/// 共享服务器：goal 由本 broker 的客户端设置（`by_client`）或只从通知得知，再开首轮、定案为自有，
/// 首轮完成后认领等服务器续跑。
fn shared_awaiting(by_client: bool) -> Machine {
    let mut m = Machine::new(true);
    m.record("线程").subscribed = true;
    if by_client {
        let a = apply(
            &mut m,
            client(request(
                1,
                40,
                "thread/goal/set",
                json!({"threadId": "线程", "objective": "目标", "status": "active"}),
            )),
        );
        reply(&mut m, &first_request(&a), Ok(goal_reply("目标", "active")));
    } else {
        apply(&mut m, goal_event("目标", "active", 0));
    }
    shared_first_turn(&mut m);
    m
}

fn shared_first_turn(m: &mut Machine) {
    let a = apply(
        m,
        client(request(
            1,
            42,
            "turn/start",
            json!({"threadId": "线程", "input": [{"type": "text", "text": "干活"}]}),
        )),
    );
    let context = first_request(&a);
    apply(m, turn("turn/started", "首轮"));
    reply(m, &context, Ok(json!({"turn": {"id": "首轮"}})));
    assert!(
        matches!(m.claims["线程"].state, State::Running { joined: false, .. }),
        "首轮应定案为自有：{:?}",
        m.claims["线程"].state
    );
    apply(m, done("首轮", json!([agent("首轮回答")]), "full"));
    assert!(matches!(
        m.claims["线程"].state,
        State::AwaitingContinuation { .. }
    ));
}

fn inner(context: &RequestContext) -> &Operation {
    let mut operation = &context.operation;
    while let Operation::GoalChange { inner, .. } | Operation::GoalRead { inner, .. } = operation {
        operation = inner;
    }
    operation
}

fn pause_request(actions: &[Action]) -> Option<RequestContext> {
    actions.iter().find_map(|a| match a {
        Action::Request(c) if matches!(inner(c), Operation::LoopGuardPause(_)) => Some(c.clone()),
        _ => None,
    })
}

fn interrupted(actions: &[Action]) -> bool {
    actions
        .iter()
        .any(|a| matches!(a, Action::Request(c) if c.method == "turn/interrupt"))
}

/// 连续三个无动作的自动轮，返回第三轮完成时的动作。
fn three_idle(m: &mut Machine, from: usize) -> Vec<Action> {
    let mut actions = Vec::new();
    for i in from..from + 3 {
        actions.extend(idle(m, &format!("续{i}")));
    }
    actions
}

#[test]
fn 共享_本运行设置的_goal_空转时只暂停_goal_不中断() {
    let mut m = shared_awaiting(true);
    assert!(m.records["线程"].goal_controlled);
    let a = three_idle(&mut m, 1);
    let pause = pause_request(&a).expect("应暂停 goal");
    assert_eq!(pause.method, "thread/goal/set");
    assert_eq!(pause.params.as_ref().unwrap()["status"], "paused");
    assert!(!interrupted(&a), "共享模式不中断任何轮");
    assert!(m.stops.operations.is_empty(), "不走停止流程");
    let report = record(&m).loop_guard.clone().expect("应记下刹车详情");
    assert_eq!(report.kind, LoopGuardKind::NoAction);
    assert_eq!(report.turn_ids, vec!["续1", "续2", "续3"]);
    assert_eq!(report.action, LoopGuardAction::PauseGoal);
    assert_eq!(report.pause, Some(PauseOutcome::Pending));
    reply(&mut m, &pause, Ok(goal_reply("目标", "paused")));
    assert_eq!(
        record(&m).loop_guard.as_ref().unwrap().pause,
        Some(PauseOutcome::Paused)
    );
    // 等续跑时 goal 不再 active：认领释放，运行按最后一轮正常结束，不算中断。
    assert!(m.claims.is_empty());
    assert_eq!(record(&m).status, Some(RunStatus::Completed));
    assert_eq!(record(&m).interrupt_reason, None);
}

#[test]
fn 共享_下一轮已在跑时也只暂停_那一轮照常跑完() {
    let mut m = shared_awaiting(true);
    idle(&mut m, "续1");
    idle(&mut m, "续2");
    apply(&mut m, turn("turn/started", "续3"));
    let a = apply(&mut m, done("续3", json!([agent("摘要")]), "summary"));
    let read = result_read(&a, "续3");
    apply(&mut m, turn("turn/started", "续4"));
    let a = reply(
        &mut m,
        &read,
        Ok(read_reply("续3", json!([agent("全文")]), None)),
    );
    let pause = pause_request(&a).expect("证据到齐后应暂停 goal");
    assert!(!interrupted(&a));
    reply(&mut m, &pause, Ok(goal_reply("目标", "paused")));
    assert_eq!(
        m.claims["线程"].turn_id(),
        Some("续4"),
        "正在跑的轮不受影响"
    );
    apply(&mut m, done("续4", json!([agent("收尾")]), "full"));
    assert!(m.claims.is_empty(), "goal 已暂停，这一轮结束后认领释放");
}

#[test]
fn 共享_只从通知得知的_goal_不暂停且重新计数() {
    let mut m = shared_awaiting(false);
    assert!(!m.records["线程"].goal_controlled);
    let a = three_idle(&mut m, 1);
    assert!(pause_request(&a).is_none());
    assert!(record(&m).loop_guard.is_none());
    let a = three_idle(&mut m, 4);
    assert!(pause_request(&a).is_none());
    assert!(m.claims.contains_key("线程"));
}

/// 本 broker 的客户端发 goal/set，回包之前先到一条 updated 通知。
fn set_with_notice_first(notice: Event) -> Machine {
    let mut m = Machine::new(true);
    m.record("线程").subscribed = true;
    let a = apply(
        &mut m,
        client(request(
            1,
            40,
            "thread/goal/set",
            json!({"threadId": "线程", "objective": "目标", "status": "active"}),
        )),
    );
    let set = first_request(&a);
    apply(&mut m, notice);
    let a = reply(&mut m, &set, Ok(goal_reply("目标", "active")));
    assert!(
        sends(&a).iter().any(|(_, msg)| msg["id"] == 40),
        "客户端照常收到回包"
    );
    m
}

#[test]
fn 共享_写入自己引起的通知先于回包到达也授予资格() {
    let m = set_with_notice_first(goal_event("目标", "active", 0));
    assert!(m.records["线程"].goal_controlled);
}

#[test]
fn 共享_回包之前目标被别人换掉不授予() {
    let m = set_with_notice_first(goal_event("别人的目标", "active", 0));
    assert!(!m.records["线程"].goal_controlled);
}

#[test]
fn 共享_自动轮出现用户输入即撤销资格() {
    let mut m = shared_awaiting(true);
    idle(&mut m, "续1");
    auto(
        &mut m,
        "续2",
        json!([{"type": "userMessage", "id": "u", "content": []}, agent("答")]),
    );
    assert!(!m.records["线程"].goal_controlled);
    let a = three_idle(&mut m, 3);
    assert!(pause_request(&a).is_none());
    assert!(record(&m).loop_guard.is_none());
}

#[test]
fn 共享_外部暂停后又恢复不恢复资格() {
    let mut m = shared_awaiting(true);
    apply(&mut m, turn("turn/started", "续1"));
    apply(&mut m, goal_event("目标", "paused", 3));
    apply(&mut m, goal_event("目标", "active", 3));
    apply(&mut m, done("续1", json!([agent("说")]), "full"));
    assert!(!m.records["线程"].goal_controlled);
    let a = three_idle(&mut m, 2);
    assert!(pause_request(&a).is_none());
}

#[test]
fn 共享_自动轮里_goal_被换成别的即撤销资格() {
    let mut m = shared_awaiting(true);
    apply(&mut m, turn("turn/started", "续1"));
    apply(&mut m, goal_event("别人的目标", "active", 0));
    apply(&mut m, done("续1", json!([agent("说")]), "full"));
    assert!(!m.records["线程"].goal_controlled);
    let a = three_idle(&mut m, 2);
    assert!(pause_request(&a).is_none());
}

#[test]
fn 共享_自有客户端轮内模型新建_goal_授予资格() {
    let mut m = Machine::new(true);
    m.record("线程").subscribed = true;
    let a = apply(
        &mut m,
        client(request(
            1,
            41,
            "thread/goal/get",
            json!({"threadId": "线程"}),
        )),
    );
    reply(&mut m, &first_request(&a), Ok(json!({"goal": null})));
    let a = apply(
        &mut m,
        client(request(
            1,
            42,
            "turn/start",
            json!({"threadId": "线程", "input": [{"type": "text", "text": "干活"}]}),
        )),
    );
    let context = first_request(&a);
    apply(&mut m, turn("turn/started", "首轮"));
    reply(&mut m, &context, Ok(json!({"turn": {"id": "首轮"}})));
    apply(&mut m, goal_event("模型自己定的目标", "active", 0));
    assert!(m.records["线程"].goal_controlled);
    apply(&mut m, done("首轮", json!([agent("首轮回答")]), "full"));
    let a = three_idle(&mut m, 1);
    assert!(pause_request(&a).is_some());
}

#[test]
fn 共享_第一次见到的_goal_即使在自有轮里也不授予() {
    let mut m = Machine::new(true);
    m.record("线程").subscribed = true;
    let a = apply(
        &mut m,
        client(request(
            1,
            42,
            "turn/start",
            json!({"threadId": "线程", "input": [{"type": "text", "text": "干活"}]}),
        )),
    );
    let context = first_request(&a);
    apply(&mut m, turn("turn/started", "首轮"));
    reply(&mut m, &context, Ok(json!({"turn": {"id": "首轮"}})));
    // 无从分辨是模型刚建的还是别的客户端早就设好的。
    apply(&mut m, goal_event("早就有的目标", "active", 0));
    assert!(!m.records["线程"].goal_controlled);
}

#[test]
fn 共享_暂停回包按身份核对_被接管或失败分别报告() {
    for (result, expected) in [
        (
            Ok(goal_reply("别人的目标", "active")),
            PauseOutcome::Superseded,
        ),
        (Ok(json!({"goal": null})), PauseOutcome::Superseded),
        (Ok(goal_reply("目标", "active")), PauseOutcome::Failed),
        (
            Err(json!({"code": -32000, "message": "写失败"})),
            PauseOutcome::Failed,
        ),
    ] {
        let mut m = shared_awaiting(true);
        let a = three_idle(&mut m, 1);
        let pause = pause_request(&a).expect("应暂停 goal");
        reply(&mut m, &pause, result.clone());
        assert_eq!(
            record(&m).loop_guard.as_ref().unwrap().pause,
            Some(expected),
            "{result:?}"
        );
    }
}

/// 共享模式下刹车已发出暂停：返回机器和暂停请求。
fn shared_paused_request() -> (Machine, RequestContext) {
    let mut m = shared_awaiting(true);
    let a = three_idle(&mut m, 1);
    let pause = pause_request(&a).expect("应暂停 goal");
    (m, pause)
}

fn pause_outcome(m: &Machine) -> Option<PauseOutcome> {
    record(m).loop_guard.as_ref().and_then(|r| r.pause)
}

fn user_item(id: &str) -> Value {
    json!({"type": "userMessage", "id": id, "content": [{"type": "text", "text": "插一句"}]})
}

/// 自动候选一轮：开始并以摘要完成，返回它的补读请求。
fn summary_turn(m: &mut Machine, id: &str) -> RequestContext {
    apply(m, turn("turn/started", id));
    let a = apply(m, done(id, json!([agent("摘要")]), "summary"));
    result_read(&a, id)
}

#[test]
fn 共享_暂停超时后迟到的成功回包改为已暂停() {
    let (mut m, pause) = shared_paused_request();
    apply(
        &mut m,
        Event::TimedOut {
            tag: 9,
            deadline: std::time::Instant::now(),
            context: pause.clone(),
            error: json!({"code": -32000, "message": "超时"}),
        },
    );
    assert_eq!(pause_outcome(&m), Some(PauseOutcome::Failed));
    apply(
        &mut m,
        Event::LateReply {
            context: pause,
            result: Ok(goal_reply("目标", "paused")),
        },
    );
    assert_eq!(pause_outcome(&m), Some(PauseOutcome::Paused));
    assert!(
        m.claims.is_empty(),
        "迟到回包学到 goal 已暂停，等续跑的认领释放"
    );
}

#[test]
fn 共享_暂停回包之前_goal_已被换掉_旧回包不覆盖新事实() {
    let (mut m, pause) = shared_paused_request();
    apply(&mut m, goal_event("别人的目标", "active", 0));
    reply(&mut m, &pause, Ok(goal_reply("目标", "paused")));
    let r = &m.records["线程"];
    assert_eq!(r.goal_objective.as_deref(), Some("别人的目标"));
    assert_eq!(r.goal_status.as_deref(), Some("active"));
    assert!(r.goal_active);
    assert!(
        m.claims.contains_key("线程"),
        "新 goal 仍在进行，等续跑的认领不能被旧回包释放"
    );
    assert_eq!(pause_outcome(&m), Some(PauseOutcome::Superseded));
}

#[test]
fn 共享_暂停写入自己引起的通知先到_报已暂停() {
    let (mut m, pause) = shared_paused_request();
    apply(&mut m, goal_event("目标", "paused", 0));
    reply(&mut m, &pause, Ok(goal_reply("目标", "paused")));
    assert_eq!(pause_outcome(&m), Some(PauseOutcome::Paused));
    assert!(m.claims.is_empty());
}

#[test]
fn 共享_已暂停不被迟到的失败改写() {
    let (mut m, pause) = shared_paused_request();
    reply(&mut m, &pause, Ok(goal_reply("目标", "paused")));
    let mut actions = Vec::new();
    m.run_loop_guard_pause(RUN, PauseOutcome::Failed, &mut actions);
    assert_eq!(pause_outcome(&m), Some(PauseOutcome::Paused));
}

#[test]
fn 已看到用户插话后_旧补读回来也不刹车() {
    for shared in [false, true] {
        let mut m = if shared {
            shared_awaiting(true)
        } else {
            awaiting()
        };
        let reads: Vec<_> = (1..=3)
            .map(|i| summary_turn(&mut m, &format!("续{i}")))
            .collect();
        apply(&mut m, turn("turn/started", "续4"));
        apply(
            &mut m,
            notify(
                "item/completed",
                json!({"threadId": "线程", "turnId": "续4", "item": user_item("u")}),
            ),
        );
        if shared {
            assert!(!m.records["线程"].goal_controlled, "看到用户输入当场撤销");
        }
        let mut actions = Vec::new();
        for (i, read) in reads.iter().enumerate() {
            let id = format!("续{}", i + 1);
            actions.extend(reply(
                &mut m,
                read,
                Ok(read_reply(&id, json!([agent("全文")]), None)),
            ));
        }
        assert!(!braked(&m), "共享={shared}");
        assert!(pause_request(&actions).is_none(), "共享={shared}");
        assert!(record(&m).loop_guard.is_none(), "共享={shared}");
    }
}

#[test]
fn 共享_被挤出队列的轮补读出用户输入也撤销资格() {
    let mut m = shared_awaiting(true);
    let reads: Vec<_> = (1..=MAX_PENDING_TURNS + 1)
        .map(|i| summary_turn(&mut m, &format!("续{i}")))
        .collect();
    assert!(m.records["线程"].goal_controlled);
    reply(
        &mut m,
        &reads[0],
        Ok(read_reply(
            "续1",
            json!([user_item("u"), agent("答")]),
            None,
        )),
    );
    assert!(!m.records["线程"].goal_controlled);
}

#[test]
fn 刹车二不把有用户输入的轮算作自动轮() {
    let mut m = awaiting();
    let limit = CONTINUATION_LIMIT as usize;
    for i in 1..limit {
        busy(&mut m, &format!("续{i}"));
    }
    auto(
        &mut m,
        "插话轮",
        json!([user_item("u"), command("c"), agent("答")]),
    );
    assert!(!braked(&m), "有用户输入的轮不计入上限");
    busy(&mut m, "续末");
    let report = record(&m).loop_guard.clone().expect("第五十个合格轮应触发");
    assert_eq!(report.kind, LoopGuardKind::ContinuationLimit);
    assert_eq!(report.count, CONTINUATION_LIMIT);
    assert_eq!(report.turn_ids, vec!["续末"]);
}

#[test]
fn 共享_刹车二等第五十轮的完整证据_补读出用户输入就不刹() {
    let mut m = shared_awaiting(true);
    let limit = CONTINUATION_LIMIT as usize;
    for i in 1..limit {
        busy(&mut m, &format!("续{i}"));
    }
    apply(&mut m, turn("turn/started", "续末"));
    let a = apply(&mut m, done("续末", json!([agent("摘要")]), "summary"));
    assert!(pause_request(&a).is_none(), "证据未到不能按开轮数刹车");
    let read = result_read(&a, "续末");
    let a = reply(
        &mut m,
        &read,
        Ok(read_reply(
            "续末",
            json!([user_item("u"), agent("答")]),
            None,
        )),
    );
    assert!(pause_request(&a).is_none());
    assert!(record(&m).loop_guard.is_none());
    assert!(!m.records["线程"].goal_controlled);
}

#[test]
fn 刹车二_共享模式拿不到完整证据的轮不计入_私有模式计入() {
    for shared in [false, true] {
        let mut m = if shared {
            shared_awaiting(true)
        } else {
            awaiting()
        };
        let limit = CONTINUATION_LIMIT as usize;
        for i in 1..limit {
            busy(&mut m, &format!("续{i}"));
        }
        let read = summary_turn(&mut m, "读不到");
        let a = exhaust_read(&mut m, read);
        if shared {
            assert!(
                pause_request(&a).is_none(),
                "共享模式不能把读不到的轮算进上限"
            );
            busy(&mut m, "续末");
            let report = record(&m).loop_guard.clone().expect("下一个完整的轮才凑满");
            assert_eq!(report.turn_ids, vec!["续末"]);
        } else {
            let report = record(&m)
                .loop_guard
                .clone()
                .expect("私有模式读不到也算自动轮");
            assert_eq!(report.kind, LoopGuardKind::ContinuationLimit);
            assert_eq!(report.turn_ids, vec!["读不到"]);
        }
    }
}

/// 先见过一个旧 goal，再由本 broker 的客户端把目标换成「目标」，返回写入请求。
fn replace_by_client(m: &mut Machine) -> RequestContext {
    m.record("线程").subscribed = true;
    apply(m, goal_event("旧目标", "active", 0));
    let a = apply(
        m,
        client(request(
            1,
            40,
            "thread/goal/set",
            json!({"threadId": "线程", "objective": "目标", "status": "active"}),
        )),
    );
    first_request(&a)
}

#[test]
fn 共享_换成本次写入的目标_自己引起的通知先到也授予() {
    let mut m = Machine::new(true);
    let set = replace_by_client(&mut m);
    apply(&mut m, goal_event("目标", "active", 0));
    reply(&mut m, &set, Ok(goal_reply("目标", "active")));
    assert!(m.records["线程"].goal_controlled);
}

#[test]
fn 共享_换成本次写入的目标_回包先于通知也授予() {
    let mut m = Machine::new(true);
    let set = replace_by_client(&mut m);
    reply(&mut m, &set, Ok(goal_reply("目标", "active")));
    assert!(m.records["线程"].goal_controlled);
    // 这次写入自己引起的通知随后才到：身份不变，资格保留。
    apply(&mut m, goal_event("目标", "active", 0));
    assert!(m.records["线程"].goal_controlled);
}

#[test]
fn 共享_超时结果未知的旧写入不能豁免外部替换() {
    let mut m = Machine::new(true);
    m.record("线程").subscribed = true;
    apply(&mut m, goal_event("旧目标", "active", 0));
    let a = apply(
        &mut m,
        client(request(
            1,
            40,
            "thread/goal/set",
            json!({"threadId": "线程", "objective": "乙", "status": "active"}),
        )),
    );
    let old = first_request(&a);
    apply(
        &mut m,
        Event::TimedOut {
            tag: 3,
            deadline: std::time::Instant::now(),
            context: old,
            error: json!({"code": -32000, "message": "超时"}),
        },
    );
    let a = apply(
        &mut m,
        client(request(
            1,
            41,
            "thread/goal/set",
            json!({"threadId": "线程", "objective": "目标", "status": "active"}),
        )),
    );
    let new = first_request(&a);
    // 外部依次改成「乙」「目标」：超时的旧写入不能证明「乙」是本 broker 引起的。
    apply(&mut m, goal_event("乙", "active", 0));
    apply(&mut m, goal_event("目标", "active", 0));
    reply(&mut m, &new, Ok(goal_reply("目标", "active")));
    assert!(!m.records["线程"].goal_controlled);
}

#[test]
fn 共享_证据未到就超出记录上限的轮_保守撤销资格() {
    let mut m = shared_awaiting(true);
    let read = summary_turn(&mut m, "续1");
    for i in 2..=MAX_AUTO_TURNS {
        busy(&mut m, &format!("续{i}"));
    }
    assert!(m.records["线程"].goal_controlled, "上限之内照常");
    busy(&mut m, &format!("续{}", MAX_AUTO_TURNS + 1));
    assert!(
        !m.records["线程"].goal_controlled,
        "续1 证据未到就被挤出记录：不能排除其中有用户输入"
    );
    // 续1 的补读这时才回来：已认不出，只是不再参与判断。
    let a = reply(
        &mut m,
        &read,
        Ok(read_reply(
            "续1",
            json!([user_item("u"), agent("答")]),
            None,
        )),
    );
    assert!(pause_request(&a).is_none());
}

#[test]
fn 共享_外部把目标改走再改回_旧回包不能恢复资格() {
    let mut m = Machine::new(true);
    let set = replace_by_client(&mut m);
    apply(&mut m, goal_event("目标", "active", 0));
    apply(&mut m, goal_event("别人的目标", "active", 0));
    apply(&mut m, goal_event("目标", "active", 0));
    reply(&mut m, &set, Ok(goal_reply("目标", "active")));
    assert!(!m.records["线程"].goal_controlled);
}

#[test]
fn 否决同一轮只生效一次_插话之后的计数保留() {
    let mut m = awaiting();
    apply(&mut m, turn("turn/started", "续1"));
    apply(
        &mut m,
        notify(
            "item/completed",
            json!({"threadId": "线程", "turnId": "续1", "item": user_item("u")}),
        ),
    );
    let a = apply(&mut m, done("续1", json!([agent("摘要")]), "summary"));
    let read = result_read(&a, "续1");
    idle(&mut m, "续2");
    idle(&mut m, "续3");
    // 续1 的补读这时才回来，又一次看到用户输入：不能清掉插话之后的续2、续3。
    reply(
        &mut m,
        &read,
        Ok(read_reply(
            "续1",
            json!([user_item("u"), agent("答")]),
            None,
        )),
    );
    assert!(!braked(&m));
    idle(&mut m, "续4");
    assert_report(&m, LoopGuardKind::NoAction, &["续2", "续3", "续4"]);
}

#[test]
fn 被挤出队列的插话轮补读迟到_只清掉它之前的计数() {
    let mut m = awaiting();
    // 续1 的补读一直不回，后面的轮把它挤出等证据队列。
    let read = summary_turn(&mut m, "续1");
    for i in 2..=MAX_PENDING_TURNS {
        busy(&mut m, &format!("续{i}"));
    }
    let last = MAX_PENDING_TURNS + 1;
    idle(&mut m, &format!("续{last}"));
    idle(&mut m, &format!("续{}", last + 1));
    assert!(!braked(&m));
    // 续1 原来是用户插话的轮：只清掉开始早于它的计数，它之后的两轮保留。
    reply(
        &mut m,
        &read,
        Ok(read_reply(
            "续1",
            json!([user_item("u"), agent("答")]),
            None,
        )),
    );
    assert!(!braked(&m));
    let third = format!("续{}", last + 2);
    idle(&mut m, &third);
    let expected = [format!("续{last}"), format!("续{}", last + 1), third];
    let expected: Vec<&str> = expected.iter().map(String::as_str).collect();
    assert_report(&m, LoopGuardKind::NoAction, &expected);
}

#[test]
fn 共享_同一插话轮的补读不再撤销之后重新授予的资格() {
    let mut m = shared_awaiting(true);
    apply(&mut m, turn("turn/started", "续1"));
    apply(
        &mut m,
        notify(
            "item/completed",
            json!({"threadId": "线程", "turnId": "续1", "item": user_item("u")}),
        ),
    );
    assert!(!m.records["线程"].goal_controlled);
    let a = apply(&mut m, done("续1", json!([agent("摘要")]), "summary"));
    let read = result_read(&a, "续1");
    // 本 broker 的客户端重新把 goal 设为 active：再次授予。
    let a = apply(
        &mut m,
        client(request(
            1,
            41,
            "thread/goal/set",
            json!({"threadId": "线程", "status": "active"}),
        )),
    );
    reply(&mut m, &first_request(&a), Ok(goal_reply("目标", "active")));
    assert!(m.records["线程"].goal_controlled);
    reply(
        &mut m,
        &read,
        Ok(read_reply(
            "续1",
            json!([user_item("u"), agent("答")]),
            None,
        )),
    );
    assert!(m.records["线程"].goal_controlled, "同一轮不重复撤销");
    let a = three_idle(&mut m, 2);
    assert!(pause_request(&a).is_some());
}

#[test]
fn 共享_撤销后旧的_active_回包不能恢复资格() {
    let mut m = Machine::new(true);
    m.record("线程").subscribed = true;
    let a = apply(
        &mut m,
        client(request(
            1,
            40,
            "thread/goal/set",
            json!({"threadId": "线程", "objective": "目标", "status": "active"}),
        )),
    );
    let set = first_request(&a);
    // 写入还没回包：同一个 goal 被外部暂停、再恢复。
    apply(&mut m, goal_event("目标", "paused", 0));
    apply(&mut m, goal_event("目标", "active", 0));
    reply(&mut m, &set, Ok(goal_reply("目标", "active")));
    assert!(!m.records["线程"].goal_controlled);
}

#[test]
fn 共享_goal_变化后正在跑的自动轮不被当成客户端轮() {
    let mut m = shared_awaiting(true);
    apply(&mut m, turn("turn/started", "续1"));
    apply(&mut m, goal_event("目标二", "active", 0));
    assert!(!m.records["线程"].goal_controlled, "自动轮里换目标即撤销");
    apply(&mut m, goal_event("目标三", "active", 0));
    assert!(
        !m.records["线程"].goal_controlled,
        "计数清零后续1 仍是自动轮，不能被当成客户端轮而授予"
    );
}

#[test]
fn 共享_刹车二同样只暂停() {
    let mut m = shared_awaiting(true);
    let limit = CONTINUATION_LIMIT as usize;
    let mut last = Vec::new();
    for i in 1..=limit {
        last = busy(&mut m, &format!("续{i}"));
    }
    let pause = pause_request(&last).expect("第上限轮结束应暂停 goal");
    assert!(!interrupted(&last));
    assert_eq!(pause.params.as_ref().unwrap()["status"], "paused");
    let report = record(&m).loop_guard.clone().unwrap();
    assert_eq!(report.kind, LoopGuardKind::ContinuationLimit);
    assert_eq!(report.action, LoopGuardAction::PauseGoal);
}

#[test]
fn 运行记录的_loop_guard_字段按约定序列化() {
    let mut m = awaiting();
    let snapshot = m.run_snapshot(RUN).unwrap();
    assert!(snapshot.get("loopGuard").is_none(), "没触发时不写字段");
    idle(&mut m, "续1");
    idle(&mut m, "续2");
    idle(&mut m, "续3");
    let snapshot = m.run_snapshot(RUN).unwrap();
    assert_eq!(snapshot["interruptReason"], "goalLoop");
    assert_eq!(
        snapshot["loopGuard"],
        json!({"kind": "noAction", "count": 3, "threshold": 3, "turnIds": ["续1", "续2", "续3"],
            "action": "stop"})
    );
}
