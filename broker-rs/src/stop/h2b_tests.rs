//! H2-b：停止核实、启动超时对账、迟到启动归属经状态读取层后的行为（任务说明第 3 节新增用例）。
//! 读取一律经单步驱动回答；不依赖真实时间，不用固定延迟。
use super::*;
use crate::test_support::status_read::{error_reply, live_status_requests};

/// 共享模式下启动超时、对账读到线程空闲而释放，随后迟到回包给出「旧轮」：进入迟到归属核实。
/// 返回迟到回包产生的动作（含第一页轮次请求）。
fn late_settling() -> (Machine, Vec<Action>) {
    let (mut m, c) = started(true, "turn/start");
    let a = begin_unresolved(&mut m, &c, 70);
    snapshot(&mut m, &req(&a, "thread/read"), &[]);
    assert!(m.claims.is_empty());
    let a = apply(
        &mut m,
        Event::LateReply {
            context: c,
            result: Ok(json!({"turn":{"id":"旧轮"}})),
        },
    );
    assert!(m.stops.late[0].late_settling);
    (m, a)
}

fn status_request_of(actions: &[Action], phase: StatusPhase) -> StatusRequest {
    let request = status_requests(actions)
        .pop()
        .unwrap_or_else(|| panic!("缺少状态读取请求：{actions:?}"));
    assert_eq!(request.phase, phase);
    request
}

/// 迟到归属取得自有证据后先读 goal，再中断目标轮次。
fn interrupt_after_goal(m: &mut Machine, actions: &[Action]) -> Vec<Action> {
    reply(
        m,
        &req(actions, "thread/goal/get"),
        Ok(json!({"goal":null})),
    )
}

fn reconcile_timer_delay(actions: &[Action]) -> u64 {
    scheduled_delay(actions, |k| matches!(k, TimerKind::Reconcile { .. }))
}

#[test]
fn 迟到归属目标轮次在第二页以后仍取得自有证据() {
    let (mut m, a) = late_settling();
    let first = status_request_of(&a, StatusPhase::Turns);
    let params = first.params.clone().unwrap();
    assert_eq!(params["threadId"], "线程");
    assert_eq!(params["itemsView"], "notLoaded");
    assert!(params.get("includeTurns").is_none());
    assert!(requests(&a, "thread/read").is_empty(), "不得再发全量读");
    let a = reply_status(
        &mut m,
        &first,
        Ok(turns_page(
            &[("后来轮", "completed"), ("别人轮", "inProgress")],
            Some("第二页"),
        )),
    );
    // 第一页没有目标不等于目标不存在：不结束跟踪、不中断，继续翻页。
    assert_eq!(m.stops.late.len(), 1);
    assert!(requests(&a, "turn/interrupt").is_empty());
    assert!(requests(&a, "thread/goal/get").is_empty());
    let second = status_request_of(&a, StatusPhase::Turns);
    assert_eq!(second.params.as_ref().unwrap()["cursor"], "第二页");
    let a = reply_status(
        &mut m,
        &second,
        Ok(turns_page(&[("旧轮", "inProgress")], None)),
    );
    assert!(m.stops.late[0].ownership_proven);
    let a = interrupt_after_goal(&mut m, &a);
    let interrupt = req(&a, "turn/interrupt");
    assert_eq!(interrupt.params.unwrap()["turnId"], "旧轮");
}

#[test]
fn 迟到归属只有命中且在活动才取得证据终态或不存在才结束跟踪() {
    // (各页, 末尾元数据, 是否结束跟踪)：结束跟踪只在目标为终态或确认不存在时；否则退避重试。
    let cases: Vec<(Vec<Value>, Option<&str>, bool)> = vec![
        (vec![turns_page(&[("旧轮", "completed")], None)], None, true),
        (
            vec![turns_page(&[("旧轮", "interrupted")], None)],
            None,
            true,
        ),
        (vec![turns_page(&[("旧轮", "failed")], None)], None, true),
        // 遍历完未命中：线程非 active 才是不存在。
        (
            vec![turns_page(&[("别人轮", "completed")], None)],
            Some("idle"),
            true,
        ),
        // 遍历完未命中但线程仍 active：目标可能尚未持久化，不能结束跟踪。
        (
            vec![turns_page(&[("别人轮", "inProgress")], None)],
            Some("active"),
            false,
        ),
        (vec![turns_page(&[], None)], Some("active"), false),
        (
            vec![json!({"data":[{"id":"旧轮","status":"未知"}],"nextCursor":null})],
            None,
            false,
        ),
    ];
    for (pages, metadata, finished) in cases {
        let (mut m, mut a) = late_settling();
        for page in &pages {
            let request = status_request_of(&a, StatusPhase::Turns);
            a = reply_status(&mut m, &request, Ok(page.clone()));
        }
        if let Some(status) = metadata {
            let request = status_request_of(&a, StatusPhase::Metadata);
            a = reply_status(&mut m, &request, Ok(meta("线程", status)));
        }
        assert!(live_status_requests(&m).is_empty());
        assert!(requests(&a, "turn/interrupt").is_empty());
        assert!(requests(&a, "thread/goal/get").is_empty());
        if finished {
            assert!(m.stops.late.is_empty(), "{pages:?} {metadata:?}");
        } else {
            assert_eq!(m.stops.late.len(), 1, "{pages:?} {metadata:?}");
            assert!(m.stops.late[0].late_settling);
            assert!(!m.stops.late[0].ownership_proven);
            assert_eq!(reconcile_timer_delay(&a), VERIFY_MS);
        }
    }
}

#[test]
fn 迟到归属第一次不支持分页之后恢复() {
    let (mut m, a) = late_settling();
    let first = status_request_of(&a, StatusPhase::Turns);
    let a = reply_status(
        &mut m,
        &first,
        Err(error_reply(-32601, "list_turns is not supported yet")),
    );
    assert_eq!(m.stops.late.len(), 1, "-32601 不能结束跟踪");
    assert!(m.stops.late[0].late_settling);
    let retry = timer(&a, |k| matches!(k, TimerKind::Reconcile { .. }));
    let a = apply(&mut m, retry);
    // 不支持只作用于那一次读取：重试照常分页。
    let request = status_request_of(&a, StatusPhase::Turns);
    let a = reply_status(
        &mut m,
        &request,
        Ok(turns_page(&[("旧轮", "inProgress")], None)),
    );
    assert_eq!(m.stops.late[0].reconcile_delay_ms, 0, "读到结果后退避清零");
    let a = interrupt_after_goal(&mut m, &a);
    assert_eq!(req(&a, "turn/interrupt").params.unwrap()["turnId"], "旧轮");
}

#[test]
fn 迟到归属旧代号回包与旧轮次结果无效() {
    let (mut m, a) = late_settling();
    let first = status_request_of(&a, StatusPhase::Turns);
    let a = reply_status(&mut m, &first, Err(err()));
    let retry = timer(&a, |k| matches!(k, TimerKind::Reconcile { .. }));
    let a = apply(&mut m, retry);
    let current = status_request_of(&a, StatusPhase::Turns);
    let before = m.clone();
    // 已消费的旧请求重复回包：读取层按读取编号与代号丢弃，不改任何状态。
    let a = reply_status(
        &mut m,
        &first,
        Ok(turns_page(&[("旧轮", "inProgress")], None)),
    );
    assert!(requests(&a, "thread/goal/get").is_empty());
    assert_eq!(m.stops, before.stops);
    // 旧轮次的完成结果：处理函数按 round 丢弃。
    let reg = m.stops.late[0].clone();
    m.late_settlement_status_done(
        StatusDone {
            read_id: first.read_id,
            thread: "线程".into(),
            token: Some(reg.token.clone()),
            caller: StatusCaller::LateSettlement {
                generation: reg.generation,
                round: reg.round - 1,
            },
            result: StatusResult::Found {
                status: StatusTurn::InProgress,
                shell: json!({}),
            },
        },
        &mut Vec::new(),
    );
    assert_eq!(m.stops, before.stops);
    // 当前请求照常完成。
    let a = reply_status(
        &mut m,
        &current,
        Ok(turns_page(&[("旧轮", "completed")], None)),
    );
    assert!(m.stops.late.is_empty());
    assert!(requests(&a, "turn/interrupt").is_empty());
}

#[test]
fn 迟到归属读取无响应仍置熔断并退避重试() {
    let (mut m, a) = late_settling();
    let first = status_request_of(&a, StatusPhase::Turns);
    assert!(!m.app_server_unresponsive);
    let a = begin_unresolved(&mut m, &first.context, 71);
    assert!(m.app_server_unresponsive);
    assert!(a
        .iter()
        .any(|a| matches!(a, Action::Warn(text) if text.contains("Codex 无响应"))));
    assert_eq!(m.stops.late.len(), 1);
    assert!(m.stops.late[0].late_settling);
    assert_eq!(reconcile_timer_delay(&a), VERIFY_MS);
    // 熔断期间新流式请求被拒。
    let a = client(&mut m, "turn/start", 9, json!({"threadId":"新线程"}));
    assert!(requests(&a, "turn/start").is_empty());
}

#[test]
fn 停止核实读取超时不置熔断且不放行() {
    let mut m = running();
    let initial = stop(&mut m, 2, false);
    reply(
        &mut m,
        &req(&initial, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    let verify = verification(&mut m, &initial);
    let read = req(&verify, "thread/read");
    let a = begin_unresolved(&mut m, &read, 72);
    assert!(!m.app_server_unresponsive);
    assert!(a
        .iter()
        .any(|a| matches!(a, Action::Warn(text) if text.contains("停止核实读取未知"))));
    assert!(op(&m).read_done);
    assert!(!op(&m).turn_absent);
    reply(
        &mut m,
        &req(&verify, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    assert!(m.claims.contains_key("线程"), "未知读取不得放行");
}

#[test]
fn 核实元数据活动但轮次页暂时为空不算空闲下一轮空闲才确认() {
    let mut m = running();
    let initial = stop(&mut m, 2, false);
    reply(
        &mut m,
        &req(&initial, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    let verify = verification(&mut m, &initial);
    let read = status_request(&req(&verify, "thread/read"));
    let a = reply_status(&mut m, &read, Ok(meta("线程", "active")));
    let page = status_request_of(&a, StatusPhase::Turns);
    let a = reply_status(&mut m, &page, Ok(turns_page(&[], None)));
    assert!(requests(&a, "turn/interrupt").is_empty());
    assert!(op(&m).read_done);
    assert!(!op(&m).turn_absent);
    assert_eq!(
        op(&m).target.as_deref(),
        Some("本轮"),
        "没有可见轮次不改目标"
    );
    let a = reply(
        &mut m,
        &req(&verify, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    assert!(m.claims.contains_key("线程"));
    assert!(m.stops_busy());
    let next = apply(
        &mut m,
        timer(&a, |k| matches!(k, TimerKind::Stop(_, StopTimer::Verify))),
    );
    snapshot(&mut m, &req(&next, "thread/read"), &[]);
    let a = reply(
        &mut m,
        &req(&next, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    assert!(m.claims.is_empty());
    assert_eq!(outcome(&a, 2)["stopOutcome"], "confirmed");
}

#[test]
fn 核实活动轮次跨页按升序取第一个为新目标() {
    let mut m = running();
    let initial = stop(&mut m, 2, false);
    reply(
        &mut m,
        &req(&initial, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    let verify = verification(&mut m, &initial);
    let read = status_request(&req(&verify, "thread/read"));
    let a = reply_status(&mut m, &read, Ok(meta("线程", "active")));
    let page = status_request_of(&a, StatusPhase::Turns);
    let a = reply_status(
        &mut m,
        &page,
        Ok(turns_page(&[("最新轮", "inProgress")], Some("游标"))),
    );
    assert!(!op(&m).read_done, "未遍历完不得定案");
    let page = status_request_of(&a, StatusPhase::Turns);
    let mut all = reply_status(
        &mut m,
        &page,
        Ok(turns_page(
            &[("本轮", "completed"), ("较早轮", "inProgress")],
            None,
        )),
    );
    all.extend(reply(
        &mut m,
        &req(&verify, "thread/goal/get"),
        Ok(json!({"goal":null})),
    ));
    assert_eq!(op(&m).target.as_deref(), Some("较早轮"));
    assert_eq!(m.claims["线程"].turn_id(), Some("较早轮"));
    assert_eq!(
        req(&all, "turn/interrupt").params.unwrap()["turnId"],
        "较早轮"
    );
}

#[test]
fn 核实分页期间轮次换代从第一页重读() {
    let mut m = running();
    let initial = stop(&mut m, 2, false);
    reply(
        &mut m,
        &req(&initial, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    let verify = verification(&mut m, &initial);
    let read = status_request(&req(&verify, "thread/read"));
    let a = reply_status(&mut m, &read, Ok(meta("线程", "active")));
    let page = status_request_of(&a, StatusPhase::Turns);
    // 第一页之后同线程有轮次结束：页间不是同一快照，第二页回包到达时整次读取从元数据重读。
    let a = reply_status(
        &mut m,
        &page,
        Ok(turns_page(&[("本轮", "inProgress")], Some("游标"))),
    );
    let second = status_request_of(&a, StatusPhase::Turns);
    turn(&mut m, "turn/completed", "无关轮");
    let a = reply_status(&mut m, &second, Ok(turns_page(&[], None)));
    assert!(!op(&m).read_done, "版本变化后的结果不得采用");
    let reread = status_request_of(&a, StatusPhase::Metadata);
    assert_eq!(reread.read_id, read.read_id);
    let a = reply_status(&mut m, &reread, Ok(meta("线程", "idle")));
    assert!(op(&m).read_done);
    assert!(op(&m).turn_absent);
    let a = [
        a,
        reply(
            &mut m,
            &req(&verify, "thread/goal/get"),
            Ok(json!({"goal":null})),
        ),
    ]
    .concat();
    assert!(m.claims.is_empty());
    assert_eq!(outcome(&a, 2)["stopOutcome"], "confirmed");
}

#[test]
fn 对账元数据活动但轮次页暂时为空继续对账不算失败() {
    let (mut m, c) = started(false, "turn/start");
    let a = begin_unresolved(&mut m, &c, 73);
    let read = status_request(&req(&a, "thread/read"));
    let a = reply_status(&mut m, &read, Ok(meta("线程", "active")));
    let page = status_request_of(&a, StatusPhase::Turns);
    let a = reply_status(&mut m, &page, Ok(turns_page(&[], None)));
    // Active([]) 不是 Idle：不释放、不交给停止，按成功读取（不退避）继续对账。
    assert!(m.claims.contains_key("线程"));
    assert!(m.claims["线程"].request_pending);
    assert_eq!(m.stops.reconciliations.len(), 1);
    assert!(m.stops.operations.is_empty());
    assert_eq!(reconcile_timer_delay(&a), VERIFY_MS);
    assert_eq!(
        m.stops
            .reconciliations
            .values()
            .next()
            .unwrap()
            .reconcile_delay_ms,
        0
    );
    let a = apply(
        &mut m,
        timer(&a, |k| matches!(k, TimerKind::Reconcile { .. })),
    );
    let a = snapshot(&mut m, &req(&a, "thread/read"), &[("本轮", "inProgress")]);
    let a = reply(
        &mut m,
        &req(&a, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    assert_eq!(req(&a, "turn/interrupt").params.unwrap()["turnId"], "本轮");
}

#[test]
fn 对账第一次不支持分页之后恢复() {
    let (mut m, c) = started(false, "turn/start");
    let a = begin_unresolved(&mut m, &c, 74);
    let read = status_request(&req(&a, "thread/read"));
    let a = reply_status(&mut m, &read, Ok(meta("线程", "active")));
    let page = status_request_of(&a, StatusPhase::Turns);
    let a = reply_status(
        &mut m,
        &page,
        Err(error_reply(-32601, "list_turns is not supported yet")),
    );
    assert!(m.claims.contains_key("线程"));
    assert_eq!(m.stops.reconciliations.len(), 1);
    assert_eq!(reconcile_timer_delay(&a), VERIFY_MS);
    let a = apply(
        &mut m,
        timer(&a, |k| matches!(k, TimerKind::Reconcile { .. })),
    );
    snapshot(&mut m, &req(&a, "thread/read"), &[]);
    assert!(m.claims.is_empty(), "恢复后读到空闲即释放");
    assert!(m.stops.reconciliations.is_empty());
}

#[test]
fn 对账旧代号回包与旧轮次结果无效() {
    let (mut m, c) = started(false, "turn/start");
    let a = begin_unresolved(&mut m, &c, 75);
    let first = status_request(&req(&a, "thread/read"));
    let a = reply_status(&mut m, &first, Err(err()));
    let a = apply(
        &mut m,
        timer(&a, |k| matches!(k, TimerKind::Reconcile { .. })),
    );
    let current = status_request(&req(&a, "thread/read"));
    let before = m.clone();
    reply_status(&mut m, &first, Ok(meta("线程", "idle")));
    assert_eq!(m.stops, before.stops);
    assert_eq!(m.claims, before.claims);
    let (generation, round) = {
        let reg = m.stops.reconciliations.values().next().unwrap();
        (reg.generation, reg.round)
    };
    m.reconcile_status_done(
        StatusDone {
            read_id: first.read_id,
            thread: "线程".into(),
            token: None,
            caller: StatusCaller::Reconcile {
                generation,
                round: round - 1,
            },
            result: StatusResult::Idle,
        },
        &mut Vec::new(),
    );
    assert_eq!(m.stops, before.stops);
    assert_eq!(m.claims, before.claims);
    reply_status(&mut m, &current, Ok(meta("线程", "idle")));
    assert!(m.claims.is_empty());
}

#[test]
fn 对账认领过期后读取完成只转入迟到跟踪收尾() {
    let (mut m, c) = started(false, "turn/start");
    let a = begin_unresolved(&mut m, &c, 76);
    let read = req(&a, "thread/read");
    // 认领换代（token 不再当前），登记仍在：结果不得用于当前认领，只把登记转入迟到跟踪。
    m.claims.get_mut("线程").unwrap().claim_id += 10_000;
    let newer = m.claims["线程"].clone();
    let a = snapshot(&mut m, &read, &[("本轮", "inProgress")]);
    assert!(m.stops.reconciliations.is_empty());
    assert_eq!(m.stops.late.len(), 1);
    assert!(!m.stops.late[0].reconciling);
    assert!(m.stops.operations.is_empty());
    assert!(requests(&a, "thread/goal/get").is_empty());
    assert!(requests(&a, "turn/interrupt").is_empty());
    assert_eq!(m.claims["线程"], newer);
    // 迟到回包随后按旧编号收尾，同样不碰换代后的认领。
    let a = apply(
        &mut m,
        Event::LateReply {
            context: c,
            result: Ok(json!({"turn":{"id":"旧轮"}})),
        },
    );
    let a = interrupt_after_goal(&mut m, &a);
    assert_eq!(req(&a, "turn/interrupt").params.unwrap()["turnId"], "旧轮");
    assert_eq!(m.claims["线程"], newer);
}

#[test]
fn 对账读取期间换代按重读后的快照定案() {
    // 共享档：启动未定案时通知进入 held，但活动版本在进入 held 之前已推进。
    let (mut m, c) = started(true, "turn/start");
    let a = begin_unresolved(&mut m, &c, 77);
    let read = status_request(&req(&a, "thread/read"));
    let a = reply_status(&mut m, &read, Ok(meta("线程", "active")));
    let page = status_request_of(&a, StatusPhase::Turns);
    // 页回包前本线程有无关轮次结束：版本变化，从元数据重读；旧页（含活动轮次）不得采用。
    turn(&mut m, "turn/completed", "无关轮");
    let a = reply_status(
        &mut m,
        &page,
        Ok(turns_page(&[("本轮", "inProgress")], None)),
    );
    assert!(m.claims["线程"].request_pending);
    assert!(m.stops.operations.is_empty());
    let reread = status_request_of(&a, StatusPhase::Metadata);
    reply_status(&mut m, &reread, Ok(meta("线程", "idle")));
    assert!(m.claims.is_empty());
}

#[test]
fn 核实活动轮次扣除已结束后为空不算空闲() {
    let mut m = running();
    let initial = stop(&mut m, 2, false);
    reply(
        &mut m,
        &req(&initial, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    let verify = verification(&mut m, &initial);
    // 已独立收到结束通知的自有轮次仍出现在活动页里（存储页滞后），目标「本轮」本身没有结束证据。
    let id = m.stop_key("线程").unwrap();
    m.stops
        .operations
        .get_mut(&id)
        .unwrap()
        .ended
        .insert("已结束轮".into());
    snapshot(
        &mut m,
        &req(&verify, "thread/read"),
        &[("已结束轮", "inProgress")],
    );
    assert!(op(&m).read_done);
    assert!(!op(&m).turn_absent, "过滤后为空不能解释成线程空闲");
    assert_eq!(
        op(&m).target.as_deref(),
        Some("本轮"),
        "不把已结束轮次当目标"
    );
    assert!(
        op(&m).ended.contains("已结束轮"),
        "已收到的结束证据不被撤销"
    );
    reply(
        &mut m,
        &req(&verify, "thread/goal/get"),
        Ok(json!({"goal":null})),
    );
    assert!(m.claims.contains_key("线程"));
    assert!(m.stops_busy());
    // 目标随后真正结束，照常确认。
    let a = turn(&mut m, "turn/completed", "本轮");
    assert_eq!(outcome(&a, 2)["stopOutcome"], "confirmed");
}
