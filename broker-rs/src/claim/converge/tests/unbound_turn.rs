//! 未归属轮次（子代理上没有操作归属的轮）：运行中阻塞收敛，结束后从账本移除，结果不进汇总。
//! 结束证据可以来自 turn/completed 通知，也可以在通知丢失时由发现回读补上。
//! 移出后若补上归属（消费记录迟到），按归属轮重建并进入汇总。
use super::causal_pairing::{assert_no_alarm, base, drive_run, interacted, op, pending, raw, C};
use super::placeholder::replies;
use super::rollout_pairing::{
    call_line, files, interacted_to, message_line, poll_for, turn_marker,
};
use super::*;
use crate::relations::rollout_pairing::ROLLOUT_POLL_MS;
use crate::test_support::rollout as fx;

/// 卡住的两种形态：子代理做完有归属的轮后自己再开一轮。
#[derive(Clone, Copy, Debug)]
enum Shape {
    /// 派生轮结束后自开一轮。
    AfterSpawn,
    /// 跟进轮结束后自开一轮。
    AfterFollowup,
}

fn unbound_turn_released(shape: Shape, lost: bool) {
    const U: &str = "自开轮";
    let (mut m, mut mock) = base();
    let mut all = complete(&mut m, &mut mock, C, "旧轮", "completed");
    if matches!(shape, Shape::AfterFollowup) {
        raw(&mut m, "B", "新工作");
        interacted(&mut m, "B");
        let actions = apply(&mut m, mock.turn_started(C, "跟进轮"));
        all.extend(replies(&mut m, &mock, actions));
        let actions = complete(&mut m, &mut mock, C, "跟进轮", "completed");
        all.extend(replies(&mut m, &mock, actions));
        assert!(
            m.binding(C, "跟进轮")
                .is_some_and(|b| b.status == BindingStatus::Bound),
            "前置：跟进轮已归属本运行"
        );
    }
    let actions = apply(&mut m, mock.turn_started(C, U));
    all.extend(replies(&mut m, &mock, actions));
    assert!(m.binding(C, U).is_none(), "前置：自开轮没有操作归属");
    let row = |m: &Machine| {
        m.convergences.ledgers[R]
            .activities
            .iter()
            .find(|a| a.key.turn_id.as_deref() == Some(U))
            .cloned()
    };
    let running = row(&m).expect("前置：运行中的未归属轮进入账本");
    assert!(!running.verified && running.activity == MemberActivity::Running);

    let completed = mock.turn_completed(C, U, "completed");
    if !lost {
        let actions = apply(&mut m, completed);
        all.extend(replies(&mut m, &mock, actions));
    }
    let actions = complete(&mut m, &mut mock, P, T, "completed");
    all.extend(replies(&mut m, &mock, actions));
    all.extend(drive_run(&mut m, &mock, R));
    if lost {
        assert!(
            m.relations.aux.observed[C]
                .iter()
                .any(|o| o.turn == U && o.completed),
            "通知丢失时须由发现回读补上结束证据"
        );
    }
    assert!(
        m.convergences.progress[R].reading.is_some(),
        "{shape:?} 丢通知={lost}：未归属轮结束后应进入汇总，不能等到收敛期限"
    );
    assert!(row(&m).is_none(), "已结束的未归属轮须移出账本");
    assert!(
        m.convergences.ledgers[R]
            .results
            .iter()
            .all(|r| r.locator.turn_id.as_deref() != Some(U)),
        "未归属轮的结果不进汇总"
    );
    confirm_parent(&mut m);
    let actions = complete(&mut m, &mut mock, P, "汇总轮", "completed");
    all.extend(replies(&mut m, &mock, actions));
    assert_eq!(m.runs.runs[R].state, RunState::Ended);
    assert_eq!(m.runs.runs[R].status, Some(RunStatus::Completed));
    assert_no_alarm(&all);
}

#[test]
fn 未归属轮_派生轮后自开一轮_收到结束通知后进入汇总() {
    unbound_turn_released(Shape::AfterSpawn, false);
}

#[test]
fn 未归属轮_派生轮后自开一轮_结束通知丢失由发现回读补上() {
    unbound_turn_released(Shape::AfterSpawn, true);
}

#[test]
fn 未归属轮_跟进轮后自开一轮_收到结束通知后进入汇总() {
    unbound_turn_released(Shape::AfterFollowup, false);
}

#[test]
fn 未归属轮_跟进轮后自开一轮_结束通知丢失由发现回读补上() {
    unbound_turn_released(Shape::AfterFollowup, true);
}

/// 被消息唤醒的那一轮的轮号。消费方向按轮号里的开始时间登记，要用上游的格式（UUIDv7），开始时间晚于 core 启动。
const MESSAGE_TURN: &str = "ffffffff-ffff-7000-8000-000000000001";

/// 子代理 C 的一轮由消费的消息唤醒、没有本运行的操作归属。经真实通知入口（raw 消费记录、条目通知）走到摘要暂存到期，
/// 返回到期那一步之前的全部动作。
fn consumed_turn_expired(author: &str) -> Vec<Action> {
    const U: &str = MESSAGE_TURN;
    let (mut m, mut mock) = base();
    let mut all = complete(&mut m, &mut mock, C, "旧轮", "completed");
    let actions = apply(&mut m, mock.turn_started(C, U));
    all.extend(replies(&mut m, &mock, actions));
    let text = format!(
        "Message Type: MESSAGE\nTask name: /root/{C}\nSender: {author}\nPayload:\n请帮忙核对"
    );
    let actions = notify(
        &mut m,
        "rawResponseItem/completed",
        json!({"threadId":C,"turnId":U,"item":{
        "type":"agent_message","id":"amsg-1","author":author,"recipient":format!("/root/{C}"),
        "content":[{"type":"input_text","text":text}]}}),
    );
    all.extend(replies(&mut m, &mock, actions));
    let item = |id: &str| json!({"threadId":C,"turnId":U,"item":{"id":id,"type":"agentMessage","text":"处理中"}});
    let actions = notify(&mut m, "item/completed", item("条目一"));
    all.extend(replies(&mut m, &mock, actions));
    assert!(m.binding(C, U).is_none(), "前置：U 没有本运行的操作归属");
    // 下一条摘要到达时先处理已到期的暂存（与计时器到期走同一出口）。
    m.clock.mono_ms += 31_000;
    m.clock.now_ms += 31_000;
    let actions = notify(&mut m, "item/completed", item("条目二"));
    all.extend(replies(&mut m, &mock, actions));
    all
}

/// 兄弟子代理之间互发：这一轮只消费了平级发来的协作消息，本来就不归本运行管，暂存到期正常丢弃，不留「记录缺口」。
/// 对照：同样没有归属证据、消费的却是直接上级（根）的消息，照常留缺口。
#[test]
fn 记录缺口_只被平级消息唤醒的轮到期不告警_上级消息照常告警() {
    assert_no_alarm(&consumed_turn_expired("/root/兄弟"));
    let control = consumed_turn_expired("/root");
    let gap = control.iter().any(|a| {
        matches!(a, Action::Store(crate::store::StoreOp::AppendLog { lines, .. })
            if lines.iter().any(|l| l.contains("beforeBindingUnknown") && l.contains(MESSAGE_TURN)))
    });
    assert!(gap, "对照：上级消息唤醒却无归属证据，必须留缺口");
}

/// 迟到归属的完整往返：文件严格跟进在等消费记录时，子代理开出承载轮 U，记录还没读到，U 暂时未归属；
/// U 结束后移出账本，这期间跟进的等待和占位仍阻塞收尾。记录读到后 U 归属这次跟进，
/// 按归属轮重建、核实并带上结果引用，汇总读取包含 U。
#[test]
fn 未归属轮_结束移出后消费记录迟到补上归属_重建并进入汇总() {
    const U: &str = "承载轮";
    let row = |m: &Machine, turn: Option<&str>| {
        m.convergences.ledgers[R]
            .activities
            .iter()
            .find(|a| a.key.thread_id == C && a.key.turn_id.as_deref() == turn)
            .cloned()
    };
    let (mut m, mut mock) = base();
    files(&m);
    let mut all = complete(&mut m, &mut mock, C, "旧轮", "completed");
    call_line(&m, "跟进", "followup_task", C, "新工作");
    all.extend(interacted_to(&mut m, "跟进", C));
    assert!(op(&m, "跟进").v2_strict, "前置：按会话文件严格配对");
    pending(&m, 1);

    // 子代理读到跟进开出 U，消费记录已写入文件，但读取被暂扣。
    m.clock.now_ms += 20;
    turn_marker(C, U, m.clock.now_ms);
    message_line(&m, C, "入站", "NEW_TASK", "新工作");
    fx::with(|f| f.hold = true);
    let started_at = m.clock.mono_ms;
    let actions = apply(&mut m, mock.turn_started(C, U));
    all.extend(replies(&mut m, &mock, actions));
    all.extend(fx::poll(&mut m));
    assert!(
        m.binding(C, U).is_none(),
        "前置：消费记录读到之前 U 没有归属"
    );
    assert!(
        row(&m, Some(U)).is_some_and(|a| !a.verified),
        "前置：运行中的 U 以未核实活动进入账本"
    );

    // U 结束：移出账本；跟进尚未核销，等待与占位继续阻塞。
    let actions = complete(&mut m, &mut mock, C, U, "completed");
    all.extend(replies(&mut m, &mock, actions));
    assert!(row(&m, Some(U)).is_none(), "已结束的未归属轮移出账本");
    assert!(
        row(&m, None).is_some_and(|a| !a.verified),
        "占位原因仍在，同一次同步重建占位"
    );
    pending(&m, 1);
    let actions = complete(&mut m, &mut mock, P, T, "completed");
    all.extend(replies(&mut m, &mock, actions));
    all.extend(drive_run(&mut m, &mock, R));
    assert_eq!(m.runs.runs[R].state, RunState::Running);
    assert!(
        m.convergences.progress[R].reading.is_none(),
        "跟进未核销前不得进入汇总"
    );

    // 消费记录读到：U 归属这次跟进，按归属轮重建。
    assert!(
        m.clock.mono_ms >= started_at + 30_000,
        "前置：归属确认晚于 30 秒摘要暂存期限"
    );
    all.extend(fx::release_held(&mut m));
    all.extend(poll_for(&mut m, &mock, ROLLOUT_POLL_MS));
    pending(&m, 0);
    // 续期的摘要在归属确认后按到达顺序补写，各写一次。
    let late: Vec<String> = all
        .iter()
        .flat_map(|a| match a {
            Action::Store(crate::store::StoreOp::AppendLog { lines, .. }) => lines.clone(),
            _ => vec![],
        })
        .filter(|l| l.contains(U) && l.contains("\"late\":true"))
        .map(|l| l.split(' ').nth(2).unwrap_or_default().to_owned())
        .collect();
    assert_eq!(
        late.first().map(String::as_str),
        Some("轮次开始"),
        "补写从 U 的开始摘要起：{late:?}"
    );
    assert_eq!(
        late.last().map(String::as_str),
        Some("轮次完成"),
        "补写到 U 的完成摘要止：{late:?}"
    );
    for kind in ["轮次开始", "轮次完成"] {
        assert_eq!(
            late.iter().filter(|k| k.as_str() == kind).count(),
            1,
            "{kind} 不重复补写：{late:?}"
        );
    }
    assert_eq!(op(&m, "跟进").proven_turns[C], [U.to_owned()].into());
    assert!(
        m.binding(C, U)
            .is_some_and(|b| b.status == BindingStatus::Bound),
        "U 归属本运行"
    );
    assert!(row(&m, None).is_none(), "占位撤销");
    let rebuilt = row(&m, Some(U)).expect("补上归属后按归属轮重建");
    assert!(rebuilt.verified, "重建的 U 已核实");
    assert_eq!(rebuilt.activity, MemberActivity::Settled);
    assert!(rebuilt.result_status.is_some(), "重建的 U 带终态");
    assert!(
        m.convergences.ledgers[R].results.iter().any(|r| {
            r.key == rebuilt.key && r.locator.turn_id.as_deref() == Some(U) && r.available
        }),
        "重建的 U 有结果引用"
    );

    // 进入汇总，汇总读取包含 U。
    all.extend(drive_run(&mut m, &mock, R));
    assert!(
        m.convergences.progress[R].reading.is_some(),
        "补上归属后进入汇总"
    );
    // 汇总读取按收敛事实里的活动与结果引用逐条发读（relations/results.rs 的 read_child_results）。
    let facts = m.convergence_facts(R).expect("汇总时有收敛事实");
    assert!(
        facts.activities.iter().any(|a| a.key == rebuilt.key)
            && facts.result_refs.iter().any(|r| {
                r.key == rebuilt.key && r.locator.turn_id.as_deref() == Some(U) && r.available
            }),
        "汇总读取包含 U 的结果"
    );
    confirm_parent(&mut m);
    let actions = complete(&mut m, &mut mock, P, "汇总轮", "completed");
    all.extend(replies(&mut m, &mock, actions));
    assert_eq!(m.runs.runs[R].state, RunState::Ended);
    assert_eq!(m.runs.runs[R].status, Some(RunStatus::Completed));
    // 归属确认晚于摘要暂存期限（30 秒）：跟进证据还在等消费记录，暂存续期，补上归属后补写，不留缺口。
    assert_no_alarm(&all);
}
