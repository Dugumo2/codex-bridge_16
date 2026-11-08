//! 未归属轮次（子代理上没有操作归属的轮）：运行中阻塞收敛，结束后从账本移除，结果不进汇总。
//! 结束证据可以来自 turn/completed 通知，也可以在通知丢失时由发现回读补上。
use super::causal_pairing::{assert_no_alarm, base, drive_run, interacted, raw, C};
use super::placeholder::replies;
use super::*;

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
