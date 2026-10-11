//! 模型登记的资格与逐轮改道：经真实状态机入口验证乱序，避免把轮次关联当开轮证据。
use super::*;
use crate::runs::{RunRecord, RunState};
use crate::store::StoreOp;

const RUN: &str = "run-测试-000001";

fn confirmed(m: &Machine) -> bool {
    m.runs.runs[RUN].start_confirmation.confirmed
}

fn persisted(actions: &[Action], id: &str) -> RunRecord {
    actions
        .iter()
        .rev()
        .find_map(|action| match action {
            Action::Store(StoreOp::PutRun(record)) if record.run_id == id => Some(*record.clone()),
            _ => None,
        })
        .expect("应持久化运行记录")
}

#[test]
fn mi_开轮回包先到必须等待共享自有定案() {
    let (mut m, context) = start(true, "turn/start");
    let actions = reply(&mut m, &context, Ok(json!({"turn":{"id":"本轮"}})));
    assert!(!confirmed(&m));
    let evidence = &persisted(&actions, RUN).start_confirmation;
    assert_eq!(evidence.turn_id.as_deref(), Some("本轮"));
    assert_eq!(evidence.owned, None);
    let actions = super::settlement::settle_active(&mut m, &["本轮"]);
    assert!(confirmed(&m));
    assert!(persisted(&actions, RUN).start_confirmation.confirmed);
}

#[test]
fn mi_先定案自有仍须开轮证据且迟到回包可补() {
    for notification in [false, true] {
        let (mut m, context) = start(true, "turn/start");
        // 成功回包不带轮次时，真实定案入口会先定案自有。
        reply(&mut m, &context, Ok(json!({})));
        assert_eq!(m.runs.runs[RUN].start_confirmation.owned, Some(true));
        assert!(!confirmed(&m));
        let actions = if notification {
            apply(&mut m, turn("turn/started", "本轮"))
        } else {
            reply(&mut m, &context, Ok(json!({"turn":{"id":"本轮"}})))
        };
        assert!(confirmed(&m));
        assert_eq!(
            persisted(&actions, RUN)
                .start_confirmation
                .thread_id
                .as_deref(),
            Some("线程")
        );
    }
}

#[test]
fn mi_仅通知也能确认私有轮而共享缓冲须等待定案() {
    for shared in [false, true] {
        let (mut m, context) = start(shared, "turn/start");
        apply(&mut m, turn("turn/started", "本轮"));
        assert_eq!(confirmed(&m), !shared);
        if shared {
            // 回包没有轮次，只有之前缓冲的 started 能提供开轮证据。
            reply(&mut m, &context, Ok(json!({})));
            assert!(confirmed(&m));
        }
        assert_eq!(
            m.runs.runs[RUN].start_confirmation.turn_id.as_deref(),
            Some("本轮")
        );
    }
}

#[test]
fn mi_吸收或主动joined及steer均不确认自有轮() {
    for explicit in [false, true] {
        let (mut m, context) = start(true, "turn/start");
        reply(&mut m, &context, Ok(json!({"turn":{"id":"外轮"}})));
        if explicit {
            apply(
                &mut m,
                client(request(
                    1,
                    90,
                    "broker/joined",
                    json!({"threadId":"线程","turnId":"外轮"}),
                )),
            );
        } else {
            // 吸收判定发现另一个活动轮，真实读取路径会定案 joined。
            super::settlement::settle_active(&mut m, &["别人的轮"]);
        }
        assert!(m.runs.runs[RUN].joined);
        assert_eq!(m.runs.runs[RUN].start_confirmation.owned, Some(false));
        assert!(!confirmed(&m));
        apply(&mut m, turn("turn/started", "外轮"));
        assert!(!confirmed(&m));
    }
    let (mut m, context) = start(false, "turn/steer");
    reply(&mut m, &context, Ok(json!({"turn":{"id":"本轮"}})));
    apply(&mut m, turn("turn/started", "本轮"));
    assert!(!confirmed(&m));
}

#[test]
fn mi_开轮失败被拒空轮次及确认前失联都不合格() {
    for shared in [false, true] {
        for result in [
            Err(json!({"code":-32600,"message":"拒绝开轮"})),
            Ok(json!({"turn":{"id":""}})),
        ] {
            let (mut m, context) = start(shared, "turn/start");
            reply(&mut m, &context, result);
            assert!(!confirmed(&m));
        }
        let (mut m, context) = start(shared, "turn/start");
        apply(&mut m, Event::Shutdown);
        reply(&mut m, &context, Ok(json!({"turn":{"id":"迟到轮"}})));
        assert_eq!(m.runs.runs[RUN].state, RunState::Lost);
        assert!(!confirmed(&m));
    }
}

#[test]
fn mi_确认后完成中断或失联不撤销资格() {
    for end in ["completed", "interrupted", "lost"] {
        let (mut m, context) = start(false, "turn/start");
        reply(&mut m, &context, Ok(json!({"turn":{"id":"本轮"}})));
        assert!(confirmed(&m));
        if end == "lost" {
            apply(&mut m, Event::Shutdown);
        } else {
            apply(
                &mut m,
                notify(
                    "turn/completed",
                    json!({"threadId":"线程","turn":{"id":"本轮","status":end,"items":[]}}),
                ),
            );
        }
        assert!(confirmed(&m));
        assert_ne!(m.runs.runs[RUN].state, RunState::Running);
    }
}

#[test]
fn mi_迟到启动回包只补原运行不能借当前认领() {
    stale_start_reply(false);
}

#[test]
fn mi_rpc超时后迟到回包只补原运行不能借当前认领() {
    stale_start_reply(true);
}

fn stale_start_reply(late: bool) {
    let (mut m, old) = start(false, "turn/start");
    // 完整结果先到，普通释放且没有补读；原请求仍在途，新运行创建会触发淘汰。
    let completed = apply(
        &mut m,
        notify(
            "turn/completed",
            json!({"threadId":"线程","turn":{"id":"旧轮","status":"completed","items":[{"id":"输出","type":"agentMessage","phase":"final_answer","text":"完整结果"}]}}),
        ),
    );
    assert!(m.claims.is_empty());
    assert!(m.runs.verifying_history.is_empty());
    assert!(!completed.iter().any(|action| matches!(action, Action::Request(context) if matches!(context.operation, Operation::ResultRead(_)))));
    assert!(!confirmed(&m));
    m.record("线程").subscribed = true;
    apply(
        &mut m,
        Event::ClientRequest {
            request: request(2, 43, "turn/start", json!({"threadId":"线程"})),
            run_id: Some("run-new".into()),
        },
    );
    assert!(m.runs.runs.contains_key(RUN));
    let actions = if late {
        apply(
            &mut m,
            Event::LateReply {
                context: old,
                result: Ok(json!({"turn":{"id":"旧轮"}})),
            },
        )
    } else {
        reply(&mut m, &old, Ok(json!({"turn":{"id":"旧轮"}})))
    };
    assert!(confirmed(&m));
    assert_eq!(
        persisted(&actions, RUN)
            .start_confirmation
            .turn_id
            .as_deref(),
        Some("旧轮")
    );
    assert!(!m.runs.runs["run-new"].start_confirmation.confirmed);
    assert!(m.runs.runs["run-new"].start_confirmation.turn_id.is_none());
    assert!(!m.runs.inflight_starts.contains(RUN));
    assert!(!m.runs.late_starts.contains(RUN));
    apply(
        &mut m,
        Event::ClientRequest {
            request: request(2, 44, "turn/start", json!({"threadId":"其他线程"})),
            run_id: Some("run-other".into()),
        },
    );
    assert!(
        !m.runs.runs.contains_key(RUN),
        "回包消费后解除保护，后续创建可正常淘汰原运行"
    );
}

#[test]
fn mi_旧轮迟到started不能确认新运行或改变新认领() {
    let (mut m, _old) = start(false, "turn/start");
    apply(
        &mut m,
        notify(
            "turn/completed",
            json!({"threadId":"线程","turn":{"id":"旧轮","status":"completed","items":[{"id":"输出","type":"agentMessage","phase":"final_answer","text":"完整结果"}]}}),
        ),
    );
    apply(
        &mut m,
        Event::ClientRequest {
            request: request(2, 43, "turn/start", json!({"threadId":"线程"})),
            run_id: Some("run-new".into()),
        },
    );
    let claim = m.claims["线程"].clone();
    let record = m.runs.runs["run-new"].clone();
    apply(&mut m, turn("turn/started", "旧轮"));
    assert_eq!(m.claims["线程"], claim);
    assert_eq!(m.runs.runs["run-new"], record);
    assert!(!m.runs.runs["run-new"].start_confirmation.confirmed);
}

#[test]
fn mi_恢复屏障前外部started不能确认尚未提交的新运行() {
    let mut m = Machine::new(false);
    m.record("线程").subscribed = false;
    let actions = apply(
        &mut m,
        client(request(1, 1, "turn/start", json!({"threadId":"线程"}))),
    );
    assert_eq!(first_request(&actions).operation, Operation::ResumeBarrier);
    apply(&mut m, turn("turn/started", "外部轮"));
    assert!(!confirmed(&m));
    assert!(m.runs.runs[RUN].start_confirmation.turn_id.is_none());
}

#[test]
fn mi_当前轮只前进且迟到回包通知完成均不回退() {
    for reply_first in [false, true] {
        let (mut m, context) = start(false, "turn/start");
        if reply_first {
            reply(&mut m, &context, Ok(json!({"turn":{"id":"首轮"}})));
        }
        let actions = apply(&mut m, turn("turn/started", "首轮"));
        assert_eq!(m.runs.runs[RUN].current_turn_id.as_deref(), Some("首轮"));
        if !reply_first {
            assert_eq!(
                persisted(&actions, RUN).current_turn_id.as_deref(),
                Some("首轮")
            );
        }
        // 故意缺首轮 completed，检验开始通知的历史而不是只查已完成结果。
        let actions = apply(&mut m, turn("turn/started", "次轮"));
        assert_eq!(
            persisted(&actions, RUN).current_turn_id.as_deref(),
            Some("次轮")
        );
        apply(&mut m, rerouted("线程", "次轮"));
        reply(&mut m, &context, Ok(json!({"turn":{"id":"首轮"}})));
        apply(&mut m, turn("turn/started", "首轮"));
        assert_eq!(m.runs.runs[RUN].current_turn_id.as_deref(), Some("次轮"));
        assert_eq!(
            m.runs.runs[RUN].pending_reroute.as_ref().unwrap().turn_id,
            "次轮"
        );
        apply(&mut m, turn("turn/completed", "首轮"));
        assert_eq!(m.runs.runs[RUN].current_turn_id.as_deref(), Some("次轮"));
    }
}

fn rerouted(thread: &str, turn: &str) -> Event {
    notify(
        "model/rerouted",
        json!({"threadId":thread,"turnId":turn,"fromModel":"astra","toModel":"sol","reason":"highRiskCyberActivity"}),
    )
}

#[test]
fn mi_改道仅接受所属运行当前轮且暂存立即持久化() {
    let (mut m, context) = start(false, "turn/start");
    apply(&mut m, rerouted("线程", "本轮"));
    assert!(m.runs.runs[RUN].pending_reroute.is_none());
    reply(&mut m, &context, Ok(json!({"turn":{"id":"本轮"}})));
    for event in [
        rerouted("别的线程", "本轮"),
        rerouted("线程", "别的轮"),
        notify(
            "model/rerouted",
            json!({"threadId":"线程","turnId":"本轮","fromModel":123}),
        ),
    ] {
        apply(&mut m, event);
        assert!(m.runs.runs[RUN].pending_reroute.is_none());
    }
    m.runs.runs.get_mut(RUN).unwrap().meta =
        Some(json!({"source":"run","model":"astra","effort":"high"}));
    let actions = apply(&mut m, rerouted("线程", "本轮"));
    let record = persisted(&actions, RUN);
    assert_eq!(
        serde_json::to_value(record.pending_reroute).unwrap(),
        json!({"turnId":"本轮","fromModel":"astra","toModel":"sol","reason":"highRiskCyberActivity"})
    );
    assert_eq!(record.meta.unwrap()["model"], "astra");
}

#[test]
fn mi_改道完成清空且目标自动续轮不残留() {
    let (mut m, context) = start(false, "turn/start");
    reply(&mut m, &context, Ok(json!({"turn":{"id":"本轮"}})));
    apply(
        &mut m,
        notify(
            "thread/goal/updated",
            json!({"threadId":"线程","goal":{"status":"active"}}),
        ),
    );
    apply(&mut m, rerouted("线程", "本轮"));
    let actions = apply(&mut m, turn("turn/completed", "本轮"));
    let record = persisted(&actions, RUN);
    assert!(record.pending_reroute.is_none());
    assert_eq!(record.turns[0].reroute.as_ref().unwrap().to_model, "sol");
    apply(&mut m, turn("turn/started", "次轮"));
    apply(&mut m, rerouted("线程", "本轮"));
    apply(&mut m, turn("turn/completed", "次轮"));
    assert_eq!(m.runs.runs.len(), 1);
    assert_eq!(m.runs.runs[RUN].turns.len(), 2);
    assert!(m.runs.runs[RUN].turns[1].reroute.is_none());
    assert!(m.runs.runs[RUN].pending_reroute.is_none());
}

#[test]
fn mi_共享定案回放改道按轮消费() {
    let (mut m, context) = start(true, "turn/start");
    apply(&mut m, turn("turn/started", "本轮"));
    apply(&mut m, rerouted("线程", "本轮"));
    apply(&mut m, turn("turn/completed", "本轮"));
    assert!(m.runs.runs[RUN].pending_reroute.is_none());
    reply(&mut m, &context, Ok(json!({"turn":{"id":"本轮"}})));
    assert!(confirmed(&m));
    assert!(m.runs.runs[RUN].pending_reroute.is_none());
    assert_eq!(
        m.runs.runs[RUN].turns[0].reroute.as_ref().unwrap().to_model,
        "sol"
    );
}
