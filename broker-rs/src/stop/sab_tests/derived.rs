//! 派生停止：四种读取结果、中断前授权核对、v1 goal 制动与清除、v2 能力限制、不跳过首次核实。
use super::*;
use crate::relations::HandoffKind;
use crate::stop::tree::{TreePhase, FIRST_DELIVERY_MS};

fn stop(s: &mut Sim, params: Value) -> (u64, Vec<Action>) {
    s.call(OWNER, "broker/threads/stop", params)
}

fn reads_of(s: &Sim, from: usize, thread: &str) -> usize {
    s.log[from..]
        .iter()
        .filter(|a| {
            matches!(a, Action::Request(c)
            if c.method == "thread/read"
                && matches!(c.operation, Operation::StatusRead { .. })
                && c.params.as_ref().and_then(|p| p["threadId"].as_str()) == Some(thread))
        })
        .count()
}

#[test]
fn 读取结果为空闲_成员停止成立且不中断() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.turn_completed("子甲", "子轮甲", "completed");
    let start = s.log.len();
    let (id, _) = stop(&mut s, json!({"threadId": P}));
    assert_eq!(s.interrupts(&s.log[start..], "子甲"), 0);
    assert!(reads_of(&s, start, "子甲") >= 1, "先核实再判定");
    let reply = s.response(&s.log[start..], id).unwrap()["result"].clone();
    assert_eq!(reply["childrenStop"]["stopped"], json!(["子甲"]));
}

#[test]
fn 读取结果为活动且已绑定_中断后再核实成立() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.hold.push(("turn/interrupt".into(), "子甲".into()));
    let start = s.log.len();
    stop(&mut s, json!({"threadId": P}));
    let interrupt = s.requests(&s.log[start..], "turn/interrupt", "子甲");
    assert_eq!(interrupt.len(), 1);
    assert_eq!(interrupt[0].params.as_ref().unwrap()["turnId"], "子轮甲");
    let tree = s.tree_op().unwrap();
    // 中断已发出、等回包期间仍占名额；回包后让出，等完成通知或 15 秒再核实。
    assert_eq!(s.m.trees.operations[&tree].in_flight, 1);
    let before = s.log.len();
    s.release("turn/interrupt", "子甲");
    assert!(reads_of(&s, before, "子甲") >= 1, "完成通知触发再核实");
    assert!(s.tree_op().is_none(), "再核实为空闲后确认");
}

#[test]
fn 读取结果为活动但无活动轮次_不成立十五秒后再核实() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.hold.push(("thread/turns/list".into(), "子甲".into()));
    let start = s.log.len();
    stop(&mut s, json!({"threadId": P}));
    s.reply_status_read(
        "thread/turns/list",
        "子甲",
        Ok(crate::test_support::status_read::turns_page(&[], None)),
    );
    assert_eq!(
        s.interrupts(&s.log[start..], "子甲"),
        0,
        "Active([]) 不中断"
    );
    let tree = s.tree_op().unwrap();
    let derived = &s.m.trees.aux.derived[&(tree, "子甲".to_owned())];
    assert!(!derived.stopped, "不成立");
    let before = s.log.len();
    s.advance(TREE_RETRY - 1);
    assert_eq!(reads_of(&s, before, "子甲"), 0, "15 秒内不再读");
    s.advance(1);
    assert!(reads_of(&s, before, "子甲") >= 1, "15 秒后再核实");
}

const TREE_RETRY: u64 = crate::stop::tree::TREE_VERIFY_MS;

#[test]
fn 读取结果未知_退避后再核实() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.hold.push(("thread/read".into(), "子甲".into()));
    stop(&mut s, json!({"threadId": P}));
    s.reply_status_read(
        "thread/read",
        "子甲",
        Err(json!({"code": -32000, "message": "读失败"})),
    );
    let tree = s.tree_op().unwrap();
    let derived = &s.m.trees.aux.derived[&(tree, "子甲".to_owned())];
    assert!(!derived.stopped);
    assert!(derived.error.is_some(), "未知记为错误原因");
    let before = s.log.len();
    s.advance(crate::stop::derived::DERIVED_BACKOFF_MS[0]);
    assert!(reads_of(&s, before, "子甲") >= 1, "第一档退避后再读");
}

#[test]
fn 未绑定的活动轮次列为冲突不中断() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.turn_completed("子甲", "子轮甲", "completed");
    // v1 sendInput 本身不授予下一轮：子线程的新轮次保持 unbound。
    let (started, completed) = SubagentMock::v1_message("sendInput", P, T, "发送1", "子甲", false);
    s.apply(started);
    s.apply(completed);
    s.turn_started("子甲", "子轮2");
    assert!(!s.bound("子甲", "子轮2"));
    let start = s.log.len();
    let (id, _) = stop(&mut s, json!({"threadId": P}));
    assert_eq!(s.interrupts(&s.log[start..], "子甲"), 0, "未绑定轮次不中断");
    let actions = s.advance(FIRST_DELIVERY_MS);
    let reply = s.response(&actions, id).unwrap()["result"].clone();
    assert_eq!(reply["stopOutcome"], "unconfirmed");
    assert!(reply["childrenStop"]["conflicts"]
        .as_array()
        .unwrap()
        .iter()
        .any(|c| c["thread"] == "子甲"));
}

#[test]
fn 每次中断前核对授权_撤权后不再中断() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.hold.push(("thread/turns/list".into(), "子甲".into()));
    let start = s.log.len();
    stop(&mut s, json!({"threadId": P}));
    // 读取在途时运行被外部接管：之后的中断前核对失败。
    let mut actions = Vec::new();
    s.m.on_handoff(R, HandoffKind::External, &mut actions);
    s.absorb(actions);
    s.reply_status_read(
        "thread/turns/list",
        "子甲",
        Ok(crate::test_support::status_read::turns_page(
            &[("子轮甲", "inProgress")],
            None,
        )),
    );
    assert_eq!(s.interrupts(&s.log[start..], "子甲"), 0, "撤权后不中断");
}

#[test]
fn v1子线程按现有规则暂停goal_带clear时清除() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.goals.insert("子甲".into(), goal("子甲", "active"));
    stop(&mut s, json!({"threadId": P}));
    let writes = s.mock.state()["子甲"]["goalWrites"].clone();
    assert_eq!(writes.as_array().unwrap().len(), 1);
    assert_eq!(writes[0]["status"], "paused");
    assert_eq!(s.goals["子甲"]["status"], "paused");

    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.goals.insert("子甲".into(), goal("子甲", "active"));
    let start = s.log.len();
    let (id, _) = stop(&mut s, json!({"threadId": P, "clear": true}));
    assert_eq!(
        s.requests(&s.log[start..], "thread/goal/clear", "子甲")
            .len(),
        1
    );
    assert!(s.goals["子甲"].is_null());
    let reply = s.response(&s.log[start..], id).unwrap()["result"].clone();
    assert_eq!(reply["stopOutcome"], "confirmed");
}

#[test]
fn v2子线程不发goal写入_仍active时标能力限制且不宣称已清除() {
    let mut s = Sim::new();
    s.spawn("子乙", "活动1", "子轮乙", "v2");
    s.learn_thread("子乙");
    assert_eq!(s.m.thread_capability("子乙"), Some(false));
    s.goals.insert("子乙".into(), goal("子乙", "active"));
    let (id, _) = stop(&mut s, json!({"threadId": P, "clear": true}));
    let writes = s.mock.state()["子乙"]["goalWrites"].clone();
    assert_eq!(writes, json!([]), "v2 不发 goal 写入");
    assert!(s.interrupts(&s.log, "子乙") >= 1, "v2 允许中断");
    let actions = s.advance(FIRST_DELIVERY_MS);
    let reply = s.response(&actions, id).unwrap()["result"].clone();
    assert_eq!(reply["stopOutcome"], "unconfirmed", "能力限制不能确认");
    assert_eq!(reply["childrenStop"]["capabilityLimited"], json!(["子乙"]));
    let tree = s.tree_op().unwrap();
    assert_eq!(
        s.m.trees.aux.derived[&(tree, "子乙".to_owned())].goal_report,
        "notCleared"
    );
}

#[test]
fn 派生停止不跳过首次核实_活动成员不会未核实就判已停() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮甲");
    s.never.insert("子甲".into());
    s.hold.push(("thread/read".into(), "子甲".into()));
    stop(&mut s, json!({"threadId": P}));
    let tree = s.tree_op().unwrap();
    let derived = &s.m.trees.aux.derived[&(tree, "子甲".to_owned())];
    assert!(!derived.stopped, "核实回来之前不成立");
    assert!(!derived.verified);
    let report = &s.m.trees.operations[&tree].report.children;
    assert!(!report.stopped.contains(&"子甲".to_owned()));
    assert_ne!(report.phase, TreePhase::Confirmed);
    assert!(s.record().stop_outcome != Some(crate::runs::StopOutcome::Confirmed));
}
