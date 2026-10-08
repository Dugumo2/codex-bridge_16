//! 能力门禁与生命周期门禁：子线程直接输入、rm/purge 的安全检查集合与发现检查。
use super::*;
use crate::proto::code;

fn start_child(s: &mut Sim, thread: &str) -> (u64, Vec<Action>) {
    s.m.records.entry(thread.into()).or_default().subscribed = true;
    s.call(
        2,
        "turn/start",
        json!({"threadId": thread, "input": [{"type": "text", "text": "直接输入"}]}),
    )
}

fn forwarded_start(actions: &[Action], thread: &str) -> bool {
    actions.iter().any(|a| matches!(a, Action::Request(c)
        if c.method == "turn/start" && c.params.as_ref().and_then(|p| p["threadId"].as_str()) == Some(thread)))
}

/// 父运行正常结束（子代理先完成）。
fn finished_run(s: &mut Sim, child: &str, version: &str) {
    s.spawn(child, "派生1", "子轮1", version);
    s.learn_thread(child);
    s.turn_completed(child, "子轮1", "completed");
    s.turn_completed(P, T, "completed");
    assert!(!s.m.claims.contains_key(P));
}

#[test]
fn 不接受直接输入的子线程拒绝resume与continue() {
    let mut s = Sim::new();
    finished_run(&mut s, "子乙", "v2");
    assert_eq!(s.m.thread_capability("子乙"), Some(false));
    let (id, actions) = s.call(2, "thread/resume", json!({"threadId": "子乙"}));
    assert_eq!(
        s.response(&actions, id).unwrap()["error"]["data"]["reason"],
        "subagentNoDirectInput"
    );
    let (id, actions) = start_child(&mut s, "子乙");
    let error = s.response(&actions, id).unwrap()["error"].clone();
    assert_eq!(error["code"], code::GENERAL_ERROR);
    assert_eq!(error["data"]["reason"], "subagentNoDirectInput");
    assert_eq!(error["data"]["parentThreadId"], P);
    assert!(
        !forwarded_start(&actions, "子乙"),
        "不静默改投到父线程，也不转发"
    );
    // 续跑同样拒绝。
    let (id, actions) = s.call(
        2,
        "turn/start",
        json!({"threadId": "子乙", "input": [], "cxbContinue": {"fromRunId": "旧"}}),
    );
    assert_eq!(
        s.response(&actions, id).unwrap()["error"]["data"]["reason"],
        "subagentNoDirectInput"
    );
}

#[test]
fn 未知父线程归档删除必须发现_有子树或发现失败拒绝() {
    for method in [
        "broker/threads/archive",
        "thread/archive",
        "thread/delete",
        "broker/threads/purge",
    ] {
        for failed in [false, true] {
            let mut s = Sim::new();
            s.mock.register("冷父", None, "v1", false, false);
            s.mock.register("冷子", Some("冷父"), "v2", false, false);
            s.children.insert("冷父".into(), vec!["冷子".into()]);
            s.list_fault = failed;
            assert!(s.m.thread_relation("冷子").is_none());
            let (id, actions) = s.call(
                OWNER,
                method,
                json!({"threadId": "冷父", "serverDelete": true}),
            );
            let list = blockers(&s, &actions, id);
            assert!(
                list.iter().any(|b| b["reason"]
                    == if failed {
                        "discoveryIncomplete"
                    } else {
                        "hasSubtree"
                    }),
                "{list:?}"
            );
            assert!(!actions.iter().any(|a| matches!(a, Action::Request(c) if c.method == "thread/archive" || c.method == "thread/delete")));
        }
    }
}

#[test]
fn 可接受输入的子线程_仍在活动或属于未提交运行时拒绝_结束后放行() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮1");
    s.learn_thread("子甲");
    assert_eq!(s.m.thread_capability("子甲"), Some(true));
    let (id, actions) = start_child(&mut s, "子甲");
    let error = s.response(&actions, id).unwrap()["error"].clone();
    assert_eq!(error["code"], code::THREAD_BUSY);
    assert_eq!(error["data"]["reason"], "subagentBusy");
    assert_eq!(error["data"]["runId"], R);
    // 子线程完成、父运行结束后：沿用现有行为。
    s.turn_completed("子甲", "子轮1", "completed");
    s.turn_completed(P, T, "completed");
    let (_, actions) = start_child(&mut s, "子甲");
    assert!(forwarded_start(&actions, "子甲"));
}

#[test]
fn 能力未知的子线程交给上游校验() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮1");
    s.turn_completed("子甲", "子轮1", "completed");
    s.turn_completed(P, T, "completed");
    assert_eq!(s.m.thread_capability("子甲"), None);
    let (_, actions) = start_child(&mut s, "子甲");
    assert!(forwarded_start(&actions, "子甲"));
}

fn blockers(s: &Sim, actions: &[Action], id: u64) -> Vec<Value> {
    let error = s.response(actions, id).expect("有回包")["error"].clone();
    assert_eq!(error["code"], code::THREAD_BUSY, "{error}");
    assert_eq!(error["data"]["reason"], "threadBusy");
    error["data"]["blockers"].as_array().unwrap().clone()
}

#[test]
fn 有子树的父线程拒绝归档与删除并逐条列原因() {
    let mut s = Sim::new();
    finished_run(&mut s, "子甲", "v1");
    let (id, actions) = s.call(OWNER, "broker/threads/archive", json!({"threadId": P}));
    let list = blockers(&s, &actions, id);
    let subtree = list
        .iter()
        .find(|b| b["reason"] == "hasSubtree")
        .expect("列出子树");
    assert_eq!(subtree["children"], json!(["子甲"]));
    let (id, actions) = s.call(
        OWNER,
        "broker/threads/purge",
        json!({"threadId": P, "serverDelete": true}),
    );
    assert!(blockers(&s, &actions, id)
        .iter()
        .any(|b| b["reason"] == "hasSubtree"));
    let (id, actions) = s.call(OWNER, "broker/threads/purge", json!({"threadId": P}));
    assert!(blockers(&s, &actions, id)
        .iter()
        .any(|b| b["reason"] == "hasSubtree"));
}

#[test]
fn 正被运行使用的子线程拒绝删除() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮1");
    let (id, actions) = s.call(OWNER, "broker/threads/archive", json!({"threadId": "子甲"}));
    let list = blockers(&s, &actions, id);
    assert!(list
        .iter()
        .any(|b| b["reason"] == "subagentInUse" && b["runId"] == R));
}

#[test]
fn 树停止未确认时拒绝删除() {
    let mut s = Sim::new();
    s.spawn_v1("子甲", "派生1", "子轮1");
    s.never.insert("子甲".into());
    s.turn_completed(P, T, "completed");
    s.advance(crate::stop::tree::FIRST_DELIVERY_MS);
    let (id, actions) = s.call(OWNER, "broker/threads/archive", json!({"threadId": P}));
    let list = blockers(&s, &actions, id);
    assert!(list.iter().any(|b| b["reason"] == "treeStopUnconfirmed"));
}

#[test]
fn 子代理目标先向上游发现子树_无后代放行_有后代或不完整拒绝() {
    // 已结束运行里的子代理：已知事实不挡，先发现它的后代。
    let mut s = Sim::new();
    finished_run(&mut s, "子甲", "v1");
    let (id, actions) = s.call(OWNER, "broker/threads/archive", json!({"threadId": "子甲"}));
    assert!(
        s.response(&actions, id).is_none_or(|r| r
            .get("error")
            .is_none_or(|e| e["data"]["reason"] != "threadBusy")),
        "没有后代、发现完整：放行原请求"
    );
    assert!(s.m.trees.aux.lifecycle.is_empty());

    // 发现到孙线程：拒绝并列出。
    let mut s = Sim::new();
    finished_run(&mut s, "子甲", "v1");
    s.mock.register("孙线程", Some("子甲"), "v1", false, false);
    s.children.insert("子甲".into(), vec!["孙线程".into()]);
    s.m.relations.edges.retain(|k, _| k != "孙线程");
    let (id, actions) = s.call(OWNER, "broker/threads/archive", json!({"threadId": "子甲"}));
    let list = blockers(&s, &actions, id);
    assert!(list.iter().any(|b| b["reason"] == "hasSubtree"), "{list:?}");

    // 发现失败：不完整，拒绝。
    let mut s = Sim::new();
    finished_run(&mut s, "子甲", "v1");
    s.list_fault = true;
    let (id, actions) = s.call(OWNER, "broker/threads/archive", json!({"threadId": "子甲"}));
    let list = blockers(&s, &actions, id);
    assert!(
        list.iter().any(|b| b["reason"] == "discoveryIncomplete"),
        "{list:?}"
    );
}

#[test]
fn 无子代理事实的线程保持原归档行为() {
    let mut s = Sim::new();
    s.turn_completed(P, T, "completed");
    let (id, actions) = s.call(OWNER, "broker/threads/archive", json!({"threadId": P}));
    // 现有生命周期规则可能因补读等原因列出阻塞项，但不会出现子代理相关的原因。
    let reply = s.response(&actions, id);
    let ours = [
        "subagentInUse",
        "hasSubtree",
        "discoveryIncomplete",
        "treeStopUnconfirmed",
    ];
    assert!(reply.is_none_or(|r| r["error"]["data"]["blockers"]
        .as_array()
        .is_none_or(|list| list
            .iter()
            .all(|b| !ours.contains(&b["reason"].as_str().unwrap_or_default())))));
    assert!(s.m.trees.aux.lifecycle.is_empty(), "不发起发现");
}
