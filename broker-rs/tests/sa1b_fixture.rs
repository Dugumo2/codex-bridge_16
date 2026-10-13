//! 共享协议构造器自身验收；业务组合在 src/cxb/sa1b-blackbox.test.ts 跑真实 broker。
#[path = "../src/test_support/sa1b.rs"]
mod fixture;
use fixture::*;
use serde_json::json;

#[test]
fn client_id_is_not_server_item_id() {
    let item = user_message("服务端条目", "客户端唯一标识", "证据包");
    assert_ne!(item["id"], item["clientId"]);
    assert_eq!(item["type"], "userMessage");
}

#[test]
fn fence_enforces_parent_final_before_child_result_and_releases_once() {
    let mut fence = EventFence::default();
    let child = turn(
        "子",
        "子轮",
        false,
        "completed",
        vec![final_answer("结果", "随机标记")],
    );
    fence.emit(child.clone(), Some("父已完成"));
    fence.emit(turn("父", "父轮", false, "completed", vec![]), None);
    assert_eq!(fence.delivered().len(), 1);
    assert_eq!(fence.release("父已完成"), vec![child]);
    assert!(fence.release("父已完成").is_empty());
    assert_eq!(fence.delivered()[1]["params"]["threadId"], "子");
}

#[test]
fn v1_spawn_initial_has_no_receiver_v2_has_two_identical_success_items() {
    let v1 = spawn("父", "父轮", "子", "派生", false);
    assert_eq!(v1[0]["params"]["item"]["receiverThreadIds"], json!([]));
    assert_eq!(v1[1]["params"]["item"]["receiverThreadIds"], json!(["子"]));
    let v2 = spawn("子", "子轮", "孙", "孙派生", true);
    assert_eq!(v2[0]["params"]["item"], v2[1]["params"]["item"]);
    assert_eq!(v2[0]["params"]["threadId"], "子");
}

#[test]
fn client_message_can_be_located_only_on_later_page() {
    let items = vec![
        final_answer("旧", "旧输出"),
        json!({"id":"推理","type":"reasoning"}),
        user_message("新", "client", "证据"),
    ];
    let first = page(&items, 0, 2);
    assert_eq!(first["nextCursor"], "2");
    assert!(first["data"]
        .as_array()
        .unwrap()
        .iter()
        .all(|x| x["clientId"].is_null()));
    let last = page(&items, 2, 2);
    assert_eq!(last["data"][0]["clientId"], "client");
    assert!(last["nextCursor"].is_null());
}

#[test]
fn goal_and_usage_keep_raw_counts_for_overlap_verification() {
    let u = usage("子", "轮", 100, 40, 10);
    let g = goal("父", "complete", 100, 70);
    assert_eq!(u["params"]["tokenUsage"]["total"]["cachedInputTokens"], 40);
    assert_eq!(g["params"]["goal"]["tokensUsed"], 70);
    assert_eq!(g["params"]["goal"]["tokenBudget"], 100);
}
