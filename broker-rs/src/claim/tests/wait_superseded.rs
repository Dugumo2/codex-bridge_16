//! 独占线程等待的登记、通知、预算与断开不变量。
use super::*;
use crate::claim::ext::{MAX_WATCHERS, MAX_WATCHERS_PER_CONN, MAX_WATCHERS_TOTAL};

fn setup() -> Machine {
    let (mut m, _) = running();
    for conn in 1..=4 {
        apply(&mut m, Event::Connected { conn });
        apply(&mut m, Event::Initialized { conn, cxb: true });
    }
    m.record("其他线程").subscribed = true;
    m
}

fn watch(m: &mut Machine, conn: u64, params: Value) -> Vec<Action> {
    let actions = apply(m, client(request(conn, 99, "broker/threads/watch", params)));
    for record in m.records.values() {
        assert!(record
            .exclusive_waiter
            .is_none_or(|id| record.watchers.contains(&id)));
    }
    actions
}

fn exclusive(m: &mut Machine, conn: u64) -> Vec<Action> {
    watch(m, conn, json!({"threadId":"线程","exclusiveWaiter":true}))
}

fn notices(actions: &[Action]) -> Vec<(u64, Value)> {
    sends(actions)
        .into_iter()
        .filter(|(_, v)| v["method"] == "broker/wait/superseded")
        .collect()
}

fn expected(conn: u64) -> Vec<(u64, Value)> {
    vec![(
        conn,
        proto::notification("broker/wait/superseded", Some(json!({"threadId":"线程"}))),
    )]
}

fn response(actions: &[Action]) -> Value {
    sends(actions)
        .into_iter()
        .find(|(_, v)| v["id"] == 99)
        .expect("watch 应回包")
        .1
}

#[test]
fn 接替仅向旧连接通知一次并更新字段() {
    let mut m = setup();
    assert!(notices(&exclusive(&mut m, 2)).is_empty());
    assert_eq!(notices(&exclusive(&mut m, 3)), expected(2));
    assert_eq!(m.records["线程"].exclusive_waiter, Some(3));
}

#[test]
fn broker关闭不发接替通知() {
    let mut m = setup();
    exclusive(&mut m, 2);
    assert!(notices(&apply(&mut m, Event::Shutdown)).is_empty());
}

#[test]
fn 同连接重复及带运行号补读均不接替() {
    let mut m = setup();
    exclusive(&mut m, 2);
    assert!(notices(&exclusive(&mut m, 2)).is_empty());
    assert!(notices(&watch(
        &mut m,
        2,
        json!({"threadId":"线程","runId":"run-测试-000001"})
    ))
    .is_empty());
    assert_eq!(m.records["线程"].exclusive_waiter, Some(2));
    assert_eq!(m.records["线程"].watchers, vec![2]);
}

#[test]
fn 断开立即清空字段后新等待无通知() {
    let mut m = setup();
    exclusive(&mut m, 2);
    assert!(notices(&apply(&mut m, Event::Disconnected { conn: 2 })).is_empty());
    // owner 的活动认领保证该记录未被回收，直接检验清理而非 send_ext 的离线过滤。
    assert!(m.claims.contains_key("线程"));
    assert_eq!(m.records["线程"].exclusive_waiter, None);
    assert!(notices(&exclusive(&mut m, 3)).is_empty());
}

#[test]
fn 旧等待断开不能清掉新等待() {
    let mut m = setup();
    exclusive(&mut m, 2);
    exclusive(&mut m, 3);
    assert!(notices(&apply(&mut m, Event::Disconnected { conn: 2 })).is_empty());
    assert_eq!(m.records["线程"].exclusive_waiter, Some(3));
    assert_eq!(notices(&exclusive(&mut m, 4)), expected(3));
}

#[test]
fn 不同线程与普通观察者互不影响() {
    let mut m = setup();
    assert!(notices(&watch(&mut m, 2, json!({"threadId":"线程"}))).is_empty());
    assert!(notices(&exclusive(&mut m, 3)).is_empty());
    assert!(notices(&watch(
        &mut m,
        2,
        json!({"threadId":"其他线程","exclusiveWaiter":true})
    ))
    .is_empty());
    assert!(notices(&watch(
        &mut m,
        4,
        json!({"threadId":"线程","exclusiveWaiter":false})
    ))
    .is_empty());
    assert_eq!(m.records["线程"].exclusive_waiter, Some(3));
    assert_eq!(m.records["其他线程"].exclusive_waiter, Some(2));
}

#[test]
fn 非布尔参数无副作用() {
    let mut m = setup();
    exclusive(&mut m, 2);
    for value in [json!(null), json!(1), json!("true"), json!([]), json!({})] {
        let out = watch(
            &mut m,
            3,
            json!({"threadId":"线程","exclusiveWaiter":value}),
        );
        assert_eq!(response(&out)["error"]["code"], -32602);
        assert!(notices(&out).is_empty());
        assert_eq!(m.records["线程"].exclusive_waiter, Some(2));
        assert_eq!(m.records["线程"].watchers, vec![2]);
    }
}

#[test]
fn 三层观察者预算失败不改变独占字段() {
    for budget in ["线程", "连接", "总量"] {
        let mut m = setup();
        exclusive(&mut m, 2);
        match budget {
            "线程" => m.record("线程").watchers = (2..MAX_WATCHERS as u64 + 2).collect(),
            "连接" => {
                for i in 0..MAX_WATCHERS_PER_CONN {
                    m.record(&format!("额外{i}")).watchers.push(999);
                }
            }
            _ => {
                for i in 0..MAX_WATCHERS_TOTAL / MAX_WATCHERS {
                    m.record(&format!("额外{i}")).watchers =
                        (10..MAX_WATCHERS as u64 + 10).collect();
                }
            }
        }
        let out = exclusive(&mut m, 999);
        assert_eq!(response(&out)["error"]["data"]["reason"], "tooManyWatchers");
        assert!(notices(&out).is_empty());
        assert_eq!(m.records["线程"].exclusive_waiter, Some(2));
        assert!(!m.records["线程"].watchers.contains(&999));
    }
}

#[test]
fn 普通订阅可原地升级独占() {
    let mut m = setup();
    watch(&mut m, 2, json!({"threadId":"线程"}));
    assert_eq!(m.records["线程"].exclusive_waiter, None);
    assert!(response(&exclusive(&mut m, 2)).get("error").is_none());
    assert_eq!(m.records["线程"].exclusive_waiter, Some(2));
    assert_eq!(m.records["线程"].watchers, vec![2]);
}

#[test]
fn 中间普通观察者不清字段() {
    let mut m = setup();
    exclusive(&mut m, 2);
    assert!(notices(&watch(&mut m, 3, json!({"threadId":"线程"}))).is_empty());
    assert_eq!(notices(&exclusive(&mut m, 4)), expected(2));
}

#[test]
fn 被接替者的迟到补读不能抢回登记() {
    let mut m = setup();
    exclusive(&mut m, 2);
    assert_eq!(notices(&exclusive(&mut m, 3)), expected(2));
    assert!(notices(&watch(
        &mut m,
        2,
        json!({"threadId":"线程","runId":"run-测试-000001"})
    ))
    .is_empty());
    assert_eq!(notices(&exclusive(&mut m, 4)), expected(3));
}

#[test]
fn 活动独占等待受原有回收条件保护且断开后可淘汰() {
    let mut m = setup();
    watch(
        &mut m,
        2,
        json!({"threadId":"其他线程","exclusiveWaiter":true}),
    );
    for i in 0..MAX_HISTORY + 5 {
        m.record(&format!("历史{i}"));
    }
    m.evict_records();
    assert_eq!(m.records["其他线程"].exclusive_waiter, Some(2));
    apply(&mut m, Event::Disconnected { conn: 2 });
    for i in 0..MAX_HISTORY + 5 {
        m.record(&format!("新历史{i}"));
    }
    m.evict_records();
    assert!(!m.records.contains_key("其他线程"));
}
