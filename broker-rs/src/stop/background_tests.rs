//! 后台方法的能力拒绝只接受本次请求对应的确定性信号。
use super::*;

fn pending(method: &str) -> Background {
    let mut background = Background::new(40_000);
    assert_eq!(background.next("线程", 1).unwrap().0, LIST);
    if method == TERMINATE {
        background.reply(
            Ok(json!({"data":[{"processId":"1","itemId":"命令"}]})),
            2,
            |_| true,
        );
        assert_eq!(background.next("线程", 3).unwrap().0, TERMINATE);
    }
    background
}

fn assert_error(method: &str, error: Value, state: &str) {
    let mut background = pending(method);
    background.reply(Err(error.clone()), 4, |_| true);
    assert_eq!(background.report.state, state, "{method}: {error}");
    assert_eq!(background.report.failure_reason, Some(error.to_string()));
    assert!(background.failed);
    assert!(!background.confirmed);
    assert!(!background.verifying);
    if method == TERMINATE {
        assert_eq!(background.report.termination_results["1"], None);
        assert_eq!(background.report.remaining, ["1"]);
    }
}

#[test]
fn 白名单_方法不存在() {
    for method in [LIST, TERMINATE] {
        assert_error(
            method,
            json!({"code":-32601,"message":"Method not found"}),
            "capabilityLimited",
        );
    }
}

#[test]
fn 白名单_顶层未知方法() {
    for method in [LIST, TERMINATE] {
        assert_error(
            method,
            json!({"code":-32600,"message":format!("Invalid request: unknown variant `{method}`, expected one of `initialize`, `thread/read`")}),
            "capabilityLimited",
        );
    }
}

#[test]
fn 白名单_缺少实验能力() {
    for method in [LIST, TERMINATE] {
        assert_error(
            method,
            json!({"code":-32600,"message":format!("{method} requires experimentalApi capability")}),
            "capabilityLimited",
        );
    }
}

#[test]
fn 反例_普通故障及关键词不表示能力受限() {
    for method in [LIST, TERMINATE] {
        for error in [
            json!({"message":"request timeout"}),
            json!({"message":"connection closed"}),
            json!({"code":-32800,"message":"Request cancelled"}),
            json!({"code":-32001,"message":"Server busy"}),
            json!({"code":-32603,"message":"Internal experimental error"}),
            json!({"code":-32600}),
            json!({"code":-32602}),
            json!({"code":-32600,"message":"missing field `threadId`"}),
            json!({"code":-32600,"message":"Not initialized"}),
            json!({"code":-32600,"message":"Server is draining; retry after reconnecting"}),
            json!({"code":-32600,"message":"experimental API disabled"}),
            json!({"code":-32602,"message":"unsupported parameter"}),
            json!({"code":-32600,"message":"value not supported"}),
            json!({"code":-32600,"message":"permission denied"}),
            json!({"code":-32600,"message":"ownership conflict"}),
            json!({"code":-32600,"message":"invalid params","data":{"detail":"unsupported experimental value not supported"}}),
        ] {
            assert_error(method, error, "unknown");
        }
    }
}

#[test]
fn 反例_白名单必须匹配方法错误码及顶层消息() {
    for (method, other) in [(LIST, TERMINATE), (TERMINATE, LIST)] {
        let experimental = format!("{method} requires experimentalApi capability");
        let unknown = format!("Invalid request: unknown variant `{method}`, expected `initialize`");
        for message in [
            format!("{other} requires experimentalApi capability"),
            format!("Invalid request: unknown variant `{other}`, expected `initialize`"),
            "thread/read requires experimentalApi capability".into(),
            "Invalid request: unknown variant `experimental`, expected `valid`".into(),
            format!("Invalid request: params.mode: unknown variant `{method}`, expected `valid`"),
            format!("params: {unknown}"),
            format!(" {unknown}"),
            format!("Invalid request: unknown variant `{method}`, expected "),
            format!("Invalid request: unknown variant `{method}`"),
            format!("{experimental}."),
            format!("{experimental}\n"),
            format!("prefix: {experimental}"),
            experimental.to_ascii_lowercase(),
        ] {
            assert_error(method, json!({"code":-32600,"message":message}), "unknown");
        }
        for message in [experimental, unknown] {
            for code in [json!(-32602), json!(-32603), json!(null), json!("-32600")] {
                assert_error(method, json!({"code":code,"message":message}), "unknown");
            }
            assert_error(
                method,
                json!({"code":-32600,"message":"invalid params","data":{"message":message}}),
                "unknown",
            );
            assert_error(
                method,
                json!({"code":-32600,"message":{"message":message}}),
                "unknown",
            );
        }
    }
}

#[test]
fn 反例_线程未加载保持原路径() {
    for method in [LIST, TERMINATE] {
        assert_error(
            method,
            json!({"code":-32600,"message":"thread not found: experimental-unsupported-not supported"}),
            "notLoaded",
        );
    }
}

#[test]
fn 反例_成功回包缺字段或错误类型() {
    for (method, value) in [
        (LIST, json!({})),
        (LIST, json!({"data":"unsupported"})),
        (LIST, json!({"data":[{"itemId":"experimental"}]})),
        (TERMINATE, json!({})),
        (TERMINATE, json!({"terminated":"not supported"})),
    ] {
        let mut background = pending(method);
        background.reply(Ok(value), 4, |_| true);
        assert_eq!(background.report.state, "unknown");
        assert!(!background.confirmed);
    }
}

#[test]
fn 反例_分页重复() {
    let mut background = pending(LIST);
    let page = json!({"data":[],"nextCursor":"experimental"});
    background.reply(Ok(page.clone()), 2, |_| true);
    background.next("线程", 3).unwrap();
    background.reply(Ok(page), 4, |_| true);
    assert_eq!(background.report.state, "unknown");
    assert!(!background.confirmed);
}

#[test]
fn 反例_终止布尔值及剩余命令不表示能力受限() {
    for terminated in [false, true] {
        let mut background = pending(TERMINATE);
        background.reply(Ok(json!({"terminated":terminated})), 4, |_| true);
        assert!(!background.confirmed);
        assert_ne!(background.report.state, "capabilityLimited");
        assert_eq!(background.report.termination_results["1"], Some(terminated));
        assert_eq!(background.next("线程", 5).unwrap().0, LIST);
        background.reply(
            Ok(json!({"data":[{"processId":"1","itemId":"命令"}]})),
            6,
            |_| true,
        );
        assert_eq!(background.report.state, "unconfirmed");
        assert_eq!(background.report.remaining, ["1"]);
        assert!(!background.confirmed);
    }
}

#[test]
fn 反例_总期限超时() {
    let mut background = Background::new(1);
    assert!(background.next("线程", 1).is_none());
    assert_eq!(background.report.state, "unknown");
    assert!(!background.confirmed);
}
