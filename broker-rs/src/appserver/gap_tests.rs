//! 走真实 RPC／WebSocket 入站入口，验证损坏记录与后继完成事件的交付顺序。

use super::{rpc, ws, AppEvent};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

fn assert_gap(event: AppEvent) {
    // 旧实现尚无该枚举变体，使用调试名让红测能在原代码编译运行。
    assert!(
        format!("{event:?}").starts_with("ObservationGap"),
        "损坏输入必须先交付观察缺口，实际收到 {event:?}"
    );
}

#[tokio::test]
async fn 方案b_rpc坏json在完成前交付观察缺口() {
    rpc_gap_before_completed("{bad").await;
}

#[tokio::test]
async fn 方案b_rpc坏形状在完成前交付观察缺口() {
    for bad in [
        "[]",
        r#"{"method":4}"#,
        r#"{"id":{},"method":"turn/completed"}"#,
    ] {
        rpc_gap_before_completed(bad).await;
    }
}

async fn rpc_gap_before_completed(bad: &str) {
    let (input, inbound) = mpsc::unbounded_channel();
    let (writer, _output) = mpsc::unbounded_channel::<String>();
    let (client, mut events) = rpc::new_client(inbound, writer, || async {}, None);
    input.send(r#"{"method":"turn/started"}"#.into()).unwrap();
    input.send(bad.into()).unwrap();
    input.send(r#"{"method":"turn/completed"}"#.into()).unwrap();
    assert!(
        matches!(events.recv().await, Some(AppEvent::Notification { method, .. }) if method == "turn/started"),
        "前置：同一读循环已接收开始通知"
    );
    assert_gap(events.recv().await.unwrap());
    assert!(
        matches!(events.recv().await, Some(AppEvent::Notification { method, .. }) if method == "turn/completed")
    );
    client.close().await;
}

#[tokio::test]
async fn 方案b_ws非utf8在完成前交付观察缺口() {
    let (local, remote) = tokio::io::duplex(4096);
    let server = tokio::spawn(async move {
        let mut socket = tokio_tungstenite::accept_async(remote).await.unwrap();
        socket
            .send(Message::Text(r#"{"method":"turn/started"}"#.into()))
            .await
            .unwrap();
        socket
            .send(Message::Binary(vec![0xff, 0xfe].into()))
            .await
            .unwrap();
        socket
            .send(Message::Text(r#"{"method":"turn/completed"}"#.into()))
            .await
            .unwrap();
        while let Some(Ok(message)) = socket.next().await {
            if message.is_close() {
                break;
            }
        }
    });
    let (client, mut events) = ws::client_over(local, || async {}, None).await.unwrap();
    assert!(
        matches!(events.recv().await, Some(AppEvent::Notification { method, .. }) if method == "turn/started"),
        "前置：真实 WebSocket 已握手且接收开始通知"
    );
    assert_gap(events.recv().await.unwrap());
    assert!(
        matches!(events.recv().await, Some(AppEvent::Notification { method, .. }) if method == "turn/completed")
    );
    client.close().await;
    server.await.unwrap();
}

#[tokio::test]
async fn 方案b_rpc合法无关回包不产生观察缺口() {
    let (input, inbound) = mpsc::unbounded_channel();
    let (writer, _output) = mpsc::unbounded_channel::<String>();
    let (client, mut events) = rpc::new_client(inbound, writer, || async {}, None);
    for text in [
        r#"{"id":999,"result":null}"#,
        r#"{"id":999,"error":{"code":-32601,"message":"unknown"}}"#,
        r#"{"id":999,"error":{"message":7}}"#,
        r#"{"id":999}"#,
        r#"{"id":{},"result":null}"#,
        r#"{"method":"turn/completed"}"#,
    ] {
        input.send(text.into()).unwrap();
    }
    assert!(
        matches!(events.recv().await, Some(AppEvent::Notification { method, .. }) if method == "turn/completed")
    );
    client.close().await;
}

#[tokio::test]
async fn 方案b_ws断开与十mib超限均有序关闭() {
    for overflow in [false, true] {
        let (local, remote) = tokio::io::duplex(65536);
        let server = tokio::spawn(async move {
            let mut socket = tokio_tungstenite::accept_async(remote).await.unwrap();
            socket
                .send(Message::Text(r#"{"method":"turn/started"}"#.into()))
                .await
                .unwrap();
            if overflow {
                let _ = socket
                    .send(Message::Text("x".repeat(ws::MAX_MESSAGE_BYTES + 1).into()))
                    .await;
            }
        });
        let (client, mut events) = ws::client_over(local, || async {}, None).await.unwrap();
        assert!(
            matches!(events.recv().await, Some(AppEvent::Notification { method, .. }) if method == "turn/started"),
            "前置：断开或超限前的通知不能丢失"
        );
        assert!(matches!(events.recv().await, Some(AppEvent::Closed { .. })));
        assert!(events.recv().await.is_none(), "Closed 必须终止事件流");
        client.close().await;
        server.await.unwrap();
    }
}
