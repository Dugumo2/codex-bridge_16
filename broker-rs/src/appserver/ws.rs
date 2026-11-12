//! 第 1、2 档共用的 WebSocket 适配（详细设计 6.2 节）：在任意字节流上完成客户端握手
//! （`GET /rpc`，S0 探测 p2-afunix/src/main.rs:295 实测可用），把「一帧一条 JSON-RPC 消息」
//! 接到 [`super::rpc::new_client`] 的整条消息收发接口上。
//!
//! 传输约定：
//! - 握手与帧编解码全部交给 tokio-tungstenite（`client_async`），不手写（1.1 节，上游 shared-server.ts 手写，不照搬）；
//! - 入站：Text 帧原样作为一条消息交给 rpc 读端；Binary 帧按 UTF-8 解码后同样处理（上游只认 Text，
//!   见 shared-server.ts:295；Binary 解码失败时按序投递观察缺口）；Ping 由库自动回 Pong；收到 Close 后视为对端结束；
//! - 单条消息上限 10 MiB，与私有档 stdout 的行上限一致（清单 1.2 第 13 条），超限按出错关闭；
//! - 出站：每条消息写成一个 Text 帧，不带换行；写失败返回 Err，由 rpc 层按「出站写入失败」关闭；
//! - 关闭：rpc 正常关闭时先发 Close 帧再结束底层流（上游 shared-server.ts:354–367）；**1 秒期限覆盖发送
//!   Close 与等待对端**，期满主动终止读任务、释放读写两半，然后才执行 `cleanup`（rpc.rs 在丢弃写端之后调用它），
//!   避免「cleanup 等桥接线程、桥接线程等读半释放」的互等（活性预审建议）；
//! - 入站队列与私有档一致（rpc 的入站 mpsc），单条受 [`MAX_MESSAGE_BYTES`] 约束，不另设总量。

use std::future::Future;
use std::time::Duration;

use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{
    tungstenite::{protocol::WebSocketConfig, Message},
    WebSocketStream,
};

use super::rpc::{self, ClientHandle, MessageWriter};
use super::AppEvent;

/// 单条 WebSocket 消息的上限（字节）。
pub const MAX_MESSAGE_BYTES: usize = 10 * 1024 * 1024;

/// 在 `stream` 上完成 WebSocket 客户端握手，返回 rpc 客户端句柄和事件通道。
/// 握手失败（非 101、Accept 不符、流提前结束）返回 Err(原因)，此时 `cleanup` 已执行。
/// 调用方负责整体期限（每档 3 秒，见 [`super::select::TIER_TIMEOUT`]），被取消时也不能泄漏线程或子进程。
pub async fn client_over<S, C, F>(
    stream: S,
    cleanup: C,
    request_timeout_ms: Option<u64>,
) -> Result<(ClientHandle, mpsc::UnboundedReceiver<AppEvent>), String>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: FnOnce() -> F + Send + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    let cleanup = CleanupGuard(Some(cleanup));
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_MESSAGE_BYTES));
    let socket = match tokio_tungstenite::client_async_with_config(
        "ws://localhost/rpc",
        stream,
        Some(config),
    )
    .await
    {
        Ok((socket, _)) => socket,
        Err(error) => {
            cleanup.finish().await;
            return Err(format!("WebSocket 握手失败：{error}"));
        }
    };
    let (sink, mut source) = socket.split();
    let (input, inbound) = mpsc::unbounded_channel();
    let mut reader = tokio::spawn(async move {
        while let Some(message) = source.next().await {
            let incoming = match message {
                Ok(Message::Text(text)) => rpc::Inbound::Text(text.to_string()),
                Ok(Message::Binary(bytes)) => match String::from_utf8(bytes.to_vec()) {
                    Ok(text) => rpc::Inbound::Text(text),
                    Err(error) => {
                        tracing::warn!(%error, "不是 UTF-8 的 WebSocket 二进制消息，报告观察缺口");
                        rpc::Inbound::ObservationGap("WebSocket 二进制消息不是 UTF-8".into())
                    }
                },
                Ok(Message::Close(_)) => break,
                // 持续轮询读半，让库刷新自动排队的 Pong；无需手工编解码控制帧。
                Ok(_) => continue,
                Err(error) => {
                    tracing::warn!(%error, "读取 WebSocket 失败，关闭连接");
                    break;
                }
            };
            // RPC 开始关闭后仍读控制帧，直到 Close 或关闭期限；不再积压业务消息。
            let _ = input.send(incoming);
        }
    });
    let (release, released) = oneshot::channel();
    let writer = WsWriter {
        sink: Some(sink),
        release: Some(release),
    };
    Ok(rpc::new_client_inbound(
        inbound,
        writer,
        move || async move {
            // rpc 先丢弃写入器；这里独占写半，1 秒同时覆盖 Close 发送与对端响应。
            if let Ok(mut sink) = released.await {
                let closed = tokio::time::timeout(Duration::from_secs(1), async {
                    let _ = sink.send(Message::Close(None)).await;
                    let _ = (&mut reader).await;
                })
                .await;
                if closed.is_err() {
                    reader.abort();
                    let _ = reader.await;
                }
                drop(sink);
            } else {
                reader.abort();
                let _ = reader.await;
            }
            cleanup.finish().await;
        },
        request_timeout_ms,
    ))
}

type WsSink<S> = SplitSink<WebSocketStream<S>, Message>;

struct WsWriter<S> {
    sink: Option<WsSink<S>>,
    release: Option<oneshot::Sender<WsSink<S>>>,
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> MessageWriter for WsWriter<S> {
    async fn write(&mut self, text: String) -> Result<(), String> {
        self.sink
            .as_mut()
            .expect("写半尚未释放")
            .send(Message::Text(text.into()))
            .await
            .map_err(|error| format!("写入 WebSocket 失败：{error}"))
    }
}

impl<S> Drop for WsWriter<S> {
    fn drop(&mut self) {
        if let (Some(sink), Some(release)) = (self.sink.take(), self.release.take()) {
            let _ = release.send(sink);
        }
    }
}

/// 握手取消、错误与正常关闭共用一次清理；等待清理的调用方被取消也不撤销清理任务。
struct CleanupGuard<C: FnOnce() -> F + Send + 'static, F: Future<Output = ()> + Send + 'static>(
    Option<C>,
);

impl<C, F> CleanupGuard<C, F>
where
    C: FnOnce() -> F + Send + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    async fn finish(mut self) {
        if let Some(cleanup) = self.0.take() {
            let _ = tokio::spawn(async move { cleanup().await }).await;
        }
    }
}

impl<C, F> Drop for CleanupGuard<C, F>
where
    C: FnOnce() -> F + Send + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    fn drop(&mut self) {
        if let Some(cleanup) = self.0.take() {
            tokio::spawn(async move { cleanup().await });
        }
    }
}

/// initialize 被取消时主动失败 RPC，回收由 RPC 的独立任务完成。
pub(super) struct StartupGuard(pub Option<ClientHandle>);

impl Drop for StartupGuard {
    fn drop(&mut self) {
        if let Some(client) = self.0.take() {
            client.fail("共享 app-server 初始化已取消");
        }
    }
}
