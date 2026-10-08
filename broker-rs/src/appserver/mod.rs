//! app-server 接入层（详细设计第 6 节）。
//!
//! 三档接入共用同一个 JSON-RPC 客户端（[`rpc`]），区别只在「消息怎么收发」：
//! - 第 3 档（[`private`]，M1）：私有子进程 `cmd.exe /c codex app-server`，stdin/stdout 上按行收发；
//! - 第 1 档（[`shared`]，M7）：直连共享 app-server 的 AF_UNIX 控制 socket，经 [`ws`] 走 WebSocket；
//! - 第 2 档（[`proxy`]，M7）：`codex app-server proxy` 子进程的 stdin/stdout，同样经 [`ws`] 走 WebSocket；
//! - 选档（[`select`]）：auto 依次试 1 → 2 → 3，每档 3 秒；私有档的初始化由 [`init_lock`] 串行（0.1 节）。
//!
//! 与 core 的约定（1.2 节「core 不 await 任何外部 I/O」）：
//! - core 只调用 [`AppHandle`] 上的非阻塞方法，它们只把消息放进唯一的出站队列，立即返回；
//!   出站顺序就是调用顺序（1.2 节「发往 app-server 的请求顺序就是 core 做决定的顺序」）。
//! - core 发的请求用 [`AppHandle::request_ordered`]：回包作为 [`AppEvent::Reply`] 进事件通道，
//!   与通知、服务端请求按读入顺序交付（例如「turn/started 先于失败回包」不会被颠倒）；
//!   超时通过 TimedOut 传递迟到截止时刻，断线失败与入站消息之间按入锁先后排队，每个请求只结算一次。
//!   core 用自己分配的编号 `tag` 认回包，本层不认识 core 的任何类型。
//! - [`AppHandle::request`] 返回 oneshot，只用于不需要与通知排序的场合（握手、测试）。
//! - app-server 主动发来的通知、服务端请求和连接关闭，按到达顺序进 [`AppEvent`] 通道；
//!   连接关闭时，在途请求的失败回包排在 [`AppEvent::Closed`] 之前。

use serde_json::Value;
use thiserror::Error;
use tokio::sync::oneshot;

pub mod init_lock;
pub mod private;
pub mod proxy;
pub mod rpc;
pub mod select;
pub mod shared;
pub mod ws;

/// app-server 主动发给 broker 的消息，按到达顺序交付。
#[derive(Debug, Clone, PartialEq)]
pub enum AppEvent {
    /// 私有 app-server 进程实例已退出；身份用于结算旧实例上的生命周期请求。
    PrivateInstanceExited {
        instance: crate::store::lifecycle::Instance,
    },
    /// 通知，原样透传（6.2 节「协议升级防护」：不按白名单过滤）。
    Notification {
        method: String,
        params: Option<Value>,
    },
    /// 服务端请求。同一个 id（按 `String(id)` 比较，清单第 8 节第 39 条）在 broker 回答之前
    /// 再次到达时，客户端层已经丢弃重复的那条，这里只交付第一次。
    ServerRequest {
        id: Value,
        method: String,
        params: Option<Value>,
    },
    /// [`AppHandle::request_ordered`] 发出的请求有结果了（成功、错误回包、超时或断线）。
    Reply {
        tag: u64,
        result: Result<Value, AppError>,
    },
    /// 有序请求超时后的迟到回包只补充事实，不再次结算原请求。
    LateReply {
        tag: u64,
        result: Result<Value, AppError>,
    },
    /// 超时时确定唯一的迟到截止时刻，三层登记均使用该值。
    TimedOut {
        tag: u64,
        error: AppError,
        deadline: std::time::Instant,
    },
    /// RPC 迟到登记已到截止时刻，通知 core 同步收尾。
    LateDiscarded { tag: u64 },
    /// RPC 迟到登记因 64 条上限被挤出：只表示不再等线上回包，原截止时刻未到，
    /// 不能当作「结果未知已满期限」处理。
    LateEvicted { tag: u64 },
    /// 本连接发过的编号收到真实回包，即使业务上下文已清理也只报告存活。
    Responsive,
    /// [`AppHandle::respond_acked`] 发出的回包已写进连接、写失败或在写出前被撤回（M6 inbox 的提交边界）。
    ReplyWritten {
        op: u64,
        outcome: crate::inbox::ReplyOutcome,
    },
    /// 连接已断开（子进程退出、读到 EOF、输出超限、写入失败）。之后不会再有任何事件；
    /// 所有在途请求都已以 [`AppError::Closed`] 失败。core 收到后按 6.2 节立即关闭 broker。
    Closed { reason: String },
}

/// 一次请求的失败原因。
#[derive(Debug, Clone, PartialEq, Error)]
pub enum AppError {
    /// app-server 回了 JSON-RPC 错误。code、message、data 原样保留，
    /// broker 转发给客户端时照此透传（详细设计 2.1 节、清单第 8 节第 31 条）。
    #[error("JSON-RPC error {code}: {message}")]
    Rpc {
        code: i64,
        message: String,
        data: Option<Value>,
    },
    /// 超过请求超时仍未回包（缺省 30 秒，config.ts:82）。
    #[error("Request {method} timed out after {timeout_ms}ms")]
    Timeout { method: String, timeout_ms: u64 },
    /// 连接已断开，或在请求发出之前就已断开。
    #[error("{0}")]
    Closed(String),
}

/// 当前接入的是哪种 app-server，用于 `initialize` 回包的 `server` 字段和 `broker/health`。
/// 序列化形状与上游一致：`{"kind":"private","pid":123}`、`{"kind":"shared","socketPath":"…"}`；
/// 共享档另带 `via`（`socket` 或 `proxy`），上游没有第 2 档，这是新增字段。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum ServerInfo {
    Private {
        #[serde(skip_serializing_if = "Option::is_none")]
        pid: Option<u32>,
    },
    Shared {
        #[serde(rename = "socketPath")]
        socket_path: String,
        via: SharedVia,
    },
}

impl ServerInfo {
    /// 共享服务器上才有吸收判定、关闭第 2–3 步等共享专用规则（详细设计 3.4 节、第 5 节）。
    pub fn is_shared(&self) -> bool {
        matches!(self, ServerInfo::Shared { .. })
    }
}

/// 共享档经哪条路连上（第 1 档直连 socket，第 2 档经 proxy 子进程）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SharedVia {
    Socket,
    Proxy,
}

/// 三档共用的初始化握手（参数与上游 client.ts、shared-server.ts:377–398 一致）：发 initialize，
/// 成功后发 initialized，返回 userAgent。失败时不关闭连接，由调用方回收自己的资源。
pub(crate) async fn initialize(client: &rpc::ClientHandle) -> Result<Option<String>, AppError> {
    let reply = client
        .request(
            "initialize",
            Some(serde_json::json!({
                "clientInfo": {"name":"codex-bridge", "title":null, "version":env!("CARGO_PKG_VERSION")},
                "capabilities": {"experimentalApi":true, "optOutNotificationMethods":["item/reasoning/textDelta"]}
            })),
            None,
        )
        .await
        .unwrap_or_else(|_| Err(AppError::Closed("initialize 请求结果通道已结束".into())))?;
    client.notify("initialized", None);
    Ok(reply
        .get("userAgent")
        .and_then(|v| v.as_str())
        .map(str::to_owned))
}

/// app-server 连接的句柄。可克隆；所有方法都不阻塞。
#[derive(Debug, Clone)]
pub struct AppHandle {
    pub(crate) inner: rpc::ClientHandle,
}

impl AppHandle {
    /// 发一个请求，结果从返回的 oneshot 取。超时用客户端的缺省值。
    pub fn request(
        &self,
        method: &str,
        params: Option<Value>,
    ) -> oneshot::Receiver<Result<Value, AppError>> {
        self.inner.request(method, params, None)
    }

    /// 发一个请求，回包以 [`AppEvent::Reply`]`{tag}` 按读入顺序进事件通道（超时用缺省值）。
    /// 连接已关闭时立即返回错误，之后不会有对应的事件。
    pub fn request_ordered(
        &self,
        method: &str,
        params: Option<Value>,
        tag: u64,
    ) -> Result<(), AppError> {
        self.inner.request_ordered(method, params, None, tag)
    }

    /// 同 [`Self::request`]，但指定这一次的超时（毫秒）。
    pub fn request_with_timeout(
        &self,
        method: &str,
        params: Option<Value>,
        timeout_ms: u64,
    ) -> oneshot::Receiver<Result<Value, AppError>> {
        self.inner.request(method, params, Some(timeout_ms))
    }

    /// 发一条通知。
    pub fn notify(&self, method: &str, params: Option<Value>) {
        self.inner.notify(method, params)
    }

    /// 回答一个服务端请求（成功）。回答之后该 id 不再视为「处理中」。
    pub fn respond(&self, id: Value, result: Value) {
        self.inner.respond(id, Ok(result))
    }

    /// 回答一个服务端请求（错误）。`data` 为 `None` 时不带 data 字段。
    pub fn respond_error(&self, id: Value, code: i64, message: String, data: Option<Value>) {
        self.inner.respond(id, Err((code, message, data)))
    }

    /// 带写出确认地回答一个服务端请求（inbox 的提交边界，2.3 节）：消息真正写进连接、写失败、
    /// 连接已关闭或在写出前被 [`Self::cancel_reply`] 撤回后，以 [`AppEvent::ReplyWritten`]`{op}` 进事件通道，
    /// 每个 op 恰好一次。`payload` 为 Ok(result) 或 Err(JSON-RPC error 对象)。
    /// 受理失败时返回 Err(原因)，这次回答不会再有 `ReplyWritten`，由调用方结算为 `Failed`。
    pub fn respond_acked(
        &self,
        op: u64,
        id: Value,
        payload: Result<Value, Value>,
    ) -> Result<(), String> {
        self.inner.respond_acked(op, id, payload)
    }

    /// 撤回还在排队的回答；开始写出后只能等待写入结果。
    pub fn cancel_reply(&self, op: u64) {
        self.inner.cancel_reply(op);
    }

    /// 关系墓碑已退役，释放对应的 RPC 去重键，不产生线上消息。
    pub(crate) fn forget_server_request(&self, key: &str) {
        self.inner.forget_server_request(key);
    }

    /// 不回答某个服务端请求，把它移出「处理中」，同一请求再次到达时会重新交付（4.2 节第 1 条的「不答」）。
    pub fn dismiss(&self, id: &Value) {
        self.inner.dismiss(id)
    }

    /// 关闭连接并回收子进程（详细设计第 5 节第 6 步）：先把已排队的消息写完（最多 2 秒），
    /// 再关 stdin，最多等 5 秒；仍未退出就强制结束，最多等 3 秒；进程树最终由 Job 兜底。
    /// 返回时子进程已退出或已放弃等待。
    pub async fn close(&self) {
        self.inner.close().await
    }
}

/// 接入成功后交给 core 的全部东西。
#[derive(Debug)]
pub struct AppConnection {
    pub handle: AppHandle,
    pub events: tokio::sync::mpsc::UnboundedReceiver<AppEvent>,
    pub server: ServerInfo,
    /// app-server 在 initialize 回包里给的 userAgent，仅记日志用。
    pub user_agent: Option<String>,
}
