//! 与传输无关的 JSON-RPC 客户端；传输层负责帧边界，本层只收发完整 JSON 文本。

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::AbortHandle;

use super::{AppError, AppEvent};
use crate::inbox::ReplyOutcome;
use crate::proto::{self, Incoming};

pub const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;

/// 正常关闭时最多等多久把已排队的出站消息写完；超时（例如对端不再读取）就强制中止写任务。
const WRITE_LIMIT: Duration = Duration::from_secs(10);
const MAX_ACKED_REPLIES: usize = crate::inbox::MAX_ITEMS;

enum Outbound {
    Message(String),
    Reply(u64),
}

/// payload 被取走即表示开始写出；排队时才能撤回。
struct AckedReply {
    payload: Option<String>,
}

const CLOSE_DRAIN_LIMIT: Duration = Duration::from_secs(2);

/// 写入一条完整 JSON 文本，不含行分隔符。JSONL 和 WebSocket 各自添加传输帧。
pub trait MessageWriter: Send + 'static {
    fn write(&mut self, text: String) -> impl Future<Output = Result<(), String>> + Send;
}

/// 内存传输也走同一写任务，便于测试顺序和连接失败。
impl MessageWriter for mpsc::UnboundedSender<String> {
    async fn write(&mut self, text: String) -> Result<(), String> {
        self.send(text).map_err(|_| "出站传输已关闭".to_owned())
    }
}

/// 回包交给谁。
enum ReplyTo {
    /// 调用方自己等（握手等不需要与通知排序的场合）。
    Oneshot(oneshot::Sender<Result<Value, AppError>>),
    /// 作为 [`AppEvent::Reply`] 放进事件通道：与通知、服务端请求同一条通道、同一把锁下交付。
    /// 保证的是：读任务读入的消息彼此保持读入顺序到达 core（详细设计 1.2 节「按到达顺序处理」）。
    /// 超时、断线这类本地产生的结果与入站消息之间只按入锁先后排队，不对应任何线上顺序；
    /// 无论谁先，每个请求都只结算一次。
    Event(u64),
}

struct Pending {
    reply: ReplyTo,
    timer: AbortHandle,
}

impl State {
    fn finish_ack(&mut self, op: u64, outcome: ReplyOutcome) {
        if self.acked.remove(&op).is_some() {
            if let Some(events) = &self.events {
                let _ = events.send(AppEvent::ReplyWritten { op, outcome });
            }
        }
        self.emit_closed();
    }
    fn emit_closed(&mut self) {
        if !self.closed_sent && self.acked.is_empty() {
            if let Some(reason) = &self.closed_reason {
                self.closed_sent = true;
                // Closed 是事件通道的最后一条：发出后释放发送端，读端随之结束（M2 起的约定）。
                if let Some(events) = self.events.take() {
                    let _ = events.send(AppEvent::Closed {
                        reason: reason.clone(),
                    });
                }
            }
        }
    }
    fn prune_timeouts(&mut self) {
        let now = tokio::time::Instant::now();
        while self
            .timed_out
            .front()
            .is_some_and(|(_, _, deadline)| now >= *deadline)
        {
            let (_, tag, _) = self.timed_out.pop_front().unwrap();
            self.discard_timeout(tag);
            tracing::warn!("RPC 迟到回包登记已满 10 分钟，丢弃");
        }
    }
    fn discard_timeout(&self, tag: u64) {
        if let Some(events) = &self.events {
            let _ = events.send(AppEvent::LateDiscarded { tag });
        }
    }
    fn evict_timeout(&self, tag: u64) {
        if let Some(events) = &self.events {
            let _ = events.send(AppEvent::LateEvicted { tag });
        }
    }
    /// 交付一个请求的结果。必须在持有状态锁时调用，事件通道的入队顺序才与读入顺序一致。
    fn settle(&self, reply: ReplyTo, result: Result<Value, AppError>) {
        match reply {
            ReplyTo::Oneshot(sender) => {
                let _ = sender.send(result);
            }
            ReplyTo::Event(tag) => {
                if let Some(events) = &self.events {
                    let _ = events.send(AppEvent::Reply { tag, result });
                }
            }
        }
    }
}

struct State {
    next_id: u64,
    acked: HashMap<u64, AckedReply>,
    ack_slots: usize,
    last_ack: Option<u64>,
    closed_sent: bool,
    pending: HashMap<u64, Pending>,
    timed_out: VecDeque<(u64, u64, tokio::time::Instant)>,
    server_requests: HashSet<String>,
    closed_reason: Option<String>,
    // 已受理回答先结算，随后发送 Closed 并释放发送端。
    // 关闭后未受理的回答返回 Err，由 core 补投本地 Failed。
    events: Option<mpsc::UnboundedSender<AppEvent>>,
}

struct Shared {
    state: Mutex<State>,
    /// 立即中止读写任务（出错关闭，或正常关闭写不完时）。
    shutdown: watch::Sender<bool>,
    /// 正常关闭：写任务把关闭前已排队的消息写完再结束（第 5 节：先回完挂起的请求，再关 stdin）。
    drain: watch::Sender<bool>,
    /// 写任务已离开写循环（之后才释放写入器、执行回收）。
    drained: watch::Sender<bool>,
    done: watch::Sender<bool>,
    timeout_ms: u64,
}

impl Shared {
    /// 出错关闭：转入关闭状态并立即中止读写任务。正常关闭之后又出错时同样立即中止。
    fn fail(&self, reason: String) {
        self.mark_closed(reason);
        self.shutdown.send_replace(true);
        let mut state = self.state.lock().unwrap();
        let reason = state
            .closed_reason
            .clone()
            .unwrap_or_else(|| "连接已关闭".into());
        let mut ops: Vec<_> = state.acked.keys().copied().collect();
        ops.sort_unstable();
        for op in ops {
            state.finish_ack(op, ReplyOutcome::Failed(reason.clone()));
        }
        state.emit_closed();
    }

    /// 转入关闭状态：在途请求全部失败、Closed 只发一次、之后的调用立即失败或静默丢弃。
    /// 返回本次是否真正发生了状态转换。
    fn mark_closed(&self, reason: String) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.closed_reason.is_some() {
            return false;
        }
        state.closed_reason = Some(reason.clone());
        // 在途请求按发出顺序失败；事件形式的失败回包排在 Closed 之前进入通道。
        let mut pending: Vec<(u64, Pending)> = state.pending.drain().collect();
        pending.sort_unstable_by_key(|(id, _)| *id);
        for (_, pending) in pending {
            pending.timer.abort();
            state.settle(pending.reply, Err(AppError::Closed(reason.clone())));
        }
        state.server_requests.clear();
        state.timed_out.clear();
        let mut queued: Vec<_> = state
            .acked
            .iter()
            .filter(|(_, reply)| reply.payload.is_some())
            .map(|(op, _)| *op)
            .collect();
        queued.sort_unstable();
        for op in queued {
            state.finish_ack(op, ReplyOutcome::Failed(reason.clone()));
        }
        // 写出中的回答在写任务返回或被强制关闭时结算，所有已登记确认先于 Closed。
        state.emit_closed();
        true
    }

    fn dispatch(&self, text: &str) {
        let value = match serde_json::from_str::<Value>(text) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(%error, "app-server 入站 JSON 解析失败，跳过");
                return;
            }
        };
        // 公共分类器保留宽松的透传语义；客户端额外拒绝把回包误当成请求的形状。
        let malformed = !value.is_object()
            || value.get("id").is_some_and(|id| !valid_id(id))
            || (value.get("method").is_some()
                && (value.get("result").is_some() || value.get("error").is_some()));
        if malformed {
            tracing::warn!("app-server 入站消息形状不正确，跳过");
            return;
        }
        let incoming = Incoming::classify(value);
        let mut state = self.state.lock().unwrap();
        if state.closed_reason.is_some() {
            return;
        }
        match incoming {
            Incoming::Response { id, result, error } => {
                // 同时有 result 和 error 时沿用上游的 result 优先规则；null 也是结果。
                let reply = if let Some(result) = result {
                    Ok(result)
                } else if let Some(error) = error {
                    match (
                        error.get("code").and_then(Value::as_i64),
                        error.get("message").and_then(Value::as_str),
                    ) {
                        (Some(code), Some(message)) => Err(AppError::Rpc {
                            code,
                            message: message.to_owned(),
                            data: error.get("data").cloned(),
                        }),
                        _ => {
                            tracing::warn!("app-server 错误回包形状不正确，跳过");
                            return;
                        }
                    }
                } else {
                    tracing::warn!("app-server 回包缺少 result 和 error，跳过");
                    return;
                };
                // 请求表保持数字 id 的精确类型；仅服务端请求使用 String(id) 去重。
                let numeric_id = id.as_u64().or_else(|| {
                    id.as_f64()
                        .filter(|n| *n >= 0.0 && n.fract() == 0.0 && *n < u64::MAX as f64)
                        .map(|n| n as u64)
                });
                let pending = numeric_id.and_then(|id| state.pending.remove(&id));
                if let Some(pending) = pending {
                    pending.timer.abort();
                    state.settle(pending.reply, reply);
                } else if let Some(index) = numeric_id.and_then(|id| {
                    state.prune_timeouts();
                    state.timed_out.iter().position(|(got, _, _)| *got == id)
                }) {
                    let (_, tag, _) = state.timed_out.remove(index).unwrap();
                    if let Some(events) = &state.events {
                        let _ = events.send(AppEvent::LateReply { tag, result: reply });
                    }
                } else if numeric_id.is_some_and(|id| id > 0 && id < state.next_id) {
                    // 本连接的请求编号从 1 单调分配；清理业务登记不抹去服务器已回应的事实。
                    if let Some(events) = &state.events {
                        let _ = events.send(AppEvent::Responsive);
                    }
                } else {
                    tracing::debug!(%id, "忽略 app-server 未知 id 的回包");
                }
            }
            Incoming::Request { id, method, params } => {
                if state.server_requests.insert(proto::request_id_key(&id)) {
                    if let Some(events) = &state.events {
                        let _ = events.send(AppEvent::ServerRequest { id, method, params });
                    }
                }
            }
            Incoming::Notification { method, params } => {
                if let Some(events) = &state.events {
                    let _ = events.send(AppEvent::Notification { method, params });
                }
            }
            Incoming::Invalid(_) => {
                tracing::warn!("app-server 入站消息形状不正确，跳过");
            }
        }
    }
}

fn valid_id(id: &Value) -> bool {
    matches!(id, Value::Null | Value::String(_) | Value::Number(_))
}

/// 可克隆的非阻塞句柄；所有调用共用一把短锁和一个出站队列。
#[derive(Clone)]
pub struct ClientHandle {
    shared: Arc<Shared>,
    outbound: mpsc::UnboundedSender<Outbound>,
}

/// 监控任务不持有出站发送端，避免最后一个业务句柄释放后仍把连接留住。
#[derive(Clone)]
pub(crate) struct FailureReporter(Weak<Shared>);

impl FailureReporter {
    pub(crate) fn fail(&self, reason: impl Into<String>) {
        if let Some(shared) = self.0.upgrade() {
            shared.fail(reason.into());
        }
    }
}

impl std::fmt::Debug for ClientHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientHandle")
            .field("closed_reason", &self.closed_reason())
            .finish_non_exhaustive()
    }
}

impl ClientHandle {
    pub(crate) fn failure_reporter(&self) -> FailureReporter {
        FailureReporter(Arc::downgrade(&self.shared))
    }

    /// 发一个请求，结果从返回的 oneshot 取。只用于不需要与通知排序的场合（例如握手）。
    pub fn request(
        &self,
        method: &str,
        params: Option<Value>,
        timeout_ms: Option<u64>,
    ) -> oneshot::Receiver<Result<Value, AppError>> {
        let (reply, receiver) = oneshot::channel();
        if let Err((ReplyTo::Oneshot(reply), error)) =
            self.start(method, params, timeout_ms, ReplyTo::Oneshot(reply))
        {
            let _ = reply.send(Err(error));
        }
        receiver
    }

    /// 发一个请求，回包（含超时、断线造成的失败）以 `AppEvent::Reply { tag, .. }` 进事件通道，
    /// 与通知按读入顺序交付。连接已关闭或 id 耗尽时立即返回错误，不会再有对应的事件。
    pub fn request_ordered(
        &self,
        method: &str,
        params: Option<Value>,
        timeout_ms: Option<u64>,
        tag: u64,
    ) -> Result<(), AppError> {
        self.start(method, params, timeout_ms, ReplyTo::Event(tag))
            .map_err(|(_, error)| error)
    }

    /// 登记并发出一个请求。失败时把交付方式原样还给调用方，由调用方决定怎样报告。
    fn start(
        &self,
        method: &str,
        params: Option<Value>,
        timeout_ms: Option<u64>,
        reply: ReplyTo,
    ) -> Result<(), (ReplyTo, AppError)> {
        let mut state = self.shared.state.lock().unwrap();
        if let Some(reason) = &state.closed_reason {
            return Err((reply, AppError::Closed(reason.clone())));
        }
        let id = state.next_id;
        let Some(next_id) = id.checked_add(1) else {
            drop(state);
            self.fail("请求 id 已耗尽");
            return Err((reply, AppError::Closed("请求 id 已耗尽".into())));
        };
        state.next_id = next_id;
        let timeout_ms = timeout_ms.unwrap_or(self.shared.timeout_ms);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
        let shared = Arc::downgrade(&self.shared);
        let method_name = method.to_owned();
        let timer = tokio::spawn(async move {
            tokio::time::sleep_until(deadline).await;
            if let Some(shared) = shared.upgrade() {
                let mut state = shared.state.lock().unwrap();
                if let Some(pending) = state.pending.remove(&id) {
                    let error = AppError::Timeout {
                        method: method_name,
                        timeout_ms,
                    };
                    if let ReplyTo::Event(tag) = &pending.reply {
                        let tag = *tag;
                        let late_deadline = tokio::time::Instant::now()
                            + Duration::from_millis(crate::stop::LATE_MS);
                        state.prune_timeouts();
                        state.timed_out.push_back((id, tag, late_deadline));
                        if state.timed_out.len() > crate::stop::MAX_LATE {
                            let (_, retired, _) = state.timed_out.pop_front().unwrap();
                            state.evict_timeout(retired);
                            tracing::warn!("RPC 迟到回包登记超过 64 条，丢弃最旧登记");
                        }
                        if let Some(events) = &state.events {
                            let _ = events.send(AppEvent::TimedOut {
                                tag,
                                error,
                                deadline: late_deadline.into_std(),
                            });
                        }
                        let shared = Arc::downgrade(&shared);
                        tokio::spawn(async move {
                            tokio::time::sleep_until(late_deadline).await;
                            if let Some(shared) = shared.upgrade() {
                                shared.state.lock().unwrap().prune_timeouts();
                            }
                        });
                    } else {
                        state.settle(pending.reply, Err(error));
                    }
                }
            }
        });
        state.pending.insert(
            id,
            Pending {
                reply,
                timer: timer.abort_handle(),
            },
        );
        let failed = self
            .outbound
            .send(Outbound::Message(
                proto::request(id.into(), method, params).to_string(),
            ))
            .is_err();
        drop(state);
        if failed {
            // fail 会让刚登记的请求以 Closed 失败（oneshot 或事件），这里不再重复交付。
            self.fail("出站写任务已结束");
        }
        Ok(())
    }

    /// core 已结束跟踪时同时撤销 wire id 登记，不产生第二次请求结算。
    pub(crate) fn forget_timeout(&self, tag: u64) {
        self.shared
            .state
            .lock()
            .unwrap()
            .timed_out
            .retain(|(_, got, _)| *got != tag);
    }

    /// 不回答某个服务端请求（交给真正的 owner，详细设计 4.2 节第 1 条）：把它移出「处理中」，
    /// 同一请求再次到达时会重新交付。与上游一致：处理函数一结束（含返回 NO_RESPONSE）就移除，见 rpc.ts 的 finally。
    pub fn dismiss(&self, id: &Value) {
        let mut state = self.shared.state.lock().unwrap();
        if state.closed_reason.is_none() {
            state.server_requests.remove(&proto::request_id_key(id));
        }
    }

    pub fn notify(&self, method: &str, params: Option<Value>) {
        self.enqueue(proto::notification(method, params), None);
    }

    pub fn respond(&self, id: Value, result: Result<Value, (i64, String, Option<Value>)>) {
        let key = proto::request_id_key(&id);
        let message = match result {
            Ok(value) => proto::response_ok(id, value),
            Err((code, message, data)) => {
                proto::response_err(id, proto::error_object(code, message, data))
            }
        };
        self.enqueue(message, Some(key));
    }

    /// op 由状态机单调分配；一个代号只能受理一次，不保存无限增长的终态墓碑。
    /// 受理失败（连接已关闭、待写回答已满、代号重复）时返回 Err(原因)，不进事件通道：
    /// Closed 之后通道已结束，由调用方自己把这次回答结算为 `Failed`。
    pub fn respond_acked(
        &self,
        op: u64,
        id: Value,
        payload: Result<Value, Value>,
    ) -> Result<(), String> {
        let mut state = self.shared.state.lock().unwrap();
        if state.last_ack.is_some_and(|last| op <= last) {
            tracing::warn!(op, "忽略重复或倒序的回答操作代号");
            return Err("回答操作代号重复或倒序".into());
        }
        state.last_ack = Some(op);
        state.server_requests.remove(&proto::request_id_key(&id));
        let failure = state.closed_reason.clone().or_else(|| {
            (state.ack_slots >= MAX_ACKED_REPLIES).then(|| "待写回答数量已达上限".into())
        });
        if let Some(reason) = failure {
            return Err(reason);
        }
        let message = match payload {
            Ok(value) => proto::response_ok(id, value),
            Err(error) => proto::response_err(id, error),
        };
        state.acked.insert(
            op,
            AckedReply {
                payload: Some(message.to_string()),
            },
        );
        state.ack_slots += 1;
        let failed = self.outbound.send(Outbound::Reply(op)).is_err();
        drop(state);
        if failed {
            // 已登记的回答由关闭流程以 Failed 结算，仍是恰好一次。
            self.fail("出站写任务已结束");
        }
        Ok(())
    }

    pub fn cancel_reply(&self, op: u64) {
        let mut state = self.shared.state.lock().unwrap();
        if state
            .acked
            .get(&op)
            .is_some_and(|reply| reply.payload.is_some())
        {
            state.finish_ack(op, ReplyOutcome::Cancelled);
        }
    }

    fn enqueue(&self, message: Value, answered: Option<String>) {
        let mut state = self.shared.state.lock().unwrap();
        if state.closed_reason.is_some() {
            return;
        }
        if let Some(key) = answered {
            state.server_requests.remove(&key);
        }
        let failed = self
            .outbound
            .send(Outbound::Message(message.to_string()))
            .is_err();
        drop(state);
        if failed {
            self.fail("出站写任务已结束");
        }
    }

    /// 传输监控可同步报告断开，立即失败所有请求；回收仍由唯一写任务完成。
    pub fn fail(&self, reason: impl Into<String>) {
        self.shared.fail(reason.into());
    }

    pub fn closed_reason(&self) -> Option<String> {
        self.shared.state.lock().unwrap().closed_reason.clone()
    }

    /// 正常关闭：先把已排队的出站消息写完（最多 [`CLOSE_DRAIN_LIMIT`]），再释放写入器并执行回收。
    /// 并发、重复或取消一次等待都不会取消底层回收任务：写不完时的强制中止由独立任务负责。
    pub async fn close(&self) {
        if self.shared.mark_closed("Client is closed".into()) {
            self.shared.drain.send_replace(true);
            let shared = Arc::downgrade(&self.shared);
            let mut drained = self.shared.drained.subscribe();
            tokio::spawn(async move {
                let finished =
                    tokio::time::timeout(CLOSE_DRAIN_LIMIT, drained.wait_for(|d| *d)).await;
                if finished.is_err() {
                    tracing::warn!("关闭时出站消息 2 秒内未写完，强制中止");
                    if let Some(shared) = shared.upgrade() {
                        shared.fail("关闭时出站写入未完成".into());
                    }
                }
            });
        }
        let mut done = self.shared.done.subscribe();
        let _ = done.wait_for(|done| *done).await;
    }
}

/// 从排队转入写出与撤回共用状态锁；整帧写入超过 10 秒后整条传输作废。
async fn write_outbound<W: MessageWriter>(
    shared: &Shared,
    writer: &mut W,
    outgoing: Outbound,
) -> Result<(), String> {
    let (text, op) = match outgoing {
        Outbound::Message(text) => (text, None),
        Outbound::Reply(op) => {
            let mut state = shared.state.lock().unwrap();
            state.ack_slots = state.ack_slots.saturating_sub(1);
            let Some(reply) = state.acked.get_mut(&op) else {
                return Ok(());
            };
            let Some(text) = reply.payload.take() else {
                return Ok(());
            };
            (text, Some(op))
        }
    };
    let result = tokio::time::timeout(WRITE_LIMIT, writer.write(text))
        .await
        .unwrap_or_else(|_| Err("完整消息写入超过 10 秒，关闭传输".into()));
    if let Some(op) = op {
        let outcome = match &result {
            Ok(()) => ReplyOutcome::Written,
            Err(error) => ReplyOutcome::Failed(error.clone()),
        };
        shared.state.lock().unwrap().finish_ack(op, outcome);
    }
    result
}

/// 创建客户端。EOF、写失败、fail/close 共用一次关闭状态转换。
/// `cleanup` 在写入器被丢弃之后执行，例如关闭 stdin 后等待子进程退出。
pub fn new_client<W, C, F>(
    mut inbound: mpsc::UnboundedReceiver<String>,
    mut writer: W,
    cleanup: C,
    request_timeout_ms: Option<u64>,
) -> (ClientHandle, mpsc::UnboundedReceiver<AppEvent>)
where
    W: MessageWriter,
    C: FnOnce() -> F + Send + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    let (outbound, mut outgoing) = mpsc::unbounded_channel();
    let (events, receiver) = mpsc::unbounded_channel();
    let (shutdown, mut read_shutdown) = watch::channel(false);
    let mut write_shutdown = shutdown.subscribe();
    let (drain, mut read_drain) = watch::channel(false);
    let mut write_drain = drain.subscribe();
    let (drained, _) = watch::channel(false);
    let (done, _) = watch::channel(false);
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            next_id: 1,
            acked: HashMap::new(),
            ack_slots: 0,
            last_ack: None,
            closed_sent: false,
            pending: HashMap::new(),
            timed_out: VecDeque::new(),
            server_requests: HashSet::new(),
            closed_reason: None,
            events: Some(events),
        }),
        shutdown,
        drain,
        drained,
        done,
        timeout_ms: request_timeout_ms.unwrap_or(DEFAULT_REQUEST_TIMEOUT_MS),
    });
    let reader_shared = shared.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = read_shutdown.wait_for(|closed| *closed) => break,
                _ = read_drain.wait_for(|draining| *draining) => break,
                text = inbound.recv() => match text {
                    Some(text) => reader_shared.dispatch(&text),
                    None => {
                        reader_shared.fail("入站传输已结束".into());
                        break;
                    }
                }
            }
        }
    });
    let writer_shared = shared.clone();
    tokio::spawn(async move {
        // 各 wait_for 分支都先丢掉返回的读锁守卫：select! 的输出枚举包含所有分支的类型，
        // 守卫类型不是 Send，而处理分支里还要 await。
        'run: loop {
            let text = tokio::select! {
                biased;
                () = async { let _ = write_shutdown.wait_for(|closed| *closed).await; } => break 'run,
                () = async { let _ = write_drain.wait_for(|draining| *draining).await; } => {
                    // 正常关闭：关闭前已排队的消息照常写完；关闭之后入队的已被 enqueue 丢弃。
                    while let Ok(text) = outgoing.try_recv() {
                        tokio::select! {
                            biased;
                            () = async { let _ = write_shutdown.wait_for(|closed| *closed).await; } => break 'run,
                            result = write_outbound(&writer_shared, &mut writer, text) => if let Err(error) = result {
                                writer_shared.fail(format!("出站写入失败：{error}"));
                                break 'run;
                            }
                        }
                    }
                    break 'run;
                }
                text = outgoing.recv() => match text {
                    Some(text) => text,
                    None => {
                        writer_shared.fail("客户端句柄已全部释放".into());
                        break 'run;
                    }
                }
            };
            // 中止也能中断一个被背压阻塞的写操作。
            tokio::select! {
                biased;
                () = async { let _ = write_shutdown.wait_for(|closed| *closed).await; } => break 'run,
                result = write_outbound(&writer_shared, &mut writer, text) => if let Err(error) = result {
                    writer_shared.fail(format!("出站写入失败：{error}"));
                    break 'run;
                }
            }
        }
        writer_shared.drained.send_replace(true);
        drop(writer);
        drop(outgoing);
        cleanup().await;
        writer_shared.done.send_replace(true);
    });
    (ClientHandle { shared, outbound }, receiver)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn 服务端数字_id_使用_js_字符串格式() {
        for (value, expected) in [
            (json!(1.0), "1"),
            (json!(-0.0), "0"),
            (json!(1e21), "1e+21"),
            (json!(1e20), "100000000000000000000"),
            (json!(1e-6), "0.000001"),
            (json!(1e-7), "1e-7"),
            (json!(-1.25e22), "-1.25e+22"),
            (json!(9007199254740993u64), "9007199254740992"),
            (Value::Null, "null"),
        ] {
            assert_eq!(proto::request_id_key(&value), expected);
            assert_eq!(proto::request_id_key(&json!(expected)), expected);
        }
    }

    struct Memory {
        client: ClientHandle,
        input: mpsc::UnboundedSender<String>,
        output: mpsc::UnboundedReceiver<String>,
        events: mpsc::UnboundedReceiver<AppEvent>,
    }

    impl Memory {
        fn new(timeout_ms: Option<u64>) -> Self {
            let (input, inbound) = mpsc::unbounded_channel();
            let (outbound, output) = mpsc::unbounded_channel();
            let (client, events) = new_client(inbound, outbound, || async {}, timeout_ms);
            Self {
                client,
                input,
                output,
                events,
            }
        }

        fn feed(&self, value: Value) {
            self.input.send(value.to_string()).unwrap();
        }

        async fn sent(&mut self) -> Value {
            serde_json::from_str(&self.output.recv().await.unwrap()).unwrap()
        }

        async fn barrier(&mut self) {
            self.feed(json!({"method":"barrier"}));
            loop {
                match self.events.recv().await.unwrap() {
                    AppEvent::Responsive => {}
                    AppEvent::Notification { method, params } if method == "barrier" => {
                        assert!(params.is_none());
                        break;
                    }
                    other => panic!("接收屏障前出现意外业务事件：{other:?}"),
                }
            }
        }
    }

    #[tokio::test]
    async fn 有序请求的回包与通知按读入顺序进事件通道() {
        let mut m = Memory::new(None);
        m.client
            .request_ordered("turn/start", None, None, 7)
            .unwrap();
        let sent = m.sent().await;
        // 同一批读入：先通知、后回包、再通知。
        m.feed(json!({"method":"turn/started","params":{"turn":{"id":"t1"}}}));
        m.feed(json!({"id": sent["id"], "error":{"code":-32000,"message":"失败"}}));
        m.feed(json!({"method":"after"}));
        assert!(matches!(
            m.events.recv().await,
            Some(AppEvent::Notification { method, .. }) if method == "turn/started"
        ));
        assert_eq!(
            m.events.recv().await,
            Some(AppEvent::Reply {
                tag: 7,
                result: Err(AppError::Rpc {
                    code: -32000,
                    message: "失败".into(),
                    data: None,
                }),
            })
        );
        assert!(matches!(
            m.events.recv().await,
            Some(AppEvent::Notification { method, .. }) if method == "after"
        ));
        m.client.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn 有序请求超时也以事件交付() {
        let mut m = Memory::new(Some(100));
        m.client.request_ordered("slow", None, None, 3).unwrap();
        m.sent().await;
        tokio::time::advance(Duration::from_millis(150)).await;
        assert!(matches!(
            m.events.recv().await,
            Some(AppEvent::TimedOut {
                tag: 3,
                error: AppError::Timeout {
                    timeout_ms: 100,
                    ..
                },
                ..
            })
        ));
        m.client.close().await;
    }

    #[tokio::test]
    async fn 断线时有序请求按发出顺序失败且排在关闭事件之前() {
        let mut m = Memory::new(None);
        m.client.request_ordered("a", None, None, 1).unwrap();
        m.client.request_ordered("b", None, None, 2).unwrap();
        m.client.fail("断线");
        for tag in [1, 2] {
            assert_eq!(
                m.events.recv().await,
                Some(AppEvent::Reply {
                    tag,
                    result: Err(AppError::Closed("断线".into())),
                })
            );
        }
        assert_eq!(
            m.events.recv().await,
            Some(AppEvent::Closed {
                reason: "断线".into()
            })
        );
        assert_eq!(
            m.client.request_ordered("c", None, None, 3),
            Err(AppError::Closed("断线".into()))
        );
        assert!(m.events.try_recv().is_err());
    }

    /// 收集事件直到出现名为 barrier 的通知，返回其间收到的、属于 tag 的回包。
    async fn replies_until_barrier(m: &mut Memory, tag: u64) -> Vec<Result<Value, AppError>> {
        m.feed(json!({"method":"barrier"}));
        let mut replies = Vec::new();
        loop {
            match m.events.recv().await.unwrap() {
                AppEvent::Reply { tag: got, result } if got == tag => replies.push(result),
                AppEvent::TimedOut {
                    tag: got, error, ..
                } if got == tag => replies.push(Err(error)),
                AppEvent::Notification { method, .. } if method == "barrier" => return replies,
                _ => {}
            }
        }
    }

    /// 暂停时钟下 advance 只让到期计时器就绪、不等它执行（tokio time/clock.rs 203–215 行）；
    /// 再让出几次，保证超时任务真的跑过。
    async fn pass_deadline(ms: u64) {
        tokio::time::advance(Duration::from_millis(ms)).await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    /// 断开后排空事件通道，返回 (属于 tag 的回包个数, Closed 个数)。
    async fn drain_after_close(m: &mut Memory, tag: u64) -> (usize, usize) {
        let (mut replies, mut closed) = (0, 0);
        while let Some(event) = m.events.recv().await {
            match event {
                AppEvent::Reply { tag: got, .. } if got == tag => replies += 1,
                AppEvent::Closed { .. } => {
                    closed += 1;
                    break;
                }
                _ => {}
            }
        }
        (replies, closed)
    }

    fn timeout_of(method: &str) -> AppError {
        AppError::Timeout {
            method: method.into(),
            timeout_ms: 100,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn 回包先到之后越过截止时间不再以超时结算() {
        let mut m = Memory::new(Some(100));
        m.client.request_ordered("race", None, None, 5).unwrap();
        let sent = m.sent().await;
        m.feed(json!({"id": sent["id"], "result": "ok"}));
        assert_eq!(
            replies_until_barrier(&mut m, 5).await,
            vec![Ok(json!("ok"))]
        );
        pass_deadline(150).await;
        assert!(replies_until_barrier(&mut m, 5).await.is_empty());
        m.client.fail("结束");
        assert_eq!(drain_after_close(&mut m, 5).await, (0, 1));
    }

    #[tokio::test(start_paused = true)]
    async fn 有序请求超时只结算一次迟到回包按读入顺序送达() {
        let mut m = Memory::new(Some(100));
        m.client.request_ordered("race", None, None, 6).unwrap();
        let sent = m.sent().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(matches!(
            m.events.recv().await,
            Some(AppEvent::TimedOut {
                tag: 6,
                error: AppError::Timeout { .. },
                ..
            })
        ));
        m.feed(json!({"method":"之前"}));
        m.feed(json!({"id":sent["id"],"result":"迟到"}));
        m.feed(json!({"method":"之后"}));
        assert!(
            matches!(m.events.recv().await,Some(AppEvent::Notification{method,..}) if method=="之前")
        );
        assert_eq!(
            m.events.recv().await,
            Some(AppEvent::LateReply {
                tag: 6,
                result: Ok(json!("迟到"))
            })
        );
        assert!(
            matches!(m.events.recv().await,Some(AppEvent::Notification{method,..}) if method=="之后")
        );
        m.feed(json!({"id":sent["id"],"result":"重复"}));
        m.barrier().await;
        assert!(m.events.try_recv().is_err());
        m.client.fail("结束");
        assert_eq!(drain_after_close(&mut m, 6).await, (0, 1));
    }

    #[tokio::test(start_paused = true)]
    async fn 非有序请求超时后迟到回包仍丢弃() {
        let mut m = Memory::new(Some(100));
        let answer = m.client.request("握手", None, None);
        let sent = m.sent().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(matches!(
            answer.await.unwrap(),
            Err(AppError::Timeout { .. })
        ));
        m.feed(json!({"id":sent["id"],"result":"迟到"}));
        m.barrier().await;
        assert!(m.events.try_recv().is_err());
        assert!(m.client.shared.state.lock().unwrap().timed_out.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn 有序超时登记六十四条上限十分钟过期与关闭清除() {
        let mut m = Memory::new(Some(100));
        let mut ids = Vec::new();
        for tag in 1..=65 {
            m.client.request_ordered("请求", None, None, tag).unwrap();
            ids.push(m.sent().await["id"].clone());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut evicted = false;
        for _ in 0..65 {
            let event = m.events.recv().await.unwrap();
            let event = if matches!(event, AppEvent::LateEvicted { tag: 1 }) {
                evicted = true;
                m.events.recv().await.unwrap()
            } else {
                event
            };
            assert!(matches!(
                Some(event),
                Some(AppEvent::TimedOut {
                    error: AppError::Timeout { .. },
                    ..
                })
            ));
        }
        assert!(evicted);
        assert_eq!(m.client.shared.state.lock().unwrap().timed_out.len(), 64);
        m.feed(json!({"id":ids[0],"result":"已淘汰"}));
        m.barrier().await;
        assert!(m.events.try_recv().is_err());
        m.feed(json!({"id":ids[1],"result":"仍登记"}));
        assert!(matches!(
            m.events.recv().await,
            Some(AppEvent::LateReply { tag: 2, .. })
        ));
        tokio::time::sleep(Duration::from_millis(crate::stop::LATE_MS)).await;
        while matches!(m.events.try_recv(), Ok(AppEvent::LateDiscarded { .. })) {}
        m.feed(json!({"id":ids[2],"result":"已过期"}));
        m.feed(json!({"method":"barrier"}));
        loop {
            match m.events.recv().await.unwrap() {
                AppEvent::LateDiscarded { .. } | AppEvent::Responsive => {}
                AppEvent::Notification { method, .. } if method == "barrier" => break,
                other => panic!("过期后收到意外事件：{other:?}"),
            }
        }
        assert!(m.events.try_recv().is_err());
        assert!(m.client.shared.state.lock().unwrap().timed_out.is_empty());
        m.client.request_ordered("请求", None, None, 66).unwrap();
        m.sent().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        m.events.recv().await.unwrap();
        m.client.fail("结束");
        assert!(m.client.shared.state.lock().unwrap().timed_out.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn 超时事件传递原截止且无入站流量也通知过期() {
        let mut m = Memory::new(Some(100));
        m.client.request_ordered("请求", None, None, 72).unwrap();
        m.sent().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let deadline = match m.events.recv().await.unwrap() {
            AppEvent::TimedOut {
                tag: 72, deadline, ..
            } => deadline,
            other => panic!("预期超时事件：{other:?}"),
        };
        assert_eq!(
            m.client
                .shared
                .state
                .lock()
                .unwrap()
                .timed_out
                .front()
                .unwrap()
                .2
                .into_std(),
            deadline
        );
        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
        assert_eq!(
            m.events.recv().await,
            Some(AppEvent::LateDiscarded { tag: 72 })
        );
        assert!(m.client.shared.state.lock().unwrap().timed_out.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn 核心结束跟踪撤销线编号后不再交付迟到回包() {
        let mut m = Memory::new(Some(100));
        m.client.request_ordered("请求", None, None, 73).unwrap();
        let sent = m.sent().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        m.events.recv().await.unwrap();
        m.client.forget_timeout(73);
        m.feed(json!({"id":sent["id"],"result":"迟到"}));
        m.barrier().await;
        assert!(m.events.try_recv().is_err());
        assert!(m.client.shared.state.lock().unwrap().timed_out.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn 已清理请求的成功错误回包只报告存活不恢复业务() {
        for mode in ["忘记", "过期", "淘汰"] {
            for failed in [false, true] {
                let mut m = Memory::new(Some(100));
                m.client.request_ordered("原请求", None, None, 77).unwrap();
                let sent = m.sent().await;
                tokio::time::sleep(Duration::from_millis(100)).await;
                assert!(matches!(
                    m.events.recv().await,
                    Some(AppEvent::TimedOut { tag: 77, .. })
                ));
                match mode {
                    "忘记" => m.client.forget_timeout(77),
                    "过期" => {
                        tokio::time::sleep(Duration::from_millis(crate::stop::LATE_MS)).await;
                        assert_eq!(
                            m.events.recv().await,
                            Some(AppEvent::LateDiscarded { tag: 77 })
                        );
                    }
                    _ => {
                        for tag in 100..164 {
                            m.client
                                .request_ordered("后继请求", None, None, tag)
                                .unwrap();
                            m.sent().await;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        let mut discarded = false;
                        let mut timeouts = 0;
                        while timeouts < 64 || !discarded {
                            match m.events.recv().await.unwrap() {
                                AppEvent::TimedOut { .. } => timeouts += 1,
                                // 容量淘汰与到期分开报告（淘汰不代表原截止已到）。
                                AppEvent::LateEvicted { tag: 77 } => discarded = true,
                                other => panic!("清理阶段意外事件：{other:?}"),
                            }
                        }
                    }
                }
                let message = if failed {
                    proto::response_err(
                        sent["id"].clone(),
                        proto::error_object(-32000, "原请求失败", None),
                    )
                } else {
                    proto::response_ok(sent["id"].clone(), json!({}))
                };
                m.feed(message);
                m.feed(json!({"method":"回包后"}));
                assert_eq!(m.events.recv().await, Some(AppEvent::Responsive));
                assert!(
                    matches!(m.events.recv().await,Some(AppEvent::Notification {method,..}) if method=="回包后")
                );
                m.client.fail("结束");
                assert_eq!(drain_after_close(&mut m, 77).await, (0, 1));
            }
        }
    }

    #[tokio::test]
    async fn 从未分配的编号和本地关闭不产生存活信号() {
        let mut m = Memory::new(None);
        m.feed(json!({"id":1,"result":{}}));
        m.barrier().await;
        assert!(m.events.try_recv().is_err());
        m.client.request_ordered("请求", None, None, 78).unwrap();
        m.sent().await;
        m.feed(json!({"id":0,"result":{}}));
        m.feed(json!({"id":999,"error":{"code":-32000,"message":"未知"}}));
        m.barrier().await;
        assert!(m.events.try_recv().is_err());
        m.client.fail("关闭");
        assert!(matches!(
            m.events.recv().await,
            Some(AppEvent::Reply {
                result: Err(AppError::Closed(_)),
                ..
            })
        ));
        assert!(matches!(
            m.events.recv().await,
            Some(AppEvent::Closed { .. })
        ));
        assert!(m.events.try_recv().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn 超时先于断线时断线不再结算该请求() {
        let mut m = Memory::new(Some(100));
        m.client.request_ordered("race", None, None, 7).unwrap();
        m.sent().await;
        pass_deadline(150).await;
        assert_eq!(
            replies_until_barrier(&mut m, 7).await,
            vec![Err(timeout_of("race"))]
        );
        m.client.fail("断线");
        assert_eq!(drain_after_close(&mut m, 7).await, (0, 1));
    }

    #[tokio::test(start_paused = true)]
    async fn 断线先于超时时失败回包排在关闭之前且截止后不再结算() {
        let mut m = Memory::new(Some(100));
        m.client.request_ordered("race", None, None, 8).unwrap();
        m.sent().await;
        m.client.fail("断线");
        assert_eq!(
            m.events.recv().await,
            Some(AppEvent::Reply {
                tag: 8,
                result: Err(AppError::Closed("断线".into())),
            })
        );
        assert_eq!(
            m.events.recv().await,
            Some(AppEvent::Closed {
                reason: "断线".into()
            })
        );
        pass_deadline(150).await;
        assert!(m.events.try_recv().is_err());
    }

    #[tokio::test]
    async fn 请求递增并匹配乱序回包() {
        let mut m = Memory::new(None);
        let first = m.client.request("one", Some(json!({"a":1})), None);
        let second = m.client.clone().request("two", None, None);
        assert_eq!(
            m.sent().await,
            json!({"id":1,"method":"one","params":{"a":1}})
        );
        assert_eq!(m.sent().await, json!({"id":2,"method":"two"}));
        m.feed(json!({"id":2,"result":"two"}));
        m.feed(json!({"id":1.0,"result":null,"error":{"code":9,"message":"ignored"}}));
        assert_eq!(second.await.unwrap(), Ok(json!("two")));
        assert_eq!(first.await.unwrap(), Ok(Value::Null));
        m.client.close().await;
    }

    #[tokio::test]
    async fn 错误回包原样保留_data() {
        let mut m = Memory::new(None);
        let request = m.client.request("bad", None, None);
        m.sent().await;
        m.feed(json!({"id":1,"error":{"code":-32001,"message":"busy","data":{"detail":[1,null]}}}));
        assert_eq!(
            request.await.unwrap(),
            Err(AppError::Rpc {
                code: -32001,
                message: "busy".into(),
                data: Some(json!({"detail":[1,null]})),
            })
        );
        m.client.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn 默认超时和单次覆盖移除请求且忽略迟到回包() {
        let mut m = Memory::new(None);
        let default = m.client.request("default", None, None);
        let custom = m.client.request("custom", None, Some(7));
        m.sent().await;
        m.sent().await;
        tokio::time::advance(Duration::from_millis(7)).await;
        assert_eq!(
            custom.await.unwrap(),
            Err(AppError::Timeout {
                method: "custom".into(),
                timeout_ms: 7
            })
        );
        m.feed(json!({"id":2,"result":"late"}));
        m.barrier().await;
        assert_eq!(m.client.shared.state.lock().unwrap().pending.len(), 1);
        tokio::time::advance(Duration::from_millis(29_993)).await;
        assert_eq!(
            default.await.unwrap(),
            Err(AppError::Timeout {
                method: "default".into(),
                timeout_ms: 30_000
            })
        );
        assert!(m.client.shared.state.lock().unwrap().pending.is_empty());
        m.client.close().await;
    }

    #[tokio::test]
    async fn 未知_id_及坏消息不影响后续回包和事件() {
        let mut m = Memory::new(None);
        let request = m.client.request("good", None, None);
        m.sent().await;
        m.input.send("{bad".into()).unwrap();
        for value in [
            json!([]),
            json!({"id":1}),
            json!({"id":1,"error":{"code":"bad"}}),
            json!({"id":1,"method":"bad","result":1}),
            json!({"method":4}),
            json!({"id":{},"method":"bad"}),
            json!({"id":99,"result":1}),
            json!({"id":"1","result":"wrong type"}),
        ] {
            m.feed(value);
        }
        m.feed(json!({"id":1,"result":42}));
        assert_eq!(request.await.unwrap(), Ok(json!(42)));
        m.barrier().await;
        m.client.close().await;
    }

    #[tokio::test]
    async fn 服务端请求按字符串去重且回答后释放() {
        let mut m = Memory::new(None);
        m.feed(json!({"id":1,"method":"approve","params":{}}));
        assert!(
            matches!(m.events.recv().await, Some(AppEvent::ServerRequest { id, .. }) if id == json!(1))
        );
        m.feed(json!({"id":1,"method":"approve"}));
        m.feed(json!({"id":"1","method":"approve"}));
        m.feed(json!({"id":1.0,"method":"approve"}));
        m.barrier().await;
        m.client.respond(json!("1"), Ok(json!(true)));
        assert_eq!(m.sent().await, json!({"id":"1","result":true}));
        m.feed(json!({"id":1,"method":"approve"}));
        assert!(matches!(
            m.events.recv().await,
            Some(AppEvent::ServerRequest { .. })
        ));
        m.client.respond(
            json!(1),
            Err((-42, "no".into(), Some(json!({"why":"test"})))),
        );
        assert_eq!(
            m.sent().await,
            json!({"id":1,"error":{"code":-42,"message":"no","data":{"why":"test"}}})
        );
        m.feed(json!({"id":"1","method":"approve"}));
        assert!(matches!(
            m.events.recv().await,
            Some(AppEvent::ServerRequest { .. })
        ));
        m.client.close().await;
    }

    #[tokio::test]
    async fn 通知按到达顺序透传未知方法() {
        let mut m = Memory::new(None);
        for i in 0..10 {
            m.feed(json!({"method":"future/notification","params":i}));
        }
        for i in 0..10 {
            assert_eq!(
                m.events.recv().await.unwrap(),
                AppEvent::Notification {
                    method: "future/notification".into(),
                    params: Some(json!(i)),
                }
            );
        }
        m.client.close().await;
    }

    #[tokio::test]
    async fn 关闭失败所有在途请求且只发一次_closed() {
        let mut m = Memory::new(None);
        let a = m.client.request("a", None, None);
        let b = m.client.request("b", None, None);
        m.client.fail("test close");
        m.client.fail("second close");
        m.feed(json!({"method":"ignored"}));
        let ((), ()) = tokio::join!(m.client.close(), m.client.close());
        for request in [a, b] {
            assert_eq!(
                request.await.unwrap(),
                Err(AppError::Closed("test close".into()))
            );
        }
        assert_eq!(
            m.events.recv().await,
            Some(AppEvent::Closed {
                reason: "test close".into()
            })
        );
        assert!(m.events.try_recv().is_err());
        let mut after = m.client.request("after", None, None);
        assert_eq!(
            after.try_recv().unwrap(),
            Err(AppError::Closed("test close".into()))
        );
        m.client.notify("ignored", None);
        m.client.respond(json!(1), Ok(Value::Null));
        assert_eq!(m.output.recv().await, None);
    }

    #[tokio::test]
    async fn 入站_eof_和写失败均关闭客户端() {
        let mut m = Memory::new(None);
        let request = m.client.request("pending", None, None);
        drop(m.input);
        assert!(matches!(
            m.events.recv().await,
            Some(AppEvent::Closed { .. })
        ));
        assert!(matches!(request.await.unwrap(), Err(AppError::Closed(_))));
        m.client.close().await;
        let mut m = Memory::new(None);
        drop(m.output);
        let request = m.client.request("pending", None, None);
        assert!(
            matches!(request.await.unwrap(), Err(AppError::Closed(reason)) if reason.contains("出站写入失败"))
        );
        assert!(matches!(
            m.events.recv().await,
            Some(AppEvent::Closed { .. })
        ));
        m.client.close().await;
    }

    #[tokio::test]
    async fn 出站顺序等于跨克隆句柄的调用顺序() {
        let mut m = Memory::new(None);
        let clone = m.client.clone();
        let a = m.client.request("a", None, None);
        clone.notify("b", None);
        m.client.respond(json!("server"), Ok(json!(3)));
        let d = clone.request("d", None, None);
        assert_eq!(m.sent().await, json!({"id":1,"method":"a"}));
        assert_eq!(m.sent().await, json!({"method":"b"}));
        assert_eq!(m.sent().await, json!({"id":"server","result":3}));
        assert_eq!(m.sent().await, json!({"id":2,"method":"d"}));
        m.client.close().await;
        assert!(matches!(a.await.unwrap(), Err(AppError::Closed(_))));
        assert!(matches!(d.await.unwrap(), Err(AppError::Closed(_))));
    }

    fn parse(text: Option<String>) -> Option<Value> {
        text.map(|t| serde_json::from_str(&t).unwrap())
    }

    #[tokio::test]
    async fn 正常关闭先写完已排队的消息() {
        let mut m = Memory::new(None);
        m.feed(json!({"id":"srv-1","method":"approve"}));
        assert!(matches!(
            m.events.recv().await,
            Some(AppEvent::ServerRequest { .. })
        ));
        m.client.respond(
            json!("srv-1"),
            Err((-32603, "Broker shutting down".into(), None)),
        );
        m.client.notify("last", None);
        m.client.close().await;
        assert_eq!(
            parse(m.output.recv().await),
            Some(json!({"id":"srv-1","error":{"code":-32603,"message":"Broker shutting down"}}))
        );
        assert_eq!(parse(m.output.recv().await), Some(json!({"method":"last"})));
        assert_eq!(m.output.recv().await, None, "关闭之后不应再有出站消息");
    }

    #[tokio::test]
    async fn dismiss_后同一服务端请求可再次交付() {
        let mut m = Memory::new(None);
        let request =
            json!({"id":5,"method":"item/tool/requestUserInput","params":{"threadId":"t"}});
        m.feed(request.clone());
        assert!(matches!(
            m.events.recv().await,
            Some(AppEvent::ServerRequest { .. })
        ));
        m.feed(request.clone());
        m.barrier().await;
        m.client.dismiss(&json!("5"));
        m.feed(request);
        assert!(matches!(
            m.events.recv().await,
            Some(AppEvent::ServerRequest { .. })
        ));
        m.client.close().await;
    }

    struct BlockedWriter(Option<oneshot::Sender<()>>);

    impl MessageWriter for BlockedWriter {
        async fn write(&mut self, _: String) -> Result<(), String> {
            self.0.take().unwrap().send(()).unwrap();
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn 阻塞写入可中断且取消_close_等待不影响回收() {
        let (_input, inbound) = mpsc::unbounded_channel();
        let (writing, written) = oneshot::channel();
        let (cleaning, cleanup_started) = oneshot::channel();
        let (finish, finished) = oneshot::channel();
        let (client, mut events) = new_client(
            inbound,
            BlockedWriter(Some(writing)),
            move || async move {
                cleaning.send(()).unwrap();
                finished.await.unwrap();
            },
            None,
        );
        let first = client.request("blocked", None, None);
        written.await.unwrap();
        client.notify("queued", None);
        let second = client.request("queued", None, None);
        let closing = client.clone();
        let wait = tokio::spawn(async move { closing.close().await });
        cleanup_started.await.unwrap();
        wait.abort();
        finish.send(()).unwrap();
        client.close().await;
        for pending in [first, second] {
            assert!(matches!(pending.await.unwrap(), Err(AppError::Closed(_))));
        }
        assert!(matches!(events.recv().await, Some(AppEvent::Closed { .. })));
        assert!(events.try_recv().is_err());
    }
    struct ControlledWriter {
        entered: mpsc::UnboundedSender<String>,
        results: mpsc::UnboundedReceiver<Result<(), String>>,
    }
    impl MessageWriter for ControlledWriter {
        async fn write(&mut self, text: String) -> Result<(), String> {
            self.entered.send(text).unwrap();
            self.results
                .recv()
                .await
                .unwrap_or_else(|| Err("测试写入器已关闭".into()))
        }
    }
    struct Controlled {
        client: ClientHandle,
        _input: mpsc::UnboundedSender<String>,
        entered: mpsc::UnboundedReceiver<String>,
        results: mpsc::UnboundedSender<Result<(), String>>,
        events: mpsc::UnboundedReceiver<AppEvent>,
    }
    impl Controlled {
        fn new() -> Self {
            let (input, inbound) = mpsc::unbounded_channel();
            let (entered, writing) = mpsc::unbounded_channel();
            let (results, outcomes) = mpsc::unbounded_channel();
            let (client, events) = new_client(
                inbound,
                ControlledWriter {
                    entered,
                    results: outcomes,
                },
                || async {},
                None,
            );
            Self {
                client,
                _input: input,
                entered: writing,
                results,
                events,
            }
        }
        async fn outcome(&mut self, expected: u64) -> ReplyOutcome {
            match self.events.recv().await.unwrap() {
                AppEvent::ReplyWritten { op, outcome } => {
                    assert_eq!(op, expected);
                    outcome
                }
                other => panic!("应先收到写出确认：{other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn 带确认回答成功撤回与写出中的撤回无效() {
        let mut m = Controlled::new();
        m.client
            .shared
            .state
            .lock()
            .unwrap()
            .server_requests
            .insert("7".into());
        m.client
            .respond_acked(1, json!(7.0), Ok(json!({"answer":1})))
            .unwrap();
        assert!(!m
            .client
            .shared
            .state
            .lock()
            .unwrap()
            .server_requests
            .contains("7"));
        let text = m.entered.recv().await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&text).unwrap()["result"]["answer"],
            1
        );
        m.client.cancel_reply(1);
        assert!(m.events.try_recv().is_err());
        m.client.respond_acked(2, json!(8), Ok(json!(2))).unwrap();
        m.client.cancel_reply(2);
        m.client.cancel_reply(2);
        assert_eq!(m.outcome(2).await, ReplyOutcome::Cancelled);
        m.results.send(Ok(())).unwrap();
        assert_eq!(m.outcome(1).await, ReplyOutcome::Written);
        // 同一 op 重复受理也不产生第二次确认；后继消息作为写队列屏障。
        assert!(m.client.respond_acked(1, json!(7), Ok(json!(9))).is_err());
        m.client.respond_acked(3, json!(9), Ok(json!(3))).unwrap();
        let next = serde_json::from_str::<Value>(&m.entered.recv().await.unwrap()).unwrap();
        assert_eq!(next["id"], 9);
        m.results.send(Ok(())).unwrap();
        assert_eq!(m.outcome(3).await, ReplyOutcome::Written);
        assert!(m.events.try_recv().is_err());
        m.client.close().await;
    }

    #[tokio::test]
    async fn 带确认回答写失败时先结算所有_op_再关闭() {
        let mut m = Controlled::new();
        m.client
            .respond_acked(1, json!(1), Ok(json!(true)))
            .unwrap();
        m.entered.recv().await.unwrap();
        m.client
            .respond_acked(2, json!(2), Ok(json!(true)))
            .unwrap();
        m.results.send(Err("测试写失败".into())).unwrap();
        for op in [1, 2] {
            assert!(matches!(m.outcome(op).await, ReplyOutcome::Failed(_)));
        }
        assert!(matches!(
            m.events.recv().await,
            Some(AppEvent::Closed { .. })
        ));
        m.client.fail("重复关闭");
        m.client.cancel_reply(1);
        m.client.cancel_reply(2);
        assert!(m.events.try_recv().is_err());
        // 关闭之后的新回答不被受理，由调用方结算；事件通道在 Closed 之后已结束。
        assert!(m
            .client
            .respond_acked(3, json!(3), Ok(json!(true)))
            .is_err());
        assert!(m.events.recv().await.is_none());
        m.client.close().await;
    }

    #[tokio::test]
    async fn 关闭丢弃排队回答_写出中等待真实结果() {
        let mut m = Controlled::new();
        m.client
            .respond_acked(1, json!(1), Ok(json!(true)))
            .unwrap();
        m.entered.recv().await.unwrap();
        m.client
            .respond_acked(2, json!(2), Ok(json!(true)))
            .unwrap();
        let closing = m.client.clone();
        let close = tokio::spawn(async move { closing.close().await });
        assert!(matches!(m.outcome(2).await, ReplyOutcome::Failed(_)));
        assert!(m.events.try_recv().is_err());
        m.results.send(Ok(())).unwrap();
        assert_eq!(m.outcome(1).await, ReplyOutcome::Written);
        assert!(matches!(
            m.events.recv().await,
            Some(AppEvent::Closed { .. })
        ));
        close.await.unwrap();
        assert!(m.events.try_recv().is_err());
        assert!(m.entered.try_recv().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn 整帧写入十秒超时关闭传输且全部确认恰好一次() {
        let mut m = Controlled::new();
        m.client
            .respond_acked(1, json!(1), Ok(json!(true)))
            .unwrap();
        m.entered.recv().await.unwrap();
        m.client
            .respond_acked(2, json!(2), Ok(json!(true)))
            .unwrap();
        tokio::time::advance(Duration::from_millis(9999)).await;
        assert!(m.events.try_recv().is_err());
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(
            matches!(m.outcome(1).await,ReplyOutcome::Failed(message) if message.contains("10 秒"))
        );
        assert!(matches!(m.outcome(2).await, ReplyOutcome::Failed(_)));
        assert!(matches!(
            m.events.recv().await,
            Some(AppEvent::Closed { .. })
        ));
        m.client.close().await;
        assert!(m.events.try_recv().is_err());
        assert!(m.entered.try_recv().is_err());
    }

    #[tokio::test]
    async fn 传输断开在途与排队回答全部失败并先于_closed() {
        let mut m = Controlled::new();
        m.client
            .respond_acked(1, json!(1), Ok(json!(true)))
            .unwrap();
        m.entered.recv().await.unwrap();
        m.client
            .respond_acked(2, json!(2), Ok(json!(true)))
            .unwrap();
        m.client.fail("入站已断开");
        let mut ops = Vec::new();
        for _ in 0..2 {
            match m.events.recv().await.unwrap() {
                AppEvent::ReplyWritten {
                    op,
                    outcome: ReplyOutcome::Failed(_),
                } => ops.push(op),
                other => panic!("关闭过早：{other:?}"),
            }
        }
        ops.sort_unstable();
        assert_eq!(ops, vec![1, 2]);
        assert!(matches!(
            m.events.recv().await,
            Some(AppEvent::Closed { .. })
        ));
        m.client.close().await;
        assert!(m.events.try_recv().is_err());
    }

    #[tokio::test]
    async fn 撤回队列占位也受上限约束() {
        let mut m = Controlled::new();
        m.client.notify("阻塞写出", None);
        m.entered.recv().await.unwrap();
        for op in 1..=MAX_ACKED_REPLIES as u64 {
            m.client
                .respond_acked(op, json!(op), Ok(json!(true)))
                .unwrap();
            m.client.cancel_reply(op);
            assert_eq!(m.outcome(op).await, ReplyOutcome::Cancelled);
        }
        let next = MAX_ACKED_REPLIES as u64 + 1;
        assert!(
            matches!(m.client.respond_acked(next, json!(next), Ok(json!(true))), Err(message) if message.contains("上限"))
        );
        assert!(m.client.shared.state.lock().unwrap().acked.is_empty());
        m.client.fail("测试结束");
        m.client.close().await;
    }

    #[tokio::test]
    async fn 完成关闭后提交带确认回答不被受理() {
        let mut m = Memory::new(None);
        m.client.close().await;
        assert!(matches!(
            m.events.recv().await,
            Some(AppEvent::Closed { .. })
        ));
        assert!(m.events.recv().await.is_none());
        assert!(m.client.respond_acked(1, json!(1), Ok(json!({}))).is_err());
        assert!(m.client.respond_acked(1, json!(1), Ok(json!({}))).is_err());
    }
}
