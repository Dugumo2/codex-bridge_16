//! 唯一的核心事件循环（详细设计 1.2 节）。
//!
//! - core 串行持有连接、认领、线程记录、在途计数、空闲计时和关闭阶段。
//! - **core 不 await 任何外部 I/O**：发给 app-server 的请求、写客户端、关闭子进程，都只是投出去，
//!   结果以 [`CoreMsg`] 回到同一个邮箱。处理一条消息期间不让出，因此不存在「await 回来状态已变」的问题。
//! - 所有输入只有一个入口：客户端连接事件、app-server 事件、回包、关闭进度，都进同一个邮箱，按到达顺序处理。
//! - 客户端请求的回包与认领无关：转发时登记 (连接, 客户端 id)，回包一定送回原连接（连接还在的话）。

use std::collections::HashMap;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::appserver::{AppError, AppEvent, AppHandle, ServerInfo};
use crate::claim::{
    self, Action, ClaimToken, ClientRequest, Event, Machine, RequestContext, TimerKind,
};
use crate::ipc::server::{AcceptControl, CloseReason, ConnEvent, ConnId, WriterCmd};
use crate::proto::{self, code, Incoming};
use crate::runs::RunTimer;
use crate::store::{DrainReport, Loaded, StoreEvent, StoreHandle};

mod routing;
#[cfg(test)]
mod tests;

/// broker 私有扩展协议版本（详细设计 3.6 节）。
pub const PROTOCOL_VERSION: u64 = 1;

/// 上游 initialize 回包的 userAgent，黑盒测试 1 逐字断言。
pub const USER_AGENT: &str = "cxb-broker";

/// 关闭第 7 步：最多等多久让连接写完（上游 1334–1337 行）。
const DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

/// 关闭第 2、3 步（共享专用）各自最多等多久（上游 1291–1309 行，第 8 节）。
const SHUTDOWN_SETTLE_WAIT: Duration = Duration::from_secs(5);
const SHUTDOWN_STOP_WAIT: Duration = Duration::from_secs(5);

/// 进入 core 邮箱的全部消息。
#[derive(Debug)]
pub enum CoreMsg {
    /// 客户端连接层的事件。
    Conn(ConnEvent),
    /// app-server 发来的事件，包括 core 所发请求的回包（[`AppEvent::Reply`]）；读入的消息彼此保持读入顺序。
    App(AppEvent),
    /// 带认领代号的下一拍检查与吸收判定重试。
    ClaimTimer {
        token: ClaimToken,
        kind: TimerKind,
    },
    /// 转发超时核对转发编号和独立代号，避免旧计时器误结算。
    ForwardTimer {
        forward_id: String,
        generation: u64,
    },
    BreakerProbeTimer {
        generation: u64,
    },
    /// 关闭流程中，app-server 已关闭（或已放弃等待）。
    AppServerClosed,
    /// 关闭流程中，连接已排空（或等满 2 秒）。
    ConnectionsDrained,
    Store(StoreEvent),
    RunsDeadline,
    StoreDrained(DrainReport),
    /// 关闭第 2 或第 3 步的等待期满（只在仍处于该阶段时生效）。
    ShutdownWaitDeadline(ShutdownWait),
    /// 看门狗探针（[`crate::watchdog`]）：处理到即记下 core 仍在前进。
    WatchdogProbe(u64),
    /// 经安全阀计数的 app-server 消息；守卫在处理完、或消息随邮箱丢弃时扣回。
    AppIn {
        event: AppEvent,
        inflight: crate::watchdog::Inflight,
    },
}

/// 关闭第 2、3 步（共享专用）的等待阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownWait {
    /// 第 2 步：等 settling 定案。
    Settle,
    /// 第 3 步：等自有 turn 的暂停与中断回包。
    StopOwn,
}

/// core 的静态配置。
#[derive(Debug, Clone)]
pub struct CoreConfig {
    pub idle_timeout: Duration,
    pub server: ServerInfo,
}

#[derive(Debug)]
struct Conn {
    writer: mpsc::UnboundedSender<WriterCmd>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Running,
    /// 关闭中：已拒绝新连接、已让客户端连接收尾，正在关 app-server。
    ShuttingDown {
        exit_code: i32,
        app_closed: bool,
        drained: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShutdownStage {
    /// 第 2、3 步（共享专用）：认领仍在，邮箱照常处理；见 `claim::shutdown`。
    Waiting(ShutdownWait),
    Collecting,
    Draining,
    Closing,
}

pub struct Core {
    config: CoreConfig,
    app: AppHandle,
    /// 自己邮箱的发送端：投出去的异步操作完成后把结果投回来。
    mailbox: mpsc::UnboundedSender<CoreMsg>,
    accept: Option<AcceptControl>,
    conns: HashMap<ConnId, Conn>,
    machine: Machine,
    /// 已发给 app-server、尚未回包的请求：编号 → 发起时的上下文（空闲判定和 ifIdle 也看它）。
    pending_app: HashMap<u64, RequestContext>,
    late_app: std::collections::VecDeque<(u64, RequestContext, Instant)>,
    forward_timers: HashMap<(String, u64), tokio::task::AbortHandle>,
    probe_timer: Option<(u64, tokio::task::AbortHandle)>,
    store: Option<StoreHandle>,
    /// 写入任务终态后只保留句柄用于状态查询和排盘，不再投递写入。
    store_terminal: bool,
    loaded: Loaded,
    epoch: u64,
    survives_parent_job: bool,
    /// 外层 Job 的检测依据（[`crate::job::ParentJob`]），health 原样返回。
    parent_job_evidence: Value,
    progress_timers: HashMap<String, (u64, tokio::task::AbortHandle)>,
    /// 带键的可撤销计时（M6，见 `claim::TimerKey`）：同键只保留一个，记登记时的 kind 以便到期时认领。
    keyed_timers: HashMap<claim::TimerKey, (TimerKind, tokio::task::AbortHandle)>,
    daily_timer: Option<tokio::task::AbortHandle>,
    runs_deadline: Option<tokio::task::AbortHandle>,
    shutdown_wait_timer: Option<tokio::task::AbortHandle>,
    /// 看门狗与入站积压计数；测试与未接看门狗时为 None。
    health: Option<std::sync::Arc<crate::watchdog::Health>>,
    shutdown_stage: Option<ShutdownStage>,
    app_disconnected: bool,
    #[cfg(test)]
    fixed_clock: Option<crate::claim::Clock>,
    next_tag: u64,
    last_activity: Instant,
    /// 单调时钟的起点，`Clock.mono_ms` 从这里起算。
    started: Instant,
    phase: Phase,
}

impl Core {
    pub fn new(
        config: CoreConfig,
        app: AppHandle,
        mailbox: mpsc::UnboundedSender<CoreMsg>,
        accept: AcceptControl,
    ) -> Self {
        Self {
            machine: Machine::new(config.server.is_shared()),
            config,
            app,
            mailbox,
            accept: Some(accept),
            conns: HashMap::new(),
            pending_app: HashMap::new(),
            late_app: std::collections::VecDeque::new(),
            forward_timers: HashMap::new(),
            probe_timer: None,
            store: None,
            store_terminal: false,
            loaded: Loaded::default(),
            epoch: 0,
            survives_parent_job: false,
            parent_job_evidence: Value::Null,
            progress_timers: HashMap::new(),
            keyed_timers: HashMap::new(),
            daily_timer: None,
            runs_deadline: None,
            shutdown_wait_timer: None,
            health: None,
            shutdown_stage: None,
            app_disconnected: false,
            #[cfg(test)]
            fixed_clock: None,
            next_tag: 0,
            last_activity: Instant::now(),
            started: Instant::now(),
            phase: Phase::Running,
        }
    }

    /// 工作区标识（数据目录名里的哈希），gd_ 编号用。
    pub fn with_workspace_key(mut self, key: String) -> Self {
        self.machine.workspace_key = key;
        self
    }

    pub fn with_store(mut self, store: Option<StoreHandle>, mut loaded: Loaded) -> Self {
        if store.is_none() && loaded.lifecycle.is_none() {
            loaded.lifecycle = Some(Err("存储初始化失败，无法核实旧生命周期记录".into()));
        }
        self.epoch = loaded.epoch;
        self.store = store;
        self.loaded = loaded;
        self
    }

    pub fn with_health(mut self, health: std::sync::Arc<crate::watchdog::Health>) -> Self {
        self.health = Some(health);
        self
    }

    pub fn with_parent_job(mut self, parent_job: &crate::job::ParentJob) -> Self {
        self.survives_parent_job = parent_job.survives;
        self.parent_job_evidence = parent_job.evidence.clone();
        self
    }

    fn restore(&mut self) {
        let loaded = std::mem::take(&mut self.loaded);
        self.machine
            .lifecycle_restore(loaded.lifecycle.unwrap_or_else(|| Ok(Default::default())));
        self.dispatch(Event::Restore {
            index: loaded.index,
            runs: loaded.runs,
            inbox: loaded.inbox,
        });
    }

    /// 运行到关闭完成，返回进程退出码。
    pub async fn run(mut self, mut inbox: mpsc::UnboundedReceiver<CoreMsg>) -> i32 {
        self.restore();
        loop {
            if let Phase::ShuttingDown {
                exit_code,
                app_closed: true,
                drained: true,
            } = self.phase
            {
                if let Some(accept) = self.accept.take() {
                    accept.stop().await;
                }
                return exit_code;
            }
            let idle_deadline = (self.phase == Phase::Running)
                .then(|| self.last_activity + self.config.idle_timeout);
            tokio::select! {
                biased;
                msg = inbox.recv() => match msg {
                    Some(msg) => self.handle(msg),
                    // 发送端至少还有 self.mailbox，正常不会走到这里。
                    None => self.begin_shutdown(1),
                },
                () = sleep_until_opt(idle_deadline) => self.on_idle_deadline(),
            }
        }
    }

    fn handle(&mut self, msg: CoreMsg) {
        match msg {
            CoreMsg::Store(StoreEvent::ThreadsWritten { version, error }) => {
                self.machine.threads_written(version, error);
            }
            CoreMsg::Store(StoreEvent::Superseded { file }) => {
                self.store_terminal = true;
                tracing::error!(%file, "状态文件已由更新的 broker 接管，正常关闭");
                self.begin_shutdown(0);
            }
            CoreMsg::Store(StoreEvent::WriterDied { reason }) => {
                self.store_terminal = true;
                tracing::error!(%reason, "存储写入任务退出，降级继续服务");
            }
            CoreMsg::RunsDeadline => self.finalize_shutdown(),
            CoreMsg::StoreDrained(report) => {
                if self.shutdown_stage == Some(ShutdownStage::Draining) {
                    if !report.abandoned.is_empty() {
                        tracing::warn!(abandoned = ?report.abandoned, "关闭时放弃未写成功的文件");
                    }
                    self.close_transports();
                }
            }
            CoreMsg::Conn(ev) => self.on_conn_event(ev),
            CoreMsg::BreakerProbeTimer { generation } => {
                if self
                    .probe_timer
                    .as_ref()
                    .is_some_and(|(current, _)| *current == generation)
                {
                    self.probe_timer = None;
                }
                self.dispatch(Event::BreakerProbeTimer { generation });
            }
            CoreMsg::App(ev) => self.on_app_event(ev),
            CoreMsg::AppIn { event, inflight } => {
                self.on_app_event(event);
                drop(inflight);
            }
            CoreMsg::WatchdogProbe(seq) => {
                if let Some(health) = &self.health {
                    health.ack(seq);
                }
            }
            CoreMsg::ClaimTimer { token, kind } => {
                match &kind {
                    TimerKind::Run(RunTimer::ProgressFlush { run_id, generation }) => {
                        if self
                            .progress_timers
                            .get(run_id)
                            .is_some_and(|(g, _)| g == generation)
                        {
                            self.progress_timers.remove(run_id);
                        }
                    }
                    TimerKind::Run(RunTimer::DailyCleanup) => {
                        self.daily_timer = None;
                    }
                    _ => {
                        if let Some(key) = kind.key(&token) {
                            if self
                                .keyed_timers
                                .get(&key)
                                .is_some_and(|(registered, _)| *registered == kind)
                            {
                                self.keyed_timers.remove(&key);
                            }
                        }
                    }
                }
                self.dispatch(Event::Timer { token, kind });
            }
            CoreMsg::ForwardTimer {
                forward_id,
                generation,
            } => self.dispatch(Event::ForwardTimeout {
                forward_id,
                generation,
            }),
            CoreMsg::AppServerClosed => {
                if let Phase::ShuttingDown { app_closed, .. } = &mut self.phase {
                    *app_closed = true;
                }
            }
            CoreMsg::ConnectionsDrained => {
                if let Phase::ShuttingDown { drained, .. } = &mut self.phase {
                    *drained = true;
                }
            }
            CoreMsg::ShutdownWaitDeadline(wait) => {
                if self.shutdown_stage == Some(ShutdownStage::Waiting(wait)) {
                    tracing::warn!(?wait, "关闭等待期满，继续下一步");
                    self.shutdown_wait_over(wait);
                }
            }
        }
        self.advance_shutdown_wait();
        if self.shutdown_stage == Some(ShutdownStage::Collecting) && !self.results_pending() {
            self.finalize_shutdown();
        }
    }

    // ─── 客户端连接 ─────────────────────────────────────────────────────

    fn on_conn_event(&mut self, ev: ConnEvent) {
        match ev {
            ConnEvent::Connected { conn, writer } => {
                if self.phase != Phase::Running {
                    // 关闭开始前已接入、但事件晚于关闭到达的连接：直接让它收尾。
                    let _ = writer.send(WriterCmd::Close { done: None });
                    return;
                }
                self.touch();
                self.conns.insert(conn, Conn { writer });
                self.dispatch(Event::Connected { conn });
            }
            ConnEvent::Message { conn, message } => {
                if !self.conns.contains_key(&conn) {
                    return;
                }
                self.touch();
                match message {
                    Ok(value) => self.on_client_message(conn, value),
                    Err(reason) => self.send(conn, &proto::parse_error(reason)),
                }
            }
            ConnEvent::Closed { conn, reason } => {
                if reason == CloseReason::Overflow {
                    tracing::warn!(conn, "客户端缓冲超过上限，已断开");
                }
                // 丢掉写端：写任务随之结束，管道句柄关闭。
                self.conns.remove(&conn);
                self.dispatch(Event::Disconnected { conn });
            }
        }
    }

    fn on_client_message(&mut self, conn: ConnId, value: Value) {
        match Incoming::classify(value) {
            Incoming::Request { id, method, params } => {
                self.on_client_request(conn, id, method, params)
            }
            // 只吞掉 initialized；其余客户端通知一律忽略（上游 1648–1651、1727–1730 行）。
            Incoming::Notification { method, params } if method == "$/cancelRequest" => {
                if let Some(id) = params.as_ref().and_then(|p| p.get("id")) {
                    self.machine.lifecycle_cancel(conn, id);
                }
            }
            Incoming::Notification { method, .. } => {
                tracing::debug!(conn, %method, "忽略客户端通知");
            }
            Incoming::Response { id, result, error } => {
                self.dispatch(Event::ForwardResponse {
                    conn,
                    id,
                    result,
                    error,
                });
            }
            Incoming::Invalid(v) => tracing::debug!(conn, message = %v, "忽略不合规范的消息"),
        }
    }

    fn on_client_request(
        &mut self,
        conn: ConnId,
        id: Value,
        method: String,
        params: Option<Value>,
    ) {
        match method.as_str() {
            "initialize" => {
                let mut result = json!({
                    "userAgent": USER_AGENT,
                    // 整个 broker 永远不忙；同线程冲突按请求回 -32001（清单第 8 节第 5 条）。
                    "busy": false,
                    "server": self.config.server,
                });
                let cxb = params
                    .as_ref()
                    .and_then(|p| p.get("capabilities"))
                    .and_then(|c| c.get("cxb"))
                    .is_some_and(|c| !c.is_null());
                if cxb {
                    result["brokerVersion"] = json!(env!("CARGO_PKG_VERSION"));
                    result["protocolVersion"] = json!(PROTOCOL_VERSION);
                    result["epoch"] = json!(self.epoch);
                } else if let Some(server) = result["server"].as_object_mut() {
                    // `via` 是上游没有的字段（第 2 档），按 3.6 节门控只给 cxb 连接；health 不受门控。
                    server.remove("via");
                }
                self.send(conn, &proto::response_ok(id, result));
                self.dispatch(Event::Initialized { conn, cxb });
            }
            "broker/health" => self.health(conn, id),
            "broker/shutdown" => {
                let if_idle = params
                    .as_ref()
                    .and_then(|p| p.get("ifIdle"))
                    .and_then(Value::as_bool)
                    == Some(true);
                if if_idle && self.is_busy() {
                    let err = proto::error_object(
                        code::THREAD_BUSY,
                        "A turn is running on this broker.",
                        None,
                    );
                    self.send(conn, &proto::response_err(id, err));
                    return;
                }
                self.send(conn, &proto::response_ok(id, json!({})));
                self.begin_shutdown(0);
            }
            _ => {
                let run_id = routing::run_id_for(&method, &params);
                self.dispatch(Event::ClientRequest {
                    request: ClientRequest {
                        conn,
                        id,
                        method,
                        params,
                    },
                    run_id,
                });
            }
        }
    }

    /// 把请求发给 app-server。回包以 [`AppEvent::Reply`] 和通知走同一条通道，与读入的通知保持
    /// 读入顺序进邮箱（1.2 节），再凭编号找回发起时的上下文。客户端事件、计时器与 app-server
    /// 消息之间没有线上顺序可言，各处按 claim_id 和状态核对，不依赖它们的先后。
    fn forward_request(&mut self, context: RequestContext) {
        self.next_tag += 1;
        let tag = self.next_tag;
        // 上游转发时缺省 params 为 {}（broker-server.ts 1814 行）。
        let sent = self.app.request_ordered(
            &context.method,
            Some(context.params.clone().unwrap_or_else(|| json!({}))),
            tag,
        );
        if let Some(thread) = context
            .params
            .as_ref()
            .and_then(|p| p.get("threadId"))
            .and_then(Value::as_str)
            .or_else(|| context.token.as_ref().map(|token| token.thread_id.as_str()))
        {
            if !matches!(context.operation, claim::Operation::Lifecycle { .. }) {
                self.machine.purge_inflight.insert(tag, thread.to_owned());
            }
        }
        self.pending_app.insert(tag, context);
        if let Err(error) = sent {
            // 连接已关闭：失败也投回邮箱，与正常回包走同一条路径，不在这里重入状态机。
            let _ = self.mailbox.send(CoreMsg::App(AppEvent::Reply {
                tag,
                result: Err(error),
            }));
        }
    }

    fn on_app_reply(&mut self, tag: u64, result: Result<Value, AppError>) {
        self.machine.purge_inflight.remove(&tag);
        if matches!(&result, Ok(_) | Err(AppError::Rpc { .. })) {
            self.dispatch(Event::AppResponsive);
        }
        let Some(context) = self.pending_app.remove(&tag) else {
            tracing::warn!(tag, "收到未登记的 app-server 回包编号，忽略");
            return;
        };
        if matches!(&result, Err(AppError::Closed(_))) {
            self.dispatch(Event::TransportFailed {
                context,
                error: app_result(result).unwrap_err(),
            });
        } else {
            self.dispatch(Event::Reply {
                context,
                result: app_result(result),
            });
        }
    }

    fn on_timeout(&mut self, tag: u64, error: AppError, deadline: std::time::Instant) {
        let Some(context) = self.pending_app.remove(&tag) else {
            return;
        };
        if matches!(context.operation, claim::Operation::ResultRead(_)) {
            self.machine.purge_inflight.remove(&tag);
            self.app.inner.forget_timeout(tag);
            // 本地超时没有服务器存活证据，不能走会恢复熔断状态的 Reply 入口。
            self.dispatch(Event::TransportFailed {
                context,
                error: app_result(Err(error)).unwrap_err(),
            });
            return;
        }
        self.late_app
            .push_back((tag, context.clone(), Instant::from_std(deadline)));
        self.dispatch(Event::TimedOut {
            tag,
            deadline,
            context,
            error: app_result(Err(error)).unwrap_err(),
        });
    }

    fn forget_late(&mut self, tag: u64) {
        self.machine.purge_inflight.remove(&tag);
        self.app.inner.forget_timeout(tag);
        if let Some(index) = self.late_app.iter().position(|(got, _, _)| *got == tag) {
            let (_, context, _) = self.late_app.remove(index).unwrap();
            self.dispatch(Event::LateDiscarded { context });
        }
    }

    /// RPC 登记表满而挤出最旧一条：线上回包不再交付，但原截止时刻仍有效，
    /// 由状态机决定哪些状态要保留到原截止（例如结果未知的 goal 写入屏障）。
    fn evict_late(&mut self, tag: u64) {
        self.machine.purge_inflight.remove(&tag);
        if let Some(index) = self.late_app.iter().position(|(got, _, _)| *got == tag) {
            let (_, context, deadline) = self.late_app.remove(index).unwrap();
            self.dispatch(Event::LateEvicted {
                context,
                deadline: deadline.into_std(),
            });
        }
    }

    fn on_late_reply(&mut self, tag: u64, result: Result<Value, AppError>) {
        if !matches!(&result, Ok(_) | Err(AppError::Rpc { .. })) {
            return;
        }
        if matches!(&result, Ok(_) | Err(AppError::Rpc { .. })) {
            self.dispatch(Event::AppResponsive);
        }
        if let Some(index) = self.late_app.iter().position(|(got, _, _)| *got == tag) {
            self.machine.purge_inflight.remove(&tag);
            let (_, context, _) = self.late_app.remove(index).unwrap();
            // 入站与 RPC 的截止事件同序；邮箱处理延迟不能改变 RPC 已接收的回包效力。
            if matches!(result, Err(AppError::Closed(_)))
                && matches!(context.operation, claim::Operation::Lifecycle { .. })
            {
                self.dispatch(Event::TransportFailed {
                    context,
                    error: app_result(result).unwrap_err(),
                });
            } else {
                self.dispatch(Event::LateReply {
                    context,
                    result: app_result(result),
                });
            }
        }
    }

    // ─── app-server 事件 ───────────────────────────────────────────────

    fn on_app_event(&mut self, ev: AppEvent) {
        match ev {
            AppEvent::Responsive => {
                self.touch();
                self.dispatch(Event::AppResponsive);
            }
            AppEvent::PrivateInstanceExited { instance } => {
                self.dispatch(Event::PrivateInstanceExited { instance });
            }
            AppEvent::TimedOut {
                tag,
                error,
                deadline,
            } => self.on_timeout(tag, error, deadline),
            AppEvent::LateDiscarded { tag } => self.forget_late(tag),
            AppEvent::LateEvicted { tag } => self.evict_late(tag),
            AppEvent::LateReply { tag, result } => self.on_late_reply(tag, result),
            AppEvent::Notification { method, params } => {
                self.touch();
                self.dispatch(Event::Notification { method, params });
            }
            AppEvent::ServerRequest { id, method, params } => {
                self.touch();
                let forward_id = loop {
                    let candidate = routing::forward_id();
                    if !self.machine.requests.pending.contains_key(&candidate)
                        && !self
                            .machine
                            .claims
                            .values()
                            .any(|c| c.held_requests.iter().any(|r| r.forward_id == candidate))
                    {
                        break candidate;
                    }
                };
                self.dispatch(Event::ServerRequest {
                    id,
                    forward_id,
                    method,
                    params,
                });
            }
            AppEvent::Reply { tag, result } => self.on_app_reply(tag, result),
            AppEvent::ReplyWritten { op, outcome } => {
                self.dispatch(Event::AppReplyWritten { op, outcome })
            }
            AppEvent::Closed { reason } => {
                self.app_disconnected = true;
                if self.phase == Phase::Running {
                    tracing::error!(%reason, "app-server 意外退出，broker 关闭");
                    eprintln!("[cxb-broker] app-server 意外退出，正在关闭");
                    self.begin_shutdown(1);
                } else if self.shutdown_stage == Some(ShutdownStage::Collecting) {
                    self.finalize_shutdown();
                }
            }
        }
    }

    // ─── 空闲与关闭 ─────────────────────────────────────────────────────

    fn touch(&mut self) {
        self.last_activity = Instant::now();
    }

    /// 空闲判定（第 8 节）：活动认领与在途请求阻止空闲退出。
    fn is_busy(&self) -> bool {
        if self.machine.lifecycle_busy() {
            return true;
        }
        // 结果补读与释放后的自动退订都是内部收尾，不代表有 turn 在跑；关闭会断开连接，不必等它们回包。
        self.pending_app.values().any(|c| {
            !matches!(
                c.operation,
                claim::Operation::ResultRead(_) | claim::Operation::Unsubscribe
            )
        }) || !self.machine.claims.is_empty()
            || !self.machine.requests.pending.is_empty()
            || self.machine.stops_busy()
            || self.machine.tree_busy(None)
            || self.machine.app_server_unresponsive
            || self.machine.inbox_busy()
            || self.machine.watched_results_pending()
    }

    /// `broker/health`（2.3 节）：storage 取 [`StoreHandle::status`]，没有 store 时为 degraded。
    fn health(&mut self, conn: ConnId, id: Value) {
        let storage = if !self.store_terminal
            && self.machine.lifecycle.degraded.is_none()
            && self
                .store
                .as_ref()
                .is_some_and(|s| s.status() == crate::store::StorageStatus::Ok)
        {
            "ok"
        } else {
            "degraded"
        };
        let inbox_pending = self.machine.inbox_pending_total();
        let pending = self.machine.requests.pending.len() + inbox_pending;
        self.send(conn, &proto::response_ok(id, json!({
            "brokerVersion":env!("CARGO_PKG_VERSION"),"protocolVersion":PROTOCOL_VERSION,"epoch":self.epoch,
            "server":self.config.server,"claims":self.machine.claims.len(),"pending":pending,
            "uptimeMs":self.started.elapsed().as_millis() as u64,"survivesParentJob":self.survives_parent_job,"parentJob":self.parent_job_evidence,"storage":storage,"storageReason":self.machine.lifecycle.degraded
        })));
    }

    fn on_idle_deadline(&mut self) {
        if self.is_busy() {
            self.touch();
            return;
        }
        tracing::info!("空闲超时，broker 关闭");
        eprintln!("[cxb-broker] 空闲超时，正在关闭");
        self.begin_shutdown(0);
    }

    /// 第 1 步拒绝新连接；共享服务器先走第 2、3 步（[`ShutdownStage::Waiting`]），再进第 4 步。
    fn begin_shutdown(&mut self, exit_code: i32) {
        if self.phase != Phase::Running {
            return;
        }
        self.phase = Phase::ShuttingDown {
            exit_code,
            app_closed: false,
            drained: false,
        };
        if let Some(accept) = &self.accept {
            accept.refuse_new();
        }
        if self.machine.shared && !self.app_disconnected {
            self.dispatch(Event::ShutdownBegin);
            self.enter_shutdown_wait(ShutdownWait::Settle);
        } else {
            self.enter_collecting();
        }
    }

    fn enter_shutdown_wait(&mut self, wait: ShutdownWait) {
        self.shutdown_stage = Some(ShutdownStage::Waiting(wait));
        if wait == ShutdownWait::StopOwn {
            self.dispatch(Event::ShutdownStopOwn);
        }
        let limit = match wait {
            ShutdownWait::Settle => SHUTDOWN_SETTLE_WAIT,
            ShutdownWait::StopOwn => SHUTDOWN_STOP_WAIT,
        };
        let mailbox = self.mailbox.clone();
        if let Some(timer) = self.shutdown_wait_timer.replace(
            tokio::spawn(async move {
                tokio::time::sleep(limit).await;
                let _ = mailbox.send(CoreMsg::ShutdownWaitDeadline(wait));
            })
            .abort_handle(),
        ) {
            timer.abort();
        }
        self.advance_shutdown_wait();
    }

    /// 每处理完一条消息检查一次（上游每 50 ms 轮询一次，这里由事件驱动）：等的东西没了就进下一步。
    /// app-server 已断开时不再等任何回包。
    fn advance_shutdown_wait(&mut self) {
        let Some(ShutdownStage::Waiting(wait)) = self.shutdown_stage else {
            return;
        };
        if self.app_disconnected {
            // 等待中 app-server 断开：第 3 步的请求只会本地失败，直接进第 4 步，不再派发停止动作。
            if let Some(timer) = self.shutdown_wait_timer.take() {
                timer.abort();
            }
            self.enter_collecting();
            return;
        }
        let done = match wait {
            ShutdownWait::Settle => self.machine.settling_count() == 0,
            ShutdownWait::StopOwn => self.machine.shutdown_stops_pending() == 0,
        };
        if done {
            self.shutdown_wait_over(wait);
        }
    }

    fn shutdown_wait_over(&mut self, wait: ShutdownWait) {
        if let Some(timer) = self.shutdown_wait_timer.take() {
            timer.abort();
        }
        match wait {
            ShutdownWait::Settle => self.enter_shutdown_wait(ShutdownWait::StopOwn),
            ShutdownWait::StopOwn => self.enter_collecting(),
        }
    }

    /// 第 4 步先标 lost 再释放；第 5 步以邮箱事件推进补读与排盘；第 6、7 步并行关闭。
    /// 计时在这里才作废：第 2、3 步期间认领和 inbox 仍在正常运转。
    fn enter_collecting(&mut self) {
        self.shutdown_stage = Some(ShutdownStage::Collecting);
        if let Some(timer) = self.daily_timer.take() {
            timer.abort();
        }
        for (_, (_, timer)) in self.progress_timers.drain() {
            timer.abort();
        }
        for (_, (_, timer)) in self.keyed_timers.drain() {
            timer.abort();
        }
        self.dispatch(Event::Shutdown);
        self.late_app.clear();
        if !self.app_disconnected && self.results_pending() {
            let mailbox = self.mailbox.clone();
            let deadline = Instant::now() + Duration::from_secs(3);
            self.runs_deadline = Some(
                tokio::spawn(async move {
                    tokio::time::sleep_until(deadline).await;
                    let _ = mailbox.send(CoreMsg::RunsDeadline);
                })
                .abort_handle(),
            );
        } else {
            self.finalize_shutdown();
        }
    }

    fn results_pending(&self) -> bool {
        self.machine.result_reads_pending()
            || self
                .pending_app
                .values()
                .any(|c| matches!(c.operation, claim::Operation::ResultRead(_)))
    }

    fn finalize_shutdown(&mut self) {
        if self.shutdown_stage != Some(ShutdownStage::Collecting) {
            return;
        }
        if let Some(timer) = self.runs_deadline.take() {
            timer.abort();
        }
        self.dispatch(Event::RunsFinalize);
        self.pending_app
            .retain(|_, c| !matches!(c.operation, claim::Operation::ResultRead(_)));
        self.shutdown_stage = Some(ShutdownStage::Draining);
        if let Some(store) = self.store.clone() {
            let mailbox = self.mailbox.clone();
            tokio::spawn(async move {
                let report = store.drain(Duration::from_secs(2)).await;
                let _ = mailbox.send(CoreMsg::StoreDrained(report));
            });
        } else {
            self.close_transports();
        }
    }

    fn close_transports(&mut self) {
        self.shutdown_stage = Some(ShutdownStage::Closing);
        let mut done = Vec::new();
        for (_, conn) in self.conns.drain() {
            let (tx, rx) = oneshot::channel();
            if conn
                .writer
                .send(WriterCmd::Close { done: Some(tx) })
                .is_ok()
            {
                done.push(rx);
            }
        }
        let mailbox = self.mailbox.clone();
        tokio::spawn(async move {
            let all = async {
                for rx in done {
                    let _ = rx.await;
                }
            };
            let _ = tokio::time::timeout(DRAIN_TIMEOUT, all).await;
            let _ = mailbox.send(CoreMsg::ConnectionsDrained);
        });

        let app = self.app.clone();
        let mailbox = self.mailbox.clone();
        tokio::spawn(async move {
            app.close().await;
            let _ = mailbox.send(CoreMsg::AppServerClosed);
        });
    }

    fn send(&self, conn: ConnId, message: &Value) {
        if let Some(c) = self.conns.get(&conn) {
            let _ = c.writer.send(WriterCmd::Line(proto::to_line(message)));
        }
    }
}

async fn sleep_until_opt(deadline: Option<Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending().await,
    }
}

fn app_result(result: Result<Value, AppError>) -> Result<Value, Value> {
    match result {
        Ok(value) => Ok(value),
        Err(AppError::Rpc {
            code,
            message,
            data,
        }) => Err(proto::error_object(code, message, data)),
        Err(other) => Err(proto::error_object(
            code::GENERAL_ERROR,
            other.to_string(),
            None,
        )),
    }
}
