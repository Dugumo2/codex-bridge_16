//! 纯函数认领状态机（详细设计 1.2、2.4、3.1–3.5、8 节）。
//!
//! 状态包括跨认领保留的线程记录。step 返回新状态和动作，外部 I/O 由
//! core 执行。本模块仅依赖标准库、proto 与 serde_json。

use crate::proto::{self, code};
use crate::runs::upstream_error::{UpstreamError, UpstreamErrorSource};
use crate::runs::{ReleaseKind, RunOpen};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Instant;

pub mod converge;
mod ext;
pub mod finish_gate;
pub mod lifecycle;
pub mod loop_guard;
pub mod loop_guard_input;
#[cfg(test)]
#[path = "tests/m7.rs"]
mod m7_tests;
mod raw_events;
pub mod resume;
mod settling;
pub mod shutdown;
pub mod status_read;
pub mod subagent_reconcile;
#[cfg(test)]
mod tests;

pub type ConnId = u64;
pub type RunId = String;
pub const MAX_HELD_DELTAS: usize = 200;
/// settling 缓冲的总量预算（M7，S1-评审第 22 节，用户 2026-10-04 选定方案 A）。按认领计；字节按
/// 「方法名长度 + params 序列化长度」估算，进缓冲时一次算好、增量维护，不重复序列化。
/// 软上限：达到后新到的可丢通知（[`settling::droppable_while_settling`]）只计数、不缓冲。
pub const SETTLING_SOFT_ITEMS: usize = 1000;
pub const SETTLING_SOFT_BYTES: usize = 8 * 1024 * 1024;
/// 硬上限：照收的通知也算在内，超过即按 joined 保守定案（与 undecided 十次后的出口相同）。
pub const SETTLING_HARD_ITEMS: usize = 4000;
pub const SETTLING_HARD_BYTES: usize = 32 * 1024 * 1024;
/// 所有 settling 认领合计的字节上限（与 inbox 一致）；超出时占用最多的认领按硬上限处理。
pub const SETTLING_GLOBAL_BYTES: usize = 64 * 1024 * 1024;
/// 每个 settling 认领最多挂起的服务端请求；多出的不答（Dismiss），交给真正的 owner。
pub const MAX_HELD_REQUESTS: usize = 64;
/// 回包前已溢出的认领，最多再缓冲多少条 `turn/` 生命周期通知（之后只转发）。
pub const OVERFLOW_LIFECYCLE_ITEMS: usize = 256;
pub const MAX_UNCLAIMED: usize = 500;
pub const ENDED_PER_THREAD: usize = 16;
pub const MAX_HISTORY: usize = 200;
/// 关闭中不再开工时回给客户端的错误（迁移表 #1，上游 1756–1765 行原文）。还在等恢复订阅、
/// 尚未发出的流式请求在关闭清理时也回这一条。
pub const SHUTTING_DOWN: &str =
    "The broker is shutting down; nothing new is started here. Retry in a moment.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Owner {
    Client(ConnId),
    /// 托管（#26a）。时刻取 `Machine.clock.mono_ms`（单调毫秒），#26d 回收按它判断。
    Broker {
        policy: crate::policy::Policy,
        approvals_reviewer: Option<String>,
        owned_at: u64,
        last_watcher_at: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReqKind {
    TurnStart,
    ReviewStart,
    TurnSteer,
}

impl ReqKind {
    fn from_method(method: &str) -> Option<Self> {
        match method {
            "turn/start" => Some(Self::TurnStart),
            "review/start" => Some(Self::ReviewStart),
            "turn/steer" => Some(Self::TurnSteer),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContinueCheck {
    pub from_run: RunId,
    pub resume_goal: bool,
    /// core 收到请求时预先生成的运行编号；#2d 核实通过时才绑定到认领、建运行记录。
    pub run_id: RunId,
}

/// #26a'：settling 期间排队等定案的交接请求（丙路）。每个认领最多一个；定案、断开、释放时清理。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingDetach {
    pub request: ClientRequest,
    pub policy: crate::policy::Policy,
    pub approvals_reviewer: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldNotification {
    pub method: String,
    pub params: Option<Value>,
    /// 子线程早到通知在缓冲前已提交，回放只驱动新认领。
    pub committed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldRequest {
    pub id: Value,
    pub forward_id: String,
    pub method: String,
    pub params: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    Idle,
    Starting {
        owner: Owner,
        req: ReqKind,
        turn_id: Option<String>,
        cont: Option<ContinueCheck>,
        settling: bool,
        undecided: Option<u8>,
        submitted_at: usize,
        held: Vec<HeldNotification>,
    },
    Running {
        owner: Owner,
        turn_id: String,
        joined: bool,
    },
    /// SA1b 正常收敛，保留认领；骨架不构造此状态。
    Converging {
        owner: Owner,
        turn_id: Option<String>,
        parent_status: Option<crate::runs::TurnStatus>,
        goal_status: Option<String>,
        token: crate::sa1b::ConvergeToken,
        phase: crate::sa1b::JoinPhase,
    },
    /// SA-b 持有父轮结束快照；迟到事件不得把它恢复为 Running。
    FinishingTree {
        owner: Owner,
        turn_id: Option<String>,
        parent_status: Option<crate::runs::TurnStatus>,
        goal_status: Option<String>,
        identity: crate::relations::RunIdentity,
        gen: u64,
        tree_op: u64,
        release_kind: ReleaseKind,
    },
    AwaitingContinuation {
        owner: Owner,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claim {
    pub stream_id: Option<u64>,
    pub owned_ended: VecDeque<String>,
    /// 仅记录已观察且归属本认领的轮次，供停止操作筛选结束证据。
    pub owned_turns: HashSet<String>,
    pub claim_id: u64,
    pub state: State,
    pub run_id: Option<RunId>,
    pub resume_goal: bool,
    pub request_pending: bool,
    pub owner_gone: bool,
    /// 依赖屏障解除前保留原请求，之后消费。
    pub pending_start: Option<ClientRequest>,
    pub submitted_input: Vec<String>,
    pub held_requests: Vec<HeldRequest>,
    /// 单独计数，避免 outputDelta 很多时每次入队都扫描整个 held。
    pub held_deltas: usize,
    /// held 的估算字节数（增量维护，定案或释放时清零并从 `Machine.settling_bytes` 扣回）。
    pub held_bytes: usize,
    /// 因软上限没有缓冲的 `item/started`、`item/completed` 条数（delta 不计：它们不进进度记录）；
    /// 定案为自己的且大于 0 时，运行标「进度不完整」。
    pub held_dropped_items: usize,
    /// 硬上限在 start 回包之前触发：已决定按 joined 保守定案，等回包给出目标 turn（见 `settling_overflow`）。
    pub overflowed: bool,
    /// 首次判定前暂存回包；undecided 后立即交付客户端。
    pub settlement_reply: Option<(RequestContext, Value)>,
    /// #26a'：settling 期间排队的交接（丙路）。
    pub pending_detach: Option<PendingDetach>,
}

impl Claim {
    pub fn owner(&self) -> Option<&Owner> {
        match &self.state {
            State::Starting { owner, .. }
            | State::Running { owner, .. }
            | State::Converging { owner, .. }
            | State::FinishingTree { owner, .. }
            | State::AwaitingContinuation { owner } => Some(owner),
            State::Idle => None,
        }
    }
    pub fn turn_id(&self) -> Option<&str> {
        match &self.state {
            State::Starting { turn_id, .. }
            | State::Converging { turn_id, .. }
            | State::FinishingTree { turn_id, .. } => turn_id.as_deref(),
            State::Running { turn_id, .. } => Some(turn_id),
            _ => None,
        }
    }
    pub fn joined(&self) -> bool {
        matches!(
            self.state,
            State::Running { joined: true, .. }
                | State::Starting {
                    req: ReqKind::TurnSteer,
                    ..
                }
        )
    }
    pub fn settling(&self) -> bool {
        matches!(self.state, State::Starting { settling: true, .. })
    }
    /// 续跑预检中（#2c）：还没有运行、请求未转发。
    pub fn precheck(&self) -> bool {
        matches!(self.state, State::Starting { cont: Some(_), .. })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ThreadRecord {
    /// 客户端主动请求的 raw 可见性；broker 自动开启不等于客户端选择接收。
    pub raw_clients: HashSet<ConnId>,
    pub ended_turns: VecDeque<String>,
    /// 已释放认领的最近轮次，只隔离迟到生命周期；最多 ENDED_PER_THREAD 项，随线程记录淘汰。
    pub released_turns: VecDeque<String>,
    pub goal_active: bool,
    pub subscribed: bool,
    pub last_run: Option<RunId>,
    pub watchers: Vec<ConnId>,
    /// 按线程等待的独占观察者；非空时该连接一定在 watchers 中。
    pub exclusive_waiter: Option<ConnId>,
    pub pending_meta: HashMap<ConnId, Value>,
    pub result_pending: bool,
    /// 释放后保留代号，旧退订检查不能作用于已再次释放的认领。
    pub last_claim_id: u64,
    /// 空转刹车判断「goal 真正变化」用的最近目标与状态（`loop_guard::guard_goal`）；
    /// `goal_seen` 为假时第一次观察只记下、不算变化。
    pub goal_seen: bool,
    pub goal_objective: Option<String>,
    pub goal_status: Option<String>,
    /// goal 的 createdAt（JSON 文本），与目标原文一起认定是不是同一个 goal。
    pub goal_created: Option<String>,
    /// 共享模式：当前 goal 是本 broker 的客户端设置或恢复的，空转刹车可以暂停它（S1-评审第 26 节）。
    pub goal_controlled: bool,
    /// 控制资格代号：goal 变为非 active、自动轮出现用户输入时推进；发出时记下旧代号的写入回包不再授予。
    pub goal_control_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientRequest {
    pub conn: ConnId,
    pub id: Value,
    pub method: String,
    pub params: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimToken {
    pub thread_id: String,
    pub claim_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    BudgetVerify {
        verification: crate::runs::budget::BudgetVerification,
    },
    StartReconciliation {
        token: crate::sa1b::ConvergeToken,
        op_id: crate::sa1b::ReconcileOpId,
    },
    ChildResultRead {
        token: crate::sa1b::ConvergeToken,
        read_id: crate::sa1b::BatchReadId,
        key: crate::sa1b::ActivityKey,
        page_generation: u64,
    },
    /// H2-a 接读取分派；本批只保留未启用分支。
    StatusRead {
        read_id: u64,
        generation: u64,
    },
    RelationLookup {
        op_id: u64,
        generation: u64,
        thread: String,
    },
    Discovery {
        op_id: u64,
        generation: u64,
    },
    DerivedStop {
        tree_op: u64,
        generation: u64,
        thread: String,
        round: u64,
    },
    AttachThread {
        op_id: u64,
        generation: u64,
        thread: String,
    },
    /// 子线程责任操作（恢复、退订及只读核实），由唯一操作编号隔离迟到回包。
    ChildSubscription {
        thread: String,
        op: u64,
    },

    Lifecycle {
        thread: String,
        operation: String,
        attempt: u64,
    },
    BreakerProbe {
        generation: u64,
    },
    Forward,
    ResumeBarrier,
    /// 流式请求；带上运行编号，父认领过期时 review 子线程仍能接上同一次运行。
    Stream {
        run_id: Option<RunId>,
    },
    Unsubscribe,
    InterruptRetry {
        original_error: Value,
    },
    Stop(crate::stop::StopTag, crate::stop::StopRequest),
    LateInterrupt {
        generation: u64,
    },
    LateGoal {
        generation: u64,
        round: u64,
        version: u64,
        write: bool,
    },
    GoalChange {
        id: u64,
        version: u64,
        inner: Box<Operation>,
    },
    GoalRead {
        version: u64,
        inner: Box<Operation>,
    },
    /// 结果补读（3.6 节）；回包交给 runs 模块，不计入空闲判定。
    ResultRead(crate::runs::ResultRead),
    /// 续跑预检（#2c）的恢复订阅；回包交给乙路 `continue_reply`。
    ContinueResume,
    /// 续跑预检的核实读；回包交给乙路 `continue_reply`。
    ContinueRead(resume::ContinueRead),
    /// goal 恢复操作（#2e）的读写；写入经 stop 模块统一登记，解包后交给乙路 `goal_restore_reply`。
    GoalRestore(resume::GoalRestoreStep),
    /// inbox 发出的请求（Guardian override），回包交给甲路 `inbox_rpc_reply`。
    Inbox(crate::inbox::InboxRpc),
    /// 关闭第 3 步（共享专用）发出的暂停 goal 或中断；回包交给 `shutdown::shutdown_stop_reply`。
    ShutdownStop(shutdown::ShutdownStopPhase),
    /// 共享模式空转刹车的暂停 goal（不中断任何轮）；带刹车时受控的 goal 身份，回包交给
    /// `loop_guard::loop_guard_pause_reply`。
    LoopGuardPause(loop_guard::PauseTarget),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestContext {
    /// 每次内部恢复最多一次未知配置键降级；随请求贯穿超时与迟到回包。
    pub resume_retried: bool,
    /// 从受理到落定的唯一开工名额，resume 屏障与超时对账沿用同一标识。
    pub stream_id: Option<u64>,
    /// 每个实际发出的审查请求有独立标识，普通回包与迟到收尾共用一次计数结算。
    pub review_id: Option<u64>,
    pub client: Option<ClientRequest>,
    pub token: Option<ClaimToken>,
    pub operation: Operation,
    pub method: String,
    pub params: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerKind {
    ChildBarrier {
        waiter: crate::relations::ChildWaiter,
    },
    ChildSubscription {
        thread: String,
        op: u64,
    },
    ChildRetry {
        thread: String,
        op: u64,
    },
    Stall {
        run_id: String,
        generation: u64,
    },
    BudgetVerifyDeadline {
        token: crate::sa1b::ConvergeToken,
    },
    ConvergeDeadline {
        token: crate::sa1b::ConvergeToken,
    },
    ConvergeRetry {
        token: crate::sa1b::ConvergeToken,
        attempt: u32,
    },
    ChildResultsDeadline {
        token: crate::sa1b::ConvergeToken,
        read_id: crate::sa1b::BatchReadId,
    },
    ReconcileVerifyDeadline {
        token: crate::sa1b::ConvergeToken,
        op_id: crate::sa1b::ReconcileOpId,
    },
    /// 状态读取的 20 秒总期限（可撤销，完成时撤销）。
    StatusReadDeadline {
        read_id: u64,
    },
    LookupDeadline {
        op_id: u64,
        generation: u64,
    },
    DiscoveryDeadline {
        op_id: u64,
        generation: u64,
    },
    TreeStopWait {
        tree_op: u64,
        waiter: Option<u64>,
        generation: u64,
    },
    TreeStopVerify {
        tree_op: u64,
        round: u64,
        generation: u64,
    },
    TombstoneExpire {
        conn_epoch: u64,
        generation: u64,
    },

    Lifecycle,
    Unsubscribe,
    SettlementRetry {
        attempt: u8,
    },
    JoinedCheckRetry {
        attempt: u8,
    },
    Stop(crate::stop::StopTag, crate::stop::StopTimer),
    Reconcile {
        generation: u64,
        round: u64,
    },
    LateExpiry {
        generation: u64,
    },
    GoalChangeExpiry {
        id: u64,
    },
    Run(crate::runs::RunTimer),
    /// inbox 的过期与 wait 超时（甲路）。
    Inbox(crate::inbox::InboxTimer),
    /// #26d 托管回收（丙路）；token 带 claim_id，generation 区分同一认领内的重排，触发时再核对。
    Reclaim {
        generation: u64,
    },
    /// #26a'：settling 期间排队的交接等待期限（丙路，`ext::DETACH_WAIT_MS`）。
    DetachDeadline {
        generation: u64,
    },
}

/// 可撤销计时的键（M6）。core 对带键的计时只保留一个：同键再次 `Schedule` 时撤销旧计时，
/// `Action::Unschedule` 直接撤销；计时到期时若仍是登记的那一个才从表里移除。
/// 没有键的计时（M2–M5 的各类）行为不变。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TimerKey {
    EventLogSweep,
    ChildBarrier(crate::relations::ChildWaiter),
    ChildSubscription(String),
    ChildRetry(String),
    Stall(String),
    BudgetVerifyDeadline(String, u64),
    ConvergeDeadline(String, u64),
    ConvergeRetry(String, u64),
    ChildResultsDeadline(crate::sa1b::BatchReadId),
    ReconcileVerifyDeadline(crate::sa1b::ReconcileOpId),
    Stop(String, u64, crate::stop::StopTimer),
    StatusRead(u64),
    LookupDeadline(u64),
    DiscoveryDeadline(u64),
    TreeStopWait {
        tree_op: u64,
        waiter: Option<u64>,
    },
    TreeStopVerify(u64),
    TombstoneExpire(u64),
    /// inbox 事项的过期与已关闭项的清理共用一个键（甲路）。
    InboxItem(String),
    /// `broker/inbox/wait` 等待者的超时（甲路）。
    InboxWaiter(u64),
    /// 线程上托管认领的回收计时（丙路）。
    Reclaim(String),
    /// 线程上排队交接的等待期限（丙路）。
    DetachDeadline(String),
    JoinedCheck(String),
}

impl TimerKind {
    pub fn key(&self, token: &ClaimToken) -> Option<TimerKey> {
        match self {
            TimerKind::Run(crate::runs::RunTimer::EventLogSweep { .. }) => {
                Some(TimerKey::EventLogSweep)
            }
            TimerKind::ChildBarrier { waiter } => Some(TimerKey::ChildBarrier(waiter.clone())),
            TimerKind::ChildSubscription { thread, .. } => {
                Some(TimerKey::ChildSubscription(thread.clone()))
            }
            TimerKind::ChildRetry { thread, .. } => Some(TimerKey::ChildRetry(thread.clone())),
            TimerKind::Stall { run_id, .. } => Some(TimerKey::Stall(run_id.clone())),
            TimerKind::BudgetVerifyDeadline { token } => Some(TimerKey::BudgetVerifyDeadline(
                token.run_id.clone(),
                token.generation,
            )),
            TimerKind::ConvergeDeadline { token } => Some(TimerKey::ConvergeDeadline(
                token.run_id.clone(),
                token.generation,
            )),
            TimerKind::ConvergeRetry { token, .. } => Some(TimerKey::ConvergeRetry(
                token.run_id.clone(),
                token.generation,
            )),
            TimerKind::ChildResultsDeadline { read_id, .. } => {
                Some(TimerKey::ChildResultsDeadline(*read_id))
            }
            TimerKind::ReconcileVerifyDeadline { op_id, .. } => {
                Some(TimerKey::ReconcileVerifyDeadline(*op_id))
            }
            TimerKind::Stop(tag, kind) => {
                Some(TimerKey::Stop(tag.id.clone(), tag.generation, kind.clone()))
            }
            TimerKind::StatusReadDeadline { read_id } => Some(TimerKey::StatusRead(*read_id)),
            TimerKind::LookupDeadline { op_id, .. } => Some(TimerKey::LookupDeadline(*op_id)),
            TimerKind::DiscoveryDeadline { op_id, .. } => Some(TimerKey::DiscoveryDeadline(*op_id)),
            TimerKind::TreeStopWait {
                tree_op, waiter, ..
            } => Some(TimerKey::TreeStopWait {
                tree_op: *tree_op,
                waiter: *waiter,
            }),
            TimerKind::TreeStopVerify { tree_op, .. } => Some(TimerKey::TreeStopVerify(*tree_op)),
            TimerKind::TombstoneExpire { conn_epoch, .. } => {
                Some(TimerKey::TombstoneExpire(*conn_epoch))
            }
            TimerKind::Inbox(crate::inbox::InboxTimer::Expiry { id, .. }) => {
                Some(TimerKey::InboxItem(id.clone()))
            }
            TimerKind::Inbox(crate::inbox::InboxTimer::WaitTimeout { waiter }) => {
                Some(TimerKey::InboxWaiter(*waiter))
            }
            TimerKind::Reclaim { .. } => Some(TimerKey::Reclaim(token.thread_id.clone())),
            TimerKind::DetachDeadline { .. } => {
                Some(TimerKey::DetachDeadline(token.thread_id.clone()))
            }
            TimerKind::JoinedCheckRetry { .. } => {
                Some(TimerKey::JoinedCheck(token.thread_id.clone()))
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    StallTimeout,
    UpstreamAccessError,
    ClientDisconnected,
    Stop,
    /// 自动续跑空转刹车（S1-评审第 26 节，私有模式）。
    GoalLoop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    ScheduleBreakerProbe {
        generation: u64,
        delay_ms: u64,
    },
    CancelBreakerProbe {
        generation: u64,
    },
    ForgetLate {
        tag: u64,
    },
    ScheduleAt {
        token: ClaimToken,
        kind: TimerKind,
        deadline: Instant,
    },
    Send {
        conn: ConnId,
        message: Value,
    },
    /// 发给所有已连接的客户端。
    Broadcast {
        message: Value,
    },
    Request(RequestContext),
    Schedule {
        token: ClaimToken,
        kind: TimerKind,
        delay_ms: u64,
    },
    Dismiss {
        id: Value,
    },
    ServerError {
        id: Value,
        error: Value,
    },
    ServerResult {
        id: Value,
        result: Value,
    },
    ScheduleForward {
        forward_id: String,
        generation: u64,
        delay_ms: u64,
    },
    CancelForwardTimer {
        forward_id: String,
        generation: u64,
    },
    /// 投递给唯一写入任务（store 模块），core 不等待写盘结果。
    Store(crate::store::StoreOp),
    /// 带写出确认地回答一个服务端请求（inbox 的提交边界，2.3 节）；结果以 [`Event::AppReplyWritten`] 回来。
    /// `payload` 为 Ok(result) 或 Err(error 对象)。
    AppReplyAcked {
        op: u64,
        id: Value,
        payload: Result<Value, Value>,
    },
    /// 撤回还没写出的带确认回包（B2）；已写出的不受影响，结果仍以 AppReplyWritten 报告。
    CancelAppReply {
        op: u64,
    },
    /// 有界墓碑退役后释放 RPC 层的去重登记，不发送上游消息。
    ForgetServerRequest {
        request_key: String,
    },
    /// 撤销带键的计时（见 [`TimerKey`]）；没有登记时什么都不做。
    Unschedule {
        key: TimerKey,
    },
    Warn(String),
    Debug(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    PrivateInstanceExited {
        instance: crate::store::lifecycle::Instance,
    },
    AppResponsive,
    BreakerProbeTimer {
        generation: u64,
    },
    /// 本地产生的传输失败不证明 app-server 已响应，不恢复熔断器。
    TransportFailed {
        context: RequestContext,
        error: Value,
    },
    TimedOut {
        tag: u64,
        deadline: Instant,
        context: RequestContext,
        error: Value,
    },
    LateDiscarded {
        context: RequestContext,
    },
    /// RPC 迟到登记因容量被挤出，deadline 是该请求原定的迟到截止时刻。
    LateEvicted {
        context: RequestContext,
        deadline: Instant,
    },
    LateReply {
        context: RequestContext,
        result: Result<Value, Value>,
    },
    ClientRequest {
        request: ClientRequest,
        run_id: Option<RunId>,
    },
    Reply {
        context: RequestContext,
        result: Result<Value, Value>,
    },
    Notification {
        method: String,
        params: Option<Value>,
    },
    ServerRequest {
        id: Value,
        forward_id: String,
        method: String,
        params: Option<Value>,
    },
    ForwardResponse {
        conn: ConnId,
        id: Value,
        result: Option<Value>,
        error: Option<Value>,
    },
    ForwardTimeout {
        forward_id: String,
        generation: u64,
    },
    Timer {
        token: ClaimToken,
        kind: TimerKind,
    },
    Connected {
        conn: ConnId,
    },
    Disconnected {
        conn: ConnId,
    },
    Shutdown,
    /// 关闭第 1 步（仅共享服务器，第 2、3 步之前）：不再开工新的流式请求和续跑，其余照常处理，
    /// settling 继续定案（详细设计第 5 节）。私有服务器直接走 [`Event::Shutdown`]。
    ShutdownBegin,
    /// 关闭第 3 步（仅共享服务器）：对自有、非 joined、非 settling 的认领暂停 goal 并中断。
    ShutdownStopOwn,
    /// 连接完成 initialize；`cxb` 表示声明了 `capabilities.cxb`（同一连接以最后一次为准，3.6 节）。
    Initialized {
        conn: ConnId,
        cxb: bool,
    },
    /// 带确认的回包写出、写失败或被撤回（[`Action::AppReplyAcked`]）。
    AppReplyWritten {
        op: u64,
        outcome: crate::inbox::ReplyOutcome,
    },
    /// 启动时读出的旧数据，core 在处理任何其他输入之前投入（7.2 节）。
    Restore {
        index: crate::runs::ThreadIndexFile,
        runs: Vec<crate::runs::RunRecord>,
        inbox: Option<crate::inbox::InboxFile>,
    },
    /// 关闭第 5 步：等补读的期限已到。
    RunsFinalize,
}

/// core 在每次 step 前填写的墙钟毫秒与随机数；纯逻辑模块不自行读取时钟或随机源。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Clock {
    pub now_ms: u64,
    pub entropy: u64,
    /// 单调毫秒（core 启动起算），供 #26d 托管回收这类不能受墙钟调整影响的判断。
    pub mono_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Machine {
    pub(crate) diagnostic_batch_active: bool,
    pub convergences: converge::Convergences,
    /// H2 线程状态读取层：在途读取、页代号与活动版本号。
    pub status_reads: status_read::StatusReads,
    pub relations: crate::relations::Relations,
    pub trees: crate::stop::tree::Trees,
    pub lifecycle: crate::claim::lifecycle::Lifecycle,
    /// core 按线上编号登记仍可回写线程事实的请求；超时保留到迟到处理退役。
    pub purge_inflight: HashMap<u64, String>,
    pub extensions: ext::Extensions,
    pub stream_slots: HashSet<u64>,
    next_stream_id: u64,
    pub stops: crate::stop::Stops,
    pub runs: crate::runs::Runs,
    pub inbox: crate::inbox::Inbox,
    pub clock: Clock,
    /// 声明了 capabilities.cxb 的连接：扩展字段与 `broker/*` 推送只发给它们（3.6 节）。
    pub cxb_conns: HashSet<ConnId>,
    /// 续跑预检的进度（按线程；乙路）。
    pub continues: resume::Continues,
    /// 挂在运行上的 goal 恢复操作（#2e，按 runId；乙路）。
    pub goal_restores: resume::GoalRestores,
    /// 工作区标识（数据目录名里的哈希），gd_ 编号用（命令面规格 7.1 节）。
    pub workspace_key: String,
    pub requests: crate::requests::Requests,
    pub claims: HashMap<String, Claim>,
    pub records: HashMap<String, ThreadRecord>,
    pub pending_reviews: usize,
    pub app_server_unresponsive: bool,
    review_requests: HashSet<u64>,
    next_review_id: u64,
    pub unclaimed: HashMap<String, Vec<HeldNotification>>,
    pub closing: bool,
    /// 关闭第 1–3 步（共享专用，[`Event::ShutdownBegin`] 起）：拒绝新的流式请求和续跑；`closing` 仍在第 4 步置位。
    pub quiescing: bool,
    /// 所有 settling 认领的 `held_bytes` 之和（全局预算 [`SETTLING_GLOBAL_BYTES`]）。
    pub settling_bytes: usize,
    /// 每个有缓冲的 settling 认领一项，缓存逐条字节与一次性告警标志；离开 settling 即移除。
    settling_accounting: HashMap<String, settling::HeldAccounting>,
    /// 每个丢失生命周期证据的 joined 认领最多一项，释放或核实完成时清理。
    joined_checks: HashMap<String, settling::JoinedCheck>,
    /// 只保存可能激活 goal 的变更编号；生命周期依附 stops.changes，回包或到期后删除。
    activating_goal_changes: HashSet<u64>,
    /// 关闭第 3 步发出、还没回包的暂停与中断（按线程）。
    pub shutdown_stops: HashMap<String, shutdown::ShutdownStop>,
    /// 第 3 步只进入一次，登记清空后重复事件也不能重发。
    shutdown_stops_started: bool,
    /// 关闭第 5 步封口后不再向账本交付迟到补读。
    pub runs_finalized: bool,
    /// 自动续跑空转刹车，按运行编号；认领不再属于该运行时在每次 step 中删除。
    pub loop_guards: HashMap<RunId, loop_guard::Guard>,
    /// goal 写入回包解包时取出的授予意图，同一次回包处理里由 `guard_goal` 参考、`grant_goal_control` 取走，
    /// 每次 step 末尾清空。
    pub(crate) grant_intent: Option<loop_guard::GrantIntent>,
    pub shared: bool,
    /// 仍连着的客户端；父认领过期时判断 review 子线程的 owner 是否在线。
    pub live_conns: HashSet<ConnId>,
    next_claim_id: u64,
    record_order: VecDeque<String>,
    ended_order: VecDeque<String>,
}

/// 所有输入通过同一个纯函数迁移；运行时只持有返回的新状态。
pub fn step(mut state: Machine, event: Event) -> (Machine, Vec<Action>) {
    state.diagnostic_batch_active = true;
    let mut actions = Vec::new();
    state.child_subscription_event(&event, &mut actions);
    // 先保留身份，事件完全处理后才撤销线上保护，避免回包处理中的释放触发淘汰。
    let finished_start = match &event {
        Event::Reply { context, .. }
        | Event::TransportFailed { context, .. }
        | Event::LateReply { context, .. }
        | Event::LateDiscarded { context }
        | Event::LateEvicted { context, .. } => {
            start_request_run(context).map(|run| (run.to_owned(), None))
        }
        Event::TimedOut { context, tag, .. } => {
            start_request_run(context).map(|run| (run.to_owned(), Some(*tag)))
        }
        _ => None,
    };
    // goal 通知可能早于写回包；请求落定后再推进已弃管根，不能依赖第二条 inactive 通知。
    let settled_goal = match &event {
        Event::Reply { context, .. }
        | Event::LateReply { context, .. }
        | Event::TransportFailed { context, .. }
            if matches!(context.operation, Operation::GoalChange { .. }) =>
        {
            context
                .params
                .as_ref()
                .and_then(|p| p.get("threadId"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        }
        _ => None,
    };
    match event {
        Event::PrivateInstanceExited { instance } => state.lifecycle_instance_exited(&instance),
        Event::AppResponsive => state.observe_app_response(&mut actions),
        Event::BreakerProbeTimer { generation } => {
            state.breaker_probe_timer(generation, &mut actions)
        }
        Event::TransportFailed { context, error } => {
            if matches!(context.operation, Operation::StartReconciliation { .. }) {
                state.subagent_reconcile_transport(
                    &context,
                    crate::sa1b::ReconcileTransportEvent::TransportFailed,
                    &mut actions,
                );
            } else if let Operation::Lifecycle {
                thread,
                operation,
                attempt,
            } = &context.operation
            {
                state.lifecycle_reply(thread, operation, *attempt, Err(error), false, &mut actions);
            } else {
                state.apply_reply(context, Err(error), &mut actions)
            }
        }
        Event::TimedOut {
            tag,
            deadline,
            context,
            error,
        } => {
            if matches!(context.operation, Operation::StartReconciliation { .. }) {
                state.subagent_reconcile_transport(
                    &context,
                    crate::sa1b::ReconcileTransportEvent::TimedOut,
                    &mut actions,
                );
            } else if state.overflowed_start(&context) {
                // 已放弃归属的启动超时不能继续占用认领或转入自有停止对账。
                state.apply_reply(context, Err(error), &mut actions);
                actions.push(Action::ForgetLate { tag });
            } else {
                state.timeout_reply(context, error, tag, deadline, &mut actions);
            }
        }
        Event::LateDiscarded { context } => {
            if matches!(context.operation, Operation::StartReconciliation { .. }) {
                state.subagent_reconcile_transport(
                    &context,
                    crate::sa1b::ReconcileTransportEvent::LateDiscarded,
                    &mut actions,
                );
            } else {
                state.discard_late_context(&context, &mut actions);
            }
        }
        Event::LateEvicted { context, deadline } => {
            if matches!(context.operation, Operation::StartReconciliation { .. }) {
                state.subagent_reconcile_transport(
                    &context,
                    crate::sa1b::ReconcileTransportEvent::LateEvicted,
                    &mut actions,
                );
            } else {
                state.evict_late_context(&context, deadline, &mut actions);
            }
        }
        Event::LateReply { context, result } => {
            state.observe_app_response(&mut actions);
            if matches!(context.operation, Operation::StartReconciliation { .. }) {
                state.subagent_reconcile_reply(&context, result, &mut actions);
            } else {
                state.late_reply(context, result, &mut actions);
            }
        }
        Event::ClientRequest { request, run_id } => {
            state.client_request(request, run_id, &mut actions)
        }
        Event::Reply { context, result } => {
            state.observe_app_response(&mut actions);
            state.apply_reply(context, result, &mut actions);
        }
        Event::Notification { method, params } => state.arrive_notification(
            HeldNotification {
                method,
                params,
                committed: false,
            },
            &mut actions,
        ),
        Event::ServerRequest {
            id,
            forward_id,
            method,
            params,
        } => state.server_request(
            HeldRequest {
                id,
                forward_id,
                method,
                params,
            },
            false,
            &mut actions,
        ),
        Event::ForwardResponse {
            conn,
            id,
            result,
            error,
        } => state.forward_response(conn, id, result, error, &mut actions),
        Event::ForwardTimeout {
            forward_id,
            generation,
        } => state.forward_timeout(&forward_id, generation, &mut actions),
        Event::Timer { token, kind } => state.timer(token, kind, &mut actions),
        Event::Connected { conn } => {
            state.live_conns.insert(conn);
        }
        Event::Disconnected { conn } => state.disconnect(conn, &mut actions),
        Event::Shutdown => state.shutdown(&mut actions),
        Event::ShutdownBegin => state.quiesce(&mut actions),
        Event::ShutdownStopOwn => state.begin_shutdown_stops(&mut actions),
        Event::Initialized { conn, cxb } => {
            if cxb {
                state.cxb_conns.insert(conn);
            } else {
                state.cxb_conns.remove(&conn);
            }
        }
        Event::AppReplyWritten { op, outcome } => {
            state.child_reply_written(op, &outcome, &mut actions);
            state.inbox_reply_written(op, outcome, &mut actions)
        }
        Event::Restore { index, runs, inbox } => {
            let previous: Vec<_> = runs
                .iter()
                .filter(|r| r.subagent_join.is_some())
                .map(|r| (r.run_id.clone(), r.interrupt_reason))
                .collect();
            state.restore_runs(index, runs, &mut actions);
            state.convergence_restored(&previous, &mut actions);
            state.inbox_restore(inbox, &mut actions);
        }
        Event::RunsFinalize => {
            if !state.runs_finalized {
                state.finalize_runs(&mut actions);
                state.runs_finalized = true;
            }
        }
    }
    if let Some(thread) = settled_goal {
        state.abandoned_root_goal_settled(&thread, &mut actions);
    }
    // 空转刹车在停止驱动之前结算，触发的停止当场发出请求。
    state.drive_convergence_safeguards(&mut actions);
    state.drive_loop_guards(&mut actions);
    state.drive_child_results(&mut actions);
    state.drive_subagent_reconcile(&mut actions);
    for run in state.take_reconcile_changes() {
        state.convergence_changed(&run, &mut actions);
    }
    state.drive_convergences(&mut actions);
    // stop 的取消屏障与超时对账可能直接结束 settling；在同一事件内清掉旧缓冲，
    // 再驱动停止后继。stop 模块保持 M4 的接口与行为。
    state.clear_cancelled_settling(&mut actions);
    state.track_goal_requests(&mut actions);
    let tracked = actions.len();
    state.drive_stops(&mut actions);
    state.drive_late_goals(&mut actions);
    state.clear_cancelled_settling(&mut actions);
    state.track_goal_requests(&mut actions[tracked..]);
    state.track_activating_goal_changes(&actions);
    state.schedule_next_verifications(&mut actions);
    state.ensure_breaker_probe(&mut actions);
    state.track_review_requests(&mut actions);
    state.child_subscriptions_after_step(&mut actions);
    state.flush_run_diagnostics(&mut actions);
    state.flush_run_notices(&mut actions);
    state.evict_records();
    state.prune_ext_meta();
    state.prune_reclaims(&mut actions);
    state.lifecycle_drive(&mut actions);
    state.prune_goal_versions();
    state.prune_contexts();
    state.convergence_prune();
    if let Some((run, timeout_tag)) = finished_start {
        if timeout_tag.is_none_or(|tag| actions.iter().any(|action| matches!(action, Action::ForgetLate { tag: forgotten } if *forgotten == tag))) {
            state.runs.inflight_starts.remove(&run);
        }
    }
    for action in &mut actions {
        if let Action::Request(context) = action {
            // 上游只有 thread/start 支持此开关；保留 client 原始参数以区分主动订阅。
            if context.method == "thread/start" {
                if let Some(params) = context
                    .params
                    .get_or_insert_with(|| json!({}))
                    .as_object_mut()
                {
                    params.insert("experimentalRawEvents".into(), json!(true));
                }
            }
            if let Some(run) = start_request_run(context) {
                state.runs.inflight_starts.insert(run.to_owned());
            }
        }
    }
    state.runs.late_starts = state
        .stops
        .late
        .iter()
        .chain(state.stops.reconciliations.values())
        .filter_map(|registration| registration.run_id.clone())
        .collect();
    if state.closing {
        state.runs.inflight_starts.clear();
        state.runs.late_starts.clear();
    }
    // 资格代号只服务于产生它的那一次回包处理。
    state.grant_intent = None;
    #[cfg(test)]
    assert_eq!(
        state.settling_bytes,
        state
            .claims
            .values()
            .filter(|c| c.settling())
            .map(|c| c.held_bytes)
            .sum::<usize>(),
        "settling 字节必须守恒"
    );
    #[cfg(test)]
    state.assert_held_accounting();
    state.diagnostic_batch_active = false;
    (state, actions)
}

fn start_request_run(context: &RequestContext) -> Option<&str> {
    match &context.operation {
        Operation::Stream { run_id } if context.method == "turn/start" => run_id.as_deref(),
        _ => None,
    }
}

impl Machine {
    pub fn new(shared: bool) -> Self {
        Self {
            shared,
            ..Self::default()
        }
    }

    pub(crate) fn record(&mut self, thread_id: &str) -> &mut ThreadRecord {
        self.record_order.retain(|id| id != thread_id);
        self.record_order.push_back(thread_id.to_owned());
        self.records.entry(thread_id.to_owned()).or_default()
    }
    fn allocate_claim_id(&mut self) -> u64 {
        self.next_claim_id = self.next_claim_id.checked_add(1).expect("认领代号已耗尽");
        self.next_claim_id
    }
    pub(crate) fn token(&self, thread_id: &str) -> Option<ClaimToken> {
        self.claims.get(thread_id).map(|c| ClaimToken {
            thread_id: thread_id.into(),
            claim_id: c.claim_id,
        })
    }
    pub(crate) fn current(&self, token: &ClaimToken) -> bool {
        self.claims
            .get(&token.thread_id)
            .is_some_and(|c| c.claim_id == token.claim_id)
    }

    fn reserve_stream_slot(&mut self) -> u64 {
        self.next_stream_id = self
            .next_stream_id
            .checked_add(1)
            .expect("开工名额标识已耗尽");
        self.stream_slots.insert(self.next_stream_id);
        self.next_stream_id
    }

    pub(crate) fn release_stream_slot(&mut self, id: Option<u64>) {
        if let Some(id) = id {
            self.stream_slots.remove(&id);
        }
    }

    /// 自有轮次只保留未结束者及最近十六个已结束者；停止证据由 StopOp 独立增量保存。
    pub(crate) fn remember_owned_turn(&mut self, thread: &str, turn: &str) {
        let ended = self
            .records
            .get(thread)
            .is_some_and(|record| record.ended_turns.iter().any(|id| id == turn));
        let Some(claim) = self.claims.get_mut(thread) else {
            return;
        };
        if claim.joined() || claim.settling() || turn.is_empty() {
            return;
        }
        claim.owned_turns.insert(turn.into());
        let claim_id = claim.claim_id;
        self.sync_stop_owned_turn(thread, claim_id, turn, ended);
        if ended {
            self.mark_owned_ended(thread, turn);
        }
    }

    fn mark_owned_ended(&mut self, thread: &str, turn: &str) {
        let Some(claim) = self.claims.get_mut(thread) else {
            return;
        };
        if claim.joined() || claim.settling() || !claim.owned_turns.contains(turn) {
            return;
        }
        let claim_id = claim.claim_id;
        if !claim.owned_ended.iter().any(|id| id == turn) {
            claim.owned_ended.push_back(turn.into());
        }
        while claim.owned_ended.len() > ENDED_PER_THREAD {
            if let Some(oldest) = claim.owned_ended.pop_front() {
                claim.owned_turns.remove(&oldest);
            }
        }
        self.sync_stop_owned_turn(thread, claim_id, turn, true);
    }

    /// #1–#3、#23、#25：请求先占住线程，再决定是否等待恢复订阅。
    fn client_request(
        &mut self,
        request: ClientRequest,
        run_id: Option<RunId>,
        actions: &mut Vec<Action>,
    ) {
        if self.subagent_work_gate(&request, actions)
            || self.subagent_lifecycle_gate(&request, actions)
        {
            return;
        }
        let thread_id = thread_id(&request.params).map(str::to_owned);
        match request.method.as_str() {
            "broker/threads/archive" | "thread/archive" => {
                self.lifecycle_request(
                    request,
                    crate::store::lifecycle::Kind::Archive,
                    false,
                    actions,
                );
                return;
            }
            "thread/delete" => {
                self.lifecycle_request(
                    request,
                    crate::store::lifecycle::Kind::Delete,
                    false,
                    actions,
                );
                return;
            }
            "broker/threads/purge"
                if request
                    .params
                    .as_ref()
                    .is_some_and(|p| p["serverDelete"] == true) =>
            {
                self.lifecycle_request(
                    request,
                    crate::store::lifecycle::Kind::Delete,
                    true,
                    actions,
                );
                return;
            }
            _ => {}
        }
        if !matches!(
            request.method.as_str(),
            "broker/threads/watch"
                | "broker/threads/list"
                | "broker/runs/convergenceDiagnostics"
                | "broker/runs/modelSummaries"
                | "thread/read"
                | "thread/turns/list"
                | "thread/items/list"
                | "thread/goal/get"
                | "broker/inbox/list"
                | "broker/inbox/wait"
                | "broker/health"
                | "broker/inbox/resolve"
                | "broker/threads/stop"
        ) && self.lifecycle_gate(&request, thread_id.as_deref().unwrap_or(""), actions)
        {
            return;
        }
        // 删除清单已锁定的线程不能在磁盘操作期间重新认领或改写索引。
        if thread_id
            .as_deref()
            .is_some_and(|thread| self.run_purge_busy(thread))
            && !matches!(
                request.method.as_str(),
                "broker/threads/watch"
                    | "broker/threads/purge"
                    | "thread/read"
                    | "thread/turns/list"
                    | "thread/items/list"
                    | "thread/goal/get"
                    | "broker/inbox/list"
                    | "broker/inbox/wait"
                    | "broker/threads/list"
                    | "broker/runs/convergenceDiagnostics"
                    | "broker/runs/modelSummaries"
                    | "broker/health"
            )
        {
            actions.push(Action::Send {
                conn: request.conn,
                message: proto::response_err(
                    request.id.clone(),
                    proto::error_object(
                        -32001,
                        "线程正在删除本地记录",
                        Some(json!({"reason":"purgePending"})),
                    ),
                ),
            });
            return;
        }
        if (self.closing || self.quiescing)
            && request.method == "thread/goal/set"
            && shutdown::goal_set_can_activate(&request.params)
        {
            send_error(&request, code::GENERAL_ERROR, SHUTTING_DOWN, actions);
            return;
        }
        match request.method.as_str() {
            "broker/threads/resumeOverrides"
            | "broker/threads/purge"
            | "broker/threads/annotate"
            | "broker/threads/detach"
            | "broker/threads/watch"
            | "broker/threads/list"
            | "broker/runs/convergenceDiagnostics"
            | "broker/runs/modelSummaries" => {
                self.ext_request(request, actions);
                return;
            }
            "broker/inbox/list" | "broker/inbox/resolve" | "broker/inbox/wait" => {
                self.inbox_request(request, actions);
                return;
            }
            // #2c：带 cxbContinue 的 turn/start 整条交给续跑处理（含关闭中、线程忙、参数不合法）。
            "turn/start" if resume::continue_field(&request.params).is_some() => {
                self.continue_request(request, run_id, actions);
                return;
            }
            _ => {}
        }
        if request.method == "broker/threads/stop" {
            self.client_stop(request, actions);
            return;
        }
        if request.method == "broker/joined" {
            self.client_joined(request, actions);
            return;
        }
        if ReqKind::from_method(&request.method).is_some() {
            if self.closing || self.quiescing {
                send_error(&request, code::GENERAL_ERROR, SHUTTING_DOWN, actions);
                return;
            }
            if self.reject_stream_admission(&request, actions) {
                return;
            }
        }
        if request.method == "turn/interrupt" && self.reject_interrupt(&request) {
            send_error(&request, code::GENERAL_ERROR,
                "The turn running on this thread belongs to another client of the shared app-server; it was not interrupted.", actions);
            return;
        }
        if let (Some(req), Some(thread_id)) =
            (ReqKind::from_method(&request.method), thread_id.clone())
        {
            if self.closing || self.quiescing {
                send_error(&request, code::GENERAL_ERROR, SHUTTING_DOWN, actions);
                return;
            }
            if self.claims.contains_key(&thread_id) || self.stop_busy(&thread_id) {
                send_error(
                    &request,
                    code::THREAD_BUSY,
                    "A turn is already running on this thread.",
                    actions,
                );
                return;
            }
            let stream_id = self.reserve_stream_slot();
            let claim_id = self.allocate_claim_id();
            let subscribed = self.record(&thread_id).subscribed;
            self.record(&thread_id).last_claim_id = claim_id;
            let submitted_input = input_texts(&request.params);
            let open = run_id.as_ref().map(|run| RunOpen {
                thread_id: thread_id.clone(),
                run_id: run.clone(),
                conn: Some(request.conn),
                kind: req,
                joined: req == ReqKind::TurnSteer,
                input: submitted_input.join("\n"),
                continued_from: None,
            });
            self.claims.insert(
                thread_id.clone(),
                Claim {
                    stream_id: Some(stream_id),
                    owned_ended: Default::default(),
                    owned_turns: Default::default(),
                    claim_id,
                    state: State::Starting {
                        owner: Owner::Client(request.conn),
                        req,
                        turn_id: None,
                        cont: None,
                        settling: self.shared,
                        undecided: None,
                        submitted_at: 0,
                        held: Vec::new(),
                    },
                    run_id,
                    resume_goal: false,
                    request_pending: true,
                    owner_gone: false,
                    pending_start: Some(request),
                    submitted_input,
                    held_requests: Vec::new(),
                    held_deltas: 0,
                    held_bytes: 0,
                    held_dropped_items: 0,
                    overflowed: false,
                    settlement_reply: None,
                    pending_detach: None,
                },
            );
            self.begin_child_run_barrier(&thread_id, actions);
            if let Some(open) = open {
                let run = open.run_id.clone();
                self.run_opened(open, actions);
                if let Some(request) = self.claims[&thread_id].pending_start.as_ref() {
                    self.send_run_started(&thread_id, &run, request, actions);
                }
            }
            if subscribed {
                self.submit_stream(&thread_id, actions);
            } else {
                actions.push(Action::Request(RequestContext {
                    resume_retried: false,
                    stream_id: None,
                    review_id: None,
                    client: None,
                    token: self.token(&thread_id),
                    operation: Operation::ResumeBarrier,
                    method: "thread/resume".into(),
                    params: Some(self.resume_params(&thread_id, json!({ "threadId": thread_id }))),
                }));
            }
            return;
        }
        // thread/goal/clear 只在回包成功后学习（learn_traffic，上游 891–904 行）：清除失败时 goal 仍是 active。
        if request.method == "thread/unsubscribe" {
            if let Some(thread) = thread_id
                .as_deref()
                .filter(|t| self.thread_relation(t).is_some())
            {
                if !self.child_release_capacity(thread, actions) {
                    send_error(
                        &request,
                        code::GENERAL_ERROR,
                        "子线程释放责任表已满，请稍后重试",
                        actions,
                    );
                    return;
                }
            }
        }
        let stream_id = ReqKind::from_method(&request.method).map(|_| self.reserve_stream_slot());
        actions.push(Action::Request(RequestContext {
            resume_retried: false,
            stream_id,
            review_id: None,
            token: thread_id.as_deref().and_then(|t| self.token(t)),
            method: request.method.clone(),
            params: request.params.clone(),
            operation: Operation::Forward,
            client: Some(request),
        }));
    }

    /// #2 的屏障：只有 resume 回包进入邮箱后才取出流式请求。
    pub(crate) fn submit_stream(&mut self, thread_id: &str, actions: &mut Vec<Action>) {
        if self.child_start_waiting(thread_id) {
            return;
        }
        if let Some(value) = self
            .claims
            .get(thread_id)
            .and_then(|c| c.pending_start.as_ref())
            .and_then(|r| r.params.as_ref())
            .and_then(|p| p.get("cxbRunPolicy"))
            .cloned()
        {
            let policy = serde_json::from_value::<crate::runs::RunPolicy>(value)
                .ok()
                .filter(|p| p.stall_timeout_ms <= 9_007_199_254_740_991);
            let Some(policy) = policy else {
                if let Some(request) = self.claims.get_mut(thread_id).and_then(|c| {
                    c.request_pending = false;
                    c.pending_start.take()
                }) {
                    send_error(
                        &request,
                        -32602,
                        "cxbRunPolicy 必须是有效的运行策略",
                        actions,
                    );
                }
                self.release_with_kind(thread_id, ReleaseKind::StartFailed, actions);
                return;
            };
            if let Some(run) = self.claims.get(thread_id).and_then(|c| c.run_id.clone()) {
                if let Some(record) = self.runs.runs.get_mut(&run) {
                    record.run_policy = Some(policy);
                }
                self.runs.put(&run, actions);
                self.stall_arm(&run, actions);
            }
            if let Some(params) = self
                .claims
                .get_mut(thread_id)
                .and_then(|c| c.pending_start.as_mut())
                .and_then(|r| r.params.as_mut())
                .and_then(Value::as_object_mut)
            {
                params.remove("cxbRunPolicy");
            }
        }
        // 独立收敛配置只在 broker 内保存，不能作为未知扩展参数发给上游。
        if let Some(value) = self
            .claims
            .get(thread_id)
            .and_then(|c| c.pending_start.as_ref())
            .and_then(|r| r.params.as_ref())
            .and_then(|p| p.get("cxbConvergence"))
            .cloned()
        {
            let config = serde_json::from_value::<crate::sa1b::ConvergenceConfig>(value)
                .ok()
                .filter(|c| {
                    c.timeout_ms > 0
                        && c.discovery_timeout_ms > 0
                        && c.discovery_max_interval_ms > 0
                        && c.discovery_backoff_ms.iter().all(|v| *v > 0)
                        && c.result_limits.read_timeout_ms > 0
                        && c.result_limits.max_concurrent_reads > 0
                });
            let Some(config) = config else {
                if let Some(request) = self.claims.get_mut(thread_id).and_then(|c| {
                    c.request_pending = false;
                    c.pending_start.take()
                }) {
                    send_error(
                        &request,
                        -32602,
                        "cxbConvergence 必须是有效的独立收敛配置",
                        actions,
                    );
                }
                self.release_with_kind(thread_id, ReleaseKind::StartFailed, actions);
                return;
            };
            if let Some(run) = self.claims.get(thread_id).and_then(|c| c.run_id.clone()) {
                if let Some(record) = self.runs.runs.get_mut(&run) {
                    record.convergence_config = Some(config);
                }
                self.runs.put(&run, actions);
            }
            if let Some(params) = self
                .claims
                .get_mut(thread_id)
                .and_then(|c| c.pending_start.as_mut())
                .and_then(|r| r.params.as_mut())
                .and_then(Value::as_object_mut)
            {
                params.remove("cxbConvergence");
            }
        }

        if let Some(error) = self.lifecycle_error(thread_id) {
            if let Some(request) = self
                .claims
                .get_mut(thread_id)
                .and_then(|c| c.pending_start.take())
            {
                actions.push(Action::Send {
                    conn: request.conn,
                    message: proto::response_err(request.id, error),
                });
                self.release_with_kind(thread_id, ReleaseKind::StartFailed, actions);
            }
            return;
        }
        let Some(claim) = self.claims.get_mut(thread_id) else {
            return;
        };
        let Some(request) = claim.pending_start.take() else {
            return;
        };
        if self.closing || self.quiescing {
            claim.request_pending = false;
            send_error(&request, code::GENERAL_ERROR, SHUTTING_DOWN, actions);
            self.release_with_kind(thread_id, ReleaseKind::Shutdown, actions);
            return;
        }
        if claim.owner_gone {
            // #20：owner 在屏障解除前已断开，流式请求还没发出就取消，不为没人接收的运行开工；
            // 原连接已不在，不回包。
            actions.push(Action::Debug(format!(
                "线程 {thread_id} 的 owner 在恢复订阅期间断开，取消尚未发出的 {}",
                request.method
            )));
            claim.request_pending = false;
            if let Some(id) = claim.stream_id.take() {
                self.stream_slots.remove(&id);
            }
            return;
        }
        if let State::Starting {
            held, submitted_at, ..
        } = &mut claim.state
        {
            *submitted_at = held.len();
        }
        let stream_id = claim.stream_id;
        let claim_id = claim.claim_id;
        let run_id = claim.run_id.clone();
        let mut request = request;
        if self.convergence_enabled() && request.method == "turn/start" {
            if let Some(run) = &run_id {
                // 初始请求也登记输入身份；只用于被动识别首轮插话，不授予 SA1b 或共享控制权。
                let guard = self
                    .loop_guards
                    .entry(run.clone())
                    .or_insert_with(|| loop_guard::Guard::new(thread_id));
                if let Some(params) = request.params.as_mut().and_then(Value::as_object_mut) {
                    if params.get("clientUserMessageId").is_none_or(Value::is_null) {
                        params.insert(
                            "clientUserMessageId".into(),
                            json!(format!(
                                "cxb-initial-{}-{}-{}",
                                self.runs.epoch, self.clock.entropy, claim_id
                            )),
                        );
                    }
                    guard.initial_client_id = params["clientUserMessageId"]
                        .as_str()
                        .filter(|s| !s.is_empty())
                        .map(str::to_owned);
                }
            }
        }
        if let (Some(run), Some(params)) = (&run_id, &request.params) {
            self.run_save_execution_config(run, params, actions);
        }
        actions.push(Action::Request(RequestContext {
            resume_retried: false,
            stream_id,
            review_id: None,
            client: Some(request.clone()),
            token: Some(ClaimToken {
                thread_id: thread_id.into(),
                claim_id,
            }),
            operation: Operation::Stream { run_id },
            method: request.method,
            params: request.params,
        }));
    }

    /// 正常和 RPC 超时后的迟到回包共用；只取发请求时保存的运行身份。
    pub(crate) fn record_start_reply(
        &mut self,
        context: &RequestContext,
        result: &Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        if context.method != "turn/start" {
            return;
        }
        if let Operation::Stream { run_id: Some(run) } = &context.operation {
            if let (Some(thread), Some(turn)) = (
                thread_id(&context.params),
                result.as_ref().ok().and_then(response_turn_id),
            ) {
                self.run_start_evidence(run, thread, turn, actions);
            }
        }
    }

    /// #7–#11、#24、#27：回包交付独立于认领；事实学习独立于运行阶段。
    pub(crate) fn apply_reply(
        &mut self,
        mut context: RequestContext,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        if let Operation::Lifecycle {
            thread,
            operation,
            attempt,
        } = &context.operation
        {
            self.lifecycle_reply(thread, operation, *attempt, result, true, actions);
            return;
        }
        self.release_stream_slot(context.stream_id);
        if self.closing {
            if let Operation::ResultRead(read) = &context.operation {
                if !self.runs_finalized {
                    self.run_result_reply(read.clone(), result, actions);
                }
            } else {
                self.deliver(&context, result, actions);
            }
            return;
        }
        if self.closing && context.method.starts_with("thread/goal/") {
            // 关闭只作废内部目标操作；客户端的原请求仍按原结果结算，不再学习事实。
            self.deliver(&context, result, actions);
            return;
        }
        if self.retry_resume_unknown_key(&context, &result, actions) {
            return;
        }
        if self.stale_shutdown_reply(&context) {
            actions.push(Action::Debug("忽略关闭第 3 步的重复或过期回包".into()));
            return;
        }
        if self.unwrap_goal_reply(&mut context, &result, false, actions) {
            return;
        }
        self.stop_diag_rpc_reply(&context, &result, false);
        if let Operation::BudgetVerify { verification } = &context.operation {
            self.convergence_budget_reply(verification, result, actions);
            return;
        }
        // 收敛发现回包先校验全身份，再由发现层学习；旧代际不得进入公共 learn_traffic。
        if let Operation::Discovery { op_id, generation } = context.operation {
            if let Some(crate::relations::DiscoveryCaller::Converge { token, .. }) = self
                .relations
                .discoveries
                .get(&op_id)
                .map(|o| o.caller.clone())
            {
                if self.convergence_token_current(&token) && !self.closing && !self.quiescing {
                    self.relation_reply(&context, result, actions);
                }
                let _ = generation;
                return;
            }
        }
        // 清理已撤销或代号已换代的内部回包在事实学习之前隔离，不能污染后续运行。
        let stale = match &context.operation {
            Operation::DerivedStop {
                tree_op,
                generation,
                thread,
                round,
            } => {
                !self
                    .trees
                    .operations
                    .get(tree_op)
                    .is_some_and(|t| !t.cancelled && t.generation == *generation)
                    || !self
                        .trees
                        .aux
                        .derived
                        .get(&(*tree_op, thread.clone()))
                        .is_some_and(|d| d.request_round == *round)
            }
            Operation::StatusRead {
                read_id,
                generation,
            } => !self
                .status_reads
                .reads
                .get(read_id)
                .is_some_and(|r| r.generation == *generation),
            Operation::Discovery { op_id, .. } => !self.relations.discoveries.contains_key(op_id),
            _ => false,
        };
        if stale {
            if let Operation::StatusRead {
                read_id,
                generation,
            } = context.operation
            {
                // 读取层仍须结算已到总期限的 RPC 证据；只跳过公共事实学习。
                self.status_read_reply(read_id, generation, result, actions);
            }
            return;
        }
        // 乙路：续跑核实读与 goal 恢复先核对自己持有的版本、认领与阶段，再由处理函数自己调用
        // `learn_traffic` 学习事实（回包自身会推进 goal 版本，先学习会把本次读数误判为过期）。
        match &context.operation {
            Operation::ContinueRead(_) => {
                self.continue_reply(&context, result, actions);
                return;
            }
            Operation::GoalRestore(step) => {
                let step = step.clone();
                self.goal_restore_reply(&context, step, result, actions);
                return;
            }
            _ => {}
        }
        match &context.operation {
            Operation::StartReconciliation { .. } => {
                self.subagent_reconcile_reply(&context, result, actions);
                return;
            }
            Operation::ChildResultRead { .. } => {
                self.child_result_reply(&context, result, actions);
                return;
            }
            _ => {}
        }
        let current = context.token.as_ref().is_none_or(|t| self.current(t));
        // 空转刹车的暂停回包先核对版本，由处理函数自己决定是否学习（旧回包不得覆盖新事实）。
        if !matches!(
            context.operation,
            Operation::Stop(..) | Operation::LateGoal { .. } | Operation::LoopGuardPause(_)
        ) && !matches!(&context.operation, Operation::Inbox(rpc) if !self.guardian_resume_can_learn(rpc))
        {
            self.learn_traffic(&context, &result, actions);
        }
        match &context.operation {
            Operation::StatusRead {
                read_id,
                generation,
            } => self.status_read_reply(*read_id, *generation, result, actions),
            Operation::RelationLookup { .. }
            | Operation::Discovery { .. }
            | Operation::AttachThread { .. }
            | Operation::ChildSubscription { .. } => self.relation_reply(&context, result, actions),
            Operation::DerivedStop { .. } => self.tree_reply(&context, result, actions),
            Operation::BudgetVerify { .. }
            | Operation::StartReconciliation { .. }
            | Operation::ChildResultRead { .. } => {
                actions.push(Action::Warn("SA1b 回包重复分派，已忽略".into()));
            }
            Operation::BreakerProbe { generation } => {
                self.breaker_probe_reply(*generation, actions)
            }
            Operation::Lifecycle { .. } => unreachable!("生命周期回包已分派"),
            Operation::ResultRead(read) => self.run_result_reply(read.clone(), result, actions),
            Operation::ContinueResume => self.continue_reply(&context, result, actions),
            Operation::ContinueRead(_) | Operation::GoalRestore(_) => {
                unreachable!("乙路的读回包已在学习事实之前分派")
            }
            Operation::Inbox(rpc) => self.inbox_rpc_reply(rpc.clone(), result, actions),
            Operation::ShutdownStop(phase) => {
                self.shutdown_stop_reply(&context, *phase, result, actions)
            }
            Operation::LoopGuardPause(target) => {
                self.loop_guard_pause_reply(target.clone(), &context, &result, actions)
            }
            Operation::Stop(tag, request) => {
                self.stop_reply(tag.clone(), request.clone(), result, actions)
            }
            Operation::LateInterrupt { generation } => {
                self.late_interrupt_reply(*generation, result, actions)
            }
            Operation::LateGoal {
                generation,
                round,
                version,
                write,
            } => self.late_goal_reply(*generation, *round, *version, *write, result, actions),
            Operation::GoalChange { .. } | Operation::GoalRead { .. } => {
                unreachable!("goal 包装已解开")
            }
            Operation::ResumeBarrier => {
                if let Some(token) = &context.token {
                    if current {
                        if result.is_err() {
                            actions.push(Action::Warn(format!(
                                "线程 {} 恢复订阅失败，继续转发流式请求",
                                token.thread_id
                            )));
                        }
                        self.submit_stream(&token.thread_id, actions);
                    } else {
                        actions.push(Action::Warn("丢弃过期认领的订阅屏障回包".into()));
                        self.schedule_unsubscribe(token.clone(), actions);
                    }
                }
            }
            Operation::Stream { run_id } => {
                self.record_start_reply(&context, &result, actions);
                let review_child = result
                    .as_ref()
                    .ok()
                    .filter(|_| context.method == "review/start")
                    .and_then(|v| v.get("reviewThreadId"))
                    .and_then(Value::as_str)
                    .filter(|child| {
                        context
                            .token
                            .as_ref()
                            .is_some_and(|token| token.thread_id != *child)
                    })
                    .map(str::to_owned);
                if let (Some(child), Ok(value)) = (review_child, &result) {
                    self.review_reply(
                        &context,
                        run_id.clone(),
                        current,
                        &child,
                        response_turn_id(value),
                        actions,
                    );
                    self.deliver(&context, result.clone(), actions);
                } else if current {
                    self.stream_reply(context.clone(), result.clone(), actions);
                } else {
                    actions.push(Action::Warn("丢弃过期认领的启动状态更新".into()));
                    self.deliver(&context, result.clone(), actions);
                }
                if context.method == "review/start" {
                    self.finish_review(context.review_id);
                }
            }
            Operation::InterruptRetry { original_error } => self.deliver(
                &context,
                result.map_err(|_| original_error.clone()),
                actions,
            ),
            Operation::Unsubscribe => {
                if result.is_err() {
                    actions.push(Action::Warn(format!("{} 请求失败", context.method)));
                }
            }
            Operation::Forward => {
                if context.method == "turn/interrupt" && current {
                    if let Err(error) = &result {
                        if self.retarget_interrupt(&context, error, actions) {
                            return;
                        }
                    }
                }
                self.deliver(&context, result, actions);
                if context.method == "review/start" {
                    self.finish_review(context.review_id);
                }
            }
        }
    }

    fn stream_reply(
        &mut self,
        context: RequestContext,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        let Some(token) = &context.token else {
            self.deliver(&context, result, actions);
            return;
        };
        let thread_id = token.thread_id.clone();
        if self
            .claims
            .get(&thread_id)
            .is_some_and(|c| matches!(c.state, State::Idle) || !c.request_pending)
        {
            actions.push(Action::Warn("启动回包与当前认领阶段不匹配".into()));
            self.deliver(&context, result, actions);
            return;
        }
        // 认领仍在表中时发诊断，保证启动失败的 owner 不因删除认领而漏收通知。
        if let Err(error) = &result {
            if let Some(run) = self
                .claims
                .get(&thread_id)
                .filter(|claim| matches!(claim.state, State::Starting { turn_id: None, .. }))
                .and_then(|claim| claim.run_id.clone())
            {
                self.run_rpc_error(&run, &thread_id, error, actions);
            }
        }
        let Some(mut claim) = self.claims.remove(&thread_id) else {
            self.deliver(&context, result, actions);
            return;
        };
        claim.request_pending = false;
        match &mut claim.state {
            State::Starting {
                owner,
                req,
                turn_id,
                settling,
                ..
            } => match &result {
                Ok(value) => {
                    let response_id = response_turn_id(value).map(str::to_owned);
                    if *settling && (*req == ReqKind::TurnStart || claim.overflowed) {
                        *turn_id = response_id;
                        claim.settlement_reply = Some((context, value.clone()));
                        self.claims.insert(thread_id.clone(), claim);
                        self.begin_settlement(&thread_id, 0, actions);
                        return;
                    }
                    if turn_id.is_none() {
                        *turn_id = response_id;
                    }
                    if *settling {
                        let joined = *req == ReqKind::TurnSteer;
                        let target = turn_id.clone();
                        self.claims.insert(thread_id.clone(), claim);
                        self.settle(&thread_id, joined, target, actions);
                        self.deliver(&context, result, actions);
                        return;
                    }
                    claim.state = State::Running {
                        owner: owner.clone(),
                        turn_id: turn_id.clone().unwrap_or_default(),
                        joined: *req == ReqKind::TurnSteer,
                    };
                }
                Err(error) => {
                    if turn_id.is_none() {
                        if let Some(run) = &claim.run_id {
                            self.run_start_failed(run, error, actions);
                        }
                    }
                    // 失败的 turn/steer 加入的是别人的 turn：只释放，不中断（上游 1428–1435 行）。
                    if *settling || turn_id.is_none() || *req == ReqKind::TurnSteer {
                        self.claims.insert(thread_id.clone(), claim);
                        self.fail_settlement(&thread_id, actions);
                        self.deliver(&context, result, actions);
                        return;
                    }
                    claim.state = State::Running {
                        owner: owner.clone(),
                        turn_id: turn_id.clone().unwrap_or_default(),
                        joined: false,
                    };
                    claim.owner_gone = true;
                    self.claims.insert(thread_id.clone(), claim);
                    self.begin_stop(&thread_id, StopReason::Stop, false, actions);
                    self.deliver(&context, result, actions);
                    return;
                }
            },
            // #9：完成已进入空窗或下一轮，迟到响应只清 pending。
            State::Running { .. }
            | State::AwaitingContinuation { .. }
            | State::Converging { .. }
            | State::FinishingTree { .. } => {}
            State::Idle => actions.push(Action::Warn("启动回包与认领阶段不匹配".into())),
        }
        let gone = claim.owner_gone;
        let joined = claim.joined();
        let learned_turn = (!joined && !claim.settling())
            .then(|| claim.turn_id().map(str::to_owned))
            .flatten();
        self.claims.insert(thread_id.clone(), claim);
        if let Some(turn) = self.claims[&thread_id]
            .turn_id()
            .filter(|t| !t.is_empty())
            .map(str::to_owned)
        {
            self.record_run_turn_started(&thread_id, &turn, actions);
        }
        if let Some(turn) = learned_turn {
            self.remember_owned_turn(&thread_id, &turn);
        }
        if gone {
            if joined {
                self.release(&thread_id, actions);
            } else {
                self.begin_stop(&thread_id, StopReason::ClientDisconnected, false, actions);
            }
        }
        self.deliver(&context, result, actions);
    }

    /// 3.5：review 回包给出子线程（上游 1516–1521、1549–1589 行）。
    /// - 父认领仍是本请求建立的那个，才动父认领：共享服务器上未溢出的先按自己的定案回放 held
    ///   （inline review 的早到完成在这里记账，可能已释放父认领），再悄悄释放（运行转到子线程，不算结束）。
    /// - 父认领已释放或换代：不碰新的父认领，但子线程照样按自身占用决定是否建立认领，owner 取原请求的连接。
    /// - inline review 已经结束的，不再为它建认领。
    /// - 溢出的 detached review 只转发父缓冲，悄悄释放父认领，在实际子线程保持 joined 并回放早到通知。
    fn review_reply(
        &mut self,
        context: &RequestContext,
        run_id: Option<RunId>,
        current: bool,
        child: &str,
        turn_id: Option<&str>,
        actions: &mut Vec<Action>,
    ) {
        let Some(token) = context.token.clone() else {
            return;
        };
        if self.closing {
            // 关闭清理已释放全部认领（第 5 节第 4 步）：迟到的回包只交还客户端、收尾 review 计数，
            // 不再建立认领、回放生命周期或发起停止。客户端事件可能插在两条 app-server 消息之间，
            // 统一通道挡不住这种交错，只能在这里拦。
            actions.push(Action::Debug(format!(
                "关闭期间收到 review 回包，不再认领子线程 {child}"
            )));
            return;
        }
        let parent = token.thread_id.as_str();
        let joined = current && self.claims.get(parent).is_some_and(|c| c.overflowed);
        let mut lifecycle_incomplete = false;
        let (mut owner, mut gone) = match self.claims.get(parent).filter(|_| current) {
            Some(claim) => (claim.owner().cloned(), claim.owner_gone),
            None => {
                let conn = context.client.as_ref().map(|c| c.conn);
                (
                    conn.map(Owner::Client),
                    !conn.is_some_and(|c| self.live_conns.contains(&c)),
                )
            }
        };
        if current {
            if let Some(claim) = self.claims.get_mut(parent) {
                claim.request_pending = false;
            }
        }
        if current && self.claims.get(parent).is_some_and(Claim::settling) {
            if joined && child == parent {
                // 内联审查的轮次确实在父线程，可直接按 joined 回放；不能重建已完成的认领。
                if let Some(run) = &run_id {
                    self.run_review_moved(
                        run,
                        child,
                        match &owner {
                            Some(Owner::Client(conn)) if !gone => Some(*conn),
                            _ => None,
                        },
                        actions,
                    );
                }
                self.settle(parent, true, turn_id.map(str::to_owned), actions);
                if turn_id.is_none() {
                    self.release_with_kind(parent, ReleaseKind::Joined, actions);
                }
                return;
            } else if joined {
                lifecycle_incomplete = self.abandon_review_parent_buffer(parent, actions);
            } else {
                self.settle(parent, false, None, actions);
            }
            // 定案可能兑现排队交接，子线程必须继承交接后的 owner。
            if let Some(claim) = self
                .claims
                .get(parent)
                .filter(|c| c.claim_id == token.claim_id)
            {
                owner = claim.owner().cloned();
                gone = claim.owner_gone;
            }
        }
        let moving_stop = if current {
            self.take_moving_stop(parent)
        } else {
            None
        };
        let parent_live = current && self.current(&token);
        let ended = child == parent
            && turn_id.is_some_and(|id| {
                self.records
                    .get(child)
                    .is_some_and(|r| r.ended_turns.iter().any(|t| t == id))
            });
        let occupied = self.claims.contains_key(child) && !(parent_live && child == parent);
        let create = owner.is_some() && !ended && !occupied;
        if parent_live && ended && moving_stop.is_some() {
            self.restore_moving_stop(moving_stop, parent, token.claim_id);
            if let Some(turn) = turn_id {
                self.stop_completed_turn(parent, turn, actions);
            }
            return;
        }
        let mut reuse = None;
        if parent_live {
            if create {
                self.cancel_reclaim(parent, actions);
                let claim = self.claims.remove(parent).expect("已核对父认领");
                for request in claim.held_requests {
                    actions.push(Action::Dismiss { id: request.id });
                }
                self.record(parent).last_claim_id = claim.claim_id;
                if child == parent {
                    reuse = Some(claim.claim_id);
                } else {
                    self.schedule_unsubscribe(token.clone(), actions);
                }
            } else {
                self.release(parent, actions);
            }
        }
        if occupied {
            actions.push(Action::Warn(
                "review 子线程已有另一认领，保留其所有权".into(),
            ));
        }
        let Some(owner) = owner.filter(|_| create) else {
            self.preserve_review_collision(moving_stop, child, turn_id);
            return;
        };
        if let Some(run) = &run_id {
            self.run_review_moved(
                run,
                child,
                match &owner {
                    Owner::Client(conn) if !gone => Some(*conn),
                    _ => None,
                },
                actions,
            );
        }
        let claim_id = reuse.unwrap_or_else(|| self.allocate_claim_id());
        self.claims.insert(
            child.into(),
            Claim {
                stream_id: None,
                owned_ended: Default::default(),
                owned_turns: Default::default(),
                claim_id,
                state: State::Running {
                    owner: owner.clone(),
                    turn_id: turn_id.unwrap_or_default().into(),
                    joined,
                },
                run_id,
                resume_goal: false,
                request_pending: false,
                owner_gone: gone,
                pending_start: None,
                submitted_input: Vec::new(),
                held_requests: Vec::new(),
                held_deltas: 0,
                held_bytes: 0,
                held_dropped_items: 0,
                overflowed: false,
                settlement_reply: None,
                pending_detach: None,
            },
        );
        self.record(child).last_claim_id = claim_id;
        if joined {
            if let Some(run) = self.claims[child].run_id.clone() {
                self.run_settled(&run, true, actions);
                self.notify_run_settled(child, &run, turn_id.map(str::to_owned), actions);
            }
        }
        if let Some(turn) = turn_id.filter(|id| !id.is_empty()) {
            self.record_run_turn_started(child, turn, actions);
            if !joined {
                self.remember_owned_turn(child, turn);
            }
        }
        self.restore_moving_stop(moving_stop, child, claim_id);
        self.lifecycle_review_moved(parent, child, actions);
        // 执行线程及停止目标已迁移，C1 此时能读取新线程的有效 token。
        if let Some(run) = self
            .claims
            .get(child)
            .and_then(|claim| claim.run_id.clone())
        {
            if self
                .runs
                .runs
                .get(&run)
                .and_then(|record| record.review_thread_id.as_deref())
                == Some(child)
            {
                self.stall_review_moved(&run, actions);
            }
        }
        self.reclaim_check(child, actions);
        if let Some(events) = self.unclaimed.remove(child) {
            let mut completed = false;
            for event in events {
                // 到达时已经提交，整批输出仍交给原 review owner，完成后统一
                // 释放，避免再次记账或漏掉同批中完成通知之后的输出。
                self.record_run_notification(child, &event, actions);
                // 到达时已发给 watcher（commit_notification），这里只补发给 owner。
                forward_owner(&owner, gone, &event, actions);
                if event.method == "turn/started" {
                    if let Some(id) = notification_turn_id(&event.params) {
                        self.started(child, id, actions);
                        self.run_diagnostic_event(child, id, true, None, true, actions);
                        self.dispatch_run_progress(
                            child,
                            id,
                            &event.method,
                            &event.params,
                            true,
                            actions,
                        );
                    }
                }
                completed |= event.method == "turn/completed";
            }
            if completed {
                if self.stop_busy(child) {
                    self.stop_completed(child, actions);
                } else {
                    self.release(child, actions);
                }
            }
        }
        if gone && self.claims.contains_key(child) {
            if joined {
                self.release_with_kind(child, ReleaseKind::Joined, actions);
            } else {
                self.begin_stop(child, StopReason::ClientDisconnected, false, actions);
            }
        }
        if joined && lifecycle_incomplete {
            self.begin_joined_check(child, actions);
        }
        if joined && turn_id.is_none() {
            self.release_with_kind(child, ReleaseKind::Joined, actions);
        }
    }

    pub(crate) fn finish_timed_out_review(&mut self, review_id: Option<u64>) {
        self.finish_review(review_id);
    }

    fn finish_review(&mut self, review_id: Option<u64>) {
        let Some(id) = review_id else {
            return;
        };
        if !self.review_requests.remove(&id) {
            return;
        }
        self.pending_reviews = self.review_requests.len();
        if self.pending_reviews == 0 {
            self.unclaimed.clear();
        }
    }

    fn track_review_requests(&mut self, actions: &mut [Action]) {
        for action in actions {
            let Action::Request(context) = action else {
                continue;
            };
            if context.method == "review/start"
                && context
                    .params
                    .as_ref()
                    .and_then(|params| params.get("delivery"))
                    .and_then(Value::as_str)
                    == Some("detached")
                && context.client.is_some()
                && context.review_id.is_none()
            {
                self.next_review_id = self
                    .next_review_id
                    .checked_add(1)
                    .expect("审查请求标识已耗尽");
                context.review_id = Some(self.next_review_id);
                self.review_requests.insert(self.next_review_id);
                self.pending_reviews = self.review_requests.len();
            }
        }
    }

    /// #5 的到达阶段先于 completed 查重与 goal 学习。
    fn arrive_notification(&mut self, event: HeldNotification, actions: &mut Vec<Action>) {
        self.runtime_notification(&event);
        // 先撤销历史根的旧证据，避免关系通知中的同步协调提前确认树停止。
        if !self.closing {
            if let Some(thread) = thread_id(&event.params)
                .filter(|t| !self.claims.contains_key(*t))
                .map(str::to_owned)
            {
                let changed = matches!(event.method.as_str(), "turn/started" | "turn/completed")
                    || event.method == "thread/goal/updated"
                        && event
                            .params
                            .as_ref()
                            .is_some_and(|p| p["goal"]["status"] == "active");
                if changed {
                    self.unclaimed_tree_root_changed(&thread, actions);
                }
            }
        }
        self.on_relation_notification(&event, actions);
        // raw 已交给内部因果观察；不占 settling/review 缓冲，也不进入普通通知广播。
        if raw_events::is_raw(&event.method) {
            if let Some(thread) = thread_id(&event.params) {
                let claim = self.claims.get(thread);
                self.forward_notification(
                    thread,
                    claim.and_then(Claim::owner),
                    claim.is_some_and(|c| c.owner_gone),
                    &event,
                    actions,
                );
            }
            return;
        }
        if matches!(event.method.as_str(), "item/started" | "item/completed") {
            if let Some(p) = &event.params {
                if p["item"]["type"] == "commandExecution" {
                    if let (Some(thread), Some(turn), Some(item)) = (
                        p["threadId"].as_str(),
                        p["turnId"].as_str(),
                        p["item"]["id"].as_str(),
                    ) {
                        if self.stops.command_turns.len() < 16_384
                            || self
                                .stops
                                .command_turns
                                .contains_key(&(thread.into(), item.into()))
                        {
                            let owner = self
                                .binding(thread, turn)
                                .filter(|b| b.status == crate::relations::BindingStatus::Bound)
                                .map(|b| b.run_id.clone())
                                .or_else(|| {
                                    self.claims
                                        .get(thread)
                                        .filter(|c| {
                                            c.owned_turns.contains(turn)
                                                && !c.joined()
                                                && !c.settling()
                                        })
                                        .and_then(|c| c.run_id.clone())
                                });
                            if let Some(run) = owner {
                                self.stops
                                    .command_owners
                                    .insert((thread.into(), item.into()), run);
                            }
                            self.stops
                                .command_turns
                                .insert((thread.into(), item.into()), turn.into());
                        }
                    }
                }
            }
        }

        if self.closing {
            return;
        }
        let Some(thread_id) = thread_id(&event.params).map(str::to_owned) else {
            // 不带 threadId 的视为全局通知，广播给所有连接（2.4 节「通知」）。
            actions.push(Action::Broadcast {
                message: proto::notification(&event.method, event.params),
            });
            return;
        };
        // H2：活动版本号在进入 held 之前推进；settling 结束后的回放走 commit_notification，不再推进。
        if matches!(event.method.as_str(), "turn/started" | "turn/completed") {
            self.status_activity_changed(&thread_id);
        }
        if event.method == "serverRequest/resolved" {
            self.resolve_held_request(&thread_id, &event.params, actions);
        }
        let settling = self
            .claims
            .get(&thread_id)
            .and_then(|claim| match &claim.state {
                State::Starting {
                    settling: true,
                    undecided,
                    turn_id,
                    ..
                } => {
                    let own = undecided.is_some()
                        && event.method == "turn/started"
                        && turn_id
                            .as_deref()
                            .is_some_and(|t| notification_turn_id(&event.params) == Some(t));
                    Some((own, turn_id.clone()))
                }
                _ => None,
            });
        if let Some((own, target)) = settling {
            // 预算与溢出处置统一在 settling 模块（M7）；返回 false 表示硬上限已触发、已按 joined 定案。
            if self.hold_notification(&thread_id, event, actions) && own {
                self.settle(&thread_id, false, target, actions);
            }
            return;
        }
        self.commit_notification(&thread_id, event, actions);
    }

    /// #4、#9、#12–#17：提交时查重，review 回放只驱动新认领。
    fn commit_notification(
        &mut self,
        thread_id: &str,
        mut event: HeldNotification,
        actions: &mut Vec<Action>,
    ) {
        self.buffer_child_event_log(
            thread_id,
            &event.method,
            &event.params,
            event.committed,
            actions,
        );
        let child_handled = self.dispatch_child_diagnostic(
            thread_id,
            &event.method,
            &event.params,
            event.committed,
            actions,
        );
        // review 子线程未知归属时延迟到账本回放；其余完成在认领去重前交给账本。
        if self.claims.contains_key(thread_id) || self.pending_reviews == 0 || event.committed {
            self.record_run_notification(thread_id, &event, actions);
        }
        if !event.committed && event.method == "turn/completed" {
            if let Some(turn_id) = notification_turn_id(&event.params) {
                if !self.mark_ended(thread_id, turn_id) {
                    actions.push(Action::Debug("丢弃已提交过的迟到完成通知".into()));
                    return;
                }
            }
        }
        // #2c：预检期间外部 turn 开始，必须在旧轮过滤之前结束预检（#2d' threadBusy），
        // 否则迟到处理可能直接消化这条 started，预检按此前的空快照放行。
        if !event.committed
            && event.method == "turn/started"
            && self.claims.get(thread_id).is_some_and(Claim::precheck)
        {
            self.continue_cancel(thread_id, resume::CancelCause::ExternalTurn, actions);
        }
        if let Some(id) = notification_turn_id(&event.params) {
            if self.handle_late_notification(thread_id, &event.method, id, actions) {
                return;
            }
        }
        let has_claim = self.claims.contains_key(thread_id);
        if !has_claim && event.method == "turn/completed" {
            if let Some(turn) = notification_turn_id(&event.params) {
                self.on_unclaimed_turn(thread_id, turn, false, actions);
            }
        }
        let (owner, gone) = self
            .claims
            .get(thread_id)
            .map(|claim| (claim.owner().cloned(), claim.owner_gone))
            .unwrap_or((None, false));
        self.forward_notification(thread_id, owner.as_ref(), gone, &event, actions);
        if !event.committed && event.method == "item/autoApprovalReview/completed" {
            self.inbox_guardian_review(thread_id, &event.params, actions);
        }
        if !event.committed {
            if event.method == "thread/tokenUsage/updated" {
                if let Some(params) = event.params.as_ref() {
                    if let (Some(turn), Some(usage)) = (
                        params.get("turnId").and_then(Value::as_str),
                        params.get("tokenUsage"),
                    ) {
                        if self.convergences.usage_evidence.len() < 4096 {
                            self.convergences
                                .usage_evidence
                                .insert((thread_id.into(), turn.into()), usage.clone());
                        }
                    }
                }
            }
            if event.method == "thread/goal/updated" {
                let active = event
                    .params
                    .as_ref()
                    .and_then(|p| p.get("goal"))
                    .and_then(|g| g.get("status"))
                    .and_then(Value::as_str)
                    == Some("active");
                let status = event
                    .params
                    .as_ref()
                    .and_then(|p| p.get("goal"))
                    .and_then(|g| g.get("status"))
                    .and_then(Value::as_str);
                if status.is_some() {
                    self.run_goal_status(thread_id, status);
                }
                self.guard_goal(
                    thread_id,
                    event.params.as_ref().and_then(|p| p.get("goal")),
                    loop_guard::GoalSource::Notification,
                );
                self.learn_goal(thread_id, active, actions);
            } else if event.method == "thread/goal/cleared" {
                self.run_goal_status(thread_id, None);
                self.guard_goal(thread_id, None, loop_guard::GoalSource::Notification);
                self.learn_goal(thread_id, false, actions);
            }
        }
        if !event.committed && event.method == "thread/deleted" {
            self.lifecycle_observe(thread_id, true, actions);
            self.run_forget_thread(thread_id, actions);
        }
        if !event.committed && event.method == "turn/started" {
            self.lifecycle_observe(thread_id, false, actions);
        }
        if !has_claim && self.pending_reviews > 0 && !event.committed && !child_handled {
            event.committed = true;
            let count: usize = self.unclaimed.values().map(Vec::len).sum();
            if count < MAX_UNCLAIMED {
                self.unclaimed
                    .entry(thread_id.into())
                    .or_default()
                    .push(event);
            }
            return;
        }
        if event.method == "turn/completed"
            && notification_turn_id(&event.params).is_some_and(|turn| {
                self.records
                    .get(thread_id)
                    .is_some_and(|r| r.released_turns.iter().any(|t| t == turn))
                    && self.claims.get(thread_id).and_then(Claim::turn_id) != Some(turn)
            })
        {
            // 旧轮结果已经交给账本，但不得推进新认领。
            return;
        }
        // #2c：预检期间通知照常转发，但不驱动 #4–#17；外部 turn/started 使预检以 threadBusy 结束。
        if self.continue_precheck_driver(thread_id, &event, actions) {
            return;
        }
        if event.method == "turn/started" {
            if let Some(id) = notification_turn_id(&event.params) {
                self.started(thread_id, id, actions);
                self.run_diagnostic_event(thread_id, id, event.committed, None, true, actions);
                self.dispatch_run_progress(
                    thread_id,
                    id,
                    &event.method,
                    &event.params,
                    event.committed,
                    actions,
                );
            }
        } else if event.method == "turn/completed" {
            if self.claims.get(thread_id).is_some_and(|c| {
                c.run_id.as_ref().is_some_and(|run| {
                    // 只有已启用 SA1b 的运行才按自有轮过滤完成通知；否则保持原语义（审查的外层轮
                    // 在内层轮开始后完成，不能被丢弃）。
                    self.sa1b_active(run)
                }) && notification_turn_id(&event.params)
                    .is_some_and(|t| !c.owned_turns.contains(t))
            }) {
                return;
            }
            if self.stop_busy(thread_id) {
                if let Some(id) = notification_turn_id(&event.params) {
                    self.stop_completed_turn(thread_id, id, actions);
                }
            } else {
                self.completed(
                    thread_id,
                    notification_turn_id(&event.params).unwrap_or_default(),
                    actions,
                );
            }
        }
    }

    pub(crate) fn record_run_turn_started(
        &mut self,
        thread: &str,
        turn: &str,
        actions: &mut Vec<Action>,
    ) {
        if let Some(run) = self.claims.get(thread).and_then(|c| c.run_id.clone()) {
            self.run_turn_started(&run, thread, turn, actions);
        }
    }

    fn record_run_notification(
        &mut self,
        thread: &str,
        event: &HeldNotification,
        actions: &mut Vec<Action>,
    ) {
        if event.method != "turn/started" {
            let turn = event
                .params
                .as_ref()
                .and_then(|p| {
                    if event.method == "turn/completed" {
                        p.get("turn").and_then(|t| t.get("id"))
                    } else {
                        p.get("turnId")
                    }
                })
                .and_then(Value::as_str);
            if let Some(turn) = turn {
                if self.claims.contains_key(thread) {
                    self.dispatch_run_progress(
                        thread,
                        turn,
                        &event.method,
                        &event.params,
                        event.committed,
                        actions,
                    );
                }
            }
        }
        if self
            .claims
            .get(thread)
            .is_some_and(|claim| claim.run_id.is_none())
        {
            return;
        }
        // 关联被淘汰后，只有尚未结束且符合当前认领轮次的通知可以回退；
        // 旧轮仍交给账本按实际线程和 turnId 查找，不能借用新认领的 runId。
        let turn = if matches!(event.method.as_str(), "item/started" | "item/completed") {
            event
                .params
                .as_ref()
                .and_then(|p| p.get("turnId"))
                .and_then(Value::as_str)
        } else {
            notification_turn_id(&event.params)
        };
        let run = self
            .claims
            .get(thread)
            .filter(|claim| {
                turn.filter(|turn| !turn.is_empty()).is_some_and(|turn| {
                    claim
                        .turn_id()
                        .filter(|current| !current.is_empty())
                        .is_none_or(|current| current == turn)
                        && !self.records.get(thread).is_some_and(|record| {
                            record
                                .ended_turns
                                .iter()
                                .chain(&record.released_turns)
                                .any(|old| old == turn)
                        })
                })
            })
            .and_then(|claim| claim.run_id.clone());
        match event.method.as_str() {
            "error" => {
                if let (Some(turn), Some(params)) = (turn, event.params.as_ref()) {
                    if let (Some(error), Some(will_retry)) = (
                        params.get("error").filter(|error| error.is_object()),
                        params.get("willRetry").and_then(Value::as_bool),
                    ) {
                        let diagnostic = UpstreamError::from_error(
                            thread,
                            Some(turn),
                            UpstreamErrorSource::ErrorNotification,
                            error,
                            Some(will_retry),
                            self.clock.now_ms,
                        );
                        self.run_diagnostic_event(
                            thread,
                            turn,
                            event.committed,
                            Some(diagnostic),
                            false,
                            actions,
                        );
                    }
                }
            }
            "model/rerouted" => self.run_rerouted(&event.params, actions),
            "item/started" | "item/completed" => {
                if let Some(turn) = turn {
                    self.run_diagnostic_event(thread, turn, event.committed, None, true, actions);
                }
                if let (Some(turn), Some(item)) =
                    (turn, event.params.as_ref().and_then(|p| p.get("item")))
                {
                    self.guard_item(thread, turn, item);
                }
                self.run_item(
                    thread,
                    run.as_deref(),
                    &event.method,
                    &event.params,
                    actions,
                )
            }
            "turn/completed" => {
                if let Some(turn) = event.params.as_ref().and_then(|p| p.get("turn")) {
                    if let Some(id) = turn.get("id").and_then(Value::as_str) {
                        let diagnostic = turn
                            .get("error")
                            .filter(|error| {
                                turn.get("status").and_then(Value::as_str) == Some("failed")
                                    && error.is_object()
                            })
                            .map(|error| {
                                UpstreamError::from_error(
                                    thread,
                                    Some(id),
                                    UpstreamErrorSource::TurnCompleted,
                                    error,
                                    Some(false),
                                    self.clock.now_ms,
                                )
                            });
                        self.run_diagnostic_event(
                            thread,
                            id,
                            event.committed,
                            diagnostic,
                            true,
                            actions,
                        );
                    }
                    if run.is_none() {
                        if let Some(current_run) =
                            self.claims.get(thread).and_then(|c| c.run_id.clone())
                        {
                            self.reconcile_observe_parent_completed(
                                &current_run,
                                thread,
                                turn,
                                actions,
                            );
                        }
                    }
                    self.run_turn_completed(thread, run.as_deref(), turn, actions);
                }
            }
            _ => {}
        }
    }

    pub(crate) fn started(&mut self, thread_id: &str, id: &str, actions: &mut Vec<Action>) {
        if self.convergence_event(
            thread_id,
            converge::ConvergeEvent::ParentTurn {
                turn: id,
                started: true,
            },
            actions,
        ) {
            return;
        }
        // 必须在改变认领轮次及写开轮证据之前隔离旧轮；通知自报 threadId 不证明归属。
        if id.is_empty()
            || self.claims.get(thread_id).is_some_and(|claim| {
                self.records.get(thread_id).is_some_and(|record| {
                    record
                        .ended_turns
                        .iter()
                        .chain(&record.released_turns)
                        .any(|turn| turn == id)
                }) || claim.run_id.as_deref().is_some_and(|run| {
                    self.runs.find_run(thread_id, id, Some(run)).as_deref() != Some(run)
                        || self.runs.superseded_turn(run, thread_id, id)
                })
            })
        {
            return;
        }
        if self
            .claims
            .get(thread_id)
            .is_some_and(|c| matches!(c.state, State::FinishingTree { .. }))
        {
            self.finishing_tree_turn(thread_id, id, true, actions);
            return;
        }
        if !self.claims.contains_key(thread_id) {
            self.on_unclaimed_turn(thread_id, id, true, actions);
        }
        self.late_started(thread_id, id, actions);
        let Some(claim) = self.claims.get_mut(thread_id) else {
            return;
        };
        // 恢复屏障尚未放行时，还没有本认领提交的开轮请求。
        if claim.pending_start.is_some() {
            return;
        }
        let auto = matches!(claim.state, State::AwaitingContinuation { .. });
        match &mut claim.state {
            State::Starting { turn_id, .. } => *turn_id = Some(id.into()),
            State::Running { turn_id, .. } => *turn_id = id.into(),
            State::AwaitingContinuation { owner } => {
                claim.state = State::Running {
                    owner: owner.clone(),
                    turn_id: id.into(),
                    joined: false,
                }
            }
            State::Converging { .. } | State::FinishingTree { .. } => return,
            State::Idle => {
                actions.push(Action::Warn("启动通知与认领阶段不匹配".into()));
                return;
            }
        }
        let run = claim.run_id.clone();
        let own = !claim.joined() && !claim.settling() && claim.pending_start.is_none();
        let gone = claim.owner_gone && !claim.joined();
        if let Some(run) = run {
            if own {
                self.run_start_evidence(&run, thread_id, id, actions);
            }
            self.run_turn_started(&run, thread_id, id, actions);
            if auto {
                self.guard_auto_started(&run, thread_id, id, actions);
            }
        }
        if own {
            self.remember_owned_turn(thread_id, id);
            if let Some(token) = self
                .claims
                .get(thread_id)
                .and_then(|c| c.run_id.as_ref())
                .and_then(|run| self.convergences.runs.get(run))
                .map(|r| r.token.clone())
            {
                self.reconcile_parent_started(
                    &token,
                    id,
                    if auto {
                        crate::sa1b::TurnOrigin::Goal
                    } else {
                        crate::sa1b::TurnOrigin::User
                    },
                    actions,
                );
            }
        }
        // #2e：本认领的 turn/started 之后带防护地恢复 goal（乙路判断全部条件）。
        self.maybe_restore_goal(thread_id, actions);
        if gone {
            self.begin_stop(thread_id, StopReason::ClientDisconnected, false, actions);
        }
        self.stop_started(thread_id, id);
    }

    /// C1 核实读发现的真实终态走原有收尾，不转发合成的上游完成通知。
    #[allow(dead_code)] // 骨架阶段由 C1 填写 stall_verify_done 后调用。
    pub(crate) fn commit_verified_turn_end(
        &mut self,
        run: &str,
        token: &ClaimToken,
        status: status_read::StatusTurn,
        shell: &Value,
        actions: &mut Vec<Action>,
    ) -> bool {
        let expected = match status {
            status_read::StatusTurn::Completed => "completed",
            status_read::StatusTurn::Failed => "failed",
            status_read::StatusTurn::Interrupted => "interrupted",
            status_read::StatusTurn::InProgress => return false,
        };
        let Some(turn) = shell
            .get("id")
            .and_then(Value::as_str)
            .filter(|t| !t.is_empty())
        else {
            return false;
        };
        let thread = &token.thread_id;
        if self.closing
            || self.quiescing
            || !self.current(token)
            || shell.get("status").and_then(Value::as_str) != Some(expected)
            || self.claims.get(thread).is_none_or(|claim| {
                claim.run_id.as_deref() != Some(run) || claim.joined() || claim.settling()
            })
            || self.diagnostic_run(thread, turn, false).as_deref() != Some(run)
        {
            return false;
        }
        self.status_activity_changed(thread);
        self.parent_turn_ended(thread, turn, actions);
        self.record_run_notification(
            thread,
            &HeldNotification {
                method: "turn/completed".into(),
                params: Some(json!({"threadId":thread,"turn":shell})),
                committed: false,
            },
            actions,
        );
        if !self.mark_ended(thread, turn) {
            return false;
        }
        if self.stop_busy(thread) {
            self.stop_completed_turn(thread, turn, actions);
        } else {
            self.completed(thread, turn, actions);
        }
        true
    }

    fn completed(&mut self, thread_id: &str, convergence_turn: &str, actions: &mut Vec<Action>) {
        if self.convergence_enabled()
            && self
                .claims
                .get(thread_id)
                .and_then(|c| c.run_id.as_deref())
                .is_some_and(|run| self.sa1b_active(run))
            && self
                .claims
                .get(thread_id)
                .and_then(Claim::turn_id)
                .is_some_and(|id| id != convergence_turn)
        {
            return;
        }
        if self.convergence_event(
            thread_id,
            converge::ConvergeEvent::ParentTurn {
                turn: convergence_turn,
                started: false,
            },
            actions,
        ) {
            return;
        }
        if let Some(claim) = self
            .claims
            .get(thread_id)
            .filter(|c| matches!(c.state, State::FinishingTree { .. }))
        {
            let turn = claim.turn_id().unwrap_or_default().to_owned();
            self.finishing_tree_turn(thread_id, &turn, false, actions);
            return;
        }
        let parent_status = self
            .claims
            .get(thread_id)
            .and_then(|c| c.run_id.as_ref())
            .and_then(|run| self.runs.runs.get(run))
            .and_then(|r| {
                r.current_turn_id
                    .as_ref()
                    .and_then(|id| r.turns.iter().find(|t| &t.turn_id == id))
                    .or_else(|| r.turns.last())
            })
            .map(|t| t.status);
        let goal_status = self
            .records
            .get(thread_id)
            .and_then(|r| r.goal_status.clone());
        let exceptional = matches!(
            parent_status,
            Some(crate::runs::TurnStatus::Failed | crate::runs::TurnStatus::Interrupted)
        );
        if exceptional
            && self.natural_release_gate(thread_id, goal_status.as_deref(), parent_status, actions)
                == finish_gate::ReleaseDecision::Hold
        {
            return;
        }
        if self.stop_busy(thread_id) {
            self.stop_completed(thread_id, actions);
            return;
        }
        let Some(claim) = self.claims.get_mut(thread_id) else {
            return;
        };
        let active = self.records.get(thread_id).is_some_and(|r| r.goal_active);
        if active && !claim.owner_gone && !claim.joined() {
            if let Some(owner) = claim.owner().cloned() {
                claim.state = State::AwaitingContinuation { owner };
            }
        } else {
            if !exceptional
                && self.natural_release_gate(
                    thread_id,
                    goal_status.as_deref(),
                    parent_status,
                    actions,
                ) == finish_gate::ReleaseDecision::Hold
            {
                return;
            }
            self.release(thread_id, actions);
        }
    }

    fn mark_ended(&mut self, thread_id: &str, turn_id: &str) -> bool {
        let record = self.record(thread_id);
        if record.ended_turns.iter().any(|id| id == turn_id) {
            return false;
        }
        record.ended_turns.push_back(turn_id.into());
        while record.ended_turns.len() > ENDED_PER_THREAD {
            record.ended_turns.pop_front();
        }
        self.mark_owned_ended(thread_id, turn_id);
        self.ended_order.retain(|id| id != thread_id);
        self.ended_order.push_back(thread_id.into());
        while self.ended_order.len() > MAX_HISTORY {
            if let Some(oldest) = self.ended_order.pop_front() {
                if let Some(record) = self.records.get_mut(&oldest) {
                    record.ended_turns.clear();
                }
            }
        }
        true
    }

    /// goal 是线程级事实（3.1 节）：不论来自哪个认领的请求，接受了就按当前认领执行 #17，
    /// 与上游 onGoalInactive 一致（879–904 行），不能只改记录、留下等不来的续跑空窗。
    pub(crate) fn learn_goal(&mut self, thread_id: &str, active: bool, actions: &mut Vec<Action>) {
        self.record(thread_id).goal_active = active;
        self.observe_goal(thread_id, active);
        self.convergence_observe_goal(thread_id, active, actions);
        if self.convergence_event(
            thread_id,
            converge::ConvergeEvent::GoalChanged { active },
            actions,
        ) {
            return;
        }
        if self
            .claims
            .get(thread_id)
            .is_some_and(|c| matches!(c.state, State::FinishingTree { .. }))
        {
            self.finishing_tree_goal(thread_id, active, actions);
            return;
        }
        if !active
            && !self.stop_busy(thread_id)
            && self
                .claims
                .get(thread_id)
                .is_some_and(|c| matches!(c.state, State::AwaitingContinuation { .. }))
        {
            let goal = self
                .records
                .get(thread_id)
                .and_then(|r| r.goal_status.clone());
            let status = self
                .claims
                .get(thread_id)
                .and_then(|c| c.run_id.as_ref())
                .and_then(|run| self.runs.runs.get(run))
                .and_then(|r| r.selected_parent_result())
                .map(|t| t.status);
            if self.natural_release_gate(thread_id, goal.as_deref(), status, actions)
                == finish_gate::ReleaseDecision::Release
            {
                self.release(thread_id, actions);
            }
        }
    }

    pub(crate) fn learn_traffic(
        &mut self,
        context: &RequestContext,
        result: &Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        if self.relations.aux.ignored_subscription_reply
            && matches!(
                context.method.as_str(),
                "thread/resume" | "thread/unsubscribe"
            )
        {
            return;
        }
        if self.closing {
            return;
        }
        let Ok(value) = result else { return };
        if matches!(
            context.method.as_str(),
            "thread/start" | "thread/resume" | "thread/fork"
        ) {
            if let Some(id) = value
                .get("thread")
                .and_then(|t| t.get("id"))
                .and_then(Value::as_str)
            {
                self.forget_closed_runtime(id);
                self.status_activity_changed(id);
                if context.method == "thread/start" {
                    if let Some(client) = context.client.as_ref().filter(|client| {
                        client
                            .params
                            .as_ref()
                            .is_some_and(|p| p["experimentalRawEvents"] == true)
                    }) {
                        self.record(id).raw_clients.insert(client.conn);
                    }
                }
            }
            // 裸 resume 已有确切的请求线程，成功就恢复订阅事实，不依赖服务器
            // 是否在回包里重复给出 thread.id；普通创建请求仍从回包学习新线程。
            if matches!(
                context.operation,
                Operation::ResumeBarrier | Operation::ContinueResume
            ) {
                if let Some(thread_id) = thread_id(&context.params) {
                    self.record(thread_id).subscribed = true;
                }
            }
            // 子线程重新附着只恢复附着证据（relation_reply），不记订阅事实、不学线程索引：
            // 否则子线程会被当作 broker 自己订阅的线程，运行结束后再也不随根退订。
            if let Some(thread_id) = value
                .get("thread")
                .and_then(|t| t.get("id"))
                .and_then(Value::as_str)
                .filter(|_| !matches!(context.operation, Operation::ChildSubscription { .. }))
            {
                if context.method != "thread/fork" {
                    self.record(thread_id).subscribed = true;
                }
                // broker 自发恢复（屏障、续跑预检、inbox）的回包反映上游会话配置，
                // 不代表本次开轮的请求值；只有客户端线程请求的回包才改写索引模型。
                let learn_model = context.operation == Operation::Forward;
                self.run_learn_thread(thread_id, value, learn_model, actions);
            }
        } else if matches!(context.method.as_str(), "thread/archive" | "thread/delete")
            && context.operation == Operation::Forward
        {
            if let Some(thread) = thread_id(&context.params) {
                self.run_forget_thread(thread, actions);
            }
        } else if context.method == "thread/unsubscribe" {
            // 自动退订在发出时已改事实，迟到回包不能覆盖新认领的恢复订阅。
            if matches!(context.operation, Operation::Forward) {
                if let Some(thread_id) = thread_id(&context.params) {
                    self.record(thread_id).subscribed = false;
                    self.detach_evidence(thread_id);
                }
            }
        } else if context.method == "thread/goal/get" || context.method == "thread/goal/set" {
            if let Some(goal) = value.get("goal") {
                if let Some(thread_id) = goal
                    .get("threadId")
                    .and_then(Value::as_str)
                    .or_else(|| thread_id(&context.params))
                {
                    let status = goal.get("status").and_then(Value::as_str);
                    if goal.is_null() || status.is_some() {
                        self.run_goal_status(thread_id, status);
                    }
                    self.guard_goal(thread_id, Some(goal), loop_guard::GoalSource::Reply);
                    let active = goal.get("status").and_then(Value::as_str) == Some("active");
                    // 本 broker 的客户端设置成 active、或 #2e 恢复成功：这个 goal 归本次运行控制。
                    if context.method == "thread/goal/set"
                        && ((context.operation == Operation::Forward && context.client.is_some())
                            || matches!(
                                context.operation,
                                Operation::GoalRestore(resume::GoalRestoreStep::SetActive { .. })
                            ))
                    {
                        self.grant_goal_control(thread_id, goal);
                    }
                    self.learn_goal(thread_id, active, actions);
                }
            }
        } else if context.method == "thread/goal/clear" {
            if let Some(thread_id) = thread_id(&context.params) {
                self.run_goal_status(thread_id, None);
                self.guard_goal(thread_id, None, loop_guard::GoalSource::Reply);
                self.learn_goal(thread_id, false, actions);
            }
        }
    }

    /// #18 与停止操作：joined 不碰服务器，自有认领仅标记 owner_gone。
    fn disconnect(&mut self, conn: ConnId, actions: &mut Vec<Action>) {
        self.on_convergence_owner_disconnect(conn, actions);
        self.lifecycle_disconnect(conn);
        self.live_conns.remove(&conn);
        for record in self.records.values_mut() {
            record.raw_clients.remove(&conn);
        }
        self.cxb_conns.remove(&conn);
        self.inbox_disconnect(conn);
        self.inbox_flush_actions(actions);
        self.disconnect_forwards(conn, actions);
        let ids: Vec<String> = self
            .claims
            .iter()
            .filter(|(_, c)| c.owner() == Some(&Owner::Client(conn)))
            .map(|(id, _)| id.clone())
            .collect();
        for thread_id in ids {
            if self.claims[&thread_id].precheck() {
                // #20：预检中断开，直接取消，什么都不发。
                self.continue_cancel(&thread_id, resume::CancelCause::Disconnected, actions);
                continue;
            }
            if let Some(run) = self.claims.get(&thread_id).and_then(|c| c.run_id.clone()) {
                let kind = if self.claims[&thread_id].joined() {
                    crate::relations::HandoffKind::Joined
                } else {
                    crate::relations::HandoffKind::OwnerGone
                };
                self.on_handoff(&run, kind, actions);
            }
            // #26a'：本连接排队的交接撤销（运行日志记一行，不回包）。
            self.detach_disconnected(&thread_id, conn, actions);
            let claim = self.claims.get_mut(&thread_id).expect("断开事件中的认领");
            if claim.joined() {
                self.release(&thread_id, actions);
            } else {
                claim.owner_gone = true;
                self.begin_stop(&thread_id, StopReason::ClientDisconnected, false, actions);
            }
        }
        let watched: Vec<_> = self
            .records
            .iter()
            .filter(|(_, r)| r.watchers.contains(&conn))
            .map(|(id, _)| id.clone())
            .collect();
        for record in self.records.values_mut() {
            record.watchers.retain(|id| *id != conn);
            if record.exclusive_waiter == Some(conn) {
                record.exclusive_waiter = None;
            }
            record.pending_meta.remove(&conn);
        }
        for thread in watched {
            self.watcher_activity(&thread, actions);
        }
        self.prune_ext_meta();
        self.disconnect_stops(conn);
    }

    /// 3.3 统一释放出口：撤销 held（settling 期间挂起、尚未处理的请求），记录最近运行，再登记下一拍退订。
    /// 已转发给 owner 的请求不撤销，计时器也保留：与上游 releaseThread 一致，owner 仍可回答。
    pub(crate) fn release(&mut self, thread_id: &str, actions: &mut Vec<Action>) {
        let kind = if self.closing {
            ReleaseKind::Shutdown
        } else if self.claims.get(thread_id).is_some_and(Claim::joined) {
            ReleaseKind::Joined
        } else {
            ReleaseKind::Natural
        };
        self.release_with_kind(thread_id, kind, actions);
    }

    pub(crate) fn release_with_kind(
        &mut self,
        thread_id: &str,
        kind: ReleaseKind,
        actions: &mut Vec<Action>,
    ) {
        if kind == ReleaseKind::Natural {
            if let Some(run) = self.claims.get(thread_id).and_then(|c| c.run_id.clone()) {
                let waiting = self
                    .convergences
                    .runs
                    .get(&run)
                    .is_some_and(|r| !r.automatic_control_revoked)
                    && self
                        .runs
                        .runs
                        .get(&run)
                        .and_then(|r| r.subagent_join.as_ref())
                        .is_some_and(|j| j.phase != crate::sa1b::JoinPhase::Released);
                if waiting && self.trees.aux.committing.as_deref() != Some(thread_id) {
                    let status = self
                        .runs
                        .runs
                        .get(&run)
                        .and_then(|r| {
                            r.current_turn_id
                                .as_ref()
                                .and_then(|id| r.turns.iter().find(|t| &t.turn_id == id))
                        })
                        .map(|t| t.status);
                    let goal = self
                        .records
                        .get(thread_id)
                        .and_then(|r| r.goal_status.clone());
                    if self
                        .convergence_release_gate(thread_id, goal.as_deref(), status, actions)
                        .is_some()
                    {
                        return;
                    }
                }
            }
        }
        if self.tree_release_gate(thread_id, kind, actions) {
            return;
        }
        let owner = self.owner_conn(thread_id);
        let Some(mut claim) = self.claims.remove(thread_id) else {
            return;
        };
        self.clear_held_accounting(thread_id, &mut claim);
        self.cancel_joined_check(thread_id, actions);
        self.cancel_reclaim(thread_id, actions);
        if let Some(run) = &claim.run_id {
            self.run_released_owned(run, kind, owner, actions);
        }
        self.inbox_claim_released(thread_id, claim.claim_id, actions);
        // #26a'：仍排队的交接随释放结算（运行已结束回 runEnded；关闭中回 shuttingDown）。
        self.detach_released(thread_id, &mut claim, actions);
        if claim.pending_start.is_some() || !claim.request_pending {
            self.release_stream_slot(claim.stream_id);
        }
        self.detach_reconciliations(thread_id, claim.claim_id, actions);
        let mut released: Vec<_> = claim.owned_turns.iter().cloned().collect();
        released.sort();
        if let Some(turn) = claim.turn_id().filter(|t| !t.is_empty()) {
            released.push(turn.into());
        }
        let record = self.record(thread_id);
        for turn in released {
            record.released_turns.retain(|old| old != &turn);
            record.released_turns.push_back(turn);
            while record.released_turns.len() > ENDED_PER_THREAD {
                record.released_turns.pop_front();
            }
        }
        self.record(thread_id).last_run = claim.run_id.take();
        let token = ClaimToken {
            thread_id: thread_id.into(),
            claim_id: claim.claim_id,
        };
        self.record(thread_id).last_claim_id = token.claim_id;
        for request in claim.held_requests {
            actions.push(Action::Dismiss { id: request.id });
        }
        self.schedule_unsubscribe(token, actions);
    }
    pub(crate) fn schedule_unsubscribe(&self, token: ClaimToken, actions: &mut Vec<Action>) {
        if !self.closing {
            actions.push(Action::Schedule {
                token,
                kind: TimerKind::Unsubscribe,
                delay_ms: 0,
            });
        }
    }
    /// 沿用普通退订的代号及全部活动保护；直接复查，不反复安排零延迟计时器。
    pub(crate) fn retry_deferred_child_unsubscribes(&mut self, actions: &mut Vec<Action>) {
        if self.closing {
            self.relations.aux.deferred_child_unsubscribes.clear();
        }
        if self.relations.aux.deferred_child_unsubscribes.is_empty() {
            return;
        }
        let pending: Vec<_> = self
            .relations
            .aux
            .deferred_child_unsubscribes
            .values()
            .cloned()
            .collect();
        for token in pending {
            let current = self
                .records
                .get(&token.thread_id)
                .is_some_and(|record| record.subscribed && record.last_claim_id == token.claim_id);
            if !current {
                self.relations
                    .aux
                    .deferred_child_unsubscribes
                    .remove(&token.thread_id);
                continue;
            }
            // 只复用计时器处理，不递归进入 step；活动保护与容量检查均由同一入口执行。
            self.timer(token.clone(), TimerKind::Unsubscribe, actions);
            if self
                .records
                .get(&token.thread_id)
                .is_none_or(|record| !record.subscribed)
            {
                self.relations
                    .aux
                    .deferred_child_unsubscribes
                    .remove(&token.thread_id);
            }
        }
    }
    fn timer(&mut self, token: ClaimToken, kind: TimerKind, actions: &mut Vec<Action>) {
        if self.closing {
            return;
        }
        match kind {
            TimerKind::ChildBarrier { waiter } => self.child_barrier_timeout(waiter, actions),
            TimerKind::ChildSubscription { thread, op } => {
                self.child_subscription_timeout(&thread, op, actions)
            }
            TimerKind::ChildRetry { thread, op } => {
                self.child_subscription_retry(&thread, op, actions)
            }
            TimerKind::Stall { run_id, generation } => {
                self.stall_timer(&run_id, generation, actions)
            }
            TimerKind::BudgetVerifyDeadline { .. }
            | TimerKind::ConvergeDeadline { .. }
            | TimerKind::ConvergeRetry { .. }
            | TimerKind::ChildResultsDeadline { .. }
            | TimerKind::ReconcileVerifyDeadline { .. } => self.convergence_timer(kind, actions),
            TimerKind::StatusReadDeadline { read_id } => {
                self.status_read_deadline(read_id, actions)
            }
            TimerKind::LookupDeadline { .. }
            | TimerKind::DiscoveryDeadline { .. }
            | TimerKind::TombstoneExpire { .. } => self.relation_timer(kind, actions),
            TimerKind::TreeStopWait { .. } | TimerKind::TreeStopVerify { .. } => {
                self.tree_timer(kind, actions)
            }
            TimerKind::Lifecycle => self.lifecycle_tick(),
            TimerKind::JoinedCheckRetry { attempt } => {
                self.joined_check_retry(&token, attempt, actions)
            }
            TimerKind::Stop(tag, kind) => self.stop_timer(tag, kind, actions),
            TimerKind::Reconcile { generation, round } => {
                self.reconcile_timer(token, generation, round, actions)
            }
            TimerKind::LateExpiry { generation } => self.expire_late(generation, actions),
            TimerKind::Run(timer) => self.run_timer(timer, actions),
            TimerKind::GoalChangeExpiry { id } => self.local_goal_change_expiry(id, actions),
            TimerKind::Inbox(timer) => self.inbox_timer(token, timer, actions),
            TimerKind::Reclaim { generation } => self.reclaim_timer(token, generation, actions),
            TimerKind::DetachDeadline { generation } => {
                self.detach_deadline(token, generation, actions)
            }
            TimerKind::Unsubscribe => {
                if self.records.get(&token.thread_id).is_none_or(|record| {
                    !record.subscribed || record.last_claim_id != token.claim_id
                }) {
                    return;
                }
                let child = self.thread_relation(&token.thread_id).is_some();
                if child
                    && (self.child_activity_protected(&token.thread_id)
                        || !self.child_release_capacity(&token.thread_id, actions))
                {
                    self.relations
                        .aux
                        .deferred_child_unsubscribes
                        .insert(token.thread_id.clone(), token);
                    return;
                }
                if self.closing
                    || self.claims.contains_key(&token.thread_id)
                    || self.tree_busy(Some(&token.thread_id))
                    || self.relation_needs_subscription(&token.thread_id)
                {
                    return;
                }
                if let Some(record) = self.records.get_mut(&token.thread_id) {
                    if record.goal_active
                        || record.last_claim_id != token.claim_id
                        || !record.subscribed
                    {
                        return;
                    }
                    record.subscribed = false;
                    self.detach_evidence(&token.thread_id);
                    let context = RequestContext {
                        resume_retried: false,
                        stream_id: None,
                        review_id: None,
                        client: None,
                        params: Some(json!({"threadId": token.thread_id})),
                        token: Some(token),
                        operation: Operation::Unsubscribe,
                        method: "thread/unsubscribe".into(),
                    };
                    // 必须在请求入队及扫描其他子线程之前占位，不能让同一步共用剩余名额。
                    if child {
                        self.child_register_subscription(&context, actions);
                    }
                    actions.push(Action::Request(context));
                    // 子线程由 app-server 自动挂到本连接，随根一起退订，否则一直不卸载。
                    self.release_idle_children(None, actions);
                }
            }
            TimerKind::SettlementRetry { attempt } => {
                if self.current(&token) {
                    self.begin_settlement(&token.thread_id, attempt, actions);
                } else {
                    actions.push(Action::Warn("丢弃过期认领的吸收判定计时器".into()));
                }
            }
        }
    }
    fn shutdown(&mut self, actions: &mut Vec<Action>) {
        self.stall_shutdown(actions);
        self.convergence_shutdown(actions);
        if self.closing {
            return;
        }
        self.closing = true;
        self.relations_shutdown(actions);
        self.trees_shutdown(actions);
        self.status_reads_shutdown(actions);
        self.shutdown_stops.clear();
        self.shutdown_runs(actions);
        self.shutdown_stops(actions);
        self.shutdown_forwards(actions);
        self.inbox_shutdown(actions);
        let ids: Vec<String> = self.claims.keys().cloned().collect();
        for id in ids {
            if self.claims.get(&id).is_some_and(Claim::precheck) {
                self.continue_cancel(&id, resume::CancelCause::Shutdown, actions);
                continue;
            }
            if let Some(claim) = self.claims.get_mut(&id) {
                if let Some(request) = claim.pending_start.take() {
                    send_error(&request, code::GENERAL_ERROR, SHUTTING_DOWN, actions);
                }
                if let Some((context, result)) = claim.settlement_reply.take() {
                    self.deliver(&context, Ok(result), actions);
                }
            }
            self.release(&id, actions);
        }
        self.unclaimed.clear();
        self.pending_reviews = 0;
        self.review_requests.clear();
        self.stream_slots.clear();
    }

    /// 上限只计可淘汰历史，活动认领与待补全结果不挤占这 200 个名额。
    fn evict_records(&mut self) {
        let eligible = |id: &str, record: &ThreadRecord| {
            !self.claims.contains_key(id)
                && !record.result_pending
                && record.watchers.is_empty()
                && record.pending_meta.is_empty()
        };
        let mut count = self
            .records
            .iter()
            .filter(|(id, r)| eligible(id, r))
            .count();
        let mut evicted = Vec::new();
        for id in &self.record_order {
            if count <= MAX_HISTORY {
                break;
            }
            if self.records.get(id).is_some_and(|r| eligible(id, r)) {
                evicted.push(id.clone());
                count -= 1;
            }
        }
        for id in evicted {
            self.records.remove(&id);
            self.relations.aux.deferred_child_unsubscribes.remove(&id);
            self.record_order.retain(|t| t != &id);
            self.ended_order.retain(|t| t != &id);
        }
    }

    fn reject_interrupt(&self, request: &ClientRequest) -> bool {
        let Some(thread_id) = thread_id(&request.params) else {
            return false;
        };
        let Some(claim) = self.claims.get(thread_id) else {
            return false;
        };
        let named = request
            .params
            .as_ref()
            .and_then(|p| p.get("turnId"))
            .and_then(Value::as_str)
            .is_some_and(|id| claim.turn_id() == Some(id));
        (claim.joined() || claim.settling())
            && (claim.owner() == Some(&Owner::Client(request.conn)) || named)
    }
    fn client_joined(&mut self, request: ClientRequest, actions: &mut Vec<Action>) {
        let thread_id = thread_id(&request.params).map(str::to_owned);
        let turn_id = request
            .params
            .as_ref()
            .and_then(|p| p.get("turnId"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let allowed = thread_id
            .as_ref()
            .and_then(|id| self.claims.get(id))
            .is_some_and(|c| {
                // 预检没有运行，也还没转发 turn/start，不存在可加入的 turn（#2c）。
                c.owner() == Some(&Owner::Client(request.conn))
                    && !c.precheck()
                    && matches!(c.state, State::Starting { .. } | State::Running { .. })
            });
        if allowed {
            let thread_id = thread_id.expect("已核对 joined 的线程");
            let claim_id = self.claims[&thread_id].claim_id;
            self.retire_thread_forwards(&thread_id, claim_id, actions);
            if self.claims[&thread_id].settling() {
                self.settle(&thread_id, true, turn_id, actions);
            } else if let Some(claim) = self.claims.get_mut(&thread_id) {
                if let Some(owner) = claim.owner().cloned() {
                    let turn_id = turn_id
                        .or_else(|| claim.turn_id().map(str::to_owned))
                        .unwrap_or_default();
                    claim.state = State::Running {
                        owner,
                        turn_id,
                        joined: true,
                    };
                    claim.resume_goal = false;
                }
                let run = claim.run_id.clone();
                let turn = claim.turn_id().map(str::to_owned);
                if let Some(run) = run {
                    self.on_handoff(&run, crate::relations::HandoffKind::External, actions);
                    self.run_settled(&run, true, actions);
                    self.notify_run_settled(&thread_id, &run, turn.clone(), actions);
                    if let Some(turn) = turn {
                        self.run_turn_started(&run, &thread_id, &turn, actions);
                    }
                }
            }
            self.finish_joined_stop(&thread_id, claim_id, actions);
            if self.claims.get(&thread_id).is_some_and(|c| c.owner_gone) {
                self.release(&thread_id, actions);
            }
        }
        actions.push(Action::Send {
            conn: request.conn,
            message: proto::response_ok(request.id, json!({"joined": allowed})),
        });
    }

    /// #24：只有当前 owner 能根据服务器提示改投，失败保留原错误。
    fn retarget_interrupt(
        &self,
        context: &RequestContext,
        error: &Value,
        actions: &mut Vec<Action>,
    ) -> bool {
        let Some(client) = &context.client else {
            return false;
        };
        let Some(thread_id) = thread_id(&context.params) else {
            return false;
        };
        let Some(claim) = self.claims.get(thread_id) else {
            return false;
        };
        if claim.owner() != Some(&Owner::Client(client.conn)) || claim.joined() || claim.settling()
        {
            return false;
        }
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let words: Vec<&str> = message.split_whitespace().collect();
        let found = words.windows(8).find_map(|w| {
            (w[..4] == ["expected", "active", "turn", "id"] && w[5..7] == ["but", "found"])
                .then_some(w[7])
        });
        let Some(found) = found else { return false };
        if context
            .params
            .as_ref()
            .and_then(|p| p.get("turnId"))
            .and_then(Value::as_str)
            == Some(found)
            || (self.shared && claim.turn_id() != Some(found))
        {
            return false;
        }
        actions.push(Action::Request(RequestContext {
            resume_retried: false,
            stream_id: None,
            review_id: None,
            client: context.client.clone(),
            token: self.token(thread_id),
            operation: Operation::InterruptRetry {
                original_error: error.clone(),
            },
            method: "turn/interrupt".into(),
            params: Some(json!({"threadId": thread_id, "turnId": found})),
        }));
        true
    }
}

pub(crate) fn send_error(
    request: &ClientRequest,
    code: i64,
    message: &str,
    actions: &mut Vec<Action>,
) {
    actions.push(Action::Send {
        conn: request.conn,
        message: proto::response_err(request.id.clone(), proto::error_object(code, message, None)),
    });
}
impl Machine {
    /// 只在最终交付时按最近一次 initialize 添加扩展字段，暂存结果保持上游原样。
    fn deliver(
        &self,
        context: &RequestContext,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        let mut result = result;
        if let (Some(client), Operation::Stream { run_id: Some(run) }, Ok(value)) =
            (&context.client, &context.operation, &mut result)
        {
            if self.cxb_conns.contains(&client.conn) {
                if let Some(object) = value.as_object_mut() {
                    object.insert("runId".into(), json!(run));
                    object.insert(
                        "shortId".into(),
                        json!(self.runs.runs.get(run).map(|r| &r.short_id)),
                    );
                }
            }
        }

        if let Some(client) = &context.client {
            let message = match result {
                Ok(value) => proto::response_ok(client.id.clone(), value),
                Err(error) => proto::response_err(client.id.clone(), error),
            };
            actions.push(Action::Send {
                conn: client.conn,
                message,
            });
        }
    }
}
/// 只发给 owner（watcher 已在别处收到时用，例如 review 子线程早到通知的回放）。
fn forward_owner(owner: &Owner, gone: bool, event: &HeldNotification, actions: &mut Vec<Action>) {
    if !gone {
        if let Owner::Client(conn) = owner {
            actions.push(Action::Send {
                conn: *conn,
                message: proto::notification(&event.method, event.params.clone()),
            });
        }
    }
}
fn thread_id(params: &Option<Value>) -> Option<&str> {
    params
        .as_ref()
        .and_then(|p| p.get("threadId"))
        .and_then(Value::as_str)
}
fn notification_turn_id(params: &Option<Value>) -> Option<&str> {
    params.as_ref().and_then(|p| {
        p.get("turn")
            .and_then(|t| t.get("id"))
            .and_then(Value::as_str)
            .or_else(|| p.get("turnId").and_then(Value::as_str))
    })
}
fn response_turn_id(value: &Value) -> Option<&str> {
    value
        .get("turn")
        .and_then(|t| t.get("id"))
        .and_then(Value::as_str)
        .or_else(|| value.get("turnId").and_then(Value::as_str))
}
fn input_texts(params: &Option<Value>) -> Vec<String> {
    params
        .as_ref()
        .and_then(|p| p.get("input"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|i| i.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|i| i.get("text").and_then(Value::as_str).map(str::to_owned))
        .collect()
}
