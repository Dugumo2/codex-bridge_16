//! 运行记录、每轮结果与线程索引的数据类型（详细设计 2.3 节 RunResult、TurnResult，3.6 节，7.1 节）。
//!
//! 这些类型既是内存账本的结构，也是 runs/<runId>.json、threads.json 外壳里 payload 的格式，
//! 字段名按线上约定用 camelCase。字段只增不改：相对 2.3 节新增 `progress.doneTotal` 与 `stopCheck`，
//! 见 S1-评审.md 第 20 节。

use super::upstream_error::UpstreamError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

fn is_zero(value: &u64) -> bool {
    *value == 0
}

pub type RunId = String;
pub type ShortId = String;

/// 运行的生命周期。`Stopping` 表示发起了停止、唯一判定尚未通过；`Lost` 表示 broker 关闭或重启时
/// 仍在运行或停止中，本 broker 已不再跟踪（7.2 节）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RunState {
    Running,
    Stopping,
    Ended,
    Lost,
}

/// 运行的整体终态，只在运行结束（认领释放）时确定。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RunStatus {
    Completed,
    Interrupted,
    Failed,
    GoalBlocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum InterruptReason {
    BudgetExhausted,
    BudgetIncomplete,
    ConvergeDeadline,
    WakeLimit,
    AutoTurnLimit,
    DiscoveryIncomplete,
    OwnershipConflict,
    WakeUnknown,
    WakeFailed,
    GoalInactive,
    RootNoDirectInput,
    Abandoned,
    SubagentsUnfinished,
    ClientDisconnected,
    Stop,
    BrokerLost,
    /// 自动续跑空转刹车（S1-评审第 26 节）；详情见 `RunRecord.loop_guard`。
    GoalLoop,
    StallTimeout,
    UpstreamAccessError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum UpstreamErrorPolicy {
    Observe,
    StopAccess,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunPolicy {
    pub stall_timeout_ms: u64,
    pub upstream_error_policy: UpstreamErrorPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StallPhase {
    Verifying,
    Resumed,
    Stopping,
    Deferred,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StallVerify {
    Progressed,
    TurnEnded,
    InProgress,
    Absent,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StallReport {
    pub timeout_ms: u64,
    pub phase: StallPhase,
    pub expired_at: u64,
    pub idle_ms: u64,
    pub last_progress_at: Option<u64>,
    pub verify: Option<StallVerify>,
    pub expiries: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StopRpcOutcome {
    Ok,
    Error,
    Timeout,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StopRpcIssue {
    pub method: String,
    pub kind: StopRpcOutcome,
    pub at: u64,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StopDiagnostics {
    pub reason: InterruptReason,
    pub started_at: u64,
    pub goal_started_at: Option<u64>,
    pub goal_settled_at: Option<u64>,
    pub interrupt_sent: bool,
    pub interrupt_sent_at: Option<u64>,
    pub interrupt_reply: Option<StopRpcOutcome>,
    pub interrupt_replied_at: Option<u64>,
    pub turn_ended_at: Option<u64>,
    pub unconfirmed_at: Option<u64>,
    pub confirmed_at: Option<u64>,
    pub rpc_issues: Vec<StopRpcIssue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChildUpstreamError {
    pub thread_id: String,
    pub parent_thread_id: String,
    pub upstream_error: UpstreamError,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildRunRef {
    pub run_id: RunId,
    pub parent_thread_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProgressTarget {
    Own { run_id: RunId },
    Child(ChildRunRef),
}

impl ProgressTarget {
    pub fn run_id(&self) -> &str {
        match self {
            Self::Own { run_id } => run_id,
            Self::Child(child) => &child.run_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StallVerifyState {
    pub read_id: u64,
    pub generation: u64,
    pub claim_id: u64,
    pub conn_epoch: u64,
    pub turn_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StallState {
    pub baseline_mono: u64,
    pub timeout_ms: u64,
    pub generation: u64,
    pub timer_scheduled: bool,
    pub verify: Option<StallVerifyState>,
    pub phase: Option<StallPhase>,
}

/// 每类最多 1024 项，由 C1 在插入时淘汰最旧项。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProgressDedup {
    pub items: std::collections::VecDeque<(String, String, String, String)>,
    pub hooks: std::collections::VecDeque<(String, String, String, String)>,
    pub snapshots: std::collections::VecDeque<(String, String, String, String, u64)>,
    pub token_high_water: std::collections::VecDeque<(String, u64)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopDiagSend {
    pub stop_generation: u64,
    pub turn: Option<String>,
    pub thread: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DiagnosticField {
    UpstreamError,
    LastProgressAt,
    Stall,
    StopDiagnostics,
    ChildUpstreamErrors,
    StopOutcome,
    StopCheck,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiagnosticDirty {
    pub fields: std::collections::BTreeSet<DiagnosticField>,
    pub child: Option<ChildUpstreamError>,
    /// 首次标脏时捕获，释放或迁移认领后仍能发送。
    pub recipients: Vec<crate::claim::ConnId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LoopGuardKind {
    /// 连续几个证据完整的自动续跑轮没有外部动作。
    NoAction,
    /// 本次运行受控的自动续跑轮达到上限。
    ContinuationLimit,
}

/// 刹车采取的动作：私有模式走停止流程（暂停 goal 并中断，结果见 stopOutcome）；
/// 共享模式只暂停 goal、不中断任何轮（结果见 `pause`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LoopGuardAction {
    Stop,
    PauseGoal,
}

/// 共享模式暂停 goal 的结果；与线程是否已停分开报告。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PauseOutcome {
    /// 暂停请求已发出，还没回包。
    Pending,
    /// 服务器确认暂停的就是刹车时受控的那个 goal。
    Paused,
    /// 回包显示服务器上的 goal 已换成别的（检查与暂停之间被接管）。
    Superseded,
    /// 请求失败或超时；迟到的成功回包仍会改为 paused。
    Failed,
}

/// 空转刹车的触发记录：哪种刹车、数到几轮、阈值、构成判断的轮次、采取的动作。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoopGuardReport {
    pub kind: LoopGuardKind,
    pub count: u32,
    pub threshold: u32,
    pub turn_ids: Vec<String>,
    pub action: LoopGuardAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pause: Option<PauseOutcome>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StopOutcome {
    Confirmed,
    Unconfirmed,
}

/// 唯一「已停」判定各条件的当前值（M4 留给 M5 的注意事项第 2 条：运行查询要能说明为什么还挡着）。
/// 五项全为 true 才等价于 `confirmed()`（四项判定加上「核实读已落定」这一屏障，stop/mod.rs `confirmed`）；
/// 本结构只用于展示，由 `confirmed()` 拆出的只读函数计算，不能反过来驱动判定。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StopCheck {
    /// 归属已定（不在 settling、不是 joined、不是别人的认领）。
    pub ownership: bool,
    /// 目标 turn 已有结束证据或确认不存在。
    pub turn_ended: bool,
    /// goal 制动已确认（带版本号的证据）。
    pub goal_braked: bool,
    /// 在途的启动请求与 goal 写入都已落定（超时、结果未知不算）。
    pub settled: bool,
    /// 正在进行的核实轮次（thread/read 与 goal 读取）都已回来；没有核实轮次时为 true。
    pub verify_done: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Ready {
    Pending,
    Ready,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TurnStatus {
    Completed,
    Interrupted,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OutputSource {
    Review,
    FinalAnswer,
    Accumulated,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<crate::sa1b::TurnOrigin>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reroute: Option<Reroute>,
    pub turn_id: String,
    pub ready: Ready,
    pub status: TurnStatus,
    pub output: String,
    pub output_source: OutputSource,
    /// 原样保存 turn/completed 里 turn.error（TurnError），或启动失败时的 RPC 错误对象。
    pub error: Option<Value>,
    /// 毫秒时间戳。
    pub completed_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProgressItem {
    pub item_id: String,
    #[serde(rename = "type")]
    pub item_type: String,
    pub summary: String,
}

/// 续跑资料里的进度（3.6 节「中断后接着干」第 2 条）。各列表有上限，见 runs 模块常量。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Progress {
    /// 最近完成的条目（只保留最近 [`super::MAX_DONE_ITEMS`] 条）。
    pub done: Vec<ProgressItem>,
    /// 完成条目的总数，含被截掉的。
    pub done_total: u64,
    /// 已开始但未完成的命令与工具调用；完成即移出。
    pub unconfirmed: Vec<ProgressItem>,
    /// 观察到的、状态为 completed 的 fileChange 路径，去重，最多 [`super::MAX_OBSERVED_FILES`] 个。
    pub observed_files: Vec<String>,
    /// settling 缓冲超过软上限、丢过条目通知时为 true（M7）：上面的列表可能缺项，续跑时不能当作完整进度。
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub incomplete: bool,
}

/// 运行记录：runs/<runId>.json 的 payload，即 2.3 节的 RunResult。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunRecord {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_policy: Option<RunPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stall: Option<StallReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_diagnostics: Option<StopDiagnostics>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub child_upstream_errors: Vec<ChildUpstreamError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_error: Option<UpstreamError>,
    /// 只随上游报错或最近进展变化递增，与存储和协调版本独立。
    #[serde(default, skip_serializing_if = "is_zero")]
    pub diagnostic_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_progress_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_turns_started: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subagent_join: Option<crate::sa1b::SubagentJoin>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_budget: Option<crate::sa1b::RunBudget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub convergence_config: Option<crate::sa1b::ConvergenceConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_config: Option<crate::sa1b::RunExecutionConfig>,

    /// 最近一次已开始的轮次；结束不清空，旧记录不反推。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_reroute: Option<PendingReroute>,
    /// 创建时固定；文件外壳的 epoch/revision 随重写变化，不能用来判断运行先后。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creation_order: Option<CreationOrder>,
    /// 只由 broker 更新，旧记录缺省为未确认；重启不得从结果或 joined 反推。
    #[serde(default)]
    pub start_confirmation: StartConfirmation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background_cleanup: Option<crate::stop::background::BackgroundReport>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<crate::relations::ChildRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub children_stop: Option<crate::stop::tree::ChildrenStopReport>,

    pub run_id: RunId,
    pub thread_id: String,
    pub short_id: ShortId,
    pub joined: bool,
    pub state: RunState,
    pub status: Option<RunStatus>,
    pub started_at: u64,
    pub ended_at: Option<u64>,
    pub meta: Option<Value>,
    pub review_thread_id: Option<String>,
    /// review 子线程自己的 shortId，重建索引时用来恢复子线程的索引项。
    pub review_short_id: Option<ShortId>,
    pub input: String,
    pub continued_from: Option<RunId>,
    pub interrupt_reason: Option<InterruptReason>,
    pub stop_outcome: Option<StopOutcome>,
    /// 停止未确认时四项判定的当前值；没有停止操作时为 None。
    pub stop_check: Option<StopCheck>,
    /// 空转刹车触发过才有（S1-评审第 26 节）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loop_guard: Option<LoopGuardReport>,
    pub progress: Progress,
    /// 最近的轮次结果（最多 100 轮，见任务说明 8.1 第 11 条）。
    pub turns: Vec<TurnResult>,
    /// 本运行一共完成过多少轮，含被截掉的。
    pub turns_total: u64,
}

/// 一次性模型查询的投影，不复制输入、输出或完整元数据。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunModelSummary<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_error: Option<&'a UpstreamError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_turn_id: Option<&'a str>,
    pub run_id: &'a str,
    pub thread_id: &'a str,
    pub review_thread_id: Option<&'a str>,
    pub state: RunState,
    pub started_at: u64,
    pub creation_order: Option<CreationOrder>,
    pub start_confirmation: &'a StartConfirmation,
    pub meta: Option<ModelMeta<'a>>,
    pub current_turn_id: Option<&'a str>,
    pub last_turn: Option<ModelTurn<'a>>,
    pub pending_reroute: Option<&'a PendingReroute>,
}

#[derive(Debug, Serialize)]
pub struct ModelMeta<'a> {
    pub source: Option<&'a str>,
    pub model: Option<&'a str>,
    pub effort: Option<&'a str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelTurn<'a> {
    pub turn_id: &'a str,
    pub reroute: Option<&'a Reroute>,
}

impl RunRecord {
    pub fn model_summary(&self) -> RunModelSummary<'_> {
        RunModelSummary {
            upstream_error: self.upstream_error.as_ref(),
            final_turn_id: self.final_turn_id.as_deref(),
            run_id: &self.run_id,
            thread_id: &self.thread_id,
            review_thread_id: self.review_thread_id.as_deref(),
            state: self.state,
            started_at: self.started_at,
            creation_order: self.creation_order,
            start_confirmation: &self.start_confirmation,
            meta: self.meta.as_ref().map(|meta| ModelMeta {
                source: meta.get("source").and_then(Value::as_str),
                model: meta.get("model").and_then(Value::as_str),
                effort: meta.get("effort").and_then(Value::as_str),
            }),
            current_turn_id: self.current_turn_id.as_deref(),
            last_turn: self.selected_parent_result().map(|turn| ModelTurn {
                turn_id: &turn.turn_id,
                reroute: turn.reroute.as_ref(),
            }),
            pending_reroute: self.pending_reroute.as_ref(),
        }
    }
}

/// threads.json 的一项：线程索引中需要持久化的部分。2.3 节 ThreadSummary 的运行时字段
/// （state、owner、turnId、goalActive、pendingCount）由 M6 在查询时从内存状态拼出，不落盘。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadIndexEntry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relation_kind: Option<crate::relations::RelationKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_summary: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub can_accept_direct_input: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_version: Option<crate::relations::AgentVersion>,

    pub thread_id: String,
    pub short_id: ShortId,
    pub last_run_id: Option<RunId>,
    /// 毫秒时间戳；30 天清理按它判断（S1-评审.md 第 20 节）。
    pub updated_at: u64,
    pub model: Option<String>,
    pub preview: Option<String>,
    pub cwd: Option<String>,
    pub meta: Option<Value>,
    /// broker 自发恢复时补传；旧索引缺省为空，索引重建不猜测覆盖。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_overrides: Option<ResumeOverrides>,
}

/// 只接受两种已批准的恢复覆盖；显式 null 字段与未知键均拒绝。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResumeOverrides {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_value"
    )]
    pub config: Option<serde_json::Map<String, Value>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_value"
    )]
    pub developer_instructions: Option<String>,
}

fn present_value<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

/// shortId → 索引项。用 BTreeMap 保证写盘内容稳定。
pub type ThreadIndex = BTreeMap<ShortId, ThreadIndexEntry>;

/// threads.json 的 payload。`removed` 是明确删除的线程（threadId → 删除时刻毫秒）：
/// 启动时用 runs 重建索引要跳过它们，否则 archive、delete 删掉的项会被近期运行记录重建回来；
/// 删除标记保留 30 天（与运行记录同一保留期），之后随清理删掉。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadIndexFile {
    pub version: u64,
    pub threads: ThreadIndex,
    pub removed: BTreeMap<String, u64>,
    /// 子线程曾属于哪些运行；只恢复历史事实，不恢复任何管理权。
    /// 可读兼容，不保证无损降级：旧版全量写回会丢弃 childHistory；新版再次恢复时
    /// 按历史证据缺失处理，仅从仍保留的运行记录补回正向事实，不推断没有历史。
    #[serde(default)]
    pub child_history: super::child_history::ChildHistory,
    /// purge 的文件清单随删除标记保留，供部分失败重试与跨重启幂等报告。
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub purges: BTreeMap<String, PurgePlan>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PurgePlan {
    pub threads: std::collections::BTreeSet<String>,
    pub files: std::collections::BTreeSet<String>,
    /// 本次扫描的待删运行，用于整体终态检查；重试时重新扫描，不写进删除清单。
    #[serde(skip)]
    pub runs: BTreeMap<RunId, PurgeRun>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PurgeRun {
    pub thread_id: String,
    pub review_thread_id: Option<String>,
    pub state: Option<RunState>,
    pub stop_outcome: Option<StopOutcome>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PurgeReport {
    pub thread_id: String,
    pub deleted: Vec<String>,
    pub already_deleted: Vec<String>,
    pub failed: Vec<PurgeFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PurgeFailure {
    pub path: String,
    pub reason: String,
}

/// 2.3 节 RunMeta：`broker/threads/annotate` 登记的、只有 CLI 才知道的元数据。
/// 内存与运行记录里仍以 JSON 保存（`RunRecord.meta`、`ThreadRecord.pending_meta`），本类型用于 annotate 的参数校验。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunMeta {
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approvals_reviewer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal: Option<Value>,
    pub source: RunSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RunSource {
    Run,
    Review,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreationOrder {
    pub epoch: u64,
    pub sequence: u64,
}

/// 服务器本轮实际改道，仅用于显示，不覆盖登记的请求模型。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Reroute {
    pub from_model: String,
    pub to_model: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingReroute {
    pub turn_id: String,
    #[serde(flatten)]
    pub reroute: Reroute,
}

/// 开轮证据与归属定案分开保存，允许两者以任意顺序到达。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartConfirmation {
    pub confirmed: bool,
    /// null：尚未定案；true：自有；false：被吸收/旁观。
    pub owned: Option<bool>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
}

/// 2.3 节 ThreadSummary 的 state。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SummaryState {
    Starting,
    Running,
    Awaiting,
    Stopping,
    Idle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SummaryOwner {
    Client,
    Broker,
}

/// 2.3 节 ThreadSummary：`cxb ls`、`show`、`wait` 共用的唯一定义。持久部分来自 [`ThreadIndexEntry`]，
/// 运行时部分（state、owner、turnId、goalActive、pendingCount）由丙路在查询时从认领、停止操作、inbox 拼出，不落盘。
/// state 为 idle 时 owner 为 null；owner 已断开、正在停止时 state 为 stopping、owner 为 null。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadSummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<Value>,
    pub thread_id: String,
    pub short_id: ShortId,
    pub state: SummaryState,
    pub owner: Option<SummaryOwner>,
    pub turn_id: Option<String>,
    pub goal_active: bool,
    pub pending_count: u64,
    pub last_run_id: Option<RunId>,
    pub updated_at: u64,
    pub model: Option<String>,
    pub preview: Option<String>,
    pub cwd: Option<String>,
    pub meta: Option<Value>,
}
