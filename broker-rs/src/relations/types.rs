//! SA-a 的事实、授权与证据契约。操作/证据期限用单调毫秒；请求的收到/截止时间沿用 inbox 墙钟毫秒。
use crate::claim::{ClientRequest, ConnId, Owner};
use crate::policy::Policy;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RelationKind {
    ReviewExecution,
    Subagent,
    Fork,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AgentVersion {
    V1,
    V2,
    Unknown,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveredBy {
    Started,
    Lookup,
    AncestorList,
    LoadedList,
    SpawnEvent,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationEdge {
    pub child_thread_id: String,
    pub parent_thread_id: String,
    pub kind: RelationKind,
    pub source_summary: Value,
    pub can_accept_direct_input: Option<bool>,
    pub agent_version: AgentVersion,
    pub discovered_by: DiscoveredBy,
}
/// run_id 和逻辑根不变，执行线程随 review 迁移更新。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunIdentity {
    pub run_id: String,
    pub logical_root_thread_id: String,
    pub execution_thread_id: String,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BindingEvidence {
    V1Spawn,
    V1Message,
    V2Started,
    V2Interacted,
    Inherited,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindingStatus {
    Bound,
    Unbound,
    Conflict(String),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityBinding {
    pub thread_id: String,
    pub turn_id: String,
    pub run_id: String,
    pub parent_thread_id: String,
    pub evidence: BindingEvidence,
    pub operation: EvidenceKey,
    pub status: BindingStatus,
}
/// started/completed 共用这个键；不能由接收方线程预约下一轮。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct EvidenceKey {
    pub parent_thread_id: String,
    pub parent_turn_id: String,
    pub item_id: String,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceKind {
    V1Spawn,
    V1SendInput,
    V1ResumeAgent,
    V2Started,
    V2Interacted,
    V2Interrupted,
    V2Completed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceOutcome {
    Pending,
    Proven,
    Unproven,
    Failed,
    Revoked,
}
/// v2 跟进为什么只能宽松配对；在 Interacted 首次到达时按当时已知事实判定。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V2FallbackReason {
    /// 本连接内还没收到过父线程的任何 raw 事件；典型是跨重启 `--resume` 的线程，上游只有 thread/start 有 raw 开关。
    ParentWithoutRaw,
    /// 父线程送过 raw，但这次调用的开始记录没有先于 Interacted 到达（迟到或因上限未登记）。
    StartMissing,
    /// 有开始记录，但连接、运行或授权代际已不符，不能沿用。
    StartStale,
    /// 没有 raw，会话文件的路径拿不到或不合规（thread/read 失败、没有 path、不是本线程的 .jsonl 绝对路径）。
    RolloutPathUnavailable,
    /// 没有 raw，会话文件不存在或读不了（含 app-server 在别的机器上）。
    RolloutUnreadable,
    /// 没有 raw，等文件的窗口内没在父线程文件里找到这次调用（例如嵌套调用不落 function_call）。
    RolloutCallMissing,
    /// 曾按会话文件严格配对，之后文件被截断或读不了，降为宽松。
    RolloutLost,
    /// 等文件期间操作已失效（运行结束、停止、连接或授权换代），不再等。
    RolloutCancelled,
    /// 没有 raw，文件里找到了 followup_task，但发起方的代理路径未知，比对不了接收方的消费记录。
    RolloutSenderUnknown,
    /// 没有 raw，接收方文件的末尾窗口向前找到上限仍定位不到当前轮，读到的消息对不上轮次。
    RolloutTurnUnknown,
}

/// 跟进配对证据的来源，只供诊断。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V2EvidenceSource {
    /// 本连接的 raw 事件。
    Raw,
    /// 会话文件（没有 raw 时）。
    RolloutFile,
    /// 没有证据，宽松配对。
    None,
}

/// 跟进接收方不纳管的理由。向上与平级是子代理之间的正常协作，不是跟进，不能当「跟进被拒」统计。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FollowupRejection {
    /// 接收方是发起方的上级。
    Upward,
    /// 接收方与发起方同属一棵代理树、又不是它的下级。
    Peer,
    /// 未核实为本父线程的子代理（含越级跟进孙代理）。
    NotOwnChild,
    /// 接收方已有独立认领。
    IndependentClaim,
    /// 接收方已绑定其他运行。
    OtherRunBinding,
}

impl FollowupRejection {
    /// 运行记录与诊断用的说明文字。
    pub fn reason(self) -> &'static str {
        match self {
            Self::Upward => "接收方是发起方的上级，不属于跟进",
            Self::Peer => "接收方是发起方的平级，不属于跟进",
            Self::NotOwnChild => "未核实为本父线程的子代理",
            Self::IndependentClaim => "接收方已有独立认领",
            Self::OtherRunBinding => "接收方已绑定其他运行",
        }
    }

    /// 向上或平级消息：不授予管理权，也不是归属问题。
    pub fn is_collaboration(self) -> bool {
        matches!(self, Self::Upward | Self::Peer)
    }
}

/// v1 completed 的代理状态不等于发送成功；v2 双通知只登记一次。
/// 证据最迟于父轮结束、授权撤销、连接换代失效；已绑定轮次保留历史。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceOperation {
    pub key: EvidenceKey,
    pub run_id: String,
    pub conn_epoch: u64,
    pub authority_gen: u64,
    pub kind: EvidenceKind,
    pub started_seen: bool,
    pub completed_seen: bool,
    pub outcome: EvidenceOutcome,
    pub receivers: BTreeSet<String>,
    /// 跟进接收方的拒绝纳管理由；保留上游接收事实，不把拒绝误记成发送失败。
    pub rejected_receivers: BTreeMap<String, FollowupRejection>,
    pub proven_turns: BTreeMap<String, BTreeSet<String>>,
    /// 操作登记瞬间每个接收方已完成轮次的基线；配对只排除这些轮次。
    pub baseline_turns: BTreeMap<String, BTreeSet<String>>,
    /// 在 Interacted 首次到达时固定；缺 raw 的旧线程可等会话文件定案（见 `rollout_pairing`），
    /// 定案后不因迟到的 raw 或文件行改写模式。
    pub v2_strict: bool,
    /// 由 raw 认出是 `send_message`：上游只入队不开新轮，可能永远等不到承载轮，不授予管理权也不进待定账本。
    pub v2_queue_only: bool,
    /// 严格配对的证据来源；与 `v2_strict` 同时固定，只供诊断。
    pub v2_source: V2EvidenceSource,
    /// 宽松配对的原因，与 `v2_strict` 同时固定；只供诊断，不参与配对、门禁或告警。
    pub v2_fallback: Option<V2FallbackReason>,
    pub first_seen_ms: u64,
    pub expires_at_ms: u64,
    pub parent_ended: bool,
    pub failure: Option<String>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MemberActivity {
    Starting,
    Running,
    Settled,
    Unknown,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberState {
    pub parent: String,
    pub kind: RelationKind,
    pub capability: Option<bool>,
    pub agent_version: AgentVersion,
    pub bindings: Vec<String>,
    pub conflict: Option<String>,
    pub activity: MemberActivity,
    pub pending_requests: usize,
    pub active_goal: Option<bool>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Authority {
    Owner(ConnId),
    Broker { policy: Policy },
    Cleanup { tree_op: u64 },
    Revoked,
}
/// ended 后撤销 owner 回答权；只保留既定停止范围的清理权，不允许启动新工作。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupContext {
    pub tree_op: u64,
    pub gen: u64,
    pub policy: Policy,
    pub scope: BTreeSet<String>,
    pub bindings: Vec<ActivityBinding>,
    pub subscriptions: BTreeMap<String, AttachmentEvidence>,
    pub barrier: bool,
    pub committed: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManageContext {
    pub identity: RunIdentity,
    pub gen: u64,
    pub authority: Authority,
    pub policy: Policy,
    pub members: BTreeMap<String, MemberState>,
    pub in_flight_spawns: BTreeSet<EvidenceKey>,
    pub revision: u64,
    pub discovery: DiscoveryState,
    pub cleanup: Option<CleanupContext>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryState {
    #[serde(default)]
    pub missing: BTreeMap<String, Vec<String>>,
    pub complete: bool,
    pub version: u64,
    pub incomplete_reasons: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttachmentSource {
    ThreadStarted,
    ThreadEvent,
    ServerRequest,
    ExplicitResume,
}
/// 只证明本 epoch 本线程被观察；断线、退订、卸载、已知缺口使证据失效。
/// ExplicitResume 仅记录既有恢复回包；SA1a 不主动 resume 补监听。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentEvidence {
    pub thread_id: String,
    pub conn_epoch: u64,
    pub source: AttachmentSource,
    pub observed_at_ms: u64,
    pub revision: u64,
    pub valid: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubtreeView {
    pub members: BTreeMap<String, MemberState>,
    pub discovery: DiscoveryState,
    pub revision: u64,
    pub gen: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Activity {
    None,
    Some,
    Unknown,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandoffKind {
    Detach,
    OwnerGone,
    Joined,
    External,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberChange {
    ResultReady,
    ConvergencePhase,
    Budget,
    Relation,
    Binding,
    Activity,
    Request,
    Capability,
    Conflict,
    Discovery,
    Goal,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoveryScope {
    Run { run_id: String },
    Subtree,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoveryCaller {
    Converge {
        token: crate::sa1b::ConvergeToken,
        work_epoch: u64,
    },
    TreeStop {
        tree_op: u64,
        round: u64,
    },
    FinishGate {
        run_id: String,
        claim_id: u64,
    },
    Lifecycle {
        request: ClientRequest,
        attempt: u64,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryResult {
    pub root: String,
    pub scope: DiscoveryScope,
    pub generation: u64,
    pub members: BTreeMap<String, RelationEdge>,
    pub state: DiscoveryState,
    pub attachments: BTreeMap<String, AttachmentEvidence>,
    pub cancelled: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryOperation {
    pub op_id: u64,
    pub generation: u64,
    pub root: String,
    pub scope: DiscoveryScope,
    pub caller: DiscoveryCaller,
    pub started_at_ms: u64,
    pub deadline_ms: u64,
    pub completed: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChildRequestStage {
    Lookup,
    Held,
    Forwarded {
        forward_id: String,
        conn: ConnId,
        gen: u64,
    },
    Inbox {
        item_id: String,
    },
    Writing {
        op: u64,
        gen: u64,
    },
    Answered,
    Revoked,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildRequest {
    pub conn_epoch: u64,
    pub upstream_request_id: Value,
    pub method: String,
    pub params: Option<Value>,
    pub source_thread_id: String,
    pub source_turn_id: Option<String>,
    pub run_id: Option<String>,
    pub received_at_ms: u64,
    pub deadline_ms: u64,
    pub stage: ChildRequestStage,
}
/// 冻结停止期间的策略输入，不借用父线程普通转发规则。
/// 私有/共享的已绑定审批均按冻结 policy 应答；人工事项进 inbox，禁止新转发。
/// 动态工具：私有回 -32601，共享静默不答。Forwarded 撤回并保留原期限。
/// Writing 以实际写出确认为提交边界；ended 撤销旧 owner，后续只走受限清理路由。
/// 未证明归属或外部接管时不答，不使用 Dismiss 清除上游去重登记。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupRequestPolicy {
    pub shared: bool,
    pub policy: Policy,
    pub authority_gen: u64,
    pub owner_before_stop: Option<Owner>,
    pub tree_op: u64,
    pub ended: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChildRecord {
    pub thread_id: String,
    pub parent_thread_id: String,
    pub relation_kind: RelationKind,
    pub turn_ids: Vec<String>,
    pub evidence: BindingEvidence,
    pub final_state: MemberActivity,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CxbOrigin {
    pub root_thread_id: String,
    pub run_id: String,
    pub parent_thread_id: String,
    pub relation_kind: RelationKind,
    pub source_thread_id: String,
}
