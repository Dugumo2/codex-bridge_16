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
    pub rejected_receivers: BTreeMap<String, String>,
    pub proven_turns: BTreeMap<String, BTreeSet<String>>,
    /// 操作登记瞬间每个接收方已完成轮次的基线；配对只排除这些轮次。
    pub baseline_turns: BTreeMap<String, BTreeSet<String>>,
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
