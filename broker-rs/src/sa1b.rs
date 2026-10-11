//! SA1b 冻结契约。声明可用，自动收敛尚未接线；实施分工见仓库外的 sa1b-接口.md。
//! 持久化摘要不是管理凭据；重启不得从这些字段恢复自动控制权。

use crate::claim::ClaimToken;
use crate::relations::{DiscoveryState, MemberActivity};
use crate::runs::{InterruptReason, OutputSource, RunStatus, TurnStatus};
use crate::stop::tree::TreeTrigger;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// 同时校验运行、认领、管理授权、连接及协调器代号；任一变化均使旧操作失效。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConvergeToken {
    pub run_id: String,
    pub claim: ClaimToken,
    pub manage_gen: u64,
    pub conn_epoch: u64,
    pub generation: u64,
}

/// 活动身份以线程及 activity_seq 为准；turn_id 是后补定位，不产生新活动。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivityKey {
    pub thread_id: String,
    pub activity_seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ActivityResultStatus {
    Completed,
    Failed,
    Interrupted,
    Shutdown,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultLocator {
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    /// 结果事实版本；不同于分页读取的页代号。
    pub result_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityFact {
    pub key: ActivityKey,
    pub parent_thread_id: String,
    pub activity: MemberActivity,
    pub result_status: Option<ActivityResultStatus>,
    pub verified: bool,
    pub active_goal: Option<bool>,
    pub pending_requests: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultRef {
    pub key: ActivityKey,
    pub locator: ResultLocator,
    pub available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConvergenceFacts {
    pub token: ConvergeToken,
    pub work_epoch: u64,
    pub revision: u64,
    pub discovery: DiscoveryState,
    pub discovery_covered_epoch: Option<u64>,
    pub activities: Vec<ActivityFact>,
    /// 包含父轮结束后已失去绑定授予权、但尚未核实落定的派生。
    pub pending_starts: usize,
    pub pending_requests: usize,
    pub unresolved_lookups: usize,
    pub conflicts: Vec<String>,
    pub result_refs: Vec<ResultRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BatchReadId(pub u64);

impl BatchReadId {
    /// 骨架没有调度读取；零号不得等待回调、推进覆盖或视为成功。
    pub const DISABLED: Self = Self(0);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReconcileOpId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ResultLimits {
    pub total_bytes: usize,
    pub activity_bytes: usize,
    pub read_timeout_ms: u64,
    pub max_concurrent_reads: usize,
}

impl Default for ResultLimits {
    fn default() -> Self {
        Self {
            total_bytes: 64 * 1024,
            activity_bytes: 8 * 1024,
            read_timeout_ms: 20_000,
            max_concurrent_reads: 4,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ResultCoverage {
    pub epoch: u64,
    pub total: usize,
    pub available: usize,
    pub truncated: usize,
    pub unavailable: usize,
    pub omitted: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReconcileItem {
    pub key: ActivityKey,
    pub parent_thread_id: String,
    pub status: ActivityResultStatus,
    pub output: String,
    pub output_source: OutputSource,
    pub locator: ResultLocator,
    pub truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unavailable_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileBatch {
    pub work_epoch: u64,
    pub items: Vec<ReconcileItem>,
    /// 最终将提交的完整 UTF-8 输入，包含提示、来源、截断标记、有界遗漏清单。
    pub input: String,
    pub total_bytes: usize,
    pub omitted: usize,
    pub coverage: ResultCoverage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileRefusal {
    Disabled,
    StaleToken,
    Stopping,
    Abandoned,
    AlreadyInFlight,
    AlreadyDelivered,
    WorkChanged,
    DiscoveryIncomplete,
    OwnershipConflict,
    RootNoDirectInput,
    CapabilityUnknown,
    GoalInactive,
    BudgetExhausted,
    BudgetIncomplete,
    WakeLimit,
    AutoTurnLimit,
    Deadline,
    InvalidBatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TurnOrigin {
    User,
    Goal,
    SubagentReconcile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum JoinPhase {
    Running,
    WaitingSubtree,
    PreparingReconcile,
    Reconciling,
    FinalCheck,
    Released,
    Stopping,
    Abnormal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TerminalReason {
    ParentFailed,
    ParentInterrupted,
    UserStop,
    OwnerGone,
    BudgetExhausted,
    BudgetIncomplete,
    ConvergeDeadline,
    WakeLimit,
    AutoTurnLimit,
    NoAction,
    DiscoveryIncomplete,
    OwnershipConflict,
    WakeUnknown,
    WakeFailed,
    GoalPaused,
    GoalBlocked,
    GoalBudgetLimited,
    GoalUsageLimited,
    RootNoDirectInput,
    Abandoned,
    BrokerShutdown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubagentJoin {
    pub phase: JoinPhase,
    pub wakeups_started: u32,
    pub wakeup_attempts: u32,
    pub start_retries_used: u32,
    pub work_epoch: u64,
    pub delivered_epoch: u64,
    pub reconciled_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reconcile_turn_id: Option<String>,
    /// 仅用于展示；期限判定使用 ConvergeRuntime.deadline_mono_ms。
    pub deadline_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_reason: Option<TerminalReason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_coverage: Option<ResultCoverage>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub automatic_control_revoked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ConvergenceConfig {
    pub max_wakeups: u32,
    pub max_start_retries: u32,
    pub max_auto_turns: u32,
    pub max_no_action_turns: u32,
    pub timeout_ms: u64,
    pub discovery_timeout_ms: u64,
    pub discovery_backoff_ms: [u64; 3],
    pub discovery_max_interval_ms: u64,
    pub wake_verify_timeout_ms: u64,
    pub result_limits: ResultLimits,
}

impl Default for ConvergenceConfig {
    fn default() -> Self {
        Self {
            max_wakeups: 5,
            max_start_retries: 2,
            max_auto_turns: 50,
            max_no_action_turns: 3,
            timeout_ms: 3_600_000,
            discovery_timeout_ms: 20_000,
            discovery_backoff_ms: [5_000, 15_000, 30_000],
            discovery_max_interval_ms: 60_000,
            wake_verify_timeout_ms: 20_000,
            result_limits: ResultLimits::default(),
        }
    }
}

/// 创建运行时保存的实际执行配置；None 表示未知，不准用新 CLI 默认值补齐。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct RunExecutionConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    /// 已解析且允许用于 turn/start 的执行参数白名单；不包含输入、线程、触发器或身份标识。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn_params: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BudgetCoverage {
    Complete,
    Incomplete,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BudgetSource {
    GoalAggregate,
    ActivityLedger,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BudgetWatermark {
    pub key: ActivityKey,
    /// 此活动累计的输入减缓存输入加输出，不是 totalTokens。
    pub tokens: u64,
    pub evidence_version: u64,
    /// 根 goal 汇总中已覆盖的本活动水位；不能再次计费。
    pub goal_covered_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunBudget {
    pub token_budget: u64,
    pub used_at_start: u64,
    pub tokens_charged: u64,
    pub coverage: BudgetCoverage,
    pub exhausted: bool,
    pub source: BudgetSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_id: Option<String>,
    pub goal_aggregate_watermark: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub activities: Vec<BudgetWatermark>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetEvidence {
    pub key: ActivityKey,
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub output_tokens: u64,
    pub evidence_version: u64,
    pub goal_covered_tokens: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalBudgetEvidence {
    pub goal_id: String,
    pub tokens_used: u64,
    pub evidence_version: u64,
    /// 无法证明与逐活动账本的重叠范围时为 None，不能直接相加。
    pub covered_activities: Option<Vec<BudgetWatermark>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileTransportEvent {
    TimedOut,
    TransportFailed,
    LateDiscarded,
    LateEvicted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetAdmission {
    Allowed,
    Exhausted,
    NeedsVerification,
    Disabled,
}

/// 已发出之后的取消只撤销自动资格，不抹掉待核实的副作用。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileSubmission {
    NotSent,
    DefinitelyNotSubmitted,
    OutcomeUnknown,
    ConfirmedOwnTurn,
    MergedOwnGoalTurn,
    MergedForeignOrUnknownTurn,
    CancelledBeforeSubmit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileOperation {
    pub token: ConvergeToken,
    pub work_epoch: u64,
    pub op_id: ReconcileOpId,
    pub client_user_message_id: String,
    pub expected_previous_turn_id: Option<String>,
    pub target_turn_id: Option<String>,
    pub submission: ReconcileSubmission,
    pub outcome_unknown: bool,
    pub automatic_control_revoked: bool,
    pub verify_deadline_mono_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileCancel {
    Stop,
    OwnerGone,
    Shutdown,
    Abandon,
    UserInput,
    GoalInactive,
    Deadline,
    AuthorityChanged,
}

/// 仅内存中的登记可授信；持久化的文本、origin 或 clientId 本身不能授信。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustedInputRegistration {
    pub token: ConvergeToken,
    pub op_id: ReconcileOpId,
    pub client_user_message_id: String,
    pub work_epoch: u64,
    pub expected_turn_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputSource {
    ExternalUser,
    TrustedReconcile,
    NotUserMessage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalInput {
    pub reason: TerminalReason,
    pub parent_status: Option<TurnStatus>,
    pub existing_interrupt: Option<InterruptReason>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalDisposition {
    pub status: RunStatus,
    pub interrupt_reason: Option<InterruptReason>,
    pub terminal_reason: TerminalReason,
    pub tree_trigger: Option<TreeTrigger>,
    pub exit_code: u8,
}

/// watch 快照和 phase 变化通知共用的增量字段；缺省时不写，保持旧 JSON 不变。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ConvergenceSummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subagent_join: Option<SubagentJoin>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_budget: Option<RunBudget>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_turn_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_turns_started: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claim::{Action, Machine};
    use serde_json::json;

    fn token() -> ConvergeToken {
        ConvergeToken {
            run_id: "运行一".into(),
            claim: ClaimToken {
                thread_id: "父线程".into(),
                claim_id: 1,
            },
            manage_gen: 2,
            conn_epoch: 3,
            generation: 4,
        }
    }

    #[test]
    fn legacy_record_roundtrip_does_not_invent_sa1b_fields() {
        let record = crate::store::tests::run("旧运行", "旧线程", 1);
        let value = serde_json::to_value(&record).unwrap();
        for field in [
            "finalTurnId",
            "autoTurnsStarted",
            "subagentJoin",
            "runBudget",
            "convergenceConfig",
            "executionConfig",
        ] {
            assert!(value.get(field).is_none(), "旧记录不应写出 {field}");
        }
        let restored: crate::runs::RunRecord = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(restored).unwrap(), value);
        assert!(serde_json::to_value(record.model_summary())
            .unwrap()
            .get("finalTurnId")
            .is_none());
    }

    #[test]
    fn legacy_turn_origin_stays_absent() {
        let value = json!({"turnId":"旧轮", "ready":"ready", "status":"completed", "output":"旧结果",
            "outputSource":"finalAnswer", "error":null, "completedAt":1});
        let turn: crate::runs::TurnResult = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(turn.origin, None);
        assert_eq!(serde_json::to_value(turn).unwrap(), value);
    }

    #[test]
    fn configured_limits_match_approved_values_and_partial_defaults() {
        let defaults = ConvergenceConfig::default();
        assert_eq!(
            (
                defaults.max_wakeups,
                defaults.max_start_retries,
                defaults.max_auto_turns,
                defaults.max_no_action_turns
            ),
            (5, 2, 50, 3)
        );
        assert_eq!(defaults.timeout_ms, 3_600_000);
        assert_eq!(defaults.discovery_backoff_ms, [5_000, 15_000, 30_000]);
        assert_eq!(defaults.discovery_max_interval_ms, 60_000);
        assert_eq!(
            (
                defaults.discovery_timeout_ms,
                defaults.wake_verify_timeout_ms,
                defaults.result_limits.read_timeout_ms
            ),
            (20_000, 20_000, 20_000)
        );
        assert_eq!(
            (
                defaults.result_limits.total_bytes,
                defaults.result_limits.activity_bytes,
                defaults.result_limits.max_concurrent_reads
            ),
            (65536, 8192, 4)
        );
        let partial: ConvergenceConfig =
            serde_json::from_value(json!({"timeoutMs":120000})).unwrap();
        assert_eq!(
            partial,
            ConvergenceConfig {
                timeout_ms: 120_000,
                ..defaults
            }
        );
    }

    #[test]
    fn disabled_entrypoints_never_schedule_or_mutate_machine() {
        let mut machine = Machine::default();
        let before = machine.clone();
        let mut actions = Vec::new();
        assert_eq!(machine.convergence_facts("缺失运行"), None);
        assert_eq!(
            machine.convergence_release_gate("父线程", None, None, &mut actions),
            None
        );
        assert_eq!(
            machine.read_child_results(token(), 1, ResultLimits::default(), &mut actions),
            BatchReadId::DISABLED
        );
        let batch = ReconcileBatch {
            work_epoch: 1,
            items: vec![],
            input: String::new(),
            total_bytes: 0,
            omitted: 0,
            coverage: ResultCoverage::default(),
        };
        assert_eq!(
            machine.begin_subagent_reconcile(token(), batch, &mut actions),
            Err(ReconcileRefusal::Disabled)
        );
        assert_eq!(
            machine.reconcile_budget_admission(&token()),
            BudgetAdmission::Disabled
        );
        assert!(actions
            .iter()
            .all(|action| matches!(action, Action::Warn(_))));
        assert_eq!(machine, before);
    }

    #[test]
    fn unregistered_client_id_or_prompt_never_grants_trust() {
        let mut machine = Machine::default();
        let mut actions = Vec::new();
        let registration = TrustedInputRegistration {
            token: token(),
            op_id: ReconcileOpId(1),
            client_user_message_id: "伪造编号".into(),
            work_epoch: 1,
            expected_turn_id: None,
        };
        assert!(!machine.register_reconcile_input(registration, &mut actions));
        let item = json!({"type":"userMessage", "id":"条目编号", "clientId":"伪造编号", "text":"cxb 运行协调通知"});
        assert_eq!(
            machine.classify_run_input("运行一", "父线程", "轮一", &item),
            InputSource::ExternalUser
        );
        assert!(crate::results::Activity::of(&[item]).user_input);
    }

    #[test]
    fn mixed_real_input_remains_visible_to_guard() {
        let mut activity = crate::results::Activity::default();
        activity.observe_with_source(
            &json!({"type":"userMessage"}),
            InputSource::TrustedReconcile,
        );
        assert!(!activity.user_input);
        activity.observe(&json!({"type":"userMessage"}));
        assert!(activity.user_input);
        assert!(!activity.action);
    }

    #[test]
    fn convergence_timer_keys_isolate_generations_and_operations() {
        let first = token();
        let mut second = first.clone();
        second.generation += 1;
        use crate::claim::TimerKind;
        assert_ne!(
            TimerKind::ConvergeDeadline {
                token: first.clone()
            }
            .key(&first.claim),
            TimerKind::ConvergeDeadline { token: second }.key(&first.claim)
        );
        assert_ne!(
            TimerKind::ChildResultsDeadline {
                token: first.clone(),
                read_id: BatchReadId(1)
            }
            .key(&first.claim),
            TimerKind::ChildResultsDeadline {
                token: first.clone(),
                read_id: BatchReadId(2)
            }
            .key(&first.claim)
        );
    }

    #[test]
    fn absent_summary_does_not_add_json_fields() {
        assert_eq!(
            serde_json::to_value(ConvergenceSummary::default()).unwrap(),
            json!({})
        );
        let summary = ConvergenceSummary {
            final_turn_id: Some("最终轮".into()),
            auto_turns_started: Some(0),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(summary).unwrap(),
            json!({"finalTurnId":"最终轮", "autoTurnsStarted":0})
        );
    }
}
