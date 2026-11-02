/** SA1b 冻结的增量 JSON；缺省表示旧记录或尚未启用，不推造历史事实。 */
export type TurnOrigin = "user" | "goal" | "subagentReconcile";
export type JoinPhase = "running" | "waitingSubtree" | "preparingReconcile" | "reconciling" | "finalCheck" | "released" | "stopping" | "abnormal";
export type TerminalReason = "parentFailed" | "parentInterrupted" | "userStop" | "ownerGone"
  | "budgetExhausted" | "budgetIncomplete" | "convergeDeadline" | "wakeLimit" | "autoTurnLimit" | "noAction"
  | "discoveryIncomplete" | "ownershipConflict" | "wakeUnknown" | "wakeFailed" | "goalPaused" | "goalBlocked"
  | "goalBudgetLimited" | "goalUsageLimited" | "rootNoDirectInput" | "abandoned" | "brokerShutdown"
  | "stallTimeout" | "upstreamAccessError";
export type Sa1bInterruptReason = "budgetExhausted" | "budgetIncomplete" | "convergeDeadline" | "wakeLimit"
  | "autoTurnLimit" | "discoveryIncomplete" | "ownershipConflict" | "wakeUnknown" | "wakeFailed"
  | "goalInactive" | "rootNoDirectInput" | "abandoned" | "stallTimeout" | "upstreamAccessError";
export type Sa1bTreeTrigger = "budgetExhausted" | "budgetIncomplete" | "convergeDeadline" | "wakeLimit"
  | "autoTurnLimit" | "discoveryIncomplete" | "ownershipConflict" | "wakeUnknown" | "wakeFailed"
  | "goalPaused" | "goalBlocked" | "goalBudgetLimited" | "goalUsageLimited" | "rootNoDirectInput"
  | "stallTimeout" | "upstreamAccessError";

export interface ActivityKey { threadId: string; activitySeq: number; turnId?: string }
export interface ResultLocator { threadId: string; turnId?: string; resultVersion: number }
export interface ResultLimits { totalBytes: number; activityBytes: number; readTimeoutMs: number; maxConcurrentReads: number }
export interface ResultCoverage {
  epoch: number; total: number; available: number; truncated: number; unavailable: number; omitted: number;
}
export interface ReconcileItem {
  key: ActivityKey; parentThreadId: string;
  status: "completed" | "failed" | "interrupted" | "shutdown" | "unavailable";
  output: string; outputSource: "review" | "finalAnswer" | "accumulated" | "none";
  locator: ResultLocator; truncated: boolean; unavailableReason?: string;
}
export interface SubagentJoin {
  phase: JoinPhase; wakeupsStarted: number; wakeupAttempts: number; startRetriesUsed: number;
  workEpoch: number; deliveredEpoch: number; reconciledEpoch: number;
  lastReconcileTurnId?: string; deadlineAt: number; terminalReason?: TerminalReason;
  resultCoverage?: ResultCoverage; automaticControlRevoked?: boolean;
}
export interface BudgetWatermark {
  key: ActivityKey; tokens: number; evidenceVersion: number; goalCoveredTokens: number;
}
export interface RunBudget {
  tokenBudget: number; usedAtStart: number; tokensCharged: number;
  coverage: "complete" | "incomplete" | "unknown"; exhausted: boolean;
  source: "goalAggregate" | "activityLedger"; goalId?: string; goalAggregateWatermark: number;
  activities?: BudgetWatermark[];
}
export interface ConvergenceConfig {
  maxWakeups: number; maxStartRetries: number; maxAutoTurns: number; maxNoActionTurns: number;
  timeoutMs: number; discoveryTimeoutMs: number; discoveryBackoffMs: [number, number, number];
  discoveryMaxIntervalMs: number; wakeVerifyTimeoutMs: number; resultLimits: ResultLimits;
}
export interface RunExecutionConfig { model?: string; effort?: string; turnParams?: unknown }
export interface ConvergenceSummary {
  subagentJoin?: SubagentJoin; runBudget?: RunBudget; finalTurnId?: string; autoTurnsStarted?: number;
  convergenceDiagnostic?: ConvergenceDiagnostic | null;
}
/** 在线只读快照；没有正文。null 明确表示没有当前诊断，不能沿用旧快照。 */
export interface ConvergenceDiagnostic {
  runId: string; collectedAt: number; revision: number; connectionGeneration: number;
  workEpoch: number; coveredEpoch: number | null; gate: string; gates: string[];
  counts: {
    pendingOperations: number; pendingRequests: number; unresolvedLookups: number; unresolvedCandidates: number;
    conflicts: number; activities: number; unverifiedActivities: number; unsettledActivities: number;
    missingResults: number; activeGoals: number; unknownGoals: number;
  };
  pendingOperations: Array<{
    kind: string; outcome: string; receivers: string[];
    provenTurns: Array<{ threadId: string; turnIds: string[]; omittedTurns: number }>;
    expiresAt: number | null; expired: boolean; missing: boolean; omittedReceivers: number; omittedProvenTurns: number;
  }>;
  activities: Array<{
    threadId: string; turnId: string | null; activitySeq: number; verified: boolean; activity: string;
    resultStatus: string | null; goal: boolean | "unknown"; pendingRequests: number;
  }>;
  omitted: { pendingOperations: number; activities: number; identifierBytes: number; gates: number };
}
/** phase 单独变化也发送；revision 与 childChanged、watch 快照共享同一运行序列。 */
export interface RunConvergenceChanged extends ConvergenceSummary {
  runId: string; rootThreadId: string; revision: number;
}
export interface ConvergenceWatchFields extends ConvergenceSummary { revision: number }
