import type { ChildUpstreamError, StallReport, StopDiagnostics } from "../contracts";
import { upstreamError } from "./upstream-errors";

export function stall(patch: Partial<StallReport> = {}): StallReport {
  return { timeoutMs: 600_000, phase: "verifying", expiredAt: 2000, idleMs: 601_500,
    lastProgressAt: 1000, verify: null, expiries: 1, ...patch };
}
export function stopDiagnostics(patch: Partial<StopDiagnostics> = {}): StopDiagnostics {
  return { reason: "stallTimeout", startedAt: 2000, goalStartedAt: null, goalSettledAt: null,
    interruptSent: false, interruptSentAt: null, interruptReply: null, interruptRepliedAt: null,
    turnEndedAt: null, unconfirmedAt: null, confirmedAt: null, rpcIssues: [], ...patch };
}
export function childError(threadId = "019a3f2c-child", count = 3): ChildUpstreamError {
  return { threadId, parentThreadId: "thread-test", upstreamError: upstreamError({ threadId, count }) };
}
