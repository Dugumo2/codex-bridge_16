import type { StopDiagnostics } from "./contracts";

const STOP_CHECK_FIELDS = ["ownership", "turnEnded", "goalBraked", "settled", "verifyDone"] as const;

/** 按停止契约的优先级解释根停止阶段；只列明确为 false 的检查项。 */
export function stopStageDescription(diagnostics?: StopDiagnostics | null, stopCheck?: unknown): string {
  const check = stopCheck !== null && typeof stopCheck === "object" && !Array.isArray(stopCheck)
    ? stopCheck as Record<string, unknown> : {};
  const missing = STOP_CHECK_FIELDS.filter((field) => check[field] === false).join("、");
  if (!diagnostics?.interruptSent) return `尚未发出 interrupt${missing ? `（未满足：${missing}）` : ""}`;
  if (diagnostics.interruptReply === "error" || diagnostics.interruptReply === "timeout") return "interrupt 请求失败或超时";
  if (check.turnEnded === false) return "已发出 interrupt，尚未收到轮次结束证据";
  return `未满足：${missing}`;
}
