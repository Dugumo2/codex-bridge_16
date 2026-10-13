import type { BrokerRpc, RunResult, UpstreamError, UpstreamErrorCategory } from "./contracts";
import { resultTurn } from "./run-result";
import { redactParams } from "./server-requests";

const labels: Record<UpstreamErrorCategory, string> = {
  authentication: "上游认证失败", paymentRequired: "上游要求付费", permissionDenied: "上游拒绝访问",
  usageLimit: "上游用量已达上限", rateLimit: "上游限流", server: "上游服务器错误", network: "上游连接失败", unknown: "上游报错",
};
function object(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}
export function codexErrorVariant(info: unknown): string {
  if (typeof info === "string") return info;
  const keys = Object.keys(object(info));
  return keys.length === 1 ? keys[0]! : "";
}
/** 只取结构化 HTTP 状态，不从错误正文猜测。 */
function httpStatus(info: unknown): number | null {
  const variant = codexErrorVariant(info);
  const status = object(object(info)[variant]).httpStatusCode;
  return Number.isInteger(status) && Number(status) >= 0 && Number(status) <= 65535 ? Number(status) : null;
}
/** 字符数按 Unicode 码点计，避免截断代理对；原因始终只占一行。 */
export function diagnosticLine(value: unknown): string {
  const text = typeof value === "string" ? value : value == null ? "" : JSON.stringify(value);
  const clean = String(redactParams(text)).replace(/[\r\n\t]+/g, " ").replace(/\s+/gu, " ").trim();
  const chars = [...clean];
  return chars.length > 300 ? chars.slice(0, 300).join("") + "…" : clean;
}
function statusNote(error: Pick<UpstreamError, "httpStatusCode" | "codexErrorInfo">): string {
  if (error.httpStatusCode !== null) return `（HTTP ${error.httpStatusCode}）`;
  const variant = codexErrorVariant(error.codexErrorInfo);
  return variant ? `（${diagnosticLine(variant)}）` : "";
}
export function upstreamLabel(error: UpstreamError): string {
  return `${labels[error.category] ?? labels.unknown}${statusNote(error)}`;
}
function reason(error: UpstreamError): string { return diagnosticLine(error.additionalDetails?.trim() ? error.additionalDetails : error.message); }
export function upstreamNoticeLines(error: UpstreamError, ended = false): string[] {
  const label = upstreamLabel(error);
  const first = ended ? `[cxb] 最近上游报错：${label}，共观察到 ${error.count} 次（运行已结束）。`
    : error.willRetry === true ? `[cxb] ${label}，Codex 正在重试；已观察到 ${error.count} 次。`
      : error.willRetry === false ? `[cxb] ${label}，上游已报告终止错误，等待轮次结束确认。` : `[cxb] ${label}。`;
  return [first, `[cxb] 原因：${reason(error)}`];
}
export function upstreamShowLines(run: Pick<RunResult, "state" | "upstreamError">): string[] {
  const error = run.upstreamError;
  if (!error) return [];
  const status = run.state === "ended" || run.state === "lost" ? "运行已结束"
    : error.willRetry === true ? "仍在重试" : error.willRetry === false ? "已报告终止错误" : "重试状态未知";
  return [`最近上游报错：${upstreamLabel(error)}；${error.count} 次；首次 ${new Date(error.firstSeenAt).toISOString()}，最近 ${new Date(error.lastSeenAt).toISOString()}；${status}`,
    `原因：${reason(error)}`];
}
/** 与 broker 的分组键相同：只能移除整段 Reconnecting... n/m 提示。 */
export function upstreamGroupKey(error: UpstreamError): string {
  const body = (error.additionalDetails?.trim() ? error.additionalDetails : error.message).trim();
  const normalized = /^Reconnecting\.\.\. [0-9]+\/[0-9]+$/.test(body) ? "" : body;
  return JSON.stringify([error.threadId, error.turnId, codexErrorVariant(error.codexErrorInfo), error.category, error.httpStatusCode, normalized]);
}
export function turnErrorLine(error: unknown): string {
  const value = object(error);
  const info = value.codexErrorInfo ?? object(value.data).codexErrorInfo ?? null;
  return `${diagnosticLine(value.message ?? error)}${statusNote({ httpStatusCode: httpStatus(info), codexErrorInfo: info })}`;
}
/** 新字段缺省不输出，选定最终轮的错误不能被旧轮或子线程替代。 */
export function upstreamFields(run: Pick<RunResult, "upstreamError">) {
  const error = run.upstreamError;
  return error ? { upstreamError: { ...error, message: String(redactParams(error.message)),
    additionalDetails: error.additionalDetails === null ? null : String(redactParams(error.additionalDetails)) } } : {};
}
export function upstreamResultFields(run: RunResult) {
  const error = resultTurn(run)?.error;
  return { ...upstreamFields(run), ...(error != null ? { error: redactParams(error) } : {}) };
}

type View = Pick<Partial<RunResult>, "upstreamError" | "diagnosticRevision" | "lastProgressAt" | "state" | "status"> & { runId?: string };
type Diagnostic = { revision: number; fields: Pick<Partial<RunResult>, "upstreamError" | "diagnosticRevision" | "lastProgressAt"> };

/** 先订阅、后快照；诊断版本与最终失败摘要分别去重，彼此不能吞掉。 */
export function observeUpstreamErrors(rpc: BrokerRpc, runId: () => string | null | undefined, notice: (line: string) => void,
  now: () => number = Date.now) {
  let active = true;
  const buffered: View[] = [];
  const views = new Map<string, Diagnostic>();
  const printed = new Map<string, { key: string; retry: boolean | null; at: number; ended: boolean }>();
  const failures = new Set<string>();
  const display = (view: View, forceEnded = false) => {
    if (!view.runId || !view.upstreamError) return;
    const error = view.upstreamError;
    const key = upstreamGroupKey(error);
    const ended = forceEnded || view.state === "ended" || view.state === "lost";
    const previous = printed.get(view.runId);
    if (previous && previous.key === key && previous.retry === error.willRetry && previous.ended === ended
      && (ended || now() - previous.at < 30_000)) return;
    printed.set(view.runId, { key, retry: error.willRetry, at: now(), ended });
    for (const line of upstreamNoticeLines(error, ended)) notice(line);
  };
  const acceptKnown = (view: View) => {
    const id = runId();
    if (!active || !id || view.runId !== id) return;
    const revision = view.diagnosticRevision ?? 0;
    if (!Number.isSafeInteger(revision) || revision < 0) return;
    const previous = views.get(id);
    if (previous && revision <= previous.revision) return;
    const fields = { upstreamError: view.upstreamError, diagnosticRevision: revision, lastProgressAt: view.lastProgressAt };
    views.set(id, { revision, fields });
    display(view);
  };
  const refresh = () => {
    if (!active || !runId()) return;
    for (const view of buffered.splice(0)) acceptKnown(view);
  };
  const accept = (view: View) => {
    if (!active || !view.runId) return;
    if (!runId()) { buffered.push(view); if (buffered.length > 64) buffered.shift(); return; }
    refresh();
    acceptKnown(view);
  };
  const merge = (run: RunResult): RunResult => {
    const view = views.get(run.runId);
    return view && view.revision > (run.diagnosticRevision ?? 0) ? { ...run, ...view.fields } : run;
  };
  const apply = (run: RunResult): RunResult => {
    accept(run);
    const result = merge(run);
    // 运行终态不受 diagnosticRevision 去重影响；历史 willRetry 不能冒充当前重试。
    if (run.state === "ended" || run.state === "lost") display(result, true);
    return result;
  };
  const failure = (run: RunResult) => {
    if (!active || run.runId !== runId() || run.status !== "failed" || failures.has(run.runId)) return;
    failures.add(run.runId);
    const error = resultTurn(run)?.error;
    if (error != null) notice(`[cxb] 失败原因：${turnErrorLine(error)}`);
    else display(merge(run), true);
  };
  const off = rpc.on("broker/run/updated", (raw) => { if (raw && typeof raw === "object") accept(raw as View); });
  const watchedRpc: BrokerRpc = { ...rpc, async request<T>(method: string, params?: unknown, options?: Parameters<BrokerRpc["request"]>[2]): Promise<T> {
    const result = await rpc.request<T>(method, params, options);
    if (active && method === "broker/threads/watch" && result && typeof result === "object") {
      const snapshot = result as { run?: RunResult | null };
      if (snapshot.run) return { ...snapshot, run: apply(snapshot.run) } as T;
    }
    return result;
  } };
  return { accept, refresh, apply, failure, rpc: watchedRpc,
    fields: () => upstreamFields(views.get(runId() ?? "")?.fields ?? {}),
    dispose: () => { active = false; off(); buffered.length = 0; views.clear(); printed.clear(); failures.clear(); } };
}
