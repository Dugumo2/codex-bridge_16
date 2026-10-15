import type { BrokerRpc, RunResult, UpstreamError, UpstreamErrorCategory } from "./contracts";
import { resultTurn } from "./run-result";
import { redactParams } from "./server-requests";
import { createDiagnosticStore, diagnosticFields, isUnconfirmedRun, type DiagnosticView } from "./run-diagnostics";
import { stopStageDescription } from "./stop-diagnostics";

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
export function turnCodexErrorInfo(error: unknown): unknown {
  const value = object(error);
  return value.codexErrorInfo ?? object(value.data).codexErrorInfo ?? null;
}
export function turnErrorLine(error: unknown): string {
  const value = object(error);
  const info = turnCodexErrorInfo(error);
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
  return { ...upstreamFields(run), ...diagnosticFields(run), ...(error != null ? { error: redactParams(error) } : {}) };
}

type View = DiagnosticView;
/** 通知视图不带轮次；只有完整运行记录才能判断选定最终轮是否有错误。 */
function failedWithTurnError(view: View): boolean {
  const run = view as Partial<RunResult>;
  return view.status === "failed" && Array.isArray(run.turns) && resultTurn(run as RunResult)?.error != null;
}

/** 先订阅、后快照；诊断版本与最终失败摘要分别去重，彼此不能吞掉。 */
export function observeUpstreamErrors(rpc: BrokerRpc, runId: () => string | null | undefined, notice: (line: string) => void,
  now: () => number = Date.now) {
  let active = true;
  const buffered: View[] = [];
  const views = createDiagnosticStore();
  const printed = new Map<string, { key: string; retry: boolean | null; at: number; ended: boolean; revision?: number }>();
  /** 每个运行已在「原因」行输出过的文本，失败摘要据此避免把同一原因再说一遍。 */
  const reasons = new Map<string, Set<string>>();
  const failures = new Set<string>();
  const stages = new Set<string>();
  const display = (view: View, forceEnded = false) => {
    if (!view.runId || !view.upstreamError) return;
    const error = view.upstreamError;
    const key = upstreamGroupKey(error);
    const ended = !isUnconfirmedRun(view) && (forceEnded || view.state === "ended" || view.state === "lost");
    // 失败且选定最终轮有错误时由 failure 只输出一行「失败原因」，不再补历史摘要。
    if (ended && failedWithTurnError(view)) return;
    const previous = printed.get(view.runId);
    // 运行中已提示过同一组错误，结束时不再重复；只有从未提示过（如 wait 晚接入）才补「运行已结束」摘要。
    if (ended && previous?.key === key) return;
    if (previous?.revision !== undefined && previous.revision === view.diagnosticRevision) return;
    if (previous && previous.key === key && previous.retry === error.willRetry && !ended && now() - previous.at < 30_000) return;
    printed.set(view.runId, { key, retry: error.willRetry, at: now(), ended, revision: view.diagnosticRevision });
    const lines = upstreamNoticeLines(error, ended);
    let shown = reasons.get(view.runId);
    if (!shown) reasons.set(view.runId, shown = new Set());
    shown.add(reason(error));
    for (const line of lines) notice(line);
  };
  const diagnostics = (view: View) => {
    if (!view.runId) return;
    const once = (stage: string, line: string) => {
      const key = `${view.runId}\0${stage}`;
      if (stages.has(key)) return;
      stages.add(key); notice(line);
    };
    const stall = view.stall;
    if (stall?.phase === "verifying") once("stall:verifying", `[cxb] 已 ${Math.floor(stall.idleMs / 1000)} 秒未观察到进展（无进展期限 ${Math.floor(stall.timeoutMs / 1000)} 秒），正在核实；仍无进展将停止本次运行。`);
    if (stall?.phase === "resumed") once("stall:resumed", "[cxb] 核实时发现新进展或轮次已结束，继续运行。");
    if (stall?.phase === "stopping") {
      const verify = stall.verify === "inProgress" ? "轮次仍在进行" : stall.verify === "absent" ? "没有进行中的父轮" : "状态读取失败或超时";
      once("stall:stopping", `[cxb] 无进展核实结束（${verify}），正在停止本次运行。`);
    }
    if (view.interruptReason === "upstreamAccessError") {
      const access = [view.upstreamError, ...(view.childUpstreamErrors ?? []).map((child) => child.upstreamError)]
        .find((error) => error && ["authentication", "paymentRequired", "permissionDenied"].includes(error.category));
      if (access) once("access", `[cxb] 已按 --upstream-error-policy stop-access 停止本次运行：${upstreamLabel(access)}。`);
    }
    if (isUnconfirmedRun(view)) {
      const stage = stopStageDescription(view.stopDiagnostics, view.stopCheck);
      once(`unconfirmed:${stage}`, `[cxb] 停止尚未确认：${stage}。`);
    } else if (view.interruptReason === "stallTimeout" && (view.state === "ended" || view.state === "lost") && stall) {
      once("stall:ended", `[cxb] 已因无进展停止本次运行（${Math.floor(stall.timeoutMs / 1000)} 秒未观察到进展）。`);
    }
    for (const child of view.childUpstreamErrors ?? []) {
      const error = child.upstreamError;
      const key = upstreamGroupKey(error);
      const printedId = `${view.runId}\0${child.threadId}`;
      const previous = printed.get(printedId);
      const ended = !isUnconfirmedRun(view) && (view.state === "ended" || view.state === "lost");
      if (previous?.revision !== undefined && previous.revision === view.diagnosticRevision) continue;
      if (previous?.key === key && (ended || previous.retry === error.willRetry && now() - previous.at < 30_000)) continue;
      printed.set(printedId, { key, retry: error.willRetry, at: now(), ended, revision: view.diagnosticRevision });
      const [first, cause] = upstreamNoticeLines(error, ended);
      notice(first!.replace("[cxb] ", `[cxb] 子代理 ${child.threadId.slice(0, 8)} `));
      notice(cause!);
    }
  };
  const acceptKnown = (view: View) => {
    const id = runId();
    if (!active || !id || view.runId !== id) return;
    const complete = Array.isArray((view as Partial<RunResult>).turns)
      || ["stall", "stopDiagnostics", "childUpstreamErrors", "stopOutcome", "stopCheck"].every((key) => Object.hasOwn(view, key));
    const accepted = views.accept(view, complete);
    if (!accepted && view.state !== "ended" && view.state !== "lost") return;
    const merged = views.merge(view);
    display(merged); diagnostics(merged);
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
    return views.merge(run) as RunResult;
  };
  const apply = (run: RunResult): RunResult => {
    accept(run);
    const result = merge(run);
    // 运行终态不受 diagnosticRevision 去重影响；历史 willRetry 不能冒充当前重试。
    if (active && run.runId === runId() && (run.state === "ended" || run.state === "lost")) {
      display(result, true); diagnostics(result);
    }
    return result;
  };
  const failure = (run: RunResult) => {
    if (!active || run.runId !== runId() || run.status !== "failed" || failures.has(run.runId)) return;
    failures.add(run.runId);
    const error = resultTurn(run)?.error;
    if (error != null) {
      const value = object(error);
      const info = turnCodexErrorInfo(error);
      const said = reasons.get(run.runId);
      // 原因正文已在上方「原因」行出现过时只指回去，避免同一段文字在 stderr 里连说两遍。
      const repeated = said !== undefined && [value.message, value.additionalDetails]
        .some((text) => typeof text === "string" && text.trim() !== "" && said.has(diagnosticLine(text)));
      notice(repeated ? `[cxb] 失败原因：见上方上游报错${statusNote({ httpStatusCode: httpStatus(info), codexErrorInfo: info })}。`
        : `[cxb] 失败原因：${turnErrorLine(error)}`);
    } else display(merge(run), true);
  };
  const off = rpc.on("broker/run/updated", (raw) => { if (raw && typeof raw === "object") accept(raw as View); });
  const watchedRpc: BrokerRpc = { ...rpc, on(method, handler) {
    const dispose = rpc.on(method, handler);
    // 开轮回包之前的诊断已由观察器接住；等待器订阅后先补交这一版，再接受后续通知。
    if (active && method === "broker/run/updated") {
      const view = views.get(runId() ?? "");
      if (view) handler(view);
    }
    return dispose;
  }, async request<T>(method: string, params?: unknown, options?: Parameters<BrokerRpc["request"]>[2]): Promise<T> {
    const result = await rpc.request<T>(method, params, options);
    if (active && method === "broker/threads/watch" && result && typeof result === "object") {
      const snapshot = result as { run?: RunResult | null };
      if (snapshot.run) return { ...snapshot, run: apply(snapshot.run) } as T;
    }
    return result;
  } };
  return { accept, refresh, apply, failure, rpc: watchedRpc,
    fields: () => { const view = views.get(runId() ?? "") ?? {}; return { ...upstreamFields(view as Partial<RunResult>), ...diagnosticFields(view as Partial<RunResult>) }; },
    dispose: () => { active = false; off(); buffered.length = 0; views.clear(); printed.clear(); reasons.clear(); failures.clear(); stages.clear(); } };
}
