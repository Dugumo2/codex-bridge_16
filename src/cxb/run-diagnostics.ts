import type { RunResult } from "./contracts";
import { redactParams } from "./server-requests";

const keys = ["upstreamError", "lastProgressAt", "stall", "stopDiagnostics", "childUpstreamErrors",
  "interruptReason", "stopOutcome", "stopCheck"] as const;
type DiagnosticKey = typeof keys[number];
export type DiagnosticView = { runId?: string; diagnosticRevision?: number; state?: RunResult["state"]; status?: RunResult["status"] }
  & { [K in DiagnosticKey]?: RunResult[K] | null };

export function isCompleteDiagnosticNotice(view: DiagnosticView): boolean {
  return ["diagnosticRevision", "state", "status", ...keys].every((key) => Object.hasOwn(view, key));
}

export function isUnconfirmedRun(run: Pick<Partial<RunResult>, "interruptReason" | "stopOutcome" | "status">): boolean {
  return (run.interruptReason === "stallTimeout" || run.interruptReason === "upstreamAccessError")
    && run.stopOutcome === "unconfirmed";
}

/** 缺省诊断不改变旧命令的 JSON；策略只用于开轮，不进入输出。 */
export function diagnosticFields(run: Pick<Partial<RunResult>, "stall" | "stopDiagnostics" | "childUpstreamErrors">) {
  return { ...(run.stall ? { stall: run.stall } : {}),
    ...(run.stopDiagnostics ? { stopDiagnostics: redactParams(run.stopDiagnostics) } : {}),
    ...(run.childUpstreamErrors?.length ? { childUpstreamErrors: redactParams(run.childUpstreamErrors) } : {}) };
}

/** 完整记录和完整诊断通知按版替换；部分通知只更新实际出现的字段。 */
export function createDiagnosticStore() {
  const views = new Map<string, { revision: number; view: DiagnosticView; complete: boolean }>();
  const terminals = new Map<string, Pick<DiagnosticView, "state" | "status">>();
  const accept = (incoming: DiagnosticView, complete = false): boolean => {
    if (!incoming.runId) return false;
    const id = incoming.runId;
    // 终态与诊断版本独立接纳，旧版 ended 也不能被运行中快照吞掉。
    if (incoming.state === "ended" || incoming.state === "lost") {
      const previous = terminals.get(id);
      terminals.set(id, { state: incoming.state, status: previous?.status === "failed" ? "failed" : incoming.status });
    } else if (incoming.status === "failed") terminals.set(id, { ...terminals.get(id), status: "failed" });
    const revision = incoming.diagnosticRevision ?? 0;
    if (!Number.isSafeInteger(revision) || revision < 0) return false;
    const previous = views.get(id);
    if (previous && revision < previous.revision) return false;
    if (previous && revision === previous.revision && (!complete || previous.complete && incoming.diagnosticRevision !== undefined)) return false;
    const fields: DiagnosticView = complete ? {} : { ...previous?.view };
    for (const key of keys) {
      if (complete || Object.hasOwn(incoming, key)) (fields as Record<string, unknown>)[key] = incoming[key] ?? undefined;
    }
    if (Object.hasOwn(incoming, "interruptReason")) fields.interruptReason = incoming.interruptReason;
    if (Object.hasOwn(incoming, "stopOutcome")) fields.stopOutcome = incoming.stopOutcome;
    fields.runId = id;
    if (Object.hasOwn(incoming, "diagnosticRevision") || previous?.view.diagnosticRevision !== undefined) fields.diagnosticRevision = revision;
    fields.state = incoming.state ?? previous?.view.state;
    fields.status = incoming.status === undefined ? previous?.view.status : incoming.status;
    views.set(id, { revision, view: fields, complete });
    return true;
  };
  const merge = <T extends DiagnosticView>(run: T): T => {
    const latest = run.runId ? views.get(run.runId) : undefined;
    const terminal = run.runId ? terminals.get(run.runId) : undefined;
    const fields = latest && latest.revision >= (run.diagnosticRevision ?? 0) ? latest.view : {};
    const merged = { ...run, ...fields, ...terminal };
    for (const key of keys) if (Object.hasOwn(fields, key) && fields[key] === undefined) delete merged[key];
    return merged;
  };
  return { accept, merge, get: (id: string) => {
    const value = views.get(id)?.view;
    return value ? merge(value) : undefined;
  }, clear: () => { views.clear(); terminals.clear(); } };
}
