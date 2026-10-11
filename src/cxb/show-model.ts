import type { Reroute, RunResult } from "./contracts";
import { latestModelRun } from "./ledger";

/** 展示的是请求值；服务器改道单独注明，不覆盖请求模型。 */
export interface ModelDisplay {
  model: string | null;
  reasoning: string | null;
  modelSource: "run" | "index";
  reroute: Reroute | null;
}

export function runModelDisplay(run: RunResult, currentTurnId?: string | null): ModelDisplay {
  const last = run.turns.at(-1);
  const pending = run.pendingReroute;
  // 在线摘要已进入下一轮时，不把上一轮的改道挂在新轮上。
  const reroute = pending && (!currentTurnId || pending.turnId === currentTurnId) ? pending
    : currentTurnId && currentTurnId !== last?.turnId ? null : last?.reroute ?? null;
  return { model: run.meta?.model ?? null, reasoning: run.meta?.effort ?? null, modelSource: "run",
    reroute: reroute ? { fromModel: reroute.fromModel, toModel: reroute.toModel, reason: reroute.reason } : null };
}

export function threadModelDisplay(threadId: string, indexModel: string | null, disk: RunResult[], online: RunResult[] = [],
  current?: { lastRunId: string | null; turnId: string | null }): ModelDisplay {
  const run = latestModelRun(threadId, disk, online);
  return run ? runModelDisplay(run, current?.lastRunId === run.runId ? current.turnId : null)
    : { model: indexModel, reasoning: null, modelSource: "index", reroute: null };
}

export function rerouteLabel(reroute: Reroute | null | undefined): string {
  return reroute ? `（服务器改道为 ${reroute.toModel}）` : "";
}

export function modelLabel(display: ModelDisplay): string {
  return `${display.model ?? "-"}（${display.modelSource === "index" ? "线程索引" : "请求值"}）${rerouteLabel(display.reroute)}`;
}
