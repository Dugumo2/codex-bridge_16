import type { Reroute, ModelRun } from "./contracts";
import { latestModelRun } from "./ledger";
import { modelTurn } from "./run-result";

/** 展示的是请求值；服务器改道单独注明，不覆盖请求模型。 */
export interface ModelDisplay {
  model: string | null;
  reasoning: string | null;
  modelSource: "run" | "index";
  reroute: Reroute | null;
}

export function runModelDisplay(run: ModelRun): ModelDisplay {
  const last = "turns" in run ? modelTurn(run) : run.lastTurn;
  const currentTurnId = run.finalTurnId ?? run.currentTurnId;
  const pending = run.pendingReroute;
  // 只显示本运行当前轮的改道；旧记录没有当前轮时退回最后一轮结果。
  const reroute = currentTurnId == null ? last?.reroute ?? null
    : pending?.turnId === currentTurnId ? pending
    : last?.turnId === currentTurnId ? last.reroute ?? null : null;
  return { model: run.meta?.model ?? null, reasoning: run.meta?.effort ?? null, modelSource: "run",
    reroute: reroute ? { fromModel: reroute.fromModel, toModel: reroute.toModel, reason: reroute.reason } : null };
}

export function threadModelDisplay(threadId: string, indexModel: string | null, disk: ModelRun[], online: ModelRun[] = []): ModelDisplay {
  const run = latestModelRun(threadId, disk, online);
  return run ? runModelDisplay(run)
    : { model: indexModel, reasoning: null, modelSource: "index", reroute: null };
}

export function rerouteLabel(reroute: Reroute | null | undefined): string {
  return reroute ? `（服务器改道为 ${reroute.toModel}）` : "";
}

export function modelLabel(display: ModelDisplay): string {
  return `${display.model ?? "-"}（${display.modelSource === "index" ? "线程索引" : "请求值"}）${rerouteLabel(display.reroute)}`;
}
