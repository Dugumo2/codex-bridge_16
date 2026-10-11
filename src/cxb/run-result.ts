import type { RunResult } from "./contracts";

/** 最终指针优先；存在指针但结果缺失时绝不能借旧轮冒充最终答复。 */
export function resultTurn(run: RunResult) {
  return run.finalTurnId !== undefined ? run.turns.find((turn) => turn.turnId === run.finalTurnId) : run.turns.at(-1);
}

export function resultPending(run: RunResult): boolean {
  const turn = resultTurn(run);
  return turn?.ready === "pending" || (run.finalTurnId !== undefined && !turn);
}

/** 新运行输出的就绪状态与来源；旧 CLI JSON 不增添推造字段。 */
export function selectedResultFields(run: RunResult) {
  if (run.finalTurnId === undefined && !run.subagentJoin) return {};
  const turn = resultTurn(run);
  return { ready: turn?.ready ?? "pending", ...(turn?.origin !== undefined ? { origin: turn.origin } : {}) };
}

/** 模型改道跟随已确认当前父轮；历史新记录跟随最终轮，旧记录保留原回退。 */
export function modelTurn(run: RunResult) {
  const id = run.finalTurnId ?? run.currentTurnId;
  return id != null ? run.turns.find((turn) => turn.turnId === id) : resultTurn(run);
}

/** JSON 增量字段原样透传；缺省不补默认值，不伪造已启用事实。 */
export function convergenceFields(run: Partial<RunResult>) {
  return {
    ...(run.finalTurnId !== undefined ? { finalTurnId: run.finalTurnId } : {}),
    ...(run.subagentJoin !== undefined ? { subagentJoin: run.subagentJoin } : {}),
    ...(run.runBudget !== undefined ? { runBudget: run.runBudget } : {}),
    ...(run.autoTurnsStarted !== undefined ? { autoTurnsStarted: run.autoTurnsStarted } : {}),
    ...(run.convergenceConfig !== undefined ? { convergenceConfig: run.convergenceConfig } : {}),
    ...(run.executionConfig !== undefined ? { executionConfig: run.executionConfig } : {}),
  };
}
