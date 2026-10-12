import { expect, test } from "bun:test";
import type { RunResult } from "./contracts";
import { resultPending, resultReady, resultTurn, selectedResultFields } from "./run-result";
import { modelSummary, runResult } from "./test-fixtures/run-fakes";
import type { SubagentJoin } from "./sa1b-contracts";

const join: SubagentJoin = { phase: "released", wakeupsStarted: 1, wakeupAttempts: 1, startRetriesUsed: 0, workEpoch: 1,
  deliveredEpoch: 1, reconciledEpoch: 1, deadlineAt: 0 };
const turn = (turnId: string, output: string) => ({ ...runResult().turns[0]!, turnId, output });
/** 三轮：中段是最终汇总轮，尾部是迟到的旧轮结果。 */
function record(patch: Partial<RunResult>): RunResult {
  return runResult({ turns: [turn("首轮", "首轮输出"), turn("汇总轮", "汇总输出"), turn("迟到轮", "迟到旧输出")], turnsTotal: 3, ...patch });
}

test("旧记录（无协调字段）保留尾项回退", () => {
  const run = record({});
  expect(resultTurn(run)?.output).toBe("迟到旧输出");
  expect(selectedResultFields(run)).toEqual({});
  expect(modelSummary(run).lastTurn?.turnId).toBe("迟到轮");
  expect(modelSummary(run)).not.toHaveProperty("finalTurnId");
});

test("最终轮在中段：按最终指针交付，模拟摘要与 broker 一致带指针", () => {
  const run = record({ subagentJoin: join, finalTurnId: "汇总轮", currentTurnId: "迟到轮" });
  expect(resultTurn(run)?.output).toBe("汇总输出");
  expect(modelSummary(run)).toMatchObject({ finalTurnId: "汇总轮", lastTurn: { turnId: "汇总轮" } });
});

test("最终指针的目标缺失：不借旧轮冒充，结果仍待补读", () => {
  const run = record({ subagentJoin: join, finalTurnId: "未读到" });
  expect(resultTurn(run)).toBeUndefined();
  expect(resultPending(run)).toBe(true);
  expect(modelSummary(run).lastTurn).toBeNull();
});

test("协调运行进行中：取已确认当前父轮作阶段结果，不取尾部旧轮", () => {
  const run = record({ subagentJoin: { ...join, phase: "waitingSubtree" }, state: "running", status: null, currentTurnId: "汇总轮" });
  expect(resultTurn(run)?.output).toBe("汇总输出");
  expect(modelSummary(run).lastTurn?.turnId).toBe("汇总轮");
  expect(resultTurn({ ...run, currentTurnId: null })).toBeUndefined();
});

for (const state of ["ended", "lost"] as const) test(`协调运行${state === "lost" ? "失联" : "已结束"}却无最终指针：明确不可用，不无限等待`, () => {
  const run = record({ subagentJoin: { ...join, phase: "abnormal" }, state, status: state === "lost" ? null : "interrupted", currentTurnId: "迟到轮" });
  expect(resultTurn(run)).toBeUndefined();
  expect(resultPending(run)).toBe(false);
  expect(resultReady(run)).toBe("unavailable");
  expect(selectedResultFields(run)).toEqual({ ready: "unavailable" });
  expect(modelSummary(run).lastTurn).toBeNull();
});
