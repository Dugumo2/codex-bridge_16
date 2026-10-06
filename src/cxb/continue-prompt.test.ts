// continue-prompt.ts 的单测：原始要求的追溯与断链、本地拒绝、broker 拒绝原因的退出码、「丁＋」提示内容。
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { RpcError } from "../types";
import { CxbError, EXIT, type RunResult } from "./contracts";
import {
  MAX_LISTED_DONE, absorbedRefusal, buildContinuePrompt, continueRefusal, localContinueRefusal,
  resolveRunChain,
} from "./continue-prompt";
import { readRunRecord } from "./ledger";
import { removeTemp } from "./test-fixtures/main-test-support";

function run(over: Partial<RunResult> = {}): RunResult {
  return {
    runId: "run_b", threadId: "thread-1", shortId: "a1b2c3d4", joined: false, state: "ended", status: "interrupted",
    startedAt: 1, endedAt: 2, meta: { source: "run" }, reviewThreadId: null, input: "续跑提示（不应被当作原始要求）",
    continuedFrom: null, interruptReason: "stop", stopOutcome: "confirmed",
    progress: { done: [], doneTotal: 0, unconfirmed: [], observedFiles: [] }, turns: [], turnsTotal: 0,
    ...over,
  };
}

function wireError(reason: string): CxbError {
  return new CxbError("BROKER_UNAVAILABLE", "broker 请求失败", {
    cause: new RpcError(`JSON-RPC error -32000: x`, -32000, "x", { reason }),
  });
}

describe("resolveRunChain：沿 continuedFrom 追原始要求", () => {
  test("没有 continuedFrom：原始就是自己", () => {
    const from = run({ input: "原始" });
    expect(resolveRunChain(from, () => null)).toEqual({ original: from, broken: false, missingRunId: null });
  });

  test("多层续跑追到最初那次", () => {
    const first = run({ runId: "run_a", input: "最初的要求" });
    const second = run({ runId: "run_m", continuedFrom: "run_a" });
    const from = run({ runId: "run_z", continuedFrom: "run_m" });
    const records: Record<string, RunResult> = { run_a: first, run_m: second };
    const chain = resolveRunChain(from, (id) => records[id] ?? null);
    expect(chain.original.input).toBe("最初的要求");
    expect(chain.broken).toBe(false);
  });

  test("中间记录缺失：停在能找到的最早一次并标记断链", () => {
    const second = run({ runId: "run_m", continuedFrom: "run_gone", input: "中间那次" });
    const from = run({ runId: "run_z", continuedFrom: "run_m" });
    const chain = resolveRunChain(from, (id) => (id === "run_m" ? second : null));
    expect(chain).toMatchObject({ broken: true, missingRunId: "run_gone" });
    expect(chain.original.runId).toBe("run_m");
  });

  test("成环或读取抛错都按断链处理，不会死循环", () => {
    const a = run({ runId: "run_a", continuedFrom: "run_b" });
    const b = run({ runId: "run_b", continuedFrom: "run_a" });
    expect(resolveRunChain(b, (id) => (id === "run_a" ? a : b)).broken).toBe(true);
    expect(resolveRunChain(run({ continuedFrom: "run_x" }), () => { throw new Error("坏"); }).broken).toBe(true);
  });
});

describe("readRunRecord：只读 broker 的运行记录", () => {
  let dir = "";
  beforeEach(() => { dir = mkdtempSync(join(tmpdir(), "cxb-continue-")); mkdirSync(join(dir, "runs")); });
  afterEach(async () => { await removeTemp(dir); });

  test("读外壳里的 payload；不存在返回 null", () => {
    const record = run({ runId: "run_ok" });
    writeFileSync(join(dir, "runs", "run_ok.json"), JSON.stringify({ epoch: 1, revision: 3, payload: record }));
    expect(readRunRecord(dir, "run_ok")).toEqual(record);
    expect(readRunRecord(dir, "run_none")).toBeNull();
  });

  test("内容损坏或 runId 不符：LOCAL_ERROR", () => {
    writeFileSync(join(dir, "runs", "run_bad.json"), "{不是 JSON");
    writeFileSync(join(dir, "runs", "run_other.json"), JSON.stringify({ payload: run({ runId: "run_x" }) }));
    for (const id of ["run_bad", "run_other"]) {
      expect(() => readRunRecord(dir, id)).toThrow(CxbError);
      try { readRunRecord(dir, id); } catch (error) { expect((error as CxbError).exitCode).toBe(EXIT.USAGE_OR_LOCAL_ERROR); }
    }
  });
});

describe("拒绝与退出码（命令面规格 3.1 节）", () => {
  test("本地拒绝：review、joined、正常完成退出 1；已中断与 lost 交给 broker", () => {
    expect(localContinueRefusal(run({ reviewThreadId: "child" }), "a1")?.exitCode).toBe(1);
    expect(localContinueRefusal(run({ meta: { source: "review" } }), "a1")?.exitCode).toBe(1);
    expect(localContinueRefusal(run({ joined: true }), "a1")?.exitCode).toBe(1);
    expect(localContinueRefusal(run({ status: "completed", interruptReason: null, stopOutcome: null }), "a1")?.exitCode).toBe(1);
    expect(localContinueRefusal(run(), "a1")).toBeNull();
    expect(localContinueRefusal(run({ state: "lost", status: null, interruptReason: "brokerLost" }), "a1")).toBeNull();
  });

  test.each([
    ["stopping", EXIT.THREAD_HELD], ["threadBusy", EXIT.THREAD_HELD], ["notLatestRun", EXIT.THREAD_HELD],
    ["resumeCheckFailed", EXIT.CODEX_FAILED], ["notResumable", EXIT.USAGE_OR_LOCAL_ERROR],
    ["unknownRun", EXIT.NOT_FOUND_OR_AMBIGUOUS], ["runMismatch", EXIT.NOT_FOUND_OR_AMBIGUOUS],
    ["continueCancelled", EXIT.INTERRUPTED], ["shuttingDown", EXIT.BROKER_UNAVAILABLE],
  ] as const)("broker 拒绝 %s → 退出 %d", (reason, exitCode) => {
    const refused = continueRefusal(wireError(reason), "a1b2");
    expect(refused?.exitCode).toBe(exitCode);
    expect(refused?.id).toBe("a1b2");
  });

  test("resumeCheckFailed 与 shuttingDown 可重试；stopping 提示先 stop", () => {
    expect(continueRefusal(wireError("resumeCheckFailed"), "a1")?.retryable).toBe(true);
    expect(continueRefusal(wireError("shuttingDown"), "a1")?.retryable).toBe(true);
    expect(continueRefusal(wireError("stopping"), "a1")?.message).toContain("cxb stop a1");
  });

  test("不是续跑拒绝（无线上原因、未知原因、本地错误）：返回 null 交给通用处理", () => {
    expect(continueRefusal(new CxbError("BROKER_UNAVAILABLE", "断开"), "a1")).toBeNull();
    expect(continueRefusal(wireError("somethingNew"), "a1")).toBeNull();
    expect(continueRefusal(new Error("x"), "a1")).toBeNull();
  });

  test("被别的 turn 吸收：退出 8，提示先查看、不自动重试", () => {
    const refused = absorbedRefusal("turn-other", "a1");
    expect(refused.exitCode).toBe(EXIT.THREAD_HELD);
    expect(refused.message).toContain("cxb show a1");
  });
});

describe("buildContinuePrompt：「丁＋」（S1-评审第 25 节）", () => {
  const done = Array.from({ length: 25 }, (_, i) => ({ itemId: `d${i}`, type: "commandExecution", summary: `步骤${i}` }));
  const from = run({
    runId: "run_z", continuedFrom: "run_a",
    progress: {
      done, doneTotal: 25,
      unconfirmed: [{ itemId: "u1", type: "commandExecution", summary: "npm publish" }],
      observedFiles: ["src/a.ts", "README.md"], incomplete: true,
    },
  });
  const original = run({ runId: "run_a", input: "把 a.ts 重构成两个文件" });

  test("各部分齐全且顺序固定：说明 → 目标 → 补充 → 原始要求 → 进度 → 怎么继续", () => {
    const text = buildContinuePrompt({
      from, chain: { original, broken: false, missingRunId: null },
      goal: { objective: "完成重构并通过测试", status: "paused" }, supplement: "顺便更新 README",
    });
    const order = ["【续跑说明】", "【目标原文】", "【本次补充】", "【原始要求】", "【已有进度】", "【怎么继续】"].map((h) => text.indexOf(h));
    expect(order.every((at) => at >= 0)).toBe(true);
    expect([...order].sort((x, y) => x - y)).toEqual(order);
    expect(text).toContain("用户主动恢复");
    expect(text).toContain("不再阻止");
    expect(text).toContain("完成重构并通过测试");
    expect(text).toContain("顺便更新 README");
    expect(text).toContain("不替换原始要求");
    expect(text).toContain("把 a.ts 重构成两个文件");
    expect(text).not.toContain("续跑提示（不应被当作原始要求）");
    expect(text).toContain("用户执行了停止");
  });

  test("进度：只列最近若干步并给总数；未确认操作要求先核验；文件清单注明可能不完整；丢过条目时注明", () => {
    const text = buildContinuePrompt({ from, chain: { original, broken: false, missingRunId: null }, goal: null, supplement: null });
    expect(text).toContain(`共 25 项，以下是最近 ${MAX_LISTED_DONE} 项`);
    expect(text).toContain("步骤24");
    expect(text).not.toContain("步骤4\n");
    expect(text).toContain("npm publish");
    expect(text).toContain("先核验");
    expect(text).toContain("src/a.ts");
    expect(text).toContain("可能不完整");
    expect(text).toContain("丢过条目");
  });

  test("没有 goal、没有补充：不出现对应段落；断链时注明原始要求可能不是最初的", () => {
    const text = buildContinuePrompt({ from, chain: { original, broken: true, missingRunId: "run_gone" }, goal: null, supplement: "  " });
    expect(text).not.toContain("【目标原文】");
    expect(text).not.toContain("【本次补充】");
    expect(text).toContain("run_gone");
    expect(text).toContain("可能不是最初的要求");
  });
});
