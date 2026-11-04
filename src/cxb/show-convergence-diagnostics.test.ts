import { afterEach, beforeEach, expect, test } from "bun:test";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { show } from "./show";
import { convergenceLines, observeConvergence } from "./convergence";
import { convergenceFields } from "./run-result";
import type { ConvergenceDiagnostic, SubagentJoin } from "./sa1b-contracts";
import { bounded, removeTemp } from "./test-fixtures/main-test-support";
import { context, ledger } from "./test-fixtures/n4a-unit";
import { FakeRpc, runResult } from "./test-fixtures/run-fakes";

let dir: string;
beforeEach(() => { dir = mkdtempSync(join(tmpdir(), "cxb-show-convergence-")); });
afterEach(async () => { await removeTemp(dir); }, 10_000);
const subagentJoin: SubagentJoin = { phase: "waitingSubtree", wakeupsStarted: 0, wakeupAttempts: 0, startRetriesUsed: 0,
  workEpoch: 3, deliveredEpoch: 0, reconciledEpoch: 0, deadlineAt: 9999 };
const diagnostic: ConvergenceDiagnostic = {
  runId: "run-test", collectedAt: 3000, revision: 4, connectionGeneration: 1, workEpoch: 3, coveredEpoch: 3,
  gate: "pendingOperations", gates: ["pendingOperations", "activityUnverified"],
  counts: { pendingOperations: 1, pendingRequests: 0, unresolvedLookups: 0, unresolvedCandidates: 0, conflicts: 0,
    activities: 1, unverifiedActivities: 1, unsettledActivities: 1, missingResults: 1, activeGoals: 0, unknownGoals: 1 },
  pendingOperations: [{ kind: "v2Interacted", outcome: "unproven", receivers: ["child"], pairingMode: "strict", fallbackReason: null, provenTurns: [],
    expiresAt: 2000, expired: true, missing: false, omittedReceivers: 0, omittedProvenTurns: 0 }],
  activities: [{ threadId: "child", turnId: null, activitySeq: 2, verified: false, activity: "unknown",
    resultStatus: null, goal: "unknown", pendingRequests: 0 }],
  omitted: { pendingOperations: 0, activities: 0, identifierBytes: 0, gates: 0 },
};

test("show 运行中查询在线诊断，带明确请求期限且不修改落盘记录", async () => {
  ledger(dir, [runResult({ state: "running", subagentJoin })]);
  const path = join(dir, "runs", "run-test.json");
  const before = readFileSync(path, "utf8");
  const t = context(dir, ["show", "abcd", "--json"]);
  t.fake.handle = method => method === "broker/runs/convergenceDiagnostics" ? { diagnostic } : { runs: [] };
  expect(await bounded(show(t.ctx), 2000, "在线诊断未返回")).toBe(5);
  expect(t.json[0]).toMatchObject({ convergenceDiagnostic: diagnostic, convergenceDiagnosticStatus: "current" });
  expect(t.fake.calls.find(call => call.method === "broker/runs/convergenceDiagnostics")).toMatchObject({
    params: { runId: "run-test" }, options: { timeoutMs: 3000 },
  });
  expect(readFileSync(path, "utf8")).toBe(before);
  expect(t.cleanup.size).toBe(0);
});

test("离线不沿用落盘旧诊断，也不带出异常正文", async () => {
  ledger(dir, [runResult({ state: "running", subagentJoin, convergenceDiagnostic: diagnostic })]);
  const t = context(dir, ["show", "abcd", "--json"]);
  t.ctx.connect = async () => { throw new Error("正文哨兵-禁止输出"); };
  expect(await bounded(show(t.ctx), 2000, "离线诊断未返回")).toBe(5);
  expect(t.json[0]).toMatchObject({ convergenceDiagnostic: null, convergenceDiagnosticStatus: "unavailable" });
  expect(t.notices.join()).toContain("无当前诊断");
  expect(JSON.stringify([t.json, t.notices])).not.toContain("正文哨兵");
});

test("诊断请求被扣住仍按期限返回，迟到回包不能改已输出结果", async () => {
  ledger(dir, [runResult({ state: "running", subagentJoin })]);
  const t = context(dir, ["show", "abcd", "--json"]);
  let release!: (value: unknown) => void;
  const held = new Promise(resolve => { release = resolve; });
  t.fake.handle = method => method === "broker/runs/convergenceDiagnostics" ? held : { runs: [] };
  try {
    expect(await bounded(show(t.ctx), 4500, "诊断查询超过自身期限")).toBe(5);
    expect(t.json[0]).toMatchObject({ convergenceDiagnostic: null, convergenceDiagnosticStatus: "unavailable" });
    const output = JSON.stringify(t.json);
    release({ diagnostic });
    await bounded(held, 500, "已释放的诊断回包没有结算");
    expect(JSON.stringify(t.json)).toBe(output);
    expect(t.cleanup.size).toBe(0);
  } finally { release({ diagnostic }); }
}, 6000);

test("终态清除旧诊断，成功阶段只输出中性门名", () => {
  expect(convergenceFields(runResult({ convergenceDiagnostic: diagnostic })).convergenceDiagnostic).toBeNull();
  const lines = convergenceLines({ subagentJoin, convergenceDiagnostic: diagnostic });
  expect(lines).toContain("正在等待：派生操作落定");
  expect(lines.join()).not.toMatch(/缺口|陈旧|不完整|卡住/);
  expect(convergenceLines({ subagentJoin: { ...subagentJoin, phase: "released" }, convergenceDiagnostic: diagnostic }).join()).not.toContain("正在等待");
  const closing = convergenceLines({ subagentJoin, convergenceDiagnostic: { ...diagnostic, gate: "brokerClosingOrQuiescing" } });
  expect(closing).toContain("正在等待：broker 正在关闭或静默");
  expect(closing.join()).not.toContain("自动汇总权限核实");
});

test("观察器接收在线门名，终态 null 清除且迟到 revision 不能复活", () => {
  const rpc = new FakeRpc();
  const lines: string[] = [];
  const observer = observeConvergence(rpc.rpc, () => "run-test", line => lines.push(line));
  try {
    observer.accept({ runId: "run-test", revision: 4, subagentJoin, convergenceDiagnostic: diagnostic });
    expect(lines).toContain("正在等待：派生操作落定");
    observer.accept({ runId: "run-test", revision: 5, subagentJoin: { ...subagentJoin, phase: "released" }, convergenceDiagnostic: null });
    observer.accept({ runId: "run-test", revision: 4, subagentJoin, convergenceDiagnostic: diagnostic });
    expect(observer.fields().convergenceDiagnostic).toBeNull();
  } finally { observer.dispose(); }
  expect(rpc.subscriptions).toBe(0);
});
