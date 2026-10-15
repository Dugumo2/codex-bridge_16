import { afterEach, beforeEach, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { RunResult, ServerRequestDeps } from "./contracts";
import { executeRun } from "./run";
import { wait } from "./wait";
import { bounded, exitCodeForRun, waitRun } from "./wait-run";
import { context, summary } from "./test-fixtures/n4a-unit";
import { FakeRpc, runResult } from "./test-fixtures/run-fakes";
import { childError, stall, stopDiagnostics } from "./test-fixtures/c3-diagnostics";
import { removeTemp } from "./test-fixtures/main-test-support";

const options = { threadId: "thread-test", runId: "run-test", timeoutMs: 1000 };
const stopping = (patch: Partial<RunResult> = {}) => runResult({ state: "stopping", status: null, diagnosticRevision: 5,
  interruptReason: "stallTimeout", stopOutcome: "unconfirmed", stopDiagnostics: stopDiagnostics(),
  turns: [{ ...runResult().turns[0]!, ready: "pending" }], ...patch });
const deps: ServerRequestDeps = { attachServerRequests: () => () => {},
  handover: async () => { throw new Error("本用例不交接"); }, renderPending: () => { throw new Error("本用例无待答"); } };
let dir: string, oldHome: string | undefined;
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "cxb-c3-wait-")); mkdirSync(join(dir, "home"));
  oldHome = process.env.CODEX_HOME; process.env.CODEX_HOME = join(dir, "home");
});
afterEach(async () => { if (oldHome === undefined) delete process.env.CODEX_HOME; else process.env.CODEX_HOME = oldHome; await removeTemp(dir); }, 10000);

for (const state of ["stopping", "ended"] as const) test(`新原因${state}未确认直接退出3，不等pending结果补读`, async () => {
  const fake = new FakeRpc(); fake.handle = () => ({ run: stopping({ state }) });
  const run = await bounded(waitRun(fake.rpc, options), 500);
  expect(run.state).toBe(state); expect(run.turns[0]?.ready).toBe("pending"); expect(exitCodeForRun(run)).toBe(3);
  expect(fake.subscriptions).toBe(0); expect(fake.calls).toHaveLength(1);
});

test("waitRun自己订阅诊断，较新通知覆盖旧完整快照，真实failed仍为7", async () => {
  const fake = new FakeRpc();
  let release!: (value: unknown) => void;
  fake.handle = () => new Promise((resolve) => { release = resolve; });
  const work = waitRun(fake.rpc, options);
  fake.emit("broker/run/updated", { runId: "run-test", diagnosticRevision: 5, state: "stopping", status: null,
    interruptReason: "upstreamAccessError", stopOutcome: "unconfirmed", stopDiagnostics: stopDiagnostics({ reason: "upstreamAccessError" }) });
  release({ run: runResult({ state: "running", status: null, diagnosticRevision: 2 }) });
  const run = await bounded(work, 500);
  expect(run.state).toBe("stopping"); expect(run.interruptReason).toBe("upstreamAccessError"); expect(exitCodeForRun(run)).toBe(3);
  expect(fake.notificationErrors).toEqual([]); expect(fake.subscriptions).toBe(0);
  const failed = new FakeRpc(); failed.handle = () => ({ run: stopping({ state: "ended", status: "failed", turns: runResult().turns }) });
  expect(exitCodeForRun(await waitRun(failed.rpc, options))).toBe(7);
});

test("只有部分通知时按runId有界补取完整记录，不把通知强转RunResult", async () => {
  const fake = new FakeRpc();
  let watches = 0;
  fake.handle = () => ++watches === 1 ? new Promise(() => {}) : { run: runResult({ state: "running", status: null, diagnosticRevision: 1, input: "完整快照输入" }) };
  const work = waitRun(fake.rpc, options);
  fake.emit("broker/run/updated", { runId: "run-test", diagnosticRevision: 3, state: "stopping", interruptReason: "stallTimeout", stopOutcome: "unconfirmed" });
  const run = await bounded(work, 500);
  expect(run.input).toBe("完整快照输入"); expect(run.turns).toHaveLength(1); expect(run.state).toBe("stopping");
  expect(fake.calls).toHaveLength(2);
  expect(fake.calls[1]!.params).toEqual({ threadId: "thread-test", runId: "run-test" });
  expect(fake.calls[1]!.options.timeoutMs).toBeLessThanOrEqual(5000);
  expect(fake.subscriptions).toBe(0); expect(fake.notificationErrors).toEqual([]);
});

test("补取失败安全降级为空结果和已知诊断；已有完整记录直接合并，不另等补取", async () => {
  const fake = new FakeRpc();
  let calls = 0;
  fake.handle = () => ++calls === 1 ? new Promise(() => {}) : Promise.reject(new Error("补取失败"));
  const work = waitRun(fake.rpc, options);
  fake.emit("broker/run/updated", { runId: "run-test", diagnosticRevision: 3, state: "stopping", status: null, interruptReason: "stallTimeout", stopOutcome: "unconfirmed" });
  const run = await bounded(work, 500);
  expect(run.runId).toBe("run-test"); expect(run.turns).toEqual([]); expect(run.turnsTotal).toBe(0); expect(exitCodeForRun(run)).toBe(3);
  expect(fake.subscriptions).toBe(0);
  const known = new FakeRpc(); known.handle = () => ({ run: runResult({ state: "running", status: null, diagnosticRevision: 1 }) });
  const running = waitRun(known.rpc, options);
  await new Promise<void>((resolve) => setImmediate(resolve));
  known.emit("broker/run/updated", { runId: "run-test", diagnosticRevision: 2, state: "stopping", interruptReason: "stallTimeout", stopOutcome: "unconfirmed" });
  expect((await running).turns).toHaveLength(1); expect(known.calls).toHaveLength(1); expect(known.notificationErrors).toEqual([]);
});

test("旧原因未确认仍继续等，旧版通知不能改写新版本诊断", async () => {
  const fake = new FakeRpc(); fake.handle = () => ({ run: stopping({ interruptReason: "stop" }) });
  let resolved = false;
  const work = waitRun(fake.rpc, options).then((run) => { resolved = true; return run; });
  await new Promise<void>((resolve) => setImmediate(resolve));
  fake.emit("broker/run/updated", { runId: "run-test", diagnosticRevision: 1, state: "stopping", interruptReason: "stallTimeout", stopOutcome: "unconfirmed" });
  await new Promise<void>((resolve) => setImmediate(resolve));
  expect(resolved).toBe(false);
  fake.emit("broker/run/ended", { run: runResult({ diagnosticRevision: 6, interruptReason: "stop", status: "interrupted" }) });
  expect(exitCodeForRun(await work)).toBe(4); expect(fake.subscriptions).toBe(0); expect(fake.notificationErrors).toEqual([]);
});

function serve(args: string[], run: RunResult) {
  const c = context(dir, args);
  c.ctx.connect = async () => ({ rpc: c.fake.rpc, launched: false, close: async () => {},
    handshake: { userAgent: "模拟", brokerVersion: "0.1", protocolVersion: 1, epoch: 1, server: { kind: "private" } } });
  c.fake.handle = (method) => {
    if (method === "thread/start") return { thread: { id: "thread-test" } };
    if (method === "broker/threads/annotate") return { shortId: "abcd1234" };
    if (method === "turn/start" || method === "review/start") return { runId: "run-test" };
    if (method === "broker/threads/watch") return { thread: summary(), run, pending: [] };
    if (method === "broker/threads/list") return { threads: [summary()] };
    if (method === "broker/runs/modelSummaries") return { runs: [] };
    return {};
  };
  return c;
}
async function command(command: "run" | "review" | "wait", c: ReturnType<typeof serve>) {
  return command === "wait" ? wait(c.ctx) : executeRun(c.ctx, deps, command === "run"
    ? { command, sandbox: "workspace-write", prompt: "任务" }
    : { command, sandbox: "read-only", review: { mode: "custom", ref: null, base: null, target: { type: "custom", instructions: "任务" } } });
}

for (const name of ["run", "review", "wait"] as const) for (const state of ["stopping", "ended"] as const) test(`${name} ${state}未确认JSON单对象字段齐全且没有运行结束文字`, async () => {
  const run = stopping({ state, stall: stall({ phase: "stopping", verify: "inProgress" }) });
  const c = serve([name, name === "wait" ? "abcd1234" : "任务", "--json", "-v"], run);
  expect(await bounded(command(name, c), 1000)).toBe(3);
  expect(c.json).toHaveLength(1);
  expect(c.json[0]).toMatchObject({ ok: false, state, stopOutcome: "unconfirmed", interruptReason: "stallTimeout", stopDiagnostics: run.stopDiagnostics });
  expect(c.progress.join("\n") + c.notices.join("\n") + c.texts.join("\n")).not.toMatch(/运行(?:已)?结束/);
  expect(c.fake.notificationErrors).toEqual([]); expect(c.fake.subscriptions).toBe(0);
});

for (const name of ["run", "review", "wait"] as const) test(`${name}新原因终态退出码和JSON增量，旧interruptReason条件保留`, async () => {
  for (const reason of ["stallTimeout", "upstreamAccessError", "stop"] as const) {
    const run = runResult({ status: "interrupted", interruptReason: reason, stall: stall(), stopDiagnostics: stopDiagnostics({ reason }), childUpstreamErrors: [childError()] });
    const c = serve([name, name === "wait" ? "abcd1234" : "任务", "--json"], run);
    expect(await command(name, c)).toBe(reason === "stallTimeout" ? 3 : 4);
    expect(c.json[0]).toMatchObject({ stall: run.stall, stopDiagnostics: run.stopDiagnostics, childUpstreamErrors: run.childUpstreamErrors });
    if (name === "wait" && reason === "stop") expect(c.json[0]).not.toHaveProperty("interruptReason");
    else expect(c.json[0].interruptReason).toBe(reason);
    expect(c.fake.subscriptions).toBe(0);
  }
});
