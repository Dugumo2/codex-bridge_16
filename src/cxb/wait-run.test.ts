import { afterEach, expect, test } from "bun:test";
import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { exitCodeForRun, waitRun as rawWaitRun } from "./wait-run";
import type { WaitRun } from "./contracts";
import { FakeRpc, runResult } from "./test-fixtures/run-fakes";

const opts = { threadId: "thread-test", runId: "run-test", timeoutMs: 1000 };
const pending = new Map<AbortController, Promise<unknown>>();
const waitRun: WaitRun = (rpc, options) => {
  const controller = new AbortController();
  const signal = options.signal ? AbortSignal.any([controller.signal, options.signal]) : controller.signal;
  const work = rawWaitRun(rpc, { ...options, signal });
  pending.set(controller, work);
  void work.catch(() => {});
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("测试等待防挂期限")), 2000);
    void work.then((value) => { clearTimeout(timer); resolve(value); }, (error) => { clearTimeout(timer); reject(error); });
  });
};
afterEach(async () => {
  for (const controller of pending.keys()) controller.abort();
  await Promise.allSettled(pending.values());
  pending.clear();
}, 5000);

test("快照即终态：ended、lost、无轮次与 unavailable 都直接返回并撤销订阅", async () => {
  for (const run of [runResult(), runResult({ state: "lost", status: "interrupted" }), runResult({ turns: [] }),
    runResult({ turns: [{ ...runResult().turns[0], ready: "unavailable" }] })]) {
    const fake = new FakeRpc();
    fake.handle = () => { expect(fake.subscriptions).toBe(3); return { run }; };
    expect(await waitRun(fake.rpc, opts)).toEqual(run);
    expect(fake.subscriptions).toBe(0);
  }
});

test("先 ended 后 result：仅接纳对应 run、零基轮次序号与 turnId，超过百轮仍定位最后一轮", async () => {
  const fake = new FakeRpc();
  fake.handle = () => ({ run: runResult({ state: "running", status: null }) });
  let resolved = false;
  const waiting = waitRun(fake.rpc, opts).then((value) => { resolved = true; return value; });
  const run = runResult({ turnsTotal: 121, turns: [{ ...runResult().turns[0], ready: "pending", output: "" }] });
  fake.emit("broker/run/ended", { run: { ...run, runId: "别的运行" } });
  fake.emit("broker/run/ended", { run });
  for (const event of [
    { runId: "别的运行", turn: 120, result: runResult().turns[0] },
    { runId: opts.runId, turn: 119, result: runResult().turns[0] },
    { runId: opts.runId, turn: 120, result: { ...runResult().turns[0], turnId: "别的轮次" } },
  ]) {
    fake.emit("broker/thread/result", event);
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(resolved).toBe(false);
  }
  fake.emit("broker/thread/result", { runId: opts.runId, turn: 120, result: runResult().turns[0] });
  const result = await waiting;
  expect(result.turns[0].output).toBe("中文结果");
  expect(result.runId).toBe(opts.runId);
  expect(fake.subscriptions).toBe(0);
});

test("result、ended 早于旧快照回包不会丢失或倒退", async () => {
  const fake = new FakeRpc();
  let release!: (value: unknown) => void;
  fake.handle = () => new Promise((resolve) => { release = resolve; });
  const waiting = waitRun(fake.rpc, opts);
  fake.emit("broker/thread/result", { runId: opts.runId, turn: 0, result: runResult().turns[0] });
  fake.emit("broker/run/ended", { run: runResult({ turns: [{ ...runResult().turns[0], ready: "pending" }] }) });
  release({ run: runResult({ state: "running", status: null }) });
  expect((await waiting).turns[0].ready).toBe("ready");
  expect(fake.subscriptions).toBe(0);
});

test("运行结束前的单轮结果不算整个运行结束", async () => {
  const fake = new FakeRpc();
  fake.handle = () => ({ run: runResult({ state: "running", status: null }) });
  let resolved = false;
  const work = waitRun(fake.rpc, opts).then((value) => { resolved = true; return value; });
  fake.emit("broker/thread/result", { runId: opts.runId, turn: 0, result: runResult().turns[0] });
  await new Promise<void>((resolve) => setImmediate(resolve));
  expect(resolved).toBe(false);
  fake.emit("broker/run/ended", { run: runResult() });
  await work;
});

test("ended 早到但补读在服务器 watch 生效前完成时，以就绪快照补全", async () => {
  const fake = new FakeRpc();
  let release!: (value: unknown) => void;
  fake.handle = () => new Promise((resolve) => { release = resolve; });
  const work = waitRun(fake.rpc, opts);
  fake.emit("broker/run/ended", { run: runResult({ turns: [{ ...runResult().turns[0], ready: "pending" }] }) });
  release({ run: runResult() });
  expect((await work).turns[0].ready).toBe("ready");
});

test("快照挂起时整体期限、取消、断开都拒绝等待并收束订阅", async () => {
  for (const kind of ["期限", "取消", "断开"] as const) {
    const fake = new FakeRpc();
    fake.handle = () => new Promise(() => {});
    const controller = new AbortController();
    const work = waitRun(fake.rpc, { ...opts, timeoutMs: kind === "期限" ? 1 : 1000, signal: controller.signal });
    if (kind === "取消") controller.abort();
    if (kind === "断开") fake.disconnect();
    await expect(work).rejects.toMatchObject({ code: kind === "期限" ? "TIMEOUT" : kind === "取消" ? "INTERRUPTED" : "BROKER_UNAVAILABLE" });
    expect(fake.subscriptions).toBe(0);
  }
});

test("预先取消不发快照请求；快照错误也清理订阅", async () => {
  const fake = new FakeRpc();
  const controller = new AbortController();
  controller.abort();
  await expect(waitRun(fake.rpc, { ...opts, signal: controller.signal })).rejects.toMatchObject({ code: "INTERRUPTED" });
  expect(fake.calls).toHaveLength(0);
  fake.handle = () => { throw new Error("模拟断开"); };
  await expect(waitRun(fake.rpc, opts)).rejects.toMatchObject({ code: "BROKER_UNAVAILABLE" });
  expect(fake.subscriptions).toBe(0);
});

test("快照按 runId 取这次运行；broker 内存里没有时读磁盘记录，磁盘也没有或属于别的线程则 NOT_FOUND", async () => {
  const dir = mkdtempSync(join(tmpdir(), "cxb-waitrun-"));
  try {
    mkdirSync(join(dir, "runs"));
    const fake = new FakeRpc();
    fake.handle = () => ({ run: null });
    const disk = runResult({ status: "interrupted", interruptReason: "stop", stopOutcome: "confirmed" });
    writeFileSync(join(dir, "runs", "run-test.json"), JSON.stringify({ epoch: 1, revision: 1, payload: disk }));
    expect(await waitRun(fake.rpc, { ...opts, workspaceDir: dir })).toEqual(disk);
    expect(fake.calls[0]).toMatchObject({ method: "broker/threads/watch", params: { threadId: opts.threadId, runId: opts.runId } });
    expect(fake.subscriptions).toBe(0);
    await expect(waitRun(fake.rpc, opts)).rejects.toMatchObject({ code: "NOT_FOUND" });
    await expect(waitRun(fake.rpc, { ...opts, runId: "run-none", workspaceDir: dir })).rejects.toMatchObject({ code: "NOT_FOUND" });
    writeFileSync(join(dir, "runs", "run-other.json"), JSON.stringify({ epoch: 1, revision: 1, payload: runResult({ runId: "run-other", threadId: "别的线程" }) }));
    await expect(waitRun(fake.rpc, { ...opts, runId: "run-other", workspaceDir: dir })).rejects.toMatchObject({ code: "NOT_FOUND" });
    for (const [runId, stale] of [["run-live", runResult({ runId: "run-live", state: "running", status: null })],
      ["run-wip", runResult({ runId: "run-wip", turns: [{ ...runResult().turns[0], ready: "pending" }] })]] as const) {
      writeFileSync(join(dir, "runs", `${runId}.json`), JSON.stringify({ epoch: 1, revision: 1, payload: stale }));
      await expect(waitRun(fake.rpc, { ...opts, runId, workspaceDir: dir })).rejects.toMatchObject({ code: "BROKER_UNAVAILABLE", retryable: true });
    }
    // 先到的 ended 带着 pending 的最后一轮，补读在 watch 生效前完成且运行随即被淘汰：以磁盘记录补全，不干等。
    const raced = new FakeRpc();
    let release!: (value: unknown) => void;
    raced.handle = () => new Promise((resolve) => { release = resolve; });
    const racing = waitRun(raced.rpc, { ...opts, workspaceDir: dir });
    raced.emit("broker/run/ended", { run: runResult({ turns: [{ ...runResult().turns[0], ready: "pending", output: "" }] }) });
    release({ run: null });
    expect(await racing).toEqual(disk);
    writeFileSync(join(dir, "runs", "run-bad.json"), "{坏");
    await expect(waitRun(fake.rpc, { ...opts, runId: "run-bad", workspaceDir: dir })).rejects.toMatchObject({ code: "LOCAL_ERROR" });
    expect(fake.subscriptions).toBe(0);
  } finally {
    rmSync(dir, { recursive: true, force: true });
    expect(existsSync(dir)).toBe(false);
  }
});

test("退出码优先级：私有与共享刹车均为九，其余遵守运行状态", () => {
  for (const [status, code] of [["completed", 0], ["interrupted", 4], ["failed", 7], ["goalBlocked", 9]] as const) {
    expect(exitCodeForRun(runResult({ status }))).toBe(code);
  }
  expect(exitCodeForRun(runResult({ status: "interrupted", interruptReason: "goalLoop" }))).toBe(9);
  expect(exitCodeForRun(runResult({ loopGuard: { kind: "noAction", count: 3, threshold: 3, turnIds: [], action: "pauseGoal" } }))).toBe(9);
  // 未启用协调的记录逐字沿用 s4-cli：刹车优先于 failed，失联不额外改变原状态映射。
  expect(exitCodeForRun(runResult({ status: "failed", interruptReason: "goalLoop" }))).toBe(9);
  expect(exitCodeForRun(runResult({ state: "lost", status: "completed" }))).toBe(0);
});
