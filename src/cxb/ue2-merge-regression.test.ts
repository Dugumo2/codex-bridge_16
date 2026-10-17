import { expect, test } from "bun:test";
import { mkdirSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { BrokerConnection, RunResult } from "./contracts";
import { createRunHandler } from "./run";
import { wait } from "./wait";
import { observeUpstreamErrors } from "./upstream-errors";
import { bounded, waitRun } from "./wait-run";
import { FakeRpc, runResult } from "./test-fixtures/run-fakes";
import { RunHarness, fakeServerRequests, until } from "./test-fixtures/run-harness";
import { StallHarness } from "./test-fixtures/stall-harness";
import { context, summary } from "./test-fixtures/n4a-unit";
import { createReviewHandler } from "./review";
import { deferred, messageBarrier, watchdog } from "./test-fixtures/test-watchdog";
import { removeTemp } from "./test-fixtures/main-test-support";

/** 记录真实 CLI 连接收到的事件，确认测试确实穿过“诊断先到、完整结果后到”的接缝。 */
function trace(h: RunHarness, connection: BrokerConnection) {
  const events: Array<{ method: string; run: Partial<RunResult> }> = [];
  for (const method of ["broker/run/updated", "broker/run/ended"]) h.disposers.push(connection.rpc.on(method, (raw: any) => {
    events.push({ method, run: method === "broker/run/ended" ? raw.run : raw });
  }));
  return events;
}
function stoppedBeforeEnded(events: ReturnType<typeof trace>) {
  const diagnostic = events.findIndex(event => event.method === "broker/run/updated" && !!event.run.stopDiagnostics);
  const ended = events.findIndex(event => event.method === "broker/run/ended");
  expect(diagnostic).toBeGreaterThanOrEqual(0);
  expect(ended).toBeGreaterThan(diagnostic);
}

test("合并回归：用户 stop 的正常终态保留清理报告、中断原因和模型", async () => {
  expect(process.env.CXB_BROKER_BIN).toBeTruthy();
  const h = new RunHarness();
  try {
    const observer = await h.start({ hold: true, backgroundTerminals: { enabled: true } });
    const connection = await h.connect();
    const events = trace(h, connection);
    const work = h.invoke(createRunHandler(fakeServerRequests()), ["run", "停止回归", "--model", "original-model", "--json"], connection);
    await until(async () => (await observer.rpc.request<any>("fixture/state")).calls.some((call: any) => call.method === "turn/start"));
    const { run } = await observer.rpc.request<{ run: RunResult }>("broker/threads/watch", { threadId: "thread-1" });
    const waitingConnection = await h.connect();
    const waitingEvents = trace(h, waitingConnection);
    const watching = deferred<void>();
    const request = waitingConnection.rpc.request.bind(waitingConnection.rpc);
    waitingConnection.rpc.request = async <T>(method: string, params?: unknown, options?: { timeoutMs?: number }): Promise<T> => {
      const reply = await request<T>(method, params, options);
      if (method === "broker/threads/watch") watching.resolve();
      return reply;
    };
    const waiting = h.invoke(wait, ["wait", run.threadId, "--json"], waitingConnection);
    await watching.wait();
    await observer.rpc.request("broker/threads/stop", { threadId: run.threadId, runId: run.runId });
    const result = await work, waited = await waiting;
    const { run: final } = await observer.rpc.request<{ run: RunResult }>("broker/threads/watch", { threadId: run.threadId, runId: run.runId });
    expect(result.code).toBe(4); expect(waited.code).toBe(4);
    expect(JSON.parse(result.stdout)).toMatchObject({ interruptReason: "stop", model: "original-model", stopDiagnostics: final.stopDiagnostics });
    expect(final.backgroundCleanup).toMatchObject({ state: "confirmed", remaining: [] });
    expect(JSON.parse(waited.stdout).backgroundCleanup).toEqual(final.backgroundCleanup);
    for (const stderr of [result.stderr, waited.stderr]) {
      expect(stderr).toContain("Codex 受管的后台命令");
      expect(stderr).toContain("终止回包：");
    }
    stoppedBeforeEnded(events); stoppedBeforeEnded(waitingEvents);
  } finally { await h.cleanup(); }
}, 90000);

test("合并回归：停止诊断先到时失败运行保留 error 与失败原因", async () => {
  const h = new StallHarness();
  try {
    await h.open(); await h.control("mode", { mode: "failed" });
    const proc = h.run(["--json", "--stall-timeout", "0", "--upstream-error-policy", "stop-access"]);
    await h.started(); await h.control("errors");
    expect(await bounded(proc.closed, 20000)).toBe(7);
    const result = proc.output();
    expect(JSON.parse(result.stdout)).toMatchObject({ status: "failed", interruptReason: "upstreamAccessError", error: { message: "测试失败" } });
    expect(result.stderr).toContain("[cxb] 原因：测试失败");
    expect(result.stderr).toContain("[cxb] 失败原因：见上方上游报错。");
    stoppedBeforeEnded(h.events.map(event => ({ method: event.method, run: event.params.run ?? event.params })));
  } finally { await h.close(); }
}, 90000);

test("合并回归：停止诊断先到时 loop guard 保留种类与轮数", async () => {
  expect(process.env.CXB_BROKER_BIN).toBeTruthy();
  const h = new RunHarness();
  try {
    const observer = await h.start({ hold: true, backgroundTerminals: { enabled: true } });
    const connection = await h.connect(), events = trace(h, connection);
    const work = h.invoke(createRunHandler(fakeServerRequests()), ["run", "空转回归", "--goal", "完成目标", "--budget", "10000", "--json"], connection);
    await until(async () => (await observer.rpc.request<any>("fixture/state")).calls.some((call: any) => call.method === "turn/start"));
    await observer.rpc.request("fixture/control", { action: "finish", threadId: "thread-1" });
    for (let i = 0; i < 3; i++) await observer.rpc.request("fixture/control", { action: "next", threadId: "thread-1" });
    const result = await work;
    expect(result.code).toBe(9);
    expect(JSON.parse(result.stdout).loopGuard).toMatchObject({ kind: "noAction", count: 3, action: "stop" });
    expect(result.stderr).toContain("自动续跑刹车：连续无动作，3 轮");
    stoppedBeforeEnded(events);
  } finally { await h.cleanup(); }
}, 90000);

test("合并回归：受管子树失败保留完整终态的中断原因", async () => {
  const h = new StallHarness();
  try {
    await h.open(); await h.control("mode", { mode: "failed" });
    // 开轮即登记待答屏障；派生确认前无进展策略不能提交停止。
    await h.control("approvalOnStart");
    const detached = h.run(["--detach", "--json", "--stall-timeout", "1"]);
    expect(await bounded(detached.closed, 20000), JSON.stringify(detached.output())).toBe(0);
    await h.started();
    let approvalId = "";
    await h.until(async () => {
      const inbox = await h.owner.request<any>("broker/inbox/list", {});
      approvalId = inbox.items.find((item: any) => item.status === "pending")?.id ?? "";
      return !!approvalId;
    });
    const turnId = (await h.state()).threads.root.turns[0].id;
    await h.sa1b("spawn", { parentThreadId: "root", parentTurnId: turnId, childThreadId: "child" });
    await h.until(async () => !!(await h.record()).subagentJoin);
    const armed = await h.record();
    expect(armed.subagentJoin).toBeDefined(); expect(armed.state).toBe("running");
    expect((await h.state()).calls.some((call: any) => call.method === "turn/interrupt")).toBe(false);
    await h.owner.request("broker/inbox/resolve", { id: approvalId, answer: { decision: "accept" } });
    const result = await h.cli(["wait", "root", "--json", "--timeout", "20"]);
    expect(result.code).toBe(7);
    const record = await h.persisted(), final = JSON.parse(result.stdout);
    const diagnosticState = (run: any) => ({ revision: run.diagnosticRevision, state: run.state, status: run.status, interruptReason: run.interruptReason });
    expect(final.interruptReason, JSON.stringify({ record: diagnosticState(record), updates: h.updated().map(event => diagnosticState(event.params)),
      ended: h.events.filter(event => event.method === "broker/run/ended").map(event => diagnosticState(event.params.run)) })).toBe("stallTimeout");
    expect(final.error.message).toBe("测试失败");
    expect(record.subagentJoin).toBeDefined(); expect(final.subagentJoin).toBeDefined();
    expect(final.subagentJoin).toEqual(record.subagentJoin);
  } finally { await h.close(); }
}, 90000);

/** 把完整快照、终态和最终轮补读分别扣住，测试看门狗只负责判失败。 */
function unconfirmedCommand(name: "run" | "review" | "wait", reason: "stallTimeout" | "upstreamAccessError") {
  const dir = mkdtempSync(join(tmpdir(), "cxb-ue2-failed-"));
  mkdirSync(join(dir, "home"));
  const oldHome = process.env.CODEX_HOME;
  process.env.CODEX_HOME = join(dir, "home");
  const c = context(dir, [name, name === "wait" ? "abcd1234" : "失败交付回归", "--json", "--timeout", "5"]);
  c.ctx.connect = async () => ({ rpc: c.fake.rpc, launched: false, close: async () => {},
    handshake: { userAgent: "模拟", brokerVersion: "0.1", protocolVersion: 1, epoch: 1, server: { kind: "private" } } });
  const initial = runResult({ state: "running", status: null, diagnosticRevision: 1, turns: [], turnsTotal: 0 });
  const snapshot = { thread: summary(), run: initial, pending: [] };
  const watchReply = deferred<typeof snapshot>(), watching = deferred<void>();
  let watches = 0, finished = false;
  c.fake.handle = method => {
    if (method === "thread/start") return { thread: { id: "thread-test" } };
    if (method === "broker/threads/annotate") return { shortId: "abcd1234" };
    if (method === "turn/start" || method === "review/start") return { runId: "run-test" };
    if (method === "broker/threads/list") return { threads: [summary()] };
    if (method === "broker/runs/modelSummaries") return { runs: [] };
    if (method === "broker/threads/watch") {
      // wait 先定位运行，再由 waitRun 订阅并补取；只扣住后一个接缝。
      if (name === "wait" && watches++ === 0) return snapshot;
      watching.resolve(); return watchReply.promise;
    }
    return {};
  };
  const selected = { ...runResult().turns[0]!, turnId: "final-turn", status: "failed" as const,
    output: "", error: { message: "最终失败明细" } };
  const final = runResult({ status: "failed", diagnosticRevision: 4, interruptReason: reason, stopOutcome: "unconfirmed",
    finalTurnId: selected.turnId, turnsTotal: 2,
    turns: [{ ...runResult().turns[0]!, error: { message: "旧轮错误不可冒充" } }, { ...selected, ready: "pending", error: null }] });
  const handler = name === "wait" ? wait : name === "run" ? createRunHandler(fakeServerRequests()) : createReviewHandler(fakeServerRequests());
  const work = handler(c.ctx).finally(() => { finished = true; });
  void work.catch(() => {});
  return { ...c, initial, snapshot, watchReply, watching, selected, final, work,
    get finished() { return finished; },
    drain: () => messageBarrier(() => [c.fake.calls.length, c.fake.subscriptions, c.json.length, finished]),
    async close() {
      try {
        watchReply.resolve(snapshot);
        c.emit("broker/run/ended", { run: final });
        c.emit("broker/thread/result", { runId: "run-test", turn: 1, result: selected });
        await watchdog(work, 6000).catch(() => {});
      } finally {
        if (oldHome === undefined) delete process.env.CODEX_HOME; else process.env.CODEX_HOME = oldHome;
        await removeTemp(dir);
      }
    } };
}

for (const name of ["run", "review", "wait"] as const) for (const reason of ["stallTimeout", "upstreamAccessError"] as const)
for (const noticeFirst of [false, true]) for (const resultFirst of [false, true])
test(`三轮回归：${name} ${reason} ${noticeFirst ? "诊断先到" : "快照先到"} ${resultFirst ? "补读先到" : "补读后到"}交付完整失败`, async () => {
  const c = unconfirmedCommand(name, reason);
  try {
    await c.watching.wait();
    if (!noticeFirst) { c.watchReply.resolve(c.snapshot); await c.drain(); }
    c.emit("broker/run/updated", { runId: "run-test", diagnosticRevision: 3, state: "ended", status: "failed",
      interruptReason: reason, stopOutcome: "unconfirmed" });
    if (noticeFirst) c.watchReply.resolve(c.snapshot);
    await c.drain();
    expect(c.finished).toBe(false); expect(c.json).toHaveLength(0);
    if (resultFirst) {
      c.emit("broker/thread/result", { runId: "run-test", turn: 1, result: c.selected });
      await c.drain(); expect(c.finished).toBe(false);
    }
    c.emit("broker/run/ended", { run: c.final });
    if (!resultFirst) {
      await c.drain(); expect(c.finished).toBe(false); expect(c.json).toHaveLength(0);
      c.emit("broker/thread/result", { runId: "run-test", turn: 0, result: c.final.turns[0] });
      await c.drain(); expect(c.finished).toBe(false);
      c.emit("broker/thread/result", { runId: "run-test", turn: 1, result: c.selected });
    }
    expect(await watchdog(c.work)).toBe(7);
    expect(c.json).toHaveLength(1);
    expect(c.json[0]).toMatchObject({ ok: false, status: "failed", interruptReason: reason,
      stopOutcome: "unconfirmed", error: c.selected.error, finalTurnId: c.selected.turnId });
    expect(c.notices.join("\n")).toContain("[cxb] 失败原因：最终失败明细");
    expect(c.notices.join("\n")).not.toContain("旧轮错误不可冒充");
    expect(c.fake.notificationErrors).toEqual([]); expect(c.fake.subscriptions).toBe(0);
  } finally { await c.close(); }
});

for (const name of ["run", "review", "wait"] as const) for (const reason of ["stallTimeout", "upstreamAccessError"] as const)
test(`三轮回归：${name} ${reason} 未确认且无Failed仍提前退出3`, async () => {
  const c = unconfirmedCommand(name, reason);
  try {
    await c.watching.wait();
    c.watchReply.resolve(c.snapshot); await c.drain();
    c.emit("broker/run/updated", { runId: "run-test", diagnosticRevision: 3, state: "stopping", status: null,
      interruptReason: reason, stopOutcome: "unconfirmed" });
    expect(await watchdog(c.work)).toBe(3);
    expect(c.json).toHaveLength(1);
    expect(c.json[0]).toMatchObject({ ok: false, state: "stopping", stopOutcome: "unconfirmed", interruptReason: reason });
    expect(c.json[0]).not.toHaveProperty("error");
    expect(c.notices.join("\n") + c.progress.join("\n")).not.toMatch(/失败原因|运行(?:已)?结束/);
    expect(c.fake.notificationErrors).toEqual([]); expect(c.fake.subscriptions).toBe(0);
  } finally { await c.close(); }
});

for (const throughObserver of [false, true]) for (const noticeFirst of [false, true]) test(`合并回归：${throughObserver ? "观察器包装" : "直接等待"}、${noticeFirst ? "诊断先到" : "快照先到"}不能用 ended 诊断提前交付旧完整快照`, async () => {
  const fake = new FakeRpc();
  let release!: (snapshot: unknown) => void;
  fake.handle = () => new Promise(resolve => { release = resolve; });
  const observer = observeUpstreamErrors(fake.rpc, () => "run-test", () => {});
  const controller = new AbortController();
  let finished = false;
  const work = waitRun(throughObserver ? observer.rpc : fake.rpc,
    { threadId: "thread-test", runId: "run-test", timeoutMs: 1000, signal: controller.signal }).then(run => { finished = true; return run; });
  try {
    const snapshot = { run: runResult({ state: "running", status: null, turns: [], turnsTotal: 0, diagnosticRevision: 1 }) };
    if (!noticeFirst) release(snapshot);
    await new Promise<void>(resolve => setImmediate(resolve));
    fake.emit("broker/run/updated", { runId: "run-test", diagnosticRevision: 3, state: "ended", status: "failed" });
    if (noticeFirst) release(snapshot);
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(finished).toBe(false);
    const final = runResult({ diagnosticRevision: 2, status: "failed", turns: [{ ...runResult().turns[0]!, error: { message: "完整终态错误" } }] });
    fake.emit("broker/run/ended", { run: final });
    const result = await work;
    expect(result.turns).toEqual(final.turns);
    expect(result.diagnosticRevision).toBe(3);
    expect(result.state).toBe("ended"); expect(result.status).toBe("failed");
  } finally { controller.abort(); await work.catch(() => {}); observer.dispose(); }
  expect(fake.notificationErrors).toEqual([]); expect(fake.subscriptions).toBe(0);
});
