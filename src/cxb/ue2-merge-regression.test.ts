import { expect, test } from "bun:test";
import type { BrokerConnection, RunResult } from "./contracts";
import { createRunHandler } from "./run";
import { wait } from "./wait";
import { observeUpstreamErrors } from "./upstream-errors";
import { bounded, waitRun } from "./wait-run";
import { FakeRpc, runResult } from "./test-fixtures/run-fakes";
import { RunHarness, fakeServerRequests, until } from "./test-fixtures/run-harness";
import { StallHarness } from "./test-fixtures/stall-harness";

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
    let watching = false;
    const request = waitingConnection.rpc.request.bind(waitingConnection.rpc);
    waitingConnection.rpc.request = (method, params, options) => {
      if (method === "broker/threads/watch") watching = true;
      return request(method, params, options);
    };
    const waiting = h.invoke(wait, ["wait", run.threadId, "--json"], waitingConnection);
    await until(() => watching);
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
    const proc = h.run(["--json", "--stall-timeout", "1"]);
    await h.started();
    const turnId = (await h.state()).threads.root.turns[0].id;
    await h.sa1b("spawn", { parentThreadId: "root", parentTurnId: turnId, childThreadId: "child" });
    expect(await bounded(proc.closed, 20000)).toBe(7);
    const record = await h.persisted(), final = JSON.parse(proc.output().stdout);
    const diagnosticState = (run: any) => ({ revision: run.diagnosticRevision, state: run.state, status: run.status, interruptReason: run.interruptReason });
    expect(final.interruptReason, JSON.stringify({ record: diagnosticState(record), updates: h.updated().map(event => diagnosticState(event.params)),
      ended: h.events.filter(event => event.method === "broker/run/ended").map(event => diagnosticState(event.params.run)) })).toBe("stallTimeout");
    expect(final.error.message).toBe("测试失败");
    expect(final.subagentJoin).toEqual(record.subagentJoin);
  } finally { await h.close(); }
}, 90000);

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
