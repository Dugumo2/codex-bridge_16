import { afterEach, expect, spyOn, test } from "bun:test";
import { Writable } from "node:stream";
import { wait } from "./wait";
import { createOutput } from "./output";
import { cleanupContexts, context, errorCode, gate, item, thread } from "./test-fixtures/n4b-support";
import { runResult } from "./test-fixtures/run-fakes";
import { messageBarrier, watchdog } from "./test-fixtures/test-watchdog";
import { CxbError, type ChildrenStopReport, type RunResult } from "./contracts";

afterEach(cleanupContexts);
const child = { threadId: "child", parentThreadId: "thread-test", relationKind: "subagent" as const,
  turnIds: ["child-turn"], evidence: "v2Started" as const, finalState: "settled" as const };
const ended = (patch: Partial<RunResult> = {}) => runResult({ children: [child], ...patch });
const released = () => ended({ finalTurnId: "turn-test", subagentJoin: { phase: "released", wakeupsStarted: 1, wakeupAttempts: 1, startRetriesUsed: 0,
  workEpoch: 2, deliveredEpoch: 2, reconciledEpoch: 2, deadlineAt: 1000 } });
const children = { members: [{ ...child, activity: "settled", pendingCount: 0, conflict: null }],
  discovery: { complete: true, version: 1, incompleteReasons: [] }, inFlightSpawns: 0, stop: null };
const view = (run = ended()) => ({ thread: thread(), run, pending: [], revision: 1, children });
function wire(c: ReturnType<typeof context>) {
  let stdout = "", stderr = "";
  c.ctx.out = createOutput({ command: "wait", json: c.ctx.parsed.globals.json, verbose: c.ctx.parsed.globals.verbose,
    stdout: new Writable({ write(chunk, _encoding, done) { stdout += chunk.toString(); done(); } }),
    stderr: new Writable({ write(chunk, _encoding, done) { stderr += chunk.toString(); done(); } }) });
  return { get stdout() { return stdout; }, get stderr() { return stderr; } };
}
function handle(c: ReturnType<typeof context>, run = ended()) {
  c.fake.handle = (method) => {
    if (method === "broker/threads/list") return { threads: [thread()] };
    if (method === "broker/threads/watch") return view(run);
    if (method === "thread/turns/list") return { data: [{ id: "child-turn", status: "failed" }], nextCursor: null };
    if (method === "thread/items/list") return { data: [{ turnId: "child-turn", item: { type: "agentMessage", text: "子结果", phase: "final_answer" } }], nextCursor: null };
    throw new Error(`非预期请求 ${method}`);
  };
}

test("alreadyDone：父成功子失败、JSON仅一对象、默认无实时树、只读无resume", async () => {
  const c = context(["wait", "abcd1234", "--tree", "--json"]), output = wire(c);
  handle(c);
  expect(await watchdog(wait(c.ctx))).toBe(0); await c.ctx.out.flush();
  expect(output.stdout.trim().split("\n")).toHaveLength(1);
  const json = JSON.parse(output.stdout);
  expect(json).toMatchObject({ ok: true, result: "中文结果", alreadyDone: true, tree: { view: "terminal",
    outputCoverage: { total: 1, available: 1, truncated: 0, unavailable: 0, omitted: 0 } } });
  expect(json.tree.nodes[0].result).toMatchObject({ status: "failed", output: "子结果", turnId: "child-turn" });
  expect(output.stderr).toBe("");
  expect(c.fake.calls.map((call) => call.method)).toEqual(["broker/threads/list", "broker/threads/watch", "broker/threads/watch", "thread/turns/list", "thread/items/list"]);
  c.checkClean();
});

test("正文读取失败保留父0和正文，不把失败升级为父失败", async () => {
  const c = context(["wait", "abcd1234", "--tree", "--json"]);
  handle(c); const original = c.fake.handle;
  c.fake.handle = (method, params) => { if (method === "thread/items/list") throw new Error("模拟上游读失败"); return original(method, params); };
  expect(await watchdog(wait(c.ctx))).toBe(0);
  expect(c.json).toHaveLength(1);
  expect(c.json[0]).toMatchObject({ ok: true, result: "中文结果" });
  expect(c.json[0].tree.nodes[0].result).toMatchObject({ status: "failed", availability: "unavailable", output: "" });
  c.checkClean();
});

test("补读中原命令期限到达仍交付父结果，迟到接替和事项不改变出口", async () => {
  const c = context(["wait", "abcd1234", "--tree", "--json"]), reached = gate<void>(), reply = gate<unknown>();
  handle(c); const original = c.fake.handle;
  let now = performance.now();
  const clock = spyOn(performance, "now").mockImplementation(() => now);
  const work = wait(c.ctx);
  c.fake.handle = (method, params) => {
    if (method === "thread/items/list") { reached.resolve(); return reply.promise; }
    return original(method, params);
  };
  try {
    await reached.wait();
    now += 3_600_001;
    c.fake.emit("broker/wait/superseded", { threadId: "thread-test" });
    c.fake.emit("broker/inbox/added", { item: item() });
    c.fake.emit("broker/run/childChanged", { runId: "run-test", rootThreadId: "thread-test", threadId: "late-child", revision: 99 });
    reply.resolve({ data: [{ turnId: "child-turn", item: { type: "agentMessage", text: "过期正文" } }], nextCursor: null });
    expect(await watchdog(work)).toBe(0);
    expect(c.json).toHaveLength(1);
    expect(c.json[0]).toMatchObject({ result: "中文结果", tree: { nodes: [{ threadId: "child", result: { availability: "unavailable", output: "" } }] } });
    expect(c.notices).not.toContain("[cxb] 本次等待已被同一线程的新等待接替，已退出。");
    c.checkClean();
  } finally { clock.mockRestore(); await c.cancel(); await work.catch(() => undefined); }
});

test("实际补读计时到期不受外层bounded改判3，迟到回包不追加JSON", async () => {
  const c = context(["wait", "abcd1234", "--tree", "--json"]), reached = gate<void>(), reply = gate<unknown>();
  c.ctx.parsed.globals.timeout = 0.1;
  const pendingTimers = new Map<ReturnType<typeof setTimeout>, () => void>();
  const originalSet = globalThis.setTimeout, originalClear = globalThis.clearTimeout;
  // 扣住短期限计时器；补读开始后按登记顺序放行，稳定覆盖旧外层 race 先到期的分支。
  const timerSpy = spyOn(globalThis, "setTimeout").mockImplementation(((callback: (...args: any[]) => void, ms?: number, ...args: any[]) => {
    if (ms === undefined || ms > 150) return originalSet(callback, ms, ...args);
    const timer = originalSet(() => {}, 60_000);
    pendingTimers.set(timer, () => callback(...args));
    return timer;
  }) as typeof setTimeout);
  const clearSpy = spyOn(globalThis, "clearTimeout").mockImplementation(((timer: ReturnType<typeof setTimeout>) => {
    pendingTimers.delete(timer); originalClear(timer);
  }) as typeof clearTimeout);
  handle(c); const original = c.fake.handle;
  c.fake.handle = (method, params) => { if (method === "thread/items/list") { reached.resolve(); return reply.promise; } return original(method, params); };
  const work = wait(c.ctx);
  try {
    await reached.wait();
    expect(pendingTimers.size).toBeGreaterThan(0);
    for (const [timer, callback] of [...pendingTimers]) { clearTimeout(timer); callback(); }
    expect(await watchdog(work)).toBe(0);
    expect(c.json[0].tree.nodes[0].result.availability).toBe("unavailable");
    const before = JSON.stringify(c.json);
    reply.resolve({ data: [], nextCursor: null });
    await messageBarrier(() => [c.json.length]);
    expect(JSON.stringify(c.json)).toBe(before); c.checkClean();
  } finally {
    await c.cancel(); await work.catch(() => undefined);
    for (const timer of pendingTimers.keys()) originalClear(timer);
    timerSpy.mockRestore(); clearSpy.mockRestore();
  }
});

test("初始和缓冲子事项退出5，partial树且不补读正文", async () => {
  for (const buffered of [false, true]) {
    const c = context(["wait", "abcd1234", "--tree", "--json"]);
    c.fake.handle = (method) => {
      if (method === "broker/threads/list") return { threads: [thread()] };
      if (buffered) c.fake.emit("broker/inbox/added", { item: item({ threadId: "child" }) });
      return { ...view(ended({ state: "running", status: null })), pending: buffered ? [] : [item({ threadId: "child" })] };
    };
    expect(await watchdog(wait(c.ctx))).toBe(5);
    expect(c.json).toHaveLength(1); expect(c.json[0].tree.view).toBe("partial");
    expect(c.fake.calls.some((call) => call.method.startsWith("thread/"))).toBe(false); c.checkClean();
  }
});

test("等待中接替10不输出终态树，未选父结果时到期仍3", async () => {
  for (const mode of ["superseded", "timeout"] as const) {
    const c = context(["wait", "abcd1234", "--tree", "--json"]), ready = gate<void>();
    c.ctx.parsed.globals.timeout = mode === "timeout" ? 0.05 : 5;
    c.fake.handle = (method, params) => {
      if (method === "broker/threads/list") return { threads: [thread()] };
      if (!params.exclusiveWaiter) ready.resolve();
      return view(ended({ state: "running", status: null }));
    };
    const work = wait(c.ctx); await ready.wait();
    if (mode === "superseded") {
      c.fake.emit("broker/wait/superseded", { threadId: "thread-test" });
      expect(await watchdog(work)).toBe(10); expect(c.json).toHaveLength(1);
      expect(c.json[0].tree).toBeUndefined();
    } else { await errorCode(work, 3); expect(c.json).toEqual([]); }
    c.checkClean();
  }
});

test("父最终结果pending时子树settled不得促使返回", async () => {
  const c = context(["wait", "abcd1234", "--tree", "--json"]), ready = gate<void>();
  const run = ended({ finalTurnId: "final", turns: [{ turnId: "final", ready: "pending", status: "completed", output: "", outputSource: "none", error: null, completedAt: 2 }] });
  handle(c, run); const original = c.fake.handle;
  c.fake.handle = (method, params) => { if (method === "broker/threads/watch" && !params.exclusiveWaiter) ready.resolve(); return original(method, params); };
  const work = wait(c.ctx); await ready.wait();
  await messageBarrier(() => [c.fake.calls.length]);
  expect(c.json).toEqual([]); expect(c.fake.calls.some((call) => call.method.startsWith("thread/"))).toBe(false);
  c.fake.emit("broker/thread/result", { runId: "run-test", turn: 0, result: { ...run.turns[0], ready: "ready", output: "最终父结果" } });
  expect(await watchdog(work)).toBe(0); expect(c.json[0].result).toBe("最终父结果"); c.checkClean();
});

for (const [reason, status, code] of [["stallTimeout", null, 3], ["upstreamAccessError", null, 3], ["upstreamAccessError", "failed", 7]] as const) {
  test(`停止诊断提前${code}附partial树且不补读`, async () => {
    const c = context(["wait", "abcd1234", "--tree", "--json"]);
    handle(c, ended({ state: "stopping", status, interruptReason: reason, stopOutcome: "unconfirmed" }));
    expect(await watchdog(wait(c.ctx))).toBe(code); expect(c.json[0].tree.view).toBe("partial");
    expect(c.fake.calls.some((call) => call.method.startsWith("thread/"))).toBe(false); c.checkClean();
  });
}

test("访问报错停止已落定仍保持既有退出4", async () => {
  const c = context(["wait", "abcd1234", "--tree", "--json"]);
  handle(c, ended({ status: "interrupted", interruptReason: "upstreamAccessError", stopOutcome: "confirmed" }));
  expect(await watchdog(wait(c.ctx))).toBe(4); expect(c.json[0].tree.view).toBe("terminal"); c.checkClean();
});

test("文本先父后子，alreadyDone不演示实时树且不重复数量行", async () => {
  const c = context(["wait", "abcd1234", "--tree"]), output = wire(c);
  handle(c); expect(await watchdog(wait(c.ctx))).toBe(0); await c.ctx.out.flush();
  expect(output.stdout.startsWith("中文结果\n子代理结果（本次运行最近受管轮）：")).toBe(true);
  expect(output.stdout).toContain("子结果"); expect(output.stderr).toBe(""); c.checkClean();
});

for (const held of ["broker/threads/list", "broker/threads/watch"]) test(`树模式等待裁决仍约束初始化期限：${held}`, async () => {
  const c = context(["wait", "abcd1234", "--tree", "--json"]), reply = gate<unknown>();
  c.ctx.parsed.globals.timeout = 0.04;
  handle(c); const original = c.fake.handle;
  c.fake.handle = (method, params) => method === held ? reply.promise : original(method, params);
  await errorCode(wait(c.ctx), 3);
  expect(c.json).toEqual([]); c.checkClean();
  reply.resolve(held === "broker/threads/list" ? { threads: [thread()] } : view());
  await messageBarrier(() => [c.json.length, c.fake.calls.length]);
  expect(c.json).toEqual([]); c.checkClean();
});

test("树模式等待中断连保持6，取消等待解除订阅", async () => {
  for (const disconnect of [true, false]) {
    const c = context(["wait", "abcd1234", "--tree", "--json"]), ready = gate<void>();
    handle(c, ended({ state: "running", status: null })); const original = c.fake.handle;
    c.fake.handle = (method, params) => { if (method === "broker/threads/watch" && !params.exclusiveWaiter) ready.resolve(); return original(method, params); };
    const work = wait(c.ctx); await ready.wait();
    if (disconnect) c.fake.disconnect(); else await c.cancel();
    await errorCode(work, disconnect ? 6 : 4);
    expect(c.json).toEqual([]); c.checkClean();
  }
});

for (const verbose of [false, true]) test(`活动树 JSON 实时变化仅在 verbose=${verbose} 时写 stderr`, async () => {
  const c = context(["wait", "abcd1234", "--tree", "--json", ...(verbose ? ["-v"] : [])]), ready = gate<void>(), output = wire(c);
  handle(c, ended({ state: "running", status: null })); const original = c.fake.handle;
  c.fake.handle = (method, params) => { if (method === "broker/threads/watch" && !params.exclusiveWaiter) ready.resolve(); return original(method, params); };
  const work = wait(c.ctx); await ready.wait();
  c.fake.emit("broker/run/childChanged", { runId: "run-test", rootThreadId: "thread-test", threadId: "child", parentThreadId: "thread-test",
    activity: "running", pendingCount: 2, revision: 2 });
  await c.ctx.out.flush();
  expect(output.stdout).toBe("");
  if (verbose) expect(output.stderr).toContain("待处理 2 项"); else expect(output.stderr).toBe("");
  c.fake.emit("broker/run/ended", { run: ended() });
  expect(await watchdog(work)).toBe(0); await c.ctx.out.flush();
  expect(JSON.parse(output.stdout).tree.nodes).toHaveLength(1); c.checkClean();
});

test("第二次 watch 挂起：命令等待期限退出3且 stdout 留空", async () => {
  const c = context(["wait", "abcd1234", "--tree", "--json", "--timeout", "1"]), output = wire(c);
  const watching = gate<void>(), reply = gate<unknown>();
  handle(c, ended({ state: "running", status: null })); const original = c.fake.handle;
  c.fake.handle = (method, params) => {
    if (method === "broker/threads/watch" && !params.exclusiveWaiter) { watching.resolve(); return reply.promise; }
    return original(method, params);
  };
  const work = wait(c.ctx);
  try {
    await watching.wait();
    const failure = await watchdog(work.then(() => null, (error: unknown) => error));
    expect(failure).toBeInstanceOf(CxbError);
    expect((failure as CxbError).exitCode).toBe(3);
    c.ctx.out.error(failure); await c.ctx.out.flush();
    expect(output.stdout).toBe("");
    expect(JSON.parse(output.stderr).error.code).toBe("TIMEOUT");
    expect(c.fake.calls.filter((call) => call.method === "broker/threads/watch").map((call) => call.params)).toEqual([
      { threadId: "thread-test", exclusiveWaiter: true }, { threadId: "thread-test", runId: "run-test" },
    ]);
    c.checkClean();
  } finally {
    reply.resolve(view()); await c.cancel(); await work.catch(() => undefined);
  }
});

for (const alreadyDone of [false, true]) test(`真实停止摘要与完整报告共存，alreadyDone=${alreadyDone} 正常退出4`, async () => {
  const c = context(["wait", "abcd1234", "--tree", "--json"]), output = wire(c), watching = gate<void>();
  const report: ChildrenStopReport = { trigger: "userStop", phase: "unconfirmed", version: 8,
    root: { thread: "thread-test", phase: "confirmed", check: { ownership: true, turnEnded: true, goalBraked: true, settled: true, verifyDone: true },
      clearRequested: false, clearConfirmed: false, error: null }, stopped: [], unconfirmed: ["child"], failed: [], conflicts: [], capabilityLimited: [],
    discovery: { complete: true, version: 2, incompleteReasons: [] }, unsettledSpawns: 0 };
  const final = ended({ status: "interrupted", interruptReason: "stop", stopOutcome: "unconfirmed", childrenStop: report });
  handle(c, final); const original = c.fake.handle;
  c.fake.handle = (method, params) => {
    if (method !== "broker/threads/watch") return original(method, params);
    if (!params.exclusiveWaiter) watching.resolve();
    const run = alreadyDone ? final : { ...final, state: "stopping" as const, status: null };
    return { ...view(run), children: { ...children, stop: { phase: report.phase, version: 8, unconfirmed: 1, conflicts: 0 } } };
  };
  const work = wait(c.ctx);
  try {
    if (!alreadyDone) { await watching.wait(); c.fake.emit("broker/run/ended", { run: final }); }
    expect(await watchdog(work)).toBe(4); await c.ctx.out.flush();
    const result = JSON.parse(output.stdout);
    expect(result).toMatchObject({ alreadyDone, status: "interrupted", tree: { stopVersion: 8,
      stopSummary: { phase: "unconfirmed", version: 8, unconfirmed: 1, conflicts: 0 }, nodes: [{ threadId: "child", stop: { source: "runRecord", phase: "unconfirmed" } }] } });
    expect(result.tree.nodes[0].stop.issues).toContain("尚未确认停下"); c.checkClean();
  } finally { await c.cancel(); await work.catch(() => undefined); }
});

for (const inFlight of [false, true]) for (const json of [false, true]) {
  test(`完整终态不沿用在线陈旧标记：${inFlight ? "快照在途" : "合并窗口"}，${json ? "JSON" : "文本"}`, async () => {
    const c = context(["wait", "abcd1234", "--tree", ...(json ? ["--json"] : [])]), output = wire(c);
    const watching = gate<void>(), refreshing = gate<void>(), reply = gate<unknown>();
    const originalSet = globalThis.setTimeout;
    const heldTimers = new Set<ReturnType<typeof setTimeout>>();
    let release: (() => void) | undefined;
    const timerSpy = spyOn(globalThis, "setTimeout").mockImplementation(((callback: (...args: any[]) => void, ms?: number, ...args: any[]) => {
      if (ms !== 200) return originalSet(callback, ms, ...args);
      const timer = originalSet(() => {}, 60_000); heldTimers.add(timer);
      release = () => { clearTimeout(timer); heldTimers.delete(timer); callback(...args); };
      return timer;
    }) as typeof setTimeout);
    handle(c); const original = c.fake.handle;
    let watches = 0;
    c.fake.handle = (method, params) => {
      if (method !== "broker/threads/watch") return original(method, params);
      watches++;
      if (watches === 2) watching.resolve();
      if (watches > 2) { refreshing.resolve(); return reply.promise; }
      return { ...view(ended({ state: "running", status: null })), children: { ...children, inFlightSpawns: 1 } };
    };
    const work = wait(c.ctx);
    try {
      await watching.wait();
      c.fake.emit("broker/run/childChanged", { runId: "run-test", rootThreadId: "thread-test", threadId: null, revision: 2 });
      expect(release).toBeDefined();
      if (inFlight) { release!(); await refreshing.wait(); }
      c.fake.emit("broker/run/ended", { run: released() });
      expect(await watchdog(work)).toBe(0); await c.ctx.out.flush();
      if (json) {
        expect(JSON.parse(output.stdout)).toMatchObject({ result: "中文结果", tree: { view: "terminal", source: "runRecord", revision: null,
          inFlightSpawns: null, discovery: null, nodes: [{ threadId: "child", pendingCount: null }] } });
        expect(JSON.parse(output.stdout).tree.stale).toBeUndefined();
      }
      else {
        expect(output.stdout).not.toContain("树信息可能陈旧");
        expect(output.stdout).not.toContain("在途派生 1 个");
      }
      const before = output.stdout;
      reply.resolve({ ...view(), revision: 2 });
      await messageBarrier(() => [output.stdout.length, c.fake.calls.length]);
      expect(output.stdout).toBe(before); c.checkClean();
      expect(watches).toBe(inFlight ? 3 : 2);
    } finally {
      reply.resolve(view()); await c.cancel(); await work.catch(() => undefined);
      timerSpy.mockRestore(); for (const timer of heldTimers) clearTimeout(timer);
    }
  });
}

for (const json of [false, true]) test(`释放后 watch revision 回落0，再收到 ended：${json ? "JSON" : "文本"}终态不报陈旧`, async () => {
  const c = context(["wait", "abcd1234", "--tree", ...(json ? ["--json"] : [])]), output = wire(c);
  const watching = gate<void>(), refreshed = gate<void>();
  const originalSet = globalThis.setTimeout;
  let timer: ReturnType<typeof setTimeout> | undefined, release: (() => void) | undefined;
  const timerSpy = spyOn(globalThis, "setTimeout").mockImplementation(((callback: (...args: any[]) => void, ms?: number, ...args: any[]) => {
    if (ms !== 200) return originalSet(callback, ms, ...args);
    timer = originalSet(() => {}, 60_000);
    release = () => { clearTimeout(timer); callback(...args); };
    return timer;
  }) as typeof setTimeout);
  handle(c); const original = c.fake.handle;
  let watches = 0;
  c.fake.handle = (method, params) => {
    if (method !== "broker/threads/watch") return original(method, params);
    watches++;
    if (watches === 2) watching.resolve();
    if (watches > 2) {
      refreshed.resolve();
      return { ...view(released()), revision: 0, children: { members: [], discovery: null, inFlightSpawns: 0, stop: null } };
    }
    return { ...view(ended({ state: "running", status: null })), revision: 10,
      children: { ...children, discovery: { complete: false, version: 3, incompleteReasons: ["早期发现未完成"] } } };
  };
  const work = wait(c.ctx);
  try {
    await watching.wait();
    c.fake.emit("broker/run/childChanged", { runId: "run-test", rootThreadId: "thread-test", threadId: null, revision: 12 });
    expect(release).toBeDefined(); release!(); await refreshed.wait();
    await messageBarrier(() => [c.fake.calls.length]);
    c.fake.emit("broker/run/ended", { run: released() });
    expect(await watchdog(work)).toBe(0); await c.ctx.out.flush();
    if (json) {
      const result = JSON.parse(output.stdout);
      expect(result.tree.stale).toBeUndefined();
      expect(result.tree).toMatchObject({ source: "runRecord", revision: null, discovery: null, inFlightSpawns: null,
        nodes: [{ threadId: "child", result: { turnId: "child-turn", output: "子结果" } }] });
    } else {
      expect(output.stdout).not.toContain("树信息可能陈旧");
      expect(output.stdout).not.toContain("早期发现未完成");
      expect(output.stdout).toContain("子结果");
    }
    expect(watches).toBe(3); c.checkClean();
  } finally { await c.cancel(); await work.catch(() => undefined); clearTimeout(timer); timerSpy.mockRestore(); }
});
