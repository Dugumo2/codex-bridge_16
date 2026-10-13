import { afterEach, beforeEach, expect, spyOn, test } from "bun:test";
import { mkdirSync, writeFileSync } from "node:fs";
import type { RunResult } from "./contracts";
import { join } from "node:path";
import { ASK_FEATURE_KEY, CxbError, type BrokerConnection, type ServerRequestDeps } from "./contracts";
import { ASK_INSTRUCTIONS } from "./ask-instructions";
import { createRunHandler, readPrompt } from "./run";
import { createReviewHandler } from "./review";
import { RpcError } from "../types";
import { parseArgv } from "./args";
import type { CommandContext } from "./main";
import { createOutput, registerSignalHandlers, type Cleanup } from "./output";
import { FakeRpc, runResult, wireError } from "./test-fixtures/run-fakes";
import { bounded, waitRun } from "./wait-run";
import { deferred, messageBarrier, watchdog } from "./test-fixtures/test-watchdog";
import { attachServerRequests, handover, renderPending } from "./server-requests";
import { CLEANUP_BUDGET, TEST_BUDGET, RunHarness, fakeServerRequests, until, alive } from "./test-fixtures/run-harness";
import { upstreamError } from "./test-fixtures/upstream-errors";

let h: RunHarness;
let oldHome: string | undefined;
beforeEach(() => { h = new RunHarness(); oldHome = process.env.CODEX_HOME; process.env.CODEX_HOME = h.env.CODEX_HOME; });
afterEach(async () => {
  try { await h.cleanup(); } finally {
    if (oldHome === undefined) delete process.env.CODEX_HOME; else process.env.CODEX_HOME = oldHome;
  }
}, CLEANUP_BUDGET);
const handler = () => createRunHandler(fakeServerRequests());

test("位置提示词、启动载荷、开轮前 goal、每轮沙箱和新消息 ID", async () => {
  const observer = await h.start();
  const driver = await h.connect();
  const requested: string[] = [];
  const originalRequest = driver.rpc.request;
  driver.rpc = { ...driver.rpc, request<T>(method: string, params?: unknown, opts?: { timeoutMs?: number }) {
    requested.push(method);
    return originalRequest<T>(method, params, opts);
  } };
  const first = await h.invoke(handler(), ["run", "检查", "中文", "--json", "-v", "-m", "mock-model", "-r", "high", "--approval", "auto"], driver);
  expect(first.code).toBe(0);
  const output = JSON.parse(first.stdout);
  expect(output).toMatchObject({ ok: true, command: "run", result: "完整中文结果", model: "mock-model", reasoning: "high", sandbox: "workspace-write", approval: "auto", joined: false, goal: null });
  expect(first.stderr).toContain("thread started");
  expect(first.stderr).toContain("turn started");
  expect(requested.filter((method) => method === "thread/goal/get")).toHaveLength(1);
  expect(requested.indexOf("thread/goal/get")).toBeLessThan(requested.indexOf("turn/start"));
  const second = await h.invoke(handler(), ["run", "新要求", "--resume", output.thread.shortId, "-s", "read-only", "--memory"]);
  expect(second.code).toBe(0);
  const { calls } = await observer.rpc.request<any>("fixture/state");
  const start = calls.find((call: any) => call.method === "thread/start").params;
  expect(start).toMatchObject({ config: { [ASK_FEATURE_KEY]: true, model_reasoning_effort: "high" }, developerInstructions: ASK_INSTRUCTIONS,
    model: "mock-model", cwd: h.workspace.root, sandbox: "workspace-write", approvalPolicy: "on-request", approvalsReviewer: "auto_review", ephemeral: false });
  expect(start).not.toHaveProperty("collaborationMode");
  const resume = calls.find((call: any) => call.method === "thread/resume").params;
  expect(resume).toMatchObject({ threadId: output.thread.id, config: { [ASK_FEATURE_KEY]: true }, developerInstructions: ASK_INSTRUCTIONS });
  const overrides = calls.filter((call: any) => call.method === "fixture/resumeOverrides");
  expect(overrides).toHaveLength(2);
  expect(overrides[0].params.overrides).toEqual({ config: start.config, developerInstructions: ASK_INSTRUCTIONS });
  const turns = calls.filter((call: any) => call.method === "turn/start");
  expect(turns).toHaveLength(2);
  expect(turns[0].params.input[0].text).toBe("检查 中文");
  expect(turns[0].params.sandboxPolicy).toEqual({ type: "workspaceWrite" });
  expect(turns[1].params.sandboxPolicy).toEqual({ type: "readOnly" });
  expect(turns[0].params.clientUserMessageId).toMatch(/^[\da-f-]{36}$/);
  expect(turns[1].params.clientUserMessageId).not.toBe(turns[0].params.clientUserMessageId);
  expect(calls.findIndex((call: any) => call.method === "thread/goal/get")).toBeLessThan(calls.findIndex((call: any) => call.method === "turn/start"));
  expect(calls.filter((call: any) => call.method === "thread/memoryMode/set")).toHaveLength(1);
}, TEST_BUDGET);

test("文件与标准输入保留完整 UTF-8，空来源不连接 broker", async () => {
  const observer = await h.start();
  const file = join(h.dir, "提示词.txt");
  writeFileSync(file, "  文件中文\n第二行\n");
  const fileResult = await h.invoke(handler(), ["run", "--file", file]);
  expect(fileResult.code).toBe(0);
  const child = h.spawn([process.execPath, join(import.meta.dir, "test-fixtures/run-client.ts"), "run", "-", "--json"],
    { ...h.env, RUN_FIXTURE_WORKSPACE: JSON.stringify(h.workspace) });
  child.child.stdin!.end("  管道中文\n第二行\n");
  expect(await bounded(child.closed, 15_000)).toBe(0);
  expect(JSON.parse(child.output().stdout).result).toBe("完整中文结果");
  const { calls } = await observer.rpc.request<any>("fixture/state");
  expect(calls.filter((call: any) => call.method === "turn/start").map((call: any) => call.params.input[0].text)).toEqual(["  文件中文\n第二行\n", "  管道中文\n第二行\n"]);
  writeFileSync(file, " \r\n\t");
  for (const args of [["run", "--file", file], ["run", "--file", join(h.dir, "不存在")]]) {
    const result = await h.invoke(handler(), args);
    expect(result.code).toBe(1);
    expect(result.connectCount).toBe(0);
  }
  const empty = h.spawn([process.execPath, join(import.meta.dir, "test-fixtures/run-client.ts"), "run", "-", "--json"],
    { ...h.env, RUN_FIXTURE_WORKSPACE: JSON.stringify({ ...h.workspace, pipePath: "不存在的管道" }) });
  empty.child.stdin!.end(" \n");
  expect(await bounded(empty.closed, 10_000)).toBe(1);
  expect(JSON.parse(empty.output().stderr).error.code).toBe("USAGE");
}, TEST_BUDGET);

test("模板建议与显式沙箱优先级、memory 选项", async () => {
  const observer = await h.start();
  const dir = join(h.workspace.dataRoot, "templates");
  mkdirSync(dir);
  writeFileSync(join(dir, "审阅.md"), "---\nsandbox: read-only\n---\n请审阅：{{PROMPT}}");
  expect((await h.invoke(handler(), ["run", "中文", "--template", "审阅", "--memory"])).code).toBe(0);
  expect((await h.invoke(handler(), ["run", "中文", "--template", "审阅", "--memory", "-s", "workspace-write"])).code).toBe(0);
  const { calls } = await observer.rpc.request<any>("fixture/state");
  const starts = calls.filter((call: any) => call.method === "thread/start");
  expect(starts.map((call: any) => call.params.sandbox)).toEqual(["read-only", "workspace-write"]);
  expect(calls.some((call: any) => call.method === "thread/memoryMode/set")).toBe(false);
  expect(calls.find((call: any) => call.method === "turn/start").params.input[0].text).toBe("请审阅：中文");
  const absent = await h.invoke(handler(), ["run", "中文", "--template", "不存在"]);
  expect(absent.code).toBe(1);
  expect(absent.connectCount).toBe(0);
}, TEST_BUDGET);

for (const scenario of ["warnAsk", "rejectAsk"] as const) test(`未知配置键：${scenario === "warnAsk" ? "告警提示" : "去键后仅重试一次且登记无该键"}`, async () => {
  const observer = await h.start({ [scenario]: true });
  const result = await h.invoke(handler(), ["run", "中文", "--json", "-v"]);
  expect(result.code).toBe(0);
  expect(result.stderr.match(/提问工具不可用，将退回文字提问/g)).toHaveLength(1);
  expect(result.stderr).not.toContain("秘密不应回显");
  const { calls } = await observer.rpc.request<any>("fixture/state");
  const starts = calls.filter((call: any) => call.method === "thread/start");
  expect(starts).toHaveLength(scenario === "rejectAsk" ? 2 : 1);
  const registered = calls.find((call: any) => call.method === "fixture/resumeOverrides").params.overrides;
  if (scenario === "rejectAsk") {
    expect(starts[1].params.config).not.toHaveProperty(ASK_FEATURE_KEY);
    expect(registered.config).not.toHaveProperty(ASK_FEATURE_KEY);
  } else expect(registered.config[ASK_FEATURE_KEY]).toBe(true);
  expect(registered.developerInstructions).toBe(ASK_INSTRUCTIONS);
}, TEST_BUDGET);

test("resume 未知键重试先清除旧登记，成功后只登记有效覆盖", async () => {
  const observer = await h.start({ rejectAsk: true });
  const first = await h.invoke(handler(), ["run", "中文", "--json"]);
  const id = JSON.parse(first.stdout).thread.id;
  const second = await h.invoke(handler(), ["run", "继续新要求", "--resume", id]);
  expect(second.code).toBe(0);
  const { calls } = await observer.rpc.request<any>("fixture/state");
  const resumes = calls.filter((call: any) => call.method === "thread/resume");
  expect(resumes).toHaveLength(2);
  expect(resumes[0].params.config[ASK_FEATURE_KEY]).toBe(true);
  expect(resumes[1].params.config).not.toHaveProperty(ASK_FEATURE_KEY);
  const registrations = calls.filter((call: any) => call.method === "fixture/resumeOverrides");
  expect(registrations.at(-2).params.overrides).toBeNull();
  expect(registrations.at(-1).params.overrides.config).not.toHaveProperty(ASK_FEATURE_KEY);
}, TEST_BUDGET);

test("审批预检失败不阻止 run；风险提醒恰好一条", async () => {
  await h.start();
  const file = join(h.env.CODEX_HOME!, "config.toml");
  writeFileSync(file, "broken = [");
  const unavailable = await h.invoke(handler(), ["run", "中文", "-v"]);
  expect(unavailable.code).toBe(0);
  expect(unavailable.stderr).toContain("approval preflight: unavailable");
  writeFileSync(file, "[mcp_servers.one]\n[mcp_servers.two]\n");
  const risky = await h.invoke(handler(), ["run", "中文"]);
  expect(risky.code).toBe(0);
  expect(risky.stderr.split("\n").filter(Boolean)).toHaveLength(1);
}, TEST_BUDGET);

for (const mode of ["normal", "alreadyDetached", "runEnded"] as const) test(`提前提问的交接：${mode}，不等待启动回包`, async () => {
  const observer = await h.start({ earlyQuestion: true });
  const result = await h.invoke(createRunHandler(fakeServerRequests(mode)), ["run", "中文", "--json", "--approval", "on-request"]);
  expect(result.code).toBe(mode === "runEnded" ? 0 : 5);
  const payload = JSON.parse(result.stdout);
  if (mode !== "runEnded") {
    expect(payload.status).toBe("waiting");
    expect(payload.pending[0].id).toMatch(/^qu_/);
    const snapshot = await observer.rpc.request<any>("broker/threads/watch", { threadId: payload.threadId });
    expect(snapshot.run.state).toBe("running");
    expect(snapshot.thread.owner).toBe("broker");
    const { calls } = await observer.rpc.request<any>("fixture/state");
    expect(calls.some((call: any) => call.method === "turn/interrupt")).toBe(false);
  } else expect(payload.result).toBe("完整中文结果");
}, TEST_BUDGET);

test("交接发现 runEnded 但补读仍 pending 时等待补全", async () => {
  const observer = await h.start({ earlyQuestion: true, pendingResult: true });
  let finished = false;
  const work = h.invoke(createRunHandler(fakeServerRequests("runEnded")), ["run", "中文", "--json"]).then((value) => { finished = true; return value; });
  await until(async () => (await observer.rpc.request<any>("fixture/state")).pendingReads > 0);
  expect(finished).toBe(false);
  await observer.rpc.request("fixture/control", { action: "release" });
  expect((await work).code).toBe(0);
}, TEST_BUDGET);

test("detach 先托管再返回运行句柄；被占退出八并给 wait 提示", async () => {
  const observer = await h.start({ hold: true });
  const detached = await h.invoke(handler(), ["run", "中文", "--detach", "--json"]);
  expect(detached.code).toBe(0);
  const output = JSON.parse(detached.stdout);
  expect(output.status).toBe("running");
  const snapshot = await observer.rpc.request<any>("broker/threads/watch", { threadId: output.threadId });
  expect(snapshot.thread.owner).toBe("broker");
  expect(snapshot.run.runId).toBe(output.runId);
  const busy = await h.invoke(handler(), ["run", "中文", "--resume", output.threadId]);
  expect(busy.code).toBe(8);
  expect(busy.stderr).toContain("cxb wait");
}, TEST_BUDGET);

test("私有 goal 自动轮空转由真实 broker 刹车，退出九并输出种类与轮数", async () => {
  const observer = await h.start({ hold: true, backgroundTerminals: { enabled: true } });
  const work = h.invoke(handler(), ["run", "中文", "--goal", "完成目标", "--budget", "10000", "--json"]);
  await until(async () => (await observer.rpc.request<any>("fixture/state")).calls.some((call: any) => call.method === "turn/start"));
  await observer.rpc.request("fixture/control", { action: "finish", threadId: "thread-1" });
  for (let index = 0; index < 3; index++) await observer.rpc.request("fixture/control", { action: "next", threadId: "thread-1" });
  const result = await work;
  expect(result.code).toBe(9);
  expect(result.stderr).toContain("连续无动作，3 轮");
  expect(JSON.parse(result.stdout).loopGuard).toMatchObject({ kind: "noAction", count: 3, action: "stop" });
  const { calls } = await observer.rpc.request<any>("fixture/state");
  expect(calls.find((call: any) => call.method === "thread/goal/set").params).toMatchObject({ objective: "完成目标", tokenBudget: 10000 });
}, TEST_BUDGET);

test("超时退出三并关闭连接；失败状态退出七", async () => {
  await h.start({ hold: true });
  const result = await h.invoke(handler(), ["run", "中文", "--timeout", "1", "--json"]);
  expect(result.code).toBe(3);
  expect(JSON.parse(result.stderr).error.code).toBe("TIMEOUT");
}, TEST_BUDGET);

test("大于 64 KiB 的中文 JSON 经独立进程管道完整输出并自然退出", async () => {
  const output = "这是中文完整结果。\n".repeat(16000);
  await h.start({ output });
  const child = h.spawn([process.execPath, join(import.meta.dir, "test-fixtures/run-client.ts"), "run", "验证大输出", "--json", "-v"],
    { ...h.env, RUN_FIXTURE_WORKSPACE: JSON.stringify(h.workspace) });
  child.child.stdin!.end();
  expect(await bounded(child.closed, 20_000)).toBe(0);
  expect(alive(child.child.pid!)).toBe(false);
  expect(JSON.parse(child.output().stdout).result).toBe(output);
  expect(child.output().stderr).toContain("运行结束");
}, TEST_BUDGET);

test("标准输入不结束时按期限拒绝等待并自然退出", async () => {
  const child = h.spawn([process.execPath, join(import.meta.dir, "test-fixtures/run-client.ts"), "run", "-", "--timeout", "1", "--json"],
    { ...h.env, RUN_FIXTURE_WORKSPACE: JSON.stringify(h.workspace) });
  child.child.stdin!.write("尚未到 EOF");
  expect(await bounded(child.closed, 8000)).toBe(3);
  expect(JSON.parse(child.output().stderr).error.code).toBe("TIMEOUT");
}, TEST_BUDGET);

for (const [signal, code] of [["SIGINT", 130], ["SIGTERM", 143]] as const) test(`${signal} 经输出层清理机制先 stop 后退出 ${code}`, async () => {
  const observer = await h.start({ hold: true, backgroundTerminals: { enabled: true } });
  const child = h.spawn([process.execPath, join(import.meta.dir, "test-fixtures/run-client.ts"), "run", "信号测试", "--json"],
    { ...h.env, RUN_FIXTURE_WORKSPACE: JSON.stringify(h.workspace), RUN_FIXTURE_SIGNAL: signal });
  child.child.stdin!.end();
  expect(await bounded(child.closed, 15_000)).toBe(code);
  const state = await observer.rpc.request<any>("fixture/state");
  expect(state.calls.some((call: any) => call.method === "turn/interrupt")).toBe(true);
  expect(alive(child.child.pid!)).toBe(false);
}, TEST_BUDGET);

test("真实 broker 断开唤醒 WaitRun", async () => {
  const observer = await h.start({ hold: true });
  const detached = await h.invoke(handler(), ["run", "中文", "--detach", "--json"]);
  const output = JSON.parse(detached.stdout);
  const work = waitRun(observer.rpc, { threadId: output.threadId, runId: output.runId, timeoutMs: 5000 });
  h.broker!.kill("SIGKILL");
  await expect(work).rejects.toMatchObject({ code: "BROKER_UNAVAILABLE" });
}, TEST_BUDGET);

function unitExecution(args: string[], server: "private" | "shared" = "private") {
  const fake = new FakeRpc();
  let closed = false;
  const connection: BrokerConnection = { rpc: fake.rpc, handshake: { userAgent: "桩", brokerVersion: "0.1.0", protocolVersion: 1, epoch: 1, server: { kind: server } },
    launched: false, close: async () => { closed = true; } };
  const capture = h.capture();
  const callbacks = new Set<Cleanup>();
  h.disposers.push(async () => { await Promise.all([...callbacks].map((callback) => callback())); });
  h.disposers.push(() => { expect(fake.notificationErrors).toEqual([]); });
  const parsed = parseArgv(args);
  const out = createOutput({ command: "run", json: parsed.globals.json, verbose: parsed.globals.verbose, stdout: capture.output, stderr: capture.diagnostic });
  const ctx: CommandContext = { parsed, out, workspace: h.workspace, connect: async () => connection,
    registerCleanup(callback) { callbacks.add(callback); return () => { callbacks.delete(callback); }; } };
  fake.handle = (method) => {
    if (method === "broker/runs/modelSummaries") return { runs: [] };
    if (method === "model/list") return { data: [{ id: "mock-model", isDefault: true, upgrade: null,
      supportedReasoningEfforts: [{ reasoningEffort: "high" }] }], nextCursor: null };
    if (method === "thread/start") return { thread: { id: "thread-test" } };
    if (method === "broker/threads/annotate") return { shortId: "abcd1234" };
    if (method === "thread/goal/get") return { goal: null };
    if (method === "turn/start") { fake.emit("broker/run/started", { threadId: "thread-test", runId: "run-test" }); return { runId: "run-test" }; }
    if (method === "broker/threads/watch") return { run: runResult() };
    if (method === "broker/health") return { survivesParentJob: false };
    return {};
  };
  return { fake, ctx, callbacks, out, output: capture.value, closed: () => closed };
}

test("执行层空白与来源互斥不连接，清理标准输入前即可拒绝", async () => {
  const unit = unitExecution(["run", "占位"]);
  for (const [positional, file] of [[[" \n"], undefined], [["-", "额外"], undefined], [["位置"], "文件"]] as const) {
    unit.ctx.parsed.positional = [...positional];
    unit.ctx.parsed.options.file = file;
    await expect(readPrompt(unit.ctx)).rejects.toMatchObject({ code: "USAGE" });
    expect(unit.fake.calls).toHaveLength(0);
  }
});

test("信号清理先发期限至少 45 秒的 stop，确认后才关闭连接", async () => {
  const unit = unitExecution(["run", "中文"]);
  const original = unit.fake.handle;
  let release!: () => void;
  let stopRequested!: () => void;
  const stopping = new Promise<void>((resolve) => { stopRequested = resolve; });
  unit.fake.handle = (method, params) => {
    if (method === "broker/threads/watch") return { run: runResult({ state: "running", status: null }) };
    if (method === "broker/threads/stop") {
      stopRequested();
      return new Promise<void>((resolve) => { release = resolve; });
    }
    return original(method, params);
  };
  const work = createRunHandler(fakeServerRequests())(unit.ctx);
  void work.catch(() => {});
  await until(() => unit.fake.calls.some((call) => call.method === "broker/threads/watch"));
  const cleanup = Promise.all([...unit.callbacks].map((callback) => callback("SIGINT")));
  await bounded(stopping, 1000);
  expect(unit.fake.calls.find((call) => call.method === "broker/threads/stop")!.options.timeoutMs).toBeGreaterThanOrEqual(45_000);
  expect(unit.closed()).toBe(false);
  release();
  await bounded(cleanup, 1000);
  await expect(work).rejects.toMatchObject({ code: "INTERRUPTED" });
  expect(unit.closed()).toBe(true);
  expect(unit.fake.subscriptions).toBe(0);
});

function reasonError(reason: string) {
  return new CxbError("BROKER_UNAVAILABLE", "远端请求失败", {
    cause: new RpcError(`JSON-RPC error -32000: 扩展请求未完成：${reason}`, -32000, "扩展请求未完成", { reason }),
  });
}

test("信号清理的 stop 带本次 runId；broker 回 runMismatch 不算清理失败", async () => {
  const unit = unitExecution(["run", "中文"]);
  const original = unit.fake.handle;
  unit.fake.handle = (method, params) => {
    if (method === "broker/threads/watch") return { run: runResult({ state: "running", status: null }) };
    if (method === "broker/threads/stop") throw reasonError("runMismatch");
    return original(method, params);
  };
  const work = createRunHandler(fakeServerRequests())(unit.ctx);
  void work.catch(() => {});
  await until(() => unit.fake.calls.some((call) => call.method === "broker/threads/watch"));
  await bounded(Promise.all([...unit.callbacks].map((callback) => callback("SIGINT"))), 1000);
  expect(unit.fake.calls.find((call) => call.method === "broker/threads/stop")!.params).toMatchObject({ runId: "run-test" });
  await expect(work).rejects.toMatchObject({ code: "INTERRUPTED" });
  expect(unit.closed()).toBe(true);
});

test("本次运行尚未开工（没有 runId）时信号清理不发 stop", async () => {
  const unit = unitExecution(["run", "中文"]);
  const original = unit.fake.handle;
  unit.fake.handle = (method, params) => {
    if (method === "turn/start") return new Promise(() => {});
    return original(method, params);
  };
  const work = createRunHandler(fakeServerRequests())(unit.ctx);
  void work.catch(() => {});
  await until(() => unit.fake.calls.some((call) => call.method === "turn/start"));
  await bounded(Promise.all([...unit.callbacks].map((callback) => callback("SIGINT"))), 1000);
  await expect(work).rejects.toMatchObject({ code: "INTERRUPTED" });
  expect(unit.fake.calls.some((call) => call.method === "broker/threads/stop")).toBe(false);
});

test("本次运行已结束（收到自己的 run/ended）后信号清理不再发 stop", async () => {
  const unit = unitExecution(["run", "中文"]);
  const original = unit.fake.handle;
  const pendingRun = runResult({ turns: [{ ...runResult().turns[0]!, ready: "pending" }] });
  unit.fake.handle = (method, params) => {
    if (method === "broker/threads/watch") return { run: runResult({ state: "running", status: null }) };
    return original(method, params);
  };
  const work = createRunHandler(fakeServerRequests())(unit.ctx);
  void work.catch(() => {});
  await until(() => unit.fake.calls.some((call) => call.method === "broker/threads/watch"));
  // 运行已结束、结果还在补读：线程随时可能被别的连接开新运行，此时 Ctrl-C 不能再按线程停。
  unit.fake.emit("broker/run/ended", { threadId: "thread-test", run: pendingRun });
  await bounded(Promise.all([...unit.callbacks].map((callback) => callback("SIGINT"))), 1000);
  await expect(work).rejects.toMatchObject({ code: "INTERRUPTED" });
  expect(unit.fake.calls.some((call) => call.method === "broker/threads/stop")).toBe(false);
});

test("--detach 交接恰逢运行结束：按终态输出与退出，不报运行中", async () => {
  const unit = unitExecution(["run", "中文", "--detach", "--json"]);
  const failed = runResult({ status: "failed", turns: [{ ...runResult().turns[0]!, status: "failed", output: "失败输出" }] });
  const deps: ServerRequestDeps = { ...fakeServerRequests(),
    handover: async () => ({ outcome: "runEnded", turnId: null, pending: [], result: failed }) };
  expect(await createRunHandler(deps)(unit.ctx)).toBe(7);
  await unit.out.flush();
  const output = JSON.parse(unit.output().stdout);
  expect(output.status).toBe("failed");
  expect(output.result).toBe("失败输出");
});

test("恢复不存在的线程（上游 no rollout found）退出 2", async () => {
  const unit = unitExecution(["run", "--resume", "0199aaaa-bbbb-7ccc-8ddd-eeeeffff0000", "接着做"]);
  const original = unit.fake.handle;
  unit.fake.handle = (method, params) => {
    if (method === "broker/threads/list") return { threads: [] };
    if (method === "thread/resume") throw wireError(-32600, "no rollout found for thread id 0199aaaa-bbbb-7ccc-8ddd-eeeeffff0000");
    return original(method, params);
  };
  await expect(createRunHandler(fakeServerRequests())(unit.ctx)).rejects.toMatchObject({ code: "NOT_FOUND", exitCode: 2 });
  expect(unit.closed()).toBe(true);
});

test("review --resume：恢复后实际权限不是只读就拒绝，不发 review/start；是只读才开审", async () => {
  for (const [sandbox, ok] of [[{ type: "workspaceWrite" }, false], [{ type: "readOnly" }, true]] as const) {
    const unit = unitExecution(["review", "--resume", "thread-test", "--mode", "uncommitted"]);
    const original = unit.fake.handle;
    unit.fake.handle = (method, params) => {
      if (method === "broker/threads/list") return { threads: [] };
      if (method === "thread/resume") return { thread: { id: "thread-test" }, sandbox, approvalPolicy: "never" };
      if (method === "review/start") {
        unit.fake.emit("broker/run/started", { threadId: "thread-test", runId: "run-test" });
        return { runId: "run-test" };
      }
      return original(method, params);
    };
    const work = createReviewHandler(fakeServerRequests())(unit.ctx);
    if (ok) expect(await work).toBe(0);
    else await expect(work).rejects.toMatchObject({ code: "LOCAL_ERROR" });
    expect(unit.fake.calls.some((call) => call.method === "review/start")).toBe(ok);
  }
});

test("登记恢复参数失败即报错、不开轮，并关闭连接", async () => {
  const unit = unitExecution(["run", "中文"]);
  const original = unit.fake.handle;
  unit.fake.handle = (method, params) => {
    if (method === "broker/threads/resumeOverrides") throw wireError(-32602, "参数不合法");
    return original(method, params);
  };
  await expect(createRunHandler(fakeServerRequests())(unit.ctx)).rejects.toBeInstanceOf(CxbError);
  expect(unit.fake.calls.some((call) => call.method === "turn/start")).toBe(false);
  expect(unit.closed()).toBe(true);
});

test("未知键不匹配时不重试，失败后仍关闭连接", async () => {
  for (const failure of ["其他配置键", "重复未知键"]) {
    const unit = unitExecution(["run", "中文"]);
    const original = unit.fake.handle;
    unit.fake.handle = (method, params) => {
      if (method === "thread/start") throw wireError(-32600, failure === "其他配置键" ? "另一个配置键" : ASK_FEATURE_KEY);
      return original(method, params);
    };
    await expect(createRunHandler(fakeServerRequests())(unit.ctx)).rejects.toBeInstanceOf(CxbError);
    expect(unit.closed()).toBe(true);
    expect(unit.fake.calls.filter((call) => call.method === "thread/start")).toHaveLength(failure === "重复未知键" ? 2 : 1);
    expect(unit.fake.subscriptions).toBe(0);
  }
});

test("交接的存活风险先提示，失败交接不能假报成功", async () => {
  const unit = unitExecution(["run", "中文", "--detach", "--json"]);
  const deps: ServerRequestDeps = { ...fakeServerRequests(), handover: async () => ({ outcome: "detached", turnId: "turn-test", pending: [] }) };
  expect(await createRunHandler(deps)(unit.ctx)).toBe(0);
  await unit.out.flush();
  expect(unit.output().stderr).toContain("托管不保证可靠");
  expect(JSON.parse(unit.output().stdout).runId).toBe("run-test");
  const failed = unitExecution(["run", "中文", "--detach", "--json"]);
  await expect(createRunHandler({ ...deps, handover: async () => { throw new CxbError("THREAD_HELD", "交接被拒绝"); } })(failed.ctx)).rejects.toMatchObject({ code: "THREAD_HELD" });
  expect(failed.output().stdout).toBe("");
  expect(failed.closed()).toBe(true);
});

test("设置目标时线程被归档门禁挡住：退出 8 并提示核实命令，不报成目标能力问题（N7-A 2.3.2 节）", async () => {
  const unit = unitExecution(["run", "中文", "--json", "--goal", "目标"]);
  const original = unit.fake.handle;
  unit.fake.handle = (method, params) => {
    if (method === "thread/goal/set") {
      throw new CxbError("BROKER_UNAVAILABLE", "远端失败", { cause: { rpcCode: -32001, detail: "线程生命周期操作尚未完成",
        data: { reason: "lifecyclePending", kind: "archive", phase: "outcomeUnknown", threadId: "thread-占用" } } });
    }
    return original(method, params);
  };
  const error = await createRunHandler(fakeServerRequests())(unit.ctx).then(() => null, (error) => error);
  expect(error).toMatchObject({ code: "THREAD_HELD", exitCode: 8 });
  expect(error.message).toContain("cxb rm thread-占用");
  expect(error.message).not.toContain("目标能力");
});

test("共享刹车 completed 仍退出九；失败七；goal 不可用为本地错误", async () => {
  for (const kind of ["共享刹车", "失败", "goal 不可用"]) {
    const unit = unitExecution(["run", "中文", "--json", ...(kind === "goal 不可用" ? ["--goal", "目标"] : [])]);
    const original = unit.fake.handle;
    unit.fake.handle = (method, params) => {
      if (method === "thread/goal/set") throw wireError(-32600, "goal disabled secret=不可泄露");
      if (method === "broker/threads/watch") return { run: kind === "共享刹车" ? runResult({ loopGuard: { kind: "continuationLimit", count: 50, threshold: 50, turnIds: [], action: "pauseGoal" } }) : runResult({ status: "failed" }) };
      return original(method, params);
    };
    if (kind === "goal 不可用") await expect(createRunHandler(fakeServerRequests())(unit.ctx)).rejects.toMatchObject({ code: "LOCAL_ERROR" });
    else {
      expect(await createRunHandler(fakeServerRequests())(unit.ctx)).toBe(kind === "共享刹车" ? 9 : 7);
      await unit.out.flush();
      expect(JSON.parse(unit.output().stdout).ok).toBe(false);
      if (kind === "共享刹车") expect(unit.output().stderr).toContain("轮数上限，50 轮");
    }
    expect(unit.output().stderr).not.toContain("不可泄露");
  }
});

test("never 下 MCP 失败显示服务器与建议，错误中的凭据不回显", async () => {
  const unit = unitExecution(["run", "中文", "-v"]);
  const original = unit.fake.handle;
  unit.fake.handle = (method, params) => {
    if (method === "broker/threads/watch") return { run: runResult({ status: "failed", turns: [{ ...runResult().turns[0], status: "failed",
      error: { message: "MCP server `search` denied token=不可泄露", serverName: "search" } }] }) };
    return original(method, params);
  };
  expect(await createRunHandler(fakeServerRequests())(unit.ctx)).toBe(7);
  await unit.out.flush();
  expect(unit.output().stderr).toContain("服务器：search");
  expect(unit.output().stderr).toContain("--approval auto");
  expect(unit.output().stderr).not.toContain("不可泄露");
});

// ─── N3：--continue 续跑（命令面规格 3.1 节、详细设计 2.3 节「续跑的条件请求」、3.6 节）────────────────

const interrupted = (patch: Partial<RunResult> = {}) => runResult({ runId: "run-old", state: "ended", status: "interrupted",
  interruptReason: "stop", stopOutcome: "confirmed", input: "原运行的要求", meta: { source: "run", goal: "登记的目标" }, ...patch });

/** 按 broker 写法落一条运行记录（外壳 {epoch, revision, payload}）。 */
function writeRunRecord(run: RunResult) {
  const dir = join(h.workspace.workspaceDir, "runs");
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, `${run.runId}.json`), JSON.stringify({ epoch: 1, revision: 1, payload: run }));
}

/** 续跑桩：模型历史查询独立；第一次普通 watch 返回原运行，之后返回新运行。 */
function continueUnit(args: string[], opts: { from?: RunResult; server?: "private" | "shared"; turnStart?: (params: any) => unknown } = {}) {
  const unit = unitExecution(["run", "--resume", "abcd1234", "--continue", ...args], opts.server);
  const original = unit.fake.handle;
  let watches = 0;
  unit.fake.handle = (method, params) => {
    if (method === "broker/threads/list") return { threads: [{ threadId: "thread-test", shortId: "abcd1234" }] };
    if (method === "broker/runs/modelSummaries") return { runs: [] };
    if (method === "broker/threads/watch") return { run: watches++ === 0 ? (opts.from ?? interrupted()) : runResult({ continuedFrom: "run-old" }) };
    if (method === "thread/goal/get") return { goal: { objective: "目标原文", status: "paused" } };
    if (method === "turn/start" && opts.turnStart) return opts.turnStart(params);
    return original(method, params);
  };
  return unit;
}

const methods = (unit: { fake: FakeRpc }) => unit.fake.calls.map((call) => call.method);
const turnText = (unit: { fake: FakeRpc }) => unit.fake.calls.find((call) => call.method === "turn/start")?.params.input[0].text as string;

const realRequests: ServerRequestDeps = { attachServerRequests, handover, renderPending };

test("续跑：启动回包等待审批，真实处理器先回包再完成运行", async () => {
  const unit = continueUnit(["--json", "--approval", "never"]);
  const original = unit.fake.handle;
  unit.fake.handle = async (method, params) => {
    if (method !== "turn/start") return original(method, params);
    unit.fake.emit("broker/run/started", { threadId: "thread-test", runId: "run-test" });
    await unit.fake.serverRequest("item/commandExecution/requestApproval", {
      threadId: "thread-test", turnId: "turn-test", itemId: "approval-1", command: "echo 测试", cwd: h.workspace.root,
    }, "approval-1");
    expect(unit.fake.replies).toEqual([{ id: "approval-1", result: { decision: "accept" } }]);
    return { runId: "run-test" };
  };
  expect(await watchdog(createRunHandler(realRequests)(unit.ctx))).toBe(0);
  expect(unit.fake.replies).toEqual([{ id: "approval-1", result: { decision: "accept" } }]);
  expect(unit.fake.inFlightServerRequests).toBe(0);
  expect(unit.fake.subscriptions).toBe(0);
  expect(unit.closed()).toBe(true);
});

test("续跑：启动回包尚未放行的提问触发交接，不代答、不停止运行", async () => {
  const startReply = deferred<object>();
  const delivered = deferred<void>();
  const unit = continueUnit(["--json"]);
  const original = unit.fake.handle;
  unit.fake.handle = async (method, params) => {
    if (method === "turn/start") {
      unit.fake.emit("broker/run/started", { threadId: "thread-test", runId: "run-test" });
      await unit.fake.serverRequest("item/tool/requestUserInput", { threadId: "thread-test", turnId: "turn-test",
        itemId: "ask-1", questions: [{ id: "q1", header: "选择", question: "选哪个？", isOther: false, isSecret: false, options: null }] }, "ask-1");
      delivered.resolve();
      return startReply.promise;
    }
    if (method === "broker/threads/detach") return { outcome: "detached", turnId: "turn-test", pending: [] };
    if (method === "broker/threads/watch" && unit.fake.calls.some((c) => c.method === "turn/start")) {
      return { run: runResult({ state: "running", status: null }) };
    }
    return original(method, params);
  };
  try {
    expect(await watchdog(createRunHandler(realRequests)(unit.ctx))).toBe(5);
    await delivered.wait();
    expect(methods(unit)).toContain("broker/threads/detach");
    expect(methods(unit)).not.toContain("broker/threads/stop");
    expect(unit.fake.replies).toEqual([]);
    expect(unit.fake.inFlightServerRequests).toBe(0);
    expect(unit.fake.subscriptions).toBe(0);
    expect(unit.closed()).toBe(true);
  } finally { startReply.resolve({ runId: "run-test" }); await messageBarrier(() => [unit.fake.calls.length, unit.fake.replies.length, unit.fake.inFlightServerRequests]); }
});

test("续跑 --detach：交接完成后返回新运行句柄且不停止、不等待结果", async () => {
  const unit = continueUnit(["--detach", "--json"]);
  const original = unit.fake.handle;
  unit.fake.handle = (method, params) => method === "broker/threads/detach"
    ? { outcome: "detached", turnId: "turn-test", pending: [] }
    : method === "broker/threads/watch" && params.runId === "run-test"
      ? { run: runResult({ state: "running", status: null, finalTurnId: undefined }), revision: 0, pending: [] }
      : original(method, params);
  expect(await watchdog(createRunHandler(realRequests)(unit.ctx))).toBe(0);
  await unit.out.flush();
  expect(JSON.parse(unit.output().stdout)).toEqual({ threadId: "thread-test", shortId: "abcd1234", runId: "run-test", status: "running" });
  expect(unit.fake.calls.find((c) => c.method === "broker/threads/detach")?.params).toMatchObject({ threadId: "thread-test", runId: "run-test" });
  // 原运行续跑预检与交接后的新运行协调快照各一次；运行仍 running，不能等到结果才返回。
  const watches = unit.fake.calls.filter((c) => c.method === "broker/threads/watch");
  expect(watches).toHaveLength(2);
  expect(watches[1]!.params).toEqual({ threadId: "thread-test", runId: "run-test" });
  expect(methods(unit)).not.toContain("broker/threads/stop");
  expect(unit.fake.subscriptions).toBe(0);
  expect(unit.closed()).toBe(true);
});

for (const [stage, method, hasRun] of [
  ["读取模型", "model/list", false], ["解析线程", "broker/threads/list", false],
  ["读取原运行", "broker/threads/watch", false], ["登记恢复参数", "broker/threads/resumeOverrides", false],
  ["登记元数据", "broker/threads/annotate", false], ["读取目标", "thread/goal/get", false],
  ["启动尚未认领", "turn/start", false], ["启动已认领", "turn/start", true],
  ["等待共享定案", "定案", true], ["等待结果", "broker/threads/watch", true],
  ["交接尚未确认", "broker/threads/detach", true],
] as const) test(`续跑 Ctrl-C 清理：${stage}取消等待，只停止本次已认领运行`, async () => {
  const unit = continueUnit(method === "broker/threads/detach" ? ["--detach"] : [], { server: "shared" });
  const savedCode = process.exitCode ?? 0;
  const listeners = process.listenerCount("SIGINT");
  const exitCalls: unknown[] = [];
  const exit = spyOn(process, "exit").mockImplementation(((code: unknown) => { exitCalls.push(code); }) as typeof process.exit);
  const signals = registerSignalHandlers(unit.out, { cleanupTimeoutMs: 50_000 });
  const register = unit.ctx.registerCleanup;
  unit.ctx.registerCleanup = (callback) => {
    const cancelUnit = register(callback);
    const cancelSignal = signals.registerCleanup(callback);
    return () => { cancelUnit(); cancelSignal(); };
  };
  const reached = deferred<void>();
  const release = deferred<any>();
  const original = unit.fake.handle;
  unit.fake.handle = (called, params) => {
    const afterStart = unit.fake.calls.some((c) => c.method === "turn/start");
    if (called === "broker/threads/stop") return { stopOutcome: "confirmed" };
    if (called === method && (method !== "broker/threads/watch" || (afterStart === hasRun))) {
      if (hasRun && called === "turn/start") unit.fake.emit("broker/run/started", { threadId: "thread-test", runId: "run-test" });
      reached.resolve();
      return release.promise;
    }
    if (called === "turn/start") {
      unit.fake.emit("broker/run/started", { threadId: "thread-test", runId: "run-test" });
      if (method === "定案") { reached.resolve(); return { runId: "run-test" }; }
      return { runId: "run-test", absorbedBy: null };
    }
    return original(called, params);
  };
  const work = createRunHandler(realRequests)(unit.ctx);
  void work.catch(() => {});
  try {
    await reached.wait();
    // 进程内投递 SIGINT，经过真实输出层，覆盖 130 映射；不声称验证 Windows 控制台按键。
    process.emit("SIGINT");
    await watchdog(signals.shutdown!);
    await expect(watchdog(work)).rejects.toMatchObject({ code: "INTERRUPTED", exitCode: 4 });
    expect(process.exitCode).toBe(130);
    expect(exitCalls).toEqual([130]);
    const stops = unit.fake.calls.filter((c) => c.method === "broker/threads/stop");
    expect(stops).toHaveLength(hasRun ? 1 : 0);
    if (hasRun) expect(stops[0]!.params).toEqual({ threadId: "thread-test", runId: "run-test" });
    const callsBeforeLateReply = unit.fake.calls.length;
    release.resolve({ runId: "run-test", absorbedBy: null });
    await messageBarrier(() => [unit.fake.calls.length, unit.fake.replies.length, unit.fake.inFlightServerRequests]);
    expect(unit.fake.calls.length).toBe(callsBeforeLateReply);
    expect(unit.fake.subscriptions).toBe(0);
    expect(unit.callbacks.size).toBe(0);
    expect(unit.closed()).toBe(true);
  } finally {
    release.resolve({});
    try { await watchdog(Promise.allSettled([work, ...[...unit.callbacks].map((cb) => Promise.resolve(cb()))])); }
    finally {
      signals.dispose();
      exit.mockRestore();
      process.exitCode = savedCode;
      expect(process.listenerCount("SIGINT")).toBe(listeners);
    }
  }
});

function refusalError(reason: string) {
  const code = reason === "stopping" || reason === "threadBusy" ? -32001 : -32000;
  return new CxbError("BROKER_UNAVAILABLE", "远端请求失败", {
    cause: new RpcError(`JSON-RPC error ${code}: 续跑条件未满足`, code, "续跑条件未满足", { reason }),
  });
}

test("续跑：不发 thread/start、thread/resume；先登记覆盖再发带 cxbContinue 的 turn/start；提示含目标、补充与原始要求", async () => {
  const unit = continueUnit(["本次", "补充", "--json"]);
  expect(await createRunHandler(fakeServerRequests())(unit.ctx)).toBe(0);
  const called = methods(unit);
  expect(called).not.toContain("thread/start");
  expect(called).not.toContain("thread/resume");
  expect(called).not.toContain("thread/memoryMode/set");
  expect(called).not.toContain("thread/goal/set");
  expect(called.indexOf("broker/threads/resumeOverrides")).toBeGreaterThanOrEqual(0);
  expect(called.indexOf("broker/threads/resumeOverrides")).toBeLessThan(called.indexOf("turn/start"));
  expect(called.indexOf("thread/goal/get")).toBeLessThan(called.indexOf("turn/start"));
  expect(called.filter((method) => method === "thread/goal/get")).toHaveLength(1);
  const registered = unit.fake.calls.find((call) => call.method === "broker/threads/resumeOverrides")!.params;
  expect(registered).toMatchObject({ threadId: "thread-test", overrides: { config: { [ASK_FEATURE_KEY]: true }, developerInstructions: ASK_INSTRUCTIONS } });
  const start = unit.fake.calls.find((call) => call.method === "turn/start")!.params;
  expect(start).toMatchObject({ threadId: "thread-test", cxbContinue: { fromRunId: "run-old" }, sandboxPolicy: { type: "workspaceWrite" } });
  expect(start.cxbContinue).not.toHaveProperty("resumeGoal");
  expect(start.clientUserMessageId).toMatch(/^[\da-f-]{36}$/);
  const text = turnText(unit);
  expect(text).toContain("【续跑说明】");
  expect(text).toContain("目标原文");
  expect(text).toContain("本次 补充");
  expect(text).toContain("原运行的要求");
  const annotate = unit.fake.calls.find((call) => call.method === "broker/threads/annotate")!.params;
  expect(annotate.meta).toMatchObject({ source: "run", goal: "登记的目标" });
  await unit.out.flush();
  expect(JSON.parse(unit.output().stdout)).toMatchObject({ ok: true, continuedFrom: "run-old", goal: { objective: "目标原文" } });
  expect(unit.closed()).toBe(true);
  expect(unit.fake.subscriptions).toBe(0);
});

test("续跑：原始要求沿 continuedFrom 取自链首；链断开时提示一行仍续跑；没有补充时不出现补充段", async () => {
  writeRunRecord(runResult({ runId: "run-first", state: "ended", status: "interrupted", interruptReason: "clientDisconnected", input: "最初的要求" }));
  const unit = continueUnit([], { from: interrupted({ continuedFrom: "run-first", input: "上一次续跑提示" }) });
  expect(await createRunHandler(fakeServerRequests())(unit.ctx)).toBe(0);
  expect(turnText(unit)).toContain("最初的要求");
  expect(turnText(unit)).not.toContain("上一次续跑提示");
  expect(turnText(unit)).not.toContain("【本次补充】");
  const broken = continueUnit(["补充"], { from: interrupted({ continuedFrom: "run-missing" }) });
  expect(await createRunHandler(fakeServerRequests())(broken.ctx)).toBe(0);
  await broken.out.flush();
  expect(broken.output().stderr).toContain("run-missing 已缺失");
  expect(turnText(broken)).toContain("原运行的要求");
});

test("续跑：本地拒绝正常完成、审查与旁观运行，不登记、不开轮", async () => {
  for (const from of [interrupted({ interruptReason: null, status: "completed" }), interrupted({ reviewThreadId: "thread-review" }),
    interrupted({ joined: true })]) {
    const unit = continueUnit([], { from });
    await expect(createRunHandler(fakeServerRequests())(unit.ctx)).rejects.toMatchObject({ code: "LOCAL_ERROR", exitCode: 1 });
    expect(methods(unit)).not.toContain("turn/start");
    expect(methods(unit)).not.toContain("broker/threads/resumeOverrides");
    expect(unit.closed()).toBe(true);
  }
});

test("续跑 --run：读运行记录；不属于该线程或找不到时退出 2，不开轮", async () => {
  writeRunRecord(interrupted({ runId: "run-chosen", input: "指定运行的要求" }));
  const chosen = continueUnit(["--run", "run-chosen"]);
  expect(await createRunHandler(fakeServerRequests())(chosen.ctx)).toBe(0);
  expect(chosen.fake.calls.find((call) => call.method === "turn/start")!.params.cxbContinue).toEqual({ fromRunId: "run-chosen" });
  expect(turnText(chosen)).toContain("指定运行的要求");
  writeRunRecord(interrupted({ runId: "run-other", threadId: "thread-other" }));
  for (const runId of ["run-other", "run-absent"]) {
    const unit = continueUnit(["--run", runId]);
    await expect(createRunHandler(fakeServerRequests())(unit.ctx)).rejects.toMatchObject({ code: "NOT_FOUND", exitCode: 2 });
    expect(methods(unit)).not.toContain("turn/start");
  }
});

test("续跑：读不到 goal 时改用运行记录登记的目标", async () => {
  const unit = continueUnit([]);
  const handle = unit.fake.handle;
  unit.fake.handle = (method, params) => {
    if (method === "thread/goal/get") throw wireError(-32600, "goals feature is disabled");
    return handle(method, params);
  };
  expect(await createRunHandler(fakeServerRequests())(unit.ctx)).toBe(0);
  expect(turnText(unit)).toContain("登记的目标");
});

test("续跑：broker 的九种拒绝原因各自映射退出码，连接关闭", async () => {
  const expected: Array<[string, number]> = [["stopping", 8], ["threadBusy", 8], ["notLatestRun", 8], ["resumeCheckFailed", 7],
    ["notResumable", 1], ["unknownRun", 2], ["runMismatch", 2], ["continueCancelled", 4], ["shuttingDown", 6]];
  for (const [reason, exitCode] of expected) {
    const unit = continueUnit([], { turnStart: () => { throw refusalError(reason); } });
    await expect(createRunHandler(fakeServerRequests())(unit.ctx)).rejects.toMatchObject({ exitCode });
    expect(unit.closed()).toBe(true);
    expect(unit.fake.subscriptions).toBe(0);
  }
  const stopping = continueUnit([], { turnStart: () => { throw refusalError("stopping"); } });
  await expect(createRunHandler(fakeServerRequests())(stopping.ctx)).rejects.toThrow("cxb stop abcd1234");
});

test("共享服务器：回包 absorbedBy 为字符串退出 8，不再等待结果", async () => {
  const unit = continueUnit([], { server: "shared", turnStart: () => {
    unit.fake.emit("broker/run/started", { threadId: "thread-test", runId: "run-test" });
    return { runId: "run-test", absorbedBy: "turn-外部" };
  } });
  await expect(createRunHandler(fakeServerRequests())(unit.ctx)).rejects.toMatchObject({ code: "THREAD_HELD", exitCode: 8 });
  expect(unit.fake.calls.filter((call) => call.method === "broker/threads/watch")).toHaveLength(1);
  expect(unit.closed()).toBe(true);
});

test("共享服务器：回包没有 absorbedBy 时扣住到 broker/run/settled；定案为自己才等结果，被吸收退出 8", async () => {
  for (const absorbedBy of [null, "turn-外部"]) {
    const unit = continueUnit(["--json"], { server: "shared", turnStart: () => {
      unit.fake.emit("broker/run/started", { threadId: "thread-test", runId: "run-test" });
      return { runId: "run-test" };
    } });
    const work = createRunHandler(fakeServerRequests())(unit.ctx);
    void work.catch(() => {});
    await until(() => unit.fake.calls.some((call) => call.method === "turn/start"));
    // turn/start 已到达；按内存夹具调度假设观察连续三轮状态稳定后，只有目标定案才能放行。
    await messageBarrier(() => [unit.fake.calls.length, unit.fake.replies.length, unit.fake.inFlightServerRequests]);
    expect(unit.fake.calls.filter((call) => call.method === "broker/threads/watch")).toHaveLength(1);
    unit.fake.emit("broker/run/settled", { threadId: "thread-test", runId: "run-另一次", absorbedBy: null });
    await messageBarrier(() => [unit.fake.calls.length, unit.fake.replies.length, unit.fake.inFlightServerRequests]);
    expect(unit.fake.calls.filter((call) => call.method === "broker/threads/watch")).toHaveLength(1);
    unit.fake.emit("broker/run/settled", { threadId: "thread-test", runId: "run-test", absorbedBy });
    if (absorbedBy === null) expect(await work).toBe(0);
    else await expect(work).rejects.toMatchObject({ code: "THREAD_HELD", exitCode: 8 });
    expect(unit.closed()).toBe(true);
  }
});

test("共享服务器：定案通知早于回包也被接住", async () => {
  const unit = continueUnit([], { server: "shared", turnStart: () => {
    unit.fake.emit("broker/run/started", { threadId: "thread-test", runId: "run-test" });
    unit.fake.emit("broker/run/settled", { threadId: "thread-test", runId: "run-test", absorbedBy: null });
    return { runId: "run-test" };
  } });
  expect(await bounded(createRunHandler(fakeServerRequests())(unit.ctx), 5000)).toBe(0);
});

test("共享服务器：回包没有 absorbedBy、定案前断线，立即退出 6，不拖到全局期限", async () => {
  const unit = continueUnit([], { server: "shared", turnStart: () => {
    unit.fake.emit("broker/run/started", { threadId: "thread-test", runId: "run-test" });
    return { runId: "run-test" };
  } });
  const work = createRunHandler(fakeServerRequests())(unit.ctx);
  void work.catch(() => {});
  await until(() => unit.fake.calls.some((call) => call.method === "turn/start"));
  unit.fake.disconnect();
  await expect(bounded(work, 5000)).rejects.toMatchObject({ code: "BROKER_UNAVAILABLE", exitCode: 6 });
  expect(unit.closed()).toBe(true);
});

test("非 auto 审批明确传 approvalsReviewer:user，auto 传 auto_review；续跑同样", async () => {
  for (const [approval, reviewer] of [["on-request", "user"], ["never", "user"], ["auto", "auto_review"]] as const) {
    const unit = unitExecution(["run", "中文", "--approval", approval]);
    expect(await createRunHandler(fakeServerRequests())(unit.ctx)).toBe(0);
    for (const method of ["thread/start", "turn/start"]) {
      expect(unit.fake.calls.find((call) => call.method === method)!.params.approvalsReviewer).toBe(reviewer);
    }
  }
  const continued = continueUnit(["--approval", "on-request", "-v"]);
  expect(await createRunHandler(fakeServerRequests())(continued.ctx)).toBe(0);
  expect(continued.fake.calls.find((call) => call.method === "turn/start")!.params).toMatchObject({ approvalPolicy: "on-request", approvalsReviewer: "user" });
  await continued.out.flush();
  expect(continued.output().stderr).toContain("本次权限：沙箱 workspace-write，审批 on-request");
});

test("共享服务器：定案通知缺 absorbedBy 不算定案，继续等明确的定案", async () => {
  const unit = continueUnit([], { server: "shared", turnStart: () => {
    unit.fake.emit("broker/run/started", { threadId: "thread-test", runId: "run-test" });
    return { runId: "run-test" };
  } });
  const work = createRunHandler(fakeServerRequests())(unit.ctx);
  void work.catch(() => {});
  await until(() => unit.fake.calls.some((call) => call.method === "turn/start"));
  unit.fake.emit("broker/run/settled", { threadId: "thread-test", runId: "run-test" });
  // 缺字段通知处理后连续三轮状态稳定；它不能放行到结果等待。
  await messageBarrier(() => [unit.fake.calls.length, unit.fake.replies.length, unit.fake.inFlightServerRequests]);
  expect(unit.fake.calls.filter((call) => call.method === "broker/threads/watch")).toHaveLength(1);
  unit.fake.emit("broker/run/settled", { threadId: "thread-test", runId: "run-test", absorbedBy: "turn-外部" });
  await expect(bounded(work, 5000)).rejects.toMatchObject({ code: "THREAD_HELD", exitCode: 8 });
});

for (const json of [false, true]) for (const command of ['run', 'review'] as const) test(`上游诊断 ${command}${json ? ' --json' : ''}：不加 -v 即时 stderr 提示，最终单对象与退出七`, async () => {
  const unit = unitExecution([command, '中文', ...(json ? ['--json'] : [])]);
  const original = unit.fake.handle;
  const error = { message: '最终失败 Bearer private-final', codexErrorInfo: { responseTooManyFailedAttempts: { httpStatusCode: 429 } } };
  const result = runResult({ status: 'failed', diagnosticRevision: 2, upstreamError: upstreamError({ willRetry: false }),
    turns: [{ ...runResult().turns[0]!, status: 'failed', error }] });
  unit.fake.handle = (method, params) => {
    if (method === 'turn/start' || method === 'review/start') {
      // 诊断早于 runId：订阅必须在开轮前生效，且不可回显原始 error 通知。
      unit.fake.emit('error', { threadId: 'thread-test', error: { message: '原始通知不应显示' }, willRetry: true });
      unit.fake.emit('broker/run/updated', { runId: 'run-test', threadId: 'thread-test', diagnosticRevision: 1,
        upstreamError: upstreamError(), lastProgressAt: null, state: 'running', status: null });
      unit.fake.emit('broker/run/started', { threadId: 'thread-test', runId: 'run-test' });
      return { runId: 'run-test' };
    }
    if (method === 'broker/threads/watch') return { run: result };
    return original(method, params);
  };
  const handler = command === 'review' ? createReviewHandler(fakeServerRequests()) : createRunHandler(fakeServerRequests());
  expect(await handler(unit.ctx)).toBe(7);
  await unit.out.flush();
  const output = unit.output();
  expect(output.stderr).toContain('[cxb] 上游认证失败（HTTP 401），Codex 正在重试；已观察到 3 次。');
  expect(output.stderr).toContain('[cxb] 失败原因：最终失败 Bearer ***（HTTP 429）');
  expect(output.stderr.indexOf('Codex 执行失败')).toBeLessThan(output.stderr.indexOf('[cxb] 失败原因'));
  expect(output.stderr).not.toContain('原始通知不应显示');
  expect(output.stdout).not.toContain('[cxb]');
  expect(output.stdout + output.stderr).not.toContain('private-final');
  if (json) {
    const object = JSON.parse(output.stdout);
    expect(output.stdout.trim().split('\n')).toHaveLength(1);
    expect(object).toMatchObject({ command, status: 'failed', upstreamError: result.upstreamError,
      error: { message: '最终失败 Bearer ***', codexErrorInfo: error.codexErrorInfo } });
    expect(object).not.toHaveProperty('diagnosticRevision');
    expect(object).not.toHaveProperty('lastProgressAt');
  } else expect(output.stdout).toBe('中文结果');
  expect(unit.fake.subscriptions).toBe(0);
  expect(unit.fake.notificationErrors).toEqual([]);
});

test('上游诊断 run SA1b 最终轮选择：失败摘要与 JSON 取 finalTurnId，阶段错误不冒充最终原因', async () => {
  for (const hasFinalError of [true, false]) {
    const unit = unitExecution(['run', '中文', '--json']);
    const original = unit.fake.handle;
    const stage = { ...runResult().turns[0]!, turnId: 'stage', status: 'failed' as const, error: { message: '旧阶段错误' } };
    const final = { ...stage, turnId: 'final', error: hasFinalError ? { message: '真正最终错误' } : null };
    unit.fake.handle = (method, params) => method === 'broker/threads/watch' ? { run: runResult({ status: 'failed', finalTurnId: 'final',
      diagnosticRevision: 1, upstreamError: upstreamError(), turns: [final, stage] }) } : original(method, params);
    expect(await handler()(unit.ctx)).toBe(7);
    await unit.out.flush();
    const output = unit.output(), value = JSON.parse(output.stdout);
    expect(output.stderr).not.toContain('旧阶段错误');
    if (hasFinalError) {
      expect(output.stderr).toContain('[cxb] 失败原因：真正最终错误');
      expect(value.error).toEqual({ message: '真正最终错误' });
    } else {
      expect(output.stderr).not.toContain('失败原因');
      expect(output.stderr).toContain('最近上游报错：上游认证失败（HTTP 401）');
      expect(value).not.toHaveProperty('error');
    }
  }
});

test("上游诊断 run 无报错：只有进展版本时 stdout 不增字段、stderr 不增提示", async () => {
  const unit = unitExecution(["run", "中文", "--json"]);
  const original = unit.fake.handle;
  unit.fake.handle = (method, params) => method === "broker/threads/watch"
    ? { run: runResult({ diagnosticRevision: 9, lastProgressAt: 1234 }) } : original(method, params);
  expect(await handler()(unit.ctx)).toBe(0);
  await unit.out.flush();
  const value = JSON.parse(unit.output().stdout);
  for (const key of ["upstreamError", "diagnosticRevision", "lastProgressAt", "error"]) expect(value).not.toHaveProperty(key);
  expect(unit.output().stderr).toBe("");
});

test("上游诊断 run 活动轮：默认即时提示，willRetry:false 仍等待真实收尾", async () => {
  const unit = unitExecution(["run", "中文"]);
  const original = unit.fake.handle;
  unit.fake.handle = (method, params) => method === "broker/threads/watch"
    ? { run: runResult({ state: "running", status: null }) } : original(method, params);
  let settled = false;
  const work = handler()(unit.ctx);
  void work.then(() => { settled = true; }, () => { settled = true; });
  try {
    await until(() => unit.fake.calls.some(call => call.method === "broker/threads/watch"));
    const emit = (revision: number, willRetry: boolean) => unit.fake.emit("broker/run/updated", {
      runId: "run-test", threadId: "thread-test", diagnosticRevision: revision,
      upstreamError: upstreamError({ willRetry }), lastProgressAt: null, state: "running", status: null,
    });
    emit(1, true);
    await unit.out.flush();
    expect(unit.output().stderr).toContain("Codex 正在重试");
    expect(unit.output().stdout).toBe("");
    expect(settled).toBe(false);
    emit(2, false);
    await unit.out.flush();
    expect(unit.output().stderr).toContain("上游已报告终止错误，等待轮次结束确认");
    expect(settled).toBe(false);
    expect(unit.fake.calls.some(call => call.method === "broker/threads/stop")).toBe(false);
    unit.fake.emit("broker/run/ended", { run: runResult({ upstreamError: upstreamError({ willRetry: false }), diagnosticRevision: 2 }) });
    expect(await bounded(work, 3000)).toBe(0);
    await unit.out.flush();
    expect(unit.output().stdout).toBe("中文结果");
  } finally {
    if (!settled) unit.fake.emit("broker/run/ended", { run: runResult() });
    await bounded(work.catch(() => undefined), 3000);
  }
});

test("上游诊断 run 启动 RPC 失败：等待本运行失败记录，输出单对象与真实错误", async () => {
  const unit = unitExecution(["run", "中文", "--json"]);
  const original = unit.fake.handle;
  const snapshot = deferred<{ run: RunResult }>();
  const error = { code: -32603, message: "启动失败 sk-privateStartKey", data: { codexErrorInfo: "unauthorized" } };
  const diagnostic = upstreamError({ source: "rpcResponse", willRetry: null, turnId: null, count: 1 });
  unit.fake.handle = (method, params) => {
    if (method === "turn/start") {
      unit.fake.emit("broker/run/started", { threadId: "thread-test", runId: "run-test" });
      unit.fake.emit("broker/run/updated", { runId: "run-test", threadId: "thread-test", diagnosticRevision: 1,
        upstreamError: diagnostic, lastProgressAt: null, state: "ended", status: "failed" });
      throw wireError(-32603, error.message);
    }
    if (method === "broker/threads/watch") return snapshot.promise;
    return original(method, params);
  };
  const work = handler()(unit.ctx);
  void work.catch(() => undefined);
  try {
    await until(() => unit.fake.calls.some(call => call.method === "broker/threads/watch"));
    await messageBarrier(() => [unit.fake.calls.length]);
    snapshot.resolve({ run: runResult({ status: "failed", diagnosticRevision: 1, upstreamError: diagnostic,
      turns: [{ ...runResult().turns[0]!, error, status: "failed" }] }) });
    expect(await bounded(work, 3000)).toBe(7);
    await unit.out.flush();
    expect(JSON.parse(unit.output().stdout)).toMatchObject({ status: "failed", upstreamError: diagnostic,
      error: { ...error, message: "启动失败 ***" } });
    expect(unit.output().stdout.trim().split("\n")).toHaveLength(1);
    expect(unit.output().stderr).toContain("[cxb] 失败原因：启动失败 ***（unauthorized）");
    expect(unit.output().stderr + unit.output().stdout).not.toContain("privateStartKey");
  } finally {
    snapshot.resolve({ run: runResult() });
    await bounded(work.catch(() => undefined), 3000);
  }
});
