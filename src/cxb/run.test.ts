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

test("恢复未知键重试先登记去键后的有效覆盖，成功后保留 MCP 关闭键", async () => {
  const observer = await h.start({ rejectAsk: true, mcpServers: { backlog: {} } });
  const first = await h.invoke(handler(), ["run", "中文", "--json"]);
  const id = JSON.parse(first.stdout).thread.id;
  const second = await h.invoke(handler(), ["run", "继续新要求", "--resume", id, "--no-mcp", "backlog"]);
  expect(second.code).toBe(0);
  const { calls } = await observer.rpc.request<any>("fixture/state");
  const resumes = calls.filter((call: any) => call.method === "thread/resume");
  expect(resumes).toHaveLength(2);
  expect(resumes[0].params.config[ASK_FEATURE_KEY]).toBe(true);
  expect(resumes[1].params.config).not.toHaveProperty(ASK_FEATURE_KEY);
  const registrations = calls.filter((call: any) => call.method === "fixture/resumeOverrides");
  expect(registrations.at(-2).params.overrides).toEqual({
    config: resumes[1].params.config, developerInstructions: ASK_INSTRUCTIONS,
  });
  expect(registrations.at(-2).params.overrides.config["mcp_servers.backlog.enabled"]).toBe(false);
  expect(registrations.at(-2).params.overrides.config).not.toHaveProperty(ASK_FEATURE_KEY);
  expect(registrations.at(-1).params.overrides.config).not.toHaveProperty(ASK_FEATURE_KEY);
}, TEST_BUDGET);

test("恢复提问开关去键重试失败后，最后登记仍保留 MCP 关闭键与其他覆盖", async () => {
  const observer = await h.start({ rejectAsk: true, failResumeRetry: true, mcpServers: { backlog: {} } });
  const first = await h.invoke(handler(), ["run", "创建", "--json"]);
  expect(first.code).toBe(0);
  const result = await h.invoke(handler(), ["run", "恢复", "--resume", JSON.parse(first.stdout).thread.id, "--no-mcp", "backlog"]);
  expect(result.code).toBe(7);
  const { calls } = await observer.rpc.request<any>("fixture/state");
  const last = calls.filter((call: any) => call.method === "fixture/resumeOverrides").at(-1).params.overrides;
  expect(last).toMatchObject({ config: { "mcp_servers.backlog.enabled": false, model_reasoning_effort: "high" }, developerInstructions: ASK_INSTRUCTIONS });
  expect(last.config).not.toHaveProperty(ASK_FEATURE_KEY);
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
    if (method === "config/read") return { config: { mcp_servers: {} } };
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

function handoffRun(patch: Partial<RunResult> = {}): RunResult {
  return interrupted({ children: [{ threadId: "thread-child", parentThreadId: "thread-test", relationKind: "subagent",
    turnIds: ["turn-managed"], evidence: "v2Started", finalState: "settled" }], ...patch });
}

function handoffIndex() {
  writeFileSync(join(h.workspace.workspaceDir, "threads.json"), JSON.stringify({ payload: { threads: {
    abcd1234: { threadId: "thread-test", shortId: "abcd1234", updatedAt: 1000, lastRunId: "run-old" },
    child123: { threadId: "thread-child", shortId: "child123", updatedAt: 1000, lastRunId: null,
      parentThreadId: "thread-test", relationKind: "subagent", agentVersion: "v2", canAcceptDirectInput: false,
      sourceSummary: { kind: "threadSpawn", agentPath: "/root/research", agentNickname: "调研员" } },
    unrelated: { threadId: "thread-unrelated", shortId: "unrelated", updatedAt: 2000, lastRunId: null,
      parentThreadId: "thread-test", relationKind: "subagent", agentVersion: "v2" },
  } } }));
}

function withHandoffResults(unit: ReturnType<typeof unitExecution>, fail = false) {
  const original = unit.fake.handle;
  unit.fake.handle = (method, params) => {
    if (method === "broker/threads/list") return { threads: [{ threadId: "thread-test", shortId: "abcd1234" }] };
    if (method === "thread/resume") return { thread: { id: "thread-test" }, sandbox: { type: "readOnly" }, approvalPolicy: "never" };
    if (method === "thread/turns/list") {
      if (fail) throw new CxbError("BROKER_UNAVAILABLE", "结果读取失败");
      return { data: [{ id: "turn-unrelated-newest", status: "completed" }, { id: "turn-managed", status: "completed" }], nextCursor: null };
    }
    if (method === "thread/items/list") return { data: [{ turnId: params.turnId,
      item: { type: "agentMessage", phase: "final_answer", text: params.turnId === "turn-managed" ? "受管轮成果：已验证原接口" : "错误的新轮成果" } }], nextCursor: null };
    return original(method, params);
  };
}

test("子代理交接：continue 包含原运行受管轮与完整原有各节，不混入索引全血缘", async () => {
  handoffIndex();
  const unit = continueUnit(["本次补充"], { from: handoffRun() });
  withHandoffResults(unit);
  expect(await handler()(unit.ctx)).toBe(0);
  const text = turnText(unit);
  for (const heading of ["【续跑说明】", "【目标原文】", "【本次补充】", "【原始要求】", "【已有进度】", "【怎么继续】"]) expect(text).toContain(heading);
  for (const value of ['来源运行 "run-old"', "thread-child", "/root/research", "followup_task", "受管轮成果：已验证原接口"]) expect(text).toContain(value);
  expect(text).not.toContain("thread-unrelated");
  expect(text).not.toContain("错误的新轮成果");
  expect(unit.fake.calls.find((call) => call.method === "thread/items/list")!.params.turnId).toBe("turn-managed");
});

test("子代理交接：普通 resume 取最近有记录运行，仅作历史参考且保留本次任务", async () => {
  handoffIndex();
  writeRunRecord(handoffRun({ runId: "run-earlier", startedAt: 500,
    children: [{ threadId: "thread-earlier-child", parentThreadId: "thread-test", relationKind: "subagent", turnIds: [], evidence: "v1Spawn", finalState: "settled" }] }));
  writeRunRecord(handoffRun());
  writeRunRecord(runResult({ runId: "run-latest-empty", startedAt: 3000, children: [] }));
  const unit = unitExecution(["run", "只检查新需求，不恢复旧任务", "--resume", "abcd1234"]);
  withHandoffResults(unit);
  expect(await handler()(unit.ctx)).toBe(0);
  const text = turnText(unit);
  expect(text.startsWith("只检查新需求，不恢复旧任务\n\n")).toBe(true);
  for (const value of ["仅作历史参考", "本次用户输入才是任务本身", '来源运行 "run-old"', "受管轮成果：已验证原接口"]) expect(text).toContain(value);
  for (const forbidden of ["【续跑说明】", "不再阻止", "接着完成原始要求", "继续完成原始要求", "thread-earlier-child", '来源运行 "run-latest-empty"']) expect(text).not.toContain(forbidden);
  expect(unit.fake.calls.find((call) => call.method === "turn/start")!.params).not.toHaveProperty("cxbContinue");
});

test("子代理交接：新线程与 review --resume 不补读、不附交接单", async () => {
  handoffIndex();
  writeRunRecord(handoffRun());
  const fresh = unitExecution(["run", "新线程要求"]);
  withHandoffResults(fresh);
  expect(await handler()(fresh.ctx)).toBe(0);
  expect(turnText(fresh)).toBe("新线程要求");
  expect(methods(fresh)).not.toContain("thread/turns/list");
  const review = unitExecution(["review", "只审查当前改动", "--resume", "abcd1234"]);
  withHandoffResults(review);
  const original = review.fake.handle;
  review.fake.handle = (method, params) => {
    if (method === "review/start") return original("turn/start", params);
    return original(method, params);
  };
  expect(await createReviewHandler(fakeServerRequests())(review.ctx)).toBe(0);
  expect(methods(review)).not.toContain("thread/turns/list");
  expect(review.fake.calls.find((call) => call.method === "review/start")!.params.target).toEqual({ type: "custom", instructions: "只审查当前改动" });
});

test("子代理交接：broker 结果读取失败仍可续跑，提示只降级且不中断开工", async () => {
  handoffIndex();
  const unit = continueUnit([], { from: handoffRun() });
  withHandoffResults(unit, true);
  expect(await handler()(unit.ctx)).toBe(0);
  expect(turnText(unit)).toContain("thread-child");
  expect(turnText(unit)).toContain("读取失败");
  expect(methods(unit)).toContain("turn/start");
});

test("子代理交接：旧运行没有 children 或 subagentJoin 时不产生空标题，坏条目标不完整", async () => {
  const legacy = continueUnit([], { from: interrupted() });
  expect(await handler()(legacy.ctx)).toBe(0);
  expect(turnText(legacy)).not.toContain("子代理记录");
  expect(methods(legacy)).not.toContain("thread/turns/list");
  const ordinary = unitExecution(["run", "没有子代理的新要求", "--resume", "abcd1234"]);
  withHandoffResults(ordinary);
  expect(await handler()(ordinary.ctx)).toBe(0);
  expect(turnText(ordinary)).toBe("没有子代理的新要求");
  expect(methods(ordinary)).not.toContain("thread/turns/list");
  const malformed = continueUnit([], { from: interrupted({ children: [null] as unknown as RunResult["children"] }) });
  expect(await handler()(malformed.ctx)).toBe(0);
  expect(turnText(malformed)).toContain("记录不完整");
});

test("子代理交接：补读期间取消退出四且不开工，关闭连接与全部监听", async () => {
  const unit = continueUnit([], { from: handoffRun() });
  const original = unit.fake.handle;
  const requested = deferred<void>();
  const response = deferred<unknown>();
  h.disposers.push(() => response.resolve({ data: [], nextCursor: null }));
  unit.fake.handle = (method, params) => {
    if (method === "thread/turns/list") { requested.resolve(); return response.promise; }
    return original(method, params);
  };
  const work = handler()(unit.ctx);
  void work.catch(() => {});
  await bounded(requested.promise, 3000);
  await bounded(Promise.all([...unit.callbacks].map((callback) => callback("SIGINT"))), 3000);
  await expect(work).rejects.toMatchObject({ code: "INTERRUPTED", exitCode: 4 });
  expect(methods(unit)).not.toContain("turn/start");
  expect(unit.closed()).toBe(true);
  expect(unit.fake.subscriptions).toBe(0);
});

test("子代理交接：补读受总命令期限约束，超时退出三且不开工", async () => {
  const unit = continueUnit(["--timeout", "1"], { from: handoffRun() });
  const original = unit.fake.handle;
  const response = deferred<unknown>();
  h.disposers.push(() => response.resolve({ data: [], nextCursor: null }));
  unit.fake.handle = (method, params) => method === "thread/turns/list" ? response.promise : original(method, params);
  await expect(watchdog(handler()(unit.ctx), 5000, "交接单总命令期限")).rejects.toMatchObject({ code: "TIMEOUT", exitCode: 3 });
  expect(methods(unit)).toContain("thread/turns/list");
  expect(methods(unit)).not.toContain("turn/start");
  expect(unit.closed()).toBe(true);
  expect(unit.fake.subscriptions).toBe(0);
});

const realRequests: ServerRequestDeps = { attachServerRequests, handover, renderPending };

for (const scenario of ["清空名单", "名单相同", "仅顺序与其他字段变化", "索引缺失", "线程缺失", "索引损坏", "登记形状异常", "配置形状异常", "只计 false", "全关改回 keep"] as const)
  test(`MCP 恢复登记对比：${scenario}`, async () => {
    const unchanged = scenario === "名单相同" || scenario === "仅顺序与其他字段变化";
    const args = scenario === "清空名单" || scenario === "全关改回 keep" ? [] : ["--no-mcp", unchanged ? "backlog,serena" : "backlog"];
    const unit = unitExecution(["run", "恢复", "--resume", "thread-test", ...args]);
    if (scenario === "全关改回 keep") unit.ctx.parsed.options["mcp-when-never"] = "keep";
    const original = unit.fake.handle;
    unit.fake.handle = (method, params) => {
      if (method === "broker/threads/list") return { threads: [{ threadId: "thread-test", shortId: "abcd1234" }] };
      if (method === "thread/resume") return { thread: { id: "thread-test" } };
      if (method === "config/read") return { config: { mcp_servers: { backlog: {}, serena: {} } } };
      return original(method, params);
    };
    const stored = scenario === "登记形状异常" ? [] : { config: scenario === "配置形状异常" ? null : {
      "mcp_servers.serena.enabled": unchanged ? false : true,
      "mcp_servers.backlog.enabled": scenario === "只计 false" ? true : false,
      "mcp_servers.backlog.timeout": false, model_reasoning_effort: "low",
    } };
    if (scenario !== "索引缺失") writeFileSync(join(h.workspace.workspaceDir, "threads.json"), scenario === "索引损坏" ? "{坏"
      : JSON.stringify({ epoch: 1, revision: 1, payload: { threads: { abcd1234: {
        threadId: scenario === "线程缺失" ? "other-thread" : "thread-test", shortId: "abcd1234", updatedAt: 1, resumeOverrides: stored,
      } } } }));
    expect(await handler()(unit.ctx)).toBe(0);
    await unit.out.flush();
    expect(unit.output().stderr.includes("本次 MCP 开关未确认生效")).toBe(!unchanged);
  });

test("续跑清空旧 MCP 关闭登记时提醒未确认生效", async () => {
  const unit = continueUnit([]);
  writeFileSync(join(h.workspace.workspaceDir, "threads.json"), JSON.stringify({ payload: { threads: { abcd1234: {
    threadId: "thread-test", shortId: "abcd1234", updatedAt: 1,
    resumeOverrides: { config: { "mcp_servers.backlog.enabled": false } },
  } } } }));
  expect(await handler()(unit.ctx)).toBe(0);
  await unit.out.flush();
  expect(unit.output().stderr.match(/本次 MCP 开关未确认生效/g)).toHaveLength(1);
  expect(unit.fake.calls.find((call) => call.method === "broker/threads/resumeOverrides")!.params.overrides.config)
    .not.toHaveProperty("mcp_servers.backlog.enabled");
});

for (const [label, resumeOverrides] of [["省略字段", undefined], ["空值", null], ["省略配置", {}]] as const)
  test(`已有线程${label}表示没有旧覆盖，仅豁免时关闭集合不变不提醒`, async () => {
    const unit = continueUnit(["--mcp", "backlog"]);
    writeFileSync(join(h.workspace.workspaceDir, "threads.json"), JSON.stringify({ payload: { threads: { abcd1234: {
      threadId: "thread-test", shortId: "abcd1234", updatedAt: 1, resumeOverrides,
    } } } }));
    expect(await handler()(unit.ctx)).toBe(0);
    await unit.out.flush();
    expect(unit.output().stderr).not.toContain("本次 MCP 开关未确认生效");
  });

test("续跑按线程保存的目录读取有效配置，关闭键登记但不主动恢复线程", async () => {
  const unit = continueUnit(["--no-mcp", "project_only"]);
  const cwd = join(h.dir, "线程原目录");
  const original = unit.fake.handle;
  unit.fake.handle = (method, params) => {
    if (method === "thread/read") return { thread: { cwd } };
    if (method === "config/read") return { config: { mcp_servers: { project_only: {} } } };
    return original(method, params);
  };
  expect(await createRunHandler(fakeServerRequests())(unit.ctx)).toBe(0);
  expect(unit.fake.calls.find((call) => call.method === "config/read")!.params).toEqual({ cwd });
  expect(unit.fake.calls.filter((call) => call.method === "thread/read")).toHaveLength(1);
  expect(unit.fake.calls.find((call) => call.method === "broker/threads/resumeOverrides")!.params.overrides.config)
    .toMatchObject({ "mcp_servers.project_only.enabled": false });
  expect(methods(unit)).not.toContain("thread/resume");
  await unit.out.flush();
  expect(unit.output().stderr).toContain("本次 MCP 开关未确认生效");
});

for (const unreadable of [false, true]) test(`续跑${unreadable ? "读不到线程" : "缺少线程目录"}时不猜测配置目录、不下发关闭键`, async () => {
  const unit = continueUnit(["--no-mcp", "backlog"]);
  const original = unit.fake.handle;
  unit.fake.handle = (method, params) => {
    if (method === "thread/read") {
      if (unreadable) throw wireError(-32601, "无法读取线程");
      return { thread: {} };
    }
    return original(method, params);
  };
  expect(await createRunHandler(fakeServerRequests())(unit.ctx)).toBe(0);
  expect(methods(unit)).not.toContain("config/read");
  const config = unit.fake.calls.find((call) => call.method === "broker/threads/resumeOverrides")!.params.overrides.config;
  expect(Object.keys(config).some((key) => key.startsWith("mcp_servers."))).toBe(false);
  await unit.out.flush();
  expect(unit.output().stderr).toContain("读不到 Codex 的 MCP 配置，本次未关闭任何 MCP 服务");
});

for (const failure of ["方法不存在", "读取失败", "请求超时"] as const) test(`有效配置${failure}时降级运行且不登记关闭键`, async () => {
  const unit = unitExecution(["run", "中文", "--no-mcp", "backlog"]);
  const original = unit.fake.handle;
  unit.fake.handle = (method, params) => {
    if (method === "config/read") throw failure === "请求超时" ? new CxbError("BROKER_UNAVAILABLE", "模拟 RPC 超时")
      : wireError(failure === "方法不存在" ? -32601 : -32000, "模拟配置读取失败");
    return original(method, params);
  };
  expect(await createRunHandler(fakeServerRequests())(unit.ctx)).toBe(0);
  const config = unit.fake.calls.find((call) => call.method === "broker/threads/resumeOverrides")!.params.overrides.config;
  expect(Object.keys(config).some((key) => key.startsWith("mcp_servers."))).toBe(false);
  await unit.out.flush();
  expect(unit.output().stderr).toContain("读不到 Codex 的 MCP 配置，本次未关闭任何 MCP 服务");
});

test("读取有效配置期间取消仍上抛中断，不继续开线程", async () => {
  const unit = unitExecution(["run", "中文", "--no-mcp", "backlog"]);
  const original = unit.fake.handle;
  unit.fake.handle = (method, params) => method === "config/read" ? new Promise(() => {}) : original(method, params);
  const work = createRunHandler(fakeServerRequests())(unit.ctx);
  void work.catch(() => {});
  await until(() => methods(unit).includes("config/read"));
  await Promise.all([...unit.callbacks].map((callback) => callback()));
  await expect(work).rejects.toMatchObject({ code: "INTERRUPTED" });
  expect(methods(unit)).not.toContain("thread/start");
  expect(unit.closed()).toBe(true);
});

test("读取有效配置触及命令期限时上抛超时，不降级成成功运行", async () => {
  const unit = unitExecution(["run", "中文", "--no-mcp", "backlog", "--timeout", "1"]);
  const original = unit.fake.handle;
  unit.fake.handle = (method, params) => method === "config/read" ? new Promise(() => {}) : original(method, params);
  await expect(createRunHandler(fakeServerRequests())(unit.ctx)).rejects.toMatchObject({ code: "TIMEOUT" });
  expect(methods(unit)).not.toContain("thread/start");
  expect(unit.closed()).toBe(true);
});

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
