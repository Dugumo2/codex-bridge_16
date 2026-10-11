import { afterEach, beforeEach, expect, test } from "bun:test";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import type { Model } from "../types";
import { CxbError, type BrokerConnection, type RunResult } from "./contracts";
import { compareRunCreation, latestModelRun, readAllRuns } from "./ledger";
import { createRunHandler } from "./run";
import { createReviewHandler } from "./review";
import { applyUserConfig } from "./user-config";
import { renderHelp } from "./help";
import { FakeRpc, runResult, wireError } from "./test-fixtures/run-fakes";
import { CLEANUP_BUDGET, TEST_BUDGET, RunHarness, fakeServerRequests } from "./test-fixtures/run-harness";

let h: RunHarness;
let oldHome: string | undefined;
beforeEach(() => { h = new RunHarness(); oldHome = process.env.CODEX_HOME; process.env.CODEX_HOME = h.env.CODEX_HOME; });
afterEach(async () => {
  try { await h.cleanup(); } finally {
    if (oldHome === undefined) delete process.env.CODEX_HOME; else process.env.CODEX_HOME = oldHome;
  }
}, CLEANUP_BUDGET);

const catalog = [
  { id: "astra", isDefault: false, upgrade: null, supportedReasoningEfforts: [{ reasoningEffort: "high" }, { reasoningEffort: "xhigh" }] },
  { id: "sol", isDefault: true, upgrade: null, supportedReasoningEfforts: [{ reasoningEffort: "low" }, { reasoningEffort: "medium" }] },
] as Model[];
const qualified = (patch: Partial<RunResult> = {}) => runResult({ runId: "run-a", meta: { source: "run", model: "astra", effort: "high" },
  creationOrder: { epoch: 1, sequence: 1 }, startConfirmation: { confirmed: true, owned: true, threadId: patch.threadId ?? "thread-test", turnId: "turn-a" },
  ...patch });
function save(run: RunResult, epoch = 1, revision = 1) {
  const dir = join(h.workspace.workspaceDir, "runs");
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, `${run.runId}.json`), JSON.stringify({ epoch, revision, payload: run }));
}
const invalidCases: Array<[string, Partial<RunResult>]> = [
  ["joined", { joined: true, startConfirmation: { confirmed: false, owned: false, threadId: "thread-test", turnId: "turn-b" } }],
  ["未定案", { state: "running", status: null, startConfirmation: { confirmed: false, owned: null, threadId: "thread-test", turnId: "turn-b" } }],
  ["开轮失败", { status: "failed", startConfirmation: { confirmed: false, owned: true, threadId: null, turnId: null } }],
  ["被拒", { status: "failed", turns: [], turnsTotal: 0, startConfirmation: { confirmed: false, owned: null, threadId: null, turnId: null } }],
  ["确认前失联", { state: "lost", status: null, startConfirmation: { confirmed: false, owned: true, threadId: null, turnId: null } }],
  ["无确认字段的旧记录", { startConfirmation: undefined }],
  ["无 model", { meta: { source: "run", effort: "medium" } }],
  ["source=review", { meta: { source: "review", model: "review-model", effort: "low" } }],
  ["无 source", { meta: { model: "无来源模型" } as RunResult["meta"] }],
];

/** 模拟 broker 的在线快照；thread/start/resume 回包故意与请求不同，暴露输出误取回包的回归。 */
function fixture(options: { runs?: RunResult[]; latest?: RunResult | null; shared?: boolean; models?: Model[] | Error;
  upstream?: { model?: string | null; reasoningEffort?: string | null } | Error; joined?: boolean; threadId?: string; watchError?: Error } = {}) {
  const fake = new FakeRpc();
  const threadId = options.threadId ?? "thread-test";
  const ended = runResult({ threadId, joined: options.joined ?? false });
  let closed = false;
  const connection: BrokerConnection = { rpc: fake.rpc,
    handshake: { userAgent: "模型沿用测试", brokerVersion: "0.1.0", protocolVersion: 1, epoch: 2, server: { kind: options.shared ? "shared" : "private" } },
    launched: false, close: async () => { closed = true; } };
  fake.handle = (method, params) => {
    if (method === "broker/threads/list") return { threads: [] };
    if (method === "model/list") {
      if (options.models instanceof Error) throw options.models;
      return { data: options.models ?? catalog, nextCursor: null };
    }
    if (method === "thread/read") {
      if (options.upstream instanceof Error) throw options.upstream;
      return { thread: { id: threadId, canAcceptDirectInput: true, ...(options.upstream ?? { model: "upstream-other", reasoningEffort: "low" }) } };
    }
    if (method === "thread/start" || method === "thread/resume") {
      return { thread: { id: threadId }, model: "reply-other", reasoningEffort: "ultra", sandbox: { type: "readOnly" }, approvalPolicy: "never" };
    }
    if (method === "broker/threads/annotate") return { shortId: "abcd1234" };
    if (method === "thread/goal/get") return { goal: null };
    if (method === "turn/start" || method === "review/start") return { runId: ended.runId, absorbedBy: options.joined ? "turn-external" : null };
    if (method === "broker/threads/watch") {
      if (params.includeRuns && options.watchError) throw options.watchError;
      if (params.includeRuns) return { run: options.latest ?? options.runs?.at(-1) ?? null, runs: options.runs ?? [] };
      if (!fake.calls.some((call) => call.method === "turn/start" || call.method === "review/start")) return { run: options.latest ?? options.runs?.at(-1) ?? null };
      return { run: ended };
    }
    if (method === "broker/health") return { survivesParentJob: true };
    if (method === "broker/threads/detach") return { outcome: "detached", turnId: "turn-test", pending: [] };
    return {};
  };
  const invoke = (args: string[] = ["--resume", threadId], config: Record<string, unknown> = {}, review = false) => h.invoke(async (ctx) => {
    applyUserConfig(ctx.parsed, { path: "测试配置", values: config, unknownKeys: [] });
    return (review ? createReviewHandler : createRunHandler)(fakeServerRequests())(ctx);
  }, [review ? "review" : "run", "测试要求", "--json", ...args], connection);
  h.disposers.push(() => {
    expect(fake.notificationErrors).toEqual([]);
    expect(fake.subscriptions).toBe(0);
  });
  return { fake, invoke, closed: () => closed };
}
function requestValues(f: ReturnType<typeof fixture>) {
  const turn = f.fake.calls.find((call) => call.method === "turn/start")?.params;
  const meta = f.fake.calls.find((call) => call.method === "broker/threads/annotate")?.params.meta;
  return { turn, meta };
}
function expectRequest(f: ReturnType<typeof fixture>, output: string, model: string | null, effort: string | null, modelSource: string, reasoningSource: string) {
  const { turn, meta } = requestValues(f);
  expect(turn).toBeDefined();
  expect(turn.model ?? null).toBe(model);
  expect(turn.effort ?? null).toBe(effort);
  expect(meta).toMatchObject({ source: "run", model, effort });
  expect(JSON.parse(output)).toMatchObject({ model, reasoning: effort, modelSource, reasoningSource });
  expect(f.closed()).toBe(true);
}

for (const continuing of [false, true]) test(`CLI 沿用：${continuing ? "--continue" : "--resume"} 使用最近合格请求且 JSON 不取不同的回包`, async () => {
  const previous = qualified({ status: "interrupted", stopOutcome: "confirmed", interruptReason: "stop" });
  save(previous);
  const f = fixture({ runs: [previous], latest: previous });
  const result = await f.invoke(["--resume", "thread-test", ...(continuing ? ["--continue"] : [])], { model: "sol", reasoning: "medium" });
  expect(result.code, result.stderr).toBe(0);
  expectRequest(f, result.stdout, "astra", "high", "沿用线程", "沿用线程");
  expect(f.fake.calls.filter((call) => call.method === "broker/threads/watch" && call.params.includeRuns)).toHaveLength(1);
  if (continuing) expect(f.fake.calls.some((call) => call.method === "thread/resume")).toBe(false);
  else expect(f.fake.calls.find((call) => call.method === "thread/resume")?.params).toMatchObject({ model: "astra", config: { model_reasoning_effort: "high" } });
}, TEST_BUDGET);

test("CLI 沿用：无 config 不跟随 pickBestModel 升级链", async () => {
  save(qualified());
  const f = fixture({ models: [catalog[0]!, { ...catalog[1]!, upgrade: "sol-next" }, { ...catalog[1]!, id: "sol-next", isDefault: false }] });
  const result = await f.invoke();
  expect(result.code).toBe(0);
  expectRequest(f, result.stdout, "astra", "high", "沿用线程", "沿用线程");
}, TEST_BUDGET);

for (const [label, patch] of invalidCases) test(`CLI 历史回溯：最近${label}取更早合格运行`, async () => {
  save(qualified());
  const recent = qualified({ runId: "run-b", creationOrder: { epoch: 9, sequence: 9 }, ...patch });
  save(recent);
  const f = fixture({ runs: [recent], latest: recent });
  const result = await f.invoke();
  expect(result.code).toBe(0);
  expectRequest(f, result.stdout, "astra", "high", "沿用线程", "沿用线程");
}, TEST_BUDGET);

for (const state of ["中断", "失联"] as const) test(`CLI 历史回溯：确认后${state}仍合格`, async () => {
  const previous = qualified({ state: state === "失联" ? "lost" : "ended", status: "interrupted" });
  save(previous);
  const f = fixture();
  const result = await f.invoke();
  expect(result.code).toBe(0);
  expectRequest(f, result.stdout, "astra", "high", "沿用线程", "沿用线程");
}, TEST_BUDGET);

test("CLI 历史回溯：淘汰内存、重启、索引缺失或重建均不影响磁盘 A", async () => {
  const a = qualified();
  const b = qualified({ runId: "run-b", creationOrder: { epoch: 1, sequence: 2 }, startConfirmation: undefined });
  save(a, 800, 99);
  save(b, 900, 999);
  for (const rebuilt of [false, true]) {
    if (rebuilt) writeFileSync(join(h.workspace.workspaceDir, "threads.json"), JSON.stringify({ payload: { threads: {
      abcd1234: { threadId: "thread-test", shortId: "abcd1234", updatedAt: 500, lastRunId: "run-b", model: "错误索引模型" },
    } } }));
    const f = fixture({ runs: [], latest: null, ...(rebuilt ? {} : { watchError: unknownThread() }) });
    const result = await f.invoke();
    expect(result.code).toBe(0);
    expectRequest(f, result.stdout, "astra", "high", "沿用线程", "沿用线程");
  }
}, TEST_BUDGET);

function unknownThread() {
  return new CxbError("BROKER_UNAVAILABLE", "远端请求失败", { cause: { rpcCode: -32000, data: { reason: "unknownThread" } } });
}
test("CLI broker 不认识线程时仍可沿用上游记录", async () => {
  const f = fixture({ watchError: unknownThread(), upstream: { model: "astra", reasoningEffort: "high" } });
  const result = await f.invoke();
  expect(result.code).toBe(0);
  expectRequest(f, result.stdout, "astra", "high", "沿用上游记录", "沿用上游记录");
  expect(result.stderr).toContain("没有合格的 cxb 模型记录，沿用上游记录");
}, TEST_BUDGET);
test("CLI 历史快照的连接失败不能伪装成没有历史", async () => {
  save(qualified());
  const f = fixture({ watchError: new CxbError("BROKER_UNAVAILABLE", "连接已断开") });
  const result = await f.invoke();
  expect(result.code).toBe(6);
  expect(f.fake.calls.some((call) => call.method === "turn/start")).toBe(false);
}, TEST_BUDGET);

test("CLI 在线未落盘状态覆盖同 runId 磁盘旧状态，并合并全部在线运行", async () => {
  const old = qualified({ runId: "run-a", startConfirmation: undefined });
  save(old);
  const confirmed = qualified();
  const invalid = qualified({ runId: "run-b", creationOrder: { epoch: 3, sequence: 1 }, meta: null });
  const f = fixture({ runs: [confirmed, invalid], latest: invalid });
  const result = await f.invoke();
  expect(result.code).toBe(0);
  expectRequest(f, result.stdout, "astra", "high", "沿用线程", "沿用线程");
  // 在线反证也须覆盖旧磁盘，不能只将合格在线项追加后再取最新。
  expect(latestModelRun("thread-test", [confirmed], [old])).toBeUndefined();
}, TEST_BUDGET);

test("CLI 父线程、子代理与 review 子线程独立选取各自 run", async () => {
  for (const [threadId, model] of [["parent", "parent-model"], ["child", "child-model"], ["review-child", "review-child-model"]]) {
    save(qualified({ runId: `run-${threadId}`, threadId, meta: { source: "run", model, effort: "high" } }));
  }
  save(qualified({ runId: "run-review", threadId: "parent", reviewThreadId: "review-child", creationOrder: { epoch: 9, sequence: 9 },
    meta: { source: "review", model: "不得沿用审查模型", effort: "low" } }));
  for (const threadId of ["parent", "child", "review-child"]) {
    const f = fixture({ threadId });
    const result = await f.invoke();
    expect(result.code).toBe(0);
    expectRequest(f, result.stdout, `${threadId}-model`, "high", "沿用线程", "沿用线程");
  }
  expect(latestModelRun("从未独立运行的子线程", readAllRuns(h.workspace.workspaceDir))).toBeUndefined();
}, TEST_BUDGET);

test("历史顺序：同毫秒、回拨、跨重启、旧记录及文件重写", () => {
  const a = qualified({ runId: "run-z", startedAt: 9000 });
  const b = qualified({ runId: "run-a", startedAt: 9000, creationOrder: { epoch: 1, sequence: 2 } });
  const c = qualified({ runId: "run-c", startedAt: 10, creationOrder: { epoch: 2, sequence: 1 } });
  const old1 = qualified({ runId: "run-old-a", startedAt: 99_000, creationOrder: undefined });
  const old2 = qualified({ runId: "run-old-b", startedAt: 99_000, creationOrder: undefined });
  const old3 = qualified({ runId: "run-old-0", startedAt: 99_001, creationOrder: undefined });
  expect([c, old3, b, old2, a, old1].sort(compareRunCreation).map((r) => r.runId))
    .toEqual([old1.runId, old2.runId, old3.runId, a.runId, b.runId, c.runId]);
  save(a, 999, 999);
  save(c, 2, 1);
  expect(latestModelRun("thread-test", readAllRuns(h.workspace.workspaceDir))?.runId).toBe(c.runId);
}, TEST_BUDGET);

test("CLI 无合格记录沿用 thread/read 并在无 -v 时提示", async () => {
  const f = fixture({ upstream: { model: "astra", reasoningEffort: "high" } });
  const result = await f.invoke();
  expect(result.code).toBe(0);
  expectRequest(f, result.stdout, "astra", "high", "沿用上游记录", "沿用上游记录");
  expect(result.stderr).toContain("没有合格的 cxb 模型记录，沿用上游记录");
  expect(f.fake.calls.filter((call) => call.method === "thread/read")).toHaveLength(1);
}, TEST_BUDGET);

for (const missing of ["无值", "失败"] as const) test(`CLI 上游${missing}退回 config 或自动并提示`, async () => {
  for (const config of [{}, { model: "astra", reasoning: "high" }]) {
    const f = fixture({ upstream: missing === "失败" ? wireError(-32603, "读取失败") : {} });
    const result = await f.invoke(undefined, config);
    expect(result.code).toBe(0);
    expectRequest(f, result.stdout, config.model ?? "sol", config.reasoning ?? "medium",
      config.model ? "config 默认" : "自动选择", config.reasoning ? "config 默认" : "自动选择");
    expect(result.stderr).toContain("无法确定线程原模型");
  }
}, TEST_BUDGET);

for (const failed of [false, true]) test(`CLI 共享模式${failed ? "读取失败" : "核对不一致"}提示且不阻塞`, async () => {
  save(qualified());
  const f = fixture({ shared: true, upstream: failed ? wireError(-32603, "读取失败") : { model: "sol", reasoningEffort: "medium" } });
  const result = await f.invoke();
  expect(result.code).toBe(0);
  expectRequest(f, result.stdout, "astra", "high", "沿用线程", "沿用线程");
  expect(result.stderr).toContain(failed ? "未能与上游核对" : "沿用 cxb 最近记录 astra/high，上游当前配置为 sol/medium");
  expect(f.fake.calls.filter((call) => call.method === "thread/read")).toHaveLength(1);
}, TEST_BUDGET);

test("CLI 共享核对相同无警告，仅强度不同也提示", async () => {
  save(qualified());
  for (const effort of ["high", "low"]) {
    const f = fixture({ shared: true, upstream: { model: "astra", reasoningEffort: effort } });
    const result = await f.invoke();
    expect(result.code).toBe(0);
    expect(result.stderr.includes("上游当前配置")).toBe(effort === "low");
  }
}, TEST_BUDGET);

test("CLI 共享显式换模型不误报沿用旧模型，同模型沿用强度仍核对", async () => {
  save(qualified());
  for (const model of ["sol", "astra"]) {
    const f = fixture({ shared: true });
    const result = await f.invoke(["--resume", "thread-test", "-m", model]);
    expect(result.code).toBe(0);
    expect(result.stderr.includes("沿用 cxb 最近记录")).toBe(model === "astra");
  }
}, TEST_BUDGET);

for (const [name, args, config, effort, source] of [
  ["换模型自动", ["-m", "sol"], {}, "medium", "自动选择"],
  ["换模型 config", ["-m", "sol"], { reasoning: "low" }, "low", "config 默认"],
  ["显式强度", ["-m", "sol", "-r", "high"], { reasoning: "low" }, "high", "-r 指定"],
] as const) test(`CLI parsed.explicit：${name}`, async () => {
  save(qualified());
  const f = fixture();
  const result = await f.invoke(["--resume", "thread-test", ...args], config);
  expect(result.code).toBe(0);
  expectRequest(f, result.stdout, "sol", effort, "-m 指定", source);
  if (source === "-r 指定") expect(result.stderr).toContain("目录未列出强度 high，按原样提交");
}, TEST_BUDGET);

const unknownCatalogs: Array<[string, Model[] | Error]> = [["目录失败", new Error("目录失败")], ["模型不在目录", []],
  ["S 为空", [{ ...catalog[1]!, supportedReasoningEfforts: [] }]]];
for (const [name, models] of unknownCatalogs) {
  test(`CLI ${name}换模型必须 -r 且不开轮退出 1`, async () => {
    save(qualified());
    const f = fixture({ models: models instanceof Error ? models : [...models] });
    const result = await f.invoke(["--resume", "thread-test", "-m", "sol"]);
    expect(result.code).toBe(1);
    expect(result.stderr).toContain("无法确定 sol 支持的推理强度，请用 -r 指定");
    expect(f.fake.calls.some((call) => ["turn/start", "thread/resume", "broker/threads/annotate"].includes(call.method))).toBe(false);
  }, TEST_BUDGET);
}

test("CLI notice：目录失败、未知模型与无法核实强度在无 -v 时可见", async () => {
  save(qualified());
  const f = fixture({ models: new Error("目录失败") });
  const result = await f.invoke();
  expect(result.code).toBe(0);
  expectRequest(f, result.stdout, "astra", "high", "沿用线程", "沿用线程");
  for (const text of ["无法读取模型目录", "无法从模型目录确认 astra", "无法从目录核实强度，沿用 high"]) expect(result.stderr).toContain(text);
  expect(result.stderr).not.toContain("模型：");
}, TEST_BUDGET);

test("CLI notice：沿用与 config 强度不支持时替换为自动选择", async () => {
  save(qualified({ meta: { source: "run", model: "astra", effort: "minimal" } }));
  const f = fixture();
  const result = await f.invoke(undefined, { reasoning: "low" });
  expect(result.code).toBe(0);
  expectRequest(f, result.stdout, "astra", "xhigh", "沿用线程", "自动选择");
  expect(result.stderr).toContain("目录未列出原强度 minimal");
  expect(result.stderr).toContain("目录未列出 config 强度 low");
}, TEST_BUDGET);

test("CLI 未发送值 annotate 与 JSON 为 null，续跑同模型保留当前设置", async () => {
  save(qualified({ meta: { source: "run", model: "astra", effort: null } }));
  const f = fixture({ models: [] });
  const result = await f.invoke();
  expect(result.code).toBe(0);
  expectRequest(f, result.stdout, "astra", null, "沿用线程", "上游当前设置");
  expect(requestValues(f).turn).not.toHaveProperty("effort");
  expect(result.stderr).toContain("推理强度沿用上游当前设置（未核实）");
}, TEST_BUDGET);

test("CLI 新开线程不沿用已有历史，默认与空目录行为保持", async () => {
  save(qualified());
  for (const empty of [false, true]) {
    const f = fixture({ models: empty ? [] : catalog });
    const result = await f.invoke([]);
    expect(result.code).toBe(0);
    expectRequest(f, result.stdout, empty ? null : "sol", empty ? null : "medium", "自动选择", empty ? "服务器默认" : "自动选择");
    expect(f.fake.calls.some((call) => call.method === "thread/read" || call.params?.includeRuns)).toBe(false);
    if (empty) {
      expect(result.stderr).toContain("推理强度由服务器默认决定");
      expect(requestValues(f).turn).not.toHaveProperty("model");
      expect(requestValues(f).turn).not.toHaveProperty("effort");
    }
  }
}, TEST_BUDGET);

test("CLI joined 的 JSON 仍为请求值；detach 仍只返回句柄", async () => {
  save(qualified());
  const f = fixture({ shared: true, joined: true });
  const result = await f.invoke();
  expect(result.code).toBe(0);
  expectRequest(f, result.stdout, "astra", "high", "沿用线程", "沿用线程");
  expect(JSON.parse(result.stdout).joined).toBe(true);
  const detached = fixture();
  const handle = await detached.invoke(["--resume", "thread-test", "--detach"]);
  expect(handle.code).toBe(0);
  expect(JSON.parse(handle.stdout)).toEqual({ threadId: "thread-test", shortId: "abcd1234", runId: "run-test", status: "running" });
}, TEST_BUDGET);

test("CLI review 不沿用模型、不登记请求模型或强度", async () => {
  save(qualified());
  const f = fixture();
  const result = await f.invoke(["--resume", "thread-test", "-m", "sol", "-r", "medium"], {}, true);
  expect(result.code).toBe(0);
  const meta = requestValues(f).meta;
  expect(meta.source).toBe("review");
  expect(meta).not.toHaveProperty("model");
  expect(meta).not.toHaveProperty("effort");
  expect(JSON.parse(result.stdout)).not.toHaveProperty("modelSource");
  expect(f.fake.calls.some((call) => call.params?.includeRuns)).toBe(false);
  expect(f.fake.calls.find((call) => call.method === "thread/resume")?.params.config.review_model).toBe("sol");
}, TEST_BUDGET);

test("CLI 改道只影响显示，不改变下一次沿用值", async () => {
  const original = qualified();
  original.turns[0]!.reroute = { fromModel: "astra", toModel: "sol", reason: "服务器改道" };
  original.pendingReroute = { turnId: "turn-next", fromModel: "astra", toModel: "sol", reason: "运行中改道" };
  save(original);
  const f = fixture();
  const result = await f.invoke();
  expect(result.code).toBe(0);
  expectRequest(f, result.stdout, "astra", "high", "沿用线程", "沿用线程");
}, TEST_BUDGET);

for (const method of ["thread/read", "model/list"]) test(`CLI ${method} 超时不能被降级逻辑吞掉`, async () => {
  save(qualified());
  const error = new CxbError("TIMEOUT", "测试超时");
  const f = fixture({ shared: true, ...(method === "thread/read" ? { upstream: error } : { models: error }) });
  const result = await f.invoke();
  expect(result.code).toBe(3);
  expect(f.fake.calls.some((call) => call.method === "turn/start")).toBe(false);
}, TEST_BUDGET);

test("CLI 帮助包含沿用和换模型强度规则", () => {
  const help = renderHelp("run").replace(/\s+/g, "");
  for (const text of ["续跑默认沿用线程原模型", "续跑默认沿用原强度", "换模型不带-r时按config或自动选择", "无法确定时要求-r"]) expect(help).toContain(text);
}, TEST_BUDGET);

test("真 broker：新开请求登记与不同启动回包下的 JSON、续跑沿用", async () => {
  const observer = await h.start();
  const driver = await h.connect();
  const original = driver.rpc.request;
  driver.rpc = { ...driver.rpc, request: async <T>(method: string, params?: unknown, opts?: { timeoutMs?: number }): Promise<T> => {
    const result = await original<T>(method, params, opts);
    return method === "thread/start" ? { ...result, model: "reply-other", reasoningEffort: "ultra" } : result;
  } };
  const first = await h.invoke(createRunHandler(fakeServerRequests()), ["run", "新要求", "-m", "astra", "-r", "high", "--json"], driver);
  expect(first.code).toBe(0);
  const payload = JSON.parse(first.stdout);
  expect(payload).toMatchObject({ model: "astra", reasoning: "high", modelSource: "-m 指定", reasoningSource: "-r 指定" });
  const run = await observer.rpc.request<{ run: RunResult }>("broker/threads/watch", { threadId: payload.thread.id });
  expect(run.run.meta).toMatchObject({ source: "run", model: "astra", effort: "high" });
  const second = await h.invoke(createRunHandler(fakeServerRequests()), ["run", "继续", "--resume", payload.thread.id, "--json"]);
  expect(second.code).toBe(0);
  expect(JSON.parse(second.stdout)).toMatchObject({ model: "astra", reasoning: "high", modelSource: "沿用线程", reasoningSource: "沿用线程" });
  const { calls } = await observer.rpc.request<any>("fixture/state");
  expect(calls.filter((call: any) => call.method === "turn/start").map((call: any) => [call.params.model, call.params.effort]))
    .toEqual([["astra", "high"], ["astra", "high"]]);
}, TEST_BUDGET);
