import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseArgv } from "./args";
import { CONFIG_SCHEMA } from "./config-schema";
import type { ChildUpstreamError, RunPolicy, RunResult, ServerRequestDeps, StallReport, StopDiagnostics, StopReply } from "./contracts";
import { renderHelp } from "./help";
import { executeRun } from "./run";
import { applyUserConfig, CONFIG_DEFAULTS, readUserConfig, writeUserConfig } from "./user-config";
import { exitCodeForRun } from "./wait-run";
import { removeTemp } from "./test-fixtures/main-test-support";
import { context, summary } from "./test-fixtures/n4a-unit";
import { runResult } from "./test-fixtures/run-fakes";
import { upstreamError } from "./test-fixtures/upstream-errors";

const maxSeconds = Math.floor(Number.MAX_SAFE_INTEGER / 1000);
const deps: ServerRequestDeps = {
  attachServerRequests: () => () => {},
  handover: async () => { throw new Error("本用例不应交接"); },
  renderPending: () => { throw new Error("本用例不应等待人工回应"); },
};
let dir: string;
let oldCodexHome: string | undefined;
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "cxb-run-policy-"));
  const home = join(dir, "home");
  mkdirSync(home);
  oldCodexHome = process.env.CODEX_HOME;
  process.env.CODEX_HOME = home;
});
afterEach(async () => {
  if (oldCodexHome === undefined) delete process.env.CODEX_HOME;
  else process.env.CODEX_HOME = oldCodexHome;
  await removeTemp(dir);
}, 10_000);

describe("运行策略参数与配置", () => {
  test.each(["run", "review"])("%s 默认 600 秒且只观察，0 与安全整数上界合法", (command) => {
    expect(parseArgv([command, "任务"]).options).toMatchObject({ "stall-timeout": 600, "upstream-error-policy": "observe" });
    for (const seconds of [0, 1, maxSeconds]) {
      const parsed = parseArgv([command, "任务", "--stall-timeout", String(seconds), "--upstream-error-policy", "stop-access"]);
      expect(parsed.options["stall-timeout"]).toBe(seconds);
      expect(parsed.options["upstream-error-policy"]).toBe("stop-access");
      expect(Number.isSafeInteger(seconds * 1000)).toBe(true);
      expect(parsed.explicit.has("stall-timeout")).toBe(true);
    }
  });
  test.each(["run", "review"])("%s 拒绝非法秒数、未知策略和缺失值", (command) => {
    for (const bad of ["", "-1", "1.5", "1e3", "NaN", "Infinity", " 1", "1 ", String(maxSeconds + 1)]) {
      expect(() => parseArgv([command, "任务", "--stall-timeout", bad])).toThrow();
    }
    for (const bad of ["", "stopAccess", "stop", "OBSERVE", "authentication"]) {
      expect(() => parseArgv([command, "任务", "--upstream-error-policy", bad])).toThrow();
    }
    expect(() => parseArgv([command, "任务", "--stall-timeout"])).toThrow();
    expect(() => parseArgv([command, "任务", "--upstream-error-policy"])).toThrow();
  });
  test("wait 只观察，旧秒数规格仍拒绝 0", () => {
    for (const option of ["--stall-timeout", "--upstream-error-policy"]) {
      expect(() => parseArgv(["wait", "abcd1234", option, option === "--stall-timeout" ? "0" : "observe"])).toThrow();
    }
    for (const key of ["timeout", "converge-timeout", "broker-idle-timeout"] as const) {
      expect(CONFIG_SCHEMA[key].validate("0")).toBe(false);
      expect(CONFIG_SCHEMA[key].isValid(0)).toBe(false);
    }
  });
  test("配置读写保留秒数和线上前的策略值，错误存储类型不覆盖文件", () => {
    writeUserConfig(dir, { "stall-timeout": 0, "upstream-error-policy": "stop-access", future: true });
    expect(readUserConfig(dir)).toMatchObject({ values: { "stall-timeout": 0, "upstream-error-policy": "stop-access", future: true }, unknownKeys: ["future"] });
    expect(CONFIG_DEFAULTS).toMatchObject({ "stall-timeout": 600, "upstream-error-policy": "observe" });
    for (const value of [-1, 0.5, maxSeconds + 1, "0", null, false]) {
      const before = readFileSync(join(dir, "config.json"));
      expect(() => writeUserConfig(dir, { "stall-timeout": value })).toThrow();
      expect(readFileSync(join(dir, "config.json"))).toEqual(before);
    }
    for (const value of ["stopAccess", "stop", 0, null, false]) {
      expect(() => writeUserConfig(dir, { "upstream-error-policy": value })).toThrow();
    }
    writeUserConfig(dir, { "stall-timeout": maxSeconds, "upstream-error-policy": "observe" });
    expect(readUserConfig(dir).values["stall-timeout"]).toBe(maxSeconds);
  });
  test("配置只作用于 run/review，显式命令行分别覆盖各键", () => {
    const stored = { path: "测试配置", values: { "stall-timeout": 0, "upstream-error-policy": "stop-access" }, unknownKeys: [] };
    for (const command of ["run", "review"]) {
      const parsed = parseArgv([command, "任务"]);
      applyUserConfig(parsed, stored);
      expect(parsed.options).toMatchObject(stored.values);
      const explicit = parseArgv([command, "任务", "--stall-timeout", "42", "--upstream-error-policy", "observe"]);
      applyUserConfig(explicit, stored);
      expect(explicit.options).toMatchObject({ "stall-timeout": 42, "upstream-error-policy": "observe" });
    }
    for (const argv of [["wait", "abcd1234"], ["show", "abcd1234"], ["ls"]]) {
      const parsed = parseArgv(argv);
      applyUserConfig(parsed, stored);
      expect(parsed.options).not.toHaveProperty("stall-timeout");
      expect(parsed.options).not.toHaveProperty("upstream-error-policy");
    }
  });
  test.each(["run", "review"] as const)("%s 帮助解释期限边界、待答暂停和访问错误策略", (command) => {
    const help = renderHelp(command).replace(/\s+/g, "");
    for (const text of ["--stall-timeout", "600秒", "重复的上游报错不算", "等你回答／审批时不计时", "静默命令、MCP工具、深度推理", "请调大或设0", "退出3",
      "--upstream-error-policy", "observe", "stop-access", "认证、付费、权限三类结构化错误", "不登出、不改账户", "退出4"]) expect(help).toContain(text);
  });
});

function serve(t: ReturnType<typeof context>, run = runResult()) {
  const connect = t.ctx.connect;
  t.ctx.connect = async () => ({ ...await connect(), handshake: { userAgent: "模拟 broker", brokerVersion: "0.1.0", protocolVersion: 1, epoch: 1, server: { kind: "private" } } });
  t.fake.handle = (method) => {
    if (method === "thread/start" || method === "thread/resume") return { thread: { id: "thread-test" } };
    if (method === "broker/threads/annotate") return { shortId: "abcd1234" };
    if (method === "turn/start" || method === "review/start") return { runId: "run-test" };
    if (method === "broker/threads/list") return { threads: [summary()] };
    if (method === "broker/threads/watch") return { thread: summary(), run, pending: [] };
    if (method === "broker/runs/modelSummaries") return { runs: [] };
    return {};
  };
}

describe("开轮策略线上形状", () => {
  for (const command of ["run", "review"] as const) {
    for (const seconds of [undefined, 0, maxSeconds]) test(`${command} 开轮总带策略：${seconds ?? "默认"}`, async () => {
      const args = [command, "任务", "--json", "--timeout", "1", ...(seconds === undefined ? [] : ["--stall-timeout", String(seconds), "--upstream-error-policy", "stop-access"])];
      const t = context(dir, args);
      serve(t);
      const plan = command === "run" ? { command, sandbox: "workspace-write" as const, prompt: "任务" }
        : { command, sandbox: "read-only" as const, review: { mode: "custom" as const, ref: null, base: null, target: { type: "custom", instructions: "任务" } } };
      expect(await executeRun(t.ctx, deps, plan)).toBe(0);
      const start = t.fake.calls.find((call) => call.method === `${command === "run" ? "turn" : "review"}/start`)!.params;
      expect(start.cxbRunPolicy).toEqual({ stallTimeoutMs: (seconds ?? 600) * 1000, upstreamErrorPolicy: seconds === undefined ? "observe" : "stopAccess" });
      expect(start.cxbConvergence).toEqual({ timeoutMs: 3_600_000 });
      expect(t.fake.calls.filter((call) => call.method === "thread/start").every((call) => !("cxbRunPolicy" in call.params))).toBe(true);
      for (const key of ["runPolicy", "stall", "stopDiagnostics", "childUpstreamErrors"]) expect(t.json[0]).not.toHaveProperty(key);
      expect(t.fake.notificationErrors).toEqual([]);
      expect(t.fake.subscriptions).toBe(0);
    });
  }
  test("续跑用本次配置，不继承来源运行的策略", async () => {
    const t = context(dir, ["run", "补充", "--resume", "abcd1234", "--continue", "--json", "--timeout", "1", "-r", "high"]);
    serve(t);
    applyUserConfig(t.ctx.parsed, { path: "测试配置", values: { "stall-timeout": 42, "upstream-error-policy": "observe" }, unknownKeys: [] });
    const original = t.fake.handle;
    let watches = 0;
    t.fake.handle = (method, params) => method === "broker/threads/watch" && watches++ === 0
      ? { run: runResult({ runId: "run-old", status: "interrupted", interruptReason: "stallTimeout", runPolicy: { stallTimeoutMs: 1000, upstreamErrorPolicy: "stopAccess" } }) }
      : original(method, params);
    expect(await executeRun(t.ctx, deps, { command: "run", sandbox: "workspace-write", continue: { supplement: "补充" } })).toBe(0);
    const start = t.fake.calls.find((call) => call.method === "turn/start")!.params;
    expect(start.cxbRunPolicy).toEqual({ stallTimeoutMs: 42000, upstreamErrorPolicy: "observe" });
    expect(start.cxbContinue).toEqual({ fromRunId: "run-old" });
    expect(t.fake.subscriptions).toBe(0);
  });
});

describe("新停止原因退出码顺序", () => {
  const loopGuard = { kind: "noAction" as const, count: 3, threshold: 3, turnIds: [], action: "stop" as const };
  for (const reason of ["stallTimeout", "upstreamAccessError"] as const) {
    test(`${reason} 真实 failed 优先于未确认、空转与 goalBlocked`, () => {
      for (const state of ["stopping", "ended"] as const) {
        expect(exitCodeForRun(runResult({ state, status: "failed", interruptReason: reason, stopOutcome: "unconfirmed", loopGuard }))).toBe(7);
        expect(exitCodeForRun(runResult({ state, status: "goalBlocked", interruptReason: reason, stopOutcome: "unconfirmed", loopGuard }))).toBe(3);
      }
      expect(exitCodeForRun(runResult({ status: "interrupted", interruptReason: reason, stopOutcome: "confirmed", loopGuard }))).toBe(reason === "stallTimeout" ? 3 : 4);
      expect(exitCodeForRun(runResult({ status: "goalBlocked", interruptReason: reason, stopOutcome: "confirmed" }))).toBe(reason === "stallTimeout" ? 3 : 4);
    });
  }
  test("只看最终 status 和有效 interruptReason，不由 terminalReason 推断；旧原因顺序不变", () => {
    const subagentJoin = { phase: "abnormal" as const, wakeupsStarted: 0, wakeupAttempts: 0, startRetriesUsed: 0, workEpoch: 0, deliveredEpoch: 0,
      reconciledEpoch: 0, deadlineAt: 0, terminalReason: "stallTimeout" as const };
    expect(exitCodeForRun(runResult({ subagentJoin, interruptReason: "stop", status: "interrupted" }))).toBe(4);
    expect(exitCodeForRun(runResult({ interruptReason: "stop", status: "interrupted", stopOutcome: "unconfirmed" }))).toBe(4);
    expect(exitCodeForRun(runResult({ interruptReason: "goalLoop", status: "failed" }))).toBe(9);
    expect(exitCodeForRun(runResult({ status: "completed", interruptReason: null, stopOutcome: "confirmed" }))).toBe(0);
  });
});

test("新增诊断类型保留内部 null 与完整子项，旧记录不推造字段", () => {
  const runPolicy: RunPolicy = { stallTimeoutMs: 600000, upstreamErrorPolicy: "observe" };
  const stall: StallReport = { timeoutMs: 600000, phase: "verifying", expiredAt: 1000, idleMs: 600000, lastProgressAt: null, verify: null, expiries: 1 };
  const stopDiagnostics: StopDiagnostics = { reason: "stallTimeout", startedAt: 1000, goalStartedAt: null, goalSettledAt: null,
    interruptSent: false, interruptSentAt: null, interruptReply: null, interruptRepliedAt: null, turnEndedAt: null,
    unconfirmedAt: null, confirmedAt: null, rpcIssues: [{ method: "turn/interrupt", kind: "timeout", at: 2000, message: "请求超时" }] };
  const childUpstreamErrors: ChildUpstreamError[] = [{ threadId: "child-test", parentThreadId: "thread-test", upstreamError: upstreamError({ threadId: "child-test" }) }];
  const current: RunResult = runResult({ runPolicy, stall, stopDiagnostics, childUpstreamErrors });
  expect(JSON.parse(JSON.stringify(current))).toMatchObject({ runPolicy, stall, stopDiagnostics, childUpstreamErrors });
  const reply: Pick<StopReply, "stopDiagnostics"> = { stopDiagnostics: null };
  expect(JSON.stringify(reply)).toBe('{"stopDiagnostics":null}');
  const old = runResult();
  for (const key of ["runPolicy", "stall", "stopDiagnostics", "childUpstreamErrors"]) expect(JSON.parse(JSON.stringify(old))).not.toHaveProperty(key);
});
