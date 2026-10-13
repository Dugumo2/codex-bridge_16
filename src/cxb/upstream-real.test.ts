import { describe, expect, test } from "bun:test";
import type { RealTrace } from "../test-support/upstream-real";
import type { UpstreamScenario } from "../test-support/upstream-model-server";

const enabled = process.env.CXB_UPSTREAM_REAL === "1";
const suite = enabled ? describe : describe.skip;
const errors = (trace: RealTrace) => trace.events.filter((event) => event.direction === "received" && event.message.method === "error");
const retryInfo = (status: number | null) => ({ responseStreamDisconnected: { httpStatusCode: status } });
const finalInfo = (status: number) => ({ httpConnectionFailed: { httpStatusCode: status } });

function assertTrace(trace: RealTrace) {
  expect(trace.version).toBe("codex-cli 0.160.0");
  expect(trace.failure).toBeUndefined();
  expect(trace.cleaned).toBe(true);
  expect(trace.isolation.configVerified).toBe(true);
  expect(trace.isolation.noProxy).toBe("*");
  expect(trace.isolation.controlledProxy).toBe(new URL(trace.isolation.baseUrl).origin);
  expect(trace.isolation.baseUrl).toMatch(/^http:\/\/127\.0\.0\.1:\d+\/v1$/);
  expect(trace.violations).toEqual([]);
  if (trace.scenario === "connection") expect(trace.requests).toHaveLength(0);
  else expect(trace.requests.length).toBeGreaterThan(0);
  expect(trace.requests.map((request) => request.sequence)).toEqual(trace.requests.map((_, i) => i + 1));
  for (const request of trace.requests) {
    expect(Object.keys(request).sort()).toEqual(["at", "elapsedMs", "path", "sequence", "status"]);
    expect(request.path).toBe("/v1/responses");
  }
  for (const event of errors(trace)) {
    const params = event.message.params;
    expect(params.threadId).toBe(trace.threadId); expect(params.turnId).toBe(trace.turnId);
    expect(typeof params.willRetry).toBe("boolean"); expect(typeof params.error.message).toBe("string");
    expect(params.error).toHaveProperty("codexErrorInfo");
    // HTTP 码只能在 codexErrorInfo 的带字段变体内，不能从外层或正文取。
    expect(params.httpStatusCode).toBeUndefined(); expect(params.error.httpStatusCode).toBeUndefined();
    const info = params.error.codexErrorInfo;
    expect(info === null || typeof info === "string" || (typeof info === "object" && Object.keys(info).length === 1)).toBe(true);
  }
  expect(trace.events.some((event) => event.direction === "received" && event.message.method === "turn/started")).toBe(true);
  expect(trace.events.filter((event) => event.direction === "received" && event.message.method === "turn/completed")).toHaveLength(1);
}

function assertRetries(trace: RealTrace, count: number, status: number | null) {
  const retry = errors(trace).filter((event) => event.message.params.willRetry);
  expect(retry).toHaveLength(count);
  retry.forEach((event, i) => {
    const error = event.message.params.error;
    expect(error.message).toBe(`Reconnecting... ${i + 1}/5`);
    expect(error.codexErrorInfo).toEqual(retryInfo(status));
    expect(typeof error.additionalDetails).toBe("string");
    expect(error.additionalDetails.length).toBeGreaterThan(0);
  });
}
function assertFailed(trace: RealTrace, info: unknown) {
  const notifications = errors(trace);
  const last = notifications.at(-1)!;
  expect(last.message.params.willRetry).toBe(false);
  expect(last.message.params.error.codexErrorInfo).toEqual(info);
  expect(last.message.params.error.additionalDetails).toBeNull();
  expect(last.message.params.error.message).not.toMatch(/^Reconnecting/);
  expect(trace.completed?.status).toBe("failed");
  expect(trace.completed?.error).toEqual(last.message.params.error);
  const finalIndex = trace.events.findIndex((event) => event === last);
  expect(trace.events.findIndex((event) => event.message.method === "turn/completed")).toBeGreaterThan(finalIndex);
}

// 这是冻结契约 2.2 的输入对照，独立于 A 路实现，避免测试复制被测分类器。
export const REAL_CATEGORY_EXPECTATIONS = [
  { scenario: "401", retryCategory: "authentication", finalCategory: "authentication", requests: 6 },
  { scenario: "402", retryCategory: "paymentRequired", finalCategory: "paymentRequired", requests: 6 },
  { scenario: "403", retryCategory: "permissionDenied", finalCategory: "permissionDenied", requests: 6 },
  { scenario: "429", retryCategory: null, finalCategory: "rateLimit", requests: 1 },
  { scenario: "429-usage", retryCategory: null, finalCategory: "usageLimit", requests: 1 },
  { scenario: "500", retryCategory: "unknown", finalCategory: "server", requests: 30 },
  { scenario: "drop", retryCategory: "unknown", finalCategory: "unknown", requests: 6 },
  { scenario: "401-then-ok", retryCategory: "authentication", finalCategory: null, requests: 3 },
  { scenario: "sse429-long", retryCategory: "unknown", finalCategory: null, requests: 1 },
] as const;

suite("真实 0.160.0 上游报错（全流量本地隔离）", () => {
  test("环境采用白名单，凭证、代理和共享选择不继承", async () => {
    const { isolatedEnvironment } = await import("../test-support/upstream-isolation");
    const env = isolatedEnvironment((process.env.TEMP! + "\\ue-white-list-test"), {
      PATH: "path", SYSTEMROOT: "system", OPENAI_API_KEY: "must-not-inherit", CODEX_AUTH_TOKEN: "must-not-inherit",
      CODEX_HOME: "must-not-inherit", CODEX_REFRESH_TOKEN_URL_OVERRIDE: "must-not-inherit", HTTP_PROXY: "must-not-inherit",
      HTTPS_PROXY: "must-not-inherit", ALL_PROXY: "must-not-inherit", NO_PROXY: "must-not-inherit",
      CXB_SERVER: "must-not-inherit",UNRELATED_SECRET: "must-not-inherit",
    });
    for (const key of ["OPENAI_API_KEY", "CODEX_AUTH_TOKEN", "CODEX_REFRESH_TOKEN_URL_OVERRIDE", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "CXB_SERVER", "UNRELATED_SECRET"]) expect(env[key]).toBeUndefined();
    expect(env.NO_PROXY).toBe("*");
    expect(env.PATH).toBe("path"); expect(env.CXB_UE_FAKE_KEY).toBe("ue-local-fixed-fake-key");
    expect(env.CODEX_HOME).toBe((process.env.TEMP! + "\\ue-white-list-test\\codex-home"));
    expect(env.CXB_SERVER).toBe("private");
  });

  test("有效配置被更改时立即拒绝，不回退真实 provider", async () => {
    const { verifyEffectiveConfig } = await import("../test-support/upstream-isolation");
    const config = { model_provider: "ue_script", cli_auth_credentials_store: "file", features: { respect_system_proxy: false, system_proxy_fallback: false }, model_providers: { ue_script: {
      base_url: "http://127.0.0.1:1234/v1", wire_api: "responses", requires_openai_auth: false,
      supports_websockets: false, env_key: "CXB_UE_FAKE_KEY" } } };
    expect(() => verifyEffectiveConfig({ config }, "http://127.0.0.1:1234/v1")).not.toThrow();
    for (const [key, value] of [["base_url", "https://api.openai.com/v1"], ["supports_websockets", true],
      ["requires_openai_auth", true], ["env_key", "OPENAI_API_KEY"], ["request_max_retries", 0], ["stream_max_retries", 0]] as const) {
      const unsafe = structuredClone(config); Object.assign(unsafe.model_providers.ue_script, { [key]: value });
      expect(() => verifyEffectiveConfig({ config: unsafe }, "http://127.0.0.1:1234/v1")).toThrow();
    }
    expect(() => verifyEffectiveConfig({ config: { ...config, model_provider: "openai" } }, "http://127.0.0.1:1234/v1")).toThrow();
    for (const key of ["respect_system_proxy", "system_proxy_fallback"] as const) {
      const unsafe = structuredClone(config); unsafe.features[key] = true;
      expect(() => verifyEffectiveConfig({ config: unsafe }, "http://127.0.0.1:1234/v1")).toThrow();
    }
  });

  test("系统代理占位仅指向脚本本机地址，配置关闭系统代理及回退", async () => {
    const { createUpstreamIsolation, removeIsolation } = await import("../test-support/upstream-isolation");
    const { readFileSync } = await import("node:fs");
    const isolation = createUpstreamIsolation("http://127.0.0.1:1234/v1");
    try {
      for (const key of ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"]) expect(isolation.env[key]).toBe("http://127.0.0.1:1234");
      expect(isolation.env.NO_PROXY).toBe("*");
      const config = readFileSync(`${isolation.env.CODEX_HOME}/config.toml`, "utf8");
      expect(config).toContain("respect_system_proxy = false"); expect(config).toContain("system_proxy_fallback = false");
      expect(config).not.toContain("request_max_retries"); expect(config).not.toContain("stream_max_retries");
    } finally { await removeIsolation(isolation.root); }
  });

  test("ok：一次请求、无 error、真实 completed", async () => {
    const { runUpstreamReal } = await import("../test-support/upstream-real");
    const trace = await runUpstreamReal("ok"); assertTrace(trace);
    expect(trace.requests).toHaveLength(1); expect(errors(trace)).toHaveLength(0);
    expect(trace.completed?.status).toBe("completed"); expect(trace.completed?.error).toBeNull();
    expect(JSON.stringify(trace.completed)).toContain("上游本地脚本执行成功");
  }, 45_000);

  for (const status of [401, 402, 403]) test(`${status}：五次重试、六次请求后 failed`, async () => {
    const { runUpstreamReal } = await import("../test-support/upstream-real");
    const trace = await runUpstreamReal(String(status) as UpstreamScenario); assertTrace(trace);
    expect(trace.requests).toHaveLength(6);
    expect(errors(trace).map((event) => event.message.params.willRetry)).toEqual([true, true, true, true, true, false]);
    assertRetries(trace, 5, status); assertFailed(trace, finalInfo(status));
    for (const event of errors(trace).slice(0, 5)) expect(event.message.params.error.additionalDetails).toContain(`UE scripted HTTP ${status}`);
  }, 45_000);

  for (const scenario of ["429", "429-long"] as const) test(`${scenario}：HTTP 429 直接终止，不做外层重试`, async () => {
    const { runUpstreamReal } = await import("../test-support/upstream-real");
    const trace = await runUpstreamReal(scenario); assertTrace(trace);
    expect(trace.requests).toHaveLength(1); expect(errors(trace).map((event) => event.message.params.willRetry)).toEqual([false]);
    assertFailed(trace, { responseTooManyFailedAttempts: { httpStatusCode: 429 } });
    expect(trace.completed?.durationMs).toBeLessThan(5000);
  }, 45_000);

  test("额度类 429：usageLimitExceeded 字符串，HTTP 码不保留", async () => {
    const { runUpstreamReal } = await import("../test-support/upstream-real");
    const trace = await runUpstreamReal("429-usage"); assertTrace(trace);
    expect(trace.requests).toHaveLength(1); expect(errors(trace).map((event) => event.message.params.willRetry)).toEqual([false]);
    assertFailed(trace, "usageLimitExceeded");
  }, 45_000);

  test("500：请求层五次乘外层六次，重试 null 包装与最终 server", async () => {
    const { runUpstreamReal } = await import("../test-support/upstream-real");
    const trace = await runUpstreamReal("500"); assertTrace(trace);
    expect(trace.requests).toHaveLength(30); assertRetries(trace, 5, null); assertFailed(trace, "internalServerError");
    expect(errors(trace)).toHaveLength(6);
  }, 85_000);

  test("drop：流中途断开五次重试后 failed，未知分类保留详情", async () => {
    const { runUpstreamReal } = await import("../test-support/upstream-real");
    const trace = await runUpstreamReal("drop"); assertTrace(trace);
    expect(trace.requests).toHaveLength(6); assertRetries(trace, 5, null); assertFailed(trace, "other");
  }, 45_000);

  test("401-then-ok：前两次重试后成功，历史报错不能成为失败原因", async () => {
    const { runUpstreamReal } = await import("../test-support/upstream-real");
    const trace = await runUpstreamReal("401-then-ok", { failures: 2 }); assertTrace(trace);
    expect(trace.requests).toHaveLength(3); assertRetries(trace, 2, 401);
    expect(errors(trace).map((event) => event.message.params.willRetry)).toEqual([true, true]);
    expect(trace.completed?.status).toBe("completed"); expect(trace.completed?.error).toBeNull();
  }, 45_000);

  for (const [scenario, interruptOn] of [["401", "retry"], ["slow", "request"], ["500-long", "request"],
    ["sse429-long", "retry"], ["connection", "retry"]] as const) test(`${scenario}：${interruptOn}信号后立即 interrupt 能打断等待`, async () => {
    const { runUpstreamReal } = await import("../test-support/upstream-real");
    const trace = await runUpstreamReal(scenario, { interruptOn, traceName: `${scenario}-interrupt`, deadlineMs: scenario === "connection" ? 60_000 : 25_000 }); assertTrace(trace);
    expect(trace.interrupt).toBeDefined(); expect(trace.completed?.status).toBe("interrupted"); expect(trace.completed?.error).toBeNull();
    expect(trace.interrupt!.acknowledgedAt! - trace.interrupt!.sentAt).toBeLessThan(5000);
    expect(trace.interrupt!.completedAt! - trace.interrupt!.sentAt).toBeLessThan(5000);
    if (["slow", "500-long", "sse429-long"].includes(scenario)) expect(trace.requests).toHaveLength(1);
    if (["slow", "500-long"].includes(scenario)) expect(errors(trace)).toHaveLength(0);
    if (scenario === "sse429-long") { assertRetries(trace, 1, null); expect(errors(trace)[0]!.message.params.error.additionalDetails).toContain("try again in 3600 seconds"); }
    if (scenario === "connection") {
      expect(errors(trace)[0]!.message.params.error.message).toBe("Reconnecting... waiting for network");
      expect(errors(trace)[0]!.message.params.error.codexErrorInfo).toEqual(retryInfo(null));
    }
    if (scenario === "401") expect(errors(trace).map((event) => event.message.params.willRetry)).toEqual([true]);
  }, scenario === "connection" ? 75_000 : 40_000);
});
