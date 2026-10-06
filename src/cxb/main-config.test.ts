import { afterEach, beforeEach, expect, test } from "bun:test";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { N5Harness } from "./test-fixtures/n5a-support";
import { FakeRpc, runResult } from "./test-fixtures/run-fakes";
import { writeUserConfig } from "./user-config";
import { HANDLERS, type CommandContext } from "./main";
import type { ProbeConnectOptions } from "./broker-conn";

let h: N5Harness;
let fake: FakeRpc;
let options: ProbeConnectOptions[];
let contexts: CommandContext[];
beforeEach(() => {
  h = new N5Harness(); fake = new FakeRpc(); options = []; contexts = [];
  fake.handle = (method) => {
    if (method === "model/list") return { data: [], nextCursor: null };
    if (method === "thread/start") return { thread: { id: "thread-test" } };
    if (method === "broker/threads/annotate") return { shortId: "abcd1234" };
    if (method === "thread/goal/get") return { goal: null };
    if (method === "turn/start" || method === "review/start") return { runId: "run-test" };
    if (method === "broker/threads/watch") return { run: runResult() };
    if (method === "broker/inbox/wait") return { event: "idle" };
    if (method === "broker/threads/list") return { threads: [] };
    if (method === "thread/list") return { data: [], nextCursor: null };
    return {};
  };
});
afterEach(async () => { await h.cleanup(); expect(fake.subscriptions).toBe(0); }, 10_000);

async function invoke(args: string[]) {
  const command = args[0] as keyof typeof HANDLERS;
  return h.invoke([...args, "--json"], { ...HANDLERS, [command]: async (ctx: CommandContext) => { contexts.push(ctx); return HANDLERS[command]!(ctx); } }, {
    resolveBrokerBin: () => "测试二进制", brokerVersionOf: async () => "1.0.0", connectBroker: async (opts) => {
      options.push(opts);
      return { rpc: fake.rpc, handshake: { brokerVersion: "1.0.0", protocolVersion: 1, epoch: 1, userAgent: "测试", server: { kind: "private" } }, launched: false, close: async () => {} };
    },
  });
}

for (const [key, stored, explicit, flags, fallback] of [
  ["model", "config-model", "cli-model", ["-m", "cli-model"], undefined],
  ["reasoning", "high", "low", ["-r", "low"], undefined],
  ["sandbox", "danger-full-access", "read-only", ["-s", "read-only"], "workspace-write"],
  ["approval", "auto", "on-request", ["--approval", "on-request"], "never"],
  ["timeout", 7, 11, ["--timeout", "11"], 3600],
  ["memory", true, true, ["--memory"], false],
] as const) test(`真实 run 路径显式高于配置高于默认：${key}`, async () => {
  for (const mode of ["default", "config", "explicit"]) {
    // memory 显式只有 true，配置 false 用来证明显式优先。
    writeUserConfig(h.home, mode === "default" ? {} : { [key]: mode === "explicit" && key === "memory" ? false : stored });
    const result = await invoke(["run", "测试要求", ...(mode === "explicit" ? flags : [])]);
    expect(result.code).toBe(0);
    const ctx = contexts.at(-1)!;
    const expected = mode === "default" ? fallback : mode === "config" ? stored : explicit;
    expect(key === "memory" ? ctx.parsed.options.memory : ctx.parsed.globals[key]).toBe(expected);
    expect(ctx.parsed.explicit.has(key)).toBe(mode === "explicit");
    const starts = fake.calls.filter((call) => call.method === "thread/start");
    if (key === "model") expect(starts.at(-1)!.params.model).toBe(expected);
    if (key === "reasoning") expect(starts.at(-1)!.params.config.model_reasoning_effort).toBe(expected);
    if (key === "sandbox") expect(starts.at(-1)!.params.sandbox).toBe(expected);
    if (key === "approval") expect(JSON.parse(result.stdout).approval).toBe(expected);
    if (key === "memory") expect(fake.calls.some((call) => call.method === "thread/memoryMode/set")).toBe(!expected);
    fake.calls.length = 0;
  }
});

test("模板建议高于配置、显式 sandbox 高于模板；未建议时使用配置", async () => {
  mkdirSync(join(h.home, "templates"));
  writeUserConfig(h.home, { sandbox: "danger-full-access" });
  writeFileSync(join(h.home, "templates", "sample.md"), "---\nsandbox: read-only\n---\n模板 {{PROMPT}}");
  expect(JSON.parse((await invoke(["run", "要求", "--template", "sample"])).stdout).sandbox).toBe("read-only");
  expect(JSON.parse((await invoke(["run", "要求", "--template", "sample", "-s", "workspace-write"])).stdout).sandbox).toBe("workspace-write");
  writeFileSync(join(h.home, "templates", "sample.md"), "模板 {{PROMPT}}");
  expect(JSON.parse((await invoke(["run", "要求", "--template", "sample"])).stdout).sandbox).toBe("danger-full-access");
  expect(fake.calls.filter((call) => call.method === "turn/start").at(-1)!.params.input[0].text).toBe("模板 要求");
});

test("真实 wait 将配置 timeout 传入等待请求，显式值仍优先", async () => {
  for (const [stored, flags, expected] of [[{}, [], 3600], [{ timeout: 7 }, [], 7], [{ timeout: 7 }, ["--timeout", "11"], 11]] as const) {
    writeUserConfig(h.home, stored);
    const result = await invoke(["wait", ...flags]);
    expect(result.code).toBe(10);
    const value = fake.calls.filter((call) => call.method === "broker/inbox/wait").at(-1)!.params.timeoutMs;
    expect(value).toBeGreaterThan((expected - 1) * 1000);
    expect(value).toBeLessThanOrEqual(expected * 1000);
  }
});

test("review 固定只读 never，配置不产生显式参数告警", async () => {
  writeUserConfig(h.home, { sandbox: "danger-full-access", approval: "auto" });
  const result = await invoke(["review", "检查本次改动"]);
  expect(result.code).toBe(0);
  expect(result.stderr).toBe("");
  expect(fake.calls.find((call) => call.method === "thread/start")!.params).toMatchObject({ sandbox: "read-only", approvalPolicy: "never" });
});

test("连接传递配置的 idle 毫秒；缺省不传；server 不由 CLI 应用", async () => {
  await invoke(["ls"]);
  expect(options[0]).not.toHaveProperty("idleTimeoutMs");
  writeUserConfig(h.home, { "broker-idle-timeout": 47, server: "shared" });
  await invoke(["ls"]);
  expect(options[1].idleTimeoutMs).toBe(47_000);
  expect(process.env.CXB_SERVER).toBe("private");
});

for (const command of ["run", "review", "ls"]) test(`损坏配置的命令分流与原字节保护：${command}`, async () => {
  const path = join(h.home, "config.json"); const raw = '{"timeout":0}'; writeFileSync(path, raw);
  const result = await invoke([command, ...(command === "ls" ? [] : ["测试"])]);
  expect(result.code).toBe(command === "ls" ? 0 : 1);
  expect(options).toHaveLength(command === "ls" ? 1 : 0);
  if (command === "ls") expect(result.stderr.trim().split("\n")).toEqual(["CLI 配置不可用，本次按内置默认值运行（broker 仍可能按配置或环境变量选择服务器）；详情见 cxb doctor"]);
  expect(readFileSync(path, "utf8")).toBe(raw);
});

test("帮助、版本与参数错误不读取配置；未知键仅详细模式告警", async () => {
  writeFileSync(join(h.home, "config.json"), "损坏");
  expect((await h.invoke(["config", "--help"])).code).toBe(0);
  expect((await h.invoke(["--version"])).code).toBe(0);
  expect((await h.invoke(["不存在", "--json"])).stderr).not.toContain("配置文件不可用");
  writeFileSync(join(h.home, "config.json"), '{"future":1}');
  expect((await h.invoke(["config", "--json"])).stderr).toBe("");
  expect((await h.invoke(["config", "--json", "-v"])).stderr).toContain("未知配置键：future");
});
