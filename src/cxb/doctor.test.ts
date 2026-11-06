import { afterEach, beforeEach, expect, test } from "bun:test";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { BrokerNotRunning, resolveBrokerBin } from "./broker-conn";
import { CXB_PROTOCOL_VERSION, CxbError, type BrokerConnection } from "./contracts";
import { createDoctorHandler, describeAuth, runDiagnosticProcess, type DoctorDeps } from "./doctor";
import { HANDLERS } from "./main";
import { N5Harness } from "./test-fixtures/n5a-support";
import { FakeRpc } from "./test-fixtures/run-fakes";
import { alive, until } from "./test-fixtures/main-test-support";
import { processGone, reapTestProcess, waitForProcessGone } from "../test-support/process-identity";
import { bounded } from "./wait-run";
import { writeUserConfig } from "./user-config";

let h: N5Harness;
let rpc: FakeRpc;
let deps: Partial<DoctorDeps>;
let conn: BrokerConnection;
let closes: number;
let probes: unknown[];
let children: Set<number>;
beforeEach(() => {
  h = new N5Harness(); rpc = new FakeRpc(); closes = 0; probes = []; children = new Set();
  conn = { rpc: rpc.rpc, handshake: { brokerVersion: "1.2.3", protocolVersion: CXB_PROTOCOL_VERSION, epoch: 1, userAgent: "模拟服务器", server: { kind: "private", via: "spawn" } }, launched: false, close: async () => { closes++; } };
  rpc.handle = (method) => {
    if (method === "broker/health") return { brokerVersion: "1.2.3", protocolVersion: CXB_PROTOCOL_VERSION, epoch: 1, uptimeMs: 100, claims: 0, pending: 0, storage: "ok", survivesParentJob: true, parentJob: false };
    if (method === "account/read") return { account: { type: "chatgpt", email: "a@b.com", planType: "pro" } };
    if (method === "model/list") return { data: [{ id: "模型甲", isDefault: true, supportedReasoningEfforts: [{ reasoningEffort: "high" }] }], nextCursor: null };
    if (method === "config/read") return { config: { mcp_servers: {} } };
    return {};
  };
  deps = { findExecutable: () => "模拟-codex", resolveBrokerBin: () => "模拟-broker",
    runProcess: async (bin, args) => bin === "模拟-broker" ? "cxb-broker 1.2.3\n" : args[0] === "--version" ? "codex 0.160.0\n" : "default_mode_request_user_input experimental false\n",
    connectBroker: async (opts) => { expect(opts.probe).toBe(true); probes.push(opts); return conn; },
    preflight: async () => ({ check: "mcpApprovalRisk", status: "ok", servers: [] }) };
});
afterEach(async () => {
  const errors: unknown[] = [];
  for (const pid of children) {
    try {
      if (await reapTestProcess(pid, 2000) === "killed") throw new Error("诊断子进程遗留，已强制回收");
    } catch (error) { errors.push(error); }
  }
  try { await h.cleanup(); } catch (error) { errors.push(error); }
  if (errors.length) throw new AggregateError(errors, "doctor 测试清理失败");
}, 15_000);

async function invoke(flags = ["--json", "-v"]) {
  return h.invoke(["doctor", ...flags], { ...HANDLERS, doctor: createDoctorHandler(deps) });
}
const checkOf = (result: { stdout: string }, name: string) => JSON.parse(result.stdout).checks.find((item: any) => item.name === name);

test("所有诊断通过、完整健康信息、有效审批和只读方法", async () => {
  writeUserConfig(h.home, { approval: "auto", "broker-idle-timeout": 123 });
  let approval = "";
  deps.preflight = async (policy, _env, limit) => { approval = policy; expect(limit.timeoutMs).toBeLessThanOrEqual(10_000); expect(limit.signal.aborted).toBe(false); return { check: "mcpApprovalRisk", status: "ok", servers: [] }; };
  const result = await invoke();
  expect(result.code).toBe(0);
  expect(JSON.parse(result.stdout).summary).toEqual({ pass: 11, warn: 0, fail: 0 });
  expect(checkOf(result, "config")).toMatchObject({ idleTimeout: 123, source: "config" });
  expect(checkOf(result, "broker")).toMatchObject({ status: "pass", launched: false, storage: "ok", epoch: 1, uptimeMs: 100, claims: 0, pending: 0 });
  expect(checkOf(result, "broker").detail).toContain("运行中实例的空闲期限未知");
  expect(checkOf(result, "models")).toMatchObject({ count: 1, defaults: ["模型甲"] });
  expect(checkOf(result, "askUserInput").detail).toContain("false");
  expect(approval).toBe("auto");
  expect(closes).toBe(1);
  expect(rpc.calls.map((call) => call.method).sort()).toEqual(["account/read", "broker/health", "config/read", "model/list"]);
  expect(rpc.calls.find((call) => call.method === "account/read")!.params).toEqual({ refreshToken: false });
  expect(rpc.calls.find((call) => call.method === "config/read")!.params).toEqual({ cwd: h.root });
});

test("MCP 配置名单、有效关闭名单与无效名字分开显示，关闭后不报该服务审批风险", async () => {
  writeUserConfig(h.home, { approval: "never", "mcp-disabled": "project,reenabled,disabled,ghost,with.dot" });
  const original = rpc.handle;
  rpc.handle = (method, params) => method === "config/read" ? { config: { mcp_servers: {
    project: { command: "项目服务" }, reenabled: { enabled: true }, disabled: { enabled: false }, "with.dot": {}, kept: {},
  } } } : original(method, params);
  deps.preflight = async () => ({ check: "mcpApprovalRisk", status: "warn", servers: [
    { name: "project", source: "config.toml", effectiveMode: "未配置", risk: true, suggestion: "请加 --approval auto" },
    { name: "with.dot", source: "config.toml", effectiveMode: "未配置", risk: true, suggestion: "请加 --approval auto" },
  ] });
  const result = await invoke();
  const check = checkOf(result, "mcpApprovalRisk");
  expect(check).toMatchObject({ status: "warn", mcpDisabled: ["project", "reenabled", "disabled", "ghost", "with.dot"], mcpWhenNever: "keep",
    mcpEffectiveDisabled: ["project", "reenabled"], mcpUnknown: ["disabled", "ghost"], mcpDotted: ["with.dot"], mcpUnverified: [], mcpConfigVerified: true });
  expect(check.servers.map((server: any) => server.name)).toEqual(["with.dot"]);
  for (const text of ["配置名单", "预计有效关闭名单：project、reenabled", "不存在或已停用：disabled、ghost", "名字含「.」，无法按线程关闭：with.dot", "MCP 工具可能被拒绝"]) expect(check.detail).toContain(text);
  const plain = await invoke([]);
  for (const text of ["配置名单", "预计有效关闭名单", "无法核实或无效的名字", "with.dot"]) expect(plain.stdout).toContain(text);
});

for (const scenario of ["broker 未运行", "方法不可用", "形状异常", "auto 不触发全关"] as const)
  test(`无关闭请求且${scenario}时，配置未核实仍通过`, async () => {
    writeUserConfig(h.home, scenario === "auto 不触发全关" ? { approval: "auto", "mcp-when-never": "off" } : {});
    const original = rpc.handle;
    if (scenario === "broker 未运行") deps.connectBroker = async () => { throw new BrokerNotRunning(); };
    rpc.handle = (method, params) => {
      if (method !== "config/read") return original(method, params);
      if (scenario === "方法不可用") throw Object.assign(new Error("方法不存在"), { code: -32601 });
      return {};
    };
    const check = checkOf(await invoke(), "mcpApprovalRisk");
    expect(check).toMatchObject({ status: "pass", mcpConfigVerified: false, mcpEffectiveDisabled: [] });
    expect(check.detail).toContain("预计有效关闭名单：无");
    expect(check.detail).not.toContain("未能核实");
  });

test("无关闭请求时保留审批预检的风险状态", async () => {
  deps.preflight = async () => ({ check: "mcpApprovalRisk", status: "warn", servers: [] });
  expect(checkOf(await invoke(), "mcpApprovalRisk").status).toBe("warn");
});

test("唯一风险服务预计关闭后，MCP 诊断恢复通过", async () => {
  writeUserConfig(h.home, { approval: "never", "mcp-disabled": "project" });
  const original = rpc.handle;
  rpc.handle = (method, params) => method === "config/read" ? { config: { mcp_servers: { project: {} } } } : original(method, params);
  deps.preflight = async () => ({ check: "mcpApprovalRisk", status: "warn", servers: [
    { name: "project", source: "config.toml", effectiveMode: "未配置", risk: true, suggestion: "请加 --approval auto" },
  ] });
  const check = checkOf(await invoke(), "mcpApprovalRisk");
  expect(check).toMatchObject({ status: "pass", mcpEffectiveDisabled: ["project"], servers: [] });
  expect(check.detail).toContain("未发现审批风险");
});

for (const policy of ["never", "auto"] as const) test(`MCP never 全关按本次有效审批 ${policy} 选择，带点名字不计入关闭`, async () => {
  writeUserConfig(h.home, { approval: policy, "mcp-when-never": "off" });
  const original = rpc.handle;
  rpc.handle = (method, params) => method === "config/read" ? { config: { mcp_servers: {
    project: {}, "with.dot": {}, disabled: { enabled: false },
  } } } : original(method, params);
  const check = checkOf(await invoke(), "mcpApprovalRisk");
  expect(check).toMatchObject({ mcpDisabled: [], mcpWhenNever: "off", mcpEffectiveDisabled: policy === "never" ? ["project"] : [],
    mcpDotted: policy === "never" ? ["with.dot"] : [], mcpUnknown: [], mcpUnverified: [], mcpConfigVerified: true });
  expect(check.detail).toContain(`有效审批 ${policy}`);
});

for (const scenario of ["broker 未运行", "读取失败", "不支持方法", "形状异常", "请求超时"]) test(`MCP ${scenario} 时明确未能核实且不漏报风险`, async () => {
  writeUserConfig(h.home, { approval: "never", "mcp-disabled": "project", "mcp-when-never": "off" });
  const original = rpc.handle;
  if (scenario === "broker 未运行") deps.connectBroker = async () => { throw new BrokerNotRunning(); };
  rpc.handle = (method, params) => {
    if (method !== "config/read") return original(method, params);
    if (scenario === "请求超时") return new Promise<never>(() => {});
    if (scenario === "读取失败") throw new Error("模拟读取失败");
    if (scenario === "不支持方法") throw Object.assign(new Error("方法不存在"), { code: -32601 });
    return { config: { mcp_servers: [] } };
  };
  if (scenario === "请求超时") { deps.itemMs = 300; deps.probeWritable = () => {}; }
  deps.preflight = async () => ({ check: "mcpApprovalRisk", status: "warn", servers: [
    { name: "project", source: "config.toml", effectiveMode: "未配置", risk: true, suggestion: "请加 --approval auto" },
  ] });
  const result = await invoke();
  expect(result.code).toBe(0);
  const check = checkOf(result, "mcpApprovalRisk");
  expect(check).toMatchObject({ status: "warn", mcpEffectiveDisabled: [], mcpUnknown: [], mcpDotted: [], mcpUnverified: ["project"], mcpConfigVerified: false });
  expect(check.servers.map((server: any) => server.name)).toEqual(["project"]);
  expect(check.detail).toContain("预计有效关闭名单：未能核实");
  expect(check.detail).toContain("未能核实：project");
  expect(check.detail).toContain("MCP 工具可能被拒绝");
});

test("未运行正常退出且依赖项未检查，缺省空闲期限三百秒", async () => {
  deps.connectBroker = async (opts) => { expect(opts.probe).toBe(true); throw new BrokerNotRunning(); };
  const result = await invoke();
  expect(result.code).toBe(0);
  expect(checkOf(result, "broker")).toMatchObject({ status: "pass", launched: false });
  expect(checkOf(result, "broker").detail).toContain("未运行");
  for (const name of ["appServer", "account", "models"]) expect(checkOf(result, name)).toMatchObject({ status: "warn", detail: "未检查（broker 未运行或不可用）" });
  expect(checkOf(result, "config")).toMatchObject({ idleTimeout: 300, source: "default" });
  expect(rpc.calls).toHaveLength(0);
});

for (const scenario of ["找不到codex", "codex执行失败", "配置损坏", "目录不可写", "找不到broker", "broker版本损坏", "连接失败", "握手版本不符", "健康协议不符"]) test(`必需检查失败、退出一并继续：${scenario}`, async () => {
  let name = "broker";
  if (scenario === "找不到codex") { deps.findExecutable = () => null; name = "codex"; }
  if (scenario === "codex执行失败") { const run = deps.runProcess!; deps.runProcess = async (bin, args, limit) => { if (bin === "模拟-codex") throw new CxbError("LOCAL_ERROR", "模拟执行失败"); return run(bin, args, limit); }; name = "codex"; }
  if (scenario === "配置损坏") { writeFileSync(join(h.home, "config.json"), "{"); name = "config"; }
  if (scenario === "目录不可写") { deps.probeWritable = () => { throw new CxbError("LOCAL_ERROR", "目录只读"); }; name = "workspace"; }
  if (scenario === "找不到broker") { deps.resolveBrokerBin = () => { throw new CxbError("LOCAL_ERROR", "找不到二进制"); }; name = "brokerBinary"; }
  if (scenario === "broker版本损坏") { const run = deps.runProcess!; deps.runProcess = async (bin, args, limit) => bin === "模拟-broker" ? "错误版本" : run(bin, args, limit); name = "brokerBinary"; }
  if (scenario === "连接失败") deps.connectBroker = async () => { throw new CxbError("BROKER_UNAVAILABLE", "管道不可访问"); };
  if (scenario === "握手版本不符") conn.handshake.brokerVersion = "9.9.9";
  if (scenario === "健康协议不符") { const handle = rpc.handle; rpc.handle = (method, params) => method === "broker/health" ? { ...(handle(method, params) as object), protocolVersion: 999 } : handle(method, params); }
  const result = await invoke();
  expect(result.code).toBe(1);
  expect(JSON.parse(result.stdout).ok).toBe(false);
  expect(checkOf(result, name).status).toBe("fail");
  expect(checkOf(result, "askUserInput")).toBeDefined();
  if (scenario.includes("不符")) {
    expect(checkOf(result, "broker").detail).toContain("下一个普通命令");
    expect(rpc.calls.map((call) => call.method)).toEqual(["broker/health"]);
    expect(closes).toBe(1);
  }
});

for (const scenario of ["未知配置键", "存储降级", "明确未登录", "账号失败", "模型失败", "风险MCP", "预检不可用", "提问开关不存在", "features失败"]) test(`可选检查告警仍退出零：${scenario}`, async () => {
  let name = "account";
  const original = rpc.handle;
  if (scenario === "未知配置键") { writeUserConfig(h.home, { future: 1 }); name = "config"; }
  if (scenario === "存储降级") { rpc.handle = (m, p) => m === "broker/health" ? { ...(original(m, p) as object), storage: "degraded" } : original(m, p); name = "broker"; }
  if (scenario === "明确未登录") rpc.handle = (m, p) => m === "account/read" ? { account: null, requiresOpenaiAuth: true } : original(m, p);
  if (scenario === "账号失败" || scenario === "模型失败") { name = scenario === "账号失败" ? "account" : "models"; rpc.handle = (m, p) => { if (m === (name === "account" ? "account/read" : "model/list")) throw new CxbError("CODEX_FAILED", "请求失败"); return original(m, p); }; }
  if (scenario === "风险MCP" || scenario === "预检不可用") { name = "mcpApprovalRisk"; deps.preflight = async () => ({ check: "mcpApprovalRisk", status: scenario === "风险MCP" ? "warn" : "unavailable", servers: [{ name: "搜索", source: "config.toml", effectiveMode: "未配置", risk: true, suggestion: "请加 --approval auto" }] }); }
  if (scenario === "提问开关不存在" || scenario === "features失败") { name = "askUserInput"; const run = deps.runProcess!; deps.runProcess = async (bin, args, limit) => { if (args[0] === "features") { if (scenario === "features失败") throw new Error("失败"); return "其他开关 true"; } return run(bin, args, limit); }; }
  const result = await invoke();
  expect(result.code).toBe(0); expect(checkOf(result, name).status).toBe("warn");
  if (scenario === "风险MCP") expect(checkOf(result, name).servers).toEqual([{ name: "搜索", source: "config.toml", mode: "未配置", risk: true, recommendation: "请加 --approval auto" }]);
});

test("共享服务器接入信息与本机检查的局限", async () => {
  conn.handshake.server = { kind: "shared", via: "socket", socketPath: "管道路径" };
  const result = await invoke();
  expect(checkOf(result, "appServer")).toMatchObject({ server: conn.handshake.server, userAgent: "模拟服务器" });
  expect(checkOf(result, "appServer").detail).toContain("其他路径未探测");
  expect(checkOf(result, "appServer").detail).toContain("本机 features 结果不能代表共享服务器");
});

for (const [read, ready, text] of [
  [{ account: { type: "chatgpt", email: "a@b.com", planType: "pro" }, requiresOpenaiAuth: true }, true, "pro，a@b.com"],
  [{ account: { type: "apiKey" }, requiresOpenaiAuth: true }, true, "未验证"],
  [{ account: null, requiresOpenaiAuth: false }, true, "不要求"],
  [{ account: { type: "somethingNew" }, requiresOpenaiAuth: true }, true, "未知账号类型"],
  ["unknown", true, "未能确认"], [{ account: null, requiresOpenaiAuth: true }, false, "明确未登录"],
  [{ account: null }, true, "暂按可用"], [{ account: null, requiresOpenaiAuth: null }, true, "暂按可用"], [{}, true, "暂按可用"],
] as const) test(`迁入账号规则：${text} ${JSON.stringify(read)}`, () => {
  expect(describeAuth(read)).toMatchObject({ ready }); expect(describeAuth(read).detail).toContain(text);
});

for (const repeated of [true, false]) test(`模型分页${repeated ? "游标不前进" : "超过二十页"}停止并告警`, async () => {
  const original = rpc.handle; let pages = 0;
  rpc.handle = (m, p) => m === "model/list" ? { data: [], nextCursor: repeated ? "相同" : String(++pages) } : original(m, p);
  const result = await invoke();
  expect(checkOf(result, "models").status).toBe("warn");
  expect(rpc.calls.filter((call) => call.method === "model/list")).toHaveLength(repeated ? 2 : 20);
});

test("模型分页取完，普通文本只列数量和默认，详细 JSON 带推理能力", async () => {
  const original = rpc.handle;
  rpc.handle = (m, p) => m === "model/list" ? p.cursor ? { data: [{ id: "模型乙" }], nextCursor: null } : { data: [{ id: "模型甲", isDefault: true }], nextCursor: "下一页" } : original(m, p);
  const result = await invoke([]);
  expect(result.stdout).toContain("共 2 个模型；默认模型：模型甲"); expect(result.stdout).not.toContain("模型乙");
  const verbose = await invoke(["-v"]);
  expect(verbose.stdout).toContain("全部模型：模型甲、模型乙");
  expect(verbose.stdout.trim().split("\n")).toHaveLength(12);
});

for (const target of ["account/read", "model/list", "features", "codex"]) test(`单项期限传到底层并按检查类型判定：${target}`, async () => {
  deps.itemMs = 20;
  const original = rpc.handle; const run = deps.runProcess!;
  const pending = new Promise<never>(() => {});
  if (target.includes("/")) rpc.handle = (m, p) => m === target ? pending : original(m, p);
  else deps.runProcess = async (bin, args, limit) => {
    if ((target === "features" && args[0] === "features") || (target === "codex" && bin === "模拟-codex" && args[0] === "--version")) {
      await new Promise<void>((resolve) => limit.signal.addEventListener("abort", () => resolve(), { once: true }));
      throw new CxbError("TIMEOUT", "模拟进程已取消并回收");
    }
    return run(bin, args, limit);
  };
  const result = await invoke();
  const name = target === "account/read" ? "account" : target === "model/list" ? "models" : target === "features" ? "askUserInput" : "codex";
  expect(checkOf(result, name).status).toBe(target === "codex" ? "fail" : "warn");
  expect(result.code).toBe(target === "codex" ? 1 : 0);
  expect(closes).toBe(1);
});

test("总预算用尽单列失败，剩余项目未检查，不继续连接", async () => {
  let now = 0; deps.now = () => now; deps.totalMs = 50;
  deps.runProcess = async (_bin, _args, limit) => { expect(limit.timeoutMs).toBe(50); now = 51; return "版本"; };
  const result = await invoke();
  expect(result.code).toBe(1);
  expect(checkOf(result, "doctorBudget").status).toBe("fail");
  for (const name of ["config", "workspace", "brokerBinary", "broker", "appServer", "account", "models", "mcpApprovalRisk", "askUserInput"]) expect(checkOf(result, name)).toMatchObject({ status: "warn", detail: "未检查（总期限用尽）" });
  expect(probes).toHaveLength(0);
});

test("连接迟到收到取消并关闭，不发健康请求", async () => {
  // 只让连接一项吃掉总预算：前面的检查不碰磁盘（真实探测要 fsync，负载高时会在连接前就耗尽 35 毫秒的旧预算）。
  deps.probeWritable = () => {};
  deps.totalMs = 300;
  let stopped = false;
  deps.connectBroker = (opts) => new Promise((resolve) => opts.signal!.addEventListener("abort", () => { stopped = true; resolve(conn); }, { once: true }));
  const result = await invoke();
  await bounded(Promise.resolve(), 1000);
  expect(stopped).toBe(true); expect(closes).toBe(1); expect(rpc.calls).toHaveLength(0);
  expect(checkOf(result, "doctorBudget").status).toBe("fail");
});

test("取消传递给正在等待的连接并收束句柄", async () => {
  let started!: () => void; const ready = new Promise<void>((resolve) => started = resolve);
  let aborted = false;
  deps.connectBroker = (opts) => new Promise((resolve) => { opts.signal!.addEventListener("abort", () => { aborted = true; resolve(conn); }, { once: true }); started(); });
  const work = invoke(); await bounded(ready, 2000);
  // 信号处理层最终覆盖为 130/143；这里验证处理器收到取消和连接回收。
  const signals = [...h.cleanups].map((cleanup) => Promise.resolve(cleanup("SIGINT")));
  const result = await bounded(work, 3000);
  await bounded(Promise.all(signals), 4000);
  expect(result.code).toBe(4); expect(aborted).toBe(true); expect(closes).toBe(1);
});

test("上游伪造换行与秘密在文本和 JSON 中均脱敏为有界单行", async () => {
  conn.handshake.userAgent = "Bearer 隐私甲\nPASS forged: 成功\ttoken=隐私乙";
  conn.handshake.server = { kind: "shared", apiKey: "隐私丙", via: "authorization: 隐私丁" };
  const original = rpc.handle;
  rpc.handle = (m, p) => { if (m === "account/read") throw new Error("Bearer 隐私戊\nPASS evil: 伪造\npassword=隐私己"); return original(m, p); };
  for (const flags of [["--json", "-v"], []]) {
    const result = await invoke(flags);
    for (const secret of ["隐私甲", "隐私乙", "隐私丙", "隐私丁", "隐私戊", "隐私己"]) expect(result.stdout).not.toContain(secret);
    expect(result.stdout).not.toMatch(/\nPASS (forged|evil)/);
    if (flags.length) {
      const object = JSON.parse(result.stdout);
      for (const check of object.checks) { expect(check.detail).not.toMatch(/[\r\n\t]/); expect(check.detail.length).toBeLessThanOrEqual(300); }
      expect(object.checks).toHaveLength(11);
    } else expect(result.stdout.trim().split("\n")).toHaveLength(12);
  }
});

test("codex 的两项诊断经 broker 的 diag-exec 运行，broker 自身版本直接运行", async () => {
  const calls: Array<{ bin: string; args: string[]; broker?: string }> = [];
  const run = deps.runProcess!;
  deps.runProcess = async (bin, args, limit, broker) => { calls.push({ bin, args, broker }); return run(bin, args, limit, broker); };
  expect((await invoke()).code).toBe(0);
  expect(calls).toEqual([
    { bin: "模拟-codex", args: ["--version"], broker: "模拟-broker" },
    { bin: "模拟-broker", args: ["--version"], broker: undefined },
    { bin: "模拟-codex", args: ["features", "list"], broker: "模拟-broker" },
  ]);
});
test("找不到 broker 时不直接运行 codex，codex 一项失败", async () => {
  const calls: string[] = [];
  deps.resolveBrokerBin = () => { throw new CxbError("BROKER_UNAVAILABLE", "找不到 cxb-broker"); };
  const run = deps.runProcess!;
  deps.runProcess = async (bin, args, limit, broker) => { calls.push(bin); return run(bin, args, limit, broker); };
  const result = await invoke();
  expect(checkOf(result, "codex").status).toBe("fail");
  expect(calls).not.toContain("模拟-codex");
  expect(checkOf(result, "askUserInput").status).toBe("warn");
});

for (const cancel of [false, true]) test(`直接运行的不退出子进程${cancel ? "取消" : "超时"}后确认已退出`, async () => {
  const file = join(h.dir, "pid"); const fixture = join(import.meta.dir, "test-fixtures", "n5a-hang.ts");
  const control = new AbortController();
  const work = runDiagnosticProcess(process.execPath, [fixture, file], { timeoutMs: 5000, signal: control.signal });
  void work.catch(() => undefined);
  await until(() => existsSync(file), 3000, "子进程没有登记 PID");
  const pid = Number(readFileSync(file, "utf8")); children.add(pid);
  expect(alive(pid)).toBe(true);
  if (cancel) control.abort();
  await expect(bounded(work, 6000)).rejects.toBeInstanceOf(CxbError);
  expect(await processGone(pid)).toBe(true);
}, 10_000);

/** 用 diag-exec 替身当 broker：.cmd 包装把模式与记录文件放在参数最前面。 */
function fakeDiag(mode: string) {
  const record = join(h.dir, `diag-${mode}.json`);
  const shim = join(h.dir, `diag-${mode}.cmd`);
  writeFileSync(shim, `@"${process.execPath}" "${join(import.meta.dir, "test-fixtures", "n7b-fake-diag.ts")}" ${mode} "${record}" %*\r\n`);
  return { shim, record, argv: () => JSON.parse(readFileSync(record, "utf8")) as string[] };
}
const exitFixture = join(import.meta.dir, "test-fixtures", "n7b-exit.ts");

test("经 diag-exec 运行：参数约定、按状态文件取结果、状态文件用后删除", async () => {
  const diag = fakeDiag("exited");
  const output = await runDiagnosticProcess(process.execPath, [exitFixture, "codex 0.160.0", "", "0"], { timeoutMs: 9000, signal: new AbortController().signal }, diag.shim);
  expect(output).toBe("codex 0.160.0");
  const argv = diag.argv();
  expect(argv.slice(0, 2)).toEqual(["diag-exec", "--timeout-ms"]);
  expect(Number(argv[2])).toBeGreaterThan(0);
  expect(Number(argv[2])).toBeLessThanOrEqual(9000 - 4000);
  expect(argv[3]).toBe("--status-file");
  expect(argv.slice(5)).toEqual(["--", process.execPath, exitFixture, "codex 0.160.0", "", "0"]);
  expect(existsSync(argv[4]!)).toBe(false);
}, 15_000);

for (const [label, mode, args, expected] of [
  ["诊断程序自己退出 124（与超时退出码重叠）按程序失败报告", "exited", ["", "坏了", "124"], { code: "LOCAL_ERROR", message: "诊断程序退出 124" }],
  ["状态文件里大于 255 的退出码原样报告（不用 diag-exec 自身被截断的退出码）", "big", [], { code: "LOCAL_ERROR", message: "诊断程序退出 300" }],
  ["状态为 timeout 报超时", "timeout", [], { code: "TIMEOUT" }],
  ["状态为 failed 报 diag-exec 失败并附原因", "failed", [], { code: "LOCAL_ERROR", message: "模拟：创建进程失败" }],
  ["没有状态文件报 diag-exec 异常", "missing", [], { code: "LOCAL_ERROR", message: "没有有效的状态文件" }],
  ["状态文件损坏报 diag-exec 异常", "corrupt", [], { code: "LOCAL_ERROR", message: "没有有效的状态文件" }],
  ["状态文件里退出码为负数报 diag-exec 异常", "negative", [], { code: "LOCAL_ERROR", message: "没有有效的状态文件" }],
  ["状态文件里退出码超出 u32 报 diag-exec 异常", "overflow", [], { code: "LOCAL_ERROR", message: "没有有效的状态文件" }],
] as const) test(`经 diag-exec 运行：${label}`, async () => {
  const diag = fakeDiag(mode);
  const work = runDiagnosticProcess(process.execPath, [exitFixture, ...args], { timeoutMs: 9000, signal: new AbortController().signal }, diag.shim);
  const error = await work.then(() => null, (error) => error);
  expect(error).toBeInstanceOf(CxbError);
  expect(error.code).toBe(expected.code);
  if ("message" in expected) expect(error.message).toContain(expected.message);
}, 15_000);

for (const cancel of [false, true]) test(`经 diag-exec 运行：${cancel ? "取消" : "超出期限"}时结束 diag-exec 进程并按期返回`, async () => {
  const diag = fakeDiag("hang");
  const control = new AbortController();
  const started = performance.now();
  const work = runDiagnosticProcess(process.execPath, [exitFixture], { timeoutMs: 6000, signal: control.signal }, diag.shim);
  void work.catch(() => undefined);
  await until(() => existsSync(`${diag.record}.pid`), 5000, "diag-exec 替身没有登记 PID");
  const pid = Number(readFileSync(`${diag.record}.pid`, "utf8")); children.add(pid);
  if (cancel) control.abort();
  const error = await bounded(work.then(() => null, (error) => error), 8000);
  expect(error).toBeInstanceOf(CxbError);
  expect(error.code).toBe(cancel ? "INTERRUPTED" : "TIMEOUT");
  expect(performance.now() - started).toBeLessThan(cancel ? 4000 : 7000);
  // .cmd 包装下 diag-exec 替身是孙进程：结束包装进程不一定结束它；真 diag-exec 由 Job 保证（合并后经真 broker 验证）。
  await reapTestProcess(pid, 3000);
  children.delete(pid);
}, 15_000);

// 经真 broker 的 diag-exec（N7-A 甲路）：N5 的已知限制——先退出的诊断程序留下的后代——由 Job 回收。
test("经真 diag-exec 运行：诊断程序先退出、后代占着输出时，按期正常返回且后代已被结束", async () => {
  const file = join(h.dir, "pid");
  const started = performance.now();
  const output = await runDiagnosticProcess(process.execPath, [join(import.meta.dir, "test-fixtures", "n7b-orphan.ts"), file],
    { timeoutMs: 9000, signal: new AbortController().signal }, resolveBrokerBin());
  const pid = Number(readFileSync(file, "utf8")); children.add(pid);
  expect(output).toContain("父程序退出");
  expect(performance.now() - started).toBeLessThan(8000);
  await waitForProcessGone(pid, 3000, "后代没有被 Job 结束");
  children.delete(pid);
}, 15_000);
test("经真 diag-exec 运行：诊断程序超时，整棵树被结束并报超时", async () => {
  const file = join(h.dir, "pid");
  const work = runDiagnosticProcess(process.execPath, [join(import.meta.dir, "test-fixtures", "n5a-hang.ts"), file],
    { timeoutMs: 6000, signal: new AbortController().signal }, resolveBrokerBin());
  await until(() => existsSync(file), 5000, "子进程没有登记 PID");
  const pid = Number(readFileSync(file, "utf8")); children.add(pid);
  await expect(bounded(work, 9000)).rejects.toMatchObject({ code: "TIMEOUT" });
  await waitForProcessGone(pid, 3000, "超时后子进程没有被结束");
  children.delete(pid);
}, 15_000);

// 直接运行（只用于不派生子进程的 cxb-broker --version）时，先退出的程序留下的后代无法可靠回收；
// 这里只验证 doctor 按期返回、不被管道挂住，后代由本测试清理。经 diag-exec 时后代由 Job 回收（合并后经真 broker 验证）。
test("直接运行的诊断程序先退出、后代仍占着输出管道时，按期以诊断错误返回并取消读取", async () => {
  const file = join(h.dir, "pid"); const fixture = join(import.meta.dir, "test-fixtures", "n5a-orphan.ts");
  const timeoutMs = 6000;
  const started = performance.now();
  const work = runDiagnosticProcess(process.execPath, [fixture, file], { timeoutMs, signal: new AbortController().signal });
  void work.catch(() => undefined);
  await until(() => existsSync(file) && Number(readFileSync(file, "utf8")) > 0, 3000, "后代没有登记 PID");
  const pid = Number(readFileSync(file, "utf8")); children.add(pid);
  let watchdog: ReturnType<typeof setTimeout> | undefined;
  const outcome = await Promise.race([
    work.then(() => "正常结束", (error) => error),
    new Promise((resolve) => { watchdog = setTimeout(() => resolve(new Error("看门狗：诊断未落定")), timeoutMs + 2000); }),
  ]).finally(() => clearTimeout(watchdog));
  expect(outcome).toBeInstanceOf(CxbError);
  expect(performance.now() - started).toBeLessThan(timeoutMs + 1000);
  // 已知限制：后代残留，由本用例结束并确认退出。
  await reapTestProcess(pid, 3000);
  children.delete(pid);
}, 15_000);

for (const mode of ["both", "stdout-only"] as const) test(`诊断程序先退出、后代占着${mode === "both" ? "两路" : "仅标准输出"}管道时，运行 doctor 的进程收尾后能自然退出（未结束的读取已取消）`, async () => {
  const pidFile = join(h.dir, "pid"); const doneFile = join(h.dir, "done");
  const runner = Bun.spawn([process.execPath, join(import.meta.dir, "test-fixtures", "n5a-orphan-runner.ts"), pidFile, doneFile, mode],
    { stdin: "ignore", stdout: "ignore", stderr: "ignore", windowsHide: true });
  children.add(runner.pid);
  await until(() => existsSync(pidFile) && Number(readFileSync(pidFile, "utf8")) > 0, 3000, "后代没有登记 PID");
  const pid = Number(readFileSync(pidFile, "utf8")); children.add(pid);
  let watchdog: ReturnType<typeof setTimeout> | undefined;
  const exited = await Promise.race([
    runner.exited.then(() => true),
    new Promise<boolean>((resolve) => { watchdog = setTimeout(() => resolve(false), 9000); }),
  ]).finally(() => clearTimeout(watchdog));
  expect(existsSync(doneFile)).toBe(true);
  expect(exited).toBe(true);
  children.delete(runner.pid);
  await reapTestProcess(pid, 3000);
  children.delete(pid);
}, 20_000);
