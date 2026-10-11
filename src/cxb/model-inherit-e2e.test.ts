/**
 * 模型沿用 D 路：Windows、真实 CLI / broker、既有模拟上游。
 * 先完整执行六步再分别断言，使骨架上的预期失败不会遮住后续步骤。
 * 只观察请求、输出与落盘记录；不调用待测模型解析函数，也不伪造 broker 账本。
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { existsSync, mkdirSync, mkdtempSync, readFileSync } from "node:fs";
import { delimiter, join } from "node:path";
import { BROKER_BIN, writeMockCodexScript } from "../test-support/broker-harness";
import type { Model } from "../types";
import { brokerVersionOf, connectBroker } from "./broker-conn";
import { EXIT, type BrokerConnection } from "./contracts";
import { Processes, alive, bounded, privateEnv, removeTemp, until, type Ran } from "./test-fixtures/main-test-support";
import { resolveWorkspace } from "./workspace";

const ASTRA = "gpt-6-astra";
const MODEL_B = "gpt-6.1-sol";
const MODEL_C = "gpt-6-sol";
// 自动选择、固定恢复回包、改道目标都与 A/B/C 不同，避免错误实现碰巧通过。
const AUTO = "gpt-mock-auto";
const RESPONSE = "gpt-mock-response";
const REROUTED = "gpt-mock-rerouted";
const MAIN = join(import.meta.dir, "main.ts");
const MOCK = join(import.meta.dir, "test-fixtures", "e2e-codex.ts");
const COMMAND_MS = 30_000;

interface Frame {
  id?: number | string; method?: string; params?: any; result?: any;
  error?: { code: number; message: string };
}
interface Trace { kind: string; pid?: number; id?: number | string; method?: string; params?: any; message?: Frame }
interface RunSnapshot { label: string; command: Ran; output: any; request: Frame; trace: Trace[] }
interface Listing { model: unknown; text: string }
interface Check { label: string; actual: unknown; expected: unknown }

function catalog(): Model[] {
  return [ASTRA, MODEL_B, MODEL_C, AUTO].map((id) => ({
    id, model: id, upgrade: null, displayName: id, description: "模型沿用测试目录", hidden: false,
    supportedReasoningEfforts: ["low", "high", "xhigh"].map((reasoningEffort) => ({ reasoningEffort, description: reasoningEffort })),
    defaultReasoningEffort: "low", inputModalities: ["text"], supportsPersonality: false, isDefault: id === AUTO,
  }));
}

describe("模型沿用 Windows 黑盒（真 broker）", () => {
  let sandbox = "";
  const processes = new Processes();
  const knownBoots = new Set<number>();
  let registeredBoots = 0;
  const observerErrors: unknown[] = [];
  let observer: ReturnType<typeof setInterval> | undefined;
  let connection: BrokerConnection | null = null;
  let threadId = "";
  let shortId = "";
  const runs: RunSnapshot[] = [];
  const checks: Check[][] = Array.from({ length: 6 }, () => []);
  let oldPid = 0;
  let restartedPid = 0;

  function env(): NodeJS.ProcessEnv {
    const base: NodeJS.ProcessEnv = {};
    let path = "";
    for (const [key, value] of Object.entries(privateEnv(process.env))) {
      const name = key.toUpperCase();
      if (name === "PATH") { path = value ?? ""; continue; }
      if (["CXB_HOME", "CODEX_HOME", "USERPROFILE", "HOME"].includes(name) || name.startsWith("MOCK_")) continue;
      base[key] = value;
    }
    return { ...base, PATH: `${join(sandbox, "mock")}${delimiter}${path}`,
      USERPROFILE: join(sandbox, "profile"), HOME: join(sandbox, "profile"),
      CXB_HOME: join(sandbox, "cxb-home"), CODEX_HOME: join(sandbox, "codex-home"), CXB_BROKER_BIN: BROKER_BIN,
      CXB_SERVER: "private", MOCK_E2E_SCENARIO: "complete", MOCK_E2E_LOG: join(sandbox, "mock.jsonl"),
      MOCK_E2E_MODEL_INHERIT: JSON.stringify({ models: catalog(), responseModel: RESPONSE, responseEffort: "low" }),
    };
  }
  function workspace() { return resolveWorkspace(join(sandbox, "ws"), env()); }
  function brokerPid(): number | undefined {
    const path = join(workspace().workspaceDir, "broker.json");
    return existsSync(path) ? JSON.parse(readFileSync(path, "utf8")).payload.pid : undefined;
  }
  function trace(): Trace[] {
    const path = join(sandbox, "mock.jsonl");
    if (!existsSync(path)) return [];
    const text = readFileSync(path, "utf8");
    // 文件观察可能撞上正在追加的一行，只解析完整行。
    return text.slice(0, text.lastIndexOf("\n") + 1).split("\n").filter(Boolean).map((line) => JSON.parse(line));
  }
  function registerProcesses(): void {
    const pid = brokerPid();
    if (pid && alive(pid)) processes.add(pid);
    const boots = trace().filter((entry) => entry.kind === "boot");
    for (const entry of boots.slice(registeredBoots)) if (entry.pid) {
      processes.add(entry.pid);
      knownBoots.add(entry.pid);
    }
    registeredBoots = boots.length;
  }
  async function attach(): Promise<BrokerConnection> {
    if (!connection) connection = await connectBroker({ workspace: workspace(), brokerBin: BROKER_BIN,
      expectedBrokerVersion: await brokerVersionOf(BROKER_BIN), timeoutMs: 15_000, env: env() });
    registerProcesses();
    return connection;
  }
  async function shutdown(): Promise<void> {
    if (!connection) return;
    const current = connection;
    const pid = brokerPid();
    registerProcesses();
    const upstreamPids = [...processes.pids].filter((item) => knownBoots.has(item));
    try {
      await current.rpc.request("broker/shutdown", {}, { timeoutMs: 5000 });
      if (pid) await until(() => !alive(pid), 10_000, "broker 关闭后未退出");
      for (const item of upstreamPids) {
        await until(() => !alive(item), 5000, "模拟上游关闭后未退出");
        processes.pids.delete(item);
      }
      if (pid) processes.pids.delete(pid);
    } finally {
      await bounded(current.close(), 5000, "关闭测试连接超时");
      connection = null;
    }
  }
  async function cxb(command: string, ...args: string[]): Promise<Ran> {
    try {
      return await processes.run(process.execPath, [MAIN, command, "-d", join(sandbox, "ws"), ...args], {
        cwd: sandbox, env: env(), limitMs: COMMAND_MS, onStderr: (chunk) => processes.log(chunk),
      });
    } finally { registerProcesses(); }
  }
  function json(result: Ran): any { return JSON.parse(result.stdout.trim()); }
  async function run(label: string, args: string[], failed = false): Promise<RunSnapshot> {
    const offset = trace().length;
    const command = await cxb("run", "--json", ...args, `模型沿用黑盒：${label}`);
    const observed = trace().slice(offset);
    const starts = observed.filter((entry) => entry.kind === "request" && entry.method === "turn/start");
    expect(starts, `${label} 必须恰好到达上游一次，CLI=${JSON.stringify(command)}`).toHaveLength(1);
    expect(command.code, `${label} CLI 退出：${JSON.stringify(command)}`).toBe(failed ? EXIT.CODEX_FAILED : EXIT.OK);
    const output = failed ? JSON.parse((command.stdout.trim() || command.stderr.trim()).split(/\r?\n/).at(-1)!) : json(command);
    if (!failed) {
      expect(output.status).toBe("completed");
      expect(output.thread.id).toBe(threadId || "thread-e2e");
      expect(output.runId).toMatch(/^run-/);
    }
    const snapshot = { label, command, output, request: starts[0]!, trace: observed };
    runs.push(snapshot);
    return snapshot;
  }
  async function ls(): Promise<Listing> {
    const machine = await cxb("ls", "--json");
    const human = await cxb("ls");
    expect(machine.code, machine.stderr).toBe(0);
    expect(human.code, human.stderr).toBe(0);
    const rows = json(machine).items.filter((item: any) => item.threadId === threadId);
    expect(rows).toHaveLength(1);
    expect(human.stdout).toContain(shortId);
    return { model: rows[0].model, text: human.stdout };
  }
  function check(step: number, label: string, actual: unknown, expected: unknown): void {
    checks[step - 1]!.push({ label, actual, expected });
  }
  function requestCheck(step: number, result: RunSnapshot, model: string, effort: string): void {
    check(step, `${result.label}：上游请求`, { model: result.request.params?.model, effort: result.request.params?.effort }, { model, effort });
    check(step, `${result.label}：JSON 取请求值`, { model: result.output.model, reasoning: result.output.reasoning }, { model, reasoning: effort });
  }
  function listingCheck(step: number, label: string, listing: Listing, model: string): void {
    check(step, `${label}：ls JSON 模型`, listing.model, model);
    check(step, `${label}：ls 文本模型`, listing.text.includes(model), true);
  }

  beforeAll(async () => {
    sandbox = mkdtempSync(join(process.env.TEMP!, "cxb-model-inherit-"));
    for (const sub of ["profile", "cxb-home", "codex-home", "mock", "ws"]) mkdirSync(join(sandbox, sub));
    writeMockCodexScript(join(sandbox, "mock"), readFileSync(MOCK, "utf8"));
    observer = setInterval(() => { try { registerProcesses(); } catch (error) { observerErrors.push(error); } }, 50);
    await attach();
    const first = await run("astra/high 新开", ["-m", ASTRA, "-r", "high"]);
    threadId = first.output.thread.id;
    shortId = first.output.thread.shortId;
    requestCheck(1, first, ASTRA, "high");
    const resumed = await run("不写 -m/-r 续跑", ["--resume", shortId]);
    requestCheck(1, resumed, ASTRA, "high");
    listingCheck(1, "astra 续跑后", await ls(), ASTRA);

    const changed = await run("只用 -m 换 B", ["--resume", shortId, "-m", MODEL_B]);
    requestCheck(2, changed, MODEL_B, "xhigh");
    listingCheck(2, "换 B 后", await ls(), MODEL_B);

    const continued = await run("再次不写 -m/-r 续跑", ["--resume", shortId]);
    requestCheck(3, continued, MODEL_B, "xhigh");
    listingCheck(3, "再次续跑后", await ls(), MODEL_B);

    await (await attach()).rpc.request("fixture/modelNextTurn", { fail: true }, { timeoutMs: 5000 });
    const failed = await run("指定 C 但开轮失败", ["--resume", shortId, "-m", MODEL_C, "-r", "high"], true);
    check(4, "失败开轮确实提交了不同的 C", failed.request.params.model, MODEL_C);
    listingCheck(4, "失败开轮之后、重试之前", await ls(), MODEL_B);
    const retry = await run("开轮失败后不写 -m/-r 续跑", ["--resume", shortId]);
    requestCheck(4, retry, MODEL_B, "xhigh");
    listingCheck(4, "失败后再续跑", await ls(), MODEL_B);

    oldPid = brokerPid()!;
    // 通过关闭回包及 PID 退出确认跨重启；保留同一工作区的真实持久化文件。
    await shutdown();
    await attach();
    restartedPid = brokerPid()!;
    const restarted = await run("broker 重启后无参数续跑", ["--resume", shortId]);
    requestCheck(5, restarted, MODEL_B, "xhigh");
    listingCheck(5, "重启续跑后", await ls(), MODEL_B);

    await (await attach()).rpc.request("fixture/modelNextTurn", {
      reroute: { toModel: REROUTED, reason: "模型沿用黑盒模拟服务器改道" },
    }, { timeoutMs: 5000 });
    const rerouted = await run("收到 model/rerouted", ["--resume", shortId]);
    requestCheck(6, rerouted, MODEL_B, "xhigh");
    const withReroute = await ls();
    listingCheck(6, "改道轮之后", withReroute, MODEL_B);
    check(6, "ls 注明服务器改道及目标", /服务器改道/.test(withReroute.text) && withReroute.text.includes(REROUTED), true);
    const clean = await run("下一轮无改道", ["--resume", shortId]);
    requestCheck(6, clean, MODEL_B, "xhigh");
    const withoutReroute = await ls();
    listingCheck(6, "无改道轮之后", withoutReroute, MODEL_B);
    check(6, "ls 不残留改道说明或旧目标", /服务器改道/.test(withoutReroute.text) || withoutReroute.text.includes(REROUTED), false);

    // 输出所有开轮请求和每个检查点，骨架上失败的步骤仍有完整的集成诊断依据。
    console.error("模型黑盒上游 turn/start：" + JSON.stringify(runs.map((item) => ({ label: item.label, params: item.request.params }))));
    console.error("模型黑盒逐步观测：" + JSON.stringify(checks));
  }, 900_000);

  afterAll(async () => {
    if (observer) clearInterval(observer);
    const errors: unknown[] = [...observerErrors];
    try { registerProcesses(); await shutdown(); } catch (error) { errors.push(error); }
    try { await processes.cleanup(); } catch (error) { errors.push(error); }
    try {
      if (!processes.allExited()) throw new Error("仍有存活进程，保留测试目录");
      if (sandbox) await removeTemp(sandbox);
    } catch (error) { errors.push(error); }
    if (errors.length) throw new AggregateError(errors, "模型黑盒清理失败");
  }, 60_000);

  test("夹具自检：九次上游请求、不同的恢复回包、一次失败、真实重启、一次改道", () => {
    expect(runs).toHaveLength(9);
    expect(oldPid).toBeGreaterThan(0);
    expect(restartedPid).toBeGreaterThan(0);
    expect(trace().filter((entry) => entry.kind === "boot")).toHaveLength(2);
    const messages = trace();
    for (const method of ["thread/resume", "thread/read"]) {
      const requests = messages.filter((entry) => entry.kind === "request" && entry.method === method);
      expect(requests.length, `${method} 必须被真实路径调用`).toBeGreaterThan(0);
      for (const request of requests) {
        const index = messages.indexOf(request);
        const reply = messages.slice(index + 1).find((entry) => entry.kind === "sent" && entry.message?.id === request.id)?.message;
        expect(reply?.result?.thread).toMatchObject({ model: RESPONSE, reasoningEffort: "low" });
        if (method === "thread/resume") expect(reply?.result?.model).toBe(RESPONSE);
        expect(request.params?.model).not.toBe(RESPONSE);
      }
    }
    const failed = runs[4]!;
    expect(failed.trace.some((entry) => entry.message?.error?.message === "模型沿用测试：本次开轮失败")).toBe(true);
    expect(failed.trace.some((entry) => entry.message?.method === "turn/started")).toBe(false);
    const notifications = messages.filter((entry) => entry.message?.method === "model/rerouted");
    expect(notifications).toHaveLength(1);
    expect(notifications[0]!.message!.params).toMatchObject({ threadId, fromModel: runs[7]!.request.params.model,
      toModel: REROUTED, reason: "模型沿用黑盒模拟服务器改道" });
    const started = runs[7]!.trace.find((entry) => entry.message?.method === "turn/started");
    expect(notifications[0]!.message!.params.turnId).toBe(started?.message?.params.turn.id);
    expect(runs[8]!.trace.some((entry) => entry.message?.method === "model/rerouted")).toBe(false);
    const turnIds = messages.filter((entry) => entry.message?.method === "turn/started").map((entry) => entry.message!.params.turn.id);
    expect(turnIds).toHaveLength(8);
    expect(new Set(turnIds).size).toBe(8);
  });

  const labels = ["1 astra/high 新开与无参数续跑", "2 显式换 B 与 ls", "3 再次无参数续跑沿用 B",
    "4 开轮失败不覆盖 B", "5 broker 重启后沿用 B", "6 改道注明及下一轮清除"];
  for (const [index, label] of labels.entries()) test(label, () => {
    const failures: unknown[] = [];
    expect(checks[index]!.length).toBeGreaterThan(0);
    for (const item of checks[index]!) {
      try { expect(item.actual, item.label).toEqual(item.expected); }
      catch (error) { failures.push(error); }
    }
    if (failures.length) throw new AggregateError(failures, `${label}：${failures.length}/${checks[index]!.length} 个检查点未满足`);
  });
});
