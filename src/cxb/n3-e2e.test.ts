import { waitEnded } from "./test-fixtures/wait-ended";
// N3 端到端验收：从外部启动 `bun src/cxb/main.ts run`，经真 Rust broker 与模拟 codex（test-fixtures/e2e-codex.ts），
// 验证 `run --resume <id> --continue`（命令面规格 3.1 节、详细设计 2.3 节「续跑的条件请求」、3.6 节）：
// 停止后续跑成功；broker 重启后原运行 lost、核实不通过退出 8；核实通过后 broker 自发的恢复订阅带上登记的提问开关与指令
// （S1-评审第 28 节）。临时目录与进程登记、清理沿用 N2 端到端的做法。

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, join } from "node:path";
import { BROKER_BIN, writeMockCodexScript } from "../test-support/broker-harness";
import { brokerVersionOf, connectBroker } from "./broker-conn";
import { ASK_FEATURE_KEY, type BrokerConnection, type RunResult } from "./contracts";
import { Processes, alive, bounded, privateEnv, removeTemp, until, type Ran } from "./test-fixtures/main-test-support";
import { reapTestProcess } from "../test-support/process-identity";
import { resolveWorkspace } from "./workspace";

const MAIN = join(import.meta.dir, "main.ts");
const MOCK = join(import.meta.dir, "test-fixtures", "e2e-codex.ts");
const RUN_LIMIT_MS = 40_000;
const TEST_LIMIT_MS = 120_000;

let sandbox = "";
let processes: Processes;
let connection: BrokerConnection | null = null;

beforeEach(() => {
  sandbox = mkdtempSync(join(tmpdir(), "cxb-n3e2e-"));
  processes = new Processes();
  for (const sub of ["profile", "cxb-home", "codex-home", "mock", "ws"]) mkdirSync(join(sandbox, sub));
  writeMockCodexScript(join(sandbox, "mock"), readFileSync(MOCK, "utf8"));
});

afterEach(async () => {
  const errors: unknown[] = [];
  try {
    if (!connection && existsSync(brokerJsonPath())) await bounded(attach("complete"), 20_000, "清理时连接 broker 超时");
  } catch (error) { errors.push(error); }
  try {
    if (connection) await bounded(connection.rpc.request("broker/shutdown", {}, { timeoutMs: 5000 }), 6000, "关闭 broker 超时");
  } catch (error) { errors.push(error); }
  try { await connection?.close(); } catch (error) { errors.push(error); }
  connection = null;
  try { registerMockPids(); await processes.cleanup(); } catch (error) { errors.push(error); }
  try {
    if (!processes.allExited()) throw new Error("仍有存活进程，保留临时目录供排查");
    await removeTemp(sandbox);
  } catch (error) { errors.push(error); }
  if (errors.length) throw new AggregateError(errors, "N3 端到端清理失败");
}, 40_000);

/** 子进程环境：清掉服务器选择与开发者真实目录，PATH 前置模拟 codex。 */
function cliEnv(scenario: string): NodeJS.ProcessEnv {
  const base = privateEnv(process.env);
  let inheritedPath = "";
  const env: NodeJS.ProcessEnv = {};
  for (const [key, value] of Object.entries(base)) {
    const upper = key.toUpperCase();
    if (upper === "PATH") { inheritedPath = value ?? ""; continue; }
    if (["CXB_HOME", "CODEX_HOME", "USERPROFILE", "HOME"].includes(upper) || upper.startsWith("MOCK_")) continue;
    env[key] = value;
  }
  return {
    ...env,
    PATH: `${join(sandbox, "mock")}${delimiter}${inheritedPath}`,
    USERPROFILE: join(sandbox, "profile"),
    HOME: join(sandbox, "profile"),
    CXB_HOME: join(sandbox, "cxb-home"),
    CODEX_HOME: join(sandbox, "codex-home"),
    CXB_BROKER_BIN: BROKER_BIN,
    CXB_SERVER: "private",
    MOCK_BACKGROUND_TERMINALS: JSON.stringify({ enabled: true }), MOCK_E2E_PENDING_METADATA: "1", MOCK_E2E_SCENARIO: scenario,
    MOCK_E2E_LOG: join(sandbox, "mock.log"),
    MOCK_E2E_BUSY: join(sandbox, "busy.flag"),
  };
}

const workspaceDir = () => resolveWorkspace(join(sandbox, "ws"), cliEnv("complete")).workspaceDir;
const brokerJsonPath = () => join(workspaceDir(), "broker.json");

async function cxbRun(scenario: string, ...args: string[]): Promise<Ran> {
  return processes.run(process.execPath, [MAIN, "run", "-v", "-d", join(sandbox, "ws"), ...args], {
    cwd: sandbox, env: cliEnv(scenario), limitMs: RUN_LIMIT_MS,
    onStderr: (chunk) => processes.log(chunk),
  });
}

/** 连上 CLI 已拉起的 broker（不应再拉起），并登记 broker.json 里的进程。 */
async function attach(scenario: string): Promise<BrokerConnection> {
  const env = cliEnv(scenario);
  const workspace = resolveWorkspace(join(sandbox, "ws"), env);
  if (existsSync(brokerJsonPath())) processes.add(brokerPid());
  connection = await connectBroker({
    workspace, brokerBin: BROKER_BIN, expectedBrokerVersion: await brokerVersionOf(BROKER_BIN),
    timeoutMs: 15_000, env,
  });
  expect(connection.launched).toBe(false);
  return connection;
}

const brokerPid = (): number => JSON.parse(readFileSync(brokerJsonPath(), "utf8")).payload.pid;

interface MockEntry { kind: string; method?: string; params?: any; pid?: number }
function mockLog(): MockEntry[] {
  const path = join(sandbox, "mock.log");
  if (!existsSync(path)) return [];
  return readFileSync(path, "utf8").split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
}
const requestsOf = (method: string) => mockLog().filter((e) => e.kind === "request" && e.method === method);
/** 模拟 codex 是 broker 的子进程；broker 被强杀后它应随标准输入结束自行退出，登记后由清理核对。 */
function registerMockPids(): void { for (const entry of mockLog()) if (entry.kind === "boot") processes.add(entry.pid); }


/** 托管后台运行一轮（hold 场景第一轮一直运行），返回 --json 句柄。 */
async function detachedRun(prompt: string): Promise<{ threadId: string; shortId: string; runId: string }> {
  const r = await cxbRun("hold-pending", "--detach", "--json", prompt);
  expect(r.code).toBe(0);
  const handle = JSON.parse(r.stdout);
  expect(handle.status).toBe("running");
  return handle;
}

describe("N3 端到端：--continue 续跑", () => {
  test("停止后续跑：条件续跑开工，提示含原始要求与补充，结果按新运行输出", async () => {
    const first = await detachedRun("原始要求甲");
    const conn = await attach("hold");
    await bounded(conn.rpc.request("broker/threads/stop", { threadId: first.threadId, runId: first.runId }, { timeoutMs: 45_000 }), 46_000, "停止没有回包");
    let pendingSeen = false;
    const stopped = await waitEnded(conn, first.threadId, first.runId, async () => {
      pendingSeen = true;
      await conn.rpc.request("fixture/releaseResult", {}, { timeoutMs: 5000 });
    });
    expect(pendingSeen).toBe(true);
    expect(stopped.interruptReason).toBe("stop");
    expect(stopped.stopOutcome).toBe("confirmed");
    await conn.close();
    connection = null;

    const r = await cxbRun("hold", "--resume", first.shortId, "--continue", "--json", "补充乙");
    expect(r.code).toBe(0);
    const out = JSON.parse(r.stdout);
    expect(out).toMatchObject({ ok: true, continuedFrom: first.runId, result: "续跑完成" });
    expect(out.runId).not.toBe(first.runId);
    const turns = requestsOf("turn/start");
    expect(turns).toHaveLength(2);
    const text: string = turns[1]!.params.input[0].text;
    expect(text).toContain("【续跑说明】");
    expect(text).toContain("原始要求甲");
    expect(text).toContain("补充乙");
    // broker 转发前去掉条件字段；CLI 不发 thread/start（只有第一轮的那一次）。
    expect(turns[1]!.params).not.toHaveProperty("cxbContinue");
    expect(requestsOf("thread/start")).toHaveLength(1);

    const conn2 = await attach("hold");
    const run = await waitEnded(conn2, first.threadId, out.runId);
    expect(run.continuedFrom).toBe(first.runId);
    expect(run.status).toBe("completed");
  }, TEST_LIMIT_MS);

  test("broker 重启后原运行 lost：核实不通过退出 8 并提示 cxb stop；核实通过后续跑，恢复订阅带登记的提问开关与指令", async () => {
    const first = await detachedRun("原始要求丙");
    await until(() => existsSync(join(workspaceDir(), "runs", `${first.runId}.json`)), 10_000, "运行记录没有落盘");
    const beforeRestart = await attach("hold");
    await beforeRestart.rpc.request("broker/threads/resumeOverrides", { threadId: first.threadId, overrides: null }, { timeoutMs: 5000 });
    const indexPath = join(workspaceDir(), "threads.json");
    await until(() => {
      const index = JSON.parse(readFileSync(indexPath, "utf8")).payload.threads;
      return Object.values(index).some((entry: any) => entry.threadId === first.threadId && entry.resumeOverrides == null);
    }, 10_000, "清除恢复登记没有落盘");
    await beforeRestart.close();
    connection = null;
    const pid = brokerPid();
    processes.add(pid);
    registerMockPids();
    expect(await reapTestProcess(pid, 10_000)).toBe("killed");

    writeFileSync(join(sandbox, "busy.flag"), "仍在运行");
    const refused = await cxbRun("complete", "--resume", first.shortId, "--continue");
    expect(refused.code).toBe(8);
    expect(refused.stderr).toContain("cxb stop");
    expect(requestsOf("turn/start")).toHaveLength(1);

    const restarted = await attach("complete");
    await restarted.rpc.request("broker/threads/resumeOverrides", { threadId: first.threadId, overrides: null }, { timeoutMs: 5000 });
    await restarted.close();
    connection = null;
    rmSync(join(sandbox, "busy.flag"));
    const resumesBefore = requestsOf("thread/resume").length;
    const r = await cxbRun("complete", "--resume", first.shortId, "--continue", "--json", "补充丁");
    expect(r.code).toBe(0);
    expect(JSON.parse(r.stdout)).toMatchObject({ ok: true, continuedFrom: first.runId, result: "续跑完成" });
    const resumes = requestsOf("thread/resume").slice(resumesBefore);
    expect(resumes.length).toBeGreaterThanOrEqual(1);
    expect(resumes[0]!.params.threadId).toBe(first.threadId);
    expect(resumes[0]!.params.config?.[ASK_FEATURE_KEY]).toBe(true);
    expect(typeof resumes[0]!.params.developerInstructions).toBe("string");
    const turns = requestsOf("turn/start");
    expect(turns).toHaveLength(2);
    expect(turns[1]!.params.input[0].text).toContain("原始要求丙");
    expect(turns[1]!.params.input[0].text).toContain("broker 重启");
  }, TEST_LIMIT_MS);
});
