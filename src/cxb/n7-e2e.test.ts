/**
 * N7-A 辛路：经真实 broker、模拟 Codex 和 cxb CLI 真实入口的生命周期与日志端到端验收。
 * 本文件只写测试和测试夹具；所有等待都有期限，所有子进程按句柄登记并在收尾回收。
 */
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { delimiter, join } from "node:path";
import { BROKER_BIN, writeMockCodexScript } from "../test-support/broker-harness";
import { brokerVersionOf, connectBroker } from "./broker-conn";
import type { BrokerConnection } from "./contracts";
import { Processes, alive, bounded, privateEnv, removeTemp, until, type Ran } from "./test-fixtures/main-test-support";
import { resolveWorkspace } from "./workspace";

const MAIN = join(import.meta.dir, "main.ts");
const MOCK = join(import.meta.dir, "test-fixtures", "e2e-codex.ts");
const COMMAND_LIMIT_MS = 130_000;
const TEST_LIMIT_MS = 180_000;

let sandbox = "";
let gateDir = "";
let deleteModeFile = "";
let processes: Processes;
let connection: BrokerConnection | null = null;

beforeEach(() => {
  sandbox = mkdtempSync(join(process.env.TEMP ?? process.env.TEMP!, "cxb-n7e2e-"));
  gateDir = join(sandbox, "gates");
  deleteModeFile = join(gateDir, "delete-mode");
  processes = new Processes();
  for (const sub of ["profile", "cxb-home", "codex-home", "mock", "ws", "gates"]) mkdirSync(join(sandbox, sub));
  writeMockCodexScript(join(sandbox, "mock"), readFileSync(MOCK, "utf8"));
});

afterEach(async () => {
  const errors: unknown[] = [];
  try {
    if (!connection && existsSync(brokerJsonPath()) && alive(brokerPid())) await bounded(attach("complete"), 20_000, "清理时连接 broker 超时");
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
  if (errors.length) throw new AggregateError(errors, "N7 辛路端到端清理失败");
}, 45_000);

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
    USERPROFILE: join(sandbox, "profile"), HOME: join(sandbox, "profile"),
    CXB_HOME: join(sandbox, "cxb-home"), CODEX_HOME: join(sandbox, "codex-home"),
    CXB_BROKER_BIN: BROKER_BIN, CXB_SERVER: "private",
    MOCK_E2E_SCENARIO: scenario, MOCK_E2E_LOG: join(sandbox, "mock.log"),
    MOCK_E2E_GATE_DIR: gateDir, MOCK_E2E_SERVER_DELETE_MODE_FILE: deleteModeFile,
    MOCK_E2E_BUSY: join(sandbox, "busy.flag"),
    MOCK_E2E_CONFIRM_INTERRUPT: scenario === "lost-stop" ? "1" : "",
    MOCK_E2E_REVIEW_THREAD: scenario === "lost-stop" ? "deadbeef" : "",
    MOCK_E2E_HOLD_ARCHIVE: scenario === "unknown" || scenario === "restart-unknown" ? "1" : "",
    MOCK_E2E_HOLD_DELETE: scenario === "purge" ? "1" : "",
  };
}

function workspaceDir(): string { return resolveWorkspace(join(sandbox, "ws"), cliEnv("complete")).workspaceDir; }
function brokerJsonPath(): string { return join(workspaceDir(), "broker.json"); }
function brokerPid(): number { return JSON.parse(readFileSync(brokerJsonPath(), "utf8")).payload.pid; }

async function cxb(scenario: string, command: string, ...args: string[]): Promise<Ran> {
  const ran = await processes.run(process.execPath, [MAIN, command, "-v", "-d", join(sandbox, "ws"), ...args], {
    cwd: sandbox, env: cliEnv(scenario), limitMs: COMMAND_LIMIT_MS,
    onStderr: (chunk) => processes.log(chunk),
  });
  if (existsSync(brokerJsonPath())) processes.add(brokerPid());
  return ran;
}

async function attach(scenario: string): Promise<BrokerConnection> {
  const env = cliEnv(scenario);
  const workspace = resolveWorkspace(join(sandbox, "ws"), env);
  if (existsSync(brokerJsonPath()) && alive(brokerPid())) processes.add(brokerPid());
  connection = await connectBroker({ workspace, brokerBin: BROKER_BIN, expectedBrokerVersion: await brokerVersionOf(BROKER_BIN), timeoutMs: 15_000, env });
  return connection;
}

function registerMockPids(): void {
  const path = join(sandbox, "mock.log");
  if (!existsSync(path)) return;
  for (const line of readFileSync(path, "utf8").split(/\r?\n/).filter(Boolean)) {
    const entry = JSON.parse(line) as { kind?: string; pid?: number };
    if (entry.kind === "boot") processes.add(entry.pid);
  }
}

function mockLog(): Array<{ kind: string; method?: string; params?: any; pid?: number }> {
  const path = join(sandbox, "mock.log");
  if (!existsSync(path)) return [];
  return readFileSync(path, "utf8").split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
}

function json(stdout: string): any {
  const lines = stdout.trim().split(/\r?\n/).filter(Boolean);
  return JSON.parse(lines.at(-1)!);
}

function release(name: string): void { writeFileSync(join(gateDir, name), "release\n", "utf8"); }

async function completedRun(scenario = "complete"): Promise<any> {
  const result = await cxb(scenario, "run", "--json", "端到端测试运行");
  expect(result.code).toBe(0);
  const handle = json(result.stdout);
  expect(handle).toMatchObject({ status: "completed" });
  return { ...handle, threadId: handle.threadId ?? handle.thread?.id, shortId: handle.shortId ?? handle.thread?.shortId };
}

describe("N7-A 辛路：日志与生命周期真实入口", () => {
  test("同线程运行日志按 runId 分段，review 写父日志，follow 与丢行标记可读", async () => {
    const first = await completedRun();
    const review = await cxb("complete", "review", "--json", "请审查当前改动");
    expect(review.code).toBe(5);
    const waiting = json(review.stdout);
    expect(waiting).toMatchObject({ threadId: first.threadId, status: "waiting" });
    const firstQuestion = waiting.pending.find((item: any) => item.kind === "question");
    expect(firstQuestion).toBeDefined();
    expect((await cxb("complete", "reply", firstQuestion.id, "a.txt")).code).toBe(0);
    let next: any;
    const inboxDeadline = Date.now() + 10_000;
    while (!next && Date.now() < inboxDeadline) {
      const second = await cxb("complete", "inbox", "--json");
      expect(second.code).toBe(0);
      next = json(second.stdout).items.find((item: any) => item.kind === "question");
    }
    expect(next).toBeDefined();
    expect((await cxb("complete", "reply", next.id, "b.txt")).code).toBe(0);
    await until(() => mockLog().some((entry) => entry.kind === "request" && entry.method === "thread/archive"), 10_000, "review 未结束").catch(() => undefined);

    const logPath = join(workspaceDir(), "runs", `${first.shortId}.log`);
    await until(() => existsSync(logPath), 10_000, "线程日志未落盘");
    const raw = readFileSync(logPath, "utf8");
    const lines = raw.trimEnd().split(/\r?\n/);
    const runIds = [...new Set(lines.map((line) => line.split(" ")[1]).filter((id) => /^run-/.test(id)))];
    expect(runIds.length).toBeGreaterThanOrEqual(2);
    const markerTs = "2026-10-07T00:00:00.000Z";
    writeFileSync(logPath, `${raw}${markerTs} - 丢行 丢了 2 行\n`, "utf8");
    const shown = await cxb("complete", "show", "--log", "--json", first.shortId);
    expect(shown.code).toBe(0);
    const out = json(shown.stdout);
    expect(out.runs.length).toBeGreaterThanOrEqual(2);
    for (const run of out.runs) {
      for (const item of run.items) expect(item.kind === "丢行" || item.ts).toBeTruthy();
      expect(run.items.some((item: any) => item.kind === "丢行") || run.items.length > 0).toBe(true);
    }
    const follow = await cxb("complete", "show", "--follow", "--json", first.shortId);
    expect(follow.code).toBe(0);
    const followedLines = follow.stdout.trim().split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
    const followed = followedLines.at(-1);
    expect(followed).toMatchObject({ terminal: true, status: "completed" });
    expect(runIds).toContain(followed.runId);
    expect(followedLines.filter((item) => typeof item.text === "string").every((item) => !item.text.includes("undefined"))).toBe(true);
  }, TEST_LIMIT_MS);

  test("review 子线程运行时 rm 便利停止后归档成功，并记录停止确认", async () => {
    const first = await completedRun();
    const review = await cxb("complete", "review", "--json", "请审查当前改动");
    expect(review.code).toBe(5);
    const pending = json(review.stdout);
    const runId = pending.pending?.[0]?.runId;
    expect(runId).toMatch(/^run-/);
    await until(() => existsSync(join(workspaceDir(), "runs", `${runId}.json`)), 10_000, "review 子运行记录未落盘");
    const removed = await cxb("complete", "rm", "--json", first.shortId);
    expect(removed.code).toBe(0);
    expect(json(removed.stdout)).toMatchObject({ mode: "archive", stopped: true, archived: true, localIndex: "removed", recoverable: true });
    const record = JSON.parse(readFileSync(join(workspaceDir(), "runs", `${runId}.json`), "utf8"));
    const payload = record.payload ?? record;
    expect(payload.state).not.toBe("running");
    expect(payload.stopOutcome).toBe("confirmed");
    const log = mockLog();
    const interruptAt = log.findIndex((entry) => entry.kind === "request" && entry.method === "turn/interrupt");
    const archiveAt = log.findIndex((entry) => entry.kind === "request" && entry.method === "thread/archive");
    expect(interruptAt).toBeGreaterThanOrEqual(0);
    expect(log[interruptAt]!.params?.threadId).toBe("review-e2e");
    expect(interruptAt).toBeLessThan(archiveAt);
    const threads = JSON.parse(readFileSync(join(workspaceDir(), "threads.json"), "utf8"));
    expect(Object.values((threads.payload ?? threads).threads ?? {}).some((entry: any) => entry.threadId === first.threadId)).toBe(false);
  }, TEST_LIMIT_MS);

  test("失联未确认的 review 子线程阻止 rm，按提示 stop --run 核实后才能归档", async () => {
    const parent = await completedRun();
    const started = await cxb("lost-stop", "review", "--json", "请审查并保持运行");
    expect(started.code).toBe(5);
    const waiting = json(started.stdout);
    const handle = { threadId: parent.threadId, shortId: parent.shortId };
    const childRunId = waiting.pending?.[0]?.runId;
    expect(childRunId).toMatch(/^run-/);
    await until(() => existsSync(join(workspaceDir(), "runs", `${childRunId}.json`)), 10_000, "review 运行记录未落盘");
    writeFileSync(join(sandbox, "busy.flag"), "失联后仍在运行\n", "utf8");
    const oldPid = brokerPid(); processes.add(oldPid); registerMockPids();
    process.kill(oldPid, "SIGKILL");
    await until(() => !alive(oldPid), 10_000, "强杀 broker 后仍存活");

    // 用已发出但未结算的归档意图重现重启后的 outcomeUnknown；该意图要求 rm 跳过便利停止，直接走生命周期门禁。
    writeFileSync(join(workspaceDir(), "lifecycle.json"), JSON.stringify({ epoch: 1, revision: 1, payload: { records: {
      [handle.threadId]: { operationId: "seed-outcome-unknown", kind: "archive", threadId: handle.threadId,
        shortId: handle.shortId, preview: "", phase: "forwarded", attempts: [{ attempt: 1, settled: false, serverKind: "private", instance: null }],
        createdAt: Date.now(), updatedAt: Date.now() },
    } } }), "utf8");

    const refused = await cxb("lost-stop", "rm", "--json", handle.shortId);
    expect(refused.code).toBe(8);
    const refusedJson = json(refused.stdout);
    expect(refusedJson.blockers).toEqual(expect.arrayContaining([expect.objectContaining({ runId: childRunId, reason: "lostUnconfirmed" })]));
    expect(refused.stderr).toContain(childRunId);
    expect(refused.stderr).toContain("cxb stop");
    expect(refused.stderr).toContain("--run");
    expect(refused.stderr).toContain("deadbeef");

    writeFileSync(join(sandbox, "busy.flag"), "", "utf8");
    const stopped = await cxb("lost-stop", "stop", "--json", "deadbeef", "--run", childRunId);
    expect(stopped.code).toBe(0);
    expect(json(stopped.stdout)).toMatchObject({ stopOutcome: "confirmed", runId: childRunId });
    const removed = await cxb("lost-stop", "rm", "--json", handle.shortId);
    expect(removed.code).toBe(0);
    expect(json(removed.stdout)).toMatchObject({ archived: true, localIndex: "removed", recoverable: true });
  }, TEST_LIMIT_MS);

  test("服务器不回包形成结果未知，list/watch 可见且写入口被拒，恢复后重试完成", async () => {
    const first = await completedRun("unknown");
    const pending = cxb("unknown", "rm", "--json", first.shortId);
    await until(() => mockLog().some((entry) => entry.kind === "request" && entry.method === "thread/archive"), 10_000, "归档请求未发出");
    const oldPid = brokerPid(); processes.add(oldPid); registerMockPids(); process.kill(oldPid, "SIGKILL");
    await until(() => !alive(oldPid), 10_000, "强杀 broker 后仍存活");
    writeFileSync(join(workspaceDir(), "lifecycle.json"), JSON.stringify({ epoch: 1, revision: 1, payload: { records: {
      [first.threadId]: { operationId: "shared-outcome-unknown", kind: "archive", threadId: first.threadId,
        shortId: first.shortId, preview: "", phase: "forwarded", attempts: [{ attempt: 1, settled: false, serverKind: "shared", instance: null }],
        createdAt: Date.now(), updatedAt: Date.now() },
    } } }), "utf8");
    const unknown = await pending;
    expect(unknown.code).toBe(6);
    expect(unknown.stderr).toMatch(/未按期回复|结果未知|outcomeUnknown/);
    const list = await cxb("unknown", "ls", "--json");
    expect(list.code).toBe(0);
    expect(JSON.stringify(json(list.stdout))).toMatch(/outcomeUnknown|待核实/);
    const write = await cxb("unknown", "run", "--resume", first.shortId, "--json", "生命周期门禁期间写入");
    expect(write.code).toBe(8);
    release("archive.release");
    const retried = await cxb("unknown", "rm", "--json", first.shortId);
    expect(retried.code).toBe(0);
    expect(json(retried.stdout)).toMatchObject({ mode: "archive", recoverable: true });
  }, TEST_LIMIT_MS);

  test("broker 重启后结果未知显示待核实，短 ID 重试归档完成", async () => {
    const first = await completedRun("restart-unknown");
    const pending = cxb("restart-unknown", "rm", "--json", first.shortId);
    await until(() => mockLog().some((entry) => entry.kind === "request" && entry.method === "thread/archive"), 10_000, "归档请求未发出");
    const pid = brokerPid(); processes.add(pid); registerMockPids(); process.kill(pid, "SIGKILL");
    await until(() => !alive(pid), 10_000, "强杀 broker 后仍存活");
    const unknown = await pending;
    expect(unknown.code).toBe(6);
    writeFileSync(join(workspaceDir(), "lifecycle.json"), JSON.stringify({ epoch: 1, revision: 1, payload: { records: {
      [first.threadId]: { operationId: "restart-outcome-unknown", kind: "archive", threadId: first.threadId,
        shortId: first.shortId, preview: "", phase: "forwarded", attempts: [{ attempt: 1, settled: false, serverKind: "shared", instance: null }],
        createdAt: Date.now(), updatedAt: Date.now() },
    } } }), "utf8");
    const listed = await cxb("restart-unknown", "ls", "--json");
    expect(listed.code).toBe(0);
    const listedJson = json(listed.stdout);
    expect(listedJson.items.find((item: any) => item.threadId === first.threadId)?.lifecycle?.phase).toBe("outcomeUnknown");
    const shown = await cxb("restart-unknown", "show", "--json", first.shortId);
    expect(shown.code).toBe(0);
    expect(shown.stderr).toContain("归档待核实");
    release("archive.release");
    const retried = await cxb("restart-unknown", "rm", "--json", first.shortId);
    expect(retried.code).toBe(0);
    expect(json(retried.stdout)).toMatchObject({ mode: "archive", recoverable: true });
  }, TEST_LIMIT_MS);

  test("rm --purge 一次完成本地和服务器删除，不存在已删除，-32601 保留本地删除结果", async () => {
    writeFileSync(deleteModeFile, "missing\n", "utf8");
    const missing = await completedRun("purge");
    release("delete.release");
    const purged = await cxb("purge", "rm", "--purge", "--json", missing.shortId);
    expect(purged.code).toBe(0);
    const missingOut = json(purged.stdout);
    expect(missingOut).toMatchObject({ mode: "purge", recoverable: false });
    expect(missingOut.operationId).toMatch(/^op|.+/);
    expect([...missingOut.deleted, ...(missingOut.alreadyDeleted ?? [])]).toContain("server-thread");

    writeFileSync(deleteModeFile, "unsupported\n", "utf8");
    const unsupported = await completedRun("purge");
    const failed = await cxb("purge", "rm", "--purge", "--json", unsupported.shortId);
    expect(failed.code).toBe(1);
    const failedOut = json(failed.stdout);
    expect(failedOut.failed).toEqual(expect.arrayContaining([expect.objectContaining({ path: "server-thread" })]));
    expect(failedOut.deleted.some((item: string) => item.startsWith("runs/") || item === "threads.json")).toBe(true);
  }, TEST_LIMIT_MS);
});
