// N4 端到端验收：从外部启动 `bun src/cxb/main.ts <命令>`，经真 Rust broker 与模拟 codex（test-fixtures/e2e-codex.ts），
// 验证命令面规格第 10 节里跨命令的几条：run 以 5 交接 → inbox → reply → wait 接上拿到最终结果（结果大于 64 KiB、
// 全是中文，经管道完整输出）；broker 重启后旧事项显示 expired、reply 退出 2；purge 拒绝运行中线程，stop 之后才能删。
// 临时目录与进程登记、清理沿用 N3 端到端的做法。

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { NO_RESPONSE } from "../rpc";
import { until as untilAsync } from "./test-fixtures/run-harness";
import { existsSync, mkdirSync, mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, join } from "node:path";
import { BROKER_BIN, writeMockCodexScript } from "../test-support/broker-harness";
import { brokerVersionOf, connectBroker } from "./broker-conn";
import type { BrokerConnection } from "./contracts";
import { Processes, alive, bounded, privateEnv, removeTemp, until, type Ran } from "./test-fixtures/main-test-support";
import { reapTestProcess } from "../test-support/process-identity";
import { resolveWorkspace } from "./workspace";

const MAIN = join(import.meta.dir, "main.ts");
const MOCK = join(import.meta.dir, "test-fixtures", "e2e-codex.ts");
const CMD_LIMIT_MS = 60_000;
const TEST_LIMIT_MS = 180_000;

let sandbox = "";
let processes: Processes;
let connection: BrokerConnection | null = null;
let legacyReview = false;
let innerReview = false;

beforeEach(() => {
  legacyReview = false;
  innerReview = false;
  sandbox = mkdtempSync(join(tmpdir(), "cxb-n4e2e-"));
  processes = new Processes();
  for (const sub of ["profile", "cxb-home", "codex-home", "mock", "ws"]) mkdirSync(join(sandbox, sub));
  writeMockCodexScript(join(sandbox, "mock"), readFileSync(MOCK, "utf8"));
});

afterEach(async () => {
  const errors: unknown[] = [];
  try {
    if (!connection && existsSync(brokerJsonPath()) && alive(brokerPid())) await bounded(attach(), 20_000, "清理时连接 broker 超时");
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
  if (errors.length) throw new AggregateError(errors, "N4 端到端清理失败");
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
    MOCK_E2E_LEGACY_REVIEW: legacyReview ? "1" : "",
    MOCK_E2E_REVIEW_INNER_TURN: innerReview ? "1" : "",
    MOCK_BACKGROUND_TERMINALS: JSON.stringify({ enabled: true }), MOCK_E2E_SCENARIO: scenario,
    MOCK_E2E_LOG: join(sandbox, "mock.log"),
  };
}

const workspaceDir = () => resolveWorkspace(join(sandbox, "ws"), cliEnv("complete")).workspaceDir;
const brokerJsonPath = () => join(workspaceDir(), "broker.json");
const brokerPid = (): number => JSON.parse(readFileSync(brokerJsonPath(), "utf8")).payload.pid;

/** 运行一个 cxb 命令；broker 由第一个需要它的命令按场景拉起（场景经 launch 传给 broker 与模拟 codex）。 */
async function cxb(scenario: string, command: string, ...args: string[]): Promise<Ran> {
  const ran = await processes.run(process.execPath, [MAIN, command, "-v", "-d", join(sandbox, "ws"), ...args], {
    cwd: sandbox, env: cliEnv(scenario), limitMs: CMD_LIMIT_MS,
    onStderr: (chunk) => processes.log(chunk),
  });
  if (existsSync(brokerJsonPath())) processes.add(brokerPid());
  return ran;
}

/** 连上 CLI 已拉起的 broker（不应再拉起），只在清理时用来发 shutdown。 */
async function attach(): Promise<BrokerConnection> {
  if (connection) return connection;
  const env = cliEnv("complete");
  connection = await connectBroker({
    workspace: resolveWorkspace(join(sandbox, "ws"), env), brokerBin: BROKER_BIN,
    expectedBrokerVersion: await brokerVersionOf(BROKER_BIN), timeoutMs: 15_000, env,
  });
  return connection;
}

interface MockEntry { kind: string; method?: string; params?: any; result?: any; pid?: number }
function mockLog(): MockEntry[] {
  const path = join(sandbox, "mock.log");
  if (!existsSync(path)) return [];
  return readFileSync(path, "utf8").split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
}
function registerMockPids(): void { for (const entry of mockLog()) if (entry.kind === "boot") processes.add(entry.pid!); }

/** run 遇提问以 5 交接：返回线程与待答事项。 */
async function handedOver(scenario: string, prompt: string): Promise<{ threadId: string; itemId: string }> {
  const r = await cxb(scenario, "run", "--json", prompt);
  expect(r.code).toBe(5);
  const waiting = JSON.parse(r.stdout);
  expect(waiting).toMatchObject({ ok: false, status: "waiting" });
  const item = waiting.pending.find((p: { kind: string }) => p.kind === "question");
  expect(item.id).toMatch(/^qu_/);
  return { threadId: waiting.threadId, itemId: item.id };
}

describe("N4 端到端：跨命令链路", () => {
  test("run 以 5 交接 → inbox 列出 → reply 回答（重复回答幂等）→ wait 拿到最终结果，大于 64 KiB 的中文完整输出", async () => {
    const { threadId, itemId } = await handedOver("ask-big", "需要提问的任务");

    const inbox = await cxb("ask-big", "inbox", "--json");
    expect(inbox.code).toBe(0);
    expect(JSON.parse(inbox.stdout).items).toContainEqual(expect.objectContaining({ id: itemId, kind: "question", status: "pending", threadId }));

    const reply = await cxb("ask-big", "reply", "--json", itemId, "b.txt");
    expect(reply.code).toBe(0);
    expect(JSON.parse(reply.stdout)).toMatchObject({ ok: true, command: "reply", id: itemId, kind: "question", duplicate: false });
    const again = await cxb("ask-big", "reply", "--json", itemId, "b.txt");
    expect(again.code).toBe(0);
    expect(JSON.parse(again.stdout)).toMatchObject({ ok: true, duplicate: true });
    const differently = await cxb("ask-big", "reply", itemId, "a.txt");
    expect(differently.code).toBe(1);

    const wait = await cxb("ask-big", "wait", "--json", threadId);
    expect(wait.code).toBe(0);
    expect(Buffer.byteLength(wait.stdout, "utf8")).toBeGreaterThan(64 * 1024);
    const out = JSON.parse(wait.stdout);
    expect(out).toMatchObject({ ok: true, command: "wait", waitFor: "thread", threadId, status: "completed" });
    expect(out.result.startsWith("已选择：b.txt\n")).toBe(true);
    expect(out.result.endsWith("结束标记")).toBe(true);
    expect(out.result).toContain("大段中文结果".repeat(12_000));

    // 模拟 codex 恰好收到一次回答，答案是 b.txt（重复与不同的回答都没有再交付）。
    const replies = mockLog().filter((e) => e.kind === "reply" && e.method === "item/tool/requestUserInput");
    expect(replies).toHaveLength(1);
    expect(replies[0]!.result).toEqual({ answers: { q1: { answers: ["b.txt"] } } });

    // 运行已结束后再 wait：直接给 broker 保存的最近结果（alreadyDone）。
    const later = await cxb("ask-big", "wait", "--json", threadId);
    expect(later.code).toBe(0);
    expect(JSON.parse(later.stdout)).toMatchObject({ alreadyDone: true, status: "completed" });
  }, TEST_LIMIT_MS);

  test("broker 重启后旧事项显示 expired，reply 退出 2", async () => {
    const { itemId } = await handedOver("ask", "重启前的提问");
    const pid = brokerPid();
    processes.add(pid);
    registerMockPids();
    expect(await reapTestProcess(pid, 10_000)).toBe("killed");

    const inbox = await cxb("ask", "inbox", "--json");
    expect(inbox.code).toBe(0);
    expect(JSON.parse(inbox.stdout).items).toContainEqual(expect.objectContaining({ id: itemId, status: "expired" }));
    const reply = await cxb("ask", "reply", itemId, "b.txt");
    expect(reply.code).toBe(2);
    expect(mockLog().filter((e) => e.kind === "reply" && e.method === "item/tool/requestUserInput")).toHaveLength(0);
  }, TEST_LIMIT_MS);

  for (const mode of ["inline", "inline-inner", "legacy"] as const) test(`${mode} 审查提问交接与 wait 完整取得一次结果`, async () => {
    legacyReview = mode === "legacy";
    innerReview = mode === "inline-inner";
    const r = await startReview();
    expect(r.code).toBe(5);
    const waiting = JSON.parse(r.stdout);
    const first = waiting.pending.find((p: { kind: string }) => p.kind === "question");
    expect(first.id).toMatch(/^qu_/);
    const parent: string = waiting.threadId;
    expect(parent).toBe("thread-e2e");

    const observer = await attach();
    const ended: any[] = [];
    const unwatch = observer.rpc.on("broker/run/ended", (params) => ended.push(params));
    const snapshot = await observer.rpc.request<any>("broker/threads/watch", { threadId: parent }, { timeoutMs: 5000 });
    expect(snapshot.run.reviewThreadId).toBe(legacyReview ? "review-e2e" : null);
    expect((await cxb("complete", "reply", first.id, "a.txt")).code).toBe(0);
    const second = await cxb("complete", "wait", "--json", parent);
    expect(second.code).toBe(5);
    const next = JSON.parse(second.stdout).pending.find((p: { kind: string }) => p.kind === "question");
    expect(next.id).not.toBe(first.id);
    expect(next.threadId).toBe(legacyReview ? "review-e2e" : parent);

    expect((await cxb("complete", "reply", next.id, "b.txt")).code).toBe(0);
    const done = await cxb("complete", "wait", "--json", parent);
    expect(done.code).toBe(0);
    expect(JSON.parse(done.stdout)).toMatchObject({ ok: true, waitFor: "thread", status: "completed", result: "审查结论：没有问题" });
    await until(() => ended.length > 0, 5000, "缺少运行结束通知");
    const final = await observer.rpc.request<any>("broker/threads/watch", { threadId: parent }, { timeoutMs: 5000 });
    expect(final.run.runId).toBe(snapshot.run.runId);
    expect(final.run.turns).toHaveLength(1);
    expect(final.run.turnsTotal).toBe(1);
    expect(final.run.turns[0].turnId).toMatch(/^review-turn-\d+$/);
    expect(ended.filter((event) => event.run.runId === snapshot.run.runId)).toHaveLength(1);
    if (innerReview) {
      expect(mockLog().some((entry) => entry.method === "thread/items/list" && entry.params.turnId === final.run.turns[0].turnId)).toBe(true);
      // H2：结果补读只走条目分页，不再整线程回退。
      expect(mockLog().some((entry) => entry.method === "thread/read" && entry.params.threadId === parent && entry.params.includeTurns)).toBe(false);
    }
    unwatch();
  }, TEST_LIMIT_MS);

  for (const legacy of [false, true]) test(`${legacy ? "legacy broker 互操作" : "inline CLI"} stop 与 rm 按实际审查线程停止`, async () => {
    legacyReview = legacy;
    const r = await startReview();
    expect(r.code).toBe(5);
    const parent: string = JSON.parse(r.stdout).threadId;

    const stop = await cxb("complete", "stop", "--json", parent);
    expect(stop.code).toBe(0);
    expect(JSON.parse(stop.stdout)).toMatchObject({ ok: true, stopOutcome: "confirmed", alreadyStopped: false });
    const interrupts = mockLog().filter((e) => e.kind === "request" && e.method === "turn/interrupt");
    expect(interrupts.length).toBeGreaterThan(0);
    expect(interrupts.every((e) => e.params.threadId === (legacyReview ? "review-e2e" : parent))).toBe(true);

    const rm = await cxb("complete", "rm", "--json", parent);
    expect(rm.code).toBe(0);
    expect(JSON.parse(rm.stdout)).toMatchObject({ ok: true, mode: "archive", archived: true });
    const log = mockLog();
    const archiveAt = log.findIndex((e) => e.kind === "request" && e.method === "thread/archive");
    const interruptAt = log.findIndex((e) => e.kind === "request" && e.method === "turn/interrupt");
    expect(log[archiveAt]!.params.threadId).toBe(parent);
    expect(interruptAt).toBeLessThan(archiveAt);
  }, TEST_LIMIT_MS);

  test("purge 拒绝运行中线程并提示 cxb stop；stop 确认后 purge 删除运行记录与服务器线程", async () => {
    const detached = await cxb("hold", "run", "--detach", "--json", "长时间运行的任务");
    expect(detached.code).toBe(0);
    const handle = JSON.parse(detached.stdout) as { threadId: string; shortId: string; runId: string };
    await until(() => existsSync(join(workspaceDir(), "runs", `${handle.runId}.json`)), 10_000, "运行记录没有落盘");

    const refused = await cxb("hold", "rm", "--purge", "--json", handle.shortId);
    expect(refused.code).toBe(8);
    expect(refused.stderr).toContain("cxb stop");
    expect(existsSync(join(workspaceDir(), "runs", `${handle.runId}.json`))).toBe(true);
    expect(mockLog().some((e) => e.kind === "request" && e.method === "thread/delete")).toBe(false);

    const stop = await cxb("hold", "stop", "--json", handle.shortId);
    expect(stop.code).toBe(0);
    expect(JSON.parse(stop.stdout)).toMatchObject({ ok: true, command: "stop", stopOutcome: "confirmed" });

    const purged = await cxb("hold", "rm", "--purge", "--json", handle.shortId);
    expect(purged.code).toBe(0);
    const out = JSON.parse(purged.stdout);
    expect(out).toMatchObject({ ok: true, command: "rm", mode: "purge", recoverable: false, failed: [] });
    expect(out.deleted).toContain(`runs/${handle.runId}.json`);
    expect(out.deleted).toContain("server-thread");
    expect(existsSync(join(workspaceDir(), "runs", `${handle.runId}.json`))).toBe(false);
    expect(mockLog().filter((e) => e.kind === "request" && e.method === "thread/delete").map((e) => e.params.threadId)).toEqual([handle.threadId]);

    const ls = await cxb("hold", "ls", "--local", "--json");
    expect(ls.code).toBe(0);
    expect(JSON.parse(ls.stdout).items.some((i: { threadId: string }) => i.threadId === handle.threadId)).toBe(false);
  }, TEST_LIMIT_MS);
});

/** legacy 用例直接模拟其他 broker 客户端；CLI 生产请求仍始终为 inline。 */
async function startReview(parent?: string): Promise<{ code: number | null; stdout: string }> {
  if (!legacyReview) return cxb("complete", "review", ...(parent ? ["--resume", parent] : []), "--json", "请审查");
  const conn = await attach();
  conn.rpc.onAnyRequest(() => NO_RESPONSE);
  if (!parent) parent = (await conn.rpc.request<any>("thread/start", { sandbox: "read-only", approvalPolicy: "never" }, { timeoutMs: 5000 })).thread.id;
  const run = await conn.rpc.request<any>("review/start", { threadId: parent, delivery: "detached", target: { type: "uncommittedChanges" } }, { timeoutMs: 5000 });
  await conn.rpc.request("broker/threads/detach", { threadId: parent, runId: run.runId, policy: "never" }, { timeoutMs: 5000 });
  let pending: any[] = [];
  await untilAsync(async () => {
    pending = (await conn.rpc.request<any>("broker/inbox/list", { threadId: run.reviewThreadId }, { timeoutMs: 5000 })).items;
    return pending.length > 0;
  }, 10_000);
  return { code: 5, stdout: JSON.stringify({ threadId: parent, status: "waiting", pending }) };
}
