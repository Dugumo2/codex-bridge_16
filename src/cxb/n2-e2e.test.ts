// N2 端到端验收（Claude 写，只依据规格，不看各路自测）：从外部启动 `bun src/cxb/main.ts run`，
// 经真 Rust broker 与模拟 codex（test-fixtures/e2e-codex.ts，服务端请求答复前本轮一直运行），验证
// 「等待回应时的交接」（命令面规格 3.1 节）、提问一律交给人（S1-评审第 27 节 L）、inbox 生命周期（详细设计 2.3 节）。
// 每个用例都在临时 USERPROFILE、HOME、CXB_HOME、CODEX_HOME 下运行；broker 进程从 `-v` 的拉起行登记，
// 结束时经 broker/shutdown 关闭并确认退出。

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, join } from "node:path";
import { BROKER_BIN, writeMockCodexScript } from "../test-support/broker-harness";
import { brokerVersionOf, connectBroker } from "./broker-conn";
import { ASK_FEATURE_KEY, rpcFailure, type BrokerConnection, type InboxItem, type RunResult } from "./contracts";
import { Processes, bounded, privateEnv, removeTemp, until, type Ran } from "./test-fixtures/main-test-support";
import { resolveWorkspace } from "./workspace";

const MAIN = join(import.meta.dir, "main.ts");
const MOCK = join(import.meta.dir, "test-fixtures", "e2e-codex.ts");
const RUN_LIMIT_MS = 40_000;
const TEST_LIMIT_MS = 90_000;

let sandbox = "";
let processes: Processes;
let connection: BrokerConnection | null = null;

beforeEach(() => {
  sandbox = mkdtempSync(join(tmpdir(), "cxb-n2e2e-"));
  processes = new Processes();
  for (const sub of ["profile", "cxb-home", "codex-home", "mock", "ws"]) mkdirSync(join(sandbox, sub));
  writeMockCodexScript(join(sandbox, "mock"), readFileSync(MOCK, "utf8"));
});

afterEach(async () => {
  const errors: unknown[] = [];
  try {
    const brokerJson = join(resolveWorkspace(join(sandbox, "ws"), cliEnv("ask")).workspaceDir, "broker.json");
    if (!connection && existsSync(brokerJson)) await bounded(attach("ask"), 20_000, "清理时连接 broker 超时");
  } catch (error) { errors.push(error); }
  try {
    if (connection) {
      await bounded(connection.rpc.request("broker/shutdown", {}, { timeoutMs: 5000 }), 6000, "关闭 broker 超时");
    }
  } catch (error) { errors.push(error); }
  try { await connection?.close(); } catch (error) { errors.push(error); }
  connection = null;
  try { await processes.cleanup(); } catch (error) { errors.push(error); }
  try {
    if (!processes.allExited()) throw new Error("仍有存活进程，保留临时目录供排查");
    await removeTemp(sandbox);
  } catch (error) { errors.push(error); }
  if (errors.length) throw new AggregateError(errors, "N2 端到端清理失败");
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
    MOCK_E2E_SCENARIO: scenario,
    MOCK_E2E_LOG: join(sandbox, "mock.log"),
    MOCK_E2E_TRIGGER: join(sandbox, "cancel.trigger"),
  };
}

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
  const brokerJson = join(workspace.workspaceDir, "broker.json");
  if (existsSync(brokerJson)) processes.add(JSON.parse(readFileSync(brokerJson, "utf8")).payload.pid);
  connection = await connectBroker({
    workspace, brokerBin: BROKER_BIN, expectedBrokerVersion: await brokerVersionOf(BROKER_BIN),
    timeoutMs: 15_000, env,
  });
  expect(connection.launched).toBe(false);
  return connection;
}

interface MockEntry { kind: "request" | "reply" | "cancelled"; method?: string; params?: any; id?: string; result?: any; error?: any }
function mockLog(): MockEntry[] {
  const path = join(sandbox, "mock.log");
  if (!existsSync(path)) return [];
  return readFileSync(path, "utf8").split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
}
const requestsOf = (method: string) => mockLog().filter((e) => e.kind === "request" && e.method === method);
const repliesOf = (method: string) => mockLog().filter((e) => e.kind === "reply" && e.method === method);

async function pendingItems(conn: BrokerConnection, includeClosed = false): Promise<InboxItem[]> {
  return (await conn.rpc.request<{ items: InboxItem[] }>("broker/inbox/list", { includeClosed })).items;
}

/** 等这次运行结束且最后一轮结果就绪：订阅先于快照。 */
async function waitEnded(conn: BrokerConnection, threadId: string): Promise<RunResult> {
  let ended: RunResult | null = null;
  const off = conn.rpc.on("broker/run/ended", (params) => {
    const p = params as { threadId: string; run: RunResult };
    if (p.threadId === threadId) ended = p.run;
  });
  try {
    const snapshot = await conn.rpc.request<{ run: RunResult | null }>("broker/threads/watch", { threadId });
    if (snapshot.run && snapshot.run.state === "ended") ended = snapshot.run;
    await until(() => ended !== null && ended.turns.at(-1)?.ready !== "pending", 20_000, "运行没有结束");
    return ended!;
  } finally { off(); }
}

const ANSWER_B = { answers: { q1: { answers: ["b.txt"] } } };

describe("N2 端到端：提问交给人、交接退出 5、托管后回答", () => {
  test("提问：run 交接退出 5，turn 不被中断；回答后运行按回答完成，app-server 恰好收到一次回包", async () => {
    const r = await cxbRun("ask", "请选择文件并创建它");
    expect(r.code).toBe(5);
    expect(r.stdout).toMatch(/qu_\w+/);
    expect(r.stdout).toContain("cxb reply");
    expect(r.stdout).toContain("cxb wait");
    expect(requestsOf("turn/interrupt")).toHaveLength(0);

    // 规格与第 27、28 节：新建线程带提问开关与开发者指令；每轮 turn/start 带 sandboxPolicy 与 clientUserMessageId。
    const start = requestsOf("thread/start")[0]?.params;
    expect(start?.config?.[ASK_FEATURE_KEY]).toBe(true);
    expect(typeof start?.developerInstructions).toBe("string");
    const turn = requestsOf("turn/start")[0]?.params;
    expect(turn?.sandboxPolicy).toBeDefined();
    expect(typeof turn?.clientUserMessageId).toBe("string");
    expect(requestsOf("thread/goal/get").length).toBeGreaterThanOrEqual(1);
    expect(repliesOf("item/tool/requestUserInput")).toHaveLength(0);

    const conn = await attach("ask");
    const items = await pendingItems(conn);
    expect(items).toHaveLength(1);
    expect(items[0]!.kind).toBe("question");
    expect(r.stdout).toContain(items[0]!.id);
    const resolved = await conn.rpc.request<{ status: string; duplicate: boolean }>("broker/inbox/resolve", { id: items[0]!.id, answer: ANSWER_B });
    expect(resolved).toEqual({ status: "resolved", duplicate: false });

    const run = await waitEnded(conn, items[0]!.threadId);
    expect(run.status).toBe("completed");
    expect(run.turns.at(-1)!.output).toBe("已选择：b.txt");
    const replies = repliesOf("item/tool/requestUserInput");
    expect(replies).toHaveLength(1);
    expect(replies[0]!.result).toEqual(ANSWER_B);
  }, TEST_LIMIT_MS);

  test("默认 never：交接前的审批由 CLI 自动接受，交接后的审批由 broker 自动接受，都不进 inbox", async () => {
    const r = await cxbRun("approval-ask-approval", "先审批再提问");
    expect(r.code).toBe(5);
    const first = repliesOf("item/commandExecution/requestApproval");
    expect(first).toHaveLength(1);
    expect(first[0]!.result).toEqual({ decision: "accept" });

    const conn = await attach("approval-ask-approval");
    const [question] = await pendingItems(conn);
    expect(question!.kind).toBe("question");
    await conn.rpc.request("broker/inbox/resolve", { id: question!.id, answer: ANSWER_B });
    const run = await waitEnded(conn, question!.threadId);
    expect(run.status).toBe("completed");

    const approvals = repliesOf("item/commandExecution/requestApproval");
    expect(approvals).toHaveLength(2);
    expect(approvals[1]!.result).toEqual({ decision: "accept" });
    const all = await pendingItems(conn, true);
    expect(all.map((item) => item.kind)).toEqual(["question"]);
  }, TEST_LIMIT_MS);

  test("turn/start 回包前到达的提问同样交接；--json 输出 waiting 对象", async () => {
    const r = await cxbRun("early-ask", "--json", "提前提问");
    expect(r.code).toBe(5);
    const out = JSON.parse(r.stdout);
    expect(out.ok).toBe(false);
    expect(out.status).toBe("waiting");
    expect(typeof out.threadId).toBe("string");
    expect(out.pending).toHaveLength(1);
    expect(out.pending[0].id).toMatch(/^qu_/);
    expect(out.pending[0].kind).toBe("question");

    const conn = await attach("early-ask");
    await conn.rpc.request("broker/inbox/resolve", { id: out.pending[0].id, answer: ANSWER_B });
    const run = await waitEnded(conn, out.threadId);
    expect(run.turns.at(-1)!.output).toBe("已选择：b.txt");
  }, TEST_LIMIT_MS);

  test("不得双答：同一答案重复提交回 duplicate，不同答案回 alreadyResolved，app-server 只收到一次", async () => {
    const r = await cxbRun("ask", "选一个");
    expect(r.code).toBe(5);
    const conn = await attach("ask");
    const [item] = await pendingItems(conn);
    const firstResolve = await conn.rpc.request<{ duplicate: boolean }>("broker/inbox/resolve", { id: item!.id, answer: ANSWER_B });
    expect(firstResolve.duplicate).toBe(false);
    const again = await conn.rpc.request<{ duplicate: boolean }>("broker/inbox/resolve", { id: item!.id, answer: ANSWER_B });
    expect(again.duplicate).toBe(true);
    const other = await conn.rpc.request("broker/inbox/resolve", { id: item!.id, answer: { answers: { q1: { answers: ["a.txt"] } } } })
      .then(() => null, (error: unknown) => error);
    expect(rpcFailure(other).reason).toBe("alreadyResolved");
    await waitEnded(conn, item!.threadId);
    expect(repliesOf("item/tool/requestUserInput")).toHaveLength(1);
  }, TEST_LIMIT_MS);

  test("撤销：交接后上游 serverRequest/resolved 撤销提问，事项转 cancelled，回答报 cancelled，不发回包", async () => {
    const r = await cxbRun("ask-cancel", "会被撤销的提问");
    expect(r.code).toBe(5);
    const conn = await attach("ask-cancel");
    const [item] = await pendingItems(conn);
    writeFileSync(join(sandbox, "cancel.trigger"), "撤销");
    await until(() => mockLog().some((e) => e.kind === "cancelled"), 10_000, "模拟 codex 没有撤销提问");
    let status = "";
    const deadline = Date.now() + 10_000;
    while (status !== "cancelled") {
      if (Date.now() > deadline) throw new Error(`事项没有转为 cancelled（当前 ${status}）`);
      status = (await pendingItems(conn, true)).find((i) => i.id === item!.id)?.status ?? "";
      if (status !== "cancelled") await Bun.sleep(50);
    }
    const late = await conn.rpc.request("broker/inbox/resolve", { id: item!.id, answer: ANSWER_B })
      .then(() => null, (error: unknown) => error);
    expect(rpcFailure(late).reason).toBe("cancelled");
    await waitEnded(conn, item!.threadId);
    expect(repliesOf("item/tool/requestUserInput")).toHaveLength(0);
  }, TEST_LIMIT_MS);
});

