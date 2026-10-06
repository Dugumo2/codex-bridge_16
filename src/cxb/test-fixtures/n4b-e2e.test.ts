import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, watch, type FSWatcher } from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, join } from "node:path";
import { BROKER_BIN, writeMockCodexScript } from "../../test-support/broker-harness";
import { brokerVersionOf, connectBroker } from "../broker-conn";
import type { BrokerConnection } from "../contracts";
import { resolveWorkspace } from "../workspace";
import { bounded, Processes, removeTemp, until } from "./main-test-support";

let sandbox: string;
let processes: Processes;
let connection: BrokerConnection | undefined;
let watcher: FSWatcher | undefined;
let watchErrors: unknown[];
let scenario: string;

function env(): NodeJS.ProcessEnv {
  let inheritedPath = "";
  const clean: NodeJS.ProcessEnv = {};
  for (const [key, value] of Object.entries(process.env)) {
    const upper = key.toUpperCase();
    if (upper === "PATH") { inheritedPath = value ?? ""; continue; }
    if (["CXB_HOME", "CODEX_HOME", "USERPROFILE", "HOME", "CXB_SERVER"].includes(upper)
      || /(?:CXB|CODEX|COLLAB).*(?:SOCKET|ENDPOINT)/.test(upper) || upper.startsWith("MOCK_")) continue;
    clean[key] = value;
  }
  return { ...clean, PATH: `${join(sandbox, "mock")}${delimiter}${inheritedPath}`,
    CXB_HOME: join(sandbox, "data"), CODEX_HOME: join(sandbox, "codex"), USERPROFILE: join(sandbox, "profile"),
    HOME: join(sandbox, "profile"), CXB_SERVER: "private", CXB_BROKER_BIN: BROKER_BIN,
    MOCK_E2E_SCENARIO: scenario, MOCK_E2E_LOG: join(sandbox, "mock.log") };
}

function entries(): any[] {
  const file = join(sandbox, "mock.log");
  if (!existsSync(file)) return [];
  // 写入中的最后一行留待下次通知读取。
  return readFileSync(file, "utf8").split("\n").slice(0, -1).filter(Boolean).map((line) => JSON.parse(line));
}

beforeEach(() => {
  sandbox = mkdtempSync(join(tmpdir(), "cxb-n4b-e2e-"));
  processes = new Processes(); watchErrors = []; connection = undefined; scenario = "ask";
  for (const name of ["mock", "data", "codex", "profile", "ws"]) mkdirSync(join(sandbox, name));
  writeMockCodexScript(join(sandbox, "mock"), readFileSync(join(import.meta.dir, "e2e-codex.ts"), "utf8"));
  watcher = watch(sandbox, () => {
    try { for (const entry of entries()) if (entry.kind === "boot") processes.add(entry.pid); }
    catch (error) { watchErrors.push(error); }
  });
  watcher.on("error", (error) => watchErrors.push(error));
});

afterEach(async () => {
  const errors: unknown[] = [...watchErrors];
  try { if (connection) await bounded(connection.rpc.request("broker/shutdown", {}, { timeoutMs: 5000 }), 6000, "关闭 broker 超时"); } catch (e) { errors.push(e); }
  try { if (connection) await bounded(connection.close(), 2000, "关闭连接超时"); } catch (e) { errors.push(e); }
  try { watcher?.close(); } catch (e) { errors.push(e); }
  try { await processes.cleanup(); } catch (e) { errors.push(e); }
  try {
    if (!processes.allExited()) throw new Error("仍有进程存活，保留临时目录");
    await removeTemp(sandbox);
    expect(existsSync(sandbox)).toBe(false);
  } catch (e) { errors.push(e); }
  if (errors.length) throw new AggregateError(errors, "乙组真 broker 用例清理失败");
}, 40000);

async function start(mode: string) {
  scenario = mode;
  connection = await connectBroker({ workspace: resolveWorkspace(join(sandbox, "ws"), env()), brokerBin: BROKER_BIN,
    expectedBrokerVersion: await brokerVersionOf(BROKER_BIN), timeoutMs: 15000, env: env(),
    log: (line) => processes.log(line) });
  // 初始化返回时模拟 app-server 已启动；登记通知外再核对一次，绝不等到清理才发现句柄。
  for (const entry of entries()) if (entry.kind === "boot") processes.add(entry.pid);
}

function cli(...args: string[]) {
  return processes.run(process.execPath, [join(import.meta.dir, "..", "main.ts"), ...args, "-d", join(sandbox, "ws")],
    { cwd: sandbox, env: env(), limitMs: 50000, onStderr: (chunk) => processes.log(chunk) });
}

describe("乙组真 broker", () => {
  test("托管提问出现在 inbox；reply 交付后上游恰好收到准确答案", async () => {
    await start("ask");
    const running = await cli("run", "测试提问", "--detach", "--json");
    expect(running.code).toBe(0);
    const handle = JSON.parse(running.stdout);
    const pending = await connection!.rpc.request<any>("broker/inbox/wait", { timeoutMs: 10000 }, { timeoutMs: 15000 });
    expect(pending.event).toBe("added");
    const listed = await cli("inbox", "--json");
    expect(listed.code).toBe(0);
    const rows = JSON.parse(listed.stdout).items;
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ kind: "question", status: "pending", threadId: handle.threadId, shortId: handle.shortId });
    expect(rows[0].questions).toEqual([{ id: "q1", text: "选哪个文件？", options: ["a.txt", "b.txt"] }]);
    const answered = await cli("reply", rows[0].id, "b.txt", "--json");
    expect(answered.code).toBe(0);
    expect(JSON.parse(answered.stdout)).toMatchObject({ accepted: true, duplicate: false });
    await until(() => entries().some((e) => e.kind === "reply"), 10000, "上游未收到回复");
    const delivered = entries().filter((e) => e.kind === "reply");
    expect(delivered).toHaveLength(1);
    expect(delivered[0].result).toEqual({ answers: { q1: { answers: ["b.txt"] } } });
  }, 90000);

  test("detach 后 wait 读取结果，主入口四命令接线有效", async () => {
    await start("complete");
    const running = await cli("run", "测试后台结果", "--detach", "--json");
    expect(running.code).toBe(0);
    const handle = JSON.parse(running.stdout);
    const waited = await cli("wait", handle.shortId, "--timeout", "20", "--json");
    expect(waited.code).toBe(0);
    expect(JSON.parse(waited.stdout)).toMatchObject({ command: "wait", waitFor: "thread", threadId: handle.threadId,
      runId: handle.runId, status: "completed", result: "续跑完成" });
  }, 90000);

  test("stop 停止托管线程并返回确认，不直接持有上游运行", async () => {
    await start("hold");
    const running = await cli("run", "测试停止", "--detach", "--json");
    expect(running.code).toBe(0);
    const handle = JSON.parse(running.stdout);
    const stopped = await cli("stop", handle.shortId, "--clear", "--json");
    expect(stopped.code).toBe(0);
    expect(JSON.parse(stopped.stdout)).toMatchObject({ ok: true, command: "stop", threadId: handle.threadId,
      runId: handle.runId, stopOutcome: "confirmed", interruptSent: true });
    expect(entries().filter((e) => e.method === "turn/interrupt")).toHaveLength(1);
  }, 90000);
});
