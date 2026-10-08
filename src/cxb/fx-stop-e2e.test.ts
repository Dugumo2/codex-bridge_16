/** Windows 真 broker + CLI 入口：后台命令清理与显式放弃树清理。 */
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { brokerVersionOf, connectBroker } from "./broker-conn";
import type { BrokerConnection } from "./contracts";
import { resolveWorkspace } from "./workspace";
import { Processes, bounded, removeTemp } from "./test-fixtures/main-test-support";
import type { BackgroundTerminalsMockOptions, BackgroundTerminalSeed, BackgroundThreadScenario } from "../test-support/background-terminals-mock";

const windows = process.platform === "win32";
const harness = windows ? await import("../test-support/broker-harness") : null;
const PARENT = "thread-e2e";
const CHILD = "child-background";
const PREFIX = "thread/backgroundTerminals/";
const sessions: BackgroundTerminalSeed[] = [{ processId: "21001", itemId: "cmd-a" }, { processId: "21002", itemId: "cmd-b" }];
let dir: string;
let env: NodeJS.ProcessEnv;
let processes: Processes;
let connection: BrokerConnection | undefined;
let broker: ChildProcess | undefined;
let brokerClosed: Promise<void> | undefined;

describe.skipIf(!windows)("fx-stop Windows 后台命令黑盒", () => {
  beforeEach(() => {
    dir = mkdtempSync((process.env.TEMP! + "/cxb-fx-stop-"));
    processes = new Processes();
    connection = undefined;
    broker = undefined;
    brokerClosed = undefined;
    for (const sub of ["mock", "ws", "profile", "codex-home", "cxb-home"]) mkdirSync(join(dir, sub));
    harness!.writeMockCodexScript(join(dir, "mock"), readFileSync(join(import.meta.dir, "test-fixtures/e2e-codex.ts"), "utf8"));
    env = Object.fromEntries(Object.entries(harness!.envWithPathPrefix(join(dir, "mock"))).filter(([key]) =>
      !/^(MOCK_.*|CXB_HOME|CODEX_HOME|HOME|USERPROFILE|CXB_SERVER.*)$/i.test(key)));
    Object.assign(env, { CXB_HOME: join(dir, "cxb-home"), CODEX_HOME: join(dir, "codex-home"),
      HOME: join(dir, "profile"), USERPROFILE: join(dir, "profile"), CXB_SERVER: "private",
      CXB_BROKER_BIN: harness!.BROKER_BIN, TEMP: process.env.TEMP!, TMP: process.env.TEMP!,
      MOCK_SUBAGENTS: JSON.stringify({ enabled: true }), MOCK_STATUS_READ: JSON.stringify({ historyMode: "paged" }),
      MOCK_E2E_SCENARIO: "hold", MOCK_E2E_LOG: join(dir, "mock.log") });
  });
  afterEach(async () => {
    const errors: unknown[] = [];
    try { await connection?.rpc.request("broker/shutdown", {}, { timeoutMs: 5000 }); } catch (error) { errors.push(error); }
    try { await connection?.close(); } catch (error) { errors.push(error); }
    // 测试直接持有 serve 句柄；关闭其 Job 回收模拟服务器，避免长用例中旧 PID 被复用。
    if (broker?.exitCode === null && broker.signalCode === null) broker.kill("SIGKILL");
    try { if (brokerClosed) await bounded(brokerClosed, 10_000, "测试 broker 退出超时"); } catch (error) { errors.push(error); }
    try { await processes.cleanup(); } catch (error) { errors.push(error); }
    if (processes.allExited()) await removeTemp(dir);
    if (errors.length) throw new AggregateError(errors, "后台命令黑盒清理失败");
  }, 30_000);

  async function cli(...args: string[]) {
    const result = await processes.run(process.execPath, [join(import.meta.dir, "main.ts"), ...args, "-v", "-d", join(dir, "ws")],
      { cwd: dir, env, limitMs: 65_000 }).finally(() => processes.pids.clear());
    // run 已按 ChildProcess 句柄等到 close；不在 40 秒后的清理中再次使用这些旧 PID。
    const registry = join(resolveWorkspace(join(dir, "ws"), env).workspaceDir, "broker.json");
    if (existsSync(registry)) expect(JSON.parse(readFileSync(registry, "utf8")).payload.pid).toBe(broker!.pid);
    return { ...result, json: () => JSON.parse(result.stdout.trim().split(/\r?\n/).at(-1)!) };
  }
  async function start(options: BackgroundTerminalsMockOptions = { enabled: true, threads: { [PARENT]: { sessions } } }) {
    env.MOCK_BACKGROUND_TERMINALS = JSON.stringify(options);
    const workspace = resolveWorkspace(join(dir, "ws"), env);
    broker = spawn(harness!.BROKER_BIN, ["serve", "--endpoint", workspace.endpoint, "--cwd", workspace.root, "--idle-timeout", "60000"],
      { cwd: workspace.root, env, windowsHide: true, stdio: ["ignore", "ignore", "ignore"] });
    brokerClosed = new Promise<void>((resolve, reject) => { broker!.once("close", () => resolve()); broker!.once("error", reject); });
    void brokerClosed.catch(() => {});
    await harness!.waitForSocket(workspace.pipePath);
    connection = await connectBroker({ workspace, brokerBin: harness!.BROKER_BIN,
      expectedBrokerVersion: await brokerVersionOf(harness!.BROKER_BIN), timeoutMs: 15_000, env });
    const run = await cli("run", "--detach", "--json", "启动后台命令场景");
    expect(run.code, JSON.stringify(run)).toBe(0);
    const result = run.json();
    expect(result.runId).toMatch(/^run-/);
    return result as { runId: string; threadId: string };
  }
  const rpc = <T = any>(method: string, params: object = {}) => connection!.rpc.request<T>(method, params, { timeoutMs: 10_000 });
  const state = () => rpc("fixture/backgroundTerminals/state");
  const set = (threadId: string, scenario: BackgroundThreadScenario) => rpc("fixture/backgroundTerminals/set", { threadId, scenario });
  async function commandEvents(threadId: string, turnId: string, commands: BackgroundTerminalSeed[]) {
    for (const command of commands) await rpc("fixture/inject", { threadId, kind: "notification", payload: {
      method: "item/started", params: { turnId, item: { id: command.itemId, type: "commandExecution", command: "模拟后台命令",
        cwd: "D:\\", processId: command.processId, status: "inProgress", commandActions: [] } } } });
  }
  async function child() {
    const spawned = await rpc("fixture/spawnChild", { parentThreadId: PARENT, parentTurnId: "turn-001", itemId: "spawn-background",
      childThreadId: CHILD, version: "v2", started: "now" });
    const commands = [{ itemId: "child-command", processId: "31001" }];
    await set(CHILD, { sessions: commands });
    await commandEvents(CHILD, spawned.turnId, commands);
  }
  function order(snapshot: any, threadId: string, ids: string[]) {
    const calls = snapshot.calls.filter((c: any) => c.params.threadId === threadId);
    const firstList = calls.findIndex((c: any) => c.method === PREFIX + "list");
    const lastList = calls.findLastIndex((c: any) => c.method === PREFIX + "list");
    expect(firstList).toBeGreaterThanOrEqual(0);
    for (const processId of ids) {
      const terminated = calls.findIndex((c: any) => c.method === PREFIX + "terminate" && c.params.processId === processId);
      expect(terminated).toBeGreaterThan(firstList);
      expect(terminated).toBeLessThan(lastList);
    }
    expect(calls.some((c: any) => c.method === PREFIX + "clean")).toBe(false);
    expect(snapshot.threads[threadId]).toEqual([]);
  }

  for (const termination of ["success", "exited"] as const) test(`单线程 ${termination}：中断后 list → terminate → list 才确认`, async () => {
    await start({ enabled: true, threads: { [PARENT]: { sessions: sessions.map((s) => ({ ...s, termination })) } } });
    const stopped = await cli("stop", PARENT, "--json");
    expect(stopped.code, JSON.stringify(stopped)).toBe(0);
    // 协议字段按 fx-stop 提交 768f0a4 核对；这里只确认受管命令列表。
    expect(stopped.json()).toMatchObject({ stopOutcome: "confirmed", backgroundCleanup: { scope: "Codex 受管的后台命令",
      state: "confirmed", discovered: 2, remaining: [], terminationResults: { "21001": termination === "success", "21002": termination === "success" } } });
    expect(stopped.json().backgroundCleanup.lastVerifiedAt).toBeNumber();
    const snapshot = await state();
    order(snapshot, PARENT, sessions.map((s) => s.processId));
    expect(snapshot.calls.findIndex((c: any) => c.method === "turn/interrupt")).toBeLessThan(
      snapshot.calls.findIndex((c: any) => c.method === PREFIX + "list"));
    expect(stopped.stderr).toContain("Codex 受管的后台命令");
  }, 90_000);

  test("重复 stop 已结束运行：清理再次出现的已归属会话", async () => {
    const run = await start();
    await commandEvents(PARENT, "turn-001", sessions);
    expect((await cli("stop", PARENT, "--json")).code).toBe(0);
    const before = (await state()).calls.length;
    await set(PARENT, { sessions });
    const again = await cli("stop", PARENT, "--run", run.runId, "--json");
    expect(again.code, JSON.stringify(again)).toBe(0);
    expect(again.json()).toMatchObject({ runId: run.runId, stopOutcome: "confirmed", backgroundCleanup: { state: "confirmed" } });
    const snapshot = await state();
    order({ ...snapshot, calls: snapshot.calls.slice(before) }, PARENT, sessions.map((s) => s.processId));
  }, 120_000);

  for (const fault of ["unsupported", "hang"] as const) test(`${fault}：退出 3 且不把能力不足或未知报 confirmed`, async () => {
    await start({ enabled: true, threads: { [PARENT]: { sessions, fault } } });
    const stopped = await cli("stop", PARENT, "--json");
    expect(stopped.code, JSON.stringify(stopped)).toBe(3);
    expect(stopped.json()).toMatchObject({ ok: false, stopOutcome: "unconfirmed", backgroundCleanup: {
      state: fault === "unsupported" ? "capabilityLimited" : "unknown" } });
    expect(stopped.json().backgroundCleanup.failureReason).toBeString();
    expect(stopped.stderr).toContain("Codex 受管的后台命令");
    expect((await state()).threads[PARENT]).toHaveLength(2);
  }, 90_000);

  test("整树停止：父线程与 v2 子线程的会话全部清空才 confirmed", async () => {
    const run = await start();
    await commandEvents(PARENT, "turn-001", sessions);
    await child();
    const stopped = await cli("stop", PARENT, "--json");
    expect(stopped.code, JSON.stringify(stopped)).toBe(0);
    expect(stopped.json()).toMatchObject({ stopOutcome: "confirmed", childrenStop: { phase: "confirmed",
      root: { backgroundCleanup: { state: "confirmed", remaining: [] } },
      members: { [CHILD]: { backgroundCleanup: { state: "confirmed", remaining: [] } } } } });
    const snapshot = await state();
    order(snapshot, PARENT, sessions.map((s) => s.processId));
    order(snapshot, CHILD, ["31001"]);
    // 已结束的树仍可能出现残留；第二次停止必须重新取得每个成员的清理证据。
    const before = snapshot.calls.length;
    await set(PARENT, { sessions });
    await set(CHILD, { sessions: [{ itemId: "child-command", processId: "31001" }] });
    const repeated = await cli("stop", PARENT, "--run", run.runId, "--json");
    expect(repeated.code, JSON.stringify(repeated)).toBe(0);
    expect(repeated.json()).toMatchObject({ stopOutcome: "confirmed", childrenStop: { phase: "confirmed" } });
    const after = await state();
    order({ ...after, calls: after.calls.slice(before) }, PARENT, sessions.map((s) => s.processId));
    order({ ...after, calls: after.calls.slice(before) }, CHILD, ["31001"]);
  }, 90_000);

  test("树停止 unconfirmed 后 --abandon：保留诊断、解除屏障且可再派单", async () => {
    const run = await start();
    await commandEvents(PARENT, "turn-001", sessions);
    await child();
    await set(CHILD, { sessions: [{ itemId: "child-command", processId: "31001", termination: "remaining" }] });
    const stopped = await cli("stop", PARENT, "--run", run.runId, "--json");
    expect(stopped.code, JSON.stringify(stopped)).toBe(3);
    // 首次等待到期后后台复核仍可处于 pending；对外停止结果必须是 unconfirmed。
    expect(stopped.json()).toMatchObject({ stopOutcome: "unconfirmed", childrenStop: { cleanupState: "active" } });
    expect(stopped.json().childrenStop.phase).not.toBe("confirmed");
    expect(stopped.json().childrenStop.members[CHILD].backgroundCleanup.remaining).toEqual(["31001"]);
    const blocked = await cli("run", "--resume", PARENT, "--json", "屏障期间不能派单");
    expect(blocked.code, JSON.stringify(blocked)).toBe(8);
    const abandoned = await cli("stop", PARENT, "--run", run.runId, "--abandon", "--json");
    expect(abandoned.code, JSON.stringify(abandoned)).toBe(3);
    expect(abandoned.json()).toMatchObject({ stopOutcome: "unconfirmed", actionApplied: true,
      childrenStop: { cleanupState: "abandoned", phase: "unconfirmed" } });
    expect(abandoned.stderr).toContain("可能仍有子代理或命令在运行");
    const watched = await rpc("broker/threads/watch", { threadId: PARENT });
    expect(watched.run).toMatchObject({ runId: run.runId, stopOutcome: "unconfirmed", childrenStop: { cleanupState: "abandoned" } });
    const before = (await state()).calls.length;
    const repeated = await cli("stop", PARENT, "--run", run.runId, "--json");
    expect(repeated.code).toBe(3);
    expect(repeated.json()).toMatchObject({ actionApplied: false, reason: "alreadyAbandoned", childrenStop: { cleanupState: "abandoned" } });
    expect((await state()).calls).toHaveLength(before);
    const resumed = await cli("run", "--resume", PARENT, "--json", "放弃清理后再派单");
    expect(resumed.code, JSON.stringify(resumed)).toBe(0);
    expect(resumed.json().runId).not.toBe(run.runId);
    expect((await state()).threads[CHILD]).toHaveLength(1);
  }, 150_000);
});
