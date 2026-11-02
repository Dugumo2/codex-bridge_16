/** 静态树日志端到端：真实 broker、可扣放的模拟上游和真实 CLI。 */
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, readFileSync, watch, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BROKER_BIN, createMockCodex, envWithPathPrefix } from "../test-support/broker-harness";
import { brokerVersionOf, connectBroker } from "./broker-conn";
import { readRunLog, readThreadIndex, type LogLine } from "./ledger";
import { show } from "./show";
import { bounded, until } from "./test-fixtures/main-test-support";
import { N4Broker } from "./test-fixtures/n4a-broker";
import { context } from "./test-fixtures/n4a-unit";

const ROOT = "thread-001";

class TreeBroker extends N4Broker {
  override async setup() {
    this.dir = mkdtempSync(join(tmpdir(), "cxb-tree-e2e-"));
    for (const name of ["mock", "ws", "profile", "data", "codex"]) mkdirSync(join(this.dir, name));
    const mockDir = createMockCodex(join(this.dir, "mock"), { subagents: { enabled: true }, sendTurnCompleted: false });
    const registry = join(this.dir, "mock-pids");
    const script = join(mockDir, "codex-mock.ts");
    // 模拟进程启动即登记；在连接与任何等待之前安装观察器。
    writeFileSync(script, readFileSync(script, "utf8").replace('import { appendFileSync } from "node:fs";',
      'import { appendFileSync } from "node:fs";\nappendFileSync(process.env.MOCK_TREE_REGISTRY!, `${process.pid}\\n`);'));
    this.watcher = watch(this.dir, () => {
      try { this.processes.readRegistry(registry); } catch (error) { this.failure = error; }
    });
    this.watcher.on("error", (error) => { this.failure = error; });
    this.env = Object.fromEntries(Object.entries(envWithPathPrefix(mockDir)).filter(([key]) => {
      const name = key.toUpperCase();
      return !["CXB_HOME", "CODEX_HOME", "HOME", "USERPROFILE", "CXB_SERVER"].includes(name)
        && !/SOCKET|ENDPOINT/.test(name) && !name.startsWith("MOCK_");
    }));
    Object.assign(this.env, { CXB_HOME: join(this.dir, "data"), CODEX_HOME: join(this.dir, "codex"),
      HOME: join(this.dir, "profile"), USERPROFILE: join(this.dir, "profile"), CXB_SERVER: "private",
      CXB_BROKER_BIN: BROKER_BIN, MOCK_TREE_REGISTRY: registry });
    this.connection = await connectBroker({ workspace: this.workspace, brokerBin: BROKER_BIN,
      expectedBrokerVersion: await brokerVersionOf(BROKER_BIN), timeoutMs: 15_000, env: this.env,
      log: (line) => this.processes.log(line) });
    this.processes.readRegistry(registry);
  }

  async start() {
    await this.request("thread/start", { cwd: this.workspace.root });
    return this.request<{ runId: string; turn: { id: string } }>("turn/start", {
      threadId: ROOT, input: [{ type: "text", text: "交错子代理日志" }],
    });
  }

  async inject(threadId: string, method: string, params: Record<string, unknown>) {
    return this.request("fixture/inject", { threadId, kind: "notification", payload: { method, params } });
  }

  async spawn(parentThreadId: string, parentTurnId: string, childThreadId: string,
    version: "v1" | "v2", started: "now" | "held" = "now") {
    return this.request<{ turnId: string; holdId: string | null }>("fixture/spawnChild", {
      parentThreadId, parentTurnId, childThreadId, version, started, itemId: `spawn-${childThreadId}`,
    });
  }

  rows(): LogLine[] {
    const root = readThreadIndex(this.workspace.workspaceDir).find((entry) => entry.threadId === ROOT);
    return root ? readRunLog(this.workspace.workspaceDir, root.shortId) ?? [] : [];
  }

  async hasRow(check: (row: LogLine) => boolean) {
    await until(() => this.rows().some(check), 8000, "期待的结构化日志未落盘");
  }

  async stopServer() {
    await this.request("broker/shutdown");
    await bounded(this.connection!.close(), 2000, "关闭 broker 连接超时");
    this.connection = null;
    await this.processes.cleanup();
    expect(this.processes.allExited()).toBe(true);
  }
}

describe.skipIf(process.platform !== "win32")("静态树日志真实 broker", () => {
  let broker: TreeBroker;
  beforeEach(async () => { broker = new TreeBroker(); await broker.setup(); }, 25_000);
  afterEach(async () => { await broker.cleanup(); }, 40_000);

  for (const version of ["v1", "v2"] as const) test(`${version} 父子孙交错、子完成不结束父、复用新轮、关闭后离线读取`, async () => {
    const run = await broker.start();
    const a = await broker.spawn(ROOT, run.turn.id, "child-a", version);
    const b = await broker.spawn(ROOT, run.turn.id, "child-b", version);
    const g = await broker.spawn("child-a", a.turnId, "grandchild", version);
    for (const [thread, turn, text] of [[ROOT, run.turn.id, "根标记"], ["child-b", b.turnId, "乙子标记"],
      ["grandchild", g.turnId, "孙标记"], ["child-a", a.turnId, "甲子标记"]]) {
      await broker.inject(thread!, "item/completed", { turnId: turn, item: { id: `item-${thread}`, type: "agentMessage", text } });
    }
    await broker.inject("child-b", "error", { turnId: b.turnId, willRetry: true,
      error: { message: "请求失败 Authorization: secret-tree-value", codexErrorInfo: "other" } });
    for (const threadId of ["grandchild", "child-a", "child-b"]) await broker.request("fixture/turn", { threadId, action: "complete" });
    await broker.hasRow((row) => row.source?.threadId === "child-b" && row.event === "轮次完成");
    const snapshot = await broker.request("broker/threads/watch", { threadId: ROOT });
    expect(snapshot.run).toMatchObject({ runId: run.runId, state: "running", currentTurnId: run.turn.id });
    expect(snapshot.run.turns).toEqual([]);
    expect(snapshot.run.progress.done.every((item: { itemId: string }) => !["item-child-a", "item-child-b", "item-grandchild"].includes(item.itemId))).toBe(true);
    const reused = await broker.request("fixture/reuseChild", { parentThreadId: ROOT, parentTurnId: run.turn.id,
      itemId: "reuse-a", childThreadId: "child-a", version, mode: version === "v1" ? "v1Input" : "v2Followup" });
    await broker.inject("child-a", "item/completed", { turnId: reused.turnId, item: { id: "item-reused", type: "agentMessage", text: "复用标记" } });
    await broker.hasRow((row) => row.source?.itemId === "item-reused");
    const rows = broker.rows();
    expect(rows.filter((row) => row.source?.itemId?.startsWith("item-")).map((row) => row.source?.threadId))
      .toEqual([ROOT, "child-b", "grandchild", "child-a", "child-a"]);
    expect(rows.find((row) => row.source?.itemId === "item-grandchild")?.source).toMatchObject({
      rootThreadId: ROOT, threadId: "grandchild", parentThreadId: "child-a", sourceRole: "subagent", turnId: g.turnId });
    expect(rows.find((row) => row.source?.itemId === "item-reused")?.source?.turnId).not.toBe(a.turnId);
    expect(rows.filter((row) => row.event === "轮次完成" && row.source?.turnId === a.turnId)).toHaveLength(1);
    expect(rows.find((row) => row.event === "子线程错误")?.content).toContain("willRetry=true");
    expect(rows.filter((row) => row.event === "子线程错误").map((row) => row.content).join("\n")).not.toContain("secret-tree-value");

    // 正常旧命令依然只展示根／review；先过滤再取尾，不能被子代理活动挤掉。
    const old = await broker.cli(["show", ROOT, "--log", "--full", "--json"]);
    expect(old.code).toBe(0);
    const ownItems = JSON.parse(old.stdout).runs.flatMap((r: { items: unknown[] }) => r.items);
    expect(JSON.stringify(ownItems)).toContain("根标记");
    for (const text of ["乙子标记", "孙标记", "甲子标记", "复用标记", "@cxb2"]) expect(JSON.stringify(ownItems)).not.toContain(text);
    await broker.request("fixture/turn", { threadId: "child-a", action: "complete" });
    await broker.inject(ROOT, "turn/completed", { turn: { id: run.turn.id, status: "completed", items: [], error: null } });
    await broker.hasRow((row) => row.event === "轮次完成" && row.source?.threadId === ROOT && row.source.turnId === run.turn.id);
    await broker.stopServer();
    broker.env.CXB_BROKER_BIN = join(broker.dir, "不存在的-broker.exe");
    const tree = await broker.cli(["show", ROOT, "--log", "--tree", "--full", "--json"]);
    expect(tree.code).toBe(0);
    const result = JSON.parse(tree.stdout);
    expect(result).toMatchObject({ tree: true, threadId: ROOT, coverage: { scope: "retained" } });
    expect(result.items.some((item: { threadId: string }) => item.threadId === "grandchild")).toBe(true);
    expect(new Set(result.items.map((item: { eventId: string }) => item.eventId)).size).toBe(result.items.length);
    const child = await broker.cli(["show", "child-a", "--log", "--tree", "--json"]);
    expect(child.code).toBe(1);
    expect(child.stdout + child.stderr).toContain("根线程");
  }, 60_000);

  test("绑定证据晚于子轮完成：只补日志摘要，保留观察顺序且不重驱动父轮", async () => {
    const run = await broker.start();
    const child = await broker.spawn(ROOT, run.turn.id, "late-child", "v1", "held");
    // 血缘仅建立候选；真正派生成功证据仍扣在 spawn 批次内。
    await broker.inject("late-child", "thread/started", { thread: { id: "late-child", parentThreadId: ROOT,
      source: { subAgent: { thread_spawn: { parent_thread_id: ROOT, depth: 1 } } }, status: { type: "idle" },
      canAcceptDirectInput: true, turns: [] } });
    await broker.inject("late-child", "turn/started", { turn: { id: child.turnId, status: "inProgress", items: [] } });
    await broker.inject("late-child", "item/completed", { turnId: child.turnId,
      item: { id: "late-item", type: "agentMessage", text: "先完成后绑定" } });
    await broker.inject("late-child", "turn/completed", { turn: { id: child.turnId, status: "completed", items: [], error: null } });
    // 请求回包作为处理屏障，不用固定睡眠制造时序。
    const before = await broker.request("broker/threads/watch", { threadId: ROOT });
    expect(broker.rows().some((row) => row.source?.itemId === "late-item")).toBe(false);
    await broker.request("fixture/release", { holdId: child.holdId });
    await broker.hasRow((row) => row.source?.itemId === "late-item");
    await broker.hasRow((row) => row.source?.threadId === "late-child" && row.event === "轮次完成");
    const rows = broker.rows();
    expect(rows.find((row) => row.source?.itemId === "late-item")?.source).toMatchObject({ late: true, turnId: child.turnId });
    expect(rows.filter((row) => row.source?.threadId === "late-child" && row.event === "轮次完成")).toHaveLength(1);
    const after = await broker.request("broker/threads/watch", { threadId: ROOT });
    expect(after.run.state).toBe("running");
    expect(after.run.turns).toEqual(before.run.turns);
    expect(after.run.turnsTotal).toBe(before.run.turnsTotal);
    expect(after.run.progress.done.some((item: { itemId: string }) => item.itemId === "late-item")).toBe(false);
    const t = context(broker.workspace.workspaceDir, ["show", ROOT, "--tail", "--tree", "--limit", "100", "--json"]);
    expect(await show(t.ctx)).toBe(0);
    expect(t.connects).toBe(0);
    expect(t.cleanup.size).toBe(0);
    const items = t.json[0].items;
    expect(items.every((item: { ts: string }, i: number) => i === 0 || items[i - 1].ts <= item.ts)).toBe(true);
  }, 60_000);
});
