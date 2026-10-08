/**
 * H2-c 黑盒：共享模式下，长历史线程（3072 个已完成轮次，全量读回包超过 10 MiB）上
 * - 吸收判定经 Settlement 组合查询（元数据 + notLoaded 轮次分页 + 候选条目分页）定案；
 * - joined 核实经 Turn 查询翻页找到目标，终态交结果补读（条目分页）补齐；
 * - `show --server` 只读最新一轮的 notLoaded 元数据与条目分页；
 * 全程不发 `thread/read{includeTurns:true}`，broker 不因超限退出。双连接：owner 与旁观连接共用同一个 broker。
 *
 * 共享服务器是本文件内的进程内模拟（Bun.serve 挂在 AF_UNIX 套接字上，所有 WebSocket 连接共用同一份线程状态），
 * 「桌面端」的轮次由用例直接驱动。broker 只支持 Windows：其他平台整组跳过。
 */
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BROKER_BIN, createMockCodex, envWithPathPrefix } from "../test-support/broker-harness";
import { brokerVersionOf, connectBroker } from "./broker-conn";
import type { BrokerConnection } from "./contracts";
import { show } from "./show";
import { bounded, Processes, removeTemp } from "./test-fixtures/main-test-support";
import { context } from "./test-fixtures/n4a-unit";
import { resolveWorkspace } from "./workspace";

const RUNNABLE = process.platform === "win32";
const THREAD = "长历史线程";
const HISTORY_TURNS = 3072;
/** 每轮条目前置的推理条目数，等于条目页大小，使关键条目必然落在第二页以后。 */
const FILLER_ITEMS = 64;

type Item = Record<string, unknown>;
interface MockTurn { id: string; status: "inProgress" | "completed"; items: Item[] }
type Plan = { kind: "own" } | { kind: "absorb"; into: string; complete: boolean } | { kind: "hold" };

/** 有界轮询：条件可以是异步的（经 broker 往返查询）；每次间隔 50 毫秒。 */
async function until(check: () => boolean | Promise<boolean>, ms: number, message: string): Promise<void> {
  const deadline = performance.now() + ms;
  while (!(await check())) {
    if (performance.now() >= deadline) throw new Error(message);
    await Bun.sleep(50);
  }
}

const user = (text: string): Item => ({ type: "userMessage", id: `u-${text}`, content: [{ type: "text", text, text_elements: [] }] });
const agent = (text: string): Item => ({ type: "agentMessage", id: `a-${text}`, text, phase: "final_answer" });

/** 共享的分页服务器：严格执行 limit、cursor（以轮次编号锚定）、方向与 itemsView；全量读返回超过 10 MiB。 */
function servePagedShared(socketPath: string) {
  const log: Array<{ method: string; params: any }> = [];
  const threads = new Map<string, MockTurn[]>();
  const sockets = new Set<{ send(data: string): unknown }>();
  let seq = 0;
  let plan: Plan = { kind: "own" };
  let heldReply: (() => void) | null = null;
  let heldTurn: string | null = null;

  const turnsOf = (threadId: string) => {
    let turns = threads.get(threadId);
    if (!turns) {
      turns = Array.from({ length: HISTORY_TURNS }, (_, i) => ({
        id: `历史-${i}`, status: "completed" as const, items: [user(`历史输入${i}`), agent(`历史回答${i}`)] }));
      threads.set(threadId, turns);
    }
    return turns;
  };
  const find = (threadId: string, turnId: string) => turnsOf(threadId).find((turn) => turn.id === turnId);
  const broadcast = (method: string, params: unknown) => {
    const line = JSON.stringify({ method, params });
    for (const ws of sockets) { try { ws.send(line); } catch { /* 连接已断开 */ } }
  };
  const status = (threadId: string) => ({ type: turnsOf(threadId).some((turn) => turn.status === "inProgress") ? "active" : "idle" });
  const view = (turn: MockTurn, itemsView: string) => itemsView === "notLoaded"
    ? { id: turn.id, status: turn.status, items: [], itemsView: "notLoaded", error: null }
    : { id: turn.id, status: turn.status, items: turn.items, itemsView: "full", error: null };
  const notLoaded = (turn: MockTurn) => view(turn, "notLoaded");

  function page<T>(list: T[], cursor: unknown, limit: number, key: (value: T, index: number) => string) {
    let start = 0;
    if (cursor !== undefined && cursor !== null) {
      start = typeof cursor === "string" ? list.findIndex((value, index) => key(value, index) === cursor) : -1;
      if (start < 0) return null;
    }
    const data = list.slice(start, start + limit);
    const next = start + limit < list.length ? key(list[start + limit]!, start + limit) : null;
    return { data, nextCursor: next, backwardsCursor: null };
  }

  function handle(send: (message: unknown) => void, msg: any) {
    const p = msg.params ?? {};
    if (msg.method !== "initialize") log.push({ method: msg.method, params: p });
    const reply = (result: unknown) => send({ id: msg.id, result });
    const fail = (code: number, message: string) => send({ id: msg.id, error: { code, message } });
    switch (msg.method) {
      case "initialize":
        reply({ userAgent: "h2c-paged-shared" });
        break;
      case "thread/resume":
        // 恢复历史的大小不是本路的验收点，始终不带历史。
        reply({ thread: { id: p.threadId, preview: "", cwd: "/work", status: status(p.threadId), turns: [] }, model: "mock-model", cwd: "/work" });
        break;
      case "thread/read": {
        const thread = { id: p.threadId, preview: "", cwd: "/work", status: status(p.threadId), turns: [] as unknown[] };
        // 全量读：每轮附带约 6 KiB 的条目，合计超过 10 MiB（broker 收到即按超限退出）。
        if (p.includeTurns === true) thread.turns = turnsOf(p.threadId).map((turn) => ({ ...view(turn, "full"), items: [...turn.items, agent("填".repeat(2048))] }));
        reply({ thread });
        break;
      }
      case "thread/turns/list": {
        const turns = turnsOf(p.threadId);
        const ordered = p.sortDirection === "asc" ? turns : [...turns].reverse();
        const limit = Math.min(100, Math.max(1, typeof p.limit === "number" ? p.limit : 25));
        const result = page(ordered, p.cursor, limit, (turn) => `轮次:${turn.id}`);
        if (!result) { fail(-32600, "invalid cursor"); break; }
        const itemsView = typeof p.itemsView === "string" ? p.itemsView : "summary";
        reply({ ...result, data: result.data.map((turn) => view(turn, itemsView)) });
        break;
      }
      case "thread/items/list": {
        const turn = find(p.threadId, p.turnId);
        if (!turn) { fail(-32600, `turn not found: ${p.turnId}`); break; }
        const filler = Array.from({ length: FILLER_ITEMS }, (_, i) => ({ type: "reasoning", id: `推理-${turn.id}-${i}`, summary: [], content: [] }));
        const items = [...filler, ...turn.items];
        const ordered = p.sortDirection === "desc" ? [...items].reverse() : items;
        const limit = Math.min(64, Math.max(1, typeof p.limit === "number" ? p.limit : 64));
        const result = page(ordered, p.cursor, limit, (_, index) => `条目:${index}`);
        if (!result) { fail(-32600, "invalid cursor"); break; }
        reply({ ...result, data: result.data.map((item) => ({ turnId: turn.id, item, startedAtMs: null, completedAtMs: null })) });
        break;
      }
      case "turn/start": {
        const text = String(p.input?.[0]?.text ?? "");
        const current = plan;
        plan = { kind: "own" };
        if (current.kind === "absorb") {
          // 被正在运行的他轮吸收：输入成为他轮的又一条用户消息；回包给出一个从不宣布、也不入存储的轮次。
          const theirs = find(p.threadId, current.into)!;
          theirs.items.push(user(text));
          if (current.complete) {
            theirs.items.push(agent("他轮结论"));
            theirs.status = "completed";
            broadcast("turn/completed", { threadId: p.threadId, turn: notLoaded(theirs) });
          }
          reply({ turn: { id: `幽灵-${++seq}`, status: "inProgress", items: [], error: null } });
          break;
        }
        const turn: MockTurn = { id: `本轮-${++seq}`, status: "inProgress", items: [user(text)] };
        turnsOf(p.threadId).push(turn);
        const answer = () => {
          reply({ turn: { id: turn.id, status: "inProgress", items: [], error: null } });
          broadcast("turn/started", { threadId: p.threadId, turn: notLoaded(turn) });
        };
        if (current.kind === "hold") { heldReply = answer; heldTurn = turn.id; } else answer();
        break;
      }
      case "thread/goal/get":
        reply({ goal: null });
        break;
      case "thread/unsubscribe":
        reply({ status: "unsubscribed" });
        break;
      case "turn/interrupt":
        reply({});
        break;
      default:
        fail(-32601, `Method not found: ${msg.method}`);
    }
  }

  const server = Bun.serve({
    unix: socketPath,
    fetch(req, srv) {
      if (srv.upgrade(req)) return undefined as unknown as Response;
      return new Response("not a websocket", { status: 400 });
    },
    websocket: {
      open(ws) { sockets.add(ws); },
      message(ws, raw) {
        for (const line of String(raw).split("\n")) {
          if (!line.trim()) continue;
          let msg: any;
          try { msg = JSON.parse(line); } catch { continue; }
          if (msg.id === undefined || msg.method === undefined) continue;
          handle((message) => ws.send(JSON.stringify(message)), msg);
        }
      },
      close(ws) { sockets.delete(ws); },
    },
  });

  return {
    log,
    setPlan(next: Plan) { plan = next; },
    get heldTurn() { return heldTurn; },
    release() { const answer = heldReply; heldReply = null; answer?.(); },
    /** 桌面端（不经 broker）开一轮。 */
    desktopStart(threadId: string, text: string) {
      const turn: MockTurn = { id: `他轮-${++seq}`, status: "inProgress", items: [user(text)] };
      turnsOf(threadId).push(turn);
      broadcast("turn/started", { threadId, turn: notLoaded(turn) });
      return turn.id;
    },
    /** 以给定结论完成一轮（完成通知不带条目，结果须经条目分页补读）。 */
    complete(threadId: string, turnId: string, text: string) {
      const turn = find(threadId, turnId)!;
      turn.items.push(agent(text));
      turn.status = "completed";
      broadcast("turn/completed", { threadId, turn: notLoaded(turn) });
    },
    flood(threadId: string, method: string, count: number) {
      for (let i = 0; i < count; i++) broadcast(method, { threadId, turnId: null, seq: i });
    },
    stop() { server.stop(true); },
  };
}

describe.skipIf(!RUNNABLE)("H2-c 黑盒：共享模式长历史下的吸收判定、joined 核实与 show --server", () => {
  let dir = "";
  let server: ReturnType<typeof servePagedShared> | null = null;
  let env: NodeJS.ProcessEnv = {};
  let connections: BrokerConnection[] = [];
  let processes = new Processes();

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "cxb-h2c-"));
    for (const name of ["mock", "ws", "profile", "data", "codex"]) mkdirSync(join(dir, name));
    const socket = join(dir, "cx.sock");
    server = servePagedShared(socket);
    // 共享档不会拉起 codex；PATH 上照常放一个模拟，与其他共享黑盒一致。
    createMockCodex(join(dir, "mock"));
    env = Object.fromEntries(Object.entries(envWithPathPrefix(join(dir, "mock"))).filter(([key]) => {
      const name = key.toUpperCase();
      return !["CXB_HOME", "CODEX_HOME", "HOME", "USERPROFILE", "CXB_SERVER"].includes(name)
        && !/SOCKET|ENDPOINT/.test(name) && !name.startsWith("MOCK_");
    }));
    Object.assign(env, { CXB_HOME: join(dir, "data"), CODEX_HOME: join(dir, "codex"), USERPROFILE: join(dir, "profile"),
      HOME: join(dir, "profile"), CXB_SERVER: "shared", CXB_SERVER_SOCKET: socket, CXB_BROKER_BIN: BROKER_BIN });
    connections = [];
    processes = new Processes();
  });

  afterEach(async () => {
    const errors: unknown[] = [];
    for (const connection of connections) {
      try { await bounded(connection.rpc.request("broker/shutdown", {}, { timeoutMs: 5000 }), 6000, "关闭 broker 超时"); break; }
      catch { /* 该连接已关闭，换下一个 */ }
    }
    for (const connection of connections) {
      try { await bounded(connection.close(), 2000, "关闭连接超时"); } catch (error) { errors.push(error); }
    }
    connections = [];
    try { server?.stop(); } catch (error) { errors.push(error); }
    server = null;
    try { await processes.cleanup(); } catch (error) { errors.push(error); }
    try { await removeTemp(dir); } catch (error) { errors.push(error); }
    if (errors.length) throw new AggregateError(errors, "H2-c 黑盒清理失败");
  }, 40_000);

  const workspace = () => resolveWorkspace(join(dir, "ws"), env);
  async function connect(): Promise<BrokerConnection> {
    const connection = await connectBroker({ workspace: workspace(), brokerBin: BROKER_BIN,
      expectedBrokerVersion: await brokerVersionOf(BROKER_BIN), timeoutMs: 15_000, env, log: (line) => processes.log(line) });
    connections.push(connection);
    return connection;
  }
  const request = <T = any>(connection: BrokerConnection, method: string, params: unknown = {}, ms = 10_000) =>
    bounded(connection.rpc.request<T>(method, params, { timeoutMs: ms }), ms + 1000, `请求超时：${method}`);
  const mock = () => server!;
  /** 全量读一律不得出现；轮次页一律 notLoaded。 */
  function assertBounded() {
    expect(mock().log.filter((e) => e.method === "thread/read" && e.params?.includeTurns === true)).toEqual([]);
    expect(mock().log.filter((e) => e.method === "thread/turns/list").every((e) => e.params.itemsView === "notLoaded")).toBe(true);
  }
  /** broker 仍在服务（没有因超限退出）。 */
  async function alive(connection: BrokerConnection) {
    expect(await request(connection, "broker/threads/list")).toBeDefined();
  }
  async function watchRun(connection: BrokerConnection) {
    return (await request<{ run: any }>(connection, "broker/threads/watch", { threadId: THREAD })).run;
  }

  test("吸收判定：本次输入是已完成他轮的第二条用户消息（摘要看不到），经条目分页认出载体按 joined 定案", async () => {
    const theirs = mock().desktopStart(THREAD, "别人的输入");
    mock().setPlan({ kind: "absorb", into: theirs, complete: true });
    const owner = await connect();
    const observer = await connect();
    const reply = await request<any>(owner, "turn/start", { threadId: THREAD, input: [{ type: "text", text: "本次输入" }] }, 20_000);
    expect(reply.absorbedBy).toBe(theirs);
    // 线程空闲 → 逐个候选读条目：第一页全是推理条目，两条用户消息都在后续页。
    const pages = mock().log.filter((e) => e.method === "thread/items/list" && e.params.turnId === theirs);
    expect(pages.length).toBeGreaterThanOrEqual(2);
    expect(pages[0]!.params).toMatchObject({ threadId: THREAD, cursor: null, limit: 64, sortDirection: "asc" });
    assertBounded();
    await until(async () => (await watchRun(observer))?.state === "ended", 10_000, "加入的运行没有结束");
    expect((await watchRun(observer)).joined).toBe(true);
    await alive(observer);
  }, 60_000);

  test("吸收判定：线程活动时完整遍历长历史轮次页，取升序第一个活动轮次按 joined 定案", async () => {
    const theirs = mock().desktopStart(THREAD, "别人的输入");
    mock().setPlan({ kind: "absorb", into: theirs, complete: false });
    const owner = await connect();
    const observer = await connect();
    const reply = await request<any>(owner, "turn/start", { threadId: THREAD, input: [{ type: "text", text: "本次输入" }] }, 20_000);
    expect(reply.absorbedBy).toBe(theirs);
    const pages = mock().log.filter((e) => e.method === "thread/turns/list");
    // 3073 轮、每页 100 轮：活动集合要遍历到末页。
    expect(pages.length).toBeGreaterThanOrEqual(31);
    expect(pages.every((e) => e.params.limit === 100 && e.params.sortDirection === "desc")).toBe(true);
    assertBounded();
    mock().complete(THREAD, theirs, "他轮结论");
    await until(async () => (await watchRun(observer))?.state === "ended", 10_000, "加入的运行没有结束");
    await alive(owner);
  }, 60_000);

  test("joined 核实：溢出丢失目标完成后翻到第二页找到终态，结果经条目分页补齐", async () => {
    mock().setPlan({ kind: "hold" });
    const owner = await connect();
    const observer = await connect();
    const start = request<any>(owner, "turn/start", { threadId: THREAD, input: [{ type: "text", text: "本次输入" }] }, 30_000);
    await until(() => mock().heldTurn !== null, 10_000, "启动请求没有到达服务器");
    const target = mock().heldTurn!;
    // 回包前缓冲超过硬上限 → 溢出；目标的完成因生命周期名额用尽而未保存，只能定案后核实。
    mock().flood(THREAD, "turn/future", 4001);
    // 目标之后又有 150 个他轮：目标在倒序第二页。
    for (let i = 0; i < 150; i++) {
      const other = mock().desktopStart(THREAD, `他轮输入${i}`);
      mock().complete(THREAD, other, `他轮结论${i}`);
    }
    mock().complete(THREAD, target, "目标结论");
    mock().release();
    const reply = await start;
    expect(reply.absorbedBy).toBe(target);
    await until(async () => {
      const run = await watchRun(observer);
      return run?.state === "ended" && run.turns.at(-1)?.ready !== "pending";
    }, 20_000, "joined 运行没有结束或结果没有补齐");
    const run = await watchRun(observer);
    expect(run.joined).toBe(true);
    expect(run.turns.at(-1)).toMatchObject({ turnId: target, ready: "ready", output: "目标结论" });
    const turnPages = mock().log.filter((e) => e.method === "thread/turns/list");
    expect(turnPages.length).toBeGreaterThanOrEqual(2);
    expect(turnPages[1]!.params.cursor).toEqual(expect.any(String));
    expect(mock().log.some((e) => e.method === "thread/items/list" && e.params.turnId === target)).toBe(true);
    assertBounded();
    await alive(owner);
  }, 90_000);

  test("show --server：长历史线程只读最新一轮的 notLoaded 元数据与条目分页", async () => {
    const latest = mock().desktopStart(THREAD, "最新输入");
    mock().complete(THREAD, latest, "最新结论");
    const owner = await connect();
    const t = context(workspace().workspaceDir, ["show", THREAD, "--server", "--json"]);
    t.ctx.workspace = workspace();
    // show 结束时会关闭会话连接；给它单独的连接，owner 留给断言与清理。
    t.ctx.connect = async () => await connect();
    expect(await bounded(show(t.ctx), 30_000, "show --server 超时")).toBe(0);
    expect(t.json[0]).toMatchObject({ mode: "result", result: "最新结论", outputSource: "finalAnswer", status: "completed" });
    const first = mock().log.find((e) => e.method === "thread/turns/list")!;
    expect(first.params).toEqual({ threadId: THREAD, cursor: null, limit: 1, sortDirection: "desc", itemsView: "notLoaded" });
    expect(mock().log.filter((e) => e.method === "thread/items/list" && e.params.turnId === latest).length).toBeGreaterThanOrEqual(2);
    assertBounded();
    await alive(owner);
  }, 60_000);
});
