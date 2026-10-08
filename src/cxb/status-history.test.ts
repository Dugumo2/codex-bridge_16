// H2-a：三份模拟服务器的状态读取扩展（StatusReadMockOptions，接口第 7 节）与接口一致。
// 直接经标准输入输出驱动模拟进程，不经 broker：严格分页、notLoaded 条目为空、活动轮次不进存储页、
// 第一轮前 -32601、legacy、旧服务器、长历史下的全量读超过 10 MiB；未启用时保持原有行为。
import { afterEach, beforeEach, expect, test } from "bun:test";
import { join } from "node:path";
import { readFileSync } from "node:fs";
import { bounded } from "./wait-run";
import { RunHarness, TEST_BUDGET, CLEANUP_BUDGET } from "./test-fixtures/run-harness";
import { createMockCodex, writeMockCodexScript } from "../test-support/broker-harness";
import type { StatusReadMockOptions } from "../test-support/subagent-mock";
import { STATUS_FILLER_ITEMS } from "../test-support/status-history";

type Fixture = "run" | "e2e" | "broker";
const FIXTURES: Fixture[] = ["run", "e2e", "broker"];
const TEN_MIB = 10 * 1024 * 1024;

let h: RunHarness;
beforeEach(() => { h = new RunHarness(); });
afterEach(async () => { await h.cleanup(); }, CLEANUP_BUDGET);

interface Frame { line: string; message: any }

/** 启动一个模拟进程，按 id 收回包；通知按到达顺序保留。 */
function start(fixture: Fixture, statusRead: StatusReadMockOptions | undefined, extra: Record<string, unknown> = {}) {
  if (fixture === "broker") createMockCodex(h.dir, { statusRead });
  if (fixture === "e2e") writeMockCodexScript(h.dir, readFileSync(join(import.meta.dir, "test-fixtures/e2e-codex.ts"), "utf8"));
  const script = fixture === "run" ? join(import.meta.dir, "test-fixtures", "run-app-server.ts")
    : join(h.dir, process.platform === "win32" ? "codex-mock.ts" : "codex");
  const child = h.spawn([process.execPath, script, "app-server"], { ...h.env,
    RUN_FIXTURE_OPTIONS: JSON.stringify({ statusRead, ...extra }),
    MOCK_STATUS_READ: JSON.stringify(statusRead ?? {}), MOCK_E2E_SCENARIO: "complete" });
  const frames: Frame[] = [];
  const waiters: Array<{ check: (frame: Frame) => boolean; resolve: (frame: Frame) => void }> = [];
  let pending = "";
  child.child.stdout!.on("data", (chunk: string) => {
    pending += chunk;
    let end: number;
    while ((end = pending.indexOf("\n")) >= 0) {
      const line = pending.slice(0, end);
      pending = pending.slice(end + 1);
      if (!line.trim()) continue;
      const frame = { line, message: JSON.parse(line) };
      frames.push(frame);
      for (const waiter of waiters.splice(0)) {
        if (waiter.check(frame)) waiter.resolve(frame); else waiters.push(waiter);
      }
    }
  });
  let nextId = 0;
  const wait = (check: (frame: Frame) => boolean, what: string) => {
    const seen = frames.find(check);
    return bounded(seen ? Promise.resolve(seen) : new Promise<Frame>((resolve) => waiters.push({ check, resolve })), 15_000)
      .catch((error) => { throw new Error(`等待 ${what} 失败：${error}\n${child.output().stderr}`); });
  };
  const call = async (method: string, params: unknown = {}): Promise<Frame> => {
    const id = `t-${++nextId}`;
    child.child.stdin!.write(JSON.stringify({ id, method, params }) + "\n");
    return wait(frame => frame.message.id === id && frame.message.method === undefined, method);
  };
  const notified = (method: string, turnId: string) =>
    wait(frame => frame.message.method === method && (frame.message.params?.turn?.id ?? frame.message.params?.turnId) === turnId,
      `${method} ${turnId}`);
  return { call, notified, frames };
}

/** 在夹具上跑一轮并等它完成；返回轮次编号。 */
async function runTurn(fx: ReturnType<typeof start>, fixture: Fixture, threadId: string, text: string): Promise<string> {
  const reply = await fx.call("turn/start", { threadId, input: [{ type: "text", text, text_elements: [] }] });
  const turnId = reply.message.result.turn.id as string;
  if (fixture !== "run") await fx.notified("turn/completed", turnId);
  return turnId;
}

async function thread(fx: ReturnType<typeof start>, _fixture: Fixture): Promise<string> {
  const reply = await fx.call("thread/start", {});
  return reply.message.result.thread.id;
}

for (const fixture of FIXTURES) test(`默认未启用：${fixture} 不接管分页方法，旧行为不变`, async () => {
  const fx = start(fixture, undefined);
  const id = await thread(fx, fixture);
  for (const method of ["thread/turns/list", "thread/items/list"]) {
    const reply = await fx.call(method, { threadId: id, turnId: "turn-001" });
    expect(reply.message.error.code).toBe(-32601);
  }
  const read = await fx.call("thread/read", { threadId: id });
  expect(read.message.result.thread.status.type).toBe("idle");
}, TEST_BUDGET);

for (const fixture of ["run", "e2e"] as const) test(`严格分页：${fixture} 的 turns/list 执行 limit、cursor、方向与 notLoaded`, async () => {
  const fx = start(fixture, { strictPagination: true });
  const id = await thread(fx, fixture);
  const turns: string[] = [];
  for (const text of ["第一问", "第二问", "第三问"]) turns.push(await runTurn(fx, fixture, id, text));
  const page1 = (await fx.call("thread/turns/list", { threadId: id, limit: 2, sortDirection: "desc", itemsView: "notLoaded" })).message.result;
  expect(page1.data.map((t: any) => t.id)).toEqual([turns[2], turns[1]]);
  expect(page1.data.every((t: any) => t.items.length === 0 && t.itemsView === "notLoaded" && t.status === "completed")).toBe(true);
  expect(typeof page1.nextCursor).toBe("string");
  const page2 = (await fx.call("thread/turns/list", { threadId: id, cursor: page1.nextCursor, limit: 2, sortDirection: "desc", itemsView: "notLoaded" })).message.result;
  expect(page2.data.map((t: any) => t.id)).toEqual([turns[0]]);
  expect(page2.nextCursor).toBeNull();
  const asc = (await fx.call("thread/turns/list", { threadId: id, limit: 100, sortDirection: "asc", itemsView: "full" })).message.result;
  expect(asc.data.map((t: any) => t.id)).toEqual(turns);
  expect(asc.data[0].items.some((item: any) => item.type === "userMessage" && item.content[0].text === "第一问")).toBe(true);
  expect(asc.nextCursor).toBeNull();
  const bad = await fx.call("thread/turns/list", { threadId: id, cursor: "不存在", limit: 2 });
  expect(bad.message.error.code).toBe(-32600);
}, TEST_BUDGET);

for (const fixture of ["run", "e2e"] as const) test(`严格分页：${fixture} 的 items/list 按 turnId 分页，关键条目在后续页`, async () => {
  const fx = start(fixture, { strictPagination: true });
  const id = await thread(fx, fixture);
  const first = await runTurn(fx, fixture, id, "第一问");
  const second = await runTurn(fx, fixture, id, "第二问");
  const params = { threadId: id, turnId: second, limit: 64, sortDirection: "asc" };
  const page1 = (await fx.call("thread/items/list", params)).message.result;
  expect(page1.data).toHaveLength(STATUS_FILLER_ITEMS);
  expect(page1.data.every((entry: any) => entry.turnId === second && entry.item.type === "reasoning")).toBe(true);
  expect(typeof page1.nextCursor).toBe("string");
  const page2 = (await fx.call("thread/items/list", { ...params, cursor: page1.nextCursor })).message.result;
  expect(page2.nextCursor).toBeNull();
  const types = page2.data.map((entry: any) => entry.item.type);
  expect(types[0]).toBe("userMessage");
  expect(page2.data[0].item.content[0].text).toBe("第二问");
  expect(types).toContain("agentMessage");
  expect(page2.data.every((entry: any) => entry.turnId === second)).toBe(true);
  const other = (await fx.call("thread/items/list", { ...params, turnId: first, cursor: page1.nextCursor })).message.result;
  expect(other.data[0].item.content[0].text).toBe("第一问");
  const missing = await fx.call("thread/items/list", { ...params, turnId: "不存在" });
  expect(missing.message.error.code).toBe(-32600);
}, TEST_BUDGET);

test("活动轮次不进存储页：元数据为 active，存储页不含该轮，完成后出现", async () => {
  const fx = start("run", { strictPagination: true, omitActiveTurns: true }, { hold: true });
  const id = await thread(fx, "run");
  const done = await runTurn(fx, "run", id, "第一问");
  await fx.call("fixture/control", { action: "finish", threadId: id });
  const active = await runTurn(fx, "run", id, "第二问");
  const read = (await fx.call("thread/read", { threadId: id })).message.result.thread;
  expect(read.status.type).toBe("active");
  expect(read.turns).toEqual([]);
  const page = (await fx.call("thread/turns/list", { threadId: id, limit: 100, sortDirection: "desc", itemsView: "notLoaded" })).message.result;
  expect(page.data.map((t: any) => t.id)).toEqual([done]);
  await fx.call("fixture/control", { action: "finish", threadId: id });
  const after = (await fx.call("thread/turns/list", { threadId: id, limit: 100, sortDirection: "desc", itemsView: "notLoaded" })).message.result;
  expect(after.data.map((t: any) => [t.id, t.status])).toEqual([[active, "completed"], [done, "completed"]]);
  expect((await fx.call("thread/read", { threadId: id })).message.result.thread.status.type).toBe("idle");
}, TEST_BUDGET);

test("第一轮前：turns/list 回 -32601，有已存轮次后恢复", async () => {
  const fx = start("run", { strictPagination: true, omitActiveTurns: true, historyMode: "beforeFirstTurn" }, { hold: true });
  const id = await thread(fx, "run");
  const params = { threadId: id, limit: 100, sortDirection: "desc", itemsView: "notLoaded" };
  const before = await fx.call("thread/turns/list", params);
  expect(before.message.error).toEqual({ code: -32601, message: "list_turns is not supported yet" });
  const turn = await runTurn(fx, "run", id, "第一问");
  // 首轮还在跑、尚未持久化：仍不支持。
  expect((await fx.call("thread/turns/list", params)).message.error.code).toBe(-32601);
  await fx.call("fixture/control", { action: "finish", threadId: id });
  const after = (await fx.call("thread/turns/list", params)).message.result;
  expect(after.data.map((t: any) => t.id)).toEqual([turn]);
}, TEST_BUDGET);

for (const fixture of FIXTURES) test(`legacy 与旧服务器：${fixture} 的能力分开降级`, async () => {
  for (const historyMode of ["legacy", "unsupported"] as const) {
    const fx = start(fixture, { historyMode });
    const id = await thread(fx, fixture);
    const turn = await runTurn(fx, fixture, id, "第一问");
    const turns = await fx.call("thread/turns/list", { threadId: id, sortDirection: "desc", itemsView: "notLoaded" });
    const items = await fx.call("thread/items/list", { threadId: id, turnId: turn });
    expect(items.message.error.code).toBe(-32601);
    if (historyMode === "legacy") expect(turns.message.result.data.map((t: any) => t.id)).toEqual([turn]);
    else expect(turns.message.error.code).toBe(-32601);
    // 全量读仍可用（旧调用点的兼容路径）。
    const full = (await fx.call("thread/read", { threadId: id, includeTurns: true })).message.result.thread;
    expect(full.turns.map((t: any) => t.id)).toEqual([turn]);
    await h.cleanup();
    h = new RunHarness();
  }
}, TEST_BUDGET * 2);

for (const fixture of FIXTURES) test(`长历史：${fixture} 的全量读超过 10 MiB，元数据读与分页读保持小`, async () => {
  const fx = start(fixture, { strictPagination: true, longHistory: true });
  const id = await thread(fx, fixture);
  const turn = await runTurn(fx, fixture, id, "最新一问");
  const full = await fx.call("thread/read", { threadId: id, includeTurns: true });
  expect(Buffer.byteLength(full.line)).toBeGreaterThan(TEN_MIB);
  expect(full.message.result.thread.turns.at(-1).id).toBe(turn);
  const meta = await fx.call("thread/read", { threadId: id });
  expect(Buffer.byteLength(meta.line)).toBeLessThan(4096);
  expect(meta.message.result.thread.turns).toEqual([]);
  const page = await fx.call("thread/turns/list", { threadId: id, limit: 100, sortDirection: "desc", itemsView: "notLoaded" });
  expect(Buffer.byteLength(page.line)).toBeLessThan(64 * 1024);
  expect(page.message.result.data).toHaveLength(100);
  expect(page.message.result.data[0].id).toBe(turn);
  expect(typeof page.message.result.nextCursor).toBe("string");
  const resume = await fx.call("thread/resume", { threadId: id });
  expect(Buffer.byteLength(resume.line)).toBeGreaterThan(TEN_MIB);
  const excluded = await fx.call("thread/resume", { threadId: id, excludeTurns: true });
  expect(Buffer.byteLength(excluded.line)).toBeLessThan(4096);
  // 单一写出队列：大回包与其后的小消息各自成行，不交错。
  expect(fx.frames.every(frame => frame.message !== null && typeof frame.message === "object")).toBe(true);
}, TEST_BUDGET * 2);
