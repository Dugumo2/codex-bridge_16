import { afterEach, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import { createSa1bMock, type RpcMessage } from "../test-support/sa1b-mock";

/** 模拟上游的会话文件按实测形状写：冷恢复后 raw 关闭，但会话文件照写，thread/read 给出 path。 */
const dirs: string[] = [];
afterEach(() => { for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true }); });

function harness(rolloutDir: string, snapshot?: any) {
  const sent: RpcMessage[] = [];
  const mock = createSa1bMock(m => sent.push(m), { rolloutDir, ...(snapshot ? { snapshot } : {}) });
  let serial = 0;
  const call = (method: string, params: any = {}) => {
    const id = `${method}-${++serial}`; mock.handle({ id, method, params });
    const response = sent.find(m => m.id === id);
    expect(response?.error, method).toBeUndefined(); return response?.result;
  };
  const lines = (thread: string) => readFileSync(call("thread/read", { threadId: thread }).thread.path, "utf8")
    .split("\n").filter(Boolean).map(line => JSON.parse(line));
  return { mock, sent, call, lines };
}

/** 行所在的轮：之前最近一条 task_started。 */
function turnOf(lines: any[], index: number): string | null {
  for (let i = index; i >= 0; i--) {
    const line = lines[i];
    if (line.type === "event_msg" && line.payload.type === "task_started") return line.payload.turn_id;
  }
  return null;
}

test("会话文件：path 合规，每行有时间戳、类型与载荷", () => {
  const dir = mkdtempSync(join(tmpdir(), "sa1b-rollout-")); dirs.push(dir);
  const { call, lines } = harness(dir);
  const t = call("turn/start", { threadId: "root", input: [] }).turn.id;
  const path = call("thread/read", { threadId: "root" }).thread.path as string;
  expect(isAbsolute(path)).toBe(true);
  expect(path.endsWith(".jsonl")).toBe(true);
  expect(path).toContain("rollout-");
  expect(path.split(/[\\/]/).at(-1)).toContain("root");
  const all = lines("root");
  for (const line of all) {
    expect(line.timestamp).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
    expect(typeof line.type).toBe("string");
    expect(line.payload).toBeDefined();
  }
  expect(all[0].type).toBe("session_meta");
  expect(all.some(line => line.type === "turn_context" && line.payload.turn_id === t)).toBe(true);
});

test("冷恢复后 raw 关闭：父文件记工具调用，子文件把消费记录记在消费它的轮", () => {
  const dir = mkdtempSync(join(tmpdir(), "sa1b-rollout-")); dirs.push(dir);
  const first = harness(dir);
  const t1 = first.call("turn/start", { threadId: "root", input: [] }).turn.id;
  const spawned = first.call("fixture/sa1b/spawn", { parentThreadId: "root", parentTurnId: t1, childThreadId: "kid", version: "v2" }).turnId;
  first.call("fixture/sa1b/turn", { threadId: "kid", turnId: spawned, action: "complete", output: "首轮结果" });
  first.call("fixture/sa1b/turn", { threadId: "root", turnId: t1, action: "complete" });
  const pathBefore = first.call("thread/read", { threadId: "kid" }).thread.path;

  // 重启：同一个会话目录，快照恢复；raw 关闭。
  const second = harness(dir, first.mock.snapshot());
  const resumed = second.call("thread/resume", { threadId: "root" });
  expect(resumed.thread.path).toBe(first.call("thread/read", { threadId: "root" }).thread.path);
  expect(second.call("thread/read", { threadId: "kid" }).thread.path).toBe(pathBefore);
  const t2 = second.call("turn/start", { threadId: "root", input: [] }).turn.id;
  const reused = second.call("fixture/sa1b/reuse", { parentThreadId: "root", parentTurnId: t2, childThreadId: "kid", message: "继续" });
  expect(second.sent.some(m => m.method === "rawResponseItem/completed")).toBe(false);
  const interacted = second.sent.find(m => m.method === "item/started" && m.params.item.kind === "interacted")!.params.item;
  const parent = second.lines("root");
  const call = parent.findIndex(line => line.type === "response_item" && line.payload.type === "function_call");
  expect(parent[call].payload).toMatchObject({ name: "followup_task", namespace: "collaboration", call_id: interacted.id });
  const args = JSON.parse(parent[call].payload.arguments);
  expect(args.target).toBe("agent_kid");
  const child = second.lines("kid");
  const delivery = child.findIndex(line => line.type === "response_item" && line.payload.type === "agent_message");
  expect(turnOf(child, delivery)).toBe(reused.turnId);
  expect(child[delivery].payload.content[1].encrypted_content).toBe(args.message);
  expect(child[delivery].payload.content[0].text.startsWith("Message Type: NEW_TASK\n")).toBe(true);
  // 调用先写入父文件（工具执行前），消费记录不早于调用。
  expect(Date.parse(child[delivery].timestamp)).toBeGreaterThanOrEqual(Date.parse(parent[call].timestamp));
});

test("send_message 给空闲子代理：当时不记录，下一轮开始才以 MESSAGE 记在那一轮", () => {
  const dir = mkdtempSync(join(tmpdir(), "sa1b-rollout-")); dirs.push(dir);
  const { call, lines } = harness(dir);
  const t = call("turn/start", { threadId: "root", input: [] }).turn.id;
  const first = call("fixture/sa1b/spawn", { parentThreadId: "root", parentTurnId: t, childThreadId: "kid", version: "v2" }).turnId;
  call("fixture/sa1b/turn", { threadId: "kid", turnId: first, action: "complete" });
  call("fixture/sa1b/reuse", { parentThreadId: "root", parentTurnId: t, childThreadId: "kid", tool: "send_message", message: "顺便" });
  const parent = lines("root");
  expect(parent.some(line => line.payload?.type === "function_call" && line.payload.name === "send_message")).toBe(true);
  expect(lines("kid").some(line => line.payload?.type === "agent_message")).toBe(false);
  const next = call("fixture/sa1b/turn", { threadId: "kid", action: "start" }).turnId;
  const child = lines("kid");
  const delivery = child.findIndex(line => line.payload?.type === "agent_message");
  expect(turnOf(child, delivery)).toBe(next);
  expect(child[delivery].payload.content[0].text.startsWith("Message Type: MESSAGE\n")).toBe(true);
});

test("新派子代理的会话文件开头抄有父线程历史，之后才是它自己的轮", () => {
  const dir = mkdtempSync(join(tmpdir(), "sa1b-rollout-")); dirs.push(dir);
  const { call, lines } = harness(dir);
  const old = call("turn/start", { threadId: "root", input: [] }).turn.id;
  call("fixture/sa1b/turn", { threadId: "root", turnId: old, action: "complete" });
  const t = call("turn/start", { threadId: "root", input: [] }).turn.id;
  const own = call("fixture/sa1b/spawn", { parentThreadId: "root", parentTurnId: t, childThreadId: "kid", version: "v2" }).turnId;
  const child = lines("kid");
  const copied = child.findIndex(line => line.payload?.type === "task_started" && line.payload.turn_id === old);
  const mine = child.findIndex(line => line.payload?.type === "task_started" && line.payload.turn_id === own);
  expect(copied).toBeGreaterThan(0);
  expect(mine).toBeGreaterThan(copied);
  expect(child.some(line => line.payload?.type === "task_complete" && line.payload.turn_id === old)).toBe(true);
});
