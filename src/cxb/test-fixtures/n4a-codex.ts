// 甲组真 broker 测试的模拟服务器；通过明确的放行请求结束运行，不靠延迟制造竞态。
import { appendFileSync } from "node:fs";

const registry = process.env.MOCK_N4A_REGISTRY;
if (registry) appendFileSync(registry, `${process.pid}\n`);
const send = (message: unknown) => process.stdout.write(JSON.stringify(message) + "\n");
const threads = new Map<string, any>();
let count = 0;
let active: { threadId: string; turnId: string } | null = null;
const item = { id: "回答", type: "agentMessage", text: "服务器末轮结果", phase: "final_answer" };
const makeThread = (id: string, updatedAt = 1) => ({ id, cwd: process.cwd(), preview: "服务器预览", updatedAt, createdAt: 1,
  modelProvider: "mock", status: { type: "idle" }, source: "vscode", turns: [] });
threads.set("server-only-thread", makeThread("server-only-thread", 9_000_000_000));
function complete() {
  if (!active) return;
  const { threadId, turnId } = active;
  threads.get(threadId).turns = [{ id: turnId, items: [item], status: "completed", itemsView: "full" }];
  send({ method: "item/completed", params: { threadId, turnId, item } });
  send({ method: "turn/completed", params: { threadId, turn: { id: turnId, items: [item], itemsView: "full", status: "completed", error: null } } });
  active = null;
}
let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk: string) => {
  buffer += chunk;
  for (;;) {
    const end = buffer.indexOf("\n");
    if (end < 0) break;
    const line = buffer.slice(0, end); buffer = buffer.slice(end + 1);
    if (!line.trim()) continue;
    const message = JSON.parse(line);
    if (!message.method || message.id === undefined) continue;
    const p = message.params ?? {};
    const reply = (result: unknown) => send({ id: message.id, result });
    switch (message.method) {
      case "initialize": reply({ userAgent: "甲组模拟服务器/0.160.0" }); break;
      case "thread/start": {
        const thread = makeThread(`n4a-thread-${++count}`); threads.set(thread.id, thread);
        reply({ thread, cwd: process.cwd(), model: "本地模型", modelProvider: "mock", approvalPolicy: "never", sandbox: null }); break;
      }
      case "thread/resume": reply({ thread: threads.get(p.threadId), model: "本地模型", modelProvider: "mock", cwd: process.cwd() }); break;
      case "thread/list": reply({ data: [...threads.values()].sort((a, b) => b.updatedAt - a.updatedAt), nextCursor: null }); break;
      case "thread/turns/list": reply({ data: [{ id: "最近轮", status: "completed", items: [], itemsView: "summary" }], nextCursor: null }); break;
      case "thread/items/list": reply({ data: [{ turnId: p.turnId, item }], nextCursor: null }); break;
      case "thread/read": reply({ thread: threads.get(p.threadId) ?? makeThread(p.threadId) }); break;
      case "thread/goal/get": reply({ goal: null }); break;
      case "turn/start":
        active = { threadId: p.threadId, turnId: `轮-${count}` };
        reply({ turn: { id: active.turnId, items: [], status: "inProgress", error: null } });
        send({ method: "turn/started", params: { threadId: p.threadId, turn: { id: active.turnId, items: [], status: "inProgress" } } });
        break;
      case "n4a/release": reply({}); complete(); break;
      case "turn/interrupt": reply({}); complete(); break;
      case "thread/archive": case "thread/delete": threads.delete(p.threadId); reply({}); break;
      case "thread/loaded/list": reply({ data: [...threads.keys()], nextCursor: null }); break;
      case "thread/unsubscribe": reply({ status: "unsubscribed" }); break;
      default: send({ id: message.id, error: { code: -32601, message: "方法尚未实现" } });
    }
  }
});
