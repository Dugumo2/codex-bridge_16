// 私有 app-server 桩：由显式请求推进，完整记录每次回包（包括重复回包）。
import { writeFileSync } from "node:fs";

writeFileSync(process.env.SR_MOCK_PID!, String(process.pid));
let buffer = "";
let sequence = 100;
let running = false;
const asks = new Map<number, string>();
const answers: Array<{ id: number; method: string; result?: unknown; error?: unknown }> = [];
const send = (value: unknown) => process.stdout.write(JSON.stringify(value) + "\n");
const turn = () => ({ id: "轮次", status: running ? "inProgress" : "completed", items: [], error: null });
const watchdog = setTimeout(() => { process.stdin.destroy(); process.exitCode = 1; }, 60_000);
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk: string) => {
  buffer += chunk;
  let end: number;
  while ((end = buffer.indexOf("\n")) >= 0) {
    const message = JSON.parse(buffer.slice(0, end));
    buffer = buffer.slice(end + 1);
    if (message.id === undefined) continue;
    if (message.method === undefined) {
      const method = asks.get(message.id);
      if (method) answers.push({ id: message.id, method, result: message.result, error: message.error });
      continue;
    }
    const params = message.params ?? {};
    let result: unknown = {};
    switch (message.method) {
      case "initialize": result = { userAgent: "乙路模拟服务器" }; break;
      case "thread/resume": result = { thread: { id: params.threadId, turns: [] }, model: "模拟模型" }; break;
      case "thread/goal/get": result = { goal: null }; break;
      case "turn/start":
        running = true;
        send({ method: "turn/started", params: { threadId: params.threadId, turn: turn() } });
        result = { turn: turn() };
        break;
      case "thread/read": result = { thread: { id: params.threadId, status: { type: running ? "active" : "idle" }, turns: [turn()] } }; break;
      case "mock/ask": {
        const id = sequence++;
        asks.set(id, params.method);
        send({ id, method: params.method, params: { threadId: params.threadId, turnId: "轮次", ...params.params } });
        result = { id };
        break;
      }
      case "mock/state": result = { pid: process.pid, answers: [...answers] }; break;
      case "mock/finish":
      case "turn/interrupt":
        running = false;
        send({ method: "turn/completed", params: { threadId: params.threadId, turn: { ...turn(), items: [{ type: "agentMessage", id: "最终", text: "已完成", phase: "final_answer" }] } } });
        break;
    }
    send({ id: message.id, result });
  }
});
process.stdin.on("end", () => { clearTimeout(watchdog); });
process.stdin.on("error", () => { clearTimeout(watchdog); process.exitCode = 1; });
