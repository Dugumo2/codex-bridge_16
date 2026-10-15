/** 第二批独占模拟上游；复用既有协议状态机，不修改第一批夹具。 */
import { createSa1bMock, type RpcMessage } from "../../test-support/sa1b-mock";

export type InterruptMode = "interrupted" | "completed" | "failed" | "replyOnly" | "silent";
export function createStallMock(send: (message: RpcMessage) => void, options: { joinedStart?: boolean } = {}) {
  const calls: Array<RpcMessage & { at: number }> = [];
  const emitted: RpcMessage[] = [];
  const timers = new Set<ReturnType<typeof setTimeout>>();
  let mode: InterruptMode = "interrupted", holdReads = false;
  const heldReads: RpcMessage[] = [];
  const output = (message: RpcMessage) => { emitted.push(structuredClone(message)); send(message); };
  const base = createSa1bMock(output);
  const later = (ms: number, body: () => void) => {
    const timer = setTimeout(() => { timers.delete(timer); body(); }, ms); timers.add(timer);
  };
  const state = () => ({ ...base.state(), calls, emitted, heldReads, mode });
  const root = () => base.state().threads.root;
  const active = () => root()?.turns.findLast((turn) => turn.status === "inProgress");
  let serial = 0;
  const command = (name: string, params: any = {}) => base.handle({ id: `stall-internal-${++serial}`, method: `fixture/sa1b/${name}`, params });
  const inject = (message: RpcMessage) => command("inject", { message });
  const error = () => {
    const turn = active(); if (!turn) return;
    inject({ method: "error", params: { threadId: "root", turnId: turn.id, willRetry: true,
      error: { message: "Reconnecting... 1/5", additionalDetails: "本地脚本认证失败",
        codexErrorInfo: { responseStreamDisconnected: { httpStatusCode: 401 } } } } });
  };
  const approval = () => {
    const turn = active(); if (!turn) throw new Error("审批夹具缺少活动轮");
    const id = `stall-approval-${++serial}`;
    inject({ id, method: "item/commandExecution/requestApproval", params: { threadId: "root", turnId: turn.id,
      itemId: id, command: "echo 本地审批", cwd: process.cwd(), reason: "无进展待答暂停测试" } });
    return id;
  };
  function handle(message: RpcMessage) {
    const p = message.params ?? {};
    if (message.method?.startsWith("fixture/stall/")) {
      let result: any = {};
      switch (message.method.slice("fixture/stall/".length)) {
        case "state": result = state(); break;
        case "mode": mode = p.mode; break;
        case "errors": {
          const repeat = () => { if (!active()) return; error(); later(p.everyMs ?? 500, repeat); };
          repeat(); break;
        }
        case "progress": {
          for (let n = 1; n <= (p.count ?? 6); n++) later(n * (p.everyMs ?? 1000), () => {
            const turn = active(); if (turn) inject({ method: "item/agentMessage/delta", params: {
              threadId: "root", turnId: turn.id, itemId: "stall-progress", delta: `进展${n}` } });
          });
          break;
        }
        case "approval": result = { id: approval() }; break;
        case "holdReads": holdReads = true; break;
        case "releaseReads": {
          holdReads = false; const batch = heldReads.splice(0); for (const read of batch) base.handle(read);
          result = { released: batch.length }; break;
        }
        case "finish": command("turn", { threadId: p.threadId ?? "root", action: "complete", status: p.status ?? "completed", output: p.output ?? "本地脚本结果" }); break;
        case "finishAfter": later(p.ms, () => command("turn", { threadId: "root", action: "complete", status: "completed", output: "本地脚本结果" })); break;
        default: throw new Error(`未知 stall 控制：${message.method}`);
      }
      if (message.id !== undefined) send({ id: message.id, result });
      return;
    }
    if (message.method) calls.push({ ...structuredClone(message), at: Date.now() });
    if (options.joinedStart && message.method === "turn/start" && active()) {
      // 回包给出提交编号，但读取揭示真正活动的外部轮；不伪造本客户端的 started/input 证据。
      output({ id: message.id, result: { turn: { id: "submitted-turn", status: "inProgress", items: [], error: null } } }); return;
    }
    if (message.method === "turn/interrupt") {
      if (mode === "silent") return;
      output({ id: message.id, result: {} });
      if (mode !== "replyOnly") command("turn", { threadId: p.threadId, turnId: p.turnId, action: "complete", status: mode,
        ...(mode === "completed" ? { output: "本地脚本结果" } : {}) });
      return;
    }
    // 未确认场景的所有读口都继续返回同一活动轮，绝不伪造结束证据。
    if (holdReads && ["thread/read", "thread/turns/list", "thread/items/list"].includes(message.method ?? "")) {
      heldReads.push(structuredClone(message)); return;
    }
    base.handle(message);
  }
  return { handle, state, command, close() { for (const timer of timers) clearTimeout(timer); timers.clear(); } };
}

if (import.meta.main) {
  const mock = createStallMock((message) => {
    // 内部控制回包不进入 broker 的请求命名空间。
    if (String(message.id ?? "").startsWith("stall-internal-")) return;
    process.stdout.write(`${JSON.stringify(message)}\n`);
  });
  let buffer = "";
  process.stdin.setEncoding("utf8");
  process.stdin.on("data", (chunk) => {
    buffer += chunk; let end: number;
    while ((end = buffer.indexOf("\n")) >= 0) {
      const line = buffer.slice(0, end); buffer = buffer.slice(end + 1);
      if (line.trim()) mock.handle(JSON.parse(line));
    }
  });
  process.stdin.on("end", () => { mock.close(); process.exit(0); });
}
