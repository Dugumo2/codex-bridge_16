// 真实子进程夹具：固定模式控制失败，运行目录记录启动次数和实际 bun PID。
import { appendFileSync, writeFileSync } from "node:fs";

const mode = process.env.MOCK_PAGING_MODE ?? process.argv[2] ?? "normal";
appendFileSync("attempts.log", `${process.pid}\n`);
const send = (message: unknown) => process.stdout.write(JSON.stringify(message) + "\n");
const pause = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

if (mode === "early-exit" || mode === "unopenable" || mode === "locked") {
  const message = process.argv[3] ?? "启动失败：中文 stderr 尾巴\n";
  // 刻意跨写块切开错误签名，客户端不得擅自插入换行或 trim。
  const marker = message.indexOf("database");
  const cut = marker >= 0 ? marker + 8 : 3;
  process.stderr.write(message.slice(0, cut));
  await pause(15);
  process.stderr.write(message.slice(cut));
  await pause(15);
  process.exit(1);
}

let pending = "";
const decoder = new TextDecoder();
let initializeParams: unknown;
const calls: Array<{ method: string; params: any }> = [];
const turns = new Map<string, any[]>();
let turnNumber = 0;

/** 分页夹具与协议 0.160.0 一致：条目外层有 turnId 与两个时间戳。 */
function pageItems(turnId: string, items: any[], cursor: number, limit: number) {
  const end = Math.min(cursor + limit, items.length);
  return {
    data: items.slice(cursor, end).map((item) => ({ turnId, item, startedAtMs: null, completedAtMs: null })),
    nextCursor: end < items.length ? `条目游标:${end}` : null,
    backwardsCursor: end > cursor ? `反向游标:${cursor}` : null,
  };
}
for await (const chunk of process.stdin) {
  pending += decoder.decode(chunk, { stream: true });
  let newline: number;
  while ((newline = pending.indexOf("\n")) >= 0) {
    const line = pending.slice(0, newline).trim();
    pending = pending.slice(newline + 1);
    if (!line) continue;
    const message = JSON.parse(line);
    if ("jsonrpc" in message) throw new Error("不应携带 jsonrpc 字段");
    if (mode.startsWith("paging-") && message.method) {
      calls.push({ method: message.method, params: message.params });
    }
    switch (message.method) {
      case "initialize":
        initializeParams = message.params;
        if (mode === "init-error") {
          process.stderr.write("initialize 被拒绝\n");
          send({ id: message.id, error: { code: -32000, message: "拒绝握手", data: { mode } } });
        } else if (mode !== "hang-init") {
          send({ id: message.id, result: { userAgent: "fake-app-server" } });
        }
        break;
      case "initialized":
        if (mode.startsWith("paging-")) break;
        send({ method: "fixture/ready", params: { initialized: true } });
        send({ id: "approval-1", method: "fixture/approve", params: { command: "echo" } });
        break;
      case "echo":
        send({ id: message.id, result: message.params ?? null });
        break;
      case "initializeParams":
        send({ id: message.id, result: initializeParams });
        break;
      case "pid":
        send({ id: message.id, result: process.pid });
        break;
      case "fixture/state":
        send({ id: message.id, result: { calls, pid: process.pid } });
        break;
      case "thread/start":
      case "thread/resume":
        send({ id: message.id, result: { thread: { id: message.params?.threadId ?? "分页线程", turns: [] }, model: "mock" } });
        break;
      case "thread/goal/get":
        send({ id: message.id, result: { goal: null } });
        break;
      case "thread/list":
      case "thread/loaded/list":
        // 分页结果测试没有子代理；仍提供真实发现协议的明确空集合。
        send({ id: message.id, result: { data: [], nextCursor: null } });
        break;
      case "thread/unsubscribe":
        send({ id: message.id, result: {} });
        break;
      case "turn/start": {
        const turnId = `分页轮-${++turnNumber}`;
        const threadId = message.params.threadId;
        const items = [
          { id: "答一", type: "agentMessage", phase: "final_answer", text: "分页一 中文" },
          ...Array.from({ length: 128 }, (_, i) => ({ id: `命令-${i}`, type: "commandExecution", command: "只读检查", status: "completed" })),
          { id: "答二", type: "agentMessage", phase: "final_answer", text: "分页二 中文" },
        ];
        turns.set(turnId, items);
        send({ id: message.id, result: { turn: { id: turnId, status: "inProgress", items: [] } } });
        send({ method: "turn/started", params: { threadId, turn: { id: turnId, status: "inProgress", items: [] } } });
        send({ method: "turn/completed", params: { threadId, turn: { id: turnId, status: "completed", error: null, itemsView: "summary", items: [items.at(-1)] } } });
        break;
      }
      case "thread/turns/list": {
        const itemsView = message.params.itemsView ?? "summary";
        const all = [...turns].map(([id, items]) => ({
          id, status: "completed", error: null, itemsView,
          items: itemsView === "full" ? items : itemsView === "summary" ? items.filter((item) => item.type === "agentMessage").slice(-1) : [],
          startedAt: null, completedAt: null, durationMs: null,
        }));
        if (message.params.sortDirection !== "asc") all.reverse();
        const offset = Number(String(message.params.cursor ?? "0").replace("轮次游标:", ""));
        const end = Math.min(offset + (message.params.limit ?? 20), all.length);
        send({ id: message.id, result: { data: all.slice(offset, end), nextCursor: end < all.length ? `轮次游标:${end}` : null, backwardsCursor: end > offset ? `轮次游标:${offset}` : null } });
        break;
      }
      case "thread/items/list": {
        if (mode === "paging-unsupported" || !mode.startsWith("paging-")) {
          send({ id: message.id, error: { code: -32601, message: "thread/items/list 尚未实现" } });
          break;
        }
        const { turnId, cursor, limit = 100 } = message.params;
        if (mode === "paging-fail-middle" && cursor) {
          send({ id: message.id, error: { code: -32000, message: "分页读取中途失败" } });
          break;
        }
        if (mode === "paging-huge") {
          send({ id: message.id, result: pageItems(turnId, [{ id: "超大条目", type: "agentMessage", text: "大".repeat(4 * 1024 * 1024) }], 0, 1) });
          break;
        }
        const offset = Number(String(cursor ?? "0").replace("条目游标:", ""));
        if (!turns.has(turnId) || !Number.isSafeInteger(offset) || offset < 0) {
          send({ id: message.id, error: { code: -32600, message: "轮次或分页游标无效" } });
          break;
        }
        send({ id: message.id, result: pageItems(turnId, turns.get(turnId)!, offset, Math.min(limit, 64)) });
        break;
      }
      case "thread/read":
        send({ id: message.id, result: { thread: { id: message.params.threadId, turns: [...turns].map(([id, items]) => ({ id, status: "completed", error: null, itemsView: "full", items })) } } });
        break;
      case "exit":
        process.exit(7);
        break;
      case "overflow":
        process.stdout.write("x".repeat(10 * 1024 * 1024 + 1));
        break;
      default:
        if (message.id === "approval-1" && ("result" in message || "error" in message)) {
          send({ method: "fixture/answered", params: message });
        }
    }
  }
}
writeFileSync("stdin-closed.log", String(process.pid));
// 正常关闭必须让管道 EOF 和真正的进程退出都可观察。
process.exit(0);
