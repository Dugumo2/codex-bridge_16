import { writeFileSync, renameSync } from "node:fs";

// 仅甲路使用的 app-server 桩；所有时序由 fixture/control 显式放行。
const pidFile = process.env.RUN_FIXTURE_PID!;
writeFileSync(`${pidFile}.tmp`, String(process.pid));
renameSync(`${pidFile}.tmp`, pidFile);
const scenario = JSON.parse(process.env.RUN_FIXTURE_OPTIONS ?? "{}");
const calls: Array<{ method: string; params: any }> = [];
const threads = new Map<string, { turns: any[]; goal: any }>();
const reads: Array<{ id: unknown; threadId: string }> = [];
let nextThread = 0;
let nextTurn = 0;
let released = false;
let pendingStart: { id: unknown; threadId: string; turn: any } | undefined;
const send = (value: unknown) => process.stdout.write(JSON.stringify(value) + "\n");
const reply = (id: unknown, result: unknown) => send({ id, result });
const notify = (method: string, params: unknown) => send({ method, params });
function state(id: string) {
  let value = threads.get(id);
  if (!value) { value = { turns: [], goal: null }; threads.set(id, value); }
  return value;
}
function thread(id: string) {
  return { id, cwd: process.cwd(), turns: state(id).turns, status: { type: state(id).turns.at(-1)?.status === "inProgress" ? "active" : "idle" } };
}
function begin(id: string) {
  const turn = { id: `turn-${++nextTurn}`, status: "inProgress", items: [], itemsView: "full", error: null };
  state(id).turns.push(turn);
  notify("turn/started", { threadId: id, turn });
  return turn;
}
function finish(id: string, interrupted = false) {
  const turn = state(id).turns.at(-1);
  if (!turn || turn.status !== "inProgress") return;
  turn.status = interrupted ? "interrupted" : scenario.fail ? "failed" : "completed";
  turn.error = scenario.fail ? { message: "模拟 Codex 失败" } : null;
  turn.items = [{ id: `answer-${turn.id}`, type: "agentMessage", phase: "final_answer", text: scenario.output ?? "完整中文结果" }];
  if (id.startsWith("review-")) turn.items = [{ id: "review-result", type: "exitedReviewMode", review: scenario.output ?? "完整中文审查" }];
  notify("turn/completed", { threadId: id, turn: scenario.pendingResult && !released ? { ...turn, items: [], itemsView: "notLoaded" } : turn });
}
process.stdin.setEncoding("utf8");
let input = "";
process.stdin.on("data", (chunk) => {
  input += chunk;
  let end: number;
  while ((end = input.indexOf("\n")) >= 0) {
    const line = input.slice(0, end);
    input = input.slice(end + 1);
    if (!line.trim()) continue;
    const msg = JSON.parse(line);
    if (!msg.method) continue;
    const p = msg.params ?? {};
    calls.push({ method: msg.method, params: p });
    switch (msg.method) {
      case "initialize": reply(msg.id, { userAgent: "甲路模拟 Codex" }); break;
      case "initialized": break;
      case "model/list": reply(msg.id, { data: [{ id: "mock-model", isDefault: true, upgrade: null, supportedReasoningEfforts: [{ reasoningEffort: "high" }] }], nextCursor: null }); break;
      case "thread/start":
      case "thread/resume": {
        const key = "features.default_mode_request_user_input";
        if (scenario.rejectAsk && p.config?.[key]) {
          send({ id: msg.id, error: { code: -32600, message: `unknown configuration field ${key} 秘密不应回显` } });
          break;
        }
        if (scenario.warnAsk) notify("configWarning", { summary: `忽略 ${key}`, details: "秘密不应回显" });
        const id = p.threadId ?? `thread-${++nextThread}`;
        if (id === "missing") { send({ id: msg.id, error: { code: -32000, message: "thread not found" } }); break; }
        reply(msg.id, { thread: thread(id), model: p.model ?? "mock-model", reasoningEffort: p.config?.model_reasoning_effort ?? "high", cwd: process.cwd() });
        break;
      }
      case "thread/memoryMode/set": reply(msg.id, {}); break;
      case "fixture/resumeOverrides": reply(msg.id, { threadId: p.threadId }); break;
      case "thread/goal/get": reply(msg.id, { goal: state(p.threadId).goal }); break;
      case "thread/goal/set": {
        const old = state(p.threadId).goal;
        const goal = { objective: p.objective ?? old?.objective ?? "测试目标", status: p.status ?? "active", tokenBudget: p.tokenBudget ?? null, tokensUsed: 0, createdAt: old?.createdAt ?? 1 };
        state(p.threadId).goal = goal;
        notify("thread/goal/updated", { threadId: p.threadId, goal });
        reply(msg.id, { goal });
        break;
      }
      case "turn/start": {
        const turn = begin(p.threadId);
        if (scenario.earlyQuestion) {
          pendingStart = { id: msg.id, threadId: p.threadId, turn };
          send({ id: "question-1", method: "item/tool/requestUserInput", params: { threadId: p.threadId, turnId: turn.id, isBlocking: false,
            questions: [{ id: "choice", header: "选择", question: "请选择文件名", options: [{ label: "甲" }, { label: "乙" }] }] } });
          break;
        }
        reply(msg.id, { turn });
        if (!scenario.hold) finish(p.threadId);
        break;
      }
      case "review/start": {
        const id = `review-${++nextThread}`;
        const turn = begin(id);
        reply(msg.id, { turn, reviewThreadId: id });
        if (scenario.reviewQuestion) send({ id: "review-question", method: "item/tool/requestUserInput", params: {
          threadId: id, turnId: turn.id, isBlocking: false, questions: [{ id: "choice", question: "审查范围由用户确认" }] } });
        else if (!scenario.hold) finish(id);
        break;
      }
      case "thread/read":
        if (scenario.pendingResult && !released && p.includeTurns) reads.push({ id: msg.id, threadId: p.threadId });
        else reply(msg.id, { thread: thread(p.threadId) });
        break;
      case "thread/unsubscribe": reply(msg.id, { status: "unsubscribed" }); break;
      case "turn/interrupt": finish(p.threadId, true); reply(msg.id, {}); break;
      case "fixture/state": reply(msg.id, { calls, threads: [...threads.entries()], pendingReads: reads.length, pid: process.pid }); break;
      case "fixture/control": {
        if (p.action === "finish") finish(p.threadId);
        if (p.action === "next") { begin(p.threadId); finish(p.threadId); }
        if (p.action === "release") {
          released = true;
          for (const read of reads.splice(0)) reply(read.id, { thread: thread(read.threadId) });
          if (pendingStart) { reply(pendingStart.id, { turn: pendingStart.turn }); pendingStart = undefined; }
        }
        reply(msg.id, {});
        break;
      }
      default: send({ id: msg.id, error: { code: -32601, message: "模拟方法不存在" } });
    }
  }
});
