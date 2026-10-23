import { writeFileSync, renameSync } from "node:fs";
import { fixtureSender, longResumeTurns } from "../../test-support/resume-history";
import { createSubagentMock } from "../../test-support/subagent-mock";
import { createStatusHistory } from "../../test-support/status-history";

// 仅甲路使用的 app-server 桩；所有时序由 fixture/control 显式放行。
const pidFile = process.env.RUN_FIXTURE_PID!;
writeFileSync(`${pidFile}.tmp`, String(process.pid));
renameSync(`${pidFile}.tmp`, pidFile);
const scenario = JSON.parse(process.env.RUN_FIXTURE_OPTIONS ?? "{}");
const calls: Array<{ method: string; params: any }> = [];
const threads = new Map<string, { turns: any[]; goal: any }>();
const reads: Array<{ id: unknown; threadId: string; turnId?: string }> = [];
const reviewTurns = new Set<string>();
let nextThread = 0;
let nextTurn = 0;
let released = false;
let pendingStart: { id: unknown; threadId: string; turn: any } | undefined;
// 只有一个写出队列；状态历史从包装后的发送函数学习本桩发出的轮次与条目。
const rawSend = fixtureSender();
const statusHistory = createStatusHistory(scenario.statusRead, rawSend, longResumeTurns);
const send = statusHistory.wrap(rawSend);
const subagents = createSubagentMock(send, scenario.subagents, scenario.backgroundTerminals);
const reply = (id: unknown, result: unknown) => send({ id, result });
const notify = (method: string, params: unknown) => send({ method, params });
function state(id: string) {
  let value = threads.get(id);
  if (!value) { value = { turns: [], goal: null }; threads.set(id, value); }
  return value;
}
function thread(id: string) {
  return { id, cwd: scenario.threadCwd === null ? undefined : scenario.threadCwd ?? process.cwd(), turns: state(id).turns, status: { type: state(id).turns.at(-1)?.status === "inProgress" ? "active" : "idle" } };
}
/** 结果补读的条目分页一页给完；找不到轮次时回错误。 */
function sendItems(id: unknown, threadId: string, turnId: string) {
  const turn = state(threadId).turns.find((t) => t.id === turnId);
  if (!turn) { send({ id, error: { code: -32600, message: `turn not found: ${turnId}` } }); return; }
  reply(id, { data: turn.items.map((item: unknown) => ({ turnId, item, startedAtMs: null, completedAtMs: null })), nextCursor: null, backwardsCursor: null });
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
  if (reviewTurns.has(turn.id)) turn.items = [{ id: "review-result", type: "exitedReviewMode", review: scenario.output ?? "完整中文审查" }];
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
    if (subagents.handle(msg)) continue;
    if (!msg.method) continue;
    const p = msg.params ?? {};
    calls.push({ method: msg.method, params: p });
    if (statusHistory.handle(msg)) continue;
    switch (msg.method) {
      case "initialize": reply(msg.id, { userAgent: "甲路模拟 Codex" }); break;
      case "initialized": break;
      case "config/read":
        if (scenario.configReadError) send({ id: msg.id, error: { code: scenario.configReadError, message: "模拟读取配置失败" } });
        else reply(msg.id, "configRead" in scenario ? scenario.configRead : { config: { mcp_servers: scenario.mcpServers ?? {} } });
        break;
      case "model/list": reply(msg.id, { data: [{ id: "mock-model", isDefault: true, upgrade: null, supportedReasoningEfforts: [{ reasoningEffort: "high" }] }], nextCursor: null }); break;
      case "thread/start":
      case "thread/resume": {
        const key = "features.default_mode_request_user_input";
        if (scenario.rejectAsk && p.config?.[key]) {
          send({ id: msg.id, error: { code: -32600, message: `unknown configuration field ${key} 秘密不应回显` } });
          break;
        }
        if (scenario.failResumeRetry && msg.method === "thread/resume" && !p.config?.[key]) {
          send({ id: msg.id, error: { code: -32000, message: "模拟恢复重试失败" } });
          break;
        }
        if (scenario.warnAsk) notify("configWarning", { summary: `忽略 ${key}`, details: "秘密不应回显" });
        const id = p.threadId ?? `thread-${++nextThread}`;
        if (id === "missing") { send({ id: msg.id, error: { code: -32000, message: "thread not found" } }); break; }
        reply(msg.id, { thread: { ...thread(id), ...((scenario.longHistory || scenario.statusRead?.longHistory) && msg.method === "thread/resume" ? { turns: p.excludeTurns === true ? [] : longResumeTurns() } : {}) }, model: p.model ?? "mock-model", reasoningEffort: p.config?.model_reasoning_effort ?? "high", cwd: process.cwd(), sandbox: { type: p.sandbox === "read-only" ? "readOnly" : "workspaceWrite" }, approvalPolicy: p.approvalPolicy ?? "never" });
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
        if (p.delivery === "detached" && !scenario.legacyReview) {
          send({ id: msg.id, error: { code: -32600, message: "paginated threads do not support detached review" } });
          break;
        }
        const id = p.delivery === "detached" ? `review-${++nextThread}` : p.threadId;
        if (p.delivery === "detached") notify("thread/started", { thread: thread(id) });
        const turn = begin(id);
        reviewTurns.add(turn.id);
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
      case "thread/items/list":
        // 结果补读的条目分页只在 pendingResult 场景提供，放行前扣住；其余场景仍回 -32601。
        if (!scenario.pendingResult) send({ id: msg.id, error: { code: -32601, message: "模拟方法不存在" } });
        else if (!released) reads.push({ id: msg.id, threadId: p.threadId, turnId: p.turnId });
        else sendItems(msg.id, p.threadId, p.turnId);
        break;
      case "thread/unsubscribe": reply(msg.id, { status: "unsubscribed" }); break;
      case "turn/interrupt": finish(p.threadId, true); reply(msg.id, {}); break;
      case "fixture/state": reply(msg.id, { calls, threads: [...threads.entries()], pendingReads: reads.length, pid: process.pid }); break;
      case "fixture/control": {
        if (p.action === "finish") finish(p.threadId);
        if (p.action === "next") { begin(p.threadId); finish(p.threadId); }
        if (p.action === "release") {
          released = true;
          for (const read of reads.splice(0)) {
            if (read.turnId === undefined) reply(read.id, { thread: thread(read.threadId) });
            else sendItems(read.id, read.threadId, read.turnId);
          }
          if (pendingStart) { reply(pendingStart.id, { turn: pendingStart.turn }); pendingStart = undefined; }
        }
        reply(msg.id, {});
        break;
      }
      default: send({ id: msg.id, error: { code: -32601, message: "模拟方法不存在" } });
    }
  }
});
