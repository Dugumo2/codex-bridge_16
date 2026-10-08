// N2 端到端测试用的模拟 codex app-server（经 broker-harness 的 writeMockCodexScript 落成 codex.cmd 调用的脚本）。
// 场景由环境变量选择（broker 经 launch 继承 CLI 的环境）：
//   MOCK_E2E_SCENARIO：ask（开轮后提问，答后完成）、approval-ask-approval（审批 → 提问 → 审批 → 完成）、
//                      early-ask（turn/start 回包前就提问）、ask-cancel（提问后等触发文件，发 serverRequest/resolved 撤销再完成）、
//                      hold（本进程第一轮一直运行到 turn/interrupt，之后各轮立即完成）、complete（每轮立即完成）、
//                      ask-big（提问，答后以远大于 64 KiB 的中文结果完成）；thread/archive、thread/delete 回成功，thread/list 回空；
//                      review/start 默认 inline；显式 MOCK_E2E_LEGACY_REVIEW 才允许 detached，在审查线程上先后提问两次，答完以审查结论完成；
//   MOCK_E2E_LOG：每条收到的请求、对服务端请求的回包各追加一行 JSON，供用例断言载荷与「恰好一次」；启动时记一行 boot（含 pid）；
//   MOCK_E2E_TRIGGER：ask-cancel 场景里出现该文件即撤销提问；
//   MOCK_E2E_BUSY：该文件存在时 thread/read 报告一个 inProgress 的 turn（模拟 broker 重启后核实不通过），元数据状态为 active；
//                  未启用 MOCK_STATUS_READ 时 thread/turns/list 也只给出这一轮（broker 的状态读取层先读元数据、再分页）。
//   结果补读的条目分页 thread/items/list 只在 -pending 场景与 MOCK_E2E_REVIEW_INNER_TURN=1 时提供（一页给完已完成轮次的条目，
//                     -pending 时与 thread/read 一样扣到 fixture/releaseResult）；其余场景仍回 -32601。
//   MOCK_E2E_LONG_HISTORY=1：独立开关，仅恢复未排除历史时回约 12 MiB 历史，不改变场景选择。
//   MOCK_STATUS_READ：状态读取的分页与历史选项（JSON，StatusReadMockOptions），默认关闭；开启后由
//                     test-support/status-history.ts 接管 thread/read、thread/turns/list、thread/items/list。
// 服务端请求的回包到达前，本轮一直保持运行（不像 harness 默认 mock 那样到点就完成）。
// MOCK_BACKGROUND_TERMINALS：独立后台命令场景（JSON），默认关闭时三个接口均回 -32601。
import { appendFileSync, existsSync, readFileSync } from "node:fs";
// CXB_INLINE_RESUME_HISTORY_BEGIN
import { fixtureSender, longResumeTurns } from "../../test-support/resume-history";
// CXB_INLINE_RESUME_HISTORY_END
// CXB_INLINE_SUBAGENT_MOCK_BEGIN
import { createSubagentMock } from "../../test-support/subagent-mock";
// CXB_INLINE_SUBAGENT_MOCK_END
// CXB_INLINE_STATUS_HISTORY_BEGIN
import { createStatusHistory } from "../../test-support/status-history";
// CXB_INLINE_STATUS_HISTORY_END

const args = process.argv.slice(2);
if (args[0] !== "app-server") {
  process.stderr.write("模拟 codex：只支持 app-server 子命令\n");
  process.exit(1);
}
const selectedScenario = process.env.MOCK_E2E_SCENARIO ?? "ask";
const delayResult = selectedScenario.endsWith("-pending");
const scenario = selectedScenario.replace(/-pending$/, "");
const logPath = process.env.MOCK_E2E_LOG;
const trigger = process.env.MOCK_E2E_TRIGGER;
const busyFlag = process.env.MOCK_E2E_BUSY;
const gateDir = process.env.MOCK_E2E_GATE_DIR;

function log(entry: unknown): void {
  if (logPath) appendFileSync(logPath, JSON.stringify(entry) + "\n");
}
log({ kind: "boot", pid: process.pid });
const statusReadOptions = JSON.parse(process.env.MOCK_STATUS_READ ?? "{}");
// 只有一个写出队列；状态历史从包装后的发送函数学习本模拟发出的轮次与条目。
const rawSend = fixtureSender();
const statusHistory = createStatusHistory(statusReadOptions, rawSend, longResumeTurns);
const send = statusHistory.wrap(rawSend);
const subagents = createSubagentMock(send, JSON.parse(process.env.MOCK_SUBAGENTS ?? "{}"), JSON.parse(process.env.MOCK_BACKGROUND_TERMINALS ?? "{}"));

const waiting = new Map<string, (message: { result?: unknown; error?: unknown }) => void>();
let serverRequestCounter = 0;
const interruptedTurns = new Set<string>();
const holds = new Map<string, () => void>();
const completedTurns = new Map<string, unknown[]>();
const heldReads: Array<{ id: unknown; threadId: string }> = [];
const heldItemReads: Array<{ id: unknown; threadId: string; turnId: string }> = [];
let resultsReleased = false;
/** 只有完成通知不带完整条目的场景才需要结果补读的条目分页。 */
const itemPaging = delayResult || process.env.MOCK_E2E_REVIEW_INNER_TURN === "1";
const heldLifecycle: Array<{ id: unknown; method: string; threadId: string }> = [];

function busy(): boolean {
  return !!busyFlag && existsSync(busyFlag);
}

/** 已完成轮次的条目，按 thread/items/list 的包装一页给完；找不到轮次时回错误。 */
function sendItems(id: unknown, threadId: string, turnId: string): void {
  const turn = (completedTurns.get(threadId) ?? []).find((t) => (t as { id?: string }).id === turnId) as { items?: unknown[] } | undefined;
  if (!turn) {
    send({ id, error: { code: -32600, message: `turn not found: ${turnId}` } });
    return;
  }
  send({ id, result: { data: (turn.items ?? []).map((item) => ({ turnId, item, startedAtMs: null, completedAtMs: null })),
    nextCursor: null, backwardsCursor: null } });
}

function gate(name: string): boolean {
  return !gateDir || existsSync(`${gateDir}/${name}`);
}

function deleteMode(): string | undefined {
  const path = process.env.MOCK_E2E_SERVER_DELETE_MODE_FILE;
  if (path && existsSync(path)) return readFileSync(path, "utf8").trim();
  return process.env.MOCK_E2E_SERVER_DELETE_MODE;
}

function holdLifecycle(method: string): boolean {
  if (!gateDir) return false;
  if (method === "thread/archive" && process.env.MOCK_E2E_HOLD_ARCHIVE === "1" && !gate("archive.release")) return true;
  if (method === "thread/delete" && process.env.MOCK_E2E_HOLD_DELETE === "1" && !gate("delete.release")) return true;
  return false;
}

function releaseLifecycle(): void {
  for (const pending of heldLifecycle.splice(0)) {
    const releaseName = pending.method === "thread/delete" ? "delete.release" : "archive.release";
    if (!gate(releaseName)) {
      heldLifecycle.push(pending);
      continue;
    }
    if (deleteMode() === "unsupported" && pending.method === "thread/delete") {
      send({ id: pending.id, error: { code: -32601, message: "Method not found: thread/delete" } });
    } else if (deleteMode() === "missing" && pending.method === "thread/delete") {
      send({ id: pending.id, error: { code: -32600, message: `thread not found: ${pending.threadId}` } });
    } else {
      send({ id: pending.id, result: {} });
    }
  }
}

/** 发服务端请求，等回包（回包写进日志）。 */
function ask(method: string, params: Record<string, unknown>): Promise<{ result?: unknown; error?: unknown }> {
  const id = `srv-${++serverRequestCounter}`;
  return new Promise((resolve) => {
    waiting.set(id, (message) => {
      log({ kind: "reply", id, method, result: message.result ?? null, error: message.error ?? null });
      resolve(message);
    });
    send({ id, method, params });
  });
}

const questionParams = (threadId: string, turnId: string) => ({
  threadId, turnId, itemId: "ask-1",
  questions: [{ id: "q1", header: "选择", question: "选哪个文件？", isOther: false,
    options: [{ label: "a.txt", description: "甲" }, { label: "b.txt", description: "乙" }] }],
});
const approvalParams = (threadId: string, turnId: string, itemId: string) => ({
  threadId, turnId, itemId, command: `echo ${itemId}`, cwd: "D:\\",
});

function answerText(reply: { result?: unknown }): string {
  const answers = (reply.result as { answers?: Record<string, { answers?: string[] }> } | null)?.answers;
  return answers?.q1?.answers?.join("、") ?? "（无回答）";
}

function complete(threadId: string, turnId: string, text: string): void {
  const item = { type: "agentMessage", id: "final-1", text, phase: "final_answer" };
  const turn = { id: turnId, items: [item], itemsView: "full", status: interruptedTurns.has(turnId) ? "interrupted" : "completed", error: null };
  completedTurns.set(threadId, [turn]);
  const pending = delayResult && !resultsReleased;
  send({ method: "item/started", params: { threadId, turnId, item: { type: "agentMessage", id: "final-1" } } });
  if (!pending) send({ method: "item/completed", params: { threadId, turnId, item } });
  send({ method: "turn/completed", params: { threadId, turn: pending ? { ...turn, items: [], itemsView: "notLoaded" } : turn } });
}

async function runTurn(threadId: string, turnId: string, early: Promise<{ result?: unknown }> | null): Promise<void> {
  if (scenario === "ask") {
    const reply = await ask("item/tool/requestUserInput", questionParams(threadId, turnId));
    complete(threadId, turnId, `已选择：${answerText(reply)}`);
  } else if (scenario === "early-ask") {
    const reply = await early!;
    complete(threadId, turnId, `已选择：${answerText(reply)}`);
  } else if (scenario === "approval-ask-approval") {
    await ask("item/commandExecution/requestApproval", approvalParams(threadId, turnId, "approve-1"));
    const reply = await ask("item/tool/requestUserInput", questionParams(threadId, turnId));
    await ask("item/commandExecution/requestApproval", approvalParams(threadId, turnId, "approve-2"));
    complete(threadId, turnId, `已选择：${answerText(reply)}`);
  } else if (scenario === "ask-cancel") {
    const id = `srv-${++serverRequestCounter}`;
    waiting.set(id, (message) => log({ kind: "reply", id, method: "item/tool/requestUserInput", result: message.result ?? null, error: message.error ?? null }));
    send({ id, method: "item/tool/requestUserInput", params: questionParams(threadId, turnId) });
    await new Promise<void>((resolve) => {
      const timer = setInterval(() => {
        if (trigger && existsSync(trigger)) { clearInterval(timer); resolve(); }
      }, 20);
    });
    waiting.delete(id);
    send({ method: "serverRequest/resolved", params: { threadId, requestId: id } });
    log({ kind: "cancelled", id });
    complete(threadId, turnId, "提问已被撤销");
  } else if (scenario === "ask-big") {
    // 结果远大于 64 KiB 且全是多字节字符，验证 wait --json 经管道完整输出（规格 6.3 节）。
    const reply = await ask("item/tool/requestUserInput", questionParams(threadId, turnId));
    complete(threadId, turnId, `已选择：${answerText(reply)}\n${"大段中文结果".repeat(12_000)}\n结束标记`);
  } else if (scenario === "hold" && turnCounter === 1) {
    await new Promise<void>((resolve) => holds.set(turnId, resolve));
    complete(threadId, turnId, "已中断");
  } else if (["hold", "complete", "unknown", "restart-unknown", "purge", "lost-stop"].includes(scenario)) {
    complete(threadId, turnId, "续跑完成");
  }
}

/** 仅 legacy detached 互操作夹具使用独立子线程。 */
const REVIEW_THREAD = process.env.MOCK_E2E_REVIEW_THREAD || "review-e2e";

function reviewDone(threadId: string, turnId: string, text: string): void {
  const item = { type: "exitedReviewMode", id: "review-exit", review: text };
  send({ method: "item/completed", params: { threadId, turnId, item } });
  const turn = { id: turnId, items: [item], itemsView: "full",
    status: interruptedTurns.has(turnId) ? "interrupted" : "completed", error: null };
  completedTurns.set(threadId, [turn]);
  const completed = { method: "turn/completed", params: { threadId,
    turn: process.env.MOCK_E2E_REVIEW_INNER_TURN === "1" ? { ...turn, items: [], itemsView: "notLoaded" } : turn } };
  send(completed);
  if (process.env.MOCK_E2E_REVIEW_INNER_TURN === "1") send(completed);
}

/** 审查线程上先后提问两次（第二次在 CLI 交接、broker 托管之后到达），答完以审查结论完成；
 *  被 turn/interrupt 时撤销在等的提问（serverRequest/resolved）并以 interrupted 结束。 */
async function runReview(threadId: string, turnId: string): Promise<void> {
  for (const n of [1, 2]) {
    const id = `srv-${++serverRequestCounter}`;
    const answered = new Promise<{ result?: unknown } | null>((resolve) => {
      waiting.set(id, (message) => {
        log({ kind: "reply", id, method: "item/tool/requestUserInput", result: message.result ?? null, error: message.error ?? null });
        resolve(message);
      });
      holds.set(turnId, () => {
        waiting.delete(id);
        send({ method: "serverRequest/resolved", params: { threadId, requestId: id } });
        log({ kind: "cancelled", id });
        reviewDone(threadId, turnId, "审查已中断");
        resolve(null);
      });
    });
    send({ id, method: "item/tool/requestUserInput", params: { ...questionParams(threadId, turnId), itemId: `review-ask-${n}` } });
    const reply = await answered;
    holds.delete(turnId);
    if (!reply) return;
  }
  reviewDone(threadId, turnId, "审查结论：没有问题");
}

const thread = (id: string) => ({
  id, preview: "", modelProvider: "openai", createdAt: 1, updatedAt: 1, status: { type: "idle" },
  path: null, cwd: "D:\\", cliVersion: "0.160.0", source: "mock", name: null, agentNickname: null,
  agentRole: null, gitInfo: null, turns: [],
});

let buffer = "";
let turnCounter = 0;
if (gateDir) setInterval(releaseLifecycle, 25);
process.stdin.setEncoding("utf-8");
process.stdin.on("data", (chunk: string) => {
  buffer += chunk;
  let idx: number;
  while ((idx = buffer.indexOf("\n")) !== -1) {
    const line = buffer.slice(0, idx).trim();
    buffer = buffer.slice(idx + 1);
    if (!line) continue;
    const msg = JSON.parse(line);
    if (subagents.handle(msg)) continue;
    if (msg.id === undefined) continue;
    if (msg.method === undefined) {
      const waiter = waiting.get(String(msg.id));
      if (waiter) { waiting.delete(String(msg.id)); waiter(msg); }
      continue;
    }
    log({ kind: "request", method: msg.method, params: msg.params ?? null });
    if (statusHistory.handle(msg)) continue;
    const threadId: string = msg.params?.threadId ?? "thread-e2e";
    switch (msg.method) {
      case "initialize":
        send({ id: msg.id, result: { userAgent: "mock-codex-e2e/0.160.0" } });
        break;
      case "thread/start":
      case "thread/resume":
        send({ id: msg.id, result: { thread: { ...thread(msg.method === "thread/start" ? "thread-e2e" : threadId),
          ...((process.env.MOCK_E2E_LONG_HISTORY === "1" || statusReadOptions.longHistory) && msg.method === "thread/resume"
            ? { turns: msg.params?.excludeTurns === true ? [] : longResumeTurns() } : {}) },
          model: "gpt-mock", modelProvider: "openai", cwd: "D:\\", approvalPolicy: msg.params?.approvalPolicy ?? "never", sandbox: { type: msg.params?.sandbox === "read-only" ? "readOnly" : "workspaceWrite" } } });
        break;
      case "thread/memoryMode/set":
        send({ id: msg.id, result: {} });
        break;
      case "thread/archive":
      case "thread/delete":
        if (holdLifecycle(msg.method)) {
          heldLifecycle.push({ id: msg.id, method: msg.method, threadId });
          break;
        }
        if (deleteMode() === "unsupported" && msg.method === "thread/delete") {
          send({ id: msg.id, error: { code: -32601, message: "Method not found: thread/delete" } });
          break;
        }
        if (deleteMode() === "missing" && msg.method === "thread/delete") {
          send({ id: msg.id, error: { code: -32600, message: `thread not found: ${threadId}` } });
          break;
        }
        send({ id: msg.id, result: {} });
        break;
      case "thread/list":
        send({ id: msg.id, result: { data: [], nextCursor: null, backwardsCursor: null } });
        break;
      case "model/list":
        send({ id: msg.id, result: { data: [], nextCursor: null } });
        break;
      case "thread/goal/get":
        send({ id: msg.id, result: { goal: null } });
        break;
      case "thread/read":
        if (completedTurns.has(threadId) && !resultsReleased && delayResult) {
          heldReads.push({ id: msg.id, threadId });
          break;
        }
        send({ id: msg.id, result: { thread: { ...thread(threadId),
          ...(busy() ? { status: { type: "active", activeFlags: [] } } : {}),
          turns: busy() ? [{ id: "turn-lost", items: [], status: "inProgress", error: null }] : completedTurns.get(threadId) ?? [] } } });
        break;
      case "thread/turns/list":
        if (!busy()) {
          send({ id: msg.id, error: { code: -32601, message: `模拟 codex 不支持 ${msg.method}` } });
          break;
        }
        send({ id: msg.id, result: { data: [{ id: "turn-lost", items: [], itemsView: "notLoaded", status: "inProgress", error: null }],
          nextCursor: null, backwardsCursor: null } });
        break;
      case "thread/items/list": {
        if (!itemPaging) {
          send({ id: msg.id, error: { code: -32601, message: `模拟 codex 不支持 ${msg.method}` } });
          break;
        }
        const turnId = String(msg.params?.turnId ?? "");
        if (completedTurns.has(threadId) && !resultsReleased && delayResult) {
          heldItemReads.push({ id: msg.id, threadId, turnId });
          break;
        }
        sendItems(msg.id, threadId, turnId);
        break;
      }
      case "fixture/releaseResult":
        resultsReleased = true;
        for (const read of heldReads.splice(0)) send({ id: read.id, result: { thread: { ...thread(read.threadId), turns: completedTurns.get(read.threadId) ?? [] } } });
        for (const read of heldItemReads.splice(0)) sendItems(read.id, read.threadId, read.turnId);
        send({ id: msg.id, result: {} });
        break;
      case "thread/unsubscribe":
        send({ id: msg.id, result: { status: "unsubscribed" } });
        break;
      case "thread/loaded/list":
        send({ id: msg.id, result: { data: [], nextCursor: null } });
        break;
      case "turn/interrupt": {
        const turnId = String(msg.params?.turnId ?? "");
        interruptedTurns.add(turnId);
        send({ id: msg.id, result: {} });
        if (process.env.MOCK_E2E_CONFIRM_INTERRUPT === "1") {
          send({ method: "turn/completed", params: { threadId, turn: { id: turnId, items: [], status: "interrupted", error: null } } });
        }
        holds.get(turnId)?.();
        holds.delete(turnId);
        break;
      }
      case "review/start": {
        if (msg.params?.delivery === "detached" && process.env.MOCK_E2E_LEGACY_REVIEW !== "1") {
          send({ id: msg.id, error: { code: -32600, message: "paginated threads do not support detached review" } });
          break;
        }
        const reviewThreadId = msg.params?.delivery === "detached" ? REVIEW_THREAD : threadId;
        if (reviewThreadId !== threadId) send({ method: "thread/started", params: { thread: thread(reviewThreadId) } });
        const turnId = `review-turn-${String(++turnCounter).padStart(3, "0")}`;
        const turn = { id: turnId, items: [], status: "inProgress", error: null };
        completedTurns.set(reviewThreadId, [turn]);
        send({ id: msg.id, result: { turn, reviewThreadId } });
        send({ method: "turn/started", params: { threadId: reviewThreadId, turn } });
        if (process.env.MOCK_E2E_REVIEW_INNER_TURN === "1") {
          send({ method: "turn/started", params: { threadId: reviewThreadId, turn: { ...turn, id: `${turnId}-inner` } } });
        }
        void runReview(reviewThreadId, turnId);
        break;
      }
      case "turn/start": {
        const turnId = `turn-${String(++turnCounter).padStart(3, "0")}`;
        let early: Promise<{ result?: unknown }> | null = null;
        if (scenario === "early-ask") early = ask("item/tool/requestUserInput", questionParams(threadId, turnId));
        const reply = () => {
          send({ id: msg.id, result: { turn: { id: turnId, items: [], status: "inProgress", error: null } } });
          send({ method: "turn/started", params: { threadId, turn: { id: turnId, items: [], status: "inProgress", error: null } } });
          void runTurn(threadId, turnId, early);
        };
        if (early) setTimeout(reply, 200); else reply();
        break;
      }
      default:
        send({ id: msg.id, error: { code: -32601, message: `模拟 codex 不支持 ${msg.method}` } });
    }
  }
});
