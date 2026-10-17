/**
 * broker 黑盒测试的工具层；由调用方提供临时目录和进程清理状态。
 */

import net from "node:net";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { basename, delimiter, join } from "node:path";
import type { FileSink, Subprocess } from "bun";

/** 被测 broker：Rust 可执行文件。默认取仓库内 `cargo build` 的产物；CXB_BROKER_BIN 可改指别的构建（如 release）。 */
export const BROKER_BIN = process.env.CXB_BROKER_BIN
  || join(import.meta.dir, "..", "..", "broker-rs", "target", "debug", process.platform === "win32" ? "cxb-broker.exe" : "cxb-broker");
if (!existsSync(BROKER_BIN)) {
  throw new Error(`找不到被测 broker：${BROKER_BIN}。先在 broker-rs 下运行 cargo build，或用 CXB_BROKER_BIN 指定可执行文件。`);
}

export interface BrokerHarnessContext {
  readonly tempDir: string;
  readonly spawnedProcesses: Subprocess[];
}

/** 递归删除目录，并容忍 Windows 异步释放进程句柄；仅对占用相关错误重试。 */
export async function removeDirWithRetry(dir: string, attempts = 40): Promise<void> {
  for (let i = 0; i < attempts; i++) {
    try {
      rmSync(dir, { recursive: true, force: true });
      return;
    } catch (e) {
      const code = (e as NodeJS.ErrnoException).code;
      if (code !== "EBUSY" && code !== "ENOTEMPTY" && code !== "EPERM") throw e;
      await new Promise((r) => setTimeout(r, 50));
    }
  }
  // 停止重试，残留的临时目录交由操作系统清理。
  // 避免仅因目录清理失败而使通过的用例失败。
}

/** 生成被测 broker 的连接地址。Unix 使用临时目录中的套接字；Windows 管道名由唯一临时目录和进程号组成，避免连接到残留管道。 */
export function testSocketPath(dir: string): string {
  if (process.platform === "win32") {
    return `\\\\.\\pipe\\cxb-test-${process.pid}-${basename(dir)}`;
  }
  return join(dir, "broker.sock");
}

/** 把 {@link testSocketPath} 返回的路径转换为 --endpoint 参数。 */
export function endpointFor(socketPath: string): string {
  return process.platform === "win32" ? `pipe:${socketPath}` : `unix:${socketPath}`;
}

/**
 * 模拟 `codex app-server proxy --sock <路径>`（共享接入第 2 档）：与真实 proxy 一样只在 stdio 和
 * AF_UNIX socket 之间搬字节、不分帧（实测核查记录：proxy 不自己分帧）。
 * 测试用环境变量（broker 原样继承给子进程）：
 * - MOCK_PROXY_TARGET：实际连接的 socket，覆盖 --sock。让第 1 档连 --sock 的不存在路径而失败、
 *   第 2 档经 proxy 连上，从而单独验证「第 1 档失败回退第 2 档」；
 * - MOCK_PROXY_HANG=1：连上前什么都不做、也不退出，用于验证每档 3 秒期限；
 * - MOCK_PROXY_LOG：每次启动追加一行（参数与进程号），供用例断言 proxy 是否被拉起、是否已被回收。
 */
export const MOCK_PROXY_SCRIPT = `#!/usr/bin/env bun
import { appendFileSync } from "node:fs";
const args = process.argv.slice(2);
if (process.env.MOCK_PROXY_LOG) appendFileSync(process.env.MOCK_PROXY_LOG, args.join(" ") + " pid=" + process.pid + "\\n");
if (process.env.MOCK_PROXY_HANG === "1") {
  setInterval(() => {}, 60_000);
} else {
  const target = process.env.MOCK_PROXY_TARGET || args[args.indexOf("--sock") + 1];
  let sock;
  try {
    sock = await Bun.connect({ unix: target, socket: {
      data(_s, data) { process.stdout.write(data); },
      close() { process.exit(0); },
      error() { process.exit(1); },
    }});
  } catch (e) {
    process.stderr.write("mock proxy: cannot connect " + target + ": " + (e && e.message) + "\\n");
    process.exit(1);
  }
  process.stdin.on("data", (data) => sock.write(data));
  process.stdin.on("end", () => { sock.end(); process.exit(0); });
}
`;

/** 模拟脚本里需要内联的测试支持模块：每个模块的导入必须包在自己的一对标记之间。 */
const INLINE_MODULES = [
  { marker: "RESUME_HISTORY", label: "恢复历史", file: "resume-history.ts", specifier: "../../test-support/resume-history" },
  { marker: "SUBAGENT_MOCK", label: "子代理模拟", file: "subagent-mock.ts", specifier: "../../test-support/subagent-mock" },
  { marker: "BACKGROUND_TERMINALS", label: "后台命令模拟", file: "background-terminals-mock.ts", specifier: "./background-terminals-mock" },
  { marker: "STATUS_HISTORY", label: "状态历史", file: "status-history.ts", specifier: "../../test-support/status-history" },
] as const;

/** 把标记对之间的导入整体换成模块源码（去掉 export）。标记缺失、重复、失配或替换次数不是一次都报错，不写任何文件。 */
function inlineModule(script: string, module: (typeof INLINE_MODULES)[number]): string {
  const begin = `// CXB_INLINE_${module.marker}_BEGIN`;
  const end = `// CXB_INLINE_${module.marker}_END`;
  if (!script.includes(module.specifier) && !script.includes(begin) && !script.includes(end)) return script;
  if (script.split(begin).length !== 2 || script.split(end).length !== 2) {
    throw new Error(`${module.label}内联标记必须各出现一次`);
  }
  let replacements = 0;
  const pattern = new RegExp(`^// CXB_INLINE_${module.marker}_BEGIN\\r?\\n[\\s\\S]*?^// CXB_INLINE_${module.marker}_END\\r?$`, "gm");
  const result = script.replace(pattern, () => {
    replacements++;
    return readFileSync(join(import.meta.dir, module.file), "utf8").replace(/^export /gm, "");
  });
  if (replacements !== 1) throw new Error(`${module.label}内联替换必须恰好一次，实际为 ${replacements}`);
  return result;
}

/** 在 PATH 目录中创建模拟 codex。Unix 使用可执行脚本；Windows 使用 codex-mock.ts 和 codex.cmd，
 *  `codex app-server proxy …` 分派到 {@link MOCK_PROXY_SCRIPT}。必须关闭命令回显，避免污染 stdout 上的 JSON-RPC。 */
export function writeMockCodexScript(dir: string, script: string): void {
  // 复制后的夹具保持自包含，避免模块加载期间预加载观测器提前排空握手。
  for (const module of INLINE_MODULES) script = inlineModule(script, module);
  if (process.platform === "win32") {
    writeFileSync(join(dir, "codex-mock.ts"), script);
    writeFileSync(join(dir, "codex-proxy.ts"), MOCK_PROXY_SCRIPT);
    writeFileSync(join(dir, "codex.cmd"), [
      "@echo off",
      'if "%~2"=="proxy" goto proxy',
      'bun run "%~dp0codex-mock.ts" %*',
      "exit /b %errorlevel%",
      ":proxy",
      'bun run "%~dp0codex-proxy.ts" %*',
      "",
    ].join("\r\n"));
    return;
  }
  writeFileSync(join(dir, "codex"), script, { mode: 0o755 });
}

/** 复制 process.env 并将目录加到 PATH 前面。Windows 环境变量名不区分大小写，因此先移除所有大小写形式的 PATH。 */
export function envWithPathPrefix(dir: string): Record<string, string> {
  const env: Record<string, string> = {};
  let inherited = "";
  for (const [k, v] of Object.entries(process.env)) {
    if (v === undefined) continue;
    if (k.toLowerCase() === "path") { inherited = v; continue; }
    env[k] = v;
  }
  env.PATH = `${dir}${delimiter}${inherited}`;
  // 隔离开发者正在运行的 app-server 守护进程，
  // 确保被测 broker 连接 PATH 中的模拟程序。
  env.CXB_SERVER = "private";
  return env;
}

/** 创建用于 codex app-server 的 JSON-RPC 模拟脚本，支持初始化、线程、轮次、审批和通知。 */
export function createMockCodex(dir: string, opts?: {
  backgroundTerminals?: import("./background-terminals-mock").BackgroundTerminalsMockOptions;
  subagents?: import("./subagent-mock").SubagentMockOptions;
  statusRead?: import("./subagent-mock").StatusReadMockOptions;
  /** 仅恢复未排除历史时返回约 12 MiB 历史，默认关闭。 */
  longHistory?: boolean;
  /** 响应 turn/start 前的延迟，单位为毫秒。 */
  turnDelay?: number;
  /** 是否在 turn/start 响应之后发送 turn/completed 通知。 */
  sendTurnCompleted?: boolean;
  /** 是否在 turn/start 之后发送审批请求。 */
  sendApproval?: boolean;
  /** 在 turn/start 响应前发送审批请求，响应延后 200 毫秒，模拟审批与启动的竞态。 */
  approvalBeforeResponse?: boolean;
  /** 提前发送的请求方法，默认为命令审批。 */
  earlyRequestMethod?: string;
  /** thread/read 报告其他客户端的轮次正在运行，turn/start 不发送启动通知，模拟请求被该轮次吸收。 */
  absorbedStart?: boolean;
  /** 提交后立即收到上一轮完成通知；本轮在 100 毫秒后启动并完成，均早于响应。 */
  predecessorCompletion?: boolean;
  /** 启动后的 thread/read 尚未返回时收到上一轮完成通知；读取结果报告本轮运行中，本轮在启动 500 毫秒后才宣布并完成。 */
  predecessorDuringRead?: boolean;
  /** thread/read 失败；本轮在启动 300 毫秒后宣布，再过 50 毫秒请求审批，在 700 毫秒时完成。 */
  undecidedThenOwn?: boolean;
  /** 响应后发送 turn/completed 的延迟，单位为毫秒。 */
  turnCompletedDelay?: number;
  /** 在 turn/start 响应前写入 turn/completed，模拟 broker 尚未解析启动响应时已收到完成通知。 */
  completeBeforeResponse?: boolean;
  /** 响应 review/start 前的延迟，单位为毫秒；让客户端在请求期间断开，以测试孤立轮次的清理。 */
  reviewDelay?: number;
  /** 扣住 review 回包，测试通过 fixture/releaseReview 显式放行。 */
  holdReview?: boolean;
  /** 模拟目标模式线程：目标仍活跃时，首轮完成后由服务器续跑一轮，再完成目标。 */
  goalContinuation?: boolean;
  /** 模拟轮次间隙暂停目标：首轮完成时目标仍活跃，但不续跑，改为发送 goal/updated(paused)。 */
  goalPausedInGap?: boolean;
  /** 模拟已有活跃目标：thread/goal/get 返回活跃目标，但不发送 goal/updated；首轮完成后续跑。 */
  goalPreexisting?: boolean;
  /** 模拟目标线程的快速完成竞态：goal/updated(active) 和 turn/completed 均早于 turn/start 响应，然后续跑；首轮已完成仍须取得所有权。 */
  goalFastFirstTurn?: boolean;
  /** 各轮使用独立标识；第二轮开始后重放已结算的第一轮完成通知，第二轮本身不完成。 */
  staleCompletionReplay?: boolean;
  /** 首次 turn/start 宣布轮次后返回错误；轮次继续运行，其完成通知在第二轮取得线程所有权后才到达。 */
  startedThenError?: boolean;
  /** 模拟 Codex 0.147.0 的审查内部轮次：review/start 返回轮次 A，随后宣布轮次 B，再完成 A；不能把 A 的完成误判为其他轮次。 */
  reviewInnerTurn?: boolean;
  /** 仅显式 legacy 夹具接受 detached；缺省与 inline 始终留在请求线程。 */
  legacyReview?: boolean;
  /** turn/interrupt 拒绝与指定标识不同的 turnId，模拟运行期间轮次更换后的不匹配错误。 */
  activeTurnId?: string;
}): string {
  const turnDelay = opts?.turnDelay ?? 0;
  const sendTurnCompleted = opts?.sendTurnCompleted ?? true;
  const sendApproval = opts?.sendApproval ?? false;
  const approvalBeforeResponse = opts?.approvalBeforeResponse ?? false;
  const earlyRequestMethod = opts?.earlyRequestMethod ?? "item/commandExecution/requestApproval";
  const absorbedStart = opts?.absorbedStart ?? false;
  const predecessorCompletion = opts?.predecessorCompletion ?? false;
  const predecessorDuringRead = opts?.predecessorDuringRead ?? false;
  const undecidedThenOwn = opts?.undecidedThenOwn ?? false;
  const turnCompletedDelay = opts?.turnCompletedDelay ?? 10;
  const completeBeforeResponse = opts?.completeBeforeResponse ?? false;
  const reviewDelay = opts?.reviewDelay ?? 0;
  const reviewInnerTurn = opts?.reviewInnerTurn ?? false;
  const startedThenError = opts?.startedThenError ?? false;
  const goalContinuation = opts?.goalContinuation ?? false;
  const goalPausedInGap = opts?.goalPausedInGap ?? false;
  const goalPreexisting = opts?.goalPreexisting ?? false;
  const goalFastFirstTurn = opts?.goalFastFirstTurn ?? false;
  const staleCompletionReplay = opts?.staleCompletionReplay ?? false;
  const activeTurnId = opts?.activeTurnId ?? null;

  const interruptLog = join(dir, "interrupts.log");
  const script = `#!/usr/bin/env bun
import { appendFileSync } from "node:fs";
// CXB_INLINE_RESUME_HISTORY_BEGIN
import { fixtureSender, longResumeTurns } from "../../test-support/resume-history";
// CXB_INLINE_RESUME_HISTORY_END
// CXB_INLINE_SUBAGENT_MOCK_BEGIN
import { createSubagentMock } from "../../test-support/subagent-mock";
// CXB_INLINE_SUBAGENT_MOCK_END
// CXB_INLINE_STATUS_HISTORY_BEGIN
import { createStatusHistory } from "../../test-support/status-history";
// CXB_INLINE_STATUS_HISTORY_END
// broker 测试使用的模拟 codex app-server。
const args = process.argv.slice(2);
if (args[0] !== "app-server") {
  process.stderr.write("Mock codex: expected 'app-server' subcommand\\n");
  process.exit(1);
}

// 只有一个写出队列；状态历史从包装后的发送函数学习本模拟发出的轮次与条目。
const rawRespond = fixtureSender();
const statusHistory = createStatusHistory(${JSON.stringify(opts?.statusRead ?? {})}, rawRespond, longResumeTurns);
const respond = statusHistory.wrap(rawRespond);
const subagents = createSubagentMock(respond, ${JSON.stringify(opts?.subagents ?? {})}, ${JSON.stringify(opts?.backgroundTerminals ?? {})});

let buffer = "";
let approvalIdCounter = 1;
let turnCounter = 0;
let predecessorSent = false;
// 记录供清理行为测试通过 mock/state 读取的状态。
const unsubscribed = [];
const calls = [];
const approvalAnswers = [];
const pendingServerRequests = new Map();
process.stdin.setEncoding("utf-8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  let idx;
  while ((idx = buffer.indexOf("\\n")) !== -1) {
    const line = buffer.slice(0, idx).trim();
    buffer = buffer.slice(idx + 1);
    if (!line) continue;
    let msg;
    try { msg = JSON.parse(line); } catch { continue; }
    if (subagents.handle(msg)) continue;

    // 没有 id 的消息是通知。
    if (msg.id === undefined) continue;

    // 这是对模拟程序主动发出请求的响应，
    // 交给等待者处理，不把它当作新的调用。
    if (msg.method === undefined) {
      const waiter = pendingServerRequests.get(String(msg.id));
      if (waiter) { pendingServerRequests.delete(String(msg.id)); waiter(msg); }
      continue;
    }

    calls.push(msg.method);
    if (statusHistory.handle(msg)) continue;
    switch (msg.method) {
      case "thread/resume":
        respond({ id: msg.id, result: {
          thread: { id: msg.params?.threadId || "thread-001", preview: "", modelProvider: "openai",
            createdAt: Date.now(), updatedAt: Date.now(), status: { type: "idle" }, path: null, cwd: "/tmp",
            cliVersion: "0.1.0", source: "mock", name: null, agentNickname: null, agentRole: null, gitInfo: null, turns: ${!!(opts?.longHistory || opts?.statusRead?.longHistory)} && msg.params?.excludeTurns !== true ? longResumeTurns() : [] },
          model: "gpt-5.3-codex", modelProvider: "openai", cwd: "/tmp", approvalPolicy: "never", sandbox: null,
        }});
        break;

      case "thread/unsubscribe":
        unsubscribed.push(msg.params?.threadId);
        respond({ id: msg.id, result: { status: "unsubscribed" } });
        break;

      case "thread/loaded/list":
        respond({ id: msg.id, result: { data: [], nextCursor: null } });
        break;

      case "mock/state":
        respond({ id: msg.id, result: { unsubscribed: [...unsubscribed], calls: [...calls], approvalAnswers: [...approvalAnswers] } });
        break;

      case "mock/foreignApproval": {
        // 为调用者指定的线程发送服务器请求，默认线程没有 broker 客户端持有。
        // 模拟共享 app-server 在 Codex 应用或终端界面运行轮次时，
        // 向所有已订阅客户端发送请求。
        // method 参数选择请求类型，
        // 返回 broker 是否在等待期限内回答；为较慢的持续集成环境预留时间。
        const reqId = "foreign-" + (approvalIdCounter++);
        let answer = null;
        pendingServerRequests.set(reqId, (m) => { answer = m; });
        respond({
          id: reqId,
          method: msg.params?.method ?? "item/commandExecution/requestApproval",
          params: { threadId: msg.params?.threadId ?? "foreign-001", turnId: "turn-f", itemId: "item-f", command: "rm -rf /", cwd: "/tmp" },
        });
        setTimeout(() => {
          pendingServerRequests.delete(reqId);
          respond({ id: msg.id, result: { answered: answer !== null, answer } });
        }, 1500);
        break;
      }

      case "turn/steer":
        // 加入正在运行的轮次，响应返回该轮次标识。
        respond({ id: msg.id, result: { turnId: "turn-active" } });
        break;

      case "initialize":
        respond({ id: msg.id, result: { userAgent: "mock-codex/0.1.0" } });
        break;

      case "thread/start":
        respond({ id: msg.id, result: {
          thread: {
            id: "thread-001", preview: "", modelProvider: "openai",
            createdAt: Date.now(), updatedAt: Date.now(),
            status: { type: "idle" }, path: null, cwd: "/tmp",
            cliVersion: "0.1.0", source: "mock", name: null,
            agentNickname: null, agentRole: null, gitInfo: null, turns: [],
          },
          model: "gpt-5.3-codex", modelProvider: "openai",
          cwd: "/tmp", approvalPolicy: "never", sandbox: null,
        }});
        break;

      case "thread/goal/get": {
        ${goalPreexisting ? `
        respond({ id: msg.id, result: { goal: {
          threadId: msg.params?.threadId || "thread-001", objective: "pre-existing objective",
          status: "active", tokenBudget: 1000, tokensUsed: 100, timeUsedSeconds: 1,
          createdAt: 1, updatedAt: 2,
        }}});
        ` : `
        respond({ id: msg.id, result: { goal: null } });
        `}
        break;
      }

      case "turn/start": {
        const threadId = msg.params?.threadId || "thread-001";
        ${startedThenError ? `
        turnCounter++;
        if (turnCounter === 1) {
          respond({ method: "turn/started", params: { threadId: threadId, turn: { id: "turn-001", items: [], status: "inProgress", error: null } } });
          respond({ id: msg.id, error: { code: -32000, message: "simulated request failure after turn/started" } });
          setTimeout(() => {
            respond({ method: "turn/completed", params: { threadId: threadId, turn: { id: "turn-001", items: [], status: "completed", error: null } } });
          }, 400);
        } else {
          respond({ id: msg.id, result: { turn: { id: "turn-002", items: [], status: "inProgress", error: null } } });
          respond({ method: "turn/started", params: { threadId: threadId, turn: { id: "turn-002", items: [], status: "inProgress", error: null } } });
        }
        break;
        ` : ""}
        ${staleCompletionReplay ? `
        turnCounter++;
        const thisTurnId = "turn-" + String(turnCounter).padStart(3, "0");
        respond({ id: msg.id, result: { turn: { id: thisTurnId, items: [], status: "inProgress", error: null } } });
        respond({ method: "turn/started", params: { threadId: threadId, turn: { id: thisTurnId, items: [], status: "inProgress", error: null } } });
        if (turnCounter === 1) {
          setTimeout(() => {
            respond({ method: "turn/completed", params: { threadId: threadId, turn: { id: "turn-001", items: [], status: "completed", error: null } } });
          }, 30);
        } else {
          // 第二轮正在运行，重放已结算的第一轮完成通知，
          // 此时线程所有权已经更换。
          setTimeout(() => {
            respond({ method: "turn/completed", params: { threadId: threadId, turn: { id: "turn-001", items: [], status: "completed", error: null } } });
          }, 60);
        }
        break;
        ` : ""}
        ${goalFastFirstTurn ? `
        // 目标线程的快速完成竞态：目标激活且轮次完成，
        // 均发生在 broker 收到 turn/start 响应之前。
        respond({ method: "thread/goal/updated", params: { threadId: threadId, turnId: "turn-001", goal: {
          threadId: threadId, objective: "fast goal", status: "active", tokenBudget: 1000,
          tokensUsed: 50, timeUsedSeconds: 1, createdAt: 1, updatedAt: 2,
        }}});
        respond({ method: "turn/completed", params: { threadId: threadId, turn: { id: "turn-001", items: [], status: "completed", error: null } } });
        setTimeout(() => {
          respond({ method: "turn/started", params: { threadId: threadId, turn: { id: "turn-002", items: [], status: "inProgress", error: null } } });
          respond({ method: "item/agentMessage/delta", params: { threadId: threadId, turnId: "turn-002", itemId: "m2", delta: "continuation output" } });
        }, 60);
        ` : ""}
        ${completeBeforeResponse ? `
        // 快速完成竞态：先写入 turn/completed，
        // 再写入 turn/start 响应，使 broker 在同一数据块中
        // 先读取完成通知。
        respond({
          method: "turn/completed",
          params: {
            threadId: threadId,
            turn: { id: "turn-001", items: [], status: "completed", error: null },
          },
        });
        ` : ""}
        ${undecidedThenOwn ? `
        setTimeout(() => {
          respond({ method: "turn/started", params: { threadId: threadId, turn: { id: "turn-001", items: [], status: "inProgress", error: null } } });
          setTimeout(() => {
            const lateId = "approval-" + (approvalIdCounter++);
            pendingServerRequests.set(lateId, (m) => { approvalAnswers.push(m.result ?? m.error ?? null); });
            respond({ id: lateId, method: "item/commandExecution/requestApproval", params: { threadId: threadId, turnId: "turn-001", itemId: "item-late", command: "echo late", cwd: "/tmp" } });
          }, 50);
          setTimeout(() => {
            respond({ method: "turn/completed", params: { threadId: threadId, turn: { id: "turn-001", items: [], status: "completed", error: null } } });
          }, 400);
        }, 300);
        ` : ""}
        ${predecessorDuringRead ? `
        setTimeout(() => {
          respond({ method: "turn/started", params: { threadId: threadId, turn: { id: "turn-001", items: [], status: "inProgress", error: null } } });
          respond({ method: "item/agentMessage/delta", params: { threadId: threadId, turnId: "turn-001", itemId: "m1", delta: "own output" } });
          respond({ method: "turn/completed", params: { threadId: threadId, turn: { id: "turn-001", items: [], status: "completed", error: null } } });
        }, 500);
        ` : ""}
        ${predecessorCompletion ? `
        respond({ method: "turn/completed", params: { threadId: threadId, turn: { id: "turn-000", items: [], status: "completed", error: null } } });
        setTimeout(() => {
          respond({ method: "turn/started", params: { threadId: threadId, turn: { id: "turn-001", items: [], status: "inProgress", error: null } } });
          respond({ method: "item/agentMessage/delta", params: { threadId: threadId, turnId: "turn-001", itemId: "m1", delta: "own output" } });
          respond({ method: "turn/completed", params: { threadId: threadId, turn: { id: "turn-001", items: [], status: "completed", error: null } } });
        }, 100);
        ` : ""}
        ${approvalBeforeResponse ? `
        {
          const earlyId = "approval-" + (approvalIdCounter++);
          pendingServerRequests.set(earlyId, (m) => { approvalAnswers.push(m.result ?? m.error ?? null); });
          respond({
            id: earlyId,
            method: ${JSON.stringify(earlyRequestMethod)},
            params: { threadId: threadId, turnId: "turn-001", itemId: "item-early", command: "echo early", cwd: "/tmp" },
          });
        }
        ` : ""}
        setTimeout(() => {
          respond({ id: msg.id, result: {
            turn: { id: "turn-001", items: [], status: "inProgress", error: null },
          }});

          ${sendApproval ? `
          // 在 turn/start 响应之后发送审批请求。
          setTimeout(() => {
            const approvalId = "approval-" + (approvalIdCounter++);
            respond({
              id: approvalId,
              method: "item/commandExecution/requestApproval",
              params: {
                threadId: threadId,
                turnId: "turn-001",
                itemId: "item-001",
                command: "echo hello",
                cwd: "/tmp",
              },
            });
          }, 5);
          ` : ""}

          ${goalContinuation ? `
          // 目标模式流程：目标激活、首轮完成、服务器自主续跑，
          // 随后续跑轮次完成，
          // 最后目标完成；时间压缩，顺序保持与实际流程一致。
          const goal = (status, tokensUsed) => ({
            threadId: threadId, objective: "mock objective", status: status,
            tokenBudget: 1000, tokensUsed: tokensUsed, timeUsedSeconds: 1,
            createdAt: 1, updatedAt: 2,
          });
          setTimeout(() => {
            respond({ method: "thread/goal/updated", params: { threadId: threadId, turnId: "turn-001", goal: goal("active", 100) } });
            respond({ method: "turn/completed", params: { threadId: threadId, turn: { id: "turn-001", items: [], status: "completed", error: null } } });
          }, ${turnCompletedDelay});
          setTimeout(() => {
            respond({ method: "turn/started", params: { threadId: threadId, turn: { id: "turn-002", items: [], status: "inProgress", error: null } } });
            respond({ method: "item/agentMessage/delta", params: { threadId: threadId, turnId: "turn-002", itemId: "m2", delta: "continuation output" } });
          }, ${turnCompletedDelay} + 30);
          setTimeout(() => {
            respond({ method: "thread/goal/updated", params: { threadId: threadId, turnId: "turn-002", goal: goal("complete", 200) } });
            respond({ method: "turn/completed", params: { threadId: threadId, turn: { id: "turn-002", items: [], status: "completed", error: null } } });
          }, ${turnCompletedDelay} + 60);
          ` : goalPausedInGap ? `
          // 首轮完成时目标仍活跃，随后在轮次间隙暂停，
          // 不启动续跑轮次。
          const goal = (status) => ({
            threadId: threadId, objective: "mock objective", status: status,
            tokenBudget: 1000, tokensUsed: 100, timeUsedSeconds: 1,
            createdAt: 1, updatedAt: 2,
          });
          setTimeout(() => {
            respond({ method: "thread/goal/updated", params: { threadId: threadId, turnId: "turn-001", goal: goal("active") } });
            respond({ method: "turn/completed", params: { threadId: threadId, turn: { id: "turn-001", items: [], status: "completed", error: null } } });
          }, ${turnCompletedDelay});
          setTimeout(() => {
            respond({ method: "thread/goal/updated", params: { threadId: threadId, turnId: null, goal: goal("paused") } });
          }, ${turnCompletedDelay} + 40);
          ` : goalPreexisting ? `
          // 已有目标不发送任何 goal/updated 通知，
          // broker 只能从 thread/goal/get 得知目标状态；首轮
          // 完成后服务器开始续跑。
          setTimeout(() => {
            respond({ method: "turn/completed", params: { threadId: threadId, turn: { id: "turn-001", items: [], status: "completed", error: null } } });
          }, ${turnCompletedDelay});
          setTimeout(() => {
            respond({ method: "turn/started", params: { threadId: threadId, turn: { id: "turn-002", items: [], status: "inProgress", error: null } } });
            respond({ method: "item/agentMessage/delta", params: { threadId: threadId, turnId: "turn-002", itemId: "m2", delta: "continuation output" } });
          }, ${turnCompletedDelay} + 30);
          ` : sendTurnCompleted ? `
          setTimeout(() => {
            respond({
              method: "turn/completed",
              params: {
                threadId: threadId,
                turn: { id: "turn-001", items: [], status: "completed", error: null },
              },
            });
          }, ${turnCompletedDelay});
          ` : ""}
        }, ${approvalBeforeResponse ? 200 : turnDelay});
        break;
      }

      case "fixture/reviewGate":
        respond({ id: msg.id, result: { held: !!globalThis.heldReview } });
        break;
      case "fixture/releaseReview": {
        const release = globalThis.heldReview;
        globalThis.heldReview = null;
        if (release) release();
        respond({ id: msg.id, result: { released: !!release } });
        break;
      }
      case "review/start": {
        const threadId = msg.params?.threadId || "thread-001";
        if (msg.params?.delivery === "detached" && !${opts?.legacyReview ?? false}) {
          respond({ id: msg.id, error: { code: -32600, message: "paginated threads do not support detached review" } });
          break;
        }
        const reviewThreadId = msg.params?.delivery === "detached" ? "review-thread-001" : threadId;
        const finishReview = () => {
          if (reviewThreadId !== threadId) respond({ method: "thread/started", params: { thread: { id: reviewThreadId } } });
          respond({ id: msg.id, result: {
            turn: { id: "review-turn-001", items: [], status: "inProgress", error: null },
            reviewThreadId: reviewThreadId,
          }});
          ${reviewInnerTurn ? `
          // 审查过程中，真实 Codex 会以独立标识宣布内部轮次，
          // 且早于审查轮次自身完成。
          setTimeout(() => {
            respond({
              method: "turn/started",
              params: {
                threadId: reviewThreadId,
                turn: { id: "review-inner-001", items: [], status: "inProgress", error: null },
              },
            });
          }, 20);
          ` : ""}
          ${sendTurnCompleted ? `
          setTimeout(() => {
            respond({
              method: "turn/completed",
              params: {
                threadId: reviewThreadId,
                turn: { id: "review-turn-001", items: [{ id: "review-result", type: "exitedReviewMode", review: "审查完成" }], itemsView: "full", status: "completed", error: null },
              },
            });
          }, ${turnCompletedDelay});
          ` : ""}
        };
        if (${opts?.holdReview ?? false}) globalThis.heldReview = finishReview;
        else setTimeout(finishReview, ${reviewDelay});
        break;
      }

      case "turn/interrupt":
        appendFileSync(${JSON.stringify(interruptLog)}, JSON.stringify({
          threadId: msg.params?.threadId ?? null,
          turnId: msg.params?.turnId ?? null,
        }) + "\\n");
        ${activeTurnId ? `
        if (msg.params?.turnId !== ${JSON.stringify(activeTurnId)}) {
          respond({ id: msg.id, error: { code: -32000, message: "expected active turn id " + msg.params?.turnId + " but found ${activeTurnId}" } });
          break;
        }` : ""}
        respond({ id: msg.id, result: {} });
        break;

      case "thread/read":
        ${undecidedThenOwn ? `
        respond({ id: msg.id, error: { code: -32000, message: "simulated read failure" } });
        break;
        ` : ""}
        ${predecessorDuringRead ? `
        // 上一轮的完成只在首次读取期间到达一次（吸收判定经 H2 状态读取层会因此重读）。
        if (!predecessorSent) {
          predecessorSent = true;
          respond({ method: "turn/completed", params: { threadId: msg.params?.threadId || "thread-001", turn: { id: "turn-000", items: [], status: "completed", error: null } } });
        }
        setTimeout(() => respond({ id: msg.id, result: { thread: {
          id: msg.params?.threadId || "thread-001", preview: "", modelProvider: "openai", createdAt: Date.now(), updatedAt: Date.now(),
          status: { type: "active" }, path: null, cwd: "/tmp", cliVersion: "0.1.0", source: "mock", name: null,
          agentNickname: null, agentRole: null, gitInfo: null,
          turns: [{ id: "turn-001", status: "inProgress", items: [], error: null }],
        }}}), 100);
        break;
        ` : ""}
        respond({ id: msg.id, result: {
          thread: {
            id: msg.params?.threadId || "thread-001", preview: "",
            modelProvider: "openai", createdAt: Date.now(), updatedAt: Date.now(),
            status: { type: ${absorbedStart ? '"active"' : '"idle"'} }, path: null, cwd: "/tmp",
            cliVersion: "0.1.0", source: "mock", name: null,
            agentNickname: null, agentRole: null, gitInfo: null,
            turns: ${absorbedStart ? '[{ id: "theirs-1", status: "inProgress", items: [], error: null }]' : '[]'},
          },
        }});
        break;

      ${absorbedStart || predecessorDuringRead ? `
      case "thread/turns/list":
        // 吸收判定经 H2 状态读取层分页读轮次（场景数据）：与 thread/read 报告的活动轮次一致，一页给完，不带条目。
        respond({ id: msg.id, result: {
          data: [{ id: ${absorbedStart ? '"theirs-1"' : '"turn-001"'}, status: "inProgress", items: [], itemsView: "notLoaded", error: null }],
          nextCursor: null, backwardsCursor: null,
        }});
        break;
      ` : ""}

      case "thread/list":
        respond({ id: msg.id, result: { data: [], nextCursor: null } });
        break;

      case "model/list":
        respond({ id: msg.id, result: {
          data: [{ id: "mock-model", description: "mock", supportedReasoningEfforts: [] }],
          nextCursor: null,
        }});
        break;

      default:
        respond({ id: msg.id, error: { code: -32601, message: "Method not found: " + msg.method } });
    }
  }
});

process.stdin.on("end", () => process.exit(0));
process.stdin.on("error", () => process.exit(1));
`;

  writeMockCodexScript(dir, script);
  return dir; // 返回需要加到 PATH 前面的目录。
}

/** 通过 Unix 套接字上的 WebSocket 提供共享模拟 app-server。每个连接启动独立子进程，双向桥接 JSON 行；供 shared 模式的路由测试使用。 */
export function serveMockOverSocket(context: BrokerHarnessContext, mockCodexDir: string, socketPath: string): { stop: () => void } {
  // Windows 上 mock 是 codex-mock.ts 加 codex.cmd：直接用 bun 起脚本，不经 cmd，免得多一层进程。
  const mockCommand = process.platform === "win32"
    ? ["bun", "run", join(mockCodexDir, "codex-mock.ts"), "app-server"]
    : [join(mockCodexDir, "codex"), "app-server"];
  const childOf = new Map<object, Subprocess>();
  const server = Bun.serve({
    unix: socketPath,
    fetch(req, srv) {
      if (srv.upgrade(req)) return undefined as unknown as Response;
      return new Response("not a websocket", { status: 400 });
    },
    websocket: {
      open(ws) {
        const child = Bun.spawn(mockCommand, {
          stdin: "pipe", stdout: "pipe", stderr: "inherit", env: envWithPathPrefix(mockCodexDir),
        });
        childOf.set(ws, child);
        context.spawnedProcesses.push(child);
        void (async () => {
          const reader = child.stdout.getReader();
          const decoder = new TextDecoder();
          let buf = "";
          for (;;) {
            const { value, done } = await reader.read();
            if (done) break;
            buf += decoder.decode(value);
            let i: number;
            while ((i = buf.indexOf("\n")) !== -1) {
              const line = buf.slice(0, i).trim();
              buf = buf.slice(i + 1);
              if (line) { try { ws.send(line); } catch { /* 客户端已断开 */ } }
            }
          }
        })();
      },
      message(ws, raw) {
        const child = childOf.get(ws);
        if (!child) return;
        const stdin = child.stdin as FileSink;
        stdin.write(String(raw) + "\n");
        stdin.flush();
      },
      close(ws) {
        const child = childOf.get(ws);
        childOf.delete(ws);
        if (child) { try { (child.stdin as FileSink).end(); } catch { /* 输入流已关闭 */ } child.kill(); }
      },
    },
  });
  return {
    stop: () => {
      server.stop(true);
      for (const child of childOf.values()) child.kill();
      childOf.clear();
    },
  };
}

/** 去掉开发者环境里可能设置的接入选择变量，用例只按自己传入的值选档。 */
function withoutServerSelection(env: Record<string, string>): Record<string, string> {
  const dropped = new Set(["CXB_SERVER", "CXB_SERVER_SOCKET", "MOCK_PROXY_TARGET", "MOCK_PROXY_HANG", "MOCK_PROXY_LOG"]);
  return Object.fromEntries(Object.entries(env).filter(([k]) => !dropped.has(k.toUpperCase())));
}

/** 以 PATH 中的模拟 codex 启动被测 broker 子进程。 */
export function spawnBroker(
  context: BrokerHarnessContext,
  endpoint: string,
  mockCodexDir: string,
  opts?: {
    idleTimeout?: number;
    cwd?: string;
    env?: Record<string, string>;
  },
): Subprocess {
  const args = [
    BROKER_BIN,
    "serve",
    "--endpoint", endpoint,
    "--idle-timeout", String(opts?.idleTimeout ?? 30000),
  ];
  if (opts?.cwd) {
    args.push("--cwd", opts.cwd);
  }

  const proc = Bun.spawn(args, {
    // CXB_HOME：broker 的状态文件根目录。每次拉起默认用一个新的临时目录，既不写开发者的
    // ~/.codex-bridge，也不让前一个用例留下的运行记录在下一个 broker 启动时被当作旧数据处理；
    // 要测重启的用例自己传同一个 CXB_HOME。
    // 接入方式（M7）：默认私有，绝不连开发者本机正在运行的 Codex 共享服务器；CODEX_HOME 每次一个
    // 临时目录，让缺省 socket 路径不存在、初始化互斥量互不排队（要测互斥的用例自己传同一个）。
    // 用例要测共享接入时自己传 CXB_SERVER 和 socket 变量，覆盖这里的缺省值。
    env: {
      ...withoutServerSelection(envWithPathPrefix(mockCodexDir)),
      CXB_HOME: mkdtempSync(join(context.tempDir, "cxb-home-")),
      CODEX_HOME: mkdtempSync(join(context.tempDir, "codex-home-")),
      CXB_SERVER: "private",
      ...(opts?.env ?? {}),
    },
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
    cwd: opts?.cwd ?? context.tempDir,
  });

  context.spawnedProcesses.push(proc);
  return proc;
}

/** 等待 broker 套接字可连接。 */
export async function waitForSocket(
  sockPath: string,
  timeoutMs = 10_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const sock = new net.Socket();
      await new Promise<void>((resolve, reject) => {
        sock.on("connect", () => { sock.destroy(); resolve(); });
        sock.on("error", reject);
        sock.connect({ path: sockPath });
      });
      return;
    } catch {
      await new Promise((r) => setTimeout(r, 50));
    }
  }
  throw new Error(`Socket ${sockPath} did not become available within ${timeoutMs}ms`);
}

/** 测试用的最小 JSON-RPC 客户端，通过套接字连接，完成初始化握手，并提供请求、通知和消息处理工具。 */
export class TestClient {
  private socket: net.Socket;
  private buffer = "";
  private nextId = 1;
  private pending = new Map<string | number, {
    resolve: (v: unknown) => void;
    reject: (e: Error) => void;
  }>();
  private notificationHandlers: Array<(msg: Record<string, unknown>) => void> = [];
  private requestHandlers: Array<(msg: Record<string, unknown>) => void> = [];
  private allMessages: Array<Record<string, unknown>> = [];

  private constructor(socket: net.Socket) {
    this.socket = socket;
    socket.setEncoding("utf8");
    socket.on("data", (chunk: string) => {
      this.buffer += chunk;
      let idx: number;
      while ((idx = this.buffer.indexOf("\n")) !== -1) {
        const line = this.buffer.slice(0, idx).trim();
        this.buffer = this.buffer.slice(idx + 1);
        if (!line) continue;
        try {
          const msg = JSON.parse(line) as Record<string, unknown>;
          this.allMessages.push(msg);
          this.dispatch(msg);
        } catch {}
      }
    });
  }

  static async connect(sockPath: string): Promise<TestClient> {
    const socket = await new Promise<net.Socket>((resolve, reject) => {
      const sock = new net.Socket();
      const timer = setTimeout(() => {
        sock.destroy();
        reject(new Error("Connection timed out"));
      }, 5000);
      sock.on("connect", () => { clearTimeout(timer); resolve(sock); });
      sock.on("error", (err) => { clearTimeout(timer); reject(err); });
      sock.connect({ path: sockPath });
    });
    return new TestClient(socket);
  }

  /** 连接并完成初始化握手；按需声明 cxb 协议版本 1，默认握手保持不变。 */
  static async connectAndInit(sockPath: string, opts?: { cxb?: boolean }): Promise<TestClient> {
    const client = await TestClient.connect(sockPath);
    const result = await client.request("initialize", {
      clientInfo: { name: "test", title: null, version: "0.0.1" },
      capabilities: { experimentalApi: false, ...(opts?.cxb ? { cxb: { protocolVersion: 1 } } : {}) },
    }) as { userAgent: string };
    client.send({ method: "initialized" });
    return client;
  }

  private dispatch(msg: Record<string, unknown>): void {
    // 响应包含 id 和 result 或 error，不包含 method。
    if (msg.id !== undefined && !("method" in msg)) {
      const entry = this.pending.get(msg.id as string | number);
      if (entry) {
        this.pending.delete(msg.id as string | number);
        if ("error" in msg) {
          const err = msg.error as { code: number; message: string };
          const error = new Error(err.message) as Error & { code: number };
          error.code = err.code;
          entry.reject(error);
        } else {
          entry.resolve(msg.result);
        }
      }
      return;
    }

    // 服务器请求同时包含 id 和 method。
    if (msg.id !== undefined && "method" in msg) {
      for (const h of this.requestHandlers) h(msg);
      return;
    }

    // 通知包含 method，不包含 id。
    if ("method" in msg && msg.id === undefined) {
      for (const h of this.notificationHandlers) h(msg);
    }
  }

  send(msg: Record<string, unknown>): void {
    this.socket.write(JSON.stringify(msg) + "\n");
  }

  async request(method: string, params?: unknown, timeoutMs = 10_000): Promise<unknown> {
    return new Promise((resolve, reject) => {
      const id = this.nextId++;
      const msg: Record<string, unknown> = { id, method };
      if (params !== undefined) msg.params = params;
      this.pending.set(id, { resolve, reject });
      this.send(msg);
      // 普通请求默认 10 秒；停止等有协议总期限的调用可显式覆盖。
      setTimeout(() => {
        if (this.pending.has(id)) {
          this.pending.delete(id);
          reject(new Error(`Request ${method} (id=${id}) timed out`));
        }
      }, timeoutMs);
    });
  }

  onNotification(handler: (msg: Record<string, unknown>) => void): void {
    this.notificationHandlers.push(handler);
  }

  onRequest(handler: (msg: Record<string, unknown>) => void): void {
    this.requestHandlers.push(handler);
  }

  get messages(): Array<Record<string, unknown>> {
    return this.allMessages;
  }

  async close(): Promise<void> {
    this.socket.end();
    await new Promise<void>((resolve) => {
      this.socket.on("close", resolve);
      if (this.socket.destroyed) resolve();
      setTimeout(resolve, 1000);
    });
  }

  get destroyed(): boolean {
    return this.socket.destroyed;
  }
}

/** 把客户端通知收集到数组中，并返回该数组引用。 */
export function collectNotifications(
  client: TestClient,
): Array<Record<string, unknown>> {
  const collected: Array<Record<string, unknown>> = [];
  client.onNotification((msg) => collected.push(msg));
  return collected;
}

/** 在超时前等待条件成立。 */
export async function waitFor(
  condFn: () => boolean,
  timeoutMs = 5000,
  pollMs = 20,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (condFn()) return;
    await new Promise((r) => setTimeout(r, pollMs));
  }
  throw new Error("waitFor timed out");
}

export async function exitsWithin(proc: Subprocess, timeoutMs: number): Promise<boolean> {
  return Promise.race([
    proc.exited.then(() => true),
    new Promise<false>((resolve) => setTimeout(() => resolve(false), timeoutMs)),
  ]);
}
