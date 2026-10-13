import { childEntry, showChildThread, showSubagents } from "./subagents";
import { sanitizeForTerminal } from "../questions";
import { CxbError, rpcFailure, type BrokerRpc, type RunResult, type RunModelSummary, type ThreadSummary, type TurnResult } from "./contracts";
import { readAllRuns, readRunLog, readThreadIndex, listThreadRuns, type LogLine } from "./ledger";
import { modelLabel, rerouteLabel, runModelDisplay, threadModelDisplay, type ModelDisplay } from "./show-model";
import { lifecycleHint, lifecycleLabel } from "./lifecycle";
import type { CommandContext, CommandHandler } from "./main";
import { redactParams } from "./server-requests";
import { resolveThreadLocal, resolveThreadViaBroker, type ThreadRef } from "./thread-ref";
import { waitRun } from "./wait-run";
import { convergenceFields, resultPending, resultTurn, selectedResultFields } from "./run-result";
import { observeConvergence } from "./convergence";
import { diagnosticLine, observeUpstreamErrors, turnCodexErrorInfo, turnErrorLine, upstreamFields, upstreamShowLines } from "./upstream-errors";
import type { ConvergenceSummary } from "./sa1b-contracts";
import { commandSession, wireMessage } from "./command-session";

function unsupported(error: unknown): boolean {
  if (rpcFailure(error).code === -32601) return true;
  const message = wireMessage(error).trim().toLowerCase();
  return /^(not implemented|method not implemented|unimplemented|method not found|unknown method|未实现|尚未实现|方法未实现)$/.test(message)
    || (/thread\/(items|turns)\/list/.test(message) && /not supported|not implemented|unimplemented|unknown method|method not found/.test(message));
}

const clean = (text: string) => sanitizeForTerminal(text);
const safeSummary = (value: unknown) => clean(JSON.stringify(redactParams(value)) ?? "");
const eventLine = (line: LogLine) => ({ ts: line.ts, kind: clean(line.event), text: clean(line.text) });
function tail(ctx: CommandContext, ref: ThreadRef) {
  return (ref.shortId ? readRunLog(ctx.workspace.workspaceDir, ref.shortId) ?? [] : [])
    .slice(-Number(ctx.parsed.options.limit)).map(eventLine);
}
function printLine(ctx: CommandContext, line: { ts: string; kind: string; text: string }) {
  ctx.out.result(`${line.ts} ${line.text}\n`);
  ctx.out.resultJson(line);
}

function modelNotice(ctx: CommandContext, model: ModelDisplay) {
  ctx.out.notice(clean(`模型：${modelLabel(model)}；请求强度：${model.reasoning ?? "-"}`));
}

/** 所有 show 模式共享续跑的候选集合；正文仍按各模式原有来源读取。 */
async function readThreadModel(ctx: CommandContext, ref: ThreadRef, connect: () => Promise<BrokerRpc>,
  indexModel: string | null, disk = readAllRuns(ctx.workspace.workspaceDir)): Promise<ModelDisplay> {
  let online: RunModelSummary[] = [];
  try {
    const rpc = await connect();
    online = (await rpc.request<{ runs: RunModelSummary[] }>("broker/runs/modelSummaries", { threadIds: [ref.threadId] })).runs;
  } catch (error) {
    if (error instanceof CxbError && error.code === "INTERRUPTED") throw error;
    ctx.out.notice("无法连接 broker 或读取在线运行摘要，模型来源降级为本地账本");
  }
  return threadModelDisplay(ref.threadId, indexModel, disk, online);
}

function resultNotice(ctx: CommandContext, run: RunResult, id: string) {
  showSubagents(ctx, run);
  if (run.childrenStop?.cleanupState === "abandoned") return;
  if (run.state === "lost" || run.stopOutcome === "unconfirmed") {
    ctx.out.notice(`停止中（未确认），先 cxb stop ${id} 核实`);
  } else if (run.status === "interrupted") {
    const reasons: Record<NonNullable<RunResult["interruptReason"]>, string> = {
      budgetExhausted: "原运行预算已耗尽",
      budgetIncomplete: "预算证据不完整",
      convergeDeadline: "子树收敛期限已到",
      wakeLimit: "汇总轮次数已用尽",
      autoTurnLimit: "自动父轮次数已用尽",
      discoveryIncomplete: "子树发现不完整",
      ownershipConflict: "轮次归属存在冲突",
      wakeUnknown: "汇总启动结果不明",
      wakeFailed: "汇总启动失败",
      goalInactive: "目标禁止自动续轮",
      rootNoDirectInput: "根线程不能直接输入",
      abandoned: "已放弃子树清理责任",
      stallTimeout: "无进展超时", upstreamAccessError: "上游访问报错即停",
      clientDisconnected: "客户端断开", stop: "主动停止", brokerLost: "broker 失联", goalLoop: "空转刹车", subagentsUnfinished: "子代理未完成" };
    ctx.out.notice(`已中断（${run.interruptReason ? reasons[run.interruptReason] : "服务器中断"}），可继续：cxb run --resume ${id} --continue`);
  }
}

function showUpstream(ctx: CommandContext, run: RunResult, output = "") {
  const lines = upstreamShowLines(run);
  if (lines.length) ctx.out.result(`${output && !output.endsWith("\n") ? "\n" : ""}${lines.join("\n")}\n`);
}

async function localResult(ctx: CommandContext, ref: ThreadRef, connect: () => Promise<BrokerRpc>) {
  const index = readThreadIndex(ctx.workspace.workspaceDir).find((entry) => entry.threadId === ref.threadId)!;
  const disk = readAllRuns(ctx.workspace.workspaceDir);
  const run = disk.find((record) => record.runId === index.lastRunId);
  const model = await readThreadModel(ctx, ref, connect, index.model, disk);
  modelNotice(ctx, model);
  if (!run) throw new CxbError("NOT_FOUND", "线程没有运行记录", { id: ref.threadId });
  const last = resultTurn(run);
  resultNotice(ctx, run, ref.shortId ?? ref.threadId);
  if (run.state === "running" || run.state === "stopping" || resultPending(run)) {
    const stage = run.subagentJoin && run.finalTurnId === undefined && last?.ready === "ready" ? last : undefined;
    if (stage) { ctx.out.notice("阶段结果（运行尚未完成）"); ctx.out.result(stage.output); }
    ctx.out.notice(`结果尚未就绪，请用 cxb wait ${ref.shortId ?? ref.threadId}`);
    showUpstream(ctx, run, stage?.output);
    ctx.out.resultJson({ ok: true, command: "show", ...ref, ...model, mode: "result", runId: run.runId, state: run.state,
      ...convergenceFields(run), ...selectedResultFields(run), ...upstreamFields(run), ...(stage ? { result: stage.output, stageResult: true } : {}),
      stopOutcome: run.stopOutcome, childrenStop: run.childrenStop, backgroundCleanup: run.backgroundCleanup });
    return 5 as const;
  }
  ctx.out.result(last?.output ?? "");
  showUpstream(ctx, run, last?.output);
  ctx.out.resultJson({ ok: true, command: "show", ...ref, ...model, mode: "result", runId: run.runId, state: run.state,
    status: run.status, ready: last?.ready ?? null, result: last?.output ?? "", outputSource: last?.outputSource ?? "none", ...convergenceFields(run), ...selectedResultFields(run), ...upstreamFields(run),
    childrenStop: run.childrenStop, backgroundCleanup: run.backgroundCleanup, interruptReason: run.interruptReason, stopOutcome: run.stopOutcome, continuedFrom: run.continuedFrom, updatedAt: index.updatedAt });
  if (!last) {
    const failure = ref.shortId ? readRunLog(ctx.workspace.workspaceDir, ref.shortId)
      ?.filter((line) => line.runId === run.runId && line.event === "启动失败").at(-1) : null;
    if (run.subagentJoin) ctx.out.notice("最终结果不可用（unavailable）");
    else ctx.out.notice(failure ? String(redactParams(failure.text)) : "运行启动失败，没有生成轮次");
  }
  ctx.out.progress(`结果来自本地；运行 ${run.runId}，状态 ${run.state}/${run.status ?? "未知"}`);
  return 0 as const;
}

interface ServerItem { type: string; text?: string; phase?: string; review?: string }
interface ServerTurn { id: string; status: string; items: ServerItem[]; error?: unknown }
/** 与 broker 的输出优先级一致；保持条目顺序。 */
export function serverOutput(items: ServerItem[]): { output: string; outputSource: TurnResult["outputSource"] } {
  const review = items.filter((item) => item.type === "exitedReviewMode" && typeof item.review === "string").at(-1)?.review;
  const agents = items.filter((item) => item.type === "agentMessage" && typeof item.text === "string" && item.text.length > 0);
  const finals = agents.filter((item) => item.phase === "final_answer").map((item) => item.text!);
  if (review) return { output: review, outputSource: "review" };
  if (finals.length) return { output: finals.join("\n"), outputSource: "finalAnswer" };
  return { output: agents.map((item) => item.text!).join("\n"), outputSource: agents.length ? "accumulated" : "none" };
}

const TURN_STATUSES = new Set(["inProgress", "completed", "failed", "interrupted"]);
/** 轮次分页的一页：data 必须是数组；空页不得带续页游标；轮次要有非空 id 与四种状态之一。 */
function checkedTurn(page: { data?: unknown; nextCursor?: unknown }): ServerTurn | undefined {
  if (!Array.isArray(page?.data) || !(page.nextCursor === null || typeof page.nextCursor === "string")
    || (page.data.length === 0 && page.nextCursor !== null)) throw new CxbError("CODEX_FAILED", "服务器轮次分页回包损坏");
  const turn = page.data[0] as ServerTurn | undefined;
  if (turn !== undefined && (typeof turn?.id !== "string" || !turn.id || !TURN_STATUSES.has(turn.status))) {
    throw new CxbError("CODEX_FAILED", "服务器轮次分页回包损坏");
  }
  return turn;
}

/** 最新一轮的全部条目：按 turnId 分页到 nextCursor 为 null；畸形或游标不前进都明确失败，不交付部分结果。 */
async function turnItems(rpc: BrokerRpc, threadId: string, turnId: string): Promise<ServerItem[]> {
  const items: ServerItem[] = [];
  const seen = new Set<string>();
  let cursor: string | null = null;
  do {
    const page: { data: Array<{ turnId: string; item: ServerItem }>; nextCursor: string | null } = await rpc.request("thread/items/list", {
      threadId, turnId, cursor, limit: 64, sortDirection: "asc",
    });
    if (!Array.isArray(page.data) || !(page.nextCursor === null || typeof page.nextCursor === "string")
      || page.data.some((entry) => entry.turnId !== turnId || !entry.item || typeof entry.item.type !== "string")) {
      throw new CxbError("CODEX_FAILED", "服务器条目分页回包损坏");
    }
    items.push(...page.data.map((entry) => entry.item));
    cursor = page.nextCursor;
    if (cursor !== null && (!cursor || seen.has(cursor) || !page.data.length)) throw new CxbError("CODEX_FAILED", "服务器条目分页游标没有前进");
    if (cursor !== null) seen.add(cursor);
  } while (cursor !== null);
  return items;
}

/**
 * 条目分页不支持（legacy 线程）时的单轮兼容读：同一请求改要 full 视图，只取最新一轮并核对编号。
 * 单轮仍可能很大，但不再整线程回退（H2 接口第 6 节）。
 */
async function legacyTurnItems(rpc: BrokerRpc, threadId: string, turnId: string): Promise<ServerItem[]> {
  const page = await rpc.request<{ data?: unknown; nextCursor?: unknown }>("thread/turns/list", {
    threadId, cursor: null, limit: 1, sortDirection: "desc", itemsView: "full",
  });
  const turn = checkedTurn(page) as (ServerTurn & { itemsView?: unknown }) | undefined;
  if (!turn || turn.id !== turnId) throw new CxbError("CODEX_FAILED", "服务器最新轮次在读取期间变化，请重试");
  if (turn.itemsView !== "full" || !Array.isArray(turn.items)
    || turn.items.some((item) => !item || typeof item.type !== "string")) {
    throw new CxbError("CODEX_FAILED", "服务器没有给出最新轮次的完整条目");
  }
  return turn.items;
}

/**
 * `show --server`：从服务器读最新一轮。首个轮次请求只要 notLoaded 元数据；条目经 items/list 分页。
 * 轮次分页与条目分页的能力分开降级：轮次分页不支持（旧服务器，或无法证明的第一轮前）明确失败；
 * 只有条目分页不支持时改用单轮完整读。拿不到结果时明确失败，不伪装成空结果，也不整线程回退。
 */
async function serverResult(ctx: CommandContext, rpc: BrokerRpc, ref: ThreadRef, indexModel: string | null = null) {
  let turn: ServerTurn | undefined;
  try {
    turn = checkedTurn(await rpc.request("thread/turns/list", {
      threadId: ref.threadId, cursor: null, limit: 1, sortDirection: "desc", itemsView: "notLoaded",
    }));
  } catch (error) {
    if (!unsupported(error)) throw error;
    throw new CxbError("CODEX_FAILED", "服务器不支持轮次分页（thread/turns/list），无法从服务器读取结果；去掉 --server 可查看本地结果");
  }
  if (turn) {
    let items: ServerItem[];
    try {
      items = await turnItems(rpc, ref.threadId, turn.id);
    } catch (error) {
      if (!unsupported(error)) throw error;
      ctx.out.progress("服务器不支持条目分页，改用单轮完整读取");
      items = await legacyTurnItems(rpc, ref.threadId, turn.id);
    }
    turn = { ...turn, items };
  }
  const result = serverOutput(turn?.items ?? []);
  const model = await readThreadModel(ctx, ref, async () => rpc, indexModel);
  modelNotice(ctx, model);
  ctx.out.result(result.output);
  if (turn?.status === "failed" && turn.error != null) {
    ctx.out.result(`${result.output && !result.output.endsWith("\n") ? "\n" : ""}错误：${turnErrorLine(turn.error)}\n`);
    const info = turnCodexErrorInfo(turn.error);
    if (info != null) ctx.out.result(`codexErrorInfo：${diagnosticLine(info)}\n`);
  }
  ctx.out.resultJson({ ok: true, command: "show", ...ref, ...model, mode: "result", runId: null, status: turn?.status ?? null,
    result: result.output, outputSource: result.outputSource, updatedAt: null,
    ...(turn?.status === "failed" && turn.error != null ? { error: redactParams(turn.error) } : {}) });
  ctx.out.progress("结果来自服务器");
  return 0 as const;
}

/** 不带 --full 时显示的事件（任务说明 N7-A 2.1 节的清单去掉逐条目进展）。 */
const SUMMARY_EVENTS = new Set(["运行开始", "运行结束", "轮次开始", "轮次完成", "停止发起", "停止确认", "空转刹车", "空转刹车暂停目标",
  "启动失败", "交接撤销", "丢行"]);

/** 属于 runId 的行，外加夹在它首末行之间的丢行标记（丢掉的行可能属于它）。 */
function linesOfRun(all: LogLine[], runId: string): LogLine[] {
  const first = all.findIndex((line) => line.runId === runId);
  if (first < 0) return [];
  const last = all.findLastIndex((line) => line.runId === runId);
  return all.slice(first, last + 1).filter((line) => line.runId === runId || line.runId === null);
}

async function showLog(ctx: CommandContext, ref: ThreadRef, connect: () => Promise<BrokerRpc>) {
  const all = ref.shortId ? readRunLog(ctx.workspace.workspaceDir, ref.shortId) ?? [] : [];
  const records = listThreadRuns(ctx.workspace.workspaceDir, ref.threadId);
  const index = readThreadIndex(ctx.workspace.workspaceDir).find((entry) => entry.threadId === ref.threadId);
  const model = await readThreadModel(ctx, ref, connect, index?.model ?? null, records);
  modelNotice(ctx, model);
  const runs = records.map((run) => {
    const lines = linesOfRun(all, run.runId).filter((line) => ctx.parsed.options.full || SUMMARY_EVENTS.has(line.event)).map(eventLine);
    const entry = { runId: run.runId, state: run.state, status: run.status, startedAt: run.startedAt,
      ...runModelDisplay(run), ...convergenceFields(run), ...upstreamFields(run), pendingReroute: run.pendingReroute ?? null,
      input: clean(run.input), items: lines, turns: run.turns.map((turn) => ({ ...turn, output: clean(turn.output), error: redactParams(turn.error) })),
      ...(ctx.parsed.options.full ? { meta: redactParams(run.meta), interruptReason: run.interruptReason, stopOutcome: run.stopOutcome,
        progress: redactParams(run.progress) } : {}) };
    ctx.out.result(`运行 ${run.runId} ${run.state}/${run.status ?? "未知"} ${new Date(run.startedAt).toISOString()}\n用户：${entry.input}\n`);
    showUpstream(ctx, run);
    ctx.out.result(clean(`请求模型：${entry.model ?? "-"}；请求强度：${entry.reasoning ?? "-"}\n`));
    for (const line of lines) ctx.out.result(`${line.ts} ${line.text}\n`);
    for (const turn of entry.turns) {
      if (turn.reroute) ctx.out.result(clean(`轮次 ${turn.turnId}${rerouteLabel(turn.reroute)}：${turn.reroute.fromModel} → ${turn.reroute.toModel}；原因：${turn.reroute.reason}\n`));
      ctx.out.result(`Codex：${turn.output}\n`);
      if (turn.error != null) ctx.out.result(`错误：${turnErrorLine(turn.error)}\n`);
    }
    if (entry.pendingReroute) ctx.out.result(clean(`运行中轮次 ${entry.pendingReroute.turnId}${rerouteLabel(entry.pendingReroute)}：${entry.pendingReroute.fromModel} → ${entry.pendingReroute.toModel}；原因：${entry.pendingReroute.reason}\n`));
    if (ctx.parsed.options.full) ctx.out.result(`元数据：${safeSummary({ meta: run.meta, interruptReason: run.interruptReason,
      stopOutcome: run.stopOutcome, progress: run.progress })}\n`);
    return entry;
  });
  ctx.out.resultJson({ ok: true, command: "show", ...ref, ...model, mode: "log", runs });
  return 0 as const;
}

interface Snapshot extends ConvergenceSummary { thread: ThreadSummary; run: RunResult | null; revision?: number }
const PROGRESS_LABELS: Record<string, string> = { "turn/started": "轮次开始", "turn/completed": "轮次完成", "item/started": "条目开始", "item/completed": "条目完成" };
/** 上游进展通知 → 显示行；只认 `source` 线程上的（null 表示还不知道该认哪个线程，一律丢弃）。 */
function renderNotification(method: string, raw: unknown, source: string | null) {
  const p = raw as { threadId?: string; turn?: { id?: string }; item?: { type?: string; id?: string; text?: string } } | null;
  const label = PROGRESS_LABELS[method];
  if (!label || !source || p?.threadId !== source) return null;
  return { ts: new Date().toISOString(), kind: method, text: clean(`${label} ${p.turn?.id ?? p.item?.type ?? ""} ${p.item?.id ?? ""} ${p.item?.text ?? ""}`.trim()) };
}

/** 本地日志里属于运行 runId 的行（行内带运行编号；迁走的 review 与父线程上的新运行交错写同一文件也分得开）。 */
function runLogLines(ctx: CommandContext, shortId: string | null, runId: string): LogLine[] {
  return (shortId ? readRunLog(ctx.workspace.workspaceDir, shortId) ?? [] : []).filter((line) => line.runId === runId);
}

/**
 * `show --follow`：尽力而为的实时进展（完整进展以 `show --log` 为准，任务说明 5.4）。
 * 先只显示本线程 P 上的通知（R 迁移前占着 P 的认领）；R 是迁到独立子线程 C 的 review 时改为只显示 C 上的通知：
 * 从快照的 reviewThreadId 或 `broker/run/moved` 得知 C 后，在同一连接上 watch C；订阅生效前 C 的事件先缓冲，
 * 订阅后读本地日志里属于 R、不早于已显示最后一行且未显示过的行补上（GitHub 式补发；日志异步写盘，可能多或少一行），再排出缓冲。
 * R 结束后（父线程上的结束通知或快照；迁移后的结束通知也发给父线程的 watcher）不再接收新的原始进展：
 * 子线程空出后可能被别的运行接着用，上游通知不带运行编号；
 * 之后只等 R 的结果补齐。终态等子线程订阅与补发做完再输出。
 */
async function follow(ctx: CommandContext, rpc: BrokerRpc, ref: ThreadRef, signal: AbortSignal) {
  const buffered: Array<{ method: string; params: unknown }> = [];
  let live = false;
  /** 当前认的进展来源线程；切到 review 子线程、订阅完成前为 null。 */
  let source: string | null = null;
  let childPending: Array<{ method: string; params: unknown }> | null = null;
  /** 子线程订阅与补发；终态前要等它做完。 */
  let switching: Promise<void> | null = null;
  /** R 已结束：不再接收原始进展。 */
  let ended = false;
  /** follow 已返回：迟到的回调不再输出。 */
  let finished = false;
  let snapshot: Snapshot | undefined;
  let runId: string | null = null;
  const convergence = observeConvergence(rpc, () => runId, (line) => ctx.out.notice(line));
  const upstream = observeUpstreamErrors(convergence.rpc, () => runId, (line) => ctx.out.notice(line));
  /** 已显示行的出现次数（同一毫秒可以有内容相同的多行）。 */
  const printed = new Map<string, number>();
  let lastTs = "";
  const emit = (line: { ts: string; kind: string; text: string }) => {
    const key = `${line.ts} ${line.text}`;
    printed.set(key, (printed.get(key) ?? 0) + 1);
    if (line.ts > lastTs) lastTs = line.ts;
    printLine(ctx, line);
  };
  const isEnd = (run: RunResult | null | undefined) => run?.runId === runId && (run.state === "ended" || run.state === "lost");
  let reject!: (error: unknown) => void;
  const failure = new Promise<never>((_, fail) => { reject = fail; });
  void failure.catch(() => undefined);
  const switchToChild = (child: string | null | undefined) => {
    if (!child || child === ref.threadId || childPending) return;
    childPending = [];
    source = null;
    switching = rpc.request("broker/threads/watch", { threadId: child }).then(() => {
      // 回包与取消、断线同批到达时，follow 可能已返回。
      if (finished) return;
      // 补发迁移空档：日志里属于 R、不早于补发开始时的水位、尚未显示过的行。水位整批固定，同一毫秒的行按出现次数去重。
      const floor = lastTs;
      const seen = new Map<string, number>();
      for (const line of runId ? runLogLines(ctx, snapshot?.run?.shortId ?? ref.shortId, runId) : []) {
        if (line.ts < floor) continue;
        const shown = eventLine(line);
        const key = `${shown.ts} ${shown.text}`;
        const count = (seen.get(key) ?? 0) + 1;
        seen.set(key, count);
        if (count > (printed.get(key) ?? 0)) emit(shown);
      }
      source = child;
      const queued = childPending ?? [];
      childPending = [];
      for (const event of queued) { const line = renderNotification(event.method, event.params, source); if (line) emit(line); }
    });
    // 终态前等的是 switching 本身：失败必须保留为拒绝，不能用 .catch 的返回值顶替成成功。
    switching.catch(reject);
  };
  const handle = (method: string, params: unknown) => {
    if (method === "broker/run/moved") {
      const moved = params as { runId?: string; reviewThreadId?: string } | null;
      if (runId && moved?.runId === runId) switchToChild(moved.reviewThreadId);
      return;
    }
    if (method === "broker/run/ended") {
      if (isEnd((params as { run?: RunResult } | null)?.run)) ended = true;
      return;
    }
    if (ended) return;
    if (childPending && source === null) {
      // 子线程订阅中：子线程的事件先存着，补完日志再按序输出。
      if ((params as { threadId?: string } | null)?.threadId !== ref.threadId) childPending.push({ method, params });
      return;
    }
    const line = renderNotification(method, params, source);
    if (line) emit(line);
  };
  const off = rpc.onAny((method, params) => {
    try {
      if (!live) buffered.push({ method, params });
      else handle(method, params);
    } catch (error) { reject(error); }
  });
  const close = rpc.onClose(() => reject(new CxbError("BROKER_UNAVAILABLE", "跟随期间 broker 连接断开")));
  try {
    snapshot = await Promise.race([rpc.request<Snapshot>("broker/threads/watch", { threadId: ref.threadId }), failure]);
    runId = snapshot.run?.runId ?? snapshot.thread.lastRunId;
    convergence.accept({ ...snapshot.run, ...snapshot, runId: runId ?? undefined });
    if (snapshot.run) snapshot.run = upstream.apply(snapshot.run);
    else upstream.refresh();
    for (const line of tail(ctx, ref)) emit(line);
    // 先把快照前后同批通知交给等待器，再清空缓冲；不能以 await 返回时刻划分通知新旧。
    const replayRpc: BrokerRpc = { ...upstream.rpc, on(method, handler) {
      const dispose = upstream.rpc.on(method, handler);
      for (const event of buffered) if (event.method === method) handler(event.params);
      return dispose;
    } };
    const ready = snapshot.run && (snapshot.run.state === "ended" || snapshot.run.state === "lost") && !resultPending(snapshot.run);
    const waiting = runId && !ready ? waitRun(replayRpc, { threadId: ref.threadId, runId, timeoutMs: null, signal,
      workspaceDir: ctx.workspace.workspaceDir }) : Promise.resolve(snapshot.run);
    void waiting.catch(() => undefined);
    // 先认 P：R 迁移前占着 P 的认领，P 上不会有别的运行，P 的通知就是 R 的；inline review 不迁移，一直认 P。
    // R 已在（或之后迁到）独立子线程 C 上时改认 C。
    source = ref.threadId;
    switchToChild(snapshot.run?.runId === runId ? snapshot.run?.reviewThreadId : null);
    // 快照已结束：缓冲里有 R 的结束通知时，排在它前面的进展属于 R，按序处理到它再停；
    // 没有则 R 在订阅前就已结束，缓冲里的进展都晚于结束。
    if (isEnd(snapshot.run) && !buffered.some((event) => event.method === "broker/run/ended"
      && isEnd((event.params as { run?: RunResult } | null)?.run))) ended = true;
    live = true;
    for (const event of buffered) handle(event.method, event.params);
    buffered.length = 0;
    const final = await Promise.race([waiting, failure]);
    const run = final ? upstream.apply(final) : final;
    // 订阅子线程的请求有自身期限，取消与断线经 failure 收束。
    if (switching) await Promise.race([switching, failure]);
    const model = await Promise.race([readThreadModel(ctx, ref, async () => rpc, snapshot.thread.model), failure]);
    modelNotice(ctx, model);
    ctx.out.result(`运行结束：${run?.status ?? "无运行"}${runId ? `（${runId}）` : ""}\n`);
    const hasConvergence = run && (run.finalTurnId !== undefined || run.subagentJoin);
    if (hasConvergence) { showSubagents(ctx, run); ctx.out.result(resultTurn(run)?.output ?? ""); }
    ctx.out.resultJson({ terminal: true, status: run?.status ?? null, runId, ...model,
      ...(hasConvergence ? { result: resultTurn(run)?.output ?? "", ...convergenceFields(run), ...selectedResultFields(run) } : {}) });
    return 0 as const;
  } finally { finished = true; off(); close(); convergence.dispose(); upstream.dispose(); buffered.length = 0; }
}

export const show: CommandHandler = async (ctx) => {
  const session = commandSession(ctx);
  try {
    const options = ctx.parsed.options;
    const id = ctx.parsed.positional[0]!;
    if (options.follow || options.server) {
      const rpc = await session.connect();
      const resolved = await resolveThreadViaBroker(rpc, id, { passThrough: !!options.server });
      const lifecycle = resolved.summary?.lifecycle;
      // 待核实的归档或删除照样提示（与本地分支一致）；--server 的 JSON 一并带出记录。
      if (lifecycle) ctx.out.notice(`${lifecycleLabel(lifecycle)}：${lifecycleHint(lifecycle.kind, lifecycle.phase, resolved.shortId ?? resolved.threadId)}`);
      const ref = { threadId: resolved.threadId, shortId: resolved.shortId, ...(lifecycle ? { lifecycle } : {}) };
      return options.follow ? await follow(ctx, rpc, ref, session.signal) : await serverResult(ctx, rpc, ref, resolved.summary?.model ?? null);
    }
    const ref = resolveThreadLocal(ctx.workspace.workspaceDir, id);
    if (ref.lifecycle) {
      ctx.out.notice(`${lifecycleLabel(ref.lifecycle)}：${lifecycleHint(ref.lifecycle.kind, ref.lifecycle.phase, ref.shortId ?? ref.threadId)}`);
      // purge 已删索引、只剩记录：没有本地结果可显示，--json 带出记录供调用方判断。
      if (!readThreadIndex(ctx.workspace.workspaceDir).some((entry) => entry.threadId === ref.threadId)) {
        const model = await readThreadModel(ctx, ref, session.connect, null);
        modelNotice(ctx, model);
        ctx.out.resultJson({ ok: true, command: "show", ...ref, ...model, mode: options.log ? "log" : options.tail ? "tail" : "result", runId: null });
        return 0;
      }
    }
    if (options.log) return await showLog(ctx, ref, session.connect);
    if (options.tail) {
      const items = tail(ctx, ref);
      const index = readThreadIndex(ctx.workspace.workspaceDir).find((entry) => entry.threadId === ref.threadId);
      const model = await readThreadModel(ctx, ref, session.connect, index?.model ?? null);
      modelNotice(ctx, model);
      for (const line of items) ctx.out.result(`${line.ts} ${line.text}\n`);
      ctx.out.resultJson({ ok: true, command: "show", ...ref, ...model, mode: "tail", items, next: null });
      return 0;
    }
    // SA1 第 11 节：没有 RunRecord 的子代理线程，说明血缘后从服务器读它的结果。
    const child = childEntry(ctx.workspace.workspaceDir, ref.threadId);
    if (child && !child.lastRunId) return await showChildThread(ctx, child, async () => serverResult(ctx, await session.connect(), ref, child.model));
    return await localResult(ctx, ref, session.connect);
  } finally { session.dispose(); }
};
