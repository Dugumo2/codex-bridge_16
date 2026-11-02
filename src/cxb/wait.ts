import { showSubagents } from "./subagents";
import { convergenceFields, resultTurn, selectedResultFields } from "./run-result";
import { convergenceLines, observeConvergence } from "./convergence";
import { observeUpstreamErrors, upstreamResultFields } from "./upstream-errors";
import { CxbError, EXIT, rpcFailure, type BrokerRpc, type InboxItem, type InboxWaitReply, type RunResult, type ThreadSummary } from "./contracts";
import { presentItem, safeLine } from "./inbox";
import { commandSession } from "./command-session";
import type { CommandContext, CommandHandler } from "./main";
import { readRunRecord, readThreadIndex } from "./ledger";
import { renderPending } from "./server-requests";
import { resolveThreadViaBroker } from "./thread-ref";
import { bounded, exitCodeForRun, waitRun } from "./wait-run";
import { isUnconfirmedRun } from "./run-diagnostics";
import { observeWaitTree } from "./wait-tree";
import { readThreadResults } from "./thread-result-reader";
import { treeResultText } from "./subagent-tree";
import type { TreeSnapshot } from "./tree-contracts";

const MAX_WAIT_MS = 86_400_000;

async function waitInbox(ctx: CommandContext, rpc: BrokerRpc, deadline: number, id?: string) {
  for (;;) {
    const remaining = Math.ceil(deadline - performance.now());
    if (remaining <= 0) throw new CxbError("TIMEOUT", "等待事项超时", { id });
    const timeoutMs = Math.min(MAX_WAIT_MS, remaining);
    let event: InboxWaitReply;
    try { event = await rpc.request<InboxWaitReply>("broker/inbox/wait", { ...(id ? { id } : {}), timeoutMs }, { timeoutMs: timeoutMs + 5000 }); }
    catch (cause) {
      if (["notFound", "expired", "cancelled"].includes(rpcFailure(cause).reason ?? "")) throw new CxbError("NOT_FOUND", "事项不存在、已过期或已撤销", { id, cause });
      throw cause;
    }
    // 24 小时分段到期还不能结束整个长等待。
    if (event.event === "timeout") {
      if (remaining > MAX_WAIT_MS) continue;
      throw new CxbError("TIMEOUT", "等待事项超时", { id });
    }
    if (id) {
      if (event.event === "expired" || event.event === "cancelled") throw new CxbError("NOT_FOUND", "事项已过期或已撤销", { id });
      if (event.event !== "resolved") continue;
      ctx.out.result(`事项 ${safeLine(id)} 已回复。`);
      ctx.out.resultJson({ ok: true, command: "wait", waitFor: "inbox", id, event: "resolved", resolvedKind: event.item?.kind ?? null });
      return EXIT.OK;
    }
    if (event.event === "idle") {
      ctx.out.result("当前工作区没有活动运行或待处理事项。");
      ctx.out.resultJson({ ok: true, command: "wait", waitFor: "inbox", event: "idle" });
      return EXIT.IDLE;
    }
    // 其他运行结束、事项已答、过期或撤销均只唤醒等待，不是无 ID 等待的结果。
    if (event.event !== "added" || !event.item) continue;
    const item = event.item;
    const { threads } = await rpc.request<{ threads: ThreadSummary[] }>("broker/threads/list", {});
    const shortId = threads.find((t) => t.threadId === item.threadId)?.shortId ?? item.threadId.slice(0, 8);
    const view = presentItem(item, shortId);
    ctx.out.result(`待处理事项：${safeLine(item.id)}（${item.kind}，线程 ${safeLine(shortId)}）\n${view.action}`);
    ctx.out.resultJson({ ok: true, command: "wait", waitFor: "inbox", event: { type: "added", item: view.json } });
    return EXIT.OK;
  }
}

async function waitThread(ctx: CommandContext, rpc: BrokerRpc, signal: AbortSignal, deadline: number, id: string, treeMode = false) {
  const ref = await (treeMode ? bounded(resolveThreadViaBroker(rpc, id), Math.max(0, deadline - performance.now()), signal)
    : resolveThreadViaBroker(rpc, id));
  const label = ref.shortId ?? ref.threadId.slice(0, 8);
  let superseded = false;
  let decided = false;
  let replace!: () => void;
  const replacement = new Promise<{ superseded: true }>((resolve) => { replace = () => resolve({ superseded: true }); });
  // RPC 同批派发先于 await 续体；标志负责终结裁决，Promise 负责唤醒正在等待的 race。
  const offSuperseded = rpc.on("broker/wait/superseded", (raw) => {
    if (decided || (raw as { threadId?: string } | null)?.threadId !== ref.threadId) return;
    superseded = true;
    replace();
  });
  const outputSuperseded = () => {
    decided = true;
    ctx.out.notice("[cxb] 本次等待已被同一线程的新等待接替，已退出。");
    ctx.out.resultJson({ ok: false, command: "wait", waitFor: "thread", threadId: ref.threadId,
      shortId: ref.shortId, event: "superseded", superseded: true });
    return EXIT.IDLE;
  };
  let snapshot: { thread: ThreadSummary; run: RunResult | null; pending: InboxItem[]; revision?: number } & import("./sa1b-contracts").ConvergenceSummary & TreeSnapshot | undefined;
  let runId: string | null = null;
  const liveNotice = (line: string) => { if (!treeMode || !ctx.parsed.globals.json || ctx.parsed.globals.verbose) ctx.out.notice(line); };
  const tree = treeMode ? observeWaitTree(rpc, { threadId: ref.threadId, shortId: ref.shortId, deadline, signal,
    notice: liveNotice, shortIds: () => {
      try { return new Map(readThreadIndex(ctx.workspace.workspaceDir).map((entry) => [entry.threadId, entry.shortId])); }
      catch { return new Map(); }
    } }) : undefined;
  const convergence = observeConvergence(rpc, () => runId, liveNotice);
  const upstream = observeUpstreamErrors(convergence.rpc, () => runId, (line) => ctx.out.notice(line));
  const buffered: InboxItem[] = [];
  let found!: (items: InboxItem[]) => void;
  const pending = new Promise<InboxItem[]>((resolve) => { found = resolve; });
  // 非 guardian 事项只产生在 broker 托管的线程上（详细设计 4.3 节），出现即说明已交接；
  // 不能按快照时的 owner 过滤，否则等待途中由别的客户端交接给 broker 的事项会被漏掉。
  // 也不按 threadId 过滤：review 迁到子线程后，它的事项 threadId 是子线程（本连接会另行订阅子线程，见 followChild）。
  const accept = (item: InboxItem) => {
    if (decided) return;
    if (item.runId === runId && item.kind !== "guardian") found([item]);
  };
  const off = rpc.on("broker/inbox/added", (raw) => {
    const item = (raw as { item?: InboxItem })?.item;
    if (!item) return;
    if (!snapshot) buffered.push(item); else accept(item);
  });
  let failed!: (error: unknown) => void;
  const failure = new Promise<never>((_, reject) => { failed = reject; });
  void failure.catch(() => undefined);
  // review 迁到独立子线程后，它的事项只发给子线程的 owner 与 watcher（任务说明 5.2）：同一连接订阅子线程，
  // 并检查子线程快照里仍未决的事项（覆盖迁移到订阅之间出现的事项）。
  const watchedChildren = new Set<string>();
  const followChild = (child: string | null | undefined) => {
    if (decided) return;
    if (!child || child === ref.threadId || watchedChildren.has(child)) return;
    watchedChildren.add(child);
    rpc.request<{ pending: InboxItem[] }>("broker/threads/watch", { threadId: child }).then((view) => {
      for (const item of view.pending ?? []) accept(item);
    }, failed);
  };
  const moves: Array<{ runId?: string; reviewThreadId?: string }> = [];
  const offMoved = rpc.on("broker/run/moved", (raw) => {
    const moved = raw as { runId?: string; reviewThreadId?: string } | null;
    if (!moved) return;
    if (!snapshot) moves.push(moved); else if (moved.runId === runId) followChild(moved.reviewThreadId);
  });
  const controller = new AbortController();
  const abort = () => controller.abort();
  signal.addEventListener("abort", abort, { once: true });
  if (signal.aborted) abort();
  let running: Promise<RunResult> | undefined;
  const outputPending = (items: InboxItem[]) => {
    if (superseded) return outputSuperseded();
    decided = true;
    const rendered = renderPending(items, { threadId: ref.threadId, shortId: label });
    ctx.out.result(rendered.text);
    for (const line of convergenceLines(convergence.fields(), items.length)) ctx.out.notice(line);
    ctx.out.resultJson({ ...rendered.json as object, ...convergence.fields(), ...upstream.fields(),
      ...(tree ? { tree: tree.current(snapshot?.run) } : {}) });
    return EXIT.WAITING_RESPONSE;
  };
  try {
    const initialWatch = rpc.request<NonNullable<typeof snapshot>>("broker/threads/watch", { threadId: ref.threadId, exclusiveWaiter: true });
    snapshot = await (treeMode ? bounded(initialWatch, Math.max(0, deadline - performance.now()), signal) : initialWatch);
    if (superseded) return outputSuperseded();
    runId = snapshot!.run?.runId ?? snapshot!.thread.lastRunId;
    convergence.accept({ ...snapshot!.run, ...snapshot, runId: runId ?? undefined });
    if (snapshot!.run) snapshot!.run = upstream.apply(snapshot!.run);
    else upstream.refresh();
    if (!runId) throw new CxbError("NOT_FOUND", "线程没有运行记录", { id });
    if (tree) {
      const recorded = snapshot!.run ?? readRunRecord(ctx.workspace.workspaceDir, runId);
      tree.initialize(runId, { ...snapshot, run: recorded }, recorded?.state === "ended" || recorded?.state === "lost");
    }
    // 快照、缓冲、实时三处事项统一只认目标运行（终审讨论：别的运行的事项不能让 wait 误退 5）。
    const initial = snapshot!.pending.filter((i) => i.kind !== "guardian" && i.runId === runId);
    if (initial.length) return outputPending(initial);
    for (const item of buffered) accept(item);
    buffered.length = 0;
    followChild(snapshot!.run?.runId === runId ? snapshot!.run?.reviewThreadId : null);
    for (const moved of moves) if (moved.runId === runId) followChild(moved.reviewThreadId);
    moves.length = 0;
    const atSnapshot = snapshot!.run ?? readRunRecord(ctx.workspace.workspaceDir, runId);
    const alreadyDone = atSnapshot?.state === "ended" || atSnapshot?.state === "lost";
    running = waitRun(upstream.rpc, { threadId: ref.threadId, runId, workspaceDir: ctx.workspace.workspaceDir,
      timeoutMs: Math.max(0, deadline - performance.now()), signal: controller.signal,
      log: (text) => ctx.out.progress(text) });
    const winner = await Promise.race([pending.then((items) => ({ items })), running.then((run) => ({ run })), failure, replacement]);
    if (superseded || "superseded" in winner) return outputSuperseded();
    if ("items" in winner) return outputPending(winner.items);
    // 从这里开始已选定结果出口；输出期间及 finally 收束期间的迟到通知不再改变结果。
    decided = true;
    const run = upstream.apply(winner.run);
    const code = exitCodeForRun(run);
    // 树模式的等待裁决到此结束。补读与总等待分阶段，原始期限只限制补读预算。
    // 观察器先冻结，迟到事项、接替、快照和正文回包都不能改变已选出口。
    if (tree) {
      if (code === EXIT.CODEX_FAILED) upstream.failure(run);
      tree.dispose(); convergence.dispose(); upstream.dispose();
    }
    if (isUnconfirmedRun(run)) {
      if (code === EXIT.CODEX_FAILED) upstream.failure(run);
      ctx.out.resultJson({ ok: false, command: "wait", waitFor: "thread", threadId: ref.threadId, shortId: ref.shortId,
        runId, state: run.state, status: run.status, stopOutcome: run.stopOutcome, interruptReason: run.interruptReason,
        stopDiagnostics: null, ...convergenceFields(run), ...selectedResultFields(run), ...upstreamResultFields(run),
        ...(tree ? { tree: tree.current(run) } : {}) });
      return code;
    }
    showSubagents(ctx, run, treeMode ? { tree: true } : undefined);
    if (code === EXIT.CODEX_FAILED) {
      if (run.upstreamError || resultTurn(run)?.error != null) ctx.out.notice("Codex 执行失败，请用 cxb show 查看运行结果");
      upstream.failure(run);
    }
    ctx.out.result(resultTurn(run)?.output ?? "");
    let finalTree = tree?.current(run, "terminal");
    if (finalTree) {
      const results = await readThreadResults(rpc, finalTree.nodes, { deadline, signal });
      // 只读结果补充失败不改变父结果。取消仍由命令会话负责资源收束。
      finalTree = { ...finalTree, nodes: finalTree.nodes.map((node) => ({ ...node, result: results.get(node.threadId) ?? node.result })) };
      finalTree.outputCoverage = { total: 0, available: 0, truncated: 0, unavailable: 0, omitted: 0 };
      for (const node of finalTree.nodes) if (node.relationKind === "subagent" && node.result) {
        finalTree.outputCoverage.total++;
        finalTree.outputCoverage[node.result.availability]++;
      }
      ctx.out.result(`\n${treeResultText(finalTree)}`);
    }
    ctx.out.resultJson({ ok: code === EXIT.OK, command: "wait", waitFor: "thread", threadId: ref.threadId,
      shortId: ref.shortId, runId, status: run.status, result: resultTurn(run)?.output ?? "", alreadyDone, ...convergenceFields(run), ...selectedResultFields(run), ...upstreamResultFields(run),
      ...((run.subagentJoin || run.interruptReason === "stallTimeout" || run.interruptReason === "upstreamAccessError") && run.interruptReason ? { interruptReason: run.interruptReason } : {}),
      ...(run.loopGuard ? { loopGuard: run.loopGuard } : {}),
      // 子代理停止报告与中断原因（subagentsUnfinished 等）原样带出；没有子代理时字段缺省，旧输出不变。
      ...(run.backgroundCleanup ? { backgroundCleanup: run.backgroundCleanup } : {}),
      ...(run.childrenStop ? { interruptReason: run.interruptReason, childrenStop: run.childrenStop } : {}),
      ...(finalTree ? { tree: finalTree } : {}) });
    return code;
  } catch (error) {
    if (!decided && superseded) return outputSuperseded();
    decided = true;
    throw error;
  } finally {
    decided = true;
    offSuperseded(); off(); offMoved(); tree?.dispose(); convergence.dispose(); upstream.dispose(); signal.removeEventListener("abort", abort); controller.abort();
    // 交接胜出时仍等取消收束，避免遗留 waitRun 的订阅和计时器。
    await running?.catch(() => undefined);
  }
}

export const wait: CommandHandler = async (ctx) => {
  const scope = commandSession(ctx);
  const started = performance.now();
  const timeoutMs = ctx.parsed.globals.timeout * 1000;
  const deadline = started + timeoutMs;
  try {
    const rpc = await bounded(scope.connect(), timeoutMs, scope.signal);
    const id = ctx.parsed.positional[0];
    if (!id || /^(ap_|qu_|mc_|gd_)/.test(id)) return await waitInbox(ctx, rpc, deadline, id);
    if (ctx.parsed.options.tree) return await waitThread(ctx, rpc, scope.signal, deadline, id, true);
    return await bounded(waitThread(ctx, rpc, scope.signal, deadline, id), Math.max(0, deadline - performance.now()), scope.signal);
  } finally {
    scope.dispose();
    ctx.out.progress(`等待耗时 ${Math.round(performance.now() - started)} 毫秒`);
  }
};
