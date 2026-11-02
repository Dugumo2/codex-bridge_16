import type { BrokerRpc, ChildChanged, RunResult } from "./contracts";
import type { TreeSnapshot, TreeView, WatchTreeMember } from "./tree-contracts";
import { buildTree, treeChanges, treeLines } from "./subagent-tree";
import { bounded } from "./wait-run";

/** 通知驱动的树观察器；成员版本和停止版本是两个独立时钟。 */
export function observeWaitTree(rpc: BrokerRpc, options: {
  threadId: string;
  shortId?: string | null;
  shortIds?: () => ReadonlyMap<string, string>;
  notice: (line: string) => void;
  signal?: AbortSignal;
  deadline: number;
}) {
  let active = true;
  let runId: string | null = null;
  let snapshot: TreeSnapshot = {};
  let revision = -1;
  let snapshotRevision = -1;
  let stopVersion = -1;
  let stale = false;
  let silent = false;
  let printed: TreeView | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let inFlight = false;
  let dirty = false;
  const buffered: ChildChanged[] = [];
  // 每个成员只需保存快照之后的最新通知，防止长等待累积全部活动历史。
  const changes = new Map<string, ChildChanged>();
  const controller = new AbortController();
  const abort = () => dispose();

  /** 终态成员来自账本；释放后的在线 revision 不再是这份记录的新鲜度依据。 */
  const terminalRecordReady = (run: RunResult | null | undefined): run is RunResult => {
    if (!run || run.runId !== runId || run.state !== "ended" || !Array.isArray(run.children)) return false;
    const recorded = new Set(run.children.filter((child) => child.relationKind === "subagent").map((child) => child.threadId));
    // 通知中有成员但账本尚未保存时，不能借 released 把缺失事实隐藏掉。
    if (snapshot.children?.members.some((child) => child.relationKind === "subagent" && !recorded.has(child.threadId))) return false;
    const report = run.childrenStop;
    if ((stopVersion >= 0 || run.stopOutcome != null) && (!report || report.version < stopVersion)) return false;
    if (report?.discovery.complete === false) return false;
    // released 由 broker 在发现完整且成员落定后写入；不据此伪造已丢弃的发现版本和在途数量。
    const released = run.status === "completed" && run.subagentJoin?.phase === "released" && !run.subagentJoin.terminalReason
      && run.children.every((child) => child.relationKind !== "subagent" || child.finalState === "settled");
    return released || report?.discovery.complete === true;
  };
  const current = (run?: RunResult | null, view: TreeView["view"] = "partial"): TreeView => {
    if (view === "terminal" && terminalRecordReady(run)) {
      // 不混入旧 pendingCount、冲突、发现状态或在途数量；正文仍按账本绑定集合独立补读。
      return buildTree({ rootThreadId: options.threadId, run, view, shortIds: options.shortIds?.() });
    }
    const stopMissing = view === "terminal" && (stopVersion >= 0 || run?.stopOutcome != null)
      && (!run?.childrenStop || run.childrenStop.version < stopVersion);
    const viewStale = stale || stopMissing;
    const tree = buildTree({ rootThreadId: options.threadId, snapshot, run, view, stale: viewStale,
      shortIds: options.shortIds?.() });
    tree.revision = revision < 0 ? null : revision;
    tree.stopVersion = Math.max(stopVersion, tree.stopVersion ?? -1);
    if (tree.stopVersion < 0) tree.stopVersion = null;
    // 成员通知没有全局数量；尚未补齐时只保留成员事实，不把缓存数量当终态证据。
    if (viewStale) {
      tree.discovery = view === "terminal" ? run?.childrenStop?.discovery ?? null : null;
      tree.inFlightSpawns = null;
    }
    return tree;
  };
  const display = () => {
    if (!active || silent) return;
    const next = current();
    const lines = printed ? treeChanges(printed, next, options.shortId) : treeLines(next, options.shortId);
    printed = next;
    for (const line of lines) options.notice(line);
  };
  const applyMember = (members: Map<string, WatchTreeMember>, event: ChildChanged) => {
    if (!event.threadId) return;
    const before = members.get(event.threadId);
    members.set(event.threadId, { threadId: event.threadId,
      parentThreadId: event.parentThreadId ?? before?.parentThreadId ?? null,
      relationKind: event.relationKind ?? before?.relationKind ?? "subagent",
      activity: event.activity ?? before?.activity ?? "unknown",
      pendingCount: event.pendingCount ?? before?.pendingCount ?? null,
      conflict: before?.conflict ?? null, turnIds: before?.turnIds ?? [],
      canAcceptDirectInput: before?.canAcceptDirectInput, agentVersion: before?.agentVersion });
  };
  const acceptSnapshot = (next: TreeSnapshot) => {
    if (!active || (next.run && next.run.runId !== runId)) return;
    const nextRevision = Number.isSafeInteger(next.revision) ? next.revision! : -1;
    const priorStop = snapshot.children?.stop;
    const nextStop = next.children?.stop;
    const stop = nextStop && (!priorStop || nextStop.version >= priorStop.version) ? nextStop : priorStop ?? null;
    // 完整报告只来自运行记录，与 watch 的数量摘要分别保留版本。
    const priorReport = snapshot.run?.childrenStop;
    const nextReport = next.run?.childrenStop;
    const report = nextReport && (!priorReport || nextReport.version >= priorReport.version) ? nextReport : priorReport;
    if (stop || report) stopVersion = Math.max(stopVersion, stop?.version ?? -1, report?.version ?? -1);
    const baseRun = nextRevision >= snapshotRevision ? next.run ?? snapshot.run : snapshot.run ?? next.run;
    const run = baseRun && report ? { ...baseRun, childrenStop: report } : baseRun;
    if (nextRevision >= snapshotRevision) {
      // 空快照可能来自已经释放的在线上下文，不能抹掉历史或已经观察到的成员。
      const members = new Map((snapshot.children?.members ?? []).map((member) => [member.threadId, member]));
      for (const member of next.children?.members ?? []) members.set(member.threadId, member);
      for (const event of [...changes.values()].sort((a, b) => a.revision - b.revision)) {
        if (event.revision > nextRevision) applyMember(members, event);
        else if (event.threadId) changes.delete(event.threadId);
      }
      snapshot = { run, revision: Math.max(revision, nextRevision),
        children: { members: [...members.values()], discovery: next.children?.discovery ?? snapshot.children?.discovery ?? null,
          // 无发现上下文时，线上兼容值 0 不能证明派生已全部落定。
          inFlightSpawns: next.children?.discovery ? next.children.inFlightSpawns : snapshot.children?.inFlightSpawns ?? null, stop } };
      snapshotRevision = nextRevision;
      revision = Math.max(revision, nextRevision);
    } else snapshot = { ...snapshot, run, ...(snapshot.children ? { children: { ...snapshot.children, stop } } : {}) };
    // 旧快照可以补成员字段，却不能证明通知之后的全局数量已经刷新。
    stale = nextRevision < revision || Math.max(nextStop?.version ?? -1, nextReport?.version ?? -1) < stopVersion;
  };
  const schedule = () => {
    if (!active || !runId || timer || inFlight || !dirty) return;
    timer = setTimeout(() => { timer = undefined; void refresh(); }, 200);
  };
  const refresh = async () => {
    if (!active || !runId || inFlight) return;
    const remaining = options.deadline - performance.now();
    if (remaining <= 0) return;
    dirty = false;
    inFlight = true;
    try {
      const timeoutMs = Math.min(2000, remaining);
      // 仅首次 watch 取得独占权，补快照始终固定本运行且不得接替其他等待者。
      const next = await bounded(rpc.request<TreeSnapshot>("broker/threads/watch", {
        threadId: options.threadId, runId,
      }, { timeoutMs }), timeoutMs, controller.signal);
      acceptSnapshot(next);
    } catch {
      if (active) stale = true;
    } finally {
      inFlight = false;
      if (active) { display(); schedule(); }
    }
  };
  const accept = (event: ChildChanged) => {
    if (!active || event.runId !== runId || event.rootThreadId !== options.threadId) return;
    const newerMember = Number.isSafeInteger(event.revision) && event.revision > revision;
    const newerStop = Number.isSafeInteger(event.stopVersion) && event.stopVersion! > stopVersion;
    if (!newerMember && !newerStop) return;
    if (newerMember) {
      revision = event.revision;
      const members = new Map((snapshot.children?.members ?? []).map((member) => [member.threadId, member]));
      applyMember(members, event);
      if (event.threadId) changes.set(event.threadId, event);
      snapshot = { ...snapshot, revision, children: { members: [...members.values()],
        discovery: snapshot.children?.discovery ?? null, inFlightSpawns: snapshot.children?.inFlightSpawns ?? null,
        stop: snapshot.children?.stop ?? null } };
    }
    if (newerStop) stopVersion = event.stopVersion!;
    dirty = true;
    stale = true;
    display();
    schedule();
  };
  const off = rpc.on("broker/run/childChanged", (raw) => {
    if (!active || !raw || typeof raw !== "object") return;
    const event = raw as ChildChanged;
    if (!runId) buffered.push(event); else accept(event);
  });
  function dispose() {
    if (!active) return;
    active = false;
    off();
    clearTimeout(timer);
    controller.abort();
    options.signal?.removeEventListener("abort", abort);
    buffered.length = 0;
    changes.clear();
  }
  options.signal?.addEventListener("abort", abort, { once: true });
  if (options.signal?.aborted) dispose();
  return {
    initialize(id: string, initial: TreeSnapshot, quiet = false) {
      if (!active || runId) return;
      runId = id;
      silent = true;
      acceptSnapshot(initial);
      for (const event of buffered.splice(0)) accept(event);
      silent = quiet;
      display();
    },
    current,
    dispose,
  };
}
