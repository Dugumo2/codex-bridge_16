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

  const current = (run?: RunResult | null, view: TreeView["view"] = "partial"): TreeView => {
    const tree = buildTree({ rootThreadId: options.threadId, snapshot, run, view, stale,
      shortIds: options.shortIds?.() });
    tree.revision = revision < 0 ? null : revision;
    tree.stopVersion = Math.max(stopVersion, tree.stopVersion ?? -1);
    if (tree.stopVersion < 0) tree.stopVersion = null;
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
    const priorStop = snapshot.children?.stop ?? snapshot.run?.childrenStop;
    const nextStop = next.children?.stop ?? next.run?.childrenStop;
    const stop = nextStop && (!priorStop || nextStop.version >= priorStop.version) ? nextStop : priorStop ?? null;
    if (stop) stopVersion = Math.max(stopVersion, stop.version);
    if (nextRevision >= snapshotRevision) {
      // 空快照可能来自已经释放的在线上下文，不能抹掉历史或已经观察到的成员。
      const members = new Map((snapshot.children?.members ?? []).map((member) => [member.threadId, member]));
      for (const member of next.children?.members ?? []) members.set(member.threadId, member);
      for (const event of [...changes.values()].sort((a, b) => a.revision - b.revision)) {
        if (event.revision > nextRevision) applyMember(members, event);
        else if (event.threadId) changes.delete(event.threadId);
      }
      snapshot = { run: next.run ?? snapshot.run, revision: Math.max(revision, nextRevision),
        children: { members: [...members.values()], discovery: next.children?.discovery ?? snapshot.children?.discovery ?? null,
          // 无发现上下文时，线上兼容值 0 不能证明派生已全部落定。
          inFlightSpawns: next.children?.discovery ? next.children.inFlightSpawns : snapshot.children?.inFlightSpawns ?? null, stop } };
      snapshotRevision = nextRevision;
      revision = Math.max(revision, nextRevision);
    } else if (snapshot.children) snapshot = { ...snapshot, children: { ...snapshot.children, stop } };
    stale = false;
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
