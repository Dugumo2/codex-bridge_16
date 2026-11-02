import type { ChildrenStopReport, ChildActivity } from "./contracts";
import { backgroundLines, threadLabel } from "./subagents";
import { sanitizeForTerminal } from "./terminal-text";
import type { TreeBuildInput, TreeNode, TreeNodeStop, TreeView, WatchStopSummary } from "./tree-contracts";

const compare = (a: string, b: string) => a < b ? -1 : a > b ? 1 : 0;
const safeText = (text: string) => sanitizeForTerminal(text).replace(/[\u0080-\u009f\u202a-\u202e\u2066-\u2069]/g, "");
const safeLine = (text: string) => safeText(text).replace(/[\r\n\t\u2028\u2029]/g, " ");
const label = (node: Pick<TreeNode, "threadId" | "shortId">) => safeLine(threadLabel(node.threadId, node.shortId));
const activityText: Record<ChildActivity, string> = {
  starting: "启动中", running: "运行中", settled: "活动已落定", unknown: "活动未知",
};

function memberStop(threadId: string, report: ChildrenStopReport | null): TreeNodeStop | null {
  if (!report) return null;
  const member = report.members?.[threadId] ?? null;
  const issues: string[] = [];
  if (report.unconfirmed.includes(threadId)) issues.push("尚未确认停下");
  if (report.failed.includes(threadId)) issues.push("核实失败");
  for (const conflict of report.conflicts.filter((item) => item.thread === threadId)) issues.push(`归属冲突：${conflict.reason}`);
  if (report.capabilityLimited.includes(threadId)) issues.push("能力受限");
  if (member?.error) issues.push(`核实错误：${member.error}`);
  const phase = member?.phase ?? (report.stopped.includes(threadId) ? "confirmed" : report.unconfirmed.includes(threadId) ? "unconfirmed" : null);
  return member || phase || issues.length ? { phase, issues, source: "runRecord", report: member } : null;
}

interface Placement { node: TreeNode; depth: number; group: "root" | "missing" | "cycle" }

/** 只在投影中切断循环，原始父标识仍留在 JSON 节点里。 */
function hierarchy(nodes: TreeNode[], rootThreadId: string): Placement[] {
  const byId = new Map(nodes.map((node) => [node.threadId, node]));
  const parents = new Map(nodes.map((node) => [node.threadId, node.parentThreadId]));
  const cuts = new Set<string>();
  const visited = new Set<string>();
  for (const id of [...byId.keys()].sort(compare)) {
    const path: string[] = [];
    const positions = new Map<string, number>();
    let current: string | null = id;
    while (current && current !== rootThreadId && byId.has(current) && !visited.has(current)) {
      const index = positions.get(current);
      if (index !== undefined) {
        const cycle = path.slice(index).sort(compare);
        cuts.add(cycle[0]!);
        for (const member of cycle) byId.get(member)!.hierarchyIssue = "cycle";
        break;
      }
      positions.set(current, path.length);
      path.push(current);
      current = parents.get(current) ?? null;
    }
    for (const member of path) visited.add(member);
  }
  const children = new Map<string, TreeNode[]>();
  const roots: Record<Placement["group"], TreeNode[]> = { root: [], missing: [], cycle: [] };
  for (const node of nodes) {
    const parent = parents.get(node.threadId);
    if (cuts.has(node.threadId)) roots.cycle.push(node);
    else if (parent === rootThreadId) roots.root.push(node);
    else if (parent && byId.has(parent)) {
      const siblings = children.get(parent) ?? [];
      siblings.push(node);
      children.set(parent, siblings);
    } else {
      node.hierarchyIssue = "missingParent";
      roots.missing.push(node);
    }
  }
  const result: Placement[] = [];
  for (const group of ["root", "missing", "cycle"] as const) {
    const stack = roots[group].sort((a, b) => compare(b.threadId, a.threadId)).map((node) => ({ node, depth: 1, group }));
    while (stack.length) {
      const next = stack.pop()!;
      result.push(next);
      const siblings = [...(children.get(next.node.threadId) ?? [])].sort((a, b) => compare(b.threadId, a.threadId));
      stack.push(...siblings.map((node) => ({ node, depth: next.depth + 1, group })));
    }
  }
  return result;
}

export function buildTree(input: TreeBuildInput): TreeView {
  const run = input.run ?? input.snapshot?.run;
  const watch = input.snapshot?.children;
  const records = run?.children ?? [];
  const nodes = new Map<string, TreeNode>();
  for (const child of records) {
    nodes.set(child.threadId, { threadId: child.threadId, parentThreadId: child.parentThreadId,
      relationKind: child.relationKind, activity: child.finalState ?? "unknown", pendingCount: null,
      conflict: null, turnIds: [...child.turnIds], shortId: null, stop: null, result: null });
  }
  // 空在线快照可能来自终态上下文清除；它不是删除历史成员的证据。
  for (const child of watch?.members ?? []) {
    const prior = nodes.get(child.threadId);
    nodes.set(child.threadId, { ...prior, ...child,
      parentThreadId: child.parentThreadId ?? prior?.parentThreadId ?? null,
      turnIds: [...new Set([...(prior?.turnIds ?? []), ...child.turnIds])].sort(compare),
      shortId: null, stop: null, result: null });
  }
  // review 执行线程仅补齐已知的结构关系，不从索引发现额外成员。
  if (run?.reviewThreadId && !nodes.has(run.reviewThreadId)
    && [...nodes.values()].some((node) => node.parentThreadId === run.reviewThreadId)) {
    nodes.set(run.reviewThreadId, { threadId: run.reviewThreadId, parentThreadId: input.rootThreadId,
      relationKind: "reviewExecution", activity: "unknown", pendingCount: null, conflict: null,
      turnIds: [], shortId: run.reviewShortId ?? null, stop: null, result: null });
  }
  const watchStop = watch?.stop ?? null;
  const reports = [input.run?.childrenStop, input.snapshot?.run?.childrenStop].filter((value) => value != null);
  // 摘要与完整报告各自选版；摘要再新也不能替代逐成员核验证据。
  const recordStop = reports.reduce<ChildrenStopReport | null>((latest, value) =>
    !latest || value.version > latest.version ? value : latest, null);
  const stopSummary: WatchStopSummary | null = recordStop && (!watchStop || recordStop.version >= watchStop.version)
    ? { phase: recordStop.phase, version: recordStop.version, unconfirmed: recordStop.unconfirmed.length, conflicts: recordStop.conflicts.length }
    : watchStop;
  // 发现状态有自己的版本；较新的停止报告不能被较旧在线发现快照覆盖。
  const discoveries = [watch?.discovery, ...reports.map((report) => report.discovery)].filter((value) => value != null);
  const discovery = discoveries.reduce<(typeof discoveries)[number] | null>((latest, value) =>
    !latest || value.version > latest.version ? value : latest, null);
  const members = [...nodes.values()].filter((node) => node.relationKind !== "fork" && node.threadId !== input.rootThreadId);
  const finalRecords = new Map(records.map((record) => [record.threadId, record]));
  for (const node of members) {
    const finalRecord = input.view === "terminal" ? finalRecords.get(node.threadId) : undefined;
    if (finalRecord) {
      node.turnIds = [...finalRecord.turnIds];
      node.activity = finalRecord.finalState ?? "unknown";
    }
    node.shortId = input.shortIds?.get(node.threadId) ?? node.shortId;
    node.stop = memberStop(node.threadId, recordStop);
    node.result = node.relationKind === "subagent" ? input.results?.get(node.threadId) ?? null : null;
    node.turnIds = [...new Set(node.turnIds)].sort(compare);
  }
  const ordered = hierarchy(members, input.rootThreadId).map(({ node }) => node);
  const coverage: TreeView["outputCoverage"] = { total: 0, available: 0, truncated: 0, unavailable: 0, omitted: 0 };
  for (const node of ordered.filter((node) => node.relationKind === "subagent")) {
    coverage.total++;
    coverage[node.result?.availability ?? "unavailable"]++;
  }
  return { rootThreadId: input.rootThreadId, view: input.view ?? "partial",
    source: watch ? records.length || recordStop ? "mixed" : "watch" : "runRecord",
    revision: input.snapshot?.revision ?? null, stopVersion: stopSummary?.version ?? null, stopSummary,
    discovery,
    inFlightSpawns: watch?.inFlightSpawns ?? null, unsettledSpawns: recordStop?.unsettledSpawns ?? null,
    resultScope: "latestBoundTurnPerThread", outputCoverage: coverage, nodes: ordered,
    ...(input.stale ? { stale: true } : {}) };
}

function nodeStatus(node: TreeNode): string {
  const parts = [activityText[node.activity]];
  if (node.relationKind === "reviewExecution") parts.push("review 执行线程（结构节点）");
  if (node.pendingCount !== null) parts.push(`待处理 ${node.pendingCount} 项`);
  if (node.conflict) parts.push(`归属冲突：${safeLine(node.conflict)}`);
  if (node.stop?.phase === "confirmed") parts.push("已确认停下");
  else if (node.stop?.phase) parts.push("尚未确认停下");
  parts.push(...(node.stop?.issues ?? []).map(safeLine));
  if (node.stop?.report?.backgroundCleanup) {
    parts.push(...backgroundLines(node.threadId, node.stop.report.backgroundCleanup).map((line) => safeLine(line).trim()));
  }
  if (node.hierarchyIssue === "cycle") parts.push("父子关系循环，层级已切断");
  else if (node.hierarchyIssue === "missingParent") parts.push("父节点信息缺失");
  return [...new Set(parts)].join("；");
}

function globalLines(tree: TreeView): string[] {
  const lines: string[] = [];
  if (tree.stopSummary) {
    const stop = tree.stopSummary;
    const phase = { pending: "核验中", confirmed: "已确认", unconfirmed: "尚未确认" }[stop.phase];
    lines.push(`整树停止：${phase}；未确认 ${stop.unconfirmed} 个；归属冲突 ${stop.conflicts} 个`);
  }
  if (!tree.nodes.some((node) => node.relationKind === "subagent")) lines.push(tree.discovery?.complete
    ? "本次运行没有子代理" : "未记录子代理成员，发现完整性未知");
  if (tree.discovery?.complete === false) lines.push(`子代理发现不完整${tree.discovery.incompleteReasons.length
    ? `：${safeLine(tree.discovery.incompleteReasons.join("；"))}` : ""}`);
  if (tree.inFlightSpawns !== null) lines.push(`在途派生 ${tree.inFlightSpawns} 个`);
  if (tree.unsettledSpawns !== null && tree.unsettledSpawns > 0) lines.push(`还有 ${tree.unsettledSpawns} 个派生尚未落定`);
  if (tree.stale) lines.push("树信息可能陈旧，已保留最后已知成员");
  return lines;
}

function placements(tree: TreeView): Placement[] {
  return hierarchy(tree.nodes.map((node) => ({ ...node })), tree.rootThreadId);
}

export function treeLines(tree: TreeView, rootShortId?: string | null): string[] {
  const lines = [safeLine(threadLabel(tree.rootThreadId, rootShortId))];
  let previousGroup = "root";
  for (const { node, depth, group } of placements(tree)) {
    if (group !== previousGroup) lines.push(group === "missing" ? "  父节点信息缺失：" : "  父子关系异常：");
    previousGroup = group;
    lines.push(`${"  ".repeat(depth)}└─ ${label(node)}  ${nodeStatus(node)}`);
  }
  return [...lines, ...globalLines(tree)];
}

export function treeChanges(previous: TreeView, next: TreeView, rootShortId?: string | null): string[] {
  const before = new Map(previous.nodes.map((node) => [node.threadId, node]));
  const after = new Map(next.nodes.map((node) => [node.threadId, node]));
  const lines: string[] = [];
  for (const { node, depth } of placements(next)) {
    const prior = before.get(node.threadId);
    if (prior && label(prior) === label(node) && nodeStatus(prior) === nodeStatus(node) && prior.parentThreadId === node.parentThreadId) continue;
    const parent = node.parentThreadId === next.rootThreadId ? safeLine(threadLabel(next.rootThreadId, rootShortId))
      : node.parentThreadId ? after.has(node.parentThreadId) ? label(after.get(node.parentThreadId)!) : safeLine(node.parentThreadId) : "未知";
    lines.push(`${"  ".repeat(depth)}${label(node)}（父 ${parent}）${prior ? `${nodeStatus(prior)} → ` : "新增："}${nodeStatus(node)}`);
  }
  const beforeGlobal = new Set(globalLines(previous));
  lines.push(...globalLines(next).filter((line) => !beforeGlobal.has(line)));
  if (previous.discovery?.complete === false && next.discovery?.complete === true) lines.push("子代理发现已完整");
  if ((previous.unsettledSpawns ?? 0) > 0 && next.unsettledSpawns === 0) lines.push("派生已全部落定");
  return lines;
}

function summary(text: string): string {
  const cleaned = safeText(text).replace(/\r\n?/g, "\n").replace(/[\u2028\u2029]/g, "\n");
  const lines = cleaned.split("\n");
  const limited = lines.slice(0, 4).join("\n");
  const characters = Array.from(limited);
  return characters.length > 300 || lines.length > 4 ? characters.slice(0, 299).join("") + "…" : limited;
}

export function treeResultText(tree: TreeView): string {
  const lines = ["子代理结果（本次运行最近受管轮）："];
  for (const { node, depth } of placements(tree)) {
    const indent = "  ".repeat(depth);
    if (node.relationKind === "reviewExecution") {
      lines.push(`${indent}${label(node)}  review 执行线程（结构节点）`);
      continue;
    }
    const result = node.result;
    const status = result?.status ? { inProgress: "进行中", completed: "完成", failed: "失败", interrupted: "中断" }[result.status] : "任务结果未知";
    lines.push(`${indent}${label(node)}  ${status}；${nodeStatus(node)}`);
    if (result?.availability === "available" || result?.availability === "truncated") {
      if (result.output) lines.push(...summary(result.output).split("\n").map((line) => `${indent}  ${line}`));
      else lines.push(`${indent}  正文为空`);
      if (result.availability === "truncated" || result.truncated) lines.push(`${indent}  正文已截断${result.reason ? `：${safeLine(result.reason)}` : ""}`);
    } else lines.push(`${indent}  ${result?.availability === "omitted" ? "正文已省略" : "正文不可用"}：${safeLine(result?.reason ?? "尚未取得本次运行受管轮结果")}`);
  }
  lines.push(...globalLines(tree));
  return lines.join("\n");
}
