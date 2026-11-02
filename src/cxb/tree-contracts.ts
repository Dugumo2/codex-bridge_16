import type { ChildActivity, DiscoveryState, RelationKind, RootStopReport, RunResult, TreePhase, TurnResult } from "./contracts";

/** watch 的树字段；缺省与 null 都表示没有相应证据。 */
export interface WatchTreeMember {
  threadId: string;
  parentThreadId: string | null;
  relationKind: RelationKind;
  activity: ChildActivity;
  pendingCount: number | null;
  conflict: string | null;
  turnIds: string[];
  canAcceptDirectInput?: boolean | null;
  agentVersion?: string | null;
}
/** watch 只提供阶段与数量，不包含任何成员级停止核验证据。 */
export interface WatchStopSummary {
  phase: TreePhase;
  version: number;
  unconfirmed: number;
  conflicts: number;
}
export interface WatchChildren {
  members: WatchTreeMember[];
  discovery: DiscoveryState | null;
  inFlightSpawns: number | null;
  stop: WatchStopSummary | null;
}
export interface TreeSnapshot {
  run?: RunResult | null;
  revision?: number;
  children?: WatchChildren | null;
}
export interface TreeNodeResult {
  turnId: string | null;
  status: "inProgress" | "completed" | "failed" | "interrupted" | null;
  source: "server" | null;
  availability: "available" | "truncated" | "unavailable" | "omitted";
  output: string;
  outputSource: TurnResult["outputSource"];
  truncated: boolean;
  reason: string | null;
}
export interface TreeNodeStop {
  phase: TreePhase | null;
  issues: string[];
  source: "runRecord";
  report: RootStopReport | null;
}
export interface TreeNode extends WatchTreeMember {
  shortId: string | null;
  stop: TreeNodeStop | null;
  result: TreeNodeResult | null;
  /** 层级投影切断循环，不修改原始父标识。 */
  hierarchyIssue?: "missingParent" | "cycle";
}
export interface TreeView {
  rootThreadId: string;
  view: "terminal" | "partial";
  source: "watch" | "runRecord" | "mixed";
  revision: number | null;
  stopVersion: number | null;
  stopSummary: WatchStopSummary | null;
  discovery: DiscoveryState | null;
  inFlightSpawns: number | null;
  unsettledSpawns: number | null;
  resultScope: "latestBoundTurnPerThread";
  outputCoverage: { total: number; available: number; truncated: number; unavailable: number; omitted: number };
  nodes: TreeNode[];
  stale?: boolean;
}
export interface WaitTreeFields { tree: TreeView }

/** 纯投影输入；索引只补标签，绝不扩张成员集合。 */
export interface TreeBuildInput {
  rootThreadId: string;
  snapshot?: TreeSnapshot;
  run?: RunResult | null;
  shortIds?: ReadonlyMap<string, string>;
  results?: ReadonlyMap<string, TreeNodeResult>;
  view?: TreeView["view"];
  stale?: boolean;
}
