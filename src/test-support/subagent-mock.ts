/** SA-a 子代理控制面。三个夹具共用；未启用时不消费消息、不改变历史行为。 */
export interface SubagentMockOptions {
  enabled?: boolean;
  version?: "v1" | "v2";
  autoAttach?: "confirmed" | "bestEffort" | "missing";
}
/** H2-a 冻结选项；实现由分页夹具负责，子代理模块不处理分页。 */
export interface StatusReadMockOptions {
  strictPagination?: boolean;
  omitActiveTurns?: boolean;
  historyMode?: "paged" | "beforeFirstTurn" | "legacy" | "unsupported";
  longHistory?: boolean;
}
export interface FixtureMessage {
  id?: unknown; method?: string; params?: any; result?: unknown; error?: unknown;
}
export type SubagentControl =
  | { method: "fixture/inject"; params: { threadId: string; kind: string; payload: unknown; hold?: string; requestId?: unknown } }
  | { method: "fixture/release"; params: { holdId: string } }
  | { method: "fixture/spawnChild"; params: { parentThreadId: string; parentTurnId: string; itemId: string; childThreadId: string; version: "v1" | "v2"; started: "now" | "held" | "never"; archived?: boolean; loaded?: boolean } }
  | { method: "fixture/reuseChild"; params: { parentThreadId: string; parentTurnId: string; itemId: string; childThreadId: string; version: "v1" | "v2"; mode: "v1Input" | "v1Steer" | "v1ResumeNoop" | "v2Message" | "v2Followup"; fail?: boolean } }
  | { method: "fixture/turn"; params: { threadId: string; action: "start" | "complete" | "fail" } }
  | { method: "fixture/interruptBehavior"; params: { threadId: string; mode: "normal" | "never" | "delayed" } }
  | { method: "fixture/listFault"; params: { mode: "pageError" | "duplicateCursor" | "slow" } }
  | { method: "fixture/goal"; params: { threadId: string; status: string } }
  | { method: "fixture/pendingInitStuck"; params: { childThreadId: string } }
  | { method: "fixture/state"; params?: Record<string, never> };
export interface SubagentThreadState {
  turns: unknown[];
  pendingRequests: FixtureMessage[];
  answers: Array<{ requestId: unknown; result?: unknown; error?: unknown; count: number }>;
  interrupts: unknown[];
  goalWrites: unknown[];
  attachments: unknown[];
}
export interface SubagentMock {
  handle(message: FixtureMessage): boolean;
  state(): Record<string, SubagentThreadState>;
}
export function createSubagentMock(_send: (message: unknown) => unknown, _options: SubagentMockOptions = {}): SubagentMock {
  return { handle: () => false, state: () => ({}) };
}
