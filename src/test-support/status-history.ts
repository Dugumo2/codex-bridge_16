/** H2 状态读取的模拟历史（接口第 7 节），三份模拟服务器共用；选项见 subagent-mock.ts 的 StatusReadMockOptions。
 *
 * 未启用任何选项时不接管消息，夹具保持原有行为。启用后接管 thread/read、thread/turns/list、thread/items/list：
 * - 轮次与条目从夹具自己发出的消息里学（turn/started、item/completed、turn/completed、turn/start 回包），
 *   客户端的 turn/start 输入记为该轮第一条 userMessage；
 * - strictPagination：严格执行 limit（缺省 25、上限 100）、cursor、方向与 itemsView（缺省 summary），notLoaded 时条目为空；
 *   条目分页严格按 turnId，且每轮前置 64 条推理条目，使关键条目落在后续页；不严格时忽略 limit，一页给完；
 * - omitActiveTurns：活动轮次不进存储页（模拟分页线程未持久化的活动轮次），元数据状态仍为 active；
 * - historyMode：paged（缺省）、beforeFirstTurn（没有已存轮次时 turns/list 回 -32601）、
 *   legacy（turns 可用、items 回 -32601）、unsupported（两者都回 -32601）；
 * - longHistory：线程前置 3072 个已完成的历史轮次；thread/read 带 includeTurns:true 时回包超过 10 MiB。
 * 本文件被 writeMockCodexScript 原样内联进模拟脚本，只能有类型导入。 */
import type { StatusReadMockOptions } from "./subagent-mock";

export interface StatusHistoryTurn {
  id: string;
  status: string;
  items: any[];
  error?: unknown;
  [key: string]: unknown;
}

/** 严格模式每轮前置的推理条目数，等于 broker 读条目的页大小，使关键条目必然落在第二页以后。 */
export const STATUS_FILLER_ITEMS = 64;

export function statusReadEnabled(options: StatusReadMockOptions | undefined): boolean {
  return !!options && !!(options.strictPagination || options.omitActiveTurns || options.historyMode || options.longHistory);
}

export function createStatusHistory(
  options: StatusReadMockOptions | undefined,
  send: (message: unknown) => unknown,
  longTurns: () => StatusHistoryTurn[],
) {
  const enabled = statusReadEnabled(options);
  const mode = options?.historyMode ?? "paged";
  const strict = !!options?.strictPagination;
  const threads = new Map<string, StatusHistoryTurn[]>();
  const pendingItems = new Map<string, any[]>();
  const pendingInput = new Map<string, any[]>();
  /** turn/start 请求编号 → 线程，用来认出回包里的轮次。 */
  const startThreads = new Map<string, string>();
  let history: StatusHistoryTurn[] | undefined;

  const live = (threadId: string) => {
    let turns = threads.get(threadId);
    if (!turns) { turns = []; threads.set(threadId, turns); }
    return turns;
  };
  const longHistory = () => options?.longHistory ? (history ??= longTurns()) : [];
  const all = (threadId: string) => [...longHistory(), ...live(threadId)];
  const active = (threadId: string) => live(threadId).some(turn => turn.status === "inProgress");
  const stored = (threadId: string) => all(threadId).filter(turn => !(options?.omitActiveTurns && turn.status === "inProgress"));
  const userItem = (turnId: string, input: any[]) => ({ type: "userMessage", id: `user-${turnId}`, content: input });

  function upsert(threadId: string, turn: any): StatusHistoryTurn | undefined {
    if (!turn || typeof turn.id !== "string") return undefined;
    const turns = live(threadId);
    let known = turns.find(t => t.id === turn.id);
    if (!known) {
      const input = pendingInput.get(threadId);
      pendingInput.delete(threadId);
      known = { id: turn.id, status: "inProgress", items: input ? [userItem(turn.id, input)] : [], error: null };
      turns.push(known);
    }
    return known;
  }

  /** 学习夹具发出的每条消息。 */
  function observe(message: any): void {
    if (!enabled || !message || typeof message !== "object") return;
    const params = message.params ?? {};
    if (message.method === "turn/started") upsert(params.threadId, params.turn);
    else if (message.method === "item/completed" && params.item) {
      const key = `${params.threadId}\u0000${params.turnId}`;
      const items = pendingItems.get(key) ?? [];
      items.push(params.item);
      pendingItems.set(key, items);
    } else if (message.method === "turn/completed") {
      const turn = upsert(params.threadId, params.turn);
      if (!turn) return;
      const key = `${params.threadId}\u0000${turn.id}`;
      const reported: any[] = Array.isArray(params.turn.items) && params.turn.itemsView !== "notLoaded" ? params.turn.items : [];
      const streamed = pendingItems.get(key) ?? [];
      pendingItems.delete(key);
      const learned = [...streamed, ...reported.filter(item => !streamed.some(seen => seen?.id !== undefined && seen.id === item?.id))];
      turn.status = String(params.turn.status ?? "completed");
      turn.error = params.turn.error ?? null;
      turn.items = [...turn.items.filter(item => item.type === "userMessage"), ...learned];
    } else if (message.id !== undefined && message.result?.turn && message.result.reviewThreadId === undefined) {
      // turn/start 回包早于 turn/started 时也要认得这一轮；线程取自对应请求。
      const threadId = startThreads.get(String(message.id));
      startThreads.delete(String(message.id));
      if (threadId) upsert(threadId, message.result.turn);
    }
  }
  function wrap<T extends (message: unknown) => unknown>(inner: T): T {
    return ((message: unknown) => { observe(message); return inner(message); }) as T;
  }

  const error = (id: unknown, code: number, message: string) => { send({ id, error: { code, message } }); return true; };

  function view(turn: StatusHistoryTurn, itemsView: string) {
    if (itemsView === "notLoaded") return { ...turn, items: [], itemsView: "notLoaded" };
    if (itemsView === "summary") {
      const first = turn.items.find(item => item.type === "userMessage");
      const last = [...turn.items].reverse().find(item => item.type === "agentMessage");
      return { ...turn, items: [first, last].filter(Boolean), itemsView: "summary" };
    }
    return { ...turn, itemsView: "full" };
  }

  function page<T>(list: T[], cursor: unknown, limit: unknown, key: (value: T, index: number) => string):
    { data: T[]; nextCursor: string | null } | string {
    let start = 0;
    if (cursor !== undefined && cursor !== null) {
      if (typeof cursor !== "string") return "invalid cursor";
      start = list.findIndex((value, index) => key(value, index) === cursor);
      if (start < 0) return `invalid cursor: ${cursor}`;
    }
    const size = strict ? Math.min(100, Math.max(1, typeof limit === "number" ? limit : 25)) : list.length;
    const data = list.slice(start, start + size);
    const next = start + size < list.length ? key(list[start + size]!, start + size) : null;
    return { data, nextCursor: next };
  }

  /** 接管状态读取相关方法；返回 true 表示已回包。 */
  function handle(msg: any): boolean {
    if (!enabled || msg?.id === undefined || typeof msg.method !== "string") return false;
    const p = msg.params ?? {};
    if (msg.method === "turn/start" && typeof p.threadId === "string") {
      startThreads.set(String(msg.id), p.threadId);
      if (Array.isArray(p.input)) pendingInput.set(p.threadId, p.input);
      return false;
    }
    if (msg.method === "thread/read") {
      const threadId = String(p.threadId ?? "");
      const status = active(threadId) ? { type: "active", activeFlags: [] } : { type: "idle" };
      // 旧的全量读先持久化，包含活动轮次。
      const turns = p.includeTurns === true ? all(threadId).map(turn => view(turn, "full")) : [];
      send({ id: msg.id, result: { thread: { id: threadId, preview: "", modelProvider: "openai", createdAt: 1, updatedAt: 1,
        status, path: null, cwd: process.cwd(), cliVersion: "0.160.0", source: "mock", name: null, turns } } });
      return true;
    }
    if (msg.method === "thread/turns/list") {
      if (mode === "unsupported") return error(msg.id, -32601, "Method not found: thread/turns/list");
      const threadId = String(p.threadId ?? "");
      const turns = stored(threadId);
      if (mode === "beforeFirstTurn" && turns.length === 0) return error(msg.id, -32601, "list_turns is not supported yet");
      const ordered = p.sortDirection === "asc" ? turns : [...turns].reverse();
      const result = page(ordered, p.cursor, p.limit, turn => `turn:${turn.id}`);
      if (typeof result === "string") return error(msg.id, -32600, result);
      const itemsView = typeof p.itemsView === "string" ? p.itemsView : "summary";
      send({ id: msg.id, result: { data: result.data.map(turn => view(turn, itemsView)), nextCursor: result.nextCursor, backwardsCursor: null } });
      return true;
    }
    if (msg.method === "thread/items/list") {
      if (mode === "legacy" || mode === "unsupported") return error(msg.id, -32601, "Method not found: thread/items/list");
      const threadId = String(p.threadId ?? "");
      const turn = all(threadId).find(t => t.id === p.turnId);
      if (!turn) return error(msg.id, -32600, `turn not found: ${String(p.turnId)}`);
      const filler = strict ? Array.from({ length: STATUS_FILLER_ITEMS }, (_, index) =>
        ({ type: "reasoning", id: `filler-${turn.id}-${index}`, summary: [], content: [] })) : [];
      const items = [...filler, ...turn.items];
      const ordered = p.sortDirection === "desc" ? [...items].reverse() : items;
      const result = page(ordered, p.cursor, p.limit, (_, index) => `item:${index}`);
      if (typeof result === "string") return error(msg.id, -32600, result);
      send({ id: msg.id, result: { data: result.data.map(item => ({ turnId: turn.id, item, startedAtMs: null, completedAtMs: null })),
        nextCursor: result.nextCursor, backwardsCursor: null } });
      return true;
    }
    return false;
  }

  return { enabled, handle, observe, wrap, turns: all };
}
