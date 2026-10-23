/** SA1b 独立模拟上游；只用于测试，不推断或替 broker 实现收敛。 */
import { createBackgroundTerminalsMock } from "./background-terminals-mock";
export type ProtocolVersion = "v1" | "v2";
export type DeliveryMode = "normal" | "delayed" | "dropped";
export type StartMode = "started" | "steered" | "replyLost" | "transportError";
export interface RpcMessage { id?: any; method?: string; params?: any; result?: any; error?: any }
export interface MockTurn { id: string; status: string; items: any[]; error: any }
export interface MockThread {
  id: string; parentThreadId: string | null; version: ProtocolVersion; turns: MockTurn[];
  loaded: boolean; archived: boolean; pendingInit: boolean; stored: boolean; closing: boolean; canAcceptDirectInput?: boolean;
  goal: any; usage: any; delivery: DeliveryMode; startMode: StartMode;
  interrupts: any[]; goalWrites: any[]; inputs: any[]; answers: any[]; pendingRequests: RpcMessage[];
}
export interface Sa1bOptions { version?: ProtocolVersion; pageSize?: number }

/** 命令皆为 fixture/sa1b/<名称>，避免与旧夹具控制面相互消费。 */
export function createSa1bMock(send: (message: RpcMessage, connectionId?: string) => unknown, options: Sa1bOptions = {}) {
  const threads = new Map<string, MockThread>();
  const calls: RpcMessage[] = [];
  const events: RpcMessage[] = [];
  const held = new Map<string, RpcMessage[]>();
  const fences = new Map<string, { method: string; threadId?: string }>();
  const requestFences = new Map<string, { method: string; threadId?: string }>();
  const requestHeld = new Map<string, RpcMessage[]>();
  const snapshotFences = new Map<string, { method: string; threadId?: string }>();
  const responseFences = new Map<string, { method: string; threadId?: string }>();
  const subscriptions = new Map<string, Set<string>>([["broker", new Set()]]);
  const deliveries: { connectionId: string; message: RpcMessage }[] = [];
  const heldDeliveries = new Map<string, { message: RpcMessage; recipients?: string[]; connectionId: string }[]>();
  const requestConnections = new Map<RpcMessage, string>();
  const busy = new Set<string>();
  const queued = new Map<string, { message: RpcMessage; connectionId: string }[]>();
  const responseContexts = new Map<string, { method: string; threadId?: string; connectionId: string }>();
  let currentConnection = "broker";
  const broadcasts = new Set(["thread/started", "thread/status/changed", "thread/goal/updated", "thread/goal/cleared", "thread/closed",
    "thread/name/updated", "thread/archived", "thread/unarchived", "thread/deleted", "thread/reverted", "thread/attachment/updated", "thread/project/updated"]);
  const faults = new Map<string, { mode: string; remaining: number }>();
  const background = createBackgroundTerminalsMock(message => emit(message as RpcMessage), { enabled: true });
  let serial = 0;
  const clone = <T>(value: T): T => structuredClone(value);
  const thread = (id: string): MockThread => {
    let t = threads.get(id);
    if (!t) {
      t = { id, parentThreadId: null, version: options.version ?? "v1", turns: [], loaded: true, archived: false, stored: true, closing: false,
        pendingInit: false, canAcceptDirectInput: true, goal: null, usage: null, delivery: "normal", startMode: "started",
        interrupts: [], goalWrites: [], inputs: [], answers: [], pendingRequests: [] };
      threads.set(id, t);
      for (const subscribed of subscriptions.values()) subscribed.add(id);
    }
    return t;
  };
  const emit = (message: RpcMessage, hold?: string, captured?: string[], connectionId = currentConnection) => {
    const target = message.params?.threadId ?? message.params?.thread?.id;
    const context = message.method ? undefined : responseContexts.get(JSON.stringify([connectionId, message.id]));
    // 快照栅栏在读取订阅集合前停住，送达栅栏保存当时的接收者；二者不能互相替代。
    const beforeSnapshot = !captured && [...snapshotFences].find(([, f]) => f.method === message.method && (!f.threadId || f.threadId === target))?.[0];
    const recipients = beforeSnapshot ? undefined : captured ?? (!message.method ? [context?.connectionId ?? connectionId]
      : [...subscriptions].filter(([, subscribed]) => !target || broadcasts.has(message.method!) || subscribed.has(target)).map(([id]) => id));
    const fence = beforeSnapshot || hold || (context && [...responseFences].find(([, f]) => f.method === context.method && (!f.threadId || f.threadId === context.threadId))?.[0])
      || [...fences].find(([, f]) => f.method === message.method && (!f.threadId || f.threadId === target))?.[0];
    if (fence) {
      held.set(fence, [...held.get(fence) ?? [], clone(message)]);
      heldDeliveries.set(fence, [...heldDeliveries.get(fence) ?? [], { message: clone(message), recipients, connectionId }]); return;
    }
    for (const recipient of recipients ?? []) {
      deliveries.push({ connectionId: recipient, message: clone(message) });
      if (recipient === "broker") events.push(clone(message));
      send(clone(message), recipient);
    }
  };
  const reply = (id: any, result: any, hold?: string) => emit({ id, result }, hold);
  const error = (id: any, message: string, code = -32600) => emit({ id, error: { code, message } });
  const notify = (method: string, params: any, hold?: string) => emit({ method, params }, hold);
  const meta = (t: MockThread, includeTurns = false) => ({
    id: t.id, parentThreadId: t.parentThreadId, forkedFromId: null, preview: "", ephemeral: false,
    modelProvider: "openai", createdAt: 1, updatedAt: 1, path: null, cwd: process.env.TEMP!, cliVersion: "0.160.0",
    status: { type: !t.loaded ? "notLoaded" : t.pendingInit ? "active" : t.turns.some(x => x.status === "inProgress") ? "active" : "idle", activeFlags: [] },
    source: t.parentThreadId ? { subAgent: { thread_spawn: { parent_thread_id: t.parentThreadId,
      depth: ancestors(t).length, agent_path: t.version === "v2" ? `/root/${t.id}` : null,
      agent_nickname: "测试代理", agent_role: t.version === "v1" ? "default" : null } } } : "vscode",
    ...(t.canAcceptDirectInput === undefined ? {} : { canAcceptDirectInput: t.canAcceptDirectInput }),
    agentNickname: null, agentRole: null, gitInfo: null, name: null, turns: includeTurns ? clone(t.turns) : [],
  });
  function ancestors(t: MockThread): string[] {
    const ids: string[] = [];
    let next = t.parentThreadId;
    while (next && !ids.includes(next)) { ids.push(next); next = threads.get(next)?.parentThreadId ?? null; }
    return ids;
  }
  function start(t: MockThread, id?: string, announce = true): MockTurn {
    const turn: MockTurn = { id: id ?? `${t.id}-turn-${++serial}`, status: "inProgress", items: [], error: null };
    t.pendingInit = false; t.turns.push(turn);
    if (announce) notify("turn/started", { threadId: t.id, turn });
    return turn;
  }
  function item(t: MockThread, turn: MockTurn, value: any, announce = true) {
    const old = turn.items.findIndex(x => x.id === value.id);
    if (old < 0) turn.items.push(clone(value)); else turn.items[old] = clone(value);
    if (announce) notify("item/completed", { threadId: t.id, turnId: turn.id, item: value });
  }
  function finish(t: MockThread, turn: MockTurn, status: string, output?: string, announce = true) {
    if (output !== undefined) item(t, turn, { id: `answer-${turn.id}`, type: "agentMessage", text: output, phase: "final_answer" }, announce);
    // errored/shutdown 是 CollabAgentStatus，不是协议 TurnStatus。
    turn.status = status === "errored" ? "failed" : status === "shutdown" ? "interrupted" : status;
    if (status === "failed" || status === "errored") turn.error = { message: "测试失败", codexErrorInfo: null, additionalDetails: null };
    if (announce) notify("turn/completed", { threadId: t.id, turn });
    if (status === "shutdown") {
      t.loaded = false;
      for (const subscribed of subscriptions.values()) subscribed.delete(t.id);
      notify("thread/closed", { threadId: t.id });
    }
    if (t.parentThreadId && (status === "errored" || status === "shutdown")) {
      const parent = thread(t.parentThreadId), parentTurn = parent.turns.at(-1);
      if (parentTurn) item(parent, parentTurn, { type: "collabAgentToolCall", id: `status-${turn.id}`,
        tool: status === "shutdown" ? "closeAgent" : "wait", status: "completed", senderThreadId: parent.id,
        receiverThreadIds: [t.id], agentsStates: { [t.id]: { status, message: output ?? null } } });
    }
    if (!t.parentThreadId || t.delivery === "dropped") return;
    // 模拟父收件内容，不将它偷写进父 userMessage，也不代表真实 core 的采样顺序。
    notify("fixture/sa1b/completionMessage", { threadId: t.parentThreadId, childThreadId: t.id, turnId: turn.id, status, output },
      t.delivery === "delayed" ? `completion:${t.id}` : undefined);
    if (t.version === "v2") {
      const parent = thread(t.parentThreadId);
      const parentTurn = parent.turns.at(-1);
      if (parentTurn) {
        const activity = { type: "subAgentActivity", id: `done-${turn.id}`, kind: "completed", agentThreadId: t.id, agentPath: `/root/${t.id}` };
        item(parent, parentTurn, activity, false);
        // 活动事实与尽力投递的结果消息是两种独立信息。
        notify("item/started", { threadId: parent.id, turnId: parentTurn.id, item: activity });
        notify("item/completed", { threadId: parent.id, turnId: parentTurn.id, item: activity });
      }
    }
  }
  function input(t: MockThread, turn: MockTurn, p: any) {
    const value = { type: "userMessage", id: `item-${++serial}`, clientId: p.clientUserMessageId ?? null, content: p.input ?? [] };
    t.inputs.push({ turnId: turn.id, ...clone(value) }); item(t, turn, value);
  }
  function page(id: any, data: any[], p: any) {
    const offset = Number(p.cursor ?? 0);
    if (!Number.isInteger(offset) || offset < 0) { error(id, "invalid cursor"); return; }
    const size = Math.max(1, Math.min(options.pageSize ?? 100, p.limit ?? 100));
    reply(id, { data: data.slice(offset, offset + size), nextCursor: offset + size < data.length ? String(offset + size) : null, backwardsCursor: null });
  }
  function control(msg: RpcMessage) {
    const p = msg.params ?? {};
    const t = p.threadId ? thread(p.threadId) : undefined;
    switch (msg.method?.slice("fixture/sa1b/".length)) {
      // 观测响应不能再次进入事件日志，否则连续 state 会递归复制之前的整份快照。
      case "state": send({ id: msg.id, result: state() }, currentConnection); break;
      case "connection": subscriptions.set(p.connectionId, new Set(p.threadIds ?? [])); reply(msg.id, {}); break;
      case "fence": (p.direction === "request" ? requestFences : p.direction === "snapshot" ? snapshotFences : p.direction === "response" ? responseFences : fences).set(p.name, { method: p.method, threadId: p.threadId }); reply(msg.id, {}); break;
      case "release": {
        fences.delete(p.name); snapshotFences.delete(p.name); responseFences.delete(p.name);
        const batch = held.get(p.name) ?? []; held.delete(p.name);
        const captured = heldDeliveries.get(p.name); heldDeliveries.delete(p.name);
        if (captured) for (const event of captured) emit(event.message, undefined, event.recipients, event.connectionId);
        else for (const message of batch) emit(message);
        requestFences.delete(p.name); const requests = requestHeld.get(p.name) ?? []; requestHeld.delete(p.name);
        for (const request of requests) execute(request, requestConnections.get(request) ?? "broker");
        reply(msg.id, { released: batch.length + requests.length }); break;
      }
      case "unload": {
        t!.loaded = false; t!.closing = false;
        if (p.stored === false) t!.stored = false;
        for (const subscribed of subscriptions.values()) subscribed.delete(t!.id);
        if (p.closed !== false) notify("thread/closed", { threadId: t!.id });
        reply(msg.id, {}); break;
      }
      case "barrier": reply(msg.id, { eventCount: events.length, callCount: calls.length }); break;
      case "spawn": {
        const parent = thread(p.parentThreadId);
        const pt = parent.turns.find(x => x.id === p.parentTurnId);
        if (!pt) { error(msg.id, "父轮不存在"); break; }
        const child = thread(p.childThreadId); child.parentThreadId = parent.id; child.version = p.version ?? parent.version;
        child.canAcceptDirectInput = child.version !== "v2"; child.pendingInit = p.pendingInit === true;
        const id = p.itemId ?? `spawn-${++serial}`;
        const value = child.version === "v1"
          ? { type: "collabAgentToolCall", id, tool: "spawnAgent", status: "inProgress", senderThreadId: parent.id, receiverThreadIds: [], prompt: null, model: null, reasoningEffort: null, agentsStates: {} }
          : { type: "subAgentActivity", id, kind: "started", agentThreadId: child.id, agentPath: `/root/${child.id}` };
        if (child.version === "v1") notify("item/started", { threadId: parent.id, turnId: pt.id, item: value });
        if (child.pendingInit) { item(parent, pt, value, false); reply(msg.id, { childThreadId: child.id, turnId: null }); break; }
        if (p.announce !== false) notify("thread/started", { thread: meta(child) });
        if (child.version === "v1") { value.status = "completed"; value.receiverThreadIds = [child.id] as never; }
        else notify("item/started", { threadId: parent.id, turnId: pt.id, item: value });
        item(parent, pt, value);
        const turn = p.firstTurn === false ? null : start(child);
        reply(msg.id, { childThreadId: child.id, turnId: turn?.id ?? null }); break;
      }
      case "reuse": {
        const parent = thread(p.parentThreadId), child = thread(p.childThreadId);
        const pt = parent.turns.find(x => x.id === p.parentTurnId);
        if (!pt) { error(msg.id, "父轮不存在"); break; }
        const value = child.version === "v1"
          ? { type: "collabAgentToolCall", id: `reuse-${++serial}`, tool: "sendInput", status: "completed", senderThreadId: parent.id, receiverThreadIds: [child.id], agentsStates: {} }
          : { type: "subAgentActivity", id: `reuse-${++serial}`, kind: "interacted", agentThreadId: child.id, agentPath: `/root/${child.id}` };
        // 上游先提交消息后通知 interacted；也支持 sendInput 并入既有轮、不再开轮。
        let turn = p.open === false ? child.turns.at(-1) : p.order === "turnFirst" ? start(child) : undefined;
        notify("item/started", { threadId: parent.id, turnId: pt.id, item: value }); item(parent, pt, value);
        if (p.open !== false && !turn) turn = start(child);
        reply(msg.id, { turnId: turn?.id }); break;
      }
      case "turn": {
        if (!t) throw new Error("turn 缺 threadId");
        const turn = p.action === "start" ? start(t, p.turnId, p.announce !== false) : t.turns.find(x => x.id === p.turnId) ?? t.turns.at(-1);
        if (!turn) { error(msg.id, "轮次不存在"); break; }
        if (p.action !== "start") finish(t, turn, p.status ?? "completed", p.output, p.announce !== false);
        reply(msg.id, { turnId: turn.id }); break;
      }
      case "delivery": thread(p.childThreadId).delivery = p.mode; reply(msg.id, {}); break;
      case "startBehavior": t!.startMode = p.mode; reply(msg.id, {}); break;
      case "metadata": {
        for (const key of ["loaded", "archived", "canAcceptDirectInput", "pendingInit", "stored", "closing"] as const) if (key in p) (t as any)[key] = p[key] === null ? undefined : p[key];
        reply(msg.id, {}); break;
      }
      case "fault": faults.set(`${p.method}:${p.threadId ?? "*"}`, { mode: p.mode, remaining: p.count ?? -1 }); reply(msg.id, {}); break;
      case "goal": {
        t!.goal = { id: p.goalId ?? `goal-${t!.id}`, threadId: t!.id, objective: p.objective ?? "测试目标", status: p.status,
          tokenBudget: p.tokenBudget ?? null, tokensUsed: p.tokensUsed ?? 0, timeUsedSeconds: 0, createdAt: 1, updatedAt: ++serial };
        notify("thread/goal/updated", { threadId: t!.id, turnId: p.turnId ?? null, goal: t!.goal }); reply(msg.id, {}); break;
      }
      case "usage": {
        const usage = { inputTokens: p.inputTokens, cachedInputTokens: p.cachedInputTokens ?? 0, outputTokens: p.outputTokens, reasoningOutputTokens: 0, totalTokens: p.inputTokens + p.outputTokens };
        t!.usage = { total: usage, last: usage, modelContextWindow: 128000 };
        notify("thread/tokenUsage/updated", { threadId: t!.id, turnId: p.turnId ?? t!.turns.at(-1)?.id, tokenUsage: t!.usage }); reply(msg.id, {}); break;
      }
      case "item": {
        const turn = t!.turns.find(x => x.id === p.turnId) ?? t!.turns.at(-1);
        if (!turn) { error(msg.id, "轮次不存在"); break; }
        item(t!, turn, p.item, p.announce !== false); reply(msg.id, {}); break;
      }
      case "inject": {
        const value = p.message as RpcMessage;
        if (value.id !== undefined && value.method) thread(value.params.threadId).pendingRequests.push(clone(value));
        emit(value, p.hold); reply(msg.id, {}); break;
      }
      case "replay": {
        const batch = events.filter(x => x.method === p.method && (!p.threadId || x.params?.threadId === p.threadId));
        for (const message of batch.slice(-(p.count ?? 1))) emit(message);
        reply(msg.id, {}); break;
      }
      default: error(msg.id, "未知 SA1b 控制方法", -32601);
    }
  }
  function processMessage(msg: RpcMessage): boolean {
    if (msg.method?.startsWith("fixture/sa1b/")) { control(msg); return true; }
    if (!msg.method) {
      for (const t of threads.values()) {
        const pending = t.pendingRequests.find(x => x.id === msg.id);
        if (pending) { t.answers.push(clone(msg)); t.pendingRequests = t.pendingRequests.filter(x => x !== pending); }
      }
      return true;
    }
    const p = msg.params ?? {};
    const fault = faults.get(`${msg.method}:${p.threadId ?? "*"}`) ?? faults.get(`${msg.method}:*`);
    if (fault && fault.remaining !== 0) {
      if (fault.remaining > 0) fault.remaining--;
      if (fault.mode === "error" || fault.mode === "notFound" || fault.mode === "closing" || fault.mode === "attachedError") {
        if (fault.mode === "attachedError") { thread(p.threadId).loaded = true; subscriptions.get(currentConnection)!.add(p.threadId); }
        error(msg.id, fault.mode === "notFound" ? (msg.method === "thread/read" ? `thread not loaded: ${p.threadId}` : `no rollout found for thread id ${p.threadId}`)
          : fault.mode === "closing" ? `thread ${p.threadId} is closing; retry thread/resume after the thread is closed`
          : fault.mode === "attachedError" ? "模拟已订阅后构造响应失败" : "模拟读取失败"); return true;
      }
      if (fault.mode === "dropped") return true;
      if (fault.mode === "duplicateCursor") { reply(msg.id, { data: [], nextCursor: "duplicate" }); return true; }
      if (fault.mode === "notLoaded") {
        if (msg.method === "thread/unsubscribe") {
          thread(p.threadId).loaded = false;
          for (const subscribed of subscriptions.values()) subscribed.delete(p.threadId);
          reply(msg.id, { status: "notLoaded" });
        } else reply(msg.id, { thread: { ...meta(thread(p.threadId)), status: { type: "notLoaded" } } });
        return true;
      }
    }
    if (background.handle(msg)) return true;
    const t = p.threadId ? thread(p.threadId) : undefined;
    switch (msg.method) {
      case "initialize": reply(msg.id, { userAgent: "sa1b-mock/0.160.0", platformFamily: "windows", platformOs: "windows" }); break;
      case "initialized": break;
      case "model/list": reply(msg.id, { data: [{ id: "original-model", model: "original-model", displayName: "测试模型", description: "", isDefault: true,
        supportedReasoningEfforts: [{ reasoningEffort: "high", description: "高" }], defaultReasoningEffort: "high", inputModalities: ["text"] }], nextCursor: null }); break;
      case "thread/start": { const root = thread(p.id ?? "root"); notify("thread/started", { thread: meta(root) }); reply(msg.id, { thread: meta(root), model: p.model ?? "mock-model", cwd: p.cwd, approvalPolicy: "never", sandbox: { type: "dangerFullAccess" } }); break; }
      case "thread/resume": {
        if (!t!.stored) { error(msg.id, `no rollout found for thread id ${t!.id}`); break; }
        if (t!.closing) { error(msg.id, `thread ${t!.id} is closing; retry thread/resume after the thread is closed`); break; }
        if (!t!.loaded) {
          t!.loaded = true;
          for (const subscribed of subscriptions.values()) subscribed.add(t!.id);
          notify("thread/started", { thread: meta(t!) });
        }
        subscriptions.get(currentConnection)!.add(t!.id);
        reply(msg.id, { thread: meta(t!), model: p.model ?? "mock-model", cwd: process.env.TEMP!, approvalPolicy: "never", sandbox: { type: "dangerFullAccess" } }); break;
      }
      case "thread/read":
        if (!t!.stored && !t!.loaded) error(msg.id, `thread not loaded: ${t!.id}`);
        else reply(msg.id, { thread: meta(t!, p.includeTurns === true) }); break;
      case "thread/list": page(msg.id, [...threads.values()].filter(x => (!p.ancestorThreadId || ancestors(x).includes(p.ancestorThreadId)) && x.archived === (p.archived === true)).map(x => meta(x)), p); break;
      case "thread/loaded/list": page(msg.id, [...threads.values()].filter(x => x.loaded && !x.archived).map(x => x.id), p); break;
      case "thread/turns/list": {
        const turns = p.sortDirection === "asc" ? t!.turns : [...t!.turns].reverse();
        page(msg.id, turns.map(x => ({ ...x, items: p.itemsView === "full" ? x.items : [], itemsView: p.itemsView === "full" ? "full" : "notLoaded" })), p); break;
      }
      case "thread/items/list": page(msg.id, (t!.turns.find(x => x.id === p.turnId)?.items ?? []).map(item => ({ turnId: p.turnId, item, startedAtMs: null, completedAtMs: null })), p); break;
      case "turn/start": case "turn/steer": {
        if (t!.canAcceptDirectInput === false) { error(msg.id, "agent thread does not accept direct input"); break; }
        let turn = t!.turns.findLast(x => x.status === "inProgress");
        if (msg.method === "turn/steer" && (!turn || turn.id !== p.expectedTurnId)) { error(msg.id, "expected turn mismatch"); break; }
        if (!turn && t!.startMode === "steered") { error(msg.id, "没有可并入的活动轮"); break; }
        // turn/start 是 start-or-steer；started 模式也不偷偷创建并发轮。
        turn ??= start(t!);
        input(t!, turn, p);
        const response = msg.method === "turn/start" ? { turn: clone(turn) } : { turnId: turn.id };
        if (t!.startMode === "replyLost") held.set(`reply:${t!.id}`, [...held.get(`reply:${t!.id}`) ?? [], { id: msg.id, result: response }]);
        else if (t!.startMode === "transportError") error(msg.id, "transport failed after submit", -32000);
        else reply(msg.id, response);
        break;
      }
      case "turn/interrupt": {
        t!.interrupts.push(clone(p)); const turn = t!.turns.find(x => x.id === p.turnId);
        reply(msg.id, {}); if (turn?.status === "inProgress") finish(t!, turn, "interrupted"); break;
      }
      case "thread/goal/get": reply(msg.id, { goal: t!.goal }); break;
      case "thread/goal/set": case "thread/goal/clear": {
        if (t!.canAcceptDirectInput === false) { error(msg.id, "agent thread does not accept goal writes"); break; }
        t!.goalWrites.push(clone(msg));
        t!.goal = msg.method.endsWith("clear") ? null : { ...t!.goal, ...p, status: p.status ?? "active" };
        reply(msg.id, { goal: t!.goal, cleared: t!.goal === null }); notify("thread/goal/updated", { threadId: t!.id, goal: t!.goal }); break;
      }
      case "thread/backgroundTerminals/list": reply(msg.id, { data: [], nextCursor: null }); break;
      case "thread/unsubscribe": {
        const subscribed = subscriptions.get(currentConnection)!;
        const status = !t!.loaded ? "notLoaded" : subscribed.has(t!.id) ? "unsubscribed" : "notSubscribed";
        subscribed.delete(t!.id); reply(msg.id, { status }); break;
      }
      case "thread/memoryMode/set": reply(msg.id, { threadId: p.threadId, mode: p.mode }); break;
      default: if (msg.id !== undefined) error(msg.id, `未实现方法 ${msg.method}`, -32601);
    }
    return true;
  }
  function execute(message: RpcMessage, connectionId: string) {
    const previous = currentConnection; currentConnection = connectionId;
    try { processMessage(message); } finally { currentConnection = previous; }
    const id = message.params?.threadId;
    if (id) {
      busy.delete(id);
      const next = queued.get(id)?.shift();
      if (next) dispatch(next.message, next.connectionId);
    }
  }
  function dispatch(message: RpcMessage, connectionId: string) {
    const id = message.params?.threadId;
    if (id && busy.has(id)) { queued.set(id, [...queued.get(id) ?? [], { message, connectionId }]); return; }
    if (id) busy.add(id);
    const fence = [...requestFences].find(([, f]) => f.method === message.method && (!f.threadId || f.threadId === id))?.[0];
    if (fence) {
      requestHeld.set(fence, [...requestHeld.get(fence) ?? [], message]); requestConnections.set(message, connectionId); return;
    }
    execute(message, connectionId);
  }
  function handle(message: RpcMessage, recordCall = true, connectionId = "broker"): boolean {
    if (!subscriptions.has(connectionId)) subscriptions.set(connectionId, new Set());
    if (message.id !== undefined && message.method) responseContexts.set(JSON.stringify([connectionId, message.id]), { method: message.method, threadId: message.params?.threadId, connectionId });
    if (!message.method || message.method.startsWith("fixture/sa1b/")) {
      const previous = currentConnection; currentConnection = connectionId;
      try { return processMessage(message); } finally { currentConnection = previous; }
    }
    if (recordCall) calls.push(clone(message));
    dispatch(clone(message), connectionId); return true;
  }
  function state() {
    return clone({ threads: Object.fromEntries(threads), calls, events, held: Object.fromEntries(held), requestHeld: Object.fromEntries(requestHeld),
      subscriptions: Object.fromEntries([...subscriptions].map(([id, subscribed]) => [id, [...subscribed]])), deliveries,
      queued: Object.fromEntries([...queued].map(([id, requests]) => [id, requests.map(x => x.message)])) });
  }
  return { handle, state };
}
