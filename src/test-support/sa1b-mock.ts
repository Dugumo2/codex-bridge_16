/** SA1b 独立模拟上游；只用于测试，不推断或替 broker 实现收敛。 */
import { appendFileSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { createBackgroundTerminalsMock } from "./background-terminals-mock";
export type ProtocolVersion = "v1" | "v2";
export type DeliveryMode = "normal" | "delayed" | "dropped";
export type StartMode = "started" | "steered" | "replyLost" | "transportError";
export interface RpcMessage { id?: any; method?: string; params?: any; result?: any; error?: any }
export interface MockTurn { id: string; status: string; items: any[]; error: any }
export interface MockThread {
  experimentalRawEvents?: boolean;
  id: string; parentThreadId: string | null; version: ProtocolVersion; turns: MockTurn[];
  loaded: boolean; archived: boolean; pendingInit: boolean; stored: boolean; closing: boolean; canAcceptDirectInput?: boolean;
  goal: any; usage: any; delivery: DeliveryMode; startMode: StartMode;
  interrupts: any[]; goalWrites: any[]; inputs: any[]; answers: any[]; pendingRequests: RpcMessage[];
}
/** 仅保存模拟服务器的持久事实；连接、通知队列与订阅必须在重连后重新建立。 */
export interface Sa1bSnapshot { version: 1; serial: number; threads: Record<string, MockThread> }
export interface Sa1bOptions {
  version?: ProtocolVersion; pageSize?: number; snapshot?: Sa1bSnapshot;
  /** 在祖先分页末尾再次返回指定线程，模拟分页间重复但游标仍前进。 */
  duplicateListIds?: string[];
  /**
   * 会话文件根目录（相当于 CODEX_HOME）。设置后与真实 Codex 一样：每个线程写一个 jsonl 会话文件，
   * thread/read、thread/resume 回包的 thread.path 给出它；文件在磁盘上，重启（快照恢复）后路径不变、内容保留。
   * raw 开关只影响实时通知，不影响会话文件。
   */
  rolloutDir?: string;
}

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
  // 只扣放 raw 消费回执，轮次推进仍由现有 turn/reuse 入口控制。
  const queuedRawInputs = new Map<string, any[]>();
  // 跟进排到下一轮的子线程：当前轮结束后自动开下一轮，与上游一致（留言不会触发）。
  const autoNextTurn = new Set<string>();
  // holdConsume 扣住的消费记录：跟进已被当前轮吸收，记录晚一点才落盘并通知，用来看清这段中间状态。
  const heldConsumes = new Map<string, { turnId: string; input: any }[]>();
  let currentConnection = "broker";
  const broadcasts = new Set(["thread/started", "thread/status/changed", "thread/goal/updated", "thread/goal/cleared", "thread/closed",
    "thread/name/updated", "thread/archived", "thread/unarchived", "thread/deleted", "thread/reverted", "thread/attachment/updated", "thread/project/updated"]);
  const faults = new Map<string, { mode: string; remaining: number }>();
  const background = createBackgroundTerminalsMock(message => emit(message as RpcMessage), { enabled: true });
  let serial = options.snapshot?.serial ?? 0;
  const clone = <T>(value: T): T => structuredClone(value);
  // 会话文件：行形如 {"timestamp":"…Z","type":…,"payload":…}，按写入顺序追加；行所在的轮由之前最近的 task_started 决定。
  const rolloutPath = (id: string): string | null => options.rolloutDir
    ? join(options.rolloutDir, "sessions", "2026", "11", "05", `rollout-2026-11-05T00-00-00-${id}.jsonl`) : null;
  const rolloutLine = (type: string, payload: any) => JSON.stringify({ timestamp: new Date().toISOString(), type, payload }) + "\n";
  const writeRollout = (id: string, type: string, payload: any) => {
    const path = rolloutPath(id);
    if (!path) return;
    mkdirSync(dirname(path), { recursive: true });
    appendFileSync(path, rolloutLine(type, payload));
  };
  const ensureRollout = (id: string) => {
    const path = rolloutPath(id);
    if (path && !existsSync(path)) writeRollout(id, "session_meta", { id, cwd: process.env.TEMP!, originator: "sa1b-mock", cli_version: "0.160.0" });
  };
  if (options.snapshot) {
    if (options.snapshot.version !== 1 || !Number.isSafeInteger(serial) || serial < 0) throw new Error("历史快照版本或编号状态无效");
    for (const [id, saved] of Object.entries(options.snapshot.threads)) {
      if (saved.id !== id) throw new Error("历史快照线程键与标识不一致");
      // raw 是监听器的内存开关，app-server 重启不从持久快照恢复。
      threads.set(id, { ...clone(saved), experimentalRawEvents: false });
    }
  }
  const thread = (id: string): MockThread => {
    let t = threads.get(id);
    if (!t) {
      t = { id, parentThreadId: null, version: options.version ?? "v1", turns: [], loaded: true, archived: false, stored: true, closing: false,
        pendingInit: false, experimentalRawEvents: false, canAcceptDirectInput: true, goal: null, usage: null, delivery: "normal", startMode: "started",
        interrupts: [], goalWrites: [], inputs: [], answers: [], pendingRequests: [] };
      threads.set(id, t);
      ensureRollout(id);
      for (const subscribed of subscriptions.values()) subscribed.add(id);
    }
    return t;
  };
  const emit = (message: RpcMessage, hold?: string, captured?: string[], connectionId = currentConnection) => {
    const target = message.params?.threadId ?? message.params?.thread?.id;
    // 与上游一致：raw 开关作用于源线程，先过滤再进入通知扣放设施。
    if ((message.method === "rawResponseItem/completed" || message.method === "rawResponse/completed")
      && threads.get(target)?.experimentalRawEvents !== true) return;
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
  // 与真实探针一致：target 用短名、路径带 /root 前缀，二者都不是线程 ID。
  const agentName = (t: MockThread): string => `agent_${t.id}`;
  const agentPath = (t: MockThread): string => t.parentThreadId === null ? "/root" : `/root/${agentName(t)}`;
  const meta = (t: MockThread, includeTurns = false) => ({
    id: t.id, parentThreadId: t.parentThreadId, forkedFromId: null, preview: "", ephemeral: false,
    modelProvider: "openai", createdAt: 1, updatedAt: 1, path: rolloutPath(t.id), cwd: process.env.TEMP!, cliVersion: "0.160.0",
    status: { type: !t.loaded ? "notLoaded" : t.pendingInit ? "active" : t.turns.some(x => x.status === "inProgress") ? "active" : "idle", activeFlags: [] },
    source: t.parentThreadId ? { subAgent: { thread_spawn: { parent_thread_id: t.parentThreadId,
      depth: ancestors(t).length, agent_path: t.version === "v2" ? agentPath(t) : null,
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
    writeRollout(t.id, "turn_context", { turn_id: turn.id, cwd: process.env.TEMP! });
    writeRollout(t.id, "event_msg", { type: "task_started", turn_id: turn.id });
    // 排队的消息在下一轮开始时才记入，记在这一轮里。
    for (const value of queuedRawInputs.get(t.id) ?? []) writeRollout(t.id, "response_item", value);
    if (announce) notify("turn/started", { threadId: t.id, turn });
    for (const value of queuedRawInputs.get(t.id) ?? []) notify("rawResponseItem/completed", { threadId: t.id, turnId: turn.id, item: value });
    queuedRawInputs.delete(t.id);
    return turn;
  }
  function item(t: MockThread, turn: MockTurn, value: any, announce = true) {
    const old = turn.items.findIndex(x => x.id === value.id);
    if (old < 0) turn.items.push(clone(value)); else turn.items[old] = clone(value);
    if (announce) notify("item/completed", { threadId: t.id, turnId: turn.id, item: value });
  }
  function finish(t: MockThread, turn: MockTurn, status: string, output?: string, announce = true) {
    finishTurn(t, turn, status, output, announce);
    if (autoNextTurn.delete(t.id) && status !== "shutdown") start(t);
  }
  function finishTurn(t: MockThread, turn: MockTurn, status: string, output?: string, announce = true) {
    if (output !== undefined) item(t, turn, { id: `answer-${turn.id}`, type: "agentMessage", text: output, phase: "final_answer" }, announce);
    // errored/shutdown 是 CollabAgentStatus，不是协议 TurnStatus。
    turn.status = status === "errored" ? "failed" : status === "shutdown" ? "interrupted" : status;
    writeRollout(t.id, "event_msg", { type: "task_complete", turn_id: turn.id });
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
        const activity = { type: "subAgentActivity", id: `done-${turn.id}`, kind: "completed", agentThreadId: t.id, agentPath: agentPath(t) };
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
      case "snapshot": send({ id: msg.id, result: snapshot() }, currentConnection); break;
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
        const fresh = !threads.has(p.childThreadId);
        const child = thread(p.childThreadId); child.parentThreadId = parent.id; child.version = p.version ?? parent.version;
        // 与实测一致：新派子代理的会话文件开头抄有父线程的历史（别的轮次的开始与结束等），之后才是它自己的轮。
        const parentRollout = rolloutPath(parent.id), childRollout = rolloutPath(child.id);
        if (fresh && child.version === "v2" && parentRollout && childRollout && existsSync(parentRollout)) {
          for (const line of readFileSync(parentRollout, "utf8").split("\n").filter(Boolean)) {
            const old = JSON.parse(line);
            if (old.type !== "session_meta") writeRollout(child.id, old.type, old.payload);
          }
        }
        child.experimentalRawEvents ||= parent.experimentalRawEvents === true;
        child.canAcceptDirectInput = child.version !== "v2"; child.pendingInit = p.pendingInit === true;
        const id = p.itemId ?? `spawn-${++serial}`;
        const value = child.version === "v1"
          ? { type: "collabAgentToolCall", id, tool: "spawnAgent", status: "inProgress", senderThreadId: parent.id, receiverThreadIds: [], prompt: null, model: null, reasoningEffort: null, agentsStates: {} }
          : { type: "subAgentActivity", id, kind: "started", agentThreadId: child.id, agentPath: agentPath(child) };
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
        if (!child.loaded) {
          // 对应 core 恢复运行时后的 thread-created 监听器：为当前连接重新附着。
          // 只恢复本次复用的线程，不回放旧 spawn 条目或历史轮通知。
          child.loaded = true;
          child.experimentalRawEvents = parent.experimentalRawEvents === true;
          for (const subscribed of subscriptions.values()) subscribed.add(child.id);
          if (p.announce !== false) notify("thread/started", { thread: meta(child) });
        }
        const value = child.version === "v1"
          ? { type: "collabAgentToolCall", id: `reuse-${++serial}`, tool: "sendInput", status: "completed", senderThreadId: parent.id, receiverThreadIds: [child.id], agentsStates: {} }
          : { type: "subAgentActivity", id: `reuse-${++serial}`, kind: "interacted", agentThreadId: child.id, agentPath: agentPath(child) };
        const message = p.message ?? `跟进:${value.id}`;
        // 与真实探针一致：工具参数和接收方输入里的正文是同一段密文，同一明文每次调用密文不同；
        // plaintext 对应不加密的模型（探针二），同一明文的正文相同。send_message 只入队，不开新轮。
        const queueOnly = p.tool === "send_message";
        const body = p.plaintext ? message : `gAAAAA${Buffer.from(`${value.id}:${message}`).toString("base64")}`;
        // 与探针二一致，由子代理当时的状态决定：空闲开新轮；忙且没答完，被当前轮吸收；已答完，排到下一轮，
        // 当前轮结束后自动开。显式的 open、consumeOnNextTurn 只用来拉开时序。
        const current = child.turns.at(-1);
        const decides = child.version === "v2" && !queueOnly && p.open === undefined && p.consumeOnNextTurn === undefined && current?.status === "inProgress";
        const answered = decides && current!.items.some(x => x.phase === "final_answer");
        const absorbed = decides && !answered, deferred = decides && answered;
        if (child.version === "v2") {
          const call = { type: "function_call", call_id: value.id, name: queueOnly ? "send_message" : "followup_task", namespace: "collaboration",
            arguments: JSON.stringify({ target: agentName(child), message: body }) };
          // 工具调用先进写入队列并落盘，工具才执行；raw 关闭时会话文件照写。
          writeRollout(parent.id, "response_item", call);
          notify("rawResponseItem/completed", { threadId: parent.id, turnId: pt.id, item: call });
        }
        // 上游先提交消息后通知 interacted；也支持 sendInput 并入既有轮、不再开轮。
        let turn = p.open === false || queueOnly || decides ? current : p.order === "turnFirst" ? start(child) : undefined;
        notify("item/started", { threadId: parent.id, turnId: pt.id, item: value }); item(parent, pt, value);
        if (!queueOnly && !decides && p.open !== false && !turn) turn = start(child);
        if (child.version === "v2") {
          const header = `Message Type: ${queueOnly ? "MESSAGE" : "NEW_TASK"}\nTask name: ${agentPath(child)}\nSender: ${agentPath(parent)}\nPayload:\n`;
          const input = { type: "agent_message", id: `delivery-${value.id}`, author: agentPath(parent), recipient: agentPath(child),
            content: p.plaintext ? [{ type: "input_text", text: `${header}${body}` }]
              : [{ type: "input_text", text: header }, { type: "encrypted_content", encrypted_content: body }] };
          if (deferred) autoNextTurn.add(child.id);
          if (p.consumeOnNextTurn || deferred || (queueOnly && turn?.status !== "inProgress")) queuedRawInputs.set(child.id, [...queuedRawInputs.get(child.id) ?? [], input]);
          else if (turn && p.holdConsume) heldConsumes.set(child.id, [...heldConsumes.get(child.id) ?? [], { turnId: turn.id, input }]);
          else if (turn) {
            // 消费记录记在消费它的轮里（被忙碌轮吸收时记在当前轮）。
            writeRollout(child.id, "response_item", input);
            notify("rawResponseItem/completed", { threadId: child.id, turnId: turn.id, item: input });
          }
        }
        reply(msg.id, { turnId: turn?.id }); break;
      }
      case "consumeRelease": {
        const turn = t!.turns.at(-1);
        const held = heldConsumes.get(t!.id) ?? [];
        // 吸收它的就是扣住时的那一轮：换了轮再放行不是真实时序，要测顺延用已答完的状态。
        if (!turn || turn.status !== "inProgress" || held.some(entry => entry.turnId !== turn.id)) { error(msg.id, "放行消费记录时子代理必须还在这一轮"); break; }
        for (const { input: value } of held) {
          writeRollout(t!.id, "response_item", value);
          notify("rawResponseItem/completed", { threadId: t!.id, turnId: turn.id, item: value });
        }
        heldConsumes.delete(t!.id);
        reply(msg.id, { turnId: turn.id }); break;
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
      case "thread/start": { const root = thread(p.id ?? "root"); root.experimentalRawEvents ||= p.experimentalRawEvents === true; notify("thread/started", { thread: meta(root) }); reply(msg.id, { thread: meta(root), model: p.model ?? "mock-model", cwd: p.cwd, approvalPolicy: "never", sandbox: { type: "dangerFullAccess" } }); break; }
      case "thread/resume": {
        if (!t!.stored) { error(msg.id, `no rollout found for thread id ${t!.id}`); break; }
        if (t!.closing) { error(msg.id, `thread ${t!.id} is closing; retry thread/resume after the thread is closed`); break; }
        if (!t!.loaded) {
          t!.loaded = true;
          t!.experimentalRawEvents = false;
          for (const subscribed of subscriptions.values()) subscribed.add(t!.id);
          notify("thread/started", { thread: meta(t!) });
        }
        subscriptions.get(currentConnection)!.add(t!.id);
        reply(msg.id, { thread: meta(t!), model: p.model ?? "mock-model", cwd: process.env.TEMP!, approvalPolicy: "never", sandbox: { type: "dangerFullAccess" } }); break;
      }
      case "thread/read":
        if (!t!.stored && !t!.loaded) error(msg.id, `thread not loaded: ${t!.id}`);
        else reply(msg.id, { thread: meta(t!, p.includeTurns === true) }); break;
      case "thread/list": {
        const listed = [...threads.values()].filter(x => (!p.ancestorThreadId || ancestors(x).includes(p.ancestorThreadId)) && x.archived === (p.archived === true));
        const duplicates = p.ancestorThreadId ? (options.duplicateListIds ?? []).flatMap(id => listed.filter(x => x.id === id)) : [];
        page(msg.id, [...listed, ...duplicates].map(x => meta(x)), p); break;
      }
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
  function snapshot(): Sa1bSnapshot { return clone({ version: 1, serial, threads: Object.fromEntries(threads) }); }
  return { handle, state, snapshot };
}
