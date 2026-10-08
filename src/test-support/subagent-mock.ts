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

/**
 * 每线程独立状态的子代理模拟。事件按真实时序（`S4-子代理事件实测`、上游 0.160.0）：
 * - v1 spawnAgent：item/started 的 receiverThreadIds 为空；成功的 item/completed 才带 C；失败没有 C。
 * - v1 sendInput/resumeAgent：item/started 已带接收方。
 * - v2：操作成功后连续发出同一 itemId、内容相同的 subAgentActivity started/completed，不造 collabAgentToolCall。
 * - 子线程的 thread/started 只在 spawnChild.started 为 now（立即）或 held（随扣住的批次放行）时发送；never 不发（实测）。
 * - started 为 held 时，派生成功的证据（v1 completed、v2 活动）与子线程首轮一起扣在 `spawn:<childThreadId>`，
 *   用 fixture/release 放行，可模拟「子线程请求先于派生证据和 thread/started」。
 * - 中断父线程不影响子线程；v2 子线程拒绝 turn/start、turn/steer 与 goal 写入，允许 turn/interrupt。
 * - autoAttach=missing 时子线程的通知与请求不发给本连接，直到收到该子线程的 thread/resume（附着请求）。
 */
export function createSubagentMock(send: (message: unknown) => unknown, options: SubagentMockOptions = {}): SubagentMock {
  if (!options.enabled) return { handle: () => false, state: () => ({}) };
  type Child = {
    parent: string | null; version: "v1" | "v2"; archived: boolean; loaded: boolean; attached: boolean;
    turns: Array<{ id: string; status: string }>; turnCounter: number; goal: unknown;
    interruptMode: "normal" | "never" | "delayed";
    pendingRequests: FixtureMessage[];
    answers: Array<{ requestId: unknown; result?: unknown; error?: unknown; count: number }>;
    interrupts: unknown[]; goalWrites: unknown[]; attachments: unknown[];
  };
  const threads = new Map<string, Child>();
  const holds = new Map<string, unknown[]>();
  const injected = new Map<string, string>();
  const suppressed = new Map<string, unknown[]>();
  let requestCounter = 0;
  let listFault: "pageError" | "duplicateCursor" | "slow" | null = null;

  const thread = (id: string, parent: string | null = null): Child => {
    let value = threads.get(id);
    if (!value) {
      value = {
        parent, version: options.version ?? "v1", archived: false, loaded: true, attached: options.autoAttach !== "missing",
        turns: [], turnCounter: 0, goal: null, interruptMode: "normal",
        pendingRequests: [], answers: [], interrupts: [], goalWrites: [], attachments: [],
      };
      threads.set(id, value);
    }
    return value;
  };
  const isChild = (id: unknown) => typeof id === "string" && threads.get(id)?.parent != null;
  /** 子线程上的消息：未附着时不发给本连接（只记下，附着后补发）。 */
  const emit = (message: any, hold?: string) => {
    if (hold) {
      const list = holds.get(hold) ?? [];
      list.push(message);
      holds.set(hold, list);
      return;
    }
    const target = message?.params?.threadId ?? message?.params?.thread?.id;
    const child = typeof target === "string" ? threads.get(target) : undefined;
    if (child?.parent != null && !child.attached) {
      const list = suppressed.get(target) ?? [];
      list.push(message);
      suppressed.set(target, list);
      return;
    }
    send(message);
  };
  const reply = (id: unknown, result: unknown) => send({ id, result });
  const fail = (id: unknown, message: string, code = -32600) => send({ id, error: { code, message } });
  const threadObject = (id: string) => {
    const t = thread(id);
    const v2 = t.version === "v2";
    return {
      id, parentThreadId: t.parent, forkedFromId: null, preview: "", ephemeral: false, modelProvider: "openai",
      createdAt: 1, updatedAt: 1, status: { type: t.turns.some((x) => x.status === "inProgress") ? "active" : "idle" },
      path: null, cwd: "/tmp", cliVersion: "0.160.0",
      source: t.parent == null ? "vscode" : { subAgent: { thread_spawn: { parent_thread_id: t.parent, depth: depthOf(id),
        agent_path: v2 ? `/root/${id}` : null, agent_nickname: "代理", agent_role: v2 ? null : "default" } } },
      canAcceptDirectInput: !v2, agentNickname: "代理", agentRole: v2 ? null : "default", gitInfo: null, name: null, turns: [],
    };
  };
  const depthOf = (id: string) => {
    let depth = 0;
    let current = threads.get(id)?.parent ?? null;
    while (current != null && depth < 64) { depth++; current = threads.get(current)?.parent ?? null; }
    return depth;
  };
  const isDescendant = (id: string, ancestor: string) => {
    let current = threads.get(id)?.parent ?? null;
    for (let i = 0; current != null && i < 64; i++) {
      if (current === ancestor) return true;
      current = threads.get(current)?.parent ?? null;
    }
    return false;
  };
  const turnMessage = (method: string, threadId: string, turn: { id: string; status: string }) =>
    ({ method, params: { threadId, turn: { id: turn.id, items: [], status: turn.status, error: null } } });
  const startTurn = (threadId: string, hold?: string) => {
    const t = thread(threadId);
    const turn = { id: `${threadId}-turn-${++t.turnCounter}`, status: "inProgress" };
    t.turns.push(turn);
    emit(turnMessage("turn/started", threadId, turn), hold);
    return turn;
  };
  const finishTurn = (threadId: string, status: string) => {
    const turn = thread(threadId).turns.findLast((x) => x.status === "inProgress");
    if (!turn) return null;
    turn.status = status;
    emit(turnMessage("turn/completed", threadId, turn));
    return turn;
  };
  const item = (method: string, threadId: string, turnId: string, value: unknown, hold?: string) =>
    emit({ method, params: { threadId, turnId, item: value } }, hold);
  const collab = (id: string, tool: string, status: string, sender: string, receivers: string[]) =>
    ({ type: "collabAgentToolCall", id, tool, status, senderThreadId: sender, receiverThreadIds: receivers,
      prompt: null, model: null, reasoningEffort: null, agentsStates: {} });
  const activity = (id: string, kind: string, child: string) =>
    ({ type: "subAgentActivity", id, kind, agentThreadId: child, agentPath: `/root/${child}` });
  const status = (threadId: string, type: string, hold?: string) =>
    emit({ method: "thread/status/changed", params: { threadId, status: { type, ...(type === "active" ? { activeFlags: [] } : {}) } } }, hold);
  const record = (id: unknown, message: FixtureMessage) => {
    const owner = injected.get(String(id));
    if (!owner) return false;
    const answers = thread(owner).answers;
    const entry = answers.find((a) => String(a.requestId) === String(id));
    const outcome = "error" in message ? { error: message.error } : { result: message.result };
    if (entry) { entry.count++; Object.assign(entry, outcome); }
    else answers.push({ requestId: id, count: 1, ...outcome });
    thread(owner).pendingRequests = thread(owner).pendingRequests.filter((r) => String(r.id) !== String(id));
    return true;
  };

  const control = (msg: FixtureMessage): boolean => {
    const p = msg.params ?? {};
    switch (msg.method) {
      case "fixture/spawnChild": {
        const child = thread(p.childThreadId, p.parentThreadId);
        thread(p.parentThreadId);
        child.parent = p.parentThreadId;
        child.version = p.version ?? options.version ?? "v1";
        child.archived = p.archived === true;
        child.loaded = p.loaded !== false;
        const hold = p.started === "held" ? `spawn:${p.childThreadId}` : undefined;
        if (p.started === "now") emit({ method: "thread/started", params: { thread: threadObject(p.childThreadId) } });
        if (child.version === "v1") {
          item("item/started", p.parentThreadId, p.parentTurnId, collab(p.itemId, "spawnAgent", "inProgress", p.parentThreadId, []));
          if (hold) emit({ method: "thread/started", params: { thread: threadObject(p.childThreadId) } }, hold);
          status(p.childThreadId, "idle", hold);
          item("item/completed", p.parentThreadId, p.parentTurnId, collab(p.itemId, "spawnAgent", "completed", p.parentThreadId, [p.childThreadId]), hold);
        } else {
          if (hold) emit({ method: "thread/started", params: { thread: threadObject(p.childThreadId) } }, hold);
          item("item/started", p.parentThreadId, p.parentTurnId, activity(p.itemId, "started", p.childThreadId), hold);
          status(p.childThreadId, "idle", hold);
          item("item/completed", p.parentThreadId, p.parentTurnId, activity(p.itemId, "started", p.childThreadId), hold);
        }
        const turn = startTurn(p.childThreadId, hold);
        reply(msg.id, { childThreadId: p.childThreadId, turnId: turn.id, holdId: hold ?? null });
        return true;
      }
      case "fixture/reuseChild": {
        const child = thread(p.childThreadId);
        const v1 = p.mode.startsWith("v1");
        const tool = p.mode === "v1ResumeNoop" ? "resumeAgent" : "sendInput";
        const opens = !p.fail && (p.mode === "v1Input" || p.mode === "v2Followup");
        let turn: { id: string } | null = null;
        if (v1) {
          item("item/started", p.parentThreadId, p.parentTurnId, collab(p.itemId, tool, "inProgress", p.parentThreadId, [p.childThreadId]));
          if (opens) turn = startTurn(p.childThreadId);
          item("item/completed", p.parentThreadId, p.parentTurnId,
            collab(p.itemId, tool, p.fail ? "failed" : "completed", p.parentThreadId, [p.childThreadId]));
        } else if (!p.fail) {
          // v2 interacted 只在成功后发出；失败不发活动条目。
          item("item/started", p.parentThreadId, p.parentTurnId, activity(p.itemId, "interacted", p.childThreadId));
          item("item/completed", p.parentThreadId, p.parentTurnId, activity(p.itemId, "interacted", p.childThreadId));
          if (opens) turn = startTurn(p.childThreadId);
        }
        void child;
        reply(msg.id, { childThreadId: p.childThreadId, turnId: turn?.id ?? null });
        return true;
      }
      case "fixture/turn": {
        const t = thread(p.threadId);
        if (p.action === "start") {
          const turn = startTurn(p.threadId);
          reply(msg.id, { turnId: turn.id });
        } else {
          const turn = finishTurn(p.threadId, p.action === "fail" ? "failed" : "completed");
          if (turn && t.version === "v2" && t.parent != null) {
            // v2 子线程完成时父线程收到 completed 活动（新 itemId）。
            const parentTurn = thread(t.parent).turns.findLast((x) => x.status === "inProgress");
            if (parentTurn) {
              item("item/started", t.parent, parentTurn.id, activity(`${turn.id}-done`, "completed", p.threadId));
              item("item/completed", t.parent, parentTurn.id, activity(`${turn.id}-done`, "completed", p.threadId));
            }
          }
          reply(msg.id, { turnId: turn?.id ?? null });
        }
        return true;
      }
      case "fixture/inject": {
        const kind = String(p.kind);
        const payload = (p.payload ?? {}) as { method?: string; params?: Record<string, unknown> };
        const params = { threadId: p.threadId, ...(payload.params ?? {}) };
        if (kind === "request") {
          const requestId = p.requestId ?? `sa-${++requestCounter}`;
          injected.set(String(requestId), p.threadId);
          const message = { id: requestId, method: payload.method ?? "item/commandExecution/requestApproval", params };
          thread(p.threadId).pendingRequests.push(message);
          emit(message, p.hold);
          reply(msg.id, { requestId });
        } else {
          emit({ method: payload.method, params }, p.hold);
          reply(msg.id, {});
        }
        return true;
      }
      case "fixture/release": {
        const list = holds.get(p.holdId) ?? [];
        holds.delete(p.holdId);
        for (const message of list) emit(message);
        reply(msg.id, { released: list.length });
        return true;
      }
      case "fixture/interruptBehavior":
        thread(p.threadId).interruptMode = p.mode;
        reply(msg.id, {});
        return true;
      case "fixture/listFault":
        listFault = p.mode;
        reply(msg.id, {});
        return true;
      case "fixture/goal": {
        const t = thread(p.threadId);
        t.goal = { threadId: p.threadId, objective: "模拟目标", status: p.status, tokenBudget: null, tokensUsed: 0, timeUsedSeconds: 0, createdAt: 1, updatedAt: 1 };
        emit({ method: "thread/goal/updated", params: { threadId: p.threadId, turnId: null, goal: t.goal } });
        reply(msg.id, {});
        return true;
      }
      case "fixture/pendingInitStuck": {
        // 派生停在 pendingInit：父线程只有 started 条目，子线程已加载但没有任何轮次。
        const child = thread(p.childThreadId);
        const parentTurn = child.parent ? thread(child.parent).turns.findLast((x) => x.status === "inProgress") : undefined;
        if (child.parent && parentTurn) {
          item("item/started", child.parent, parentTurn.id, collab(`${p.childThreadId}-stuck`, "spawnAgent", "inProgress", child.parent, []));
        }
        reply(msg.id, {});
        return true;
      }
      case "fixture/state":
        reply(msg.id, state());
        return true;
      default:
        return false;
    }
  };

  const listPage = (msg: FixtureMessage) => {
    const p = msg.params ?? {};
    if (listFault === "slow") {
      holds.set("list", [...(holds.get("list") ?? []), null]);
      const pending = holds.get("list")!;
      pending[pending.length - 1] = { id: msg.id, result: listResult(p) };
      return;
    }
    if (listFault === "pageError") {
      listFault = null;
      fail(msg.id, "模拟分页失败", -32603);
      return;
    }
    reply(msg.id, listResult(p));
  };
  const listResult = (p: any) => {
    const all = [...threads.keys()].filter((id) => isDescendant(id, p.ancestorThreadId) && threads.get(id)!.archived === (p.archived === true));
    const start = Number(p.cursor ?? 0) || 0;
    const limit = Number(p.limit ?? 100) || 100;
    const page = all.slice(start, start + limit);
    const next = start + limit < all.length ? String(start + limit) : null;
    return { data: page.map(threadObject), nextCursor: listFault === "duplicateCursor" ? "dup" : next, backwardsCursor: null };
  };

  const app = (msg: FixtureMessage): boolean => {
    const p = msg.params ?? {};
    switch (msg.method) {
      case "thread/read":
        // 只接管子线程；父线程的读取仍交给所在夹具。
        if (!isChild(p.threadId)) return false;
        reply(msg.id, { thread: threadObject(p.threadId) });
        return true;
      case "thread/list":
        if (typeof p.ancestorThreadId !== "string") return false;
        listPage(msg);
        return true;
      case "thread/loaded/list":
        reply(msg.id, { data: [...threads.entries()].filter(([, t]) => t.parent != null && t.loaded && !t.archived).map(([id]) => id), nextCursor: null });
        return true;
      case "thread/resume": {
        if (!isChild(p.threadId)) return false;
        const t = thread(p.threadId);
        t.attachments.push(p);
        t.attached = true;
        reply(msg.id, { thread: threadObject(p.threadId), model: "mock-model", cwd: "/tmp", approvalPolicy: "on-request", sandbox: null });
        for (const message of suppressed.get(p.threadId) ?? []) send(message);
        suppressed.delete(p.threadId);
        return true;
      }
      case "turn/interrupt": {
        if (!isChild(p.threadId)) return false;
        const t = thread(p.threadId);
        t.interrupts.push(p);
        reply(msg.id, {});
        if (t.interruptMode === "normal") finishTurn(p.threadId, "interrupted");
        else if (t.interruptMode === "delayed") setTimeout(() => finishTurn(p.threadId, "interrupted"), 300);
        return true;
      }
      case "turn/start":
      case "turn/steer": {
        if (!isChild(p.threadId)) return false;
        const t = thread(p.threadId);
        if (t.version === "v2") { fail(msg.id, "agent thread does not accept direct input"); return true; }
        const turn = msg.method === "turn/start" ? startTurn(p.threadId) : t.turns.findLast((x) => x.status === "inProgress");
        if (!turn) { fail(msg.id, "no active turn to steer"); return true; }
        reply(msg.id, msg.method === "turn/start" ? { turn: { id: turn.id, items: [], status: "inProgress", error: null } } : { turnId: turn.id });
        return true;
      }
      case "thread/goal/set":
      case "thread/goal/clear": {
        if (!isChild(p.threadId)) return false;
        const t = thread(p.threadId);
        if (t.version === "v2") { fail(msg.id, "agent thread does not accept goal writes"); return true; }
        t.goalWrites.push({ method: msg.method, ...p });
        t.goal = msg.method === "thread/goal/clear" ? null : { ...(t.goal as object ?? {}), threadId: p.threadId, status: p.status ?? "active" };
        reply(msg.id, msg.method === "thread/goal/clear" ? { cleared: true } : { goal: t.goal });
        return true;
      }
      case "thread/goal/get":
        if (!isChild(p.threadId)) return false;
        reply(msg.id, { goal: thread(p.threadId).goal });
        return true;
      default:
        return false;
    }
  };

  function state(): Record<string, SubagentThreadState> {
    const out: Record<string, SubagentThreadState> = {};
    for (const [id, t] of threads) {
      out[id] = {
        turns: t.turns.map((x) => ({ ...x })), pendingRequests: [...t.pendingRequests],
        answers: t.answers.map((a) => ({ ...a })), interrupts: [...t.interrupts], goalWrites: [...t.goalWrites], attachments: [...t.attachments],
      };
    }
    return out;
  }

  return {
    handle(message: FixtureMessage): boolean {
      if (message.method === undefined) {
        // 对模拟主动请求的回答：记下实际收到的回答与次数。
        return message.id !== undefined && record(message.id, message);
      }
      if (typeof message.method === "string" && message.method.startsWith("fixture/")) {
        if (message.method === "fixture/release" && message.params?.holdId === "list") {
          const list = (holds.get("list") ?? []) as unknown[];
          holds.delete("list");
          listFault = null;
          for (const response of list) if (response) send(response);
          reply(message.id, { released: list.length });
          return true;
        }
        return control(message);
      }
      return app(message);
    },
    state,
  };
}
