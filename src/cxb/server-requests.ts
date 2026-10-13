// 服务端请求策略、托管交接与待答摘要（S4 N2 乙路）。
import { describeGuardianAction } from "../guardian";
import { questionSummary, sanitizeForTerminal } from "../questions";
import { NO_RESPONSE } from "../rpc";
import {
  CxbError, rpcFailure, type AttachServerRequests, type DecidePolicy, type ErrorCode,
  type Handover, type HandoverResult, type InboxItem, type RenderPending, type ServerRequestRouterOptions,
} from "./contracts";

function object(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown> : {};
}

export const decidePolicy: DecidePolicy = (policy, method, params) => {
  if (method === "item/tool/call") return { kind: "methodNotFound" };
  // 拍板 L、28：提问与审批策略无关，Default 的非阻塞标记也不能代答。
  if (method === "item/tool/requestUserInput" || policy !== "never") return { kind: "inbox" };
  switch (method) {
    case "item/commandExecution/requestApproval":
    case "item/fileChange/requestApproval":
      return { kind: "answer", result: { decision: "accept" } };
    case "item/permissions/requestApproval":
      return { kind: "answer", result: { scope: "session", permissions: object(params).permissions ?? {} } };
    case "mcpServer/elicitation/request":
      return { kind: "answer", result: { action: "accept", content: {} } };
    default:
      return { kind: "inbox" };
  }
};

/**
 * 服务端请求属于本次运行的哪一部分（SA1 接口第 5 节）：`threadId` 等于当前执行线程照旧处理；
 * 否则只接受 broker 标注的子线程请求——`cxbOrigin.rootThreadId` 等于本次运行的逻辑根，runId 已知时还要一致，
 * 来源线程与请求的 threadId 相同。broker 只把子请求转给运行的 owner 连接，早于启动回包到达的也照收。
 */
export function requestOrigin(opts: Pick<ServerRequestRouterOptions, "threadId" | "logicalRootThreadId" | "getRunId">,
  params: unknown): "execution" | "child" | null {
  const p = object(params);
  if (p.threadId === opts.threadId) return "execution";
  const origin = object(p.cxbOrigin);
  const root = opts.logicalRootThreadId;
  if (!root || typeof origin.rootThreadId !== "string" || origin.rootThreadId !== root) return null;
  if (typeof p.threadId !== "string" || origin.sourceThreadId !== p.threadId) return null;
  const runId = opts.getRunId?.();
  if (runId && origin.runId !== runId) return null;
  return "child";
}

export const attachServerRequests: AttachServerRequests = (rpc, opts) => {
  let active = true;
  let needsHuman = false;
  const log = (line: string) => {
    try { opts.log?.(line); } catch { /* 输出层记录失败，诊断不能导致意外回包。 */ }
  };
  const unsubscribe = rpc.onAnyRequest((method, params) => {
    // RPC 已排入微任务、但尚未调用处理器时撤销，也必须保持静默。
    if (!active) return NO_RESPONSE;
    const origin = requestOrigin(opts, params);
    if (!origin) {
      log("忽略不属于当前运行的服务端请求");
      return NO_RESPONSE;
    }
    if (origin === "child") log("服务端请求来自本次运行的子代理线程");
    const decision = decidePolicy(opts.policy, method, params);
    // 未知方法名也可能含任意数据，只记录白名单类别。
    const kind = method === "item/tool/requestUserInput" ? "提问"
      : method === "mcpServer/elicitation/request" ? "MCP"
      : method === "item/tool/call" ? "动态工具"
      : ["item/commandExecution/requestApproval", "item/fileChange/requestApproval", "item/permissions/requestApproval"].includes(method) ? "审批" : "其他";
    log(`服务端请求：${kind}`);
    if (decision.kind === "answer") return decision.result;
    if (decision.kind === "methodNotFound") {
      // rpc.ts 的处理器错误以数值 code 编码；这是只送往对端的协议错误，
      // 不交给 Output 的命令错误映射。保留 CxbError 实例及其本地退出码。
      throw Object.assign(new CxbError("LOCAL_ERROR", "不支持的服务端请求方法"), { code: -32601 });
    }
    if (!needsHuman) {
      needsHuman = true;
      try { opts.onNeedsHuman(method); } catch {
        // 即使交接启动回调失败，也不能由 RPC 把异常变成对问题的拒绝回答。
        log("启动人工处理失败，待答请求保持未回答");
      }
    }
    return NO_RESPONSE;
  });
  return () => {
    if (!active) return;
    active = false;
    unsubscribe();
  };
};

export const handover: Handover = async (rpc, args, opts = {}) => {
  const timeoutMs = opts.timeoutMs ?? 30_000;
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0 || timeoutMs > 2_147_483_647) {
    throw new CxbError("USAGE", "交接期限必须是有效的正数毫秒值");
  }
  const deadline = performance.now() + timeoutMs;
  // 每次重试使用交接开始时的同一份参数，避免调用方中途修改对象。
  const params = { ...args };
  let disconnected = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let retryTimer: ReturnType<typeof setTimeout> | undefined;
  let rejectAttempt: ((error: CxbError) => void) | undefined;
  const cancelled = () => new CxbError("INTERRUPTED", "交接已取消", { id: args.threadId });
  const expired = () => new CxbError(disconnected ? "BROKER_UNAVAILABLE" : "TIMEOUT",
    disconnected ? "连接已断开，无法确认交接结果" : "确认交接结果超时", { retryable: true, id: args.threadId });
  let abort: () => void = () => {};
  let unsubscribe: () => void = () => {};
  const interrupted = new Promise<never>((_, reject) => {
    abort = () => reject(cancelled());
    timer = setTimeout(() => reject(expired()), timeoutMs);
  });
  // 后续任何路径都观察拒绝，防止预先取消时产生未处理拒绝。
  void interrupted.catch(() => undefined);
  try {
    opts.signal?.addEventListener("abort", abort, { once: true });
    if (opts.signal?.aborted) throw cancelled();
    unsubscribe = rpc.onClose(() => {
      disconnected = true;
      rejectAttempt?.(new CxbError("BROKER_UNAVAILABLE", "交接回包前连接断开"));
    });
    for (;;) {
      if (opts.signal?.aborted) throw cancelled();
      const left = deadline - performance.now();
      if (left <= 0) throw expired();
      try {
        const closed = new Promise<never>((_, reject) => { rejectAttempt = reject; });
        // 为丢失回包预留重试机会；RPC 本身也有期限，不留下无限 pending。
        return await Promise.race([
          rpc.request<HandoverResult>("broker/threads/detach", params, { timeoutMs: Math.max(1, Math.min(5_000, Math.floor(left / 2))) }),
          closed, interrupted,
        ]);
      } catch (error) {
        if (opts.signal?.aborted) throw cancelled();
        if (performance.now() >= deadline) throw expired();
        const remote = rpcFailure(error);
        const retryable = remote.code === -32000 && (remote.reason === "detachPending" || remote.reason === "settlingTimeout");
        if (remote.code !== null && !retryable) {
          const mapping: Record<string, [ErrorCode, string]> = {
            notOwner: ["THREAD_HELD", "当前连接不是线程所有者，可用 cxb wait 观察"],
            joined: ["THREAD_HELD", "旁观运行不能交接，可用 cxb wait 观察"],
            unknownRun: ["NOT_FOUND", "找不到要交接的运行"],
            runMismatch: ["NOT_FOUND", "要交接的运行不属于该线程"],
          };
          const [code, message] = (remote.code === -32000 ? mapping[remote.reason ?? ""] : undefined)
            ?? ["BROKER_UNAVAILABLE", "broker 拒绝交接请求"];
          throw new CxbError(code, message, { id: args.threadId, cause: error });
        }
        if (!retryable && error instanceof CxbError && !["BROKER_UNAVAILABLE", "TIMEOUT"].includes(error.code)) throw error;
        // 连接已断开：同一个 BrokerRpc 无法恢复，交接是否已生效未知，立即如实报告，不空转到期限。
        if (disconnected) throw expired();
        // BrokerRpc 没有重连句柄：同一接口可原样重试；永久断开到总期限后明确失败。
        await Promise.race([
          new Promise<void>((resolve) => { retryTimer = setTimeout(resolve, Math.min(100, Math.max(1, deadline - performance.now()))); }),
          interrupted,
        ]);
      } finally { rejectAttempt = undefined; }
    }
  } finally {
    clearTimeout(timer);
    clearTimeout(retryTimer);
    opts.signal?.removeEventListener("abort", abort);
    unsubscribe();
  }
};

const SECRET_KEY = /token|cookie|authorization|password|passwd|secret|api[\W_]*key|credential/i;

/** N4 inbox 可复用；先收集秘密再渲染，连同摘要中重复引用的原值一起隐藏。 */
export function redactParams(params: unknown): unknown {
  const secrets = new Set<string>();
  function collect(value: unknown, sensitive = false): void {
    if (typeof value === "string") { if (sensitive && value) secrets.add(value); return; }
    if (Array.isArray(value)) { for (const child of value) collect(child, sensitive); return; }
    for (const [key, child] of Object.entries(object(value))) collect(child, sensitive || SECRET_KEY.test(key));
  }
  collect(params);
  const values = [...secrets].sort((a, b) => b.length - a.length);
  function clean(value: unknown): unknown {
    if (typeof value === "string") {
      let result = value;
      for (const secret of values) result = result.split(secret).join("***");
      // URL 查询、命令参数及头部文本也可能携带凭据。
      result = result.replace(/\bBearer\s+[^\s,;]+/gi, "Bearer ***")
        .replace(/\bsk-(?:proj-)?[A-Za-z0-9_-]+/g, "***")
        .replace(/\beyJ[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g, "***")
        .replace(/([?&][^\s?&=#]*(?:key|token|secret|password|auth|signature)[^\s?&=#]*=)[^\s&#]*/gi, "$1***")
        .replace(/^(\s*(?:Authorization|Cookie|Set-Cookie)\s*:)\s*[^\r\n]*/gim, "$1 ***")
        .replace(/\b[A-Za-z0-9+/=]{32,}\b/g, "***")
        .replace(/((?:[\w-]*(?:token|cookie|authorization|password|passwd|secret|api[_-]?key|credential)[\w-]*)\s*[=:]\s*)(?:"[^"]*"|'[^']*'|[^\s&;]+)/gi, "$1***")
        .replace(/(https?:\/\/)[^\s/@]+:[^\s/@]+@/gi, "$1***@");
      result = result.replace(/\u001b(?:\[[0-?]*[ -/]*[@-~]|\][^\u0007\u001b]*(?:\u0007|\u001b\\)|[@-_])|\u009b[0-?]*[ -/]*[@-~]/g, "");
      return sanitizeForTerminal(result).replace(/[\r\u0080-\u009f\u202a-\u202e\u2066-\u2069]/g, "");
    }
    if (Array.isArray(value)) return value.map(clean);
    if (value !== null && typeof value === "object") {
      return Object.fromEntries(Object.entries(value).map(([key, child]) => [String(clean(key)), SECRET_KEY.test(key) ? "***" : clean(child)]));
    }
    return value;
  }
  return clean(params);
}

function line(value: unknown): string {
  const text = typeof value === "string" ? value : value === undefined ? "" : JSON.stringify(value);
  return sanitizeForTerminal(text).replace(/[\r\n\t\u0080-\u009f\u202a-\u202e\u2066-\u2069]/g, " ");
}

function summary(item: InboxItem): string {
  const params = object(item.params);
  switch (item.kind) {
    case "approval": {
      const paths = Array.isArray(params.changes)
        ? params.changes.map((change) => object(change).path).filter((path) => typeof path === "string")
        : Object.keys(object(params.changes));
      // 0.160.0 的文件审批请求只有 grantRoot/reason，不保证携带文件清单。
      const target = params.command ?? params.files ?? (paths.length ? paths : undefined)
        ?? params.file ?? params.path ?? params.grantRoot ?? params.permissions;
      if (target === undefined && item.method === "item/fileChange/requestApproval") {
        return `审批：文件修改（${line(params.reason ?? params.itemId ?? "未提供路径")}）`;
      }
      return `审批：${line(target ?? params.reason ?? "请求授权")}`;
    }
    case "question": {
      const questions = Array.isArray(params.questions) ? params.questions : [];
      return `提问：${questions.map((value) => {
        const question = object(value);
        const options = Array.isArray(question.options) ? question.options : [];
        return `${questionSummary(line(question.question))}${options.length ? `（选项：${options.map((option) => line(object(option).label)).join(" / ")}）` : ""}`;
      }).join("；") || "等待回答"}`;
    }
    case "mcp":
      return `MCP：${line(params.serverName ?? params.server ?? "未知服务器")} ${params.mode === "url" ? line(params.url) : `表单字段：${Object.keys(object(object(params.requestedSchema).properties)).map(line).join("、") || "（无）"}`}`;
    case "guardian": {
      try {
        const action = describeGuardianAction({ reviewId: String(params.reviewId ?? ""), threadId: item.threadId, receivedAt: "", notification: params });
        const translated = action.replace(/^command:/, "命令：").replace(/^execve:/, "程序：").replace(/^patch:/, "文件：")
          .replace(/^network:/, "网络：").replace(/^mcp tool:/, "MCP 工具：").replace(/^permissions request/, "权限请求");
        return `Guardian 拒绝：${line(translated)}`;
      } catch { return "Guardian 拒绝：动作详情不可用"; }
    }
  }
}

export const renderPending: RenderPending = (items, ctx) => {
  const pending = items.map((item) => ({ ...item, params: redactParams(item.params) }));
  const lines = pending.map((item) => {
    const params = object(item.params);
    const answer = item.kind === "question" ? '"你的回答"'
      : item.kind === "mcp" && params.mode !== "url" ? "-（从标准输入提供表单 JSON）" : "yes（接受）或 no（拒绝）";
    return `${line(item.id)} ${summary(item)}；回答：cxb reply ${line(item.id)} ${answer}${item.kind === "guardian" ? `；同意后用 cxb run --resume ${line(ctx.shortId)} \"继续\" 开启下一轮` : ""}`;
  });
  lines.push(`cxb wait ${line(ctx.shortId)}`);
  return { text: lines.join("\n"), json: { ok: false, status: "waiting", threadId: ctx.threadId, pending } };
};
