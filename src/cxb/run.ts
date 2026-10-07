import { randomUUID } from "node:crypto";
import { createReadStream, existsSync } from "node:fs";
import { join, resolve } from "node:path";
import { config, interpolateTemplate, loadTemplateWithMeta, resolveModel, validateEffort, type SandboxMode } from "../config";
import { builtinTemplatesDir } from "./config-cmd";
import { sandboxModeOf, sandboxPolicyFor } from "../config";
import { resolveModelDefaults } from "../models";
import { sanitizeForTerminal } from "../questions";
import { NO_RESPONSE, type AnyServerRequestHandler } from "../rpc";
import { askOverrides } from "./ask-instructions";
import { absorbedRefusal, buildContinuePrompt, continueRefusal, localContinueRefusal, resolveRunChain } from "./continue-prompt";
import { ASK_FEATURE_KEY, CxbError, EXIT, rpcFailure, type BrokerConnection, type BrokerRpc, type ExitCode,
  type ResumeOverrides, type RunResult, type ServerRequestDeps } from "./contracts";
import { readRunRecord } from "./ledger";
import { lifecyclePendingError } from "./lifecycle";
import { resolveThreadViaBroker } from "./thread-ref";
import type { CommandContext, CommandHandler } from "./main";
import type { Output } from "./output";
import { approvalPreflight, reportApprovalPreflight } from "./preflight";
import { bounded, exitCodeForRun, waitRun } from "./wait-run";

/** 不把上游消息直接输出，防止错误文本回显提示词或凭据。 */
export function rpcDetail(error: unknown): { code?: number; message: string } {
  let current = error;
  for (let i = 0; i < 5 && current && typeof current === "object"; i++) {
    const value = current as { rpcCode?: number; message?: string; detail?: string; cause?: unknown };
    if (typeof value.rpcCode === "number") return { code: value.rpcCode, message: value.detail ?? value.message ?? "" };
    current = value.cause;
  }
  return { message: "" };
}

/** 已按续跑规则定好退出码的错误（如 shuttingDown 的 BROKER_UNAVAILABLE），不再按线上错误码重新归类。 */
const decided = new WeakSet<CxbError>();
function decide(error: CxbError): CxbError { decided.add(error); return error; }

function executionError(error: unknown, id?: string): CxbError {
  if (error instanceof CxbError && (error.code !== "BROKER_UNAVAILABLE" || decided.has(error))) return error;
  const detail = rpcDetail(error);
  // 归档、删除进行中或待核实的线程被 broker 门禁挡住（N7-A 2.3.6 节），提示完成核实的命令而不是 wait。
  if (detail.code === -32001 && rpcFailure(error).reason === "lifecyclePending") return lifecyclePendingError(error, safe(id ?? "<线程ID>"));
  if (detail.code === -32001) return new CxbError("THREAD_HELD", `线程被占用，可用 cxb wait ${safe(id ?? "<线程ID>")} 观察`, { id });
  if (detail.code !== undefined) {
    // 0.160.0 恢复不存在的线程报「no rollout found for thread id …」（thread_processor.rs:5916）；broker 私有方法报 data.reason unknownThread。
    if (rpcFailure(error).reason === "unknownThread" || /no rollout found for thread/i.test(detail.message)) {
      return new CxbError("NOT_FOUND", "找不到指定线程，请检查 ID 与工作区", { id });
    }
    return new CxbError("CODEX_FAILED", "Codex 请求失败，请检查服务器配置与运行结果", { cause: error, id });
  }
  return error instanceof CxbError ? error : new CxbError("LOCAL_ERROR", "执行命令失败", { cause: error, id });
}

function safe(value: string): string { return sanitizeForTerminal(value).replace(/[\r\n\u2028\u2029]/g, " "); }

/** 读取 EOF 有期限；结束时移除监听并关闭自己打开的文件流。 */
export async function readPrompt(ctx: CommandContext): Promise<string> {
  const { options, positional, globals } = ctx.parsed;
  const stdin = positional.length === 1 && positional[0] === "-";
  if ((options.file && positional.length) || (positional.includes("-") && !stdin)) throw new CxbError("USAGE", "位置提示词、标准输入和 --file 必须三选一");
  let prompt = positional.join(" ");
  if (stdin || options.file) {
    const stream = stdin ? process.stdin : createReadStream(resolve(globals.dir, options.file!), { encoding: "utf8" });
    let onData!: (data: string | Buffer) => void;
    let onEnd!: () => void;
    let onError!: (error: unknown) => void;
    const controller = new AbortController();
    const unregister = ctx.registerCleanup(() => { controller.abort(); });
    try {
      const work = new Promise<string>((done, reject) => {
        let text = "";
        onData = (data) => { text += data; };
        onEnd = () => done(text);
        onError = reject;
        stream.setEncoding("utf8");
        stream.on("data", onData);
        stream.once("end", onEnd);
        stream.once("error", onError);
      });
      prompt = await bounded(work, globals.timeout > 0 ? globals.timeout * 1000 : 30_000, controller.signal);
    } catch (error) {
      if (error instanceof CxbError) throw error;
      throw new CxbError("LOCAL_ERROR", "无法读取提示词", { cause: error });
    } finally {
      unregister();
      stream.removeListener("data", onData);
      stream.removeListener("end", onEnd);
      stream.removeListener("error", onError);
      if (stdin) stream.pause(); else stream.destroy();
    }
  }
  if (!prompt.trim()) throw new CxbError("USAGE", "提示词不能为空或只有空白");
  return prompt;
}

export interface ExecutionPlan {
  command: "run" | "review";
  sandbox: SandboxMode;
  prompt?: string;
  review?: { mode: string; ref: string | null; base: string | null; target: object };
  /** `run --resume <id> --continue`：supplement 来自位置参数（拍板 J），fromRunId 来自 `--run`。 */
  continue?: { supplement: string | null; fromRunId?: string };
}

interface ContinueSource { from: RunResult; prompt: string; goal: unknown }

/**
 * 选定原运行并拼「丁＋」续跑提示（命令面规格 3.1 节、详细设计 3.6 节）：默认取 watch 快照里的最近一次运行，
 * `--run` 指定别的运行时读 broker 写的运行记录。只读，不发 thread/resume：能否开工由 broker 在开工那一刻核实。
 * goal 原文用 thread/goal/get 读（0.160.0 对未加载线程按会话记录从状态库读，thread_goal_processor.rs:305–332），
 * 读不到时退回运行记录里登记的 goal。
 */
async function prepareContinue(request: <T>(method: string, params?: unknown) => Promise<T>, out: Output,
  args: { workspaceDir: string; threadId: string; label: string; fromRunId?: string; supplement: string | null }): Promise<ContinueSource> {
  const { workspaceDir, threadId, label } = args;
  // 带 runId 时快照给的就是那次运行；broker 内存里已没有它时退回读运行记录。
  const snapshot = await request<{ run: RunResult | null }>("broker/threads/watch",
    { threadId, ...(args.fromRunId ? { runId: args.fromRunId } : {}) });
  let from = snapshot.run;
  if (args.fromRunId && from?.runId !== args.fromRunId) from = readRunRecord(workspaceDir, args.fromRunId);
  if (!from) {
    throw new CxbError("NOT_FOUND", args.fromRunId ? `找不到运行 ${safe(args.fromRunId)}，请用 cxb show ${safe(label)} 确认运行 ID。`
      : `线程 ${safe(label)} 没有可续跑的运行记录。`, { id: label });
  }
  if (from.threadId !== threadId) throw new CxbError("NOT_FOUND", `运行 ${safe(from.runId)} 不属于线程 ${safe(label)}。`, { id: label });
  const refusal = localContinueRefusal(from, label);
  if (refusal) throw refusal;
  const chain = resolveRunChain(from, (runId) => readRunRecord(workspaceDir, runId));
  if (chain.broken) out.notice(`更早的运行记录 ${safe(chain.missingRunId ?? "")} 已缺失，原始要求取自能找到的最早一次运行。`);
  let goal: unknown = null;
  try { goal = (await request<{ goal: unknown }>("thread/goal/get", { threadId })).goal ?? null; } catch (error) {
    if (rpcDetail(error).code === undefined) throw error;
    const objective = from.meta?.goal ?? chain.original.meta?.goal;
    goal = typeof objective === "string" ? { objective, status: "unknown" } : null;
    out.progress(objective ? "无法读取目标，改用运行记录里登记的目标" : "无法读取目标，续跑提示省略目标原文");
  }
  const objective = (goal as { objective?: unknown } | null)?.objective;
  const status = (goal as { status?: unknown } | null)?.status;
  const prompt = buildContinuePrompt({ from, chain, supplement: args.supplement,
    goal: typeof objective === "string" ? { objective, status: typeof status === "string" ? status : "unknown" } : null });
  return { from, prompt, goal };
}

/** run/review 的连接、生命周期、交接与结果共用，只有目标和输出形状不同。 */
export async function executeRun(ctx: CommandContext, deps: ServerRequestDeps, plan: ExecutionPlan): Promise<ExitCode> {
  const { options, globals } = ctx.parsed;
  const { out } = ctx;
  const policy = plan.command === "review" ? "never" : globals.approval;
  // 审批人必须随策略一起明确传：上游省略 approvalsReviewer 时沿用线程旧值、冷恢复也读回持久值
  // （0.160.0 step_settings.rs:291、thread_processor.rs:4245），否则用过 auto 的线程改选人工审批仍会走审核模型。
  const approval = policy === "auto" ? { approvalPolicy: "on-request", approvalsReviewer: "auto_review" }
    : { approvalPolicy: policy, approvalsReviewer: "user" };
  const controller = new AbortController();
  const waitController = new AbortController();
  const deadline = globals.timeout > 0 ? performance.now() + globals.timeout * 1000 : Infinity;
  const remaining = () => Number.isFinite(deadline) ? Math.max(0, deadline - performance.now()) : null;
  let connection: BrokerConnection | undefined;
  let threadId: string | undefined;
  let executionThread: string | undefined;
  let shortId = "";
  /** 提示里给用户看的线程标识：先用 --resume 解析出的短 ID，annotate 后换成 broker 的短 ID。 */
  let label = "";
  let continued: ContinueSource | undefined;
  let runId = "";
  let handedOver = false;
  let finished = false;
  let active = true;
  let stop: Promise<unknown> | undefined;
  let detachRouter: (() => void) | undefined;
  const disposers: Array<() => void> = [];
  const cleanup = ctx.registerCleanup(async (signal) => {
    if (signal && connection && threadId && runId && !handedOver && !finished) {
      // stop 的服务端确认窗口为 40 秒，客户端至少允许 45 秒。只停本次运行：broker 按 runId 原子核对，
      // 线程上已是别的运行时回 runMismatch，不停别人的运行。
      stop ??= bounded(connection.rpc.request("broker/threads/stop", { threadId: executionThread ?? threadId, runId }, { timeoutMs: 45_000 }), 45_000)
        .catch((error: unknown) => { if (rpcFailure(error).reason !== "runMismatch") throw error; });
    }
    controller.abort();
    waitController.abort();
    if (stop) await stop;
  });
  const request = async <T = unknown>(method: string, params?: unknown): Promise<T> => {
    if (controller.signal.aborted) throw new CxbError("INTERRUPTED", "命令已取消");
    // RPC 期限略大于剩余命令期限，确保全局超时稳定映射为三，而非先被 RPC 映射成六。
    const timeout = Math.max(1, Math.min(30_000, (remaining() ?? 30_000) + 1000));
    return bounded(connection!.rpc.request<T>(method, params, { timeoutMs: timeout }), remaining(), controller.signal);
  };
  try {
    connection = await ctx.connect();
    const rpc = connection.rpc;
    if (controller.signal.aborted) throw new CxbError("INTERRUPTED", "命令已取消");
    let overrides = askOverrides();
    // 版本握手已保证 broker 支持登记；失败说明参数、线程或连接有问题，照常上抛。
    const registerOverrides = (id: string, value: ResumeOverrides | null) =>
      request("broker/threads/resumeOverrides", { threadId: id, overrides: value });
    let warned = false;
    const warnAsk = () => {
      if (!warned) { warned = true; out.notice("提问工具不可用，将退回文字提问"); }
    };
    disposers.push(rpc.on("configWarning", (params) => {
      if (JSON.stringify(params)?.includes("default_mode_request_user_input")) warnAsk();
    }));
    let model = resolveModel(globals.model);
    let reasoning: string | undefined;
    try { reasoning = validateEffort(globals.reasoning); } catch (error) {
      throw new CxbError("USAGE", "推理强度无效", { cause: error });
    }
    try {
      const defaults = await resolveModelDefaults({ request }, { model, effort: reasoning });
      model ??= defaults?.model;
      reasoning ??= defaults?.effort;
    } catch (error) {
      if (controller.signal.aborted || (error instanceof CxbError && error.code === "TIMEOUT")) throw error;
      out.progress("无法读取模型列表，使用服务器默认模型");
    }
    if (reasoning) overrides.config!.model_reasoning_effort = reasoning;
    if (plan.command === "review" && model) overrides.config!.review_model = model;
    if (options.resume) {
      // 索引里没有的 ID 原样交给服务器判定（只在服务器上的线程）；不存在时上游报 no rollout found，退出 2。
      const ref = await resolveThreadViaBroker({ request }, options.resume, { passThrough: true });
      threadId = ref.threadId;
      label = ref.shortId ?? options.resume;
    }
    type ThreadResponse = { thread: { id: string }; model?: string; reasoningEffort?: string };
    let response: ThreadResponse | undefined;
    if (plan.continue) {
      // 续跑不发 thread/start、thread/resume：恢复订阅由 broker 在条件核实里做，并按登记补传覆盖（S1-评审第 28 节）。
      if (!threadId) throw new CxbError("USAGE", "--continue 必须与 --resume 同用。");
      continued = await prepareContinue(request, out, { workspaceDir: ctx.workspace.workspaceDir, threadId, label,
        fromRunId: plan.continue.fromRunId, supplement: plan.continue.supplement });
    } else {
      const method = threadId ? "thread/resume" : "thread/start";
      const params = { ...(threadId ? { threadId } : { ephemeral: false }), cwd: ctx.workspace.root,
        ...(model ? { model } : {}), sandbox: plan.sandbox, ...approval };
      try { response = await request<ThreadResponse>(method, { ...params, ...overrides }); } catch (error) {
        const detail = rpcDetail(error);
        if (detail.code !== -32600 || !detail.message.includes("default_mode_request_user_input")) throw error;
        const { [ASK_FEATURE_KEY]: _, ...rest } = overrides.config!;
        overrides = { ...overrides, config: rest };
        warnAsk();
        // 恢复已有线程时先清除旧登记，避免重试失败后 broker 继续补传已失效的键。
        if (threadId) await registerOverrides(threadId, null);
        response = await request<ThreadResponse>(method, { ...params, ...overrides });
      }
      if (method === "thread/resume" && plan.command === "review") {
        const actual = response as { sandbox?: unknown; approvalPolicy?: unknown };
        if (sandboxModeOf(actual.sandbox) !== "read-only" || actual.approvalPolicy !== "never") {
          throw new CxbError("LOCAL_ERROR", "该线程恢复后的权限不是只读（可能正被其他客户端使用，只读覆盖未生效），无法保证审查只读；"
            + "请等它空闲后再试，或不带 --resume 新开审查。", { id: options.resume });
        }
      }
      threadId = response.thread.id;
    }
    executionThread = threadId;
    // 只登记恢复需要的覆盖，不写任何 broker 状态文件。续跑时必须先于条件续跑登记（N2 审查第 5 条）：
    // 索引重建、旧线程或此前登记失败的线程，broker 内部恢复订阅时才会带上提问开关与指令。
    await registerOverrides(threadId, overrides);
    const inheritedGoal = continued?.from.meta?.goal;
    const annotation = await request<{ shortId: string }>("broker/threads/annotate", { threadId, meta: {
      approval: policy, ...(policy === "auto" ? { approvalsReviewer: "auto_review" } : {}), sandbox: plan.sandbox,
      ...(options.template ? { template: options.template } : {}), ...(options.goal ? { goal: options.goal } : {}),
      ...(typeof inheritedGoal === "string" ? { goal: inheritedGoal } : {}), source: plan.command,
    } });
    shortId = annotation.shortId;
    label = shortId;
    out.progress(continued ? `续跑线程：${safe(shortId)}，接运行 ${safe(continued.from.runId)}` : `thread started：${safe(shortId)}`);
    // 续跑不继承原运行的权限（用户 2026-10-06 定）：每次都显示本次实际生效的沙箱与审批。
    out.progress(`本次权限：沙箱 ${plan.sandbox}，审批 ${policy}`);
    if (!options.resume && !options.memory) await request("thread/memoryMode/set", { threadId, mode: "disabled" });
    if (options.goal) {
      try { await request("thread/goal/set", { threadId, objective: options.goal, ...(options.budget ? { tokenBudget: options.budget } : {}) }); } catch (error) {
        if (rpcDetail(error).code !== undefined) throw new CxbError("LOCAL_ERROR", "无法设置目标，请确认 Codex 已开启目标能力", { cause: error });
        throw error;
      }
    }
    let goal: unknown = continued ? continued.goal : (await request<{ goal: unknown }>("thread/goal/get", { threadId })).goal;
    disposers.push(rpc.on("thread/goal/updated", (raw) => {
      const event = raw as { threadId?: string; goal?: unknown };
      if (event.threadId === threadId) goal = event.goal ?? null;
    }));
    let human!: () => void;
    const needsHuman = new Promise<"human">((resolve) => { human = () => resolve("human"); });
    let reviewReady!: () => void;
    let reviewFailed!: (error: unknown) => void;
    const reviewResponse = new Promise<void>((resolve, reject) => { reviewReady = resolve; reviewFailed = reject; });
    void reviewResponse.catch(() => {});
    let routedHandler: AnyServerRequestHandler | undefined;
    const routerRpc: BrokerRpc = { ...rpc, onAnyRequest(handler) {
      routedHandler = handler;
      return () => { if (routedHandler === handler) routedHandler = undefined; };
    } };
    disposers.push(rpc.onAnyRequest(async (method, params, id) => {
      // broker 可能在 review/start 回包前回放子线程请求，待响应确认子线程归属后再路由。
      if (plan.command === "review" && (params as { threadId?: string })?.threadId !== threadId) {
        await bounded(reviewResponse, remaining(), controller.signal);
      }
      if (!active || handedOver || !routedHandler) return NO_RESPONSE;
      return routedHandler(method, params, id);
    }));
    const attach = (id: string) => {
      detachRouter?.();
      detachRouter = deps.attachServerRequests(routerRpc, { threadId: id, policy, onNeedsHuman(method) {
        // 仅记录方法名，不回显 params 或外部日志文本。
        out.progress(`需要回应的请求类型：${safe(method)}`);
        human();
      }, log() { out.progress("收到服务端请求"); } });
    };
    attach(threadId);
    let started!: (id: string) => void;
    let startFailed!: (error: unknown) => void;
    const startedPromise = new Promise<string>((resolve, reject) => { started = resolve; startFailed = reject; });
    disposers.push(rpc.on("broker/run/started", (raw) => {
      const event = raw as { threadId?: string; runId?: string; shortId?: string };
      if (event.threadId !== threadId || !event.runId) return;
      runId = event.runId;
      shortId = event.shortId ?? shortId;
      started(runId);
    }));
    // 本次运行一结束（认领释放）就不再由信号清理发 stop：之后线程上可能已是别的连接的运行。
    disposers.push(rpc.on("broker/run/ended", (raw) => {
      if (runId && (raw as { run?: { runId?: string } })?.run?.runId === runId) finished = true;
    }));
    // 共享服务器上的归属定案（详细设计 2.3 节 broker/run/settled）：只认本次运行的那一条，可能早于 turn/start 回包。
    const settlements = new Map<string, string | null>();
    const settlementWaiters = new Set<() => void>();
    disposers.push(rpc.on("broker/run/settled", (raw) => {
      const event = raw as { threadId?: string; runId?: string; absorbedBy?: unknown };
      if (event.threadId !== threadId || typeof event.runId !== "string") return;
      // 只有字符串或 null 才是定案；其他取值不算，继续等。
      if (typeof event.absorbedBy !== "string" && event.absorbedBy !== null) return;
      settlements.set(event.runId, event.absorbedBy);
      for (const waiter of [...settlementWaiters]) waiter();
    }));
    disposers.push(() => settlementWaiters.clear());
    const settlementOf = (id: string) => new Promise<string | null>((resolve) => {
      const check = () => {
        if (!settlements.has(id)) return;
        settlementWaiters.delete(check);
        resolve(settlements.get(id)!);
      };
      settlementWaiters.add(check);
      check();
    });
    disposers.push(rpc.onClose(() => startFailed(new CxbError("BROKER_UNAVAILABLE", "broker 连接断开"))));
    // 等定案时 startedPromise、startReply 都已兑现，断线必须另有一路把等待及时结束（退出 6，而不是拖到全局期限）。
    let disconnected!: (error: unknown) => void;
    const connectionLost = new Promise<never>((_, reject) => { disconnected = reject; });
    void connectionLost.catch(() => {});
    disposers.push(rpc.onClose(() => disconnected(new CxbError("BROKER_UNAVAILABLE", "等待续跑定案时 broker 连接断开", { retryable: true }))));
    const startMethod = plan.command === "review" ? "review/start" : "turn/start";
    const startParams = plan.command === "review"
      ? { threadId, target: plan.review!.target, delivery: "detached" }
      : { threadId, input: [{ type: "text", text: continued ? continued.prompt : plan.prompt!, text_elements: [] }], clientUserMessageId: randomUUID(),
        sandboxPolicy: sandboxPolicyFor(plan.sandbox), ...(model ? { model } : {}), ...(reasoning ? { effort: reasoning } : {}), ...approval,
        // 条件请求：broker 在开工那一刻核实原运行仍可续，不满足就不开工（详细设计 2.3 节「续跑的条件请求」）。
        ...(continued ? { cxbContinue: { fromRunId: continued.from.runId } } : {}) };
    type StartReply = { runId?: string; shortId?: string; reviewThreadId?: string; absorbedBy?: unknown };
    const startReply = request<StartReply>(startMethod, startParams).catch((error: unknown) => {
      const refusal = continued ? continueRefusal(error, label) : null;
      throw refusal ? decide(refusal) : error;
    }).then((reply) => {
      if (!active) return reply;
      if (reply.reviewThreadId) { executionThread = reply.reviewThreadId; attach(executionThread); }
      reviewReady();
      if (!runId && reply.runId) { runId = reply.runId; shortId = reply.shortId ?? shortId; started(runId); }
      if (!runId) throw new CxbError("BROKER_UNAVAILABLE", "broker 未返回运行标识");
      return reply;
    });
    // 审批可能卡住启动回包：通知已经给出 runId 时必须能立即交接。
    void startReply.catch((error) => { startFailed(error); reviewFailed(error); });
    await bounded(startedPromise, remaining(), controller.signal);
    out.progress(`turn started：${safe(runId)}`);
    if (continued && connection.handshake.server.kind === "shared") {
      // 共享服务器上新一轮可能被别的客户端的 turn 吸收：回包带 absorbedBy 就以它为准，没带就等 broker/run/settled，
      // 收到之前不宣布续跑成功（命令面规格 3.1 节、S1-评审第 17 节）。私有服务器没有别的客户端，不定案也不发这条通知。
      const started = runId;
      const absorbedBy = await bounded(Promise.race([settlementOf(started), connectionLost,
        startReply.then((reply) => typeof reply.absorbedBy === "string" || reply.absorbedBy === null ? reply.absorbedBy : settlementOf(started))]),
      remaining(), controller.signal);
      if (absorbedBy !== null) throw absorbedRefusal(absorbedBy, label);
    }
    const handover = async () => {
      const result = await bounded(deps.handover(rpc, { threadId: threadId!, runId, policy,
        ...(policy === "auto" ? { approvalsReviewer: "auto_review" } : {}) },
      { signal: controller.signal, timeoutMs: Math.min(30_000, remaining() ?? 30_000) }), remaining(), controller.signal);
      if (result.outcome !== "runEnded") {
        handedOver = true;
        const health = await request<{ survivesParentJob: boolean }>("broker/health", {});
        if (!health.survivesParentJob) out.notice("当前环境下 broker 可能随本命令所在的进程组一起退出，托管不保证可靠");
      }
      return result;
    };
    let result: RunResult;
    if (options.detach) {
      const transferred = await handover();
      if (transferred.outcome !== "runEnded") {
        out.result(`${threadId} ${shortId} ${runId} 运行中\n`);
        out.resultJson({ threadId, shortId, runId, status: "running" });
        return EXIT.OK;
      }
      // 交接恰逢运行结束：已就绪的结果直接用，否则按共用等待补全，再照常输出与退出。
      const known = transferred.result;
      result = known && known.runId === runId && known.turns.at(-1)?.ready !== "pending" ? known
        : await bounded(waitRun(rpc, { threadId, runId, timeoutMs: remaining(), signal: waitController.signal, workspaceDir: ctx.workspace.workspaceDir }),
          remaining(), controller.signal);
    } else {
      const waiting = waitRun(rpc, { threadId, runId, timeoutMs: remaining(), signal: waitController.signal, workspaceDir: ctx.workspace.workspaceDir });
      void waiting.catch(() => {});
      const abortWait = () => waitController.abort();
      controller.signal.addEventListener("abort", abortWait, { once: true });
      disposers.push(() => controller.signal.removeEventListener("abort", abortWait));
      // 启动 RPC 失败不能被已收到 started 的 Promise 吞掉。
      const failedStart = startReply.then(() => new Promise<never>(() => {}));
      const first = await bounded(Promise.race([waiting, needsHuman, failedStart]), remaining(), controller.signal);
      if (first === "human") {
        const transferred = await handover();
        if (transferred.outcome !== "runEnded") {
          waitController.abort();
          const rendered = deps.renderPending(transferred.pending, { threadId, shortId });
          out.result(rendered.text);
          if (rendered.json && typeof rendered.json === "object") out.resultJson(rendered.json);
          return EXIT.WAITING_RESPONSE;
        }
        result = await bounded(waiting, remaining(), controller.signal);
      } else result = first;
    }
    finished = true;
    const code = exitCodeForRun(result);
    const last = result.turns.at(-1);
    if (result.loopGuard) out.notice(`自动续跑刹车：${result.loopGuard.kind === "noAction" ? "连续无动作" : "轮数上限"}，${result.loopGuard.count} 轮`);
    else if (result.interruptReason === "goalLoop") out.notice(`自动续跑刹车：goalLoop，${result.turnsTotal} 轮`);
    if (code === EXIT.CODEX_FAILED) {
      const detail = JSON.stringify(last?.error ?? "");
      if (policy === "never" && /mcp/i.test(detail)) {
        const report = await approvalPreflight(policy, process.env,
          { signal: controller.signal, ...(Number.isFinite(deadline) ? { deadline } : {}) });
        // 只提取受限的服务器标识；绝不把上游整段错误（可能含凭据）写入 stderr。
        const error = last?.error as { message?: unknown; serverName?: unknown } | null;
        const message = typeof error?.message === "string" ? error.message : "";
        const reported = typeof error?.serverName === "string" ? error.serverName
          : /MCP\s+(?:server|服务器)\s+[`'"]?([\p{L}\p{N}_.-]{1,128})/iu.exec(message)?.[1];
        const names = reported && /^[\p{L}\p{N}_.-]{1,128}$/u.test(reported) ? [reported]
          : report.servers.filter((server) => server.risk && detail.includes(server.name)).map((server) => safe(server.name));
        out.notice(`Codex 的 MCP 请求失败${names.length ? `（服务器：${names.join("、")}）` : ""}；需要搜索/MCP 时请加 --approval auto。`);
      } else out.notice("Codex 执行失败，请用 cxb show 查看运行结果");
    }
    out.progress(`运行结束：${result.status ?? result.state}，耗时 ${Math.max(0, (result.endedAt ?? Date.now()) - result.startedAt)} 毫秒`);
    out.result(last?.output ?? "");
    const common = { ok: code === EXIT.OK, command: plan.command, thread: { id: result.threadId, shortId: result.shortId },
      runId: result.runId, status: result.status ?? result.state, result: last?.output ?? "",
      ...(result.loopGuard ? { loopGuard: result.loopGuard } : {}) };
    if (plan.command === "review") out.resultJson({ ...common, mode: plan.review!.mode, ref: plan.review!.ref, base: plan.review!.base });
    else out.resultJson({ ...common, model: response?.model ?? model ?? null, reasoning: response?.reasoningEffort ?? reasoning ?? null,
      sandbox: plan.sandbox, approval: policy, startedAt: new Date(result.startedAt).toISOString(),
      completedAt: result.endedAt === null ? null : new Date(result.endedAt).toISOString(), joined: result.joined, goal,
      ...(continued ? { continuedFrom: continued.from.runId } : {}) });
    return code;
  } catch (error) {
    throw executionError(error, shortId || threadId);
  } finally {
    active = false;
    controller.abort();
    waitController.abort();
    detachRouter?.();
    for (const dispose of disposers) dispose();
    try { if (stop) await stop; } finally {
      cleanup();
      await connection?.close();
    }
  }
}

export function createRunHandler(deps: ServerRequestDeps): CommandHandler {
  return async (ctx) => {
    const { options, positional, globals } = ctx.parsed;
    if (options.continue) {
      // 解析层已拒绝标准输入、文件、--goal、--template；位置参数是本次补充，不替换原始要求。
      const supplement = positional.join(" ").trim() ? positional.join(" ") : null;
      reportApprovalPreflight(await cancellablePreflight(ctx), ctx.out);
      return executeRun(ctx, deps, { command: "run", sandbox: globals.sandbox,
        continue: { supplement, ...(options.run ? { fromRunId: options.run } : {}) } });
    }
    let prompt = await readPrompt(ctx);
    let sandbox = ctx.parsed.globals.sandbox;
    const name = ctx.parsed.options.template;
    if (name) {
      try {
        // 优先使用 N1 迁入的新目录；内置模板仍复用既有解析器。
        const userDir = join(ctx.workspace.dataRoot, "templates");
        const builtinDir = builtinTemplatesDir();
        const { body, meta } = loadTemplateWithMeta(name, existsSync(join(userDir, `${name}.md`)) ? userDir : builtinDir);
        prompt = interpolateTemplate(body, { PROMPT: prompt });
        if (meta.sandbox && !ctx.parsed.explicit.has("sandbox")) {
          if (!(config.sandboxModes as readonly string[]).includes(meta.sandbox)) throw new CxbError("LOCAL_ERROR", "模板的沙箱建议无效");
          sandbox = meta.sandbox as SandboxMode;
        }
      } catch (error) {
        throw new CxbError("LOCAL_ERROR", "无法加载模板或模板配置无效", { cause: error });
      }
    }
    if (!prompt.trim()) throw new CxbError("USAGE", "模板生成的提示词不能为空");
    reportApprovalPreflight(await cancellablePreflight(ctx), ctx.out);
    return executeRun(ctx, deps, { command: "run", sandbox, prompt });
  };
}

/** 开跑前的审批预检：Ctrl-C 取消（INTERRUPTED），读配置的期限不超过本命令的 --timeout。 */
async function cancellablePreflight(ctx: CommandContext) {
  const { globals } = ctx.parsed;
  const controller = new AbortController();
  const unregister = ctx.registerCleanup(() => { controller.abort(); });
  try {
    return await approvalPreflight(globals.approval, process.env,
      { signal: controller.signal, ...(globals.timeout > 0 ? { deadline: performance.now() + globals.timeout * 1000 } : {}) });
  } finally { unregister(); }
}
