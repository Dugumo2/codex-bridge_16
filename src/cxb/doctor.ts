import { closeSync, fsyncSync, mkdirSync, openSync, readFileSync, renameSync, unlinkSync, writeFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { config } from "../config";
import type { AccountRead } from "../types";
import { BrokerNotRunning, connectBroker, resolveBrokerBin, safeSummary } from "./broker-conn";
import { CXB_PROTOCOL_VERSION, CxbError, EXIT, type BrokerConnection } from "./contracts";
import type { CommandHandler } from "./main";
import { approvalPreflight } from "./preflight";
import { bounded } from "./wait-run";

type Status = "pass" | "warn" | "fail";
export interface DoctorCheck { name: string; status: Status; detail: string; [key: string]: unknown }
export interface DiagnosticLimit { timeoutMs: number; signal: AbortSignal }

/** 输出尚未认证的唯一证据是明确需要认证且没有账号；未知情况宁可放行。 */
export function describeAuth(read: AccountRead | "unknown"): { ready: boolean; detail: string } {
  if (read === "unknown") return { ready: true, detail: "未能确认账号（服务器未报告）" };
  const account = read.account;
  if (account?.type === "chatgpt") return { ready: true, detail: `ChatGPT 已登录（${[account.planType, account.email].filter(Boolean).join("，")}）` };
  if (account?.type === "apiKey") return { ready: true, detail: "已配置 API key（本次未验证有效性）" };
  if (read.requiresOpenaiAuth === false) return { ready: true, detail: "提供方不要求 OpenAI 认证" };
  if (account?.type) return { ready: true, detail: `未知账号类型 ${account.type}，暂按可用处理` };
  if (read.requiresOpenaiAuth === true) return { ready: false, detail: "明确未登录" };
  return { ready: true, detail: "账号与认证要求未报告，暂按可用处理" };
}

/** 所有外部文本在最终输出前统一脱敏；结构中的敏感字段也不展示原值。 */
export function safeDiagnostic(value: unknown): unknown {
  if (typeof value === "string") return safeSummary(value);
  if (Array.isArray(value)) return value.map(safeDiagnostic);
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value).map(([key, entry]) =>
    [safeSummary(key), /token|password|secret|api[_ -]?key|cookie|authorization/i.test(key) ? "[已隐藏]" : safeDiagnostic(entry)]));
  return value;
}

export function findExecutable(name: string): string | null { return Bun.which(name); }

/** diag-exec 期限到后终止 Job、等进程退出（最多 2 秒）并写状态文件的预留，再加 1 秒余量。 */
const DIAG_RESERVE_MS = 3000;
/** 结束进程后确认退出与读取落定的等待上限。 */
const FINAL_WAIT_MS = 1000;

interface DiagStatus { outcome: "exited" | "timeout" | "failed"; exitCode?: number; reason?: string }

/** 读 diag-exec 的状态文件；缺失或损坏返回 null（调用方按 diag-exec 异常处理）。 */
function readDiagStatus(path: string): DiagStatus | null {
  try {
    const value = JSON.parse(readFileSync(path, "utf8")) as DiagStatus;
    // Windows 退出码是 u32：越界的值说明状态文件损坏，按 diag-exec 异常处理，不能当成诊断程序的结果。
    if (value?.outcome === "exited" && Number.isSafeInteger(value.exitCode) && value.exitCode! >= 0 && value.exitCode! <= 0xffff_ffff) return value;
    if (value?.outcome === "timeout") return value;
    if (value?.outcome === "failed" && typeof value.reason === "string") return value;
    return null;
  } catch { return null; }
}

/**
 * 运行诊断程序（如 `codex --version`）。给了 `broker` 时经 `cxb-broker diag-exec` 运行（任务说明 N7-A 2.2 节）：
 * 诊断程序在创建时即属于关闭即结束的 Job，正常退出或期限到时 diag-exec 终止整个 Job，
 * 诊断程序先退出、后代仍占着输出管道的情况也随之回收；结果以状态文件为准（退出码 124、125 可能与诊断程序自己的退出码重叠）。
 * 不给 `broker` 时直接运行（只用于不会派生子进程的 `cxb-broker --version`）。
 * 超出期限或取消时结束本端启动的进程（diag-exec 的 Job 句柄随之关闭，整棵树由系统结束）并取消读取；
 * 等待、结束、收尾共用一个绝对截止时间，整体不超过 `limit.timeoutMs`。
 */
export async function runDiagnosticProcess(bin: string, args: string[], limit: DiagnosticLimit, broker?: string): Promise<string> {
  if (limit.signal.aborted) throw new CxbError("INTERRUPTED", "诊断已取消");
  // 尾部预算不足时不再创建无法保证按期回收的新进程。
  if (limit.timeoutMs <= DIAG_RESERVE_MS + FINAL_WAIT_MS) throw new CxbError("TIMEOUT", "剩余期限不足以执行并回收诊断程序");
  const deadline = performance.now() + limit.timeoutMs;
  const left = () => Math.max(0, deadline - performance.now());
  const statusFile = broker ? join(tmpdir(), `cxb-diag-${randomUUID()}.json`) : null;
  const argv = broker
    ? [broker, "diag-exec", "--timeout-ms", String(Math.floor(limit.timeoutMs - DIAG_RESERVE_MS - FINAL_WAIT_MS)), "--status-file", statusFile!, "--", bin, ...args]
    : [bin, ...args];
  let child: ReturnType<typeof Bun.spawn>;
  try { child = Bun.spawn(argv, { stdin: "ignore", stdout: "pipe", stderr: "pipe", windowsHide: true }); }
  catch (cause) { throw new CxbError("LOCAL_ERROR", "无法执行诊断程序", { cause }); }
  const stop = () => { try { child.kill("SIGKILL"); } catch { /* 可能已经退出，仍等 exited 确认。 */ } };
  const abort = () => stop();
  const readers: Array<ReadableStreamDefaultReader<Uint8Array>> = [];
  /** 每路读取是否仍未结束；一路先失败时另一路可能还挂着，要分别记录。 */
  const open = [true, true];
  const collect = async (stream: ReadableStream<Uint8Array>, index: number): Promise<string> => {
    const reader = stream.getReader();
    readers[index] = reader;
    const chunks: Uint8Array[] = [];
    let size = 0;
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        size += value.byteLength;
        if (size > 64 * 1024) throw new CxbError("LOCAL_ERROR", "诊断程序输出超过上限");
        chunks.push(value);
      }
      return Buffer.concat(chunks).toString("utf8");
    } finally { open[index] = false; reader.releaseLock(); }
  };
  limit.signal.addEventListener("abort", abort, { once: true });
  const outputs = [collect(child.stdout as ReadableStream<Uint8Array>, 0), collect(child.stderr as ReadableStream<Uint8Array>, 1)];
  const io = Promise.all([child.exited, ...outputs]);
  try {
    const [code, stdout, stderr] = await bounded(io, left() - FINAL_WAIT_MS, limit.signal);
    if (!statusFile) {
      if (code !== 0) throw new CxbError("LOCAL_ERROR", `诊断程序退出 ${code}：${safeSummary(stderr)}`);
      return stdout;
    }
    const status = readDiagStatus(statusFile);
    if (!status) throw new CxbError("LOCAL_ERROR", `diag-exec 异常（退出 ${code}，没有有效的状态文件）：${safeSummary(stderr)}`);
    if (status.outcome === "timeout") throw new CxbError("TIMEOUT", "诊断程序超过期限，已结束其进程树");
    if (status.outcome === "failed") throw new CxbError("LOCAL_ERROR", `diag-exec 失败：${safeSummary(status.reason!)}`);
    if (status.exitCode !== 0) throw new CxbError("LOCAL_ERROR", `诊断程序退出 ${status.exitCode}：${safeSummary(stderr)}`);
    return stdout;
  } finally {
    limit.signal.removeEventListener("abort", abort);
    stop();
    // 无论哪一路先失败，未结束的读取都要取消。
    readers.forEach((reader, index) => { if (open[index]) void reader.cancel().catch(() => undefined); });
    // 收尾只在剩余期限内尽力等待，超时不覆盖原来的诊断错误。
    try { await bounded(Promise.allSettled([child.exited, ...outputs]), Math.max(1, Math.min(FINAL_WAIT_MS, left()))); }
    catch { /* 已按期限放弃等待。 */ }
    if (statusFile) { try { unlinkSync(statusFile); } catch { /* 未写出或已删除。 */ } }
  }
}

export function probeWritable(directory: string): void {
  const source = join(directory, `.cxb-doctor-${randomUUID()}.tmp`);
  const target = `${source}.probe`;
  let fd: number | undefined;
  try {
    mkdirSync(directory, { recursive: true });
    fd = openSync(source, "wx");
    writeFileSync(fd, "cxb 诊断探测\n");
    fsyncSync(fd);
    closeSync(fd); fd = undefined;
    renameSync(source, target);
    unlinkSync(target);
  } catch (cause) { throw new CxbError("LOCAL_ERROR", `目录不可原子写入：${directory}`, { cause }); }
  finally {
    if (fd !== undefined) closeSync(fd);
    for (const path of [source, target]) {
      try { unlinkSync(path); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw new CxbError("LOCAL_ERROR", "无法清除诊断探测文件", { cause: error }); }
    }
  }
}

export interface DoctorDeps {
  findExecutable: typeof findExecutable;
  runProcess: typeof runDiagnosticProcess;
  resolveBrokerBin: typeof resolveBrokerBin;
  connectBroker: typeof connectBroker;
  preflight: (policy: Parameters<typeof approvalPreflight>[0], env: NodeJS.ProcessEnv, limit: DiagnosticLimit) => ReturnType<typeof approvalPreflight>;
  probeWritable: typeof probeWritable;
  now: () => number;
  totalMs: number;
  itemMs: number;
}

export function createDoctorHandler(overrides: Partial<DoctorDeps> = {}): CommandHandler {
  const deps: DoctorDeps = { findExecutable, runProcess: runDiagnosticProcess, resolveBrokerBin, connectBroker,
    preflight: (policy, env, limit) => approvalPreflight(policy, env, { deadline: performance.now() + limit.timeoutMs, signal: limit.signal }), probeWritable, now: () => performance.now(), totalMs: 60_000, itemMs: 10_000, ...overrides };
  return async (ctx) => {
    const controller = new AbortController();
    const unregister = ctx.registerCleanup(() => controller.abort());
    const deadline = deps.now() + deps.totalMs;
    const checks: DoctorCheck[] = [];
    let connection: BrokerConnection | undefined;
    let closing: Promise<void> | undefined;
    const close = () => connection ? (closing ??= connection.close()) : Promise.resolve();
    let codex: string | null = null;
    /** 运行 codex 诊断用的 broker 可执行文件（diag-exec）。 */
    let diagBroker: string | undefined;
    let brokerBin: string | undefined;
    let brokerVersion: string | undefined;
    let brokerUsable = false;
    let exhausted = false;
    const processes = new Set<Promise<string>>();
    const runProcess: DoctorDeps["runProcess"] = (bin, args, limit, broker) => {
      const work = deps.runProcess(bin, args, limit, broker);
      processes.add(work);
      void work.finally(() => processes.delete(work)).catch(() => undefined);
      return work;
    };
    const markBudget = () => {
      if (exhausted) return;
      exhausted = true;
      checks.push({ name: "doctorBudget", status: "fail", detail: "诊断总期限用尽" });
    };
    const check = async (name: string, failure: Status, work: (limit: DiagnosticLimit) => Promise<Omit<DoctorCheck, "name">>, cap = deps.itemMs) => {
      if (controller.signal.aborted) throw new CxbError("INTERRUPTED", "诊断已取消");
      const remaining = deadline - deps.now();
      if (remaining <= 0 || exhausted) {
        markBudget(); checks.push({ name, status: "warn", detail: "未检查（总期限用尽）" }); return;
      }
      const item = new AbortController();
      const cancel = () => item.abort();
      controller.signal.addEventListener("abort", cancel, { once: true });
      const timeoutMs = Math.max(1, Math.min(cap, remaining));
      const timer = setTimeout(cancel, timeoutMs);
      try {
        const result = await bounded(work({ timeoutMs, signal: item.signal }), timeoutMs, item.signal);
        checks.push({ ...result, name } as DoctorCheck);
      } catch (error) {
        if (controller.signal.aborted) throw new CxbError("INTERRUPTED", "诊断已取消");
        checks.push({ name, status: failure, detail: `${name === "account" ? "未能确认" : "检查失败或超过期限"}：${error instanceof Error ? error.message : "未知错误"}` });
      } finally {
        clearTimeout(timer); item.abort(); controller.signal.removeEventListener("abort", cancel);
      }
      if (deps.now() >= deadline) markBudget();
    };
    const unchecked = () => ({ status: "warn" as const, detail: "未检查（broker 未运行或不可用）" });
    const request = async <T>(method: string, params: unknown, limit: DiagnosticLimit): Promise<T> => {
      // 诊断连接没有写操作，取消时直接关闭，终止底层 RPC 的 pending，不能只丢弃外层 Promise。
      const abort = () => { brokerUsable = false; void close(); };
      limit.signal.addEventListener("abort", abort, { once: true });
      try {
        if (limit.signal.aborted) { abort(); throw new CxbError("INTERRUPTED", "诊断请求已取消"); }
        return await bounded(connection!.rpc.request<T>(method, params, { timeoutMs: Math.max(1, Math.min(limit.timeoutMs, deadline - deps.now())) }), limit.timeoutMs, limit.signal);
      } finally { limit.signal.removeEventListener("abort", abort); }
    };
    try {
      await check("cxb", "fail", async () => ({ status: "pass", detail: `cxb ${config.clientVersion}，Bun ${Bun.version}` }));
      await check("codex", "fail", async (limit) => {
        codex = deps.findExecutable("codex");
        if (!codex) throw new CxbError("LOCAL_ERROR", "PATH 中找不到 codex");
        // codex 经 broker 的 diag-exec 在 Job 里运行，broker 找不到时不直接运行（无法保证回收进程树）。
        diagBroker = deps.resolveBrokerBin();
        const version = await runProcess(codex, ["--version"], limit, diagBroker);
        if (!version.trim()) throw new CxbError("LOCAL_ERROR", "codex 未报告版本");
        return { status: "pass", detail: `${codex}；${version}`, path: codex, version };
      });
      await check("config", "fail", async () => {
        if (ctx.userConfigError) throw ctx.userConfigError;
        if (!ctx.userConfig) throw new CxbError("LOCAL_ERROR", "入口未提供配置快照");
        const { values, unknownKeys, path } = ctx.userConfig;
        const idleTimeout = values["broker-idle-timeout"] ?? 300;
        const source = Object.hasOwn(values, "broker-idle-timeout") ? "config" : "default";
        return { status: unknownKeys.length ? "warn" : "pass", detail: `空闲期限 ${idleTimeout} 秒（${source === "config" ? "配置，broker 重启后生效" : "缺省"}）；${unknownKeys.length ? `未知键：${unknownKeys.join("、")}` : "配置正常"}；${path}`, idleTimeout, source, unknownKeys, path };
      });
      await check("workspace", "fail", async () => {
        for (const directory of new Set([ctx.workspace.root, ctx.workspace.dataRoot, ctx.workspace.workspaceDir])) deps.probeWritable(directory);
        return { status: "pass", detail: `目录可原子写入；工作区 ${ctx.workspace.root}；键 ${ctx.workspace.key}；管道 ${ctx.workspace.pipePath}`, root: ctx.workspace.root, key: ctx.workspace.key, pipePath: ctx.workspace.pipePath, dataRoot: ctx.workspace.dataRoot };
      });
      await check("brokerBinary", "fail", async (limit) => {
        brokerBin = deps.resolveBrokerBin();
        const output = await runProcess(brokerBin, ["--version"], limit);
        const match = /^cxb-broker (\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?)$/.exec(output.trim());
        if (!match) throw new CxbError("LOCAL_ERROR", "无法读取 broker 版本");
        brokerVersion = match[1];
        return { status: "pass", detail: `${brokerBin}；版本 ${brokerVersion}`, path: brokerBin, version: brokerVersion };
      });
      await check("broker", "fail", async (limit) => {
        const began = deps.now();
        if (!brokerBin || !brokerVersion) throw new CxbError("LOCAL_ERROR", "broker 二进制不可用，未连接");
        try {
          const connecting = deps.connectBroker({ workspace: ctx.workspace, brokerBin, expectedBrokerVersion: brokerVersion, probe: true,
            timeoutMs: limit.timeoutMs, signal: limit.signal });
          // 迟到的连接由取得方关闭；检查退出不能丢掉句柄。
          const conn = await connecting.then(async (value) => {
            connection = value;
            if (limit.signal.aborted) { await close(); throw new CxbError("INTERRUPTED", "探测连接已取消"); }
            return value;
          });
          const health = await request<Record<string, unknown>>("broker/health", {}, { ...limit, timeoutMs: Math.max(1, Math.min(deps.itemMs, limit.timeoutMs - (deps.now() - began))) });
          const handshake = conn.handshake;
          const compatible = handshake?.brokerVersion === brokerVersion && handshake?.protocolVersion === CXB_PROTOCOL_VERSION && Number.isSafeInteger(handshake?.epoch)
            && health.brokerVersion === brokerVersion && health.protocolVersion === CXB_PROTOCOL_VERSION;
          brokerUsable = compatible;
          return { status: !compatible ? "fail" : health.storage === "degraded" ? "warn" : "pass",
            detail: !compatible ? "版本或协议不符；下一个普通命令会在 broker 空闲时自动替换" : `运行中实例的空闲期限未知；storage=${health.storage}，epoch=${health.epoch}，uptimeMs=${health.uptimeMs}，claims=${health.claims}，pending=${health.pending}；本次未拉起实例`,
            launched: false, storage: health.storage, epoch: health.epoch, uptimeMs: health.uptimeMs, claims: health.claims, pending: health.pending,
            survivesParentJob: health.survivesParentJob, parentJob: health.parentJob };
        } catch (error) {
          if (error instanceof BrokerNotRunning) return { status: "pass", detail: "未运行（空闲退出属正常，下个命令会自动拉起）", launched: false };
          throw error;
        }
      }, 30_000);
      await check("appServer", "warn", async () => {
        if (!brokerUsable) return unchecked();
        const { server, userAgent } = connection!.handshake;
        return { status: "pass", detail: `只验证了当前接入路径，其他路径未探测；${server.kind === "shared" ? "本机 features 结果不能代表共享服务器；" : ""}${JSON.stringify(safeDiagnostic(server))}；握手标识 ${userAgent}（broker 自报，不是 app-server 版本）`, server, userAgent };
      });
      await check("account", "warn", async (limit) => {
        if (!brokerUsable) return unchecked();
        const result = describeAuth(await request<AccountRead>("account/read", { refreshToken: false }, limit));
        return { status: result.ready ? "pass" : "warn", detail: result.detail };
      });
      await check("models", "warn", async (limit) => {
        if (!brokerUsable) return unchecked();
        const models: Array<Record<string, unknown>> = [];
        const seen = new Set<string>();
        let cursor: string | undefined;
        const end = deps.now() + limit.timeoutMs;
        for (let page = 0; page < 20; page++) {
          if (limit.signal.aborted) throw new CxbError("TIMEOUT", "模型检查超过期限");
          const result = await request<{ data: Array<Record<string, unknown>>; nextCursor?: string | null }>("model/list", { ...(cursor ? { cursor } : {}), limit: 100 }, { ...limit, timeoutMs: Math.max(1, end - deps.now()) });
          if (!Array.isArray(result.data)) throw new CxbError("LOCAL_ERROR", "模型分页数据异常");
          models.push(...result.data);
          if (!result.nextCursor) {
            const defaults = models.filter((model) => model.isDefault).map((model) => model.model ?? model.id);
            return { status: "pass", detail: `共 ${models.length} 个模型；默认模型：${defaults.join("、") || "未报告"}`,
              count: models.length, defaults, ...(ctx.parsed.globals.verbose ? { models: models.map((m) => ({ id: m.id, model: m.model, isDefault: m.isDefault, supportedReasoningEfforts: m.supportedReasoningEfforts })) } : {}) };
          }
          if (typeof result.nextCursor !== "string" || seen.has(result.nextCursor)) throw new CxbError("LOCAL_ERROR", "模型分页游标没有前进");
          cursor = result.nextCursor; seen.add(cursor);
        }
        throw new CxbError("LOCAL_ERROR", "模型分页超过 20 页，未能完整检查");
      });
      await check("mcpApprovalRisk", "warn", async (limit) => {
        const result = await deps.preflight(ctx.parsed.globals.approval, process.env, limit);
        // 按线程关闭的 MCP 来自持久配置（mcp-select.ts）；只展示名单，是否生效取决于 Codex 配置里有没有该服务。
        const stored = ctx.userConfig?.values ?? {};
        const mcpDisabled = typeof stored["mcp-disabled"] === "string" ? stored["mcp-disabled"].split(",") : [];
        const mcpWhenNever = stored["mcp-when-never"] === "off" ? "off" : "keep";
        const closing = `cxb 开线程关闭 MCP：${mcpDisabled.length ? mcpDisabled.join("、") : "无"}${mcpWhenNever === "off" ? "；审批 never 时全关" : ""}`;
        return { status: result.status === "ok" ? "pass" : "warn", detail: `${result.status === "unavailable" ? "未能检查 MCP 审批风险" : `有效审批 ${ctx.parsed.globals.approval}；${result.status === "warn" ? "MCP 工具可能被拒绝" : "未发现审批风险"}`}；${closing}`,
          servers: result.servers.map((server) => ({ name: server.name, source: server.source, mode: server.effectiveMode, risk: server.risk, recommendation: server.suggestion })),
          mcpDisabled, mcpWhenNever };
      });
      await check("askUserInput", "warn", async (limit) => {
        if (!codex || !diagBroker) return { status: "warn", detail: "未检查（codex 不可用）" };
        const output = await runProcess(codex, ["features", "list"], limit, diagBroker);
        const line = output.split(/\r?\n/).find((line) => /^\s*default_mode_request_user_input\s/.test(line));
        return line ? { status: "pass", detail: `提问开关存在，cxb 会按线程打开；当前状态：${line}` } : { status: "warn", detail: "提问将退回文字提问" };
      });
      const safeChecks = safeDiagnostic(checks) as DoctorCheck[];
      const summary = { pass: 0, warn: 0, fail: 0 };
      for (const check of safeChecks) summary[check.status]++;
      const ok = summary.fail === 0;
      ctx.out.resultJson({ ok, command: "doctor", version: safeSummary(config.clientVersion), checks: safeChecks, summary });
      const lines = safeChecks.map((check) => {
        // 每个外部模型名分别脱敏并限长，详细模式仍在同一检查行列出所有模型。
        const allModels = check.name === "models" && Array.isArray(check.models)
          ? `；全部模型：${check.models.map((model) => safeSummary(String(model.model ?? model.id ?? "未命名"))).join("、")}` : "";
        return `${check.status.toUpperCase()} ${check.name}: ${check.detail}${allModels}`;
      });
      ctx.out.result(`${lines.join("\n")}\nDoctor result: ${ok ? "PASS" : "FAIL"} (warnings: ${summary.warn}, failures: ${summary.fail})\n`);
      return ok ? EXIT.OK : EXIT.USAGE_OR_LOCAL_ERROR;
    } finally {
      controller.abort(); unregister();
      try { await bounded(Promise.allSettled([...processes]), 4500); }
      finally { await close(); }
    }
  };
}

export const doctor = createDoctorHandler();
