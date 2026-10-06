import { closeSync, fsyncSync, mkdirSync, openSync, renameSync, unlinkSync, writeFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { execFile } from "node:child_process";
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

/** 回收预留：taskkill 最多 3 秒，最后确认退出与读取落定最多 1 秒。 */
const RECLAIM_RESERVE_MS = 4000;
const FINAL_WAIT_MS = 1000;

/**
 * 运行诊断程序（如 `codex --version`）。超时或取消时用 taskkill /T 结束仍在运行的进程树（codex.cmd 经命令解释器，不能只杀包装进程）；
 * 等待输出、回收、收尾共用一个绝对截止时间，整体不超过 `limit.timeoutMs`。
 * 已知限制：诊断程序自己先退出、留下的后代仍占着输出管道时，无法可靠找到这些后代——Windows 不维护进程树，
 * 按父进程编号查找会因编号复用误杀无关进程，中间进程退出后也会断链。此时只取消本端读取、按期返回，后代可能残留；
 * 根治需在启动时用 Job Object 管住整棵树（留 N7）。
 */
export async function runDiagnosticProcess(bin: string, args: string[], limit: DiagnosticLimit): Promise<string> {
  if (limit.signal.aborted) throw new CxbError("INTERRUPTED", "诊断已取消");
  // 尾部预算不足时不再创建无法保证回收的新进程。
  if (limit.timeoutMs <= RECLAIM_RESERVE_MS) throw new CxbError("TIMEOUT", "剩余期限不足以执行并回收诊断程序");
  const deadline = performance.now() + limit.timeoutMs;
  const left = () => Math.max(0, deadline - performance.now());
  let child: ReturnType<typeof Bun.spawn>;
  try { child = Bun.spawn([bin, ...args], { stdin: "ignore", stdout: "pipe", stderr: "pipe", windowsHide: true }); }
  catch (cause) { throw new CxbError("LOCAL_ERROR", "无法执行诊断程序", { cause }); }
  let stopping: Promise<void> | undefined;
  const stop = () => stopping ??= (async () => {
    if (child.exitCode !== null || child.signalCode !== null) return;
    if (process.platform === "win32") {
      await new Promise<void>((resolve, reject) => {
        const killer = execFile("taskkill.exe", ["/PID", String(child.pid), "/T", "/F"],
          { windowsHide: true, timeout: Math.max(1, Math.floor(Math.min(3000, left() - FINAL_WAIT_MS))), killSignal: "SIGKILL", maxBuffer: 4096 },
          (error, _stdout, stderr) => {
            if (error && child.exitCode === null && child.signalCode === null) reject(new CxbError("LOCAL_ERROR", `无法回收诊断进程树：${safeSummary(stderr) || "taskkill 未按期完成"}`, { cause: error }));
            else resolve();
          });
        killer.stdin?.destroy();
      });
    }
    try { child.kill("SIGKILL"); } catch { /* 可能已经退出，仍等 exited 确认。 */ }
  })();
  const abort = () => { void stop().catch(() => undefined); };
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
    const [code, stdout, stderr] = await bounded(io, left() - RECLAIM_RESERVE_MS, limit.signal);
    if (code !== 0) throw new CxbError("LOCAL_ERROR", `诊断程序退出 ${code}：${safeSummary(stderr)}`);
    return stdout;
  } finally {
    limit.signal.removeEventListener("abort", abort);
    let cleanupError: unknown;
    try { await stop(); }
    catch (error) { cleanupError = error; try { child.kill("SIGKILL"); } catch { /* 仍通过 exited 确认。 */ } }
    // 无论哪一路先失败，未结束的读取都要取消：管道可能仍被已退出进程的后代占着。
    readers.forEach((reader, index) => { if (open[index]) void reader.cancel().catch(() => undefined); });
    // 收尾只在剩余期限内尽力等待，超时不覆盖原来的诊断错误。
    try { await bounded(Promise.allSettled([child.exited, ...outputs]), Math.max(1, Math.min(FINAL_WAIT_MS, left()))); }
    catch { /* 已按期限放弃等待。 */ }
    if (cleanupError) throw cleanupError;
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
    preflight: (policy, env) => approvalPreflight(policy, env), probeWritable, now: () => performance.now(), totalMs: 60_000, itemMs: 10_000, ...overrides };
  return async (ctx) => {
    const controller = new AbortController();
    const unregister = ctx.registerCleanup(() => controller.abort());
    const deadline = deps.now() + deps.totalMs;
    const checks: DoctorCheck[] = [];
    let connection: BrokerConnection | undefined;
    let closing: Promise<void> | undefined;
    const close = () => connection ? (closing ??= connection.close()) : Promise.resolve();
    let codex: string | null = null;
    let brokerBin: string | undefined;
    let brokerVersion: string | undefined;
    let brokerUsable = false;
    let exhausted = false;
    const processes = new Set<Promise<string>>();
    const runProcess: DoctorDeps["runProcess"] = (bin, args, limit) => {
      const work = deps.runProcess(bin, args, limit);
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
        const version = await runProcess(codex, ["--version"], limit);
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
        return { status: result.status === "ok" ? "pass" : "warn", detail: result.status === "unavailable" ? "未能检查 MCP 审批风险" : `有效审批 ${ctx.parsed.globals.approval}；${result.status === "warn" ? "MCP 工具可能被拒绝" : "未发现审批风险"}`,
          servers: result.servers.map((server) => ({ name: server.name, source: server.source, mode: server.effectiveMode, risk: server.risk, recommendation: server.suggestion })) };
      });
      await check("askUserInput", "warn", async (limit) => {
        if (!codex) return { status: "warn", detail: "未检查（codex 不可用）" };
        const output = await runProcess(codex, ["features", "list"], limit);
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
