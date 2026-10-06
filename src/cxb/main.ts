#!/usr/bin/env bun
// src/cxb/main.ts — cxb 入口：解析参数 → 帮助、版本 → 工作区与旧数据迁移 → 交给命令处理（S4 N1 起）。
//
// 各命令的处理函数从 N2 起逐个登记进 HANDLERS；没登记的命令报「尚未提供」并退出 1。
// broker 连接按需建立（ctx.connect），只看帮助、版本或参数出错时不拉起 broker。

import { config } from "../config";
import { parseArgv, prescanArgv, type CommandName, type ParsedCommand } from "./args";
import { brokerVersionOf, connectBroker, resolveBrokerBin } from "./broker-conn";
import { applyUserConfig, readUserConfig, safeSummary, type UserConfig } from "./user-config";
import { CxbError, EXIT, type BrokerConnection, type ExitCode, type WorkspaceInfo } from "./contracts";
import { renderHelp } from "./help";
import { createReviewHandler } from "./review";
import { createRunHandler } from "./run";
import { attachServerRequests, handover, renderPending } from "./server-requests";
import { runMain, type Output, type RegisterCleanup } from "./output";
import { migrateLegacyData, resolveWorkspace } from "./workspace";

/** 连接 broker 的总期限：含拉起 serve 与 app-server 握手（冷启动实测约 1–3 秒，留足余量）。 */
const CONNECT_TIMEOUT_MS = 30_000;
/** 关闭连接前最多等命令处理器收尾的时间：run 在信号清理里发 stop 并等回包，客户端期限 45 秒（broker 40 秒确认窗口加余量）。 */
const HANDLER_SETTLE_MS = 50_000;
/** 清理总期限：覆盖上面的等待、关闭连接（最多 1 秒）与已开始的 launch（最多 10 秒，与 stop 不会同时发生），留调度余量。 */
const CLEANUP_TIMEOUT_MS = 60_000;

export interface CommandContext {
  parsed: ParsedCommand;
  out: Output;
  registerCleanup: RegisterCleanup;
  workspace: WorkspaceInfo;
  userConfig?: UserConfig;
  userConfigError?: CxbError;
  /** 首次调用时连接（必要时拉起）broker，之后复用同一连接；连接在命令结束时由清理回调关闭。 */
  connect(): Promise<BrokerConnection>;
}

export type CommandHandler = (ctx: CommandContext) => Promise<ExitCode>;

/** 已提供的命令；N2–N5 按 S4-计划逐个登记。 */
const SERVER_REQUESTS = { attachServerRequests, handover, renderPending };
export const HANDLERS: Partial<Record<CommandName, CommandHandler>> = {
  run: createRunHandler(SERVER_REQUESTS),
  review: createReviewHandler(SERVER_REQUESTS),
  show: async (ctx) => (await import("./show")).show(ctx),
  ls: async (ctx) => (await import("./ls")).ls(ctx),
  rm: async (ctx) => (await import("./rm")).rm(ctx),
  inbox: async (ctx) => (await import("./inbox")).inbox(ctx),
  reply: async (ctx) => (await import("./reply")).reply(ctx),
  wait: async (ctx) => (await import("./wait")).wait(ctx),
  stop: async (ctx) => (await import("./stop")).stop(ctx),
  config: async (ctx) => (await import("./config-cmd")).configCommand(ctx),
  doctor: async (ctx) => (await import("./doctor")).doctor(ctx),
};

export async function execute(parsed: ParsedCommand, out: Output, registerCleanup: RegisterCleanup,
  handlers: Partial<Record<CommandName, CommandHandler>>,
  deps = { resolveBrokerBin, brokerVersionOf, connectBroker }): Promise<ExitCode> {
  const { globals, command } = parsed;
  for (const warning of parsed.warnings) {
    if (!warning.verboseOnly || globals.verbose) out.notice(`[cxb] 告警：${warning.message}`);
  }
  if (globals.help) {
    const text = renderHelp(command);
    out.result(text);
    out.resultJson({ ok: true, command, help: text });
    return EXIT.OK;
  }
  if (globals.version || command === null) {
    out.result(`cxb ${config.clientVersion}\n`);
    out.resultJson({ ok: true, command: null, version: config.clientVersion });
    return EXIT.OK;
  }

  const workspace = resolveWorkspace(globals.dir);
  const migration = migrateLegacyData({ dataRoot: workspace.dataRoot });
  if (migration.migrated) {
    out.progress(safeSummary(`已从旧版 ~/.cxb-legacy 迁入配置键 ${migration.configKeys.join("、") || "（无）"}、模板 ${migration.templates.length} 个`));
    if (migration.droppedKeys.length) out.progress(safeSummary(`未迁入的旧配置键：${migration.droppedKeys.join("、")}`));
  }
  for (const warning of migration.warnings) out.progress(safeSummary(`旧数据迁移：${warning}`));

  let userConfig: UserConfig | undefined;
  let userConfigError: CxbError | undefined;
  try { userConfig = readUserConfig(workspace.dataRoot); }
  catch (error) {
    userConfigError = error as CxbError;
    if (["run", "review", "config"].includes(command)) throw error;
    if (command !== "doctor") out.notice("CLI 配置不可用，本次按内置默认值运行（broker 仍可能按配置或环境变量选择服务器）；详情见 cxb doctor");
  }
  if (userConfig) {
    applyUserConfig(parsed, userConfig);
    if (userConfig.unknownKeys.length) out.progress(`未知配置键：${userConfig.unknownKeys.map(safeSummary).join("、")}`);
  }

  const handler = handlers[command];
  if (!handler) throw new CxbError("LOCAL_ERROR", `cxb ${command} 尚未提供（按 S4 计划在后续里程碑实现）。`);

  let connection: Promise<BrokerConnection> | undefined;
  let connected: BrokerConnection | undefined;
  let closing: Promise<void> | undefined;
  // 迟到的连接可能同时被连接流程与清理回调关闭：只关一次，两处等同一个结果。
  const closeConnected = () => (connected ? (closing ??= connected.close()) : Promise.resolve());
  const controller = new AbortController();
  const interrupted = () => new CxbError("INTERRUPTED", "命令已结束，取消连接 broker");
  let handlerDone: Promise<unknown> = Promise.resolve();
  registerCleanup(async () => {
    controller.abort();
    // 清理回调并发执行：命令自己的收尾（run 发 stop 并等回包）必须先于关闭连接，所以先有界等待处理器结束；
    // 到期仍关闭，不让卡住的处理器拖住退出。
    let timer: ReturnType<typeof setTimeout> | undefined;
    await Promise.race([handlerDone.catch(() => undefined),
      new Promise<void>((resolve) => { timer = setTimeout(resolve, HANDLER_SETTLE_MS); })]);
    clearTimeout(timer);
    // 句柄独立于初始化 Promise：取得连接后的日志异常也不能丢失所有权。
    await closeConnected();
    await connection?.catch(() => undefined);
  });
  const running = handler({
    parsed,
    out,
    registerCleanup,
    workspace,
    userConfig,
    userConfigError,
    connect() {
      if (controller.signal.aborted) return Promise.reject(interrupted());
      connection ??= (async () => {
        const brokerBin = deps.resolveBrokerBin();
        const expectedBrokerVersion = await deps.brokerVersionOf(brokerBin);
        // 版本查询本身有期限；清理期间完成查询也不能继续拉起 broker。
        if (controller.signal.aborted) throw interrupted();
        const conn = await deps.connectBroker({
          workspace,
          brokerBin,
          expectedBrokerVersion,
          timeoutMs: CONNECT_TIMEOUT_MS,
          ...(userConfig?.values["broker-idle-timeout"] === undefined ? {} : { idleTimeoutMs: (userConfig.values["broker-idle-timeout"] as number) * 1000 }),
          signal: controller.signal,
          log: (line) => out.progress(`[cxb] ${line}`),
        });
        connected = conn;
        // 处理取消和连接完成相邻发生的情况，迟到句柄由取得方立即关闭。
        if (controller.signal.aborted) {
          await closeConnected();
          throw interrupted();
        }
        if (conn.launched) out.progress(`[cxb] 已拉起本工作区的 broker（${workspace.pipePath}）`);
        return conn;
      })();
      // 处理器可以启动连接后提前结束；清理仍会等待它，期间避免未处理拒绝。
      void connection.catch(() => undefined);
      return connection;
    },
  });
  handlerDone = running;
  return running;
}

/** 嵌入与验收可注入已有命令的处理器；生产入口传 HANDLERS，不扩展命令解析表。 */
export async function runCli(argv: string[], handlers: Partial<Record<CommandName, CommandHandler>>): Promise<void> {
  let parsed: ParsedCommand;
  try {
    parsed = parseArgv(argv);
  } catch (error) {
    const pre = prescanArgv(argv);
    await runMain(async () => { throw error; }, pre);
    return;
  }
  await runMain(
    (out, registerCleanup) => execute(parsed, out, registerCleanup, handlers),
    { command: parsed.command, json: parsed.globals.json, verbose: parsed.globals.verbose,
      cleanupTimeoutMs: CLEANUP_TIMEOUT_MS },
  );
}

export async function main(argv: string[]): Promise<void> {
  await runCli(argv, HANDLERS);
}

if (import.meta.main) await main(process.argv.slice(2));
