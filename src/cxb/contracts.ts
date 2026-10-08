// src/cxb/contracts.ts — cxb CLI 各模块之间的接口约定（S4 N1）。
//
// 这里只放各路共用的常量、类型与错误类；实现分别在：
//   workspace.ts（工作区、数据目录、旧数据迁移，拍板 G、H）
//   broker-conn.ts（broker 可执行文件查找、拉起、连接与版本握手，详细设计 2.2 节）
//   args.ts（参数解析与命令选项表，命令面规格 2.1、2.2 节）
//   output.ts（stdout/stderr 纪律、JSON 错误对象、退出前 flush，命令面规格 2.4、6.3 节）
// 依赖方向：各模块只依赖本文件与 src/ 下的既有底层模块（rpc.ts 等），互不直接引用；
// 由命令层（main.ts）把它们接起来。

import type { RpcEndpoint } from "../rpc";

// ─── 退出码（命令面规格第 4 节，命令不得私自增加含义）──────────────────────────

export const EXIT = {
  OK: 0,
  USAGE_OR_LOCAL_ERROR: 1,
  NOT_FOUND_OR_AMBIGUOUS: 2,
  TIMEOUT: 3,
  INTERRUPTED: 4,
  WAITING_RESPONSE: 5,
  BROKER_UNAVAILABLE: 6,
  CODEX_FAILED: 7,
  THREAD_HELD: 8,
  GOAL_BLOCKED_OR_LIMITED: 9,
  IDLE: 10,
  // 11 保留不用（原 peer 不可用）。
  PROTOCOL_MISMATCH: 12,
  SIGINT: 130,
  SIGTERM: 143,
} as const;

export type ExitCode = (typeof EXIT)[keyof typeof EXIT];

// ─── 错误（命令面规格 2.4 节）────────────────────────────────────────────────

/** JSON 错误对象的 `code`：大写稳定枚举，程序只看它和退出码，不看 message。 */
export const ERROR_CODES = {
  USAGE: EXIT.USAGE_OR_LOCAL_ERROR,
  LOCAL_ERROR: EXIT.USAGE_OR_LOCAL_ERROR,
  NOT_FOUND: EXIT.NOT_FOUND_OR_AMBIGUOUS,
  AMBIGUOUS: EXIT.NOT_FOUND_OR_AMBIGUOUS,
  TIMEOUT: EXIT.TIMEOUT,
  INTERRUPTED: EXIT.INTERRUPTED,
  WAITING_RESPONSE: EXIT.WAITING_RESPONSE,
  BROKER_UNAVAILABLE: EXIT.BROKER_UNAVAILABLE,
  CODEX_FAILED: EXIT.CODEX_FAILED,
  THREAD_HELD: EXIT.THREAD_HELD,
  GOAL_BLOCKED_OR_LIMITED: EXIT.GOAL_BLOCKED_OR_LIMITED,
  IDLE: EXIT.IDLE,
  PROTOCOL_MISMATCH: EXIT.PROTOCOL_MISMATCH,
  SIGINT: EXIT.SIGINT,
  SIGTERM: EXIT.SIGTERM,
} as const satisfies Record<string, ExitCode>;

export type ErrorCode = keyof typeof ERROR_CODES;

/**
 * cxb 内部统一抛出的错误。退出码由 code 决定（ERROR_CODES），命令层不另行映射。
 * message 面向人读，不得包含令牌、Cookie、完整环境变量或提示词原文。
 */
export class CxbError extends Error {
  readonly code: ErrorCode;
  readonly exitCode: ExitCode;
  readonly retryable: boolean;
  /** 相关的线程或事项 ID（若有），进 JSON 错误对象的 `id`。 */
  readonly id: string | null;

  constructor(code: ErrorCode, message: string, opts: { retryable?: boolean; id?: string | null; cause?: unknown } = {}) {
    super(message, opts.cause === undefined ? undefined : { cause: opts.cause });
    this.name = "CxbError";
    this.code = code;
    this.exitCode = ERROR_CODES[code];
    this.retryable = opts.retryable ?? false;
    this.id = opts.id ?? null;
  }
}

/** stderr 上的 JSON 错误对象（`--json` 时）。 */
export interface CxbErrorJson {
  ok: false;
  error: {
    code: ErrorCode;
    message: string;
    retryable: boolean;
    command: string | null;
    id: string | null;
  };
}

// ─── 工作区与数据目录（拍板 G、H；workspace.ts 实现）──────────────────────────

/** 环境变量：数据根目录本身（与 Rust store::data_root 一致：设了就直接用，否则 %USERPROFILE%\.codex-bridge）。 */
export const CXB_HOME_ENV = "CXB_HOME";

export interface WorkspaceInfo {
  /** 用户给的目录（`-d` 或 cwd）解析成的绝对路径。 */
  dir: string;
  /** 工作区根：`dir` 所在 git 仓库的顶层目录，不是仓库就是 `dir` 本身。作为 `--cwd` 传给 `cxb-broker launch`。 */
  root: string;
  /** 工作区键：与 Rust `store::path_key(canonicalize(root))` 逐字相同的 16 位小写十六进制。 */
  key: string;
  /** 数据根目录（`CXB_HOME` 或 `%USERPROFILE%\.codex-bridge`）。 */
  dataRoot: string;
  /** `<dataRoot>\workspaces\<key>`（broker 写，CLI 只读）。 */
  workspaceDir: string;
  /** 命名管道完整路径 `\\.\pipe\cxb-<key>`。 */
  pipePath: string;
  /** broker `--endpoint` 参数：`pipe:` + pipePath。 */
  endpoint: string;
}

/** 解析工作区。`dir` 不存在或不是目录时抛 CxbError("USAGE")；git 不可用按「不是仓库」处理。 */
export type ResolveWorkspace = (dir: string, env?: NodeJS.ProcessEnv) => WorkspaceInfo;

/** 旧数据迁移的结果（拍板 H：只迁 config.json 的有效键与用户模板，不碰旧文件，写已迁移标记）。 */
export interface MigrationReport {
  /** 本次是否做了迁移（已有标记、没有旧目录时为 false）。 */
  migrated: boolean;
  /** 迁入的配置键。 */
  configKeys: string[];
  /** 丢弃的旧配置键（peer 系等已删除的键），供 `-v` 提示。 */
  droppedKeys: string[];
  /** 复制的用户模板文件名。 */
  templates: string[];
  /** 未迁移的原因或告警（不阻断命令）。 */
  warnings: string[];
}

// ─── broker 连接（详细设计 2.2 节；broker-conn.ts 实现）────────────────────────

/** CLI 期望的 broker 协议版本；与 Rust core::PROTOCOL_VERSION 一致。 */
export const CXB_PROTOCOL_VERSION = 1;

/** 环境变量：显式指定 cxb-broker 可执行文件（开发与测试用）。 */
export const CXB_BROKER_BIN_ENV = "CXB_BROKER_BIN";

/** broker 对声明了 `capabilities.cxb` 的连接在 initialize 里返回的握手信息。 */
export interface BrokerHandshake {
  userAgent: string;
  brokerVersion: string;
  protocolVersion: number;
  epoch: number;
  server: { kind: "private" | "shared"; [k: string]: unknown };
}

/** 单个请求的期限：不传用 30 秒（config.requestTimeout）；停止确认等长操作必须显式放宽
 *  （`broker/threads/stop` 的确认窗口 40 秒，客户端至少等 45 秒，S3-计划 8.1；inbox/wait 为 timeoutMs 加 5 秒）。 */
export interface BrokerRequestOptions {
  timeoutMs?: number;
}

/** 连上 broker 后交给命令层的句柄：沿用 rpc.ts 的通知、服务端请求接口，请求可逐个指定期限。 */
export interface BrokerRpc extends Pick<RpcEndpoint, "notify" | "on" | "onAny" | "onRequest" | "onAnyRequest" | "respond" | "onClose"> {
  /** 失败一律抛 CxbError：线上错误为 cause（可读 rpcCode），超时与断开为 BROKER_UNAVAILABLE。 */
  request<T = unknown>(method: string, params?: unknown, opts?: BrokerRequestOptions): Promise<T>;
}

/** 从 BrokerRpc 抛出的 CxbError（或原始 RpcError）取线上错误码与 `data.reason`；不是线上错误时两者为 null。 */
export function rpcFailure(error: unknown): { code: number | null; reason: string | null } {
  const wire = error instanceof CxbError ? error.cause : error;
  const rpc = wire as { rpcCode?: unknown; data?: unknown } | null | undefined;
  const code = typeof rpc?.rpcCode === "number" ? rpc.rpcCode : null;
  const reason = (rpc?.data as { reason?: unknown } | null | undefined)?.reason;
  return { code, reason: code !== null && typeof reason === "string" ? reason : null };
}

/** 线上错误的 `data` 对象（生命周期错误的 kind、blockers、upstream 等附加字段）；没有时为空对象。 */
export function rpcData(error: unknown): Record<string, unknown> {
  const wire = error instanceof CxbError ? error.cause : error;
  const data = (wire as { data?: unknown } | null | undefined)?.data;
  return data && typeof data === "object" && !Array.isArray(data) ? data as Record<string, unknown> : {};
}

export interface BrokerConnection {
  rpc: BrokerRpc;
  handshake: BrokerHandshake;
  /** 本次是否由 CLI 拉起了 broker（`-v` 提示用）。 */
  launched: boolean;
  /** 关闭连接；幂等，有期限，不抛错。 */
  close(): Promise<void>;
}

export interface ConnectOptions {
  workspace: WorkspaceInfo;
  /** cxb-broker 可执行文件绝对路径（resolveBrokerBin 的结果）。 */
  brokerBin: string;
  /** 本次 broker 可执行文件的版本（`<brokerBin> --version` 输出的版本号），握手时与 brokerVersion 比对。 */
  expectedBrokerVersion: string;
  /** 整个连接过程（含拉起、退避重连、握手）的总期限，毫秒。 */
  timeoutMs: number;
  /** 拉起时传给 `launch --idle-timeout` 的毫秒数（配置键 `broker-idle-timeout`）；不传用 broker 缺省（5 分钟）。 */
  idleTimeoutMs?: number;
  /** 拉起 `cxb-broker launch` 时的环境变量（serve 经 launch 继承它）；不传用 process.env。测试用它注入模拟 codex 与临时 CXB_HOME。 */
  env?: NodeJS.ProcessEnv;
  /** `-v` 过程行（写 stderr 由调用方决定）；不传则不输出。 */
  log?: (line: string) => void;
  /** 取消正在进行的连接（拉起、退避、握手；Ctrl-C 时由清理回调触发）：立即停止后续步骤、关闭已开的
   *  socket，抛 CxbError("INTERRUPTED")；已开始的 launch 子进程等它自行结束（它本身很快退出），不再重连。 */
  signal?: AbortSignal;
}

/**
 * 连接工作区的 broker：连不上就 `cxb-broker launch --endpoint <ep> --cwd <root>` 拉起后重连；
 * 「连上就被断开」（broker 关闭中）按 100 ms 起、最长 2 秒退避；initialize 声明 `capabilities.cxb`；
 * protocolVersion 或 brokerVersion 不符时发 `broker/shutdown{ifIdle:true}`，成功后等旧实例退出：
 * 期间连上的实例照常握手，版本相符（不论是谁拉起的）就直接使用，管道不存在（ENOENT）才自己拉起；
 * 替换只做一轮。broker 忙（-32001）则抛 CxbError("PROTOCOL_MISMATCH")；拉起失败、期限内连不上抛
 * CxbError("BROKER_UNAVAILABLE")。
 */
export type ConnectBroker = (opts: ConnectOptions) => Promise<BrokerConnection>;

/** 查找 cxb-broker：CXB_BROKER_BIN → 与 CLI 同目录的 cxb-broker.exe（安装形态）→ 仓库内 broker-rs\target\debug；都没有抛 BROKER_UNAVAILABLE。 */
export type ResolveBrokerBin = (env?: NodeJS.ProcessEnv) => string;

// ─── broker 返回的数据类型（S4 N2 起；详细设计 2.3 节，字段与 Rust runs/types.rs、inbox/mod.rs 逐一对应）──

export type ApprovalPolicyName = "never" | "auto" | "on-request" | "on-failure" | "untrusted";

/** 2.3 节 RunMeta：annotate 登记、随运行记录返回。 */
export interface RunMeta {
  approval?: ApprovalPolicyName;
  approvalsReviewer?: string;
  sandbox?: string;
  template?: string;
  goal?: string;
  source: "run" | "review";
  [k: string]: unknown;
}

export interface TurnResult {
  turnId: string;
  ready: "pending" | "ready" | "unavailable";
  status: "completed" | "interrupted" | "failed";
  output: string;
  outputSource: "review" | "finalAnswer" | "accumulated" | "none";
  /** 原样的上游 TurnError 或启动失败的 RPC 错误对象。 */
  error: unknown;
  completedAt: number;
}

export interface ProgressItem { itemId: string; type: string; summary: string }

/** 空转刹车记录（S1-评审第 26 节；拍板 K：私有 action "stop"、共享 action "pauseGoal" 都退出 9）。 */
export interface LoopGuardReport {
  kind: "noAction" | "continuationLimit";
  count: number;
  threshold: number;
  turnIds: string[];
  action: "stop" | "pauseGoal";
  pause?: "pending" | "paused" | "superseded" | "failed";
}

export interface RunResult {
  children?: ChildRecord[];
  childrenStop?: ChildrenStopReport;
  runId: string;
  threadId: string;
  shortId: string;
  joined: boolean;
  state: "running" | "stopping" | "ended" | "lost";
  status: "completed" | "interrupted" | "failed" | "goalBlocked" | null;
  startedAt: number;
  endedAt: number | null;
  meta: RunMeta | null;
  reviewThreadId: string | null;
  reviewShortId?: string | null;
  input: string;
  continuedFrom: string | null;
  interruptReason: "clientDisconnected" | "stop" | "brokerLost" | "goalLoop" | "subagentsUnfinished" | null;
  stopOutcome: "confirmed" | "unconfirmed" | null;
  stopCheck?: unknown;
  loopGuard?: LoopGuardReport;
  progress: { done: ProgressItem[]; doneTotal: number; unconfirmed: ProgressItem[]; observedFiles: string[]; incomplete?: boolean };
  /** 最近一轮在最后；最多保留 100 轮。 */
  turns: TurnResult[];
  turnsTotal: number;
}

export type InboxKind = "approval" | "question" | "mcp" | "guardian";

export interface InboxItem {
  rootThreadId?: string;
  parentThreadId?: string;
  /** typed ID：ap_、qu_、mc_、gd_ 前缀（命令面规格第 7 节）。 */
  id: string;
  kind: InboxKind;
  threadId: string;
  runId: string | null;
  turnId: string | null;
  method: string;
  /** 原样保存；显示前必须脱敏（规格 2.4 节）。 */
  params: unknown;
  createdAt: number;
  expiresAt: number;
  status: "pending" | "resolving" | "resolved" | "expired" | "cancelled";
}

export interface ThreadSummary {
  threadId: string;
  shortId: string;
  state: "starting" | "running" | "awaiting" | "stopping" | "idle";
  owner: "client" | "broker" | null;
  turnId: string | null;
  goalActive: boolean;
  pendingCount: number;
  lastRunId: string | null;
  updatedAt: number;
  model: string | null;
  preview: string | null;
  cwd: string | null;
  meta: RunMeta | null;
  /** 有待核实的归档或删除时 broker 带出；只有记录、没有索引项的线程其余字段为空闲默认值。 */
  lifecycle?: LifecycleInfo;
}

// ─── 提问通道（拍板 I、L；S1-评审第 27、28 节）────────────────────────────────

/** 按线程打开 Default 模式提问工具的配置键（点号路径，p10 实测发出的载荷）。 */
export const ASK_FEATURE_KEY = "features.default_mode_request_user_input";

/** thread/start、thread/resume 与 `broker/threads/resumeOverrides` 共用的按线程覆盖。 */
export interface ResumeOverrides {
  config?: Record<string, unknown>;
  developerInstructions?: string;
}

// ─── 服务端请求（乙路 server-requests.ts；命令面规格 6.1 节、S1-评审第 27 节 L）──────

/** 与 broker-rs/tests/fixtures/policy.json 的 expect.kind 一一对应：answer 原样回包；inbox 表示需要人答
 *  （CLI 持有线程时走交接、退出 5）；methodNotFound 回 -32601。 */
export type PolicyDecision =
  | { kind: "answer"; result: unknown }
  | { kind: "inbox" }
  | { kind: "methodNotFound" };

/** 纯函数，必须逐条通过 policy.json（与 Rust policy::decide 同一份用例）。 */
export type DecidePolicy = (policy: ApprovalPolicyName, method: string, params: unknown) => PolicyDecision;

export interface HandoverResult {
  outcome: "detached" | "alreadyDetached" | "runEnded";
  turnId: string | null;
  /** 交接那一刻的待答事项快照。 */
  pending: InboxItem[];
  /** outcome 为 runEnded 时附带；最后一轮可能仍是 ready:"pending"，调用方按共用等待模块继续等。 */
  result?: RunResult;
}

/** `broker/threads/detach`：回包丢失（超时、断开前未回）可原样重试，alreadyDetached 视同成功；
 *  notOwner、joined、unknownRun、runMismatch 映射为 CxbError（分别为 THREAD_HELD、THREAD_HELD、NOT_FOUND、NOT_FOUND）。 */
export type Handover = (rpc: BrokerRpc, args: {
  threadId: string;
  runId: string;
  policy: ApprovalPolicyName;
  approvalsReviewer?: string;
}, opts?: { signal?: AbortSignal; timeoutMs?: number }) => Promise<HandoverResult>;

export interface ServerRequestRouterOptions {
  logicalRootThreadId?: string;
  getRunId?: () => string | undefined;
  /** 只处理这个线程的请求；别的线程的请求不答（broker 不会转来，出现即记 -v 日志）。 */
  threadId: string;
  policy: ApprovalPolicyName;
  /** 第一个需要人答的请求到达时调用一次（之后到达的同样不答，留给交接转进 inbox）。 */
  onNeedsHuman(method: string): void;
  log?: (line: string) => void;
}

/** 在连接上登记服务端请求处理（rpc.onAnyRequest）：按 DecidePolicy 自动回包；需要人答的不回包、
 *  调 onNeedsHuman。返回的函数撤销登记（幂等）。 */
export type AttachServerRequests = (rpc: BrokerRpc, opts: ServerRequestRouterOptions) => () => void;

/** 交接后（退出 5）stdout 的待答摘要：文本给人看（脱敏、每项一行摘要加回答方法 `cxb reply <id> …`、
 *  再 `cxb wait <线程短ID>`）；json 为规格 3.1 节的 `{ok:false,status:"waiting",threadId,pending}`（params 脱敏）。
 *  N4 的 inbox 命令复用同一脱敏规则。 */
export type RenderPending = (items: InboxItem[], ctx: { threadId: string; shortId: string }) => { text: string; json: unknown };

/** run、review 处理器需要的服务端请求能力，由 main.ts 注入（甲路不直接引用乙路模块，便于并行与替换）。 */
export interface ServerRequestDeps {
  attachServerRequests: AttachServerRequests;
  handover: Handover;
  renderPending: RenderPending;
}

// ─── 共用等待（甲路 wait-run.ts；run、review、--detach 之后的 wait、show 共用，详细设计 3.6 节「wait 等什么」）──

export interface WaitRunOptions {
  threadId: string;
  runId: string;
  /** 整体期限（毫秒）；null 表示不限。到期抛 CxbError("TIMEOUT")。 */
  timeoutMs: number | null;
  signal?: AbortSignal;
  log?: (line: string) => void;
  /** 工作区数据目录：broker 内存里已没有这次运行时（快照 run 为 null；内存每个线程只留最近一次、全局最多
   *  MAX_HISTORY 次），读 `runs/<runId>.json` 与已收到的通知合并作终态；仍是运行中或结果未就绪（写入未落盘或
   *  记录未完整加载）抛可重试的 BROKER_UNAVAILABLE；不传且没收到过这次运行则 NOT_FOUND。 */
  workspaceDir?: string;
}

/** 等整次运行结束（`broker/run/ended`，认领释放）且最后一轮结果不再是 pending（`broker/thread/result`，
 *  只发给 watcher，所以内部先 `broker/threads/watch{threadId, runId}`，快照给的就是这次运行，不受线程上更新的运行影响）。
 *  快照里已经结束且就绪的直接返回；快照 run 为 null 时按 workspaceDir 读磁盘记录，没有则 NOT_FOUND。
 *  连接断开抛 BROKER_UNAVAILABLE。不处理服务端请求（那是 AttachServerRequests 的事）。 */
export type WaitRun = (rpc: BrokerRpc, opts: WaitRunOptions) => Promise<RunResult>;

// ─── N4 命令用到的 broker 回包（详细设计 2.3 节；字段与 Rust 回包逐一对应）──────────────

/** `broker/threads/stop` 的回包：确认已停或 40 秒窗口到了才回。 */
export interface StopReply {
  threadId: string;
  runId: string | null;
  interruptSent: boolean;
  goal: "paused" | "cleared" | "none" | "unchanged";
  alreadyStopped: boolean;
  stopOutcome: "confirmed" | "unconfirmed";
  run: RunResult | null;
}

/**
 * 线程生命周期操作（归档、永久删除）的待核实记录（任务说明 N7-A 2.3.1、2.3.5 节）：
 * broker 的 `broker/threads/watch`、`broker/threads/list` 在线程摘要上带出，本地 `lifecycle.json` 保存同一事实。
 */
export interface LifecycleInfo {
  operationId: string;
  kind: "archive" | "delete";
  /** broker 内存阶段（如 `outcomeUnknown`、`draining`、`committing`）或落盘阶段（`localPurging`、`forwarded`、`upstreamDone`）。 */
  phase: string;
  shortId: string | null;
  preview: string | null;
}

/** `broker/threads/archive` 的回包（2.3.1 节）。 */
export interface ArchiveReply {
  threadId: string;
  operationId: string;
  archived: true | "alreadyDone";
  localIndex: "removed" | "alreadyAbsent";
}

/** 归档、删除被挡住时 broker 列出的阻塞项（2.3.2 节，`threadBusy` 的 `data.blockers`）。 */
export interface LifecycleBlocker {
  threadId: string;
  runId: string | null;
  reason: string;
}

/** `broker/threads/purge` 的回包：路径相对工作区数据目录；部分失败不回滚、如实列出。`serverDelete` 时含 `server-thread` 一项与 operationId。 */
export interface PurgeReply {
  threadId: string;
  operationId?: string;
  deleted: string[];
  alreadyDeleted: string[];
  failed: Array<{ path: string; reason: string }>;
}

/** `broker/inbox/resolve` 的回包；错误原因见 2.3 节（expired、notFound、alreadyResolved、cancelled、purgePending 等）。 */
export interface InboxResolveReply {
  status: "resolved";
  duplicate: boolean;
}

/** `broker/inbox/wait` 的回包。客户端期限为 timeoutMs 加 5 秒（S3-计划 8.1）。 */
export interface InboxWaitReply {
  event: "added" | "resolved" | "expired" | "cancelled" | "runEnded" | "idle" | "timeout";
  item?: InboxItem;
  run?: RunResult;
}

/** 运行终态 → 退出码：completed 0；interrupted 4（interruptReason goalLoop 为 9）；failed 7；goalBlocked 9；
 *  loopGuard 存在（含共享模式 pauseGoal 而 status 为 completed）一律 9。 */
export type ExitCodeForRun = (run: RunResult) => ExitCode;

// SA1a 共用线上声明；消费与展示由 SA-b 实现，缺省字段保持旧协议输出。
export type RelationKind = "reviewExecution" | "subagent" | "fork";
export type AgentVersion = "v1" | "v2" | "unknown";
export type ChildActivity = "starting" | "running" | "settled" | "unknown";
export interface ChildRecord {
  threadId: string; parentThreadId: string; relationKind: RelationKind;
  turnIds: string[]; evidence: "v1Spawn" | "v1Message" | "v2Started" | "v2Interacted" | "inherited";
  finalState: ChildActivity;
}
export interface DiscoveryState { complete: boolean; version: number; incompleteReasons: string[] }
export type TreePhase = "pending" | "confirmed" | "unconfirmed";
export interface RootStopReport {
  thread: string; phase: TreePhase; check: { ownership: boolean; turnEnded: boolean; goalBraked: boolean; settled: boolean; verifyDone: boolean };
  clearRequested: boolean; clearConfirmed: boolean; error: string | null;
}
export interface ChildrenStopReport {
  trigger: "userStop" | "ownerGone" | "parentFinishedEarly" | "loopGuard" | "parentFailed" | "parentInterrupted";
  phase: TreePhase; version: number; root: RootStopReport;
  stopped: string[]; unconfirmed: string[]; failed: string[];
  conflicts: Array<{ thread: string; reason: string }>; capabilityLimited: string[];
  discovery: DiscoveryState; unsettledSpawns: number;
}
export interface CxbOrigin {
  rootThreadId: string; runId: string; parentThreadId: string; relationKind: RelationKind; sourceThreadId: string;
}
export interface ChildChanged {
  runId: string; rootThreadId: string; threadId: string | null; parentThreadId?: string;
  relationKind?: RelationKind; activity?: ChildActivity; pendingCount?: number; revision: number; stopPhase?: TreePhase;
}
