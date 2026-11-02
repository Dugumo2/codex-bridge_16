// src/cxb/ledger.ts — broker 账本的只读访问（S4 N4 起 show、ls、run 共用）。
//
// broker 是数据目录的唯一写者（详细设计 7.2 节）；CLI 只在 broker 不在、或命令按规格优先读本地时读这些文件，
// 从不写。文件布局（详细设计 7.1 节）：`threads.json`、`runs/<runId>.json`（外壳 {epoch, revision, payload}）、
// `runs/<shortId>.log`（broker 追加写，轮转到 `.log.1`）。

import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { modelTurn } from "./run-result";
import { CxbError, type LifecycleInfo, type RunMeta, type RunResult, type RunModelSummary, type ModelRun } from "./contracts";
import type { LogCoverageIssue, TreeLogPayload } from "./tree-log-types";

/**
 * 子线程来源摘要（Rust relations::facts::edge_from_thread）：只含身份字段，不含正文。
 * `agentPath` 是 v2 协作工具的寻址键；昵称只供展示，不能用来寻址。
 */
export interface SourceSummary {
  kind?: "threadSpawn" | "review" | "other";
  agentPath?: string;
  agentNickname?: string;
  agentRole?: string;
  depth?: number;
}

/** threads.json 里的一项（Rust runs::types::ThreadIndexEntry，只取 CLI 用到的字段）。 */
export interface ThreadIndexEntry {
  /** 原样读取恢复登记；使用方核实形状，兼容旧索引缺少此字段。 */
  resumeOverrides?: unknown;
  parentThreadId?: string;
  relationKind?: import("./contracts").RelationKind;
  sourceSummary?: SourceSummary;
  canAcceptDirectInput?: boolean;
  agentVersion?: import("./contracts").AgentVersion;
  threadId: string;
  shortId: string;
  lastRunId: string | null;
  /** 毫秒时间戳。 */
  updatedAt: number;
  model: string | null;
  preview: string | null;
  cwd: string | null;
  meta: RunMeta | null;
}

/**
 * 日志的一行：`<UTC 时间戳> <runId> <事件> <内容>`（Rust runs::state::log；任务说明 N7-A 2.1 节）。
 * 按前三个空格切分；不属于任何运行的行（丢行标记）runId 写 `-`。事件名不含空格，内容截到 240 个字符、控制字符换成空格。
 */
export interface LogLine {
  /** 新格式身份；旧行保持没有此字段，正文已还原为 summary。 */
  source?: TreeLogPayload;
  /** ISO 8601 毫秒精度 UTC 时间戳，如 `2026-10-06T08:00:00.000Z`。 */
  ts: string;
  /** 所属运行；丢行标记等不属于任何运行的行为 null。 */
  runId: string | null;
  event: string;
  /** 可为空串。 */
  content: string;
  /** 显示用：`事件 内容`（内容为空时只有事件）。 */
  text: string;
}

const TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;
/** broker 的 runId 形如 `run-<base36>-<6 位十六进制>`；只要求 `run-` 前缀，旧格式（第二段是中文事件名）因此会被判为损坏。 */
const RUN_ID = /^run-\S+$/;
const RUN_STATES = new Set(["running", "stopping", "ended", "lost"]);
const RUN_STATUSES = new Set(["completed", "interrupted", "failed", "goalBlocked"]);
const TURN_READY = new Set(["pending", "ready", "unavailable"]);
const TURN_STATUSES = new Set(["completed", "interrupted", "failed"]);

function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
function optionalString(value: unknown): boolean {
  return value === undefined || value === null || typeof value === "string";
}

/** 运行记录里调用方依赖的字段（结果与就绪、运行状态与退出码、续跑资料）都在且取值合法。 */
function isRunRecord(value: unknown, runId: string): value is RunResult {
  if (!isObject(value) || value.runId !== runId || typeof value.threadId !== "string" || typeof value.shortId !== "string"
    || !RUN_STATES.has(value.state as string) || typeof value.startedAt !== "number" || typeof value.turnsTotal !== "number"
    || typeof value.input !== "string" || !optionalString(value.reviewThreadId) || !optionalString(value.continuedFrom)
    // status 只能是 null（运行中）或合法终态；缺失（undefined）同样拒绝，否则会被退出码映射当成「中断」。
    || (value.status !== null && !RUN_STATUSES.has(value.status as string))
    || (value.finalTurnId !== undefined && typeof value.finalTurnId !== "string")
    || !isProgress(value.progress) || !Array.isArray(value.turns)) return false;
  return value.turns.every((turn) => isObject(turn) && typeof turn.turnId === "string" && TURN_READY.has(turn.ready as string)
    && TURN_STATUSES.has(turn.status as string) && typeof turn.output === "string");
}

/** 续跑提示直接读 done、unconfirmed、observedFiles 与 doneTotal。 */
function isProgress(value: unknown): boolean {
  const items = (list: unknown) => Array.isArray(list) && list.every((item) => isObject(item)
    && typeof item.itemId === "string" && typeof item.type === "string" && typeof item.summary === "string");
  return isObject(value) && items(value.done) && items(value.unconfirmed) && typeof value.doneTotal === "number"
    && Array.isArray(value.observedFiles) && value.observedFiles.every((file) => typeof file === "string");
}

function readPayload(path: string, label: string): unknown {
  try {
    return (JSON.parse(readFileSync(path, "utf8")) as { payload?: unknown }).payload;
  } catch (cause) {
    throw new CxbError("LOCAL_ERROR", `${label}无法解析`, { cause });
  }
}

/** lifecycle.json 的一条记录（Rust 的生命周期操作意图，任务说明 N7-A 2.3.5 节；只取 CLI 用到的字段）。 */
export interface LifecycleRecord extends LifecycleInfo {
  threadId: string;
  updatedAt: number;
}

/** 读生命周期记录；文件不存在返回空数组，损坏抛 LOCAL_ERROR（broker 同样拒绝改状态，不能当作「没有待核实」）。按 updatedAt 倒序。 */
export function readLifecycle(workspaceDir: string): LifecycleRecord[] {
  const path = join(workspaceDir, "lifecycle.json");
  if (!existsSync(path)) return [];
  const payload = readPayload(path, "生命周期记录 lifecycle.json ") as { records?: unknown } | undefined;
  const records = payload?.records;
  if (!isObject(records)) throw new CxbError("LOCAL_ERROR", "生命周期记录 lifecycle.json 缺少 records");
  return Object.entries(records).map(([key, record]) => {
    if (!isObject(record) || record.threadId !== key || typeof record.operationId !== "string"
      || (record.kind !== "archive" && record.kind !== "delete") || typeof record.phase !== "string"
      || typeof record.updatedAt !== "number" || !optionalString(record.shortId) || !optionalString(record.preview)) {
      throw new CxbError("LOCAL_ERROR", `生命周期记录 lifecycle.json 的条目 ${key} 损坏`);
    }
    return { threadId: key, operationId: record.operationId, kind: record.kind as LifecycleInfo["kind"], phase: record.phase,
      shortId: (record.shortId as string | null | undefined) ?? null, preview: (record.preview as string | null | undefined) ?? null,
      updatedAt: record.updatedAt };
  }).sort((a, b) => b.updatedAt - a.updatedAt || a.threadId.localeCompare(b.threadId));
}

const SOURCE_KINDS = new Set(["threadSpawn", "review", "other"]);

/** 只带出形状合法的来源字段；与索引其他可选字段一致，类型不符的字段丢弃而不报损坏。 */
function withSourceSummary(value: unknown): { sourceSummary?: SourceSummary } {
  if (!isObject(value)) return {};
  const summary: SourceSummary = {};
  if (SOURCE_KINDS.has(value.kind as string)) summary.kind = value.kind as SourceSummary["kind"];
  for (const key of ["agentPath", "agentNickname", "agentRole"] as const) {
    if (typeof value[key] === "string" && value[key] !== "") summary[key] = value[key] as string;
  }
  if (Number.isSafeInteger(value.depth) && (value.depth as number) >= 0) summary.depth = value.depth as number;
  return Object.keys(summary).length ? { sourceSummary: summary } : {};
}

const CHILD_EVIDENCE = new Set(["v1Spawn", "v1Message", "v2Started", "v2Interacted", "inherited"]);
const CHILD_ACTIVITY = new Set(["starting", "running", "settled", "unknown"]);
const RELATION_KINDS = new Set(["reviewExecution", "subagent", "fork"]);

/**
 * 运行记录里本次有归属证据的子线程（`children`）。isRunRecord 不校验这一可选字段，
 * 使用方经此读取：形状不合法的条目不采用、只计数，调用方据此标注「记录不完整」，不能当作没有子代理。
 */
export function readChildRecords(run: RunResult): { records: import("./contracts").ChildRecord[]; malformed: number } {
  const raw: unknown = run.children;
  if (raw === undefined) return { records: [], malformed: 0 };
  if (!Array.isArray(raw)) return { records: [], malformed: 1 };
  const records = raw.filter((child): child is import("./contracts").ChildRecord => isObject(child)
    && typeof child.threadId === "string" && child.threadId !== "" && typeof child.parentThreadId === "string"
    && RELATION_KINDS.has(child.relationKind as string) && CHILD_EVIDENCE.has(child.evidence as string)
    && CHILD_ACTIVITY.has(child.finalState as string)
    && Array.isArray(child.turnIds) && child.turnIds.every((turnId) => typeof turnId === "string" && turnId !== ""));
  return { records, malformed: raw.length - records.length };
}

/** 读线程索引；文件不存在返回空数组，损坏抛 LOCAL_ERROR。按 updatedAt 倒序。 */
export function readThreadIndex(workspaceDir: string): ThreadIndexEntry[] {
  const path = join(workspaceDir, "threads.json");
  if (!existsSync(path)) return [];
  const payload = readPayload(path, "线程索引 threads.json ") as { threads?: unknown } | undefined;
  const threads = payload?.threads;
  if (!isObject(threads)) throw new CxbError("LOCAL_ERROR", "线程索引 threads.json 缺少 threads");
  return Object.entries(threads)
    .map(([key, entry]) => {
      // 坏条目不能静默跳过：否则损坏会被报成「线程不存在」或空列表。
      if (!isObject(entry) || typeof entry.threadId !== "string" || entry.shortId !== key || typeof entry.updatedAt !== "number"
        || !optionalString(entry.lastRunId)) {
        throw new CxbError("LOCAL_ERROR", `线程索引 threads.json 的条目 ${key} 损坏`);
      }
      return entry as unknown as ThreadIndexEntry;
    })
    .map((entry) => ({
      threadId: entry.threadId, shortId: entry.shortId, lastRunId: entry.lastRunId ?? null,
      updatedAt: Number(entry.updatedAt) || 0, model: entry.model ?? null, preview: entry.preview ?? null,
      cwd: entry.cwd ?? null, meta: entry.meta ?? null,
      ...(entry.resumeOverrides !== undefined ? { resumeOverrides: entry.resumeOverrides } : {}),
      // SA1：子线程的血缘字段只在索引里有时带出（旧索引没有这些键）。
      ...(typeof entry.parentThreadId === "string" ? { parentThreadId: entry.parentThreadId } : {}),
      ...(typeof entry.relationKind === "string" ? { relationKind: entry.relationKind } : {}),
      ...(typeof entry.canAcceptDirectInput === "boolean" ? { canAcceptDirectInput: entry.canAcceptDirectInput } : {}),
      ...(typeof entry.agentVersion === "string" ? { agentVersion: entry.agentVersion } : {}),
      ...withSourceSummary(entry.sourceSummary),
    }))
    .sort((a, b) => b.updatedAt - a.updatedAt || a.threadId.localeCompare(b.threadId));
}

/** inbox.json 里仍未决（pending、resolving）的事项；文件不存在或无法解析时返回 null（只供展示，broker 不跨重启沿用）。 */
export function readPendingInbox(workspaceDir: string): import("./contracts").InboxItem[] | null {
  const path = join(workspaceDir, "inbox.json");
  if (!existsSync(path)) return null;
  try {
    const items = (readPayload(path, "收件箱 inbox.json ") as { items?: unknown } | undefined)?.items;
    if (!Array.isArray(items)) return null;
    return items.filter((item): item is import("./contracts").InboxItem => isObject(item) && typeof item.id === "string"
      && typeof item.threadId === "string" && (item.status === "pending" || item.status === "resolving"));
  } catch { return null; }
}

/** 读一条运行记录；不存在返回 null，内容损坏或 runId 不符抛 LOCAL_ERROR。 */
export function readRunRecord(workspaceDir: string, runId: string): RunResult | null {
  const path = join(workspaceDir, "runs", `${runId}.json`);
  if (!existsSync(path)) return null;
  const payload = readPayload(path, `运行记录 ${runId} `);
  if (!isRunRecord(payload, runId)) throw new CxbError("LOCAL_ERROR", `运行记录 ${runId} 无法解析`);
  return payload;
}

/** 该线程的全部运行记录（运行属于它，或它是运行的 review 子线程），按创建顺序升序。
 *  任何一条损坏都抛 LOCAL_ERROR（规格 3.3 节：日志损坏为 1）。 */
export function listThreadRuns(workspaceDir: string, threadId: string): RunResult[] {
  return readAllRuns(workspaceDir).filter((run) => run.threadId === threadId || run.reviewThreadId === threadId)
    .sort(compareRunCreation);
}

/** 一次扫描全部保留记录；沿用与批量显示共用，不能依赖线程索引或内存保留窗口。 */
export function readAllRuns(workspaceDir: string): RunResult[] {
  const dir = join(workspaceDir, "runs");
  if (!existsSync(dir)) return [];
  const runs: RunResult[] = [];
  for (const name of readdirSync(dir)) {
    if (!name.endsWith(".json")) continue;
    const run = readRunRecord(workspaceDir, name.slice(0, -".json".length));
    if (run) runs.push(run);
  }
  return runs;
}

/** 创建顺序不受墙钟回拨、同毫秒随机 runId、重写文件影响；旧记录统一排在新格式之前。 */
export function compareRunCreation(a: ModelRun, b: ModelRun): number {
  const x = a.creationOrder, y = b.creationOrder;
  if (x && y) return x.epoch - y.epoch || x.sequence - y.sequence || a.runId.localeCompare(b.runId);
  if (x || y) return x ? 1 : -1;
  return a.startedAt - b.startedAt || a.runId.localeCompare(b.runId);
}

/** 在线状态覆盖同一运行的磁盘旧快照；根线程严格匹配，review/血缘不是继承关系。 */
export function latestModelRun(threadId: string, disk: ModelRun[], online: ModelRun[] = []): ModelRun | undefined {
  const runs = new Map([...disk, ...online].map((run) => [run.runId, run]));
  return [...runs.values()].filter((run) => run.threadId === threadId && run.meta?.source === "run"
    && run.startConfirmation?.confirmed === true && !!run.meta.model)
    .sort(compareRunCreation).at(-1);
}

/** 模型沿用专用扫描：坏文件告警后跳过，不改变其他命令的严格账本校验。 */
export function readModelRuns(workspaceDir: string, notice: (message: string) => void): {
  runs: RunModelSummary[]; unreadable: Array<{ runId: string; threadId: string | null }>;
} {
  const runs: RunModelSummary[] = [];
  const unreadable: Array<{ runId: string; threadId: string | null }> = [];
  const dir = join(workspaceDir, "runs");
  if (!existsSync(dir)) return { runs, unreadable };
  for (const name of readdirSync(dir)) {
    if (!name.endsWith(".json")) continue;
    const runId = name.slice(0, -5);
    let payload: unknown;
    try {
      payload = readPayload(join(dir, name), `运行记录 ${runId} `);
      if (!isRunRecord(payload, runId)) throw new CxbError("LOCAL_ERROR", "运行记录结构损坏");
      // 逐文件丢弃正文，避免保留整个工作区的历史输出。
      const last = modelTurn(payload);
      runs.push({ runId, threadId: payload.threadId, reviewThreadId: payload.reviewThreadId, state: payload.state,
        startedAt: payload.startedAt, creationOrder: payload.creationOrder ?? null,
        startConfirmation: payload.startConfirmation ?? { confirmed: false, owned: null, threadId: null, turnId: null },
        meta: payload.meta ? { source: payload.meta.source, model: payload.meta.model, effort: payload.meta.effort } : null,
        currentTurnId: payload.currentTurnId ?? null, lastTurn: last ? { turnId: last.turnId, reroute: last.reroute ?? null } : null,
        pendingReroute: payload.pendingReroute ?? null,
        ...(payload.finalTurnId !== undefined ? { finalTurnId: payload.finalTurnId } : {}) });
    } catch {
      const threadId = isObject(payload) && payload.runId === runId && typeof payload.threadId === "string" && payload.threadId
        ? payload.threadId : null;
      unreadable.push({ runId, threadId });
      notice(`运行记录 ${name} 无法解析，已跳过模型沿用读取`);
    }
  }
  return { runs, unreadable };
}

function parseLog(text: string, label: string): LogLine[] {
  const lines = text.split("\n");
  // broker 按已提交长度追加；没有换行结尾的最后一段可能是写到一半的行，不展示。
  if (!text.endsWith("\n")) lines.pop();
  const parsed: LogLine[] = [];
  for (const raw of lines) {
    const line = raw.replace(/\r$/, "");
    if (!line) continue;
    const [ts, run, event, ...rest] = line.split(" ");
    let content = rest.join(" ");
    if (!TIMESTAMP.test(ts!) || run === undefined || (run !== "-" && !RUN_ID.test(run)) || !event) {
      throw new CxbError("LOCAL_ERROR", `${label}有损坏的行`);
    }
    let source: TreeLogPayload | undefined;
    if (/^@cxb2(?:\s|$)/.test(content)) {
      try {
        const value: unknown = JSON.parse(content.slice(6));
        const nullable = (v: unknown) => v === null || typeof v === "string";
        if (!isObject(value) || !["eventId", "rootThreadId", "threadId"].every((key) => typeof value[key] === "string" && value[key] !== "")
          || !["parentThreadId", "shortId", "turnId", "itemId"].every((key) => nullable(value[key]))
          || !["run", "execution", "subagent"].includes(value.sourceRole as string)
          || typeof value.summary !== "string" || typeof value.truncated !== "boolean"
          || (value.late !== undefined && typeof value.late !== "boolean")
          || (value.coverageIssue !== undefined && !["beforeBindingUnknown", "discoveryIncomplete"].includes(value.coverageIssue as string))) {
          throw new Error("结构化日志字段损坏");
        }
        source = value as unknown as TreeLogPayload;
        content = source.summary;
      } catch (cause) { throw new CxbError("LOCAL_ERROR", `${label}有损坏的 @cxb2 行`, { cause }); }
    }
    parsed.push({ ts: ts!, runId: run === "-" ? null : run, event, content, text: content ? `${event} ${content}` : event,
      ...(source ? { source } : {}) });
  }
  return parsed;
}

/** 旧模式单次读取已保留的完整行；正常追加不要求稳定快照，也不重读或重新去重。 */
export function readRunLog(workspaceDir: string, shortId: string, io: LogReadIO = legacyLogIO): LogLine[] | null {
  const base = join(workspaceDir, "runs", `${shortId}.log`);
  const label = `线程 ${shortId} 的日志`;
  try {
    const parts = [`${base}.1`, base].filter((path) => io.fingerprint(path) !== null);
    if (!parts.length) return null;
    return parts.flatMap((path) => parseLog(io.read(path), label));
  } catch (cause) {
    if (cause instanceof CxbError) throw cause;
    throw new CxbError("LOCAL_ERROR", `${label}无法读取`, { cause });
  }
}

/** 文件身份与长度一起核验，避免轮转时把旧活动文件与新备份拼错。接口允许确定性模拟轮转。 */
export interface LogReadIO {
  fingerprint(path: string): string | null;
  read(path: string): string;
}
const legacyLogIO: LogReadIO = {
  fingerprint: (path) => existsSync(path) ? "存在" : null,
  read: (path) => readFileSync(path, "utf8"),
};
const logIO: LogReadIO = {
  fingerprint(path) {
    try {
      const stat = statSync(path, { bigint: true });
      return `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}`;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
      throw error;
    }
  },
  read: (path) => readFileSync(path, "utf8"),
};

/** 静态保留窗口：最多三次整批读取，不做增量读取或终态水位推断。 */
export function readRunLogSnapshot(workspaceDir: string, shortId: string, io: LogReadIO = logIO): {
  lines: LogLine[] | null; issues: LogCoverageIssue[];
} {
  const base = join(workspaceDir, "runs", `${shortId}.log`);
  const parts = [`${base}.1`, base];
  const label = `线程 ${shortId} 的日志`;
  let changed = false;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const before = parts.map((path) => io.fingerprint(path));
      const texts = parts.map((path, i) => before[i] === null ? "" : io.read(path));
      const after = parts.map((path) => io.fingerprint(path));
      if (before.some((stamp, i) => stamp !== after[i])) { changed = true; continue; }
      if (before.every((stamp) => stamp === null)) return { lines: null, issues: [{ code: "missingLog" }] };
      const issues: LogCoverageIssue[] = [];
      if (changed || before[0] !== null) issues.push({ code: "rotationWindow" });
      if (texts.some((text) => text.length > 0 && !text.endsWith("\n"))) issues.push({ code: "trailingPartialLine" });
      const seen = new Set<string>();
      const lines = texts.flatMap((text) => parseLog(text, label)).filter((line) => {
        const id = line.source?.eventId;
        if (!id) return true;
        if (seen.has(id)) return false;
        seen.add(id);
        return true;
      });
      return { lines, issues };
    } catch (cause) {
      if ((cause as NodeJS.ErrnoException).code === "ENOENT") { changed = true; continue; }
      if (cause instanceof CxbError) throw cause;
      throw new CxbError("LOCAL_ERROR", `${label}无法读取`, { cause });
    }
  }
  throw new CxbError("LOCAL_ERROR", `${label}持续变化，无法取得稳定快照，请重试`);
}

/** 旧 show 的事件范围：必须在 limit、摘要筛选和补发去重之前调用。 */
export function ownLogLines(lines: LogLine[]): LogLine[] {
  // 树发现缺口可挂在根身份上，它也是本期新增的树范围诊断，不能挤占旧视图。
  return lines.filter((line) => line.source?.sourceRole !== "subagent" && !line.source?.coverageIssue);
}
