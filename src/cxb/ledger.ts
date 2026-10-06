// src/cxb/ledger.ts — broker 账本的只读访问（S4 N4 起 show、ls、run 共用）。
//
// broker 是数据目录的唯一写者（详细设计 7.2 节）；CLI 只在 broker 不在、或命令按规格优先读本地时读这些文件，
// 从不写。文件布局（详细设计 7.1 节）：`threads.json`、`runs/<runId>.json`（外壳 {epoch, revision, payload}）、
// `runs/<shortId>.log`（broker 追加写，轮转到 `.log.1`）。

import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { CxbError, type RunMeta, type RunResult } from "./contracts";

/** threads.json 里的一项（Rust runs::types::ThreadIndexEntry，只取 CLI 用到的字段）。 */
export interface ThreadIndexEntry {
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

/** 日志的一行：`<UTC 时间戳> <事件> <内容>`（Rust runs::state::log，内容截到 240 个字符、控制字符换成空格）。 */
export interface LogLine {
  /** ISO 8601 毫秒精度 UTC 时间戳，如 `2026-10-06T08:00:00.000Z`。 */
  ts: string;
  /** 时间戳之后的全部文字（事件名本身可能含空格，如「空转刹车暂停 goal」，不再拆分）。 */
  text: string;
}

const TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;
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
    }))
    .sort((a, b) => b.updatedAt - a.updatedAt || a.threadId.localeCompare(b.threadId));
}

/** 读一条运行记录；不存在返回 null，内容损坏或 runId 不符抛 LOCAL_ERROR。 */
export function readRunRecord(workspaceDir: string, runId: string): RunResult | null {
  const path = join(workspaceDir, "runs", `${runId}.json`);
  if (!existsSync(path)) return null;
  const payload = readPayload(path, `运行记录 ${runId} `);
  if (!isRunRecord(payload, runId)) throw new CxbError("LOCAL_ERROR", `运行记录 ${runId} 无法解析`);
  return payload;
}

/** 该线程的全部运行记录（运行属于它，或它是运行的 review 子线程），按 startedAt 升序。
 *  任何一条损坏都抛 LOCAL_ERROR（规格 3.3 节：日志损坏为 1）。 */
export function listThreadRuns(workspaceDir: string, threadId: string): RunResult[] {
  const dir = join(workspaceDir, "runs");
  if (!existsSync(dir)) return [];
  const runs: RunResult[] = [];
  for (const name of readdirSync(dir)) {
    if (!name.endsWith(".json")) continue;
    const run = readRunRecord(workspaceDir, name.slice(0, -".json".length));
    if (run && (run.threadId === threadId || run.reviewThreadId === threadId)) runs.push(run);
  }
  return runs.sort((a, b) => a.startedAt - b.startedAt || a.runId.localeCompare(b.runId));
}

function parseLog(text: string, label: string): LogLine[] {
  const lines = text.split("\n");
  // broker 按已提交长度追加；没有换行结尾的最后一段可能是写到一半的行，不展示。
  if (!text.endsWith("\n")) lines.pop();
  const parsed: LogLine[] = [];
  for (const raw of lines) {
    const line = raw.replace(/\r$/, "");
    if (!line) continue;
    const space = line.indexOf(" ");
    const ts = space < 0 ? line : line.slice(0, space);
    if (!TIMESTAMP.test(ts)) throw new CxbError("LOCAL_ERROR", `${label}有损坏的行`);
    parsed.push({ ts, text: space < 0 ? "" : line.slice(space + 1) });
  }
  return parsed;
}

/** 读线程日志（先 `.log.1` 再 `.log`，按写入顺序）；两者都不存在返回 null。 */
export function readRunLog(workspaceDir: string, shortId: string): LogLine[] | null {
  const base = join(workspaceDir, "runs", `${shortId}.log`);
  const parts = [`${base}.1`, base].filter((path) => existsSync(path));
  if (!parts.length) return null;
  const label = `线程 ${shortId} 的日志`;
  return parts.flatMap((path) => {
    let text: string;
    try { text = readFileSync(path, "utf8"); } catch (cause) { throw new CxbError("LOCAL_ERROR", `${label}无法读取`, { cause }); }
    return parseLog(text, label);
  });
}
