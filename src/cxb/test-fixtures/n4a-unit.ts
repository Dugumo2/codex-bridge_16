import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { parseArgv } from "../args";
import { CxbError, type BrokerConnection, type RunResult, type ThreadSummary } from "../contracts";
import type { CommandContext } from "../main";
import type { Cleanup } from "../output";
import { FakeRpc, runResult } from "./run-fakes";

export function summary(patch: Partial<ThreadSummary> = {}): ThreadSummary {
  return { threadId: "thread-test", shortId: "abcd1234", state: "idle", owner: null, turnId: null, goalActive: false,
    pendingCount: 0, lastRunId: "run-test", updatedAt: 2000, model: "模型", preview: "预览", cwd: null, meta: null, ...patch };
}
export function ledger(dir: string, runs: RunResult[] = [runResult()], threads: ThreadSummary[] = [summary()]) {
  mkdirSync(join(dir, "runs"), { recursive: true });
  writeFileSync(join(dir, "threads.json"), JSON.stringify({ payload: { threads: Object.fromEntries(threads.map((t) => [t.shortId, t])) } }));
  for (const run of runs) writeFileSync(join(dir, "runs", `${run.runId}.json`), JSON.stringify({ payload: run }));
}
export function logs(dir: string, lines: string[]) {
  writeFileSync(join(dir, "runs", "abcd1234.log"), lines.map((text, i) => `2026-10-06T00:00:${String(i).padStart(2, "0")}.000Z ${text}\n`).join(""));
}
export function reasonError(reason: string, code = -32000) {
  return new CxbError("BROKER_UNAVAILABLE", "远端失败", { cause: { rpcCode: code, detail: "远端失败", data: { reason } } });
}
export function context(dir: string, args: string[], fake = new FakeRpc()) {
  const parsed = parseArgv(args);
  const texts: string[] = [], json: any[] = [], notices: string[] = [], progress: string[] = [];
  const cleanup = new Set<Cleanup>();
  const any = new Set<(method: string, params: unknown) => void>();
  fake.rpc.onAny = (handler) => { any.add(handler); return () => { any.delete(handler); }; };
  const emit = (method: string, params: unknown) => { fake.emit(method, params); for (const handler of any) handler(method, params); };
  let connects = 0;
  const ctx: CommandContext = {
    parsed, workspace: { dir, root: dir, key: "测试", dataRoot: dir, workspaceDir: dir, pipePath: "", endpoint: "" },
    registerCleanup(fn) { cleanup.add(fn); return () => { cleanup.delete(fn); }; },
    connect: async () => { connects++; return { rpc: fake.rpc, close: async () => {} } as BrokerConnection; },
    out: { result(text) { if (!parsed.globals.json) texts.push(text); }, resultJson(value) { if (parsed.globals.json) json.push(value); },
      notice(text) { notices.push(text); }, progress(text) { progress.push(text); }, error() {}, flush: async () => {}, finish: async () => {} },
  };
  return { ctx, fake, texts, json, notices, progress, emit, cleanup, any, get connects() { return connects; } };
}
