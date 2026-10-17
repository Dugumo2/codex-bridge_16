/**
 * 真 broker＋仓库 CLI＋真实 Codex 0.160.0，仅访问本地脚本模型。
 * --smoke 记录骨架行为并用显式用户停止释放挂起轮；完整契约待合并 C1/C2/C3。
 */
import assert from "node:assert/strict";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { delimiter, join, resolve } from "node:path";
import { openStallRpc } from "../cxb/test-fixtures/stall-harness";
import { resolveWorkspace } from "../cxb/workspace";
import { OK_TEXT } from "./upstream-model-server";
import { startStallModel, type StallModelScenario } from "./upstream-model-server-stall";
import { CLEANUP_MS, codexCommand, createUpstreamIsolation, removeIsolation, startManaged, stopManaged,
  verifyEffectiveConfig, within, type ManagedProcess } from "./upstream-isolation";

const REPORT = resolve(import.meta.dir, "../../.ue2-verify");
interface CliResult { args: string[]; code: number | null; stdout: string; stderr: string }
export interface StallE2eResult {
  scenario: StallModelScenario; mode: "smoke" | "contract"; version: string; passed: boolean; cleaned: boolean;
  startedAt: number; durationMs: number; deadlineMs: number; cleanupMs: number;
  requests: unknown[]; events: Array<{ at: number; method: string; params: any }>; cli: CliResult[];
  baseline?: { elapsedMs: number; state: string; stall?: unknown; active: boolean }; record?: any; failure?: string;
}
const parse = (result: CliResult) => {
  const value = JSON.parse(result.stdout.trim()); assert(value && typeof value === "object" && !Array.isArray(value), "stdout 必须只有最终单个 JSON 对象");
  assert.equal(value.runPolicy, undefined, "CLI 最终输出不得暴露运行策略"); return value;
};
const exact = (result: CliResult, text: string) => assert(result.stderr.split(/\r?\n/u).includes(text), `stderr 缺少固定行：${text}`);

/** 正式挂起场景必须证明请求先到达本地模型，不能把请求发出前的停滞当作模型挂起。 */
export function assertHangRequestBeforeExpiry(requests: ReadonlyArray<{ at: number }>, expiredAt: number) {
  assert(requests.length > 0, "挂起模型服务必须已接受请求");
  assert(Number.isFinite(expiredAt) && Number.isFinite(requests[0].at) && requests[0].at < expiredAt,
    "挂起请求必须在无进展到期之前到达模型服务");
}

export async function runStallE2e(scenario: StallModelScenario, mode: "smoke" | "contract" = "contract"): Promise<StallE2eResult> {
  const bin = process.env.CXB_BROKER_BIN;
  if (!bin || !existsSync(bin)) throw new Error("必须显式指定 CXB_BROKER_BIN，禁止回退到其他 broker");
  const startedAt = Date.now(), deadlineMs = 110_000, deadline = startedAt + deadlineMs;
  const result: StallE2eResult = { scenario, mode, version: "", passed: false, cleaned: false, startedAt, durationMs: 0,
    deadlineMs, cleanupMs: CLEANUP_MS, requests: [], events: [], cli: [] };
  let model: Awaited<ReturnType<typeof startStallModel>> | undefined;
  let isolation: ReturnType<typeof createUpstreamIsolation> | undefined;
  const processes: ManagedProcess[] = [];
  let connection: Awaited<ReturnType<typeof openStallRpc>> | undefined;
  const remaining = (limit = 10_000) => {
    const time = deadline - Date.now(); if (time <= 0) throw new Error("真实第二批场景超过 110 秒总期限"); return Math.min(limit, time);
  };
  const until = async (check: () => Promise<boolean> | boolean, label: string, limit = 90_000) => {
    const end = Date.now() + remaining(limit);
    while (Date.now() < end) { if (await check()) return; await Bun.sleep(10); }
    throw new Error(`${label} 超过场景期限`);
  };
  try {
    model = await startStallModel(scenario); result.requests = model.requests;
    isolation = createUpstreamIsolation(model.baseUrl);
    const shim = join(isolation.root, "bin"); mkdirSync(shim);
    const native = codexCommand([])[0];
    writeFileSync(join(shim, "codex.cmd"), `@echo off\r\n"${native}" %*\r\nexit /b %errorlevel%\r\n`);
    let oldPath = "";
    for (const key of Object.keys(isolation.env)) if (key.toUpperCase() === "PATH") { oldPath = isolation.env[key] ?? ""; delete isolation.env[key]; }
    Object.assign(isolation.env, { PATH: `${shim}${delimiter}${oldPath}`, CXB_BROKER_BIN: bin });
    const workspace = resolveWorkspace(isolation.cwd, isolation.env);
    const version = startManaged(codexCommand(["--version"]), isolation); processes.push(version);
    await within(version.closed, remaining(5000), "真实版本查询"); result.version = version.output().stdout.trim(); assert.equal(result.version, "codex-cli 0.160.0");
    const broker = startManaged([bin, "serve", "--endpoint", workspace.endpoint, "--cwd", isolation.cwd, "--idle-timeout", "120000"], isolation); processes.push(broker);
    connection = await openStallRpc(workspace.pipePath, Date.now() + remaining(12_000)); const rpc = connection.rpc;
    rpc.onAny((method, params) => result.events.push({ at: Date.now(), method, params }));
    const handshake = await rpc.request<any>("initialize", { clientInfo: { name: "ue2-real", version: "1" }, capabilities: { experimentalApi: true, cxb: { protocolVersion: 1 } } });
    rpc.notify("initialized"); assert.equal(handshake.server.kind, "private");
    verifyEffectiveConfig(await rpc.request("config/read", { cwd: isolation.cwd, includeLayers: false }), model.baseUrl);
    const command = async (args: string[]) => {
      const proc = startManaged([process.execPath, resolve(import.meta.dir, "../cxb/main.ts"), ...args, "--dir", isolation!.cwd], isolation!);
      processes.push(proc); proc.child.stdin.end(); const code = await within(proc.closed, remaining(95_000), `真实 CLI ${args[0]}`);
      const output = { args, code, ...proc.output() }; result.cli.push(output); return output;
    };
    const args = ["run", "只输出本地脚本结果。", "-m", "gpt-5.4", "-r", "medium", "--json", "--approval", "never", "--sandbox", "danger-full-access", "--timeout", "90",
      "--stall-timeout", scenario === "hang" ? "20" : "0", "--upstream-error-policy", scenario === "401" ? "stop-access" : "observe"];
    const running = command(args); void running.catch(() => {});
    let id = "", runId = "";
    await until(async () => {
      const listed = await rpc.request<any>("broker/threads/list", {});
      const thread = listed.threads.find((entry: any) => entry.lastRunId);
      if (thread) { id = thread.threadId; runId = thread.lastRunId; return true; }
      if (result.cli.length) throw new Error(`真实 CLI 提前退出 ${result.cli[0].code}：${result.cli[0].stderr}`);
      return false;
    }, "真实运行未开始", 15_000);
    await rpc.request("broker/threads/watch", { threadId: id, runId });
    if (scenario === "hang" || mode === "smoke" && scenario !== "ok") {
      await until(() => model!.requests.length > 0, "模型服务未接受本机请求", 15_000);
    }
    if (mode === "smoke" && scenario !== "ok") {
      if (scenario === "hang") {
        // 让 20 秒策略完整经过，证明骨架仍在运行；只记录当前基线，不把它当契约通过。
        const observeUntil = Date.now() + 22_000;
        while (Date.now() < observeUntil) await Bun.sleep(Math.min(250, observeUntil - Date.now()));
      } else await until(async () => (await rpc.request<any>("broker/threads/watch", { threadId: id, runId })).run?.upstreamError?.willRetry === true, "真实 401 重试未出现", 10_000);
      const before = (await rpc.request<any>("broker/threads/watch", { threadId: id, runId })).run;
      result.baseline = { elapsedMs: Date.now() - startedAt, state: before.state, stall: before.stall, active: before.state === "running" };
      assert.equal(before.state, "running", "骨架基线应尚未自动停止"); assert.equal(before.interruptReason, null); assert.equal(before.stall, undefined);
      const stopped = await rpc.request<any>("broker/threads/stop", { threadId: id, runId }, { timeoutMs: remaining(45_000) });
      assert.equal(stopped.stopOutcome, "confirmed"); assert.equal(stopped.interruptSent, true);
    }
    const run = await running, final = parse(run);
    const record = (await rpc.request<any>("broker/threads/watch", { threadId: id, runId })).run; result.record = record;
    const path = join(workspace.workspaceDir, "runs", `${runId}.json`); let persisted: any;
    await until(() => { if (!existsSync(path)) return false; persisted = JSON.parse(readFileSync(path, "utf8")).payload;
      return persisted.runId === runId && persisted.state === record.state && persisted.diagnosticRevision === record.diagnosticRevision; }, "真实记录落盘", 5000);
    assert.deepEqual(persisted.runPolicy, { stallTimeoutMs: scenario === "hang" ? 20_000 : 0, upstreamErrorPolicy: scenario === "401" ? "stopAccess" : "observe" });
    if (scenario === "ok") {
      assert.equal(run.code, 0); assert.equal(final.result, OK_TEXT); assert.equal(final.status, "completed");
      for (const key of ["runPolicy", "stall", "stopDiagnostics", "childUpstreamErrors", "upstreamError", "diagnosticRevision"]) assert.equal(final[key], undefined, `正常完成不得新增 ${key}`);
      assert.equal(model.requests.length, 1);
    } else if (mode === "contract") {
      const expectedReason = scenario === "hang" ? "stallTimeout" : "upstreamAccessError";
      assert.equal(run.code, scenario === "hang" ? 3 : 4); assert.equal(final.status, "interrupted");
      assert.equal(record.interruptReason, expectedReason); assert.equal(record.stopOutcome, "confirmed");
      assert.equal(record.stopDiagnostics.interruptSent, true); assert(record.stopDiagnostics.turnEndedAt); assert(record.stopDiagnostics.confirmedAt);
      assert.deepEqual(final.stopDiagnostics, persisted.stopDiagnostics);
      const updates = result.events.filter((event) => event.method === "broker/run/updated" && event.params.runId === runId);
      assert(updates.every((event, i) => !i || event.params.diagnosticRevision > updates[i - 1].params.diagnosticRevision));
      if (scenario === "hang") {
        assert.equal(record.stall.verify, "inProgress"); assert.deepEqual(final.stall, persisted.stall);
        const expired = updates.find((event) => event.params.stall?.phase === "verifying")?.params.stall; assert(expired);
        assertHangRequestBeforeExpiry(model.requests, expired.expiredAt);
        exact(run, `[cxb] 已 ${Math.floor(expired.idleMs / 1000)} 秒未观察到进展（无进展期限 20 秒），正在核实；仍无进展将停止本次运行。`);
        exact(run, "[cxb] 无进展核实结束（轮次仍在进行），正在停止本次运行。");
        exact(run, "[cxb] 已因无进展停止本次运行（20 秒未观察到进展）。");
      } else {
        assert.equal(record.upstreamError.category, "authentication"); assert.equal(record.upstreamError.httpStatusCode, 401); assert.equal(record.upstreamError.willRetry, true);
        exact(run, "[cxb] 已按 --upstream-error-policy stop-access 停止本次运行：上游认证失败（HTTP 401）。");
      }
      const waited = await command(["wait", id, "--json", "--timeout", "10"]); assert.equal(waited.code, run.code); assert.equal(parse(waited).interruptReason, expectedReason);
      const shown = await command(["show", id, "--json"]); assert.equal(shown.code, 0); assert.deepEqual(parse(shown).stopDiagnostics, record.stopDiagnostics);
    } else { assert.equal(run.code, 4); assert.equal(record.status, "interrupted"); assert.equal(record.interruptReason, "stop"); }
    assert.deepEqual(model.violations, []); result.passed = true;
  } catch (error) { result.failure = String(error); }
  finally {
    const cleanupAt = Date.now(), errors: string[] = []; connection?.close();
    const cleanup = async (operation: () => Promise<unknown>) => {
      try { await within(Promise.resolve().then(operation), Math.max(1, CLEANUP_MS - (Date.now() - cleanupAt)), "真实第二批清理"); }
      catch (error) { errors.push(String(error)); }
    };
    for (const proc of processes.toReversed()) await cleanup(() => stopManaged(proc));
    if (model) await cleanup(() => model!.close()); if (isolation) await cleanup(() => removeIsolation(isolation!.root));
    result.cleaned = errors.length === 0 && Date.now() - cleanupAt <= CLEANUP_MS;
    if (!result.cleaned) { result.passed = false; result.failure = [result.failure, ...errors, "清理未在 12 秒内完成"].filter(Boolean).join("；"); }
    result.durationMs = Date.now() - startedAt; mkdirSync(REPORT, { recursive: true });
    writeFileSync(join(REPORT, `real-stall-${mode}-${scenario}.json`), JSON.stringify(result, null, 2));
  }
  return result;
}

if (import.meta.main) {
  const mode = process.argv.includes("--smoke") ? "smoke" : "contract";
  const selected = process.argv.find((arg) => arg.startsWith("--scenario="))?.slice("--scenario=".length);
  if (selected && !["hang", "401", "ok"].includes(selected)) throw new Error("场景只允许 hang、401、ok");
  const results = [];
  for (const scenario of selected ? [selected as StallModelScenario] : ["hang", "401", "ok"] as const) results.push(await runStallE2e(scenario, mode));
  console.log(JSON.stringify(results.map(({ scenario, mode, passed, cleaned, durationMs, failure, baseline }) => ({ scenario, mode, passed, cleaned, durationMs, failure, baseline })), null, 2));
  process.exitCode = results.every((result) => result.passed && result.cleaned) ? 0 : 1;
}
