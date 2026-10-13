/**
 * 真 broker＋本仓库 cxb CLI＋真实 Codex 0.160.0＋本地脚本模型。
 * --smoke 先验收隔离、真实私有上游、退出码和 ok 输出；合并 A/B 后不带该参数验收诊断契约。
 * PowerShell：$env:CXB_BROKER_BIN='<编译目录>\debug\cxb-broker.exe'; bun src/test-support/upstream-e2e.ts --smoke
 */
import assert from "node:assert/strict";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import net from "node:net";
import { delimiter, join, resolve } from "node:path";
import { createRpcEndpoint, type RpcEndpoint } from "../rpc";
import { resolveWorkspace } from "../cxb/workspace";
import { startUpstreamModel, OK_TEXT, type UpstreamScenario } from "./upstream-model-server";
import { TRACE_ROOT, CLEANUP_MS, codexCommand, createUpstreamIsolation, startManaged, stopManaged, removeIsolation,
  verifyEffectiveConfig, within, type ManagedProcess } from "./upstream-isolation";

type Scenario = "ok" | "401-then-ok" | "429";
interface CliResult { args: string[]; code: number | null; stdout: string; stderr: string }
interface E2eResult { scenario: Scenario; mode: "smoke" | "contract"; passed: boolean; durationMs: number; requests: unknown[];
  cli: CliResult[]; brokerEvents: unknown[]; failure?: string; cleaned: boolean }

async function openRpc(pipe: string, deadline: number) {
  let socket: net.Socket | undefined;
  while (!socket) {
    if (Date.now() >= deadline) throw new Error("E2E 管道连接超过总期限");
    const candidate = net.createConnection(pipe); candidate.on("error", () => {});
    try {
      await within(new Promise<void>((done, reject) => { candidate.once("connect", done); candidate.once("error", reject); }), 500, "管道探测");
      socket = candidate;
    } catch { candidate.destroy(); await Bun.sleep(20); }
  }
  const transport = socket;
  const endpoint = createRpcEndpoint({ label: "UE 端到端", requestTimeout: 5000, send: (data) => { transport.write(data); },
    downReason: () => transport.destroyed ? "测试连接已关闭" : null, overflowReason: "测试协议超限", onOverflow: () => transport.destroy(),
    writeFailedPrefix: "测试协议写入失败", log() {} });
  transport.setEncoding("utf8"); transport.on("data", (chunk: string) => endpoint.feed(chunk));
  transport.on("error", () => endpoint.fail("测试管道错误")); transport.on("close", () => endpoint.fail("测试管道关闭"));
  return { rpc: endpoint, close() { endpoint.markClosed(); transport.destroy(); } };
}

function parseSingleJson(result: CliResult): Record<string, any> {
  const trimmed = result.stdout.trim();
  assert(trimmed.startsWith("{"), `${result.args[0]} stdout 必须为最终单个 JSON 对象`);
  const parsed: unknown = JSON.parse(trimmed);
  assert(parsed !== null && typeof parsed === "object" && !Array.isArray(parsed));
  return parsed as Record<string, any>;
}
function lines(text: string): string[] { return text.split(/\r?\n/).filter(Boolean); }
function exactLine(result: CliResult, expected: string) { assert(lines(result.stderr).includes(expected), `${result.args.join(" ")} stderr 缺少固定行：${expected}`); }
function reason(error: Record<string, any>): string { return String(error.additionalDetails?.trim() || error.message).replace(/\s+/gu, " ").trim(); }
function hasDiagnostic(value: unknown): boolean {
  if (Array.isArray(value)) return value.some(hasDiagnostic);
  if (!value || typeof value !== "object") return false;
  return Object.entries(value).some(([key, item]) => key === "upstreamError" || key === "diagnosticRevision" || key === "error" && item != null || hasDiagnostic(item));
}

export async function runUpstreamE2e(scenario: Scenario, mode: "smoke" | "contract" = "contract"): Promise<E2eResult> {
  const bin = process.env.CXB_BROKER_BIN;
  if (!bin || !existsSync(bin)) throw new Error("E2E 必须用 CXB_BROKER_BIN 显式指定已编译的本路／合并后 broker");
  const started = Date.now(), deadline = started + 75_000;
  const model = await startUpstreamModel(scenario as UpstreamScenario, { failures: 2 });
  const isolation = createUpstreamIsolation(model.baseUrl);
  const cli = resolve(import.meta.dir, "../cxb/main.ts");
  const native = codexCommand([])[0];
  const shimDir = join(isolation.root, "bin"); mkdirSync(shimDir);
  // broker 的固定 codex app-server 命令经此透明包装调用指定版本；不扣留 interrupt。
  writeFileSync(join(shimDir, "codex.cmd"), `@echo off\r\n"${native}" %*\r\nexit /b %errorlevel%\r\n`);
  const pathKey = Object.keys(isolation.env).find((key) => key.toUpperCase() === "PATH");
  const oldPath = pathKey ? isolation.env[pathKey] : ""; if (pathKey) delete isolation.env[pathKey];
  Object.assign(isolation.env, { PATH: `${shimDir}${delimiter}${oldPath ?? ""}`, CXB_BROKER_BIN: bin });
  const workspace = resolveWorkspace(isolation.cwd, isolation.env);
  const processes: ManagedProcess[] = [];
  const result: E2eResult = { scenario, mode, passed: false, durationMs: 0, requests: model.requests, cli: [], brokerEvents: [], cleaned: false };
  let connection: Awaited<ReturnType<typeof openRpc>> | undefined;
  let rpc: RpcEndpoint | undefined;
  const command = async (args: string[]) => {
    const proc = startManaged([process.execPath, cli, ...args, "--dir", isolation.cwd], isolation); processes.push(proc);
    proc.child.stdin.end();
    const code = await within(proc.closed, Math.max(1, deadline - Date.now()), `CLI ${args[0]}`);
    const output = { args, code, ...proc.output() }; result.cli.push(output); return output;
  };
  try {
    const version = startManaged(codexCommand(["--version"]), isolation); processes.push(version);
    await within(version.closed, 5000, "E2E Codex 版本"); assert.equal(version.output().stdout.trim(), "codex-cli 0.160.0");
    const broker = startManaged([bin, "serve", "--endpoint", workspace.endpoint, "--cwd", isolation.cwd, "--idle-timeout", "60000"], isolation);
    processes.push(broker);
    connection = await openRpc(workspace.pipePath, Math.min(deadline, Date.now() + 10_000)); rpc = connection.rpc;
    rpc.onAny((method, params) => { result.brokerEvents.push({ at: Date.now(), method, params }); });
    const handshake = await rpc.request<any>("initialize", { clientInfo: { name: "ue-e2e", version: "1" },
      capabilities: { experimentalApi: true, cxb: { protocolVersion: 1 } } });
    rpc.notify("initialized"); assert.equal(handshake.server?.kind, "private");
    verifyEffectiveConfig(await rpc.request("config/read", { cwd: isolation.cwd, includeLayers: false }), model.baseUrl);
    const run = await command(["run", "只输出本地脚本结果。", "--json", "--approval", "never", "--sandbox", "danger-full-access", "--timeout", "30"]);
    const final = parseSingleJson(run);
    const code = scenario === "429" ? 7 : 0;
    assert.equal(run.code, code); assert.equal(final.status, scenario === "429" ? "failed" : "completed");
    assert.equal(model.requests.length, scenario === "401-then-ok" ? 3 : 1);
    if (scenario !== "429") assert.equal(final.result, OK_TEXT);
    const id = final.thread?.id; assert.equal(typeof id, "string");
    // 持久化排队不保证通知先于写盘；先用真实 watch 核对当前记录，再等待 show 的落盘可读性。
    const watch = await rpc.request<any>("broker/threads/watch", { threadId: id });
    assert.equal(watch.run?.runId, final.runId);
    assert.equal(watch.run?.status, final.status);
    const shown = await command(["show", id, "--json"]); assert.equal(shown.code, 0);
    const showJson = parseSingleJson(shown);
    const listed = await command(["ls", "--json", "--all"]); assert.equal(listed.code, 0);
    const listJson = parseSingleJson(listed);
    const item = listJson.items.find((entry: any) => entry.lastRunId === final.runId); assert(item, "ls 未按 lastRunId 精确找到本运行");
    const waited = await command(["wait", id, "--json", "--timeout", "15"]); assert.equal(waited.code, code);
    const waitJson = parseSingleJson(waited);
    assert.equal(waitJson.runId, final.runId); assert.equal(waitJson.status, final.status);
    if (scenario === "ok") {
      for (const value of [final, showJson, item, waitJson]) assert.equal(hasDiagnostic(value), false, "无错误场景不得新增诊断／错误字段");
      for (const output of result.cli) assert.equal(/上游(?:认证失败|限流|报错)|最近上游报错|失败原因/u.test(output.stderr), false);
    } else if (mode === "contract") {
      const error = final.upstreamError; assert(error, "合并 A/B 后最终 JSON 必须含 upstreamError");
      assert.equal(error.category, scenario === "429" ? "rateLimit" : "authentication");
      assert.equal(error.httpStatusCode, scenario === "429" ? 429 : 401);
      assert.equal(error.count, scenario === "429" ? 1 : 2);
      assert.equal(error.willRetry, scenario === "429" ? false : true);
      for (const value of [watch.run, showJson, item, waitJson]) assert.deepEqual(value.upstreamError, error);
      exactLine(run, scenario === "429"
        ? "[cxb] 上游限流（HTTP 429），上游已报告终止错误，等待轮次结束确认。"
        : "[cxb] 上游认证失败（HTTP 401），Codex 正在重试；已观察到 1 次。");
      exactLine(run, `[cxb] 原因：${reason(error)}`);
      const label = scenario === "429" ? "上游限流（HTTP 429）" : "上游认证失败（HTTP 401）";
      exactLine(waited, `[cxb] 最近上游报错：${label}，共观察到 ${error.count} 次（运行已结束）。`);
      exactLine(waited, `[cxb] 原因：${reason(error)}`);
      const showText = await command(["show", id]); assert.equal(showText.code, 0);
      assert((showText.stdout + showText.stderr).includes(`最近上游报错：${label}；${error.count} 次；首次 ${new Date(error.firstSeenAt).toISOString()}，最近 ${new Date(error.lastSeenAt).toISOString()}；运行已结束`));
      assert((showText.stdout + showText.stderr).includes(`原因：${reason(error)}`));
      const listText = await command(["ls", "--all"]); assert.equal(listText.code, 0);
      assert(listText.stdout.includes(`${final.status}（上游 ${error.httpStatusCode}×${error.count}）`));
      if (scenario === "429") {
        assert(final.error && waitJson.error, "429 最终失败 JSON 必须带真实 error");
        assert.deepEqual(waitJson.error, final.error);
        const failureLine = `[cxb] 失败原因：${final.error.message.replace(/\s+/gu, " ").trim()}（HTTP 429）`;
        exactLine(run, failureLine); exactLine(waited, failureLine);
      } else { assert.equal(final.error, undefined); assert.equal(waitJson.error, undefined); }
    }
    assert.deepEqual(model.violations, []);
    result.passed = true;
  } catch (error) { result.failure = String(error); }
  finally {
    const cleanupStart = Date.now(); connection?.close();
    const errors: string[] = [];
    const cleanup = async (work: () => Promise<unknown>) => {
      try { await within(Promise.resolve().then(work), Math.max(1, CLEANUP_MS - (Date.now() - cleanupStart)), "E2E 资源清理"); }
      catch (error) { errors.push(String(error)); }
    };
    // broker 放在 CLI 前结束时，Windows Job 将真实私有上游整树一并收回。
    for (const proc of [...processes].reverse()) await cleanup(() => stopManaged(proc));
    await cleanup(() => model.close()); await cleanup(() => removeIsolation(isolation.root));
    if (Date.now() - cleanupStart > CLEANUP_MS) errors.push("E2E 清理超过 12 秒期限");
    result.cleaned = errors.length === 0; result.durationMs = Date.now() - started;
    if (errors.length) { result.passed = false; result.failure = [result.failure, ...errors].filter(Boolean).join("；"); }
    mkdirSync(TRACE_ROOT, { recursive: true });
    writeFileSync(join(TRACE_ROOT, `e2e-${mode}-${scenario}.json`), JSON.stringify(result, null, 2));
  }
  return result;
}

if (import.meta.main) {
  const mode = process.argv.includes("--smoke") ? "smoke" : "contract";
  const selected = process.argv.find((arg) => arg.startsWith("--scenario="))?.slice("--scenario=".length) as Scenario | undefined;
  const results = [];
  for (const scenario of selected ? [selected] : ["401-then-ok", "429", "ok"] as const) results.push(await runUpstreamE2e(scenario, mode));
  console.log(JSON.stringify(results.map(({ scenario, passed, requests, durationMs, failure, cleaned }) => ({ scenario, passed, requestCount: requests.length, durationMs, failure, cleaned })), null, 2));
  process.exitCode = results.every((result) => result.passed) ? 0 : 1;
}
