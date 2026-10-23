import { afterEach, beforeEach, expect, test } from "bun:test";
import { existsSync, mkdirSync, readFileSync } from "node:fs";
import { delimiter, join } from "node:path";
import { BROKER_BIN, writeMockCodexScript } from "../test-support/broker-harness";
import { brokerVersionOf, connectBroker } from "./broker-conn";
import type { BrokerConnection } from "./contracts";
import { N5Harness } from "./test-fixtures/n5a-support";
import { Processes, bounded } from "./test-fixtures/main-test-support";
import { resolveWorkspace } from "./workspace";

let h: N5Harness;
let processes: Processes;
let env: NodeJS.ProcessEnv;
let timer: ReturnType<typeof setInterval>;
let conn: BrokerConnection | undefined;
let registrationErrors: unknown[];
beforeEach(() => {
  h = new N5Harness(); processes = new Processes(); conn = undefined; registrationErrors = [];
  const mock = join(h.dir, "mock"); mkdirSync(mock);
  writeMockCodexScript(mock, readFileSync(join(import.meta.dir, "test-fixtures", "n5a-codex.ts"), "utf8"));
  env = { ...process.env, PATH: `${mock}${delimiter}${process.env.PATH}`, N5A_PIDS: join(h.dir, "pids") };
  timer = setInterval(() => { try { processes.readRegistry(env.N5A_PIDS!); } catch (error) { registrationErrors.push(error); } }, 20);
});
afterEach(async () => {
  const errors: unknown[] = [...registrationErrors];
  try { if (conn) await bounded(conn.rpc.request("broker/shutdown", {}, { timeoutMs: 3000 }), 4000, "关闭测试 broker 超时"); } catch (error) { errors.push(error); }
  try { await conn?.close(); } catch (error) { errors.push(error); }
  clearInterval(timer);
  try { processes.readRegistry(env.N5A_PIDS!); await processes.cleanup(); expect(processes.allExited()).toBe(true); } catch (error) { errors.push(error); }
  try { await h.cleanup(); } catch (error) { errors.push(error); }
  if (errors.length) throw new AggregateError(errors, "doctor 端到端清理失败");
}, 30_000);

async function cli(...args: string[]) {
  return processes.run(process.execPath, [join(import.meta.dir, "main.ts"), ...args, "-d", h.root], { cwd: h.root, env, limitMs: 20_000, onStderr: (line) => processes.log(line) });
}

test("真实 broker：未拉起诊断正常；普通命令拉起后只读诊断通过并复用实例", async () => {
  const cold = await cli("doctor", "--json", "-v");
  expect(cold.code).toBe(0);
  const coldOutput = JSON.parse(cold.stdout);
  expect(coldOutput.checks.find((c: any) => c.name === "broker")).toMatchObject({ status: "pass", launched: false });
  expect(coldOutput.checks.find((c: any) => c.name === "broker").detail).toContain("未运行");
  expect(coldOutput.checks.find((c: any) => c.name === "account").status).toBe("warn");
  const workspace = resolveWorkspace(h.root, env);
  expect(existsSync(join(workspace.workspaceDir, "broker.json"))).toBe(false);
  const started = await cli("ls", "--json", "-v");
  expect(started.code).toBe(0);
  expect(started.stderr).toContain("候选进程已拉起");
  conn = await connectBroker({ workspace, brokerBin: BROKER_BIN, expectedBrokerVersion: await brokerVersionOf(BROKER_BIN), probe: true, timeoutMs: 5000 });
  const warm = await cli("doctor", "--json", "-v");
  expect(warm.code).toBe(0);
  const warmOutput = JSON.parse(warm.stdout);
  expect(warmOutput.checks.find((c: any) => c.name === "broker")).toMatchObject({ status: "pass", launched: false, epoch: conn.handshake.epoch });
  expect(warmOutput.checks.find((c: any) => c.name === "models")).toMatchObject({ status: "pass", count: 1, defaults: ["n5-model"] });
  expect(warmOutput.checks.find((c: any) => c.name === "account").detail).toContain("n5@example.test");
  // 没有关闭请求，旧模拟服务器缺少 config/read 形状不应导致审批风险误报。
  expect(warmOutput.checks.find((c: any) => c.name === "mcpApprovalRisk")).toMatchObject({ status: "pass", mcpConfigVerified: false, mcpEffectiveDisabled: [] });
  expect(warmOutput.checks.find((c: any) => c.name === "mcpApprovalRisk").detail).toContain("预计有效关闭名单：无");
  expect(warmOutput.summary).toEqual({ pass: 11, warn: 0, fail: 0 });
  expect(warm.stderr).not.toContain("候选进程已拉起");
}, 55_000);

for (const [signal, code] of [["SIGINT", 130], ["SIGTERM", 143]] as const) test(`doctor ${signal} 经真实入口清理并退出 ${code}`, async () => {
  const marker = join(h.dir, "cancelled");
  const result = await processes.run(process.execPath, [join(import.meta.dir, "test-fixtures", "n5a-signal.ts"), signal, marker], { cwd: h.root, env, limitMs: 10_000 });
  expect(result.code).toBe(code);
  expect(readFileSync(marker, "utf8")).toBe("已取消并回收");
});
