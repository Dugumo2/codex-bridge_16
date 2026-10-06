// 真入口冒烟：只注入公开 doctor 处理器，不新增 CLI 命令。
import childProcess from "node:child_process";
import { appendFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import { EXIT, type BrokerConnection } from "../contracts";

const registry = process.env.CXB_TEST_PID_REGISTRY!;
const record = (pid: number | undefined) => {
  if (pid !== undefined) appendFileSync(registry, `${pid}\n`);
};
const execute = childProcess.execFile;
childProcess.execFile = ((...args: Parameters<typeof execute>) => {
  const child = execute(...args);
  record(child.pid);
  return child;
}) as typeof execute;
const executeSync = childProcess.spawnSync;
childProcess.spawnSync = ((...args: Parameters<typeof executeSync>) => {
  const child = executeSync(...args);
  if (child.pid > 0) record(child.pid);
  return child;
}) as typeof executeSync;

const { runCli } = await import("../main");
let connection: BrokerConnection | undefined;
let closed = false;
await runCli(["doctor", "--json", "-v", "-d", process.cwd()], {
  doctor: async (ctx) => {
    if (ctx.parsed.command !== "doctor") throw new Error("命令未经过解析");
    if (!existsSync(join(ctx.workspace.dataRoot, "migrated-from-cxb.json"))) throw new Error("迁移未完成");
    const progress = ctx.out.progress;
    ctx.out.progress = (line) => {
      const match = /候选进程已拉起：pid=(\d+)/.exec(line);
      if (match) record(Number(match[1]));
      progress(line);
    };
    connection = await ctx.connect();
    const close = connection.close;
    connection.close = async () => { await close(); closed = true; };
    const health = await connection.rpc.request("broker/health", {}, { timeoutMs: 5000 });
    ctx.out.resultJson({ ok: true, command: ctx.parsed.command, key: ctx.workspace.key, launched: connection.launched, health });
    return EXIT.OK;
  },
});
// runCli 返回时清理和 finish 都必须完成；连接不能继续发送请求。
if (!closed || process.exitCode !== EXIT.OK) throw new Error("入口未完成关闭连接与 finish");
let rejected = false;
try { await connection!.rpc.request("broker/health", {}, { timeoutMs: 5000 }); }
catch { rejected = true; }
if (!rejected) throw new Error("清理后的连接仍然可用");
process.stderr.write("入口清理与 finish 已完成\n");
