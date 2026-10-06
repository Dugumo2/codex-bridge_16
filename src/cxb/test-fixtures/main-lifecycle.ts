// 在独立进程中替换连接层，避免 mock.module 污染真 broker 联调。
import { mock } from "bun:test";
import { writeSync } from "node:fs";
import { CxbError, EXIT, type BrokerConnection, type ConnectOptions } from "../contracts";
import type { CommandContext } from "../main";

const mode = process.argv[2];
const events: string[] = [];
let signal: AbortSignal | undefined;
let calls = 0;
let context: CommandContext | undefined;
let ready!: () => void;
const started = new Promise<void>((resolve) => { ready = resolve; });
let releaseVersion!: (value: string) => void;
let keepAlive: ReturnType<typeof setInterval> | undefined;
const conn = {
  launched: true,
  handshake: { brokerVersion: "0.1.0", protocolVersion: 1, epoch: 1, userAgent: "测试", server: { kind: "private" } },
  rpc: {},
  async close() {
    if (mode === "slow-cancel") await Bun.sleep(1000);
    events.push(`关闭：${signal?.aborted}`);
    clearInterval(keepAlive);
    // 信号路径最终以 130 结束，到不了文件末尾的汇总输出，这里同步写出事件序列。
    if (mode === "signal-order") writeSync(1, JSON.stringify({ events }) + "\n");
  },
} as BrokerConnection;

mock.module("../broker-conn", () => ({
  resolveBrokerBin: () => "测试可执行文件",
  brokerVersionOf: async () => {
    if (mode !== "version") return "0.1.0";
    ready();
    return new Promise<string>((resolve) => { releaseVersion = resolve; });
  },
  connectBroker: async (opts: ConnectOptions) => {
    calls++;
    signal = opts.signal;
    if (!signal) throw new Error("入口没有传入取消信号");
    events.push("连接开始");
    keepAlive = setInterval(() => {}, 1000);
    if (mode === "cancel" || mode === "late" || mode === "slow-cancel") {
      return new Promise<BrokerConnection>((resolve, reject) => {
        signal!.addEventListener("abort", () => {
          events.push("取消");
          if (mode === "slow-cancel") {
            // 模拟已开始的 launch 用满 10 秒预算，之后还需等待迟到连接关闭。
            setTimeout(() => { events.push("launch 已收束"); resolve(conn); }, 10_000);
          } else if (mode === "late") resolve(conn);
          else {
            clearInterval(keepAlive);
            reject(new CxbError("INTERRUPTED", "测试连接已取消"));
          }
        }, { once: true });
        ready();
      });
    }
    return conn;
  },
}));

const { runCli } = await import("../main");
await runCli(["doctor", "-v", "-d", process.cwd()], {
  doctor: async (ctx) => {
    context = ctx;
    if (mode === "log-fail") {
      const progress = ctx.out.progress;
      ctx.out.progress = (line) => {
        if (line.includes("已拉起本工作区的 broker")) {
          events.push("日志失败");
          throw new CxbError("LOCAL_ERROR", "模拟 verbose 写出失败");
        }
        progress(line);
      };
      await ctx.connect();
    } else if (mode === "signal-order") {
      // 命令的信号收尾比入口慢：入口必须等它结束后才关闭连接（run 在这里发 stop 并等回包）。
      await ctx.connect();
      let finish!: () => void;
      const done = new Promise<void>((resolve) => { finish = resolve; });
      ctx.registerCleanup(async () => { await Bun.sleep(200); events.push("命令收尾"); finish(); });
      setImmediate(() => process.emit("SIGINT", "SIGINT"));
      await done;
    } else if (mode === "reuse") {
      const [first, second] = await Promise.all([ctx.connect(), ctx.connect()]);
      if (first !== second) throw new Error("未复用同一连接");
    } else {
      // 故意不等待连接完成：验证命令提前结束时清理仍取消并收束它。
      void ctx.connect();
      await started;
      if (mode === "version") setImmediate(() => releaseVersion("0.1.0"));
    }
    return EXIT.OK;
  },
});
let afterCleanup: string | undefined;
try { await context!.connect(); }
catch (error) { afterCleanup = (error as CxbError).code; }
process.stdout.write(JSON.stringify({ events, calls, afterCleanup, code: process.exitCode }) + "\n");
