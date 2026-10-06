// 独立进程客户端：验证失败/取消后的自然退出以及 Windows 对端实际断开。
import childProcess from "node:child_process";
import { existsSync } from "node:fs";
import { connectBroker } from "../broker-conn";
import { CxbError, type ConnectOptions } from "../contracts";

const mode = process.argv[2];
const input = JSON.parse(process.argv[3]) as ConnectOptions;
const controller = new AbortController();
const emit = (value: unknown) => process.stdout.write(JSON.stringify(value) + "\n");
const execute = childProcess.execFile;
childProcess.execFile = ((...args: Parameters<typeof execute>) => {
  const child = execute(...args);
  if (child.pid) emit({ process: child.pid });
  if (mode === "取消拉起") child.once("spawn", () => controller.abort());
  return child;
}) as typeof execute;

let cancelTimer: ReturnType<typeof setTimeout> | undefined;
let attempts = 0;
const onInput = (data: Buffer) => {
  if (data.toString().includes("取消")) controller.abort();
};
process.stdin.on("data", onInput);
try {
  const connection = await connectBroker({
    ...input,
    env: process.env,
    signal: controller.signal,
    log(line) {
      emit({ log: line });
      // 测试专用有界屏障：错开两个 CLI 的重连，不以机器运行速度决定是否覆盖竞态。
      if (mode === "错开重连" && line.startsWith("连接 broker：") && ++attempts === 2) {
        emit({ log: "重连已暂停" });
        const deadline = performance.now() + 10_000;
        while (!existsSync(process.argv[4])) {
          if (performance.now() >= deadline) throw new Error("等待测试放行重连超时");
          Bun.sleepSync(10);
        }
      }
      if (mode === "取消退避" && line.startsWith("等待 ") && cancelTimer === undefined) {
        cancelTimer = setTimeout(() => controller.abort(), 10);
      }
    },
  });
  emit({ result: "已连接", launched: connection.launched });
  await connection.close();
} catch (error) {
  if (!(error instanceof CxbError)) throw error;
  emit({ result: error.code, retryable: error.retryable, exitCode: error.exitCode, message: error.message });
} finally {
  clearTimeout(cancelTimer);
  process.stdin.off("data", onInput);
  process.stdin.destroy();
}
