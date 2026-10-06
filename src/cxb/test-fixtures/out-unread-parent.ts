// 必须由真正的 Node 运行：Bun 的 child_process 兼容层在 pause 后仍可能预读整个管道。
import { spawn } from "node:child_process";
import { existsSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { CxbError } from "../contracts.ts";

async function deadline<T>(work: Promise<T>, ms: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new CxbError("LOCAL_ERROR", "未读管道测试超过期限")), ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

const registration = process.argv[4];
const readyFile = process.argv[5];
const child = spawn(process.argv[2], [
  fileURLToPath(new URL("./out-main.ts", import.meta.url)), process.argv[3], readyFile,
], {
  stdio: ["ignore", "pipe", "pipe"], windowsHide: true, env: process.env,
  // 故障注入用例需要后代独立存活，避免平台在包装进程退出时顺带回收它。
  detached: process.argv[3] === "blocked-orphan",
});
const children = new Map<number, typeof child>();
if (child.pid !== undefined) children.set(child.pid, child);
const errors: Buffer[] = [];
let spawnFailed = false;
let hasExited = false;
child.stdout.pause();
child.stderr.on("data", (data: Buffer) => errors.push(data));
const exited = new Promise<number | null>((resolve) => {
  child.once("exit", (code) => { hasExited = true; resolve(code); });
  child.once("error", () => { hasExited = true; spawnFailed = true; resolve(null); });
});
const closed = new Promise<void>((resolve) => child.once("close", () => resolve()));
let forced = false;
let ready = false;
let code: number | null = null;
const cleanupErrors: string[] = [];
try {
  // 创建子进程后立即登记到外层持有的文件，包装进程被强杀也不会丢失 PID。
  if (child.pid !== undefined) writeFileSync(registration, JSON.stringify({ pid: child.pid }), { flag: "wx" });
  const startupDeadline = performance.now() + 10_000;
  while (!existsSync(readyFile)) {
    if (hasExited || performance.now() >= startupDeadline) {
      throw new CxbError("LOCAL_ERROR", "被测子进程未就绪");
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 20));
  }
  ready = true;
  if (process.argv[3] === "blocked-orphan") process.exit(77);
  // 就绪后才计时；5 秒远大于夹具两次各 100 ms 的 flush 期限，另留调度余量。
  code = await deadline(exited, 5_000);
} catch {
  forced = true;
} finally {
  for (const [pid, proc] of children) {
    try {
      if (forced && !hasExited) {
        proc.kill("SIGKILL");
        await deadline(exited, 4_000);
      }
      // 等进程退出后才关闭 stdout 读端，不能通过提前关管道来解除写入阻塞。
      proc.stdout.destroy();
      await deadline(closed, 4_000);
      try {
        process.kill(pid, 0);
        cleanupErrors.push("子进程仍存活");
      } catch (err) {
        if ((err as NodeJS.ErrnoException).code !== "ESRCH") throw err;
      }
    } catch {
      cleanupErrors.push("子进程清理失败");
    }
  }
}
process.stdout.write(`${JSON.stringify({ code, ready, forced, spawnFailed, cleanupErrors, stderr: Buffer.concat(errors).toString() })}\n`);
process.exitCode = forced || spawnFailed || cleanupErrors.length > 0 ? 1 : 0;
