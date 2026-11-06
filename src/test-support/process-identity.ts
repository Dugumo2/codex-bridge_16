// 测试清理按进程号回收进程时使用。Windows 会复用进程号，强杀前要确认它还是这次测试拉起的程序，
// 否则可能把恰好占用了这个号的无关程序（比如用户正在用的软件）结束掉。
import { basename } from "node:path";

/** 这次测试运行的起点：测试拉起的进程都晚于它，比它早启动的程序一定不是测试拉起的。 */
const RUN_STARTED_AT_MS = performance.timeOrigin;
/** 测试会按进程号回收的只有这些程序；有句柄的进程不走这里。 */
const TEST_PROGRAMS = new Set(["bun", "cxb-broker", basename(process.execPath, ".exe").toLowerCase()]);
/** 多数进程此时正在退出，先等一小会儿，免得每个都启动一次查询（一次约 0.8 秒）。 */
const EXIT_GRACE_MS = 250;
const IDENTITY_QUERY_TIMEOUT_MS = 5000;

export interface ProcessIdentity { name: string; startedAtMs: number }
export type ReapOutcome = "exited" | "killed" | "reused";
export interface ReapOptions {
  readIdentity?: (pid: number) => Promise<ProcessIdentity | null>;
  runStartedAtMs?: number;
}

function alive(pid: number): boolean {
  try { process.kill(pid, 0); return true; } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    throw error;
  }
}

async function exitedWithin(pid: number, ms: number): Promise<boolean> {
  const deadline = performance.now() + ms;
  while (alive(pid)) {
    if (performance.now() >= deadline) return false;
    await Bun.sleep(25);
  }
  return true;
}

export function isTestProcess(identity: ProcessIdentity, runStartedAtMs = RUN_STARTED_AT_MS): boolean {
  return TEST_PROGRAMS.has(identity.name.toLowerCase()) && identity.startedAtMs >= runStartedAtMs;
}

/** 进程已不存在时返回 null；查询本身失败时抛错，调用方不得在身份不明时强杀。 */
export async function readProcessIdentity(pid: number): Promise<ProcessIdentity | null> {
  const script = `$p = Get-Process -Id ${pid} -ErrorAction SilentlyContinue; if ($p) { $p.ProcessName; [DateTimeOffset]::new($p.StartTime).ToUnixTimeMilliseconds() }`;
  const child = Bun.spawn(["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script], {
    stdin: "ignore", stdout: "pipe", stderr: "ignore", windowsHide: true,
  });
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const output = await Promise.race([
      new Response(child.stdout).text(),
      new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error(`查询进程 ${pid} 的身份超时`)), IDENTITY_QUERY_TIMEOUT_MS); }),
    ]);
    const [name, startedAt] = output.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
    if (!name) return null;
    // 读不到启动时间说明没有权限访问，测试拉起的进程不会这样，按「比测试早」处理。
    if (!startedAt) return { name, startedAtMs: 0 };
    const startedAtMs = Number(startedAt);
    if (!Number.isFinite(startedAtMs)) throw new Error(`进程 ${pid} 的启动时间无法解析：${startedAt}`);
    return { name, startedAtMs };
  } finally {
    clearTimeout(timer);
    if (child.exitCode === null) child.kill();
  }
}

/**
 * 回收只知道进程号的测试进程。
 * - exited：已经退出；killed：确认是测试进程，已强杀并确认退出；
 * - reused：进程号已被无关程序占用，不动它，打一条提示。
 * 查询失败或强杀后仍存活时抛错。其他平台进程号复用少，产品也只支持 Windows，不做核实。
 */
export async function reapTestProcess(pid: number, waitMs = 5000, options: ReapOptions = {}): Promise<ReapOutcome> {
  if (await exitedWithin(pid, EXIT_GRACE_MS)) return "exited";
  if (process.platform === "win32") {
    const identity = await (options.readIdentity ?? readProcessIdentity)(pid);
    if (!identity) return "exited";
    if (!isTestProcess(identity, options.runStartedAtMs)) {
      console.warn(`[测试清理] 进程号 ${pid} 现在是 ${identity.name}（启动于 ${new Date(identity.startedAtMs).toISOString()}），不是本次测试的进程，跳过`);
      return "reused";
    }
  }
  try { process.kill(pid, "SIGKILL"); } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
  }
  if (!await exitedWithin(pid, waitMs)) throw new Error(`进程 ${pid} 强杀后仍存活`);
  return "killed";
}
