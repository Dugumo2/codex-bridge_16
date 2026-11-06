// 测试清理按进程号回收进程时使用。Windows 会复用进程号，强杀前要确认它还是这次测试拉起的程序，
// 否则可能把恰好占用了这个号的无关程序（比如用户正在用的软件）结束掉。
import { tmpdir } from "node:os";
import { resolve } from "node:path";

/** 这次测试运行的起点：测试拉起的进程都晚于它，比它早启动的程序一定不是测试拉起的。 */
const RUN_STARTED_AT_MS = performance.timeOrigin;
/** 测试用的脚本和 broker 只会放在仓库或临时目录下；正式安装的 cxb 不在这里。 */
const TEST_ROOTS = [resolve(import.meta.dir, "..", ".."), resolve(tmpdir())].map(normalizePath);
/** 多数进程此时正在退出，先等一小会儿，免得每个都启动一次查询（一次 1.4～4 秒）。 */
const EXIT_GRACE_MS = 250;
/** 实测 6 路并行时中位约 3.9 秒，单路偶尔约 5 秒，给足余量。 */
const IDENTITY_QUERY_TIMEOUT_MS = 15_000;

export interface ProcessIdentity { name: string; exePath: string; commandLine: string; startedAtMs: number }
export type ReapOutcome = "exited" | "killed" | "reused";
export interface IdentityOptions {
  readIdentity?: (pid: number) => Promise<ProcessIdentity | null>;
  runStartedAtMs?: number;
  kill?: (pid: number) => void;
}

function normalizePath(path: string): string {
  return path.replaceAll("/", "\\").toLowerCase();
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

function mentionsTestRoot(text: string): boolean {
  const normalized = normalizePath(text);
  return TEST_ROOTS.some((root) => normalized.includes(root));
}

/**
 * 只有同时满足才算这次测试拉起的：启动不早于测试开始；是 bun 或 cxb-broker；
 * broker 的程序文件、bun 运行的脚本在测试目录下（或 bun -e 内联脚本）。
 * 这样用户在测试期间用正式安装的 cxb 拉起的 bun 和 broker 也不会被当成测试进程。
 */
export function isTestProcess(identity: ProcessIdentity, runStartedAtMs = RUN_STARTED_AT_MS): boolean {
  if (identity.startedAtMs < runStartedAtMs) return false;
  const name = identity.name.toLowerCase().replace(/\.exe$/, "");
  if (name === "cxb-broker") {
    const override = process.env.CXB_BROKER_BIN;
    return mentionsTestRoot(identity.exePath) || (!!override && normalizePath(identity.exePath) === normalizePath(resolve(override)));
  }
  if (name === "bun") return mentionsTestRoot(identity.commandLine) || /\s-e\s/.test(identity.commandLine);
  return false;
}

/** 进程已不存在时返回 null；查询本身失败时抛错，调用方不得在身份不明时强杀。 */
export async function readProcessIdentity(pid: number): Promise<ProcessIdentity | null> {
  const script = `[Console]::OutputEncoding = [Text.Encoding]::UTF8; $p = Get-CimInstance Win32_Process -Filter 'ProcessId=${pid}'; if ($p) { @{ name = $p.Name; exePath = $p.ExecutablePath; commandLine = $p.CommandLine; startedAtMs = [DateTimeOffset]::new($p.CreationDate).ToUnixTimeMilliseconds() } | ConvertTo-Json -Compress }`;
  const child = Bun.spawn(["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script], {
    stdin: "ignore", stdout: "pipe", stderr: "ignore", windowsHide: true,
  });
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const output = await Promise.race([
      new Response(child.stdout).text(),
      new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error(`查询进程 ${pid} 的身份超时`)), IDENTITY_QUERY_TIMEOUT_MS); }),
    ]);
    const text = output.replace(/^﻿/, "").trim();
    if (!text) return null;
    const value = JSON.parse(text) as { name?: unknown; exePath?: unknown; commandLine?: unknown; startedAtMs?: unknown };
    if (typeof value.name !== "string" || typeof value.startedAtMs !== "number") throw new Error(`进程 ${pid} 的身份无法解析：${text}`);
    // 没有权限时路径和命令行读出来是空的，测试拉起的进程不会这样，后面按「不是测试进程」处理。
    return {
      name: value.name, startedAtMs: value.startedAtMs,
      exePath: typeof value.exePath === "string" ? value.exePath : "",
      commandLine: typeof value.commandLine === "string" ? value.commandLine : "",
    };
  } finally {
    clearTimeout(timer);
    if (child.exitCode === null) child.kill();
  }
}

/** 这个进程号上已经没有测试进程了：要么没人用，要么已被无关程序占用。 */
export async function processGone(pid: number, options: IdentityOptions = {}): Promise<boolean> {
  if (!alive(pid)) return true;
  if (process.platform !== "win32") return false;
  const identity = await (options.readIdentity ?? readProcessIdentity)(pid);
  if (!identity) return true;
  if (isTestProcess(identity, options.runStartedAtMs)) return false;
  console.warn(`[测试清理] 进程号 ${pid} 现在是 ${identity.name}（启动于 ${new Date(identity.startedAtMs).toISOString()}），不是本次测试的进程，跳过`);
  return true;
}

/** 等测试进程退出；到期时再核实一次身份，进程号被别的程序占用不算没退出。 */
export async function waitForProcessGone(pid: number, ms: number, message: string, options: IdentityOptions = {}): Promise<void> {
  if (await exitedWithin(pid, ms)) return;
  if (!await processGone(pid, options)) throw new Error(message);
}

/**
 * 回收只知道进程号的测试进程。
 * - exited：已经退出；killed：确认是测试进程，已强杀并确认退出；
 * - reused：进程号已被无关程序占用，不动它，打一条提示。
 * 查询失败或强杀后仍存活时抛错。其他平台进程号复用少，产品也只支持 Windows，不做核实。
 */
export async function reapTestProcess(pid: number, waitMs = 5000, options: IdentityOptions = {}): Promise<ReapOutcome> {
  if (await exitedWithin(pid, EXIT_GRACE_MS)) return "exited";
  if (process.platform === "win32") {
    const identity = await (options.readIdentity ?? readProcessIdentity)(pid);
    if (!identity) return "exited";
    if (!isTestProcess(identity, options.runStartedAtMs)) {
      console.warn(`[测试清理] 进程号 ${pid} 现在是 ${identity.name}（启动于 ${new Date(identity.startedAtMs).toISOString()}），不是本次测试的进程，跳过`);
      return "reused";
    }
  }
  try { (options.kill ?? ((target) => process.kill(target, "SIGKILL")))(pid); } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
  }
  await waitForProcessGone(pid, waitMs, `进程 ${pid} 强杀后仍存活`, options);
  return "killed";
}
