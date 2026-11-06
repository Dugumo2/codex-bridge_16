import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve, sep } from "node:path";
import type { Subprocess } from "bun";
import { reapTestProcess } from "../../test-support/process-identity";

// 各组进程并行回收：直接子进程最多 5+5 秒，后代最多 3+5 秒（核实身份另加最多 5 秒），目录重试 5 秒；
// 工作区用例还可能回收一个 SUBST 映射（5 秒），另留 7 秒调度余量，合计 40 秒。
export const CLEANUP_HOOK_TIMEOUT_MS = 40_000;

/** 清除大小写形式的两套服务选择变量，再明确隔离到私有模拟服务。 */
export function privateTestEnv(source: NodeJS.ProcessEnv = process.env): NodeJS.ProcessEnv {
  const excluded = new Set(["cxb_server", "cxb_server_socket", "cxb_server", "cxb_server_socket"]);
  return {
    ...Object.fromEntries(Object.entries(source).filter(([key]) => !excluded.has(key.toLowerCase()))),
    CXB_SERVER: "private",
  };
}

export async function bounded<T>(promise: Promise<T>, timeoutMs: number, message: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error(message)), timeoutMs); }),
    ]);
  } finally { clearTimeout(timer); }
}

export async function eventually(condition: () => boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() >= deadline) throw new Error("等待条件成立超时");
    await Bun.sleep(20);
  }
}

export function isAlive(pid: number): boolean {
  try { process.kill(pid, 0); return true; }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    throw error;
  }
}

export class ProcessTracker {
  readonly children = new Map<number, Subprocess>();
  readonly descendants = new Set<number>();

  add<T extends Subprocess>(child: T): T {
    this.children.set(child.pid, child);
    return child;
  }

  /** 以子进程句柄的退出状态为准：PID 退出后可能马上被别的进程占用，按 PID 判活会误报。 */
  async exited(child: Subprocess, timeoutMs = 5000): Promise<number> {
    return bounded(child.exited, timeoutMs, `进程 ${child.pid} 未按时退出`);
  }

  async cleanup(): Promise<void> {
    const leaked: number[] = [];
    const errors: unknown[] = [];
    await Promise.all([...this.children].map(async ([pid, child]) => {
      try {
        if (child.exitCode === null) { leaked.push(pid); child.kill(); }
        await this.exited(child);
      } catch (error) { errors.push(error); }
    }));
    await Promise.all([...this.descendants].map(async (pid) => {
      try { await eventually(() => !isAlive(pid), 3000); }
      catch {
        try {
          if (await reapTestProcess(pid) !== "reused") leaked.push(pid);
        } catch (error) { errors.push(error); }
      }
    }));
    this.children.clear();
    this.descendants.clear();
    if (leaked.length) errors.push(new Error(`测试遗留进程已强制结束：${leaked.join("、")}`));
    if (errors.length) throw new AggregateError(errors, "测试进程清理失败");
  }
}

export function makeTemp(prefix: string): string {
  return mkdtempSync(join(resolve(tmpdir()), prefix));
}

export async function removeTemp(path: string): Promise<void> {
  const absolute = resolve(path);
  const allowed = resolve(tmpdir()) + sep;
  if (!absolute.toLowerCase().startsWith(allowed.toLowerCase())) throw new Error("拒绝清理测试临时目录以外的路径");
  const deadline = Date.now() + 5000;
  for (;;) {
    try { rmSync(absolute, { recursive: true, force: true }); return; }
    catch (error) {
      if (Date.now() >= deadline) throw error;
      await Bun.sleep(50);
    }
  }
}

/** 每项清理都执行，避免目录清理错误掩盖进程清理错误。 */
export async function cleanupTestResources(processes: ProcessTracker, temporary: string): Promise<void> {
  const errors: unknown[] = [];
  try { await processes.cleanup(); } catch (error) { errors.push(error); }
  try { await removeTemp(temporary); } catch (error) { errors.push(error); }
  if (errors.length) throw new AggregateError(errors, "测试资源清理失败");
}
