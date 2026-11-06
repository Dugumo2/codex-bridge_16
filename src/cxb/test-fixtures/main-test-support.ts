// 戊路测试专用：截止等待、子进程登记、严格清理与环境隔离。
import { tmpdir } from "node:os";
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, readFileSync, rmSync } from "node:fs";
import { resolve, sep } from "node:path";
import { reapTestProcess } from "../../test-support/process-identity";

export async function bounded<T>(work: Promise<T>, ms: number, message: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([work, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(message)), ms);
    })]);
  } finally { clearTimeout(timer); }
}

export function alive(pid: number): boolean {
  try { process.kill(pid, 0); return true; }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    throw error;
  }
}

export async function until(check: () => boolean, ms: number, message: string): Promise<void> {
  const end = Date.now() + ms;
  while (!check()) {
    if (Date.now() >= end) throw new Error(message);
    await Bun.sleep(25);
  }
}

export function privateEnv(source: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const names = new Set(["CXB_SERVER", "CXB_SERVER_SOCKET"]);
  return {
    ...Object.fromEntries(Object.entries(source).filter(([key]) => !names.has(key.toUpperCase()))),
    CXB_SERVER: "private",
  };
}

export interface Ran { code: number | null; stdout: string; stderr: string }

export class Processes {
  readonly pids = new Set<number>();
  private readonly children = new Map<number, ChildProcess>();

  add(pid: number | undefined): void {
    if (pid !== undefined && Number.isSafeInteger(pid) && pid > 0) this.pids.add(pid);
  }

  log(line: string): void {
    const match = /候选进程已拉起：pid=(\d+)/.exec(line);
    if (match) this.add(Number(match[1]));
  }

  readRegistry(path: string): void {
    if (!existsSync(path)) return;
    for (const line of readFileSync(path, "utf8").split(/\r?\n/).filter(Boolean)) {
      this.add(Number(line));
      if (!Number.isSafeInteger(Number(line)) || Number(line) <= 0) throw new Error("进程登记文件损坏");
    }
  }

  async run(file: string, args: string[], opts: {
    cwd: string; env: NodeJS.ProcessEnv; limitMs?: number; onStderr?: (chunk: string) => void;
  }): Promise<Ran> {
    const child = spawn(file, args, {
      cwd: opts.cwd, env: opts.env, windowsHide: true, stdio: ["ignore", "pipe", "pipe"],
    });
    this.add(child.pid);
    if (child.pid !== undefined) this.children.set(child.pid, child);
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    let failure: Error | undefined;
    child.on("error", (error) => { failure = error; });
    child.stdout!.on("data", (chunk: Buffer) => { stdout.push(chunk); });
    child.stderr!.on("data", (chunk: Buffer) => {
      stderr.push(chunk);
      try { opts.onStderr?.(chunk.toString("utf8")); }
      catch (error) { failure = error instanceof Error ? error : new Error("错误输出观察失败"); }
    });
    child.stdout!.on("error", (error) => { failure = error; });
    child.stderr!.on("error", (error) => { failure = error; });
    const exited = new Promise<void>((done) => {
      child.once("exit", () => done());
      child.once("error", () => { if (child.pid === undefined) done(); });
    });
    // close 在进程退出且两路管道 EOF 后才到达，不能只给 exited 加期限。
    const completed = new Promise<Ran>((done) => child.once("close", (code) => done({
      code, stdout: Buffer.concat(stdout).toString("utf8"), stderr: Buffer.concat(stderr).toString("utf8"),
    })));
    try {
      const result = await bounded(completed, opts.limitMs ?? 20_000, "子进程退出或输出 EOF 超过期限");
      if (failure) throw failure;
      if (child.pid !== undefined) await until(() => !alive(child.pid!), 5000, "子进程退出后 PID 仍存活");
      // 已按句柄收到 close 并确认退出，不把旧 PID 留到长时间 stop 之后再检查。
      if (child.pid !== undefined) { this.pids.delete(child.pid); this.children.delete(child.pid); }
      return result;
    } catch (error) {
      const errors: unknown[] = [error];
      try {
        if (child.pid !== undefined && alive(child.pid)) child.kill("SIGKILL");
        await bounded(exited, 5000, "强制结束后未收到退出事件");
        if (child.pid !== undefined) await until(() => !alive(child.pid!), 5000, "强制结束后 PID 仍存活");
      } catch (cleanupError) { errors.push(cleanupError); }
      finally {
        // 后代仍持有写端时，本地读端也必须主动关闭。
        child.stdout!.destroy();
        child.stderr!.destroy();
      }
      try { await bounded(completed, 5000, "关闭输出流后仍未收束"); }
      catch (cleanupError) { errors.push(cleanupError); }
      throw new AggregateError(errors, "子进程执行失败");
    }
  }

  async cleanup(): Promise<void> {
    const results = await Promise.allSettled([...this.pids].map(async (pid) => {
      try {
        await until(() => !alive(pid), 10_000, `进程 ${pid} 未自然退出`);
        // Windows 会复用 PID；已确认结束的登记不能影响后续 allExited 核验。
        this.pids.delete(pid);
      }
      catch (error) {
        const errors = [error];
        try {
          // 进程号已被无关程序占用：登记的进程其实早已退出，不算遗留。
          if (await reapTestProcess(pid) === "reused") { this.pids.delete(pid); return; }
        } catch (killError) { errors.push(killError); }
        throw new AggregateError(errors, `遗留进程 ${pid} 已尝试强制回收`);
      } finally {
        this.children.get(pid)?.stdout?.destroy();
        this.children.get(pid)?.stderr?.destroy();
      }
    }));
    const errors = results.filter((r) => r.status === "rejected").map((r) => r.reason);
    if (errors.length) throw new AggregateError(errors, "进程清理失败");
  }

  allExited(): boolean { return [...this.pids].every((pid) => !alive(pid)); }
}

export async function removeTemp(path: string): Promise<void> {
  const target = resolve(path);
  const allowed = resolve(tmpdir()) + sep;
  if (!target.toLowerCase().startsWith(allowed.toLowerCase())) throw new Error("拒绝删除测试临时目录以外的路径");
  const end = Date.now() + 5000;
  for (;;) {
    try {
      rmSync(target, { recursive: true, force: true });
      if (existsSync(target)) throw new Error("临时目录删除后仍存在");
      return;
    } catch (error) {
      if (Date.now() >= end) throw error;
      await Bun.sleep(50);
    }
  }
}
