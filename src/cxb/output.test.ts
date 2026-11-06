import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test";
import * as fs from "node:fs";
import { spawn } from "node:child_process";
import { closeSync, mkdtempSync, openSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Writable } from "node:stream";
import { fileURLToPath } from "node:url";
import { CxbError, ERROR_CODES, EXIT, type ErrorCode, type ExitCode } from "./contracts";
import { createOutput, registerSignalHandlers, runMain, toErrorJson } from "./output";
import { NOTICE, PRIVATE_ERROR_MESSAGE, PROGRESS, RESULT } from "./test-fixtures/out-data";
import { processGone, reapTestProcess, waitForProcessGone } from "../test-support/process-identity";

const fixture = fileURLToPath(new URL("./test-fixtures/out-main.ts", import.meta.url));
const CHILD_TIMEOUT_MS = 8_000;
// 包装进程最多启动 10 秒、验证 5 秒、回收两步各 4 秒；外层与钩子须覆盖完整预算。
const UNREAD_TIMEOUT_MS = 30_000;
const TEST_TIMEOUT_MS = 40_000;
let temporaryDir: string;
let savedExitCode: typeof process.exitCode;

interface TrackedProcess {
  pid: number;
  done?: Promise<unknown>;
  kill?(): void;
}
const children = new Map<number, TrackedProcess>();
const registrations = new Set<string>();

function childEnv(): NodeJS.ProcessEnv {
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) =>
    !/^(?:CXB_SERVER|CXB_SERVER_SOCKET)$/i.test(key)));
  return { ...env, CXB_SERVER: "private" };
}

async function deadline<T>(work: Promise<T>, ms = CHILD_TIMEOUT_MS): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new CxbError("LOCAL_ERROR", "测试等待超过期限")), ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function isAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ESRCH") return false;
    throw err;
  }
}

function track(pid: number, done: Promise<unknown>, kill: () => void): void {
  children.set(pid, { pid, done, kill });
}

async function waitUntil(predicate: () => boolean, ms: number): Promise<void> {
  const until = performance.now() + ms;
  while (!predicate()) {
    if (performance.now() >= until) throw new CxbError("LOCAL_ERROR", "测试状态等待超过期限");
    await new Promise<void>((resolve) => setTimeout(resolve, 20));
  }
}

function readRegistration(file: string): number | undefined {
  if (!fs.existsSync(file)) return undefined;
  const { pid } = JSON.parse(readFileSync(file, "utf8")) as { pid: unknown };
  if (!Number.isSafeInteger(pid) || (pid as number) <= 0 || pid === process.pid) {
    throw new CxbError("LOCAL_ERROR", "子进程登记 PID 无效");
  }
  if (!children.has(pid as number)) {
    children.set(pid as number, { pid: pid as number });
  }
  return pid as number;
}

async function reapTrackedProcesses(): Promise<{ killed: number[]; errors: unknown[] }> {
  const killed: number[] = [];
  const errors: unknown[] = [];
  const processed = new Set<number>();
  const reap = async (child: TrackedProcess) => {
    processed.add(child.pid);
    try {
      // 没有 kill 的是只从登记文件读到进程号的后代，强杀前要核实身份。
      if (!child.kill) {
        const outcome = await reapTestProcess(child.pid);
        if (outcome === "killed") killed.push(child.pid);
        if (outcome === "reused") return;
      } else if (!await processGone(child.pid)) {
        killed.push(child.pid);
        child.kill();
      }
    } catch (err) { errors.push(err); }
    if (child.done) {
      try { await deadline(child.done, 4_000); } catch (err) { errors.push(err); }
    }
    // 不能把 kill() 成功或包装进程退出当成后代已经退出。
    try { await waitForProcessGone(child.pid, 4_000, `进程 ${child.pid} 没有退出`); } catch (err) { errors.push(err); }
  };
  await Promise.all([...children.values()].map(reap));
  // 先收束包装进程，再重读持久登记，覆盖其退出前最后一刻创建的后代。
  for (const file of registrations) {
    try { readRegistration(file); } catch (err) { errors.push(err); }
  }
  await Promise.all([...children.values()].filter((child) => !processed.has(child.pid)).map(reap));
  return { killed, errors };
}

beforeEach(() => {
  // Bun 在 Windows 下给 exitCode 赋 undefined 不会可靠地清掉已有非零状态。
  savedExitCode = process.exitCode ?? 0;
  temporaryDir = mkdtempSync(join(tmpdir(), "cxb-out-"));
});

afterEach(async () => {
  try {
    const result = await reapTrackedProcesses();
    expect(result.errors).toEqual([]);
    expect(result.killed).toEqual([]);
  } finally {
    children.clear();
    registrations.clear();
    process.exitCode = savedExitCode;
    rmSync(temporaryDir, { recursive: true, force: true, maxRetries: 3, retryDelay: 30 });
  }
}, TEST_TIMEOUT_MS);

interface Captured {
  stdout: Buffer;
  stderr: Buffer;
  code: number | null;
  signal?: string | null;
  descendantPid?: number;
}

async function runChild(
  launcher: "bun" | "node",
  mode: string,
  opts: { file?: string; signal?: "SIGINT" | "SIGTERM"; unread?: boolean } = {},
): Promise<Captured> {
  const fd = opts.file ? openSync(opts.file, "w") : undefined;
  try {
    if (launcher === "bun") {
      const child = Bun.spawn([process.execPath, fixture, mode], {
        stdin: "ignore", stdout: fd ?? "pipe", stderr: "pipe", env: childEnv(), windowsHide: true,
      });
      track(child.pid, child.exited, () => { child.kill("SIGKILL"); });
      const stdout = child.stdout && typeof child.stdout !== "number"
        ? new Response(child.stdout).arrayBuffer().then((data) => Buffer.from(data))
        : Promise.resolve(Buffer.alloc(0));
      const stderr = new Response(child.stderr).arrayBuffer().then((data) => Buffer.from(data));
      const [out, err, code] = await deadline(Promise.all([stdout, stderr, child.exited]));
      return { stdout: out, stderr: err, code };
    }
    const registration = opts.unread ? join(temporaryDir, `后代-${registrations.size}.json`) : undefined;
    if (registration) registrations.add(registration);
    const args = registration
      ? [fileURLToPath(new URL("./test-fixtures/out-unread-parent.ts", import.meta.url)), process.execPath, mode, registration, `${registration}.ready`]
      : [fixture, mode];
    const child = spawn(opts.unread ? "node" : process.execPath, args, {
      stdio: ["ignore", fd ?? "pipe", "pipe"], windowsHide: true, env: childEnv(),
    });
    const out: Buffer[] = [];
    const err: Buffer[] = [];
    let sent = false;
    let spawnError: Error | undefined;
    child.stdout?.on("data", (data: Buffer) => out.push(data));
    child.stderr?.on("data", (data: Buffer) => {
      err.push(data);
      if (opts.signal && !sent && Buffer.concat(err).includes(Buffer.from("[cxb] 等待信号\n"))) {
        sent = true;
        child.kill(opts.signal);
      }
    });
    child.on("error", (error) => { spawnError = error; });
    const done = new Promise<Captured>((resolve) => {
      child.once("close", (code, signal) => resolve({
        stdout: Buffer.concat(out), stderr: Buffer.concat(err), code, signal,
      }));
    });
    if (child.pid !== undefined) track(child.pid, done, () => { child.kill("SIGKILL"); });
    let descendantPid: number | undefined;
    const registered = registration ? waitUntil(() => {
      try { descendantPid = readRegistration(registration); } catch (err) {
        // 文件刚创建但尚未写完时重试；结束后的钩子仍会严格检查登记内容。
        if (err instanceof SyntaxError) return false;
        throw err;
      }
      return descendantPid !== undefined;
    }, 10_000) : Promise.resolve();
    const [result] = await deadline(Promise.all([done, registered]), opts.unread ? UNREAD_TIMEOUT_MS : CHILD_TIMEOUT_MS);
    if (spawnError) throw new CxbError("LOCAL_ERROR", "测试子进程启动失败", { cause: spawnError });
    if (opts.signal) expect(sent).toBe(true);
    return { ...result, descendantPid };
  } finally {
    if (fd !== undefined) closeSync(fd);
  }
}

function memoryStream() {
  const chunks: Buffer[] = [];
  const stream = new Writable({
    write(chunk, _encoding, callback) { chunks.push(Buffer.from(chunk)); callback(); },
  });
  return { stream, text: () => Buffer.concat(chunks).toString("utf8") };
}

describe("输出与错误约定", () => {
  for (const [stdoutTTY, stderrTTY] of [[true, true], [true, false], [false, true], [false, false]]) {
    test(`标准流选择：stdout ${stdoutTTY ? "终端" : "重定向"}、stderr ${stderrTTY ? "终端" : "重定向"}`, async () => {
      const terminalOut = memoryStream();
      const terminalErr = memoryStream();
      const redirectedOut = memoryStream();
      const redirectedErr = memoryStream();
      const outDescriptor = Object.getOwnPropertyDescriptor(process.stdout, "isTTY");
      const errDescriptor = Object.getOwnPropertyDescriptor(process.stderr, "isTTY");
      const factory = spyOn(fs, "createWriteStream").mockImplementation((_path, options) =>
        ((typeof options === "object" && options?.fd === 1) ? redirectedOut.stream : redirectedErr.stream) as fs.WriteStream);
      const stdoutWrite = spyOn(process.stdout, "write").mockImplementation(terminalOut.stream.write.bind(terminalOut.stream));
      const stderrWrite = spyOn(process.stderr, "write").mockImplementation(terminalErr.stream.write.bind(terminalErr.stream));
      let out: ReturnType<typeof createOutput> | undefined;
      let streamOptions: unknown[] = [];
      try {
        Object.defineProperty(process.stdout, "isTTY", { configurable: true, value: stdoutTTY });
        Object.defineProperty(process.stderr, "isTTY", { configurable: true, value: stderrTTY });
        out = createOutput({ command: "run", json: false, verbose: false });
        out.result("终端结果🙂");
        out.notice("终端诊断🙂");
        streamOptions = factory.mock.calls.map((call) => call[1]);
      } finally {
        stdoutWrite.mockRestore();
        stderrWrite.mockRestore();
        factory.mockRestore();
        if (outDescriptor) Object.defineProperty(process.stdout, "isTTY", outDescriptor);
        else Reflect.deleteProperty(process.stdout, "isTTY");
        if (errDescriptor) Object.defineProperty(process.stderr, "isTTY", errDescriptor);
        else Reflect.deleteProperty(process.stderr, "isTTY");
      }
      await deadline(out!.flush());
      // 只核对保留标准描述符的设计约束，不约束路径参数、调用顺序或其他选项。
      const redirectedFds = [...(!stdoutTTY ? [1] : []), ...(!stderrTTY ? [2] : [])];
      expect(streamOptions).toHaveLength(redirectedFds.length);
      for (const fd of redirectedFds) {
        expect(streamOptions).toContainEqual(expect.objectContaining({ fd, autoClose: false }));
      }
      expect(terminalOut.text()).toBe(stdoutTTY ? "终端结果🙂" : "");
      expect(terminalErr.text()).toBe(stderrTTY ? "终端诊断🙂\n" : "");
      expect(redirectedOut.text()).toBe(stdoutTTY ? "" : "终端结果🙂");
      expect(redirectedErr.text()).toBe(stderrTTY ? "" : "终端诊断🙂\n");
    });
  }

  test("每个错误枚举沿用公共退出码和完整 JSON 字段", () => {
    for (const [code, exitCode] of Object.entries(ERROR_CODES) as [ErrorCode, ExitCode][]) {
      const err = new CxbError(code, "错误说明", { retryable: true, id: "线程甲" });
      expect(err.exitCode).toBe(exitCode);
      expect(toErrorJson(err, "run")).toEqual({
        ok: false,
        error: { code, message: "错误说明", retryable: true, command: "run", id: "线程甲" },
      });
    }
  });

  test("未知异常不泄漏内部消息和堆栈，缺省字段为 null", () => {
    for (const value of [new Error("敏感的内部消息"), "内部字符串", null, undefined, { detail: "内部信息" }]) {
      expect(toErrorJson(value, null)).toEqual({
        ok: false,
        error: { code: "LOCAL_ERROR", message: "发生本地错误", retryable: false, command: null, id: null },
      });
    }
  });

  test("普通模式保留结果原文，过程受 verbose 控制，提醒总是输出", async () => {
    const stdout = memoryStream();
    const stderr = memoryStream();
    const out = createOutput({ command: null, json: false, verbose: false, stdout: stdout.stream, stderr: stderr.stream });
    out.result("  中文🙂\n");
    out.resultJson({ ignored: true });
    out.progress("隐藏过程");
    out.notice("[cxb] 提醒：需要留意\n");
    out.error(new CxbError("USAGE", "参数不正确"));
    await deadline(out.finish(EXIT.USAGE_OR_LOCAL_ERROR));
    expect(stdout.text()).toBe("  中文🙂\n");
    expect(stderr.text()).toBe("[cxb] 提醒：需要留意\ncxb: error: 参数不正确\n");
    expect(process.exitCode).toBe(1);
    expect(stdout.stream.writableEnded).toBe(false);
  });

  for (const json of [false, true]) {
    for (const verbose of [false, true]) {
      test(`${json ? "JSON" : "文本"} ${verbose ? "详细" : "普通"}模式：多行敏感消息和伪装帧均不回显`, async () => {
        const stdout = memoryStream();
        const stderr = memoryStream();
        const out = createOutput({ command: "run", json, verbose, stdout: stdout.stream, stderr: stderr.stream });
        const err = new TypeError(PRIVATE_ERROR_MESSAGE);
        err.name = "Cookie: x";
        out.error(err);
        await deadline(out.finish(EXIT.USAGE_OR_LOCAL_ERROR));
        const text = stderr.text();
        expect(stdout.text()).toBe("");
        expect(text).not.toContain("token=abc123");
        expect(text).not.toContain("Cookie: x");
        expect(text).not.toContain("伪造.ts");
        const [first, ...rest] = text.split("\n");
        if (json) expect(JSON.parse(first)).toEqual(toErrorJson(err, "run"));
        else expect(first).toBe("cxb: error: 发生本地错误");
        if (verbose) {
          expect(rest[0]).toBe("[cxb] 异常类型：TypeError");
          expect(rest.join("\n")).toMatch(/at output\.test\.ts:\d+:\d+/);
        } else expect(rest).toEqual([""]);
      });
    }
  }

  test("无法序列化或不是对象的 JSON 结果统一抛本地错误", () => {
    const out = createOutput({ command: "run", json: true, verbose: false });
    const cyclic: { self?: unknown } = {};
    cyclic.self = cyclic;
    for (const obj of [cyclic, { value: 1n }, [], { toJSON: () => undefined }, { toJSON: () => "文本" }]) {
      expect(() => out.resultJson(obj)).toThrow(CxbError);
    }
  });

  test("写回调完成之前不设置退出码，即便 write 未触发背压", async () => {
    let release: (() => void) | undefined;
    const stdout = new Writable({ write(_chunk, _encoding, callback) { release = callback; } });
    const stderr = memoryStream();
    const out = createOutput({ command: "run", json: false, verbose: false, stdout, stderr: stderr.stream });
    process.exitCode = 42;
    out.result("待写出的中文🙂");
    out.notice("过程仍可写出");
    const finishing = out.finish(EXIT.THREAD_HELD);
    try {
      await deadline(new Promise<void>((resolve) => setImmediate(resolve)));
      expect(process.exitCode).toBe(42);
      expect(release).toBeDefined();
    } finally {
      release?.();
      await deadline(finishing);
    }
    expect(process.exitCode).toBe(8);
    expect(stderr.text()).toBe("过程仍可写出\n");
  });

  test("写失败不成为未处理异常，另一路仍然写完", async () => {
    const stdout = new Writable({
      write(_chunk, _encoding, callback) { callback(new Error("模拟管道断开")); },
    });
    const stderr = memoryStream();
    const out = createOutput({ command: "run", json: false, verbose: false, stdout, stderr: stderr.stream });
    out.result("失败的输出");
    out.notice("保留诊断");
    await expect(deadline(out.finish(EXIT.OK))).rejects.toBeInstanceOf(CxbError);
    expect(process.exitCode).toBe(1);
    expect(stderr.text()).toBe("保留诊断\n");
  });

  test("写回调不返回时 flush 有期限且保留已有非零退出码", async () => {
    const stdout = new Writable({ write() {} });
    const out = createOutput({ command: "run", json: false, verbose: false, stdout, flushTimeoutMs: 30 });
    out.result("无法排空");
    try {
      await expect(deadline(out.finish(EXIT.THREAD_HELD))).rejects.toMatchObject({ code: "LOCAL_ERROR" });
      expect(process.exitCode).toBe(8);
    } finally {
      stdout.destroy();
    }
  });

  test("同步写异常也归一化为 CxbError", async () => {
    const stdout = memoryStream().stream;
    stdout.write = () => { throw new Error("模拟同步失败"); };
    const out = createOutput({ command: null, json: false, verbose: false, stdout });
    expect(() => out.result("结果")).toThrow(CxbError);
    await expect(deadline(out.flush())).rejects.toBeInstanceOf(CxbError);
  });

  test("正常入口会注销信号监听，清理可取消注册且只运行一次", async () => {
    const stdout = memoryStream();
    const stderr = memoryStream();
    const before = [process.listenerCount("SIGINT"), process.listenerCount("SIGTERM")];
    const called: string[] = [];
    await deadline(runMain(async (out, registerCleanup) => {
      registerCleanup(() => { called.push("保留"); out.notice("已清理"); });
      const cancel = registerCleanup(() => { called.push("已撤销"); });
      cancel();
      out.result("最终结果");
      return EXIT.IDLE;
    }, { command: "wait", json: false, verbose: false, stdout: stdout.stream, stderr: stderr.stream }));
    expect(called).toEqual(["保留"]);
    expect(process.exitCode).toBe(10);
    expect(stdout.text()).toBe("最终结果");
    expect(stderr.text()).toBe("已清理\n");
    expect([process.listenerCount("SIGINT"), process.listenerCount("SIGTERM")]).toEqual(before);
  });

  test("某个清理回调失败时其余回调照常执行，整体报清理失败", async () => {
    const out = createOutput({ command: null, json: false, verbose: false });
    const signals = registerSignalHandlers(out);
    const events: string[] = [];
    signals.registerCleanup(() => { events.push("入口"); });
    signals.registerCleanup(() => { throw new Error("命令清理失败"); });
    try {
      await expect(deadline(signals.cleanup())).rejects.toBeInstanceOf(CxbError);
      expect(events).toEqual(["入口"]);
    } finally { signals.dispose(); }
  });

  test("显式注册的清理幂等，清理后不能再注册", async () => {
    const out = createOutput({ command: null, json: false, verbose: false });
    const signals = registerSignalHandlers(out);
    let count = 0;
    signals.registerCleanup(() => { count++; });
    try {
      await deadline(Promise.all([signals.cleanup(), signals.cleanup()]));
      expect(count).toBe(1);
      expect(() => signals.registerCleanup(() => {})).toThrow(CxbError);
    } finally { signals.dispose(); }
  });
});

describe("真实子进程的输出保证", () => {
  for (const [mode, code] of [["blocked", 1], ["blocked-held", 8]] as const) {
    test(`stdout 无人读取：${mode} 在期限内失败退出且保留非零码`, async () => {
      const result = await runChild("node", mode, { unread: true });
      expect(JSON.parse(result.stdout.toString())).toEqual({
        code, ready: true, forced: false, spawnFailed: false, cleanupErrors: [],
        stderr: "cxb: error: 标准输出 写出超时\n",
      });
      expect(result.code).toBe(0);
      expect(result.stderr.length).toBe(0);
      expect(result.descendantPid).toBeDefined();
      expect(children.has(result.descendantPid!)).toBe(true);
      expect(await processGone(result.descendantPid!)).toBe(true);
    }, TEST_TIMEOUT_MS);
  }

  test("包装进程异常退出后，外层按登记 PID 强杀后代并确认退出", async () => {
    const result = await runChild("node", "blocked-orphan", { unread: true });
    expect(result.code).toBe(77);
    expect(result.descendantPid).toBeDefined();
    const pid = result.descendantPid!;
    expect(children.has(pid)).toBe(true);
    expect(isAlive(pid)).toBe(true);
    const reaped = await reapTrackedProcesses();
    expect(reaped.errors).toEqual([]);
    expect(reaped.killed).toEqual([pid]);
    expect(await processGone(pid)).toBe(true);
  }, TEST_TIMEOUT_MS);

  for (const [mode, code] of [["injected-blocked", 1], ["injected-blocked-held", 8]] as const) {
    test(`注入流写出超时：${mode} 返回失败且不终止宿主或关闭注入流`, async () => {
      const result = await runChild("node", mode);
      expect(result.code).toBe(code);
      expect(JSON.parse(result.stdout.toString())).toEqual({ returned: true, destroyed: false });
      expect(result.stderr.toString()).toBe("cxb: error: 标准输出 写出超时\n");
    }, TEST_TIMEOUT_MS);
  }

  for (const launcher of ["bun", "node"] as const) {
    test(`${launcher} 启动：大段中文和 emoji 结果、过程完整且自然退出`, async () => {
      expect(Buffer.byteLength(RESULT)).toBeGreaterThan(300 * 1024);
      const result = await runChild(launcher, "large");
      expect(result.code).toBe(0);
      expect(result.stdout).toEqual(Buffer.from(RESULT));
      expect(result.stderr).toEqual(Buffer.from(PROGRESS + NOTICE));
    }, TEST_TIMEOUT_MS);

    test(`${launcher} 启动：THREAD_HELD 错误仍排空大结果并退出 8`, async () => {
      const result = await runChild(launcher, "held");
      expect(result.code).toBe(8);
      expect(result.stdout).toEqual(Buffer.from(RESULT));
      expect(result.stderr).toEqual(Buffer.from(PROGRESS + NOTICE + "cxb: error: 线程正被占用\n"));
    }, TEST_TIMEOUT_MS);

    test(`${launcher} 启动：JSON 成功是一个对象且过程只在 stderr`, async () => {
      const result = await runChild(launcher, "json-ok");
      expect(result.code).toBe(0);
      expect(result.stdout).toEqual(Buffer.from(`${JSON.stringify({ ok: true, result: RESULT })}\n`));
      expect(JSON.parse(result.stdout.toString())).toEqual({ ok: true, result: RESULT });
      expect(result.stderr.toString()).toBe("[cxb] JSON 过程\n");
    }, TEST_TIMEOUT_MS);

    test(`${launcher} 启动：JSON 失败只在 stderr 写标准错误对象`, async () => {
      const result = await runChild(launcher, "json-error");
      expect(result.code).toBe(8);
      expect(result.stdout.length).toBe(0);
      expect(JSON.parse(result.stderr.toString())).toEqual({
        ok: false,
        error: { code: "THREAD_HELD", message: "线程正被占用", retryable: true, command: "run", id: "线程甲" },
      });
      expect(result.stderr.toString().endsWith("\n")).toBe(true);
    }, TEST_TIMEOUT_MS);

    test(`${launcher} 启动：stdout 重定向到文件仍逐字节完整`, async () => {
      const file = join(temporaryDir, "结果🙂.txt");
      const result = await runChild(launcher, "large", { file });
      expect(result.code).toBe(0);
      expect(readFileSync(file)).toEqual(Buffer.from(RESULT));
      expect(result.stderr).toEqual(Buffer.from(PROGRESS + NOTICE));
    }, TEST_TIMEOUT_MS);
  }

  test("默认模式不输出过程，正常返回的非零码保持不变", async () => {
    const quiet = await runChild("bun", "quiet");
    expect(quiet.code).toBe(0);
    expect(quiet.stdout).toEqual(Buffer.from(RESULT));
    expect(quiet.stderr.toString()).toBe(NOTICE);
    const idle = await runChild("node", "idle");
    expect(idle.code).toBe(10);
    expect(idle.stdout).toEqual(Buffer.from(RESULT));
  }, TEST_TIMEOUT_MS);

  for (const mode of ["unknown", "unknown-verbose", "unknown-json", "unknown-json-verbose"]) {
    test(`真实进程未知异常：${mode} 返回 1 且合成敏感串不回显`, async () => {
      const result = await runChild("node", mode);
      const text = result.stderr.toString();
      expect(result.code).toBe(1);
      expect(result.stdout.length).toBe(0);
      expect(text).not.toContain("token=abc123");
      expect(text).not.toContain("Cookie: x");
      expect(text).not.toContain("伪造.ts");
      const [first, ...rest] = text.split("\n");
      if (mode.includes("json")) expect(JSON.parse(first)).toEqual(toErrorJson(new Error(), "run"));
      else expect(first).toBe("cxb: error: 发生本地错误");
      if (mode.endsWith("verbose")) {
        expect(rest[0]).toBe("[cxb] 异常类型：TypeError");
        expect(rest.join("\n")).toMatch(/at out-main\.ts:\d+:\d+/);
      } else expect(rest).toEqual([""]);
    }, TEST_TIMEOUT_MS);
  }

  for (const [mode, code, message] of [
    ["cleanup", 0, ""],
    ["cleanup-fail", 1, "cxb: error: 清理资源失败\n"],
    ["cleanup-timeout", 1, "cxb: error: 清理资源超时\n"],
  ] as const) {
    test(`正常入口：${mode} 清理后排空输出并自然退出`, async () => {
      const result = await runChild("bun", mode);
      expect(result.code).toBe(code);
      expect(result.stdout).toEqual(Buffer.from(RESULT));
      expect(result.stderr.toString()).toBe(PROGRESS + NOTICE + "[cxb] 已关闭资源\n" + message);
    }, TEST_TIMEOUT_MS);
  }

  for (const [mode, signal, code, message] of [
    ["signal-int", "SIGINT", 130, ""],
    ["signal-term", "SIGTERM", 143, ""],
    ["signal-int-fail", "SIGINT", 130, "cxb: error: 清理资源失败\n"],
    ["signal-term-fail", "SIGTERM", 143, "cxb: error: 清理资源失败\n"],
    ["signal-int-timeout", "SIGINT", 130, "cxb: error: 清理资源超时\n"],
  ] as const) {
    test(`进程内模拟信号（真实控制台 Ctrl-C 未验证）：${mode} 清理一次并排空大输出`, async () => {
      const result = await runChild("node", mode);
      expect(result.code).toBe(code);
      expect(result.stdout).toEqual(Buffer.from(RESULT));
      expect(result.stderr.toString()).toBe(`[cxb] 等待信号\n[cxb] 清理：${signal}\n${message}`);
    }, TEST_TIMEOUT_MS);
  }

  for (const signal of ["SIGINT", "SIGTERM"] as const) {
    test(`child.kill(${signal}) 终止行为（不代表真实控制台 Ctrl-C 清理）`, async () => {
      const result = await runChild("node", "wait-signal", { signal });
      const cleaned = result.stderr.toString().includes(`[cxb] 清理：${signal}`);
      if (process.platform === "win32") {
        console.info(`Windows ${signal} 实测：退出码=${result.code}，终止信号=${result.signal}，清理回调=${cleaned}`);
        expect(result.code !== null || result.signal !== null).toBe(true);
      } else {
        expect(cleaned).toBe(true);
        expect(result.code).toBe(signal === "SIGINT" ? 130 : 143);
      }
    }, TEST_TIMEOUT_MS);
  }
});
