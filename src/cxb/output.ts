// src/cxb/output.ts — stdout/stderr 纪律、JSON 错误对象与退出前 flush（命令面规格 2.4、6.3 节）。
//
// stdout 只放结果（文本或一个 JSON 对象），过程、提醒、错误一律 stderr；正常路径等两路写回调都完成后
// 才设置 process.exitCode，不调用 process.exit()。信号或自有标准流写出失败时，在有界清理与 flush 后强制退出。
// 自有标准流接管道或文件时使用异步文件流，避免 Windows/Bun 的同步标准流写入阻塞事件循环，使期限失效；
// 交互式终端仍用 process.stdout/stderr（见 ownStream）。
// 注入流的失败只返回并设置失败退出码，不终止宿主进程，也不关闭注入流。
// result 只在非 JSON 模式写出、resultJson 只在 JSON 模式写出，命令可以两个都调，由模式决定实际输出哪一个。

import { createWriteStream } from "node:fs";
import { CxbError, EXIT, type CxbErrorJson, type ExitCode } from "./contracts";

const DEFAULT_TIMEOUT_MS = 5_000;

/** 只在输出层内部携带流的归属，不改变公共错误对象或退出码。 */
class OutputError extends CxbError {
  constructor(message: string, readonly ownedStream: boolean, cause?: unknown) {
    super("LOCAL_ERROR", message, { cause });
  }
}

export interface OutputOptions {
  command: string | null;
  json: boolean;
  verbose: boolean;
  stdout?: NodeJS.WritableStream;
  stderr?: NodeJS.WritableStream;
  /** 整次 flush 的期限；允许嵌入方按自己的输出设备调整。 */
  flushTimeoutMs?: number;
}

export interface Output {
  result(text: string): void;
  resultJson(obj: object): void;
  progress(line: string): void;
  notice(line: string): void;
  error(err: unknown): void;
  flush(): Promise<void>;
  finish(code: ExitCode): Promise<void>;
}

function asCxbError(err: unknown): CxbError {
  if (err instanceof CxbError) return err;
  // 未知异常可能把提示词、凭据或路径内容放进 message；默认只给稳定的摘要。
  return new CxbError("LOCAL_ERROR", "发生本地错误", { cause: err });
}

function safeErrorDetails(err: Error): string {
  // name 可被用户数据覆盖，只允许输出已知的异常类型名。
  const types = [TypeError, RangeError, ReferenceError, SyntaxError, URIError, EvalError, AggregateError];
  const type = types.find((constructor) => err instanceof constructor)?.name ?? "Error";
  const stack = err.stack ?? "";
  const header = `${err.name}: ${err.message}`;
  // message 可能有多行，甚至伪装成 at 帧；移除整个标题，而非仅移除第一行。
  const body = stack.startsWith(header) ? stack.slice(header.length) : stack.split("\n").slice(1).join("\n");
  const frames: string[] = [];
  for (const line of body.split("\n")) {
    if (!/^\s+at\s/.test(line)) continue;
    // 只保留源码基名和行列，不输出函数名、目录、URL 查询参数或其他任意载荷。
    const location = line.match(/(?:[\\/]|\bat\s)([\p{L}\p{N}_.-]+\.[cm]?[jt]sx?:\d+:\d+)\)?\s*$/u)?.[1];
    if (location) frames.push(`    at ${location}`);
  }
  return [`[cxb] 异常类型：${type}`, ...frames].join("\n");
}

export function toErrorJson(err: unknown, command: string | null): CxbErrorJson {
  const error = asCxbError(err);
  return {
    ok: false,
    error: {
      code: error.code,
      message: error.message,
      retryable: error.retryable,
      command,
      id: error.id,
    },
  };
}

function validateTimeout(timeoutMs: number): void {
  if (!Number.isInteger(timeoutMs) || timeoutMs <= 0 || timeoutMs > 2_147_483_647) {
    throw new CxbError("LOCAL_ERROR", "输出与清理期限必须是有效的正整数毫秒数");
  }
}

async function withDeadline<T>(work: Promise<T>, timeoutMs: number, message: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new CxbError("LOCAL_ERROR", message)), timeoutMs);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

/** write 的返回值只表示背压；必须等写回调，不能把 true 当成已经写出。 */
function createWriter(stream: NodeJS.WritableStream, name: string, ownedStream: boolean) {
  let pending = 0;
  let failure: CxbError | undefined;
  let listening = false;
  const waiters = new Set<() => void>();
  const wake = () => {
    for (const resolve of waiters) resolve();
    waiters.clear();
  };
  const fail = (cause: unknown) => {
    failure ??= new OutputError(`${name} 写出失败`, ownedStream, cause);
    wake();
  };
  const onClose = () => {
    if (pending > 0) fail(new CxbError("LOCAL_ERROR", `${name} 在写出完成前关闭`));
    detach();
  };
  const attach = () => {
    if (listening) return;
    listening = true;
    stream.on("error", fail);
    stream.on("close", onClose);
  };
  const detach = () => {
    stream.removeListener("error", fail);
    stream.removeListener("close", onClose);
    listening = false;
  };

  return {
    write(text: string): void {
      if (failure) throw failure;
      attach();
      pending++;
      let completed = false;
      const complete = (err?: Error | null) => {
        if (completed) return;
        completed = true;
        pending--;
        if (err) fail(err);
        if (pending === 0) {
          wake();
          setImmediate(() => { if (pending === 0) detach(); });
        }
      };
      try {
        stream.write(text, "utf8", complete);
      } catch (err) {
        complete();
        fail(err);
        throw failure;
      }
    },
    async flush(timeoutMs: number): Promise<void> {
      let waiter: (() => void) | undefined;
      try {
        const waiting = pending === 0 || failure ? Promise.resolve() : new Promise<void>((resolve) => {
          waiter = resolve;
          waiters.add(resolve);
        });
        await withDeadline(waiting, timeoutMs, `${name} 写出超时`);
        // Writable 的 error 事件可能在写回调之后才派发，保留监听到下一轮。
        await new Promise<void>((resolve) => setImmediate(resolve));
        if (failure) throw failure;
      } catch (err) {
        throw err instanceof OutputError ? err : new OutputError(asCxbError(err).message, ownedStream, err);
      } finally {
        if (waiter) waiters.delete(waiter);
        // 尚未回调的流仍可能派发 error；等关闭后再移除监听，避免未处理异常。
        if (pending === 0) detach();
      }
    },
  };
}

/**
 * 自有标准流：管道与文件用按描述符写的异步文件流（不会被不读的下游卡住事件循环；fd 已给出时不打开路径，
 * autoClose:false 保留进程的标准描述符）。交互式终端仍用 process.stdout/stderr：Windows 控制台要经 libuv
 * 转成宽字符写出，直接写原始字节会按控制台代码页解释，中文变成乱码；终端也不存在「下游不读」的问题。
 */
function ownStream(fd: 1 | 2, std: NodeJS.WriteStream): NodeJS.WritableStream {
  return std.isTTY ? std : createWriteStream("", { fd, autoClose: false });
}

export function createOutput(opts: OutputOptions): Output {
  const timeoutMs = opts.flushTimeoutMs ?? DEFAULT_TIMEOUT_MS;
  validateTimeout(timeoutMs);
  const stdout = createWriter(opts.stdout ?? ownStream(1, process.stdout), "标准输出", opts.stdout === undefined);
  const stderr = createWriter(opts.stderr ?? ownStream(2, process.stderr), "标准错误输出", opts.stderr === undefined);
  const line = (text: string) => text.endsWith("\n") ? text : `${text}\n`;
  const flush = async () => {
    // 一路失败也要等另一路写完，尤其要保证 stderr 的诊断能送达。
    const results = await Promise.allSettled([stdout.flush(timeoutMs), stderr.flush(timeoutMs)]);
    // 任一自有流失败都必须带出终止标记，不能被另一路注入流的错误遮蔽。
    for (const result of results) {
      if (result.status === "rejected" && result.reason instanceof OutputError && result.reason.ownedStream) {
        throw result.reason;
      }
    }
    for (const result of results) {
      if (result.status === "rejected") throw asCxbError(result.reason);
    }
  };
  return {
    result(text) {
      if (!opts.json) stdout.write(text);
    },
    resultJson(obj) {
      if (!opts.json) return;
      let serialized: string | undefined;
      try {
        serialized = JSON.stringify(obj);
        if (serialized === undefined || !serialized.startsWith("{")) {
          throw new CxbError("LOCAL_ERROR", "结果必须能序列化为 JSON 对象");
        }
      } catch (err) {
        throw new CxbError("LOCAL_ERROR", "结果无法序列化为 JSON 对象", { cause: err });
      }
      stdout.write(`${serialized}\n`);
    },
    progress(text) {
      if (opts.verbose) stderr.write(line(text));
    },
    notice(text) {
      stderr.write(line(text));
    },
    error(err) {
      const normalized = asCxbError(err);
      stderr.write(opts.json
        ? `${JSON.stringify(toErrorJson(normalized, opts.command))}\n`
        : `cxb: error: ${normalized.message}\n`);
      if (opts.verbose && !(err instanceof CxbError) && err instanceof Error) {
        stderr.write(line(safeErrorDetails(err)));
      }
    },
    flush,
    async finish(code) {
      try {
        await flush();
        process.exitCode = code;
      } catch (err) {
        process.exitCode = code === EXIT.OK ? EXIT.USAGE_OR_LOCAL_ERROR : code;
        throw asCxbError(err);
      }
    },
  };
}

export type Cleanup = (signal?: "SIGINT" | "SIGTERM") => void | Promise<void>;
export type RegisterCleanup = (cleanup: Cleanup) => () => void;

/** 注册的回调也由 runMain 在正常完成时执行；回调负责关闭自己持有的连接和文件流。 */
export function registerSignalHandlers(out: Output, opts: { cleanupTimeoutMs?: number } = {}) {
  const timeoutMs = opts.cleanupTimeoutMs ?? DEFAULT_TIMEOUT_MS;
  validateTimeout(timeoutMs);
  const callbacks = new Set<Cleanup>();
  let cleanupPromise: Promise<void> | undefined;
  let shutdownPromise: Promise<void> | undefined;
  const cleanup = (signal?: "SIGINT" | "SIGTERM"): Promise<void> => {
    cleanupPromise ??= withDeadline((async () => {
      const results = await Promise.allSettled([...callbacks].map((callback) =>
        Promise.resolve().then(() => callback(signal))));
      callbacks.clear();
      if (results.some((result) => result.status === "rejected")) {
        throw new CxbError("LOCAL_ERROR", "清理资源失败");
      }
    })(), timeoutMs, "清理资源超时");
    return cleanupPromise;
  };
  const report = (err: unknown) => {
    try { out.error(err); } catch { /* 错误输出本身不可写时保留原退出码。 */ }
  };
  const onSignal = (signal: "SIGINT" | "SIGTERM") => {
    if (shutdownPromise) return;
    shutdownPromise = (async () => {
      const code = signal === "SIGINT" ? EXIT.SIGINT : EXIT.SIGTERM;
      try { await cleanup(signal); } catch (err) { report(err); }
      try {
        await out.finish(code);
      } catch (err) {
        report(err);
        try { await out.flush(); } catch { /* 信号码优先于清理与写出失败。 */ }
      } finally {
        // 仅信号路径强制退出，避免未关闭的外部句柄阻止中断。
        process.exit(code);
      }
    })();
  };
  const onInt = () => onSignal("SIGINT");
  const onTerm = () => onSignal("SIGTERM");
  process.on("SIGINT", onInt);
  process.on("SIGTERM", onTerm);
  return {
    registerCleanup: ((callback) => {
      if (cleanupPromise) throw new CxbError("LOCAL_ERROR", "清理开始后不能再注册回调");
      callbacks.add(callback);
      return () => { callbacks.delete(callback); };
    }) satisfies RegisterCleanup,
    cleanup,
    get shutdown() { return shutdownPromise; },
    dispose() {
      process.removeListener("SIGINT", onInt);
      process.removeListener("SIGTERM", onTerm);
    },
  };
}

/** main 负责选择命令结果；本层统一处理错误、资源清理、写出和进程退出码。 */
export async function runMain(
  main: (out: Output, registerCleanup: RegisterCleanup) => Promise<ExitCode>,
  opts: OutputOptions & { cleanupTimeoutMs?: number } = { command: null, json: false, verbose: false },
): Promise<void> {
  const out = createOutput(opts);
  const signals = registerSignalHandlers(out, opts);
  let code: ExitCode = EXIT.OK;
  const report = (err: unknown) => {
    try { out.error(err); } catch { /* 输出故障不再递归报告。 */ }
  };
  try {
    try {
      code = await main(out, signals.registerCleanup);
    } catch (err) {
      code = asCxbError(err).exitCode;
      report(err);
    }
    if (signals.shutdown) return await signals.shutdown;
    try { await signals.cleanup(); } catch (err) {
      if (code === EXIT.OK) code = EXIT.USAGE_OR_LOCAL_ERROR;
      report(err);
    }
    if (signals.shutdown) return await signals.shutdown;
    try { await out.finish(code); } catch (err) {
      let forceExit = err instanceof OutputError && err.ownedStream;
      report(err);
      try { await out.flush(); } catch (flushError) {
        forceExit ||= flushError instanceof OutputError && flushError.ownedStream;
      }
      if (signals.shutdown) return await signals.shutdown;
      if (forceExit) {
        // 真实未读管道会让 pending 写入阻止自然退出；异常路径有界报告后终止。
        process.exit(code === EXIT.OK ? EXIT.USAGE_OR_LOCAL_ERROR : code);
      }
    }
    if (signals.shutdown) await signals.shutdown;
  } finally {
    signals.dispose();
  }
}
