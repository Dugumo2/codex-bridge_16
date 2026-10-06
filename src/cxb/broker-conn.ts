// src/cxb/broker-conn.ts — 查找 cxb-broker、读取版本、拉起、连接与版本握手（详细设计 0.1、2.2、第 5 节）。
//
// 只有管道不存在（ENOENT）才经 `cxb-broker launch` 拉起，同一次连接最多拉起 3 次；管道存在但连上就被断开
// （broker 关闭中）或管道忙时只退避重连（100 ms 起翻倍、最长 2 秒）。握手声明 capabilities.cxb，版本不符
// 先请求 ifIdle 关闭：忙则 PROTOCOL_MISMATCH（退出 12）；之后照常握手，可复用另一 CLI 拉起的新版。
// 替换只做一轮。连接受 opts.timeoutMs 与 signal 约束，超时为 BROKER_UNAVAILABLE，取消为 INTERRUPTED。

import childProcess from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { config } from "../config";
import { safeSummary } from "./user-config";
export { safeSummary } from "./user-config";
import { createRpcEndpoint } from "../rpc";
import { RpcError } from "../types";
import {
  CXB_BROKER_BIN_ENV,
  CXB_PROTOCOL_VERSION,
  CxbError,
  type BrokerHandshake,
  type BrokerRpc,
  type BrokerRequestOptions,
  type ConnectOptions,
  type BrokerConnection,
  type ResolveBrokerBin,
} from "./contracts";

const MAX_TIMER_MS = 2_147_483_647;

function unavailable(message: string, cause?: unknown, retryable = false): CxbError {
  return new CxbError("BROKER_UNAVAILABLE", message, { cause, retryable });
}

/** 显式配置即为唯一选择，不能悄悄回退到另一份可执行文件。 */
export const resolveBrokerBin: ResolveBrokerBin = (env = process.env) => {
  const explicit = env[CXB_BROKER_BIN_ENV];
  const candidates = explicit !== undefined
    ? [explicit === "" ? "" : resolve(explicit)]
    : [
      join(dirname(resolve(process.argv[1] ?? process.execPath)), "cxb-broker.exe"),
      resolve(dirname(fileURLToPath(import.meta.url)), "../../broker-rs/target/debug/cxb-broker.exe"),
    ];
  for (const candidate of candidates) {
    try {
      if (fs.statSync(candidate).isFile()) return candidate;
    } catch { /* 找不到或不可访问时，按查找顺序继续。 */ }
  }
  throw unavailable(`找不到 cxb-broker 可执行文件，已查找：${candidates.map((p) => p || "（空路径）").join("；")}`);
};


function runBinary(bin: string, args: string[], timeoutMs: number, env?: NodeJS.ProcessEnv): Promise<string> {
  return new Promise((resolveResult, reject) => {
    const child = childProcess.execFile(bin, args, {
      env,
      windowsHide: true,
      encoding: "utf8",
      timeout: Math.max(1, Math.ceil(timeoutMs)),
      killSignal: "SIGKILL",
      maxBuffer: 64 * 1024,
    }, (error, stdout, stderr) => {
      if (error) {
        reject(unavailable(
          `执行 cxb-broker ${args[0]} 失败（${bin}）：${safeSummary(stderr) || "进程未成功结束"}`,
          error,
          error.killed === true,
        ));
      } else {
        resolveResult(stdout);
      }
    });
    // execFile 不使用 shell；立即关闭输入，子进程不能等待交互输入。
    child.stdin?.destroy();
  });
}

export async function brokerVersionOf(bin: string): Promise<string> {
  try {
    const output = await runBinary(bin, ["--version"], 5000);
    const match = /^cxb-broker (\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?)$/.exec(output.trim());
    if (!match) throw unavailable(`无法解析 cxb-broker 版本（${bin}）`);
    return match[1];
  } catch (error) {
    if (error instanceof CxbError) throw error;
    throw unavailable(`读取 cxb-broker 版本失败（${bin}）`, error);
  }
}

function errorCode(error: unknown): string | undefined {
  if (error instanceof CxbError) return errorCode(error.cause);
  return (error as NodeJS.ErrnoException | null)?.code;
}

function interrupted(): CxbError {
  return new CxbError("INTERRUPTED", "连接 broker 已取消");
}

function checkCancelled(signal?: AbortSignal): void {
  if (signal?.aborted) throw interrupted();
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((done, reject) => {
    const cancel = () => { clearTimeout(timer); signal?.removeEventListener("abort", cancel); reject(interrupted()); };
    const timer = setTimeout(() => { signal?.removeEventListener("abort", cancel); done(); }, ms);
    signal?.addEventListener("abort", cancel, { once: true });
    if (signal?.aborted) cancel();
  });
}

/** 安装接收和断开处理后才发起连接，避免瞬间断开漏掉 pending 的拒绝。 */
function openPipe(pipePath: string, timeoutMs: number, log?: (line: string) => void) {
  const socket = new net.Socket();
  socket.setEncoding("utf8");
  let reason: string | null = null;
  const endpoint = createRpcEndpoint({
    label: "cxb",
    log: log ?? (() => {}),
    requestTimeout: config.requestTimeout,
    send(data: string) {
      if (socket.destroyed) throw unavailable(reason ?? "broker 连接已断开");
      socket.write(data);
    },
    downReason: () => reason ?? (socket.destroyed ? "broker 连接已断开" : null),
    overflowReason: "broker 回包缓冲区超过上限",
    onOverflow: () => socket.destroy(),
    writeFailedPrefix: "写入 broker 管道失败：",
  });
  function fail(message: string): void {
    reason ??= message;
    endpoint.fail(reason);
  }
  /** 逐请求期限（不传用 30 秒）；超时只拒绝这一个请求，连接与其他在途请求不受影响。 */
  function request<T>(method: string, params?: unknown, opts?: BrokerRequestOptions): Promise<T> {
    const timeoutMs = opts?.timeoutMs ?? config.requestTimeout;
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0 || timeoutMs > MAX_TIMER_MS) {
      throw new CxbError("USAGE", `broker 请求期限必须大于 0 且不超过 ${MAX_TIMER_MS} 毫秒`);
    }
    return endpoint.request<T>(method, params, { timeoutMs: Math.ceil(timeoutMs) });
  }

  socket.on("data", (chunk: string) => endpoint.feed(chunk));
  socket.on("error", () => {
    fail("broker 管道发生错误，连接已断开");
  });
  socket.on("close", () => {
    fail("broker 连接意外断开");
  });

  function destroy(): void {
    reason ??= "broker 连接已关闭";
    endpoint.rejectAllPending(reason);
    endpoint.markClosed();
    socket.destroy();
  }

  let closing: Promise<void> | undefined;
  function close(): Promise<void> {
    return closing ??= new Promise<void>((done) => {
      if (socket.closed) { destroy(); done(); return; }
      const finish = () => {
        clearTimeout(timer);
        socket.off("close", finish);
        done();
      };
      const timer = setTimeout(finish, 1000);
      socket.once("close", finish);
      destroy();
    });
  }

  const connected = new Promise<void>((done, reject) => {
    const finish = (error?: unknown) => {
      clearTimeout(timer);
      socket.off("connect", onConnect);
      socket.off("error", onError);
      socket.off("close", onClose);
      if (error) { destroy(); reject(error); } else done();
    };
    const onConnect = () => finish();
    const onError = (error: Error) => finish(unavailable("无法连接 broker 管道", error, true));
    const onClose = () => finish(unavailable("broker 在连接完成前断开", undefined, true));
    const timer = setTimeout(() => finish(unavailable("连接 broker 管道超时", undefined, true)), timeoutMs);
    socket.once("connect", onConnect);
    socket.once("error", onError);
    socket.once("close", onClose);
    try { socket.connect(pipePath); } catch (error) { onError(error as Error); }
  });
  return { connected, endpoint, request, destroy, close, disconnected: () => reason !== null || socket.destroyed };
}

/** RPC 的线上错误保留在 cause 中，供命令层读取 rpcCode；对外统一使用 CxbError。 */
function publicRpc(transport: ReturnType<typeof openPipe>): BrokerRpc {
  return {
    ...transport.endpoint,
    async request<T>(method: string, params?: unknown, opts?: BrokerRequestOptions): Promise<T> {
      try {
        return await transport.request<T>(method, params, opts);
      } catch (error) {
        if (error instanceof CxbError) throw error;
        const detail = error instanceof RpcError ? `远端错误 ${error.rpcCode}` : "连接断开或请求超时";
        throw unavailable(`broker 请求 ${method} 失败：${detail}`, error);
      }
    },
  };
}

/** 探测只连接现有实例；缺管道用独立错误区分，旧版本仍交回句柄供只读诊断。 */
export class BrokerNotRunning extends CxbError {
  constructor() { super("BROKER_UNAVAILABLE", "broker 未运行"); }
}

export type ProbeConnectOptions = ConnectOptions & { probe?: boolean };

export const connectBroker = async (opts: ProbeConnectOptions): Promise<BrokerConnection> => {
  const deadline = performance.now() + opts.timeoutMs;
  const timedOut = () => unavailable("连接 broker 已超过总期限，请稍后重试", undefined, true);
  const remaining = (cap = MAX_TIMER_MS) => {
    checkCancelled(opts.signal);
    const left = deadline - performance.now();
    if (!Number.isFinite(left) || left <= 0) throw timedOut();
    return Math.max(1, Math.ceil(Math.min(left, cap)));
  };
  let backoff = 100;
  let launches = 0;
  let replaced = false;
  let oldHandshake: BrokerHandshake | undefined;
  let observedGone = false;
  let active: ReturnType<typeof openPipe> | undefined;
  const cancel = () => active?.destroy();
  opts.signal?.addEventListener("abort", cancel, { once: true });

  async function retry(): Promise<void> {
    const delay = Math.min(backoff, remaining());
    opts.log?.(`等待 ${delay} 毫秒后重试连接 broker`);
    await sleep(delay, opts.signal);
    backoff = Math.min(backoff * 2, 2000);
  }

  async function request<T>(method: string, params?: unknown): Promise<T> {
    const transport = active!;
    return transport.request<T>(method, params, { timeoutMs: remaining(config.requestTimeout) });
  }

  async function launch(): Promise<void> {
    if (launches >= 3) throw unavailable("已尝试拉起 broker 三次，仍无法连接", undefined, true);
    launches++;
    const args = ["launch", "--endpoint", opts.workspace.endpoint, "--cwd", opts.workspace.root];
    if (opts.idleTimeoutMs !== undefined) args.push("--idle-timeout", String(opts.idleTimeoutMs));
    opts.log?.(`拉起 broker（第 ${launches} 次）：${opts.brokerBin}`);
    // 已开始的 launch 等它在原期限内结束，解析候选 PID 后再响应取消，不能遗失进程信息。
    const output = await runBinary(opts.brokerBin, args, remaining(10_000), opts.env);
    let result: { pid?: unknown; breakaway?: unknown };
    try { result = JSON.parse(output.trim()); } catch (error) {
      throw unavailable("broker launch 未返回有效的进程信息", error);
    }
    if (!result || !Number.isSafeInteger(result.pid) || (result.pid as number) <= 0) {
      throw unavailable("broker launch 未返回有效的进程编号");
    }
    opts.log?.(`broker 候选进程已拉起：pid=${result.pid}`);
    checkCancelled(opts.signal);
  }

  try {
    while (true) {
      remaining();
      opts.log?.(`连接 broker：${opts.workspace.pipePath}`);
      active = openPipe(opts.workspace.pipePath, remaining(5000), opts.log);
      try {
        await active.connected;
      } catch (error) {
        active.destroy();
        remaining();
        if (errorCode(error) === "ENOENT") {
          if (opts.probe) throw new BrokerNotRunning();
          observedGone = true;
          await launch();
        } else if (!["EBUSY", "EAGAIN", "ECONNREFUSED", "ECONNRESET"].includes(errorCode(error) ?? "")) {
          throw error;
        }
        await retry();
        continue;
      }

      let handshake: BrokerHandshake;
      try {
        handshake = await request<BrokerHandshake>("initialize", {
          clientInfo: { name: "cxb", version: config.clientVersion },
          capabilities: {
            cxb: { protocolVersion: CXB_PROTOCOL_VERSION },
            experimentalApi: true,
            optOutNotificationMethods: ["item/reasoning/textDelta"],
          },
        });
      } catch (error) {
        const disconnected = active.disconnected();
        active.destroy();
        remaining();
        if (disconnected || (error instanceof RpcError && error.rpcCode === -32001)) {
          await retry();
          continue;
        }
        throw unavailable("broker 初始化失败或未在期限内响应", error, true);
      }
      remaining();
      active.endpoint.notify("initialized");
      const versions = `CLI 期望 broker ${opts.expectedBrokerVersion}、协议 ${CXB_PROTOCOL_VERSION}；当前 broker ${handshake?.brokerVersion ?? "缺失"}、协议 ${handshake?.protocolVersion ?? "缺失"}`;
      opts.log?.(safeSummary(`broker 版本：${versions}`));
      if (opts.probe || (handshake?.brokerVersion === opts.expectedBrokerVersion &&
          handshake?.protocolVersion === CXB_PROTOCOL_VERSION &&
          Number.isSafeInteger(handshake?.epoch))) {
        checkCancelled(opts.signal);
        return { rpc: publicRpc(active), handshake, launched: launches > 0, close: active.close };
      }
      if (replaced) {
        if (!observedGone && handshake?.epoch === oldHandshake?.epoch &&
            handshake?.brokerVersion === oldHandshake?.brokerVersion &&
            handshake?.protocolVersion === oldHandshake?.protocolVersion) {
          active.destroy();
          await retry();
          continue;
        }
        throw new CxbError("PROTOCOL_MISMATCH", `重新连接后版本仍不匹配：${versions}`);
      }
      try {
        await request("broker/shutdown", { ifIdle: true });
      } catch (error) {
        remaining();
        if (error instanceof RpcError && error.rpcCode === -32001) {
          throw new CxbError("PROTOCOL_MISMATCH", `${versions}；broker 正忙，等任务结束再试`, { cause: error });
        }
        throw unavailable(`版本不匹配，无法确认旧 broker 已接受关闭请求：${versions}`, error, true);
      }
      active.destroy();
      replaced = true;
      oldHandshake = handshake;
      observedGone = false;
      await retry();
    }
  } catch (error) {
    active?.destroy();
    checkCancelled(opts.signal);
    if (performance.now() >= deadline) throw timedOut();
    if (error instanceof CxbError) throw error;
    throw unavailable("无法连接 cxb-broker", error);
  } finally {
    opts.signal?.removeEventListener("abort", cancel);
  }
};
