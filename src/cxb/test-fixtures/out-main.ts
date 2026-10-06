import { Writable } from "node:stream";
import { writeFileSync } from "node:fs";
import { CxbError, EXIT } from "../contracts";
import { runMain } from "../output";
import { NOTICE, PRIVATE_ERROR_MESSAGE, PROGRESS, RESULT, RESULT_CHUNK } from "./out-data";

const mode = process.argv[2];
const json = mode?.includes("json") ?? false;
const injected = mode?.startsWith("injected-blocked") ? new Writable({ write() {} }) : undefined;

await runMain(async (out, registerCleanup) => {
  if (injected) {
    out.result("模拟注入流写入阻塞");
    return mode.endsWith("held") ? EXIT.THREAD_HELD : EXIT.OK;
  }
  if (mode === "blocked" || mode === "blocked-held" || mode === "blocked-orphan") {
    // 独立控制文件表示入口已就绪，不依赖被刻意暂停读取的 stdout。
    if (process.argv[3]) writeFileSync(process.argv[3], "就绪", { flag: "wx" });
    if (mode === "blocked-orphan") {
      // 包装进程会故意退出；此句柄用于验证外层能回收仍存活的实际子进程。
      setInterval(() => {}, 1_000);
      return await new Promise<never>(() => {});
    }
    // 远大于系统管道缓冲；父进程刻意保持读端打开却不读取。
    for (let i = 0; i < 4_096; i++) out.result(RESULT_CHUNK);
    return mode === "blocked-held" ? EXIT.THREAD_HELD : EXIT.OK;
  }
  if (mode?.startsWith("signal-") || mode?.startsWith("wait-signal")) {
    const signal = mode.includes("term") ? "SIGTERM" : "SIGINT";
    registerCleanup(async (received) => {
      await new Promise<void>((resolve) => setTimeout(resolve, 10));
      out.notice(`[cxb] 清理：${received}`);
      if (mode.includes("fail")) throw new CxbError("LOCAL_ERROR", "模拟状态写入失败");
    });
    if (mode.includes("timeout")) registerCleanup(() => new Promise(() => {}));
    out.result(RESULT);
    out.notice("[cxb] 等待信号");
    // 保留句柄来检验信号清理后确实终止；仅此测试夹具故意不注销它。
    setInterval(() => {}, 1_000);
    if (!mode.startsWith("wait-signal")) {
      setImmediate(() => {
        process.emit(signal, signal);
        process.emit(signal, signal);
      });
    }
    return await new Promise<never>(() => {});
  }

  if (mode === "cleanup" || mode === "cleanup-fail" || mode === "cleanup-timeout") {
    const keepAlive = setInterval(() => {}, 1_000);
    registerCleanup(async () => {
      await new Promise<void>((resolve) => setTimeout(resolve, 10));
      clearInterval(keepAlive);
      out.notice("[cxb] 已关闭资源");
      if (mode === "cleanup-fail") throw new CxbError("LOCAL_ERROR", "模拟清理失败");
    });
    if (mode === "cleanup-timeout") registerCleanup(() => new Promise(() => {}));
  }
  if (mode?.startsWith("unknown")) {
    const error = new TypeError(PRIVATE_ERROR_MESSAGE);
    error.name = "Cookie: x";
    throw error;
  }
  if (mode === "json-error") {
    throw new CxbError("THREAD_HELD", "线程正被占用", { retryable: true, id: "线程甲" });
  }
  if (mode === "json-ok") {
    out.result("这段普通文本不能混进 JSON");
    out.progress("[cxb] JSON 过程");
    out.resultJson({ ok: true, result: RESULT });
    return EXIT.OK;
  }
  out.progress(PROGRESS);
  out.notice(NOTICE);
  // 多次 write，覆盖队列里已有内容和最后一块多字节内容一起排空的情形。
  for (let i = 0; i < 100; i++) out.result(RESULT_CHUNK);
  if (mode === "held") throw new CxbError("THREAD_HELD", "线程正被占用");
  return mode === "idle" ? EXIT.IDLE : EXIT.OK;
}, {
  command: "run",
  json,
  verbose: mode !== "quiet" && (!mode?.startsWith("unknown") || mode.endsWith("verbose")),
  stdout: injected,
  cleanupTimeoutMs: 100,
  flushTimeoutMs: mode?.includes("blocked") ? 100 : undefined,
});

if (injected) {
  // 此标记只能在 runMain 返回后出现，用于证明注入流不会导致宿主被强制退出。
  process.stdout.write(`${JSON.stringify({ returned: true, destroyed: injected.destroyed })}\n`);
  injected.destroy();
}
