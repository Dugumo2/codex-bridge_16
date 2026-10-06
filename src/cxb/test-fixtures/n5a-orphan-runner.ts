// 在独立进程里运行一次诊断（目标是 n5a-orphan.ts：先退出、后代占着输出管道），完成后不调用 process.exit，靠事件循环自然退出。
// 用来验证 doctor 收尾会取消未结束的读取：否则挂着的读取会让进程在后代退出前一直不退出。第三个参数原样传给 n5a-orphan.ts。
import { runDiagnosticProcess } from "../doctor";
import { join } from "node:path";

const [pidFile, doneFile, mode] = process.argv.slice(2) as [string, string, string | undefined];
try {
  await runDiagnosticProcess(process.execPath, [join(import.meta.dir, "n5a-orphan.ts"), pidFile, ...(mode ? [mode] : [])],
    { timeoutMs: 5000, signal: new AbortController().signal });
} catch { /* 预期按期以诊断错误返回。 */ }
await Bun.write(doneFile, "done");
