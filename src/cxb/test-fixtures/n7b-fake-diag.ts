// diag-exec 替身（任务说明 N7-A 2.2 节的调用约定），经 .cmd 包装当作「broker 可执行文件」交给 runDiagnosticProcess。
// 用法：<bun> n7b-fake-diag.ts <模式> <记录文件> diag-exec --timeout-ms <毫秒> --status-file <路径> -- <程序> [参数...]
// 模式：exited（运行程序、继承输出、写真实退出码）、big（状态文件写退出码 300、自身退出 44）、negative、overflow（越界退出码）、timeout、failed、missing（不写状态文件）、corrupt（写损坏的状态文件）、
// hang（登记自己的 PID 后不退出，等调用方结束）。记录文件写入收到的参数（JSON），供用例核对调用约定。
import { spawnSync } from "node:child_process";
import { renameSync, writeFileSync } from "node:fs";

const [mode, record, ...argv] = process.argv.slice(2);
writeFileSync(record!, JSON.stringify(argv));
const status = argv[argv.indexOf("--status-file") + 1]!;
const program = argv.slice(argv.indexOf("--") + 1);
const write = (value: string) => { writeFileSync(`${status}.tmp`, value); renameSync(`${status}.tmp`, status); };

if (mode === "hang") {
  writeFileSync(`${record}.pid`, String(process.pid));
  setInterval(() => {}, 1000);
} else if (mode === "exited") {
  const result = spawnSync(program[0]!, program.slice(1), { stdio: "inherit", windowsHide: true });
  write(JSON.stringify({ outcome: "exited", exitCode: result.status ?? 1 }));
  process.exit(result.status ?? 1);
} else if (mode === "big") {
  // Bun 的进程退出码只保留低 8 位，大于 255 的 Windows 退出码只能由替身直接写进状态文件。
  write(JSON.stringify({ outcome: "exited", exitCode: 300 }));
  process.exit(44);
} else if (mode === "negative" || mode === "overflow") {
  // 越界退出码（不是 u32）说明状态文件损坏。
  write(JSON.stringify({ outcome: "exited", exitCode: mode === "negative" ? -1 : 4294967296 }));
  process.exit(0);
} else if (mode === "timeout") {
  write(JSON.stringify({ outcome: "timeout" }));
  process.exit(124);
} else if (mode === "failed") {
  write(JSON.stringify({ outcome: "failed", reason: "模拟：创建进程失败" }));
  process.exit(125);
} else if (mode === "corrupt") {
  write("{损坏");
  process.exit(0);
} else {
  process.exit(0);
}
