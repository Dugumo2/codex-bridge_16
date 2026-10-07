// 诊断程序替身：启动一个继承输出的后代（n5a-hang.ts，登记 PID 后不退出），等后代登记好 PID 再自己退出（最多 5 秒）。
// 与 n5a-orphan.ts 的区别：确保后代在本进程退出前已运行，用来证明经 diag-exec 时后代由 Job 回收，而不是来不及启动。
import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { join } from "node:path";

const pidFile = process.argv[2]!;
spawn(process.execPath, [join(import.meta.dir, "n5a-hang.ts"), pidFile], { stdio: "inherit", detached: true, windowsHide: true }).unref();
const deadline = Date.now() + 5000;
while (!existsSync(pidFile) && Date.now() < deadline) await Bun.sleep(20);
process.stdout.write("父程序退出\n");
process.exit(existsSync(pidFile) ? 0 : 3);
