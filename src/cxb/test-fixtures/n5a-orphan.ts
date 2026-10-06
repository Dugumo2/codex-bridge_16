// 诊断程序替身：启动一个继承输出的后代（n5a-hang.ts，登记 PID 后不退出），自己立即退出。
// 第二个参数为 stdout-only 时，后代只继承标准输出（标准错误随本进程退出而关闭），用来区分两路读取的结束状态。
// 用来验证父进程先退出、后代仍占着输出管道时，doctor 按期返回并取消未结束的读取。
import { spawn, type StdioOptions } from "node:child_process";
import { join } from "node:path";

const stdio: StdioOptions = process.argv[3] === "stdout-only" ? ["ignore", "inherit", "ignore"] : "inherit";
spawn(process.execPath, [join(import.meta.dir, "n5a-hang.ts"), process.argv[2]!], { stdio, detached: true, windowsHide: true }).unref();
process.exit(0);
