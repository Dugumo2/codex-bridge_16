import { writeFileSync } from "node:fs";
import { createDoctorHandler } from "../doctor";
import { runCli } from "../main";
import { CxbError } from "../contracts";

const signal = process.argv[2] as "SIGINT" | "SIGTERM";
await runCli(["doctor", "--json", "-d", process.cwd()], { doctor: createDoctorHandler({
  findExecutable: () => "可取消的模拟进程",
  runProcess: async (_bin, _args, limit) => new Promise<string>((_resolve, reject) => {
    const keepAlive = setInterval(() => {}, 1000);
    limit.signal.addEventListener("abort", () => {
      clearInterval(keepAlive);
      writeFileSync(process.argv[3], "已取消并回收");
      reject(new CxbError("INTERRUPTED", "模拟进程已取消"));
    }, { once: true });
    setImmediate(() => process.emit(signal, signal));
  }),
}) });
