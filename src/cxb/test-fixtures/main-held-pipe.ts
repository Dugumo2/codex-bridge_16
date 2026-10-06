// 故障夹具：父进程自然退出，后代仍持有 stdout/stderr，直到测试明确放行。
import { spawn } from "node:child_process";
import { appendFileSync, existsSync } from "node:fs";

const [mode, registry, release] = process.argv.slice(2);
if (mode === "hold") {
  const deadline = Date.now() + 15_000;
  const timer = setInterval(() => {
    if (existsSync(release)) clearInterval(timer);
    else if (Date.now() >= deadline) {
      clearInterval(timer);
      process.exitCode = 1;
    }
  }, 25);
} else {
  const child = spawn(process.execPath, [import.meta.filename, "hold", registry, release], {
    stdio: ["ignore", "inherit", "inherit"], windowsHide: true, detached: true,
  });
  child.on("error", () => { process.exitCode = 1; });
  if (child.pid === undefined) throw new Error("无法启动持有输出管道的测试后代");
  appendFileSync(registry, `${process.pid}\n${child.pid}\n`);
  child.unref();
}
