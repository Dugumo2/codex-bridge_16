// 在独立进程中控制文件系统故障，避免模块替换影响其他用例或真实 broker。
import { mock } from "bun:test";
import * as fs from "node:fs";
import { join } from "node:path";

const actual = { ...fs };
const [mode, dataRoot, legacyRoot, ready, release] = process.argv.slice(2);
const sourceTemplate = join(legacyRoot, "templates", "a.md");

mock.module("node:fs", () => ({
  ...actual,
  copyFileSync: (source: fs.PathLike, destination: fs.PathLike, flags?: number): void => {
    if (mode !== "partial-failure" || String(source) !== sourceTemplate) {
      actual.copyFileSync(source, destination, flags);
      return;
    }
    const content = actual.readFileSync(source);
    actual.writeFileSync(destination, content.subarray(0, Math.floor(content.length / 2)), { flag: "wx" });
    actual.writeFileSync(ready, JSON.stringify({ temporary: String(destination) }));
    const deadline = Date.now() + 15_000;
    const signal = new Int32Array(new SharedArrayBuffer(4));
    while (!actual.existsSync(release)) {
      if (Date.now() >= deadline) throw new Error("等待释放模板复制控制点超时");
      Atomics.wait(signal, 0, 0, 10);
    }
    // 模拟 copyFileSync 失败时移除已创建的半文件。
    actual.unlinkSync(destination);
    throw Object.assign(new Error("模拟模板复制中途失败"), { code: "EIO" });
  },
  linkSync: (source: fs.PathLike, target: fs.PathLike): void => {
    if (mode === "EPERM" || mode === "ENOTSUP") {
      throw Object.assign(new Error("模拟目标目录不支持硬链接发布"), { code: mode });
    }
    if (mode === "existing-at-publish") {
      // 在存在性检查之后、发布之前，模拟另一个进程先发布完整模板。
      actual.writeFileSync(target, "并发进程已发布的完整模板", { flag: "wx" });
    }
    actual.linkSync(source, target);
  },
}));

const { migrateLegacyData } = await import("../workspace");
process.stdout.write(JSON.stringify(migrateLegacyData({ dataRoot, legacyRoot })) + "\n");
