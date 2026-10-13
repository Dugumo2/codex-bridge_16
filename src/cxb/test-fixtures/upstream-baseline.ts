import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve, sep } from "node:path";

/** 从冻结提交导出只读 CLI，不依赖主工作树当前处于哪个版本。 */
export function upstreamBaselineCli() {
  const parent = resolve(tmpdir());
  const dir = mkdtempSync(join(parent, "cxb-ue-b-baseline-"));
  const dispose = () => {
    const target = resolve(dir);
    if (!target.startsWith(parent + sep)) throw new Error("基线临时目录不在允许清理范围");
    rmSync(target, { recursive: true, force: true });
  };
  try {
    const archive = spawnSync("git", ["archive", "--format=tar", "--output", join(dir, "source.tar"), "54434d2", "src", "package.json"],
      { cwd: resolve(import.meta.dir, "../../.."), timeout: 30_000, windowsHide: true, encoding: "utf8" });
    if (archive.status !== 0) throw new Error(`导出冻结 CLI 失败：${archive.stderr || archive.error?.message}`);
    // 使用相对文件名，兼容把 Windows 盘符误当远端主机的 Git tar。
    const extract = spawnSync("tar", ["-xf", "source.tar", "-C", "."], { cwd: dir, timeout: 30_000, windowsHide: true, encoding: "utf8" });
    if (extract.status !== 0) throw new Error(`解包冻结 CLI 失败：${extract.stderr || extract.error?.message}`);
    return { main: join(dir, "src/cxb/main.ts"), dispose };
  } catch (error) { dispose(); throw error; }
}
