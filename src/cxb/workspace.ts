// src/cxb/workspace.ts — 工作区根、工作区键、数据目录（S4 拍板 G）。
//
// 工作区根取 git 顶层目录（不是仓库就取目录本身），作为 `--cwd` 传给 broker；工作区键与 Rust
// `store::path_key(canonicalize(root))` 逐字一致（去掉扩展路径前缀，UNC 保留 `UNC\` 形式），管道名 `cxb-<键>`。
// 同步文件操作没有应用层期限；本模块面向本机路径，网络路径或存储异常可能阻塞主线程，
// 此时定时器和信号处理不能中断同步 I/O。这是已接受的限制，不引入工作线程。

import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { realpathSync, statSync } from "node:fs";
import { isAbsolute, join, resolve } from "node:path";
import { CxbError, CXB_HOME_ENV, type ResolveWorkspace } from "./contracts";

/** 对齐 Rust canonicalize → 去掉开头的 \\?\；UNC 不能还原成普通双反斜杠。 */
export function canonicalWorkspacePath(root: string): string {
  try { return normalizeCanonicalPath(realpathSync.native(root)); }
  catch {
    const absolute = resolve(root);
    return absolute.startsWith("\\\\?\\") ? absolute.slice(4) : absolute;
  }
}

/** Node 的普通 UNC 结果补回 Rust canonicalize 的 UNC 字面量；不做大小写折叠。 */
export function normalizeCanonicalPath(normalized: string): string {
  if (normalized.startsWith("\\\\?\\")) return normalized.slice(4);
  if (process.platform === "win32" && normalized.startsWith("\\\\")) {
    return `UNC\\${normalized.slice(2)}`;
  }
  return normalized;
}

export const resolveWorkspace: ResolveWorkspace = (dir, env = process.env) => {
  let absolute: string;
  try {
    absolute = resolve(dir);
    if (!statSync(absolute).isDirectory()) throw new Error("不是目录");
  } catch (cause) {
    throw new CxbError("USAGE", `工作区目录不存在或不是目录：${dir}`, { cause });
  }

  const configured = env[CXB_HOME_ENV];
  let dataRoot: string;
  if (configured) {
    if (!isAbsolute(configured)) throw new CxbError("USAGE", "CXB_HOME 必须是绝对路径。");
    dataRoot = configured;
  } else {
    if (!env.USERPROFILE) throw new CxbError("LOCAL_ERROR", "无法确定数据目录：CXB_HOME 和 USERPROFILE 均未设置。");
    dataRoot = join(env.USERPROFILE, ".codex-bridge");
  }

  let root = absolute;
  try {
    const result = spawnSync("git", ["rev-parse", "--show-toplevel"], {
      cwd: absolute, env, timeout: 5000, windowsHide: true, encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"], maxBuffer: 1024 * 1024,
    });
    if (!result.error && result.status === 0 && result.stdout.trim()) {
      root = resolve(result.stdout.trim());
    }
  } catch { /* git 不可用或不是仓库时，以传入目录为根。 */ }

  const key = createHash("sha256").update(canonicalWorkspacePath(root), "utf8").digest("hex").slice(0, 16);
  const pipePath = `\\\\.\\pipe\\cxb-${key}`;
  return { dir: absolute, root, key, dataRoot, workspaceDir: join(dataRoot, "workspaces", key), pipePath, endpoint: `pipe:${pipePath}` };
};
