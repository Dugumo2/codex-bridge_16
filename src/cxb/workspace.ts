// src/cxb/workspace.ts — 工作区根、工作区键、数据目录与旧数据迁移（S4 拍板 G、H）。
//
// 工作区根取 git 顶层目录（不是仓库就取目录本身），作为 `--cwd` 传给 broker；工作区键与 Rust
// `store::path_key(canonicalize(root))` 逐字一致（去掉扩展路径前缀，UNC 保留 `UNC\` 形式），管道名 `cxb-<键>`。
// 旧数据迁移只迁 config.json 的有效键与用户模板，不碰旧文件；按缺键补齐、模板不覆盖实现无锁幂等，
// 全部成功后排他创建完成标记。已接受的限制：另一进程恰好在读配置到改名提交之间手工修改配置时，
// 修改可能被本次合并覆盖；不为这个极小窗口引入迁移锁。
// 同步文件操作没有应用层期限；本模块面向本机路径，网络路径或存储异常可能阻塞主线程，
// 此时定时器和信号处理不能中断同步 I/O。这是已接受的限制，不引入工作线程。
// 模板先完整复制到目标目录内的唯一临时文件，再以硬链接不覆盖地原子发布；硬链接不受支持或被拒绝时，
// 告警并清理本次临时文件，不发布完成标记。保留重试机会，不退回可能暴露半文件的直接复制。

import { spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import {
  closeSync, constants, copyFileSync, existsSync, fsyncSync, linkSync, lstatSync, mkdirSync,
  openSync, readFileSync, readdirSync, realpathSync, statSync, unlinkSync, writeFileSync,
} from "node:fs";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { CxbError, CXB_HOME_ENV, type MigrationReport, type ResolveWorkspace } from "./contracts";
import { CONFIG_SCHEMA, isConfigKey } from "./config-schema";
import { readUserConfig, writeUserConfig } from "./user-config";

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

const MARKER = "migrated-from-cxb.json";

function errorCode(error: unknown): string {
  return (error as NodeJS.ErrnoException)?.code ?? "未知错误";
}

function exists(path: string): boolean {
  try { lstatSync(path); return true; }
  catch (error) { if (errorCode(error) === "ENOENT") return false; throw error; }
}

function readConfig(path: string): Record<string, unknown> {
  if (!exists(path)) return {};
  const value: unknown = JSON.parse(readFileSync(path, "utf8"));
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new CxbError("LOCAL_ERROR", "配置文件必须是 JSON 对象，请备份并修复后重试。");
  }
  return value as Record<string, unknown>;
}

/** 同目录暂存后不覆盖发布，其他迁移进程看到的最终名称一定对应完整文件。 */
function publishTemplate(source: string, target: string): boolean {
  const temporary = join(dirname(target), `.cxb-template-${process.pid}-${randomUUID()}.tmp`);
  let ownsTemporary = true;
  try {
    try {
      // copyFileSync 返回时复制已完成且内部句柄已关闭；临时文件不会被当成模板。
      copyFileSync(source, temporary, constants.COPYFILE_EXCL);
    } catch (error) {
      if (errorCode(error) === "EEXIST") ownsTemporary = false;
      throw error;
    }
    try {
      linkSync(temporary, target);
      return true;
    } catch (error) {
      if (errorCode(error) === "EEXIST") return false;
      if (["EPERM", "EACCES", "ENOTSUP", "EOPNOTSUPP", "ENOSYS", "EXDEV"].includes(errorCode(error))) {
        throw new CxbError("LOCAL_ERROR", `无法用硬链接原子发布（${errorCode(error)}）；请检查权限或改用支持硬链接的数据目录后重试。`, { cause: error });
      }
      throw error;
    }
  } finally {
    if (ownsTemporary) {
      try { unlinkSync(temporary); }
      catch (error) { if (errorCode(error) !== "ENOENT") throw error; }
    }
  }
}

/** 解析尚未创建路径的现有祖先，防止通过 junction 把新数据写进旧目录。 */
function physicalPath(path: string): string {
  const absolute = resolve(path);
  if (exists(absolute)) return realpathSync.native(absolute);
  const parent = resolve(absolute, "..");
  return join(physicalPath(parent), relative(parent, absolute));
}

function overlaps(a: string, b: string): boolean {
  const inside = (parent: string, child: string) => {
    const rel = relative(parent, child);
    return rel === "" || (!isAbsolute(rel) && rel !== ".." && !rel.startsWith(`..${sep}`));
  };
  return inside(a, b) || inside(b, a);
}

/** 只读旧配置与用户模板；失败降级为告警，保留已经成功复制的内容供下次幂等重试。 */
export function migrateLegacyData(opts: { dataRoot: string; legacyRoot?: string }): MigrationReport {
  const report: MigrationReport = { migrated: false, configKeys: [], droppedKeys: [], templates: [], warnings: [] };
  let markerPath: string | undefined;
  let markerOwned = false;
  try {
    markerPath = join(opts.dataRoot, MARKER);
    if (exists(markerPath)) return report;
    const home = process.env.USERPROFILE;
    if (!opts.legacyRoot && !home) {
      report.warnings.push("无法确定旧数据目录：USERPROFILE 未设置；下次重试迁移。");
      return report;
    }
    const legacyRoot = opts.legacyRoot ?? join(home!, ".cxb-legacy");
    if (overlaps(physicalPath(opts.dataRoot), physicalPath(legacyRoot))) {
      report.warnings.push("新旧数据目录不能相同或相互包含；未迁移任何文件。");
      return report;
    }
    mkdirSync(opts.dataRoot, { recursive: true });
    const hasLegacy = exists(legacyRoot);
    if (hasLegacy) {
      if (!statSync(legacyRoot).isDirectory()) throw new CxbError("LOCAL_ERROR", "旧数据路径不是目录。");
      try {
        const oldConfig = readConfig(join(legacyRoot, "config.json"));
        const newConfig = readUserConfig(opts.dataRoot).values;
        const keys: string[] = [];
        for (const [key, value] of Object.entries(oldConfig)) {
          if (!isConfigKey(key) || !CONFIG_SCHEMA[key].isValid(value)) report.droppedKeys.push(key);
          else if (!Object.hasOwn(newConfig, key)) { newConfig[key] = value; keys.push(key); }
        }
        if (keys.length) {
          writeUserConfig(opts.dataRoot, newConfig);
          report.configKeys.push(...keys);
        }
      } catch (error) {
        report.warnings.push(`配置迁移失败（${errorCode(error)}）；请备份并检查新旧 config.json，原文件未覆盖，下次重试。`);
      }

      const source = join(legacyRoot, "templates");
      try {
        if (exists(source)) {
          const destination = join(opts.dataRoot, "templates");
          const files = readdirSync(source, { withFileTypes: true }).filter((entry) => entry.isFile() && entry.name.endsWith(".md"));
          if (files.length) {
            if (overlaps(physicalPath(destination), physicalPath(legacyRoot))) {
              throw new CxbError("LOCAL_ERROR", "模板目标指向旧目录。");
            }
            mkdirSync(destination, { recursive: true });
          }
          for (const file of files) {
            try {
              if (exists(join(destination, file.name))) continue;
              if (publishTemplate(join(source, file.name), join(destination, file.name))) report.templates.push(file.name);
            } catch (error) {
              const detail = error instanceof CxbError ? error.message : `迁移失败（${errorCode(error)}），下次重试。`;
              report.warnings.push(`模板 ${file.name} ${detail}`);
            }
          }
        }
      } catch (error) { report.warnings.push(`模板目录迁移失败（${errorCode(error)}），下次重试。`); }
    }

    if (report.warnings.length === 0) {
      let markerFd: number | undefined;
      try {
        try { markerFd = openSync(markerPath, "wx"); }
        catch (error) {
          // 另一 CLI 已完成迁移；不覆盖或清理它创建的标记。
          if (errorCode(error) === "EEXIST") return report;
          throw error;
        }
        markerOwned = true;
        writeFileSync(markerFd, `${JSON.stringify({ legacyRoot, migratedAt: new Date().toISOString() })}\n`, "utf8");
        fsyncSync(markerFd);
      } finally { if (markerFd !== undefined) closeSync(markerFd); }
    }
    report.migrated = hasLegacy && report.warnings.length === 0;
  } catch (error) {
    report.warnings.push(`旧数据迁移失败（${errorCode(error)}），下次重试。`);
  } finally {
    if (report.warnings.length && markerOwned && markerPath) {
      try { unlinkSync(markerPath); } catch (error) { report.warnings.push(`清理迁移标记失败（${errorCode(error)}）。`); }
    }
    if (report.warnings.length) report.migrated = false;
  }
  return report;
}
