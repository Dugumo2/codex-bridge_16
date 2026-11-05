/** 真实上游测试专用隔离、期限与进程回收；不使用继承环境的 broker-harness。 */
import { spawn } from "node:child_process";
import type { Subprocess } from "bun";
import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { createRequire } from "node:module";
import { FAKE_KEY } from "./upstream-model-server";

export const TEMP_ROOT = process.env.TEMP!;
export const TRACE_ROOT = join((process.env.TEMP! + "\\ue-traces"), `process-${process.pid}`);
export const CODEX_CMD = (Bun.which("codex.cmd") ?? "codex.cmd");
export const CLEANUP_MS = 12_000;

export function isolatedEnvironment(root: string, inherited: NodeJS.ProcessEnv = process.env): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {};
  const allowed = new Set(["PATH", "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT", "NUMBER_OF_PROCESSORS", "PROCESSOR_ARCHITECTURE"]);
  for (const [name, value] of Object.entries(inherited)) if (allowed.has(name.toUpperCase())) env[name] = value;
  Object.assign(env, { CODEX_HOME: join(root, "codex-home"), CXB_HOME: join(root, "cxb-home"),
    USERPROFILE: join(root, "profile"), HOME: join(root, "profile"), APPDATA: join(root, "profile", "AppData", "Roaming"),
    LOCALAPPDATA: join(root, "profile", "AppData", "Local"), TEMP: TEMP_ROOT, TMP: TEMP_ROOT,
    CXB_UE_FAKE_KEY: FAKE_KEY, CXB_SERVER: "private", NO_PROXY: "*" });
  return env;
}

export function createUpstreamIsolation(baseUrl: string) {
  if (!/^http:\/\/127\.0\.0\.1:\d+\/v1$/.test(baseUrl)) throw new Error("模型端点必须是 127.0.0.1 的 /v1");
  mkdirSync(TEMP_ROOT, { recursive: true });
  const root = mkdtempSync(join(TEMP_ROOT, "cxb-ue-"));
  const env = isolatedEnvironment(root);
  // 关闭系统代理功能，并把环境代理占位钉在脚本端点；不继承开发者的代理配置。
  // NO_PROXY=* 请求直接连接；即使底层忽略绕过规则，代理也仍是同一个脚本服务。
  Object.assign(env, { HTTP_PROXY: new URL(baseUrl).origin, HTTPS_PROXY: new URL(baseUrl).origin, ALL_PROXY: new URL(baseUrl).origin });
  const cwd = join(root, "workspace");
  for (const path of [cwd, env.CODEX_HOME!, env.CXB_HOME!, env.USERPROFILE!, env.APPDATA!, env.LOCALAPPDATA!]) mkdirSync(path, { recursive: true });
  writeFileSync(join(env.CODEX_HOME!, "config.toml"), [
    'model = "gpt-5.4"', 'model_provider = "ue_script"', 'approval_policy = "never"', 'sandbox_mode = "danger-full-access"',
    'cli_auth_credentials_store = "file"', 'web_search = "disabled"', 'check_for_update_on_startup = false',
    '[analytics]', 'enabled = false', '[feedback]', 'enabled = false', '[features]',
    'respect_system_proxy = false', 'system_proxy_fallback = false', '[model_providers.ue_script]',
    'name = "UE 本地脚本模型"', `base_url = ${JSON.stringify(baseUrl)}`, 'wire_api = "responses"',
    'requires_openai_auth = false', 'supports_websockets = false', 'env_key = "CXB_UE_FAKE_KEY"', "",
  ].join("\n"));
  return { root, cwd, env, baseUrl };
}

/** 从指定 npm 入口解析其原生二进制，避免 cmd／Node 包装层持有 stdio 句柄。 */
export function codexCommand(args: string[], binary = CODEX_CMD): string[] {
  if (!existsSync(binary)) throw new Error(`本机 Codex 入口不存在：${binary}`);
  if (!binary.toLowerCase().endsWith(".cmd")) return [binary, ...args];
  const script = join(dirname(binary), "node_modules", "@openai", "codex", "bin", "codex.js");
  if (!existsSync(script) || process.platform !== "win32") throw new Error("无法定位指定 npm Codex 入口");
  const packageName = process.arch === "arm64" ? "codex-win32-arm64" : "codex-win32-x64";
  const triple = process.arch === "arm64" ? "aarch64-pc-windows-msvc" : "x86_64-pc-windows-msvc";
  const packageFile = createRequire(script).resolve(`@openai/${packageName}/package.json`);
  const native = join(dirname(packageFile), "vendor", triple, "bin", "codex.exe");
  if (!existsSync(native)) throw new Error("指定 npm Codex 的原生二进制不存在");
  return [native, ...args];
}

export interface ManagedProcess {
  child: Subprocess<"pipe", "pipe", "pipe">; closed: Promise<number | null>; output(): { stdout: string; stderr: string };
  onStdout(handler: (chunk: string) => void): void;
  closeStreams(): Promise<void>;
}
export function startManaged(command: string[], isolation: { cwd: string; env: NodeJS.ProcessEnv }): ManagedProcess {
  const child = Bun.spawn(command, { cwd: isolation.cwd, env: isolation.env, windowsHide: true, stdin: "pipe", stdout: "pipe", stderr: "pipe" });
  let stdout = "", stderr = "";
  const handlers: Array<(chunk: string) => void> = [];
  const readers: ReadableStreamDefaultReader<Uint8Array>[] = [];
  const drain = async (stream: ReadableStream<Uint8Array>, write: (chunk: string) => void) => {
    const decoder = new TextDecoder();
    const reader = stream.getReader(); readers.push(reader);
    try {
      while (true) { const result = await reader.read(); if (result.done) break; write(decoder.decode(result.value, { stream: true })); }
      const tail = decoder.decode(); if (tail) write(tail);
    } finally { reader.releaseLock(); }
  };
  const drained = Promise.all([drain(child.stdout, (chunk) => { stdout += chunk; for (const handler of handlers) handler(chunk); }),
    drain(child.stderr, (chunk) => { stderr += chunk; })]);
  const closed = child.exited.then(async (code) => { await drained; return code; });
  void closed.catch(() => {});
  return { child, closed, output: () => ({ stdout, stderr }), onStdout(handler) { handlers.push(handler); },
    async closeStreams() { child.stdin.end(); await Promise.all(readers.map((reader) => reader.cancel().catch(() => {}))); await drained; } };
}
export async function within<T>(operation: Promise<T>, ms: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try { return await Promise.race([operation, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error(`${label} 超过 ${ms} 毫秒期限`)), ms); })]); }
  finally { clearTimeout(timer); }
}
export function alive(pid: number): boolean {
  try { process.kill(pid, 0); return true; } catch (error) { if ((error as NodeJS.ErrnoException).code === "ESRCH") return false; throw error; }
}
export async function stopManaged(proc: ManagedProcess): Promise<void> {
  if (proc.child.exitCode !== null || proc.child.signalCode !== null) { await within(proc.closed, 1000, "已退出进程收尾"); return; }
  const pid = proc.child.pid;
  if (!pid) { await within(proc.closed, 1000, "未启动进程收尾"); return; }
  if (process.platform === "win32" && alive(pid)) {
    // 先收回树再关闭父进程，避免父先退出后丢失仍持有 stdio／cwd 的 shell 快照子进程。
    const killer = Bun.spawn(["taskkill.exe", "/PID", String(pid), "/T", "/F"], { windowsHide: true, stdout: "ignore", stderr: "ignore" });
    const code = await within(killer.exited, 5000, "指定进程树回收");
    if (code !== 0 && alive(pid)) throw new Error(`指定进程树回收失败（taskkill ${code}）`);
    // 同时通知 Bun 释放原生句柄；外部 taskkill 在 Windows 上偶尔不唤醒流的结束事件。
    proc.child.kill("SIGKILL");
    await within(proc.closeStreams(), 2000, "本地 stdio 句柄回收");
    await within(proc.child.exited, 3000, "进程树退出");
    if (alive(pid)) throw new Error(`本路进程 ${pid} 未退出`);
    return;
  }
  proc.child.stdin.end();
  try { await within(proc.closed, 2000, "关闭 stdin 后退出"); }
  catch {
    if (process.platform === "win32" && alive(pid)) {
      const killer = spawn("taskkill.exe", ["/PID", String(pid), "/T", "/F"], { windowsHide: true, stdio: "ignore" });
      await within(new Promise<void>((resolveResult, reject) => { killer.once("error", reject); killer.once("close", () => resolveResult()); }), 5000, "指定进程树回收");
    } else if (alive(pid)) proc.child.kill("SIGKILL");
    await within(proc.closed, 3000, "强制回收后退出");
  }
  if (alive(pid)) throw new Error(`本路进程 ${pid} 未退出`);
}
export async function removeIsolation(root: string): Promise<void> {
  const rel = relative(resolve(TEMP_ROOT), resolve(root));
  if (!rel || isAbsolute(rel) || rel === ".." || rel.startsWith(`..${sep}`)) throw new Error("拒绝清理测试临时目录之外的路径");
  const end = Date.now() + 5000;
  while (true) {
    try { rmSync(root, { recursive: true, force: true }); return; }
    catch (error) { if (Date.now() >= end) throw error; await Bun.sleep(50); }
  }
}

/** 在首个模型请求之前核对有效配置，不满足安全边界就终止。 */
export function verifyEffectiveConfig(result: unknown, expected: string): void {
  const config = (result as { config?: Record<string, any> })?.config;
  const provider = config?.model_providers?.ue_script;
  if (config?.model_provider !== "ue_script" || provider?.base_url !== expected || provider?.wire_api !== "responses"
      || provider?.requires_openai_auth !== false || provider?.supports_websockets !== false || provider?.env_key !== "CXB_UE_FAKE_KEY"
      || provider?.request_max_retries != null || provider?.stream_max_retries != null || config?.cli_auth_credentials_store !== "file"
      || config?.features?.respect_system_proxy !== false || config?.features?.system_proxy_fallback !== false) {
    throw new Error("最终有效 provider、端点、认证方式、传输或默认重试核对失败");
  }
}
