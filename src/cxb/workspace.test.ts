import { afterEach, beforeEach, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, readdirSync, realpathSync, writeFileSync } from "node:fs";
import net from "node:net";
import { join, resolve } from "node:path";
import { createRpcEndpoint } from "../rpc";
import { BROKER_BIN, createMockCodex, envWithPathPrefix } from "../test-support/broker-harness";
import { CxbError } from "./contracts";
import { canonicalWorkspacePath, normalizeCanonicalPath, resolveWorkspace } from "./workspace";
import { bounded, CLEANUP_HOOK_TIMEOUT_MS, cleanupTestResources, eventually, makeTemp, privateTestEnv, ProcessTracker } from "./test-fixtures/ws-test-support";

let temporary: string;
let processes: ProcessTracker;
let env: NodeJS.ProcessEnv;
let substMappings: Set<string>;

beforeEach(() => {
  temporary = makeTemp("cxb-n1b-workspace-");
  processes = new ProcessTracker();
  env = { ...privateTestEnv(), CXB_HOME: join(temporary, "data") };
  substMappings = new Set();
});

afterEach(async () => {
  const errors: unknown[] = [];
  for (const drive of substMappings) {
    try { removeSubstMapping(drive); } catch (error) { errors.push(error); }
  }
  try { await cleanupTestResources(processes, temporary); } catch (error) { errors.push(error); }
  if (errors.length) throw new AggregateError(errors, "工作区测试资源清理失败");
}, CLEANUP_HOOK_TIMEOUT_MS);

function runTool(command: string, args: string[]) {
  const result = spawnSync(command, args, {
    cwd: temporary, env: privateTestEnv(), timeout: 5000, windowsHide: true,
    encoding: "utf8", stdio: ["ignore", "pipe", "pipe"],
  });
  if (result.pid > 0) processes.descendants.add(result.pid);
  return result;
}

function removeSubstMapping(drive: string): void {
  const result = runTool("subst.exe", [drive, "/D"]);
  if (result.error || result.status !== 0) throw new Error(`清理 SUBST 映射 ${drive} 失败，退出码 ${result.status}`);
  substMappings.delete(drive);
}

function directory(name: string): string {
  const path = join(temporary, name);
  mkdirSync(path, { recursive: true });
  return path;
}

function initRepository(): string {
  const repo = directory("仓库 root");
  const result = runTool("git", ["init", "--quiet", repo]);
  if (result.error || result.status !== 0) throw new Error("无法初始化临时测试仓库");
  mkdirSync(join(repo, "a", "b"), { recursive: true });
  return repo;
}

async function connect(path: string): Promise<net.Socket> {
  return new Promise((resolveConnection, reject) => {
    const socket = new net.Socket();
    const timer = setTimeout(() => { socket.destroy(); reject(new Error("连接测试管道超时")); }, 1000);
    socket.once("connect", () => { clearTimeout(timer); resolveConnection(socket); });
    socket.on("error", (error) => { clearTimeout(timer); socket.destroy(); reject(error); });
    socket.connect(path);
  });
}

/** 真 broker 作为 Rust canonicalize/path_key 的对照，不重编 Rust，不连接真实 Codex。 */
async function compareRust(dir: string): Promise<string> {
  const workspace = resolveWorkspace(dir, env);
  const data = directory(`broker-data-${randomUUID()}`);
  const mock = createMockCodex(directory(`mock-${randomUUID()}`));
  const mockPid = join(mock, "pid.txt");
  const script = join(mock, "codex-mock.ts");
  writeFileSync(script, `import { writeFileSync } from "node:fs";\nwriteFileSync(${JSON.stringify(mockPid)}, String(process.pid));\n${readFileSync(script, "utf8").replace(/^#![^\n]*\n/, "")}`);
  const path = `\\\\.\\pipe\\cxb-n1b-${randomUUID()}`;
  const childEnv = privateTestEnv(envWithPathPrefix(mock));
  const child = processes.add(Bun.spawn([BROKER_BIN, "serve", "--endpoint", `pipe:${path}`, "--cwd", workspace.root, "--idle-timeout", "30000"], {
    cwd: temporary,
    env: { ...childEnv, CXB_HOME: data, CODEX_HOME: directory(`codex-${randomUUID()}`), CXB_SERVER: "private" },
    stdin: "ignore", stdout: "pipe", stderr: "pipe",
  }));
  const stdout = new Response(child.stdout).text();
  const stderr = new Response(child.stderr).text();
  let socket: net.Socket | undefined;
  let rpc: ReturnType<typeof createRpcEndpoint> | undefined;
  try {
    const deadline = Date.now() + 10000;
    while (!socket) {
      if (child.exitCode !== null) throw new Error(`测试 broker 提前退出：${await bounded(stderr, 1000, "读取错误输出超时")}`);
      try { socket = await connect(path); }
      catch { if (Date.now() >= deadline) throw new Error("测试 broker 管道未就绪"); await Bun.sleep(20); }
    }
    const connected = socket;
    connected.setEncoding("utf8");
    rpc = createRpcEndpoint({
      label: "工作区对照测试", requestTimeout: 5000,
      send: (text) => { connected.write(text); },
      downReason: () => connected.destroyed ? "测试连接已关闭" : null,
      overflowReason: "测试响应超过上限", onOverflow: () => connected.destroy(), writeFailedPrefix: "测试写入失败：",
    });
    const endpoint = rpc;
    connected.on("data", (chunk: string) => endpoint.feed(chunk));
    connected.on("close", () => endpoint.fail("测试连接已断开"));
    connected.on("error", () => endpoint.fail("测试管道错误"));
    await rpc.request("initialize", { clientInfo: { name: "cxb-n1b", version: "0.0.1" }, capabilities: { cxb: { protocolVersion: 1 } } });
    rpc.notify("initialized");
    await eventually(() => existsSync(join(data, "workspaces")) && readdirSync(join(data, "workspaces")).length > 0);
    const rustDirectories = readdirSync(join(data, "workspaces"));
    console.info(`TS/Rust 路径对照：${JSON.stringify({ 输入: dir, 工作区根: workspace.root, TS规范化字符串: canonicalWorkspacePath(workspace.root), Rust工作区目录名: rustDirectories })}`);
    expect(rustDirectories).toEqual([workspace.key]);
    return workspace.key;
  } finally {
    try {
      if (existsSync(mockPid)) processes.descendants.add(Number(readFileSync(mockPid, "utf8")));
      if (rpc && !socket?.destroyed) await rpc.request("broker/shutdown", { ifIdle: true });
    } finally {
      rpc?.markClosed();
      socket?.destroy();
      // 即使断言或关闭失败，afterEach 仍按登记的 PID 清理，并将强制结束判为失败。
      expect(await processes.exited(child)).toBe(0);
      await bounded(Promise.all([stdout, stderr]), 1000, "读取 broker 输出超时");
    }
  }
}

test("临时仓库根及子目录得到同一根、键与端点", () => {
  const root = initRepository();
  const a = resolveWorkspace(root, env);
  const b = resolveWorkspace(join(root, "a", "b"), env);
  expect(a.root).toBe(resolve(root));
  expect(b.root).toBe(a.root);
  expect(b.key).toBe(a.key);
  expect(a.key).toMatch(/^[0-9a-f]{16}$/);
  expect(a.pipePath).toBe(`\\\\.\\pipe\\cxb-${a.key}`);
  expect(a.endpoint).toBe(`pipe:${a.pipePath}`);
  expect(a.workspaceDir).toBe(join(env.CXB_HOME!, "workspaces", a.key));
  expect(b.dir).toBe(resolve(root, "a", "b"));
});

test("非仓库目录以自身为根，解析过程不创建数据目录", () => {
  const dir = directory("plain");
  const workspace = resolveWorkspace(dir, env);
  expect(workspace.root).toBe(resolve(dir));
  expect(workspace.dataRoot).toBe(env.CXB_HOME!);
  expect(workspace.key).toMatch(/^[0-9a-f]{16}$/);
  expect(workspace.pipePath).toBe(`\\\\.\\pipe\\cxb-${workspace.key}`);
  expect(workspace.endpoint).toBe(`pipe:${workspace.pipePath}`);
  expect(workspace.workspaceDir).toBe(join(env.CXB_HOME!, "workspaces", workspace.key));
  expect(existsSync(workspace.dataRoot)).toBe(false);
  expect(workspace.key).toBe(createHash("sha256").update(realpathSync.native(dir)).digest("hex").slice(0, 16));
});

test("git 不可用时退回指定目录", () => {
  const isolated = Object.fromEntries(Object.entries(env).filter(([key]) => key.toLowerCase() !== "path"));
  const dir = directory("without-git");
  expect(resolveWorkspace(dir, { ...isolated, PATH: directory("empty-bin") }).root).toBe(dir);
});

test("模拟服务环境清除所有大小写形式的开发者接入选择", () => {
  const isolated = privateTestEnv({
    cXb_SeRvEr: "shared", CXB_SERVER_SOCKET: "真实服务路径", CXB_SERVER: "shared",
    cxb_server_socket: "另一个真实服务路径", PATH: "保留路径",
  });
  expect(isolated).toEqual({ PATH: "保留路径", CXB_SERVER: "private" });
});

test("不存在的路径及普通文件都是 USAGE", () => {
  const file = join(temporary, "file");
  writeFileSync(file, "不是目录");
  for (const dir of [file, join(temporary, "missing")]) {
    try { resolveWorkspace(dir, env); throw new Error("应该报错"); }
    catch (error) { expect(error).toBeInstanceOf(CxbError); expect((error as CxbError).code).toBe("USAGE"); }
  }
});

test("CXB_HOME 必须绝对，空值回退 USERPROFILE，缺少主目录是 LOCAL_ERROR", () => {
  for (const value of ["relative", "D:relative"]) {
    expect(() => resolveWorkspace(temporary, { ...env, CXB_HOME: value })).toThrow(CxbError);
  }
  const emptyHome = resolveWorkspace(temporary, { ...env, CXB_HOME: "", USERPROFILE: temporary });
  expect(emptyHome.dataRoot).toBe(join(temporary, ".codex-bridge"));
  const absentHome = resolveWorkspace(temporary, { ...env, CXB_HOME: undefined, USERPROFILE: temporary });
  expect(absentHome.dataRoot).toBe(emptyHome.dataRoot);
  try { resolveWorkspace(temporary, { CXB_HOME: "" }); throw new Error("应该报错"); }
  catch (error) { expect(error).toBeInstanceOf(CxbError); expect((error as CxbError).code).toBe("LOCAL_ERROR"); }
});

test("路径不存在时规范化退回绝对路径", () => {
  const missing = join(temporary, "missing", "..", "absent");
  expect(canonicalWorkspacePath(missing)).toBe(resolve(missing));
});

test("UNC 与扩展 UNC 的前缀规则保留 Rust 的 UNC 字面量", () => {
  // 纯字符串验证，避免无共享环境中的网络路径解析阻塞。
  const tail = "server\\share\\目录";
  expect(normalizeCanonicalPath(`\\\\?\\UNC\\${tail}`)).toBe(`UNC\\${tail}`);
  expect(normalizeCanonicalPath(`\\\\${tail}`)).toBe(`UNC\\${tail}`);
});

test("TS/Rust 对照：同仓库根与不同子目录", async () => {
  const root = initRepository();
  const a = await compareRust(root);
  expect(await compareRust(join(root, "a"))).toBe(a);
  expect(await compareRust(join(root, "a", "b"))).toBe(a);
}, 60000);

test("TS/Rust 对照：非仓库、路径大小写、正斜杠、空格与中文", async () => {
  const root = directory("混合 Case 与空格");
  const key = await compareRust(root);
  expect(await compareRust(root.toLowerCase())).toBe(key);
  expect(await compareRust(root.replaceAll("\\", "/"))).toBe(key);
}, 60000);

test("TS/Rust 对照：目录 junction", async () => {
  const target = directory("junction 目标");
  const junction = join(temporary, "junction 入口");
  const result = runTool("cmd.exe", ["/d", "/c", "mklink", "/J", junction, target]);
  if (result.error || result.status !== 0) {
    console.warn(`跳过 junction 对照：mklink /J 失败，退出码 ${result.status}，错误码 ${result.error?.message ?? "未提供"}`);
    test.skip("当前系统不能创建 junction", () => {});
    return;
  }
  expect(await compareRust(junction)).toBe(await compareRust(target));
}, 45000);

test("TS/Rust 对照：目录符号链接（mklink /D）", async () => {
  const target = directory("符号链接目标");
  const link = join(temporary, "目录符号链接");
  const result = runTool("cmd.exe", ["/d", "/c", "mklink", "/D", link, target]);
  if (result.error || result.status !== 0) {
    const reason = `mklink /D 不可用（需开发者模式或管理员权限），退出码 ${result.status}，错误码 ${(result.error as NodeJS.ErrnoException | undefined)?.code ?? "无"}`;
    console.warn(`跳过目录符号链接对照：${reason}`);
    test.skip(`目录符号链接对照未验证：${reason}`, () => {});
    return;
  }
  expect(await compareRust(link)).toBe(await compareRust(target));
}, 45000);

test("TS/Rust 对照：SUBST 映射盘符，结束后移除映射", async () => {
  const target = directory("SUBST 映射目标");
  const drive = [..."ZYXWVUTSRQPONMLKJI"].map((letter) => `${letter}:`).find((candidate) => !existsSync(`${candidate}\\`));
  if (!drive) {
    console.warn("跳过 SUBST 对照：没有可用的测试盘符。");
    test.skip("SUBST 对照未验证：没有可用的测试盘符", () => {});
    return;
  }
  const result = runTool("subst.exe", [drive, target]);
  if (result.error || result.status !== 0) {
    const reason = `无法创建 ${drive} 映射，退出码 ${result.status}，错误码 ${(result.error as NodeJS.ErrnoException | undefined)?.code ?? "无"}`;
    console.warn(`跳过 SUBST 对照：${reason}`);
    test.skip(`SUBST 对照未验证：${reason}`, () => {});
    return;
  }
  substMappings.add(drive);
  try {
    // 映射路径是否折叠到物理盘符以 Rust 实测为准，不预设两种路径必须同键。
    await compareRust(`${drive}\\`);
    await compareRust(target);
  } finally { removeSubstMapping(drive); }
  expect(existsSync(`${drive}\\`)).toBe(false);
}, 45000);

test("TS/Rust 对照：扩展长度路径", async () => {
  const target = directory("extended-path");
  expect(await compareRust(`\\\\?\\${target}`)).toBe(await compareRust(target));
}, 45000);

test("TS/Rust 对照：Windows 短文件名", async () => {
  const target = directory("long-directory-name-for-short-alias");
  const child = processes.add(Bun.spawn([
    join(process.env.SystemRoot ?? "C:\\Windows", "System32", "WindowsPowerShell", "v1.0", "powershell.exe"),
    "-NoProfile", "-NonInteractive", "-Command",
    '[Console]::OutputEncoding = [Text.UTF8Encoding]::new(); $fs = New-Object -ComObject Scripting.FileSystemObject; $fs.GetFolder($env:CXB_N1B_PROBE_PATH).ShortPath',
  ], { env: { ...privateTestEnv(), CXB_N1B_PROBE_PATH: target }, stdin: "ignore", stdout: "pipe", stderr: "pipe" }));
  const stdout = new Response(child.stdout).text();
  const stderr = new Response(child.stderr).text();
  expect(await processes.exited(child)).toBe(0);
  const short = (await bounded(stdout, 1000, "读取短路径超时")).trim();
  await bounded(stderr, 1000, "读取短路径错误输出超时");
  if (short.toLowerCase() === target.toLowerCase()) {
    console.warn("跳过短文件名对照：测试卷未为目录分配 8.3 别名。");
    test.skip("测试卷没有可用的短文件名", () => {});
    return;
  }
  expect(await compareRust(short)).toBe(await compareRust(target));
}, 45000);

test("TS/Rust 对照：本机可访问的 UNC 共享", async () => {
  const target = directory("UNC 中文路径");
  const unc = `\\\\localhost\\${target[0]}$${target.slice(2)}`;
  const child = processes.add(Bun.spawn([process.execPath, "-e",
    'import { statSync } from "node:fs"; try { if (!statSync(process.argv[1]).isDirectory()) process.exitCode = 2; } catch { process.exitCode = 2; }', unc,
  ], { env: privateTestEnv(), stdin: "ignore", stdout: "ignore", stderr: "ignore" }));
  const code = await processes.exited(child);
  if (code === 2) {
    console.warn("跳过真实 UNC 对照：当前用户无法访问本机测试卷的管理共享；纯字符串前缀用例仍会执行。");
    test.skip("本机 UNC 管理共享不可访问", () => {});
    return;
  }
  expect(code).toBe(0);
  // UNC 路径与盘符路径本就不是同一字符串；这里只断言各自与 Rust 一致。
  await compareRust(unc);
  await compareRust(`\\\\?\\UNC\\${unc.slice(2)}`);
}, 45000);
