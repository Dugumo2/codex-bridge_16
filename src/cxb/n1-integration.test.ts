// 工作区、连接层和入口联调；仅使用临时目录与模拟 app-server，真 broker 不重新编译。
// 候选 PID 在 launch 日志返回时登记；版本查询和 launch 进程在创建时登记，模拟进程启动时写登记文件。
import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test";
import childProcess from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BROKER_BIN, createMockCodex, envWithPathPrefix } from "../test-support/broker-harness";
import { CxbError, type BrokerConnection, type WorkspaceInfo } from "./contracts";
import { brokerVersionOf, connectBroker } from "./broker-conn";
import { resolveWorkspace } from "./workspace";
import { Processes, alive, bounded, privateEnv, removeTemp, until } from "./test-fixtures/main-test-support";

const CONNECT_LIMIT_MS = 30_000;
const TEST_LIMIT_MS = 65_000;
let root: string;
let env: NodeJS.ProcessEnv;
let registry: string;
let processes: Processes;
let processSpy: ReturnType<typeof spyOn<typeof childProcess, "execFile">>;
let syncSpy: ReturnType<typeof spyOn<typeof childProcess, "spawnSync">>;
const opened: BrokerConnection[] = [];
const stopped = new Set<number>();
const pending = new Set<Promise<BrokerConnection>>();
const controllers = new Set<AbortController>();
const connectionWorkspaces = new Map<BrokerConnection, WorkspaceInfo>();
const candidates = new Map<number, { workspace: WorkspaceInfo; version: string }>();
const shutdownEndpoints = new Set<string>();

beforeEach(() => {
  root = mkdtempSync(join(tmpdir(), "cxb-n1-int-"));
  processes = new Processes();
  registry = join(root, "pids.txt");
  writeFileSync(registry, "");
  const execute = childProcess.execFile;
  processSpy = spyOn(childProcess, "execFile").mockImplementation(((...args: Parameters<typeof execute>) => {
    const child = execute(...args);
    processes.add(child.pid);
    return child;
  }) as typeof execute);
  const executeSync = childProcess.spawnSync;
  syncSpy = spyOn(childProcess, "spawnSync").mockImplementation(((...args: Parameters<typeof executeSync>) => {
    const child = executeSync(...args);
    processes.add(child.pid);
    return child;
  }) as typeof executeSync);
  const mockDir = createMockCodex(root);
  const script = join(mockDir, "codex-mock.ts");
  // 模拟程序的父进程是 codex.cmd 的宿主（若平台直接执行则是 broker），一并登记。
  writeFileSync(script, `import { appendFileSync as recordPid } from "node:fs";\nrecordPid(${JSON.stringify(registry)}, process.ppid + "\\n" + process.pid + "\\n");\n${readFileSync(script, "utf8").replace(/^#![^\n]*\n/, "")}`);
  for (const name of ["cxb-home", "codex-home", "profile"]) mkdirSync(join(root, name));
  env = {
    ...privateEnv(envWithPathPrefix(mockDir)),
    CXB_HOME: join(root, "cxb-home"), CODEX_HOME: join(root, "codex-home"),
    USERPROFILE: join(root, "profile"), HOME: join(root, "profile"),
    CXB_BROKER_BIN: BROKER_BIN, CXB_TEST_PID_REGISTRY: registry,
  };
});

afterEach(async () => {
  const errors: unknown[] = [];
  const attempt = async (action: () => void | Promise<unknown>) => {
    try { await action(); } catch (error) { errors.push(error); }
  };
  for (const controller of controllers) controller.abort();
  // 每个连接自带总期限；先等全部分支收束，之后不会再出现迟到句柄或新 PID。
  await attempt(() => bounded(Promise.allSettled([...pending]), 36_000, "连接分支未在清理前收束"));
  const seen = new Set(stopped);
  for (const connection of opened) {
    if (!seen.has(connection.handshake.epoch)) {
      seen.add(connection.handshake.epoch);
      await attempt(() => shutdown(connection));
    }
    await attempt(() => bounded(connection.close(), 3000, "关闭连接超时"));
  }
  // 取消只结束 CLI 连接，不会关闭已拉起的共享 broker；补发 shutdown 后才能判定 PID 泄漏。
  await attempt(() => shutdownUnownedCandidates());
  await attempt(() => { processes.readRegistry(registry); });
  await attempt(() => processes.cleanup());
  processSpy.mockRestore();
  syncSpy.mockRestore();
  await attempt(async () => {
    if (!processes.allExited()) throw new Error("仍有存活进程，保留临时目录供排查");
    await removeTemp(root);
  });
  opened.length = 0;
  stopped.clear();
  pending.clear();
  controllers.clear();
  connectionWorkspaces.clear();
  candidates.clear();
  shutdownEndpoints.clear();
  if (errors.length) throw new AggregateError(errors, "联调测试资源清理失败");
  // 36 秒分支收束、既有连接关闭、并行补关候选（最多 24 秒）、15 秒 PID 回收及 5 秒目录清理。
}, 120_000);

/** broker.json 外壳是 {epoch, revision, payload}；仅用于断言，不作为唯一的 PID 登记来源。 */
function readBrokerJson(workspaceDir: string): { pid: number; endpoint: string; epoch: number } {
  return JSON.parse(readFileSync(join(workspaceDir, "broker.json"), "utf8")).payload;
}

async function shutdown(connection: BrokerConnection): Promise<void> {
  await bounded(connection.rpc.request("broker/shutdown", {}, { timeoutMs: 5000 }), 6000, "关闭 broker 超时");
  stopped.add(connection.handshake.epoch);
  const workspace = connectionWorkspaces.get(connection);
  if (workspace) shutdownEndpoints.add(workspace.endpoint);
}

/** 每个端点只补关一次；仅处理本用例登记的候选，不能在清理时另行拉起 broker。 */
async function shutdownUnownedCandidates(): Promise<number[]> {
  if (pending.size) throw new Error("连接任务尚未收束，不能开始候选清理");
  const groups = new Map<string, { workspace: WorkspaceInfo; version: string; pids: number[] }>();
  for (const [pid, candidate] of candidates) {
    if (!alive(pid) || shutdownEndpoints.has(candidate.workspace.endpoint)) continue;
    const group = groups.get(candidate.workspace.endpoint) ?? { ...candidate, pids: [] };
    group.pids.push(pid);
    groups.set(candidate.workspace.endpoint, group);
  }
  const results = await Promise.allSettled([...groups.values()].map(async ({ workspace, version, pids }) => {
    let connection: BrokerConnection | undefined;
    try {
      const deadline = Date.now() + 15_000;
      for (;;) {
        let pipeNotReady = false;
        try {
          connection = await connectBroker({
            workspace, brokerBin: BROKER_BIN, expectedBrokerVersion: version,
            timeoutMs: Math.max(1, deadline - Date.now()), env,
            log(line) {
              if (line.startsWith("拉起 broker")) {
                pipeNotReady = true;
                throw new CxbError("INTERRUPTED", "清理阶段不拉起新 broker");
              }
            },
          });
          break;
        } catch (error) {
          // launch 返回 PID 早于 serve 管道就绪；仍存活的候选可在同一总期限内继续等。
          if (!pipeNotReady || !pids.some(alive) || Date.now() >= deadline) throw error;
          await Bun.sleep(25);
        }
      }
      connectionWorkspaces.set(connection, workspace);
      await shutdown(connection);
      return pids;
    } catch (error) {
      // 候选可能在检查存活后自行退出；这种情况下不需要重启它再关闭。
      if (pids.some(alive)) throw error;
      return [];
    } finally {
      if (connection) {
        try { await bounded(connection.close(), 3000, "关闭候选清理连接超时"); }
        finally { connectionWorkspaces.delete(connection); }
      }
    }
  }));
  const errors = results.filter((r) => r.status === "rejected").map((r) => r.reason);
  if (errors.length) throw new AggregateError(errors, "补关候选 broker 失败");
  return results.flatMap((r) => r.status === "fulfilled" ? r.value : []);
}

function connect(ws: WorkspaceInfo, controller = new AbortController(), onCandidate?: (pid: number) => void): Promise<BrokerConnection> {
  controllers.add(controller);
  const work = (async () => {
    const version = await brokerVersionOf(BROKER_BIN);
    if (controller.signal.aborted) throw new CxbError("INTERRUPTED", "测试已取消连接");
    const connection = await connectBroker({
      workspace: ws, brokerBin: BROKER_BIN, expectedBrokerVersion: version,
      timeoutMs: CONNECT_LIMIT_MS, idleTimeoutMs: 60_000, env,
      signal: controller.signal,
      log(line) {
        processes.log(line);
        const match = /候选进程已拉起：pid=(\d+)/.exec(line);
        if (match) {
          const pid = Number(match[1]);
          candidates.set(pid, { workspace: ws, version });
          onCandidate?.(pid);
        }
      },
    });
    opened.push(connection);
    connectionWorkspaces.set(connection, ws);
    return connection;
  })();
  pending.add(work);
  void work.then(() => pending.delete(work), () => pending.delete(work));
  return work;
}

async function connectTogether(workspaces: WorkspaceInfo[]): Promise<BrokerConnection[]> {
  return settleConnections(workspaces.map((workspace) => (controller) => connect(workspace, controller)));
}

async function settleConnections(branches: Array<(controller: AbortController) => Promise<BrokerConnection>>): Promise<BrokerConnection[]> {
  const controller = new AbortController();
  const results = await Promise.allSettled(branches.map((branch) => branch(controller).catch((error) => {
    controller.abort();
    throw error;
  })));
  const failures = results.filter((r) => r.status === "rejected").map((r) => r.reason);
  if (failures.length) throw new AggregateError(failures, "并发连接失败，全部分支已收束");
  return results.map((r) => (r as PromiseFulfilledResult<BrokerConnection>).value);
}

describe("工作区与 broker 连接联调（N1）", () => {
  test("拉起 broker 的落盘目录与工作区键一致，握手版本匹配", async () => {
    const dir = join(root, "项目 甲");
    mkdirSync(dir);
    const ws = resolveWorkspace(dir, env);
    const c = await connect(ws);
    expect(c.launched).toBe(true);
    expect(c.handshake.protocolVersion).toBe(1);
    expect(c.handshake.brokerVersion).toBe(await brokerVersionOf(BROKER_BIN));
    expect(readdirSync(join(root, "cxb-home", "workspaces"))).toEqual([ws.key]);
    const info = readBrokerJson(ws.workspaceDir);
    expect(info.endpoint).toBe(ws.endpoint);
    expect(info.epoch).toBe(c.handshake.epoch);
    expect(processes.pids.has(info.pid)).toBe(true);
  }, TEST_LIMIT_MS);

  test("同一临时仓库的根与子目录复用一个 broker", async () => {
    const repo = join(root, "repo");
    mkdirSync(join(repo, "a", "b"), { recursive: true });
    const init = childProcess.spawnSync("git", ["init", "-q"], { cwd: repo, timeout: 30_000, windowsHide: true });
    expect(init.status).toBe(0);
    const top = resolveWorkspace(repo, env);
    const sub = resolveWorkspace(join(repo, "a", "b"), env);
    expect(sub.key).toBe(top.key);
    expect(sub.pipePath).toBe(top.pipePath);
    const c1 = await connect(sub);
    const c2 = await connect(top);
    expect(c1.launched).toBe(true);
    expect(c2.launched).toBe(false);
    expect(c2.handshake.epoch).toBe(c1.handshake.epoch);
  }, 100_000);

  test("同时连接冷工作区：epoch 相同，关闭 broker 后另一连接断开", async () => {
    const ws = resolveWorkspace(root, env);
    const [c1, c2] = await connectTogether([ws, ws]);
    expect(c1.handshake.epoch).toBe(c2.handshake.epoch);
    let unsubscribe = () => {};
    const closed = new Promise<void>((done) => { unsubscribe = c2.rpc.onClose(done); });
    try {
      await shutdown(c1);
      await bounded(closed, 15_000, "另一连接没有收到 broker 退出通知");
    } finally { unsubscribe(); }
  }, TEST_LIMIT_MS);

  test("候选已创建后另一并发分支才失败：取消收束后补发 shutdown，不误判为泄漏", async () => {
    const ws = resolveWorkspace(root, env);
    const events: string[] = [];
    let candidatePid = 0;
    let failOther!: (error: Error) => void;
    const other = new Promise<BrokerConnection>((_, reject) => { failOther = reject; });
    await expect(settleConnections([
      (controller) => connect(ws, controller, (pid) => {
        candidatePid = pid;
        events.push("候选已创建");
        // 在 launch 的 PID 回报控制点放行失败分支，无需靠延时猜测两个分支的调度。
        events.push("另一分支失败");
        failOther(new CxbError("LOCAL_ERROR", "受控并发分支失败"));
      }),
      () => bounded(other, 36_000, "候选创建控制点未到达"),
    ])).rejects.toThrow("全部分支已收束");
    expect(events).toEqual(["候选已创建", "另一分支失败"]);
    expect(pending.size).toBe(0);
    expect(opened).toHaveLength(0);
    expect(candidates.get(candidatePid)?.workspace.endpoint).toBe(ws.endpoint);
    expect(alive(candidatePid)).toBe(true);
    expect(await shutdownUnownedCandidates()).toContain(candidatePid);
    await until(() => !alive(candidatePid), 10_000, "候选收到 shutdown 后没有退出");
    expect(shutdownEndpoints.has(ws.endpoint)).toBe(true);
    expect(await shutdownUnownedCandidates()).toEqual([]);
  }, TEST_LIMIT_MS);

  test("混合大小写的共享服务变量被清除，真 broker 使用私有模拟服务", async () => {
    const polluted = {
      ...env, cXb_SeRvEr: "shared", CxB_sErVeR_sOcKeT: join(root, "不存在的共享服务"),
      cxb_SERVER: "shared", cxb_Server_Socket: join(root, "不存在的共享服务二"),
    };
    env = privateEnv(polluted);
    expect(Object.keys(env).filter((key) => /^cxb_server_socket$/i.test(key))).toEqual([]);
    expect(Object.keys(env).filter((key) => /^cxb_server$/i.test(key))).toEqual(["CXB_SERVER"]);
    const c = await connect(resolveWorkspace(root, env));
    expect(c.handshake.server.kind).toBe("private");
    expect(readFileSync(registry, "utf8").trim()).not.toBe("");
  }, TEST_LIMIT_MS);

  test("注入处理器经 runCli 完成解析、迁移、真连接、health、清理与 finish 后自然退出", async () => {
    const legacy = join(env.USERPROFILE!, ".cxb-legacy");
    mkdirSync(legacy);
    writeFileSync(join(legacy, "config.json"), JSON.stringify({ model: "mock-model" }));
    const ws = resolveWorkspace(root, env);
    try {
      const result = await processes.run(process.execPath, [join(import.meta.dir, "test-fixtures/main-smoke.ts")], {
        cwd: root, env, limitMs: 45_000,
      });
      expect(result.code).toBe(0);
      expect(JSON.parse(result.stdout)).toMatchObject({ ok: true, command: "doctor", key: ws.key, launched: true });
      expect(JSON.parse(result.stdout).health).toBeObject();
      expect(result.stderr).toContain("入口清理与 finish 已完成");
      expect(JSON.parse(readFileSync(join(ws.dataRoot, "config.json"), "utf8"))).toEqual({ model: "mock-model" });
      expect(existsSync(join(ws.dataRoot, "migrated-from-cxb.json"))).toBe(true);
      expect(alive(readBrokerJson(ws.workspaceDir).pid)).toBe(true);
    } finally {
      // CLI 只关连接；测试负责关闭仍在运行的 broker，不靠它的 idle 超时结束。
      processes.readRegistry(registry);
      if (existsSync(join(ws.workspaceDir, "broker.json")) && alive(readBrokerJson(ws.workspaceDir).pid)) await connect(ws);
    }
  }, 100_000);

  test.each([
    { mode: "log-fail", code: 1, calls: 1, events: ["连接开始", "日志失败", "关闭：true"] },
    { mode: "cancel", code: 0, calls: 1, events: ["连接开始", "取消"] },
    { mode: "late", code: 0, calls: 1, events: ["连接开始", "取消", "关闭：true"] },
    { mode: "version", code: 0, calls: 0, events: [] },
    { mode: "reuse", code: 0, calls: 1, events: ["连接开始", "关闭：true"] },
    { mode: "slow-cancel", code: 0, calls: 1, events: ["连接开始", "取消", "launch 已收束", "关闭：true"] },
  ])("入口生命周期：%j", async ({ mode, code, calls, events }) => {
    const result = await processes.run(process.execPath, [join(import.meta.dir, "test-fixtures/main-lifecycle.ts"), mode], {
      cwd: root, env, limitMs: 20_000,
    });
    expect(result.code).toBe(code);
    expect(JSON.parse(result.stdout)).toEqual({ code, calls, events, afterCleanup: "INTERRUPTED" });
    if (mode === "log-fail") expect(result.stderr).toContain("模拟 verbose 写出失败");
    if (mode === "slow-cancel") expect(result.stderr).not.toContain("清理资源超时");
  }, 40_000);

  test("入口生命周期：信号清理时先等命令收尾，再关闭连接", async () => {
    const result = await processes.run(process.execPath, [join(import.meta.dir, "test-fixtures/main-lifecycle.ts"), "signal-order"], {
      cwd: root, env, limitMs: 20_000,
    });
    expect(result.code).toBe(130);
    expect(JSON.parse(result.stdout.trim().split(/\r?\n/).at(-1)!).events).toEqual(["连接开始", "命令收尾", "关闭：true"]);
  }, 40_000);
});
