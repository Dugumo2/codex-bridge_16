/** 第二批黑盒独占隔离；资源仅由用例 beforeAll/afterAll 打开和关闭。 */
import assert from "node:assert/strict";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import net from "node:net";
import { delimiter, join, resolve } from "node:path";
import { createRpcEndpoint, NO_RESPONSE, type RpcEndpoint } from "../../rpc";
import { createUpstreamIsolation, startManaged, stopManaged, removeIsolation, within, CLEANUP_MS, type ManagedProcess } from "../../test-support/upstream-isolation";
import { resolveWorkspace } from "../workspace";
import { createStallMock } from "./stall-upstream";

export interface StallCliResult { code: number | null; stdout: string; stderr: string }
export async function openStallRpc(pipe: string, deadline: number) {
  let socket: net.Socket | undefined;
  while (!socket) {
    if (Date.now() >= deadline) throw new Error("第二批管道连接超过期限");
    const candidate = net.createConnection(pipe); candidate.on("error", () => {});
    try {
      await within(new Promise<void>((done, reject) => { candidate.once("connect", done); candidate.once("error", reject); }), 500, "管道探测"); socket = candidate;
    } catch { candidate.destroy(); await Bun.sleep(20); }
  }
  const transport = socket;
  const rpc = createRpcEndpoint({ label: "第二批黑盒", requestTimeout: 5000, send: (data) => { transport.write(data); },
    downReason: () => transport.destroyed ? "夹具连接关闭" : null, overflowReason: "夹具协议超限", onOverflow: () => transport.destroy(),
    writeFailedPrefix: "夹具协议写入失败", log() {} });
  transport.setEncoding("utf8"); transport.on("data", (chunk: string) => rpc.feed(chunk));
  transport.on("error", () => rpc.fail("夹具管道错误")); transport.on("close", () => rpc.fail("夹具管道关闭"));
  rpc.onAnyRequest(() => NO_RESPONSE);
  return { rpc, close() { rpc.markClosed(); transport.destroy(); } };
}

export class StallHarness {
  isolation!: ReturnType<typeof createUpstreamIsolation>;
  workspace!: ReturnType<typeof resolveWorkspace>;
  owner!: RpcEndpoint;
  watcher!: RpcEndpoint;
  readonly events: Array<{ at: number; method: string; params: any }> = [];
  readonly processes: ManagedProcess[] = [];
  readonly connections: Array<Awaited<ReturnType<typeof openStallRpc>>> = [];
  runId = "";
  private shared?: ReturnType<typeof Bun.serve>;
  private core?: ReturnType<typeof createStallMock>;
  async open(shared = false) {
    const bin = process.env.CXB_BROKER_BIN;
    if (!bin || !existsSync(bin)) throw new Error("黑盒必须显式设置 CXB_BROKER_BIN 指向已编译 broker");
    this.isolation = createUpstreamIsolation("http://127.0.0.1:1/v1");
    const { isolation } = this;
    Object.assign(isolation.env, { CXB_BROKER_BIN: bin });
    const shim = join(isolation.root, "bin"); mkdirSync(shim);
    const fixture = resolve(import.meta.dir, "stall-upstream.ts");
    writeFileSync(join(shim, "codex.cmd"), `@echo off\r\n"${process.execPath}" "${fixture}" %*\r\nexit /b %errorlevel%\r\n`);
    let oldPath = "";
    for (const key of Object.keys(isolation.env)) if (key.toUpperCase() === "PATH") { oldPath = isolation.env[key] ?? ""; delete isolation.env[key]; }
    isolation.env.PATH = `${shim}${delimiter}${oldPath}`;
    if (shared) {
      const path = join(isolation.root, "shared.sock");
      const peers = new Set<any>();
      this.core = createStallMock((message) => {
        if (String(message.id ?? "").startsWith("stall-internal-")) return;
        for (const peer of peers) peer.send(JSON.stringify(message));
      });
      // 外部轮先存在，再让 broker 的 turn/start 并入，真实走 joined 定案。
      this.core.command("turn", { threadId: "root", action: "start", turnId: "foreign-turn", announce: false });
      const core = this.core;
      this.shared = Bun.serve({ unix: path, fetch(req, server) { if (server.upgrade(req, { data: undefined })) return; return new Response("仅测试 WebSocket", { status: 400 }); },
        websocket: { open(ws) { peers.add(ws); }, close(ws) { peers.delete(ws); }, message(_ws, data) { core.handle(JSON.parse(String(data))); } } });
      Object.assign(isolation.env, { CXB_SERVER: "shared", CXB_SERVER_SOCKET: path });
    }
    this.workspace = resolveWorkspace(isolation.cwd, isolation.env);
    const broker = startManaged([bin, "serve", "--endpoint", this.workspace.endpoint, "--cwd", isolation.cwd, "--idle-timeout", "120000"], isolation);
    this.processes.push(broker);
    for (const name of ["owner", "watcher"] as const) {
      const connection = await openStallRpc(this.workspace.pipePath, Date.now() + 12_000); this.connections.push(connection); this[name] = connection.rpc;
      const reply = await connection.rpc.request<any>("initialize", { clientInfo: { name: `ue2-${name}`, version: "1" }, capabilities: { experimentalApi: true, cxb: { protocolVersion: 1 } } });
      connection.rpc.notify("initialized"); assert.equal(reply.server.kind, shared ? "shared" : "private");
    }
    this.watcher.onAny((method, params) => this.events.push({ at: Date.now(), method, params }));
    await this.owner.request("thread/start", { cwd: isolation.cwd, approvalPolicy: "on-request", sandbox: "danger-full-access" });
    await this.watcher.request("broker/threads/watch", { threadId: "root" });
    return this;
  }
  control(name: string, params: any = {}): Promise<any> { return this.owner.request(`fixture/stall/${name}`, params); }
  sa1b(name: string, params: any = {}): Promise<any> { return this.owner.request(`fixture/sa1b/${name}`, params); }
  async state() { return this.control("state"); }
  async record() { return (await this.watcher.request<any>("broker/threads/watch", { threadId: "root" })).run; }
  updated() { return this.events.filter((event) => event.method === "broker/run/updated" && event.params.runId === this.runId); }
  async until(check: () => Promise<boolean> | boolean, timeout = 8000, label = "黑盒条件未满足") {
    const deadline = Date.now() + timeout;
    while (Date.now() < deadline) { if (await check()) return; await Bun.sleep(20); }
    throw new Error(label);
  }
  async cli(args: string[], timeout = 20_000) {
    const proc = this.startCli(args);
    const code = await within(proc.closed, timeout, `CLI ${args[0]}`);
    return { code, ...proc.output() };
  }
  startCli(args: string[]) {
    const proc = startManaged([process.execPath, resolve(import.meta.dir, "../main.ts"), ...args, "--dir", this.isolation.cwd], this.isolation);
    proc.child.stdin.end(); this.processes.push(proc); return proc;
  }
  run(args: string[] = []) {
    return this.startCli(["run", "本地无进展契约测试", "--resume", "root", "--approval", "on-request", "--sandbox", "danger-full-access",
      ...(!args.includes("--stall-timeout") ? ["--stall-timeout", "3"] : []), "--timeout", "90", ...args]);
  }
  async started() {
    await this.until(async () => {
      const run = await this.record(); this.runId = run?.runId ?? "";
      const cli = this.processes.at(-1);
      if (!this.runId && cli?.child.exitCode !== null) throw new Error(`CLI 启动失败：${JSON.stringify(cli?.output())}`);
      return !!this.runId && run.state === "running" && !!(await this.state()).threads.root?.turns.length;
    }, 10_000, "CLI 没有建立运行");
    return this.record();
  }
  async persisted() {
    const path = join(this.workspace.workspaceDir, "runs", `${this.runId}.json`);
    let record: any;
    const current = await this.record();
    await this.until(() => {
      if (!existsSync(path)) return false;
      record = JSON.parse(readFileSync(path, "utf8")).payload;
      return record?.runId === this.runId && record?.diagnosticRevision === current?.diagnosticRevision && record?.stopOutcome === current?.stopOutcome && record?.state === current?.state;
    }, 5000, "最终诊断尚未落盘");
    return record;
  }
  async close() {
    const started = Date.now(), errors: string[] = [];
    for (const connection of this.connections) connection.close();
    const clean = async (operation: () => Promise<unknown> | unknown) => {
      try { await within(Promise.resolve().then(operation), Math.max(1, CLEANUP_MS - (Date.now() - started)), "第二批黑盒清理"); }
      catch (error) { errors.push(String(error)); }
    };
    for (const proc of this.processes.toReversed()) await clean(() => stopManaged(proc));
    this.core?.close(); await clean(() => this.shared?.stop(true));
    if (this.isolation) await clean(() => removeIsolation(this.isolation.root));
    assert.deepEqual(errors, [], "第二批黑盒清理失败");
  }
}
