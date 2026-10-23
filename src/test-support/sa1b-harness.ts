import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import type { Subprocess } from "bun";
import { TestClient, endpointFor, writeMockCodexScript, spawnBroker, waitForSocket, removeDirWithRetry, envWithPathPrefix } from "./broker-harness";
import { resolveWorkspace } from "../cxb/workspace";

export const SA1B_ROOT = "root";
/** 每个场景独立进程、目录、连接；失败路径同样清理。 */
export class Sa1bHarness {
  readonly tempDir = mkdtempSync(join(tmpdir(), "sa1b-e-"));
  readonly spawnedProcesses: Subprocess[] = [];
  readonly clients: TestClient[] = [];
  readonly home = join(this.tempDir, "home");
  readonly socket = resolveWorkspace(this.tempDir, { ...process.env, CXB_HOME: this.home }).pipePath;
  owner!: TestClient;
  runId = "";
  firstTurn = "";
  private drains: Promise<string>[] = [];
  async open(options: { pageSize?: number } = {}) {
    const module = pathToFileURL(join(import.meta.dir, "sa1b-mock.ts")).href;
    writeMockCodexScript(this.tempDir, `import { createSa1bMock } from ${JSON.stringify(module)};
const mock = createSa1bMock((message, connectionId) => { if (connectionId === undefined || connectionId === "broker") process.stdout.write(JSON.stringify(message) + "\\n"); }, ${JSON.stringify(options)});
let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", chunk => { buffer += chunk; let end; while ((end = buffer.indexOf("\\n")) >= 0) { const line = buffer.slice(0,end); buffer = buffer.slice(end+1); if (line.trim()) mock.handle(JSON.parse(line)); } });
process.stdin.on("end", () => process.exit(0));`);
    await this.launch();
    this.owner = await this.connect();
    return this;
  }
  async launch() {
    const proc = spawnBroker(this, endpointFor(this.socket), this.tempDir, { cwd: this.tempDir, env: { CXB_HOME: this.home } });
    this.drains.push(new Response(proc.stdout as ReadableStream).text(), new Response(proc.stderr as ReadableStream).text());
    await waitForSocket(this.socket);
  }
  async connect() {
    const client = await TestClient.connectAndInit(this.socket, { cxb: true }); this.clients.push(client); return client;
  }
  async start(extra: Record<string, any> = {}) {
    const response: any = await this.owner.request("turn/start", { threadId: SA1B_ROOT, input: [{ type: "text", text: "请完成测试任务" }], model: "original-model", effort: "high", ...extra });
    this.runId = response.runId;
    if (!this.runId) throw new Error("夹具前置失败：未返回 runId");
    await this.until(async () => {
      const state = await this.state(); this.firstTurn = state.threads.root?.turns[0]?.id ?? "";
      return this.firstTurn !== "" && (await this.watch()).thread?.state === "running";
    }, 8000, "初始自有轮未开始");
    return this;
  }
  control(name: string, params: any = {}): Promise<any> { return this.owner.request(`fixture/sa1b/${name}`, params, 6000); }
  state(): Promise<any> { return this.control("state"); }
  watch(): Promise<any> { return this.owner.request("broker/threads/watch", { threadId: SA1B_ROOT }); }
  async record(): Promise<any> {
    return (await this.watch()).run;
  }
  notes(method: string): any[] { return this.owner.messages.filter(x => x.method === method && x.id === undefined).map(x => x.params); }
  ended(): any[] { return this.notes("broker/run/ended").map(x => x.run).filter(x => x?.runId === this.runId); }
  async spawn(child = "child", parent = SA1B_ROOT, parentTurn = this.firstTurn, extra: any = {}): Promise<string> {
    return (await this.control("spawn", { parentThreadId: parent, parentTurnId: parentTurn, childThreadId: child, ...extra })).turnId;
  }
  finish(threadId: string, output = "阶段输出", status = "completed", turnId?: string) {
    return this.control("turn", { threadId, turnId, action: "complete", output, status });
  }
  async starts(): Promise<any[]> {
    return (await this.state()).calls.filter((x: any) => ["turn/start", "turn/steer"].includes(x.method) && x.params?.threadId === SA1B_ROOT);
  }
  async until(condition: () => Promise<boolean>, timeout = 4000, label = "条件未成立") {
    const end = Date.now() + timeout;
    while (Date.now() < end) { if (await condition()) return; await Bun.sleep(20); }
    throw new Error(label);
  }
  async observe(ms = 150) { await this.control("barrier"); await Bun.sleep(ms); }
  /** 等待汇总请求或提前结束；后者立即返回，让断言指出真实失败而不是通用超时。 */
  async reconciliation(timeout = 2000): Promise<any | undefined> {
    let request: any;
    await this.until(async () => { request = (await this.starts()).slice(1).find(x => x.params.clientUserMessageId); return !!request || this.ended().length > 0; }, timeout, "功能未实现或汇总未发出").catch(error => {
      if (!String(error).includes("汇总未发出")) throw error;
    });
    return request;
  }
  async restart(options: { trimTurns?: number } = {}) {
    // 落盘栅栏：避免用进程强杀与异步存储竞争伪造恢复故障。
    const workspace = resolveWorkspace(this.tempDir, { ...process.env, CXB_HOME: this.home });
    const path = join(workspace.workspaceDir, "runs", `${this.runId}.json`);
    await this.until(async () => existsSync(path) && JSON.parse(readFileSync(path, "utf8")).payload?.runId === this.runId, 3000, "运行尚未落盘");
    for (const client of this.clients) await client.close();
    for (const proc of this.spawnedProcesses) { proc.kill(); await proc.exited; }
    if (options.trimTurns !== undefined) {
      // 在测试私有存储中构造被裁剪的历史；独立计数与总轮数保持原始值。
      const envelope = JSON.parse(readFileSync(path, "utf8"));
      envelope.payload.turns = envelope.payload.turns.slice(-options.trimTurns);
      writeFileSync(path, JSON.stringify(envelope));
    }
    await this.launch(); this.owner = await this.connect();
  }
  async cli(args: string[], timeout = 10000) {
    const proc = Bun.spawn([process.execPath, join(import.meta.dir, "../cxb/main.ts"), ...args, "--dir", this.tempDir], {
      cwd: this.tempDir, stdin: "ignore", stdout: "pipe", stderr: "pipe",
      env: { ...envWithPathPrefix(this.tempDir), CXB_HOME: this.home, CODEX_HOME: join(this.tempDir, "cli-codex-home"), USERPROFILE: this.tempDir, HOME: this.tempDir },
    });
    this.spawnedProcesses.push(proc);
    let timedOut = false;
    const timer = setTimeout(() => { timedOut = true; proc.kill(); }, timeout);
    try {
      const [stdout, stderr, code] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited]);
      if (timedOut) throw new Error(`CLI ${args[0]} 超过${timeout}ms`);
      return { code, stdout, stderr };
    } finally { clearTimeout(timer); }
  }
  async close() {
    for (const client of this.clients) await client.close().catch(() => {});
    for (const proc of this.spawnedProcesses) { try { proc.kill(); } catch {} }
    await Promise.race([Promise.all(this.spawnedProcesses.map(p => p.exited)), Bun.sleep(5000)]);
    await Promise.race([Promise.all(this.drains), Bun.sleep(1000)]);
    await removeDirWithRetry(this.tempDir);
  }
}
