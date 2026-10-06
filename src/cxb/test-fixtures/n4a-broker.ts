import { existsSync, mkdirSync, mkdtempSync, readFileSync, watch, type FSWatcher } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BROKER_BIN, envWithPathPrefix, writeMockCodexScript } from "../../test-support/broker-harness";
import { brokerVersionOf, connectBroker } from "../broker-conn";
import type { BrokerConnection } from "../contracts";
import { resolveWorkspace } from "../workspace";
import { bounded, Processes, removeTemp } from "./main-test-support";
import { context } from "./n4a-unit";

export class N4Broker {
  dir = "";
  connection: BrokerConnection | null = null;
  watcher: FSWatcher | null = null;
  processes = new Processes();
  env: NodeJS.ProcessEnv = {};
  failure: unknown;
  async setup() {
    this.dir = mkdtempSync(join(tmpdir(), "cxb-n4a-real-"));
    for (const name of ["mock", "ws", "profile", "data", "codex"]) mkdirSync(join(this.dir, name));
    writeMockCodexScript(join(this.dir, "mock"), readFileSync(join(import.meta.dir, "n4a-codex.ts"), "utf8"));
    const registry = join(this.dir, "mock-pids");
    // 在拉起之前监听登记文件，每个模拟进程启动即写入 PID；不依赖清理阶段发现句柄。
    this.watcher = watch(this.dir, () => { try { this.processes.readRegistry(registry); } catch (error) { this.failure = error; } });
    this.watcher.on("error", (error) => { this.failure = error; });
    this.env = Object.fromEntries(Object.entries(envWithPathPrefix(join(this.dir, "mock"))).filter(([key]) => {
      const name = key.toUpperCase();
      return !["CXB_HOME", "CODEX_HOME", "HOME", "USERPROFILE", "CXB_SERVER"].includes(name)
        && !/SOCKET|ENDPOINT/.test(name) && !name.startsWith("MOCK_");
    }));
    Object.assign(this.env, { CXB_HOME: join(this.dir, "data"), CODEX_HOME: join(this.dir, "codex"),
      USERPROFILE: join(this.dir, "profile"), HOME: join(this.dir, "profile"), CXB_SERVER: "private",
      CXB_BROKER_BIN: BROKER_BIN, MOCK_N4A_REGISTRY: registry });
    this.connection = await connectBroker({ workspace: this.workspace, brokerBin: BROKER_BIN,
      expectedBrokerVersion: await brokerVersionOf(BROKER_BIN), timeoutMs: 15_000, env: this.env,
      log: (line) => this.processes.log(line) });
    this.processes.readRegistry(registry);
  }
  get workspace() { return resolveWorkspace(join(this.dir, "ws"), this.env); }
  async request<T = any>(method: string, params: unknown = {}): Promise<T> {
    return bounded(this.connection!.rpc.request<T>(method, params, { timeoutMs: 5000 }), 6000, `真 broker 请求超时：${method}`);
  }
  command(args: string[]) {
    const t = context(this.workspace.workspaceDir, args);
    t.ctx.workspace = this.workspace;
    t.ctx.connect = async () => this.connection!;
    return t;
  }
  async cli(args: string[]) {
    return this.processes.run(process.execPath, [join(import.meta.dir, "..", "main.ts"), ...args, "-d", this.workspace.root],
      { cwd: this.dir, env: this.env, limitMs: 20_000, onStderr: (chunk) => this.processes.log(chunk) });
  }
  async cleanup() {
    const errors: unknown[] = [];
    try { if (this.connection) await this.request("broker/shutdown"); } catch (error) { errors.push(error); }
    try { if (this.connection) await bounded(this.connection.close(), 2000, "关闭连接超时"); } catch (error) { errors.push(error); }
    this.connection = null;
    try { this.watcher?.close(); } catch (error) { errors.push(error); }
    this.watcher = null;
    if (this.failure) errors.push(this.failure);
    try { await this.processes.cleanup(); } catch (error) { errors.push(error); }
    try {
      if (!this.processes.allExited()) throw new Error("仍有进程，保留现场");
      if (this.dir) { await removeTemp(this.dir); if (existsSync(this.dir)) throw new Error("临时目录仍存在"); }
    } catch (error) { errors.push(error); }
    if (errors.length) throw new AggregateError(errors, "甲组真 broker 清理失败");
  }
}
