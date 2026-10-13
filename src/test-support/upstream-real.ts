/** 真实 Codex 0.160.0 的 stdio 驱动；重试和 interrupt 均不作传输改写。 */
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { startUpstreamModel, type ModelRequest, type UpstreamScenario } from "./upstream-model-server";
import { CLEANUP_MS, TRACE_ROOT, codexCommand, createUpstreamIsolation, removeIsolation, startManaged, stopManaged,
  verifyEffectiveConfig, within, type ManagedProcess } from "./upstream-isolation";

export interface ProtocolEvent { at: number; elapsedMs: number; direction: "sent" | "received"; message: Record<string, any> }
export interface RealTrace {
  scenario: UpstreamScenario; version: string; startedAt: number; deadlineMs: number; cleanupMs: number;
  isolation: { provider: string; baseUrl: string; supportsWebsockets: false; defaultRetries: true; configVerified: boolean };
  events: ProtocolEvent[]; requests: ModelRequest[]; violations: string[]; stderr: string;
  threadId?: string; turnId?: string; interrupt?: { sentAt: number; acknowledgedAt?: number; completedAt?: number };
  completed?: Record<string, any>; failure?: string; cleaned?: boolean;
}

export class UpstreamStdio {
  private nextId = 1;
  private buffer = "";
  private pending = new Map<number, { resolve: (value: any) => void; reject: (error: Error) => void }>();
  private failure?: Error;
  constructor(readonly proc: ManagedProcess, readonly events: ProtocolEvent[], readonly startedAt: number) {
    proc.onStdout((chunk: string) => {
      this.buffer += chunk;
      let index: number;
      while ((index = this.buffer.indexOf("\n")) >= 0) {
        const line = this.buffer.slice(0, index).trim(); this.buffer = this.buffer.slice(index + 1);
        if (!line) continue;
        try {
          const message = JSON.parse(line);
          this.events.push({ at: Date.now(), elapsedMs: Date.now() - startedAt, direction: "received", message });
          if (message.id !== undefined && !message.method) {
            const entry = this.pending.get(message.id); this.pending.delete(message.id);
            if (message.error) entry?.reject(new Error(JSON.stringify(message.error))); else entry?.resolve(message.result);
          } else if (message.id !== undefined && message.method) {
            // 本路不实现审批或认证恢复；拒绝意外服务端请求，而非提供真实信息。
            this.send({ id: message.id, error: { code: -32601, message: "UE 本地测试不提供外部交互" } });
          }
        } catch (error) { this.failure = new Error(`app-server 输出不是有效协议：${String(error)}`); }
      }
    });
    void proc.closed.catch((error) => { this.failure = error; });
    void proc.closed.then(() => {
      this.failure ??= new Error("app-server 已退出");
      for (const entry of this.pending.values()) entry.reject(this.failure);
      this.pending.clear();
    }).catch(() => {});
  }
  send(message: Record<string, unknown>) {
    this.events.push({ at: Date.now(), elapsedMs: Date.now() - this.startedAt, direction: "sent", message });
    this.proc.child.stdin!.write(`${JSON.stringify(message)}\n`);
  }
  async request(method: string, params: unknown, timeoutMs = 10_000): Promise<any> {
    if (this.failure) throw this.failure;
    const id = this.nextId++;
    const reply = new Promise<any>((resolve, reject) => this.pending.set(id, { resolve, reject }));
    this.send({ id, method, params });
    try { return await within(reply, timeoutMs, method); } finally { this.pending.delete(id); }
  }
  async until(check: () => boolean, deadline: number, label: string) {
    while (!check()) {
      if (this.failure) throw this.failure;
      if (Date.now() >= deadline) throw new Error(`${label} 超过场景总期限`);
      await Bun.sleep(5);
    }
  }
}

export interface RealOptions { deadlineMs?: number; traceName?: string; failures?: number; interruptOn?: "request" | "retry" }
export async function runUpstreamReal(scenario: UpstreamScenario, options: RealOptions = {}): Promise<RealTrace> {
  const deadlineMs = options.deadlineMs ?? (scenario === "500" ? 70_000 : 30_000);
  const startedAt = Date.now(), deadline = startedAt + deadlineMs;
  const server = await startUpstreamModel(scenario, { failures: options.failures });
  const isolation = createUpstreamIsolation(server.baseUrl);
  const trace: RealTrace = { scenario, startedAt, version: "", deadlineMs, cleanupMs: CLEANUP_MS,
    isolation: { provider: "ue_script", baseUrl: server.baseUrl, supportsWebsockets: false, defaultRetries: true, configVerified: false },
    events: [], requests: server.requests, violations: server.violations, stderr: "" };
  let proc: ManagedProcess | undefined;
  try {
    const version = startManaged(codexCommand(["--version"]), isolation);
    try { await within(version.closed, 5000, "Codex 版本查询"); trace.version = version.output().stdout.trim(); }
    finally { await stopManaged(version); }
    if (trace.version !== "codex-cli 0.160.0") throw new Error(`Codex 版本不符：${trace.version}`);
    proc = startManaged(codexCommand(["app-server"]), isolation);
    const rpc = new UpstreamStdio(proc, trace.events, startedAt);
    await rpc.request("initialize", { clientInfo: { name: "ue-local-real", title: "上游本地复现", version: "1" }, capabilities: { experimentalApi: true } });
    rpc.send({ method: "initialized" });
    const config = await rpc.request("config/read", { cwd: isolation.cwd, includeLayers: false });
    verifyEffectiveConfig(config, server.baseUrl); trace.isolation.configVerified = true;
    const thread = await rpc.request("thread/start", { cwd: isolation.cwd, approvalPolicy: "never", sandbox: "danger-full-access",
      experimentalRawEvents: false, persistExtendedHistory: true });
    if (thread.modelProvider !== "ue_script") throw new Error("实际线程 provider 未保持隔离");
    trace.threadId = thread.thread.id;
    if (scenario === "connection") await server.stopListening();
    const started = await rpc.request("turn/start", { threadId: trace.threadId, input: [{ type: "text", text: "只输出本地脚本结果。" }] });
    trace.turnId = started.turn.id;
    const final = () => trace.events.find((e) => e.direction === "received" && e.message.method === "turn/completed"
      && e.message.params.threadId === trace.threadId && e.message.params.turn.id === trace.turnId);
    if (options.interruptOn) {
      await rpc.until(() => options.interruptOn === "request" ? server.requests.length > 0
        : trace.events.some((e) => e.direction === "received" && e.message.method === "error" && e.message.params.willRetry === true), deadline, "中断触发信号");
      trace.interrupt = { sentAt: Date.now() };
      await rpc.request("turn/interrupt", { threadId: trace.threadId, turnId: trace.turnId }, Math.min(5000, deadline - Date.now()));
      trace.interrupt.acknowledgedAt = Date.now();
    }
    await rpc.until(() => Boolean(final()), deadline, "真实轮次完成");
    trace.completed = final()!.message.params.turn;
    if (trace.interrupt) trace.interrupt.completedAt = final()!.at;
    if (server.violations.length) throw new Error(server.violations.join("；"));
  } catch (error) { trace.failure = String(error); }
  finally {
    const cleanupAt = Date.now();
    const errors: string[] = [];
    for (const cleanup of [async () => { if (proc) await stopManaged(proc); }, () => server.close(), () => removeIsolation(isolation.root)]) {
      try { await cleanup(); } catch (error) { errors.push(String(error)); }
    }
    if (proc) trace.stderr = proc.output().stderr;
    trace.cleaned = errors.length === 0;
    if (errors.length) trace.failure = [trace.failure, `资源回收失败：${errors.join("；")}`].filter(Boolean).join("；");
    if (Date.now() - cleanupAt > CLEANUP_MS) trace.failure = [trace.failure, "清理超过冻结期限"].filter(Boolean).join("；");
    mkdirSync(TRACE_ROOT, { recursive: true });
    writeFileSync(join(TRACE_ROOT, `${options.traceName ?? scenario}.json`), JSON.stringify(trace, null, 2));
  }
  if (trace.failure) throw new Error(`${scenario}：${trace.failure}；轨迹已写入 ${TRACE_ROOT}`);
  return trace;
}

if (import.meta.main) {
  const scenario = (process.argv[2] ?? "ok") as UpstreamScenario;
  const trace = await runUpstreamReal(scenario, { ...(process.argv.includes("--interrupt-request") ? { interruptOn: "request" as const } : {}),
    ...(process.argv.includes("--interrupt-retry") ? { interruptOn: "retry" as const } : {}) });
  console.log(JSON.stringify({ scenario, requests: trace.requests.length, status: trace.completed?.status, errors: trace.events.filter((e) => e.message.method === "error").map((e) => e.message.params) }, null, 2));
}
