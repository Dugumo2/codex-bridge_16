/** 上游 0.160.0 受管后台命令协议；不模拟或承诺操作系统进程退出。 */
export interface BackgroundTerminalSeed {
  processId: string;
  itemId: string;
  command?: string;
  cwd?: string;
  /** false 且移除表示自然退出；false 且保留表示终止失败。 */
  termination?: "success" | "exited" | "remaining";
}
export interface BackgroundThreadScenario {
  sessions?: BackgroundTerminalSeed[];
  fault?: "unsupported" | "hang" | "notFound";
  /** 缺省故障作用于三个接口，也可只挂起某一个步骤。 */
  faultMethod?: "list" | "terminate" | "clean";
}
export interface BackgroundTerminalsMockOptions {
  enabled?: boolean;
  threads?: Record<string, BackgroundThreadScenario>;
}

export function createBackgroundTerminalsMock(send: (message: unknown) => unknown, options: BackgroundTerminalsMockOptions = {}) {
  const prefix = "thread/backgroundTerminals/";
  const threads = new Map<string, BackgroundThreadScenario>(Object.entries(structuredClone(options.threads ?? {})));
  const calls: Array<{ method: string; params: any }> = [];
  const terminal = (seed: BackgroundTerminalSeed) => ({ itemId: seed.itemId, processId: seed.processId,
    command: seed.command ?? `模拟命令 ${seed.processId}`, cwd: seed.cwd ?? "D:\\", osPid: null, cpuPercent: null, rssKb: null });
  return {
    handle(msg: { id?: unknown; method?: string; params?: any }): boolean {
      if (msg.id === undefined || !msg.method) return false;
      const p = msg.params ?? {};
      const reply = (result: unknown) => send({ id: msg.id, result });
      const fail = (code: number, message: string) => send({ id: msg.id, error: { code, message } });
      if (options.enabled && msg.method === "turn/interrupt") calls.push({ method: msg.method, params: structuredClone(p) });
      if (options.enabled && msg.method === "fixture/backgroundTerminals/set") {
        threads.set(p.threadId, structuredClone(p.scenario));
        reply({});
        return true;
      }
      if (options.enabled && msg.method === "fixture/backgroundTerminals/state") {
        reply({ calls: structuredClone(calls), threads: Object.fromEntries([...threads].map(([id, t]) =>
          [id, (t.sessions ?? []).map(terminal)])) });
        return true;
      }
      if (!msg.method.startsWith(prefix)) return false;
      calls.push({ method: msg.method, params: structuredClone(p) });
      const method = msg.method.slice(prefix.length);
      if (!options.enabled || !["list", "terminate", "clean"].includes(method)) {
        fail(-32601, `Method not found: ${msg.method}`);
        return true;
      }
      if (typeof p.threadId !== "string" || (method === "terminate" &&
          (typeof p.processId !== "string" || !/^-?\d+$/.test(p.processId) || Number(p.processId) < -2147483648 || Number(p.processId) > 2147483647))) {
        fail(-32600, "invalid threadId or processId");
        return true;
      }
      const scenario = threads.get(p.threadId) ?? {};
      threads.set(p.threadId, scenario);
      if (!scenario.faultMethod || scenario.faultMethod === method) {
        if (scenario.fault === "hang") return true;
        if (scenario.fault === "unsupported") { fail(-32601, `Method not found: ${msg.method}`); return true; }
        if (scenario.fault === "notFound") { fail(-32600, `thread not found: ${p.threadId}`); return true; }
      }
      const sessions = scenario.sessions ?? [];
      if (method === "list") {
        if (p.cursor != null && (typeof p.cursor !== "string" || !/^-?\d+$/.test(p.cursor) ||
            Number(p.cursor) < -2147483648 || Number(p.cursor) > 2147483647)) {
          fail(-32600, "invalid cursor");
          return true;
        }
        const all = [...sessions].sort((a, b) => Number(a.processId) - Number(b.processId))
          .filter((s) => p.cursor == null || Number(s.processId) > Number(p.cursor));
        const page = all.slice(0, Math.max(1, p.limit ?? all.length));
        reply({ data: page.map(terminal), nextCursor: page.length < all.length ? page.at(-1)!.processId : null });
      } else if (method === "terminate") {
        const found = sessions.find((s) => s.processId === p.processId);
        if (found?.termination !== "remaining") scenario.sessions = sessions.filter((s) => s !== found);
        reply({ terminated: !!found && (found.termination ?? "success") === "success" });
      } else {
        // clean 只提交本线程清理，不递归子线程，也不返回逐项终止证据。
        scenario.sessions = [];
        reply({});
      }
      return true;
    },
  };
}
