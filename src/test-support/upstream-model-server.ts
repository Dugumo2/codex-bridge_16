/** 仅监听 127.0.0.1 的 Responses 脚本服务；不记录请求正文或凭证头。 */
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createServer, type ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";
import type { Socket } from "node:net";
import { execFileSync } from "node:child_process";

export type UpstreamScenario = "ok" | "401" | "402" | "403" | "429" | "429-usage" | "429-long"
  | "500" | "500-long" | "slow" | "drop" | "401-then-ok" | "sse429-long" | "connection";
export interface ModelRequest { at: number; elapsedMs: number; path: string; sequence: number; status: number | null }
export const FAKE_KEY = "ue-local-fixed-fake-key";
export const OK_TEXT = "上游本地脚本执行成功";

const sse = (event: Record<string, unknown>) => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`;
export function successEvents(): string {
  const id = `resp_${randomUUID()}`;
  return [
    { type: "response.created", response: { id } },
    { type: "response.output_item.done", item: { id: `msg_${randomUUID()}`, type: "message", role: "assistant",
      phase: "final_answer", content: [{ type: "output_text", text: OK_TEXT }] } },
    { type: "response.completed", response: { id, usage: { input_tokens: 10, input_tokens_details: { cached_tokens: 0 },
      output_tokens: 5, output_tokens_details: null, total_tokens: 15 } } },
  ].map(sse).join("");
}

export async function startUpstreamModel(scenario: UpstreamScenario, options: { failures?: number; slowMs?: number } = {}) {
  const startedAt = Date.now();
  const requests: ModelRequest[] = [];
  const violations: string[] = [];
  if (scenario === "connection") {
    // 独立 Node 进程先绑定再关闭临时端口，退出本身证明句柄已释放；避开 Bun 关监听的延迟。
    const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => ["PATH", "SYSTEMROOT", "WINDIR"].includes(key.toUpperCase())));
    const profile = join(tmpdir(), `ue-port-only-${process.pid}-${randomUUID()}`);
    Object.assign(env, { HOME: profile, USERPROFILE: profile, TEMP: tmpdir(), TMP: tmpdir() });
    const port = Number(execFileSync("node.exe", ["-e", 'const net=require("node:net"); const s=net.createServer(); s.listen(0,"127.0.0.1",()=>{console.log(s.address().port);s.close();});'],
      { cwd: import.meta.dir, env, windowsHide: true, timeout: 30_000, encoding: "utf8" }).trim());
    if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error("无法取得本机连接拒绝端口");
    return { scenario, baseUrl: `http://127.0.0.1:${port}/v1`, requests, violations, async close() {} };
  }
  const sockets = new Set<Socket>();
  const timers = new Set<ReturnType<typeof setTimeout>>();
  const responses = new Set<ServerResponse>();
  const later = (ms: number, fn: () => void) => {
    const timer = setTimeout(() => { timers.delete(timer); fn(); }, ms);
    timers.add(timer);
  };
  const server = createServer((request, response) => {
    responses.add(response);
    response.on("close", () => responses.delete(response));
    const path = new URL(request.url ?? "/", "http://127.0.0.1").pathname;
    const entry: ModelRequest = { at: Date.now(), elapsedMs: Date.now() - startedAt, path, sequence: requests.length + 1, status: null };
    requests.push(entry);
    request.resume();
    if (request.method !== "POST" || path !== "/v1/responses") {
      violations.push("收到 Responses 之外的端点或方法");
      entry.status = 404; response.writeHead(404); response.end("未实现端点"); return;
    }
    if (request.headers.authorization !== `Bearer ${FAKE_KEY}`) {
      violations.push("模型请求未使用固定假凭证");
      entry.status = 400; response.writeHead(400); response.end("隔离核对失败"); return;
    }
    const selected = scenario === "401-then-ok" ? (entry.sequence <= (options.failures ?? 2) ? "401" : "ok") : scenario;
    if (selected === "ok" || selected === "slow" || selected === "drop" || selected === "sse429-long") {
      entry.status = 200;
      const respond = () => {
        if (response.destroyed) return;
        response.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache" });
        if (selected === "drop") {
          response.write(sse({ type: "response.created", response: { id: `resp_${entry.sequence}` } }));
          later(80, () => response.destroy());
        } else if (selected === "sse429-long") {
          response.end(sse({ type: "response.failed", response: { id: `resp_${entry.sequence}`, error: {
            type: "rate_limit_error", code: "rate_limit_exceeded", message: "UE SSE rate limit; try again in 3600 seconds" } } }));
        } else response.end(successEvents());
      };
      if (selected === "slow") later(options.slowMs ?? 60_000, respond); else respond();
      return;
    }
    const status = selected.startsWith("429") ? 429 : selected.startsWith("500") ? 500 : Number(selected);
    entry.status = status;
    const usage = selected === "429-usage";
    const body = { error: { message: `UE scripted HTTP ${status}`, type: usage ? "usage_limit_reached" : "ue_script_error",
      code: usage ? "usage_limit_reached" : `ue_http_${status}`, ...(usage ? { plan_type: "plus", resets_at: 1_900_000_000 } : {}) } };
    response.writeHead(status, { "Content-Type": "application/json", ...(selected.endsWith("-long") ? { "Retry-After": "3600" } : {}) });
    response.end(JSON.stringify(body));
  });
  server.on("connection", (socket) => { sockets.add(socket); socket.on("close", () => sockets.delete(socket)); });
  await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  const address = server.address();
  if (!address || typeof address === "string" || address.address !== "127.0.0.1") throw new Error("脚本模型未绑定本机地址");
  return {
    scenario, baseUrl: `http://127.0.0.1:${address.port}/v1`, requests, violations,
    async close() {
      for (const timer of timers) clearTimeout(timer);
      for (const response of responses) response.destroy();
      for (const socket of sockets) socket.destroy();
      server.unref();
      if (server.listening) await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("脚本服务关闭超过 1000 毫秒期限")), 1000);
        server.close((error) => { clearTimeout(timer); if (error) reject(error); else resolve(); });
      });
    },
  };
}
