/** 第二批本地模型：挂起场景完全不发响应字节，其余复用第一批服务。 */
import { createServer } from "node:http";
import type { Socket } from "node:net";
import { FAKE_KEY, startUpstreamModel, type ModelRequest } from "./upstream-model-server";
import { within } from "./upstream-isolation";

export type StallModelScenario = "hang" | "401" | "ok";
export async function startStallModel(scenario: StallModelScenario) {
  if (scenario !== "hang") return { ...await startUpstreamModel(scenario), scenario };
  const started = Date.now(), requests: ModelRequest[] = [], violations: string[] = [];
  const sockets = new Set<Socket>();
  let closed = 0;
  const server = createServer((request, response) => {
    const path = new URL(request.url ?? "/", "http://127.0.0.1").pathname;
    request.resume();
    if (request.method !== "POST" || path !== "/v1/responses" || request.headers.authorization !== `Bearer ${FAKE_KEY}`) {
      violations.push("挂起服务收到非 Responses 请求或非固定假凭证"); response.writeHead(400); response.end("本地隔离核对失败"); return;
    }
    requests.push({ at: Date.now(), elapsedMs: Date.now() - started, path, sequence: requests.length + 1, status: null });
    response.on("close", () => { closed++; });
    // 不 writeHead、不 flushHeaders、不 write；模拟接受请求后无限等待首字节。
  });
  server.on("connection", (socket) => { sockets.add(socket); socket.on("close", () => sockets.delete(socket)); });
  await within(new Promise<void>((done, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", done); }), 5000, "挂起服务监听");
  const address = server.address();
  if (!address || typeof address === "string" || address.address !== "127.0.0.1") throw new Error("挂起服务必须绑定 127.0.0.1");
  return { scenario, baseUrl: `http://127.0.0.1:${address.port}/v1`, requests, violations,
    closedRequests: () => closed,
    async close() {
      for (const socket of sockets) socket.destroy(); server.unref();
      if (server.listening) await within(new Promise<void>((done, reject) => server.close((error) => error ? reject(error) : done())), 1000, "挂起服务清理");
    } };
}
