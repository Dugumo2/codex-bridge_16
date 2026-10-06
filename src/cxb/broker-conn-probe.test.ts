import { afterEach, beforeEach, expect, spyOn, test } from "bun:test";
import childProcess from "node:child_process";
import net from "node:net";
import { BrokerNotRunning, connectBroker } from "./broker-conn";
import { CXB_PROTOCOL_VERSION, type BrokerConnection } from "./contracts";
import { N5Harness } from "./test-fixtures/n5a-support";
import { resolveWorkspace } from "./workspace";
import { bounded } from "./wait-run";

let h: N5Harness;
let server: net.Server | undefined;
let sockets: Set<net.Socket>;
let connections: BrokerConnection[];
let processSpy: ReturnType<typeof spyOn<typeof childProcess, "execFile">>;
beforeEach(() => {
  h = new N5Harness(); sockets = new Set(); connections = []; server = undefined;
  processSpy = spyOn(childProcess, "execFile").mockImplementation((() => { throw new Error("探测禁止启动外部进程"); }) as unknown as typeof childProcess.execFile);
});
afterEach(async () => {
  processSpy.mockRestore();
  const errors: unknown[] = [];
  for (const conn of connections) { try { await bounded(conn.close(), 1500); } catch (error) { errors.push(error); } }
  for (const socket of sockets) socket.destroy();
  try { if (server) await bounded(new Promise<void>((resolve) => server!.close(() => resolve())), 2000); } catch (error) { errors.push(error); }
  try { await h.cleanup(); } catch (error) { errors.push(error); }
  if (errors.length) throw new AggregateError(errors, "探测测试清理失败");
}, 10_000);

test("探测缺失管道绝不 launch", async () => {
  await expect(connectBroker({ workspace: resolveWorkspace(h.root), brokerBin: "禁止执行", expectedBrokerVersion: "1.2.3", timeoutMs: 1000, probe: true })).rejects.toBeInstanceOf(BrokerNotRunning);
  expect(processSpy).not.toHaveBeenCalled();
});

for (const [version, protocol] of [["1.2.3", CXB_PROTOCOL_VERSION], ["0.0.0", CXB_PROTOCOL_VERSION], ["1.2.3", 999]] as const) test(`探测握手 ${version}/${protocol} 只发只读方法、不 shutdown、不 launch`, async () => {
  const workspace = resolveWorkspace(h.root);
  const calls: string[] = [];
  server = net.createServer((socket) => {
    sockets.add(socket); socket.on("close", () => sockets.delete(socket)); socket.on("error", () => {});
    let input = "";
    socket.setEncoding("utf8");
    socket.on("data", (chunk) => {
      input += chunk;
      let end: number;
      while ((end = input.indexOf("\n")) >= 0) {
        const message = JSON.parse(input.slice(0, end)); input = input.slice(end + 1); calls.push(message.method);
        if (message.id !== undefined) socket.write(JSON.stringify({ id: message.id, result: message.method === "initialize" ? { brokerVersion: version, protocolVersion: protocol, epoch: 1, userAgent: "探测桩", server: { kind: "private" } } : { storage: "ok" } }) + "\n");
      }
    });
  });
  await bounded(new Promise<void>((resolve, reject) => { server!.once("error", reject); server!.listen(workspace.pipePath, resolve); }), 2000);
  const conn = await connectBroker({ workspace, brokerBin: "禁止执行", expectedBrokerVersion: "1.2.3", timeoutMs: 500, probe: true });
  connections.push(conn);
  expect(conn.launched).toBe(false);
  expect(conn.handshake.brokerVersion).toBe(version);
  await bounded(conn.rpc.request("broker/health", {}, { timeoutMs: 500 }), 600);
  expect(calls).toEqual(["initialize", "initialized", "broker/health"]);
  expect(processSpy).not.toHaveBeenCalled();
});
