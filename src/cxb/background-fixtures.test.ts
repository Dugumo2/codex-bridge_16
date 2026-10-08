/** 验证黑盒实际复制执行的两种模拟服务器，而不只验证内存模块。 */
import { describe, expect, test } from "bun:test";
import { spawn } from "node:child_process";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createMockCodex, removeDirWithRetry, writeMockCodexScript } from "../test-support/broker-harness";
import { createRpcEndpoint } from "../rpc";
import { bounded } from "./test-fixtures/main-test-support";

describe("黑盒模拟服务器后台命令协议", () => {
  for (const kind of ["createMockCodex", "e2e-codex"] as const) for (const enabled of [false, true]) {
    test(`${kind}：后台命令开关 ${enabled}`, async () => {
      const dir = mkdtempSync(join(tmpdir(), "cxb-bg-fixture-"));
      const options = { enabled, threads: { 父: { sessions: [{ processId: "17", itemId: "command-17" }] } } };
      if (kind === "createMockCodex") createMockCodex(dir, { backgroundTerminals: options });
      else writeMockCodexScript(dir, readFileSync(join(import.meta.dir, "test-fixtures/e2e-codex.ts"), "utf8"));
      const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("MOCK_")));
      const child = spawn(process.execPath, [join(dir, process.platform === "win32" ? "codex-mock.ts" : "codex"), "app-server"], {
        env: { ...env, MOCK_BACKGROUND_TERMINALS: JSON.stringify(options) }, windowsHide: true, stdio: ["pipe", "pipe", "pipe"],
      });
      const closed = new Promise<void>((resolve, reject) => { child.once("close", () => resolve()); child.once("error", reject); });
      const rpc = createRpcEndpoint({ label: "后台夹具协议", requestTimeout: 5000, send: (data) => { child.stdin.write(data); },
        downReason: () => child.exitCode === null ? null : "夹具已退出", overflowReason: "夹具输出超限",
        writeFailedPrefix: "夹具写入失败：", onOverflow: () => child.kill() });
      child.stdout.setEncoding("utf8");
      child.stdout.on("data", (data) => rpc.feed(data));
      child.stderr.resume();
      try {
        await rpc.request("initialize", { capabilities: { experimentalApi: true } });
        const prefix = "thread/backgroundTerminals/";
        if (!enabled) {
          for (const method of ["list", "terminate", "clean"]) {
            const error = await rpc.request<any>(prefix + method, { threadId: "父", processId: "17" }).then(() => null, (error) => error);
            expect(error).toMatchObject({ rpcCode: -32601 });
          }
        } else {
          expect(await rpc.request<any>(prefix + "list", { threadId: "父" })).toMatchObject({ data: [{ processId: "17", osPid: null }], nextCursor: null });
          expect(await rpc.request<any>(prefix + "terminate", { threadId: "父", processId: "17" })).toEqual({ terminated: true });
          expect(await rpc.request<any>(prefix + "list", { threadId: "父" })).toEqual({ data: [], nextCursor: null });
          expect(await rpc.request<any>(prefix + "clean", { threadId: "父" })).toEqual({});
          expect(await rpc.request("fixture/backgroundTerminals/state")).toMatchObject({ threads: { 父: [] } });
        }
      } finally {
        rpc.markClosed();
        child.kill();
        await bounded(closed, 5000, "后台夹具退出超时");
        await removeDirWithRetry(dir);
      }
    }, 30_000);
  }
});
