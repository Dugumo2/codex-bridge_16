import { afterEach, beforeEach, expect, test } from "bun:test";
import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Writable } from "node:stream";
import { createOutput } from "./output";
import { approvalPreflight, reportApprovalPreflight } from "./preflight";

let dir: string;
let streams: Writable[];
beforeEach(() => { dir = mkdtempSync(join(tmpdir(), "cxb-n2a-preflight-")); streams = []; });
afterEach(() => {
  const failures: unknown[] = [];
  for (const stream of streams) try { stream.destroy(); } catch (error) { failures.push(error); }
  try { rmSync(dir, { recursive: true, force: true }); expect(existsSync(dir)).toBe(false); } catch (error) { failures.push(error); }
  expect(failures).toEqual([]);
});

test("never 下多个风险只输出一条固定格式提醒，结构化结果不含秘密", async () => {
  writeFileSync(join(dir, "config.toml"), '[mcp_servers."搜索"]\ncommand="工具"\nenv={TOKEN="不可泄露"}\n[mcp_servers."资料"]\ndefault_tools_approval_mode="prompt"\n[mcp_servers."可信"]\ndefault_tools_approval_mode="approve"\n');
  const result = await approvalPreflight("never", { CODEX_HOME: dir });
  expect(result.status).toBe("warn");
  expect(result.servers.map((server) => [server.name, server.risk])).toEqual([["搜索", true], ["资料", true], ["可信", false]]);
  expect(JSON.stringify(result)).not.toContain("不可泄露");
  let text = "";
  const stream = new Writable({ write(chunk, _, callback) { text += chunk; callback(); } });
  streams.push(stream);
  const out = createOutput({ command: "run", json: true, verbose: false, stdout: stream, stderr: stream });
  reportApprovalPreflight(result, out);
  await out.flush();
  stream.destroy();
  expect(text).toBe("[cxb] 提醒：当前审批为 never，MCP 服务器 搜索、资料 的工具请求可能被 Codex 拒绝；需要搜索/MCP 时请加 --approval auto。\n");
  expect((await approvalPreflight("auto", { CODEX_HOME: dir })).status).toBe("ok");
});

test("明确的单工具审批覆盖可触发风险，停用服务器和工具不触发", async () => {
  writeFileSync(join(dir, "config.toml"), '[mcp_servers.a]\ndefault_tools_approval_mode="approve"\n[mcp_servers.a.tools.write]\napproval_mode="prompt"\n[mcp_servers.b]\nenabled=false\n[mcp_servers.c]\ndefault_tools_approval_mode="approve"\ndisabled_tools=["write"]\n[mcp_servers.c.tools.write]\napproval_mode="prompt"\n');
  const result = await approvalPreflight("never", { codex_home: dir });
  expect(result.servers.map((server) => [server.name, server.risk])).toEqual([["a", true], ["c", false]]);
});

test("缺省路径与缺文件为正常；解析失败或不可读只在详细模式输出 unavailable", async () => {
  expect((await approvalPreflight("never", { CODEX_HOME: dir })).status).toBe("ok");
  mkdirSync(join(dir, ".codex"));
  writeFileSync(join(dir, ".codex", "config.toml"), "[mcp_servers.a]\n");
  expect((await approvalPreflight("never", { USERPROFILE: dir })).status).toBe("warn");
  writeFileSync(join(dir, "config.toml"), "broken = [");
  const result = await approvalPreflight("never", { CODEX_HOME: dir });
  expect(result.status).toBe("unavailable");
  let text = "";
  const stream = new Writable({ write(chunk, _, callback) { text += chunk; callback(); } });
  streams.push(stream);
  for (const verbose of [false, true]) {
    const out = createOutput({ command: "run", json: false, verbose, stderr: stream, stdout: stream });
    reportApprovalPreflight(result, out);
    await out.flush();
    expect(text).toBe(verbose ? "approval preflight: unavailable\n" : "");
  }
  stream.destroy();
  rmSync(join(dir, "config.toml"));
  mkdirSync(join(dir, "config.toml"));
  expect((await approvalPreflight("never", { CODEX_HOME: dir })).status).toBe("unavailable");
});
