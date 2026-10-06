import { afterEach, beforeEach, expect, test } from "bun:test";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { CONFIG_KEYS } from "./config-schema";
import { builtinTemplatesDir, listTemplates } from "./config-cmd";
import { N5Harness } from "./test-fixtures/n5a-support";
import { readUserConfig, writeUserConfig } from "./user-config";

let h: N5Harness;
beforeEach(() => { h = new N5Harness(); });
afterEach(async () => { await h.cleanup(); }, 10_000);

test("显示全部及单键 JSON 形状，不连接 broker", async () => {
  const all = await h.invoke(["config", "--json"]);
  expect(all.code).toBe(0);
  expect(JSON.parse(all.stdout)).toEqual({ ok: true, command: "config", path: join(h.home, "config.json"), values: {
    model: { value: null, source: "default" }, reasoning: { value: null, source: "default" }, sandbox: { value: "workspace-write", source: "default" }, approval: { value: "never", source: "default" }, timeout: { value: 3600, source: "default" }, memory: { value: false, source: "default" }, server: { value: "auto", source: "default" }, "broker-idle-timeout": { value: 300, source: "default" },
  }, unknownKeys: [] });
  expect(JSON.parse((await h.invoke(["config", "timeout", "--json"])).stdout)).toEqual({ ok: true, command: "config", key: "timeout", value: 3600, source: "default" });
  expect((await h.invoke(["config"])).stdout).toContain("配置文件");
});

const valid = { model: "model-a", reasoning: "high", sandbox: "read-only", approval: "auto", timeout: "7", memory: "true", server: "private", "broker-idle-timeout": "42" };
for (const key of CONFIG_KEYS) test(`设置、读取、单键清除及生效提示：${key}`, async () => {
  writeUserConfig(h.home, { future: { x: 1 } });
  const value = ["timeout", "broker-idle-timeout"].includes(key) ? Number(valid[key]) : key === "memory" ? true : valid[key];
  const set = await h.invoke(["config", key, valid[key], "--json", "-v"]);
  expect(set.code).toBe(0);
  expect(JSON.parse(set.stdout)).toEqual({ ok: true, command: "config", action: "set", key, value, appliesTo: ["server", "broker-idle-timeout"].includes(key) ? "brokerRestart" : "newRuns" });
  expect(set.stderr).toContain(["server", "broker-idle-timeout"].includes(key) ? "broker 重启后生效" : "只影响之后新开的运行");
  expect(JSON.parse((await h.invoke(["config", key, "--json"])).stdout)).toMatchObject({ key, value, source: "config" });
  expect(JSON.parse((await h.invoke(["config", key, "--unset", "--json"])).stdout)).toEqual({ ok: true, command: "config", action: "unset", key });
  expect(readUserConfig(h.home).values).toEqual({ future: { x: 1 } });
});

test("全部清除写空对象并明确包含未知键", async () => {
  writeUserConfig(h.home, { timeout: 5, future: 1 });
  expect(JSON.parse((await h.invoke(["config", "--unset", "--json"])).stdout)).toEqual({ ok: true, command: "config", action: "unsetAll" });
  expect(readUserConfig(h.home).values).toEqual({});
  expect((await h.invoke(["config", "--unset"])).stdout).toContain("包括未知键");
});

for (const key of CONFIG_KEYS) test(`非法写入退出一且字节不变：${key}`, async () => {
  writeUserConfig(h.home, { timeout: 5 });
  const path = join(h.home, "config.json");
  const before = readFileSync(path);
  const result = await h.invoke(["config", key, "非法 秘密", "--json"]);
  expect(result.code).toBe(1);
  expect(JSON.parse(result.stderr)).toMatchObject({ ok: false, error: { code: "USAGE", command: "config" } });
  expect(result.stderr).not.toContain("秘密");
  expect(readFileSync(path)).toEqual(before);
});

for (const args of [[], ["timeout"], ["timeout", "7"], ["timeout", "--unset"], ["--unset"], ["templates"]]) test(`损坏配置拒绝所有形式：${args.join(" ") || "显示"}`, async () => {
  writeFileSync(join(h.home, "config.json"), '{"timeout":0}');
  const result = await h.invoke(["config", ...args, "--json"]);
  expect(result.code).toBe(1);
  expect(JSON.parse(result.stderr).error.code).toBe("LOCAL_ERROR");
  expect(readFileSync(join(h.home, "config.json"), "utf8")).toBe('{"timeout":0}');
});

test("模板内置列表、用户覆盖、损坏跳过、JSON 告警与共同定位", async () => {
  mkdirSync(join(h.home, "templates"));
  writeFileSync(join(h.home, "templates", "collab.md"), "---\nname: collab\ndescription: 用户说明\nsandbox: read-only\n---\n{{PROMPT}}");
  for (const [name, text] of [["bad", "---\n未闭合"], ["bad2", "---\n非法格式\n---"], ["bad3", "---\nsandbox: fake\n---"]]) writeFileSync(join(h.home, "templates", `${name}.md`), text);
  const result = await h.invoke(["config", "templates", "--json", "-v"]);
  expect(result.code).toBe(0);
  const output = JSON.parse(result.stdout);
  expect(output).toMatchObject({ ok: true, command: "config", subcommand: "templates" });
  expect(output.items.find((item: any) => item.name === "collab")).toEqual({ name: "collab", description: "用户说明", sandbox: "read-only", source: "user" });
  expect(output.items.some((item: any) => item.source === "builtin")).toBe(true);
  expect(output.warnings).toHaveLength(3);
  expect(result.stderr.match(/已跳过/g)).toHaveLength(3);
  expect(builtinTemplatesDir(join(h.dir, "scripts"))).toBe(join(h.dir, "prompts"));
  expect(listTemplates(h.home).items.every((item) => !item.name.startsWith("bad"))).toBe(true);
});
