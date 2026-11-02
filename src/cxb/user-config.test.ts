import { afterEach, beforeEach, expect, spyOn, test } from "bun:test";
import fs, { readFileSync, readdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { CONFIG_KEYS } from "./config-schema";
import { readUserConfig, writeUserConfig } from "./user-config";
import { N5Harness } from "./test-fixtures/n5a-support";

let h: N5Harness;
beforeEach(() => { h = new N5Harness(); });
afterEach(async () => { await h.cleanup(); }, 10_000);

test("不存在、空对象、合法值与未知键读写保留", () => {
  expect(readUserConfig(h.home).values).toEqual({});
  writeUserConfig(h.home, {});
  expect(readUserConfig(h.home).unknownKeys).toEqual([]);
  const values = { model: "gpt-test", reasoning: "high", sandbox: "read-only", approval: "auto", timeout: 12, memory: true, server: "private", "broker-idle-timeout": 47, future: { token: "不回显", list: [1, 2] } };
  writeUserConfig(h.home, values);
  expect(readUserConfig(h.home)).toMatchObject({ values, unknownKeys: ["future"] });
  writeUserConfig(h.home, { ...readUserConfig(h.home).values, timeout: 13 });
  expect(readUserConfig(h.home).values.future).toEqual(values.future);
});

for (const raw of ["null", "[]", "3", '"秘密"', "{", ...CONFIG_KEYS.map((key) => JSON.stringify({ [key]: "非法 秘密" }))]) {
  test(`损坏配置拒绝读取且不覆盖：${raw.split(":")[0]}`, () => {
    const path = join(h.home, "config.json");
    writeFileSync(path, raw);
    expect(() => readUserConfig(h.home)).toThrow("请备份后修复或删除该文件");
    try { readUserConfig(h.home); } catch (error) { expect(String(error)).toContain(path); expect(String(error)).not.toContain("秘密"); }
    expect(() => writeUserConfig(h.home, {})).toThrow();
    expect(readFileSync(path, "utf8")).toBe(raw);
  });
}

test("原子写入只发布完整 JSON，改名失败清理临时文件且保留旧内容", () => {
  writeUserConfig(h.home, { timeout: 12 });
  const original = fs.renameSync;
  const observed: unknown[] = [];
  const spy = spyOn(fs, "renameSync").mockImplementation((source, target) => {
    observed.push(JSON.parse(readFileSync(source, "utf8")));
    expect(JSON.parse(readFileSync(target, "utf8"))).toEqual({ timeout: 12 });
    throw new Error("模拟提交失败");
  });
  try { expect(() => writeUserConfig(h.home, { timeout: 13 })).toThrow("无法原子保存配置"); }
  finally { spy.mockRestore(); }
  expect(observed).toEqual([{ timeout: 13 }]);
  expect(readUserConfig(h.home).values).toEqual({ timeout: 12 });
  expect(readdirSync(h.home)).toEqual(["config.json"]);
  expect(fs.renameSync).toBe(original);
  writeUserConfig(join(h.home, "new"), { memory: true });
  expect(readUserConfig(join(h.home, "new")).values).toEqual({ memory: true });
});

for (const key of CONFIG_KEYS) test(`写入 API 拒绝非法已知键且保留合法原文件：${key}`, () => {
  writeUserConfig(h.home, { timeout: 8 });
  const path = join(h.home, "config.json"); const before = readFileSync(path);
  expect(() => writeUserConfig(h.home, { [key]: "非法 值" })).toThrow("请备份后修复或删除该文件");
  expect(readFileSync(path)).toEqual(before);
});
