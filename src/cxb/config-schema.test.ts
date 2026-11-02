import { describe, expect, test } from "bun:test";
import { CONFIG_KEYS, CONFIG_SCHEMA } from "./config-schema";
import { CxbError } from "./contracts";

describe("配置键表", () => {
  test("包含运行策略的十三个键，字符串转换为各自的存储类型", () => {
    expect(CONFIG_KEYS).toEqual(["model", "reasoning", "sandbox", "approval", "timeout", "converge-timeout", "stall-timeout", "upstream-error-policy", "memory", "server", "broker-idle-timeout", "mcp-disabled", "mcp-when-never"]);
    const values = { model: "模型", reasoning: "ultra", sandbox: "read-only", approval: "auto", timeout: "3600", memory: "false", server: "private", "converge-timeout": "3600", "stall-timeout": "0", "upstream-error-policy": "observe", "broker-idle-timeout": "300", "mcp-disabled": "backlog, serena,backlog", "mcp-when-never": "keep" };
    values.model = "provider/model:[v1]";
    for (const key of CONFIG_KEYS) {
      const spec = CONFIG_SCHEMA[key];
      expect(spec.validate(values[key])).toBe(true);
      expect(spec.isValid(spec.parse(values[key]))).toBe(true);
      expect(spec.hint.length).toBeGreaterThan(0);
    }
    expect(CONFIG_SCHEMA.timeout.parse("3600")).toBe(3600);
    expect(CONFIG_SCHEMA.memory.parse("false")).toBe(false);
    expect(CONFIG_SCHEMA.memory.parse("true")).toBe(true);
    expect(CONFIG_SCHEMA["broker-idle-timeout"].parse("300")).toBe(300);
  });

  test("严格拒绝非法枚举、错误存储类型和不精确的秒数", () => {
    for (const value of ["", "0", "-1", "1.5", "Infinity", "1e3", " 1", "2147484"]) {
      expect(CONFIG_SCHEMA.timeout.validate(value)).toBe(false);
      expect(() => CONFIG_SCHEMA.timeout.parse(value)).toThrow(CxbError);
    }
    expect(CONFIG_SCHEMA.timeout.parse("2147483")).toBe(2_147_483);
    expect(CONFIG_SCHEMA.timeout.isValid("300")).toBe(false);
    expect(CONFIG_SCHEMA.memory.isValid("true")).toBe(false);
    expect(CONFIG_SCHEMA.memory.validate("TRUE")).toBe(false);
    for (const key of ["reasoning", "sandbox", "approval", "server"] as const) {
      expect(CONFIG_SCHEMA[key].validate("不存在")).toBe(false);
    }
    for (const value of ["", "有空格 model", "name;command", "name\n"]) expect(CONFIG_SCHEMA.model.validate(value)).toBe(false);
    expect(CONFIG_SCHEMA["broker-idle-timeout"].validate("9007199254740992")).toBe(false);
    expect(CONFIG_SCHEMA["broker-idle-timeout"].parse("2147484")).toBe(2_147_484);
  });
});
