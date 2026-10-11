// src/cxb/config-schema.ts — cxb 持久配置键表（命令面规格 3.11 节）：命令行字符串与存储值分别校验；
// 参数解析（args.ts）、旧数据迁移与 N5 的 config 命令共用这一份。

import { config } from "../config";
import { CxbError } from "./contracts";

export type ConfigValue = string | number | boolean;

/** `server`：auto 在共享服务器可达时接入、shared 只用共享服务器、private 总用私有服务器（broker 读取）。 */
export const SERVER_PREFERENCES = ["auto", "shared", "private"] as const;

export interface ConfigKeySpec {
  /** 校验命令行字符串；不隐式接受空白、指数或小数。 */
  validate(value: string): boolean;
  /** 校验配置文件中已经转换过的存储值。 */
  isValid(value: unknown): value is ConfigValue;
  /** 转换命令行字符串；非法值抛出 USAGE，且不回显原值。 */
  parse(value: string): ConfigValue;
  hint: string;
}

function spec(
  hint: string,
  isValid: (value: unknown) => boolean,
  convert: (value: string) => ConfigValue = (value) => value,
  syntax: (value: string) => boolean = () => true,
): ConfigKeySpec {
  const validate = (value: string) => syntax(value) && isValid(convert(value));
  return Object.freeze({
    hint,
    validate,
    isValid: (value: unknown): value is ConfigValue => isValid(value),
    parse(value: string): ConfigValue {
      if (!validate(value)) throw new CxbError("USAGE", `配置值不合法；应为${hint}。`);
      return convert(value);
    },
  });
}

function enumeration(values: readonly string[]): ConfigKeySpec {
  return spec(`以下值之一：${values.join("、")}`, (value) => typeof value === "string" && values.includes(value));
}

function seconds(max: number): ConfigKeySpec {
  return spec(`1–${max} 秒整数`,
    (value) => typeof value === "number" && Number.isSafeInteger(value) && value >= 1 && value <= max,
    Number, (value) => /^\d+$/.test(value));
}

/** cxb 的持久配置白名单；迁移与后续 config 命令共用。秒数在存储时不转为毫秒。 */
export const CONFIG_SCHEMA = Object.freeze({
  model: spec("非空模型名（字母、数字及 . _ - / : [ ]）",
    (value) => typeof value === "string" && /^[a-zA-Z0-9._\-/:\[\]]+$/.test(value)),
  reasoning: enumeration(config.reasoningEfforts),
  sandbox: enumeration(config.sandboxModes),
  approval: enumeration(config.approvalModes),
  timeout: seconds(config.maxTimeoutSeconds),
  "converge-timeout": seconds(Math.floor(Number.MAX_SAFE_INTEGER / 1000)),
  memory: spec("true 或 false", (value) => typeof value === "boolean",
    (value) => value === "true", (value) => value === "true" || value === "false"),
  server: enumeration(SERVER_PREFERENCES),
  // 秒转毫秒后也必须能精确表示，不能把大整数静默舍入后传给 broker。
  "broker-idle-timeout": seconds(Math.floor(Number.MAX_SAFE_INTEGER / 1000)),
});

export type ConfigKey = keyof typeof CONFIG_SCHEMA;
export const CONFIG_KEYS = Object.freeze(Object.keys(CONFIG_SCHEMA) as ConfigKey[]);

export function isConfigKey(key: string): key is ConfigKey {
  return Object.hasOwn(CONFIG_SCHEMA, key);
}
