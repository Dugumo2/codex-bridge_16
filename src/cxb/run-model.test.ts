import { expect, test } from "bun:test";
import type { Model } from "../types";
import { selectRunModel, type InheritedModel, type ModelSelection } from "./run-model";

function model(id: string, efforts: string[], isDefault = false): Model {
  return { id, model: id, displayName: id, description: "测试模型", hidden: false, upgrade: null,
    supportedReasoningEfforts: efforts.map((reasoningEffort) => ({ reasoningEffort, description: "测试强度" })),
    defaultReasoningEffort: "low", inputModalities: ["text"], supportsPersonality: false, isDefault };
}
const catalog = [model("astra", ["low", "medium", "high", "xhigh", "ultra"]), model("sol", ["low", "medium"], true)];
const inherited: InheritedModel = { model: "astra", effort: "high", source: "沿用线程" };
type Options = Partial<Omit<Parameters<typeof selectRunModel>[0], "out">>;
function select(patch: Options = {}) {
  const notices: string[] = [];
  const selection = selectRunModel({ explicit: new Set(), resume: true, inherited, models: catalog, ...patch,
    out: { notice: (text) => notices.push(text) } });
  return { ...selection, notices };
}
type Case = { name: string; options: Options; expected: Partial<ModelSelection>; notice?: string };
const cases: Case[] = [
  { name: "续跑不写参数沿用模型与强度", options: {}, expected: { model: "astra", reasoning: "high", modelSource: "沿用线程", reasoningSource: "沿用线程" } },
  { name: "config 模型与强度不压过沿用", options: { model: "sol", reasoning: "medium" }, expected: { model: "astra", reasoning: "high", modelSource: "沿用线程", reasoningSource: "沿用线程" } },
  { name: "显式同模型仍沿用强度", options: { model: "astra", explicit: new Set(["model"]) }, expected: { modelSource: "-m 指定", reasoning: "high", reasoningSource: "沿用线程" } },
  { name: "显式换模型优先 config 强度", options: { model: "sol", reasoning: "low", explicit: new Set(["model"]) }, expected: { model: "sol", modelSource: "-m 指定", reasoning: "low", reasoningSource: "config 默认" } },
  { name: "显式换模型无 config 自动选最高支持强度", options: { model: "sol", explicit: new Set(["model"]) }, expected: { reasoning: "medium", reasoningSource: "自动选择" } },
  { name: "显式强度原样发送", options: { reasoning: "low", explicit: new Set(["reasoning"]) }, expected: { model: "astra", reasoning: "low", reasoningSource: "-r 指定" } },
  { name: "显式不支持的强度仅提示", options: { reasoning: "minimal", explicit: new Set(["reasoning"]) }, expected: { reasoning: "minimal", reasoningSource: "-r 指定" }, notice: "目录未列出强度 minimal，按原样提交" },
  { name: "沿用强度不受自动 xhigh 上限影响", options: { inherited: { ...inherited, effort: "ultra" } }, expected: { reasoning: "ultra", reasoningSource: "沿用线程" } },
  { name: "原强度不支持转 config", options: { inherited: { ...inherited, effort: "minimal" }, reasoning: "low" }, expected: { reasoning: "low", reasoningSource: "config 默认" }, notice: "目录未列出原强度 minimal，不沿用" },
  { name: "原强度不支持转自动", options: { inherited: { ...inherited, effort: "minimal" } }, expected: { reasoning: "xhigh", reasoningSource: "自动选择" }, notice: "目录未列出原强度 minimal" },
  { name: "无原强度转 config", options: { inherited: { ...inherited, effort: null }, reasoning: "medium" }, expected: { reasoning: "medium", reasoningSource: "config 默认" } },
  { name: "config 不支持转自动且不选模型默认 low", options: { model: "sol", reasoning: "high", explicit: new Set(["model"]) }, expected: { reasoning: "medium", reasoningSource: "自动选择" }, notice: "目录未列出 config 强度 high，改为自动选择" },
  { name: "原强度及 config 均不支持仍选自动", options: { inherited: { ...inherited, effort: "minimal" }, reasoning: "none" }, expected: { reasoning: "xhigh", reasoningSource: "自动选择" }, notice: "目录未列出 config 强度 none" },
  { name: "上游记录有独立来源标注", options: { inherited: { ...inherited, source: "沿用上游记录" } }, expected: { modelSource: "沿用上游记录", reasoningSource: "沿用上游记录" } },
  { name: "无沿用记录按 config", options: { inherited: undefined, model: "astra", reasoning: "low" }, expected: { modelSource: "config 默认", reasoningSource: "config 默认" } },
  { name: "无沿用无 config 自动选择", options: { inherited: undefined }, expected: { model: "sol", reasoning: "medium", modelSource: "自动选择", reasoningSource: "自动选择" } },
  { name: "新开线程保持 config 默认", options: { resume: false, inherited: undefined, model: "astra", reasoning: "medium" }, expected: { model: "astra", reasoning: "medium", modelSource: "config 默认", reasoningSource: "config 默认" } },
  { name: "新开线程保持自动规则", options: { resume: false, inherited: undefined }, expected: { model: "sol", reasoning: "medium", modelSource: "自动选择", reasoningSource: "自动选择" } },
  { name: "模型别名先展开再判断是否换模型", options: { model: "spark", explicit: new Set(["model"]), inherited: { ...inherited, model: "gpt-5.3-codex-spark" }, models: undefined }, expected: { model: "gpt-5.3-codex-spark", reasoning: "high", reasoningSource: "沿用线程" }, notice: "沿用 high" },
];
for (const { name, options, expected, notice } of cases) test(`决策表：${name}`, () => {
  const result = select(options);
  expect(result).toMatchObject(expected);
  if (notice) expect(result.notices.join("\n")).toContain(notice);
  else expect(result.notices).toEqual([]);
}, 5000);

const unknownCatalogs: Array<[string, Model[] | undefined]> = [["目录失败", undefined], ["模型不在目录", [catalog[1]!]], ["S 为空", [model("astra", [])]]];
for (const [label, models] of unknownCatalogs) {
  test(`决策表：${label}，同模型有原强度仍沿用`, () => {
    expect(select({ models })).toMatchObject({ model: "astra", reasoning: "high", reasoningSource: "沿用线程" });
    const notices = select({ models }).notices.join("\n");
    expect(notices).toContain("无法从目录核实强度，沿用 high");
    if (label !== "S 为空") expect(notices).toContain("无法从模型目录确认 astra，按原样提交");
  }, 5000);
  test(`决策表：${label}，换模型且无 config 要求 -r`, () => {
    expect(() => select({ models, inherited: { ...inherited, model: "旧模型" }, model: "astra", explicit: new Set(["model"]) }))
      .toThrow("无法确定 astra 支持的推理强度，请用 -r 指定");
  }, 5000);
  test(`决策表：${label}，换模型有 config 可发送`, () => {
    const result = select({ models, inherited: { ...inherited, model: "旧模型" }, model: "astra", reasoning: "medium", explicit: new Set(["model"]) });
    expect(result).toMatchObject({ model: "astra", reasoning: "medium", reasoningSource: "config 默认" });
    expect(result.notices.join("\n")).toContain("无法从目录核实强度");
  }, 5000);
  test(`决策表：${label}，同模型无原强度不发送`, () => {
    const result = select({ models, inherited: { ...inherited, effort: null } });
    expect(result).toMatchObject({ reasoning: undefined, reasoningSource: "上游当前设置" });
    expect(result.notices.join("\n")).toContain("推理强度沿用上游当前设置（未核实）");
  }, 5000);
  test(`决策表：${label}，新开线程不发送强度`, () => {
    const result = select({ models, resume: false, inherited: undefined, model: "astra" });
    expect(result).toMatchObject({ reasoning: undefined, reasoningSource: "服务器默认" });
    expect(result.notices.join("\n")).toContain("推理强度由服务器默认决定");
  }, 5000);
  test(`决策表：${label}，显式强度不拦截`, () => {
    expect(select({ models, reasoning: "ultra", explicit: new Set(["reasoning"]) }))
      .toMatchObject({ reasoning: "ultra", reasoningSource: "-r 指定" });
  }, 5000);
}
test("决策表：续跑无法判断是否换模型且目录未知退出 1", () => {
  try {
    select({ models: undefined, inherited: undefined });
    throw new Error("应当拒绝开轮");
  } catch (error) { expect(error).toMatchObject({ code: "LOCAL_ERROR", exitCode: 1 }); }
}, 5000);
test("决策表：非法显式强度仍按用法错误退出 1", () => {
  try {
    select({ reasoning: "不存在的强度", explicit: new Set(["reasoning"]) });
    throw new Error("应当拒绝非法强度");
  } catch (error) { expect(error).toMatchObject({ code: "USAGE", exitCode: 1 }); }
}, 5000);
test("决策表：目录仅列 ultra 时不突破自动上限且提示", () => {
  const result = select({ models: [model("astra", ["ultra"])], inherited: { ...inherited, effort: null } });
  expect(result.reasoning).toBeUndefined();
  expect(result.notices.join("\n")).toContain("没有可自动选择的推理强度（最高 xhigh）");
}, 5000);
