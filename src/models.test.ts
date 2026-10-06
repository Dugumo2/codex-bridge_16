// src/models.test.ts — 模型选择用例（自 src/commands/shared.test.ts 迁入）
//
// 这些用例测的一直是 src/models.ts 的 pickBestModel（旧 shared.ts 只是
// re-export）；旧命令层删除后改为直接导入 ./models。用例名保持原文以便核对。

import { describe, expect, test } from "bun:test";
import { pickBestModel } from "./models";
import type { Model } from "./types";

describe("pickBestModel", () => {
  const m = (id: string, opts: { upgrade?: string; isDefault?: boolean } = {}): Model => ({
    id,
    model: id,
    upgrade: opts.upgrade ?? null,
    isDefault: opts.isDefault ?? false,
    displayName: id,
    description: "",
    hidden: false,
    supportedReasoningEfforts: [],
    defaultReasoningEffort: "medium",
    inputModalities: ["text"],
    supportsPersonality: false,
  });

  test("follows upgrade chain to latest model", () => {
    const models = [
      m("old", { upgrade: "mid", isDefault: true }),
      m("mid", { upgrade: "new" }),
      m("new"),
    ];
    expect(pickBestModel(models)).toBe("new");
  });

  test("prefers -codex variant at end of chain", () => {
    const models = [
      m("gpt-5", { isDefault: true }),
      m("gpt-5-codex"),
    ];
    expect(pickBestModel(models)).toBe("gpt-5-codex");
  });

  test("does not prefer -codex variant if it has an upgrade itself", () => {
    const models = [
      m("gpt-5", { isDefault: true }),
      m("gpt-5-codex", { upgrade: "gpt-6-codex" }),
    ];
    // codexVariant.upgrade !== null, so returns current (gpt-5)
    expect(pickBestModel(models)).toBe("gpt-5");
  });

  test("returns undefined when no default model", () => {
    const models = [m("gpt-5")];
    expect(pickBestModel(models)).toBeUndefined();
  });

  test("handles circular upgrade chain via visited guard", () => {
    const models = [
      m("a", { upgrade: "b", isDefault: true }),
      m("b", { upgrade: "a" }),
    ];
    // a -> b (visited={a}), b -> a (visited={a,b}), a: visited.has(a) -> exit loop, current = a
    expect(pickBestModel(models)).toBe("a");
  });

  test("returns default when upgrade target not in list", () => {
    const models = [m("gpt-5", { upgrade: "nonexistent", isDefault: true })];
    expect(pickBestModel(models)).toBe("gpt-5");
  });

  test("already a -codex model stays as-is", () => {
    const models = [m("gpt-5-codex", { isDefault: true })];
    expect(pickBestModel(models)).toBe("gpt-5-codex");
  });
});
