import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { mkdtempSync, writeFileSync, rmSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";
import {
  config,
  validateId,
  resolveModel,
  validateEffort,
  loadTemplateWithMeta,
  interpolateTemplate,
  parseTemplateFrontmatter,
} from "./config";

// ─── config object ──────────────────────────────────────────────────────────

describe("config object", () => {
  test("has protocol timeouts", () => {
    expect(config.requestTimeout).toBeGreaterThan(0);
    expect(config.defaultTimeout).toBeGreaterThan(0);
  });

  test("has accepted reasoning efforts", () => {
    expect(config.reasoningEfforts).toEqual(["none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"]);
  });

  test("auto-select ceiling is a known effort below the top of the enum", () => {
    const efforts = config.reasoningEfforts as readonly string[];
    expect(efforts).toContain(config.autoEffortCeiling);
    expect(efforts.indexOf(config.autoEffortCeiling)).toBeLessThan(efforts.length - 1);
  });

  test("is frozen", () => {
    expect(Object.isFrozen(config)).toBe(true);
  });
});

// ─── validateId ─────────────────────────────────────────────────────────────

describe("validateId", () => {
  test("accepts valid IDs", () => {
    expect(validateId("abc-123_XYZ")).toBe("abc-123_XYZ");
  });

  test("rejects invalid IDs", () => {
    expect(() => validateId("has spaces")).toThrow("Invalid ID");
    expect(() => validateId("../escape")).toThrow("Invalid ID");
  });
});

// ─── resolveModel ───────────────────────────────────────────────────────────

describe("resolveModel", () => {
  test("resolves spark alias", () => {
    expect(resolveModel("spark")).toBe("gpt-5.3-codex-spark");
  });

  test("passes through unknown model names", () => {
    expect(resolveModel("o4-mini")).toBe("o4-mini");
    expect(resolveModel("gpt-5")).toBe("gpt-5");
  });

  test("returns undefined for undefined input", () => {
    expect(resolveModel(undefined)).toBeUndefined();
  });
});

// ─── validateEffort ─────────────────────────────────────────────────────────

describe("validateEffort", () => {
  test("accepts all valid effort levels", () => {
    for (const level of ["none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"] as const) {
      expect(validateEffort(level)).toBe(level);
    }
  });

  test("throws on invalid effort", () => {
    expect(() => validateEffort("turbo")).toThrow();
    expect(() => validateEffort("")).toThrow();
  });

  test("returns undefined for undefined input", () => {
    expect(validateEffort(undefined)).toBeUndefined();
  });
});

// ─── loadTemplateWithMeta ───────────────────────────────────────────────────

describe("loadTemplateWithMeta", () => {
  let tmpDir: string;

  beforeAll(() => {
    tmpDir = mkdtempSync(join(tmpdir(), "config-test-prompts-"));
    writeFileSync(join(tmpDir, "greeting.md"), "Hello, {{NAME}}!");
  });

  afterAll(() => {
    if (tmpDir) rmSync(tmpDir, { recursive: true, force: true });
  });

  test("从指定目录读取无元数据的模板", () => {
    expect(loadTemplateWithMeta("greeting", tmpDir)).toEqual({
      meta: { name: "", description: "" }, body: "Hello, {{NAME}}!",
    });
  });

  test("指定目录缺少模板时报错", () => {
    expect(() => loadTemplateWithMeta("nonexistent", tmpDir)).toThrow("Template not found:");
  });

  test("拒绝模板名称中的路径穿越", () => {
    for (const name of ["../escape", "sub/path", "..\\escape"]) {
      expect(() => loadTemplateWithMeta(name, tmpDir)).toThrow("Invalid template name");
    }
  });

  test("loadTemplateWithMeta returns both metadata and body", () => {
    writeFileSync(join(tmpDir, "meta-test.md"), "---\nname: meta-test\ndescription: Test template\nsandbox: read-only\n---\nTemplate body {{PROMPT}}");
    const { meta, body } = loadTemplateWithMeta("meta-test", tmpDir);
    expect(meta.name).toBe("meta-test");
    expect(meta.description).toBe("Test template");
    expect(meta.sandbox).toBe("read-only");
    expect(body).toBe("Template body {{PROMPT}}");
  });

});

// ─── parseTemplateFrontmatter ───────────────────────────────────────────────

describe("parseTemplateFrontmatter", () => {
  test("extracts frontmatter fields", () => {
    const raw = "---\nname: test\ndescription: A test template\nsandbox: read-only\n---\nBody content";
    const { meta, body } = parseTemplateFrontmatter(raw);
    expect(meta.name).toBe("test");
    expect(meta.description).toBe("A test template");
    expect(meta.sandbox).toBe("read-only");
    expect(body).toBe("Body content");
  });

  test("returns empty meta and full body when no frontmatter", () => {
    const raw = "Just plain content\nNo frontmatter here";
    const { meta, body } = parseTemplateFrontmatter(raw);
    expect(meta.name).toBe("");
    expect(meta.description).toBe("");
    expect(meta.sandbox).toBeUndefined();
    expect(body).toBe(raw);
  });

  test("handles missing closing delimiter", () => {
    const raw = "---\nname: broken\nNo closing delimiter";
    const { body } = parseTemplateFrontmatter(raw);
    expect(body).toBe(raw);
  });

  test("strips leading blank lines after frontmatter", () => {
    const raw = "---\nname: test\n---\n\n\nBody";
    const { body } = parseTemplateFrontmatter(raw);
    expect(body).toBe("Body");
  });

  test("handles CRLF line endings", () => {
    const raw = "---\r\nname: test\r\ndescription: CRLF template\r\nsandbox: read-only\r\n---\r\nBody with CRLF";
    const { meta, body } = parseTemplateFrontmatter(raw);
    expect(meta.name).toBe("test");
    expect(meta.description).toBe("CRLF template");
    expect(meta.sandbox).toBe("read-only");
    expect(body).toBe("Body with CRLF");
  });
});

// ─── interpolateTemplate ────────────────────────────────────────────────────

describe("interpolateTemplate", () => {
  test("replaces known variables", () => {
    const result = interpolateTemplate("Hello, {{NAME}}! Welcome to {{PLACE}}.", {
      NAME: "Alice",
      PLACE: "Wonderland",
    });
    expect(result).toBe("Hello, Alice! Welcome to Wonderland.");
  });

  test("leaves unknown variables as-is", () => {
    const result = interpolateTemplate("{{KNOWN}} and {{UNKNOWN}}", {
      KNOWN: "replaced",
    });
    expect(result).toBe("replaced and {{UNKNOWN}}");
  });

  test("handles empty vars", () => {
    const result = interpolateTemplate("no vars here", {});
    expect(result).toBe("no vars here");
  });

  test("replaces multiple occurrences of the same variable", () => {
    const result = interpolateTemplate("{{X}} and {{X}}", { X: "y" });
    expect(result).toBe("y and y");
  });
});
