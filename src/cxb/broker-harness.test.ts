import { expect, test } from "bun:test";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { writeMockCodexScript } from "../test-support/broker-harness";

const begin = "// CXB_INLINE_RESUME_HISTORY_BEGIN";
const end = "// CXB_INLINE_RESUME_HISTORY_END";
const helperImport = 'import { fixtureSender, longResumeTurns } from "../../test-support/resume-history";';

test("模拟脚本按标记内联一次且不依赖导入格式", () => {
  const dir = mkdtempSync(join(tmpdir(), "cxb-inline-"));
  try {
    writeMockCodexScript(dir, `${begin}\r\nimport {\r\n  fixtureSender, longResumeTurns,\r\n} from '../../test-support/resume-history';\r\n${end}\r\nfixtureSender();\r\n`);
    const script = readFileSync(join(dir, process.platform === "win32" ? "codex-mock.ts" : "codex"), "utf8");
    expect(script.split("function fixtureSender()")).toHaveLength(2);
    expect(script.split("function longResumeTurns()")).toHaveLength(2);
    expect(script).not.toContain(begin);
    expect(script).not.toContain(end);
    expect(script).not.toContain("../../test-support/resume-history");
    expect(script).toEndWith("fixtureSender();\r\n");
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

for (const [name, script] of [
  ["缺失标记", helperImport],
  ["重复标记", `${begin}\n${helperImport}\n${end}\n${begin}\n${helperImport}\n${end}`],
  ["不完整标记", `${begin}\n${helperImport}`],
  ["标记顺序失配", `${end}\n${helperImport}\n${begin}`],
] as const) test(`模拟脚本${name}时在写文件前报错`, () => {
  const dir = mkdtempSync(join(tmpdir(), "cxb-inline-"));
  try {
    expect(() => writeMockCodexScript(dir, script)).toThrow("恢复历史内联");
    for (const file of ["codex", "codex-mock.ts", "codex-proxy.ts", "codex.cmd"]) {
      expect(existsSync(join(dir, file))).toBe(false);
    }
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("不需要恢复历史内联的模拟脚本保持原内容", () => {
  const dir = mkdtempSync(join(tmpdir(), "cxb-inline-"));
  try {
    const script = 'process.stdout.write("模拟脚本");\n';
    writeMockCodexScript(dir, script);
    expect(readFileSync(join(dir, process.platform === "win32" ? "codex-mock.ts" : "codex"), "utf8")).toBe(script);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
