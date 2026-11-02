import { describe, expect, test } from "bun:test";
import { questionSummary, sanitizeForTerminal } from "./terminal-text";

describe("text helpers", () => {
  test("questionSummary takes the first line and clips", () => {
    expect(questionSummary("short question\nsecond line")).toBe("short question");
    const long = "x".repeat(300);
    expect(questionSummary(long).length).toBe(160);
    expect(questionSummary(long).endsWith("…")).toBe(true);
  });

  test("sanitizeForTerminal strips control chars but keeps newlines and tabs", () => {
    expect(sanitizeForTerminal("a\x1b[31mred\x07\nb\tc")).toBe("a[31mred\nb\tc");
  });
});
