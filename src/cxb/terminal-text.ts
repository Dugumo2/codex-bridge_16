// cxb 终端文本清理与问题摘要。

/** 提取问题首行并裁剪为摘要，默认最多 160 个字符。 */
export function questionSummary(question: string, maxLen = 160): string {
  const firstLine = question.split("\n", 1)[0].trim();
  return firstLine.length > maxLen ? firstLine.slice(0, maxLen - 1) + "…" : firstLine;
}

/** 清理终端和日志文本中的控制字符，保留换行与制表符等原有空白行为。 */
export function sanitizeForTerminal(text: string): string {
  // eslint-disable-next-line no-control-regex
  return text.replace(/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/g, "");
}
