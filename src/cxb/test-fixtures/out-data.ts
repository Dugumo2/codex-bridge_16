// 超过 300 KiB 的多字节内容，同时覆盖中文和代理对字符。
export const RESULT_CHUNK = "中文输出🙂🚀，逐字节核对。\n".repeat(100);
export const RESULT = RESULT_CHUNK.repeat(100);
export const PROGRESS = Array.from({ length: 96 }, (_, i) => `[cxb] 过程 ${i}：正在输出🙂\n`).join("");
export const NOTICE = "[cxb] 提醒：输出测试\n";
// 仅用于脱敏回归测试的合成载荷，不是真实凭据；最后一行故意伪装成堆栈帧。
export const PRIVATE_ERROR_MESSAGE = "token=abc123\nCookie: x\n    at token=abc123 (D:/Cookie: x/伪造.ts:1:2)";
