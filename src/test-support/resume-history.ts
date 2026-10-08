import type { Writable } from "node:stream";

/** 只用于恢复回包：约 12 MiB，由正常大小的历史条目组成，不写入夹具状态。 */
export function longResumeTurns() {
  return Array.from({ length: 3072 }, (_, index) => ({
    id: `history-${index}`, status: "completed", error: null, itemsView: "full",
    items: [{ id: `answer-${index}`, type: "agentMessage", phase: "final_answer", text: "x".repeat(4096) }],
  }));
}

/** 所有消息共用队列；背压时等待 drain，关闭、错误与期限均会终止等待。 */
export function createJsonLineWriter(output: Writable, onError: (error: Error) => void, timeoutMs = 5000) {
  let tail = Promise.resolve();
  let failed = false;
  return (value: unknown): Promise<void> => {
    // 入队时冻结消息，避免轮次对象随后被完成通知改写。
    const line = JSON.stringify(value) + "\n";
    tail = tail.then(async () => {
      if (failed) return;
      const bytes = Buffer.from(line);
      for (let offset = 0; offset < bytes.length; offset += 8192) {
        await new Promise<void>((resolve, reject) => {
          let timer: ReturnType<typeof setTimeout> | undefined;
          const done = (error?: Error | null) => {
            clearTimeout(timer);
            output.off("close", closed);
            output.off("error", broken);
            output.off("drain", drained);
            if (error) reject(error); else resolve();
          };
          const closed = () => done(new Error("模拟输出已关闭"));
          const broken = (error: Error) => done(error);
          const drained = () => done();
          output.once("close", closed);
          output.once("error", broken);
          output.once("drain", drained);
          timer = setTimeout(() => done(new Error("模拟输出背压等待超时")), timeoutMs);
          if (output.destroyed || output.writableEnded) closed();
          else {
            try { if (output.write(bytes.subarray(offset, offset + 8192))) done(); }
            catch (error) { done(error as Error); }
          }
        });
      }
    }).catch((error: Error) => {
      failed = true;
      onError(error);
    });
    return tail;
  };
}

export function fixtureSender() {
  // 对端关闭时保留错误监听，避免迟到的 EPIPE 成为未处理事件。
  const failed = () => {
    process.exitCode = 1;
    process.stdin.destroy();
    process.stdout.destroy();
  };
  process.stdout.on("error", failed);
  return createJsonLineWriter(process.stdout, failed);
}
