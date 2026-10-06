import { appendFileSync } from "node:fs";

const args = process.argv.slice(2);
if (args[0] === "--version") {
  process.stdout.write("codex 0.160.0\n");
} else if (args[0] === "features" && args[1] === "list") {
  process.stdout.write("default_mode_request_user_input experimental false\n");
} else if (args[0] === "app-server") {
  if (process.env.N5A_PIDS) appendFileSync(process.env.N5A_PIDS, `${process.pid}\n`);
  let buffer = "";
  process.stdin.setEncoding("utf8");
  process.stdin.on("data", (data) => {
    buffer += data;
    let end: number;
    while ((end = buffer.indexOf("\n")) >= 0) {
      const message = JSON.parse(buffer.slice(0, end)); buffer = buffer.slice(end + 1);
      if (message.id === undefined) continue;
      const result = message.method === "initialize" ? { userAgent: "N5 甲路模拟 Codex 0.160.0" }
        : message.method === "account/read" ? { account: { type: "chatgpt", email: "n5@example.test", planType: "pro" }, requiresOpenaiAuth: true }
          : message.method === "model/list" ? { data: [{ id: "n5-model", model: "n5-model", isDefault: true, supportedReasoningEfforts: [{ reasoningEffort: "high" }] }], nextCursor: null }
            : message.method === "thread/list" ? { data: [], nextCursor: null } : {};
      process.stdout.write(JSON.stringify({ id: message.id, result }) + "\n");
    }
  });
  process.stdin.on("end", () => { process.stdin.pause(); });
} else {
  process.stderr.write("模拟 codex 不支持此命令\n");
  process.exitCode = 1;
}
