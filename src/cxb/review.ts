import { getDefaultBranch } from "../git";
import { CxbError, type ServerRequestDeps } from "./contracts";
import type { CommandContext, CommandHandler } from "./main";
import { executeRun, type ExecutionPlan } from "./run";

export function reviewPlan(ctx: CommandContext): ExecutionPlan {
  const { options, positional, explicit } = ctx.parsed;
  let mode = options.mode ?? "pr";
  const instructions = positional.join(" ");
  if (positional.length) {
    if (explicit.has("mode") && mode !== "custom") throw new CxbError("USAGE", "此审查模式不接受位置说明");
    mode = "custom";
  }
  if (mode === "custom" && !instructions.trim()) throw new CxbError("USAGE", "custom 审查需要说明");
  if ((options.ref && mode !== "commit") || (options.base && mode !== "pr")) throw new CxbError("USAGE", "审查引用与模式不匹配");
  if (options.goal || options.budget !== undefined) throw new CxbError("USAGE", "审查不接受目标或预算");
  const ref = mode === "commit" ? options.ref ?? "HEAD" : null;
  const base = mode === "pr" ? options.base ?? getDefaultBranch(ctx.workspace.root) : null;
  const target = mode === "custom" ? { type: "custom", instructions }
    : mode === "commit" ? { type: "commit", sha: ref }
      : mode === "pr" ? { type: "baseBranch", branch: base } : { type: "uncommittedChanges" };
  return { command: "review", sandbox: "read-only", review: { mode, ref, base, target } };
}

export function createReviewHandler(deps: ServerRequestDeps): CommandHandler {
  return async (ctx) => {
    const plan = reviewPlan(ctx);
    // 审查固定 never：按 mcp-when-never 与名单关闭 MCP，但不报告审批风险，保持审查原有输出。
    return executeRun(ctx, deps, plan);
  };
}
