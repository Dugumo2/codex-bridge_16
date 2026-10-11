import { ASK_FEATURE_KEY, type ResumeOverrides } from "./contracts";

/** 与默认协作模式共用；工具不可用时保留文字提问的降级通道。 */
export const ASK_RULES = `能通过检查文件、运行验证或已有上下文自行确认的事情，请自行确认，不要询问用户。
遇到必须由用户决定的问题，request_user_input 工具可用时必须用它提问，并等待用户回答后再继续依赖该答案的工作。
如果调用 request_user_input 返回 unavailable（工具不可用），请用一句文字写出问题，然后结束本轮，等待用户回复。
在 goal 模式下，如果提问工具不可用，请先写出问题，再调用 update_goal 将目标状态设为 blocked，避免在缺少用户回答时反复自动续跑。
不要猜测用户尚未作出的决定，也不要把等待时间当作用户回答。`;

/** 子代理规则的版本号；措辞变化时加一，恢复覆盖随之重新登记。 */
export const SUBAGENT_RULES_VERSION = 1;

/**
 * 甲：条件式的子代理收尾规则（SA1b 接口第 7、11 节，唤醒讨论第 6 节）。v1 已覆盖后代、待答与限制，无需改版。
 * cxb 自有的根运行统一注入；已加载恢复仍提交并登记，不保证当轮实际重载开发者指令。
 * 不启用多代理、也不授权主动派生；正确性靠 broker 的自然结束门禁，不靠这段一定被加载。
 */
export const SUBAGENT_RULES = `【cxb 子代理规则 v${SUBAGENT_RULES_VERSION}】
如果本次任务实际使用了子代理，你负责跟踪本次任务派生或重新启动的全部子任务，包括仍在执行的后代任务。结束前须等待这些工作完成、失败或被明确取消，读取可用结果并纳入最终答复。
一次等待返回、等待超时或部分代理完成，都不表示全部完成。
待答的审批和提问仍属于未完成的工作，须按上面的提问与授权规则处理，不得猜测答案。
不要仅因等待超时而重新派生相同的任务。收到用户停止、预算或执行期限限制时，遵守停止要求并说明未完成的部分。
本规则不要求你主动使用子代理。`;

/** 传给 thread/start、thread/resume 并随恢复覆盖登记的完整开发者指令：原有提问规则在前，子代理规则追加在后。 */
export const ASK_INSTRUCTIONS = `${ASK_RULES}

${SUBAGENT_RULES}`;

export function askOverrides(): ResumeOverrides {
  return { config: { [ASK_FEATURE_KEY]: true }, developerInstructions: ASK_INSTRUCTIONS };
}
