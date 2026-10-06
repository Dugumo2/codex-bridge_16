import { ASK_FEATURE_KEY, type ResumeOverrides } from "./contracts";

/** 与默认协作模式共用；工具不可用时保留文字提问的降级通道。 */
export const ASK_INSTRUCTIONS = `能通过检查文件、运行验证或已有上下文自行确认的事情，请自行确认，不要询问用户。
遇到必须由用户决定的问题，request_user_input 工具可用时必须用它提问，并等待用户回答后再继续依赖该答案的工作。
如果调用 request_user_input 返回 unavailable（工具不可用），请用一句文字写出问题，然后结束本轮，等待用户回复。
在 goal 模式下，如果提问工具不可用，请先写出问题，再调用 update_goal 将目标状态设为 blocked，避免在缺少用户回答时反复自动续跑。
不要猜测用户尚未作出的决定，也不要把等待时间当作用户回答。`;

export function askOverrides(): ResumeOverrides {
  return { config: { [ASK_FEATURE_KEY]: true }, developerInstructions: ASK_INSTRUCTIONS };
}
