/** 测试看门狗只能使测试失败，绝不能冒充产品的 TIMEOUT/退出码 3。 */
export class TestWatchdogError extends Error {
  constructor(label: string) { super(`测试看门狗超时：${label}`); this.name = "TestWatchdogError"; }
}

export async function watchdog<T>(work: Promise<T>, ms = 5000, label = "异步操作"): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([work, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new TestWatchdogError(label)), ms);
    })]);
  } finally { clearTimeout(timer); }
}

/**
 * 内存夹具的稳定屏障：连续三轮事件循环的观测值不变才返回。
 * 假设待验证工作只通过微任务或相邻轮次的 setImmediate 推进，并且三轮内会改变观测值；
 * 调用方须覆盖所有相关活动（调用/回包数、在途请求数等）。这不能证明任意延时、外部 I/O 已排空；
 * 这些场景必须另设明确控制点。取值函数返回原始值数组，避免可变对象引用掩盖变化。
 */
export async function messageBarrier(observe: () => readonly (string | number | boolean | null | undefined)[], timeoutMs = 5000): Promise<void> {
  let immediate: ReturnType<typeof setImmediate> | undefined;
  let active = true;
  const drain = async () => {
    let previous = [...observe()];
    let stableRounds = 0;
    while (active && stableRounds < 3) {
      await new Promise<void>((resolve) => { immediate = setImmediate(resolve); });
      immediate = undefined;
      if (!active) return;
      const current = [...observe()];
      const unchanged = current.length === previous.length && current.every((value, i) => Object.is(value, previous[i]));
      stableRounds = unchanged ? stableRounds + 1 : 0;
      previous = current;
    }
  };
  try { await watchdog(drain(), timeoutMs, "消息状态持续变化，未能排空"); }
  finally { active = false; if (immediate !== undefined) clearImmediate(immediate); }
}

export function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => { resolve = done; });
  return { promise, resolve, wait: () => watchdog(promise) };
}
