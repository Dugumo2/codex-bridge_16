/**
 * 跟踪某个进程的全部后代，只用于发现残留，不结束任何进程。
 * 走 Toolhelp 快照而不是 WMI 或外部命令：约 20 毫秒一次、不起子进程，分片运行时可以每秒采样。
 * 局限：两次采样之间生灭的中间进程看不到，它留下的孙进程可能漏报。
 */
import { dlopen, FFIType, ptr } from "bun:ffi";

export interface ProcessEntry { pid: number; parent: number; name: string }
export interface TrackedProcess { created: bigint; name: string }

/**
 * 用一次快照更新跟踪表：退出的移除；父进程在表内的新进程纳入。
 * 父 PID 不会随父进程退出而更新，比父进程还早创建的只能是 PID 复用的巧合，不算后代。
 */
export function updateDescendants(tracked: Map<number, TrackedProcess>, snapshot: ProcessEntry[], createdOf: (pid: number) => bigint | undefined): void {
  const present = new Set(snapshot.map(entry => entry.pid));
  for (const [pid, info] of tracked) {
    if (!present.has(pid) || createdOf(pid) !== info.created) tracked.delete(pid);
  }
  // 快照顺序与父子关系无关，纳入到不再增长为止。
  for (let grew = true; grew;) {
    grew = false;
    for (const entry of snapshot) {
      if (tracked.has(entry.pid)) continue;
      const parent = tracked.get(entry.parent);
      if (!parent) continue;
      const created = createdOf(entry.pid);
      if (created === undefined || created < parent.created) continue;
      tracked.set(entry.pid, { created, name: entry.name });
      grew = true;
    }
  }
}

const TH32CS_SNAPPROCESS = 2;
const PROCESS_QUERY_LIMITED_INFORMATION = 0x1000;
const INVALID_HANDLE = 0xffffffffffffffffn;
// PROCESSENTRY32W 在 64 位下的布局。
const ENTRY_SIZE = 568, PID_OFFSET = 8, PARENT_OFFSET = 32, NAME_OFFSET = 44;

let kernel32: ReturnType<typeof loadKernel32> | undefined;
function loadKernel32() {
  return dlopen("kernel32.dll", {
    CreateToolhelp32Snapshot: { args: [FFIType.u32, FFIType.u32], returns: FFIType.u64 },
    Process32FirstW: { args: [FFIType.u64, FFIType.ptr], returns: FFIType.i32 },
    Process32NextW: { args: [FFIType.u64, FFIType.ptr], returns: FFIType.i32 },
    CloseHandle: { args: [FFIType.u64], returns: FFIType.i32 },
    OpenProcess: { args: [FFIType.u32, FFIType.i32, FFIType.u32], returns: FFIType.u64 },
    GetProcessTimes: { args: [FFIType.u64, FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.ptr], returns: FFIType.i32 },
  }).symbols;
}
function api() {
  if (process.platform !== "win32") throw new Error("进程快照只支持 Windows");
  return kernel32 ??= loadKernel32();
}

export function snapshotProcesses(): ProcessEntry[] {
  const k = api();
  const snapshot = BigInt(k.CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0));
  if (snapshot === INVALID_HANDLE) throw new Error("CreateToolhelp32Snapshot 失败");
  try {
    const buffer = new Uint8Array(ENTRY_SIZE), view = new DataView(buffer.buffer), entries: ProcessEntry[] = [];
    view.setUint32(0, ENTRY_SIZE, true);
    for (let ok = k.Process32FirstW(snapshot, ptr(buffer)); ok; ok = k.Process32NextW(snapshot, ptr(buffer))) {
      let name = "";
      for (let offset = NAME_OFFSET; offset + 1 < ENTRY_SIZE; offset += 2) {
        const unit = view.getUint16(offset, true);
        if (!unit) break;
        name += String.fromCharCode(unit);
      }
      entries.push({ pid: view.getUint32(PID_OFFSET, true), parent: view.getUint32(PARENT_OFFSET, true), name });
    }
    return entries;
  } finally { k.CloseHandle(snapshot); }
}

/** 进程创建时间；进程已退出或无权查询时返回 undefined。 */
export function processCreated(pid: number): bigint | undefined {
  const k = api();
  const handle = BigInt(k.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid));
  if (handle === 0n) return undefined;
  try {
    const times = new BigUint64Array(4);
    if (!k.GetProcessTimes(handle, ptr(times, 0), ptr(times, 8), ptr(times, 16), ptr(times, 24))) return undefined;
    return times[0];
  } finally { k.CloseHandle(handle); }
}

export class DescendantWatcher {
  readonly tracked = new Map<number, TrackedProcess>();

  addRoot(pid: number, name: string): void {
    const created = processCreated(pid);
    if (created !== undefined) this.tracked.set(pid, { created, name });
  }

  sample(): void {
    updateDescendants(this.tracked, snapshotProcesses(), processCreated);
  }
}
