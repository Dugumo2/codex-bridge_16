//! Windows Job Object（详细设计第 5 节）。
//!
//! broker 启动第一件事：创建一个设了 `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` 的 Job，把自身加入。
//! 之后 spawn 的子进程（私有 app-server、`cmd.exe` 外壳及其子孙、第 2 档的 proxy）自动属于它；
//! broker 无论怎样退出（包括崩溃、被 `taskkill /F`），最后一个句柄关闭时系统结束整棵树。
//! 这取代上游的 startupGuard 循环和 `taskkill /T /F`。
//!
//! - 句柄不可继承：`CreateJobObjectW` 的安全属性传空，子进程拿不到这个句柄，
//!   所以 broker 退出就是最后一个句柄关闭。
//! - 嵌套 Job 可用：S3 环境验证实测，已在 Claude Code 的 Job 里的进程仍能加入自己的 Job。
//! - [`BrokerJob`] 必须一直持有到进程退出；提前 drop 会立刻结束所有子进程以及 broker 自身。

use std::sync::OnceLock;

use win32job::{ExtendedLimitInfo, Job, JobError};

/// 进程级的 Job：静态变量不会被析构，句柄一直保持到进程退出，由系统关闭。
static PROCESS_JOB: OnceLock<BrokerJob> = OnceLock::new();

/// broker 自己的 Job。只要它还活着，Job 就在；进程退出时由系统关闭句柄。
#[derive(Debug)]
pub struct BrokerJob {
    job: Job,
}

impl BrokerJob {
    /// 创建设了「关闭即结束」的 Job（尚未加入任何进程）。
    pub fn new_kill_on_close() -> Result<Self, JobError> {
        let mut info = ExtendedLimitInfo::new();
        info.limit_kill_on_job_close();
        let job = Job::create_with_limit_info(&info)?;
        Ok(Self { job })
    }

    /// 创建 Job 并把当前进程加入。失败时调用方记告警后继续运行：没有 Job 时子进程
    /// 只能靠关闭流程回收，崩溃时可能残留，但不影响正常服务。
    fn adopt_current_process() -> Result<Self, JobError> {
        let job = Self::new_kill_on_close()?;
        job.job.assign_current_process()?;
        Ok(job)
    }

    /// 创建 Job、把当前进程加入，并保存到进程级静态变量，保证句柄活到进程退出。
    /// 不能把返回值放在局部变量里：局部变量一释放，Job 关闭，系统会立刻结束包括 broker 自身在内的所有进程。
    /// 重复调用返回第一次创建的那个。
    pub fn adopt_for_process_lifetime() -> Result<&'static BrokerJob, JobError> {
        if let Some(job) = PROCESS_JOB.get() {
            return Ok(job);
        }
        let mut created = Some(Self::adopt_current_process()?);
        let stored = PROCESS_JOB.get_or_init(|| created.take().expect("只取一次"));
        if let Some(extra) = created {
            // 并发调用时多建的那个 Job 也已经包含当前进程，释放它同样会结束进程，只能放着不关。
            std::mem::forget(extra);
        }
        Ok(stored)
    }

    /// 把指定进程加入本 Job（测试和将来的显式加入用）。
    pub fn assign(&self, process_handle: isize) -> Result<(), JobError> {
        self.job.assign_process(process_handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::windows::io::AsRawHandle;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    #[test]
    fn 释放_job_时其中的进程被结束() {
        let job = BrokerJob::new_kill_on_close().expect("创建 Job");
        // ping 会持续运行约 30 秒，足够观察是否被 Job 结束。
        let mut child = Command::new("ping")
            .args(["-n", "30", "127.0.0.1"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("启动 ping");
        job.assign(child.as_raw_handle() as isize)
            .expect("加入 Job");
        assert!(child.try_wait().unwrap().is_none(), "加入后应仍在运行");

        drop(job);
        let deadline = Instant::now() + Duration::from_secs(5);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break Some(status);
            }
            if Instant::now() >= deadline {
                break None;
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        if status.is_none() {
            let _ = child.kill();
        }
        assert!(status.is_some(), "释放 Job 后 5 秒内子进程应被结束");
    }
}

/// 外层 Job 的检测结论与依据（详细设计第 5 节、S1-评审第 24 节）。
///
/// 必须在加入自身 Job 前检测；允许脱离不等于已经脱离（任务说明 8.4.8）。依据对象的字段：
/// - `inJob`、`killOnClose`：serve 自查的结果，存活判定只看这两项；
/// - `breakawayAllowed`：所查 Job 的限制标志是否允许脱离（BREAKAWAY_OK 或 SILENT_BREAKAWAY_OK）；
/// - `breakaway`：launch 本次是否带 `CREATE_BREAKAWAY_FROM_JOB` 创建成功，直接启动 serve 时为 null。
///   为 true 也不等于「已证明脱离全部 Job」：调用方原本不在 Job 里时创建同样会成功；
/// - `queryScope`：固定为 `immediateJob`。`QueryInformationJobObject` 传 NULL 时只查直接所属的 Job
///   （微软文档：If the job is nested, the immediate job of the calling process is used），
///   外层祖先 Job 的设置测不出；
/// - 检测失败时 `inJob`、`killOnClose`、`breakawayAllowed` 为 null，`breakaway` 与 `queryScope` 照常，另带 `error`。
#[derive(Debug, Clone)]
pub struct ParentJob {
    pub survives: bool,
    pub evidence: serde_json::Value,
}

// ABI 对照 Windows SDK winnt.h 的 JOBOBJECT_BASIC_LIMIT_INFORMATION、
// IO_COUNTERS 与 JOBOBJECT_EXTENDED_LIMIT_INFORMATION；保持现有依赖与功能开关不变。
#[repr(C)]
#[derive(Default)]
struct BasicLimits {
    process_time: i64,
    job_time: i64,
    flags: u32,
    min_working_set: usize,
    max_working_set: usize,
    active_processes: u32,
    affinity: usize,
    priority: u32,
    scheduling: u32,
}
#[repr(C)]
#[derive(Default)]
struct ExtendedLimits {
    basic: BasicLimits,
    io: [u64; 6],
    process_memory: usize,
    job_memory: usize,
    peak_process_memory: usize,
    peak_job_memory: usize,
}
#[link(name = "kernel32")]
extern "system" {
    fn IsProcessInJob(
        process: *mut std::ffi::c_void,
        job: *mut std::ffi::c_void,
        result: *mut i32,
    ) -> i32;
    fn QueryInformationJobObject(
        job: *mut std::ffi::c_void,
        class: i32,
        info: *mut std::ffi::c_void,
        length: u32,
        returned: *mut u32,
    ) -> i32;
}
const QUERY_SCOPE: &str = "immediateJob";

fn parent_job_result(in_job: bool, flags: u32, launched_breakaway: Option<bool>) -> ParentJob {
    let kill = flags & 0x2000 != 0;
    let allowed = flags & (0x800 | 0x1000) != 0;
    ParentJob {
        survives: !in_job || !kill,
        evidence: serde_json::json!({
            "inJob": in_job,
            "killOnClose": kill,
            "breakawayAllowed": allowed,
            "breakaway": launched_breakaway,
            "queryScope": QUERY_SCOPE,
        }),
    }
}

/// `launched_breakaway` 是 launch 经隐藏参数传来的结果（[`crate::launch`]），只记作依据。
pub fn detect_parent_job(launched_breakaway: Option<bool>) -> ParentJob {
    let detect = || -> std::io::Result<ParentJob> {
        let mut in_job = 0;
        // 指针指向可写本地变量；NULL Job 查询当前进程所属 Job，不创建或接管任何句柄。
        unsafe {
            if IsProcessInJob(
                windows_sys::Win32::System::Threading::GetCurrentProcess(),
                std::ptr::null_mut(),
                &mut in_job,
            ) == 0
            {
                return Err(std::io::Error::last_os_error());
            }
            if in_job == 0 {
                return Ok(parent_job_result(false, 0, launched_breakaway));
            }
            let mut info = ExtendedLimits::default();
            if QueryInformationJobObject(
                std::ptr::null_mut(),
                9,
                (&mut info as *mut ExtendedLimits).cast(),
                std::mem::size_of::<ExtendedLimits>() as u32,
                std::ptr::null_mut(),
            ) == 0
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(parent_job_result(
                true,
                info.basic.flags,
                launched_breakaway,
            ))
        }
    };
    detect().unwrap_or_else(|error| {
        tracing::warn!(%error, "父 Job 检测失败，按不能保证存活报告");
        detection_failed(&error, launched_breakaway)
    })
}

fn detection_failed(error: &std::io::Error, launched_breakaway: Option<bool>) -> ParentJob {
    ParentJob {
        survives: false,
        evidence: serde_json::json!({
            "inJob": null,
            "killOnClose": null,
            "breakawayAllowed": null,
            "breakaway": launched_breakaway,
            "queryScope": QUERY_SCOPE,
            "error": error.to_string(),
        }),
    }
}

/// 恢复以 `CREATE_SUSPENDED` 创建的进程的主线程。用法：挂起创建 → 加入 Job → 再恢复，
/// 堵住「先生成子孙、再加入 Job」的竞态（M7 第 2 档；M8 测试把 launch 放进指定 Job 也用它）。
/// 挂起创建的进程只有主线程，所以按进程号在线程快照里找到的第一个线程就是它。
pub fn resume_suspended(pid: u32) -> Result<(), String> {
    use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, Thread32First, Thread32Next, TH32CS_SNAPTHREAD, THREADENTRY32,
    };
    use windows_sys::Win32::System::Threading::{OpenThread, ResumeThread, THREAD_SUSPEND_RESUME};
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
    if snapshot == INVALID_HANDLE_VALUE {
        return Err(format!(
            "获取挂起进程的线程快照失败：{}",
            std::io::Error::last_os_error()
        ));
    }
    let mut entry: THREADENTRY32 = unsafe { std::mem::zeroed() };
    entry.dwSize = std::mem::size_of::<THREADENTRY32>() as u32;
    let mut found = unsafe { Thread32First(snapshot, &mut entry) };
    let mut result = Err("找不到挂起进程的主线程".into());
    while found != 0 {
        if entry.th32OwnerProcessID == pid {
            let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) };
            if thread.is_null() {
                result = Err(format!(
                    "打开挂起进程的主线程失败：{}",
                    std::io::Error::last_os_error()
                ));
            } else {
                result = if unsafe { ResumeThread(thread) } == u32::MAX {
                    Err(format!(
                        "恢复挂起进程的主线程失败：{}",
                        std::io::Error::last_os_error()
                    ))
                } else {
                    Ok(())
                };
                unsafe {
                    CloseHandle(thread);
                }
            }
            break;
        }
        found = unsafe { Thread32Next(snapshot, &mut entry) };
    }
    unsafe {
        CloseHandle(snapshot);
    }
    result
}

#[cfg(test)]
mod parent_tests {
    use super::*;
    #[test]
    fn 允许脱离不能抵消仍在父任务中的关闭风险() {
        assert!(parent_job_result(false, 0, None).survives);
        assert!(parent_job_result(true, 0, None).survives);
        for flags in [0x2000, 0x2000 | 0x800, 0x2000 | 0x1000] {
            let result = parent_job_result(true, flags, None);
            assert!(!result.survives);
            assert_eq!(result.evidence["killOnClose"], true);
        }
        assert_eq!(std::mem::size_of::<ExtendedLimits>(), 144);
    }

    #[test]
    fn 依据区分允许脱离与实际脱离且只查直接所属_job() {
        let allowed = parent_job_result(true, 0x800, Some(false));
        assert_eq!(
            allowed.evidence,
            serde_json::json!({"inJob": true, "killOnClose": false, "breakawayAllowed": true,
                "breakaway": false, "queryScope": "immediateJob"})
        );
        // 脱离成功只作依据：判定仍看 serve 自查到的 Job。
        let launched = parent_job_result(true, 0x2000, Some(true));
        assert!(!launched.survives);
        assert_eq!(launched.evidence["breakaway"], true);
        assert_eq!(launched.evidence["breakawayAllowed"], false);
        let direct = parent_job_result(false, 0, None);
        assert!(direct.evidence["breakaway"].is_null());
    }

    #[test]
    fn 检测失败按不能保证存活并保留错误与_launch_结果() {
        let failed = detection_failed(&std::io::Error::from_raw_os_error(5), Some(false));
        assert!(!failed.survives);
        assert!(failed.evidence["inJob"].is_null());
        assert!(failed.evidence["breakawayAllowed"].is_null());
        assert_eq!(failed.evidence["breakaway"], false);
        assert_eq!(failed.evidence["queryScope"], "immediateJob");
        assert!(failed.evidence["error"].is_string());
    }

    #[test]
    fn 本进程检测结果字段齐全() {
        let detected = detect_parent_job(None);
        for field in [
            "inJob",
            "killOnClose",
            "breakawayAllowed",
            "breakaway",
            "queryScope",
        ] {
            assert!(detected.evidence.get(field).is_some(), "缺少 {field}");
        }
        assert_eq!(detected.evidence["error"], serde_json::Value::Null);
    }
}
