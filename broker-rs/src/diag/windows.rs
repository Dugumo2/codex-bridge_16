//! Windows 句柄均由 RAII 持有；扩展属性在 CreateProcessW 返回前保持有效。

use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::mem::{size_of, size_of_val, zeroed};
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::path::{Path, PathBuf};
use std::ptr::{null, null_mut};
use std::time::{Duration, Instant};

use windows_sys::Win32::Foundation::*;
use windows_sys::Win32::System::Console::{GetStdHandle, STD_ERROR_HANDLE, STD_OUTPUT_HANDLE};
use windows_sys::Win32::System::JobObjects::*;
use windows_sys::Win32::System::Threading::*;

use super::{DiagArgs, Outcome};

fn error(context: &str) -> String {
    format!("{context}：{}", std::io::Error::last_os_error())
}

fn owned(handle: HANDLE, context: &str) -> Result<OwnedHandle, String> {
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        Err(error(context))
    } else {
        // 调用者交付的是新建且未被其他 RAII 对象接管的句柄。
        Ok(unsafe { OwnedHandle::from_raw_handle(handle) })
    }
}

fn wide(value: &OsStr) -> Result<Vec<u16>, String> {
    let mut units: Vec<_> = value.encode_wide().collect();
    if units.contains(&0) {
        return Err("程序路径或参数含空字符".into());
    }
    units.push(0);
    Ok(units)
}

/// Windows C 运行库的 argv 编码；以 UTF-16 单元处理，保留非 Unicode 路径。
fn crt_arg(value: &OsStr) -> Result<Vec<u16>, String> {
    let units = wide(value)?;
    let units = &units[..units.len() - 1];
    if !units.is_empty() && !units.iter().any(|c| [32, 9, 34].contains(c)) {
        return Ok(units.to_vec());
    }
    let mut result = vec![34];
    let mut slashes = 0;
    for &ch in units {
        if ch == 92 {
            slashes += 1;
            continue;
        }
        result.extend(std::iter::repeat_n(
            92,
            if ch == 34 { slashes * 2 + 1 } else { slashes },
        ));
        result.push(ch);
        slashes = 0;
    }
    result.extend(std::iter::repeat_n(92, slashes * 2));
    result.push(34);
    Ok(result)
}

fn command_line(command: &[OsString]) -> Result<(Vec<u16>, Vec<u16>), String> {
    let program = Path::new(&command[0]);
    let batch = program
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("cmd") || ext.eq_ignore_ascii_case("bat"));
    let (application, mut line) = if batch {
        let parts: Result<Vec<_>, _> = command
            .iter()
            .map(|v| {
                v.to_str()
                    .map(|s| (s, false))
                    .ok_or("批处理参数不是有效 Unicode".to_owned())
            })
            .collect();
        let tail = super::cmd_line(&parts?)?;
        let system = std::env::var_os("SystemRoot").ok_or("缺少系统目录 SystemRoot")?;
        let cmd = PathBuf::from(system).join("System32").join("cmd.exe");
        let mut line = crt_arg(cmd.as_os_str())?;
        line.push(32);
        line.extend(tail.encode_utf16());
        (wide(cmd.as_os_str())?, line)
    } else {
        let mut line = Vec::new();
        for (index, arg) in command.iter().enumerate() {
            if index != 0 {
                line.push(32);
            }
            line.extend(crt_arg(arg)?);
        }
        (wide(program.as_os_str())?, line)
    };
    line.push(0);
    Ok((application, line))
}

struct Attributes {
    // usize 提供所需对齐；分配后不再调整容量。
    storage: Vec<usize>,
    initialized: bool,
}

impl Attributes {
    fn new() -> Result<Self, String> {
        let mut bytes = 0;
        unsafe {
            InitializeProcThreadAttributeList(null_mut(), 2, 0, &mut bytes);
        }
        let mut this = Self {
            storage: vec![0; bytes.div_ceil(size_of::<usize>())],
            initialized: false,
        };
        if unsafe { InitializeProcThreadAttributeList(this.ptr(), 2, 0, &mut bytes) } == 0 {
            // 初始化失败，不能调用 DeleteProcThreadAttributeList。
            return Err(error("初始化进程属性失败"));
        }
        this.initialized = true;
        Ok(this)
    }

    fn ptr(&mut self) -> LPPROC_THREAD_ATTRIBUTE_LIST {
        self.storage.as_mut_ptr().cast()
    }

    fn set(&mut self, attribute: u32, values: &[HANDLE]) -> Result<(), String> {
        if unsafe {
            UpdateProcThreadAttribute(
                self.ptr(),
                0,
                attribute as usize,
                values.as_ptr().cast(),
                std::mem::size_of_val(values),
                null_mut(),
                null(),
            )
        } == 0
        {
            return Err(error("设置进程属性失败"));
        }
        Ok(())
    }
}

impl Drop for Attributes {
    fn drop(&mut self) {
        if self.initialized {
            unsafe {
                DeleteProcThreadAttributeList(self.ptr());
            }
        }
    }
}

fn inherited(source: HANDLE) -> Result<OwnedHandle, String> {
    let mut handle = null_mut();
    let current = unsafe { GetCurrentProcess() };
    if unsafe {
        DuplicateHandle(
            current,
            source,
            current,
            &mut handle,
            0,
            1,
            DUPLICATE_SAME_ACCESS,
        )
    } == 0
    {
        return Err(error("复制标准句柄失败"));
    }
    owned(handle, "复制标准句柄失败")
}

fn output_handle(kind: u32) -> Result<OwnedHandle, String> {
    let handle = unsafe { GetStdHandle(kind) };
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        let file = File::options()
            .write(true)
            .open("NUL")
            .map_err(|e| format!("打开空输出失败：{e}"))?;
        inherited(file.as_raw_handle())
    } else {
        inherited(handle)
    }
}

fn new_job() -> Result<OwnedHandle, String> {
    let job = owned(
        unsafe { CreateJobObjectW(null(), null()) },
        "创建诊断 Job 失败",
    )?;
    let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { zeroed() };
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    if unsafe {
        SetInformationJobObject(
            job.as_raw_handle(),
            JobObjectExtendedLimitInformation,
            (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
            size_of_val(&limits) as u32,
        )
    } == 0
    {
        return Err(error("设置诊断 Job 限制失败"));
    }
    Ok(job)
}

fn finish_job(job: HANDLE) -> Result<(), String> {
    if unsafe { TerminateJobObject(job, 125) } == 0 {
        return Err(error("终止诊断 Job 失败"));
    }
    wait_job_empty(job)
}

fn wait_job_empty(job: HANDLE) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let mut info: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION = unsafe { zeroed() };
        if unsafe {
            QueryInformationJobObject(
                job,
                JobObjectBasicAccountingInformation,
                (&mut info as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
                size_of_val(&info) as u32,
                null_mut(),
            )
        } == 0
        {
            return Err(error("查询诊断 Job 收尾失败"));
        }
        if info.ActiveProcesses == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("诊断 Job 收尾超过 2 秒".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

pub(super) fn execute(args: &DiagArgs) -> Result<Outcome, String> {
    let (application, mut line) = command_line(&args.command)?;
    let job = new_job()?;
    let null_input = File::open("NUL").map_err(|e| format!("打开空输入失败：{e}"))?;
    let stdin = inherited(null_input.as_raw_handle())?;
    let stdout = output_handle(STD_OUTPUT_HANDLE)?;
    let stderr = output_handle(STD_ERROR_HANDLE)?;
    let jobs = [job.as_raw_handle()];
    let handles = [
        stdin.as_raw_handle(),
        stdout.as_raw_handle(),
        stderr.as_raw_handle(),
    ];
    let mut attributes = Attributes::new()?;
    attributes.set(PROC_THREAD_ATTRIBUTE_JOB_LIST, &jobs)?;
    attributes.set(PROC_THREAD_ATTRIBUTE_HANDLE_LIST, &handles)?;
    let mut startup: STARTUPINFOEXW = unsafe { zeroed() };
    startup.StartupInfo.cb = size_of::<STARTUPINFOEXW>() as u32;
    startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    startup.StartupInfo.hStdInput = handles[0];
    startup.StartupInfo.hStdOutput = handles[1];
    startup.StartupInfo.hStdError = handles[2];
    startup.lpAttributeList = attributes.ptr();
    let mut info: PROCESS_INFORMATION = unsafe { zeroed() };
    // 挂起仅用于立即核实指定 Job 的成员身份；加入本身由内核在创建时完成。
    if unsafe {
        CreateProcessW(
            application.as_ptr(),
            line.as_mut_ptr(),
            null(),
            null(),
            1,
            EXTENDED_STARTUPINFO_PRESENT | CREATE_NO_WINDOW | CREATE_SUSPENDED,
            null(),
            null(),
            &startup.StartupInfo,
            &mut info,
        )
    } == 0
    {
        return Err(error("创建诊断进程失败"));
    }
    let process = owned(info.hProcess, "取得诊断进程句柄失败")?;
    let thread = owned(info.hThread, "取得诊断线程句柄失败")?;
    let result = (|| {
        let mut in_job = 0;
        if unsafe { IsProcessInJob(process.as_raw_handle(), job.as_raw_handle(), &mut in_job) } == 0
            || in_job == 0
        {
            // 防御性核实失败时也收束尚未运行的进程。
            unsafe {
                TerminateProcess(process.as_raw_handle(), 125);
                if WaitForSingleObject(process.as_raw_handle(), 2000) != WAIT_OBJECT_0 {
                    return Err("未加入指定 Job 的诊断进程未能按期回收".into());
                }
            }
            return Err("创建后的诊断进程不属于指定独立 Job".into());
        }
        if unsafe { ResumeThread(thread.as_raw_handle()) } == u32::MAX {
            return Err(error("恢复诊断进程失败"));
        }
        match unsafe { WaitForSingleObject(process.as_raw_handle(), args.timeout_ms as u32) } {
            WAIT_OBJECT_0 => {
                let mut exit_code = 0;
                if unsafe { GetExitCodeProcess(process.as_raw_handle(), &mut exit_code) } == 0 {
                    return Err(error("读取诊断退出码失败"));
                }
                Ok(Outcome::Exited { exit_code })
            }
            WAIT_TIMEOUT => Ok(Outcome::Timeout),
            _ => Err(error("等待诊断进程失败")),
        }
    })();
    // 正常退出同样要结束后代，先确认 Job 归零，再允许发布状态。
    finish_job(job.as_raw_handle())?;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 直接执行拒绝空字符() {
        assert!(crt_arg(OsStr::new("a\0b")).is_err());
    }
}
