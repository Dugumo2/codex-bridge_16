//! `cxb-broker launch`：经中间进程拉起 serve 后立即退出（详细设计第 5 节「broker 怎么拉起」）。
//!
//! - serve 的父进程（launch）随即退出，serve 不在任何调用方的进程树里，按父子关系杀树
//!   （Claude Code 停止后台任务）牵连不到它（S3-环境验证.md 追加一节）。
//! - 直接调 `CreateProcessW` 且**不继承任何句柄**：std 的 `Command` 在 Windows 上恒以
//!   `bInheritHandles=TRUE` 创建（`library/std/src/sys/process/windows.rs`），能关掉它的
//!   `inherit_handles` 尚未稳定。继承下去的话，launch 从调用方继承来的可继承句柄（例如调用方的
//!   stdout 管道）会被 serve 一直持有，调用方等管道 EOF 就要卡到 broker 退出。
//!   所以 serve 没有标准句柄；它的 stderr 输出被丢弃，文件日志在存储初始化后照常写。
//! - 先带 `CREATE_BREAKAWAY_FROM_JOB` 创建，失败再去掉重试一次；结果以隐藏参数
//!   `--launched-breakaway` 传给 serve，只作检测依据，存活判定由 serve 自查（[`crate::job`]）。
//! - 成功时 stdout 打一行 `{"pid":<候选 serve 的进程号>,"breakaway":<bool>}`。这个进程号只是刚创建的
//!   候选：它可能抢不到管道名而退出 0，也可能握手失败退出 1；调用方仍要连管道判断就绪（0.1 节）。
//! - 转义与创建方式沿用 S3 环境探测 内部探针（`lib.rs` 的 `command_line`、
//!   `job_breakaway.rs` 的 `launch`），那里已在本机实测过。

use std::ffi::OsStr;
use std::io;
use std::path::Path;

use crate::cli::ServeConfig;

/// launch 的结果：候选 serve 的进程号，以及本次是否带脱离标志创建成功。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Launched {
    pub pid: u32,
    pub breakaway: bool,
}

/// 两次创建都失败：保留各自的系统错误，不把首次失败一概说成「脱离被拒」。
#[derive(Debug)]
pub struct LaunchFailed {
    pub with_breakaway: io::Error,
    pub without_breakaway: io::Error,
}

impl std::fmt::Display for LaunchFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "带脱离标志创建失败（{}），不带脱离标志也失败（{}）",
            self.with_breakaway, self.without_breakaway
        )
    }
}

impl std::error::Error for LaunchFailed {}

/// 先带脱离标志试一次，只在创建失败时去掉标志重试一次；`create(breakaway)` 返回进程号。
/// 每次调用 `create` 都重新构造命令行（`--launched-breakaway` 的值随尝试而变）。
pub fn launch_with<F>(mut create: F) -> Result<Launched, LaunchFailed>
where
    F: FnMut(bool) -> io::Result<u32>,
{
    let with_breakaway = match create(true) {
        Ok(pid) => {
            return Ok(Launched {
                pid,
                breakaway: true,
            })
        }
        Err(error) => error,
    };
    tracing::info!(error = %with_breakaway, "带脱离标志创建 serve 失败，去掉标志重试");
    match create(false) {
        Ok(pid) => Ok(Launched {
            pid,
            breakaway: false,
        }),
        Err(without_breakaway) => Err(LaunchFailed {
            with_breakaway,
            without_breakaway,
        }),
    }
}

/// serve 的参数：与 launch 收到的相同（cwd 已转绝对路径），外加隐藏的脱离结果。
pub fn serve_arguments(
    endpoint: &str,
    config: &ServeConfig,
    breakaway: bool,
) -> Vec<std::ffi::OsString> {
    vec![
        "serve".into(),
        "--endpoint".into(),
        endpoint.into(),
        "--idle-timeout".into(),
        config.idle_timeout.as_millis().to_string().into(),
        "--cwd".into(),
        config.cwd.clone().into_os_string(),
        "--launched-breakaway".into(),
        breakaway.to_string().into(),
    ]
}

/// 拉起 serve 并返回结果；调用方负责打印 JSON 与退出码。
pub fn launch(endpoint: &str, config: &ServeConfig) -> Result<Launched, LaunchFailed> {
    launch_with(|breakaway| {
        let exe = std::env::current_exe()?;
        let arguments = serve_arguments(endpoint, config, breakaway);
        create_detached(&exe, &arguments, &config.cwd, breakaway)
    })
}

/// 成功时 launch 写到 stdout 的那一行。
pub fn report_line(launched: &Launched) -> String {
    serde_json::json!({"pid": launched.pid, "breakaway": launched.breakaway}).to_string()
}

/// 以 NUL 结尾的 UTF-16；内嵌 NUL 拒绝。
fn wide(value: &OsStr) -> io::Result<Vec<u16>> {
    use std::os::windows::ffi::OsStrExt;
    let mut result: Vec<u16> = value.encode_wide().collect();
    if result.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "路径或参数含有空字符",
        ));
    }
    result.push(0);
    Ok(result)
}

/// 按 Windows 参数解析规则（CommandLineToArgvW 与 MSVCRT 一致的部分）拼命令行：每个参数都加引号，
/// 引号前与参数尾部的反斜杠加倍，参数里的引号转义。返回以 NUL 结尾、可写的 UTF-16 缓冲。
pub fn command_line(arguments: &[&OsStr]) -> io::Result<Vec<u16>> {
    use std::os::windows::ffi::OsStrExt;
    let mut result = Vec::new();
    for (index, argument) in arguments.iter().enumerate() {
        if index > 0 {
            result.push(b' ' as u16);
        }
        result.push(b'"' as u16);
        let mut slashes = 0;
        for unit in argument.encode_wide() {
            if unit == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "命令行参数含有空字符",
                ));
            }
            if unit == b'\\' as u16 {
                slashes += 1;
            } else {
                let count = if unit == b'"' as u16 {
                    slashes * 2 + 1
                } else {
                    slashes
                };
                result.extend(std::iter::repeat_n(b'\\' as u16, count));
                result.push(unit);
                slashes = 0;
            }
        }
        result.extend(std::iter::repeat_n(b'\\' as u16, slashes * 2));
        result.push(b'"' as u16);
    }
    result.push(0);
    Ok(result)
}

/// 以 `DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP`（可选加 `CREATE_BREAKAWAY_FROM_JOB`）创建进程，
/// 不继承句柄、不设标准句柄；成功后关闭进程与线程句柄（不影响子进程），返回进程号。
pub fn create_detached(
    exe: &Path,
    arguments: &[std::ffi::OsString],
    cwd: &Path,
    breakaway: bool,
) -> io::Result<u32> {
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{
        CreateProcessW, CREATE_BREAKAWAY_FROM_JOB, CREATE_NEW_PROCESS_GROUP,
        CREATE_UNICODE_ENVIRONMENT, DETACHED_PROCESS, PROCESS_INFORMATION, STARTUPINFOW,
    };

    let application = wide(exe.as_os_str())?;
    let mut all: Vec<&OsStr> = vec![exe.as_os_str()];
    all.extend(arguments.iter().map(|a| a.as_os_str()));
    let mut line = command_line(&all)?;
    let directory = wide(cwd.as_os_str())?;
    // 安全依据：两个结构按 Windows 约定以全零初始化，cb 填结构大小。
    let mut startup: STARTUPINFOW = unsafe { std::mem::zeroed() };
    startup.cb = std::mem::size_of::<STARTUPINFOW>() as u32;
    let mut information: PROCESS_INFORMATION = unsafe { std::mem::zeroed() };
    let flags = DETACHED_PROCESS
        | CREATE_NEW_PROCESS_GROUP
        | CREATE_UNICODE_ENVIRONMENT
        | if breakaway {
            CREATE_BREAKAWAY_FROM_JOB
        } else {
            0
        };
    // 安全依据：程序路径、工作目录为 NUL 结尾缓冲，命令行为可写缓冲，均在调用期间有效；
    // 环境块传空表示继承当前环境；bInheritHandles 为 0，子进程拿不到本进程的任何句柄。
    let ok = unsafe {
        CreateProcessW(
            application.as_ptr(),
            line.as_mut_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            0,
            flags,
            std::ptr::null(),
            directory.as_ptr(),
            &startup,
            &mut information,
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    // 安全依据：两个句柄由 CreateProcessW 返回、归本函数所有，各关闭一次；关闭不会结束子进程。
    unsafe {
        CloseHandle(information.hThread);
        CloseHandle(information.hProcess);
    }
    Ok(information.dwProcessId)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::os::windows::ffi::OsStringExt;
    use std::time::Duration;

    #[link(name = "shell32")]
    extern "system" {
        fn CommandLineToArgvW(line: *const u16, count: *mut i32) -> *mut *mut u16;
    }
    #[link(name = "kernel32")]
    extern "system" {
        fn LocalFree(memory: *mut std::ffi::c_void) -> *mut std::ffi::c_void;
    }

    /// 用系统的解析函数把命令行拆回参数（只作交叉校验；真实子进程的校验在集成测试里）。
    fn split(line: &[u16]) -> Vec<OsString> {
        let mut count = 0;
        // 安全依据：line 以 NUL 结尾；返回的数组按文档用 LocalFree 释放一次。
        unsafe {
            let argv = CommandLineToArgvW(line.as_ptr(), &mut count);
            assert!(!argv.is_null());
            let mut result = Vec::new();
            for index in 0..count as usize {
                let pointer = *argv.add(index);
                let mut length = 0;
                while *pointer.add(length) != 0 {
                    length += 1;
                }
                result.push(OsString::from_wide(std::slice::from_raw_parts(
                    pointer, length,
                )));
            }
            LocalFree(argv.cast());
            result
        }
    }

    #[test]
    fn 命令行转义经系统解析原样还原() {
        let cases = [
            r"C:\Program Files\cxb\cxb-broker.exe",
            "serve",
            "",
            "带 空格",
            "中文参数",
            r#"含"引号"#,
            r#"引号前反斜杠\"x"#,
            r"末尾反斜杠\",
            r"末尾两个反斜杠\\",
            r"\\server\share\dir",
            "\t制表符",
        ];
        let arguments: Vec<&OsStr> = cases.iter().map(OsStr::new).collect();
        let line = command_line(&arguments).unwrap();
        let parsed = split(&line);
        assert_eq!(parsed, cases.iter().map(OsString::from).collect::<Vec<_>>());
    }

    #[test]
    fn 参数含空字符被拒() {
        assert!(command_line(&[OsStr::new("a\0b")]).is_err());
        assert!(wide(OsStr::new("a\0b")).is_err());
    }

    #[test]
    fn 带脱离标志成功就不再重试() {
        let mut attempts = Vec::new();
        let result = launch_with(|breakaway| {
            attempts.push(breakaway);
            Ok(42)
        })
        .unwrap();
        assert_eq!(
            result,
            Launched {
                pid: 42,
                breakaway: true
            }
        );
        assert_eq!(attempts, [true]);
    }

    #[test]
    fn 脱离被拒后去掉标志重试一次() {
        let mut attempts = Vec::new();
        let result = launch_with(|breakaway| {
            attempts.push(breakaway);
            if breakaway {
                Err(io::Error::from_raw_os_error(5))
            } else {
                Ok(7)
            }
        })
        .unwrap();
        assert_eq!(
            result,
            Launched {
                pid: 7,
                breakaway: false
            }
        );
        assert_eq!(attempts, [true, false]);
    }

    #[test]
    fn 两次都失败时保留各自错误() {
        let error = launch_with(|breakaway| {
            Err(io::Error::from_raw_os_error(if breakaway { 5 } else { 2 }))
        })
        .unwrap_err();
        assert_eq!(error.with_breakaway.raw_os_error(), Some(5));
        assert_eq!(error.without_breakaway.raw_os_error(), Some(2));
        let text = error.to_string();
        assert!(text.contains("带脱离标志创建失败") && text.contains("不带脱离标志也失败"));
    }

    #[test]
    fn serve_参数带绝对工作区与脱离结果() {
        let config = ServeConfig {
            endpoint: crate::endpoint::Endpoint::Pipe("x".into()),
            cwd: std::path::PathBuf::from(r"D:\带 空格\ws"),
            idle_timeout: Duration::from_millis(1500),
        };
        let arguments = serve_arguments("pipe:x", &config, false);
        let text: Vec<String> = arguments
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            text,
            [
                "serve",
                "--endpoint",
                "pipe:x",
                "--idle-timeout",
                "1500",
                "--cwd",
                r"D:\带 空格\ws",
                "--launched-breakaway",
                "false"
            ]
        );
    }

    #[test]
    fn 输出行只有进程号与脱离结果() {
        let line = report_line(&Launched {
            pid: 9,
            breakaway: false,
        });
        let value: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(value, serde_json::json!({"pid": 9, "breakaway": false}));
        assert!(!line.contains('\n'));
    }
}
