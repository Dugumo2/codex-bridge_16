#![cfg(windows)]
//! 独立进程契约验证：登记 PID 与创建时间，清理时逐项确认退出。

use std::ffi::OsStr;
use std::io;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};
use windows_sys::Win32::Foundation::*;
use windows_sys::Win32::System::Threading::*;

const BROKER: &str = env!("CARGO_BIN_EXE_cxb-broker");
const GUARD: Duration = Duration::from_secs(12);

struct Process {
    pid: u32,
    created: u64,
    handle: OwnedHandle,
}

fn created(handle: HANDLE) -> io::Result<u64> {
    let mut times = [FILETIME::default(); 4];
    if unsafe {
        GetProcessTimes(
            handle,
            &mut times[0],
            &mut times[1],
            &mut times[2],
            &mut times[3],
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok((u64::from(times[0].dwHighDateTime) << 32) | u64::from(times[0].dwLowDateTime))
}

impl Process {
    fn open(pid: u32) -> io::Result<Self> {
        let handle = unsafe {
            OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_TERMINATE | PROCESS_SYNCHRONIZE,
                0,
                pid,
            )
        };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        let handle = unsafe { OwnedHandle::from_raw_handle(handle) };
        Ok(Self {
            pid,
            created: created(handle.as_raw_handle())?,
            handle,
        })
    }

    fn from_child(child: &Child) -> Self {
        let pid = child.id().expect("子进程尚未等待");
        let mut handle = std::ptr::null_mut();
        let current = unsafe { GetCurrentProcess() };
        assert_ne!(
            unsafe {
                DuplicateHandle(
                    current,
                    child.raw_handle().unwrap(),
                    current,
                    &mut handle,
                    0,
                    0,
                    DUPLICATE_SAME_ACCESS,
                )
            },
            0,
            "登记子进程句柄"
        );
        let handle = unsafe { OwnedHandle::from_raw_handle(handle) };
        Self {
            pid,
            created: created(handle.as_raw_handle()).expect("登记创建时间"),
            handle,
        }
    }

    fn ended(&self) -> bool {
        unsafe { WaitForSingleObject(self.handle.as_raw_handle(), 0) == WAIT_OBJECT_0 }
    }

    fn cleanup(&self) -> Result<(), String> {
        if created(self.handle.as_raw_handle()).map_err(|e| e.to_string())? != self.created {
            return Err(format!("进程 {} 创建时间不符", self.pid));
        }
        if !self.ended() {
            if unsafe { TerminateProcess(self.handle.as_raw_handle(), 125) } == 0 && !self.ended() {
                return Err(format!("结束进程 {} 失败", self.pid));
            }
            if unsafe { WaitForSingleObject(self.handle.as_raw_handle(), 2000) } != WAIT_OBJECT_0 {
                return Err(format!("进程 {} 未在清理期限内退出", self.pid));
            }
        }
        Ok(())
    }
}

struct Harness {
    root: PathBuf,
    processes: Vec<Process>,
}

impl Harness {
    async fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let salt = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cxb-n7a-{}-{salt}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).expect("创建独立临时目录");
        let mut this = Self {
            root,
            processes: Vec::new(),
        };
        let mut command = this.command("rustc");
        command
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/diag-child.rs"))
            .arg("--edition=2021")
            .arg("-o")
            .arg(this.program());
        let mut compiler = this.spawn(command);
        let output = capture(&mut compiler).await;
        assert!(
            output.0.success(),
            "编译诊断夹具失败：{}",
            String::from_utf8_lossy(&output.2)
        );
        this
    }

    fn program(&self) -> PathBuf {
        self.root.join("diagnostic child.exe")
    }
    fn status(&self) -> PathBuf {
        self.root.join("status.json")
    }

    fn command(&self, program: impl AsRef<OsStr>) -> Command {
        let mut command = Command::new(program);
        for (key, _) in std::env::vars_os() {
            let name = key.to_string_lossy().to_ascii_uppercase();
            if name.starts_with("CXB_")
                || name.starts_with("CODEX_")
                || name.contains("SOCKET")
                || name.contains("ENDPOINT")
            {
                command.env_remove(key);
            }
        }
        for key in ["CXB_HOME", "USERPROFILE", "HOME", "CODEX_HOME"] {
            command.env(key, &self.root);
        }
        command
            .env("CXB_SERVER", "private")
            .env("CXB_DIAG_FIXTURE_ROOT", &self.root)
            .env("CXB_DIAG_FIXTURE_MODE", "echo")
            .current_dir(&self.root)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .creation_flags(CREATE_NO_WINDOW)
            .kill_on_drop(true);
        command
    }

    fn diag(&self, timeout: u32, program: &Path) -> Command {
        let mut command = self.command(BROKER);
        command
            .args([
                "diag-exec",
                "--timeout-ms",
                &timeout.to_string(),
                "--status-file",
            ])
            .arg(self.status())
            .arg("--")
            .arg(program);
        command
    }

    fn spawn(&mut self, mut command: Command) -> Child {
        let child = command.spawn().expect("启动测试进程");
        self.processes.push(Process::from_child(&child));
        child
    }

    fn status_value(&self) -> Value {
        serde_json::from_slice(&std::fs::read(self.status()).expect("必须生成状态文件"))
            .expect("完整状态 JSON")
    }

    fn descendants(&mut self) -> Vec<usize> {
        let mut indexes = Vec::new();
        for entry in std::fs::read_dir(&self.root).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|v| v == "identity") {
                let contents = std::fs::read_to_string(path).unwrap();
                let pair: Vec<_> = contents.split_whitespace().collect();
                let pid = pair[0].parse().unwrap();
                let start: u64 = pair[1].parse().unwrap();
                let process = Process::open(pid).expect("在放行前登记仍存活的子进程");
                assert_eq!(process.created, start, "PID 不得复用");
                indexes.push(self.processes.len());
                self.processes.push(process);
            }
        }
        indexes
    }

    fn assert_no_broker_data(&self) {
        for path in ["workspaces", "logs", ".codex-bridge", ".cxb-legacy"] {
            assert!(
                !self.root.join(path).exists(),
                "诊断不能初始化 broker 数据或日志"
            );
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let mut errors = Vec::new();
        // 先结束已登记的包装器，冻结进程树，再按身份文件补收夹具后代。
        for process in &self.processes {
            if let Err(error) = process.cleanup() {
                errors.push(error);
            }
        }
        match std::fs::read_dir(&self.root) {
            Ok(entries) => {
                for entry in entries {
                    let result = (|| -> Result<(), String> {
                        let path = entry.map_err(|e| e.to_string())?.path();
                        if !path.extension().is_some_and(|v| v == "identity") {
                            return Ok(());
                        }
                        let data = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
                        let pair: Vec<_> = data.split_whitespace().collect();
                        let pid = pair[0].parse().map_err(|e| format!("进程登记损坏：{e}"))?;
                        let start: u64 =
                            pair[1].parse().map_err(|e| format!("创建时间损坏：{e}"))?;
                        match Process::open(pid) {
                            Ok(process) if process.created == start => process.cleanup(),
                            Ok(_) => Ok(()), // PID 已复用，绝不能结束另一个进程。
                            Err(error)
                                if error.raw_os_error() == Some(ERROR_INVALID_PARAMETER as i32) =>
                            {
                                Ok(())
                            }
                            Err(error) => Err(error.to_string()),
                        }
                    })();
                    if let Err(error) = result {
                        errors.push(error);
                    }
                }
            }
            Err(error) => errors.push(error.to_string()),
        }
        self.processes.clear();
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            match std::fs::remove_dir_all(&self.root) {
                Ok(()) => break,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20))
                }
                Err(error) => {
                    errors.push(error.to_string());
                    break;
                }
            }
        }
        if self.root.exists() {
            errors.push("临时目录清理后仍存在".into());
        }
        if !errors.is_empty() {
            if std::thread::panicking() {
                eprintln!("测试收尾失败：{errors:?}");
            } else {
                panic!("测试收尾失败：{errors:?}");
            }
        }
    }
}

async fn capture(child: &mut Child) -> (ExitStatus, Vec<u8>, Vec<u8>) {
    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let mut out = Vec::new();
    let mut err = Vec::new();
    tokio::time::timeout(GUARD, async {
        let (status, output, error) = tokio::join!(
            child.wait(),
            stdout.read_to_end(&mut out),
            stderr.read_to_end(&mut err)
        );
        output.expect("读取标准输出");
        error.expect("读取标准错误");
        (status.expect("等待包装器"), out, err)
    })
    .await
    .expect("进程及输出 EOF 必须在防挂期限内结束")
}

async fn ready(path: &Path) {
    tokio::time::timeout(GUARD, async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("夹具控制点必须按期到达");
}

#[tokio::test]
async fn 正常退出完整状态与原样输出及创建时_job_核实() {
    let mut harness = Harness::new().await;
    for code in [0u32, 7, 124, 125, 256, 0x8000_0001, u32::MAX] {
        let mut command = harness.diag(5000, &harness.program());
        command.env("CXB_DIAG_FIXTURE_CODE", code.to_string());
        let mut child = harness.spawn(command);
        let (status, stdout, stderr) = capture(&mut child).await;
        assert_eq!(
            harness.status_value(),
            json!({"outcome":"exited","exitCode":code}),
            "必须在创建后立即核实独立 Job，且状态保留原值"
        );
        assert_eq!(status.code().unwrap() as u32, code, "包装器退出码不得截断");
        assert_eq!(stdout, b"stdout\0\xff\r\n");
        assert_eq!(stderr, b"stderr\0\xfe\r\n");
    }
    harness.assert_no_broker_data();
}

#[tokio::test]
async fn 直接执行参数逐字相等() {
    let mut harness = Harness::new().await;
    let args = [
        "",
        "含 空格",
        "尾\\",
        "有 空格\\",
        "含\"引号",
        "反\\\"引号",
        "双\\\\\"引号",
        "制\t表",
        "&|<>^%!()",
        "换\n行",
    ];
    let mut command = harness.diag(5000, &harness.program());
    command.args(args);
    let mut child = harness.spawn(command);
    assert!(capture(&mut child).await.0.success());
    let actual: Vec<String> =
        serde_json::from_slice(&std::fs::read(harness.root.join("argv.json")).unwrap()).unwrap();
    assert_eq!(actual, args);
}

#[tokio::test]
async fn 批处理参数逐字相等且程序路径含空格() {
    let mut harness = Harness::new().await;
    // 批处理逐个提取 cmd 收到的参数，直接写 JSON；不能再经 CRT 编码掩盖反斜杠错误。
    let batch = harness.root.join("diagnostic script.CmD");
    std::fs::write(&batch, concat!(
        "@echo off\r\nsetlocal\r\n",
        "set \"a=x%~1\"\r\nset \"b=x%~2\"\r\nset \"c=x%~3\"\r\nset \"d=x%~4\"\r\n",
        "set \"a=%a:\\=\\\\%\"\r\nset \"b=%b:\\=\\\\%\"\r\nset \"c=%c:\\=\\\\%\"\r\nset \"d=%d:\\=\\\\%\"\r\n",
        "> \"%CXB_DIAG_FIXTURE_ROOT%\\batch.json\" echo [\"%a:~1%\",\"%b:~1%\",\"%c:~1%\",\"%d:~1%\"]\r\n"
    )).unwrap();
    let args = ["", "with space", "tail\\", "with space\\"];
    let mut command = harness.diag(5000, &batch);
    command.args(args);
    let mut child = harness.spawn(command);
    let output = capture(&mut child).await;
    assert!(
        output.0.success(),
        "批处理失败：{:?} {:?}",
        harness.status_value(),
        output.2
    );
    let actual: Vec<String> =
        serde_json::from_slice(&std::fs::read(harness.root.join("batch.json")).unwrap()).unwrap();
    assert_eq!(actual, args);
    assert_eq!(
        harness.status_value(),
        json!({"outcome":"exited","exitCode":0})
    );
}

#[tokio::test]
async fn 批处理参数含_cmd_分隔符逗号分号等号时仍逐字相等() {
    let mut harness = Harness::new().await;
    // cmd 把逗号、分号、等号与空白一样当参数分隔符；不加引号时 a=b 会被拆成两个参数。
    let batch = harness.root.join("delimiters.cmd");
    std::fs::write(&batch, concat!(
        "@echo off\r\nsetlocal\r\n",
        "set \"a=x%~1\"\r\nset \"b=x%~2\"\r\nset \"c=x%~3\"\r\nset \"d=x%~4\"\r\n",
        "> \"%CXB_DIAG_FIXTURE_ROOT%\\batch.json\" echo [\"%a:~1%\",\"%b:~1%\",\"%c:~1%\",\"%d:~1%\"]\r\n"
    )).unwrap();
    let args = ["a=b", "c,d", "e;f", "plain"];
    let mut command = harness.diag(5000, &batch);
    command.args(args);
    let mut child = harness.spawn(command);
    let output = capture(&mut child).await;
    assert!(
        output.0.success(),
        "批处理失败：{:?} {:?}",
        harness.status_value(),
        output.2
    );
    let actual: Vec<String> =
        serde_json::from_slice(&std::fs::read(harness.root.join("batch.json")).unwrap()).unwrap();
    assert_eq!(actual, args);
}

#[tokio::test]
async fn 参数错误不写状态且运行失败有中文原因() {
    let mut harness = Harness::new().await;
    for args in [
        vec!["diag-exec"],
        vec!["diag-exec", "--timeout-ms", "100", "--"],
    ] {
        let mut command = harness.command(BROKER);
        command.args(args).arg(harness.program());
        let mut child = harness.spawn(command);
        assert_eq!(capture(&mut child).await.0.code(), Some(2));
        assert!(!harness.status().exists());
    }
    for timeout in [0, 600001] {
        let mut child = harness.spawn(harness.diag(timeout, &harness.program()));
        assert_eq!(capture(&mut child).await.0.code(), Some(2));
        assert!(!harness.status().exists());
    }
    let mut command = harness.command(BROKER);
    command
        .arg("diag-exec")
        .arg("--status-file")
        .arg(harness.status())
        .arg("--")
        .arg(harness.program());
    let mut child = harness.spawn(command);
    assert_eq!(capture(&mut child).await.0.code(), Some(2));
    assert!(!harness.status().exists());
    let mut child = harness.spawn(harness.diag(100, Path::new("relative.exe")));
    assert_eq!(capture(&mut child).await.0.code(), Some(2));
    assert!(!harness.status().exists());
    for path in [
        PathBuf::from("relative.json"),
        harness.root.join("absent/status.json"),
    ] {
        let mut command = harness.command(BROKER);
        command
            .args(["diag-exec", "--timeout-ms", "100", "--status-file"])
            .arg(path)
            .arg("--")
            .arg(harness.program());
        let mut child = harness.spawn(command);
        assert_eq!(capture(&mut child).await.0.code(), Some(2));
        assert!(!harness.status().exists());
    }
    for ch in ['&', '|', '<', '>', '^', '%', '!', '"', '(', ')', '\r', '\n'] {
        let mut command = harness.diag(100, &harness.root.join("missing.bat"));
        command.arg(format!("a{ch}b"));
        let mut child = harness.spawn(command);
        assert_eq!(capture(&mut child).await.0.code(), Some(125));
        assert_eq!(harness.status_value()["outcome"], "failed");
        assert!(harness.status_value()["reason"]
            .as_str()
            .unwrap()
            .contains("元字符"));
    }
    let mut child = harness.spawn(harness.diag(100, &harness.root.join("missing.exe")));
    assert_eq!(capture(&mut child).await.0.code(), Some(125));
    assert!(harness.status_value()["reason"]
        .as_str()
        .unwrap()
        .contains("创建诊断进程失败"));
}

#[tokio::test]
async fn 空字符在操作系统创建前被拒且写_failed() {
    let harness = Harness::new().await;
    // Windows 自身不能传递含 NUL 的 argv，因此从同一执行入口验证该边界。
    for extension in ["exe", "cmd", "bat"] {
        for in_program in [false, true] {
            let program = harness.root.join(format!(
                "bad{}.{extension}",
                if in_program { "\0" } else { "" }
            ));
            let args = cxb_broker::cli::diag::DiagArgs {
                timeout_ms: 100,
                status_file: harness.status(),
                command: vec![
                    program.into_os_string(),
                    if in_program { "有效参数" } else { "a\0b" }.into(),
                ],
            };
            assert_eq!(cxb_broker::cli::diag::run(&args), 125);
            assert_eq!(harness.status_value()["outcome"], "failed");
            assert!(harness.status_value()["reason"]
                .as_str()
                .unwrap()
                .contains("空字符"));
        }
    }
}

async fn tree(timeout: u32) -> (Harness, Child, Vec<usize>) {
    let mut harness = Harness::new().await;
    let mut command = harness.diag(timeout, &harness.program());
    command.env("CXB_DIAG_FIXTURE_MODE", "tree");
    let child = harness.spawn(command);
    ready(&harness.root.join("tree.ready")).await;
    let indexes = harness.descendants();
    assert_eq!(indexes.len(), 2, "实际子孙均须登记");
    assert!(indexes.iter().all(|&i| !harness.processes[i].ended()));
    (harness, child, indexes)
}

#[tokio::test]
async fn 期限到终止整棵树并发布_timeout() {
    let (harness, mut child, indexes) = tree(3000).await;
    assert_eq!(capture(&mut child).await.0.code(), Some(124));
    assert_eq!(harness.status_value(), json!({"outcome":"timeout"}));
    assert!(
        indexes.iter().all(|&i| harness.processes[i].ended()),
        "发布状态前整棵树必须退出"
    );
}

#[tokio::test]
async fn 父程序先退出仍终止占输出的后代() {
    let (harness, mut child, indexes) = tree(10000).await;
    std::fs::write(harness.root.join("parent.release"), "允许父程序退出").unwrap();
    assert_eq!(capture(&mut child).await.0.code(), Some(0));
    assert_eq!(
        harness.status_value(),
        json!({"outcome":"exited","exitCode":0})
    );
    assert!(
        indexes.iter().all(|&i| harness.processes[i].ended()),
        "后代不得因父程序先退而泄漏"
    );
}

#[tokio::test]
async fn 外部结束包装器后_job_关闭即回收子孙() {
    let (harness, mut child, indexes) = tree(10000).await;
    child.start_kill().expect("模拟外部结束包装器");
    let _ = capture(&mut child).await;
    for index in indexes {
        assert_eq!(
            unsafe { WaitForSingleObject(harness.processes[index].handle.as_raw_handle(), 2000) },
            WAIT_OBJECT_0,
            "包装器被结束后独立 Job 必须回收子孙"
        );
    }
    assert!(!harness.status().exists(), "外部强杀不能伪造诊断结果");
}
