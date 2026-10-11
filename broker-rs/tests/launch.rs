//! M8 进程级验收（S3-计划 M8、详细设计第 5 节与 0.1 节、第 9 节第 4、5、7 条、S1-评审第 24 节）：
//! 用真实的 `cxb-broker` 进程与 bun 替身 app-server（`tests/fixtures/launch-app-server.ts`）。
//!
//! 资源与时序约定（M8 活性审查与讨论后定）：
//! - **管道客户端用 tokio 的命名管道**（重叠 I/O，读写可并发）。同步打开的句柄上读写会被串行化
//!   （微软 CreateFileW 文档：未带 FILE_FLAG_OVERLAPPED 时 "I/O operations are serialized"），
//!   一个线程阻塞读会挡住另一个线程的写。每个请求从发出起有一个总期限；超时就丢掉这条连接。
//! - **进程按句柄登记**（[`Registry`]）：一出现就登记，结束与等待都经同一个句柄，不按旧进程号重新打开。
//!   清理先取走登记表、释放锁，再在一个共享的总期限内结束与等待；taskkill 也限时。
//! - **每个用例一个看守线程**（[`Watchdog`]）：用例总期限到了就结束所有已登记的进程，让卡在管道、
//!   进程等待上的调用返回，测试随之失败并照常清理；超时后才登记的进程立即结束。
//!   局限：磁盘 I/O 卡死、测试线程自身死锁这类结束进程解不开的阻塞，看守救不了。
//! - 时序交错不用固定延迟：替身在关键处停住，等测试创建闸门文件才继续（`FIXTURE_GATE`）。
//! - 本文件的用例互相串行（[`serial`]）：「额外的可继承句柄不泄漏」一例要求同一时刻没有别的用例在
//!   创建子进程，按进程数断言的用例也不该交错。

#![cfg(windows)]

use std::ffi::OsString;
use std::io::Read;
use std::os::windows::io::AsRawHandle;
use std::os::windows::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient};
use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, SetHandleInformation, ERROR_INVALID_PARAMETER, HANDLE_FLAG_INHERIT,
    WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::System::Threading::{
    GetExitCodeProcess, OpenProcess, TerminateProcess, WaitForSingleObject, CREATE_SUSPENDED,
    PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE, PROCESS_TERMINATE,
};

const BROKER: &str = env!("CARGO_BIN_EXE_cxb-broker");
const CREATE_NO_WINDOW: u32 = 0x0800_0000;
/// 单个用例的总期限：正常用例几秒内完成，期限只防卡死。
const CASE_LIMIT: Duration = Duration::from_secs(90);
/// 单个请求（写出到读到对应回包）的总期限。
const REQUEST_LIMIT: Duration = Duration::from_secs(15);
/// 清理（结束全部登记进程并等它们退出）的共享总期限。
const CLEANUP_LIMIT: Duration = Duration::from_secs(10);
static SERIAL: Mutex<()> = Mutex::new(());
static NEXT: AtomicU64 = AtomicU64::new(0);

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn unique() -> String {
    format!(
        "{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// 每个用例自带的单线程运行时（启用 I/O 与计时器）。
fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

/// 用句柄跟踪的进程：等待、读退出码、结束都经同一个句柄，不受进程号复用影响。
struct Proc {
    handle: isize,
    pid: u32,
}

impl Proc {
    /// 进程已不存在时返回 None；其他打开失败（权限、资源）直接判失败，不当作「已退出」。
    fn open(pid: u32) -> Option<Proc> {
        // 安全依据：只申请等待、查询与结束权限；返回的句柄归本对象，Drop 时关闭一次。
        let handle = unsafe {
            OpenProcess(
                PROCESS_SYNCHRONIZE | PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_TERMINATE,
                0,
                pid,
            )
        };
        if handle.is_null() {
            let error = unsafe { GetLastError() };
            assert_eq!(
                error, ERROR_INVALID_PARAMETER,
                "打开进程 {pid} 失败，系统错误码 {error}"
            );
            return None;
        }
        Some(Proc {
            handle: handle as isize,
            pid,
        })
    }

    fn raw(&self) -> *mut std::ffi::c_void {
        self.handle as *mut std::ffi::c_void
    }

    /// 期限内退出返回退出码，超时返回 None；等待本身失败直接判失败。
    fn wait(&self, limit: Duration) -> Option<u32> {
        // 安全依据：句柄在本对象存活期间有效。
        unsafe {
            match WaitForSingleObject(self.raw(), limit.as_millis() as u32) {
                WAIT_OBJECT_0 => {
                    let mut code = 0;
                    assert_ne!(
                        GetExitCodeProcess(self.raw(), &mut code),
                        0,
                        "读取退出码失败"
                    );
                    Some(code)
                }
                WAIT_TIMEOUT => None,
                other => panic!(
                    "等待进程 {} 失败：{other}，系统错误码 {}",
                    self.pid,
                    GetLastError()
                ),
            }
        }
    }

    fn alive(&self) -> bool {
        self.wait(Duration::ZERO).is_none()
    }

    fn terminate(&self) {
        // 安全依据：句柄带 PROCESS_TERMINATE 权限；进程已退出时调用无副作用（返回拒绝访问）。
        unsafe {
            TerminateProcess(self.raw(), 1);
        }
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        // 安全依据：句柄由 OpenProcess 返回且只关闭一次。
        unsafe {
            CloseHandle(self.raw());
        }
    }
}

/// 限时运行 taskkill：期限内以 0 退出返回 true；超时就结束 taskkill 本身并返回 false。
fn taskkill(pid: u32, tree: bool, limit: Duration) -> bool {
    let deadline = Instant::now() + limit;
    let mut command = Command::new("taskkill.exe");
    command.arg("/F");
    if tree {
        command.arg("/T");
    }
    let Ok(mut child) = command
        .args(["/PID", &pid.to_string()])
        .creation_flags(CREATE_NO_WINDOW)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    else {
        return false;
    };
    // 安全依据：直接使用 Child 持有的进程句柄，直到收取退出状态前都有效。
    let handle = child.as_raw_handle() as _;
    let remaining = deadline.saturating_duration_since(Instant::now());
    // 在原总期限内预留最多 1 秒用于终止后的等待，不能超时后另外续期。
    let normal_wait = remaining.saturating_sub(Duration::from_secs(1));
    let exited =
        unsafe { WaitForSingleObject(handle, normal_wait.as_millis() as u32) } == WAIT_OBJECT_0;
    if !exited {
        unsafe { TerminateProcess(handle, 1) };
        let remaining = deadline.saturating_duration_since(Instant::now());
        if unsafe { WaitForSingleObject(handle, remaining.as_millis() as u32) } != WAIT_OBJECT_0 {
            return false;
        }
    }
    // 两条路径都必须先由句柄确认退出，才能进入没有期限的 Child::wait。
    let status = child.wait();
    exited && status.map(|s| s.success()).unwrap_or(false)
}

/// 只把底层 read 返回 0 当作 EOF；超限或读取失败经通道交回调用线程判失败。
/// stdout 的首行一旦完整就立即送出，后续仍继续读，不能用读取预算伪装 EOF。
fn read_launch_output(
    mut reader: impl Read,
    mut first_line: Option<mpsc::Sender<Result<String, String>>>,
) -> Result<Vec<u8>, String> {
    let result = (|| {
        let mut output = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            let read = reader
                .read(&mut buffer)
                .map_err(|e| format!("读取输出失败：{e}"))?;
            if read == 0 {
                if let Some(tx) = first_line.take() {
                    let line = String::from_utf8(output.clone())
                        .map_err(|e| format!("结果行不是 UTF-8：{e}"));
                    let _ = tx.send(line);
                }
                return Ok(output);
            }
            output.extend_from_slice(&buffer[..read]);
            if first_line.is_some() {
                if let Some(end) = output.iter().position(|b| *b == b'\n') {
                    let line = String::from_utf8(output[..=end].to_vec())
                        .map_err(|e| format!("结果行不是 UTF-8：{e}"));
                    let _ = first_line.take().unwrap().send(line);
                }
            }
            if output.len() > 64 * 1024 {
                return Err("输出超过 64 KiB 上限，尚未确认 EOF".to_owned());
            }
        }
    })();
    if let (Some(tx), Err(error)) = (first_line, &result) {
        let _ = tx.send(Err(error.clone()));
    }
    result
}

struct Tracked {
    process: Arc<Proc>,
    /// 包装进程要连同子树一起结束（taskkill /T 按进程号走树；持有句柄期间进程号不会被复用）。
    tree: bool,
}

/// 用例里出现过的进程登记表：看守与 [`Env`] 共享，各自都能触发同一个幂等的清理。
#[derive(Default)]
struct Registry {
    tracked: Mutex<Vec<Tracked>>,
    cancelled: AtomicBool,
    diagnostics: Mutex<Vec<String>>,
}

impl Registry {
    fn adopt(&self, process: Proc, tree: bool) -> Arc<Proc> {
        let process = Arc::new(process);
        self.tracked.lock().unwrap().push(Tracked {
            process: process.clone(),
            tree,
        });
        // 看守已触发后才登记的进程（工作线程刚创建出来的）立即结束，不留到清理扫描之后。
        if self.cancelled.load(Ordering::SeqCst) {
            let problems = Self::reap(&[(process.clone(), tree)], Instant::now() + CLEANUP_LIMIT);
            self.diagnostics.lock().unwrap().extend(problems);
        }
        process
    }

    /// 与看守的取消置位互斥：取消后不能再恢复包装进程，让它生成未登记的子孙。
    fn resume_wrapper(&self, process: &Proc) {
        let _guard = self.tracked.lock().unwrap();
        if self.cancelled.load(Ordering::SeqCst) {
            process.terminate();
        } else {
            cxb_broker::job::resume_suspended(process.pid).expect("恢复包装进程主线程");
        }
    }

    /// 结束全部仍在运行的登记进程，并在共享总期限内等它们退出；返回没能结束的说明。
    fn kill_all(&self, limit: Duration) -> Vec<String> {
        let snapshot: Vec<(Arc<Proc>, bool)> = self
            .tracked
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|t| (t.process.clone(), t.tree))
            .collect();
        Self::reap(&snapshot, Instant::now() + limit)
    }

    /// 批量清理与迟到登记共用有界回收，保留包装进程的子树语义。
    fn reap(snapshot: &[(Arc<Proc>, bool)], deadline: Instant) -> Vec<String> {
        let mut problems = Vec::new();
        for (process, tree) in snapshot {
            if process.alive() {
                if *tree {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if !taskkill(process.pid, true, remaining.min(Duration::from_secs(5))) {
                        process.terminate();
                        problems.push(format!("进程 {} 的子树未能确认回收", process.pid));
                    }
                } else {
                    process.terminate();
                }
            }
        }
        for (process, _) in snapshot {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if process.wait(remaining).is_none() {
                problems.push(format!("进程 {} 在清理期限内未结束", process.pid));
            }
        }
        problems
    }
}

/// 用例看守：独立的 std 线程，到期就把登记表标为已取消并结束全部登记进程。
struct Watchdog {
    disarm: Option<mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Watchdog {
    fn arm(registry: Arc<Registry>, limit: Duration) -> Watchdog {
        let (disarm, armed) = mpsc::channel::<()>();
        let thread = std::thread::spawn(move || {
            if let Err(mpsc::RecvTimeoutError::Timeout) = armed.recv_timeout(limit) {
                {
                    let _guard = registry.tracked.lock().unwrap();
                    registry.cancelled.store(true, Ordering::SeqCst);
                }
                let problems = registry.kill_all(CLEANUP_LIMIT);
                registry.diagnostics.lock().unwrap().push(format!(
                    "用例超过 {limit:?}：看守已执行登记进程回收；清理问题：{problems:?}"
                ));
            }
        });
        Watchdog {
            disarm: Some(disarm),
            thread: Some(thread),
        }
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        // 发送或断开都让看守线程立即返回；它若已在清理，等它做完（清理本身有总期限）。
        drop(self.disarm.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// 一次用例的隔离环境：临时 CXB_HOME、CODEX_HOME、工作区、PATH 前缀里的 codex.cmd、替身记录与闸门文件。
struct Env {
    root: PathBuf,
    bin: PathBuf,
    cxb_home: PathBuf,
    codex_home: PathBuf,
    log: PathBuf,
    gate: PathBuf,
    registry: Arc<Registry>,
    watchdog: Option<Watchdog>,
}

impl Env {
    fn new() -> Env {
        let root = std::env::temp_dir().join(format!("cxb-m8-{}", unique()));
        let bin = root.join("bin");
        let cxb_home = root.join("cxb-home");
        let codex_home = root.join("codex-home");
        for dir in [&bin, &cxb_home, &codex_home] {
            std::fs::create_dir_all(dir).unwrap();
        }
        let fixture =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/launch-app-server.ts");
        // 私有档按 `cmd.exe /c codex app-server` 启动、第 2 档按 `codex app-server proxy` 启动；
        // PATH 前缀里的 codex.cmd 都转给 bun 替身。
        std::fs::write(
            bin.join("codex.cmd"),
            format!("@bun \"{}\" %*\r\n", fixture.display()),
        )
        .unwrap();
        let registry = Arc::new(Registry::default());
        Env {
            log: root.join("pids.log"),
            gate: root.join("gate"),
            watchdog: Some(Watchdog::arm(registry.clone(), CASE_LIMIT)),
            registry,
            root,
            bin,
            cxb_home,
            codex_home,
        }
    }

    fn workspace(&self, name: &str) -> PathBuf {
        let dir = self.root.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn endpoint(&self, tag: &str) -> String {
        format!(r"pipe:\\.\pipe\cxb-m8-{tag}-{}", unique())
    }

    /// 打开闸门：替身在 FIXTURE_GATE 处停住的地方继续。
    fn release(&self) {
        std::fs::write(&self.gate, "").unwrap();
    }

    /// 以 `sub`（serve 或 launch）启动 broker 的命令，环境与黑盒测试工具层一致：默认私有档。
    fn broker(&self, sub: &str, endpoint: &str, cwd: &Path) -> Command {
        let mut command = Command::new(BROKER);
        command
            .args([
                sub,
                "--endpoint",
                endpoint,
                "--idle-timeout",
                "60000",
                "--cwd",
            ])
            .arg(cwd)
            .current_dir(cwd)
            .creation_flags(CREATE_NO_WINDOW)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        self.apply_env(&mut command);
        command
    }

    fn apply_env(&self, command: &mut Command) {
        let mut path = OsString::from(self.bin.as_os_str());
        path.push(";");
        path.push(std::env::var_os("PATH").unwrap_or_default());
        for name in [
            "CXB_SERVER",
            "CXB_SERVER_SOCKET",
            "CXB_SERVER_SOCKET",
            "FIXTURE_INIT_ERROR",
            "FIXTURE_HOLD_EXIT",
        ] {
            command.env_remove(name);
        }
        command
            .env("PATH", path)
            .env("CXB_HOME", &self.cxb_home)
            .env("CODEX_HOME", &self.codex_home)
            .env("CXB_SERVER", "private")
            .env("CXB_PEER", "off")
            .env("FIXTURE_PID_LOG", &self.log)
            .env("FIXTURE_GATE", &self.gate);
    }

    /// 按进程号登记；进程已不存在时返回 None。
    fn adopt(&self, pid: u32) -> Option<Arc<Proc>> {
        Some(self.registry.adopt(Proc::open(pid)?, false))
    }

    /// 登记一个直接子进程：Child 尚未被等待，进程号不会被复用，打开的一定是它。
    fn adopt_child(&self, child: &Child, tree: bool) -> Arc<Proc> {
        let process = Proc::open(child.id()).expect("刚创建的子进程应能打开");
        self.registry.adopt(process, tree)
    }

    /// 创建并立即登记（批量创建时每个都先登记再创建下一个）。
    fn spawn(&self, command: &mut Command) -> (Child, Arc<Proc>) {
        let child = command.spawn().unwrap();
        let process = self.adopt_child(&child, false);
        (child, process)
    }

    /// 运行 launch：创建后立即登记 launch 本身；读到 stdout 那一行立刻登记 serve；
    /// 再等 stdout、stderr 都 EOF、launch 以 0 退出。`setup` 在创建之后运行（例如加入 Job 再恢复）。
    /// 读输出的线程在 serve 不再持有管道（或被清理结束）时退出；超时由调用方断言失败、清理兜底。
    fn run_launch(
        &self,
        mut command: Command,
        setup: impl FnOnce(&Child),
    ) -> (Value, Option<Arc<Proc>>) {
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        let (mut child, launcher) = self.spawn(&mut command);
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        setup(&child);
        let (line_tx, line_rx) = mpsc::channel();
        let (eof_tx, eof_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = eof_tx.send(read_launch_output(stdout, Some(line_tx)));
        });
        let (err_tx, err_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = err_tx.send(read_launch_output(stderr, None));
        });
        let line = line_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("launch 10 秒内没有写出结果行")
            .unwrap_or_else(|e| panic!("launch 的 stdout 结果行读取失败：{e}"));
        let value: Value = serde_json::from_str(line.trim_end())
            .unwrap_or_else(|e| panic!("launch 的 stdout 不是一行 JSON：{line:?}（{e}）"));
        let serve = self.adopt(value["pid"].as_u64().unwrap() as u32);
        assert!(line.ends_with('\n'), "结果行应以换行结尾：{line:?}");
        assert_eq!(
            value.as_object().unwrap().len(),
            2,
            "只有 pid 与 breakaway：{value}"
        );
        assert!(value["breakaway"].is_boolean());
        let output = eof_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("launch 的 stdout 10 秒内没有关闭：serve 持有了它")
            .unwrap_or_else(|e| panic!("launch 的 stdout 未能确认 EOF：{e}"));
        let rest = &output[line.len()..];
        assert!(
            rest.is_empty(),
            "stdout 应恰好一行，多出：{:?}",
            String::from_utf8_lossy(rest)
        );
        let stderr_text = err_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("launch 的 stderr 10 秒内没有关闭：serve 持有了它")
            .unwrap_or_else(|e| panic!("launch 的 stderr 未能确认 EOF：{e}"));
        let stderr_text = String::from_utf8(stderr_text).expect("launch 的 stderr 不是 UTF-8");
        assert_eq!(
            launcher.wait(Duration::from_secs(10)),
            Some(0),
            "launch 应以 0 退出，stderr：{stderr_text}"
        );
        let _ = child.wait();
        (value, serve)
    }

    fn launch(
        &self,
        endpoint: &str,
        cwd: &Path,
        extra: &[(&str, &str)],
    ) -> (Value, Option<Arc<Proc>>) {
        let mut command = self.broker("launch", endpoint, cwd);
        for (k, v) in extra {
            command.env(k, v);
        }
        self.run_launch(command, |_| {})
    }

    /// broker.json 的 payload（按端点在 workspaces 下查找），等到写出为止。
    fn broker_info(&self, endpoint: &str) -> Value {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(entries) = std::fs::read_dir(self.cxb_home.join("workspaces")) {
                for entry in entries.flatten() {
                    let Ok(text) = std::fs::read_to_string(entry.path().join("broker.json")) else {
                        continue;
                    };
                    let Ok(value) = serde_json::from_str::<Value>(&text) else {
                        continue;
                    };
                    if value["payload"]["endpoint"] == endpoint {
                        return value["payload"].clone();
                    }
                }
            }
            assert!(
                Instant::now() < deadline,
                "10 秒内没有 {endpoint} 的 broker.json"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// 替身的记录：只解析以换行结尾的完整行（替身可能正在追加末行）。
    fn fixture_events(&self) -> Vec<Value> {
        let text = std::fs::read_to_string(&self.log).unwrap_or_default();
        let complete = match text.rfind('\n') {
            Some(end) => &text[..end],
            None => "",
        };
        complete
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn wait_fixture(&self, what: &str, pred: impl Fn(&[Value]) -> bool) -> Vec<Value> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let events = self.fixture_events();
            if pred(&events) {
                return events;
            }
            assert!(
                Instant::now() < deadline,
                "10 秒内替身没有{what}：{events:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        // 先解除看守（它若已在清理就等它做完），再走同一个幂等清理。
        drop(self.watchdog.take());
        let mut problems = self.registry.kill_all(CLEANUP_LIMIT);
        if self.registry.cancelled.load(Ordering::SeqCst) {
            let diagnostics = self.registry.diagnostics.lock().unwrap();
            problems.push(format!("看守已触发，用例失败：{diagnostics:?}"));
        }
        // 只删除本用例在系统临时目录下建的目录；Job 结束子进程后句柄才释放，所以有限次重试。
        // 单次 remove_dir_all 本身没有硬期限（磁盘卡死时救不了）。
        assert!(self.root.starts_with(std::env::temp_dir()));
        let mut removed = false;
        for _ in 0..50 {
            if std::fs::remove_dir_all(&self.root).is_ok() || !self.root.exists() {
                removed = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        if !removed {
            problems.push(format!("临时目录 {} 未能删除", self.root.display()));
        }
        if !problems.is_empty() {
            if std::thread::panicking() {
                eprintln!("清理未完成：{problems:?}");
            } else {
                panic!("清理未完成：{problems:?}");
            }
        }
    }
}

fn pipe_name(endpoint: &str) -> &str {
    endpoint.strip_prefix("pipe:").unwrap()
}

/// 管道客户端：tokio 命名管道（重叠 I/O），一个对象持有整条连接，不另起读线程；
/// 丢弃即关闭连接。一次只等一个回包，读到的通知与服务端请求直接跳过。
struct Client {
    pipe: BufReader<NamedPipeClient>,
    next_id: u64,
}

impl Client {
    /// 在期限内反复尝试打开（管道还不存在或实例忙时重试）。
    async fn open(endpoint: &str, limit: Duration) -> Client {
        let name = pipe_name(endpoint).to_owned();
        let opening = async {
            loop {
                match ClientOptions::new().open(&name) {
                    Ok(pipe) => return pipe,
                    // 2：管道不存在；231：所有实例都忙。
                    Err(error) if matches!(error.raw_os_error(), Some(2 | 231)) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => panic!("打不开 {name}：{error}"),
                }
            }
        };
        let pipe = tokio::time::timeout(limit, opening)
            .await
            .unwrap_or_else(|_| panic!("{limit:?} 内打不开 {endpoint}"));
        Client {
            pipe: BufReader::new(pipe),
            next_id: 1,
        }
    }

    async fn send(&mut self, message: &Value) {
        let mut line = message.to_string();
        line.push('\n');
        self.pipe.write_all(line.as_bytes()).await.unwrap();
    }

    /// 发请求并等对应回包；从写出开始算总期限，期间收到的无关消息不重新计时。
    async fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let exchange = async {
            self.send(&json!({"id": id, "method": method, "params": params}))
                .await;
            loop {
                let mut line = String::new();
                let read = self.pipe.read_line(&mut line).await.unwrap();
                assert_ne!(read, 0, "等 {method} 回包时连接被关闭");
                let message: Value = serde_json::from_str(&line).unwrap();
                if message["id"] == id && message.get("method").is_none() {
                    return message;
                }
            }
        };
        tokio::time::timeout(REQUEST_LIMIT, exchange)
            .await
            .unwrap_or_else(|_| panic!("{method} {REQUEST_LIMIT:?} 内没有回包"))
    }

    /// 期限内被服务端断开（读到 EOF 或断开错误）返回 true；收到消息或超时返回 false。
    async fn closed_within(&mut self, limit: Duration) -> bool {
        let mut line = String::new();
        match tokio::time::timeout(limit, self.pipe.read_line(&mut line)).await {
            Ok(Ok(0)) | Ok(Err(_)) => true,
            Ok(Ok(_)) | Err(_) => false,
        }
    }

    /// 打开并完成握手（声明 cxb）：一个总期限覆盖重试打开与握手。
    async fn connect(endpoint: &str, limit: Duration) -> Client {
        let connecting = async {
            let mut client = Client::open(endpoint, limit).await;
            let reply = client
                .request(
                    "initialize",
                    json!({"clientInfo": {"name": "m8", "title": null, "version": "0.0.1"},
                           "capabilities": {"experimentalApi": false, "cxb": {"protocolVersion": 1}}}),
                )
                .await;
            assert!(reply.get("result").is_some(), "握手失败：{reply}");
            client.send(&json!({"method": "initialized"})).await;
            client
        };
        tokio::time::timeout(limit, connecting)
            .await
            .unwrap_or_else(|_| panic!("{limit:?} 内没能连上 {endpoint} 并握手"))
    }

    async fn health(&mut self) -> Value {
        self.request("broker/health", json!({})).await["result"].clone()
    }
}

/// 让 broker 关闭并等它以 0 退出。
async fn shutdown(endpoint: &str, process: &Proc) {
    let mut client = Client::connect(endpoint, Duration::from_secs(10)).await;
    let reply = client.request("broker/shutdown", json!({})).await;
    assert!(reply.get("error").is_none(), "shutdown 被拒：{reply}");
    drop(client);
    assert_eq!(
        process.wait(Duration::from_secs(20)),
        Some(0),
        "broker 应以 0 退出"
    );
}

fn survives_formula(evidence: &Value) -> bool {
    evidence["inJob"] == false || evidence["killOnClose"] == false
}

fn pid_of(value: &Value) -> u32 {
    value["pid"].as_u64().unwrap() as u32
}

#[test]
fn mi_命名管道六兆输出摘要无正文且批量超过256线程不占配额() {
    let _serial = serial();
    let env = Env::new();
    let cwd = env.workspace("模型摘要");
    let endpoint = env.endpoint("model-summary");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let turns: Vec<_> = (0..6).map(|i| json!({
        "turnId":format!("轮-{i}"),"ready":"ready","status":"completed",
        "output":"x".repeat(1024*1024),"outputSource":"finalAnswer","error":null,"completedAt":now,
    })).collect();
    let record: cxb_broker::runs::RunRecord = serde_json::from_value(json!({
        "runId":"run-big","threadId":"大输出线程","shortId":"12345678","joined":false,
        "state":"ended","status":"completed","startedAt":now,"endedAt":now,
        "meta":{"source":"run","model":"astra","effort":"high","goal":{"text":"不应出现在摘要"}},
        "reviewThreadId":null,"reviewShortId":null,"input":"不应出现在摘要的输入",
        "continuedFrom":null,"interruptReason":null,"stopOutcome":null,"stopCheck":null,
        "progress":{"done":[],"doneTotal":0,"unconfirmed":[],"observedFiles":[]},
        "turns":turns,"turnsTotal":6,"currentTurnId":"轮-5",
        "startConfirmation":{"confirmed":true,"owned":true,"threadId":"大输出线程","turnId":"轮-0"},
    }))
    .unwrap();
    let runs = cxb_broker::store::workspace_dir(&env.cxb_home, &cwd).join("runs");
    std::fs::create_dir_all(&runs).unwrap();
    std::fs::write(
        runs.join("run-big.json"),
        serde_json::to_vec(&json!({"epoch":0,"revision":1,"payload":record})).unwrap(),
    )
    .unwrap();
    let (_, broker) = env.launch(&endpoint, &cwd, &[]);
    let broker = broker.expect("broker 应存活");
    block_on(async {
        let mut client = Client::connect(&endpoint, REQUEST_LIMIT).await;
        let summary = client
            .request(
                "broker/runs/modelSummaries",
                json!({"threadIds":["大输出线程"]}),
            )
            .await;
        assert!(summary.to_string().len() < 2048);
        let run = &summary["result"]["runs"][0];
        assert_eq!(run["runId"], "run-big");
        assert_eq!(run["currentTurnId"], "轮-5");
        assert_eq!(run["lastTurn"], json!({"turnId":"轮-5","reroute":null}));
        assert_eq!(
            run["meta"],
            json!({"source":"run","model":"astra","effort":"high"})
        );
        for field in ["input", "turns", "progress", "output"] {
            assert!(run.get(field).is_none());
        }
        // 旧客户端即使仍携带已删除的参数，也不能再收到重复正文。
        let snapshot = client
            .request(
                "broker/threads/watch",
                json!({"threadId":"大输出线程","includeRuns":true}),
            )
            .await;
        assert!(snapshot["result"].get("runs").is_none());
        let bytes = snapshot.to_string().len();
        assert!(
            (6 * 1024 * 1024..10 * 1024 * 1024).contains(&bytes),
            "watch 不得重复发送正文：{bytes}"
        );
        let threads: Vec<_> = (0..300).map(|i| format!("线程-{i}")).collect();
        for thread in &threads {
            let started = client
                .request("turn/start", json!({"threadId":thread,"input":[]}))
                .await;
            assert!(started.get("error").is_none(), "{started}");
        }
        let summary = client
            .request("broker/runs/modelSummaries", json!({"threadIds":threads}))
            .await;
        assert_eq!(summary["result"]["runs"].as_array().unwrap().len(), 300);
        // 大输出线程已占一个 watch 槽；摘要不能消费余下 255 个。
        for thread in threads.iter().take(255) {
            let watched = client
                .request("broker/threads/watch", json!({"threadId":thread}))
                .await;
            assert!(watched.get("error").is_none(), "{watched}");
        }
        let summary = client
            .request("broker/runs/modelSummaries", json!({"threadIds":threads}))
            .await;
        assert_eq!(summary["result"]["runs"].as_array().unwrap().len(), 300);
    });
    block_on(shutdown(&endpoint, &broker));
}

#[test]
fn launch_立即退出_输出与额外句柄都不泄漏给_serve_且依据一致() {
    let _guard = serial();
    let env = Env::new();
    let workspace = env.workspace("工作 区");
    // 真实的 Rust/clap 解析 launch 拼出的命令行：管道名带空格、中文和引号；cwd 带空格、中文，
    // 且以反斜杠结尾（std::path::absolute 保留结尾分隔符），验证「结尾反斜杠加倍」。
    // 空参数、引号前的反斜杠在 launch 实际传出的参数里不会出现，只在 launch.rs 单测里经系统解析校验。
    let endpoint = format!(r#"pipe:\\.\pipe\cxb m8 测试 "q" {}"#, unique());
    let mut cwd_arg = workspace.clone().into_os_string();
    cwd_arg.push(r"\");
    let cwd_arg = PathBuf::from(cwd_arg);

    // 一个显式可继承的额外句柄：launch 会继承它，serve 不应该。
    let (mut reader, writer) = std::io::pipe().unwrap();
    // 安全依据：句柄由 writer 持有且有效；只改继承标志。
    assert_ne!(
        unsafe {
            SetHandleInformation(
                writer.as_raw_handle() as _,
                HANDLE_FLAG_INHERIT,
                HANDLE_FLAG_INHERIT,
            )
        },
        0
    );
    let command = env.broker("launch", &endpoint, &cwd_arg);
    let (launched, serve) = env.run_launch(command, |_| {});
    drop(writer);
    let serve = serve.expect("launch 退出后 serve 应仍在运行");
    // 读线程在所有写端关闭（或清理结束 serve）时返回。
    let (eof_tx, eof_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut byte = [0u8; 1];
        let _ = eof_tx.send(reader.read(&mut byte).map_err(|e| e.kind()));
    });
    let eof = eof_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("额外句柄的读端 5 秒内没有读到 EOF：serve 持有了 launch 继承来的句柄");
    assert!(
        matches!(eof, Ok(0) | Err(std::io::ErrorKind::BrokenPipe)),
        "应读到 EOF：{eof:?}"
    );
    assert!(serve.alive());

    block_on(async {
        let mut client = Client::connect(&endpoint, Duration::from_secs(15)).await;
        let health = client.health().await;
        let evidence = &health["parentJob"];
        eprintln!("本环境的存活依据：{evidence}");
        assert_eq!(evidence["breakaway"], launched["breakaway"]);
        assert_eq!(evidence["queryScope"], "immediateJob");
        assert!(evidence["breakawayAllowed"].is_boolean());
        assert_eq!(health["survivesParentJob"], survives_formula(evidence));
        let info = env.broker_info(&endpoint);
        assert_eq!(info["pid"], serve.pid);
        assert_eq!(&info["parentJob"], evidence);
        assert_eq!(info["survivesParentJob"], health["survivesParentJob"]);
        let events = env.wait_fixture("启动", |e| !e.is_empty());
        let cwd = events[0]["cwd"].as_str().unwrap().trim_end_matches('\\');
        assert_eq!(
            cwd,
            workspace.to_str().unwrap(),
            "app-server 的工作目录应原样传到"
        );
        drop(client);
        shutdown(&endpoint, &serve).await;
    });
}

#[test]
fn 进程树被杀时直接_serve_同死而经_launch_的存活() {
    let _guard = serial();
    let env = Env::new();
    let (launched_ws, direct_ws) = (env.workspace("launched"), env.workspace("direct"));
    let (launched_ep, direct_ep) = (env.endpoint("tree-launched"), env.endpoint("tree-direct"));
    let out = env.root.join("launch.out");
    // 包装进程在 launch 退出后继续存活（ping），否则 taskkill /T 没有可杀的树，用例会空过。
    let script = env.root.join("wrapper.cmd");
    std::fs::write(
        &script,
        format!(
            "@echo off\r\n\"{exe}\" launch --endpoint \"{launched_ep}\" --cwd \"{lws}\" --idle-timeout 60000 > \"{out}\"\r\n\
             start \"\" /b \"{exe}\" serve --endpoint \"{direct_ep}\" --cwd \"{dws}\" --idle-timeout 60000\r\n\
             ping -n 120 127.0.0.1 > nul\r\n",
            exe = BROKER,
            lws = launched_ws.display(),
            dws = direct_ws.display(),
            out = out.display(),
        ),
    )
    .unwrap();
    let mut wrapper = Command::new("cmd.exe");
    wrapper
        .args(["/d", "/c"])
        .arg(&script)
        .current_dir(&env.root)
        .creation_flags(CREATE_NO_WINDOW | CREATE_SUSPENDED)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    env.apply_env(&mut wrapper);
    let mut wrapper: Child = wrapper.spawn().unwrap();
    let wrapper_proc = env.adopt_child(&wrapper, true);
    // 挂起创建后先登记；看守已取消时直接结束，不能恢复后才补做树清理。
    env.registry.resume_wrapper(&wrapper_proc);

    // 就绪屏障：launch 已退出并写出那一行，两个 broker 都能握手，包装进程仍在。
    let launched = {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(text) = std::fs::read_to_string(&out) {
                if let Ok(value) = serde_json::from_str::<Value>(text.trim()) {
                    break env
                        .adopt(pid_of(&value))
                        .expect("经 launch 的 serve 应在运行");
                }
            }
            assert!(Instant::now() < deadline, "launch 10 秒内没有写出结果");
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    block_on(async {
        drop(Client::connect(&launched_ep, Duration::from_secs(15)).await);
        drop(Client::connect(&direct_ep, Duration::from_secs(15)).await);
    });
    let direct = env
        .adopt(pid_of(&env.broker_info(&direct_ep)))
        .expect("直接 serve 应在运行");
    assert!(wrapper_proc.alive(), "包装进程应仍在运行");

    assert!(
        taskkill(wrapper_proc.pid, true, Duration::from_secs(10)),
        "taskkill /T 包装进程失败"
    );
    assert!(
        wrapper_proc.wait(Duration::from_secs(5)).is_some(),
        "包装进程应已结束"
    );
    wrapper.wait().unwrap();
    assert!(
        direct.wait(Duration::from_secs(5)).is_some(),
        "直接 serve 在包装进程的树里，应一起被杀"
    );
    assert!(
        launched.wait(Duration::from_secs(2)).is_none(),
        "经 launch 的 serve 不在树里，应存活"
    );
    block_on(async {
        let mut client = Client::connect(&launched_ep, Duration::from_secs(5)).await;
        assert_eq!(client.health().await["claims"], 0);
        drop(client);
        shutdown(&launched_ep, &launched).await;
    });
}

#[test]
fn 并发十个_serve_只活一个其余以_0_退出() {
    let _guard = serial();
    let env = Env::new();
    let workspace = env.workspace("ws");
    let endpoint = env.endpoint("race-serve");
    // 每创建一个就先登记，再创建下一个。
    let (mut children, processes): (Vec<Child>, Vec<Arc<Proc>>) = (0..10)
        .map(|_| env.spawn(&mut env.broker("serve", &endpoint, &workspace)))
        .unzip();
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut exited = Vec::new();
    while exited.len() < 9 {
        for (index, process) in processes.iter().enumerate() {
            if !exited.iter().any(|(i, _)| *i == index) {
                if let Some(code) = process.wait(Duration::ZERO) {
                    exited.push((index, code));
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "15 秒内只有 {} 个退出：{exited:?}",
            exited.len()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        exited.iter().all(|(_, code)| *code == 0),
        "落败者都应以 0 退出：{exited:?}"
    );
    let alive: Vec<usize> = (0..10).filter(|i| processes[*i].alive()).collect();
    assert_eq!(alive.len(), 1, "应恰好一个存活");
    let winner = processes[alive[0]].clone();
    assert_eq!(env.broker_info(&endpoint)["pid"], winner.pid);
    block_on(shutdown(&endpoint, &winner));
    for child in &mut children {
        child.wait().unwrap();
    }
}

#[test]
fn 并发十个_launch_只活一个_杀掉赢家整串进程消失且能立即重新抢到() {
    let _guard = serial();
    let env = Arc::new(Env::new());
    let workspace = env.workspace("ws");
    let endpoint = env.endpoint("race-launch");
    let (done_tx, done_rx) = mpsc::channel();
    for _ in 0..10 {
        let (env, endpoint, workspace, done_tx) = (
            env.clone(),
            endpoint.clone(),
            workspace.clone(),
            done_tx.clone(),
        );
        std::thread::spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let (value, serve) = env.launch(&endpoint, &workspace, &[]);
                (pid_of(&value), serve)
            }));
            let _ = done_tx.send(result.map_err(|_| ()));
        });
    }
    drop(done_tx);
    // 有界地收齐十个结果再断言；某个失败不影响其余进程已登记、会被清理。
    let mut candidates = Vec::new();
    for _ in 0..10 {
        let result = done_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("30 秒内没有收齐十次 launch 的结果");
        candidates.push(result);
    }
    let candidates: Vec<(u32, Option<Arc<Proc>>)> = candidates
        .into_iter()
        .map(|r| r.unwrap_or_else(|_| panic!("有一次 launch 失败")))
        .collect();
    block_on(async {
        let mut client = Client::connect(&endpoint, Duration::from_secs(15)).await;
        let winner_pid = pid_of(&env.broker_info(&endpoint));
        let mut winner = None;
        let mut verified_losers = 0;
        for (pid, process) in &candidates {
            if *pid == winner_pid {
                winner = Some(process.clone().expect("赢家应在运行"));
            } else if let Some(process) = process {
                assert_eq!(
                    process.wait(Duration::from_secs(15)),
                    Some(0),
                    "落败的候选 {pid} 应以 0 退出"
                );
                verified_losers += 1;
            }
        }
        let winner = winner.expect("赢家应是某次 launch 的候选");
        assert!(winner.alive());
        eprintln!(
            "落败的候选 9 个，其中 {verified_losers} 个拿到句柄并核对了退出码 0（其余登记前已退出）"
        );
        assert_eq!(client.health().await["claims"], 0);

        // 只有赢家拉起了 app-server：cmd.exe（替身的父进程）与 bun 都在 broker 的 Job 里。
        let events = env.wait_fixture("启动", |e| !e.is_empty());
        let starts: Vec<&Value> = events.iter().filter(|e| e["event"] == "start").collect();
        assert_eq!(starts.len(), 1, "只有赢家应启动 app-server：{events:?}");
        let bun = env.adopt(pid_of(starts[0])).expect("bun 替身应在运行");
        let shell = env
            .adopt(starts[0]["ppid"].as_u64().unwrap() as u32)
            .expect("cmd.exe 应在运行");
        drop(client);

        assert!(
            taskkill(winner.pid, false, Duration::from_secs(10)),
            "taskkill /F 赢家失败"
        );
        assert!(winner.wait(Duration::from_secs(5)).is_some());
        assert!(
            bun.wait(Duration::from_secs(5)).is_some(),
            "taskkill /F 杀 broker 后 bun 应随 Job 结束"
        );
        assert!(
            shell.wait(Duration::from_secs(5)).is_some(),
            "taskkill /F 杀 broker 后 cmd.exe 应随 Job 结束"
        );

        // 管道名随赢家退出释放：打开应很快得到「不存在」。
        let name = pipe_name(&endpoint).to_owned();
        let deadline = Instant::now() + Duration::from_secs(2);
        while ClientOptions::new()
            .open(&name)
            .err()
            .and_then(|e| e.raw_os_error())
            != Some(2)
        {
            assert!(Instant::now() < deadline, "赢家退出后 2 秒内管道名应已释放");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let (relaunched, serve) = env.launch(&endpoint, &workspace, &[]);
        let started = Instant::now();
        drop(Client::open(&endpoint, Duration::from_secs(2)).await);
        eprintln!("重新拉起后 {:?} 连上", started.elapsed());
        let mut client = Client::connect(&endpoint, Duration::from_secs(15)).await;
        assert_eq!(client.health().await["claims"], 0);
        drop(client);
        let serve = serve.expect("重新拉起的 serve 应在运行");
        assert_eq!(serve.pid, pid_of(&relaunched));
        shutdown(&endpoint, &serve).await;
    });
}

#[test]
fn 关闭期间抢不到管道名_旧的退出后新的立即抢到() {
    let _guard = serial();
    let env = Env::new();
    let workspace = env.workspace("ws");
    let endpoint = env.endpoint("closing");
    let mut old = env.broker("serve", &endpoint, &workspace);
    old.env("FIXTURE_HOLD_EXIT", "1");
    let (mut old_child, old) = env.spawn(&mut old);
    block_on(async {
        let mut client = Client::connect(&endpoint, Duration::from_secs(15)).await;
        let reply = client.request("broker/shutdown", json!({})).await;
        assert!(reply.get("error").is_none(), "{reply}");
    });
    // 屏障：替身收到 stdin EOF 后停住，broker 停在关闭第 6 步，监听实例仍占着管道名。
    // 这个窗口受私有档「关 stdin 后最多等 5 秒」约束。从看到 EOF 起共用 4 秒绝对期限，
    // 创建进程、运行时与调度耗时也算在内；放行前超期就不能采信关闭窗口结论。
    // EOF 的观测仍有延迟，所以放行前还须确认旧 broker 存活；精确交错需可控时钟测试。
    env.wait_fixture("收到 EOF", |e| e.iter().any(|x| x["event"] == "eof"));
    let deadline = Instant::now() + Duration::from_secs(4);
    let remaining = || {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "检查超过期限，关闭窗口结论不可信");
        remaining
    };
    assert!(old.alive());

    let (mut second, second_proc) = env.spawn(&mut env.broker("serve", &endpoint, &workspace));
    let second_code = second_proc.wait(remaining().min(Duration::from_secs(3)));
    remaining();
    assert_eq!(
        second_code,
        Some(0),
        "关闭期间新 serve 应抢不到管道名并静默以 0 退出"
    );
    second.wait().unwrap();
    block_on(async {
        let mut refused =
            Client::open(&endpoint, remaining().min(Duration::from_millis(500))).await;
        let closed = refused
            .closed_within(remaining().min(Duration::from_millis(500)))
            .await;
        remaining();
        assert!(closed, "关闭期间的新连接应被立即断开");
    });
    remaining();
    assert!(old.alive(), "放行前旧 broker 应仍在关闭中");
    remaining();
    env.release();

    assert_eq!(old.wait(Duration::from_secs(15)), Some(0));
    old_child.wait().unwrap();
    let (_, serve) = env.launch(&endpoint, &workspace, &[]);
    block_on(async {
        drop(Client::open(&endpoint, Duration::from_secs(2)).await);
        shutdown(&endpoint, &serve.expect("新 serve 应在运行")).await;
    });
}

#[test]
fn 十个客户端并发连接都能握手() {
    let _guard = serial();
    let env = Env::new();
    let workspace = env.workspace("ws");
    let endpoint = env.endpoint("clients");
    let (_, serve) = env.launch(&endpoint, &workspace, &[]);
    let serve = serve.expect("serve 应在运行");
    block_on(async {
        // 十个连接在同一个运行时里并发：各自打开、握手、查询。
        let clients = (0..10).map(|_| async {
            let mut client = Client::connect(&endpoint, Duration::from_secs(15)).await;
            client.health().await["protocolVersion"].clone()
        });
        let versions = futures_util::future::join_all(clients).await;
        assert!(
            versions.iter().all(|v| *v == 1),
            "每个客户端都应握手成功：{versions:?}"
        );
        shutdown(&endpoint, &serve).await;
    });
}

#[test]
fn 脱离控制台的_serve_在握手失败与存储不可用时按约定退出或降级() {
    let _guard = serial();
    let env = Env::new();
    let workspace = env.workspace("ws");

    // 握手失败：没有标准句柄也要正常走到「写明原因、以 1 退出」，而不是崩溃。替身停在 initialize，
    // 测试拿到 serve 句柄后才放行错误回包。
    let failing = env.endpoint("init-error");
    let (_, serve) = env.launch(&failing, &workspace, &[("FIXTURE_INIT_ERROR", "1")]);
    let serve = serve.expect("替身扣住 initialize，serve 应仍在");
    env.wait_fixture("收到 initialize", |e| {
        e.iter().any(|x| x["event"] == "initialize")
    });
    env.release();
    assert_eq!(serve.wait(Duration::from_secs(15)), Some(1));

    // 存储不可用（CXB_HOME 是个文件）：不写盘但继续服务，health 报 degraded。
    let blocked = env.root.join("not-a-dir");
    std::fs::write(&blocked, "x").unwrap();
    let degraded = env.endpoint("no-store");
    let mut command = env.broker("launch", &degraded, &workspace);
    command.env("CXB_HOME", &blocked);
    let (launched, serve) = env.run_launch(command, |_| {});
    let serve = serve.expect("serve 应在运行");
    block_on(async {
        let mut client = Client::connect(&degraded, Duration::from_secs(15)).await;
        let health = client.health().await;
        assert_eq!(health["storage"], "degraded");
        assert_eq!(health["parentJob"]["breakaway"], launched["breakaway"]);
        drop(client);
        shutdown(&degraded, &serve).await;
    });
}

#[test]
fn 经_launch_的接入冒烟_shared_失败退出_auto_回退私有() {
    let _guard = serial();
    let env = Env::new();
    let workspace = env.workspace("ws");
    let missing = env.root.join("no-such.sock");
    let missing = missing.to_str().unwrap();

    // shared：第 1 档 socket 不存在、第 2 档 proxy 子进程（替身不会说 WebSocket）也失败 → 以 1 退出。
    // 环境变量经 launch 原样传到 serve，否则会按缺省的私有档启动成功。
    let shared = env.endpoint("shared");
    let (_, serve) = env.launch(
        &shared,
        &workspace,
        &[("CXB_SERVER", "shared"), ("CXB_SERVER_SOCKET", missing)],
    );
    let serve = serve.expect("两档接入至少要几百毫秒，serve 应仍在");
    assert_eq!(
        serve.wait(Duration::from_secs(20)),
        Some(1),
        "shared 两档都不可用应以 1 退出"
    );
    env.wait_fixture("被当作 proxy 启动", |e| {
        e.iter().any(|x| {
            x["argv"]
                .as_array()
                .is_some_and(|a| a.iter().any(|v| v == "proxy"))
        })
    });

    // auto：两档共享都失败后回退私有档，健康查询显示 private。
    let auto = env.endpoint("auto");
    let (_, serve) = env.launch(
        &auto,
        &workspace,
        &[("CXB_SERVER", "auto"), ("CXB_SERVER_SOCKET", missing)],
    );
    let serve = serve.expect("auto 下 serve 应在运行");
    block_on(async {
        let mut client = Client::connect(&auto, Duration::from_secs(20)).await;
        assert_eq!(client.health().await["server"]["kind"], "private");
        drop(client);
        shutdown(&auto, &serve).await;
    });
}

/// 把 launch 放进测试自建的 Job：挂起创建 → 加入 Job → 恢复主线程（与第 2 档 proxy 同一写法）。
fn launch_in_job(env: &Env, job: &win32job::Job, endpoint: &str, cwd: &Path) -> (Value, Arc<Proc>) {
    let mut command = env.broker("launch", endpoint, cwd);
    command.creation_flags(CREATE_NO_WINDOW | CREATE_SUSPENDED);
    let (value, serve) = env.run_launch(command, |child| {
        job.assign_process(child.as_raw_handle() as isize)
            .expect("把 launch 加入测试 Job");
        cxb_broker::job::resume_suspended(child.id()).expect("恢复 launch 主线程");
    });
    (value, serve.expect("serve 应在运行"))
}

#[test]
fn 真实_job_组合_关闭即杀且不许脱离时报告不保证存活并确实被连累() {
    let _guard = serial();
    let env = Env::new();
    let workspace = env.workspace("ws");
    let endpoint = env.endpoint("job-deny");
    let mut info = win32job::ExtendedLimitInfo::new();
    info.limit_kill_on_job_close();
    let job = win32job::Job::create_with_limit_info(&info).unwrap();
    let (launched, serve) = launch_in_job(&env, &job, &endpoint, &workspace);
    assert_eq!(
        launched["breakaway"], false,
        "测试 Job 不许脱离，带脱离标志的创建应失败并回退"
    );
    block_on(async {
        let mut client = Client::connect(&endpoint, Duration::from_secs(15)).await;
        let health = client.health().await;
        assert_eq!(
            health["parentJob"],
            json!({"inJob": true, "killOnClose": true, "breakawayAllowed": false,
                   "breakaway": false, "queryScope": "immediateJob"})
        );
        assert_eq!(health["survivesParentJob"], false);
    });
    // 这正是存活提示要警告的情况：外层 Job 关闭时 serve 被一起结束。
    drop(job);
    assert!(
        serve.wait(Duration::from_secs(5)).is_some(),
        "外层 Job 关闭后 serve 应被结束"
    );
}

#[test]
fn 真实_job_组合_允许脱离时按实际结果报告且与存活一致() {
    let _guard = serial();
    let env = Env::new();
    let workspace = env.workspace("ws");
    let endpoint = env.endpoint("job-allow");
    let mut info = win32job::ExtendedLimitInfo::new();
    info.limit_kill_on_job_close().limit_breakaway_ok();
    let job = win32job::Job::create_with_limit_info(&info).unwrap();
    let (launched, serve) = launch_in_job(&env, &job, &endpoint, &workspace);
    let health = block_on(async {
        let mut client = Client::connect(&endpoint, Duration::from_secs(15)).await;
        client.health().await
    });
    let evidence = health["parentJob"].clone();
    assert_eq!(evidence["breakaway"], launched["breakaway"]);
    // 外层还有别的 Job（例如 Claude Code 的不许脱离）时，脱离仍可能被拒；两种结果分别核对。
    if launched["breakaway"] == true {
        eprintln!("本环境下脱离成功：{evidence}");
        // 脱离只离开测试 Job 这一层：外面若还有 Job（例如 cargo test 进程所在的关闭即杀 Job），
        // serve 仍在其中，所以不能断言「能存活」，只核对判定与依据一致（S1-评审第 24 节）。
        assert_eq!(health["survivesParentJob"], survives_formula(&evidence));
        drop(job);
        assert!(
            serve.wait(Duration::from_secs(2)).is_none(),
            "已脱离，测试 Job 关闭不应连累 serve"
        );
        block_on(shutdown(&endpoint, &serve));
    } else {
        eprintln!("本环境下脱离被拒（外层 Job 不许脱离）：{evidence}");
        assert_eq!(
            evidence,
            json!({"inJob": true, "killOnClose": true, "breakawayAllowed": true,
                   "breakaway": false, "queryScope": "immediateJob"})
        );
        assert_eq!(health["survivesParentJob"], false);
        drop(job);
        assert!(
            serve.wait(Duration::from_secs(5)).is_some(),
            "未脱离，测试 Job 关闭应结束 serve"
        );
    }
}
