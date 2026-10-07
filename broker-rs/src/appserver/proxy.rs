//! 第 2 档：经 `codex app-server proxy --sock <控制 socket>` 子进程接入共享 app-server（详细设计 6.2 节）。
//!
//! 实测（实测核查记录.md，S0 p1-proxy）：proxy 只在 stdio 与 socket 之间搬字节、不自己分帧，
//! 所以 stdin/stdout 上要走 WebSocket；直接写 JSONL 会 10 秒无回包。
//!
//! 传输约定：
//! - Windows 上经 `cmd.exe /c codex app-server proxy --sock <路径>` 启动（6.2 节第 2、3 档的统一做法，
//!   与 private.rs 的 spawn_process 同一套规则：CREATE_NO_WINDOW、kill_on_drop、进程树由 Job 兜底）；
//!   socket 路径来自环境变量或 CODEX_HOME，含 cmd 元字符（见 private.rs 的拒绝列表）时这一档直接失败、写明原因，
//!   不做转义拼接；
//! - stdin、stdout 用 `tokio::io::join` 合成一个双向流交给 [`super::ws::client_over`]；stderr 只保留末尾，
//!   用于失败说明；
//! - 回收顺序与私有档一致：先关 stdin，最多等 5 秒，再强制结束，最多等 3 秒（config.ts:111–117）；
//!   期限内没握手成功（调用方取消）时也要回收子进程；
//! - **回收整棵进程树**（活性预审第 4 条）：proxy 失败后 auto 会回退到私有档、broker 继续运行，broker 的总 Job
//!   要到 broker 退出才兜底。所以每个 proxy 子进程放进自己的 Job（KILL_ON_JOB_CLOSE，嵌套 Job 已实测可用，
//!   S3-环境验证 job_kill_tree），回收时关闭这个 Job，`cmd.exe → codex → proxy` 整棵树一起结束；
//!   「3 秒接入截止」只管什么时候放弃这一档，回收在后台有界完成（上面的 5 + 3 秒），不阻塞回退；
//! - 握手成功才算成功，`ServerInfo::Shared{socket_path, via: Proxy}`。

use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::process::{Child, Command};
use tokio::sync::{oneshot, watch};

use super::AppConnection;
use crate::job::BrokerJob;

/// 启动 proxy 子进程，经它连接 `socket_path` 上的共享 app-server 并完成握手。
pub async fn connect_proxy(
    cwd: &Path,
    socket_path: &Path,
    request_timeout_ms: u64,
) -> Result<AppConnection, String> {
    let path = validate_path(socket_path)?;
    let (mut child, job) = spawn_proxy(cwd, path).await?;
    let stdin = child.stdin.take().expect("已配置 proxy stdin");
    let stdout = child.stdout.take().expect("已配置 proxy stdout");
    let stderr = child.stderr.take().expect("已配置 proxy stderr");
    let tail = Arc::new(Mutex::new(String::new()));
    let stderr_task = tokio::spawn(super::private::collect_stderr(stderr, tail.clone()));
    let (stop, stopped) = oneshot::channel();
    let (done, mut finished) = watch::channel(false);
    tokio::spawn(async move {
        supervise(child, job, stopped).await;
        super::private::finish_pipe(stderr_task).await;
        done.send_replace(true);
    });
    let connected = super::ws::client_over(
        tokio::io::join(stdout, stdin),
        move || async move {
            let _ = stop.send(());
            let _ = finished.wait_for(|done| *done).await;
        },
        Some(request_timeout_ms),
    )
    .await;
    let (client, events) = connected.map_err(|error| with_stderr(error, &tail))?;
    let mut guard = super::ws::StartupGuard(Some(client.clone()));
    let user_agent = match super::initialize(&client).await {
        Ok(agent) => agent,
        Err(error) => {
            client.close().await;
            return Err(with_stderr(
                format!("proxy initialize 失败：{error}"),
                &tail,
            ));
        }
    };
    guard.0 = None;
    Ok(AppConnection {
        handle: super::AppHandle { inner: client },
        events,
        user_agent,
        server: super::ServerInfo::Shared {
            socket_path: path.into(),
            via: super::SharedVia::Proxy,
        },
    })
}

fn validate_path(path: &Path) -> Result<&str, String> {
    let path = path.to_str().ok_or("proxy socket 路径不是有效 UTF-8")?;
    if path.is_empty() || !crate::cli::diag::cmd_safe(path) {
        return Err("proxy socket 路径不能为空，且不能包含 cmd 元字符或空字符".into());
    }
    Ok(path)
}

fn with_stderr(reason: String, tail: &Mutex<String>) -> String {
    let tail = tail.lock().unwrap();
    if tail.is_empty() {
        reason
    } else {
        format!("{reason}\nproxy stderr：\n{tail}")
    }
}

async fn spawn_proxy(cwd: &Path, path: &str) -> Result<(Child, BrokerJob), String> {
    spawn_command(proxy_command(cwd, path)).await
}

fn proxy_command(cwd: &Path, path: &str) -> Command {
    #[cfg(windows)]
    let mut command = {
        use windows_sys::Win32::System::Threading::{CREATE_NO_WINDOW, CREATE_SUSPENDED};
        let mut command = Command::new("cmd.exe");
        // 参数已拒绝全部 cmd 元字符。raw_arg 保留引号，兼容空格及末尾反斜杠（S0 p1-proxy:341–348）。
        command.raw_arg(
            crate::cli::diag::cmd_line(&[
                ("codex", false),
                ("app-server", false),
                ("proxy", false),
                ("--sock", false),
                (path, true),
            ])
            .expect("proxy 路径已经校验"),
        );
        // 先暂停主线程，加入独立 Job 后才运行，堵住先生成子孙、再加入 Job 的竞态。
        command.creation_flags(CREATE_NO_WINDOW | CREATE_SUSPENDED);
        command
    };
    #[cfg(not(windows))]
    let mut command = {
        let mut command = Command::new("codex");
        command.args(["app-server", "proxy", "--sock", path]);
        command
    };
    command
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command
}

async fn spawn_command(mut command: Command) -> Result<(Child, BrokerJob), String> {
    let job = BrokerJob::new_kill_on_close()
        .map_err(|error| format!("创建 proxy 独立 Job 失败：{error}"))?;
    let mut child = command
        .spawn()
        .map_err(|error| format!("启动 codex app-server proxy 失败：{error}"))?;
    #[cfg(windows)]
    {
        let setup = job
            .assign(child.raw_handle().expect("子进程尚未等待") as isize)
            .map_err(|error| format!("将 proxy 加入独立 Job 失败：{error}"))
            .and_then(|_| crate::job::resume_suspended(child.id().expect("子进程尚未等待")));
        if let Err(error) = setup {
            // 尚未运行的主线程不可能生成子孙；已加入时关闭 Job 同时兜底。
            let _ = child.start_kill();
            drop(job);
            let _ = tokio::time::timeout(Duration::from_secs(3), child.wait()).await;
            return Err(error);
        }
    }
    Ok((child, job))
}

async fn supervise(mut child: Child, job: BrokerJob, mut stop: oneshot::Receiver<()>) {
    tokio::select! {
        biased;
        result = child.wait() => {
            if let Err(error) = result { tracing::warn!(%error, "等待 proxy 退出失败"); }
            // cmd 已退出也要清理仍持有管道的子孙。
            drop(job);
            return;
        }
        _ = &mut stop => {}
    }
    // ws 已释放 stdin/stdout，最多等 5 秒优雅退出；之后独立 Job 结束整棵树。
    let exited = matches!(
        tokio::time::timeout(Duration::from_secs(5), child.wait()).await,
        Ok(Ok(_))
    );
    drop(job);
    if !exited {
        let _ = child.start_kill();
        match tokio::time::timeout(Duration::from_secs(3), child.wait()).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => tracing::warn!(%error, "回收 proxy 进程失败"),
            Err(_) => tracing::warn!("关闭 proxy Job 后等待超过 3 秒"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn 路径拒绝元字符但允许空格和反斜杠() {
        for ch in [
            '&', '|', '<', '>', '^', '%', '!', '(', ')', '"', '\r', '\n', '\0',
        ] {
            assert!(validate_path(Path::new(&format!("C:\\a{ch}b.sock"))).is_err());
        }
        assert!(validate_path(Path::new("")).is_err());
        assert_eq!(
            validate_path(Path::new(r"C:\目录 含空格\a.sock")).unwrap(),
            r"C:\目录 含空格\a.sock"
        );
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn cmd_原样传递空格路径且独立_job_终止子孙() {
        use tokio::io::{AsyncBufReadExt, BufReader};
        use windows_sys::Win32::Foundation::CloseHandle;
        use windows_sys::Win32::System::Threading::{
            OpenProcess, WaitForSingleObject, PROCESS_SYNCHRONIZE,
        };
        let dir = std::env::temp_dir().join(format!("cxb-m7-proxy space-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // cmd 自带命令检验 argv；ping 是实际的孙进程，测试不依赖 Bun 或真实 Codex。
        std::fs::write(
            dir.join("codex.cmd"),
            "@echo off\r\necho [%~1][%~2][%~3][%~4] 1>&2\r\nping -n 30 127.0.0.1 > nul\r\n",
        )
        .unwrap();
        let path = r"C:\some directory\socket name.sock";
        let mut command = proxy_command(&dir, path);
        // 只给假 codex 和系统命令提供 PATH；不继承任何本机 Codex 安装目录。
        let system = Path::new(&std::env::var_os("SystemRoot").unwrap()).join("System32");
        command.env("PATH", std::env::join_paths([&dir, &system]).unwrap());
        let (mut child, job) = spawn_command(command).await.unwrap();
        let mut stderr = BufReader::new(child.stderr.take().unwrap());
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(2), stderr.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        assert!(
            line.contains(&format!("[app-server][proxy][--sock][{path}]")),
            "实际参数：{line:?}"
        );
        let parent = child.id().unwrap();
        let mut descendant = None;
        for _ in 0..100 {
            use windows_sys::Win32::System::Diagnostics::ToolHelp::{
                CreateToolhelp32Snapshot, Process32FirstW, Process32NextW, PROCESSENTRY32W,
                TH32CS_SNAPPROCESS,
            };
            let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
            assert_ne!(
                snapshot,
                windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE
            );
            let mut entry: PROCESSENTRY32W = unsafe { std::mem::zeroed() };
            entry.dwSize = std::mem::size_of::<PROCESSENTRY32W>() as u32;
            let mut found = unsafe { Process32FirstW(snapshot, &mut entry) };
            while found != 0 {
                if entry.th32ParentProcessID == parent {
                    let process =
                        unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, entry.th32ProcessID) };
                    assert!(!process.is_null());
                    descendant = Some(process);
                    break;
                }
                found = unsafe { Process32NextW(snapshot, &mut entry) };
            }
            unsafe {
                CloseHandle(snapshot);
            }
            if descendant.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let descendant = descendant.expect("应生成实际孙进程");
        assert_eq!(
            unsafe { WaitForSingleObject(descendant, 0) },
            windows_sys::Win32::Foundation::WAIT_TIMEOUT
        );
        drop(job);
        tokio::time::timeout(Duration::from_secs(2), child.wait())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            unsafe { WaitForSingleObject(descendant, 1000) },
            windows_sys::Win32::Foundation::WAIT_OBJECT_0
        );
        unsafe {
            CloseHandle(descendant);
        }
        drop(stderr);
        drop(child);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
