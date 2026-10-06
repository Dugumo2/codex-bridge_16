//! 私有 app-server 初始化互斥量（详细设计 0.1 节「app-server 初始化互斥」、第 8 节）。
//!
//! 不同工作区的 broker 共用同一个 CODEX_HOME，私有 app-server 的初始化要串行。上游用按
//! CODEX_HOME 分的锁文件（client.ts:359–424、598–613），这里改用命名互斥量
//! `Local\cxb-init-<CODEX_HOME 哈希>`，只在启动私有 app-server 到握手完成这段时间持有，
//! 最多等 10 秒，等不到就照常启动并告警（与上游「锁失败仍启动」一致）。
//!
//! 互斥量约定：
//! - **Windows 互斥量归线程所有**：获取和释放必须在同一个线程上。tokio 任务会在工作线程间迁移，
//!   所以 `CreateMutexW`、`WaitForSingleObject`、`ReleaseMutex`、`CloseHandle` 全部放在一个专用线程里，
//!   守卫被丢弃时通知该线程释放并退出；
//! - `WAIT_ABANDONED`（上一个持有者没释放就退出）视同拿到，记一条告警；
//! - 等待期间 tokio 任务不阻塞（用 oneshot 等专用线程的结果）；**`acquire` 的 future 被取消、守卫还没交出去时，
//!   专用线程随后拿到锁也要立即释放并退出**（活性预审建议）；
//! - 哈希：CODEX_HOME 取绝对路径、去掉末尾分隔符、Windows 上转小写，sha256 后取前 16 个十六进制字符
//!   （与 store 的工作区目录名同一写法）；
//! - 非 Windows 平台：直接返回 [`Acquired::Unsupported`]。

use sha2::{Digest, Sha256};
use std::path::Path;
use std::time::Duration;

/// 最多等多久（第 8 节「app-server 初始化互斥量等待」）。
pub const INIT_LOCK_WAIT: Duration = Duration::from_secs(10);

/// 获取结果；只有 `Owned`、`Abandoned` 真正持有互斥量，其余照常启动。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Acquired {
    Owned,
    /// 上一个持有者异常退出后留下的互斥量，视同拿到。
    Abandoned,
    /// 等满期限仍未拿到。
    TimedOut,
    /// 创建或等待失败（原因给人看）。
    Failed(String),
    Unsupported,
}

/// 守卫：丢弃时释放互斥量（若持有）。
#[derive(Debug)]
pub struct InitGuard {
    pub acquired: Acquired,
    // 发送端随守卫丢弃，专用线程的 recv 随即结束，在原线程释放互斥量。
    release: Option<std::sync::mpsc::Sender<()>>,
}

impl Drop for InitGuard {
    fn drop(&mut self) {
        self.release.take();
    }
}

/// 互斥量名：`Local\cxb-init-<CODEX_HOME 哈希>`。
pub fn mutex_name(codex_home: &Path) -> String {
    let absolute = std::path::absolute(codex_home).unwrap_or_else(|_| codex_home.to_path_buf());
    let text = absolute.to_string_lossy();
    #[cfg(windows)]
    let text = text.replace('/', "\\").to_lowercase();
    let text = text.trim_end_matches(if cfg!(windows) {
        &['/', '\\'][..]
    } else {
        &['/'][..]
    });
    let hash = format!("{:x}", Sha256::digest(text.as_bytes()));
    format!("Local\\cxb-init-{}", &hash[..16])
}

/// 获取初始化互斥量，最多等 `wait`。任何失败都不阻止启动，由调用方记告警。
pub async fn acquire(codex_home: &Path, wait: Duration) -> InitGuard {
    #[cfg(windows)]
    {
        let name = mutex_name(codex_home);
        let (result, received) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        // 取消 acquire 会丢弃 release；结果尚未交给调用方也不会留下持有者。
        let mut guard = InitGuard {
            acquired: Acquired::TimedOut,
            release: Some(release),
        };
        if let Err(error) = std::thread::Builder::new()
            .name("cxb-init-lock".into())
            .spawn(move || {
                hold_mutex(&name, wait.min(INIT_LOCK_WAIT), result, released);
            })
        {
            guard.acquired = Acquired::Failed(format!("启动初始化互斥量线程失败：{error}"));
            guard.release.take();
            return guard;
        }
        guard.acquired = received
            .await
            .unwrap_or_else(|_| Acquired::Failed("初始化互斥量线程提前退出".into()));
        guard
    }
    #[cfg(not(windows))]
    {
        let _ = (codex_home, wait);
        InitGuard {
            acquired: Acquired::Unsupported,
            release: None,
        }
    }
}

#[cfg(windows)]
fn hold_mutex(
    name: &str,
    wait: Duration,
    result: tokio::sync::oneshot::Sender<Acquired>,
    released: std::sync::mpsc::Receiver<()>,
) {
    use windows_sys::Win32::Foundation::{
        CloseHandle, WAIT_ABANDONED, WAIT_OBJECT_0, WAIT_TIMEOUT,
    };
    use windows_sys::Win32::System::Threading::{CreateMutexW, ReleaseMutex, WaitForSingleObject};
    let name: Vec<u16> = name.encode_utf16().chain(Some(0)).collect();
    // 句柄不跨线程；每一个创建成功的出口都 CloseHandle。
    let handle = unsafe { CreateMutexW(std::ptr::null(), 0, name.as_ptr()) };
    if handle.is_null() {
        let _ = result.send(Acquired::Failed(format!(
            "创建初始化互斥量失败：{}",
            std::io::Error::last_os_error()
        )));
        return;
    }
    let start = std::time::Instant::now();
    let acquired = loop {
        if result.is_closed() {
            unsafe {
                CloseHandle(handle);
            }
            return;
        }
        let remaining = wait.saturating_sub(start.elapsed());
        let millis = remaining.as_millis().min(20) as u32;
        match unsafe { WaitForSingleObject(handle, millis) } {
            WAIT_OBJECT_0 => break Acquired::Owned,
            WAIT_ABANDONED => {
                tracing::warn!("初始化互斥量原持有线程异常退出，已接管");
                break Acquired::Abandoned;
            }
            WAIT_TIMEOUT if start.elapsed() < wait => continue,
            WAIT_TIMEOUT => break Acquired::TimedOut,
            _ => {
                break Acquired::Failed(format!(
                    "等待初始化互斥量失败：{}",
                    std::io::Error::last_os_error()
                ))
            }
        }
    };
    let owned = matches!(acquired, Acquired::Owned | Acquired::Abandoned);
    if result.send(acquired).is_ok() && owned {
        let _ = released.recv();
    }
    if owned {
        unsafe {
            ReleaseMutex(handle);
        }
    }
    unsafe {
        CloseHandle(handle);
    }
}
