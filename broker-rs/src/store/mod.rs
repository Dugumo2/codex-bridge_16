//! 数据目录与唯一写入任务（详细设计 7.1、7.2 节，第 5 节第 5 步）。【甲路实现】
//!
//! 初始化和状态文件 I/O 全部在同一个专用线程执行。约束：
//! - broker 内只有这一个写入任务；core 只通过 [`StoreHandle::submit`] 投递，从不 await 写盘。
//! - 同一文件的写请求合并，只写最新内容，所以队列长度不超过不同文件的个数；日志追加另有字节上限。
//! - 外壳 `{epoch, revision, payload}`；临时文件 `<文件名>.<pid>.<revision>.tmp`；
//!   写临时文件 → FlushFileBuffers → MoveFileExW(REPLACE_EXISTING|WRITE_THROUGH)。
//! - 写失败：记错误日志、内存内容保留、该文件标 dirty，由写入任务自己按 1、2、4…秒（最长 30 秒）重试，
//!   不依赖下一个写请求触发，不因写失败退出；有 dirty 文件时 [`StoreHandle::status`] 为 Degraded。
//! - epoch：启动时读 broker.json 的 epoch 加一写回；之后写任何状态文件前读出目标文件外壳的 epoch，
//!   比自己大就放弃写，发 [`StoreEvent::Superseded`]，由 core 关闭。

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

mod disk;
mod load;
pub(crate) mod purge;
mod queue;
#[cfg(test)]
pub(crate) mod tests;

use crate::runs::types::{RunId, RunRecord, ShortId, ThreadIndexFile};

/// 状态文件根目录的环境变量：设了就用它代替 `~/.codex-bridge`（S1-评审.md 第 20 节，测试用）。
pub const HOME_ENV: &str = "CXB_HOME";
/// 运行记录、运行日志、索引项的保留期：30 天没有活动就删除（S1-评审.md 第 20 节）。
pub const RETENTION_MS: u64 = 30 * 24 * 3600 * 1000;
/// broker 自身滚动日志保留 7 天（7.1 节）。
pub const BROKER_LOG_DAYS: usize = 7;
/// 写盘失败重试：1 秒起翻倍，最长 30 秒（第 8 节）。
pub const RETRY_MIN: Duration = Duration::from_secs(1);
pub const RETRY_MAX: Duration = Duration::from_secs(30);
/// 运行日志（runs/<shortId>.log）尚未写出的追加内容总上限；超出丢最旧的行，并在写出时补一行说明丢了多少。
pub const MAX_PENDING_LOG_BYTES: usize = 1024 * 1024;

/// broker.json 的 payload（7.1 节）。`survivesParentJob` 与 `parentJob` 是 serve 启动时的外层 Job
/// 检测结论与依据（[`crate::job::ParentJob`]、详细设计第 5 节）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerInfo {
    pub pid: u32,
    /// 进程创建时间（GetProcessTimes 的 FILETIME，100ns 单位），与 pid 一起防止 pid 复用误判。
    pub proc_start_ft: u64,
    pub epoch: u64,
    pub endpoint: String,
    pub broker_version: String,
    pub survives_parent_job: Option<bool>,
    pub parent_job: Option<serde_json::Value>,
}

/// core 投递给写入任务的请求。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreOp {
    /// 查找或删除文件也交给唯一写入任务；结果由有期限的状态机计时器收取。
    Purge(purge::PurgeWork),
    /// 写 runs/<runId>.json，payload 为完整运行记录。
    PutRun(Box<RunRecord>),
    /// 写 threads.json，payload 为完整索引快照（含删除标记）。
    PutThreads(ThreadIndexFile),
    /// 写 inbox.json，payload 为全部事项（M6，4.3 节「不跨重启」：重启后只供展示）。
    PutInbox(crate::inbox::InboxFile),
    /// 追加 runs/<shortId>.log；每个元素是一行，不含换行。
    AppendLog {
        short_id: ShortId,
        lines: Vec<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageStatus {
    Ok,
    Degraded,
}

/// 写入任务主动通知 core 的事件；经 [`StoreConfig`] 里的回调投进 core 邮箱。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreEvent {
    /// 目标文件外壳的 epoch 比本 broker 大：已有更新的 broker，本进程应关闭。
    Superseded { file: String },
    /// 写入任务意外退出（panic 等），之后不再写盘；core 记 error、继续服务（任务说明 8.2 第 3 条）。
    WriterDied { reason: String },
}

/// 关闭第 5 步：写入队列在期限内写完了哪些、丢了哪些。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DrainReport {
    pub written: usize,
    /// 期限到时仍未写成功的文件名（相对工作区目录）。
    pub abandoned: Vec<String>,
}

pub struct StoreConfig {
    /// 根目录：`CXB_HOME`，否则 `~/.codex-bridge`。见 [`data_root`]。
    pub root: PathBuf,
    /// 工作区目录（serve 的 --cwd，已是绝对路径）；工作区子目录名为规范化路径 sha256 的前 16 位十六进制。
    pub workspace: PathBuf,
    /// broker.json 中除 epoch 外的字段；epoch 由 open 计算。
    pub broker: BrokerInfo,
    pub on_event: Box<dyn Fn(StoreEvent) + Send + Sync>,
}

/// 启动时读出的旧数据。规则（哪些标 lost、哪些要补读）由 runs 模块按 3.6、7.2 节处理，store 只负责读和重建索引。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Loaded {
    pub epoch: u64,
    /// threads.json 的内容；缺项或比 runs 旧时已用 runs/*.json 重建（7.2「跨文件一致性」），
    /// `removed` 里的线程不重建；运行记录带 reviewThreadId 与 reviewShortId 的，同时重建子线程的索引项。
    pub index: ThreadIndexFile,
    /// 需要进入内存账本的运行记录：每个线程最近一次运行，加上任何 state 为 running、stopping、lost
    /// 或任一轮 ready 为 pending 的运行。无法解析的文件记告警后跳过，不删除。
    pub runs: Vec<RunRecord>,
    /// inbox.json 的内容（M6）；没有或无法解析时为 None。旧事项由 inbox 模块在启动时一律标 expired。
    pub inbox: Option<crate::inbox::InboxFile>,
    /// 工作区目录名（规范化 cwd 的 sha256 前 16 位），供 gd_ 编号使用；存储不可用时为空。
    pub workspace_key: String,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("无法确定数据目录：{0}")]
    NoHome(String),
    #[error("数据目录读写失败：{0}")]
    Io(#[from] std::io::Error),
    #[error("已有更新的 broker（{file} 的 epoch 更大），本进程不再写入")]
    Superseded { file: String },
    #[error("打开数据目录超时（{0:?}），本次不写盘")]
    Timeout(Duration),
}

/// `CXB_HOME` 环境变量，否则用户主目录下的 `.codex-bridge`。
pub fn data_root() -> Result<PathBuf, StoreError> {
    data_root_from(|name| std::env::var_os(name))
}

fn data_root_from(get: impl Fn(&str) -> Option<std::ffi::OsString>) -> Result<PathBuf, StoreError> {
    if let Some(root) = get(HOME_ENV).filter(|value| !value.is_empty()) {
        return Ok(root.into());
    }
    let home = if cfg!(windows) { "USERPROFILE" } else { "HOME" };
    get(home)
        .filter(|value| !value.is_empty())
        .map(|value| PathBuf::from(value).join(".codex-bridge"))
        .ok_or_else(|| StoreError::NoHome(format!("{HOME_ENV} 和 {home} 均未设置")))
}

/// `<root>/workspaces/<sha256(规范化 cwd) 前 16 位>`。
pub fn workspace_dir(root: &std::path::Path, cwd: &std::path::Path) -> PathBuf {
    let normalized = std::fs::canonicalize(cwd)
        .unwrap_or_else(|_| std::path::absolute(cwd).unwrap_or_else(|_| cwd.to_owned()));
    root.join("workspaces").join(path_key(&normalized))
}

/// 不访问文件系统的工作区标识：只做词法上的绝对化，不解析符号链接。
/// 只在有期限的存储初始化失败、拿不到 `Loaded.workspace` 时使用（启动不得再次阻塞在路径解析上）。
pub fn workspace_key_lexical(cwd: &std::path::Path) -> String {
    path_key(&std::path::absolute(cwd).unwrap_or_else(|_| cwd.to_owned()))
}

fn path_key(path: &std::path::Path) -> String {
    use sha2::{Digest, Sha256};
    let text = path.to_string_lossy();
    let text = text.strip_prefix(r"\\?\").unwrap_or(&text);
    let hash = Sha256::digest(text.as_bytes());
    format!("{hash:x}")[..16].to_owned()
}

/// 句柄可克隆；全部克隆被丢弃后写入任务在写完队列后退出。
#[derive(Clone)]
pub struct StoreHandle {
    // 线程只持有 Shared，最后一个外部句柄释放时 Owner::drop 可立即封口。
    owner: std::sync::Arc<queue::Owner>,
}

impl StoreHandle {
    /// 非阻塞投递；同一文件合并为最新内容。关闭的句柄上投递只记告警。
    pub fn submit(&self, op: StoreOp) {
        self.owner.shared.submit(op);
    }
    pub fn status(&self) -> StorageStatus {
        self.owner.shared.status()
    }
    /// 关闭第 5 步：最多等 `timeout` 让队列写完（含 dirty 文件再试一次），之后不再接受投递。
    pub async fn drain(&self, timeout: Duration) -> DrainReport {
        self.owner.shared.drain(timeout).await
    }
}

/// 打开数据目录：建目录、递增 epoch 并写 broker.json、读出旧数据（必要时重建索引）、
/// 执行一次 30 天清理、启动写入任务和每日清理计时。
pub async fn open(config: StoreConfig) -> Result<(StoreHandle, Loaded), StoreError> {
    queue::open_with_timeout(config, Duration::from_secs(10)).await
}

/// 本进程的 Windows FILETIME 创建时间；其他平台返回 0。
pub fn process_start_ft() -> u64 {
    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::FILETIME;
        use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetProcessTimes};
        let mut created = FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };
        let mut exited = created;
        let mut kernel = created;
        let mut user = created;
        // 所有输出指针均指向本栈帧中有效的 FILETIME，本进程伪句柄无需关闭。
        if unsafe {
            GetProcessTimes(
                GetCurrentProcess(),
                &mut created,
                &mut exited,
                &mut kernel,
                &mut user,
            )
        } != 0
        {
            return (u64::from(created.dwHighDateTime) << 32) | u64::from(created.dwLowDateTime);
        }
        tracing::warn!(error = %std::io::Error::last_os_error(), "读取进程创建时间失败");
    }
    0
}

/// runs 记录文件名，供测试与 M6 读取：`runs/<runId>.json`。
pub fn run_file(run_id: &RunId) -> String {
    format!("runs/{run_id}.json")
}
