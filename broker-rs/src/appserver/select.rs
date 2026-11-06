//! 选档（详细设计 6.1 节、第 8 节「每档接入超时」）。
//!
//! - 模式：`CXB_SERVER` → 数据根目录 config.json 的 `server` 字段 → `auto`。
//!   取值只认 `auto`、`shared`、`private`，其他值跳过、看下一个来源（与上游 shared-server.ts:61–77 一致）。
//!   broker 只读 config.json，不写（7.1 节：config.json 只由 CLI 写）。
//! - 控制 socket：`CXB_SERVER_SOCKET`（相对路径按 broker 的工作目录解析）
//!   → `<CODEX_HOME>\app-server-control\app-server-control.sock`（上游 shared-server.ts:50–55）。
//! - auto：1 → 2 → 3，第 1、2 档各最多 3 秒；shared：只试 1、2，都失败就报错退出；private：只用 3。
//! - 第 3 档启动前持有 CODEX_HOME 初始化互斥量（0.1 节），握手结束即释放。

use std::path::PathBuf;
use std::time::Duration;

use super::init_lock::{self, Acquired};
use super::private::{connect_private, PrivateOptions, StartupError};
use super::{proxy, shared, AppConnection};

/// 第 1、2 档各自的接入期限（连接 + WebSocket 握手 + initialize）。
pub const TIER_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerMode {
    Auto,
    Shared,
    Private,
}

impl ServerMode {
    fn parse(value: &str) -> Option<Self> {
        match value.trim() {
            "auto" => Some(Self::Auto),
            "shared" => Some(Self::Shared),
            "private" => Some(Self::Private),
            _ => None,
        }
    }
}

/// 选档要用的外部输入，启动时一次读齐；纯函数 [`resolve`] 只依赖它，便于单测。
#[derive(Debug, Clone, Default)]
pub struct ServerEnv {
    pub cxb_server: Option<String>,
    /// config.json 的 `server` 字段（读不到、不是字符串都为 None）。
    pub config_server: Option<String>,
    pub cxb_server_socket: Option<String>,
    pub codex_home: Option<String>,
    /// 主目录：Windows 取 USERPROFILE，其他平台取 HOME。
    pub home: Option<String>,
    /// 相对路径的基准（broker 进程的工作目录）。
    pub cwd: PathBuf,
}

/// config.json 最多读多少字节、等多久（活性预审：已占管道名、客户端已能连入，读盘不能无期限地挡住启动）。
pub const CONFIG_READ_LIMIT: u64 = 64 * 1024;
pub const CONFIG_READ_TIMEOUT: Duration = Duration::from_secs(1);

impl ServerEnv {
    /// 只读环境变量（不碰磁盘）；`config_server` 由 [`Self::load_config`] 按需补上。
    pub fn from_env() -> Self {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
        Self {
            cxb_server: var("CXB_SERVER"),
            config_server: None,
            cxb_server_socket: var("CXB_SERVER_SOCKET"),
            codex_home: var("CODEX_HOME"),
            home: var(if cfg!(windows) { "USERPROFILE" } else { "HOME" }),
            cwd: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        }
    }

    /// 环境变量已给出有效模式时不读 config.json。
    pub fn needs_config(&self) -> bool {
        self.cxb_server
            .as_deref()
            .and_then(ServerMode::parse)
            .is_none()
    }

    /// 读 `<data_root>\config.json` 的 `server` 字段：最多 [`CONFIG_READ_LIMIT`] 字节、[`CONFIG_READ_TIMEOUT`]，
    /// 读不到、超限、超时、格式不对都当作没有配置（上游 shared-server.ts:69–75「坏配置在守护进程里只降级、不退出」）。
    /// 超时后后台读线程可能仍阻塞在文件系统上，但它不持有任何 broker 资源，结果被丢弃。
    pub async fn load_config(&mut self, data_root: Option<PathBuf>) {
        let Some(root) = data_root else { return };
        let read = tokio::task::spawn_blocking(move || {
            use std::io::Read;
            let mut bytes = Vec::new();
            std::fs::File::open(root.join("config.json"))
                .ok()?
                .take(CONFIG_READ_LIMIT + 1)
                .read_to_end(&mut bytes)
                .ok()?;
            if bytes.len() as u64 > CONFIG_READ_LIMIT {
                tracing::warn!("config.json 超过 {CONFIG_READ_LIMIT} 字节，忽略");
                return None;
            }
            let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
            value.get("server")?.as_str().map(str::to_owned)
        });
        match tokio::time::timeout(CONFIG_READ_TIMEOUT, read).await {
            Ok(Ok(server)) => self.config_server = server,
            Ok(Err(_)) => {}
            Err(_) => tracing::warn!(
                "读 config.json 超过 {} 秒，按没有配置处理",
                CONFIG_READ_TIMEOUT.as_secs()
            ),
        }
    }
}

/// 选档结论。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    pub mode: ServerMode,
    /// 模式从哪里来，写进日志和报错（如 `CXB_SERVER=shared`、`config server private`、`default`）。
    pub source: String,
    pub socket_path: PathBuf,
    pub codex_home: PathBuf,
}

pub fn resolve(env: &ServerEnv) -> Resolved {
    let (mode, source) = env
        .cxb_server
        .as_deref()
        .and_then(|value| {
            ServerMode::parse(value).map(|mode| (mode, format!("CXB_SERVER={}", value.trim())))
        })
        .or_else(|| {
            let value = env.config_server.as_deref()?;
            ServerMode::parse(value).map(|mode| (mode, format!("config server {}", value.trim())))
        })
        .unwrap_or((ServerMode::Auto, "default".into()));
    let absolute = |raw: &str| {
        let path = PathBuf::from(raw.trim());
        if path.is_absolute() {
            path
        } else {
            env.cwd.join(path)
        }
    };
    let codex_home = env
        .codex_home
        .as_deref()
        .map(absolute)
        .unwrap_or_else(|| PathBuf::from(env.home.as_deref().unwrap_or(".")).join(".codex"));
    let socket_path = env
        .cxb_server_socket
        .as_deref()
        .map(absolute)
        .unwrap_or_else(|| {
            codex_home
                .join("app-server-control")
                .join("app-server-control.sock")
        });
    Resolved {
        mode,
        source,
        socket_path,
        codex_home,
    }
}

/// 共享档的两档。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    Socket,
    Proxy,
}

impl Tier {
    fn label(self) -> &'static str {
        match self {
            Tier::Socket => "第 1 档（直连 socket）",
            Tier::Proxy => "第 2 档（codex app-server proxy）",
        }
    }
}

#[derive(Debug)]
pub enum ConnectError {
    /// 第 3 档失败（含「auto 回退到私有后失败」）。
    Private(StartupError),
    /// 要求共享（shared）但两档都没连上。
    SharedUnavailable {
        source: String,
        socket_path: PathBuf,
        attempts: Vec<(Tier, String)>,
    },
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectError::Private(error) => write!(f, "{error}"),
            ConnectError::SharedUnavailable {
                source,
                socket_path,
                attempts,
            } => {
                write!(
                    f,
                    "连接 Codex 共享 app-server 失败：{source} 要求使用共享服务器，但 {} 上没有可用的服务器",
                    socket_path.display()
                )?;
                for (tier, reason) in attempts {
                    write!(f, "\n  {}：{reason}", tier.label())?;
                }
                write!(
                    f,
                    "\n可以先用 `codex app-server daemon start` 启动共享服务器，或改用 CXB_SERVER=auto 让 broker 自己启动私有 app-server。"
                )
            }
        }
    }
}

impl std::error::Error for ConnectError {}

/// 按 [`Resolved`] 依次尝试各档，返回第一个握手成功的连接。
pub async fn connect_app_server(
    resolved: &Resolved,
    private: PrivateOptions,
) -> Result<AppConnection, ConnectError> {
    if resolved.mode != ServerMode::Private {
        let mut attempts = Vec::new();
        for tier in [Tier::Socket, Tier::Proxy] {
            let attempt = async {
                match tier {
                    Tier::Socket => {
                        shared::connect_socket(&resolved.socket_path, private.request_timeout_ms)
                            .await
                    }
                    Tier::Proxy => {
                        proxy::connect_proxy(
                            &private.cwd,
                            &resolved.socket_path,
                            private.request_timeout_ms,
                        )
                        .await
                    }
                }
            };
            match tokio::time::timeout(TIER_TIMEOUT, attempt).await {
                Ok(Ok(connection)) => {
                    tracing::info!(tier = tier.label(), socket = %resolved.socket_path.display(), "已接入共享 app-server");
                    return Ok(connection);
                }
                Ok(Err(reason)) => attempts.push((tier, reason)),
                Err(_) => {
                    attempts.push((tier, format!("{} 秒内没有完成接入", TIER_TIMEOUT.as_secs())))
                }
            }
        }
        if resolved.mode == ServerMode::Shared {
            return Err(ConnectError::SharedUnavailable {
                source: resolved.source.clone(),
                socket_path: resolved.socket_path.clone(),
                attempts,
            });
        }
        tracing::info!(socket = %resolved.socket_path.display(), ?attempts, "共享 app-server 不可用，改为启动私有 app-server");
    }
    let guard = init_lock::acquire(&resolved.codex_home, init_lock::INIT_LOCK_WAIT).await;
    match &guard.acquired {
        Acquired::TimedOut => tracing::warn!(
            codex_home = %resolved.codex_home.display(),
            "等初始化互斥量超过 {} 秒，照常启动私有 app-server",
            init_lock::INIT_LOCK_WAIT.as_secs()
        ),
        Acquired::Failed(reason) => {
            tracing::warn!(%reason, "初始化互斥量不可用，照常启动私有 app-server")
        }
        Acquired::Abandoned => tracing::warn!("上一个持有者异常退出，已接管初始化互斥量"),
        Acquired::Owned | Acquired::Unsupported => {}
    }
    let result = connect_private(private).await;
    drop(guard);
    result.map_err(ConnectError::Private)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env() -> ServerEnv {
        ServerEnv {
            home: Some(r"C:\Users\u".into()),
            cwd: PathBuf::from(r"D:\work"),
            ..ServerEnv::default()
        }
    }

    #[test]
    fn 缺省为_auto_且_socket_在_codex_home_下() {
        let resolved = resolve(&env());
        assert_eq!(resolved.mode, ServerMode::Auto);
        assert_eq!(resolved.source, "default");
        assert_eq!(
            resolved.codex_home,
            PathBuf::from(r"C:\Users\u").join(".codex")
        );
        assert_eq!(
            resolved.socket_path,
            PathBuf::from(r"C:\Users\u")
                .join(".codex")
                .join("app-server-control")
                .join("app-server-control.sock")
        );
    }

    #[test]
    fn 模式来源按优先级且跳过无效值() {
        let mut e = env();
        e.config_server = Some("private".into());
        assert_eq!(resolve(&e).source, "config server private");
        e.cxb_server = Some("bogus".into());
        assert_eq!(resolve(&e).source, "config server private");
        e.cxb_server = Some(" shared ".into());
        let resolved = resolve(&e);
        assert_eq!(resolved.mode, ServerMode::Shared);
        assert_eq!(resolved.source, "CXB_SERVER=shared");
    }

    #[test]
    fn 环境变量给出有效模式时不读配置文件() {
        let mut e = env();
        assert!(e.needs_config());
        e.cxb_server = Some("bogus".into());
        assert!(e.needs_config());
        e.cxb_server = Some("private".into());
        assert!(!e.needs_config());
    }

    #[test]
    fn socket_变量按优先级且相对路径按工作目录() {
        let mut e = env();
        e.codex_home = Some(r"E:\codex".into());
        e.cxb_server_socket = Some("cx.sock".into());
        assert_eq!(
            resolve(&e).socket_path,
            PathBuf::from(r"D:\work").join("cx.sock")
        );
        e.cxb_server_socket = Some(r"F:\a.sock".into());
        assert_eq!(resolve(&e).socket_path, PathBuf::from(r"F:\a.sock"));
        assert_eq!(resolve(&e).codex_home, PathBuf::from(r"E:\codex"));
    }
}
