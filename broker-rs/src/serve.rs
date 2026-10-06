//! `cxb-broker serve` 的启动流程（详细设计 0.1 节「启动顺序与三种互斥」、2.1 节、第 5 节）。
//!
//! 顺序：
//! 1. 创建 Job 并把自身加入（失败只告警）；
//! 2. **先抢管道名**（工作区互斥）：抢不到说明已有 broker 在服务这个工作区，静默退出 0，什么都不启动；
//! 3. 立即启动接入循环：客户端此后就能连上，消息在 core 邮箱里排队；
//! 4. 按选档结果接入 app-server（6.1 节：auto 依次试共享第 1、2 档，再拉起私有 app-server）并完成握手；
//!    失败时写明原因、以 1 退出，排队的连接随进程退出断开；
//! 5. 把 app-server 事件接进同一个邮箱，启动 core，直到关闭完成。

use tokio::sync::mpsc;

use crate::appserver::private::PrivateOptions;
use crate::appserver::select::{self, ServerEnv};
use crate::appserver::AppConnection;
use crate::cli::ServeConfig;
use crate::core::{Core, CoreConfig, CoreMsg};
use crate::endpoint::Endpoint;
use crate::error::BrokerError;
use crate::ipc::server::{spawn_accept_loop, BindError, ConnConfig, PipeListener};
use crate::job::BrokerJob;
use crate::store::{self, BrokerInfo, Loaded, StoreConfig, StoreError, StoreHandle};
use crate::watchdog::{self, Health};

/// 运行 serve，返回进程退出码。`launched_breakaway` 来自 launch 的隐藏参数，直接启动时为 None。
pub fn run(config: ServeConfig, launched_breakaway: Option<bool>) -> Result<i32, BrokerError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let code = runtime.block_on(serve(config, launched_breakaway));
    // 不无限等后台任务（例如阻塞读 stderr 的线程）：关闭流程已经回收了子进程，剩下的由 Job 兜底。
    runtime.shutdown_timeout(std::time::Duration::from_millis(500));
    code
}

async fn serve(config: ServeConfig, launched_breakaway: Option<bool>) -> Result<i32, BrokerError> {
    let parent_job = crate::job::detect_parent_job(launched_breakaway);
    // Job 句柄保存在进程级静态变量里，活到进程退出；进程退出时由系统关闭，Job 里的子进程随之结束。
    if let Err(e) = BrokerJob::adopt_for_process_lifetime() {
        tracing::warn!(error = %e, "无法把 broker 加入 Job，子进程只能靠关闭流程回收");
    }

    let pipe_name = match &config.endpoint {
        Endpoint::Pipe(name) => name.clone(),
        Endpoint::Unix(path) => {
            return Err(BrokerError::UnsupportedEndpoint(format!(
                "unix:{}",
                path.display()
            )));
        }
    };

    let listener = match PipeListener::bind_first(&pipe_name) {
        Ok(listener) => listener,
        Err(BindError::Taken) => {
            eprintln!("[cxb-broker] {pipe_name} 已被另一个 broker 占用，本进程退出");
            return Ok(0);
        }
        Err(BindError::Io(e)) => return Err(e.into()),
    };

    let (mailbox, inbox) = mpsc::unbounded_channel::<CoreMsg>();
    let conn_mailbox = mailbox.clone();
    let accept = spawn_accept_loop(listener, ConnConfig::default(), move |ev| {
        let _ = conn_mailbox.send(CoreMsg::Conn(ev));
    });
    tracing::info!(pipe = %pipe_name, idle_timeout_ms = config.idle_timeout.as_millis() as u64, "已占用管道名，等待 app-server 握手");

    let options = PrivateOptions {
        cwd: config.cwd.clone(),
        ..PrivateOptions::default()
    };
    // 环境变量已给出模式时不碰磁盘；否则读 config.json（有字节与时间上限，见 select::load_config）。
    let mut server_env = ServerEnv::from_env();
    if server_env.needs_config() {
        server_env.load_config(store::data_root().ok()).await;
    }
    let resolved = select::resolve(&server_env);
    tracing::info!(mode = ?resolved.mode, source = %resolved.source, socket = %resolved.socket_path.display(), "选定 app-server 接入方式");
    let (connection, storage) = tokio::join!(
        select::connect_app_server(&resolved, options),
        open_store(&config, &pipe_name, mailbox.clone(), &parent_job)
    );
    let (store, loaded) = match storage {
        Ok(value) => value,
        Err(StoreError::Superseded { file }) => {
            tracing::error!(%file, "已有更新的 broker，启动取消");
            if let Ok(connection) = connection {
                connection.handle.close().await;
            }
            accept.stop().await;
            return Ok(0);
        }
        Err(error) => {
            tracing::warn!(%error, "打开存储失败，本次不写盘并继续服务");
            (None, Loaded::default())
        }
    };
    let AppConnection {
        handle,
        mut events,
        server,
        user_agent,
    } = match connection {
        Ok(connection) => connection,
        Err(e) => {
            tracing::error!(error = %e, "连接 app-server 失败");
            // 两种失败的说明都以「连接 Codex … 失败：」开头，不再加前缀。
            eprintln!("[cxb-broker] {e}");
            return Ok(1);
        }
    };
    tracing::info!(
        ?server,
        user_agent = user_agent.as_deref().unwrap_or(""),
        "app-server 握手完成"
    );
    eprintln!(
        "[cxb-broker] Listening on {pipe_name} (idle timeout: {}ms)",
        config.idle_timeout.as_millis()
    );

    // 入站积压安全阀与 core 看门狗（crate::watchdog）：转发前计字节，core 处理完扣回。
    let health = Health::new();
    let app_mailbox = mailbox.clone();
    let inflow = health.clone();
    tokio::spawn(async move {
        while let Some(ev) = events.recv().await {
            let (inflight, over) = inflow.admit(watchdog::event_bytes(&ev));
            if let Some(reason) = over {
                watchdog::exit_on_trip(reason);
            }
            // 发送失败时消息连同守卫一起被丢弃，守卫扣回。
            if app_mailbox
                .send(CoreMsg::AppIn {
                    event: ev,
                    inflight,
                })
                .is_err()
            {
                break;
            }
        }
    });
    let probe_mailbox = mailbox.clone();
    if let Err(error) = watchdog::spawn(
        health.clone(),
        watchdog::PROBE_INTERVAL,
        watchdog::STALL_LIMIT,
        move |seq| probe_mailbox.send(CoreMsg::WatchdogProbe(seq)).is_ok(),
        watchdog::exit_on_trip,
    ) {
        tracing::warn!(%error, "无法启动 core 看门狗线程，继续运行但不看守");
    }

    let core = Core::new(
        CoreConfig {
            idle_timeout: config.idle_timeout,
            server,
        },
        handle,
        mailbox,
        accept,
    );
    // 复用有期限初始化得到的工作区目录名；存储不可用时用不碰磁盘的词法标识。
    let workspace_key = Some(loaded.workspace_key.clone())
        .filter(|key| !key.is_empty())
        .unwrap_or_else(|| store::workspace_key_lexical(&config.cwd));
    Ok(core
        .with_health(health)
        .with_parent_job(&parent_job)
        .with_store(store, loaded)
        .with_workspace_key(workspace_key)
        .run(inbox)
        .await)
}

/// 打开数据目录（详细设计第 7 节）；调用方在抢到管道名之后、与 app-server 握手并发调用。
async fn open_store(
    config: &ServeConfig,
    endpoint: &str,
    mailbox: mpsc::UnboundedSender<CoreMsg>,
    parent_job: &crate::job::ParentJob,
) -> Result<(Option<StoreHandle>, Loaded), StoreError> {
    // 读环境变量与进程创建时间都很快，但仍不占用 tokio 工作线程。
    let (root, proc_start_ft) = tokio::task::spawn_blocking(|| {
        Ok::<_, StoreError>((store::data_root()?, store::process_start_ft()))
    })
    .await
    .map_err(|error| {
        StoreError::Io(std::io::Error::other(format!(
            "存储初始化任务退出：{error}"
        )))
    })??;
    let (handle, loaded) = store::open(StoreConfig {
        root,
        workspace: config.cwd.clone(),
        broker: BrokerInfo {
            pid: std::process::id(),
            proc_start_ft,
            epoch: 0,
            endpoint: format!("pipe:{endpoint}"),
            broker_version: env!("CARGO_PKG_VERSION").into(),
            survives_parent_job: Some(parent_job.survives),
            parent_job: Some(parent_job.evidence.clone()),
        },
        on_event: Box::new(move |event| {
            let _ = mailbox.send(CoreMsg::Store(event));
        }),
    })
    .await?;
    Ok((Some(handle), loaded))
}
