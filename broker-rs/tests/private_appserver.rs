use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use cxb_broker::appserver::private::{connect_private, PrivateOptions, StateFailure};
use cxb_broker::appserver::{AppConnection, AppError, AppEvent, ServerInfo};
use serde_json::{json, Value};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
const WAIT: Duration = Duration::from_secs(15);

struct Fixture {
    cwd: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let cwd = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join(format!(
                "runtime-{}-{}",
                std::process::id(),
                NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
            ));
        std::fs::create_dir(&cwd).unwrap();
        Self { cwd }
    }

    fn options(&self, mode: &str, stderr: Option<&str>) -> PrivateOptions {
        let mut command = vec![
            "bun".into(),
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/app-server.ts")
                .to_string_lossy()
                .into_owned(),
            mode.into(),
        ];
        if let Some(text) = stderr {
            command.push(text.into());
        }
        PrivateOptions {
            cwd: self.cwd.clone(),
            command,
            request_timeout_ms: 3000,
        }
    }

    fn attempts(&self) -> Vec<u32> {
        std::fs::read_to_string(self.cwd.join("attempts.log"))
            .unwrap()
            .lines()
            .map(|line| line.parse().unwrap())
            .collect()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // 只删除本测试创建且已核实位于 tests 下的目录。
        let tests = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests");
        assert_eq!(self.cwd.parent(), Some(tests.as_path()));
        let _ = std::fs::remove_dir_all(&self.cwd);
    }
}

async fn request(
    connection: &AppConnection,
    method: &str,
    params: Option<Value>,
) -> Result<Value, AppError> {
    tokio::time::timeout(WAIT, connection.handle.request(method, params))
        .await
        .unwrap()
        .unwrap()
}

async fn event(connection: &mut AppConnection) -> AppEvent {
    tokio::time::timeout(WAIT, connection.events.recv())
        .await
        .unwrap()
        .unwrap()
}

/// 私有实例退出后，监督方先报告实例退出、Closed 仍是通道最后一条（任务说明 N7-A 2.3.5 终结证据）。
async fn closed_after_exit(connection: &mut AppConnection) -> AppEvent {
    let exited = event(connection).await;
    assert!(
        matches!(exited, AppEvent::PrivateInstanceExited { .. }),
        "Closed 之前应先收到实例退出事件，实际：{exited:?}"
    );
    event(connection).await
}

async fn close(connection: &AppConnection) {
    tokio::time::timeout(WAIT, connection.handle.close())
        .await
        .unwrap();
}

#[cfg(windows)]
async fn process_alive(pid: u32) -> bool {
    // tasklist 的 CSV PID 字段不受系统显示语言影响，且不需要管理权限。
    let output = tokio::process::Command::new("tasklist.exe")
        .creation_flags(0x0800_0000)
        .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    String::from_utf8_lossy(&output.stdout).contains(&format!(",\"{pid}\","))
}

#[cfg(not(windows))]
async fn process_alive(pid: u32) -> bool {
    tokio::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .unwrap()
        .success()
}

async fn assert_gone(pid: u32) {
    tokio::time::timeout(WAIT, async {
        while process_alive(pid).await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("close 应回收真实子进程");
}

#[tokio::test]
async fn 真实进程握手请求通知服务端请求和关闭回收() {
    let fixture = Fixture::new();
    let mut connection = connect_private(fixture.options("normal", None))
        .await
        .unwrap();
    assert_eq!(connection.user_agent.as_deref(), Some("fake-app-server"));
    assert_eq!(
        request(&connection, "initializeParams", None)
            .await
            .unwrap(),
        json!({
            "clientInfo":{"name":"codex-bridge","title":null,"version":env!("CARGO_PKG_VERSION")},
            "capabilities":{"experimentalApi":true,"optOutNotificationMethods":["item/reasoning/textDelta"]}
        })
    );
    assert_eq!(
        request(&connection, "echo", Some(json!({"中文":[1,null]})))
            .await
            .unwrap(),
        json!({"中文":[1,null]})
    );
    assert_eq!(
        event(&mut connection).await,
        AppEvent::Notification {
            method: "fixture/ready".into(),
            params: Some(json!({"initialized":true})),
        }
    );
    assert_eq!(
        event(&mut connection).await,
        AppEvent::ServerRequest {
            id: json!("approval-1"),
            method: "fixture/approve".into(),
            params: Some(json!({"command":"echo"})),
        }
    );
    connection
        .handle
        .respond(json!("approval-1"), json!({"approved":true}));
    assert_eq!(
        event(&mut connection).await,
        AppEvent::Notification {
            method: "fixture/answered".into(),
            params: Some(json!({"id":"approval-1","result":{"approved":true}})),
        }
    );
    let bun_pid = request(&connection, "pid", None)
        .await
        .unwrap()
        .as_u64()
        .unwrap() as u32;
    let ServerInfo::Private {
        pid: Some(shell_pid),
    } = connection.server
    else {
        panic!("必须提供 PID")
    };
    assert!(process_alive(shell_pid).await);
    assert!(process_alive(bun_pid).await);
    let ((), ()) = tokio::join!(close(&connection), close(&connection));
    assert_eq!(fixture.attempts(), vec![bun_pid]);
    assert_eq!(
        std::fs::read_to_string(fixture.cwd.join("stdin-closed.log")).unwrap(),
        bun_pid.to_string()
    );
    assert_gone(shell_pid).await;
    assert_gone(bun_pid).await;
    assert!(matches!(
        closed_after_exit(&mut connection).await,
        AppEvent::Closed { .. }
    ));
    assert_eq!(connection.events.recv().await, None);
    assert!(matches!(
        request(&connection, "after-close", None).await,
        Err(AppError::Closed(_))
    ));
}

#[tokio::test]
async fn 握手前退出只重试一次并附带原样_stderr() {
    let fixture = Fixture::new();
    let stderr = "启动失败：中文 stderr 尾巴";
    let started = tokio::time::Instant::now();
    let error = connect_private(fixture.options("early-exit", Some(stderr)))
        .await
        .unwrap_err();
    assert!(error.retried);
    assert_eq!(error.state_failure, StateFailure::None);
    assert_eq!(error.stderr_tail, stderr);
    assert!(error.to_string().contains("已重试一次"));
    assert!(started.elapsed() >= Duration::from_millis(750));
    assert_eq!(fixture.attempts().len(), 2);
    for pid in fixture.attempts() {
        assert_gone(pid).await;
    }
}

#[tokio::test]
async fn 状态库无法打开不重试并给出_doctor_建议() {
    let fixture = Fixture::new();
    let stderr = "failed to initialize sqlite state runtime: unable to open database";
    let error = connect_private(fixture.options("unopenable", Some(stderr)))
        .await
        .unwrap_err();
    assert!(!error.retried);
    assert_eq!(error.state_failure, StateFailure::Unopenable);
    assert_eq!(error.stderr_tail, stderr);
    assert!(error.to_string().contains("cxb doctor"));
    assert_eq!(fixture.attempts().len(), 1);
    assert_gone(fixture.attempts()[0]).await;
}

#[tokio::test]
async fn 状态库锁冲突重试且分类一致() {
    let fixture = Fixture::new();
    let error = connect_private(fixture.options("locked", Some("database is locked")))
        .await
        .unwrap_err();
    assert!(error.retried);
    assert_eq!(error.state_failure, StateFailure::Locked);
    assert_eq!(error.stderr_tail, "database is locked");
    assert_eq!(fixture.attempts().len(), 2);
    for pid in fixture.attempts() {
        assert_gone(pid).await;
    }
}

#[tokio::test]
async fn 运行中退出收到一次_closed_且不重启() {
    let fixture = Fixture::new();
    let mut connection = connect_private(fixture.options("normal", None))
        .await
        .unwrap();
    event(&mut connection).await;
    event(&mut connection).await;
    let pending = connection.handle.request("exit", None);
    assert_eq!(
        closed_after_exit(&mut connection).await,
        AppEvent::Closed {
            reason: "App server process exited unexpectedly".into(),
        }
    );
    assert_eq!(
        pending.await.unwrap(),
        Err(AppError::Closed(
            "App server process exited unexpectedly".into()
        ))
    );
    close(&connection).await;
    assert_eq!(connection.events.recv().await, None);
    assert_eq!(fixture.attempts().len(), 1);
    assert_gone(fixture.attempts()[0]).await;
}

#[tokio::test]
async fn 握手错误先回收子进程且保留_rpc_data_不重试() {
    let fixture = Fixture::new();
    let error = connect_private(fixture.options("init-error", None))
        .await
        .unwrap_err();
    assert!(!error.retried);
    assert_eq!(
        error.cause,
        AppError::Rpc {
            code: -32000,
            message: "拒绝握手".into(),
            data: Some(json!({"mode":"init-error"})),
        }
    );
    assert_eq!(error.stderr_tail, "initialize 被拒绝\n");
    assert_eq!(fixture.attempts().len(), 1);
    assert!(fixture.cwd.join("stdin-closed.log").exists());
    assert_gone(fixture.attempts()[0]).await;
}

#[tokio::test]
async fn 握手超时先回收且不重试() {
    let fixture = Fixture::new();
    let mut options = fixture.options("hang-init", None);
    options.request_timeout_ms = 1000;
    let error = connect_private(options).await.unwrap_err();
    assert!(!error.retried);
    assert_eq!(
        error.cause,
        AppError::Timeout {
            method: "initialize".into(),
            timeout_ms: 1000
        }
    );
    assert_eq!(fixture.attempts().len(), 1);
    assert!(fixture.cwd.join("stdin-closed.log").exists());
    assert_gone(fixture.attempts()[0]).await;
}

#[tokio::test]
async fn stdout_未换行超过十兆关闭并回收() {
    let fixture = Fixture::new();
    let mut connection = connect_private(fixture.options("normal", None))
        .await
        .unwrap();
    event(&mut connection).await;
    event(&mut connection).await;
    let pending = connection.handle.request("overflow", None);
    assert!(
        matches!(closed_after_exit(&mut connection).await, AppEvent::Closed { reason } if reason.contains("10 MiB"))
    );
    assert!(matches!(pending.await.unwrap(), Err(AppError::Closed(_))));
    close(&connection).await;
    assert_gone(fixture.attempts()[0]).await;
}

#[cfg(windows)]
#[tokio::test]
async fn windows_命令不存在也经_cmd_意外退出而重试() {
    let fixture = Fixture::new();
    let mut options = fixture.options("normal", None);
    options.command = vec!["cxb-fixture-missing-command-915710cc".into()];
    let started = tokio::time::Instant::now();
    let error = connect_private(options).await.unwrap_err();
    assert!(error.retried);
    assert_eq!(
        error.cause,
        AppError::Closed("App server process exited unexpectedly".into())
    );
    assert!(!error.stderr_tail.is_empty());
    assert!(started.elapsed() >= Duration::from_millis(750));
}
