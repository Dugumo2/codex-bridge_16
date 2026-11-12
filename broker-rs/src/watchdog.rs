//! core 卡死看门狗与 app-server 入站积压安全阀（M7，用户 2026-10-04 选定，S1-评审第 23 节）。
//!
//! 背景：app-server 发来的消息经读任务 → 转发任务 → core 邮箱，全程无界队列，读入从不等 core
//! （共享服务器对读得慢的连接会直接断开，见 openai/codex issue #18203、PR #19246，所以 broker 一侧不能用背压）。
//! 只有 core 长时间不前进时内存才会持续增长；这里在 core 之外兜底，触发即退出进程（core 已不能走正常关闭）：
//! - **看门狗**：运行时之外的 std 线程每 [`PROBE_INTERVAL`] 往 core 邮箱投一个探针，core 处理到它时记下时间。
//!   探针排在已积压的消息之后，所以不论卡住时堆的是 app-server 消息、客户端请求还是计时器，都能发现。
//!   判据是「core 是否前进」：每轮看已确认的最大编号有没有比上一轮大，**连续**若干轮
//!   （[`STALL_LIMIT`] / [`PROBE_INTERVAL`]，即约 30 秒）都没前进、且还有未确认的探针，才判定卡死；
//!   落后几轮但一直在确认的不算（活性复审）。按轮数而不按墙钟判断：系统睡眠期间 core 与看守线程一起暂停，
//!   跨过睡眠的那一轮只算一轮，唤醒后 core 一处理探针就恢复，不会把「刚醒」误判成卡死。
//!   启动宽限：core 处理第一个探针之前（读旧数据、处理第一批排队消息）最多宽限 [`STARTUP_GRACE_ROUNDS`] 轮，
//!   超过也判定卡死，不会因为一开始就卡住而永远不看守（活性复审）。结构参照 tokio_util_watchdog（运行时外线程检查心跳），但看的是 core 这一个任务的进度：
//!   运行时有 2 个工作线程，只有 core 卡住时整个运行时仍有心跳，通用心跳发现不了。
//! - **安全阀**：从 app-server 读入、core 还没处理完的消息按字节计数，超过 [`BACKLOG_LIMIT_BYTES`] 即退出
//!   （与 NATS 的 max_pending、Codex 对慢连接断开同一思路）。
//!
//! 触发后写明原因与积压，由调用方退出进程；私有档子进程随 Job 结束，共享档上未停的 turn 下次显示为 lost。

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// core 超过这么久没有处理新的探针即判定卡死。
pub const STALL_LIMIT: Duration = Duration::from_secs(30);
/// 探针间隔，不超过期限的一半（systemd 建议心跳间隔取 WatchdogSec 的一半）。
pub const PROBE_INTERVAL: Duration = Duration::from_secs(5);
/// app-server 入站已读入、未处理的字节上限。
pub const BACKLOG_LIMIT_BYTES: usize = 256 * 1024 * 1024;
/// core 处理第一个探针之前的宽限轮数（约 2 分钟，按 [`PROBE_INTERVAL`] 计）。
pub const STARTUP_GRACE_ROUNDS: u64 = 24;
/// 看门狗或安全阀触发时的进程退出码（BSD sysexits 的 EX_SOFTWARE）。
pub const EXIT_CORE_STALLED: i32 = 70;

/// core、转发任务与看守线程共享的进度与积压计数。
#[derive(Debug, Default)]
pub struct Health {
    /// core 已处理的最大探针编号；0 表示还没处理过（尚未开始看守）。
    acked: AtomicU64,
    backlog_bytes: AtomicUsize,
    backlog_messages: AtomicUsize,
    tripped: AtomicBool,
}

impl Health {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// core 处理到编号为 `seq` 的探针。
    pub fn ack(&self, seq: u64) {
        self.acked.fetch_max(seq, Ordering::AcqRel);
    }

    /// 转发任务把一条 app-server 消息放进 core 邮箱前调用；超过上限时返回触发原因（只返回一次）。
    pub fn enqueued(&self, bytes: usize) -> Option<String> {
        self.backlog_messages.fetch_add(1, Ordering::AcqRel);
        let total = self.backlog_bytes.fetch_add(bytes, Ordering::AcqRel) + bytes;
        if total <= BACKLOG_LIMIT_BYTES || self.tripped.swap(true, Ordering::AcqRel) {
            return None;
        }
        Some(format!(
            "app-server 入站积压超过 {} MiB（{} 字节、{} 条未处理），core 跟不上",
            BACKLOG_LIMIT_BYTES / 1024 / 1024,
            total,
            self.backlog_messages.load(Ordering::Acquire)
        ))
    }

    fn handled(&self, bytes: usize) {
        self.backlog_messages.fetch_sub(1, Ordering::AcqRel);
        self.backlog_bytes.fetch_sub(bytes, Ordering::AcqRel);
    }

    /// 计入一条 app-server 消息，返回随消息走的守卫：消息被 core 处理、发送失败、或随邮箱一起丢弃时，
    /// 守卫在 Drop 里恰好扣回一次（活性复审）。第二个值是越过上限时的触发原因（只出现一次）。
    pub fn admit(self: &Arc<Self>, bytes: usize) -> (Inflight, Option<String>) {
        let reason = self.enqueued(bytes);
        (
            Inflight {
                health: self.clone(),
                bytes,
            },
            reason,
        )
    }

    fn stalled(&self, missed: u64, interval: Duration) -> Option<String> {
        if self.tripped.swap(true, Ordering::AcqRel) {
            return None;
        }
        Some(format!(
            "core 连续 {missed} 轮（约 {} 秒）没有前进，判定卡死（app-server 积压 {} 字节、{} 条）",
            missed * interval.as_secs().max(1),
            self.backlog_bytes.load(Ordering::Acquire),
            self.backlog_messages.load(Ordering::Acquire)
        ))
    }
}

/// 随一条已计数的 app-server 消息走的守卫；Drop 时扣回。
#[derive(Debug)]
pub struct Inflight {
    health: Arc<Health>,
    bytes: usize,
}

impl Drop for Inflight {
    fn drop(&mut self) {
        self.health.handled(self.bytes);
    }
}

/// 启动看守线程。每轮用 `probe(seq)` 投一个编号递增的探针（邮箱已关闭、core 已退出时返回 false，线程随之结束），
/// 睡 `interval` 后看已确认编号是否前进；连续 `limit / interval` 轮没前进且仍有未确认探针，或第一个探针
/// 超过 [`STARTUP_GRACE_ROUNDS`] 轮仍未确认，即调用一次 `on_trip`。
pub fn spawn(
    health: Arc<Health>,
    interval: Duration,
    limit: Duration,
    probe: impl Fn(u64) -> bool + Send + 'static,
    on_trip: impl FnOnce(String) + Send + 'static,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    let rounds = (limit.as_nanos() / interval.as_nanos().max(1)).max(1) as u64;
    std::thread::Builder::new()
        .name("cxb-core-watchdog".into())
        .spawn(move || {
            let mut seq = 0u64;
            let mut missed = 0u64;
            let mut last_acked = 0u64;
            loop {
                seq += 1;
                if !probe(seq) {
                    return;
                }
                std::thread::sleep(interval);
                let acked = health.acked.load(Ordering::Acquire);
                if acked > last_acked || acked >= seq {
                    last_acked = acked;
                    missed = 0;
                    continue;
                }
                missed += 1;
                let limit = if acked == 0 {
                    STARTUP_GRACE_ROUNDS.max(rounds)
                } else {
                    rounds
                };
                if missed >= limit {
                    if let Some(reason) = health.stalled(missed, interval) {
                        on_trip(reason);
                    }
                    return;
                }
            }
        })
}

/// 触发后写诊断最多等多久；期限到无条件退出（规则复审：stderr 管道满、或 core 正卡在持有 stderr 锁的输出里时，
/// 同步写日志会把看守线程也卡住，永远到不了 exit）。
pub const EXIT_LOG_GRACE: Duration = Duration::from_secs(2);

/// 触发后的统一出口：先起保底线程（[`EXIT_LOG_GRACE`] 后无条件以 [`EXIT_CORE_STALLED`] 退出），
/// 再尽力写日志、刷新日志文件，然后退出。
pub fn exit_on_trip(reason: String) {
    trip(
        reason,
        EXIT_LOG_GRACE,
        |reason| {
            tracing::error!(%reason, "看门狗触发，broker 退出");
            eprintln!("[cxb-broker] {reason}；broker 退出");
            crate::logging::shutdown();
        },
        |code| std::process::exit(code),
    );
}

/// [`exit_on_trip`] 的可测版本：`exit` 在生产中不返回；测试里用它记录调用。
fn trip(
    reason: String,
    grace: Duration,
    log: impl FnOnce(&str),
    exit: impl Fn(i32) + Send + Sync + 'static,
) {
    let exit = Arc::new(exit);
    let fallback = exit.clone();
    let armed = std::thread::Builder::new()
        .name("cxb-watchdog-exit".into())
        .spawn(move || {
            std::thread::sleep(grace);
            fallback(EXIT_CORE_STALLED);
        });
    if armed.is_err() {
        // 连保底线程都起不来时不冒险写日志，直接退出。
        exit(EXIT_CORE_STALLED);
        return;
    }
    log(&reason);
    exit(EXIT_CORE_STALLED);
}

/// app-server 消息的估算字节数：方法名、请求 id、params、成功回包、错误回包的 message 与 data 都计入
/// （序列化到计数器，不分配）；另加 64 字节的固定开销。
pub fn event_bytes(event: &crate::appserver::AppEvent) -> usize {
    use crate::appserver::{AppError, AppEvent};
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(64);
    let mut value = |v: &serde_json::Value| {
        let _ = serde_json::to_writer(&mut counter, v);
    };
    let mut error_len = 0;
    match event {
        AppEvent::Notification { method, params } => {
            error_len += method.len();
            if let Some(params) = params {
                value(params);
            }
        }
        AppEvent::ServerRequest { id, method, params } => {
            error_len += method.len();
            value(id);
            if let Some(params) = params {
                value(params);
            }
        }
        AppEvent::Reply { result, .. } | AppEvent::LateReply { result, .. } => match result {
            Ok(v) => value(v),
            Err(AppError::Rpc { message, data, .. }) => {
                error_len += message.len();
                if let Some(data) = data {
                    value(data);
                }
            }
            Err(AppError::Timeout { method, .. }) => error_len += method.len(),
            Err(AppError::Closed(reason)) => error_len += reason.len(),
        },
        AppEvent::TimedOut { error, .. } => error_len += error.to_string().len(),
        AppEvent::Closed { reason } | AppEvent::ObservationGap { reason } => {
            error_len += reason.len()
        }
        AppEvent::LateDiscarded { .. }
        | AppEvent::LateEvicted { .. }
        | AppEvent::PrivateInstanceExited { .. }
        | AppEvent::Responsive
        | AppEvent::ReplyWritten { .. } => {}
    }
    counter.0 + error_len
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Instant;

    const FAST: Duration = Duration::from_millis(10);
    const LIMIT: Duration = Duration::from_millis(150);

    #[test]
    fn 处理第一个探针之前有宽限_超过宽限也判定卡死() {
        let (tripped, trips) = mpsc::channel();
        spawn(
            Health::new(),
            FAST,
            LIMIT,
            |_| true,
            move |r| {
                let _ = tripped.send((Instant::now(), r));
            },
        )
        .unwrap();
        let started = Instant::now();
        let (at, reason) = trips.recv_timeout(Duration::from_secs(3)).unwrap();
        // 宽限 24 轮，远长于普通期限的 15 轮。
        assert!(
            at - started >= FAST * (STARTUP_GRACE_ROUNDS as u32 - 2),
            "{:?}",
            at - started
        );
        assert!(reason.contains("判定卡死"), "{reason}");
    }

    #[test]
    fn 每轮都落后一轮但一直在确认时不触发() {
        let health = Health::new();
        let running = Arc::new(AtomicBool::new(true));
        let (tripped, trips) = mpsc::channel();
        let probe_health = health.clone();
        let probe_running = running.clone();
        let thread = spawn(
            health,
            FAST,
            LIMIT,
            move |seq| {
                // core 一直在前进，只是每次都只处理到上一轮的探针。
                probe_health.ack(seq.saturating_sub(1).max(1));
                probe_running.load(Ordering::Acquire)
            },
            move |r| {
                let _ = tripped.send(r);
            },
        )
        .unwrap();
        assert!(trips.recv_timeout(Duration::from_millis(600)).is_err());
        running.store(false, Ordering::Release);
        thread.join().unwrap();
    }

    #[test]
    fn 守卫在处理或丢弃时恰好扣回一次() {
        let health = Health::new();
        let (first, _) = health.admit(BACKLOG_LIMIT_BYTES);
        drop(first);
        let (second, reason) = health.admit(BACKLOG_LIMIT_BYTES);
        assert!(reason.is_none(), "前一条已扣回，不应越限");
        let (third, reason) = health.admit(1);
        assert!(reason.is_some());
        drop((second, third));
        assert_eq!(health.backlog_bytes.load(Ordering::Acquire), 0);
        assert_eq!(health.backlog_messages.load(Ordering::Acquire), 0);
    }

    #[test]
    fn 宽限之内不因为还没处理探针而触发() {
        let running = Arc::new(AtomicBool::new(true));
        let (tripped, trips) = mpsc::channel();
        let probe_running = running.clone();
        let thread = spawn(
            Health::new(),
            Duration::from_millis(50),
            Duration::from_millis(150),
            move |_| probe_running.load(Ordering::Acquire),
            move |r| {
                let _ = tripped.send(r);
            },
        )
        .unwrap();
        // 宽限 24 轮 × 50 ms = 1.2 秒；普通期限 3 轮早已过去也不能触发。
        assert!(trips.recv_timeout(Duration::from_millis(600)).is_err());
        running.store(false, Ordering::Release);
        thread.join().unwrap();
    }

    #[test]
    fn 探针持续被确认时不触发_停止确认后约在期限触发且只一次() {
        let health = Health::new();
        let healthy = Arc::new(AtomicBool::new(true));
        let (tripped, trips) = mpsc::channel();
        let probe_health = health.clone();
        let probe_flag = healthy.clone();
        spawn(
            health,
            FAST,
            LIMIT,
            move |seq| {
                // 模拟 core：健康时立即处理探针。
                if probe_flag.load(Ordering::Acquire) {
                    probe_health.ack(seq);
                }
                true
            },
            move |r| {
                let _ = tripped.send((Instant::now(), r));
            },
        )
        .unwrap();
        assert!(trips.recv_timeout(Duration::from_millis(500)).is_err());
        let stopped = Instant::now();
        healthy.store(false, Ordering::Release);
        let (at, reason) = trips.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(at - stopped >= LIMIT - FAST, "过早触发：{:?}", at - stopped);
        assert!(reason.contains("判定卡死"), "{reason}");
        assert!(trips.recv_timeout(Duration::from_millis(300)).is_err());
    }

    #[test]
    fn 邮箱关闭后看守线程退出() {
        let thread = spawn(Health::new(), FAST, LIMIT, |_| false, |_| unreachable!()).unwrap();
        thread.join().unwrap();
    }

    #[test]
    fn 入站积压超过上限只报一次_处理后扣回() {
        let health = Health::new();
        assert!(health.enqueued(BACKLOG_LIMIT_BYTES / 2).is_none());
        health.handled(BACKLOG_LIMIT_BYTES / 2);
        assert!(health.enqueued(BACKLOG_LIMIT_BYTES).is_none());
        let reason = health.enqueued(1).expect("超过上限");
        assert!(reason.contains("256 MiB"), "{reason}");
        assert!(health.enqueued(1).is_none());
        // 已触发后卡死判定不再重复报告。
        assert!(health.stalled(6, Duration::from_secs(5)).is_none());
    }

    #[test]
    fn 估算字节覆盖通知请求与成功和错误回包() {
        use crate::appserver::{AppError, AppEvent};
        use serde_json::json;
        let note = AppEvent::Notification {
            method: "m".into(),
            params: Some(json!({"a": "x".repeat(100)})),
        };
        assert!(event_bytes(&note) > 100);
        let reply = AppEvent::Reply {
            tag: 1,
            result: Ok(json!("y".repeat(1000))),
        };
        assert!(event_bytes(&reply) > 1000);
        let big = 3 * 1024 * 1024;
        let error = AppEvent::Reply {
            tag: 2,
            result: Err(AppError::Rpc {
                code: -32603,
                message: "e".repeat(1000),
                data: Some(json!({"blob": "z".repeat(big)})),
            }),
        };
        assert!(event_bytes(&error) > big + 1000);
        let late = AppEvent::LateReply {
            tag: 3,
            result: Err(AppError::Rpc {
                code: 1,
                message: "m".into(),
                data: Some(json!("d".repeat(big))),
            }),
        };
        assert!(event_bytes(&late) > big);
        let request = AppEvent::ServerRequest {
            id: json!("r".repeat(500)),
            method: "item/tool/call".into(),
            params: None,
        };
        assert!(event_bytes(&request) > 500);
        assert_eq!(event_bytes(&AppEvent::Responsive), 64);
    }

    #[test]
    fn 大错误回包累计越过上限会触发_处理后扣回() {
        use crate::appserver::{AppError, AppEvent};
        use serde_json::json;
        let health = Health::new();
        let event = AppEvent::Reply {
            tag: 1,
            result: Err(AppError::Rpc {
                code: -32603,
                message: "x".into(),
                data: Some(json!("d".repeat(100 * 1024 * 1024))),
            }),
        };
        let bytes = event_bytes(&event);
        assert!(health.enqueued(bytes).is_none());
        assert!(health.enqueued(bytes).is_none());
        health.handled(bytes);
        assert!(health.enqueued(bytes).is_none());
        assert!(
            health.enqueued(bytes).is_some(),
            "第三条未处理时超过 256 MiB"
        );
    }

    #[test]
    fn 写日志卡住时保底线程仍在期限内退出() {
        let (exited, exits) = mpsc::channel();
        let (release, blocked) = mpsc::channel::<()>();
        let started = Instant::now();
        let worker = std::thread::spawn(move || {
            trip(
                "测试".into(),
                Duration::from_millis(100),
                move |_| {
                    // 模拟 stderr 管道满：一直阻塞到测试放行。
                    let _ = blocked.recv();
                },
                move |code| {
                    let _ = exited.send((code, Instant::now()));
                },
            );
        });
        let (code, at) = exits.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(code, EXIT_CORE_STALLED);
        assert!(
            at - started < Duration::from_secs(1),
            "保底退出太晚：{:?}",
            at - started
        );
        drop(release);
        worker.join().unwrap();
    }
}
