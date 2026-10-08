use super::*;
use crate::runs::types::RunRecord;
use crate::store::tests::{read, run, wait_for, Temp};
use std::fs;

fn shared() -> Shared {
    Shared {
        state: Mutex::new(State::default()),
        wake: Condvar::new(),
        changed: Notify::new(),
        cancelled: Arc::new(AtomicBool::new(false)),
        on_event: Box::new(|_| {}),
        io_hook: Mutex::new(None),
    }
}

#[test]
fn n7_生命周期失败票据终结后不得重发旧版本() {
    use lifecycle::{File, ResultValue, Ticket, Work, WorkKind};
    let temp = Temp::new();
    let mut disk = temp.disk();
    let shared = shared();
    fs::create_dir(disk.dir.join("lifecycle.json")).unwrap();
    let ticket = Ticket::default();
    shared.submit(StoreOp::Lifecycle(Work {
        kind: WorkKind::Write {
            version: 1,
            file: File::default(),
        },
        ticket: ticket.clone(),
    }));
    shared.begin_drain();
    worker(&mut disk, &shared);
    assert!(matches!(ticket.result(), Some(ResultValue::Failed(_))));
    assert!(shared.lock().pending.is_empty());
    fs::remove_dir(disk.dir.join("lifecycle.json")).unwrap();
    assert!(!disk.dir.join("lifecycle.json").exists());
}

#[test]
fn n7_普通合并索引写入确认最高版本() {
    let temp = Temp::new();
    let mut disk = temp.disk();
    let versions = Arc::new(Mutex::new(Vec::new()));
    let captured = versions.clone();
    let mut shared = shared();
    shared.on_event = Box::new(move |event| {
        if let StoreEvent::ThreadsWritten { version, error } = event {
            assert!(error.is_none());
            captured.lock().unwrap().push(version);
        }
    });
    for version in [1, 2, 3] {
        shared.submit(StoreOp::PutThreads(ThreadIndexFile {
            version,
            ..Default::default()
        }));
    }
    shared.begin_drain();
    worker(&mut disk, &shared);
    assert_eq!(*versions.lock().unwrap(), vec![3]);
    assert_eq!(
        read::<ThreadIndexFile>(&disk.dir.join("threads.json"))
            .payload
            .version,
        3
    );
}

#[test]
fn n7_全部丢弃仍有带时间戳的单一标记() {
    let temp = Temp::new();
    let mut disk = temp.disk();
    let shared = shared();
    shared.submit(StoreOp::AppendLog {
        short_id: "t".into(),
        lines: vec!["甲".repeat(MAX_PENDING_LOG_BYTES)],
    });
    shared.begin_drain();
    worker(&mut disk, &shared);
    let text = fs::read_to_string(disk.dir.join("runs/t.log")).unwrap();
    assert_eq!(text.lines().count(), 1);
    let parts: Vec<_> = text.trim_end().splitn(4, ' ').collect();
    assert_eq!(parts[0].len(), 24);
    assert!(parts[0].ends_with('Z'));
    assert_eq!(&parts[1..], &["-", "丢行", "丢了 1 行"]);
}

#[test]
fn n7_两路截断计数合并只写一个标记() {
    let temp = Temp::new();
    let mut disk = temp.disk();
    let shared = shared();
    let line = format!(
        "2026-10-07T00:00:00.000Z run-abc-123abc 条目完成 {}",
        "甲".repeat(2000)
    );
    for _ in 0..2 {
        shared.submit(StoreOp::AppendLog {
            short_id: "t".into(),
            lines: vec![line.clone(); 300],
        });
    }
    let dropped = match &shared.lock().pending["runs/t.log"].content {
        Content::Log(log) => log.dropped,
        _ => panic!("必须是日志"),
    };
    shared.begin_drain();
    worker(&mut disk, &shared);
    let text = fs::read_to_string(disk.dir.join("runs/t.log")).unwrap();
    assert_eq!(text.matches(" - 丢行 ").count(), 1);
    assert_eq!(text.lines().count() as u64 + dropped, 601);
}

#[test]
fn 删除扫描纳入待写历史且在途写入整体拒绝() {
    use crate::runs::{PurgePlan, ThreadIndexFile};
    use crate::store::purge::{PurgeTicket, PurgeWork};
    let shared = Arc::new(shared());
    shared.submit(StoreOp::PutRun(Box::new(run("history", "t", 1))));
    let mut work = PurgeWork {
        thread: "t".into(),
        plan: PurgePlan::default(),
        index: None,
        ticket: PurgeTicket::default(),
        seeds: vec![],
    };
    shared.submit(StoreOp::Purge(work.clone()));
    {
        let state = shared.lock();
        let Content::Purge(prepared) = &state.pending["purge"].content else {
            panic!("应排入删除扫描");
        };
        assert_eq!(prepared.seeds[0].run_id, "history");
    }
    shared.lock().pending.remove("purge");
    shared.submit(StoreOp::PutThreads(ThreadIndexFile::default()));
    shared.submit(StoreOp::AppendLog {
        short_id: "short-t".into(),
        lines: vec!["日志".into()],
    });
    work.index = Some(ThreadIndexFile::default());
    work.plan
        .files
        .extend(["runs/history.json".into(), "runs/short-t.log".into()]);
    {
        let mut state = shared.lock();
        for pending in state.pending.values_mut() {
            pending.dirty = true;
            pending.retry_at = Instant::now() + Duration::from_secs(60);
        }
    }
    shared.submit(StoreOp::Purge(work.clone()));
    assert_eq!(
        shared.lock().pending.len(),
        3,
        "整体拒绝时保留旧写入，不提交任何删除"
    );
    assert_eq!(
        work.ticket.result(),
        Some(purge::PurgeResult::Busy("settling".into()))
    );
    assert!(shared.lock().purged_files.is_empty());
}

#[test]
fn 删除拒绝保留待写运行() {
    use crate::runs::{PurgePlan, ThreadIndexFile};
    use crate::store::purge::{PurgeTicket, PurgeWork};
    let temp = Temp::new();
    let mut disk = temp.disk();
    fs::create_dir(disk.dir.join("threads.json")).unwrap();
    let shared = Arc::new(shared());
    shared.submit(StoreOp::PutRun(Box::new(run("history", "t", 1))));
    shared
        .lock()
        .pending
        .get_mut("runs/history.json")
        .unwrap()
        .retry_at = Instant::now() + Duration::from_secs(60);
    let work = PurgeWork {
        thread: "t".into(),
        plan: PurgePlan {
            threads: ["t".into()].into(),
            files: ["runs/history.json".into()].into(),
            runs: Default::default(),
        },
        index: Some(ThreadIndexFile::default()),
        ticket: PurgeTicket::default(),
        seeds: vec![],
    };
    shared.submit(StoreOp::Purge(work.clone()));
    shared.begin_drain();
    worker(&mut disk, &shared);
    assert!(matches!(
        work.ticket.result(),
        Some(purge::PurgeResult::Busy(_))
    ));
    assert!(
        disk.dir.join("runs/history.json").exists(),
        "删除失败后原待写记录仍应落盘"
    );
}

#[test]
fn 入口合并_文件数量预算_核心索引不淘汰() {
    let shared = shared();
    for n in 0..1000 {
        shared.submit(StoreOp::PutRun(Box::new(run("same", "t", n))));
    }
    assert_eq!(shared.lock().pending.len(), 1);
    let state = shared.lock();
    let Content::Json(bytes) = &state.pending["runs/same.json"].content else {
        panic!("应为状态文件");
    };
    assert_eq!(
        serde_json::from_slice::<RunRecord>(bytes)
            .unwrap()
            .started_at,
        999
    );
    drop(state);
    shared.submit(StoreOp::PutThreads(ThreadIndexFile::default()));
    for n in 0..600 {
        shared.submit(StoreOp::PutRun(Box::new(run(&format!("run{n}"), "t", n))));
    }
    let state = shared.lock();
    assert_eq!(state.pending.len(), MAX_PENDING_FILES);
    assert!(state.pending.contains_key("threads.json"));
    assert!(!state.pending.contains_key("runs/same.json"));
    drop(state);
    assert_eq!(shared.status(), StorageStatus::Degraded);
}

#[test]
fn 六十四_mib_预算包含在途内容() {
    let shared = shared();
    shared.lock().active = Some(Active {
        name: "runs/active.json".into(),
        bytes: 40 * 1024 * 1024,
        log_bytes: 0,
        dirty: false,
    });
    for n in 0..30 {
        let mut record = run(&format!("r{n}"), "t", n);
        record.input = "a".repeat(1024 * 1024);
        shared.submit(StoreOp::PutRun(Box::new(record)));
    }
    let state = shared.lock();
    let total = state
        .pending
        .values()
        .map(|pending| pending.content.bytes())
        .sum::<usize>()
        + state.active.as_ref().unwrap().bytes;
    assert!(total <= MAX_PENDING_BYTES);
    assert!(!state.pending.contains_key("runs/r0.json"));
    assert!(state.pending.contains_key("runs/r29.json"));
}

#[tokio::test]
async fn 日志入口上限与丢行提示() {
    let temp = Temp::new();
    let mut disk = temp.disk();
    let shared = Arc::new(shared());
    let line = format!(
        "2026-10-07T00:00:00.000Z run-abc-123abc 条目完成 {}",
        "甲".repeat(1000)
    );
    shared.submit(StoreOp::AppendLog {
        short_id: "t".into(),
        lines: vec![line.clone(); 1000],
    });
    shared.submit(StoreOp::AppendLog {
        short_id: "t".into(),
        lines: vec!["2026-10-07T00:00:01.000Z run-abc-123abc 运行结束 最后一行".into()],
    });
    let dropped = {
        let state = shared.lock();
        let Content::Log(log) = &state.pending["runs/t.log"].content else {
            panic!("应为日志");
        };
        assert!(log.bytes + LOG_MARKER_RESERVE <= MAX_PENDING_LOG_BYTES);
        assert!(log.dropped > 0);
        log.dropped
    };
    let writer = shared.clone();
    let thread = std::thread::spawn(move || worker(&mut disk, &writer));
    let report = shared.drain(Duration::from_secs(2)).await;
    assert!(report.abandoned.is_empty());
    thread.join().unwrap();
    let text = fs::read_to_string(temp.dir().join("runs/t.log")).unwrap();
    assert!(text.starts_with(&format!(
        "2026-10-07T00:00:00.000Z - 丢行 丢了 {dropped} 行\n"
    )));
    assert!(text.ends_with("最后一行\n"));
    assert!(text.len() <= MAX_PENDING_LOG_BYTES);
}

#[tokio::test]
async fn drain_重试_dirty_并发等待者共享报告_关闭后忽略请求() {
    let temp = Temp::new();
    let (store, _) = open(temp.config()).await.unwrap();
    let path = temp.dir().join("runs/fail.json");
    fs::create_dir(&path).unwrap();
    store.submit(StoreOp::PutRun(Box::new(run("fail", "t", 1))));
    wait_for(|| store.status() == StorageStatus::Degraded).await;
    fs::remove_dir(&path).unwrap();
    let (left, right) = tokio::join!(
        store.drain(Duration::from_secs(2)),
        store.drain(Duration::from_secs(2))
    );
    assert_eq!(left, right);
    assert_eq!(left.written, 1);
    assert!(left.abandoned.is_empty());
    store.submit(StoreOp::PutRun(Box::new(run("ignored", "t", 2))));
    assert!(!temp.dir().join("runs/ignored.json").exists());
    assert_eq!(read::<RunRecord>(&path).payload.started_at, 1);
}

#[tokio::test]
async fn drain_最终失败只放弃一次() {
    let temp = Temp::new();
    let (store, _) = open(temp.config()).await.unwrap();
    fs::create_dir(temp.dir().join("runs/fail.json")).unwrap();
    store.submit(StoreOp::PutRun(Box::new(run("fail", "t", 1))));
    wait_for(|| store.status() == StorageStatus::Degraded).await;
    let report = store.drain(Duration::from_secs(2)).await;
    assert_eq!(report.written, 0);
    assert_eq!(report.abandoned, vec!["runs/fail.json"]);
    assert_eq!(report, store.drain(Duration::ZERO).await);
}

#[tokio::test]
async fn 写入线程异常通知一次_全部_drain_立即醒来() {
    let temp = Temp::new();
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = count.clone();
    let mut config = temp.config();
    config.on_event = Box::new(move |event| {
        assert!(matches!(event, StoreEvent::WriterDied { .. }));
        observed.fetch_add(1, Ordering::SeqCst);
    });
    let (store, _) = open(config).await.unwrap();
    store.owner.shared.lock().panic_writer = true;
    store.submit(StoreOp::PutRun(Box::new(run("lost", "t", 1))));
    let (left, right) = tokio::join!(
        store.drain(Duration::from_secs(2)),
        store.drain(Duration::from_secs(2))
    );
    assert_eq!(left, right);
    assert_eq!(left.abandoned, vec!["runs/lost.json"]);
    wait_for(|| count.load(Ordering::SeqCst) == 1).await;
    assert_eq!(store.status(), StorageStatus::Degraded);
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn 文件_io_阻塞不妨碍_drain_期限且迟到不发布() {
    let temp = Temp::new();
    let (store, _) = open(temp.config()).await.unwrap();
    let entered = Arc::new(AtomicBool::new(false));
    let unblocked = Arc::new((Mutex::new(false), Condvar::new()));
    let released = unblocked.clone();
    let marked = entered.clone();
    *store.owner.shared.io_hook.lock().unwrap() = Some(Arc::new(move || {
        marked.store(true, Ordering::SeqCst);
        let (lock, changed) = &*released;
        let guard = lock.lock().unwrap();
        let _guard = changed.wait_while(guard, |value| !*value).unwrap();
    }));
    store.submit(StoreOp::PutRun(Box::new(run("blocked", "t", 1))));
    wait_for(|| entered.load(Ordering::SeqCst)).await;
    let start = Instant::now();
    let report = store.drain(Duration::from_millis(40)).await;
    assert!(start.elapsed() < Duration::from_millis(500));
    assert_eq!(report.abandoned, vec!["runs/blocked.json"]);
    *unblocked.0.lock().unwrap() = true;
    unblocked.1.notify_all();
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(!temp.dir().join("runs/blocked.json").exists());
}

#[tokio::test]
async fn 初始化阻塞超时_迟到结果不得创建状态文件() {
    let temp = Temp::new();
    let unblock = Arc::new((Mutex::new(false), Condvar::new()));
    let finished = Arc::new(AtomicBool::new(false));
    let release = unblock.clone();
    let complete = finished.clone();
    let result = open_with_initializer(
        temp.config(),
        Duration::from_millis(40),
        move |config, gate| {
            let (lock, changed) = &*release;
            let guard = lock.lock().unwrap();
            let _guard = changed.wait_while(guard, |value| !*value).unwrap();
            let result = Disk::initialize(config, gate);
            complete.store(true, Ordering::SeqCst);
            result
        },
    )
    .await;
    assert!(matches!(result, Err(StoreError::Timeout(_))));
    *unblock.0.lock().unwrap() = true;
    unblock.1.notify_all();
    wait_for(|| finished.load(Ordering::SeqCst)).await;
    assert!(!temp.dir().exists());
}

#[tokio::test]
async fn 每日清理不用新提交驱动_关闭后不再清理() {
    let temp = Temp::new();
    let mut disk = temp.disk();
    let path = temp.dir().join("runs/old.log");
    disk.append_log("runs/old.log", b"old\n").unwrap();
    fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_modified(SystemTime::now() - Duration::from_millis(RETENTION_MS + 1000))
        .unwrap();
    let shared = Arc::new(shared());
    let writer = shared.clone();
    let thread = std::thread::spawn(move || {
        worker_with_interval(&mut disk, &writer, Duration::from_millis(30))
    });
    wait_for(|| !path.exists()).await;
    shared.drain(Duration::from_secs(2)).await;
    thread.join().unwrap();
    fs::write(&path, b"new\n").unwrap();
    fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_modified(SystemTime::now() - Duration::from_millis(RETENTION_MS + 1000))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(70)).await;
    assert!(path.exists());
}

#[tokio::test]
async fn 最后一个句柄释放后写完队列并退出() {
    let temp = Temp::new();
    let (store, _) = open(temp.config()).await.unwrap();
    let observed = store.owner.shared.clone();
    let clone = store.clone();
    drop(store);
    assert!(!observed.lock().draining);
    clone.submit(StoreOp::PutRun(Box::new(run("last", "t", 1))));
    drop(clone);
    wait_for(|| observed.lock().terminal).await;
    assert!(observed.lock().report.abandoned.is_empty());
    assert_eq!(
        read::<RunRecord>(&temp.dir().join("runs/last.json"))
            .payload
            .run_id,
        "last"
    );
}

#[test]
fn 多日志文件共用一_mib_预算_失败重试退避最多三十秒() {
    let shared = shared();
    for n in 0..4 {
        shared.submit(StoreOp::AppendLog {
            short_id: format!("s{n}"),
            lines: vec!["中".repeat(1000); 200],
        });
    }
    let state = shared.lock();
    let log_bytes = state
        .pending
        .values()
        .map(|pending| pending.content.log_bytes())
        .sum::<usize>();
    assert!(log_bytes <= MAX_PENDING_LOG_BYTES - LOG_MARKER_RESERVE);
    let Content::Log(oldest) = &state.pending["runs/s0.log"].content else {
        panic!("应为日志");
    };
    assert_eq!(oldest.dropped, 200);
    let mut delay = RETRY_MIN;
    let mut sequence = Vec::new();
    for _ in 0..7 {
        sequence.push(delay.as_secs());
        delay = (delay * 2).min(RETRY_MAX);
    }
    assert_eq!(sequence, [1, 2, 4, 8, 16, 30, 30]);
}

#[test]
fn submit_封口后不序列化且序列化期间终态仍拒绝入队() {
    for terminal in [false, true] {
        let shared = shared();
        if terminal {
            shared.finish(Some(StoreEvent::WriterDied {
                reason: "测试异常".into(),
            }));
        } else {
            shared.begin_drain();
        }
        shared.submit_with(StoreOp::PutRun(Box::new(run("large", "t", 1))), |_| {
            panic!("封口后不应序列化")
        });
        shared.submit_with(StoreOp::PutThreads(ThreadIndexFile::default()), |_| {
            panic!("封口后不应序列化索引")
        });
        assert!(shared.lock().pending.is_empty());
    }
    let shared = shared();
    shared.submit_with(StoreOp::PutThreads(ThreadIndexFile::default()), |_| {
        shared.finish(None);
        Ok(b"{}".to_vec())
    });
    assert!(shared.lock().pending.is_empty());
}

#[tokio::test]
async fn 文件日志初始化也受总期限限制_超时不发布迟到结果() {
    let temp = Temp::new();
    let unblock = Arc::new((Mutex::new(false), Condvar::new()));
    let entered = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let release = unblock.clone();
    let marked = entered.clone();
    let complete = finished.clone();
    let result = open_with_initializer(
        temp.config(),
        Duration::from_millis(100),
        move |config, gate| {
            let result = Disk::initialize_with_logging(config, gate, |_| {
                marked.store(true, Ordering::SeqCst);
                let (lock, changed) = &*release;
                let guard = lock.lock().unwrap();
                let _guard = changed.wait_while(guard, |value| !*value).unwrap();
                Ok(None)
            });
            complete.store(true, Ordering::SeqCst);
            result
        },
    )
    .await;
    assert!(entered.load(Ordering::SeqCst));
    assert!(matches!(result, Err(StoreError::Timeout(_))));
    *unblock.0.lock().unwrap() = true;
    unblock.1.notify_all();
    wait_for(|| finished.load(Ordering::SeqCst)).await;
    assert!(!temp.dir().join("broker.json").exists());
}

#[tokio::test]
async fn 文件日志创建失败只降级日志不阻断存储() {
    let temp = Temp::new();
    let (store, loaded) =
        open_with_initializer(temp.config(), Duration::from_secs(2), |config, gate| {
            Disk::initialize_with_logging(
                config,
                gate,
                |_| Err("测试注入：无法打开文件日志".into()),
            )
        })
        .await
        .unwrap();
    assert_eq!(loaded.epoch, 1);
    store.submit(StoreOp::PutRun(Box::new(run("ok", "t", 1))));
    assert!(store
        .drain(Duration::from_secs(2))
        .await
        .abandoned
        .is_empty());
    assert!(temp.dir().join("runs/ok.json").exists());
}

#[tokio::test]
async fn 超过六十四_mib_的索引可缩小写回并清除_dirty() {
    let temp = Temp::new();
    let mut disk = temp.disk();
    let shared = Arc::new(shared());
    let mut large = ThreadIndexFile::default();
    large.threads.insert(
        "t".into(),
        crate::runs::types::ThreadIndexEntry {
            parent_thread_id: None,
            relation_kind: None,
            source_summary: None,
            can_accept_direct_input: None,
            agent_version: None,

            resume_overrides: None,
            thread_id: "t".into(),
            short_id: "t".into(),
            last_run_id: None,
            updated_at: 1,
            model: None,
            preview: Some("甲".repeat(23 * 1024 * 1024)),
            cwd: None,
            meta: None,
        },
    );
    shared.submit(StoreOp::PutThreads(large));
    let writer = shared.clone();
    let thread = std::thread::spawn(move || worker(&mut disk, &writer));
    async fn written(shared: &Shared, count: usize) {
        tokio::time::timeout(Duration::from_secs(90), async {
            while shared.lock().report.written < count {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("大索引写入应在测试期限内完成");
    }
    written(&shared, 1).await;
    let path = temp.dir().join("threads.json");
    assert!(fs::metadata(&path).unwrap().len() > super::super::load::MAX_INITIALIZATION_BYTES);
    let blocked = temp
        .dir()
        .join(format!("threads.json.{}.2.tmp", std::process::id()));
    fs::create_dir(&blocked).unwrap();
    shared.submit(StoreOp::PutThreads(ThreadIndexFile::default()));
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            if shared
                .lock()
                .pending
                .get("threads.json")
                .is_some_and(|entry| entry.dirty)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("注入的写入失败应标记 dirty");
    fs::remove_dir(blocked).unwrap();
    written(&shared, 2).await;
    {
        let state = shared.lock();
        assert!(state.pending.is_empty());
        assert!(state.active.is_none());
    }
    let report = shared.drain(Duration::from_secs(2)).await;
    thread.join().unwrap();
    assert!(report.abandoned.is_empty());
    let restored = read::<ThreadIndexFile>(&path);
    assert_eq!(restored.revision, 2);
    assert_eq!(restored.payload, ThreadIndexFile::default());
}

#[test]
fn inbox_与线程索引同样受保护并合并到最新快照() {
    let shared = shared();
    shared.submit(StoreOp::PutInbox(crate::inbox::InboxFile::default()));
    shared.submit(StoreOp::PutThreads(ThreadIndexFile::default()));
    for n in 0..600 {
        shared.submit(StoreOp::PutRun(Box::new(run(
            &format!("inbox-budget-{n}"),
            "t",
            n,
        ))));
    }
    let state = shared.lock();
    assert_eq!(state.pending.len(), MAX_PENDING_FILES);
    assert!(state.pending.contains_key("inbox.json"));
    assert!(state.pending.contains_key("threads.json"));
    drop(state);
    shared.lock().active = Some(Active {
        name: "runs/oversized.json".into(),
        bytes: MAX_PENDING_BYTES,
        log_bytes: 0,
        dirty: false,
    });
    shared.submit(StoreOp::PutInbox(crate::inbox::InboxFile::default()));
    let state = shared.lock();
    assert!(state.pending.contains_key("inbox.json"));
    assert!(state.pending.contains_key("threads.json"));
    assert!(state.lost_data);
}

#[test]
fn 删除集合中任一文件正在写入则整体拒绝() {
    use crate::runs::PurgePlan;
    use crate::store::purge::{PurgeTicket, PurgeWork};
    let shared = shared();
    shared.lock().active = Some(Active {
        name: "runs/child.json".into(),
        bytes: 10,
        log_bytes: 0,
        dirty: false,
    });
    let work = PurgeWork {
        thread: "parent".into(),
        plan: PurgePlan {
            threads: ["parent".into(), "child".into()].into(),
            files: ["runs/child.json".into()].into(),
            ..Default::default()
        },
        index: Some(Default::default()),
        ticket: PurgeTicket::default(),
        seeds: vec![],
    };
    shared.submit(StoreOp::Purge(work.clone()));
    assert_eq!(
        work.ticket.result(),
        Some(purge::PurgeResult::Busy("settling".into()))
    );
    assert!(shared.lock().pending.is_empty());
    assert!(shared.lock().purged_files.is_empty());
}

#[tokio::test]
async fn 删除期间及完成后的迟到运行和日志写入不复活() {
    use crate::runs::PurgePlan;
    use crate::store::purge::{PurgeTicket, PurgeWork};
    let temp = Temp::new();
    let (store, _) = open(temp.config()).await.unwrap();
    store.submit(StoreOp::PutRun(Box::new(run("old", "t", 1))));
    store.submit(StoreOp::AppendLog {
        short_id: "short-t".into(),
        lines: vec!["原日志".into()],
    });
    wait_for(|| {
        let state = store.owner.shared.lock();
        state.pending.is_empty() && state.active.is_none()
    })
    .await;
    let work = PurgeWork {
        thread: "t".into(),
        plan: PurgePlan {
            threads: ["t".into()].into(),
            files: ["runs/old.json".into(), "runs/short-t.log".into()].into(),
            ..Default::default()
        },
        index: Some(Default::default()),
        ticket: PurgeTicket::default(),
        seeds: vec![],
    };
    let weak = Arc::downgrade(&store.owner.shared);
    *store.owner.shared.io_hook.lock().unwrap() = Some(Arc::new(move || {
        let shared = weak.upgrade().unwrap();
        shared.submit(StoreOp::PutRun(Box::new(run("old", "t", 2))));
        shared.submit(StoreOp::AppendLog {
            short_id: "short-t".into(),
            lines: vec!["删除中迟到".into()],
        });
    }));
    store.submit(StoreOp::Purge(work.clone()));
    wait_for(|| work.ticket.result().is_some()).await;
    *store.owner.shared.io_hook.lock().unwrap() = None;
    assert!(matches!(
        work.ticket.result(),
        Some(purge::PurgeResult::Finished(_))
    ));
    store.submit(StoreOp::PutRun(Box::new(run("old", "t", 3))));
    store.submit(StoreOp::AppendLog {
        short_id: "short-t".into(),
        lines: vec!["删除后迟到".into()],
    });
    store.drain(Duration::from_secs(3)).await;
    assert!(!temp.dir().join("runs/old.json").exists());
    assert!(!temp.dir().join("runs/short-t.log").exists());
}
