//! 存储契约测试：真实文件系统、损坏恢复和故障注入。

use super::disk::{Disk, Envelope, Gate};
use super::*;
use crate::runs::types::*;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime};

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

pub(super) struct Temp {
    pub root: PathBuf,
}
impl Temp {
    pub fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "cxb-store-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).unwrap();
        Self { root }
    }
    pub fn config(&self) -> StoreConfig {
        StoreConfig {
            root: self.root.clone(),
            workspace: self.root.clone(),
            broker: BrokerInfo {
                pid: std::process::id(),
                proc_start_ft: process_start_ft(),
                epoch: 0,
                endpoint: "测试端点".into(),
                broker_version: "测试版本".into(),
                survives_parent_job: None,
                parent_job: None,
            },
            on_event: Box::new(|_| {}),
        }
    }
    pub fn dir(&self) -> PathBuf {
        workspace_dir(&self.root, &self.root)
    }
    pub fn disk(&self) -> Disk {
        Disk::initialize(
            &mut self.config(),
            Gate {
                cancelled: Arc::new(AtomicBool::new(false)),
                initialization: None,
            },
        )
        .unwrap()
        .0
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        // 测试只删除本例创建且仍位于系统临时目录内的专用目录。
        assert!(self.root.starts_with(std::env::temp_dir()));
        fs::remove_dir_all(&self.root).expect("测试临时目录必须删除成功");
        assert!(!self.root.exists());
    }
}

pub(crate) fn run(id: &str, thread: &str, started: u64) -> RunRecord {
    RunRecord {
        creation_order: None,
        start_confirmation: Default::default(),
        children: Vec::new(),
        children_stop: None,
        background_cleanup: None,

        run_id: id.into(),
        thread_id: thread.into(),
        short_id: format!("short-{thread}"),
        joined: false,
        state: RunState::Ended,
        status: Some(RunStatus::Completed),
        started_at: started,
        ended_at: Some(started + 1),
        meta: None,
        review_thread_id: None,
        review_short_id: None,
        input: "输入".into(),
        continued_from: None,
        interrupt_reason: None,
        stop_outcome: None,
        stop_check: None,
        loop_guard: None,
        progress: Progress::default(),
        turns: vec![],
        turns_total: 0,
    }
}

fn put<T: Serialize>(path: &Path, epoch: u64, revision: u64, payload: T) {
    fs::write(
        path,
        serde_json::to_vec(&Envelope {
            epoch,
            revision,
            payload,
        })
        .unwrap(),
    )
    .unwrap();
}

pub(super) fn read<T: serde::de::DeserializeOwned>(path: &Path) -> Envelope<T> {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

pub(super) async fn wait_for(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(6);
    while !predicate() {
        assert!(Instant::now() < deadline, "等待条件超时");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[test]
fn 数据根目录与工作区哈希() {
    use sha2::{Digest, Sha256};
    assert_eq!(
        data_root_from(|name| (name == HOME_ENV).then(|| "自定目录".into())).unwrap(),
        PathBuf::from("自定目录")
    );
    let home = if cfg!(windows) { "USERPROFILE" } else { "HOME" };
    assert_eq!(
        data_root_from(|name| match name {
            HOME_ENV => Some("".into()),
            name if name == home => Some("主目录".into()),
            _ => None,
        })
        .unwrap(),
        PathBuf::from("主目录/.codex-bridge")
    );
    assert!(matches!(
        data_root_from(|_| None),
        Err(StoreError::NoHome(_))
    ));
    let temp = Temp::new();
    assert_eq!(temp.dir(), workspace_dir(&temp.root, &temp.root.join(".")));
    let missing = temp.root.join("不存在");
    let hash = format!("{:x}", Sha256::digest(missing.to_string_lossy().as_bytes()));
    assert_eq!(
        workspace_dir(&temp.root, &missing),
        temp.root.join("workspaces").join(&hash[..16])
    );
    #[cfg(windows)]
    assert!(process_start_ft() > 0);
    #[cfg(not(windows))]
    assert_eq!(process_start_ft(), 0);
}

#[tokio::test]
async fn 外壳与每文件_revision_跨启动延续() {
    let temp = Temp::new();
    let (store, loaded) = open(temp.config()).await.unwrap();
    assert_eq!(loaded.epoch, 1);
    store.submit(StoreOp::PutRun(Box::new(run("one", "t", 1))));
    store.submit(StoreOp::PutThreads(ThreadIndexFile::default()));
    assert!(store
        .drain(Duration::from_secs(2))
        .await
        .abandoned
        .is_empty());
    let file: Envelope<RunRecord> = read(&temp.dir().join("runs/one.json"));
    assert_eq!((file.epoch, file.revision), (1, 1));
    let (store, loaded) = open(temp.config()).await.unwrap();
    assert_eq!(loaded.epoch, 2);
    store.submit(StoreOp::PutRun(Box::new(run("one", "t", 2))));
    store.submit(StoreOp::PutRun(Box::new(run("two", "t", 3))));
    store.drain(Duration::from_secs(2)).await;
    let file: Envelope<RunRecord> = read(&temp.dir().join("runs/one.json"));
    assert_eq!(
        (file.epoch, file.revision, file.payload.started_at),
        (2, 2, 2)
    );
    assert_eq!(
        read::<RunRecord>(&temp.dir().join("runs/two.json")).revision,
        1
    );
    assert_eq!(
        read::<BrokerInfo>(&temp.dir().join("broker.json")).revision,
        2
    );
}

#[test]
fn 残留临时文件与并发读取只见完整版本() {
    let temp = Temp::new();
    let mut disk = temp.disk();
    let name = "runs/atomic.json";
    disk.write_json(name, br#"{"number":0}"#).unwrap();
    fs::write(
        temp.dir().join("runs/atomic.json.777.2.tmp"),
        b"{\"epoch\":",
    )
    .unwrap();
    fs::write(temp.dir().join("threads.json.777.2.tmp"), b"{").unwrap();
    let (mut disk, _) = Disk::initialize(
        &mut temp.config(),
        Gate {
            cancelled: Arc::new(AtomicBool::new(false)),
            initialization: None,
        },
    )
    .unwrap();
    assert!(!temp.dir().join("runs/atomic.json.777.2.tmp").exists());
    assert!(!temp.dir().join("threads.json.777.2.tmp").exists());
    let done = Arc::new(AtomicBool::new(false));
    let flag = done.clone();
    let path = temp.dir().join(name);
    let reader = std::thread::spawn(move || {
        let mut count = 0;
        while !flag.load(Ordering::Acquire) || count < 50 {
            let envelope: Envelope<serde_json::Value> = read(&path);
            assert!(envelope.payload["number"].as_u64().unwrap() <= 120);
            count += 1;
        }
        count
    });
    for number in 1..=120 {
        let deadline = Instant::now() + Duration::from_secs(2);
        // Windows 上并发打开目标文件可能暂时阻止替换；失败仍须保留完整旧版。
        loop {
            match disk.write_json(name, format!("{{\"number\":{number}}}").as_bytes()) {
                Ok(()) => break,
                Err(StoreError::Io(error))
                    if error.kind() == std::io::ErrorKind::PermissionDenied
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(1))
                }
                Err(error) => {
                    done.store(true, Ordering::Release);
                    reader.join().unwrap();
                    panic!("原子替换失败：{error}");
                }
            }
        }
    }
    done.store(true, Ordering::Release);
    assert!(reader.join().unwrap() >= 50);
    assert_eq!(
        read::<serde_json::Value>(&temp.dir().join(name)).payload["number"],
        120
    );
}

#[tokio::test]
async fn 写失败降级_新内容覆盖_dirty_无需新投递也重试() {
    let temp = Temp::new();
    let (store, _) = open(temp.config()).await.unwrap();
    let target = temp.dir().join("runs/fail.json");
    fs::create_dir(&target).unwrap();
    store.submit(StoreOp::PutRun(Box::new(run("fail", "t", 1))));
    wait_for(|| store.status() == StorageStatus::Degraded).await;
    store.submit(StoreOp::PutRun(Box::new(run("fail", "t", 9))));
    fs::remove_dir(&target).unwrap();
    wait_for(|| target.is_file() && store.status() == StorageStatus::Ok).await;
    assert_eq!(read::<RunRecord>(&target).payload.started_at, 9);
    assert!(store
        .drain(Duration::from_secs(2))
        .await
        .abandoned
        .is_empty());
}

#[tokio::test]
async fn 更高_epoch_只通知一次且停止所有写入() {
    let temp = Temp::new();
    let count = Arc::new(AtomicU64::new(0));
    let events = count.clone();
    let mut config = temp.config();
    config.on_event = Box::new(move |event| {
        assert!(matches!(event, StoreEvent::Superseded { .. }));
        events.fetch_add(1, Ordering::SeqCst);
    });
    let (store, _) = open(config).await.unwrap();
    put(&temp.dir().join("runs/new.json"), 50, 1, run("new", "t", 1));
    store.submit(StoreOp::PutRun(Box::new(run("new", "t", 2))));
    wait_for(|| count.load(Ordering::SeqCst) == 1).await;
    assert_eq!(store.status(), StorageStatus::Degraded);
    store.submit(StoreOp::PutThreads(ThreadIndexFile::default()));
    let report = store.drain(Duration::from_secs(2)).await;
    assert_eq!(report.abandoned, vec!["runs/new.json"]);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(
        read::<RunRecord>(&temp.dir().join("runs/new.json")).epoch,
        50
    );
    assert!(!temp.dir().join("threads.json").exists());
}

#[tokio::test]
async fn 从运行恢复索引_删除标记_review_任意_pending_与损坏文件() {
    let temp = Temp::new();
    let mut disk = temp.disk();
    let mut old = run("old", "parent", 10);
    old.turns = vec![
        TurnResult {
            turn_id: "turn1".into(),
            ready: Ready::Pending,
            status: TurnStatus::Completed,
            output: String::new(),
            output_source: OutputSource::None,
            error: None,
            completed_at: 11,
        },
        TurnResult {
            turn_id: "turn2".into(),
            ready: Ready::Ready,
            status: TurnStatus::Completed,
            output: "第二轮".into(),
            output_source: OutputSource::FinalAnswer,
            error: None,
            completed_at: 12,
        },
    ];
    let mut new = run("new", "parent", 20);
    new.review_thread_id = Some("child".into());
    new.review_short_id = Some("short-child".into());
    let obsolete = run("obsolete", "parent", 5);
    let removed = run("removed", "removed-thread", 30);
    let mut lost = run("lost", "parent", 2);
    lost.state = RunState::Lost;
    for record in [&old, &new, &obsolete, &removed, &lost] {
        disk.write_json(
            &run_file(&record.run_id),
            &serde_json::to_vec(record).unwrap(),
        )
        .unwrap();
    }
    let mut index = ThreadIndexFile::default();
    index.removed.insert("removed-thread".into(), 40);
    index.threads.insert(
        old.short_id.clone(),
        ThreadIndexEntry {
            parent_thread_id: None,
            relation_kind: None,
            source_summary: None,
            can_accept_direct_input: None,
            agent_version: None,

            resume_overrides: None,
            thread_id: old.thread_id.clone(),
            short_id: old.short_id.clone(),
            last_run_id: Some(old.run_id.clone()),
            updated_at: 999,
            model: Some("保留模型".into()),
            preview: None,
            cwd: None,
            meta: None,
        },
    );
    disk.write_json("threads.json", &serde_json::to_vec(&index).unwrap())
        .unwrap();
    fs::write(temp.dir().join("runs/broken.json"), b"{").unwrap();
    let (store, loaded) = open(temp.config()).await.unwrap();
    assert_eq!(
        loaded.index.threads["short-parent"].last_run_id.as_deref(),
        Some("new")
    );
    assert_eq!(
        loaded.index.threads["short-parent"].model.as_deref(),
        Some("保留模型")
    );
    assert_eq!(
        loaded.index.threads["short-child"].last_run_id.as_deref(),
        Some("new")
    );
    assert!(!loaded.index.threads.contains_key("short-removed-thread"));
    assert!(loaded.runs.iter().any(|record| record.run_id == "old"));
    assert!(loaded.runs.iter().any(|record| record.run_id == "lost"));
    assert!(!loaded.runs.iter().any(|record| record.run_id == "obsolete"));
    assert!(temp.dir().join("runs/broken.json").exists());
    store.drain(Duration::from_secs(2)).await;
}

#[test]
fn 三十天清理同步删除元数据_不删损坏记录() {
    let temp = Temp::new();
    let mut disk = temp.disk();
    let name = "runs/old.json";
    disk.write_json(name, &serde_json::to_vec(&run("old", "t", 1)).unwrap())
        .unwrap();
    disk.append_log("runs/old.log", b"old\n").unwrap();
    fs::write(temp.dir().join("runs/old.log.1"), b"older\n").unwrap();
    fs::write(temp.dir().join("runs/broken.json"), b"{").unwrap();
    let now = SystemTime::now();
    let old = now - Duration::from_millis(RETENTION_MS + 1000);
    for file in [name, "runs/old.log", "runs/old.log.1", "runs/broken.json"] {
        fs::File::options()
            .write(true)
            .open(temp.dir().join(file))
            .unwrap()
            .set_modified(old)
            .unwrap();
    }
    disk.cleanup(now).unwrap();
    assert!(!temp.dir().join(name).exists());
    assert!(!temp.dir().join("runs/old.log").exists());
    assert!(!temp.dir().join("runs/old.log.1").exists());
    assert!(temp.dir().join("runs/broken.json").exists());
    assert!(!disk.metadata.contains_key(name));
    assert!(!disk.metadata.contains_key("runs/old.log"));
    disk.write_json(name, &serde_json::to_vec(&run("old", "t", 2)).unwrap())
        .unwrap();
    assert_eq!(read::<RunRecord>(&temp.dir().join(name)).revision, 1);
}

#[test]
fn 日志部分追加重试不重复_超过十_mib_轮转一份() {
    let temp = Temp::new();
    let mut disk = temp.disk();
    let name = "runs/log.log";
    disk.append_log(name, "已提交\n".as_bytes()).unwrap();
    disk.fail_after_log_bytes = Some(4);
    assert!(disk.append_log(name, "失败后重试\n".as_bytes()).is_err());
    disk.append_log(name, "失败后重试\n".as_bytes()).unwrap();
    assert_eq!(
        fs::read_to_string(temp.dir().join(name)).unwrap(),
        "已提交\n失败后重试\n"
    );
    let block = vec![b'x'; 1024 * 1024];
    for _ in 0..11 {
        disk.append_log(name, &block).unwrap();
    }
    assert!(
        fs::metadata(temp.dir().join("runs/log.log.1"))
            .unwrap()
            .len()
            <= disk::MAX_LOG_FILE_BYTES
    );
    assert!(fs::metadata(temp.dir().join(name)).unwrap().len() <= disk::MAX_LOG_FILE_BYTES);
    for _ in 0..11 {
        disk.append_log(name, &block).unwrap();
    }
    assert!(!temp.dir().join("runs/log.log.2").exists());
}

#[test]
fn n7_轮转前后逐行保留时间戳运行编号与事件格式() {
    let temp = Temp::new();
    let mut disk = temp.disk();
    let line = format!(
        "2026-10-07T00:00:00.000Z run-abc-123abc 条目完成 {}\n",
        "甲".repeat(240)
    );
    let block = line.repeat(1024);
    for _ in 0..16 {
        disk.append_log("runs/n7.log", block.as_bytes()).unwrap();
    }
    for file in ["runs/n7.log", "runs/n7.log.1"] {
        let text = fs::read_to_string(temp.dir().join(file)).unwrap();
        assert!(!text.is_empty());
        for value in text.lines() {
            let fields: Vec<_> = value.splitn(4, ' ').collect();
            assert_eq!(
                &fields[..3],
                &["2026-10-07T00:00:00.000Z", "run-abc-123abc", "条目完成"]
            );
            assert_eq!(fields[3].chars().count(), 240);
        }
    }
}

#[tokio::test]
async fn 初始化超时之后不再创建文件() {
    let temp = Temp::new();
    assert!(matches!(
        queue::open_with_timeout(temp.config(), Duration::ZERO).await,
        Err(StoreError::Timeout(_))
    ));
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(!temp.dir().join("broker.json").exists());
}

#[tokio::test]
async fn 损坏索引启动后原子修复文件并保留诊断副本() {
    for corrupt in [
        b"{\"epoch\":".as_slice(),
        br#"{"epoch":1,"revision":7,"payload":[]}"#.as_slice(),
    ] {
        let temp = Temp::new();
        let mut disk = temp.disk();
        let record = run("recover", "thread", 100);
        disk.write_json(
            &run_file(&record.run_id),
            &serde_json::to_vec(&record).unwrap(),
        )
        .unwrap();
        fs::write(temp.dir().join("threads.json"), corrupt).unwrap();
        let (store, loaded) = open(temp.config()).await.unwrap();
        let restored: Envelope<ThreadIndexFile> = read(&temp.dir().join("threads.json"));
        assert_eq!(restored.epoch, loaded.epoch);
        assert_eq!(
            restored.payload.threads["short-thread"]
                .last_run_id
                .as_deref(),
            Some("recover")
        );
        let backups: Vec<_> = fs::read_dir(temp.dir())
            .unwrap()
            .map(Result::unwrap)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("threads.json.corrupt-")
            })
            .collect();
        assert_eq!(backups.len(), 1);
        assert_eq!(fs::read(backups[0].path()).unwrap(), corrupt);
        let mut changed = loaded.index;
        changed.removed.insert("已删除线程".into(), 101);
        store.submit(StoreOp::PutThreads(changed.clone()));
        assert!(store
            .drain(Duration::from_secs(2))
            .await
            .abandoned
            .is_empty());
        assert_eq!(store.status(), StorageStatus::Ok);
        assert_eq!(
            read::<ThreadIndexFile>(&temp.dir().join("threads.json")).payload,
            changed
        );
    }
}

#[tokio::test]
async fn 索引_io_错误与更大_epoch_不得当作损坏覆盖() {
    let temp = Temp::new();
    let disk = temp.disk();
    let path = disk.dir.join("threads.json");
    fs::create_dir(&path).unwrap();
    assert!(matches!(open(temp.config()).await, Err(StoreError::Io(_))));
    assert!(path.is_dir());
    fs::remove_dir(&path).unwrap();
    let bytes = br#"{"epoch":100,"revision":1,"payload":[]}"#;
    fs::write(&path, bytes).unwrap();
    assert!(matches!(
        open(temp.config()).await,
        Err(StoreError::Superseded { .. })
    ));
    assert_eq!(fs::read(path).unwrap(), bytes);
    assert!(!fs::read_dir(temp.dir())
        .unwrap()
        .map(Result::unwrap)
        .any(|entry| entry
            .file_name()
            .to_string_lossy()
            .starts_with("threads.json.corrupt-")));
}

#[tokio::test]
async fn 同毫秒编号逆序恢复保留索引指向的后受理运行() {
    let temp = Temp::new();
    let mut disk = temp.disk();
    let old = run("z-old-a", "parent", 100);
    let new = run("a-new-b", "parent", 100);
    for record in [&old, &new] {
        disk.write_json(
            &run_file(&record.run_id),
            &serde_json::to_vec(record).unwrap(),
        )
        .unwrap();
    }
    let mut index = ThreadIndexFile::default();
    index.threads.insert(
        new.short_id.clone(),
        ThreadIndexEntry {
            parent_thread_id: None,
            relation_kind: None,
            source_summary: None,
            can_accept_direct_input: None,
            agent_version: None,

            resume_overrides: None,
            thread_id: new.thread_id.clone(),
            short_id: new.short_id.clone(),
            last_run_id: Some(new.run_id.clone()),
            updated_at: 101,
            model: None,
            preview: None,
            cwd: None,
            meta: None,
        },
    );
    disk.write_json("threads.json", &serde_json::to_vec(&index).unwrap())
        .unwrap();
    let (store, loaded) = open(temp.config()).await.unwrap();
    assert_eq!(
        loaded.index.threads["short-parent"].last_run_id.as_deref(),
        Some("a-new-b")
    );
    assert_eq!(loaded.runs.len(), 1);
    assert_eq!(loaded.runs[0].run_id, "a-new-b");
    store.drain(Duration::from_secs(2)).await;
    // 指定两种扫描顺序，覆盖旧记录先到与索引记录先到。
    for ids in [["z-old-a", "a-new-b"], ["a-new-b", "z-old-a"]] {
        let mut selection =
            super::load::Selection::new(&index, 0, super::load::MAX_INITIALIZATION_BYTES);
        for id in ids {
            selection
                .scan(
                    &disk.dir.join(run_file(&id.into())),
                    loaded.epoch,
                    &disk.gate,
                )
                .unwrap();
        }
        let mut restored = index.clone();
        assert_eq!(
            selection.finish(&mut restored).unwrap()[0].run_id,
            "a-new-b"
        );
        assert_eq!(restored, index);
    }
}

#[test]
fn 超过初始化预算的过期运行按外壳校验清理() {
    use std::io::Write;
    let temp = Temp::new();
    let mut disk = temp.disk();
    let name = "runs/large.json";
    disk.write_json(name, &serde_json::to_vec(&run("large", "t", 1)).unwrap())
        .unwrap();
    let path = disk.dir.join(name);
    let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
    // JSON 尾随空白仍合法，文件体积超过预算，测试不必分配同样大的输出字符串。
    let padding = vec![b' '; 1024 * 1024];
    for _ in 0..65 {
        file.write_all(&padding).unwrap();
    }
    let now = SystemTime::now();
    file.set_modified(now - Duration::from_millis(RETENTION_MS + 1000))
        .unwrap();
    drop(file);
    assert!(fs::metadata(&path).unwrap().len() > super::load::MAX_INITIALIZATION_BYTES);
    assert_eq!(
        disk::read_envelope(&path).unwrap().unwrap().epoch,
        disk.epoch
    );
    disk.cleanup(now).unwrap();
    assert!(!path.exists());
    assert!(!disk.metadata.contains_key(name));
}

#[cfg(windows)]
#[test]
fn 备份成功但替换失败的重复启动最多留下三份副本() {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::{FILE_SHARE_READ, FILE_SHARE_WRITE};
    let temp = Temp::new();
    let disk = temp.disk();
    let path = disk.dir.join("threads.json");
    let corrupt = b"{\"epoch\":";
    fs::write(&path, corrupt).unwrap();
    // 允许读取和备份，但不允许删除共享，使 MoveFileExW 确实在替换时失败。
    let blocker = fs::OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .open(&path)
        .unwrap();
    for _ in 0..8 {
        assert!(matches!(
            Disk::initialize(
                &mut temp.config(),
                Gate {
                    cancelled: Arc::new(AtomicBool::new(false)),
                    initialization: None,
                }
            ),
            Err(StoreError::Io(_))
        ));
        let copies: Vec<_> = fs::read_dir(temp.dir())
            .unwrap()
            .map(Result::unwrap)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("threads.json.corrupt-")
            })
            .collect();
        assert!(!copies.is_empty());
        assert!(copies.len() <= 3);
        for copy in copies {
            assert_eq!(fs::read(copy.path()).unwrap(), corrupt);
        }
        assert_eq!(fs::read(&path).unwrap(), corrupt);
    }
    drop(blocker);
    let _disk = temp.disk();
    assert_eq!(
        read::<ThreadIndexFile>(&path).payload,
        ThreadIndexFile::default()
    );
    assert_eq!(
        fs::read_dir(temp.dir())
            .unwrap()
            .map(Result::unwrap)
            .filter(|entry| entry
                .file_name()
                .to_string_lossy()
                .starts_with("threads.json.corrupt-"))
            .count(),
        3
    );
}

#[cfg(windows)]
#[test]
fn 删不掉的过期诊断副本不挡住过期运行文件清理() {
    use std::os::windows::fs::OpenOptionsExt;
    let temp = Temp::new();
    let mut disk = temp.disk();
    let now = SystemTime::now();
    let old = now - Duration::from_millis(RETENTION_MS + 1000);
    let backup = disk.dir.join("threads.json.corrupt-1");
    fs::write(&backup, b"{").unwrap();
    fs::File::options()
        .write(true)
        .open(&backup)
        .unwrap()
        .set_modified(old)
        .unwrap();
    fs::create_dir_all(disk.dir.join("runs")).unwrap();
    let run = disk.dir.join("runs").join("run-old.log");
    fs::write(
        &run, b"line
",
    )
    .unwrap();
    fs::File::options()
        .write(true)
        .open(&run)
        .unwrap()
        .set_modified(old)
        .unwrap();
    // 只允许共享读：持有期间别的句柄无法删除这个副本。
    let lock = fs::File::options()
        .read(true)
        .share_mode(1)
        .open(&backup)
        .unwrap();
    disk.cleanup(now).unwrap();
    assert!(backup.exists());
    assert!(!run.exists());
    drop(lock);
}

#[test]
fn 诊断副本同时受三份上限与三十天期限约束() {
    let temp = Temp::new();
    let mut disk = temp.disk();
    let now = SystemTime::now();
    for number in 0..5 {
        let path = disk.dir.join(format!("threads.json.corrupt-{number}"));
        fs::write(&path, b"{").unwrap();
        let age = if number == 0 {
            Duration::from_millis(RETENTION_MS + 1000)
        } else {
            Duration::from_secs(10 - number)
        };
        fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(now - age)
            .unwrap();
    }
    disk.cleanup(now).unwrap();
    for number in 0..5 {
        assert_eq!(
            disk.dir
                .join(format!("threads.json.corrupt-{number}"))
                .exists(),
            number >= 2
        );
    }
    disk.cleanup(now + Duration::from_millis(RETENTION_MS + 1000))
        .unwrap();
    assert!(!fs::read_dir(disk.dir)
        .unwrap()
        .map(Result::unwrap)
        .any(|entry| entry
            .file_name()
            .to_string_lossy()
            .starts_with("threads.json.corrupt-")));
}

#[test]
fn inbox_读出完整快照且损坏时跳过() {
    let temp = Temp::new();
    let disk = temp.disk();
    let item = crate::inbox::InboxItem {
        root_thread_id: None,
        parent_thread_id: None,

        id: "qu_0123456789abcdef".into(),
        kind: crate::inbox::InboxKind::Question,
        thread_id: "t".into(),
        run_id: None,
        turn_id: None,
        method: "未来方法".into(),
        params: serde_json::json!({"原样":true}),
        created_at: 1,
        expires_at: 2,
        status: crate::inbox::InboxStatus::Pending,
    };
    let file = crate::inbox::InboxFile { items: vec![item] };
    put(&temp.dir().join("inbox.json"), disk.epoch, 1, &file);
    drop(disk);
    let (_, loaded) = Disk::initialize(
        &mut temp.config(),
        Gate {
            cancelled: Arc::new(AtomicBool::new(false)),
            initialization: None,
        },
    )
    .unwrap();
    assert_eq!(loaded.inbox, Some(file));
    fs::write(temp.dir().join("inbox.json"), b"{broken").unwrap();
    let (_, loaded) = Disk::initialize(
        &mut temp.config(),
        Gate {
            cancelled: Arc::new(AtomicBool::new(false)),
            initialization: None,
        },
    )
    .unwrap();
    assert!(loaded.inbox.is_none());
}

#[test]
fn 恢复覆盖真实落盘往返及旧索引缺省() {
    let temp = Temp::new();
    let mut disk = temp.disk();
    let mut machine = crate::claim::Machine::new(false);
    let short = machine.runs.ensure_thread(
        "t",
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        1,
    );
    let entry = machine.runs.index.threads.get_mut(&short).unwrap();
    let legacy = serde_json::to_value(&*entry).unwrap();
    assert!(legacy.get("resumeOverrides").is_none());
    assert!(serde_json::from_value::<ThreadIndexEntry>(legacy)
        .unwrap()
        .resume_overrides
        .is_none());
    entry.resume_overrides = Some(serde_json::from_value(serde_json::json!({"config":{"features.default_mode_request_user_input":true},"developerInstructions":"请提问"})).unwrap());
    disk.write_json(
        "threads.json",
        &serde_json::to_vec(&machine.runs.index).unwrap(),
    )
    .unwrap();
    drop(disk);
    let loaded = Disk::initialize(
        &mut temp.config(),
        Gate {
            cancelled: Arc::new(AtomicBool::new(false)),
            initialization: None,
        },
    )
    .unwrap()
    .1;
    assert_eq!(loaded.index, machine.runs.index);
}

#[test]
fn 删除全部历史及审查子线程_部分失败重试幂等_标记阻止重建() {
    use super::purge::{PurgeTicket, PurgeWork};
    let temp = Temp::new();
    let mut disk = temp.disk();
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    for id in ["old", "new"] {
        let mut record = run(id, "t", now);
        record.review_thread_id = Some("child".into());
        record.review_short_id = Some("short-child".into());
        disk.write_json(&run_file(&id.into()), &serde_json::to_vec(&record).unwrap())
            .unwrap();
    }
    disk.append_log("runs/short-t.log", "日志".as_bytes())
        .unwrap();
    fs::create_dir(disk.dir.join("runs/short-child.log")).unwrap();
    fs::write(
        disk.dir.join("runs/short-child.log/held"),
        "占用".as_bytes(),
    )
    .unwrap();
    let mut work = PurgeWork {
        thread: "t".into(),
        plan: PurgePlan::default(),
        index: None,
        ticket: PurgeTicket::default(),
        seeds: vec![],
    };
    work.plan = disk.prepare_purge(&work).unwrap();
    assert_eq!(work.plan.threads, ["t".into(), "child".into()].into());
    assert_eq!(work.plan.files.len(), 6);
    let mut index = ThreadIndexFile::default();
    for thread in &work.plan.threads {
        index.removed.insert(thread.clone(), now);
    }
    index.purges.insert("t".into(), work.plan.clone());
    work.index = Some(index);
    let report = disk.commit_purge(&work).unwrap();
    assert_eq!(report.deleted.len(), 3);
    assert_eq!(report.failed.len(), 1);
    assert_eq!(report.failed[0].path, "runs/short-child.log");
    assert!(!report.failed[0].reason.is_empty());
    fs::remove_dir_all(disk.dir.join("runs/short-child.log")).unwrap();
    let report = disk.commit_purge(&work).unwrap();
    assert_eq!(report.already_deleted.len(), 6);
    assert!(report.failed.is_empty());
    // 留下可重建的旧运行，验证 tombstone 同时挡住父、子线程索引。
    let mut record = run("leftover", "t", now);
    record.review_thread_id = Some("child".into());
    record.review_short_id = Some("short-child".into());
    disk.write_json(
        &run_file(&"leftover".into()),
        &serde_json::to_vec(&record).unwrap(),
    )
    .unwrap();
    drop(disk);
    let loaded = Disk::initialize(
        &mut temp.config(),
        Gate {
            cancelled: Arc::new(AtomicBool::new(false)),
            initialization: None,
        },
    )
    .unwrap()
    .1;
    assert!(loaded.index.threads.is_empty());
    assert_eq!(loaded.index.purges["t"].files.len(), 6);
}

#[test]
fn 删除标记写入失败不删文件且拒绝越界清单() {
    use super::purge::{PurgeTicket, PurgeWork};
    let temp = Temp::new();
    let mut disk = temp.disk();
    disk.write_json(
        &run_file(&"one".into()),
        &serde_json::to_vec(&run("one", "t", 1)).unwrap(),
    )
    .unwrap();
    let mut work = PurgeWork {
        thread: "t".into(),
        plan: PurgePlan {
            threads: ["t".into()].into(),
            files: ["runs/one.json".into()].into(),
            runs: Default::default(),
        },
        index: Some(ThreadIndexFile::default()),
        ticket: PurgeTicket::default(),
        seeds: vec![],
    };
    fs::create_dir(disk.dir.join("threads.json")).unwrap();
    assert!(disk.commit_purge(&work).is_err());
    assert!(disk.dir.join("runs/one.json").exists());
    fs::remove_dir(disk.dir.join("threads.json")).unwrap();
    work.plan.files.insert("runs/../broker.json".into());
    assert!(disk.commit_purge(&work).is_err());
    assert!(disk.dir.join("broker.json").exists());
}

#[test]
fn 删除扫描保留磁盘失联运行的停止确认() {
    use super::purge::{PurgeTicket, PurgeWork};
    let temp = Temp::new();
    let mut disk = temp.disk();
    for (id, outcome) in [
        ("unknown", None),
        ("unconfirmed", Some(StopOutcome::Unconfirmed)),
        ("confirmed", Some(StopOutcome::Confirmed)),
    ] {
        let mut record = run(id, "t", 1);
        record.state = RunState::Lost;
        record.stop_outcome = outcome;
        disk.write_json(&run_file(&id.into()), &serde_json::to_vec(&record).unwrap())
            .unwrap();
    }
    let plan = disk
        .prepare_purge(&PurgeWork {
            thread: "t".into(),
            plan: Default::default(),
            index: None,
            ticket: PurgeTicket::default(),
            seeds: vec![],
        })
        .unwrap();
    assert_eq!(plan.runs["unknown"].stop_outcome, None);
    assert_eq!(
        plan.runs["unconfirmed"].stop_outcome,
        Some(StopOutcome::Unconfirmed)
    );
    assert_eq!(
        plan.runs["confirmed"].stop_outcome,
        Some(StopOutcome::Confirmed)
    );
}
