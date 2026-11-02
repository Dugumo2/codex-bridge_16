use super::*;
use crate::runs::child_history::test_child;

fn seed_runs(disk: &mut Disk) {
    let mut r0 = run("r0", "parent", 1);
    r0.children = vec![
        test_child("child-a"),
        test_child("child-b"),
        test_child("child-c"),
    ];
    for record in [r0, run("r1", "parent", 2)] {
        disk.write_json(
            &run_file(&record.run_id),
            &serde_json::to_vec(&record).unwrap(),
        )
        .unwrap();
    }
}

#[tokio::test]
async fn history_production_load_recovers_r0_without_old_operations_or_bindings() {
    let temp = Temp::new();
    let mut disk = temp.disk();
    seed_runs(&mut disk);
    let (store, loaded) = open(temp.config()).await.unwrap();
    assert_eq!(loaded.runs.len(), 1);
    assert_eq!(loaded.runs[0].run_id, "r1");
    assert!(loaded.runs[0].children.is_empty());
    for thread in ["child-a", "child-b", "child-c"] {
        assert!(loaded.index.child_history.has_other_run(thread, "r2"));
    }
    assert_eq!(
        loaded.index.child_history.coverage_reason(),
        "historyMissing"
    );
    store.submit(StoreOp::PutThreads(loaded.index));
    assert!(store
        .drain(Duration::from_secs(2))
        .await
        .abandoned
        .is_empty());
    let (store, loaded) = open(temp.config()).await.unwrap();
    assert!(loaded.index.child_history.has_other_run("child-a", "r2"));
    assert!(store
        .drain(Duration::from_secs(2))
        .await
        .abandoned
        .is_empty());
}

#[tokio::test]
async fn history_corrupt_index_rebuild_is_positive_evidence_only() {
    let temp = Temp::new();
    let mut disk = temp.disk();
    seed_runs(&mut disk);
    fs::write(temp.dir().join("threads.json"), b"{").unwrap();
    let (store, loaded) = open(temp.config()).await.unwrap();
    assert!(loaded.index.child_history.has_other_run("child-a", "r2"));
    assert!(!loaded.index.child_history.has_other_run("unseen", "r2"));
    assert_eq!(
        loaded.index.child_history.coverage_reason(),
        "historyCorrupt"
    );
    assert!(store
        .drain(Duration::from_secs(2))
        .await
        .abandoned
        .is_empty());
}

#[tokio::test]
async fn history_corrupt_run_and_legacy_children_remain_unverified() {
    for corrupt in [true, false] {
        let temp = Temp::new();
        let mut disk = temp.disk();
        let mut index = ThreadIndexFile::default();
        index.child_history.verify_empty_store();
        disk.write_json("threads.json", &serde_json::to_vec(&index).unwrap())
            .unwrap();
        if corrupt {
            fs::write(temp.dir().join("runs/broken.json"), b"{").unwrap();
        } else {
            let mut old = serde_json::to_value(run("r0", "parent", 1)).unwrap();
            old.as_object_mut().unwrap().remove("children");
            disk.write_json("runs/r0.json", &serde_json::to_vec(&old).unwrap())
                .unwrap();
        }
        let (store, loaded) = open(temp.config()).await.unwrap();
        assert_eq!(
            loaded.index.child_history.coverage_reason(),
            if corrupt {
                "historyCorrupt"
            } else {
                "historyMissing"
            }
        );
        assert!(!loaded.index.child_history.has_other_run("unseen", "r2"));
        assert!(store
            .drain(Duration::from_secs(2))
            .await
            .abandoned
            .is_empty());
    }
}

#[tokio::test]
async fn history_deleted_children_are_not_rebuilt_from_retained_run_files() {
    let temp = Temp::new();
    let mut disk = temp.disk();
    seed_runs(&mut disk);
    let mut index = ThreadIndexFile::default();
    index.removed.insert("child-b".into(), 3);
    disk.write_json("threads.json", &serde_json::to_vec(&index).unwrap())
        .unwrap();
    let (store, loaded) = open(temp.config()).await.unwrap();
    assert!(loaded.index.child_history.has_other_run("child-a", "r2"));
    assert!(!loaded.index.child_history.has_other_run("child-b", "r2"));
    assert!(loaded.index.child_history.has_other_run("child-c", "r2"));
    assert!(store
        .drain(Duration::from_secs(2))
        .await
        .abandoned
        .is_empty());
}

#[tokio::test]
async fn history_verified_empty_store_does_not_turn_missing_index_into_empty_history() {
    let temp = Temp::new();
    let (store, loaded) = open(temp.config()).await.unwrap();
    assert_eq!(
        loaded.index.child_history.coverage_reason(),
        "historyVerified"
    );
    assert!(store
        .drain(Duration::from_secs(2))
        .await
        .abandoned
        .is_empty());
    let (store, loaded) = open(temp.config()).await.unwrap();
    assert_eq!(
        loaded.index.child_history.coverage_reason(),
        "historyMissing"
    );
    assert!(store
        .drain(Duration::from_secs(2))
        .await
        .abandoned
        .is_empty());
}

#[tokio::test]
async fn history_downgrade_writeback_restores_missing_evidence_conservatively() {
    let temp = Temp::new();
    let mut disk = temp.disk();
    let mut index = ThreadIndexFile::default();
    index.child_history.verify_empty_store();
    index.child_history.remember(
        "retired-run",
        "parent",
        &[test_child("retired-child")],
        10,
        &index.removed,
    );
    index.removed.insert("deleted-child".into(), 10);
    disk.write_json("threads.json", &serde_json::to_vec(&index).unwrap())
        .unwrap();
    // 完整旧运行已被清理，只剩摘要；旧版读取不认识的字段后全量写回会丢失它。
    let mut legacy = read::<serde_json::Value>(&temp.dir().join("threads.json")).payload;
    assert!(legacy
        .as_object_mut()
        .unwrap()
        .remove("childHistory")
        .is_some());
    disk.write_json("threads.json", &serde_json::to_vec(&legacy).unwrap())
        .unwrap();
    drop(disk);

    let (store, loaded) = open(temp.config()).await.unwrap();
    assert_eq!(
        loaded.index.child_history.coverage_reason(),
        "historyMissing"
    );
    assert_eq!(loaded.index.removed.get("deleted-child"), Some(&10));
    assert!(!loaded
        .index
        .child_history
        .has_other_run("retired-child", "new-run"));
    let mut restarted = crate::claim::Machine::new(false);
    restarted.clock.now_ms = 11;
    restarted.restore_runs(loaded.index, loaded.runs, &mut Vec::new());
    assert_eq!(
        restarted.classify_candidate("new-run", "retired-child"),
        crate::relations::Membership::Pending("historyMissing".into()),
        "降级丢失证据后必须待核实，不能把候选排除或升级为本次活动"
    );
    assert!(!restarted.current_management_evidence("new-run", "retired-child"));
    assert!(store
        .drain(Duration::from_secs(2))
        .await
        .abandoned
        .is_empty());
}

/// 有界手动基准；输出逻辑写盘字节，不把文件系统元数据或设备写放大算作有效载荷。
#[tokio::test]
#[ignore = "满载性能记录：显式运行并保留 --nocapture 输出"]
async fn history_full_capacity_cost_record() {
    use crate::runs::child_history::MAX_CHILD_HISTORY;
    const ID_BYTES: usize = 1024;
    let id =
        |prefix: char, number: usize| format!("{prefix}{number:010}{}", "x".repeat(ID_BYTES - 11));
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let temp = Temp::new();
    let mut disk = temp.disk();
    let mut index = ThreadIndexFile::default();
    index.child_history.verify_empty_store();
    for number in 0..MAX_CHILD_HISTORY {
        index.child_history.remember(
            &id('r', number),
            &id('p', number),
            &[test_child(&id('c', number))],
            now,
            &index.removed,
        );
    }
    let encoded = serde_json::to_vec(&index).unwrap();
    disk.write_json("threads.json", &encoded).unwrap();
    let path = temp.dir().join("threads.json");
    let bytes = fs::read(&path).unwrap();
    let parse_started = Instant::now();
    let parsed: Envelope<ThreadIndexFile> = serde_json::from_slice(&bytes).unwrap();
    let parse_ms = parse_started.elapsed().as_secs_f64() * 1000.0;
    let initial_revision = parsed.revision;
    assert_eq!(parsed.payload, index);
    let entry_count: usize = serde_json::to_value(&parsed.payload).unwrap()["childHistory"]
        ["entries"]
        .as_object()
        .unwrap()
        .values()
        .map(|runs| runs.as_object().unwrap().len())
        .sum();
    assert_eq!(entry_count, MAX_CHILD_HISTORY);
    drop(parsed);
    drop(disk);

    let empty = Temp::new();
    let mut empty_disk = empty.disk();
    let mut empty_index = ThreadIndexFile::default();
    empty_index.child_history.verify_empty_store();
    empty_disk
        .write_json("threads.json", &serde_json::to_vec(&empty_index).unwrap())
        .unwrap();
    drop(empty_disk);
    let startup_empty_started = Instant::now();
    let (empty_store, empty_loaded) = open(empty.config()).await.unwrap();
    let mut empty_machine = crate::claim::Machine::new(false);
    empty_machine.clock.now_ms = now;
    empty_machine.restore_runs(empty_loaded.index, empty_loaded.runs, &mut Vec::new());
    let empty_startup_ms = startup_empty_started.elapsed().as_secs_f64() * 1000.0;
    assert!(empty_store
        .drain(Duration::from_secs(10))
        .await
        .abandoned
        .is_empty());

    let startup_started = Instant::now();
    let (store, loaded) = open(temp.config()).await.unwrap();
    let mut machine = crate::claim::Machine::new(false);
    machine.clock.now_ms = now;
    machine.restore_runs(loaded.index, loaded.runs, &mut Vec::new());
    let startup_ms = startup_started.elapsed().as_secs_f64() * 1000.0;
    assert!(machine
        .runs
        .index
        .child_history
        .has_other_run(&id('c', 0), "new-run"));

    // 一项已确认归属更新 observedAt，走实际全量 PutThreads 写盘路径。
    let mut changed = machine.runs.index.clone();
    assert!(changed.child_history.remember(
        &id('r', 0),
        &id('p', 0),
        &[test_child(&id('c', 0))],
        now + 1,
        &changed.removed,
    ));
    changed.version += 1;
    let write_started = Instant::now();
    store.submit(StoreOp::PutThreads(changed.clone()));
    assert!(store
        .drain(Duration::from_secs(10))
        .await
        .abandoned
        .is_empty());
    let write_ms = write_started.elapsed().as_secs_f64() * 1000.0;
    let changed_bytes = fs::read(&path).unwrap();
    let written: Envelope<ThreadIndexFile> = serde_json::from_slice(&changed_bytes).unwrap();
    assert_eq!(written.payload, changed);
    assert!(written.revision > initial_revision);
    println!(
        "CHILD_HISTORY_COST {}",
        serde_json::json!({
            "entries": entry_count, "identifierBytes": ID_BYTES,
            "threadsJsonBytes": bytes.len(), "parseMs": parse_ms,
            "emptyStoreAndMachineStartupMs": empty_startup_ms,
            "fullStoreAndMachineStartupMs": startup_ms,
            "startupDeltaMs": startup_ms - empty_startup_ms,
            "singleStateChangeWriteBytes": changed_bytes.len(), "writeAndDrainMs": write_ms,
            "cacheMode": "写入后测量，未清除操作系统文件缓存；单次有界样本",
            "startupScope": "store::open + Machine::new + restore_runs；不含进程创建、上游握手或恢复动作写盘"
        })
    );
}
