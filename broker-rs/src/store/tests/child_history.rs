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
