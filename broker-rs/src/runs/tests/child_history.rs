use super::*;

#[test]
fn history_children_are_persisted_before_full_run_and_operation_eviction() {
    let mut m = machine();
    open(&mut m, "r1", "parent");
    let mut actions = Vec::new();
    m.run_set_children(
        "r1",
        vec![crate::runs::child_history::test_child("old-child")],
        &mut actions,
    );
    assert!(actions.iter().any(|action| matches!(action,
        Action::Store(StoreOp::PutThreads(index))
        if index.child_history.has_other_run("old-child", "r2")
    )));
    m.run_released("r1", ReleaseKind::Natural, &mut Vec::new());
    m.flush_run_notices(&mut Vec::new());
    m.clock.now_ms += 1;
    open(&mut m, "r2", "parent");
    assert!(!m.runs.runs.contains_key("r1"));
    assert!(m.relations.operations.is_empty());
    assert!(m.runs.index.child_history.has_other_run("old-child", "r2"));
    assert!(m.runs.runs["r2"].children.is_empty());
}

#[test]
fn history_evict_preserves_legacy_record_before_removing_it() {
    let mut m = machine();
    open(&mut m, "r0", "parent");
    m.runs.runs.get_mut("r0").unwrap().children =
        vec![crate::runs::child_history::test_child("old-child")];
    m.run_released("r0", ReleaseKind::Natural, &mut Vec::new());
    m.flush_run_notices(&mut Vec::new());
    m.clock.now_ms += 1;
    open(&mut m, "r1", "parent");
    assert!(!m.runs.runs.contains_key("r0"));
    assert!(m.runs.index.child_history.has_other_run("old-child", "r1"));
}
