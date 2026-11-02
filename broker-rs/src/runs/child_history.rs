//! 仅保存已经确认的运行归属；历史事实不携带绑定、操作或管理权限。

use crate::relations::ChildRecord;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const MAX_CHILD_HISTORY: usize = 4096;
// 将完整 childHistory 的紧凑 JSON 限为 512 KiB，限制每次 threads.json 全量写的放大。
pub const MAX_CHILD_HISTORY_BYTES: usize = 512 * 1024;
const MAX_ID_BYTES: usize = 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum HistoryCoverage {
    #[default]
    Missing,
    Corrupt,
    Trimmed,
    Verified,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Attribution {
    root_thread_id: String,
    observed_at: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChildHistory {
    coverage: HistoryCoverage,
    retained_after: u64,
    entries: BTreeMap<String, BTreeMap<String, Attribution>>,
}

impl<'de> Deserialize<'de> for ChildHistory {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Wire {
            coverage: HistoryCoverage,
            #[serde(default)]
            retained_after: u64,
            entries: BTreeMap<String, BTreeMap<String, Attribution>>,
        }
        // 局部损坏不能连带丢掉 threads.json 中的删除标记。
        let value = serde_json::Value::deserialize(deserializer)?;
        let Ok(wire) = serde_json::from_value::<Wire>(value) else {
            return Ok(Self {
                coverage: HistoryCoverage::Corrupt,
                ..Self::default()
            });
        };
        let mut history = Self {
            coverage: wire.coverage,
            retained_after: wire.retained_after,
            entries: wire.entries,
        };
        let mut invalid = false;
        history.entries.retain(|thread, runs| {
            if !valid_id(thread) {
                invalid = true;
                return false;
            }
            runs.retain(|run, fact| {
                let valid = valid_id(run) && valid_id(&fact.root_thread_id);
                invalid |= !valid;
                valid
            });
            !runs.is_empty()
        });
        if invalid {
            history.mark_corrupt();
        }
        history.enforce_limit();
        Ok(history)
    }
}

fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= MAX_ID_BYTES
}

impl ChildHistory {
    pub(crate) fn verify_empty_store(&mut self) {
        if self.coverage == HistoryCoverage::Missing && self.entries.is_empty() {
            self.coverage = HistoryCoverage::Verified;
        }
    }

    pub fn has_other_run(&self, thread: &str, run: &str) -> bool {
        self.entries
            .get(thread)
            .is_some_and(|runs| runs.keys().any(|id| id != run))
    }

    pub fn coverage_reason(&self) -> &'static str {
        match self.coverage {
            HistoryCoverage::Missing => "historyMissing",
            HistoryCoverage::Corrupt => "historyCorrupt",
            HistoryCoverage::Trimmed => "historyTrimmed",
            HistoryCoverage::Verified => "historyVerified",
        }
    }

    pub(crate) fn mark_corrupt(&mut self) {
        self.coverage = HistoryCoverage::Corrupt;
    }

    pub(crate) fn mark_missing(&mut self) {
        if self.coverage == HistoryCoverage::Verified {
            self.coverage = HistoryCoverage::Missing;
        }
    }

    pub(crate) fn mark_trimmed(&mut self) {
        if self.coverage != HistoryCoverage::Corrupt {
            self.coverage = HistoryCoverage::Trimmed;
        }
    }

    pub(crate) fn remember(
        &mut self,
        run: &str,
        root: &str,
        children: &[ChildRecord],
        observed_at: u64,
        removed: &BTreeMap<String, u64>,
    ) -> bool {
        if removed.contains_key(root) {
            return false;
        }
        if observed_at < self.retained_after {
            if !children.is_empty() {
                self.mark_trimmed();
            }
            return false;
        }
        if !valid_id(run) || !valid_id(root) {
            self.mark_trimmed();
            return true;
        }
        let mut changed = false;
        for child in children {
            if removed.contains_key(&child.thread_id) {
                continue;
            }
            if !valid_id(&child.thread_id) {
                self.mark_trimmed();
                changed = true;
                continue;
            }
            let runs = self.entries.entry(child.thread_id.clone()).or_default();
            let fact = Attribution {
                root_thread_id: root.into(),
                observed_at: runs
                    .get(run)
                    .map_or(observed_at, |old| old.observed_at.max(observed_at)),
            };
            if runs.get(run) != Some(&fact) {
                runs.insert(run.into(), fact);
                changed = true;
            }
        }
        self.enforce_limit();
        changed
    }

    fn enforce_limit(&mut self) -> bool {
        let mut count: usize = self.entries.values().map(BTreeMap::len).sum();
        if count <= MAX_CHILD_HISTORY && json_bytes(self) <= MAX_CHILD_HISTORY_BYTES {
            return false;
        }
        self.mark_trimmed();
        let mut bytes = json_bytes(self);
        let mut facts: Vec<_> = self
            .entries
            .iter()
            .flat_map(|(thread, runs)| {
                runs.iter()
                    .map(move |(run, fact)| (fact.observed_at, thread.clone(), run.clone()))
            })
            .collect();
        facts.sort();
        // observed_at 同时用于 30 天保留期：先裁最早观察的事实；同时间按线程、运行标识排序。
        for (_, thread, run) in facts {
            if count <= MAX_CHILD_HISTORY && bytes <= MAX_CHILD_HISTORY_BYTES {
                break;
            }
            let thread_count = self.entries.len();
            let runs = self.entries.get_mut(&thread).unwrap();
            let fact = runs.remove(&run).unwrap();
            // 按实际 JSON 编码扣除键、值、冒号、逗号和空对象外壳，包含标识转义的开销。
            bytes -= json_bytes(&run) + 1 + json_bytes(&fact);
            if runs.is_empty() {
                bytes -= json_bytes(&thread) + 3 + usize::from(thread_count > 1);
                self.entries.remove(&thread);
            } else {
                bytes -= 1;
            }
            count -= 1;
        }
        true
    }

    /// 删除与过期都是真正的证据缺口，不能在后续扫描中升级为「没有历史」。
    pub(crate) fn prune(&mut self, now: u64, removed: &BTreeMap<String, u64>) -> bool {
        self.retained_after = self
            .retained_after
            .max(now.saturating_sub(crate::store::RETENTION_MS));
        let mut changed = false;
        self.entries.retain(|thread, runs| {
            runs.retain(|_, fact| {
                let keep = !removed.contains_key(thread)
                    && !removed.contains_key(&fact.root_thread_id)
                    && now.saturating_sub(fact.observed_at) <= crate::store::RETENTION_MS;
                changed |= !keep;
                keep
            });
            !runs.is_empty()
        });
        if changed {
            self.mark_trimmed();
        }
        // retained_after 的十进制位数也占预算；水位增长后再次检查。
        self.enforce_limit() || changed
    }
}

fn json_bytes(value: &impl Serialize) -> usize {
    serde_json::to_vec(value).expect("历史字段可序列化").len()
}

#[cfg(test)]
pub(crate) fn test_budget_children() -> Vec<ChildRecord> {
    // 控制字符与多字节字符使序列化大小不同于原始标识长度，且单标识仍小于 1024 字节。
    (0..600)
        .map(|number| test_child(&format!("预算{number:04}{}", "\n\"\\中".repeat(100))))
        .collect()
}

#[cfg(test)]
pub(crate) fn test_child(thread: &str) -> ChildRecord {
    ChildRecord {
        thread_id: thread.into(),
        parent_thread_id: "parent".into(),
        relation_kind: crate::relations::RelationKind::Subagent,
        turn_ids: vec![format!("{thread}-turn")],
        evidence: crate::relations::BindingEvidence::V1Spawn,
        final_state: crate::relations::MemberActivity::Settled,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runs::ThreadIndexFile;

    #[test]
    fn history_byte_budget_handles_shared_threads_and_watermark_growth() {
        let mut history = ChildHistory::default();
        history.verify_empty_store();
        let children = test_budget_children();
        for number in 0..3 {
            history.remember(
                &format!("运行{number}\n\""),
                &"根".repeat(300),
                &children,
                1000 + number,
                &BTreeMap::new(),
            );
            assert!(json_bytes(&history) <= MAX_CHILD_HISTORY_BYTES);
        }
        assert_eq!(history.coverage_reason(), "historyTrimmed");
        // 不过期任何记录，只增长水位编码；预算必须仍成立。
        history.prune(crate::store::RETENTION_MS + 1000, &BTreeMap::new());
        assert!(json_bytes(&history) <= MAX_CHILD_HISTORY_BYTES);
        let restored: ChildHistory =
            serde_json::from_slice(&serde_json::to_vec(&history).unwrap()).unwrap();
        assert_eq!(restored, history);
    }

    #[test]
    fn history_old_new_and_corrupt_formats_keep_explicit_coverage() {
        let old: ThreadIndexFile = serde_json::from_value(serde_json::json!({
            "version":0,"threads":{},"removed":{"deleted":3}
        }))
        .unwrap();
        assert_eq!(old.child_history.coverage_reason(), "historyMissing");
        assert!(!old.child_history.has_other_run("child", "r2"));
        let mut index = old;
        index
            .child_history
            .remember("r1", "parent", &[test_child("child")], 1, &index.removed);
        let value = serde_json::to_value(&index).unwrap();
        let restored: ThreadIndexFile = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(restored, index);
        assert!(restored.child_history.has_other_run("child", "r2"));
        assert!(!restored.child_history.has_other_run("child", "r1"));
        let mut corrupt = value;
        corrupt["childHistory"] = serde_json::json!({"entries":"broken"});
        let restored: ThreadIndexFile = serde_json::from_value(corrupt).unwrap();
        assert_eq!(restored.child_history.coverage_reason(), "historyCorrupt");
        assert_eq!(restored.removed.get("deleted"), Some(&3));
    }

    #[test]
    fn history_capacity_keeps_newest_and_marks_gap() {
        let mut history = ChildHistory::default();
        history.verify_empty_store();
        assert_eq!(history.coverage_reason(), "historyVerified");
        for number in 0..=MAX_CHILD_HISTORY {
            history.remember(
                &format!("run{number}"),
                "parent",
                &[test_child("child")],
                number as u64,
                &BTreeMap::new(),
            );
        }
        assert_eq!(history.entries["child"].len(), MAX_CHILD_HISTORY);
        assert!(!history.entries["child"].contains_key("run0"));
        assert!(history.entries["child"].contains_key(&format!("run{MAX_CHILD_HISTORY}")));
        assert_eq!(history.coverage_reason(), "historyTrimmed");
        let restored: ChildHistory =
            serde_json::from_slice(&serde_json::to_vec(&history).unwrap()).unwrap();
        assert_eq!(restored, history);
    }

    #[test]
    fn history_expiry_and_deletion_cannot_resurrect_from_old_records() {
        let mut history = ChildHistory::default();
        let child = test_child("child");
        history.remember(
            "r0",
            "parent",
            std::slice::from_ref(&child),
            5,
            &BTreeMap::new(),
        );
        history.prune(crate::store::RETENTION_MS + 6, &BTreeMap::new());
        history.remember(
            "r0",
            "parent",
            std::slice::from_ref(&child),
            5,
            &BTreeMap::new(),
        );
        assert!(!history.has_other_run("child", "r2"));
        assert_eq!(history.coverage_reason(), "historyTrimmed");
        history.remember(
            "r1",
            "parent",
            std::slice::from_ref(&child),
            10,
            &BTreeMap::new(),
        );
        assert!(history.has_other_run("child", "r2"));
        let removed = BTreeMap::from([("child".into(), 11)]);
        history.prune(0, &removed);
        history.remember("r1", "parent", &[child], 10, &removed);
        assert!(!history.has_other_run("child", "r2"));
    }

    #[test]
    fn history_oversized_identifiers_are_not_accepted_as_facts() {
        let mut history = ChildHistory::default();
        history.remember(
            "r1",
            "parent",
            &[test_child(&"x".repeat(MAX_ID_BYTES + 1))],
            0,
            &BTreeMap::new(),
        );
        assert!(history.entries.is_empty());
        assert_eq!(history.coverage_reason(), "historyTrimmed");
    }

    #[test]
    fn history_oversized_serialized_index_prunes_old_facts() {
        let mut unbounded = ChildHistory::default();
        unbounded.verify_empty_store();
        let runs = unbounded.entries.entry("child".into()).or_default();
        for number in 0..MAX_CHILD_HISTORY + 2 {
            runs.insert(
                format!("r{number}"),
                Attribution {
                    root_thread_id: "parent".into(),
                    observed_at: number as u64,
                },
            );
        }
        let restored: ChildHistory =
            serde_json::from_slice(&serde_json::to_vec(&unbounded).unwrap()).unwrap();
        assert_eq!(restored.entries["child"].len(), MAX_CHILD_HISTORY);
        assert!(!restored.entries["child"].contains_key("r0"));
        assert!(!restored.entries["child"].contains_key("r1"));
        assert_eq!(restored.coverage_reason(), "historyTrimmed");
    }
}
