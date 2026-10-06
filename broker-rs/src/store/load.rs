//! 按线程增量保留候选，避免先把全部历史运行的输出读进内存。

use super::disk::{read_envelope_limited, Gate};
use super::*;
use crate::runs::types::{Ready, RunState};
use serde::de::{SeqAccess, Visitor};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// 初始化中索引和保留运行的源文件字节总预算；单次读取也不得超过它。
pub(super) const MAX_INITIALIZATION_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Summary {
    run_id: String,
    thread_id: String,
    started_at: u64,
    state: RunState,
    review_thread_id: Option<String>,
    review_short_id: Option<String>,
    #[serde(rename = "turns", deserialize_with = "pending_turn")]
    pending: bool,
}

fn pending_turn<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    struct PendingVisitor;
    impl<'de> Visitor<'de> for PendingVisitor {
        type Value = bool;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("运行轮次数组")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<bool, A::Error> {
            #[derive(Deserialize)]
            struct Turn {
                ready: Ready,
            }
            let mut pending = false;
            while let Some(turn) = sequence.next_element::<Turn>()? {
                pending |= turn.ready == Ready::Pending;
            }
            Ok(pending)
        }
    }
    deserializer.deserialize_seq(PendingVisitor)
}

impl Summary {
    fn threads(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.thread_id.as_str()).chain(
            self.review_thread_id
                .as_deref()
                .filter(|_| self.review_short_id.is_some()),
        )
    }
    fn protected(&self) -> bool {
        self.pending
            || matches!(
                self.state,
                RunState::Running | RunState::Stopping | RunState::Lost
            )
    }
}

struct Retained {
    summary: Summary,
    // 腾出解析预算时只释放 payload；来源与选择关系保留到候选验证成功。
    record: Option<RunRecord>,
    path: PathBuf,
    epoch: u64,
    revision: u64,
    bytes: u64,
}

pub(super) struct Selection {
    records: BTreeMap<String, Retained>,
    latest: BTreeMap<String, String>,
    indexed: BTreeMap<String, String>,
    referenced: BTreeSet<String>,
    pub indexed_starts: BTreeMap<String, u64>,
    limit: u64,
    bytes: u64,
    #[cfg(test)]
    pub peak_bytes: u64,
}

impl Selection {
    pub fn new(index: &ThreadIndexFile, index_bytes: u64, limit: u64) -> Self {
        let indexed: BTreeMap<_, _> = index
            .threads
            .values()
            .filter_map(|entry| {
                entry
                    .last_run_id
                    .as_ref()
                    .map(|id| (entry.thread_id.clone(), id.clone()))
            })
            .collect();
        Self {
            referenced: indexed.values().cloned().collect(),
            indexed,
            indexed_starts: BTreeMap::new(),
            records: BTreeMap::new(),
            latest: BTreeMap::new(),
            limit,
            bytes: index_bytes,
            #[cfg(test)]
            peak_bytes: index_bytes,
        }
    }

    fn prefers(&self, thread: &str, candidate: &Summary) -> bool {
        let Some(current) = self.latest.get(thread).and_then(|id| self.records.get(id)) else {
            return true;
        };
        candidate.started_at > current.summary.started_at
            || (candidate.started_at == current.summary.started_at
                && self.indexed.get(thread) == Some(&candidate.run_id))
    }

    fn remove(&mut self, id: &str) {
        if let Some(record) = self.records.remove(id) {
            if record.record.is_some() {
                self.bytes -= record.bytes;
            }
        }
        self.latest.retain(|_, value| value != id);
    }

    fn restore(&mut self, suspended: &[String], gate: &Gate) -> Result<(), StoreError> {
        for id in suspended {
            gate.check()?;
            let retained = self.records.get_mut(id).unwrap();
            let restored = read_envelope_limited::<RunRecord>(&retained.path, retained.bytes)?
                .ok_or_else(|| std::io::Error::other("恢复旧候选失败：源文件已不存在"))?;
            gate.check()?;
            if restored.epoch != retained.epoch
                || restored.revision != retained.revision
                || restored.payload.run_id != retained.summary.run_id
                || restored.payload.started_at != retained.summary.started_at
            {
                return Err(
                    std::io::Error::other("恢复旧候选失败：源文件在扫描期间发生变化").into(),
                );
            }
            retained.record = Some(restored.payload);
            self.bytes += retained.bytes;
            #[cfg(test)]
            {
                self.peak_bytes = self.peak_bytes.max(self.bytes);
            }
        }
        Ok(())
    }

    pub fn complete(&self) -> bool {
        self.records.values().all(|record| record.record.is_some())
    }

    pub fn scan(&mut self, path: &Path, epoch: u64, gate: &Gate) -> Result<(), StoreError> {
        gate.check()?;
        let bytes = std::fs::metadata(path)?.len();
        if bytes > self.limit {
            tracing::warn!(file = %path.display(), bytes, budget = self.limit, "运行文件超过初始化预算，跳过但保留磁盘文件");
            return Ok(());
        }
        let Some(envelope) = read_envelope_limited::<Summary>(path, self.limit)? else {
            return Ok(());
        };
        gate.check()?;
        if envelope.epoch > epoch {
            return Err(StoreError::Superseded {
                file: path.display().to_string(),
            });
        }
        let summary = envelope.payload;
        if self.referenced.contains(&summary.run_id) {
            self.indexed_starts
                .insert(summary.run_id.clone(), summary.started_at);
        }
        if !summary.protected()
            && !summary
                .threads()
                .any(|thread| self.prefers(thread, &summary))
        {
            return Ok(());
        }
        // 先规划完整淘汰集合；预算仍不足时不能先丢掉其中一部分。
        let mut suspended: Vec<String> = Vec::new();
        let mut available_bytes = self.bytes;
        while available_bytes.saturating_add(bytes) > self.limit {
            let oldest = self
                .records
                .iter()
                .filter(|(id, retained)| {
                    !suspended.contains(*id)
                        && (retained.summary.started_at < summary.started_at
                            || (retained.summary.started_at == summary.started_at
                                && summary.threads().any(|thread| {
                                    self.indexed.get(thread) == Some(&summary.run_id)
                                        && self
                                            .latest
                                            .get(thread)
                                            .is_some_and(|id| id == &retained.summary.run_id)
                                })))
                })
                .min_by_key(|(_, record)| record.summary.started_at)
                .map(|(id, _)| id.clone());
            let Some(id) = oldest else {
                tracing::warn!(file = %path.display(), budget = self.limit, "初始化总字节预算不足，跳过运行文件但不删除");
                return Ok(());
            };
            available_bytes -= self.records[&id].bytes;
            suspended.push(id);
        }
        // 只释放旧 payload，不删除元数据和 latest 指针；失败时逐份按原版本恢复。
        for id in &suspended {
            let retained = self.records.get_mut(id).unwrap();
            retained.record = None;
            self.bytes -= retained.bytes;
        }
        #[cfg(test)]
        {
            self.peak_bytes = self.peak_bytes.max(self.bytes + bytes);
        }
        let parsed = (|| {
            let envelope = read_envelope_limited::<RunRecord>(path, self.limit - self.bytes)?
                .ok_or_else(|| std::io::Error::other("扫描期间运行文件已不存在"))?;
            gate.check()?;
            if envelope.epoch > epoch {
                return Err(StoreError::Superseded {
                    file: path.display().to_string(),
                });
            }
            // 两次读取之间若外部改动文件，不把摘要与另一个运行的内容拼接。
            if envelope.payload.run_id != summary.run_id
                || envelope.payload.started_at != summary.started_at
            {
                return Err(std::io::Error::other("扫描期间运行记录发生变化").into());
            }
            Ok(envelope)
        })();
        let envelope = match parsed {
            Ok(envelope) => envelope,
            Err(error) => {
                self.restore(&suspended, gate)?;
                return Err(error);
            }
        };
        for id in suspended {
            tracing::warn!(file = %run_file(&id), budget = self.limit, "初始化总字节预算不足，移出旧记录但不删除文件");
            self.remove(&id);
        }
        for thread in summary.threads() {
            if self.prefers(thread, &summary) {
                self.latest
                    .insert(thread.to_owned(), summary.run_id.clone());
            }
        }
        let id = summary.run_id.clone();
        if let Some(previous) = self.records.insert(
            id,
            Retained {
                summary,
                record: Some(envelope.payload),
                path: path.to_owned(),
                epoch: envelope.epoch,
                revision: envelope.revision,
                bytes,
            },
        ) {
            self.bytes -= previous.bytes;
        }
        self.bytes += bytes;
        let obsolete: Vec<_> = self
            .records
            .iter()
            .filter(|(id, record)| {
                !record.summary.protected() && !self.latest.values().any(|latest| latest == *id)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in obsolete {
            self.remove(&id);
        }
        Ok(())
    }

    pub fn finish(self, index: &mut ThreadIndexFile) -> Result<Vec<RunRecord>, StoreError> {
        if !self.complete() {
            return Err(std::io::Error::other("旧候选恢复未完成，放弃本次初始化").into());
        }
        for (thread, id) in &self.latest {
            let run = self.records[id].record.as_ref().unwrap();
            super::disk::rebuild_entry(index, thread, run, &self.indexed_starts);
        }
        Ok(self
            .records
            .into_values()
            .map(|record| record.record.unwrap())
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::tests::{run, Temp};

    #[test]
    fn 同线程历史增量筛选不累积全部输出() {
        let temp = Temp::new();
        let mut disk = temp.disk();
        let mut selected = Selection::new(&ThreadIndexFile::default(), 0, 256 * 1024);
        for number in 0..24 {
            let mut record = run(&format!("run{number}"), "same", number);
            record.input = "甲".repeat(20000);
            disk.write_json(
                &run_file(&record.run_id),
                &serde_json::to_vec(&record).unwrap(),
            )
            .unwrap();
            selected
                .scan(
                    &disk.dir.join(run_file(&record.run_id)),
                    disk.epoch,
                    &disk.gate,
                )
                .unwrap();
            assert_eq!(selected.records.len(), 1);
        }
        assert!(selected.peak_bytes < 128 * 1024);
        let loaded = selected.finish(&mut ThreadIndexFile::default()).unwrap();
        assert_eq!(loaded[0].run_id, "run23");
        assert_eq!(
            std::fs::read_dir(disk.dir.join("runs")).unwrap().count(),
            24
        );
    }

    #[test]
    fn 初始化总预算跳过旧记录与超大文件且不删盘() {
        let temp = Temp::new();
        let mut disk = temp.disk();
        let mut selection = Selection::new(&ThreadIndexFile::default(), 0, 100 * 1024);
        for number in 0..4 {
            let mut record = run(&format!("r{number}"), &format!("t{number}"), number);
            record.state = RunState::Lost;
            record.input = "甲".repeat(if number == 3 { 50000 } else { 20000 });
            disk.write_json(
                &run_file(&record.run_id),
                &serde_json::to_vec(&record).unwrap(),
            )
            .unwrap();
            selection
                .scan(
                    &disk.dir.join(run_file(&record.run_id)),
                    disk.epoch,
                    &disk.gate,
                )
                .unwrap();
        }
        assert!(selection.peak_bytes <= 100 * 1024);
        let records = selection.finish(&mut ThreadIndexFile::default()).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].run_id, "r2");
        assert_eq!(std::fs::read_dir(disk.dir.join("runs")).unwrap().count(), 4);
    }

    #[test]
    fn 摘要合法但完整候选非法时恢复预算内旧选择() {
        let temp = Temp::new();
        let mut disk = temp.disk();
        let limit = 100 * 1024;
        let mut selection = Selection::new(&ThreadIndexFile::default(), 0, limit);
        let mut original = Vec::new();
        for number in 0..2 {
            let mut record = run(&format!("old{number}"), &format!("t{number}"), number);
            record.state = RunState::Lost;
            record.input = "甲".repeat(10000);
            let path = disk.dir.join(run_file(&record.run_id));
            disk.write_json(
                &run_file(&record.run_id),
                &serde_json::to_vec(&record).unwrap(),
            )
            .unwrap();
            selection.scan(&path, disk.epoch, &disk.gate).unwrap();
            original.push(record);
        }
        let original_bytes = selection.bytes;
        let original_latest = selection.latest.clone();
        let mut invalid = run("new", "t0", 10);
        invalid.input = "乙".repeat(27000);
        let mut payload = serde_json::to_value(invalid).unwrap();
        payload.as_object_mut().unwrap().remove("input");
        // 保留大字段以确实触发淘汰，而必填 input 缺失只会在完整解析时发现。
        payload["unknownLargeField"] = serde_json::Value::String("乙".repeat(27000));
        disk.write_json("runs/new.json", &serde_json::to_vec(&payload).unwrap())
            .unwrap();
        let candidate = disk.dir.join("runs/new.json");
        assert!(read_envelope_limited::<Summary>(&candidate, limit).is_ok());
        assert!(original_bytes + std::fs::metadata(&candidate).unwrap().len() > limit);
        assert!(selection.scan(&candidate, disk.epoch, &disk.gate).is_err());
        assert!(selection.complete());
        assert_eq!(selection.bytes, original_bytes);
        assert_eq!(selection.latest, original_latest);
        assert!(selection.peak_bytes <= limit);
        let loaded = selection.finish(&mut ThreadIndexFile::default()).unwrap();
        assert_eq!(loaded, original);
    }

    #[test]
    fn 部分淘汰仍不足预算时不改变旧选择() {
        let temp = Temp::new();
        let mut disk = temp.disk();
        let limit = 100 * 1024;
        let mut selection = Selection::new(&ThreadIndexFile::default(), 0, limit);
        for (id, started) in [("old", 1), ("newer", 100)] {
            let mut record = run(id, id, started);
            record.input = "甲".repeat(15000);
            disk.write_json(
                &run_file(&record.run_id),
                &serde_json::to_vec(&record).unwrap(),
            )
            .unwrap();
            selection
                .scan(
                    &disk.dir.join(run_file(&record.run_id)),
                    disk.epoch,
                    &disk.gate,
                )
                .unwrap();
        }
        let original_latest = selection.latest.clone();
        let mut candidate = run("middle", "middle", 50);
        candidate.input = "甲".repeat(25000);
        disk.write_json("runs/middle.json", &serde_json::to_vec(&candidate).unwrap())
            .unwrap();
        selection
            .scan(&disk.dir.join("runs/middle.json"), disk.epoch, &disk.gate)
            .unwrap();
        assert_eq!(selection.latest, original_latest);
        assert_eq!(
            selection
                .finish(&mut ThreadIndexFile::default())
                .unwrap()
                .len(),
            2
        );
    }
}
