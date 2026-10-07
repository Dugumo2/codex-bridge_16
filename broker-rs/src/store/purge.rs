//! purge 的磁盘阶段：先准备清单，再持久化删除标记，最后逐项删除。
use super::{disk::Disk, StoreError};
use crate::runs::{PurgeFailure, PurgePlan, PurgeReport, RunRecord, ThreadIndexFile};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::fs;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PurgeResult {
    Prepared(PurgePlan),
    Finished(PurgeReport),
    Failed(String),
    Busy(String),
}

/// 只传递小结果；状态机读取互斥内存，不等待文件 I/O。
#[derive(Debug, Clone, Default)]
pub struct PurgeTicket(Arc<Mutex<Option<PurgeResult>>>);
impl PartialEq for PurgeTicket {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for PurgeTicket {}
impl PurgeTicket {
    pub fn result(&self) -> Option<PurgeResult> {
        self.0.lock().unwrap().clone()
    }
    pub fn finish(&self, result: PurgeResult) {
        let mut slot = self.0.lock().unwrap();
        if slot.is_none() {
            *slot = Some(result);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PurgeWork {
    pub thread: String,
    pub plan: PurgePlan,
    /// None 为准备阶段；Some 是状态机核实终态后的完整索引快照。
    pub index: Option<ThreadIndexFile>,
    pub ticket: PurgeTicket,
    pub seeds: Vec<RunHeader>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunHeader {
    pub run_id: String,
    pub thread_id: String,
    pub short_id: String,
    pub review_thread_id: Option<String>,
    pub review_short_id: Option<String>,
    pub state: Option<crate::runs::RunState>,
    pub stop_outcome: Option<crate::runs::StopOutcome>,
}
impl From<&RunRecord> for RunHeader {
    fn from(r: &RunRecord) -> Self {
        Self {
            run_id: r.run_id.clone(),
            thread_id: r.thread_id.clone(),
            short_id: r.short_id.clone(),
            review_thread_id: r.review_thread_id.clone(),
            review_short_id: r.review_short_id.clone(),
            state: Some(r.state),
            stop_outcome: r.stop_outcome,
        }
    }
}

pub fn valid_file(file: &str) -> bool {
    file.strip_prefix("runs/").is_some_and(|name| {
        name.strip_suffix(".json")
            .or_else(|| name.strip_suffix(".log"))
            .or_else(|| name.strip_suffix(".log.1"))
            .is_some_and(super::queue::valid_id)
    })
}

impl Disk {
    pub(super) fn prepare_purge(&mut self, work: &PurgeWork) -> Result<PurgePlan, StoreError> {
        let mut plan = work.plan.clone();
        plan.threads.insert(work.thread.clone());
        let mut headers = self.scan_run_headers()?;
        for header in &work.seeds {
            headers.insert(header.run_id.clone(), header.clone());
        }
        loop {
            let before = plan.threads.len();
            for header in headers.values() {
                if plan.threads.contains(&header.thread_id) {
                    plan.threads.extend(header.review_thread_id.clone());
                }
            }
            if before == plan.threads.len() {
                break;
            }
        }
        for header in headers.into_values() {
            if plan.threads.contains(&header.thread_id)
                || header
                    .review_thread_id
                    .as_ref()
                    .is_some_and(|t| plan.threads.contains(t))
            {
                plan.files.insert(super::run_file(&header.run_id));
                plan.runs.insert(
                    header.run_id.clone(),
                    crate::runs::PurgeRun {
                        thread_id: header.thread_id.clone(),
                        review_thread_id: header.review_thread_id.clone(),
                        state: header.state,
                        stop_outcome: header.stop_outcome,
                    },
                );
                // 从子线程 purge 不删除父线程与其他 review 共用的日志。
                if plan.threads.contains(&header.thread_id) {
                    plan.files.insert(format!("runs/{}.log", header.short_id));
                }
                if let Some(short) = header.review_short_id {
                    plan.files.insert(format!("runs/{short}.log"));
                }
            }
        }
        include_rotated_logs(&mut plan);
        if !plan.files.iter().all(|file| valid_file(file)) {
            return Err(std::io::Error::other("删除清单含无效的文件标识").into());
        }
        Ok(plan)
    }

    pub(super) fn commit_purge(&mut self, work: &PurgeWork) -> Result<PurgeReport, StoreError> {
        let mut report = PurgeReport {
            thread_id: work.thread.clone(),
            ..Default::default()
        };
        if !work.plan.files.iter().all(|file| valid_file(file)) {
            return Err(std::io::Error::other("删除清单含无效的文件标识").into());
        }
        // 标记必须先成功落盘；否则一个文件都不删除。
        self.write_json(
            "threads.json",
            &serde_json::to_vec(work.index.as_ref().expect("提交带索引")).expect("索引可序列化"),
        )?;
        for file in &work.plan.files {
            self.gate.check()?;
            if work.ticket.result().is_some() {
                return Err(std::io::Error::other("删除任务已取消").into());
            }
            self.check_epoch("broker.json")?;
            // JSON 文件还须检查它自己的 epoch；日志没有 JSON 外壳。
            if file.ends_with(".json") {
                match super::disk::read_envelope(&self.dir.join(file)) {
                    Ok(Some(envelope)) if envelope.epoch > self.epoch => {
                        return Err(StoreError::Superseded { file: file.clone() });
                    }
                    Err(error) => {
                        report.failed.push(PurgeFailure {
                            path: file.clone(),
                            reason: format!("无法核实文件：{error}"),
                        });
                        continue;
                    }
                    _ => {}
                }
            }
            match fs::remove_file(self.dir.join(file)) {
                Ok(()) => {
                    self.metadata.remove(file);
                    report.deleted.push(file.clone());
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    self.metadata.remove(file);
                    report.already_deleted.push(file.clone());
                }
                Err(error) => report.failed.push(PurgeFailure {
                    path: file.clone(),
                    reason: format!("删除失败：{error}"),
                }),
            }
        }
        Ok(report)
    }
}

pub fn include_rotated_logs(plan: &mut PurgePlan) {
    let rotated: Vec<_> = plan
        .files
        .iter()
        .filter(|file| file.ends_with(".log"))
        .map(|file| format!("{file}.1"))
        .collect();
    plan.files.extend(rotated);
}

/// 把待写旧记录纳入扫描，防止写失败退避的文件漏出清单。
pub(super) fn seed_pending(work: &mut PurgeWork, bytes: &[u8]) {
    if let Ok(header) = serde_json::from_slice::<RunHeader>(bytes) {
        work.seeds.push(header);
    }
}

pub fn deleted_files(report: &PurgeReport) -> BTreeSet<&str> {
    report
        .deleted
        .iter()
        .chain(&report.already_deleted)
        .map(String::as_str)
        .collect()
}
