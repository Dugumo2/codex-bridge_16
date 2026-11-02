//! 专用线程中的读写、恢复和清理；任何变更前都检查取消与 epoch。

use super::*;
use crate::runs::types::ThreadIndexEntry;
use serde::de::{DeserializeOwned, IgnoredAny};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime};

pub(super) const MAX_LOG_FILE_BYTES: u64 = 10 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
pub(super) struct Envelope<T> {
    pub epoch: u64,
    pub revision: u64,
    pub payload: T,
}

/// 取消令牌与初始化期限在每次 I/O 返回后再次检查，阻止迟到初始化发布状态。
pub(super) struct Gate {
    pub cancelled: Arc<AtomicBool>,
    pub initialization: Option<(Instant, Duration)>,
}

impl Gate {
    pub fn check(&self) -> Result<(), StoreError> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "存储任务已取消").into());
        }
        if let Some((deadline, timeout)) = self.initialization {
            if Instant::now() >= deadline {
                return Err(StoreError::Timeout(timeout));
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct Metadata {
    revision: u64,
    committed_len: Option<u64>,
}

pub(super) struct Disk {
    pub lifecycle_version: u64,
    pub dir: PathBuf,
    pub epoch: u64,
    pub gate: Gate,
    // 每个现存文件最多一项；删除文件时同时删除，线程退出时整体释放。
    pub metadata: BTreeMap<String, Metadata>,
    pub prepared_log: Option<tracing_appender::rolling::RollingFileAppender>,
    corrupt_index: bool,
    corrupt_backup: Option<PathBuf>,
    #[cfg(test)]
    pub fail_after_log_bytes: Option<usize>,
}

/// 通用外壳检查不受初始化预算约束；流式跳过 payload，不构造大输出或索引。
pub(super) fn read_envelope(path: &Path) -> io::Result<Option<Envelope<IgnoredAny>>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    decode_envelope(BufReader::new(file))
}

pub(super) fn read_envelope_limited<T: DeserializeOwned>(
    path: &Path,
    limit: u64,
) -> io::Result<Option<Envelope<T>>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if file.metadata()?.len() > limit {
        return Err(io::Error::new(
            io::ErrorKind::FileTooLarge,
            "状态文件超过初始化字节预算",
        ));
    }
    decode_envelope(BufReader::new(file.take(limit.saturating_add(1))))
}

fn decode_envelope<T: DeserializeOwned>(reader: impl Read) -> io::Result<Option<Envelope<T>>> {
    serde_json::from_reader(reader).map(Some).map_err(|error| {
        io::Error::new(
            error.io_error_kind().unwrap_or(io::ErrorKind::InvalidData),
            format!("状态文件格式无效：{error}"),
        )
    })
}

impl Disk {
    pub fn initialize(config: &mut StoreConfig, gate: Gate) -> Result<(Self, Loaded), StoreError> {
        Self::initialize_with_logging(config, gate, crate::logging::prepare_file)
    }

    pub(super) fn initialize_with_logging(
        config: &mut StoreConfig,
        gate: Gate,
        prepare_log: impl FnOnce(
            &Path,
        ) -> Result<
            Option<tracing_appender::rolling::RollingFileAppender>,
            String,
        >,
    ) -> Result<(Self, Loaded), StoreError> {
        gate.check()?;
        let dir = workspace_dir(&config.root, &config.workspace);
        gate.check()?;
        fs::create_dir_all(dir.join("runs"))?;
        gate.check()?;
        let prepared_log = match prepare_log(&dir) {
            Ok(file) => file,
            Err(error) => {
                tracing::warn!(%error, "文件日志不可用，仅输出到 stderr");
                None
            }
        };
        gate.check()?;
        let previous = read_envelope(&dir.join("broker.json"))?;
        gate.check()?;
        let epoch = previous
            .as_ref()
            .map_or(0, |entry| entry.epoch)
            .checked_add(1)
            .ok_or_else(|| io::Error::other("存储 epoch 已溢出"))?;
        let mut disk = Self {
            lifecycle_version: 0,
            dir,
            epoch,
            gate,
            metadata: BTreeMap::new(),
            prepared_log,
            corrupt_index: false,
            corrupt_backup: None,
            #[cfg(test)]
            fail_after_log_bytes: None,
        };
        disk.check_epoch("broker.json")?;
        disk.clean_temps()?;
        disk.cleanup(SystemTime::now())?;
        let mut loaded = disk.load()?;
        // 只有首次建立且确实没有旧文件的工作区，才能把空历史标为已核实。
        if previous.is_none()
            && !disk.dir.join("threads.json").exists()
            && fs::read_dir(disk.dir.join("runs"))?.next().is_none()
        {
            loaded.index.child_history.verify_empty_store();
        }
        config.broker.epoch = epoch;
        let payload = serde_json::to_vec(&config.broker).map_err(json_error)?;
        disk.write_json("broker.json", &payload)?;
        if disk.corrupt_index {
            disk.write_json(
                "threads.json",
                &serde_json::to_vec(&loaded.index).map_err(json_error)?,
            )?;
        }
        disk.gate.check()?;
        loaded.epoch = epoch;
        Ok((disk, loaded))
    }

    pub fn check_epoch(&self, file: &str) -> Result<(), StoreError> {
        self.gate.check()?;
        let envelope = read_envelope(&self.dir.join(file))?;
        self.gate.check()?;
        if envelope.is_some_and(|entry| entry.epoch > self.epoch) {
            return Err(StoreError::Superseded {
                file: file.to_owned(),
            });
        }
        Ok(())
    }

    pub fn write_json(&mut self, name: &str, payload: &[u8]) -> Result<(), StoreError> {
        // broker.json 也要检查，避免旧实例继续写尚未被新实例触碰的运行文件。
        self.check_epoch("broker.json")?;
        if name == "threads.json" && self.corrupt_index {
            self.backup_corrupt_index()?;
        }
        let previous = self.write_target(name)?;
        self.gate.check()?;
        if previous
            .as_ref()
            .is_some_and(|entry| entry.epoch > self.epoch)
        {
            return Err(StoreError::Superseded {
                file: name.to_owned(),
            });
        }
        let revision = previous
            .as_ref()
            .map_or(0, |entry| entry.revision)
            .max(self.metadata.get(name).map_or(0, |entry| entry.revision))
            .checked_add(1)
            .ok_or_else(|| io::Error::other("文件 revision 已溢出"))?;
        let destination = self.dir.join(name);
        let temp = destination.with_file_name(format!(
            "{}.{}.{revision}.tmp",
            destination.file_name().unwrap().to_string_lossy(),
            std::process::id()
        ));
        let result = (|| {
            self.gate.check()?;
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp)?;
            self.gate.check()?;
            write!(
                file,
                "{{\"epoch\":{},\"revision\":{},\"payload\":",
                self.epoch, revision
            )?;
            self.gate.check()?;
            file.write_all(payload)?;
            self.gate.check()?;
            file.write_all(b"}\n")?;
            self.gate.check()?;
            file.sync_all()?;
            drop(file);
            self.check_epoch("broker.json")?;
            self.write_target(name)?;
            self.gate.check()?;
            replace(&temp, &destination)?;
            Ok::<_, StoreError>(())
        })();
        if result.is_err() {
            // 取消后只允许清理本次私有临时文件，不再发布或改写状态文件。
            let _ = fs::remove_file(&temp);
        } else {
            self.metadata.entry(name.to_owned()).or_default().revision = revision;
            if name == "threads.json" {
                self.corrupt_index = false;
                self.corrupt_backup = None;
            }
        }
        result
    }

    fn write_target(&self, name: &str) -> Result<Option<Envelope<IgnoredAny>>, StoreError> {
        let target = match read_envelope(&self.dir.join(name)) {
            Ok(target) => target,
            Err(error)
                if error.kind() == io::ErrorKind::InvalidData
                    && name == "threads.json"
                    && self.corrupt_backup.is_some() =>
            {
                None
            }
            Err(error) => return Err(error.into()),
        };
        self.gate.check()?;
        if target.as_ref().is_some_and(|file| file.epoch > self.epoch) {
            return Err(StoreError::Superseded { file: name.into() });
        }
        Ok(target)
    }

    fn backup_corrupt_index(&mut self) -> Result<(), StoreError> {
        self.check_epoch("broker.json")?;
        // payload 错误也可能有合法且更大的 epoch，必须先核对外壳。
        match read_envelope(&self.dir.join("threads.json")) {
            Ok(Some(file)) if file.epoch > self.epoch => {
                return Err(StoreError::Superseded {
                    file: "threads.json".into(),
                })
            }
            Err(error) if error.kind() != io::ErrorKind::InvalidData => return Err(error.into()),
            _ => (),
        }
        if self.corrupt_backup.is_some() {
            return Ok(());
        }
        // 先为新副本预留一份空间；清理失败就停止备份，不能继续增加文件数。
        self.cleanup_corrupt_backups(SystemTime::now(), 2)?;
        let mut timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let (backup, mut destination) = loop {
            self.gate.check()?;
            let backup = self.dir.join(format!("threads.json.corrupt-{timestamp}"));
            match OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&backup)
            {
                Ok(file) => break (backup, file),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => timestamp += 1,
                Err(error) => return Err(error.into()),
            }
        };
        let result = (|| {
            self.gate.check()?;
            let mut source = File::open(self.dir.join("threads.json"))?;
            io::copy(&mut source, &mut destination)?;
            self.gate.check()?;
            destination.sync_all()?;
            self.check_epoch("broker.json")
        })();
        drop(destination);
        if let Err(error) = result {
            let _ = fs::remove_file(backup);
            return Err(error);
        }
        tracing::warn!(file = %backup.display(), "已保留损坏索引诊断副本，准备原子恢复");
        self.corrupt_backup = Some(backup);
        Ok(())
    }

    pub fn append_log(&mut self, name: &str, bytes: &[u8]) -> Result<(), StoreError> {
        self.check_epoch("broker.json")?;
        let path = self.dir.join(name);
        let mut committed = match self.metadata.get(name).and_then(|meta| meta.committed_len) {
            Some(len) => len,
            None => match fs::metadata(&path) {
                Ok(meta) => meta.len(),
                Err(error) if error.kind() == io::ErrorKind::NotFound => 0,
                Err(error) => return Err(error.into()),
            },
        };
        self.gate.check()?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)?;
        // 只有实际打开了文件才登记，持续创建失败不会产生无文件对应的元数据。
        self.metadata
            .entry(name.to_owned())
            .or_default()
            .committed_len = Some(committed);
        self.gate.check()?;
        file.set_len(committed)?;
        if committed > 0 && committed.saturating_add(bytes.len() as u64) > MAX_LOG_FILE_BYTES {
            self.gate.check()?;
            file.sync_all()?;
            drop(file);
            self.check_epoch("broker.json")?;
            self.gate.check()?;
            replace(&path, &path.with_extension("log.1"))?;
            committed = 0;
            self.metadata.get_mut(name).unwrap().committed_len = Some(0);
            self.gate.check()?;
            file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&path)?;
        }
        self.gate.check()?;
        file.seek(SeekFrom::Start(committed))?;
        #[cfg(test)]
        if let Some(count) = self.fail_after_log_bytes.take() {
            file.write_all(&bytes[..count.min(bytes.len())])?;
            return Err(io::Error::other("测试注入：日志部分追加失败").into());
        }
        self.gate.check()?;
        file.write_all(bytes)?;
        self.gate.check()?;
        file.sync_all()?;
        self.metadata.get_mut(name).unwrap().committed_len = Some(committed + bytes.len() as u64);
        Ok(())
    }

    fn clean_temps(&self) -> Result<(), StoreError> {
        for dir in [&self.dir, &self.dir.join("runs")] {
            self.gate.check()?;
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                if entry.path().extension().is_some_and(|ext| ext == "tmp")
                    && entry.file_type()?.is_file()
                {
                    self.check_epoch("broker.json")?;
                    self.gate.check()?;
                    if let Err(error) = fs::remove_file(entry.path()) {
                        tracing::warn!(file = %entry.path().display(), %error, "清理残留临时文件失败");
                    }
                }
            }
        }
        Ok(())
    }

    fn cleanup_corrupt_backups(&mut self, now: SystemTime, keep: usize) -> Result<(), StoreError> {
        self.check_epoch("broker.json")?;
        // 扫描时只保留最近 keep 份的文件信息，不先收集全部历史副本。
        let mut recent = Vec::with_capacity(keep + 1);
        for entry in fs::read_dir(&self.dir)? {
            self.gate.check()?;
            let entry = entry?;
            if !entry
                .file_name()
                .to_string_lossy()
                .starts_with("threads.json.corrupt-")
                || !entry.file_type()?.is_file()
            {
                continue;
            }
            let modified = entry.metadata()?.modified()?;
            let path = entry.path();
            let expired = now.duration_since(modified).unwrap_or_default()
                > Duration::from_millis(RETENTION_MS);
            let remove = if expired {
                Some(path)
            } else {
                recent.push((modified, path));
                recent.sort();
                (recent.len() > keep).then(|| recent.remove(0).1)
            };
            if let Some(path) = remove {
                self.check_epoch("broker.json")?;
                self.gate.check()?;
                fs::remove_file(&path)?;
                if self.corrupt_backup.as_ref() == Some(&path) {
                    self.corrupt_backup = None;
                }
            }
        }
        Ok(())
    }

    pub fn cleanup(&mut self, now: SystemTime) -> Result<(), StoreError> {
        self.check_epoch("broker.json")?;
        // 单个诊断副本删不掉（被别的进程锁住、权限不足）只影响副本本身，不能挡住 runs/ 的过期清理；
        // 取消、超时与 epoch 被超越仍立即退出。新建备份前的清理照旧「失败就不新增」。
        match self.cleanup_corrupt_backups(now, 3) {
            Ok(()) => {}
            Err(error @ (StoreError::Superseded { .. } | StoreError::Timeout(_))) => {
                return Err(error)
            }
            Err(StoreError::Io(error)) if error.kind() == io::ErrorKind::Interrupted => {
                return Err(StoreError::Io(error))
            }
            Err(error) => {
                tracing::warn!(%error, "清理损坏索引诊断副本失败，继续清理过期运行文件");
            }
        }
        for entry in fs::read_dir(self.dir.join("runs"))? {
            self.gate.check()?;
            let entry = entry?;
            let path = entry.path();
            let filename = entry.file_name().to_string_lossy().into_owned();
            if !entry.file_type()?.is_file()
                || !(filename.ends_with(".json")
                    || filename.ends_with(".log")
                    || filename.ends_with(".log.1"))
            {
                continue;
            }
            let age = now
                .duration_since(entry.metadata()?.modified()?)
                .unwrap_or_default();
            if age <= Duration::from_millis(RETENTION_MS) {
                continue;
            }
            let name = format!("runs/{filename}");
            if filename.ends_with(".json") {
                // 仅校验外壳与 JSON 完整性，不加载输出，也不套用初始化预算。
                match read_envelope(&path) {
                    Ok(Some(record)) if record.epoch > self.epoch => {
                        return Err(StoreError::Superseded { file: name })
                    }
                    Ok(Some(_)) => (),
                    Ok(None) => continue,
                    Err(error) => {
                        tracing::warn!(file = %name, %error, "跳过无法解析的运行文件");
                        continue;
                    }
                }
            }
            self.check_epoch("broker.json")?;
            self.gate.check()?;
            match fs::remove_file(&path) {
                Ok(()) => {
                    self.metadata.remove(&name);
                }
                Err(error) => tracing::warn!(file = %name, %error, "清理过期运行文件失败"),
            }
        }
        Ok(())
    }

    fn load(&mut self) -> Result<Loaded, StoreError> {
        self.gate.check()?;
        let index_bytes = match fs::metadata(self.dir.join("threads.json")) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        let mut index = match read_envelope_limited::<ThreadIndexFile>(
            &self.dir.join("threads.json"),
            super::load::MAX_INITIALIZATION_BYTES,
        ) {
            Ok(Some(file)) => {
                if file.epoch > self.epoch {
                    return Err(StoreError::Superseded {
                        file: "threads.json".into(),
                    });
                }
                file.payload
            }
            Ok(None) => ThreadIndexFile::default(),
            Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                match read_envelope(&self.dir.join("threads.json")) {
                    Ok(Some(file)) if file.epoch > self.epoch => {
                        return Err(StoreError::Superseded {
                            file: "threads.json".into(),
                        })
                    }
                    Err(error) if error.kind() != io::ErrorKind::InvalidData => {
                        return Err(error.into())
                    }
                    _ => (),
                }
                self.corrupt_index = true;
                tracing::warn!(%error, "读取线程索引失败，尝试从运行记录重建");
                let mut index = ThreadIndexFile::default();
                index.child_history.mark_corrupt();
                index
            }
            Err(error) => return Err(error.into()),
        };
        let indexed_threads: std::collections::BTreeSet<_> = index
            .threads
            .values()
            .map(|e| e.thread_id.clone())
            .collect();
        let mut selection =
            super::load::Selection::new(&index, index_bytes, super::load::MAX_INITIALIZATION_BYTES);
        for entry in fs::read_dir(self.dir.join("runs"))? {
            self.gate.check()?;
            let entry = entry?;
            if entry.path().extension().is_none_or(|ext| ext != "json")
                || !entry.file_type()?.is_file()
            {
                continue;
            }
            match selection.scan(&entry.path(), self.epoch, &self.gate) {
                Ok(()) => (),
                Err(error @ (StoreError::Superseded { .. } | StoreError::Timeout(_))) => {
                    return Err(error)
                }
                Err(error) => {
                    selection.mark_corrupt_history();
                    if !selection.complete() {
                        return Err(error);
                    }
                    tracing::warn!(file = %entry.path().display(), %error, "跳过无法解析的运行文件")
                }
            }
        }
        self.gate.check()?;
        let runs = selection.finish(&mut index)?;
        let inbox = match read_envelope_limited::<crate::inbox::InboxFile>(
            &self.dir.join("inbox.json"),
            crate::inbox::MAX_INBOX_BYTES as u64 + 4096,
        ) {
            Ok(Some(file)) if file.epoch > self.epoch => {
                return Err(StoreError::Superseded {
                    file: "inbox.json".into(),
                })
            }
            Ok(Some(file)) => Some(file.payload),
            Ok(None) => None,
            Err(error) => {
                tracing::warn!(%error, "读取待回应事项失败，按空事项恢复");
                None
            }
        };
        self.gate.check()?;
        let lifecycle = read_envelope_limited::<super::lifecycle::File>(
            &self.dir.join("lifecycle.json"),
            super::load::MAX_INITIALIZATION_BYTES,
        )
        .and_then(|envelope| {
            if envelope.as_ref().is_some_and(|f| f.epoch > self.epoch) {
                return Err(io::Error::other("生命周期文件属于更新的 broker"));
            }
            let mut file = envelope.map(|f| f.payload).unwrap_or_default();
            for (id, record) in &file.records {
                if id != &record.thread_id
                    || record.operation_id.is_empty()
                    || record.short_id.is_empty()
                    || record
                        .attempts
                        .windows(2)
                        .any(|pair| pair[0].attempt >= pair[1].attempt)
                    || record
                        .attempts
                        .iter()
                        .any(|a| !matches!(a.server_kind.as_str(), "private" | "shared"))
                {
                    return Err(io::Error::other("生命周期记录结构无效"));
                }
            }
            super::lifecycle::recover_instances(&mut file);
            Ok(file)
        })
        .map_err(|error| format!("生命周期文件无法读取：{error}"));
        if let Ok(file) = &lifecycle {
            index.threads.retain(|_, entry| {
                indexed_threads.contains(&entry.thread_id)
                    || !file.records.contains_key(&entry.thread_id)
            });
        }
        Ok(Loaded {
            lifecycle: Some(lifecycle),
            epoch: self.epoch,
            index,
            runs,
            workspace_key: self
                .dir
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default(),
            inbox,
        })
    }
}

fn json_error(error: serde_json::Error) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("状态序列化失败：{error}"),
    )
}

pub(super) fn activity(run: &RunRecord) -> u64 {
    run.turns
        .iter()
        .map(|turn| turn.completed_at)
        .chain(run.ended_at)
        .fold(run.started_at, u64::max)
}

pub(super) fn rebuild_entry(
    index: &mut ThreadIndexFile,
    thread: &str,
    run: &RunRecord,
    indexed_starts: &BTreeMap<String, u64>,
) {
    if index.removed.contains_key(thread) {
        return;
    }
    let old_key = index
        .threads
        .iter()
        .find(|(_, entry)| entry.thread_id == thread)
        .map(|(key, _)| key.clone());
    let old = old_key.as_ref().and_then(|key| index.threads.get(key));
    let update = match old {
        None => true,
        Some(entry) => match entry.last_run_id.as_deref() {
            None => true,
            Some(id) if id == run.run_id => activity(run) > entry.updated_at,
            Some(id) => indexed_starts
                .get(id)
                .map_or(activity(run) > entry.updated_at, |previous| {
                    run.started_at > *previous
                }),
        },
    };
    if !update {
        return;
    }
    let short_id = if thread == run.thread_id {
        &run.short_id
    } else {
        run.review_short_id.as_ref().unwrap()
    };
    if index
        .threads
        .get(short_id)
        .is_some_and(|entry| entry.thread_id != thread)
    {
        tracing::warn!(thread, short_id, "重建索引遇到 shortId 冲突，保留已有项");
        return;
    }
    let mut entry = old.cloned().unwrap_or_else(|| ThreadIndexEntry {
        parent_thread_id: None,
        relation_kind: None,
        source_summary: None,
        can_accept_direct_input: None,
        agent_version: None,

        resume_overrides: None,
        thread_id: thread.to_owned(),
        short_id: short_id.clone(),
        last_run_id: None,
        updated_at: 0,
        model: None,
        preview: None,
        cwd: None,
        meta: run.meta.clone(),
    });
    if let Some(key) = old_key {
        index.threads.remove(&key);
    }
    entry.short_id = short_id.clone();
    entry.last_run_id = Some(run.run_id.clone());
    entry.updated_at = entry.updated_at.max(activity(run));
    index.threads.insert(short_id.clone(), entry);
}

#[cfg(not(windows))]
fn replace(from: &Path, to: &Path) -> io::Result<()> {
    fs::rename(from, to)
}

#[cfg(windows)]
fn replace(from: &Path, to: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
    };
    let from: Vec<_> = from.as_os_str().encode_wide().chain(Some(0)).collect();
    let to: Vec<_> = to.as_os_str().encode_wide().chain(Some(0)).collect();
    // 两个缓冲区都以 NUL 结尾，且在调用期间有效。
    if unsafe {
        MoveFileExW(
            from.as_ptr(),
            to.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    } == 0
    {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}
