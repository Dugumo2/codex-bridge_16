//! 有界入口、唯一写入线程与不依赖文件 I/O 的关闭期限。

use super::disk::{Disk, Gate};
use super::*;
use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Instant, SystemTime};
use tokio::sync::{oneshot, Notify};

const MAX_PENDING_FILES: usize = 512;
const MAX_PENDING_BYTES: usize = 64 * 1024 * 1024;
const DAILY: Duration = Duration::from_secs(24 * 3600);
// 为「丢了 N 行」以及换行预留空间，文本与提示合计不超过 1 MiB。
const LOG_MARKER_RESERVE: usize = 128;

fn log_timestamp() -> String {
    crate::runs::utc_timestamp(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
    )
}

#[derive(Default)]
struct LogBuffer {
    lines: VecDeque<(u64, String)>,
    bytes: usize,
    dropped: u64,
    dropped_at: String,
}

enum Content {
    Lifecycle(lifecycle::Work),
    Json(Vec<u8>),
    Log(LogBuffer),
    Purge(purge::PurgeWork),
}

impl Content {
    fn bytes(&self) -> usize {
        match self {
            Self::Lifecycle(work) => format!("{:?}", work.kind).len(),
            Self::Json(bytes) => bytes.len(),
            Self::Log(log) => log.bytes + LOG_MARKER_RESERVE,
            Self::Purge(work) => {
                work.plan.files.iter().map(String::len).sum::<usize>() + work.seeds.len() * 256
            }
        }
    }
    fn log_bytes(&self) -> usize {
        match self {
            Self::Log(log) => log.bytes,
            Self::Json(_) | Self::Purge(_) | Self::Lifecycle(_) => 0,
        }
    }
}

struct Pending {
    content: Content,
    order: u64,
    dirty: bool,
    retry_at: Instant,
    retry_delay: Duration,
}

struct Active {
    name: String,
    bytes: usize,
    log_bytes: usize,
    dirty: bool,
}

#[derive(Default)]
struct State {
    // 入口即合并，含在途最多 512 个文件、64 MiB；没有中间无界通道。
    pending: BTreeMap<String, Pending>,
    /// 已获准删除的不可复用文件名；挡住删除期间和完成后的迟到写入。
    purged_files: std::collections::BTreeSet<String>,
    active: Option<Active>,
    sequence: u64,
    draining: bool,
    terminal: bool,
    lost_data: bool,
    report: DrainReport,
    #[cfg(test)]
    panic_writer: bool,
}

pub(super) struct Shared {
    state: Mutex<State>,
    wake: Condvar,
    changed: Notify,
    cancelled: Arc<AtomicBool>,
    on_event: Box<dyn Fn(StoreEvent) + Send + Sync>,
    #[cfg(test)]
    io_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

pub(super) struct Owner {
    pub shared: Arc<Shared>,
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.shared.begin_drain();
    }
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    pub fn submit(&self, op: StoreOp) {
        if let StoreOp::Lifecycle(mut work) = op {
            let mut state = self.lock();
            if state.draining || state.terminal {
                work.ticket
                    .finish(lifecycle::ResultValue::Failed("存储已关闭".into()));
                return;
            }
            if let lifecycle::WorkKind::Scan { seeds } = &mut work.kind {
                for (name, pending) in &state.pending {
                    if name.starts_with("runs/") && name.ends_with(".json") {
                        if let Content::Json(bytes) = &pending.content {
                            if let Ok(header) = serde_json::from_slice(bytes) {
                                seeds.push(header);
                            }
                        }
                    }
                }
            }
            state.sequence += 1;
            let order = state.sequence;
            state.pending.insert(
                format!("lifecycle-{order}"),
                Pending {
                    content: Content::Lifecycle(work),
                    order,
                    dirty: false,
                    retry_at: Instant::now(),
                    retry_delay: RETRY_MIN,
                },
            );
            drop(state);
            self.wake.notify_one();
            return;
        }
        if let StoreOp::Purge(mut work) = op {
            let mut state = self.lock();
            if state.draining || state.terminal {
                work.ticket
                    .finish(purge::PurgeResult::Failed("存储已关闭".into()));
                return;
            }
            if work.index.is_none() {
                for (name, pending) in &state.pending {
                    if name.starts_with("runs/") && name.ends_with(".json") {
                        if let Content::Json(bytes) = &pending.content {
                            purge::seed_pending(&mut work, bytes);
                        }
                    }
                }
            } else {
                let busy = state
                    .pending
                    .keys()
                    .any(|name| work.plan.files.contains(name))
                    || state
                        .active
                        .as_ref()
                        .is_some_and(|active| work.plan.files.contains(&active.name));
                if busy {
                    work.ticket
                        .finish(purge::PurgeResult::Busy("settling".into()));
                    return;
                }
                state.purged_files.extend(work.plan.files.iter().cloned());
            }
            state.sequence += 1;
            let order = state.sequence;
            state.pending.insert(
                "purge".into(),
                Pending {
                    content: Content::Purge(work),
                    order,
                    dirty: false,
                    retry_at: Instant::now(),
                    retry_delay: RETRY_MIN,
                },
            );
            drop(state);
            self.wake.notify_one();
            return;
        }
        self.submit_with(op, |op| match op {
            StoreOp::PutRun(run) => serde_json::to_vec(run),
            StoreOp::PutThreads(index) => serde_json::to_vec(index),
            StoreOp::PutInbox(inbox) => serde_json::to_vec(inbox),
            StoreOp::AppendLog { .. } => unreachable!("日志不经过状态序列化"),
            StoreOp::Purge(_) => unreachable!("删除任务不经过状态序列化"),
            StoreOp::Lifecycle(_) => unreachable!("生命周期任务不经过通用序列化"),
        });
    }

    fn submit_with(
        &self,
        op: StoreOp,
        serialize: impl FnOnce(&StoreOp) -> serde_json::Result<Vec<u8>>,
    ) {
        {
            let state = self.lock();
            if state.draining || state.terminal {
                drop(state);
                tracing::warn!("存储已关闭，忽略写入请求");
                return;
            }
        }
        // 序列化不占共享锁；本函数没有任何文件 I/O。
        let payload = match &op {
            StoreOp::AppendLog { .. } => None,
            _ => Some(serialize(&op).expect("状态记录可序列化")),
        };
        let (name, content) = match op {
            StoreOp::Lifecycle(_) => unreachable!("生命周期任务单独登记"),
            StoreOp::Purge(_) => unreachable!("删除任务单独登记"),
            StoreOp::PutRun(run) => {
                if !valid_id(&run.run_id) {
                    tracing::error!("拒绝含路径分隔符的运行标识");
                    return;
                }
                (run_file(&run.run_id), Content::Json(payload.unwrap()))
            }
            StoreOp::PutThreads(_) => ("threads.json".to_owned(), Content::Json(payload.unwrap())),
            // inbox.json 与 threads.json 一样是受保护内容，预算仅淘汰 runs/。
            StoreOp::PutInbox(_) => ("inbox.json".to_owned(), Content::Json(payload.unwrap())),
            StoreOp::AppendLog { short_id, lines } => {
                if !valid_id(&short_id) {
                    tracing::error!("拒绝含路径分隔符的线程短标识");
                    return;
                }
                if lines.is_empty() {
                    return;
                }
                let mut log = LogBuffer::default();
                // 先在调用方批次内截断，避免把超大日志请求整体登记进共享表。
                for line in lines {
                    log.bytes += line.len() + 1;
                    log.lines.push_back((0, line));
                    while log.bytes > MAX_PENDING_LOG_BYTES - LOG_MARKER_RESERVE {
                        let (_, line) = log.lines.pop_front().unwrap();
                        log.bytes -= line.len() + 1;
                        log.dropped += 1;
                        log.dropped_at = log_timestamp();
                    }
                }
                (format!("runs/{short_id}.log"), Content::Log(log))
            }
        };
        let mut state = self.lock();
        if state.draining || state.terminal {
            drop(state);
            tracing::warn!(file = %name, "存储已关闭，忽略写入请求");
            return;
        }
        if state.purged_files.contains(&name) {
            tracing::debug!("忽略已删除运行文件的迟到写入");
            return;
        }
        state.sequence += 1;
        let order = state.sequence;
        let mut content = content;
        if let Content::Log(log) = &mut content {
            for (sequence, _) in &mut log.lines {
                *sequence = order;
            }
        }
        let previous = state.pending.remove(&name);
        let (content, dirty, retry_at, retry_delay) = match previous {
            Some(previous) => (
                merge(previous.content, content),
                previous.dirty,
                previous.retry_at,
                previous.retry_delay,
            ),
            None => (content, false, Instant::now(), RETRY_MIN),
        };
        state.pending.insert(
            name,
            Pending {
                content,
                order,
                dirty,
                retry_at,
                retry_delay,
            },
        );
        let evicted = enforce_limits(&mut state);
        drop(state);
        log_evicted(evicted);
        self.wake.notify_one();
    }

    pub fn status(&self) -> StorageStatus {
        let state = self.lock();
        if state.lost_data
            || state.pending.values().any(|pending| pending.dirty)
            || state.active.as_ref().is_some_and(|active| active.dirty)
        {
            StorageStatus::Degraded
        } else {
            StorageStatus::Ok
        }
    }

    fn begin_drain(&self) {
        self.lock().draining = true;
        self.wake.notify_one();
    }

    pub async fn drain(&self, timeout: Duration) -> DrainReport {
        self.begin_drain();
        let done = async {
            loop {
                // 先登记通知再读状态，兼容多个等待者并避免丢失唤醒。
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                {
                    let state = self.lock();
                    if state.terminal {
                        return state.report.clone();
                    }
                }
                notified.await;
            }
        };
        match tokio::time::timeout(timeout, done).await {
            Ok(report) => report,
            Err(_) => {
                self.finish(None);
                self.lock().report.clone()
            }
        }
    }

    fn finish(&self, event: Option<StoreEvent>) {
        let mut state = self.lock();
        if state.terminal {
            return;
        }
        state.draining = true;
        state.terminal = true;
        state.lost_data |= event.is_some() || !state.pending.is_empty() || state.active.is_some();
        let mut names: Vec<_> = state.pending.keys().cloned().collect();
        if let Some(active) = state.active.take() {
            names.push(active.name);
        }
        state.report.abandoned.append(&mut names);
        state.report.abandoned.sort();
        state.report.abandoned.dedup();
        for pending in state.pending.values() {
            if let Content::Lifecycle(work) = &pending.content {
                work.ticket.finish(lifecycle::ResultValue::Failed(
                    "存储任务未完成即关闭".into(),
                ));
            }
            if let Content::Purge(work) = &pending.content {
                work.ticket
                    .finish(purge::PurgeResult::Failed("存储任务未完成即关闭".into()));
            }
        }
        state.pending.clear();
        self.cancelled.store(true, Ordering::Release);
        drop(state);
        self.wake.notify_all();
        self.changed.notify_waiters();
        if let Some(event) = event {
            // 回调不持共享锁；即使回调本身 panic，也不会丢失终态与 drain 通知。
            let _ = catch_unwind(AssertUnwindSafe(|| (self.on_event)(event)));
        }
    }
}

pub(super) fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

fn merge(old: Content, new: Content) -> Content {
    match (old, new) {
        (Content::Log(mut old), Content::Log(new)) => {
            old.bytes += new.bytes;
            old.dropped = old.dropped.saturating_add(new.dropped);
            if new.dropped != 0 {
                old.dropped_at = new.dropped_at;
            }
            old.lines.extend(new.lines);
            Content::Log(old)
        }
        (_, new) => new,
    }
}

fn enforce_limits(state: &mut State) -> Vec<String> {
    let active_bytes = state.active.as_ref().map_or(0, |active| active.bytes);
    let mut log_bytes = state.active.as_ref().map_or(0, |active| active.log_bytes)
        + state
            .pending
            .values()
            .map(|entry| entry.content.log_bytes())
            .sum::<usize>();
    while log_bytes > MAX_PENDING_LOG_BYTES - LOG_MARKER_RESERVE {
        let oldest = state
            .pending
            .iter()
            .filter_map(|(name, entry)| match &entry.content {
                Content::Log(log) => log
                    .lines
                    .front()
                    .map(|(sequence, _)| (*sequence, name.clone())),
                _ => None,
            })
            .min();
        let Some((_, name)) = oldest else {
            break;
        };
        let Content::Log(log) = &mut state.pending.get_mut(&name).unwrap().content else {
            unreachable!()
        };
        let (_, line) = log.lines.pop_front().unwrap();
        log.bytes -= line.len() + 1;
        log_bytes -= line.len() + 1;
        log.dropped = log.dropped.saturating_add(1);
        log.dropped_at = log_timestamp();
    }
    let mut evicted = Vec::new();
    loop {
        let active_extra = usize::from(
            state
                .active
                .as_ref()
                .is_some_and(|active| !state.pending.contains_key(&active.name)),
        );
        let bytes = active_bytes
            + state
                .pending
                .values()
                .map(|entry| entry.content.bytes())
                .sum::<usize>();
        if state.pending.len() + active_extra <= MAX_PENDING_FILES && bytes <= MAX_PENDING_BYTES {
            break;
        }
        let oldest = state
            .pending
            .iter()
            .filter(|(name, _)| name.starts_with("runs/"))
            .min_by_key(|(_, entry)| entry.order)
            .map(|(name, _)| name.clone());
        let Some(name) = oldest else {
            // 核心索引永不丢弃；仅核心索引本身超过预算时无法同时满足两个硬约束。
            state.lost_data = true;
            evicted
                .push("threads.json / inbox.json（受保护内容本身超过待写预算，保留并降级）".into());
            break;
        };
        state.pending.remove(&name);
        state.lost_data = true;
        evicted.push(name);
    }
    evicted
}

fn log_evicted(files: Vec<String>) {
    for file in files {
        tracing::error!(%file, "待写预算已用尽，淘汰最旧运行内容或保留超大核心索引");
    }
}

fn worker(disk: &mut Disk, shared: &Shared) {
    worker_with_interval(disk, shared, DAILY);
}

fn worker_with_interval(disk: &mut Disk, shared: &Shared, cleanup_interval: Duration) {
    let mut cleanup_at = Instant::now() + cleanup_interval;
    loop {
        let mut state = shared.lock();
        if state.terminal {
            return;
        }
        #[cfg(test)]
        if state.panic_writer {
            panic!("测试注入：写入线程意外退出");
        }
        if state.draining && state.pending.is_empty() {
            drop(state);
            shared.finish(None);
            return;
        }
        let now = Instant::now();
        if now >= cleanup_at && !state.draining {
            drop(state);
            match disk.cleanup(SystemTime::now()) {
                Err(StoreError::Superseded { file }) => {
                    shared.finish(Some(StoreEvent::Superseded { file }));
                    return;
                }
                Err(error) => tracing::warn!(%error, "每日清理运行文件失败"),
                Ok(()) => (),
            }
            cleanup_at = Instant::now() + cleanup_interval;
            continue;
        }
        let next = state
            .pending
            .iter()
            .filter(|(_, pending)| state.draining || pending.retry_at <= now)
            .min_by_key(|(_, pending)| pending.order)
            .map(|(name, _)| name.clone());
        if let Some(name) = next {
            let pending = state.pending.remove(&name).unwrap();
            let final_attempt = state.draining;
            state.active = Some(Active {
                name: name.clone(),
                bytes: pending.content.bytes(),
                log_bytes: pending.content.log_bytes(),
                dirty: pending.dirty,
            });
            drop(state);
            #[cfg(test)]
            {
                let hook = shared.io_hook.lock().unwrap().clone();
                if let Some(hook) = hook {
                    hook();
                }
            }
            let result = match &pending.content {
                Content::Lifecycle(work) => {
                    disk.gate.initialization = Some((
                        Instant::now() + Duration::from_secs(10),
                        Duration::from_secs(10),
                    ));
                    let result = disk.lifecycle_work(work);
                    disk.gate.initialization = None;
                    match result {
                        Ok(result) => {
                            work.ticket.finish(result);
                            Ok(())
                        }
                        Err(error) => {
                            work.ticket.finish(lifecycle::ResultValue::Failed(format!(
                                "生命周期存储失败：{error}"
                            )));
                            if matches!(error, StoreError::Superseded { .. }) {
                                Err(error)
                            } else {
                                Ok(())
                            }
                        }
                    }
                }
                Content::Purge(work) => {
                    if work.ticket.result().is_some() {
                        Ok(())
                    } else {
                        let result = if work.index.is_some() {
                            disk.commit_purge(work).map(purge::PurgeResult::Finished)
                        } else {
                            disk.prepare_purge(work).map(purge::PurgeResult::Prepared)
                        };
                        match result {
                            Ok(result) => {
                                if matches!(result, purge::PurgeResult::Finished(_)) {
                                    if let Some(index) = &work.index {
                                        (shared.on_event)(StoreEvent::ThreadsWritten {
                                            version: index.version,
                                            error: None,
                                        });
                                    }
                                }
                                work.ticket.finish(result);
                                Ok(())
                            }
                            Err(error) => {
                                work.ticket.finish(purge::PurgeResult::Failed(format!(
                                    "存储操作失败：{error}"
                                )));
                                if matches!(error, StoreError::Superseded { .. }) {
                                    Err(error)
                                } else {
                                    Ok(())
                                }
                            }
                        }
                    }
                }
                Content::Json(payload) => disk.write_json(&name, payload),
                Content::Log(log) => {
                    let mut bytes = Vec::with_capacity(log.bytes + LOG_MARKER_RESERVE);
                    if log.dropped != 0 {
                        let timestamp = log
                            .lines
                            .front()
                            .and_then(|(_, line)| line.split_once(' ').map(|(ts, _)| ts))
                            .unwrap_or(&log.dropped_at);
                        bytes.extend_from_slice(
                            format!("{timestamp} - 丢行 丢了 {} 行\n", log.dropped).as_bytes(),
                        );
                    }
                    for (_, line) in &log.lines {
                        bytes.extend_from_slice(line.as_bytes());
                        bytes.push(b'\n');
                    }
                    disk.append_log(&name, &bytes)
                }
            };
            if name == "threads.json" {
                if let Content::Json(bytes) = &pending.content {
                    if let Ok(index) = serde_json::from_slice::<ThreadIndexFile>(bytes) {
                        (shared.on_event)(StoreEvent::ThreadsWritten {
                            version: index.version,
                            error: result.as_ref().err().map(ToString::to_string),
                        });
                    }
                }
            }
            if let Err(StoreError::Superseded { file }) = &result {
                shared.finish(Some(StoreEvent::Superseded { file: file.clone() }));
                return;
            }
            let mut state = shared.lock();
            if state.terminal {
                return;
            }
            state.active = None;
            if let Content::Purge(work) = &pending.content {
                if let Some(purge::PurgeResult::Finished(report)) = work.ticket.result() {
                    // 标记和逐项删除成功后才撤销旧重试；标记失败不能丢掉待写内容。
                    let deleted = purge::deleted_files(&report);
                    state.pending.retain(|name, older| {
                        older.order > pending.order
                            || (name != "threads.json" && !deleted.contains(name.as_str()))
                    });
                }
            }
            match result {
                Ok(()) => state.report.written += 1,
                Err(error) => {
                    if final_attempt {
                        state.lost_data = true;
                        state.report.abandoned.push(name.clone());
                    } else {
                        let mut pending = pending;
                        pending.dirty = true;
                        pending.retry_at = Instant::now() + pending.retry_delay;
                        pending.retry_delay = (pending.retry_delay * 2).min(RETRY_MAX);
                        if let Some(newer) = state.pending.remove(&name) {
                            pending.content = merge(pending.content, newer.content);
                            pending.order = newer.order;
                        }
                        state.pending.insert(name.clone(), pending);
                    }
                    let evicted = enforce_limits(&mut state);
                    drop(state);
                    tracing::error!(file = %name, %error, "文件写入失败，保留待重试或在关闭时放弃");
                    log_evicted(evicted);
                    shared.changed.notify_waiters();
                    continue;
                }
            }
            drop(state);
            shared.changed.notify_waiters();
            continue;
        }
        // 同一线程只有一个等待点，同时覆盖最近的重试与每日清理。
        let wake_at = state
            .pending
            .values()
            .map(|pending| pending.retry_at)
            .fold(cleanup_at, Instant::min);
        let _ = shared
            .wake
            .wait_timeout(state, wake_at.saturating_duration_since(Instant::now()))
            .unwrap_or_else(|poison| poison.into_inner());
    }
}

struct OpenGuard {
    shared: Arc<Shared>,
    armed: bool,
}
impl Drop for OpenGuard {
    fn drop(&mut self) {
        if self.armed {
            self.shared.finish(None);
        }
    }
}

pub(super) async fn open_with_timeout(
    config: StoreConfig,
    timeout: Duration,
) -> Result<(StoreHandle, Loaded), StoreError> {
    open_with_initializer(config, timeout, Disk::initialize).await
}

async fn open_with_initializer(
    mut config: StoreConfig,
    timeout: Duration,
    initialize: impl FnOnce(&mut StoreConfig, Gate) -> Result<(Disk, Loaded), StoreError>
        + Send
        + 'static,
) -> Result<(StoreHandle, Loaded), StoreError> {
    let deadline = Instant::now() + timeout;
    let cancelled = Arc::new(AtomicBool::new(false));
    let on_event = std::mem::replace(&mut config.on_event, Box::new(|_| {}));
    let shared = Arc::new(Shared {
        state: Mutex::new(State::default()),
        wake: Condvar::new(),
        changed: Notify::new(),
        cancelled: cancelled.clone(),
        on_event,
        #[cfg(test)]
        io_hook: Mutex::new(None),
    });
    let mut guard = OpenGuard {
        shared: shared.clone(),
        armed: true,
    };
    let (sender, receiver) = oneshot::channel();
    let background = shared.clone();
    std::thread::Builder::new()
        .name("cxb-store".into())
        .spawn(move || {
            let result = catch_unwind(AssertUnwindSafe(|| {
                let gate = Gate {
                    cancelled,
                    initialization: Some((deadline, timeout)),
                };
                match initialize(&mut config, gate) {
                    Ok((mut disk, loaded)) => {
                        background.lock().purged_files.extend(
                            loaded
                                .index
                                .purges
                                .values()
                                .flat_map(|plan| plan.files.iter().cloned()),
                        );
                        if disk.gate.check().is_err()
                            || sender.send(Ok((loaded, disk.prepared_log.take()))).is_err()
                        {
                            return;
                        }
                        disk.gate.initialization = None;
                        if disk.gate.check().is_ok() {
                            worker(&mut disk, &background);
                        }
                    }
                    Err(error) => {
                        let _ = sender.send(Err(error));
                    }
                }
            }));
            if result.is_err() {
                tracing::error!("存储写入线程意外退出");
                background.finish(Some(StoreEvent::WriterDied {
                    reason: "存储写入线程发生异常".into(),
                }));
            }
        })?;
    let (loaded, prepared_log) =
        match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), receiver).await {
            Ok(Ok(result)) => result?,
            Ok(Err(_)) => return Err(io::Error::other("存储初始化线程意外退出").into()),
            Err(_) => return Err(StoreError::Timeout(timeout)),
        };
    if Instant::now() >= deadline {
        return Err(StoreError::Timeout(timeout));
    }
    crate::logging::install_file(prepared_log);
    guard.armed = false;
    Ok((
        StoreHandle {
            owner: Arc::new(Owner { shared }),
        },
        loaded,
    ))
}

#[cfg(test)]
mod tests;
