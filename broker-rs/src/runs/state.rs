//! 账本内部有界登记、索引、进度与淘汰；所有时间均由调用方传入。

use super::*;
use crate::claim::{ClaimToken, TimerKind};
use crate::store::{StoreOp, RETENTION_MS};
use std::collections::HashSet;

pub const MAX_TURNS: usize = 100;
pub const MAX_RESULT_READS: usize = 16;
pub const MAX_RESULT_QUEUE: usize = 256;
const MAX_INDEX_PREVIEW_CHARS: usize = 200;
const MAX_INDEX_META_BYTES: usize = 4 * 1024;

pub(super) fn index_preview(preview: &str) -> String {
    preview.chars().take(MAX_INDEX_PREVIEW_CHARS).collect()
}

/// 只统计 JSON 序列化字节，不分配完整缓冲；超过预算立即结束序列化。
struct MetaBudget(usize);

impl std::io::Write for MetaBudget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_sub(bytes.len())
            .ok_or_else(|| std::io::Error::other("线程索引元数据序列化后超过 4 KiB"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) fn index_meta(meta: Option<&Value>, actions: &mut Vec<Action>) -> Option<Value> {
    let meta = meta?;
    if serde_json::to_writer(&mut MetaBudget(MAX_INDEX_META_BYTES), meta).is_ok() {
        Some(meta.clone())
    } else {
        actions.push(Action::Warn(
            "线程索引 meta 序列化后超过 4 KiB，已记为 null".into(),
        ));
        None
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum Phase {
    #[default]
    Open,
    Draining,
    Finalized,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct ActiveRun {
    /// 每个有效活动绑定最多一个完成标记；数量不超过该运行有效绑定数。
    /// 不独立淘汰，避免旧完成复活；随 ActiveRun 的普通释放、树提交及关闭一同回收。
    pub child_completions: HashSet<(String, String)>,
    pub current_turn: Option<(String, String)>,
    pub snapshot: crate::results::Snapshot,
    pub progress_generation: Option<u64>,
    pub completed_since_flush: u64,
    // 以事件先后而非毫秒大小判断，固定时钟或墙钟回拨不影响停止竞态。
    pub stop_started: bool,
    pub completed_after_stop: bool,
    pub goal_active_at_stop: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TurnLink {
    pub started: bool,
    pub turn: String,
    pub run: RunId,
    pub completed: bool,
}

#[allow(dead_code)]
impl Runs {
    pub(crate) fn superseded_turn(&self, run: &str, thread: &str, turn: &str) -> bool {
        self.runs.get(run).is_some_and(|record| {
            record
                .current_turn_id
                .as_deref()
                .is_some_and(|current| current != turn)
                && (record.turns.iter().any(|old| old.turn_id == turn)
                    || self.turn_runs.get(thread).is_some_and(|links| {
                        links.iter().any(|link| link.turn == turn && link.started)
                    }))
        })
    }

    pub(super) fn generation(&mut self) -> u64 {
        self.next_generation = self
            .next_generation
            .checked_add(1)
            .expect("运行登记代号已耗尽");
        self.next_generation
    }

    pub(crate) fn find_run(
        &self,
        thread: &str,
        turn: &str,
        fallback: Option<&str>,
    ) -> Option<String> {
        if let Some(link) = self
            .turn_runs
            .get(thread)
            .and_then(|links| links.iter().find(|l| l.turn == turn))
        {
            return self.runs.contains_key(&link.run).then(|| link.run.clone());
        }
        // 关联只有 16 项，仍保留的 100 轮结果也可证明旧轮归属，避免污染新认领。
        if let Some(record) = self.runs.values().find(|r| {
            (r.thread_id == thread || r.review_thread_id.as_deref() == Some(thread))
                && r.turns.iter().any(|t| t.turn_id == turn)
        }) {
            return Some(record.run_id.clone());
        }
        fallback
            .filter(|run| {
                self.runs.get(*run).is_some_and(|r| {
                    r.thread_id == thread || r.review_thread_id.as_deref() == Some(thread)
                })
            })
            .map(str::to_owned)
    }

    pub(super) fn link_turn(&mut self, run: &str, thread: &str, turn: &str, completed: bool) {
        let links = self.turn_runs.entry(thread.into()).or_default();
        if let Some(link) = links.iter_mut().find(|l| l.turn == turn) {
            if link.run == run {
                link.completed |= completed;
            }
            return;
        }
        links.push_back(TurnLink {
            started: false,
            turn: turn.into(),
            run: run.into(),
            completed,
        });
        while links.len() > TURN_RUNS_PER_THREAD {
            links.pop_front();
        }
    }

    pub(crate) fn ensure_thread(&mut self, thread: &str, now: u64, entropy: u64) -> String {
        if let Some(entry) = self
            .index
            .threads
            .values()
            .find(|entry| entry.thread_id == thread)
        {
            return entry.short_id.clone();
        }
        if let Some(short) = self.lifecycle_gates.get(thread) {
            return short.clone();
        }
        let mut candidate = (entropy ^ (entropy >> 32)) as u32;
        let short = loop {
            let short = format!("{candidate:08x}");
            // 内存中尚有旧运行时也不复用它的日志名。
            if !self.index.threads.contains_key(&short)
                && !self
                    .index
                    .purges
                    .values()
                    .any(|plan| plan.files.contains(&format!("runs/{short}.log")))
                && !self
                    .runs
                    .values()
                    .any(|r| r.short_id == short || r.review_short_id.as_ref() == Some(&short))
            {
                break short;
            }
            candidate = candidate.wrapping_add(0x9e37_79b9);
        };
        self.index.removed.remove(thread);
        self.index.threads.insert(
            short.clone(),
            ThreadIndexEntry {
                parent_thread_id: None,
                relation_kind: None,
                source_summary: None,
                can_accept_direct_input: None,
                agent_version: None,

                resume_overrides: None,
                thread_id: thread.into(),
                short_id: short.clone(),
                last_run_id: None,
                updated_at: now,
                model: None,
                preview: None,
                cwd: None,
                meta: None,
            },
        );
        short
    }

    pub(super) fn touch(&mut self, run: &str, now: u64, actions: &mut Vec<Action>) {
        self.updated.insert(run.into(), now);
        if self.phase != Phase::Open {
            return;
        }
        let Some(record) = self.runs.get(run) else {
            return;
        };
        let mut changed = false;
        for entry in self.index.threads.values_mut().filter(|entry| {
            entry.thread_id == record.thread_id
                || record.review_thread_id.as_ref() == Some(&entry.thread_id)
        }) {
            // 旧运行的迟到事实不能替换较新运行，活动时间仍按当前事实刷新。
            entry.updated_at = now;
            changed = true;
        }
        if changed {
            self.put_threads(actions);
        }
    }

    pub(crate) fn put(&self, run: &str, actions: &mut Vec<Action>) {
        if self.phase != Phase::Finalized {
            if let Some(record) = self.runs.get(run) {
                actions.push(Action::Store(StoreOp::PutRun(Box::new(record.clone()))));
            }
        }
    }

    pub(crate) fn put_threads(&mut self, actions: &mut Vec<Action>) {
        if self.phase == Phase::Open {
            self.index.version = self.index.version.checked_add(1).expect("索引版本已耗尽");
            actions.push(Action::Store(StoreOp::PutThreads(self.index.clone())));
        }
    }

    pub(super) fn progress_changed(
        &mut self,
        run: &str,
        completed: bool,
        actions: &mut Vec<Action>,
    ) {
        let Some(active) = self.active.get_mut(run) else {
            return;
        };
        if completed {
            active.completed_since_flush += 1;
        }
        if active.completed_since_flush >= PROGRESS_FLUSH_ITEMS {
            active.completed_since_flush = 0;
            active.progress_generation = None;
            self.put(run, actions);
        } else if active.progress_generation.is_none() {
            let generation = self.generation();
            self.active
                .get_mut(run)
                .expect("活动运行存在")
                .progress_generation = Some(generation);
            schedule(
                "",
                RunTimer::ProgressFlush {
                    run_id: run.into(),
                    generation,
                },
                PROGRESS_FLUSH_MS,
                actions,
            );
        }
    }

    pub(super) fn cleanup_index(&mut self, now: u64, actions: &mut Vec<Action>) {
        let before = (self.index.threads.len(), self.index.removed.len());
        self.index
            .threads
            .retain(|_, entry| now.saturating_sub(entry.updated_at) <= RETENTION_MS);
        self.index
            .removed
            .retain(|_, removed| now.saturating_sub(*removed) <= RETENTION_MS);
        self.index
            .purges
            .retain(|thread, _| self.index.removed.contains_key(thread));
        if before != (self.index.threads.len(), self.index.removed.len()) {
            self.put_threads(actions);
        }
        self.cleanup_goals();
    }

    fn cleanup_goals(&mut self) {
        self.goals.retain(|thread, _| {
            self.index.threads.values().any(|e| &e.thread_id == thread)
                || self.active.keys().any(|id| {
                    self.runs.get(id).is_some_and(|r| {
                        &r.thread_id == thread || r.review_thread_id.as_ref() == Some(thread)
                    })
                })
        });
    }

    pub(crate) fn evict(&mut self) {
        // 先找父线程的最近运行，再纳入所有索引引用，避免遗漏 review 子线程。
        let mut newest: HashMap<String, (u64, String)> = HashMap::new();
        for record in self.runs.values() {
            let key = (record.started_at, record.run_id.clone());
            let value = newest
                .entry(record.thread_id.clone())
                .or_insert_with(|| key.clone());
            if key > *value {
                *value = key;
            }
        }
        for entry in self.index.threads.values() {
            if let Some(record) = entry.last_run_id.as_ref().and_then(|id| self.runs.get(id)) {
                if record.thread_id == entry.thread_id {
                    newest.insert(
                        record.thread_id.clone(),
                        (record.started_at, record.run_id.clone()),
                    );
                }
            }
        }
        let latest: HashSet<_> = newest
            .into_values()
            .map(|(_, run)| run)
            .chain(
                self.index
                    .threads
                    .values()
                    .filter_map(|entry| entry.last_run_id.as_ref())
                    .filter(|run| self.runs.contains_key(*run))
                    .cloned(),
            )
            .collect();
        let protected: HashSet<_> =
            self.runs
                .values()
                .filter(|r| {
                    self.notices.iter().any(|n| match n {
                        RunNotice::TurnResult { run_id, .. }
                        | RunNotice::RunEnded { run_id, .. } => run_id == &r.run_id,
                    }) || self.diagnostic_dirty.contains_key(&r.run_id)
                        || self.tree_retention_required(&r.run_id)
                        || self.reconciliation.operations.values().any(|p| {
                            p.operation.token.run_id == r.run_id && p.operation.outcome_unknown
                        })
                        || self.verifying_history.contains(&r.run_id)
                        || self.inflight_starts.contains(&r.run_id)
                        || self.late_starts.contains(&r.run_id)
                        || matches!(r.state, RunState::Running | RunState::Stopping)
                        || self.reads.values().any(|read| {
                            read.read.run_id == r.run_id && read.phase != reads::ReadPhase::Done
                        })
                        || (r.state == RunState::Lost
                            && r.stop_outcome == Some(StopOutcome::Unconfirmed)
                            && latest.contains(&r.run_id))
                })
                .map(|r| r.run_id.clone())
                .collect();
        let mut recent: Vec<_> = self
            .runs
            .values()
            .filter(|r| !protected.contains(&r.run_id) && latest.contains(&r.run_id))
            .map(|r| {
                (
                    *self.updated.get(&r.run_id).unwrap_or(&r.started_at),
                    r.run_id.clone(),
                )
            })
            .collect();
        recent.sort_by(|a, b| b.cmp(a));
        let keep: HashSet<_> = recent
            .into_iter()
            .take(MAX_RECENT_RUNS)
            .map(|(_, id)| id)
            .chain(protected)
            .collect();
        let mut retired: Vec<_> = self
            .runs
            .values()
            .filter(|r| !keep.contains(&r.run_id))
            .map(|r| (r.started_at, r.run_id.clone(), r.thread_id.clone()))
            .collect();
        retired.sort();
        for (_, id, thread) in retired {
            self.retire(id, thread);
        }
        self.runs.retain(|id, _| keep.contains(id));
        self.updated.retain(|id, _| keep.contains(id));
        self.active.retain(|id, _| keep.contains(id));
        self.turn_runs.retain(|_, links| {
            links.retain(|link| keep.contains(&link.run));
            !links.is_empty()
        });
        self.cleanup_goals();
    }
}

pub(super) fn schedule(thread: &str, timer: RunTimer, delay_ms: u64, actions: &mut Vec<Action>) {
    actions.push(Action::Schedule {
        token: ClaimToken {
            thread_id: thread.into(),
            claim_id: 0,
        },
        kind: TimerKind::Run(timer),
        delay_ms,
    });
}

/// 公历日期换算，以 Unix 毫秒为唯一输入；按 400 年周期处理闰年。
pub(crate) fn utc_timestamp(now: u64) -> String {
    let seconds = now / 1000;
    let days = (seconds / 86400) as i64 + 719468;
    let era = days / 146097;
    let day_of_era = days - era * 146097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_part = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_part + 2) / 5 + 1;
    let month = month_part + if month_part < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        seconds / 3600 % 24,
        seconds / 60 % 60,
        seconds % 60,
        now % 1000
    )
}

impl super::Runs {
    /// SA-b：树未确认或仍有清理屏障时禁止淘汰（登记由树协调器在发起时加入、确认或取消时移除）。
    pub fn tree_retention_required(&self, run_id: &str) -> bool {
        self.tree_retained.contains(run_id)
    }
}
