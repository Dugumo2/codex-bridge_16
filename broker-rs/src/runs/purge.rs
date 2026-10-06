//! 删除前后均通过状态机：磁盘准备 → 终态复查 → 单写者删除 → 逐项回包。
use super::*;
use crate::claim::ClientRequest;
use crate::proto;
use crate::store::{
    purge::{PurgeResult, PurgeTicket, PurgeWork, RunHeader},
    StoreOp,
};
use serde_json::json;

const PURGE_DEADLINE_MS: u64 = 30_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PendingPurge {
    request: ClientRequest,
    work: PurgeWork,
    generation: u64,
    deadline: u64,
    before_index: Option<ThreadIndexFile>,
}

fn error(request: &ClientRequest, code: i64, reason: &str, actions: &mut Vec<Action>) {
    actions.push(Action::Send {
        conn: request.conn,
        message: proto::response_err(
            request.id.clone(),
            proto::error_object(
                code,
                if reason == "stopping" {
                    "停止尚未确认，请先执行 stop 核实".into()
                } else {
                    format!("无法删除线程运行记录：{reason}")
                },
                Some(json!({"reason":reason})),
            ),
        ),
    });
}

impl Machine {
    pub(crate) fn run_purge_busy(&self, thread: &str) -> bool {
        self.runs.purge.as_ref().is_some_and(|pending| {
            pending.work.plan.threads.contains(thread)
                || pending.work.plan.runs.values().any(|run| {
                    run.thread_id == thread || run.review_thread_id.as_deref() == Some(thread)
                })
        })
    }

    fn purge_rejection(&self, thread: &str) -> Option<&'static str> {
        if self.purge_guardian_pending(thread) {
            return Some("purgePending");
        }
        if self.runs.runs.values().any(|run| {
            (run.thread_id == thread || run.review_thread_id.as_deref() == Some(thread))
                && run.state == RunState::Lost
                && run.stop_outcome != Some(StopOutcome::Confirmed)
        }) {
            return Some("stopping");
        }
        if self.stop_busy(thread) {
            return Some("stopping");
        }
        if let Some(claim) = self.claims.get(thread) {
            return Some(if claim.settling() {
                "settling"
            } else {
                "threadBusy"
            });
        }
        if self.runs.thread_reads_pending(thread) {
            return Some("settling");
        }
        if self.purge_inflight.values().any(|target| target == thread)
            || self
                .stops
                .changes
                .values()
                .any(|change| change.thread == thread)
        {
            return Some("settling");
        }
        if self.runs.runs.values().any(|run| {
            (run.thread_id == thread || run.review_thread_id.as_deref() == Some(thread))
                && matches!(run.state, RunState::Running | RunState::Stopping)
        }) {
            return Some("threadBusy");
        }
        None
    }

    pub(crate) fn run_purge_request(
        &mut self,
        request: ClientRequest,
        thread: &str,
        actions: &mut Vec<Action>,
    ) {
        if self.runs.purge.is_some() {
            error(&request, -32001, "purgePending", actions);
            return;
        }
        if let Some(reason) = self.purge_rejection(thread) {
            error(&request, -32001, reason, actions);
            return;
        }
        let mut plan = self
            .runs
            .index
            .purges
            .get(thread)
            .cloned()
            .unwrap_or_default();
        plan.threads.insert(thread.into());
        for entry in self
            .runs
            .index
            .threads
            .values()
            .filter(|e| e.thread_id == thread)
        {
            plan.files.insert(format!("runs/{}.log", entry.short_id));
        }
        let generation = self.runs.generation();
        let work = PurgeWork {
            thread: thread.into(),
            plan,
            index: None,
            ticket: PurgeTicket::default(),
            seeds: self.runs.runs.values().map(RunHeader::from).collect(),
        };
        actions.push(Action::Store(StoreOp::Purge(work.clone())));
        self.runs.purge = Some(PendingPurge {
            request,
            work,
            generation,
            deadline: self.clock.mono_ms.saturating_add(PURGE_DEADLINE_MS),
            before_index: None,
        });
        state::schedule(thread, RunTimer::Purge { generation }, 10, actions);
    }

    pub(crate) fn run_purge_poll(&mut self, generation: u64, actions: &mut Vec<Action>) {
        let Some(mut pending) = self.runs.purge.take() else {
            return;
        };
        if pending.generation != generation {
            self.runs.purge = Some(pending);
            return;
        }
        let result = pending.work.ticket.result();
        if result.is_none() && self.clock.mono_ms >= pending.deadline {
            pending
                .work
                .ticket
                .finish(PurgeResult::Failed("删除操作超过期限".into()));
            error(&pending.request, -32000, "storageTimeout", actions);
            return;
        }
        match result {
            None => {}
            Some(PurgeResult::Busy(reason)) => {
                // 写入任务尚未删除任何文件；只恢复本次暂存的索引变更，不覆盖别的线程的新事实。
                if let Some(before) = pending.before_index {
                    for thread in &pending.work.plan.threads {
                        self.runs
                            .index
                            .threads
                            .retain(|_, entry| &entry.thread_id != thread);
                        self.runs.index.threads.extend(
                            before
                                .threads
                                .iter()
                                .filter(|(_, entry)| &entry.thread_id == thread)
                                .map(|(key, entry)| (key.clone(), entry.clone())),
                        );
                        if let Some(time) = before.removed.get(thread) {
                            self.runs.index.removed.insert(thread.clone(), *time);
                        } else {
                            self.runs.index.removed.remove(thread);
                        }
                    }
                    match before.purges.get(&pending.work.thread) {
                        Some(plan) => {
                            self.runs
                                .index
                                .purges
                                .insert(pending.work.thread.clone(), plan.clone());
                        }
                        None => {
                            self.runs.index.purges.remove(&pending.work.thread);
                        }
                    }
                    self.runs.put_threads(actions);
                }
                error(&pending.request, -32001, &reason, actions);
                return;
            }
            Some(PurgeResult::Failed(message)) => {
                actions.push(Action::Warn(message));
                error(&pending.request, -32000, "storageError", actions);
                return;
            }
            Some(PurgeResult::Prepared(mut plan)) => {
                // 磁盘旧状态可被当前账本更新，但不能漏查已淘汰或仅由子线程命中的运行。
                for (id, run) in &mut plan.runs {
                    if let Some(current) = self.runs.runs.get(id) {
                        run.state = Some(current.state);
                        run.stop_outcome = current.stop_outcome;
                    }
                    if run.state == Some(RunState::Lost)
                        && run.stop_outcome != Some(StopOutcome::Confirmed)
                    {
                        error(&pending.request, -32001, "stopping", actions);
                        return;
                    }
                    if !matches!(run.state, Some(RunState::Ended | RunState::Lost)) {
                        error(&pending.request, -32001, "threadBusy", actions);
                        return;
                    }
                    for thread in
                        std::iter::once(&run.thread_id).chain(run.review_thread_id.as_ref())
                    {
                        if let Some(reason) = self.purge_rejection(thread) {
                            error(&pending.request, -32001, reason, actions);
                            return;
                        }
                    }
                }
                // 扫描期间新出现的子线程必须复查，不能仅相信扫描前的父线程状态。
                pending.before_index = Some(self.runs.index.clone());
                for thread in &plan.threads {
                    if let Some(reason) = self.purge_rejection(thread) {
                        error(&pending.request, -32001, reason, actions);
                        return;
                    }
                }
                // 子线程有独立索引、尚无运行时也可能有日志。
                for entry in self
                    .runs
                    .index
                    .threads
                    .values()
                    .filter(|e| plan.threads.contains(&e.thread_id))
                {
                    plan.files.insert(format!("runs/{}.log", entry.short_id));
                }
                for thread in &plan.threads {
                    self.runs
                        .index
                        .threads
                        .retain(|_, e| &e.thread_id != thread);
                    self.runs
                        .index
                        .removed
                        .insert(thread.clone(), self.clock.now_ms);
                }
                crate::store::purge::include_rotated_logs(&mut plan);
                self.runs
                    .index
                    .purges
                    .insert(pending.work.thread.clone(), plan.clone());
                pending.work.plan = plan;
                pending.work.index = Some(self.runs.index.clone());
                pending.work.seeds.clear();
                pending.work.ticket = PurgeTicket::default();
                actions.push(Action::Store(StoreOp::Purge(pending.work.clone())));
            }
            Some(PurgeResult::Finished(report)) => {
                let deleted = crate::store::purge::deleted_files(&report);
                let removed: std::collections::HashSet<_> = self
                    .runs
                    .runs
                    .keys()
                    .filter(|id| deleted.contains(crate::store::run_file(id).as_str()))
                    .cloned()
                    .collect();
                self.runs.runs.retain(|id, _| !removed.contains(id));
                self.runs.active.retain(|id, _| !removed.contains(id));
                self.runs.updated.retain(|id, _| !removed.contains(id));
                self.runs
                    .retired
                    .retain(|(id, _)| !deleted.contains(crate::store::run_file(id).as_str()));
                self.runs.turn_runs.retain(|_, links| {
                    links.retain(|link| !removed.contains(&link.run));
                    !links.is_empty()
                });
                for record in self.records.values_mut() {
                    if record
                        .last_run
                        .as_ref()
                        .is_some_and(|id| removed.contains(id))
                    {
                        record.last_run = None;
                    }
                }
                actions.push(Action::Send {
                    conn: pending.request.conn,
                    message: proto::response_ok(pending.request.id, json!(report)),
                });
                return;
            }
        }
        state::schedule(
            &pending.work.thread,
            RunTimer::Purge { generation },
            10,
            actions,
        );
        self.runs.purge = Some(pending);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request() -> ClientRequest {
        ClientRequest {
            conn: 1,
            id: json!(1),
            method: "broker/threads/purge".into(),
            params: Some(json!({"threadId":"t"})),
        }
    }
    fn result(actions: &[Action]) -> Value {
        actions
            .iter()
            .find_map(|a| match a {
                Action::Send { message, .. } => Some(message.clone()),
                _ => None,
            })
            .unwrap()
    }
    #[test]
    fn 删除拒绝活动运行且准备后复查子线程() {
        for state in [RunState::Running, RunState::Stopping] {
            let mut m = Machine::new(false);
            let mut run = crate::store::tests::run("r", "t", 1);
            run.state = state;
            m.runs.runs.insert("r".into(), run);
            let mut actions = Vec::new();
            m.run_purge_request(request(), "t", &mut actions);
            assert_eq!(result(&actions)["error"]["code"], -32001);
            assert_eq!(result(&actions)["error"]["data"]["reason"], "threadBusy");
            assert!(!actions.iter().any(|a| matches!(a, Action::Store(_))));
        }
        let mut m = Machine::new(false);
        m.run_purge_request(request(), "t", &mut Vec::new());
        let pending = m.runs.purge.as_ref().unwrap();
        let generation = pending.generation;
        let plan = PurgePlan {
            threads: ["t".into(), "child".into()].into(),
            ..Default::default()
        };
        pending.work.ticket.finish(PurgeResult::Prepared(plan));
        let mut child = crate::store::tests::run("r", "child", 1);
        child.state = RunState::Running;
        m.runs.runs.insert("r".into(), child);
        let mut actions = Vec::new();
        m.run_purge_poll(generation, &mut actions);
        assert_eq!(result(&actions)["error"]["code"], -32001);
        assert!(m.runs.index.removed.is_empty());
    }

    #[test]
    fn 删除逐项清理内存并保留失败记录与标记() {
        let mut m = Machine::new(false);
        for id in ["a", "b"] {
            m.runs
                .runs
                .insert(id.into(), crate::store::tests::run(id, "t", 1));
        }
        m.run_purge_request(request(), "t", &mut Vec::new());
        let pending = m.runs.purge.as_ref().unwrap();
        let generation = pending.generation;
        pending.work.ticket.finish(PurgeResult::Prepared(PurgePlan {
            threads: ["t".into()].into(),
            files: ["runs/a.json".into(), "runs/b.json".into()].into(),
            runs: Default::default(),
        }));
        let mut actions = Vec::new();
        m.run_purge_poll(generation, &mut actions);
        assert!(m.runs.index.removed.contains_key("t"));
        assert!(m.run_purge_busy("t"));
        assert!(actions
            .iter()
            .any(|a| matches!(a, Action::Store(StoreOp::Purge(work)) if work.index.is_some())));
        let report = PurgeReport {
            thread_id: "t".into(),
            deleted: vec!["runs/a.json".into()],
            already_deleted: vec![],
            failed: vec![PurgeFailure {
                path: "runs/b.json".into(),
                reason: "文件被占用".into(),
            }],
        };
        m.runs
            .purge
            .as_ref()
            .unwrap()
            .work
            .ticket
            .finish(PurgeResult::Finished(report.clone()));
        actions.clear();
        m.run_purge_poll(generation, &mut actions);
        assert_eq!(result(&actions)["result"], json!(report));
        assert!(!m.runs.runs.contains_key("a"));
        assert!(m.runs.runs.contains_key("b"));
        assert!(!m.run_purge_busy("t"));
        m.run_purge_request(request(), "t", &mut Vec::new());
        assert_eq!(m.runs.purge.as_ref().unwrap().work.plan.files.len(), 2);
    }

    #[test]
    fn 删除超时拒绝等待并取消迟到任务() {
        let mut m = Machine::new(false);
        m.run_purge_request(request(), "t", &mut Vec::new());
        let pending = m.runs.purge.as_ref().unwrap().clone();
        m.clock.mono_ms = PURGE_DEADLINE_MS;
        let mut actions = Vec::new();
        m.run_purge_poll(pending.generation, &mut actions);
        assert_eq!(
            result(&actions)["error"]["data"]["reason"],
            "storageTimeout"
        );
        assert!(pending.work.ticket.result().is_some());
        assert!(!m.run_purge_busy("t"));
    }

    #[test]
    fn 删除拒绝认领停止与未定案并锁定新的开工请求() {
        for shared in [false, true] {
            let mut start = request();
            start.method = "turn/start".into();
            let (mut m, _) = crate::claim::step(
                Machine::new(shared),
                crate::claim::Event::ClientRequest {
                    request: start.clone(),
                    run_id: Some("run".into()),
                },
            );
            let mut actions = Vec::new();
            m.run_purge_request(request(), "t", &mut actions);
            assert_eq!(
                result(&actions)["error"]["data"]["reason"],
                if shared { "settling" } else { "threadBusy" }
            );
            m.begin_stop("t", StopReason::Stop, false, &mut Vec::new());
            actions.clear();
            m.run_purge_request(request(), "t", &mut actions);
            assert_eq!(result(&actions)["error"]["data"]["reason"], "stopping");
            let mut m = Machine::new(false);
            m.run_purge_request(request(), "t", &mut Vec::new());
            let (_, actions) = crate::claim::step(
                m,
                crate::claim::Event::ClientRequest {
                    request: start,
                    run_id: Some("new".into()),
                },
            );
            assert_eq!(result(&actions)["error"]["data"]["reason"], "purgePending");
            assert!(!actions.iter().any(|a| matches!(a, Action::Request(_))));
        }
    }
    #[test]
    fn 删除集合核实全部磁盘运行及其父线程在途写入() {
        for case in 0..3 {
            let mut m = Machine::new(false);
            m.run_purge_request(request(), "t", &mut Vec::new());
            let pending = m.runs.purge.as_ref().unwrap();
            let generation = pending.generation;
            let mut plan = PurgePlan {
                threads: ["t".into()].into(),
                files: ["runs/old.json".into()].into(),
                ..Default::default()
            };
            plan.runs.insert(
                "old".into(),
                PurgeRun {
                    stop_outcome: None,
                    thread_id: "parent".into(),
                    review_thread_id: Some("t".into()),
                    state: Some(if case == 0 {
                        RunState::Running
                    } else {
                        RunState::Ended
                    }),
                },
            );
            pending.work.ticket.finish(PurgeResult::Prepared(plan));
            if case == 1 {
                m.purge_inflight.insert(9, "parent".into());
            }
            let mut actions = Vec::new();
            m.run_purge_poll(generation, &mut actions);
            if case < 2 {
                assert_eq!(result(&actions)["error"]["code"], -32001);
                assert!(m.runs.index.removed.is_empty());
            } else {
                assert!(m.run_purge_busy("parent"));
                assert!(m.run_purge_busy("t"));
            }
        }
    }

    #[test]
    fn 写入任务拒绝删除时恢复目标索引但保留其他线程更新() {
        let mut m = Machine::new(false);
        m.runs.ensure_thread("t", 1, 1);
        let before = m.runs.index.clone();
        m.run_purge_request(request(), "t", &mut Vec::new());
        let pending = m.runs.purge.as_ref().unwrap();
        let generation = pending.generation;
        pending.work.ticket.finish(PurgeResult::Prepared(PurgePlan {
            threads: ["t".into()].into(),
            ..Default::default()
        }));
        m.run_purge_poll(generation, &mut Vec::new());
        m.runs.ensure_thread("other", 2, 2);
        m.runs
            .purge
            .as_ref()
            .unwrap()
            .work
            .ticket
            .finish(PurgeResult::Busy("settling".into()));
        let mut actions = Vec::new();
        m.run_purge_poll(generation, &mut actions);
        assert_eq!(result(&actions)["error"]["data"]["reason"], "settling");
        assert!(before
            .threads
            .iter()
            .all(|(id, e)| m.runs.index.threads.get(id) == Some(e)));
        assert!(m
            .runs
            .index
            .threads
            .values()
            .any(|e| e.thread_id == "other"));
        assert!(!m.runs.index.removed.contains_key("t"));
        assert!(!m.runs.index.purges.contains_key("t"));
    }
    #[test]
    fn 失联停止未确认内存及磁盘集合均拒绝() {
        for outcome in [
            None,
            Some(StopOutcome::Unconfirmed),
            Some(StopOutcome::Confirmed),
        ] {
            let mut m = Machine::new(false);
            let mut run = crate::store::tests::run("lost", "t", 1);
            run.state = RunState::Lost;
            run.stop_outcome = outcome;
            m.runs.runs.insert("lost".into(), run);
            assert_eq!(
                m.purge_rejection("t"),
                if outcome == Some(StopOutcome::Confirmed) {
                    None
                } else {
                    Some("stopping")
                }
            );
            m.runs.runs.clear();
            m.run_purge_request(request(), "t", &mut Vec::new());
            let pending = m.runs.purge.as_ref().unwrap();
            let generation = pending.generation;
            pending.work.ticket.finish(PurgeResult::Prepared(PurgePlan {
                threads: ["t".into()].into(),
                runs: [(
                    "lost".into(),
                    PurgeRun {
                        thread_id: "t".into(),
                        review_thread_id: None,
                        state: Some(RunState::Lost),
                        stop_outcome: outcome,
                    },
                )]
                .into(),
                ..Default::default()
            }));
            let mut actions = Vec::new();
            m.run_purge_poll(generation, &mut actions);
            if outcome != Some(StopOutcome::Confirmed) {
                assert_eq!(result(&actions)["error"]["data"]["reason"], "stopping");
                assert!(m.runs.index.removed.is_empty());
            } else {
                assert!(actions
                    .iter()
                    .any(|a| matches!(a, Action::Store(StoreOp::Purge(_)))));
            }
        }
    }
}
