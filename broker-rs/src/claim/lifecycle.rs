//! 与认领、停止并列的生命周期操作；磁盘和网络等待均由票据或事件推进。
use super::*;
use crate::runs::{RunState, StopOutcome};
use crate::store::{
    lifecycle::{
        self as disk, Attempt, File, Kind, Phase as DiskPhase, Record, ResultValue, Ticket, Work,
        WorkKind,
    },
    purge::RunHeader,
    StoreOp,
};
use std::collections::{BTreeMap, BTreeSet};

const STORAGE_MS: u64 = 10_000;
#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Scanning,
    Persisting,
    LocalPurging,
    Forwarded,
    OutcomeUnknown,
    Committing,
    Draining,
    Revoking,
}
impl Phase {
    fn name(self) -> &'static str {
        match self {
            Self::Scanning => "scanning",
            Self::Persisting => "persisting",
            Self::LocalPurging => "localPurging",
            Self::Forwarded => "forwarded",
            Self::OutcomeUnknown => "outcomeUnknown",
            Self::Committing => "committing",
            Self::Draining => "draining",
            Self::Revoking => "revoking",
        }
    }
}

fn visible_phase(op: &Pending) -> &'static str {
    if op.record.phase == DiskPhase::UpstreamDone && op.phase == Phase::Persisting {
        "upstreamDone"
    } else {
        op.phase.name()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WritePurpose {
    Intent,
    Facts,
    UpstreamDone,
    Revoke,
    Drain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Pending {
    record: Record,
    phase: Phase,
    gates: BTreeSet<String>,
    headers: Vec<RunHeader>,
    scan: Option<Ticket>,
    waiters: Vec<ClientRequest>,
    retry: bool,
    deadline: u64,
    write: Option<WritePurpose>,
    sent: BTreeSet<u64>,
    deleted: bool,
    result: Option<Result<Value, Value>>,
    already_done: bool,
    local_absent: bool,
    index_version: Option<u64>,
    error: Option<Value>,
    purge: bool,
    purge_report: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Writing {
    thread: String,
    purpose: WritePurpose,
    file: File,
    ticket: Ticket,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HistoryStop {
    request: ClientRequest,
    run: String,
    ticket: Ticket,
    deadline: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Lifecycle {
    pub stored_version: u64,
    pub storage_error: Option<String>,
    pub degraded: Option<String>,
    pub instance: Option<disk::Instance>,
    operations: BTreeMap<String, Pending>,
    file: File,
    writing: Option<Writing>,
    next_version: u64,
    next_operation: u64,
    timer: bool,
    history: Vec<HistoryStop>,
}

fn response(request: &ClientRequest, result: Result<Value, Value>, actions: &mut Vec<Action>) {
    actions.push(Action::Send {
        conn: request.conn,
        message: match result {
            Ok(value) => proto::response_ok(request.id.clone(), value),
            Err(error) => proto::response_err(request.id.clone(), error),
        },
    });
}
fn failure(reason: &str, op: Option<&Pending>, data: Value) -> Value {
    let mut data = data.as_object().cloned().unwrap_or_default();
    data.insert("reason".into(), json!(reason));
    if let Some(op) = op {
        data.insert("operationId".into(), json!(op.record.operation_id));
    }
    proto::error_object(
        if matches!(reason, "threadBusy" | "lifecyclePending") {
            -32001
        } else {
            -32000
        },
        match reason {
            "threadBusy" => "关联线程仍有未结束的工作",
            "lifecyclePending" => "线程生命周期操作尚未完成",
            "outcomeUnknown" => "上游请求的结果尚未核实",
            "storageTimeout" => "存储确认超过期限",
            "shuttingDown" => "broker 正在关闭",
            _ => "生命周期存储操作失败",
        },
        Some(Value::Object(data)),
    )
}
fn settle(op: &mut Pending, result: Result<Value, Value>, actions: &mut Vec<Action>) {
    for waiter in std::mem::take(&mut op.waiters) {
        let result = if waiter.method == "broker/threads/purge"
            && result.as_ref().err().is_some_and(|e| {
                matches!(
                    e["data"]["reason"].as_str(),
                    Some("outcomeUnknown" | "upstreamRejected")
                )
            }) {
            let error = result.as_ref().unwrap_err();
            let reason = if error["data"]["reason"] == "outcomeUnknown" {
                "outcomeUnknown".to_owned()
            } else if error["code"] == -32601 {
                "此 Codex 版本不支持 thread/delete，服务器线程保留".into()
            } else {
                error["message"]
                    .as_str()
                    .unwrap_or("服务器拒绝删除线程")
                    .to_owned()
            };
            let mut report=op.purge_report.clone().unwrap_or(json!({"threadId":op.record.thread_id,"deleted":[],"alreadyDeleted":[],"failed":[]}));
            report["operationId"] = json!(op.record.operation_id);
            report["failed"]
                .as_array_mut()
                .unwrap()
                .push(json!({"path":"server-thread","reason":reason}));
            Ok(report)
        } else {
            result.clone()
        };
        response(&waiter, result, actions);
    }
}
fn storage_failure(op: &mut Pending, timeout: bool, actions: &mut Vec<Action>) {
    let err = failure(
        if timeout {
            "storageTimeout"
        } else {
            "storageError"
        },
        Some(op),
        json!({"upstreamDone": op.record.phase == DiskPhase::UpstreamDone}),
    );
    settle(op, Err(err), actions);
}

/// 仅真实上游 RPC 错误进入这里；精确比较方法、错误码及完整线程编号。
pub(crate) fn already_done(kind: Kind, thread: &str, error: &Value) -> bool {
    let code = error["code"].as_i64();
    let message = error["message"].as_str().unwrap_or("");
    if code == Some(-32600) {
        return [
            format!("thread not found: {thread}"),
            format!("no rollout found for thread id {thread}"),
            format!("no rollout found for conversation id {thread}"),
        ]
        .contains(&message.to_owned())
            || kind == Kind::Archive && message == format!("thread {thread} is archived");
    }
    kind == Kind::Archive
        && code == Some(-32603)
        && message == format!("failed to archive session: thread {thread} not found")
}

fn closure(thread: &str, headers: &[RunHeader]) -> BTreeSet<String> {
    let mut set = BTreeSet::from([thread.into()]);
    loop {
        let before = set.len();
        for h in headers {
            if set.contains(&h.thread_id)
                || h.review_thread_id.as_ref().is_some_and(|c| set.contains(c))
            {
                set.insert(h.thread_id.clone());
                set.extend(h.review_thread_id.clone());
            }
        }
        if before == set.len() {
            return set;
        }
    }
}

impl Machine {
    pub(crate) fn lifecycle_history_stop(
        &mut self,
        request: ClientRequest,
        run: String,
        actions: &mut Vec<Action>,
    ) {
        let ticket = Ticket::default();
        actions.push(Action::Store(StoreOp::Lifecycle(Work {
            kind: WorkKind::ReadRun(run.clone()),
            ticket: ticket.clone(),
        })));
        self.lifecycle.history.push(HistoryStop {
            request,
            run,
            ticket,
            deadline: self.clock.mono_ms + STORAGE_MS,
        });
    }
    pub(crate) fn lifecycle_review_moved(
        &mut self,
        parent: &str,
        child: &str,
        actions: &mut Vec<Action>,
    ) {
        let run = self.claims.get(child).and_then(|c| c.run_id.clone());
        let blocker = json!({"threadId":child,"runId":run,"reason":"running"});
        self.lifecycle_observe_blocker(parent, false, Some(blocker.clone()), actions);
        self.lifecycle_observe_blocker(child, false, Some(blocker), actions);
    }
    pub(crate) fn lifecycle_instance_exited(&mut self, instance: &disk::Instance) {
        for op in self.lifecycle.operations.values_mut() {
            for attempt in &mut op.record.attempts {
                if attempt.server_kind == "private" && attempt.instance.as_ref() == Some(instance) {
                    attempt.settled = true;
                }
            }
        }
    }
    pub(crate) fn lifecycle_restore(&mut self, loaded: Result<File, String>) {
        let file = match loaded {
            Ok(file) => file,
            Err(error) => {
                self.lifecycle.degraded = Some(error);
                return;
            }
        };
        self.lifecycle.file = file.clone();
        for (thread, record) in file.records {
            let phase = match record.phase {
                DiskPhase::LocalPurging => Phase::LocalPurging,
                DiskPhase::Forwarded => Phase::OutcomeUnknown,
                DiskPhase::UpstreamDone => Phase::Persisting,
            };
            let write =
                (record.phase == DiskPhase::UpstreamDone).then_some(WritePurpose::UpstreamDone);
            let sent = record.attempts.iter().map(|a| a.attempt).collect();
            self.lifecycle.operations.insert(
                thread.clone(),
                Pending {
                    record,
                    phase,
                    gates: BTreeSet::from([thread]),
                    headers: vec![],
                    scan: None,
                    waiters: vec![],
                    retry: false,
                    deadline: self.clock.mono_ms + STORAGE_MS,
                    write,
                    sent,
                    deleted: false,
                    result: None,
                    already_done: false,
                    local_absent: false,
                    index_version: None,
                    error: None,
                    purge: phase == Phase::LocalPurging,
                    purge_report: None,
                },
            );
        }
        self.lifecycle_recompute_gates();
    }
    pub(crate) fn lifecycle_recompute_gates(&mut self) {
        let headers = self.lifecycle_headers(&[]);
        let threads: Vec<_> = self.lifecycle.operations.keys().cloned().collect();
        for thread in threads {
            if let Some(op) = self.lifecycle.operations.get_mut(&thread) {
                op.headers = headers.clone();
                op.gates = closure(&op.record.thread_id, &op.headers);
            }
        }
        self.lifecycle_sync_gates();
    }
    pub(crate) fn lifecycle_threads(&self) -> impl Iterator<Item = &String> {
        self.lifecycle.operations.keys()
    }
    pub(crate) fn lifecycle_record(&self, thread: &str) -> Option<&Record> {
        self.lifecycle.operations.get(thread).map(|o| &o.record)
    }
    pub(crate) fn lifecycle_only(&self, thread: &str) -> bool {
        self.lifecycle_record(thread).is_some()
            && !self
                .runs
                .index
                .threads
                .values()
                .any(|e| e.thread_id == thread)
    }
    pub(crate) fn lifecycle_busy(&self) -> bool {
        !self.lifecycle.operations.is_empty()
    }
    pub(crate) fn lifecycle_owns_local_purge(&self, thread: &str) -> bool {
        self.lifecycle
            .operations
            .values()
            .any(|op| op.phase == Phase::LocalPurging && op.gates.contains(thread))
    }
    pub(crate) fn threads_written(&mut self, version: u64, error: Option<String>) {
        if error.is_none() {
            self.lifecycle.stored_version = self.lifecycle.stored_version.max(version);
        }
        self.lifecycle.storage_error = error;
    }

    pub(crate) fn lifecycle_error(&self, thread: &str) -> Option<Value> {
        if let Some(reason) = &self.lifecycle.degraded {
            return Some(failure(
                "storageError",
                None,
                json!({"upstreamDone":null,"detail":reason}),
            ));
        }
        self.lifecycle
            .operations
            .values()
            .find(|op| op.gates.contains(thread))
            .map(|op| {
                failure(
                    "lifecyclePending",
                    Some(op),
                    json!({"threadId":thread,"kind":op.record.kind,"phase":visible_phase(op)}),
                )
            })
    }

    pub(crate) fn lifecycle_gate(
        &self,
        request: &ClientRequest,
        thread: &str,
        actions: &mut Vec<Action>,
    ) -> bool {
        if let Some(err) = self.lifecycle_error(thread) {
            response(request, Err(err), actions);
            true
        } else {
            false
        }
    }

    pub(crate) fn lifecycle_projection(&self, thread: &str) -> Option<Value> {
        self.lifecycle.operations.get(thread).map(|op| json!({"operationId":op.record.operation_id,
            "kind":op.record.kind,"phase":visible_phase(op),"shortId":op.record.short_id,"preview":op.record.preview}))
    }

    fn lifecycle_headers(&self, headers: &[RunHeader]) -> Vec<RunHeader> {
        let mut all: BTreeMap<_, _> = headers
            .iter()
            .map(|h| (h.run_id.clone(), h.clone()))
            .collect();
        for run in self.runs.runs.values() {
            all.insert(run.run_id.clone(), RunHeader::from(run));
        }
        all.into_values().collect()
    }

    fn lifecycle_blockers(&self, gates: &BTreeSet<String>, headers: &[RunHeader]) -> Vec<Value> {
        let mut blockers = Vec::new();
        for h in self.lifecycle_headers(headers) {
            if !gates.contains(&h.thread_id)
                && !h
                    .review_thread_id
                    .as_ref()
                    .is_some_and(|c| gates.contains(c))
            {
                continue;
            }
            let reason = match h.state {
                Some(RunState::Running) => Some("running"),
                Some(RunState::Stopping) => Some("stopping"),
                Some(RunState::Lost) if h.stop_outcome != Some(StopOutcome::Confirmed) => {
                    Some("lostUnconfirmed")
                }
                None => Some("lostUnconfirmed"),
                _ => None,
            };
            if let Some(reason) = reason {
                blockers.push(json!({"threadId":h.review_thread_id.as_ref().unwrap_or(&h.thread_id),"runId":h.run_id,"reason":reason}));
            }
        }
        for thread in gates {
            let run = self.claims.get(thread).and_then(|c| c.run_id.clone());
            let mut add = |reason: &str| {
                blockers.push(json!({"threadId":thread,"runId":run,"reason":reason}))
            };
            if let Some(c) = self.claims.get(thread) {
                add(if c.settling() {
                    "settling"
                } else if c.joined() {
                    "joined"
                } else {
                    "running"
                });
            }
            if self.stop_busy(thread) {
                add("stopInProgress");
            }
            if self.runs.thread_reads_pending(thread)
                || self.purge_inflight.values().any(|t| t == thread)
                || self.stops.changes.values().any(|c| &c.thread == thread)
            {
                add("reading");
            }
            if self.run_purge_busy(thread) {
                add("purgePending");
            }
            if self.purge_guardian_pending(thread) {
                add("guardianPending");
            }
        }
        blockers.sort_by_key(Value::to_string);
        blockers.dedup();
        blockers
    }

    fn lifecycle_sync_gates(&mut self) {
        self.runs.lifecycle_gates = self
            .lifecycle
            .operations
            .values()
            .flat_map(|op| {
                op.gates
                    .iter()
                    .map(|thread| (thread.clone(), op.record.short_id.clone()))
            })
            .collect();
    }

    fn lifecycle_scan(&mut self, op: &mut Pending, actions: &mut Vec<Action>) {
        op.headers = self.lifecycle_headers(&op.headers);
        op.gates = closure(&op.record.thread_id, &op.headers);
        op.phase = Phase::Scanning;
        op.deadline = self.clock.mono_ms + STORAGE_MS;
        let ticket = Ticket::default();
        actions.push(Action::Store(StoreOp::Lifecycle(Work {
            kind: WorkKind::Scan {
                seeds: self.lifecycle_headers(&[]),
            },
            ticket: ticket.clone(),
        })));
        op.scan = Some(ticket);
    }

    pub(crate) fn lifecycle_request(
        &mut self,
        request: ClientRequest,
        kind: Kind,
        purge: bool,
        actions: &mut Vec<Action>,
    ) {
        let Some(thread) = thread_id(&request.params)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
        else {
            response(
                &request,
                Err(proto::error_object(-32602, "必须提供非空 threadId", None)),
                actions,
            );
            return;
        };
        if self.closing || self.quiescing {
            response(
                &request,
                Err(failure("shuttingDown", None, json!({}))),
                actions,
            );
            return;
        }
        if self.lifecycle.degraded.is_some() {
            response(
                &request,
                Err(self.lifecycle_error(&thread).unwrap()),
                actions,
            );
            return;
        }
        if let Some(mut op) = self.lifecycle.operations.remove(&thread) {
            if op.record.kind != kind
                || !(matches!(
                    op.phase,
                    Phase::OutcomeUnknown
                        | Phase::Committing
                        | Phase::Draining
                        | Phase::LocalPurging
                ) || op.record.phase == DiskPhase::UpstreamDone)
            {
                let err = failure(
                    "lifecyclePending",
                    Some(&op),
                    json!({"threadId":thread,"kind":op.record.kind,"phase":visible_phase(&op)}),
                );
                response(&request, Err(err), actions);
            } else {
                op.purge |= purge;
                op.waiters.push(request);
                if matches!(op.phase, Phase::OutcomeUnknown | Phase::LocalPurging)
                    && !self.run_purge_busy(&thread)
                    && op.write.is_none()
                    && !self
                        .lifecycle
                        .writing
                        .as_ref()
                        .is_some_and(|w| w.thread == thread && w.purpose != WritePurpose::Facts)
                {
                    op.retry = true;
                    op.purge |= purge;
                    let gates = closure(&thread, &self.lifecycle_headers(&op.headers));
                    if let Some(error) = gates.iter().find_map(|t| self.lifecycle_error(t)) {
                        self.lifecycle_pre_send_failure(&mut op, error, actions);
                    } else {
                        self.lifecycle_scan(&mut op, actions);
                    }
                } else if op.phase == Phase::Draining {
                    Self::lifecycle_success(&mut op, actions);
                }
            }
            self.lifecycle.operations.insert(thread, op);
            self.lifecycle_sync_gates();
            return;
        }
        let headers = self.lifecycle_headers(&[]);
        let gates = closure(&thread, &headers);
        for target in &gates {
            if self.lifecycle_gate(&request, target, actions) {
                return;
            }
        }
        let blockers = self.lifecycle_blockers(&gates, &headers);
        if !blockers.is_empty() {
            response(
                &request,
                Err(failure("threadBusy", None, json!({"blockers":blockers}))),
                actions,
            );
            return;
        }
        self.lifecycle.next_operation += 1;
        let index = self
            .runs
            .index
            .threads
            .values()
            .find(|e| e.thread_id == thread);
        let mut op = Pending {
            record: Record {
                operation_id: format!(
                    "lifecycle-{:x}-{:x}-{:x}",
                    self.clock.now_ms, self.clock.entropy, self.lifecycle.next_operation
                ),
                kind,
                thread_id: thread.clone(),
                short_id: index
                    .map(|e| e.short_id.clone())
                    .unwrap_or_else(|| format!("{:08x}", self.clock.entropy as u32)),
                preview: index.and_then(|e| e.preview.clone()),
                phase: if purge {
                    DiskPhase::LocalPurging
                } else {
                    DiskPhase::Forwarded
                },
                attempts: vec![],
                created_at: self.clock.now_ms,
                updated_at: self.clock.now_ms,
            },
            phase: Phase::Scanning,
            gates,
            headers,
            scan: None,
            waiters: vec![request],
            retry: false,
            deadline: 0,
            write: None,
            sent: BTreeSet::new(),
            deleted: false,
            result: None,
            already_done: false,
            local_absent: index.is_none(),
            index_version: None,
            error: None,
            purge,
            purge_report: None,
        };
        self.lifecycle_scan(&mut op, actions);
        self.lifecycle.operations.insert(thread, op);
        self.lifecycle_sync_gates();
    }

    fn lifecycle_pre_send_failure(
        &mut self,
        op: &mut Pending,
        error: Value,
        actions: &mut Vec<Action>,
    ) -> bool {
        if op.retry {
            if let Some(attempt) = op.record.attempts.last_mut() {
                if !op.sent.contains(&attempt.attempt) {
                    attempt.settled = true;
                }
            }
            op.phase = if op.record.phase == DiskPhase::LocalPurging {
                Phase::LocalPurging
            } else {
                Phase::OutcomeUnknown
            };
            op.gates = BTreeSet::from([op.record.thread_id.clone()]);
            op.write = None;
            // 未发出的尝试也必须以新版本结算，不能让重启凭旧快照虚构在途请求。
            if self
                .lifecycle
                .file
                .records
                .contains_key(&op.record.thread_id)
                && !self
                    .lifecycle
                    .writing
                    .as_ref()
                    .is_some_and(|w| w.thread == op.record.thread_id)
            {
                op.write = Some(WritePurpose::Facts);
            }
            settle(op, Err(error), actions);
            false
        } else if self
            .lifecycle
            .file
            .records
            .contains_key(&op.record.thread_id)
        {
            op.error = Some(error);
            op.phase = Phase::Revoking;
            op.write = Some(WritePurpose::Revoke);
            op.deadline = self.clock.mono_ms + STORAGE_MS;
            false
        } else {
            settle(op, Err(error), actions);
            true
        }
    }

    fn lifecycle_prepare_attempt(&self, op: &mut Pending) {
        let attempt = op.record.attempts.last().map_or(1, |a| a.attempt + 1);
        op.record.phase = DiskPhase::Forwarded;
        op.record.attempts.push(Attempt {
            attempt,
            settled: false,
            server_kind: if self.shared { "shared" } else { "private" }.into(),
            instance: self.lifecycle.instance.clone(),
        });
    }

    fn lifecycle_send(&mut self, op: &mut Pending, actions: &mut Vec<Action>) -> bool {
        let blockers = self.lifecycle_blockers(&op.gates, &op.headers);
        if !blockers.is_empty() {
            let err = failure("threadBusy", Some(op), json!({"blockers":blockers}));
            return self.lifecycle_pre_send_failure(op, err, actions);
        }
        if op.record.phase == DiskPhase::LocalPurging {
            op.phase = Phase::LocalPurging;
            let request = ClientRequest {
                conn: u64::MAX,
                id: json!(op.record.operation_id),
                method: "broker/threads/purge".into(),
                params: Some(json!({"threadId":op.record.thread_id})),
            };
            self.run_purge_request(request, &op.record.thread_id, actions);
            return false;
        }
        let attempt = op
            .record
            .attempts
            .last()
            .expect("发送前已经保存尝试")
            .attempt;
        op.sent.insert(attempt);
        op.phase = Phase::Forwarded;
        op.retry = false;
        actions.push(Action::Request(RequestContext {
            resume_retried: false,
            stream_id: None,
            review_id: None,
            client: None,
            token: None,
            operation: Operation::Lifecycle {
                thread: op.record.thread_id.clone(),
                operation: op.record.operation_id.clone(),
                attempt,
            },
            method: if op.record.kind == Kind::Archive {
                "thread/archive"
            } else {
                "thread/delete"
            }
            .into(),
            params: Some(json!({"threadId":op.record.thread_id})),
        }));
        false
    }

    pub(crate) fn lifecycle_reply(
        &mut self,
        thread: &str,
        operation: &str,
        attempt: u64,
        result: Result<Value, Value>,
        known: bool,
        actions: &mut Vec<Action>,
    ) {
        let Some(mut op) = self.lifecycle.operations.remove(thread) else {
            return;
        };
        if op.record.operation_id != operation || self.closing {
            self.lifecycle.operations.insert(thread.into(), op);
            return;
        }
        if let Some(a) = op.record.attempts.iter_mut().find(|a| a.attempt == attempt) {
            if a.settled {
                self.lifecycle.operations.insert(thread.into(), op);
                return;
            }
            a.settled = known;
        }
        if !known {
            if op.record.phase != DiskPhase::UpstreamDone {
                op.phase = Phase::OutcomeUnknown;
                op.gates = BTreeSet::from([thread.into()]);
                let error = failure("outcomeUnknown", Some(&op), json!({}));
                settle(&mut op, Err(error), actions);
            }
        } else if result.is_ok()
            || op.deleted
            || result
                .as_ref()
                .err()
                .is_some_and(|e| already_done(op.record.kind, thread, e))
        {
            if op.record.phase != DiskPhase::UpstreamDone {
                op.already_done = result.is_err();
                op.result = Some(result);
                op.record.phase = DiskPhase::UpstreamDone;
                op.phase = Phase::Persisting;
                op.write = Some(WritePurpose::UpstreamDone);
                op.deadline = self.clock.mono_ms + STORAGE_MS;
            }
        } else if op.record.phase != DiskPhase::UpstreamDone {
            if op.record.attempts.iter().all(|a| a.settled) {
                let mut error = result.unwrap_err();
                let upstream = json!({"code":error["code"],"message":error["message"]});
                error["data"] = json!({"reason":"upstreamRejected","upstream":upstream});
                op.error = Some(error);
                op.phase = Phase::Revoking;
                op.write = Some(WritePurpose::Revoke);
                op.deadline = self.clock.mono_ms + STORAGE_MS;
            } else {
                op.phase = Phase::OutcomeUnknown;
                op.gates = BTreeSet::from([thread.into()]);
                let error = failure("outcomeUnknown", Some(&op), json!({}));
                settle(&mut op, Err(error), actions);
            }
        }
        self.lifecycle.operations.insert(thread.into(), op);
        if let Some(op) = self.lifecycle.operations.get_mut(thread) {
            if known
                && op.write.is_none()
                && !(op.phase == Phase::Draining && op.record.attempts.iter().all(|a| a.settled))
                && !self
                    .lifecycle
                    .writing
                    .as_ref()
                    .is_some_and(|w| w.thread == thread)
            {
                op.write = Some(WritePurpose::Facts);
            }
        }
        self.lifecycle_sync_gates();
    }

    fn lifecycle_success(op: &mut Pending, actions: &mut Vec<Action>) {
        for waiter in std::mem::take(&mut op.waiters) {
            let result = if waiter.method.starts_with("thread/") {
                op.result.clone().unwrap_or(Ok(json!({})))
            } else if op.purge {
                let mut report=op.purge_report.clone().unwrap_or(json!({"threadId":op.record.thread_id,"deleted":[],"alreadyDeleted":[],"failed":[]}));
                report["operationId"] = json!(op.record.operation_id);
                report[if op.already_done {
                    "alreadyDeleted"
                } else {
                    "deleted"
                }]
                .as_array_mut()
                .unwrap()
                .push(json!("server-thread"));
                Ok(report)
            } else {
                Ok(
                    json!({"threadId":op.record.thread_id,"operationId":op.record.operation_id,
                "archived":if op.already_done {json!("alreadyDone")} else {json!(true)},"localIndex":if op.local_absent {"alreadyAbsent"} else {"removed"}}),
                )
            };
            response(&waiter, result, actions);
        }
    }

    pub(crate) fn lifecycle_observe(
        &mut self,
        thread: &str,
        deleted: bool,
        actions: &mut Vec<Action>,
    ) {
        self.lifecycle_observe_blocker(thread, deleted, None, actions);
    }

    fn lifecycle_observe_blocker(
        &mut self,
        thread: &str,
        deleted: bool,
        blocker: Option<Value>,
        actions: &mut Vec<Action>,
    ) {
        let targets: Vec<_> = self
            .lifecycle
            .operations
            .iter()
            .filter(|(_, op)| op.gates.contains(thread))
            .map(|(t, _)| t.clone())
            .collect();
        for target in targets {
            let mut op = self.lifecycle.operations.remove(&target).unwrap();
            if deleted {
                if thread == target {
                    op.deleted = true;
                }
            } else if matches!(op.phase, Phase::Scanning | Phase::Persisting)
                && (op.sent.is_empty() || op.retry)
            {
                let error = failure(
                    "threadBusy",
                    Some(&op),
                    json!({"blockers":[blocker.clone().unwrap_or_else(||json!({"threadId":thread,"runId":null,"reason":"externalTurn"}))]}),
                );
                // 写入尚未结算时先保存中止原因，待结局明确后再撤销。
                if self
                    .lifecycle
                    .writing
                    .as_ref()
                    .is_some_and(|w| w.thread == target)
                {
                    op.error = Some(error);
                } else if self.lifecycle_pre_send_failure(&mut op, error, actions) {
                    continue;
                }
            } else {
                actions.push(Action::Warn(
                    "生命周期门禁期间观察到外部开轮，保留上游操作结局".into(),
                ));
            }
            self.lifecycle.operations.insert(target, op);
        }
        self.lifecycle_sync_gates();
    }

    pub(crate) fn lifecycle_disconnect(&mut self, conn: ConnId) {
        for op in self.lifecycle.operations.values_mut() {
            op.waiters.retain(|w| w.conn != conn);
        }
        self.lifecycle.history.retain(|h| h.request.conn != conn);
    }
    pub(crate) fn lifecycle_cancel(&mut self, conn: ConnId, id: &Value) {
        for op in self.lifecycle.operations.values_mut() {
            op.waiters.retain(|w| w.conn != conn || &w.id != id);
        }
        self.lifecycle
            .history
            .retain(|h| h.request.conn != conn || &h.request.id != id);
    }

    pub(crate) fn lifecycle_drive(&mut self, actions: &mut Vec<Action>) {
        // SA-b：rm、purge 的子树发现检查有结论时，先判定再交给下面的生命周期推进。
        self.subagent_lifecycle_poll(actions);
        if self.closing || self.quiescing {
            for pending in std::mem::take(&mut self.lifecycle.history) {
                response(
                    &pending.request,
                    Err(failure("shuttingDown", None, json!({}))),
                    actions,
                );
            }
            for op in self.lifecycle.operations.values_mut() {
                let err = failure("shuttingDown", Some(op), json!({}));
                settle(op, Err(err), actions);
            }
            return;
        }
        // 本地 purge 的内部回包只交回同一操作，不经过任何客户端连接。
        let mut local = Vec::new();
        actions.retain(|a| {
            if let Action::Send { conn, message } = a {
                if *conn == u64::MAX {
                    local.push(message.clone());
                    return false;
                }
            }
            true
        });
        for message in local {
            if let Some(op) = self
                .lifecycle
                .operations
                .values_mut()
                .find(|op| message["id"] == op.record.operation_id)
            {
                if message.get("error").is_some()
                    || message["result"]["failed"]
                        .as_array()
                        .is_some_and(|a| !a.is_empty())
                {
                    let mut report=message.get("result").cloned().unwrap_or(json!({"threadId":op.record.thread_id,"deleted":[],"alreadyDeleted":[],"failed":[]}));
                    report["failed"].as_array_mut().unwrap().push(
                        json!({"path":"server-thread","reason":"本地删除未完成，未删除服务器线程"}),
                    );
                    report["operationId"] = json!(op.record.operation_id);
                    settle(op, Ok(report), actions);
                } else {
                    op.purge_report = Some(message["result"].clone());
                    op.phase = Phase::Persisting;
                    op.write = Some(WritePurpose::Intent);
                    op.deadline = self.clock.mono_ms + STORAGE_MS;
                }
            }
        }
        if let Some(writing) = self.lifecycle.writing.take() {
            if let Some(result) = writing.ticket.result() {
                let success = matches!(result, ResultValue::Written(_));
                if success {
                    self.lifecycle.file = writing.file;
                }
                if let Some(mut op) = self.lifecycle.operations.remove(&writing.thread) {
                    let mut remove = false;
                    if success {
                        match writing.purpose {
                            WritePurpose::Facts => {}
                            WritePurpose::Intent => {
                                if let Some(error) = op.error.take() {
                                    remove =
                                        self.lifecycle_pre_send_failure(&mut op, error, actions);
                                } else if op.record.phase == DiskPhase::UpstreamDone {
                                    op.write = Some(WritePurpose::UpstreamDone);
                                } else if op.retry && op.phase == Phase::OutcomeUnknown {
                                    if let Some(a) = op.record.attempts.last_mut() {
                                        a.settled = true;
                                    }
                                    op.write = Some(WritePurpose::Facts);
                                } else {
                                    remove = self.lifecycle_send(&mut op, actions);
                                }
                            }
                            WritePurpose::UpstreamDone => {
                                self.run_forget_thread(&writing.thread, actions);
                                op.index_version = Some(self.runs.index.version);
                                op.phase = Phase::Committing;
                                op.deadline = self.clock.mono_ms + STORAGE_MS;
                            }
                            WritePurpose::Revoke => {
                                let error = op.error.take().unwrap_or_else(|| {
                                    failure(
                                        "storageError",
                                        Some(&op),
                                        json!({"upstreamDone":false}),
                                    )
                                });
                                settle(&mut op, Err(error), actions);
                                remove = true;
                            }
                            WritePurpose::Drain => {
                                remove = true;
                            }
                        }
                    } else {
                        storage_failure(&mut op, false, actions);
                        if writing.purpose == WritePurpose::Intent
                            && op.record.phase != DiskPhase::UpstreamDone
                        {
                            let error =
                                failure("storageError", Some(&op), json!({"upstreamDone":false}));
                            remove = self.lifecycle_pre_send_failure(&mut op, error, actions);
                        } else {
                            op.write = Some(
                                if op.record.phase == DiskPhase::UpstreamDone
                                    && writing.purpose == WritePurpose::Intent
                                {
                                    WritePurpose::UpstreamDone
                                } else {
                                    writing.purpose
                                },
                            );
                            op.deadline = self.clock.mono_ms + STORAGE_MS;
                        }
                    }
                    if !remove {
                        self.lifecycle.operations.insert(writing.thread, op);
                    }
                }
            } else {
                self.lifecycle.writing = Some(writing);
            }
        }
        let targets: Vec<_> = self.lifecycle.operations.keys().cloned().collect();
        for thread in targets {
            let mut op = self.lifecycle.operations.remove(&thread).unwrap();
            let mut remove = false;
            if op.phase == Phase::Scanning {
                if let Some(result) = op.scan.as_ref().and_then(Ticket::result) {
                    op.scan = None;
                    match result {
                        ResultValue::Scanned(headers) => {
                            op.headers = self.lifecycle_headers(&headers);
                            let gates = closure(&thread, &op.headers);
                            let conflict = gates.iter().find_map(|t| self.lifecycle_error(t));
                            let blockers = self.lifecycle_blockers(&gates, &op.headers);
                            if let Some(error) = conflict {
                                remove = self.lifecycle_pre_send_failure(&mut op, error, actions);
                            } else if !blockers.is_empty() {
                                let err =
                                    failure("threadBusy", Some(&op), json!({"blockers":blockers}));
                                remove = self.lifecycle_pre_send_failure(&mut op, err, actions);
                            } else {
                                op.gates = gates;
                                op.phase = Phase::Persisting;
                                op.write = Some(WritePurpose::Intent);
                                op.deadline = self.clock.mono_ms + STORAGE_MS;
                            }
                        }
                        _ => {
                            let err =
                                failure("storageError", Some(&op), json!({"upstreamDone":false}));
                            remove = self.lifecycle_pre_send_failure(&mut op, err, actions);
                        }
                    }
                } else if self.clock.mono_ms >= op.deadline {
                    let err = failure("storageTimeout", Some(&op), json!({"upstreamDone":false}));
                    remove = self.lifecycle_pre_send_failure(&mut op, err, actions);
                }
            } else if op.phase == Phase::Committing {
                if op
                    .index_version
                    .is_some_and(|v| self.lifecycle.stored_version >= v)
                {
                    op.phase = Phase::Draining;
                    op.gates = BTreeSet::from([thread.clone()]);
                    Self::lifecycle_success(&mut op, actions);
                } else if self.lifecycle.storage_error.is_some()
                    || self.clock.mono_ms >= op.deadline
                {
                    storage_failure(&mut op, self.lifecycle.storage_error.is_none(), actions);
                }
            } else if matches!(op.phase, Phase::Persisting | Phase::Revoking)
                && self.clock.mono_ms >= op.deadline
            {
                storage_failure(&mut op, true, actions);
                if op.retry
                    && (op.write == Some(WritePurpose::Intent)
                        || self.lifecycle.writing.as_ref().is_some_and(|w| {
                            w.thread == thread && w.purpose == WritePurpose::Intent
                        }))
                {
                    op.phase = Phase::OutcomeUnknown;
                    op.gates = BTreeSet::from([thread.clone()]);
                    op.write = None;
                }
            }
            if op.phase == Phase::Draining
                && op.record.attempts.iter().all(|a| a.settled)
                && op.write.is_none()
                && !self
                    .lifecycle
                    .writing
                    .as_ref()
                    .is_some_and(|w| w.thread == thread)
            {
                op.write = Some(WritePurpose::Drain);
            }
            if !remove {
                self.lifecycle.operations.insert(thread, op);
            }
        }
        if self.lifecycle.writing.is_none() {
            if let Some(thread) = self
                .lifecycle
                .operations
                .iter()
                .find(|(_, o)| o.write.is_some())
                .map(|(t, _)| t.clone())
            {
                let mut op = self.lifecycle.operations.remove(&thread).unwrap();
                let purpose = op.write.take().unwrap();
                if purpose == WritePurpose::Intent
                    && (op.record.phase != DiskPhase::LocalPurging || op.purge_report.is_some())
                {
                    self.lifecycle_prepare_attempt(&mut op);
                }
                let mut file = self.lifecycle.file.clone();
                if matches!(purpose, WritePurpose::Revoke | WritePurpose::Drain) {
                    file.records.remove(&thread);
                } else {
                    op.record.updated_at = self.clock.now_ms;
                    file.records.insert(thread.clone(), op.record.clone());
                }
                self.lifecycle.next_version += 1;
                let ticket = Ticket::default();
                actions.push(Action::Store(StoreOp::Lifecycle(Work {
                    kind: WorkKind::Write {
                        version: self.lifecycle.next_version,
                        file: file.clone(),
                    },
                    ticket: ticket.clone(),
                })));
                self.lifecycle.writing = Some(Writing {
                    thread: thread.clone(),
                    purpose,
                    file,
                    ticket,
                });
                self.lifecycle.operations.insert(thread, op);
            }
        }
        self.lifecycle_sync_gates();
        for pending in std::mem::take(&mut self.lifecycle.history) {
            match pending.ticket.result() {
                Some(ResultValue::Run(Some(record)))
                    if record.run_id == pending.run
                        && thread_id(&pending.request.params).is_some_and(|t| {
                            record.thread_id == t || record.review_thread_id.as_deref() == Some(t)
                        }) =>
                {
                    self.runs.runs.entry(pending.run).or_insert(*record);
                    self.client_stop(pending.request, actions);
                }
                Some(ResultValue::Run(_)) => response(
                    &pending.request,
                    Err(proto::error_object(
                        -32000,
                        "历史运行不属于该线程或不存在",
                        Some(json!({"reason":"runMismatch"})),
                    )),
                    actions,
                ),
                Some(_) => response(
                    &pending.request,
                    Err(failure("storageError", None, json!({"upstreamDone":false}))),
                    actions,
                ),
                None if self.clock.mono_ms >= pending.deadline => response(
                    &pending.request,
                    Err(failure(
                        "storageTimeout",
                        None,
                        json!({"upstreamDone":false}),
                    )),
                    actions,
                ),
                None => self.lifecycle.history.push(pending),
            }
        }
        if self.lifecycle_needs_timer() && !self.lifecycle.timer {
            self.lifecycle.timer = true;
            actions.push(Action::Schedule {
                token: ClaimToken {
                    thread_id: String::new(),
                    claim_id: 0,
                },
                kind: TimerKind::Lifecycle,
                delay_ms: 20,
            });
        }
    }

    pub(crate) fn lifecycle_tick(&mut self) {
        self.lifecycle.timer = false;
    }

    fn lifecycle_needs_timer(&self) -> bool {
        self.lifecycle.writing.is_some()
            || self
                .lifecycle
                .history
                .iter()
                .any(|pending| pending.ticket.result().is_none())
            || self.lifecycle.operations.values().any(|op| {
                op.scan
                    .as_ref()
                    .is_some_and(|ticket| ticket.result().is_none())
                    || op.phase == Phase::Committing
            })
    }
}
