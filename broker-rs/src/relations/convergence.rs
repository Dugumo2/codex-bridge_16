//! 收敛活动账本与保守事实快照。授权过期不等于派生工作已结束。
use super::*;
use crate::sa1b::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConvergenceLedger {
    pub work_epoch: u64,
    pub covered_epoch: Option<u64>,
    pub activities: Vec<ActivityFact>,
    pub results: Vec<ResultRef>,
    pub pending: BTreeSet<EvidenceKey>,
    pub seen_operations: BTreeSet<EvidenceKey>,
    pub next_seq: BTreeMap<String, u64>,
    pub result_versions: BTreeMap<(String, u64), u64>,
    pub overflow: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityObservation {
    pub activity: MemberActivity,
    pub result: Option<ActivityResultStatus>,
    pub conn_epoch: u64,
}

pub(crate) fn normalize_activity(
    status: &str,
    closed_verified: bool,
) -> (MemberActivity, Option<ActivityResultStatus>) {
    use ActivityResultStatus as R;
    use MemberActivity as A;
    match status {
        "completed" => (A::Settled, Some(R::Completed)),
        "failed" | "errored" => (A::Settled, Some(R::Failed)),
        "interrupted" => (A::Settled, Some(R::Interrupted)),
        "shutdown" if closed_verified => (A::Settled, Some(R::Shutdown)),
        "pendingInit" => (A::Starting, None),
        "running" | "inProgress" => (A::Running, None),
        _ => (A::Unknown, None),
    }
}

impl Machine {
    pub(crate) fn convergence_observe_goal(
        &mut self,
        thread: &str,
        active: bool,
        actions: &mut Vec<Action>,
    ) {
        self.convergences
            .goals
            .insert(thread.into(), (self.relations.conn_epoch, active));
        if !self.convergence_enabled() {
            return;
        }
        let runs: Vec<_> = self
            .relations
            .contexts
            .iter()
            .filter(|(_, c)| c.members.contains_key(thread))
            .map(|(run, _)| run.clone())
            .collect();
        for run in runs {
            self.refresh_member(&run, thread, MemberChange::Goal, actions);
            self.convergence_sync_facts(&run);
            self.convergence_drive(&run, actions);
        }
    }

    pub(crate) fn convergence_observation_gap(&mut self, thread: &str, actions: &mut Vec<Action>) {
        if !self.convergence_enabled() {
            return;
        }
        self.convergences
            .observations
            .retain(|(t, _), _| t != thread);
        self.convergences.goals.remove(thread);
        self.convergences.capabilities.remove(thread);
        let runs: Vec<_> = self
            .relations
            .contexts
            .iter()
            .filter(|(_, c)| {
                c.members.contains_key(thread) || c.identity.execution_thread_id == thread
            })
            .map(|(r, _)| r.clone())
            .collect();
        for run in runs {
            if let Some(ledger) = self.convergences.ledgers.get_mut(&run) {
                ledger.covered_epoch = None;
            }
            self.convergence_sync_facts(&run);
            self.touch_context(&run, MemberChange::Activity, actions);
        }
    }

    pub(crate) fn convergence_agent_states(
        &mut self,
        parent: &str,
        parent_turn: &str,
        item: &serde_json::Value,
        actions: &mut Vec<Action>,
    ) {
        if !self.convergence_enabled() {
            return;
        }
        let super::facts::ParentRun::Own { run, .. } = self.parent_run(parent, parent_turn) else {
            return;
        };
        let Some(states) = item
            .get("agentsStates")
            .and_then(serde_json::Value::as_object)
        else {
            return;
        };
        for (thread, state) in states {
            if !self
                .manage_context(&run)
                .is_some_and(|c| c.members.contains_key(thread))
            {
                continue;
            }
            let Some(status) = state.get("status").and_then(serde_json::Value::as_str) else {
                continue;
            };
            let closed = status == "shutdown"
                && item.get("tool").and_then(serde_json::Value::as_str) == Some("closeAgent")
                && item.get("status").and_then(serde_json::Value::as_str) == Some("completed");
            let turn = self.unique_active_bound_turn(thread).or_else(|| {
                closed
                    .then(|| {
                        self.relations
                            .aux
                            .observed
                            .get(thread)?
                            .back()
                            .map(|o| o.turn.clone())
                    })
                    .flatten()
                    .filter(|turn| {
                        self.relations
                            .bindings
                            .get(&(thread.clone(), turn.clone()))
                            .is_some_and(|b| b.run_id == run && b.status == BindingStatus::Bound)
                    })
            });
            if let Some(turn) = turn {
                self.convergence_observe_activity(thread, &turn, status, closed);
                if matches!(status, "completed" | "failed" | "errored" | "interrupted") || closed {
                    self.observe_turn(thread, &turn, true, true, actions);
                }
                if closed {
                    self.convergences.closed_threads.insert(thread.clone());
                    self.convergence_observe_goal(thread, false, actions);
                }
            } else if status == "pendingInit" {
                self.convergence_observe_activity(thread, "", status, false);
            } else if matches!(status, "notFound" | "notLoaded" | "shutdown") {
                self.convergence_observation_gap(thread, actions);
            }
            self.convergence_sync_facts(&run);
            self.touch_context(&run, MemberChange::Activity, actions);
        }
    }

    /// 仅保存观察；归属仍由 binding 与管理上下文独立证明。
    pub(crate) fn convergence_observe_activity(
        &mut self,
        thread: &str,
        turn: &str,
        status: &str,
        closed_verified: bool,
    ) {
        let key = (thread.to_owned(), turn.to_owned());
        if self.convergences.observations.len() >= 4096
            && !self.convergences.observations.contains_key(&key)
        {
            for ledger in self.convergences.ledgers.values_mut() {
                ledger.overflow = true;
            }
            return;
        }
        let (activity, result) = normalize_activity(status, closed_verified);
        if activity == MemberActivity::Running {
            self.convergences.closed_threads.remove(thread);
        }
        if result.is_some()
            && self
                .convergences
                .observations
                .get(&key)
                .is_some_and(|o| o.result == Some(ActivityResultStatus::Shutdown))
        {
            return;
        }
        // 同一轮已结束后迟到 started 不允许回退；复用线程使用新的 turnId。
        if result.is_none()
            && activity == MemberActivity::Running
            && self
                .convergences
                .observations
                .get(&key)
                .is_some_and(|o| o.result.is_some())
        {
            return;
        }
        self.convergences.observations.insert(
            key,
            ActivityObservation {
                activity,
                result,
                conn_epoch: self.relations.conn_epoch,
            },
        );
    }

    pub(crate) fn convergence_sync_facts(&mut self, run: &str) {
        let Some(context) = self.relations.contexts.get(run).cloned() else {
            return;
        };
        let ledger = self.convergences.ledgers.entry(run.into()).or_default();
        let before = ledger.work_epoch;
        for op in self.relations.operations.values().filter(|o| {
            o.run_id == run
                && matches!(
                    o.kind,
                    EvidenceKind::V1Spawn
                        | EvidenceKind::V2Started
                        | EvidenceKind::V1SendInput
                        | EvidenceKind::V1ResumeAgent
                        | EvidenceKind::V2Interacted
                )
        }) {
            if ledger.seen_operations.len() >= 4096 && !ledger.seen_operations.contains(&op.key) {
                ledger.overflow = true;
                continue;
            }
            if ledger.seen_operations.insert(op.key.clone()) {
                ledger.work_epoch = ledger.work_epoch.saturating_add(1);
                ledger.pending.insert(op.key.clone());
            }
            let bound = !op.receivers.is_empty()
                && op.receivers.iter().all(|thread| {
                    op.rejected_receivers.contains_key(thread)
                        || self.relations.bindings.values().any(|b| {
                            b.run_id == run
                                && b.thread_id == *thread
                                && b.operation == op.key
                                && b.status == BindingStatus::Bound
                        })
                });
            if op.outcome == EvidenceOutcome::Failed || bound {
                ledger.pending.remove(&op.key);
            }
        }
        for (thread, member) in &context.members {
            if member.kind != RelationKind::Subagent {
                continue;
            }
            let mut turns = member.bindings.clone();
            // 发现的未归属活动仍阻塞，但不能由此授予停止权。
            if let Some(observed) = self.relations.aux.observed.get(thread) {
                for o in observed.iter().filter(|o| !o.completed) {
                    if !turns.contains(&o.turn) {
                        turns.push(o.turn.clone());
                    }
                }
            }
            let mut keys: Vec<Option<String>> = if turns.is_empty() {
                vec![None]
            } else {
                turns.into_iter().map(Some).collect()
            };
            if self.relations.operations.values().any(|op| {
                op.run_id == run
                    && ledger.pending.contains(&op.key)
                    && op.receivers.contains(thread)
            }) && !keys.contains(&None)
                && !self
                    .relations
                    .aux
                    .observed
                    .get(thread)
                    .is_some_and(|turns| {
                        turns
                            .iter()
                            .any(|o| !o.completed && !member.bindings.contains(&o.turn))
                    })
            {
                keys.push(None);
            }
            for turn in keys {
                let existing = ledger
                    .activities
                    .iter()
                    .position(|a| a.key.thread_id == *thread && a.key.turn_id == turn)
                    .or_else(|| {
                        turn.as_ref().and_then(|_| {
                            ledger
                                .activities
                                .iter()
                                .position(|a| a.key.thread_id == *thread && a.key.turn_id.is_none())
                        })
                    });
                let index = if let Some(index) = existing {
                    index
                } else {
                    if ledger.activities.len() >= 4096 {
                        ledger.overflow = true;
                        continue;
                    }
                    let seq = ledger.next_seq.entry(thread.clone()).or_default();
                    *seq += 1;
                    ledger.activities.push(ActivityFact {
                        key: ActivityKey {
                            thread_id: thread.clone(),
                            activity_seq: *seq,
                            turn_id: None,
                        },
                        parent_thread_id: member.parent.clone(),
                        activity: MemberActivity::Unknown,
                        result_status: None,
                        verified: false,
                        active_goal: None,
                        pending_requests: 0,
                    });
                    ledger.work_epoch = ledger.work_epoch.saturating_add(1);
                    ledger.activities.len() - 1
                };
                let fact = &mut ledger.activities[index];
                fact.key.turn_id = turn.clone();
                fact.parent_thread_id = member.parent.clone();
                fact.pending_requests = member.pending_requests;
                fact.active_goal = self
                    .convergences
                    .goals
                    .get(thread)
                    .filter(|(epoch, _)| *epoch == self.relations.conn_epoch)
                    .map(|(_, active)| *active);
                let bound = turn.as_ref().is_some_and(|t| {
                    self.relations
                        .bindings
                        .get(&(thread.clone(), t.clone()))
                        .is_some_and(|b| b.run_id == run && b.status == BindingStatus::Bound)
                });
                let observation = self
                    .convergences
                    .observations
                    .get(&(thread.clone(), turn.clone().unwrap_or_default()))
                    .filter(|o| o.conn_epoch == self.relations.conn_epoch);
                let (activity, status) = observation.map(|o| (o.activity, o.result)).unwrap_or((
                    if turn.is_none() && member.activity == MemberActivity::Starting {
                        MemberActivity::Starting
                    } else {
                        MemberActivity::Unknown
                    },
                    None,
                ));
                if status.is_some() && fact.result_status != status {
                    ledger.work_epoch = ledger.work_epoch.saturating_add(1);
                    let version = ledger
                        .result_versions
                        .entry((thread.clone(), fact.key.activity_seq))
                        .or_default();
                    *version += 1;
                    ledger.results.retain(|r| {
                        !(r.key.thread_id == *thread && r.key.activity_seq == fact.key.activity_seq)
                    });
                    ledger.results.push(ResultRef {
                        key: fact.key.clone(),
                        locator: ResultLocator {
                            thread_id: thread.clone(),
                            turn_id: turn.clone(),
                            result_version: *version,
                        },
                        available: turn.is_some(),
                    });
                }
                fact.activity = activity;
                if status.is_some() {
                    fact.result_status = status;
                }
                fact.verified = bound && observation.is_some() && member.conflict.is_none();
            }
        }
        if before != ledger.work_epoch {
            ledger.covered_epoch = None;
        }
    }

    pub fn convergence_facts(&self, run_id: &str) -> Option<ConvergenceFacts> {
        let context = self.manage_context(run_id)?;
        let runtime = self.convergences.runs.get(run_id)?;
        if !self.convergence_token_current(&runtime.token) {
            return None;
        }
        let ledger = self.convergences.ledgers.get(run_id)?;
        let mut conflicts: Vec<String> = context
            .members
            .iter()
            .filter_map(|(t, m)| m.conflict.as_ref().map(|c| format!("{t}: {c}")))
            .take(32)
            .collect();
        if ledger.overflow {
            conflicts.push("活动事实超出有界容量".into());
        }
        // Guardian 是已拒绝动作的可选事后复核，不是仍悬挂的上游审批/提问请求。
        let inbox: Vec<_> = self
            .inbox_pending_run_items(run_id)
            .into_iter()
            .filter(|i| i.kind != crate::inbox::InboxKind::Guardian)
            .collect();
        let inbox_ids: BTreeSet<_> = inbox.iter().map(|item| item.id.as_str()).collect();
        // 同一子请求转入收件箱后仍有路由记录，按实际事项去重。
        let pending_requests = self
            .relations
            .requests
            .iter()
            .filter(|(key, r)| {
                r.run_id.as_deref() == Some(run_id)
                    && routing::live_stage(&r.stage)
                    && !self
                        .relations
                        .aux
                        .request_meta
                        .get(*key)
                        .and_then(|meta| meta.inbox_item.as_deref())
                        .is_some_and(|id| inbox_ids.contains(id))
            })
            .count()
            + self
                .requests
                .pending
                .values()
                .filter(|r| r.thread_id == context.identity.execution_thread_id)
                .count()
            + inbox.len();
        let unresolved_lookups = self
            .relations
            .aux
            .lookups
            .keys()
            .filter(|t| self.lookup_may_belong(run_id, t))
            .count();
        Some(ConvergenceFacts {
            token: runtime.token.clone(),
            work_epoch: ledger.work_epoch,
            revision: context.revision,
            discovery: context.discovery.clone(),
            discovery_covered_epoch: ledger.covered_epoch,
            activities: ledger.activities.clone(),
            pending_starts: ledger.pending.union(&context.in_flight_spawns).count(),
            pending_requests,
            unresolved_lookups,
            conflicts,
            result_refs: ledger.results.clone(),
        })
    }
}

pub(crate) fn settled(facts: &ConvergenceFacts) -> bool {
    facts.pending_starts == 0
        && facts.pending_requests == 0
        && facts.unresolved_lookups == 0
        && facts.conflicts.is_empty()
        && facts.activities.iter().all(|a| {
            a.verified
                && a.activity == MemberActivity::Settled
                && a.result_status.is_some()
                && a.active_goal == Some(false)
                && a.pending_requests == 0
        })
}
