//! 线程血缘、运行活动和未决候选的独立投影。分类本身不授予管理权。
use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Membership {
    Active,
    Historical,
    Pending(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CandidateClassification {
    pub membership: Membership,
    pub source: String,
    pub conn_epoch: u64,
    pub manage_gen: u64,
    pub work_epoch: u64,
    pub checks: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CandidateObservation {
    pub conn_epoch: u64,
    pub inactive: bool,
}

impl Machine {
    /// 观察版本有界；淘汰后回包找不到原非零版本，会保守拒绝，不会退回旧结论。
    pub(crate) fn candidate_changed(&mut self, thread: &str) {
        if !self.relations.aux.candidate_versions.contains_key(thread)
            && self.relations.aux.candidate_versions.len() >= MAX_OBSERVED_THREADS
        {
            if let Some(first) = self.relations.aux.candidate_versions.keys().next().cloned() {
                self.relations.aux.candidate_versions.remove(&first);
            }
        }
        let version = self.relations.aux.next();
        self.relations
            .aux
            .candidate_versions
            .insert(thread.into(), version);
    }
    /// 通知与请求会改变候选事实；刷新已知血缘和已有分类，不授予管理权。
    pub(crate) fn refresh_thread_classifications(
        &mut self,
        thread: &str,
        change: MemberChange,
        actions: &mut Vec<Action>,
    ) {
        let runs: Vec<String> = self
            .relations
            .contexts
            .keys()
            .filter(|run| {
                self.run_tracks_thread(run, thread)
                    || self.run_lineage_threads(run).contains(thread)
            })
            .cloned()
            .collect();
        for run in runs {
            self.refresh_member(&run, thread, change, actions);
            self.convergence_sync_facts(&run);
        }
    }

    /// 合法交接迁移有效操作的管理代际；所有归属入口共享这一判定。
    /// 当前授权撤销不抹掉已经成立的活动归属；实际绑定与路由另行检查授权。
    pub(crate) fn operation_generation_current(&self, op: &EvidenceOperation) -> bool {
        op.conn_epoch == self.relations.conn_epoch
            && self
                .manage_context(&op.run_id)
                .is_some_and(|c| c.gen == op.authority_gen)
    }

    /// 共同资格判定：成员表与发现分类均不能反过来证明归属。
    pub(crate) fn current_management_evidence(&self, run: &str, thread: &str) -> bool {
        if self
            .relations
            .bindings
            .values()
            .any(|b| b.run_id == run && b.thread_id == thread && b.status == BindingStatus::Bound)
        {
            return true;
        }
        let valid = |op: &EvidenceOperation| {
            op.run_id == run
                && op.receivers.contains(thread)
                && !op.rejected_receivers.contains_key(thread)
                && self.op_confirmed(&op.key)
                && matches!(
                    op.outcome,
                    EvidenceOutcome::Pending | EvidenceOutcome::Proven
                )
                && self.operation_generation_current(op)
                && matches!(
                    op.kind,
                    EvidenceKind::V1Spawn
                        | EvidenceKind::V2Started
                        | EvidenceKind::V1SendInput
                        | EvidenceKind::V1ResumeAgent
                        | EvidenceKind::V2Interacted
                )
        };
        self.relations.operations.values().any(valid)
    }

    pub(crate) fn run_lineage_threads(&self, run: &str) -> BTreeSet<String> {
        let Some(c) = self.manage_context(run) else {
            return BTreeSet::new();
        };
        self.relations
            .edges
            .keys()
            .cloned()
            .chain(
                self.runs
                    .index
                    .threads
                    .values()
                    .map(|e| e.thread_id.clone()),
            )
            .filter(|thread| {
                let mut current = thread.clone();
                let mut seen = BTreeSet::new();
                for _ in 0..MAX_DEPTH {
                    if !seen.insert(current.clone()) {
                        return false;
                    }
                    let Some(edge) = self.thread_relation(&current) else {
                        return false;
                    };
                    if edge.kind != RelationKind::Subagent {
                        return false;
                    }
                    current = edge.parent_thread_id;
                    if current == c.identity.logical_root_thread_id
                        || current == c.identity.execution_thread_id
                    {
                        return true;
                    }
                }
                false
            })
            .collect()
    }

    pub(crate) fn run_has_candidate(&self, run: &str, thread: &str) -> bool {
        self.relations
            .aux
            .classifications
            .get(&(run.into(), thread.into()))
            .is_some_and(|c| matches!(c.membership, Membership::Pending(_)))
    }

    pub(crate) fn run_has_candidates(&self, run: &str) -> bool {
        self.relations
            .aux
            .classifications
            .iter()
            .any(|((r, _), c)| r == run && matches!(c.membership, Membership::Pending(_)))
    }

    pub(crate) fn run_observes_thread(&self, run: &str, thread: &str) -> bool {
        self.manage_context(run)
            .is_some_and(|c| c.members.contains_key(thread))
            || self.run_has_candidate(run, thread)
    }

    pub(crate) fn run_tracks_thread(&self, run: &str, thread: &str) -> bool {
        self.run_observes_thread(run, thread)
            || self
                .relations
                .aux
                .classifications
                .contains_key(&(run.into(), thread.into()))
    }

    pub(crate) fn classify_candidate(&self, run: &str, thread: &str) -> Membership {
        if self.current_management_evidence(run, thread) {
            return Membership::Active;
        }
        let reason =
            if self.claims.contains_key(thread) {
                "independentClaim"
            } else if self.relations.bindings.values().any(|b| {
                b.thread_id == thread && b.run_id != run && b.status == BindingStatus::Bound
            }) {
                "otherRunBinding"
            } else if self.rejected_followup_for_run(run, thread) {
                "rejectedFollowup"
            } else if self
                .relations
                .requests
                .values()
                .any(|r| r.source_thread_id == thread && routing::live_stage(&r.stage))
            {
                "candidateRequestPending"
            } else if self
                .convergences
                .goals
                .get(thread)
                .is_some_and(|(epoch, active)| *epoch == self.relations.conn_epoch && *active)
            {
                "candidateGoalActive"
            } else if self
                .relations
                .aux
                .observed
                .get(thread)
                .is_some_and(|o| o.iter().any(|t| !t.completed))
            {
                "candidateActivityActive"
            } else if self.historical_subagent_for_run(run, thread) {
                let inactive = self
                    .relations
                    .aux
                    .candidate_observations
                    .get(thread)
                    .is_some_and(|o| o.conn_epoch == self.relations.conn_epoch && o.inactive);
                let goal_clear = self
                    .convergences
                    .goals
                    .get(thread)
                    .is_some_and(|(epoch, active)| *epoch == self.relations.conn_epoch && !*active);
                if inactive && goal_clear {
                    return Membership::Historical;
                }
                "historicalActivityUnverified"
            } else {
                self.runs.index.child_history.coverage_reason()
            };
        Membership::Pending(reason.into())
    }

    /// 每次结论改变都撤销发现覆盖；日志只记标识与原因，不含正文。
    pub(crate) fn refresh_classification(
        &mut self,
        run: &str,
        thread: &str,
        source: &str,
        actions: &mut Vec<Action>,
    ) -> Membership {
        let membership = self.classify_candidate(run, thread);
        let key = (run.to_owned(), thread.to_owned());
        let old = self.relations.aux.classifications.get(&key);
        let changed = old.is_none_or(|c| {
            c.membership != membership || c.conn_epoch != self.relations.conn_epoch
        });
        if changed {
            let count = self
                .relations
                .aux
                .classifications
                .keys()
                .filter(|(r, _)| r == run)
                .count();
            if old.is_none() && count >= MAX_MEMBERS {
                self.convergences
                    .ledgers
                    .entry(run.into())
                    .or_default()
                    .overflow = true;
                if let Some(c) = self.relations.contexts.get_mut(run) {
                    c.discovery.complete = false;
                    if !c
                        .discovery
                        .incomplete_reasons
                        .iter()
                        .any(|r| r == "candidateOverflow")
                    {
                        c.discovery
                            .incomplete_reasons
                            .push("candidateOverflow".into());
                    }
                }
                return membership;
            }
            let checks = old.map_or(0, |c| c.checks);
            let ledger = self.convergences.ledgers.entry(run.into()).or_default();
            // 分类变化撤销覆盖，不把历史核实本身记作需要再次汇总的新工作。
            ledger.covered_epoch = None;
            let epoch = ledger.work_epoch;
            let gen = self.manage_context(run).map_or(0, |c| c.gen);
            self.relations.aux.classifications.insert(
                key,
                CandidateClassification {
                    membership: membership.clone(),
                    source: source.into(),
                    conn_epoch: self.relations.conn_epoch,
                    manage_gen: gen,
                    work_epoch: epoch,
                    checks: checks.saturating_add(1),
                },
            );
            if let Some(context) = self.relations.contexts.get_mut(run) {
                context.discovery.complete = false;
            }
            if checks < 16 {
                let coverage_issue =
                    matches!(&membership, Membership::Pending(_)).then_some("discoveryIncomplete");
                self.runs.log_source(run, thread, None, "subagent", None, None, self.clock.now_ms,
                    "候选分类", &format!("runId={run} source={source} generation={gen} workEpoch={epoch} classification={membership:?}"),
                    false, false, coverage_issue, actions);
            }
        }
        membership
    }
}
