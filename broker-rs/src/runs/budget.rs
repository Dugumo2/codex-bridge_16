//! 运行共享预算。权威 goal 水位加未覆盖活动增量，禁止无重叠证明的简单相加。

use crate::claim::{Action, Machine};
use crate::sa1b::*;
use std::collections::{BTreeMap, BTreeSet};

type Key = (String, u64);
fn key(value: &ActivityKey) -> Key {
    (value.thread_id.clone(), value.activity_seq)
}

/// A 的传输层领取此任务并补查 goal／活动证据；期限固定，重复调用不能续期。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetVerification {
    pub token: ConvergeToken,
    pub deadline_mono_ms: u64,
    pub activities: Vec<ActivityKey>,
    pub goal_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BudgetTracker {
    goal_version: Option<u64>,
    /// 最近一次 goal 汇总缺少完整交叠映射时，逐活动通知不能擅自替它完成证明。
    goal_overlap_known: bool,
    expected: BTreeMap<Key, ActivityKey>,
    observed: BTreeSet<Key>,
    overlap_known: BTreeSet<Key>,
    invalid: BTreeMap<Key, u64>,
    goal_invalid: bool,
    goal_invalid_version: Option<u64>,
    verification: Option<BudgetVerification>,
    verification_taken: bool,
}

impl BudgetTracker {
    fn finish_verified_request(&mut self, budget: &RunBudget) {
        if budget.coverage == BudgetCoverage::Complete {
            self.verification = None;
            self.verification_taken = false;
        }
    }
    pub(crate) fn validate_initial(&self, budget: &mut RunBudget) {
        self.recompute(budget);
    }
    pub(crate) fn rebind(&mut self, token: &ConvergeToken) {
        if let Some(verification) = &mut self.verification {
            verification.token = token.clone();
            // 旧授权的补查回包不再授信；新读取沿用原截止，不继承旧回包。
            self.verification_taken = false;
        }
    }
    pub(crate) fn initialize(&mut self, budget: &RunBudget) {
        self.goal_overlap_known = budget.coverage == BudgetCoverage::Complete;
        for watermark in &budget.activities {
            let id = key(&watermark.key);
            self.expected.insert(id.clone(), watermark.key.clone());
            self.observed.insert(id.clone());
            if budget.coverage == BudgetCoverage::Complete {
                self.overlap_known.insert(id);
            }
        }
    }

    fn recompute(&self, budget: &mut RunBudget) {
        let mut identities = BTreeSet::new();
        let covered = budget
            .activities
            .iter()
            .try_fold(0u64, |sum, w| sum.checked_add(w.goal_covered_tokens));
        let valid = !self.goal_invalid
            && self.invalid.is_empty()
            && budget
                .activities
                .iter()
                .all(|w| identities.insert(key(&w.key)))
            && covered.is_some_and(|n| n <= budget.goal_aggregate_watermark)
            && budget.goal_aggregate_watermark >= budget.used_at_start
            && budget
                .activities
                .iter()
                .all(|w| w.goal_covered_tokens <= w.tokens);
        let complete = valid
            && self.goal_overlap_known
            && self
                .expected
                .keys()
                .all(|k| self.observed.contains(k) && self.overlap_known.contains(k));
        budget.coverage = if complete {
            BudgetCoverage::Complete
        } else {
            BudgetCoverage::Incomplete
        };
        // 不确定交叠时仅保留已确认的下界，绝不把疑似重叠的账本重复加到 goal。
        let aggregate = budget
            .goal_aggregate_watermark
            .saturating_sub(budget.used_at_start);
        let extras = budget
            .activities
            .iter()
            .filter(|w| self.overlap_known.contains(&key(&w.key)))
            .fold(0u64, |sum, w| {
                sum.saturating_add(w.tokens.saturating_sub(w.goal_covered_tokens))
            });
        if complete {
            let charged = aggregate.saturating_add(extras);
            if charged < budget.tokens_charged {
                // 新的交叠证明不得推翻已确认扣费；这说明证据区间或基线存在冲突。
                budget.coverage = BudgetCoverage::Incomplete;
            } else {
                budget.tokens_charged = charged;
            }
        } else {
            budget.tokens_charged = budget.tokens_charged.max(aggregate);
        }
        budget.exhausted |=
            budget.used_at_start.saturating_add(budget.tokens_charged) >= budget.token_budget;
    }

    fn activity(&mut self, budget: &mut RunBudget, evidence: BudgetEvidence) {
        let id = key(&evidence.key);
        self.expected.insert(id.clone(), evidence.key.clone());
        let prior = budget.activities.iter().position(|w| key(&w.key) == id);
        if prior.is_some_and(|i| evidence.evidence_version < budget.activities[i].evidence_version)
            || self
                .invalid
                .get(&id)
                .is_some_and(|v| evidence.evidence_version <= *v)
        {
            return;
        }
        let tokens = evidence
            .input_tokens
            .checked_sub(evidence.cached_input_tokens)
            .and_then(|n| n.checked_add(evidence.output_tokens));
        let Some(tokens) = tokens else {
            self.invalid.insert(id, evidence.evidence_version);
            self.recompute(budget);
            return;
        };
        let covered = evidence
            .goal_covered_tokens
            .unwrap_or_else(|| prior.map_or(0, |i| budget.activities[i].goal_covered_tokens));
        if covered > tokens
            || covered > budget.goal_aggregate_watermark
            || prior.is_some_and(|i| {
                let old = &budget.activities[i];
                tokens < old.tokens
                    || covered < old.goal_covered_tokens
                    || (evidence.evidence_version == old.evidence_version && tokens != old.tokens)
            })
        {
            self.invalid.insert(id, evidence.evidence_version);
            self.recompute(budget);
            return;
        }
        let mut watermark = BudgetWatermark {
            key: evidence.key,
            tokens,
            evidence_version: evidence.evidence_version,
            goal_covered_tokens: covered,
        };
        if let Some(index) = prior {
            if watermark.key.turn_id.is_none() {
                watermark.key.turn_id = budget.activities[index].key.turn_id.clone();
            }
            budget.activities[index] = watermark;
        } else {
            budget.activities.push(watermark);
        }
        self.observed.insert(id.clone());
        self.invalid.remove(&id);
        if evidence.goal_covered_tokens.is_some() {
            self.overlap_known.insert(id);
        }
        self.recompute(budget);
        self.finish_verified_request(budget);
    }

    fn goal(&mut self, budget: &mut RunBudget, evidence: GoalBudgetEvidence) {
        if self
            .goal_version
            .is_some_and(|v| evidence.evidence_version < v)
            || self
                .goal_invalid_version
                .is_some_and(|v| evidence.evidence_version <= v)
        {
            return;
        }
        if budget.goal_id.as_deref() != Some(evidence.goal_id.as_str())
            || evidence.tokens_used < budget.goal_aggregate_watermark
            || (self.goal_version == Some(evidence.evidence_version)
                && evidence.tokens_used != budget.goal_aggregate_watermark)
        {
            self.goal_invalid = true;
            self.goal_invalid_version = Some(evidence.evidence_version);
            self.recompute(budget);
            return;
        }
        // 先验证整个映射，不能一半生效后才发现重复身份或非法覆盖。
        let mut ids = BTreeSet::new();
        let valid = evidence.covered_activities.as_ref().is_none_or(|items| {
            let mut sum = 0u64;
            items.iter().all(|w| {
                let id = key(&w.key);
                let monotonic = budget
                    .activities
                    .iter()
                    .find(|old| key(&old.key) == id)
                    .is_none_or(|old| w.goal_covered_tokens >= old.goal_covered_tokens);
                sum = sum.saturating_add(w.goal_covered_tokens);
                ids.insert(id)
                    && monotonic
                    && w.goal_covered_tokens <= w.tokens
                    && sum <= evidence.tokens_used
            }) && self.expected.keys().all(|id| ids.contains(id))
        });
        if !valid {
            // 交叠缺失不能吞掉可信根汇总的耗尽信号；只采用其用量下界。
            budget.goal_aggregate_watermark = evidence.tokens_used;
            self.goal_version = Some(evidence.evidence_version);
            self.goal_invalid = true;
            self.goal_invalid_version = Some(evidence.evidence_version);
            self.recompute(budget);
            return;
        }
        self.goal_invalid = false;
        self.goal_invalid_version = None;
        self.goal_version = Some(evidence.evidence_version);
        budget.goal_aggregate_watermark = evidence.tokens_used;
        // complete 后已切到账本时，即使晚到最后一份 goal 水位也不恢复旧活动记账。
        if budget.source != BudgetSource::ActivityLedger {
            budget.source = BudgetSource::GoalAggregate;
        }
        self.goal_overlap_known = evidence.covered_activities.is_some();
        self.overlap_known.clear();
        if let Some(items) = evidence.covered_activities {
            for mut watermark in items {
                let id = key(&watermark.key);
                self.expected.insert(id.clone(), watermark.key.clone());
                self.observed.insert(id.clone());
                self.overlap_known.insert(id.clone());
                if self
                    .invalid
                    .get(&id)
                    .is_none_or(|v| watermark.evidence_version > *v)
                {
                    self.invalid.remove(&id);
                }
                if let Some(old) = budget.activities.iter_mut().find(|w| key(&w.key) == id) {
                    // goal 的映射可以迟于逐活动通知；保留更新的用量，只推进已覆盖部分。
                    old.goal_covered_tokens = watermark.goal_covered_tokens;
                    if watermark.tokens > old.tokens {
                        old.tokens = watermark.tokens;
                        old.evidence_version = old.evidence_version.max(watermark.evidence_version);
                    }
                    if old.key.turn_id.is_none() {
                        old.key.turn_id = watermark.key.turn_id.take();
                    }
                } else {
                    budget.activities.push(watermark);
                }
            }
        }
        self.recompute(budget);
        self.finish_verified_request(budget);
    }
}

impl Machine {
    /// 使用现有存储封口规则；同一次变化只推进一次 revision。
    pub(crate) fn persist_safeguards(&mut self, run: &str, actions: &mut Vec<Action>) {
        self.runs.put(run, actions);
        if let Some(context) = self.relations.contexts.get_mut(run) {
            context.revision = context.revision.saturating_add(1);
        }
        self.notify_convergence_changed(run, actions);
    }

    /// 用量和缺失覆盖也作为协调事件交给 A；观察耗尽后不等待下一次启动才制动。
    fn publish_budget(&mut self, run: &str, actions: &mut Vec<Action>) {
        self.persist_safeguards(run, actions);
        if let Some(revision) = self.manage_context(run).map(|c| c.revision) {
            self.on_member_changed(
                run,
                revision,
                crate::relations::MemberChange::Budget,
                actions,
            );
        }
    }

    /// A 在确认自有活动及新代际时登记需求，缺失通知不可能被当成零用量。
    pub fn require_budget_activity(
        &mut self,
        token: &ConvergeToken,
        activity: ActivityKey,
        actions: &mut Vec<Action>,
    ) {
        let Some(safeguards) = self.safeguards_mut(token) else {
            return;
        };
        safeguards.budget.expected.insert(key(&activity), activity);
        self.refresh_run_budget(token, actions);
    }

    fn refresh_run_budget(&mut self, token: &ConvergeToken, actions: &mut Vec<Action>) {
        let Some(tracker) = self.safeguards(token).map(|g| g.budget.clone()) else {
            return;
        };
        if let Some(budget) = self
            .runs
            .runs
            .get_mut(&token.run_id)
            .and_then(|r| r.run_budget.as_mut())
        {
            let before = budget.clone();
            tracker.recompute(budget);
            if *budget != before {
                self.publish_budget(&token.run_id, actions);
            }
        }
    }

    pub fn observe_run_budget(
        &mut self,
        token: &ConvergeToken,
        evidence: BudgetEvidence,
        actions: &mut Vec<Action>,
    ) {
        let Some(mut tracker) = self.safeguards(token).map(|g| g.budget.clone()) else {
            return;
        };
        let Some(budget) = self
            .runs
            .runs
            .get_mut(&token.run_id)
            .and_then(|r| r.run_budget.as_mut())
        else {
            return;
        };
        let before = budget.clone();
        tracker.activity(budget, evidence);
        let changed = *budget != before;
        self.safeguards_mut(token).expect("身份未变").budget = tracker;
        if changed {
            self.publish_budget(&token.run_id, actions);
        }
    }

    pub fn observe_goal_budget(
        &mut self,
        token: &ConvergeToken,
        evidence: GoalBudgetEvidence,
        actions: &mut Vec<Action>,
    ) {
        let Some(mut tracker) = self.safeguards(token).map(|g| g.budget.clone()) else {
            return;
        };
        let Some(budget) = self
            .runs
            .runs
            .get_mut(&token.run_id)
            .and_then(|r| r.run_budget.as_mut())
        else {
            return;
        };
        let before = budget.clone();
        tracker.goal(budget, evidence);
        let changed = *budget != before;
        self.safeguards_mut(token).expect("身份未变").budget = tracker;
        if changed {
            self.publish_budget(&token.run_id, actions);
        }
    }

    pub fn reconcile_budget_admission(&self, token: &ConvergeToken) -> BudgetAdmission {
        if self.safeguards(token).is_none() {
            return BudgetAdmission::Disabled;
        }
        let Some(budget) = self
            .runs
            .runs
            .get(&token.run_id)
            .and_then(|r| r.run_budget.as_ref())
        else {
            return BudgetAdmission::Allowed;
        };
        if budget.exhausted
            || budget.used_at_start.saturating_add(budget.tokens_charged) >= budget.token_budget
        {
            BudgetAdmission::Exhausted
        } else if budget.coverage != BudgetCoverage::Complete {
            BudgetAdmission::NeedsVerification
        } else {
            BudgetAdmission::Allowed
        }
    }

    /// 首次缺证据建立一次有界核实，A 领取任务接传输层。再次调用不重排、不刷新截止。
    pub fn verify_run_budget(&mut self, token: &ConvergeToken, actions: &mut Vec<Action>) {
        if self.reconcile_budget_admission(token) != BudgetAdmission::NeedsVerification {
            return;
        }
        let runtime = &self.convergences.runs[&token.run_id];
        let deadline_mono_ms = self
            .clock
            .mono_ms
            .saturating_add(20_000)
            .min(runtime.deadline_mono_ms);
        let goal_id = self.runs.runs[&token.run_id]
            .run_budget
            .as_ref()
            .and_then(|b| b.goal_id.clone());
        let tracker = &mut self.safeguards_mut(token).expect("准入已核实").budget;
        if tracker.verification.is_some() {
            return;
        }
        tracker.verification = Some(BudgetVerification {
            token: token.clone(),
            deadline_mono_ms,
            activities: tracker.expected.values().cloned().collect(),
            goal_id,
        });
        actions.push(Action::Debug(format!(
            "运行 {} 预算证据不完整，已登记有界核实",
            token.run_id
        )));
    }

    /// A 只领取一次，必须携带完整 token 回传证据；不把 RPC 成功当成 coverage 完整。
    pub fn take_budget_verification(
        &mut self,
        token: &ConvergeToken,
    ) -> Option<BudgetVerification> {
        let tracker = &mut self.safeguards_mut(token)?.budget;
        if tracker.verification_taken {
            return None;
        }
        let request = tracker.verification.clone()?;
        tracker.verification_taken = true;
        Some(request)
    }

    pub fn budget_verification_expired(&self, token: &ConvergeToken) -> bool {
        self.safeguards(token)
            .and_then(|g| g.budget.verification.as_ref())
            .is_some_and(|v| self.clock.mono_ms >= v.deadline_mono_ms)
            && self.reconcile_budget_admission(token) == BudgetAdmission::NeedsVerification
    }

    /// goal complete 后冻结已覆盖基线；保留原额度，后续汇总只加新活动水位。
    pub fn freeze_goal_budget(&mut self, token: &ConvergeToken, actions: &mut Vec<Action>) {
        if self.safeguards(token).is_none() {
            return;
        }
        if let Some(budget) = self
            .runs
            .runs
            .get_mut(&token.run_id)
            .and_then(|r| r.run_budget.as_mut())
        {
            if budget.source != BudgetSource::ActivityLedger {
                budget.source = BudgetSource::ActivityLedger;
                self.publish_budget(&token.run_id, actions);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn activity(thread: &str, seq: u64, turn: Option<&str>) -> ActivityKey {
        ActivityKey {
            thread_id: thread.into(),
            activity_seq: seq,
            turn_id: turn.map(str::to_owned),
        }
    }
    fn budget() -> RunBudget {
        RunBudget {
            token_budget: 1000,
            used_at_start: 100,
            tokens_charged: 0,
            coverage: BudgetCoverage::Complete,
            exhausted: false,
            source: BudgetSource::GoalAggregate,
            goal_id: Some("原目标".into()),
            goal_aggregate_watermark: 100,
            activities: vec![],
        }
    }
    fn usage(key: ActivityKey, tokens: u64, version: u64, covered: Option<u64>) -> BudgetEvidence {
        BudgetEvidence {
            key,
            input_tokens: tokens + 30,
            cached_input_tokens: 30,
            output_tokens: 0,
            evidence_version: version,
            goal_covered_tokens: covered,
        }
    }
    fn watermark(key: ActivityKey, tokens: u64, covered: u64) -> BudgetWatermark {
        BudgetWatermark {
            key,
            tokens,
            evidence_version: 1,
            goal_covered_tokens: covered,
        }
    }
    fn aggregate(
        tokens: u64,
        version: u64,
        items: Option<Vec<BudgetWatermark>>,
    ) -> GoalBudgetEvidence {
        GoalBudgetEvidence {
            goal_id: "原目标".into(),
            tokens_used: tokens,
            evidence_version: version,
            covered_activities: items,
        }
    }

    #[test]
    fn sa1b_budget_root_aggregate_deduplicates_descendants_and_preserves_baseline() {
        let mut b = budget();
        let mut tracker = BudgetTracker::default();
        tracker.initialize(&b);
        let child = activity("子", 1, None);
        tracker.activity(&mut b, usage(child.clone(), 200, 1, None));
        assert_eq!(b.coverage, BudgetCoverage::Incomplete);
        tracker.goal(
            &mut b,
            aggregate(400, 1, Some(vec![watermark(child.clone(), 200, 200)])),
        );
        assert_eq!((b.used_at_start, b.tokens_charged), (100, 300));
        assert_eq!(b.coverage, BudgetCoverage::Complete);
        tracker.activity(
            &mut b,
            usage(activity("子", 1, Some("补全轮")), 250, 2, Some(200)),
        );
        assert_eq!(b.tokens_charged, 350);
        assert_eq!(b.activities.len(), 1);
        tracker.activity(&mut b, usage(child.clone(), 250, 2, Some(200)));
        tracker.activity(&mut b, usage(child, 50, 1, Some(0)));
        assert_eq!(b.tokens_charged, 350);
        assert_eq!(b.activities[0].key.turn_id.as_deref(), Some("补全轮"));
    }

    #[test]
    fn sa1b_budget_aggregate_catches_up_to_ledger_without_double_charge() {
        let mut b = budget();
        let mut t = BudgetTracker::default();
        t.initialize(&b);
        let k = activity("子", 1, None);
        t.activity(&mut b, usage(k.clone(), 200, 1, Some(0)));
        assert_eq!(b.tokens_charged, 200);
        t.goal(
            &mut b,
            aggregate(250, 1, Some(vec![watermark(k.clone(), 200, 150)])),
        );
        assert_eq!(b.tokens_charged, 200);
        t.goal(
            &mut b,
            aggregate(300, 2, Some(vec![watermark(k, 200, 200)])),
        );
        assert_eq!(b.tokens_charged, 200);
    }

    #[test]
    fn sa1b_budget_missing_overlap_is_not_max_or_sum() {
        let mut b = budget();
        let mut t = BudgetTracker::default();
        t.initialize(&b);
        t.activity(&mut b, usage(activity("子", 1, None), 200, 1, None));
        t.goal(&mut b, aggregate(400, 1, None));
        assert_eq!(b.coverage, BudgetCoverage::Incomplete);
        assert_eq!(b.tokens_charged, 300);
        t.activity(&mut b, usage(activity("子", 1, None), 200, 2, Some(200)));
        assert_eq!(
            b.coverage,
            BudgetCoverage::Incomplete,
            "单活动不能代替整段交叠证明"
        );
    }

    #[test]
    fn sa1b_budget_expected_missing_activity_prevents_admission() {
        let mut b = budget();
        let mut t = BudgetTracker::default();
        t.initialize(&b);
        let k = activity("孙", 3, None);
        t.expected.insert(key(&k), k.clone());
        t.recompute(&mut b);
        assert_eq!(b.coverage, BudgetCoverage::Incomplete);
        t.activity(&mut b, usage(k, 0, 1, Some(0)));
        assert_eq!(
            b.coverage,
            BudgetCoverage::Complete,
            "明确零证据和未读到不同"
        );
    }

    #[test]
    fn sa1b_budget_complete_goal_retains_original_limit_for_new_summary() {
        let mut b = budget();
        let mut t = BudgetTracker::default();
        t.initialize(&b);
        t.goal(&mut b, aggregate(900, 1, Some(vec![])));
        b.source = BudgetSource::ActivityLedger;
        t.activity(
            &mut b,
            usage(activity("父", 2, Some("汇总")), 100, 1, Some(0)),
        );
        assert_eq!(
            (b.token_budget, b.used_at_start, b.tokens_charged),
            (1000, 100, 900)
        );
        assert!(b.exhausted);
        t.goal(
            &mut b,
            GoalBudgetEvidence {
                goal_id: "新目标".into(),
                tokens_used: 0,
                evidence_version: 2,
                covered_activities: Some(vec![]),
            },
        );
        assert!(b.exhausted);
        assert_eq!(b.token_budget, 1000);
        assert_eq!(b.goal_id.as_deref(), Some("原目标"));
    }

    #[test]
    fn sa1b_budget_activity_reuse_is_separate_but_turn_enrichment_is_not() {
        let mut b = budget();
        let mut t = BudgetTracker::default();
        t.initialize(&b);
        for seq in 1..=2 {
            t.activity(&mut b, usage(activity("子", seq, None), 100, 1, Some(0)));
        }
        t.activity(
            &mut b,
            usage(activity("子", 1, Some("第一轮")), 100, 1, Some(0)),
        );
        assert_eq!(b.activities.len(), 2);
        assert_eq!(b.tokens_charged, 200);
    }

    #[test]
    fn sa1b_budget_invalid_cache_overflow_and_regression_fail_closed() {
        for (input, cache, output) in [(1, 2, 0), (u64::MAX, 0, 1)] {
            let mut b = budget();
            let mut t = BudgetTracker::default();
            t.initialize(&b);
            let k = activity("父", 1, None);
            t.activity(
                &mut b,
                BudgetEvidence {
                    key: k.clone(),
                    input_tokens: input,
                    cached_input_tokens: cache,
                    output_tokens: output,
                    evidence_version: 1,
                    goal_covered_tokens: Some(0),
                },
            );
            assert_eq!(b.coverage, BudgetCoverage::Incomplete);
            t.activity(&mut b, usage(k.clone(), 100, 2, Some(0)));
            assert_eq!(b.coverage, BudgetCoverage::Complete);
            t.activity(&mut b, usage(k, 90, 3, Some(0)));
            assert_eq!(b.coverage, BudgetCoverage::Incomplete);
            assert_eq!(b.tokens_charged, 100);
        }
    }

    #[test]
    fn sa1b_budget_duplicate_mapping_or_unsupported_overlap_never_grants_coverage() {
        for items in [
            vec![
                watermark(activity("子", 1, None), 100, 100),
                watermark(activity("子", 1, Some("同轮")), 100, 100),
            ],
            vec![watermark(activity("子", 1, None), 100, 101)],
            vec![watermark(activity("子", 1, None), 500, 500)],
        ] {
            let mut b = budget();
            let mut t = BudgetTracker::default();
            t.initialize(&b);
            t.goal(&mut b, aggregate(300, 1, Some(items)));
            assert_eq!(b.coverage, BudgetCoverage::Incomplete);
        }
    }

    #[test]
    fn sa1b_budget_bad_overlap_keeps_root_exhaustion_signal() {
        let mut b = budget();
        let mut t = BudgetTracker::default();
        t.initialize(&b);
        t.expected
            .insert(("未报告".into(), 1), activity("未报告", 1, None));
        t.goal(&mut b, aggregate(1000, 1, Some(vec![])));
        assert!(b.exhausted);
        assert_eq!(b.coverage, BudgetCoverage::Incomplete);
        assert_eq!(b.tokens_charged, 900);
    }

    #[test]
    fn sa1b_budget_overlap_cannot_refund_confirmed_charges() {
        let mut b = budget();
        let mut t = BudgetTracker::default();
        t.initialize(&b);
        let k = activity("子", 1, None);
        t.activity(&mut b, usage(k.clone(), 200, 1, Some(0)));
        t.goal(
            &mut b,
            aggregate(200, 1, Some(vec![watermark(k, 200, 200)])),
        );
        assert_eq!(b.tokens_charged, 200);
        assert_eq!(
            b.coverage,
            BudgetCoverage::Incomplete,
            "区间冲突必须重查，不能退款后继续开轮"
        );
    }

    #[test]
    fn sa1b_budget_old_duplicates_cannot_repair_newer_invalid_evidence() {
        let mut b = budget();
        let mut t = BudgetTracker::default();
        t.initialize(&b);
        let k = activity("子", 1, None);
        t.activity(&mut b, usage(k.clone(), 100, 1, Some(0)));
        t.activity(&mut b, usage(k.clone(), 90, 2, Some(0)));
        t.activity(&mut b, usage(k.clone(), 100, 1, Some(0)));
        assert_eq!(b.coverage, BudgetCoverage::Incomplete);
        t.activity(&mut b, usage(k.clone(), 110, 3, Some(0)));
        assert_eq!(b.coverage, BudgetCoverage::Complete);
        t.goal(
            &mut b,
            aggregate(210, 1, Some(vec![watermark(k.clone(), 110, 110)])),
        );
        t.goal(
            &mut b,
            aggregate(200, 2, Some(vec![watermark(k.clone(), 110, 110)])),
        );
        t.goal(
            &mut b,
            aggregate(210, 1, Some(vec![watermark(k.clone(), 110, 110)])),
        );
        assert_eq!(b.coverage, BudgetCoverage::Incomplete);
        t.goal(
            &mut b,
            aggregate(220, 3, Some(vec![watermark(k, 110, 110)])),
        );
        assert_eq!(b.coverage, BudgetCoverage::Complete);
    }
}
