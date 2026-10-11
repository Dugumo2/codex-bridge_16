//! 运行保护初始化及预算核实接线；上游线程累计用量没有活动基线时保持不完整。
use super::*;
use crate::claim::{Operation, RequestContext};
use serde_json::{json, Value};

impl Machine {
    pub(crate) fn initial_convergence_budget(&self, thread: &str) -> Option<RunBudget> {
        let goal = self.convergences.goal_evidence.get(thread)?;
        let limit = goal.get("tokenBudget")?.as_u64()?;
        let used = goal.get("tokensUsed").and_then(Value::as_u64);
        Some(RunBudget {
            token_budget: limit,
            used_at_start: used.unwrap_or(0),
            tokens_charged: 0,
            coverage: BudgetCoverage::Incomplete,
            exhausted: used.is_some_and(|n| n >= limit),
            source: BudgetSource::GoalAggregate,
            goal_id: Some(goal_identity(goal)),
            goal_aggregate_watermark: used.unwrap_or(0),
            activities: vec![],
        })
    }

    pub(crate) fn drive_convergence_safeguards(&mut self, actions: &mut Vec<Action>) {
        if !self.convergence_enabled() || self.closing || self.quiescing {
            return;
        }
        let threads: Vec<_> = self
            .claims
            .iter()
            .filter(|(_, c)| {
                !c.joined()
                    && !c.settling()
                    && !c.precheck()
                    && !c.owner_gone
                    && matches!(
                        c.state,
                        State::Running { .. } | State::AwaitingContinuation { .. }
                    )
                    && c.run_id
                        .as_ref()
                        .is_some_and(|r| !self.convergences.runs.contains_key(r))
            })
            .map(|(t, _)| t.clone())
            .collect();
        for thread in threads {
            let before = actions.len();
            if let Some(run) = self.convergence_begin(&thread, actions) {
                // 先建立可信身份和计数；总期限从首次收敛门禁开始。
                self.convergences
                    .runs
                    .get_mut(&run)
                    .unwrap()
                    .deadline_mono_ms = u64::MAX;
                let kept: Vec<_> = actions
                    .drain(before..)
                    .filter(|a| {
                        !matches!(
                            a,
                            Action::Schedule {
                                kind: TimerKind::ConvergeDeadline { .. },
                                ..
                            }
                        )
                    })
                    .collect();
                actions.extend(kept);
                let join = self
                    .runs
                    .runs
                    .get_mut(&run)
                    .unwrap()
                    .subagent_join
                    .as_mut()
                    .unwrap();
                join.phase = JoinPhase::Running;
                join.deadline_at = 0;
                self.convergence_changed(&run, actions);
            }
        }
        let tokens: Vec<_> = self
            .convergences
            .runs
            .values()
            .map(|r| r.token.clone())
            .collect();
        for token in tokens {
            if !self.safeguards_token_current(&token) {
                continue;
            }
            self.convergence_sync_facts(&token.run_id);
            if let Some(facts) = self.convergence_facts(&token.run_id) {
                for fact in facts.activities {
                    self.require_budget_activity(&token, fact.key, actions);
                }
            }
            if self.runs.runs[&token.run_id].run_budget.is_none() {
                if let Some(budget) = self.initial_convergence_budget(&token.claim.thread_id) {
                    self.runs.runs.get_mut(&token.run_id).unwrap().run_budget = Some(budget);
                }
            }
            if let Some(goal) = self
                .convergences
                .goal_evidence
                .get(&token.claim.thread_id)
                .cloned()
            {
                self.convergence_budget_evidence(&token, &goal, actions);
            }
            match self.reconcile_budget_admission(&token) {
                BudgetAdmission::Exhausted => {
                    self.convergence_fail(&token.run_id, TerminalReason::BudgetExhausted, actions)
                }
                BudgetAdmission::NeedsVerification => {
                    self.verify_run_budget(&token, actions);
                    if let Some(verification) = self.take_budget_verification(&token) {
                        actions.push(Action::Schedule {
                            token: token.claim.clone(),
                            kind: TimerKind::BudgetVerifyDeadline {
                                token: token.clone(),
                            },
                            delay_ms: verification
                                .deadline_mono_ms
                                .saturating_sub(self.clock.mono_ms),
                        });
                        actions.push(Action::Request(RequestContext {
                            resume_retried: false,
                            stream_id: None,
                            review_id: None,
                            client: None,
                            token: Some(token.claim.clone()),
                            method: "thread/goal/get".into(),
                            params: Some(json!({"threadId":token.claim.thread_id})),
                            operation: Operation::BudgetVerify { verification },
                        }));
                    }
                }
                _ => {}
            }
        }
    }

    fn convergence_budget_evidence(
        &mut self,
        token: &ConvergeToken,
        goal: &Value,
        actions: &mut Vec<Action>,
    ) {
        if let Some(tokens_used) = goal.get("tokensUsed").and_then(Value::as_u64) {
            self.observe_goal_budget(
                token,
                GoalBudgetEvidence {
                    goal_id: goal_identity(goal),
                    tokens_used,
                    evidence_version: tokens_used,
                    // 公开 goal/get 不提供逐活动归集区间，不能伪造交叠证明。
                    covered_activities: None,
                },
                actions,
            );
        }
        if goal.get("status").and_then(Value::as_str) == Some("complete") {
            self.freeze_goal_budget(token, actions);
        }
    }

    pub(crate) fn convergence_budget_reply(
        &mut self,
        verification: &crate::runs::budget::BudgetVerification,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        if !self.convergence_token_current(&verification.token)
            || self.clock.mono_ms >= verification.deadline_mono_ms
        {
            return;
        }
        if let Ok(value) = result {
            if let Some(goal) = value.get("goal") {
                self.convergence_budget_evidence(&verification.token, goal, actions);
            }
        }
    }
}

fn goal_identity(goal: &Value) -> String {
    goal.get("id")
        .or_else(|| goal.get("goalId"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| format!("{}:{}", goal["createdAt"], goal["objective"]))
}
