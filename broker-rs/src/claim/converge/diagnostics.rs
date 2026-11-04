//! 在线诊断只读取事实；绝不重新发现、同步账本、绑定活动或推进 epoch。
use super::*;
use crate::relations::{EvidenceKind, EvidenceOutcome, MemberActivity, V2FallbackReason};
use serde_json::{json, Value};

const MAX_ITEMS: usize = 32;
const MAX_NESTED: usize = 8;
const MAX_IDENTIFIER_BYTES: usize = 128;
// 留出 RPC 外层对象开销，保证整个 diagnostic 回包仍小于 16 KiB。
const MAX_BYTES: usize = 16 * 1024 - 64;

fn evidence_kind_label(kind: EvidenceKind) -> &'static str {
    match kind {
        EvidenceKind::V1Spawn => "v1Spawn",
        EvidenceKind::V1SendInput => "v1SendInput",
        EvidenceKind::V1ResumeAgent => "v1ResumeAgent",
        EvidenceKind::V2Started => "v2Started",
        EvidenceKind::V2Interacted => "v2Interacted",
        EvidenceKind::V2Interrupted => "v2Interrupted",
        EvidenceKind::V2Completed => "v2Completed",
    }
}

fn evidence_outcome_label(outcome: EvidenceOutcome) -> &'static str {
    match outcome {
        EvidenceOutcome::Pending => "pending",
        EvidenceOutcome::Proven => "proven",
        EvidenceOutcome::Unproven => "unproven",
        EvidenceOutcome::Failed => "failed",
        EvidenceOutcome::Revoked => "revoked",
    }
}

fn identifier(value: &str, omitted: &mut usize) -> String {
    let mut end = value.len().min(MAX_IDENTIFIER_BYTES);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    *omitted += value.len() - end;
    value[..end].to_owned()
}

impl Machine {
    pub fn convergence_diagnostics(&self, run: &str) -> Option<Value> {
        let runtime = self.convergences.runs.get(run)?;
        let record = self.runs.runs.get(run)?;
        let join = record.subagent_join.as_ref()?;
        if matches!(join.phase, JoinPhase::Released | JoinPhase::Abnormal)
            || matches!(
                record.state,
                crate::runs::RunState::Ended | crate::runs::RunState::Lost
            )
        {
            return None;
        }
        let facts = self.convergence_facts(run)?;
        let ledger = self.convergences.ledgers.get(run)?;
        let context = self.relations.contexts.get(run)?;
        let progress = self.convergences.progress.get(run)?;
        let thread = &runtime.token.claim.thread_id;
        let mut gates = Vec::<&str>::new();
        // 顺序与 drive_once 的短路门一致，后续条目补充同一快照中仍成立的事实门。
        if runtime.automatic_control_revoked {
            gates.push("automaticControlRevoked");
        }
        if self.closing || self.quiescing {
            gates.push("brokerClosingOrQuiescing");
        }
        // 连接或授权身份失配时，convergence_facts 已返回 None，不能展示旧快照。
        if self
            .goal_terminal(
                run,
                self.records
                    .get(thread)
                    .and_then(|r| r.goal_status.as_deref()),
            )
            .is_some()
            && !self.goal_restoration_pending(thread)
        {
            gates.push("goalTerminal");
        }
        if matches!(
            self.claims.get(thread).map(|c| &c.state),
            Some(State::Converging { .. })
        ) && self.budget_verification_expired(&runtime.token)
        {
            gates.push("budgetVerificationExpired");
        }
        if self.clock.mono_ms >= runtime.deadline_mono_ms {
            gates.push("convergenceDeadline");
        }
        if !matches!(
            self.claims.get(thread).map(|c| &c.state),
            Some(State::Converging { .. })
        ) && self.reconcile_goal_target(&runtime.token).is_none()
        {
            gates.push("parentNotSettled");
        }
        if facts.work_epoch == 0 && self.reconcile_goal_target(&runtime.token).is_some() {
            gates.push("rootGoalTurnInFlight");
        }
        if !facts.conflicts.is_empty() {
            gates.push("conflicts");
        }
        if self.convergence_root_unsettled(run) {
            gates.push("rootUnsettled");
        }
        if self.claims.get(thread).is_some_and(|c| c.request_pending) {
            gates.push("requestInFlight");
        }
        if self.stops.changes.values().any(|c| &c.thread == thread) {
            gates.push("stopChangeInFlight");
        }
        if progress.discovery.is_some() {
            gates.push("discoveryInFlight");
        }
        if progress.reading.is_some() {
            gates.push("resultReadInFlight");
        }
        if progress.retry_at.is_some() {
            gates.push("retryScheduled");
        }
        if progress
            .operations
            .values()
            .any(|o| o.outcome_unknown || o.submission == ReconcileSubmission::OutcomeUnknown)
        {
            gates.push("reconcileOutcomeUnknown");
        }
        if progress
            .operations
            .values()
            .any(|o| o.submission == ReconcileSubmission::NotSent)
        {
            gates.push("reconcileSubmissionInFlight");
        }
        if facts.discovery_covered_epoch != Some(facts.work_epoch) {
            gates.push("discoveryEpochUncovered");
        }
        if !facts.discovery.complete {
            gates.push("discoveryIncomplete");
        }
        if facts.pending_starts > 0 {
            gates.push("pendingOperations");
        }
        if facts.pending_requests > 0 {
            gates.push("pendingRequests");
        }
        if facts.unresolved_lookups > 0 {
            gates.push("unresolvedLookups");
        }
        if facts.unresolved_candidates > 0 {
            gates.push("unresolvedCandidates");
        }
        let unverified = facts.activities.iter().filter(|a| !a.verified).count();
        let unsettled = facts
            .activities
            .iter()
            .filter(|a| a.activity != MemberActivity::Settled)
            .count();
        let missing = facts
            .activities
            .iter()
            .filter(|a| a.result_status.is_none())
            .count();
        let active = facts
            .activities
            .iter()
            .filter(|a| a.active_goal == Some(true))
            .count();
        let unknown = facts
            .activities
            .iter()
            .filter(|a| a.active_goal.is_none())
            .count();
        if unverified > 0 {
            gates.push("activityUnverified");
        }
        if unsettled > 0 {
            gates.push("activityUnsettled");
        }
        if missing > 0 {
            gates.push("activityResultUnknown");
        }
        if active > 0 {
            gates.push("activityGoalActive");
        }
        if unknown > 0 {
            gates.push("activityGoalUnknown");
        }
        if facts.activities.iter().any(|a| a.pending_requests > 0) {
            gates.push("activityPendingRequests");
        }
        if gates.is_empty() {
            if facts.work_epoch == 0 || join.reconciled_epoch == facts.work_epoch {
                gates.push(if self.reconcile_goal_target(&runtime.token).is_some() {
                    "rootGoalTurnInFlight"
                } else {
                    "readyToRelease"
                });
            } else if join.delivered_epoch >= facts.work_epoch {
                gates.push("awaitingReconcile");
            } else {
                gates.push(match self.thread_capability(thread) {
                    None => "rootCapabilityUnknown",
                    Some(false) => "rootNoDirectInput",
                    Some(true) => {
                        let config = record.convergence_config.clone().unwrap_or_default();
                        let merging_goal = self.reconcile_goal_target(&runtime.token).is_some();
                        if !merging_goal && join.wakeups_started >= config.max_wakeups {
                            "wakeLimit"
                        } else if !merging_goal
                            && record.auto_turns_started.unwrap_or(0) >= config.max_auto_turns
                        {
                            "autoTurnLimit"
                        } else {
                            match self.convergence_budget(&runtime.token) {
                                BudgetAdmission::Exhausted => "budgetExhausted",
                                BudgetAdmission::NeedsVerification => "budgetVerification",
                                BudgetAdmission::Disabled => "budgetUnavailable",
                                BudgetAdmission::Allowed => "preparingReconcile",
                            }
                        }
                    }
                });
            }
        }
        let omitted_gates = gates.len().saturating_sub(MAX_NESTED);
        gates.truncate(MAX_NESTED);
        let mut omitted_bytes = 0;
        let pending_keys: Vec<_> = ledger.pending.union(&context.in_flight_spawns).collect();
        let pending: Vec<_> = pending_keys.iter().take(MAX_ITEMS).map(|key| {
            let op = self.relations.operations.get(*key);
            // 等待配对的操作没有期限；模式只供排查，不参与门禁、覆盖问题或告警。
            let deadline = op.filter(|o| !self.v2_awaits_pairing(o));
            let pairing_mode = op.filter(|o| o.kind == EvidenceKind::V2Interacted)
                .map(|o| if o.v2_strict { "strict" } else { "fallback" });
            let fallback_reason = op.filter(|o| o.kind == EvidenceKind::V2Interacted)
                .and_then(|o| o.v2_fallback).map(|reason| match reason {
                    V2FallbackReason::ParentWithoutRaw => "parentWithoutRaw",
                    V2FallbackReason::StartMissing => "startMissing",
                    V2FallbackReason::StartStale => "startStale",
                });
            let receiver_ids: Vec<_> = if let Some(op) = op { op.receivers.iter().collect() } else {
                ledger.placeholder_operations.iter().filter(|(_, reasons)| reasons.contains(*key)).map(|(thread, _)| thread).collect()
            };
            let receivers: Vec<_> = receiver_ids.iter().take(MAX_NESTED)
                .map(|s| identifier(s, &mut omitted_bytes)).collect();
            let proven: Vec<_> = op.into_iter().flat_map(|o| o.proven_turns.iter()).take(MAX_NESTED).map(|(thread, turns)| {
                json!({"threadId":identifier(thread, &mut omitted_bytes),
                    "turnIds":turns.iter().take(MAX_NESTED).map(|s| identifier(s, &mut omitted_bytes)).collect::<Vec<_>>(),
                    "omittedTurns":turns.len().saturating_sub(MAX_NESTED)})
            }).collect();
            json!({"kind":op.map(|o| evidence_kind_label(o.kind)).unwrap_or("unknown"),
                "outcome":op.map(|o| evidence_outcome_label(o.outcome)).unwrap_or("unknown"),
                "receivers":receivers,"provenTurns":proven,
                "pairingMode":pairing_mode,"fallbackReason":fallback_reason,
                "expiresAt":deadline.map(|o| if o.expires_at_ms >= self.clock.mono_ms {
                    self.clock.now_ms.saturating_add(o.expires_at_ms - self.clock.mono_ms)
                } else { self.clock.now_ms.saturating_sub(self.clock.mono_ms - o.expires_at_ms) }),
                "expired":deadline.is_some_and(|o| self.clock.mono_ms >= o.expires_at_ms),
                "missing":op.is_none(),"omittedReceivers":receiver_ids.len().saturating_sub(MAX_NESTED),
                "omittedProvenTurns":op.map_or(0, |o| o.proven_turns.len().saturating_sub(MAX_NESTED))})
        }).collect();
        let activities: Vec<_> = facts.activities.iter().take(MAX_ITEMS).map(|a| json!({
            "threadId":identifier(&a.key.thread_id, &mut omitted_bytes),
            "turnId":a.key.turn_id.as_deref().map(|s| identifier(s, &mut omitted_bytes)),
            "activitySeq":a.key.activity_seq,"verified":a.verified,"activity":a.activity,
            "resultStatus":a.result_status,"goal":a.active_goal.map_or(json!("unknown"), |v| json!(v)),
            "pendingRequests":a.pending_requests
        })).collect();
        let mut snapshot = json!({
            "runId":identifier(run, &mut omitted_bytes),"collectedAt":self.clock.now_ms,"revision":facts.revision,
            "connectionGeneration":self.relations.conn_epoch,"workEpoch":facts.work_epoch,"coveredEpoch":facts.discovery_covered_epoch,
            "gate":gates[0],"gates":gates,
            "counts":{"pendingOperations":facts.pending_starts,"pendingRequests":facts.pending_requests,
                "unresolvedLookups":facts.unresolved_lookups,"unresolvedCandidates":facts.unresolved_candidates,
                "conflicts":facts.conflicts.len(),"activities":facts.activities.len(),"unverifiedActivities":unverified,
                "unsettledActivities":unsettled,"missingResults":missing,"activeGoals":active,"unknownGoals":unknown},
            "pendingOperations":pending,"activities":activities,
            "omitted":{"pendingOperations":pending_keys.len().saturating_sub(MAX_ITEMS),
                "activities":facts.activities.len().saturating_sub(MAX_ITEMS),"identifierBytes":omitted_bytes,"gates":omitted_gates}
        });
        bound_snapshot(&mut snapshot);
        Some(snapshot)
    }
}

fn bound_snapshot(snapshot: &mut Value) {
    // JSON 转义也计入预算。逐项撤回保证 UTF-8 和 JSON 始终完整，计数保持真实。
    while serde_json::to_vec(snapshot).expect("诊断可序列化").len() > MAX_BYTES {
        let field = if !snapshot["pendingOperations"].as_array().unwrap().is_empty() {
            "pendingOperations"
        } else {
            "activities"
        };
        if snapshot[field].as_array_mut().unwrap().pop().is_none() {
            break;
        }
        let omitted = snapshot["omitted"][field].as_u64().unwrap();
        snapshot["omitted"][field] = json!(omitted + 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claim::{step, ClientRequest, Event};
    use crate::relations::{
        EvidenceKey, EvidenceKind, EvidenceOperation, EvidenceOutcome, V2FallbackReason,
    };
    use crate::test_support::subagent::SubagentMock;
    use std::collections::{BTreeMap, BTreeSet};

    const SENTINEL: &str = "正文哨兵_绝不能出现在诊断_秘密目标输出异常";

    fn apply(machine: &mut Machine, event: Event) -> Vec<Action> {
        let (next, actions) = step(std::mem::take(machine), event);
        *machine = next;
        actions
    }

    fn fixture() -> Machine {
        let mut machine = Machine::default();
        assert!(machine.enable_convergence(ConvergenceReadiness {
            trusted_inputs: true,
            budget_admission: true,
            asynchronous_results: true,
            terminal_mapping: true,
            cancellation_verified: true,
        }));
        apply(&mut machine, Event::Connected { conn: 1 });
        apply(&mut machine, Event::Initialized { conn: 1, cxb: true });
        machine.record("root").subscribed = true;
        let actions = apply(
            &mut machine,
            Event::ClientRequest {
                request: ClientRequest {
                    conn: 1,
                    id: json!(1),
                    method: "turn/start".into(),
                    params: Some(
                        json!({"threadId":"root","input":[{"type":"text","text":SENTINEL}]}),
                    ),
                },
                run_id: Some("run-diagnostic".into()),
            },
        );
        let context = actions
            .into_iter()
            .find_map(|a| match a {
                Action::Request(c) if c.method == "turn/start" => Some(c),
                _ => None,
            })
            .unwrap();
        apply(
            &mut machine,
            Event::Reply {
                context,
                result: Ok(json!({"turn":{"id":"root-turn"}})),
            },
        );
        let (start, end) = SubagentMock::v1_spawn("root", "root-turn", "spawn", "child", false);
        apply(&mut machine, start);
        apply(&mut machine, end);
        machine.convergence_begin("root", &mut vec![]).unwrap();
        machine
    }

    fn add_operation(machine: &mut Machine, index: usize, nested: usize) {
        let key = EvidenceKey {
            parent_thread_id: "root".into(),
            parent_turn_id: "root-turn".into(),
            item_id: format!("op-{index}"),
        };
        let receivers: BTreeSet<_> = (0..nested)
            .map(|i| format!("线程{i}{}", "多字节🦀".repeat(80)))
            .collect();
        let turns: BTreeSet<_> = (0..nested)
            .map(|i| format!("轮{i}{}", "多字节🦀".repeat(80)))
            .collect();
        machine.relations.operations.insert(
            key.clone(),
            EvidenceOperation {
                key: key.clone(),
                run_id: "run-diagnostic".into(),
                conn_epoch: machine.relations.conn_epoch,
                authority_gen: 1,
                kind: EvidenceKind::V2Interacted,
                started_seen: true,
                completed_seen: true,
                outcome: EvidenceOutcome::Unproven,
                proven_turns: receivers
                    .iter()
                    .map(|r| (r.clone(), turns.clone()))
                    .collect(),
                rejected_receivers: BTreeMap::from([("rejected".into(), SENTINEL.into())]),
                receivers,
                baseline_turns: BTreeMap::new(),
                v2_strict: false,
                v2_queue_only: false,
                v2_fallback: Some(V2FallbackReason::ParentWithoutRaw),
                first_seen_ms: 0,
                expires_at_ms: 1,
                parent_ended: true,
                failure: Some(SENTINEL.into()),
            },
        );
        machine
            .convergences
            .ledgers
            .get_mut("run-diagnostic")
            .unwrap()
            .pending
            .insert(key);
    }

    #[test]
    fn diagnostics_are_read_only_and_never_expose_bodies() {
        let mut machine = fixture();
        machine.convergences.goal_evidence.insert(
            "child".into(),
            json!({"objective":SENTINEL,"output":SENTINEL}),
        );
        let record = machine.runs.runs.get_mut("run-diagnostic").unwrap();
        record.meta = Some(json!({"goal":{"objective":SENTINEL}}));
        record.turns.push(crate::runs::TurnResult {
            origin: None,
            reroute: None,
            turn_id: "result-sentinel-turn".into(),
            ready: crate::runs::Ready::Ready,
            status: crate::runs::TurnStatus::Completed,
            output: SENTINEL.into(),
            output_source: crate::runs::OutputSource::FinalAnswer,
            error: Some(json!({"message":SENTINEL})),
            completed_at: 1,
        });
        add_operation(&mut machine, 1, 2);
        let before = machine.clone();
        let snapshot = machine.convergence_diagnostics("run-diagnostic").unwrap();
        assert_eq!(machine, before, "诊断查询不得改变任何状态");
        let mut actions = vec![];
        machine.ext_request(
            ClientRequest {
                conn: 1,
                id: json!(2),
                method: "broker/runs/convergenceDiagnostics".into(),
                params: Some(json!({"runId":"run-diagnostic"})),
            },
            &mut actions,
        );
        assert_eq!(
            machine, before,
            "诊断 RPC 不得改变绑定、pending、epoch 或状态"
        );
        assert!(actions.iter().any(|a| matches!(a, Action::Send { message, .. } if message["result"]["diagnostic"] == snapshot)));
        assert!(
            !snapshot.to_string().contains(SENTINEL),
            "诊断不得泄漏正文哨兵"
        );
        assert!(snapshot["pendingOperations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|op| op["kind"] == "v2Interacted"));
        assert!(snapshot["gates"]
            .as_array()
            .unwrap()
            .contains(&json!("pendingOperations")));
    }

    #[test]
    fn diagnostics_bound_nested_sets_and_utf8_identifiers() {
        let mut machine = fixture();
        add_operation(&mut machine, 1, 40);
        let snapshot = machine.convergence_diagnostics("run-diagnostic").unwrap();
        let op = snapshot["pendingOperations"]
            .as_array()
            .unwrap()
            .iter()
            .find(|op| op["kind"] == "v2Interacted")
            .unwrap();
        assert_eq!(op["receivers"].as_array().unwrap().len(), 8);
        assert_eq!(op["omittedReceivers"], 32);
        assert_eq!(op["provenTurns"].as_array().unwrap().len(), 8);
        assert_eq!(op["provenTurns"][0]["turnIds"].as_array().unwrap().len(), 8);
        assert_eq!(op["provenTurns"][0]["omittedTurns"], 32);
        for id in op["receivers"].as_array().unwrap() {
            assert!(id.as_str().unwrap().len() <= 128);
            assert!(!id.as_str().unwrap().contains('\u{fffd}'));
        }
        assert!(snapshot["omitted"]["identifierBytes"].as_u64().unwrap() > 0);
    }

    #[test]
    fn diagnostics_enforce_total_utf8_budget_and_item_limits() {
        let mut machine = fixture();
        for i in 0..70 {
            add_operation(&mut machine, i, 8);
        }
        let ledger = machine
            .convergences
            .ledgers
            .get_mut("run-diagnostic")
            .unwrap();
        let activity = ledger.activities[0].clone();
        for i in 0..70 {
            let mut item = activity.clone();
            item.key.thread_id = format!("{i}{}", "多字节🦀".repeat(80));
            item.key.turn_id = Some("轮次🦀".repeat(80));
            ledger.activities.push(item);
        }
        let snapshot = machine.convergence_diagnostics("run-diagnostic").unwrap();
        assert!(snapshot["gates"].as_array().unwrap().len() <= 8);
        let encoded = serde_json::to_vec(&json!({"diagnostic":snapshot})).unwrap();
        assert!(
            encoded.len() <= 16 * 1024,
            "诊断 UTF-8 回包超出 16 KiB: {}",
            encoded.len()
        );
        assert!(snapshot["pendingOperations"].as_array().unwrap().len() <= 32);
        assert!(snapshot["activities"].as_array().unwrap().len() <= 32);
        assert!(snapshot["omitted"]["pendingOperations"].as_u64().unwrap() > 0);
        assert!(snapshot["omitted"]["activities"].as_u64().unwrap() > 0);
        assert!(serde_json::from_slice::<Value>(&encoded).is_ok());
    }

    #[test]
    fn diagnostics_keep_expired_and_missing_pending_visible_and_clear_terminal() {
        let mut machine = fixture();
        add_operation(&mut machine, 1, 1);
        machine.clock.mono_ms = 2;
        machine.clock.now_ms = 100;
        let expired = machine.convergence_diagnostics("run-diagnostic").unwrap();
        assert!(expired["pendingOperations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|op| op["expired"] == true));
        assert!(expired["pendingOperations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|op| op["kind"] == "v2Interacted" && op["expiresAt"] == 99));
        machine.relations.operations.clear();
        let missing = machine.convergence_diagnostics("run-diagnostic").unwrap();
        assert!(missing["pendingOperations"]
            .as_array()
            .unwrap()
            .iter()
            .all(|op| op["missing"] == true));
        for terminal in [crate::runs::RunState::Ended, crate::runs::RunState::Lost] {
            machine.runs.runs.get_mut("run-diagnostic").unwrap().state = terminal;
            assert_eq!(machine.convergence_diagnostics("run-diagnostic"), None);
        }
        machine.runs.runs.get_mut("run-diagnostic").unwrap().state = crate::runs::RunState::Running;
        machine
            .runs
            .runs
            .get_mut("run-diagnostic")
            .unwrap()
            .subagent_join
            .as_mut()
            .unwrap()
            .phase = JoinPhase::Released;
        assert_eq!(machine.convergence_diagnostics("run-diagnostic"), None);
        assert_eq!(
            machine
                .convergence_summary("run-diagnostic")
                .unwrap()
                .convergence_diagnostic,
            None
        );
        machine.convergences.runs.remove("run-diagnostic");
        assert_eq!(machine.convergence_diagnostics("run-diagnostic"), None);
    }

    #[test]
    fn diagnostics_gates_have_bounded_detail_and_keep_actual_first_branch() {
        let mut machine = fixture();
        let progress = machine
            .convergences
            .progress
            .get_mut("run-diagnostic")
            .unwrap();
        progress.discovery = Some((1, 1));
        progress.reading = Some((BatchReadId(1), 1));
        progress.retry_at = Some(1);
        let ledger = machine
            .convergences
            .ledgers
            .get_mut("run-diagnostic")
            .unwrap();
        ledger.activities[0].pending_requests = 1;
        ledger.activities[0].active_goal = None;
        ledger.activities[0].verified = false;
        let snapshot = machine.convergence_diagnostics("run-diagnostic").unwrap();
        assert_eq!(snapshot["gate"], "parentNotSettled");
        assert_eq!(snapshot["gates"].as_array().unwrap().len(), 8);
        assert!(snapshot["omitted"]["gates"].as_u64().unwrap() > 0);
    }

    #[test]
    fn diagnostics_first_gate_distinguishes_runtime_shutdown_from_revoked_control() {
        let mut machine = fixture();
        machine.closing = true;
        let closing = machine.convergence_diagnostics("run-diagnostic").unwrap();
        assert_eq!(closing["gate"], "brokerClosingOrQuiescing");
        machine.closing = false;
        machine.quiescing = true;
        let quiescing = machine.convergence_diagnostics("run-diagnostic").unwrap();
        assert_eq!(quiescing["gate"], "brokerClosingOrQuiescing");
        machine.quiescing = false;
        machine
            .convergences
            .runs
            .get_mut("run-diagnostic")
            .unwrap()
            .automatic_control_revoked = true;
        let revoked = machine.convergence_diagnostics("run-diagnostic").unwrap();
        assert_eq!(revoked["gate"], "automaticControlRevoked");
    }

    #[test]
    fn diagnostics_rpc_rejects_missing_null_and_non_string_run_id() {
        for params in [
            None,
            Some(json!({})),
            Some(json!({"runId": null})),
            Some(json!({"runId": 7})),
            Some(json!({"runId": ""})),
        ] {
            let mut machine = fixture();
            let actions = apply(
                &mut machine,
                Event::ClientRequest {
                    request: ClientRequest {
                        conn: 1,
                        id: json!(99),
                        method: "broker/runs/convergenceDiagnostics".into(),
                        params,
                    },
                    run_id: None,
                },
            );
            assert!(actions
                .iter()
                .any(|action| matches!(action, Action::Send { message, .. }
                if message["error"]["code"] == -32602)));
        }
    }

    fn rpc(machine: &mut Machine, thread: &str) -> Value {
        let actions = apply(
            machine,
            Event::ClientRequest {
                request: ClientRequest {
                    conn: 1,
                    id: json!(991),
                    method: "broker/runs/convergenceDiagnostics".into(),
                    params: Some(json!({"runId":"run-diagnostic","threadId":thread})),
                },
                run_id: None,
            },
        );
        actions
            .into_iter()
            .find_map(|action| match action {
                Action::Send { message, .. } if message["id"] == 991 => Some(message),
                _ => None,
            })
            .expect("完整请求路由须返回诊断 envelope")
    }

    #[test]
    fn diagnostics_full_rpc_lifecycle_purge_shutdown_and_terminal() {
        for mode in ["lifecycle", "purge", "closing", "quiescing", "terminal"] {
            let mut machine = fixture();
            let thread = if matches!(mode, "lifecycle" | "purge") {
                "idle-thread"
            } else {
                "root"
            };
            if mode == "lifecycle" {
                machine.lifecycle_request(
                    ClientRequest {
                        conn: 1,
                        id: json!(81),
                        method: "thread/archive".into(),
                        params: Some(json!({"threadId":thread})),
                    },
                    crate::store::lifecycle::Kind::Archive,
                    false,
                    &mut vec![],
                );
                assert!(
                    machine.lifecycle_error(thread).is_some(),
                    "前置：生命周期门已存在"
                );
            }
            if mode == "purge" {
                machine.run_purge_request(
                    ClientRequest {
                        conn: 1,
                        id: json!(82),
                        method: "broker/threads/purge".into(),
                        params: Some(json!({"threadId":thread})),
                    },
                    thread,
                    &mut vec![],
                );
                assert!(machine.run_purge_busy(thread), "前置：purge门已存在");
            }
            machine.closing = mode == "closing";
            machine.quiescing = mode == "quiescing";
            if mode == "terminal" {
                machine.runs.runs.get_mut("run-diagnostic").unwrap().state =
                    crate::runs::RunState::Ended;
            }
            let before = machine.convergences.ledgers["run-diagnostic"].clone();
            let reply = rpc(&mut machine, thread);
            assert!(reply.get("error").is_none(), "{mode}: {reply}");
            assert_eq!(machine.convergences.ledgers["run-diagnostic"], before);
            if mode == "terminal" {
                assert!(reply["result"]["diagnostic"].is_null());
            } else {
                assert_eq!(reply["result"]["diagnostic"]["runId"], "run-diagnostic");
            }
            if matches!(mode, "closing" | "quiescing") {
                assert_eq!(
                    reply["result"]["diagnostic"]["gate"],
                    "brokerClosingOrQuiescing"
                );
            }
        }
    }

    #[test]
    fn diagnostics_full_rpc_envelope_is_bounded() {
        let mut machine = fixture();
        for index in 0..70 {
            add_operation(&mut machine, index, 40);
        }
        let reply = rpc(&mut machine, "root");
        assert!(reply.get("error").is_none());
        assert!(serde_json::to_vec(&reply).unwrap().len() <= 16 * 1024);
        assert!(
            reply["result"]["diagnostic"]["omitted"]["pendingOperations"]
                .as_u64()
                .unwrap()
                > 0
        );
    }

    #[test]
    fn diagnostics_first_gate_matches_actual_driver_actions() {
        for mode in ["closing", "parent", "request", "discovery"] {
            let mut machine = fixture();
            machine
                .convergences
                .runs
                .get_mut("run-diagnostic")
                .unwrap()
                .deadline_mono_ms = 100_000;
            machine
                .convergences
                .progress
                .get_mut("run-diagnostic")
                .unwrap()
                .discovery = None;
            machine.claims.get_mut("root").unwrap().request_pending = mode != "discovery";
            machine.closing = mode == "closing";
            if matches!(mode, "request" | "discovery") {
                machine.claims.get_mut("root").unwrap().state = State::Converging {
                    owner: Owner::Client(1),
                    turn_id: Some("root-turn".into()),
                    parent_status: Some(crate::runs::TurnStatus::Completed),
                    goal_status: None,
                    token: machine.convergences.runs["run-diagnostic"].token.clone(),
                    phase: JoinPhase::WaitingSubtree,
                };
            }
            let before = machine.clone();
            let diagnostic = machine.convergence_diagnostics("run-diagnostic").unwrap();
            assert_eq!(machine, before, "诊断不能预先改变驱动状态");
            let expected = match mode {
                "closing" => "brokerClosingOrQuiescing",
                "parent" => "parentNotSettled",
                "request" => "requestInFlight",
                _ => "discoveryEpochUncovered",
            };
            assert_eq!(diagnostic["gate"], expected);
            let mut actions = vec![];
            machine.convergence_drive("run-diagnostic", &mut actions);
            let discovers = actions.iter().any(|a| matches!(a,Action::Request(c) if matches!(c.operation,crate::claim::Operation::Discovery { .. })));
            let retries = actions.iter().any(|a| {
                matches!(
                    a,
                    Action::Schedule {
                        kind: TimerKind::ConvergeRetry { .. },
                        ..
                    }
                )
            });
            match mode {
                "closing" | "parent" => {
                    assert!(
                        actions.is_empty(),
                        "首门 {expected} 必须真正阻止驱动产生动作"
                    );
                    assert_eq!(machine, before);
                }
                "request" => {
                    assert!(
                        retries && !discovers,
                        "请求门必须先排重试，不能越过它发起发现"
                    );
                    assert!(machine.convergences.progress["run-diagnostic"]
                        .discovery
                        .is_none());
                }
                _ => {
                    assert!(discovers && !retries, "发现覆盖门必须真正启动发现");
                    assert!(machine.convergences.progress["run-diagnostic"]
                        .discovery
                        .is_some());
                }
            }
        }
    }
}
