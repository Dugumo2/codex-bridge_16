//! 覆盖证明与消费汇总只向保守方向推进；辅助配对缓存不拥有这些事实。
use super::{turn_started_ms, Machine, Value, COVERAGE_TURNS};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StartProof {
    pub(crate) conn_epoch: u64,
    pub(crate) observation_gen: u64,
    pub(crate) start_seq: u64,
    pub(crate) raw_seq_at_start: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CompleteProof {
    pub(super) start: StartProof,
    pub(super) end_seq: u64,
    pub(super) completion_fingerprint: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Consumption {
    Unseen,
    CollaborationOnly,
    ContainsOther,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum TurnCoverage {
    Veto {
        reason: String,
    },
    Open {
        start: StartProof,
        consumption: Consumption,
    },
    Complete {
        proof: CompleteProof,
        consumption: Consumption,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CoverageDisposition {
    Wait,
    AllowCollaborationExclusion,
    OrdinaryGap,
}

/// 显式按键排序，摘要不依赖 serde_json 的映射特性或插入顺序。
fn stable_fingerprint(value: &Value) -> [u8; 32] {
    fn canonical(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let sorted: std::collections::BTreeMap<_, _> = map
                    .iter()
                    .map(|(key, value)| (key.clone(), canonical(value)))
                    .collect();
                Value::Object(sorted.into_iter().collect())
            }
            Value::Array(values) => Value::Array(values.iter().map(canonical).collect()),
            value => value.clone(),
        }
    }
    Sha256::digest(serde_json::to_vec(&canonical(value)).expect("JSON 可序列化")).into()
}

fn completion_fingerprint(value: &Value) -> Option<[u8; 32]> {
    let status = value.get("status")?.as_str()?;
    if !matches!(status, "completed" | "failed" | "interrupted") {
        return None;
    }
    // 完成事实只取终态；错误正文和新增展示字段不改变这一轮是否已经结束。
    Some(stable_fingerprint(&serde_json::json!({"status":status})))
}

fn key(thread: &str, turn: &str) -> Option<(u64, String, String)> {
    if thread.is_empty() || thread.len() > 256 || turn.len() > 256 {
        return None;
    }
    Some((turn_started_ms(turn)?, thread.to_owned(), turn.to_owned()))
}

impl Machine {
    /// 不存在的旧轮不能越过水位重生；同毫秒也保守拒绝。
    fn coverage_insert(&mut self, key: (u64, String, String), value: TurnCoverage) {
        let log = &mut self.runs.event_log;
        if log.coverage.contains_key(&key) {
            return;
        }
        if log.evicted_through_ms.is_some_and(|floor| key.0 <= floor) {
            return;
        }
        if log.coverage.len() >= COVERAGE_TURNS {
            let oldest = log.coverage.first_key_value().expect("非空覆盖表").0 .0;
            if key.0 <= oldest {
                log.evicted_through_ms = Some(log.evicted_through_ms.unwrap_or(0).max(key.0));
                return;
            }
            let (old, _) = log.coverage.pop_first().expect("非空覆盖表");
            log.evicted_through_ms = Some(log.evicted_through_ms.unwrap_or(0).max(old.0));
            log.fingerprints
                .retain(|(thread, turn, _), _| thread != &old.1 || turn != &old.2);
        }
        log.coverage.insert(key, value);
    }

    /// 辅助开始表淘汰不能使已见过或已越过淘汰水位的轮重新成为真实开始。
    pub(crate) fn coverage_turn_seen(&self, thread: &str, turn: &str) -> bool {
        let Some(key) = key(thread, turn) else {
            return true;
        };
        self.runs.event_log.coverage.contains_key(&key)
            || self
                .runs
                .event_log
                .evicted_through_ms
                .is_some_and(|floor| key.0 <= floor)
    }

    pub(crate) fn coverage_start(&mut self, thread: &str, turn: &str, proof: Option<StartProof>) {
        let Some(key) = key(thread, turn) else {
            return;
        };
        let value = proof.map_or_else(
            || TurnCoverage::Veto {
                reason: "缺少真实开始或先验 raw 见证".into(),
            },
            |start| TurnCoverage::Open {
                start,
                consumption: Consumption::Unseen,
            },
        );
        self.coverage_insert(key, value);
    }

    pub(crate) fn coverage_open_proof(&self, thread: &str, turn: &str) -> Option<&StartProof> {
        match self.runs.event_log.coverage.get(&key(thread, turn)?)? {
            TurnCoverage::Open { start, .. } => Some(start),
            _ => None,
        }
    }

    /// 返回新增矛盾，调用方据此封锁后续 raw 授信。一致重复完成不读当前凭据。
    pub(crate) fn coverage_complete(
        &mut self,
        thread: &str,
        turn: &str,
        completed: &Value,
        end_seq: u64,
        valid_open: bool,
    ) -> bool {
        let Some(key) = key(thread, turn) else {
            return true;
        };
        let fingerprint = completion_fingerprint(completed);
        let known = self.runs.event_log.coverage.get(&key).cloned();
        let (next, contradiction) = match (known, fingerprint) {
            (Some(TurnCoverage::Complete { proof, .. }), Some(fingerprint))
                if proof.completion_fingerprint == fingerprint =>
            {
                return false
            }
            (Some(TurnCoverage::Veto { .. }), _) => return false,
            (Some(TurnCoverage::Open { start, consumption }), Some(fingerprint)) if valid_open => (
                TurnCoverage::Complete {
                    proof: CompleteProof {
                        start,
                        end_seq,
                        completion_fingerprint: fingerprint,
                    },
                    consumption,
                },
                false,
            ),
            (Some(TurnCoverage::Complete { .. }), _) => (
                TurnCoverage::Veto {
                    reason: "完成终态存在矛盾".into(),
                },
                true,
            ),
            (_, None) => (
                TurnCoverage::Veto {
                    reason: "无法解释的完成终态".into(),
                },
                true,
            ),
            _ => (
                TurnCoverage::Veto {
                    reason: "结束前缺少连续覆盖".into(),
                },
                false,
            ),
        };
        if let std::collections::btree_map::Entry::Occupied(mut entry) =
            self.runs.event_log.coverage.entry(key.clone())
        {
            entry.insert(next);
        } else {
            self.coverage_insert(key, next);
        }
        contradiction
    }

    /// 生命周期丢失只撤销开放轮；无法排除历史消费反证时也撤销封存轮。
    pub(crate) fn coverage_veto(
        &mut self,
        thread: Option<&str>,
        turn: Option<&str>,
        reason: &str,
        include_complete: bool,
    ) {
        for ((_, known_thread, known_turn), coverage) in &mut self.runs.event_log.coverage {
            if thread.is_none_or(|thread| thread == known_thread)
                && turn.is_none_or(|turn| turn == known_turn)
                && (include_complete || matches!(coverage, TurnCoverage::Open { .. }))
            {
                *coverage = TurnCoverage::Veto {
                    reason: reason.chars().take(256).collect(),
                };
            }
        }
        if let (Some(thread), Some(turn)) = (thread, turn) {
            if let Some(key) = key(thread, turn) {
                self.coverage_insert(
                    key,
                    TurnCoverage::Veto {
                        reason: reason.chars().take(256).collect(),
                    },
                );
            }
        }
    }

    /// 在配对去重前汇总原始消费；完成后的精确重复只核对指纹，不改变证明。
    pub(crate) fn coverage_consume(
        &mut self,
        thread: &str,
        turn: &str,
        item_id: &str,
        raw: &Value,
        collaboration: bool,
    ) -> bool {
        let Some(key) = key(thread, turn) else {
            return true;
        };
        let fingerprint_key = (thread.to_owned(), turn.to_owned(), item_id.to_owned());
        let fingerprint = stable_fingerprint(raw);
        let known = self.runs.event_log.coverage.get(&key);
        if matches!(known, Some(TurnCoverage::Veto { .. })) {
            return false;
        }
        let registered = self.runs.event_log.fingerprints.get(&fingerprint_key);
        let conflict = registered.is_some_and(|known| known != &fingerprint);
        let late = matches!(known, Some(TurnCoverage::Complete { .. }))
            && registered != Some(&fingerprint);
        if item_id.is_empty() || item_id.len() > 256 || conflict || late {
            self.coverage_veto(Some(thread), Some(turn), "迟到、冲突或无法登记的消费", true);
            return true;
        }
        if matches!(known, Some(TurnCoverage::Complete { .. })) {
            return false;
        }
        if known.is_none() {
            self.coverage_insert(
                key,
                TurnCoverage::Veto {
                    reason: "消费前没有真实开始".into(),
                },
            );
            return false;
        }
        if registered.is_none() && self.runs.event_log.fingerprints.len() >= COVERAGE_TURNS {
            self.coverage_veto(Some(thread), Some(turn), "消费指纹容量不足", true);
            return false;
        }
        self.runs
            .event_log
            .fingerprints
            .insert(fingerprint_key, fingerprint);
        if let Some(TurnCoverage::Open { consumption, .. }) =
            self.runs.event_log.coverage.get_mut(&key)
        {
            *consumption = if !collaboration || *consumption == Consumption::ContainsOther {
                Consumption::ContainsOther
            } else {
                Consumption::CollaborationOnly
            };
        }
        false
    }

    pub(crate) fn coverage_disposition(&self, thread: &str, turn: &str) -> CoverageDisposition {
        let Some(key) = key(thread, turn) else {
            return CoverageDisposition::OrdinaryGap;
        };
        match self.runs.event_log.coverage.get(&key) {
            Some(TurnCoverage::Open { start, consumption })
                if *consumption != Consumption::ContainsOther
                    && self.coverage_open_valid(thread, turn, start) =>
            {
                CoverageDisposition::Wait
            }
            Some(TurnCoverage::Complete {
                consumption: Consumption::CollaborationOnly,
                ..
            }) => {
                if self.runs.event_log.processing_step
                    || self.coverage_read_deadline(thread).is_some()
                {
                    CoverageDisposition::Wait
                } else {
                    CoverageDisposition::AllowCollaborationExclusion
                }
            }
            _ => CoverageDisposition::OrdinaryGap,
        }
    }
}
