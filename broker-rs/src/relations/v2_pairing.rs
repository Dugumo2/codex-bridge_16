//! v2 因果证据：新轮开始顺序，或接收轮里实际记录的同一输入；不保存正文。
use super::*;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FollowupStart {
    pub epoch: u64,
    pub seq: u64,
    pub at: u64,
    pub run: String,
    pub generation: u64,
    pub target: Option<String>,
    pub message: Option<[u8; 32]>,
    pub trigger: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryFact {
    pub epoch: u64,
    pub seq: u64,
    pub thread: String,
    pub turn: String,
    pub id: String,
    pub author: String,
    pub recipient: String,
    pub message: [u8; 32],
    pub trigger: bool,
}

fn digest(text: &str) -> [u8; 32] {
    Sha256::digest(text.as_bytes()).into()
}

impl Machine {
    pub(super) fn v2_observe_raw(
        &mut self,
        thread: &str,
        turn: &str,
        item: &Value,
        actions: &mut Vec<Action>,
    ) -> bool {
        match item.get("type").and_then(Value::as_str) {
            Some("function_call") => {
                let name = item.get("name").and_then(Value::as_str).unwrap_or("");
                let name = name.rsplit('.').next().unwrap_or("");
                if !matches!(name, "followup_task" | "send_message") {
                    return false;
                }
                let Some(id) = item.get("call_id").and_then(Value::as_str) else {
                    return false;
                };
                let facts::ParentRun::Own { run, .. } = self.parent_run(thread, turn) else {
                    return false;
                };
                if !self.ensure_context(&run) {
                    return false;
                }
                let Some(context) = self.manage_context(&run) else {
                    return false;
                };
                let key = EvidenceKey {
                    parent_thread_id: thread.into(),
                    parent_turn_id: turn.into(),
                    item_id: id.into(),
                };
                // 开始记录晚于 Interacted 时不补造基线；已知操作的身份不可回写。
                if self.relations.operations.contains_key(&key) {
                    return false;
                }
                let generation = context.gen;
                let now = self.clock.mono_ms;
                self.relations.aux.followup_starts.retain(|_, start| {
                    start.epoch == self.relations.conn_epoch
                        && now < start.at.saturating_add(EVIDENCE_TTL_MS)
                });
                if self.relations.aux.followup_starts.len() >= MAX_OPERATIONS {
                    return false;
                }
                let args = item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .and_then(|s| serde_json::from_str::<Value>(s).ok());
                let target = args
                    .as_ref()
                    .and_then(|a| a.get("target"))
                    .and_then(Value::as_str)
                    .filter(|s| s.len() <= 256)
                    .map(str::to_owned);
                let message = args
                    .as_ref()
                    .and_then(|a| a.get("message"))
                    .and_then(Value::as_str)
                    .map(digest);
                self.relations
                    .aux
                    .followup_starts
                    .entry(key)
                    .or_insert(FollowupStart {
                        epoch: self.relations.conn_epoch,
                        seq: self.relations.aux.observation_seq,
                        at: now,
                        run,
                        generation,
                        target,
                        message,
                        trigger: name == "followup_task",
                    });
                false
            }
            Some("agent_message") => {
                let (Some(id), Some(author), Some(recipient), Some(content)) = (
                    item.get("id").and_then(Value::as_str),
                    item.get("author").and_then(Value::as_str),
                    item.get("recipient").and_then(Value::as_str),
                    item.get("content").and_then(Value::as_array),
                ) else {
                    return false;
                };
                if self
                    .relations
                    .aux
                    .v2_deliveries
                    .iter()
                    .any(|d| d.thread == thread && d.id == id)
                {
                    return false;
                }
                let text = content
                    .first()
                    .filter(|c| c["type"] == "input_text")
                    .and_then(|c| c["text"].as_str());
                let Some(text) = text else {
                    return false;
                };
                let (trigger, body) =
                    if let Some(body) = text.strip_prefix("Message Type: NEW_TASK\n") {
                        (true, body)
                    } else if let Some(body) = text.strip_prefix("Message Type: MESSAGE\n") {
                        (false, body)
                    } else {
                        return false;
                    };
                let prefix = format!("Task name: {recipient}\nSender: {author}\nPayload:\n");
                let Some(payload) = body.strip_prefix(&prefix) else {
                    return false;
                };
                let message = if content.len() == 1 {
                    digest(payload)
                } else if content.len() == 2
                    && payload.is_empty()
                    && content[1]["type"] == "encrypted_content"
                {
                    let Some(encrypted) = content[1]["encrypted_content"].as_str() else {
                        return false;
                    };
                    digest(encrypted)
                } else {
                    return false;
                };
                if author.len() > 256 || recipient.len() > 256 || id.len() > 256 {
                    return false;
                }
                self.relations.aux.observation_seq =
                    self.relations.aux.observation_seq.saturating_add(1);
                if self.relations.aux.v2_deliveries.len() >= MAX_OPERATIONS {
                    self.relations.aux.v2_deliveries.pop_front();
                }
                self.relations.aux.v2_deliveries.push_back(DeliveryFact {
                    epoch: self.relations.conn_epoch,
                    seq: self.relations.aux.observation_seq,
                    thread: thread.into(),
                    turn: turn.into(),
                    id: id.into(),
                    author: author.into(),
                    recipient: recipient.into(),
                    message,
                    trigger,
                });
                self.retry_v2_pairing(thread, actions);
                true
            }
            _ => false,
        }
    }

    pub(super) fn v2_observe_start(&mut self, thread: &str, turn: &str) {
        if self
            .relations
            .aux
            .observed
            .get(thread)
            .is_some_and(|turns| turns.iter().any(|o| o.turn == turn))
        {
            return;
        }
        self.relations.aux.observation_seq = self.relations.aux.observation_seq.saturating_add(1);
        self.relations
            .aux
            .live_turn_starts
            .entry((thread.into(), turn.into()))
            .or_insert((
                self.relations.conn_epoch,
                self.relations.aux.observation_seq,
            ));
    }

    pub(super) fn v2_start(&self, op: &EvidenceOperation) -> Option<&FollowupStart> {
        self.relations
            .aux
            .followup_starts
            .get(&op.key)
            .filter(|start| {
                start.epoch == op.conn_epoch
                    && start.epoch == self.relations.conn_epoch
                    && start.run == op.run_id
                    && start.generation == op.authority_gen
            })
    }

    fn v2_sender_path(&self, op: &EvidenceOperation) -> Option<String> {
        self.thread_relation(&op.key.parent_thread_id)
            .and_then(|edge| edge.source_summary["agentPath"].as_str().map(str::to_owned))
            .or_else(|| {
                self.manage_context(&op.run_id)
                    .filter(|c| c.identity.logical_root_thread_id == op.key.parent_thread_id)
                    .map(|_| "/root".into())
            })
    }

    fn v2_input_matches(&self, op: &EvidenceOperation, child: &str, turn: &str) -> bool {
        let Some(start) = self.v2_start(op) else {
            return false;
        };
        let Some(message) = start.message else {
            return false;
        };
        let Some(recipient) = self.relations.aux.v2_receiver_paths.get(&op.key) else {
            return false;
        };
        let Some(author) = self.v2_sender_path(op) else {
            return false;
        };
        self.relations.aux.v2_deliveries.iter().any(|d| d.epoch == start.epoch && d.thread == child && d.turn == turn
            && d.seq > start.seq && d.author == author && &d.recipient == recipient && d.message == message && d.trigger == start.trigger
            && !self.relations.aux.v2_deliveries.iter().any(|previous| previous.thread == child && previous.author == author
                && previous.message == message && previous.seq <= start.seq)
            // 相同内容被发送多次时没有可区分的回执，不能凭同一条输入同时核销。
            && !self.relations.aux.followup_starts.iter().any(|(key, other)| key != &op.key && key.parent_thread_id == op.key.parent_thread_id
                && other.epoch == start.epoch && other.seq < d.seq && other.message == Some(message) && other.trigger == start.trigger))
    }

    pub(super) fn v2_candidate_turns(&self, op: &EvidenceOperation, child: &str) -> Vec<String> {
        let Some(start) = self.v2_start(op) else {
            return vec![];
        };
        self.relations
            .aux
            .observed
            .get(child)
            .into_iter()
            .flatten()
            .filter(|o| {
                if self.v2_input_matches(op, child, &o.turn) {
                    return true;
                }
                let Some(&(epoch, seq)) = self
                    .relations
                    .aux
                    .live_turn_starts
                    .get(&(child.into(), o.turn.clone()))
                else {
                    return false;
                };
                if epoch != start.epoch || seq <= start.seq {
                    return false;
                }
                // 另一个操作丢失开始记录也不能被忽略；它仍可能是这个新轮的原因。
                if self.relations.operations.values().any(|other| {
                    other.key != op.key
                        && other.kind == EvidenceKind::V2Interacted
                        && other.receivers.contains(child)
                        && !other.rejected_receivers.contains_key(child)
                        && !matches!(
                            other.outcome,
                            EvidenceOutcome::Failed | EvidenceOutcome::Revoked
                        )
                        && !other.proven_turns.contains_key(child)
                        && self.v2_start(other).is_none()
                }) {
                    return false;
                }
                if self
                    .convergences
                    .ledgers
                    .get(&op.run_id)
                    .and_then(|ledger| ledger.placeholder_operations.get(child))
                    .is_some_and(|keys| {
                        keys.iter().any(|key| {
                            key != &op.key && !self.relations.operations.contains_key(key)
                        })
                    })
                {
                    return false;
                }
                // 单个候选必须同时对所有操作唯一；未到 Interacted 的开始记录也参加歧义判断。
                !self
                    .relations
                    .aux
                    .followup_starts
                    .iter()
                    .any(|(key, other)| {
                        if key == &op.key || other.epoch != start.epoch || other.seq >= seq {
                            return false;
                        }
                        if let Some(competing) = self.relations.operations.get(key) {
                            competing.receivers.contains(child)
                                && !competing.rejected_receivers.contains_key(child)
                                && !matches!(
                                    competing.outcome,
                                    EvidenceOutcome::Failed | EvidenceOutcome::Revoked
                                )
                                && (competing
                                    .proven_turns
                                    .get(child)
                                    .is_some_and(|turns| turns.contains(&o.turn))
                                    || !competing.proven_turns.contains_key(child))
                        } else {
                            other.target.as_ref().is_none_or(|target| {
                                target == child
                                    || self
                                        .relations
                                        .aux
                                        .v2_receiver_paths
                                        .get(&op.key)
                                        .is_none_or(|path| {
                                            target == path || path.ends_with(&format!("/{target}"))
                                        })
                            })
                        }
                    })
            })
            .map(|o| o.turn.clone())
            .collect()
    }

    pub(super) fn retry_v2_pairing(&mut self, thread: &str, actions: &mut Vec<Action>) {
        let keys: Vec<_> = self
            .relations
            .operations
            .values()
            .filter(|op| {
                op.kind == EvidenceKind::V2Interacted
                    && op.receivers.contains(thread)
                    && self.evidence_valid_for_pairing(op, self.clock.mono_ms)
            })
            .map(|op| op.key.clone())
            .collect();
        for key in keys {
            let previous = self.relations.operations[&key].proven_turns.clone();
            self.pair_first_turn(&key, thread, actions);
            // 没有新增配对时不能重复 materialize：它会使正在进行的发现快照失效。
            if self.op_confirmed(&key) && self.relations.operations[&key].proven_turns != previous {
                self.materialize(&key, actions);
            }
        }
    }
}
