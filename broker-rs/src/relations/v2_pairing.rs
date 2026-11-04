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
    pub at: u64,
    pub thread: String,
    pub turn: String,
    pub id: String,
    pub author: String,
    pub recipient: String,
    pub message: [u8; 32],
    pub trigger: bool,
}

/// 消费记录按（连接代际，接收线程，发送方路径，内容摘要）分组的序号；开始记录按（代际，父线程，内容摘要）分组。
/// 只由 `v2_pairing` 维护：插入时同步登记，清理或挤出后整体重建，始终与原表一致。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct V2Index {
    pub(super) deliveries: BTreeMap<(u64, String, String, [u8; 32]), BTreeSet<u64>>,
    pub(super) delivery_ids: BTreeSet<(String, String)>,
    pub(super) starts: BTreeMap<(u64, String, [u8; 32]), BTreeSet<EvidenceKey>>,
}

impl V2Index {
    fn delivery_group(d: &DeliveryFact) -> (u64, String, String, [u8; 32]) {
        (d.epoch, d.thread.clone(), d.author.clone(), d.message)
    }

    fn add_delivery(&mut self, d: &DeliveryFact) {
        self.deliveries
            .entry(Self::delivery_group(d))
            .or_default()
            .insert(d.seq);
        self.delivery_ids.insert((d.thread.clone(), d.id.clone()));
    }

    fn add_start(&mut self, key: &EvidenceKey, start: &FollowupStart) {
        if let Some(message) = start.message {
            self.starts
                .entry((start.epoch, key.parent_thread_id.clone(), message))
                .or_default()
                .insert(key.clone());
        }
    }

    fn rebuild_deliveries(&mut self, deliveries: &VecDeque<DeliveryFact>) {
        self.deliveries.clear();
        self.delivery_ids.clear();
        for d in deliveries {
            self.add_delivery(d);
        }
    }

    fn rebuild_starts(&mut self, starts: &BTreeMap<EvidenceKey, FollowupStart>) {
        self.starts.clear();
        for (key, start) in starts {
            self.add_start(key, start);
        }
    }
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
        self.prune_v2_deliveries();
        self.note_raw_thread(thread);
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
                self.prune_followup_starts();
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
                if !self.relations.aux.followup_starts.contains_key(&key) {
                    let start = FollowupStart {
                        epoch: self.relations.conn_epoch,
                        seq: self.relations.aux.observation_seq,
                        at: now,
                        run,
                        generation,
                        target,
                        message,
                        trigger: name == "followup_task",
                    };
                    self.relations.aux.v2_index.add_start(&key, &start);
                    self.relations.aux.followup_starts.insert(key, start);
                }
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
                    .v2_index
                    .delivery_ids
                    .contains(&(thread.to_owned(), id.to_owned()))
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
                let aux = &mut self.relations.aux;
                if aux.v2_deliveries.len() >= MAX_OPERATIONS {
                    aux.v2_deliveries.pop_front();
                    aux.v2_index.rebuild_deliveries(&aux.v2_deliveries);
                }
                let fact = DeliveryFact {
                    epoch: self.relations.conn_epoch,
                    seq: aux.observation_seq,
                    at: self.clock.mono_ms,
                    thread: thread.into(),
                    turn: turn.into(),
                    id: id.into(),
                    author: author.into(),
                    recipient: recipient.into(),
                    message,
                    trigger,
                };
                aux.v2_index.add_delivery(&fact);
                aux.v2_deliveries.push_back(fact);
                self.retry_v2_pairing(thread, actions);
                true
            }
            _ => false,
        }
    }

    /// 记下线程送来过 raw；满额时先丢旧代际，仍满就不再登记（只影响诊断措辞）。
    fn note_raw_thread(&mut self, thread: &str) {
        let epoch = self.relations.conn_epoch;
        let threads = &mut self.relations.aux.v2_raw_threads;
        if threads.get(thread) == Some(&epoch) {
            return;
        }
        if threads.len() >= MAX_OPERATIONS && !threads.contains_key(thread) {
            threads.retain(|_, seen| *seen == epoch);
            if threads.len() >= MAX_OPERATIONS {
                return;
            }
        }
        threads.insert(thread.into(), epoch);
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

    /// 本操作的输入被子代理哪一轮消费；没有可核实的消费记录时为 None。
    ///
    /// 同一运行、同一代际里发给同一子代理的同内容跟进可以互换：子代理每消费一条就留下一条消费记录，
    /// 只要「已核销的操作数」不超过「消费记录数」，待定数就等于真实还在队列里的条数，承载轮也都归同一运行。
    /// 因此一条消费记录只核销一个同内容操作：已配对的操作先在自己的承载轮里各认一条，
    /// 其余记录按先后分给尚未配对的操作，且只能分给开始记录早于它的操作（发出之前不可能被消费）。
    /// 有分不出去的同内容记录，说明有本 broker 没看到开始的同内容发送，整组都不认，等新轮或收敛期限。
    /// 不同运行或代际的同内容发送不可互换，也整组不认。
    fn v2_input_turn(&self, op: &EvidenceOperation, child: &str) -> Option<String> {
        let start = self.v2_start(op)?;
        let message = start.message?;
        let recipient = self.relations.aux.v2_receiver_paths.get(&op.key)?;
        let author = self.v2_sender_path(op)?;
        let index = &self.relations.aux.v2_index;
        let mut members: Vec<(&EvidenceKey, &FollowupStart)> = Vec::new();
        for key in index
            .starts
            .get(&(start.epoch, op.key.parent_thread_id.clone(), message))
            .into_iter()
            .flatten()
        {
            let Some(other) = self.relations.aux.followup_starts.get(key) else {
                continue;
            };
            if other.trigger != start.trigger {
                continue;
            }
            let target_matches = || {
                other.target.as_ref().is_none_or(|target| {
                    target == child
                        || target == recipient
                        || recipient.ends_with(&format!("/{target}"))
                })
            };
            let same_receiver = match self.relations.operations.get(key) {
                // 确认发送失败的不会留下消费记录。
                Some(other_op) if other_op.outcome == EvidenceOutcome::Failed => false,
                Some(other_op) if !other_op.receivers.is_empty() => {
                    other_op.receivers.contains(child)
                        && self
                            .relations
                            .aux
                            .v2_receiver_paths
                            .get(key)
                            .is_none_or(|path| path == recipient)
                }
                _ => target_matches(),
            };
            if !same_receiver {
                continue;
            }
            if other.run != start.run || other.generation != start.generation {
                return None;
            }
            members.push((key, other));
        }
        members.sort_by_key(|(_, other)| other.seq);
        let now = self.clock.mono_ms;
        // BTreeSet 按序号升序，即消费记录到达的先后。
        let deliveries: Vec<&DeliveryFact> = index
            .deliveries
            .get(&(start.epoch, child.to_owned(), author, message))
            .into_iter()
            .flatten()
            .filter_map(|seq| self.v2_delivery(*seq))
            .filter(|d| {
                now < d.at.saturating_add(EVIDENCE_TTL_MS)
                    && &d.recipient == recipient
                    && d.trigger == start.trigger
            })
            .collect();
        let mut used = vec![false; deliveries.len()];
        let mut waiting = Vec::new();
        for (key, other) in &members {
            match self
                .relations
                .operations
                .get(*key)
                .and_then(|o| o.proven_turns.get(child))
            {
                Some(turns) => {
                    if let Some(i) = (0..deliveries.len())
                        .find(|&i| !used[i] && turns.contains(&deliveries[i].turn))
                    {
                        used[i] = true;
                    }
                }
                None => waiting.push((*key, *other)),
            }
        }
        let mut assigned = None;
        for (key, other) in waiting {
            if let Some(i) =
                (0..deliveries.len()).find(|&i| !used[i] && deliveries[i].seq > other.seq)
            {
                used[i] = true;
                if key == &op.key {
                    assigned = Some(deliveries[i].turn.clone());
                }
            }
        }
        if used.iter().any(|used| !used) {
            return None;
        }
        assigned
    }

    /// 已配对操作的消费记录还在时保留其开始记录，否则这条记录会变成来历不明、被同内容的后来者认领。
    fn v2_consumption_retained(&self, op: &EvidenceOperation) -> bool {
        let Some(start) = self.relations.aux.followup_starts.get(&op.key) else {
            return false;
        };
        let (Some(message), Some(author)) = (start.message, self.v2_sender_path(op)) else {
            return false;
        };
        op.proven_turns.iter().any(|(child, turns)| {
            self.relations
                .aux
                .v2_index
                .deliveries
                .get(&(start.epoch, child.clone(), author.clone(), message))
                .into_iter()
                .flatten()
                .filter_map(|seq| self.v2_delivery(*seq))
                .any(|d| turns.contains(&d.turn) && d.trigger == start.trigger)
        })
    }

    /// 消费记录按观察序号递增存放，按序号二分查找。
    fn v2_delivery(&self, seq: u64) -> Option<&DeliveryFact> {
        let deliveries = &self.relations.aux.v2_deliveries;
        deliveries
            .binary_search_by_key(&seq, |d| d.seq)
            .ok()
            .map(|i| &deliveries[i])
    }

    pub(super) fn v2_candidate_turns(&self, op: &EvidenceOperation, child: &str) -> Vec<String> {
        if !op.v2_strict {
            // 冷恢复等无 raw 的线程沿用旧版唯一候选规则；它可能误认旧活动轮，属于显式兼容策略。
            return self
                .relations
                .aux
                .observed
                .get(child)
                .into_iter()
                .flatten()
                .filter(|o| {
                    !op.baseline_turns
                        .get(child)
                        .is_some_and(|old| old.contains(&o.turn))
                })
                .map(|o| o.turn.clone())
                .collect();
        }
        let Some(start) = self.v2_start(op) else {
            return vec![];
        };
        let input_turn = self.v2_input_turn(op, child);
        self.relations
            .aux
            .observed
            .get(child)
            .into_iter()
            .flatten()
            .filter(|o| {
                if input_turn.as_deref() == Some(o.turn.as_str()) {
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
                        && (!other.proven_turns.contains_key(child)
                            || other
                                .proven_turns
                                .get(child)
                                .is_some_and(|turns| turns.contains(&o.turn)))
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
                        // 只入队的消息不会开新轮，不是这一轮的候选原因。
                        if key == &op.key
                            || !other.trigger
                            || other.epoch != start.epoch
                            || other.seq >= seq
                        {
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

    /// 跟进已进入子代理队列，何时读到取决于它手头的工作，不能按证据期限判失败；
    /// 一直读不到时由收敛期限兜底。配对或拒绝后 outcome 不再是 Pending，连接或授权换代后也不再成立。
    pub(crate) fn v2_awaits_pairing(&self, op: &EvidenceOperation) -> bool {
        op.kind == EvidenceKind::V2Interacted
            && !op.v2_queue_only
            && op.outcome == EvidenceOutcome::Pending
            && !op.receivers.is_empty()
            && self.op_confirmed(&op.key)
            && self.operation_generation_current(op)
    }

    /// 开始记录默认只留证据期限；操作还在等配对或归属未定案时留到有结果，否则配对依据就丢了；
    /// 已配对操作的消费记录还在时一并保留，供同内容一一核销。
    pub(super) fn prune_followup_starts(&mut self) {
        let now = self.clock.mono_ms;
        let epoch = self.relations.conn_epoch;
        let keep: BTreeSet<EvidenceKey> = self
            .relations
            .operations
            .values()
            .filter(|op| {
                self.v2_awaits_pairing(op)
                    || self.relations.aux.parent_waits.contains_key(&op.key)
                    || self.v2_consumption_retained(op)
            })
            .map(|op| op.key.clone())
            .collect();
        let aux = &mut self.relations.aux;
        let before = aux.followup_starts.len();
        aux.followup_starts.retain(|key, start| {
            start.epoch == epoch
                && (now < start.at.saturating_add(EVIDENCE_TTL_MS) || keep.contains(key))
        });
        if aux.followup_starts.len() != before {
            aux.v2_index.rebuild_starts(&aux.followup_starts);
        }
    }

    pub(super) fn prune_v2_deliveries(&mut self) {
        let now = self.clock.mono_ms;
        let epoch = self.relations.conn_epoch;
        let aux = &mut self.relations.aux;
        let before = aux.v2_deliveries.len();
        aux.v2_deliveries
            .retain(|d| d.epoch == epoch && now < d.at.saturating_add(EVIDENCE_TTL_MS));
        if aux.v2_deliveries.len() != before {
            aux.v2_index.rebuild_deliveries(&aux.v2_deliveries);
        }
    }

    /// 测试用：索引必须与原表重建的结果完全一致。
    #[cfg(test)]
    pub(crate) fn v2_index_consistent(&self) -> bool {
        let aux = &self.relations.aux;
        let mut rebuilt = V2Index::default();
        rebuilt.rebuild_deliveries(&aux.v2_deliveries);
        rebuilt.rebuild_starts(&aux.followup_starts);
        rebuilt == aux.v2_index
    }
}
