//! v2 因果证据：新轮开始顺序，或接收轮里实际记录的同一输入；不保存正文。
use super::*;
use sha2::{Digest, Sha256};

/// 线程在本连接代际里是否送过 raw。只有确认没有（`NotSeen`）才能改读会话文件；
/// 登记表满了、没登记上的线程是 `Unknown`，照原来的宽松走，一次都不读文件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawCapability {
    Seen,
    NotSeen,
    Unknown,
}

/// 开始记录与消费记录的来源。raw 来自本连接的实时事件，观察序号即因果先后；
/// 会话文件的观察序号只反映读取时刻，跨文件不可比，先后改看行时间戳（`wall_ms`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceSource {
    Raw,
    RolloutFile,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FollowupStart {
    pub epoch: u64,
    pub seq: u64,
    pub at: u64,
    pub source: EvidenceSource,
    /// raw：broker 收到时的墙钟毫秒；会话文件：该行的写入时间（同一台机器上的 Codex 时钟）。
    pub wall_ms: u64,
    /// 空串表示登记时发起方这一轮的归属还没定（等绑定）；绑定后补上运行与代际。
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
    pub source: EvidenceSource,
    pub wall_ms: u64,
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

/// 协作工具调用里与配对有关的字段；raw 与会话文件共用同一套解析。
pub(super) struct ParsedCall {
    pub call_id: String,
    pub trigger: bool,
    pub target: Option<String>,
    pub message: Option<[u8; 32]>,
}

pub(super) fn parse_call(item: &Value) -> Option<ParsedCall> {
    if item.get("type").and_then(Value::as_str) != Some("function_call") {
        return None;
    }
    let name = item.get("name").and_then(Value::as_str).unwrap_or("");
    let name = name.rsplit('.').next().unwrap_or("");
    if !matches!(name, "followup_task" | "send_message") {
        return None;
    }
    let call_id = item.get("call_id").and_then(Value::as_str)?;
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
    Some(ParsedCall {
        call_id: call_id.into(),
        trigger: name == "followup_task",
        target,
        message,
    })
}

/// 子代理收到的一条消息：只留身份、类型与内容摘要。
pub(super) struct ParsedDelivery {
    pub raw: Value,
    pub id: String,
    pub author: String,
    pub recipient: String,
    pub trigger: bool,
    pub message: [u8; 32],
}

pub(super) fn parse_delivery(item: &Value) -> Option<ParsedDelivery> {
    if item.get("type").and_then(Value::as_str) != Some("agent_message") {
        return None;
    }
    let (Some(id), Some(author), Some(recipient), Some(content)) = (
        item.get("id").and_then(Value::as_str),
        item.get("author").and_then(Value::as_str),
        item.get("recipient").and_then(Value::as_str),
        item.get("content").and_then(Value::as_array),
    ) else {
        return None;
    };
    let text = content
        .first()
        .filter(|c| c["type"] == "input_text")
        .and_then(|c| c["text"].as_str())?;
    let (trigger, body) = match text.strip_prefix("Message Type: NEW_TASK\n") {
        Some(body) => (true, body),
        None => (false, text.strip_prefix("Message Type: MESSAGE\n")?),
    };
    let prefix = format!("Task name: {recipient}\nSender: {author}\nPayload:\n");
    let payload = body.strip_prefix(&prefix)?;
    let message = if content.len() == 1 {
        digest(payload)
    } else if content.len() == 2 && payload.is_empty() && content[1]["type"] == "encrypted_content"
    {
        digest(content[1]["encrypted_content"].as_str()?)
    } else {
        return None;
    };
    if author.len() > 256 || recipient.len() > 256 || id.len() > 256 {
        return None;
    }
    Some(ParsedDelivery {
        raw: item.clone(),
        id: id.into(),
        author: author.into(),
        recipient: recipient.into(),
        trigger,
        message,
    })
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
        self.coverage_raw_witness(thread, turn);
        match item.get("type").and_then(Value::as_str) {
            Some("function_call") => {
                let Some(call) = parse_call(item) else {
                    return false;
                };
                let key = EvidenceKey {
                    parent_thread_id: thread.into(),
                    parent_turn_id: turn.into(),
                    item_id: call.call_id.clone(),
                };
                // 开始记录晚于 Interacted 时不补造基线；已知操作的身份不可回写。
                if self.relations.operations.contains_key(&key) {
                    return false;
                }
                let (run, generation) = match self.parent_run(thread, turn) {
                    facts::ParentRun::Own { run, .. } => {
                        if !self.ensure_context(&run) {
                            return false;
                        }
                        let Some(context) = self.manage_context(&run) else {
                            return false;
                        };
                        let generation = context.gen;
                        (run, generation)
                    }
                    // 发起方这一轮的归属还没定（子代理刚被跟进就转手跟进孙代理）：调用证据与归属分开保存，
                    // 先存下调用、归属留空；这一轮绑定后由 `adopt_pending_starts` 补上运行与代际。
                    // 血缘已证明是外部的线程不存。
                    facts::ParentRun::AwaitBinding if self.lineage_owner(thread) != Some(false) => {
                        (String::new(), 0)
                    }
                    // 共享服务器上这一轮还没定案是不是自己的：raw 不进 settling 缓冲，错过就没有了。
                    // 代际取值与同时到达的 Interacted 相同；定案是自己的由 `adopt_settled_starts` 对齐，
                    // 不是自己的，操作转未证明，这条记录没有操作能配上，随证据期限清理。
                    facts::ParentRun::Settling { run, .. } => {
                        let generation = self.relations.contexts.get(&run).map_or(0, |c| c.gen);
                        (run, generation)
                    }
                    _ => return false,
                };
                let wall_ms = self.clock.now_ms;
                self.v2_record_start(key, run, generation, call, EvidenceSource::Raw, wall_ms);
                false
            }
            Some("agent_message") => {
                let Some(delivery) = parse_delivery(item) else {
                    return false;
                };
                let wall_ms = self.clock.now_ms;
                self.v2_record_delivery(
                    thread,
                    turn,
                    delivery,
                    EvidenceSource::Raw,
                    wall_ms,
                    actions,
                )
            }
            _ => false,
        }
    }

    /// 发起方的一轮绑定到运行：归属未定时存下的开始记录（raw 或会话文件）补上运行与当前代际，
    /// 之后与发起方本来就有归属时登记的记录完全相同。必须先于接回等它的操作。
    pub(super) fn adopt_pending_starts(&mut self, thread: &str, turn: &str, run: &str) {
        let Some(generation) = self.relations.contexts.get(run).map(|c| c.gen) else {
            return;
        };
        let epoch = self.relations.conn_epoch;
        let from = EvidenceKey {
            parent_thread_id: thread.into(),
            parent_turn_id: turn.into(),
            item_id: String::new(),
        };
        for (_, start) in self
            .relations
            .aux
            .followup_starts
            .range_mut(from..)
            .take_while(|(k, _)| k.parent_thread_id == thread && k.parent_turn_id == turn)
        {
            if start.run.is_empty() && start.epoch == epoch {
                start.run = run.into();
                start.generation = generation;
            }
        }
    }

    /// settling 定案是自己的：这时才建运行上下文，定案前存下的开始记录对齐到它的代际。
    /// 只动上下文建立前的那一代，已经因交接或撤权换代的记录不受影响。
    pub(super) fn adopt_settled_starts(&mut self, thread: &str, run: &str) {
        let before = self.relations.contexts.get(run).map_or(0, |c| c.gen);
        if !self.ensure_context(run) {
            return;
        }
        let generation = self.relations.contexts.get(run).map_or(0, |c| c.gen);
        if generation == before {
            return;
        }
        let epoch = self.relations.conn_epoch;
        let from = EvidenceKey {
            parent_thread_id: thread.into(),
            parent_turn_id: String::new(),
            item_id: String::new(),
        };
        for (_, start) in self
            .relations
            .aux
            .followup_starts
            .range_mut(from..)
            .take_while(|(k, _)| k.parent_thread_id == thread)
        {
            if start.run == run && start.generation == before && start.epoch == epoch {
                start.generation = generation;
            }
        }
    }

    /// 定案不是自己的，或认领没定案就被释放：这段时间按这个运行存下的开始记录和它们对应的消费记录一起清掉。
    /// 只清一半都不行：只留开始记录，同内容的新跟进会被它挡住；只留消费记录，新跟进会认领它。
    /// 操作还有效的不动；别的子代理收到的同内容消息不动。
    /// 消费记录在清理之后才到、又碰上同内容的新跟进时仍可能被认领（已知局限，只在不加密时才有同内容）。
    pub(crate) fn v2_forget_unowned(&mut self, thread: &str, run: &str) {
        let stale: Vec<(EvidenceKey, FollowupStart, BTreeSet<String>)> = self
            .relations
            .aux
            .followup_starts
            .iter()
            .filter(|(key, start)| key.parent_thread_id == thread && start.run == run)
            .filter_map(|(key, start)| {
                let op = self.relations.operations.get(key);
                op.is_none_or(|op| op.outcome == EvidenceOutcome::Unproven)
                    .then(|| {
                        let receivers = op.map(|op| op.receivers.clone()).unwrap_or_default();
                        (key.clone(), start.clone(), receivers)
                    })
            })
            .collect();
        if stale.is_empty() {
            return;
        }
        // 接收方已知就只认线程。未知时（还没等到操作）短名可能和别的运行重名，还要血缘属于发起线程。
        let forgotten: BTreeSet<u64> = self
            .relations
            .aux
            .v2_deliveries
            .iter()
            .filter(|d| {
                stale.iter().any(|(_, start, receivers)| {
                    start.message == Some(d.message)
                        && start.trigger == d.trigger
                        && if receivers.is_empty() {
                            start.target.as_ref().is_some_and(|target| {
                                target == &d.thread
                                    || target == &d.recipient
                                    || d.recipient.ends_with(&format!("/{target}"))
                            }) && self
                                .thread_relation(&d.thread)
                                .is_some_and(|edge| edge.parent_thread_id == thread)
                        } else {
                            receivers.contains(&d.thread)
                        }
                })
            })
            .map(|d| d.seq)
            .collect();
        let aux = &mut self.relations.aux;
        for (key, _, _) in &stale {
            aux.followup_starts.remove(key);
        }
        aux.v2_deliveries.retain(|d| !forgotten.contains(&d.seq));
        aux.v2_index.rebuild_starts(&aux.followup_starts);
        aux.v2_index.rebuild_deliveries(&aux.v2_deliveries);
    }

    /// 登记开始记录；满额或已有同键记录时不登记。返回是否新登记。
    pub(super) fn v2_record_start(
        &mut self,
        key: EvidenceKey,
        run: String,
        generation: u64,
        call: ParsedCall,
        source: EvidenceSource,
        wall_ms: u64,
    ) -> bool {
        let now = self.clock.mono_ms;
        self.prune_followup_starts();
        if self.relations.aux.followup_starts.len() >= MAX_OPERATIONS {
            return false;
        }
        if self.relations.aux.followup_starts.contains_key(&key) {
            return false;
        }
        let start = FollowupStart {
            epoch: self.relations.conn_epoch,
            seq: self.relations.aux.observation_seq,
            at: now,
            source,
            wall_ms,
            run,
            generation,
            target: call.target,
            message: call.message,
            trigger: call.trigger,
        };
        self.relations.aux.v2_index.add_start(&key, &start);
        self.relations.aux.followup_starts.insert(key, start);
        true
    }

    /// 登记一条消费记录并重试配对；同一线程同一条目只登记一次。
    pub(super) fn v2_record_delivery(
        &mut self,
        thread: &str,
        turn: &str,
        delivery: ParsedDelivery,
        source: EvidenceSource,
        wall_ms: u64,
        actions: &mut Vec<Action>,
    ) -> bool {
        // 覆盖消费必须先于配对查重；文件来源永远不能授予完整观察资格。
        if source == EvidenceSource::RolloutFile {
            self.coverage_veto(Some(thread), Some(turn), "文件来源消费", true);
        } else {
            let collaboration = crate::runs::event_log::collaboration_delivery(
                &delivery.author,
                &delivery.recipient,
            );
            if self.coverage_consume(thread, turn, &delivery.id, &delivery.raw, collaboration) {
                self.observation_gap(Some(thread), "完成后新增或冲突消费");
            }
        }
        if self
            .relations
            .aux
            .v2_index
            .delivery_ids
            .contains(&(thread.to_owned(), delivery.id.clone()))
        {
            return false;
        }
        self.relations.aux.observation_seq = self.relations.aux.observation_seq.saturating_add(1);
        let aux = &mut self.relations.aux;
        if aux.v2_deliveries.len() >= MAX_OPERATIONS {
            aux.v2_deliveries.pop_front();
            aux.v2_index.rebuild_deliveries(&aux.v2_deliveries);
        }
        let fact = DeliveryFact {
            epoch: self.relations.conn_epoch,
            seq: aux.observation_seq,
            at: self.clock.mono_ms,
            source,
            wall_ms,
            thread: thread.into(),
            turn: turn.into(),
            id: delivery.id,
            author: delivery.author,
            recipient: delivery.recipient,
            message: delivery.message,
            trigger: delivery.trigger,
        };
        aux.v2_index.add_delivery(&fact);
        aux.v2_deliveries.push_back(fact);
        self.retry_v2_pairing(thread, actions);
        true
    }

    /// 记下线程送来过 raw；满额时先丢旧代际，仍满就记下「本代际登记不全」。
    /// 登记决定会不会读会话文件：登记不上的线程只能算能力未知，不能算没有 raw（见 [`RawCapability`]）。
    fn note_raw_thread(&mut self, thread: &str) {
        let epoch = self.relations.conn_epoch;
        let aux = &mut self.relations.aux;
        let threads = &mut aux.v2_raw_threads;
        if threads.get(thread) == Some(&epoch) {
            return;
        }
        if threads.len() >= MAX_OPERATIONS && !threads.contains_key(thread) {
            threads.retain(|_, seen| *seen == epoch);
            if threads.len() >= MAX_OPERATIONS {
                aux.v2_raw_overflow = Some(epoch);
                return;
            }
        }
        threads.insert(thread.into(), epoch);
    }

    /// 线程在本连接代际里有没有送过 raw。
    pub(crate) fn raw_capability(&self, thread: &str) -> RawCapability {
        let epoch = self.relations.conn_epoch;
        if self.relations.aux.v2_raw_threads.get(thread) == Some(&epoch) {
            RawCapability::Seen
        } else if self.relations.aux.v2_raw_overflow == Some(epoch) {
            RawCapability::Unknown
        } else {
            RawCapability::NotSeen
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
        self.coverage_real_start(thread, turn);
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

    /// 记下子代理的规范代理路径（上游 subAgentActivity 条目的 agentPath）。
    /// 不需要另外重试配对：非根发起方要先被这样的条目（派生或跟进）接纳、它的轮才会绑定，
    /// 它发出的跟进在绑定后才接纳接收方、开始配对，那时路径已经在了。
    ///
    /// 表满时只挤出没人在用的线程；都在用时不登记新线程，它发出的跟进由配对明确退回宽松。
    pub(super) fn v2_learn_agent_path(&mut self, thread: &str, path: &str) {
        if path.is_empty() || path.len() > 256 {
            return;
        }
        let known = self.relations.aux.v2_agent_paths.get(thread);
        if known.map(String::as_str) == Some(path) {
            return;
        }
        if known.is_none() && self.relations.aux.v2_agent_paths.len() >= MAX_OBSERVED_THREADS {
            let Some(victim) = self.v2_unused_agent_path() else {
                return;
            };
            self.relations.aux.v2_agent_paths.remove(&victim);
        }
        self.relations
            .aux
            .v2_agent_paths
            .insert(thread.into(), path.into());
    }

    /// 没人在用的线程：运行里没有已绑定的轮，也不是未决跟进的发起方或接收方。
    fn v2_unused_agent_path(&self) -> Option<String> {
        let bound = self
            .relations
            .bindings
            .values()
            .filter(|b| b.status == BindingStatus::Bound)
            .map(|b| b.thread_id.as_str());
        let pending = self
            .relations
            .operations
            .values()
            .filter(|op| {
                op.kind == EvidenceKind::V2Interacted && op.outcome == EvidenceOutcome::Pending
            })
            .flat_map(|op| {
                std::iter::once(op.key.parent_thread_id.as_str())
                    .chain(op.receivers.iter().map(String::as_str))
            });
        let in_use: BTreeSet<&str> = bound.chain(pending).collect();
        self.relations
            .aux
            .v2_agent_paths
            .keys()
            .find(|thread| !in_use.contains(thread.as_str()))
            .cloned()
    }

    /// 发起方的规范代理路径：血缘边的来源摘要，其次是活动条目里学到的路径；运行的逻辑根是 `/root`。
    pub(super) fn v2_sender_path(&self, op: &EvidenceOperation) -> Option<String> {
        self.thread_relation(&op.key.parent_thread_id)
            .and_then(|edge| edge.source_summary["agentPath"].as_str().map(str::to_owned))
            .or_else(|| {
                self.relations
                    .aux
                    .v2_agent_paths
                    .get(&op.key.parent_thread_id)
                    .cloned()
            })
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
        // 会话文件的观察序号只反映读取时刻：有文件来源的成员时，先后与配对都改看行时间戳。
        let by_file = members
            .iter()
            .any(|(_, other)| other.source == EvidenceSource::RolloutFile);
        if by_file {
            members.sort_by_key(|(_, other)| (other.wall_ms, other.seq));
        } else {
            members.sort_by_key(|(_, other)| other.seq);
        }
        // 文件末尾窗口里可能有本组成员都还没发出时的旧记录（重启前的同内容消息），它们不可能是本组的。
        let floor = by_file
            .then(|| members.iter().map(|(_, other)| other.wall_ms).min())
            .flatten();
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
                    && (d.source == EvidenceSource::Raw || floor.is_none_or(|f| d.wall_ms >= f))
            })
            .collect();
        // 父子事件由不同监听器送来，消费记录可能先于开始记录被看到，不能一律按先后排除。
        // 只有这个父线程对该子代理还有不知正文的（宽松）跟进时，早到的记录才可能是别人的。
        let unknown_sender = self.relations.operations.values().any(|o| {
            o.kind == EvidenceKind::V2Interacted
                && !o.v2_strict
                && o.conn_epoch == start.epoch
                && o.key.parent_thread_id == op.key.parent_thread_id
                && o.receivers.contains(child)
                && !o.rejected_receivers.contains_key(child)
                && o.outcome != EvidenceOutcome::Failed
        });
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
            if let Some(i) = (0..deliveries.len()).find(|&i| {
                !used[i] && Self::v2_delivered_after(deliveries[i], other, unknown_sender)
            }) {
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

    /// 这条消费记录能否归给这个开始记录。都来自 raw 时观察序号不代表真实先后，
    /// 只在可能有别的发送者（`unknown_sender`）时才按序号排除早到的记录；
    /// 有一方来自会话文件时按时间戳（同一 Codex 进程写入，毫秒精度，同一毫秒算作之后）。
    fn v2_delivered_after(d: &DeliveryFact, start: &FollowupStart, unknown_sender: bool) -> bool {
        if d.source == EvidenceSource::Raw && start.source == EvidenceSource::Raw {
            !unknown_sender || d.seq > start.seq
        } else {
            d.wall_ms >= start.wall_ms
        }
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
                .filter(|o| {
                    // 当时会配哪一轮还原不了时，只认此后真实开始的轮；读到的历史轮没有开始记录。
                    op.lenient_floor.is_none_or(|floor| {
                        self.relations
                            .aux
                            .live_turn_starts
                            .get(&(child.to_owned(), o.turn.clone()))
                            .is_some_and(|(epoch, seq)| {
                                *epoch == self.relations.conn_epoch && *seq > floor
                            })
                    })
                })
                .map(|o| o.turn.clone())
                .collect();
        }
        let Some(start) = self.v2_start(op) else {
            return vec![];
        };
        let input_turn = self.v2_input_turn(op, child);
        if start.source == EvidenceSource::RolloutFile {
            // 文件证据的观察序号由读取时刻决定，可能晚于子线程的 turn/started：
            // 不走「开始于跟进之后的新轮」这条顺序证据，只认消费记录。
            return self
                .relations
                .aux
                .observed
                .get(child)
                .into_iter()
                .flatten()
                .filter(|o| input_turn.as_deref() == Some(o.turn.as_str()))
                .map(|o| o.turn.clone())
                .collect();
        }
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
                        // 文件来源的开始记录序号不可比，保守地一律当作可能的竞争原因。
                        if key == &op.key
                            || !other.trigger
                            || other.epoch != start.epoch
                            || (other.source == EvidenceSource::Raw && other.seq >= seq)
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
