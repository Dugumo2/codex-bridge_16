//! 连续观察凭据：真实开始只使用先验 raw，任何断档在本进程内不可恢复。
use super::*;
use crate::runs::event_log::StartProof;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawWitness {
    epoch: u64,
    turn: String,
    start_seq: u64,
    raw_seq: u64,
    generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RawObservation {
    Clean {
        generation: u64,
        since_seq: u64,
        witness: Option<RawWitness>,
    },
    Blocked {
        generation: u64,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawStart {
    epoch: u64,
    generation: u64,
    seq: u64,
    ended: bool,
}

fn identity(value: Option<&Value>) -> Option<&str> {
    value
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty() && s.len() <= 256)
}

impl Machine {
    pub(crate) fn coverage_check_epoch(&mut self) {
        let epoch = self.relations.conn_epoch;
        let previous = self.relations.aux.coverage_epoch.replace(epoch);
        if previous.is_some_and(|previous| previous != epoch) {
            self.observation_gap(None, "上游连接代际变化");
        }
    }

    pub(crate) fn coverage_next_seq(&mut self) -> Option<u64> {
        self.coverage_check_epoch();
        let Some(next) = self.relations.aux.coverage_seq.checked_add(1) else {
            self.observation_gap(None, "覆盖顺序耗尽");
            return None;
        };
        self.relations.aux.coverage_seq = next;
        Some(next)
    }

    pub(crate) fn coverage_read_observation_gen(&self, thread: &str) -> u64 {
        match self.relations.aux.raw_observations.get(thread) {
            Some(
                RawObservation::Clean { generation, .. }
                | RawObservation::Blocked { generation, .. },
            ) => *generation,
            None => 0,
        }
    }

    pub(crate) fn coverage_observation_gen(&self, thread: &str) -> Option<u64> {
        if self.relations.aux.raw_globally_blocked {
            return None;
        }
        match self.relations.aux.raw_observations.get(thread) {
            Some(RawObservation::Clean { generation, .. }) => Some(*generation),
            _ => None,
        }
    }

    /// 无法解释的记录可能影响尚未裁决的完成轮，故包含封存证明。
    pub(crate) fn observation_gap(&mut self, thread: Option<&str>, reason: &str) {
        self.coverage_veto(thread, None, reason, true);
        match thread {
            Some(thread) => self.block_raw_observation(thread, reason),
            None => {
                self.relations.aux.raw_globally_blocked = true;
                let threads: Vec<_> = self
                    .relations
                    .aux
                    .raw_observations
                    .keys()
                    .cloned()
                    .collect();
                for thread in threads {
                    self.block_raw_observation(&thread, reason);
                }
            }
        }
    }

    /// 正常结束后退订只否决未来资格，不反推已封存轮有缺口。
    pub(crate) fn observation_continuity_lost(&mut self, thread: &str, reason: &str) {
        self.coverage_veto(Some(thread), None, reason, false);
        self.block_raw_observation(thread, reason);
    }

    fn block_raw_observation(&mut self, thread: &str, reason: &str) {
        let observations = &mut self.relations.aux.raw_observations;
        if matches!(
            observations.get(thread),
            Some(RawObservation::Blocked { .. })
        ) {
            return;
        }
        if observations.len() >= MAX_OPERATIONS && !observations.contains_key(thread) {
            return;
        }
        let generation = match observations.get(thread) {
            Some(RawObservation::Clean { generation, .. }) => generation.checked_add(1),
            _ => Some(1),
        };
        let Some(generation) = generation else {
            self.relations.aux.raw_globally_blocked = true;
            return;
        };
        observations.insert(
            thread.into(),
            RawObservation::Blocked {
                generation,
                reason: reason.into(),
            },
        );
    }

    fn coverage_attached(&self, thread: &str) -> bool {
        !self.closing
            && self
                .relations
                .attachments
                .get(thread)
                .is_some_and(|a| a.valid && a.conn_epoch == self.relations.conn_epoch)
            && self
                .relations
                .aux
                .released
                .get(thread)
                .is_none_or(|r| r.result == release::SubscriptionResult::Attached)
    }

    pub(crate) fn coverage_open_valid(&self, thread: &str, turn: &str, start: &StartProof) -> bool {
        start.conn_epoch == self.relations.conn_epoch
            && self.coverage_observation_gen(thread) == Some(start.observation_gen)
            && self.coverage_attached(thread)
            && self
                .relations
                .aux
                .live_turn_starts
                .contains_key(&(thread.into(), turn.into()))
            && self
                .relations
                .aux
                .coverage_starts
                .get(&(thread.into(), turn.into()))
                .is_some_and(|anchor| {
                    !anchor.ended
                        && anchor.epoch == start.conn_epoch
                        && anchor.generation == start.observation_gen
                        && anchor.seq == start.start_seq
                })
    }

    pub(super) fn coverage_real_start(&mut self, thread: &str, turn: &str) {
        if thread.is_empty() || thread.len() > 256 || turn.is_empty() || turn.len() > 256 {
            return;
        }
        if self
            .relations
            .aux
            .coverage_starts
            .contains_key(&(thread.into(), turn.into()))
        {
            return;
        }
        if self.coverage_turn_seen(thread, turn) {
            return;
        }
        let Some(seq) = self.coverage_next_seq() else {
            return;
        };
        if !self.relations.aux.raw_globally_blocked
            && self.relations.aux.raw_observations.len() < MAX_OPERATIONS
        {
            self.relations
                .aux
                .raw_observations
                .entry(thread.into())
                .or_insert(RawObservation::Clean {
                    generation: 0,
                    since_seq: seq,
                    witness: None,
                });
        }
        // 开始锚点也有独立上限，挤出前撤销对应线程连续性。
        if self.relations.aux.coverage_starts.len() >= MAX_OPERATIONS {
            if let Some((oldest, ended)) = self
                .relations
                .aux
                .coverage_starts
                .iter()
                .min_by_key(|(_, s)| (!s.ended, s.seq))
                .map(|(key, start)| (key.clone(), start.ended))
            {
                if !ended {
                    self.observation_continuity_lost(&oldest.0, "真实开始锚点淘汰");
                }
                self.relations.aux.coverage_starts.remove(&oldest);
            }
        }
        let generation = self.coverage_read_observation_gen(thread);
        let witness = match self.relations.aux.raw_observations.get(thread) {
            Some(RawObservation::Clean {
                witness: Some(w), ..
            }) if w.raw_seq < seq
                && w.generation == generation
                && w.epoch == self.relations.conn_epoch =>
            {
                Some(w.raw_seq)
            }
            _ => None,
        };
        let proof = witness
            .filter(|_| self.coverage_attached(thread) && !self.relations.aux.raw_globally_blocked)
            .map(|raw_seq_at_start| StartProof {
                conn_epoch: self.relations.conn_epoch,
                observation_gen: generation,
                start_seq: seq,
                raw_seq_at_start,
            });
        self.relations.aux.coverage_starts.insert(
            (thread.into(), turn.into()),
            RawStart {
                epoch: self.relations.conn_epoch,
                generation,
                seq,
                ended: false,
            },
        );
        self.coverage_start(thread, turn, proof);
    }

    pub(super) fn coverage_raw_witness(&mut self, thread: &str, turn: &str) {
        let Some(seq) = self.coverage_next_seq() else {
            return;
        };
        let Some(generation) = self.coverage_observation_gen(thread) else {
            return;
        };
        if !self.coverage_attached(thread) {
            return;
        }
        let Some(start) = self
            .relations
            .aux
            .coverage_starts
            .get(&(thread.into(), turn.into()))
        else {
            return;
        };
        if start.ended
            || start.epoch != self.relations.conn_epoch
            || start.generation != generation
            || !self
                .relations
                .aux
                .live_turn_starts
                .contains_key(&(thread.into(), turn.into()))
        {
            return;
        }
        let witness = RawWitness {
            epoch: self.relations.conn_epoch,
            turn: turn.into(),
            start_seq: start.seq,
            raw_seq: seq,
            generation,
        };
        if let Some(RawObservation::Clean { witness: slot, .. }) =
            self.relations.aux.raw_observations.get_mut(thread)
        {
            *slot = Some(witness);
        }
    }

    /// 必须早于自动附着和任何 completed 引起的退订、嵌套扫描。
    pub(crate) fn coverage_preprocess_notification(&mut self, event: &HeldNotification) {
        self.coverage_check_epoch();
        let method = event.method.as_str();
        if irrelevant_notification(method) {
            return;
        }
        let params = event.params.as_ref();
        let thread = identity(params.and_then(|p| p.get("threadId"))).or_else(|| {
            identity(
                params
                    .and_then(|p| p.get("thread"))
                    .and_then(|t| t.get("id")),
            )
        });
        let turn = facts::notification_turn(params).filter(|t| t.len() <= 256);
        match method {
            "turn/started" | "turn/completed" => {
                let (Some(thread), Some(turn)) = (thread, turn) else {
                    self.observation_gap(thread, "轮边界缺少有效身份");
                    return;
                };
                if method == "turn/started"
                    && params
                        .and_then(|p| p.get("turn"))
                        .and_then(|t| t.get("status"))
                        .and_then(Value::as_str)
                        != Some("inProgress")
                {
                    self.observation_gap(Some(thread), "无法解释的开始状态");
                    return;
                }
                if method == "turn/completed" {
                    let Some(seq) = self.coverage_next_seq() else {
                        return;
                    };
                    let valid = self
                        .coverage_open_proof(thread, turn)
                        .is_some_and(|proof| self.coverage_open_valid(thread, turn, proof));
                    let completed = params.and_then(|p| p.get("turn")).unwrap_or(&Value::Null);
                    if !matches!(
                        completed.get("status").and_then(Value::as_str),
                        Some("completed" | "failed" | "interrupted")
                    ) {
                        self.observation_gap(Some(thread), "无法解释的完成终态");
                    }
                    if self.coverage_complete(thread, turn, completed, seq, valid) {
                        self.observation_gap(Some(thread), "矛盾或畸形完成");
                    }
                    if let Some(start) = self
                        .relations
                        .aux
                        .coverage_starts
                        .get_mut(&(thread.into(), turn.into()))
                    {
                        start.ended = true;
                    }
                }
            }
            // 这是响应级用量/展示通知，不是轮完成或消费记录。
            "rawResponse/completed" => {}
            "rawResponseItem/completed" => {
                let item = params.and_then(|p| p.get("item"));
                let consumption = item.is_some_and(|i| i["type"] == "agent_message");
                if !item.is_some_and(explained_raw_item)
                    || (consumption && (thread.is_none() || turn.is_none()))
                {
                    self.observation_gap(thread, "raw消费或记录类型无法解释");
                }
            }
            "item/started" | "item/completed" => {
                let item = params.and_then(|p| p.get("item"));
                if !item.is_some_and(explained_thread_item) {
                    self.observation_gap(thread, "条目可能包含未观察的输入");
                }
            }
            "thread/started" => {
                if let Some(thread) = thread {
                    if self.relations.aux.raw_observations.contains_key(thread) {
                        self.observation_continuity_lost(thread, "线程重新开始");
                    }
                } else {
                    self.observation_gap(None, "线程开始缺少身份");
                }
            }
            "thread/status/changed" => {
                let status = params
                    .and_then(|p| p.get("status"))
                    .and_then(|s| s.get("type"))
                    .and_then(Value::as_str);
                match (thread, status) {
                    (Some(thread), Some("notLoaded")) => {
                        self.observation_continuity_lost(thread, "线程卸载")
                    }
                    (Some(_), Some("active" | "idle" | "systemError")) => {}
                    _ => self.observation_gap(thread, "线程状态无法解释"),
                }
            }
            "thread/closed"
            | "thread/deleted"
            | "thread/archived"
            | "thread/unarchived"
            | "thread/reverted"
            | "thread/settings/updated"
            | "thread/environment/connected"
            | "thread/environment/disconnected" => {
                if let Some(thread) = thread {
                    self.observation_continuity_lost(thread, "线程生命周期变化");
                } else {
                    self.observation_gap(None, "生命周期缺少身份");
                }
            }
            _ => self.observation_gap(thread, "未知通知或不受支持的实时模式"),
        }
    }
}

/// 只判断是否可能漏掉消费；已确认的输出类型不检查正文、展示字段或内部枚举。
fn explained_raw_item(item: &Value) -> bool {
    match item.get("type").and_then(Value::as_str) {
        Some("agent_message") => v2_pairing::parse_delivery(item).is_some(),
        // 输入仍是非协作反证；输出内容的格式不影响代理间消息的覆盖。
        Some("message") => item.get("role").and_then(Value::as_str) == Some("assistant"),
        Some(
            "reasoning"
            | "function_call"
            | "function_call_output"
            | "custom_tool_call"
            | "custom_tool_call_output"
            | "tool_search_call"
            | "tool_search_output"
            | "web_search_call"
            | "local_shell_call"
            | "image_generation_call"
            | "additional_tools"
            | "configuration_update"
            | "compaction"
            | "compaction_summary"
            | "context_compaction"
            | "compaction_trigger",
        ) => true,
        // 未知记录类型可能是新的消费记录，不能按已知输出放行。
        _ => false,
    }
}

fn explained_thread_item(item: &Value) -> bool {
    matches!(
        item.get("type").and_then(Value::as_str),
        Some(
            "agentMessage"
                | "plan"
                | "reasoning"
                | "functionCallOutput"
                | "commandExecution"
                | "fileChange"
                | "mcpToolCall"
                | "dynamicToolCall"
                | "collabAgentToolCall"
                | "subAgentActivity"
                | "imageView"
                | "enteredReviewMode"
                | "exitedReviewMode"
                | "contextCompaction"
                | "webSearch"
                | "imageGeneration"
                | "sleep"
        )
    )
}

fn irrelevant_notification(method: &str) -> bool {
    matches!(
        method,
        "account/gatewayOAuth/changed"
            | "account/login/completed"
            | "account/rateLimits/updated"
            | "account/updated"
            | "app/list/updated"
            | "autoApprovalReview/strictReviewRequired"
            | "command/exec/outputDelta"
            | "configWarning"
            | "deprecationNotice"
            | "error"
            | "externalAgentConfig/import/completed"
            | "externalAgentConfig/import/progress"
            | "fs/changed"
            | "fuzzyFileSearch/sessionCompleted"
            | "fuzzyFileSearch/sessionUpdated"
            | "guardianWarning"
            | "hook/completed"
            | "hook/started"
            | "item/agentMessage/delta"
            | "item/autoApprovalReview/completed"
            | "item/autoApprovalReview/started"
            | "item/commandExecution/outputDelta"
            | "item/commandExecution/terminalInteraction"
            | "item/fileChange/outputDelta"
            | "item/fileChange/patchUpdated"
            | "item/mcpToolCall/progress"
            | "item/plan/delta"
            | "item/reasoning/summaryPartAdded"
            | "item/reasoning/summaryTextDelta"
            | "item/reasoning/textDelta"
            | "mcpServer/event/stream/notification"
            | "mcpServer/oauthLogin/completed"
            | "mcpServer/startupStatus/updated"
            | "model/rerouted"
            | "model/safetyBuffering/updated"
            | "model/verification"
            | "modelProvider/authRecoveryCompleted"
            | "modelProvider/authRecoveryStarted"
            | "process/exited"
            | "process/outputDelta"
            | "project/changed"
            | "remoteControl/status/changed"
            | "serverRequest/resolved"
            | "skills/changed"
            | "thread/attachment/updated"
            | "thread/compacted"
            | "thread/goal/cleared"
            | "thread/goal/updated"
            | "thread/name/updated"
            | "thread/project/updated"
            | "thread/queue/changed"
            | "thread/tokenUsage/updated"
            | "turn/diff/updated"
            | "turn/moderationMetadata"
            | "turn/plan/updated"
            | "warning"
            | "windows/worldWritableWarning"
            | "windowsSandbox/setupCompleted"
    )
}
