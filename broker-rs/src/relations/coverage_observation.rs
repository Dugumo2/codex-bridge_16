//! 连续观察凭据：真实开始只使用先验 raw，任何断档在本进程内不可恢复。
pub(crate) mod validation;
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
                    ) || completed
                        .get("error")
                        .is_some_and(|error| !error.is_null() && !error.is_object())
                    {
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
            "rawResponse/completed" => {
                if thread.is_none() || turn.is_none() {
                    self.observation_gap(thread, "raw响应缺少有效身份");
                }
            }
            "rawResponseItem/completed" => {
                let item = params.and_then(|p| p.get("item"));
                if thread.is_none() || turn.is_none() || !item.is_some_and(explained_raw_item) {
                    self.observation_gap(thread, "raw条目无法解释");
                }
            }
            "item/started" | "item/completed" => {
                let item = params.and_then(|p| p.get("item"));
                if thread.is_none() || turn.is_none() || !item.is_some_and(explained_thread_item) {
                    self.observation_gap(thread, "轮条目无法解释");
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
                    (Some(_), Some("idle" | "systemError")) => {}
                    (Some(_), Some("active"))
                        if params
                            .and_then(|p| p.get("status"))
                            .and_then(|status| status.get("activeFlags"))
                            .and_then(Value::as_array)
                            .is_some_and(|flags| {
                                flags.iter().all(|flag| {
                                    matches!(
                                        flag.as_str(),
                                        Some("waitingOnApproval" | "waitingOnUserInput")
                                    )
                                })
                            }) => {}
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

fn string_array(value: &Value) -> bool {
    value
        .as_array()
        .is_some_and(|items| items.iter().all(Value::is_string))
}

fn optional_text(item: &Value, field: &str) -> bool {
    item.get(field)
        .is_none_or(|value| value.is_null() || value.is_string())
}

fn raw_content(value: &Value) -> bool {
    value.as_array().is_some_and(|parts| {
        parts
            .iter()
            .all(|part| match part.get("type").and_then(Value::as_str) {
                Some("input_text" | "output_text") => {
                    part.get("text").is_some_and(Value::is_string)
                }
                Some("input_image") => {
                    part.get("image_url").is_some_and(Value::is_string)
                        || part.get("file_id").is_some_and(Value::is_string)
                }
                Some("input_audio") => part.get("audio_url").is_some_and(Value::is_string),
                _ => false,
            })
    })
}

fn output_body(value: Option<&Value>) -> bool {
    // 工具结果有独立的内容枚举，不能把加密结果误当未知输入，也不接纳 output_text。
    value.is_some_and(|value| {
        value.is_string()
            || value.as_array().is_some_and(|parts| {
                parts
                    .iter()
                    .all(|part| match part.get("type").and_then(Value::as_str) {
                        Some("input_text") => part.get("text").is_some_and(Value::is_string),
                        Some("input_image") => {
                            part.get("image_url").is_some_and(Value::is_string)
                                || part.get("file_id").is_some_and(Value::is_string)
                        }
                        Some("input_audio") => part.get("audio_url").is_some_and(Value::is_string),
                        Some("encrypted_content") => {
                            part.get("encrypted_content").is_some_and(Value::is_string)
                        }
                        _ => false,
                    })
            })
    })
}

/// 公开条目使用驼峰动作名，raw 使用下划线动作名；参数的可选性按上游枚举处理。
fn explained_search_action(action: &Value, camel_case: bool) -> bool {
    match action.get("type").and_then(Value::as_str) {
        Some("search") => {
            optional_text(action, "query")
                && action
                    .get("queries")
                    .is_none_or(|queries| queries.is_null() || string_array(queries))
        }
        Some(kind) if kind == if camel_case { "openPage" } else { "open_page" } => {
            optional_text(action, "url")
        }
        Some(kind)
            if kind
                == if camel_case {
                    "findInPage"
                } else {
                    "find_in_page"
                } =>
        {
            optional_text(action, "url") && optional_text(action, "pattern")
        }
        Some("other") => true,
        _ => false,
    }
}

fn explained_raw_item(item: &Value) -> bool {
    if !validation::response_optional_fields(item) {
        return false;
    }
    match item.get("type").and_then(Value::as_str) {
        Some("agent_message") => v2_pairing::parse_delivery(item).is_some(),
        Some("reasoning") => {
            item.get("summary")
                .and_then(Value::as_array)
                .is_some_and(|parts| {
                    parts
                        .iter()
                        .all(|part| part["type"] == "summary_text" && part["text"].is_string())
                })
                && item.get("content").is_none_or(|value| {
                    value.is_null()
                        || value.as_array().is_some_and(|parts| {
                            parts.iter().all(|part| {
                                matches!(part["type"].as_str(), Some("reasoning_text" | "text"))
                                    && part["text"].is_string()
                            })
                        })
                })
                && optional_text(item, "encrypted_content")
        }
        Some("function_call") => {
            identity(item.get("call_id")).is_some()
                && identity(item.get("name")).is_some()
                && item.get("arguments").is_some_and(Value::is_string)
        }
        Some("function_call_output") => {
            optional_text(item, "call_id") && output_body(item.get("output"))
        }
        Some("message") => {
            matches!(item.get("role").and_then(Value::as_str), Some("assistant"))
                && item.get("content").is_some_and(raw_content)
        }
        Some("custom_tool_call") => {
            identity(item.get("call_id")).is_some()
                && identity(item.get("name")).is_some()
                && item.get("input").is_some_and(Value::is_string)
        }
        Some("custom_tool_call_output") => {
            identity(item.get("call_id")).is_some() && output_body(item.get("output"))
        }
        Some("tool_search_call") => {
            item.get("execution").is_some_and(Value::is_string)
                && item.get("arguments").is_some()
                && optional_text(item, "call_id")
                && optional_text(item, "status")
        }
        Some("tool_search_output") => {
            item.get("execution").is_some_and(Value::is_string)
                && item.get("status").is_some_and(Value::is_string)
                && item.get("tools").is_some_and(Value::is_array)
                && optional_text(item, "call_id")
        }
        Some("web_search_call") => {
            optional_text(item, "status")
                && item
                    .get("action")
                    .is_none_or(|action| action.is_null() || explained_search_action(action, false))
        }
        Some("local_shell_call") => {
            matches!(
                item["status"].as_str(),
                Some("in_progress" | "completed" | "incomplete")
            ) && item["action"]["type"] == "exec"
                && string_array(&item["action"]["command"])
        }
        Some("image_generation_call") => {
            item.get("status").is_some_and(Value::is_string)
                && item.get("result").is_some_and(Value::is_string)
        }
        Some("compaction" | "compaction_summary") => {
            item.get("encrypted_content").is_some_and(Value::is_string)
        }
        Some("context_compaction") => optional_text(item, "encrypted_content"),
        Some("compaction_trigger") => true,
        // 用户、hook、附加工具及配置输入不属于已确认无关输出；保守处理。
        _ => false,
    }
}

fn explained_thread_item(item: &Value) -> bool {
    if !validation::public_optional_fields(item) {
        return false;
    }
    if identity(item.get("id")).is_none() {
        return false;
    }
    let text = |key| item.get(key).is_some_and(Value::is_string);
    let status = || {
        matches!(
            item["status"].as_str(),
            Some("inProgress" | "completed" | "failed" | "declined")
        )
    };
    match item.get("type").and_then(Value::as_str) {
        Some("agentMessage" | "plan") => text("text"),
        Some("reasoning") => {
            item.get("summary").is_none_or(string_array)
                && item.get("content").is_none_or(string_array)
        }
        Some("functionCallOutput") => text("name") && output_body(item.get("output")),
        Some("commandExecution") => {
            text("command")
                && text("cwd")
                && status()
                && item
                    .get("commandActions")
                    .is_some_and(validation::command_actions)
        }
        Some("fileChange") => status() && item.get("changes").is_some_and(validation::file_changes),
        Some("mcpToolCall") => {
            text("server")
                && text("tool")
                && matches!(
                    item["status"].as_str(),
                    Some("inProgress" | "completed" | "failed")
                )
                && item.get("arguments").is_some()
        }
        Some("dynamicToolCall") => {
            text("tool")
                && matches!(
                    item["status"].as_str(),
                    Some("inProgress" | "completed" | "failed")
                )
                && item.get("arguments").is_some()
        }
        Some("collabAgentToolCall") => {
            matches!(
                item["tool"].as_str(),
                Some(
                    "spawnAgent"
                        | "sendInput"
                        | "resumeAgent"
                        | "wait"
                        | "closeAgent"
                        | "sendMessage"
                        | "followupTask"
                        | "interruptAgent"
                        | "listAgents"
                )
            ) && matches!(
                item["status"].as_str(),
                Some("inProgress" | "completed" | "failed" | "interrupted")
            ) && identity(item.get("senderThreadId")).is_some()
                && item.get("receiverThreadIds").is_some_and(string_array)
                && item
                    .get("agentsStates")
                    .is_some_and(validation::agent_states)
        }
        Some("subAgentActivity") => {
            matches!(
                item["kind"].as_str(),
                Some("started" | "interacted" | "interrupted" | "completed")
            ) && identity(item.get("agentThreadId")).is_some()
                && identity(item.get("agentPath")).is_some()
        }
        Some("imageView") => text("path"),
        Some("enteredReviewMode" | "exitedReviewMode") => text("review"),
        Some("contextCompaction") => true,
        Some("webSearch") => {
            text("query")
                && item
                    .get("action")
                    .is_none_or(|action| action.is_null() || explained_search_action(action, true))
                && item
                    .get("results")
                    .is_none_or(|results| results.is_null() || results.is_array())
        }
        Some("sleep") => item.get("durationMs").is_some_and(Value::is_u64),
        Some("imageGeneration") => {
            text("status") && text("result") && optional_text(item, "revisedPrompt")
        }
        _ => false,
    }
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
