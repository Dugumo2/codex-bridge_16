//! 状态机动作的运行时执行（详细设计 1.2、2.1、3.4、4.2、5、8 节）。
//!
//! 请求在 core 决定的顺序放入 app-server 出站队列，计时与等待在独立任务
//! 中完成。app-server 回包带操作类别与 claim_id；转发计时器带编号与独立代号。

use std::hash::{BuildHasher, RandomState};
use std::time::{SystemTime, UNIX_EPOCH};

use super::*;

impl Core {
    pub(super) fn dispatch(&mut self, event: Event) {
        if let Some((instance, exited)) = self.app.inner.private_instance() {
            self.machine.lifecycle.instance = Some(instance.clone());
            if exited {
                self.machine.lifecycle_instance_exited(&instance);
            }
        }
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.machine.clock = crate::claim::Clock {
            now_ms,
            entropy: RandomState::new().hash_one((now_ms, self.next_tag)),
            mono_ms: self.started.elapsed().as_millis() as u64,
        };
        #[cfg(test)]
        if let Some(clock) = &self.fixed_clock {
            self.machine.clock = *clock;
        }
        let (machine, actions) = claim::step(std::mem::take(&mut self.machine), event);
        self.machine = machine;
        self.execute(actions);
        self.progress_timers.retain(|run, (_, timer)| {
            let live = self
                .machine
                .claims
                .values()
                .any(|c| c.run_id.as_ref() == Some(run));
            if !live {
                timer.abort();
            }
            live
        });
    }

    pub(super) fn execute(&mut self, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::ScheduleBreakerProbe {
                    generation,
                    delay_ms,
                } => {
                    let mailbox = self.mailbox.clone();
                    let deadline = Instant::now() + Duration::from_millis(delay_ms);
                    let task = tokio::spawn(async move {
                        tokio::time::sleep_until(deadline).await;
                        let _ = mailbox.send(CoreMsg::BreakerProbeTimer { generation });
                    });
                    if let Some((_, timer)) =
                        self.probe_timer.replace((generation, task.abort_handle()))
                    {
                        timer.abort();
                    }
                }
                Action::CancelBreakerProbe { generation } => {
                    if self
                        .probe_timer
                        .as_ref()
                        .is_some_and(|(current, _)| *current == generation)
                    {
                        if let Some((_, timer)) = self.probe_timer.take() {
                            timer.abort();
                        }
                    }
                }
                Action::ForgetLate { tag } => self.forget_late(tag),
                Action::ScheduleAt {
                    token,
                    kind,
                    deadline,
                } => {
                    let mailbox = self.mailbox.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep_until(Instant::from_std(deadline)).await;
                        let _ = mailbox.send(CoreMsg::ClaimTimer { token, kind });
                    });
                }
                Action::Broadcast { message } => {
                    let line = proto::to_line(&message);
                    for conn in self.conns.values() {
                        let _ = conn.writer.send(WriterCmd::Line(line.clone()));
                    }
                }
                Action::Send { conn, message } => self.send(conn, &message),
                Action::Request(context) => self.forward_request(context),
                Action::Schedule {
                    token,
                    kind,
                    delay_ms,
                } => {
                    if matches!(&kind, TimerKind::Run(_)) && self.machine.closing {
                        continue;
                    }
                    if matches!(&kind, TimerKind::Run(RunTimer::DailyCleanup))
                        && self.daily_timer.is_some()
                    {
                        continue;
                    }
                    let run_timer = match &kind {
                        TimerKind::Run(timer) => Some(timer.clone()),
                        _ => None,
                    };
                    let keyed = kind.key(&token).map(|key| (key, kind.clone()));
                    let mailbox = self.mailbox.clone();
                    let deadline = Instant::now() + Duration::from_millis(delay_ms);
                    let task = tokio::spawn(async move {
                        if delay_ms == 0 {
                            tokio::task::yield_now().await;
                        } else {
                            tokio::time::sleep_until(deadline).await;
                        }
                        let _ = mailbox.send(CoreMsg::ClaimTimer { token, kind });
                    });
                    match run_timer {
                        Some(RunTimer::ProgressFlush { run_id, generation }) => {
                            if let Some((_, old)) = self
                                .progress_timers
                                .insert(run_id, (generation, task.abort_handle()))
                            {
                                old.abort();
                            }
                        }
                        Some(RunTimer::DailyCleanup) => {
                            self.daily_timer = Some(task.abort_handle())
                        }
                        _ => {}
                    }
                    if let Some((key, kind)) = keyed {
                        if let Some((_, old)) =
                            self.keyed_timers.insert(key, (kind, task.abort_handle()))
                        {
                            old.abort();
                        }
                    }
                }
                Action::Dismiss { id } => self.app.dismiss(&id),
                Action::ServerResult { id, result } => self.app.respond(id, result),
                Action::ScheduleForward {
                    forward_id,
                    generation,
                    delay_ms,
                } => {
                    let mailbox = self.mailbox.clone();
                    let key = (forward_id.clone(), generation);
                    let task = tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        let _ = mailbox.send(CoreMsg::ForwardTimer {
                            forward_id,
                            generation,
                        });
                    });
                    self.forward_timers.insert(key, task.abort_handle());
                }
                Action::CancelForwardTimer {
                    forward_id,
                    generation,
                } => {
                    if let Some(timer) = self.forward_timers.remove(&(forward_id, generation)) {
                        timer.abort();
                    }
                }
                Action::ServerError { id, error } => self.app.respond_error(
                    id,
                    error["code"].as_i64().unwrap_or(code::GENERAL_ERROR),
                    error["message"]
                        .as_str()
                        .unwrap_or("服务端请求处理失败")
                        .into(),
                    error.get("data").cloned(),
                ),
                Action::Store(op) => {
                    if self.store_terminal {
                        continue;
                    }
                    if let Some(store) = &self.store {
                        store.submit(op);
                    }
                }
                Action::AppReplyAcked { op, id, payload } => {
                    if let Err(reason) = self.app.respond_acked(op, id, payload) {
                        // 未被受理的回答走同一条事件路径结算，保证每个 op 恰好一次 ReplyWritten。
                        let _ = self.mailbox.send(CoreMsg::App(AppEvent::ReplyWritten {
                            op,
                            outcome: crate::inbox::ReplyOutcome::Failed(reason),
                        }));
                    }
                }
                Action::CancelAppReply { op } => self.app.cancel_reply(op),
                Action::ForgetServerRequest { request_key } => {
                    self.app.forget_server_request(&request_key)
                }
                Action::Unschedule { key } => {
                    if let Some((_, timer)) = self.keyed_timers.remove(&key) {
                        timer.abort();
                    }
                }
                Action::ReadRollout(request) => {
                    // 文件读取是阻塞 IO：放到阻塞线程，读完把结果送回邮箱；状态机不碰文件。
                    let mailbox = self.mailbox.clone();
                    tokio::task::spawn_blocking(move || {
                        let result =
                            crate::rollout::read_chunk(&request.path, request.cursor.as_ref());
                        let _ = mailbox.send(CoreMsg::RolloutRead {
                            id: request.id,
                            thread: request.thread,
                            result,
                        });
                    });
                }
                Action::Warn(message) => tracing::warn!("{message}"),
                Action::Debug(message) => tracing::debug!("{message}"),
            }
        }
    }
}

/// 2.1：十进制毫秒时间戳加六位 base36；沿用 runId 的系统随机种子，无新增依赖。
pub(super) fn forward_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let suffix = RandomState::new().hash_one((millis, std::process::id())) % 36_u64.pow(6);
    format!("broker-{millis}-{:0>6}", base36(u128::from(suffix)))
}

pub(super) fn run_id_for(method: &str, params: &Option<Value>) -> Option<String> {
    if !matches!(method, "turn/start" | "review/start" | "turn/steer")
        || !params
            .as_ref()
            .and_then(|p| p.get("threadId"))
            .is_some_and(Value::is_string)
    {
        return None;
    }
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    // RandomState 的系统随机种子生成三字节后缀，保持上游 run-时间36进制-六位十六进制格式。
    let suffix = RandomState::new().hash_one((millis, std::process::id())) & 0xff_ffff;
    Some(format!("run-{}-{suffix:06x}", base36(millis)))
}

fn base36(mut value: u128) -> String {
    let mut digits = Vec::new();
    loop {
        let digit = (value % 36) as u8;
        digits.push(if digit < 10 {
            b'0' + digit
        } else {
            b'a' + digit - 10
        });
        value /= 36;
        if value == 0 {
            break;
        }
    }
    digits.reverse();
    String::from_utf8(digits).expect("运行编号只含 ASCII 字符")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn 转发编号采用十进制毫秒和六位小写三十六进制() {
        for _ in 0..100 {
            let id = forward_id();
            let parts: Vec<_> = id.split('-').collect();
            assert_eq!(parts.len(), 3);
            assert_eq!(parts[0], "broker");
            assert!(parts[1].parse::<u128>().is_ok());
            assert_eq!(parts[2].len(), 6);
            assert!(parts[2]
                .bytes()
                .all(|b| b.is_ascii_digit() || b.is_ascii_lowercase()));
        }
    }
    #[test]
    fn 运行编号采用上游格式且非流式不分配() {
        let id = run_id_for("turn/start", &Some(json!({"threadId": "t"}))).unwrap();
        let parts: Vec<_> = id.split('-').collect();
        assert_eq!(parts[0], "run");
        assert!(u128::from_str_radix(parts[1], 36).is_ok());
        assert_eq!(parts[2].len(), 6);
        assert!(u32::from_str_radix(parts[2], 16).is_ok());
        assert!(run_id_for("thread/start", &None).is_none());
        assert!(run_id_for("turn/start", &Some(json!({"threadId": 7}))).is_none());
    }
}
