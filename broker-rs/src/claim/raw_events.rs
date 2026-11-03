//! raw 开关由 broker 自动打开；客户端可见性只来自其自身明确请求。
use super::*;

pub(super) fn is_raw(method: &str) -> bool {
    matches!(
        method,
        "rawResponseItem/completed" | "rawResponse/completed"
    )
}

impl Machine {
    pub(crate) fn client_wants_raw(&self, thread: &str, conn: ConnId) -> bool {
        let mut current = thread.to_owned();
        for _ in 0..=crate::relations::MAX_DEPTH {
            if self
                .records
                .get(&current)
                .is_some_and(|record| record.raw_clients.contains(&conn))
            {
                return true;
            }
            let Some(edge) = self.thread_relation(&current) else {
                return false;
            };
            current = edge.parent_thread_id;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn apply(m: &mut Machine, event: Event) -> Vec<Action> {
        let (next, actions) = step(std::mem::take(m), event);
        *m = next;
        actions
    }
    fn request(m: &mut Machine, conn: ConnId, method: &str, params: Value) -> Vec<Action> {
        apply(
            m,
            Event::ClientRequest {
                request: ClientRequest {
                    conn,
                    id: json!(11),
                    method: method.into(),
                    params: Some(params),
                },
                run_id: Some("raw-test-run".into()),
            },
        )
    }
    #[test]
    fn raw_switch_start_forces_true_keeps_client_preference_and_never_buffers() {
        for preference in [None, Some(false), Some(true)] {
            let mut m = Machine::new(true);
            apply(&mut m, Event::Connected { conn: 1 });
            apply(&mut m, Event::Initialized { conn: 1, cxb: true });
            apply(&mut m, Event::Connected { conn: 2 });
            apply(&mut m, Event::Initialized { conn: 2, cxb: true });
            let mut params = json!({});
            if let Some(value) = preference {
                params["experimentalRawEvents"] = json!(value);
            }
            let actions = request(&mut m, 1, "thread/start", params.clone());
            let context = actions
                .into_iter()
                .find_map(|a| match a {
                    Action::Request(c) if c.method == "thread/start" => Some(c),
                    _ => None,
                })
                .unwrap();
            assert_eq!(
                context.params.as_ref().unwrap()["experimentalRawEvents"],
                true,
                "broker 必须开启上游 raw"
            );
            assert_eq!(
                context.client.as_ref().unwrap().params.as_ref().unwrap(),
                &params
            );
            apply(
                &mut m,
                Event::Reply {
                    context,
                    result: Ok(json!({"thread":{"id":"root"}})),
                },
            );
            request(
                &mut m,
                1,
                "broker/threads/watch",
                json!({"threadId":"root"}),
            );
            request(
                &mut m,
                2,
                "broker/threads/watch",
                json!({"threadId":"root"}),
            );
            request(
                &mut m,
                1,
                "turn/start",
                json!({"threadId":"root","input":[]}),
            );
            assert!(
                m.claims["root"].settling(),
                "前置：共享模式 settling 尚未定案"
            );
            for method in ["rawResponseItem/completed", "rawResponse/completed"] {
                let actions = apply(
                    &mut m,
                    Event::Notification {
                        method: method.into(),
                        params: Some(json!({"threadId":"root","turnId":"t",
                    "item":{"type":"tool_output","body":"大".repeat(12*1024*1024)}})),
                    },
                );
                let recipients: Vec<_> = actions
                    .iter()
                    .filter_map(|a| match a {
                        Action::Send { conn, message } if message["method"] == method => {
                            Some(*conn)
                        }
                        _ => None,
                    })
                    .collect();
                assert_eq!(
                    recipients,
                    if preference == Some(true) {
                        vec![1]
                    } else {
                        vec![]
                    },
                    "raw 只转发给主动请求的客户端"
                );
                assert!(m.claims["root"].settling());
                assert_eq!(
                    m.claims["root"].held_bytes, 0,
                    "大 raw 不得占 settling 预算"
                );
                assert_eq!(m.settling_bytes, 0);
                assert!(!m.claims["root"].overflowed);
            }
        }
    }
    #[test]
    fn raw_switch_fork_and_resume_do_not_inject_unsupported_field() {
        let mut m = Machine::default();
        apply(&mut m, Event::Connected { conn: 1 });
        apply(&mut m, Event::Initialized { conn: 1, cxb: true });
        for method in ["thread/fork", "thread/resume"] {
            let actions = request(&mut m, 1, method, json!({"threadId":"root"}));
            let c = actions
                .iter()
                .find_map(|a| match a {
                    Action::Request(c) if c.method == method => Some(c),
                    _ => None,
                })
                .unwrap();
            assert!(c
                .params
                .as_ref()
                .unwrap()
                .get("experimentalRawEvents")
                .is_none());
        }
    }
}
