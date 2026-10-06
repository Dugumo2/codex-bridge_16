//! 将状态机动作交给真实 RPC 写任务；用屏障控制排队、写出及确认到达顺序。
use super::*;
use crate::appserver::{rpc, AppEvent};
use crate::claim::{step, Event};
use tokio::sync::mpsc;

struct BarrierWriter {
    entered: mpsc::UnboundedSender<String>,
    release: mpsc::UnboundedReceiver<Result<(), String>>,
    written: mpsc::UnboundedSender<String>,
}
impl rpc::MessageWriter for BarrierWriter {
    async fn write(&mut self, text: String) -> Result<(), String> {
        self.entered.send(text.clone()).unwrap();
        self.release.recv().await.expect("测试释放写入屏障")?;
        self.written.send(text).unwrap();
        Ok(())
    }
}
fn execute(client: &rpc::ClientHandle, actions: Vec<Action>) {
    for action in actions {
        match action {
            Action::AppReplyAcked { op, id, payload } => {
                client.respond_acked(op, id, payload).unwrap();
            }
            Action::CancelAppReply { op } => client.cancel_reply(op),
            Action::ServerError { id, error } => client.respond(
                id,
                Err((
                    error["code"].as_i64().unwrap(),
                    error["message"].as_str().unwrap().into(),
                    error.get("data").cloned(),
                )),
            ),
            Action::ServerResult { id, result } => client.respond(id, Ok(result)),
            _ => (),
        }
    }
}

#[tokio::test]
async fn 关闭按原请求编号最多写一份回答() {
    for phase in ["排队", "写出中", "已写出尚未交付确认", "写入失败"] {
        let (_input, inbound) = mpsc::unbounded_channel();
        let (entered, mut entering) = mpsc::unbounded_channel();
        let (release, results) = mpsc::unbounded_channel();
        let (written, mut wire) = mpsc::unbounded_channel();
        let (client, mut events) = rpc::new_client(
            inbound,
            BarrierWriter {
                entered,
                release: results,
                written,
            },
            || async {},
            None,
        );
        let mut m = machine();
        own(&mut m, "t");
        let app_id = json!("原服务端请求-7");
        let id = register(
            &mut m,
            "未来方法",
            json!({}),
            app_id.clone(),
            &mut Vec::new(),
        );
        if phase == "排队" {
            client.notify("前置屏障", None);
            assert_eq!(
                serde_json::from_str::<Value>(&entering.recv().await.unwrap()).unwrap()["method"],
                "前置屏障"
            );
        }
        execute(&client, resolve(&mut m, 1, &id, json!({"回答":true})));
        if phase != "排队" {
            assert_eq!(
                serde_json::from_str::<Value>(&entering.recv().await.unwrap()).unwrap()["id"],
                app_id
            );
        }
        let held_confirmation = if phase == "已写出尚未交付确认" {
            release.send(Ok(())).unwrap();
            Some(events.recv().await.unwrap())
        } else {
            None
        };
        let (next, closing) = step(m, Event::Shutdown);
        m = next;
        // 此处包含关闭之后的认领释放，必须保留待确认 op。
        assert_eq!(m.inbox.entries[&id].item.status, InboxStatus::Resolving);
        assert!(!closing
            .iter()
            .any(|a| matches!(a, Action::ServerError { id, .. } if id == &app_id)));
        execute(&client, closing);
        if phase == "写出中" {
            release.send(Ok(())).unwrap();
        }
        if phase == "写入失败" {
            release.send(Err("测试写失败".into())).unwrap();
        }
        let confirmation = match held_confirmation {
            Some(event) => event,
            None => events.recv().await.unwrap(),
        };
        let AppEvent::ReplyWritten { op, outcome } = confirmation else {
            panic!("应先确认回答");
        };
        let (next, actions) = step(m, Event::AppReplyWritten { op, outcome });
        m = next;
        execute(&client, actions);
        if phase == "写入失败" {
            assert!(matches!(events.recv().await, Some(AppEvent::Closed { .. })));
            client.close().await;
        } else {
            client.notify("最终屏障", None);
            if phase == "排队" {
                release.send(Ok(())).unwrap();
            }
            loop {
                let text = entering.recv().await.unwrap();
                let value: Value = serde_json::from_str(&text).unwrap();
                release.send(Ok(())).unwrap();
                if value["method"] == "最终屏障" {
                    break;
                }
            }
            // 正常 close 排空写任务，之后采集的就是完整线上记录。
            client.close().await;
        }
        let mut replies = Vec::new();
        while let Some(text) = wire.recv().await {
            let value: Value = serde_json::from_str(&text).unwrap();
            if value.get("id") == Some(&app_id) {
                replies.push(value);
            }
        }
        assert!(replies.len() <= 1, "阶段 {phase} 不得重复回答：{replies:?}");
        if phase == "写入失败" {
            assert!(replies.is_empty());
        } else if phase == "排队" {
            assert_eq!(replies.len(), 1);
            assert_eq!(
                replies[0]["error"]["message"],
                "Handler error: Broker shutting down"
            );
        } else {
            assert_eq!(replies.len(), 1);
            assert_eq!(replies[0]["result"], json!({"回答":true}));
        }
        assert_eq!(
            m.inbox.entries[&id].item.status,
            if matches!(phase, "排队" | "写入失败") {
                InboxStatus::Cancelled
            } else {
                InboxStatus::Resolved
            }
        );
        assert!(m.inbox.operations.is_empty());
    }
}
