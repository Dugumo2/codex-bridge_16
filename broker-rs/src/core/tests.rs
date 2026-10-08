//! core 动作执行的集成验证（详细设计 1.2、3.4、8 节）。
//!
//! 内存传输使用真实 app-server JSON-RPC 客户端，验证请求出站、回包入箱、
//! 计时器和认领代号，而非把异步动作替换成空分支。

use super::*;
use crate::appserver::rpc;
use crate::claim::{Operation, State};

#[tokio::test(start_paused = true)]
async fn m7普通客户端无status目标写入在途时关闭先暂停再中断() {
    let mut h = Harness::new(false);
    h.start_running().await;
    h.core.machine.shared = true;
    h.client(
        8,
        "thread/goal/set",
        json!({"threadId":"线程","objective":"继续工作","tokenBudget":100}),
    );
    let active = h.outgoing().await;
    assert!(active["params"].get("status").is_none());
    assert!(!h.core.machine.records["线程"].goal_active);
    h.client(9, "broker/shutdown", json!({}));
    assert_eq!(
        h.core.shutdown_stage,
        Some(ShutdownStage::Waiting(ShutdownWait::StopOwn))
    );
    let pause = h.outgoing().await;
    assert_eq!(pause["method"], "thread/goal/set");
    assert_eq!(pause["params"]["status"], "paused");
    h.answer(&active, Ok(json!({"goal":{"status":"active"}})))
        .await;
    h.answer(&pause, Ok(json!({"goal":{"status":"paused"}})))
        .await;
    let interrupt = h.outgoing().await;
    assert_eq!(interrupt["method"], "turn/interrupt");
    h.answer(&interrupt, Ok(json!({}))).await;
    assert!(h.core.machine.closing);
}

#[tokio::test(start_paused = true)]
async fn m7关闭stop_own暂停在途断线立即清理且迟到回包不再中断() {
    let mut h = Harness::new(false);
    h.start_running().await;
    h.core.machine.shared = true;
    h.core.machine.records.get_mut("线程").unwrap().goal_active = true;
    h.core.begin_shutdown(0);
    let pause = h.outgoing().await;
    assert_eq!(pause["method"], "thread/goal/set");
    let (&tag, context) = h
        .core
        .pending_app
        .iter()
        .find(|(_, c)| c.method == "thread/goal/set")
        .unwrap();
    let context = context.clone();
    assert_eq!(
        h.core.shutdown_stage,
        Some(ShutdownStage::Waiting(ShutdownWait::StopOwn))
    );
    let before = Instant::now();
    h.core.handle(CoreMsg::App(AppEvent::Closed {
        reason: "测试暂停期间断开".into(),
    }));
    assert_eq!(Instant::now(), before);
    assert_eq!(h.core.shutdown_stage, Some(ShutdownStage::Closing));
    assert!(h.core.shutdown_wait_timer.is_none());
    assert_eq!(h.core.machine.shutdown_stops_pending(), 0);
    assert!(h.core.machine.claims.is_empty());
    h.core.handle(CoreMsg::App(AppEvent::Reply {
        tag,
        result: Ok(json!({"goal":{"status":"paused"}})),
    }));
    h.core
        .handle(CoreMsg::ShutdownWaitDeadline(ShutdownWait::StopOwn));
    let actions = crate::claim::step(
        h.core.machine.clone(),
        Event::Reply {
            context,
            result: Ok(json!({})),
        },
    )
    .1;
    assert!(!actions.iter().any(|a| matches!(a, Action::Request(_))));
    tokio::task::yield_now().await;
    while let Ok(line) = h.app_output.try_recv() {
        let value: Value = serde_json::from_str(&line).unwrap();
        assert_ne!(value["method"], "turn/interrupt");
    }
}

#[tokio::test(start_paused = true)]
async fn m7溢出丢失目标completed后补读释放并允许if_idle关闭() {
    let mut h = Harness::new(true);
    h.core
        .machine
        .records
        .entry("线程".into())
        .or_default()
        .subscribed = true;
    h.client(1, "turn/start", json!({"threadId":"线程"}));
    let start = h.outgoing().await;
    for _ in 0..=crate::claim::SETTLING_HARD_ITEMS {
        h.core.dispatch(Event::Notification {
            method: "future/event".into(),
            params: Some(json!({"threadId":"线程"})),
        });
    }
    for _ in 0..crate::claim::OVERFLOW_LIFECYCLE_ITEMS {
        h.core.dispatch(Event::Notification {
            method: "turn/future".into(),
            params: Some(json!({"threadId":"线程"})),
        });
    }
    h.notification(
        "turn/completed",
        json!({"threadId":"线程","turn":{"id":"目标轮","status":"completed","items":[]}}),
    )
    .await;
    h.answer(&start, Ok(json!({"turn":{"id":"目标轮"}}))).await;
    let check = h.outgoing().await;
    assert_eq!(check["method"], "thread/read");
    assert_eq!(check["params"]["threadId"], "线程");
    assert!(h.core.is_busy());
    h.answer(&check, Ok(json!({"thread":{"turns":[]}}))).await;
    assert!(h.core.machine.claims.is_empty());
    assert!(!h.core.is_busy());
    h.client(99, "broker/shutdown", json!({"ifIdle":true}));
    assert_ne!(h.core.phase, Phase::Running);
    assert!(h.core.machine.closing);
}

#[tokio::test(start_paused = true)]
async fn m7共享关闭先定案再暂停中断最后收集结果() {
    let mut h = Harness::new(true);
    h.core
        .machine
        .records
        .entry("线程".into())
        .or_default()
        .subscribed = true;
    h.client(1, "turn/start", json!({"threadId":"线程"}));
    let start = h.outgoing().await;
    h.answer(&start, Ok(json!({"turn":{"id":"本轮"}}))).await;
    let settlement = h.outgoing().await;
    assert_eq!(settlement["method"], "thread/read");
    h.core.machine.records.get_mut("线程").unwrap().goal_active = true;
    // 留一个真实结果补读在途，便于观察 Collecting，而不让它立即推进到 Closing。
    h.core.forward_request(result_context());
    let result = h.outgoing().await;
    h.core.begin_shutdown(0);
    assert!(h.core.machine.quiescing);
    assert_eq!(
        h.core.shutdown_stage,
        Some(ShutdownStage::Waiting(ShutdownWait::Settle))
    );
    h.notification(
        "turn/started",
        json!({"threadId":"线程","turn":{"id":"本轮"}}),
    )
    .await;
    h.answer(
        &settlement,
        Ok(json!({"thread":{"turns":[{"id":"本轮","status":"inProgress"}]}})),
    )
    .await;
    assert_eq!(
        h.core.shutdown_stage,
        Some(ShutdownStage::Waiting(ShutdownWait::StopOwn))
    );
    let pause = h.outgoing().await;
    assert_eq!(pause["method"], "thread/goal/set");
    assert_eq!(pause["params"]["status"], "paused");
    h.core
        .handle(CoreMsg::ShutdownWaitDeadline(ShutdownWait::Settle));
    assert_eq!(
        h.core.shutdown_stage,
        Some(ShutdownStage::Waiting(ShutdownWait::StopOwn))
    );
    h.answer(&pause, Ok(json!({"goal":{"status":"paused"}})))
        .await;
    let interrupt = h.outgoing().await;
    assert_eq!(interrupt["method"], "turn/interrupt");
    assert_eq!(interrupt["params"]["turnId"], "本轮");
    h.answer(&interrupt, Ok(json!({}))).await;
    assert_eq!(h.core.shutdown_stage, Some(ShutdownStage::Collecting));
    assert!(h.core.machine.claims.is_empty());
    assert_eq!(h.core.machine.settling_bytes, 0);
    let before = h.core.machine.clone();
    h.core
        .handle(CoreMsg::ShutdownWaitDeadline(ShutdownWait::Settle));
    h.core
        .handle(CoreMsg::ShutdownWaitDeadline(ShutdownWait::StopOwn));
    assert_eq!(h.core.machine, before);
    h.answer(&result, Ok(json!({"thread":{"turns":[]}}))).await;
    assert_eq!(h.core.shutdown_stage, Some(ShutdownStage::Closing));
}

#[tokio::test(start_paused = true)]
async fn m7共享settling五秒期满不停止未定案认领() {
    let mut h = Harness::new(true);
    h.core
        .machine
        .records
        .entry("线程".into())
        .or_default()
        .subscribed = true;
    h.client(1, "turn/start", json!({"threadId":"线程"}));
    let _start = h.outgoing().await;
    h.notification("future/event", json!({"threadId":"线程","data":"缓冲"}))
        .await;
    h.core.begin_shutdown(0);
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(4999)).await;
    assert_eq!(
        h.core.shutdown_stage,
        Some(ShutdownStage::Waiting(ShutdownWait::Settle))
    );
    tokio::time::advance(Duration::from_millis(1)).await;
    h.pump_until(|msg| matches!(msg, CoreMsg::ShutdownWaitDeadline(ShutdownWait::Settle)))
        .await;
    assert!(h.core.machine.closing);
    assert_eq!(h.core.machine.settling_bytes, 0);
    assert!(h.core.machine.shutdown_stops.is_empty());
    tokio::task::yield_now().await;
    while let Ok(line) = h.app_output.try_recv() {
        let value: Value = serde_json::from_str(&line).unwrap();
        assert_ne!(value["method"], "turn/interrupt");
        assert_ne!(value["method"], "thread/goal/set");
    }
}

#[tokio::test(start_paused = true)]
async fn m7共享停止五秒期满后暂停回包不能再中断() {
    let mut h = Harness::new(false);
    h.start_running().await;
    h.core.machine.shared = true;
    h.core.machine.records.get_mut("线程").unwrap().goal_active = true;
    h.core.begin_shutdown(0);
    let pause = h.outgoing().await;
    let context = h
        .core
        .pending_app
        .values()
        .find(|c| c.method == "thread/goal/set")
        .unwrap()
        .clone();
    assert_eq!(pause["params"]["status"], "paused");
    tokio::time::advance(Duration::from_secs(5)).await;
    h.pump_until(|msg| matches!(msg, CoreMsg::ShutdownWaitDeadline(ShutdownWait::StopOwn)))
        .await;
    assert!(h.core.machine.closing);
    assert_eq!(h.core.machine.shutdown_stops_pending(), 0);
    let (_, actions) = crate::claim::step(
        h.core.machine.clone(),
        Event::Reply {
            context,
            result: Ok(json!({"goal":{"status":"paused"}})),
        },
    );
    assert!(!actions.iter().any(|a| matches!(a, Action::Request(_))));
}

#[tokio::test(start_paused = true)]
async fn m7共享连接断开不等待而私有关闭直接收集() {
    for waiting in [false, true] {
        let mut h = Harness::new(true);
        h.core
            .machine
            .records
            .entry("线程".into())
            .or_default()
            .subscribed = true;
        h.client(1, "turn/start", json!({"threadId":"线程"}));
        let _start = h.outgoing().await;
        if waiting {
            h.core.begin_shutdown(0);
        }
        h.core.handle(CoreMsg::App(AppEvent::Closed {
            reason: "测试断开".into(),
        }));
        assert!(h.core.machine.closing);
        assert!(h.core.machine.shutdown_stops.is_empty());
        assert!(h.core.shutdown_wait_timer.is_none());
        assert_eq!(h.core.shutdown_stage, Some(ShutdownStage::Closing));
    }
    let mut h = Harness::new(false);
    h.start_running().await;
    h.core.forward_request(result_context());
    let _result = h.outgoing().await;
    h.core.begin_shutdown(0);
    assert_eq!(h.core.shutdown_stage, Some(ShutdownStage::Collecting));
    assert!(!h.core.machine.quiescing);
    assert!(h.core.shutdown_wait_timer.is_none());
    assert_eq!(h.core.machine.shutdown_stops_pending(), 0);
}

struct Harness {
    core: Core,
    inbox: mpsc::UnboundedReceiver<CoreMsg>,
    app_input: mpsc::UnboundedSender<String>,
    app_output: mpsc::UnboundedReceiver<String>,
    client_output: mpsc::UnboundedReceiver<WriterCmd>,
}

#[tokio::test(start_paused = true)]
async fn n7_存储初始化失败不能把未知生命周期当成空记录开放写请求() {
    let mut h = Harness::new(false);
    h.core = h.core.with_store(None, Loaded::default());
    h.core.restore();
    h.client(1, "turn/start", json!({"threadId":"P"}));
    let reply = h.incoming_client().await;
    assert_eq!(reply["error"]["data"]["reason"], "storageError");
    assert!(h.app_output.try_recv().is_err());
    h.client(2, "broker/health", json!({}));
    let reply = h.incoming_client().await;
    assert_eq!(reply["result"]["storage"], "degraded");
    assert!(reply["result"]["storageReason"]
        .as_str()
        .unwrap()
        .contains("无法核实"));
    h.client(3, "thread/read", json!({"threadId":"P"}));
    let request = h.outgoing().await;
    assert_eq!(request["method"], "thread/read");
}

impl Harness {
    fn new(shared: bool) -> Self {
        let (app_input, inbound) = mpsc::unbounded_channel();
        let (writer, app_output) = mpsc::unbounded_channel();
        let (inner, mut events) = rpc::new_client(inbound, writer, || async {}, Some(5000));
        let (mailbox, inbox) = mpsc::unbounded_channel();
        let event_mailbox = mailbox.clone();
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                if event_mailbox.send(CoreMsg::App(event)).is_err() {
                    break;
                }
            }
        });
        let mut core = Core {
            config: CoreConfig {
                idle_timeout: Duration::from_millis(50),
                server: ServerInfo::Private { pid: None },
            },
            app: AppHandle { inner },
            mailbox,
            accept: None,
            conns: HashMap::new(),
            machine: Machine::new(shared),
            pending_app: HashMap::new(),
            late_app: std::collections::VecDeque::new(),
            forward_timers: HashMap::new(),
            probe_timer: None,
            store: None,
            store_terminal: false,
            loaded: Loaded::default(),
            epoch: 0,
            survives_parent_job: false,
            parent_job_evidence: Value::Null,
            progress_timers: HashMap::new(),
            keyed_timers: HashMap::new(),
            daily_timer: None,
            runs_deadline: None,
            shutdown_wait_timer: None,
            health: None,
            shutdown_stage: None,
            app_disconnected: false,
            fixed_clock: None,
            next_tag: 0,
            last_activity: Instant::now(),
            started: Instant::now(),
            phase: Phase::Running,
        };
        let (writer, client_output) = mpsc::unbounded_channel();
        core.handle(CoreMsg::Conn(ConnEvent::Connected { conn: 1, writer }));
        Self {
            core,
            inbox,
            app_input,
            app_output,
            client_output,
        }
    }

    fn client(&mut self, id: u64, method: &str, params: Value) {
        self.core.handle(CoreMsg::Conn(ConnEvent::Message {
            conn: 1,
            message: Ok(proto::request(json!(id), method, Some(params))),
        }));
    }

    async fn outgoing(&mut self) -> Value {
        let line = tokio::time::timeout(Duration::from_secs(5), self.app_output.recv())
            .await
            .expect("等待服务器请求超时")
            .expect("服务器请求通道已关闭");
        serde_json::from_str(&line).unwrap()
    }

    async fn incoming_client(&mut self) -> Value {
        let command = tokio::time::timeout(Duration::from_secs(5), self.client_output.recv())
            .await
            .expect("等待客户端消息超时")
            .expect("客户端通道已关闭");
        let WriterCmd::Line(line) = command else {
            panic!("应收到消息而非关闭指令")
        };
        serde_json::from_str(&line).unwrap()
    }

    async fn pump_until(&mut self, predicate: impl Fn(&CoreMsg) -> bool) {
        loop {
            let message = tokio::time::timeout(Duration::from_secs(5), self.inbox.recv())
                .await
                .expect("等待 core 邮箱超时")
                .expect("core 邮箱已关闭");
            let done = predicate(&message);
            self.core.handle(message);
            if done {
                break;
            }
        }
    }

    async fn answer(&mut self, request: &Value, result: Result<Value, Value>) {
        let message = match result {
            Ok(value) => proto::response_ok(request["id"].clone(), value),
            Err(error) => proto::response_err(request["id"].clone(), error),
        };
        self.app_input.send(message.to_string()).unwrap();
        self.pump_until(|message| matches!(message, CoreMsg::App(AppEvent::Reply { .. })))
            .await;
    }

    async fn notification(&mut self, method: &str, params: Value) {
        self.app_input
            .send(proto::notification(method, Some(params)).to_string())
            .unwrap();
        self.pump_until(|message| matches!(message, CoreMsg::App(AppEvent::Notification { method: got, .. }) if got == method)).await;
    }

    async fn server_request(&mut self, id: Value, method: &str, params: Value) {
        self.app_input
            .send(proto::request(id, method, Some(params)).to_string())
            .unwrap();
        self.pump_until(|message| matches!(message, CoreMsg::App(AppEvent::ServerRequest { .. })))
            .await;
    }

    fn client_response(&mut self, conn: ConnId, response: Value) {
        self.core.handle(CoreMsg::Conn(ConnEvent::Message {
            conn,
            message: Ok(response),
        }));
    }

    async fn start_running(&mut self) {
        self.core
            .machine
            .records
            .entry("线程".into())
            .or_default()
            .subscribed = true;
        self.client(1, "turn/start", json!({"threadId": "线程"}));
        let start = self.outgoing().await;
        self.answer(&start, Ok(json!({"turn": {"id": "本轮"}})))
            .await;
        assert_eq!(self.incoming_client().await["id"], 1);
    }
}

#[tokio::test]
async fn 核心所有服务端请求类别转发往返并保留参数与原编号() {
    let mut harness = Harness::new(false);
    harness.start_running().await;
    for (index, method) in [
        "item/commandExecution/requestApproval",
        "item/fileChange/requestApproval",
        "item/tool/call",
        "item/tool/requestUserInput",
        "item/permissions/requestApproval",
        "mcpServer/elicitation/request",
        "未来/方法",
    ]
    .into_iter()
    .enumerate()
    {
        let original_id = json!(format!("服务器请求{index}"));
        let params = json!({"threadId": "线程", "turnId": "本轮", "嵌套": {"数据": [null, 1]}});
        harness
            .server_request(original_id.clone(), method, params.clone())
            .await;
        let forwarded = harness.incoming_client().await;
        assert_eq!(forwarded["method"], method);
        assert_eq!(forwarded["params"], params);
        assert!(forwarded["id"].as_str().unwrap().starts_with("broker-"));
        assert_eq!(harness.core.forward_timers.len(), 1);
        let result = json!({"answers": {}, "类别": index});
        harness.client_response(
            1,
            proto::response_ok(forwarded["id"].clone(), result.clone()),
        );
        assert_eq!(
            harness.outgoing().await,
            proto::response_ok(original_id, result)
        );
        assert!(harness.core.machine.requests.pending.is_empty());
        assert!(harness.core.forward_timers.is_empty());
    }
}

#[tokio::test]
async fn 核心启动回包未到时审批回包仍能通过且错误数据透传() {
    let mut harness = Harness::new(false);
    harness
        .core
        .machine
        .records
        .entry("线程".into())
        .or_default()
        .subscribed = true;
    harness.client(1, "turn/start", json!({"threadId": "线程"}));
    let start = harness.outgoing().await;
    harness
        .server_request(
            json!(91),
            "item/commandExecution/requestApproval",
            json!({"threadId": "线程"}),
        )
        .await;
    let forwarded = harness.incoming_client().await;
    let error = proto::error_object(-1234, "用户拒绝", Some(json!({"原始数据": null})));
    harness.client_response(
        1,
        proto::response_err(forwarded["id"].clone(), error.clone()),
    );
    assert_eq!(
        harness.outgoing().await,
        proto::response_err(json!(91), error)
    );
    assert_eq!(harness.core.pending_app.len(), 1);
    harness
        .answer(&start, Ok(json!({"turn": {"id": "本轮"}})))
        .await;
    assert_eq!(harness.incoming_client().await["id"], 1);
}

#[tokio::test]
async fn 核心非原连接回包不消费请求且空结果优先于错误() {
    let mut harness = Harness::new(false);
    harness.start_running().await;
    let (writer, _other_output) = mpsc::unbounded_channel();
    harness
        .core
        .handle(CoreMsg::Conn(ConnEvent::Connected { conn: 2, writer }));
    harness
        .server_request(
            json!(91),
            "item/tool/requestUserInput",
            json!({"threadId": "线程"}),
        )
        .await;
    let forwarded = harness.incoming_client().await;
    harness.client_response(
        2,
        proto::response_ok(forwarded["id"].clone(), json!({"越权": true})),
    );
    assert_eq!(harness.core.machine.requests.pending.len(), 1);
    assert_eq!(harness.core.forward_timers.len(), 1);
    harness.client_response(
        1,
        json!({"id": forwarded["id"], "result": null, "error": {"code": -1, "message": "忽略"}}),
    );
    assert_eq!(
        harness.outgoing().await,
        proto::response_ok(json!(91), Value::Null)
    );
    assert!(harness.core.forward_timers.is_empty());
}

#[tokio::test]
async fn 核心残缺回包返回上游错误且取消转发计时器() {
    let mut harness = Harness::new(false);
    harness.start_running().await;
    harness
        .server_request(
            json!(91),
            "item/permissions/requestApproval",
            json!({"threadId": "线程"}),
        )
        .await;
    let forwarded = harness.incoming_client().await;
    harness.client_response(1, json!({"id": forwarded["id"]}));
    assert_eq!(
        harness.outgoing().await,
        proto::response_err(
            json!(91),
            crate::requests::handler_error(
                "Malformed forwarded response: missing both 'result' and 'error'"
            )
        )
    );
    assert!(harness.core.forward_timers.is_empty());
}

#[tokio::test]
async fn 核心断开撤销只影响该连接并先拒绝审批再发目标处理再中断() {
    let mut harness = Harness::new(false);
    harness.start_running().await;
    let (writer, mut other_output) = mpsc::unbounded_channel();
    harness
        .core
        .handle(CoreMsg::Conn(ConnEvent::Connected { conn: 2, writer }));
    let mut other_claim = harness.core.machine.claims["线程"].clone();
    other_claim.claim_id = 2;
    other_claim.state = State::Running {
        owner: crate::claim::Owner::Client(2),
        turn_id: "其他轮".into(),
        joined: false,
    };
    harness
        .core
        .machine
        .claims
        .insert("其他线程".into(), other_claim);
    harness
        .server_request(
            json!(91),
            "item/commandExecution/requestApproval",
            json!({"threadId": "线程"}),
        )
        .await;
    let first = harness.incoming_client().await;
    harness
        .server_request(
            json!(92),
            "item/commandExecution/requestApproval",
            json!({"threadId": "其他线程"}),
        )
        .await;
    let WriterCmd::Line(line) = other_output.recv().await.unwrap() else {
        panic!("应收到转发")
    };
    let second: Value = serde_json::from_str(&line).unwrap();
    harness.core.handle(CoreMsg::Conn(ConnEvent::Closed {
        conn: 1,
        reason: CloseReason::Eof,
    }));
    assert_eq!(
        harness.outgoing().await,
        proto::response_err(
            json!(91),
            crate::requests::handler_error("Client disconnected while awaiting approval response")
        )
    );
    let goal = harness.outgoing().await;
    assert_eq!(goal["method"], "thread/goal/get");
    harness.answer(&goal, Ok(json!({"goal":null}))).await;
    assert_eq!(harness.outgoing().await["method"], "turn/interrupt");
    assert_eq!(harness.core.machine.requests.pending.len(), 1);
    assert_eq!(harness.core.forward_timers.len(), 1);
    assert!(!harness
        .core
        .machine
        .requests
        .pending
        .contains_key(first["id"].as_str().unwrap()));
    harness.client_response(
        2,
        proto::response_ok(second["id"].clone(), json!({"decision": "accept"})),
    );
    assert_eq!(
        harness.outgoing().await,
        proto::response_ok(json!(92), json!({"decision": "accept"}))
    );
    assert!(harness.core.forward_timers.is_empty());
}

#[tokio::test(start_paused = true)]
async fn 核心一小时转发计时器实际入箱并拒绝超时请求() {
    let mut harness = Harness::new(false);
    harness.start_running().await;
    harness
        .server_request(
            json!(91),
            "item/commandExecution/requestApproval",
            json!({"threadId": "线程"}),
        )
        .await;
    let forwarded = harness.incoming_client().await;
    // 先让计时任务登记单调时钟截止点，再推进虚拟时钟。
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(3599)).await;
    assert_eq!(harness.core.machine.requests.pending.len(), 1);
    assert!(harness.inbox.try_recv().is_err());
    tokio::time::advance(Duration::from_secs(1)).await;
    harness
        .pump_until(|message| matches!(message, CoreMsg::ForwardTimer { .. }))
        .await;
    assert_eq!(
        harness.outgoing().await,
        proto::response_err(
            json!(91),
            crate::requests::handler_error("Request forwarding timed out")
        )
    );
    assert!(harness.core.forward_timers.is_empty());
    harness.client_response(1, proto::response_ok(forwarded["id"].clone(), json!({})));
    assert!(harness.core.machine.requests.pending.is_empty());
}

#[tokio::test]
async fn 核心挂起转发独立阻止空闲退出与条件关闭() {
    let mut harness = Harness::new(false);
    harness.start_running().await;
    harness
        .server_request(
            json!(91),
            "item/commandExecution/requestApproval",
            json!({"threadId": "线程"}),
        )
        .await;
    let forwarded = harness.incoming_client().await;
    // 隔离空闲判定的三个来源，证明转发表本身也算忙。
    harness.core.machine.claims.clear();
    assert!(harness.core.pending_app.is_empty());
    harness.core.on_idle_deadline();
    assert_eq!(harness.core.phase, Phase::Running);
    harness.client(2, "broker/shutdown", json!({"ifIdle": true}));
    assert_eq!(harness.incoming_client().await["error"]["code"], -32001);
    harness.client_response(1, proto::response_ok(forwarded["id"].clone(), json!({})));
    assert_eq!(harness.outgoing().await["id"], 91);
    assert!(!harness.core.is_busy());
}

#[tokio::test]
async fn 核心关闭先写转发拒绝再关闭服务器并回收计时器() {
    let mut harness = Harness::new(false);
    harness.start_running().await;
    harness
        .server_request(
            json!(91),
            "item/commandExecution/requestApproval",
            json!({"threadId": "线程"}),
        )
        .await;
    harness.incoming_client().await;
    harness.core.begin_shutdown(0);
    assert_eq!(
        harness.outgoing().await,
        proto::response_err(
            json!(91),
            crate::requests::handler_error("Broker shutting down")
        )
    );
    assert!(harness.core.forward_timers.is_empty());
    assert!(harness.core.machine.requests.pending.is_empty());
    assert!(harness.core.machine.claims.is_empty());
}

#[tokio::test]
async fn 核心外部解决退役转发且迟到回包不会阻塞下一请求() {
    let mut harness = Harness::new(false);
    harness.start_running().await;
    harness
        .server_request(
            json!(91),
            "item/commandExecution/requestApproval",
            json!({"threadId": "线程"}),
        )
        .await;
    let forwarded = harness.incoming_client().await;
    harness
        .notification(
            "serverRequest/resolved",
            json!({"threadId": "线程", "requestId": "91"}),
        )
        .await;
    assert_eq!(
        harness.incoming_client().await["method"],
        "serverRequest/resolved"
    );
    assert!(harness.core.forward_timers.is_empty());
    harness.client_response(
        1,
        proto::response_ok(forwarded["id"].clone(), json!({"迟到": true})),
    );
    harness
        .server_request(
            json!(92),
            "item/tool/requestUserInput",
            json!({"threadId": "线程"}),
        )
        .await;
    let next = harness.incoming_client().await;
    harness.client_response(
        1,
        proto::response_ok(next["id"].clone(), json!({"answers": {}})),
    );
    assert_eq!(
        harness.outgoing().await,
        proto::response_ok(json!(92), json!({"answers": {}}))
    );
    assert!(harness.app_output.try_recv().is_err());
}

#[tokio::test]
async fn 核心真实恢复屏障阻止提前启动并拒绝空闲关闭() {
    let mut harness = Harness::new(false);
    harness.client(1, "turn/start", json!({"threadId": "线程"}));
    let resume = harness.outgoing().await;
    assert_eq!(resume["method"], "thread/resume");
    assert_eq!(resume["params"], json!({"threadId": "线程"}));
    assert!(harness.app_output.try_recv().is_err());
    harness.client(2, "turn/start", json!({"threadId": "线程"}));
    assert_eq!(harness.incoming_client().await["error"]["code"], -32001);
    harness.client(3, "broker/shutdown", json!({"ifIdle": true}));
    assert_eq!(harness.incoming_client().await["error"]["code"], -32001);
    harness
        .answer(&resume, Err(proto::error_object(-32000, "恢复失败", None)))
        .await;
    assert_eq!(harness.outgoing().await["method"], "turn/start");
}

#[tokio::test]
async fn 核心完成后执行下一拍退订并再次启动前恢复() {
    let mut harness = Harness::new(false);
    harness.start_running().await;
    harness
        .notification(
            "turn/completed",
            json!({"threadId": "线程", "turn": {"id": "本轮"}}),
        )
        .await;
    assert_eq!(harness.incoming_client().await["method"], "turn/completed");
    harness
        .pump_until(|message| {
            matches!(
                message,
                CoreMsg::ClaimTimer {
                    kind: TimerKind::Unsubscribe,
                    ..
                }
            )
        })
        .await;
    let unsubscribe = harness.outgoing().await;
    assert_eq!(unsubscribe["method"], "thread/unsubscribe");
    harness.answer(&unsubscribe, Ok(json!({}))).await;
    harness.client(2, "turn/start", json!({"threadId": "线程"}));
    let resume = harness.outgoing().await;
    assert_eq!(resume["method"], "thread/resume");
    harness
        .answer(&resume, Ok(json!({"thread": {"id": "线程"}})))
        .await;
    assert_eq!(harness.outgoing().await["method"], "turn/start");
}

#[tokio::test(start_paused = true)]
async fn 核心吸收判定实际读服务器且计时器会再次发读请求() {
    let mut harness = Harness::new(true);
    harness
        .core
        .machine
        .records
        .entry("线程".into())
        .or_default()
        .subscribed = true;
    harness.client(1, "turn/start", json!({"threadId": "线程"}));
    let start = harness.outgoing().await;
    harness
        .answer(&start, Ok(json!({"turn": {"id": "本轮"}})))
        .await;
    let read = harness.outgoing().await;
    assert_eq!(read["method"], "thread/read");
    assert_eq!(
        read["params"],
        json!({"threadId": "线程", "includeTurns": true})
    );
    harness
        .answer(&read, Err(proto::error_object(-32000, "读失败", None)))
        .await;
    let response = harness.incoming_client().await;
    assert!(!response["result"]
        .as_object()
        .unwrap()
        .contains_key("absorbedBy"));
    let token = harness.core.machine.claims["线程"].claim_id;
    tokio::time::advance(Duration::from_secs(1)).await;
    harness
        .pump_until(|message| {
            matches!(message, CoreMsg::ClaimTimer {
        token: ClaimToken { claim_id, .. }, kind: TimerKind::SettlementRetry { attempt: 1 }
    } if *claim_id == token)
        })
        .await;
    let retry = harness.outgoing().await;
    assert_eq!(retry["method"], "thread/read");
    harness
        .notification(
            "turn/started",
            json!({"threadId": "线程", "turn": {"id": "本轮"}}),
        )
        .await;
    assert!(!harness.core.machine.claims["线程"].settling());
    harness
        .answer(&retry, Err(proto::error_object(-32000, "迟到读失败", None)))
        .await;
    assert!(!harness.core.machine.claims["线程"].joined());
}

#[tokio::test]
async fn 核心断开时未知轮次在启动回包后中断且回包不释放() {
    let mut harness = Harness::new(false);
    harness
        .core
        .machine
        .records
        .entry("线程".into())
        .or_default()
        .subscribed = true;
    harness.client(1, "turn/start", json!({"threadId": "线程"}));
    let start = harness.outgoing().await;
    harness.core.handle(CoreMsg::Conn(ConnEvent::Closed {
        conn: 1,
        reason: CloseReason::Eof,
    }));
    let goal = harness.outgoing().await;
    assert_eq!(goal["method"], "thread/goal/get");
    harness.answer(&goal, Ok(json!({"goal":null}))).await;
    harness
        .answer(&start, Ok(json!({"turn": {"id": "本轮"}})))
        .await;
    let interrupt = harness.outgoing().await;
    assert_eq!(interrupt["method"], "turn/interrupt");
    assert_eq!(
        interrupt["params"],
        json!({"threadId": "线程", "turnId": "本轮"})
    );
    harness.answer(&interrupt, Ok(json!({}))).await;
    assert!(harness.core.machine.claims.contains_key("线程"));
    harness.core.on_idle_deadline();
    assert_eq!(harness.core.phase, Phase::Running);
    harness
        .notification(
            "turn/completed",
            json!({"threadId": "线程", "turn": {"id": "本轮"}}),
        )
        .await;
    assert!(harness.core.machine.claims.is_empty());
}

#[tokio::test]
async fn 核心旧认领中断回包不会释放新认领() {
    let mut harness = Harness::new(false);
    harness.start_running().await;
    let token = ClaimToken {
        thread_id: "线程".into(),
        claim_id: harness.core.machine.claims["线程"].claim_id,
    };
    harness.core.forward_request(RequestContext {
        resume_retried: false,
        stream_id: None,
        review_id: None,
        client: None,
        token: Some(token),
        operation: Operation::Forward,
        method: "turn/interrupt".into(),
        params: Some(json!({"threadId": "线程", "turnId": "本轮"})),
    });
    let interrupt = harness.outgoing().await;
    harness
        .notification(
            "turn/completed",
            json!({"threadId": "线程", "turn": {"id": "本轮"}}),
        )
        .await;
    harness.client(2, "turn/start", json!({"threadId": "线程"}));
    let new = harness.core.machine.claims["线程"].clone();
    harness.answer(&interrupt, Ok(json!({}))).await;
    assert_eq!(harness.core.machine.claims["线程"], new);
}

#[tokio::test]
async fn 核心有认领但没有在途请求仍阻止空闲退出() {
    let mut harness = Harness::new(false);
    harness.start_running().await;
    assert!(harness.core.pending_app.is_empty());
    assert!(matches!(
        harness.core.machine.claims["线程"].state,
        State::Running { .. }
    ));
    harness.core.on_idle_deadline();
    assert_eq!(harness.core.phase, Phase::Running);
}

#[tokio::test]
async fn 核心按服务器写出顺序处理先宣布再失败的启动() {
    // app-server 先写 turn/started、紧接着写失败回包：两者必须按这个顺序进 core（1.2 节），
    // 否则失败回包先到会把认领当成「没学到轮次」释放掉，正在跑的 turn 没人中断（#11，测试 63）。
    for _ in 0..20 {
        let mut harness = Harness::new(false);
        harness
            .core
            .machine
            .records
            .entry("线程".into())
            .or_default()
            .subscribed = true;
        harness.client(1, "turn/start", json!({"threadId": "线程"}));
        let start = harness.outgoing().await;
        harness
            .app_input
            .send(
                proto::notification(
                    "turn/started",
                    Some(json!({"threadId": "线程", "turn": {"id": "本轮"}})),
                )
                .to_string(),
            )
            .unwrap();
        harness
            .app_input
            .send(
                proto::response_err(
                    start["id"].clone(),
                    proto::error_object(-32000, "启动失败", None),
                )
                .to_string(),
            )
            .unwrap();
        harness
            .pump_until(|message| matches!(message, CoreMsg::App(AppEvent::Reply { .. })))
            .await;
        let goal = harness.outgoing().await;
        assert_eq!(goal["method"], "thread/goal/get");
        harness.answer(&goal, Ok(json!({"goal":null}))).await;
        let interrupt = harness.outgoing().await;
        assert_eq!(interrupt["method"], "turn/interrupt");
        assert_eq!(
            interrupt["params"],
            json!({"threadId": "线程", "turnId": "本轮"})
        );
        assert!(harness.core.machine.claims.contains_key("线程"));
    }
}

#[tokio::test]
async fn 核心在连接关闭后发出的请求也会得到失败回包() {
    let mut harness = Harness::new(false);
    harness.core.app.inner.fail("测试关闭");
    // core 还没处理到 Closed 事件时，客户端请求已经进来：发送立即失败，失败回包投回邮箱。
    harness.client(7, "thread/read", json!({"threadId": "线程"}));
    assert_eq!(harness.core.pending_app.len(), 1);
    // 只处理这条失败回包；Closed 会触发关闭流程、断开测试客户端，这里跳过它。
    loop {
        let message = tokio::time::timeout(Duration::from_secs(5), harness.inbox.recv())
            .await
            .expect("等待 core 邮箱超时")
            .expect("core 邮箱已关闭");
        if matches!(message, CoreMsg::App(AppEvent::Reply { .. })) {
            harness.core.handle(message);
            break;
        }
    }
    assert!(harness.core.pending_app.is_empty());
    let answer = harness.incoming_client().await;
    assert_eq!(answer["id"], 7);
    assert_eq!(answer["error"]["code"], -32000);
}

#[tokio::test]
async fn 核心失败回包先于关闭事件时请求结算一次并回给客户端() {
    let mut harness = Harness::new(false);
    harness.core.app.inner.fail("测试关闭");
    // 发送立即失败，失败回包同步投进邮箱；Closed 要等转发任务被调度，所以一定排在后面。
    harness.client(7, "thread/read", json!({"threadId": "线程"}));
    assert_eq!(harness.core.pending_app.len(), 1);
    let first = harness.inbox.recv().await.unwrap();
    assert!(matches!(first, CoreMsg::App(AppEvent::Reply { .. })));
    harness.core.handle(first);
    assert!(harness.core.pending_app.is_empty());
    let answer = harness.incoming_client().await;
    assert_eq!(answer["id"], 7);
    assert_eq!(answer["error"]["code"], -32000);
    harness
        .pump_until(|message| matches!(message, CoreMsg::App(AppEvent::Closed { .. })))
        .await;
    assert!(harness.core.machine.claims.is_empty());
    // 之后客户端只会收到关闭指令，不会再收到第二个回包。
    while let Ok(command) = harness.client_output.try_recv() {
        assert!(matches!(command, WriterCmd::Close { .. }), "不应再有回包");
    }
}

#[tokio::test]
async fn 核心关闭事件先于失败回包时不重建认领且不再回包() {
    let mut harness = Harness::new(false);
    harness.core.app.inner.fail("测试关闭");
    // 先把 Closed 取到手但暂不处理，再发请求：失败回包排在 Closed 之后被处理。
    let closed = tokio::time::timeout(Duration::from_secs(5), harness.inbox.recv())
        .await
        .expect("等待关闭事件超时")
        .expect("core 邮箱已关闭");
    assert!(matches!(closed, CoreMsg::App(AppEvent::Closed { .. })));
    harness.client(7, "thread/read", json!({"threadId": "线程"}));
    harness.client(8, "turn/start", json!({"threadId": "线程"}));
    assert_eq!(harness.core.pending_app.len(), 2);
    assert!(harness.core.machine.claims.contains_key("线程"));
    harness.core.handle(closed);
    assert!(harness.core.machine.claims.is_empty());
    // 两个失败回包逐个处理：各结算一次，不因 resume 失败而重新开工。
    for _ in 0..2 {
        harness
            .pump_until(|message| matches!(message, CoreMsg::App(AppEvent::Reply { .. })))
            .await;
    }
    assert!(harness.core.pending_app.is_empty());
    assert!(harness.core.machine.claims.is_empty());
    // 关闭清理时，还在等恢复订阅、尚未发出的 turn/start（id 8）先收到一条「正在关闭」，
    // 随后连接收到关闭指令；thread/read（id 7）的失败回包到达时连接已断开，不再回包。
    let mut lines = Vec::new();
    let mut closes = 0;
    while let Ok(command) = harness.client_output.try_recv() {
        match command {
            WriterCmd::Close { .. } => closes += 1,
            WriterCmd::Line(line) => {
                assert_eq!(closes, 0, "关闭指令之后不应再回包：{line}");
                lines.push(serde_json::from_str::<Value>(&line).unwrap());
            }
        }
    }
    assert_eq!(
        lines,
        vec![proto::response_err(
            json!(8),
            proto::error_object(code::GENERAL_ERROR, claim::SHUTTING_DOWN, None)
        )]
    );
    assert_eq!(closes, 1);
}

#[tokio::test(start_paused = true)]
async fn 核心停止四十秒与十五秒核实按操作代号处理() {
    let mut h = Harness::new(false);
    h.start_running().await;
    h.client(2, "broker/threads/stop", json!({"threadId":"线程"}));
    let generation = h
        .core
        .machine
        .stops
        .operations
        .values()
        .next()
        .unwrap()
        .generation;
    let get = h.outgoing().await;
    assert_eq!(get["method"], "thread/goal/get");
    h.answer(&get, Ok(json!({"goal":null}))).await;
    let interrupt = h.outgoing().await;
    h.answer(&interrupt, Ok(json!({}))).await;
    assert_eq!(
        h.core
            .machine
            .stops
            .operations
            .values()
            .next()
            .unwrap()
            .outcome,
        crate::stop::Outcome::Pending
    );
    tokio::time::sleep(Duration::from_secs(39)).await;
    assert!(h.client_output.try_recv().is_err());
    assert!(h.inbox.try_recv().is_err());
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut deadline = false;
    let mut waiter = false;
    while !(deadline && waiter) {
        let message = h.inbox.recv().await.unwrap();
        if let CoreMsg::ClaimTimer {
            kind: TimerKind::Stop(tag, kind),
            ..
        } = &message
        {
            if tag.generation == generation {
                match kind {
                    crate::stop::StopTimer::Deadline => deadline = true,
                    crate::stop::StopTimer::Waiter(_) => waiter = true,
                    _ => {}
                }
            }
        }
        h.core.handle(message);
    }
    assert_eq!(
        h.incoming_client().await["result"]["stopOutcome"],
        "unconfirmed"
    );
    assert!(h.core.machine.claims.contains_key("线程"));
    tokio::time::sleep(Duration::from_secs(14)).await;
    assert!(h.inbox.try_recv().is_err());
    tokio::time::sleep(Duration::from_secs(1)).await;
    loop {
        let message = h.inbox.recv().await.unwrap();
        let done = matches!(&message,CoreMsg::ClaimTimer{kind:TimerKind::Stop(tag,crate::stop::StopTimer::Verify),..} if tag.generation==generation);
        h.core.handle(message);
        if done {
            break;
        }
    }
    let read = h.outgoing().await;
    let goal = h.outgoing().await;
    assert_eq!(read["method"], "thread/read");
    assert_eq!(goal["method"], "thread/goal/get");
    h.answer(&read, Ok(json!({"thread":{"turns":[]}}))).await;
    assert!(h.core.machine.claims.contains_key("线程"));
    h.answer(&goal, Ok(json!({"goal":null}))).await;
    assert!(h.core.machine.claims.is_empty());
    assert!(h.core.machine.stops.operations.is_empty());
    assert!(!h.core.machine.stops_busy());
}

#[tokio::test(start_paused = true)]
async fn 核心重复停止各等四十秒不重置原核实计划() {
    let mut h = Harness::new(false);
    h.start_running().await;
    h.client(2, "broker/threads/stop", json!({"threadId":"线程"}));
    let get = h.outgoing().await;
    h.answer(&get, Ok(json!({"goal":null}))).await;
    let interrupt = h.outgoing().await;
    h.answer(&interrupt, Ok(json!({}))).await;
    let generation = h
        .core
        .machine
        .stops
        .operations
        .values()
        .next()
        .unwrap()
        .generation;
    tokio::time::sleep(Duration::from_secs(10)).await;
    h.client(3, "broker/threads/stop", json!({"threadId":"线程"}));
    let interrupt = h.outgoing().await;
    h.answer(&interrupt, Ok(json!({}))).await;
    tokio::time::sleep(Duration::from_secs(30)).await;
    loop {
        let msg = h.inbox.recv().await.unwrap();
        let done = matches!(&msg,CoreMsg::ClaimTimer{kind:TimerKind::Stop(tag,crate::stop::StopTimer::Waiter( _)),..} if tag.generation==generation);
        h.core.handle(msg);
        if done {
            break;
        }
    }
    let first = h.incoming_client().await;
    assert_eq!(first["id"], 2);
    assert_eq!(first["result"]["stopOutcome"], "unconfirmed");
    assert!(h.client_output.try_recv().is_err());
    tokio::time::sleep(Duration::from_secs(10)).await;
    loop {
        let msg = h.inbox.recv().await.unwrap();
        let done = matches!(&msg,CoreMsg::ClaimTimer{kind:TimerKind::Stop(tag,crate::stop::StopTimer::Waiter(_)),..} if tag.generation==generation);
        h.core.handle(msg);
        if done {
            break;
        }
    }
    assert_eq!(h.incoming_client().await["id"], 3);
    assert_eq!(
        h.core
            .machine
            .stops
            .operations
            .values()
            .next()
            .unwrap()
            .generation,
        generation
    );
}

#[tokio::test]
async fn 核心只有未确认停止操作时条件关闭仍回忙() {
    let mut h = Harness::new(false);
    h.start_running().await;
    h.client(2, "broker/threads/stop", json!({"threadId":"线程"}));
    let get = h.outgoing().await;
    h.answer(&get, Ok(json!({"goal":null}))).await;
    let interrupt = h.outgoing().await;
    h.answer(&interrupt, Ok(json!({}))).await;
    h.core.machine.claims.clear();
    assert!(h.core.pending_app.is_empty());
    h.client(3, "broker/shutdown", json!({"ifIdle":true}));
    assert_eq!(h.incoming_client().await["error"]["code"], -32001);
    assert_eq!(h.core.phase, Phase::Running);
}

#[tokio::test(start_paused = true)]
async fn 核心启动超时对账与迟到回包真实经过有序通道() {
    let mut h = Harness::new(false);
    h.core
        .machine
        .records
        .entry("线程".into())
        .or_default()
        .subscribed = true;
    h.client(1, "turn/start", json!({"threadId":"线程"}));
    let start = h.outgoing().await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    loop {
        let msg = h.inbox.recv().await.unwrap();
        let done = matches!(
            &msg,
            CoreMsg::App(AppEvent::TimedOut {
                error: AppError::Timeout { .. },
                ..
            })
        );
        h.core.handle(msg);
        if done {
            break;
        }
    }
    assert!(h.incoming_client().await.get("error").is_some());
    let read = h.outgoing().await;
    assert_eq!(read["method"], "thread/read");
    h.answer(&read, Ok(json!({"thread":{"turns":[]}}))).await;
    assert!(h.core.machine.claims.is_empty());
    h.pump_until(|msg| {
        matches!(
            msg,
            CoreMsg::ClaimTimer {
                kind: TimerKind::Unsubscribe,
                ..
            }
        )
    })
    .await;
    let unsubscribe = h.outgoing().await;
    assert_eq!(unsubscribe["method"], "thread/unsubscribe");
    h.answer(&unsubscribe, Ok(json!({}))).await;
    h.app_input
        .send(proto::response_ok(start["id"].clone(), json!({"turn":{"id":"迟到轮"}})).to_string())
        .unwrap();
    loop {
        let msg = h.inbox.recv().await.unwrap();
        let done = matches!(&msg, CoreMsg::App(AppEvent::LateReply { .. }));
        h.core.handle(msg);
        if done {
            break;
        }
    }
    let goal = h.outgoing().await;
    assert_eq!(goal["method"], "thread/goal/get");
    h.answer(&goal, Ok(json!({"goal":null}))).await;
    let interrupt = h.outgoing().await;
    assert_eq!(interrupt["method"], "turn/interrupt");
    assert_eq!(interrupt["params"]["turnId"], "迟到轮");
    assert!(h.client_output.try_recv().is_err());
}

#[tokio::test(start_paused = true)]
async fn 核心延迟处理超时仍使用原截止且到期同步清理审查计数() {
    let mut h = Harness::new(false);
    h.core
        .machine
        .records
        .entry("线程".into())
        .or_default()
        .subscribed = true;
    h.client(
        1,
        "review/start",
        json!({"threadId":"线程","delivery":"detached"}),
    );
    h.outgoing().await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let message = loop {
        let msg = h.inbox.recv().await.unwrap();
        if matches!(msg, CoreMsg::App(AppEvent::TimedOut { .. })) {
            break msg;
        }
        h.core.handle(msg);
    };
    let (tag, deadline) = match &message {
        CoreMsg::App(AppEvent::TimedOut { tag, deadline, .. }) => (*tag, *deadline),
        _ => unreachable!(),
    };
    tokio::time::sleep(Duration::from_secs(20)).await;
    h.core.handle(message);
    assert_eq!(h.core.late_app.front().unwrap().2.into_std(), deadline);
    assert_eq!(
        h.core
            .machine
            .stops
            .reconciliations
            .values()
            .next()
            .unwrap()
            .deadline,
        deadline
    );
    let read = h.outgoing().await;
    h.answer(&read, Ok(json!({"thread":{"turns":[]}}))).await;
    h.notification(
        "turn/started",
        json!({"threadId":"子线程","turn":{"id":"子轮"}}),
    )
    .await;
    assert_eq!(h.core.machine.pending_reviews, 1);
    assert!(!h.core.machine.unclaimed.is_empty());
    tokio::time::sleep_until(Instant::from_std(deadline)).await;
    loop {
        let msg = h.inbox.recv().await.unwrap();
        let done = matches!(&msg,CoreMsg::App(AppEvent::LateDiscarded {tag:got}) if *got==tag);
        h.core.handle(msg);
        if done {
            break;
        }
    }
    assert!(h.core.late_app.iter().all(|(got, ..)| *got != tag));
    assert!(h.core.machine.stops.reconciliations.is_empty());
    assert_eq!(h.core.machine.pending_reviews, 0);
    assert!(h.core.machine.unclaimed.is_empty());
}

#[tokio::test]
async fn 核心审查冲突操作不锁别的认领完成后条件关闭成功() {
    let mut h = Harness::new(false);
    h.core
        .machine
        .records
        .entry("线程".into())
        .or_default()
        .subscribed = true;
    h.client(
        1,
        "review/start",
        json!({"threadId":"线程","delivery":"detached"}),
    );
    let review = h.outgoing().await;
    let mut child = h.core.machine.claims["线程"].clone();
    child.claim_id = 99;
    child.run_id = Some("其他运行".into());
    child.request_pending = false;
    child.pending_start = None;
    child.state = State::Running {
        owner: crate::claim::Owner::Client(1),
        turn_id: "别的轮".into(),
        joined: false,
    };
    h.core.machine.claims.insert("子线程".into(), child);
    // 不制造额外订阅检查，使条件关闭只观察当前认领与冲突收尾操作。
    h.core.machine.records.get_mut("线程").unwrap().subscribed = false;
    h.client(2, "broker/threads/stop", json!({"threadId":"线程"}));
    h.answer(
        &review,
        Ok(json!({"reviewThreadId":"子线程","turn":{"id":"审查轮"}})),
    )
    .await;
    assert!(!h.core.machine.stop_busy("子线程"));
    assert!(!h.core.machine.stops_busy());
    h.notification(
        "turn/completed",
        json!({"threadId":"子线程","turn":{"id":"别的轮"}}),
    )
    .await;
    assert!(h.core.machine.claims.is_empty());
    assert!(h.core.pending_app.is_empty());
    h.client(3, "broker/shutdown", json!({"ifIdle":true}));
    let mut replies = HashMap::new();
    while replies.len() < 3 {
        let msg = h.incoming_client().await;
        if let Some(id) = msg["id"].as_u64() {
            replies.insert(id, msg);
        }
    }
    assert_eq!(replies[&3]["result"], json!({}));
    assert_eq!(replies[&2]["result"]["stopOutcome"], "unconfirmed");
    assert!(h.core.machine.stops.operations.is_empty());
}

#[tokio::test(start_paused = true)]
async fn 核心线回包已接收转发延迟且旧本地截止先到仍处理迟到回包() {
    // 保留 RPC 事件接收端，不启动 serve 的转发任务，明确控制两条入箱路径的交错。
    let (input, inbound) = mpsc::unbounded_channel();
    let (writer, mut output) = mpsc::unbounded_channel();
    let (inner, mut events) = rpc::new_client(inbound, writer, || async {}, Some(5000));
    let (mailbox, mut inbox) = mpsc::unbounded_channel();
    let mut core = Core {
        config: CoreConfig {
            idle_timeout: Duration::from_secs(900),
            server: ServerInfo::Private { pid: None },
        },
        app: AppHandle { inner },
        mailbox,
        accept: None,
        conns: HashMap::new(),
        machine: Machine::new(false),
        pending_app: HashMap::new(),
        late_app: std::collections::VecDeque::new(),
        forward_timers: HashMap::new(),
        probe_timer: None,
        store: None,
        store_terminal: false,
        loaded: Loaded::default(),
        epoch: 0,
        survives_parent_job: false,
        parent_job_evidence: Value::Null,
        progress_timers: HashMap::new(),
        keyed_timers: HashMap::new(),
        daily_timer: None,
        runs_deadline: None,
        shutdown_wait_timer: None,
        health: None,
        shutdown_stage: None,
        app_disconnected: false,
        fixed_clock: None,
        next_tag: 0,
        last_activity: Instant::now(),
        started: Instant::now(),
        phase: Phase::Running,
    };
    let (client_writer, _client_output) = mpsc::unbounded_channel();
    core.handle(CoreMsg::Conn(ConnEvent::Connected {
        conn: 1,
        writer: client_writer,
    }));
    core.machine
        .records
        .entry("线程".into())
        .or_default()
        .subscribed = true;
    core.on_client_request(
        1,
        json!(1),
        "turn/start".into(),
        Some(json!({"threadId":"线程"})),
    );
    let start: Value = serde_json::from_str(&output.recv().await.unwrap()).unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;
    let timeout = events.recv().await.unwrap();
    let (tag, deadline) = match &timeout {
        AppEvent::TimedOut { tag, deadline, .. } => (*tag, *deadline),
        other => panic!("预期超时事件：{other:?}"),
    };
    core.handle(CoreMsg::App(timeout));
    let registration = core
        .machine
        .stops
        .reconciliations
        .values()
        .next()
        .unwrap()
        .clone();
    let read: Value = serde_json::from_str(&output.recv().await.unwrap()).unwrap();
    input
        .send(proto::response_ok(read["id"].clone(), json!({"thread":{"turns":[]}})).to_string())
        .unwrap();
    core.handle(CoreMsg::App(events.recv().await.unwrap()));
    assert!(core.machine.claims.is_empty());
    input
        .send(proto::response_ok(start["id"].clone(), json!({"turn":{"id":"迟到轮"}})).to_string())
        .unwrap();
    input
        .send(proto::notification("接收屏障", None).to_string())
        .unwrap();
    let accepted = events.recv().await.unwrap();
    assert!(matches!(&accepted,AppEvent::LateReply {tag:got,..} if *got==tag));
    assert!(
        matches!(events.recv().await,Some(AppEvent::Notification {method,..}) if method=="接收屏障")
    );
    // RPC 已在截止前接收，但其转发仍被测试阻挡；独立旧计时器先进入 core。
    tokio::time::sleep_until(Instant::from_std(deadline)).await;
    core.handle(CoreMsg::ClaimTimer {
        token: registration.token,
        kind: TimerKind::LateExpiry {
            generation: registration.generation,
        },
    });
    assert_eq!(core.late_app.len(), 1);
    assert_eq!(core.machine.stops.late.len(), 1);
    assert!(events.try_recv().is_err());
    core.handle(CoreMsg::App(accepted));
    assert!(core.late_app.is_empty());
    let goal: Value = serde_json::from_str(&output.recv().await.unwrap()).unwrap();
    assert_eq!(goal["method"], "thread/goal/get");
    input
        .send(proto::response_ok(goal["id"].clone(), json!({"goal":null})).to_string())
        .unwrap();
    core.handle(CoreMsg::App(events.recv().await.unwrap()));
    let interrupt: Value = serde_json::from_str(&output.recv().await.unwrap()).unwrap();
    assert_eq!(interrupt["method"], "turn/interrupt");
    assert_eq!(interrupt["params"]["turnId"], "迟到轮");
    // 后续本地截止仅对已消费回包的定案/收尾有效，原等待阶段未登记本地截止。
    let queued: Vec<_> = std::iter::from_fn(|| inbox.try_recv().ok()).collect();
    assert!(queued
        .iter()
        .all(|msg| !matches!(msg,CoreMsg::App(AppEvent::LateDiscarded {tag:got}) if *got==tag)));
}

#[tokio::test(start_paused = true)]
async fn 核心普通审查超时迟到不提前结算另一审查且早到子线程完成释放() {
    let mut h = Harness::new(false);
    h.client(1, "review/start", json!({"delivery":"detached"}));
    let forwarded = h.outgoing().await;
    let forwarded_tag = h.core.next_tag;
    tokio::time::sleep(Duration::from_secs(4)).await;
    h.core
        .machine
        .records
        .entry("线程".into())
        .or_default()
        .subscribed = true;
    h.client(
        2,
        "review/start",
        json!({"threadId":"线程","delivery":"detached"}),
    );
    let normal = h.outgoing().await;
    assert_eq!(h.core.machine.pending_reviews, 2);
    tokio::time::sleep(Duration::from_secs(1)).await;
    h.pump_until(
        |msg| matches!(msg,CoreMsg::App(AppEvent::TimedOut {tag,..}) if *tag==forwarded_tag),
    )
    .await;
    assert_eq!(h.core.machine.pending_reviews, 1);
    assert_eq!(h.incoming_client().await["id"], 1);
    h.notification(
        "turn/completed",
        json!({"threadId":"子线程","turn":{"id":"子轮"}}),
    )
    .await;
    let held = h.core.machine.unclaimed["子线程"].clone();
    h.app_input
        .send(
            proto::response_err(
                forwarded["id"].clone(),
                proto::error_object(-32000, "审查失败", None),
            )
            .to_string(),
        )
        .unwrap();
    h.pump_until(
        |msg| matches!(msg,CoreMsg::App(AppEvent::LateReply {tag,..}) if *tag==forwarded_tag),
    )
    .await;
    assert_eq!(h.core.machine.pending_reviews, 1);
    assert_eq!(h.core.machine.unclaimed["子线程"], held);
    h.answer(
        &normal,
        Ok(json!({"reviewThreadId":"子线程","turn":{"id":"子轮"}})),
    )
    .await;
    assert_eq!(h.core.machine.pending_reviews, 0);
    assert!(h.core.machine.unclaimed.is_empty());
    assert!(h.core.machine.claims.is_empty());
}

#[tokio::test(start_paused = true)]
async fn 核心真实启动与对账双超时触发熔断错误回包立即恢复() {
    let mut h = Harness::new(false);
    h.core
        .machine
        .records
        .entry("线程".into())
        .or_default()
        .subscribed = true;
    h.client(1, "turn/start", json!({"threadId":"线程"}));
    h.outgoing().await;
    let start_tag = h.core.next_tag;
    tokio::time::sleep(Duration::from_secs(5)).await;
    h.pump_until(|msg| matches!(msg,CoreMsg::App(AppEvent::TimedOut {tag,..}) if *tag==start_tag))
        .await;
    assert!(!h.core.machine.app_server_unresponsive);
    assert_eq!(h.incoming_client().await["id"], 1);
    let read = h.outgoing().await;
    assert_eq!(read["method"], "thread/read");
    let read_tag = h.core.next_tag;
    tokio::time::sleep(Duration::from_secs(5)).await;
    h.pump_until(|msg| matches!(msg,CoreMsg::App(AppEvent::TimedOut {tag,..}) if *tag==read_tag))
        .await;
    assert!(h.core.machine.app_server_unresponsive);
    h.client(2, "turn/start", json!({"threadId":"新线程"}));
    let denied = h.incoming_client().await;
    assert_eq!(denied["error"]["code"], -32000);
    assert_eq!(denied["error"]["data"]["reason"], "appServerUnresponsive");
    h.client(3, "broker/threads/stop", json!({"threadId":"线程"}));
    let goal = h.outgoing().await;
    assert_eq!(goal["method"], "thread/goal/get");
    assert!(h.core.machine.app_server_unresponsive);
    h.client(4, "普通请求", json!({}));
    let ordinary = h.outgoing().await;
    assert_eq!(ordinary["method"], "普通请求");
    h.answer(
        &ordinary,
        Err(proto::error_object(-32000, "服务器已回应", None)),
    )
    .await;
    assert!(!h.core.machine.app_server_unresponsive);
    assert_eq!(h.incoming_client().await["id"], 4);
    h.core
        .machine
        .records
        .entry("新线程".into())
        .or_default()
        .subscribed = true;
    h.client(5, "turn/start", json!({"threadId":"新线程"}));
    assert_eq!(h.outgoing().await["method"], "turn/start");
}

#[tokio::test(start_paused = true)]
async fn 核心业务登记忘记或过期后真实回包仍恢复熔断且不重复交付() {
    for expired in [false, true] {
        for failed in [false, true] {
            let mut h = Harness::new(false);
            h.client(1, "普通请求", json!({}));
            let old = h.outgoing().await;
            let tag = h.core.next_tag;
            tokio::time::sleep(Duration::from_secs(5)).await;
            h.pump_until(
                |msg| matches!(msg,CoreMsg::App(AppEvent::TimedOut {tag:got,..}) if *got==tag),
            )
            .await;
            assert_eq!(h.incoming_client().await["id"], 1);
            if expired {
                tokio::time::sleep(Duration::from_millis(crate::stop::LATE_MS)).await;
                h.pump_until(|msg|matches!(msg,CoreMsg::App(AppEvent::LateDiscarded {tag:got}) if *got==tag)).await;
            } else {
                h.core.forget_late(tag);
            }
            assert!(h.core.late_app.is_empty());
            assert!(h.core.pending_app.is_empty());
            h.core.machine.app_server_unresponsive = true;
            h.core.dispatch(Event::Connected { conn: 1 });
            assert!(h.core.machine.stops.probe.scheduled);
            let message = if failed {
                proto::response_err(
                    old["id"].clone(),
                    proto::error_object(-32000, "服务器已回应", None),
                )
            } else {
                proto::response_ok(old["id"].clone(), json!({}))
            };
            h.app_input.send(message.to_string()).unwrap();
            h.pump_until(|msg| matches!(msg, CoreMsg::App(AppEvent::Responsive)))
                .await;
            assert!(!h.core.machine.app_server_unresponsive);
            assert!(!h.core.machine.stops.probe.scheduled);
            assert!(h.core.probe_timer.is_none());
            assert!(h.core.late_app.is_empty());
            assert!(h.core.pending_app.is_empty());
            assert!(h.client_output.try_recv().is_err());
        }
    }
}

#[tokio::test(start_paused = true)]
async fn 核心最后审查对账过期旧消息排空后独立试探恢复接单() {
    let mut h = Harness::new(false);
    h.core
        .machine
        .records
        .entry("线程".into())
        .or_default()
        .subscribed = true;
    h.client(
        1,
        "review/start",
        json!({"threadId":"线程","delivery":"detached"}),
    );
    h.outgoing().await;
    let original_tag = h.core.next_tag;
    tokio::time::sleep(Duration::from_secs(5)).await;
    h.pump_until(
        |msg| matches!(msg,CoreMsg::App(AppEvent::TimedOut {tag,..}) if *tag==original_tag),
    )
    .await;
    assert_eq!(h.incoming_client().await["id"], 1);
    let read = h.outgoing().await;
    assert_eq!(read["method"], "thread/read");
    let read_tag = h.core.next_tag;
    tokio::time::sleep(Duration::from_secs(5)).await;
    h.pump_until(|msg| matches!(msg,CoreMsg::App(AppEvent::TimedOut {tag,..}) if *tag==read_tag))
        .await;
    assert!(h.core.machine.app_server_unresponsive);
    let deadline = h
        .core
        .late_app
        .iter()
        .find(|(tag, ..)| *tag == original_tag)
        .unwrap()
        .2;
    h.core.machine.records.get_mut("线程").unwrap().subscribed = false;
    tokio::time::sleep_until(deadline).await;
    let mut held = Vec::new();
    loop {
        let msg = h.inbox.recv().await.unwrap();
        if matches!(&msg,CoreMsg::App(AppEvent::LateDiscarded {tag}) if *tag==original_tag) {
            h.core.handle(msg);
            break;
        }
        held.push(msg);
    }
    assert!(h.core.machine.stops.reconciliations.is_empty());
    assert!(h.core.machine.stream_slots.is_empty());
    assert_eq!(h.core.machine.pending_reviews, 0);
    assert!(h.core.machine.stops.probe.scheduled);
    assert!(h.core.is_busy());
    for msg in held {
        h.core.handle(msg);
    }
    assert!(h.app_output.try_recv().is_err());
    h.client(2, "turn/start", json!({"threadId":"线程"}));
    assert_eq!(
        h.incoming_client().await["error"]["data"]["reason"],
        "appServerUnresponsive"
    );
    let generation = h.core.machine.stops.probe.generation;
    tokio::time::sleep(Duration::from_secs(15)).await;
    h.pump_until(
        |msg| matches!(msg,CoreMsg::BreakerProbeTimer {generation:got} if *got==generation),
    )
    .await;
    let probe = h.outgoing().await;
    assert_eq!(probe["method"], "thread/list");
    assert_eq!(probe["params"], json!({"limit":1}));
    h.answer(&probe, Ok(json!({"data":[]}))).await;
    assert!(!h.core.machine.app_server_unresponsive);
    assert!(h.core.machine.stops.probe.pending.is_none());
    assert!(h.core.probe_timer.is_none());
    h.core.machine.records.get_mut("线程").unwrap().subscribed = true;
    h.client(3, "turn/start", json!({"threadId":"线程"}));
    assert_eq!(h.outgoing().await["method"], "turn/start");
}

#[tokio::test(start_paused = true)]
async fn 核心迟到处理晚于线上截止新收尾截止先到暂停回包仍完成中断() {
    // 保留 RPC 接收端，精确安排线上接收、转发、收尾截止和暂停回包的顺序。
    let (input, inbound) = mpsc::unbounded_channel();
    let (writer, mut output) = mpsc::unbounded_channel();
    let (inner, mut events) = rpc::new_client(inbound, writer, || async {}, Some(5000));
    let (mailbox, mut inbox) = mpsc::unbounded_channel();
    let mut core = Core {
        config: CoreConfig {
            idle_timeout: Duration::from_secs(3000),
            server: ServerInfo::Private { pid: None },
        },
        app: AppHandle { inner },
        mailbox,
        accept: None,
        conns: HashMap::new(),
        machine: Machine::new(false),
        pending_app: HashMap::new(),
        late_app: std::collections::VecDeque::new(),
        forward_timers: HashMap::new(),
        probe_timer: None,
        store: None,
        store_terminal: false,
        loaded: Loaded::default(),
        epoch: 0,
        survives_parent_job: false,
        parent_job_evidence: Value::Null,
        progress_timers: HashMap::new(),
        keyed_timers: HashMap::new(),
        daily_timer: None,
        runs_deadline: None,
        shutdown_wait_timer: None,
        health: None,
        shutdown_stage: None,
        app_disconnected: false,
        fixed_clock: None,
        next_tag: 0,
        last_activity: Instant::now(),
        started: Instant::now(),
        phase: Phase::Running,
    };
    let (client_writer, _client_output) = mpsc::unbounded_channel();
    core.handle(CoreMsg::Conn(ConnEvent::Connected {
        conn: 1,
        writer: client_writer,
    }));
    core.machine
        .records
        .entry("线程".into())
        .or_default()
        .subscribed = true;
    core.on_client_request(
        1,
        json!(1),
        "turn/start".into(),
        Some(json!({"threadId":"线程"})),
    );
    let start: Value = serde_json::from_str(&output.recv().await.unwrap()).unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;
    let timeout = events.recv().await.unwrap();
    let wire_deadline = match &timeout {
        AppEvent::TimedOut { deadline, .. } => *deadline,
        _ => unreachable!(),
    };
    core.handle(CoreMsg::App(timeout));
    let read: Value = serde_json::from_str(&output.recv().await.unwrap()).unwrap();
    core.machine.records.get_mut("线程").unwrap().subscribed = false;
    input
        .send(proto::response_ok(read["id"].clone(), json!({"thread":{"turns":[]}})).to_string())
        .unwrap();
    core.handle(CoreMsg::App(events.recv().await.unwrap()));
    // 截止前 RPC 收下启动回包，业务转发在原截止五秒以后才继续。
    input
        .send(proto::response_ok(start["id"].clone(), json!({"turn":{"id":"迟到轮"}})).to_string())
        .unwrap();
    let accepted = events.recv().await.unwrap();
    assert!(matches!(accepted, AppEvent::LateReply { .. }));
    tokio::time::sleep_until(Instant::from_std(wire_deadline) + Duration::from_secs(5)).await;
    let handled_at = Instant::now();
    core.handle(CoreMsg::App(accepted));
    let generation = core.machine.stops.late[0].generation;
    let get: Value = serde_json::from_str(&output.recv().await.unwrap()).unwrap();
    assert_eq!(get["method"], "thread/goal/get");
    input
        .send(
            proto::response_ok(get["id"].clone(), json!({"goal":{"status":"active"}})).to_string(),
        )
        .unwrap();
    core.handle(CoreMsg::App(events.recv().await.unwrap()));
    let pause: Value = serde_json::from_str(&output.recv().await.unwrap()).unwrap();
    assert_eq!(pause["method"], "thread/goal/set");
    let pause_tag = core.next_tag;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let timeout = events.recv().await.unwrap();
    assert!(matches!(&timeout,AppEvent::TimedOut {tag,..} if *tag==pause_tag));
    core.handle(CoreMsg::App(timeout));
    assert!(core
        .machine
        .stops
        .changes
        .values()
        .any(|change| change.unknown && change.late_brake));
    let queued: Vec<_> = std::iter::from_fn(|| inbox.try_recv().ok()).collect();
    assert!(queued.iter().all(|msg| !matches!(
        msg,
        CoreMsg::ClaimTimer {
            kind: TimerKind::LateExpiry { .. },
            ..
        }
    )));
    tokio::time::sleep_until(handled_at + Duration::from_millis(crate::stop::LATE_MS)).await;
    loop {
        let msg = inbox.recv().await.unwrap();
        let done = matches!(&msg,CoreMsg::ClaimTimer {kind:TimerKind::LateExpiry {generation:got},..} if *got==generation);
        core.handle(msg);
        if done {
            break;
        }
    }
    assert_eq!(core.machine.stops.late.len(), 1);
    assert!(core.machine.stops.late[0].cleanup_due);
    assert!(output.try_recv().is_err());
    input
        .send(
            proto::response_ok(pause["id"].clone(), json!({"goal":{"status":"paused"}}))
                .to_string(),
        )
        .unwrap();
    let message = events.recv().await.unwrap();
    assert!(matches!(&message,AppEvent::LateReply {tag,..} if *tag==pause_tag));
    core.handle(CoreMsg::App(message));
    let interrupt: Value = serde_json::from_str(&output.recv().await.unwrap()).unwrap();
    assert_eq!(interrupt["method"], "turn/interrupt");
    assert_eq!(interrupt["params"]["turnId"], "迟到轮");
    assert!(core.machine.stops.changes.is_empty());
    assert!(core.machine.stops.late.is_empty());
}

fn result_context() -> RequestContext {
    RequestContext {
        resume_retried: false,
        stream_id: None,
        review_id: None,
        client: None,
        token: Some(ClaimToken {
            thread_id: "线程".into(),
            claim_id: 0,
        }),
        operation: Operation::ResultRead(crate::runs::ResultRead {
            run_id: "运行".into(),
            thread_id: "线程".into(),
            turn_id: "本轮".into(),
            turn_index: 0,
            attempt: 0,
            generation: 1,
        }),
        method: "thread/read".into(),
        params: Some(json!({"threadId":"线程","includeTurns":true})),
    }
}

#[tokio::test(start_paused = true)]
async fn 补读不阻止条件关闭且三秒内回包先于封口() {
    let mut h = Harness::new(false);
    h.core.forward_request(result_context());
    let read = h.outgoing().await;
    assert!(!h.core.is_busy());
    h.client(90, "broker/shutdown", json!({"ifIdle":true}));
    assert_eq!(h.incoming_client().await["result"], json!({}));
    assert_eq!(h.core.shutdown_stage, Some(ShutdownStage::Collecting));
    assert_eq!(h.core.conns.len(), 1);
    h.answer(&read, Ok(json!({"thread":{"turns":[]}}))).await;
    assert_eq!(
        &h.core.machine.runs.hook_log[h.core.machine.runs.hook_log.len() - 2..],
        ["run_result_reply", "finalize_runs"]
    );
    assert_eq!(h.core.shutdown_stage, Some(ShutdownStage::Closing));
    assert!(h.core.runs_deadline.is_none());
}

#[tokio::test(start_paused = true)]
async fn 补读不挡空闲退出且三秒期限只设一次() {
    let mut h = Harness::new(false);
    h.core.forward_request(result_context());
    let _read = h.outgoing().await;
    h.core.on_idle_deadline();
    assert_eq!(h.core.shutdown_stage, Some(ShutdownStage::Collecting));
    tokio::time::advance(Duration::from_secs(2)).await;
    h.core.begin_shutdown(0);
    tokio::time::advance(Duration::from_secs(1)).await;
    h.pump_until(|msg| matches!(msg, CoreMsg::RunsDeadline))
        .await;
    assert_eq!(h.core.shutdown_stage, Some(ShutdownStage::Closing));
    let before = h.core.machine.runs.hook_log.clone();
    h.core.on_app_reply(1, Ok(json!({"thread":{"turns":[]}})));
    assert_eq!(h.core.machine.runs.hook_log, before);
    assert!(!h.core.results_pending());
}

#[tokio::test(start_paused = true)]
async fn 服务器已断开不等补读且无存储关闭零等待() {
    let mut h = Harness::new(false);
    h.core.forward_request(result_context());
    h.core.handle(CoreMsg::App(AppEvent::Closed {
        reason: "测试断开".into(),
    }));
    assert_eq!(h.core.shutdown_stage, Some(ShutdownStage::Closing));
    assert!(h.core.runs_deadline.is_none());
    assert!(h.core.machine.runs_finalized);
    let mut h = Harness::new(false);
    h.core.begin_shutdown(0);
    assert_eq!(h.core.shutdown_stage, Some(ShutdownStage::Closing));
    assert_eq!(
        h.core.machine.runs.hook_log,
        ["shutdown_runs", "finalize_runs"]
    );
}

#[tokio::test(start_paused = true)]
async fn 恢复先于排队请求且固定时钟注入() {
    let mut h = Harness::new(false);
    h.core.fixed_clock = Some(claim::Clock {
        now_ms: 123,
        entropy: 456,
        mono_ms: 0,
    });
    h.core
        .mailbox
        .send(CoreMsg::Conn(ConnEvent::Message {
            conn: 1,
            message: Ok(proto::request(
                json!(1),
                "turn/start",
                Some(json!({"threadId":"线程"})),
            )),
        }))
        .unwrap();
    h.core.restore();
    h.core.handle(h.inbox.recv().await.unwrap());
    assert_eq!(
        &h.core.machine.runs.hook_log[..2],
        ["restore_runs", "run_opened"]
    );
    assert_eq!(
        h.core.machine.clock,
        claim::Clock {
            now_ms: 123,
            entropy: 456,
            mono_ms: 0,
        }
    );
}

#[tokio::test(start_paused = true)]
async fn 存储被接管正常关闭而写入任务死亡继续服务() {
    let mut h = Harness::new(false);
    h.core.handle(CoreMsg::Store(StoreEvent::WriterDied {
        reason: "测试退出".into(),
    }));
    assert_eq!(h.core.phase, Phase::Running);
    h.core.handle(CoreMsg::Store(StoreEvent::Superseded {
        file: "threads.json".into(),
    }));
    assert!(matches!(
        h.core.phase,
        Phase::ShuttingDown { exit_code: 0, .. }
    ));
    assert_eq!(
        h.core.machine.runs.hook_log,
        ["shutdown_runs", "finalize_runs"]
    );
}

#[tokio::test(start_paused = true)]
async fn 进度计时复用每日单例关闭取消() {
    let mut h = Harness::new(false);
    h.core
        .machine
        .records
        .entry("线程".into())
        .or_default()
        .subscribed = true;
    h.client(1, "turn/start", json!({"threadId":"线程"}));
    let run = h.core.machine.claims["线程"].run_id.clone().unwrap();
    let token = h.core.machine.token("线程").unwrap();
    let schedule = |timer| Action::Schedule {
        token: token.clone(),
        kind: TimerKind::Run(timer),
        delay_ms: 5000,
    };
    h.core.execute(vec![schedule(RunTimer::ProgressFlush {
        run_id: run.clone(),
        generation: 1,
    })]);
    let first = h.core.progress_timers[&run].1.clone();
    h.core.execute(vec![
        schedule(RunTimer::ProgressFlush {
            run_id: run.clone(),
            generation: 2,
        }),
        schedule(RunTimer::DailyCleanup),
        schedule(RunTimer::DailyCleanup),
    ]);
    tokio::task::yield_now().await;
    assert!(first.is_finished());
    assert_eq!(h.core.progress_timers.len(), 1);
    assert_eq!(h.core.progress_timers[&run].0, 2);
    assert!(h.core.daily_timer.is_some());
    h.core.begin_shutdown(0);
    assert!(h.core.daily_timer.is_none());
    assert!(h.core.progress_timers.is_empty());
}

#[tokio::test(start_paused = true)]
async fn 补读超时只结算一次且不登记迟到回包() {
    let mut h = Harness::new(false);
    h.core.forward_request(result_context());
    let _read = h.outgoing().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    h.pump_until(|msg| matches!(msg, CoreMsg::App(AppEvent::TimedOut { .. })))
        .await;
    assert!(h.core.pending_app.is_empty());
    assert!(h.core.late_app.is_empty());
    assert_eq!(
        h.core
            .machine
            .runs
            .hook_log
            .iter()
            .filter(|h| *h == "run_result_reply")
            .count(),
        1
    );
    h.core.on_app_reply(1, Ok(json!({})));
    assert_eq!(
        h.core
            .machine
            .runs
            .hook_log
            .iter()
            .filter(|h| *h == "run_result_reply")
            .count(),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn 熔断已断开时补读超时不合上熔断器() {
    let mut h = Harness::new(false);
    h.core
        .machine
        .records
        .entry("线程".into())
        .or_default()
        .subscribed = true;
    h.client(1, "turn/start", json!({"threadId":"线程"}));
    let start = h.outgoing().await;
    h.answer(
        &start,
        Ok(json!({"turn":{"id":"本轮","status":"inProgress","items":[],"error":null}})),
    )
    .await;
    let run = h.core.machine.claims["线程"].run_id.clone().unwrap();
    h.notification("turn/completed", json!({"threadId":"线程","turn":{"id":"本轮","status":"completed","items":[],"error":null}})).await;
    let read = h.outgoing().await;
    assert_eq!(read["method"], "thread/items/list");
    assert_eq!(read["params"]["turnId"], "本轮");
    assert!(read["params"]["cursor"].is_null());
    assert_eq!(read["params"]["threadId"], "线程");
    let read_tag = *h
        .core
        .pending_app
        .iter()
        .find(|(_, context)| matches!(context.operation, Operation::ResultRead(_)))
        .unwrap()
        .0;
    h.core.machine.app_server_unresponsive = true;
    // 令独立试探在本次 5 秒 RPC 超时之后触发，以便精确核对未被取消或重排。
    h.core.machine.stops.probe.delay_ms = 30_000;
    h.core.dispatch(Event::Connected { conn: 2 });
    let generation = h.core.machine.stops.probe.generation;
    let timer = h.core.probe_timer.as_ref().unwrap().1.clone();
    tokio::time::advance(Duration::from_secs(5)).await;
    h.pump_until(
        |msg| matches!(msg, CoreMsg::App(AppEvent::TimedOut { tag, .. }) if *tag == read_tag),
    )
    .await;
    assert!(h.core.machine.app_server_unresponsive);
    assert!(h.core.machine.stops.probe.scheduled);
    assert_eq!(h.core.machine.stops.probe.generation, generation);
    assert_eq!(h.core.probe_timer.as_ref().unwrap().0, generation);
    assert!(!timer.is_finished());
    assert!(!h.core.pending_app.contains_key(&read_tag));
    assert!(h.core.late_app.is_empty());
    assert_eq!(
        h.core.machine.runs.runs[&run].turns[0].ready,
        crate::runs::Ready::Pending
    );
    assert_eq!(
        h.core
            .machine
            .runs
            .hook_log
            .iter()
            .filter(|hook| *hook == "run_result_reply")
            .count(),
        1
    );
    // 同次超时即使重复进入本地入口，也不能再次结算或恢复熔断状态。
    h.core.on_timeout(
        read_tag,
        AppError::Closed("本地重复失败".into()),
        Instant::now().into_std(),
    );
    assert!(h.core.machine.app_server_unresponsive);
    assert_eq!(
        h.core
            .machine
            .runs
            .hook_log
            .iter()
            .filter(|hook| *hook == "run_result_reply")
            .count(),
        1
    );
    h.core.begin_shutdown(0);
    h.core.finalize_shutdown();
}

struct StoreTestDir(std::path::PathBuf);
impl StoreTestDir {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let serial = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cxb-core-store-{}-{now}-{serial}",
            std::process::id()
        ));
        std::fs::create_dir(&root).unwrap();
        Self(root)
    }
}
impl Drop for StoreTestDir {
    fn drop(&mut self) {
        // 仅删除本测试创建且规范化后仍属于系统临时目录的独立目录。
        let root = std::fs::canonicalize(&self.0).unwrap();
        let parent = std::fs::canonicalize(std::env::temp_dir()).unwrap();
        assert!(root.starts_with(&parent) && root != parent);
        std::fs::remove_dir_all(root).unwrap();
    }
}

async fn assert_store_terminal(event: Option<StoreEvent>) {
    use crate::store::{self, BrokerInfo, StoreConfig, StoreOp};
    let temp = StoreTestDir::new();
    let (store, _) = store::open(StoreConfig {
        root: temp.0.clone(),
        workspace: temp.0.clone(),
        broker: BrokerInfo {
            pid: std::process::id(),
            proc_start_ft: store::process_start_ft(),
            epoch: 0,
            endpoint: "测试端点".into(),
            broker_version: "测试版本".into(),
            survives_parent_job: None,
            parent_job: None,
        },
        on_event: Box::new(|_| {}),
    })
    .await
    .unwrap();
    let mut h = Harness::new(false);
    h.core
        .machine
        .records
        .entry("线程".into())
        .or_default()
        .subscribed = true;
    h.client(1, "turn/start", json!({"threadId":"线程"}));
    let run = h.core.machine.claims["线程"].run_id.clone().unwrap();
    // 此前未接 store，确保 Superseded 触发的 shutdown_runs 也不能投递运行记录。
    h.core.store = Some(store.clone());
    let terminal = event.is_some();
    if let Some(event) = event {
        h.core.handle(CoreMsg::Store(event));
    }
    h.client(99, "broker/health", json!({}));
    loop {
        let message = h.incoming_client().await;
        if message["id"] == 99 {
            assert_eq!(
                message["result"]["storage"],
                if terminal { "degraded" } else { "ok" }
            );
            break;
        }
    }
    assert_eq!(h.core.store_terminal, terminal);
    assert!(h.core.store.is_some(), "保留句柄供查询和排盘");
    h.core.execute(vec![Action::Store(StoreOp::AppendLog {
        short_id: "terminal-check".into(),
        lines: vec!["检查终态后的投递".into()],
    })]);
    let report = store.drain(Duration::from_secs(2)).await;
    assert!(report.abandoned.is_empty());
    let directory = store::workspace_dir(&temp.0, &temp.0);
    assert_eq!(
        directory.join("runs/terminal-check.log").exists(),
        !terminal
    );
    assert!(!directory.join(format!("runs/{run}.json")).exists());
    if matches!(h.core.phase, Phase::Running) {
        h.core.store = None;
        h.core.begin_shutdown(0);
    }
    drop(h);
    drop(store);
}

#[tokio::test]
async fn 写入任务死亡后停止投递但保留查询句柄() {
    assert_store_terminal(None).await;
    assert_store_terminal(Some(StoreEvent::WriterDied {
        reason: "测试写入线程退出".into(),
    }))
    .await;
}

#[tokio::test]
async fn 存储被接管后关闭产生的写入也不再投递() {
    assert_store_terminal(Some(StoreEvent::Superseded {
        file: "threads.json".into(),
    }))
    .await;
}

#[tokio::test(start_paused = true)]
async fn 初始化扩展门控重复声明与恢复后纪元不丢失() {
    assert_eq!(USER_AGENT, "cxb-broker");
    let mut h = Harness::new(false);
    h.core.epoch = 42;
    h.core.loaded.epoch = 42;
    h.core.restore();
    h.client(10, "initialize", json!({}));
    let plain = h.incoming_client().await;
    assert_eq!(
        plain["result"],
        json!({"userAgent":USER_AGENT,"busy":false,"server":{"kind":"private"}})
    );
    h.client(
        11,
        "initialize",
        json!({"capabilities":{"cxb":{"protocolVersion":1}}}),
    );
    let extended = h.incoming_client().await;
    assert_eq!(extended["result"]["epoch"], 42);
    assert_eq!(extended["result"]["protocolVersion"], 1);
    assert_eq!(
        extended["result"]["brokerVersion"],
        env!("CARGO_PKG_VERSION")
    );
    assert!(h.core.machine.cxb_conns.contains(&1));
    h.client(12, "initialize", json!({}));
    h.incoming_client().await;
    assert!(!h.core.machine.cxb_conns.contains(&1));
}
#[tokio::test(start_paused = true)]
async fn 健康查询无存储与终态为降级并在关闭期间可读() {
    let mut h = Harness::new(false);
    h.core.epoch = 9;
    h.core.survives_parent_job = true;
    h.core.parent_job_evidence = json!({"inJob": true, "killOnClose": false});
    tokio::time::advance(Duration::from_millis(123)).await;
    h.client(10, "broker/health", json!({}));
    let value = h.incoming_client().await;
    assert_eq!(value["result"]["storage"], "degraded");
    assert_eq!(value["result"]["epoch"], 9);
    assert_eq!(value["result"]["uptimeMs"], 123);
    assert_eq!(value["result"]["claims"], 0);
    assert_eq!(value["result"]["pending"], 0);
    assert_eq!(value["result"]["survivesParentJob"], true);
    assert_eq!(
        value["result"]["parentJob"],
        json!({"inJob": true, "killOnClose": false})
    );
    h.core.machine.closing = true;
    h.core.store_terminal = true;
    h.client(11, "broker/health", json!({}));
    assert_eq!(h.incoming_client().await["result"]["storage"], "degraded");
}
