//! 命名管道服务端与连接读写任务（详细设计 0.1、2.1 节，第 5 节关闭第 1、7 步）。
//!
//! 接入循环照 tokio 官方示例（named_pipe.rs 的 server 例子）和 S3 环境探测 pipe_single_instance：
//! - **只有第一个实例**带 `first_pipe_instance(true)`，用来抢占管道名；之后每接入一个客户端，
//!   先用不带该标志的选项建好下一个监听实例，再把已连接的实例交出去。任何时刻至少保留一个实例，
//!   管道名才占得住；新建失败时不交出当前实例，记错误并重试。
//! - **关闭期间不释放管道名**：进入拒绝模式后接入循环照常建实例、接连接，但连接一进来就立即断开；
//!   只有收到停止信号、丢弃最后的监听实例时，管道名才释放（第 5 节第 1、7 步）。
//! - 连接一建立就启动读写任务，读到的消息投进 core 的邮箱；core 在 app-server 握手完成之前不处理
//!   邮箱，消息就在邮箱里排队（2.1 节「启动顺序」），客户端看到的效果与上游「握手后才 bind」一致。
//!
//! 本模块不认识 core 的类型：事件通过调用方给的 sink 闭包投出，保证与其他来源进入同一个邮箱、
//! 按到达顺序处理（1.2 节「所有输入只有一个入口」）。

use std::io;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::windows::named_pipe::{NamedPipeServer, ServerOptions};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::AbortHandle;

use crate::ipc::frame::{LineFramer, MAX_PENDING_BYTES};

/// 连接编号，在本 broker 进程内单调递增、不复用。
pub type ConnId = u64;

/// core 发给某个连接写任务的指令。
#[derive(Debug)]
pub enum WriterCmd {
    /// 写出一行（调用方已带好结尾的 `\n`）。
    Line(String),
    /// 写完之前排队的内容后关闭连接（第 5 节第 6 步「半关闭」：命名管道没有半关闭，
    /// 这里在写完后同时结束读任务，使管道句柄真正关闭，客户端读到连接结束）。
    /// `done` 在写完、关闭之后被触发，关闭流程据此等连接排空（第 7 步）。
    Close { done: Option<oneshot::Sender<()>> },
}

/// 连接为什么结束。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloseReason {
    /// 客户端关闭了连接。
    Eof,
    /// 未换行的数据超过上限（清单 1.2 第 13 条），broker 主动断开。
    Overflow,
    /// 读出错（通常是客户端异常断开）。
    ReadError(String),
}

/// 接入层投给 core 的事件。同一连接的事件按发生顺序投出：先 `Connected`，再若干 `Message`，最后 `Closed`。
#[derive(Debug)]
pub enum ConnEvent {
    Connected {
        conn: ConnId,
        writer: mpsc::UnboundedSender<WriterCmd>,
    },
    /// 一行解析后的消息；解析失败时为错误说明，core 据此回 -32700。
    Message {
        conn: ConnId,
        message: Result<Value, String>,
    },
    Closed {
        conn: ConnId,
        reason: CloseReason,
    },
}

/// 抢占管道名失败的原因。
#[derive(Debug)]
pub enum BindError {
    /// 同名管道已被别的进程占用（另一个 broker 已在服务这个工作区）。
    Taken,
    Io(io::Error),
}

/// 持有当前监听实例的管道服务端。
#[derive(Debug)]
pub struct PipeListener {
    name: String,
    current: NamedPipeServer,
}

impl PipeListener {
    /// 以首实例抢占管道名。同名管道已存在时返回 [`BindError::Taken`]（实测错误类别为权限不足）。
    pub fn bind_first(name: &str) -> Result<Self, BindError> {
        match ServerOptions::new().first_pipe_instance(true).create(name) {
            Ok(current) => Ok(Self {
                name: name.to_owned(),
                current,
            }),
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => Err(BindError::Taken),
            Err(e) => Err(BindError::Io(e)),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// 等下一个客户端连上，返回已连接的实例；返回前已建好替补的监听实例。
    async fn accept(&mut self) -> io::Result<NamedPipeServer> {
        let connected = self.current.connect().await;
        // 不论这次接入成败，都先建好下一个实例再交出（或丢弃）当前实例，保证管道名一直被占着。
        let next = self.create_next().await;
        let previous = std::mem::replace(&mut self.current, next);
        connected.map(|()| previous)
    }

    async fn create_next(&self) -> NamedPipeServer {
        let mut delay = Duration::from_millis(10);
        loop {
            match ServerOptions::new().create(&self.name) {
                Ok(server) => return server,
                Err(e) => {
                    tracing::error!(pipe = %self.name, error = %e, "创建下一个监听实例失败，稍后重试");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(1));
                }
            }
        }
    }
}

/// 接入循环的控制端。
#[derive(Debug)]
pub struct AcceptControl {
    refuse: watch::Sender<bool>,
    stop: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl AcceptControl {
    /// 进入拒绝模式：之后接入的连接立即断开，但管道名继续占着（第 5 节第 1 步）。
    pub fn refuse_new(&self) {
        let _ = self.refuse.send(true);
    }

    /// 停止接入循环并释放管道名（第 5 节第 7 步），等循环真正退出后返回。
    pub async fn stop(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let _ = (&mut self.task).await;
    }
}

/// 每个连接任务的配置。
#[derive(Debug, Clone, Copy)]
pub struct ConnConfig {
    /// 单连接未换行数据上限，缺省 10 MiB。
    pub max_pending_bytes: usize,
    /// 读缓冲大小。
    pub read_chunk: usize,
}

impl Default for ConnConfig {
    fn default() -> Self {
        Self {
            max_pending_bytes: MAX_PENDING_BYTES,
            read_chunk: 64 * 1024,
        }
    }
}

/// 启动接入循环。`sink` 收到的事件应原样投进 core 邮箱。
pub fn spawn_accept_loop<F>(
    mut listener: PipeListener,
    config: ConnConfig,
    sink: F,
) -> AcceptControl
where
    F: Fn(ConnEvent) + Send + Sync + 'static,
{
    let sink = Arc::new(sink);
    let (refuse_tx, refuse_rx) = watch::channel(false);
    let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let mut next_conn: ConnId = 1;
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                accepted = listener.accept() => match accepted {
                    Ok(stream) => {
                        if *refuse_rx.borrow() {
                            tracing::debug!("broker 正在关闭，断开新连接");
                            drop(stream);
                            continue;
                        }
                        let conn = next_conn;
                        next_conn += 1;
                        start_connection(conn, stream, config, Arc::clone(&sink));
                    }
                    Err(e) => tracing::warn!(error = %e, "接入客户端失败"),
                },
            }
        }
        tracing::debug!(pipe = %listener.name(), "接入循环结束，释放管道名");
    });
    AcceptControl {
        refuse: refuse_tx,
        stop: Some(stop_tx),
        task,
    }
}

fn start_connection<F>(conn: ConnId, stream: NamedPipeServer, config: ConnConfig, sink: Arc<F>)
where
    F: Fn(ConnEvent) + Send + Sync + 'static,
{
    let (reader, writer) = tokio::io::split(stream);
    let (writer_tx, writer_rx) = mpsc::unbounded_channel();
    // 先投 Connected，再启动读任务：同一连接的事件在邮箱里一定先于它的消息。
    sink(ConnEvent::Connected {
        conn,
        writer: writer_tx,
    });
    let read_task = tokio::spawn(read_loop(conn, reader, config, Arc::clone(&sink)));
    tokio::spawn(write_loop(writer, writer_rx, read_task.abort_handle()));
}

async fn read_loop<F>(
    conn: ConnId,
    mut reader: ReadHalf<NamedPipeServer>,
    config: ConnConfig,
    sink: Arc<F>,
) where
    F: Fn(ConnEvent) + Send + Sync + 'static,
{
    let mut framer = LineFramer::with_limit(config.max_pending_bytes);
    let mut buf = vec![0u8; config.read_chunk];
    let reason = loop {
        match reader.read(&mut buf).await {
            Ok(0) => break CloseReason::Eof,
            Ok(n) => match framer.push(&buf[..n]) {
                Ok(lines) => {
                    for line in lines {
                        let message =
                            serde_json::from_str::<Value>(&line).map_err(|e| e.to_string());
                        sink(ConnEvent::Message { conn, message });
                    }
                }
                Err(overflow) => {
                    tracing::warn!(conn, %overflow, "客户端未换行数据超过上限，断开连接");
                    break CloseReason::Overflow;
                }
            },
            Err(e) => break CloseReason::ReadError(e.to_string()),
        }
    };
    sink(ConnEvent::Closed { conn, reason });
}

async fn write_loop(
    mut writer: WriteHalf<NamedPipeServer>,
    mut commands: mpsc::UnboundedReceiver<WriterCmd>,
    reader: AbortHandle,
) {
    while let Some(cmd) = commands.recv().await {
        match cmd {
            WriterCmd::Line(line) => {
                if let Err(e) = writer.write_all(line.as_bytes()).await {
                    tracing::debug!(error = %e, "写客户端失败，连接可能已断开");
                    break;
                }
            }
            WriterCmd::Close { done } => {
                let _ = writer.flush().await;
                // 结束读任务，读写两半都释放后管道句柄才真正关闭。
                reader.abort();
                drop(writer);
                if let Some(done) = done {
                    let _ = done.send(());
                }
                return;
            }
        }
    }
    // 发送端被丢弃（core 已忘记这个连接）时也走到这里：写半随之释放。
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient};
    use tokio::time::{sleep, timeout, Instant};

    fn unique_pipe_name() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        format!(
            r"\\.\pipe\cxb-ipc-test-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        )
    }

    async fn open_client(name: &str) -> NamedPipeClient {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match ClientOptions::new().open(name) {
                Ok(c) => return c,
                Err(e) if Instant::now() < deadline => {
                    let _ = e;
                    sleep(Duration::from_millis(10)).await;
                }
                Err(e) => panic!("连接管道失败：{e}"),
            }
        }
    }

    type Events = Arc<Mutex<Vec<ConnEvent>>>;

    fn collecting_sink() -> (Events, impl Fn(ConnEvent) + Send + Sync + 'static) {
        let events: Events = Arc::default();
        let sink_events = Arc::clone(&events);
        (events, move |ev| sink_events.lock().unwrap().push(ev))
    }

    async fn wait_until(events: &Events, pred: impl Fn(&[ConnEvent]) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !pred(&events.lock().unwrap()) {
            assert!(Instant::now() < deadline, "等待事件超时");
            sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn 同名首实例被拒() {
        let name = unique_pipe_name();
        let _first = PipeListener::bind_first(&name).expect("首实例");
        assert!(matches!(
            PipeListener::bind_first(&name),
            Err(BindError::Taken)
        ));
    }

    #[tokio::test]
    async fn 十个客户端并发连接都能收发() {
        let name = unique_pipe_name();
        let listener = PipeListener::bind_first(&name).unwrap();
        let (events, sink) = collecting_sink();
        let control = spawn_accept_loop(listener, ConnConfig::default(), sink);

        let mut clients = Vec::new();
        for i in 0..10 {
            let mut c = open_client(&name).await;
            c.write_all(format!("{{\"id\":{i}}}\n").as_bytes())
                .await
                .unwrap();
            clients.push(c);
        }
        wait_until(&events, |evs| {
            evs.iter()
                .filter(|e| matches!(e, ConnEvent::Message { .. }))
                .count()
                == 10
        })
        .await;

        // 通过各自的写任务回一行，客户端都能收到。
        let writers: Vec<_> = events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                ConnEvent::Connected { writer, .. } => Some(writer.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(writers.len(), 10);
        for w in &writers {
            w.send(WriterCmd::Line("{\"ok\":true}\n".into())).unwrap();
        }
        for c in clients {
            let mut line = String::new();
            timeout(
                Duration::from_secs(5),
                BufReader::new(c).read_line(&mut line),
            )
            .await
            .expect("读回包超时")
            .unwrap();
            assert_eq!(line, "{\"ok\":true}\n");
        }
        control.stop().await;
    }

    #[tokio::test]
    async fn 坏_json_以错误说明投出() {
        let name = unique_pipe_name();
        let (events, sink) = collecting_sink();
        let control = spawn_accept_loop(
            PipeListener::bind_first(&name).unwrap(),
            ConnConfig::default(),
            sink,
        );
        let mut c = open_client(&name).await;
        c.write_all(b"not json\n").await.unwrap();
        wait_until(&events, |evs| {
            evs.iter().any(|e| {
                matches!(
                    e,
                    ConnEvent::Message {
                        message: Err(_),
                        ..
                    }
                )
            })
        })
        .await;
        control.stop().await;
    }

    #[tokio::test]
    async fn 未换行数据超限时断开() {
        let name = unique_pipe_name();
        let (events, sink) = collecting_sink();
        let config = ConnConfig {
            max_pending_bytes: 1024,
            read_chunk: 256,
        };
        let control = spawn_accept_loop(PipeListener::bind_first(&name).unwrap(), config, sink);
        let mut c = open_client(&name).await;
        c.write_all(&vec![b'x'; 4096]).await.unwrap();
        wait_until(&events, |evs| {
            evs.iter().any(|e| {
                matches!(
                    e,
                    ConnEvent::Closed {
                        reason: CloseReason::Overflow,
                        ..
                    }
                )
            })
        })
        .await;
        control.stop().await;
    }

    #[tokio::test]
    async fn 拒绝模式下新连接立即断开但管道名仍被占用() {
        let name = unique_pipe_name();
        let (events, sink) = collecting_sink();
        let control = spawn_accept_loop(
            PipeListener::bind_first(&name).unwrap(),
            ConnConfig::default(),
            sink,
        );
        control.refuse_new();
        let c = open_client(&name).await;
        let mut line = String::new();
        let n = timeout(
            Duration::from_secs(5),
            BufReader::new(c).read_line(&mut line),
        )
        .await
        .expect("应很快读到连接结束")
        .unwrap_or(0);
        assert_eq!(n, 0, "被拒绝的连接应直接结束");
        assert!(
            events.lock().unwrap().is_empty(),
            "被拒绝的连接不应投出事件"
        );
        assert!(matches!(
            PipeListener::bind_first(&name),
            Err(BindError::Taken)
        ));

        control.stop().await;
        // 停止后管道名立即可以重新抢占。
        assert!(PipeListener::bind_first(&name).is_ok());
    }

    #[tokio::test]
    async fn close_指令写完后关闭连接() {
        let name = unique_pipe_name();
        let (events, sink) = collecting_sink();
        let control = spawn_accept_loop(
            PipeListener::bind_first(&name).unwrap(),
            ConnConfig::default(),
            sink,
        );
        let c = open_client(&name).await;
        wait_until(&events, |evs| {
            evs.iter().any(|e| matches!(e, ConnEvent::Connected { .. }))
        })
        .await;
        let writer = match &events.lock().unwrap()[0] {
            ConnEvent::Connected { writer, .. } => writer.clone(),
            _ => unreachable!(),
        };
        writer
            .send(WriterCmd::Line("{\"bye\":1}\n".into()))
            .unwrap();
        let (done_tx, done_rx) = oneshot::channel();
        writer
            .send(WriterCmd::Close {
                done: Some(done_tx),
            })
            .unwrap();
        let mut reader = BufReader::new(c);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(line, "{\"bye\":1}\n");
        line.clear();
        let n = timeout(Duration::from_secs(5), reader.read_line(&mut line))
            .await
            .expect("应读到连接结束")
            .unwrap_or(0);
        assert_eq!(n, 0);
        timeout(Duration::from_secs(5), done_rx)
            .await
            .expect("应收到写完回执")
            .unwrap();
        control.stop().await;
    }
}
