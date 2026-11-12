//! M7 甲路传输与初始化互斥量：全部使用本地内存流、假 socket 或内核互斥量。

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use cxb_broker::appserver::{init_lock, shared, ws, AppError, AppEvent, ServerInfo, SharedVia};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;
use tokio_tungstenite::{accept_async, tungstenite::Message};

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(4), future)
        .await
        .expect("测试操作应在 4 秒内结束")
}

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "cxb-m7-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn socket(&self) -> PathBuf {
        self.0.join("共享 a.sock")
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).expect("测试资源应已释放");
    }
}

#[tokio::test]
async fn 文本请求往返且二进制通知按_utf8_解码() {
    let (client_stream, server_stream) = tokio::io::duplex(65536);
    let server = tokio::spawn(async move {
        let mut socket = accept_async(server_stream).await.unwrap();
        let request: Value =
            serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        assert_eq!(request["method"], "测试/请求");
        socket
            .send(Message::Text(
                json!({"id":request["id"],"result":{"ok":true}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        socket
            .send(Message::Binary(vec![0xff].into()))
            .await
            .unwrap();
        socket
            .send(Message::Binary(
                br#"{"method":"test/binary","params":{"n":1}}"#.to_vec().into(),
            ))
            .await
            .unwrap();
        assert!(matches!(socket.next().await, Some(Ok(Message::Close(_)))));
        socket.flush().await.unwrap();
    });
    let (client, mut events) = ws::client_over(client_stream, || async {}, None)
        .await
        .unwrap();
    assert_eq!(
        bounded(client.request("测试/请求", None, None))
            .await
            .unwrap()
            .unwrap(),
        json!({"ok":true})
    );
    assert!(matches!(
        bounded(events.recv()).await,
        Some(AppEvent::ObservationGap { .. })
    ));
    assert_eq!(
        bounded(events.recv()).await,
        Some(AppEvent::Notification {
            method: "test/binary".into(),
            params: Some(json!({"n":1}))
        })
    );
    bounded(client.close()).await;
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn 无业务写入时服务端_ping_也能收到_pong() {
    let (client_stream, server_stream) = tokio::io::duplex(1024);
    let server = tokio::spawn(async move {
        let mut socket = accept_async(server_stream).await.unwrap();
        socket
            .send(Message::Ping("探活".as_bytes().to_vec().into()))
            .await
            .unwrap();
        assert_eq!(
            socket.next().await.unwrap().unwrap(),
            Message::Pong("探活".as_bytes().to_vec().into())
        );
        socket.close(None).await.unwrap();
        let _ = socket.next().await;
    });
    let (client, mut events) = ws::client_over(client_stream, || async {}, None)
        .await
        .unwrap();
    assert!(matches!(
        bounded(events.recv()).await,
        Some(AppEvent::Closed { .. })
    ));
    bounded(client.close()).await;
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn 服务端_close_和突然_eof_均结算在途请求() {
    for graceful in [true, false] {
        let (client_stream, server_stream) = tokio::io::duplex(1024);
        let server = tokio::spawn(async move {
            let mut socket = accept_async(server_stream).await.unwrap();
            let _ = socket.next().await;
            if graceful {
                socket.close(None).await.unwrap();
                let _ = socket.next().await;
            }
        });
        let (client, mut events) = ws::client_over(client_stream, || async {}, None)
            .await
            .unwrap();
        let reply = bounded(client.request("待关闭", None, None)).await.unwrap();
        assert!(matches!(reply, Err(AppError::Closed(_))));
        assert!(matches!(
            bounded(events.recv()).await,
            Some(AppEvent::Closed { .. })
        ));
        bounded(client.close()).await;
        bounded(server).await.unwrap();
    }
}

#[tokio::test]
async fn 超限单帧和分片消息都关闭连接() {
    use tokio_tungstenite::tungstenite::protocol::{
        frame::{
            coding::{Data, OpCode},
            Frame,
        },
        WebSocketConfig,
    };
    for fragmented in [false, true] {
        let (client_stream, server_stream) = tokio::io::duplex(65536);
        let server = tokio::spawn(async move {
            let mut socket = tokio_tungstenite::accept_async_with_config(
                server_stream,
                Some(WebSocketConfig::default()),
            )
            .await
            .unwrap();
            if fragmented {
                for (code, finished) in [(Data::Text, false), (Data::Continue, true)] {
                    let frame = Frame::message(
                        vec![b'a'; ws::MAX_MESSAGE_BYTES / 2 + 1],
                        OpCode::Data(code),
                        finished,
                    );
                    if socket.send(Message::Frame(frame)).await.is_err() {
                        break;
                    }
                }
            } else {
                let _ = socket
                    .send(Message::Text("a".repeat(ws::MAX_MESSAGE_BYTES + 1).into()))
                    .await;
            }
        });
        let (client, mut events) = ws::client_over(client_stream, || async {}, None)
            .await
            .unwrap();
        assert!(matches!(
            bounded(events.recv()).await,
            Some(AppEvent::Closed { .. })
        ));
        bounded(client.close()).await;
        bounded(server).await.unwrap();
    }
}

#[tokio::test]
async fn 非101握手失败先回收再返回错误() {
    let (client_stream, mut server_stream) = tokio::io::duplex(1024);
    let server = tokio::spawn(async move {
        let mut bytes = [0; 1024];
        assert!(server_stream.read(&mut bytes).await.unwrap() > 0);
        server_stream
            .write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    });
    let (clean, cleaned) = oneshot::channel();
    let error = ws::client_over(
        client_stream,
        move || async move {
            let _ = clean.send(());
        },
        None,
    )
    .await
    .unwrap_err();
    assert!(error.contains("WebSocket 握手失败") && error.contains("403"));
    bounded(cleaned).await.unwrap();
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn 握手取消关闭流并且清理恰好一次() {
    let (client_stream, mut server_stream) = tokio::io::duplex(1024);
    let (seen, received) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut bytes = [0; 1024];
        assert!(server_stream.read(&mut bytes).await.unwrap() > 0);
        seen.send(()).unwrap();
        assert_eq!(server_stream.read(&mut bytes).await.unwrap(), 0);
    });
    let (clean, cleaned) = oneshot::channel();
    let connecting = tokio::spawn(ws::client_over(
        client_stream,
        move || async move {
            let _ = clean.send(());
        },
        None,
    ));
    bounded(received).await.unwrap();
    connecting.abort();
    assert!(connecting.await.unwrap_err().is_cancelled());
    bounded(cleaned).await.unwrap();
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn 关闭的一秒期限覆盖阻塞的_close_发送且先释放流再清理() {
    use std::pin::Pin;
    use std::sync::{atomic::AtomicBool, Arc};
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};

    struct ObservedStream {
        inner: DuplexStream,
        armed: Arc<AtomicBool>,
        blocked: Option<oneshot::Sender<()>>,
    }
    impl AsyncRead for ObservedStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(cx, buffer)
        }
    }
    impl AsyncWrite for ObservedStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let result = Pin::new(&mut self.inner).poll_write(cx, bytes);
            // 握手完成后才启用，确保信号对应业务帧已填满底层缓冲。
            if result.is_pending() && self.armed.load(Ordering::SeqCst) {
                if let Some(blocked) = self.blocked.take() {
                    let _ = blocked.send(());
                }
            }
            result
        }
        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    let (client_stream, server_stream) = tokio::io::duplex(64);
    let armed = Arc::new(AtomicBool::new(false));
    let (blocked, write_blocked) = oneshot::channel();
    let client_stream = ObservedStream {
        inner: client_stream,
        armed: armed.clone(),
        blocked: Some(blocked),
    };
    let (ready, accepted) = oneshot::channel();
    let (inspect, inspected) = oneshot::channel();
    let server = tokio::spawn(async move {
        let socket = accept_async(server_stream).await.unwrap();
        ready.send(()).unwrap();
        inspected.await.unwrap();
        let mut raw = socket.into_inner();
        let mut bytes = Vec::new();
        raw.read_to_end(&mut bytes).await.unwrap();
    });
    let (clean, cleaned) = oneshot::channel();
    let (client, _) = ws::client_over(
        client_stream,
        move || async move {
            let _ = clean.send(());
        },
        None,
    )
    .await
    .unwrap();
    accepted.await.unwrap();
    armed.store(true, Ordering::SeqCst);
    client.notify("占满缓冲", Some(json!("x".repeat(4096))));
    bounded(write_blocked).await.unwrap();
    client.fail("测试主动中止写入");
    tokio::time::timeout(Duration::from_millis(1400), client.close())
        .await
        .unwrap();
    cleaned.await.unwrap();
    inspect.send(()).unwrap();
    bounded(server).await.unwrap();
}

#[cfg(windows)]
fn fake_shared(path: &Path, initialize_error: bool) -> std::thread::JoinHandle<()> {
    let listener = uds_windows::UnixListener::bind(path).unwrap();
    std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut socket = tokio_tungstenite::tungstenite::accept(stream).unwrap();
        let request: Value =
            serde_json::from_str(socket.read().unwrap().to_text().unwrap()).unwrap();
        assert_eq!(request["method"], "initialize");
        assert_eq!(request["params"]["capabilities"]["experimentalApi"], true);
        let reply = if initialize_error {
            json!({"id":request["id"],"error":{"code":-32603,"message":"假初始化失败"}})
        } else {
            json!({"id":request["id"],"result":{"userAgent":"本地假服务端"}})
        };
        socket
            .send(Message::Text(reply.to_string().into()))
            .unwrap();
        if !initialize_error {
            let initialized: Value =
                serde_json::from_str(socket.read().unwrap().to_text().unwrap()).unwrap();
            assert_eq!(initialized["method"], "initialized");
        }
        assert!(matches!(socket.read(), Ok(Message::Close(_))));
        assert!(matches!(
            socket.flush(),
            Ok(()) | Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed)
        ));
    })
}

#[cfg(windows)]
#[tokio::test]
async fn 直连共享成功保留路径与接入方式_初始化失败有阶段说明() {
    for fail in [false, true] {
        let temp = Temp::new();
        let path = temp.socket();
        let server = fake_shared(&path, fail);
        let connection = bounded(shared::connect_socket(&path, 1000)).await;
        if fail {
            assert!(connection.unwrap_err().contains("initialize 失败"));
        } else {
            let connection = connection.unwrap();
            assert_eq!(
                connection.server,
                ServerInfo::Shared {
                    socket_path: path.to_string_lossy().into_owned(),
                    via: SharedVia::Socket
                }
            );
            assert_eq!(connection.user_agent.as_deref(), Some("本地假服务端"));
            tokio::time::timeout(Duration::from_secs(1), connection.handle.close())
                .await
                .unwrap();
        }
        server.join().unwrap();
    }
}

#[cfg(windows)]
#[tokio::test]
async fn 不存在的_socket_快速失败并带_windows_错误码() {
    let temp = Temp::new();
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        shared::connect_socket(&temp.socket(), 1000),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(
        error.contains("连接") && error.contains("Windows 错误码") && !error.contains("不可用")
    );
}

#[cfg(windows)]
#[tokio::test]
async fn socket_握手前断开和_initialize_前取消均回收连接() {
    use std::io::Read;
    for cancel in [false, true] {
        let temp = Temp::new();
        let path = temp.socket();
        let listener = uds_windows::UnixListener::bind(&path).unwrap();
        let (seen, received) = oneshot::channel();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            if cancel {
                let mut bytes = [0; 4096];
                assert!(stream.read(&mut bytes).unwrap() > 0);
                seen.send(()).unwrap();
                assert_eq!(stream.read(&mut bytes).unwrap(), 0);
            }
        });
        let attempt = tokio::spawn(async move { shared::connect_socket(&path, 1000).await });
        if cancel {
            bounded(received).await.unwrap();
            attempt.abort();
            assert!(attempt.await.unwrap_err().is_cancelled());
        } else {
            assert!(bounded(attempt)
                .await
                .unwrap()
                .unwrap_err()
                .contains("WebSocket 握手失败"));
        }
        // 不阻塞当前单线程运行时：回收任务仍要被轮询。
        bounded(tokio::task::spawn_blocking(move || server.join().unwrap()))
            .await
            .unwrap();
    }
}

#[cfg(windows)]
#[tokio::test]
async fn 同_home_串行_不同_home_独立_短等待超时() {
    let temp = Temp::new();
    let first = init_lock::acquire(&temp.0, Duration::from_secs(1)).await;
    assert_eq!(first.acquired, init_lock::Acquired::Owned);
    let timed_out = init_lock::acquire(&temp.0, Duration::from_millis(30)).await;
    assert_eq!(timed_out.acquired, init_lock::Acquired::TimedOut);
    let other = init_lock::acquire(&temp.0.join("other"), Duration::from_millis(30)).await;
    assert_eq!(other.acquired, init_lock::Acquired::Owned);
    let home = temp.0.clone();
    let mut second =
        tokio::spawn(async move { init_lock::acquire(&home, Duration::from_secs(1)).await });
    assert!(tokio::time::timeout(Duration::from_millis(40), &mut second)
        .await
        .is_err());
    drop(first);
    assert_eq!(
        bounded(second).await.unwrap().acquired,
        init_lock::Acquired::Owned
    );
}

#[cfg(windows)]
#[tokio::test]
async fn 取消等待的_acquire_不抢占后续获取者() {
    let temp = Temp::new();
    let first = init_lock::acquire(&temp.0, Duration::from_secs(1)).await;
    let home = temp.0.clone();
    let second =
        tokio::spawn(async move { init_lock::acquire(&home, Duration::from_secs(1)).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    second.abort();
    assert!(second.await.unwrap_err().is_cancelled());
    drop(first);
    let third = init_lock::acquire(&temp.0, Duration::from_millis(200)).await;
    assert_eq!(third.acquired, init_lock::Acquired::Owned);
}

#[cfg(windows)]
#[tokio::test]
async fn 持有线程异常退出时能接管_abandoned_互斥量() {
    use windows_sys::Win32::Foundation::{CloseHandle, WAIT_OBJECT_0};
    use windows_sys::Win32::System::Threading::{CreateMutexW, WaitForSingleObject};
    let temp = Temp::new();
    let name: Vec<u16> = init_lock::mutex_name(&temp.0)
        .encode_utf16()
        .chain(Some(0))
        .collect();
    // 保留一个非持有句柄，避免原线程退出时内核对象消失。
    let handle = unsafe { CreateMutexW(std::ptr::null(), 0, name.as_ptr()) };
    assert!(!handle.is_null());
    let raw = handle as usize;
    std::thread::spawn(move || {
        assert_eq!(
            unsafe { WaitForSingleObject(raw as _, 1000) },
            WAIT_OBJECT_0
        );
    })
    .join()
    .unwrap();
    let next = init_lock::acquire(&temp.0, Duration::from_secs(1)).await;
    assert_eq!(next.acquired, init_lock::Acquired::Abandoned);
    drop(next);
    unsafe {
        CloseHandle(handle);
    }
}

#[test]
fn 互斥量名取绝对路径和十六位哈希() {
    let temp = Temp::new();
    let name = init_lock::mutex_name(&temp.0);
    assert_eq!(name.len(), "Local\\cxb-init-".len() + 16);
    assert_eq!(name, init_lock::mutex_name(&temp.0.join("")));
    #[cfg(windows)]
    assert_eq!(
        name,
        init_lock::mutex_name(Path::new(&format!(
            "{}/",
            temp.0.to_string_lossy().to_uppercase().replace('\\', "/")
        )))
    );
    assert_eq!(
        init_lock::mutex_name(Path::new("relative")),
        init_lock::mutex_name(&std::env::current_dir().unwrap().join("relative"))
    );
}
