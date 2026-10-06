//! 第 1 档：直连 Codex 共享 app-server 的 AF_UNIX 控制 socket（详细设计 6.2 节，S3-环境验证.md
//! uds_try_clone 全部通过）。
//!
//! 传输约定：
//! - Winsock 非阻塞连接在异步任务里轮询，避免留下不可取消的同步 connect 线程。连接成功后
//!   交给 uds_windows，`try_clone` 出读写两个句柄，各一个专用线程，把字节搬进、搬出
//!   `tokio::io::duplex` 的一端，另一端交给 [`super::ws::client_over`]。
//!   桥接只搬字节、不碰帧，所以不存在两个线程交错写帧的问题；
//! - socket 始终非阻塞（克隆句柄共享模式）；收发遇到 WouldBlock 后以 20 毫秒的 select 等待就绪，
//!   每次醒来检查停止信号。任一方向结束都发布停止并 shutdown(Both)，不依赖 shutdown 唤醒阻塞 send；
//! - **连接阶段本身必须有界**（活性预审第 3 条）：调用方 3 秒后取消 future 时，不能只丢弃结果接收端、
//!   等同步 `connect` 自己返回。用非阻塞连接加有界等待（例如 socket 设为非阻塞后 `connect`，
//!   WSAEWOULDBLOCK 时用 `select`/`WSAPoll` 等到期限），或其他能在期限内让连接线程退出的办法；
//!   连接线程在期限后连上时也要自行关闭 socket。测试要扣住**连接阶段**（例如 listener 不 accept、
//!   backlog 占满）验证线程退出，不只取消已连接后的握手；
//! - 不用 `Path::exists` 预判 socket 是否存在：Windows 上 AF_UNIX 重解析点的元数据不可靠
//!   （S3-验证记录 M0：bun 的 existsSync 对它返回 false），一律直接连接、按错误判定；
//! - 握手（[`super::initialize`]）成功才算这一档成功，`ServerInfo::Shared{socket_path, via: Socket}`。

use std::path::Path;

use super::AppConnection;

/// 连接 `socket_path` 上的共享 app-server 并完成握手。失败返回 Err(给人看的原因)。
pub async fn connect_socket(
    socket_path: &Path,
    request_timeout_ms: u64,
) -> Result<AppConnection, String> {
    #[cfg(windows)]
    {
        let stream = windows::connect(socket_path).await.map_err(|error| {
            format!(
                "连接 {} 失败：{error}（Windows 错误码：{}）",
                socket_path.display(),
                error
                    .raw_os_error()
                    .map_or_else(|| "不可用".into(), |code| code.to_string())
            )
        })?;
        let (stream, bridge) = windows::bridge(stream)
            .await
            .map_err(|error| format!("创建 socket 字节桥接失败：{error}"))?;
        let (client, events) = super::ws::client_over(
            stream,
            move || async move {
                bridge.finish().await;
            },
            Some(request_timeout_ms),
        )
        .await?;
        let mut guard = super::ws::StartupGuard(Some(client.clone()));
        let user_agent = match super::initialize(&client).await {
            Ok(agent) => agent,
            Err(error) => {
                client.close().await;
                return Err(format!("共享 socket initialize 失败：{error}"));
            }
        };
        guard.0 = None;
        Ok(AppConnection {
            handle: super::AppHandle { inner: client },
            events,
            user_agent,
            server: super::ServerInfo::Shared {
                socket_path: socket_path.to_string_lossy().into_owned(),
                via: super::SharedVia::Socket,
            },
        })
    }
    #[cfg(not(windows))]
    {
        let _ = (socket_path, request_timeout_ms);
        Err("当前平台未实现共享 AF_UNIX 字节桥接".into())
    }
}

#[cfg(windows)]
mod windows {
    use std::io::{self, Read, Write};
    use std::net::Shutdown;
    use std::os::windows::io::{AsRawSocket, FromRawSocket};
    use std::path::Path;
    use std::sync::{Arc, OnceLock};
    use std::thread::JoinHandle;
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
    use tokio::sync::watch;
    use uds_windows::UnixStream;
    use windows_sys::Win32::Networking::WinSock::*;

    const POLL_INTERVAL: Duration = Duration::from_millis(10);
    /// 专用线程等待 socket 就绪的最长单次间隔；取消不依赖对端继续收发。
    const IO_POLL_MICROS: i32 = 20_000;
    /// 关闭时最多等桥接线程多久。
    const FINISH_LIMIT: Duration = Duration::from_secs(1);

    /// 非阻塞连接直接由异步任务拥有，取消 future 当场关闭 socket，不产生连接线程。
    pub(super) async fn connect(path: &Path) -> io::Result<UnixStream> {
        static START: OnceLock<i32> = OnceLock::new();
        let startup = *START.get_or_init(|| unsafe { WSAStartup(0x202, &mut std::mem::zeroed()) });
        if startup != 0 {
            return Err(io::Error::from_raw_os_error(startup));
        }
        let path = path.to_str().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "socket 路径不是有效 UTF-8")
        })?;
        if path.is_empty() || path.as_bytes().contains(&0) || path.len() >= 108 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "socket 路径必须为 1–107 字节且不能包含空字符",
            ));
        }
        let mut address: SOCKADDR_UN = unsafe { std::mem::zeroed() };
        address.sun_family = AF_UNIX;
        for (dst, src) in address.sun_path.iter_mut().zip(path.bytes()) {
            *dst = src as _;
        }
        let raw = unsafe {
            WSASocketW(
                AF_UNIX as i32,
                SOCK_STREAM,
                0,
                std::ptr::null(),
                0,
                WSA_FLAG_OVERLAPPED | WSA_FLAG_NO_HANDLE_INHERIT,
            )
        };
        if raw == INVALID_SOCKET {
            return Err(last_error());
        }
        // 从此所有失败与取消出口都由 UnixStream 关闭唯一句柄。
        let stream = unsafe { UnixStream::from_raw_socket(raw as _) };
        stream.set_nonblocking(true)?;
        let result = unsafe {
            windows_sys::Win32::Networking::WinSock::connect(
                raw,
                &address as *const _ as _,
                (2 + path.len() + 1) as i32,
            )
        };
        if result == SOCKET_ERROR {
            let error = last_error();
            if !matches!(
                error.raw_os_error(),
                Some(WSAEWOULDBLOCK | WSAEINPROGRESS | WSAEALREADY)
            ) {
                return Err(error);
            }
            wait_connected(&stream, connected).await?;
        }
        Ok(stream)
    }

    fn last_error() -> io::Error {
        io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
    }

    async fn wait_connected(
        stream: &UnixStream,
        mut poll: impl FnMut(&UnixStream) -> io::Result<bool>,
    ) -> io::Result<()> {
        tokio::time::timeout(super::super::select::TIER_TIMEOUT, async {
            loop {
                // select 的 timeout 为零：不在运行时工作线程上阻塞。
                if poll(stream)? {
                    return Ok(());
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "共享 socket 连接超过 3 秒"))?
    }

    fn connected(stream: &UnixStream) -> io::Result<bool> {
        let raw = stream.as_raw_socket() as SOCKET;
        let mut ready: FD_SET = unsafe { std::mem::zeroed() };
        ready.fd_count = 1;
        ready.fd_array[0] = raw;
        let mut failed = ready;
        let zero = TIMEVAL {
            tv_sec: 0,
            tv_usec: 0,
        };
        let result = unsafe { select(0, std::ptr::null_mut(), &mut ready, &mut failed, &zero) };
        if result == SOCKET_ERROR {
            return Err(last_error());
        }
        if result == 0 {
            return Ok(false);
        }
        match stream.take_error()? {
            Some(error) => Err(error),
            None if ready.fd_count != 0 => Ok(true),
            None => Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "共享 socket 连接失败",
            )),
        }
    }

    struct BridgeState {
        socket: UnixStream,
        stop: watch::Sender<bool>,
        #[cfg(test)]
        observed: TestObserver,
    }

    #[cfg(test)]
    struct TestObserver {
        send_blocked: watch::Sender<bool>,
        duplex_blocked: watch::Sender<bool>,
        joined: std::sync::atomic::AtomicUsize,
    }

    impl BridgeState {
        fn stop(&self) {
            self.stop.send_replace(true);
            let _ = self.socket.shutdown(Shutdown::Both);
        }
    }

    /// 每连接至多两个线程；取消、任一方向结束、初始化失败与正常关闭共用此守卫。
    pub(super) struct Bridge {
        state: Arc<BridgeState>,
        threads: Vec<JoinHandle<()>>,
    }

    impl Bridge {
        pub(super) async fn finish(self) {
            self.state.stop();
            // socket 等待每 20 毫秒检查停止；watch 立即唤醒 duplex 上的等待。
            let deadline = tokio::time::Instant::now() + FINISH_LIMIT;
            while self.threads.iter().any(|thread| !thread.is_finished()) {
                if tokio::time::Instant::now() >= deadline {
                    // 仅防御异常调度或系统调用故障；正常取消路径必须真正退出并被 join。
                    tracing::warn!("socket 桥接线程 1 秒内未退出，放弃等待");
                    return;
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
            // 此时 join 不再等待 I/O，也不会阻塞运行时。
            let mut this = self;
            for thread in this.threads.drain(..) {
                if thread.join().is_err() {
                    tracing::warn!("socket 桥接线程异常退出");
                }
                #[cfg(test)]
                this.state
                    .observed
                    .joined
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
    }

    impl Drop for Bridge {
        fn drop(&mut self) {
            self.state.stop();
        }
    }

    struct Direction(Arc<BridgeState>);
    impl Drop for Direction {
        fn drop(&mut self) {
            self.0.stop();
        }
    }

    pub(super) async fn bridge(socket: UnixStream) -> io::Result<(DuplexStream, Bridge)> {
        // 对原句柄设置一次，所有 try_clone 句柄共享非阻塞模式；两个方向都处理 WouldBlock。
        socket.set_nonblocking(true)?;
        let mut reader = socket.try_clone()?;
        let mut writer = socket.try_clone()?;
        // 不设 SO_RCVTIMEO/SO_SNDTIMEO：等待的是就绪事件，收发本身不会阻塞或超时后重用连接。
        let (stop, _) = watch::channel(false);
        let state = Arc::new(BridgeState {
            socket,
            stop,
            #[cfg(test)]
            observed: TestObserver {
                send_blocked: watch::channel(false).0,
                duplex_blocked: watch::channel(false).0,
                joined: std::sync::atomic::AtomicUsize::new(0),
            },
        });
        let mut bridge = Bridge {
            state: state.clone(),
            threads: Vec::with_capacity(2),
        };
        let (client, server) = tokio::io::duplex(64 * 1024);
        let (mut source, mut sink) = tokio::io::split(server);
        let runtime = tokio::runtime::Handle::current();
        let read_runtime = runtime.clone();
        let read_state = Direction(state.clone());
        let mut read_stop = state.stop.subscribe();
        bridge.threads.push(std::thread::Builder::new().name("cxb-socket-read".into()).spawn(move || {
            let _guard = read_state;
            let mut bytes = [0; 8192];
            while !*read_stop.borrow() {
                match reader.read(&mut bytes) {
                    Ok(0) => break,
                    Ok(n) => {
                        let result = read_runtime.block_on(async {
                            let write = sink.write_all(&bytes[..n]);
                            #[cfg(test)]
                            let write = async {
                                use std::future::Future;
                                tokio::pin!(write);
                                std::future::poll_fn(|cx| {
                                    let result = write.as_mut().poll(cx);
                                    if result.is_pending() {
                                        _guard.0.observed.duplex_blocked.send_replace(true);
                                    }
                                    result
                                }).await
                            };
                            tokio::select! {
                                biased;
                                _ = read_stop.wait_for(|stop| *stop) => Err(io::Error::from(io::ErrorKind::Interrupted)),
                                result = write => result,
                            }
                        });
                        if result.is_err() { break; }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        if !matches!(wait_ready(&reader, false, &read_stop), Ok(true)) { break; }
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(_) => break,
                }
            }
        })?);
        let write_state = Direction(state.clone());
        let mut write_stop = state.stop.subscribe();
        let writing = std::thread::Builder::new()
            .name("cxb-socket-write".into())
            .spawn(move || {
                let _guard = write_state;
                let mut bytes = [0; 8192];
                loop {
                    let result = runtime.block_on(async {
                        tokio::select! {
                            biased;
                            _ = write_stop.wait_for(|stop| *stop) => Ok(0),
                            result = source.read(&mut bytes) => result,
                        }
                    });
                    let n = match result {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    let mut offset = 0;
                    while offset < n && !*write_stop.borrow() {
                        match writer.write(&bytes[offset..n]) {
                            Ok(0) => return,
                            Ok(written) => offset += written,
                            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                                #[cfg(test)]
                                _guard.0.observed.send_blocked.send_replace(true);
                                if !matches!(wait_ready(&writer, true, &write_stop), Ok(true)) {
                                    return;
                                }
                            }
                            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                            Err(_) => return,
                        }
                    }
                }
            });
        match writing {
            Ok(thread) => bridge.threads.push(thread),
            Err(error) => {
                drop(client);
                bridge.finish().await;
                return Err(error);
            }
        }
        Ok((client, bridge))
    }

    /// 返回 false 表示取消，只有可读/可写才返回 true；错误由调用方结束整个桥接。
    fn wait_ready(
        socket: &UnixStream,
        writing: bool,
        stop: &watch::Receiver<bool>,
    ) -> io::Result<bool> {
        loop {
            if *stop.borrow() {
                return Ok(false);
            }
            let mut ready: FD_SET = unsafe { std::mem::zeroed() };
            ready.fd_count = 1;
            ready.fd_array[0] = socket.as_raw_socket() as SOCKET;
            let timeout = TIMEVAL {
                tv_sec: 0,
                tv_usec: IO_POLL_MICROS,
            };
            let (read, write) = if writing {
                (std::ptr::null_mut(), &mut ready as *mut _)
            } else {
                (&mut ready as *mut _, std::ptr::null_mut())
            };
            let result = unsafe { select(0, read, write, std::ptr::null_mut(), &timeout) };
            if *stop.borrow() {
                return Ok(false);
            }
            if result == SOCKET_ERROR {
                let error = last_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            if result > 0 {
                return Ok(true);
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[tokio::test(start_paused = true)]
        async fn 连接阶段持续未就绪时有三秒期限且取消当场释放_socket() {
            for cancel in [false, true] {
                let (stream, mut peer) = UnixStream::pair().unwrap();
                peer.set_read_timeout(Some(Duration::from_millis(200)))
                    .unwrap();
                let (seen, received) = tokio::sync::oneshot::channel();
                let attempt = tokio::spawn(async move {
                    let mut seen = Some(seen);
                    // 在非阻塞连接完成的判定处扣住，不进入字节桥接或 WebSocket 握手。
                    wait_connected(&stream, |_| {
                        if let Some(seen) = seen.take() {
                            let _ = seen.send(());
                        }
                        Ok(false)
                    })
                    .await
                });
                received.await.unwrap();
                if cancel {
                    attempt.abort();
                    assert!(attempt.await.unwrap_err().is_cancelled());
                } else {
                    tokio::time::advance(Duration::from_secs(3)).await;
                    assert_eq!(
                        attempt.await.unwrap().unwrap_err().kind(),
                        io::ErrorKind::TimedOut
                    );
                }
                assert_eq!(peer.read(&mut [0; 1]).unwrap(), 0);
            }
        }

        async fn signal(mut signal: watch::Receiver<bool>) {
            tokio::time::timeout(Duration::from_secs(4), async {
                signal.wait_for(|value| *value).await.unwrap();
            })
            .await
            .expect("必须观察到实际背压或停止信号");
        }

        async fn assert_exited(threads: &[JoinHandle<()>]) {
            assert_eq!(threads.len(), 2);
            tokio::time::timeout(Duration::from_millis(200), async {
                while threads.iter().any(|thread| !thread.is_finished()) {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("两个桥接线程应在 200 毫秒内真正退出");
            assert!(threads.iter().all(JoinHandle::is_finished));
        }

        fn small_send_buffer(socket: &UnixStream) {
            let size: i32 = 4096;
            assert_eq!(
                unsafe {
                    setsockopt(
                        socket.as_raw_socket() as _,
                        SOL_SOCKET,
                        SO_SNDBUF,
                        &size as *const _ as _,
                        std::mem::size_of_val(&size) as i32,
                    )
                },
                0
            );
        }

        #[tokio::test]
        async fn 普通关闭直接确认两个线程退出并且_finish_完成_join() {
            let (socket, _peer) = UnixStream::pair().unwrap();
            let (stream, bridge) = bridge(socket).await.unwrap();
            let state = bridge.state.clone();
            drop(stream);
            assert_exited(&bridge.threads).await;
            bridge.finish().await;
            assert_eq!(
                state
                    .observed
                    .joined
                    .load(std::sync::atomic::Ordering::SeqCst),
                2
            );
        }

        #[tokio::test]
        async fn 读线程确认_duplex_背压后取消_两线程在二百毫秒内退出() {
            let (socket, mut peer) = UnixStream::pair().unwrap();
            peer.set_nonblocking(true).unwrap();
            let (stream, mut bridge) = bridge(socket).await.unwrap();
            let state = bridge.state.clone();
            // 不消费 stream；等读线程实际 poll_write 返回 Pending，不能靠睡眠猜测。
            tokio::time::timeout(Duration::from_secs(4), async {
                while !*state.observed.duplex_blocked.borrow() {
                    match peer.write(&[b'x'; 8192]) {
                        Ok(n) => assert!(n > 0),
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                        Err(error) => panic!("填充读方向失败：{error}"),
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let threads = std::mem::take(&mut bridge.threads);
            drop(bridge);
            assert_exited(&threads).await;
            for thread in threads {
                thread.join().unwrap();
            }
            // 验证退出时对端和 duplex 的另一端仍持有，回收不依赖它们关闭。
            drop((stream, peer));
        }

        #[tokio::test]
        async fn 对端不读_send_返回_wouldblock_后取消仍退出两个线程() {
            let (socket, peer) = UnixStream::pair().unwrap();
            small_send_buffer(&socket);
            let (mut stream, mut bridge) = bridge(socket).await.unwrap();
            let blocked = bridge.state.observed.send_blocked.subscribe();
            let pump =
                tokio::spawn(
                    async move { while stream.write_all(&[b'x'; 65536]).await.is_ok() {} },
                );
            signal(blocked).await;
            let threads = std::mem::take(&mut bridge.threads);
            drop(bridge);
            assert_exited(&threads).await;
            for thread in threads {
                thread.join().unwrap();
            }
            tokio::time::timeout(Duration::from_millis(200), pump)
                .await
                .unwrap()
                .unwrap();
            drop(peer);
        }

        #[tokio::test]
        async fn 对端不读_send_背压期间_rpc_关闭后_finish_确实_join_两线程() {
            let (socket, peer) = UnixStream::pair().unwrap();
            small_send_buffer(&socket);
            let (stream, bridge) = bridge(socket).await.unwrap();
            let state = bridge.state.clone();
            let blocked = state.observed.send_blocked.subscribe();
            let stopped = state.stop.subscribe();
            // 只完成 WebSocket 握手，之后一直保留对端但完全不读取。
            let server = tokio::task::spawn_blocking(move || {
                tokio_tungstenite::tungstenite::accept(peer).unwrap()
            });
            let (client, _) = super::super::super::ws::client_over(
                stream,
                move || async move {
                    bridge.finish().await;
                },
                None,
            )
            .await
            .unwrap();
            let peer = server.await.unwrap();
            client.notify(
                "填满发送缓冲",
                Some(serde_json::json!("x".repeat(4 * 1024 * 1024))),
            );
            signal(blocked).await;
            // 保留正常 RPC 排空、WS Close 的期限；200 毫秒从桥接停止信号发出时计起。
            let closed = tokio::spawn(async move { client.close().await });
            signal(stopped).await;
            tokio::time::timeout(Duration::from_millis(200), closed)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                state
                    .observed
                    .joined
                    .load(std::sync::atomic::Ordering::SeqCst),
                2
            );
            drop(peer);
        }
    }
}
