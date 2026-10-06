use thiserror::Error;

/// broker 进程级错误：导致 `serve` 或 `launch` 无法继续、需要以非零码退出的情况。
/// 单条消息的处理错误不走这里，而是按 JSON-RPC 错误码回给对端（见 [`crate::proto::code`]）。
#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("无效的端点「{0}」：必须以 unix: 或 pipe: 开头")]
    EndpointPrefix(String),
    #[error("无效的端点「{0}」：路径为空")]
    EndpointEmpty(String),
    /// Windows 上只用命名管道（详细设计 2.4 节「坑点」第 21、22、42 条不适用）。
    #[error("当前平台不支持端点「{0}」，Windows 上请用 pipe:")]
    UnsupportedEndpoint(String),
    /// launch 两次创建 serve 都失败（详细设计第 5 节）。
    #[error("拉起 serve 失败：{0}")]
    Launch(#[from] crate::launch::LaunchFailed),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
