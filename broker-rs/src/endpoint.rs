//! `--endpoint` 参数：只接受 `unix:<路径>` 与 `pipe:<名字>` 两种前缀（详细设计 2.1 节，
//! 与上游 src/broker.ts 的 parseEndpoint 一致）。Windows 上 pipe 的值是完整的
//! `\\.\pipe\<名字>` 路径，原样用于创建命名管道。

use std::path::PathBuf;

use crate::error::BrokerError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    Unix(PathBuf),
    Pipe(String),
}

impl Endpoint {
    pub fn parse(raw: &str) -> Result<Self, BrokerError> {
        let (rest, make): (&str, fn(&str) -> Endpoint) =
            if let Some(rest) = raw.strip_prefix("unix:") {
                (rest, |p| Endpoint::Unix(PathBuf::from(p)))
            } else if let Some(rest) = raw.strip_prefix("pipe:") {
                (rest, |p| Endpoint::Pipe(p.to_owned()))
            } else {
                return Err(BrokerError::EndpointPrefix(raw.to_owned()));
            };
        if rest.is_empty() {
            return Err(BrokerError::EndpointEmpty(raw.to_owned()));
        }
        Ok(make(rest))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 解析管道端点保留完整路径() {
        let ep = Endpoint::parse(r"pipe:\\.\pipe\cxb-test-1-abc").unwrap();
        assert_eq!(ep, Endpoint::Pipe(r"\\.\pipe\cxb-test-1-abc".to_owned()));
    }

    #[test]
    fn 解析_unix_端点() {
        let ep = Endpoint::parse("unix:/tmp/x/broker.sock").unwrap();
        assert_eq!(ep, Endpoint::Unix(PathBuf::from("/tmp/x/broker.sock")));
    }

    #[test]
    fn 拒绝空路径和未知前缀() {
        assert!(matches!(
            Endpoint::parse("pipe:"),
            Err(BrokerError::EndpointEmpty(_))
        ));
        assert!(matches!(
            Endpoint::parse("unix:"),
            Err(BrokerError::EndpointEmpty(_))
        ));
        assert!(matches!(
            Endpoint::parse("tcp:127.0.0.1:1"),
            Err(BrokerError::EndpointPrefix(_))
        ));
        assert!(matches!(
            Endpoint::parse(""),
            Err(BrokerError::EndpointPrefix(_))
        ));
    }
}
