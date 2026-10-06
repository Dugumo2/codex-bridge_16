//! 行帧：每行一条 UTF-8 JSON，以 `\n` 分隔（详细设计 2.1 节）。
//!
//! 规则（对照上游 broker-server.ts 1932–1947 行）：
//! - 按行 trim，忽略空行；
//! - 在 `\n` 字节处切分再逐行解码，所以跨读块的多字节字符会先拼好再解码；
//!   非法 UTF-8 用替换字符解码，与上游 `setEncoding("utf8")` 的行为一致；
//! - EOF 时没有换行的尾巴直接丢弃（调用方丢弃 [`LineFramer`] 即可）；
//! - 未换行的数据累计超过 [`MAX_PENDING_BYTES`] 就报溢出，调用方断开连接。
//!   上游按 UTF-16 字符数、在切行之前检查整块；这里按 UTF-8 字节数、只计未换行部分，
//!   是设计 2.2 节记录的有意偏离（测试 54 只发 ASCII，不受影响）。

use thiserror::Error;

/// 单连接未换行数据的上限：10 MiB（上游 rpc.ts:22）。
pub const MAX_PENDING_BYTES: usize = 10 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("连接上未换行的数据超过 {limit} 字节")]
pub struct FrameOverflow {
    pub limit: usize,
}

#[derive(Debug)]
pub struct LineFramer {
    pending: Vec<u8>,
    /// `pending` 中已确认没有 `\n` 的前缀长度，避免每次从头扫描（大块无换行输入时为线性开销）。
    scanned: usize,
    limit: usize,
}

impl Default for LineFramer {
    fn default() -> Self {
        Self::with_limit(MAX_PENDING_BYTES)
    }
}

impl LineFramer {
    pub fn with_limit(limit: usize) -> Self {
        Self {
            pending: Vec::new(),
            scanned: 0,
            limit,
        }
    }

    /// 追加一块读到的字节，返回其中完整的非空行（已 trim）。
    /// 返回溢出错误时，本块中的行一并丢弃，调用方应断开连接。
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<String>, FrameOverflow> {
        self.pending.extend_from_slice(chunk);
        let mut lines = Vec::new();
        let mut consumed = 0;
        let mut search_from = self.scanned;
        while let Some(offset) = self.pending[search_from..].iter().position(|&b| b == b'\n') {
            let end = search_from + offset;
            let text = String::from_utf8_lossy(&self.pending[consumed..end]);
            let line = text.trim();
            if !line.is_empty() {
                lines.push(line.to_owned());
            }
            consumed = end + 1;
            search_from = consumed;
        }
        self.pending.drain(..consumed);
        self.scanned = self.pending.len();
        if self.pending.len() > self.limit {
            return Err(FrameOverflow { limit: self.limit });
        }
        Ok(lines)
    }

    /// 尚未换行的字节数。
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 按换行切分并忽略空行和首尾空白() {
        let mut f = LineFramer::default();
        let lines = f.push(b"  {\"a\":1}  \n\n\r\n{\"b\":2}\r\n").unwrap();
        assert_eq!(lines, vec!["{\"a\":1}", "{\"b\":2}"]);
        assert_eq!(f.pending_len(), 0);
    }

    #[test]
    fn 跨块的行和多字节字符能拼好() {
        let mut f = LineFramer::default();
        let text = "{\"msg\":\"中文\"}\n".as_bytes();
        // 在「中」字的三个字节中间切开
        let cut = text.iter().position(|&b| b >= 0x80).unwrap() + 1;
        assert!(f.push(&text[..cut]).unwrap().is_empty());
        assert_eq!(f.push(&text[cut..]).unwrap(), vec!["{\"msg\":\"中文\"}"]);
    }

    #[test]
    fn 未换行的尾巴保留到下一块() {
        let mut f = LineFramer::default();
        assert_eq!(f.push(b"{\"a\":1}\n{\"b\"").unwrap(), vec!["{\"a\":1}"]);
        assert_eq!(f.pending_len(), "{\"b\"".len());
        assert_eq!(f.push(b":2}\n").unwrap(), vec!["{\"b\":2}"]);
    }

    #[test]
    fn 未换行数据超过上限时报溢出() {
        let mut f = LineFramer::with_limit(8);
        assert!(f.push(b"12345678").unwrap().is_empty());
        assert_eq!(f.push(b"9"), Err(FrameOverflow { limit: 8 }));
    }

    #[test]
    fn 已换行的数据不计入上限() {
        let mut f = LineFramer::with_limit(8);
        let lines = f.push(b"0123456789abcdef\n1234").unwrap();
        assert_eq!(lines, vec!["0123456789abcdef"]);
        assert_eq!(f.pending_len(), 4);
    }

    #[test]
    fn 非法_utf8_用替换字符解码() {
        let mut f = LineFramer::default();
        let lines = f.push(b"\xff{}\n").unwrap();
        assert_eq!(lines, vec!["\u{fffd}{}"]);
    }

    #[test]
    fn 十兆无换行输入分块推送开销为线性() {
        let mut f = LineFramer::default();
        let block = vec![b'x'; 64 * 1024];
        let mut overflowed = false;
        for _ in 0..170 {
            if f.push(&block).is_err() {
                overflowed = true;
                break;
            }
        }
        assert!(overflowed, "超过 10 MiB 时应报溢出");
    }
}
