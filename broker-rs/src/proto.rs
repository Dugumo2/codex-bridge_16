//! 线上消息：分类与构造（详细设计 2.1 节）。
//!
//! 线格式与上游一致：**不带** `"jsonrpc":"2.0"`；请求 `{id,method,params?}`，
//! 通知 `{method,params?}`，响应 `{id,result}` 或 `{id,error}`。
//! 透传部分一律保留为 `serde_json::Value`，只解析路由需要的字段（6.2 节「协议升级防护」）。

use serde_json::{json, Map, Value};

/// JSON-RPC 错误码（详细设计 2.1 节）。app-server 返回的错误码与 detail 原样透传，不在此列。
pub mod code {
    /// 解析失败，回包的 id 为 null。
    pub const PARSE_ERROR: i64 = -32700;
    /// 参数形状不正确。
    pub const INVALID_PARAMS: i64 = -32602;
    /// 方法不存在。
    pub const METHOD_NOT_FOUND: i64 = -32601;
    /// 处理异常，消息带 `Handler error:` 前缀。
    pub const INTERNAL_ERROR: i64 = -32603;
    /// 线程忙（同一线程已有认领），以及 `ifIdle` 时 broker 忙。
    pub const THREAD_BUSY: i64 = -32001;
    /// 一般错误；broker 私有扩展用 `data.reason` 给出具体原因。
    pub const GENERAL_ERROR: i64 = -32000;
}

/// 一条已解析为 JSON 的入站消息的分类。
///
/// 判定规则对照上游 processMessage（broker-server.ts 1629 行起）：上游用
/// `message.id !== undefined` 判断有无 id，所以 JSON 里显式写 `"id": null` 也算带了 id。
#[derive(Debug, Clone, PartialEq)]
pub enum Incoming {
    Request {
        id: Value,
        method: String,
        params: Option<Value>,
    },
    Notification {
        method: String,
        params: Option<Value>,
    },
    /// 对 broker 转发出去的请求的回包。`result` 与 `error` 的取舍由调用方按 1.2 节处理。
    Response {
        id: Value,
        result: Option<Value>,
        error: Option<Value>,
    },
    /// 能解析成 JSON 但不合规范（不是对象、method 不是字符串、既无 id 也无 method）。
    /// 上游对这类消息大多只记日志、不回复，这里同样处理（清单第 8 节第 4、17 条）。
    Invalid(Value),
}

impl Incoming {
    pub fn classify(value: Value) -> Self {
        let Value::Object(mut obj) = value else {
            return Incoming::Invalid(value);
        };
        let id = obj.remove("id");
        let params = obj.remove("params");
        match (obj.remove("method"), id) {
            (Some(Value::String(method)), Some(id)) => Incoming::Request { id, method, params },
            (Some(Value::String(method)), None) => Incoming::Notification { method, params },
            (None, Some(id)) => Incoming::Response {
                id,
                result: obj.remove("result"),
                error: obj.remove("error"),
            },
            (method, id) => {
                // 还原成原样，便于记日志排查。
                if let Some(m) = method {
                    obj.insert("method".into(), m);
                }
                if let Some(i) = id {
                    obj.insert("id".into(), i);
                }
                if let Some(p) = params {
                    obj.insert("params".into(), p);
                }
                Incoming::Invalid(Value::Object(obj))
            }
        }
    }
}

/// 构造错误对象：`data` 为 `None` 时不带 data 字段（与上游 buildJsonRpcError 一致）。
pub fn error_object(code: i64, message: impl Into<String>, data: Option<Value>) -> Value {
    let mut obj = Map::new();
    obj.insert("code".into(), json!(code));
    obj.insert("message".into(), Value::String(message.into()));
    if let Some(d) = data {
        obj.insert("data".into(), d);
    }
    Value::Object(obj)
}

pub fn response_ok(id: Value, result: Value) -> Value {
    json!({ "id": id, "result": result })
}

pub fn response_err(id: Value, error: Value) -> Value {
    json!({ "id": id, "error": error })
}

/// 解析失败的回包：id 为 null，消息格式与上游一致（`Invalid JSON: <原因>`）。
pub fn parse_error(reason: impl std::fmt::Display) -> Value {
    response_err(
        Value::Null,
        error_object(code::PARSE_ERROR, format!("Invalid JSON: {reason}"), None),
    )
}

pub fn notification(method: &str, params: Option<Value>) -> Value {
    let mut obj = Map::new();
    obj.insert("method".into(), Value::String(method.to_owned()));
    if let Some(p) = params {
        obj.insert("params".into(), p);
    }
    Value::Object(obj)
}

pub fn request(id: Value, method: &str, params: Option<Value>) -> Value {
    let mut obj = Map::new();
    obj.insert("id".into(), id);
    obj.insert("method".into(), Value::String(method.to_owned()));
    if let Some(p) = params {
        obj.insert("params".into(), p);
    }
    Value::Object(obj)
}

/// 序列化为一行线上文本（紧凑 JSON，末尾带 `\n`）。
pub fn to_line(message: &Value) -> String {
    let mut line = message.to_string();
    line.push('\n');
    line
}

/// 服务端请求 id 的比较键，与上游 rpc.ts 一致按 JavaScript `String(id)`：JSON 数字先按双精度数解析，
/// 所以 `7`、`7.0`、`"7"` 是同一个请求。rpc 层去重、`serverRequest/resolved` 撤销都必须用它，
/// 线上回答仍用原始 id。
pub fn request_id_key(id: &Value) -> String {
    match id {
        Value::String(text) => text.clone(),
        // 上游把 JSON 数字解析为双精度数，再按 JavaScript String(number) 比较。
        Value::Number(number) => javascript_number_key(number.as_f64().unwrap()),
        _ => id.to_string(),
    }
}

/// JavaScript 的 `String(number)`。
pub fn javascript_number_key(number: f64) -> String {
    if number == 0.0 {
        return "0".into();
    }
    let sign = if number < 0.0 { "-" } else { "" };
    // Rust 的 Display 给出最短十进制表示；根据 JS 的指数区间选择普通或科学计数法。
    let shortest = number.abs().to_string();
    let (mantissa, exponent) = shortest
        .split_once(['e', 'E'])
        .map_or((shortest.as_str(), 0), |(m, e)| {
            (m, e.parse::<i32>().unwrap())
        });
    let decimal = mantissa.find('.').unwrap_or(mantissa.len()) as i32;
    let mut digits = mantissa.replace('.', "");
    let leading = digits.bytes().take_while(|byte| *byte == b'0').count();
    let position = decimal + exponent - leading as i32;
    digits.drain(..leading);
    while digits.ends_with('0') {
        digits.pop();
    }
    if position > 0 && position <= 21 {
        if position as usize >= digits.len() {
            format!(
                "{sign}{digits}{}",
                "0".repeat(position as usize - digits.len())
            )
        } else {
            let (before, after) = digits.split_at(position as usize);
            format!("{sign}{before}.{after}")
        }
    } else if position > -6 && position <= 0 {
        format!("{sign}0.{}{digits}", "0".repeat((-position) as usize))
    } else {
        let exponent = position - 1;
        let suffix = if digits.len() == 1 {
            String::new()
        } else {
            format!(".{}", &digits[1..])
        };
        let exponent_sign = if exponent >= 0 { "+" } else { "" };
        format!("{sign}{}{suffix}e{exponent_sign}{exponent}", &digits[..1])
    }
}

/// JavaScript 的 `String(value)`：上游把客户端错误的 message 交给 `new Error(message)`，按此转成字符串。
pub fn javascript_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => javascript_number_key(number.as_f64().unwrap_or(0.0)),
        Value::Bool(flag) => flag.to_string(),
        Value::Null => "null".into(),
        // 数组按元素逐个转换、null 元素为空串、以逗号连接；对象一律是 [object Object]。
        Value::Array(items) => items
            .iter()
            .map(|item| match item {
                Value::Null => String::new(),
                other => javascript_string(other),
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 分类请求通知响应() {
        assert_eq!(
            Incoming::classify(
                json!({"id": 1, "method": "thread/read", "params": {"threadId": "t"}})
            ),
            Incoming::Request {
                id: json!(1),
                method: "thread/read".into(),
                params: Some(json!({"threadId": "t"}))
            }
        );
        assert_eq!(
            Incoming::classify(json!({"method": "initialized"})),
            Incoming::Notification {
                method: "initialized".into(),
                params: None
            }
        );
        assert_eq!(
            Incoming::classify(json!({"id": "broker-1-abc", "result": {"ok": true}})),
            Incoming::Response {
                id: json!("broker-1-abc"),
                result: Some(json!({"ok": true})),
                error: None
            }
        );
    }

    #[test]
    fn 显式_null_的_id_也算带了_id() {
        assert!(matches!(
            Incoming::classify(json!({"id": null, "method": "initialize"})),
            Incoming::Request {
                id: Value::Null,
                ..
            }
        ));
    }

    #[test]
    fn 不合规范的消息归为_invalid() {
        for v in [
            json!(null),
            json!([1, 2]),
            json!(123),
            json!({"method": 5, "id": 1}),
            json!({"params": {}}),
        ] {
            assert!(matches!(Incoming::classify(v), Incoming::Invalid(_)));
        }
    }

    #[test]
    fn 构造的消息不带_jsonrpc_字段() {
        let line = to_line(&response_ok(json!(7), json!({})));
        assert_eq!(line, "{\"id\":7,\"result\":{}}\n");
        assert!(!to_line(&notification("turn/started", None)).contains("jsonrpc"));
    }

    #[test]
    fn 错误对象没有_data_时不带该字段() {
        assert_eq!(
            error_object(code::THREAD_BUSY, "busy", None),
            json!({"code": -32001, "message": "busy"})
        );
        assert_eq!(
            error_object(code::GENERAL_ERROR, "x", Some(json!({"reason": "joined"}))),
            json!({"code": -32000, "message": "x", "data": {"reason": "joined"}})
        );
    }

    #[test]
    fn 解析失败回包的_id_为_null() {
        let v = parse_error("Unexpected token");
        assert_eq!(v["id"], Value::Null);
        assert_eq!(v["error"]["code"], json!(-32700));
        assert_eq!(
            v["error"]["message"],
            json!("Invalid JSON: Unexpected token")
        );
    }
}
