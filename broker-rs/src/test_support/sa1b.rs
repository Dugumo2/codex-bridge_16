//! SA1b 可复用协议夹具：不依赖 Machine 私有状态；单测或集成测试均可按 path 引入。
use serde_json::{json, Value};
use std::collections::{BTreeMap, VecDeque};

#[derive(Debug, Default)]
pub struct EventFence {
    held: BTreeMap<String, VecDeque<Value>>,
    delivered: Vec<Value>,
}

impl EventFence {
    pub fn emit(&mut self, message: Value, fence: Option<&str>) {
        if let Some(fence) = fence {
            self.held
                .entry(fence.into())
                .or_default()
                .push_back(message);
        } else {
            self.delivered.push(message);
        }
    }

    /// 放行不重复；由测试调用方将返回的协议消息注入真实状态机。
    pub fn release(&mut self, fence: &str) -> Vec<Value> {
        let messages: Vec<_> = self
            .held
            .remove(fence)
            .unwrap_or_default()
            .into_iter()
            .collect();
        self.delivered.extend(messages.iter().cloned());
        messages
    }

    pub fn delivered(&self) -> &[Value] {
        &self.delivered
    }
}

/// 故意让服务端条目 id 与客户端 clientId 不同，防止测试误授信。
pub fn user_message(item_id: &str, client_id: &str, text: &str) -> Value {
    json!({"type":"userMessage","id":item_id,"clientId":client_id,
        "content":[{"type":"text","text":text}]})
}

pub fn final_answer(item_id: &str, text: &str) -> Value {
    json!({"type":"agentMessage","id":item_id,"text":text,"phase":"final_answer"})
}

pub fn turn(thread: &str, turn: &str, started: bool, status: &str, items: Vec<Value>) -> Value {
    json!({"method":if started {"turn/started"} else {"turn/completed"},
        "params":{"threadId":thread,"turn":{"id":turn,"status":status,"items":items,"error":null}}})
}

/// 父轮与孙代理父轮使用同一函数；v1 起始不含接收方，v2 成功后双通知。
pub fn spawn(parent: &str, parent_turn: &str, child: &str, item: &str, v2: bool) -> Vec<Value> {
    let completed = if v2 {
        json!({"type":"subAgentActivity","id":item,"kind":"started","agentThreadId":child,"agentPath":format!("/root/{child}")})
    } else {
        json!({"type":"collabAgentToolCall","id":item,"tool":"spawnAgent","status":"completed",
            "senderThreadId":parent,"receiverThreadIds":[child],"agentsStates":{}})
    };
    let mut started = completed.clone();
    if !v2 {
        started["status"] = json!("inProgress");
        started["receiverThreadIds"] = json!([]);
    }
    [("item/started", started), ("item/completed", completed)]
        .into_iter()
        .map(|(method, item)| json!({"method":method,"params":{"threadId":parent,"turnId":parent_turn,"item":item}}))
        .collect()
}

pub fn usage(thread: &str, turn: &str, input: u64, cached: u64, output: u64) -> Value {
    let value = json!({"inputTokens":input,"cachedInputTokens":cached,"outputTokens":output,
        "reasoningOutputTokens":0,"totalTokens":input+output});
    json!({"method":"thread/tokenUsage/updated","params":{"threadId":thread,"turnId":turn,
        "tokenUsage":{"total":value,"last":value,"modelContextWindow":128000}}})
}

pub fn goal(thread: &str, status: &str, budget: u64, used: u64) -> Value {
    json!({"method":"thread/goal/updated","params":{"threadId":thread,"turnId":null,
        "goal":{"threadId":thread,"objective":"测试目标","status":status,"tokenBudget":budget,
        "tokensUsed":used,"timeUsedSeconds":0,"createdAt":1,"updatedAt":1}}})
}

/// 按页返回完整条目，不用 summary 代替 full；用于 clientId 后页核实与字节上限测试。
pub fn page(items: &[Value], offset: usize, limit: usize) -> Value {
    assert!(limit > 0, "分页大小必须非零");
    let end = offset.saturating_add(limit).min(items.len());
    let data = items.get(offset..end).unwrap_or_default();
    json!({"data":data,"nextCursor":if end < items.len() { Some(end.to_string()) } else { None },"backwardsCursor":null})
}
