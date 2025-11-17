use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JsonRpcMessage {
    Request(JsonRpcRequest),
    Response(JsonRpcResponse),
    Notification(JsonRpcNotification),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: u64,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcNotification {
    pub jsonrpc: String,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

pub struct RequestIdManager {
    next_id: Arc<Mutex<u64>>,
    pending_requests: Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcResponse>>>>,
}

impl RequestIdManager {
    pub fn new() -> Self {
        Self {
            next_id: Arc::new(Mutex::new(1)),
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn allocate_id(&self) -> u64 {
        let mut next_id = self.next_id.lock().unwrap();
        let id = *next_id;
        *next_id += 1;
        id
    }

    pub fn register_request(&self, id: u64, sender: oneshot::Sender<JsonRpcResponse>) {
        self.pending_requests.lock().unwrap().insert(id, sender);
    }

    pub fn complete_request(
        &self,
        id: u64,
        response: JsonRpcResponse,
    ) -> Result<(), JsonRpcResponse> {
        let mut pending = self.pending_requests.lock().unwrap();
        if let Some(sender) = pending.remove(&id) {
            sender.send(response).map_err(|_| {
                // チャネルが閉じられている場合は無視
                JsonRpcResponse {
                    jsonrpc: "2.0".to_string(),
                    id,
                    result: None,
                    error: Some(JsonRpcError {
                        code: -32603,
                        message: "Internal error: channel closed".to_string(),
                        data: None,
                    }),
                }
            })
        } else {
            Err(response)
        }
    }

    pub fn cancel_all(&self) {
        let mut pending = self.pending_requests.lock().unwrap();
        pending.clear();
    }
}

impl Default for RequestIdManager {
    fn default() -> Self {
        Self::new()
    }
}
