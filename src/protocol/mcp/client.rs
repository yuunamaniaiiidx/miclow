use anyhow::{Context, Result};
use serde_json::Value;
use tokio::sync::oneshot;
use tokio::time::{timeout, Duration};
use super::jsonrpc::{JsonRpcMessage, JsonRpcRequest, JsonRpcResponse, RequestIdManager};
use super::stdio::StdioTransport;
use super::types::*;
use tokio::process::Command;
use std::sync::Arc;
use std::collections::HashMap;

pub struct McpClient {
    transport: StdioTransport,
    id_manager: Arc<RequestIdManager>,
    initialized: bool,
}

impl McpClient {
    pub async fn new(
        command: &str,
        args: &[String],
        working_directory: Option<&str>,
        environment_vars: Option<&HashMap<String, String>>,
    ) -> Result<Self> {
        let mut cmd = Command::new(command);
        cmd.args(args);
        if let Some(dir) = working_directory {
            cmd.current_dir(dir);
        }
        if let Some(env_vars) = environment_vars {
            for (key, value) in env_vars {
                cmd.env(key, value);
            }
        }
        cmd.stdin(std::process::Stdio::piped());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        let child = cmd.spawn()
            .context("Failed to spawn MCP server process")?;

        let (transport, mut message_rx) = StdioTransport::new(child).await
            .context("Failed to create stdio transport")?;

        let id_manager = Arc::new(RequestIdManager::new());
        let id_manager_clone = Arc::clone(&id_manager);

        // メッセージ処理タスクを開始
        tokio::spawn(async move {
            while let Some(msg) = message_rx.recv().await {
                match msg {
                    JsonRpcMessage::Response(response) => {
                        let id = response.id;
                        if let Err(_) = id_manager_clone.complete_request(id, response) {
                            log::warn!("Received response for unknown request ID: {}", id);
                        }
                    }
                    JsonRpcMessage::Notification(notification) => {
                        log::debug!("Received notification: {}", notification.method);
                    }
                    JsonRpcMessage::Request(_) => {
                        log::warn!("Received unexpected request from server");
                    }
                }
            }
        });

        let client = Self {
            transport,
            id_manager,
            initialized: false,
        };

        Ok(client)
    }

    async fn send_request(&mut self, method: String, params: Option<Value>) -> Result<JsonRpcResponse> {
        let id = self.id_manager.allocate_id();
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id,
            method,
            params,
        };

        let (tx, rx) = oneshot::channel();
        self.id_manager.register_request(id, tx);

        let message = JsonRpcMessage::Request(request);
        self.transport.send(message).await
            .context("Failed to send request")?;

        // レスポンスを待つ（タイムアウト: 30秒）
        match timeout(Duration::from_secs(30), rx).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => Err(anyhow::anyhow!("Response channel closed")),
            Err(_) => {
                // タイムアウト
                Err(anyhow::anyhow!("Request timeout"))
            }
        }
    }

    async fn send_notification(&mut self, method: String, params: Option<Value>) -> Result<()> {
        let notification = JsonRpcMessage::Notification(super::jsonrpc::JsonRpcNotification {
            jsonrpc: "2.0".to_string(),
            method,
            params,
        });

        self.transport.send(notification).await
            .context("Failed to send notification")
    }


    pub async fn initialize(&mut self) -> Result<InitializeResult> {
        if self.initialized {
            return Err(anyhow::anyhow!("Already initialized"));
        }

        let params = InitializeParams {
            protocol_version: "2024-11-05".to_string(),
            capabilities: ClientCapabilities {
                experimental: None,
            },
            client_info: ClientInfo {
                name: "miclow".to_string(),
                version: "0.1.0".to_string(),
            },
        };

        let params_value = serde_json::to_value(params)
            .context("Failed to serialize initialize params")?;

        let response = self.send_request("initialize".to_string(), Some(params_value)).await
            .context("Failed to send initialize request")?;

        if let Some(error) = response.error {
            return Err(anyhow::anyhow!("Initialize error: {} (code: {})", error.message, error.code));
        }

        let result: InitializeResult = serde_json::from_value(
            response.result.ok_or_else(|| anyhow::anyhow!("Initialize response missing result"))?
        ).context("Failed to deserialize initialize result")?;

        // initialized通知を送信
        self.send_notification("initialized".to_string(), None).await
            .context("Failed to send initialized notification")?;

        self.initialized = true;

        Ok(result)
    }

    pub async fn list_tools(&mut self) -> Result<ListToolsResult> {
        let response = self.send_request("tools/list".to_string(), None).await
            .context("Failed to send tools/list request")?;

        if let Some(error) = response.error {
            return Err(anyhow::anyhow!("tools/list error: {} (code: {})", error.message, error.code));
        }

        let result: ListToolsResult = serde_json::from_value(
            response.result.ok_or_else(|| anyhow::anyhow!("tools/list response missing result"))?
        ).context("Failed to deserialize tools/list result")?;

        Ok(result)
    }

    pub async fn call_tool(&mut self, name: String, arguments: Option<Value>) -> Result<CallToolResult> {
        let params = CallToolParams {
            name,
            arguments,
        };

        let params_value = serde_json::to_value(params)
            .context("Failed to serialize call_tool params")?;

        let response = self.send_request("tools/call".to_string(), Some(params_value)).await
            .context("Failed to send tools/call request")?;

        if let Some(error) = response.error {
            return Err(anyhow::anyhow!("tools/call error: {} (code: {})", error.message, error.code));
        }

        let result: CallToolResult = serde_json::from_value(
            response.result.ok_or_else(|| anyhow::anyhow!("tools/call response missing result"))?
        ).context("Failed to deserialize tools/call result")?;

        Ok(result)
    }

    pub async fn list_resources(&mut self) -> Result<ListResourcesResult> {
        let response = self.send_request("resources/list".to_string(), None).await
            .context("Failed to send resources/list request")?;

        if let Some(error) = response.error {
            return Err(anyhow::anyhow!("resources/list error: {} (code: {})", error.message, error.code));
        }

        let result: ListResourcesResult = serde_json::from_value(
            response.result.ok_or_else(|| anyhow::anyhow!("resources/list response missing result"))?
        ).context("Failed to deserialize resources/list result")?;

        Ok(result)
    }

    pub async fn read_resource(&mut self, uri: String) -> Result<ReadResourceResult> {
        let params = ReadResourceParams { uri };
        let params_value = serde_json::to_value(params)
            .context("Failed to serialize read_resource params")?;

        let response = self.send_request("resources/read".to_string(), Some(params_value)).await
            .context("Failed to send resources/read request")?;

        if let Some(error) = response.error {
            return Err(anyhow::anyhow!("resources/read error: {} (code: {})", error.message, error.code));
        }

        let result: ReadResourceResult = serde_json::from_value(
            response.result.ok_or_else(|| anyhow::anyhow!("resources/read response missing result"))?
        ).context("Failed to deserialize resources/read result")?;

        Ok(result)
    }

    pub async fn list_prompts(&mut self) -> Result<ListPromptsResult> {
        let response = self.send_request("prompts/list".to_string(), None).await
            .context("Failed to send prompts/list request")?;

        if let Some(error) = response.error {
            return Err(anyhow::anyhow!("prompts/list error: {} (code: {})", error.message, error.code));
        }

        let result: ListPromptsResult = serde_json::from_value(
            response.result.ok_or_else(|| anyhow::anyhow!("prompts/list response missing result"))?
        ).context("Failed to deserialize prompts/list result")?;

        Ok(result)
    }

    pub async fn get_prompt(&mut self, name: String, arguments: Option<Value>) -> Result<GetPromptResult> {
        let params = GetPromptParams {
            name,
            arguments,
        };

        let params_value = serde_json::to_value(params)
            .context("Failed to serialize get_prompt params")?;

        let response = self.send_request("prompts/get".to_string(), Some(params_value)).await
            .context("Failed to send prompts/get request")?;

        if let Some(error) = response.error {
            return Err(anyhow::anyhow!("prompts/get error: {} (code: {})", error.message, error.code));
        }

        let result: GetPromptResult = serde_json::from_value(
            response.result.ok_or_else(|| anyhow::anyhow!("prompts/get response missing result"))?
        ).context("Failed to deserialize prompts/get result")?;

        Ok(result)
    }

    pub async fn shutdown(self) {
        self.transport.shutdown().await;
    }
}

