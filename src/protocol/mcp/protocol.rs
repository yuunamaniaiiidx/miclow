use anyhow::{Error, Result};
use tokio::task;
use tokio_util::sync::CancellationToken;
use std::collections::HashMap;
use toml::Value as TomlValue;
use crate::task_id::TaskId;
use crate::backend::TaskBackendHandle;
use crate::chunnel::{ExecutorEvent, ExecutorEventSender, ExecutorEventChannel};
use crate::chunnel::{InputChannel, InputReceiver, FunctionMessage, InputDataMessage};
use crate::chunnel::SystemResponseChannel;
use crate::chunnel::ShutdownChannel;
use crate::config::TaskConfig;
use super::client::McpClient;
use serde_json::Value as JsonValue;

#[derive(Clone)]
pub struct McpServerConfig {
    pub command: String,
    pub args: Vec<String>,
    pub working_directory: Option<String>,
    pub environment_vars: Option<HashMap<String, String>>,
}

pub fn try_mcp_server_from_task_config(config: &TaskConfig) -> Result<McpServerConfig, anyhow::Error> {
    let command: String = config.expand("command")
        .ok_or_else(|| anyhow::anyhow!("Command field is required for McpServer in task '{}'", config.name))?;
    
    if command.is_empty() {
        return Err(anyhow::anyhow!("Command field is required for McpServer in task '{}'", config.name));
    }
    
    let args: Vec<String> = config.expand("args").unwrap_or_default();
    
    let working_directory: Option<String> = config.expand("working_directory");
    
    let environment_vars = config.get_protocol_value("environment_vars")
        .and_then(|v| {
            if let TomlValue::Table(table) = v {
                let mut env_map = HashMap::new();
                for (key, value) in table {
                    if let Some(val_str) = value.as_str() {
                        env_map.insert(key.clone(), val_str.to_string());
                    } else {
                        return None;
                    }
                }
                Some(env_map)
            } else {
                None
            }
        });
    
    if let Some(ref working_dir) = working_directory {
        if !std::path::Path::new(working_dir).exists() {
            return Err(anyhow::anyhow!("Task '{}' working directory '{}' does not exist", config.name, working_dir));
        }
    }
    
    Ok(McpServerConfig {
        command,
        args,
        working_directory,
        environment_vars,
    })
}

pub async fn spawn_mcp_protocol(
    config: &McpServerConfig,
    task_id: TaskId,
) -> Result<TaskBackendHandle, Error> {
    let command = config.command.clone();
    let args = config.args.clone();
    let working_directory = config.working_directory.clone();
    let environment_vars = config.environment_vars.clone();

    let event_channel: ExecutorEventChannel = ExecutorEventChannel::new();
    let input_channel: InputChannel = InputChannel::new();
    let mut shutdown_channel = ShutdownChannel::new();
    let system_response_channel: SystemResponseChannel = SystemResponseChannel::new();
    
    let event_tx_clone: ExecutorEventSender = event_channel.sender.clone();
    let mut input_receiver: InputReceiver = input_channel.receiver;

    task::spawn(async move {
        let mut client = match McpClient::new(
            &command,
            &args,
            working_directory.as_deref(),
            environment_vars.as_ref(),
        ).await {
            Ok(client) => client,
            Err(e) => {
                let _ = event_tx_clone.send_error(format!("Failed to create MCP client: {}", e));
                return;
            }
        };

        // 環境変数を設定（必要に応じて）
        // 注: McpClient::new内でCommandを構築しているため、環境変数の設定は後で追加する必要があるかもしれません

        // 初期化
        match client.initialize().await {
            Ok(init_result) => {
                log::info!("MCP server initialized: {} {}", init_result.server_info.name, init_result.server_info.version);
            }
            Err(e) => {
                let _ = event_tx_clone.send_error(format!("Failed to initialize MCP server: {}", e));
                return;
            }
        }

        let cancel_token: CancellationToken = CancellationToken::new();

        // 入力処理タスク
        let cancel_input: CancellationToken = cancel_token.clone();
        let event_tx_input: ExecutorEventSender = event_tx_clone.clone();
        let input_handle = task::spawn(async move {
            let mut client = client;
            loop {
                tokio::select! {
                    _ = cancel_input.cancelled() => { break; }
                    input_data = input_receiver.recv() => {
                        match input_data {
                            Some(input_data_msg) => {
                                // MCPプロトコルでは、入力メッセージをツール呼び出しとして解釈
                                match input_data_msg {
                                    InputDataMessage::Function(FunctionMessage { task_name: _mcp_task_name, data, .. }) => {
                                        // dataをパースしてツール名と引数を取得
                                        // dataはJSON形式で、{"name": "ツール名", "arguments": {...}} または
                                        // 単純にツール名の文字列の場合もある
                                        let (tool_name, arguments) = if data.is_empty() {
                                            return; // ツール名が指定されていない
                                        } else {
                                            match serde_json::from_str::<JsonValue>(&data) {
                                                Ok(json_value) => {
                                                    // JSONオブジェクトの場合
                                                    if let Some(obj) = json_value.as_object() {
                                                        let name = obj.get("name")
                                                            .and_then(|v| v.as_str())
                                                            .map(|s| s.to_string());
                                                        let args = obj.get("arguments").cloned();
                                                        
                                                        if let Some(name) = name {
                                                            (name, args)
                                                        } else {
                                                            log::warn!("MCP tool call: 'name' field not found in JSON");
                                                            return;
                                                        }
                                                    } else if let Some(name_str) = json_value.as_str() {
                                                        // 文字列の場合、ツール名として扱う
                                                        (name_str.to_string(), None)
                                                    } else {
                                                        log::warn!("MCP tool call: invalid JSON format");
                                                        return;
                                                    }
                                                }
                                                Err(_) => {
                                                    // JSONとして解析できない場合は、文字列としてツール名として扱う
                                                    (data, None)
                                                }
                                            }
                                        };

                                        match client.call_tool(tool_name, arguments).await {
                                            Ok(result) => {
                                                // 結果をExecutorEventとして送信
                                                let result_json = serde_json::to_string(&result)
                                                    .unwrap_or_else(|_| "{}".to_string());
                                                
                                                // ツール呼び出し結果を返す
                                                let result_text = result.content.as_ref()
                                                    .and_then(|c| c.first())
                                                    .map(|c| c.text.clone())
                                                    .unwrap_or_else(|| result_json.clone());
                                                
                                                // system.returnとして送信
                                                let _ = event_tx_input.send(ExecutorEvent::new_return_message(result_text));
                                            }
                                            Err(e) => {
                                                let _ = event_tx_input.send_error(format!("MCP tool call failed: {}", e));
                                            }
                                        }
                                    }
                                    _ => {
                                        // その他のメッセージタイプは無視
                                        log::debug!("Ignoring non-function input message for MCP protocol");
                                    }
                                }
                            },
                            None => break,
                        }
                    }
                }
            }
            // クライアントをシャットダウン
            client.shutdown().await;
        });

        // シャットダウン処理
        let status_cancel: CancellationToken = cancel_token.clone();
        let status_handle = task::spawn(async move {
            tokio::select! {
                _ = shutdown_channel.receiver.recv() => {
                    log::info!("Shutdown signal received for MCP task {}", task_id);
                    status_cancel.cancel();
                }
            }
        });

        let _ = status_handle.await;
        cancel_token.cancel();
        let _ = input_handle.await;
    });
    
    Ok(TaskBackendHandle {
        event_receiver: event_channel.receiver,
        event_sender: event_channel.sender,
        system_response_sender: system_response_channel.sender,
        input_sender: input_channel.sender,
        shutdown_sender: shutdown_channel.sender,
    })
}

