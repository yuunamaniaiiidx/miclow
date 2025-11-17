use crate::backend::TaskBackendHandle;
use crate::channels::{
    ExecutorInputEventChannel, ExecutorInputEventReceiver, ExecutorOutputEventChannel,
    ExecutorOutputEventSender, ShutdownChannel, SystemResponseChannel,
};
use crate::config::TaskConfig;
use crate::messages::{ExecutorInputEvent, ExecutorOutputEvent};
use crate::task_id::TaskId;
use anyhow::{bail, Context, Result};
use rmcp::handler::client::ClientHandler;
use rmcp::model::{
    CallToolRequestParam, CallToolResult, ClientInfo, JsonObject, LoggingMessageNotificationParam,
    ProgressNotificationParam,
};
use rmcp::service::{Peer, QuitReason, RoleClient, ServiceExt};
use rmcp::transport::child_process::TokioChildProcess;
use serde_json::{self, Value as JsonValue};
use std::borrow::Cow;
use std::collections::HashMap;
use tokio::process::Command;
use tokio::task;
use toml::Value as TomlValue;

#[derive(Clone, Debug)]
pub struct McpServerConfig {
    pub command: String,
    pub args: Vec<String>,
    pub working_directory: Option<String>,
    pub environment_vars: Option<HashMap<String, String>>,
}

pub fn try_mcp_server_from_task_config(config: &TaskConfig) -> Result<McpServerConfig> {
    let command: String = config.expand("command").ok_or_else(|| {
        anyhow::anyhow!(
            "Command field is required for McpServer in task '{}'",
            config.name
        )
    })?;

    if command.trim().is_empty() {
        bail!(
            "Command field is required for McpServer in task '{}'",
            config.name
        );
    }

    let args: Vec<String> = config.expand("args").unwrap_or_default();
    let working_directory: Option<String> = config.expand("working_directory");
    let environment_vars = config
        .get_protocol_value("environment_vars")
        .and_then(|value| {
            if let TomlValue::Table(table) = value {
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
            bail!(
                "Task '{}' working directory '{}' does not exist",
                config.name,
                working_dir
            );
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
) -> Result<TaskBackendHandle> {
    let event_channel = ExecutorOutputEventChannel::new();
    let input_channel = ExecutorInputEventChannel::new();
    let shutdown_channel = ShutdownChannel::new();
    let system_response_channel = SystemResponseChannel::new();

    let event_sender = event_channel.sender.clone();
    let input_receiver = input_channel.receiver;
    let shutdown_receiver = shutdown_channel.receiver;
    let config = config.clone();

    task::spawn(async move {
        if let Err(err) = run_mcp_task(
            config,
            task_id,
            input_receiver,
            shutdown_receiver,
            event_sender.clone(),
        )
        .await
        {
            let _ = event_sender.send_error(format!("MCP backend error: {err:#}"));
        }
    });

    Ok(TaskBackendHandle {
        event_receiver: event_channel.receiver,
        event_sender: event_channel.sender,
        system_response_sender: system_response_channel.sender,
        input_sender: input_channel.sender,
        shutdown_sender: shutdown_channel.sender,
    })
}

async fn run_mcp_task(
    config: McpServerConfig,
    task_id: TaskId,
    mut input_receiver: ExecutorInputEventReceiver,
    mut shutdown_receiver: tokio::sync::mpsc::UnboundedReceiver<()>,
    event_sender: ExecutorOutputEventSender,
) -> Result<()> {
    let transport =
        spawn_child_transport(&config).context("failed to spawn MCP child process transport")?;

    let handler = MiclowClientHandler::new(task_id.clone(), event_sender.clone());
    let service = handler
        .serve(transport)
        .await
        .context("failed to initialize MCP client service")?;

    let peer = service.peer().clone();

    loop {
        tokio::select! {
            shutdown = shutdown_receiver.recv() => {
                if shutdown.is_some() {
                    service.cancellation_token().cancel();
                }
                break;
            }
            input = input_receiver.recv() => {
                match input {
                    Some(event) => {
                        if let Err(err) = handle_input_event(peer.clone(), event_sender.clone(), event).await {
                            let _ = event_sender.send_error(format!("MCP request failed: {err:#}"));
                        }
                    }
                    None => {
                        service.cancellation_token().cancel();
                        break;
                    }
                }
            }
        }

        if peer.is_transport_closed() {
            let _ =
                event_sender.send_error("MCP server connection closed unexpectedly".to_string());
            break;
        }
    }

    match service.waiting().await {
        Ok(QuitReason::Closed | QuitReason::Cancelled) => {
            let _ = event_sender.send_exit(0);
        }
        Ok(QuitReason::JoinError(err)) => {
            let _ = event_sender.send_error(format!("MCP service join error: {err}"));
        }
        Err(join_err) => {
            let _ = event_sender.send_error(format!("Failed to join MCP service task: {join_err}"));
        }
    }

    Ok(())
}

fn spawn_child_transport(config: &McpServerConfig) -> Result<TokioChildProcess> {
    let mut command = Command::new(&config.command);
    command.args(&config.args);

    if let Some(dir) = &config.working_directory {
        command.current_dir(dir);
    }

    if let Some(envs) = &config.environment_vars {
        for (key, value) in envs {
            command.env(key, value);
        }
    }

    TokioChildProcess::new(command).context("failed to spawn MCP child process")
}

async fn handle_input_event(
    peer: Peer<RoleClient>,
    event_sender: ExecutorOutputEventSender,
    event: ExecutorInputEvent,
) -> Result<()> {
    match event {
        ExecutorInputEvent::Function { data, .. } => {
            if data.trim().is_empty() {
                bail!("MCP function payload is empty");
            }

            let (tool_name, arguments) = parse_tool_invocation(&data)?;
            let arguments = to_argument_map(arguments)?;
            let request = CallToolRequestParam {
                name: Cow::Owned(tool_name.clone()),
                arguments,
            };

            let result = peer.call_tool(request).await?;
            forward_call_result(tool_name, result, event_sender)?;
        }
        _ => {
            // ignore non-function inputs for MCP backend
        }
    }

    Ok(())
}

fn parse_tool_invocation(payload: &str) -> Result<(String, Option<JsonValue>)> {
    match serde_json::from_str::<JsonValue>(payload) {
        Ok(JsonValue::Object(obj)) => {
            if let Some(name) = obj.get("name").and_then(|value| value.as_str()) {
                Ok((name.to_string(), obj.get("arguments").cloned()))
            } else {
                bail!("MCP tool invocation JSON missing 'name' field");
            }
        }
        Ok(JsonValue::String(name)) => Ok((name, None)),
        Ok(other) => {
            bail!("Unsupported MCP invocation payload: expected string or object, got {other}")
        }
        Err(_) => Ok((payload.to_string(), None)),
    }
}

fn to_argument_map(value: Option<JsonValue>) -> Result<Option<JsonObject>> {
    match value {
        Some(JsonValue::Object(map)) => Ok(Some(map)),
        Some(other) => bail!("MCP tool arguments must be a JSON object, got {other}"),
        None => Ok(None),
    }
}

fn forward_call_result(
    tool_name: String,
    result: CallToolResult,
    event_sender: ExecutorOutputEventSender,
) -> Result<()> {
    let payload = format_tool_result(&result)?;
    if result.is_error.unwrap_or(false) {
        event_sender
            .send_error(format!("Tool '{tool_name}' error: {payload}"))
            .ok();
    } else {
        event_sender
            .send(ExecutorOutputEvent::new_return_message(payload))
            .ok();
    }
    Ok(())
}

fn format_tool_result(result: &CallToolResult) -> Result<String> {
    if let Some(structured) = &result.structured_content {
        return Ok(structured.to_string());
    }

    if let Some(content) = result.content.first() {
        if let Some(text) = content.as_text() {
            return Ok(text.text.clone());
        }
    }

    serde_json::to_string(result).context("failed to serialize CallToolResult")
}

#[derive(Clone)]
struct MiclowClientHandler {
    task_id: TaskId,
    event_sender: ExecutorOutputEventSender,
}

impl MiclowClientHandler {
    fn new(task_id: TaskId, event_sender: ExecutorOutputEventSender) -> Self {
        Self {
            task_id,
            event_sender,
        }
    }

    fn publish_topic<T: ToString>(&self, topic: &str, data: T) {
        let _ = self.event_sender.send(ExecutorOutputEvent::new_message(
            topic.to_string(),
            data.to_string(),
        ));
    }
}

impl ClientHandler for MiclowClientHandler {
    fn get_info(&self) -> ClientInfo {
        let mut info = ClientInfo::default();
        info.client_info.name = "miclow".to_string();
        info.client_info.title = Some(format!("miclow task {}", self.task_id));
        info.client_info.version = env!("CARGO_PKG_VERSION").to_string();
        info
    }

    fn on_logging_message(
        &self,
        params: LoggingMessageNotificationParam,
        _context: rmcp::service::NotificationContext<RoleClient>,
    ) -> impl std::future::Future<Output = ()> + Send + '_ {
        let topic = format!("mcp.{}.log", self.task_id);
        let payload = serde_json::to_string(&params).unwrap_or_else(|_| "{}".to_string());
        async move {
            self.publish_topic(&topic, payload);
        }
    }

    fn on_progress(
        &self,
        params: ProgressNotificationParam,
        _context: rmcp::service::NotificationContext<RoleClient>,
    ) -> impl std::future::Future<Output = ()> + Send + '_ {
        let topic = format!("mcp.{}.progress", self.task_id);
        let payload = serde_json::to_string(&params).unwrap_or_else(|_| "{}".to_string());
        async move {
            self.publish_topic(&topic, payload);
        }
    }
}
