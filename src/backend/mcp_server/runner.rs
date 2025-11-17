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
use rmcp::transport::async_rw::AsyncRwTransport;
use rmcp::transport::child_process::TokioChildProcess;
use serde::Deserialize;
use serde_json::{self, Value as JsonValue};
use std::borrow::Cow;
use std::collections::HashMap;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader as TokioBufReader};
use tokio::net::TcpStream;
use tokio::process::{ChildStderr, Command};
use tokio::task;
use toml::Value as TomlValue;

#[derive(Clone, Debug)]
pub struct McpServerStdIOConfig {
    pub command: String,
    pub args: Vec<String>,
    pub working_directory: Option<String>,
    pub environment_vars: Option<HashMap<String, String>>,
}

pub fn try_mcp_server_stdio_from_task_config(config: &TaskConfig) -> Result<McpServerStdIOConfig> {
    let command: String = config.expand("command").ok_or_else(|| {
        anyhow::anyhow!(
            "Command field is required for McpServerStdIO in task '{}'",
            config.name
        )
    })?;

    if command.trim().is_empty() {
        bail!(
            "Command field is required for McpServerStdIO in task '{}'",
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

    Ok(McpServerStdIOConfig {
        command,
        args,
        working_directory,
        environment_vars,
    })
}

#[derive(Clone, Debug)]
pub struct McpServerTcpConfig {
    pub host: String,
    pub port: u16,
}

pub fn try_mcp_server_tcp_from_task_config(config: &TaskConfig) -> Result<McpServerTcpConfig> {
    let host: String = config
        .expand("host")
        .unwrap_or_else(|| "127.0.0.1".to_string());

    let port = config
        .get_protocol_value("port")
        .and_then(|value| match value {
            TomlValue::Integer(i) if *i >= 0 && *i <= u16::MAX as i64 => Some(*i as u16),
            TomlValue::String(s) => s.parse::<u16>().ok(),
            _ => None,
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Port field is required for McpServerTcp in task '{}' and must be a valid u16",
                config.name
            )
        })?;

    if host.trim().is_empty() {
        bail!(
            "Host field is required for McpServerTcp in task '{}'",
            config.name
        );
    }

    Ok(McpServerTcpConfig { host, port })
}

pub async fn spawn_mcp_stdio_protocol(
    config: &McpServerStdIOConfig,
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
        match spawn_child_transport(&config) {
            Ok((transport, stderr_stream)) => {
                let stderr_forwarder = stderr_stream.map(|stderr| {
                    spawn_stream_forwarder(
                        stderr,
                        event_sender.clone(),
                        ExecutorOutputEvent::new_task_stderr,
                        "stderr",
                    )
                });

                if let Err(err) = run_mcp_session(
                    transport,
                    stderr_forwarder,
                    task_id,
                    input_receiver,
                    shutdown_receiver,
                    event_sender.clone(),
                )
                .await
                {
                    let _ = event_sender.send_error(format!("MCP StdIO backend error: {err:#}"));
                }
            }
            Err(err) => {
                let _ = event_sender.send_error(format!(
                    "Failed to start MCP StdIO process for task {}: {err:#}",
                    task_id
                ));
            }
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

pub async fn spawn_mcp_tcp_protocol(
    config: &McpServerTcpConfig,
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
        let address = format!("{}:{}", config.host, config.port);
        match TcpStream::connect(&address).await {
            Ok(stream) => {
                let (reader, writer) = tokio::io::split(stream);
                let transport = AsyncRwTransport::<RoleClient, _, _>::new(reader, writer);

                if let Err(err) = run_mcp_session(
                    transport,
                    None,
                    task_id,
                    input_receiver,
                    shutdown_receiver,
                    event_sender.clone(),
                )
                .await
                {
                    let _ = event_sender.send_error(format!("MCP TCP backend error: {err:#}"));
                }
            }
            Err(err) => {
                let _ = event_sender.send_error(format!(
                    "Failed to connect to MCP server at {}:{}: {}",
                    config.host, config.port, err
                ));
            }
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

async fn run_mcp_session<T>(
    transport: T,
    mut stderr_forwarder: Option<task::JoinHandle<()>>,
    task_id: TaskId,
    mut input_receiver: ExecutorInputEventReceiver,
    mut shutdown_receiver: tokio::sync::mpsc::UnboundedReceiver<()>,
    event_sender: ExecutorOutputEventSender,
) -> Result<()>
where
    T: rmcp::transport::IntoTransport<
        RoleClient,
        std::io::Error,
        rmcp::transport::TransportAdapterIdentity,
    >,
{
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
                    Some(ExecutorInputEvent::Function { data, .. }) => {
                        match parse_tool_invocation(&data) {
                            Ok((tool_name, arguments_raw)) => {
                                let arguments = match to_argument_map(arguments_raw) {
                                    Ok(arguments) => arguments,
                                    Err(err) => {
                                        emit_stderr(&event_sender, format!("Invalid MCP function arguments: {err}"));
                                        continue;
                                    }
                                };

                                if let Err(err) = dispatch_call_tool(peer.clone(), event_sender.clone(), tool_name, arguments).await {
                                    let _ = event_sender.send_error(format!("MCP request failed: {err:#}"));
                                }
                            }
                            Err(err) => {
                                emit_stderr(&event_sender, format!("Invalid MCP function payload: {err}"));
                                continue;
                            }
                        }
                    }
                    Some(other_event) => {
                        emit_stderr(
                            &event_sender,
                            format!("Unsupported ExecutorInputEvent for MCP backend: {:?}", other_event),
                        );
                        continue;
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

    if let Some(handle) = stderr_forwarder.take() {
        let _ = handle.await;
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

fn spawn_child_transport(
    config: &McpServerStdIOConfig,
) -> Result<(TokioChildProcess, Option<ChildStderr>)> {
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

    TokioChildProcess::builder(command)
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to spawn MCP child process")
}

async fn dispatch_call_tool(
    peer: Peer<RoleClient>,
    event_sender: ExecutorOutputEventSender,
    tool_name: String,
    arguments: Option<JsonObject>,
) -> Result<()> {
    let request = CallToolRequestParam {
        name: Cow::Owned(tool_name.clone()),
        arguments,
    };

    let result = peer.call_tool(request).await?;
    forward_call_result(tool_name, result, event_sender)?;
    Ok(())
}

#[derive(Deserialize)]
struct CallToolPayload {
    name: String,
    #[serde(default)]
    arguments: Option<JsonValue>,
}

fn parse_tool_invocation(payload: &str) -> Result<(String, Option<JsonValue>)> {
    let parsed: CallToolPayload = serde_json::from_str(payload)
        .context("MCP function payload must be valid JSON with a name field")?;

    if parsed.name.trim().is_empty() {
        bail!("MCP tool invocation JSON missing non-empty 'name' field");
    }

    Ok((parsed.name, parsed.arguments))
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

fn spawn_stream_forwarder<R>(
    reader: R,
    event_sender: ExecutorOutputEventSender,
    to_event: fn(String) -> ExecutorOutputEvent,
    stream_name: &'static str,
) -> task::JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    task::spawn(async move {
        let mut reader = TokioBufReader::new(reader);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => break,
                Ok(_) => {
                    let trimmed = line.trim_end_matches(&['\r', '\n'][..]).to_string();
                    if trimmed.is_empty() {
                        continue;
                    }
                    let _ = event_sender.send(to_event(trimmed));
                }
                Err(err) => {
                    let _ =
                        event_sender.send_error(format!("Failed to read MCP {stream_name}: {err}"));
                    break;
                }
            }
        }
    })
}

fn emit_stderr(event_sender: &ExecutorOutputEventSender, message: String) {
    let _ = event_sender.send(ExecutorOutputEvent::new_task_stderr(message));
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

    fn publish_stdout<T: ToString>(&self, prefix: &str, data: T) {
        let message = format!("{} {}", prefix, data.to_string());
        let _ = self
            .event_sender
            .send(ExecutorOutputEvent::new_task_stdout(message));
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
        let payload = serde_json::to_string(&params).unwrap_or_else(|_| "{}".to_string());
        async move {
            self.publish_stdout("[mcp.log]", payload);
        }
    }

    fn on_progress(
        &self,
        params: ProgressNotificationParam,
        _context: rmcp::service::NotificationContext<RoleClient>,
    ) -> impl std::future::Future<Output = ()> + Send + '_ {
        let payload = serde_json::to_string(&params).unwrap_or_else(|_| "{}".to_string());
        async move {
            self.publish_stdout("[mcp.progress]", payload);
        }
    }
}
