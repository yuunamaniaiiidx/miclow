use anyhow::{Error, Result};
use tokio::process::Command as TokioCommand;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader};
use tokio::task;
use tokio_util::sync::CancellationToken;
use std::process::Stdio;
use std::collections::HashMap;
use toml::Value as TomlValue;
use crate::task_id::TaskId;
use crate::backend::TaskBackendHandle;
use crate::messages::ExecutorEvent;
use crate::channels::{ExecutorEventSender, ExecutorEventChannel};
use crate::messages::{TopicMessage, SystemResponseMessage, ReturnMessage, FunctionMessage, InputDataMessage};
use crate::channels::{InputChannel, InputReceiver, SystemResponseChannel, ShutdownChannel};
use super::buffer::{InputBufferManager, StreamOutcome};
use crate::config::TaskConfig;

#[derive(Clone)]
pub struct MiclowStdinConfig {
    pub command: String,
    pub args: Vec<String>,
    pub working_directory: Option<String>,
    pub environment_vars: Option<HashMap<String, String>>,
    pub stdout_topic: String,
    pub stderr_topic: String,
    pub view_stdout: bool,
    pub view_stderr: bool,
}
#[cfg(unix)]
use nix::sys::signal::{kill, Signal};
#[cfg(unix)]
use nix::unistd::Pid;

pub trait StdinProtocol: Send + Sync {
    fn to_input_lines(&self) -> Vec<String> {
        let lines = self.to_input_lines_raw();
        
        if lines.len() < 2 {
            panic!("StdinProtocol validation failed: must have at least 2 lines, got {}", lines.len());
        }
        
        let line_count: usize = lines[1].parse()
            .unwrap_or_else(|_| {
                panic!("StdinProtocol validation failed: line 2 must be a number, got '{}'", lines[1]);
            });
        
        let data_line_count = lines.len() - 2;
        if data_line_count != line_count {
            panic!(
                "StdinProtocol validation failed: expected {} data lines (from line 2), but got {} (total lines: {})",
                line_count, data_line_count, lines.len()
            );
        }
        
        lines
    }
    
    fn to_input_lines_raw(&self) -> Vec<String>;
}

impl StdinProtocol for TopicMessage {
    fn to_input_lines_raw(&self) -> Vec<String> {
        let mut lines = vec![self.topic.clone()];
        let data_lines: Vec<&str> = self.data.lines().collect();
        lines.push(data_lines.len().to_string());
        lines.extend(data_lines.iter().map(|s| s.to_string()));
        lines
    }
}

impl StdinProtocol for SystemResponseMessage {
    fn to_input_lines_raw(&self) -> Vec<String> {
        let mut lines = vec![self.topic.clone()];
        let data_lines: Vec<&str> = self.data.lines().collect();
        lines.push((data_lines.len() + 1).to_string());
        lines.push(self.status.clone());
        lines.extend(data_lines.iter().map(|s| s.to_string()));
        lines
    }
}

impl StdinProtocol for ReturnMessage {
    fn to_input_lines_raw(&self) -> Vec<String> {
        let data_lines: Vec<&str> = self.data.lines().collect();
        let mut lines = vec!["system.return".to_string(), data_lines.len().to_string()];
        lines.extend(data_lines.iter().map(|s| s.to_string()));
        lines
    }
}

impl StdinProtocol for FunctionMessage {
    fn to_input_lines_raw(&self) -> Vec<String> {
        let data_lines: Vec<&str> = self.data.lines().collect();
        let mut lines = vec!["system.function".to_string(), (data_lines.len() + 1).to_string()];
        lines.push(self.task_name.clone());
        lines.extend(data_lines.iter().map(|s| s.to_string()));
        lines
    }
}

impl StdinProtocol for InputDataMessage {
    fn to_input_lines_raw(&self) -> Vec<String> {
        match self {
            InputDataMessage::Topic(msg) => msg.to_input_lines_raw(),
            InputDataMessage::SystemResponse(msg) => msg.to_input_lines_raw(),
            InputDataMessage::Return(msg) => msg.to_input_lines_raw(),
            InputDataMessage::Function(msg) => msg.to_input_lines_raw(),
        }
    }
}

pub fn try_miclow_stdin_from_task_config(config: &TaskConfig) -> Result<MiclowStdinConfig, anyhow::Error> {
    // プロトコル固有のフィールドを抽出・バリデーション
    let command: String = config.expand("command")
        .ok_or_else(|| anyhow::anyhow!("Command field is required for MiclowStdin in task '{}'", config.name))?;
    
    if command.is_empty() {
        return Err(anyhow::anyhow!("Command field is required for MiclowStdin in task '{}'", config.name));
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
    
    // デフォルト値の生成ロジック: stdout_topic/stderr_topicが未設定の場合は"{name}.stdout"/"{name}.stderr"を使用
    let stdout_topic: String = config.expand("stdout_topic")
        .unwrap_or_else(|| format!("{}.stdout", config.name));
    
    let stderr_topic: String = config.expand("stderr_topic")
        .unwrap_or_else(|| format!("{}.stderr", config.name));
    
    // view_stdoutとview_stderrを取得（デフォルトはfalse）
    let view_stdout: bool = config.expand("view_stdout").unwrap_or(false);
    
    let view_stderr: bool = config.expand("view_stderr").unwrap_or(false);
    
    // バリデーション
    if stdout_topic.contains(' ') {
        return Err(anyhow::anyhow!("Task '{}' stdout_topic '{}' contains spaces (not allowed)", config.name, stdout_topic));
    }
    if stderr_topic.contains(' ') {
        return Err(anyhow::anyhow!("Task '{}' stderr_topic '{}' contains spaces (not allowed)", config.name, stderr_topic));
    }
    
    if let Some(ref working_dir) = working_directory {
        if !std::path::Path::new(working_dir).exists() {
            return Err(anyhow::anyhow!("Task '{}' working directory '{}' does not exist", config.name, working_dir));
        }
    }
    
    Ok(MiclowStdinConfig {
        command,
        args,
        working_directory,
        environment_vars,
        stdout_topic,
        stderr_topic,
        view_stdout,
        view_stderr,
    })
}

pub fn parse_system_control_command_from_outcome(topic: &str, data: &str) -> Option<ExecutorEvent> {
    let topic_lower = topic.to_lowercase();
    let data_trimmed = data.trim();
    
    // Allow empty data for system.status and system.function.* commands
    if data_trimmed.is_empty() && topic_lower.as_str() != "system.status" && !topic_lower.starts_with("system.function.") {
        return None;
    }
    
    let is_system_control = topic_lower.starts_with("system.") || (topic_lower.is_empty() && data_trimmed.starts_with("system."));
    
    if is_system_control {
        let actual_topic = if topic_lower.is_empty() {
            let parts: Vec<&str> = data_trimmed.splitn(2, ' ').collect();
            if parts.len() == 2 {
                parts[0].to_string()
            } else {
                data_trimmed.to_string()
            }
        } else {
            topic_lower.clone()
        };
        
        let actual_data = if topic_lower.is_empty() && actual_topic != data_trimmed {
            let parts: Vec<&str> = data_trimmed.splitn(2, ' ').collect();
            if parts.len() == 2 {
                parts[1].to_string()
            } else {
                String::new()
            }
        } else {
            data_trimmed.to_string()
        };
        
        return Some(ExecutorEvent::new_system_control(actual_topic, actual_data));
    }
    
    None
}

pub fn parse_return_message_from_outcome(topic: &str, data: &str) -> Option<ExecutorEvent> {
    let topic_lower = topic.to_lowercase();
    let data_trimmed = data.trim();
    
    if topic_lower == "system.return" {
        return Some(ExecutorEvent::new_return_message(data_trimmed.to_string()));
    }
    
    None
}

pub async fn spawn_miclow_protocol(
    config: &MiclowStdinConfig,
    task_id: TaskId,
) -> Result<TaskBackendHandle, Error> {
    let command = config.command.clone();
    let args = config.args.clone();
    let working_directory = config.working_directory.clone();
    let environment_vars = config.environment_vars.clone();
    let stdout_topic = config.stdout_topic.clone();
    let stderr_topic = config.stderr_topic.clone();
    let view_stdout = config.view_stdout;
    let view_stderr = config.view_stderr;

    let event_channel: ExecutorEventChannel = ExecutorEventChannel::new();
    let input_channel: InputChannel = InputChannel::new();
    let mut shutdown_channel = ShutdownChannel::new();
    let system_response_channel: SystemResponseChannel = SystemResponseChannel::new();
    
    let event_tx_clone: ExecutorEventSender = event_channel.sender.clone();
    let mut input_receiver: InputReceiver = input_channel.receiver;

    task::spawn(async move {
        let mut command_builder = TokioCommand::new(&command);
        
        for arg in &args {
            command_builder.arg(arg);
        }
        
        if let Some(working_dir) = &working_directory {
            command_builder.current_dir(working_dir);
        }

        if let Some(env_vars) = &environment_vars {
            for (key, value) in env_vars {
                command_builder.env(key, value);
            }
        }

        command_builder.env("MICLOW_TASK_ID", task_id.to_string());
        
        let mut child = match command_builder
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .stdin(Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(e) => {
                let _ = event_tx_clone.send(ExecutorEvent::new_error(
                    format!("Failed to start process '{}': {}", command, e),
                ));
                return;
            }
        };

        let stdout: tokio::process::ChildStdout = child.stdout.take().unwrap();
        let stderr: tokio::process::ChildStderr = child.stderr.take().unwrap();
        let mut stdin_writer = child.stdin.take().unwrap();

        let cancel_token: CancellationToken = CancellationToken::new();

        let stdout_worker = spawn_stream_reader(
            TokioBufReader::new(stdout),
            stdout_topic.clone(),
            event_tx_clone.clone(),
            cancel_token.clone(),
            task_id.clone(),
            if view_stdout { Some(ExecutorEvent::new_task_stdout as fn(String) -> ExecutorEvent) } else { None },
        );

        let stderr_worker = spawn_stream_reader(
            TokioBufReader::new(stderr),
            stderr_topic.clone(),
            event_tx_clone.clone(),
            cancel_token.clone(),
            task_id.clone(),
            if view_stderr { Some(ExecutorEvent::new_task_stderr as fn(String) -> ExecutorEvent) } else { None },
        );

        let cancel_input: CancellationToken = cancel_token.clone();
        let event_tx_input: ExecutorEventSender = event_tx_clone.clone();
        let input_worker = task::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_input.cancelled() => { break; }
                    input_data = input_receiver.recv() => {
                        match input_data {
                            Some(input_data_msg) => {
                                let lines = input_data_msg.to_input_lines();
                                for line in lines {
                                    let bytes: Vec<u8> = if line.ends_with('\n') { 
                                        line.into_bytes() 
                                    } else { 
                                        format!("{}\n", line).into_bytes() 
                                    };
                                    if let Err(e) = stdin_writer.write_all(&bytes).await {
                                        let _ = event_tx_input.send_error(format!("Failed to write to stdin: {}", e));
                                        break;
                                    }
                                }
                                if let Err(e) = stdin_writer.flush().await {
                                    let _ = event_tx_input.send_error(format!("Failed to flush stdin: {}", e));
                                    break;
                                }
                            },
                            None => break,
                        }
                    }
                }
            }
        });

        let event_tx_status: ExecutorEventSender = event_tx_clone.clone();
        let status_cancel: CancellationToken = cancel_token.clone();
        let task_id_status: TaskId = task_id.clone();
        let status_worker = task::spawn(async move {
            let notify = |res: Result<std::process::ExitStatus, anyhow::Error>| {
                match res {
                    Ok(exit_status) => {
                        let code: i32 = exit_status.code().unwrap_or(-1);
                        let _ = event_tx_status.send_exit(code);
                    }
                    Err(e) => {
                        log::error!("Error waiting for process: {}", e);
                        let _ = event_tx_status
                            .send_error(format!("Error waiting for process: {}", e));
                    }
                }
            };
            tokio::select! {
                _ = shutdown_channel.receiver.recv() => {
                    log::info!("Shutdown signal received for task {}, attempting graceful termination", task_id_status);
                    
                    #[cfg(unix)]
                    let pid_opt = child.id().map(|id| Pid::from_raw(id as i32));
                    #[cfg(not(unix))]
                    let pid_opt: Option<()> = None;
                    
                    #[cfg(unix)]
                    let graceful_shutdown_attempted = if let Some(pid) = pid_opt {
                        match kill(pid, Signal::SIGTERM) {
                            Ok(_) => {
                                log::info!("Sent SIGTERM to child process {} (task {}), waiting for graceful shutdown", pid, task_id_status);
                                true
                            }
                            Err(e) => {
                                log::warn!("Failed to send SIGTERM to child process {} (task {}): {}, will use SIGKILL", pid, task_id_status, e);
                                false
                            }
                        }
                    } else {
                        false
                    };
                    #[cfg(not(unix))]
                    let graceful_shutdown_attempted = false;
                    
                    if graceful_shutdown_attempted {
                        let graceful_timeout = tokio::time::Duration::from_secs(3);
                        
                        let mut wait_worker = tokio::spawn(async move {
                            child.wait().await.map_err(anyhow::Error::from)
                        });
                        
                        tokio::select! {
                            result = &mut wait_worker => {
                                match result {
                                    Ok(Ok(exit_status)) => {
                                        #[cfg(unix)]
                                        if let Some(pid) = pid_opt {
                                            log::info!("Child process {} (task {}) exited gracefully after SIGTERM", pid, task_id_status);
                                        }
                                        notify(Ok(exit_status));
                                        status_cancel.cancel();
                                        return;
                                    }
                                    Ok(Err(e)) => {
                                        notify(Err(e));
                                        status_cancel.cancel();
                                        return;
                                    }
                                    Err(e) => {
                                        log::warn!("Error waiting for child process (task {}): {}", task_id_status, e);
                                        notify(Err(anyhow::Error::from(e)));
                                        status_cancel.cancel();
                                        return;
                                    }
                                }
                            }
                            _ = tokio::time::sleep(graceful_timeout) => {
                                #[cfg(unix)]
                                if let Some(pid) = pid_opt {
                                    log::warn!("Child process {} (task {}) did not exit within timeout, sending SIGKILL", pid, task_id_status);
                                    let _ = kill(pid, Signal::SIGKILL);
                                }
                                match wait_worker.await {
                                    Ok(Ok(exit_status)) => {
                                        notify(Ok(exit_status));
                                    }
                                    Ok(Err(e)) => {
                                        notify(Err(e));
                                    }
                                    Err(e) => {
                                        notify(Err(anyhow::anyhow!("Child process terminated by SIGKILL: {}", e)));
                                    }
                                }
                                status_cancel.cancel();
                                return;
                            }
                        }
                    } else {
                        log::info!("Forcing termination of child process for task {}", task_id_status);
                        let _ = child.kill().await;
                        notify(child.wait().await.map_err(anyhow::Error::from));
                        status_cancel.cancel();
                    }
                }
                status = child.wait() => {
                    notify(status.map_err(anyhow::Error::from));
                    // プロセスが自然に終了した場合、stdout/stderrの読み取りが完了するまで
                    // cancel_tokenをキャンセルしない（EOFで自然に終了するため）
                    // これにより、バッファに残っているエラーメッセージが確実に読み取られる
                }
            }
        });

        let _ = status_worker.await;
        
        // プロセス終了後、stdout/stderrのストリームがEOFになるまで待機
        // タイムアウトを設けて、無限に待たないようにする
        let stream_read_timeout = tokio::time::Duration::from_millis(500);
        tokio::select! {
            _ = stdout_worker => {
                log::debug!("stdout reader completed for task {}", task_id);
            }
            _ = tokio::time::sleep(stream_read_timeout) => {
                log::debug!("stdout reader timeout for task {}, continuing", task_id);
            }
        }
        tokio::select! {
            _ = stderr_worker => {
                log::debug!("stderr reader completed for task {}", task_id);
            }
            _ = tokio::time::sleep(stream_read_timeout) => {
                log::debug!("stderr reader timeout for task {}, continuing", task_id);
            }
        }
        
        cancel_token.cancel();
        let _ = input_worker.await;
    });
    
    Ok(TaskBackendHandle {
        event_receiver: event_channel.receiver,
        event_sender: event_channel.sender,
        system_response_sender: system_response_channel.sender,
        input_sender: input_channel.sender,
        shutdown_sender: shutdown_channel.sender,
    })
}

fn spawn_stream_reader<R>(
    mut reader: R,
    topic_name: String,
    event_tx: ExecutorEventSender,
    cancel_token: CancellationToken,
    task_id: TaskId,
    emit_func: Option<fn(String) -> ExecutorEvent>,
) -> task::JoinHandle<()> 
where
    R: tokio::io::AsyncBufRead + Unpin + Send + 'static
{
    task::spawn(async move {
        let mut buffer_manager = InputBufferManager::new();
        let mut line = String::new();
        let task_id_str = task_id.to_string();
        
        let process_stream_outcome = |outcome: Result<StreamOutcome, String>, line_content: &str| {
            match outcome {
                Ok(StreamOutcome::Emit { topic, data }) => {
                    if let Some(return_message_event) = parse_return_message_from_outcome(&topic, &data) {
                        let _ = event_tx.send(return_message_event);
                    } else if let Some(system_control_cmd_event) = parse_system_control_command_from_outcome(&topic, &data) {
                        let _ = event_tx.send(system_control_cmd_event);
                    } else {
                        let _ = event_tx.send_message(topic, data);
                    }
                }
                Ok(StreamOutcome::Plain(output)) => {
                    let _ = event_tx.send_message(topic_name.clone(), output.clone());
                    if let Some(emit) = emit_func {
                        let _ = event_tx.send(emit(output));
                    }
                }
                Ok(StreamOutcome::None) => {}
                Err(e) => {
                    let _ = event_tx.send_error(e.clone());
                    let output = super::buffer::strip_crlf(line_content).to_string();
                    let _ = event_tx.send_message(topic_name.clone(), output.clone());
                    if let Some(emit) = emit_func {
                        let _ = event_tx.send(emit(output));
                    }
                }
            }
        };

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    let unfinished = buffer_manager.flush_all_unfinished();
                    for (_, topic, data) in unfinished {
                        process_stream_outcome(Ok(StreamOutcome::Emit { topic, data }), "");
                    }
                    break;
                }
                result = reader.read_line(&mut line) => {
                    match result {
                        Ok(0) => {
                            let unfinished = buffer_manager.flush_all_unfinished();
                            for (_, topic, data) in unfinished {
                                process_stream_outcome(Ok(StreamOutcome::Emit { topic, data }), "");
                            }
                            break;
                        }
                        Ok(_) => {
                            process_stream_outcome(buffer_manager.consume_stream_line(&task_id_str, &line), &line);
                            line.clear();
                        },
                        Err(e) => {
                            log::error!("Error reading from {}: {}", topic_name, e);
                            let _ = event_tx.send_error(format!("Error reading from {}: {}", topic_name, e));
                            break;
                        }
                    }
                }
            }
        }
    })
}

