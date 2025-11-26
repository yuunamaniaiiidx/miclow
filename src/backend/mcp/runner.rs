use crate::backend::mcp::config::MCPConfig;
use crate::backend::TaskBackendHandle;
use crate::channels::{ExecutorInputEventChannel, ExecutorInputEventReceiver, ShutdownChannel};
use crate::channels::{ExecutorOutputEventChannel, ExecutorOutputEventSender};
use crate::consumer::ConsumerId;
use crate::messages::MessageId;
use crate::messages::{ExecutorInputEvent, ExecutorOutputEvent};
use crate::subscription::SubscriptionId;
use crate::topic::Topic;
use anyhow::{Error, Result};
#[cfg(unix)]
use nix::sys::signal::{kill, Signal};
#[cfg(unix)]
use nix::unistd::Pid;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader};
use tokio::process::Command as TokioCommand;
use tokio::task;
use tokio_util::sync::CancellationToken;

pub async fn spawn_mcp_protocol(
    config: &MCPConfig,
    consumer_id: ConsumerId,
    subscription_id: SubscriptionId,
) -> Result<TaskBackendHandle, Error> {
    let command = config.command.clone();
    let args = config.args.clone();
    let working_directory = config.working_directory.clone();
    let environment = config.environment.clone();
    let tools = config.tools.clone();
    let view_stdout = config.view_stdout;
    let view_stderr = config.view_stderr;

    let event_channel: ExecutorOutputEventChannel = ExecutorOutputEventChannel::new();
    let input_channel: ExecutorInputEventChannel = ExecutorInputEventChannel::new();
    let mut shutdown_channel = ShutdownChannel::new();

    let event_tx_clone: ExecutorOutputEventSender = event_channel.sender.clone();
    let input_receiver = input_channel.receiver;

    task::spawn(async move {
        let mut command_builder = TokioCommand::new(command.as_ref());

        for arg in &args {
            command_builder.arg(arg.as_ref());
        }

        if let Some(working_dir) = &working_directory {
            command_builder.current_dir(working_dir.as_ref());
        }

        if let Some(env_vars) = &environment {
            for (key, value) in env_vars {
                command_builder.env(key, value);
            }
        }

        command_builder.env("MICLOW_CONSUMER_ID", consumer_id.to_string());

        let mut child = match command_builder
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .stdin(Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(e) => {
                let _ = event_tx_clone.send(ExecutorOutputEvent::new_error(
                    MessageId::new(),
                    consumer_id.clone(),
                    format!("Failed to start MCP server process '{}': {}", command, e),
                ));
                return;
            }
        };

        let stdout: tokio::process::ChildStdout = child.stdout.take().unwrap();
        let stderr: tokio::process::ChildStderr = child.stderr.take().unwrap();
        let stdin_writer = child.stdin.take().unwrap();

        let cancel_token: CancellationToken = CancellationToken::new();

        // 起動時に各ツール名に対してsystem.pop_awaitを送信
        for tool_name in &tools {
            let tool_topic = Topic::from(tool_name.as_ref());
            let pop_await_message = format!("{}", tool_topic.as_str());
            
            let message_id = MessageId::new();
            let event = ExecutorOutputEvent::new_message(
                message_id,
                consumer_id.clone(),
                subscription_id.clone(),
                "system.pop_await",
                pop_await_message,
            );
            
            if let Err(e) = event_tx_clone.send(event) {
                log::warn!(
                    "Failed to send system.pop_await for tool '{}': {}",
                    tool_name,
                    e
                );
            }
        }

        // MCPサーバーへの入力処理（ツール名のトピックからメッセージを受け取り、stdinに書き込み）
        let input_worker = spawn_mcp_input_handler(
            stdin_writer,
            tools.clone(),
            input_receiver,
            event_tx_clone.clone(),
            cancel_token.clone(),
            consumer_id.clone(),
        );

        // MCPサーバーとのJSON-RPC通信処理（stdoutから読み取り）
        let mcp_worker = spawn_mcp_communication(
            TokioBufReader::new(stdout),
            tools.clone(),
            event_tx_clone.clone(),
            cancel_token.clone(),
            consumer_id.clone(),
            subscription_id.clone(),
            view_stdout,
        );

        // stderrの読み取り処理
        let stderr_worker = spawn_stream_reader(
            TokioBufReader::new(stderr),
            "stderr".to_string(),
            event_tx_clone.clone(),
            cancel_token.clone(),
            consumer_id.clone(),
            subscription_id.clone(),
            if view_stderr {
                Some(
                    ExecutorOutputEvent::new_task_stderr
                        as fn(MessageId, ConsumerId, String) -> ExecutorOutputEvent,
                )
            } else {
                None
            },
        );

        // プロセス終了待機処理
        let event_tx_status: ExecutorOutputEventSender = event_tx_clone.clone();
        let status_cancel: CancellationToken = cancel_token.clone();
        let consumer_id_status: ConsumerId = consumer_id.clone();
        let status_worker = task::spawn(async move {
            let notify = |res: Result<std::process::ExitStatus, anyhow::Error>| {
                let message_id = MessageId::new();
                let consumer_id_clone = consumer_id_status.clone();
                match res {
                    Ok(exit_status) => {
                        let code: i32 = exit_status.code().unwrap_or(-1);
                        let _ = event_tx_status.send_exit(message_id, consumer_id_clone, code);
                    }
                    Err(e) => {
                        log::error!("Error waiting for MCP server process: {}", e);
                        let _ = event_tx_status.send_error(
                            message_id,
                            consumer_id_clone,
                            format!("Error waiting for MCP server process: {}", e),
                        );
                    }
                }
            };
            tokio::select! {
                _ = shutdown_channel.receiver.recv() => {
                    log::info!("Shutdown signal received for consumer {}, attempting graceful termination", consumer_id_status);

                    #[cfg(unix)]
                    let pid_opt = child.id().map(|id| Pid::from_raw(id as i32));
                    #[cfg(not(unix))]
                    let pid_opt: Option<()> = None;

                    #[cfg(unix)]
                    let graceful_shutdown_attempted = if let Some(pid) = pid_opt {
                        match kill(pid, Signal::SIGTERM) {
                            Ok(_) => {
                                log::info!("Sent SIGTERM to child process {} (consumer {}), waiting for graceful shutdown", pid, consumer_id_status);
                                true
                            }
                            Err(e) => {
                                log::warn!("Failed to send SIGTERM to child process {} (consumer {}): {}, will use SIGKILL", pid, consumer_id_status, e);
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
                                            log::info!("Child process {} (consumer {}) exited gracefully after SIGTERM", pid, consumer_id_status);
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
                                        log::warn!("Error waiting for child process (consumer {}): {}", consumer_id_status, e);
                                        notify(Err(anyhow::Error::from(e)));
                                        status_cancel.cancel();
                                        return;
                                    }
                                }
                            }
                            _ = tokio::time::sleep(graceful_timeout) => {
                                #[cfg(unix)]
                                if let Some(pid) = pid_opt {
                                    log::warn!("Child process {} (consumer {}) did not exit within timeout, sending SIGKILL", pid, consumer_id_status);
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
                        log::info!("Forcing termination of child process for consumer {}", consumer_id_status);
                        let _ = child.kill().await;
                        notify(child.wait().await.map_err(anyhow::Error::from));
                        status_cancel.cancel();
                    }
                }
                status = child.wait() => {
                    notify(status.map_err(anyhow::Error::from));
                }
            }
        });

        let _ = status_worker.await;

        let stream_read_timeout = tokio::time::Duration::from_millis(500);
        tokio::select! {
            _ = mcp_worker => {
                log::debug!("MCP communication completed for consumer {}", consumer_id);
            }
            _ = tokio::time::sleep(stream_read_timeout) => {
                log::debug!("MCP communication timeout for consumer {}, continuing", consumer_id);
            }
        }
        tokio::select! {
            _ = stderr_worker => {
                log::debug!("stderr reader completed for consumer {}", consumer_id);
            }
            _ = tokio::time::sleep(stream_read_timeout) => {
                log::debug!("stderr reader timeout for consumer {}, continuing", consumer_id);
            }
        }

        cancel_token.cancel();
        let _ = input_worker.await;
    });

    Ok(TaskBackendHandle {
        event_receiver: event_channel.receiver,
        input_sender: input_channel.sender,
        shutdown_sender: shutdown_channel.sender,
    })
}

fn spawn_stream_reader<R>(
    mut reader: R,
    topic_name: String,
    event_tx: ExecutorOutputEventSender,
    cancel_token: CancellationToken,
    consumer_id: ConsumerId,
    _subscription_id: SubscriptionId,
    emit_func: Option<fn(MessageId, ConsumerId, String) -> ExecutorOutputEvent>,
) -> task::JoinHandle<()>
where
    R: tokio::io::AsyncBufRead + Unpin + Send + 'static,
{
    task::spawn(async move {
        let mut line = String::new();
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    break;
                }
                result = reader.read_line(&mut line) => {
                    match result {
                        Ok(0) => {
                            break;
                        }
                        Ok(_) => {
                            let trimmed = line.trim_end();
                            if !trimmed.is_empty() {
                                let message_id = MessageId::new();
                                if let Some(emit) = emit_func {
                                    let _ = event_tx.send(emit(
                                        message_id.clone(),
                                        consumer_id.clone(),
                                        trimmed.to_string(),
                                    ));
                                }
                            }
                            line.clear();
                        }
                        Err(e) => {
                            log::error!("Error reading from {}: {}", topic_name, e);
                            let _ = event_tx.send_error(
                                MessageId::new(),
                                consumer_id.clone(),
                                format!("Error reading from {}: {}", topic_name, e),
                            );
                            break;
                        }
                    }
                }
            }
        }
    })
}

fn spawn_mcp_communication<R>(
    mut reader: R,
    tools: Vec<Arc<str>>,
    event_tx: ExecutorOutputEventSender,
    cancel_token: CancellationToken,
    consumer_id: ConsumerId,
    subscription_id: SubscriptionId,
    view_stdout: bool,
) -> task::JoinHandle<()>
where
    R: tokio::io::AsyncBufRead + Unpin + Send + 'static,
{
    task::spawn(async move {
        let mut line = String::new();
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    break;
                }
                result = reader.read_line(&mut line) => {
                    match result {
                        Ok(0) => {
                            break;
                        }
                        Ok(_) => {
                            let trimmed = line.trim_end();
                            if !trimmed.is_empty() {
                                if view_stdout {
                                    let message_id = MessageId::new();
                                    let _ = event_tx.send(ExecutorOutputEvent::new_task_stdout(
                                        message_id,
                                        consumer_id.clone(),
                                        trimmed.to_string(),
                                    ));
                                }
                                
                                // JSON-RPCメッセージをパース
                                if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(trimmed) {
                                    // MCPメッセージの処理
                                    // ツール呼び出し結果の処理（MCPサーバーからのレスポンス）
                                    if json_value.get("result").is_some() {
                                        // JSON-RPCレスポンスからツール名を取得
                                        // リクエストIDを使ってツール名を追跡する必要があるが、
                                        // 簡易実装として、すべてのツールに対して結果を送信
                                        // 実際の実装では、リクエストIDとツール名のマッピングを保持する必要がある
                                        for tool_name in &tools {
                                            // ツール呼び出し結果をreturn.{toolname}トピックに送信
                                            let return_topic = Topic::from(format!("return.{}", tool_name));
                                            let result_data = serde_json::to_string(&json_value)
                                                .unwrap_or_else(|_| "{}".to_string());
                                            
                                            let message_id = MessageId::new();
                                            let event = ExecutorOutputEvent::new_message(
                                                message_id,
                                                consumer_id.clone(),
                                                subscription_id.clone(),
                                                return_topic,
                                                result_data.clone(),
                                            );
                                            
                                            if let Err(e) = event_tx.send(event) {
                                                log::warn!("Failed to send tool result for '{}': {}", tool_name, e);
                                            } else {
                                                // 結果送信後、再度system.pop_awaitでツール名を登録
                                                let tool_topic = Topic::from(tool_name.as_ref());
                                                let pop_await_message = format!("{}", tool_topic.as_str());
                                                
                                                let message_id = MessageId::new();
                                                let event = ExecutorOutputEvent::new_message(
                                                    message_id,
                                                    consumer_id.clone(),
                                                    subscription_id.clone(),
                                                    "system.pop_await",
                                                    pop_await_message,
                                                );
                                                
                                                if let Err(e) = event_tx.send(event) {
                                                    log::warn!(
                                                        "Failed to re-register system.pop_await for tool '{}': {}",
                                                        tool_name,
                                                        e
                                                    );
                                                }
                                                // 最初のツールにのみ送信（簡易実装）
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            line.clear();
                        }
                        Err(e) => {
                            log::error!("Error reading MCP message: {}", e);
                            let _ = event_tx.send_error(
                                MessageId::new(),
                                consumer_id.clone(),
                                format!("Error reading MCP message: {}", e),
                            );
                            break;
                        }
                    }
                }
            }
        }
    })
}

fn spawn_mcp_input_handler<W>(
    mut writer: W,
    tools: Vec<Arc<str>>,
    mut input_receiver: ExecutorInputEventReceiver,
    event_tx: ExecutorOutputEventSender,
    cancel_token: CancellationToken,
    consumer_id: ConsumerId,
) -> task::JoinHandle<()>
where
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    task::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    break;
                }
                input_data = input_receiver.recv() => {
                    match input_data {
                        Some(ExecutorInputEvent::Topic { topic, data, .. }) => {
                            // ツール名のトピックか確認
                            let tool_name = topic.as_str();
                            if tools.iter().any(|t| t.as_ref() == tool_name) {
                                // データをJSON-RPCリクエストとして解釈してMCPサーバーに送信
                                if let Some(data_str) = data {
                                    let json_line = format!("{}\n", data_str.as_ref());
                                    if let Err(e) = writer.write_all(json_line.as_bytes()).await {
                                        let _ = event_tx.send_error(
                                            MessageId::new(),
                                            consumer_id.clone(),
                                            format!("Failed to write to MCP server stdin: {}", e),
                                        );
                                        break;
                                    }
                                    if let Err(e) = writer.flush().await {
                                        let _ = event_tx.send_error(
                                            MessageId::new(),
                                            consumer_id.clone(),
                                            format!("Failed to flush MCP server stdin: {}", e),
                                        );
                                        break;
                                    }
                                }
                            }
                        }
                        None => {
                            break;
                        }
                    }
                }
            }
        }
    })
}
