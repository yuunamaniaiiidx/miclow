use super::buffer::{InputBufferManager, StreamOutcome};
use crate::backend::miclowstdio::config::MiclowStdIOConfig;
use crate::backend::TaskBackendHandle;
use crate::channels::{ExecutorInputEventChannel, ExecutorInputEventReceiver, ShutdownChannel};
use crate::channels::{ExecutorOutputEventChannel, ExecutorOutputEventSender};
use crate::message_id::MessageId;
use crate::messages::{ExecutorInputEvent, ExecutorOutputEvent};
use crate::task_id::TaskId;
use anyhow::{Error, Result};
#[cfg(unix)]
use nix::sys::signal::{kill, Signal};
#[cfg(unix)]
use nix::unistd::Pid;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader};
use tokio::process::Command as TokioCommand;
use tokio::task;
use tokio_util::sync::CancellationToken;

pub struct ExecutorInputEventStdio<'a> {
    event: &'a ExecutorInputEvent,
}

impl<'a> ExecutorInputEventStdio<'a> {
    pub fn new(event: &'a ExecutorInputEvent) -> Self {
        Self { event }
    }

    pub fn to_input_lines(&self) -> Vec<String> {
        let lines = self.to_input_lines_raw();

        if lines.len() < 2 {
            panic!(
                "StdIOProtocol validation failed: must have at least 2 lines, got {}",
                lines.len()
            );
        }

        let line_count: usize = lines[1].parse().unwrap_or_else(|_| {
            panic!(
                "StdIOProtocol validation failed: line 2 must be a number, got '{}'",
                lines[1]
            );
        });

        let data_line_count = lines.len() - 2;
        if data_line_count != line_count {
            panic!(
                "StdIOProtocol validation failed: expected {} data lines (from line 2), but got {} (total lines: {})",
                line_count, data_line_count, lines.len()
            );
        }

        lines
    }

    fn to_input_lines_raw(&self) -> Vec<String> {
        if let ExecutorInputEvent::Topic { topic, data, .. } = self.event {
            let mut lines = vec![topic.clone()];
            let data_lines: Vec<&str> = data.lines().collect();
            lines.push(data_lines.len().to_string());
            lines.extend(data_lines.iter().map(|s| s.to_string()));
            return lines;
        }

        unreachable!("ExecutorInputEvent::Topic is the only supported variant");
    }
}

impl<'a> From<&'a ExecutorInputEvent> for ExecutorInputEventStdio<'a> {
    fn from(value: &'a ExecutorInputEvent) -> Self {
        Self::new(value)
    }
}

pub async fn spawn_miclow_stdio_protocol(
    config: &MiclowStdIOConfig,
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

    let event_channel: ExecutorOutputEventChannel = ExecutorOutputEventChannel::new();
    let input_channel: ExecutorInputEventChannel = ExecutorInputEventChannel::new();
    let mut shutdown_channel = ShutdownChannel::new();

    let event_tx_clone: ExecutorOutputEventSender = event_channel.sender.clone();
    let mut input_receiver: ExecutorInputEventReceiver = input_channel.receiver;

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
                let _ = event_tx_clone.send(ExecutorOutputEvent::new_error(
                    MessageId::new(),
                    task_id.clone(),
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
            if view_stdout {
                Some(
                    ExecutorOutputEvent::new_task_stdout
                        as fn(MessageId, TaskId, String) -> ExecutorOutputEvent,
                )
            } else {
                None
            },
        );

        let stderr_worker = spawn_stream_reader(
            TokioBufReader::new(stderr),
            stderr_topic.clone(),
            event_tx_clone.clone(),
            cancel_token.clone(),
            task_id.clone(),
            if view_stderr {
                Some(
                    ExecutorOutputEvent::new_task_stderr
                        as fn(MessageId, TaskId, String) -> ExecutorOutputEvent,
                )
            } else {
                None
            },
        );

        let cancel_input: CancellationToken = cancel_token.clone();
        let event_tx_input: ExecutorOutputEventSender = event_tx_clone.clone();
        let task_id_input = task_id.clone();
        let input_worker = task::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_input.cancelled() => { break; }
                    input_data = input_receiver.recv() => {
                        match input_data {
                            Some(input_data_msg) => {
                                let lines = ExecutorInputEventStdio::from(&input_data_msg).to_input_lines();
                                for line in lines {
                                    let bytes: Vec<u8> = if line.ends_with('\n') {
                                        line.into_bytes()
                                    } else {
                                        format!("{}\n", line).into_bytes()
                                    };
                                    if let Err(e) = stdin_writer.write_all(&bytes).await {
                                        let _ = event_tx_input.send_error(
                                            MessageId::new(),
                                            task_id_input.clone(),
                                            format!("Failed to write to stdin: {}", e),
                                        );
                                        break;
                                    }
                                }
                                if let Err(e) = stdin_writer.flush().await {
                                    let _ = event_tx_input.send_error(
                                        MessageId::new(),
                                        task_id_input.clone(),
                                        format!("Failed to flush stdin: {}", e),
                                    );
                                    break;
                                }
                            },
                            None => break,
                        }
                    }
                }
            }
        });

        let event_tx_status: ExecutorOutputEventSender = event_tx_clone.clone();
        let status_cancel: CancellationToken = cancel_token.clone();
        let task_id_status: TaskId = task_id.clone();
        let status_worker = task::spawn(async move {
            let notify = |res: Result<std::process::ExitStatus, anyhow::Error>| {
                let message_id = MessageId::new();
                let task_id_clone = task_id_status.clone();
                match res {
                    Ok(exit_status) => {
                        let code: i32 = exit_status.code().unwrap_or(-1);
                        let _ = event_tx_status.send_exit(message_id, task_id_clone, code);
                    }
                    Err(e) => {
                        log::error!("Error waiting for process: {}", e);
                        let _ = event_tx_status.send_error(
                            message_id,
                            task_id_clone,
                            format!("Error waiting for process: {}", e),
                        );
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
        input_sender: input_channel.sender,
        shutdown_sender: shutdown_channel.sender,
    })
}

fn spawn_stream_reader<R>(
    mut reader: R,
    topic_name: String,
    event_tx: ExecutorOutputEventSender,
    cancel_token: CancellationToken,
    task_id: TaskId,
    emit_func: Option<fn(MessageId, TaskId, String) -> ExecutorOutputEvent>,
) -> task::JoinHandle<()>
where
    R: tokio::io::AsyncBufRead + Unpin + Send + 'static,
{
    task::spawn(async move {
        let mut buffer_manager = InputBufferManager::new();
        let mut line = String::new();
        let task_id_str = task_id.to_string();
        let topic_name_clone = topic_name.clone();
        let event_tx_clone = event_tx.clone();
        let task_id_clone = task_id.clone();

        let process_stream_outcome =
            move |outcome: Result<StreamOutcome, String>, line_content: &str| {
                let message_id = MessageId::new();
                let task_id_for_outcome = task_id_clone.clone();
                let event_tx_for_outcome = event_tx_clone.clone();
                let topic_name_for_outcome = topic_name_clone.clone();
                match outcome {
                    Ok(StreamOutcome::Emit { topic, data }) => {
                        let _ = event_tx_for_outcome.send_message(
                            message_id.clone(),
                            task_id_for_outcome.clone(),
                            topic,
                            data,
                        );
                    }
                    Ok(StreamOutcome::Plain(output)) => {
                        let _ = event_tx_for_outcome.send_message(
                            message_id.clone(),
                            task_id_for_outcome.clone(),
                            topic_name_for_outcome.clone(),
                            output.clone(),
                        );
                        if let Some(emit) = emit_func {
                            let _ = event_tx_for_outcome.send(emit(
                                message_id.clone(),
                                task_id_for_outcome.clone(),
                                output,
                            ));
                        }
                    }
                    Ok(StreamOutcome::None) => {}
                    Err(e) => {
                        let _ = event_tx_for_outcome.send_error(
                            message_id.clone(),
                            task_id_for_outcome.clone(),
                            e.clone(),
                        );
                        let output = super::buffer::strip_crlf(line_content).to_string();
                        let _ = event_tx_for_outcome.send_message(
                            message_id.clone(),
                            task_id_for_outcome.clone(),
                            topic_name_for_outcome.clone(),
                            output.clone(),
                        );
                        if let Some(emit) = emit_func {
                            let _ = event_tx_for_outcome.send(emit(
                                message_id.clone(),
                                task_id_for_outcome.clone(),
                                output,
                            ));
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
                            let _ = event_tx.send_error(
                                MessageId::new(),
                                task_id.clone(),
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
