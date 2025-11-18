use crate::backend::interactive::config::InteractiveConfig;
use crate::backend::TaskBackendHandle;
use crate::channels::{
    ExecutorInputEventChannel, ExecutorOutputEventChannel, ShutdownChannel, SystemResponseChannel,
};
use crate::message_id::MessageId;
use crate::messages::ExecutorOutputEvent;
use crate::task_id::TaskId;
use anyhow::{Error, Result};
use tokio::io::{stdin, AsyncBufReadExt, BufReader as TokioBufReader};
use tokio::task;
use tokio_util::sync::CancellationToken;

pub async fn spawn_interactive_protocol(
    config: &InteractiveConfig,
    task_id: TaskId,
) -> Result<TaskBackendHandle, Error> {
    let system_input_topic = config.system_input_topic.clone();

    let event_channel: ExecutorOutputEventChannel = ExecutorOutputEventChannel::new();
    let input_channel: ExecutorInputEventChannel = ExecutorInputEventChannel::new();
    let shutdown_channel = ShutdownChannel::new();
    let system_response_channel: SystemResponseChannel = SystemResponseChannel::new();

    let event_tx_clone = event_channel.sender.clone();
    let cancel_token: CancellationToken = CancellationToken::new();
    let mut shutdown_receiver = shutdown_channel.receiver;

    task::spawn(async move {
        let stdin = stdin();
        let reader = TokioBufReader::new(stdin);
        let mut lines = reader.lines();

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    log::info!("Interactive mode received shutdown signal for task {}", task_id);
                    break;
                }
                _ = shutdown_receiver.recv() => {
                    log::info!("Interactive mode received shutdown signal via channel for task {}", task_id);
                    cancel_token.cancel();
                    break;
                }
                line_result = lines.next_line() => {
                    match line_result {
                        Ok(Some(line)) => {
                            let trimmed = line.trim();
                            if trimmed.is_empty() {
                                continue;
                            }

                            log::info!("Sending message topic:'{}' data:'{}'", system_input_topic, trimmed);

                            let event = ExecutorOutputEvent::new_message(
                                MessageId::new(),
                                task_id.clone(),
                                system_input_topic.clone(),
                                trimmed.to_string(),
                            );

                            if let Err(e) = event_tx_clone.send(event) {
                                log::error!("Failed to send event from interactive backend: {}", e);
                                break;
                            }
                        }
                        Ok(None) => {
                            log::info!("Interactive mode stdin closed for task {}", task_id);
                            break;
                        }
                        Err(e) => {
                            log::error!("Error reading from stdin for task {}: {}", task_id, e);
                            let _ = event_tx_clone.send_error(
                                MessageId::new(),
                                task_id.clone(),
                                format!("Error reading from stdin: {}", e),
                            );
                            break;
                        }
                    }
                }
            }
        }

        log::info!("Interactive mode task {} completed", task_id);
    });

    Ok(TaskBackendHandle {
        event_receiver: event_channel.receiver,
        event_sender: event_channel.sender,
        system_response_sender: system_response_channel.sender,
        input_sender: input_channel.sender,
        shutdown_sender: shutdown_channel.sender,
    })
}
