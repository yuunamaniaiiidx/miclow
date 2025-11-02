use anyhow::Error;
use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, stdin};
use tokio_util::sync::CancellationToken;
use crate::task_id::TaskId;
use crate::task_backend::TaskBackend;
use crate::task_backend_handle::TaskBackendHandle;
use crate::executor_event_channel::{ExecutorEvent, ExecutorEventSender, ExecutorEventChannel};
use crate::input_channel::InputChannel;
use crate::system_response_channel::SystemResponseChannel;
use crate::shutdown_channel::ShutdownChannel;
use tokio::io::BufReader as TokioBufReader;
use tokio::task;

#[derive(Clone)]
pub struct InteractiveBackend {
    system_input_topic: String,
}

impl InteractiveBackend {
    pub fn new(system_input_topic: String) -> Self {
        Self {
            system_input_topic,
        }
    }
}

#[async_trait]
impl TaskBackend for InteractiveBackend {
    async fn spawn(&self, task_id: TaskId) -> Result<TaskBackendHandle, Error> {
        let system_input_topic = self.system_input_topic.clone();
        
        let event_channel: ExecutorEventChannel = ExecutorEventChannel::new();
        let input_channel: InputChannel = InputChannel::new();
        let shutdown_channel = ShutdownChannel::new();
        let system_response_channel: SystemResponseChannel = SystemResponseChannel::new();
        
        let event_tx_clone: ExecutorEventSender = event_channel.sender.clone();
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
                                
                                let event = ExecutorEvent::new_message(
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
                                let _ = event_tx_clone.send_error(format!("Error reading from stdin: {}", e));
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
}

