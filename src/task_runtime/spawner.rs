use crate::backend::{ProtocolBackend, SpawnBackendResult, TaskBackend};
use crate::channels::{
    ExecutorInputEventChannel, ExecutorOutputEventChannel,
    ShutdownChannel, SystemResponseChannel, UserLogSender,
};
use crate::logging::{UserLogEvent, UserLogKind};
use crate::message_id::MessageId;
use crate::messages::{ExecutorInputEvent, ExecutorOutputEvent, SystemResponseEvent};
use crate::system_control::SystemControlQueue;
use crate::task_id::TaskId;
use crate::topic_broker::TopicBroker;
use tokio_util::sync::CancellationToken;

use super::executor::TaskExecutor;

pub struct TaskSpawner {
    pub task_id: TaskId,
    pub topic_manager: TopicBroker,
    pub system_control_manager: SystemControlQueue,
    pub task_executor: TaskExecutor,
    pub task_name: String,
    pub userlog_sender: UserLogSender,
}

impl TaskSpawner {
    pub fn new(
        task_id: TaskId,
        topic_manager: TopicBroker,
        system_control_manager: SystemControlQueue,
        task_executor: TaskExecutor,
        task_name: String,
        userlog_sender: UserLogSender,
    ) -> Self {
        Self {
            task_id,
            topic_manager,
            system_control_manager,
            task_executor,
            task_name,
            userlog_sender,
        }
    }

    pub async fn spawn_backend(
        self,
        backend: ProtocolBackend,
        shutdown_token: CancellationToken,
        subscribe_topics: Option<Vec<String>>,
        caller_task_id: Option<TaskId>,
    ) -> SpawnBackendResult {
        let task_id: TaskId = self.task_id.clone();
        let task_name: String = self.task_name.clone();
        let topic_manager: TopicBroker = self.topic_manager;
        let system_control_manager: SystemControlQueue = self.system_control_manager;
        let task_executor: TaskExecutor = self.task_executor;
        let userlog_sender = self.userlog_sender.clone();

        let mut backend_handle = match backend.spawn(task_id.clone(), caller_task_id.clone()).await {
            Ok(handle) => handle,
            Err(e) => {
                log::error!("Failed to spawn task backend for task {}: {}", task_id, e);
                let input_channel: ExecutorInputEventChannel = ExecutorInputEventChannel::new();
                let shutdown_channel = ShutdownChannel::new();
                return SpawnBackendResult {
                    worker_handle: tokio::task::spawn(async {}),
                    input_sender: input_channel.sender,
                    shutdown_sender: shutdown_channel.sender,
                };
            }
        };

        let system_response_channel: SystemResponseChannel = SystemResponseChannel::new();
        let mut system_response_receiver = system_response_channel.receiver;
        backend_handle.system_response_sender = system_response_channel.sender;

        let input_sender_for_external = backend_handle.input_sender.clone();
        let shutdown_sender_for_external = backend_handle.shutdown_sender.clone();

        let worker_handle = tokio::task::spawn(async move {
            let topic_data_channel: ExecutorOutputEventChannel = ExecutorOutputEventChannel::new();
            let mut topic_data_receiver = topic_data_channel.receiver;

            if let Some(topics) = subscribe_topics {
                log::info!(
                    "Processing initial topic subscriptions for task {}: {:?}",
                    task_id,
                    topics
                );
                for topic in topics {
                    topic_manager
                        .add_subscriber(
                            topic.clone(),
                            task_id.clone(),
                            topic_data_channel.sender.clone(),
                        )
                        .await;
                    log::info!(
                        "Added initial topic subscription for '{}' from task {}",
                        topic,
                        task_id
                    );
                }
            }

            loop {
                tokio::select! {
                    biased;

                    _ = shutdown_token.cancelled() => {
                        log::info!("Task {} received shutdown signal", task_id);
                        let _ = backend_handle.shutdown_sender.shutdown();
                        break;
                    },

                    event = backend_handle.event_receiver.recv() => {
                        match event {
                            Some(event) => {
                                let event: ExecutorOutputEvent = event;

                                match &event {
                                    ExecutorOutputEvent::Message { topic, data, .. } => {
                                        log::info!("Message event for task {} on topic '{}': '{}'", task_id, topic, data);
                                        match topic_manager.broadcast_message(event.clone()).await {
                                            Ok(success_count) => {
                                                log::info!("Broadcasted message from task {} to {} subscribers on topic '{}'", task_id, success_count, topic);
                                            },
                                            Err(e) => {
                                                log::error!("Failed to broadcast message from task {} on topic '{}': {}", task_id, topic, e);
                                            }
                                        }
                                    },
                                    ExecutorOutputEvent::SystemControl { action, .. } => {
                                        log::info!("SystemControl detected from task {}", task_id);
                                        if let Err(e) = system_control_manager.send_system_control_action(
                                            action.clone(),
                                            task_id.clone(),
                                            backend_handle.system_response_sender.clone(),
                                            topic_data_channel.sender.clone(),
                                        ).await {
                                            log::warn!("Failed to send system control action to worker (task {}): {}", task_id, e);
                                        } else {
                                            log::info!("Sent system control action to worker for task {}", task_id);
                                        }
                                    },
                                    ExecutorOutputEvent::ReturnMessage { return_to_task_id, data, .. } => {
                                        log::info!("ReturnMessage received from task {}: '{}'", task_id, data);
                                        if let Some(input_sender) = task_executor.get_input_sender_by_task_id(&return_to_task_id).await {
                                            if let Err(e) = input_sender.send(
                                                ExecutorInputEvent::FunctionResponse {
                                                    message_id: MessageId::new(),
                                                    task_id: return_to_task_id.clone(),
                                                    function_name: task_name.clone(),
                                                    data: data.clone(),
                                                }
                                            ) {
                                                log::warn!(
                                                    "Failed to send function response to task {}: {}",
                                                    return_to_task_id,
                                                    e
                                                );
                                            } else {
                                                log::info!(
                                                    "Sent function response to task {} from task {}",
                                                    return_to_task_id,
                                                    task_id
                                                );
                                            }
                                        } else {
                                            log::warn!(
                                                "ReturnMessage received but return_to_task_id {} is not found in running tasks",
                                                return_to_task_id
                                            );
                                        }
                                    },
                                    ExecutorOutputEvent::TaskStdout { data, .. } => {
                                        let flags = task_executor.get_view_flags_by_task_id(&task_id).await;
                                        if let Some((view_stdout, _)) = flags {
                                            if view_stdout {
                                                let _ = userlog_sender.send(UserLogEvent { task_id: task_id.to_string(), task_name: task_name.clone(), kind: UserLogKind::Stdout, msg: data.clone() });
                                            }
                                        }
                                    },
                                    ExecutorOutputEvent::TaskStderr { data, .. } => {
                                        let flags = task_executor.get_view_flags_by_task_id(&task_id).await;
                                        if let Some((_, view_stderr)) = flags {
                                            if view_stderr {
                                                let _ = userlog_sender.send(UserLogEvent { task_id: task_id.to_string(), task_name: task_name.clone(), kind: UserLogKind::Stderr, msg: data.clone() });
                                            }
                                        }
                                    },
                                    ExecutorOutputEvent::Error { error, .. } => {
                                        log::error!("Error event for task {}: '{}'", task_id, error);
                                    },
                                    ExecutorOutputEvent::Exit { exit_code, .. } => {
                                        log::info!("Exit event for task {} with exit code: {}", task_id, exit_code);

                                        let removed_topics: Vec<String> = topic_manager.remove_all_subscriptions_by_task(task_id.clone()).await;
                                        log::info!("Task {} exited, removed {} topic subscriptions", task_id, removed_topics.len());

                                        if let Some(_removed_task) = task_executor.unregister_task_by_task_id(&task_id).await {
                                            log::info!("Removed task with TaskId={} (Human name index updated)", task_id);
                                        } else {
                                            log::warn!("Task with TaskId={} not found in executor during cleanup", task_id);
                                        }
                                    }
                                }
                            },
                            None => {
                                log::info!("Task backend event receiver closed for task {}", task_id);
                                break;
                            }
                        }
                    },

                    topic_data = topic_data_receiver.recv() => {
                        match topic_data {
                            Some(topic_data) => {
                                if let Some(data) = topic_data.data() {
                                    if let Some(topic_name) = topic_data.topic() {
                                        if let Err(e) = backend_handle.input_sender.send(
                                            ExecutorInputEvent::Topic {
                                                message_id: MessageId::new(),
                                                task_id: task_id.clone(),
                                                topic: topic_name.clone(),
                                                data: data.clone(),
                                            },
                                        ) {
                                            log::warn!("Failed to send topic message to task backend for task {}: {}", task_id, e);
                                        }
                                    } else {
                                        log::warn!("Topic data received without topic name for task {}, skipping", task_id);
                                        continue;
                                    }
                                }
                            },
                            None => {
                                log::info!("Topic data receiver closed for task {}", task_id);
                                break;
                            }
                        }
                    },

                    system_response = system_response_receiver.recv() => {
                        match system_response {
                            Some(SystemResponseEvent::SystemResponse { topic, status, data }) => {
                                log::info!("SystemResponse event for task {}: topic='{}', status='{}', data='{}'", task_id, topic, status, data);
                                if let Err(e) = backend_handle.input_sender.send(
                                    ExecutorInputEvent::SystemResponse {
                                        message_id: MessageId::new(),
                                        task_id: task_id.clone(),
                                        topic,
                                        status,
                                        data,
                                    },
                                ) {
                                    log::warn!("Failed to send system response message to task backend for task {}: {}", task_id, e);
                                }
                            },
                            Some(SystemResponseEvent::SystemError { topic, status, error }) => {
                                log::error!("SystemError event for task {}: topic='{}', status='{}', error='{}'", task_id, topic, status, error);
                                if let Err(e) = backend_handle.input_sender.send(
                                    ExecutorInputEvent::SystemResponse {
                                        message_id: MessageId::new(),
                                        task_id: task_id.clone(),
                                        topic,
                                        status,
                                        data: error,
                                    },
                                ) {
                                    log::warn!("Failed to send system error message to task backend for task {}: {}", task_id, e);
                                }
                            },
                            None => {
                                log::info!("SystemResponse receiver closed for task {}", task_id);
                                break;
                            }
                        }
                    }
                }
            }

            log::info!("Task {} completed", task_id);
        });

        SpawnBackendResult {
            worker_handle,
            input_sender: input_sender_for_external,
            shutdown_sender: shutdown_sender_for_external,
        }
    }
}
