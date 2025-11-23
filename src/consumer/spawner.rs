use crate::backend::{ProtocolBackend, TaskBackend};
use crate::channels::{
    ExecutorInputEventSender, ConsumerEventSender, SubscriptionTopicReceiver,
    ShutdownSender, UserLogSender,
};
use crate::logging::{UserLogEvent, UserLogKind};
use crate::messages::MessageId;
use crate::messages::{ExecutorInputEvent, ExecutorOutputEvent, ConsumerEvent, SubscriptionTopicMessage};
use crate::shutdown_registry::TaskHandle;
use crate::subscription::SubscriptionId;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::consumer_id::ConsumerId;

pub struct ConsumerSpawnHandler {
    pub consumer_id: ConsumerId,
    pub worker_handle: Arc<JoinHandle<()>>,
    pub input_sender: ExecutorInputEventSender,
    pub shutdown_sender: ShutdownSender,
}

pub struct ConsumerSpawner {
    pub consumer_id: ConsumerId,
    pub subscription_id: SubscriptionId,
    pub consumer_name: Arc<str>,
    pub userlog_sender: UserLogSender,
    pub consumer_event_sender: ConsumerEventSender,
    pub view_stdout: bool,
    pub view_stderr: bool,
}

impl ConsumerSpawner {
    pub fn new(
        consumer_id: ConsumerId,
        subscription_id: SubscriptionId,
        consumer_name: Arc<str>,
        userlog_sender: UserLogSender,
        consumer_event_sender: ConsumerEventSender,
        view_stdout: bool,
        view_stderr: bool,
    ) -> Self {
        Self {
            consumer_id,
            subscription_id,
            consumer_name,
            userlog_sender,
            consumer_event_sender,
            view_stdout,
            view_stderr,
        }
    }

    pub async fn spawn(
        self,
        backend: ProtocolBackend,
        shutdown_token: CancellationToken,
        topic_data_receiver: SubscriptionTopicReceiver,
        task_handle: &TaskHandle,
    ) -> Result<ConsumerSpawnHandler, String> {
        let consumer_id: ConsumerId = self.consumer_id.clone();
        let handler_consumer_id = consumer_id.clone();
        let consumer_name: Arc<str> = self.consumer_name.clone();
        let subscription_id = self.subscription_id.clone();
        let userlog_sender = self.userlog_sender.clone();
        let consumer_event_sender = self.consumer_event_sender.clone();
        let view_stdout = self.view_stdout;
        let view_stderr = self.view_stderr;

        let mut backend_handle = backend.spawn(consumer_id.clone(), subscription_id.clone()).await.map_err(|e| {
            log::error!("Failed to spawn consumer backend for consumer {}: {}", consumer_id, e);
            format!("Failed to spawn backend for consumer {}: {}", consumer_id, e)
        })?;

        let input_sender_for_external = backend_handle.input_sender.clone();
        let shutdown_sender_for_external = backend_handle.shutdown_sender.clone();

        let consumer_task_name = format!("consumer_{}", consumer_id);
        let worker_handle = task_handle.run(consumer_task_name, {
            let consumer_id = consumer_id.clone();
            let consumer_name = consumer_name.clone();
            let userlog_sender = userlog_sender.clone();
            let consumer_event_sender = consumer_event_sender.clone();
            let mut topic_data_receiver = topic_data_receiver;

            async move {
                loop {
                    tokio::select! {
                        _ = shutdown_token.cancelled() => {
                            log::info!("Consumer {} received shutdown signal", consumer_id);
                            let _ = backend_handle.shutdown_sender.shutdown();
                            break;
                        },

                        event = backend_handle.event_receiver.recv() => {
                            match event {
                                Some(event) => {
                                    let event: ExecutorOutputEvent = event;

                                    match &event {
                                        ExecutorOutputEvent::Topic { message_id, topic, data, .. } => {
                                            log::debug!(
                                                "Message event for consumer {} on topic '{}': '{}'",
                                                consumer_id,
                                                topic,
                                                data
                                            );

                                            let consumer_topic_result = consumer_event_sender.send(ConsumerEvent::ConsumerTopic {
                                                consumer_id: consumer_id.clone(),
                                                message_id: message_id.clone(),
                                                topic: topic.clone(),
                                                data: data.clone(),
                                            });

                                            if let Err(e) = consumer_topic_result {
                                                log::warn!(
                                                    "Failed to send ConsumerTopic event for '{}': {}",
                                                    consumer_id,
                                                    e
                                                );
                                            }

                                            // アロケーションを避けるため、eq_ignore_ascii_caseを使用
                                            if topic.as_str().eq_ignore_ascii_case("system.pull") {
                                                let requested_topic = crate::topic::Topic::from(data.trim());
                                                if let Err(e) = consumer_event_sender.send(ConsumerEvent::ConsumerRequesting {
                                                    consumer_id: consumer_id.clone(),
                                                    topic: requested_topic,
                                                }) {
                                                    log::warn!(
                                                        "Failed to send ConsumerRequesting event for '{}': {}",
                                                        consumer_id,
                                                        e
                                                    );
                                                }
                                            } else if topic.as_str().eq_ignore_ascii_case("system.result") {
                                                let requested_topic = crate::topic::Topic::from(data.trim());
                                                if let Err(e) = consumer_event_sender.send(ConsumerEvent::ConsumerResultRequesting {
                                                    consumer_id: consumer_id.clone(),
                                                    topic: requested_topic,
                                                }) {
                                                    log::warn!(
                                                        "Failed to send ConsumerResultRequesting event for '{}': {}",
                                                        consumer_id,
                                                        e
                                                    );
                                                }
                                            }
                                        },
                                        ExecutorOutputEvent::Stdout { data, .. } => {
                                            if view_stdout {
                                                let _ = userlog_sender.send(UserLogEvent { consumer_id: consumer_id.to_string(), task_name: consumer_name.clone(), kind: UserLogKind::Stdout, msg: data.clone() });
                                            }
                                        },
                                        ExecutorOutputEvent::Stderr { data, .. } => {
                                            if view_stderr {
                                                let _ = userlog_sender.send(UserLogEvent { consumer_id: consumer_id.to_string(), task_name: consumer_name.clone(), kind: UserLogKind::Stderr, msg: data.clone() });
                                            }
                                        },
                                        ExecutorOutputEvent::Error { error, .. } => {
                                            log::error!("Error event for consumer {}: '{}'", consumer_id, error);
                                        },
                                        ExecutorOutputEvent::Exit { exit_code, .. } => {
                                            log::info!("Exit event for consumer {} with exit code: {}", consumer_id, exit_code);
                                            if let Err(e) = consumer_event_sender.send(ConsumerEvent::ConsumerExit {
                                                consumer_id: consumer_id.clone(),
                                            }) {
                                                log::warn!(
                                                    "Failed to notify consumer exit for '{}': {}",
                                                    consumer_id,
                                                    e
                                                );
                                            }
                                            break;
                                        }
                                    }
                                },
                                None => {
                                    log::info!("Consumer backend event receiver closed for consumer {}", consumer_id);
                                    break;
                                }
                            }
                        },

                        topic_data = topic_data_receiver.recv() => {
                            match topic_data {
                                Some(topic_data) => {
                                    let SubscriptionTopicMessage { topic, data, from_subscription_id } = topic_data;
                                    let input_event = ExecutorInputEvent::Topic {
                                        message_id: MessageId::new(),
                                        consumer_id: consumer_id.clone(),
                                        topic: topic.clone(),
                                        data: data.clone(),
                                        from_subscription_id: from_subscription_id.clone(),
                                    };

                                    if let Err(e) = backend_handle.input_sender.send(input_event) {
                                        log::warn!("Failed to send topic message to consumer backend for consumer {}: {}", consumer_id, e);
                                    }
                                },
                                None => {
                                    log::info!("Topic data receiver closed for consumer {}", consumer_id);
                                    break;
                                }
                            }
                        },
                    }
                }

                log::info!("Consumer {} completed", consumer_id);
            }
        }).map_err(|e| format!("Failed to spawn consumer task: {}", e))?;

        Ok(ConsumerSpawnHandler {
            consumer_id: handler_consumer_id,
            worker_handle,
            input_sender: input_sender_for_external,
            shutdown_sender: shutdown_sender_for_external,
        })
    }
}
