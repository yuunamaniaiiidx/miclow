use crate::backend::{ProtocolBackend, TaskBackend};
use crate::channels::{
    ConsumerEventSender, ShutdownSender, SubscriptionTopicReceiver, TopicNotificationReceiver,
    UserLogSender,
};
use crate::logging::{UserLogEvent, UserLogKind};
use crate::messages::MessageId;
use crate::messages::{
    ConsumerEvent, ExecutorInputEvent, ExecutorOutputEvent, SubscriptionTopicMessage, SystemCommand,
};
use crate::shutdown_manager::{TaskController, TaskHandle};
use crate::subscription::SubscriptionId;
use crate::topic::Topic;
use std::collections::HashSet;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use super::consumer_id::ConsumerId;

pub struct ConsumerSpawnHandler {
    pub consumer_id: ConsumerId,
    pub worker_task: TaskController,
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
        topic_notification_receiver: TopicNotificationReceiver,
    ) -> Result<ConsumerSpawnHandler, String> {
        let consumer_id: ConsumerId = self.consumer_id.clone();
        let handler_consumer_id = consumer_id.clone();
        let consumer_name: Arc<str> = self.consumer_name.clone();
        let subscription_id = self.subscription_id.clone();
        let userlog_sender = self.userlog_sender.clone();
        let consumer_event_sender = self.consumer_event_sender.clone();
        let view_stdout = self.view_stdout;
        let view_stderr = self.view_stderr;

        let mut backend_handle = backend
            .spawn(consumer_id.clone(), subscription_id.clone())
            .await
            .map_err(|e| {
                log::error!(
                    "Failed to spawn consumer backend for consumer {}: {}",
                    consumer_id,
                    e
                );
                format!(
                    "Failed to spawn backend for consumer {}: {}",
                    consumer_id, e
                )
            })?;

        let shutdown_sender_for_external = backend_handle.shutdown_sender.clone();

        let consumer_task_name = format!("consumer_{}", consumer_id);
        let worker_task = task_handle.run(consumer_task_name, move |_task_token| {
            let consumer_id = consumer_id.clone();
            let consumer_name = consumer_name.clone();
            let userlog_sender = userlog_sender.clone();
            let consumer_event_sender = consumer_event_sender.clone();
            let mut topic_data_receiver = topic_data_receiver;
            let mut topic_notification_receiver = topic_notification_receiver;

            async move {
                let mut waiting_topics: HashSet<Topic> = HashSet::new();
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

                                            if let Some(system_command) = SystemCommand::parse(topic.as_str(), &data) {
                                                if let SystemCommand::PopAwait(awaited_topic) = &system_command {
                                                    waiting_topics.insert(awaited_topic.clone());
                                                    if let Err(e) = consumer_event_sender.send(ConsumerEvent::ConsumerRequesting {
                                                        consumer_id: consumer_id.clone(),
                                                        topic: awaited_topic.clone(),
                                                    }) {
                                                        log::warn!(
                                                        "Failed to send system.pop_await converted pop for '{}': {}",
                                                            consumer_id,
                                                            e
                                                        );
                                                    }
                                                }
                                                if let Some(event) = ConsumerEvent::from_system_command(
                                                    consumer_id.clone(),
                                                    &system_command,
                                                ) {
                                                    if let Err(e) = consumer_event_sender.send(event) {
                                                        log::warn!(
                                                            "Failed to send system command event for '{}': {}",
                                                            consumer_id,
                                                            e
                                                        );
                                                    }
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
                                    let is_waiting_topic = waiting_topics.contains(&topic);

                                    if is_waiting_topic && data.is_none() {
                                        log::debug!(
                                            "Discarding empty data for awaiting topic '{}' on consumer {}",
                                            topic,
                                            consumer_id
                                        );
                                        continue;
                                    }

                                    if is_waiting_topic && data.is_some() {
                                        waiting_topics.remove(&topic);
                                    }

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
                        topic_notification = topic_notification_receiver.recv() => {
                            match topic_notification {
                                Some(topic) => {
                                    if waiting_topics.contains(&topic) {
                                        log::debug!(
                                            "Consumer {} received notification for awaited topic '{}', issuing pop",
                                            consumer_id,
                                            topic
                                        );
                                        if let Err(e) = consumer_event_sender.send(ConsumerEvent::ConsumerRequesting {
                                            consumer_id: consumer_id.clone(),
                                            topic: topic.clone(),
                                        }) {
                                            log::warn!(
                                                "Failed to send pop request from topic notification for '{}': {}",
                                                consumer_id,
                                                e
                                            );
                                        }
                                    } else {
                                        log::trace!(
                                            "Consumer {} ignoring notification for non-awaited topic '{}'",
                                            consumer_id,
                                            topic
                                        );
                                    }
                                },
                                None => {
                                    log::info!("Topic notification receiver closed for consumer {}", consumer_id);
                                    break;
                                }
                            }
                        },
                    }
                }

                log::info!("Consumer {} completed", consumer_id);
            }
        })
        .map_err(|e| format!("Failed to spawn consumer task: {}", e))?;

        Ok(ConsumerSpawnHandler {
            consumer_id: handler_consumer_id,
            worker_task,
            shutdown_sender: shutdown_sender_for_external,
        })
    }
}
