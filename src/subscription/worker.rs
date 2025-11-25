use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use crate::topic::Topic;

use crate::channels::{
    ConsumerEventChannel, ConsumerEventSender,
    SubscriptionTopicChannel, TopicNotificationChannel,
};
use crate::messages::{ExecutorOutputEvent, ConsumerEvent, SubscriptionTopicMessage};
use crate::consumer::{ConsumerId, ConsumerSpawner};
use crate::shutdown_registry::TaskHandle;
use crate::subscription::context::ConsumerStartContext;
use crate::subscription::consumer_registry::{ManagedConsumer, ConsumerRegistry};
use crate::subscription::SubscriptionId;
use tokio_util::sync::CancellationToken;
use futures::future;

pub struct SubscriptionWorker {
    subscription_id: SubscriptionId,
    task_name: Arc<str>,
    desired_instances: u32,
    start_context: ConsumerStartContext,
    shutdown_token: CancellationToken,
    task_handle: TaskHandle,
}

impl SubscriptionWorker {
    pub fn new(
        subscription_id: SubscriptionId,
        task_name: Arc<str>,
        desired_instances: u32,
        start_context: ConsumerStartContext,
        shutdown_token: CancellationToken,
        task_handle: TaskHandle,
    ) -> Self {
        Self {
            subscription_id,
            task_name,
            desired_instances,
            start_context,
            shutdown_token,
            task_handle,
        }
    }

    pub async fn run(self) {
        let SubscriptionWorker {
            subscription_id,
            task_name,
            desired_instances,
            start_context,
            shutdown_token,
            task_handle,
        } = self;

        let request_topic_to_from_subscription: HashMap<Topic, SubscriptionId> = HashMap::new();

        let topic_manager = start_context.topic_manager.clone();

        let ConsumerEventChannel {
            sender: consumer_event_sender,
            receiver: mut consumer_event_receiver,
        } = ConsumerEventChannel::new();

        let mut consumer_registry = ConsumerRegistry::new();
        let target_instances = desired_instances;

        // 初期起動: 不足分を並列で起動
        Self::ensure_consumers(
            &subscription_id,
            &task_name,
            &start_context,
            &consumer_event_sender,
            &shutdown_token,
            &task_handle,
            &mut consumer_registry,
            target_instances as usize,
        ).await;

        loop {

            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    log::info!(
                        "Subscription {} received shutdown signal",
                        subscription_id
                    );
                    return Self::shutdown_consumers(consumer_registry.drain()).await;
                }
                consumer_event = consumer_event_receiver.recv() => {
                    match consumer_event {
                        Some(ConsumerEvent::ConsumerExit { consumer_id }) => {
                            if let Some(_) = consumer_registry.remove_consumer(&consumer_id) {
                                log::info!(
                                    "Subscription {} detected exit of consumer {}, awaiting completion",
                                    subscription_id,
                                    consumer_id
                                );
                                // 即座に不足チェックして並列で再起動
                                Self::ensure_consumers(
                                    &subscription_id,
                                    &task_name,
                                    &start_context,
                                    &consumer_event_sender,
                                    &shutdown_token,
                                    &task_handle,
                                    &mut consumer_registry,
                                    target_instances as usize,
                                ).await;
                            } else {
                                log::warn!(
                                    "Subscription {} received exit notice for unknown consumer {}",
                                    subscription_id,
                                    consumer_id
                                );
                            }
                        }
                        Some(ConsumerEvent::ConsumerTopic {
                            consumer_id,
                            message_id,
                            topic,
                            data,
                        }) => {
                            log::info!(
                                "Subscription {} received Topic message from consumer {} on topic '{}': '{}'",
                                subscription_id,
                                consumer_id,
                                topic,
                                data
                            );

                            let to_subscription_id = if topic.is_result() {
                                if let Some(original_topic) = topic.original() {
                                    request_topic_to_from_subscription.get(&original_topic).cloned()
                                } else {
                                    None
                                }
                            } else {
                                None
                            };

                            let executor_event = ExecutorOutputEvent::Topic {
                                message_id,
                                consumer_id: consumer_id.clone(),
                                from_subscription_id: subscription_id.clone(),
                                to_subscription_id,
                                topic: topic.clone(),
                                data,
                            };

                            match topic_manager.store_message(executor_event.clone()).await {
                                Ok(_) => {
                                    log::info!(
                                        "Subscription {} stored message from consumer {} on topic '{}'",
                                        subscription_id,
                                        consumer_id,
                                        topic
                                    );
                                }
                                Err(e) => {
                                    log::error!(
                                        "Subscription {} failed to store message from consumer {} on topic '{}': {}",
                                        subscription_id,
                                        consumer_id,
                                        topic,
                                        e
                                    );
                                }
                            }

                            if topic.is_result() {
                                if let Some(response_consumer_id) = if let Some(consumer) = consumer_registry.get_consumer(&consumer_id) {
                                    match &consumer.state {
                                        crate::consumer::ConsumerState::Processing { from_consumer_id } => {
                                            from_consumer_id.clone()
                                        }
                                        _ => None,
                                    }
                                } else {
                                    None
                                } {
                                    match topic_manager.store_response(response_consumer_id.clone(), executor_event.clone()).await {
                                        Ok(_) => {
                                            log::info!(
                                                "Subscription {} stored response for consumer {} on topic '{}'",
                                                subscription_id,
                                                response_consumer_id,
                                                topic
                                            );
                                        }
                                        Err(e) => {
                                            log::error!(
                                                "Subscription {} failed to store response for consumer {} on topic '{}': {}",
                                                subscription_id,
                                                response_consumer_id,
                                                topic,
                                                e
                                            );
                                        }
                                    }
                                } else {
                                    log::warn!(
                                        "Subscription {} received result topic '{}' from consumer {} but no from_consumer_id in state",
                                        subscription_id,
                                        topic,
                                        consumer_id
                                    );
                                }
                            }
                        }
                        Some(ConsumerEvent::ConsumerRequesting { consumer_id, topic: requested_topic }) => {
                            log::debug!("worker: pull_message event branch reached: cid={:?}, topic={:?}", consumer_id, requested_topic);
                            // 状態をRequestingに設定
                            consumer_registry.set_consumer_requesting(&consumer_id, requested_topic.clone());
                            
                            let (data, from_consumer_id) = if let Some(topic_event) = topic_manager.pull_message(
                                subscription_id.clone(),
                                requested_topic.clone(),
                            ).await {
                                let from_consumer_id = match &topic_event {
                                    ExecutorOutputEvent::Topic { consumer_id, .. } => Some(consumer_id.clone()),
                                    _ => None,
                                };
                                (topic_event.data().map(|s| s.into()), from_consumer_id)
                            } else {
                                (None, None)
                            };
                            
                            if let Some(consumer) = consumer_registry.get_consumer_mut(&consumer_id) {
                                let subscription_message = SubscriptionTopicMessage {
                                    topic: requested_topic,
                                    data,
                                    from_subscription_id: subscription_id.clone(),
                                };
                                
                                if let Err(e) = consumer.topic_sender.send(subscription_message) {
                                    log::warn!(
                                        "Subscription {} failed to send pulled data to consumer {}: {}",
                                        subscription_id,
                                        consumer_id,
                                        e
                                    );
                                } else {
                                    // 状態を更新（idle_countの更新も含む）
                                    // 注: get_mutはO(1)なので、重複呼び出しのオーバーヘッドは小さい
                                    consumer_registry.set_consumer_processing_with_from_consumer_id(
                                        &consumer_id,
                                        from_consumer_id,
                                    );
                                }
                            } else {
                                log::warn!(
                                    "Subscription {} consumer {} not found when sending pull response",
                                    subscription_id,
                                    consumer_id
                                );
                            }
                        }
                        Some(ConsumerEvent::ConsumerResultRequesting { consumer_id, topic: requested_topic }) => {
                            log::debug!("worker: pull_response event branch reached: cid={:?}, topic={:?}", consumer_id, requested_topic);
                            // 状態をRequestingに設定
                            consumer_registry.set_consumer_requesting(&consumer_id, requested_topic.clone());
                            
                            let (data, from_consumer_id) = if let Some(topic_event) = topic_manager.pull_response(
                                consumer_id.clone(),
                                requested_topic.clone(),
                            ).await {
                                let from_consumer_id = match &topic_event {
                                    ExecutorOutputEvent::Topic { consumer_id, .. } => Some(consumer_id.clone()),
                                    _ => None,
                                };
                                (topic_event.data().map(|s| s.into()), from_consumer_id)
                            } else {
                                (None, None)
                            };
                            
                            if let Some(consumer) = consumer_registry.get_consumer_mut(&consumer_id) {
                                let subscription_message = SubscriptionTopicMessage {
                                    topic: requested_topic,
                                    data,
                                    from_subscription_id: subscription_id.clone(),
                                };
                                
                                if let Err(e) = consumer.topic_sender.send(subscription_message) {
                                    log::warn!(
                                        "Subscription {} failed to send pulled result to consumer {}: {}",
                                        subscription_id,
                                        consumer_id,
                                        e
                                    );
                                } else {
                                    // 状態を更新（idle_countの更新も含む）
                                    // 注: get_mutはO(1)なので、重複呼び出しのオーバーヘッドは小さい
                                    consumer_registry.set_consumer_processing_with_from_consumer_id(
                                        &consumer_id,
                                        from_consumer_id,
                                    );
                                }
                            } else {
                                log::warn!(
                                    "Subscription {} consumer {} not found when sending result response",
                                    subscription_id,
                                    consumer_id
                                );
                            }
                        }
                        Some(ConsumerEvent::ConsumerPeekRequesting { consumer_id, topic: requested_topic }) => {
                            log::debug!("worker: peek_message event branch reached: cid={:?}, topic={:?}", consumer_id, requested_topic);
                            // 状態は変更しない（peekは読み取りのみ）
                            
                            let data = if let Some(topic_event) = topic_manager.peek_message(
                                subscription_id.clone(),
                                requested_topic.clone(),
                            ).await {
                                topic_event.data().map(|s| s.into())
                            } else {
                                None
                            };
                            
                            if let Some(consumer) = consumer_registry.get_consumer_mut(&consumer_id) {
                                let subscription_message = SubscriptionTopicMessage {
                                    topic: requested_topic,
                                    data,
                                    from_subscription_id: subscription_id.clone(),
                                };
                                
                                if let Err(e) = consumer.topic_sender.send(subscription_message) {
                                    log::warn!(
                                        "Subscription {} failed to send peeked data to consumer {}: {}",
                                        subscription_id,
                                        consumer_id,
                                        e
                                    );
                                }
                                // peekは状態を変更しない（カーソルを進めない）
                            } else {
                                log::warn!(
                                    "Subscription {} consumer {} not found when sending peek response",
                                    subscription_id,
                                    consumer_id
                                );
                            }
                        }
                        Some(ConsumerEvent::ConsumerLatestRequesting { consumer_id, topic: requested_topic }) => {
                            log::debug!("worker: latest_message event branch reached: cid={:?}, topic={:?}", consumer_id, requested_topic);
                            // 状態は変更しない（latestは読み取りのみ）
                            
                            let data = if let Some(topic_event) = topic_manager.latest_message(
                                requested_topic.clone(),
                            ).await {
                                topic_event.data().map(|s| s.into())
                            } else {
                                None
                            };
                            
                            if let Some(consumer) = consumer_registry.get_consumer_mut(&consumer_id) {
                                let subscription_message = SubscriptionTopicMessage {
                                    topic: requested_topic,
                                    data,
                                    from_subscription_id: subscription_id.clone(),
                                };
                                
                                if let Err(e) = consumer.topic_sender.send(subscription_message) {
                                    log::warn!(
                                        "Subscription {} failed to send latest data to consumer {}: {}",
                                        subscription_id,
                                        consumer_id,
                                        e
                                    );
                                }
                                // latestは状態を変更しない
                            } else {
                                log::warn!(
                                    "Subscription {} consumer {} not found when sending latest response",
                                    subscription_id,
                                    consumer_id
                                );
                            }
                        }
                        None => {
                            log::warn!(
                                "Subscription {} consumer event receiver closed unexpectedly",
                                subscription_id
                            );
                            return Self::shutdown_consumers(consumer_registry.drain()).await;
                        }
                    }
                }
            }
        }
    }

    async fn shutdown_consumers(mut consumers: HashMap<ConsumerId, ManagedConsumer>) {
        for (_id, consumer) in consumers.drain() {
            consumer.handler.shutdown_sender.shutdown().ok();
        }
    }

    /// 不足しているconsumerを並列で起動する
    async fn ensure_consumers(
        subscription_id: &SubscriptionId,
        task_name: &Arc<str>,
        start_context: &ConsumerStartContext,
        consumer_event_sender: &ConsumerEventSender,
        shutdown_token: &CancellationToken,
        task_handle: &TaskHandle,
        consumer_registry: &mut ConsumerRegistry,
        target_instances: usize,
    ) {
        let current_count = consumer_registry.len();
        if current_count >= target_instances {
            return;
        }

        let needed = target_instances - current_count;
        log::info!(
            "Subscription {} ensuring consumers: current={}, target={}, needed={}",
            subscription_id,
            current_count,
            target_instances,
            needed
        );

        // 不足分を並列で起動
        let spawn_futures: Vec<_> = (0..needed)
            .map(|_| {
                Self::spawn_consumer(
                    subscription_id.clone(),
                    task_name.as_ref(),
                    start_context,
                    consumer_event_sender.clone(),
                    shutdown_token.clone(),
                    task_handle,
                )
            })
            .collect();

        let results = future::join_all(spawn_futures).await;

        // 成功したconsumerを登録
        for result in results {
            if shutdown_token.is_cancelled() {
                log::info!(
                    "Subscription {} received shutdown signal during parallel consumer spawn",
                    subscription_id
                );
                break;
            }

            match result {
                Ok(handle) => {
                    let handle_id = consumer_registry.add_consumer(handle);
                    log::info!("Subscription {} spawned consumer {}", subscription_id, handle_id);
                }
                Err(err) => {
                    log::error!(
                        "Subscription {} failed to spawn consumer for task '{}': {}",
                        subscription_id,
                        task_name.as_ref(),
                        err
                    );
                    // エラー時は1秒待機してから再試行（次のイベントループで再チェックされる）
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn spawn_consumer(
        subscription_id: SubscriptionId,
        task_name: &str,
        start_context: &ConsumerStartContext,
        consumer_event_sender: ConsumerEventSender,
        shutdown_token: CancellationToken,
        task_handle: &TaskHandle,
    ) -> Result<ManagedConsumer, String> {
        let consumer_id = ConsumerId::new();
        let instance_name = Arc::from(format!("{}-{}", task_name, short_consumer_suffix(&consumer_id)));
        let topic_channel = SubscriptionTopicChannel::new();
        
        // 各consumerに個別のトピック通知channelを作成し、registryに登録
        let TopicNotificationChannel {
            sender: topic_notification_sender,
            receiver: topic_notification_receiver,
        } = TopicNotificationChannel::new();
        start_context.topic_manager.register_topic_notification_sender(topic_notification_sender).await;

        let spawner = ConsumerSpawner::new(
            consumer_id.clone(),
            subscription_id.clone(),
            instance_name,
            start_context.userlog_sender.clone(),
            consumer_event_sender,
            start_context.view_stdout,
            start_context.view_stderr,
        );

        let handler = spawner
            .spawn(
                start_context.protocol_backend.clone(),
                shutdown_token,
                topic_channel.receiver,
                task_handle,
                topic_notification_receiver,
            )
            .await?;

        Ok(ManagedConsumer {
            handler,
            topic_sender: topic_channel.sender,
            state: crate::consumer::ConsumerState::Processing { from_consumer_id: None },
        })
    }

}

fn short_consumer_suffix(consumer_id: &ConsumerId) -> String {
    let id_string = consumer_id.to_string();
    let len = id_string.len();
    id_string[len.saturating_sub(8)..].to_string()
}

