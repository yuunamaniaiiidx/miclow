use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use crate::topic::Topic;

use crate::channels::{
    ConsumerEventChannel, ConsumerEventSender,
    SubscriptionTopicChannel, SubscriptionTopicMessage,
};
use crate::messages::{ExecutorOutputEvent, ConsumerEvent};
use crate::consumer::{ConsumerId, ConsumerSpawner};
use crate::subscription::context::ConsumerStartContext;
use crate::subscription::consumer_registry::{ManagedConsumer, ConsumerRegistry};
use crate::subscription::SubscriptionId;
use tokio_util::sync::CancellationToken;

pub struct SubscriptionWorker {
    subscription_id: SubscriptionId,
    task_name: Arc<str>,
    desired_instances: u32,
    start_context: ConsumerStartContext,
    shutdown_token: CancellationToken,
}

impl SubscriptionWorker {
    pub fn new(
        subscription_id: SubscriptionId,
        task_name: Arc<str>,
        desired_instances: u32,
        start_context: ConsumerStartContext,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            subscription_id,
            task_name,
            desired_instances,
            start_context,
            shutdown_token,
        }
    }

    pub async fn run(self) {
        let SubscriptionWorker {
            subscription_id,
            task_name,
            desired_instances,
            start_context,
            shutdown_token,
        } = self;

        // リクエストトピックとfrom_subscription_idのマッピング（レスポンス時に使用）
        let request_topic_to_from_subscription: HashMap<Topic, SubscriptionId> = HashMap::new();

        let topic_manager = start_context.topic_manager.clone();

        let ConsumerEventChannel {
            sender: consumer_event_sender,
            receiver: mut consumer_event_receiver,
        } = ConsumerEventChannel::new();

        let mut consumer_registry = ConsumerRegistry::new();
        let target_instances = desired_instances;

        loop {
            while consumer_registry.len() < target_instances as usize {
                if shutdown_token.is_cancelled() {
                    log::info!(
                        "Subscription {} received shutdown signal before spawning all consumers",
                        subscription_id
                    );
                    return Self::shutdown_consumers(consumer_registry.drain()).await;
                }

                match Self::spawn_consumer(
                    subscription_id.clone(),
                    task_name.as_ref(),
                    &start_context,
                    consumer_event_sender.clone(),
                    shutdown_token.clone(),
                )
                .await
                {
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

                        tokio::select! {
                            _ = shutdown_token.cancelled() => {
                                log::info!(
                                    "Subscription {} shutting down while retrying consumer spawn",
                                    subscription_id
                                );
                                return Self::shutdown_consumers(consumer_registry.drain()).await;
                            }
                            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                        }
                    }
                }
            }

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
                            if let Some(handle) = consumer_registry.remove_consumer(&consumer_id) {
                                log::info!(
                                    "Subscription {} detected exit of consumer {}, awaiting completion",
                                    subscription_id,
                                    consumer_id
                                );
                                handle.handler.worker_handle.await.ok();
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
                            // トピックメッセージを通常通り処理
                            log::info!(
                                "Subscription {} received Topic message from consumer {} on topic '{}': '{}'",
                                subscription_id,
                                consumer_id,
                                topic,
                                data
                            );

                            // レスポンストピックの場合、to_subscription_idを設定
                            let to_subscription_id = if topic.is_result() {
                                // レスポンストピックの場合、元のリクエストのfrom_subscription_idをto_subscription_idとして設定
                                if let Some(original_topic) = topic.original() {
                                    request_topic_to_from_subscription.get(&original_topic).cloned()
                                } else {
                                    None
                                }
                            } else {
                                None
                            };

                            // ExecutorOutputEventに変換してbroadcast
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

                            // レスポンストピックの場合、consumer_idごとにレスポンスを保存
                            if topic.is_result() {
                                // ConsumerStateからfrom_consumer_idを取得
                                let response_consumer_id = if let Some(consumer) = consumer_registry.get_consumer(&consumer_id) {
                                    match &consumer.state {
                                        crate::consumer::ConsumerState::Processing { from_consumer_id } => {
                                            from_consumer_id.clone().unwrap_or(consumer_id.clone())
                                        }
                                        _ => consumer_id.clone(),
                                    }
                                } else {
                                    consumer_id.clone()
                                };
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
                            }
                        }
                        Some(ConsumerEvent::ConsumerRequesting { consumer_id, topic: requested_topic }) => {
                            // Pull処理を実行
                            log::info!(
                                "Subscription {} processing Pull request from consumer {} for topic '{}'",
                                subscription_id,
                                consumer_id,
                                requested_topic
                            );
                            
                            // 要求されたトピックから1件取得
                            if let Some(topic_event) = topic_manager.pull_message(
                                subscription_id.clone(),
                                requested_topic.clone(),
                            ).await {
                                // データをConsumerに送信
                                if let Some(consumer) = consumer_registry.get_consumer_mut(&consumer_id) {
                                    let subscription_message = SubscriptionTopicMessage {
                                        topic: requested_topic.clone(),
                                        data: topic_event.data().map(|s| s.into()),
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
                                        // Topicを送信したときに、ExecutorOutputEvent::Topicのconsumer_idを取得
                                        // これが元の呼び出し元のconsumer_id
                                        let from_consumer_id = match &topic_event {
                                            ExecutorOutputEvent::Topic { consumer_id, .. } => Some(consumer_id.clone()),
                                            _ => None,
                                        };
                                        
                                        // Processing状態に遷移し、from_consumer_idを設定
                                        consumer_registry.set_consumer_processing_with_from_consumer_id(
                                            &consumer_id,
                                            from_consumer_id,
                                        );
                                        log::info!(
                                            "Subscription {} sent pulled data from topic '{}' to consumer {}",
                                            subscription_id,
                                            requested_topic,
                                            consumer_id
                                        );
                                    }
                                }
                            } else {
                                // データがない場合、Noneを送信
                                if let Some(consumer) = consumer_registry.get_consumer_mut(&consumer_id) {
                                    let subscription_message = SubscriptionTopicMessage {
                                        topic: requested_topic.clone(),
                                        data: None, // データなしを表現
                                        from_subscription_id: subscription_id.clone(),
                                    };
                                    
                                    if let Err(e) = consumer.topic_sender.send(subscription_message) {
                                        log::warn!(
                                            "Subscription {} failed to send 'no data' response to consumer {}: {}",
                                            subscription_id,
                                            consumer_id,
                                            e
                                        );
                                    } else {
                                        // Processing状態に遷移（データなしでも処理完了として扱う）
                                        consumer_registry.set_consumer_processing_with_from_consumer_id(
                                            &consumer_id,
                                            None,
                                        );
                                        log::debug!(
                                            "Subscription {} sent 'no data' response (line count 0) for topic '{}' to consumer {}",
                                            subscription_id,
                                            requested_topic,
                                            consumer_id
                                        );
                                    }
                                } else {
                                    log::warn!(
                                        "Subscription {} consumer {} not found when sending 'no data' response",
                                        subscription_id,
                                        consumer_id
                                    );
                                }
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
        for (_id, handle) in consumers.drain() {
            handle.handler.shutdown_sender.shutdown().ok();
            handle.handler.worker_handle.await.ok();
        }
    }

    async fn spawn_consumer(
        subscription_id: SubscriptionId,
        task_name: &str,
        start_context: &ConsumerStartContext,
        consumer_event_sender: ConsumerEventSender,
        shutdown_token: CancellationToken,
    ) -> Result<ManagedConsumer, String> {
        let consumer_id = ConsumerId::new();
        let instance_name = Arc::from(format!("{}-{}", task_name, short_consumer_suffix(&consumer_id)));
        let topic_channel = SubscriptionTopicChannel::new();

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

