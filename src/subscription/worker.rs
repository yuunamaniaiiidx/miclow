use std::collections::{HashMap, VecDeque};
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
use crate::subscription::route_status::RouteStatus;
use crate::subscription::SubscriptionId;
use crate::topic::TopicSubscriptionRegistry;
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
        let mut request_topic_to_from_subscription: HashMap<Topic, SubscriptionId> = HashMap::new();

        let subscribe_topics = start_context.subscribe_topics.clone();
        let private_response_topics: Vec<Topic> = start_context.private_response_topics.iter().map(|s| Topic::from(s.as_ref())).collect();
        let topic_manager = start_context.topic_manager.clone();

        // トピック購読を登録（Sender不要）
        Self::register_topic_subscriptions(
            &subscription_id,
            &subscribe_topics,
            &topic_manager,
        )
        .await;

        // private_response_topicsもtopic registryに登録
        let private_response_topics_strings: Vec<Arc<str>> = start_context.private_response_topics.clone();
        Self::register_topic_subscriptions(
            &subscription_id,
            &private_response_topics_strings,
            &topic_manager,
        )
        .await;

        let ConsumerEventChannel {
            sender: consumer_event_sender,
            receiver: mut consumer_event_receiver,
        } = ConsumerEventChannel::new();

        let mut consumer_registry = ConsumerRegistry::new();
        let mut pending_events: VecDeque<ExecutorOutputEvent> = VecDeque::new();
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
                        Self::drain_pending_events(
                            &subscription_id,
                            &mut consumer_registry,
                            &mut request_topic_to_from_subscription,
                            &mut pending_events,
                        ).await;
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
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    // Pull型ポーリング: 購読しているトピックからメッセージを取得
                    for topic_str in &subscribe_topics {
                        let topic = Topic::from(topic_str.as_ref());
                        let messages = topic_manager.pull_messages(
                            subscription_id.clone(),
                            topic.clone(),
                            10, // バッチサイズ
                        ).await;

                        for event in messages {
                            // 送信元が自分自身の場合はスキップ
                            if let Some(from_id) = event.from_subscription_id() {
                                if from_id == &subscription_id {
                                    log::debug!(
                                        "Subscription {} filtered message from itself",
                                        subscription_id
                                    );
                                    continue;
                                }
                            }

                            // private_response_topicsに含まれるトピックの場合、to_subscription_idをチェック
                            let should_accept = if let Some(event_topic) = event.topic() {
                                if private_response_topics.contains(event_topic) {
                                    // private_response_topicsに含まれるトピックの場合、to_subscription_idが自分のIDと一致するかチェック
                                    if let Some(to_id) = event.to_subscription_id() {
                                        to_id == &subscription_id
                                    } else {
                                        // to_subscription_idが設定されていない場合は拒否
                                        false
                                    }
                                } else {
                                    // private_response_topicsに含まれないトピックは常に受け入れる
                                    true
                                }
                            } else {
                                true
                            };

                            if !should_accept {
                                log::debug!(
                                    "Subscription {} filtered message on private_response_topic (to_subscription_id mismatch)",
                                    subscription_id
                                );
                                continue;
                            }

                            if consumer_registry.idle_count() > 0 {
                                match Self::route_topic_event(
                                    &subscription_id,
                                    &mut consumer_registry,
                                    &mut request_topic_to_from_subscription,
                                    event,
                                ).await {
                                    RouteStatus::Pending(event) => pending_events.push_back(event),
                                    RouteStatus::Delivered | RouteStatus::Dropped => {}
                                }
                            } else {
                                pending_events.push_back(event);
                            }
                        }
                    }

                    // private_response_topicsもポーリング
                    for topic_str in &private_response_topics_strings {
                        let topic = Topic::from(topic_str.as_ref());
                        let messages = topic_manager.pull_messages(
                            subscription_id.clone(),
                            topic.clone(),
                            10, // バッチサイズ
                        ).await;

                        for event in messages {
                            // private_response_topicsの場合はto_subscription_idをチェック
                            let should_accept = if let Some(to_id) = event.to_subscription_id() {
                                to_id == &subscription_id
                            } else {
                                false
                            };

                            if !should_accept {
                                continue;
                            }

                            if consumer_registry.idle_count() > 0 {
                                match Self::route_topic_event(
                                    &subscription_id,
                                    &mut consumer_registry,
                                    &mut request_topic_to_from_subscription,
                                    event,
                                ).await {
                                    RouteStatus::Pending(event) => pending_events.push_back(event),
                                    RouteStatus::Delivered | RouteStatus::Dropped => {}
                                }
                            } else {
                                pending_events.push_back(event);
                            }
                        }
                    }
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
                            // すべてのトピックメッセージを同じように処理
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
                                pod_id: consumer_id.clone(),
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
                        }
                        Some(ConsumerEvent::ConsumerIdle { consumer_id }) => {
                            // 状態管理：BusyからIdleに戻す
                            log::info!(
                                "Subscription {} received ConsumerIdle from consumer {}",
                                subscription_id,
                                consumer_id
                            );

                            consumer_registry.set_consumer_idle(&consumer_id);
                            Self::drain_pending_events(
                                &subscription_id,
                                &mut consumer_registry,
                                &mut request_topic_to_from_subscription,
                                &mut pending_events,
                            ).await;
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
            state: crate::consumer::ConsumerState::Busy,
        })
    }

    async fn register_topic_subscriptions(
        subscription_id: &SubscriptionId,
        topics: &[Arc<str>],
        topic_manager: &TopicSubscriptionRegistry,
    ) {
        if topics.is_empty() {
            log::info!(
                "Subscription {} has no topic subscriptions configured",
                subscription_id
            );
            return;
        }

        for topic in topics {
            topic_manager
                .add_subscriber(topic.as_ref(), subscription_id.clone())
                .await;
            log::info!(
                "Subscription {} subscribed to topic '{}'",
                subscription_id,
                topic.as_ref()
            );
        }
    }

    async fn drain_pending_events(
        subscription_id: &SubscriptionId,
        consumer_registry: &mut ConsumerRegistry,
        request_topic_to_from_subscription: &mut HashMap<Topic, SubscriptionId>,
        pending_events: &mut VecDeque<ExecutorOutputEvent>,
    ) {
        while consumer_registry.idle_count() > 0 {
            let Some(event) = pending_events.pop_front() else {
                break;
            };

            match Self::route_topic_event(subscription_id, consumer_registry, request_topic_to_from_subscription, event).await {
                RouteStatus::Delivered | RouteStatus::Dropped => continue,
                RouteStatus::Pending(event) => {
                    pending_events.push_front(event);
                    break;
                }
            }
        }
    }

    async fn route_topic_event(
        subscription_id: &SubscriptionId,
        consumer_registry: &mut ConsumerRegistry,
        request_topic_to_from_subscription: &mut HashMap<Topic, SubscriptionId>,
        event: ExecutorOutputEvent,
    ) -> RouteStatus {
        let Some(message) = Self::convert_to_subscription_message(subscription_id, &event) else {
            log::warn!(
                "Subscription {} received topic event without topic/data, dropping",
                subscription_id
            );
            return RouteStatus::Dropped;
        };
        // レスポンストピックでない場合（通常のトピック）はレスポンスを要求
        let requires_response = !message.topic.is_result();

        // リクエスト送信時（レスポンスを要求する場合）にマッピングを保存
        if requires_response {
            request_topic_to_from_subscription.insert(message.topic.clone(), message.from_subscription_id.clone());
        }

        if consumer_registry.idle_count() == 0 || consumer_registry.is_empty() {
            log::debug!(
                "Subscription {} has no consumers available; queuing topic '{}'",
                subscription_id,
                message.topic
            );
            return RouteStatus::Pending(event);
        }

        let topic_name = message.topic.clone();
        let max_attempts = consumer_registry.len();

        for _ in 0..max_attempts {
            let Some(target_consumer_id) = consumer_registry.next_consumer_id() else {
                break;
            };

            if let Some(consumer) = consumer_registry.get_consumer_mut(&target_consumer_id) {
                if !matches!(consumer.state, crate::consumer::ConsumerState::Idle) {
                    continue;
                }

                if let Err(e) = consumer.topic_sender.send(message.clone()) {
                    log::warn!(
                        "Subscription {} failed to send topic '{}' to consumer {}: {}",
                        subscription_id,
                        topic_name,
                        target_consumer_id,
                        e
                    );
                    return RouteStatus::Dropped;
                } else {
                    // メッセージをconsumerに送信したら、.resultトピックかどうかに関わらず常にBusyに遷移
                    consumer_registry.set_consumer_busy(&target_consumer_id);
                    log::debug!(
                        "Subscription {} routed topic '{}' to consumer {}",
                        subscription_id,
                        topic_name,
                        target_consumer_id
                    );
                    return RouteStatus::Delivered;
                }
            } else {
                consumer_registry.remove_consumer(&target_consumer_id);
            }
        }

        log::info!(
            "Subscription {} could not route topic '{}' because no idle consumers were reachable",
            subscription_id,
            topic_name
        );
        RouteStatus::Pending(event)
    }

    fn convert_to_subscription_message(
        _subscription_id: &SubscriptionId,
        event: &ExecutorOutputEvent,
    ) -> Option<SubscriptionTopicMessage> {
        match event {
            ExecutorOutputEvent::Topic { topic, data, from_subscription_id, .. } => {
                // リクエスト送信時は、このSubscriptionのIDをfrom_subscription_idとして設定
                // ただし、既にfrom_subscription_idが設定されている場合はそれを使用（他のSubscriptionからの転送の場合）
                let from_id = from_subscription_id.clone();
                Some(SubscriptionTopicMessage {
                    topic: topic.clone(),
                    data: data.clone(),
                    from_subscription_id: from_id,
                })
            }
            _ => None,
        }
    }
}

fn short_consumer_suffix(consumer_id: &ConsumerId) -> String {
    let id_string = consumer_id.to_string();
    let len = id_string.len();
    id_string[len.saturating_sub(8)..].to_string()
}

