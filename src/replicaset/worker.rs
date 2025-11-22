use std::collections::{HashMap, VecDeque};
use std::time::Duration;
use crate::topic::Topic;

use crate::channels::{
    ExecutorOutputEventChannel, ExecutorOutputEventSender, PodEventChannel, PodEventSender,
    ReplicaSetTopicChannel, ReplicaSetTopicMessage,
};
use crate::messages::{ExecutorOutputEvent, PodEvent};
use crate::pod::{PodId, PodSpawner};
use crate::replicaset::context::PodStartContext;
use crate::replicaset::pod_registry::{ManagedPod, PodRegistry};
use crate::replicaset::route_status::RouteStatus;
use crate::replicaset::ReplicaSetId;
use crate::topic::TopicSubscriptionRegistry;
use tokio_util::sync::CancellationToken;

pub struct ReplicaSetWorker {
    replicaset_id: ReplicaSetId,
    task_name: String,
    desired_instances: u32,
    start_context: PodStartContext,
    shutdown_token: CancellationToken,
}

impl ReplicaSetWorker {
    pub fn new(
        replicaset_id: ReplicaSetId,
        task_name: String,
        desired_instances: u32,
        start_context: PodStartContext,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            replicaset_id,
            task_name,
            desired_instances,
            start_context,
            shutdown_token,
        }
    }

    pub async fn run(self) {
        let ReplicaSetWorker {
            replicaset_id,
            task_name,
            desired_instances,
            start_context,
            shutdown_token,
        } = self;

        // リクエストトピックとfrom_replicaset_idのマッピング（レスポンス時に使用）
        let mut request_topic_to_from_replicaset: HashMap<Topic, ReplicaSetId> = HashMap::new();

        let subscribe_topics = start_context.subscribe_topics.clone();
        let private_response_topics: Vec<Topic> = start_context.private_response_topics.iter().map(|s| Topic::from(s.as_str())).collect();
        let topic_manager = start_context.topic_manager.clone();

        let ExecutorOutputEventChannel {
            sender: replicaset_subscription_sender,
            receiver: mut topic_receiver,
        } = ExecutorOutputEventChannel::new();

        Self::register_topic_subscriptions(
            &replicaset_id,
            &subscribe_topics,
            &topic_manager,
            replicaset_subscription_sender.clone(),
        )
        .await;

        // private_response_topicsもtopic registryに登録
        let private_response_topics_strings: Vec<String> = start_context.private_response_topics.clone();
        Self::register_topic_subscriptions(
            &replicaset_id,
            &private_response_topics_strings,
            &topic_manager,
            replicaset_subscription_sender.clone(),
        )
        .await;

        let PodEventChannel {
            sender: pod_event_sender,
            receiver: mut pod_event_receiver,
        } = PodEventChannel::new();

        let mut pod_registry = PodRegistry::new();
        let mut pending_events: VecDeque<ExecutorOutputEvent> = VecDeque::new();
        let target_instances = desired_instances;

        loop {
            while pod_registry.len() < target_instances as usize {
                if shutdown_token.is_cancelled() {
                    log::info!(
                        "ReplicaSet {} received shutdown signal before spawning all pods",
                        replicaset_id
                    );
                    return Self::shutdown_pods(pod_registry.drain()).await;
                }

                match Self::spawn_pod(
                    replicaset_id.clone(),
                    &task_name,
                    &start_context,
                    pod_event_sender.clone(),
                    shutdown_token.clone(),
                )
                .await
                {
                    Ok(handle) => {
                        let handle_id = pod_registry.add_pod(handle);
                        log::info!("ReplicaSet {} spawned pod {}", replicaset_id, handle_id);
                        Self::drain_pending_events(
                            &replicaset_id,
                            &mut pod_registry,
                            &mut request_topic_to_from_replicaset,
                            &mut pending_events,
                        ).await;
                    }
                    Err(err) => {
                        log::error!(
                            "ReplicaSet {} failed to spawn pod for task '{}': {}",
                            replicaset_id,
                            task_name,
                            err
                        );

                        tokio::select! {
                            _ = shutdown_token.cancelled() => {
                                log::info!(
                                    "ReplicaSet {} shutting down while retrying pod spawn",
                                    replicaset_id
                                );
                                return Self::shutdown_pods(pod_registry.drain()).await;
                            }
                            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                        }
                    }
                }
            }

            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    log::info!(
                        "ReplicaSet {} received shutdown signal",
                        replicaset_id
                    );
                    return Self::shutdown_pods(pod_registry.drain()).await;
                }
                topic_event = topic_receiver.recv() => {
                    match topic_event {
                        Some(event) => {
                            // 送信元が自分自身の場合はスキップ
                            if let Some(from_id) = event.from_replicaset_id() {
                                if from_id == &replicaset_id {
                                    log::debug!(
                                        "ReplicaSet {} filtered message from itself",
                                        replicaset_id
                                    );
                                    continue;
                                }
                            }

                            // private_response_topicsに含まれるトピックの場合、to_replicaset_idをチェック
                            let should_accept = if let Some(topic) = event.topic() {
                                if private_response_topics.contains(topic) {
                                    // private_response_topicsに含まれるトピックの場合、to_replicaset_idが自分のIDと一致するかチェック
                                    if let Some(to_id) = event.to_replicaset_id() {
                                        to_id == &replicaset_id
                                    } else {
                                        // to_replicaset_idが設定されていない場合は拒否
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
                                    "ReplicaSet {} filtered message on private_response_topic (to_replicaset_id mismatch)",
                                    replicaset_id
                                );
                                continue;
                            }

                            if pod_registry.idle_count() > 0 {
                                match Self::route_topic_event(
                                    &replicaset_id,
                                    &mut pod_registry,
                                    &mut request_topic_to_from_replicaset,
                                    event,
                                ).await {
                                    RouteStatus::Pending(event) => pending_events.push_back(event),
                                    RouteStatus::Delivered | RouteStatus::Dropped => {}
                                }
                            } else {
                                pending_events.push_back(event);
                            }
                        }
                        None => {
                            log::warn!(
                                "ReplicaSet {} topic subscription receiver closed",
                                replicaset_id
                            );
                        }
                    }
                }
                pod_event = pod_event_receiver.recv() => {
                    match pod_event {
                        Some(PodEvent::PodExit { pod_id }) => {
                            if let Some(handle) = pod_registry.remove_pod(&pod_id) {
                                log::info!(
                                    "ReplicaSet {} detected exit of pod {}, awaiting completion",
                                    replicaset_id,
                                    pod_id
                                );
                                handle.handler.worker_handle.await.ok();
                            } else {
                                log::warn!(
                                    "ReplicaSet {} received exit notice for unknown pod {}",
                                    replicaset_id,
                                    pod_id
                                );
                            }
                        }
                        Some(PodEvent::PodTopic {
                            pod_id,
                            message_id,
                            topic,
                            data,
                        }) => {
                            // すべてのトピックメッセージを同じように処理
                            log::info!(
                                "ReplicaSet {} received Topic message from pod {} on topic '{}': '{}'",
                                replicaset_id,
                                pod_id,
                                topic,
                                data
                            );

                            // レスポンストピックの場合、to_replicaset_idを設定
                            let to_replicaset_id = if topic.is_result() {
                                // レスポンストピックの場合、元のリクエストのfrom_replicaset_idをto_replicaset_idとして設定
                                if let Some(original_topic) = topic.original() {
                                    request_topic_to_from_replicaset.get(&original_topic).cloned()
                                } else {
                                    None
                                }
                            } else {
                                None
                            };

                            // ExecutorOutputEventに変換してbroadcast
                            let executor_event = ExecutorOutputEvent::Topic {
                                message_id,
                                pod_id: pod_id.clone(),
                                from_replicaset_id: replicaset_id.clone(),
                                to_replicaset_id,
                                topic: topic.clone(),
                                data,
                            };

                            match topic_manager.broadcast_message(executor_event.clone()).await {
                                Ok(_) => {
                                    log::info!(
                                        "ReplicaSet {} broadcasted message from pod {} on topic '{}'",
                                        replicaset_id,
                                        pod_id,
                                        topic
                                    );
                                }
                                Err(e) => {
                                    log::error!(
                                        "ReplicaSet {} failed to broadcast message from pod {} on topic '{}': {}",
                                        replicaset_id,
                                        pod_id,
                                        topic,
                                        e
                                    );
                                }
                            }
                        }
                        Some(PodEvent::PodIdle { pod_id }) => {
                            // 状態管理：BusyからIdleに戻す
                            log::info!(
                                "ReplicaSet {} received PodIdle from pod {}",
                                replicaset_id,
                                pod_id
                            );

                            pod_registry.set_pod_idle(&pod_id);
                            Self::drain_pending_events(
                                &replicaset_id,
                                &mut pod_registry,
                                &mut request_topic_to_from_replicaset,
                                &mut pending_events,
                            ).await;
                        }
                        None => {
                            log::warn!(
                                "ReplicaSet {} pod event receiver closed unexpectedly",
                                replicaset_id
                            );
                            return Self::shutdown_pods(pod_registry.drain()).await;
                        }
                    }
                }
            }
        }
    }

    async fn shutdown_pods(mut pods: HashMap<PodId, ManagedPod>) {
        for (_id, handle) in pods.drain() {
            handle.handler.shutdown_sender.shutdown().ok();
            handle.handler.worker_handle.await.ok();
        }
    }

    async fn spawn_pod(
        replicaset_id: ReplicaSetId,
        task_name: &str,
        start_context: &PodStartContext,
        pod_event_sender: PodEventSender,
        shutdown_token: CancellationToken,
    ) -> Result<ManagedPod, String> {
        let pod_id = PodId::new();
        let instance_name = format!("{}-{}", task_name, short_pod_suffix(&pod_id));
        let topic_channel = ReplicaSetTopicChannel::new();

        let spawner = PodSpawner::new(
            pod_id.clone(),
            replicaset_id.clone(),
            instance_name.clone(),
            start_context.userlog_sender.clone(),
            pod_event_sender,
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

        Ok(ManagedPod {
            handler,
            topic_sender: topic_channel.sender,
            state: crate::pod::PodState::Idle,
        })
    }

    async fn register_topic_subscriptions(
        replicaset_id: &ReplicaSetId,
        topics: &[String],
        topic_manager: &TopicSubscriptionRegistry,
        sender: ExecutorOutputEventSender,
    ) {
        if topics.is_empty() {
            log::info!(
                "ReplicaSet {} has no topic subscriptions configured",
                replicaset_id
            );
            return;
        }

        for topic in topics {
            topic_manager
                .add_subscriber(topic.clone(), sender.clone())
                .await;
            log::info!(
                "ReplicaSet {} subscribed to topic '{}'",
                replicaset_id,
                topic
            );
        }
    }

    async fn drain_pending_events(
        replicaset_id: &ReplicaSetId,
        pod_registry: &mut PodRegistry,
        request_topic_to_from_replicaset: &mut HashMap<Topic, ReplicaSetId>,
        pending_events: &mut VecDeque<ExecutorOutputEvent>,
    ) {
        while pod_registry.idle_count() > 0 {
            let Some(event) = pending_events.pop_front() else {
                break;
            };

            match Self::route_topic_event(replicaset_id, pod_registry, request_topic_to_from_replicaset, event).await {
                RouteStatus::Delivered | RouteStatus::Dropped => continue,
                RouteStatus::Pending(event) => {
                    pending_events.push_front(event);
                    break;
                }
            }
        }
    }

    async fn route_topic_event(
        replicaset_id: &ReplicaSetId,
        pod_registry: &mut PodRegistry,
        request_topic_to_from_replicaset: &mut HashMap<Topic, ReplicaSetId>,
        event: ExecutorOutputEvent,
    ) -> RouteStatus {
        let Some(message) = Self::convert_to_replica_message(replicaset_id, &event) else {
            log::warn!(
                "ReplicaSet {} received topic event without topic/data, dropping",
                replicaset_id
            );
            return RouteStatus::Dropped;
        };
        // レスポンストピックでない場合（通常のトピック）はレスポンスを要求
        let requires_response = !message.topic.is_result();

        // リクエスト送信時（レスポンスを要求する場合）にマッピングを保存
        if requires_response {
            request_topic_to_from_replicaset.insert(message.topic.clone(), message.from_replicaset_id.clone());
        }

        if pod_registry.idle_count() == 0 || pod_registry.is_empty() {
            log::debug!(
                "ReplicaSet {} has no pods available; queuing topic '{}'",
                replicaset_id,
                message.topic
            );
            return RouteStatus::Pending(event);
        }

        let topic_name = message.topic.clone();
        let max_attempts = pod_registry.len();

        for _ in 0..max_attempts {
            let Some(target_pod_id) = pod_registry.next_pod_id() else {
                break;
            };

            if let Some(pod) = pod_registry.get_pod_mut(&target_pod_id) {
                if !matches!(pod.state, crate::pod::PodState::Idle) {
                    continue;
                }

                if let Err(e) = pod.topic_sender.send(message.clone()) {
                    log::warn!(
                        "ReplicaSet {} failed to send topic '{}' to pod {}: {}",
                        replicaset_id,
                        topic_name,
                        target_pod_id,
                        e
                    );
                    return RouteStatus::Dropped;
                } else {
                    if requires_response {
                        pod_registry.set_pod_busy(&target_pod_id);
                    }
                    log::debug!(
                        "ReplicaSet {} routed topic '{}' to pod {}",
                        replicaset_id,
                        topic_name,
                        target_pod_id
                    );
                    return RouteStatus::Delivered;
                }
            } else {
                pod_registry.remove_pod(&target_pod_id);
            }
        }

        log::info!(
            "ReplicaSet {} could not route topic '{}' because no idle pods were reachable",
            replicaset_id,
            topic_name
        );
        RouteStatus::Pending(event)
    }

    fn convert_to_replica_message(
        _replicaset_id: &ReplicaSetId,
        event: &ExecutorOutputEvent,
    ) -> Option<ReplicaSetTopicMessage> {
        match event {
            ExecutorOutputEvent::Topic { topic, data, from_replicaset_id, .. } => {
                // リクエスト送信時は、このReplicaSetのIDをfrom_replicaset_idとして設定
                // ただし、既にfrom_replicaset_idが設定されている場合はそれを使用（他のReplicaSetからの転送の場合）
                let from_id = from_replicaset_id.clone();
                Some(ReplicaSetTopicMessage {
                    topic: topic.clone(),
                    data: data.clone(),
                    from_replicaset_id: from_id,
                })
            }
            _ => None,
        }
    }
}

fn short_pod_suffix(pod_id: &PodId) -> String {
    let id_string = pod_id.to_string();
    let len = id_string.len();
    id_string[len.saturating_sub(8)..].to_string()
}

