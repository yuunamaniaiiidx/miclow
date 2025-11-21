use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use crate::channels::{
    ExecutorOutputEventChannel, ExecutorOutputEventSender, PodEventChannel, PodEventSender,
    ReplicaSetTopicChannel, ReplicaSetTopicMessage, ReplicaSetTopicSender,
};
use crate::messages::{ExecutorOutputEvent, PodEvent};
use crate::pod::{PodId, PodSpawnHandler, PodSpawner, PodState};
use crate::replicaset::context::{PodStartContext, ReplicaSetSpec};
use crate::replicaset::ReplicaSetId;
use crate::topic::TopicSubscriptionRegistry;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub struct ReplicaSetController {
    shutdown_token: CancellationToken,
    replica_sets: HashMap<ReplicaSetId, ReplicaSetHandle>,
}

struct ReplicaSetHandle {
    join_handle: JoinHandle<()>,
    shutdown_token: CancellationToken,
}

impl ReplicaSetController {
    pub fn new(shutdown_token: CancellationToken) -> Self {
        Self {
            shutdown_token,
            replica_sets: HashMap::new(),
        }
    }

    pub fn create_replicaset(&mut self, spec: ReplicaSetSpec) -> ReplicaSetId {
        let desired_instances = spec.desired_instances.max(1);
        let replicaset_id = ReplicaSetId::new();
        let replica_token = self.shutdown_token.child_token();

        let worker = ReplicaSetWorker::new(
            replicaset_id.clone(),
            spec.task_name,
            desired_instances,
            spec.start_context,
            replica_token.clone(),
        );

        let join_handle = tokio::spawn(worker.run());

        self.replica_sets.insert(
            replicaset_id.clone(),
            ReplicaSetHandle {
                join_handle,
                shutdown_token: replica_token,
            },
        );

        replicaset_id
    }

    pub async fn shutdown_all(mut self) {
        for (_id, handle) in self.replica_sets.drain() {
            handle.shutdown_token.cancel();
            let _ = handle.join_handle.await;
        }
    }
}

struct ReplicaSetWorker {
    replicaset_id: ReplicaSetId,
    task_name: String,
    desired_instances: u32,
    start_context: PodStartContext,
    shutdown_token: CancellationToken,
}

impl ReplicaSetWorker {
    fn new(
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

    async fn run(self) {
        let ReplicaSetWorker {
            replicaset_id,
            task_name,
            desired_instances,
            start_context,
            shutdown_token,
        } = self;

        let subscribe_topics = start_context.subscribe_topics.clone();
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
                        let handle_id = handle.handler.pod_id.clone();
                        log::info!("ReplicaSet {} spawned pod {}", replicaset_id, handle_id);
                        pod_registry.add_pod(handle_id, handle);
                        Self::drain_pending_events(
                            &replicaset_id,
                            &mut pod_registry,
                            &mut pending_events,
                        );
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
                            if pod_registry.idle_count() > 0 {
                                match Self::route_topic_event(
                                    &replicaset_id,
                                    &mut pod_registry,
                                    event,
                                ) {
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

                            // ExecutorOutputEventに変換してbroadcast
                            let executor_event = ExecutorOutputEvent::Topic {
                                message_id,
                                pod_id: pod_id.clone(),
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
                                &mut pending_events,
                            );
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
            state: PodState::Idle,
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

    fn drain_pending_events(
        replicaset_id: &ReplicaSetId,
        pod_registry: &mut PodRegistry,
        pending_events: &mut VecDeque<ExecutorOutputEvent>,
    ) {
        while pod_registry.idle_count() > 0 {
            let Some(event) = pending_events.pop_front() else {
                break;
            };

            match Self::route_topic_event(replicaset_id, pod_registry, event) {
                RouteStatus::Delivered | RouteStatus::Dropped => continue,
                RouteStatus::Pending(event) => {
                    pending_events.push_front(event);
                    break;
                }
            }
        }
    }

    fn route_topic_event(
        replicaset_id: &ReplicaSetId,
        pod_registry: &mut PodRegistry,
        event: ExecutorOutputEvent,
    ) -> RouteStatus {
        let Some(message) = Self::convert_to_replica_message(&event) else {
            log::warn!(
                "ReplicaSet {} received topic event without topic/data, dropping",
                replicaset_id
            );
            return RouteStatus::Dropped;
        };
        // レスポンストピックでない場合（通常のトピック）はレスポンスを要求
        let requires_response = !message.topic.is_result();

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
                if !matches!(pod.state, PodState::Idle) {
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

    fn convert_to_replica_message(event: &ExecutorOutputEvent) -> Option<ReplicaSetTopicMessage> {
        match event {
            ExecutorOutputEvent::Topic { topic, data, .. } => Some(ReplicaSetTopicMessage {
                topic: topic.clone(),
                data: data.clone(),
            }),
            _ => None,
        }
    }
}

fn short_pod_suffix(pod_id: &PodId) -> String {
    let id_string = pod_id.to_string();
    let len = id_string.len();
    id_string[len.saturating_sub(8)..].to_string()
}

struct ManagedPod {
    handler: PodSpawnHandler,
    topic_sender: ReplicaSetTopicSender,
    state: PodState,
}

enum RouteStatus {
    Delivered,
    Pending(ExecutorOutputEvent),
    Dropped,
}

#[derive(Default)]
struct PodRouter {
    order: Vec<PodId>,
    next_index: usize,
}

impl PodRouter {
    fn add(&mut self, pod_id: PodId) {
        self.order.push(pod_id);
    }

    fn remove(&mut self, pod_id: &PodId) {
        if let Some(pos) = self.order.iter().position(|id| id == pod_id) {
            self.order.remove(pos);
            if self.order.is_empty() {
                self.next_index = 0;
                return;
            }
            if pos < self.next_index {
                self.next_index = self.next_index.saturating_sub(1);
            } else if self.next_index >= self.order.len() {
                self.next_index = 0;
            }
        }
    }

    fn next_pod(&mut self) -> Option<PodId> {
        if self.order.is_empty() {
            return None;
        }
        let pod_id = self.order[self.next_index].clone();
        self.next_index = (self.next_index + 1) % self.order.len();
        Some(pod_id)
    }

    fn is_empty(&self) -> bool {
        self.order.is_empty()
    }
}

struct PodRegistry {
    pods: HashMap<PodId, ManagedPod>,
    router: PodRouter,
    idle_count: usize,
}

impl PodRegistry {
    fn new() -> Self {
        Self {
            pods: HashMap::new(),
            router: PodRouter::default(),
            idle_count: 0,
        }
    }

    fn add_pod(&mut self, pod_id: PodId, pod: ManagedPod) {
        self.pods.insert(pod_id.clone(), pod);
        self.router.add(pod_id);
        self.idle_count += 1;
    }

    fn remove_pod(&mut self, pod_id: &PodId) -> Option<ManagedPod> {
        if let Some(pod) = self.pods.remove(pod_id) {
            if matches!(pod.state, PodState::Idle) {
                self.idle_count = self.idle_count.saturating_sub(1);
            }
            self.router.remove(pod_id);
            Some(pod)
        } else {
            None
        }
    }

    fn set_pod_busy(&mut self, pod_id: &PodId) {
        if let Some(pod) = self.pods.get_mut(pod_id) {
            if matches!(pod.state, PodState::Idle) {
                pod.state = PodState::Busy;
                self.idle_count = self.idle_count.saturating_sub(1);
            }
        }
    }

    fn set_pod_idle(&mut self, pod_id: &PodId) {
        if let Some(pod) = self.pods.get_mut(pod_id) {
            if !matches!(pod.state, PodState::Idle) {
                pod.state = PodState::Idle;
                self.idle_count += 1;
            }
        }
    }

    fn idle_count(&self) -> usize {
        self.idle_count
    }

    fn get_pod_mut(&mut self, pod_id: &PodId) -> Option<&mut ManagedPod> {
        self.pods.get_mut(pod_id)
    }

    fn next_pod_id(&mut self) -> Option<PodId> {
        self.router.next_pod()
    }

    fn is_empty(&self) -> bool {
        self.pods.is_empty() || self.router.is_empty()
    }

    fn len(&self) -> usize {
        self.pods.len()
    }

    fn drain(&mut self) -> HashMap<PodId, ManagedPod> {
        self.router = PodRouter::default();
        self.idle_count = 0;
        std::mem::take(&mut self.pods)
    }
}
