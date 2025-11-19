use crate::channels::ExecutorOutputEventSender;
use crate::config::{LifecycleMode, SystemConfig};
use crate::messages::ExecutorOutputEvent;
use crate::pod::PodManager;
use crate::task_id::TaskId;
use crate::topic_load_balancer::TopicLoadBalancer;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct TopicSubscriptionRegistry {
    subscribers: Arc<RwLock<HashMap<String, Arc<Vec<Arc<ExecutorOutputEventSender>>>>>>,
    task_subscriptions: Arc<RwLock<HashMap<(String, TaskId), Weak<ExecutorOutputEventSender>>>>,
    latest_messages: Arc<RwLock<HashMap<String, ExecutorOutputEvent>>>,
    /// PodManager への参照（タスクIDからタスク名を取得するため）
    pod_manager: PodManager,
    /// SystemConfig への参照（配信モードを判定するため）
    system_config: Arc<SystemConfig>,
    /// TopicLoadBalancer への参照（Round Robin配信のため）
    load_balancer: TopicLoadBalancer,
}

impl TopicSubscriptionRegistry {
    pub fn new(
        pod_manager: PodManager,
        system_config: SystemConfig,
        load_balancer: TopicLoadBalancer,
    ) -> Self {
        Self {
            subscribers: Arc::new(RwLock::new(HashMap::new())),
            task_subscriptions: Arc::new(RwLock::new(HashMap::new())),
            latest_messages: Arc::new(RwLock::new(HashMap::new())),
            pod_manager,
            system_config: Arc::new(system_config),
            load_balancer,
        }
    }

    pub async fn add_subscriber(
        &self,
        topic: String,
        task_id: TaskId,
        subscriber: ExecutorOutputEventSender,
    ) {
        let subscriber_arc = Arc::new(subscriber);
        let mut subscribers = self.subscribers.write().await;

        if let Some(existing_subscribers) = subscribers.get_mut(&topic) {
            let new_subscribers = Arc::new({
                let mut vec = existing_subscribers.as_ref().clone();
                vec.push(subscriber_arc.clone());
                vec
            });
            *existing_subscribers = new_subscribers;
            log::info!(
                "Added subscriber to existing topic '{}' (total subscribers: {})",
                topic,
                existing_subscribers.len()
            );
        } else {
            let new_subscribers = Arc::new(vec![subscriber_arc.clone()]);
            subscribers.insert(topic.clone(), new_subscribers);
            log::info!("Added new topic '{}' with subscriber", topic);
        }
        let mut task_subs = self.task_subscriptions.write().await;
        task_subs.insert(
            (topic.clone(), task_id.clone()),
            Arc::downgrade(&subscriber_arc),
        );
        log::info!(
            "Recorded task {} subscription to topic '{}'",
            task_id,
            topic
        );
    }

    pub async fn remove_failed_subscribers(&self, topic: &str, failed_indices: Vec<usize>) {
        let mut subscribers = self.subscribers.write().await;

        if let Some(topic_subscribers) = subscribers.get_mut(topic) {
            let mut new_subscribers = topic_subscribers.as_ref().clone();
            for &index in failed_indices.iter().rev() {
                if index < new_subscribers.len() {
                    new_subscribers.remove(index);
                }
            }

            if new_subscribers.is_empty() {
                subscribers.remove(topic);
            } else {
                *topic_subscribers = Arc::new(new_subscribers);
            }
        }
    }

    pub async fn remove_subscriber_by_task(&self, topic: String, task_id: TaskId) -> bool {
        let mut task_subs = self.task_subscriptions.write().await;
        let weak_sender = task_subs.remove(&(topic.clone(), task_id.clone()));
        drop(task_subs);

        if weak_sender.is_none() {
            log::warn!(
                "No mapping found for task {} and topic '{}'",
                task_id,
                topic
            );
            return false;
        }
        let mut subscribers = self.subscribers.write().await;
        if let Some(topic_subscribers) = subscribers.get_mut(&topic) {
            let mut new_subscribers = topic_subscribers.as_ref().clone();
            new_subscribers.retain(|sender| {
                if let Some(weak_ref) = weak_sender.as_ref() {
                    if let Some(strong_ref) = weak_ref.upgrade() {
                        !Arc::ptr_eq(sender, &strong_ref)
                    } else {
                        true
                    }
                } else {
                    true
                }
            });

            if new_subscribers.is_empty() {
                subscribers.remove(&topic);
                log::info!("Removed empty topic '{}'", topic);
            } else {
                *topic_subscribers = Arc::new(new_subscribers);
            }

            log::info!(
                "Removed subscriber for task {} from topic '{}'",
                task_id,
                topic
            );
            return true;
        } else {
            log::warn!("Topic '{}' not found for removal", topic);
            return false;
        }
    }

    pub async fn remove_all_subscriptions_by_task(&self, task_id: TaskId) -> Vec<String> {
        let mut removed_topics = Vec::new();
        let mut task_subs = self.task_subscriptions.write().await;
        let task_entries: Vec<(String, Weak<ExecutorOutputEventSender>)> = task_subs
            .iter()
            .filter(|((_, stored_task_id), _)| *stored_task_id == task_id)
            .map(|((topic, _), weak_sender)| (topic.clone(), weak_sender.clone()))
            .collect();
        task_subs.retain(|(_, stored_task_id), _| *stored_task_id != task_id);
        drop(task_subs);
        let mut subscribers = self.subscribers.write().await;
        for (topic, weak_sender) in task_entries {
            if let Some(topic_subscribers) = subscribers.get_mut(&topic) {
                let mut new_subscribers = topic_subscribers.as_ref().clone();
                if let Some(strong_ref) = weak_sender.upgrade() {
                    new_subscribers.retain(|sender| !Arc::ptr_eq(sender, &strong_ref));
                }

                if new_subscribers.is_empty() {
                    subscribers.remove(&topic);
                    log::info!("Removed empty topic '{}'", topic);
                    removed_topics.push(topic.clone());
                } else {
                    *topic_subscribers = Arc::new(new_subscribers);
                }
            }
        }

        log::info!(
            "Removed all subscriptions for task {} ({} topics affected)",
            task_id,
            removed_topics.len()
        );
        removed_topics
    }

    pub async fn get_subscribers(
        &self,
        topic: &str,
    ) -> Option<Vec<Arc<ExecutorOutputEventSender>>> {
        let subscribers = self.subscribers.read().await;
        subscribers
            .get(topic)
            .map(|arc_vec| arc_vec.as_ref().clone())
    }

    pub async fn get_topics_info(&self) -> Vec<(String, usize)> {
        let subscribers = self.subscribers.read().await;
        subscribers
            .iter()
            .map(|(topic, subscriber_list)| (topic.clone(), subscriber_list.len()))
            .collect()
    }

    pub async fn broadcast_message(&self, event: ExecutorOutputEvent) -> Result<usize, String> {
        let topic = match event.topic() {
            Some(topic) => topic,
            None => {
                return Err("Event does not contain a topic".to_string());
            }
        };
        let topic_owned = topic.clone();

        if matches!(event, ExecutorOutputEvent::Topic { .. }) {
            let mut latest_messages = self.latest_messages.write().await;
            latest_messages.insert(topic_owned.clone(), event.clone());
        }

        let subscribers = self.get_subscribers(&topic_owned).await;

        if let Some(subscriber_list) = subscribers {
            if subscriber_list.is_empty() {
                return Ok(0);
            }
            let send_workers: Vec<_> = subscriber_list
                .into_iter()
                .enumerate()
                .map(|(index, sender)| {
                    let event_clone = event.clone();
                    tokio::spawn(async move {
                        match sender.send(event_clone) {
                            Ok(_) => Ok(index),
                            Err(e) => Err((index, e)),
                        }
                    })
                })
                .collect();
            let results = futures::future::join_all(send_workers).await;

            let mut success_count = 0;
            let mut failed_indices = Vec::new();

            for result in results {
                match result {
                    Ok(Ok(_)) => success_count += 1,
                    Ok(Err((index, _))) => failed_indices.push(index),
                    Err(_) => {
                        failed_indices.push(0);
                    }
                }
            }
            if !failed_indices.is_empty() {
                self.remove_failed_subscribers(topic, failed_indices).await;
            }

            log::info!(
                "Broadcasted message to {} subscribers on topic '{}'",
                success_count,
                topic
            );
            Ok(success_count)
        } else {
            log::info!("No subscribers found for topic '{}'", topic);
            Ok(0)
        }
    }

    pub async fn get_latest_message(&self, topic: &str) -> Option<ExecutorOutputEvent> {
        let latest_messages = self.latest_messages.read().await;
        latest_messages.get(topic).cloned()
    }

    /// トピックの購読者をタスク名ごとにグループ化
    /// 戻り値: HashMap<タスク名, Vec<(TaskId, ExecutorOutputEventSender)>>
    pub async fn group_subscribers_by_task_name(
        &self,
        topic: &str,
    ) -> Result<HashMap<String, Vec<(TaskId, Arc<ExecutorOutputEventSender>)>>> {
        let subscribers = self.get_subscribers(topic).await;

        let mut grouped: HashMap<String, Vec<(TaskId, Arc<ExecutorOutputEventSender>)>> =
            HashMap::new();

        if let Some(subscriber_list) = subscribers {
            let task_subs = self.task_subscriptions.read().await;

            for subscriber in subscriber_list {
                // task_subscriptions から task_id を取得
                let mut found_task_id = None;
                for ((sub_topic, task_id), weak_sender) in task_subs.iter() {
                    if sub_topic == topic {
                        if let Some(strong_ref) = weak_sender.upgrade() {
                            if Arc::ptr_eq(&subscriber, &strong_ref) {
                                found_task_id = Some(task_id.clone());
                                break;
                            }
                        }
                    }
                }

                if let Some(task_id) = found_task_id {
                    // PodManager からタスク名を取得
                    if let Some(task_name) = self.pod_manager.get_pod_name_by_id(&task_id).await {
                        grouped
                            .entry(task_name)
                            .or_insert_with(Vec::new)
                            .push((task_id, subscriber));
                    }
                }
            }
        }

        Ok(grouped)
    }

    /// タスク名ごとの購読者を取得
    pub async fn get_subscribers_by_task_name(
        &self,
        topic: &str,
        task_name: &str,
    ) -> Result<Vec<(TaskId, Arc<ExecutorOutputEventSender>)>> {
        let grouped = self.group_subscribers_by_task_name(topic).await?;
        Ok(grouped.get(task_name).cloned().unwrap_or_default())
    }

    /// タスクの配信モードを判定
    /// Round Robin モードの場合は true、それ以外（ブロードキャスト）の場合は false
    pub async fn is_round_robin_mode(&self, task_name: &str) -> bool {
        if let Some(task_config) = self.system_config.tasks.get(task_name) {
            return task_config.lifecycle.mode == LifecycleMode::RoundRobin;
        }
        false
    }

    /// メッセージをルーティング（配信モードに応じて Round Robin またはブロードキャスト）
    pub async fn route_message(
        &self,
        topic: String,
        data: String,
    ) -> Result<usize, String> {
        // 購読者をタスク名ごとにグループ化
        let grouped = self
            .group_subscribers_by_task_name(&topic)
            .await
            .map_err(|e| format!("Failed to group subscribers: {}", e))?;

        if grouped.is_empty() {
            log::info!("No subscribers found for topic '{}'", topic);
            return Ok(0);
        }

        let mut total_sent = 0;

        // 各タスク名について配信
        for (task_name, subscribers) in grouped {
            let is_round_robin = self.is_round_robin_mode(&task_name).await;

            if is_round_robin {
                // Round Robin モード: TopicLoadBalancer を使用
                match self
                    .load_balancer
                    .dispatch_message(&task_name, topic.clone(), data.clone())
                    .await
                {
                    crate::topic_load_balancer::DispatchResult::Dispatched { .. } => {
                        total_sent += 1;
                        log::info!(
                            "Dispatched message to task '{}' via Round Robin",
                            task_name
                        );
                    }
                    crate::topic_load_balancer::DispatchResult::Queued { queue_size, .. } => {
                        log::debug!(
                            "Queued message for task '{}' (queue size: {})",
                            task_name,
                            queue_size
                        );
                    }
                }
            } else {
                // ブロードキャストモード: すべてのインスタンスに配信
                // ExecutorOutputEvent を作成して broadcast_message を使用
                let event = ExecutorOutputEvent::Topic {
                    message_id: crate::message_id::MessageId::new(),
                    task_id: TaskId::new(), // 外部からのメッセージなので task_id は新規作成
                    topic: topic.clone(),
                    data: data.clone(),
                };
                match self.broadcast_message(event).await {
                    Ok(count) => {
                        total_sent += count;
                    }
                    Err(e) => {
                        log::error!("Failed to broadcast message to task '{}': {}", task_name, e);
                    }
                }
            }
        }

        Ok(total_sent)
    }

    /// 外部からのメッセージ受信エントリーポイント
    pub async fn publish_message(
        &self,
        topic: String,
        data: String,
    ) -> Result<usize, String> {
        log::info!("Publishing message to topic '{}'", topic);

        // .result トピックの場合は既存の broadcast_message を使用
        if topic.ends_with(".result") {
            let event = ExecutorOutputEvent::Topic {
                message_id: crate::message_id::MessageId::new(),
                task_id: TaskId::new(), // 外部からのメッセージなので task_id は新規作成
                topic: topic.clone(),
                data: data.clone(),
            };
            return self.broadcast_message(event).await;
        }

        // それ以外のトピックは route_message を使用
        self.route_message(topic, data).await
    }

    /// タスクが idle に戻った時に、そのタスクが購読しているトピックのキューを処理
    pub async fn process_queue_for_task(&self, task_name: &str) {
        // SystemConfig からタスクの subscribe_topics を取得
        let subscribe_topics = if let Some(task_config) = self.system_config.tasks.get(task_name) {
            task_config.subscribe_topics.clone().unwrap_or_default()
        } else {
            log::warn!(
                "Task '{}' not found in system config, skipping queue processing",
                task_name
            );
            return;
        };

        if subscribe_topics.is_empty() {
            log::debug!(
                "Task '{}' has no subscribe_topics, skipping queue processing",
                task_name
            );
            return;
        }

        log::debug!(
            "Processing queue for task '{}' (subscribed topics: {:?})",
            task_name,
            subscribe_topics
        );

        // 各トピックについてキューを処理
        let mut total_processed = 0;
        for topic in subscribe_topics {
            let processed = self.load_balancer.process_queue(task_name, &topic).await;
            total_processed += processed;
        }

        if total_processed > 0 {
            log::info!(
                "Processed {} queued messages for task '{}'",
                total_processed,
                task_name
            );
        }
    }
}

