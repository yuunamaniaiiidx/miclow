use crate::channels::ExecutorOutputEventSender;
use crate::message_id::MessageId;
use crate::messages::ExecutorOutputEvent;
use crate::pod::PodId;
use crate::replicaset::ReplicaSetId;
use crate::topic::Topic;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct TopicSubscriptionRegistry {
    topic_senders: Arc<RwLock<HashMap<Topic, Vec<ExecutorOutputEventSender>>>>,
    latest_messages: Arc<RwLock<HashMap<Topic, ExecutorOutputEvent>>>,
}

impl TopicSubscriptionRegistry {
    pub fn new() -> Self {
        Self {
            topic_senders: Arc::new(RwLock::new(HashMap::new())),
            latest_messages: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn add_subscriber(
        &self,
        topic: impl Into<Topic>,
        sender: ExecutorOutputEventSender,
    ) {
        let topic = topic.into();
        let mut topics = self.topic_senders.write().await;
        
        topics.entry(topic.clone()).or_insert_with(Vec::new).push(sender);

        let subscriber_count = topics.get(&topic).map(|v| v.len()).unwrap_or(0);
        log::info!(
            "Registered subscriber for topic '{}' (total subscribers: {})",
            topic,
            subscriber_count
        );
    }

    pub async fn broadcast_message(&self, event: ExecutorOutputEvent) -> Result<(), String> {
        let topic_owned = match event.topic() {
            Some(topic) => topic.clone(),
            None => {
                return Err("Event does not contain a topic".to_string());
            }
        };

        // システムコマンドの処理
        if self.is_system_topic(&topic_owned) {
            return self.handle_system_command(&topic_owned, event).await;
        }

        if matches!(event, ExecutorOutputEvent::Topic { .. }) {
            let mut latest_messages = self.latest_messages.write().await;
            latest_messages.insert(topic_owned.clone(), event.clone());
        }

        let senders = {
            let topics = self.topic_senders.read().await;
            topics.get(&topic_owned).cloned()
        };

        let Some(senders) = senders else {
            log::info!(
                "No subscriber found for topic '{}', skipping broadcast",
                topic_owned
            );
            return Ok(());
        };

        let mut success_count = 0;
        let mut error_count = 0;
        for sender in senders.iter() {
            match sender.send(event.clone()) {
                Ok(_) => {
                    success_count += 1;
                }
                Err(e) => {
                    error_count += 1;
                    log::warn!(
                        "Failed to send message on topic '{}': {}",
                        topic_owned,
                        e
                    );
                }
            }
        }

        if error_count > 0 {
            log::warn!(
                "Broadcasted message on topic '{}' to {} subscribers ({} failed)",
                topic_owned,
                success_count,
                error_count
            );
        } else {
            log::info!(
                "Broadcasted message on topic '{}' to {} subscribers",
                topic_owned,
                success_count
            );
        }

        Ok(())
    }

    /// システムコマンドを処理
    async fn handle_system_command(
        &self,
        topic: &Topic,
        event: ExecutorOutputEvent,
    ) -> Result<(), String> {
        match topic.as_str() {
            "system.pull" => self.handle_pull_command(event).await,
            _ => self.handle_unknown_command(topic, event).await,
        }
    }

    /// 不明なシステムコマンドを処理
    async fn handle_unknown_command(
        &self,
        topic: &Topic,
        event: ExecutorOutputEvent,
    ) -> Result<(), String> {
        let from_replicaset_id = event.from_replicaset_id().cloned();

        // レスポンスを送信
        let response_event = ExecutorOutputEvent::Topic {
            message_id: MessageId::new(),
            pod_id: PodId::new(),
            from_replicaset_id: ReplicaSetId::new(),
            to_replicaset_id: from_replicaset_id,
            topic: topic.clone(),
            data: Arc::from("Unknown Command".to_string()),
        };

        // レスポンスを直接送信（broadcast_messageを再帰的に呼ばない）
        self.send_to_topic_subscribers(topic, response_event).await;

        Ok(())
    }

    /// system.pullコマンドを処理
    async fn handle_pull_command(
        &self,
        event: ExecutorOutputEvent,
    ) -> Result<(), String> {
        // メッセージから対象トピックを取得
        let target_topic_str = event.data()
            .ok_or_else(|| "Event does not contain data".to_string())?
            .trim();
        let target_topic = Topic::from(target_topic_str);
        let from_replicaset_id = event.from_replicaset_id().cloned();

        // 最新メッセージを取得
        let latest_event = self.get_latest_message(target_topic.clone()).await;

        // レスポンスを生成
        let response_data = if let Some(latest) = latest_event {
            if let Some(data) = latest.data() {
                format!("success\n{}", data)
            } else {
                "success\n".to_string()
            }
        } else {
            format!("error\nNo message found for topic '{}'", target_topic_str)
        };

        // レスポンスを送信（system.pullトピックで）
        let response_event = ExecutorOutputEvent::Topic {
            message_id: MessageId::new(),
            pod_id: PodId::new(),
            from_replicaset_id: ReplicaSetId::new(),
            to_replicaset_id: from_replicaset_id,
            topic: Topic::from("system.pull"),
            data: Arc::from(response_data),
        };

        // レスポンスを直接送信（broadcast_messageを再帰的に呼ばない）
        self.send_to_topic_subscribers(&Topic::from("system.pull"), response_event).await;

        Ok(())
    }

    /// 特定のトピックの購読者にイベントを送信（内部用）
    async fn send_to_topic_subscribers(
        &self,
        topic: &Topic,
        event: ExecutorOutputEvent,
    ) {
        let senders = {
            let topics = self.topic_senders.read().await;
            topics.get(topic).cloned()
        };

        if let Some(senders) = senders {
            for sender in senders.iter() {
                let _ = sender.send(event.clone());
            }
        }
    }

    pub async fn get_latest_message(&self, topic: impl Into<Topic>) -> Option<ExecutorOutputEvent> {
        let topic = topic.into();
        let latest_messages = self.latest_messages.read().await;
        latest_messages.get(&topic).cloned()
    }

    /// システムトピックかどうかを判定
    /// `system.*` プレフィックスで判定
    fn is_system_topic(&self, topic: &Topic) -> bool {
        topic.as_str().starts_with("system.")
    }
}
