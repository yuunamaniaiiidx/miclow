use crate::channels::ExecutorOutputEventSender;
use crate::message_id::MessageId;
use crate::messages::ExecutorOutputEvent;
use crate::consumer::ConsumerId;
use crate::subscription::SubscriptionId;
use crate::topic::Topic;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct TopicSubscriptionRegistry {
    topic_destination: Arc<RwLock<HashMap<Topic, HashSet<SubscriptionId>>>>,
    senders: Arc<RwLock<HashMap<SubscriptionId, ExecutorOutputEventSender>>>,
    latest_messages: Arc<RwLock<HashMap<Topic, ExecutorOutputEvent>>>,
}

impl TopicSubscriptionRegistry {
    pub fn new() -> Self {
        Self {
            topic_destination: Arc::new(RwLock::new(HashMap::new())),
            senders: Arc::new(RwLock::new(HashMap::new())),
            latest_messages: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn add_subscriber(
        &self,
        topic: impl Into<Topic>,
        subscription_id: SubscriptionId,
        sender: ExecutorOutputEventSender,
    ) {
        let topic = topic.into();
        
        // SubscriptionIdとSenderのマッピングを登録
        {
            let mut senders = self.senders.write().await;
            senders.insert(subscription_id.clone(), sender);
        }
        
        // トピックとSubscriptionIdのマッピングを登録（HashSetなので重複は自動的に防がれる）
        let subscriber_count = {
            let mut topic_dest = self.topic_destination.write().await;
            topic_dest.entry(topic.clone()).or_insert_with(HashSet::new).insert(subscription_id);
            topic_dest.get(&topic).map(|s| s.len()).unwrap_or(0)
        };
        
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

        // トピックからSubscriptionIdのリストを取得
        let subscription_ids = {
            let topic_dest = self.topic_destination.read().await;
            topic_dest.get(&topic_owned).cloned()
        };

        let Some(subscription_ids) = subscription_ids else {
            log::info!(
                "No subscriber found for topic '{}', skipping broadcast",
                topic_owned
            );
            return Ok(());
        };

        // 各SubscriptionIdからSenderを取得して送信
        let mut success_count = 0;
        let mut error_count = 0;
        let senders = self.senders.read().await;
        for subscription_id in subscription_ids.iter() {
            if let Some(sender) = senders.get(subscription_id) {
                match sender.send(event.clone()) {
                    Ok(_) => {
                        success_count += 1;
                    }
                    Err(e) => {
                        error_count += 1;
                        log::warn!(
                            "Failed to send message on topic '{}' to Subscription {}: {}",
                            topic_owned,
                            subscription_id,
                            e
                        );
                    }
                }
            } else {
                error_count += 1;
                log::warn!(
                    "Sender not found for Subscription {} on topic '{}'",
                    subscription_id,
                    topic_owned
                );
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
        match topic.as_str().to_lowercase().as_str() {
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
        let from_subscription_id = event.from_subscription_id().cloned();

        // レスポンスを送信（topic.resultトピックで）
        let response_topic = topic.result();
        let response_event = ExecutorOutputEvent::Topic {
            message_id: MessageId::new(),
            pod_id: ConsumerId::new(),
            from_subscription_id: SubscriptionId::new(),
            to_subscription_id: from_subscription_id,
            topic: response_topic.clone(),
            data: Arc::from("Unknown Command".to_string()),
        };

        // レスポンスを直接送信（broadcast_messageを再帰的に呼ばない）
        self.send_to_topic_subscribers(&response_topic, response_event).await;

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
        let from_subscription_id = event.from_subscription_id().cloned();

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
        let response_topic = Topic::from("system.pull");
        let response_event = ExecutorOutputEvent::Topic {
            message_id: MessageId::new(),
            pod_id: ConsumerId::new(),
            from_subscription_id: SubscriptionId::new(),
            to_subscription_id: from_subscription_id,
            topic: response_topic.clone(),
            data: Arc::from(response_data),
        };

        // レスポンスを直接送信（broadcast_messageを再帰的に呼ばない）
        self.send_to_topic_subscribers(&response_topic, response_event).await;

        Ok(())
    }

    /// 特定のトピックの購読者にイベントを送信（内部用）
    async fn send_to_topic_subscribers(
        &self,
        topic: &Topic,
        event: ExecutorOutputEvent,
    ) {
        // トピックからSubscriptionIdのリストを取得
        let subscription_ids = {
            let topic_dest = self.topic_destination.read().await;
            topic_dest.get(topic).cloned()
        };

        if let Some(subscription_ids) = subscription_ids {
            let senders = self.senders.read().await;
            for subscription_id in subscription_ids.iter() {
                if let Some(sender) = senders.get(subscription_id) {
                    if let Err(e) = sender.send(event.clone()) {
                        log::warn!(
                            "Failed to send message on topic '{}' to Subscription {}: {}",
                            topic,
                            subscription_id,
                            e
                        );
                    }
                }
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
        topic.as_str().to_lowercase().starts_with("system.")
    }
}
