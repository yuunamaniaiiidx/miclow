use crate::messages::ExecutorOutputEvent;
use crate::subscription::SubscriptionId;
use crate::topic::{MessageLog, Topic};
use crate::consumer::ConsumerId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

type SubscriptionKey = (SubscriptionId, Topic);

#[derive(Clone)]
pub struct TopicSubscriptionRegistry {
    // Topicごとのメッセージログ（各ログは複数のSubscriptionカーソルを持つ）
    message_logs: Arc<RwLock<HashMap<Topic, MessageLog<SubscriptionKey, ExecutorOutputEvent>>>>,
    // ConsumerIdごとのレスポンスログ
    response_logs: Arc<RwLock<HashMap<ConsumerId, MessageLog<ConsumerId, ExecutorOutputEvent>>>>,
    // ログの最大サイズ（Noneの場合は無制限）
    max_log_size: Option<usize>,
}

impl TopicSubscriptionRegistry {
    pub fn new() -> Self {
        Self {
            message_logs: Arc::new(RwLock::new(HashMap::new())),
            response_logs: Arc::new(RwLock::new(HashMap::new())),
            max_log_size: Some(10000), // デフォルトで最大10000件
        }
    }

    /// 最大ログサイズを設定
    pub fn with_max_log_size(mut self, max_size: Option<usize>) -> Self {
        self.max_log_size = max_size;
        self
    }

    /// メッセージを履歴に保存
    pub async fn store_message(&self, event: ExecutorOutputEvent) -> Result<(), String> {
        let topic_owned = match event.topic() {
            Some(topic) => topic.clone(),
            None => {
                return Err("Event does not contain a topic".to_string());
            }
        };

        // メッセージを履歴に保存（Topicイベントのみ）
        if matches!(event, ExecutorOutputEvent::Topic { .. }) {
            let mut message_logs = self.message_logs.write().await;
            let log = message_logs
                .entry(topic_owned.clone())
                .or_insert_with(|| MessageLog::<SubscriptionKey, ExecutorOutputEvent>::new(self.max_log_size));
            log.push(event).await;
        }

        Ok(())
    }

    /// SubscriptionがCursor位置から1件のメッセージを取得
    pub async fn pull_message(
        &self,
        subscription_id: SubscriptionId,
        topic: Topic,
    ) -> Option<ExecutorOutputEvent> {
        let message_logs = self.message_logs.read().await;
        let log = message_logs.get(&topic)?;
        let key = (subscription_id, topic);
        log.pull(key).await
    }

    /// レスポンストピックのメッセージをconsumer_idごとに保存
    pub async fn store_response(
        &self,
        consumer_id: ConsumerId,
        event: ExecutorOutputEvent,
    ) -> Result<(), String> {
        let mut response_logs = self.response_logs.write().await;
        let log = response_logs
            .entry(consumer_id.clone())
            .or_insert_with(|| MessageLog::<ConsumerId, ExecutorOutputEvent>::new(self.max_log_size));
        log.push(event).await;
        Ok(())
    }

    /// consumer_idを指定してレスポンスメッセージを取得
    pub async fn pull_response(
        &self,
        consumer_id: ConsumerId,
    ) -> Option<ExecutorOutputEvent> {
        let response_logs = self.response_logs.read().await;
        let log = response_logs.get(&consumer_id)?;
        log.pull(consumer_id).await
    }

    /// すべてのログをクリーンアップ（すべてのカーソルより前のメッセージを削除）
    pub async fn cleanup(&self) {
        let mut message_logs = self.message_logs.write().await;
        for log in message_logs.values_mut() {
            log.cleanup().await;
        }

        let mut response_logs = self.response_logs.write().await;
        for log in response_logs.values_mut() {
            log.cleanup().await;
        }
    }
}
