use crate::channels::TopicNotificationSender;
use crate::consumer::ConsumerId;
use crate::messages::ExecutorOutputEvent;
use crate::subscription::SubscriptionId;
use crate::topic::Topic;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

type SubscriptionKey = (SubscriptionId, Topic);
type ResponseKey = (ConsumerId, Topic);

#[derive(Clone)]
pub struct TopicSubscriptionRegistry {
    // メッセージはTopicのみをキーにグローバルに保存
    messages: Arc<RwLock<HashMap<Topic, VecDeque<ExecutorOutputEvent>>>>,
    // 各subscriptionのカーソル位置（インデックス）
    message_cursors: Arc<RwLock<HashMap<SubscriptionKey, usize>>>,
    // responseは(ConsumerId, Topic)をキーに各consumerごとに独立したキューとして保存
    responses: Arc<RwLock<HashMap<ResponseKey, VecDeque<ExecutorOutputEvent>>>>,
    // トピック通知用のsenderリスト
    topic_notification_senders: Arc<RwLock<Vec<TopicNotificationSender>>>,
    max_log_size: Option<usize>,
}

impl TopicSubscriptionRegistry {
    pub fn new() -> Self {
        Self {
            messages: Arc::new(RwLock::new(HashMap::new())),
            message_cursors: Arc::new(RwLock::new(HashMap::new())),
            responses: Arc::new(RwLock::new(HashMap::new())),
            topic_notification_senders: Arc::new(RwLock::new(Vec::new())),
            max_log_size: Some(10000),
        }
    }

    pub fn with_max_log_size(mut self, max_size: Option<usize>) -> Self {
        self.max_log_size = max_size;
        self
    }

    pub async fn store_message(&self, event: ExecutorOutputEvent) -> Result<(), String> {
        let topic_owned = match event.topic() {
            Some(topic) => topic.clone(),
            None => {
                return Err("Event does not contain a topic".to_string());
            }
        };
        if matches!(event, ExecutorOutputEvent::Topic { .. }) {
            let removed_count = {
                let mut messages = self.messages.write().await;
                let topic_str = topic_owned.as_str().to_string();
                let queue = messages
                    .entry(topic_owned.clone())
                    .or_insert_with(VecDeque::new);
                let before_len = queue.len();
                log::debug!(
                    "store_message push: topic_str={:?}, before_len={:?}",
                    topic_str,
                    before_len
                );
                queue.push_back(event);
                let mut removed_count = 0;
                if let Some(max_size) = self.max_log_size {
                    while queue.len() > max_size {
                        queue.pop_front();
                        removed_count += 1;
                    }
                }
                let after_len = queue.len();
                log::debug!(
                    "store_message push: topic_str={:?}, after_len={:?}",
                    topic_str,
                    after_len
                );
                // messagesのwriteロックをここで解放
                removed_count
            };

            // トピックが追加された際は常に通知
            let senders = self.topic_notification_senders.read().await;
            for sender in senders.iter() {
                if let Err(e) = sender.send(topic_owned.clone()) {
                    log::debug!("Failed to send topic notification: {}", e);
                }
            }
            // カーソル位置を調整（削除されたメッセージ数分だけ減らす）
            // 該当トピックのカーソルのみを更新
            // messagesのロックを解放してからcursorsのロックを取得
            if removed_count > 0 {
                let mut cursors = self.message_cursors.write().await;
                // 該当トピックのカーソルのみを効率的に更新
                // イテレータで直接更新することで、不要なクローンを避ける
                for (key, cursor) in cursors.iter_mut() {
                    if key.1 == topic_owned {
                        if *cursor >= removed_count {
                            *cursor -= removed_count;
                        } else {
                            *cursor = 0;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn pop_message(
        &self,
        subscription_id: SubscriptionId,
        topic: Topic,
    ) -> Option<ExecutorOutputEvent> {
        let key = (subscription_id, topic.clone());

        // カーソル位置を取得（readロックのみ）
        let cursor = {
            let cursors = self.message_cursors.read().await;
            cursors.get(&key).copied().unwrap_or(0)
        };

        // メッセージを読み取ってクローン（readロックのみ、最小限の保持時間）
        let result = {
            let messages = self.messages.read().await;
            let queue = messages.get(&topic)?;
            let queue_len = queue.len();

            log::debug!(
                "called pop_message: key={:?}, topic_str={:?}, cursor={:?}, queue_len={:?}",
                key,
                key.1.as_str(),
                cursor,
                queue_len
            );

            if cursor < queue_len {
                Some(queue[cursor].clone())
            } else {
                log::debug!("pop_message: key={:?}, no_data", key);
                return None;
            }
        };

        // カーソル更新のためにwriteロックを取得（メッセージのロックは既に解放済み）
        if result.is_some() {
            let mut cursors = self.message_cursors.write().await;
            let cursor_entry = cursors.entry(key.clone()).or_insert(0);
            *cursor_entry += 1;
            log::debug!(
                "pop_message: key={:?}, popped_ok, new_cursor={:?}",
                key,
                cursor_entry
            );
        }

        result
    }

    pub async fn peek_message(
        &self,
        subscription_id: SubscriptionId,
        topic: Topic,
    ) -> Option<ExecutorOutputEvent> {
        let key = (subscription_id, topic.clone());

        // カーソル位置を取得（readロックのみ、最小限の保持時間）
        let cursor = {
            let cursors = self.message_cursors.read().await;
            cursors.get(&key).copied().unwrap_or(0)
        };

        // メッセージを読み取ってクローン（readロックのみ、最小限の保持時間）
        let messages = self.messages.read().await;
        let queue = messages.get(&topic)?;
        let queue_len = queue.len();

        log::debug!(
            "called peek_message: key={:?}, topic_str={:?}, cursor={:?}, queue_len={:?}",
            key,
            key.1.as_str(),
            cursor,
            queue_len
        );

        if cursor < queue_len {
            let result = queue[cursor].clone();
            log::debug!("peek_message: key={:?}, peeked_ok", key);
            Some(result)
        } else {
            log::debug!("peek_message: key={:?}, no_data", key);
            None
        }
    }

    pub async fn latest_message(&self, topic: Topic) -> Option<ExecutorOutputEvent> {
        // readロックでメッセージを読み取る（最新のメッセージを取得）
        let messages = self.messages.read().await;
        let queue = messages.get(&topic)?;

        log::debug!(
            "called latest_message: topic_str={:?}, queue_len={:?}",
            topic.as_str(),
            queue.len()
        );

        if let Some(latest) = queue.back() {
            let result = latest.clone();
            log::debug!("latest_message: topic_str={:?}, latest_ok", topic.as_str());
            Some(result)
        } else {
            log::debug!("latest_message: topic_str={:?}, no_data", topic.as_str());
            None
        }
    }

    pub async fn store_response(
        &self,
        consumer_id: ConsumerId,
        event: ExecutorOutputEvent,
    ) -> Result<(), String> {
        let topic_owned = match event.topic() {
            Some(topic) => topic.clone(),
            None => {
                return Err("Event does not contain a topic".to_string());
            }
        };
        if topic_owned.as_str() == "system.result" {
            return Ok(());
        }
        if matches!(event, ExecutorOutputEvent::Topic { .. }) {
            let mut responses = self.responses.write().await;
            let key = (consumer_id, topic_owned);
            let queue = responses.entry(key.clone()).or_insert_with(VecDeque::new);
            let before_len = queue.len();
            log::debug!(
                "store_response push: key={:?}, topic_str={:?}, before_len={:?}",
                key,
                key.1.as_str(),
                before_len
            );
            queue.push_back(event);
            if let Some(max_size) = self.max_log_size {
                while queue.len() > max_size {
                    queue.pop_front();
                }
            }
            let after_len = queue.len();
            log::debug!(
                "store_response push: key={:?}, after_len={:?}",
                key,
                after_len
            );
        }
        Ok(())
    }

    pub async fn pop_response(
        &self,
        consumer_id: ConsumerId,
        topic: Topic,
    ) -> Option<ExecutorOutputEvent> {
        let mut responses = self.responses.write().await;
        let key = (consumer_id, topic);
        log::debug!(
            "pop_response called: key={:?}, topic_str={:?}",
            key,
            key.1.as_str()
        );

        let queue = responses.get_mut(&key)?;

        let before_len = queue.len();
        log::debug!(
            "called pop_response: key={:?}, topic_str={:?}, before_len={:?}",
            key,
            key.1.as_str(),
            before_len
        );

        let result = queue.pop_front();
        // 空になったキューを削除
        if queue.is_empty() {
            responses.remove(&key);
        }
        match &result {
            Some(_) => log::debug!("pop_response: key={:?}, popped_ok", key),
            None => log::debug!("pop_response: key={:?}, no_data", key),
        }
        result
    }

    /// トピック通知用のsenderを登録する
    pub async fn register_topic_notification_sender(&self, sender: TopicNotificationSender) {
        let mut senders = self.topic_notification_senders.write().await;
        senders.push(sender);
    }
}
