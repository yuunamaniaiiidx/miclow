use crate::messages::ExecutorOutputEvent;
use crate::subscription::SubscriptionId;
use crate::topic::Topic;
use crate::consumer::ConsumerId;
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
    max_log_size: Option<usize>,
}

impl TopicSubscriptionRegistry {
    pub fn new() -> Self {
        Self {
            messages: Arc::new(RwLock::new(HashMap::new())),
            message_cursors: Arc::new(RwLock::new(HashMap::new())),
            responses: Arc::new(RwLock::new(HashMap::new())),
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
            let mut messages = self.messages.write().await;
            // entry()は所有権を取るので、カーソル調整用にクローンを保存（必要な場合のみ）
            let topic_for_cursor = if self.max_log_size.is_some() {
                Some(topic_owned.clone())
            } else {
                None
            };
            let topic_str = topic_owned.as_str().to_string();
            let queue = messages
                .entry(topic_owned)
                .or_insert_with(VecDeque::new);
            let before_len = queue.len();
            log::debug!("store_message push: topic_str={:?}, before_len={:?}", topic_str, before_len);
            queue.push_back(event);
            let mut removed_count = 0;
            if let Some(max_size) = self.max_log_size {
                while queue.len() > max_size {
                    queue.pop_front();
                    removed_count += 1;
                }
            }
            // カーソル位置を調整（削除されたメッセージ数分だけ減らす）
            // 該当トピックのカーソルのみを更新
            if removed_count > 0 {
                if let Some(topic_for_cursor) = topic_for_cursor {
                    let mut cursors = self.message_cursors.write().await;
                    // 該当トピックのカーソルのみを効率的に更新
                    // イテレータで直接更新することで、不要なクローンを避ける
                    for (key, cursor) in cursors.iter_mut() {
                        if key.1 == topic_for_cursor {
                            if *cursor >= removed_count {
                                *cursor -= removed_count;
                            } else {
                                *cursor = 0;
                            }
                        }
                    }
                }
            }
            let after_len = queue.len();
            log::debug!("store_message push: topic_str={:?}, after_len={:?}", topic_str, after_len);
        }
        Ok(())
    }

    pub async fn pull_message(
        &self,
        subscription_id: SubscriptionId,
        topic: Topic,
    ) -> Option<ExecutorOutputEvent> {
        // まずreadロックでメッセージを読み取る
        let messages = self.messages.read().await;
        let queue = messages.get(&topic)?;
        let queue_len = queue.len();
        
        // カーソル更新のためにwriteロックを取得
        let mut cursors = self.message_cursors.write().await;
        let key = (subscription_id, topic);
        let cursor = cursors.entry(key.clone()).or_insert(0);
        
        log::debug!("called pull_message: key={:?}, topic_str={:?}, cursor={:?}, queue_len={:?}", key, key.1.as_str(), cursor, queue_len);
        
        if *cursor < queue_len {
            // readロックを保持したままクローン
            let result = queue[*cursor].clone();
            *cursor += 1;
            log::debug!("pull_message: key={:?}, pulled_ok, new_cursor={:?}", key, cursor);
            Some(result)
        } else {
            log::debug!("pull_message: key={:?}, no_data", key);
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
            let queue = responses
                .entry(key.clone())
                .or_insert_with(VecDeque::new);
            let before_len = queue.len();
            log::debug!("store_response push: key={:?}, topic_str={:?}, before_len={:?}", key, key.1.as_str(), before_len);
            queue.push_back(event);
            if let Some(max_size) = self.max_log_size {
                while queue.len() > max_size {
                    queue.pop_front();
                }
            }
            let after_len = queue.len();
            log::debug!("store_response push: key={:?}, after_len={:?}", key, after_len);
        }
        Ok(())
    }

    pub async fn pull_response(
        &self,
        consumer_id: ConsumerId,
        topic: Topic,
    ) -> Option<ExecutorOutputEvent> {
        let mut responses = self.responses.write().await;
        let key = (consumer_id, topic);
        log::debug!("pull_response called: key={:?}, topic_str={:?}", key, key.1.as_str());

        let queue = responses.get_mut(&key)?;
        
        let before_len = queue.len();
        log::debug!("called pull_response: key={:?}, topic_str={:?}, before_len={:?}", key, key.1.as_str(), before_len);
        
        let result = queue.pop_front();
        // 空になったキューを削除
        if queue.is_empty() {
            responses.remove(&key);
        }
        match &result {
            Some(_) => log::debug!("pull_response: key={:?}, pulled_ok", key),
            None => log::debug!("pull_response: key={:?}, no_data", key),
        }
        result
    }
}
