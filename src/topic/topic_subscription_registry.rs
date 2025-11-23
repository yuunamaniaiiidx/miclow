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
            let queue = messages
                .entry(topic_owned.clone())
                .or_insert_with(VecDeque::new);
            let before_len = queue.len();
            log::info!("store_message push: topic={:?}, topic_addr={:p}, topic_str={:?}, before_len={:?}", topic_owned, &topic_owned, topic_owned.as_str(), before_len);
            queue.push_back(event);
            let mut removed_count = 0;
            if let Some(max_size) = self.max_log_size {
                while queue.len() > max_size {
                    queue.pop_front();
                    removed_count += 1;
                }
            }
            // カーソル位置を調整（削除されたメッセージ数分だけ減らす）
            if removed_count > 0 {
                let mut cursors = self.message_cursors.write().await;
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
            let after_len = queue.len();
            log::info!("store_message push: topic={:?}, topic_addr={:p}, after_len={:?}", topic_owned, &topic_owned, after_len);
        }
        Ok(())
    }

    pub async fn pull_message(
        &self,
        subscription_id: SubscriptionId,
        topic: Topic,
    ) -> Option<ExecutorOutputEvent> {
        let mut messages = self.messages.write().await;
        let queue = messages.get_mut(&topic)?;
        
        let key = (subscription_id.clone(), topic.clone());
        let mut cursors = self.message_cursors.write().await;
        let cursor = cursors.entry(key.clone()).or_insert(0);
        
        log::info!("called pull_message: key={:?}, topic_addr={:p}, topic_str={:?}, cursor={:?}, queue_len={:?}", key, &key.1, key.1.as_str(), cursor, queue.len());
        
        if *cursor < queue.len() {
            let result = queue[*cursor].clone();
            *cursor += 1;
            log::info!("pull_message: key={:?}, topic_addr={:p}, pulled_ok, new_cursor={:?}", key, &key.1, cursor);
            Some(result)
        } else {
            log::info!("pull_message: key={:?}, topic_addr={:p}, no_data", key, &key.1);
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
            let key = (consumer_id.clone(), topic_owned.clone());
            let queue = responses
                .entry(key.clone())
                .or_insert_with(VecDeque::new);
            let before_len = queue.len();
            log::info!("store_response push: key={:?}, topic_addr={:p}, topic_str={:?}, before_len={:?}", key, &key.1, key.1.as_str(), before_len);
            queue.push_back(event);
            if let Some(max_size) = self.max_log_size {
                while queue.len() > max_size {
                    queue.pop_front();
                }
            }
            let after_len = queue.len();
            log::info!("store_response push: key={:?}, topic_addr={:p}, after_len={:?}", key, &key.1, after_len);
        }
        Ok(())
    }

    pub async fn pull_response(
        &self,
        consumer_id: ConsumerId,
        topic: Topic,
    ) -> Option<ExecutorOutputEvent> {
        let mut responses = self.responses.write().await;
        let key = (consumer_id.clone(), topic.clone());
        log::info!("pull_response called: key={:?}, topic_addr={:p}, topic_str={:?}", key, &key.1, key.1.as_str());

        let queue = responses.get_mut(&key)?;
        
        let before_len = queue.len();
        log::info!("called pull_response: key={:?}, topic_addr={:p}, topic_str={:?}, before_len={:?}", key, &key.1, key.1.as_str(), before_len);
        
        let result = queue.pop_front();
        // 空になったキューを削除
        if queue.is_empty() {
            responses.remove(&key);
        }
        match &result {
            Some(_) => log::info!("pull_response: key={:?}, topic_addr={:p}, pulled_ok", key, &key.1),
            None => log::info!("pull_response: key={:?}, topic_addr={:p}, no_data", key, &key.1),
        }
        result
    }
}
