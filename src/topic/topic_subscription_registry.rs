use crate::message_id::MessageId;
use crate::messages::ExecutorOutputEvent;
use crate::subscription::SubscriptionId;
use crate::topic::Topic;
use crate::consumer::ConsumerId;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

struct TimestampedMessage {
    timestamp: SystemTime,
    event: ExecutorOutputEvent,
}

#[derive(Clone)]
pub struct TopicSubscriptionRegistry {
    message_log: Arc<RwLock<HashMap<Topic, VecDeque<TimestampedMessage>>>>,
    subscription_cursors: Arc<RwLock<HashMap<(SubscriptionId, Topic), MessageId>>>,
    response_log: Arc<RwLock<HashMap<ConsumerId, VecDeque<TimestampedMessage>>>>,
    response_cursors: Arc<RwLock<HashMap<ConsumerId, MessageId>>>,
    retention_duration: Duration,
}

impl TopicSubscriptionRegistry {
    pub fn new() -> Self {
        Self {
            message_log: Arc::new(RwLock::new(HashMap::new())),
            subscription_cursors: Arc::new(RwLock::new(HashMap::new())),
            response_log: Arc::new(RwLock::new(HashMap::new())),
            response_cursors: Arc::new(RwLock::new(HashMap::new())),
            retention_duration: Duration::from_secs(60 * 60),
        }
    }

    /// Cursorを更新
    async fn update_cursor(
        &self,
        subscription_id: SubscriptionId,
        topic: Topic,
        message_id: MessageId,
    ) {
        let mut cursors = self.subscription_cursors.write().await;
        cursors.insert((subscription_id, topic), message_id);
    }

    /// 現在のCursor位置を取得（存在しない場合はNone）
    async fn get_cursor(
        &self,
        subscription_id: &SubscriptionId,
        topic: &Topic,
    ) -> Option<MessageId> {
        let cursors = self.subscription_cursors.read().await;
        cursors.get(&(subscription_id.clone(), topic.clone())).cloned()
    }

    /// メッセージを履歴に保存
    pub async fn store_message(&self, event: ExecutorOutputEvent) -> Result<(), String> {
        let topic_owned = match event.topic() {
            Some(topic) => topic.clone(),
            None => {
                return Err("Event does not contain a topic".to_string());
            }
        };

        // メッセージを履歴に保存
        if matches!(event, ExecutorOutputEvent::Topic { .. }) {
            let timestamped = TimestampedMessage {
                timestamp: SystemTime::now(),
                event: event.clone(),
            };

            let mut message_log = self.message_log.write().await;
            let log = message_log.entry(topic_owned.clone()).or_insert_with(VecDeque::new);
            log.push_back(timestamped);

            // 時間ベースのクリーンアップ
            let cutoff_time = SystemTime::now() - self.retention_duration;
            while let Some(front) = log.front() {
                if front.timestamp < cutoff_time {
                    log.pop_front();
                } else {
                    break;
                }
            }
        }

        Ok(())
    }

    /// SubscriptionがCursor位置から1件のメッセージを取得
    pub async fn pull_message(
        &self,
        subscription_id: SubscriptionId,
        topic: Topic,
    ) -> Option<ExecutorOutputEvent> {
        let message_log = self.message_log.read().await;
        let log = message_log.get(&topic)?;

        // Cursor位置を取得
        let cursor_id = self.get_cursor(&subscription_id, &topic).await;
        let mut found_cursor = cursor_id.is_none();

        for timestamped in log.iter() {
            // MessageIdを取得（Topicイベントのみ）
            let message_id = match &timestamped.event {
                ExecutorOutputEvent::Topic { message_id, .. } => Some(message_id.clone()),
                _ => None,
            };

            // Cursorが見つかるまでスキップ
            if !found_cursor {
                if let Some(cursor) = &cursor_id {
                    if let Some(msg_id) = &message_id {
                        if msg_id == cursor {
                            found_cursor = true;
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                } else {
                    found_cursor = true;
                }
            }

            // Cursor以降の最初のメッセージを取得
            if found_cursor {
                // メッセージをクローンしてからロックを解放
                let event = timestamped.event.clone();
                let msg_id_for_cursor = message_id.clone();
                drop(message_log); // ロックを解放
                
                // Cursorを更新
                if let Some(msg_id) = msg_id_for_cursor {
                    self.update_cursor(subscription_id, topic, msg_id).await;
                }
                return Some(event);
            }
        }

        None
    }

    /// レスポンストピックのメッセージをconsumer_idごとに保存
    pub async fn store_response(
        &self,
        consumer_id: ConsumerId,
        event: ExecutorOutputEvent,
    ) -> Result<(), String> {
        let timestamped = TimestampedMessage {
            timestamp: SystemTime::now(),
            event: event.clone(),
        };

        let mut response_log = self.response_log.write().await;
        let log = response_log.entry(consumer_id.clone()).or_insert_with(VecDeque::new);
        log.push_back(timestamped);

        // 時間ベースのクリーンアップ
        let cutoff_time = SystemTime::now() - self.retention_duration;
        while let Some(front) = log.front() {
            if front.timestamp < cutoff_time {
                log.pop_front();
            } else {
                break;
            }
        }

        Ok(())
    }

    /// consumer_idを指定してレスポンスメッセージを取得
    pub async fn pull_response(
        &self,
        consumer_id: ConsumerId,
    ) -> Option<ExecutorOutputEvent> {
        let response_log = self.response_log.read().await;
        let log = response_log.get(&consumer_id)?;

        // Cursor位置を取得
        let cursor_id = {
            let cursors = self.response_cursors.read().await;
            cursors.get(&consumer_id).cloned()
        };
        let mut found_cursor = cursor_id.is_none();

        for timestamped in log.iter() {
            // MessageIdを取得（Topicイベントのみ）
            let message_id = match &timestamped.event {
                ExecutorOutputEvent::Topic { message_id, .. } => Some(message_id.clone()),
                _ => None,
            };

            // Cursorが見つかるまでスキップ
            if !found_cursor {
                if let Some(cursor) = &cursor_id {
                    if let Some(msg_id) = &message_id {
                        if msg_id == cursor {
                            found_cursor = true;
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                } else {
                    found_cursor = true;
                }
            }

            // Cursor以降の最初のメッセージを取得
            if found_cursor {
                // メッセージをクローンしてからロックを解放
                let event = timestamped.event.clone();
                let msg_id_for_cursor = message_id.clone();
                drop(response_log); // ロックを解放
                
                // Cursorを更新
                if let Some(msg_id) = msg_id_for_cursor {
                    let mut cursors = self.response_cursors.write().await;
                    cursors.insert(consumer_id, msg_id);
                }
                return Some(event);
            }
        }

        None
    }
}
