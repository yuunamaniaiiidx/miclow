use crate::message_id::MessageId;
use crate::messages::ExecutorOutputEvent;
use crate::consumer::ConsumerId;
use crate::subscription::SubscriptionId;
use crate::topic::Topic;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

struct TimestampedMessage {
    timestamp: SystemTime,
    event: ExecutorOutputEvent,
}

#[derive(Clone)]
pub struct TopicSubscriptionRegistry {
    topic_destination: Arc<RwLock<HashMap<Topic, HashSet<SubscriptionId>>>>,
    message_log: Arc<RwLock<HashMap<Topic, VecDeque<TimestampedMessage>>>>,
    subscription_cursors: Arc<RwLock<HashMap<(SubscriptionId, Topic), MessageId>>>,
    retention_duration: Duration,
}

impl TopicSubscriptionRegistry {
    pub fn new() -> Self {
        Self {
            topic_destination: Arc::new(RwLock::new(HashMap::new())),
            message_log: Arc::new(RwLock::new(HashMap::new())),
            subscription_cursors: Arc::new(RwLock::new(HashMap::new())),
            retention_duration: Duration::from_secs(24 * 60 * 60), // 24時間
        }
    }

    pub async fn add_subscriber(
        &self,
        topic: impl Into<Topic>,
        subscription_id: SubscriptionId,
    ) {
        let topic = topic.into();
        
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

        // レスポンスを保存（topic.resultトピックで）
        let response_topic = topic.result();
        let response_event = ExecutorOutputEvent::Topic {
            message_id: MessageId::new(),
            pod_id: ConsumerId::new(),
            from_subscription_id: SubscriptionId::new(),
            to_subscription_id: from_subscription_id,
            topic: response_topic.clone(),
            data: Arc::from("Unknown Command".to_string()),
        };

        // レスポンスを保存（再帰的にhandle_system_commandを呼ばない）
        self.store_system_response(response_event).await?;

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

        // Pull型APIで最新メッセージを取得（limit=1）
        // from_subscription_idがNoneの場合は一時的なSubscriptionIdを使用
        let temp_subscription_id = from_subscription_id.clone().unwrap_or_else(SubscriptionId::new);
        let messages = self.pull_messages(
            temp_subscription_id.clone(),
            target_topic.clone(),
            1,
        ).await;

        // レスポンスを生成
        let response_data = if let Some(latest) = messages.first() {
            if let Some(data) = latest.data() {
                format!("success\n{}", data)
            } else {
                "success\n".to_string()
            }
        } else {
            format!("error\nNo message found for topic '{}'", target_topic_str)
        };

        // レスポンスを保存（system.pullトピックで）
        let response_topic = Topic::from("system.pull");
        let response_event = ExecutorOutputEvent::Topic {
            message_id: MessageId::new(),
            pod_id: ConsumerId::new(),
            from_subscription_id: SubscriptionId::new(),
            to_subscription_id: from_subscription_id,
            topic: response_topic.clone(),
            data: Arc::from(response_data),
        };

        // レスポンスを保存（再帰的にhandle_system_commandを呼ばない）
        self.store_system_response(response_event).await?;

        Ok(())
    }


    /// システムトピックかどうかを判定
    /// `system.*` プレフィックスで判定
    fn is_system_topic(&self, topic: &Topic) -> bool {
        topic.as_str().to_lowercase().starts_with("system.")
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

        // システムコマンドの処理（再帰を避けるため、システムコマンドは処理のみで保存しない）
        if self.is_system_topic(&topic_owned) {
            return self.handle_system_command(&topic_owned, event).await;
        }

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

    /// システムコマンドのレスポンスを保存（再帰を避けるため）
    async fn store_system_response(&self, event: ExecutorOutputEvent) -> Result<(), String> {
        let topic_owned = match event.topic() {
            Some(topic) => topic.clone(),
            None => {
                return Err("Event does not contain a topic".to_string());
            }
        };

        // メッセージを履歴に保存（システムコマンドチェックなし）
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

    /// SubscriptionがCursor位置から指定件数のメッセージを取得
    pub async fn pull_messages(
        &self,
        subscription_id: SubscriptionId,
        topic: Topic,
        limit: usize,
    ) -> Vec<ExecutorOutputEvent> {
        let message_log = self.message_log.read().await;
        let Some(log) = message_log.get(&topic) else {
            return Vec::new();
        };

        // Cursor位置を取得
        let cursor_id = self.get_cursor(&subscription_id, &topic).await;
        
        let mut result = Vec::new();
        let mut found_cursor = cursor_id.is_none();
        let mut last_message_id: Option<MessageId> = None;

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
                        }
                    }
                    continue;
                } else {
                    found_cursor = true;
                }
            }

            // Cursor以降のメッセージを取得
            if found_cursor {
                result.push(timestamped.event.clone());
                if let Some(msg_id) = message_id {
                    last_message_id = Some(msg_id);
                }
                if result.len() >= limit {
                    break;
                }
            }
        }

        // Cursorを更新（最後に取得したメッセージのID）
        if let Some(last_id) = last_message_id {
            self.update_cursor(subscription_id, topic, last_id).await;
        }

        result
    }
}
