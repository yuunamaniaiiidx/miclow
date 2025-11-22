use crate::channels::ExecutorOutputEventSender;
use crate::message_id::MessageId;
use crate::messages::ExecutorOutputEvent;
use crate::pod::PodId;
use crate::replicaset::ReplicaSetId;
use crate::topic::Topic;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// システムデータを生成する関数の型
pub type SystemDataProvider = Arc<dyn Fn(&Topic) -> Option<String> + Send + Sync>;

#[derive(Clone)]
pub struct TopicSubscriptionRegistry {
    topic_senders: Arc<RwLock<HashMap<Topic, Vec<ExecutorOutputEventSender>>>>,
    latest_messages: Arc<RwLock<HashMap<Topic, ExecutorOutputEvent>>>,
    system_data_provider: Option<SystemDataProvider>,
}

impl TopicSubscriptionRegistry {
    pub fn new() -> Self {
        Self {
            topic_senders: Arc::new(RwLock::new(HashMap::new())),
            latest_messages: Arc::new(RwLock::new(HashMap::new())),
            system_data_provider: None,
        }
    }

    /// システムデータプロバイダーを設定（オプション）
    /// 設定されていない場合はデフォルトの実装を使用
    pub fn with_system_data_provider(mut self, provider: SystemDataProvider) -> Self {
        self.system_data_provider = Some(provider);
        self
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
        if topic_owned.as_str() == "system.pull" {
            return self.handle_pull_command(event).await;
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
        let senders = {
            let topics = self.topic_senders.read().await;
            topics.get(&Topic::from("system.pull")).cloned()
        };

        if let Some(senders) = senders {
            for sender in senders.iter() {
                let _ = sender.send(response_event.clone());
            }
        }

        Ok(())
    }

    pub async fn get_latest_message(&self, topic: impl Into<Topic>) -> Option<ExecutorOutputEvent> {
        let topic = topic.into();
        let latest_messages = self.latest_messages.read().await;
        
        // まず通常のメッセージをチェック
        if let Some(event) = latest_messages.get(&topic).cloned() {
            drop(latest_messages);
            
            // 特定のシステムトピックの場合、データがあればシステムデータを返す
            if self.is_system_topic(&topic) {
                if let Some(system_response) = self.generate_system_response(&topic, &event) {
                    return Some(system_response);
                }
            }
            
            return Some(event);
        }
        
        None
    }

    /// システムトピックかどうかを判定
    /// `system.*` プレフィックスで判定
    fn is_system_topic(&self, topic: &Topic) -> bool {
        topic.as_str().starts_with("system.")
    }

    /// システムトピックの場合、システムデータを生成して返す
    /// 汎用的に使えるよう、元のイベント情報を活用
    fn generate_system_response(
        &self,
        topic: &Topic,
        original_event: &ExecutorOutputEvent,
    ) -> Option<ExecutorOutputEvent> {
        // 元のイベントから情報を取得
        let from_replicaset_id = original_event.from_replicaset_id()?.clone();
        // Topicイベントであることを確認
        match original_event {
            ExecutorOutputEvent::Topic { .. } => {}
            _ => return None,
        };

        // システムデータを生成
        let system_data = if let Some(provider) = &self.system_data_provider {
            // カスタムプロバイダーが設定されている場合はそれを使用
            provider(topic)?
        } else {
            // デフォルトの実装
            self.get_default_system_data(topic)?
        };

        // レスポンストピックでシステムデータを返す
        let response_topic = topic.result();
        Some(ExecutorOutputEvent::Topic {
            message_id: MessageId::new(),
            pod_id: PodId::new(), // システム用の仮想Pod ID
            from_replicaset_id: ReplicaSetId::new(), // システム用の仮想ReplicaSet ID
            to_replicaset_id: Some(from_replicaset_id),
            topic: response_topic,
            data: Arc::from(system_data),
        })
    }

    /// デフォルトのシステムデータを取得
    fn get_default_system_data(&self, topic: &Topic) -> Option<String> {
        match topic.as_str() {
            "system.status" => Some(r#"{"status": "running"}"#.to_string()),
            "system.config" => Some(r#"{"version": "0.1.0"}"#.to_string()),
            _ => None,
        }
    }
}
