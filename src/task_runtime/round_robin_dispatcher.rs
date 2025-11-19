use crate::message_id::MessageId;
use crate::messages::ExecutorInputEvent;
use crate::task_id::TaskId;
use crate::task_runtime::TaskExecutor;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;

/// トピックごとのキュー（FIFO）
#[derive(Clone)]
pub struct TopicQueue {
    queues: Arc<RwLock<std::collections::HashMap<String, VecDeque<QueuedMessage>>>>,
}

#[derive(Debug, Clone)]
struct QueuedMessage {
    topic: String,
    data: String,
    message_id: MessageId,
}

impl TopicQueue {
    pub fn new() -> Self {
        Self {
            queues: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// メッセージをキューに追加
    pub async fn enqueue(&self, topic: String, data: String, message_id: MessageId) {
        let mut queues = self.queues.write().await;
        queues
            .entry(topic.clone())
            .or_insert_with(VecDeque::new)
            .push_back(QueuedMessage {
                topic,
                data,
                message_id,
            });
    }

    /// キューからメッセージを取得（FIFO）
    pub async fn dequeue(&self, topic: &str) -> Option<(String, String, MessageId)> {
        let mut queues = self.queues.write().await;
        if let Some(queue) = queues.get_mut(topic) {
            if let Some(msg) = queue.pop_front() {
                if queue.is_empty() {
                    queues.remove(topic);
                }
                return Some((msg.topic, msg.data, msg.message_id));
            }
        }
        None
    }

    /// キューにメッセージが存在するか確認
    pub async fn has_messages(&self, topic: &str) -> bool {
        let queues = self.queues.read().await;
        queues.get(topic).map(|q| !q.is_empty()).unwrap_or(false)
    }

    /// キューのサイズを取得
    pub async fn queue_size(&self, topic: &str) -> usize {
        let queues = self.queues.read().await;
        queues.get(topic).map(|q| q.len()).unwrap_or(0)
    }
}

impl Default for TopicQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// Round Robin配信ディスパッチャー
#[derive(Clone)]
pub struct RoundRobinDispatcher {
    task_executor: TaskExecutor,
    topic_queue: TopicQueue,
}

impl RoundRobinDispatcher {
    pub fn new(task_executor: TaskExecutor) -> Self {
        Self {
            task_executor,
            topic_queue: TopicQueue::new(),
        }
    }

    /// メッセージをRound Robin方式で配信
    /// すべてのインスタンスがBusyの場合はキューに追加
    pub async fn dispatch_message(
        &self,
        task_name: &str,
        topic: String,
        data: String,
    ) -> DispatchResult {
        // Idleなインスタンスを選択
        if let Some(task_id) = self
            .task_executor
            .select_idle_instance_round_robin(task_name)
            .await
        {
            // タスクをBusyに設定
            self.task_executor.set_task_busy(&task_id).await;

            // メッセージを送信
            if let Some(input_sender) = self
                .task_executor
                .get_input_sender_by_task_id(&task_id)
                .await
            {
                let message_id = MessageId::new();
                match input_sender.send(ExecutorInputEvent::Topic {
                    message_id: message_id.clone(),
                    task_id: task_id.clone(),
                    topic: topic.clone(),
                    data: data.clone(),
                }) {
                    Ok(_) => {
                        log::info!(
                            "Dispatched message to task {} (instance of '{}') on topic '{}'",
                            task_id,
                            task_name,
                            topic
                        );
                        return DispatchResult::Dispatched { task_id };
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to send message to task {}: {}, setting to idle",
                            task_id,
                            e
                        );
                        // 送信失敗時はidleに戻す
                        self.task_executor.set_task_idle(&task_id).await;
                    }
                }
            } else {
                log::warn!(
                    "Input sender not found for task {}, setting to idle",
                    task_id
                );
                self.task_executor.set_task_idle(&task_id).await;
            }
        }

        // すべてBusyまたは送信失敗時はキューに追加
        let message_id = MessageId::new();
        self.topic_queue
            .enqueue(topic.clone(), data.clone(), message_id.clone())
            .await;
        let queue_size = self.topic_queue.queue_size(&topic).await;
        log::debug!(
            "All instances of task '{}' are busy, queued message for topic '{}' (queue size: {})",
            task_name,
            topic,
            queue_size
        );
        DispatchResult::Queued {
            message_id,
            queue_size,
        }
    }

    /// キューに溜まったメッセージを処理（Idleなインスタンスが利用可能になった時に呼び出す）
    pub async fn process_queue(&self, task_name: &str) -> usize {
        let mut processed = 0;

        // タスク名に属するすべてのインスタンスを取得（将来の拡張用）
        let name_to_ids = self.task_executor.task_state_manager().name_to_ids();
        let _instances = {
            let name_to_id = name_to_ids.read().await;
            name_to_id
                .get(task_name)
                .map(|ids| ids.clone())
                .unwrap_or_default()
        };

        // 各インスタンスが購読しているトピックを確認
        // 簡略化のため、タスク名をトピック名として使用（実際の実装では設定から取得）
        let topics = vec![task_name.to_string()];

        for topic in topics {
            while self.topic_queue.has_messages(&topic).await {
                if let Some(task_id) = self
                    .task_executor
                    .select_idle_instance_round_robin(task_name)
                    .await
                {
                    if let Some((queued_topic, queued_data, _message_id)) =
                        self.topic_queue.dequeue(&topic).await
                    {
                        // タスクをBusyに設定
                        self.task_executor.set_task_busy(&task_id).await;

                        // メッセージを送信
                        if let Some(input_sender) = self
                            .task_executor
                            .get_input_sender_by_task_id(&task_id)
                            .await
                        {
                            let message_id = MessageId::new();
                            match input_sender.send(ExecutorInputEvent::Topic {
                                message_id,
                                task_id: task_id.clone(),
                                topic: queued_topic.clone(),
                                data: queued_data.clone(),
                            }) {
                                Ok(_) => {
                                    log::info!(
                                        "Processed queued message for task {} (instance of '{}') on topic '{}'",
                                        task_id,
                                        task_name,
                                        queued_topic
                                    );
                                    processed += 1;
                                }
                                Err(e) => {
                                    log::error!(
                                        "Failed to send queued message to task {}: {}, setting to idle",
                                        task_id,
                                        e
                                    );
                                    // 送信失敗時はidleに戻し、メッセージをキューに戻す
                                    self.task_executor.set_task_idle(&task_id).await;
                                    self.topic_queue
                                        .enqueue(queued_topic, queued_data, MessageId::new())
                                        .await;
                                    break;
                                }
                            }
                        } else {
                            log::warn!(
                                "Input sender not found for task {}, setting to idle",
                                task_id
                            );
                            self.task_executor.set_task_idle(&task_id).await;
                            // メッセージをキューに戻す
                            self.topic_queue
                                .enqueue(queued_topic, queued_data, MessageId::new())
                                .await;
                            break;
                        }
                    }
                } else {
                    // すべてBusyなので処理を中断
                    break;
                }
            }
        }

        if processed > 0 {
            log::info!(
                "Processed {} queued messages for task '{}'",
                processed,
                task_name
            );
        }
        processed
    }

    /// トピックキューへのアクセス
    pub fn topic_queue(&self) -> &TopicQueue {
        &self.topic_queue
    }
}

#[derive(Debug, Clone)]
pub enum DispatchResult {
    /// メッセージが正常に配信された
    Dispatched { task_id: TaskId },
    /// メッセージがキューに追加された
    Queued {
        message_id: MessageId,
        queue_size: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_topic_queue() {
        let queue = TopicQueue::new();
        let message_id = MessageId::new();

        queue
            .enqueue(
                "test.topic".to_string(),
                "data1".to_string(),
                message_id.clone(),
            )
            .await;
        queue
            .enqueue(
                "test.topic".to_string(),
                "data2".to_string(),
                MessageId::new(),
            )
            .await;

        assert_eq!(queue.queue_size("test.topic").await, 2);

        let (topic, data, _) = queue.dequeue("test.topic").await.unwrap();
        assert_eq!(topic, "test.topic");
        assert_eq!(data, "data1");

        let (topic, data, _) = queue.dequeue("test.topic").await.unwrap();
        assert_eq!(topic, "test.topic");
        assert_eq!(data, "data2");

        assert!(queue.dequeue("test.topic").await.is_none());
    }
}
