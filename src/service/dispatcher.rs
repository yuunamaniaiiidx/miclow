use crate::message_id::MessageId;
use crate::messages::ExecutorInputEvent;
use crate::task_id::TaskId;
use crate::pod::{PodManager, PodStateManager};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;

/// メッセージキュー（FIFO）
#[derive(Clone)]
pub struct MessageQueue {
    queues: Arc<RwLock<std::collections::HashMap<String, VecDeque<QueuedMessage>>>>,
}

#[derive(Debug, Clone)]
struct QueuedMessage {
    topic: String,
    data: String,
    message_id: MessageId,
}

impl MessageQueue {
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

impl Default for MessageQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// Service（ロードバランシングとメッセージ配信）
#[derive(Clone)]
pub struct Service {
    pod_manager: PodManager,
    pod_state_manager: PodStateManager,
    message_queue: MessageQueue,
}

impl Service {
    pub fn new(pod_manager: PodManager, pod_state_manager: PodStateManager) -> Self {
        Self {
            pod_manager,
            pod_state_manager,
            message_queue: MessageQueue::new(),
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
        if let Some(pod_id) = self
            .pod_state_manager
            .select_idle_instance_round_robin(task_name)
            .await
        {
            // PodをBusyに設定
            self.pod_state_manager.set_busy(&pod_id).await;

            // メッセージを送信
            if let Some(input_sender) = self
                .pod_manager
                .get_input_sender_by_pod_id(&pod_id)
                .await
            {
                let message_id = MessageId::new();
                match input_sender.send(ExecutorInputEvent::Topic {
                    message_id: message_id.clone(),
                    task_id: pod_id.clone(),
                    topic: topic.clone(),
                    data: data.clone(),
                }) {
                    Ok(_) => {
                        log::info!(
                            "Dispatched message to pod {} (instance of '{}') on topic '{}'",
                            pod_id,
                            task_name,
                            topic
                        );
                        return DispatchResult::Dispatched { task_id: pod_id };
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to send message to pod {}: {}, setting to idle",
                            pod_id,
                            e
                        );
                        // 送信失敗時はidleに戻す
                        self.pod_state_manager.set_idle(&pod_id).await;
                    }
                }
            } else {
                log::warn!(
                    "Input sender not found for pod {}, setting to idle",
                    pod_id
                );
                self.pod_state_manager.set_idle(&pod_id).await;
            }
        }

        // すべてBusyまたは送信失敗時はキューに追加
        let message_id = MessageId::new();
        self.message_queue
            .enqueue(topic.clone(), data.clone(), message_id.clone())
            .await;
        let queue_size = self.message_queue.queue_size(&topic).await;
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

    /// メッセージキューへのアクセス
    pub fn message_queue(&self) -> &MessageQueue {
        &self.message_queue
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
    async fn test_message_queue() {
        let queue = MessageQueue::new();
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

