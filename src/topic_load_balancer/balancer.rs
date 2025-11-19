use crate::message_id::MessageId;
use crate::messages::ExecutorInputEvent;
use crate::task_id::TaskId;
use crate::pod::{PodManager, PodStateManager};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;

/// トピックキュー（FIFO）
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

/// TopicLoadBalancer（トピックメッセージのロードバランシングと配信）
#[derive(Clone)]
pub struct TopicLoadBalancer {
    pod_manager: PodManager,
    pod_state_manager: PodStateManager,
    topic_queue: TopicQueue,
    /// Pod名ごとのRound Robinインデックス
    round_robin_indices: Arc<RwLock<std::collections::HashMap<String, usize>>>,
}

impl TopicLoadBalancer {
    pub fn new(pod_manager: PodManager, pod_state_manager: PodStateManager) -> Self {
        Self {
            pod_manager,
            pod_state_manager,
            topic_queue: TopicQueue::new(),
            round_robin_indices: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Round Robin方式でIdleなPodインスタンスを選択
    /// すべてがBusyの場合はNoneを返す
    async fn select_idle_instance_round_robin(&self, pod_name: &str) -> Option<TaskId> {
        // すべてのインスタンスを取得（Round Robinの順序を維持するため）
        let all_instances = self.pod_state_manager.get_pod_instances(pod_name).await;

        if all_instances.is_empty() {
            return None;
        }

        let mut indices = self.round_robin_indices.write().await;
        let start_idx = *indices.get(pod_name).unwrap_or(&0);
        let mut checked = 0;
        let mut current_idx = start_idx;

        // 最大1周までチェック
        while checked < all_instances.len() {
            let pod_id = &all_instances[current_idx];
            
            // Idleかどうかを確認
            if let Some(state) = self.pod_state_manager.get_state(pod_id).await {
                if state == crate::pod::state::PodInstanceState::Idle {
                    // 次のインデックスを保存
                    let next_idx = (current_idx + 1) % all_instances.len();
                    indices.insert(pod_name.to_string(), next_idx);
                    log::debug!(
                        "Selected idle instance {} for pod '{}' (index {})",
                        pod_id,
                        pod_name,
                        current_idx
                    );
                    return Some(pod_id.clone());
                }
            }
            current_idx = (current_idx + 1) % all_instances.len();
            checked += 1;
        }

        // すべてBusy
        log::debug!(
            "All instances of pod '{}' are busy (checked {} instances)",
            pod_name,
            all_instances.len()
        );
        None
    }

    /// トピックメッセージをロードバランシング方式で配信
    /// すべてのインスタンスがBusyの場合はキューに追加
    pub async fn dispatch_message(
        &self,
        task_name: &str,
        topic: String,
        data: String,
    ) -> DispatchResult {
        // Idleなインスタンスを選択（Round Robin方式）
        if let Some(pod_id) = self.select_idle_instance_round_robin(task_name).await {
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

