use crate::backend::ProtocolBackend;
use crate::channels::{
    ExecutorInputEventSender, ExecutorOutputEventChannel, ShutdownSender, UserLogSender,
};
use crate::logging::{UserLogEvent, UserLogKind};
use crate::message_id::MessageId;
use crate::messages::{ExecutorInputEvent, ExecutorOutputEvent};
use crate::topic_subscription_registry::TopicSubscriptionRegistry;
use tokio_util::sync::CancellationToken;

use super::pod_id::PodId;

pub struct PodSpawner {
    pub pod_id: PodId,
    pub topic_manager: TopicSubscriptionRegistry,
    pub pod_name: String,
    pub userlog_sender: UserLogSender,
}

pub struct PodSpawnHandler {
    pub worker_handle: tokio::task::JoinHandle<()>,
    pub input_sender: ExecutorInputEventSender,
    pub shutdown_sender: ShutdownSender,
}

impl PodSpawner {
    pub fn new(
        pod_id: PodId,
        topic_manager: TopicSubscriptionRegistry,
        pod_name: String,
        userlog_sender: UserLogSender,
    ) -> Self {
        Self {
            task_id,
            topic_manager,
            pod_name,
            userlog_sender,
        }
    }

    pub async fn spawn(
        self,
        backend: ProtocolBackend,
        shutdown_token: CancellationToken,
        subscribe_topics: Option<Vec<String>>,
    ) -> Result<PodSpawnHandler, String> {
        let pod_id: PodId = self.pod_id.clone();
        let pod_name: String = self.pod_name.clone();
        let topic_manager: TopicSubscriptionRegistry = self.topic_manager;
        let userlog_sender = self.userlog_sender.clone();

        let mut backend_handle = backend
            .spawn(pod_id.clone())
            .await
            .map_err(|e| {
                log::error!("Failed to spawn pod backend for pod {}: {}", pod_id, e);
                format!("Failed to spawn backend for pod {}: {}", pod_id, e)
            })?;

        let input_sender_for_external = backend_handle.input_sender.clone();
        let shutdown_sender_for_external = backend_handle.shutdown_sender.clone();

        let worker_handle = tokio::task::spawn(async move {
            let topic_data_channel: ExecutorOutputEventChannel = ExecutorOutputEventChannel::new();
            let mut topic_data_receiver = topic_data_channel.receiver;

            // タスクが最初のメッセージを受信できる状態になるまで少し待機してから登録
            // これにより、タスクがwait_for_topicを呼び出す準備ができるまでメッセージ配信を遅延させる
            if let Some(topics) = subscribe_topics {
                log::info!(
                    "Waiting for pod {} to be ready before registering topic subscriptions: {:?}",
                    pod_id,
                    topics
                );
                // タスクの初期化を待つために少し待機
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                
                log::info!(
                    "Processing initial topic subscriptions for pod {}: {:?}",
                    pod_id,
                    topics
                );
                for topic in topics {
                    topic_manager
                        .add_subscriber(
                            topic.clone(),
                            topic_data_channel.sender.clone(),
                        )
                        .await;
                    log::info!(
                        "Added initial topic subscription for '{}' from pod {}",
                        topic,
                        pod_id
                    );
                }
            }

            loop {
                tokio::select! {
                    biased;

                    _ = shutdown_token.cancelled() => {
                        log::info!("Pod {} received shutdown signal", pod_id);
                        let _ = backend_handle.shutdown_sender.shutdown();
                        break;
                    },

                    event = backend_handle.event_receiver.recv() => {
                        match event {
                            Some(event) => {
                                let event: ExecutorOutputEvent = event;

                                match &event {
                                    ExecutorOutputEvent::TopicResponse {
                                        message_id,
                                        task_id: response_pod_id,
                                        topic,
                                        return_topic,
                                        data,
                                        ..
                                    } => {
                                        // TopicResponseとして扱う（backend側で既に変換済み）
                                        log::info!(
                                            "TopicResponse received from pod {} for topic '{}' (return topic '{}')",
                                            pod_id,
                                            topic,
                                            return_topic
                                        );

                                        // 配信時はExecutorOutputEvent::Topicに変換（PodSpawnerで実装）
                                        let event_to_route = ExecutorOutputEvent::Topic {
                                            message_id: message_id.clone(),
                                            task_id: response_pod_id.clone(),
                                            topic: return_topic.clone(),
                                            data: data.clone(),
                                        };
                                        match topic_manager.broadcast_message(event_to_route).await {
                                            Ok(success_count) => {
                                                log::info!(
                                                    "Broadcasted TopicResponse from pod {} to {} subscribers (return topic '{}')",
                                                    pod_id,
                                                    success_count,
                                                    return_topic
                                                );
                                                // 配信成功後にidleに戻す
                                                log::info!(
                                                    "TopicResponse received from pod {}, setting to idle",
                                                    pod_id
                                                );
                                                pod_manager.set_pod_idle(&pod_id).await;
                                            }
                                            Err(e) => {
                                                log::error!(
                                                    "Failed to broadcast TopicResponse from pod {} (return topic '{}'): {}",
                                                    pod_id,
                                                    return_topic,
                                                    e
                                                );
                                            }
                                        }
                                    }
                                    ExecutorOutputEvent::Topic { topic, data, .. } => {
                                        // 通常のトピックメッセージ（.resultで終わらない）
                                        log::info!(
                                            "Message event for pod {} on topic '{}': '{}'",
                                            pod_id,
                                            topic,
                                            data
                                        );

                                        // broadcast_messageを使用（状態管理はbroadcast_message内で行う）
                                        match topic_manager.broadcast_message(event.clone()).await {
                                            Ok(success_count) => {
                                                log::info!(
                                                    "Broadcasted message from pod {} to {} subscribers on topic '{}'",
                                                    pod_id,
                                                    success_count,
                                                    topic
                                                );
                                            }
                                            Err(e) => {
                                                log::error!(
                                                    "Failed to broadcast message from pod {} on topic '{}': {}",
                                                    pod_id,
                                                    topic,
                                                    e
                                                );
                                            }
                                        }
                                    },
                                    ExecutorOutputEvent::Stdout { data, .. } => {
                                        let flags = pod_manager.get_view_flags_by_pod_id(&pod_id).await;
                                        if let Some((view_stdout, _)) = flags {
                                            if view_stdout {
                                                let _ = userlog_sender.send(UserLogEvent { task_id: pod_id.to_string(), task_name: pod_name.clone(), kind: UserLogKind::Stdout, msg: data.clone() });
                                            }
                                        }
                                    },
                                    ExecutorOutputEvent::Stderr { data, .. } => {
                                        let flags = pod_manager.get_view_flags_by_pod_id(&pod_id).await;
                                        if let Some((_, view_stderr)) = flags {
                                            if view_stderr {
                                                let _ = userlog_sender.send(UserLogEvent { task_id: pod_id.to_string(), task_name: pod_name.clone(), kind: UserLogKind::Stderr, msg: data.clone() });
                                            }
                                        }
                                    },
                                    ExecutorOutputEvent::Error { error, .. } => {
                                        log::error!("Error event for pod {}: '{}'", pod_id, error);
                                    },
                                    ExecutorOutputEvent::Exit { exit_code, .. } => {
                                        log::info!("Exit event for pod {} with exit code: {}", pod_id, exit_code);

                                        let removed_topics: Vec<String> = topic_manager.remove_all_subscriptions_by_task(pod_id.clone()).await;
                                        log::info!("Pod {} exited, removed {} topic subscriptions", pod_id, removed_topics.len());

                                        if let Some(_removed_pod) = pod_manager.unregister_pod_by_pod_id(&pod_id).await {
                                            log::info!("Removed pod with PodId={} (Human name index updated)", pod_id);
                                        } else {
                                            log::warn!("Pod with PodId={} not found in manager during cleanup", pod_id);
                                        }
                                    }
                                }
                            },
                            None => {
                                log::info!("Pod backend event receiver closed for pod {}", pod_id);
                                break;
                            }
                        }
                    },

                    topic_data = topic_data_receiver.recv() => {
                        match topic_data {
                            Some(topic_data) => {
                                if let Some(data) = topic_data.data() {
                                    if let Some(topic_name) = topic_data.topic() {
                                        if let Err(e) = backend_handle.input_sender.send(
                                            ExecutorInputEvent::Topic {
                                                message_id: MessageId::new(),
                                                task_id: pod_id.clone(),
                                                topic: topic_name.clone(),
                                                data: data.clone(),
                                            },
                                        ) {
                                            log::warn!("Failed to send topic message to pod backend for pod {}: {}", pod_id, e);
                                        }
                                    } else {
                                        log::warn!("Topic data received without topic name for pod {}, skipping", pod_id);
                                        continue;
                                    }
                                }
                            },
                            None => {
                                log::info!("Topic data receiver closed for pod {}", pod_id);
                                break;
                            }
                        }
                    },
                }
            }

            log::info!("Pod {} completed", pod_id);
        });

        Ok(PodSpawnHandler {
            worker_handle,
            input_sender: input_sender_for_external,
            shutdown_sender: shutdown_sender_for_external,
        })
    }
}

