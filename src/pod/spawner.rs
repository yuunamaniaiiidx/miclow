use crate::backend::{ProtocolBackend, TaskBackend};
use crate::channels::{
    ExecutorInputEventSender, PodEventSender, ReplicaSetTopicReceiver, ShutdownSender,
    UserLogSender,
};
use crate::logging::{UserLogEvent, UserLogKind};
use crate::message_id::MessageId;
use crate::messages::{ExecutorInputEvent, ExecutorOutputEvent, PodEvent};
use crate::replicaset::ReplicaSetId;
use crate::topic::TopicSubscriptionRegistry;
use tokio_util::sync::CancellationToken;

use super::pod_id::PodId;
use super::state::PodState;

pub struct PodSpawnHandler {
    pub pod_id: PodId,
    pub worker_handle: tokio::task::JoinHandle<()>,
    pub input_sender: ExecutorInputEventSender,
    pub shutdown_sender: ShutdownSender,
}

pub struct PodSpawner {
    pub pod_id: PodId,
    pub replicaset_id: ReplicaSetId,
    pub topic_manager: TopicSubscriptionRegistry,
    pub pod_name: String,
    pub userlog_sender: UserLogSender,
    pub state: PodState,
    pub pod_event_sender: PodEventSender,
    pub view_stdout: bool,
    pub view_stderr: bool,
}

impl PodSpawner {
    pub fn new(
        pod_id: PodId,
        replicaset_id: ReplicaSetId,
        topic_manager: TopicSubscriptionRegistry,
        pod_name: String,
        userlog_sender: UserLogSender,
        pod_event_sender: PodEventSender,
        view_stdout: bool,
        view_stderr: bool,
    ) -> Self {
        Self {
            pod_id,
            replicaset_id,
            topic_manager,
            pod_name,
            userlog_sender,
            state: PodState::default(),
            pod_event_sender,
            view_stdout,
            view_stderr,
        }
    }

    pub fn update_state(&mut self, next_state: PodState) {
        self.state = next_state;
    }

    pub async fn spawn(
        self,
        backend: ProtocolBackend,
        shutdown_token: CancellationToken,
        topic_data_receiver: ReplicaSetTopicReceiver,
    ) -> Result<PodSpawnHandler, String> {
        let pod_id: PodId = self.pod_id.clone();
        let handler_pod_id = pod_id.clone();
        let pod_name: String = self.pod_name.clone();
        let topic_manager: TopicSubscriptionRegistry = self.topic_manager;
        let userlog_sender = self.userlog_sender.clone();
        let pod_event_sender = self.pod_event_sender.clone();
        let view_stdout = self.view_stdout;
        let view_stderr = self.view_stderr;

        let mut backend_handle = backend.spawn(pod_id.clone()).await.map_err(|e| {
            log::error!("Failed to spawn pod backend for pod {}: {}", pod_id, e);
            format!("Failed to spawn backend for pod {}: {}", pod_id, e)
        })?;

        let input_sender_for_external = backend_handle.input_sender.clone();
        let shutdown_sender_for_external = backend_handle.shutdown_sender.clone();

        let worker_handle = tokio::task::spawn({
            let pod_id = pod_id.clone();
            let pod_name = pod_name.clone();
            let topic_manager = topic_manager.clone();
            let userlog_sender = userlog_sender.clone();
            let pod_event_sender = pod_event_sender.clone();
            let mut topic_data_receiver = topic_data_receiver;

            async move {
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
                                                Ok(_) => {
                                                    log::info!(
                                                        "Broadcasted TopicResponse from pod {} (return topic '{}')",
                                                        pod_id,
                                                        return_topic
                                                    );
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

                                            if let Err(e) = pod_event_sender.send(PodEvent::PodResponse {
                                                pod_id: pod_id.clone(),
                                                message_id: message_id.clone(),
                                            }) {
                                                log::warn!(
                                                    "Failed to send PodResponse event for '{}': {}",
                                                    pod_id,
                                                    e
                                                );
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
                                                Ok(_) => {
                                                    log::info!(
                                                        "Broadcasted message from pod {} on topic '{}'",
                                                        pod_id,
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
                                            if view_stdout {
                                                let _ = userlog_sender.send(UserLogEvent { task_id: pod_id.to_string(), task_name: pod_name.clone(), kind: UserLogKind::Stdout, msg: data.clone() });
                                            }
                                        },
                                        ExecutorOutputEvent::Stderr { data, .. } => {
                                            if view_stderr {
                                                let _ = userlog_sender.send(UserLogEvent { task_id: pod_id.to_string(), task_name: pod_name.clone(), kind: UserLogKind::Stderr, msg: data.clone() });
                                            }
                                        },
                                        ExecutorOutputEvent::Error { error, .. } => {
                                            log::error!("Error event for pod {}: '{}'", pod_id, error);
                                        },
                                        ExecutorOutputEvent::Exit { exit_code, .. } => {
                                            log::info!("Exit event for pod {} with exit code: {}", pod_id, exit_code);
                                            if let Err(e) = pod_event_sender.send(PodEvent::PodExit {
                                                pod_id: pod_id.clone(),
                                            }) {
                                                log::warn!(
                                                    "Failed to notify pod exit for '{}': {}",
                                                    pod_id,
                                                    e
                                                );
                                            }
                                            break;
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
                                    if let Err(e) = backend_handle.input_sender.send(
                                        ExecutorInputEvent::Topic {
                                            message_id: MessageId::new(),
                                            task_id: pod_id.clone(),
                                            topic: topic_data.topic,
                                            data: topic_data.data,
                                        },
                                    ) {
                                        log::warn!("Failed to send topic message to pod backend for pod {}: {}", pod_id, e);
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
            }
        });

        Ok(PodSpawnHandler {
            pod_id: handler_pod_id,
            worker_handle,
            input_sender: input_sender_for_external,
            shutdown_sender: shutdown_sender_for_external,
        })
    }
}
