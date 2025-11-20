use std::collections::HashMap;
use std::time::Duration;

use crate::channels::{TaskExitChannel, TaskExitSender};
use crate::pod::{PodId, PodSpawnHandler, PodSpawner};
use crate::replicaset::context::{PodStartContext, ReplicaSetSpec};
use crate::replicaset::ReplicaSetId;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub struct ReplicaSetController {
    shutdown_token: CancellationToken,
    replica_sets: HashMap<ReplicaSetId, ReplicaSetHandle>,
}

struct ReplicaSetHandle {
    join_handle: JoinHandle<()>,
    shutdown_token: CancellationToken,
}

impl ReplicaSetController {
    pub fn new(shutdown_token: CancellationToken) -> Self {
        Self {
            shutdown_token,
            replica_sets: HashMap::new(),
        }
    }

    pub fn create_replicaset(&mut self, spec: ReplicaSetSpec) -> ReplicaSetId {
        let desired_instances = spec.desired_instances.max(1);
        let replicaset_id = ReplicaSetId::new();
        let replica_token = self.shutdown_token.child_token();

        let worker = ReplicaSetWorker::new(
            replicaset_id.clone(),
            spec.task_name,
            desired_instances,
            spec.start_context,
            replica_token.clone(),
        );

        let join_handle = tokio::spawn(worker.run());

        self.replica_sets.insert(
            replicaset_id.clone(),
            ReplicaSetHandle {
                join_handle,
                shutdown_token: replica_token,
            },
        );

        replicaset_id
    }

    pub async fn shutdown_all(mut self) {
        for (_id, handle) in self.replica_sets.drain() {
            handle.shutdown_token.cancel();
            let _ = handle.join_handle.await;
        }
    }
}

struct ReplicaSetWorker {
    replicaset_id: ReplicaSetId,
    task_name: String,
    desired_instances: u32,
    start_context: PodStartContext,
    shutdown_token: CancellationToken,
}

impl ReplicaSetWorker {
    fn new(
        replicaset_id: ReplicaSetId,
        task_name: String,
        desired_instances: u32,
        start_context: PodStartContext,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            replicaset_id,
            task_name,
            desired_instances,
            start_context,
            shutdown_token,
        }
    }

    async fn run(self) {
        let ReplicaSetWorker {
            replicaset_id,
            task_name,
            desired_instances,
            start_context,
            shutdown_token,
        } = self;

        let TaskExitChannel {
            sender: exit_sender,
            receiver: mut exit_receiver,
        } = TaskExitChannel::new();

        let mut pods: HashMap<PodId, PodSpawnHandler> = HashMap::new();
        let target_instances = desired_instances;

        loop {
            while pods.len() < target_instances as usize {
                if shutdown_token.is_cancelled() {
                    log::info!(
                        "ReplicaSet {} received shutdown signal before spawning all pods",
                        replicaset_id
                    );
                    return Self::shutdown_pods(pods).await;
                }

                match Self::spawn_pod(
                    replicaset_id.clone(),
                    &task_name,
                    &start_context,
                    exit_sender.clone(),
                    shutdown_token.clone(),
                )
                .await
                {
                    Ok(handle) => {
                        let handle_id = handle.pod_id.clone();
                        log::info!("ReplicaSet {} spawned pod {}", replicaset_id, handle_id);
                        pods.insert(handle_id, handle);
                    }
                    Err(err) => {
                        log::error!(
                            "ReplicaSet {} failed to spawn pod for task '{}': {}",
                            replicaset_id,
                            task_name,
                            err
                        );

                        tokio::select! {
                            _ = shutdown_token.cancelled() => {
                                log::info!(
                                    "ReplicaSet {} shutting down while retrying pod spawn",
                                    replicaset_id
                                );
                                return Self::shutdown_pods(pods).await;
                            }
                            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                        }
                    }
                }
            }

            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    log::info!(
                        "ReplicaSet {} received shutdown signal",
                        replicaset_id
                    );
                    return Self::shutdown_pods(pods).await;
                }
                exit_event = exit_receiver.recv() => {
                    match exit_event {
                        Some(pod_id) => {
                            if let Some(handle) = pods.remove(&pod_id) {
                                log::info!(
                                    "ReplicaSet {} detected exit of pod {}, awaiting completion",
                                    replicaset_id,
                                    pod_id
                                );
                                handle.worker_handle.await.ok();
                            } else {
                                log::warn!(
                                    "ReplicaSet {} received exit notice for unknown pod {}",
                                    replicaset_id,
                                    pod_id
                                );
                            }
                        }
                        None => {
                            log::warn!(
                                "ReplicaSet {} exit receiver closed unexpectedly",
                                replicaset_id
                            );
                            return Self::shutdown_pods(pods).await;
                        }
                    }
                }
            }
        }
    }

    async fn shutdown_pods(mut pods: HashMap<PodId, PodSpawnHandler>) {
        for (_id, handle) in pods.drain() {
            handle.shutdown_sender.shutdown().ok();
            handle.worker_handle.await.ok();
        }
    }

    async fn spawn_pod(
        replicaset_id: ReplicaSetId,
        task_name: &str,
        start_context: &PodStartContext,
        exit_sender: TaskExitSender,
        shutdown_token: CancellationToken,
    ) -> Result<PodSpawnHandler, String> {
        let pod_id = PodId::new();
        let instance_name = format!("{}-{}", task_name, short_pod_suffix(&pod_id));

        let spawner = PodSpawner::new(
            pod_id.clone(),
            replicaset_id,
            start_context.topic_manager.clone(),
            instance_name.clone(),
            start_context.userlog_sender.clone(),
            exit_sender,
            start_context.view_stdout,
            start_context.view_stderr,
        );

        spawner
            .spawn(
                start_context.protocol_backend.clone(),
                shutdown_token,
                start_context.subscribe_topics.clone(),
            )
            .await
    }
}

fn short_pod_suffix(pod_id: &PodId) -> String {
    let id_string = pod_id.to_string();
    id_string.chars().take(8).collect::<String>()
}
