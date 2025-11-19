use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::backend::ProtocolBackend;
use crate::channels::TaskExitSender;
use crate::task_id::TaskId;

use super::running::RunningPod;
use super::spawner::PodSpawner;
use super::context::PodStartContext;
use super::state::PodStateManager;

#[derive(Clone)]
pub struct PodManager {
    running_pods: Arc<RwLock<HashMap<TaskId, RunningPod>>>,
    name_to_id: Arc<RwLock<HashMap<String, Vec<TaskId>>>>,
    id_to_name: Arc<RwLock<HashMap<TaskId, String>>>,
    monitored_pod_names: Arc<RwLock<HashSet<String>>>,
    pod_state_manager: PodStateManager,
    shutdown_token: CancellationToken,
    /// Pod終了通知チャネル（Pod名を送信）
    pod_exit_sender: Arc<RwLock<Option<TaskExitSender>>>,
}

impl PodManager {
    pub fn new(shutdown_token: CancellationToken) -> Self {
        Self {
            running_pods: Arc::new(RwLock::new(HashMap::new())),
            name_to_id: Arc::new(RwLock::new(HashMap::new())),
            id_to_name: Arc::new(RwLock::new(HashMap::new())),
            monitored_pod_names: Arc::new(RwLock::new(HashSet::new())),
            pod_state_manager: PodStateManager::new(),
            shutdown_token,
            pod_exit_sender: Arc::new(RwLock::new(None)),
        }
    }

    /// Pod終了通知チャネルの送信側を設定
    pub async fn set_pod_exit_sender(&self, sender: TaskExitSender) {
        let mut exit_sender = self.pod_exit_sender.write().await;
        *exit_sender = Some(sender);
    }

    pub async fn register_pod(&self, pod_name: String, pod: RunningPod) -> Result<(), String> {
        let mut running_pods = self.running_pods.write().await;
        let mut name_to_id = self.name_to_id.write().await;
        let mut id_to_name = self.id_to_name.write().await;
        let pod_id = pod.task_id.clone();

        running_pods.insert(pod_id.clone(), pod);
        name_to_id
            .entry(pod_name.clone())
            .or_insert_with(Vec::new)
            .push(pod_id.clone());
        id_to_name.insert(pod_id.clone(), pod_name.clone());

        // 状態管理に登録
        self.pod_state_manager
            .register_pod(pod_name.clone(), pod_id)
            .await;

        if !pod_name.starts_with("system.") {
            let mut monitored = self.monitored_pod_names.write().await;
            monitored.insert(pod_name);
        }

        Ok(())
    }

    pub async fn try_register_pod(
        &self,
        pod_name: String,
        pod: RunningPod,
    ) -> Result<(), String> {
        let name_to_id = self.name_to_id.read().await;
        if let Some(ids) = name_to_id.get(&pod_name) {
            if !ids.is_empty() {
                log::warn!(
                    "Pod '{}' is already running - cancelling new pod start",
                    pod_name
                );
                return Err(format!("Pod '{}' is already running", pod_name));
            }
        }
        drop(name_to_id);

        self.register_pod(pod_name, pod).await
    }

    pub async fn unregister_pod_by_pod_id(&self, pod_id: &TaskId) -> Option<RunningPod> {
        let mut running_pods = self.running_pods.write().await;
        let mut name_to_id = self.name_to_id.write().await;
        let mut id_to_name = self.id_to_name.write().await;
        if let Some(pod) = running_pods.remove(pod_id) {
            let pod_name = id_to_name.get(pod_id).cloned();

            for ids in name_to_id.values_mut() {
                ids.retain(|id| id != pod_id);
            }
            name_to_id.retain(|_name, ids| !ids.is_empty());
            id_to_name.remove(pod_id);

            // 状態管理からも削除
            self.pod_state_manager.unregister_pod(pod_id).await;

            if let Some(name) = pod_name.clone() {
                // Pod終了通知を送信
                let exit_sender = self.pod_exit_sender.read().await;
                if let Some(sender) = exit_sender.as_ref() {
                    let _ = sender.send(name.clone());
                }

                let mut monitored = self.monitored_pod_names.write().await;
                if monitored.remove(&name) {
                    if monitored.is_empty() {
                        drop(monitored);
                        log::info!("All monitored pods finished, requesting shutdown...");
                        self.shutdown_token.cancel();
                    }
                }
            }

            Some(pod)
        } else {
            None
        }
    }

    pub async fn unregister_pod_by_name(&self, pod_name: &str) -> Option<RunningPod> {
        let id = {
            let name_to_id = self.name_to_id.read().await;
            name_to_id
                .get(pod_name)
                .and_then(|ids| ids.last())
                .cloned()
        };
        if let Some(pid) = id {
            self.unregister_pod_by_pod_id(&pid).await
        } else {
            None
        }
    }

    pub async fn stop_pod_by_pod_id(&self, pod_id: &TaskId) -> Result<()> {
        let running_pod = {
            let mut running_pods = self.running_pods.write().await;
            running_pods.remove(pod_id)
        };
        match running_pod {
            Some(pod) => {
                log::info!(
                    "Stopped pod with PodId={} (Human name index updated)",
                    pod_id
                );
                let mut name_to_id = self.name_to_id.write().await;
                let mut id_to_name = self.id_to_name.write().await;
                for ids in name_to_id.values_mut() {
                    ids.retain(|id| id != pod_id);
                }
                name_to_id.retain(|_name, ids| !ids.is_empty());
                id_to_name.remove(pod_id);
                pod.task_handle.abort();
                Ok(())
            }
            None => {
                log::warn!("Attempted to stop non-running pod PodId={}", pod_id);
                Err(anyhow::anyhow!("PodId '{}' is not running", pod_id))
            }
        }
    }

    pub async fn get_running_pods_info(&self) -> Vec<(String, TaskId)> {
        let name_to_id = self.name_to_id.read().await;
        name_to_id
            .iter()
            .filter_map(|(name, ids)| ids.last().map(|pod_id| (name.clone(), pod_id.clone())))
            .collect()
    }

    pub async fn get_view_flags_by_pod_id(&self, pod_id: &TaskId) -> Option<(bool, bool)> {
        let running_pods_guard = self.running_pods.read().await;
        running_pods_guard
            .get(pod_id)
            .map(|p| (p.view_stdout, p.view_stderr))
    }

    pub async fn get_pod_name_by_id(&self, pod_id: &TaskId) -> Option<String> {
        let id_to_name = self.id_to_name.read().await;
        id_to_name.get(pod_id).cloned()
    }

    pub async fn get_input_sender_by_pod_id(
        &self,
        pod_id: &TaskId,
    ) -> Option<crate::channels::ExecutorInputEventSender> {
        let running_pods = self.running_pods.read().await;
        running_pods
            .get(pod_id)
            .map(|pod| pod.input_sender.clone())
    }

    pub async fn graceful_shutdown_all(&self, timeout: std::time::Duration) {
        let pods: Vec<(TaskId, RunningPod)> = {
            let mut running_pods = self.running_pods.write().await;
            let drained: Vec<(TaskId, RunningPod)> = running_pods.drain().collect();
            drained
        };
        {
            let mut name_to_id = self.name_to_id.write().await;
            let mut id_to_name = self.id_to_name.write().await;
            name_to_id.clear();
            id_to_name.clear();
        }

        for (_pid, pod) in &pods {
            let _ = pod.shutdown_sender.shutdown();
        }

        for (_pid, pod) in pods {
            let _ = tokio::time::timeout(timeout, pod.task_handle).await;
        }
    }

    pub async fn start_pod_from_config(&self, context: PodStartContext) -> Result<()> {
        let pod_config = &context.task_config;
        log::info!("Starting pod '{}'", pod_config.name);

        let pod_id_new = TaskId::new();

        let backend: ProtocolBackend = pod_config.protocol_backend.clone();

        if let ProtocolBackend::MiclowStdIO(ref config) = backend {
            if !Path::new(&config.command).exists() && !which::which(&config.command).is_ok() {
                return Err(anyhow::anyhow!(
                    "Command '{}' not found in PATH or file system",
                    config.command
                ));
            }
        }

        let subscribe_topics = pod_config.subscribe_topics.clone();

        let pod_spawner = PodSpawner::new(
            pod_id_new.clone(),
            context.topic_manager,
            self.clone(),
            pod_config.name.clone(),
            context.userlog_sender,
        );

        let spawn_result = pod_spawner
            .spawn_backend(backend.clone(), context.shutdown_token, subscribe_topics)
            .await;

        let view_stdout = pod_config.view_stdout;
        let view_stderr = pod_config.view_stderr;

        let running_pod = RunningPod {
            task_id: pod_id_new.clone(),
            shutdown_sender: spawn_result.shutdown_sender.clone(),
            input_sender: spawn_result.input_sender.clone(),
            task_handle: spawn_result.worker_handle,
            view_stdout,
            view_stderr,
        };

        if pod_config.allow_duplicate {
            if let Err(e) = self
                .register_pod(pod_config.name.clone(), running_pod)
                .await
            {
                return Err(anyhow::anyhow!(
                    "Failed to register pod '{}': {}",
                    pod_config.name,
                    e
                ));
            }
            log::debug!(
                "Registered pod '{}' (ID: {}) - duplicate instances allowed",
                pod_config.name,
                pod_id_new
            );
        } else {
            if let Err(e) = self
                .try_register_pod(pod_config.name.clone(), running_pod)
                .await
            {
                return Err(anyhow::anyhow!(
                    "Failed to register pod '{}': {}",
                    pod_config.name,
                    e
                ));
            }
            log::debug!(
                "Registered pod '{}' (ID: {}) - duplicate instances not allowed",
                pod_config.name,
                pod_id_new
            );
        }

        log::info!(
            "Successfully started pod '{}' (ID: {})",
            pod_config.name,
            pod_id_new
        );
        Ok(())
    }

    /// Pod状態管理へのアクセス
    pub fn pod_state_manager(&self) -> &PodStateManager {
        &self.pod_state_manager
    }

    /// PodをIdleに戻す（TopicResponse受信時などに使用）
    /// 
    /// このメソッドは、Podの状態管理を PodManager の責務として提供する。
    /// Service などの上位レイヤーの責務ではなく、Podの基本的な状態管理として扱う。
    pub async fn set_pod_idle(&self, pod_id: &TaskId) {
        self.pod_state_manager.set_idle(pod_id).await;
    }
}

