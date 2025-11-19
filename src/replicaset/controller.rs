use crate::background_worker_registry::{
    BackgroundWorker, BackgroundWorkerContext, WorkerReadiness,
};
use crate::channels::{TaskExitReceiver, UserLogSender};
use crate::config::{SystemConfig, TaskConfig};
use crate::pod::{PodStartContext, PodManager};
use crate::topic_subscription_registry::TopicSubscriptionRegistry;
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

/// ReplicaSet コントローラー
/// 各タスクの desired_instances を監視し、不足時にインスタンスを起動・補充する
pub struct ReplicaSetController {
    /// タスク名 -> TaskConfig のマッピング（ReplicaSet管理対象のタスクのみ）
    managed_tasks: Arc<RwLock<HashMap<String, TaskConfig>>>,
    /// PodManager への参照
    pod_manager: PodManager,
    /// Pod終了通知の受信側
    pod_exit_receiver: TaskExitReceiver,
    /// TopicSubscriptionRegistry（PodStartContext作成用）
    topic_manager: TopicSubscriptionRegistry,
    /// シャットダウントークン（PodStartContext作成用）
    shutdown_token: CancellationToken,
    /// ユーザーログ送信側（PodStartContext作成用）
    userlog_sender: UserLogSender,
}

impl ReplicaSetController {
    pub fn new(
        pod_manager: PodManager,
        pod_exit_receiver: TaskExitReceiver,
        topic_manager: TopicSubscriptionRegistry,
        shutdown_token: CancellationToken,
        userlog_sender: UserLogSender,
    ) -> Self {
        Self {
            managed_tasks: Arc::new(RwLock::new(HashMap::new())),
            pod_manager,
            pod_exit_receiver,
            topic_manager,
            shutdown_token,
            userlog_sender,
        }
    }

    /// 管理対象のタスクを登録
    pub async fn register_task(&self, task_config: TaskConfig) {
        let task_name = task_config.name.clone();
        let mut tasks = self.managed_tasks.write().await;
        tasks.insert(task_name.clone(), task_config);
        log::info!(
            "Registered task '{}' for replicaset management",
            task_name
        );
    }

    /// システム設定から管理対象のタスクを登録
    pub async fn register_from_config(&self, config: &SystemConfig) {
        let mut tasks = self.managed_tasks.write().await;
        tasks.clear();

        for (task_name, task_config) in &config.tasks {
            // lifecycle が設定されているタスクのみを管理対象とする
            if task_config.lifecycle.desired_instances > 0 {
                tasks.insert(task_name.clone(), task_config.clone());
                log::info!(
                    "Registered task '{}' for replicaset management (desired_instances: {})",
                    task_name,
                    task_config.lifecycle.desired_instances
                );
            }
        }
    }

    /// 管理対象のタスクを削除
    pub async fn unregister_task(&self, task_name: &str) {
        let mut tasks = self.managed_tasks.write().await;
        if tasks.remove(task_name).is_some() {
            log::info!(
                "Unregistered task '{}' from replicaset management",
                task_name
            );
        }
    }

    /// 現在のインスタンス数を取得
    async fn get_current_instance_count(&self, task_name: &str) -> usize {
        let pod_state_manager = self.pod_manager.pod_state_manager();
        pod_state_manager
            .get_pod_instances(task_name)
            .await
            .len()
    }

    /// Podインスタンスを起動
    async fn spawn_instance(
        &self,
        task_config: &TaskConfig,
        context: &PodStartContext,
    ) -> Result<()> {
        log::info!(
            "Spawning instance for task '{}' (desired_instances: {})",
            task_config.name,
            task_config.lifecycle.desired_instances
        );

        self.pod_manager
            .start_pod_from_config(context.clone())
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to spawn instance for task '{}': {}",
                    task_config.name,
                    e
                )
            })?;

        log::info!(
            "Successfully spawned instance for task '{}'",
            task_config.name
        );
        Ok(())
    }

    /// すべての管理対象タスクのインスタンス数をチェックし、必要に応じて起動
    async fn reconcile_all(&self, shutdown_token: &tokio_util::sync::CancellationToken) {
        let tasks = self.managed_tasks.read().await;
        let task_list: Vec<(String, TaskConfig)> = tasks
            .iter()
            .map(|(name, config)| (name.clone(), config.clone()))
            .collect();
        drop(tasks);

        for (task_name, task_config) in task_list {
            // シャットダウン中は処理を中断
            if shutdown_token.is_cancelled() {
                break;
            }

            let current_count = self.get_current_instance_count(&task_name).await;
            let desired_count = task_config.lifecycle.desired_instances as usize;

            if current_count < desired_count {
                let needed = desired_count - current_count;
                log::info!(
                    "Task '{}' has {} instances, desired: {} (spawning {})",
                    task_name,
                    current_count,
                    desired_count,
                    needed
                );

                // 不足分のインスタンスを起動
                for i in 0..needed {
                    if shutdown_token.is_cancelled() {
                        break;
                    }

                    let context = PodStartContext::new(
                        task_config.clone(),
                        self.topic_manager.clone(),
                        self.shutdown_token.clone(),
                        self.userlog_sender.clone(),
                    );
                    match self.spawn_instance(&task_config, &context).await {
                        Ok(_) => {
                            log::info!(
                                "Successfully spawned instance {}/{} for task '{}'",
                                i + 1,
                                needed,
                                task_name
                            );
                        }
                        Err(e) => {
                            log::error!(
                                "Failed to spawn instance {}/{} for task '{}': {}",
                                i + 1,
                                needed,
                                task_name,
                                e
                            );
                            // エラーが発生しても次のインスタンスの起動を試みる
                        }
                    }

                    // 連続起動を避けるために少し待機
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
        }
    }

    /// Pod終了イベントを処理（再起動が必要な場合）
    async fn handle_pod_exit(&self, task_name: &str, shutdown_token: &tokio_util::sync::CancellationToken) {
        // シャットダウン中は再起動しない
        if shutdown_token.is_cancelled() {
            log::debug!(
                "Shutdown in progress, not restarting task '{}'",
                task_name
            );
            return;
        }

        let task_config = {
            let tasks = self.managed_tasks.read().await;
            tasks.get(task_name).cloned()
        };

        if let Some(config) = task_config {
            let current_count = self.get_current_instance_count(task_name).await;
            let desired_count = config.lifecycle.desired_instances as usize;

            if current_count < desired_count {
                log::info!(
                    "Task '{}' instance exited, current: {}, desired: {} (restarting)",
                    task_name,
                    current_count,
                    desired_count
                );

                let context = PodStartContext::new(
                    config.clone(),
                    self.topic_manager.clone(),
                    self.shutdown_token.clone(),
                    self.userlog_sender.clone(),
                );
                if let Err(e) = self.spawn_instance(&config, &context).await {
                    log::error!(
                        "Failed to restart task '{}' after exit: {}",
                        task_name,
                        e
                    );
                }
            } else {
                log::debug!(
                    "Task '{}' instance exited, but current count ({}) >= desired ({}) (not restarting)",
                    task_name,
                    current_count,
                    desired_count
                );
            }
        } else {
            log::debug!(
                "Task '{}' is not managed by replicaset controller (ignoring exit)",
                task_name
            );
        }
    }

    /// 監視ループを実行
    async fn run_monitoring(&mut self, shutdown_token: tokio_util::sync::CancellationToken) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // 定期的にインスタンス数をチェック
                    self.reconcile_all(&shutdown_token).await;
                }
                task_name = self.pod_exit_receiver.recv() => {
                    match task_name {
                        Some(name) => {
                            // Pod終了イベントを受信したら即座に再起動を試みる
                            log::debug!("Received pod exit notification for '{}'", name);
                            self.handle_pod_exit(&name, &shutdown_token).await;
                        }
                        None => {
                            log::warn!("Pod exit receiver closed");
                            break;
                        }
                    }
                }
                _ = shutdown_token.cancelled() => {
                    log::info!("ReplicaSet controller received shutdown signal");
                    break;
                }
            }
        }

        log::info!("ReplicaSet controller stopped");
    }
}

#[async_trait]
impl BackgroundWorker for ReplicaSetController {
    fn name(&self) -> &str {
        "replicaset_controller"
    }

    fn readiness(&self) -> WorkerReadiness {
        WorkerReadiness::Immediate
    }

    async fn run(mut self, ctx: BackgroundWorkerContext) {
        self.run_monitoring(ctx.shutdown).await;
    }
}

