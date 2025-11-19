use crate::background_worker_registry::BackgroundWorkerRegistry;
use crate::channels::{TaskExitChannel, UserLogSender};
use crate::config::{SystemConfig, TaskConfig};
use crate::logging::{
    level_from_env, set_channel_logger, LogAggregatorWorker, LogEvent, UserLogAggregatorWorker,
    UserLogEvent,
};
use crate::replicaset::ReplicaSetController;
use crate::pod::{PodStartContext, PodManager};
use crate::topic_subscription_registry::TopicSubscriptionRegistry;
use crate::topic_load_balancer::TopicLoadBalancer;
use anyhow::Result;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub struct MiclowSystem {
    pub config: SystemConfig,
    topic_manager: TopicSubscriptionRegistry,
    pod_manager: PodManager,
    shutdown_token: CancellationToken,
    background_tasks: BackgroundWorkerRegistry,
}

impl MiclowSystem {
    pub fn new(config: SystemConfig) -> Self {
        let topic_manager: TopicSubscriptionRegistry = TopicSubscriptionRegistry::new();
        let shutdown_token: CancellationToken = CancellationToken::new();
        let pod_manager: PodManager = PodManager::new(shutdown_token.clone());
        let background_tasks = BackgroundWorkerRegistry::new(shutdown_token.clone());
        Self {
            config,
            topic_manager,
            pod_manager,
            shutdown_token,
            background_tasks,
        }
    }

    async fn start_user_tasks(
        config: &SystemConfig,
        pod_manager: &PodManager,
        topic_manager: TopicSubscriptionRegistry,
        shutdown_token: CancellationToken,
        userlog_sender: UserLogSender,
    ) {
        let tasks: Vec<&TaskConfig> = config.get_autostart_tasks();

        for task_config in tasks.iter() {
            let task_name: String = task_config.name.clone();

            // TaskConfigを既に持っているので、PodStartContextを直接作成
            let ready_context = PodStartContext::new(
                (*task_config).clone(),
                topic_manager.clone(),
                shutdown_token.clone(),
                userlog_sender.clone(),
            );

            // desired_instances に基づいて初期インスタンスを起動
            let desired_instances = task_config.lifecycle.desired_instances;
            let instances_to_start = desired_instances.max(1); // 最低1つは起動

            for i in 0..instances_to_start {
                match pod_manager.start_pod_from_config(ready_context.clone()).await {
                    Ok(_) => {
                        log::info!(
                            "Started user task '{}' instance {}/{}",
                            task_name,
                            i + 1,
                            instances_to_start
                        );
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to start task '{}' instance {}/{}: {}",
                            task_name,
                            i + 1,
                            instances_to_start,
                            e
                        );
                        // エラーが発生しても次のインスタンスの起動を試みる
                    }
                }
            }
        }

        if tasks.is_empty() {
            log::info!("No tasks configured");
        } else {
            log::info!("Started initial instances for {} tasks from configuration", tasks.len());
        }
    }

    pub async fn start_system(mut self) -> Result<()> {
        let topic_manager: TopicSubscriptionRegistry = self.topic_manager.clone();

        // TopicLoadBalancer を作成
        let pod_state_manager = self.pod_manager.pod_state_manager().clone();
        let load_balancer = TopicLoadBalancer::new(self.pod_manager.clone(), pod_state_manager);

        // TopicSubscriptionRegistry に必要な参照を設定
        topic_manager.set_pod_manager(self.pod_manager.clone()).await;
        topic_manager.set_load_balancer(load_balancer.clone()).await;
        topic_manager.set_system_config(self.config.clone()).await;

        let (log_tx, log_rx) = tokio::sync::mpsc::unbounded_channel::<LogEvent>();
        let _ = set_channel_logger(log_tx, level_from_env());
        let log_worker = LogAggregatorWorker::new(log_rx);
        self.background_tasks.register_worker(log_worker).await;

        let (userlog_tx, userlog_rx) = mpsc::unbounded_channel::<UserLogEvent>();
        let userlog_sender = UserLogSender::new(userlog_tx);
        let userlog_worker = UserLogAggregatorWorker::new(userlog_rx);
        self.background_tasks.register_worker(userlog_worker).await;

        // Pod終了通知チャネルを作成
        let task_exit_channel = TaskExitChannel::new();
        self.pod_manager
            .set_pod_exit_sender(task_exit_channel.sender.clone())
            .await;

        // ReplicaSet コントローラーを初期化
        let replicaset_controller = ReplicaSetController::new(
            self.pod_manager.clone(),
            task_exit_channel.receiver,
            topic_manager.clone(),
            self.shutdown_token.clone(),
            userlog_sender.clone(),
        );
        replicaset_controller
            .register_from_config(&self.config)
            .await;

        // ReplicaSet コントローラーを BackgroundWorker として登録
        self.background_tasks.register_worker(replicaset_controller).await;

        Self::start_user_tasks(
            &self.config,
            &self.pod_manager,
            topic_manager.clone(),
            self.shutdown_token.clone(),
            userlog_sender.clone(),
        )
        .await;

        log::info!("System running. Press Ctrl+C to stop.");

        let shutdown_token = self.shutdown_token.clone();

        let ctrlc_fut = async {
            if let Err(e) = tokio::signal::ctrl_c().await {
                log::error!("Ctrl+C signal error: {}", e);
            } else {
                log::info!("Received Ctrl+C. Requesting graceful shutdown...");
                shutdown_token.cancel();
            }
        };
        tokio::select! {
            _ = shutdown_token.cancelled() => {
                log::info!("Shutdown signal received");
            },
            _ = ctrlc_fut => {
                log::info!("Ctrl+C signal received, proceeding with shutdown");
            },
        }

        log::info!("Received shutdown signal, stopping all workers...");

        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        self.background_tasks.abort_all().await;

        log::info!("Starting graceful shutdown...");

        log::info!("Cancelling shutdown token");
        shutdown_token.cancel();

        log::info!("Waiting for running pods to finish...");
        self.pod_manager
            .graceful_shutdown_all(std::time::Duration::from_secs(5))
            .await;

        log::info!("All user pods stopped");

        log::logger().flush();

        log::info!("Graceful shutdown completed");
        return Ok(());
    }
}
