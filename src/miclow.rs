use crate::background_worker_registry::BackgroundWorkerRegistry;
use crate::channels::UserLogSender;
use crate::config::SystemConfig;
use crate::deployment::DeploymentManager;
use crate::logging::{
    level_from_env, set_channel_logger, LogAggregatorWorker, LogEvent, UserLogAggregatorWorker,
    UserLogEvent,
};
use anyhow::Result;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub struct MiclowSystem {
    pub config: SystemConfig,
    deployment_manager: DeploymentManager,
    log_shutdown_token: CancellationToken,
    user_shutdown_token: CancellationToken,
    background_tasks: BackgroundWorkerRegistry,
}

impl MiclowSystem {
    pub fn new(config: SystemConfig) -> Self {
        let log_shutdown_token: CancellationToken = CancellationToken::new();
        let user_shutdown_token: CancellationToken = CancellationToken::new();
        let topic_manager = crate::topic::TopicSubscriptionRegistry::new();
        let deployment_manager =
            DeploymentManager::new(topic_manager.clone(), user_shutdown_token.clone());
        let background_tasks = BackgroundWorkerRegistry::new(log_shutdown_token.clone());
        Self {
            config,
            deployment_manager,
            log_shutdown_token,
            user_shutdown_token,
            background_tasks,
        }
    }

    pub async fn start_system(mut self) -> Result<()> {
        let (log_tx, log_rx) = tokio::sync::mpsc::unbounded_channel::<LogEvent>();
        let _ = set_channel_logger(log_tx, level_from_env());
        let log_worker = LogAggregatorWorker::new(log_rx);
        self.background_tasks.register_worker(log_worker).await;

        let (userlog_tx, userlog_rx) = mpsc::unbounded_channel::<UserLogEvent>();
        let userlog_sender = UserLogSender::new(userlog_tx);
        let userlog_worker = UserLogAggregatorWorker::new(userlog_rx);
        self.background_tasks.register_worker(userlog_worker).await;

        self.deployment_manager
            .start_all(&self.config, userlog_sender.clone());

        log::info!("System running. Press Ctrl+C to stop.");

        let log_shutdown_token = self.log_shutdown_token.clone();
        let user_shutdown_token = self.user_shutdown_token.clone();

        let ctrlc_fut = async {
            if let Err(e) = tokio::signal::ctrl_c().await {
                log::error!("Ctrl+C signal error: {}", e);
            } else {
                log::info!("Received Ctrl+C. Requesting graceful shutdown...");
                // まずログタスクの終了を開始
                log_shutdown_token.cancel();
            }
        };
        tokio::select! {
            _ = log_shutdown_token.cancelled() => {
                log::info!("Shutdown signal received");
            },
            _ = ctrlc_fut => {
                log::info!("Ctrl+C signal received, proceeding with shutdown");
            },
        }

        log::info!("Received shutdown signal, stopping log workers first...");
        // ログタスクの終了を待つ（タイムアウト付き）
        self.background_tasks.shutdown_all(std::time::Duration::from_secs(5)).await;
        log::info!("All log workers stopped");

        log::info!("Stopping user tasks...");
        // ログタスク終了後、ユーザータスクを終了
        user_shutdown_token.cancel();
        log::info!("Waiting for running pods to finish...");
        self.deployment_manager.shutdown().await;
        log::info!("All user pods stopped");

        log::logger().flush();

        log::info!("Graceful shutdown completed");
        return Ok(());
    }
}
