use crate::channels::UserLogSender;
use crate::config::SystemConfig;
use crate::coordinator::CoordinatorManager;
use crate::logging::{
    level_from_env, set_channel_logger, LogAggregatorWorker, LogEvent, UserLogAggregatorWorker,
    UserLogEvent,
};
use crate::shutdown_manager::{ShutdownLayer, ShutdownManager};
use anyhow::Result;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub struct MiclowSystem {
    pub config: SystemConfig,
    coordinator_manager: CoordinatorManager,
    log_shutdown_token: CancellationToken,
    user_shutdown_token: CancellationToken,
    shutdown_manager: ShutdownManager,
}

impl MiclowSystem {
    pub fn new(config: SystemConfig) -> Self {
        let log_shutdown_token: CancellationToken = CancellationToken::new();
        let user_shutdown_token: CancellationToken = CancellationToken::new();
        let topic_manager = crate::topic::TopicSubscriptionRegistry::new();
        let coordinator_manager =
            CoordinatorManager::new(topic_manager.clone(), user_shutdown_token.clone());
        let shutdown_manager = ShutdownManager::with_shutdown_token(log_shutdown_token.clone());
        Self {
            config,
            coordinator_manager,
            log_shutdown_token,
            user_shutdown_token,
            shutdown_manager,
        }
    }

    pub async fn start_system(mut self) -> Result<()> {
        let (log_tx, log_rx) = tokio::sync::mpsc::unbounded_channel::<LogEvent>();
        let _ = set_channel_logger(log_tx, level_from_env());
        let log_worker = LogAggregatorWorker::new(log_rx);
        let log_handle = self.shutdown_manager.task_handle(ShutdownLayer::Logging);
        let _ = log_handle
            .run(LogAggregatorWorker::TASK_NAME, move |shutdown| async move {
                log_worker.run(shutdown).await;
            })
            .expect("Failed to spawn log aggregator worker");

        let (userlog_tx, userlog_rx) = mpsc::unbounded_channel::<UserLogEvent>();
        let userlog_sender = UserLogSender::new(userlog_tx);
        let userlog_worker = UserLogAggregatorWorker::new(userlog_rx);
        let userlog_handle = self.shutdown_manager.task_handle(ShutdownLayer::Logging);
        let _ = userlog_handle
            .run(
                UserLogAggregatorWorker::TASK_NAME,
                move |shutdown| async move {
                    userlog_worker.run(shutdown).await;
                },
            )
            .expect("Failed to spawn user log aggregator worker");

        self.coordinator_manager
            .start_all(&self.config, userlog_sender.clone());

        log::info!("System running. Press Ctrl+C to stop.");

        let log_shutdown_token = self.log_shutdown_token.clone();
        let user_shutdown_token = self.user_shutdown_token.clone();

        let ctrlc_fut = async {
            if let Err(e) = tokio::signal::ctrl_c().await {
                log::error!("Ctrl+C signal error: {}", e);
            } else {
                log::info!("Received Ctrl+C. Requesting graceful shutdown...");
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
        let _ = self
            .shutdown_manager
            .shutdown_all(std::time::Duration::from_secs(5))
            .await;
        log::info!("All log workers stopped");

        log::info!("Stopping user tasks...");
        user_shutdown_token.cancel();
        log::info!("Waiting for running pods to finish...");
        self.coordinator_manager.shutdown().await;
        log::info!("All user pods stopped");

        log::logger().flush();

        log::info!("Graceful shutdown completed");
        return Ok(());
    }
}
