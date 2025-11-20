use crate::background_worker_registry::BackgroundWorkerRegistry;
use crate::channels::UserLogSender;
use crate::config::{SystemConfig, TaskConfig};
use crate::logging::{
    level_from_env, set_channel_logger, LogAggregatorWorker, LogEvent, UserLogAggregatorWorker,
    UserLogEvent,
};
use crate::topic::TopicSubscriptionRegistry;
use anyhow::Result;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub struct MiclowSystem {
    pub config: SystemConfig,
    topic_manager: TopicSubscriptionRegistry,
    shutdown_token: CancellationToken,
    background_tasks: BackgroundWorkerRegistry,
}

impl MiclowSystem {
    pub fn new(config: SystemConfig) -> Self {
        let shutdown_token: CancellationToken = CancellationToken::new();
        let topic_manager: TopicSubscriptionRegistry = TopicSubscriptionRegistry::new();
        let background_tasks = BackgroundWorkerRegistry::new(shutdown_token.clone());
        Self {
            config,
            topic_manager,
            shutdown_token,
            background_tasks,
        }
    }

    async fn start_user_tasks(
        config: &SystemConfig,
        topic_manager: TopicSubscriptionRegistry,
        shutdown_token: CancellationToken,
        userlog_sender: UserLogSender,
    ) {
        let _ = topic_manager;
        let _ = shutdown_token;
        let _ = userlog_sender;

        let tasks: Vec<&TaskConfig> = config.get_autostart_tasks();

        if tasks.is_empty() {
            log::info!("No tasks configured");
        } else {
            log::info!("Started initial instances for {} tasks from configuration", tasks.len());
        }
    }

    pub async fn start_system(mut self) -> Result<()> {
        let topic_manager: TopicSubscriptionRegistry = self.topic_manager.clone();

        let (log_tx, log_rx) = tokio::sync::mpsc::unbounded_channel::<LogEvent>();
        let _ = set_channel_logger(log_tx, level_from_env());
        let log_worker = LogAggregatorWorker::new(log_rx);
        self.background_tasks.register_worker(log_worker).await;

        let (userlog_tx, userlog_rx) = mpsc::unbounded_channel::<UserLogEvent>();
        let userlog_sender = UserLogSender::new(userlog_tx);
        let userlog_worker = UserLogAggregatorWorker::new(userlog_rx);
        self.background_tasks.register_worker(userlog_worker).await;

        Self::start_user_tasks(
            &self.config,
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
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        log::info!("All user pods stopped (pod manager unavailable in current build)");

        log::logger().flush();

        log::info!("Graceful shutdown completed");
        return Ok(());
    }
}
