use crate::background_worker_registry::BackgroundWorkerRegistry;
use crate::channels::UserLogSender;
use crate::config::{SystemConfig, TaskConfig};
use crate::logging::{
    level_from_env, set_channel_logger, LogAggregatorWorker, LogEvent, UserLogAggregatorWorker,
    UserLogEvent,
};
use crate::system_control::{SystemControlQueue, SystemControlWorker};
use crate::task_runtime::{StartContext, TaskExecutor};
use crate::topic_broker::TopicBroker;
use anyhow::Result;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub struct MiclowSystem {
    pub config: SystemConfig,
    topic_manager: TopicBroker,
    system_control_manager: SystemControlQueue,
    task_executor: TaskExecutor,
    shutdown_token: CancellationToken,
    background_tasks: BackgroundWorkerRegistry,
}

impl MiclowSystem {
    pub fn new(config: SystemConfig) -> Self {
        let topic_manager: TopicBroker = TopicBroker::new();
        let shutdown_token: CancellationToken = CancellationToken::new();
        let system_control_manager: SystemControlQueue =
            SystemControlQueue::new(shutdown_token.clone());
        let task_executor: TaskExecutor = TaskExecutor::new(shutdown_token.clone());
        let background_tasks = BackgroundWorkerRegistry::new(shutdown_token.clone());
        Self {
            config,
            topic_manager,
            system_control_manager,
            task_executor,
            shutdown_token,
            background_tasks,
        }
    }

    async fn start_user_tasks(
        config: &SystemConfig,
        task_executor: &TaskExecutor,
        topic_manager: TopicBroker,
        system_control_manager: SystemControlQueue,
        shutdown_token: CancellationToken,
        userlog_sender: UserLogSender,
    ) {
        let tasks: Vec<&TaskConfig> = config.get_autostart_tasks();

        for task_config in tasks.iter() {
            let task_name: String = task_config.name.clone();

            // TaskConfigを既に持っているので、StartContextを直接作成
            let ready_context = StartContext::new(
                (*task_config).clone(),
                topic_manager.clone(),
                system_control_manager.clone(),
                shutdown_token.clone(),
                userlog_sender.clone(),
                None,
            );

            match task_executor.start_task_from_config(ready_context).await {
                Ok(_) => {
                    log::info!("Started user task '{}'", task_name);
                }
                Err(e) => {
                    log::error!("Failed to start task '{}': {}", task_name, e);
                    continue;
                }
            }
        }

        if tasks.is_empty() {
            log::info!("No tasks configured");
        } else {
            log::info!("Started {} user tasks from configuration", tasks.len());
        }
    }

    pub async fn start_system(mut self) -> Result<()> {
        let topic_manager: TopicBroker = self.topic_manager.clone();

        let (log_tx, log_rx) = tokio::sync::mpsc::unbounded_channel::<LogEvent>();
        let _ = set_channel_logger(log_tx, level_from_env());
        let log_worker = LogAggregatorWorker::new(log_rx);
        self.background_tasks.register_worker(log_worker).await;

        let (userlog_tx, userlog_rx) = mpsc::unbounded_channel::<UserLogEvent>();
        let userlog_sender = UserLogSender::new(userlog_tx);
        let userlog_worker = UserLogAggregatorWorker::new(userlog_rx);
        self.background_tasks.register_worker(userlog_worker).await;

        let sys_worker = SystemControlWorker::new(
            self.system_control_manager.clone(),
            topic_manager.clone(),
            self.task_executor.clone(),
            self.config.clone(),
            userlog_sender.clone(),
        );
        self.background_tasks.register_worker(sys_worker).await;

        Self::start_user_tasks(
            &self.config,
            &self.task_executor,
            topic_manager.clone(),
            self.system_control_manager.clone(),
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

        log::info!("Waiting for running tasks to finish...");
        self.task_executor
            .graceful_shutdown_all(std::time::Duration::from_secs(5))
            .await;

        log::info!("All user tasks stopped");

        log::logger().flush();

        log::info!("Graceful shutdown completed");
        return Ok(());
    }
}
