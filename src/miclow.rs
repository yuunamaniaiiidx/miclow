use crate::backend::{ProtocolBackend, SpawnBackendResult, TaskBackend};
use crate::background_worker_registry::BackgroundWorkerRegistry;
use crate::channels::InputChannel;
use crate::channels::{ExecutorEventChannel, ExecutorEventSender};
use crate::channels::{ShutdownChannel, SystemResponseChannel, UserLogSender};
use crate::config::{SystemConfig, TaskConfig};
use crate::logging::{
    level_from_env, set_channel_logger, spawn_log_aggregator, spawn_user_log_aggregator, LogEvent,
    UserLogEvent, UserLogKind,
};
use crate::message_id::MessageId;
use crate::messages::ExecutorEvent;
use crate::messages::SystemResponseEvent;
use crate::messages::{
    FunctionMessage, FunctionResponseMessage, InputDataMessage, ReturnMessage, SystemResponseMessage,
    TopicMessage,
};
use crate::running_task::RunningTask;
use crate::start_context::StartContext;
use crate::system_control::system_control_action_from_event;
use crate::system_control::SystemControlQueue;
use crate::task_id::TaskId;
use crate::topic_broker::TopicBroker;
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

pub struct TaskSpawner {
    pub task_id: TaskId,
    pub topic_manager: TopicBroker,
    pub system_control_manager: SystemControlQueue,
    pub task_executor: TaskExecutor,
    pub task_name: String,
    pub userlog_sender: UserLogSender,
}

impl TaskSpawner {
    pub fn new(
        task_id: TaskId,
        topic_manager: TopicBroker,
        system_control_manager: SystemControlQueue,
        task_executor: TaskExecutor,
        task_name: String,
        userlog_sender: UserLogSender,
    ) -> Self {
        Self {
            task_id,
            topic_manager,
            system_control_manager,
            task_executor,
            task_name,
            userlog_sender,
        }
    }

    pub async fn spawn_backend(
        self,
        backend: ProtocolBackend,
        shutdown_token: CancellationToken,
        subscribe_topics: Option<Vec<String>>,
        other_return_message_sender: Option<ExecutorEventSender>,
    ) -> SpawnBackendResult {
        let task_id: TaskId = self.task_id.clone();
        let task_name: String = self.task_name.clone();
        let topic_manager: TopicBroker = self.topic_manager;
        let system_control_manager: SystemControlQueue = self.system_control_manager;
        let task_executor: TaskExecutor = self.task_executor;
        let userlog_sender = self.userlog_sender.clone();

        let mut backend_handle = match backend.spawn(task_id.clone()).await {
            Ok(handle) => handle,
            Err(e) => {
                log::error!("Failed to spawn task backend for task {}: {}", task_id, e);
                let input_channel: InputChannel = InputChannel::new();
                let shutdown_channel = ShutdownChannel::new();
                return SpawnBackendResult {
                    worker_handle: tokio::task::spawn(async {}),
                    input_sender: input_channel.sender,
                    shutdown_sender: shutdown_channel.sender,
                };
            }
        };

        let system_response_channel: SystemResponseChannel = SystemResponseChannel::new();
        let mut system_response_receiver = system_response_channel.receiver;
        backend_handle.system_response_sender = system_response_channel.sender;

        let input_sender_for_external = backend_handle.input_sender.clone();
        let shutdown_sender_for_external = backend_handle.shutdown_sender.clone();

        let worker_handle = tokio::task::spawn(async move {
            let topic_data_channel: ExecutorEventChannel = ExecutorEventChannel::new();
            let mut topic_data_receiver = topic_data_channel.receiver;

            if let Some(topics) = subscribe_topics {
                log::info!(
                    "Processing initial topic subscriptions for task {}: {:?}",
                    task_id,
                    topics
                );
                for topic in topics {
                    topic_manager
                        .add_subscriber(
                            topic.clone(),
                            task_id.clone(),
                            topic_data_channel.sender.clone(),
                        )
                        .await;
                    log::info!(
                        "Added initial topic subscription for '{}' from task {}",
                        topic,
                        task_id
                    );
                }
            }

            let return_message_channel: ExecutorEventChannel = ExecutorEventChannel::new();
            let mut return_message_receiver = return_message_channel.receiver;

            loop {
                tokio::select! {
                    biased;

                    _ = shutdown_token.cancelled() => {
                        log::info!("Task {} received shutdown signal", task_id);
                        let _ = backend_handle.shutdown_sender.shutdown();
                        break;
                    },

                    event = backend_handle.event_receiver.recv() => {
                        match event {
                            Some(event) => {
                                let event: ExecutorEvent = event;

                                match &event {
                                    ExecutorEvent::Message { topic, data } => {
                                        log::info!("Message event for task {} on topic '{}': '{}'", task_id, topic, data);
                                        match topic_manager.broadcast_message(event.clone()).await {
                                            Ok(success_count) => {
                                                log::info!("Broadcasted message from task {} to {} subscribers on topic '{}'", task_id, success_count, topic);
                                            },
                                            Err(e) => {
                                                log::error!("Failed to broadcast message from task {} on topic '{}': {}", task_id, topic, e);
                                            }
                                        }
                                    },
                                    ExecutorEvent::SystemControl { .. } => {
                                        if let Some(system_control_action) = system_control_action_from_event(&event) {
                                            log::info!("SystemControl detected from task {}", task_id);
                                            if let Err(e) = system_control_manager.send_system_control_action(
                                                system_control_action,
                                                task_id.clone(),
                                                backend_handle.system_response_sender.clone(),
                                                topic_data_channel.sender.clone(),
                                                return_message_channel.sender.clone()
                                            ).await {
                                                log::warn!("Failed to send system control action to worker (task {}): {}", task_id, e);
                                            } else {
                                                log::info!("Sent system control action to worker for task {}", task_id);
                                            }
                                        } else {
                                            log::warn!("Failed to convert SystemControl event to action for task {}", task_id);
                                        }
                                    },
                                    ExecutorEvent::ReturnMessage { data } => {
                                        log::info!("ReturnMessage received from task {}: '{}'", task_id, data);
                                        if let Some(ref sender) = other_return_message_sender {
                                            if let Err(e) = sender.send(ExecutorEvent::new_function_response(
                                                task_name.clone(),
                                                data.clone(),
                                            )) {
                                                log::warn!(
                                                    "Failed to send function response to other_return_message_sender for task {}: {}",
                                                    task_id,
                                                    e
                                                );
                                            }
                                        } else {
                                            log::warn!(
                                                "ReturnMessage received but other_return_message_sender is not available for task {}",
                                                task_id
                                            );
                                        }
                                    },
                                    ExecutorEvent::FunctionResponse { .. } => {
                                        log::debug!(
                                            "FunctionResponse event emitted directly by task {} - ignoring",
                                            task_id
                                        );
                                    },
                                    ExecutorEvent::TaskStdout { data } => {
                                        let flags = task_executor.get_view_flags_by_task_id(&task_id).await;
                                        if let Some((view_stdout, _)) = flags {
                                            if view_stdout {
                                                let _ = userlog_sender.send(UserLogEvent { task_id: task_id.to_string(), task_name: task_name.clone(), kind: UserLogKind::Stdout, msg: data.clone() });
                                            }
                                        }
                                    },
                                    ExecutorEvent::TaskStderr { data } => {
                                        let flags = task_executor.get_view_flags_by_task_id(&task_id).await;
                                        if let Some((_, view_stderr)) = flags {
                                            if view_stderr {
                                                let _ = userlog_sender.send(UserLogEvent { task_id: task_id.to_string(), task_name: task_name.clone(), kind: UserLogKind::Stderr, msg: data.clone() });
                                            }
                                        }
                                    },
                                    ExecutorEvent::Error { error } => {
                                        log::error!("Error event for task {}: '{}'", task_id, error);
                                    },
                                    ExecutorEvent::Exit { exit_code } => {
                                        log::info!("Exit event for task {} with exit code: {}", task_id, exit_code);

                                        let removed_topics: Vec<String> = topic_manager.remove_all_subscriptions_by_task(task_id.clone()).await;
                                        log::info!("Task {} exited, removed {} topic subscriptions", task_id, removed_topics.len());

                                        if let Some(_removed_task) = task_executor.unregister_task_by_task_id(&task_id).await {
                                            log::info!("Removed task with TaskId={} (Human name index updated)", task_id);
                                        } else {
                                            log::warn!("Task with TaskId={} not found in executor during cleanup", task_id);
                                        }
                                    }
                                }
                            },
                            None => {
                                log::info!("Task backend event receiver closed for task {}", task_id);
                                break;
                            }
                        }
                    },

                    topic_data = topic_data_receiver.recv() => {
                        match topic_data {
                            Some(topic_data) => {
                                if let Some(data) = topic_data.data() {
                                    if let Some(topic_name) = topic_data.topic() {
                                        let topic_msg = TopicMessage {
                                            message_id: MessageId::new(),
                                            topic: topic_name.clone(),
                                            data: data.clone(),
                                        };
                                        if let Err(e) = backend_handle.input_sender.send(InputDataMessage::Topic(topic_msg)) {
                                            log::warn!("Failed to send topic message to task backend for task {}: {}", task_id, e);
                                        }
                                    } else {
                                        log::warn!("Topic data received without topic name for task {}, skipping", task_id);
                                        continue;
                                    }
                                }
                            },
                            None => {
                                log::info!("Topic data receiver closed for task {}", task_id);
                                break;
                            }
                        }
                    },

                    return_message = return_message_receiver.recv() => {
                        match return_message {
                            Some(message) => {
                                log::info!("Return message received for task {}: {:?}", task_id, &message);
                                match message {
                                    ExecutorEvent::FunctionResponse { function_name, data } => {
                                        let function_response_msg = FunctionResponseMessage {
                                            message_id: MessageId::new(),
                                            function_name,
                                            data,
                                        };
                                        if let Err(e) = backend_handle.input_sender.send(
                                            InputDataMessage::FunctionResponse(function_response_msg),
                                        ) {
                                            log::warn!(
                                                "Failed to send function response to task backend for task {}: {}",
                                                task_id,
                                                e
                                            );
                                        }
                                    }
                                    ExecutorEvent::ReturnMessage { data } => {
                                        let return_msg = ReturnMessage {
                                            message_id: MessageId::new(),
                                            data,
                                        };
                                        if let Err(e) = backend_handle
                                            .input_sender
                                            .send(InputDataMessage::Return(return_msg))
                                        {
                                            log::warn!(
                                                "Failed to send return message to task backend for task {}: {}",
                                                task_id,
                                                e
                                            );
                                        }
                                    }
                                    unexpected => {
                                        log::warn!(
                                            "Unexpected event variant received on return channel for task {}: {:?}",
                                            task_id,
                                            unexpected
                                        );
                                    }
                                }
                            },
                            None => {
                                log::info!("Return message receiver closed for task {}", task_id);
                                break;
                            }
                        }
                    },

                    system_response = system_response_receiver.recv() => {
                        match system_response {
                            Some(SystemResponseEvent::SystemResponse { topic, status, data }) => {
                                log::info!("SystemResponse event for task {}: topic='{}', status='{}', data='{}'", task_id, topic, status, data);
                                let system_response_msg = SystemResponseMessage {
                                    message_id: MessageId::new(),
                                    topic,
                                    status,
                                    data,
                                };
                                if let Err(e) = backend_handle.input_sender.send(InputDataMessage::SystemResponse(system_response_msg)) {
                                    log::warn!("Failed to send system response message to task backend for task {}: {}", task_id, e);
                                }
                            },
                            Some(SystemResponseEvent::SystemError { topic, status, error }) => {
                                log::error!("SystemError event for task {}: topic='{}', status='{}', error='{}'", task_id, topic, status, error);
                                let system_response_msg = SystemResponseMessage {
                                    message_id: MessageId::new(),
                                    topic,
                                    status,
                                    data: error,
                                };
                                if let Err(e) = backend_handle.input_sender.send(InputDataMessage::SystemResponse(system_response_msg)) {
                                    log::warn!("Failed to send system error message to task backend for task {}: {}", task_id, e);
                                }
                            },
                            None => {
                                log::info!("SystemResponse receiver closed for task {}", task_id);
                                break;
                            }
                        }
                    }
                }
            }

            log::info!("Task {} completed", task_id);
        });

        SpawnBackendResult {
            worker_handle,
            input_sender: input_sender_for_external,
            shutdown_sender: shutdown_sender_for_external,
        }
    }
}

#[derive(Clone)]
pub struct TaskExecutor {
    running_tasks: Arc<RwLock<HashMap<TaskId, RunningTask>>>,
    name_to_id: Arc<RwLock<HashMap<String, Vec<TaskId>>>>,
    id_to_name: Arc<RwLock<HashMap<TaskId, String>>>,
    monitored_task_names: Arc<RwLock<HashSet<String>>>,
    shutdown_token: CancellationToken,
}

impl TaskExecutor {
    pub fn new(shutdown_token: CancellationToken) -> Self {
        Self {
            running_tasks: Arc::new(RwLock::new(HashMap::new())),
            name_to_id: Arc::new(RwLock::new(HashMap::new())),
            id_to_name: Arc::new(RwLock::new(HashMap::new())),
            monitored_task_names: Arc::new(RwLock::new(HashSet::new())),
            shutdown_token,
        }
    }

    pub async fn register_task(&self, task_name: String, task: RunningTask) -> Result<(), String> {
        let mut running_tasks = self.running_tasks.write().await;
        let mut name_to_id = self.name_to_id.write().await;
        let mut id_to_name = self.id_to_name.write().await;
        let task_id = task.task_id.clone();

        running_tasks.insert(task_id.clone(), task);
        name_to_id
            .entry(task_name.clone())
            .or_insert_with(Vec::new)
            .push(task_id.clone());
        id_to_name.insert(task_id, task_name.clone());

        // "system."で始まらないタスクは自動的に監視対象に追加
        if !task_name.starts_with("system.") {
            let mut monitored = self.monitored_task_names.write().await;
            monitored.insert(task_name);
        }

        Ok(())
    }

    pub async fn try_register_task(
        &self,
        task_name: String,
        task: RunningTask,
    ) -> Result<(), String> {
        let name_to_id = self.name_to_id.read().await;
        if let Some(ids) = name_to_id.get(&task_name) {
            if !ids.is_empty() {
                log::warn!(
                    "Task '{}' is already running - cancelling new task start",
                    task_name
                );
                return Err(format!("Task '{}' is already running", task_name));
            }
        }
        drop(name_to_id);

        self.register_task(task_name, task).await
    }

    pub async fn unregister_task_by_task_id(&self, task_id: &TaskId) -> Option<RunningTask> {
        let mut running_tasks = self.running_tasks.write().await;
        let mut name_to_id = self.name_to_id.write().await;
        let mut id_to_name = self.id_to_name.write().await;
        if let Some(task) = running_tasks.remove(task_id) {
            // タスク名を取得
            let task_name = id_to_name.get(task_id).cloned();

            // Vecから該当のtask_idを削除
            for ids in name_to_id.values_mut() {
                ids.retain(|id| id != task_id);
            }
            // 空になったVecのエントリを削除
            name_to_id.retain(|_name, ids| !ids.is_empty());
            id_to_name.remove(task_id);

            // 監視対象タスクの終了をチェック
            if let Some(name) = task_name {
                let mut monitored = self.monitored_task_names.write().await;
                if monitored.remove(&name) {
                    // 監視対象タスクが終了した
                    if monitored.is_empty() {
                        // すべての監視対象タスクが終了した
                        drop(monitored);
                        log::info!("All monitored tasks finished, requesting shutdown...");
                        self.shutdown_token.cancel();
                    }
                }
            }

            Some(task)
        } else {
            None
        }
    }
    pub async fn unregister_task_by_name(&self, task_name: &str) -> Option<RunningTask> {
        let id = {
            let name_to_id = self.name_to_id.read().await;
            name_to_id
                .get(task_name)
                .and_then(|ids| ids.last())
                .cloned()
        };
        if let Some(tid) = id {
            self.unregister_task_by_task_id(&tid).await
        } else {
            None
        }
    }

    pub async fn stop_task_by_task_id(&self, task_id: &TaskId) -> Result<()> {
        let running_task = {
            let mut running_tasks = self.running_tasks.write().await;
            running_tasks.remove(task_id)
        };
        match running_task {
            Some(task) => {
                log::info!(
                    "Stopped task with TaskId={} (Human name index updated)",
                    task_id
                );
                let mut name_to_id = self.name_to_id.write().await;
                let mut id_to_name = self.id_to_name.write().await;
                // Vecから該当のtask_idを削除
                for ids in name_to_id.values_mut() {
                    ids.retain(|id| id != task_id);
                }
                // 空になったVecのエントリを削除
                name_to_id.retain(|_name, ids| !ids.is_empty());
                id_to_name.remove(task_id);
                task.task_handle.abort();
                Ok(())
            }
            None => {
                log::warn!("Attempted to stop non-running task TaskId={}", task_id);
                Err(anyhow::anyhow!("TaskId '{}' is not running", task_id))
            }
        }
    }

    pub async fn get_running_tasks_info(&self) -> Vec<(String, TaskId)> {
        let name_to_id = self.name_to_id.read().await;
        name_to_id
            .iter()
            .filter_map(|(name, ids)| ids.last().map(|task_id| (name.clone(), task_id.clone())))
            .collect()
    }

    pub async fn get_view_flags_by_task_id(&self, task_id: &TaskId) -> Option<(bool, bool)> {
        let running_tasks_guard = self.running_tasks.read().await;
        running_tasks_guard
            .get(task_id)
            .map(|t| (t.view_stdout, t.view_stderr))
    }

    pub async fn get_task_name_by_id(&self, task_id: &TaskId) -> Option<String> {
        let id_to_name = self.id_to_name.read().await;
        id_to_name.get(task_id).cloned()
    }

    pub async fn graceful_shutdown_all(&self, timeout: std::time::Duration) {
        let tasks: Vec<(TaskId, RunningTask)> = {
            let mut running_tasks = self.running_tasks.write().await;
            let drained: Vec<(TaskId, RunningTask)> = running_tasks.drain().collect();
            drained
        };
        {
            let mut name_to_id = self.name_to_id.write().await;
            let mut id_to_name = self.id_to_name.write().await;
            name_to_id.clear();
            id_to_name.clear();
        }

        for (_tid, task) in &tasks {
            let _ = task.shutdown_sender.shutdown();
        }

        for (_tid, task) in tasks {
            let _ = tokio::time::timeout(timeout, task.task_handle).await;
        }
    }

    pub async fn start_task_from_config(&self, context: StartContext) -> Result<()> {
        let task_config = &context.task_config;
        log::info!("Starting task '{}'", task_config.name);

        let task_id_new = TaskId::new();

        // ProtocolBackendへの変換（バリデーションも含む）
        let backend: ProtocolBackend = ProtocolBackend::try_from(task_config.clone())
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to create protocol backend for task '{}': {}",
                    task_config.name,
                    e
                )
            })
            .unwrap_or_else(|e| {
                eprintln!(
                    "Config validation failed for task '{}': {}",
                    task_config.name, e
                );
                eprintln!("\nError details:");
                for (i, cause) in e.chain().enumerate() {
                    eprintln!("  {}: {}", i, cause);
                }
                std::process::exit(1);
            });

        // コマンドの存在確認（MiclowStdinプロトコルの場合のみ）
        if let ProtocolBackend::MiclowStdin(ref config) = backend {
            if !std::path::Path::new(&config.command).exists()
                && !which::which(&config.command).is_ok()
            {
                return Err(anyhow::anyhow!(
                    "Command '{}' not found in PATH or file system",
                    config.command
                ));
            }
        }

        let subscribe_topics = task_config.subscribe_topics.clone();

        let task_spawner = TaskSpawner::new(
            task_id_new.clone(),
            context.topic_manager,
            context.system_control_manager,
            self.clone(),
            task_config.name.clone(),
            context.userlog_sender,
        );

        let spawn_result = task_spawner
            .spawn_backend(
                backend.clone(),
                context.shutdown_token,
                subscribe_topics,
                context.return_message_sender.clone(),
            )
            .await;

        // view_stdoutとview_stderrをProtocolBackendから取得
        let (view_stdout, view_stderr) = match &backend {
            ProtocolBackend::MiclowStdin(config) => (config.view_stdout, config.view_stderr),
            ProtocolBackend::Interactive(_) => (false, false), // Interactiveプロトコルでは使用しない
            ProtocolBackend::McpServer(_) => (false, false),   // McpServerプロトコルでは使用しない
                                                                // あまり気持ちの良い設計じゃない可能性がある
        };

        let running_task = RunningTask {
            task_id: task_id_new.clone(),
            shutdown_sender: spawn_result.shutdown_sender.clone(),
            input_sender: spawn_result.input_sender.clone(),
            task_handle: spawn_result.worker_handle,
            view_stdout,
            view_stderr,
        };

        // Handle function message if this is a function call
        // Send FunctionMessage even if initial_input is None (for functions called without arguments)
        if context.return_message_sender.is_some() {
            let input_sender_for_initial = spawn_result.input_sender.clone();
            let initial_input_for_log = context
                .initial_input
                .clone()
                .unwrap_or_else(|| "".to_string());
            let task_id_for_log = task_id_new.clone();
            tokio::task::spawn(async move {
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

                let function_msg = FunctionMessage {
                    message_id: MessageId::new(),
                    data: initial_input_for_log.clone(),
                };

                if let Err(e) =
                    input_sender_for_initial.send(InputDataMessage::Function(function_msg))
                {
                    log::warn!(
                        "Failed to send function message to task {}: {}",
                        task_id_for_log,
                        e
                    );
                }

                log::info!(
                    "Sent function message to {} with data='{}'",
                    task_id_for_log,
                    initial_input_for_log
                );
            });
        }

        // Register task based on allow_duplicate flag
        if task_config.allow_duplicate {
            if let Err(e) = self
                .register_task(task_config.name.clone(), running_task)
                .await
            {
                return Err(anyhow::anyhow!(
                    "Failed to register task '{}': {}",
                    task_config.name,
                    e
                ));
            }
            log::debug!(
                "Registered task '{}' (ID: {}) - duplicate instances allowed",
                task_config.name,
                task_id_new
            );
        } else {
            if let Err(e) = self
                .try_register_task(task_config.name.clone(), running_task)
                .await
            {
                return Err(anyhow::anyhow!(
                    "Failed to register task '{}': {}",
                    task_config.name,
                    e
                ));
            }
            log::debug!(
                "Registered task '{}' (ID: {}) - duplicate instances not allowed",
                task_config.name,
                task_id_new
            );
        }

        log::info!(
            "Successfully started task '{}' (ID: {})",
            task_config.name,
            task_id_new
        );
        Ok(())
    }
}

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
        Self {
            config,
            topic_manager,
            system_control_manager,
            task_executor,
            shutdown_token,
            background_tasks: BackgroundWorkerRegistry::new(),
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
        let logging_shutdown = CancellationToken::new();
        let (log_ready_tx, log_ready_rx) = tokio::sync::oneshot::channel();
        let h_log = spawn_log_aggregator(log_rx, logging_shutdown.clone(), Some(log_ready_tx));
        // ログタスクの起動を待機
        let _ = log_ready_rx.await;
        self.background_tasks.register("log_aggregator", h_log);

        let (userlog_tx, userlog_rx) = mpsc::unbounded_channel::<UserLogEvent>();
        let userlog_sender = UserLogSender::new(userlog_tx);
        let (userlog_ready_tx, userlog_ready_rx) = tokio::sync::oneshot::channel();
        let h_userlog =
            spawn_user_log_aggregator(userlog_rx, logging_shutdown.clone(), Some(userlog_ready_tx));
        // ユーザーログタスクの起動を待機
        let _ = userlog_ready_rx.await;
        self.background_tasks
            .register("user_log_aggregator", h_userlog);

        let h_sys = crate::system_control::start_system_control_worker(
            self.system_control_manager.clone(),
            topic_manager.clone(),
            self.task_executor.clone(),
            self.config.clone(),
            self.shutdown_token.clone(),
            userlog_sender.clone(),
        );
        self.background_tasks
            .register("system_control_worker", h_sys);

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
        logging_shutdown.cancel();
        self.background_tasks.abort_all().await;

        Self::shutdown_workers(self.task_executor, self.shutdown_token).await;

        log::logger().flush();

        log::info!("Graceful shutdown completed");
        return Ok(());
    }

    async fn shutdown_workers(task_executor: TaskExecutor, shutdown_token: CancellationToken) {
        log::info!("Starting graceful shutdown...");

        log::info!("Cancelling shutdown token");
        shutdown_token.cancel();

        log::info!("Waiting for running tasks to finish...");
        task_executor
            .graceful_shutdown_all(std::time::Duration::from_secs(5))
            .await;

        log::info!("All user tasks stopped");
    }
}
