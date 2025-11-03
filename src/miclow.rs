use anyhow::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader, stdin};
use tokio::task;
use tokio_util::sync::CancellationToken;
use crate::task_id::TaskId;
use async_trait::async_trait;
use std::process::Stdio;
use crate::buffer::{InputBufferManager, StreamOutcome};
use crate::logging::{UserLogEvent, UserLogKind, spawn_user_log_aggregator, LogEvent, spawn_log_aggregator, set_channel_logger, level_from_env};
use crate::executor_event_channel::{ExecutorEvent, ExecutorEventSender, ExecutorEventReceiver, ExecutorEventChannel};
use crate::input_channel::{InputChannel, InputSender, InputReceiver, InputDataMessage, TopicMessage, SystemResponseMessage, ReturnMessage, FunctionMessage, StdinProtocol};
use crate::system_response_channel::{SystemResponseChannel, SystemResponseSender, SystemResponseEvent, SystemResponseStatus};
use crate::shutdown_channel::{ShutdownChannel, ShutdownSender};
use crate::user_log_sender::UserLogSender;
use crate::system_control_command::{SystemControlCommand, system_control_command_to_handler};
use crate::spawn_backend_result::SpawnBackendResult;
use crate::task_backend_handle::TaskBackendHandle;
use crate::running_task::RunningTask;
use crate::start_context::{StartContext, ReadyStartContext, StartContextVariant};
use crate::system_control_manager::SystemControlManager;
use crate::topic_manager::TopicManager;
use crate::task_backend::TaskBackend;
use crate::interactive_backend::InteractiveBackend;
use crate::shell_backend::ShellBackend;
use crate::background_task_manager::BackgroundTaskManager;
use crate::config::{TaskConfig, SystemConfig};
use tokio::task::JoinHandle;
#[cfg(unix)]
use nix::sys::signal::{kill, Signal};
#[cfg(unix)]
use nix::unistd::Pid;
 
pub fn start_system_control_worker(
    system_control_manager: SystemControlManager,
    topic_manager: TopicManager,
    task_executor: TaskExecutor,
    system_config: SystemConfig,
    shutdown_token: CancellationToken,
    userlog_sender: UserLogSender,
) -> tokio::task::JoinHandle<()> {
        task::spawn(async move {
        let running_commands: Arc<RwLock<HashMap<TaskId, JoinHandle<()>>>> = Arc::new(RwLock::new(HashMap::new()));
        
        loop {
            tokio::select! {
                biased;
                
                _ = shutdown_token.cancelled() => {
                    log::info!("SystemControl worker received shutdown signal, aborting all running commands");
                    
                    let mut commands = running_commands.write().await;
                    for (task_id, handle) in commands.drain() {
                        log::info!("Aborting SystemControl execution for task {}", task_id);
                        handle.abort();
                    }
                    
                    break;
                }
                
                maybe = async { system_control_manager.recv_command().await } => {
                        match maybe {
                            Some(message) => {
                                let task_id: TaskId = message.task_id.clone();
                                
                                log::info!("Spawning SystemControl for task {} (parallel execution)", task_id);
                                
                                let command_shutdown_token = shutdown_token.clone();
                                let running_commands_clone = running_commands.clone();
                                let task_id_clone = task_id.clone();
                                let topic_manager_clone = topic_manager.clone();
                                let task_executor_clone = task_executor.clone();
                                let system_config_clone = system_config.clone();
                                let shutdown_token_clone = shutdown_token.clone();
                                let userlog_sender_clone = userlog_sender.clone();
                                let system_control_manager_clone = system_control_manager.clone();
                                let response_channel_clone = message.response_channel.clone();
                                let task_event_sender_clone = message.task_event_sender.clone();
                                let return_message_sender_clone = message.return_message_sender.clone();
                                let response_channel_for_cancel = message.response_channel.clone();
                                let command_clone = message.command.clone();
                                
                                let handle = tokio::spawn(async move {
                                    let task_id_for_log = task_id_clone.clone();
                                    log::info!("Executing SystemControl for task {}", task_id_for_log);
                                    
                                    let execute_handle = tokio::spawn(async move {
                                        command_clone.execute(
                                            &topic_manager_clone,
                                            &task_executor_clone,
                                            &system_config_clone,
                                            &shutdown_token_clone,
                                            &userlog_sender_clone,
                                            &system_control_manager_clone,
                                            &task_id_clone,
                                            &response_channel_clone,
                                            &task_event_sender_clone,
                                            &return_message_sender_clone,
                                        ).await
                                    });
                                    
                                    let execute_abort_handle = execute_handle.abort_handle();
                                    let task_id_for_select = task_id_for_log.clone();
                                    
                                    tokio::select! {
                                        _ = command_shutdown_token.cancelled() => {
                                            log::info!("SystemControl for task {} cancelled due to shutdown", task_id_for_select);
                                            execute_abort_handle.abort();
                                            
                                            let status = SystemResponseStatus::Error;
                                            let cancel_error = SystemResponseEvent::new_system_error(
                                                "system.error".to_string(),
                                                status.to_string(),
                                                "cancelled".to_string(),
                                            );
                                            let _ = response_channel_for_cancel.send(cancel_error);
                                        }
                                        result = execute_handle => {
                                            match result {
                                                Ok(Ok(_)) => {
                                                    log::info!("Successfully executed SystemControl for task {}", task_id_for_select);
                                                }
                                                Ok(Err(e)) => {
                                                    log::error!("Failed to execute SystemControl for task {}: {}", task_id_for_select, e);
                                                    let status = SystemResponseStatus::Error;
                                                    let error_event = SystemResponseEvent::new_system_error(
                                                        "system.error".to_string(),
                                                        status.to_string(),
                                                        e.clone(),
                                                    );
                                                    let _ = response_channel_for_cancel.send(error_event);
                                                }
                                                Err(e) => {
                                                    if e.is_cancelled() {
                                                        log::info!("SystemControl execution was cancelled for task {}", task_id_for_select);
                                                    } else {
                                                        log::error!("SystemControl execution task panicked for task {}: {:?}", task_id_for_select, e);
                                                        let status = SystemResponseStatus::Error;
                                                        let error_event = SystemResponseEvent::new_system_error(
                                                            "system.error".to_string(),
                                                            status.to_string(),
                                                            format!("Task panicked: {:?}", e),
                                                        );
                                                        let _ = response_channel_for_cancel.send(error_event);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    
                                    let mut commands = running_commands_clone.write().await;
                                    commands.remove(&task_id_for_select);
                                });
                                
                                let mut commands = running_commands.write().await;
                                commands.insert(task_id, handle);
                            }
                            None => {
                                log::info!("SystemControl receiver closed");
                                break;
                            }
                        }
                    }
                }
            }
            
            log::info!("SystemControl worker stopped");
    })
}

pub struct TaskSpawner {
    pub task_id: TaskId,
    pub topic_manager: TopicManager,
    pub system_control_manager: SystemControlManager,
    pub task_executor: TaskExecutor,
    pub task_name: String,
    pub userlog_sender: UserLogSender,
}

impl TaskSpawner {
    pub fn new(
        task_id: TaskId,
        topic_manager: TopicManager,
        system_control_manager: SystemControlManager,
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
        backend: Box<dyn TaskBackend>,
        shutdown_token: CancellationToken,
        subscribe_topics: Option<Vec<String>>,
        other_return_message_sender: Option<ExecutorEventSender>,
    ) -> SpawnBackendResult {
        let task_id: TaskId = self.task_id.clone();
        let task_name: String = self.task_name.clone();
        let topic_manager: TopicManager = self.topic_manager;
        let system_control_manager: SystemControlManager = self.system_control_manager;
        let task_executor: TaskExecutor = self.task_executor;
        let userlog_sender = self.userlog_sender.clone();

        let mut backend_handle = match backend.spawn(task_id.clone()).await {
            Ok(handle) => handle,
            Err(e) => {
                log::error!("Failed to spawn task backend for task {}: {}", task_id, e);
                let input_channel: InputChannel = InputChannel::new();
                let shutdown_channel = ShutdownChannel::new();
                return SpawnBackendResult {
                    task_handle: tokio::task::spawn(async {}),
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

        let task_handle = tokio::task::spawn(async move {
            let topic_data_channel: ExecutorEventChannel = ExecutorEventChannel::new();
            let mut topic_data_receiver = topic_data_channel.receiver;

            if let Some(topics) = subscribe_topics {
                log::info!("Processing initial topic subscriptions for task {}: {:?}", task_id, topics);
                for topic in topics {
                    topic_manager.add_subscriber(
                        topic.clone(),
                        task_id.clone(),
                        topic_data_channel.sender.clone()
                    ).await;
                    log::info!("Added initial topic subscription for '{}' from task {}", topic, task_id);
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
                                        if let Some(system_control_cmd) = system_control_command_to_handler(&event) {
                                            log::info!("SystemControl detected from task {}", task_id);
                                            if let Err(e) = system_control_manager.send_system_control_command(
                                                system_control_cmd,
                                                task_id.clone(),
                                                backend_handle.system_response_sender.clone(),
                                                topic_data_channel.sender.clone(),
                                                return_message_channel.sender.clone()
                                            ).await {
                                                log::warn!("Failed to send system control command to worker (task {}): {}", task_id, e);
                                            } else {
                                                log::info!("Sent system control command to worker for task {}", task_id);
                                            }
                                        } else {
                                            log::warn!("Failed to convert SystemControl event to handler for task {}", task_id);
                                        }
                                    },
                                    ExecutorEvent::ReturnMessage { data } => {
                                        log::info!("ReturnMessage received from task {}: '{}'", task_id, data);
                                        if let Some(ref sender) = other_return_message_sender {
                                            if let Err(e) = sender.send(ExecutorEvent::new_return_message(data.clone())) {
                                                log::warn!("Failed to send return message to other_return_message_sender for task {}: {}", task_id, e);
                                            }
                                        } else {
                                            log::warn!("ReturnMessage received but other_return_message_sender is not available for task {}", task_id);
                                        }
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
                                log::info!("Return message received for task {}: {:?}", task_id, message);
                                if let Some(data) = message.data() {
                                    let return_msg = ReturnMessage {
                                        data: data.clone(),
                                    };
                                    if let Err(e) = backend_handle.input_sender.send(InputDataMessage::Return(return_msg)) {
                                        log::warn!("Failed to send return message to task backend for task {}: {}", task_id, e);
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
            task_handle,
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
}

impl TaskExecutor {
    pub fn new() -> Self {
        Self {
            running_tasks: Arc::new(RwLock::new(HashMap::new())),
            name_to_id: Arc::new(RwLock::new(HashMap::new())),
            id_to_name: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register_task(&self, task_name: String, task: RunningTask) -> Result<(), String> {
        let mut running_tasks = self.running_tasks.write().await;
        let mut name_to_id = self.name_to_id.write().await;
        let mut id_to_name = self.id_to_name.write().await;
        let task_id = task.task_id.clone();
        
        running_tasks.insert(task_id.clone(), task);
        name_to_id.entry(task_name.clone()).or_insert_with(Vec::new).push(task_id.clone());
        id_to_name.insert(task_id, task_name);
        Ok(())
    }

    pub async fn try_register_task(&self, task_name: String, task: RunningTask) -> Result<(), String> {
        let name_to_id = self.name_to_id.read().await;
        if let Some(ids) = name_to_id.get(&task_name) {
            if !ids.is_empty() {
                log::warn!("Task '{}' is already running - cancelling new task start", task_name);
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
            // Vecから該当のtask_idを削除
            for ids in name_to_id.values_mut() {
                ids.retain(|id| id != task_id);
            }
            // 空になったVecのエントリを削除
            name_to_id.retain(|_name, ids| !ids.is_empty());
            id_to_name.remove(task_id);
            Some(task)
        } else {
            None
        }
    }
    pub async fn unregister_task_by_name(&self, task_name: &str) -> Option<RunningTask> {
        let id = {
            let name_to_id = self.name_to_id.read().await;
            name_to_id.get(task_name)
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
                log::info!("Stopped task with TaskId={} (Human name index updated)", task_id);
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
            },
            None => {
                log::warn!("Attempted to stop non-running task TaskId={}", task_id);
                Err(anyhow::anyhow!("TaskId '{}' is not running", task_id))
            }
        }
    }

    pub async fn stop_task_by_name(&self, task_name: &str) -> Result<()> {
        let id = {
            let name_to_id = self.name_to_id.read().await;
            name_to_id.get(task_name)
                .and_then(|ids| ids.last())
                .cloned()
        };
        if let Some(tid) = id {
            self.stop_task_by_task_id(&tid).await
        } else {
            Err(anyhow::anyhow!("Task name '{}' not found", task_name))
        }
    }

    pub async fn get_running_tasks_info(&self) -> Vec<(String, TaskId)> {
        let name_to_id = self.name_to_id.read().await;
        name_to_id.iter()
            .filter_map(|(name, ids)| ids.last().map(|task_id| (name.clone(), task_id.clone())))
            .collect()
    }

    pub async fn get_view_flags_by_task_id(&self, task_id: &TaskId) -> Option<(bool, bool)> {
        let running_tasks_guard = self.running_tasks.read().await;
        running_tasks_guard.get(task_id).map(|t| (t.view_stdout, t.view_stderr))
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

    pub async fn start_task_from_config(
        &self,
        context: ReadyStartContext,
    ) -> Result<()> {
        let task_config = &context.task_config;
        log::info!("Starting task '{}'", task_config.name);
        
        if !std::path::Path::new(&task_config.command).exists() && !which::which(&task_config.command).is_ok() {
            return Err(anyhow::anyhow!("Command '{}' not found in PATH or file system", task_config.command));
        }

        if let Some(working_dir) = &task_config.working_directory {
            if !std::path::Path::new(working_dir).exists() {
                return Err(anyhow::anyhow!("Working directory '{}' does not exist", working_dir));
            }
        }
        
        let task_id_new = TaskId::new();
        
        let backend: Box<dyn TaskBackend> = Box::new(ShellBackend::new(
            task_config.command.clone(),
            task_config.args.clone(),
            task_config.working_directory.clone(),
            task_config.environment_vars.clone(),
            task_config.get_stdout_topic(),
            task_config.get_stderr_topic(),
            task_config.view_stdout,
            task_config.view_stderr,
        ));
        let subscribe_topics = task_config.subscribe_topics.clone();
        
        let task_spawner = TaskSpawner::new(
            task_id_new.clone(),
            context.topic_manager,
            context.system_control_manager,
            self.clone(),
            task_config.name.clone(),
            context.userlog_sender,
        );
        
        let return_message_sender = match &context.variant {
            StartContextVariant::Tasks => None,
            StartContextVariant::Functions { return_message_sender, .. } => Some(return_message_sender.clone()),
        };
        
        let spawn_result = task_spawner.spawn_backend(
            backend,
            context.shutdown_token,
            subscribe_topics,
            return_message_sender,
        ).await;
        
        let running_task = RunningTask {
            task_id: task_id_new.clone(),
            shutdown_sender: spawn_result.shutdown_sender.clone(),
            input_sender: spawn_result.input_sender.clone(),
            task_handle: spawn_result.task_handle,
            view_stdout: task_config.view_stdout,
            view_stderr: task_config.view_stderr,
        };

        match &context.variant {
            StartContextVariant::Functions { initial_input, caller_task_name, .. } => {
                if let Some(initial_input) = initial_input {
                    let input_sender_for_initial = spawn_result.input_sender.clone();
                    let initial_input_for_log = initial_input.clone();
                    let caller_name = caller_task_name.clone().unwrap_or_else(|| "unknown".to_string());
                    let task_id_for_log = task_id_new.clone();
                    tokio::task::spawn(async move {
                        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                        
                        let function_msg = FunctionMessage {
                            task_name: caller_name.clone(),
                            data: initial_input_for_log.clone(),
                        };
                        
                        if let Err(e) = input_sender_for_initial.send(InputDataMessage::Function(function_msg)) {
                            log::warn!("Failed to send function message to task {}: {}", task_id_for_log, e);
                        }
                        
                        log::info!("Sent function message to {}: caller='{}', data='{}'", task_id_for_log, caller_name, initial_input_for_log);
                    });
                }
                
                if let Err(e) = self.register_task(task_config.name.clone(), running_task).await {
                    return Err(anyhow::anyhow!("Failed to register task '{}': {}", task_config.name, e));
                }
                log::debug!("Registered function '{}' (ID: {}) - duplicate instances allowed", task_config.name, task_id_new);
            }
            StartContextVariant::Tasks => {
                if let Err(e) = self.try_register_task(task_config.name.clone(), running_task).await {
                    return Err(anyhow::anyhow!("Failed to register task '{}': {}", task_config.name, e));
                }
            }
        }
        
        log::info!("Successfully started task '{}' (ID: {})", task_config.name, task_id_new);
        Ok(())
    }
}

impl TaskExecutor {
    pub async fn start_single_task(
        &self,
        context: StartContext,
    ) -> Result<()> {
        match context.variant {
            StartContextVariant::Tasks => {
                if let Some(task_config) = context.config.tasks.iter().find(|t| t.name == context.task_name) {
                    let config_context = context.to_ready_context(task_config.clone());
                    return self.start_task_from_config(config_context).await;
                }
            }
            StartContextVariant::Functions { .. } => {
                if let Some(task_config) = context.config.functions.iter().find(|t| t.name == context.task_name) {
                    let config_context = context.to_ready_context(task_config.clone());
                    return self.start_task_from_config(config_context).await;
                }
            }
        }

        Err(anyhow::anyhow!("Task '{}' not found in configuration", context.task_name))
    }

}

pub struct MiclowSystem {
    pub config: SystemConfig,
    topic_manager: TopicManager,
    system_control_manager: SystemControlManager,
    task_executor: TaskExecutor,
    shutdown_token: CancellationToken,
    background_tasks: BackgroundTaskManager,
}

impl MiclowSystem {
    pub fn new(config: SystemConfig) -> Self {
        let topic_manager: TopicManager = TopicManager::new();
        let shutdown_token: CancellationToken = CancellationToken::new();
        let system_control_manager: SystemControlManager = SystemControlManager::new(shutdown_token.clone());
        let task_executor: TaskExecutor = TaskExecutor::new();
        Self {
            config,
            topic_manager,
            system_control_manager,
            task_executor,
            shutdown_token,
            background_tasks: BackgroundTaskManager::new(),
        }
    }

    async fn start_user_tasks(
        config: &SystemConfig,
        task_executor: &TaskExecutor,
        topic_manager: TopicManager,
        system_control_manager: SystemControlManager,
        shutdown_token: CancellationToken,
        userlog_sender: UserLogSender,
    ) {
        let tasks: Vec<&TaskConfig> = config.get_all_tasks();

        for task_config in tasks.iter() {
            let task_name: String = task_config.name.clone();

            let start_context = StartContext {
                task_name: task_name.clone(),
                config: config.clone(),
                topic_manager: topic_manager.clone(),
                system_control_manager: system_control_manager.clone(),
                shutdown_token: shutdown_token.clone(),
                userlog_sender: userlog_sender.clone(),
                variant: StartContextVariant::Tasks,
            };

            match task_executor.start_single_task(start_context).await {
                Ok(_) => {
                    log::info!("Started user task {} with command: {} {}",
                          task_name,
                          task_config.command,
                          task_config.args.join(" "));
                },
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

    pub async fn start_system_with_interactive(
        mut self,
    ) -> Result<()> {
        let topic_manager: TopicManager = self.topic_manager.clone();

        let (log_tx, log_rx) = tokio::sync::mpsc::unbounded_channel::<LogEvent>();
        let _ = set_channel_logger(log_tx, level_from_env());
        let logging_shutdown = CancellationToken::new();
        let h_log = spawn_log_aggregator(log_rx, logging_shutdown.clone());
        self.background_tasks.register("log_aggregator", h_log);

        let (userlog_tx, userlog_rx) = mpsc::unbounded_channel::<UserLogEvent>();
        let userlog_sender = UserLogSender::new(userlog_tx);
        let h_userlog = spawn_user_log_aggregator(userlog_rx, logging_shutdown.clone());
        self.background_tasks.register("user_log_aggregator", h_userlog);

        let h_sys = start_system_control_worker(
            self.system_control_manager.clone(),
            topic_manager.clone(),
            self.task_executor.clone(),
            self.config.clone(),
            self.shutdown_token.clone(),
            userlog_sender.clone(),
        );
        self.background_tasks.register("system_control_worker", h_sys);

        Self::start_user_tasks(
            &self.config,
            &self.task_executor,
            topic_manager.clone(),
            self.system_control_manager.clone(),
            self.shutdown_token.clone(),
            userlog_sender.clone(),
        ).await;
        
        log::info!("System running. Press Ctrl+C to stop.");
        println!("System running. Press Ctrl+C to stop.");
        
        let shutdown_token = self.shutdown_token.clone();
        let interactive_task_id = TaskId::new();
        let interactive_backend: Box<dyn TaskBackend> = Box::new(InteractiveBackend::new("system".to_string()));
        let interactive_task_spawner = TaskSpawner::new(
            interactive_task_id.clone(),
            topic_manager.clone(),
            self.system_control_manager.clone(),
            self.task_executor.clone(),
            "interactive".to_string(),
            userlog_sender.clone(),
        );
        let interactive_result = interactive_task_spawner.spawn_backend(
            interactive_backend,
            self.shutdown_token.clone(),
            None,
            None,
        ).await;
        
        let mut interactive_handle = interactive_result.task_handle;
        
        let ctrlc_fut = async {
            if let Err(e) = tokio::signal::ctrl_c().await {
                log::error!("Ctrl+C signal error: {}", e);
            } else {
                log::info!("Received Ctrl+C. Requesting graceful shutdown...");
                shutdown_token.cancel();
            }
        };
        tokio::select! {
            _ = &mut interactive_handle => {
                log::info!("Interactive mode terminated");
            },
            _ = ctrlc_fut => {
                log::info!("Ctrl+C signal received, proceeding with shutdown");
            },
        }

        log::info!("Received shutdown signal, stopping all workers...");
        
        Self::shutdown_workers(
            self.task_executor,
            self.shutdown_token,
        ).await;

        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        logging_shutdown.cancel();
        if !interactive_handle.is_finished() {
            interactive_handle.abort();
            let _ = interactive_handle.await;
        }
        self.background_tasks.abort_all().await;
        log::logger().flush();

        log::info!("Graceful shutdown completed");
        println!("Graceful shutdown completed");
        return Ok(());
    }

    async fn shutdown_workers(
        task_executor: TaskExecutor,
        shutdown_token: CancellationToken,
    ) {
        log::info!("Starting graceful shutdown...");

        log::info!("Cancelling shutdown token");
        shutdown_token.cancel();

        log::info!("Waiting for running tasks to finish...");
        task_executor.graceful_shutdown_all(std::time::Duration::from_secs(5)).await;

        log::info!("All user tasks stopped");
    }
}