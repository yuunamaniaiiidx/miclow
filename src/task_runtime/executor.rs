use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::backend::ProtocolBackend;
use crate::message_id::MessageId;
use crate::messages::ExecutorInputEvent;
use crate::task_id::TaskId;

use super::running_task::RunningTask;
use super::spawner::TaskSpawner;
use super::start_context::StartContext;

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
            let task_name = id_to_name.get(task_id).cloned();

            for ids in name_to_id.values_mut() {
                ids.retain(|id| id != task_id);
            }
            name_to_id.retain(|_name, ids| !ids.is_empty());
            id_to_name.remove(task_id);

            if let Some(name) = task_name {
                let mut monitored = self.monitored_task_names.write().await;
                if monitored.remove(&name) {
                    if monitored.is_empty() {
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
                for ids in name_to_id.values_mut() {
                    ids.retain(|id| id != task_id);
                }
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

        if let ProtocolBackend::MiclowStdin(ref config) = backend {
            if !Path::new(&config.command).exists() && !which::which(&config.command).is_ok() {
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
                context
                    .parent_invocation
                    .as_ref()
                    .map(|parent| parent.return_channel.clone()),
            )
            .await;

        let (view_stdout, view_stderr) = match &backend {
            ProtocolBackend::MiclowStdin(config) => (config.view_stdout, config.view_stderr),
            ProtocolBackend::Interactive(_) => (false, false),
            ProtocolBackend::McpServer(_) => (false, false),
        };

        let running_task = RunningTask {
            task_id: task_id_new.clone(),
            shutdown_sender: spawn_result.shutdown_sender.clone(),
            input_sender: spawn_result.input_sender.clone(),
            task_handle: spawn_result.worker_handle,
            view_stdout,
            view_stderr,
        };

        if let Some(parent_invocation) = &context.parent_invocation {
            let input_sender_for_initial = spawn_result.input_sender.clone();
            let initial_input_for_log = parent_invocation
                .initial_input
                .clone()
                .unwrap_or_else(|| "".to_string());
            let task_id_for_log = task_id_new.clone();
            tokio::task::spawn(async move {
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

                if let Err(e) = input_sender_for_initial.send(ExecutorInputEvent::Function {
                    message_id: MessageId::new(),
                    data: initial_input_for_log.clone(),
                }) {
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
