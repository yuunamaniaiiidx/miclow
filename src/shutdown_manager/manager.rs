use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use tokio::sync::{mpsc, Mutex as AsyncMutex};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use super::layer::ShutdownLayer;
use super::participant::{DynShutdownParticipant, ShutdownParticipant};
use super::task::{TaskHandle, TaskInfo};

#[derive(Clone)]
struct ParticipantEntry {
    name: String,
    layer: ShutdownLayer,
    participant: DynShutdownParticipant,
}

pub struct TaskShutdownResult {
    pub name: String,
    pub layer: ShutdownLayer,
    pub finished_in_time: bool,
}

pub struct ParticipantShutdownResult {
    pub name: String,
    pub layer: ShutdownLayer,
    pub finished_in_time: bool,
    pub error: Option<String>,
}

pub struct ShutdownReport {
    pub participants: Vec<ParticipantShutdownResult>,
    pub tasks: Vec<TaskShutdownResult>,
}

impl ShutdownReport {
    pub fn is_clean(&self) -> bool {
        self.participants
            .iter()
            .all(|p| p.finished_in_time && p.error.is_none())
            && self.tasks.iter().all(|t| t.finished_in_time)
    }
}

pub struct ShutdownManager {
    root_token: CancellationToken,
    tasks: Arc<AsyncMutex<Vec<TaskInfo>>>,
    handle_sender: mpsc::UnboundedSender<TaskInfo>,
    registry_task: AsyncMutex<Option<JoinHandle<()>>>,
    participants: StdMutex<Vec<ParticipantEntry>>,
}

impl ShutdownManager {
    pub fn new() -> Self {
        Self::with_shutdown_token(CancellationToken::new())
    }

    pub fn with_shutdown_token(root_token: CancellationToken) -> Self {
        let (handle_sender, mut handle_receiver) = mpsc::unbounded_channel();
        let tasks = Arc::new(AsyncMutex::new(Vec::new()));
        let tasks_clone = Arc::clone(&tasks);
        let registry_task = tokio::spawn(async move {
            while let Some(task_info) = handle_receiver.recv().await {
                tasks_clone.lock().await.push(task_info);
            }
        });

        Self {
            root_token,
            tasks,
            handle_sender,
            registry_task: AsyncMutex::new(Some(registry_task)),
            participants: StdMutex::new(Vec::new()),
        }
    }

    pub fn root_token(&self) -> &CancellationToken {
        &self.root_token
    }

    pub fn task_handle(&self, layer: ShutdownLayer) -> TaskHandle {
        TaskHandle::new(
            self.handle_sender.clone(),
            self.root_token.child_token(),
            layer,
        )
    }
    pub fn register_participant<P>(&self, participant: P)
    where
        P: ShutdownParticipant + 'static,
    {
        let entry = ParticipantEntry {
            name: participant.name().to_string(),
            layer: participant.layer(),
            participant: Arc::new(participant),
        };
        let mut guard = self
            .participants
            .lock()
            .expect("shutdown participants poisoned");
        guard.push(entry);
    }

    pub fn register_dyn_participant(&self, participant: DynShutdownParticipant) {
        let entry = ParticipantEntry {
            name: participant.name().to_string(),
            layer: participant.layer(),
            participant,
        };
        let mut guard = self
            .participants
            .lock()
            .expect("shutdown participants poisoned");
        guard.push(entry);
    }

    pub async fn shutdown_all(&self, grace: Duration) -> ShutdownReport {
        self.root_token.cancel();
        let participant_results = self.shutdown_participants(grace).await;
        let task_results = self.shutdown_tasks(grace).await;

        if let Some(registry) = self.registry_task.lock().await.take() {
            registry.abort();
        }

        ShutdownReport {
            participants: participant_results,
            tasks: task_results,
        }
    }

    pub async fn abort_all(&self) {
        self.root_token.cancel();
        let mut tasks = self.tasks.lock().await;
        for task in tasks.drain(..) {
            task.cancel_token.cancel();
            task.handle.abort();
        }

        if let Some(registry) = self.registry_task.lock().await.take() {
            registry.abort();
        }
    }

    async fn shutdown_participants(&self, grace: Duration) -> Vec<ParticipantShutdownResult> {
        let entries = {
            let mut guard = self
                .participants
                .lock()
                .expect("shutdown participants poisoned");
            guard.sort_by(|a, b| b.layer.cmp(&a.layer));
            guard.clone()
        };

        let mut results = Vec::with_capacity(entries.len());
        for entry in entries {
            let cancellation = self.root_token.child_token();
            let layer = entry.layer;
            let name = entry.name.clone();
            let participant = Arc::clone(&entry.participant);
            match timeout(grace, participant.shutdown(cancellation)).await {
                Ok(Ok(())) => {
                    log::info!(
                        "Shutdown participant {} [{}] completed",
                        name,
                        layer.label()
                    );
                    results.push(ParticipantShutdownResult {
                        name,
                        layer,
                        finished_in_time: true,
                        error: None,
                    });
                }
                Ok(Err(err)) => {
                    log::error!(
                        "Shutdown participant {} [{}] returned error: {}",
                        name,
                        layer.label(),
                        err
                    );
                    results.push(ParticipantShutdownResult {
                        name,
                        layer,
                        finished_in_time: true,
                        error: Some(err.to_string()),
                    });
                }
                Err(_) => {
                    log::warn!(
                        "Shutdown participant {} [{}] timed out after {:?}",
                        name,
                        layer.label(),
                        grace
                    );
                    results.push(ParticipantShutdownResult {
                        name,
                        layer,
                        finished_in_time: false,
                        error: Some("timeout".into()),
                    });
                }
            }
        }

        results
    }

    async fn shutdown_tasks(&self, grace: Duration) -> Vec<TaskShutdownResult> {
        let mut task_infos = {
            let mut guard = self.tasks.lock().await;
            guard.drain(..).collect::<Vec<_>>()
        };

        if task_infos.is_empty() {
            return Vec::new();
        }

        task_infos.sort_by(|a, b| b.layer.cmp(&a.layer));

        for info in &task_infos {
            info.cancel_token.cancel();
        }

        let mut results = Vec::with_capacity(task_infos.len());
        for info in task_infos {
            let TaskInfo {
                name,
                layer,
                handle,
                completion,
                ..
            } = info;
            let finished = match timeout(grace, completion).await {
                Ok(Ok(())) => true,
                Ok(Err(_)) => true,
                Err(_) => false,
            };

            if !finished {
                log::warn!(
                    "Task {} [{}] did not finish within {:?}, aborting",
                    name,
                    layer.label(),
                    grace
                );
                handle.abort();
            } else {
                log::info!("Task {} [{}] finished cleanly", name, layer.label());
            }

            results.push(TaskShutdownResult {
                name,
                layer,
                finished_in_time: finished,
            });
        }

        results
    }
}
