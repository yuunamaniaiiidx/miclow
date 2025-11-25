use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::layer::ShutdownLayer;

#[derive(Debug)]
pub struct TaskInfo {
    pub name: String,
    pub layer: ShutdownLayer,
    pub handle: Arc<JoinHandle<()>>,
    pub cancel_token: CancellationToken,
    pub completion: oneshot::Receiver<()>,
}

#[derive(Clone)]
pub struct TaskHandle {
    handle_sender: mpsc::UnboundedSender<TaskInfo>,
    cancel_token: CancellationToken,
    layer: ShutdownLayer,
}

#[derive(Clone)]
pub struct TaskController {
    name: Arc<str>,
    layer: ShutdownLayer,
    handle: Arc<JoinHandle<()>>,
    cancel_token: CancellationToken,
}

impl TaskHandle {
    pub fn new(
        handle_sender: mpsc::UnboundedSender<TaskInfo>,
        parent_token: CancellationToken,
        layer: ShutdownLayer,
    ) -> Self {
        Self {
            handle_sender,
            cancel_token: parent_token.child_token(),
            layer,
        }
    }

    pub fn run<F, Fut>(
        &self,
        name: impl Into<String>,
        task: F,
    ) -> Result<TaskController, mpsc::error::SendError<TaskInfo>>
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let name_string = name.into();
        let task_token = self.cancel_token.child_token();
        let task_token_for_run = task_token.clone();
        let future = task(task_token.clone());
        let (completion_tx, completion_rx) = oneshot::channel();

        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = task_token_for_run.cancelled() => {},
                _ = future => {},
            }
            let _ = completion_tx.send(());
        });

        let handle_arc = Arc::new(handle);
        self.handle_sender.send(TaskInfo {
            name: name_string.clone(),
            layer: self.layer,
            handle: handle_arc.clone(),
            cancel_token: task_token.clone(),
            completion: completion_rx,
        })?;

        Ok(TaskController::new(
            name_string,
            self.layer,
            handle_arc,
            task_token,
        ))
    }

    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }

    pub fn layer(&self) -> ShutdownLayer {
        self.layer
    }
}

impl TaskController {
    fn new(
        name: String,
        layer: ShutdownLayer,
        handle: Arc<JoinHandle<()>>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            name: Arc::from(name),
            layer,
            handle,
            cancel_token,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn layer(&self) -> ShutdownLayer {
        self.layer
    }

    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    pub fn request_shutdown(&self) {
        self.cancel_token.cancel();
    }

    pub fn abort(&self) {
        self.handle.abort();
    }

    pub fn join_handle(&self) -> Arc<JoinHandle<()>> {
        self.handle.clone()
    }
}
