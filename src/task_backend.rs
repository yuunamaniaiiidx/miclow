use anyhow::Error;
use async_trait::async_trait;
use crate::task_id::TaskId;
use crate::task_backend_handle::TaskBackendHandle;

#[async_trait]
pub trait TaskBackend: Send + Sync {
    async fn spawn(&self, task_id: TaskId) -> Result<TaskBackendHandle, Error>;
}

