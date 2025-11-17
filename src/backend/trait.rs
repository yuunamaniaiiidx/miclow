use super::handle::TaskBackendHandle;
use crate::task_id::TaskId;
use anyhow::Error;
use async_trait::async_trait;

#[async_trait]
pub trait TaskBackend: Send + Sync {
    async fn spawn(&self, task_id: TaskId) -> Result<TaskBackendHandle, Error>;
}
