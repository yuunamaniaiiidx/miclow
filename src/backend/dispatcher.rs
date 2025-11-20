use crate::backend::handle::TaskBackendHandle;
use crate::backend::interactive::{spawn_interactive_protocol, InteractiveConfig};
use crate::backend::miclowstdio::{spawn_miclow_stdio_protocol, MiclowStdIOConfig};
use crate::pod::PodId;
use anyhow::{Error, Result};
use async_trait::async_trait;

#[async_trait]
pub trait TaskBackend: Send + Sync {
    async fn spawn(&self, task_id: PodId) -> Result<TaskBackendHandle, Error>;
}

#[derive(Debug, Clone)]
pub enum ProtocolBackend {
    MiclowStdIO(MiclowStdIOConfig),
    Interactive(InteractiveConfig),
}

#[async_trait]
impl TaskBackend for ProtocolBackend {
    async fn spawn(&self, task_id: PodId) -> Result<TaskBackendHandle, Error> {
        match self {
            ProtocolBackend::MiclowStdIO(config) => {
                spawn_miclow_stdio_protocol(config, task_id).await
            }
            ProtocolBackend::Interactive(config) => {
                spawn_interactive_protocol(config, task_id).await
            }
        }
    }
}
