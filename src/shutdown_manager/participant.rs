use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::layer::ShutdownLayer;

#[async_trait]
pub trait ShutdownParticipant: Send + Sync {
    fn name(&self) -> &str;
    fn layer(&self) -> ShutdownLayer;
    async fn shutdown(&self, token: CancellationToken) -> Result<()>;
}

pub type DynShutdownParticipant = Arc<dyn ShutdownParticipant>;
