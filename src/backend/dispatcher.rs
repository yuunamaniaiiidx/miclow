use crate::backend::config::BackendConfigMeta;
use crate::backend::handle::TaskBackendHandle;
use crate::backend::interactive::config::InteractiveConfig;
use crate::backend::interactive::spawn_interactive_protocol;
use crate::backend::mcp_server::config::{McpServerStdIOConfig, McpServerTcpConfig};
use crate::backend::mcp_server::{spawn_mcp_stdio_protocol, spawn_mcp_tcp_protocol};
use crate::backend::miclowstdio::config::MiclowStdIOConfig;
use crate::backend::miclowstdio::spawn_miclow_stdio_protocol;
use crate::task_id::TaskId;
use anyhow::{Error, Result};
use async_trait::async_trait;

#[async_trait]
pub trait TaskBackend: Send + Sync {
    async fn spawn(
        &self,
        task_id: TaskId,
        caller_task_id: Option<TaskId>,
    ) -> Result<TaskBackendHandle, Error>;
}

#[derive(Debug, Clone)]
pub enum ProtocolBackend {
    MiclowStdIO(MiclowStdIOConfig),
    Interactive(InteractiveConfig),
    McpServerStdIO(McpServerStdIOConfig),
    McpServerTcp(McpServerTcpConfig),
}

impl ProtocolBackend {
    /// プロトコル名を文字列として取得
    pub fn protocol_name(&self) -> &'static str {
        match self {
            ProtocolBackend::MiclowStdIO(_) => MiclowStdIOConfig::protocol_name(),
            ProtocolBackend::Interactive(_) => InteractiveConfig::protocol_name(),
            ProtocolBackend::McpServerStdIO(_) => McpServerStdIOConfig::protocol_name(),
            ProtocolBackend::McpServerTcp(_) => McpServerTcpConfig::protocol_name(),
        }
    }
}

#[async_trait]
impl TaskBackend for ProtocolBackend {
    async fn spawn(
        &self,
        task_id: TaskId,
        caller_task_id: Option<TaskId>,
    ) -> Result<TaskBackendHandle, Error> {
        match self {
            ProtocolBackend::MiclowStdIO(config) => {
                spawn_miclow_stdio_protocol(config, task_id, caller_task_id).await
            }
            ProtocolBackend::Interactive(config) => {
                spawn_interactive_protocol(config, task_id).await
            }
            ProtocolBackend::McpServerStdIO(config) => {
                spawn_mcp_stdio_protocol(config, task_id, caller_task_id).await
            }
            ProtocolBackend::McpServerTcp(config) => {
                spawn_mcp_tcp_protocol(config, task_id, caller_task_id).await
            }
        }
    }
}
