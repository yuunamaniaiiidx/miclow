use crate::backend::interactive::{
    spawn_interactive_protocol, try_interactive_from_task_config, InteractiveConfig,
};
use crate::backend::mcp_server::{
    spawn_mcp_stdio_protocol, spawn_mcp_tcp_protocol, try_mcp_server_stdio_from_task_config,
    try_mcp_server_tcp_from_task_config, McpServerStdIOConfig, McpServerTcpConfig,
};
use crate::backend::miclowstdio::{
    spawn_miclow_stdio_protocol, try_miclow_stdio_from_task_config, MiclowStdIOConfig,
};
use crate::backend::{TaskBackend, TaskBackendHandle};
use crate::config::TaskConfig;
use crate::task_id::TaskId;
use anyhow::{Error, Result};
use async_trait::async_trait;
use std::convert::TryFrom;

#[derive(Clone)]
pub enum ProtocolBackend {
    MiclowStdIO(MiclowStdIOConfig),
    Interactive(InteractiveConfig),
    McpServerStdIO(McpServerStdIOConfig),
    McpServerTcp(McpServerTcpConfig),
}

impl TryFrom<TaskConfig> for ProtocolBackend {
    type Error = anyhow::Error;

    fn try_from(config: TaskConfig) -> Result<Self, Self::Error> {
        let protocol = config.protocol.trim();

        if protocol.is_empty() {
            return Err(anyhow::anyhow!(
                "Protocol field is required but was empty for task '{}'",
                config.name
            ));
        }

        match protocol {
            "MiclowStdIO" => {
                let config = try_miclow_stdio_from_task_config(&config)?;
                Ok(ProtocolBackend::MiclowStdIO(config))
            }
            "Interactive" => {
                let config = try_interactive_from_task_config(&config)?;
                Ok(ProtocolBackend::Interactive(config))
            }
            "McpServerStdIO" => {
                let config = try_mcp_server_stdio_from_task_config(&config)?;
                Ok(ProtocolBackend::McpServerStdIO(config))
            }
            "McpServerTcp" => {
                let config = try_mcp_server_tcp_from_task_config(&config)?;
                Ok(ProtocolBackend::McpServerTcp(config))
            }
            _ => {
                Err(anyhow::anyhow!("Unknown protocol '{}' for task '{}'. Supported protocols: MiclowStdIO, Interactive, McpServerStdIO, McpServerTcp", protocol, config.name))
            }
        }
    }
}

#[async_trait]
impl TaskBackend for ProtocolBackend {
    async fn spawn(&self, task_id: TaskId) -> Result<TaskBackendHandle, Error> {
        match self {
            ProtocolBackend::MiclowStdIO(config) => {
                spawn_miclow_stdio_protocol(config, task_id).await
            }
            ProtocolBackend::Interactive(config) => {
                spawn_interactive_protocol(config, task_id).await
            }
            ProtocolBackend::McpServerStdIO(config) => {
                spawn_mcp_stdio_protocol(config, task_id).await
            }
            ProtocolBackend::McpServerTcp(config) => spawn_mcp_tcp_protocol(config, task_id).await,
        }
    }
}
