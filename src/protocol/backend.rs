use anyhow::{Error, Result};
use std::convert::TryFrom;
use async_trait::async_trait;
use crate::task_id::TaskId;
use crate::task_backend::TaskBackend;
use crate::task_backend_handle::TaskBackendHandle;
use crate::config::TaskConfig;
use crate::protocol::{self, MiclowStdinConfig, InteractiveConfig, McpServerConfig};

#[derive(Clone)]
pub enum ProtocolBackend {
    MiclowStdin(MiclowStdinConfig),
    Interactive(InteractiveConfig),
    McpServer(McpServerConfig),
}

impl TryFrom<TaskConfig> for ProtocolBackend {
    type Error = anyhow::Error;

    fn try_from(config: TaskConfig) -> Result<Self, Self::Error> {
        let protocol = config.protocol.trim();
        
        if protocol.is_empty() {
            return Err(anyhow::anyhow!("Protocol field is required but was empty for task '{}'", config.name));
        }
        
        match protocol {
            "MiclowStdin" => {
                let config = protocol::try_miclow_stdin_from_task_config(&config)?;
                Ok(ProtocolBackend::MiclowStdin(config))
            }
            "Interactive" => {
                let config = protocol::try_interactive_from_task_config(&config)?;
                Ok(ProtocolBackend::Interactive(config))
            }
            "McpServer" => {
                let config = protocol::try_mcp_server_from_task_config(&config)?;
                Ok(ProtocolBackend::McpServer(config))
            }
            _ => {
                Err(anyhow::anyhow!("Unknown protocol '{}' for task '{}'. Supported protocols: MiclowStdin, Interactive, McpServer", protocol, config.name))
            }
        }
    }
}

#[async_trait]
impl TaskBackend for ProtocolBackend {
    async fn spawn(&self, task_id: TaskId) -> Result<TaskBackendHandle, Error> {
        match self {
            ProtocolBackend::MiclowStdin(config) => {
                protocol::spawn_miclow_protocol(config, task_id).await
            }
            ProtocolBackend::Interactive(config) => {
                protocol::spawn_interactive_protocol(config, task_id).await
            }
            ProtocolBackend::McpServer(config) => {
                protocol::spawn_mcp_protocol(config, task_id).await
            }
        }
    }
}


