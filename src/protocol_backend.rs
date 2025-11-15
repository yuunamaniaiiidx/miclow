use anyhow::{Error, Result};
use std::convert::TryFrom;
use async_trait::async_trait;
use std::collections::HashMap;
use crate::task_id::TaskId;
use crate::task_backend::TaskBackend;
use crate::task_backend_handle::TaskBackendHandle;
use crate::executor_event_channel::ExecutorEvent;
use crate::config::TaskConfig;
use crate::miclow_protocol;
use crate::interactive_protocol;

#[derive(Clone)]
pub struct MiclowStdinConfig {
    pub command: String,
    pub args: Vec<String>,
    pub working_directory: Option<String>,
    pub environment_vars: Option<HashMap<String, String>>,
    pub stdout_topic: String,
    pub stderr_topic: String,
    pub view_stdout: bool,
    pub view_stderr: bool,
}

#[derive(Clone)]
pub struct InteractiveConfig {
    pub system_input_topic: String,
}

impl InteractiveConfig {
    pub fn new(system_input_topic: String) -> Self {
        Self { system_input_topic }
    }
}

#[derive(Clone)]
pub enum ProtocolBackend {
    MiclowStdin(MiclowStdinConfig),
    Interactive(InteractiveConfig),
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
                if config.command.is_empty() {
                    return Err(anyhow::anyhow!("Command field is required for MiclowStdin in task '{}'", config.name));
                }
                
                // デフォルト値の生成ロジック: stdout_topic/stderr_topicが未設定の場合は"{name}.stdout"/"{name}.stderr"を使用
                let stdout_topic = config.stdout_topic.clone()
                    .unwrap_or_else(|| format!("{}.stdout", config.name));
                let stderr_topic = config.stderr_topic.clone()
                    .unwrap_or_else(|| format!("{}.stderr", config.name));
                
                Ok(ProtocolBackend::MiclowStdin(MiclowStdinConfig {
                    command: config.command,
                    args: config.args,
                    working_directory: config.working_directory,
                    environment_vars: config.environment_vars,
                    stdout_topic,
                    stderr_topic,
                    view_stdout: config.view_stdout,
                    view_stderr: config.view_stderr,
                }))
            }
            "Interactive" => {
                // InteractiveProtocol用のシステム入力トピック: stdout_topicが未設定の場合は"system"を使用
                let system_input_topic = config.stdout_topic.clone()
                    .unwrap_or_else(|| "system".to_string());
                
                Ok(ProtocolBackend::Interactive(InteractiveConfig {
                    system_input_topic,
                }))
            }
            _ => {
                Err(anyhow::anyhow!("Unknown protocol '{}' for task '{}'. Supported protocols: MiclowProtocol, InteractiveProtocol", protocol, config.name))
            }
        }
    }
}

#[async_trait]
impl TaskBackend for ProtocolBackend {
    async fn spawn(&self, task_id: TaskId) -> Result<TaskBackendHandle, Error> {
        match self {
            ProtocolBackend::MiclowStdin(config) => {
                miclow_protocol::spawn_miclow_protocol(config, task_id).await
            }
            ProtocolBackend::Interactive(config) => {
                interactive_protocol::spawn_interactive_protocol(config, task_id).await
            }
        }
    }
}


