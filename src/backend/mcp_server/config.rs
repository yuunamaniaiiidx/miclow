use crate::backend::config::BackendConfigMeta;
use crate::config::ExpandedTaskConfig;
use anyhow::{bail, Result};
use std::collections::HashMap;
use toml::Value as TomlValue;

#[derive(Clone, Debug)]
pub struct McpServerStdIOConfig {
    pub command: String,
    pub args: Vec<String>,
    pub working_directory: Option<String>,
    pub environment_vars: Option<HashMap<String, String>>,
}

impl BackendConfigMeta for McpServerStdIOConfig {
    fn protocol_name() -> &'static str {
        "McpServerStdIO"
    }

    fn default_allow_duplicate() -> bool {
        false
    }

    fn default_auto_start() -> bool {
        true
    }

    fn default_view_stdout() -> bool {
        false
    }

    fn default_view_stderr() -> bool {
        false
    }
}

pub fn try_mcp_server_stdio_from_expanded_config(config: &ExpandedTaskConfig) -> Result<McpServerStdIOConfig> {
    let command: String = config.expand("command").ok_or_else(|| {
        anyhow::anyhow!(
            "Command field is required for McpServerStdIO in task '{}'",
            config.name
        )
    })?;

    if command.trim().is_empty() {
        bail!(
            "Command field is required for McpServerStdIO in task '{}'",
            config.name
        );
    }

    let args: Vec<String> = config.expand("args").unwrap_or_default();
    let working_directory: Option<String> = config.expand("working_directory");
    let environment_vars = config
        .get_protocol_value("environment_vars")
        .and_then(|value| {
            if let TomlValue::Table(table) = value {
                let mut env_map = HashMap::new();
                for (key, value) in table {
                    if let Some(val_str) = value.as_str() {
                        env_map.insert(key.clone(), val_str.to_string());
                    } else {
                        return None;
                    }
                }
                Some(env_map)
            } else {
                None
            }
        });

    if let Some(ref working_dir) = working_directory {
        if !std::path::Path::new(working_dir).exists() {
            bail!(
                "Task '{}' working directory '{}' does not exist",
                config.name,
                working_dir
            );
        }
    }

    Ok(McpServerStdIOConfig {
        command,
        args,
        working_directory,
        environment_vars,
    })
}

#[derive(Clone, Debug)]
pub struct McpServerTcpConfig {
    pub host: String,
    pub port: u16,
}

impl BackendConfigMeta for McpServerTcpConfig {
    fn protocol_name() -> &'static str {
        "McpServerTcp"
    }

    fn default_allow_duplicate() -> bool {
        true
    }

    fn default_auto_start() -> bool {
        false
    }

    fn default_view_stdout() -> bool {
        false
    }

    fn default_view_stderr() -> bool {
        false
    }
}

pub fn try_mcp_server_tcp_from_expanded_config(config: &ExpandedTaskConfig) -> Result<McpServerTcpConfig> {
    let host: String = config
        .expand("host")
        .unwrap_or_else(|| "127.0.0.1".to_string());

    let port = config
        .get_protocol_value("port")
        .and_then(|value| match value {
            TomlValue::Integer(i) if *i >= 0 && *i <= u16::MAX as i64 => Some(*i as u16),
            TomlValue::String(s) => s.parse::<u16>().ok(),
            _ => None,
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Port field is required for McpServerTcp in task '{}' and must be a valid u16",
                config.name
            )
        })?;

    if host.trim().is_empty() {
        bail!(
            "Host field is required for McpServerTcp in task '{}'",
            config.name
        );
    }

    Ok(McpServerTcpConfig { host, port })
}

