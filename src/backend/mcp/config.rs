use crate::backend::config::BackendConfigMeta;
use crate::config::ExpandedTaskConfig;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use toml::Value as TomlValue;

#[derive(Debug, Clone)]
pub struct MCPConfig {
    pub command: Arc<str>,
    pub args: Vec<Arc<str>>,
    pub working_directory: Option<Arc<str>>,
    pub environment: Option<HashMap<String, String>>,
    pub tools: Vec<Arc<str>>,
    pub view_stdout: bool,
    pub view_stderr: bool,
}

impl BackendConfigMeta for MCPConfig {
    fn protocol_name() -> &'static str {
        "MCP"
    }

    fn default_view_stdout() -> bool {
        false
    }

    fn default_view_stderr() -> bool {
        false
    }
}

// MCPプロトコルで許可されているフィールド
const ALLOWED_MCP_FIELDS: &[&str] = &[
    "command",
    "args",
    "working_directory",
    "environment",
    "tools",
];

fn validate_protocol_fields(
    config: &ExpandedTaskConfig,
    allowed_fields: &[&str],
    protocol_name: &str,
) -> Result<(), anyhow::Error> {
    let mut invalid_fields = Vec::new();

    for (key, _) in &config.protocol_config {
        if !allowed_fields.contains(&key.as_str()) {
            invalid_fields.push(key.clone());
        }
    }

    if !invalid_fields.is_empty() {
        return Err(anyhow::anyhow!(
            "Task '{}' (protocol: {}) has invalid field(s): {}. Allowed fields: {}",
            config.name,
            protocol_name,
            invalid_fields.join(", "),
            allowed_fields.join(", ")
        ));
    }

    Ok(())
}

pub fn try_mcp_from_expanded_config(
    config: &ExpandedTaskConfig,
) -> Result<MCPConfig, anyhow::Error> {
    // 無効なフィールドをチェック
    validate_protocol_fields(config, ALLOWED_MCP_FIELDS, "MCP")?;

    let command_str: String = config.expand("command").ok_or_else(|| {
        anyhow::anyhow!(
            "Command field is required for MCP in task '{}'",
            config.name
        )
    })?;

    if command_str.is_empty() {
        return Err(anyhow::anyhow!(
            "Command field is required for MCP in task '{}'",
            config.name
        ));
    }
    let command = Arc::from(command_str);

    let args_vec: Vec<String> = config.expand("args").unwrap_or_default();
    let args: Vec<Arc<str>> = args_vec.into_iter().map(|s| Arc::from(s)).collect();

    let working_directory: Option<Arc<str>> = config
        .expand("working_directory")
        .map(|s: String| Arc::from(s));

    let environment = config.get_protocol_value("environment").and_then(|v| {
        if let TomlValue::Table(table) = v {
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

    let tools_vec: Vec<String> = config.expand("tools").ok_or_else(|| {
        anyhow::anyhow!(
            "Tools field is required for MCP in task '{}'",
            config.name
        )
    })?;

    if tools_vec.is_empty() {
        return Err(anyhow::anyhow!(
            "Tools field must contain at least one tool name for MCP in task '{}'",
            config.name
        ));
    }

    let tools: Vec<Arc<str>> = tools_vec.into_iter().map(|s| Arc::from(s)).collect();

    let view_stdout: bool = config.view_stdout;
    let view_stderr: bool = config.view_stderr;

    if let Some(ref working_dir) = working_directory {
        if !std::path::Path::new(working_dir.as_ref()).exists() {
            return Err(anyhow::anyhow!(
                "Task '{}' working directory '{}' does not exist",
                config.name,
                working_dir.as_ref()
            ));
        }
    }

    Ok(MCPConfig {
        command,
        args,
        working_directory,
        environment,
        tools,
        view_stdout,
        view_stderr,
    })
}

