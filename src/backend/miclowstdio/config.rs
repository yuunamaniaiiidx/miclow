use crate::backend::config::BackendConfigMeta;
use crate::config::ExpandedTaskConfig;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use toml::Value as TomlValue;

#[derive(Debug, Clone)]
pub struct MiclowStdIOConfig {
    pub command: Arc<str>,
    pub args: Vec<Arc<str>>,
    pub working_directory: Option<Arc<str>>,
    pub environment: Option<HashMap<String, String>>,
    pub stdout_topic: Arc<str>,
    pub stderr_topic: Arc<str>,
    pub view_stdout: bool,
    pub view_stderr: bool,
}

impl BackendConfigMeta for MiclowStdIOConfig {
    fn protocol_name() -> &'static str {
        "MiclowStdIO"
    }

    fn default_view_stdout() -> bool {
        false
    }

    fn default_view_stderr() -> bool {
        false
    }
}

// MiclowStdIOプロトコルで許可されているフィールド
const ALLOWED_MICLOW_STDIO_FIELDS: &[&str] = &[
    "command",
    "args",
    "working_directory",
    "environment",
    "stdout_topic",
    "stderr_topic",
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

pub fn try_miclow_stdio_from_expanded_config(
    config: &ExpandedTaskConfig,
) -> Result<MiclowStdIOConfig, anyhow::Error> {
    // 無効なフィールドをチェック
    validate_protocol_fields(config, ALLOWED_MICLOW_STDIO_FIELDS, "MiclowStdIO")?;
    
    let command_str: String = config.expand("command").ok_or_else(|| {
        anyhow::anyhow!(
            "Command field is required for MiclowStdIO in task '{}'",
            config.name
        )
    })?;

    if command_str.is_empty() {
        return Err(anyhow::anyhow!(
            "Command field is required for MiclowStdIO in task '{}'",
            config.name
        ));
    }
    let command = Arc::from(command_str);

    let args_vec: Vec<String> = config.expand("args").unwrap_or_default();
    let args: Vec<Arc<str>> = args_vec.into_iter().map(|s| Arc::from(s)).collect();

    let working_directory: Option<Arc<str>> = config.expand("working_directory").map(|s: String| Arc::from(s));

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

    let stdout_topic: Arc<str> = config
        .expand("stdout_topic")
        .map(|s: String| Arc::from(s))
        .unwrap_or_else(|| Arc::from(format!("{}.stdout", config.name)));

    let stderr_topic: Arc<str> = config
        .expand("stderr_topic")
        .map(|s: String| Arc::from(s))
        .unwrap_or_else(|| Arc::from(format!("{}.stderr", config.name)));

    let view_stdout: bool = config.view_stdout;
    let view_stderr: bool = config.view_stderr;

    if stdout_topic.contains(' ') {
        return Err(anyhow::anyhow!(
            "Task '{}' stdout_topic '{}' contains spaces (not allowed)",
            config.name,
            stdout_topic.as_ref()
        ));
    }
    if stderr_topic.contains(' ') {
        return Err(anyhow::anyhow!(
            "Task '{}' stderr_topic '{}' contains spaces (not allowed)",
            config.name,
            stderr_topic.as_ref()
        ));
    }

    if let Some(ref working_dir) = working_directory {
        if !std::path::Path::new(working_dir.as_ref()).exists() {
            return Err(anyhow::anyhow!(
                "Task '{}' working directory '{}' does not exist",
                config.name,
                working_dir.as_ref()
            ));
        }
    }

    Ok(MiclowStdIOConfig {
        command,
        args,
        working_directory,
        environment,
        stdout_topic,
        stderr_topic,
        view_stdout,
        view_stderr,
    })
}
