use crate::backend::config::BackendConfigMeta;
use crate::config::ExpandedTaskConfig;
use anyhow::Result;
use std::collections::HashMap;
use toml::Value as TomlValue;

#[derive(Debug, Clone)]
pub struct MiclowStdIOConfig {
    pub command: String,
    pub args: Vec<String>,
    pub working_directory: Option<String>,
    pub environment_vars: Option<HashMap<String, String>>,
    pub stdout_topic: String,
    pub stderr_topic: String,
    pub view_stdout: bool,
    pub view_stderr: bool,
    pub functions: Vec<String>,
}

impl BackendConfigMeta for MiclowStdIOConfig {
    fn protocol_name() -> &'static str {
        "MiclowStdIO"
    }

    fn force_allow_duplicate() -> bool {
        false
    }

    fn force_auto_start() -> bool {
        true
    }

    fn default_view_stdout() -> bool {
        false
    }

    fn default_view_stderr() -> bool {
        false
    }
}

pub fn try_miclow_stdio_from_expanded_config(
    config: &ExpandedTaskConfig,
) -> Result<MiclowStdIOConfig, anyhow::Error> {
    // プロトコル固有のフィールドを抽出・バリデーション
    let command: String = config.expand("command").ok_or_else(|| {
        anyhow::anyhow!(
            "Command field is required for MiclowStdIO in task '{}'",
            config.name
        )
    })?;

    if command.is_empty() {
        return Err(anyhow::anyhow!(
            "Command field is required for MiclowStdIO in task '{}'",
            config.name
        ));
    }

    let args: Vec<String> = config.expand("args").unwrap_or_default();

    let working_directory: Option<String> = config.expand("working_directory");

    let environment_vars = config.get_protocol_value("environment_vars").and_then(|v| {
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

    // デフォルト値の生成ロジック: stdout_topic/stderr_topicが未設定の場合は"{name}.stdout"/"{name}.stderr"を使用
    let stdout_topic: String = config
        .expand("stdout_topic")
        .unwrap_or_else(|| format!("{}.stdout", config.name));

    let stderr_topic: String = config
        .expand("stderr_topic")
        .unwrap_or_else(|| format!("{}.stderr", config.name));

    // view_stdoutとview_stderrはTaskConfigの共通フィールドとして扱う
    let view_stdout: bool = config.view_stdout;
    let view_stderr: bool = config.view_stderr;

    // バリデーション
    if stdout_topic.contains(' ') {
        return Err(anyhow::anyhow!(
            "Task '{}' stdout_topic '{}' contains spaces (not allowed)",
            config.name,
            stdout_topic
        ));
    }
    if stderr_topic.contains(' ') {
        return Err(anyhow::anyhow!(
            "Task '{}' stderr_topic '{}' contains spaces (not allowed)",
            config.name,
            stderr_topic
        ));
    }

    if let Some(ref working_dir) = working_directory {
        if !std::path::Path::new(working_dir).exists() {
            return Err(anyhow::anyhow!(
                "Task '{}' working directory '{}' does not exist",
                config.name,
                working_dir
            ));
        }
    }

    let functions: Vec<String> = config.functions.clone();

    Ok(MiclowStdIOConfig {
        command,
        args,
        working_directory,
        environment_vars,
        stdout_topic,
        stderr_topic,
        view_stdout,
        view_stderr,
        functions,
    })
}

