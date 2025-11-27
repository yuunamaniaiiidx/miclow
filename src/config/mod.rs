pub mod expansion;
mod include_loader;
mod lifecycle;
mod normalize;
mod task_conversion;
mod validation;
mod value_conversion;

pub use lifecycle::{LifecycleConfig, LifecycleEntry, LifecycleMode, RawLifecycleConfig};
pub use value_conversion::FromTomlValue;

use crate::backend::{BackendConfigMeta, InteractiveConfig, MiclowStdIOConfig, ProtocolBackend};
use crate::config::expansion::{ExpandContext, Expandable};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use toml::Value as TomlValue;

#[derive(Debug, Clone)]
pub(crate) struct RawTaskConfig {
    pub name: TomlValue,
    pub protocol: TomlValue,
    pub view_stdout: Option<TomlValue>,
    pub view_stderr: Option<TomlValue>,
    pub lifecycle: Option<RawLifecycleConfig>,

    pub protocol_config: HashMap<String, TomlValue>,
}

#[derive(Debug, Clone)]
pub struct TaskConfig {
    pub name: Arc<str>,
    pub view_stdout: bool,
    pub view_stderr: bool,
    pub lifecycle: LifecycleConfig,

    pub protocol_config: HashMap<String, TomlValue>,

    pub protocol_backend: ProtocolBackend,
}

#[derive(Debug, Clone)]
pub struct ExpandedTaskConfig {
    pub name: Arc<str>,
    pub view_stdout: bool,
    pub view_stderr: bool,
    pub lifecycle: LifecycleConfig,
    pub protocol_config: HashMap<String, TomlValue>,
}

impl ExpandedTaskConfig {
    pub fn get_protocol_value(&self, key: &str) -> Option<&TomlValue> {
        self.protocol_config.get(key)
    }

    pub fn expand<T: FromTomlValue>(&self, key: &str) -> Option<T> {
        self.get_protocol_value(key)
            .and_then(|v| T::from_toml_value(v))
    }
}

impl TaskConfig {
    pub fn get_protocol_value(&self, key: &str) -> Option<&TomlValue> {
        self.protocol_config.get(key)
    }

    pub fn expand<T: FromTomlValue>(&self, key: &str) -> Option<T> {
        self.get_protocol_value(key)
            .and_then(|v| T::from_toml_value(v))
    }
}

pub(crate) fn expand_toml_value(value: &TomlValue, context: &ExpandContext) -> Result<TomlValue> {
    match value {
        TomlValue::String(s) => {
            let expanded = s.expand(context)?;
            Ok(TomlValue::String(expanded))
        }
        TomlValue::Array(arr) => {
            let mut expanded = Vec::new();
            for item in arr {
                expanded.push(expand_toml_value(item, context)?);
            }
            Ok(TomlValue::Array(expanded))
        }
        TomlValue::Table(table) => {
            let mut expanded = toml::map::Map::new();
            for (key, val) in table {
                let expanded_key = key.expand(context)?;
                expanded.insert(expanded_key, expand_toml_value(val, context)?);
            }
            Ok(TomlValue::Table(expanded))
        }
        _ => Ok(value.clone()),
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
struct RawTaskCommand {
    command: Option<String>,
    args: Option<Vec<String>>,
    working_directory: Option<String>,
    environment: Option<HashMap<String, String>>,
    stdout_topic: Option<String>,
    stderr_topic: Option<String>,
    #[serde(flatten)]
    _unknown: HashMap<String, TomlValue>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct RawMcpToolEntry {
    name: String,
    json: Option<String>,
    #[serde(flatten)]
    _unknown: HashMap<String, TomlValue>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct RawTaskMCP {
    command: Option<String>,
    args: Option<Vec<String>>,
    working_directory: Option<String>,
    environment: Option<HashMap<String, String>>,
    tools: Option<Vec<RawMcpToolEntry>>,
    #[serde(flatten)]
    _unknown: HashMap<String, TomlValue>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct RawTaskLifecycle {
    desired_instances: Option<u32>,
    mode: Option<String>,
    #[serde(flatten)]
    _unknown: HashMap<String, TomlValue>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct RawTaskEntry {
    task_name: String,
    protocol: String,
    view_stdout: Option<bool>,
    view_stderr: Option<bool>,
    #[serde(rename = "command")]
    command: Option<RawTaskCommand>,
    #[serde(rename = "mcp")]
    mcp: Option<RawTaskMCP>,
    #[serde(rename = "lifecycle")]
    lifecycle: Option<RawTaskLifecycle>,
    #[serde(flatten)]
    protocol_config: HashMap<String, TomlValue>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct RawSystemConfig {
    include_paths: Option<Vec<String>>,
    tasks: Option<Vec<RawTaskEntry>>,
    #[serde(flatten)]
    _unknown: HashMap<String, TomlValue>,
}

impl RawSystemConfig {
    fn validate_and_convert(self) -> Result<Vec<RawTaskConfig>> {
        let mut tasks = Vec::new();
        let mut unknown_sections = Vec::new();

        if let Some(entries) = self.tasks {
            for entry in entries {
                tasks.push(Self::convert_entry(entry)?);
            }
        }

        for (key, _) in &self._unknown {
            if key != "include_paths" && key != "tasks" {
                unknown_sections.push(key.clone());
            }
        }

        if !unknown_sections.is_empty() {
            return Err(anyhow::anyhow!(
                "Unknown section(s): {}. Supported sections: [[tasks]]",
                unknown_sections.join(", ")
            ));
        }

        Ok(tasks)
    }

    fn convert_entry(entry: RawTaskEntry) -> Result<RawTaskConfig> {
        let protocol_name = match entry.protocol.as_str() {
            "Interactive" => InteractiveConfig::protocol_name().to_string(),
            "MiclowStdIO" => MiclowStdIOConfig::protocol_name().to_string(),
            "MCP" => {
                use crate::backend::mcp::config::MCPConfig;
                MCPConfig::protocol_name().to_string()
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Unknown protocol '{}' for task '{}'. Supported protocols: Interactive, MiclowStdIO, MCP",
                    entry.protocol,
                    entry.task_name
                ))
            }
        };

        let lifecycle = if let Some(lifecycle_entry) = entry.lifecycle {
            Some(RawLifecycleConfig {
                desired_instances: lifecycle_entry
                    .desired_instances
                    .map(|v| TomlValue::Integer(v as i64)),
                mode: lifecycle_entry.mode.map(|v| TomlValue::String(v)),
            })
        } else {
            None
        };

        // commandセクションまたはmcpセクションのフィールドをprotocol_configにマージ
        let mut protocol_config = entry.protocol_config;
        if let Some(cmd) = entry.command {
            if let Some(command) = cmd.command {
                protocol_config.insert("command".to_string(), TomlValue::String(command));
            }
            if let Some(args) = cmd.args {
                let args_toml: Vec<TomlValue> =
                    args.into_iter().map(|s| TomlValue::String(s)).collect();
                protocol_config.insert("args".to_string(), TomlValue::Array(args_toml));
            }
            if let Some(working_directory) = cmd.working_directory {
                protocol_config.insert(
                    "working_directory".to_string(),
                    TomlValue::String(working_directory),
                );
            }
            if let Some(environment) = cmd.environment {
                let mut env_table = toml::map::Map::new();
                for (key, value) in environment {
                    env_table.insert(key, TomlValue::String(value));
                }
                protocol_config.insert("environment".to_string(), TomlValue::Table(env_table));
            }
            if let Some(stdout_topic) = cmd.stdout_topic {
                protocol_config.insert("stdout_topic".to_string(), TomlValue::String(stdout_topic));
            }
            if let Some(stderr_topic) = cmd.stderr_topic {
                protocol_config.insert("stderr_topic".to_string(), TomlValue::String(stderr_topic));
            }
        }
        if let Some(mcp) = entry.mcp {
            if let Some(command) = mcp.command {
                protocol_config.insert("command".to_string(), TomlValue::String(command));
            }
            if let Some(args) = mcp.args {
                let args_toml: Vec<TomlValue> =
                    args.into_iter().map(|s| TomlValue::String(s)).collect();
                protocol_config.insert("args".to_string(), TomlValue::Array(args_toml));
            }
            if let Some(working_directory) = mcp.working_directory {
                protocol_config.insert(
                    "working_directory".to_string(),
                    TomlValue::String(working_directory),
                );
            }
            if let Some(environment) = mcp.environment {
                let mut env_table = toml::map::Map::new();
                for (key, value) in environment {
                    env_table.insert(key, TomlValue::String(value));
                }
                protocol_config.insert("environment".to_string(), TomlValue::Table(env_table));
            }
            if let Some(tools) = mcp.tools {
                let mut tools_toml: Vec<TomlValue> = Vec::new();
                for tool in tools {
                    let mut table = toml::map::Map::new();
                    table.insert("name".to_string(), TomlValue::String(tool.name));
                    if let Some(json_template) = tool.json {
                        table.insert("json".to_string(), TomlValue::String(json_template));
                    }
                    tools_toml.push(TomlValue::Table(table));
                }
                protocol_config.insert("tools".to_string(), TomlValue::Array(tools_toml));
            }
        }

        let mut task_config = RawTaskConfig {
            name: TomlValue::String(entry.task_name.clone()),
            protocol: TomlValue::String(protocol_name),
            view_stdout: entry.view_stdout.map(|v| TomlValue::Boolean(v)),
            view_stderr: entry.view_stderr.map(|v| TomlValue::Boolean(v)),
            lifecycle,
            protocol_config,
        };

        task_config.normalize_defaults(&entry.protocol)?;

        Ok(task_config)
    }
}

#[derive(Debug, Clone)]
pub struct SystemConfig {
    pub tasks: HashMap<Arc<str>, TaskConfig>,
    pub include_paths: Vec<String>,
}

impl SystemConfig {
    pub fn from_toml(toml_content: &str) -> Result<Self> {
        Self::from_toml_internal(toml_content, true, &ExpandContext::new())
    }

    pub(crate) fn from_toml_internal(
        toml_content: &str,
        validate: bool,
        expand_context: &ExpandContext,
    ) -> Result<Self> {
        let raw: RawSystemConfig = toml::from_str(toml_content)?;

        let include_paths_raw = raw.include_paths.clone();

        let raw_tasks = raw.validate_and_convert()?;

        let mut tasks = HashMap::new();

        for task in raw_tasks {
            let expanded = task.expand_and_create_backend(expand_context)?;
            let name = expanded.name.clone();

            if tasks.insert(name.clone(), expanded).is_some() {
                return Err(anyhow::anyhow!("Duplicate task name: {}", name.as_ref()));
            }
        }

        let include_paths = if let Some(raw_value) = include_paths_raw {
            let mut expanded_paths = Vec::new();
            for path in raw_value {
                let expanded_value = expand_toml_value(&TomlValue::String(path), expand_context)?;
                if let Some(expanded_str) = expanded_value.as_str() {
                    expanded_paths.push(expanded_str.to_string());
                }
            }
            expanded_paths
        } else {
            Vec::new()
        };

        let config = SystemConfig {
            tasks,
            include_paths,
        };

        if validate {
            config.validate().unwrap_or_else(|e| {
                eprintln!("Config validation failed: {}", e);
                eprintln!("\nError details:");
                for (i, cause) in e.chain().enumerate() {
                    eprintln!("  {}: {}", i, cause);
                }
                std::process::exit(1);
            });
        }
        Ok(config)
    }

    pub fn from_file(config_file: String) -> Result<Self> {
        let config_content: String = std::fs::read_to_string(&config_file)?;

        let expand_context = ExpandContext::from_config_path(&config_file);

        let mut config = Self::from_toml_internal(&config_content, true, &expand_context)?;

        log::debug!("Before load_includes: {} tasks", config.tasks.len());
        include_loader::load_includes(&mut config, &config_file)?;
        log::debug!("After load_includes: {} tasks", config.tasks.len());
        config.validate().unwrap_or_else(|e| {
            eprintln!("Config validation failed: {}", e);
            eprintln!("\nError details:");
            for (i, cause) in e.chain().enumerate() {
                eprintln!("  {}: {}", i, cause);
            }
            std::process::exit(1);
        });

        Ok(config)
    }

    pub fn get_autostart_tasks(&self) -> Vec<&TaskConfig> {
        self.tasks.values().collect()
    }

    pub fn validate(&self) -> Result<()> {
        if self.tasks.is_empty() {
            return Err(anyhow::anyhow!("No tasks configured"));
        }

        self.validate_tasks()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_load_tasks_from_include() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path();

        let include_file = base_dir.join("include.toml");
        fs::write(
            &include_file,
            r#"
[[tasks]]
protocol = "MiclowStdIO"
task_name = "test_function"

[tasks.command]
command = "echo"
args = ["test"]
"#,
        )
        .unwrap();

        let main_file = base_dir.join("main.toml");
        fs::write(
            &main_file,
            format!(
                r#"
include_paths = ["include.toml"]

[[tasks]]
protocol = "MiclowStdIO"
task_name = "main_task"

[tasks.command]
command = "echo"
args = ["main"]
"#
            ),
        )
        .unwrap();

        let config = SystemConfig::from_file(main_file.to_string_lossy().to_string()).unwrap();

        assert_eq!(
            config.tasks.len(),
            2,
            "Should have 2 tasks (1 from main file + 1 from include file)"
        );
        assert_eq!(
            config
                .tasks
                .get(&Arc::from("main_task"))
                .unwrap()
                .name
                .as_ref(),
            "main_task"
        );
        assert_eq!(
            config
                .tasks
                .get(&Arc::from("test_function"))
                .unwrap()
                .name
                .as_ref(),
            "test_function"
        );
    }
}
