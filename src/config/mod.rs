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
use toml::Value as TomlValue;

/// 展開前の生のタスク設定情報
#[derive(Debug, Clone)]
pub(crate) struct RawTaskConfig {
    pub name: TomlValue,
    pub protocol: TomlValue, // セクション名から設定される
    pub subscribe_topics: Option<TomlValue>,
    pub view_stdout: Option<TomlValue>,
    pub view_stderr: Option<TomlValue>,
    pub lifecycle: Option<RawLifecycleConfig>,

    /// プロトコル固有の設定（プロトコル非依存のフィールド以外のすべて）
    pub protocol_config: HashMap<String, TomlValue>,
}

/// プロトコル非依存のタスク設定情報（展開後）
#[derive(Debug, Clone)]
pub struct TaskConfig {
    pub name: String,
    pub subscribe_topics: Option<Vec<String>>,
    pub view_stdout: bool,
    pub view_stderr: bool,
    pub lifecycle: LifecycleConfig,

    /// プロトコル固有の設定（プロトコル非依存のフィールド以外のすべて）
    pub protocol_config: HashMap<String, TomlValue>,

    /// プロトコルバックエンド（セクション名から直接作成）
    pub protocol_backend: ProtocolBackend,
}

// RawTaskConfigの実装はtask_conversion.rsとnormalize.rsに分離

/// 環境変数展開後のタスク設定（ProtocolBackend作成前）
#[derive(Debug, Clone)]
pub struct ExpandedTaskConfig {
    pub name: String,
    pub subscribe_topics: Option<Vec<String>>,
    pub view_stdout: bool,
    pub view_stderr: bool,
    pub lifecycle: LifecycleConfig,
    pub protocol_config: HashMap<String, TomlValue>,
}

impl ExpandedTaskConfig {
    /// プロトコル固有設定から値を取得（展開済み）
    pub fn get_protocol_value(&self, key: &str) -> Option<&TomlValue> {
        self.protocol_config.get(key)
    }

    /// プロトコル固有設定から型指定で値を取得
    pub fn expand<T: FromTomlValue>(&self, key: &str) -> Option<T> {
        self.get_protocol_value(key)
            .and_then(|v| T::from_toml_value(v))
    }
}

impl TaskConfig {
    /// プロトコル固有設定から値を取得（展開済み）
    pub fn get_protocol_value(&self, key: &str) -> Option<&TomlValue> {
        self.protocol_config.get(key)
    }

    /// プロトコル固有設定から型指定で値を取得
    /// 型推論により、`let value: bool = config.expand("key")`のように使用可能
    pub fn expand<T: FromTomlValue>(&self, key: &str) -> Option<T> {
        self.get_protocol_value(key)
            .and_then(|v| T::from_toml_value(v))
    }
}

/// TOML値に対して環境変数展開を実行
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

/// TOMLから直接デシリアライズされるタスクエントリ
#[derive(Debug, Clone, serde::Deserialize)]
struct RawTaskEntry {
    task_name: String,
    protocol: String,
    subscribe_topics: Option<Vec<String>>,
    view_stdout: Option<bool>,
    view_stderr: Option<bool>,
    #[serde(default)]
    lifecycle: Vec<LifecycleEntry>,
    /// その他のプロトコル固有設定（flattenで展開される）
    #[serde(flatten)]
    protocol_config: HashMap<String, TomlValue>,
}

/// TOMLから直接デシリアライズされるシステム設定
#[derive(Debug, Clone, serde::Deserialize)]
struct RawSystemConfig {
    include_paths: Option<Vec<String>>,
    tasks: Option<Vec<RawTaskEntry>>,
    /// 未知のフィールドを許可（後でバリデーション）
    #[serde(flatten)]
    _unknown: HashMap<String, TomlValue>,
}

impl RawSystemConfig {
    /// RawSystemConfigを検証し、RawTaskConfigのベクターに変換
    fn validate_and_convert(self) -> Result<Vec<RawTaskConfig>> {
        let mut tasks = Vec::new();
        let mut unknown_sections = Vec::new();

        // tasksセクションを処理
        if let Some(entries) = self.tasks {
            for entry in entries {
                tasks.push(Self::convert_entry(entry)?);
            }
        }

        // 未知のセクションをチェック（include_paths、tasks、tasks.lifecycle以外）
        for (key, _) in &self._unknown {
            if key != "include_paths" && key != "tasks" && key != "tasks.lifecycle" {
                unknown_sections.push(key.clone());
            }
        }

        // バリデーション: 未知のセクションがある場合はエラー
        if !unknown_sections.is_empty() {
            return Err(anyhow::anyhow!(
                "Unknown section(s): {}. Supported sections: [[tasks]], [[tasks.lifecycle]]",
                unknown_sections.join(", ")
            ));
        }

        Ok(tasks)
    }

    fn convert_entry(entry: RawTaskEntry) -> Result<RawTaskConfig> {
        // プロトコル名をバリデーション
        let protocol_name = match entry.protocol.as_str() {
            "Interactive" => InteractiveConfig::protocol_name().to_string(),
            "MiclowStdIO" => MiclowStdIOConfig::protocol_name().to_string(),
            _ => {
                return Err(anyhow::anyhow!(
                    "Unknown protocol '{}' for task '{}'. Supported protocols: Interactive, MiclowStdIO",
                    entry.protocol,
                    entry.task_name
                ))
            }
        };

        let lifecycle = if entry.lifecycle.is_empty() {
            None
        } else if entry.lifecycle.len() == 1 {
            Some(RawLifecycleConfig::from_entry(
                entry.lifecycle.first().cloned().unwrap(),
            )?)
        } else {
            return Err(anyhow::anyhow!(
                "Task '{}' has multiple lifecycle blocks. Only one [[tasks.lifecycle]] entry is supported.",
                entry.task_name
            ));
        };

        // RawTaskConfigを作成
        let mut task_config = RawTaskConfig {
            name: TomlValue::String(entry.task_name.clone()),
            protocol: TomlValue::String(protocol_name),
            subscribe_topics: entry
                .subscribe_topics
                .map(|v| TomlValue::Array(v.into_iter().map(|s| TomlValue::String(s)).collect())),
            view_stdout: entry.view_stdout.map(|v| TomlValue::Boolean(v)),
            view_stderr: entry.view_stderr.map(|v| TomlValue::Boolean(v)),
            lifecycle,
            protocol_config: entry.protocol_config,
        };

        // 強制値とデフォルト値を設定（バリデーションも含む）
        task_config.normalize_defaults(&entry.protocol)?;

        Ok(task_config)
    }
}

#[derive(Debug, Clone)]
pub struct SystemConfig {
    pub tasks: HashMap<String, TaskConfig>,
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

        // include_pathsを先に取得
        let include_paths_raw = raw.include_paths.clone();

        // バリデーションと変換（normalize_defaultsも含まれる）
        let raw_tasks = raw.validate_and_convert()?;

        // 変数展開とProtocolBackend作成を実行してTaskConfigに変換
        let mut tasks = HashMap::new();

        for task in raw_tasks {
            let expanded = task.expand_and_create_backend(expand_context)?;
            let name = expanded.name.clone();

            if tasks.insert(name.clone(), expanded).is_some() {
                return Err(anyhow::anyhow!("Duplicate task name: {}", name));
            }
        }

        let include_paths = if let Some(raw_value) = include_paths_raw {
            // include_pathsは既にVec<String>としてデシリアライズされているので、環境変数展開のみ実行
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

        // 展開コンテキストを作成
        let expand_context = ExpandContext::from_config_path(&config_file);

        // パース後に展開処理を実行
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
        // すべてのタスクを返す（auto_startの概念を削除）
        self.tasks.values().collect()
    }

    pub fn validate(&self) -> Result<()> {
        if self.tasks.is_empty() {
            return Err(anyhow::anyhow!("No tasks configured"));
        }

        // プロトコル非依存のバリデーションのみ実行
        // プロトコル固有のバリデーションはProtocolBackend作成時に実行される
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

        // Create include file with tasks
        let include_file = base_dir.join("include.toml");
        fs::write(
            &include_file,
            r#"
[[tasks]]
protocol = "MiclowStdIO"
task_name = "test_function"
command = "echo"
args = ["test"]
"#,
        )
        .unwrap();

        // Create main config file
        let main_file = base_dir.join("main.toml");
        fs::write(
            &main_file,
            format!(
                r#"
include_paths = ["include.toml"]

[[tasks]]
protocol = "MiclowStdIO"
task_name = "main_task"
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
        assert_eq!(config.tasks.get("main_task").unwrap().name, "main_task");
        assert_eq!(
            config.tasks.get("test_function").unwrap().name,
            "test_function"
        );
    }
}
