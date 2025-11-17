pub mod expansion;

use crate::backend::{
    BackendConfigMeta, get_default_allow_duplicate, get_default_auto_start,
    get_default_view_stderr, get_default_view_stdout, InteractiveConfig,
    McpServerStdIOConfig, McpServerTcpConfig, MiclowStdIOConfig, ProtocolBackend,
};
use crate::config::expansion::{ExpandContext, Expandable};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use toml::Value as TomlValue;

/// TOML値から型への変換トレイト
pub trait FromTomlValue: Sized {
    fn from_toml_value(value: &TomlValue) -> Option<Self>;
}

impl FromTomlValue for bool {
    fn from_toml_value(value: &TomlValue) -> Option<Self> {
        match value {
            TomlValue::Boolean(b) => Some(*b),
            TomlValue::String(s) => Some(parse_bool(s)),
            _ => None,
        }
    }
}

impl FromTomlValue for String {
    fn from_toml_value(value: &TomlValue) -> Option<Self> {
        value.as_str().map(|s| s.to_string())
    }
}

impl FromTomlValue for Vec<String> {
    fn from_toml_value(value: &TomlValue) -> Option<Self> {
        value.as_array().map(|arr| {
            arr.iter()
                .filter_map(|item| item.as_str().map(|s| s.to_string()))
                .collect()
        })
    }
}

impl FromTomlValue for Option<String> {
    fn from_toml_value(value: &TomlValue) -> Option<Self> {
        Some(value.as_str().map(|s| s.to_string()))
    }
}

/// 展開前の生のタスク設定情報
#[derive(Debug, Clone)]
struct RawTaskConfig {
    pub name: TomlValue,
    pub protocol: TomlValue, // セクション名から設定される
    pub subscribe_topics: Option<TomlValue>,
    pub allow_duplicate: Option<TomlValue>,
    pub auto_start: Option<TomlValue>,
    pub view_stdout: Option<TomlValue>,
    pub view_stderr: Option<TomlValue>,

    /// プロトコル固有の設定（プロトコル非依存のフィールド以外のすべて）
    pub protocol_config: HashMap<String, TomlValue>,
}

/// プロトコル非依存のタスク設定情報（展開後）
#[derive(Debug, Clone)]
pub struct TaskConfig {
    pub name: String,
    pub subscribe_topics: Option<Vec<String>>,
    pub allow_duplicate: bool,
    pub auto_start: bool,
    pub view_stdout: bool,
    pub view_stderr: bool,

    /// プロトコル固有の設定（プロトコル非依存のフィールド以外のすべて）
    pub protocol_config: HashMap<String, TomlValue>,

    /// プロトコルバックエンド（セクション名から直接作成）
    pub protocol_backend: ProtocolBackend,
}

impl RawTaskConfig {
    /// toml::Value::TableからRawTaskConfigを作成
    fn from_toml_table(mut table: toml::map::Map<String, toml::Value>, protocol: String) -> Result<Self, String> {
        let name = table
            .remove("task_name")
            .ok_or_else(|| "task_name field is required".to_string())?;

        let subscribe_topics = table.remove("subscribe_topics");
        let allow_duplicate = table.remove("allow_duplicate");
        let auto_start = table.remove("auto_start");
        let view_stdout = table.remove("view_stdout");
        let view_stderr = table.remove("view_stderr");

        // toml::map::MapをHashMapに変換
        let protocol_config: HashMap<String, TomlValue> = table.into_iter().collect();

        Ok(RawTaskConfig {
            name,
            protocol: toml::Value::String(protocol),
            subscribe_topics,
            allow_duplicate,
            auto_start,
            view_stdout,
            view_stderr,
            protocol_config,
        })
    }

    /// 環境変数展開を実行してExpandedTaskConfigに変換
    pub fn expand(self, context: &ExpandContext) -> Result<ExpandedTaskConfig> {
        // 文字列フィールドの展開
        let expanded_name = expand_toml_value(&self.name, context)?;
        let name = String::from_toml_value(&expanded_name)
            .ok_or_else(|| anyhow::anyhow!("task_name must be a string"))?;

        let subscribe_topics = if let Some(raw_value) = self.subscribe_topics {
            let expanded_value = expand_toml_value(&raw_value, context)?;
            Some(
                Vec::<String>::from_toml_value(&expanded_value).ok_or_else(|| {
                    anyhow::anyhow!("subscribe_topics must be an array of strings")
                })?,
            )
        } else {
            None
        };

        // allow_duplicateとauto_startを展開してboolに変換
        // normalize_defaults()で既にデフォルト値が設定されているので、必ずSome(TomlValue::Boolean)になっている
        let allow_duplicate = self
            .allow_duplicate
            .map(|raw_value| {
                let expanded_value = expand_toml_value(&raw_value, context)?;
                bool::from_toml_value(&expanded_value)
                    .ok_or_else(|| anyhow::anyhow!("allow_duplicate must be a boolean"))
            })
            .transpose()?
            .expect("allow_duplicate should be set by normalize_defaults()");

        let auto_start = self
            .auto_start
            .map(|raw_value| {
                let expanded_value = expand_toml_value(&raw_value, context)?;
                bool::from_toml_value(&expanded_value)
                    .ok_or_else(|| anyhow::anyhow!("auto_start must be a boolean"))
            })
            .transpose()?
            .expect("auto_start should be set by normalize_defaults()");

        let view_stdout = self
            .view_stdout
            .map(|raw_value| {
                let expanded_value = expand_toml_value(&raw_value, context)?;
                bool::from_toml_value(&expanded_value)
                    .ok_or_else(|| anyhow::anyhow!("view_stdout must be a boolean"))
            })
            .transpose()?
            .expect("view_stdout should be set by normalize_defaults()");

        let view_stderr = self
            .view_stderr
            .map(|raw_value| {
                let expanded_value = expand_toml_value(&raw_value, context)?;
                bool::from_toml_value(&expanded_value)
                    .ok_or_else(|| anyhow::anyhow!("view_stderr must be a boolean"))
            })
            .transpose()?
            .expect("view_stderr should be set by normalize_defaults()");

        // プロトコル固有設定の展開
        let mut protocol_config = HashMap::new();
        for (key, value) in self.protocol_config {
            let expanded_key = key.expand(context)?;
            let expanded_value = expand_toml_value(&value, context)?;
            protocol_config.insert(expanded_key, expanded_value);
        }

        // 展開された値を返す（TaskConfigは統合層で構築される）
        Ok(ExpandedTaskConfig {
            name,
            subscribe_topics,
            allow_duplicate,
            auto_start,
            view_stdout,
            view_stderr,
            protocol_config,
        })
    }

    /// 環境変数展開とProtocolBackend作成を統合してTaskConfigに変換
    pub fn expand_and_create_backend(self, context: &ExpandContext) -> Result<TaskConfig> {
        let expanded_protocol = expand_toml_value(&self.protocol, context)?;
        let protocol = String::from_toml_value(&expanded_protocol)
            .ok_or_else(|| anyhow::anyhow!("protocol must be a string"))?;

        // まず環境変数展開を実行
        let expanded = self.expand(context)?;

        // ProtocolBackendを作成
        let protocol_backend = create_protocol_backend(&protocol, &expanded)?;

        // TaskConfigを構築
        Ok(TaskConfig {
            name: expanded.name,
            subscribe_topics: expanded.subscribe_topics,
            allow_duplicate: expanded.allow_duplicate,
            auto_start: expanded.auto_start,
            view_stdout: expanded.view_stdout,
            view_stderr: expanded.view_stderr,
            protocol_config: expanded.protocol_config,
            protocol_backend,
        })
    }
}

/// 環境変数展開後のタスク設定（ProtocolBackend作成前）
#[derive(Debug, Clone)]
pub struct ExpandedTaskConfig {
    pub name: String,
    pub subscribe_topics: Option<Vec<String>>,
    pub allow_duplicate: bool,
    pub auto_start: bool,
    pub view_stdout: bool,
    pub view_stderr: bool,
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

/// プロトコル名からProtocolBackendを作成
fn create_protocol_backend(
    protocol: &str,
    expanded: &ExpandedTaskConfig,
) -> Result<ProtocolBackend> {
    use crate::backend::{
        interactive::try_interactive_from_expanded_config,
        mcp_server::{
            try_mcp_server_stdio_from_expanded_config, try_mcp_server_tcp_from_expanded_config,
        },
        miclowstdio::try_miclow_stdio_from_expanded_config,
    };

    match protocol.trim() {
        "MiclowStdIO" => {
            let backend_config = try_miclow_stdio_from_expanded_config(expanded)?;
            Ok(ProtocolBackend::MiclowStdIO(backend_config))
        }
        "Interactive" => {
            let backend_config = try_interactive_from_expanded_config(expanded)?;
            Ok(ProtocolBackend::Interactive(backend_config))
        }
        "McpServerStdIO" => {
            let backend_config = try_mcp_server_stdio_from_expanded_config(expanded)?;
            Ok(ProtocolBackend::McpServerStdIO(backend_config))
        }
        "McpServerTcp" => {
            let backend_config = try_mcp_server_tcp_from_expanded_config(expanded)?;
            Ok(ProtocolBackend::McpServerTcp(backend_config))
        }
        _ => Err(anyhow::anyhow!(
            "Unknown protocol '{}' for task '{}'. Supported protocols: MiclowStdIO, Interactive, McpServerStdIO, McpServerTcp",
            protocol,
            expanded.name
        )),
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
fn expand_toml_value(value: &TomlValue, context: &ExpandContext) -> Result<TomlValue> {
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

/// 文字列をboolに変換
/// "true", "1", "yes", "on" などは true、それ以外は false
fn parse_bool(s: &str) -> bool {
    let s_lower = s.to_lowercase();
    let s_trimmed = s_lower.trim();
    matches!(s_trimmed, "true" | "1" | "yes" | "on" | "y")
}

#[derive(Debug, Clone)]
struct RawSystemConfig {
    tasks: Vec<RawTaskConfig>,
    include_paths: Option<TomlValue>,
}

impl<'de> serde::Deserialize<'de> for RawSystemConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // TOML全体をValueとして読み込む
        let value = toml::Value::deserialize(deserializer)?;
        
        let mut tasks = Vec::new();
        let mut include_paths = None;

        if let toml::Value::Table(table) = value {
            for (key, val) in table {
                if key == "include_paths" {
                    include_paths = Some(val);
                    continue;
                }

                // プロトコル名のセクションを処理
                if let toml::Value::Array(array) = val {
                    // セクション名からプロトコル名を判定
                    let protocol_name = match key.as_str() {
                        "Interactive" => InteractiveConfig::protocol_name().to_string(),
                        "MiclowStdIO" => MiclowStdIOConfig::protocol_name().to_string(),
                        "MiclowStdIOFunction" => MiclowStdIOConfig::protocol_name().to_string(),
                        "McpServerStdIO" => McpServerStdIOConfig::protocol_name().to_string(),
                        "McpServerTcp" => McpServerTcpConfig::protocol_name().to_string(),
                        _ => {
                            return Err(serde::de::Error::custom(format!(
                                "Unknown section name '{}'. Supported sections: [[Interactive]], [[MiclowStdIO]], [[MiclowStdIOFunction]], [[McpServerStdIO]], [[McpServerTcp]]",
                                key
                            )));
                        }
                    };

                    // 各エントリを処理
                    for item in array {
                        if let toml::Value::Table(entry_table) = item {
                            // RawTaskConfigを作成
                            let mut task_config = RawTaskConfig::from_toml_table(entry_table, protocol_name.clone())
                                .map_err(|e| serde::de::Error::custom(format!("Failed to create task config: {}", e)))?;
                            
                            // デフォルト値を設定（セクション名から直接判定）
                            if task_config.allow_duplicate.is_none() {
                                let default_value = get_default_allow_duplicate(key.as_str());
                                task_config.allow_duplicate = Some(TomlValue::Boolean(default_value));
                            }
                            if task_config.auto_start.is_none() {
                                let default_value = get_default_auto_start(key.as_str());
                                task_config.auto_start = Some(TomlValue::Boolean(default_value));
                            }
                            if task_config.view_stdout.is_none() {
                                let default_value = get_default_view_stdout(key.as_str());
                                task_config.view_stdout = Some(TomlValue::Boolean(default_value));
                            }
                            if task_config.view_stderr.is_none() {
                                let default_value = get_default_view_stderr(key.as_str());
                                task_config.view_stderr = Some(TomlValue::Boolean(default_value));
                            }
                            
                            tasks.push(task_config);
                        }
                    }
                }
            }
        }

        Ok(RawSystemConfig {
            tasks,
            include_paths,
        })
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

    fn from_toml_internal(
        toml_content: &str,
        validate: bool,
        expand_context: &ExpandContext,
    ) -> Result<Self> {
        let mut raw: RawSystemConfig = toml::from_str(toml_content)?;

        // デフォルト値を設定（展開前に実行）
        raw.normalize_defaults();

        // 変数展開とProtocolBackend作成を実行してTaskConfigに変換
        let mut tasks = HashMap::new();
        
        for task in raw.tasks {
            let expanded = task.expand_and_create_backend(expand_context)?;
            let name = expanded.name.clone();
            if tasks.insert(name.clone(), expanded).is_some() {
                return Err(anyhow::anyhow!("Duplicate task name: {}", name));
            }
        }

        let include_paths = if let Some(raw_value) = raw.include_paths {
            let expanded_value = expand_toml_value(&raw_value, expand_context)?;
            Vec::<String>::from_toml_value(&expanded_value)
                .ok_or_else(|| anyhow::anyhow!("include_paths must be an array of strings"))?
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

        log::debug!(
            "Before load_includes: {} tasks",
            config.tasks.len()
        );
        config.load_includes(&config_file)?;
        log::debug!(
            "After load_includes: {} tasks",
            config.tasks.len()
        );
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
        self.tasks
            .values()
            .filter(|task| task.auto_start)
            .collect()
    }

    pub fn validate(&self) -> Result<()> {
        let autostart_count = self.tasks.values().filter(|t| t.auto_start).count();
        if autostart_count == 0 {
            return Err(anyhow::anyhow!("No autostart tasks configured"));
        }

        // プロトコル非依存のバリデーションのみ実行
        // プロトコル固有のバリデーションはProtocolBackend作成時に実行される
        self.validate_tasks()?;

        Ok(())
    }

    /// タスクのプロトコル非依存バリデーション
    fn validate_tasks(&self) -> Result<()> {
        for (name, task) in &self.tasks {
            if task.name.is_empty() {
                return Err(anyhow::anyhow!("Task '{}' has empty name", name));
            }

            if task.name.starts_with("system") {
                return Err(anyhow::anyhow!(
                    "Task '{}' cannot start with 'system' (reserved for system tasks)",
                    task.name
                ));
            }

            if let Some(subscribe_topics) = &task.subscribe_topics {
                for (topic_index, topic) in subscribe_topics.iter().enumerate() {
                    if topic.is_empty() {
                        return Err(anyhow::anyhow!(
                            "Task '{}' has empty initial topic at index {}",
                            task.name,
                            topic_index
                        ));
                    }
                    if topic.contains(' ') {
                        return Err(anyhow::anyhow!(
                            "Task '{}' initial topic '{}' contains spaces (not allowed)",
                            task.name,
                            topic
                        ));
                    }
                }
            }
        }
        Ok(())
    }


    fn load_includes(&mut self, base_config_path: &str) -> Result<()> {
        let mut loaded_files = HashSet::new();
        let base_dir = std::path::Path::new(base_config_path)
            .parent()
            .unwrap_or(std::path::Path::new("."));

        let include_paths = self.include_paths.clone();
        self.load_includes_recursive(&mut loaded_files, base_dir, &include_paths)?;
        Ok(())
    }

    fn load_includes_recursive(
        &mut self,
        loaded_files: &mut HashSet<String>,
        base_dir: &std::path::Path,
        include_paths: &[String],
    ) -> Result<()> {
        for include_path in include_paths {
            let full_path = if std::path::Path::new(include_path).is_absolute() {
                include_path.clone()
            } else {
                // Standard approach: resolve relative to the config file's directory
                // This is the most common pattern used by Docker Compose, Nginx, etc.
                base_dir.join(include_path).to_string_lossy().to_string()
            };

            if loaded_files.contains(&full_path) {
                log::warn!("Circular include detected for file: {}", full_path);
                continue;
            }

            if !std::path::Path::new(&full_path).exists() {
                log::warn!(
                    "Include file not found: {} (base_dir: {}, include_path: {})",
                    full_path,
                    base_dir.display(),
                    include_path
                );
                continue;
            }

            match std::fs::read_to_string(&full_path) {
                Ok(content) => {
                    loaded_files.insert(full_path.clone());
                    log::info!("Loading include file: {}", full_path);

                    // includeファイル用の展開コンテキストを作成
                    let include_expand_context = ExpandContext::from_config_path(&full_path);

                    match Self::from_toml_internal(&content, false, &include_expand_context) {
                        Ok(included_config) => {
                            // from_toml_internal already calls normalize_defaults()
                            log::info!(
                                "Loaded {} tasks from {}",
                                included_config.tasks.len(),
                                full_path
                            );
                            for (name, task) in included_config.tasks {
                                if self.tasks.insert(name.clone(), task).is_some() {
                                    log::warn!(
                                        "Duplicate task name '{}' in include file {}, overwriting",
                                        name,
                                        full_path
                                    );
                                }
                            }
                            if !included_config.include_paths.is_empty() {
                                let include_dir = std::path::Path::new(&full_path)
                                    .parent()
                                    .unwrap_or(std::path::Path::new("."));

                                self.load_includes_recursive(
                                    loaded_files,
                                    include_dir,
                                    &included_config.include_paths,
                                )?;
                            }
                        }
                        Err(e) => {
                            log::error!("Failed to parse include file {}: {}", full_path, e);
                        }
                    }
                }
                Err(e) => {
                    log::error!("Failed to read include file {}: {}", full_path, e);
                }
            }
        }

        Ok(())
    }
}

impl RawSystemConfig {
    fn normalize_defaults(&mut self) {
        // デフォルト値は既にdeserialize時に設定されているので、ここでは何もしない
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
[[MiclowStdIOFunction]]
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

[[MiclowStdIO]]
task_name = "main_task"
command = "echo"
args = ["main"]
"#
            ),
        )
        .unwrap();

        let config = SystemConfig::from_file(main_file.to_string_lossy().to_string()).unwrap();

        assert_eq!(config.tasks.len(), 2, "Should have 2 tasks (1 from main file + 1 from include file)");
        assert_eq!(config.tasks.get("main_task").unwrap().name, "main_task");
        assert_eq!(config.tasks.get("test_function").unwrap().name, "test_function");
    }
}
