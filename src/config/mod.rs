pub mod expansion;

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
    pub protocol: TomlValue,
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
    pub protocol: String,
    pub subscribe_topics: Option<Vec<String>>,
    pub allow_duplicate: bool,
    pub auto_start: bool,
    pub view_stdout: bool,
    pub view_stderr: bool,

    /// プロトコル固有の設定（プロトコル非依存のフィールド以外のすべて）
    pub protocol_config: HashMap<String, TomlValue>,
}

impl<'de> serde::Deserialize<'de> for RawTaskConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // まず全体をTOMLのテーブルとして読み込む
        let mut protocol_config: HashMap<String, TomlValue> = HashMap::deserialize(deserializer)?;

        // プロトコル非依存のフィールドをpopで取り出す
        let name = protocol_config
            .remove("task_name")
            .ok_or_else(|| serde::de::Error::missing_field("task_name"))?;

        let protocol = protocol_config
            .remove("protocol")
            .ok_or_else(|| serde::de::Error::missing_field("protocol"))?;

        let subscribe_topics = protocol_config.remove("subscribe_topics");

        let allow_duplicate = protocol_config.remove("allow_duplicate");
        let auto_start = protocol_config.remove("auto_start");
        let view_stdout = protocol_config.remove("view_stdout");
        let view_stderr = protocol_config.remove("view_stderr");

        // 残りが全てprotocol_configに入っている

        Ok(RawTaskConfig {
            name,
            protocol,
            subscribe_topics,
            allow_duplicate,
            auto_start,
            view_stdout,
            view_stderr,
            protocol_config,
        })
    }
}

impl RawTaskConfig {
    /// 環境変数展開を実行してTaskConfigに変換
    pub fn expand(self, context: &ExpandContext) -> Result<TaskConfig> {
        // 文字列フィールドの展開
        let expanded_name = expand_toml_value(&self.name, context)?;
        let name = String::from_toml_value(&expanded_name)
            .ok_or_else(|| anyhow::anyhow!("task_name must be a string"))?;

        let expanded_protocol = expand_toml_value(&self.protocol, context)?;
        let protocol = String::from_toml_value(&expanded_protocol)
            .ok_or_else(|| anyhow::anyhow!("protocol must be a string"))?;

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
            .unwrap_or(false); // normalize_defaults()で既に設定されているはずだが、念のため

        let auto_start = self
            .auto_start
            .map(|raw_value| {
                let expanded_value = expand_toml_value(&raw_value, context)?;
                bool::from_toml_value(&expanded_value)
                    .ok_or_else(|| anyhow::anyhow!("auto_start must be a boolean"))
            })
            .transpose()?
            .unwrap_or(false); // normalize_defaults()で既に設定されているはずだが、念のため

        let view_stdout = self
            .view_stdout
            .map(|raw_value| {
                let expanded_value = expand_toml_value(&raw_value, context)?;
                bool::from_toml_value(&expanded_value)
                    .ok_or_else(|| anyhow::anyhow!("view_stdout must be a boolean"))
            })
            .transpose()?
            .unwrap_or(false);

        let view_stderr = self
            .view_stderr
            .map(|raw_value| {
                let expanded_value = expand_toml_value(&raw_value, context)?;
                bool::from_toml_value(&expanded_value)
                    .ok_or_else(|| anyhow::anyhow!("view_stderr must be a boolean"))
            })
            .transpose()?
            .unwrap_or(false);

        // プロトコル固有設定の展開
        let mut protocol_config = HashMap::new();
        for (key, value) in self.protocol_config {
            let expanded_key = key.expand(context)?;
            let expanded_value = expand_toml_value(&value, context)?;
            protocol_config.insert(expanded_key, expanded_value);
        }

        Ok(TaskConfig {
            name,
            protocol,
            subscribe_topics,
            allow_duplicate,
            auto_start,
            view_stdout,
            view_stderr,
            protocol_config,
        })
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
    functions: Vec<RawTaskConfig>,
    include_paths: Option<TomlValue>,
}

impl<'de> serde::Deserialize<'de> for RawSystemConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct Helper {
            #[serde(default)]
            tasks: Vec<RawTaskConfig>,
            #[serde(default)]
            functions: Vec<RawTaskConfig>,
            #[serde(default)]
            include_paths: Option<TomlValue>,
        }

        let helper = Helper::deserialize(deserializer)?;
        Ok(RawSystemConfig {
            tasks: helper.tasks,
            functions: helper.functions,
            include_paths: helper.include_paths,
        })
    }
}

#[derive(Debug, Clone)]
pub struct SystemConfig {
    pub tasks: HashMap<String, TaskConfig>,
    pub functions: HashMap<String, TaskConfig>,
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

        // 変数展開を実行してTaskConfigに変換
        let mut tasks = HashMap::new();
        for task in raw.tasks {
            let expanded = task.expand(expand_context)?;
            let name = expanded.name.clone();
            if tasks.insert(name.clone(), expanded).is_some() {
                return Err(anyhow::anyhow!("Duplicate task name: {}", name));
            }
        }

        let mut functions = HashMap::new();
        for func in raw.functions {
            let expanded = func.expand(expand_context)?;
            let name = expanded.name.clone();
            if functions.insert(name.clone(), expanded).is_some() {
                return Err(anyhow::anyhow!("Duplicate function name: {}", name));
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
            functions,
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
            "Before load_includes: {} tasks, {} functions",
            config.tasks.len(),
            config.functions.len()
        );
        config.load_includes(&config_file)?;
        log::debug!(
            "After load_includes: {} tasks, {} functions",
            config.tasks.len(),
            config.functions.len()
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
            .chain(self.functions.values())
            .filter(|task| task.auto_start)
            .collect()
    }

    pub fn validate(&self) -> Result<()> {
        let autostart_count = self.tasks.values().filter(|t| t.auto_start).count()
            + self.functions.values().filter(|t| t.auto_start).count();
        if autostart_count == 0 {
            return Err(anyhow::anyhow!("No autostart tasks configured"));
        }

        // プロトコル非依存のバリデーションのみ実行
        // プロトコル固有のバリデーションはProtocolBackend::try_fromで実行される
        self.validate_tasks()?;
        self.validate_functions()?;

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

            if task.protocol.is_empty() {
                return Err(anyhow::anyhow!("Task '{}' has empty protocol", task.name));
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

    /// 関数のプロトコル非依存バリデーション
    fn validate_functions(&self) -> Result<()> {
        for (name, task) in &self.functions {
            if task.name.is_empty() {
                return Err(anyhow::anyhow!("Function '{}' has empty name", name));
            }

            if task.protocol.is_empty() {
                return Err(anyhow::anyhow!(
                    "Function '{}' has empty protocol",
                    task.name
                ));
            }

            if let Some(subscribe_topics) = &task.subscribe_topics {
                for (topic_index, topic) in subscribe_topics.iter().enumerate() {
                    if topic.is_empty() {
                        return Err(anyhow::anyhow!(
                            "Function '{}' has empty initial topic at index {}",
                            task.name,
                            topic_index
                        ));
                    }
                    if topic.contains(' ') {
                        return Err(anyhow::anyhow!(
                            "Function '{}' initial topic '{}' contains spaces (not allowed)",
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
                                "Loaded {} tasks and {} functions from {}",
                                included_config.tasks.len(),
                                included_config.functions.len(),
                                full_path
                            );
                            for func in included_config.functions.values() {
                                log::info!("  Function: {}", func.name);
                            }
                            for (name, task) in included_config.tasks {
                                if self.tasks.insert(name.clone(), task).is_some() {
                                    log::warn!(
                                        "Duplicate task name '{}' in include file {}, overwriting",
                                        name,
                                        full_path
                                    );
                                }
                            }
                            for (name, func) in included_config.functions {
                                if self.functions.insert(name.clone(), func).is_some() {
                                    log::warn!("Duplicate function name '{}' in include file {}, overwriting", name, full_path);
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
        // Tasks: allow_duplicate = false, auto_start = true
        for task in &mut self.tasks {
            if task.allow_duplicate.is_none() {
                task.allow_duplicate = Some(TomlValue::Boolean(false));
            }
            if task.auto_start.is_none() {
                task.auto_start = Some(TomlValue::Boolean(true));
            }
            if task.view_stdout.is_none() {
                task.view_stdout = Some(TomlValue::Boolean(false));
            }
            if task.view_stderr.is_none() {
                task.view_stderr = Some(TomlValue::Boolean(false));
            }
        }

        // Functions: allow_duplicate = true, auto_start = false
        for task in &mut self.functions {
            if task.allow_duplicate.is_none() {
                task.allow_duplicate = Some(TomlValue::Boolean(true));
            }
            if task.auto_start.is_none() {
                task.auto_start = Some(TomlValue::Boolean(false));
            }
            if task.view_stdout.is_none() {
                task.view_stdout = Some(TomlValue::Boolean(false));
            }
            if task.view_stderr.is_none() {
                task.view_stderr = Some(TomlValue::Boolean(false));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_load_functions_from_include() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path();

        // Create include file with functions
        let include_file = base_dir.join("include.toml");
        fs::write(
            &include_file,
            r#"
[[functions]]
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
            config.functions.len(),
            1,
            "Should have 1 function from include file"
        );
        assert_eq!(
            config.functions.get("test_function").unwrap().name,
            "test_function"
        );
        assert_eq!(config.tasks.len(), 1, "Should have 1 task from main file");
        assert_eq!(config.tasks.get("main_task").unwrap().name, "main_task");
    }
}
