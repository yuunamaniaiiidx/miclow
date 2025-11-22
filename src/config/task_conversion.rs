use crate::backend::create_protocol_backend;
use crate::config::expansion::{ExpandContext, Expandable};
use crate::config::value_conversion::FromTomlValue;
use crate::config::{
    expand_toml_value, ExpandedTaskConfig, LifecycleConfig, RawTaskConfig, TaskConfig,
};
use anyhow::Result;
use std::collections::HashMap;
use toml::Value as TomlValue;

impl RawTaskConfig {
    /// 環境変数展開を実行してExpandedTaskConfigに変換
    pub fn expand(self, context: &ExpandContext) -> Result<ExpandedTaskConfig> {
        // 文字列フィールドの展開
        let expanded_name = expand_toml_value(&self.name, context)?;
        let name = String::from_toml_value(&expanded_name)
            .ok_or_else(|| anyhow::anyhow!("task_name must be a string"))?;

        let subscribe_topics = if let Some(raw_value) = self.subscribe_topics {
            let expanded_value = expand_toml_value(&raw_value, context)?;
            Vec::<String>::from_toml_value(&expanded_value)
                .ok_or_else(|| anyhow::anyhow!("subscribe_topics must be an array of strings"))?
        } else {
            Vec::new()
        };

        let private_response_topics = if let Some(raw_value) = self.private_response_topics {
            let expanded_value = expand_toml_value(&raw_value, context)?;
            Vec::<String>::from_toml_value(&expanded_value)
                .ok_or_else(|| anyhow::anyhow!("private_response_topics must be an array of strings"))?
        } else {
            Vec::new()
        };

        let view_stdout = expand_bool_field(self.view_stdout, context, "view_stdout")?
            .expect("view_stdout should be set by normalize_defaults()");

        let view_stderr = expand_bool_field(self.view_stderr, context, "view_stderr")?
            .expect("view_stderr should be set by normalize_defaults()");

        let lifecycle = if let Some(raw_lifecycle) = self.lifecycle {
            raw_lifecycle.expand(context)?
        } else {
            LifecycleConfig::default()
        };

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
            private_response_topics,
            view_stdout,
            view_stderr,
            lifecycle,
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
            private_response_topics: expanded.private_response_topics,
            view_stdout: expanded.view_stdout,
            view_stderr: expanded.view_stderr,
            lifecycle: expanded.lifecycle,
            protocol_config: expanded.protocol_config,
            protocol_backend,
        })
    }
}

/// boolフィールドの展開を実行するヘルパー関数
fn expand_bool_field(
    raw_value: Option<TomlValue>,
    context: &ExpandContext,
    field_name: &str,
) -> Result<Option<bool>> {
    raw_value
        .map(|raw_value| {
            let expanded_value = expand_toml_value(&raw_value, context)?;
            bool::from_toml_value(&expanded_value)
                .ok_or_else(|| anyhow::anyhow!("{} must be a boolean", field_name))
        })
        .transpose()
}
