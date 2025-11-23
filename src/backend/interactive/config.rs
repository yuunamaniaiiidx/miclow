use crate::backend::config::BackendConfigMeta;
use crate::config::ExpandedTaskConfig;
use anyhow::Result;

#[derive(Clone, Debug)]
pub struct InteractiveConfig {
    pub system_input_topic: String,
}

impl InteractiveConfig {
    pub fn new(system_input_topic: String) -> Self {
        Self { system_input_topic }
    }
}

impl BackendConfigMeta for InteractiveConfig {
    fn protocol_name() -> &'static str {
        "Interactive"
    }

    fn default_view_stdout() -> bool {
        false
    }

    fn default_view_stderr() -> bool {
        false
    }
}

// Interactiveプロトコルで許可されているフィールド
const ALLOWED_INTERACTIVE_FIELDS: &[&str] = &[
    "stdout_topic",
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

pub fn try_interactive_from_expanded_config(
    config: &ExpandedTaskConfig,
) -> Result<InteractiveConfig, anyhow::Error> {
    // 無効なフィールドをチェック
    validate_protocol_fields(config, ALLOWED_INTERACTIVE_FIELDS, "Interactive")?;
    
    // InteractiveProtocol用のシステム入力トピック: stdout_topicが未設定の場合は"system"を使用
    let system_input_topic: String = config
        .expand("stdout_topic")
        .unwrap_or_else(|| "system".to_string());

    // バリデーション
    if system_input_topic.contains(' ') {
        return Err(anyhow::anyhow!(
            "Task '{}' stdout_topic '{}' contains spaces (not allowed)",
            config.name,
            system_input_topic
        ));
    }

    Ok(InteractiveConfig { system_input_topic })
}
