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

    fn default_allow_duplicate_task() -> bool {
        false
    }

    fn default_allow_duplicate_function() -> bool {
        true
    }

    fn default_auto_start_task() -> bool {
        true
    }

    fn default_auto_start_function() -> bool {
        false
    }

    fn default_view_stdout_task() -> bool {
        false
    }

    fn default_view_stdout_function() -> bool {
        false
    }

    fn default_view_stderr_task() -> bool {
        false
    }

    fn default_view_stderr_function() -> bool {
        false
    }
}

pub fn try_interactive_from_expanded_config(
    config: &ExpandedTaskConfig,
) -> Result<InteractiveConfig, anyhow::Error> {
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

