use crate::backend::config::BackendConfigMeta;
use crate::config::ExpandedTaskConfig;
use anyhow::Result;

#[derive(Clone, Debug)]
pub struct InteractiveConfig {
    pub system_input_topic: String,
    pub functions: Vec<String>,
}

impl InteractiveConfig {
    pub fn new(system_input_topic: String) -> Self {
        Self {
            system_input_topic,
            functions: Vec::new(),
        }
    }
}

impl BackendConfigMeta for InteractiveConfig {
    fn protocol_name() -> &'static str {
        "Interactive"
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

    // allow_duplicateとauto_startはバックエンド実装によって固定されているため、
    // ユーザーが設定していた場合はエラーとする
    // このチェックはnormalize_defaults()で既に行われているが、念のためここでも確認

    let functions: Vec<String> = config.functions.clone();

    Ok(InteractiveConfig {
        system_input_topic,
        functions,
    })
}
