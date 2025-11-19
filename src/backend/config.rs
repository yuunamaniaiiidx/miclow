use crate::backend::interactive::{try_interactive_from_expanded_config, InteractiveConfig};
use crate::backend::miclowstdio::{try_miclow_stdio_from_expanded_config, MiclowStdIOConfig};
use crate::backend::ProtocolBackend;
use crate::config::ExpandedTaskConfig;
use anyhow::{anyhow, Result};

/// バックエンド設定のメタ情報を提供するトレイト
pub trait BackendConfigMeta {
    /// プロトコル名を取得
    fn protocol_name() -> &'static str;

    /// デフォルトのview_stdout値を取得
    fn default_view_stdout() -> bool;

    /// デフォルトのview_stderr値を取得
    fn default_view_stderr() -> bool;
}

/// セクション名からデフォルトのview_stdout値を取得
pub fn get_default_view_stdout(section_name: &str) -> Result<bool, String> {
    match section_name {
        "Interactive" => Ok(InteractiveConfig::default_view_stdout()),
        "MiclowStdIO" => Ok(MiclowStdIOConfig::default_view_stdout()),
        _ => Err(format!(
            "Unknown section name '{}' for get_default_view_stdout. Supported sections: [[Interactive]], [[MiclowStdIO]]",
            section_name
        )),
    }
}

/// セクション名からデフォルトのview_stderr値を取得
pub fn get_default_view_stderr(section_name: &str) -> Result<bool, String> {
    match section_name {
        "Interactive" => Ok(InteractiveConfig::default_view_stderr()),
        "MiclowStdIO" => Ok(MiclowStdIOConfig::default_view_stderr()),
        _ => Err(format!(
            "Unknown section name '{}' for get_default_view_stderr. Supported sections: [[Interactive]], [[MiclowStdIO]]",
            section_name
        )),
    }
}

/// プロトコル名からProtocolBackendを作成
pub fn create_protocol_backend(
    protocol: &str,
    expanded: &ExpandedTaskConfig,
) -> Result<ProtocolBackend> {
    match protocol.trim() {
        "MiclowStdIO" => {
            let backend_config = try_miclow_stdio_from_expanded_config(expanded)?;
            Ok(ProtocolBackend::MiclowStdIO(backend_config))
        }
        "Interactive" => {
            let backend_config = try_interactive_from_expanded_config(expanded)?;
            Ok(ProtocolBackend::Interactive(backend_config))
        }
        _ => Err(anyhow!(
            "Unknown protocol '{}' for task '{}'. Supported protocols: MiclowStdIO, Interactive",
            protocol,
            expanded.name
        )),
    }
}
