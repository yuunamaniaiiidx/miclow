use crate::backend::interactive::config::InteractiveConfig;
use crate::backend::mcp_server::config::{McpServerStdIOConfig, McpServerTcpConfig};
use crate::backend::miclowstdio::config::MiclowStdIOConfig;

/// バックエンド設定のメタ情報を提供するトレイト
pub trait BackendConfigMeta {
    /// プロトコル名を取得
    fn protocol_name() -> &'static str;
    
    /// デフォルトのallow_duplicate値を取得
    fn default_allow_duplicate() -> bool;
    
    /// デフォルトのauto_start値を取得
    fn default_auto_start() -> bool;
    
    /// デフォルトのview_stdout値を取得
    fn default_view_stdout() -> bool;
    
    /// デフォルトのview_stderr値を取得
    fn default_view_stderr() -> bool;
}

/// セクション名からデフォルトのallow_duplicate値を取得
pub fn get_default_allow_duplicate(section_name: &str) -> Result<bool, String> {
    match section_name {
        "Interactive" => Ok(InteractiveConfig::default_allow_duplicate()),
        "MiclowStdIO" => Ok(MiclowStdIOConfig::default_allow_duplicate()),
        "McpServerStdIO" => Ok(McpServerStdIOConfig::default_allow_duplicate()),
        "McpServerTcp" => Ok(McpServerTcpConfig::default_allow_duplicate()),
        _ => Err(format!(
            "Unknown section name '{}' for get_default_allow_duplicate. Supported sections: [[Interactive]], [[MiclowStdIO]], [[McpServerStdIO]], [[McpServerTcp]]",
            section_name
        )),
    }
}

/// セクション名からデフォルトのauto_start値を取得
pub fn get_default_auto_start(section_name: &str) -> Result<bool, String> {
    match section_name {
        "Interactive" => Ok(InteractiveConfig::default_auto_start()),
        "MiclowStdIO" => Ok(MiclowStdIOConfig::default_auto_start()),
        "McpServerStdIO" => Ok(McpServerStdIOConfig::default_auto_start()),
        "McpServerTcp" => Ok(McpServerTcpConfig::default_auto_start()),
        _ => Err(format!(
            "Unknown section name '{}' for get_default_auto_start. Supported sections: [[Interactive]], [[MiclowStdIO]], [[McpServerStdIO]], [[McpServerTcp]]",
            section_name
        )),
    }
}

/// セクション名からデフォルトのview_stdout値を取得
pub fn get_default_view_stdout(section_name: &str) -> Result<bool, String> {
    match section_name {
        "Interactive" => Ok(InteractiveConfig::default_view_stdout()),
        "MiclowStdIO" => Ok(MiclowStdIOConfig::default_view_stdout()),
        "McpServerStdIO" => Ok(McpServerStdIOConfig::default_view_stdout()),
        "McpServerTcp" => Ok(McpServerTcpConfig::default_view_stdout()),
        _ => Err(format!(
            "Unknown section name '{}' for get_default_view_stdout. Supported sections: [[Interactive]], [[MiclowStdIO]], [[McpServerStdIO]], [[McpServerTcp]]",
            section_name
        )),
    }
}

/// セクション名からデフォルトのview_stderr値を取得
pub fn get_default_view_stderr(section_name: &str) -> Result<bool, String> {
    match section_name {
        "Interactive" => Ok(InteractiveConfig::default_view_stderr()),
        "MiclowStdIO" => Ok(MiclowStdIOConfig::default_view_stderr()),
        "McpServerStdIO" => Ok(McpServerStdIOConfig::default_view_stderr()),
        "McpServerTcp" => Ok(McpServerTcpConfig::default_view_stderr()),
        _ => Err(format!(
            "Unknown section name '{}' for get_default_view_stderr. Supported sections: [[Interactive]], [[MiclowStdIO]], [[McpServerStdIO]], [[McpServerTcp]]",
            section_name
        )),
    }
}

