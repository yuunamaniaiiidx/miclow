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
pub fn get_default_allow_duplicate(section_name: &str) -> bool {
    match section_name {
        "Interactive" => InteractiveConfig::default_allow_duplicate(),
        "MiclowStdIO" => MiclowStdIOConfig::default_allow_duplicate(),
        "McpServerStdIO" => McpServerStdIOConfig::default_allow_duplicate(),
        "McpServerTcp" => McpServerTcpConfig::default_allow_duplicate(),
        _ => false, // デフォルトのフォールバック
    }
}

/// セクション名からデフォルトのauto_start値を取得
pub fn get_default_auto_start(section_name: &str) -> bool {
    match section_name {
        "Interactive" => InteractiveConfig::default_auto_start(),
        "MiclowStdIO" => MiclowStdIOConfig::default_auto_start(),
        "McpServerStdIO" => McpServerStdIOConfig::default_auto_start(),
        "McpServerTcp" => McpServerTcpConfig::default_auto_start(),
        _ => true, // デフォルトのフォールバック
    }
}

/// セクション名からデフォルトのview_stdout値を取得
pub fn get_default_view_stdout(section_name: &str) -> bool {
    match section_name {
        "Interactive" => InteractiveConfig::default_view_stdout(),
        "MiclowStdIO" => MiclowStdIOConfig::default_view_stdout(),
        "McpServerStdIO" => McpServerStdIOConfig::default_view_stdout(),
        "McpServerTcp" => McpServerTcpConfig::default_view_stdout(),
        _ => false, // デフォルトのフォールバック
    }
}

/// セクション名からデフォルトのview_stderr値を取得
pub fn get_default_view_stderr(section_name: &str) -> bool {
    match section_name {
        "Interactive" => InteractiveConfig::default_view_stderr(),
        "MiclowStdIO" => MiclowStdIOConfig::default_view_stderr(),
        "McpServerStdIO" => McpServerStdIOConfig::default_view_stderr(),
        "McpServerTcp" => McpServerTcpConfig::default_view_stderr(),
        _ => false, // デフォルトのフォールバック
    }
}

