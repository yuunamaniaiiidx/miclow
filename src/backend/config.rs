use crate::backend::interactive::config::InteractiveConfig;
use crate::backend::mcp_server::config::{McpServerStdIOConfig, McpServerTcpConfig};
use crate::backend::miclowstdio::config::MiclowStdIOConfig;

/// バックエンド設定のメタ情報を提供するトレイト
pub trait BackendConfigMeta {
    /// プロトコル名を取得
    fn protocol_name() -> &'static str;
    
    /// デフォルトのallow_duplicate値を取得（タスク用）
    fn default_allow_duplicate_task() -> bool;
    
    /// デフォルトのallow_duplicate値を取得（関数用）
    fn default_allow_duplicate_function() -> bool;
    
    /// デフォルトのauto_start値を取得（タスク用）
    fn default_auto_start_task() -> bool;
    
    /// デフォルトのauto_start値を取得（関数用）
    fn default_auto_start_function() -> bool;
    
    /// デフォルトのview_stdout値を取得（タスク用）
    fn default_view_stdout_task() -> bool;
    
    /// デフォルトのview_stdout値を取得（関数用）
    fn default_view_stdout_function() -> bool;
    
    /// デフォルトのview_stderr値を取得（タスク用）
    fn default_view_stderr_task() -> bool;
    
    /// デフォルトのview_stderr値を取得（関数用）
    fn default_view_stderr_function() -> bool;
}

/// セクション名からデフォルトのallow_duplicate値を取得
pub fn get_default_allow_duplicate(section_name: &str) -> bool {
    match section_name {
        "Interactive" => InteractiveConfig::default_allow_duplicate_task(),
        "MiclowStdIO" => MiclowStdIOConfig::default_allow_duplicate_task(),
        "MiclowStdIOFunction" => MiclowStdIOConfig::default_allow_duplicate_function(),
        "McpServerStdIO" => McpServerStdIOConfig::default_allow_duplicate_function(),
        "McpServerTcp" => McpServerTcpConfig::default_allow_duplicate_function(),
        _ => false, // デフォルトのフォールバック
    }
}

/// セクション名からデフォルトのauto_start値を取得
pub fn get_default_auto_start(section_name: &str) -> bool {
    match section_name {
        "Interactive" => InteractiveConfig::default_auto_start_task(),
        "MiclowStdIO" => MiclowStdIOConfig::default_auto_start_task(),
        "MiclowStdIOFunction" => MiclowStdIOConfig::default_auto_start_function(),
        "McpServerStdIO" => McpServerStdIOConfig::default_auto_start_function(),
        "McpServerTcp" => McpServerTcpConfig::default_auto_start_function(),
        _ => true, // デフォルトのフォールバック
    }
}

/// セクション名からデフォルトのview_stdout値を取得
pub fn get_default_view_stdout(section_name: &str) -> bool {
    match section_name {
        "Interactive" => InteractiveConfig::default_view_stdout_task(),
        "MiclowStdIO" => MiclowStdIOConfig::default_view_stdout_task(),
        "MiclowStdIOFunction" => MiclowStdIOConfig::default_view_stdout_function(),
        "McpServerStdIO" => McpServerStdIOConfig::default_view_stdout_function(),
        "McpServerTcp" => McpServerTcpConfig::default_view_stdout_function(),
        _ => false, // デフォルトのフォールバック
    }
}

/// セクション名からデフォルトのview_stderr値を取得
pub fn get_default_view_stderr(section_name: &str) -> bool {
    match section_name {
        "Interactive" => InteractiveConfig::default_view_stderr_task(),
        "MiclowStdIO" => MiclowStdIOConfig::default_view_stderr_task(),
        "MiclowStdIOFunction" => MiclowStdIOConfig::default_view_stderr_function(),
        "McpServerStdIO" => McpServerStdIOConfig::default_view_stderr_function(),
        "McpServerTcp" => McpServerTcpConfig::default_view_stderr_function(),
        _ => false, // デフォルトのフォールバック
    }
}

