pub mod backend;
pub mod interactive;
pub mod mcp;
pub mod miclowstdio;

// 各プロトコルの主要な型と関数を再エクスポート
pub use backend::ProtocolBackend;
pub use interactive::{
    spawn_interactive_protocol, try_interactive_from_task_config, InteractiveConfig,
};
pub use mcp::{spawn_mcp_protocol, try_mcp_server_from_task_config, McpServerConfig};
pub use miclowstdio::{
    spawn_miclow_protocol, try_miclow_stdin_from_task_config, MiclowStdinConfig,
};
