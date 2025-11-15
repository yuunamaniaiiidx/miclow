pub mod miclowstdio;
pub mod interactive;
pub mod mcp;
pub mod backend;

// 各プロトコルの主要な型と関数を再エクスポート
pub use miclowstdio::{MiclowStdinConfig, try_miclow_stdin_from_task_config, spawn_miclow_protocol};
pub use interactive::{InteractiveConfig, try_interactive_from_task_config, spawn_interactive_protocol};
pub use mcp::{McpServerConfig, try_mcp_server_from_task_config, spawn_mcp_protocol};
pub use backend::ProtocolBackend;

