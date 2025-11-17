// 各プロトコルの主要な型と関数を再エクスポート
pub use crate::backend::protocol_backend::ProtocolBackend;
pub use crate::backend::interactive::{
    spawn_interactive_protocol, try_interactive_from_task_config, InteractiveConfig,
};
pub use crate::backend::mcp::{spawn_mcp_protocol, try_mcp_server_from_task_config, McpServerConfig};
pub use crate::backend::miclowstdio::{
    spawn_miclow_protocol, try_miclow_stdin_from_task_config, MiclowStdinConfig,
};
