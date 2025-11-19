pub mod config;
pub mod dispatcher;
pub mod handle;
pub mod interactive;
pub mod mcp_server;
pub mod miclowstdio;
pub mod spawn_result;

pub use config::{
    create_protocol_backend, get_default_view_stderr, get_default_view_stdout,
    get_force_allow_duplicate, get_force_auto_start, BackendConfigMeta,
};
pub use dispatcher::{ProtocolBackend, TaskBackend};
pub use handle::TaskBackendHandle;
pub use interactive::config::{try_interactive_from_expanded_config, InteractiveConfig};
pub use mcp_server::config::{
    try_mcp_server_stdio_from_expanded_config, try_mcp_server_tcp_from_expanded_config,
    McpServerStdIOConfig, McpServerTcpConfig,
};
pub use miclowstdio::config::{try_miclow_stdio_from_expanded_config, MiclowStdIOConfig};
pub use spawn_result::SpawnBackendResult;
