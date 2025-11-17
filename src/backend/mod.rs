#[path = "trait.rs"]
pub mod backend_trait;
pub mod config;
pub mod dispatcher;
pub mod handle;
pub mod interactive;
pub mod mcp_server;
pub mod miclowstdio;
pub mod spawn_result;

pub use backend_trait::TaskBackend;
pub use config::{
    BackendConfigMeta, get_default_allow_duplicate, get_default_auto_start,
    get_default_view_stderr, get_default_view_stdout,
};
pub use dispatcher::ProtocolBackend;
pub use handle::TaskBackendHandle;
pub use interactive::config::{InteractiveConfig, try_interactive_from_expanded_config};
pub use mcp_server::config::{
    McpServerStdIOConfig, McpServerTcpConfig, try_mcp_server_stdio_from_expanded_config,
    try_mcp_server_tcp_from_expanded_config,
};
pub use miclowstdio::config::{MiclowStdIOConfig, try_miclow_stdio_from_expanded_config};
pub use spawn_result::SpawnBackendResult;
