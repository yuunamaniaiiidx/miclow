#[path = "trait.rs"]
pub mod backend_trait;
pub mod dispatcher;
pub mod handle;
pub mod interactive;
pub mod mcp_server;
pub mod miclowstdio;
pub mod spawn_result;

pub use backend_trait::TaskBackend;
pub use dispatcher::ProtocolBackend;
pub use handle::TaskBackendHandle;
pub use spawn_result::SpawnBackendResult;
