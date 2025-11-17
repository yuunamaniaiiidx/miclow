#[path = "trait.rs"]
pub mod backend_trait;
pub mod handle;
pub mod interactive;
pub mod mcp;
pub mod miclowstdio;
pub mod dispatcher;
pub mod protocol;
pub mod spawn_result;

pub use backend_trait::TaskBackend;
pub use handle::TaskBackendHandle;
pub use protocol::ProtocolBackend;
pub use spawn_result::SpawnBackendResult;
