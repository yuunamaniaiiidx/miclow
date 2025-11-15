#[path = "trait.rs"]
pub mod backend_trait;
pub mod handle;
pub mod spawn_result;

pub use backend_trait::TaskBackend;
pub use handle::TaskBackendHandle;
pub use spawn_result::SpawnBackendResult;

