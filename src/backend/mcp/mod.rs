pub mod config;
pub mod runner;

pub use config::{try_mcp_from_expanded_config, MCPConfig};
pub use runner::spawn_mcp_protocol;

