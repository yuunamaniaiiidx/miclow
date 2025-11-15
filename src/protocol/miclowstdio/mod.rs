pub mod buffer;
pub mod protocol;

pub use protocol::{MiclowStdinConfig, StdinProtocol, try_miclow_stdin_from_task_config, spawn_miclow_protocol, parse_system_control_command_from_outcome, parse_return_message_from_outcome};

