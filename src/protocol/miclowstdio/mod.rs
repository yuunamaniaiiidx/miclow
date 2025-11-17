pub mod buffer;
pub mod protocol;

pub use protocol::{
    parse_return_message_from_outcome, parse_system_control_command_from_outcome,
    spawn_miclow_protocol, try_miclow_stdin_from_task_config, MiclowStdinConfig, StdinProtocol,
};
