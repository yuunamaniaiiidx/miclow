pub mod executor_input_channel;
pub mod executor_output_channel;
pub mod shutdown_channel;
pub mod system_response_channel;
pub mod user_log;

// チャネルを再エクスポート
pub use executor_input_channel::{
    ExecutorInputEventChannel, ExecutorInputEventReceiver, ExecutorInputEventSender,
};

pub use executor_output_channel::{
    ExecutorOutputEventChannel, ExecutorOutputEventReceiver, ExecutorOutputEventSender,
};

pub use system_response_channel::{SystemResponseChannel, SystemResponseSender};

pub use shutdown_channel::{ShutdownChannel, ShutdownSender};

pub use user_log::UserLogSender;
