pub mod executor_event;
pub mod input;
pub mod system_response;
pub mod shutdown;
pub mod user_log;

// 各チャネルモジュールの主要な型と関数を再エクスポート
pub use executor_event::{
    ExecutorEvent,
    ExecutorEventSender,
    ExecutorEventReceiver,
    ExecutorEventChannel,
};

pub use input::{
    TopicMessage,
    SystemResponseMessage,
    ReturnMessage,
    FunctionMessage,
    InputDataMessage,
    InputSender,
    InputReceiver,
    InputChannel,
};

pub use system_response::{
    SystemResponseStatus,
    SystemResponseEvent,
    SystemResponseSender,
    SystemResponseReceiver,
    SystemResponseChannel,
};

pub use shutdown::{
    ShutdownSender,
    ShutdownChannel,
};

pub use user_log::{
    UserLogSender,
};

