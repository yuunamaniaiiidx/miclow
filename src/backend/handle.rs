use crate::channels::{
    ExecutorOutputEventReceiver, ExecutorOutputEventSender, ExecutorInputEventSender, ShutdownSender, SystemResponseSender,
};

pub struct TaskBackendHandle {
    pub event_receiver: ExecutorOutputEventReceiver,
    pub event_sender: ExecutorOutputEventSender,
    pub system_response_sender: SystemResponseSender,
    pub input_sender: ExecutorInputEventSender,
    pub shutdown_sender: ShutdownSender,
}
