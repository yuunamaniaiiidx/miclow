use crate::channels::{
    ExecutorInputEventSender, ExecutorOutputEventReceiver, ExecutorOutputEventSender,
    ShutdownSender,
};

pub struct TaskBackendHandle {
    pub event_receiver: ExecutorOutputEventReceiver,
    pub event_sender: ExecutorOutputEventSender,
    pub input_sender: ExecutorInputEventSender,
    pub shutdown_sender: ShutdownSender,
}
