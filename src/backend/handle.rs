use crate::channels::{ExecutorInputEventSender, ExecutorOutputEventReceiver, ShutdownSender};

pub struct TaskBackendHandle {
    pub event_receiver: ExecutorOutputEventReceiver,
    pub input_sender: ExecutorInputEventSender,
    pub shutdown_sender: ShutdownSender,
}
