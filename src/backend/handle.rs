use crate::chunnel::{ExecutorEventReceiver, ExecutorEventSender};
use crate::chunnel::SystemResponseSender;
use crate::chunnel::InputSender;
use crate::chunnel::ShutdownSender;

pub struct TaskBackendHandle {
    pub event_receiver: ExecutorEventReceiver,
    pub event_sender: ExecutorEventSender,
    pub system_response_sender: SystemResponseSender,
    pub input_sender: InputSender,
    pub shutdown_sender: ShutdownSender,
}

