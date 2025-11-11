use crate::executor_event_channel::{ExecutorEventReceiver, ExecutorEventSender};
use crate::system_response_channel::SystemResponseSender;
use crate::input_channel::InputSender;
use crate::shutdown_channel::ShutdownSender;

pub struct TaskBackendHandle {
    pub event_receiver: ExecutorEventReceiver,
    pub event_sender: ExecutorEventSender,
    pub system_response_sender: SystemResponseSender,
    pub input_sender: InputSender,
    pub shutdown_sender: ShutdownSender,
}

