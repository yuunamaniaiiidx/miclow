use crate::task_id::TaskId;
use crate::system_control_command::SystemControlCommand;
use crate::system_response_channel::SystemResponseSender;
use crate::executor_event_channel::ExecutorEventSender;

pub struct SystemControlMessage {
    pub command: SystemControlCommand,
    pub task_id: TaskId,
    pub response_channel: SystemResponseSender,
    pub task_event_sender: ExecutorEventSender,
    pub return_message_sender: ExecutorEventSender,
}

impl std::fmt::Debug for SystemControlMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemControlMessage")
            .field("command", &"SystemControl")
            .field("task_id", &self.task_id)
            .field("response_channel", &"SystemResponseSender")
            .field("task_event_sender", &"ExecutorEventSender")
            .finish()
    }
}

impl SystemControlMessage {
    pub fn new(command: SystemControlCommand, task_id: TaskId, response_channel: SystemResponseSender, task_event_sender: ExecutorEventSender, return_message_sender: ExecutorEventSender) -> Self {
        Self {
            command,
            task_id,
            response_channel,
            task_event_sender,
            return_message_sender,
        }
    }
}

