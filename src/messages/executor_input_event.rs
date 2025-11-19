use crate::message_id::MessageId;
use crate::task_id::TaskId;

#[derive(Clone, Debug)]
pub enum ExecutorInputEvent {
    Topic {
        message_id: MessageId,
        task_id: TaskId,
        topic: String,
        data: String,
    },
}
