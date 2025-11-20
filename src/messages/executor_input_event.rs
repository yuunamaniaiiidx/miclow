use crate::message_id::MessageId;
use crate::pod::PodId;

#[derive(Clone, Debug)]
pub enum ExecutorInputEvent {
    Topic {
        message_id: MessageId,
        task_id: PodId,
        topic: String,
        data: String,
    },
}
