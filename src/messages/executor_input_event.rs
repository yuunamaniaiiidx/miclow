use crate::message_id::MessageId;
use crate::messages::TopicResponseStatus;
use crate::pod::PodId;

#[derive(Clone, Debug)]
pub enum ExecutorInputEvent {
    Topic {
        message_id: MessageId,
        task_id: PodId,
        topic: String,
        data: String,
    },
    TopicResponse {
        message_id: MessageId,
        task_id: PodId,
        status: TopicResponseStatus,
        topic: String,
        return_topic: String,
        data: String,
    },
}
