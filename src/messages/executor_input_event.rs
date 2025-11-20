use crate::message_id::MessageId;
use crate::messages::TopicResponseStatus;
use crate::pod::PodId;

#[derive(Clone, Debug)]
pub enum ExecutorInputEvent {
    Topic {
        message_id: MessageId,
        pod_id: PodId,
        topic: String,
        data: String,
    },
    TopicResponse {
        message_id: MessageId,
        pod_id: PodId,
        status: TopicResponseStatus,
        topic: String,
        return_topic: String,
        data: String,
    },
}
