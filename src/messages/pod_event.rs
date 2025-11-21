use crate::message_id::MessageId;
use crate::messages::TopicResponseStatus;
use crate::pod::PodId;

#[derive(Clone, Debug)]
pub enum PodEvent {
    PodExit {
        pod_id: PodId,
    },
    PodTopicResponse {
        pod_id: PodId,
        message_id: MessageId,
        topic: String,
        return_topic: String,
        status: TopicResponseStatus,
        data: String,
    },
    PodTopic {
        pod_id: PodId,
        message_id: MessageId,
        topic: String,
        data: String,
    },
}
