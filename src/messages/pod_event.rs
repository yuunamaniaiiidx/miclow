use crate::message_id::MessageId;
use crate::pod::PodId;
use crate::topic::Topic;

#[derive(Clone, Debug)]
pub enum PodEvent {
    PodExit {
        pod_id: PodId,
    },
    PodTopic {
        pod_id: PodId,
        message_id: MessageId,
        topic: Topic,
        data: String,
    },
}
