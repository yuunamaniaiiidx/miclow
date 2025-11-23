use crate::message_id::MessageId;
use crate::consumer::ConsumerId;
use crate::topic::Topic;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum PodEvent {
    PodExit {
        pod_id: ConsumerId,
    },
    PodTopic {
        pod_id: ConsumerId,
        message_id: MessageId,
        topic: Topic,
        data: Arc<str>,
    },
    PodIdle {
        pod_id: ConsumerId,
    },
}
