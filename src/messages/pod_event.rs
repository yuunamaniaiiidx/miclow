use crate::message_id::MessageId;
use crate::pod::PodId;

#[derive(Clone, Debug)]
pub enum PodEvent {
    PodExit {
        pod_id: PodId,
    },
    PodResponse {
        pod_id: PodId,
        message_id: MessageId,
    },
}
