use crate::message_id::MessageId;
use crate::pod::PodId;
use crate::replicaset::ReplicaSetId;
use crate::topic::Topic;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum ExecutorInputEvent {
    Topic {
        message_id: MessageId,
        pod_id: PodId,
        topic: Topic,
        data: Arc<str>,
        from_replicaset_id: ReplicaSetId,
    },
}
