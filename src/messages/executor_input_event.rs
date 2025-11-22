use crate::message_id::MessageId;
use crate::pod::PodId;
use crate::replicaset::ReplicaSetId;
use crate::topic::Topic;

#[derive(Clone, Debug)]
pub enum ExecutorInputEvent {
    Topic {
        message_id: MessageId,
        pod_id: PodId,
        topic: Topic,
        data: String,
        from_replicaset_id: ReplicaSetId,
    },
}
