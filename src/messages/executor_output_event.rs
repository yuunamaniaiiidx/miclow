use crate::message_id::MessageId;
use crate::pod::PodId;
use crate::replicaset::ReplicaSetId;
use crate::topic::Topic;

#[derive(Debug, Clone)]
pub enum ExecutorOutputEvent {
    Topic {
        message_id: MessageId,
        pod_id: PodId,
        replicaset_id: ReplicaSetId,
        topic: Topic,
        data: String,
    },
    Stdout {
        message_id: MessageId,
        pod_id: PodId,
        data: String,
    },
    Stderr {
        message_id: MessageId,
        pod_id: PodId,
        data: String,
    },
    Error {
        message_id: MessageId,
        pod_id: PodId,
        error: String,
    },
    Exit {
        message_id: MessageId,
        pod_id: PodId,
        exit_code: i32,
    },
}

impl ExecutorOutputEvent {
    pub fn new_message(
        message_id: MessageId,
        pod_id: PodId,
        replicaset_id: ReplicaSetId,
        topic: impl Into<Topic>,
        data: String,
    ) -> Self {
        Self::Topic {
            message_id,
            pod_id,
            replicaset_id,
            topic: topic.into(),
            data,
        }
    }

    pub fn new_error(message_id: MessageId, pod_id: PodId, error: String) -> Self {
        Self::Error {
            message_id,
            pod_id,
            error,
        }
    }

    pub fn new_exit(message_id: MessageId, pod_id: PodId, exit_code: i32) -> Self {
        Self::Exit {
            message_id,
            pod_id,
            exit_code,
        }
    }

    pub fn new_task_stdout(message_id: MessageId, pod_id: PodId, data: String) -> Self {
        Self::Stdout {
            message_id,
            pod_id,
            data,
        }
    }

    pub fn new_task_stderr(message_id: MessageId, pod_id: PodId, data: String) -> Self {
        Self::Stderr {
            message_id,
            pod_id,
            data,
        }
    }

    pub fn data(&self) -> Option<&String> {
        match self {
            Self::Topic { data, .. } => Some(data),
            Self::Stdout { data, .. } => Some(data),
            Self::Stderr { data, .. } => Some(data),
            _ => None,
        }
    }

    pub fn topic(&self) -> Option<&Topic> {
        match self {
            Self::Topic { topic, .. } => Some(topic),
            _ => None,
        }
    }

    pub fn replicaset_id(&self) -> Option<&ReplicaSetId> {
        match self {
            Self::Topic { replicaset_id, .. } => Some(replicaset_id),
            _ => None,
        }
    }
}
