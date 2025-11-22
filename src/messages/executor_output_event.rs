use crate::message_id::MessageId;
use crate::pod::PodId;
use crate::replicaset::ReplicaSetId;
use crate::topic::Topic;

#[derive(Debug, Clone)]
pub enum ExecutorOutputEvent {
    Topic {
        message_id: MessageId,
        pod_id: PodId,
        from_replicaset_id: ReplicaSetId,
        to_replicaset_id: Option<ReplicaSetId>,
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
        from_replicaset_id: ReplicaSetId,
        topic: impl Into<Topic>,
        data: String,
    ) -> Self {
        Self::Topic {
            message_id,
            pod_id,
            from_replicaset_id,
            to_replicaset_id: None,
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

    pub fn from_replicaset_id(&self) -> Option<&ReplicaSetId> {
        match self {
            Self::Topic { from_replicaset_id, .. } => Some(from_replicaset_id),
            _ => None,
        }
    }

    pub fn to_replicaset_id(&self) -> Option<&ReplicaSetId> {
        match self {
            Self::Topic { to_replicaset_id, .. } => to_replicaset_id.as_ref(),
            _ => None,
        }
    }

    pub fn replicaset_id(&self) -> Option<&ReplicaSetId> {
        // 後方互換性のため、from_replicaset_idを返す
        self.from_replicaset_id()
    }
}
