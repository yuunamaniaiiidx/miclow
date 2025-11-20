use crate::message_id::MessageId;
use crate::pod::PodId;

/// すべてのレスポンス topic が従うサフィックス。
pub const RESULT_TOPIC_SUFFIX: &str = ".result";

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicResponseStatus {
    Success,
    Error,
    Unknown,
}

impl Default for TopicResponseStatus {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Debug, Clone)]
pub enum ExecutorOutputEvent {
    Topic {
        message_id: MessageId,
        task_id: PodId,
        topic: String,
        data: String,
    },
    TopicResponse {
        message_id: MessageId,
        task_id: PodId,
        to_task_id: Option<PodId>,
        status: TopicResponseStatus,
        topic: String,
        return_topic: String,
        data: String,
    },
    Stdout {
        message_id: MessageId,
        task_id: PodId,
        data: String,
    },
    Stderr {
        message_id: MessageId,
        task_id: PodId,
        data: String,
    },
    Error {
        message_id: MessageId,
        task_id: PodId,
        error: String,
    },
    Exit {
        message_id: MessageId,
        task_id: PodId,
        exit_code: i32,
    },
}

impl ExecutorOutputEvent {
    pub fn new_message(
        message_id: MessageId,
        task_id: PodId,
        topic: String,
        data: String,
    ) -> Self {
        Self::Topic {
            message_id,
            task_id,
            topic,
            data,
        }
    }

    pub fn new_error(message_id: MessageId, task_id: PodId, error: String) -> Self {
        Self::Error {
            message_id,
            task_id,
            error,
        }
    }

    pub fn new_exit(message_id: MessageId, task_id: PodId, exit_code: i32) -> Self {
        Self::Exit {
            message_id,
            task_id,
            exit_code,
        }
    }

    pub fn new_task_stdout(message_id: MessageId, task_id: PodId, data: String) -> Self {
        Self::Stdout {
            message_id,
            task_id,
            data,
        }
    }

    pub fn new_task_stderr(message_id: MessageId, task_id: PodId, data: String) -> Self {
        Self::Stderr {
            message_id,
            task_id,
            data,
        }
    }

    pub fn data(&self) -> Option<&String> {
        match self {
            Self::Topic { data, .. } => Some(data),
            Self::TopicResponse { data, .. } => Some(data),
            Self::Stdout { data, .. } => Some(data),
            Self::Stderr { data, .. } => Some(data),
            _ => None,
        }
    }

    pub fn topic(&self) -> Option<&String> {
        match self {
            Self::Topic { topic, .. } => Some(topic),
            Self::TopicResponse { return_topic, .. } => Some(return_topic),
            _ => None,
        }
    }
}
