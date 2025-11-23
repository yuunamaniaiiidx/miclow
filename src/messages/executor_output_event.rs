use crate::message_id::MessageId;
use crate::consumer::ConsumerId;
use crate::subscription::SubscriptionId;
use crate::topic::Topic;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum ExecutorOutputEvent {
    Topic {
        message_id: MessageId,
        pod_id: ConsumerId,
        from_subscription_id: SubscriptionId,
        to_subscription_id: Option<SubscriptionId>,
        topic: Topic,
        data: Arc<str>,
    },
    Stdout {
        message_id: MessageId,
        pod_id: ConsumerId,
        data: Arc<str>,
    },
    Stderr {
        message_id: MessageId,
        pod_id: ConsumerId,
        data: Arc<str>,
    },
    Error {
        message_id: MessageId,
        pod_id: ConsumerId,
        error: String,
    },
    Exit {
        message_id: MessageId,
        pod_id: ConsumerId,
        exit_code: i32,
    },
}

impl ExecutorOutputEvent {
    pub fn new_message(
        message_id: MessageId,
        pod_id: ConsumerId,
        from_subscription_id: SubscriptionId,
        topic: impl Into<Topic>,
        data: impl Into<Arc<str>>,
    ) -> Self {
        Self::Topic {
            message_id,
            pod_id,
            from_subscription_id,
            to_subscription_id: None,
            topic: topic.into(),
            data: data.into(),
        }
    }

    pub fn new_error(message_id: MessageId, pod_id: ConsumerId, error: String) -> Self {
        Self::Error {
            message_id,
            pod_id,
            error,
        }
    }

    pub fn new_exit(message_id: MessageId, pod_id: ConsumerId, exit_code: i32) -> Self {
        Self::Exit {
            message_id,
            pod_id,
            exit_code,
        }
    }

    pub fn new_task_stdout(message_id: MessageId, pod_id: ConsumerId, data: impl Into<Arc<str>>) -> Self {
        Self::Stdout {
            message_id,
            pod_id,
            data: data.into(),
        }
    }

    pub fn new_task_stderr(message_id: MessageId, pod_id: ConsumerId, data: impl Into<Arc<str>>) -> Self {
        Self::Stderr {
            message_id,
            pod_id,
            data: data.into(),
        }
    }

    pub fn data(&self) -> Option<&str> {
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

    pub fn from_subscription_id(&self) -> Option<&SubscriptionId> {
        match self {
            Self::Topic { from_subscription_id, .. } => Some(from_subscription_id),
            _ => None,
        }
    }

    pub fn to_subscription_id(&self) -> Option<&SubscriptionId> {
        match self {
            Self::Topic { to_subscription_id, .. } => to_subscription_id.as_ref(),
            _ => None,
        }
    }

    pub fn subscription_id(&self) -> Option<&SubscriptionId> {
        // 後方互換性のため、from_subscription_idを返す
        self.from_subscription_id()
    }
}
