use crate::message_id::MessageId;
use crate::system_control::SystemControlAction;
use crate::task_id::TaskId;

#[derive(Debug, Clone)]
pub enum ExecutorOutputEvent {
    Topic {
        message_id: MessageId,
        task_id: TaskId,
        topic: String,
        data: String,
    },
    Stdout {
        message_id: MessageId,
        task_id: TaskId,
        data: String,
    },
    Stderr {
        message_id: MessageId,
        task_id: TaskId,
        data: String,
    },
    SystemControl {
        message_id: MessageId,
        task_id: TaskId,
        action: SystemControlAction,
    },
    FunctionResponse {
        message_id: MessageId,
        task_id: TaskId,
        return_to_task_id: TaskId,
        data: String,
    },
    Error {
        message_id: MessageId,
        task_id: TaskId,
        error: String,
    },
    Exit {
        message_id: MessageId,
        task_id: TaskId,
        exit_code: i32,
    },
}

impl ExecutorOutputEvent {
    pub fn new_message(message_id: MessageId, task_id: TaskId, topic: String, data: String) -> Self {
        Self::Topic {
            message_id,
            task_id,
            topic,
            data,
        }
    }

    pub fn new_error(message_id: MessageId, task_id: TaskId, error: String) -> Self {
        Self::Error {
            message_id,
            task_id,
            error,
        }
    }

    pub fn new_exit(message_id: MessageId, task_id: TaskId, exit_code: i32) -> Self {
        Self::Exit {
            message_id,
            task_id,
            exit_code,
        }
    }

    pub fn new_task_stdout(message_id: MessageId, task_id: TaskId, data: String) -> Self {
        Self::Stdout {
            message_id,
            task_id,
            data,
        }
    }

    pub fn new_task_stderr(message_id: MessageId, task_id: TaskId, data: String) -> Self {
        Self::Stderr {
            message_id,
            task_id,
            data,
        }
    }

    pub fn new_system_control(
        message_id: MessageId,
        task_id: TaskId,
        action: SystemControlAction,
    ) -> Self {
        Self::SystemControl {
            message_id,
            task_id,
            action,
        }
    }

    pub fn new_return_message(message_id: MessageId, task_id: TaskId, return_to_task_id: TaskId, data: String) -> Self {
        Self::FunctionResponse {
            message_id,
            task_id,
            return_to_task_id,
            data,
        }
    }

    pub fn data(&self) -> Option<&String> {
        match self {
            Self::Topic { data, .. } => Some(data),
            Self::Stdout { data, .. } => Some(data),
            Self::Stderr { data, .. } => Some(data),
            Self::FunctionResponse { data, .. } => Some(data),
            _ => None,
        }
    }

    pub fn topic(&self) -> Option<&String> {
        match self {
            Self::Topic { topic, .. } => Some(topic),
            _ => None,
        }
    }
}
