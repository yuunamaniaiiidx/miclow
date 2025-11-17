#[derive(Debug, Clone)]
pub enum ExecutorEvent {
    Message { topic: String, data: String },
    TaskStdout { data: String },
    TaskStderr { data: String },
    SystemControl { key: String, data: String },
    ReturnMessage { data: String },
    FunctionResponse { function_name: String, data: String },
    Error { error: String },
    Exit { exit_code: i32 },
}

impl ExecutorEvent {
    pub fn new_message(topic: String, data: String) -> Self {
        Self::Message { topic, data }
    }

    pub fn new_error(error: String) -> Self {
        Self::Error { error }
    }

    pub fn new_exit(exit_code: i32) -> Self {
        Self::Exit { exit_code }
    }

    pub fn new_task_stdout(data: String) -> Self {
        Self::TaskStdout { data }
    }

    pub fn new_task_stderr(data: String) -> Self {
        Self::TaskStderr { data }
    }

    pub fn new_system_control(key: String, data: String) -> Self {
        Self::SystemControl { key, data }
    }

    pub fn new_return_message(data: String) -> Self {
        Self::ReturnMessage { data }
    }

    pub fn new_function_response(function_name: String, data: String) -> Self {
        Self::FunctionResponse {
            function_name,
            data,
        }
    }

    pub fn data(&self) -> Option<&String> {
        match self {
            Self::Message { data, .. } => Some(data),
            Self::TaskStdout { data } => Some(data),
            Self::TaskStderr { data } => Some(data),
            Self::ReturnMessage { data } => Some(data),
            Self::FunctionResponse { data, .. } => Some(data),
            _ => None,
        }
    }

    pub fn topic(&self) -> Option<&String> {
        match self {
            Self::Message { topic, .. } => Some(topic),
            _ => None,
        }
    }
}
