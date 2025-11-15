#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemResponseStatus {
    Success,
    Error,
}

impl std::fmt::Display for SystemResponseStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SystemResponseStatus::Success => write!(f, "success"),
            SystemResponseStatus::Error => write!(f, "error"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum SystemResponseEvent {
    SystemResponse {
        topic: String,
        status: String,
        data: String,
    },
    SystemError {
        topic: String,
        status: String,
        error: String,
    },
}

impl SystemResponseEvent {
    pub fn new_system_response(topic: String, status: String, data: String) -> Self {
        Self::SystemResponse { topic, status, data }
    }

    pub fn new_system_error(topic: String, status: String, error: String) -> Self {
        Self::SystemError { topic, status, error }
    }

    pub fn topic(&self) -> &str {
        match self {
            Self::SystemResponse { topic, .. } => topic,
            Self::SystemError { topic, .. } => topic,
        }
    }

    pub fn data(&self) -> Option<&String> {
        match self {
            Self::SystemResponse { data, .. } => Some(data),
            _ => None,
        }
    }
}

