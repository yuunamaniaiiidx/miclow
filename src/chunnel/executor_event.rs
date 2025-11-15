use tokio::sync::mpsc;
use anyhow::Result;

#[derive(Debug, Clone)]
pub enum ExecutorEvent {
    Message {
        topic: String,
        data: String,
    },
    TaskStdout {
        data: String,
    },
    TaskStderr {
        data: String,
    },
    SystemControl {
        key: String,
        data: String,
    },
    ReturnMessage {
        data: String,
    },
    Error {
        error: String,
    },
    Exit {
        exit_code: i32,
    },
}

impl ExecutorEvent {
    pub fn new_message(topic: String, data: String) -> Self {
        Self::Message { 
            topic, 
            data 
        }
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
        Self::SystemControl { 
            key, 
            data 
        }
    }

    pub fn new_return_message(data: String) -> Self {
        Self::ReturnMessage { data }
    }

    pub fn data(&self) -> Option<&String> {
        match self {
            Self::Message { data, .. } => Some(data),
            Self::TaskStdout { data } => Some(data),
            Self::TaskStderr { data } => Some(data),
            Self::ReturnMessage { data } => Some(data),
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

#[derive(Clone, Debug)]
pub struct ExecutorEventSender {
    sender: mpsc::UnboundedSender<ExecutorEvent>,
}

impl ExecutorEventSender {
    pub fn new(sender: mpsc::UnboundedSender<ExecutorEvent>) -> Self {
        Self { sender }
    }

    pub fn send(&self, event: ExecutorEvent) -> Result<(), mpsc::error::SendError<ExecutorEvent>> {
        self.sender.send(event)
    }

    pub fn send_message(&self, key: String, data: String) -> Result<(), mpsc::error::SendError<ExecutorEvent>> {
        self.send(ExecutorEvent::new_message(key, data))
    }

    pub fn send_error(&self, error: String) -> Result<(), mpsc::error::SendError<ExecutorEvent>> {
        self.send(ExecutorEvent::new_error(error))
    }

    pub fn send_exit(&self, code: i32) -> Result<(), mpsc::error::SendError<ExecutorEvent>> {
        self.send(ExecutorEvent::new_exit(code))
    }
}

pub struct ExecutorEventReceiver {
    receiver: mpsc::UnboundedReceiver<ExecutorEvent>,
}

impl ExecutorEventReceiver {
    pub fn new(receiver: mpsc::UnboundedReceiver<ExecutorEvent>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<ExecutorEvent> {
        self.receiver.recv().await
    }
}

pub struct ExecutorEventChannel {
    pub sender: ExecutorEventSender,
    pub receiver: ExecutorEventReceiver,
}

impl ExecutorEventChannel {
    pub fn new() -> Self {
        let (tx, receiver) = mpsc::unbounded_channel::<ExecutorEvent>();
        Self {
            sender: ExecutorEventSender::new(tx),
            receiver: ExecutorEventReceiver::new(receiver),
        }
    }
}

