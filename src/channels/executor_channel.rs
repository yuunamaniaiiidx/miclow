use crate::messages::ExecutorEvent;
use anyhow::Result;
use tokio::sync::mpsc;

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

    pub fn send_message(
        &self,
        key: String,
        data: String,
    ) -> Result<(), mpsc::error::SendError<ExecutorEvent>> {
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
