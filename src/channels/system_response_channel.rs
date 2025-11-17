use crate::messages::SystemResponseEvent;
use anyhow::Result;
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub struct SystemResponseSender {
    sender: mpsc::UnboundedSender<SystemResponseEvent>,
}

impl SystemResponseSender {
    pub fn new(sender: mpsc::UnboundedSender<SystemResponseEvent>) -> Self {
        Self { sender }
    }

    pub fn send(
        &self,
        event: SystemResponseEvent,
    ) -> Result<(), mpsc::error::SendError<SystemResponseEvent>> {
        self.sender.send(event)
    }
}

pub struct SystemResponseReceiver {
    receiver: mpsc::UnboundedReceiver<SystemResponseEvent>,
}

impl SystemResponseReceiver {
    pub fn new(receiver: mpsc::UnboundedReceiver<SystemResponseEvent>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<SystemResponseEvent> {
        self.receiver.recv().await
    }
}

pub struct SystemResponseChannel {
    pub sender: SystemResponseSender,
    pub receiver: SystemResponseReceiver,
}

impl SystemResponseChannel {
    pub fn new() -> Self {
        let (tx, receiver) = mpsc::unbounded_channel::<SystemResponseEvent>();
        Self {
            sender: SystemResponseSender::new(tx),
            receiver: SystemResponseReceiver::new(receiver),
        }
    }
}
