use crate::messages::ExecutorOutputEvent;
use anyhow::Result;
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub struct ExecutorOutputEventSender {
    sender: mpsc::UnboundedSender<ExecutorOutputEvent>,
}

impl ExecutorOutputEventSender {
    pub fn new(sender: mpsc::UnboundedSender<ExecutorOutputEvent>) -> Self {
        Self { sender }
    }

    pub fn send(
        &self,
        event: ExecutorOutputEvent,
    ) -> Result<(), mpsc::error::SendError<ExecutorOutputEvent>> {
        self.sender.send(event)
    }

    pub fn send_message(
        &self,
        key: String,
        data: String,
    ) -> Result<(), mpsc::error::SendError<ExecutorOutputEvent>> {
        self.send(ExecutorOutputEvent::new_message(key, data))
    }

    pub fn send_error(
        &self,
        error: String,
    ) -> Result<(), mpsc::error::SendError<ExecutorOutputEvent>> {
        self.send(ExecutorOutputEvent::new_error(error))
    }

    pub fn send_exit(&self, code: i32) -> Result<(), mpsc::error::SendError<ExecutorOutputEvent>> {
        self.send(ExecutorOutputEvent::new_exit(code))
    }
}

pub struct ExecutorOutputEventReceiver {
    receiver: mpsc::UnboundedReceiver<ExecutorOutputEvent>,
}

impl ExecutorOutputEventReceiver {
    pub fn new(receiver: mpsc::UnboundedReceiver<ExecutorOutputEvent>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<ExecutorOutputEvent> {
        self.receiver.recv().await
    }
}

pub struct ExecutorOutputEventChannel {
    pub sender: ExecutorOutputEventSender,
    pub receiver: ExecutorOutputEventReceiver,
}

impl ExecutorOutputEventChannel {
    pub fn new() -> Self {
        let (tx, receiver) = mpsc::unbounded_channel::<ExecutorOutputEvent>();
        Self {
            sender: ExecutorOutputEventSender::new(tx),
            receiver: ExecutorOutputEventReceiver::new(receiver),
        }
    }
}
