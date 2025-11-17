use crate::messages::ExecutorInputEvent;
use anyhow::Result;
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub struct ExecutorInputEventSender {
    sender: mpsc::UnboundedSender<ExecutorInputEvent>,
}

impl ExecutorInputEventSender {
    pub fn new(sender: mpsc::UnboundedSender<ExecutorInputEvent>) -> Self {
        Self { sender }
    }

    pub fn send(
        &self,
        input: ExecutorInputEvent,
    ) -> Result<(), mpsc::error::SendError<ExecutorInputEvent>> {
        self.sender.send(input)
    }
}

pub struct ExecutorInputEventReceiver {
    receiver: mpsc::UnboundedReceiver<ExecutorInputEvent>,
}

impl ExecutorInputEventReceiver {
    pub fn new(receiver: mpsc::UnboundedReceiver<ExecutorInputEvent>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<ExecutorInputEvent> {
        self.receiver.recv().await
    }
}

pub struct ExecutorInputEventChannel {
    pub sender: ExecutorInputEventSender,
    pub receiver: ExecutorInputEventReceiver,
}

impl ExecutorInputEventChannel {
    pub fn new() -> Self {
        let (tx, receiver) = mpsc::unbounded_channel::<ExecutorInputEvent>();
        Self {
            sender: ExecutorInputEventSender::new(tx),
            receiver: ExecutorInputEventReceiver::new(receiver),
        }
    }
}
