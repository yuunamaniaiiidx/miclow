use crate::message_id::MessageId;
use crate::messages::ExecutorOutputEvent;
use crate::consumer::ConsumerId;
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

    pub fn send_error(
        &self,
        message_id: MessageId,
        pod_id: ConsumerId,
        error: String,
    ) -> Result<(), mpsc::error::SendError<ExecutorOutputEvent>> {
        self.send(ExecutorOutputEvent::new_error(message_id, pod_id, error))
    }

    pub fn send_exit(
        &self,
        message_id: MessageId,
        pod_id: ConsumerId,
        code: i32,
    ) -> Result<(), mpsc::error::SendError<ExecutorOutputEvent>> {
        self.send(ExecutorOutputEvent::new_exit(message_id, pod_id, code))
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
