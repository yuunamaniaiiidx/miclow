use crate::message_id::MessageId;
use crate::messages::ExecutorOutputEvent;
use crate::pod::PodId;
use crate::replicaset::ReplicaSetId;
use anyhow::Result;
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub struct ExecutorOutputEventSender {
    sender: mpsc::UnboundedSender<ExecutorOutputEvent>,
    replicaset_id: Option<ReplicaSetId>,
}

impl ExecutorOutputEventSender {
    pub fn new(sender: mpsc::UnboundedSender<ExecutorOutputEvent>) -> Self {
        Self {
            sender,
            replicaset_id: None,
        }
    }

    pub fn send(
        &self,
        event: ExecutorOutputEvent,
    ) -> Result<(), mpsc::error::SendError<ExecutorOutputEvent>> {
        self.sender.send(event)
    }

    pub fn send_message(
        &self,
        message_id: MessageId,
        task_id: PodId,
        key: String,
        data: String,
    ) -> Result<(), mpsc::error::SendError<ExecutorOutputEvent>> {
        self.send(ExecutorOutputEvent::new_message(
            message_id, task_id, key, data,
        ))
    }

    pub fn send_error(
        &self,
        message_id: MessageId,
        task_id: PodId,
        error: String,
    ) -> Result<(), mpsc::error::SendError<ExecutorOutputEvent>> {
        self.send(ExecutorOutputEvent::new_error(message_id, task_id, error))
    }

    pub fn send_exit(
        &self,
        message_id: MessageId,
        task_id: PodId,
        code: i32,
    ) -> Result<(), mpsc::error::SendError<ExecutorOutputEvent>> {
        self.send(ExecutorOutputEvent::new_exit(message_id, task_id, code))
    }

    pub fn replicaset_id(&self) -> Option<ReplicaSetId> {
        self.replicaset_id.clone()
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
