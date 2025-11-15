use tokio::sync::mpsc;
use anyhow::Result;
use crate::message_id::MessageId;

#[derive(Clone, Debug)]
pub struct TopicMessage {
    pub message_id: MessageId,
    pub topic: String,
    pub data: String,
}

#[derive(Clone, Debug)]
pub struct SystemResponseMessage {
    pub message_id: MessageId,
    pub topic: String,
    pub status: String,
    pub data: String,
}

#[derive(Clone, Debug)]
pub struct ReturnMessage {
    pub message_id: MessageId,
    pub data: String,
}

#[derive(Clone, Debug)]
pub struct FunctionMessage {
    pub message_id: MessageId,
    pub task_name: String,
    pub data: String,
}

#[derive(Clone, Debug)]
pub enum InputDataMessage {
    Topic(TopicMessage),
    SystemResponse(SystemResponseMessage),
    Return(ReturnMessage),
    Function(FunctionMessage),
}

#[derive(Clone, Debug)]
pub struct InputSender {
    sender: mpsc::UnboundedSender<InputDataMessage>,
}

impl InputSender {
    pub fn new(sender: mpsc::UnboundedSender<InputDataMessage>) -> Self {
        Self { sender }
    }

    pub fn send(&self, input: InputDataMessage) -> Result<(), mpsc::error::SendError<InputDataMessage>> {
        self.sender.send(input)
    }
}

pub struct InputReceiver {
    receiver: mpsc::UnboundedReceiver<InputDataMessage>,
}

impl InputReceiver {
    pub fn new(receiver: mpsc::UnboundedReceiver<InputDataMessage>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<InputDataMessage> {
        self.receiver.recv().await
    }
}

pub struct InputChannel {
    pub sender: InputSender,
    pub receiver: InputReceiver,
}

impl InputChannel {
    pub fn new() -> Self {
        let (tx, receiver) = mpsc::unbounded_channel::<InputDataMessage>();
        Self {
            sender: InputSender::new(tx),
            receiver: InputReceiver::new(receiver),
        }
    }
}
