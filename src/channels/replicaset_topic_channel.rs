use crate::messages::TopicResponseStatus;
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub struct ReplicaSetTopicMessage {
    pub topic: String,
    pub data: String,
    pub kind: ReplicaSetTopicMessageKind,
}

#[derive(Clone, Debug)]
pub enum ReplicaSetTopicMessageKind {
    Topic,
    TopicResponse {
        status: TopicResponseStatus,
        original_topic: String,
    },
}

#[derive(Clone)]
pub struct ReplicaSetTopicSender {
    sender: mpsc::UnboundedSender<ReplicaSetTopicMessage>,
}

impl ReplicaSetTopicSender {
    pub fn new(sender: mpsc::UnboundedSender<ReplicaSetTopicMessage>) -> Self {
        Self { sender }
    }

    pub fn send(
        &self,
        message: ReplicaSetTopicMessage,
    ) -> Result<(), mpsc::error::SendError<ReplicaSetTopicMessage>> {
        self.sender.send(message)
    }
}

pub struct ReplicaSetTopicReceiver {
    receiver: mpsc::UnboundedReceiver<ReplicaSetTopicMessage>,
}

impl ReplicaSetTopicReceiver {
    pub fn new(receiver: mpsc::UnboundedReceiver<ReplicaSetTopicMessage>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<ReplicaSetTopicMessage> {
        self.receiver.recv().await
    }
}

pub struct ReplicaSetTopicChannel {
    pub sender: ReplicaSetTopicSender,
    pub receiver: ReplicaSetTopicReceiver,
}

impl ReplicaSetTopicChannel {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        Self {
            sender: ReplicaSetTopicSender::new(sender),
            receiver: ReplicaSetTopicReceiver::new(receiver),
        }
    }
}
