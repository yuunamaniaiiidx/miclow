use crate::messages::SubscriptionTopicMessage;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct SubscriptionTopicSender {
    sender: mpsc::UnboundedSender<SubscriptionTopicMessage>,
}

impl SubscriptionTopicSender {
    pub fn new(sender: mpsc::UnboundedSender<SubscriptionTopicMessage>) -> Self {
        Self { sender }
    }

    pub fn send(
        &self,
        message: SubscriptionTopicMessage,
    ) -> Result<(), mpsc::error::SendError<SubscriptionTopicMessage>> {
        self.sender.send(message)
    }
}

pub struct SubscriptionTopicReceiver {
    receiver: mpsc::UnboundedReceiver<SubscriptionTopicMessage>,
}

impl SubscriptionTopicReceiver {
    pub fn new(receiver: mpsc::UnboundedReceiver<SubscriptionTopicMessage>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<SubscriptionTopicMessage> {
        self.receiver.recv().await
    }
}

pub struct SubscriptionTopicChannel {
    pub sender: SubscriptionTopicSender,
    pub receiver: SubscriptionTopicReceiver,
}

impl SubscriptionTopicChannel {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        Self {
            sender: SubscriptionTopicSender::new(sender),
            receiver: SubscriptionTopicReceiver::new(receiver),
        }
    }
}
