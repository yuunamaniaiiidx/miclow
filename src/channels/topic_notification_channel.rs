use crate::topic::Topic;
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub struct TopicNotificationSender {
    sender: mpsc::UnboundedSender<Topic>,
}

impl TopicNotificationSender {
    pub fn new(sender: mpsc::UnboundedSender<Topic>) -> Self {
        Self { sender }
    }

    pub fn send(&self, topic: Topic) -> Result<(), mpsc::error::SendError<Topic>> {
        self.sender.send(topic)
    }
}

pub struct TopicNotificationReceiver {
    receiver: mpsc::UnboundedReceiver<Topic>,
}

impl TopicNotificationReceiver {
    pub fn new(receiver: mpsc::UnboundedReceiver<Topic>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<Topic> {
        self.receiver.recv().await
    }
}

pub struct TopicNotificationChannel {
    pub sender: TopicNotificationSender,
    pub receiver: TopicNotificationReceiver,
}

impl TopicNotificationChannel {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        Self {
            sender: TopicNotificationSender::new(sender),
            receiver: TopicNotificationReceiver::new(receiver),
        }
    }
}

