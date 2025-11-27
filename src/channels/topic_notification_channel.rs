use crate::topic::Topic;
use tokio::sync::broadcast;

#[derive(Clone, Debug)]
pub struct TopicNotificationSender {
    sender: broadcast::Sender<Topic>,
}

impl TopicNotificationSender {
    pub fn new(sender: broadcast::Sender<Topic>) -> Self {
        Self { sender }
    }

    pub fn send(&self, topic: Topic) -> Result<usize, broadcast::error::SendError<Topic>> {
        self.sender.send(topic)
    }

    pub fn subscribe(&self) -> TopicNotificationReceiver {
        TopicNotificationReceiver {
            receiver: self.sender.subscribe(),
        }
    }
}

pub struct TopicNotificationReceiver {
    receiver: broadcast::Receiver<Topic>,
}

impl TopicNotificationReceiver {
    pub fn new(receiver: broadcast::Receiver<Topic>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<Topic> {
        loop {
            match self.receiver.recv().await {
                Ok(topic) => return Some(topic),
                Err(broadcast::error::RecvError::Closed) => return None,
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    // ラグした場合は最新のメッセージを取得するために再試行
                    continue;
                }
            }
        }
    }
}

pub struct TopicNotificationChannel {
    pub sender: TopicNotificationSender,
}

impl TopicNotificationChannel {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(1024);
        Self {
            sender: TopicNotificationSender::new(sender),
        }
    }
}
