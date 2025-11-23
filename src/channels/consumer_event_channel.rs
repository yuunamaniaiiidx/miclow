use tokio::sync::mpsc;

use crate::messages::ConsumerEvent;

#[derive(Clone, Debug)]
pub struct ConsumerEventSender {
    sender: mpsc::UnboundedSender<ConsumerEvent>,
}

impl ConsumerEventSender {
    pub fn new(sender: mpsc::UnboundedSender<ConsumerEvent>) -> Self {
        Self { sender }
    }

    pub fn send(&self, event: ConsumerEvent) -> Result<(), mpsc::error::SendError<ConsumerEvent>> {
        self.sender.send(event)
    }
}

pub struct ConsumerEventReceiver {
    receiver: mpsc::UnboundedReceiver<ConsumerEvent>,
}

impl ConsumerEventReceiver {
    pub fn new(receiver: mpsc::UnboundedReceiver<ConsumerEvent>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<ConsumerEvent> {
        self.receiver.recv().await
    }
}

pub struct ConsumerEventChannel {
    pub sender: ConsumerEventSender,
    pub receiver: ConsumerEventReceiver,
}

impl ConsumerEventChannel {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<ConsumerEvent>();
        Self {
            sender: ConsumerEventSender::new(tx),
            receiver: ConsumerEventReceiver::new(rx),
        }
    }
}
