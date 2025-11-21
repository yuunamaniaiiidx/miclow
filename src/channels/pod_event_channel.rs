use tokio::sync::mpsc;

use crate::messages::PodEvent;

#[derive(Clone, Debug)]
pub struct PodEventSender {
    sender: mpsc::UnboundedSender<PodEvent>,
}

impl PodEventSender {
    pub fn new(sender: mpsc::UnboundedSender<PodEvent>) -> Self {
        Self { sender }
    }

    pub fn send(&self, event: PodEvent) -> Result<(), mpsc::error::SendError<PodEvent>> {
        self.sender.send(event)
    }
}

pub struct PodEventReceiver {
    receiver: mpsc::UnboundedReceiver<PodEvent>,
}

impl PodEventReceiver {
    pub fn new(receiver: mpsc::UnboundedReceiver<PodEvent>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<PodEvent> {
        self.receiver.recv().await
    }
}

pub struct PodEventChannel {
    pub sender: PodEventSender,
    pub receiver: PodEventReceiver,
}

impl PodEventChannel {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<PodEvent>();
        Self {
            sender: PodEventSender::new(tx),
            receiver: PodEventReceiver::new(rx),
        }
    }
}
