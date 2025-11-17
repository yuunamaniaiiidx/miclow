use anyhow::Result;
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub struct ShutdownSender {
    sender: mpsc::UnboundedSender<()>,
}

impl ShutdownSender {
    pub fn new(sender: mpsc::UnboundedSender<()>) -> Self {
        Self { sender }
    }

    pub fn shutdown(&self) -> Result<(), mpsc::error::SendError<()>> {
        self.sender.send(())
    }
}

pub struct ShutdownChannel {
    pub sender: ShutdownSender,
    pub receiver: mpsc::UnboundedReceiver<()>,
}

impl ShutdownChannel {
    pub fn new() -> Self {
        let (tx, receiver) = mpsc::unbounded_channel::<()>();
        Self {
            sender: ShutdownSender::new(tx),
            receiver,
        }
    }
}
