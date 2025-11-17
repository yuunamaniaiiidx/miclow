use crate::logging::UserLogEvent;
use anyhow::Result;
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub struct UserLogSender {
    sender: mpsc::UnboundedSender<UserLogEvent>,
}

impl UserLogSender {
    pub fn new(sender: mpsc::UnboundedSender<UserLogEvent>) -> Self {
        Self { sender }
    }

    pub fn send(&self, event: UserLogEvent) -> Result<(), mpsc::error::SendError<UserLogEvent>> {
        self.sender.send(event)
    }
}
