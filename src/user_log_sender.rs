use tokio::sync::mpsc;
use anyhow::Result;
use crate::logging::UserLogEvent;

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
