use anyhow::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use crate::system_control_message::SystemControlMessage;

#[derive(Clone, Debug)]
pub struct SystemControlManager {
    commands: Arc<RwLock<Vec<SystemControlMessage>>>,
    shutdown_token: CancellationToken,
}

impl SystemControlManager {
    pub fn new(shutdown_token: CancellationToken) -> Self {
        Self {
            commands: Arc::new(RwLock::new(Vec::new())),
            shutdown_token,
        }
    }

    pub async fn add_command(&self, message: SystemControlMessage) -> Result<(), String> {
        let mut commands = self.commands.write().await;
        commands.push(message);
        Ok(())
    }

    pub async fn send_system_control_command(&self, command: crate::system_control_command::SystemControlCommand, task_id: crate::task_id::TaskId, response_channel: crate::system_response_channel::SystemResponseSender, task_event_sender: crate::executor_event_channel::ExecutorEventSender, return_message_sender: crate::executor_event_channel::ExecutorEventSender) -> Result<(), String> {
        let message = SystemControlMessage::new(command, task_id, response_channel, task_event_sender, return_message_sender);
        self.add_command(message).await
    }

    pub async fn recv_command(&self) -> Option<SystemControlMessage> {
        loop {
            if self.shutdown_token.is_cancelled() {
                return None;
            }

            {
                let mut commands = self.commands.write().await;
                if let Some(message) = commands.pop() {
                    return Some(message);
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }
}

