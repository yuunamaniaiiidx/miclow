use anyhow::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use crate::task_id::TaskId;
use crate::system_control_command::SystemControlCommand;
use crate::channels::{SystemResponseSender, ExecutorEventSender};


pub struct SystemControlMessage {
    pub command: SystemControlCommand,
    pub task_id: TaskId,
    pub response_channel: SystemResponseSender,
    pub task_event_sender: ExecutorEventSender,
    pub return_message_sender: ExecutorEventSender,
}

impl std::fmt::Debug for SystemControlMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemControlMessage")
            .field("command", &"SystemControl")
            .field("task_id", &self.task_id)
            .field("response_channel", &"SystemResponseSender")
            .field("task_event_sender", &"ExecutorEventSender")
            .finish()
    }
}

impl SystemControlMessage {
    pub fn new(command: SystemControlCommand, task_id: TaskId, response_channel: SystemResponseSender, task_event_sender: ExecutorEventSender, return_message_sender: ExecutorEventSender) -> Self {
        Self {
            command,
            task_id,
            response_channel,
            task_event_sender,
            return_message_sender,
        }
    }
}



#[derive(Clone, Debug)]
pub struct SystemControlQueue {
    commands: Arc<RwLock<Vec<SystemControlMessage>>>,
    shutdown_token: CancellationToken,
}

impl SystemControlQueue {
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

    pub async fn send_system_control_command(&self, command: crate::system_control_command::SystemControlCommand, task_id: crate::task_id::TaskId, response_channel: crate::channels::SystemResponseSender, task_event_sender: crate::channels::ExecutorEventSender, return_message_sender: crate::channels::ExecutorEventSender) -> Result<(), String> {
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

