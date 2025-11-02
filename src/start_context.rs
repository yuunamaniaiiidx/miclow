use crate::executor_event_channel::ExecutorEventSender;
use crate::config::{SystemConfig, TaskConfig};
use crate::topic_manager::TopicManager;
use crate::system_control_manager::SystemControlManager;
use crate::user_log_sender::UserLogSender;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub enum StartContextVariant {
    Tasks,
    Functions {
        return_message_sender: ExecutorEventSender,
        initial_input: Option<String>,
    },
}

#[derive(Clone)]
pub struct StartContext {
    pub task_name: String,
    pub config: SystemConfig,
    pub topic_manager: TopicManager,
    pub system_control_manager: SystemControlManager,
    pub shutdown_token: CancellationToken,
    pub userlog_sender: UserLogSender,
    pub variant: StartContextVariant,
}

#[derive(Clone)]
pub struct StartFromConfigContext {
    pub task_config: TaskConfig,
    pub topic_manager: TopicManager,
    pub system_control_manager: SystemControlManager,
    pub shutdown_token: CancellationToken,
    pub userlog_sender: UserLogSender,
    pub variant: StartContextVariant,
}

impl StartContext {
    pub fn to_config_context(&self, task_config: TaskConfig) -> StartFromConfigContext {
        StartFromConfigContext {
            task_config,
            topic_manager: self.topic_manager.clone(),
            system_control_manager: self.system_control_manager.clone(),
            shutdown_token: self.shutdown_token.clone(),
            userlog_sender: self.userlog_sender.clone(),
            variant: self.variant.clone(),
        }
    }
}

impl StartFromConfigContext {
    pub fn is_function(&self) -> bool {
        matches!(self.variant, StartContextVariant::Functions { .. })
    }

    pub fn return_message_sender(&self) -> Option<ExecutorEventSender> {
        match &self.variant {
            StartContextVariant::Tasks => None,
            StartContextVariant::Functions { return_message_sender, .. } => Some(return_message_sender.clone()),
        }
    }

    pub fn initial_input(&self) -> Option<String> {
        match &self.variant {
            StartContextVariant::Tasks => None,
            StartContextVariant::Functions { initial_input, .. } => initial_input.clone(),
        }
    }
}

