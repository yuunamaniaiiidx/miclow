use crate::executor_event_channel::ExecutorEventSender;
use crate::config::{SystemConfig, TaskConfig};
use crate::topic_manager::TopicManager;
use crate::system_control_manager::SystemControlManager;
use crate::user_log_sender::UserLogSender;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct StartContext {
    pub task_name: String,
    pub config: SystemConfig,
    pub topic_manager: TopicManager,
    pub system_control_manager: SystemControlManager,
    pub shutdown_token: CancellationToken,
    pub userlog_sender: UserLogSender,
    pub return_message_sender: Option<ExecutorEventSender>,
    pub initial_input: Option<String>,
    pub caller_task_name: Option<String>,
}

#[derive(Clone)]
pub struct ReadyStartContext {
    pub task_config: TaskConfig,
    pub topic_manager: TopicManager,
    pub system_control_manager: SystemControlManager,
    pub shutdown_token: CancellationToken,
    pub userlog_sender: UserLogSender,
    pub return_message_sender: Option<ExecutorEventSender>,
    pub initial_input: Option<String>,
    pub caller_task_name: Option<String>,
}

impl StartContext {
    pub fn to_ready_context(&self, task_config: TaskConfig) -> ReadyStartContext {
        ReadyStartContext {
            task_config,
            topic_manager: self.topic_manager.clone(),
            system_control_manager: self.system_control_manager.clone(),
            shutdown_token: self.shutdown_token.clone(),
            userlog_sender: self.userlog_sender.clone(),
            return_message_sender: self.return_message_sender.clone(),
            initial_input: self.initial_input.clone(),
            caller_task_name: self.caller_task_name.clone(),
        }
    }
}