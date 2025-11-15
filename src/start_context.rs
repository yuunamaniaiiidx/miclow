use crate::channels::ExecutorEventSender;
use crate::config::{SystemConfig, TaskConfig};
use crate::topic_broker::TopicBroker;
use crate::system_control::SystemControlQueue;
use crate::channels::UserLogSender;
use tokio_util::sync::CancellationToken;
use anyhow::Result;

#[derive(Clone)]
pub struct StartContext {
    pub task_config: TaskConfig,
    pub topic_manager: TopicBroker,
    pub system_control_manager: SystemControlQueue,
    pub shutdown_token: CancellationToken,
    pub userlog_sender: UserLogSender,
    pub return_message_sender: Option<ExecutorEventSender>,
    pub initial_input: Option<String>,
    pub caller_task_name: Option<String>,
}

impl StartContext {
    /// TaskConfigを既に持っている場合の作成
    pub fn new(
        task_config: TaskConfig,
        topic_manager: TopicBroker,
        system_control_manager: SystemControlQueue,
        shutdown_token: CancellationToken,
        userlog_sender: UserLogSender,
        return_message_sender: Option<ExecutorEventSender>,
        initial_input: Option<String>,
        caller_task_name: Option<String>,
    ) -> Self {
        Self {
            task_config,
            topic_manager,
            system_control_manager,
            shutdown_token,
            userlog_sender,
            return_message_sender,
            initial_input,
            caller_task_name,
        }
    }

    /// タスク名から検索してStartContextを作成
    pub fn from_task_name(
        task_name: String,
        config: &SystemConfig,
        topic_manager: TopicBroker,
        system_control_manager: SystemControlQueue,
        shutdown_token: CancellationToken,
        userlog_sender: UserLogSender,
        return_message_sender: Option<ExecutorEventSender>,
        initial_input: Option<String>,
        caller_task_name: Option<String>,
    ) -> Result<Self> {
        let task_config = config.tasks.get(&task_name)
            .or_else(|| config.functions.get(&task_name))
            .ok_or_else(|| anyhow::anyhow!("Task '{}' not found in configuration", task_name))?;

        Ok(Self {
            task_config: task_config.clone(),
            topic_manager,
            system_control_manager,
            shutdown_token,
            userlog_sender,
            return_message_sender,
            initial_input,
            caller_task_name,
        })
    }
}