use crate::channels::UserLogSender;
use crate::config::{SystemConfig, TaskConfig};
use crate::system_control::SystemControlQueue;
use crate::topic_broker::TopicBroker;
use crate::task_id::TaskId;
use anyhow::Result;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct ParentInvocationContext {
    pub caller_task_id: TaskId,
    pub initial_input: Option<String>,
}

#[derive(Clone)]
pub struct StartContext {
    pub task_config: TaskConfig,
    pub topic_manager: TopicBroker,
    pub system_control_manager: SystemControlQueue,
    pub shutdown_token: CancellationToken,
    pub userlog_sender: UserLogSender,
    pub parent_invocation: Option<ParentInvocationContext>,
}

impl StartContext {
    /// TaskConfigを既に持っている場合の作成
    pub fn new(
        task_config: TaskConfig,
        topic_manager: TopicBroker,
        system_control_manager: SystemControlQueue,
        shutdown_token: CancellationToken,
        userlog_sender: UserLogSender,
        parent_invocation: Option<ParentInvocationContext>,
    ) -> Self {
        Self {
            task_config,
            topic_manager,
            system_control_manager,
            shutdown_token,
            userlog_sender,
            parent_invocation,
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
        parent_invocation: Option<ParentInvocationContext>,
    ) -> Result<Self> {
        let task_config = config
            .tasks
            .get(&task_name)
            .ok_or_else(|| anyhow::anyhow!("Task '{}' not found in configuration", task_name))?;

        Ok(Self {
            task_config: task_config.clone(),
            topic_manager,
            system_control_manager,
            shutdown_token,
            userlog_sender,
            parent_invocation,
        })
    }
}
