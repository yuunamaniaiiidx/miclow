use crate::channels::UserLogSender;
use crate::config::TaskConfig;
use crate::system_control::SystemControlQueue;
use crate::topic_broker::TopicBroker;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct StartContext {
    pub task_config: TaskConfig,
    pub topic_manager: TopicBroker,
    pub system_control_manager: SystemControlQueue,
    pub shutdown_token: CancellationToken,
    pub userlog_sender: UserLogSender,
}

impl StartContext {
    /// TaskConfigを既に持っている場合の作成
    pub fn new(
        task_config: TaskConfig,
        topic_manager: TopicBroker,
        system_control_manager: SystemControlQueue,
        shutdown_token: CancellationToken,
        userlog_sender: UserLogSender,
    ) -> Self {
        Self {
            task_config,
            topic_manager,
            system_control_manager,
            shutdown_token,
            userlog_sender,
        }
    }
}
