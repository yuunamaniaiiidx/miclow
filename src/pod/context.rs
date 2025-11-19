use crate::channels::UserLogSender;
use crate::config::TaskConfig;
use crate::topic_subscription_registry::TopicSubscriptionRegistry;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct PodStartContext {
    pub task_config: TaskConfig,
    pub topic_manager: TopicSubscriptionRegistry,
    pub shutdown_token: CancellationToken,
    pub userlog_sender: UserLogSender,
}

impl PodStartContext {
    /// TaskConfigを既に持っている場合の作成
    pub fn new(
        task_config: TaskConfig,
        topic_manager: TopicSubscriptionRegistry,
        shutdown_token: CancellationToken,
        userlog_sender: UserLogSender,
    ) -> Self {
        Self {
            task_config,
            topic_manager,
            shutdown_token,
            userlog_sender,
        }
    }
}

