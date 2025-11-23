use std::sync::Arc;
use crate::backend::ProtocolBackend;
use crate::channels::UserLogSender;
use crate::topic::TopicSubscriptionRegistry;

#[derive(Clone)]
pub struct ConsumerStartContext {
    pub protocol_backend: ProtocolBackend,
    pub topic_manager: TopicSubscriptionRegistry,
    pub userlog_sender: UserLogSender,
    pub view_stdout: bool,
    pub view_stderr: bool,
}

pub struct SubscriptionSpec {
    pub task_name: Arc<str>,
    pub desired_instances: u32,
    pub start_context: ConsumerStartContext,
}
