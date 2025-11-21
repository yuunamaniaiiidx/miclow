use crate::backend::ProtocolBackend;
use crate::channels::UserLogSender;
use crate::topic::TopicSubscriptionRegistry;

#[derive(Clone)]
pub struct PodStartContext {
    pub protocol_backend: ProtocolBackend,
    pub topic_manager: TopicSubscriptionRegistry,
    pub userlog_sender: UserLogSender,
    pub subscribe_topics: Vec<String>,
    pub view_stdout: bool,
    pub view_stderr: bool,
}

pub struct ReplicaSetSpec {
    pub task_name: String,
    pub desired_instances: u32,
    pub start_context: PodStartContext,
}
