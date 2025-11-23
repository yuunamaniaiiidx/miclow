use crate::subscription::SubscriptionId;
use crate::topic::Topic;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct SubscriptionTopicMessage {
    pub topic: Topic,
    pub data: Option<Arc<str>>,
    pub from_subscription_id: SubscriptionId,
}

