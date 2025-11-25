use crate::consumer::ConsumerId;
use crate::messages::message_id::MessageId;
use crate::subscription::SubscriptionId;
use crate::topic::Topic;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum ExecutorInputEvent {
    Topic {
        message_id: MessageId,
        consumer_id: ConsumerId,
        topic: Topic,
        data: Option<Arc<str>>,
        from_subscription_id: SubscriptionId,
    },
}
