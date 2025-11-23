use crate::message_id::MessageId;
use crate::consumer::ConsumerId;
use crate::subscription::SubscriptionId;
use crate::topic::Topic;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum ExecutorInputEvent {
    Topic {
        message_id: MessageId,
        consumer_id: ConsumerId,
        topic: Topic,
        /// データがない場合（system.pullでデータが見つからない場合など）はNone
        data: Option<Arc<str>>,
        from_subscription_id: SubscriptionId,
    },
}
