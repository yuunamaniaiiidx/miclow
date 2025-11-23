use crate::message_id::MessageId;
use crate::consumer::ConsumerId;
use crate::topic::Topic;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum ConsumerEvent {
    ConsumerExit {
        consumer_id: ConsumerId,
    },
    ConsumerTopic {
        consumer_id: ConsumerId,
        message_id: MessageId,
        topic: Topic,
        data: Arc<str>,
    },
    ConsumerIdle {
        consumer_id: ConsumerId,
    },
}
