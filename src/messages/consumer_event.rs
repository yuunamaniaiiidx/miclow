use crate::consumer::ConsumerId;
use crate::messages::{message_id::MessageId, system_command::SystemCommand};
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
    ConsumerRequesting {
        consumer_id: ConsumerId,
        topic: Topic,
    },
    ConsumerResultRequesting {
        consumer_id: ConsumerId,
        topic: Topic,
    },
    ConsumerPeekRequesting {
        consumer_id: ConsumerId,
        topic: Topic,
    },
    ConsumerLatestRequesting {
        consumer_id: ConsumerId,
        topic: Topic,
    },
}

impl ConsumerEvent {
    /// SystemCommand から ConsumerEvent へ変換
    pub fn from_system_command(consumer_id: ConsumerId, command: &SystemCommand) -> Option<Self> {
        match command {
            SystemCommand::Pop(topic) => Some(ConsumerEvent::ConsumerRequesting {
                consumer_id,
                topic: topic.clone(),
            }),
            SystemCommand::Peek(topic) => Some(ConsumerEvent::ConsumerPeekRequesting {
                consumer_id,
                topic: topic.clone(),
            }),
            SystemCommand::Latest(topic) => Some(ConsumerEvent::ConsumerLatestRequesting {
                consumer_id,
                topic: topic.clone(),
            }),
            SystemCommand::Result(topic) => Some(ConsumerEvent::ConsumerResultRequesting {
                consumer_id,
                topic: topic.clone(),
            }),
            SystemCommand::PopAwait(_) => None,
        }
    }
}
