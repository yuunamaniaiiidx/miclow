use crate::messages::message_id::MessageId;
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
    /// システムコマンド文字列からConsumerEventを生成する
    /// 
    /// 例:
    /// - "system.pull:call" -> ConsumerRequesting { topic: "call" }
    /// - "system.peek:call" -> ConsumerPeekRequesting { topic: "call" }
    /// - "system.latest:call" -> ConsumerLatestRequesting { topic: "call" }
    /// - "system.result:call.result" -> ConsumerResultRequesting { topic: "call.result" }
    pub fn from_system_command(consumer_id: ConsumerId, command: &str, topic_data: &str) -> Option<Self> {
        let topic = Topic::from(topic_data.trim());
        match command {
            "system.pull" => Some(ConsumerEvent::ConsumerRequesting {
                consumer_id,
                topic,
            }),
            "system.peek" => Some(ConsumerEvent::ConsumerPeekRequesting {
                consumer_id,
                topic,
            }),
            "system.latest" => Some(ConsumerEvent::ConsumerLatestRequesting {
                consumer_id,
                topic,
            }),
            "system.result" => Some(ConsumerEvent::ConsumerResultRequesting {
                consumer_id,
                topic,
            }),
            _ => None,
        }
    }
}
