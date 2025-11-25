use crate::consumer::ConsumerId;
use crate::topic::Topic;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsumerState {
    Requesting {
        topic: Topic,
    },
    Processing {
        from_consumer_id: Option<ConsumerId>,
    },
}

impl Default for ConsumerState {
    fn default() -> Self {
        Self::Processing {
            from_consumer_id: None,
        }
    }
}
