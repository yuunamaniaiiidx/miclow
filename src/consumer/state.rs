use crate::topic::Topic;
use crate::consumer::ConsumerId;

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
        Self::Processing { from_consumer_id: None }
    }
}
