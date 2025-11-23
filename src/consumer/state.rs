use crate::topic::Topic;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsumerState {
    Requesting {
        topic: Topic,
    },
    Processing,
}

impl Default for ConsumerState {
    fn default() -> Self {
        Self::Processing
    }
}
