use crate::topic::Topic;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsumerState {
    Requesting {
        topic: Option<Topic>,
    },
    Processing,
}

impl Default for ConsumerState {
    fn default() -> Self {
        Self::Processing
    }
}
