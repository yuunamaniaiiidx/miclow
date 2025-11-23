#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsumerState {
    Idle,
    Busy,
}

impl Default for ConsumerState {
    fn default() -> Self {
        Self::Busy
    }
}
