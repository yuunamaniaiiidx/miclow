#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodState {
    Idle,
    Busy,
}

impl Default for PodState {
    fn default() -> Self {
        Self::Idle
    }
}
