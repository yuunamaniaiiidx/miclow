pub use crate::replicaset::pod_manager::{PodStateManager, ReplicaSetPodManager};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodState {
    Idle,
    Busy,
}

impl Default for PodState {
    fn default() -> Self {
        Self::Idle
    }
}

pub type PodInstanceState = PodState;