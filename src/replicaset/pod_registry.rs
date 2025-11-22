use std::collections::HashMap;
use crate::channels::ReplicaSetTopicSender;
use crate::pod::{PodId, PodSpawnHandler, PodState};

pub struct ManagedPod {
    pub handler: PodSpawnHandler,
    pub topic_sender: ReplicaSetTopicSender,
    pub state: PodState,
}

#[derive(Default)]
struct PodRouter {
    order: Vec<PodId>,
    next_index: usize,
}

impl PodRouter {
    fn add(&mut self, pod_id: PodId) {
        self.order.push(pod_id);
    }

    fn remove(&mut self, pod_id: &PodId) {
        if let Some(pos) = self.order.iter().position(|id| id == pod_id) {
            self.order.remove(pos);
            if self.order.is_empty() {
                self.next_index = 0;
                return;
            }
            if pos < self.next_index {
                self.next_index = self.next_index.saturating_sub(1);
            } else if self.next_index >= self.order.len() {
                self.next_index = 0;
            }
        }
    }

    fn next_pod(&mut self) -> Option<PodId> {
        if self.order.is_empty() {
            return None;
        }
        let pod_id = self.order[self.next_index].clone();
        self.next_index = (self.next_index + 1) % self.order.len();
        Some(pod_id)
    }

    fn is_empty(&self) -> bool {
        self.order.is_empty()
    }
}

pub struct PodRegistry {
    pods: HashMap<PodId, ManagedPod>,
    router: PodRouter,
    idle_count: usize,
}

impl PodRegistry {
    pub fn new() -> Self {
        Self {
            pods: HashMap::new(),
            router: PodRouter::default(),
            idle_count: 0,
        }
    }

    pub fn add_pod(&mut self, pod: ManagedPod) -> PodId {
        let pod_id = pod.handler.pod_id.clone();
        self.pods.insert(pod_id.clone(), pod);
        self.router.add(pod_id.clone());
        self.idle_count += 1;
        pod_id
    }

    pub fn remove_pod(&mut self, pod_id: &PodId) -> Option<ManagedPod> {
        if let Some(pod) = self.pods.remove(pod_id) {
            if matches!(pod.state, PodState::Idle) {
                self.idle_count = self.idle_count.saturating_sub(1);
            }
            self.router.remove(pod_id);
            Some(pod)
        } else {
            None
        }
    }

    pub fn set_pod_busy(&mut self, pod_id: &PodId) {
        if let Some(pod) = self.pods.get_mut(pod_id) {
            if matches!(pod.state, PodState::Idle) {
                pod.state = PodState::Busy;
                self.idle_count = self.idle_count.saturating_sub(1);
            }
        }
    }

    pub fn set_pod_idle(&mut self, pod_id: &PodId) {
        if let Some(pod) = self.pods.get_mut(pod_id) {
            if !matches!(pod.state, PodState::Idle) {
                pod.state = PodState::Idle;
                self.idle_count += 1;
            }
        }
    }

    pub fn idle_count(&self) -> usize {
        self.idle_count
    }

    pub fn get_pod_mut(&mut self, pod_id: &PodId) -> Option<&mut ManagedPod> {
        self.pods.get_mut(pod_id)
    }

    pub fn next_pod_id(&mut self) -> Option<PodId> {
        self.router.next_pod()
    }

    pub fn is_empty(&self) -> bool {
        self.pods.is_empty() || self.router.is_empty()
    }

    pub fn len(&self) -> usize {
        self.pods.len()
    }

    pub fn drain(&mut self) -> HashMap<PodId, ManagedPod> {
        self.router = PodRouter::default();
        self.idle_count = 0;
        std::mem::take(&mut self.pods)
    }
}

