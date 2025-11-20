use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::pod::state::PodState;
use crate::pod::PodId;

#[derive(Debug, Clone)]
struct PodInstanceEntry {
    pod_id: PodId,
    state: PodState,
}

#[derive(Debug, Clone)]
pub struct ReplicaSetPodManager {
    /// Pod名ごとのインスタンス一覧（Vecで保持）
    pods: Arc<RwLock<HashMap<String, Vec<PodInstanceEntry>>>>,
    /// PodId -> Pod名
    pod_name_by_id: Arc<RwLock<HashMap<PodId, String>>>,
}

impl ReplicaSetPodManager {
    pub fn new() -> Self {
        Self {
            pods: Arc::new(RwLock::new(HashMap::new())),
            pod_name_by_id: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Podインスタンスを登録 (初期状態はIdle)
    pub async fn add_pod(&self, pod_name: String, pod_id: PodId) {
        let mut pods = self.pods.write().await;
        pods.entry(pod_name.clone())
            .or_insert_with(Vec::new)
            .push(PodInstanceEntry {
                pod_id: pod_id.clone(),
                state: PodState::Idle,
            });

        let mut name_map = self.pod_name_by_id.write().await;
        name_map.insert(pod_id, pod_name);
    }

    /// Podインスタンスを解除
    pub async fn unregister_pod(&self, pod_id: &PodId) {
        let pod_name = {
            let name_map = self.pod_name_by_id.read().await;
            name_map.get(pod_id).cloned()
        };

        if let Some(name) = pod_name {
            let mut pods = self.pods.write().await;
            if let Some(entries) = pods.get_mut(&name) {
                entries.retain(|entry| &entry.pod_id != pod_id);
                if entries.is_empty() {
                    pods.remove(&name);
                }
            }

            let mut name_map = self.pod_name_by_id.write().await;
            name_map.remove(pod_id);
        }
    }

    /// Pod名に紐づくインスタンスのPodId一覧を取得
    pub async fn get_pod_instances(&self, pod_name: &str) -> Vec<PodId> {
        let pods = self.pods.read().await;
        pods.get(pod_name)
            .map(|entries| entries.iter().map(|entry| entry.pod_id.clone()).collect())
            .unwrap_or_default()
    }

    /// 指定したPodIdの状態を取得
    pub async fn get_state(&self, pod_id: &PodId) -> Option<PodState> {
        let pod_name = {
            let name_map = self.pod_name_by_id.read().await;
            name_map.get(pod_id).cloned()
        }?;

        let pods = self.pods.read().await;
        pods.get(&pod_name)
            .and_then(|entries| {
                entries
                    .iter()
                    .find(|entry| &entry.pod_id == pod_id)
                    .map(|entry| entry.state)
            })
    }

    /// 指定したPodIdの状態をBusyに設定
    pub async fn set_busy(&self, pod_id: &PodId) {
        self.update_state(pod_id, PodState::Busy).await;
    }

    /// 指定したPodIdの状態をIdleに設定
    pub async fn set_idle(&self, pod_id: &PodId) {
        self.update_state(pod_id, PodState::Idle).await;
    }

    async fn update_state(&self, pod_id: &PodId, next_state: PodState) {
        let pod_name = {
            let name_map = self.pod_name_by_id.read().await;
            name_map.get(pod_id).cloned()
        };

        if let Some(name) = pod_name {
            let mut pods = self.pods.write().await;
            if let Some(entries) = pods.get_mut(&name) {
                if let Some(entry) = entries.iter_mut().find(|entry| &entry.pod_id == pod_id) {
                    entry.state = next_state;
                }
            }
        }
    }

    /// 現状はVecを線形探索してIdleなインスタンスを探し、該当インデックスを返す
    pub async fn send(&self, pod_name: &str) -> Option<usize> {
        let mut pods = self.pods.write().await;
        let entries = pods.get_mut(pod_name)?;

        for (idx, entry) in entries.iter_mut().enumerate() {
            if entry.state == PodState::Idle {
                entry.state = PodState::Busy;
                return Some(idx);
            }
        }
        None
    }

    /// sendで得たインデックスからPodIdを取得
    pub async fn get_pod_id_by_index(&self, pod_name: &str, index: usize) -> Option<PodId> {
        let pods = self.pods.read().await;
        pods.get(pod_name)
            .and_then(|entries| entries.get(index).map(|entry| entry.pod_id.clone()))
    }
}