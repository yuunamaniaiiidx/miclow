use crate::task_id::TaskId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Podインスタンスの状態
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodInstanceState {
    /// アイドル状態（メッセージ処理可能）
    Idle,
    /// ビジー状態（メッセージ処理中）
    Busy,
}

impl Default for PodInstanceState {
    fn default() -> Self {
        Self::Idle
    }
}

/// Pod名ごとのインスタンス状態管理
#[derive(Clone)]
pub struct PodStateManager {
    /// Pod ID -> 状態のマッピング
    pod_states: Arc<RwLock<HashMap<TaskId, PodInstanceState>>>,
    /// Pod名 -> Pod IDのリスト（TopicLoadBalancer用）
    name_to_ids: Arc<RwLock<HashMap<String, Vec<TaskId>>>>,
    /// Pod名ごとのRound Robinインデックス（TopicLoadBalancer用）
    round_robin_indices: Arc<RwLock<HashMap<String, usize>>>,
}

impl PodStateManager {
    pub fn new() -> Self {
        Self {
            pod_states: Arc::new(RwLock::new(HashMap::new())),
            name_to_ids: Arc::new(RwLock::new(HashMap::new())),
            round_robin_indices: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Podを登録（初期状態はIdle）
    pub async fn register_pod(&self, pod_name: String, pod_id: TaskId) {
        let pod_id_clone = pod_id.clone();
        let mut states = self.pod_states.write().await;
        let mut name_to_ids = self.name_to_ids.write().await;
        states.insert(pod_id_clone.clone(), PodInstanceState::Idle);
        name_to_ids
            .entry(pod_name)
            .or_insert_with(Vec::new)
            .push(pod_id_clone.clone());
        log::debug!("Registered pod {} with initial state Idle", pod_id_clone);
    }

    /// Podを登録解除
    pub async fn unregister_pod(&self, pod_id: &TaskId) {
        let mut states = self.pod_states.write().await;
        let mut name_to_ids = self.name_to_ids.write().await;
        let mut indices = self.round_robin_indices.write().await;

        states.remove(pod_id);

        // name_to_idsから削除
        let mut name_to_remove = None;
        let mut index_to_adjust: Option<(String, usize, usize)> = None;
        for (name, ids) in name_to_ids.iter_mut() {
            if let Some(pos) = ids.iter().position(|id| id == pod_id) {
                let name_clone = name.clone();
                let current_idx = indices.get(name).copied();
                ids.remove(pos);
                if ids.is_empty() {
                    name_to_remove = Some(name_clone.clone());
                }
                // インデックス調整情報を保存
                if let Some(idx) = current_idx {
                    index_to_adjust = Some((name_clone, pos, idx));
                }
                break;
            }
        }
        // インデックスを調整
        if let Some((name, pos, current_idx)) = index_to_adjust {
            if current_idx > pos && current_idx > 0 {
                indices.insert(name.clone(), current_idx - 1);
            } else if current_idx >= name_to_ids.get(&name).map(|ids| ids.len()).unwrap_or(0)
                && !name_to_ids
                    .get(&name)
                    .map(|ids| ids.is_empty())
                    .unwrap_or(true)
            {
                indices.insert(name.clone(), 0);
            }
        }
        if let Some(name) = name_to_remove {
            indices.remove(&name);
        }
        name_to_ids.retain(|_name, ids| !ids.is_empty());
        log::debug!("Unregistered pod {}", pod_id);
    }

    /// Podの状態を取得
    pub async fn get_state(&self, pod_id: &TaskId) -> Option<PodInstanceState> {
        let states = self.pod_states.read().await;
        states.get(pod_id).copied()
    }

    /// Podの状態を設定
    pub async fn set_state(&self, pod_id: &TaskId, state: PodInstanceState) {
        let mut states = self.pod_states.write().await;
        if let Some(current_state) = states.get_mut(pod_id) {
            *current_state = state;
            log::debug!("Pod {} state changed to {:?}", pod_id, state);
        }
    }

    /// PodをIdleに戻す
    pub async fn set_idle(&self, pod_id: &TaskId) {
        self.set_state(pod_id, PodInstanceState::Idle).await;
    }

    /// PodをBusyに設定
    pub async fn set_busy(&self, pod_id: &TaskId) {
        self.set_state(pod_id, PodInstanceState::Busy).await;
    }

    /// Round Robin方式でIdleなPodインスタンスを選択
    /// すべてがBusyの場合はNoneを返す
    pub async fn select_idle_instance_round_robin(&self, pod_name: &str) -> Option<TaskId> {
        let name_to_ids = self.name_to_ids.read().await;
        let pod_ids = name_to_ids.get(pod_name)?.clone();
        drop(name_to_ids);

        if pod_ids.is_empty() {
            return None;
        }

        let states = self.pod_states.read().await;
        let mut indices = self.round_robin_indices.write().await;

        let start_idx = *indices.get(pod_name).unwrap_or(&0);
        let mut checked = 0;
        let mut current_idx = start_idx;

        // 最大1周までチェック
        while checked < pod_ids.len() {
            let pod_id = &pod_ids[current_idx];
            if let Some(state) = states.get(pod_id) {
                if *state == PodInstanceState::Idle {
                    // 次のインデックスを保存
                    let next_idx = (current_idx + 1) % pod_ids.len();
                    indices.insert(pod_name.to_string(), next_idx);
                    log::debug!(
                        "Selected idle instance {} for pod '{}' (index {})",
                        pod_id,
                        pod_name,
                        current_idx
                    );
                    return Some(pod_id.clone());
                }
            }
            current_idx = (current_idx + 1) % pod_ids.len();
            checked += 1;
        }

        // すべてBusy
        log::debug!(
            "All instances of pod '{}' are busy (checked {} instances)",
            pod_name,
            pod_ids.len()
        );
        None
    }

    /// Pod名に属するすべてのインスタンスIDを取得
    pub async fn get_pod_instances(&self, pod_name: &str) -> Vec<TaskId> {
        let name_to_ids = self.name_to_ids.read().await;
        name_to_ids
            .get(pod_name)
            .map(|ids| ids.clone())
            .unwrap_or_default()
    }

    /// Pod名に属するIdleなインスタンスの数を取得
    pub async fn count_idle_instances(&self, pod_name: &str) -> usize {
        let name_to_ids = self.name_to_ids.read().await;
        let pod_ids = match name_to_ids.get(pod_name) {
            Some(ids) => ids.clone(),
            None => return 0,
        };
        drop(name_to_ids);

        let states = self.pod_states.read().await;
        pod_ids
            .iter()
            .filter(|id| {
                states
                    .get(*id)
                    .map(|s| *s == PodInstanceState::Idle)
                    .unwrap_or(false)
            })
            .count()
    }

    /// 内部のname_to_idsへのアクセス（TopicLoadBalancer用）
    pub(crate) fn name_to_ids(&self) -> Arc<RwLock<HashMap<String, Vec<TaskId>>>> {
        self.name_to_ids.clone()
    }
}

impl Default for PodStateManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_and_unregister() {
        let manager = PodStateManager::new();
        let pod_id = TaskId::new();

        manager
            .register_pod("test_pod".to_string(), pod_id.clone())
            .await;
        assert_eq!(
            manager.get_state(&pod_id).await,
            Some(PodInstanceState::Idle)
        );

        manager.unregister_pod(&pod_id).await;
        assert_eq!(manager.get_state(&pod_id).await, None);
    }

    #[tokio::test]
    async fn test_state_transitions() {
        let manager = PodStateManager::new();
        let pod_id = TaskId::new();

        manager
            .register_pod("test_pod".to_string(), pod_id.clone())
            .await;

        manager.set_busy(&pod_id).await;
        assert_eq!(
            manager.get_state(&pod_id).await,
            Some(PodInstanceState::Busy)
        );

        manager.set_idle(&pod_id).await;
        assert_eq!(
            manager.get_state(&pod_id).await,
            Some(PodInstanceState::Idle)
        );
    }

    #[tokio::test]
    async fn test_round_robin_selection() {
        let manager = PodStateManager::new();
        let pod_id1 = TaskId::new();
        let pod_id2 = TaskId::new();
        let pod_id3 = TaskId::new();

        manager
            .register_pod("test_pod".to_string(), pod_id1.clone())
            .await;
        manager
            .register_pod("test_pod".to_string(), pod_id2.clone())
            .await;
        manager
            .register_pod("test_pod".to_string(), pod_id3.clone())
            .await;

        // 最初はすべてIdleなので順番に選択される
        let selected1 = manager
            .select_idle_instance_round_robin("test_pod")
            .await
            .unwrap();
        assert_eq!(selected1, pod_id1);

        // 選択されたPodをBusyに
        manager.set_busy(&pod_id1).await;

        // 次はpod_id2が選択される
        let selected2 = manager
            .select_idle_instance_round_robin("test_pod")
            .await
            .unwrap();
        assert_eq!(selected2, pod_id2);

        // すべてBusyに
        manager.set_busy(&pod_id2).await;
        manager.set_busy(&pod_id3).await;

        // すべてBusyなのでNone
        assert!(manager
            .select_idle_instance_round_robin("test_pod")
            .await
            .is_none());

        // 1つをIdleに戻す
        manager.set_idle(&pod_id1).await;
        let selected3 = manager
            .select_idle_instance_round_robin("test_pod")
            .await
            .unwrap();
        assert_eq!(selected3, pod_id1);
    }

    #[tokio::test]
    async fn test_count_idle_instances() {
        let manager = PodStateManager::new();
        let pod_id1 = TaskId::new();
        let pod_id2 = TaskId::new();

        manager
            .register_pod("test_pod".to_string(), pod_id1.clone())
            .await;
        manager
            .register_pod("test_pod".to_string(), pod_id2.clone())
            .await;

        assert_eq!(manager.count_idle_instances("test_pod").await, 2);

        manager.set_busy(&pod_id1).await;
        assert_eq!(manager.count_idle_instances("test_pod").await, 1);

        manager.set_busy(&pod_id2).await;
        assert_eq!(manager.count_idle_instances("test_pod").await, 0);
    }
}

