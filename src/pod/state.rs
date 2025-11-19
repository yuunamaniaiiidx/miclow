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
    /// Pod名 -> Pod IDのリスト
    name_to_ids: Arc<RwLock<HashMap<String, Vec<TaskId>>>>,
}

impl PodStateManager {
    pub fn new() -> Self {
        Self {
            pod_states: Arc::new(RwLock::new(HashMap::new())),
            name_to_ids: Arc::new(RwLock::new(HashMap::new())),
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

        states.remove(pod_id);

        // name_to_idsから削除
        for (_name, ids) in name_to_ids.iter_mut() {
            ids.retain(|id| id != pod_id);
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

    /// Pod名に属するIdleなインスタンスのリストを取得
    /// Round Robin などのロードバランシングアルゴリズムで使用するための汎用的なメソッド
    pub async fn get_idle_instances(&self, pod_name: &str) -> Vec<TaskId> {
        let name_to_ids = self.name_to_ids.read().await;
        let pod_ids = match name_to_ids.get(pod_name) {
            Some(ids) => ids.clone(),
            None => return Vec::new(),
        };
        drop(name_to_ids);

        let states = self.pod_states.read().await;
        pod_ids
            .into_iter()
            .filter(|id| {
                states
                    .get(id)
                    .map(|s| *s == PodInstanceState::Idle)
                    .unwrap_or(false)
            })
            .collect()
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
    async fn test_get_idle_instances() {
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

        // 最初はすべてIdle
        let idle_instances = manager.get_idle_instances("test_pod").await;
        assert_eq!(idle_instances.len(), 3);
        assert!(idle_instances.contains(&pod_id1));
        assert!(idle_instances.contains(&pod_id2));
        assert!(idle_instances.contains(&pod_id3));

        // 1つをBusyに
        manager.set_busy(&pod_id1).await;
        let idle_instances = manager.get_idle_instances("test_pod").await;
        assert_eq!(idle_instances.len(), 2);
        assert!(!idle_instances.contains(&pod_id1));
        assert!(idle_instances.contains(&pod_id2));
        assert!(idle_instances.contains(&pod_id3));

        // すべてBusyに
        manager.set_busy(&pod_id2).await;
        manager.set_busy(&pod_id3).await;
        let idle_instances = manager.get_idle_instances("test_pod").await;
        assert_eq!(idle_instances.len(), 0);

        // 1つをIdleに戻す
        manager.set_idle(&pod_id1).await;
        let idle_instances = manager.get_idle_instances("test_pod").await;
        assert_eq!(idle_instances.len(), 1);
        assert!(idle_instances.contains(&pod_id1));
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

