use crate::task_id::TaskId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// タスクインスタンスの状態
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskInstanceState {
    /// アイドル状態（メッセージ処理可能）
    Idle,
    /// ビジー状態（メッセージ処理中）
    Busy,
}

impl Default for TaskInstanceState {
    fn default() -> Self {
        Self::Idle
    }
}

/// タスク名ごとのインスタンス状態管理
#[derive(Clone)]
pub struct TaskStateManager {
    /// タスクID -> 状態のマッピング
    task_states: Arc<RwLock<HashMap<TaskId, TaskInstanceState>>>,
    /// タスク名 -> タスクIDのリスト（Round Robin用）
    name_to_ids: Arc<RwLock<HashMap<String, Vec<TaskId>>>>,
    /// タスク名ごとのRound Robinインデックス
    round_robin_indices: Arc<RwLock<HashMap<String, usize>>>,
}

impl TaskStateManager {
    pub fn new() -> Self {
        Self {
            task_states: Arc::new(RwLock::new(HashMap::new())),
            name_to_ids: Arc::new(RwLock::new(HashMap::new())),
            round_robin_indices: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// タスクを登録（初期状態はIdle）
    pub async fn register_task(&self, task_name: String, task_id: TaskId) {
        let task_id_clone = task_id.clone();
        let mut states = self.task_states.write().await;
        let mut name_to_ids = self.name_to_ids.write().await;
        states.insert(task_id_clone.clone(), TaskInstanceState::Idle);
        name_to_ids
            .entry(task_name)
            .or_insert_with(Vec::new)
            .push(task_id_clone.clone());
        log::debug!("Registered task {} with initial state Idle", task_id_clone);
    }

    /// タスクを登録解除
    pub async fn unregister_task(&self, task_id: &TaskId) {
        let mut states = self.task_states.write().await;
        let mut name_to_ids = self.name_to_ids.write().await;
        let mut indices = self.round_robin_indices.write().await;

        states.remove(task_id);

        // name_to_idsから削除
        let mut name_to_remove = None;
        let mut index_to_adjust: Option<(String, usize, usize)> = None;
        for (name, ids) in name_to_ids.iter_mut() {
            if let Some(pos) = ids.iter().position(|id| id == task_id) {
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
        log::debug!("Unregistered task {}", task_id);
    }

    /// タスクの状態を取得
    pub async fn get_state(&self, task_id: &TaskId) -> Option<TaskInstanceState> {
        let states = self.task_states.read().await;
        states.get(task_id).copied()
    }

    /// タスクの状態を設定
    pub async fn set_state(&self, task_id: &TaskId, state: TaskInstanceState) {
        let mut states = self.task_states.write().await;
        if let Some(current_state) = states.get_mut(task_id) {
            *current_state = state;
            log::debug!("Task {} state changed to {:?}", task_id, state);
        }
    }

    /// タスクをIdleに戻す
    pub async fn set_idle(&self, task_id: &TaskId) {
        self.set_state(task_id, TaskInstanceState::Idle).await;
    }

    /// タスクをBusyに設定
    pub async fn set_busy(&self, task_id: &TaskId) {
        self.set_state(task_id, TaskInstanceState::Busy).await;
    }

    /// Round Robin方式でIdleなタスクインスタンスを選択
    /// すべてがBusyの場合はNoneを返す
    pub async fn select_idle_instance_round_robin(&self, task_name: &str) -> Option<TaskId> {
        let name_to_ids = self.name_to_ids.read().await;
        let task_ids = name_to_ids.get(task_name)?.clone();
        drop(name_to_ids);

        if task_ids.is_empty() {
            return None;
        }

        let states = self.task_states.read().await;
        let mut indices = self.round_robin_indices.write().await;

        let start_idx = *indices.get(task_name).unwrap_or(&0);
        let mut checked = 0;
        let mut current_idx = start_idx;

        // 最大1周までチェック
        while checked < task_ids.len() {
            let task_id = &task_ids[current_idx];
            if let Some(state) = states.get(task_id) {
                if *state == TaskInstanceState::Idle {
                    // 次のインデックスを保存
                    let next_idx = (current_idx + 1) % task_ids.len();
                    indices.insert(task_name.to_string(), next_idx);
                    log::debug!(
                        "Selected idle instance {} for task '{}' (index {})",
                        task_id,
                        task_name,
                        current_idx
                    );
                    return Some(task_id.clone());
                }
            }
            current_idx = (current_idx + 1) % task_ids.len();
            checked += 1;
        }

        // すべてBusy
        log::debug!(
            "All instances of task '{}' are busy (checked {} instances)",
            task_name,
            task_ids.len()
        );
        None
    }

    /// タスク名に属するすべてのインスタンスIDを取得
    pub async fn get_task_instances(&self, task_name: &str) -> Vec<TaskId> {
        let name_to_ids = self.name_to_ids.read().await;
        name_to_ids
            .get(task_name)
            .map(|ids| ids.clone())
            .unwrap_or_default()
    }

    /// タスク名に属するIdleなインスタンスの数を取得
    pub async fn count_idle_instances(&self, task_name: &str) -> usize {
        let name_to_ids = self.name_to_ids.read().await;
        let task_ids = match name_to_ids.get(task_name) {
            Some(ids) => ids.clone(),
            None => return 0,
        };
        drop(name_to_ids);

        let states = self.task_states.read().await;
        task_ids
            .iter()
            .filter(|id| {
                states
                    .get(*id)
                    .map(|s| *s == TaskInstanceState::Idle)
                    .unwrap_or(false)
            })
            .count()
    }

    /// 内部のname_to_idsへのアクセス（RoundRobinDispatcher用）
    pub(crate) fn name_to_ids(&self) -> Arc<RwLock<HashMap<String, Vec<TaskId>>>> {
        self.name_to_ids.clone()
    }
}

impl Default for TaskStateManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_and_unregister() {
        let manager = TaskStateManager::new();
        let task_id = TaskId::new();

        manager
            .register_task("test_task".to_string(), task_id.clone())
            .await;
        assert_eq!(
            manager.get_state(&task_id).await,
            Some(TaskInstanceState::Idle)
        );

        manager.unregister_task(&task_id).await;
        assert_eq!(manager.get_state(&task_id).await, None);
    }

    #[tokio::test]
    async fn test_state_transitions() {
        let manager = TaskStateManager::new();
        let task_id = TaskId::new();

        manager
            .register_task("test_task".to_string(), task_id.clone())
            .await;

        manager.set_busy(&task_id).await;
        assert_eq!(
            manager.get_state(&task_id).await,
            Some(TaskInstanceState::Busy)
        );

        manager.set_idle(&task_id).await;
        assert_eq!(
            manager.get_state(&task_id).await,
            Some(TaskInstanceState::Idle)
        );
    }

    #[tokio::test]
    async fn test_round_robin_selection() {
        let manager = TaskStateManager::new();
        let task_id1 = TaskId::new();
        let task_id2 = TaskId::new();
        let task_id3 = TaskId::new();

        manager
            .register_task("test_task".to_string(), task_id1.clone())
            .await;
        manager
            .register_task("test_task".to_string(), task_id2.clone())
            .await;
        manager
            .register_task("test_task".to_string(), task_id3.clone())
            .await;

        // 最初はすべてIdleなので順番に選択される
        let selected1 = manager
            .select_idle_instance_round_robin("test_task")
            .await
            .unwrap();
        assert_eq!(selected1, task_id1);

        // 選択されたタスクをBusyに
        manager.set_busy(&task_id1).await;

        // 次はtask_id2が選択される
        let selected2 = manager
            .select_idle_instance_round_robin("test_task")
            .await
            .unwrap();
        assert_eq!(selected2, task_id2);

        // すべてBusyに
        manager.set_busy(&task_id2).await;
        manager.set_busy(&task_id3).await;

        // すべてBusyなのでNone
        assert!(manager
            .select_idle_instance_round_robin("test_task")
            .await
            .is_none());

        // 1つをIdleに戻す
        manager.set_idle(&task_id1).await;
        let selected3 = manager
            .select_idle_instance_round_robin("test_task")
            .await
            .unwrap();
        assert_eq!(selected3, task_id1);
    }

    #[tokio::test]
    async fn test_count_idle_instances() {
        let manager = TaskStateManager::new();
        let task_id1 = TaskId::new();
        let task_id2 = TaskId::new();

        manager
            .register_task("test_task".to_string(), task_id1.clone())
            .await;
        manager
            .register_task("test_task".to_string(), task_id2.clone())
            .await;

        assert_eq!(manager.count_idle_instances("test_task").await, 2);

        manager.set_busy(&task_id1).await;
        assert_eq!(manager.count_idle_instances("test_task").await, 1);

        manager.set_busy(&task_id2).await;
        assert_eq!(manager.count_idle_instances("test_task").await, 0);
    }
}
