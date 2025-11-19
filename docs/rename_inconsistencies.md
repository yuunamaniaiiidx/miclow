# リネーム後の不整合内容

**作成日**: 2025-01-XX  
**目的**: k8s概念に合わせたリネーム後の、名称と実装の不整合を整理

---

## 概要

k8sの概念に合わせて以下のリネームを実施しました：

- `task_runtime` → `pod`
- `lifecycle` → `replicaset`
- `round_robin` → `service`

しかし、リネーム後の名称と実装内容に不整合が存在します。本ドキュメントでは、これらの不整合を整理し、今後のリファクタリング方針を明確化します。

---

## 不整合内容

### 1. `PodStateManager` に Round Robin 関連のロジックが含まれている

**問題点:**
- `PodStateManager` に `select_idle_instance_round_robin()` メソッドが存在する
- `round_robin_indices` フィールドが存在する
- Round Robin は Service 層の責務であり、Pod の状態管理とは分離すべき

**該当ファイル:**
- `src/pod/state.rs`

**該当箇所:**
- 28行目: `round_robin_indices` フィールドの定義
- 37行目: `round_robin_indices` の初期化
- 58行目: `round_robin_indices` の使用（unregister_pod 内）
- 125-170行目: `select_idle_instance_round_robin()` メソッドの実装
- 203行目: コメントに「Service用」と記載

**現状の実装:**
```rust
pub struct PodStateManager {
    pod_states: Arc<RwLock<HashMap<TaskId, PodInstanceState>>>,
    name_to_ids: Arc<RwLock<HashMap<String, Vec<TaskId>>>>,
    round_robin_indices: Arc<RwLock<HashMap<String, usize>>>,  // ← Service層の責務
}

impl PodStateManager {
    // ← Service層の責務
    pub async fn select_idle_instance_round_robin(&self, pod_name: &str) -> Option<TaskId> {
        // Round Robin の選択ロジック
    }
}
```

**問題の影響:**
- Pod 層が Service 層の実装詳細（Round Robin）を知っている
- 他のロードバランシング方式（Least Connections など）を追加する際に、Pod 層を変更する必要がある
- 責務の分離が不十分

**推奨される修正:**
1. `select_idle_instance_round_robin()` メソッドを `Service` に移動
2. `round_robin_indices` を `Service` で管理
3. `PodStateManager` は状態管理（idle/busy）とインスタンス一覧の取得のみを担当
4. `Service` が必要に応じて `PodStateManager` から idle なインスタンス一覧を取得し、Round Robin で選択

---

### 2. `Service` のコメントに実装詳細が含まれている

**問題点:**
- `Service::dispatch_message()` のコメントに「Round Robin方式」という実装詳細が記載されている
- `Service` は抽象化された概念であり、実装詳細（Round Robin、Least Connections など）は内部に隠蔽すべき

**該当ファイル:**
- `src/service/dispatcher.rs`

**該当箇所:**
- 92行目: `/// メッセージをRound Robin方式で配信`

**現状の実装:**
```rust
impl Service {
    /// メッセージをRound Robin方式で配信  // ← 実装詳細が露出
    /// すべてのインスタンスがBusyの場合はキューに追加
    pub async fn dispatch_message(...) -> DispatchResult {
        // Round Robin の実装
    }
}
```

**問題の影響:**
- 将来的に他のロードバランシング方式を追加する際、コメントと実装が不一致になる可能性
- `Service` の抽象化が不十分

**推奨される修正:**
- コメントを「メッセージをロードバランシング方式で配信」など、実装詳細を含まない表現に変更

---

### 3. `PodStateManager` のコメントに Service 層への言及がある

**問題点:**
- `PodStateManager` のコメントに「Service用」という記載がある
- Pod 層が Service 層を意識するのは責務違反

**該当ファイル:**
- `src/pod/state.rs`

**該当箇所:**
- 28行目: `/// Pod名ごとのRound Robinインデックス（Service用）`
- 203行目: `/// 内部のname_to_idsへのアクセス（Service用）`

**現状の実装:**
```rust
pub struct PodStateManager {
    /// Pod名ごとのRound Robinインデックス（Service用）  // ← Service層への言及
    round_robin_indices: Arc<RwLock<HashMap<String, usize>>>,
}

impl PodStateManager {
    /// 内部のname_to_idsへのアクセス（Service用）  // ← Service層への言及
    pub(crate) fn name_to_ids(&self) -> Arc<RwLock<HashMap<String, Vec<TaskId>>>> {
        self.name_to_ids.clone()
    }
}
```

**問題の影響:**
- Pod 層と Service 層の依存関係が逆転している
- 責務の分離が不十分

**推奨される修正:**
- コメントから「Service用」を削除
- より汎用的な説明に変更（例：「Pod名ごとのインスタンス一覧へのアクセス」）

---

## 修正方針

### 優先度: 高

1. **Round Robin ロジックの移動**
   - `PodStateManager::select_idle_instance_round_robin()` を `Service` に移動
   - `round_robin_indices` を `Service` で管理
   - `PodStateManager` は `get_idle_instances(pod_name)` のような汎用的なメソッドを提供

2. **コメントの修正**
   - `Service::dispatch_message()` のコメントから「Round Robin方式」を削除
   - `PodStateManager` のコメントから「Service用」を削除

### 優先度: 中

3. **抽象化の強化**
   - `Service` を抽象化し、将来的に異なるロードバランシング方式を追加できるようにする
   - 必要に応じて `LoadBalancer` トレイトを導入

---

## 修正後の理想的な構造

### `PodStateManager`
```rust
pub struct PodStateManager {
    pod_states: Arc<RwLock<HashMap<TaskId, PodInstanceState>>>,
    name_to_ids: Arc<RwLock<HashMap<String, Vec<TaskId>>>>,
    // round_robin_indices は削除
}

impl PodStateManager {
    // Round Robin の選択ロジックは削除
    // 代わりに汎用的なメソッドを提供
    pub async fn get_idle_instances(&self, pod_name: &str) -> Vec<TaskId> {
        // idle なインスタンスのリストを返す
    }
}
```

### `Service`
```rust
pub struct Service {
    pod_manager: PodManager,
    pod_state_manager: PodStateManager,
    message_queue: MessageQueue,
    round_robin_indices: Arc<RwLock<HashMap<String, usize>>>,  // ← ここで管理
}

impl Service {
    /// メッセージをロードバランシング方式で配信  // ← 実装詳細を隠蔽
    pub async fn dispatch_message(...) -> DispatchResult {
        // Round Robin の選択ロジックをここで実装
        let idle_instances = self.pod_state_manager.get_idle_instances(task_name).await;
        // Round Robin で選択
    }
}
```

---

## 関連ファイル

- `src/pod/state.rs` - PodStateManager の実装
- `src/service/dispatcher.rs` - Service の実装
- `src/pod/mod.rs` - Pod モジュールの公開 API

---

## 更新履歴

- 2025-01-XX: 初版作成（リネーム後の不整合内容の整理）

