# リネーム後の不整合内容と命名の再検討

**作成日**: 2025-01-XX  
**目的**: k8s概念に合わせたリネーム後の、名称と実装の不整合を整理

---

## 概要

k8sの概念に合わせて以下のリネームを実施しました：

- `task_runtime` → `pod`
- `lifecycle` → `replicaset`
- `round_robin` → `service` （**再検討が必要**）

しかし、リネーム後の名称と実装内容に不整合が存在します。特に、`service` は k8s の Service（外部アクセス向けのロードバランシング）とは異なる概念であることが判明しました。本ドキュメントでは、これらの不整合を整理し、適切な命名を再検討します。

---

## `service` の命名に関する問題

### 現状の問題

**k8s の Service との違い:**
- k8s の Service: 外部からのアクセス（HTTP、TCP など）をロードバランシングする
- 現在の実装: 内部メッセージングシステムでのトピックメッセージの配信

**現在の実装の特徴:**
- トピックベースの内部メッセージング
- 同一タスク名の複数インスタンス間での Round Robin 配信
- メッセージキューイング機能
- 外部アクセスではなく、システム内部のメッセージ配信

**結論:**
- k8s の Service とは異なる概念であるため、別の名称を割り当てる必要がある

### 命名の決定

内部メッセージングの配信に特化した名称として、**`TopicDispatcher`** を採用しました。

**採用理由:**
- トピックメッセージに特化していることを明確に表現
- `message` が付くものは `topic` に統一（`MessageQueue` → `TopicQueue`）
- k8s の概念と混同されない
- ディレクトリ: `topic_dispatcher`

---

## 不整合内容

### 1. `service` ディレクトリ名が k8s の Service と混同される

**問題点:**
- k8s の Service は外部アクセス向けのロードバランシング
- 現在の実装は内部メッセージングの配信
- 異なる概念であるため、別の名称が必要

**推奨される修正:**
- `service` → `message_dispatcher` または `dispatcher`
- `Service` → `MessageDispatcher`

---

### 2. `PodStateManager` に Round Robin 関連のロジックが含まれている

**問題点:**
- `PodStateManager` に `select_idle_instance_round_robin()` メソッドが存在する
- `round_robin_indices` フィールドが存在する
- Round Robin は MessageDispatcher 層の責務であり、Pod の状態管理とは分離すべき

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
    round_robin_indices: Arc<RwLock<HashMap<String, usize>>>,  // ← MessageDispatcher層の責務
}

impl PodStateManager {
    // ← MessageDispatcher層の責務
    pub async fn select_idle_instance_round_robin(&self, pod_name: &str) -> Option<TaskId> {
        // Round Robin の選択ロジック
    }
}
```

**問題の影響:**
- Pod 層が MessageDispatcher 層の実装詳細（Round Robin）を知っている
- 他のロードバランシング方式（Least Connections など）を追加する際に、Pod 層を変更する必要がある
- 責務の分離が不十分

**推奨される修正:**
1. `select_idle_instance_round_robin()` メソッドを `MessageDispatcher` に移動
2. `round_robin_indices` を `MessageDispatcher` で管理
3. `PodStateManager` は状態管理（idle/busy）とインスタンス一覧の取得のみを担当
4. `MessageDispatcher` が必要に応じて `PodStateManager` から idle なインスタンス一覧を取得し、Round Robin で選択

---

### 3. `Service` のコメントに実装詳細が含まれている

**問題点:**
- `Service::dispatch_message()` のコメントに「Round Robin方式」という実装詳細が記載されている
- `MessageDispatcher` は抽象化された概念であり、実装詳細（Round Robin、Least Connections など）は内部に隠蔽すべき

**該当ファイル:**
- `src/service/dispatcher.rs` （リネーム後は `src/message_dispatcher/dispatcher.rs`）

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
- `MessageDispatcher` の抽象化が不十分

**推奨される修正:**
- コメントを「メッセージをロードバランシング方式で配信」など、実装詳細を含まない表現に変更

---

### 4. `PodStateManager` のコメントに MessageDispatcher 層への言及がある

**問題点:**
- `PodStateManager` のコメントに「Service用」という記載がある
- Pod 層が MessageDispatcher 層を意識するのは責務違反

**該当ファイル:**
- `src/pod/state.rs`

**該当箇所:**
- 28行目: `/// Pod名ごとのRound Robinインデックス（Service用）`
- 203行目: `/// 内部のname_to_idsへのアクセス（Service用）`

**現状の実装:**
```rust
pub struct PodStateManager {
    /// Pod名ごとのRound Robinインデックス（Service用）  // ← MessageDispatcher層への言及
    round_robin_indices: Arc<RwLock<HashMap<String, usize>>>,
}

impl PodStateManager {
    /// 内部のname_to_idsへのアクセス（Service用）  // ← MessageDispatcher層への言及
    pub(crate) fn name_to_ids(&self) -> Arc<RwLock<HashMap<String, Vec<TaskId>>>> {
        self.name_to_ids.clone()
    }
}
```

**問題の影響:**
- Pod 層と MessageDispatcher 層の依存関係が逆転している
- 責務の分離が不十分

**推奨される修正:**
- コメントから「Service用」を削除
- より汎用的な説明に変更（例：「Pod名ごとのインスタンス一覧へのアクセス」）

---

## 修正方針

### 優先度: 高

1. **`service` → `topic_dispatcher` へのリネーム**
   - ディレクトリ: `src/service` → `src/topic_dispatcher`
   - 型名: `Service` → `TopicDispatcher`
   - `MessageQueue` → `TopicQueue`
   - すべての参照を更新

2. **Round Robin ロジックの移動**
   - `PodStateManager::select_idle_instance_round_robin()` を `TopicDispatcher` に移動
   - `round_robin_indices` を `TopicDispatcher` で管理
   - `PodStateManager` は `get_idle_instances(pod_name)` のような汎用的なメソッドを提供

3. **コメントの修正**
   - `TopicDispatcher::dispatch_message()` のコメントから「Round Robin方式」を削除（既に「ロードバランシング方式」に変更済み）
   - `PodStateManager` のコメントから「Service用」を削除（既に「TopicDispatcher用」に変更済み）

### 優先度: 中

4. **抽象化の強化**
   - `TopicDispatcher` を抽象化し、将来的に異なるロードバランシング方式を追加できるようにする
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

### `TopicDispatcher`
```rust
pub struct TopicDispatcher {
    pod_manager: PodManager,
    pod_state_manager: PodStateManager,
    topic_queue: TopicQueue,
    round_robin_indices: Arc<RwLock<HashMap<String, usize>>>,  // ← ここで管理
}

impl TopicDispatcher {
    /// トピックメッセージをロードバランシング方式で配信  // ← 実装詳細を隠蔽
    pub async fn dispatch_message(...) -> DispatchResult {
        // Round Robin の選択ロジックをここで実装
        let idle_instances = self.pod_state_manager.get_idle_instances(task_name).await;
        // Round Robin で選択
    }
}
```

---

## 命名の再検討まとめ

### 現在の命名
- `service` → k8s の Service と混同される可能性

### 決定された命名
- `topic_dispatcher` → トピックメッセージの配信に特化した名称
- `TopicDispatcher` → 責務が明確で、k8s の概念と混同されない
- `MessageQueue` → `TopicQueue` に変更（`message` が付くものは `topic` に統一）

### 命名の理由
1. **トピックメッセージに特化**: トピックベースの内部メッセージングシステムでの配信
2. **k8s との区別**: k8s の Service（外部アクセス向け）とは異なる概念であることを明確化
3. **責務の明確化**: トピックメッセージの配信という責務を明確に表現
4. **命名の一貫性**: `message` が付くものは `topic` に統一

---

## 関連ファイル

- `src/pod/state.rs` - PodStateManager の実装
- `src/topic_dispatcher/dispatcher.rs` - TopicDispatcher の実装
- `src/pod/mod.rs` - Pod モジュールの公開 API
- `src/topic_broker.rs` - TopicBroker の実装（責務整理が必要、別ドキュメント参照）

---

## 関連ドキュメント

- `docs/topic_broker_responsibilities.md` - TopicBroker の責務整理

---

## 更新履歴

- 2025-01-XX: 初版作成（リネーム後の不整合内容の整理）
- 2025-01-XX: TopicBroker の責務整理を別ドキュメントに分離

