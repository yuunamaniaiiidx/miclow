# String型のClone使用状況調査レポート

## 調査目的
`String`型が使われている箇所で`Clone`が頻繁に呼ばれている場合、`Arc<str>`に変更することでメモリ効率を向上させる可能性があるため、該当箇所を特定しました。

## 調査結果

### 1. `task_name: String` - 高頻度でCloneが発生

#### 使用箇所
- **`src/replicaset/worker.rs`** (line 20, 29)
  - `ReplicaSetWorker`構造体のフィールド
  - `&task_name`として参照で渡されているが、構造体自体が複数箇所で使用

- **`src/replicaset/context.rs`** (line 17)
  - `ReplicaSetSpec`構造体のフィールド
  - `PodStartContext`が`Clone`を実装しているため、`task_name`も`clone()`される

- **`src/deployment/mod.rs`** (line 47, 58)
  - `task.name.clone()`が2箇所で呼ばれている
  - `ReplicaSetSpec`の作成時と`deployments`への登録時

- **`src/config/mod.rs`** (line 36, 55)
  - `TaskConfig`と`ExpandedTaskConfig`の`name: String`フィールド
  - これらは`Clone`を実装しているため、`name`も`clone()`される

#### Clone呼び出し回数
- `deployment/mod.rs`: 2回
- `context.rs`経由: 複数回（`PodStartContext`の`Clone`実装により）

### 2. `pod_name: String` - 高頻度でCloneが発生

#### 使用箇所
- **`src/pod/spawner.rs`** (line 25, 37, 67, 84)
  - `PodSpawner`構造体のフィールド
  - `spawn()`メソッド内で複数回`clone()`が呼ばれている

#### Clone呼び出し回数
- line 67: `let pod_name: String = self.pod_name.clone();`
- line 84: `let pod_name = pod_name.clone();` (tokio::task::spawn内)
- line 155: `task_name: pod_name.clone()` (UserLogEvent送信時)
- line 160: `task_name: pod_name.clone()` (UserLogEvent送信時)

**合計: 4回以上**

### 3. `subscribe_topics: Vec<String>` と `private_response_topics: Vec<String>`

#### 使用箇所
- **`src/replicaset/context.rs`** (line 10, 11)
  - `PodStartContext`構造体のフィールド
  - `Clone`を実装しているため、`Vec<String>`全体が`clone()`される

- **`src/replicaset/worker.rs`** (line 55, 73)
  - `start_context.subscribe_topics.clone()` (line 55)
  - `start_context.private_response_topics.clone()` (line 73)

- **`src/deployment/mod.rs`** (line 40, 41)
  - `task.subscribe_topics.clone()`
  - `task.private_response_topics.clone()`

#### Clone呼び出し回数
- `worker.rs`: 2回
- `deployment/mod.rs`: 2回
- `context.rs`経由: 複数回（`PodStartContext`の`Clone`実装により）

### 4. `UserLogEvent`内の`task_name: String`

#### 使用箇所
- **`src/logging.rs`** (line 84)
  - `UserLogEvent`構造体のフィールド
  - `Clone`を実装しているため、`task_name`も`clone()`される

- **`src/pod/spawner.rs`** (line 155, 160)
  - `UserLogEvent`送信時に`pod_name.clone()`が呼ばれている

## 推奨される変更箇所

### 優先度: 高
1. **`task_name: String` → `Arc<str>`**
   - `src/replicaset/worker.rs`
   - `src/replicaset/context.rs`
   - `src/deployment/mod.rs`
   - `src/config/mod.rs`

2. **`pod_name: String` → `Arc<str>`**
   - `src/pod/spawner.rs`
   - `src/logging.rs` (`UserLogEvent`の`task_name`)

### 優先度: 中
3. **`subscribe_topics: Vec<String>` → `Vec<Arc<str>>`**
   - `src/replicaset/context.rs`
   - `src/replicaset/worker.rs`
   - `src/deployment/mod.rs`
   - `src/config/mod.rs`

4. **`private_response_topics: Vec<String>` → `Vec<Arc<str>>`**
   - 上記と同じ箇所

## 変更時の注意点

1. **`Arc<str>`への変換**
   - `String`から`Arc<str>`への変換: `Arc::from(string)` または `Arc::from(string.as_str())`
   - `&str`から`Arc<str>`への変換: `Arc::from(s)`

2. **`Arc<str>`から`&str`への変換**
   - `arc_str.as_ref()` または `&*arc_str`

3. **`Vec<String>`から`Vec<Arc<str>>`への変換**
   - `vec.into_iter().map(|s| Arc::from(s)).collect()`

4. **比較処理**
   - `Arc<str>`同士の比較は`==`で可能（`PartialEq`が実装されている）
   - `&str`との比較も`==`で可能

5. **ログ出力**
   - `Arc<str>`は`Display`を実装しているため、そのまま使用可能

6. **HashMap/HashSetのキー**
   - `Arc<str>`は`Hash`と`Eq`を実装しているため、キーとして使用可能

## 影響範囲

以下のファイルが影響を受ける可能性があります：

- `src/replicaset/worker.rs`
- `src/replicaset/context.rs`
- `src/pod/spawner.rs`
- `src/deployment/mod.rs`
- `src/config/mod.rs`
- `src/logging.rs`
- `src/topic/topic_subscription_registry.rs` (トピック名の処理)
- `src/backend/miclowstdio/runner.rs` (topic_nameの処理)

