# TopicBroker の責務整理

**作成日**: 2025-01-XX  
**目的**: `TopicBroker` の責務を整理し、`TopicDispatcher` との関係を明確化

**関連ドキュメント**: `docs/pubsub_naming_recommendations.md` - Pub/Sub 世界での命名推奨

---

## 概要

`TopicBroker` は現在、トピックメッセージの購読者管理とブロードキャストを担当していますが、`TopicDispatcher` との責務の分離が不十分です。本ドキュメントでは、`TopicBroker` の責務を整理し、k8s 概念との対応を検討します。

---

## 現在の `TopicBroker` の責務

### 1. 購読者管理
- トピックへの購読者（subscriber）の追加・削除
- タスク（Pod）とトピックの購読関係の管理
- 失敗した購読者の自動削除

### 2. メッセージブロードキャスト
- トピックメッセージをすべての購読者に配信
- 配信結果の追跡（成功・失敗）
- 失敗した購読者の自動削除

### 3. 最新メッセージの保持
- 各トピックの最新メッセージを保持
- 後から購読したタスクが最新メッセージを取得可能

---

## 問題点

### 1. `TopicBroker` と `TopicDispatcher` の責務の重複

**現状:**
- `TopicBroker::broadcast_message()` はすべての購読者にブロードキャスト
- Round Robin モードのタスクにもブロードキャストが適用される
- `TopicDispatcher` は Round Robin 配信を担当するが、`TopicBroker` との統合が不十分

**問題:**
- Round Robin モードのタスクには、`TopicBroker` ではなく `TopicDispatcher` で配信すべき
- 現在は両方が独立して動作しており、責務が重複している

### 2. k8s 概念との対応が不明確

**k8s の概念:**
- **Service**: 外部アクセス向けのロードバランシング（HTTP、TCP など）
- **Ingress**: 外部からのトラフィックを内部の Service にルーティング
- **Pub/Sub**: k8s には直接対応する概念がない（メッセージングシステムの概念）

**`TopicBroker` の位置づけ:**
- k8s の概念には直接対応しない
- メッセージングシステム（Kafka、RabbitMQ など）の Pub/Sub パターンに近い
- 内部メッセージングシステムの基盤として機能

---

## 責務の再定義

### `TopicBroker` の責務（推奨）

1. **購読者管理（Subscription Management）**
   - トピックへの購読者の登録・削除
   - タスク（Pod）とトピックの購読関係の管理
   - 購読者の状態管理（有効・無効）

2. **購読者リストの提供**
   - 特定のトピックの購読者リストを取得
   - タスク名ごとにグループ化された購読者リストの提供

3. **最新メッセージの保持**
   - 各トピックの最新メッセージを保持
   - 後から購読したタスクが最新メッセージを取得可能

### `TopicDispatcher` の責務（推奨）

1. **ロードバランシング配信**
   - 同一タスク名の複数インスタンス間での Round Robin 配信
   - タスクの状態（idle/busy）に基づいた配信制御
   - メッセージキューイング（すべてのインスタンスが busy の場合）

2. **`TopicBroker` との統合**
   - `TopicBroker` から購読者リストを取得
   - タスク名ごとにグループ化
   - Round Robin モードのタスクには Round Robin で配信
   - ブロードキャストモードのタスクには `TopicBroker` に委譲

---

## 理想的な実装構造

### 現在の実装の問題

```rust
// TopicBroker: すべての購読者にブロードキャスト
topic_broker.broadcast_message(event).await;

// TopicDispatcher: Round Robin 配信（ただし、TopicBroker と統合されていない）
topic_dispatcher.dispatch_message(task_name, topic, data).await;
```

### 推奨される実装

```rust
// TopicBroker: 購読者管理とブロードキャスト（Round Robin モード以外）
impl TopicBroker {
    /// 購読者リストを取得（タスク名ごとにグループ化）
    pub async fn get_subscribers_by_task_name(
        &self,
        topic: &str,
    ) -> HashMap<String, Vec<Arc<ExecutorOutputEventSender>>> {
        // タスク名ごとにグループ化して返す
    }

    /// ブロードキャスト配信（Round Robin モード以外のタスク用）
    pub async fn broadcast_message(
        &self,
        event: ExecutorOutputEvent,
    ) -> Result<usize, String> {
        // すべての購読者にブロードキャスト
    }
}

// TopicDispatcher: Round Robin 配信と TopicBroker との統合
impl TopicDispatcher {
    /// トピックメッセージを配信（Round Robin モードの判定を含む）
    pub async fn dispatch_message(
        &self,
        topic: String,
        data: String,
    ) -> DispatchResult {
        // 1. TopicBroker から購読者リストを取得
        let subscribers_by_task = self.topic_broker
            .get_subscribers_by_task_name(&topic)
            .await;

        // 2. 各タスク名ごとに処理
        for (task_name, subscribers) in subscribers_by_task {
            // 3. Round Robin モードかどうかを判定
            if self.is_round_robin_mode(&task_name) {
                // 4. Round Robin で配信
                self.dispatch_round_robin(&task_name, &topic, &data).await;
            } else {
                // 5. ブロードキャスト（TopicBroker に委譲）
                self.topic_broker.broadcast_message(event).await;
            }
        }
    }
}
```

---

## 修正方針

### 優先度: 高

1. **`TopicBroker` の責務の明確化**
   - 購読者管理とブロードキャストに特化
   - Round Robin 配信のロジックは `TopicDispatcher` に移動

2. **`TopicBroker` と `TopicDispatcher` の統合**
   - `TopicBroker` から購読者リストを取得する API を追加
   - `TopicDispatcher` が `TopicBroker` を使用して配信を制御

3. **配信モードの判定**
   - タスクの設定（`mode = "round_robin"`）に基づいて配信モードを判定
   - Round Robin モード: `TopicDispatcher` で配信
   - ブロードキャストモード: `TopicBroker` で配信

### 優先度: 中

4. **命名の検討**
   - `TopicBroker` は適切か？別の名称（`TopicRegistry`、`TopicSubscriber` など）の検討
   - k8s 概念との対応を明確化

---

## k8s 概念との対応

### 現在の実装

| コンポーネント | k8s 概念 | 説明 |
|------------|---------|------|
| `Pod` | Pod | タスクの実行単位 |
| `ReplicaSetController` | ReplicaSet | タスクのレプリカ管理 |
| `TopicDispatcher` | - | 内部メッセージングのロードバランシング（k8s にない概念） |
| `TopicBroker` | - | 内部メッセージングの Pub/Sub（k8s にない概念） |

### 結論

- `TopicBroker` と `TopicDispatcher` は k8s の概念には直接対応しない
- 内部メッセージングシステムの独自の概念として扱う
- 命名は k8s 準拠ではなく、メッセージングシステムの概念に準拠

---

## 関連ファイル

- `src/topic_broker.rs` - TopicBroker の実装
- `src/topic_dispatcher/dispatcher.rs` - TopicDispatcher の実装
- `src/pod/spawner.rs` - TopicBroker の使用箇所

---

## 関連ドキュメント

- `docs/pubsub_naming_recommendations.md` - Pub/Sub 世界での命名推奨

---

## 更新履歴

- 2025-01-XX: 初版作成（TopicBroker の責務整理）
- 2025-01-XX: Pub/Sub 世界での命名推奨を追加

