# Pub/Sub 世界での命名推奨

**作成日**: 2025-01-XX  
**目的**: `TopicBroker` と `TopicDispatcher` の Pub/Sub 世界での適切な命名を検討

---

## 現在の実装の責務

### `TopicBroker`
- 購読者管理（Subscription Management）
- メッセージブロードキャスト
- 最新メッセージの保持
- トピックと購読者の関係管理

### `TopicDispatcher`
- Round Robin 方式でのロードバランシング配信
- タスクの状態（idle/busy）に基づいた配信制御
- メッセージキューイング

---

## Pub/Sub 世界での一般的な命名

### 主要コンポーネント

1. **Message Broker**
   - メッセージのルーティングや配信を管理する仲介者
   - パブリッシャーとサブスクライバーの間でメッセージを仲介
   - 例: Kafka Broker, RabbitMQ Broker

2. **Subscription Registry / Subscription Manager**
   - 購読者の管理
   - トピックと購読者の関係を管理
   - 購読者の登録・削除

3. **Message Router**
   - メッセージのルーティングを担当
   - メッセージを適切な宛先に振り分け

4. **Load Balancer**
   - 負荷分散を担当
   - 複数のインスタンス間でメッセージを分散

5. **Dispatcher**
   - メッセージの配信を担当
   - 通常は Message Broker の一部機能

---

## 現在の実装と Pub/Sub 概念の対応

### `TopicBroker` の分析

**現在の責務:**
- 購読者管理 ✓
- ブロードキャスト配信 ✓
- 最新メッセージの保持 ✓

**Pub/Sub での位置づけ:**
- **Subscription Registry** に近い（購読者管理が主な責務）
- **Message Broker** の一部機能（ブロードキャスト配信）
- ただし、メッセージの永続化やキューイングは行わない

**推奨命名:**
1. **`TopicSubscriptionRegistry`** （推奨）
   - 購読者管理が主な責務であることを明確に表現
   - Pub/Sub の Subscription Registry に準拠

2. **`TopicRegistry`**
   - より簡潔
   - トピックと購読者の管理を表現

3. **`SubscriptionManager`**
   - 購読者管理に特化
   - ただし、トピックの概念が弱い

### `TopicDispatcher` の分析

**現在の責務:**
- Round Robin 方式でのロードバランシング配信 ✓
- タスクの状態に基づいた配信制御 ✓
- メッセージキューイング ✓

**Pub/Sub での位置づけ:**
- **Load Balancer** に近い（負荷分散が主な責務）
- **Message Router** の一部機能（ルーティング）
- ただし、メッセージの永続化は行わない

**推奨命名:**
1. **`TopicLoadBalancer`** （推奨）
   - ロードバランシングが主な責務であることを明確に表現
   - Pub/Sub の Load Balancer に準拠

2. **`TopicRouter`**
   - メッセージのルーティングを表現
   - ただし、ロードバランシングの側面が弱い

3. **`MessageLoadBalancer`**
   - より汎用的
   - ただし、`message` は `topic` に統一すべき

---

## 推奨される命名

### オプション 1: Subscription Registry + Load Balancer

| 現在の名称 | 推奨名称 | 理由 |
|----------|---------|------|
| `TopicBroker` | `TopicSubscriptionRegistry` | 購読者管理が主な責務 |
| `TopicDispatcher` | `TopicLoadBalancer` | ロードバランシングが主な責務 |

**メリット:**
- Pub/Sub の一般的な命名規則に準拠
- 責務が明確に表現される
- 他のメッセージングシステムとの整合性

**デメリット:**
- 名称が長い
- `Registry` という用語がやや堅い

### オプション 2: Registry + Router

| 現在の名称 | 推奨名称 | 理由 |
|----------|---------|------|
| `TopicBroker` | `TopicRegistry` | トピックと購読者の管理 |
| `TopicDispatcher` | `TopicRouter` | メッセージのルーティング |

**メリット:**
- 名称が簡潔
- 一般的な用語

**デメリット:**
- ロードバランシングの側面が弱い
- `Router` は一般的にルーティング全般を指す

### オプション 3: Subscription Manager + Load Balancer

| 現在の名称 | 推奨名称 | 理由 |
|----------|---------|------|
| `TopicBroker` | `TopicSubscriptionManager` | 購読者管理に特化 |
| `TopicDispatcher` | `TopicLoadBalancer` | ロードバランシングが主な責務 |

**メリット:**
- 購読者管理が明確
- ロードバランシングが明確

**デメリット:**
- `Manager` はやや汎用的

---

## 最終推奨

### **`TopicSubscriptionRegistry` + `TopicLoadBalancer`**

**理由:**
1. **Pub/Sub の一般的な命名規則に準拠**
   - Subscription Registry: 購読者管理の標準的な用語
   - Load Balancer: 負荷分散の標準的な用語

2. **責務が明確**
   - `TopicSubscriptionRegistry`: 購読者管理が主な責務
   - `TopicLoadBalancer`: ロードバランシングが主な責務

3. **他のメッセージングシステムとの整合性**
   - Kafka, RabbitMQ, NATS などの一般的な用語と整合

4. **命名の一貫性**
   - 両方とも `Topic` プレフィックスで統一
   - `message` が付くものは `topic` に統一（既存の方針）

---

## 実装例

### リネーム後の構造

```rust
// 購読者管理とブロードキャスト
pub struct TopicSubscriptionRegistry {
    subscribers: Arc<RwLock<HashMap<String, Arc<Vec<Arc<ExecutorOutputEventSender>>>>>>,
    task_subscriptions: Arc<RwLock<HashMap<(String, TaskId), Weak<ExecutorOutputEventSender>>>>,
    latest_messages: Arc<RwLock<HashMap<String, ExecutorOutputEvent>>>,
}

// ロードバランシング配信
pub struct TopicLoadBalancer {
    pod_manager: PodManager,
    pod_state_manager: PodStateManager,
    topic_queue: TopicQueue,
    round_robin_indices: Arc<RwLock<HashMap<String, usize>>>,
}
```

---

## 比較表

| 観点 | TopicBroker | TopicSubscriptionRegistry |
|------|------------|---------------------------|
| 購読者管理 | ✓ | ✓ |
| ブロードキャスト | ✓ | ✓ |
| 命名の明確性 | やや曖昧（Broker は広義） | 明確（Subscription Registry） |
| Pub/Sub 準拠 | 部分的 | 完全 |

| 観点 | TopicDispatcher | TopicLoadBalancer |
|------|----------------|-------------------|
| ロードバランシング | ✓ | ✓ |
| ルーティング | ✓ | ✓ |
| 命名の明確性 | やや曖昧（Dispatcher は広義） | 明確（Load Balancer） |
| Pub/Sub 準拠 | 部分的 | 完全 |

---

## 関連ドキュメント

- `docs/topic_broker_responsibilities.md` - TopicBroker の責務整理
- `docs/rename_inconsistencies.md` - リネーム後の不整合内容

---

## 更新履歴

- 2025-01-XX: 初版作成（Pub/Sub 世界での命名推奨）

