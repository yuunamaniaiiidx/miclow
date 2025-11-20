# Phase 3 実装完了報告

**完了日**: 2025-01-XX  
**実装内容**: キュー処理の実装

---

## 実装内容

### 1. TopicLoadBalancer の拡張

#### 追加したメソッド
- ✅ `process_queue(task_name, topic)` - 特定のトピックのキューを処理
  - キューからメッセージを取得
  - Idle なインスタンスを選択（Round Robin 方式）
  - メッセージを送信し、Pod を busy に設定
  - キューが空になるか、すべてのインスタンスが busy になるまで繰り返す

#### 設計のポイント
- `TopicLoadBalancer` は `subscribe_topics` を知る必要がない
- `task_name` と `topic` を受け取るだけで処理できる
- 責務が明確に分離されている

### 2. TopicSubscriptionRegistry の拡張

#### 追加したメソッド
- ✅ `process_queue_for_task(task_name)` - タスクが idle に戻った時にキューを処理
  - `SystemConfig` からタスクの `subscribe_topics` を取得
  - 各トピックについて `TopicLoadBalancer::process_queue()` を呼び出す

#### 設計のポイント
- `TopicSubscriptionRegistry` が「タスク名 → 購読トピック」の情報を持っている
- キュー処理のコーディネーションを担当
- `TopicLoadBalancer` は純粋にロードバランシングとキュー処理のみを担当

### 3. PodSpawner の修正

#### 変更内容
- ✅ TopicResponse 受信時に `process_queue_for_task()` を呼び出す
- タスクが idle に戻った後、自動的にキュー処理がトリガーされる

---

## 実装の詳細

### キュー処理の動作フロー

```
1. タスクが TopicResponse を返す
2. PodSpawner で TopicResponse を受信
3. タスクを idle に戻す
4. TopicSubscriptionRegistry::process_queue_for_task(task_name) を呼び出す
5. SystemConfig からタスクの subscribe_topics を取得
6. 各トピックについて:
   a. TopicLoadBalancer::process_queue(task_name, topic) を呼び出す
   b. キューからメッセージを取得
   c. Idle なインスタンスを選択（Round Robin 方式）
   d. メッセージを送信し、Pod を busy に設定
   e. キューが空になるか、すべて busy になるまで繰り返し
```

### 責務の分離

**TopicLoadBalancer:**
- 特定のトピックのキューを処理
- Round Robin 方式でインスタンスを選択
- メッセージの送信

**TopicSubscriptionRegistry:**
- タスク名から購読トピックを取得
- キュー処理のコーディネーション
- 各トピックについて `TopicLoadBalancer` に処理を依頼

---

## テスト結果

### 実行したテスト
- ✅ すべての既存テストが通過（48 passed）
- ✅ ビルド成功
- ✅ 既存の機能は壊れていません

### 未実装のテスト
- ⚠️ キュー処理の統合テスト（Phase 3-4）は後で実装予定

---

## 変更ファイル

### 修正したファイル
1. `src/topic_load_balancer/balancer.rs`
   - `process_queue()` メソッドを追加

2. `src/topic_subscription_registry.rs`
   - `process_queue_for_task()` メソッドを追加

3. `src/pod/spawner.rs`
   - TopicResponse 受信時にキュー処理をトリガー

---

## 設計の改善点

### Before（変更前）
```
TopicResponse 受信
  ↓
タスクを idle に戻す
  ↓
（キュー処理なし）
```

### After（変更後）
```
TopicResponse 受信
  ↓
タスクを idle に戻す
  ↓
キュー処理をトリガー
  ↓
購読トピックのキューを処理
  ↓
Idle なインスタンスに配信
```

---

## 次のステップ（Phase 4）

Phase 3 が完了したので、次は Phase 4 の実装に進みます：

1. **外部メッセージ受信のエントリーポイント**
   - `TopicSubscriptionRegistry::publish_message()` の実装（既に実装済み）
   - 外部システムからのメッセージ受信インターフェース

2. **統合テスト**
   - 外部からのメッセージ送信のテスト
   - Round Robin 配信のテスト
   - ブロードキャスト配信のテスト
   - キュー処理のテスト

---

## 注意事項

1. **メッセージのロスト**: 送信失敗したメッセージはキューに戻さず、ロストします。これは設計上の選択ですが、将来的にリトライ機能を追加することも可能です。

2. **パフォーマンス**: キュー処理は同期的に実行されるため、大量のメッセージがある場合は処理に時間がかかる可能性があります。将来的にバッチ処理や非同期処理を検討することも可能です。

3. **エラーハンドリング**: キュー処理中のエラーはログに記録されますが、処理は継続されます。

---

## まとめ

Phase 3 の実装が完了し、タスクが idle に戻った時に自動的にキューに蓄積されたメッセージを処理できるようになりました。これにより、すべてのインスタンスが busy の時にメッセージがキューに蓄積され、インスタンスが idle に戻った時に自動的に処理される仕組みが完成しました。


