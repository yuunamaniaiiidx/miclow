# Task Lifecycle 実装計画（最新版）

**最終更新**: 2025-01-XX  
**ベースドキュメント**: `task_lifecycle_requirements.md`

## 概要

本ドキュメントは、Task Lifecycle 要件の実装状況を整理し、残りの実装タスクと計画を明確化したものです。

**重要**: 本実装は破壊的変更を許可しており、後方互換性は考慮しません。既存の API や動作の変更を許容します。

---

## 最終的に目指す姿

### システム全体の動作

1. **常駐 worker 模式**
   - 各タスクはシステム起動時、またはライフサイクル管理ワーカーによりあらかじめ起動される
   - ランタイム途中での追加インスタンス起動はライフサイクル管理ワーカー経由のみ
   - タスクは常駐し、複数のメッセージを順次処理できる

2. **Round Robin 配信**
   - 同一タスク名の複数インスタンス間で、トピックメッセージを公平に配分
   - 処理中（busy）のインスタンスはスキップし、idle なインスタンスに割り当て
   - すべてのインスタンスが busy の場合は、メッセージをキューに蓄積
   - タスクが TopicResponse を返して idle に戻ったら、キューに蓄積されたメッセージを処理
   - **重要**: Round Robin は同一タスク名の複数インスタンス間でのみ適用される
   - 異なるタスク名間ではブロードキャスト（各タスク名に1つずつ配信）が維持される
   - 例: トピック "demo.topic" を購読しているタスクA（3インスタンス）、タスクB（2インスタンス）がある場合、タスクAの3インスタンス間でRound Robin、タスクBの2インスタンス間でRound Robin、そしてタスクAとタスクBの両方にメッセージが配信される

3. **ライフサイクル管理**
   - タスクごとの希望インスタンス数（`desired_instances`）を監視
   - 不足時に自動的にインスタンスを起動・補充
   - タスク終了時は即座に再起動（システム停止時を除く）
   - システム停止時はすべてのタスクをグレースフルに停止

4. **TopicResponse による双方向通信**
   - すべてのタスクは処理完了時に `TopicResponse` を返す
   - `TopicResponse` は `{original_topic}.result` トピックに送信される
   - TopicResponse 受信時にタスクを idle に戻し、次のメッセージ処理可能にする

5. **トピックベースの通信**
   - 手動操作 API（subscribe/unsubscribe/status など）は撤廃
   - すべての通信はトピックベースの双方向チャネルで実現
   - タスクは設定の `subscribe_topics` で指定されたトピックを購読し、メッセージを受信
   - Round Robin 配信は、タスクが購読している `subscribe_topics` に基づいて動作する

### 設定例

```toml
[[tasks]]
protocol = "MiclowStdIO"
task_name = "worker"
command = "uv"
args = ["run", "python3", "worker.py"]
subscribe_topics = ["work.queue", "work.priority"]

  [[tasks.lifecycle]]
  desired_instances = 3
  mode = "round_robin"
```

この設定により、`worker` タスクは3つのインスタンスが常駐し、`subscribe_topics` で指定されたトピック（"work.queue" と "work.priority"）のメッセージが、3つのインスタンス間で Round Robin 方式で配信される。

---

## 実装状況サマリー

### ✅ 実装完了項目

1. **Config パーサ**
   - `[[tasks.lifecycle]]` ブロックのサポート
   - `desired_instances` と `mode = "round_robin"` のパース
   - `ProtocolBackend` は Interactive / MiclowStdIO に限定
   - `function_to_task` や `[[tasks.function]]` は完全に撤廃済み

2. **SystemControl 系の削除**
   - `SystemControlAction` / `SystemControlWorker` / `SystemControlQueue` / `SystemResponseEvent` / `SystemResponseChannel` をすべて削除
   - subscribe/unsubscribe/status/get-latest などの手動操作 API は撤廃
   - `ExecutorInputEvent::SystemResponse` と `ExecutorOutputEvent::SystemControl` を削除

3. **タスク状態管理**
   - `task_runtime/task_state.rs`: idle/busy 状態管理
   - Round Robin 方式での idle インスタンス選択ロジック
   - タスク名ごとのインスタンス管理

4. **Round Robin ディスパッチャー（基盤）**
   - `task_runtime/round_robin_dispatcher.rs`: Round Robin 配信ロジック
   - トピックキューイング機能（全インスタンス busy 時の蓄積）
   - `process_queue` メソッドでキュー処理

5. **TopicResponse 処理**
   - `task_runtime/spawner.rs`: TopicResponse 受信時の idle 復帰ロジック（142行目）
   - `.result` トピックへのルーティング
   - Backend 側での `.result` トピックの自動変換

6. **ライフサイクル管理ワーカー**
   - `task_runtime/lifecycle_manager.rs`: `desired_instances` 監視
   - 不足時の自動起動
   - タスク終了時の即座再起動
   - システム停止時の再起動抑止

7. **タスク終了通知**
   - `channels/task_exit_channel.rs`: `TaskExitChannel` / `TaskExitSender` / `TaskExitReceiver`
   - `TaskExecutor` にタスク終了通知機能を追加

8. **システム起動時の初期インスタンス起動**
   - `MiclowSystem::start_user_tasks`: `desired_instances` に基づく初期起動

9. **Backend 実装**
   - `miclowstdio`: `.result` で終わるトピックを `TopicResponse` に自動変換
   - `interactive`: トピック受信をしないため、通常の `ExecutorOutputEvent::Topic` のみ

10. **Python/クライアント**
    - `call_function` / `return_value` API を廃止
    - `miclow.wait_for_topic` + `miclow.send_message("{topic}.result", ...)` による双方向通信
    - ヘルパー関数 `send_response()` と `return_topic_for()` を追加

---

## ❌ 未実装・統合が必要な項目

### 🔴 優先度: 高（必須）

#### 1. Round Robin ディスパッチャーと TopicBroker の統合

**現状の問題**:
- `RoundRobinDispatcher` は実装されているが、システム内でインスタンス化されていない
- `TopicBroker::broadcast_message` は全サブスクライバーに配信（全員に送信）
- Round Robin 方式での配信が機能していない

**必要な実装**:

1. **RoundRobinDispatcher の統合**
   - システム内で `RoundRobinDispatcher` をインスタンス化し、適切なコンポーネントに注入する
   - `TaskExecutor` への参照を保持して、タスク状態を参照できるようにする

2. **TopicBroker の拡張**
   - `TopicBroker` に Round Robin 配信機能を統合する
   - トピックメッセージ配信時に、タスクの lifecycle 設定に応じて配信方式を切り替える
   - Round Robin モードのタスクには Round Robin 方式で配信し、それ以外は既存の方式を使用する

3. **トピック→タスク名のマッピング**
   - トピック名からタスク名を特定する仕組みを実装する
   - このマッピングは Round Robin 配信時に必要となる

#### 2. キュー処理のトリガー

**現状の問題**:
- `RoundRobinDispatcher::process_queue` は実装されているが、呼び出されていない
- タスクが idle に戻った時にキューに蓄積されたメッセージが処理されない

**必要な実装**:

1. **タスク状態遷移時のキュー処理**
   - タスクが idle 状態に戻った時に、そのタスク名に対応するキューに蓄積されたメッセージを処理する
   - TopicResponse 受信時やタスク登録時など、適切なタイミングでキュー処理をトリガーする

#### 3. トピック→タスク名のマッピング機能

**現状の問題**:
- `TopicBroker` には `task_subscriptions: HashMap<(String, TaskId), ...>` があり、トピック名とタスクIDのペアで管理されている
- タスクIDからタスク名を取得するには `TaskExecutor` への参照が必要
- Round Robin 配信時に、トピックを購読しているタスク名をグループ化する必要がある

**必要な実装**:

1. **TopicBroker と TaskExecutor の統合**
   - `TopicBroker::broadcast_message` で、トピックを購読しているタスクIDからタスク名を取得
   - タスク名ごとにグループ化し、各タスク名について Round Robin モードかどうかを判定

2. **配信ロジックの実装**
   - Round Robin モードのタスク名: そのタスク名のインスタンス間で Round Robin で1つに配信
   - 通常モードのタスク名: そのタスク名の全インスタンスに配信
   - 異なるタスク名間: 各タスク名に1つずつ配信（ブロードキャスト）

---

### 🟡 優先度: 中（推奨）

#### 4. lifecycle 設定の必須化

**現状**:
- `lifecycle` 設定はオプション（デフォルト値あり）
- Round Robin モードを使用するには明示的な設定が必要

**検討事項**:
- すべてのタスクに `lifecycle` 設定を必須にするか
- デフォルトで `desired_instances = 1, mode = "round_robin"` を適用するか

---

### 🟢 優先度: 低（将来）

#### 5. エラーハンドリングの強化

- Round Robin 配信失敗時のリトライ
- キュー処理失敗時のログ出力と監視

#### 6. パフォーマンス最適化

- キュー処理のバッチ化
- マッピングのキャッシュ最適化

---

## 実装計画

### Phase 1: Round Robin ディスパッチャーの統合（最優先）

**目標**: Round Robin 方式でのメッセージ配信を機能させる

**タスク**:
1. RoundRobinDispatcher をシステムに統合
2. TopicBroker に Round Robin 配信機能を統合
3. トピック→タスク名のマッピング機能を実装
4. 統合テストで動作確認

**見積もり**: 2-3日

### Phase 2: キュー処理のトリガー

**目標**: タスクが idle に戻った時にキューに蓄積されたメッセージを処理

**タスク**:
1. タスク状態遷移時にキュー処理をトリガーする仕組みを実装
2. 統合テストで動作確認

**見積もり**: 1日

### Phase 3: テストとドキュメント

**目標**: 実装の動作確認とドキュメント更新

**タスク**:
1. Round Robin 配信の統合テスト
2. キュー処理の統合テスト
3. エンドツーエンドテスト
4. ドキュメント更新

**見積もり**: 1-2日

---

## 技術的な詳細

### Round Robin 配信の動作フロー

```
1. トピックメッセージが TopicBroker に到着
2. TopicBroker::broadcast_message が呼び出される
3. トピックを購読しているすべてのタスクIDを取得
4. タスクIDからタスク名を取得し、タスク名ごとにグループ化
5. 各タスク名について:
   a. タスクの lifecycle 設定を確認
   b. mode = "round_robin" の場合:
      - RoundRobinDispatcher::dispatch_message を呼び出す
      - idle なインスタンスを選択
      - メッセージを送信し、タスクを busy に設定
      - すべて busy の場合はキューに追加
   c. 通常モードの場合:
      - そのタスク名の全インスタンスに配信
6. .result トピックの場合:
   a. 既存の broadcast_message を使用（全サブスクライバーに配信）
```

### キュー処理の動作フロー

```
1. タスクが TopicResponse を返す
2. spawner.rs で TopicResponse を受信
3. タスクを idle に戻す
4. RoundRobinDispatcher::process_queue(task_name) を呼び出す
5. タスク名から subscribe_topics を取得
6. 各トピックについて、キューにメッセージがある場合:
   a. idle なインスタンスを選択
   b. メッセージを送信し、タスクを busy に設定
   c. キューが空になるか、すべて busy になるまで繰り返し
```

### トピック→タスク名のマッピング

- `TopicBroker` の `task_subscriptions: HashMap<(String, TaskId), ...>` から、トピック名とタスクIDのペアを取得
- `TaskExecutor` の `id_to_name: HashMap<TaskId, String>` から、タスクIDからタスク名を取得
- これにより、トピックを購読しているタスク名を特定し、タスク名ごとにグループ化できる

---

## 注意事項

1. **破壊的変更の許可**: 本実装は破壊的変更を許可しており、後方互換性は考慮しません。既存の API や動作の変更を許容します。

2. **パフォーマンス**: Round Robin 配信は追加のオーバーヘッドがあるため、必要な場合のみ使用することを推奨します。

3. **エラーハンドリング**: Round Robin 配信失敗時はキューに追加し、後でリトライする仕組みを実装することを推奨します。

4. **テスト**: Round Robin 配信の動作を確認する統合テストが必要です。

---

## 関連ファイル

実装に関連する主要なファイルは以下の通りです。具体的な実装方法は各ファイルの構造に応じて決定してください。

- Round Robin ディスパッチャー関連
- TopicBroker 関連
- タスク状態管理関連
- ライフサイクル管理関連
- 設定パース関連

---

## 更新履歴

- 2025-01-XX: 初版作成（実装状況の整理と計画の明確化）

