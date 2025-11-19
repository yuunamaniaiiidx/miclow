# Task Lifecycle / Worker Requirements (WIP)

## 背景と方針
- 既存仕様では `task` と `function` が別経路で実装されており、`function` 呼び出しはタスクを都度新規起動することで実現している。
- 今後は両者を統合し、「タスク＝常駐 worker」として扱う破壊的変更を許容する。
- `function` 呼び出し API は撤廃し、タスクは必ず `TopicResponse` を返すことを前提とした双方向チャネルを利用する。
- タスクの起動/再起動はすべてライフサイクル管理ワーカーが担当し、手動スポーンや既存の手動制御 API は一旦廃止する。
- 互換性は切り捨てる方針で、既存 config/migration の提供は行わない。
- タイムアウトやレスポンス欠落時の自動復旧は当面考慮しない。

## 合意済み要件（ドラフト）
1. **常駐 worker 模式**  
   - 各タスクはシステム起動時、またはライフサイクル管理ワーカーによりあらかじめ起動される。  
   - ランタイム途中での追加インスタンス起動はライフサイクル管理ワーカー経由のみ。

2. **レスポンス必須 & TopicResponse**  
   - すべてのタスクは処理の完了・エラーを示す `TopicResponse` を1回以上返す義務がある。  
   - `TopicResponse` フィールド（必要に応じて拡張可）:  
     - `message_id`（必要なら付与）  
     - `from_task_id`（必要なら付与）  
     - `to_task_id`（必要なら付与）  
     - `status`（成功/エラーコードを内包）  
     - `topic`（処理対象のオリジナル topic）  
     - `return_topic`（常に `{original_topic}.result` に設定）  
     - `data`（任意ペイロード）  
   - Backend は `return_topic` をカスタマイズできず、`{original_topic}.result` への送信が必須。  
   - 既存の `SystemControlAction::CallFunction` やその他の手動制御 API はすべて廃止する。

3. **ライフサイクル設定（タスク単位）**  
   - 各 `[[tasks]]` セクション内に `[[tasks.lifecycle]]`（英語キー）ブロックを追加し、`desired_instances` や `mode = "round_robin"` を指定する。  
   - 単一エントリでよく、タスクごとに独立した設定ができる。  
   - Round Robin モードの場合、同一タスク名のインスタンス間でリクエスト/トピックメッセージを公平に配分し、処理中インスタンスは飛ばす。  
   - busy→idle の状態遷移はタスクからの `TopicResponse` 受信のみで判断し、追加シグナルは導入しない（懸念がある場合は別途検討）。  
   - それ以外の英語キーは現時点では定義せず、将来必要になったタイミングで追加する。

4. **タスク/関数統合**  
   - `function` 名はタスク名に内包され、追加の `[[tasks.function]]` 定義は廃止。  
   - 既存の `function_to_task` マッピングは除去または新モデル向けに再設計する。

5. **API表現力向上**  
   - 常駐タスクが複数種類のメッセージを処理できるよう、サブスクリプション/レスポンスのメタデータを拡張する。  
   - 破壊的変更を許容するため、既存クライアントが壊れることは問題ない。

6. **互換性/タイムアウト方針**  
   - 旧仕様との互換性は維持しない（変換ツールや互換レイヤは提供しない）。  
   - レスポンスタイムアウトやハング検出は今回スコープ外とし、必要になれば別途検討する。

## 想定する設定例
```toml
[[tasks]]
protocol = "MiclowStdIO"
task_name = "worker"
command = "uv"
args = ["run", "python3", "worker.py"]

  [[tasks.lifecycle]]
  desired_instances = 3
  mode = "round_robin"
```

## ライフサイクル管理ワーカーの責務
- タスクごとの希望インスタンス数を監視し、不足時に再起動・補充する。
- タスク終了イベントを購読し、異常終了時は即座に再起動する（ポリシー次第）。
- トピック購読の Round Robin 状態を維持し、処理中ステータスのインスタンスはスキップして idle なインスタンスに割り当てる。
- タスクが `TopicResponse`（`{topic}.result`）を返したらステータスを idle に戻し、待機列へ復帰させる。
- すべてのインスタンスが busy の場合は `topic_queue` にイベントを蓄積し、空きが出次第 FIFO で投入する（バックプレッシャやドロップは行わない）。
- システム停止時はすべてのタスクをグレースフルに停止させ、再起動を抑止する。

## 破壊的変更で影響を受けるコンポーネント
- `SystemControlAction::CallFunction` と `function_to_task` マッピング（完全撤廃）。
- `SystemControlAction` / `SystemControlWorker` / `SystemControlQueue` 全体を一旦削除し、新仕様に合わせて再設計する。
- `TaskExecutor::start_task_from_config`（ライフサイクルワーカーのみが使用）。
- Config パーサ（`miclow/src/config/mod.rs`）と CLI/ドキュメント。
- Topic 伝搬：`TopicResponse` を `{topic}.result` へ配信するルーティング、および Round Robin 配信ロジック。
- 既存の手動操作 API（status, subscribe/unsubscribe, call-function など）は一旦削除し、新仕様に合わせて再設計する。

## 未確定事項・質問
現時点で追加の未確定事項はなし。必要に応じて新たな論点が出た際に本セクションを更新する。

---
WIP ドキュメントにつき、要件の追加・変更があれば本ファイルを更新してください。

## 実装状況メモ（2025-11-20 更新）
- Config パーサ: `[[tasks.lifecycle]]` を必須化し、`ProtocolBackend` は Interactive / MiclowStdIO に限定。`function_to_task` や `[[tasks.function]]` は完全に撤廃済み。
- SystemControl 系: CallFunction / 任意コマンドは削除し、subscribe／unsubscribe／status／get-latest のみをライフサイクル管理ワーカー経由で受け付ける構成に整理。
- TaskRuntime: `StartContext` の親呼び出し概念と `ExecutorInputEvent::Function*` を除去し、常駐タスク＋ `{topic}.result` 返信のみで通信する前提に統一。
- Python/クライアント: `call_function` / `return_value` API を廃止し、`miclow.wait_for_topic` + `miclow.send_message("{topic}.result", ...)` による双方向通信へ移行。`examples/basic/` に sender/receiver の最小構成を追加済み。
- MCP backend や `rmcp` 依存は一旦削除。今後必要になった場合は新ライフサイクル仕様に合わせて別途再設計する。

