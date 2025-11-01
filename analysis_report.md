# task.rs 依存関係分析レポート

## 概要

- **ファイルサイズ**: 2,227行
- **総ノード数**: 62個（構造体、列挙型、トレイト、関数など）
- **総エッジ数**: 119個（依存関係）
- **接続成分**: 1つ（すべてのノードが相互接続）

## 種類別の統計

| 種類 | 数 |
|------|-----|
| struct | 29個 |
| impl | 26個 |
| enum | 1個 |
| trait | 2個 |
| function | 3個 |
| type | 1個 |

## 主要な依存関係のハブ

### 高依存度（多くの型に依存している）

1. **`impl_TaskSpawner`** - 7個の依存
2. **`impl_TaskExecutor`** - 7個の依存
3. **`impl_CommandBackend`** - 7個の依存
4. **`SystemControlContext`** - 6個の依存
5. **`impl_MiclowSystem`** - 6個の依存

### 高利用度（多くの型から使われている）

1. **`TaskId`** - **18個から使用**（最重要の基盤型）
2. **`ExecutorEventSender`** - 10個から使用
3. **`ExecutorEvent`** - 10個から使用
4. **`SystemControlContext`** - 8個から使用
5. **`SystemControlManager`** - 8個から使用
6. **`TopicManager`** - 8個から使用
7. **`TaskExecutor`** - 7個から使用

## 機能グループ別の統計

- **SystemControl関連**: 20ノード
- **Executor関連**: 10ノード  
- **Task関連**: 17ノード

## ファイル分割の推奨構造

### モジュール1: `core` - 基盤型
**ファイル**: `task/core.rs` または `task/id.rs`, `task/events.rs`

- `TaskId` - 最も基本的な型（11個から参照）
- `ExecutorEvent` - イベント列挙型（8個から参照）
- `ExecutorEventSender` / `ExecutorEventReceiver`
- `ExecutorEventChannel`
- `TaskMessageData`

**理由**: これらは他の多くのモジュールから参照される基盤型です。

### モジュール2: `channel` - チャネル関連
**ファイル**: `task/channel.rs`

- `InputChannel`, `InputSender`, `InputReceiver`
- `ShutdownChannel`, `ShutdownSender`
- 関連するimplブロック

**理由**: チャネル関連の型は比較的独立しています。

### モジュール3: `backend` - バックエンド実装
**ファイル**: `task/backend.rs`

- `TaskBackend` (trait)
- `CommandBackend` (struct + impl)
- `TaskBackendHandle`
- `spawn_stream_reader` (function)

**理由**: バックエンド実装は比較的自己完結しています。

### モジュール4: `system_control` - システム制御
**ファイル**: `task/system_control.rs`

- `SystemControlHandler` (trait)
- `SystemControlManager`
- `SystemControlMessage`
- `SystemControlContext`
- 各種SystemControl構造体:
  - `SubscribeTopicSystemControl`
  - `UnsubscribeTopicSystemControl`
  - `StartTaskSystemControl`
  - `StopTaskSystemControl`
  - `AddTaskFromTomlSystemControl`
  - `StatusSystemControl`
  - `UnknownSystemControl`
- `system_control_command_to_handler` (function)
- `start_system_control_worker` (function)

**理由**: システム制御機能は一つのモジュールにまとめることができます。

### モジュール5: `executor` - タスク実行管理
**ファイル**: `task/executor.rs`

- `TaskExecutor`
- `RunningTask`
- 関連するimplブロック

**理由**: タスクの実行管理機能です。

### モジュール6: `spawner` - タスク生成
**ファイル**: `task/spawner.rs`

- `TaskSpawner`
- 関連するimplブロック

**理由**: タスクの生成と起動を担当します。

### モジュール7: `topic` - トピック管理
**ファイル**: `task/topic.rs`

- `TopicManager`
- 関連するimplブロック

**理由**: トピック管理機能は独立しています。

### モジュール8: `config` - 設定
**ファイル**: `task/config.rs`

- `TaskConfig`
- `SystemConfig`
- 関連するimplブロック

**理由**: 設定関連の型です。

### モジュール9: `system` - システム全体
**ファイル**: `task/system.rs`

- `MiclowSystem`
- `BackgroundTaskManager`
- 関連するimplブロック

**理由**: システム全体を統合する最上位モジュールです。

## 分割の優先順位

### フェーズ1: 基盤の分離（低リスク）
1. `core`モジュール - `TaskId`, `ExecutorEvent`など
2. `channel`モジュール - チャネル関連

### フェーズ2: 機能モジュールの分離
3. `system_control`モジュール
4. `backend`モジュール
5. `topic`モジュール
6. `config`モジュール

### フェーズ3: 統合モジュールの分離
7. `executor`モジュール
8. `spawner`モジュール
9. `system`モジュール

## 注意点

1. **`TaskId`は最重要**: **18個**の異なる型から参照されているため、最初に分離すべき基盤型です。
2. **循環依存の回避**: 
   - `SystemControlContext`は6個の型に依存し、8個の型から依存されているため、設計に注意が必要です。
   - すべてのノードが1つの接続成分に属しているため、慎重にモジュール境界を設計する必要があります。
3. **トレイトの配置**: `SystemControlHandler`と`TaskBackend`は、実装する型と同じモジュールに配置することを検討してください。
4. **高結合度**: すべてのノードが相互接続されているため、一度に大きな変更を行うのではなく、段階的に分離することを推奨します。

## 可視化

依存グラフを可視化するには:

```bash
cd tools
cargo run --release -- ../src/task.rs --output ../task_deps.dot
dot -Tpng ../task_deps.dot -o ../task_deps.png
```

Graphvizがインストールされていない場合は、オンラインのDOTビューアーを使用できます。

