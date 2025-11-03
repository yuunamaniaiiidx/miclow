# Miclow

Miclowは、軽量オーケストレーションシステムとして設計されたRust製の非同期タスク実行フレームワークです。複数のプロセス間でstdin, stdoutを介したリアルタイムメッセージングとトピックベースの通信を提供し、効率的なタスク管理と実行を実現します。

## 主な特徴

### 🔄 軽量オーケストレーション
- 複数の外部プロセスを効率的に並行実行
- グレースフルシャットダウン機能

### 📡 トピックベース通信システム
- タスク間での動的なトピック購読・購読解除
- リアルタイムメッセージ配信
- 複数のタスクが同一トピックを購読可能

### 🎯 柔軟なプロセス実行
- 任意のコマンドと引数を実行可能
- カスタム作業ディレクトリと環境変数の設定
- 標準入出力の双方向通信

### ⚙️ 設定ファイルベース
- TOML形式の設定ファイル
- 初期トピック購読の設定

### 🛠️ システムコマンド
- `system.subscribe-topic`: 実行時にトピックを動的購読開始
- `system.unsubscribe-topic`: 実行時にトピックを動的購読解除

### 🔧 堅牢なエラーハンドリング
- プロセス失敗時の自動クリーンアップ
- 詳細なログ出力
- エラーイベントの適切な伝播

### 🚀 軽量・高性能設計
- Tokioベースの非同期ランタイム
- メモリ効率的なチャネル通信
- スケーラブルなアーキテクチャ
- 最小限のリソース消費で最大のパフォーマンス

## 🚀 クイックスタート

### インストール

```bash
# リポジトリをクローン
git clone https://github.com/sugipamo/miclow.git
cd miclow

# ビルド
cargo build --release

# 実行
./target/release/miclow --config config.toml
```

### 基本的な使用方法

1. **設定ファイルの作成**
```toml
# config.toml
[[tasks]]
task_name = "python-script"
command = "python3"
args = ["main.py"]
working_directory = "./"
environment_vars = { PYTHONUNBUFFERED = "1" }
subscribe_topics = ["key1", "key2"]

[[tasks]]
task_name = "stdout"
command = "python3"
args = ["stdout.py"]
working_directory = "./"
environment_vars = { PYTHONUNBUFFERED = "1" }
subscribe_topics = ["stdout"]
```

2. **Miclowの起動**
```bash
miclow --config config.toml
```

3. **タスク間通信**
```python
# worker.py の例
import sys

# トピックにメッセージを送信
print("system.subscribe-topic:new_topic")
print("task_queue:Hello from worker!")

# メッセージを受信
for line in sys.stdin:
    if line.startswith("task_queue:"):
        message = line.split(":", 1)[1]
        print(f"results:Processed: {message}")
```

## 📖 詳細な使用方法

### 設定ファイル形式

MiclowはTOML形式の設定ファイルを使用します：

```toml
# タスク定義
[[tasks]]
task_name = "example_task"
command = "実行するコマンド"
args = ["引数1", "引数2"]
working_directory = "/作業ディレクトリ"
environment = { VAR1 = "value1", VAR2 = "value2" }
topics = ["購読するトピック1", "トピック2"]
```

### システムコマンド

実行時にタスクから送信できる特殊コマンド：

#### トピック管理
- `system.subscribe-topic:トピック名` - 新しいトピックを購読開始
- `system.unsubscribe-topic:トピック名` - トピックの購読を解除

#### システム制御
- `system.status` - システムの状態を取得

#### 使用例
```python
# トピックの購読開始
print("system.subscribe-topic:new_topic")
```

### メッセージ形式

```
トピック名:メッセージ内容
```

例：
```
task_queue: value
results: completed
```

## 🔧 開発・貢献

### 開発環境のセットアップ

```bash
# 依存関係のインストール
cargo install

# テストの実行
cargo test

# ドキュメントの生成
cargo doc --open
```

### 貢献方法

1. このリポジトリをフォーク
2. フィーチャーブランチを作成 (`git checkout -b feature/amazing-feature`)
3. 変更をコミット (`git commit -m 'Add amazing feature'`)
4. ブランチにプッシュ (`git push origin feature/amazing-feature`)
5. プルリクエストを作成

## 📊 パフォーマンス

- **メモリ使用量**: 最小限のフットプリント（~10MB base）
- **同時実行**: 数百のタスクを効率的に管理
- **レイテンシ**: マイクロ秒レベルのメッセージ配信
- **スループット**: 高負荷環境での安定した処理

## 🐛 トラブルシューティング

### よくある問題

**Q: タスクが起動しない**
A: 設定ファイルの`command`と`args`を確認してください。パスが正しく設定されているか確認してください。

**Q: メッセージが届かない**
A: トピック名の大文字小文字を確認してください。Miclowは大文字小文字を区別します。

**Q: プロセスが残る**
A: `Ctrl+C`でグレースフルシャットダウンを実行してください。強制終了の場合は`pkill -f miclow`を使用してください。

## 📄 ライセンス

このプロジェクトはMITライセンスの下で公開されています。詳細は[LICENSE](LICENSE)ファイルを参照してください。

## 📞 サポート

- 問題報告: [GitHub Issues](https://github.com/sugipamo/miclow/issues)
- ディスカッション: [GitHub Discussions](https://github.com/sugipamo/miclow/discussions)