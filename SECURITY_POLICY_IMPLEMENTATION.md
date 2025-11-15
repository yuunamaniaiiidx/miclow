# セキュリティポリシー設定機能 実装計画書

## 概要

miclowにsystem_controlの利用範囲を制限するセキュリティポリシー設定機能を追加します。config.tomlでタスクごとにsystem_controlアクションの許可/拒否を設定できるようにします。

## 現状分析

### system_controlの機能

現在、system_controlは以下のアクションを提供しています：

1. **SubscribeTopic** - トピックの購読
2. **UnsubscribeTopic** - トピックの購読解除
3. **Status** - システムステータスの取得（実行中のタスク一覧、トピック一覧）
4. **GetLatestMessage** - 指定トピックの最新メッセージ取得
5. **CallFunction** - 関数の呼び出し（他のタスク/関数の起動）

### 現在の実装箇所

- `src/system_control/action.rs`: SystemControlActionの定義と実行ロジック
- `src/system_control/worker.rs`: system_controlワーカーの実装
- `src/system_control/queue.rs`: system_controlメッセージキュー
- `src/config/mod.rs`: 設定ファイルのパース処理
- `src/miclow.rs`: system_controlの呼び出し箇所（136-152行目）

## 実装手順

### フェーズ1: 設定構造の定義

#### 1.1 SystemConfigにセキュリティポリシー設定を追加

**ファイル**: `src/config/mod.rs`

**変更内容**:
- `SystemConfig`構造体に`security_policy`フィールドを追加
- `SecurityPolicy`構造体を新規作成
- タスクごとの権限設定を保持する`TaskSecurityPolicy`構造体を定義

**設定例**:
```toml
[security_policy]
default_allow_all = false  # デフォルトで全アクションを許可するか

[security_policy.tasks."task_name"]
allow_subscribe_topic = true
allow_unsubscribe_topic = true
allow_status = false
allow_get_latest_message = true
allow_call_function = false

# 特定のトピックのみ許可
[security_policy.tasks."task_name".allowed_topics]
subscribe = ["topic1", "topic2"]
get_latest_message = ["topic1"]

# 特定の関数のみ許可
[security_policy.tasks."task_name".allowed_functions]
call = ["function1", "function2"]
```

**実装コスト**: 中（2-3時間）
- 構造体定義: 30分
- TOMLパース処理: 1時間
- バリデーション: 1時間
- テスト: 30分

### フェーズ2: 権限チェック機能の実装

#### 2.1 権限チェックモジュールの作成

**ファイル**: `src/system_control/permission.rs` (新規)

**機能**:
- タスク名から権限設定を取得
- 各アクションタイプに対する権限チェック
- トピック名や関数名の詳細な権限チェック

**実装コスト**: 中（2-3時間）
- 権限チェックロジック: 1.5時間
- エラーハンドリング: 30分
- テスト: 1時間

#### 2.2 SystemControlActionの実行前に権限チェックを追加

**ファイル**: `src/system_control/action.rs`

**変更内容**:
- `execute`メソッドの最初に権限チェックを追加
- 権限がない場合はエラーレスポンスを返す

**実装コスト**: 小（1時間）
- 権限チェックの呼び出し: 30分
- エラーレスポンス処理: 30分

### フェーズ3: 設定ファイルの統合

#### 3.1 config.tomlのパース処理を更新

**ファイル**: `src/config/mod.rs`

**変更内容**:
- `RawSystemConfig`に`security_policy`フィールドを追加
- `SystemConfig::from_toml_internal`でセキュリティポリシーを読み込み
- デフォルト値の設定処理を追加

**実装コスト**: 小（1-2時間）
- パース処理: 1時間
- デフォルト値処理: 30分
- テスト: 30分

#### 3.2 SystemConfigをSystemControlActionに渡す

**ファイル**: `src/system_control/action.rs`, `src/system_control/worker.rs`

**変更内容**:
- `execute`メソッドは既に`system_config`を受け取っているため、権限チェックで使用可能
- 権限チェック関数に`SystemConfig`を渡す

**実装コスト**: 小（30分）

### フェーズ4: エラーハンドリングとログ記録

#### 4.1 権限エラーの適切な処理

**ファイル**: `src/system_control/action.rs`

**変更内容**:
- 権限エラー時に適切なエラーメッセージを返す
- ログに権限違反を記録

**実装コスト**: 小（1時間）
- エラーメッセージ: 30分
- ログ記録: 30分

## 実装コスト総計

| フェーズ | 作業内容 | 工数 |
|---------|---------|------|
| フェーズ1 | 設定構造の定義 | 2-3時間 |
| フェーズ2 | 権限チェック機能 | 3-4時間 |
| フェーズ3 | 設定ファイル統合 | 1.5-2.5時間 |
| フェーズ4 | エラーハンドリング | 1時間 |
| **合計** | | **7.5-10.5時間** |

## 実装の詳細設計

### データ構造

```rust
#[derive(Debug, Clone)]
pub struct SecurityPolicy {
    pub default_allow_all: bool,
    pub tasks: HashMap<String, TaskSecurityPolicy>,
}

#[derive(Debug, Clone)]
pub struct TaskSecurityPolicy {
    pub allow_subscribe_topic: Option<bool>,
    pub allow_unsubscribe_topic: Option<bool>,
    pub allow_status: Option<bool>,
    pub allow_get_latest_message: Option<bool>,
    pub allow_call_function: Option<bool>,
    pub allowed_topics: Option<TopicPermissions>,
    pub allowed_functions: Option<FunctionPermissions>,
}

#[derive(Debug, Clone)]
pub struct TopicPermissions {
    pub subscribe: Option<Vec<String>>,
    pub get_latest_message: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct FunctionPermissions {
    pub call: Option<Vec<String>>,
}
```

### 権限チェックのロジック

1. タスク名から`TaskSecurityPolicy`を取得
2. 該当するアクションタイプの権限を確認
3. 詳細な権限設定（トピック名、関数名）がある場合は、それも確認
4. デフォルトポリシー（`default_allow_all`）をフォールバックとして使用

### 設定例

```toml
[security_policy]
default_allow_all = false

[security_policy.tasks."main"]
allow_subscribe_topic = true
allow_unsubscribe_topic = true
allow_status = true
allow_get_latest_message = true
allow_call_function = true

[security_policy.tasks."chat"]
allow_subscribe_topic = true
allow_unsubscribe_topic = true
allow_status = false
allow_get_latest_message = true
allow_call_function = true
allowed_functions = { call = ["gpt"] }

[security_policy.tasks."receive"]
allow_subscribe_topic = true
allow_unsubscribe_topic = false
allow_status = false
allow_get_latest_message = false
allow_call_function = false
```

## その他のセキュリティ改善提案

### 1. トピック名の検証強化

**現状**: トピック名に空白文字が含まれていないかのチェックのみ

**改善案**:
- トピック名の正規表現パターン検証
- 予約語（`system.*`など）の使用制限
- トピック名の長さ制限

**実装コスト**: 小（1時間）

### 2. 関数呼び出しのリソース制限

**改善案**:
- 同時実行数の制限
- 実行時間の制限
- メモリ使用量の監視

**実装コスト**: 大（4-6時間）

### 3. 監査ログの強化

**改善案**:
- すべてのsystem_controlアクションをログに記録
- 権限違反の試行を特別に記録
- ログの永続化（ファイル出力）

**実装コスト**: 中（2-3時間）

### 4. タスク間の通信制限

**改善案**:
- タスクごとに許可されたトピックの送信先を制限
- タスクごとに許可されたトピックの受信元を制限

**実装コスト**: 大（5-7時間）

### 5. 設定ファイルの検証強化

**改善案**:
- 起動時の設定ファイル整合性チェック
- 循環参照の検出（関数呼び出しの循環依存など）
- 存在しないタスク/関数への参照の検出

**実装コスト**: 中（2-3時間）

### 6. 認証・認可の追加（将来の拡張）

**改善案**:
- タスクごとの認証トークン
- 実行ユーザーの制限
- ネットワークアクセスの制限

**実装コスト**: 大（8-12時間）

## 推奨実装順序

1. **必須**: フェーズ1-4の実装（セキュリティポリシー基本機能）
2. **推奨**: トピック名の検証強化
3. **推奨**: 監査ログの強化
4. **任意**: 設定ファイルの検証強化
5. **将来**: リソース制限、通信制限、認証・認可

## 注意事項

1. **後方互換性**: 既存のconfig.tomlファイルで`security_policy`が未指定の場合、デフォルトで全アクションを許可する（`default_allow_all = true`）ことで、既存の動作を維持する
2. **パフォーマンス**: 権限チェックは毎回のsystem_controlアクション実行時に発生するため、オーバーヘッドを最小限に抑える必要がある
3. **テスト**: 各アクションタイプに対する権限チェックのテストケースを十分に作成する

## 参考実装箇所

- 設定パース: `src/config/mod.rs` (250-305行目)
- アクション実行: `src/system_control/action.rs` (25-242行目)
- アクション呼び出し: `src/miclow.rs` (136-152行目)

