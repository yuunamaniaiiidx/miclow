# MCP実装の残タスクと改善点

## 概要

現在の実装は基本的な動作は確認できていますが、以下の改善点と確認事項があります。

## 優先度: 高

### 1. `is_error`フラグのチェック

**問題:**
`CallToolResult`の`is_error`フラグをチェックしていないため、MCPサーバーがエラーを返しても正常な結果として扱われる可能性があります。

**現在の実装:**
```rust
// miclow/src/mcp_protocol.rs:170-177
let result_text = result.content.as_ref()
    .and_then(|c| c.first())
    .map(|c| c.text.clone())
    .unwrap_or_else(|| result_json.clone());

// system.returnとして送信
let _ = event_tx_input.send(ExecutorEvent::new_return_message(result_text));
```

**修正案:**
```rust
// is_errorフラグをチェック
if result.is_error.unwrap_or(false) {
    let _ = event_tx_input.send_error(format!("MCP tool returned error: {}", result_text));
    return;
}

// 正常な結果を返す
let _ = event_tx_input.send(ExecutorEvent::new_return_message(result_text));
```

**影響範囲:**
- `miclow/src/mcp_protocol.rs` の `spawn_mcp_protocol` 関数

### 2. 複数の`Content`の処理

**問題:**
現在は`content`の最初の要素のみを取得していますが、複数の`Content`がある場合、すべてを結合する方が適切です。

**現在の実装:**
```rust
let result_text = result.content.as_ref()
    .and_then(|c| c.first())
    .map(|c| c.text.clone())
    .unwrap_or_else(|| result_json.clone());
```

**修正案:**
```rust
let result_text = if let Some(contents) = &result.content {
    if contents.is_empty() {
        result_json.clone()
    } else {
        // すべてのContentを結合
        contents.iter()
            .map(|c| c.text.clone())
            .collect::<Vec<_>>()
            .join("\n")
    }
} else {
    result_json.clone()
};
```

**影響範囲:**
- `miclow/src/mcp_protocol.rs` の `spawn_mcp_protocol` 関数

## 優先度: 中

### 3. タイムアウトしたリクエストのクリーンアップ

**問題:**
タイムアウト時に`pending_requests`から削除する処理がないため、メモリリークの可能性があります。

**現在の実装:**
```rust
// miclow/src/mcp/client.rs:93-101
match timeout(Duration::from_secs(30), rx).await {
    Ok(Ok(response)) => Ok(response),
    Ok(Err(_)) => Err(anyhow::anyhow!("Response channel closed")),
    Err(_) => {
        // タイムアウト
        Err(anyhow::anyhow!("Request timeout"))
    }
}
```

**修正案:**
```rust
match timeout(Duration::from_secs(30), rx).await {
    Ok(Ok(response)) => Ok(response),
    Ok(Err(_)) => {
        // チャネルが閉じられた場合、pending_requestsから削除
        self.id_manager.pending_requests.lock().unwrap().remove(&id);
        Err(anyhow::anyhow!("Response channel closed"))
    }
    Err(_) => {
        // タイムアウト時もpending_requestsから削除
        self.id_manager.pending_requests.lock().unwrap().remove(&id);
        Err(anyhow::anyhow!("Request timeout"))
    }
}
```

**影響範囲:**
- `miclow/src/mcp/client.rs` の `send_request` メソッド
- `miclow/src/mcp/jsonrpc.rs` に `remove_request` メソッドを追加する必要がある可能性

### 4. メッセージ処理タスクのライフサイクル管理

**問題:**
`spawn`したメッセージ処理タスクが、クライアントのライフサイクルと正しく同期しているか確認が必要です。

**確認事項:**
- クライアントがドロップされたときに、メッセージ処理タスクも正しく終了するか
- エラー時にタスクが適切にクリーンアップされるか

**影響範囲:**
- `miclow/src/mcp/client.rs` の `new` メソッド
- `miclow/src/mcp_protocol.rs` の `spawn_mcp_protocol` 関数

## 優先度: 低

### 5. より詳細なログ出力

**提案:**
デバッグ用に、送受信したJSON-RPCメッセージをログに出力するオプションを追加。

**実装案:**
- 環境変数や設定でログレベルを制御
- JSON-RPCメッセージの送受信を`log::debug!`で出力

### 6. リクエストIDのオーバーフロー対策

**問題:**
リクエストIDが`u64`で、理論的にはオーバーフローの可能性があります（実用的には問題ないはず）。

**対策:**
- オーバーフロー時にリセットする処理を追加
- または、循環バッファを使用

## テストケース

### エラーケース

1. **存在しないツールを呼び出す**
   ```python
   result = miclow.call_function("filesystem_mcp", json.dumps({
       "name": "nonexistent_tool",
       "arguments": {}
   }))
   ```

2. **不正な引数でツールを呼び出す**
   ```python
   result = miclow.call_function("filesystem_mcp", json.dumps({
       "name": "read_file",
       "arguments": {"path": "/nonexistent/path"}
   }))
   ```

3. **MCPサーバーがクラッシュした場合**
   - プロセスが異常終了した場合の処理

### エッジケース

1. **空のレスポンス**
   - `content`が`None`または空の配列の場合

2. **非常に大きなレスポンス**
   - メモリ使用量の確認

3. **複数の`Content`を含むレスポンス**
   - すべての`Content`が正しく処理されるか

### 並行実行

1. **複数のツール呼び出しを同時に行う**
   ```python
   import asyncio
   # 複数のcall_functionを並行実行
   ```

2. **タイムアウトが発生した場合の動作**
   - タイムアウト後に他のリクエストが正常に処理されるか

### リソース管理

1. **長時間実行されるツール**
   - タイムアウトの動作確認

2. **メモリリークの確認**
   - 長時間実行時のメモリ使用量

## 実装の確認事項

### 現在の実装で動作している理由

1. **既存のMiclowプロトコルとの統合がスムーズ**
   - `TaskBackend`トレイトが既に定義されている
   - `FunctionMessage`の仕組みが既にある

2. **MCPプロトコルがシンプル**
   - JSON-RPC 2.0ベースで標準的
   - stdio経由の通信で、MiclowStdIOと似たパターン

3. **基本的なエラーハンドリングが実装済み**
   - タイムアウト処理（30秒）
   - エラーレスポンスの処理
   - プロセス管理

### 潜在的な問題

1. **エラーハンドリングの不足**
   - `is_error`フラグをチェックしていない
   - 複数の`Content`の処理が不完全

2. **リソース管理**
   - タイムアウトしたリクエストのクリーンアップ
   - メッセージ処理タスクのライフサイクル

3. **デバッグの難しさ**
   - 詳細なログがない
   - JSON-RPCメッセージのトレースが困難

## 次のステップ

1. 優先度: 高の項目から順に実装
2. テストケースを追加して動作確認
3. エラーハンドリングの強化
4. ログ出力の改善

