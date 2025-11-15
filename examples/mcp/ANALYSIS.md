# MCP初期化エラーの原因分析

## エラーメッセージ

```
Failed to initialize MCP server: Initialize error: [
  {
    "code": "invalid_type",
    "expected": "string",
    "received": "undefined",
    "path": ["params", "protocolVersion"],
    "message": "Required"
  },
  {
    "code": "invalid_type",
    "expected": "object",
    "received": "undefined",
    "path": ["params", "clientInfo"],
    "message": "Required"
  }
] (code: -32603)
```

## 原因

MCPサーバーは`protocolVersion`と`clientInfo`（キャメルケース）を期待していますが、Rustの構造体では`protocol_version`と`client_info`（スネークケース）を使用しています。

### 問題のコード

`miclow/src/mcp/types.rs`:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitializeParams {
    pub protocol_version: String,  // ← スネークケース
    pub capabilities: ClientCapabilities,
    pub client_info: ClientInfo,   // ← スネークケース
}
```

### 期待されるJSON形式

MCPプロトコルでは、JSON-RPCのパラメータはキャメルケースを使用する必要があります：

```json
{
  "protocolVersion": "2024-11-05",  // ← キャメルケース
  "capabilities": {...},
  "clientInfo": {                    // ← キャメルケース
    "name": "miclow",
    "version": "0.1.0"
  }
}
```

### 実際に送信されているJSON

現在の実装では、スネークケースのままシリアライゼーションされている可能性があります：

```json
{
  "protocol_version": "2024-11-05",  // ← スネークケース（間違い）
  "capabilities": {...},
  "client_info": {                    // ← スネークケース（間違い）
    "name": "miclow",
    "version": "0.1.0"
  }
}
```

または、フィールドが完全に欠落している可能性もあります。

## 解決方法

`serde`の`rename`属性を使用して、JSONシリアライゼーション時にキャメルケースに変換する必要があります：

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitializeParams {
    #[serde(rename = "protocolVersion")]
    pub protocol_version: String,
    pub capabilities: ClientCapabilities,
    #[serde(rename = "clientInfo")]
    pub client_info: ClientInfo,
}
```

同様に、`InitializeResult`やその他の構造体でも、必要に応じてキャメルケースへの変換が必要です。

## 確認すべきポイント

1. `InitializeParams`の`protocol_version`と`client_info`に`#[serde(rename = "...")]`属性を追加
2. `InitializeResult`の`protocol_version`と`server_info`にも同様の対応が必要か確認
3. その他のMCPプロトコルの構造体でも、キャメルケースが必要なフィールドを確認

## 参考

MCPプロトコルの仕様では、JSON-RPCメッセージのフィールド名はキャメルケースを使用します。これはJavaScript/TypeScriptの慣習に従っています。

## 変更範囲の確認

### 質問1: キャメルケースに変換したら問題なし？

**はい、問題ありません。** エラーメッセージが示すように、MCPサーバーは`protocolVersion`と`clientInfo`（キャメルケース）を期待しています。現在はスネークケースで送信されているため、`undefined`として認識されています。`#[serde(rename = "...")]`属性を追加することで、JSONシリアライゼーション時にキャメルケースに変換され、問題が解決します。

### 質問2: これはmcpのプロトコル内のみの変更で済む？

**はい、`miclow/src/mcp/types.rs`のみの変更で済みます。**

確認結果：
- MCPの型定義は`miclow/src/mcp/types.rs`にのみ存在
- これらの型は`miclow/src/mcp/client.rs`でのみ使用されている
- `mcp_protocol.rs`は`McpClient`のみを使用しており、型定義には直接依存していない
- 他のモジュール（`miclow_protocol.rs`、`interactive_protocol.rs`など）からは使用されていない

**変更が必要なファイル：**
- `miclow/src/mcp/types.rs`のみ

**変更が不要なファイル：**
- `miclow/src/mcp/client.rs` - 型の使用のみで、フィールド名は変更不要
- `miclow/src/mcp_protocol.rs` - 型定義に直接依存していない
- その他のモジュール - MCPの型定義を使用していない

**注意点：**
- `InitializeResult`もサーバーからのレスポンスをデシリアライゼーションする際にキャメルケースを期待する必要があります（`protocol_version`と`server_info`）
- ただし、これは`Deserialize`時に自動的に処理されるため、`#[serde(rename = "...")]`を追加すれば問題ありません

