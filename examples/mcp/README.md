# MCP Filesystem Server テスト例

このディレクトリは、MiclowのMCPプロトコルバックエンドのテスト用です。

## 構成

- `config.toml` - Miclowの設定ファイル
- `main.py` - MCPサーバーと通信するPythonスクリプト
- `README.md` - このファイル

## Filesystem MCP Server

Filesystem MCP Serverは、ファイルシステム操作を提供するMCPサーバーです。

### 利用可能なツール

- `read_file` - ファイルを読み取る
- `write_file` - ファイルに書き込む
- `list_directory` - ディレクトリの内容を一覧表示
- `create_directory` - ディレクトリを作成
- `move_file` - ファイルを移動/リネーム
- `delete_file` - ファイルを削除

## 使用方法

1. Miclowを起動:
   ```bash
   cd /home/coding/coto
   cargo run --bin miclow -- examples/mcp/config.toml
   ```

2. `main.py`が自動的に実行され、MCPサーバーのツールを呼び出します。

## main.pyの動作

`main.py`はディレクトリ一覧を取得します。

## カスタマイズ

`main.py`を編集して、異なるMCPツールを呼び出すことができます。

MCPツールを呼び出すには、`miclow.call_function()`を使用します：

```python
result = miclow.call_function("filesystem_mcp", json.dumps({
    "name": "ツール名",
    "arguments": {
        "パラメータ名": "値"
    }
}))
```

注意: MCPプロトコルでは、ツール呼び出しは`FunctionMessage`として送信されます。
`task_name`がMCPサーバーの関数名（`filesystem_mcp`）で、`data`がJSON形式のツール名と引数を含む必要があります。

**重要**: MCPサーバーは`[[functions]]`セクションで定義する必要があります。
`[[tasks]]`として定義すると、`call_function`で呼び出すことができません。

