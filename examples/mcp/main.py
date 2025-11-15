#!/usr/bin/env python3
"""
MCP Filesystem Server テスト用スクリプト

Miclowプロトコルを使用してMCPサーバーと通信し、
ディレクトリ一覧を取得する例です。
"""
import json

import miclow


def main():
    print("MCP Filesystem Server テストを開始します...")
    print("=" * 50)

    # MCPサーバーのタスク名（config.tomlで定義したtask_name）
    mcp_task_name = "filesystem_mcp"

    # ディレクトリ一覧を取得
    print("\nディレクトリ一覧を取得します...")
    try:
        # list_directoryツールを呼び出す
        # dataはJSON形式で，{"name": "ツール名", "arguments": {...}} を指定
        result = miclow.call_function(mcp_task_name, json.dumps({
            "name": "list_directory",
            "arguments": {
                "path": "/home/coding/coto/miclow/examples/mcp"
            }
        }))
        # call_function()はTopicMessageを返すため、data属性に直接アクセス
        print(f"結果:\n{result.data}")
    except Exception as e:
        print(f"エラー: {e}")
        import traceback
        traceback.print_exc()

    print("\n" + "=" * 50)
    print("テスト完了")


if __name__ == "__main__":
    main()

