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

    targets = [
        ("filesystem_mcp", "StdIO backend (child process)"),
        ("filesystem_mcp_tcp", "TCP backend (remote server)"),
    ]

    for task_name, label in targets:
        print(f"\n[{label}] ディレクトリ一覧を取得します ({task_name}) ...")
        try:
            result = miclow.call_function(task_name, json.dumps({
                "name": "list_directory",
                "arguments": {
                    "path": "/home/coding/coto/miclow/examples/mcp"
                }
            }))
            print(f"結果:\n{result.data}")
        except Exception as e:
            print(f"{label} との通信でエラーが発生しました: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "=" * 50)
    print("テスト完了")


if __name__ == "__main__":
    main()
