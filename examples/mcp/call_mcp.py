#!/usr/bin/env python3
"""
Call MCP server example
This script queries the MCP filesystem server using file operations.
"""
import json
import sys
import uuid
import time


def send_mcp_request(tool_name, arguments):
    """Send a request to MCP server via miclow topic"""
    # Send request to MCP tool topic
    request = {
        "jsonrpc": "2.0",
        "id": str(uuid.uuid4()),
        "method": "tools/call",
        "params": {
            "name": tool_name,
            "arguments": arguments
        }
    }

    # Send to tool topic (MCP server will pick this up)
    print(f'"{tool_name}":', json.dumps(request))
    sys.stdout.flush()

    # Wait for response from return.{tool_name} topic
    data = ""
    # system.return_awaitには元のトピック名を指定（return.プレフィックスなし）
    while data == "":
        print(f'"system.return_await": {tool_name}')
        sys.stdout.flush()
        line_count = int(input())
        if line_count > 0:
            data = "".join(input() for _ in range(line_count))

    return data


def main():
    # Send request to read_file tool
    result = send_mcp_request("read_file", {"path": "config.toml"})

    # Parse and print result
    try:
        result_json = json.loads(result)
        if "result" in result_json and "content" in result_json["result"]:
            for content in result_json["result"]["content"]:
                if content.get("type") == "text":
                    print(f"File content: {content.get('text')}")
                elif content.get("type") == "resource":
                    print(f"Resource: {content.get('resource')}")
        else:
            print(f"Response: {result}")
    except json.JSONDecodeError:
        print(f"Response: {result}")


if __name__ == "__main__":
    main()
    time.sleep(10)
