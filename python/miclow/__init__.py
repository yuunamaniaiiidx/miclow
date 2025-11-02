"""
Miclow Python Client Library

A Python client library for the miclow orchestration system.
This module can only be imported within miclow-managed Python processes.
"""

# mypy: check-untyped-defs

import os
import sys
import threading
import time
from collections import deque
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

# Check if we're running within a miclow task
if not os.environ.get('MICLOW_TASK_ID'):
    raise ImportError(
        "miclow module can only be imported within miclow-managed tasks. "
        "Make sure your Python process is started by miclow."
    )

__version__ = "0.1.0"
__all__ = [
    "MiclowClient", "get_client", "send_message", "receive_message",
    "subscribe_topic", "send_stdout", "send_stderr", "SystemResponse",
    "start_task", "stop_task", "get_status", "add_task_from_toml", "add_task",
    "call_function"
]

class SystemResponseType(Enum):
    """System response types."""
    SUCCESS = "success"
    ERROR = "error"


@dataclass
class SystemResponse:
    """System command response."""
    response_type: SystemResponseType
    topic: str
    data: str
    timestamp: str | None = None


class MiclowClient:
    """
    Client for communicating with the miclow orchestration system.

    This client provides a high-level interface for:
    - Sending messages to topics
    - Receiving messages from subscribed topics
    - Managing topic subscriptions
    - Executing system commands
    """

    def __init__(self):
        """Initialize the miclow client."""
        self.task_id = os.environ['MICLOW_TASK_ID']
        self.stdin = sys.stdin
        self.stdout = sys.stdout
        self._subscribed_topics = set()
        self._response_handlers: dict[str, deque[SystemResponse]] = {}
        self._response_lock = threading.Lock()
        self._message_buffer: deque[tuple[str, str]] = deque()
        self._buffer_lock = threading.Lock()

    def send_message(self, topic: str, message: str) -> None:
        """
        Send a message to a topic using multiline protocol.

        Args:
            topic: The topic name to send the message to
            message: The message content (can be multiline)
        """
        print(f'"{topic}"::')
        print(message)
        print(f'::"{topic}"')
        sys.stdout.flush()

    def receive_message(self) -> tuple[str, str]:
        """
        Receive a message from stdin using multiline protocol.
        First checks the message buffer, then reads from stdin if buffer is empty.

        Returns:
            A tuple of (topic, message). If no topic is specified, topic will be empty.
        """
        # バッファをチェック
        with self._buffer_lock:
            if self._message_buffer:
                return self._message_buffer.popleft()

        key = input()
        value = "\n".join(input() for _ in range(int(input())))

        return key, value

    def _handle_system_response(self, topic: str, message: str) -> None:
        """Handle system response messages."""
        timestamp = datetime.now().isoformat()
        if topic.startswith("system.error."):
            with self._response_lock:
                if topic not in self._response_handlers:
                    self._response_handlers[topic] = deque()
                self._response_handlers[topic].append(SystemResponse(
                    response_type=SystemResponseType.ERROR,
                    topic=topic,
                    data=message,
                    timestamp=timestamp
                ))
        elif topic.startswith("system."):
            with self._response_lock:
                if topic not in self._response_handlers:
                    self._response_handlers[topic] = deque()
                self._response_handlers[topic].append(SystemResponse(
                    response_type=SystemResponseType.SUCCESS,
                    topic=topic,
                    data=message,
                    timestamp=timestamp
                ))

    def _wait_for_response(self, expected_topic: str) -> SystemResponse:
        """Wait for a system response."""
        key = input()
        value = "\n".join(input() for _ in range(int(input())))

        print(key, value)

        timestamp = datetime.now().isoformat()
        # valueの一行目をステータスとして判定
        lines = value.split('\n')
        if lines:
            status_line = lines[0].strip()
            # それ以降の行をdataとして使用
            data = '\n'.join(lines[1:]) if len(lines) > 1 else ''
            # ステータスを判定して設定
            if status_line == 'success':
                response_type = SystemResponseType.SUCCESS
            elif status_line == 'error':
                response_type = SystemResponseType.ERROR
            else:
                # デフォルトはERRORとして扱う
                response_type = SystemResponseType.ERROR
                data = value  # ステータス行が不明な場合は全体をdataに
        else:
            # 空の場合はERRORとして扱う
            response_type = SystemResponseType.ERROR
            data = ''

        system_response = SystemResponse(
            response_type=response_type,
            topic=key,
            data=data,
            timestamp=timestamp
        )

        return system_response


    def subscribe_topic(self, topic: str) -> SystemResponse:
        """
        Subscribe to a topic.

        Args:
            topic: The topic name to subscribe to

        Returns:
            SystemResponse with the result
        """
        print('"system.subscribe-topic"::')
        print(topic)
        print('::"system.subscribe-topic"')
        sys.stdout.flush()
        self._subscribed_topics.add(topic)

        expected_topic = "system.subscribe-topic"
        return self._wait_for_response(expected_topic)

    def unsubscribe_topic(self, topic: str) -> SystemResponse:
        """
        Unsubscribe from a topic.

        Args:
            topic: The topic name to unsubscribe from

        Returns:
            SystemResponse with the result
        """
        print('"system.unsubscribe-topic"::')
        print(topic)
        print('::"system.unsubscribe-topic"')
        sys.stdout.flush()
        self._subscribed_topics.discard(topic)

        expected_topic = "system.unsubscribe-topic"
        return self._wait_for_response(expected_topic)

    def start_task(self, task_name: str) -> SystemResponse:
        """
        Start a task.

        Args:
            task_name: The name of the task to start

        Returns:
            SystemResponse with the result
        """
        print('"system.start-task"::')
        print(task_name)
        print('::"system.start-task"')
        sys.stdout.flush()

        expected_topic = "system.start-task"
        return self._wait_for_response(expected_topic)

    def stop_task(self, task_name: str) -> SystemResponse:
        """
        Stop a task.

        Args:
            task_name: The name of the task to stop

        Returns:
            SystemResponse with the result
        """
        print('"system.stop-task"::')
        print(task_name)
        print('::"system.stop-task"')
        sys.stdout.flush()

        expected_topic = "system.stop-task"
        return self._wait_for_response(expected_topic)

    def get_status(self) -> SystemResponse:
        """
        Get the system status.

        Returns:
            SystemResponse with the status data
        """
        print('"system.status"::')
        print('')
        print('::"system.status"')
        sys.stdout.flush()

        expected_topic = "system.status"
        return self._wait_for_response(expected_topic)

    def add_task_from_toml(self, toml_data: str) -> SystemResponse:
        """
        Add a task from TOML configuration.

        Args:
            toml_data: TOML configuration data

        Returns:
            SystemResponse with the result
        """

        print('"system.add-task-from-toml"::')
        print(toml_data)
        print('::"system.add-task-from-toml"')
        sys.stdout.flush()

        expected_topic = "system.add-task-from-toml"
        return self._wait_for_response(expected_topic)

    def add_task(
        self,
        task_name: str,
        command: str,
        args: list[str],
        working_directory: str | None = None,
        environment_vars: dict[str, str] | None = None,
        subscribe_topics: list[str] | None = None
    ) -> SystemResponse:
        """
        Add a task by constructing TOML configuration from parameters.

        Args:
            task_name: Name of the task
            command: Command to execute
            args: List of command arguments
            working_directory: Working directory for the task
            environment_vars: Environment variables as key-value pairs
            subscribe_topics: List of topics to subscribe to initially

        Returns:
            SystemResponse with the result
        """
        # TOMLを構築
        toml_lines = ['[[tasks]]']
        toml_lines.append(f'task_name = "{task_name}"')
        toml_lines.append(f'command = "{command}"')

        # argsの処理
        if args:
            args_str = ', '.join(f'"{arg}"' for arg in args)
            toml_lines.append(f'args = [{args_str}]')
        else:
            toml_lines.append('args = []')

        # オプションフィールドの処理
        if working_directory is not None:
            toml_lines.append(f'working_directory = "{working_directory}"')

        if environment_vars:
            env_str = ', '.join(f'{k} = "{v}"' for k, v in environment_vars.items())
            toml_lines.append(f'environment_vars = {{ {env_str} }}')

        if subscribe_topics:
            topics_str = ', '.join(f'"{topic}"' for topic in subscribe_topics)
            toml_lines.append(f'subscribe_topics = [{topics_str}]')

        toml_data = '\n'.join(toml_lines)

        return self.add_task_from_toml(toml_data)

    def call_function(self, function_name: str, data: str = "") -> str:
        """
        Call a function defined in [[functions]] section and wait for return value.

        Args:
            function_name: The name of the function to call (must be defined in [[functions]])
            data: Optional data to pass to the function (will be sent as input)

        Returns:
            The return value from the function (via system.return)

        Raises:
            RuntimeError: If the function call fails or no return value is received
        """
        # system.function.{function_name}コマンドを送信
        function_command = f"system.function.{function_name}"
        print(f'"{function_command}"::')
        print(data)
        print(f'::"{function_command}"')
        sys.stdout.flush()

        # システムレスポンスを待つ（関数が起動したことの確認）
        expected_topic = f"system.function.{function_name}"
        response = self._wait_for_response(expected_topic)

        if response.response_type == SystemResponseType.ERROR:
            raise RuntimeError(f"Failed to call function '{function_name}': {response.data}")

        # 関数の返り値（system.return）を待つ
        # return_messageは通常のメッセージとして受信される
        while True:
            topic, message = self.receive_message()
            # system.returnメッセージをチェック
            if topic == "system.return":
                return message
            # 他のメッセージはバッファに保存
            with self._buffer_lock:
                self._message_buffer.append((topic, message))

    @contextmanager
    def listen_to_topic(self, topic: str):
        """
        Context manager for listening to a specific topic.

        Args:
            topic: The topic to listen to

        Yields:
            Generator of (topic, message) tuples
        """
        self.subscribe_topic(topic)
        try:
            yield self._message_generator(topic)
        finally:
            self.unsubscribe_topic(topic)

    def _message_generator(self, target_topic: str) -> Generator[tuple[str, str], None, None]:
        """Generate messages for a specific topic."""
        while True:
            topic, message = self.receive_message()
            if topic == target_topic:
                yield topic, message


# Global client instance
_client: MiclowClient | None = None


def get_client() -> MiclowClient:
    """Get the global miclow client instance."""
    global _client
    if _client is None:
        _client = MiclowClient()
    return _client


# Convenience functions
def send_message(topic: str, message: str) -> None:
    """Send a message to a topic."""
    get_client().send_message(topic, message)


def receive_message() -> tuple[str, str]:
    """Receive a message."""
    return get_client().receive_message()


def subscribe_topic(topic: str) -> SystemResponse:
    """Subscribe to a topic."""
    return get_client().subscribe_topic(topic)


def unsubscribe_topic(topic: str) -> SystemResponse:
    """Unsubscribe from a topic."""
    return get_client().unsubscribe_topic(topic)


def send_stdout(message: str) -> None:
    """Send a message to stdout."""
    get_client().send_stdout(message)


def send_stderr(message: str) -> None:
    """Send a message to stderr."""
    get_client().send_stderr(message)


def start_task(task_name: str) -> SystemResponse:
    """Start a task."""
    return get_client().start_task(task_name)


def stop_task(task_name: str) -> SystemResponse:
    """Stop a task."""
    return get_client().stop_task(task_name)


def get_status() -> SystemResponse:
    """Get the system status."""
    return get_client().get_status()


def add_task_from_toml(toml_data: str) -> SystemResponse:
    """Add a task from TOML configuration."""
    return get_client().add_task_from_toml(toml_data)


def add_task(
    task_name: str,
    command: str,
    args: list[str],
    working_directory: str | None = None,
    environment_vars: dict[str, str] | None = None,
    subscribe_topics: list[str] | None = None
) -> SystemResponse:
    """Add a task by constructing TOML configuration from parameters."""
    return get_client().add_task(
        task_name=task_name,
        command=command,
        args=args,
        working_directory=working_directory,
        environment_vars=environment_vars,
        subscribe_topics=subscribe_topics
    )


def call_function(function_name: str, data: str = "") -> str:
    """
    Call a function defined in [[functions]] section and wait for return value.

    Args:
        function_name: The name of the function to call (must be defined in [[functions]])
        data: Optional data to pass to the function (will be sent as input)

    Returns:
        The return value from the function (via system.return)

    Raises:
        RuntimeError: If the function call fails or no return value is received
    """
    return get_client().call_function(function_name, data)

