"""
Miclow Python Client Library

A Python client library for the miclow orchestration system.
This module can only be imported within miclow-managed Python processes.
"""

import os
import sys
import threading
import time
import uuid
from collections import deque
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Optional

if not os.environ.get('MICLOW_TASK_ID'):
    raise ImportError(
        "miclow module can only be imported within miclow-managed tasks. "
        "Make sure your Python process is started by miclow."
    )

__version__ = "0.1.0"
__all__ = [
    "MiclowClient", "get_client", "send_message",
    "subscribe_topic", "send_stdout", "send_stderr", "TopicMessage", "SystemResponse",
    "start_task", "stop_task", "get_status", "add_task_from_toml", "add_task",
    "call_function", "wait_for_topic", "receive_message"
]

class SystemResponseType(Enum):
    """System response types."""
    SUCCESS = "success"
    ERROR = "error"


class TopicMessage:
    """Topic message."""

    def __init__(
        self,
        topic: str,
        message: str
    ):
        """Initialize TopicMessage."""
        self.topic = topic
        self.message = message

    @property
    def data(self) -> str:
        """Alias for message property for consistency with SystemResponse."""
        return self.message

    def __str__(self) -> str:
        """Return string representation of TopicMessage."""
        return f"TopicMessage(topic='{self.topic}', message='{self.message}')"

    def __repr__(self) -> str:
        """Return string representation of TopicMessage."""
        return self.__str__()


class SystemResponse:
    """System command response."""

    def __init__(
        self,
        response_type: SystemResponseType,
        topic: str,
        data: str
    ):
        """Initialize SystemResponse."""
        self.response_type = response_type
        self.topic = topic
        self.data = data

    @classmethod
    def from_message(cls, topic: str, message: str) -> "SystemResponse":
        """
        Create a SystemResponse from a message.

        Args:
            topic: Topic name
            message: Message content (first line is the status)

        Returns:
            SystemResponse object
        """
        lines = message.split('\n')
        if lines:
            status_line = lines[0].strip()
            data = '\n'.join(lines[1:]) if len(lines) > 1 else ''
            if status_line == 'success':
                response_type = SystemResponseType.SUCCESS
            elif status_line == 'error':
                response_type = SystemResponseType.ERROR
            else:
                response_type = SystemResponseType.ERROR
                data = message
        else:
            response_type = SystemResponseType.ERROR
            data = ''

        return cls(
            response_type=response_type,
            topic=topic,
            data=data
        )

    def __str__(self) -> str:
        """Return string representation of SystemResponse."""
        return f"SystemResponse(response_type={self.response_type.value}, topic='{self.topic}', data='{self.data}')"

    def __repr__(self) -> str:
        """Return string representation of SystemResponse."""
        return self.__str__()


class Buffer:
    """
    Message buffer class.

    Buffers messages by topic. When a message is requested for a specific topic,
    it returns the oldest buffered message for that topic if available.
    """

    def __init__(self) -> None:
        """Initialize the buffer."""
        self.store: dict[str, TopicMessage | SystemResponse] = {}
        self.topic_index: dict[str, deque[str]] = {}
        self.time_index: deque[str] = deque()

    def _generate_uuid(self) -> str:
        """Generate a UUID v4."""
        return str(uuid.uuid4())

    def _is_system_response(self, topic: str) -> bool:
        """
        Determine if a topic is a system response.

        Args:
            topic: Topic name

        Returns:
            True if it is a system response
        """
        return topic.startswith("system.")

    def add(self, topic: str, message: str) -> None:
        """
        Add a message to the buffer.

        Args:
            topic: Topic name
            message: Message content
        """
        uuid_key = self._generate_uuid()
        if self._is_system_response(topic):
            value: TopicMessage | SystemResponse = SystemResponse.from_message(topic, message)
        else:
            value = TopicMessage(topic, message)
        self.store[uuid_key] = value
        if topic not in self.topic_index:
            self.topic_index[topic] = deque()
        self.topic_index[topic].append(uuid_key)
        self.time_index.append(uuid_key)

    def wait_for_topic(
        self, topic: str
    ) -> TopicMessage | SystemResponse | None:
        """
        Retrieve a message for the specified topic from the buffer.

        Args:
            topic: Topic name to retrieve

        Returns:
            TopicMessage or SystemResponse if available, None otherwise
        """
        while topic in self.topic_index and self.topic_index[topic]:
            uuid_key = self.topic_index[topic].popleft()

            if uuid_key not in self.store:
                continue

            value = self.store.pop(uuid_key)

            if not self.topic_index[topic]:
                del self.topic_index[topic]
            return value

        return None

    def get_oldest(
        self
    ) -> TopicMessage | SystemResponse | None:
        """
        Get the oldest message across all topics.

        Returns:
            A tuple of (topic, TopicMessage/SystemResponse) if available, None otherwise
        """
        while self.time_index:
            uuid_key = self.time_index.popleft()

            if uuid_key not in self.store:
                continue

            value = self.store.pop(uuid_key)

            return value
        return None

class MiclowClient:
    """
    Client for communicating with the miclow orchestration system.

    This client provides a high-level interface for:
    - Sending messages to topics
    - Receiving messages from subscribed topics
    - Managing topic subscriptions
    - Executing system commands
    """

    def __init__(self) -> None:
        """Initialize the miclow client."""
        self.task_id: str = os.environ['MICLOW_TASK_ID']
        self.stdin = sys.stdin
        self.stdout = sys.stdout
        self._subscribed_topics: set[str] = set()
        self._response_handlers: dict[str, deque[SystemResponse]] = {}
        self._response_lock: threading.Lock = threading.Lock()
        self._buffer: Buffer = Buffer()

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

    def _internal_receive_message(self) -> tuple[str, str]:
        """
        Receive a message from stdin using multiline protocol.
        Reads from stdin and adds to buffer, then returns the message.

        Internal method: Use wait_for_topic() for public API.

        Returns:
            A tuple of (topic, message). If no topic is specified, topic will be empty.
        """
        key = input()
        value = "\n".join(input() for _ in range(int(input())))

        self._buffer.add(key, value)

        return key, value

    def receive_message(self) -> TopicMessage | SystemResponse | None:
        """
        Receive the oldest message across all topics based on arrival order.
        Returns buffered message if available, otherwise waits until received from stdin.
        Regular messages return TopicMessage objects, system responses return SystemResponse objects.

        Returns:
            A tuple of (topic, TopicMessage/SystemResponse), or None if no message is available
        """
        result = self._buffer.get_oldest()
        if result is not None:
            return result

        self._internal_receive_message()
        return self._buffer.get_oldest()

    def wait_for_topic(self, topic: str) -> TopicMessage | SystemResponse:
        """
        Wait for and retrieve a message for the specified topic.
        Returns buffered message if available, otherwise waits until received from stdin.
        Regular messages return TopicMessage objects, system responses return SystemResponse objects.

        Args:
            topic: Topic name to wait for

        Returns:
            TopicMessage/SystemResponse
        """

        message = self._buffer.wait_for_topic(topic)
        if message is not None:
            return message

        while True:
            received_topic, received_message = self._internal_receive_message()
            if received_topic == topic:
                value = self._buffer.wait_for_topic(received_topic)
                if value is not None:
                    return value

    def _handle_system_response(self, topic: str, message: str) -> None:
        """Handle system response messages."""
        if topic.startswith("system.error."):
            with self._response_lock:
                if topic not in self._response_handlers:
                    self._response_handlers[topic] = deque()
                self._response_handlers[topic].append(SystemResponse(
                    response_type=SystemResponseType.ERROR,
                    topic=topic,
                    data=message
                ))
        elif topic.startswith("system."):
            with self._response_lock:
                if topic not in self._response_handlers:
                    self._response_handlers[topic] = deque()
                self._response_handlers[topic].append(SystemResponse(
                    response_type=SystemResponseType.SUCCESS,
                    topic=topic,
                    data=message
                ))

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
        response = self.wait_for_topic(expected_topic)
        if isinstance(response, SystemResponse):
            return response
        raise RuntimeError(f"Expected SystemResponse but got {type(response)}")

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
        response = self.wait_for_topic(expected_topic)
        if isinstance(response, SystemResponse):
            return response
        raise RuntimeError(f"Expected SystemResponse but got {type(response)}")

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
        response = self.wait_for_topic(expected_topic)
        if isinstance(response, SystemResponse):
            return response
        raise RuntimeError(f"Expected SystemResponse but got {type(response)}")

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
        response = self.wait_for_topic(expected_topic)
        if isinstance(response, SystemResponse):
            return response
        raise RuntimeError(f"Expected SystemResponse but got {type(response)}")

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
        response = self.wait_for_topic(expected_topic)
        if isinstance(response, SystemResponse):
            return response
        raise RuntimeError(f"Expected SystemResponse but got {type(response)}")

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
        response = self.wait_for_topic(expected_topic)
        if isinstance(response, SystemResponse):
            return response
        raise RuntimeError(f"Expected SystemResponse but got {type(response)}")

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
        toml_lines = ['[[tasks]]']
        toml_lines.append(f'task_name = "{task_name}"')
        toml_lines.append(f'command = "{command}"')

        if args:
            args_str = ', '.join(f'"{arg}"' for arg in args)
            toml_lines.append(f'args = [{args_str}]')
        else:
            toml_lines.append('args = []')

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
        function_command = f"system.function.{function_name}"
        print(f'"{function_command}"::')
        print(data)
        print(f'::"{function_command}"')
        sys.stdout.flush()

        expected_topic = f"system.function.{function_name}"
        response = self.wait_for_topic(expected_topic)
        if not isinstance(response, SystemResponse):
            raise RuntimeError(f"Expected SystemResponse but got {type(response)}")

        if response.response_type == SystemResponseType.ERROR:
            raise RuntimeError(f"Failed to call function '{function_name}': {response.data}")

        return_message = self.wait_for_topic("system.return")

        if isinstance(return_message, SystemResponse):
            return return_message.data
        elif isinstance(return_message, TopicMessage):
            return return_message.message
        raise RuntimeError(f"Unexpected message type: {type(return_message)}")

    @contextmanager
    def listen_to_topic(self, topic: str):
        """
        Context manager for listening to a specific topic.

        Args:
            topic: The topic to listen to

        Yields:
            Generator of TopicMessage or SystemResponse objects
        """
        self.subscribe_topic(topic)
        try:
            yield self._message_generator(topic)
        finally:
            self.unsubscribe_topic(topic)

    def _message_generator(
        self, target_topic: str
    ) -> Generator[TopicMessage | SystemResponse, None, None]:
        """Generate messages for a specific topic."""
        while True:
            message = self.wait_for_topic(target_topic)
            yield message


# Global client instance
_client: MiclowClient | None = None


def get_client() -> MiclowClient:
    """Get the global miclow client instance."""
    global _client
    if _client is None:
        _client = MiclowClient()
    return _client


def send_message(topic: str, message: str) -> None:
    """Send a message to a topic."""
    get_client().send_message(topic, message)


def subscribe_topic(topic: str) -> SystemResponse:
    """Subscribe to a topic."""
    return get_client().subscribe_topic(topic)


def unsubscribe_topic(topic: str) -> SystemResponse:
    """Unsubscribe from a topic."""
    return get_client().unsubscribe_topic(topic)


def receive_message() -> TopicMessage | SystemResponse | None:
    """
    Receive the oldest message across all topics based on arrival order.
    Returns buffered message if available, otherwise waits until received from stdin.
    Regular messages return TopicMessage objects, system responses return SystemResponse objects.

    Returns:
        A tuple of (topic, TopicMessage/SystemResponse), or None if no message is available
    """
    return get_client().receive_message()


def wait_for_topic(topic: str) -> TopicMessage | SystemResponse:
    """
    Wait for and retrieve a message for the specified topic.
    Returns buffered message if available, otherwise waits until received from stdin.
    Regular messages return TopicMessage objects, system responses return SystemResponse objects.

    Args:
        topic: Topic name to wait for

    Returns:
        TopicMessage/SystemResponse
    """
    return get_client().wait_for_topic(topic)


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

