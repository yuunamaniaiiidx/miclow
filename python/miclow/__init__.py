"""
Miclow Python Client Library

A Python client library for the miclow orchestration system.
This module can only be imported within miclow-managed Python processes.
"""

import os
import sys
import uuid
from collections import deque
from collections.abc import Generator
from contextlib import contextmanager
from enum import Enum

if not os.environ.get('MICLOW_TASK_ID'):
    raise ImportError(
        "miclow module can only be imported within miclow-managed tasks. "
        "Make sure your Python process is started by miclow."
    )

__version__ = "0.1.0"
__all__ = [
    "MiclowClient", "get_client", "send_message",
    "subscribe_topic", "send_stdout", "send_stderr", "TopicMessage", "SystemResponse",
    "get_status",
    "call_function", "wait_for_topic", "receive_message", "MessageType"
]

class SystemResponseType(Enum):
    """System response types."""
    SUCCESS = "success"
    ERROR = "error"


class MessageType(Enum):
    """Message type enumeration."""
    TOPIC = "topic"
    SYSTEM_RESPONSE = "system_response"
    FUNCTION = "function"
    RETURN = "return"

    @classmethod
    def from_topic_and_message(cls, topic: str, message: str) -> "MessageType":
        """
        Determine the message type from topic and message.

        Args:
            topic: Topic name
            message: Message content

        Returns:
            MessageType enum value
        """
        topic_lower = topic.lower()

        # Check for specific system topics first (受信時の想定)
        if topic_lower == "system.return":
            return cls.RETURN
        elif topic_lower == "system.function":
            # 関数への入力メッセージ（FunctionMessage）は "system.function" というトピックで来る
            return cls.FUNCTION
        elif topic_lower.startswith("system."):
            # system.function.{function_name} などの関数呼び出し応答は SystemResponse として扱う
            return cls.SYSTEM_RESPONSE
        else:
            return cls.TOPIC

    def to_message(self, topic: str, message: str) -> "TopicMessage | SystemResponse | FunctionMessage":
        """
        Convert topic and message pair to appropriate message object.

        Args:
            topic: Topic name
            message: Message content

        Returns:
            TopicMessage, SystemResponse, or FunctionMessage object

        Raises:
            ValueError: If message type is not supported
        """
        if self == MessageType.TOPIC:
            return TopicMessage(topic, message)
        elif self == MessageType.SYSTEM_RESPONSE:
            return SystemResponse.from_message(topic, message)
        elif self == MessageType.FUNCTION:
            return FunctionMessage.from_message(topic, message)
        elif self == MessageType.RETURN:
            # ReturnMessage is treated as a TopicMessage for now
            # since there's no ReturnMessage class in Python side
            return TopicMessage(topic, message)
        else:
            raise ValueError(f"Unsupported message type: {self}")


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

class FunctionMessage:
    """Function message."""

    def __init__(
        self,
        caller_task_name: str,
        data: str
    ):
        """Initialize FunctionMessage."""
        self.topic = "system.function"
        self.caller_task_name = caller_task_name
        self.data = data

    @classmethod
    def from_message(cls, topic: str, message: str) -> "FunctionMessage":
        """Create a FunctionMessage from a message."""
        lines = message.split('\n')
        if lines:
            caller_task_name = lines[0].strip()
            data = '\n'.join(lines[1:]) if len(lines) > 1 else ''
            return cls(caller_task_name, data)
        else:
            raise RuntimeError(f"Expected FunctionMessage but got {type(message)}")

    def __str__(self) -> str:
        """Return string representation of FunctionMessage."""
        return f"FunctionMessage(caller_task_name='{self.caller_task_name}', data='{self.data}')"

    def __repr__(self) -> str:
        """Return string representation of FunctionMessage."""
        return self.__str__()

class Buffer:
    """
    Message buffer class.

    Buffers messages by topic. When a message is requested for a specific topic,
    it returns the oldest buffered message for that topic if available.
    """

    def __init__(self) -> None:
        """Initialize the buffer."""
        self.store: dict[str, TopicMessage | SystemResponse | FunctionMessage] = {}
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
        message_type = MessageType.from_topic_and_message(topic, message)
        value: TopicMessage | SystemResponse | FunctionMessage = message_type.to_message(topic, message)
        self.store[uuid_key] = value
        if topic not in self.topic_index:
            self.topic_index[topic] = deque()
        self.topic_index[topic].append(uuid_key)
        self.time_index.append(uuid_key)

    def wait_for_topic(
        self, topic: str
    ) -> TopicMessage | SystemResponse | FunctionMessage | None:
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
    ) -> TopicMessage | SystemResponse | FunctionMessage | None:
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

    def receive_message(self) -> TopicMessage | SystemResponse | FunctionMessage | None:
        """
        Receive the oldest message across all topics based on arrival order.
        Returns buffered message if available, otherwise waits until received from stdin.
        Regular messages return TopicMessage objects, system responses return SystemResponse objects.

        Returns:
            TopicMessage/SystemResponse/FunctionMessage, or None if no message is available
        """
        result = self._buffer.get_oldest()
        if result is not None:
            return result

        self._internal_receive_message()
        return self._buffer.get_oldest()

    def wait_for_topic(self, topic: str) -> TopicMessage | SystemResponse | FunctionMessage:
        """
        Wait for and retrieve a message for the specified topic.
        Returns buffered message if available, otherwise waits until received from stdin.
        Regular messages return TopicMessage objects, system responses return SystemResponse objects.

        Args:
            topic: Topic name to wait for

        Returns:
            TopicMessage/SystemResponse/FunctionMessage
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

        expected_topic = "system.unsubscribe-topic"
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
            Generator of TopicMessage, SystemResponse, or FunctionMessage objects
        """
        self.subscribe_topic(topic)
        try:
            yield self._message_generator(topic)
        finally:
            self.unsubscribe_topic(topic)

    def _message_generator(
        self, target_topic: str
    ) -> Generator[TopicMessage | SystemResponse | FunctionMessage, None, None]:
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


def receive_message() -> TopicMessage | SystemResponse | FunctionMessage | None:
    """
    Receive the oldest message across all topics based on arrival order.
    Returns buffered message if available, otherwise waits until received from stdin.
    Regular messages return TopicMessage objects, system responses return SystemResponse objects.

    Returns:
        TopicMessage/SystemResponse/FunctionMessage, or None if no message is available
    """
    return get_client().receive_message()


def wait_for_topic(topic: str) -> TopicMessage | SystemResponse | FunctionMessage:
    """
    Wait for and retrieve a message for the specified topic.
    Returns buffered message if available, otherwise waits until received from stdin.
    Regular messages return TopicMessage objects, system responses return SystemResponse objects.

    Args:
        topic: Topic name to wait for

    Returns:
        TopicMessage/SystemResponse/FunctionMessage
    """
    return get_client().wait_for_topic(topic)


def get_status() -> SystemResponse:
    """Get the system status."""
    return get_client().get_status()




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

