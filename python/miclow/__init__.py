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
from typing import overload

if not os.environ.get('MICLOW_POD_ID'):
    raise ImportError(
        "miclow module can only be imported within miclow-managed pods. "
        "Make sure your Python process is started by miclow."
    )

__version__ = "0.1.0"
__all__ = [
    "MiclowClient", "get_client", "publish",
    "subscribe", "unsubscribe", "TopicMessage", "SystemResponse",
    "get_status", "pull",
    "receive", "receive_response",
    "MessageType",
    "publish_response", "response_topic", "idle"
]

class SystemResponseType(Enum):
    """System response types."""
    SUCCESS = "success"
    ERROR = "error"


class MessageType(Enum):
    """Message type enumeration."""
    TOPIC = "topic"
    SYSTEM_RESPONSE = "system_response"

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

        if topic_lower.startswith("system."):
            return cls.SYSTEM_RESPONSE
        return cls.TOPIC

    def to_message(self, topic: str, message: str) -> "TopicMessage | SystemResponse":
        """
        Convert topic and message pair to appropriate message object.

        Args:
            topic: Topic name
            message: Message content

        Returns:
            TopicMessage or SystemResponse object

        Raises:
            ValueError: If message type is not supported
        """
        if self == MessageType.TOPIC:
            return TopicMessage(topic, message)
        elif self == MessageType.SYSTEM_RESPONSE:
            return SystemResponse.from_message(topic, message)
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
        message_type = MessageType.from_topic_and_message(topic, message)
        value: TopicMessage | SystemResponse = message_type.to_message(topic, message)
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
        self.pod_id: str = os.environ['MICLOW_POD_ID']
        self.stdin = sys.stdin
        self.stdout = sys.stdout
        self._buffer: Buffer = Buffer()

    def publish(self, topic: str, message: str) -> None:
        """
        Publish a message to a topic using multiline protocol.

        Args:
            topic: The topic name to publish the message to
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

    @overload
    def receive(self, topic: None = None) -> TopicMessage | SystemResponse | None:
        """Receive a message from any topic. May return None if no message is available."""
        ...

    @overload
    def receive(self, topic: str) -> TopicMessage | SystemResponse:
        """Receive a message from a specific topic. Waits until a message is available."""
        ...

    def receive(self, topic: str | None = None) -> TopicMessage | SystemResponse | None:
        """
        Receive a message from subscribed topics.
        If topic is None, returns the oldest message across all topics based on arrival order.
        If topic is specified, waits for and retrieves a message for that specific topic.
        Returns buffered message if available, otherwise waits until received from stdin.
        Regular messages return TopicMessage objects, system responses return SystemResponse objects.

        Args:
            topic: Optional topic name to receive from. If None, receives from any topic.

        Returns:
            TopicMessage/SystemResponse, or None if no message is available (only when topic is None)
        """
        if topic is None:
            # Receive from any topic
            result = self._buffer.get_oldest()
            if result is not None:
                return result

            # Signal that we're ready to receive messages
            self.idle()
            self._internal_receive_message()
            return self._buffer.get_oldest()
        else:
            # Receive from specific topic
            message = self._buffer.wait_for_topic(topic)
            if message is not None:
                return message

            # Signal that we're ready to receive messages
            self.idle()
            while True:
                received_topic, received_message = self._internal_receive_message()
                if received_topic == topic:
                    value = self._buffer.wait_for_topic(received_topic)
                    if value is not None:
                        return value

    def receive_response(self, topic: str) -> TopicMessage | SystemResponse:
        """
        Receive a response message on the return topic for the given topic.
        The return topic follows the pattern: {topic}.result

        This is typically used after publishing a message to wait for the
        response. Returns buffered message if available, otherwise waits
        until received from stdin.

        Args:
            topic: The original topic name (response will be on {topic}.result)

        Returns:
            TopicMessage/SystemResponse from the return topic

        Example:
            # Publish a message
            publish("demo.request", "Hello")

            # Receive response on "demo.request.result"
            response = receive_response("demo.request")
            print(response.data)
        """
        return_topic = response_topic(topic)
        return self.receive(return_topic)

    def pull(self, topic: str) -> TopicMessage:
        """
        Pull the latest message for a topic.

        Args:
            topic: The topic name to pull the latest message for

        Returns:
            TopicMessage with the latest message data (if available)
        """
        print('"system.pull"::')
        print(topic)
        print('::"system.pull"')
        sys.stdout.flush()

        # receive() will automatically send idle() when waiting for messages
        # 要求されたトピックでデータを待つ
        response = self.receive(topic)
        if isinstance(response, TopicMessage):
            return response
        raise RuntimeError(f"Expected TopicMessage but got {type(response)}")

    def idle(self) -> None:
        """
        Send an idle signal to the system.
        This informs the system that the process is ready to receive messages.
        The message content is empty.
        """
        self.publish("system.idle", "")

    def _message_generator(
        self, target_topic: str
    ) -> Generator[TopicMessage | SystemResponse, None, None]:
        """Generate messages for a specific topic."""
        while True:
            message = self.receive(target_topic)
            if message is not None:
                yield message


# Global client instance
_client: MiclowClient | None = None


def get_client() -> MiclowClient:
    """Get the global miclow client instance."""
    global _client
    if _client is None:
        _client = MiclowClient()
    return _client


def publish(topic: str, message: str) -> None:
    """Publish a message to a topic."""
    get_client().publish(topic, message)


def subscribe(topic: str) -> SystemResponse:
    """Subscribe to a topic."""
    return get_client().subscribe(topic)


def unsubscribe(topic: str) -> SystemResponse:
    """Unsubscribe from a topic."""
    return get_client().unsubscribe(topic)


@overload
def receive(topic: None = None) -> TopicMessage | SystemResponse | None:
    """Receive a message from any topic. May return None if no message is available."""
    ...

@overload
def receive(topic: str) -> TopicMessage | SystemResponse:
    """Receive a message from a specific topic. Waits until a message is available."""
    ...

def receive(topic: str | None = None) -> TopicMessage | SystemResponse | None:
    """
    Receive a message from subscribed topics.
    If topic is None, returns the oldest message across all topics based on arrival order.
    If topic is specified, waits for and retrieves a message for that specific topic.
    Returns buffered message if available, otherwise waits until received from stdin.
    Regular messages return TopicMessage objects, system responses return SystemResponse objects.

    Args:
        topic: Optional topic name to receive from. If None, receives from any topic.

    Returns:
        TopicMessage/SystemResponse, or None if no message is available (only when topic is None)
    """
    return get_client().receive(topic)


def receive_response(topic: str) -> TopicMessage | SystemResponse:
    """
    Receive a response message on the return topic for the given topic.
    The return topic follows the pattern: {topic}.result

    This is typically used after publishing a message to wait for the response.
    Returns buffered message if available, otherwise waits until received
    from stdin.

    Args:
        topic: The original topic name (response will be on {topic}.result)

    Returns:
        TopicMessage/SystemResponse from the return topic

    Example:
        # Publish a message
        publish("demo.request", "Hello")

        # Receive response on "demo.request.result"
        response = receive_response("demo.request")
        print(response.data)
    """
    return get_client().receive_response(topic)


def get_status() -> SystemResponse:
    """Get the system status."""
    return get_client().get_status()


def pull(topic: str) -> TopicMessage:
    """
    Pull the latest message for a topic.

    Args:
        topic: The topic name to pull the latest message for

    Returns:
        TopicMessage with the latest message data (if available)
    """
    return get_client().pull(topic)


def response_topic(topic: str) -> str:
    """
    Generate the response topic name for a given topic.
    The response topic follows the pattern: {original_topic}.result

    Args:
        topic: The original topic name

    Returns:
        The response topic name (e.g., "demo.topic" -> "demo.topic.result")
    """
    return f"{topic}.result"


def publish_response(original_topic: str, data: str) -> None:
    """
    Publish a response message to the return topic for the given topic.
    This is a convenience function that automatically constructs the return
    topic using the pattern: {original_topic}.result

    Args:
        original_topic: The original topic name that this response is for
        data: The response data to publish

    Example:
        # Receive a message on "demo.request"
        message = receive("demo.request")

        # Process the message...
        result = process(message.data)

        # Publish response to "demo.request.result"
        publish_response("demo.request", result)
    """
    return_topic = response_topic(original_topic)
    publish(return_topic, data)


def idle() -> None:
    """
    Send an idle signal to the system.
    This informs the system that the process is ready to receive messages.
    The message content is empty.

    Example:
        # Signal that the process is ready to receive messages
        idle()
    """
    get_client().idle()

