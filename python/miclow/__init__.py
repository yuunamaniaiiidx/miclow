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

if not os.environ.get('MICLOW_POD_ID'):
    raise ImportError(
        "miclow module can only be imported within miclow-managed pods. "
        "Make sure your Python process is started by miclow."
    )

__version__ = "0.1.0"
__all__ = [
    "MiclowClient", "get_client", "send_message",
    "subscribe_topic", "TopicMessage", "SystemResponse",
    "get_status", "get_latest_message",
    "wait_for_topic", "wait_for_response", "receive_message",
    "MessageType",
    "send_response", "return_topic_for"
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
            TopicMessage/SystemResponse, or None if no message is available
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

    def wait_for_response(self, topic: str) -> TopicMessage | SystemResponse:
        """
        Wait for a response message on the return topic for the given topic.
        The return topic follows the pattern: {topic}.result

        This is typically used after sending a message to wait for the
    response. Returns buffered message if available, otherwise waits
    until received from stdin.

        Args:
            topic: The original topic name (response will be on {topic}.result)

        Returns:
            TopicMessage/SystemResponse from the return topic

        Example:
            # Send a message
            send_message("demo.request", "Hello")

            # Wait for response on "demo.request.result"
            response = wait_for_response("demo.request")
            print(response.data)
        """
        return_topic = return_topic_for(topic)
        return self.wait_for_topic(return_topic)

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

    def get_latest_message(self, topic: str) -> SystemResponse:
        """
        Get the latest message for a topic.

        Args:
            topic: The topic name to get the latest message for

        Returns:
            SystemResponse with the latest message data (if available) or error
        """
        print('"system.get-latest-message"::')
        print(topic)
        print('::"system.get-latest-message"')
        sys.stdout.flush()

        expected_topic = "system.get-latest-message"
        response = self.wait_for_topic(expected_topic)
        if isinstance(response, SystemResponse):
            return response
        raise RuntimeError(f"Expected SystemResponse but got {type(response)}")


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
        TopicMessage/SystemResponse, or None if no message is available
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


def wait_for_response(topic: str) -> TopicMessage | SystemResponse:
    """
    Wait for a response message on the return topic for the given topic.
    The return topic follows the pattern: {topic}.result

    This is typically used after sending a message to wait for the response.
    Returns buffered message if available, otherwise waits until received
    from stdin.

    Args:
        topic: The original topic name (response will be on {topic}.result)

    Returns:
        TopicMessage/SystemResponse from the return topic

    Example:
        # Send a message
        send_message("demo.request", "Hello")

        # Wait for response on "demo.request.result"
        response = wait_for_response("demo.request")
        print(response.data)
    """
    return get_client().wait_for_response(topic)


def get_status() -> SystemResponse:
    """Get the system status."""
    return get_client().get_status()


def get_latest_message(topic: str) -> SystemResponse:
    """
    Get the latest message for a topic.

    Args:
        topic: The topic name to get the latest message for

    Returns:
        SystemResponse with the latest message data (if available) or error
    """
    return get_client().get_latest_message(topic)


def return_topic_for(topic: str) -> str:
    """
    Generate the return topic name for a given topic.
    The return topic follows the pattern: {original_topic}.result

    Args:
        topic: The original topic name

    Returns:
        The return topic name (e.g., "demo.topic" -> "demo.topic.result")
    """
    return f"{topic}.result"


def send_response(original_topic: str, data: str) -> None:
    """
    Send a response message to the return topic for the given topic.
    This is a convenience function that automatically constructs the return
    topic using the pattern: {original_topic}.result

    Args:
        original_topic: The original topic name that this response is for
        data: The response data to send

    Example:
        # Receive a message on "demo.request"
        message = wait_for_topic("demo.request")

        # Process the message...
        result = process(message.data)

        # Send response to "demo.request.result"
        send_response("demo.request", result)
    """
    return_topic = return_topic_for(original_topic)
    send_message(return_topic, data)

