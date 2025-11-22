# Miclow Python Client Library

A Python client library for the miclow orchestration system that provides a clean, intuitive API for communicating with miclow-managed processes.

## Features

- **Easy Integration**: Simple `import miclow` to get started
- **Type Safety**: Full type hints for better IDE support
- **Context Managers**: Clean resource management for topic subscriptions
- **Convenience Functions**: Both OOP and functional API styles
- **Security**: Only works within miclow-managed processes

## Installation

```bash
# Install from local development
uv install -e ./python

# Or install from PyPI (when published)
uv add miclow
```

## Quick Start

```python
import miclow

# Get the client
client = miclow.get_client()

# Subscribe to a topic
client.subscribe("my_topic")

# Publish a message
client.publish("my_topic", "Hello World!")

# Receive messages
message = client.receive("my_topic")
print(f"Received: {message.data}")
```

## API Reference

### MiclowClient Class

The main client class for interacting with miclow.

#### Methods

- `publish(topic: str, message: str)` - Publish a message to a topic
- `receive(topic: str | None = None) -> TopicMessage | SystemResponse | None` - Receive a message (from specific topic or any topic)
- `receive_response(topic: str) -> TopicMessage | SystemResponse` - Receive a response message on {topic}.result
- `subscribe(topic: str)` - Subscribe to a topic
- `unsubscribe(topic: str)` - Unsubscribe from a topic
- `pull(topic: str)` - Pull the latest message for a topic
- `get_status()` - Get system status
- `publish_response(original_topic: str, data: str)` - Publish a response message to {original_topic}.result

### Convenience Functions

- `miclow.publish(topic, message)`
- `miclow.receive(topic=None)`
- `miclow.receive_response(topic)`
- `miclow.subscribe(topic)`
- `miclow.unsubscribe(topic)`
- `miclow.pull(topic)`
- `miclow.publish_response(original_topic, data)`
- `miclow.response_topic(topic)` - Get response topic name ({topic}.result)

### Context Managers

```python
# Clean topic subscription management
with client.listen_to_topic("my_topic") as messages:
    for topic, message in messages:
        print(f"Received: {message}")
```

## Examples

See the `examples/` directory for more detailed usage examples.
