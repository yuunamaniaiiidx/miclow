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
client.subscribe_topic("my_topic")

# Send a message
client.send_message("my_topic", "Hello World!")

# Receive messages
topic, message = client.receive_message()
print(f"Received: {topic}: {message}")
```

## API Reference

### MiclowClient Class

The main client class for interacting with miclow.

#### Methods

- `send_message(topic: str, message: str)` - Send a message to a topic
- `receive_message() -> Tuple[str, str]` - Receive a message
- `subscribe_topic(topic: str)` - Subscribe to a topic
- `unsubscribe_topic(topic: str)` - Unsubscribe from a topic
- `stdout(message: str)` - Send message to stdout
- `stderr(message: str)` - Send message to stderr
- `start_task(task_name: str)` - Start a task
- `stop_task(task_name: str)` - Stop a task
- `get_status()` - Get system status
- `kill_server()` - Initiate graceful shutdown

### Convenience Functions

- `miclow.send_message(topic, message)`
- `miclow.receive_message()`
- `miclow.subscribe_topic(topic)`
- `miclow.unsubscribe_topic(topic)`
- `miclow.stdout(message)`
- `miclow.stderr(message)`

### Context Managers

```python
# Clean topic subscription management
with client.listen_to_topic("my_topic") as messages:
    for topic, message in messages:
        print(f"Received: {message}")
```

## Examples

See the `examples/` directory for more detailed usage examples.
