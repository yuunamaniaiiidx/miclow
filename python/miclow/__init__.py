"""
Miclow Python Client Library

A Python client library for the miclow orchestration system.
This module can only be imported within miclow-managed Python processes.
"""

import os
from typing import Any

if not os.environ.get('MICLOW_POD_ID'):
    raise ImportError(
        "miclow module can only be imported within miclow-managed pods. "
        "Make sure your Python process is started by miclow."
    )

__version__ = "0.1.0"
__all__ = [
    "publish",
]

def publish(topic: str, data: Any) -> None:
    """
    Publish a message to a topic.
    """
    pass
