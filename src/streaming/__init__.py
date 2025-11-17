"""Companies House Streaming API Integration Module.

This module provides real-time monitoring of Companies House data streams
using PostgreSQL for data persistence.
"""

from .client import StreamingClient
from .config import StreamingConfig

__all__ = [
    "StreamingClient",
    "StreamingConfig",
]
