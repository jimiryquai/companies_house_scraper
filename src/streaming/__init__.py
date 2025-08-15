"""
Companies House Streaming API Integration Module

This module provides real-time monitoring of Companies House data streams 
to detect companies entering/exiting strike-off status and ensure zero 
data gaps between bulk processing cycles.
"""

from .client import StreamingClient
from .config import StreamingConfig
from .event_processor import EventProcessor, CompanyEvent, EventValidationError
from .database import (
    DatabaseManager,
    StreamingDatabase,
    DatabaseError,
    CompanyRecord,
    StreamEventRecord
)
from .event_logger import (
    EventLogger,
    EventTracker,
    ProcessingStatus,
    EventLogError
)

__all__ = [
    "StreamingClient",
    "StreamingConfig",
    "EventProcessor",
    "CompanyEvent",
    "EventValidationError",
    "DatabaseManager",
    "StreamingDatabase",
    "DatabaseError",
    "CompanyRecord",
    "StreamEventRecord",
    "EventLogger",
    "EventTracker",
    "ProcessingStatus",
    "EventLogError"
]