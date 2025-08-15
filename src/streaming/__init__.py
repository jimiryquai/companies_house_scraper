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
from .health_monitor import (
    HealthMonitor,
    HealthStatus,
    ConnectionMetrics,
    StreamMetrics,
    ErrorMetrics,
    PerformanceMetrics
)
from .status_reporter import (
    ConnectionStatusReporter,
    ConnectionEvent,
    ConnectionReport
)
from .structured_logger import (
    StructuredLogger,
    ContextualLogger,
    LogLevel,
    LogContext,
    LogEntry,
    LoggingMixin,
    create_streaming_logger,
    create_contextual_logger
)
from .error_alerting import (
    ErrorAlertManager,
    AlertLevel,
    AlertChannel,
    ErrorPattern,
    AlertThreshold,
    EmailAlertChannel,
    SlackAlertChannel,
    WebhookAlertChannel,
    LogAlertChannel,
    setup_basic_alerting
)
from .log_filtering import (
    FilteredLogger,
    LogFilter,
    LogSampler,
    LevelFilter,
    PatternFilter,
    RateLimitFilter,
    ContextFilter,
    RandomSampler,
    TimeBasedSampler,
    VolumeBasedSampler,
    SamplingStrategy,
    setup_filtered_logger
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
    "EventLogError",
    "HealthMonitor",
    "HealthStatus",
    "ConnectionMetrics",
    "StreamMetrics",
    "ErrorMetrics",
    "PerformanceMetrics",
    "ConnectionStatusReporter",
    "ConnectionEvent",
    "ConnectionReport",
    "StructuredLogger",
    "ContextualLogger",
    "LogLevel",
    "LogContext",
    "LogEntry",
    "LoggingMixin",
    "create_streaming_logger",
    "create_contextual_logger",
    "ErrorAlertManager",
    "AlertLevel",
    "AlertChannel",
    "ErrorPattern",
    "AlertThreshold",
    "EmailAlertChannel",
    "SlackAlertChannel",
    "WebhookAlertChannel",
    "LogAlertChannel",
    "setup_basic_alerting",
    "FilteredLogger",
    "LogFilter",
    "LogSampler",
    "LevelFilter",
    "PatternFilter",
    "RateLimitFilter",
    "ContextFilter",
    "RandomSampler",
    "TimeBasedSampler",
    "VolumeBasedSampler",
    "SamplingStrategy",
    "setup_filtered_logger"
]
