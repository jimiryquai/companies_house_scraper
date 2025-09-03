"""Companies House Streaming API Integration Module.

This module provides real-time monitoring of Companies House data streams
to detect companies entering/exiting strike-off status and ensure zero
data gaps between bulk processing cycles.
"""

from .client import StreamingClient
from .config import StreamingConfig
from .database import (
    CompanyRecord,
    DatabaseError,
    DatabaseManager,
    StreamEventRecord,
    StreamingDatabase,
)
from .error_alerting import (
    AlertChannel,
    AlertLevel,
    AlertThreshold,
    EmailAlertChannel,
    ErrorAlertManager,
    ErrorPattern,
    LogAlertChannel,
    SlackAlertChannel,
    WebhookAlertChannel,
    setup_basic_alerting,
)
from .event_logger import EventLogError, EventLogger, EventTracker, ProcessingStatus
from .event_processor import CompanyEvent, EventProcessor, EventValidationError
from .health_monitor import (
    ConnectionMetrics,
    ErrorMetrics,
    HealthMonitor,
    HealthStatus,
    PerformanceMetrics,
    StreamMetrics,
)
from .log_filtering import (
    ContextFilter,
    FilteredLogger,
    LevelFilter,
    LogFilter,
    LogSampler,
    PatternFilter,
    RandomSampler,
    RateLimitFilter,
    SamplingStrategy,
    TimeBasedSampler,
    VolumeBasedSampler,
    setup_filtered_logger,
)
from .queue_manager import PriorityQueueManager, QueuedRequest, RequestPriority
from .status_reporter import ConnectionEvent, ConnectionReport, ConnectionStatusReporter
from .ch_api_processor import CompaniesHouseAPIProcessor, CompaniesHouseAPIError
from .company_state_manager import CompanyStateManager, ProcessingState
from .structured_logger import (
    ContextualLogger,
    LogContext,
    LogEntry,
    LoggingMixin,
    LogLevel,
    StructuredLogger,
    create_contextual_logger,
    create_streaming_logger,
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
    "setup_filtered_logger",
    "PriorityQueueManager",
    "QueuedRequest",
    "RequestPriority",
    "CompaniesHouseAPIProcessor",
    "CompaniesHouseAPIError", 
    "CompanyStateManager",
    "ProcessingState",
]
