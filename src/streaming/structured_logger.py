"""Structured logging implementation for streaming module.
Provides contextual, JSON-formatted logging with performance and observability features.
"""

import asyncio
import json
import logging
import logging.handlers
import traceback
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional

from .config import StreamingConfig

if TYPE_CHECKING:
    from asyncio import Queue, Task


class LogLevel(Enum):
    """Log levels for structured logging."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

    def to_logging_level(self) -> int:
        """Convert to standard logging level."""
        return {
            LogLevel.DEBUG: logging.DEBUG,
            LogLevel.INFO: logging.INFO,
            LogLevel.WARNING: logging.WARNING,
            LogLevel.ERROR: logging.ERROR,
            LogLevel.CRITICAL: logging.CRITICAL,
        }[self]


@dataclass
class LogContext:
    """Contextual information for log entries."""

    request_id: Optional[str] = None
    session_id: Optional[str] = None
    company_number: Optional[str] = None
    event_id: Optional[str] = None
    operation: Optional[str] = None
    user_agent: Optional[str] = None
    processing_time_ms: Optional[int] = None
    retry_count: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, excluding None values."""
        return {k: v for k, v in asdict(self).items() if v is not None}

    def merge(self, other: "LogContext") -> "LogContext":
        """Merge with another LogContext, preferring other's values."""
        merged_dict = self.to_dict()
        merged_dict.update(other.to_dict())
        return LogContext(**merged_dict)


@dataclass
class LogEntry:
    """Structured log entry."""

    timestamp: datetime
    level: LogLevel
    message: str
    context: LogContext
    extra_data: Optional[Dict[str, Any]] = None
    exception_info: Optional[str] = None
    stack_trace: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {
            "timestamp": self.timestamp.isoformat(),
            "level": self.level.value,
            "message": self.message,
            "context": self.context.to_dict(),
        }

        # Add extra data directly to the root level
        if self.extra_data:
            result.update(self.extra_data)

        # Add exception information if present
        if self.exception_info:
            result["exception"] = self.exception_info

        if self.stack_trace:
            result["stack_trace"] = self.stack_trace

        return result

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"))


class StructuredLogHandler:
    """Handles structured log output with rotation and filtering."""

    def __init__(
        self,
        log_file: str,
        log_level: LogLevel = LogLevel.INFO,
        max_file_size_mb: int = 100,
        backup_count: int = 10,
    ):
        """Initialize log handler."""
        self.log_file = log_file
        self.log_level = log_level
        self.max_file_size_mb = max_file_size_mb
        self.backup_count = backup_count

        self._handler: Optional[logging.handlers.RotatingFileHandler] = None
        self._logger: Optional[logging.Logger] = None
        self._queue: Optional[Queue[LogEntry]] = None
        self._worker_task: Optional[Task[None]] = None
        self._shutdown_event: Optional[asyncio.Event] = None

    async def start(self) -> None:
        """Start the log handler."""
        # Ensure log directory exists
        Path(self.log_file).parent.mkdir(parents=True, exist_ok=True)

        # Set up rotating file handler
        max_bytes = self.max_file_size_mb * 1024 * 1024
        self._handler = logging.handlers.RotatingFileHandler(
            self.log_file, maxBytes=max_bytes, backupCount=self.backup_count
        )

        # Set up logger
        self._logger = logging.getLogger(f"structured_logger_{id(self)}")
        self._logger.setLevel(logging.DEBUG)
        self._logger.addHandler(self._handler)

        # Set up async queue for non-blocking logging
        self._queue = asyncio.Queue()
        self._shutdown_event = asyncio.Event()

        # Start background worker
        self._worker_task = asyncio.create_task(self._worker())

    async def stop(self) -> None:
        """Stop the log handler and flush remaining logs."""
        if self._shutdown_event:
            self._shutdown_event.set()

        if self._worker_task:
            await self._worker_task

        if self._handler:
            self._handler.close()

        if self._logger and self._handler:
            self._logger.removeHandler(self._handler)

    async def write_log(self, entry: LogEntry) -> None:
        """Write a log entry."""
        if entry.level.to_logging_level() >= self.log_level.to_logging_level():
            if self._queue:
                await self._queue.put(entry)

    async def flush(self) -> None:
        """Flush any pending log entries."""
        if self._handler:
            self._handler.flush()

    async def _worker(self) -> None:
        """Background worker to process log entries."""
        if not self._shutdown_event or not self._queue:
            return
        while not self._shutdown_event.is_set():
            try:
                # Wait for log entry or shutdown
                if not self._queue:
                    break
                entry = await asyncio.wait_for(self._queue.get(), timeout=0.1)

                # Write to file
                if self._logger:
                    self._logger.log(entry.level.to_logging_level(), entry.to_json())

                if self._queue:
                    self._queue.task_done()

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                # Avoid logging errors in the logger itself
                print(f"Error in log worker: {e}")

        # Process remaining entries
        if not self._queue:
            return
        while not self._queue.empty():
            try:
                if not self._queue:
                    break
                entry = self._queue.get_nowait()
                if self._logger:
                    self._logger.log(entry.level.to_logging_level(), entry.to_json())
                if self._queue:
                    self._queue.task_done()
            except asyncio.QueueEmpty:
                break
            except Exception as e:
                print(f"Error processing remaining logs: {e}")


class StructuredLogger:
    """Main structured logger implementation."""

    def __init__(
        self,
        config: StreamingConfig,
        log_file: Optional[str] = None,
        log_level: LogLevel = LogLevel.INFO,
        max_file_size_mb: int = 100,
        backup_count: int = 10,
    ):
        """Initialize structured logger."""
        self.config = config
        self.log_file = log_file or f"logs/streaming_{datetime.now().strftime('%Y%m%d')}.log"
        self.log_level = log_level

        self.handler = StructuredLogHandler(
            log_file=self.log_file,
            log_level=log_level,
            max_file_size_mb=max_file_size_mb,
            backup_count=backup_count,
        )

    async def start(self) -> None:
        """Start the logger."""
        await self.handler.start()

    async def stop(self) -> None:
        """Stop the logger."""
        await self.handler.stop()

    async def log(
        self,
        level: LogLevel,
        message: str,
        context: Optional[LogContext] = None,
        extra_data: Optional[Dict[str, Any]] = None,
        exception: Optional[Exception] = None,
    ) -> None:
        """Log a message with structured context."""
        entry = LogEntry(
            timestamp=datetime.now(),
            level=level,
            message=message,
            context=context or LogContext(),
            extra_data=extra_data,
        )

        # Add exception information if provided
        if exception:
            entry.exception_info = str(exception)
            entry.stack_trace = traceback.format_exc()

        await self.handler.write_log(entry)

    async def debug(
        self,
        message: str,
        context: Optional[LogContext] = None,
        extra_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log debug message."""
        await self.log(LogLevel.DEBUG, message, context, extra_data)

    async def info(
        self,
        message: str,
        context: Optional[LogContext] = None,
        extra_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log info message."""
        await self.log(LogLevel.INFO, message, context, extra_data)

    async def warning(
        self,
        message: str,
        context: Optional[LogContext] = None,
        extra_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log warning message."""
        await self.log(LogLevel.WARNING, message, context, extra_data)

    async def error(
        self,
        message: str,
        context: Optional[LogContext] = None,
        extra_data: Optional[Dict[str, Any]] = None,
        exception: Optional[Exception] = None,
    ) -> None:
        """Log error message."""
        await self.log(LogLevel.ERROR, message, context, extra_data, exception)

    async def critical(
        self,
        message: str,
        context: Optional[LogContext] = None,
        extra_data: Optional[Dict[str, Any]] = None,
        exception: Optional[Exception] = None,
    ) -> None:
        """Log critical message."""
        await self.log(LogLevel.CRITICAL, message, context, extra_data, exception)

    async def exception(
        self,
        message: str,
        context: Optional[LogContext] = None,
        extra_data: Optional[Dict[str, Any]] = None,
        exception: Optional[Exception] = None,
    ) -> None:
        """Log exception with stack trace."""
        await self.log(LogLevel.ERROR, message, context, extra_data, exception)


class ContextualLogger:
    """Logger that automatically includes default context."""

    def __init__(self, base_logger: StructuredLogger, default_context: LogContext):
        """Initialize contextual logger."""
        self.base_logger = base_logger
        self.default_context = default_context

    def _merge_context(self, additional_context: Optional[LogContext]) -> LogContext:
        """Merge default context with additional context."""
        if additional_context:
            return self.default_context.merge(additional_context)
        return self.default_context

    async def debug(
        self,
        message: str,
        context: Optional[LogContext] = None,
        extra_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log debug message with merged context."""
        merged_context = self._merge_context(context)
        await self.base_logger.debug(message, merged_context, extra_data)

    async def info(
        self,
        message: str,
        context: Optional[LogContext] = None,
        extra_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log info message with merged context."""
        merged_context = self._merge_context(context)
        await self.base_logger.info(message, merged_context, extra_data)

    async def warning(
        self,
        message: str,
        context: Optional[LogContext] = None,
        extra_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log warning message with merged context."""
        merged_context = self._merge_context(context)
        await self.base_logger.warning(message, merged_context, extra_data)

    async def error(
        self,
        message: str,
        context: Optional[LogContext] = None,
        extra_data: Optional[Dict[str, Any]] = None,
        exception: Optional[Exception] = None,
    ) -> None:
        """Log error message with merged context."""
        merged_context = self._merge_context(context)
        await self.base_logger.error(message, merged_context, extra_data, exception)

    async def critical(
        self,
        message: str,
        context: Optional[LogContext] = None,
        extra_data: Optional[Dict[str, Any]] = None,
        exception: Optional[Exception] = None,
    ) -> None:
        """Log critical message with merged context."""
        merged_context = self._merge_context(context)
        await self.base_logger.critical(message, merged_context, extra_data, exception)

    async def exception(
        self,
        message: str,
        context: Optional[LogContext] = None,
        extra_data: Optional[Dict[str, Any]] = None,
        exception: Optional[Exception] = None,
    ) -> None:
        """Log exception with merged context."""
        merged_context = self._merge_context(context)
        await self.base_logger.exception(message, merged_context, extra_data, exception)


class LoggingMixin:
    """Mixin to add structured logging to classes."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._logger: Optional[ContextualLogger] = None

    def set_logger(self, logger: ContextualLogger) -> None:
        """Set the logger for this instance."""
        self._logger = logger

    async def log_debug(self, message: str, **kwargs: Any) -> None:
        """Log debug message if logger is available."""
        if self._logger:
            await self._logger.debug(message, **kwargs)

    async def log_info(self, message: str, **kwargs: Any) -> None:
        """Log info message if logger is available."""
        if self._logger:
            await self._logger.info(message, **kwargs)

    async def log_warning(self, message: str, **kwargs: Any) -> None:
        """Log warning message if logger is available."""
        if self._logger:
            await self._logger.warning(message, **kwargs)

    async def log_error(self, message: str, **kwargs: Any) -> None:
        """Log error message if logger is available."""
        if self._logger:
            await self._logger.error(message, **kwargs)

    async def log_critical(self, message: str, **kwargs: Any) -> None:
        """Log critical message if logger is available."""
        if self._logger:
            await self._logger.critical(message, **kwargs)

    async def log_exception(self, message: str, exception: Exception, **kwargs: Any) -> None:
        """Log exception if logger is available."""
        if self._logger:
            await self._logger.exception(message, exception=exception, **kwargs)


# Factory functions for common use cases


def create_streaming_logger(
    config: StreamingConfig, log_level: LogLevel = LogLevel.INFO, log_file: Optional[str] = None
) -> StructuredLogger:
    """Create a structured logger for streaming operations."""
    return StructuredLogger(
        config=config,
        log_level=log_level,
        log_file=log_file or f"logs/streaming_{datetime.now().strftime('%Y%m%d')}.log",
    )


def create_contextual_logger(
    base_logger: StructuredLogger,
    session_id: Optional[str] = None,
    request_id: Optional[str] = None,
    operation: Optional[str] = None,
) -> ContextualLogger:
    """Create a contextual logger with common default context."""
    default_context = LogContext(session_id=session_id, request_id=request_id, operation=operation)

    return ContextualLogger(base_logger=base_logger, default_context=default_context)


# Global logger instance for convenience
_global_logger: Optional[StructuredLogger] = None


async def get_global_logger() -> Optional[StructuredLogger]:
    """Get the global logger instance."""
    return _global_logger


async def set_global_logger(logger: StructuredLogger) -> None:
    """Set the global logger instance."""
    global _global_logger
    _global_logger = logger


async def init_global_logger(
    config: StreamingConfig, log_level: LogLevel = LogLevel.INFO, log_file: Optional[str] = None
) -> StructuredLogger:
    """Initialize and set the global logger."""
    logger = create_streaming_logger(config, log_level, log_file)
    await logger.start()
    await set_global_logger(logger)
    return logger


async def shutdown_global_logger() -> None:
    """Shutdown the global logger."""
    global _global_logger
    if _global_logger:
        await _global_logger.stop()
        _global_logger = None
