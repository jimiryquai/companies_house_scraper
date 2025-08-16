"""Log filtering and sampling functionality for streaming module.

Provides configurable filtering and sampling to manage log volume and noise.
"""

import random
import re
from abc import ABC, abstractmethod
from collections import deque
from datetime import datetime, timedelta
from enum import Enum
from re import Pattern
from typing import Any, Optional

from .structured_logger import LogContext, LogEntry, LogLevel, StructuredLogger


class FilteringError(Exception):
    """Error in filtering system."""

    pass


class LogFilter(ABC):
    """Base class for log filters."""

    @abstractmethod
    def should_log(self, log_entry: LogEntry) -> bool:
        """Determine if a log entry should be logged.

        Returns True if the entry should be logged, False otherwise.
        """
        pass


class LogSampler(ABC):
    """Base class for log samplers."""

    @abstractmethod
    def should_sample(self, log_entry: LogEntry) -> bool:
        """Determine if a log entry should be sampled (kept).

        Returns True if the entry should be kept, False if it should be dropped.
        """
        pass


class LevelFilter(LogFilter):
    """Filter based on log level."""

    def __init__(self, min_level: LogLevel):
        """Initialize level filter."""
        self.min_level = min_level

    def should_log(self, log_entry: LogEntry) -> bool:
        """Check if log entry meets minimum level requirement."""
        return log_entry.level.to_logging_level() >= self.min_level.to_logging_level()


class PatternFilter(LogFilter):
    """Filter based on message patterns."""

    def __init__(
        self,
        include_patterns: Optional[list[str]] = None,
        exclude_patterns: Optional[list[str]] = None,
    ):
        """Initialize pattern filter."""
        self.include_patterns: list[Pattern[str]] = []
        self.exclude_patterns: list[Pattern[str]] = []

        if include_patterns:
            self.include_patterns = [
                re.compile(pattern, re.IGNORECASE) for pattern in include_patterns
            ]

        if exclude_patterns:
            self.exclude_patterns = [
                re.compile(pattern, re.IGNORECASE) for pattern in exclude_patterns
            ]

    def should_log(self, log_entry: LogEntry) -> bool:
        """Check if log entry matches include/exclude patterns."""
        message = log_entry.message

        # Check exclude patterns first
        for pattern in self.exclude_patterns:
            if pattern.search(message):
                return False

        # If no include patterns, allow by default
        if not self.include_patterns:
            return True

        # Check include patterns
        return any(pattern.search(message) for pattern in self.include_patterns)


class RateLimitFilter(LogFilter):
    """Filter based on rate limiting."""

    def __init__(self, max_logs_per_minute: int, burst_size: int = 10):
        """Initialize rate limit filter."""
        self.max_logs_per_minute = max_logs_per_minute
        self.burst_size = burst_size

        # State tracking
        self.current_count = 0
        self.burst_count = 0
        self.last_reset = datetime.now()
        self.blocked_count = 0

    def should_log(self, log_entry: LogEntry) -> bool:  # noqa: ARG002
        """Check if log entry is within rate limits."""
        now = datetime.now()

        # Reset counters if a minute has passed
        if (now - self.last_reset).total_seconds() >= 60:
            self.current_count = 0
            self.burst_count = 0
            self.last_reset = now

        # Check burst limit and rate limit together
        if self.burst_count >= self.burst_size and self.current_count >= self.max_logs_per_minute:
            self.blocked_count += 1
            return False

        # Allow the log
        self.current_count += 1
        self.burst_count += 1

        # Reset burst counter periodically
        if (now - self.last_reset).total_seconds() >= 10:
            self.burst_count = 0

        return True


class ContextFilter(LogFilter):
    """Filter based on log context."""

    def __init__(
        self,
        required_context: Optional[dict[str, Any]] = None,
        excluded_context: Optional[dict[str, Any]] = None,
    ):
        """Initialize context filter."""
        self.required_context = required_context or {}
        self.excluded_context = excluded_context or {}

    def should_log(self, log_entry: LogEntry) -> bool:
        """Check if log entry matches context requirements."""
        context_dict = log_entry.context.to_dict()

        # Check excluded context
        for key, value in self.excluded_context.items():
            if context_dict.get(key) == value:
                return False

        # Check required context
        return all(context_dict.get(key) == value for key, value in self.required_context.items())


class SamplingStrategy(Enum):
    """Sampling strategies."""

    RANDOM = "random"
    TIME_BASED = "time_based"
    VOLUME_BASED = "volume_based"


class RandomSampler(LogSampler):
    """Random sampling based on sample rate."""

    def __init__(self, sample_rate: float):
        """Initialize random sampler."""
        if not 0.0 <= sample_rate <= 1.0:
            raise FilteringError("Sample rate must be between 0.0 and 1.0")

        self.sample_rate = sample_rate

    def should_sample(self, log_entry: LogEntry) -> bool:  # noqa: ARG002
        """Randomly sample based on sample rate."""
        return random.random() < self.sample_rate  # noqa: S311


class TimeBasedSampler(LogSampler):
    """Time-based sampling with fixed intervals."""

    def __init__(self, interval_seconds: int, max_per_interval: int):
        """Initialize time-based sampler."""
        self.interval_seconds = interval_seconds
        self.max_per_interval = max_per_interval

        # State tracking
        self.current_interval_start = datetime.now()
        self.current_interval_count = 0

    def should_sample(self, log_entry: LogEntry) -> bool:  # noqa: ARG002
        """Sample based on time intervals."""
        now = datetime.now()

        # Check if we need to start a new interval
        if (now - self.current_interval_start).total_seconds() >= self.interval_seconds:
            self.current_interval_start = now
            self.current_interval_count = 0

        # Check if we're under the limit for this interval
        if self.current_interval_count < self.max_per_interval:
            self.current_interval_count += 1
            return True

        return False


class VolumeBasedSampler(LogSampler):
    """Volume-based adaptive sampling."""

    def __init__(
        self,
        low_volume_threshold: int,
        high_volume_threshold: int,
        low_volume_rate: float,
        high_volume_rate: float,
        window_minutes: int = 5,
    ):
        """Initialize volume-based sampler."""
        self.low_volume_threshold = low_volume_threshold
        self.high_volume_threshold = high_volume_threshold
        self.low_volume_rate = low_volume_rate
        self.high_volume_rate = high_volume_rate
        self.window_minutes = window_minutes

        # Track recent log volume
        self.recent_logs: deque[datetime] = deque()
        self.recent_log_count = 0

    def should_sample(self, log_entry: LogEntry) -> bool:  # noqa: ARG002
        """Sample based on current log volume."""
        now = datetime.now()
        cutoff_time = now - timedelta(minutes=self.window_minutes)

        # Remove old entries
        while self.recent_logs and self.recent_logs[0] < cutoff_time:
            self.recent_logs.popleft()

        # Add current log
        self.recent_logs.append(now)
        self.recent_log_count = len(self.recent_logs)

        # Determine sample rate based on volume
        if self.recent_log_count <= self.low_volume_threshold:
            sample_rate = self.low_volume_rate
        elif self.recent_log_count >= self.high_volume_threshold:
            sample_rate = self.high_volume_rate
        else:
            # Linear interpolation between thresholds
            volume_range = self.high_volume_threshold - self.low_volume_threshold
            rate_range = self.low_volume_rate - self.high_volume_rate
            volume_position = (self.recent_log_count - self.low_volume_threshold) / volume_range
            sample_rate = self.low_volume_rate - (rate_range * volume_position)

        return random.random() < sample_rate  # noqa: S311


class FilteredLogger:
    """Logger that applies filtering and sampling before logging."""

    def __init__(self, base_logger: StructuredLogger):
        """Initialize filtered logger."""
        self.base_logger = base_logger
        self.filters: list[LogFilter] = []
        self.samplers: list[LogSampler] = []

        # Statistics
        self.total_processed = 0
        self.filtered_out = 0
        self.sampled_out = 0
        self.logged = 0

    async def start(self) -> None:
        """Start the filtered logger."""
        await self.base_logger.start()

    async def stop(self) -> None:
        """Stop the filtered logger."""
        await self.base_logger.stop()

    def add_filter(self, filter_obj: LogFilter) -> None:
        """Add a filter to the chain."""
        self.filters.append(filter_obj)

    def add_sampler(self, sampler: LogSampler) -> None:
        """Add a sampler to the chain."""
        self.samplers.append(sampler)

    def remove_filter(self, filter_obj: LogFilter) -> None:
        """Remove a filter from the chain."""
        if filter_obj in self.filters:
            self.filters.remove(filter_obj)

    def remove_sampler(self, sampler: LogSampler) -> None:
        """Remove a sampler from the chain."""
        if sampler in self.samplers:
            self.samplers.remove(sampler)

    async def _should_log(self, log_entry: LogEntry) -> bool:
        """Check if log entry should be logged based on filters and samplers."""
        self.total_processed += 1

        # Apply filters first
        for filter_obj in self.filters:
            if not filter_obj.should_log(log_entry):
                self.filtered_out += 1
                return False

        # Apply samplers
        for sampler in self.samplers:
            if not sampler.should_sample(log_entry):
                self.sampled_out += 1
                return False

        return True

    async def log(
        self,
        level: LogLevel,
        message: str,
        context: Optional[LogContext] = None,
        extra_data: Optional[dict[str, Any]] = None,
        exception: Optional[Exception] = None,
    ) -> None:
        """Log a message through filters and samplers."""
        # Create log entry for filtering
        log_entry = LogEntry(
            timestamp=datetime.now(),
            level=level,
            message=message,
            context=context or LogContext(),
            extra_data=extra_data,
        )

        # Check if should log
        if await self._should_log(log_entry):
            self.logged += 1
            await self.base_logger.log(level, message, context, extra_data, exception)

    async def debug(
        self,
        message: str,
        context: Optional[LogContext] = None,
        extra_data: Optional[dict[str, Any]] = None,
    ) -> None:
        """Log debug message with filtering."""
        await self.log(LogLevel.DEBUG, message, context, extra_data)

    async def info(
        self,
        message: str,
        context: Optional[LogContext] = None,
        extra_data: Optional[dict[str, Any]] = None,
    ) -> None:
        """Log info message with filtering."""
        await self.log(LogLevel.INFO, message, context, extra_data)

    async def warning(
        self,
        message: str,
        context: Optional[LogContext] = None,
        extra_data: Optional[dict[str, Any]] = None,
    ) -> None:
        """Log warning message with filtering."""
        await self.log(LogLevel.WARNING, message, context, extra_data)

    async def error(
        self,
        message: str,
        context: Optional[LogContext] = None,
        extra_data: Optional[dict[str, Any]] = None,
        exception: Optional[Exception] = None,
    ) -> None:
        """Log error message with filtering."""
        await self.log(LogLevel.ERROR, message, context, extra_data, exception)

    async def critical(
        self,
        message: str,
        context: Optional[LogContext] = None,
        extra_data: Optional[dict[str, Any]] = None,
        exception: Optional[Exception] = None,
    ) -> None:
        """Log critical message with filtering."""
        await self.log(LogLevel.CRITICAL, message, context, extra_data, exception)

    async def exception(
        self,
        message: str,
        context: Optional[LogContext] = None,
        extra_data: Optional[dict[str, Any]] = None,
        exception: Optional[Exception] = None,
    ) -> None:
        """Log exception with filtering."""
        await self.log(LogLevel.ERROR, message, context, extra_data, exception)

    def get_statistics(self) -> dict[str, Any]:
        """Get filtering and sampling statistics."""
        return {
            "total_processed": self.total_processed,
            "filtered_out": self.filtered_out,
            "sampled_out": self.sampled_out,
            "logged": self.logged,
            "filter_rate": self.filtered_out / max(self.total_processed, 1),
            "sample_rate": self.sampled_out / max(self.total_processed, 1),
            "pass_through_rate": self.logged / max(self.total_processed, 1),
        }

    def reset_statistics(self) -> None:
        """Reset statistics counters."""
        self.total_processed = 0
        self.filtered_out = 0
        self.sampled_out = 0
        self.logged = 0


# Factory functions for common configurations


def create_production_filters() -> list[LogFilter]:
    """Create filters for production environment."""
    return [
        LevelFilter(min_level=LogLevel.INFO),  # No debug logs in production
        RateLimitFilter(max_logs_per_minute=1000, burst_size=50),  # Rate limiting
        PatternFilter(
            exclude_patterns=[
                r".*health.*check.*",  # Exclude health check noise
                r".*heartbeat.*",  # Exclude heartbeat messages
            ]
        ),
    ]


def create_development_filters() -> list[LogFilter]:
    """Create filters for development environment."""
    return [
        LevelFilter(min_level=LogLevel.DEBUG),  # Allow all levels in dev
        RateLimitFilter(max_logs_per_minute=10000, burst_size=100),  # Higher limits
    ]


def create_adaptive_sampler() -> VolumeBasedSampler:
    """Create adaptive sampler for varying load conditions."""
    return VolumeBasedSampler(
        low_volume_threshold=50,
        high_volume_threshold=500,
        low_volume_rate=1.0,  # 100% sampling during low volume
        high_volume_rate=0.1,  # 10% sampling during high volume
        window_minutes=5,
    )


def create_debug_sampler() -> RandomSampler:
    """Create sampler for debug logs to reduce noise."""
    return RandomSampler(sample_rate=0.1)  # Sample 10% of debug logs


def setup_filtered_logger(
    base_logger: StructuredLogger, environment: str = "production"
) -> FilteredLogger:
    """Set up filtered logger with common configuration."""
    filtered_logger = FilteredLogger(base_logger)

    if environment == "production":
        # Production configuration
        for filter_obj in create_production_filters():
            filtered_logger.add_filter(filter_obj)

        filtered_logger.add_sampler(create_adaptive_sampler())

    elif environment == "development":
        # Development configuration
        for filter_obj in create_development_filters():
            filtered_logger.add_filter(filter_obj)

        # Light sampling for debug logs only
        # Debug filter and sampler available if needed
        pass

    elif environment == "testing":
        # Testing configuration - minimal filtering
        filtered_logger.add_filter(LevelFilter(min_level=LogLevel.INFO))
        filtered_logger.add_sampler(RandomSampler(sample_rate=1.0))  # No sampling

    return filtered_logger
