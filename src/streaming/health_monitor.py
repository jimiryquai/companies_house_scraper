"""Stream Health Monitoring.

Provides comprehensive health monitoring capabilities for the streaming client,
including connection health, performance metrics, and status reporting.
"""

import asyncio
import contextlib
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Optional

if TYPE_CHECKING:
    import asyncio
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health status levels."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    CRITICAL = "critical"


@dataclass
class ConnectionMetrics:
    """Connection-related metrics."""

    connection_attempts: int = 0
    successful_connections: int = 0
    connection_failures: int = 0
    last_connection_time: Optional[datetime] = None
    last_connection_failure: Optional[datetime] = None
    average_connection_time: float = 0.0
    connection_uptime: float = 0.0


@dataclass
class StreamMetrics:
    """Streaming-related metrics."""

    events_received: int = 0
    events_processed: int = 0
    events_failed: int = 0
    last_event_time: Optional[datetime] = None
    events_per_minute: float = 0.0
    average_processing_time: float = 0.0
    bytes_received: int = 0


@dataclass
class ErrorMetrics:
    """Error-related metrics."""

    total_errors: int = 0
    error_types: dict[str, int] = field(default_factory=dict)
    last_error_time: Optional[datetime] = None
    error_rate_per_minute: float = 0.0
    critical_errors: int = 0


@dataclass
class PerformanceMetrics:
    """Performance-related metrics."""

    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    network_latency_ms: float = 0.0
    queue_size: int = 0
    processing_backlog: int = 0


class HealthMonitor:
    """Comprehensive health monitor for streaming client.

    Monitors connection health, performance metrics, error rates,
    and provides health status reporting and alerting.
    """

    def __init__(self, client: Any, check_interval: float = 30.0):
        """Initialize health monitor.

        Args:
            client: StreamingClient instance to monitor
            check_interval: Health check interval in seconds
        """
        self.client = client
        self.check_interval = check_interval

        # Metrics
        self.connection_metrics = ConnectionMetrics()
        self.stream_metrics = StreamMetrics()
        self.error_metrics = ErrorMetrics()
        self.performance_metrics = PerformanceMetrics()

        # Health status
        self.current_status = HealthStatus.HEALTHY
        self.status_history: list[
            tuple[datetime, HealthStatus, str]
        ] = []  # (timestamp, status, reason)

        # Monitoring state
        self.monitoring_active = False
        self.monitoring_task: Optional[asyncio.Task[None]] = None
        self.last_health_check = datetime.now()

        # Thresholds for health status determination
        self.thresholds = {
            "max_connection_failures": 3,
            "max_error_rate_per_minute": 10.0,
            "max_processing_delay_seconds": 60.0,
            "min_events_per_minute": 0.1,  # Minimum expected event rate
            "max_memory_usage_mb": 200.0,
            "max_queue_size": 1000,
        }

        # Callbacks for health status changes
        self.status_change_callbacks: list[Callable[..., None]] = []

        # Event timing tracking
        self._event_timestamps: list[datetime] = []
        self._processing_times: list[float] = []

    def register_status_change_callback(self, callback: Callable[..., None]) -> None:
        """Register callback for health status changes."""
        self.status_change_callbacks.append(callback)
        logger.info("Registered health status change callback")

    async def start_monitoring(self) -> None:
        """Start the health monitoring loop."""
        if self.monitoring_active:
            logger.warning("Health monitoring is already active")
            return

        self.monitoring_active = True
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())
        logger.info(f"Started health monitoring with {self.check_interval}s interval")

    async def stop_monitoring(self) -> None:
        """Stop the health monitoring loop."""
        if not self.monitoring_active:
            return

        self.monitoring_active = False
        if self.monitoring_task:
            self.monitoring_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.monitoring_task

        logger.info("Stopped health monitoring")

    async def _monitoring_loop(self) -> None:
        """Main monitoring loop."""
        while self.monitoring_active:
            try:
                await self._perform_health_check()
                await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}", exc_info=True)
                await asyncio.sleep(self.check_interval)

    async def _perform_health_check(self) -> None:
        """Perform comprehensive health check."""
        try:
            # Update metrics
            await self._update_connection_metrics()
            self._update_stream_metrics()
            self._update_error_metrics()
            await self._update_performance_metrics()

            # Determine health status
            old_status = self.current_status
            self.current_status = self._calculate_health_status()

            # Record status change
            if old_status != self.current_status:
                self._record_status_change(old_status, self.current_status)
                await self._notify_status_change(old_status, self.current_status)

            self.last_health_check = datetime.now()

        except Exception as e:
            logger.error(f"Health check failed: {e}", exc_info=True)
            self.current_status = HealthStatus.CRITICAL

    async def _update_connection_metrics(self) -> None:
        """Update connection-related metrics."""
        if hasattr(self.client, "_connection_attempts"):
            self.connection_metrics.connection_attempts = self.client._connection_attempts

        if self.client.is_connected and not self.connection_metrics.last_connection_time:
            self.connection_metrics.successful_connections += 1
            self.connection_metrics.last_connection_time = datetime.now()

        # Update connection uptime
        if self.connection_metrics.last_connection_time:
            uptime = (datetime.now() - self.connection_metrics.last_connection_time).total_seconds()
            self.connection_metrics.connection_uptime = uptime

    def _update_stream_metrics(self) -> None:
        """Update streaming-related metrics."""
        # Update last event time from client heartbeat
        if self.client.last_heartbeat:
            self.stream_metrics.last_event_time = self.client.last_heartbeat

        # Calculate events per minute
        now = datetime.now()
        # Clean old timestamps (older than 1 minute)
        self._event_timestamps = [
            ts for ts in self._event_timestamps if (now - ts).total_seconds() < 60
        ]

        self.stream_metrics.events_per_minute = len(self._event_timestamps)

        # Calculate average processing time
        if self._processing_times:
            self.stream_metrics.average_processing_time = sum(self._processing_times) / len(
                self._processing_times
            )
            # Keep only recent processing times
            if len(self._processing_times) > 100:
                self._processing_times = self._processing_times[-100:]

    def _update_error_metrics(self) -> None:
        """Update error-related metrics."""
        # Get error metrics from client
        if hasattr(self.client, "get_error_metrics"):
            client_errors = self.client.get_error_metrics()
            self.error_metrics.total_errors = client_errors["total_errors"]
            self.error_metrics.error_types = client_errors["error_types"]
            self.error_metrics.error_rate_per_minute = client_errors["error_rate"]

            if client_errors["last_error_time"]:
                self.error_metrics.last_error_time = datetime.fromisoformat(
                    client_errors["last_error_time"]
                )

    async def _update_performance_metrics(self) -> None:
        """Update performance-related metrics."""
        try:
            import os

            import psutil

            # Get current process
            process = psutil.Process(os.getpid())

            # Memory usage
            memory_info = process.memory_info()
            self.performance_metrics.memory_usage_mb = memory_info.rss / 1024 / 1024

            # CPU usage (non-blocking call)
            self.performance_metrics.cpu_usage_percent = process.cpu_percent()

            # Network latency to streaming API (if connected)
            if self.client.is_connected and hasattr(self.client, "config"):
                latency = await self._measure_api_latency()
                if latency is not None:
                    self.performance_metrics.network_latency_ms = latency

            # Update queue size if client has queue information
            if hasattr(self.client, "_event_queue"):
                self.performance_metrics.queue_size = getattr(
                    self.client._event_queue, "qsize", lambda: 0
                )()

            # Processing backlog estimation
            if hasattr(self.client, "_processing_backlog"):
                self.performance_metrics.processing_backlog = self.client._processing_backlog

        except ImportError:
            # psutil not available, skip performance metrics
            logger.debug("psutil not available, skipping system performance metrics")
        except Exception as e:
            logger.debug(f"Error updating performance metrics: {e}")

    async def _measure_api_latency(self) -> Optional[float]:
        """Measure API latency."""
        return None  # Placeholder implementation

    async def _original_measure_api_latency(self) -> Optional[float]:
        """Measure network latency to the streaming API endpoint.

        Returns:
            Latency in milliseconds, or None if measurement fails
        """
        try:
            import time
            from urllib.parse import urlparse

            if not hasattr(self.client, "session") or not self.client.session:
                return None

            api_url = self.client.config.api_base_url
            parsed_url = urlparse(api_url)
            health_url = f"{parsed_url.scheme}://{parsed_url.netloc}/healthcheck"

            # Measure round-trip time for a simple HEAD request
            start_time = time.time()

            try:
                async with self.client.session.head(health_url, timeout=5) as response:
                    end_time = time.time()

                    if response.status in [200, 404]:  # 404 is fine for latency test
                        latency_ms = (end_time - start_time) * 1000
                        return round(latency_ms, 2)

            except asyncio.TimeoutError:
                return 5000.0  # 5 second timeout
            except Exception:
                return None

        except Exception as e:
            logger.debug(f"Error measuring API latency: {e}")
            return None

        return None  # Fallback return

    def collect_event_processing_metrics(self, processing_time: float, success: bool) -> None:
        """Collect metrics about event processing performance.

        Args:
            processing_time: Time taken to process event in seconds
            success: Whether the event processing was successful
        """
        # Update processing times
        self._processing_times.append(processing_time)

        # Update stream metrics
        if success:
            self.stream_metrics.events_processed += 1
        else:
            self.stream_metrics.events_failed += 1

        # Estimate processing backlog based on processing rate
        if len(self._processing_times) >= 10:
            avg_processing_time = sum(self._processing_times[-10:]) / 10
            # If processing is slower than event arrival, we have backlog
            if avg_processing_time > 1.0:  # Taking more than 1 second per event
                self.performance_metrics.processing_backlog += 1
            elif self.performance_metrics.processing_backlog > 0:
                self.performance_metrics.processing_backlog -= 1

    def collect_connection_performance(self, connection_time: float, success: bool) -> None:
        """Collect metrics about connection performance.

        Args:
            connection_time: Time taken to establish connection in seconds
            success: Whether the connection was successful
        """
        if success:
            self.connection_metrics.successful_connections += 1
            self.connection_metrics.average_connection_time = (
                self.connection_metrics.average_connection_time
                * (self.connection_metrics.successful_connections - 1)
                + connection_time
            ) / self.connection_metrics.successful_connections
        else:
            self.connection_metrics.connection_failures += 1

        self.connection_metrics.connection_attempts += 1

    def get_performance_summary(self) -> dict[str, Any]:
        """Get summary of key performance metrics."""
        return {
            "memory_usage_mb": self.performance_metrics.memory_usage_mb,
            "cpu_usage_percent": self.performance_metrics.cpu_usage_percent,
            "network_latency_ms": self.performance_metrics.network_latency_ms,
            "queue_size": self.performance_metrics.queue_size,
            "processing_backlog": self.performance_metrics.processing_backlog,
            "events_per_minute": self.stream_metrics.events_per_minute,
            "average_processing_time_ms": self.stream_metrics.average_processing_time * 1000,
            "connection_success_rate": (
                self.connection_metrics.successful_connections
                / max(self.connection_metrics.connection_attempts, 1)
            ),
            "average_connection_time_ms": self.connection_metrics.average_connection_time * 1000,
        }

    def reset_performance_counters(self) -> None:
        """Reset performance counters for a fresh measurement period."""
        # Reset stream metrics
        self.stream_metrics.events_received = 0
        self.stream_metrics.events_processed = 0
        self.stream_metrics.events_failed = 0

        # Reset connection metrics
        self.connection_metrics.connection_attempts = 0
        self.connection_metrics.successful_connections = 0
        self.connection_metrics.connection_failures = 0

        # Reset error metrics
        self.error_metrics.total_errors = 0
        self.error_metrics.error_types = {}

        # Clear processing times and event timestamps
        self._processing_times.clear()
        self._event_timestamps.clear()

        logger.info("Performance counters reset")

    def _calculate_health_status(self) -> HealthStatus:  # noqa: C901
        """Calculate overall health status based on metrics."""
        issues = []

        # Check connection health
        if not self.client.is_connected:
            issues.append("Not connected to streaming API")

        # Check error rate
        if self.error_metrics.error_rate_per_minute > self.thresholds["max_error_rate_per_minute"]:
            issues.append(f"High error rate: {self.error_metrics.error_rate_per_minute:.2f}/min")

        # Check event processing
        if self.stream_metrics.last_event_time:
            time_since_last_event = (
                datetime.now() - self.stream_metrics.last_event_time
            ).total_seconds()
            if time_since_last_event > self.thresholds["max_processing_delay_seconds"]:
                issues.append(f"No events received for {time_since_last_event:.0f}s")

        # Check event rate (if we expect regular events)
        if (
            self.stream_metrics.events_per_minute < self.thresholds["min_events_per_minute"]
            and self.client.is_connected
        ):
            issues.append(f"Low event rate: {self.stream_metrics.events_per_minute:.2f}/min")

        # Check memory usage
        if self.performance_metrics.memory_usage_mb > self.thresholds["max_memory_usage_mb"]:
            issues.append(f"High memory usage: {self.performance_metrics.memory_usage_mb:.1f}MB")

        # Check circuit breaker state
        if (
            hasattr(self.client, "_circuit_breaker_state")
            and self.client._circuit_breaker_state == "open"
        ):
            issues.append("Circuit breaker is open")

        # Check degraded mode
        if hasattr(self.client, "is_degraded_mode") and self.client.is_degraded_mode:
            issues.append("Client is in degraded mode")

        # Determine status based on issues
        if not issues:
            return HealthStatus.HEALTHY
        if len(issues) == 1 and "degraded mode" in issues[0].lower() or len(issues) <= 2:
            return HealthStatus.DEGRADED
        return HealthStatus.UNHEALTHY

    def _record_status_change(self, old_status: HealthStatus, new_status: HealthStatus) -> None:
        """Record health status change."""
        timestamp = datetime.now()
        reason = f"Status changed from {old_status.value} to {new_status.value}"

        self.status_history.append((timestamp, new_status, reason))

        # Keep only recent history (last 100 entries)
        if len(self.status_history) > 100:
            self.status_history = self.status_history[-100:]

        logger.info(f"Health status changed: {old_status.value} -> {new_status.value}")

    async def _notify_status_change(
        self, old_status: HealthStatus, new_status: HealthStatus
    ) -> None:
        """Notify registered callbacks about status changes."""
        for callback in self.status_change_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(old_status, new_status)
                else:
                    callback(old_status, new_status)
            except Exception as e:
                logger.error(f"Error in status change callback: {e}")

    def record_event(self) -> None:
        """Record that an event was received."""
        self._event_timestamps.append(datetime.now())
        self.stream_metrics.events_received += 1

    def record_event_processed(self, processing_time: float) -> None:
        """Record that an event was processed."""
        self.stream_metrics.events_processed += 1
        self._processing_times.append(processing_time)

    def record_event_failed(self) -> None:
        """Record that an event processing failed."""
        self.stream_metrics.events_failed += 1

    def get_health_report(self) -> dict[str, Any]:
        """Get comprehensive health report."""
        return {
            "timestamp": datetime.now().isoformat(),
            "overall_status": self.current_status.value,
            "last_check": self.last_health_check.isoformat(),
            "connection_metrics": {
                "attempts": self.connection_metrics.connection_attempts,
                "successes": self.connection_metrics.successful_connections,
                "failures": self.connection_metrics.connection_failures,
                "uptime_seconds": self.connection_metrics.connection_uptime,
                "last_connection": self.connection_metrics.last_connection_time.isoformat()
                if self.connection_metrics.last_connection_time
                else None,
            },
            "stream_metrics": {
                "events_received": self.stream_metrics.events_received,
                "events_processed": self.stream_metrics.events_processed,
                "events_failed": self.stream_metrics.events_failed,
                "events_per_minute": self.stream_metrics.events_per_minute,
                "avg_processing_time_ms": self.stream_metrics.average_processing_time * 1000,
                "last_event": self.stream_metrics.last_event_time.isoformat()
                if self.stream_metrics.last_event_time
                else None,
            },
            "error_metrics": {
                "total_errors": self.error_metrics.total_errors,
                "error_types": self.error_metrics.error_types,
                "error_rate_per_minute": self.error_metrics.error_rate_per_minute,
                "last_error": self.error_metrics.last_error_time.isoformat()
                if self.error_metrics.last_error_time
                else None,
            },
            "performance_metrics": {
                "memory_usage_mb": self.performance_metrics.memory_usage_mb,
                "cpu_usage_percent": self.performance_metrics.cpu_usage_percent,
                "network_latency_ms": self.performance_metrics.network_latency_ms,
                "queue_size": self.performance_metrics.queue_size,
            },
            "client_status": {
                "connected": self.client.is_connected,
                "degraded_mode": getattr(self.client, "is_degraded_mode", False),
                "polling_mode": getattr(self.client, "is_polling_mode", False),
                "circuit_breaker_state": getattr(self.client, "_circuit_breaker_state", "unknown"),
            },
            "thresholds": self.thresholds,
            "recent_status_changes": [
                {"timestamp": ts.isoformat(), "status": status.value, "reason": reason}
                for ts, status, reason in self.status_history[-10:]
            ],
        }

    def get_health_summary(self) -> dict[str, Any]:
        """Get concise health summary."""
        return {
            "status": self.current_status.value,
            "connected": self.client.is_connected,
            "events_per_minute": self.stream_metrics.events_per_minute,
            "error_rate": self.error_metrics.error_rate_per_minute,
            "memory_mb": self.performance_metrics.memory_usage_mb,
            "last_event_age_seconds": (
                (datetime.now() - self.stream_metrics.last_event_time).total_seconds()
                if self.stream_metrics.last_event_time
                else None
            ),
            "uptime_seconds": self.connection_metrics.connection_uptime,
        }

    def is_healthy(self) -> bool:
        """Check if the system is healthy."""
        return self.current_status in [HealthStatus.HEALTHY, HealthStatus.DEGRADED]

    def get_issues(self) -> list[str]:
        """Get list of current health issues."""
        issues = []

        if not self.client.is_connected:
            issues.append("Not connected")

        if self.error_metrics.error_rate_per_minute > self.thresholds["max_error_rate_per_minute"]:
            issues.append(f"High error rate ({self.error_metrics.error_rate_per_minute:.1f}/min)")

        if self.stream_metrics.last_event_time:
            age = (datetime.now() - self.stream_metrics.last_event_time).total_seconds()
            if age > self.thresholds["max_processing_delay_seconds"]:
                issues.append(f"No events for {age:.0f}s")

        if self.performance_metrics.memory_usage_mb > self.thresholds["max_memory_usage_mb"]:
            issues.append(f"High memory usage ({self.performance_metrics.memory_usage_mb:.0f}MB)")

        if getattr(self.client, "is_degraded_mode", False):
            issues.append("Degraded mode")

        if getattr(self.client, "_circuit_breaker_state", None) == "open":
            issues.append("Circuit breaker open")

        return issues
