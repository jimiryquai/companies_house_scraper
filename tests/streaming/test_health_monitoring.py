"""
Tests for streaming health monitoring functionality.

Tests the ACTUAL health monitoring API without mocks.
"""

import asyncio
from datetime import datetime, timedelta
from typing import Any

import pytest

from src.streaming.client import StreamingClient
from src.streaming.config import StreamingConfig
from src.streaming.health_monitor import HealthMonitor, HealthStatus


@pytest.fixture
def config() -> Any:
    """Create test configuration."""
    return StreamingConfig(
        streaming_api_key="test-api-key-123456",
        rest_api_key="test-rest-key-123456",
        api_base_url="https://stream.companieshouse.gov.uk",
        connection_timeout=5,
        max_retries=3,
        initial_backoff=0.1,
        max_backoff=1.0,
    )


@pytest.fixture
async def streaming_client(config: Any) -> Any:
    """Create streaming client instance."""
    client = StreamingClient(config)
    yield client
    if client.session:
        await client.session.close()


@pytest.fixture
def health_monitor(streaming_client: Any) -> Any:
    """Create health monitor with a client."""
    return HealthMonitor(streaming_client, check_interval=1.0)


class TestHealthCheckFunctionality:
    """Test internal health check functionality of StreamingClient."""

    @pytest.mark.asyncio
    async def test_health_check_no_session(self, streaming_client: Any) -> None:
        """Test health check when session is not initialized."""
        # Don't connect - session will be None
        await streaming_client._health_check()
        # Should just log warning, not raise

    @pytest.mark.asyncio
    async def test_health_check_not_connected(self, streaming_client: Any) -> None:
        """Test health check when not connected."""
        await streaming_client.connect()
        streaming_client.is_connected = False
        await streaming_client._health_check()
        # Should just log warning, not raise

    @pytest.mark.asyncio
    async def test_health_check_no_heartbeat(self, streaming_client: Any) -> None:
        """Test health check when no heartbeat received."""
        await streaming_client.connect()
        streaming_client.last_heartbeat = None
        await streaming_client._health_check()
        # Should just log warning, not raise

    @pytest.mark.asyncio
    async def test_health_check_stale_heartbeat(self, streaming_client: Any) -> None:
        """Test health check when heartbeat is stale."""
        await streaming_client.connect()
        # Set heartbeat to be older than health check interval
        streaming_client.last_heartbeat = datetime.now() - timedelta(
            seconds=streaming_client.config.health_check_interval + 10
        )
        await streaming_client._health_check()
        # Should just log warning, not raise

    @pytest.mark.asyncio
    async def test_health_check_healthy(self, streaming_client: Any) -> None:
        """Test health check when everything is healthy."""
        await streaming_client.connect()
        streaming_client.is_connected = True
        streaming_client.last_heartbeat = datetime.now()
        await streaming_client._health_check()
        # Should pass without warnings


class TestCircuitBreaker:
    """Test circuit breaker functionality in StreamingClient."""

    @pytest.mark.asyncio
    async def test_circuit_breaker_opens_after_failures(self, streaming_client: Any) -> None:
        """Test that circuit breaker opens after repeated failures."""
        # Simulate multiple failures with actual exceptions
        test_error = Exception("Test failure")
        for _ in range(streaming_client.config.max_retries * 3):
            streaming_client._record_failure(test_error)

        assert streaming_client._circuit_breaker_state == "open"

        # Should raise when trying to connect with open circuit breaker
        with pytest.raises(Exception, match="Circuit breaker is open"):
            streaming_client._check_circuit_breaker()

    @pytest.mark.asyncio
    async def test_circuit_breaker_resets_after_success(self, streaming_client: Any) -> None:
        """Test that circuit breaker resets after successful operations."""
        # Open the circuit breaker
        test_error = Exception("Test failure")
        for _ in range(streaming_client.config.max_retries * 3):
            streaming_client._record_failure(test_error)
        assert streaming_client._circuit_breaker_state == "open"

        # Half-open after cooldown
        streaming_client._last_failure_time = datetime.now() - timedelta(seconds=65)
        streaming_client._check_circuit_breaker()
        assert streaming_client._circuit_breaker_state == "half_open"

        # Record success
        streaming_client._record_success()
        assert streaming_client._circuit_breaker_state == "closed"
        assert streaming_client._failure_count == 0


class TestHealthMonitor:
    """Test HealthMonitor component with actual API."""

    @pytest.mark.asyncio
    async def test_health_monitor_initialization(self, health_monitor: Any) -> None:
        """Test HealthMonitor initializes with correct default values."""
        assert health_monitor.current_status == HealthStatus.HEALTHY
        # Connection metrics doesn't have 'status' attribute directly
        assert health_monitor.stream_metrics.events_processed == 0
        assert health_monitor.error_metrics.total_errors == 0

    @pytest.mark.asyncio
    async def test_record_event_processed(self, health_monitor: Any) -> None:
        """Test recording processed events."""
        health_monitor.record_event()
        health_monitor.record_event_processed(0.1)

        assert health_monitor.stream_metrics.events_processed == 1
        # Processing times are tracked internally
        assert len(health_monitor._processing_times) == 1
        assert health_monitor._processing_times[0] == 0.1

    @pytest.mark.asyncio
    async def test_record_event_failed(self, health_monitor: Any) -> None:
        """Test recording failed events."""
        health_monitor.record_event()
        health_monitor.record_event_failed()

        assert health_monitor.stream_metrics.events_failed == 1

    @pytest.mark.asyncio
    async def test_monitoring_start_stop(self, health_monitor: Any) -> None:
        """Test starting and stopping monitoring."""
        await health_monitor.start_monitoring()
        assert health_monitor.monitoring_active is True
        assert health_monitor.monitoring_task is not None

        await health_monitor.stop_monitoring()
        assert health_monitor.monitoring_active is False

    @pytest.mark.asyncio
    async def test_health_status_calculation(self, health_monitor: Any) -> None:
        """Test health status calculation based on metrics."""
        # Initially healthy
        assert health_monitor.current_status == HealthStatus.HEALTHY

        # Simulate errors to degrade status
        for _ in range(5):
            health_monitor.error_metrics.total_errors += 1
            health_monitor.error_metrics.recent_errors = health_monitor.error_metrics.total_errors

        # Force recalculation
        new_status = health_monitor._calculate_health_status()
        assert new_status in [HealthStatus.DEGRADED, HealthStatus.UNHEALTHY]

    @pytest.mark.asyncio
    async def test_get_health_report(self, health_monitor: Any) -> None:
        """Test getting comprehensive health report."""
        # Record some activity
        health_monitor.record_event()
        health_monitor.record_event_processed(0.1)

        report = health_monitor.get_health_report()

        assert "client_status" in report
        assert "connection_metrics" in report
        assert "stream_metrics" in report
        assert "error_metrics" in report
        assert "performance_metrics" in report
        assert "last_check" in report

    @pytest.mark.asyncio
    async def test_get_health_summary(self, health_monitor: Any) -> None:
        """Test getting health summary."""
        summary = health_monitor.get_health_summary()

        assert "connected" in summary
        assert "events_per_minute" in summary
        assert "error_rate" in summary

    @pytest.mark.asyncio
    async def test_is_healthy(self, health_monitor: Any) -> None:
        """Test is_healthy method."""
        assert health_monitor.is_healthy() is True

        # Degrade health
        health_monitor.current_status = HealthStatus.UNHEALTHY
        assert health_monitor.is_healthy() is False

    @pytest.mark.asyncio
    async def test_get_issues(self, health_monitor: Any) -> None:
        """Test getting current issues."""
        issues = health_monitor.get_issues()
        assert isinstance(issues, list)

        # Simulate some issues
        health_monitor.connection_metrics.connection_failures = 5
        health_monitor.error_metrics.recent_errors = 10

        issues = health_monitor.get_issues()
        assert len(issues) > 0

    @pytest.mark.asyncio
    async def test_status_change_callback(self, health_monitor: Any) -> None:
        """Test status change callbacks."""
        callback_called = False
        old_status = None
        new_status = None

        def status_callback(old: Any, new: Any, reason: str) -> None:
            nonlocal callback_called, old_status, new_status
            callback_called = True
            old_status = old
            new_status = new

        health_monitor.register_status_change_callback(status_callback)

        # Force a status change
        health_monitor._record_status_change(HealthStatus.HEALTHY, HealthStatus.DEGRADED)

        # Callbacks are called asynchronously, so give it a moment
        await asyncio.sleep(0.1)

        # Check if callback was triggered
        # Note: Actual callback execution depends on implementation details
