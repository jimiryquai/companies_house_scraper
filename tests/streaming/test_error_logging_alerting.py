"""
Tests for error logging and alerting functionality in streaming module.
"""

import os
import smtplib
import tempfile
from datetime import datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.streaming.config import StreamingConfig
from src.streaming.error_alerting import (
    AlertLevel,
    AlertThreshold,
    EmailAlertChannel,
    ErrorAlertManager,
    ErrorPattern,
    LogAlertChannel,
    SlackAlertChannel,
    WebhookAlertChannel,
)
from src.streaming.structured_logger import LogContext, LogEntry, LogLevel, StructuredLogger


@pytest.fixture
def config() -> Any:
    """Create test configuration."""
    return StreamingConfig(
        streaming_api_key="test-api-key-123456",
        rest_api_key="test-rest-api-key-123456",
        database_path=":memory:",
        batch_size=10,
    )


@pytest.fixture
def temp_log_file() -> Any:
    """Create a temporary log file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".log", delete=False) as temp_file:
        temp_path = temp_file.name

    yield temp_path

    # Cleanup
    if os.path.exists(temp_path):
        os.unlink(temp_path)


@pytest.fixture
def sample_error_log_entry() -> Any:
    """Sample error log entry for testing."""
    return LogEntry(
        timestamp=datetime.now(),
        level=LogLevel.ERROR,
        message="API connection failed",
        context=LogContext(request_id="req_123", operation="connect_to_api", retry_count=3),
        extra_data={
            "error_type": "ConnectionTimeout",
            "endpoint": "https://api.companieshouse.gov.uk/streaming",
            "status_code": None,
            "response_time_ms": 30000,
        },
    )


@pytest.fixture
def sample_critical_log_entry() -> Any:
    """Sample critical log entry for testing."""
    return LogEntry(
        timestamp=datetime.now(),
        level=LogLevel.CRITICAL,
        message="Database connection lost",
        context=LogContext(request_id="req_456", operation="database_query"),
        extra_data={
            "error_type": "DatabaseConnectionError",
            "database_path": "/path/to/database.db",
            "connection_attempts": 5,
        },
    )


class TestAlertLevel:
    """Test AlertLevel enum."""

    def test_alert_level_values(self) -> None:
        """Test AlertLevel enum values."""
        assert AlertLevel.LOW.value == "LOW"
        assert AlertLevel.MEDIUM.value == "MEDIUM"
        assert AlertLevel.HIGH.value == "HIGH"
        assert AlertLevel.CRITICAL.value == "CRITICAL"

    def test_alert_level_ordering(self) -> None:
        """Test AlertLevel ordering for priority."""
        levels = [AlertLevel.LOW, AlertLevel.MEDIUM, AlertLevel.HIGH, AlertLevel.CRITICAL]
        priorities = [level.priority() for level in levels]

        assert priorities == [1, 2, 3, 4]
        assert AlertLevel.CRITICAL.priority() > AlertLevel.HIGH.priority()
        assert AlertLevel.MEDIUM.priority() > AlertLevel.LOW.priority()


class TestErrorPattern:
    """Test ErrorPattern functionality."""

    def test_error_pattern_creation(self) -> None:
        """Test ErrorPattern creation."""
        pattern = ErrorPattern(
            name="connection_timeout",
            message_regex=r".*connection.*timeout.*",
            log_level=LogLevel.ERROR,
            error_type_regex=r"Connection.*Error",
            alert_level=AlertLevel.HIGH,
        )

        assert pattern.name == "connection_timeout"
        assert pattern.log_level == LogLevel.ERROR
        assert pattern.alert_level == AlertLevel.HIGH

    def test_error_pattern_matches_message(self, sample_error_log_entry: Any) -> None:
        """Test error pattern matching by message."""
        pattern = ErrorPattern(
            name="api_connection_error",
            message_regex=r".*API.*connection.*failed.*",
            log_level=LogLevel.ERROR,
            alert_level=AlertLevel.HIGH,
        )

        assert pattern.matches(sample_error_log_entry) is True

    def test_error_pattern_matches_error_type(self, sample_error_log_entry: Any) -> None:
        """Test error pattern matching by error type."""
        pattern = ErrorPattern(
            name="timeout_error",
            error_type_regex=r".*Timeout.*",
            log_level=LogLevel.ERROR,
            alert_level=AlertLevel.MEDIUM,
        )

        assert pattern.matches(sample_error_log_entry) is True

    def test_error_pattern_no_match(self, sample_error_log_entry: Any) -> None:
        """Test error pattern that doesn't match."""
        pattern = ErrorPattern(
            name="database_error",
            message_regex=r".*database.*error.*",
            log_level=LogLevel.ERROR,
            alert_level=AlertLevel.HIGH,
        )

        assert pattern.matches(sample_error_log_entry) is False

    def test_error_pattern_log_level_filter(self, sample_error_log_entry: Any) -> None:
        """Test error pattern filtering by log level."""
        # Pattern that requires CRITICAL level
        pattern = ErrorPattern(
            name="critical_only",
            message_regex=r".*",  # Matches all messages
            log_level=LogLevel.CRITICAL,
            alert_level=AlertLevel.CRITICAL,
        )

        # Should not match ERROR level entry
        assert pattern.matches(sample_error_log_entry) is False

    def test_error_pattern_complex_matching(self) -> None:
        """Test error pattern with multiple criteria."""
        pattern = ErrorPattern(
            name="api_timeout",
            message_regex=r".*API.*failed.*",
            error_type_regex=r".*Timeout.*",
            log_level=LogLevel.ERROR,
            alert_level=AlertLevel.HIGH,
            context_filters={"operation": "connect_to_api"},
        )

        log_entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.ERROR,
            message="API connection failed",
            context=LogContext(operation="connect_to_api"),
            extra_data={"error_type": "ConnectionTimeout"},
        )

        assert pattern.matches(log_entry) is True


class TestAlertThreshold:
    """Test AlertThreshold functionality."""

    def test_alert_threshold_creation(self) -> None:
        """Test AlertThreshold creation."""
        threshold = AlertThreshold(
            pattern_name="database_error",
            max_occurrences=5,
            time_window_minutes=10,
            alert_level=AlertLevel.HIGH,
        )

        assert threshold.pattern_name == "database_error"
        assert threshold.max_occurrences == 5
        assert threshold.time_window_minutes == 10
        assert threshold.alert_level == AlertLevel.HIGH

    def test_alert_threshold_check_under_limit(self) -> None:
        """Test threshold check when under the limit."""
        threshold = AlertThreshold(
            pattern_name="test_error",
            max_occurrences=3,
            time_window_minutes=5,
            alert_level=AlertLevel.MEDIUM,
        )

        now = datetime.now()
        recent_occurrences = [now - timedelta(minutes=1), now - timedelta(minutes=2)]

        should_alert, count = threshold.should_alert(recent_occurrences)
        assert should_alert is False
        assert count == 2

    def test_alert_threshold_check_over_limit(self) -> None:
        """Test threshold check when over the limit."""
        threshold = AlertThreshold(
            pattern_name="test_error",
            max_occurrences=2,
            time_window_minutes=5,
            alert_level=AlertLevel.HIGH,
        )

        now = datetime.now()
        recent_occurrences = [
            now - timedelta(minutes=1),
            now - timedelta(minutes=2),
            now - timedelta(minutes=3),
        ]

        should_alert, count = threshold.should_alert(recent_occurrences)
        assert should_alert is True
        assert count == 3

    def test_alert_threshold_time_window_filtering(self) -> None:
        """Test that old occurrences are filtered out."""
        threshold = AlertThreshold(
            pattern_name="test_error",
            max_occurrences=2,
            time_window_minutes=5,
            alert_level=AlertLevel.HIGH,
        )

        now = datetime.now()
        occurrences = [
            now - timedelta(minutes=1),  # Recent
            now - timedelta(minutes=2),  # Recent
            now - timedelta(minutes=10),  # Too old
            now - timedelta(minutes=15),  # Too old
        ]

        should_alert, count = threshold.should_alert(occurrences)
        assert should_alert is False  # Only 2 recent occurrences
        assert count == 2


class TestEmailAlertChannel:
    """Test EmailAlertChannel functionality."""

    @pytest.mark.asyncio
    async def test_email_channel_creation(self) -> None:
        """Test EmailAlertChannel creation."""
        channel = EmailAlertChannel(
            smtp_server="smtp.example.com",
            smtp_port=587,
            username="alerts@example.com",
            password="password123",  # noqa: S106
            from_email="alerts@example.com",
            to_emails=["admin@example.com", "dev@example.com"],
        )

        assert channel.smtp_server == "smtp.example.com"
        assert channel.smtp_port == 587
        assert len(channel.to_emails) == 2

    @patch("smtplib.SMTP")
    @pytest.mark.asyncio
    async def test_email_channel_send_alert(self, mock_smtp: Any) -> None:
        """Test sending email alert."""
        # Setup mock
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_server

        channel = EmailAlertChannel(
            smtp_server="smtp.example.com",
            smtp_port=587,
            username="alerts@example.com",
            password="password123",  # noqa: S106
            from_email="alerts@example.com",
            to_emails=["admin@example.com"],
        )

        alert_data = {
            "pattern_name": "database_error",
            "alert_level": AlertLevel.HIGH,
            "message": "Database connection failed",
            "count": 5,
            "time_window": "10 minutes",
        }

        success = await channel.send_alert(alert_data)

        assert success is True
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with("alerts@example.com", "password123")
        mock_server.send_message.assert_called_once()

    @patch("smtplib.SMTP")
    @pytest.mark.asyncio
    async def test_email_channel_send_alert_failure(self, mock_smtp: Any) -> None:
        """Test email alert sending failure."""
        # Setup mock to raise exception
        mock_smtp.side_effect = smtplib.SMTPException("Connection failed")

        channel = EmailAlertChannel(
            smtp_server="smtp.example.com",
            smtp_port=587,
            username="alerts@example.com",
            password="password123",  # noqa: S106
            from_email="alerts@example.com",
            to_emails=["admin@example.com"],
        )

        alert_data = {
            "pattern_name": "test_error",
            "alert_level": AlertLevel.MEDIUM,
            "message": "Test error",
            "count": 1,
        }

        success = await channel.send_alert(alert_data)
        assert success is False


class TestSlackAlertChannel:
    """Test SlackAlertChannel functionality."""

    @pytest.mark.asyncio
    async def test_slack_channel_creation(self) -> None:
        """Test SlackAlertChannel creation."""
        channel = SlackAlertChannel(
            webhook_url="https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX",
            channel="#alerts",
            username="StreamingAlerts",
        )

        assert "hooks.slack.com" in channel.webhook_url
        assert channel.channel == "#alerts"
        assert channel.username == "StreamingAlerts"

    @patch("aiohttp.ClientSession.post")
    @pytest.mark.asyncio
    async def test_slack_channel_send_alert(self, mock_post: Any) -> None:
        """Test sending Slack alert."""
        # Setup mock response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_post.return_value.__aenter__.return_value = mock_response

        channel = SlackAlertChannel(
            webhook_url="https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX",
            channel="#alerts",
            username="StreamingAlerts",
        )

        alert_data = {
            "pattern_name": "api_error",
            "alert_level": AlertLevel.HIGH,
            "message": "API connection failed",
            "count": 3,
            "time_window": "5 minutes",
        }

        success = await channel.send_alert(alert_data)

        assert success is True
        mock_post.assert_called_once()

    @patch("aiohttp.ClientSession.post")
    @pytest.mark.asyncio
    async def test_slack_channel_send_alert_failure(self, mock_post: Any) -> None:
        """Test Slack alert sending failure."""
        # Setup mock to raise exception
        mock_post.side_effect = Exception("Network error")

        channel = SlackAlertChannel(
            webhook_url="https://hooks.slack.com/services/invalid", channel="#alerts"
        )

        alert_data = {
            "pattern_name": "test_error",
            "alert_level": AlertLevel.MEDIUM,
            "message": "Test error",
            "count": 1,
        }

        success = await channel.send_alert(alert_data)
        assert success is False


class TestWebhookAlertChannel:
    """Test WebhookAlertChannel functionality."""

    @pytest.mark.asyncio
    async def test_webhook_channel_creation(self) -> None:
        """Test WebhookAlertChannel creation."""
        channel = WebhookAlertChannel(
            webhook_url="https://api.example.com/alerts",
            headers={"Authorization": "Bearer token123"},
            timeout_seconds=30,
        )

        assert channel.webhook_url == "https://api.example.com/alerts"
        assert channel.headers["Authorization"] == "Bearer token123"
        assert channel.timeout_seconds == 30

    @patch("aiohttp.ClientSession.post")
    @pytest.mark.asyncio
    async def test_webhook_channel_send_alert(self, mock_post: Any) -> None:
        """Test sending webhook alert."""
        # Setup mock response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_post.return_value.__aenter__.return_value = mock_response

        channel = WebhookAlertChannel(
            webhook_url="https://api.example.com/alerts",
            headers={"Content-Type": "application/json"},
        )

        alert_data = {
            "pattern_name": "system_error",
            "alert_level": AlertLevel.CRITICAL,
            "message": "System failure detected",
            "count": 1,
        }

        success = await channel.send_alert(alert_data)

        assert success is True
        mock_post.assert_called_once()


class TestLogAlertChannel:
    """Test LogAlertChannel functionality."""

    @pytest.mark.asyncio
    async def test_log_channel_creation(self, temp_log_file: Any) -> None:
        """Test LogAlertChannel creation."""
        channel = LogAlertChannel(log_file=temp_log_file, log_level=LogLevel.WARNING)

        assert channel.log_file == temp_log_file
        assert channel.log_level == LogLevel.WARNING

    @pytest.mark.asyncio
    async def test_log_channel_send_alert(self, temp_log_file: Any) -> None:
        """Test logging alert to file."""
        channel = LogAlertChannel(log_file=temp_log_file, log_level=LogLevel.ERROR)

        await channel.start()

        alert_data = {
            "pattern_name": "test_alert",
            "alert_level": AlertLevel.HIGH,
            "message": "Test alert message",
            "count": 5,
            "time_window": "10 minutes",
        }

        success = await channel.send_alert(alert_data)

        await channel.stop()

        assert success is True

        # Verify alert was logged
        with open(temp_log_file) as f:
            content = f.read()
            assert "ALERT" in content
            assert "test_alert" in content
            assert "Test alert message" in content


class TestErrorAlertManager:
    """Test ErrorAlertManager functionality."""

    @pytest.mark.asyncio
    async def test_alert_manager_creation(self, config: Any) -> None:
        """Test ErrorAlertManager creation."""
        manager = ErrorAlertManager(config=config)

        assert manager.config == config
        assert len(manager.patterns) == 0
        assert len(manager.channels) == 0
        assert len(manager.thresholds) == 0

    @pytest.mark.asyncio
    async def test_add_error_pattern(self, config: Any) -> None:
        """Test adding error patterns."""
        manager = ErrorAlertManager(config=config)

        pattern = ErrorPattern(
            name="connection_error",
            message_regex=r".*connection.*failed.*",
            log_level=LogLevel.ERROR,
            alert_level=AlertLevel.HIGH,
        )

        manager.add_pattern(pattern)

        assert len(manager.patterns) == 1
        assert manager.patterns[0].name == "connection_error"

    @pytest.mark.asyncio
    async def test_add_alert_channel(self, config: Any, temp_log_file: Any) -> None:
        """Test adding alert channels."""
        manager = ErrorAlertManager(config=config)

        channel = LogAlertChannel(log_file=temp_log_file, log_level=LogLevel.INFO)

        manager.add_channel("log_alerts", channel)

        assert len(manager.channels) == 1
        assert "log_alerts" in manager.channels

    @pytest.mark.asyncio
    async def test_add_threshold(self, config: Any) -> None:
        """Test adding alert thresholds."""
        manager = ErrorAlertManager(config=config)

        threshold = AlertThreshold(
            pattern_name="database_error",
            max_occurrences=3,
            time_window_minutes=5,
            alert_level=AlertLevel.HIGH,
        )

        manager.add_threshold(threshold)

        assert len(manager.thresholds) == 1
        assert manager.thresholds[0].pattern_name == "database_error"

    @pytest.mark.asyncio
    async def test_process_log_entry_no_match(
        self, config: Any, sample_error_log_entry: Any
    ) -> None:
        """Test processing log entry that doesn't match any patterns."""
        manager = ErrorAlertManager(config=config)
        await manager.start()

        # Add a pattern that won't match
        pattern = ErrorPattern(
            name="database_error",
            message_regex=r".*database.*error.*",
            log_level=LogLevel.ERROR,
            alert_level=AlertLevel.HIGH,
        )
        manager.add_pattern(pattern)

        result = await manager.process_log_entry(sample_error_log_entry)

        assert result is False  # No match, no alert sent
        await manager.stop()

    @pytest.mark.asyncio
    async def test_process_log_entry_immediate_alert(
        self, config: Any, temp_log_file: Any, sample_critical_log_entry: Any
    ) -> None:
        """Test processing log entry that triggers immediate alert."""
        manager = ErrorAlertManager(config=config)
        await manager.start()

        # Add pattern for critical errors
        pattern = ErrorPattern(
            name="critical_error",
            message_regex=r".*Database.*connection.*lost.*",
            log_level=LogLevel.CRITICAL,
            alert_level=AlertLevel.CRITICAL,
        )
        manager.add_pattern(pattern)

        # Add log channel
        channel = LogAlertChannel(log_file=temp_log_file, log_level=LogLevel.INFO)
        manager.add_channel("log_alerts", channel)

        # Add immediate threshold (alert on first occurrence)
        threshold = AlertThreshold(
            pattern_name="critical_error",
            max_occurrences=0,
            time_window_minutes=1,
            alert_level=AlertLevel.CRITICAL,
        )
        manager.add_threshold(threshold)

        result = await manager.process_log_entry(sample_critical_log_entry)

        assert result is True  # Alert should be sent

        # Verify alert was logged
        with open(temp_log_file) as f:
            content = f.read()
            assert "ALERT" in content
            assert "critical_error" in content

        await manager.stop()

    @pytest.mark.asyncio
    async def test_process_log_entry_threshold_not_met(
        self, config: Any, temp_log_file: Any, sample_error_log_entry: Any
    ) -> None:
        """Test processing log entry that doesn't meet threshold."""
        manager = ErrorAlertManager(config=config)
        await manager.start()

        # Add pattern that matches
        pattern = ErrorPattern(
            name="api_error",
            message_regex=r".*API.*connection.*failed.*",
            log_level=LogLevel.ERROR,
            alert_level=AlertLevel.HIGH,
        )
        manager.add_pattern(pattern)

        # Add channel
        channel = LogAlertChannel(log_file=temp_log_file, log_level=LogLevel.INFO)
        manager.add_channel("log_alerts", channel)

        # Add threshold that requires 3 occurrences
        threshold = AlertThreshold(
            pattern_name="api_error",
            max_occurrences=3,
            time_window_minutes=5,
            alert_level=AlertLevel.HIGH,
        )
        manager.add_threshold(threshold)

        # Process first occurrence
        result = await manager.process_log_entry(sample_error_log_entry)

        assert result is False  # Threshold not met yet

        await manager.stop()

    @pytest.mark.asyncio
    async def test_get_alert_statistics(self, config: Any, sample_error_log_entry: Any) -> None:
        """Test getting alert statistics."""
        manager = ErrorAlertManager(config=config)
        await manager.start()

        # Add pattern and process some entries
        pattern = ErrorPattern(
            name="api_error",
            message_regex=r".*API.*connection.*failed.*",
            log_level=LogLevel.ERROR,
            alert_level=AlertLevel.HIGH,
        )
        manager.add_pattern(pattern)

        # Process several entries
        for _ in range(3):
            await manager.process_log_entry(sample_error_log_entry)

        stats = manager.get_statistics()

        assert stats is not None and stats["total_patterns"] == 1
        assert stats is not None and stats["total_channels"] == 0
        assert stats is not None and stats["total_thresholds"] == 0
        assert stats is not None and "api_error" in stats["pattern_matches"]
        assert stats["pattern_matches"]["api_error"] == 3

        await manager.stop()


class TestAlertingIntegration:
    """Test integration with structured logging."""

    @pytest.mark.asyncio
    async def test_integration_with_structured_logger(
        self, config: Any, temp_log_file: Any
    ) -> None:
        """Test error alerting integration with structured logger."""
        # Create structured logger
        logger = StructuredLogger(config=config, log_file=temp_log_file, log_level=LogLevel.DEBUG)
        await logger.start()

        # Create alert manager
        alert_manager = ErrorAlertManager(config=config)
        await alert_manager.start()

        # Set up error pattern and alerting
        pattern = ErrorPattern(
            name="database_critical",
            message_regex=r".*Database.*critical.*",
            log_level=LogLevel.CRITICAL,
            alert_level=AlertLevel.CRITICAL,
        )
        alert_manager.add_pattern(pattern)

        alert_channel = LogAlertChannel(log_file=f"{temp_log_file}.alerts", log_level=LogLevel.INFO)
        alert_manager.add_channel("alert_log", alert_channel)

        threshold = AlertThreshold(
            pattern_name="database_critical",
            max_occurrences=0,
            time_window_minutes=1,
            alert_level=AlertLevel.CRITICAL,
        )
        alert_manager.add_threshold(threshold)

        # Log critical error
        context = LogContext(request_id="req_integration", operation="database_operation")

        await logger.critical(
            "Database critical failure detected",
            context,
            {
                "error_type": "DatabaseCriticalError",
                "affected_tables": ["companies", "stream_events"],
            },
        )

        # Simulate processing the log entry through alert manager
        # In real implementation, this would be done via log handler or observer pattern
        critical_entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.CRITICAL,
            message="Database critical failure detected",
            context=context,
            extra_data={
                "error_type": "DatabaseCriticalError",
                "affected_tables": ["companies", "stream_events"],
            },
        )

        alert_sent = await alert_manager.process_log_entry(critical_entry)

        assert alert_sent is True

        # Verify alert was written
        alert_file = f"{temp_log_file}.alerts"
        assert os.path.exists(alert_file)

        with open(alert_file) as f:
            alert_content = f.read()
            assert "ALERT" in alert_content
            assert "database_critical" in alert_content

        await logger.stop()
        await alert_manager.stop()

    @pytest.mark.asyncio
    async def test_multiple_channel_alerting(self, config: Any, temp_log_file: Any) -> None:
        """Test alerting through multiple channels."""
        alert_manager = ErrorAlertManager(config=config)
        await alert_manager.start()

        # Add pattern
        pattern = ErrorPattern(
            name="multi_channel_test",
            message_regex=r".*multi.*channel.*test.*",
            log_level=LogLevel.ERROR,
            alert_level=AlertLevel.HIGH,
        )
        alert_manager.add_pattern(pattern)

        # Add multiple channels
        log_channel = LogAlertChannel(
            log_file=f"{temp_log_file}.log_alerts", log_level=LogLevel.INFO
        )
        alert_manager.add_channel("log", log_channel)

        webhook_channel = WebhookAlertChannel(
            webhook_url="https://httpbin.org/post"  # Test endpoint
        )
        alert_manager.add_channel("webhook", webhook_channel)

        # Add threshold
        threshold = AlertThreshold(
            pattern_name="multi_channel_test",
            max_occurrences=0,
            time_window_minutes=1,
            alert_level=AlertLevel.HIGH,
        )
        alert_manager.add_threshold(threshold)

        # Create matching log entry
        log_entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.ERROR,
            message="This is a multi channel test error",
            context=LogContext(operation="test_multi_channel"),
            extra_data={"test": True},
        )

        alert_sent = await alert_manager.process_log_entry(log_entry)

        assert alert_sent is True

        # Verify log channel received alert
        log_alert_file = f"{temp_log_file}.log_alerts"
        assert os.path.exists(log_alert_file)

        with open(log_alert_file) as f:
            content = f.read()
            assert "multi_channel_test" in content

        await alert_manager.stop()
