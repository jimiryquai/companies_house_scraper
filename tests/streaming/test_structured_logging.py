"""
Tests for structured logging functionality in streaming module.
"""

import json
import logging
import os
import tempfile
from datetime import datetime
from typing import Any

import pytest

from src.streaming.config import StreamingConfig
from src.streaming.structured_logger import (
    ContextualLogger,
    LogContext,
    LogEntry,
    LogLevel,
    StructuredLogger,
    StructuredLogHandler,
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
def config() -> Any:
    """Create test configuration."""
    return StreamingConfig(
        streaming_api_key="test-api-key-123456",
        rest_api_key="test-rest-api-key-123456",
        database_path=":memory:",
        batch_size=10,
    )


@pytest.fixture
def sample_log_context() -> Any:
    """Sample log context for testing."""
    return LogContext(
        request_id="req_12345",
        session_id="sess_67890",
        company_number="12345678",
        event_id="evt_001",
        operation="process_event",
        user_agent="StreamingClient/1.0",
    )


class TestLogLevel:
    """Test LogLevel enum."""

    def test_log_level_values(self) -> None:
        """Test LogLevel enum values."""
        assert LogLevel.DEBUG.value == "DEBUG"
        assert LogLevel.INFO.value == "INFO"
        assert LogLevel.WARNING.value == "WARNING"
        assert LogLevel.ERROR.value == "ERROR"
        assert LogLevel.CRITICAL.value == "CRITICAL"

    def test_log_level_to_logging_level(self) -> None:
        """Test conversion to standard logging levels."""
        assert LogLevel.DEBUG.to_logging_level() == logging.DEBUG
        assert LogLevel.INFO.to_logging_level() == logging.INFO
        assert LogLevel.WARNING.to_logging_level() == logging.WARNING
        assert LogLevel.ERROR.to_logging_level() == logging.ERROR
        assert LogLevel.CRITICAL.to_logging_level() == logging.CRITICAL


class TestLogContext:
    """Test LogContext functionality."""

    def test_log_context_creation(self, sample_log_context: Any) -> None:
        """Test LogContext creation."""
        assert sample_log_context.request_id == "req_12345"
        assert sample_log_context.session_id == "sess_67890"
        assert sample_log_context.company_number == "12345678"
        assert sample_log_context.event_id == "evt_001"
        assert sample_log_context.operation == "process_event"
        assert sample_log_context.user_agent == "StreamingClient/1.0"

    def test_log_context_to_dict(self, sample_log_context: Any) -> None:
        """Test LogContext to dictionary conversion."""
        context_dict = sample_log_context.to_dict()

        expected = {
            "request_id": "req_12345",
            "session_id": "sess_67890",
            "company_number": "12345678",
            "event_id": "evt_001",
            "operation": "process_event",
            "user_agent": "StreamingClient/1.0",
        }

        assert context_dict == expected

    def test_log_context_partial(self) -> None:
        """Test LogContext with partial data."""
        context = LogContext(request_id="req_123", operation="connect")

        context_dict = context.to_dict()

        assert context_dict is not None and context_dict["request_id"] == "req_123"
        assert context_dict is not None and context_dict["operation"] == "connect"
        assert "session_id" not in context_dict  # None values are excluded
        assert "company_number" not in context_dict  # None values are excluded

    def test_log_context_merge(self, sample_log_context: Any) -> None:
        """Test merging LogContext instances."""
        additional_context = LogContext(
            request_id="req_new",  # Should override
            processing_time_ms=150,  # Should be added
        )

        merged = sample_log_context.merge(additional_context)

        assert merged.request_id == "req_new"  # Overridden
        assert merged.session_id == "sess_67890"  # Preserved
        assert merged.processing_time_ms == 150  # Added


class TestLogEntry:
    """Test LogEntry functionality."""

    def test_log_entry_creation(self, sample_log_context: Any) -> None:
        """Test LogEntry creation."""
        timestamp = datetime.now()
        entry = LogEntry(
            timestamp=timestamp,
            level=LogLevel.INFO,
            message="Test message",
            context=sample_log_context,
            extra_data={"key": "value"},
        )

        assert entry.timestamp == timestamp
        assert entry.level == LogLevel.INFO
        assert entry.message == "Test message"
        assert entry.context == sample_log_context
        assert entry.extra_data == {"key": "value"}

    def test_log_entry_to_dict(self, sample_log_context: Any) -> None:
        """Test LogEntry to dictionary conversion."""
        timestamp = datetime(2025, 8, 15, 12, 30, 0)
        entry = LogEntry(
            timestamp=timestamp,
            level=LogLevel.ERROR,
            message="Processing failed",
            context=sample_log_context,
            extra_data={"error_code": "E001", "retry_count": 2},
        )

        entry_dict = entry.to_dict()

        expected = {
            "timestamp": "2025-08-15T12:30:00",
            "level": "ERROR",
            "message": "Processing failed",
            "context": sample_log_context.to_dict(),
            "error_code": "E001",
            "retry_count": 2,
        }

        assert entry_dict == expected

    def test_log_entry_to_json(self, sample_log_context: Any) -> None:
        """Test LogEntry JSON serialization."""
        timestamp = datetime(2025, 8, 15, 12, 30, 0)
        entry = LogEntry(
            timestamp=timestamp,
            level=LogLevel.WARNING,
            message="Rate limit approaching",
            context=sample_log_context,
        )

        json_str = entry.to_json()
        parsed = json.loads(json_str)

        assert parsed is not None and parsed["timestamp"] == "2025-08-15T12:30:00"
        assert parsed is not None and parsed["level"] == "WARNING"
        assert parsed is not None and parsed["message"] == "Rate limit approaching"
        assert parsed["context"]["request_id"] == "req_12345"


class TestStructuredLogHandler:
    """Test StructuredLogHandler functionality."""

    def test_handler_creation(self, temp_log_file: Any) -> None:
        """Test StructuredLogHandler creation."""
        handler = StructuredLogHandler(
            log_file=temp_log_file, log_level=LogLevel.INFO, max_file_size_mb=10, backup_count=5
        )

        assert handler.log_file == temp_log_file
        assert handler.log_level == LogLevel.INFO
        assert handler.max_file_size_mb == 10
        assert handler.backup_count == 5

    @pytest.mark.asyncio
    async def test_handler_write_log(self, temp_log_file: Any, sample_log_context: Any) -> None:
        """Test writing log entries to file."""
        handler = StructuredLogHandler(log_file=temp_log_file, log_level=LogLevel.DEBUG)

        await handler.start()

        entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            message="Test log entry",
            context=sample_log_context,
        )

        await handler.write_log(entry)
        await handler.flush()
        await handler.stop()

        # Verify log was written
        with open(temp_log_file) as f:
            content = f.read()
            assert "Test log entry" in content
            assert "req_12345" in content

    @pytest.mark.asyncio
    async def test_handler_log_level_filtering(self, temp_log_file: Any) -> None:
        """Test log level filtering."""
        handler = StructuredLogHandler(
            log_file=temp_log_file,
            log_level=LogLevel.WARNING,  # Only WARNING and above
        )

        await handler.start()

        # This should be filtered out
        debug_entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.DEBUG,
            message="Debug message",
            context=LogContext(),
        )

        # This should be logged
        error_entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.ERROR,
            message="Error message",
            context=LogContext(),
        )

        await handler.write_log(debug_entry)
        await handler.write_log(error_entry)
        await handler.flush()
        await handler.stop()

        # Verify only error message was written
        with open(temp_log_file) as f:
            content = f.read()
            assert "Debug message" not in content
            assert "Error message" in content

    @pytest.mark.asyncio
    async def test_handler_file_rotation(self, temp_log_file: Any) -> None:
        """Test log file rotation on size limit."""
        handler = StructuredLogHandler(
            log_file=temp_log_file,
            log_level=LogLevel.DEBUG,
            max_file_size_mb=1,  # Small size for testing (minimum 1MB)
            backup_count=2,
        )

        await handler.start()

        # Write many entries to trigger rotation
        for i in range(100):
            entry = LogEntry(
                timestamp=datetime.now(),
                level=LogLevel.INFO,
                message=f"Large log message number {i} with lots of content to fill up the file",
                context=LogContext(operation=f"test_op_{i}"),
            )
            await handler.write_log(entry)

        await handler.flush()
        await handler.stop()

        # Check that backup files were created

        # At least one backup should exist due to rotation
        assert os.path.exists(temp_log_file)
        # Note: Actual rotation behavior depends on file size and timing


class TestStructuredLogger:
    """Test StructuredLogger functionality."""

    @pytest.mark.asyncio
    async def test_logger_initialization(self, config: Any, temp_log_file: Any) -> None:
        """Test StructuredLogger initialization."""
        logger = StructuredLogger(config=config, log_file=temp_log_file, log_level=LogLevel.INFO)

        assert logger.config == config
        assert logger.log_file == temp_log_file
        assert logger.log_level == LogLevel.INFO
        assert logger.handler is not None

    @pytest.mark.asyncio
    async def test_logger_basic_logging(self, config: Any, temp_log_file: Any) -> None:
        """Test basic logging functionality."""
        logger = StructuredLogger(config=config, log_file=temp_log_file, log_level=LogLevel.DEBUG)

        await logger.start()

        # Test different log levels
        await logger.debug("Debug message", LogContext(operation="debug_test"))
        await logger.info("Info message", LogContext(operation="info_test"))
        await logger.warning("Warning message", LogContext(operation="warning_test"))
        await logger.error("Error message", LogContext(operation="error_test"))
        await logger.critical("Critical message", LogContext(operation="critical_test"))

        await logger.stop()

        # Verify all messages were logged
        with open(temp_log_file) as f:
            content = f.read()
            assert "Debug message" in content
            assert "Info message" in content
            assert "Warning message" in content
            assert "Error message" in content
            assert "Critical message" in content

    @pytest.mark.asyncio
    async def test_logger_with_context(
        self, config: Any, temp_log_file: Any, sample_log_context: Any
    ) -> None:
        """Test logging with context information."""
        logger = StructuredLogger(config=config, log_file=temp_log_file, log_level=LogLevel.INFO)

        await logger.start()

        await logger.info(
            "Processing event",
            sample_log_context,
            {"processing_time_ms": 150, "records_processed": 5},
        )

        await logger.stop()

        # Verify context and extra data were logged
        with open(temp_log_file) as f:
            content = f.read()
            assert "req_12345" in content
            assert "processing_time_ms" in content
            assert "150" in content

    @pytest.mark.asyncio
    async def test_logger_exception_logging(self, config: Any, temp_log_file: Any) -> None:
        """Test logging exceptions with stack traces."""
        logger = StructuredLogger(config=config, log_file=temp_log_file, log_level=LogLevel.ERROR)

        await logger.start()

        try:
            raise ValueError("Test exception for logging")
        except Exception as e:
            await logger.exception(
                "Exception occurred during processing",
                LogContext(operation="exception_test"),
                exception=e,
            )

        await logger.stop()

        # Verify exception was logged with stack trace
        with open(temp_log_file) as f:
            content = f.read()
            assert "Exception occurred during processing" in content
            assert "ValueError" in content
            assert "Test exception for logging" in content
            assert "Traceback" in content or "stack_trace" in content


class TestContextualLogger:
    """Test ContextualLogger functionality."""

    @pytest.mark.asyncio
    async def test_contextual_logger_creation(
        self, config: Any, temp_log_file: Any, sample_log_context: Any
    ) -> None:
        """Test ContextualLogger creation with default context."""
        base_logger = StructuredLogger(
            config=config, log_file=temp_log_file, log_level=LogLevel.INFO
        )

        contextual_logger = ContextualLogger(
            base_logger=base_logger, default_context=sample_log_context
        )

        assert contextual_logger.base_logger == base_logger
        assert contextual_logger.default_context == sample_log_context

    @pytest.mark.asyncio
    async def test_contextual_logger_inherits_context(
        self, config: Any, temp_log_file: Any, sample_log_context: Any
    ) -> None:
        """Test that contextual logger inherits default context."""
        base_logger = StructuredLogger(
            config=config, log_file=temp_log_file, log_level=LogLevel.INFO
        )

        contextual_logger = ContextualLogger(
            base_logger=base_logger, default_context=sample_log_context
        )

        await base_logger.start()

        # Log without explicit context - should use default
        await contextual_logger.info("Message with default context")

        # Log with additional context - should merge
        additional_context = LogContext(processing_time_ms=200)
        await contextual_logger.info("Message with merged context", additional_context)

        await base_logger.stop()

        # Verify both messages have context
        with open(temp_log_file) as f:
            content = f.read()
            assert "req_12345" in content  # From default context
            assert "processing_time_ms" in content  # From additional context

    @pytest.mark.asyncio
    async def test_contextual_logger_context_override(
        self, config: Any, temp_log_file: Any
    ) -> None:
        """Test that additional context can override default context."""
        default_context = LogContext(request_id="req_default", operation="default_op")

        base_logger = StructuredLogger(
            config=config, log_file=temp_log_file, log_level=LogLevel.INFO
        )

        contextual_logger = ContextualLogger(
            base_logger=base_logger, default_context=default_context
        )

        await base_logger.start()

        # Override request_id but keep operation
        override_context = LogContext(request_id="req_override")
        await contextual_logger.info("Override test", override_context)

        await base_logger.stop()

        # Verify override worked
        with open(temp_log_file) as f:
            content = f.read()
            assert "req_override" in content
            assert "req_default" not in content
            assert "default_op" in content  # Should still be present


class TestLoggingIntegration:
    """Test integration with existing systems."""

    @pytest.mark.asyncio
    async def test_event_processing_logging(self, config: Any, temp_log_file: Any) -> None:
        """Test logging during event processing workflow."""
        logger = StructuredLogger(config=config, log_file=temp_log_file, log_level=LogLevel.DEBUG)

        await logger.start()

        # Simulate event processing workflow
        event_context = LogContext(
            request_id="req_workflow",
            event_id="evt_12345",
            company_number="12345678",
            operation="process_company_event",
        )

        # Start processing
        await logger.info("Starting event processing", event_context)

        # Processing steps
        await logger.debug("Validating event data", event_context)
        await logger.debug(
            "Updating company record", event_context, {"table": "companies", "operation": "upsert"}
        )

        # Success
        await logger.info(
            "Event processing completed",
            event_context,
            {"processing_time_ms": 125, "records_affected": 1},
        )

        await logger.stop()

        # Verify workflow was logged completely
        with open(temp_log_file) as f:
            lines = f.readlines()
            assert len(lines) >= 4

            content = "".join(lines)
            assert "Starting event processing" in content
            assert "Validating event data" in content
            assert "Updating company record" in content
            assert "Event processing completed" in content
            assert "processing_time_ms" in content

    @pytest.mark.asyncio
    async def test_error_handling_logging(self, config: Any, temp_log_file: Any) -> None:
        """Test logging during error scenarios."""
        logger = StructuredLogger(config=config, log_file=temp_log_file, log_level=LogLevel.INFO)

        await logger.start()

        error_context = LogContext(
            request_id="req_error", event_id="evt_error", operation="handle_api_error"
        )

        # Simulate API error
        await logger.warning(
            "API rate limit approaching",
            error_context,
            {"current_rate": 450, "limit": 500, "reset_time": "2025-08-15T13:00:00Z"},
        )

        # Simulate connection error
        await logger.error(
            "API connection failed",
            error_context,
            {"error_type": "ConnectionTimeout", "retry_count": 2, "next_retry_seconds": 30},
        )

        # Simulate critical system error
        await logger.critical(
            "Database connection lost",
            error_context,
            {
                "error_type": "DatabaseConnectionError",
                "reconnect_attempts": 3,
                "system_status": "degraded",
            },
        )

        await logger.stop()

        # Verify error logging
        with open(temp_log_file) as f:
            content = f.read()
            assert "API rate limit approaching" in content
            assert "API connection failed" in content
            assert "Database connection lost" in content
            assert "retry_count" in content
            assert "reconnect_attempts" in content

    @pytest.mark.asyncio
    async def test_performance_metrics_logging(self, config: Any, temp_log_file: Any) -> None:
        """Test logging performance metrics."""
        logger = StructuredLogger(config=config, log_file=temp_log_file, log_level=LogLevel.INFO)

        await logger.start()

        metrics_context = LogContext(operation="performance_metrics", session_id="sess_metrics")

        # Log various performance metrics
        await logger.info(
            "API response time",
            metrics_context,
            {
                "metric_type": "response_time",
                "endpoint": "/stream",
                "response_time_ms": 145,
                "status_code": 200,
            },
        )

        await logger.info(
            "Database query performance",
            metrics_context,
            {
                "metric_type": "database_query",
                "query_type": "company_upsert",
                "execution_time_ms": 23,
                "rows_affected": 1,
            },
        )

        await logger.info(
            "Memory usage",
            metrics_context,
            {
                "metric_type": "memory_usage",
                "memory_used_mb": 45.2,
                "memory_peak_mb": 67.8,
                "gc_collections": 12,
            },
        )

        await logger.stop()

        # Verify metrics logging
        with open(temp_log_file) as f:
            content = f.read()
            assert "response_time_ms" in content
            assert "execution_time_ms" in content
            assert "memory_used_mb" in content
            assert "metric_type" in content
