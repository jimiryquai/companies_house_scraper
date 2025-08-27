"""
Tests for streaming event logging and tracking functionality.
"""

import os
import sqlite3
import tempfile
from datetime import datetime, timedelta
from typing import Any

import pytest

from src.streaming.config import StreamingConfig
from src.streaming.database import StreamingDatabase
from src.streaming.event_logger import EventLogger, EventTracker, ProcessingStatus


@pytest.fixture
def temp_db() -> Any:
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
        temp_path = temp_file.name

    # Initialize database schema synchronously
    with sqlite3.connect(temp_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                company_number TEXT PRIMARY KEY,
                company_name TEXT,
                company_status TEXT,
                company_status_detail TEXT,
                incorporation_date TEXT,
                sic_codes TEXT,
                address_line_1 TEXT,
                address_line_2 TEXT,
                locality TEXT,
                region TEXT,
                country TEXT,
                postal_code TEXT,
                premises TEXT,
                stream_last_updated TEXT,
                stream_status TEXT DEFAULT 'unknown',
                data_source TEXT DEFAULT 'bulk',
                last_stream_event_id TEXT,
                stream_metadata TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS stream_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE,
                event_type TEXT NOT NULL,
                company_number TEXT,
                event_data TEXT,
                processed_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                processing_status TEXT DEFAULT 'pending',
                retry_count INTEGER DEFAULT 0,
                error_message TEXT,
                FOREIGN KEY (company_number) REFERENCES companies(company_number)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS event_processing_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT,
                status TEXT,
                timestamp TEXT,
                details TEXT,
                FOREIGN KEY (event_id) REFERENCES stream_events(event_id)
            )
        """)

        conn.commit()

    yield temp_path

    # Cleanup
    os.unlink(temp_path)


@pytest.fixture
def config(temp_db: str) -> Any:
    """Create test configuration with temp database."""
    return StreamingConfig(
        streaming_api_key="test-api-key-123456",
        rest_api_key="test-rest-api-key-123456",
        database_path=temp_db,
        batch_size=10,
    )


@pytest.fixture
def sample_event() -> Any:
    """Sample event for testing."""
    return {
        "resource_kind": "company-profile",
        "resource_id": "12345678",
        "data": {
            "company_number": "12345678",
            "company_name": "Test Company Ltd",
            "company_status": "active-proposal-to-strike-off",
        },
        "event": {"timepoint": 12345, "published_at": "2025-08-15T12:30:00Z"},
    }


class TestEventLogger:
    """Test EventLogger functionality."""

    @pytest.mark.asyncio
    async def test_event_logger_initialization(self, config: Any) -> None:
        """Test EventLogger initialization."""
        logger = EventLogger(config)

        assert logger.config == config
        assert logger.db is not None
        assert isinstance(logger.db, StreamingDatabase)
        assert logger.processing_queue == []
        assert logger.failed_events == []

    @pytest.mark.asyncio
    async def test_log_event_success(self, config: Any, sample_event: Any) -> None:
        """Test successful event logging."""
        logger = EventLogger(config)
        await logger.connect()

        event_id = await logger.log_event("evt_001", sample_event)

        assert event_id == "evt_001"

        # Verify event was logged
        event_record = await logger.get_event("evt_001")
        assert event_record is not None
        assert event_record is not None and event_record["event_id"] == "evt_001"
        assert (
            event_record is not None
            and event_record["processing_status"] == ProcessingStatus.PENDING.value
        )

        await logger.disconnect()

    @pytest.mark.asyncio
    async def test_log_duplicate_event(self, config: Any, sample_event: Any) -> None:
        """Test duplicate event detection."""
        logger = EventLogger(config)
        await logger.connect()

        # Log event first time
        event_id1 = await logger.log_event("evt_dup", sample_event)
        assert event_id1 == "evt_dup"

        # Try to log same event again
        event_id2 = await logger.log_event("evt_dup", sample_event)
        assert event_id2 is None  # Should return None for duplicate

        await logger.disconnect()

    @pytest.mark.asyncio
    async def test_update_event_status(self, config: Any, sample_event: Any) -> None:
        """Test updating event processing status."""
        logger = EventLogger(config)
        await logger.connect()

        # Log event
        await logger.log_event("evt_002", sample_event)

        # Update status to processing
        await logger.update_event_status("evt_002", ProcessingStatus.PROCESSING)
        event = await logger.get_event("evt_002")
        assert event is not None
        assert event is not None and event["processing_status"] == ProcessingStatus.PROCESSING.value

        # Update status to completed
        await logger.update_event_status("evt_002", ProcessingStatus.COMPLETED)
        event = await logger.get_event("evt_002")
        assert event is not None
        assert event is not None and event["processing_status"] == ProcessingStatus.COMPLETED.value

        await logger.disconnect()

    @pytest.mark.asyncio
    async def test_update_event_with_error(self, config: Any, sample_event: Any) -> None:
        """Test updating event status with error message."""
        logger = EventLogger(config)
        await logger.connect()

        # Log event
        await logger.log_event("evt_003", sample_event)

        # Update with error
        error_msg = "Failed to process: API timeout"
        await logger.update_event_status(
            "evt_003", ProcessingStatus.FAILED, error_message=error_msg
        )

        event = await logger.get_event("evt_003")
        assert event is not None
        assert event is not None and event["processing_status"] == ProcessingStatus.FAILED.value
        assert event is not None and event["error_message"] == error_msg

        await logger.disconnect()

    @pytest.mark.asyncio
    async def test_increment_retry_count(self, config: Any, sample_event: Any) -> None:
        """Test incrementing retry count for failed events."""
        logger = EventLogger(config)
        await logger.connect()

        # Log event
        await logger.log_event("evt_004", sample_event)

        # Increment retry count
        await logger.increment_retry_count("evt_004")
        event = await logger.get_event("evt_004")
        assert event is not None and event["retry_count"] == 1

        # Increment again
        await logger.increment_retry_count("evt_004")
        event = await logger.get_event("evt_004")
        assert event is not None and event["retry_count"] == 2

        await logger.disconnect()

    @pytest.mark.asyncio
    async def test_get_pending_events(self, config: Any, sample_event: Any) -> None:
        """Test getting pending events."""
        logger = EventLogger(config)
        await logger.connect()

        # Log multiple events
        await logger.log_event("evt_pending_1", sample_event)
        await logger.log_event("evt_pending_2", sample_event)
        await logger.log_event("evt_completed", sample_event)

        # Mark one as completed
        await logger.update_event_status("evt_completed", ProcessingStatus.COMPLETED)

        # Get pending events
        pending = await logger.get_pending_events(limit=10)

        assert len(pending) == 2
        assert all(e["processing_status"] == ProcessingStatus.PENDING.value for e in pending)

        await logger.disconnect()

    @pytest.mark.asyncio
    async def test_get_failed_events(self, config: Any, sample_event: Any) -> None:
        """Test getting failed events for retry."""
        logger = EventLogger(config)
        await logger.connect()

        # Log events
        await logger.log_event("evt_fail_1", sample_event)
        await logger.log_event("evt_fail_2", sample_event)
        await logger.log_event("evt_success", sample_event)

        # Mark some as failed
        await logger.update_event_status("evt_fail_1", ProcessingStatus.FAILED)
        await logger.update_event_status("evt_fail_2", ProcessingStatus.FAILED)
        await logger.update_event_status("evt_success", ProcessingStatus.COMPLETED)

        # Get failed events with retry count < 3
        failed = await logger.get_failed_events(max_retries=3)

        assert len(failed) == 2
        assert all(e["processing_status"] == ProcessingStatus.FAILED.value for e in failed)

        await logger.disconnect()

    @pytest.mark.asyncio
    async def test_batch_log_events(self, config: Any, sample_event: Any) -> None:
        """Test batch logging of events."""
        logger = EventLogger(config)
        await logger.connect()

        events = [
            ("evt_batch_1", sample_event),
            ("evt_batch_2", sample_event),
            ("evt_batch_3", sample_event),
        ]

        results = await logger.batch_log_events(events)

        assert len(results) == 3
        assert results == ["evt_batch_1", "evt_batch_2", "evt_batch_3"]

        # Verify all were logged
        for event_id, _ in events:
            event = await logger.get_event(event_id)
            assert event is not None

        await logger.disconnect()

    @pytest.mark.asyncio
    async def test_get_event_statistics(self, config: Any, sample_event: Any) -> None:
        """Test getting event processing statistics."""
        logger = EventLogger(config)
        await logger.connect()

        # Create events with different statuses
        await logger.log_event("evt_stat_1", sample_event)
        await logger.log_event("evt_stat_2", sample_event)
        await logger.log_event("evt_stat_3", sample_event)
        await logger.log_event("evt_stat_4", sample_event)

        await logger.update_event_status("evt_stat_1", ProcessingStatus.COMPLETED)
        await logger.update_event_status("evt_stat_2", ProcessingStatus.COMPLETED)
        await logger.update_event_status("evt_stat_3", ProcessingStatus.FAILED)
        # evt_stat_4 remains pending

        stats = await logger.get_statistics()

        assert stats is not None and stats["total_events"] == 4
        assert stats is not None and stats["pending_events"] == 1
        assert stats is not None and stats["completed_events"] == 2
        assert stats is not None and stats["failed_events"] == 1
        assert stats is not None and stats["success_rate"] == 50.0  # 2 completed out of 4 total

        await logger.disconnect()


class TestEventTracker:
    """Test EventTracker functionality."""

    @pytest.mark.asyncio
    async def test_event_tracker_initialization(self, config: Any) -> None:
        """Test EventTracker initialization."""
        tracker = EventTracker(config)

        assert tracker.config == config
        assert tracker.logger is not None
        assert tracker.last_timepoint is None
        assert tracker.processed_count == 0
        assert tracker.failed_count == 0

    @pytest.mark.asyncio
    async def test_track_event(self, config: Any, sample_event: Any) -> None:
        """Test tracking an event."""
        tracker = EventTracker(config)
        await tracker.connect()

        result = await tracker.track_event("evt_track_1", sample_event)

        assert result is True
        assert tracker.processed_count == 1
        assert tracker.last_timepoint == 12345

        await tracker.disconnect()

    @pytest.mark.asyncio
    async def test_track_duplicate_event(self, config: Any, sample_event: Any) -> None:
        """Test tracking duplicate events."""
        tracker = EventTracker(config)
        await tracker.connect()

        # Track event first time
        result1 = await tracker.track_event("evt_dup_track", sample_event)
        assert result1 is True

        # Track same event again
        result2 = await tracker.track_event("evt_dup_track", sample_event)
        assert result2 is False  # Should be rejected as duplicate

        assert tracker.processed_count == 1  # Should only count once

        await tracker.disconnect()

    @pytest.mark.asyncio
    async def test_process_event_success(self, config: Any, sample_event: Any) -> None:
        """Test successful event processing."""
        tracker = EventTracker(config)
        await tracker.connect()

        # Track and process event
        await tracker.track_event("evt_process_1", sample_event)

        # Simulate processing
        await tracker.mark_processing("evt_process_1")
        event = await tracker.logger.get_event("evt_process_1")
        assert event is not None and event["processing_status"] == ProcessingStatus.PROCESSING.value

        # Mark as completed
        await tracker.mark_completed("evt_process_1")
        event = await tracker.logger.get_event("evt_process_1")
        assert event is not None and event["processing_status"] == ProcessingStatus.COMPLETED.value

        await tracker.disconnect()

    @pytest.mark.asyncio
    async def test_process_event_failure(self, config: Any, sample_event: Any) -> None:
        """Test failed event processing."""
        tracker = EventTracker(config)
        await tracker.connect()

        # Track and process event
        await tracker.track_event("evt_fail_process", sample_event)

        # Mark as failed
        await tracker.mark_failed("evt_fail_process", "Processing error")

        event = await tracker.logger.get_event("evt_fail_process")
        assert event is not None and event["processing_status"] == ProcessingStatus.FAILED.value
        assert event is not None and event["error_message"] == "Processing error"
        assert tracker.failed_count == 1

        await tracker.disconnect()

    @pytest.mark.asyncio
    async def test_retry_failed_events(self, config: Any, sample_event: Any) -> None:
        """Test retrying failed events."""
        tracker = EventTracker(config)
        await tracker.connect()

        # Create failed events
        await tracker.track_event("evt_retry_1", sample_event)
        await tracker.track_event("evt_retry_2", sample_event)

        await tracker.mark_failed("evt_retry_1", "Error 1")
        await tracker.mark_failed("evt_retry_2", "Error 2")

        # Retry failed events
        retried = await tracker.retry_failed_events(max_retries=3)

        assert len(retried) == 2
        assert "evt_retry_1" in retried
        assert "evt_retry_2" in retried

        # Check retry counts were incremented
        event1 = await tracker.logger.get_event("evt_retry_1")
        event2 = await tracker.logger.get_event("evt_retry_2")
        assert event1 is not None and event1["retry_count"] == 1
        assert event2 is not None and event2["retry_count"] == 1
        assert event1 is not None and event1["processing_status"] == ProcessingStatus.PENDING.value
        assert event2 is not None and event2["processing_status"] == ProcessingStatus.PENDING.value

        await tracker.disconnect()

    @pytest.mark.asyncio
    async def test_get_tracking_stats(self, config: Any, sample_event: Any) -> None:
        """Test getting tracking statistics."""
        tracker = EventTracker(config)
        await tracker.connect()

        # Track multiple events
        events = [
            (
                "evt_stats_1",
                {
                    **sample_event,
                    "event": {"timepoint": 100, "published_at": "2025-08-15T12:30:00Z"},
                },
            ),
            (
                "evt_stats_2",
                {
                    **sample_event,
                    "event": {"timepoint": 200, "published_at": "2025-08-15T12:31:00Z"},
                },
            ),
            (
                "evt_stats_3",
                {
                    **sample_event,
                    "event": {"timepoint": 300, "published_at": "2025-08-15T12:32:00Z"},
                },
            ),
        ]

        for event_id, event_data in events:
            await tracker.track_event(event_id, event_data)

        # Process some events
        await tracker.mark_completed("evt_stats_1")
        await tracker.mark_failed("evt_stats_2", "Error")
        # evt_stats_3 remains pending

        stats = await tracker.get_statistics()

        assert stats is not None and stats["processed_count"] == 3
        assert stats is not None and stats["failed_count"] == 1
        assert stats is not None and stats["last_timepoint"] == 300
        assert stats["events"]["total_events"] == 3
        assert stats["events"]["completed_events"] == 1
        assert stats["events"]["failed_events"] == 1
        assert stats["events"]["pending_events"] == 1

        await tracker.disconnect()

    @pytest.mark.asyncio
    async def test_cleanup_old_events(self, config: Any, sample_event: Any) -> None:
        """Test cleaning up old completed events."""
        tracker = EventTracker(config)
        await tracker.connect()

        # Create events with different ages
        old_event = {
            **sample_event,
            "event": {"timepoint": 1, "published_at": "2025-01-01T00:00:00Z"},
        }
        recent_event = {
            **sample_event,
            "event": {"timepoint": 1000, "published_at": datetime.now().isoformat()},
        }

        await tracker.track_event("evt_old", old_event)
        await tracker.track_event("evt_recent", recent_event)

        # Mark both as completed
        await tracker.mark_completed("evt_old")
        await tracker.mark_completed("evt_recent")

        # Manually update processed_at for old event to be 60 days ago
        old_processed_at = (datetime.now() - timedelta(days=60)).isoformat()
        if tracker.logger._connection is not None:
            await tracker.logger._connection.execute(
                """
                UPDATE stream_events
                SET processed_at = ?
                WHERE event_id = ?
            """,
                (old_processed_at, "evt_old"),
            )
            await tracker.logger._connection.commit()

        # Cleanup events older than 30 days
        deleted_count = await tracker.cleanup_old_events(days=30)

        assert deleted_count == 1  # Only old event should be deleted

        # Verify old event is gone
        old = await tracker.logger.get_event("evt_old")
        assert old is None

        # Verify recent event still exists
        recent = await tracker.logger.get_event("evt_recent")
        assert recent is not None

        await tracker.disconnect()


class TestProcessingStatus:
    """Test ProcessingStatus enum."""

    def test_processing_status_values(self) -> None:
        """Test ProcessingStatus enum values."""
        assert ProcessingStatus.PENDING.value == "pending"
        assert ProcessingStatus.PROCESSING.value == "processing"
        assert ProcessingStatus.COMPLETED.value == "completed"
        assert ProcessingStatus.FAILED.value == "failed"
        assert ProcessingStatus.RETRYING.value == "retrying"

    def test_processing_status_from_string(self) -> None:
        """Test creating ProcessingStatus from string."""
        assert ProcessingStatus("pending") == ProcessingStatus.PENDING
        assert ProcessingStatus("processing") == ProcessingStatus.PROCESSING
        assert ProcessingStatus("completed") == ProcessingStatus.COMPLETED
        assert ProcessingStatus("failed") == ProcessingStatus.FAILED
        assert ProcessingStatus("retrying") == ProcessingStatus.RETRYING
