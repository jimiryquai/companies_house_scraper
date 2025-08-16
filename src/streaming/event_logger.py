"""Event logging and tracking functionality for streaming events."""

import json
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import aiosqlite

from .config import StreamingConfig
from .database import StreamingDatabase


class ProcessingStatus(Enum):
    """Event processing status."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


class EventLogError(Exception):
    """Event logging error."""

    pass


class EventLogger:
    """Manages event logging and persistence."""

    def __init__(self, config: StreamingConfig):
        """Initialize EventLogger."""
        self.config = config
        self.db = StreamingDatabase(config)
        self.processing_queue: List[str] = []
        self.failed_events: List[str] = []
        self._connection: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        """Establish database connection."""
        await self.db.connect()
        self._connection = await aiosqlite.connect(self.config.database_path)

    async def disconnect(self) -> None:
        """Close database connection."""
        if self._connection:
            await self._connection.close()
            self._connection = None
        await self.db.disconnect()

    async def log_event(self, event_id: str, event_data: Dict[str, Any]) -> Optional[str]:
        """Log an event to the database.

        Returns event_id if successful, None if duplicate.
        """
        if not self._connection:
            await self.connect()

        if not self._connection:
            raise EventLogError("Database connection not established")

        try:
            # Check for duplicate
            cursor = await self._connection.execute(
                "SELECT event_id FROM stream_events WHERE event_id = ?", (event_id,)
            )
            existing = await cursor.fetchone()

            if existing:
                return None  # Duplicate event

            # Extract event type and company number
            event_type = event_data.get("resource_kind", "unknown")
            company_number = event_data.get("resource_id", "")
            if not company_number and "data" in event_data:
                company_number = event_data["data"].get("company_number", "")

            # Insert new event
            if not self._connection:
                raise EventLogError("Database connection lost")
            await self._connection.execute(
                """
                INSERT INTO stream_events (
                    event_id, event_type, company_number,
                    event_data, processing_status, retry_count
                ) VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    event_id,
                    event_type,
                    company_number,
                    json.dumps(event_data),
                    ProcessingStatus.PENDING.value,
                    0,
                ),
            )

            if self._connection:
                await self._connection.commit()
            return event_id

        except Exception as e:
            raise EventLogError(f"Failed to log event: {e}")

    async def get_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Get event by ID."""
        if not self._connection:
            await self.connect()

        if not self._connection:
            raise EventLogError("Database connection not established")
        cursor = await self._connection.execute(
            """
            SELECT event_id, event_type, company_number, event_data,
                   processing_status, retry_count, error_message, processed_at
            FROM stream_events
            WHERE event_id = ?
        """,
            (event_id,),
        )

        row = await cursor.fetchone()
        if not row:
            return None

        return {
            "event_id": row[0],
            "event_type": row[1],
            "company_number": row[2],
            "event_data": json.loads(row[3]) if row[3] else {},
            "processing_status": row[4],
            "retry_count": row[5],
            "error_message": row[6],
            "processed_at": row[7],
        }

    async def update_event_status(
        self, event_id: str, status: ProcessingStatus, error_message: Optional[str] = None
    ) -> None:
        """Update event processing status."""
        if not self._connection:
            await self.connect()

        if not self._connection:
            raise EventLogError("Database connection not established")

        if error_message:
            await self._connection.execute(
                """
                UPDATE stream_events
                SET processing_status = ?, error_message = ?, processed_at = ?
                WHERE event_id = ?
            """,
                (status.value, error_message, datetime.now().isoformat(), event_id),
            )
        else:
            await self._connection.execute(
                """
                UPDATE stream_events
                SET processing_status = ?, processed_at = ?
                WHERE event_id = ?
            """,
                (status.value, datetime.now().isoformat(), event_id),
            )

        if self._connection:
            await self._connection.commit()

    async def increment_retry_count(self, event_id: str) -> None:
        """Increment retry count for an event."""
        if not self._connection:
            await self.connect()

        if not self._connection:
            raise EventLogError("Database connection not established")
        await self._connection.execute(
            """
            UPDATE stream_events
            SET retry_count = retry_count + 1
            WHERE event_id = ?
        """,
            (event_id,),
        )

        if self._connection:
            await self._connection.commit()

    async def get_pending_events(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get pending events for processing."""
        if not self._connection:
            await self.connect()

        if not self._connection:
            raise EventLogError("Database connection not established")
        cursor = await self._connection.execute(
            """
            SELECT event_id, event_type, company_number, event_data,
                   processing_status, retry_count, error_message, processed_at
            FROM stream_events
            WHERE processing_status = ?
            ORDER BY created_at ASC
            LIMIT ?
        """,
            (ProcessingStatus.PENDING.value, limit),
        )

        rows = await cursor.fetchall()

        return [
            {
                "event_id": row[0],
                "event_type": row[1],
                "company_number": row[2],
                "event_data": json.loads(row[3]) if row[3] else {},
                "processing_status": row[4],
                "retry_count": row[5],
                "error_message": row[6],
                "processed_at": row[7],
            }
            for row in rows
        ]

    async def get_failed_events(self, max_retries: int = 3) -> List[Dict[str, Any]]:
        """Get failed events that can be retried."""
        if not self._connection:
            await self.connect()

        if not self._connection:
            raise EventLogError("Database connection not established")
        cursor = await self._connection.execute(
            """
            SELECT event_id, event_type, company_number, event_data,
                   processing_status, retry_count, error_message, processed_at
            FROM stream_events
            WHERE processing_status = ? AND retry_count < ?
            ORDER BY created_at ASC
        """,
            (ProcessingStatus.FAILED.value, max_retries),
        )

        rows = await cursor.fetchall()

        return [
            {
                "event_id": row[0],
                "event_type": row[1],
                "company_number": row[2],
                "event_data": json.loads(row[3]) if row[3] else {},
                "processing_status": row[4],
                "retry_count": row[5],
                "error_message": row[6],
                "processed_at": row[7],
            }
            for row in rows
        ]

    async def batch_log_events(self, events: List[Tuple[str, Dict[str, Any]]]) -> List[str]:
        """Batch log multiple events."""
        results = []
        for event_id, event_data in events:
            result = await self.log_event(event_id, event_data)
            if result:
                results.append(result)
        return results

    async def get_statistics(self) -> Dict[str, Any]:
        """Get event processing statistics."""
        if not self._connection:
            await self.connect()

        if not self._connection:
            raise EventLogError("Database connection not established")
        # Get counts by status
        cursor = await self._connection.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN processing_status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN processing_status = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN processing_status = 'failed' THEN 1 ELSE 0 END) as failed
            FROM stream_events
        """)

        row = await cursor.fetchone()
        if not row:
            return {
                "total_events": 0,
                "pending_events": 0,
                "completed_events": 0,
                "failed_events": 0,
                "success_rate": 0.0,
            }
        total = row[0] or 0
        pending = row[1] or 0
        completed = row[2] or 0
        failed = row[3] or 0

        success_rate = (completed / total * 100) if total > 0 else 0.0

        return {
            "total_events": total,
            "pending_events": pending,
            "completed_events": completed,
            "failed_events": failed,
            "success_rate": success_rate,
        }


class EventTracker:
    """Tracks event processing and provides statistics."""

    def __init__(self, config: StreamingConfig):
        """Initialize EventTracker."""
        self.config = config
        self.logger = EventLogger(config)
        self.last_timepoint: Optional[int] = None
        self.processed_count = 0
        self.failed_count = 0

    async def connect(self) -> None:
        """Connect to database."""
        await self.logger.connect()

    async def disconnect(self) -> None:
        """Disconnect from database."""
        await self.logger.disconnect()

    async def track_event(self, event_id: str, event_data: Dict[str, Any]) -> bool:
        """Track an event.

        Returns True if event was tracked, False if duplicate.
        """
        # Update timepoint if present
        if "event" in event_data and "timepoint" in event_data["event"]:
            self.last_timepoint = event_data["event"]["timepoint"]

        # Log the event
        result = await self.logger.log_event(event_id, event_data)

        if result:
            self.processed_count += 1
            return True
        return False

    async def mark_processing(self, event_id: str) -> None:
        """Mark event as being processed."""
        await self.logger.update_event_status(event_id, ProcessingStatus.PROCESSING)

    async def mark_completed(self, event_id: str) -> None:
        """Mark event as completed."""
        await self.logger.update_event_status(event_id, ProcessingStatus.COMPLETED)

    async def mark_failed(self, event_id: str, error_message: str) -> None:
        """Mark event as failed."""
        await self.logger.update_event_status(
            event_id, ProcessingStatus.FAILED, error_message=error_message
        )
        self.failed_count += 1

    async def retry_failed_events(self, max_retries: int = 3) -> List[str]:
        """Retry failed events.

        Returns list of event IDs that were retried.
        """
        failed_events = await self.logger.get_failed_events(max_retries)

        retried = []
        for event in failed_events:
            event_id = event["event_id"]

            # Increment retry count
            await self.logger.increment_retry_count(event_id)

            # Reset status to pending for retry
            await self.logger.update_event_status(event_id, ProcessingStatus.PENDING)

            retried.append(event_id)

        return retried

    async def get_statistics(self) -> Dict[str, Any]:
        """Get tracking statistics."""
        event_stats = await self.logger.get_statistics()

        return {
            "processed_count": self.processed_count,
            "failed_count": self.failed_count,
            "last_timepoint": self.last_timepoint,
            "events": event_stats,
        }

    async def cleanup_old_events(self, days: int = 30) -> int:
        """Delete completed events older than specified days.

        Returns count of deleted events.
        """
        if not self.logger._connection:
            await self.logger.connect()

        cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()

        # Delete old completed events
        if not self.logger._connection:
            raise EventLogError("Database connection not established")
        cursor = await self.logger._connection.execute(
            """
            DELETE FROM stream_events
            WHERE processing_status = ? AND processed_at < ?
        """,
            (ProcessingStatus.COMPLETED.value, cutoff_date),
        )

        if self.logger._connection:
            await self.logger._connection.commit()

        return cursor.rowcount
