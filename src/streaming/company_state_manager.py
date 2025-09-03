"""Company State Manager for bulletproof API rate limiting and processing state tracking.

This module provides the ProcessingState enum and CompanyStateManager class
for managing company processing states in a queue-aware architecture that ensures
zero direct API calls and 100% rate limit compliance for cloud deployment.
"""

import asyncio
import sqlite3
import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class ProcessingState(Enum):
    """Processing states for company data management with queue integration.

    This enum defines all valid states in the company processing pipeline,
    ensuring bulletproof state tracking from initial detection through completion.

    State Flow:
    DETECTED -> STATUS_QUEUED -> STATUS_FETCHED -> STRIKE_OFF_CONFIRMED ->
    OFFICERS_QUEUED -> OFFICERS_FETCHED -> COMPLETED

    Error states can be reached from any processing state.
    """

    # Initial detection state
    DETECTED = "detected"

    # Status checking states
    STATUS_QUEUED = "status_queued"
    STATUS_FETCHED = "status_fetched"

    # Strike-off confirmation
    STRIKE_OFF_CONFIRMED = "strike_off_confirmed"

    # Officers processing states
    OFFICERS_QUEUED = "officers_queued"
    OFFICERS_FETCHED = "officers_fetched"

    # Terminal states
    COMPLETED = "completed"
    FAILED = "failed"

    @classmethod
    def get_valid_states(cls) -> list[str]:
        """Get list of all valid processing states.

        Returns:
            List of all valid state string values
        """
        return [state.value for state in cls]

    @classmethod
    def get_valid_transitions(cls) -> dict[str, list[str]]:
        """Get valid state transitions for enforcement.

        Returns:
            Dictionary mapping each state to its valid next states
        """
        return {
            cls.DETECTED.value: [cls.STATUS_QUEUED.value, cls.FAILED.value],
            cls.STATUS_QUEUED.value: [cls.STATUS_FETCHED.value, cls.FAILED.value],
            cls.STATUS_FETCHED.value: [
                cls.STRIKE_OFF_CONFIRMED.value,
                cls.COMPLETED.value,  # Non-strike-off companies complete here
                cls.FAILED.value,
            ],
            cls.STRIKE_OFF_CONFIRMED.value: [cls.OFFICERS_QUEUED.value, cls.FAILED.value],
            cls.OFFICERS_QUEUED.value: [cls.OFFICERS_FETCHED.value, cls.FAILED.value],
            cls.OFFICERS_FETCHED.value: [cls.COMPLETED.value, cls.FAILED.value],
            cls.COMPLETED.value: [],  # Terminal state - no further transitions
            cls.FAILED.value: [],  # Terminal state - no further transitions
        }

    def can_transition_to(self, target_state: "ProcessingState") -> bool:
        """Check if transition to target state is valid.

        Args:
            target_state: The state to transition to

        Returns:
            True if transition is valid, False otherwise
        """
        valid_transitions = self.get_valid_transitions()
        return target_state.value in valid_transitions.get(self.value, [])

    def is_terminal(self) -> bool:
        """Check if this is a terminal state.

        Returns:
            True if this is a terminal state (COMPLETED or FAILED)
        """
        return self in (ProcessingState.COMPLETED, ProcessingState.FAILED)

    def is_queue_aware(self) -> bool:
        """Check if this state involves queue operations.

        Returns:
            True if this state represents queued or queue-dependent operations
        """
        return self in (
            ProcessingState.STATUS_QUEUED,
            ProcessingState.STATUS_FETCHED,
            ProcessingState.OFFICERS_QUEUED,
            ProcessingState.OFFICERS_FETCHED,
        )

    def requires_api_call(self) -> bool:
        """Check if this state requires an API call to be made.

        Returns:
            True if transitioning to this state requires queuing an API request
        """
        return self in (ProcessingState.STATUS_QUEUED, ProcessingState.OFFICERS_QUEUED)

    def __str__(self) -> str:
        """String representation of the processing state."""
        return self.value


class CompanyStateManager:
    """Bulletproof company processing state manager with queue integration.

    This class provides bulletproof state management for company processing
    in cloud deployment environments, ensuring zero direct API calls and
    100% rate limit compliance through queue-only architecture.

    Key Features:
    - Thread-safe state transitions using asyncio locks
    - Database persistence for restart resilience
    - Queue-only API architecture (no direct API calls)
    - Rate limit violation tracking and handling
    - Autonomous operation with metrics and cleanup
    """

    def __init__(self, database_path: str, queue_manager: "PriorityQueueManager") -> None:
        """Initialize the CompanyStateManager.

        Args:
            database_path: Path to SQLite database
            queue_manager: PriorityQueueManager for API requests

        Raises:
            TypeError: If queue_manager is None (required for bulletproof operation)
            ValueError: If database_path is invalid
        """
        if queue_manager is None:
            raise TypeError("queue_manager is required for bulletproof rate limit compliance")
        if not database_path:
            raise ValueError("database_path is required")

        self.database_path = database_path
        self.queue_manager = queue_manager
        self._state_locks: dict[str, asyncio.Lock] = {}
        self._db_lock = asyncio.Lock()
        self._initialized = False

    async def initialize(self) -> None:
        """Initialize database connection and state management."""
        if self._initialized:
            return

        async with self._db_lock:
            # Verify database connection and required tables exist
            conn = sqlite3.connect(self.database_path)
            try:
                cursor = conn.cursor()

                # Verify company_processing_state table exists
                cursor.execute("""
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name='company_processing_state'
                """)
                if not cursor.fetchone():
                    raise RuntimeError(
                        "company_processing_state table not found. Run database migration first."
                    )

                # Verify api_rate_limit_log table exists
                cursor.execute("""
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name='api_rate_limit_log'
                """)
                if not cursor.fetchone():
                    raise RuntimeError(
                        "api_rate_limit_log table not found. Run database migration first."
                    )

            finally:
                conn.close()

        self._initialized = True

    async def _get_company_lock(self, company_number: str) -> asyncio.Lock:
        """Get or create an asyncio lock for a specific company.

        Args:
            company_number: Company number to get lock for

        Returns:
            Asyncio lock for the company
        """
        if company_number not in self._state_locks:
            self._state_locks[company_number] = asyncio.Lock()
        return self._state_locks[company_number]

    async def get_state(self, company_number: str) -> Optional[dict[str, Any]]:
        """Get current processing state for a company.

        Args:
            company_number: Company number to get state for

        Returns:
            Dictionary with company state data if found, None otherwise
        """
        if not self._initialized:
            await self.initialize()

        company_lock = await self._get_company_lock(company_number)
        async with company_lock:
            conn = sqlite3.connect(self.database_path)
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT
                        company_number, processing_state, created_at, updated_at,
                        status_queued_at, status_fetched_at, officers_queued_at,
                        officers_fetched_at, completed_at, status_retry_count,
                        officers_retry_count, status_request_id, officers_request_id,
                        last_error, last_error_at, last_429_response_at, rate_limit_violations
                    FROM company_processing_state
                    WHERE company_number = ?
                """,
                    (company_number,),
                )

                row = cursor.fetchone()
                if not row:
                    return None

                return {
                    "company_number": row[0],
                    "processing_state": row[1],
                    "created_at": row[2],
                    "updated_at": row[3],
                    "status_queued_at": row[4],
                    "status_fetched_at": row[5],
                    "officers_queued_at": row[6],
                    "officers_fetched_at": row[7],
                    "completed_at": row[8],
                    "status_retry_count": row[9],
                    "officers_retry_count": row[10],
                    "status_request_id": row[11],
                    "officers_request_id": row[12],
                    "last_error": row[13],
                    "last_error_at": row[14],
                    "last_429_response_at": row[15],
                    "rate_limit_violations": row[16],
                }
            finally:
                conn.close()

    async def update_state(self, company_number: str, state: str, **kwargs: Any) -> dict[str, Any]:
        """Update processing state with thread safety.

        Args:
            company_number: Company number to update
            state: New processing state
            **kwargs: Additional state data to update

        Returns:
            Updated state dictionary

        Raises:
            ValueError: If state transition is invalid
        """
        if not self._initialized:
            await self.initialize()

        # Validate state
        try:
            new_state = ProcessingState(state)
        except ValueError as e:
            raise ValueError(f"Invalid processing state '{state}'") from e

        company_lock = await self._get_company_lock(company_number)
        async with company_lock:
            conn = sqlite3.connect(self.database_path)
            try:
                # Get current state (without locking since we already have the lock)
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT
                        company_number, processing_state, created_at, updated_at,
                        status_queued_at, status_fetched_at, officers_queued_at,
                        officers_fetched_at, completed_at, status_retry_count,
                        officers_retry_count, status_request_id, officers_request_id,
                        last_error, last_error_at, last_429_response_at, rate_limit_violations
                    FROM company_processing_state
                    WHERE company_number = ?
                """,
                    (company_number,),
                )

                row = cursor.fetchone()
                current_data = None
                if row:
                    current_data = {
                        "company_number": row[0],
                        "processing_state": row[1],
                        "created_at": row[2],
                        "updated_at": row[3],
                        "status_queued_at": row[4],
                        "status_fetched_at": row[5],
                        "officers_queued_at": row[6],
                        "officers_fetched_at": row[7],
                        "completed_at": row[8],
                        "status_retry_count": row[9],
                        "officers_retry_count": row[10],
                        "status_request_id": row[11],
                        "officers_request_id": row[12],
                        "last_error": row[13],
                        "last_error_at": row[14],
                        "last_429_response_at": row[15],
                        "rate_limit_violations": row[16],
                    }

                if current_data:
                    current_state = ProcessingState(current_data["processing_state"])
                    # Validate transition (allow same state for data updates)
                    if current_state != new_state and not current_state.can_transition_to(
                        new_state
                    ):
                        raise ValueError(
                            f"Invalid state transition from '{current_state.value}' "
                            f"to '{new_state.value}'"
                        )

                now = datetime.now().isoformat()

                # Prepare update data
                update_data = {
                    "processing_state": state,
                    "updated_at": now,
                }

                # Add timestamp fields based on state
                if new_state == ProcessingState.STATUS_QUEUED:
                    update_data["status_queued_at"] = now
                elif new_state == ProcessingState.STATUS_FETCHED:
                    update_data["status_fetched_at"] = now
                elif new_state == ProcessingState.OFFICERS_QUEUED:
                    update_data["officers_queued_at"] = now
                elif new_state == ProcessingState.OFFICERS_FETCHED:
                    update_data["officers_fetched_at"] = now
                elif new_state == ProcessingState.COMPLETED:
                    update_data["completed_at"] = now

                # Add any additional kwargs
                for key, value in kwargs.items():
                    if key not in ["company_number", "id"]:  # Prevent overwriting protected fields
                        update_data[key] = value

                # Insert or update record
                if current_data:
                    # Update existing record
                    set_clause = ", ".join(f"{key} = ?" for key in update_data)
                    sql = f"""
                        UPDATE company_processing_state
                        SET {set_clause}
                        WHERE company_number = ?
                    """
                    params = list(update_data.values()) + [company_number]
                else:
                    # Insert new record
                    update_data.update(
                        {
                            "company_number": company_number,
                            "created_at": now,
                        }
                    )
                    columns = ", ".join(update_data.keys())
                    placeholders = ", ".join("?" * len(update_data))
                    sql = f"""
                        INSERT INTO company_processing_state ({columns})
                        VALUES ({placeholders})
                    """
                    params = list(update_data.values())

                cursor.execute(sql, params)
                conn.commit()

                # Return updated state by querying again (within the same lock)
                cursor.execute(
                    """
                    SELECT
                        company_number, processing_state, created_at, updated_at,
                        status_queued_at, status_fetched_at, officers_queued_at,
                        officers_fetched_at, completed_at, status_retry_count,
                        officers_retry_count, status_request_id, officers_request_id,
                        last_error, last_error_at, last_429_response_at, rate_limit_violations
                    FROM company_processing_state
                    WHERE company_number = ?
                """,
                    (company_number,),
                )

                row = cursor.fetchone()
                if not row:
                    return None

                return {
                    "company_number": row[0],
                    "processing_state": row[1],
                    "created_at": row[2],
                    "updated_at": row[3],
                    "status_queued_at": row[4],
                    "status_fetched_at": row[5],
                    "officers_queued_at": row[6],
                    "officers_fetched_at": row[7],
                    "completed_at": row[8],
                    "status_retry_count": row[9],
                    "officers_retry_count": row[10],
                    "status_request_id": row[11],
                    "officers_request_id": row[12],
                    "last_error": row[13],
                    "last_error_at": row[14],
                    "last_429_response_at": row[15],
                    "rate_limit_violations": row[16],
                }

            finally:
                conn.close()

    async def queue_status_check(self, company_number: str) -> str:
        """Queue a company status check request.

        Args:
            company_number: Company number to queue status check for

        Returns:
            Request ID of the queued request
        """
        if not self._initialized:
            await self.initialize()

        from .queue_manager import QueuedRequest, RequestPriority

        request_id = f"status_{company_number}_{uuid.uuid4().hex[:8]}"

        request = QueuedRequest(
            request_id=request_id,
            priority=RequestPriority.HIGH,  # Status checks are high priority
            endpoint=f"/company/{company_number}",
            params={
                "company_number": company_number,
                "request_type": "company_status",
                "state_manager_queued": True,
            },
            callback=None,  # Callback will be handled by queue processor
        )

        success = await self.queue_manager.enqueue(request)
        if not success:
            raise RuntimeError(f"Failed to queue status check for company {company_number}")

        # Update state to STATUS_QUEUED with request ID
        await self.update_state(
            company_number, ProcessingState.STATUS_QUEUED.value, status_request_id=request_id
        )

        return request_id

    async def queue_officers_fetch(self, company_number: str) -> str:
        """Queue an officers fetch request.

        Args:
            company_number: Company number to queue officers fetch for

        Returns:
            Request ID of the queued request
        """
        if not self._initialized:
            await self.initialize()

        from .queue_manager import QueuedRequest, RequestPriority

        request_id = f"officers_{company_number}_{uuid.uuid4().hex[:8]}"

        request = QueuedRequest(
            request_id=request_id,
            priority=RequestPriority.MEDIUM,  # Officers fetches are medium priority
            endpoint=f"/company/{company_number}/officers",
            params={
                "company_number": company_number,
                "request_type": "officers",
                "state_manager_queued": True,
            },
            callback=None,  # Callback will be handled by queue processor
        )

        success = await self.queue_manager.enqueue(request)
        if not success:
            raise RuntimeError(f"Failed to queue officers fetch for company {company_number}")

        # Update state to OFFICERS_QUEUED with request ID
        await self.update_state(
            company_number, ProcessingState.OFFICERS_QUEUED.value, officers_request_id=request_id
        )

        return request_id

    async def handle_429_response(self, company_number: str, request_id: str) -> None:
        """Handle rate limit violation (429 response).

        Args:
            company_number: Company number that received 429 response
            request_id: Request ID that received 429 response
        """
        if not self._initialized:
            await self.initialize()

        now = datetime.now().isoformat()

        # Get current state
        current_data = await self.get_state(company_number)
        if not current_data:
            return  # No state to update

        # Increment rate limit violations
        new_violations = current_data.get("rate_limit_violations", 0) + 1

        # Log rate limit violation
        conn = sqlite3.connect(self.database_path)
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO api_rate_limit_log
                (timestamp, endpoint, response_code, company_number, request_id, details)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    now,
                    current_data.get("status_request_id") == request_id
                    and f"/company/{company_number}"
                    or f"/company/{company_number}/officers",
                    429,
                    company_number,
                    request_id,
                    "Rate limit violation handled by CompanyStateManager",
                ),
            )
            conn.commit()
        finally:
            conn.close()

        # Update company state
        await self.update_state(
            company_number,
            current_data["processing_state"],  # Keep current state
            last_429_response_at=now,
            rate_limit_violations=new_violations,
            last_error=f"Rate limit violation (429) for request {request_id}",
            last_error_at=now,
        )

    async def cleanup_failed_companies(self) -> int:
        """Clean up companies that have exceeded retry limits.

        Returns:
            Number of companies cleaned up
        """
        if not self._initialized:
            await self.initialize()

        # Define cleanup criteria
        max_retry_count = 5
        max_rate_limit_violations = 10

        conn = sqlite3.connect(self.database_path)
        try:
            cursor = conn.cursor()

            # Find companies that should be marked as failed
            cursor.execute(
                """
                SELECT company_number, processing_state, status_retry_count,
                       officers_retry_count, rate_limit_violations
                FROM company_processing_state
                WHERE processing_state NOT IN ('completed', 'failed')
                  AND (status_retry_count >= ? OR officers_retry_count >= ?
                       OR rate_limit_violations >= ?)
            """,
                (max_retry_count, max_retry_count, max_rate_limit_violations),
            )

            failed_companies = cursor.fetchall()
            cleanup_count = 0

            for row in failed_companies:
                company_number = row[0]

                # Update to failed state
                await self.update_state(
                    company_number,
                    ProcessingState.FAILED.value,
                    last_error="Exceeded retry limits or rate limit violations",
                    last_error_at=datetime.now().isoformat(),
                )
                cleanup_count += 1

        finally:
            conn.close()

        return cleanup_count

    async def get_processing_metrics(self) -> dict[str, Any]:
        """Get processing metrics for monitoring.

        Returns:
            Dictionary with processing metrics
        """
        if not self._initialized:
            await self.initialize()

        conn = sqlite3.connect(self.database_path)
        try:
            cursor = conn.cursor()

            # Get state distribution
            cursor.execute("""
                SELECT processing_state, COUNT(*) as count
                FROM company_processing_state
                GROUP BY processing_state
            """)
            state_counts = dict(cursor.fetchall())

            # Get retry statistics
            cursor.execute("""
                SELECT
                    AVG(status_retry_count) as avg_status_retries,
                    AVG(officers_retry_count) as avg_officers_retries,
                    MAX(status_retry_count) as max_status_retries,
                    MAX(officers_retry_count) as max_officers_retries,
                    SUM(rate_limit_violations) as total_rate_limit_violations
                FROM company_processing_state
            """)
            retry_stats = cursor.fetchone()

            # Get recent rate limit violations (last 24 hours)
            cursor.execute("""
                SELECT COUNT(*)
                FROM api_rate_limit_log
                WHERE timestamp > datetime('now', '-1 day')
                  AND response_code = 429
            """)
            recent_429s = cursor.fetchone()[0]

            # Get queue metrics from queue manager
            queue_metrics = self.queue_manager.get_metrics()

            return {
                "state_distribution": state_counts,
                "retry_statistics": {
                    "avg_status_retries": retry_stats[0] or 0,
                    "avg_officers_retries": retry_stats[1] or 0,
                    "max_status_retries": retry_stats[2] or 0,
                    "max_officers_retries": retry_stats[3] or 0,
                    "total_rate_limit_violations": retry_stats[4] or 0,
                },
                "recent_rate_limit_violations_24h": recent_429s,
                "queue_metrics": queue_metrics,
                "timestamp": datetime.now().isoformat(),
            }

        finally:
            conn.close()
