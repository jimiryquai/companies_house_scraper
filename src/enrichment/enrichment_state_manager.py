"""Enrichment state tracking and management.

This module extends the existing ProcessingState system to include enrichment
states, providing comprehensive tracking of domain discovery and email finding
operations with database persistence.
"""

import asyncio
import logging
import sqlite3
from datetime import datetime
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


class EnrichmentState(Enum):
    """Extended processing states for enrichment operations."""

    # Enrichment workflow states
    ENRICHMENT_ELIGIBLE = "enrichment_eligible"  # Ready for enrichment
    DOMAIN_QUEUED = "domain_queued"              # Domain discovery queued
    DOMAIN_IN_PROGRESS = "domain_in_progress"    # Domain discovery in progress
    DOMAIN_COMPLETED = "domain_completed"        # Domain discovery completed
    DOMAIN_FAILED = "domain_failed"              # Domain discovery failed

    OFFICERS_QUEUED = "officers_queued"          # Officer email discovery queued
    OFFICERS_IN_PROGRESS = "officers_in_progress"  # Officer email discovery in progress
    OFFICERS_COMPLETED = "officers_completed"    # Officer email discovery completed
    OFFICERS_FAILED = "officers_failed"          # Officer email discovery failed

    # Final enrichment states
    ENRICHMENT_COMPLETED = "enrichment_completed"  # All enrichment complete
    ENRICHMENT_FAILED = "enrichment_failed"        # Enrichment failed
    ENRICHMENT_SKIPPED = "enrichment_skipped"      # Enrichment skipped (no officers, etc.)

    @classmethod
    def get_valid_transitions(cls) -> dict[str, list[str]]:
        """Get valid state transitions for enrichment workflow.

        Returns:
            Dictionary mapping each state to its valid next states
        """
        return {
            cls.ENRICHMENT_ELIGIBLE.value: [
                cls.DOMAIN_QUEUED.value,
                cls.ENRICHMENT_SKIPPED.value,
                cls.ENRICHMENT_FAILED.value,
            ],
            cls.DOMAIN_QUEUED.value: [
                cls.DOMAIN_IN_PROGRESS.value,
                cls.DOMAIN_FAILED.value,
            ],
            cls.DOMAIN_IN_PROGRESS.value: [
                cls.DOMAIN_COMPLETED.value,
                cls.DOMAIN_FAILED.value,
            ],
            cls.DOMAIN_COMPLETED.value: [
                cls.OFFICERS_QUEUED.value,
                cls.ENRICHMENT_COMPLETED.value,  # If no officers to process
                cls.ENRICHMENT_FAILED.value,
            ],
            cls.DOMAIN_FAILED.value: [
                cls.DOMAIN_QUEUED.value,  # Retry
                cls.ENRICHMENT_FAILED.value,
            ],
            cls.OFFICERS_QUEUED.value: [
                cls.OFFICERS_IN_PROGRESS.value,
                cls.OFFICERS_FAILED.value,
            ],
            cls.OFFICERS_IN_PROGRESS.value: [
                cls.OFFICERS_COMPLETED.value,
                cls.OFFICERS_FAILED.value,
            ],
            cls.OFFICERS_COMPLETED.value: [
                cls.ENRICHMENT_COMPLETED.value,
            ],
            cls.OFFICERS_FAILED.value: [
                cls.OFFICERS_QUEUED.value,  # Retry
                cls.ENRICHMENT_FAILED.value,
            ],
            # Terminal states
            cls.ENRICHMENT_COMPLETED.value: [],
            cls.ENRICHMENT_FAILED.value: [],
            cls.ENRICHMENT_SKIPPED.value: [],
        }

    def can_transition_to(self, target_state: "EnrichmentState") -> bool:
        """Check if transition to target state is valid.

        Args:
            target_state: The state to transition to

        Returns:
            True if transition is valid, False otherwise
        """
        valid_transitions = self.get_valid_transitions()
        return target_state.value in valid_transitions.get(self.value, [])

    def is_terminal(self) -> bool:
        """Check if this is a terminal enrichment state.

        Returns:
            True if this is a terminal state
        """
        return self in (
            EnrichmentState.ENRICHMENT_COMPLETED,
            EnrichmentState.ENRICHMENT_FAILED,
            EnrichmentState.ENRICHMENT_SKIPPED,
        )


class EnrichmentStateError(Exception):
    """Raised when enrichment state operations fail."""

    pass


class EnrichmentStateManager:
    """Manages enrichment processing states with database persistence.

    This class extends the existing state management system to track
    domain discovery and email finding operations, ensuring proper
    workflow sequencing and data integrity.
    """

    def __init__(self, database_path: str = "companies.db"):
        """Initialize enrichment state manager.

        Args:
            database_path: Path to SQLite database
        """
        self.database_path = database_path
        self._db_lock = asyncio.Lock()
        self._initialized = False

        # Statistics tracking
        self.state_transitions = 0
        self.enrichment_operations = 0
        self.start_time = datetime.now()

    async def initialize(self) -> None:
        """Initialize database connection and tables."""
        if self._initialized:
            return

        async with self._db_lock:
            conn = sqlite3.connect(self.database_path)
            try:
                cursor = conn.cursor()

                # Create enrichment_state table if not exists
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS enrichment_state (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        company_number TEXT NOT NULL,
                        enrichment_state TEXT NOT NULL DEFAULT 'enrichment_eligible',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        domain_queued_at TIMESTAMP NULL,
                        domain_completed_at TIMESTAMP NULL,
                        officers_queued_at TIMESTAMP NULL,
                        officers_completed_at TIMESTAMP NULL,
                        enrichment_completed_at TIMESTAMP NULL,
                        retry_count INTEGER DEFAULT 0,
                        max_retries INTEGER DEFAULT 3,
                        last_error TEXT NULL,
                        last_error_at TIMESTAMP NULL,
                        domains_found INTEGER DEFAULT 0,
                        officers_processed INTEGER DEFAULT 0,
                        emails_found INTEGER DEFAULT 0,
                        snov_request_id TEXT NULL,
                        
                        FOREIGN KEY (company_number) REFERENCES companies(company_number),
                        UNIQUE(company_number)
                    )
                """)

                # Create indexes for performance
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_enrichment_state_company 
                    ON enrichment_state(company_number)
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_enrichment_state_status 
                    ON enrichment_state(enrichment_state)
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_enrichment_state_updated 
                    ON enrichment_state(updated_at)
                """)

                conn.commit()
                logger.info("Enrichment state management tables initialized")

            finally:
                conn.close()

        self._initialized = True

    async def get_enrichment_state(self, company_number: str) -> Optional[dict[str, Any]]:
        """Get current enrichment state for a company.

        Args:
            company_number: Company number to get state for

        Returns:
            Dictionary with enrichment state data if found, None otherwise
        """
        if not self._initialized:
            await self.initialize()

        async with self._db_lock:
            conn = sqlite3.connect(self.database_path)
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 
                        company_number, enrichment_state, created_at, updated_at,
                        domain_queued_at, domain_completed_at, officers_queued_at,
                        officers_completed_at, enrichment_completed_at, retry_count,
                        max_retries, last_error, last_error_at, domains_found,
                        officers_processed, emails_found, snov_request_id
                    FROM enrichment_state
                    WHERE company_number = ?
                """, (company_number,))

                row = cursor.fetchone()
                if not row:
                    return None

                return {
                    "company_number": row[0],
                    "enrichment_state": row[1],
                    "created_at": row[2],
                    "updated_at": row[3],
                    "domain_queued_at": row[4],
                    "domain_completed_at": row[5],
                    "officers_queued_at": row[6],
                    "officers_completed_at": row[7],
                    "enrichment_completed_at": row[8],
                    "retry_count": row[9],
                    "max_retries": row[10],
                    "last_error": row[11],
                    "last_error_at": row[12],
                    "domains_found": row[13],
                    "officers_processed": row[14],
                    "emails_found": row[15],
                    "snov_request_id": row[16],
                }

            finally:
                conn.close()

    async def update_enrichment_state(
        self,
        company_number: str,
        state: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Update enrichment state for a company.

        Args:
            company_number: Company number to update
            state: New enrichment state
            **kwargs: Additional state data to update

        Returns:
            Updated state dictionary

        Raises:
            EnrichmentStateError: If state transition is invalid
        """
        if not self._initialized:
            await self.initialize()

        # Validate state
        try:
            new_state = EnrichmentState(state)
        except ValueError as e:
            raise EnrichmentStateError(f"Invalid enrichment state '{state}'") from e

        async with self._db_lock:
            conn = sqlite3.connect(self.database_path)
            try:
                # Get current state
                current_data = await self.get_enrichment_state(company_number)

                if current_data:
                    current_state = EnrichmentState(current_data["enrichment_state"])
                    # Validate transition (allow same state for data updates)
                    if current_state != new_state and not current_state.can_transition_to(new_state):
                        raise EnrichmentStateError(
                            f"Invalid enrichment state transition from '{current_state.value}' "
                            f"to '{new_state.value}'"
                        )

                now = datetime.now().isoformat()

                # Prepare update data
                update_data = {
                    "enrichment_state": state,
                    "updated_at": now,
                }

                # Add timestamp fields based on state
                if new_state == EnrichmentState.DOMAIN_QUEUED:
                    update_data["domain_queued_at"] = now
                elif new_state == EnrichmentState.DOMAIN_COMPLETED:
                    update_data["domain_completed_at"] = now
                elif new_state == EnrichmentState.OFFICERS_QUEUED:
                    update_data["officers_queued_at"] = now
                elif new_state == EnrichmentState.OFFICERS_COMPLETED:
                    update_data["officers_completed_at"] = now
                elif new_state == EnrichmentState.ENRICHMENT_COMPLETED:
                    update_data["enrichment_completed_at"] = now

                # Add any additional kwargs
                for key, value in kwargs.items():
                    if key not in ["company_number", "id"]:
                        update_data[key] = value

                cursor = conn.cursor()

                # Insert or update record
                if current_data:
                    # Update existing record
                    set_clause = ", ".join(f"{key} = ?" for key in update_data)
                    sql = f"""
                        UPDATE enrichment_state
                        SET {set_clause}
                        WHERE company_number = ?
                    """
                    params = list(update_data.values()) + [company_number]
                else:
                    # Insert new record
                    update_data.update({
                        "company_number": company_number,
                        "created_at": now,
                    })
                    columns = ", ".join(update_data.keys())
                    placeholders = ", ".join("?" * len(update_data))
                    sql = f"""
                        INSERT INTO enrichment_state ({columns})
                        VALUES ({placeholders})
                    """
                    params = list(update_data.values())

                cursor.execute(sql, params)
                conn.commit()

                self.state_transitions += 1
                self.enrichment_operations += 1

                logger.debug(f"Updated enrichment state for {company_number} to {state}")

                # Return updated state
                return await self.get_enrichment_state(company_number) or {}

            finally:
                conn.close()

    async def is_eligible_for_enrichment(self, company_number: str) -> bool:
        """Check if company is eligible for enrichment.

        Args:
            company_number: Company number to check

        Returns:
            True if company is eligible for enrichment
        """
        try:
            state_data = await self.get_enrichment_state(company_number)

            if not state_data:
                # No enrichment state yet - check if company processing is complete
                # This would integrate with CompanyStateManager
                return True  # Simplified for now

            current_state = EnrichmentState(state_data["enrichment_state"])

            # Company is eligible if in terminal failure state or not started
            return current_state in (
                EnrichmentState.ENRICHMENT_ELIGIBLE,
                EnrichmentState.ENRICHMENT_FAILED,
            )

        except Exception as e:
            logger.error(f"Error checking enrichment eligibility for {company_number}: {e}")
            return False

    async def get_enrichment_statistics(self) -> dict[str, Any]:
        """Get enrichment processing statistics.

        Returns:
            Dictionary with enrichment metrics
        """
        if not self._initialized:
            await self.initialize()

        async with self._db_lock:
            conn = sqlite3.connect(self.database_path)
            try:
                cursor = conn.cursor()

                # Get state distribution
                cursor.execute("""
                    SELECT enrichment_state, COUNT(*) as count
                    FROM enrichment_state
                    GROUP BY enrichment_state
                """)
                state_counts = dict(cursor.fetchall())

                # Get summary statistics
                cursor.execute("""
                    SELECT
                        COUNT(*) as total_companies,
                        SUM(domains_found) as total_domains,
                        SUM(officers_processed) as total_officers,
                        SUM(emails_found) as total_emails,
                        AVG(retry_count) as avg_retries
                    FROM enrichment_state
                """)
                summary = cursor.fetchone()

                return {
                    "state_distribution": state_counts,
                    "total_companies": summary[0] or 0,
                    "total_domains_found": summary[1] or 0,
                    "total_officers_processed": summary[2] or 0,
                    "total_emails_found": summary[3] or 0,
                    "average_retry_count": round(summary[4] or 0, 2),
                    "state_transitions": self.state_transitions,
                    "enrichment_operations": self.enrichment_operations,
                    "uptime_seconds": (datetime.now() - self.start_time).total_seconds(),
                }

            finally:
                conn.close()

    def get_health_status(self) -> dict[str, Any]:
        """Get enrichment state manager health status.

        Returns:
            Health status information
        """
        return {
            "component": "enrichment_state_manager",
            "status": "UP",
            "message": "Enrichment state manager operational",
            "timestamp": datetime.now().isoformat(),
            "details": {
                "initialized": self._initialized,
                "state_transitions": self.state_transitions,
                "enrichment_operations": self.enrichment_operations,
            },
        }

    def get_metrics(self) -> dict[str, Any]:
        """Get enrichment state metrics.

        Returns:
            Metrics data for monitoring
        """
        return {
            "enrichment_state_transitions": self.state_transitions,
            "enrichment_operations_total": self.enrichment_operations,
            "enrichment_manager_initialized": self._initialized,
        }

