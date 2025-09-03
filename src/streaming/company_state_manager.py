"""Company State Manager for tracking company processing states.

This module manages the processing state of companies through their enrichment workflow,
providing a centralized way to track companies from initial detection through completion.
"""

import logging
import sqlite3
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from .database import DatabaseManager

logger = logging.getLogger("streaming_service")


class ProcessingState(Enum):
    """Company processing states enum."""
    
    DETECTED = "detected"
    STATUS_QUEUED = "status_queued" 
    STATUS_FETCHED = "status_fetched"
    STRIKE_OFF_CONFIRMED = "strike_off_confirmed"
    OFFICERS_QUEUED = "officers_queued"
    OFFICERS_FETCHED = "officers_fetched"
    COMPLETED = "completed"
    FAILED = "failed"


class CompanyStateManager:
    """Manages company processing states in the database."""

    def __init__(self, database_manager: DatabaseManager) -> None:
        """Initialize the company state manager.
        
        Args:
            database_manager: Database manager for database operations
        """
        self.db_manager = database_manager

    async def get_state(self, company_number: str) -> Optional[Dict[str, Any]]:
        """Get the current processing state for a company.
        
        Args:
            company_number: Company number to get state for
            
        Returns:
            Dictionary with company state data or None if not found
        """
        try:
            async with self.db_manager.get_connection() as conn:
                cursor = await conn.execute(
                    """
                    SELECT * FROM company_processing_state 
                    WHERE company_number = ?
                    """,
                    (company_number,)
                )
                row = await cursor.fetchone()
                
                if row:
                    # Convert row to dictionary
                    columns = [desc[0] for desc in cursor.description]
                    return dict(zip(columns, row))
                return None
                
        except sqlite3.Error as e:
            logger.error(f"Error getting state for company {company_number}: {e}")
            return None

    async def update_state(
        self, 
        company_number: str, 
        processing_state: str,
        last_error: Optional[str] = None,
        **kwargs: Any
    ) -> bool:
        """Update the processing state for a company.
        
        Args:
            company_number: Company number to update
            processing_state: New processing state
            last_error: Optional error message
            **kwargs: Additional fields to update
            
        Returns:
            True if successful, False otherwise
        """
        try:
            async with self.db_manager.get_connection() as conn:
                # Get current timestamp
                now = datetime.now().isoformat()
                
                # Build the update query dynamically
                fields = {
                    "processing_state": processing_state,
                    "updated_at": now,
                }
                
                # Add conditional timestamp fields based on state
                if processing_state == ProcessingState.STATUS_QUEUED.value:
                    fields["status_queued_at"] = now
                elif processing_state == ProcessingState.STATUS_FETCHED.value:
                    fields["status_fetched_at"] = now
                elif processing_state == ProcessingState.OFFICERS_QUEUED.value:
                    fields["officers_queued_at"] = now
                elif processing_state == ProcessingState.OFFICERS_FETCHED.value:
                    fields["officers_fetched_at"] = now
                elif processing_state == ProcessingState.COMPLETED.value:
                    fields["completed_at"] = now
                
                # Add error information if provided
                if last_error:
                    fields["last_error"] = last_error
                    fields["last_error_at"] = now
                
                # Add any additional kwargs
                fields.update(kwargs)
                
                # Use INSERT OR REPLACE to handle both insert and update
                field_names = ", ".join(fields.keys())
                placeholders = ", ".join("?" * len(fields))
                
                # Add company_number to the fields for the query
                all_fields = {"company_number": company_number, **fields}
                field_names = "company_number, " + field_names
                placeholders = "?, " + placeholders
                
                await conn.execute(
                    f"""
                    INSERT OR REPLACE INTO company_processing_state 
                    ({field_names}) VALUES ({placeholders})
                    """,
                    tuple(all_fields.values())
                )
                await conn.commit()
                
                logger.debug(f"Updated state for company {company_number} to {processing_state}")
                return True
                
        except sqlite3.Error as e:
            logger.error(f"Error updating state for company {company_number}: {e}")
            return False

    async def handle_429_response(self, company_number: str, request_id: str) -> bool:
        """Handle a 429 rate limit response.
        
        Args:
            company_number: Company number that triggered the rate limit
            request_id: Request ID that triggered the rate limit
            
        Returns:
            True if successful, False otherwise
        """
        try:
            async with self.db_manager.get_connection() as conn:
                now = datetime.now().isoformat()
                
                await conn.execute(
                    """
                    UPDATE company_processing_state
                    SET last_429_response_at = ?,
                        rate_limit_violations = rate_limit_violations + 1,
                        updated_at = ?
                    WHERE company_number = ?
                    """,
                    (now, now, company_number)
                )
                await conn.commit()
                
                logger.warning(f"Recorded 429 response for company {company_number}, request {request_id}")
                return True
                
        except sqlite3.Error as e:
            logger.error(f"Error handling 429 response for company {company_number}: {e}")
            return False

    async def get_companies_by_state(self, state: ProcessingState, limit: Optional[int] = None) -> list[Dict[str, Any]]:
        """Get companies in a specific processing state.
        
        Args:
            state: Processing state to filter by
            limit: Optional limit on number of results
            
        Returns:
            List of company state dictionaries
        """
        try:
            async with self.db_manager.get_connection() as conn:
                query = """
                    SELECT * FROM company_processing_state 
                    WHERE processing_state = ?
                    ORDER BY updated_at ASC
                """
                params = (state.value,)
                
                if limit:
                    query += " LIMIT ?"
                    params += (limit,)
                
                cursor = await conn.execute(query, params)
                rows = await cursor.fetchall()
                
                # Convert rows to dictionaries
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in rows]
                
        except sqlite3.Error as e:
            logger.error(f"Error getting companies by state {state.value}: {e}")
            return []

    async def cleanup_completed_states(self, days_old: int = 7) -> int:
        """Clean up old completed processing states.
        
        Args:
            days_old: Remove completed states older than this many days
            
        Returns:
            Number of records cleaned up
        """
        try:
            async with self.db_manager.get_connection() as conn:
                cutoff_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                cutoff_date = cutoff_date.replace(day=cutoff_date.day - days_old)
                cutoff_iso = cutoff_date.isoformat()
                
                cursor = await conn.execute(
                    """
                    DELETE FROM company_processing_state
                    WHERE processing_state = ? AND completed_at < ?
                    """,
                    (ProcessingState.COMPLETED.value, cutoff_iso)
                )
                
                deleted_count = cursor.rowcount or 0
                await conn.commit()
                
                logger.info(f"Cleaned up {deleted_count} completed processing states older than {days_old} days")
                return deleted_count
                
        except sqlite3.Error as e:
            logger.error(f"Error cleaning up completed states: {e}")
            return 0

    async def get_processing_statistics(self) -> Dict[str, Any]:
        """Get processing statistics across all states.
        
        Returns:
            Dictionary with processing statistics
        """
        try:
            async with self.db_manager.get_connection() as conn:
                # Get counts by processing state
                cursor = await conn.execute(
                    """
                    SELECT processing_state, COUNT(*) as count
                    FROM company_processing_state
                    GROUP BY processing_state
                    """
                )
                state_counts = dict(await cursor.fetchall())
                
                # Get total rate limit violations
                cursor = await conn.execute(
                    "SELECT SUM(rate_limit_violations) FROM company_processing_state"
                )
                total_violations = (await cursor.fetchone())[0] or 0
                
                # Get companies with recent errors (last 24 hours)
                cursor = await conn.execute(
                    """
                    SELECT COUNT(*) FROM company_processing_state
                    WHERE last_error_at > datetime('now', '-1 day')
                    """
                )
                recent_errors = (await cursor.fetchone())[0] or 0
                
                return {
                    "state_counts": state_counts,
                    "total_rate_limit_violations": total_violations,
                    "recent_errors": recent_errors,
                }
                
        except sqlite3.Error as e:
            logger.error(f"Error getting processing statistics: {e}")
            return {}