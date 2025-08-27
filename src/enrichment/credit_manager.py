"""Credit consumption tracking and monitoring for Snov.io API.

This module provides comprehensive credit management for Snov.io API operations,
including consumption tracking, exhaustion handling, and integration with existing
database schema and monitoring systems.
"""

import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Optional

from ..streaming.health_endpoints import HealthStatus

logger = logging.getLogger(__name__)


class CreditExhaustionError(Exception):
    """Raised when API credits are exhausted."""

    pass


class CreditTrackingError(Exception):
    """Raised when credit tracking operations fail."""

    pass


class CreditManager:
    """Manages Snov.io API credit consumption and monitoring.

    This class provides bulletproof credit tracking and exhaustion handling,
    integrating with the existing database schema (snov_credit_usage table)
    and monitoring systems following established patterns.
    """

    def __init__(
        self,
        database_path: str = "companies.db",
        min_credit_threshold: int = 100,
        alert_threshold: int = 500,
        cache_ttl_minutes: int = 5,
    ):
        """Initialize credit manager.

        Args:
            database_path: Path to SQLite database
            min_credit_threshold: Minimum credits before blocking operations
            alert_threshold: Credit level that triggers alerts
            cache_ttl_minutes: Cache TTL for credit balance checks

        Raises:
            ValueError: If thresholds are invalid
        """
        if min_credit_threshold < 0 or alert_threshold < 0:
            raise ValueError("Credit thresholds must be non-negative")
        if min_credit_threshold > alert_threshold:
            raise ValueError("Minimum threshold cannot exceed alert threshold")

        self.database_path = database_path
        self.min_credit_threshold = min_credit_threshold
        self.alert_threshold = alert_threshold
        self.cache_ttl_minutes = cache_ttl_minutes

        # Cache for credit balance to reduce database queries
        self._cached_balance: Optional[int] = None
        self._last_balance_check: Optional[datetime] = None

        # Statistics tracking
        self.total_credits_consumed = 0
        self.operations_tracked = 0
        self.start_time = datetime.now()

        # Database lock for thread safety
        self._db_lock = asyncio.Lock()

    async def check_available_credits(self) -> int:
        """Check current available credit balance.

        Returns:
            Current credit balance

        Raises:
            CreditTrackingError: If balance check fails
        """
        # Check cache first
        if self._is_cache_valid():
            return self._cached_balance or 0

        try:
            async with self._db_lock:
                conn = sqlite3.connect(self.database_path)
                try:
                    cursor = conn.cursor()

                    # Get total credits consumed
                    cursor.execute("""
                        SELECT COALESCE(SUM(credits_consumed), 0)
                        FROM snov_credit_usage
                        WHERE success = 1
                    """)

                    result = cursor.fetchone()
                    total_consumed = int(result[0]) if result else 0

                    # For now, assume starting balance (would be configurable in production)
                    # This should be updated when we receive credit balance from Snov.io API
                    starting_balance = 5000  # Default starting balance
                    current_balance = max(0, starting_balance - total_consumed)

                    # Update cache
                    self._cached_balance = current_balance
                    self._last_balance_check = datetime.now()

                    return current_balance

                finally:
                    conn.close()

        except sqlite3.Error as e:
            raise CreditTrackingError(f"Database error checking credits: {e}") from e

    async def can_consume_credits(self, credits_needed: int) -> bool:
        """Check if sufficient credits are available for operation.

        Args:
            credits_needed: Number of credits required

        Returns:
            True if operation can proceed
        """
        if credits_needed <= 0:
            return True

        try:
            available_credits = await self.check_available_credits()
            return available_credits >= (self.min_credit_threshold + credits_needed)

        except Exception as e:
            logger.error(f"Error checking credit availability: {e}")
            # Conservative approach: block operations if we can't check credits
            return False

    async def record_credit_consumption(
        self,
        operation_type: str,
        credits_consumed: int,
        success: bool,
        request_id: Optional[str] = None,
        company_id: Optional[str] = None,
        officer_id: Optional[str] = None,
        response_data: Optional[dict[str, Any]] = None,
    ) -> None:
        """Record credit consumption in database.

        Args:
            operation_type: Type of operation (domain_search, email_finder, etc.)
            credits_consumed: Number of credits consumed
            success: Whether the operation was successful
            request_id: Associated request ID
            company_id: Associated company number
            officer_id: Associated officer ID
            response_data: Response data for tracking

        Raises:
            CreditTrackingError: If recording fails
        """
        if credits_consumed < 0:
            raise ValueError("Credits consumed cannot be negative")

        try:
            async with self._db_lock:
                conn = sqlite3.connect(self.database_path)
                try:
                    cursor = conn.cursor()

                    cursor.execute("""
                        INSERT INTO snov_credit_usage (
                            operation_type, credits_consumed, success, request_id,
                            company_id, officer_id, response_data, timestamp
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        operation_type,
                        credits_consumed,
                        success,
                        request_id,
                        company_id,
                        officer_id,
                        str(response_data) if response_data else None,
                        datetime.now().isoformat(),
                    ))

                    conn.commit()

                    # Update statistics
                    self.operations_tracked += 1
                    if success:
                        self.total_credits_consumed += credits_consumed

                    # Invalidate cache to force refresh
                    self._cached_balance = None

                    logger.debug(
                        f"Recorded {credits_consumed} credits for {operation_type} "
                        f"(success: {success}, request_id: {request_id})"
                    )

                finally:
                    conn.close()

        except sqlite3.Error as e:
            raise CreditTrackingError(f"Database error recording credits: {e}") from e

    async def update_balance_from_api(self, new_balance: int) -> None:
        """Update credit balance from Snov.io API response.

        Args:
            new_balance: Current balance from API

        Raises:
            ValueError: If balance is negative
        """
        if new_balance < 0:
            raise ValueError("Credit balance cannot be negative")

        self._cached_balance = new_balance
        self._last_balance_check = datetime.now()

        logger.info(f"Updated credit balance from API: {new_balance}")

        # Check thresholds and log warnings
        if new_balance <= self.min_credit_threshold:
            logger.error(f"CRITICAL: Credit balance ({new_balance}) at minimum threshold!")
        elif new_balance <= self.alert_threshold:
            logger.warning(f"Credit balance ({new_balance}) below alert threshold")

    async def get_credit_usage_statistics(
        self,
        hours_back: int = 24,
    ) -> dict[str, Any]:
        """Get credit usage statistics for monitoring.

        Args:
            hours_back: Number of hours to analyze

        Returns:
            Dictionary with usage statistics
        """
        try:
            async with self._db_lock:
                conn = sqlite3.connect(self.database_path)
                try:
                    cursor = conn.cursor()

                    # Calculate time window
                    since_time = datetime.now() - timedelta(hours=hours_back)
                    since_iso = since_time.isoformat()

                    # Get usage by operation type
                    cursor.execute("""
                        SELECT 
                            operation_type,
                            COUNT(*) as operations,
                            SUM(credits_consumed) as credits_used,
                            SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_ops,
                            AVG(credits_consumed) as avg_credits_per_op
                        FROM snov_credit_usage
                        WHERE timestamp >= ?
                        GROUP BY operation_type
                    """, (since_iso,))

                    usage_by_type = {}
                    for row in cursor.fetchall():
                        usage_by_type[row[0]] = {
                            "operations": row[1],
                            "credits_used": row[2],
                            "successful_operations": row[3],
                            "success_rate": (row[3] / row[1] * 100) if row[1] > 0 else 0,
                            "avg_credits_per_operation": round(row[4] or 0, 2),
                        }

                    # Get total statistics
                    cursor.execute("""
                        SELECT 
                            COUNT(*) as total_operations,
                            SUM(credits_consumed) as total_credits,
                            SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_ops
                        FROM snov_credit_usage
                        WHERE timestamp >= ?
                    """, (since_iso,))

                    totals = cursor.fetchone()

                    current_balance = await self.check_available_credits()

                    return {
                        "current_balance": current_balance,
                        "min_threshold": self.min_credit_threshold,
                        "alert_threshold": self.alert_threshold,
                        "balance_status": self._get_balance_status(current_balance),
                        "period_hours": hours_back,
                        "total_operations": totals[0],
                        "total_credits_consumed": totals[1],
                        "successful_operations": totals[2],
                        "overall_success_rate": (totals[2] / totals[0] * 100) if totals[0] > 0 else 0,
                        "usage_by_operation": usage_by_type,
                    }

                finally:
                    conn.close()

        except Exception as e:
            logger.error(f"Error getting credit statistics: {e}")
            return {"error": str(e)}

    def _is_cache_valid(self) -> bool:
        """Check if cached credit balance is still valid.

        Returns:
            True if cache is valid
        """
        if self._cached_balance is None or self._last_balance_check is None:
            return False

        cache_age = datetime.now() - self._last_balance_check
        return cache_age < timedelta(minutes=self.cache_ttl_minutes)

    def _get_balance_status(self, balance: int) -> str:
        """Get balance status description.

        Args:
            balance: Current credit balance

        Returns:
            Status description
        """
        if balance <= self.min_credit_threshold:
            return "critical"
        if balance <= self.alert_threshold:
            return "warning"
        return "healthy"

    def get_health_status(self) -> dict[str, Any]:
        """Get credit manager health status.

        Returns:
            Health status following existing patterns
        """
        try:
            # Use cached balance if available
            balance = self._cached_balance or 0
            balance_status = self._get_balance_status(balance)

            # Determine health status
            if balance_status == "critical":
                status = HealthStatus.DOWN
                message = f"Critical: {balance} credits remaining"
            elif balance_status == "warning":
                status = HealthStatus.DEGRADED
                message = f"Warning: {balance} credits remaining"
            else:
                status = HealthStatus.UP
                message = f"Healthy: {balance} credits available"

            return {
                "component": "credit_manager",
                "status": status.value,
                "message": message,
                "timestamp": datetime.now().isoformat(),
                "details": {
                    "current_balance": balance,
                    "min_threshold": self.min_credit_threshold,
                    "alert_threshold": self.alert_threshold,
                    "balance_status": balance_status,
                    "operations_tracked": self.operations_tracked,
                    "total_credits_consumed": self.total_credits_consumed,
                    "cache_valid": self._is_cache_valid(),
                }
            }

        except Exception as e:
            return {
                "component": "credit_manager",
                "status": HealthStatus.DOWN.value,
                "message": f"Health check failed: {str(e)}",
                "timestamp": datetime.now().isoformat(),
            }

    def get_metrics(self) -> dict[str, Any]:
        """Get credit management metrics.

        Returns:
            Metrics data for monitoring
        """
        return {
            "credit_current_balance": self._cached_balance or 0,
            "credit_min_threshold": self.min_credit_threshold,
            "credit_alert_threshold": self.alert_threshold,
            "credit_total_consumed": self.total_credits_consumed,
            "credit_operations_tracked": self.operations_tracked,
            "credit_cache_valid": self._is_cache_valid(),
        }

