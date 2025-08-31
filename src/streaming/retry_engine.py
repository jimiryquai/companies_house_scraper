"""Advanced retry engine with exponential backoff and priority degradation.

This module provides bulletproof retry mechanisms for Companies House API requests
with exponential backoff, priority degradation, and queue-based scheduling to ensure
100% rate limit compliance in cloud deployment environments.
"""

import asyncio
import logging
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, Optional

from .queue_manager import PriorityQueueManager, QueuedRequest, RequestPriority

logger = logging.getLogger(__name__)


class RetryReason(Enum):
    """Reasons for retry attempts."""

    RATE_LIMIT = "rate_limit"  # 429 response
    TIMEOUT = "timeout"  # Request timeout
    SERVER_ERROR = "server_error"  # 5xx responses
    NETWORK_ERROR = "network_error"  # Connection issues
    UNKNOWN_ERROR = "unknown_error"  # Other failures


@dataclass
class RetryPolicy:
    """Configuration for retry behavior."""

    max_retries: int = 5
    base_delay: float = 1.0  # Base delay in seconds
    max_delay: float = 300.0  # Maximum delay (5 minutes)
    exponential_base: float = 2.0  # Exponential backoff multiplier
    jitter_range: float = 0.25  # Jitter as fraction of delay (±25%)
    rate_limit_multiplier: float = 2.0  # Extra delay for rate limits
    priority_degradation_threshold: int = 2  # Retries before degrading priority

    # Emergency shutdown thresholds
    emergency_429_threshold: int = 10  # Consecutive 429s before circuit breaker
    emergency_failure_threshold: int = 50  # Total failures in period before emergency
    emergency_shutdown_duration: int = 30  # Emergency shutdown duration (minutes)


@dataclass
class RetryAttempt:
    """Record of a retry attempt."""

    attempt_number: int
    reason: RetryReason
    delay_seconds: float
    scheduled_at: datetime
    priority_before: RequestPriority
    priority_after: RequestPriority


class RetryEngine:
    """Advanced retry engine with exponential backoff and priority management.

    This retry engine provides bulletproof retry mechanisms for cloud deployment:
    - Exponential backoff with jitter to prevent thundering herd
    - Automatic priority degradation for persistent failures
    - Rate limit aware scheduling with extended delays
    - Circuit breaker pattern for emergency protection
    - Comprehensive retry metrics and monitoring
    """

    def __init__(
        self, queue_manager: PriorityQueueManager, policy: Optional[RetryPolicy] = None
    ) -> None:
        """Initialize the retry engine.

        Args:
            queue_manager: Queue manager for scheduling retries
            policy: Retry policy configuration
        """
        self.queue_manager = queue_manager
        self.policy = policy or RetryPolicy()

        # Retry tracking
        self.retry_history: Dict[str, list[RetryAttempt]] = {}
        self.active_delays: Dict[str, asyncio.Task] = {}

        # Circuit breaker state
        self.circuit_breaker_active = False
        self.last_circuit_breaker_reset = datetime.now()

        # Queue throttling for 429 responses
        self.throttling_active = False
        self.throttling_end_time: Optional[datetime] = None
        self.consecutive_429s = 0
        self.throttling_multiplier = 1.0

        # Metrics
        self.total_retries = 0
        self.retries_by_reason: Dict[RetryReason, int] = dict.fromkeys(RetryReason, 0)
        self.successful_recoveries = 0
        self.permanent_failures = 0

        logger.info(
            "RetryEngine initialized with policy: max_retries=%d, base_delay=%.1fs",
            self.policy.max_retries,
            self.policy.base_delay,
        )

    def calculate_backoff_delay(
        self, attempt: int, reason: RetryReason, base_delay: Optional[float] = None
    ) -> float:
        """Calculate exponential backoff delay with jitter.

        Args:
            attempt: Retry attempt number (1-based)
            reason: Reason for the retry
            base_delay: Override base delay

        Returns:
            Delay in seconds before next retry
        """
        if base_delay is None:
            base_delay = self.policy.base_delay

        # Apply reason-specific multipliers
        if reason == RetryReason.RATE_LIMIT:
            base_delay *= self.policy.rate_limit_multiplier

        # Calculate exponential delay
        exponential_delay = base_delay * (self.policy.exponential_base ** (attempt - 1))

        # Cap at maximum delay
        capped_delay = min(exponential_delay, self.policy.max_delay)

        # Add jitter to prevent thundering herd
        jitter_amount = capped_delay * self.policy.jitter_range
        jitter = (2 * random.random() - 1) * jitter_amount  # ±jitter_amount

        final_delay = max(0.1, capped_delay + jitter)  # Minimum 0.1s

        logger.debug(
            "Calculated backoff delay: attempt=%d, reason=%s, base=%.1f, "
            "exponential=%.1f, capped=%.1f, jitter=%.3f, final=%.1f",
            attempt,
            reason.value,
            base_delay,
            exponential_delay,
            capped_delay,
            jitter,
            final_delay,
        )

        return final_delay

    def should_degrade_priority(self, request: QueuedRequest, attempt: int) -> bool:
        """Check if request priority should be degraded.

        Args:
            request: Request being retried
            attempt: Current retry attempt number

        Returns:
            True if priority should be degraded
        """
        return (
            attempt >= self.policy.priority_degradation_threshold
            and request.priority < RequestPriority.BACKGROUND
        )

    def degrade_priority(self, request: QueuedRequest) -> RequestPriority:
        """Degrade request priority.

        Args:
            request: Request to degrade

        Returns:
            New priority level
        """
        old_priority = request.priority

        if request.priority < RequestPriority.BACKGROUND:
            request.priority = RequestPriority(request.priority + 1)

        logger.debug(
            "Degraded request priority: %s -> %s (request_id=%s)",
            old_priority.name,
            request.priority.name,
            request.request_id,
        )

        return request.priority

    def handle_429_response(self, request: QueuedRequest) -> None:
        """Handle a 429 rate limit response with automatic throttling.

        Args:
            request: Request that received 429 response
        """
        self.consecutive_429s += 1

        # Calculate throttling duration based on consecutive 429s
        base_throttle_seconds = 60  # 1 minute base
        throttle_duration = base_throttle_seconds * (
            2 ** min(self.consecutive_429s - 1, 6)
        )  # Cap at 64x

        # Activate or extend throttling
        new_end_time = datetime.now() + timedelta(seconds=throttle_duration)
        if not self.throttling_active or (
            self.throttling_end_time and new_end_time > self.throttling_end_time
        ):
            self.throttling_active = True
            self.throttling_end_time = new_end_time
            self.throttling_multiplier = min(self.consecutive_429s * 2.0, 10.0)  # Cap at 10x

            logger.warning(
                "Rate limit throttling activated: consecutive_429s=%d, duration=%ds, "
                "multiplier=%.1fx, end_time=%s",
                self.consecutive_429s,
                throttle_duration,
                self.throttling_multiplier,
                self.throttling_end_time.strftime("%H:%M:%S"),
            )

            # Schedule throttling reset
            asyncio.create_task(self._reset_throttling())

    async def _reset_throttling(self) -> None:
        """Reset throttling after duration expires."""
        if self.throttling_end_time:
            wait_time = (self.throttling_end_time - datetime.now()).total_seconds()
            if wait_time > 0:
                await asyncio.sleep(wait_time)

        self.throttling_active = False
        self.throttling_end_time = None
        self.consecutive_429s = max(0, self.consecutive_429s - 1)  # Gradual recovery
        self.throttling_multiplier = 1.0

        logger.info("Rate limit throttling reset - normal queue processing resumed")

    def is_throttled(self) -> bool:
        """Check if queue is currently throttled due to rate limits.

        Returns:
            True if throttling is active
        """
        if self.throttling_active and self.throttling_end_time:
            if datetime.now() >= self.throttling_end_time:
                # Throttling should have expired - reset it
                self.throttling_active = False
                self.throttling_end_time = None
                return False
            return True
        return False

    async def schedule_retry(
        self, request: QueuedRequest, reason: RetryReason, delay_override: Optional[float] = None
    ) -> bool:
        """Schedule a retry attempt for a failed request.

        Args:
            request: Failed request to retry
            reason: Reason for the retry
            delay_override: Override calculated delay

        Returns:
            True if retry was scheduled, False if max retries exceeded
        """
        # Check circuit breaker
        if self.circuit_breaker_active:
            logger.warning("Circuit breaker active - rejecting retry for %s", request.request_id)
            return False

        # Handle 429 responses with automatic throttling
        if reason == RetryReason.RATE_LIMIT:
            self.handle_429_response(request)

            # Emergency shutdown for excessive violations
            if self.consecutive_429s >= self.policy.emergency_429_threshold:
                logger.critical(
                    "EMERGENCY: %d consecutive 429 responses detected (threshold: %d) - activating circuit breaker",
                    self.consecutive_429s,
                    self.policy.emergency_429_threshold,
                )
                self.activate_circuit_breaker(
                    duration_minutes=self.policy.emergency_shutdown_duration
                )

        # Increment retry count
        request.retry_count += 1

        # Check retry limits
        if request.retry_count > self.policy.max_retries:
            logger.warning(
                "Request %s exceeded max retries (%d) - marking as permanent failure",
                request.request_id,
                self.policy.max_retries,
            )
            self.permanent_failures += 1
            return False

        # Calculate delay
        if delay_override is not None:
            delay = delay_override
        else:
            delay = self.calculate_backoff_delay(request.retry_count, reason)

        # Apply throttling multiplier for rate limits
        if reason == RetryReason.RATE_LIMIT and self.throttling_active:
            delay *= self.throttling_multiplier
            logger.debug(
                "Applied throttling multiplier %.1fx to delay: %.1fs -> %.1fs",
                self.throttling_multiplier,
                delay / self.throttling_multiplier,
                delay,
            )

        # Check for priority degradation
        old_priority = request.priority
        if self.should_degrade_priority(request, request.retry_count):
            self.degrade_priority(request)

        # Record retry attempt
        scheduled_at = datetime.now() + timedelta(seconds=delay)
        retry_attempt = RetryAttempt(
            attempt_number=request.retry_count,
            reason=reason,
            delay_seconds=delay,
            scheduled_at=scheduled_at,
            priority_before=old_priority,
            priority_after=request.priority,
        )

        if request.request_id not in self.retry_history:
            self.retry_history[request.request_id] = []
        self.retry_history[request.request_id].append(retry_attempt)

        # Schedule the retry
        retry_task = asyncio.create_task(self._execute_delayed_retry(request, delay))
        self.active_delays[request.request_id] = retry_task

        # Update metrics
        self.total_retries += 1
        self.retries_by_reason[reason] += 1

        # Check for emergency shutdown based on total failure rate
        if self.permanent_failures >= self.policy.emergency_failure_threshold:
            logger.critical(
                "EMERGENCY: %d permanent failures detected (threshold: %d) - activating circuit breaker",
                self.permanent_failures,
                self.policy.emergency_failure_threshold,
            )
            self.activate_circuit_breaker(duration_minutes=self.policy.emergency_shutdown_duration)

        logger.info(
            "Scheduled retry %d/%d for request %s: reason=%s, delay=%.1fs, "
            "priority=%s->%s, scheduled_at=%s",
            request.retry_count,
            self.policy.max_retries,
            request.request_id,
            reason.value,
            delay,
            old_priority.name,
            request.priority.name,
            scheduled_at.strftime("%H:%M:%S"),
        )

        return True

    async def _execute_delayed_retry(self, request: QueuedRequest, delay: float) -> None:
        """Execute a delayed retry after backoff period.

        Args:
            request: Request to retry
            delay: Delay in seconds
        """
        try:
            # Wait for backoff period
            await asyncio.sleep(delay)

            # Re-enqueue the request
            success = await self.queue_manager.enqueue(request, force=True)

            if success:
                logger.debug(
                    "Successfully re-enqueued request %s after %.1fs delay",
                    request.request_id,
                    delay,
                )
            else:
                logger.error("Failed to re-enqueue request %s after delay", request.request_id)

        except asyncio.CancelledError:
            logger.debug("Retry cancelled for request %s", request.request_id)
        except Exception as e:
            logger.error("Error executing delayed retry for %s: %s", request.request_id, e)
        finally:
            # Clean up tracking
            self.active_delays.pop(request.request_id, None)

    def cancel_retry(self, request_id: str) -> bool:
        """Cancel a scheduled retry.

        Args:
            request_id: Request ID to cancel

        Returns:
            True if retry was cancelled, False if not found
        """
        if request_id in self.active_delays:
            task = self.active_delays.pop(request_id)
            task.cancel()
            logger.debug("Cancelled scheduled retry for request %s", request_id)
            return True
        return False

    def get_retry_status(self, request_id: str) -> Optional[Dict[str, Any]]:
        """Get retry status for a request.

        Args:
            request_id: Request ID to check

        Returns:
            Retry status information or None if not found
        """
        if request_id not in self.retry_history:
            return None

        attempts = self.retry_history[request_id]
        last_attempt = attempts[-1] if attempts else None

        return {
            "request_id": request_id,
            "total_attempts": len(attempts),
            "last_attempt": {
                "attempt_number": last_attempt.attempt_number,
                "reason": last_attempt.reason.value,
                "delay_seconds": last_attempt.delay_seconds,
                "scheduled_at": last_attempt.scheduled_at.isoformat(),
                "priority_degraded": last_attempt.priority_before != last_attempt.priority_after,
            }
            if last_attempt
            else None,
            "is_active": request_id in self.active_delays,
            "next_attempt_eta": (
                self.active_delays[request_id].get_name()
                if request_id in self.active_delays
                else None
            ),
        }

    def activate_circuit_breaker(self, duration_minutes: int = 15) -> None:
        """Activate circuit breaker to prevent further retries.

        Args:
            duration_minutes: How long to keep circuit breaker active
        """
        self.circuit_breaker_active = True

        async def reset_circuit_breaker() -> None:
            await asyncio.sleep(duration_minutes * 60)
            self.circuit_breaker_active = False
            self.last_circuit_breaker_reset = datetime.now()
            logger.info("Circuit breaker reset after %d minutes", duration_minutes)

        asyncio.create_task(reset_circuit_breaker())

        logger.warning(
            "Circuit breaker activated for %d minutes - all retries will be rejected",
            duration_minutes,
        )

    def get_metrics(self) -> Dict[str, Any]:
        """Get comprehensive retry engine metrics.

        Returns:
            Dictionary with retry metrics and statistics
        """
        active_retries = len(self.active_delays)
        total_requests_with_retries = len(self.retry_history)

        # Calculate average attempts per request
        total_attempts = sum(len(attempts) for attempts in self.retry_history.values())
        avg_attempts = (
            total_attempts / total_requests_with_retries if total_requests_with_retries > 0 else 0
        )

        return {
            "retry_engine": {
                "policy": {
                    "max_retries": self.policy.max_retries,
                    "base_delay": self.policy.base_delay,
                    "max_delay": self.policy.max_delay,
                    "exponential_base": self.policy.exponential_base,
                },
                "totals": {
                    "total_retries": self.total_retries,
                    "successful_recoveries": self.successful_recoveries,
                    "permanent_failures": self.permanent_failures,
                    "requests_with_retries": total_requests_with_retries,
                    "average_attempts_per_request": round(avg_attempts, 2),
                },
                "active": {
                    "scheduled_retries": active_retries,
                    "circuit_breaker_active": self.circuit_breaker_active,
                    "last_circuit_breaker_reset": (
                        self.last_circuit_breaker_reset.isoformat()
                        if self.last_circuit_breaker_reset
                        else None
                    ),
                    "throttling_active": self.throttling_active,
                    "throttling_end_time": (
                        self.throttling_end_time.isoformat() if self.throttling_end_time else None
                    ),
                    "consecutive_429s": self.consecutive_429s,
                    "throttling_multiplier": self.throttling_multiplier,
                },
                "retries_by_reason": {
                    reason.value: count for reason, count in self.retries_by_reason.items()
                },
            },
            "timestamp": datetime.now().isoformat(),
        }

    def cleanup_completed_retries(self, max_age_hours: int = 24) -> int:
        """Clean up old retry history to prevent memory leaks.

        Args:
            max_age_hours: Maximum age of retry records to keep

        Returns:
            Number of requests cleaned up
        """
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        cleaned_count = 0

        # Find requests with old retry attempts and no active delays
        to_remove = []
        for request_id, attempts in self.retry_history.items():
            if (
                request_id not in self.active_delays
                and attempts
                and attempts[-1].scheduled_at < cutoff_time
            ):
                to_remove.append(request_id)

        # Remove old entries
        for request_id in to_remove:
            del self.retry_history[request_id]
            cleaned_count += 1

        if cleaned_count > 0:
            logger.info(
                "Cleaned up %d old retry records (older than %d hours)",
                cleaned_count,
                max_age_hours,
            )

        return cleaned_count

    async def shutdown(self) -> None:
        """Gracefully shutdown the retry engine."""
        logger.info("Shutting down retry engine...")

        # Cancel all active retry delays
        cancelled_count = 0
        for request_id in list(self.active_delays.keys()):
            if self.cancel_retry(request_id):
                cancelled_count += 1

        logger.info("Cancelled %d active retry delays during shutdown", cancelled_count)

        # Log final metrics
        metrics = self.get_metrics()
        logger.info("Final retry engine metrics: %s", metrics["retry_engine"]["totals"])

        logger.info("Retry engine shutdown complete")
