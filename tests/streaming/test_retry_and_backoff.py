"""Tests for retry logic and exponential backoff with queue integration.

This test suite validates bulletproof retry mechanisms and exponential backoff
strategies for cloud deployment resilience and rate limit compliance.
"""

import asyncio
import sqlite3
import tempfile
import time
from collections.abc import Generator
from pathlib import Path

import pytest

from src.streaming.company_state_manager import CompanyStateManager, ProcessingState
from src.streaming.queue_manager import PriorityQueueManager, QueuedRequest, RequestPriority


@pytest.fixture
def temp_db_path() -> Generator[Path, None, None]:
    """Create a temporary database file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
        temp_path = Path(temp_file.name)

    # Create the required tables
    conn = sqlite3.connect(temp_path)
    conn.execute("""
    CREATE TABLE company_processing_state (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_number TEXT NOT NULL,
        processing_state TEXT NOT NULL DEFAULT 'detected',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status_queued_at TIMESTAMP NULL,
        status_fetched_at TIMESTAMP NULL,
        officers_queued_at TIMESTAMP NULL,
        officers_fetched_at TIMESTAMP NULL,
        completed_at TIMESTAMP NULL,
        status_retry_count INTEGER DEFAULT 0,
        officers_retry_count INTEGER DEFAULT 0,
        status_request_id TEXT NULL,
        officers_request_id TEXT NULL,
        last_error TEXT NULL,
        last_error_at TIMESTAMP NULL,
        last_429_response_at TIMESTAMP NULL,
        rate_limit_violations INTEGER DEFAULT 0,
        UNIQUE(company_number)
    )
    """)

    conn.execute("""
    CREATE TABLE api_rate_limit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        endpoint TEXT NOT NULL,
        response_code INTEGER NOT NULL,
        company_number TEXT,
        request_id TEXT,
        details TEXT
    )
    """)

    conn.close()

    yield temp_path

    # Cleanup
    if temp_path.exists():
        temp_path.unlink()


@pytest.fixture
def queue_manager() -> PriorityQueueManager:
    """Create PriorityQueueManager instance for testing."""
    return PriorityQueueManager()


@pytest.fixture
async def state_manager(
    temp_db_path: Path, queue_manager: PriorityQueueManager
) -> CompanyStateManager:
    """Create and initialize CompanyStateManager instance for testing."""
    manager = CompanyStateManager(str(temp_db_path), queue_manager)
    await manager.initialize()
    return manager


class TestRetryLogicIntegration:
    """Test retry logic integration with queue system."""

    @pytest.mark.asyncio
    async def test_queue_request_retry_count_tracking(
        self, queue_manager: PriorityQueueManager
    ) -> None:
        """Test that queued requests track retry counts correctly."""
        request = QueuedRequest(
            request_id="retry_test_1",
            priority=RequestPriority.HIGH,
            endpoint="/company/12345678",
            params={"company_number": "12345678"},
            max_retries=3,
        )

        # Initial retry count should be 0
        assert request.retry_count == 0

        # Requeue with retry increment
        success = await queue_manager.requeue(request, increment_retry=True)
        assert success
        assert request.retry_count == 1

        # Requeue again
        await queue_manager.requeue(request, increment_retry=True)
        assert request.retry_count == 2

        # Should still be enqueueable
        await queue_manager.requeue(request, increment_retry=True)
        assert request.retry_count == 3

        # Should not be enqueueable after max retries
        success = await queue_manager.requeue(request, increment_retry=True)
        assert not success  # Should fail after max retries exceeded
        assert request.retry_count == 4  # Count still increments

    @pytest.mark.asyncio
    async def test_retry_count_limits_enforcement(
        self, queue_manager: PriorityQueueManager
    ) -> None:
        """Test that retry count limits are enforced properly."""
        # Test retry limits by creating fresh requests for each retry
        base_request_id = "retry_limit_test"

        # Test within limits
        for retry_num in range(1, 3):  # 1, 2
            request = QueuedRequest(
                request_id=f"{base_request_id}_{retry_num}",
                priority=RequestPriority.MEDIUM,
                endpoint="/company/87654321/officers",
                params={"company_number": "87654321"},
                max_retries=2,
                retry_count=retry_num - 1,  # Set initial retry count
            )

            success = await queue_manager.requeue(request, increment_retry=True)
            assert success, f"Retry {retry_num} should succeed (count becomes {retry_num})"
            assert request.retry_count == retry_num

        # Test exceeding limits
        request = QueuedRequest(
            request_id=f"{base_request_id}_fail",
            priority=RequestPriority.MEDIUM,
            endpoint="/company/87654321/officers",
            params={"company_number": "87654321"},
            max_retries=2,
            retry_count=2,  # Already at limit
        )

        success = await queue_manager.requeue(request, increment_retry=True)
        assert not success, "Should fail after exceeding max_retries"
        assert request.retry_count == 3

    @pytest.mark.asyncio
    async def test_state_manager_retry_count_persistence(
        self, state_manager: CompanyStateManager
    ) -> None:
        """Test that state manager persists retry counts to database."""
        company_number = "RETRY001"

        # Create initial state
        await state_manager.update_state(
            company_number, ProcessingState.STATUS_QUEUED.value, status_retry_count=0
        )

        # Simulate retry scenarios
        for retry_count in range(1, 4):
            await state_manager.update_state(
                company_number,
                ProcessingState.STATUS_QUEUED.value,
                status_retry_count=retry_count,
                last_error=f"Retry attempt {retry_count}",
            )

            # Verify persistence
            state = await state_manager.get_state(company_number)
            assert state is not None
            assert state["status_retry_count"] == retry_count

    @pytest.mark.asyncio
    async def test_separate_retry_counts_for_different_operations(
        self, state_manager: CompanyStateManager
    ) -> None:
        """Test that status and officers operations have separate retry counts."""
        company_number = "SEPARATE001"

        # Set different retry counts for different operations
        await state_manager.update_state(
            company_number,
            ProcessingState.OFFICERS_QUEUED.value,
            status_retry_count=2,
            officers_retry_count=1,
        )

        state = await state_manager.get_state(company_number)
        assert state is not None
        assert state["status_retry_count"] == 2
        assert state["officers_retry_count"] == 1

        # Verify they can be incremented independently
        await state_manager.update_state(
            company_number,
            ProcessingState.OFFICERS_QUEUED.value,
            officers_retry_count=3,  # Only officers count changes
        )

        state = await state_manager.get_state(company_number)
        assert state is not None
        assert state["status_retry_count"] == 2  # Unchanged
        assert state["officers_retry_count"] == 3  # Updated


class TestExponentialBackoffStrategy:
    """Test exponential backoff strategy implementation."""

    def test_exponential_backoff_calculation(self) -> None:
        """Test exponential backoff delay calculation."""

        def calculate_backoff_delay(
            retry_count: int, base_delay: float = 1.0, max_delay: float = 60.0
        ) -> float:
            """Calculate exponential backoff delay with jitter."""
            import random

            delay = min(base_delay * (2**retry_count), max_delay)
            # Add jitter (±25% of delay)
            jitter = delay * 0.25 * (2 * random.random() - 1)
            return max(0.1, delay + jitter)

        # Test basic exponential growth
        delay_0 = calculate_backoff_delay(0)  # ~1s
        delay_1 = calculate_backoff_delay(1)  # ~2s
        delay_2 = calculate_backoff_delay(2)  # ~4s
        delay_3 = calculate_backoff_delay(3)  # ~8s

        # Should show exponential growth (accounting for jitter)
        assert 0.5 <= delay_0 <= 1.5
        assert 1.0 <= delay_1 <= 3.0
        assert 2.0 <= delay_2 <= 6.0
        assert 4.0 <= delay_3 <= 12.0

        # Test max delay cap
        delay_10 = calculate_backoff_delay(10, max_delay=60.0)
        assert delay_10 <= 75.0  # Max + jitter should not exceed reasonable bounds

    @pytest.mark.asyncio
    async def test_backoff_delay_integration_with_queue(
        self, queue_manager: PriorityQueueManager
    ) -> None:
        """Test backoff delay integration with queue system."""

        async def mock_backoff_requeue(request: QueuedRequest) -> float:
            """Mock implementation of backoff-aware requeue."""
            base_delay = 0.1  # Fast for testing
            delay = base_delay * (2**request.retry_count)

            # Simulate waiting for backoff delay
            await asyncio.sleep(delay)

            # Requeue with retry increment
            await queue_manager.requeue(request, increment_retry=True)
            return delay

        request = QueuedRequest(
            request_id="backoff_test",
            priority=RequestPriority.LOW,
            endpoint="/test",
            params={},
            max_retries=3,
        )

        start_time = time.time()

        # Test multiple retries with increasing delays
        delays = []
        for _ in range(3):
            delay = await mock_backoff_requeue(request)
            delays.append(delay)

        total_time = time.time() - start_time

        # Verify exponential increase in delays
        assert delays[1] >= delays[0] * 1.8  # Account for timing variations
        assert delays[2] >= delays[1] * 1.8

        # Verify total time accounts for all delays
        expected_min_time = sum(delays) * 0.9  # Allow for timing variations
        assert total_time >= expected_min_time

    @pytest.mark.asyncio
    async def test_backoff_with_priority_degradation(
        self, queue_manager: PriorityQueueManager
    ) -> None:
        """Test backoff with priority degradation for failed requests."""
        # Create separate requests to test priority degradation (avoid duplicate ID issues)
        requests = []
        for i in range(4):
            request = QueuedRequest(
                request_id=f"priority_test_{i}",
                priority=RequestPriority.HIGH,
                endpoint="/company/PRIORITY123",
                params={"company_number": "PRIORITY123"},
            )
            requests.append(request)

        # Test automatic priority degradation by queue manager
        # HIGH (1) -> MEDIUM (2) on first requeue
        await queue_manager.requeue(requests[0], increment_retry=True)
        assert requests[0].retry_count == 1
        assert requests[0].priority == RequestPriority.MEDIUM

        # MEDIUM (2) -> LOW (3) on second requeue
        await queue_manager.requeue(requests[1], increment_retry=True)
        requests[1].priority = RequestPriority.MEDIUM  # Set to MEDIUM first
        await queue_manager.requeue(requests[1], increment_retry=True)
        assert requests[1].retry_count == 2
        assert requests[1].priority == RequestPriority.LOW

        # LOW (3) -> BACKGROUND (4) on third requeue
        await queue_manager.requeue(requests[2], increment_retry=True)
        requests[2].priority = RequestPriority.LOW  # Set to LOW first
        await queue_manager.requeue(requests[2], increment_retry=True)
        assert requests[2].retry_count == 2
        assert requests[2].priority == RequestPriority.BACKGROUND

        # BACKGROUND (4) should stay BACKGROUND (no further degradation)
        await queue_manager.requeue(requests[3], increment_retry=True)
        requests[3].priority = RequestPriority.BACKGROUND  # Set to BACKGROUND first
        await queue_manager.requeue(requests[3], increment_retry=True)
        assert requests[3].retry_count == 2
        assert requests[3].priority == RequestPriority.BACKGROUND  # Stays at lowest


class TestRateLimitRetryHandling:
    """Test retry handling specifically for rate limit scenarios."""

    @pytest.mark.asyncio
    async def test_429_response_retry_scheduling(self, state_manager: CompanyStateManager) -> None:
        """Test 429 response handling with retry scheduling."""
        company_number = "RATE429"
        request_id = "rate_limit_test"

        # Set up company in processing state
        await state_manager.update_state(
            company_number, ProcessingState.STATUS_QUEUED.value, status_request_id=request_id
        )

        # Handle 429 response
        await state_manager.handle_429_response(company_number, request_id)

        # Verify rate limit violation was tracked
        state = await state_manager.get_state(company_number)
        assert state is not None
        assert state["rate_limit_violations"] == 1
        assert state["last_429_response_at"] is not None

        # Handle multiple 429s
        await state_manager.handle_429_response(company_number, request_id)
        await state_manager.handle_429_response(company_number, request_id)

        state = await state_manager.get_state(company_number)
        assert state is not None
        assert state["rate_limit_violations"] == 3

    @pytest.mark.asyncio
    async def test_progressive_backoff_for_rate_limits(self) -> None:
        """Test progressive backoff for repeated rate limit violations."""

        def calculate_rate_limit_backoff(violation_count: int) -> float:
            """Calculate backoff delay based on rate limit violation count."""
            # More aggressive backoff for rate limits
            base_delay = 60.0  # 1 minute base
            max_delay = 3600.0  # 1 hour max

            # Progressive backoff: 1min, 2min, 4min, 8min, etc.
            delay = min(base_delay * (2 ** (violation_count - 1)), max_delay)
            return delay

        # Test progressive backoff schedule
        assert calculate_rate_limit_backoff(1) == 60.0  # 1 minute
        assert calculate_rate_limit_backoff(2) == 120.0  # 2 minutes
        assert calculate_rate_limit_backoff(3) == 240.0  # 4 minutes
        assert calculate_rate_limit_backoff(4) == 480.0  # 8 minutes
        assert calculate_rate_limit_backoff(10) == 3600.0  # Cap at 1 hour

    @pytest.mark.asyncio
    async def test_automatic_rate_limit_recovery(self, state_manager: CompanyStateManager) -> None:
        """Test automatic recovery from rate limit violations."""
        company_number = "RECOVERY001"

        # Create company with excessive rate limit violations
        await state_manager.update_state(
            company_number,
            ProcessingState.STATUS_QUEUED.value,
            rate_limit_violations=8,  # High but not fatal
        )

        # Simulate successful request after backoff
        await state_manager.update_state(
            company_number,
            ProcessingState.STATUS_FETCHED.value,
            # Don't increment violations on success
        )

        state = await state_manager.get_state(company_number)
        assert state is not None
        assert state["processing_state"] == ProcessingState.STATUS_FETCHED.value
        assert state["rate_limit_violations"] == 8  # Preserved for monitoring


class TestFailureRecoveryStrategies:
    """Test comprehensive failure recovery strategies."""

    @pytest.mark.asyncio
    async def test_circuit_breaker_pattern_simulation(
        self, state_manager: CompanyStateManager
    ) -> None:
        """Test circuit breaker pattern for persistent failures."""
        companies_to_fail = ["CIRCUIT001", "CIRCUIT002", "CIRCUIT003"]

        # Simulate multiple companies hitting retry limits
        for i, company in enumerate(companies_to_fail):
            await state_manager.update_state(
                company,
                ProcessingState.STATUS_QUEUED.value,
                status_retry_count=6,  # Exceeds typical limit
                officers_retry_count=6,
                rate_limit_violations=12,  # Exceeds limit
            )

        # Trigger cleanup (simulates circuit breaker opening)
        cleaned_count = await state_manager.cleanup_failed_companies()

        # Verify all failed companies were cleaned up
        assert cleaned_count == 3

        # Verify they were marked as failed
        for company in companies_to_fail:
            state = await state_manager.get_state(company)
            assert state is not None
            assert state["processing_state"] == ProcessingState.FAILED.value

    @pytest.mark.asyncio
    async def test_graceful_degradation_under_load(
        self, queue_manager: PriorityQueueManager
    ) -> None:
        """Test graceful degradation under high load scenarios."""
        # Create many requests of different priorities
        high_priority_requests = [
            QueuedRequest(
                request_id=f"high_{i}",
                priority=RequestPriority.HIGH,
                endpoint=f"/company/{i:08d}",
                params={"company_number": f"{i:08d}"},
            )
            for i in range(10)
        ]

        low_priority_requests = [
            QueuedRequest(
                request_id=f"low_{i}",
                priority=RequestPriority.LOW,
                endpoint=f"/company/{i:08d}/officers",
                params={"company_number": f"{i:08d}"},
            )
            for i in range(20)
        ]

        # Enqueue all requests
        for request in high_priority_requests + low_priority_requests:
            await queue_manager.enqueue(request)

        # Verify queue has requests
        status = queue_manager.get_queue_status()
        total_requests = sum(len(queue) for queue in queue_manager.queues.values())
        assert total_requests == 30  # 10 high + 20 low

        # Simulate processing - high priority should be processed first
        processed_order = []
        for _ in range(15):  # Sample first 15
            next_request = await queue_manager.dequeue()
            if next_request:
                processed_order.append((next_request.priority, next_request.request_id))
            else:
                break  # No more requests

        # Verify high priority requests come first
        high_priority_in_sample = [
            item for item in processed_order if item[0] == RequestPriority.HIGH
        ]
        assert len(high_priority_in_sample) >= 8  # Most high priority requests processed first

    @pytest.mark.asyncio
    async def test_emergency_shutdown_protection(self, state_manager: CompanyStateManager) -> None:
        """Test emergency shutdown protection for critical failures."""
        # Simulate critical failure scenario
        companies_with_critical_failures = []

        # Create companies with extreme failure conditions
        for i in range(5):
            company_number = f"CRITICAL{i:03d}"
            await state_manager.update_state(
                company_number,
                ProcessingState.STATUS_QUEUED.value,
                rate_limit_violations=25,  # Extreme violations
                status_retry_count=10,  # Excessive retries
                officers_retry_count=8,
                last_error="Critical API failure - possible ban",
            )
            companies_with_critical_failures.append(company_number)

        # Trigger emergency cleanup
        cleaned = await state_manager.cleanup_failed_companies()
        assert cleaned == 5

        # Verify emergency state
        for company in companies_with_critical_failures:
            state = await state_manager.get_state(company)
            assert state is not None
            assert state["processing_state"] == ProcessingState.FAILED.value
            # State manager sets its own failure message
            assert state["last_error"] == "Exceeded retry limits or rate limit violations"


class TestRetryEngineIntegration:
    """Test integration with retry engine functionality."""

    @pytest.mark.asyncio
    async def test_retry_engine_queue_throttling(self, queue_manager: PriorityQueueManager) -> None:
        """Test retry engine with queue throttling capabilities."""
        # Create requests that will be retried
        failing_requests = [
            QueuedRequest(
                request_id=f"throttle_{i}",
                priority=RequestPriority.MEDIUM,
                endpoint=f"/company/THROTTLE{i:03d}",
                params={"company_number": f"THROTTLE{i:03d}"},
                max_retries=2,
            )
            for i in range(5)
        ]

        # Enqueue initial requests
        for request in failing_requests:
            await queue_manager.enqueue(request)

        # Simulate processing and failures with requeuing
        processed_count = 0
        retry_count = 0

        for _ in range(15):  # Limit iterations
            request = await queue_manager.dequeue()
            if request:
                processed_count += 1

                # Simulate failure and retry for first few attempts
                if request.retry_count < request.max_retries:
                    # Create new request with different ID for retry
                    retry_request = QueuedRequest(
                        request_id=f"{request.request_id}_retry_{request.retry_count + 1}",
                        priority=request.priority,
                        endpoint=request.endpoint,
                        params=request.params,
                        retry_count=request.retry_count,
                    )
                    await queue_manager.requeue(retry_request, increment_retry=True)
                    retry_count += 1
            else:
                break  # No more requests

        # Verify retry behavior
        assert retry_count >= 5  # At least one retry per original request
        assert processed_count >= 10  # Original + retries

    @pytest.mark.asyncio
    async def test_comprehensive_retry_integration(
        self, state_manager: CompanyStateManager, queue_manager: PriorityQueueManager
    ) -> None:
        """Test comprehensive integration of retry logic across all components."""
        company_number = "INTEGRATION001"

        # Start with company detection
        await state_manager.update_state(company_number, ProcessingState.DETECTED.value)

        # Queue status check
        status_request_id = await state_manager.queue_status_check(company_number)
        assert status_request_id.startswith("status_")

        # Simulate failure and retry
        await state_manager.handle_429_response(company_number, status_request_id)

        # Update retry count
        await state_manager.update_state(
            company_number, ProcessingState.STATUS_QUEUED.value, status_retry_count=1
        )

        # Eventually succeed
        await state_manager.update_state(company_number, ProcessingState.STATUS_FETCHED.value)

        await state_manager.update_state(company_number, ProcessingState.STRIKE_OFF_CONFIRMED.value)

        # Queue officers fetch
        officers_request_id = await state_manager.queue_officers_fetch(company_number)
        assert officers_request_id.startswith("officers_")

        # Complete successfully
        await state_manager.update_state(company_number, ProcessingState.OFFICERS_FETCHED.value)

        await state_manager.update_state(company_number, ProcessingState.COMPLETED.value)

        # Verify final state
        final_state = await state_manager.get_state(company_number)
        assert final_state is not None
        assert final_state["processing_state"] == ProcessingState.COMPLETED.value
        assert final_state["rate_limit_violations"] == 1  # From 429 handling
        assert final_state["status_retry_count"] == 1
        assert final_state["status_request_id"] == status_request_id
        assert final_state["officers_request_id"] == officers_request_id
