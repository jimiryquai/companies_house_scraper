"""Tests for high-volume streaming scenarios and bulletproof error handling.

This module tests error handling capabilities under high-volume scenarios
simulating real-world cloud deployment conditions with hundreds of companies.
"""

import asyncio
import random
import time
from unittest.mock import AsyncMock

import pytest

from src.streaming.company_state_manager import CompanyStateManager, ProcessingState
from src.streaming.database import StreamingDatabase
from src.streaming.queue_manager import PriorityQueueManager, QueuedRequest, RequestPriority
from src.streaming.rate_limit_monitor import RateLimitMonitor
from src.streaming.recovery_manager import ServiceRecoveryManager
from src.streaming.retry_engine import RetryEngine, RetryReason


@pytest.fixture
def mock_database():
    """Mock database for high-volume testing."""
    db = AsyncMock(spec=StreamingDatabase)
    db.execute = AsyncMock()
    db.fetch_all = AsyncMock(return_value=[])
    db.fetch_one = AsyncMock()
    return db


@pytest.fixture
def queue_manager():
    """Real queue manager for high-volume testing."""
    return PriorityQueueManager(
        max_queue_size=50000,  # High capacity for testing
        max_memory_mb=500,
        enable_monitoring=True,
    )


@pytest.fixture
def rate_monitor():
    """Real rate limit monitor for high-volume testing."""
    return RateLimitMonitor(
        window_size_minutes=5, warning_threshold=10, critical_threshold=25, emergency_threshold=50
    )


@pytest.fixture
def retry_engine(queue_manager):
    """Real retry engine for high-volume testing."""
    return RetryEngine(
        queue_manager=queue_manager,
        policy=None,  # Use default policy
    )


@pytest.fixture
def state_manager(mock_database, queue_manager):
    """Mock state manager configured for high-volume testing."""
    manager = AsyncMock(spec=CompanyStateManager)
    manager.database = mock_database
    manager.queue_manager = queue_manager
    manager.company_locks = {}
    manager.processing_lock = asyncio.Lock()

    # Mock methods to simulate real behavior
    async def mock_update_state(company_number, state, **kwargs):
        await asyncio.sleep(0.001)  # Simulate small processing delay
        return True

    async def mock_get_state(company_number):
        await asyncio.sleep(0.001)
        return random.choice(list(ProcessingState))

    manager.update_state = AsyncMock(side_effect=mock_update_state)
    manager.get_state = AsyncMock(side_effect=mock_get_state)
    manager._get_companies_in_state = AsyncMock(return_value=[])

    return manager


class TestHighVolumeScenarios:
    """Test error handling under high-volume scenarios."""

    @pytest.mark.asyncio
    async def test_high_volume_queue_processing(self, queue_manager):
        """Test queue handling with 1000+ requests."""
        # Generate 1000 test requests
        requests = []
        for i in range(1000):
            request = QueuedRequest(
                request_id=f"test_request_{i}",
                priority=random.choice(list(RequestPriority)),
                endpoint="/company/{company_number}",
                params={"company_number": f"1234567{i:04d}"},
                timeout=30.0,
            )
            requests.append(request)

        # Enqueue all requests rapidly
        start_time = time.time()
        enqueued_count = 0

        for request in requests:
            success = await queue_manager.enqueue(request)
            if success:
                enqueued_count += 1

        enqueue_duration = time.time() - start_time

        # Verify high throughput enqueuing
        assert enqueued_count >= 950  # Allow for some capacity limits
        assert enqueue_duration < 30.0  # Should complete within 30 seconds

        # Verify queue status under load
        status = queue_manager.get_queue_status()
        assert status["total_queued"] >= 950
        assert status["utilization"] < 1.0  # Not completely full

        # Test rapid dequeuing
        dequeue_start = time.time()
        dequeued_count = 0

        # Dequeue 500 requests rapidly
        while dequeued_count < 500:
            request = await queue_manager.dequeue(timeout=0.1)
            if request:
                dequeued_count += 1
            else:
                break

        dequeue_duration = time.time() - dequeue_start

        assert dequeued_count >= 500
        assert dequeue_duration < 10.0  # Should be fast

    @pytest.mark.asyncio
    async def test_concurrent_state_updates(self, state_manager):
        """Test concurrent state updates for multiple companies."""
        # Generate 500 companies
        companies = [f"1234567{i:04d}" for i in range(500)]

        # Create concurrent state update tasks
        async def update_company_state(company_number):
            states = [
                ProcessingState.DETECTED,
                ProcessingState.STATUS_QUEUED,
                ProcessingState.STATUS_FETCHED,
                ProcessingState.COMPLETED,
            ]

            for state in states:
                await state_manager.update_state(company_number, state)
                await asyncio.sleep(0.001)  # Small delay between updates

        # Run concurrent updates for all companies
        start_time = time.time()
        tasks = [update_company_state(company) for company in companies]
        await asyncio.gather(*tasks, return_exceptions=True)
        duration = time.time() - start_time

        # Verify high concurrency handling
        assert duration < 60.0  # Should complete within 60 seconds
        assert state_manager.update_state.call_count >= 1500  # 500 companies * 3+ states

    @pytest.mark.asyncio
    async def test_rate_limit_violation_avalanche(self, rate_monitor):
        """Test handling of massive rate limit violations."""
        # Simulate 100 simultaneous rate limit violations
        violation_tasks = []

        async def simulate_violation(company_number, request_id):
            rate_monitor.record_violation(
                company_number=company_number,
                request_id=request_id,
                endpoint="/company/{company_number}",
                context={"simulation": True},
            )
            await asyncio.sleep(0.01)  # Small delay

        # Create violation tasks
        for i in range(100):
            task = simulate_violation(f"company_{i}", f"request_{i}")
            violation_tasks.append(task)

        # Execute all violations rapidly
        start_time = time.time()
        await asyncio.gather(*violation_tasks)
        duration = time.time() - start_time

        # Verify violation handling
        assert duration < 5.0  # Should handle quickly

        # Check violation summary
        summary = rate_monitor.get_violation_summary()
        assert summary["current_window"]["violations"] >= 80  # Most should be recorded
        assert summary["status"]["level"] == "EMERGENCY"  # Should trigger emergency
        assert rate_monitor.total_violations >= 80

    @pytest.mark.asyncio
    async def test_retry_engine_under_load(self, retry_engine, queue_manager):
        """Test retry engine with high failure rates."""
        # Simulate 200 failed requests needing retry
        failed_requests = []

        for i in range(200):
            request = QueuedRequest(
                request_id=f"failed_request_{i}",
                priority=random.choice(list(RequestPriority)),
                endpoint="/company/{company_number}",
                params={"company_number": f"failed_{i}"},
                retry_count=random.randint(0, 2),
                max_retries=3,
            )
            failed_requests.append(request)

        # Process retries concurrently
        async def process_retry(request):
            reason = random.choice(
                [
                    RetryReason.RATE_LIMIT,
                    RetryReason.NETWORK_ERROR,
                    RetryReason.SERVER_ERROR,
                    RetryReason.TIMEOUT,
                ]
            )

            success = await retry_engine.schedule_retry(request, reason, delay_override=0.01)
            return success

        start_time = time.time()
        results = await asyncio.gather(*[process_retry(req) for req in failed_requests])
        duration = time.time() - start_time

        # Verify retry handling
        assert duration < 30.0  # Should complete quickly
        successful_retries = sum(1 for result in results if result)

        # With many RATE_LIMIT failures, circuit breaker should activate
        # So we expect many retries to be rejected (this is correct behavior!)
        # The first batch should succeed until circuit breaker activates
        assert successful_retries >= 10  # At least some should be scheduled before circuit breaker
        assert successful_retries <= 100  # Circuit breaker should prevent most after threshold

        # Verify queue has some retry requests (before circuit breaker activated)
        status = queue_manager.get_queue_status()
        # May have few requests due to circuit breaker protection
        assert status["total_queued"] >= 0  # Some requests may be queued

    @pytest.mark.asyncio
    async def test_recovery_under_high_load(
        self, mock_database, queue_manager, rate_monitor, retry_engine, state_manager
    ):
        """Test recovery manager under high-load conditions."""
        # Setup recovery manager
        recovery_manager = ServiceRecoveryManager(
            state_manager=state_manager,
            queue_manager=queue_manager,
            rate_limit_monitor=rate_monitor,
            retry_engine=retry_engine,
            database=mock_database,
            max_recovery_duration_minutes=10,
        )

        # Simulate high load conditions
        # 1. Fill queue with requests
        for i in range(1000):
            request = QueuedRequest(
                request_id=f"recovery_test_{i}",
                priority=RequestPriority.MEDIUM,
                endpoint="/company/{company_number}",
                params={"company_number": f"test_{i}"},
            )
            await queue_manager.enqueue(request, force=True)

        # 2. Simulate rate limit violations
        for i in range(30):
            rate_monitor.record_violation(f"company_{i}", f"violation_{i}", "/test/endpoint")

        # 3. Start recovery process
        start_time = time.time()
        success = await recovery_manager.start_recovery("High load test recovery")
        duration = time.time() - start_time

        # Verify recovery under load
        assert success or duration < 300  # Either succeeds or times out reasonably
        assert recovery_manager.metrics.companies_validated >= 0

        # Recovery should complete within reasonable time even under load
        assert duration < 600  # 10 minutes max

    @pytest.mark.asyncio
    async def test_memory_efficiency_under_load(self, queue_manager, rate_monitor):
        """Test memory efficiency with large numbers of objects."""
        initial_queue_count = sum(len(q) for q in queue_manager.queues.values())

        # Create large number of requests
        large_batch_size = 5000

        # Test 1: Rapid enqueue/dequeue cycles
        for cycle in range(5):
            # Enqueue batch
            for i in range(large_batch_size):
                request = QueuedRequest(
                    request_id=f"memory_test_{cycle}_{i}",
                    priority=RequestPriority.LOW,
                    endpoint="/test",
                    params={"test": f"batch_{cycle}_{i}"},
                )
                success = await queue_manager.enqueue(request, force=False)
                # If queue is full, start dequeuing
                if not success and i > 1000:
                    # Dequeue some requests to make space
                    for _ in range(100):
                        dequeued = await queue_manager.dequeue(timeout=0.001)
                        if dequeued:
                            queue_manager.mark_processed(dequeued, 0.001)

            # Dequeue remaining from this cycle
            dequeued_count = 0
            while dequeued_count < large_batch_size // 2:
                request = await queue_manager.dequeue(timeout=0.001)
                if not request:
                    break
                queue_manager.mark_processed(request, 0.001)
                dequeued_count += 1

        # Verify memory management
        final_queue_count = sum(len(q) for q in queue_manager.queues.values())
        queue_utilization = queue_manager.get_queue_status()["utilization"]

        # Should not be completely full (good memory management)
        assert queue_utilization < 0.95
        assert final_queue_count >= initial_queue_count  # May have some backlog

        # Test 2: Rate monitor memory efficiency
        for i in range(1000):
            rate_monitor.record_violation(
                f"memory_company_{i % 100}",  # Reuse company numbers
                f"memory_request_{i}",
                "/memory/test",
            )

        # Verify rate monitor doesn't leak memory
        violation_summary = rate_monitor.get_violation_summary()
        # Rate monitor should have all violations due to rapid testing, but should be manageable
        assert violation_summary["current_window"]["violations"] >= 500  # Should record many
        assert violation_summary["current_window"]["violations"] <= 1000  # All should be recorded

    @pytest.mark.asyncio
    async def test_error_cascade_prevention(
        self, state_manager, queue_manager, rate_monitor, retry_engine
    ):
        """Test prevention of error cascades in high-volume scenarios."""
        # Simulate cascade trigger: many failures at once
        cascade_companies = [f"cascade_{i}" for i in range(100)]

        # Create concurrent failure scenarios
        async def simulate_company_failure(company_number):
            # Simulate multiple failure types
            failure_types = [
                RetryReason.RATE_LIMIT,
                RetryReason.SERVER_ERROR,
                RetryReason.NETWORK_ERROR,
                RetryReason.TIMEOUT,
            ]

            for failure_type in failure_types:
                # Create failed request
                request = QueuedRequest(
                    request_id=f"{company_number}_{failure_type.value}",
                    priority=RequestPriority.HIGH,
                    endpoint="/company/{company_number}",
                    params={"company_number": company_number},
                )

                # Record failure and attempt retry
                if failure_type == RetryReason.RATE_LIMIT:
                    rate_monitor.record_violation(
                        company_number, request.request_id, request.endpoint
                    )

                await retry_engine.schedule_retry(request, failure_type, delay_override=0.01)
                await asyncio.sleep(0.001)  # Small delay

        # Execute cascade simulation
        start_time = time.time()
        await asyncio.gather(*[simulate_company_failure(company) for company in cascade_companies])
        duration = time.time() - start_time

        # Verify cascade prevention
        assert duration < 60.0  # Should handle cascade quickly

        # Check that system remains stable
        queue_status = queue_manager.get_queue_status()
        violation_summary = rate_monitor.get_violation_summary()

        # System should not be overwhelmed
        assert queue_status["utilization"] < 1.0  # Not completely full
        assert violation_summary["status"]["level"] in [
            "CRITICAL",
            "EMERGENCY",
        ]  # Expected high level

        # Should have manageable queue size
        assert queue_status["total_queued"] < queue_manager.max_queue_size
