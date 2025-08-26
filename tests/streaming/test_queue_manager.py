"""Comprehensive tests for the priority-based request queue manager.

This test suite validates all aspects of the intelligent queuing system,
including priority handling, memory limits, persistence, and edge cases.
"""
import asyncio
import json
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.streaming.queue_manager import (
    PriorityQueueManager,
    QueuedRequest,
    QueueMetrics,
    RequestPriority,
)


class TestQueuedRequest:
    """Test QueuedRequest data class functionality."""

    def test_request_creation(self) -> None:
        """Test basic request creation with all fields."""
        request = QueuedRequest(
            request_id="test-123",
            priority=RequestPriority.HIGH,
            endpoint="/companies/12345",
            params={"include": "officers"},
            timeout=60.0,
            max_retries=5
        )

        assert request.request_id == "test-123"
        assert request.priority == RequestPriority.HIGH
        assert request.endpoint == "/companies/12345"
        assert request.params == {"include": "officers"}
        assert request.timeout == 60.0
        assert request.max_retries == 5
        assert request.retry_count == 0
        assert isinstance(request.created_at, float)

    def test_request_priority_comparison(self) -> None:
        """Test request priority-based comparison for queue ordering."""
        critical = QueuedRequest("1", RequestPriority.CRITICAL, "/health", {})
        high = QueuedRequest("2", RequestPriority.HIGH, "/status", {})
        medium = QueuedRequest("3", RequestPriority.MEDIUM, "/officers", {})

        # Higher priority (lower enum value) should come first
        assert critical < high
        assert high < medium
        assert not (medium < high)

    def test_request_age_ordering(self) -> None:
        """Test that requests of same priority are ordered by age."""
        # Create two requests with same priority but different timestamps
        older = QueuedRequest("1", RequestPriority.HIGH, "/test", {})
        time.sleep(0.01)  # Ensure different timestamps
        newer = QueuedRequest("2", RequestPriority.HIGH, "/test", {})

        # Older request should come first
        assert older < newer

    def test_request_age_calculation(self) -> None:
        """Test request age calculation."""
        request = QueuedRequest("test", RequestPriority.HIGH, "/test", {})

        # Age should be very small initially
        assert 0 <= request.age_seconds() < 1.0

        # Modify created_at to test age calculation
        request.created_at = time.time() - 30.0
        assert 29.0 < request.age_seconds() < 31.0

    def test_request_expiration(self) -> None:
        """Test request expiration logic."""
        request = QueuedRequest("test", RequestPriority.HIGH, "/test", {})

        # New request should not be expired
        assert not request.is_expired(max_age=300.0)

        # Old request should be expired
        request.created_at = time.time() - 400.0
        assert request.is_expired(max_age=300.0)


class TestQueueMetrics:
    """Test QueueMetrics functionality."""

    def test_metrics_initialization(self) -> None:
        """Test metrics are initialized with correct defaults."""
        metrics = QueueMetrics()

        assert metrics.total_enqueued == 0
        assert metrics.total_processed == 0
        assert metrics.total_failed == 0
        assert metrics.total_expired == 0
        assert metrics.total_dropped == 0
        assert len(metrics.queue_depths) == 0
        assert len(metrics.processing_times) == 0
        assert len(metrics.wait_times) == 0
        assert isinstance(metrics.last_reset, float)

    def test_average_processing_time(self) -> None:
        """Test average processing time calculation."""
        metrics = QueueMetrics()

        # No data should return 0
        assert metrics.get_average_processing_time() == 0.0

        # Add some processing times
        metrics.processing_times.extend([1.0, 2.0, 3.0])
        assert metrics.get_average_processing_time() == 2.0

    def test_average_wait_time(self) -> None:
        """Test average wait time calculation."""
        metrics = QueueMetrics()

        # No data should return 0
        assert metrics.get_average_wait_time() == 0.0

        # Add some wait times
        metrics.wait_times.extend([0.5, 1.5, 2.5])
        assert metrics.get_average_wait_time() == 1.5

    def test_success_rate_calculation(self) -> None:
        """Test success rate calculation."""
        metrics = QueueMetrics()

        # No requests should return 100%
        assert metrics.get_success_rate() == 1.0

        # Mixed success/failure
        metrics.total_processed = 8
        metrics.total_failed = 2
        assert metrics.get_success_rate() == 0.8

    def test_metrics_to_dict(self) -> None:
        """Test metrics serialization to dictionary."""
        metrics = QueueMetrics()
        metrics.total_enqueued = 10
        metrics.total_processed = 8
        metrics.total_failed = 1
        metrics.queue_depths = {"HIGH": 5, "MEDIUM": 3}

        result = metrics.to_dict()

        assert result["total_enqueued"] == 10
        assert result["total_processed"] == 8
        assert result["total_failed"] == 1
        assert result["queue_depths"] == {"HIGH": 5, "MEDIUM": 3}
        assert "success_rate" in result
        assert "avg_processing_time" in result
        assert "uptime_seconds" in result


class TestPriorityQueueManager:
    """Test PriorityQueueManager core functionality."""

    @pytest.fixture
    def queue_manager(self) -> PriorityQueueManager:
        """Create a queue manager for testing."""
        return PriorityQueueManager(
            max_queue_size=100,
            max_memory_mb=10,
            enable_monitoring=True
        )

    @pytest.fixture
    def sample_request(self) -> QueuedRequest:
        """Create a sample request for testing."""
        return QueuedRequest(
            request_id="test-request",
            priority=RequestPriority.HIGH,
            endpoint="/companies/test",
            params={"key": "value"}
        )

    @pytest.mark.asyncio
    async def test_manager_initialization(
        self,
        queue_manager: PriorityQueueManager
    ) -> None:
        """Test queue manager initialization."""
        assert queue_manager.max_queue_size == 100
        assert queue_manager.max_memory_mb == 10
        assert queue_manager.enable_monitoring is True

        # Should have queues for all priorities
        assert len(queue_manager.queues) == len(RequestPriority)
        for priority in RequestPriority:
            assert priority in queue_manager.queues
            assert len(queue_manager.queues[priority]) == 0

        assert len(queue_manager.active_requests) == 0
        assert isinstance(queue_manager.metrics, QueueMetrics)

    @pytest.mark.asyncio
    async def test_enqueue_basic(
        self,
        queue_manager: PriorityQueueManager,
        sample_request: QueuedRequest
    ) -> None:
        """Test basic request enqueuing."""
        result = await queue_manager.enqueue(sample_request)

        assert result is True
        assert sample_request.request_id in queue_manager.active_requests
        assert len(queue_manager.queues[RequestPriority.HIGH]) == 1
        assert queue_manager.metrics.total_enqueued == 1

    @pytest.mark.asyncio
    async def test_enqueue_duplicate_rejection(
        self,
        queue_manager: PriorityQueueManager,
        sample_request: QueuedRequest
    ) -> None:
        """Test that duplicate requests are rejected."""
        # Enqueue first time
        result1 = await queue_manager.enqueue(sample_request)
        assert result1 is True

        # Try to enqueue same request again
        duplicate = QueuedRequest(
            request_id=sample_request.request_id,  # Same ID
            priority=RequestPriority.CRITICAL,
            endpoint="/different",
            params={}
        )
        result2 = await queue_manager.enqueue(duplicate)

        assert result2 is False
        assert len(queue_manager.queues[RequestPriority.HIGH]) == 1
        assert len(queue_manager.queues[RequestPriority.CRITICAL]) == 0
        assert queue_manager.metrics.total_enqueued == 1

    @pytest.mark.asyncio
    async def test_enqueue_capacity_limit(
        self,
        queue_manager: PriorityQueueManager
    ) -> None:
        """Test queue capacity limits are enforced."""
        # Fill queue to capacity
        for i in range(queue_manager.max_queue_size):
            request = QueuedRequest(
                f"req-{i}",
                RequestPriority.LOW,
                f"/test{i}",
                {}
            )
            result = await queue_manager.enqueue(request)
            assert result is True

        # Next request should be dropped
        overflow_request = QueuedRequest(
            "overflow",
            RequestPriority.CRITICAL,
            "/overflow",
            {}
        )
        result = await queue_manager.enqueue(overflow_request)

        assert result is False
        assert queue_manager.metrics.total_dropped == 1
        assert "overflow" not in queue_manager.active_requests

    @pytest.mark.asyncio
    async def test_enqueue_force_override(
        self,
        queue_manager: PriorityQueueManager
    ) -> None:
        """Test force enqueue bypasses capacity limits."""
        # Fill queue to capacity
        for i in range(queue_manager.max_queue_size):
            request = QueuedRequest(
                f"req-{i}",
                RequestPriority.LOW,
                f"/test{i}",
                {}
            )
            await queue_manager.enqueue(request)

        # Force enqueue should succeed even at capacity
        overflow_request = QueuedRequest(
            "overflow",
            RequestPriority.CRITICAL,
            "/overflow",
            {}
        )
        result = await queue_manager.enqueue(overflow_request, force=True)

        assert result is True
        assert "overflow" in queue_manager.active_requests
        assert queue_manager.metrics.total_dropped == 0

    @pytest.mark.asyncio
    async def test_dequeue_priority_order(
        self,
        queue_manager: PriorityQueueManager
    ) -> None:
        """Test that requests are dequeued in priority order."""
        # Add requests in reverse priority order
        requests = [
            QueuedRequest("low", RequestPriority.LOW, "/low", {}),
            QueuedRequest("medium", RequestPriority.MEDIUM, "/medium", {}),
            QueuedRequest("high", RequestPriority.HIGH, "/high", {}),
            QueuedRequest("critical", RequestPriority.CRITICAL, "/critical", {})
        ]

        for request in requests:
            await queue_manager.enqueue(request)

        # Should dequeue in priority order (CRITICAL first)
        dequeued_order = []
        for _ in range(4):
            request = await queue_manager.dequeue(timeout=0.1)
            assert request is not None
            dequeued_order.append(request.request_id)

        assert dequeued_order == ["critical", "high", "medium", "low"]

    @pytest.mark.asyncio
    async def test_dequeue_age_order_same_priority(
        self,
        queue_manager: PriorityQueueManager
    ) -> None:
        """Test that requests of same priority are dequeued in age order."""
        # Add multiple requests with same priority
        request1 = QueuedRequest("first", RequestPriority.HIGH, "/test1", {})
        await queue_manager.enqueue(request1)

        time.sleep(0.01)  # Ensure different timestamps

        request2 = QueuedRequest("second", RequestPriority.HIGH, "/test2", {})
        await queue_manager.enqueue(request2)

        # Should dequeue older request first
        first_dequeued = await queue_manager.dequeue(timeout=0.1)
        second_dequeued = await queue_manager.dequeue(timeout=0.1)

        assert first_dequeued is not None
        assert second_dequeued is not None
        assert first_dequeued.request_id == "first"
        assert second_dequeued.request_id == "second"

    @pytest.mark.asyncio
    async def test_dequeue_expired_cleanup(
        self,
        queue_manager: PriorityQueueManager
    ) -> None:
        """Test that expired requests are automatically cleaned up."""
        # Create an expired request
        expired_request = QueuedRequest(
            "expired",
            RequestPriority.HIGH,
            "/test",
            {}
        )
        expired_request.created_at = time.time() - 400.0  # Very old

        # Create a valid request
        valid_request = QueuedRequest("valid", RequestPriority.HIGH, "/test", {})

        await queue_manager.enqueue(expired_request)
        await queue_manager.enqueue(valid_request)

        # Dequeue should skip expired and return valid
        result = await queue_manager.dequeue(timeout=0.1)

        assert result is not None
        assert result.request_id == "valid"
        assert queue_manager.metrics.total_expired == 1
        assert "expired" not in queue_manager.active_requests

    @pytest.mark.asyncio
    async def test_dequeue_empty_queue(
        self,
        queue_manager: PriorityQueueManager
    ) -> None:
        """Test dequeue behavior with empty queue."""
        result = await queue_manager.dequeue(timeout=0.1)
        assert result is None

    @pytest.mark.asyncio
    async def test_requeue_with_retry_increment(
        self,
        queue_manager: PriorityQueueManager,
        sample_request: QueuedRequest
    ) -> None:
        """Test requeuing a failed request with retry count increment."""
        # Set initial retry count
        sample_request.retry_count = 1
        sample_request.max_retries = 3

        result = await queue_manager.requeue(sample_request, increment_retry=True)

        assert result is True
        assert sample_request.retry_count == 2
        assert sample_request.request_id in queue_manager.active_requests

    @pytest.mark.asyncio
    async def test_requeue_max_retries_exceeded(
        self,
        queue_manager: PriorityQueueManager,
        sample_request: QueuedRequest
    ) -> None:
        """Test requeue rejection when max retries exceeded."""
        sample_request.retry_count = 3
        sample_request.max_retries = 3

        result = await queue_manager.requeue(sample_request, increment_retry=True)

        assert result is False
        assert queue_manager.metrics.total_failed == 1
        assert sample_request.request_id not in queue_manager.active_requests

    @pytest.mark.asyncio
    async def test_requeue_priority_degradation(
        self,
        queue_manager: PriorityQueueManager
    ) -> None:
        """Test that requeued requests get lower priority."""
        request = QueuedRequest("test", RequestPriority.HIGH, "/test", {})
        original_priority = request.priority

        await queue_manager.requeue(request, increment_retry=True)

        # Priority should be degraded
        assert request.priority > original_priority
        assert request.request_id in queue_manager.active_requests

    def test_mark_processed(
        self,
        queue_manager: PriorityQueueManager,
        sample_request: QueuedRequest
    ) -> None:
        """Test marking request as processed."""
        processing_time = 1.5

        queue_manager.mark_processed(sample_request, processing_time)

        assert queue_manager.metrics.total_processed == 1
        assert len(queue_manager.metrics.processing_times) == 1
        assert queue_manager.metrics.processing_times[0] == processing_time

    def test_mark_failed(
        self,
        queue_manager: PriorityQueueManager,
        sample_request: QueuedRequest
    ) -> None:
        """Test marking request as failed."""
        queue_manager.mark_failed(sample_request)

        assert queue_manager.metrics.total_failed == 1

    def test_get_queue_status(
        self,
        queue_manager: PriorityQueueManager
    ) -> None:
        """Test queue status reporting."""
        status = queue_manager.get_queue_status()

        assert "total_queued" in status
        assert "max_capacity" in status
        assert "utilization" in status
        assert "queues" in status
        assert "metrics" in status

        # Check queue details for each priority
        for priority in RequestPriority:
            assert priority.name in status["queues"]
            assert "depth" in status["queues"][priority.name]
            assert "oldest_age" in status["queues"][priority.name]

    @pytest.mark.asyncio
    async def test_clear_queue_specific_priority(
        self,
        queue_manager: PriorityQueueManager
    ) -> None:
        """Test clearing requests from specific priority queue."""
        # Add requests to different priorities
        high_req = QueuedRequest("high", RequestPriority.HIGH, "/test", {})
        medium_req = QueuedRequest("medium", RequestPriority.MEDIUM, "/test", {})

        await queue_manager.enqueue(high_req)
        await queue_manager.enqueue(medium_req)

        # Clear only HIGH priority
        cleared = await queue_manager.clear_queue(RequestPriority.HIGH)

        assert cleared == 1
        assert len(queue_manager.queues[RequestPriority.HIGH]) == 0
        assert len(queue_manager.queues[RequestPriority.MEDIUM]) == 1
        assert "high" not in queue_manager.active_requests
        assert "medium" in queue_manager.active_requests

    @pytest.mark.asyncio
    async def test_clear_queue_all_priorities(
        self,
        queue_manager: PriorityQueueManager
    ) -> None:
        """Test clearing requests from all queues."""
        # Add requests to different priorities
        requests = [
            QueuedRequest("high", RequestPriority.HIGH, "/test", {}),
            QueuedRequest("medium", RequestPriority.MEDIUM, "/test", {}),
            QueuedRequest("low", RequestPriority.LOW, "/test", {})
        ]

        for request in requests:
            await queue_manager.enqueue(request)

        # Clear all queues
        cleared = await queue_manager.clear_queue()

        assert cleared == 3
        assert len(queue_manager.active_requests) == 0
        for queue in queue_manager.queues.values():
            assert len(queue) == 0


class TestPersistence:
    """Test queue persistence functionality."""

    @pytest.mark.asyncio
    async def test_persistence_save_and_load(self) -> None:
        """Test saving and loading queue state to/from disk."""
        with tempfile.TemporaryDirectory() as temp_dir:
            persistence_path = Path(temp_dir) / "queue_state.json"

            # Create manager with persistence
            manager = PriorityQueueManager(
                max_queue_size=100,
                persistence_path=persistence_path,
                enable_monitoring=True
            )

            # Add some requests
            requests = [
                QueuedRequest(
                    "req1",
                    RequestPriority.CRITICAL,
                    "/critical",
                    {"key": "value"}
                ),
                QueuedRequest(
                    "req2",
                    RequestPriority.HIGH,
                    "/high",
                    {"param": "test"}
                ),
                QueuedRequest("req3", RequestPriority.MEDIUM, "/medium", {})
            ]

            for request in requests:
                await manager.enqueue(request)

            # Verify persistence file was created
            assert persistence_path.exists()

            # Create new manager that should load persisted state
            new_manager = PriorityQueueManager(
                max_queue_size=100,
                persistence_path=persistence_path,
                enable_monitoring=True
            )

            # Should have loaded all non-expired requests
            total_loaded = sum(len(queue) for queue in new_manager.queues.values())
            assert total_loaded == 3
            assert "req1" in new_manager.active_requests
            assert "req2" in new_manager.active_requests
            assert "req3" in new_manager.active_requests

    @pytest.mark.asyncio
    async def test_persistence_expired_requests_skipped(self) -> None:
        """Test that expired requests are not loaded from persistence."""
        with tempfile.TemporaryDirectory() as temp_dir:
            persistence_path = Path(temp_dir) / "queue_state.json"

            # Create expired request data manually
            expired_data = {
                "timestamp": "2025-08-26T12:00:00",
                "queues": {
                    "HIGH": [{
                        "request_id": "expired",
                        "priority": 2,  # HIGH
                        "endpoint": "/test",
                        "params": {},
                        "created_at": time.time() - 400.0,  # Very old
                        "retry_count": 0,
                        "max_retries": 3,
                        "timeout": 30.0
                    }]
                }
            }

            persistence_path.write_text(json.dumps(expired_data))

            # Create manager that should load but skip expired requests
            manager = PriorityQueueManager(
                max_queue_size=100,
                persistence_path=persistence_path,
                enable_monitoring=True
            )

            # Should not have loaded expired request
            total_loaded = sum(len(queue) for queue in manager.queues.values())
            assert total_loaded == 0
            assert "expired" not in manager.active_requests

    @pytest.mark.asyncio
    async def test_persistence_corruption_handling(self) -> None:
        """Test graceful handling of corrupted persistence file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            persistence_path = Path(temp_dir) / "queue_state.json"

            # Write corrupted JSON
            persistence_path.write_text("invalid json content")

            # Should not crash, just log error and continue
            manager = PriorityQueueManager(
                max_queue_size=100,
                persistence_path=persistence_path,
                enable_monitoring=True
            )

            # Should have empty queues
            total_loaded = sum(len(queue) for queue in manager.queues.values())
            assert total_loaded == 0

    @pytest.mark.asyncio
    async def test_persistence_atomic_write(self) -> None:
        """Test that persistence writes are atomic."""
        with tempfile.TemporaryDirectory() as temp_dir:
            persistence_path = Path(temp_dir) / "queue_state.json"

            manager = PriorityQueueManager(
                max_queue_size=100,
                persistence_path=persistence_path,
                enable_monitoring=True
            )

            # Add request to trigger persistence
            request = QueuedRequest("test", RequestPriority.HIGH, "/test", {})
            await manager.enqueue(request)

            # Verify no temporary files left behind
            temp_files = list(Path(temp_dir).glob("*.tmp"))
            assert len(temp_files) == 0

            # Verify main file exists and is valid JSON
            assert persistence_path.exists()
            data = json.loads(persistence_path.read_text())
            assert "queues" in data
            assert "timestamp" in data


class TestEdgeCases:
    """Test edge cases and error scenarios."""

    @pytest.mark.asyncio
    async def test_concurrent_enqueue_dequeue(self) -> None:
        """Test thread safety of concurrent operations."""
        manager = PriorityQueueManager(max_queue_size=1000)

        async def enqueue_worker(worker_id: int) -> None:
            for i in range(50):
                request = QueuedRequest(
                    f"w{worker_id}-r{i}",
                    RequestPriority.MEDIUM,
                    f"/test{i}",
                    {}
                )
                await manager.enqueue(request)
                await asyncio.sleep(0.001)  # Small delay for concurrency

        async def dequeue_worker() -> int:
            count = 0
            for _ in range(100):  # Try to dequeue 100 times
                request = await manager.dequeue(timeout=0.01)
                if request:
                    manager.mark_processed(request, 0.1)
                    count += 1
                await asyncio.sleep(0.001)
            return count

        # Run concurrent workers
        enqueue_tasks = [enqueue_worker(i) for i in range(2)]  # 100 total
        dequeue_task = dequeue_worker()

        results = await asyncio.gather(*enqueue_tasks, dequeue_task)
        dequeued_count = results[-1]  # Last result is from dequeue worker

        # Should have processed most or all requests
        assert dequeued_count > 0
        assert manager.metrics.total_enqueued >= dequeued_count

    @pytest.mark.asyncio
    async def test_memory_pressure_simulation(self) -> None:
        """Test behavior under simulated memory pressure."""
        manager = PriorityQueueManager(
            max_queue_size=1000,
            max_memory_mb=1
        )  # Very low limit

        # Fill with requests
        for i in range(500):
            request = QueuedRequest(
                f"request-{i}",
                RequestPriority.LOW,
                f"/large-endpoint-with-long-name-{i}",
                {"large_param": "x" * 100}  # Large parameter
            )
            await manager.enqueue(request)

        # Manager should still be functional
        status = manager.get_queue_status()
        assert status["total_queued"] > 0

        # Should be able to dequeue
        request = await manager.dequeue(timeout=0.1)
        assert request is not None

    @pytest.mark.asyncio
    async def test_extreme_age_handling(self) -> None:
        """Test handling of extremely old requests."""
        manager = PriorityQueueManager()

        # Create request with extreme age
        old_request = QueuedRequest(
            "ancient",
            RequestPriority.HIGH,
            "/test",
            {}
        )
        old_request.created_at = 0.0  # Unix epoch

        # Should detect as expired
        assert old_request.is_expired(max_age=300.0)

        # Enqueue and try to dequeue - should be cleaned up
        await manager.enqueue(old_request)
        result = await manager.dequeue(timeout=0.1)

        assert result is None  # Should have been cleaned up
        assert manager.metrics.total_expired == 1

    def test_priority_enum_completeness(self) -> None:
        """Test that all priority levels are properly defined."""
        # Ensure all expected priority levels exist
        expected_priorities = ["CRITICAL", "HIGH", "MEDIUM", "LOW", "BACKGROUND"]
        actual_priorities = [p.name for p in RequestPriority]

        for expected in expected_priorities:
            assert expected in actual_priorities

        # Ensure proper ordering (lower values = higher priority)
        assert RequestPriority.CRITICAL < RequestPriority.HIGH
        assert RequestPriority.HIGH < RequestPriority.MEDIUM
        assert RequestPriority.MEDIUM < RequestPriority.LOW
        assert RequestPriority.LOW < RequestPriority.BACKGROUND

    @pytest.mark.asyncio
    async def test_callback_handling(self) -> None:
        """Test that callback functions are preserved in requests."""
        callback_mock = MagicMock()

        request = QueuedRequest(
            "test",
            RequestPriority.HIGH,
            "/test",
            {},
            callback=callback_mock
        )

        manager = PriorityQueueManager()
        await manager.enqueue(request)

        dequeued = await manager.dequeue(timeout=0.1)
        assert dequeued is not None
        assert dequeued.callback == callback_mock

    @pytest.mark.asyncio
    async def test_metrics_disabled(self) -> None:
        """Test queue manager with metrics disabled."""
        manager = PriorityQueueManager(enable_monitoring=False)

        request = QueuedRequest("test", RequestPriority.HIGH, "/test", {})
        await manager.enqueue(request)

        # Metrics should still exist but not be populated
        status = manager.get_queue_status()
        assert status["metrics"] == {}


# Integration test for complete workflow
class TestQueueWorkflow:
    """Integration tests for complete queue workflows."""

    @pytest.mark.asyncio
    async def test_complete_request_lifecycle(self) -> None:
        """Test complete request lifecycle from enqueue to completion."""
        with tempfile.TemporaryDirectory() as temp_dir:
            persistence_path = Path(temp_dir) / "queue.json"

            manager = PriorityQueueManager(
                max_queue_size=100,
                persistence_path=persistence_path,
                enable_monitoring=True
            )

            # 1. Enqueue requests of different priorities
            requests = [
                QueuedRequest(
                    "critical-1",
                    RequestPriority.CRITICAL,
                    "/health",
                    {}
                ),
                QueuedRequest("high-1", RequestPriority.HIGH, "/status", {}),
                QueuedRequest(
                    "medium-1",
                    RequestPriority.MEDIUM,
                    "/officers",
                    {}
                ),
                QueuedRequest("low-1", RequestPriority.LOW, "/bulk", {})
            ]

            for request in requests:
                result = await manager.enqueue(request)
                assert result is True

            # 2. Process requests in priority order
            processed_order = []
            while True:
                request = await manager.dequeue(timeout=0.1)
                if not request:
                    break

                processed_order.append(request.request_id)

                # Simulate processing time
                processing_time = 0.1
                await asyncio.sleep(processing_time)
                manager.mark_processed(request, processing_time)

            # 3. Verify correct processing order
            expected_order = ["critical-1", "high-1", "medium-1", "low-1"]
            assert processed_order == expected_order

            # 4. Verify metrics
            assert manager.metrics.total_enqueued == 4
            assert manager.metrics.total_processed == 4
            assert manager.metrics.total_failed == 0
            assert manager.metrics.get_success_rate() == 1.0
            assert manager.metrics.get_average_processing_time() > 0

    @pytest.mark.asyncio
    async def test_failure_recovery_workflow(self) -> None:
        """Test request failure and retry workflow."""
        manager = PriorityQueueManager()

        # Create request that will be retried
        request = QueuedRequest(
            "retry-test",
            RequestPriority.HIGH,
            "/flaky-endpoint",
            {},
            max_retries=2
        )

        await manager.enqueue(request)

        # First attempt - simulate failure
        first_attempt = await manager.dequeue(timeout=0.1)
        assert first_attempt is not None
        assert first_attempt.retry_count == 0

        # Requeue after failure
        result = await manager.requeue(first_attempt)
        assert result is True

        # Second attempt
        second_attempt = await manager.dequeue(timeout=0.1)
        assert second_attempt is not None
        assert second_attempt.retry_count == 1
        assert second_attempt.priority > RequestPriority.HIGH  # Degraded

        # Final failure - should not requeue
        result = await manager.requeue(second_attempt)
        assert result is True  # Still within limit

        # Third attempt
        third_attempt = await manager.dequeue(timeout=0.1)
        assert third_attempt is not None
        assert third_attempt.retry_count == 2

        # Exceed max retries
        result = await manager.requeue(third_attempt)
        assert result is False  # Should reject

        # Verify failure metrics
        assert manager.metrics.total_failed == 1
