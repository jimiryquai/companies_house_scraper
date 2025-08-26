"""Priority-based request queue manager for Companies House API rate limiting resilience.

This module implements an intelligent queuing system to handle Companies House REST API
requests with different priority levels, ensuring real-time strike-off detections get
processed first while preventing system overload and memory exhaustion during rate limiting.
"""

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import IntEnum
from pathlib import Path
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class RequestPriority(IntEnum):
    """Priority levels for Companies House API requests."""

    HIGH = 1  # Real-time company status checks (strike-off detection)
    MEDIUM = 2  # Officer fetching for newly detected strike-offs
    LOW = 3  # Bulk operations and backfills
    BACKGROUND = 4  # Non-urgent maintenance tasks and cleanup


@dataclass
class QueuedRequest:
    """Represents a queued API request with priority and metadata."""

    request_id: str
    priority: RequestPriority
    endpoint: str
    params: dict[str, Any]
    callback: Optional[Callable[..., Any]] = None
    created_at: float = field(default_factory=time.time)
    retry_count: int = 0
    max_retries: int = 3
    timeout: float = 30.0

    def __lt__(self, other: "QueuedRequest") -> bool:
        """Enable priority queue sorting."""
        if self.priority != other.priority:
            return self.priority < other.priority
        return self.created_at < other.created_at

    def age_seconds(self) -> float:
        """Get request age in seconds."""
        return time.time() - self.created_at

    def is_expired(self, max_age: float = 300.0) -> bool:
        """Check if request has exceeded maximum age."""
        return self.age_seconds() > max_age


@dataclass
class QueueMetrics:
    """Metrics for queue monitoring and performance tracking."""

    total_enqueued: int = 0
    total_processed: int = 0
    total_failed: int = 0
    total_expired: int = 0
    total_dropped: int = 0

    queue_depths: dict[str, int] = field(default_factory=dict)
    processing_times: deque[float] = field(default_factory=lambda: deque(maxlen=1000))
    wait_times: deque[float] = field(default_factory=lambda: deque(maxlen=1000))

    last_reset: float = field(default_factory=time.time)

    def get_average_processing_time(self) -> float:
        """Calculate average processing time in seconds."""
        if not self.processing_times:
            return 0.0
        return sum(self.processing_times) / len(self.processing_times)

    def get_average_wait_time(self) -> float:
        """Calculate average wait time in seconds."""
        if not self.wait_times:
            return 0.0
        return sum(self.wait_times) / len(self.wait_times)

    def get_success_rate(self) -> float:
        """Calculate request success rate."""
        total = self.total_processed + self.total_failed
        if total == 0:
            return 1.0
        return self.total_processed / total

    def to_dict(self) -> dict[str, Any]:
        """Convert metrics to dictionary for reporting."""
        return {
            "total_enqueued": self.total_enqueued,
            "total_processed": self.total_processed,
            "total_failed": self.total_failed,
            "total_expired": self.total_expired,
            "total_dropped": self.total_dropped,
            "success_rate": self.get_success_rate(),
            "avg_processing_time": self.get_average_processing_time(),
            "avg_wait_time": self.get_average_wait_time(),
            "queue_depths": dict(self.queue_depths),
            "uptime_seconds": time.time() - self.last_reset,
        }


class PriorityQueueManager:
    """Manages priority-based request queues with configurable limits and persistence."""

    def __init__(
        self,
        max_queue_size: int = 10000,
        max_memory_mb: int = 100,
        persistence_path: Optional[Path] = None,
        enable_monitoring: bool = True,
    ):
        """Initialize the queue manager.

        Args:
            max_queue_size: Maximum total number of requests across all queues
            max_memory_mb: Maximum memory usage in megabytes
            persistence_path: Path for queue persistence (optional)
            enable_monitoring: Enable metrics collection
        """
        self.max_queue_size = max_queue_size
        self.max_memory_mb = max_memory_mb
        self.persistence_path = persistence_path
        self.enable_monitoring = enable_monitoring

        # Priority queues
        self.queues: dict[RequestPriority, deque[QueuedRequest]] = {
            priority: deque() for priority in RequestPriority
        }

        # Active request tracking
        self.active_requests: set[str] = set()

        # Metrics
        self.metrics = QueueMetrics()

        # Queue locks for thread safety
        self.queue_lock = asyncio.Lock()

        # Load persisted queues if available
        if self.persistence_path and self.persistence_path.exists():
            self._load_persisted_queues()

    async def enqueue(self, request: QueuedRequest, force: bool = False) -> bool:
        """Add a request to the appropriate priority queue.

        Args:
            request: The request to enqueue
            force: Force enqueue even if at capacity

        Returns:
            True if enqueued successfully, False otherwise
        """
        async with self.queue_lock:
            # Check capacity unless forced
            if not force and self._get_total_queue_size() >= self.max_queue_size:
                logger.warning(
                    f"Queue at capacity ({self.max_queue_size}), "
                    f"dropping request {request.request_id}"
                )
                self.metrics.total_dropped += 1
                return False

            # Check for duplicates
            if request.request_id in self.active_requests:
                logger.debug(f"Request {request.request_id} already in queue")
                return False

            # Add to appropriate queue
            self.queues[request.priority].append(request)
            self.active_requests.add(request.request_id)

            # Update metrics
            self.metrics.total_enqueued += 1
            self._update_queue_depths()

            logger.debug(
                f"Enqueued request {request.request_id} with priority {request.priority.name}"
            )

            # Persist if enabled
            if self.persistence_path:
                await self._persist_queues()

            return True

    async def dequeue(self, timeout: float = 1.0) -> Optional[QueuedRequest]:
        """Get the next highest priority request from the queue.

        Args:
            timeout: Maximum time to wait for a request

        Returns:
            The next request or None if queue is empty
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            async with self.queue_lock:
                # Check queues in priority order
                for priority in sorted(RequestPriority):
                    queue = self.queues[priority]

                    # Skip empty queues
                    if not queue:
                        continue

                    # Check for expired requests
                    while queue and queue[0].is_expired():
                        expired = queue.popleft()
                        self.active_requests.discard(expired.request_id)
                        self.metrics.total_expired += 1
                        logger.debug(f"Expired request {expired.request_id}")

                    # Get next valid request
                    if queue:
                        request = queue.popleft()
                        self.active_requests.discard(request.request_id)

                        # Update metrics
                        wait_time = request.age_seconds()
                        self.metrics.wait_times.append(wait_time)
                        self._update_queue_depths()

                        logger.debug(
                            f"Dequeued request {request.request_id} "
                            f"(priority: {priority.name}, wait: {wait_time:.1f}s)"
                        )

                        # Persist if enabled
                        if self.persistence_path:
                            await self._persist_queues()

                        return request

            # No requests available, wait briefly
            await asyncio.sleep(0.1)

        return None

    async def requeue(self, request: QueuedRequest, increment_retry: bool = True) -> bool:
        """Requeue a failed request with updated retry count.

        Args:
            request: The request to requeue
            increment_retry: Whether to increment the retry count

        Returns:
            True if requeued successfully, False if max retries exceeded
        """
        if increment_retry:
            request.retry_count += 1

        if request.retry_count > request.max_retries:
            logger.warning(
                f"Request {request.request_id} exceeded max retries ({request.max_retries})"
            )
            self.metrics.total_failed += 1
            return False

        # Lower priority for retried requests
        if request.priority < RequestPriority.BACKGROUND:
            request.priority = RequestPriority(request.priority + 1)

        return await self.enqueue(request, force=True)

    def mark_processed(self, request: QueuedRequest, processing_time: float) -> None:
        """Mark a request as successfully processed.

        Args:
            request: The processed request
            processing_time: Time taken to process in seconds
        """
        self.metrics.total_processed += 1
        self.metrics.processing_times.append(processing_time)

        logger.debug(f"Processed request {request.request_id} in {processing_time:.2f}s")

    def mark_failed(self, request: QueuedRequest) -> None:
        """Mark a request as failed.

        Args:
            request: The failed request
        """
        self.metrics.total_failed += 1
        logger.warning(f"Request {request.request_id} failed")

    def get_queue_status(self) -> dict[str, Any]:
        """Get current queue status and metrics.

        Returns:
            Dictionary containing queue status information
        """
        total_queued = self._get_total_queue_size()
        status: dict[str, Any] = {
            "total_queued": total_queued,
            "max_capacity": self.max_queue_size,
            "utilization": total_queued / self.max_queue_size,
            "queues": {},
            "metrics": self.metrics.to_dict() if self.enable_monitoring else {},
        }

        for priority in RequestPriority:
            queue = self.queues[priority]
            status["queues"][priority.name] = {
                "depth": len(queue),
                "oldest_age": queue[0].age_seconds() if queue else 0,
            }

        return status

    async def clear_queue(self, priority: Optional[RequestPriority] = None) -> int:
        """Clear requests from queue(s).

        Args:
            priority: Specific priority queue to clear, or None for all

        Returns:
            Number of requests cleared
        """
        async with self.queue_lock:
            cleared = 0

            if priority is not None:
                # Clear specific priority queue
                queue = self.queues[priority]
                cleared = len(queue)

                for request in queue:
                    self.active_requests.discard(request.request_id)

                queue.clear()
            else:
                # Clear all queues
                for queue in self.queues.values():
                    cleared += len(queue)
                    queue.clear()

                self.active_requests.clear()

            self._update_queue_depths()

            # Persist if enabled
            if self.persistence_path:
                await self._persist_queues()

            logger.info(f"Cleared {cleared} requests from queue(s)")
            return cleared

    def _get_total_queue_size(self) -> int:
        """Get total number of queued requests across all priorities."""
        return sum(len(queue) for queue in self.queues.values())

    def _update_queue_depths(self) -> None:
        """Update queue depth metrics."""
        if not self.enable_monitoring:
            return

        for priority in RequestPriority:
            self.metrics.queue_depths[priority.name] = len(self.queues[priority])

    async def _persist_queues(self) -> None:
        """Persist current queue state to disk."""
        if not self.persistence_path:
            return

        try:
            # Prepare queue data for serialization
            queue_data: dict[str, Any] = {"timestamp": datetime.now().isoformat(), "queues": {}}

            for priority in RequestPriority:
                queue = self.queues[priority]
                queue_data["queues"][priority.name] = [
                    {
                        "request_id": req.request_id,
                        "priority": req.priority,
                        "endpoint": req.endpoint,
                        "params": req.params,
                        "created_at": req.created_at,
                        "retry_count": req.retry_count,
                        "max_retries": req.max_retries,
                        "timeout": req.timeout,
                    }
                    for req in queue
                ]

            # Write to temporary file then rename (atomic operation)
            temp_path = self.persistence_path.with_suffix(".tmp")
            temp_path.write_text(json.dumps(queue_data, indent=2))
            temp_path.replace(self.persistence_path)

            logger.debug(
                f"Persisted {self._get_total_queue_size()} requests to {self.persistence_path}"
            )

        except Exception as e:
            logger.error(f"Failed to persist queues: {e}")

    def _load_persisted_queues(self) -> None:
        """Load persisted queue state from disk."""
        if not self.persistence_path or not self.persistence_path.exists():
            return

        try:
            queue_data = json.loads(self.persistence_path.read_text())
            loaded_count = 0

            for priority_name, requests in queue_data.get("queues", {}).items():
                try:
                    priority = RequestPriority[priority_name]
                except KeyError:
                    logger.warning(f"Unknown priority: {priority_name}")
                    continue

                for req_data in requests:
                    request = QueuedRequest(
                        request_id=req_data["request_id"],
                        priority=priority,
                        endpoint=req_data["endpoint"],
                        params=req_data["params"],
                        created_at=req_data["created_at"],
                        retry_count=req_data.get("retry_count", 0),
                        max_retries=req_data.get("max_retries", 3),
                        timeout=req_data.get("timeout", 30.0),
                    )

                    # Skip expired requests
                    if not request.is_expired():
                        self.queues[priority].append(request)
                        self.active_requests.add(request.request_id)
                        loaded_count += 1

            self._update_queue_depths()
            logger.info(f"Loaded {loaded_count} requests from {self.persistence_path}")

        except Exception as e:
            logger.error(f"Failed to load persisted queues: {e}")
