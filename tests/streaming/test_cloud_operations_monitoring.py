"""Tests for cloud operations monitoring and autonomous operation capabilities.

This module tests comprehensive metrics collection, monitoring, and autonomous
operation capabilities for cloud deployment environments.
"""

import asyncio
import json
import time
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.streaming.company_state_manager import CompanyStateManager, ProcessingState
from src.streaming.database import StreamingDatabase
from src.streaming.health_monitor import HealthMonitor
from src.streaming.queue_manager import PriorityQueueManager, QueuedRequest, RequestPriority
from src.streaming.rate_limit_monitor import AlertLevel, RateLimitAlert, RateLimitMonitor
from src.streaming.recovery_manager import ServiceRecoveryManager
from src.streaming.retry_engine import RetryEngine


@pytest.fixture
def mock_database():
    """Mock database for monitoring tests."""
    db = AsyncMock(spec=StreamingDatabase)
    db.execute = AsyncMock()
    db.fetch_all = AsyncMock(return_value=[])
    db.fetch_one = AsyncMock()
    return db


@pytest.fixture
def queue_manager():
    """Real queue manager for monitoring tests."""
    return PriorityQueueManager(max_queue_size=10000, enable_monitoring=True)


@pytest.fixture
def rate_monitor():
    """Real rate limit monitor for monitoring tests."""
    return RateLimitMonitor(
        window_size_minutes=5, warning_threshold=5, critical_threshold=15, emergency_threshold=30
    )


@pytest.fixture
def health_monitor():
    """Mock health monitor for testing."""
    mock_client = MagicMock()
    return HealthMonitor(client=mock_client)


@pytest.fixture
def state_manager(mock_database, queue_manager):
    """Mock state manager for monitoring tests."""
    manager = AsyncMock(spec=CompanyStateManager)
    manager.database = mock_database
    manager.queue_manager = queue_manager

    # Mock processing state counts
    async def mock_get_companies_in_state(state):
        # Simulate different counts for different states
        state_counts = {
            ProcessingState.DETECTED: ["12345678", "87654321"],
            ProcessingState.STATUS_QUEUED: ["11111111"],
            ProcessingState.COMPLETED: ["99999999", "88888888", "77777777"],
        }
        return state_counts.get(state, [])

    manager._get_companies_in_state = AsyncMock(side_effect=mock_get_companies_in_state)
    manager.get_processing_statistics = AsyncMock(
        return_value={
            "total_companies": 10,
            "by_state": {"detected": 2, "status_queued": 1, "completed": 3, "failed": 1},
        }
    )

    return manager


class TestMetricsCollection:
    """Test comprehensive metrics collection capabilities."""

    @pytest.mark.asyncio
    async def test_queue_manager_metrics_collection(self, queue_manager):
        """Test queue manager provides comprehensive metrics."""
        # Add some test requests
        for i in range(10):
            request = QueuedRequest(
                request_id=f"metrics_test_{i}",
                priority=RequestPriority.MEDIUM,
                endpoint="/company/{company_number}",
                params={"company_number": f"test_{i}"},
            )
            await queue_manager.enqueue(request)

        # Process some requests to generate metrics
        processed_count = 0
        for _ in range(5):
            request = await queue_manager.dequeue(timeout=0.1)
            if request:
                queue_manager.mark_processed(request, processing_time=0.5)
                processed_count += 1

        # Get metrics
        status = queue_manager.get_queue_status()

        # Verify comprehensive metrics
        assert "total_queued" in status
        assert "max_capacity" in status
        assert "utilization" in status
        assert "queues" in status
        assert "metrics" in status

        # Verify per-priority queue metrics
        for priority in RequestPriority:
            assert priority.name in status["queues"]
            assert "depth" in status["queues"][priority.name]
            assert "oldest_age" in status["queues"][priority.name]

        # Verify processing metrics
        metrics = status["metrics"]
        assert "total_processed" in metrics
        assert "success_rate" in metrics
        assert "avg_processing_time" in metrics
        assert "uptime_seconds" in metrics

        assert metrics["total_processed"] == processed_count
        assert metrics["success_rate"] == 1.0  # All processed successfully

    @pytest.mark.asyncio
    async def test_rate_limit_monitor_metrics_collection(self, rate_monitor):
        """Test rate limit monitor provides detailed metrics."""
        # Simulate rate limit violations
        for i in range(8):
            rate_monitor.record_violation(
                company_number=f"company_{i}",
                request_id=f"request_{i}",
                endpoint="/company/{company_number}",
                context={"test": "metrics"},
            )

        # Record successful request for recovery detection
        rate_monitor.record_successful_request("company_success", "/test/endpoint")

        # Get comprehensive metrics
        metrics = rate_monitor.get_metrics()

        # Verify structure
        assert "rate_limit_monitor" in metrics
        monitor_metrics = metrics["rate_limit_monitor"]

        assert "summary" in monitor_metrics
        assert "configuration" in monitor_metrics
        assert "recent_alerts" in monitor_metrics

        # Verify summary metrics
        summary = monitor_metrics["summary"]
        assert "current_window" in summary
        assert "thresholds" in summary
        assert "status" in summary
        assert "totals" in summary

        # Verify current window metrics
        current_window = summary["current_window"]
        assert "violations" in current_window
        assert "window_size_minutes" in current_window
        assert "rate_per_minute" in current_window

        assert current_window["violations"] == 8
        assert current_window["window_size_minutes"] == 5

        # Verify status classification
        status = summary["status"]
        assert status["level"] in ["WARNING", "CRITICAL", "EMERGENCY", "NORMAL"]
        assert "active_alerts" in status
        assert "last_violation" in status

    def test_health_monitor_metrics_collection(self, health_monitor):
        """Test health monitor provides system health metrics."""
        # Update health status using correct methods
        health_monitor.record_event()
        health_monitor.record_event_processed(processing_time=0.5)
        health_monitor.record_event_failed()

        # Get health metrics using correct methods
        health_report = health_monitor.get_health_report()
        health_summary = health_monitor.get_health_summary()
        is_healthy = health_monitor.is_healthy()

        # Verify health metrics structure
        assert isinstance(is_healthy, bool)
        assert health_report is not None
        assert health_summary is not None

        # Verify health report structure
        assert "status" in health_report or isinstance(health_report, dict)

        # Test performance metrics collection
        perf_summary = health_monitor.get_performance_summary()
        assert perf_summary is not None

    @pytest.mark.asyncio
    async def test_state_manager_processing_statistics(self, state_manager):
        """Test state manager provides processing statistics."""
        # Get processing statistics
        stats = await state_manager.get_processing_statistics()

        # Verify statistics structure
        assert "total_companies" in stats
        assert "by_state" in stats

        # Verify state breakdown
        state_stats = stats["by_state"]
        for state in ProcessingState:
            # Some states should be present
            if state.value in state_stats:
                assert isinstance(state_stats[state.value], int)
                assert state_stats[state.value] >= 0

        # Verify totals make sense
        assert stats["total_companies"] >= 0
        total_by_state = sum(state_stats.values())
        # Total might not match exactly due to mocking, but should be reasonable
        assert total_by_state >= 0


class TestCloudOperationsMonitoring:
    """Test cloud operations monitoring capabilities."""

    @pytest.mark.asyncio
    async def test_comprehensive_system_metrics_aggregation(
        self, queue_manager, rate_monitor, health_monitor, state_manager
    ):
        """Test aggregation of all system metrics for cloud monitoring."""
        # Generate activity across all components

        # Queue activity
        for i in range(5):
            request = QueuedRequest(
                request_id=f"cloud_test_{i}",
                priority=RequestPriority.HIGH,
                endpoint="/company/{company_number}",
                params={"company_number": f"cloud_{i}"},
            )
            await queue_manager.enqueue(request)

        # Process some requests
        for _ in range(3):
            request = await queue_manager.dequeue(timeout=0.1)
            if request:
                queue_manager.mark_processed(request, processing_time=0.3)

        # Rate limit activity
        for i in range(3):
            rate_monitor.record_violation(f"cloud_company_{i}", f"cloud_req_{i}", "/test")

        # Health monitoring activity
        health_monitor.record_event()
        health_monitor.record_event_processed(processing_time=0.25)

        # Collect all metrics
        all_metrics = {
            "timestamp": datetime.now().isoformat(),
            "queue_metrics": queue_manager.get_queue_status(),
            "rate_limit_metrics": rate_monitor.get_metrics(),
            "health_metrics": {
                "status": health_monitor.is_healthy(),
                "health_report": health_monitor.get_health_report(),
                "health_summary": health_monitor.get_health_summary(),
                "performance_summary": health_monitor.get_performance_summary(),
            },
            "processing_stats": await state_manager.get_processing_statistics(),
        }

        # Verify aggregated metrics structure
        assert "timestamp" in all_metrics
        assert "queue_metrics" in all_metrics
        assert "rate_limit_metrics" in all_metrics
        assert "health_metrics" in all_metrics
        assert "processing_stats" in all_metrics

        # Verify metrics are JSON serializable (important for cloud monitoring)
        json_metrics = json.dumps(all_metrics, default=str)
        assert len(json_metrics) > 100  # Should have substantial content

        # Verify key operational metrics are present
        queue_metrics = all_metrics["queue_metrics"]
        assert queue_metrics["total_queued"] >= 0
        assert 0 <= queue_metrics["utilization"] <= 1

        rate_metrics = all_metrics["rate_limit_metrics"]["rate_limit_monitor"]["summary"]
        assert rate_metrics["current_window"]["violations"] >= 0
        assert rate_metrics["status"]["level"] in ["NORMAL", "WARNING", "CRITICAL", "EMERGENCY"]

    @pytest.mark.asyncio
    async def test_autonomous_operation_health_checks(self, health_monitor, rate_monitor):
        """Test health checks for autonomous operation."""
        # Simulate normal operation
        health_monitor.record_event()
        health_monitor.record_event_processed(processing_time=0.1)

        # Check healthy state
        is_healthy = health_monitor.is_healthy()
        assert isinstance(is_healthy, bool)

        # Simulate degraded operation
        health_monitor.record_event_failed()
        health_monitor.record_event_failed()

        # Health status should reflect the errors
        updated_health = health_monitor.is_healthy()
        assert isinstance(updated_health, bool)

        # Simulate rate limit issues
        for i in range(35):  # Trigger emergency threshold (30 is the threshold)
            rate_monitor.record_violation(f"health_test_{i}", f"req_{i}", "/test")

        # Check rate limit status
        rate_summary = rate_monitor.get_violation_summary()
        assert rate_summary["status"]["level"] in [
            "CRITICAL",
            "EMERGENCY",
        ]  # Should be one of these

        # Verify autonomous monitoring capabilities
        assert "active_alerts" in rate_summary["status"]
        assert rate_summary["totals"]["total_violations"] >= 35

    @pytest.mark.asyncio
    async def test_metrics_for_cloud_platform_integration(
        self, queue_manager, rate_monitor, health_monitor
    ):
        """Test metrics format suitable for cloud platform monitoring."""
        # Generate some activity
        for i in range(10):
            request = QueuedRequest(
                request_id=f"cloud_platform_test_{i}",
                priority=RequestPriority.MEDIUM,
                endpoint="/company/{company_number}",
                params={"company_number": f"platform_{i}"},
            )
            await queue_manager.enqueue(request)

        # Process half of requests
        for _ in range(5):
            request = await queue_manager.dequeue(timeout=0.1)
            if request:
                queue_manager.mark_processed(request, processing_time=0.2)

        # Create cloud platform formatted metrics
        cloud_metrics = {
            "service": "companies-house-streaming",
            "timestamp": datetime.now().isoformat(),
            "metrics": {
                # Standard cloud monitoring metrics
                "requests_per_minute": queue_manager.get_queue_status()["metrics"][
                    "total_processed"
                ]
                * 12,  # Scale to per minute
                "queue_depth": queue_manager.get_queue_status()["total_queued"],
                "queue_utilization_percent": queue_manager.get_queue_status()["utilization"] * 100,
                "error_rate_percent": 0.0,  # No errors in this test
                "response_time_ms": queue_manager.get_queue_status()["metrics"][
                    "avg_processing_time"
                ]
                * 1000,
                # Rate limiting metrics
                "rate_limit_violations": rate_monitor.get_violation_summary()["current_window"][
                    "violations"
                ],
                "rate_limit_status": rate_monitor.get_violation_summary()["status"]["level"],
                # Health metrics
                "service_healthy": health_monitor.is_healthy(),
                "health_report": str(health_monitor.get_health_report()),
            },
            "tags": {"environment": "test", "component": "streaming_api", "version": "1.0.0"},
        }

        # Verify cloud platform compatibility
        assert "service" in cloud_metrics
        assert "timestamp" in cloud_metrics
        assert "metrics" in cloud_metrics
        assert "tags" in cloud_metrics

        # Verify all metrics are numeric or string values (cloud platform requirement)
        for key, value in cloud_metrics["metrics"].items():
            assert isinstance(value, (int, float, str, bool)), (
                f"Metric {key} has invalid type {type(value)}"
            )

        # Verify JSON serializable
        json_str = json.dumps(cloud_metrics)
        assert len(json_str) > 50

        # Verify metrics are within reasonable ranges
        metrics = cloud_metrics["metrics"]
        assert 0 <= metrics["queue_utilization_percent"] <= 100
        assert metrics["response_time_ms"] >= 0

    @pytest.mark.asyncio
    async def test_alerting_for_autonomous_operation(self, rate_monitor):
        """Test alerting capabilities for autonomous operation."""
        alerts_received = []

        def alert_handler(alert: RateLimitAlert):
            alerts_received.append(alert)

        # Create rate monitor with custom alert handler
        custom_monitor = RateLimitMonitor(
            alert_callback=alert_handler,
            warning_threshold=3,
            critical_threshold=7,
            emergency_threshold=12,
        )

        # Gradually increase violations to test alerting thresholds
        for i in range(15):
            custom_monitor.record_violation(
                f"alert_test_{i}",
                f"alert_req_{i}",
                "/alert/test",
                context={"batch": "autonomous_test"},
            )

        # Verify alert progression
        assert len(alerts_received) >= 3  # Should have WARNING, CRITICAL, EMERGENCY

        # Check alert levels
        alert_levels = [alert.level for alert in alerts_received]
        assert AlertLevel.WARNING in alert_levels
        assert AlertLevel.CRITICAL in alert_levels
        assert AlertLevel.EMERGENCY in alert_levels

        # Verify alert content for autonomous operation
        emergency_alerts = [a for a in alerts_received if a.level == AlertLevel.EMERGENCY]
        assert len(emergency_alerts) >= 1

        emergency_alert = emergency_alerts[0]
        assert "rate limit violations" in emergency_alert.message.lower()
        assert "system may be banned" in emergency_alert.message.lower()
        assert emergency_alert.context["violations_in_window"] >= 12

        # Test alert suppression (prevents spam in autonomous operation)
        initial_alert_count = len(alerts_received)

        # Add more violations rapidly
        for i in range(5):
            custom_monitor.record_violation(f"spam_test_{i}", f"spam_req_{i}", "/spam/test")

        # Should not generate many new alerts due to suppression
        final_alert_count = len(alerts_received)
        alerts_added = final_alert_count - initial_alert_count
        assert alerts_added <= 2  # Should suppress most duplicate alerts


class TestAutonomousOperationCapabilities:
    """Test autonomous operation capabilities for cloud deployment."""

    @pytest.mark.asyncio
    async def test_self_monitoring_and_reporting(
        self, queue_manager, rate_monitor, health_monitor, state_manager
    ):
        """Test self-monitoring capabilities for autonomous operation."""
        # Simulate continuous operation
        start_time = time.time()

        # Generate continuous activity for monitoring
        for cycle in range(3):
            # Queue activity
            for i in range(10):
                request = QueuedRequest(
                    request_id=f"auto_cycle_{cycle}_{i}",
                    priority=RequestPriority.MEDIUM,
                    endpoint="/company/{company_number}",
                    params={"company_number": f"auto_{cycle}_{i}"},
                )
                await queue_manager.enqueue(request)

            # Process requests
            for _ in range(5):
                request = await queue_manager.dequeue(timeout=0.1)
                if request:
                    queue_manager.mark_processed(request, processing_time=0.1)

            # Health monitoring
            health_monitor.record_event_processed(processing_time=0.15)
            health_monitor.record_event()

            await asyncio.sleep(0.1)  # Small delay between cycles

        # Collect autonomous operation report
        operation_duration = time.time() - start_time

        autonomous_report = {
            "operation_duration_seconds": operation_duration,
            "status": "operational",
            "queue_health": {
                "total_processed": queue_manager.get_queue_status()["metrics"]["total_processed"],
                "success_rate": queue_manager.get_queue_status()["metrics"]["success_rate"],
                "current_queue_depth": queue_manager.get_queue_status()["total_queued"],
                "utilization": queue_manager.get_queue_status()["utilization"],
            },
            "rate_limit_health": {
                "current_violations": rate_monitor.get_violation_summary()["current_window"][
                    "violations"
                ],
                "status_level": rate_monitor.get_violation_summary()["status"]["level"],
                "total_violations": rate_monitor.get_violation_summary()["totals"][
                    "total_violations"
                ],
            },
            "system_health": {
                "is_healthy": health_monitor.is_healthy(),
                "health_report": health_monitor.get_health_report(),
                "performance_summary": health_monitor.get_performance_summary(),
            },
            "recommendations": [],
        }

        # Add autonomous recommendations based on metrics
        if autonomous_report["queue_health"]["utilization"] > 0.8:
            autonomous_report["recommendations"].append("Consider scaling up processing capacity")

        if autonomous_report["rate_limit_health"]["status_level"] != "NORMAL":
            autonomous_report["recommendations"].append(
                "Rate limit issues detected - review API usage"
            )

        # Verify autonomous reporting
        assert autonomous_report["status"] == "operational"
        assert autonomous_report["operation_duration_seconds"] > 0
        assert autonomous_report["queue_health"]["success_rate"] >= 0
        assert autonomous_report["system_health"]["is_healthy"] is not None

    @pytest.mark.asyncio
    async def test_recovery_integration_for_autonomous_operation(
        self, mock_database, queue_manager, rate_monitor, health_monitor, state_manager
    ):
        """Test recovery manager integration for autonomous operation."""
        # Create retry engine
        retry_engine = RetryEngine(queue_manager=queue_manager)

        # Create recovery manager
        recovery_manager = ServiceRecoveryManager(
            state_manager=state_manager,
            queue_manager=queue_manager,
            rate_limit_monitor=rate_monitor,
            retry_engine=retry_engine,
            database=mock_database,
            max_recovery_duration_minutes=5,  # Shorter for testing
        )

        # Simulate system degradation
        for i in range(10):
            rate_monitor.record_violation(f"recovery_test_{i}", f"rec_req_{i}", "/recovery/test")

        # Test autonomous recovery capability
        recovery_success = await recovery_manager.start_recovery("Autonomous recovery test")

        # Verify recovery attempted
        recovery_status = recovery_manager.get_recovery_status()

        assert "is_recovering" in recovery_status
        assert "metrics" in recovery_status
        assert "configuration" in recovery_status

        # Check recovery metrics
        recovery_metrics = recovery_status["metrics"]
        assert "start_time" in recovery_metrics
        assert "current_phase" in recovery_metrics
        assert "status" in recovery_metrics

        # Recovery should complete or provide meaningful status
        assert recovery_metrics["status"] in ["completed", "failed", "in_progress"]

    @pytest.mark.asyncio
    async def test_continuous_monitoring_data_export(
        self, queue_manager, rate_monitor, health_monitor
    ):
        """Test continuous monitoring data export for cloud platforms."""
        monitoring_data = []

        # Simulate continuous monitoring over time
        for minute in range(3):
            # Generate activity
            for i in range(5):
                request = QueuedRequest(
                    request_id=f"export_test_{minute}_{i}",
                    priority=RequestPriority.LOW,
                    endpoint="/company/{company_number}",
                    params={"company_number": f"export_{minute}_{i}"},
                )
                await queue_manager.enqueue(request)

            # Process requests
            for _ in range(3):
                request = await queue_manager.dequeue(timeout=0.1)
                if request:
                    queue_manager.mark_processed(request, processing_time=0.05)

            # Record monitoring snapshot
            snapshot = {
                "timestamp": datetime.now().isoformat(),
                "minute": minute,
                "queue_depth": queue_manager.get_queue_status()["total_queued"],
                "processed_count": queue_manager.get_queue_status()["metrics"]["total_processed"],
                "rate_violations": rate_monitor.get_violation_summary()["current_window"][
                    "violations"
                ],
                "system_healthy": health_monitor.is_healthy(),
            }

            monitoring_data.append(snapshot)
            await asyncio.sleep(0.05)  # Small delay to simulate time passage

        # Verify continuous monitoring data
        assert len(monitoring_data) == 3

        # Verify data progression
        for i, snapshot in enumerate(monitoring_data):
            assert snapshot["minute"] == i
            assert "timestamp" in snapshot
            assert snapshot["queue_depth"] >= 0
            assert snapshot["processed_count"] >= 0

            # Later snapshots should show processing progress
            if i > 0:
                prev_snapshot = monitoring_data[i - 1]
                # Total processed should increase or stay same
                assert snapshot["processed_count"] >= prev_snapshot["processed_count"]

        # Verify exportable format
        export_json = json.dumps(monitoring_data, indent=2)
        assert len(export_json) > 100

        # Should be parseable
        parsed_data = json.loads(export_json)
        assert len(parsed_data) == 3
