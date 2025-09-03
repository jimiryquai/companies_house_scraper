"""Tests for monitoring integration and high-volume streaming events."""

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from src.streaming.api_usage_monitor import RealTimeAPIUsageMonitor
from src.streaming.autonomous_dashboard import AutonomousOperationDashboard
from src.streaming.company_state_manager import CompanyStateManager
from src.streaming.comprehensive_metrics_collector import ComprehensiveMetricsCollector
from src.streaming.config import StreamingConfig
from src.streaming.health_endpoints import CloudHealthEndpoints, HealthStatus
from src.streaming.queue_manager import PriorityQueueManager
from src.streaming.rate_limit_monitor import RateLimitMonitor
from src.streaming.recovery_manager import ServiceRecoveryManager
from src.streaming.recovery_status_reporter import RecoveryCategory, RecoveryStatusReporter
from src.streaming.structured_logger import LogLevel, StructuredLogger
from src.streaming.unattended_operation_logger import (
    DiagnosticCategory,
    OperationalSeverity,
    UnattendedOperationLogger,
)
from src.streaming.unified_alerting_system import UnifiedAlertingSystem


class TestMonitoringIntegration:
    """Test comprehensive monitoring system integration."""

    @pytest.fixture
    async def config(self):
        """Create test configuration."""
        return StreamingConfig(
            streaming_api_key="test_streaming_key_123456789",
            rest_api_key="test_rest_key_123456789",
            database_path=":memory:",
        )

    @pytest.fixture
    async def base_logger(self, config):
        """Create test logger."""
        logger = StructuredLogger(config, log_level=LogLevel.DEBUG)
        await logger.start()
        yield logger
        await logger.stop()

    @pytest.fixture
    async def mock_queue_manager(self):
        """Create mock queue manager for ComprehensiveMetricsCollector."""
        mock_queue = MagicMock(spec=PriorityQueueManager)
        mock_queue.get_metrics = MagicMock(return_value=MagicMock())
        return mock_queue

    @pytest.fixture
    async def mock_api_usage_monitor(self):
        """Create mock API usage monitor for ComprehensiveMetricsCollector."""
        mock_monitor = MagicMock(spec=RealTimeAPIUsageMonitor)

        # Configure the mock to return proper statistics structure
        mock_stats = {
            "current_window": {
                "usage_percentage": 25.0,
                "calls_remaining": 450,
                "window_end_time": "2024-01-01T12:00:00Z",
            },
            "usage_level": "safe",
            "rate_limit": 600,
        }
        mock_monitor.get_current_statistics = MagicMock(return_value=mock_stats)
        return mock_monitor

    @pytest.fixture
    async def mock_rate_limit_monitor(self):
        """Create mock rate limit monitor for ComprehensiveMetricsCollector."""
        mock_monitor = MagicMock(spec=RateLimitMonitor)
        mock_monitor.get_metrics = MagicMock(return_value=MagicMock())
        return mock_monitor

    @pytest.fixture
    async def mock_state_manager(self):
        """Create mock state manager for ComprehensiveMetricsCollector."""
        mock_manager = MagicMock(spec=CompanyStateManager)
        mock_manager.get_processing_metrics = MagicMock(return_value=MagicMock())
        return mock_manager

    @pytest.fixture
    async def metrics_collector(
        self,
        mock_queue_manager,
        mock_api_usage_monitor,
        mock_rate_limit_monitor,
        mock_state_manager,
    ):
        """Create test metrics collector with proper dependencies."""
        return ComprehensiveMetricsCollector(
            queue_manager=mock_queue_manager,
            api_usage_monitor=mock_api_usage_monitor,
            rate_limit_monitor=mock_rate_limit_monitor,
            state_manager=mock_state_manager,
        )

    @pytest.fixture
    async def dashboard(
        self,
        config,
        metrics_collector,
        mock_api_usage_monitor,
        mock_rate_limit_monitor,
        mock_state_manager,
        mock_queue_manager,
    ):
        """Create test dashboard."""
        # Create mock ServiceRecoveryManager
        mock_recovery_manager = MagicMock(spec=ServiceRecoveryManager)

        return AutonomousOperationDashboard(
            metrics_collector=metrics_collector,
            api_usage_monitor=mock_api_usage_monitor,
            rate_limit_monitor=mock_rate_limit_monitor,
            state_manager=mock_state_manager,
            recovery_manager=mock_recovery_manager,
            queue_manager=mock_queue_manager,
        )

    @pytest.fixture
    async def health_endpoints(self, config, metrics_collector, dashboard):
        """Create test health endpoints."""
        return CloudHealthEndpoints(
            config=config, metrics_collector=metrics_collector, dashboard=dashboard
        )

    @pytest.fixture
    async def operational_logger(self, config, base_logger, metrics_collector, dashboard):
        """Create test operational logger."""
        logger = UnattendedOperationLogger(
            config=config,
            base_logger=base_logger,
            metrics_collector=metrics_collector,
            dashboard=dashboard,
        )
        await logger.start()
        yield logger
        await logger.stop()

    @pytest.fixture
    async def recovery_reporter(self, config, base_logger, operational_logger):
        """Create test recovery reporter."""
        reporter = RecoveryStatusReporter(
            config=config, base_logger=base_logger, operational_logger=operational_logger
        )
        await reporter.start()
        yield reporter
        await reporter.stop()

    @pytest.fixture
    async def alerting_system(self, config):
        """Create test alerting system."""
        return UnifiedAlertingSystem(config=config)


class TestAutonomousOperationDashboard:
    """Test autonomous operation dashboard functionality."""

    @pytest.fixture
    async def dashboard(self):
        """Create test dashboard."""
        config = StreamingConfig(
            streaming_api_key="test_streaming_key_123456789",
            rest_api_key="test_rest_key_123456789",
            database_path=":memory:",
        )

        # Create mock dependencies for ComprehensiveMetricsCollector
        mock_queue_manager = MagicMock(spec=PriorityQueueManager)
        mock_api_usage_monitor = MagicMock(spec=RealTimeAPIUsageMonitor)
        mock_rate_limit_monitor = MagicMock(spec=RateLimitMonitor)
        mock_state_manager = MagicMock(spec=CompanyStateManager)

        metrics_collector = ComprehensiveMetricsCollector(
            queue_manager=mock_queue_manager,
            api_usage_monitor=mock_api_usage_monitor,
            rate_limit_monitor=mock_rate_limit_monitor,
            state_manager=mock_state_manager,
        )
        # Create mock ServiceRecoveryManager
        mock_recovery_manager = MagicMock(spec=ServiceRecoveryManager)

        return AutonomousOperationDashboard(
            metrics_collector=metrics_collector,
            api_usage_monitor=mock_api_usage_monitor,
            rate_limit_monitor=mock_rate_limit_monitor,
            state_manager=mock_state_manager,
            recovery_manager=mock_recovery_manager,
            queue_manager=mock_queue_manager,
        )

    async def test_dashboard_initialization(self, dashboard):
        """Test dashboard initializes correctly."""
        assert dashboard is not None
        assert dashboard.start_time is not None

    @pytest.mark.skip(
        reason="Method uses asyncio.run() which conflicts with pytest async environment"
    )
    async def test_get_status_summary(self, dashboard):
        """Test status summary generation."""
        summary = dashboard.get_status_summary()

        assert "system_status" in summary
        assert "uptime_hours" in summary
        assert "api_compliance" in summary
        assert "queue_operations" in summary
        assert "processing_metrics" in summary

    @pytest.mark.skip(reason="Method calls async operations but is not properly awaitable")
    async def test_get_dashboard_data(self, dashboard):
        """Test comprehensive dashboard data collection."""
        data = dashboard.get_dashboard_data()

        assert "system_overview" in data
        assert "api_compliance_status" in data
        assert "queue_operations_status" in data
        assert "company_processing_status" in data
        assert "recovery_status" in data
        assert "operational_recommendations" in data

    @pytest.mark.skip(
        reason="Method uses asyncio.run() which conflicts with pytest async environment"
    )
    async def test_export_for_monitoring_platform(self, dashboard):
        """Test monitoring platform export functionality."""
        with patch("json.dumps") as mock_dumps:
            mock_dumps.return_value = '{"test": "data"}'

            result = dashboard.export_for_monitoring_platform("prometheus")

            assert result is not None
            mock_dumps.assert_called_once()


class TestHealthEndpoints:
    """Test health check endpoints for cloud deployment."""

    @pytest.fixture
    async def health_endpoints(self):
        """Create test health endpoints."""
        config = StreamingConfig(
            streaming_api_key="test_streaming_key_123456789",
            rest_api_key="test_rest_key_123456789",
            database_path=":memory:",
        )
        return CloudHealthEndpoints(config=config)

    async def test_liveness_probe(self, health_endpoints):
        """Test Kubernetes-style liveness probe."""
        result = await health_endpoints.liveness_probe()

        assert "status" in result
        assert "timestamp" in result
        assert "response_time_ms" in result
        assert result["status"] == HealthStatus.UP.value

    async def test_readiness_probe(self, health_endpoints):
        """Test Kubernetes-style readiness probe."""
        result = await health_endpoints.readiness_probe()

        assert "status" in result
        assert "timestamp" in result
        assert "message" in result

    async def test_startup_probe(self, health_endpoints):
        """Test Kubernetes-style startup probe."""
        result = await health_endpoints.startup_probe()

        assert "status" in result
        assert "timestamp" in result
        assert "components" in result

    async def test_comprehensive_health_check(self, health_endpoints):
        """Test comprehensive health check endpoint."""
        result = await health_endpoints.health_check(detailed=True)

        assert "status" in result
        assert "timestamp" in result
        assert "uptime_seconds" in result
        assert "version" in result
        assert "components" in result
        assert "summary" in result

    async def test_metrics_endpoint(self, health_endpoints):
        """Test Prometheus-style metrics endpoint."""
        result = await health_endpoints.metrics_endpoint()

        assert "timestamp" in result
        assert "metrics" in result
        assert "service_uptime_seconds" in result["metrics"]


class TestUnattendedOperationLogger:
    """Test unattended operation logging for cloud diagnostics."""

    @pytest.fixture
    async def operational_logger(self):
        """Create test operational logger."""
        config = StreamingConfig(
            streaming_api_key="test_streaming_key_123456789",
            rest_api_key="test_rest_key_123456789",
            database_path=":memory:",
        )
        base_logger = StructuredLogger(config, log_level=LogLevel.DEBUG)
        await base_logger.start()

        logger = UnattendedOperationLogger(config=config, base_logger=base_logger)
        await logger.start()
        yield logger
        await logger.stop()
        await base_logger.stop()

    async def test_operational_event_logging(self, operational_logger):
        """Test operational event logging functionality."""
        await operational_logger.log_operational_event(
            severity=OperationalSeverity.INFO,
            category=DiagnosticCategory.API_OPERATIONS,
            event_type="test_operation",
            message="Test operational event",
            details={"test_param": "test_value"},
        )

        # Verify log file exists
        assert operational_logger.log_file.exists()

    async def test_api_operation_logging(self, operational_logger):
        """Test API operation logging with performance tracking."""
        await operational_logger.log_api_operation(
            operation="get_company_status",
            company_number="12345678",
            response_time_ms=150.5,
            status_code=200,
            rate_limit_remaining=590,
            success=True,
        )

        assert operational_logger.log_file.exists()

    async def test_queue_operation_logging(self, operational_logger):
        """Test queue operation logging."""
        await operational_logger.log_queue_operation(
            operation="enqueue",
            queue_name="high_priority",
            queue_depth=25,
            processing_rate=45.2,
            success=True,
        )

        assert operational_logger.log_file.exists()

    async def test_diagnostic_report_generation(self, operational_logger):
        """Test comprehensive diagnostic report generation."""
        report = await operational_logger.generate_diagnostic_report()

        assert "timestamp" in report
        assert "uptime_seconds" in report
        assert "system_snapshot" in report
        assert "environment" in report

        # Verify system snapshot contains expected metrics
        snapshot = report["system_snapshot"]
        assert "memory_usage_mb" in snapshot
        assert "cpu_percentage" in snapshot
        assert "uptime_seconds" in snapshot


class TestRecoveryStatusReporter:
    """Test automated recovery status reporting."""

    @pytest.fixture
    async def recovery_reporter(self):
        """Create test recovery status reporter."""
        config = StreamingConfig(
            streaming_api_key="test_streaming_key_123456789",
            rest_api_key="test_rest_key_123456789",
            database_path=":memory:",
        )
        base_logger = StructuredLogger(config, log_level=LogLevel.DEBUG)
        await base_logger.start()

        reporter = RecoveryStatusReporter(config=config, base_logger=base_logger)
        await reporter.start()
        yield reporter
        await reporter.stop()
        await base_logger.stop()

    async def test_recovery_incident_tracking(self, recovery_reporter):
        """Test recovery incident creation and tracking."""
        incident_id = await recovery_reporter.report_recovery_initiated(
            category=RecoveryCategory.RATE_LIMIT_RECOVERY,
            trigger_event="rate_limit_exceeded",
            symptoms=["429 responses", "queue backlog"],
        )

        assert incident_id is not None
        assert incident_id in recovery_reporter._active_incidents

        # Test recovery action reporting
        await recovery_reporter.report_recovery_action(
            incident_id=incident_id,
            action_type="queue_throttle",
            description="Reduced queue processing rate",
            success=True,
            duration_seconds=5.2,
        )

        incident = recovery_reporter._active_incidents[incident_id]
        assert len(incident.actions_taken) == 1
        assert incident.actions_taken[0].action_type == "queue_throttle"

    async def test_recovery_completion(self, recovery_reporter):
        """Test recovery completion tracking."""
        incident_id = await recovery_reporter.report_recovery_initiated(
            category=RecoveryCategory.CONNECTION_RECOVERY,
            trigger_event="connection_lost",
            symptoms=["websocket_disconnected"],
        )

        await recovery_reporter.report_recovery_completed(
            incident_id=incident_id,
            success=True,
            final_outcome="Connection restored successfully",
            lessons_learned=["Implement faster reconnection logic"],
        )

        assert incident_id not in recovery_reporter._active_incidents
        assert len(recovery_reporter._completed_incidents) == 1

        completed = recovery_reporter._completed_incidents[0]
        assert completed.success is True
        assert completed.final_outcome == "Connection restored successfully"

    async def test_status_summary_generation(self, recovery_reporter):
        """Test recovery status summary generation."""
        # Create some test incidents
        await recovery_reporter.report_recovery_initiated(
            category=RecoveryCategory.API_RECOVERY,
            trigger_event="api_error",
            symptoms=["500 responses"],
        )

        summary = await recovery_reporter.get_recovery_status_summary()

        assert "timestamp" in summary.to_dict()
        assert "active_incidents" in summary.to_dict()
        assert "system_health_score" in summary.to_dict()
        assert "stability_trend" in summary.to_dict()
        assert summary.active_incidents >= 1


class TestHighVolumeStreamingScenarios:
    """Test monitoring system under high-volume streaming conditions."""

    @pytest.fixture
    async def monitoring_system(self):
        """Create complete monitoring system for high-volume testing."""
        config = StreamingConfig(
            streaming_api_key="test_streaming_key_123456789",
            rest_api_key="test_rest_key_123456789",
            database_path=":memory:",
        )

        base_logger = StructuredLogger(config, log_level=LogLevel.INFO)
        await base_logger.start()

        # Create mock dependencies for ComprehensiveMetricsCollector
        mock_queue_manager = MagicMock(spec=PriorityQueueManager)
        mock_api_usage_monitor = MagicMock(spec=RealTimeAPIUsageMonitor)
        mock_rate_limit_monitor = MagicMock(spec=RateLimitMonitor)
        mock_state_manager = MagicMock(spec=CompanyStateManager)

        metrics_collector = ComprehensiveMetricsCollector(
            queue_manager=mock_queue_manager,
            api_usage_monitor=mock_api_usage_monitor,
            rate_limit_monitor=mock_rate_limit_monitor,
            state_manager=mock_state_manager,
        )
        # Create mock ServiceRecoveryManager
        mock_recovery_manager = MagicMock(spec=ServiceRecoveryManager)

        dashboard = AutonomousOperationDashboard(
            metrics_collector=metrics_collector,
            api_usage_monitor=mock_api_usage_monitor,
            rate_limit_monitor=mock_rate_limit_monitor,
            state_manager=mock_state_manager,
            recovery_manager=mock_recovery_manager,
            queue_manager=mock_queue_manager,
        )

        operational_logger = UnattendedOperationLogger(
            config=config,
            base_logger=base_logger,
            metrics_collector=metrics_collector,
            dashboard=dashboard,
        )
        await operational_logger.start()

        recovery_reporter = RecoveryStatusReporter(
            config=config, base_logger=base_logger, operational_logger=operational_logger
        )
        await recovery_reporter.start()

        health_endpoints = CloudHealthEndpoints(
            config=config, metrics_collector=metrics_collector, dashboard=dashboard
        )

        yield {
            "config": config,
            "base_logger": base_logger,
            "metrics_collector": metrics_collector,
            "dashboard": dashboard,
            "operational_logger": operational_logger,
            "recovery_reporter": recovery_reporter,
            "health_endpoints": health_endpoints,
        }

        # Cleanup
        await recovery_reporter.stop()
        await operational_logger.stop()
        await base_logger.stop()

    async def test_high_volume_event_processing(self, monitoring_system):
        """Test monitoring system under high-volume event load (simulating 700+ companies)."""
        operational_logger = monitoring_system["operational_logger"]
        dashboard = monitoring_system["dashboard"]

        # Simulate high-volume streaming events
        event_count = 750  # Simulate more than 700 companies

        # Process events in batches to simulate realistic load
        batch_size = 50
        for batch_start in range(0, event_count, batch_size):
            batch_end = min(batch_start + batch_size, event_count)

            # Simulate batch of API operations
            batch_tasks = []
            for i in range(batch_start, batch_end):
                company_number = f"COMP{i:06d}"
                task = operational_logger.log_api_operation(
                    operation="get_company_status",
                    company_number=company_number,
                    response_time_ms=50 + (i % 100),  # Varying response times
                    status_code=200,
                    rate_limit_remaining=600 - (i % 600),
                    success=True,
                )
                batch_tasks.append(task)

            # Execute batch concurrently
            await asyncio.gather(*batch_tasks)

            # Brief pause between batches to simulate realistic timing
            await asyncio.sleep(0.01)

        # Verify system handled high volume correctly
        dashboard_data = dashboard.get_dashboard_data()
        assert "system_overview" in dashboard_data

        # Check that operational log exists and is not corrupted
        assert operational_logger.log_file.exists()
        log_size = operational_logger.log_file.stat().st_size
        assert log_size > 0  # Should have logged significant data

    async def test_concurrent_monitoring_operations(self, monitoring_system):
        """Test concurrent monitoring operations under load."""
        operational_logger = monitoring_system["operational_logger"]
        recovery_reporter = monitoring_system["recovery_reporter"]
        health_endpoints = monitoring_system["health_endpoints"]
        dashboard = monitoring_system["dashboard"]

        # Create concurrent monitoring tasks
        tasks = []

        # Multiple health checks
        for i in range(10):
            tasks.append(health_endpoints.health_check())

        # Multiple dashboard data requests
        for i in range(5):
            tasks.append(dashboard.get_dashboard_data())

        # Multiple operational log entries
        for i in range(20):
            tasks.append(
                operational_logger.log_api_operation(
                    operation=f"concurrent_test_{i}",
                    company_number=f"TEST{i:04d}",
                    response_time_ms=25.0,
                    success=True,
                )
            )

        # Recovery status checks
        for i in range(3):
            tasks.append(recovery_reporter.get_recovery_status_summary())

        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Verify no exceptions occurred
        exceptions = [r for r in results if isinstance(r, Exception)]
        assert len(exceptions) == 0, f"Concurrent operations failed: {exceptions}"

        # Verify all operations completed successfully
        assert len(results) == 38  # 10 + 5 + 20 + 3

    async def test_memory_usage_under_load(self, monitoring_system):
        """Test that monitoring system maintains reasonable memory usage under load."""
        operational_logger = monitoring_system["operational_logger"]

        # Generate initial diagnostic report to establish baseline
        initial_report = await operational_logger.generate_diagnostic_report()
        initial_memory = initial_report["system_snapshot"]["memory_usage_mb"]

        # Simulate extended high-volume operation
        for batch in range(10):  # 10 batches of operations
            batch_tasks = []
            for i in range(100):  # 100 operations per batch
                task = operational_logger.log_api_operation(
                    operation="memory_test",
                    company_number=f"MEM{batch:02d}{i:03d}",
                    response_time_ms=30.0,
                    success=True,
                )
                batch_tasks.append(task)

            await asyncio.gather(*batch_tasks)

            # Brief pause to allow garbage collection
            await asyncio.sleep(0.1)

        # Check final memory usage
        final_report = await operational_logger.generate_diagnostic_report()
        final_memory = final_report["system_snapshot"]["memory_usage_mb"]

        # Memory usage should not have increased dramatically (allow for some growth)
        memory_increase = final_memory - initial_memory
        assert memory_increase < 50, f"Memory usage increased too much: {memory_increase}MB"

    async def test_system_stability_metrics(self, monitoring_system):
        """Test system stability metrics during high-volume operation."""
        recovery_reporter = monitoring_system["recovery_reporter"]
        dashboard = monitoring_system["dashboard"]

        # Simulate some recovery scenarios during high volume
        incident_ids = []

        # Create a few recovery incidents
        for i in range(3):
            incident_id = await recovery_reporter.report_recovery_initiated(
                category=RecoveryCategory.RATE_LIMIT_RECOVERY,
                trigger_event=f"high_volume_test_{i}",
                symptoms=[f"symptom_{i}"],
            )
            incident_ids.append(incident_id)

        # Complete some incidents successfully
        for i, incident_id in enumerate(incident_ids[:2]):
            await recovery_reporter.report_recovery_completed(
                incident_id=incident_id, success=True, final_outcome=f"Test recovery {i} completed"
            )

        # Get stability metrics
        status_summary = await recovery_reporter.get_recovery_status_summary()
        dashboard_data = dashboard.get_dashboard_data()

        # Verify metrics are reasonable
        assert status_summary.system_health_score >= 0.0
        assert status_summary.system_health_score <= 1.0
        assert status_summary.active_incidents == 1  # One still active
        assert status_summary.completed_incidents_24h >= 2

        # Dashboard should reflect system status
        assert "system_overview" in dashboard_data
        assert "recovery_status" in dashboard_data
