"""Tests for service recovery procedures after rate limit issues."""

import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.streaming.company_state_manager import CompanyStateManager
from src.streaming.database import StreamingDatabase
from src.streaming.queue_manager import PriorityQueueManager, RequestPriority
from src.streaming.rate_limit_monitor import RateLimitMonitor
from src.streaming.recovery_manager import (
    RecoveryMetrics,
    RecoveryPhase,
    RecoveryStatus,
    ServiceRecoveryManager,
)
from src.streaming.retry_engine import RetryEngine


@pytest.fixture
def mock_state_manager():
    """Mock company state manager."""
    manager = AsyncMock(spec=CompanyStateManager)
    manager._get_companies_in_state = AsyncMock(return_value=["12345678", "87654321"])
    manager.update_state = AsyncMock()
    return manager


@pytest.fixture
def mock_queue_manager():
    """Mock queue manager."""
    manager = MagicMock(spec=PriorityQueueManager)
    manager.queues = {
        RequestPriority.HIGH: [],
        RequestPriority.MEDIUM: [],
        RequestPriority.LOW: [],
        RequestPriority.BACKGROUND: [],
    }
    manager.get_queue_status.return_value = {
        "total_queued": 0,
        "max_capacity": 10000,
        "utilization": 0.0,
    }
    manager.max_queue_size = 10000
    manager.enqueue = AsyncMock(return_value=True)
    return manager


@pytest.fixture
def mock_rate_limit_monitor():
    """Mock rate limit monitor."""
    monitor = MagicMock(spec=RateLimitMonitor)
    monitor.reset_monitoring = MagicMock()
    monitor.get_violation_summary.return_value = {
        "current_window": {"violations": 0},
        "status": {"level": "NORMAL"},
    }
    return monitor


@pytest.fixture
def mock_retry_engine():
    """Mock retry engine."""
    return MagicMock(spec=RetryEngine)


@pytest.fixture
def mock_database():
    """Mock database."""
    db = AsyncMock(spec=StreamingDatabase)
    db.execute = AsyncMock()
    db.fetch_all = AsyncMock(return_value=[])
    return db


@pytest.fixture
def recovery_manager(
    mock_state_manager,
    mock_queue_manager,
    mock_rate_limit_monitor,
    mock_retry_engine,
    mock_database,
):
    """Create recovery manager with mocked dependencies."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        recovery_path = Path(tmp_dir) / "recovery_state.json"

        return ServiceRecoveryManager(
            state_manager=mock_state_manager,
            queue_manager=mock_queue_manager,
            rate_limit_monitor=mock_rate_limit_monitor,
            retry_engine=mock_retry_engine,
            database=mock_database,
            recovery_state_path=recovery_path,
            max_recovery_duration_minutes=5,  # Shorter for testing
            gradual_restart_delay_seconds=1,  # Faster for testing
        )


class TestRecoveryMetrics:
    """Test recovery metrics functionality."""

    def test_recovery_metrics_initialization(self):
        """Test recovery metrics are initialized correctly."""
        metrics = RecoveryMetrics()

        assert metrics.current_phase == RecoveryPhase.VALIDATION
        assert metrics.status == RecoveryStatus.NOT_STARTED
        assert metrics.companies_validated == 0
        assert metrics.companies_recovered == 0
        assert metrics.test_requests_made == 0
        assert metrics.rate_limit_violations_during_recovery == 0

    def test_recovery_metrics_to_dict(self):
        """Test metrics conversion to dictionary."""
        metrics = RecoveryMetrics()
        metrics.companies_validated = 5
        metrics.test_requests_made = 3
        metrics.test_requests_successful = 2

        result = metrics.to_dict()

        assert result["companies_validated"] == 5
        assert result["test_requests_made"] == 3
        assert result["test_requests_successful"] == 2
        assert result["success_rate"] == 2 / 3
        assert "start_time" in result
        assert "current_phase" in result

    def test_success_rate_calculation(self):
        """Test success rate calculation."""
        metrics = RecoveryMetrics()

        # No requests made
        assert metrics._calculate_success_rate() == 1.0

        # Some requests made
        metrics.test_requests_made = 10
        metrics.test_requests_successful = 8
        assert metrics._calculate_success_rate() == 0.8


class TestServiceRecoveryManager:
    """Test service recovery manager functionality."""

    @pytest.mark.asyncio
    async def test_recovery_manager_initialization(self, recovery_manager):
        """Test recovery manager initializes correctly."""
        assert not recovery_manager.is_recovering
        assert not recovery_manager.emergency_stop
        assert recovery_manager.metrics.status == RecoveryStatus.NOT_STARTED
        assert recovery_manager.max_recovery_duration.total_seconds() == 300  # 5 minutes

    @pytest.mark.asyncio
    async def test_start_recovery_success(self, recovery_manager):
        """Test successful recovery process."""
        # Mock successful phases
        with patch.object(recovery_manager, "_execute_recovery_phases", return_value=True):
            success = await recovery_manager.start_recovery("Test recovery")

            assert success
            assert recovery_manager.metrics.status == RecoveryStatus.COMPLETED
            assert recovery_manager.metrics.total_duration is not None
            assert not recovery_manager.is_recovering

    @pytest.mark.asyncio
    async def test_start_recovery_failure(self, recovery_manager):
        """Test recovery process failure."""
        # Mock failed phases
        with patch.object(recovery_manager, "_execute_recovery_phases", return_value=False):
            success = await recovery_manager.start_recovery("Test recovery")

            assert not success
            assert recovery_manager.metrics.status == RecoveryStatus.FAILED
            assert not recovery_manager.is_recovering

    @pytest.mark.asyncio
    async def test_recovery_already_in_progress(self, recovery_manager):
        """Test starting recovery when already in progress."""
        recovery_manager.is_recovering = True

        success = await recovery_manager.start_recovery("Test recovery")

        assert not success

    @pytest.mark.asyncio
    async def test_emergency_stop_during_recovery(self, recovery_manager):
        """Test emergency stop functionality."""

        async def mock_execute_phases():
            # Simulate emergency stop during recovery
            recovery_manager.request_emergency_stop()
            return await recovery_manager._execute_recovery_phases()

        with patch.object(
            recovery_manager, "_execute_recovery_phases", side_effect=mock_execute_phases
        ):
            success = await recovery_manager.start_recovery("Test recovery")

            assert not success
            assert recovery_manager.emergency_stop


class TestRecoveryPhases:
    """Test individual recovery phases."""

    @pytest.mark.asyncio
    async def test_validation_phase_success(self, recovery_manager):
        """Test validation phase success."""
        # Mock database validation
        recovery_manager.database.execute = AsyncMock()

        with patch.object(
            recovery_manager,
            "_find_stale_processing_states",
            return_value=[("12345678", "status_processing", 2.5)],
        ):
            success = await recovery_manager._phase_validation()

            assert success
            assert recovery_manager.metrics.companies_validated == 1
            recovery_manager.rate_limit_monitor.reset_monitoring.assert_called_once()

    @pytest.mark.asyncio
    async def test_validation_phase_database_failure(self, recovery_manager):
        """Test validation phase with database failure."""
        recovery_manager.database.execute = AsyncMock(side_effect=Exception("Database error"))

        success = await recovery_manager._phase_validation()

        assert not success

    @pytest.mark.asyncio
    async def test_queue_restoration_phase(self, recovery_manager):
        """Test queue restoration phase."""
        # Setup queue with some requests
        recovery_manager.queue_manager.queues[RequestPriority.HIGH] = [MagicMock(), MagicMock()]
        recovery_manager.queue_manager.queues[RequestPriority.MEDIUM] = [MagicMock()]

        success = await recovery_manager._phase_queue_restoration()

        assert success
        assert recovery_manager.metrics.queued_requests_restored == 3

    @pytest.mark.asyncio
    async def test_state_reconciliation_phase_success(self, recovery_manager):
        """Test state reconciliation phase success."""
        # Mock companies in intermediate states
        recovery_manager.state_manager._get_companies_in_state = AsyncMock(
            side_effect=[
                ["12345678", "87654321"],  # STATUS_QUEUED
                [],  # STATUS_FETCHED
                ["11111111"],  # OFFICERS_QUEUED
                [],  # OFFICERS_FETCHED
            ]
        )
        recovery_manager.state_manager.update_state = AsyncMock()

        success = await recovery_manager._phase_state_reconciliation()

        assert success
        assert recovery_manager.metrics.companies_recovered == 3
        assert recovery_manager.metrics.companies_failed == 0

        # Verify all companies were updated to DETECTED state
        assert recovery_manager.state_manager.update_state.call_count == 3

    @pytest.mark.asyncio
    async def test_state_reconciliation_phase_with_failures(self, recovery_manager):
        """Test state reconciliation phase with some failures."""
        # Mock companies in intermediate states
        recovery_manager.state_manager._get_companies_in_state = AsyncMock(
            side_effect=[["12345678"], [], [], []]
        )

        # Mock update_state to fail for some companies
        recovery_manager.state_manager.update_state = AsyncMock(
            side_effect=Exception("Update failed")
        )

        success = await recovery_manager._phase_state_reconciliation()

        assert not success
        assert recovery_manager.metrics.companies_recovered == 0
        assert recovery_manager.metrics.companies_failed == 1

    @pytest.mark.asyncio
    async def test_gradual_restart_phase_no_test_companies(self, recovery_manager):
        """Test gradual restart phase with no test companies."""
        with patch.object(recovery_manager, "_get_test_companies", return_value=[]):
            success = await recovery_manager._phase_gradual_restart()

            assert success  # Should succeed with warning

    @pytest.mark.asyncio
    async def test_gradual_restart_phase_api_test_failure(self, recovery_manager):
        """Test gradual restart phase with API test failure."""
        with patch.object(recovery_manager, "_get_test_companies", return_value=["12345678"]):
            with patch.object(recovery_manager, "_test_api_connectivity", return_value=False):
                success = await recovery_manager._phase_gradual_restart()

                assert not success

    @pytest.mark.asyncio
    async def test_gradual_restart_phase_success(self, recovery_manager):
        """Test successful gradual restart phase."""
        with patch.object(recovery_manager, "_get_test_companies", return_value=["12345678"]):
            with patch.object(recovery_manager, "_test_api_connectivity", return_value=True):
                with patch.object(recovery_manager, "_run_limited_processing", return_value=True):
                    success = await recovery_manager._phase_gradual_restart()

                    assert success


class TestRecoveryUtilities:
    """Test recovery utility functions."""

    @pytest.mark.asyncio
    async def test_find_stale_processing_states(self, recovery_manager):
        """Test finding stale processing states."""
        # Mock database response
        recovery_manager.database.fetch_all = AsyncMock(
            return_value=[
                {
                    "company_number": "12345678",
                    "state": "status_queued",
                    "updated_at": (datetime.now() - timedelta(hours=2)).isoformat(),
                },
                {
                    "company_number": "87654321",
                    "state": "officers_fetched",
                    "updated_at": (datetime.now() - timedelta(hours=1.5)).isoformat(),
                },
            ]
        )

        stale_states = await recovery_manager._find_stale_processing_states()

        assert len(stale_states) == 2
        assert stale_states[0][0] == "12345678"
        assert stale_states[0][1] == "status_queued"
        assert stale_states[0][2] > 1.8  # Age in hours
        assert stale_states[1][0] == "87654321"

    @pytest.mark.asyncio
    async def test_get_test_companies(self, recovery_manager):
        """Test getting test companies."""
        recovery_manager.database.fetch_all = AsyncMock(
            return_value=[
                {"company_number": "12345678"},
                {"company_number": "87654321"},
                {"company_number": "11111111"},
            ]
        )

        companies = await recovery_manager._get_test_companies()

        assert len(companies) == 3
        assert "12345678" in companies
        assert "87654321" in companies
        assert "11111111" in companies

    @pytest.mark.asyncio
    async def test_test_api_connectivity_success(self, recovery_manager):
        """Test successful API connectivity test."""
        success = await recovery_manager._test_api_connectivity("12345678")

        assert success
        assert recovery_manager.metrics.test_requests_made == 1
        assert recovery_manager.metrics.test_requests_successful == 1
        recovery_manager.queue_manager.enqueue.assert_called_once()

    @pytest.mark.asyncio
    async def test_test_api_connectivity_queue_failure(self, recovery_manager):
        """Test API connectivity test with queue failure."""
        recovery_manager.queue_manager.enqueue = AsyncMock(return_value=False)

        success = await recovery_manager._test_api_connectivity("12345678")

        assert not success
        assert recovery_manager.metrics.test_requests_made == 1
        assert recovery_manager.metrics.test_requests_successful == 0

    @pytest.mark.asyncio
    async def test_run_limited_processing_success(self, recovery_manager):
        """Test successful limited processing."""
        phase_config = {
            "name": "test_phase",
            "max_concurrent": 1,
            "duration_seconds": 0.1,  # Very short for testing
        }

        success = await recovery_manager._run_limited_processing(phase_config)

        assert success

    @pytest.mark.asyncio
    async def test_run_limited_processing_with_violations(self, recovery_manager):
        """Test limited processing with rate limit violations."""
        phase_config = {"name": "test_phase", "max_concurrent": 1, "duration_seconds": 0.1}

        # Mock high violation count
        recovery_manager.rate_limit_monitor.get_violation_summary.return_value = {
            "current_window": {"violations": 10}
        }

        success = await recovery_manager._run_limited_processing(phase_config)

        assert not success
        assert recovery_manager.metrics.rate_limit_violations_during_recovery > 0

    @pytest.mark.asyncio
    async def test_save_and_load_recovery_state(self, recovery_manager):
        """Test saving and loading recovery state."""
        # Set some state
        recovery_manager.metrics.companies_validated = 5
        recovery_manager.metrics.companies_recovered = 3
        recovery_manager.is_recovering = True

        # Save state
        await recovery_manager._save_recovery_state()

        # Verify file was created
        assert recovery_manager.recovery_state_path.exists()

        # Load and verify
        success = await recovery_manager.load_recovery_state()
        assert success

        # Verify content
        content = json.loads(recovery_manager.recovery_state_path.read_text())
        assert content["metrics"]["companies_validated"] == 5
        assert content["metrics"]["companies_recovered"] == 3
        assert content["is_recovering"] == True


class TestRecoveryStatus:
    """Test recovery status reporting."""

    def test_get_recovery_status(self, recovery_manager):
        """Test getting recovery status."""
        recovery_manager.is_recovering = True
        recovery_manager.metrics.companies_validated = 10

        status = recovery_manager.get_recovery_status()

        assert status["is_recovering"] == True
        assert status["emergency_stop"] == False
        assert status["metrics"]["companies_validated"] == 10
        assert "configuration" in status
        assert status["configuration"]["max_recovery_duration_minutes"] == 5.0

    def test_request_emergency_stop(self, recovery_manager):
        """Test requesting emergency stop."""
        recovery_manager.request_emergency_stop()

        assert recovery_manager.emergency_stop == True
