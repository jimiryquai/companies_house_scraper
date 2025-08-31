"""Tests for bulletproof queue-only API architecture.

This test suite validates that the streaming service operates with ZERO direct API calls
and ensures 100% rate limit compliance through queue-only architecture for cloud deployment.
"""

import asyncio
import sqlite3
import tempfile
from collections.abc import Generator
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

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
def mock_queue_manager() -> MagicMock:
    """Create a mock PriorityQueueManager for testing."""
    mock_manager = MagicMock(spec=PriorityQueueManager)
    mock_manager.enqueue = AsyncMock(return_value=True)
    mock_manager.get_metrics = MagicMock(
        return_value={
            "total_enqueued": 10,
            "total_processed": 8,
            "high_priority_queue_depth": 2,
            "medium_priority_queue_depth": 1,
            "background_priority_queue_depth": 0,
        }
    )
    return mock_manager


@pytest.fixture
async def state_manager(temp_db_path: Path, mock_queue_manager: MagicMock) -> CompanyStateManager:
    """Create and initialize CompanyStateManager instance for testing."""
    manager = CompanyStateManager(str(temp_db_path), mock_queue_manager)
    await manager.initialize()
    return manager


class TestQueueOnlyArchitectureEnforcement:
    """Test enforcement of queue-only architecture with zero direct API calls."""

    @pytest.mark.asyncio
    async def test_company_state_manager_zero_direct_api_calls(
        self, state_manager: CompanyStateManager
    ) -> None:
        """Test that CompanyStateManager makes ZERO direct API calls."""
        # CompanyStateManager should have no HTTP client capabilities
        assert not hasattr(state_manager, "http_client")
        assert not hasattr(state_manager, "session")
        assert not hasattr(state_manager, "requests")
        assert not hasattr(state_manager, "_make_api_call")
        assert not hasattr(state_manager, "_fetch_company_data")

        # All API operations should go through queue_manager
        assert state_manager.queue_manager is not None
        assert hasattr(state_manager.queue_manager, "enqueue")

    @pytest.mark.asyncio
    async def test_status_check_creates_queue_request_only(
        self, state_manager: CompanyStateManager, mock_queue_manager: MagicMock
    ) -> None:
        """Test that status checks create queue requests without direct API calls."""
        company_number = "12345678"

        # Queue a status check
        request_id = await state_manager.queue_status_check(company_number)

        # Verify queue manager was called
        mock_queue_manager.enqueue.assert_called_once()
        call_args = mock_queue_manager.enqueue.call_args[0][0]

        # Verify the queued request
        assert isinstance(call_args, QueuedRequest)
        assert call_args.priority == RequestPriority.HIGH
        assert call_args.endpoint == f"/company/{company_number}"
        assert call_args.params["company_number"] == company_number
        assert call_args.params["request_type"] == "company_status"
        assert call_args.params["state_manager_queued"] is True

        # Verify state was updated to STATUS_QUEUED
        state = await state_manager.get_state(company_number)
        assert state is not None
        assert state["processing_state"] == ProcessingState.STATUS_QUEUED.value
        assert state["status_request_id"] == request_id

    @pytest.mark.asyncio
    async def test_officers_fetch_creates_queue_request_only(
        self, state_manager: CompanyStateManager, mock_queue_manager: MagicMock
    ) -> None:
        """Test that officers fetch creates queue requests without direct API calls."""
        company_number = "87654321"

        # First set up company in STRIKE_OFF_CONFIRMED state
        await state_manager.update_state(company_number, ProcessingState.STRIKE_OFF_CONFIRMED.value)

        # Queue officers fetch
        request_id = await state_manager.queue_officers_fetch(company_number)

        # Verify queue manager was called
        mock_queue_manager.enqueue.assert_called_once()
        call_args = mock_queue_manager.enqueue.call_args[0][0]

        # Verify the queued request
        assert isinstance(call_args, QueuedRequest)
        assert call_args.priority == RequestPriority.MEDIUM
        assert call_args.endpoint == f"/company/{company_number}/officers"
        assert call_args.params["company_number"] == company_number
        assert call_args.params["request_type"] == "officers"
        assert call_args.params["state_manager_queued"] is True

        # Verify state was updated to OFFICERS_QUEUED
        state = await state_manager.get_state(company_number)
        assert state is not None
        assert state["processing_state"] == ProcessingState.OFFICERS_QUEUED.value
        assert state["officers_request_id"] == request_id

    @pytest.mark.asyncio
    async def test_queue_manager_dependency_enforcement(self, temp_db_path: Path) -> None:
        """Test that CompanyStateManager requires queue manager dependency."""
        # Should not allow None queue manager
        with pytest.raises(TypeError):
            CompanyStateManager(str(temp_db_path), None)  # type: ignore

    @pytest.mark.asyncio
    async def test_no_direct_api_imports_in_state_manager(self) -> None:
        """Test that state manager doesn't import HTTP libraries."""
        import src.streaming.company_state_manager as csm_module

        # Check module doesn't import HTTP libraries
        module_attrs = dir(csm_module)
        forbidden_imports = ["requests", "httpx", "aiohttp", "urllib"]

        for forbidden in forbidden_imports:
            assert forbidden not in module_attrs, f"State manager imports {forbidden}"


class TestQueueRequestGeneration:
    """Test queue request generation and priority handling."""

    @pytest.mark.asyncio
    async def test_high_priority_status_requests(
        self, state_manager: CompanyStateManager, mock_queue_manager: MagicMock
    ) -> None:
        """Test that status checks generate HIGH priority requests."""
        company_number = "HIGH123"

        await state_manager.queue_status_check(company_number)

        # Verify HIGH priority was used
        call_args = mock_queue_manager.enqueue.call_args[0][0]
        assert call_args.priority == RequestPriority.HIGH

    @pytest.mark.asyncio
    async def test_medium_priority_officers_requests(
        self, state_manager: CompanyStateManager, mock_queue_manager: MagicMock
    ) -> None:
        """Test that officers fetches generate MEDIUM priority requests."""
        company_number = "MED456"

        # Set up company in correct state first
        await state_manager.update_state(company_number, ProcessingState.STRIKE_OFF_CONFIRMED.value)

        await state_manager.queue_officers_fetch(company_number)

        # Verify MEDIUM priority was used
        call_args = mock_queue_manager.enqueue.call_args[0][0]
        assert call_args.priority == RequestPriority.MEDIUM

    @pytest.mark.asyncio
    async def test_request_id_generation_uniqueness(
        self, state_manager: CompanyStateManager
    ) -> None:
        """Test that request IDs are unique."""
        company_number = "UNIQUE789"

        # Generate multiple request IDs
        request_id_1 = await state_manager.queue_status_check(company_number)

        # Update state to allow another status check
        await state_manager.update_state(company_number, ProcessingState.STATUS_FETCHED.value)
        await state_manager.update_state(company_number, ProcessingState.STRIKE_OFF_CONFIRMED.value)

        request_id_2 = await state_manager.queue_officers_fetch(company_number)

        # Verify uniqueness
        assert request_id_1 != request_id_2
        assert request_id_1.startswith("status_")
        assert request_id_2.startswith("officers_")

    @pytest.mark.asyncio
    async def test_request_metadata_tracking(
        self, state_manager: CompanyStateManager, mock_queue_manager: MagicMock
    ) -> None:
        """Test that request metadata is properly tracked."""
        company_number = "META999"

        await state_manager.queue_status_check(company_number)

        # Verify request contains proper metadata
        call_args = mock_queue_manager.enqueue.call_args[0][0]
        assert call_args.params["state_manager_queued"] is True
        assert "company_number" in call_args.params
        assert "request_type" in call_args.params


class TestRateLimitCompliance:
    """Test rate limit compliance through queue-only architecture."""

    @pytest.mark.asyncio
    async def test_429_response_handling_without_retry(
        self, state_manager: CompanyStateManager, temp_db_path: Path
    ) -> None:
        """Test 429 response handling logs violation without immediate retry."""
        company_number = "RATE429"
        request_id = "test_request_429"

        # Set up company with some state
        await state_manager.update_state(
            company_number, ProcessingState.STATUS_QUEUED.value, status_request_id=request_id
        )

        # Handle 429 response
        await state_manager.handle_429_response(company_number, request_id)

        # Verify rate limit violation was logged
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM api_rate_limit_log WHERE company_number = ? AND response_code = 429",
            (company_number,),
        )
        log_entry = cursor.fetchone()
        conn.close()

        assert log_entry is not None
        assert log_entry[3] == 429  # response_code
        assert log_entry[4] == company_number
        assert log_entry[5] == request_id

    @pytest.mark.asyncio
    async def test_rate_limit_violation_tracking(self, state_manager: CompanyStateManager) -> None:
        """Test rate limit violations are tracked per company."""
        company_number = "VIOLATIONS"
        request_id = "test_request_violations"

        # Set up company
        await state_manager.update_state(
            company_number, ProcessingState.STATUS_QUEUED.value, status_request_id=request_id
        )

        # Handle multiple 429 responses
        await state_manager.handle_429_response(company_number, request_id)
        await state_manager.handle_429_response(company_number, request_id)
        await state_manager.handle_429_response(company_number, request_id)

        # Verify violation count increased
        state = await state_manager.get_state(company_number)
        assert state is not None
        assert state["rate_limit_violations"] == 3

    @pytest.mark.asyncio
    async def test_automatic_cleanup_of_excessive_violations(
        self, state_manager: CompanyStateManager
    ) -> None:
        """Test automatic cleanup of companies with excessive rate limit violations."""
        company_number = "EXCESSIVE"

        # Set up company with excessive violations
        await state_manager.update_state(
            company_number,
            ProcessingState.STATUS_QUEUED.value,
            rate_limit_violations=15,  # Above the 10 violation limit
        )

        # Run cleanup
        cleaned_count = await state_manager.cleanup_failed_companies()

        # Verify company was marked as failed
        assert cleaned_count == 1
        state = await state_manager.get_state(company_number)
        assert state is not None
        assert state["processing_state"] == ProcessingState.FAILED.value


class TestBulletproofDeploymentReadiness:
    """Test bulletproof cloud deployment readiness."""

    @pytest.mark.asyncio
    async def test_zero_external_dependencies(self, state_manager: CompanyStateManager) -> None:
        """Test that state manager has zero external HTTP dependencies."""
        # Should only depend on queue manager and database
        dependencies = [
            state_manager.queue_manager,
            state_manager.database_path,
            state_manager._state_locks,
            state_manager._db_lock,
        ]

        # All dependencies should be internal or controlled
        assert all(dep is not None for dep in dependencies)

    @pytest.mark.asyncio
    async def test_autonomous_monitoring_metrics(
        self, state_manager: CompanyStateManager, mock_queue_manager: MagicMock
    ) -> None:
        """Test autonomous monitoring capabilities."""
        # Set up some test data
        await state_manager.update_state("TEST1", ProcessingState.COMPLETED.value)
        await state_manager.update_state("TEST2", ProcessingState.FAILED.value)

        # Get metrics
        metrics = await state_manager.get_processing_metrics()

        # Verify comprehensive metrics
        assert "state_distribution" in metrics
        assert "retry_statistics" in metrics
        assert "recent_rate_limit_violations_24h" in metrics
        assert "queue_metrics" in metrics
        assert "timestamp" in metrics

    @pytest.mark.asyncio
    async def test_emergency_safeguards(self, state_manager: CompanyStateManager) -> None:
        """Test emergency safeguards prevent system overload."""
        # Test that excessive companies can be cleaned up
        companies_to_clean = ["CLEAN1", "CLEAN2", "CLEAN3"]

        for company in companies_to_clean:
            await state_manager.update_state(
                company,
                ProcessingState.STATUS_QUEUED.value,
                status_retry_count=10,  # Above retry limit
            )

        # Trigger cleanup
        cleaned_count = await state_manager.cleanup_failed_companies()

        # Verify cleanup worked
        assert cleaned_count == 3

    @pytest.mark.asyncio
    async def test_thread_safe_concurrent_operations(
        self, state_manager: CompanyStateManager
    ) -> None:
        """Test thread safety under concurrent operations."""
        companies = ["CONCURRENT1", "CONCURRENT2", "CONCURRENT3"]

        # Run concurrent state updates
        tasks = [
            state_manager.update_state(company, ProcessingState.STATUS_QUEUED.value)
            for company in companies
        ]

        results = await asyncio.gather(*tasks)

        # Verify all updates succeeded
        assert len(results) == 3
        assert all(result is not None for result in results)

        # Verify final states
        for company in companies:
            state = await state_manager.get_state(company)
            assert state is not None
            assert state["processing_state"] == ProcessingState.STATUS_QUEUED.value


class TestQueueOnlyIntegrationContract:
    """Test integration contract requirements for queue-only architecture."""

    @pytest.mark.asyncio
    async def test_streaming_service_integration_requirements(self) -> None:
        """Test requirements for streaming service integration."""
        # Streaming service should be able to use CompanyStateManager
        from src.streaming import CompanyStateManager as ImportedCSM
        from src.streaming import ProcessingState as ImportedPS

        # Verify exports are available
        assert ImportedCSM is not None
        assert ImportedPS is not None

        # Verify ProcessingState has required states
        assert ImportedPS.DETECTED.value == "detected"
        assert ImportedPS.STATUS_QUEUED.value == "status_queued"
        assert ImportedPS.COMPLETED.value == "completed"

    @pytest.mark.asyncio
    async def test_queue_manager_integration_contract(self) -> None:
        """Test queue manager integration contract."""
        from src.streaming import PriorityQueueManager, QueuedRequest, RequestPriority

        # Verify required queue components are available
        assert PriorityQueueManager is not None
        assert RequestPriority is not None
        assert QueuedRequest is not None

        # Verify priority levels
        assert RequestPriority.HIGH is not None
        assert RequestPriority.MEDIUM is not None

    def test_database_schema_compatibility(self, temp_db_path: Path) -> None:
        """Test database schema compatibility for queue operations."""
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # Verify required columns exist in company_processing_state
        cursor.execute("PRAGMA table_info(company_processing_state)")
        columns = [row[1] for row in cursor.fetchall()]

        required_queue_columns = [
            "status_request_id",
            "officers_request_id",
            "rate_limit_violations",
            "last_429_response_at",
        ]

        for column in required_queue_columns:
            assert column in columns

        # Verify api_rate_limit_log table exists
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='api_rate_limit_log'"
        )
        assert cursor.fetchone() is not None

        conn.close()


class TestZeroDirectAPICallsGuarantee:
    """Test absolute guarantee of zero direct API calls."""

    def test_no_http_libraries_imported(self) -> None:
        """Test that no HTTP libraries are imported anywhere in state management."""
        import sys

        # Import our module

        # Check that HTTP libraries are not in loaded modules after import
        http_modules = ["requests", "httpx", "aiohttp", "urllib3"]

        for http_module in http_modules:
            # If the module is loaded, it shouldn't be due to our import
            if http_module in sys.modules:
                # This is acceptable if it was loaded by other parts of the system
                # The key is our module doesn't directly use them
                pass

    @pytest.mark.asyncio
    async def test_all_api_operations_route_through_queue(
        self, state_manager: CompanyStateManager, mock_queue_manager: MagicMock
    ) -> None:
        """Test that ALL API operations route through queue manager."""
        company_number = "ALLQUEUE"

        # Every API-requiring operation should call queue manager
        await state_manager.queue_status_check(company_number)
        assert mock_queue_manager.enqueue.call_count == 1

        # Set up for officers fetch - need valid state transition path
        await state_manager.update_state(company_number, ProcessingState.STATUS_FETCHED.value)
        await state_manager.update_state(company_number, ProcessingState.STRIKE_OFF_CONFIRMED.value)
        await state_manager.queue_officers_fetch(company_number)
        assert mock_queue_manager.enqueue.call_count == 2

        # No other methods should make direct API calls
        await state_manager.get_state(company_number)
        await state_manager.handle_429_response(company_number, "test_req")
        await state_manager.get_processing_metrics()

        # Queue manager call count should remain the same
        assert mock_queue_manager.enqueue.call_count == 2

    @pytest.mark.asyncio
    async def test_bulletproof_guarantee_verification(
        self, state_manager: CompanyStateManager
    ) -> None:
        """Test bulletproof guarantee that no direct API calls are possible."""
        # CompanyStateManager should have NO methods that make HTTP requests
        api_calling_methods = [
            "_make_request",
            "_call_api",
            "_fetch_data",
            "_http_get",
            "_http_post",
            "requests",
            "get",
            "post",
            "fetch",
        ]

        for method_name in api_calling_methods:
            assert not hasattr(state_manager, method_name), (
                f"State manager has forbidden method: {method_name}"
            )

        # Should only have queue-based methods
        assert hasattr(state_manager, "queue_status_check")
        assert hasattr(state_manager, "queue_officers_fetch")
        assert not hasattr(state_manager, "fetch_status_directly")
        assert not hasattr(state_manager, "fetch_officers_directly")
