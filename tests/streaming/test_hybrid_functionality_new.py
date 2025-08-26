"""
Test the new queue-based hybrid streaming + REST API functionality.

Tests the real behavior of the hybrid system where streaming API provides
notifications and queued REST API requests provide detailed status information.
"""

import os
import tempfile
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from src.streaming.config import StreamingConfig
from src.streaming.database import StreamingDatabase
from src.streaming.migrations import DatabaseMigration


class TestHybridStreamingService:
    """Test the hybrid streaming service with queue-based architecture."""

    @pytest.fixture
    def temp_db_path(self) -> Any:
        """Create temporary database for testing."""
        with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as f:
            db_path = f.name
        yield db_path
        if os.path.exists(db_path):
            os.unlink(db_path)

    @pytest.fixture
    def streaming_config(self, temp_db_path: str) -> StreamingConfig:
        """Create test configuration."""
        return StreamingConfig(
            streaming_api_key="test-streaming-key-12345",
            rest_api_key="test-rest-api-key-67890",
            database_path=temp_db_path,
            log_level="DEBUG",
        )

    @pytest.fixture
    async def streaming_database(self, streaming_config: StreamingConfig) -> Any:
        """Create streaming database with real database."""
        migration = DatabaseMigration(streaming_config.database_path)
        migration.run_migrations()

        db = StreamingDatabase(streaming_config)
        await db.connect()
        yield db
        await db.disconnect()

    @patch("aiohttp.ClientSession.get")
    async def test_hybrid_strike_off_detection(
        self, mock_get: Any, streaming_database: Any
    ) -> None:
        """Test that hybrid system detects strike-off companies and queues officers request."""
        from streaming_service import StreamingService

        # Mock REST API response with detailed status
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={
                "company_number": "12345678",
                "company_name": "TEST COMPANY LIMITED",
                "company_status": "active",
                "company_status_detail": "Active - Proposal to Strike Off",
                "date_of_creation": "2020-01-15",
            }
        )
        mock_get.return_value.__aenter__.return_value = mock_response

        config = StreamingConfig(
            streaming_api_key="test-streaming-key-12345",
            rest_api_key="test-rest-api-key-67890",
            database_path=streaming_database.config.database_path,
        )
        service = StreamingService(config)

        # Initialize service components
        await service.initialize_components()

        # Create test streaming event (from Companies House streaming API)
        streaming_event = {
            "resource_kind": "company-profile",
            "resource_id": "12345678",
            "data": {"company_number": "12345678", "company_status": "active"},
        }

        # Process the event through the hybrid system
        await service._handle_company_event(streaming_event)

        # Verify REST API call was made to get detailed status
        mock_get.assert_called()

        # Verify company processing state was created
        async with streaming_database.manager.get_connection() as conn:
            cursor = await conn.execute(
                "SELECT processing_state, officers_queued_at FROM company_processing_state WHERE company_number = ?",
                ("12345678",),
            )
            row = await cursor.fetchone()
            assert row is not None, "Company processing state should be created"
            assert row[0] == "officers_queued", f"Expected 'officers_queued' state, got {row[0]}"
            assert row[1] is not None, "officers_queued_at should be set"

        # Verify officers request was queued
        assert service.queue_manager is not None
        queue_status = service.queue_manager.get_queue_status()
        assert queue_status["queues"]["HIGH"]["depth"] > 0, (
            "Strike-off request should be queued with high priority"
        )

    @patch("aiohttp.ClientSession.get")
    async def test_hybrid_regular_company_ignored(
        self, mock_get: Any, streaming_database: Any
    ) -> None:
        """Test that hybrid system ignores regular (non-strike-off) companies."""
        from streaming_service import StreamingService

        # Mock REST API response with regular active status
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={
                "company_number": "87654321",
                "company_name": "REGULAR COMPANY LIMITED",
                "company_status": "active",
                "company_status_detail": "Active",  # Not strike-off
                "date_of_creation": "2020-01-15",
            }
        )
        mock_get.return_value.__aenter__.return_value = mock_response

        config = StreamingConfig(
            streaming_api_key="test-streaming-key-12345",
            rest_api_key="test-rest-api-key-67890",
            database_path=streaming_database.config.database_path,
        )
        service = StreamingService(config)
        await service.initialize_components()

        # Create test streaming event
        streaming_event = {
            "resource_kind": "company-profile",
            "resource_id": "87654321",
            "data": {"company_number": "87654321", "company_status": "active"},
        }

        # Process the event
        await service._handle_company_event(streaming_event)

        # Verify REST API was called
        mock_get.assert_called()

        # Verify no company processing state was created (company ignored)
        async with streaming_database.manager.get_connection() as conn:
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM company_processing_state WHERE company_number = ?",
                ("87654321",),
            )
            count = await cursor.fetchone()
            assert count[0] == 0, "Regular company should not be tracked"

        # Verify no officers request was queued
        assert service.queue_manager is not None
        queue_status = service.queue_manager.get_queue_status()
        assert queue_status["total_queued"] == 0, "No requests should be queued for regular company"

    async def test_strike_off_status_detection(self, streaming_database: Any) -> None:
        """Test strike-off status detection logic."""
        from streaming_service import StreamingService

        config = StreamingConfig(
            streaming_api_key="test-streaming-key-12345",
            rest_api_key="test-rest-api-key-67890",
            database_path=streaming_database.config.database_path,
        )
        service = StreamingService(config)

        # Test various strike-off statuses
        strike_off_statuses = [
            "Active - Proposal to Strike Off",
            "Struck Off",
            "Strike Off Action",
            "active - proposal to strike off",  # Case insensitive
        ]

        for status in strike_off_statuses:
            assert service._is_strike_off_status(status), f"Should detect strike-off: {status}"

        # Test non-strike-off statuses
        regular_statuses = [
            "Active",
            "Dissolved",
            "Liquidation",
            "Administration",
            None,
            "",
        ]

        for status in regular_statuses:
            assert not service._is_strike_off_status(status), (
                f"Should not detect strike-off: {status}"
            )

    async def test_queue_manager_integration(self, streaming_database: Any) -> None:
        """Test that queue manager is properly integrated."""
        from streaming_service import StreamingService

        config = StreamingConfig(
            streaming_api_key="test-streaming-key-12345",
            rest_api_key="test-rest-api-key-67890",
            database_path=streaming_database.config.database_path,
        )
        service = StreamingService(config)
        await service.initialize_components()

        # Verify queue manager is initialized
        assert service.queue_manager is not None
        assert hasattr(service.queue_manager, "enqueue")
        assert hasattr(service.queue_manager, "get_queue_status")

        # Test direct queue interaction
        from src.streaming.queue_manager import QueuedRequest, RequestPriority

        request = QueuedRequest(
            request_id="test_officers_TEST123",
            priority=RequestPriority.HIGH,
            endpoint="/company/TEST123/officers",
            params={"company_number": "TEST123", "reason": "test"},
        )
        success = await service.queue_manager.enqueue(request)
        assert success, "Should successfully queue request"

        queue_status = service.queue_manager.get_queue_status()
        assert queue_status["queues"]["HIGH"]["depth"] == 1
        assert queue_status["total_queued"] == 1

    async def test_company_processing_state_creation(self, streaming_database: Any) -> None:
        """Test company processing state database integration."""
        from streaming_service import StreamingService

        config = StreamingConfig(
            streaming_api_key="test-streaming-key-12345",
            rest_api_key="test-rest-api-key-67890",
            database_path=streaming_database.config.database_path,
        )
        service = StreamingService(config)
        await service.initialize_components()

        # Test the _fetch_and_store_officers method creates correct state
        await service._fetch_and_store_officers("TEST456")

        # Verify state was created
        async with streaming_database.manager.get_connection() as conn:
            cursor = await conn.execute(
                "SELECT processing_state, officers_queued_at, created_at FROM company_processing_state WHERE company_number = ?",
                ("TEST456",),
            )
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] == "officers_queued"
            assert row[1] is not None  # officers_queued_at
            assert row[2] is not None  # created_at


class TestHybridWorkflow:
    """Test end-to-end hybrid workflow scenarios."""

    @pytest.fixture
    def temp_db_path(self) -> Any:
        """Create temporary database for testing."""
        with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as f:
            db_path = f.name
        yield db_path
        if os.path.exists(db_path):
            os.unlink(db_path)

    @pytest.fixture
    async def streaming_database(self, temp_db_path: str) -> Any:
        """Create streaming database with real database."""
        config = StreamingConfig(
            streaming_api_key="test-streaming-key-12345",
            rest_api_key="test-rest-api-key-67890",
            database_path=temp_db_path,
            log_level="DEBUG",
        )

        migration = DatabaseMigration(temp_db_path)
        migration.run_migrations()

        db = StreamingDatabase(config)
        await db.connect()
        yield db
        await db.disconnect()

    @patch("aiohttp.ClientSession.get")
    async def test_end_to_end_strike_off_workflow(
        self, mock_get: Any, streaming_database: Any
    ) -> None:
        """Test complete end-to-end workflow for strike-off detection."""
        from streaming_service import StreamingService

        # Mock REST API response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={
                "company_number": "WORKFLOW123",
                "company_name": "END TO END TEST LTD",
                "company_status": "active",
                "company_status_detail": "Active - Proposal to Strike Off",
                "date_of_creation": "2020-01-15",
            }
        )
        mock_get.return_value.__aenter__.return_value = mock_response

        config = StreamingConfig(
            streaming_api_key="test-streaming-key-12345",
            rest_api_key="test-rest-api-key-67890",
            database_path=streaming_database.config.database_path,
        )
        service = StreamingService(config)
        await service.initialize_components()

        # Simulate streaming event
        streaming_event = {
            "resource_kind": "company-profile",
            "resource_id": "WORKFLOW123",
            "data": {"company_number": "WORKFLOW123", "company_status": "active"},
        }

        # Process the event
        await service._handle_company_event(streaming_event)

        # Verify complete workflow
        async with streaming_database.manager.get_connection() as conn:
            # Check company processing state
            cursor = await conn.execute(
                "SELECT processing_state, officers_queued_at FROM company_processing_state WHERE company_number = ?",
                ("WORKFLOW123",),
            )
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] == "officers_queued"

        # Verify queue has the request
        queue_status = service.queue_manager.get_queue_status()
        assert queue_status["queues"]["HIGH"]["depth"] == 1
        assert queue_status["total_queued"] == 1

        # Verify stats updated
        assert service.stats["companies_processed"] >= 1, "At least one company should be processed"
