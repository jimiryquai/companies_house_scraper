"""
Test the new hybrid streaming + REST API functionality.

Tests the real behavior of the hybrid system where streaming API provides
notifications and REST API provides detailed status information.
"""

import asyncio
import os
import sqlite3
import tempfile
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from src.streaming.config import StreamingConfig
from src.streaming.database import StreamingDatabase
from src.streaming.migrations import DatabaseMigration


class TestHybridStreamingService:
    """Test the hybrid streaming service that combines streaming + REST API."""

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
            streaming_api_key="test-streaming-key",
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

    @pytest.fixture
    def mock_officer_config(self) -> dict[str, Any]:
        """Mock officer import configuration."""
        return {
            "api": {"key": "test-rest-api-key"},
            "api_endpoints": {"base_url": "https://api.company-information.service.gov.uk"},
        }

    def test_streaming_event_structure(self) -> None:
        """Test that we understand the actual streaming event structure."""
        # Real streaming event (basic status only)
        streaming_event = {
            "resource_kind": "company-profile",
            "resource_uri": "/company/OC453089",
            "resource_id": "OC453089",
            "data": {
                "company_name": "TEST COMPANY LLP",
                "company_number": "OC453089",
                "company_status": "active",  # Only basic status available
                "date_of_creation": "2024-07-18",
                "registered_office_address": {
                    "address_line_1": "Test Street",
                    "locality": "London",
                    "postal_code": "SW1A 1AA",
                },
            },
            "event": {
                "timepoint": 100096009,
                "published_at": "2025-08-16T10:54:03",
                "type": "changed",
            },
        }

        # Verify structure
        assert streaming_event["resource_kind"] == "company-profile"
        data = streaming_event["data"]
        assert isinstance(data, dict)
        assert data["company_status"] == "active"
        assert "company_status_detail" not in data

    @patch("aiohttp.ClientSession.get")
    async def test_hybrid_strike_off_detection(
        self, mock_get: Any, streaming_database: Any
    ) -> None:
        """Test that hybrid system detects strike-off companies correctly."""
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

        # Mock officer import functions
        with (
            patch("streaming_service.load_config") as mock_load_config,
            patch("streaming_service.get_company_officers") as mock_get_officers,
            patch("streaming_service.save_officers_to_db") as mock_save_officers,
        ):
            mock_load_config.return_value = {
                "api": {"key": "test-rest-key"},
                "api_endpoints": {"base_url": "https://api.company-information.service.gov.uk"},
            }
            mock_get_officers.return_value = [{"name": "John Doe", "officer_role": "director"}]
            mock_save_officers.return_value = 1

            # Create streaming service
            config = StreamingConfig(
                streaming_api_key="test-key", database_path=streaming_database.config.database_path
            )
            service = StreamingService(config)
            await service.initialize_components()

            # Process a company event (streaming API provides basic status)
            await service._process_company_with_rest_api("12345678")

            # Verify REST API was called
            mock_get.assert_called_once()

            # Verify officer import was triggered for strike-off company
            mock_get_officers.assert_called_once_with("12345678", "test-rest-key")
            mock_save_officers.assert_called_once()

    @patch("aiohttp.ClientSession.get")
    async def test_hybrid_cleanup_detection(self, mock_get: Any, streaming_database: Any) -> None:
        """Test that hybrid system detects companies leaving strike-off status."""
        from streaming_service import StreamingService

        # First, add a company to database (simulating it was previously in strike-off)
        company_data = {
            "company_number": "87654321",
            "company_name": "LEAVING STRIKE OFF LIMITED",
            "company_status_detail": "Active - Proposal to Strike Off",
            "stream_last_updated": datetime.now().isoformat(),
        }
        await streaming_database.upsert_company(company_data)

        # Mock REST API response showing company is now just "active"
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={
                "company_number": "87654321",
                "company_name": "LEAVING STRIKE OFF LIMITED",
                "company_status": "active",
                # No company_status_detail or it doesn't contain "proposal to strike off"
            }
        )
        mock_get.return_value.__aenter__.return_value = mock_response

        with patch("streaming_service.load_config") as mock_load_config:
            mock_load_config.return_value = {
                "api": {"key": "test-rest-key"},
                "api_endpoints": {"base_url": "https://api.company-information.service.gov.uk"},
            }

            # Create streaming service
            config = StreamingConfig(
                streaming_api_key="test-key", database_path=streaming_database.config.database_path
            )
            service = StreamingService(config)
            await service.initialize_components()

            # Process the company event
            await service._process_company_with_rest_api("87654321")

            # Verify company was marked as leaving strike-off
            conn = sqlite3.connect(streaming_database.config.database_path)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT company_status_detail FROM companies WHERE company_number = ?",
                ("87654321",),
            )
            result = cursor.fetchone()
            assert result[0] == "Left strike-off status"
            conn.close()

    @patch("aiohttp.ClientSession.get")
    async def test_hybrid_regular_company_ignored(
        self, mock_get: Any, streaming_database: Any
    ) -> None:
        """Test that regular companies not in database are ignored."""
        from streaming_service import StreamingService

        # Mock REST API response for regular company
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={
                "company_number": "99999999",
                "company_name": "REGULAR COMPANY LIMITED",
                "company_status": "active",
                # No strike-off status
            }
        )
        mock_get.return_value.__aenter__.return_value = mock_response

        with patch("streaming_service.load_config") as mock_load_config:
            mock_load_config.return_value = {
                "api": {"key": "test-rest-key"},
                "api_endpoints": {"base_url": "https://api.company-information.service.gov.uk"},
            }

            # Create streaming service
            config = StreamingConfig(
                streaming_api_key="test-key", database_path=streaming_database.config.database_path
            )
            service = StreamingService(config)
            await service.initialize_components()

            # Process the company event
            await service._process_company_with_rest_api("99999999")

            # Verify company was NOT added to database
            conn = sqlite3.connect(streaming_database.config.database_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM companies WHERE company_number = ?", ("99999999",))
            count = cursor.fetchone()[0]
            assert count == 0, "Regular company should not be added to database"
            conn.close()

    def test_strike_off_status_detection(self) -> None:
        """Test the strike-off status detection logic."""
        from streaming_service import StreamingService

        config = StreamingConfig(streaming_api_key="test")
        service = StreamingService(config)

        # Test various strike-off status formats
        assert service._is_strike_off_status("Active - Proposal to Strike Off")
        assert service._is_strike_off_status("active - proposal to strike off")
        assert service._is_strike_off_status("ACTIVE - PROPOSAL TO STRIKE OFF")
        assert service._is_strike_off_status("Struck off")
        assert service._is_strike_off_status("struck off")

        # Test non-strike-off statuses
        assert not service._is_strike_off_status("active")
        assert not service._is_strike_off_status("dissolved")
        assert not service._is_strike_off_status("liquidation")
        assert not service._is_strike_off_status("")
        assert not service._is_strike_off_status(None)

    @patch("aiohttp.ClientSession.get")
    async def test_rest_api_error_handling(self, mock_get: Any, streaming_database: Any) -> None:
        """Test handling of REST API errors."""
        from streaming_service import StreamingService

        # Mock REST API failure
        mock_response = AsyncMock()
        mock_response.status = 404
        mock_get.return_value.__aenter__.return_value = mock_response

        with patch("streaming_service.load_config") as mock_load_config:
            mock_load_config.return_value = {
                "api": {"key": "test-rest-key"},
                "api_endpoints": {"base_url": "https://api.company-information.service.gov.uk"},
            }

            config = StreamingConfig(
                streaming_api_key="test-key", database_path=streaming_database.config.database_path
            )
            service = StreamingService(config)
            await service.initialize_components()

            # Process should handle 404 gracefully
            await service._process_company_with_rest_api("00000000")

            # Should increment error stats
            assert service.stats["errors"] == 0  # 404 is not treated as error

    @patch("aiohttp.ClientSession.get")
    async def test_rate_limiting_handling(self, mock_get: Any, streaming_database: Any) -> None:
        """Test handling of rate limiting."""
        from streaming_service import StreamingService

        # Mock rate limiting response
        mock_response = AsyncMock()
        mock_response.status = 429
        mock_get.return_value.__aenter__.return_value = mock_response

        with patch("streaming_service.load_config") as mock_load_config:
            mock_load_config.return_value = {
                "api": {"key": "test-rest-key"},
                "api_endpoints": {"base_url": "https://api.company-information.service.gov.uk"},
            }

            config = StreamingConfig(
                streaming_api_key="test-key", database_path=streaming_database.config.database_path
            )
            service = StreamingService(config)
            await service.initialize_components()

            # Process should handle rate limiting gracefully
            await service._process_company_with_rest_api("12345678")

            # Should not crash or increment error count
            assert service.stats["errors"] == 0

    async def test_database_cleanup_logic(self, streaming_database: Any) -> None:
        """Test the database cleanup logic works correctly."""
        from streaming_service import StreamingService

        # Add test company to database
        company_data = {
            "company_number": "11111111",
            "company_name": "TEST CLEANUP LIMITED",
            "company_status_detail": "Active - Proposal to Strike Off",
        }
        await streaming_database.upsert_company(company_data)

        config = StreamingConfig(
            streaming_api_key="test-key", database_path=streaming_database.config.database_path
        )
        service = StreamingService(config)
        await service.initialize_components()

        # Test cleanup
        await service._cleanup_company_records("11111111")

        # Verify cleanup happened
        conn = sqlite3.connect(streaming_database.config.database_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT company_status_detail FROM companies WHERE company_number = ?", ("11111111",)
        )
        result = cursor.fetchone()
        assert result[0] == "Left strike-off status"
        conn.close()


class TestHybridWorkflow:
    """Test complete hybrid workflow scenarios."""

    @pytest.fixture
    def temp_db_path(self) -> Any:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as f:
            db_path = f.name
        yield db_path
        if os.path.exists(db_path):
            os.unlink(db_path)

    @patch("aiohttp.ClientSession.get")
    async def test_end_to_end_strike_off_workflow(self, mock_get: Any, temp_db_path: str) -> None:
        """Test complete workflow: streaming event -> REST API -> database update."""
        from streaming_service import StreamingService

        # Setup database
        migration = DatabaseMigration(temp_db_path)
        migration.run_migrations()

        # Mock REST API response for strike-off company
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={
                "company_number": "12345678",
                "company_name": "NEW STRIKE OFF LIMITED",
                "company_status": "active",
                "company_status_detail": "Active - Proposal to Strike Off",
            }
        )
        mock_get.return_value.__aenter__.return_value = mock_response

        with (
            patch("streaming_service.load_config") as mock_load_config,
            patch("streaming_service.get_company_officers") as mock_get_officers,
            patch("streaming_service.save_officers_to_db") as mock_save_officers,
        ):
            mock_load_config.return_value = {
                "api": {"key": "test-rest-key"},
                "api_endpoints": {"base_url": "https://api.company-information.service.gov.uk"},
            }
            mock_get_officers.return_value = [
                {"name": "Director One", "officer_role": "director"},
                {"name": "Director Two", "officer_role": "director"},
            ]
            mock_save_officers.return_value = 2

            # Create service and process event
            config = StreamingConfig(streaming_api_key="test-key", database_path=temp_db_path)
            service = StreamingService(config)
            await service.initialize_components()

            # Simulate streaming event triggering REST API call
            streaming_event = {
                "resource_kind": "company-profile",
                "resource_id": "12345678",
                "data": {
                    "company_number": "12345678",
                    "company_status": "active",  # Basic status only
                },
            }

            # Process the event (this triggers the hybrid workflow)
            await service._handle_company_event(streaming_event)

            # Verify complete workflow
            assert mock_get.called, "REST API should be called"
            assert mock_get_officers.called, "Officers should be fetched"
            assert mock_save_officers.called, "Officers should be saved"

            # Verify statistics
            assert service.stats["companies_processed"] == 1
            assert service.stats.get("officers_imported", 0) == 2

    async def test_performance_with_many_events(self, temp_db_path: str) -> None:
        """Test performance of hybrid approach with many events."""
        from streaming_service import StreamingService

        # Setup
        migration = DatabaseMigration(temp_db_path)
        migration.run_migrations()

        with (
            patch("aiohttp.ClientSession.get") as mock_get,
            patch("streaming_service.load_config") as mock_load_config,
        ):
            # Mock regular companies (not strike-off)
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(
                return_value={
                    "company_status": "active",
                    # No strike-off status
                }
            )
            mock_get.return_value.__aenter__.return_value = mock_response

            mock_load_config.return_value = {
                "api": {"key": "test-rest-key"},
                "api_endpoints": {"base_url": "https://api.company-information.service.gov.uk"},
            }

            config = StreamingConfig(streaming_api_key="test-key", database_path=temp_db_path)
            service = StreamingService(config)
            await service.initialize_components()

            # Process 50 company events
            start_time = datetime.now()

            tasks = []
            for i in range(50):
                company_number = f"1234567{i:02d}"
                task = service._process_company_with_rest_api(company_number)
                tasks.append(task)

            await asyncio.gather(*tasks)

            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()

            # Should complete in reasonable time (under 10 seconds)
            assert processing_time < 10.0, f"Processing 50 events took {processing_time}s"

            # Verify all events were processed
            assert service.stats["companies_processed"] == 50
