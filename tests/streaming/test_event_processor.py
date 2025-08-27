"""
Tests for streaming event processor functionality.
"""

from typing import Any
from unittest.mock import AsyncMock

import pytest

from src.streaming.config import StreamingConfig
from src.streaming.event_processor import CompanyEvent, EventProcessor, EventValidationError


# Shared fixtures for all test classes
@pytest.fixture
def config() -> Any:
    """Create test configuration."""
    return StreamingConfig(
        streaming_api_key="test-api-key-123456",
        rest_api_key="test-rest-api-key-123456",
        database_path=":memory:",
        batch_size=10,
    )


@pytest.fixture
def mock_db_connection() -> Any:
    """Create mock database connection."""
    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor


@pytest.fixture
def valid_company_event() -> Any:
    """Create valid company event data."""
    return {
        "resource_kind": "company-profile",
        "resource_uri": "/company/12345678",
        "resource_id": "12345678",
        "data": {
            "company_number": "12345678",
            "company_name": "Test Company Ltd",
            "company_status": "active-proposal-to-strike-off",
            "company_status_detail": "Active - Proposal to Strike Off",
            "date_of_creation": "2020-01-15",
            "registered_office_address": {
                "address_line_1": "123 Test Street",
                "locality": "Test City",
                "postal_code": "TE1 2ST",
                "country": "England",
            },
            "sic_codes": ["62090"],
        },
        "event": {"timepoint": 12345, "published_at": "2025-08-15T12:30:00Z"},
    }


@pytest.fixture
def valid_officers_event() -> Any:
    """Create valid officers event data."""
    return {
        "resource_kind": "company-officers",
        "resource_uri": "/company/12345678/officers",
        "resource_id": "12345678",
        "data": {
            "company_number": "12345678",
            "officers": [
                {
                    "name": "John Smith",
                    "officer_role": "director",
                    "appointed_on": "2020-01-15",
                    "address": {
                        "address_line_1": "456 Director Street",
                        "locality": "Director City",
                        "postal_code": "DI1 2CT",
                        "country": "England",
                    },
                }
            ],
        },
        "event": {"timepoint": 12346, "published_at": "2025-08-15T12:31:00Z"},
    }


class TestEventProcessor:
    """Test EventProcessor class functionality."""

    def test_event_processor_initialization(self, config: Any) -> None:
        """Test that EventProcessor initializes correctly."""
        processor = EventProcessor(config)

        assert processor.config == config
        assert processor.batch_size == config.batch_size
        assert processor.processed_events == 0
        assert processor.failed_events == 0
        assert processor.event_handlers == {}
        assert processor._shutdown_event is not None

    def test_register_event_handler(self, config: Any) -> None:
        """Test event handler registration."""
        processor = EventProcessor(config)
        mock_handler = AsyncMock()

        processor.register_event_handler("company-profile", mock_handler)

        assert "company-profile" in processor.event_handlers
        assert processor.event_handlers["company-profile"] == mock_handler

    def test_register_multiple_handlers(self, config: Any) -> None:
        """Test registering multiple event handlers."""
        processor = EventProcessor(config)
        company_handler = AsyncMock()
        officers_handler = AsyncMock()

        processor.register_event_handler("company-profile", company_handler)
        processor.register_event_handler("company-officers", officers_handler)

        assert len(processor.event_handlers) == 2
        assert processor.event_handlers["company-profile"] == company_handler
        assert processor.event_handlers["company-officers"] == officers_handler

    @pytest.mark.asyncio
    async def test_process_single_event_success(
        self, config: Any, valid_company_event: Any
    ) -> None:
        """Test processing a single valid event."""
        processor = EventProcessor(config)
        mock_handler = AsyncMock()
        processor.register_event_handler("company-profile", mock_handler)

        result = await processor.process_event(valid_company_event)

        assert result is True
        assert processor.processed_events == 1
        assert processor.failed_events == 0
        mock_handler.assert_called_once_with(valid_company_event)

    @pytest.mark.asyncio
    async def test_process_event_no_handler(self, config: Any, valid_company_event: Any) -> None:
        """Test processing event with no registered handler."""
        processor = EventProcessor(config)

        # Should still process but with default handling
        result = await processor.process_event(valid_company_event)

        assert result is True
        assert processor.processed_events == 1
        assert processor.failed_events == 0

    @pytest.mark.asyncio
    async def test_process_event_handler_exception(
        self, config: Any, valid_company_event: Any
    ) -> None:
        """Test handling of exceptions in event handlers."""
        processor = EventProcessor(config)
        mock_handler = AsyncMock(side_effect=Exception("Handler error"))
        processor.register_event_handler("company-profile", mock_handler)

        result = await processor.process_event(valid_company_event)

        assert result is False
        assert processor.processed_events == 0
        assert processor.failed_events == 1

    @pytest.mark.asyncio
    async def test_process_batch_events(
        self, config: Any, valid_company_event: Any, valid_officers_event: Any
    ) -> None:
        """Test processing a batch of events."""
        processor = EventProcessor(config)
        company_handler = AsyncMock()
        officers_handler = AsyncMock()

        processor.register_event_handler("company-profile", company_handler)
        processor.register_event_handler("company-officers", officers_handler)

        events = [valid_company_event, valid_officers_event]
        results = await processor.process_batch(events)

        assert len(results) == 2
        assert all(results)
        assert processor.processed_events == 2
        assert processor.failed_events == 0

        company_handler.assert_called_once_with(valid_company_event)
        officers_handler.assert_called_once_with(valid_officers_event)

    @pytest.mark.asyncio
    async def test_process_batch_with_failures(self, config: Any, valid_company_event: Any) -> None:
        """Test batch processing with some failures."""
        processor = EventProcessor(config)
        mock_handler = AsyncMock(side_effect=[None, Exception("Error"), None])
        processor.register_event_handler("company-profile", mock_handler)

        # Create batch with 3 events
        events = [valid_company_event, valid_company_event.copy(), valid_company_event.copy()]
        results = await processor.process_batch(events)

        assert len(results) == 3
        assert results == [True, False, True]  # Second event should fail
        assert processor.processed_events == 2
        assert processor.failed_events == 1

    def test_get_processing_stats(self, config: Any) -> None:
        """Test getting processing statistics."""
        processor = EventProcessor(config)
        processor.processed_events = 100
        processor.failed_events = 5

        stats = processor.get_processing_stats()

        assert stats is not None and stats["processed_events"] == 100
        assert stats is not None and stats["failed_events"] == 5
        assert abs(stats["success_rate"] - 95.0) < 0.5  # Allow for floating point precision
        assert "uptime_seconds" in stats

    def test_reset_stats(self, config: Any) -> None:
        """Test resetting processing statistics."""
        processor = EventProcessor(config)
        processor.processed_events = 100
        processor.failed_events = 5

        processor.reset_stats()

        assert processor.processed_events == 0
        assert processor.failed_events == 0

    @pytest.mark.asyncio
    async def test_shutdown(self, config: Any) -> None:
        """Test graceful shutdown."""
        processor = EventProcessor(config)

        await processor.shutdown()

        assert processor._shutdown_event.is_set()

    def test_is_shutdown(self, config: Any) -> None:
        """Test shutdown status check."""
        processor = EventProcessor(config)

        assert not processor.is_shutdown()

        processor._shutdown_event.set()
        assert processor.is_shutdown()


class TestCompanyEvent:
    """Test CompanyEvent data model."""

    def test_company_event_from_dict(self, valid_company_event: Any) -> None:
        """Test creating CompanyEvent from dictionary."""
        event = CompanyEvent.from_dict(valid_company_event)

        assert event.resource_kind == "company-profile"
        assert event.resource_id == "12345678"
        assert event.company_number == "12345678"
        assert event.company_name == "Test Company Ltd"
        assert event.company_status == "Active - Proposal to Strike Off"
        assert event.timepoint == 12345

    def test_company_event_is_strike_off(self, valid_company_event: Any) -> None:
        """Test strike-off status detection."""
        event = CompanyEvent.from_dict(valid_company_event)

        assert event.is_strike_off() is True

    def test_company_event_not_strike_off(self, valid_company_event: Any) -> None:
        """Test non-strike-off status detection."""
        valid_company_event["data"]["company_status_detail"] = "Active"
        event = CompanyEvent.from_dict(valid_company_event)

        assert event.is_strike_off() is False

    def test_company_event_to_dict(self, valid_company_event: Any) -> None:
        """Test converting CompanyEvent back to dictionary."""
        event = CompanyEvent.from_dict(valid_company_event)
        result_dict = event.to_dict()

        assert result_dict is not None and result_dict["company_number"] == "12345678"
        assert result_dict is not None and result_dict["company_name"] == "Test Company Ltd"
        assert (
            result_dict is not None
            and result_dict["company_status"] == "Active - Proposal to Strike Off"
        )
        assert result_dict is not None and result_dict["timepoint"] == 12345


class TestEventValidation:
    """Test event validation functionality."""

    def test_validate_valid_event(self, config: Any, valid_company_event: Any) -> None:
        """Test validation of valid event."""
        processor = EventProcessor(config)

        # Should not raise exception
        processor._validate_event(valid_company_event)

    def test_validate_missing_resource_kind(self, config: Any, valid_company_event: Any) -> None:
        """Test validation fails for missing resource_kind."""
        processor = EventProcessor(config)
        del valid_company_event["resource_kind"]

        with pytest.raises(EventValidationError, match="Missing required field: resource_kind"):
            processor._validate_event(valid_company_event)

    def test_validate_missing_data(self, config: Any, valid_company_event: Any) -> None:
        """Test validation fails for missing data section."""
        processor = EventProcessor(config)
        del valid_company_event["data"]

        with pytest.raises(EventValidationError, match="Missing required field: data"):
            processor._validate_event(valid_company_event)

    def test_validate_empty_data(self, config: Any, valid_company_event: Any) -> None:
        """Test validation fails for empty data section."""
        processor = EventProcessor(config)
        valid_company_event["data"] = {}

        with pytest.raises(EventValidationError, match="Event data is empty"):
            processor._validate_event(valid_company_event)

    def test_validate_invalid_resource_kind(self, config: Any, valid_company_event: Any) -> None:
        """Test validation handles invalid resource kinds."""
        processor = EventProcessor(config)
        valid_company_event["resource_kind"] = "invalid-resource"

        # Should not raise exception for unknown resource kinds
        processor._validate_event(valid_company_event)

    def test_validate_missing_company_number(self, config: Any, valid_company_event: Any) -> None:
        """Test validation fails for missing company number in company events."""
        processor = EventProcessor(config)
        del valid_company_event["data"]["company_number"]

        with pytest.raises(EventValidationError, match="Missing company_number in company event"):
            processor._validate_event(valid_company_event)


class TestCompanyDataExtraction:
    """Test company data extraction functionality."""

    def test_extract_company_data_complete(self, config: Any, valid_company_event: Any) -> None:
        """Test extraction of complete company data."""
        processor = EventProcessor(config)

        company_data = processor._extract_company_data(valid_company_event)

        assert company_data is not None and company_data["company_number"] == "12345678"
        assert company_data is not None and company_data["company_name"] == "Test Company Ltd"
        assert (
            company_data is not None
            and company_data["company_status"] == "active-proposal-to-strike-off"
        )
        assert (
            company_data is not None
            and company_data["company_status_detail"] == "Active - Proposal to Strike Off"
        )
        assert company_data is not None and company_data["incorporation_date"] == "2020-01-15"
        assert company_data is not None and company_data["sic_codes"] == "62090"
        assert company_data is not None and company_data["address_line_1"] == "123 Test Street"
        assert company_data is not None and company_data["locality"] == "Test City"
        assert company_data is not None and company_data["postal_code"] == "TE1 2ST"
        assert company_data is not None and company_data["country"] == "England"

    def test_extract_company_data_minimal(self, config: Any) -> None:
        """Test extraction with minimal company data."""
        processor = EventProcessor(config)
        minimal_event = {
            "resource_kind": "company-profile",
            "data": {"company_number": "87654321", "company_name": "Minimal Company"},
        }

        company_data = processor._extract_company_data(minimal_event)

        assert company_data is not None and company_data["company_number"] == "87654321"
        assert company_data is not None and company_data["company_name"] == "Minimal Company"
        assert company_data is not None and company_data["company_status"] is None
        assert company_data is not None and company_data["sic_codes"] is None

    def test_extract_company_data_multiple_sic_codes(
        self, config: Any, valid_company_event: Any
    ) -> None:
        """Test extraction with multiple SIC codes."""
        processor = EventProcessor(config)
        valid_company_event["data"]["sic_codes"] = ["62090", "62020", "62012"]

        company_data = processor._extract_company_data(valid_company_event)

        assert company_data is not None and company_data["sic_codes"] == "62090,62020,62012"

    def test_extract_company_data_no_address(self, config: Any, valid_company_event: Any) -> None:
        """Test extraction when address is missing."""
        processor = EventProcessor(config)
        del valid_company_event["data"]["registered_office_address"]

        company_data = processor._extract_company_data(valid_company_event)

        assert company_data is not None and company_data["company_number"] == "12345678"
        assert company_data is not None and company_data["address_line_1"] is None
        assert company_data is not None and company_data["locality"] is None
        assert company_data is not None and company_data["postal_code"] is None
