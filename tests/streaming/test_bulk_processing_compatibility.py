"""
Tests for streaming and bulk processing compatibility.
Verifies that streaming updates work correctly alongside existing bulk data operations.
"""

import json
import os
import sqlite3
import tempfile
from datetime import datetime, timedelta
from typing import Any

import pytest

from src.streaming import (
    CompanyEvent,
    CompanyRecord,
    EventProcessor,
    EventTracker,
    StreamingConfig,
    StreamingDatabase,
)


@pytest.fixture
def temp_db() -> Any:
    """Create a temporary database with bulk data for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
        temp_path = temp_file.name

    # Initialize database schema with bulk data
    with sqlite3.connect(temp_path) as conn:
        # Create companies table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                company_number TEXT PRIMARY KEY,
                company_name TEXT,
                company_status TEXT,
                company_status_detail TEXT,
                incorporation_date TEXT,
                sic_codes TEXT,
                address_line_1 TEXT,
                address_line_2 TEXT,
                locality TEXT,
                region TEXT,
                country TEXT,
                postal_code TEXT,
                premises TEXT,
                stream_last_updated TEXT,
                stream_status TEXT DEFAULT 'unknown',
                data_source TEXT DEFAULT 'bulk',
                last_stream_event_id TEXT,
                stream_metadata TEXT
            )
        """)

        # Create stream events table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stream_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE,
                event_type TEXT NOT NULL,
                company_number TEXT,
                event_data TEXT,
                processed_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                processing_status TEXT DEFAULT 'pending',
                retry_count INTEGER DEFAULT 0,
                error_message TEXT,
                FOREIGN KEY (company_number) REFERENCES companies(company_number)
            )
        """)

        # Insert sample bulk data
        bulk_companies = [
            (
                "BLK001",
                "Bulk Company One Ltd",
                "active",
                None,
                "2020-01-01",
                '[{"code": "62090", "description": "Other information technology services"}]',
                "123 Business St",
                None,
                "London",
                "Greater London",
                "England",
                "SW1A 1AA",
                None,
                None,
                "unknown",
                "bulk",
                None,
                None,
            ),
            (
                "BLK002",
                "Bulk Company Two Ltd",
                "active",
                "proposal-to-strike-off",
                "2019-06-15",
                '[{"code": "70100", "description": "Activities of head offices"}]',
                "456 Commerce Ave",
                "Suite 100",
                "Manchester",
                "Greater Manchester",
                "England",
                "M1 1AA",
                None,
                None,
                "unknown",
                "bulk",
                None,
                None,
            ),
            (
                "BLK003",
                "Bulk Company Three Ltd",
                "liquidation",
                None,
                "2018-12-01",
                '[{"code": "82990", "description": "Other business support service activities"}]',
                "789 Industry Rd",
                None,
                "Birmingham",
                "West Midlands",
                "England",
                "B1 1AA",
                None,
                None,
                "unknown",
                "bulk",
                None,
                None,
            ),
        ]

        conn.executemany(
            """
            INSERT INTO companies (
                company_number, company_name, company_status, company_status_detail,
                incorporation_date, sic_codes, address_line_1, address_line_2,
                locality, region, country, postal_code, premises,
                stream_last_updated, stream_status, data_source,
                last_stream_event_id, stream_metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            bulk_companies,
        )

        conn.commit()

    yield temp_path

    # Cleanup
    os.unlink(temp_path)


@pytest.fixture
def config(temp_db: Any) -> Any:
    """Create test configuration with populated database."""
    return StreamingConfig(
        streaming_api_key="12345678-1234-1234-1234-123456789012",
        database_path=temp_db,
        batch_size=10,
    )


@pytest.fixture
def stream_events() -> Any:
    """Sample streaming events that correspond to existing bulk companies."""
    return [
        {
            "resource_kind": "company-profile",
            "resource_id": "BLK001",
            "data": {
                "company_number": "BLK001",
                "company_name": "Bulk Company One Ltd - Updated",  # Updated name
                "company_status": "active",
                "company_status_detail": None,
                "sic_codes": [
                    {
                        "code": "62090",
                        "description": "Other information technology service activities",
                    },
                    {
                        "code": "62020",
                        "description": "Information technology consultancy activities",
                    },  # Added SIC
                ],
            },
            "event": {"timepoint": 5000, "published_at": "2025-08-15T14:00:00Z"},
        },
        {
            "resource_kind": "company-profile",
            "resource_id": "BLK002",
            "data": {
                "company_number": "BLK002",
                "company_name": "Bulk Company Two Ltd",
                "company_status": "active",
                "company_status_detail": None,  # Strike-off status removed
                "address": {
                    "address_line_1": "456 Commerce Ave - Updated",  # Updated address
                    "address_line_2": "Suite 200",  # Updated suite
                    "locality": "Manchester",
                    "region": "Greater Manchester",
                    "country": "England",
                    "postal_code": "M1 1BB",  # Updated postcode
                },
            },
            "event": {"timepoint": 5001, "published_at": "2025-08-15T14:01:00Z"},
        },
        {
            "resource_kind": "company-profile",
            "resource_id": "STR001",  # New company not in bulk data
            "data": {
                "company_number": "STR001",
                "company_name": "Stream Only Company Ltd",
                "company_status": "active",
                "company_status_detail": "proposal-to-strike-off",
                "incorporation_date": "2025-01-01",
                "address": {
                    "address_line_1": "999 Stream St",
                    "locality": "Leeds",
                    "region": "West Yorkshire",
                    "country": "England",
                    "postal_code": "LS1 1AA",
                },
            },
            "event": {"timepoint": 5002, "published_at": "2025-08-15T14:02:00Z"},
        },
    ]


class TestBulkStreamingCompatibility:
    """Test compatibility between bulk and streaming data operations."""

    @pytest.mark.asyncio
    async def test_bulk_data_preservation_during_streaming_updates(
        self, config: Any, stream_events: Any
    ) -> None:
        """Test that bulk data is preserved when streaming updates are applied."""
        database = StreamingDatabase(config)
        await database.connect()

        try:
            # Verify initial bulk data exists
            bulk_companies = await database.manager.fetch_all("SELECT * FROM companies", ())
            assert len(bulk_companies) == 3

            # Check specific bulk company before update
            blk001_before = await database.get_company("BLK001")
            assert blk001_before is not None
            assert (
                blk001_before is not None
                and blk001_before["company_name"] == "Bulk Company One Ltd"
            )
            assert blk001_before is not None and blk001_before["data_source"] == "bulk"

            # Apply streaming update to existing bulk company
            stream_event = stream_events[0]  # BLK001 update
            company_event = CompanyEvent.from_dict(stream_event)

            update_data = {
                "company_number": company_event.company_number,
                "company_name": company_event.company_name,
                "company_status": company_event.company_status,
                "stream_last_updated": datetime.now().isoformat(),
                "data_source": "stream",
                "last_stream_event_id": "stream_evt_001",
            }

            await database.upsert_company(update_data)

            # Verify the update was applied
            blk001_after = await database.get_company("BLK001")
            assert blk001_after is not None
            assert (
                blk001_after is not None
                and blk001_after["company_name"] == "Bulk Company One Ltd - Updated"
            )
            assert blk001_after is not None and blk001_after["data_source"] == "stream"
            assert (
                blk001_after is not None
                and blk001_after["last_stream_event_id"] == "stream_evt_001"
            )

            # Verify bulk fields are preserved
            assert blk001_after is not None and blk001_after["incorporation_date"] == "2020-01-01"
            assert blk001_after is not None and blk001_after["address_line_1"] == "123 Business St"
            assert blk001_after is not None and blk001_after["locality"] == "London"

            # Verify other bulk companies are unchanged
            blk002 = await database.get_company("BLK002")
            assert blk002 is not None and blk002["data_source"] == "bulk"
            assert blk002 is not None and blk002["company_name"] == "Bulk Company Two Ltd"

        finally:
            await database.disconnect()

    @pytest.mark.asyncio
    async def test_streaming_updates_strike_off_status(
        self, config: Any, stream_events: Any
    ) -> None:
        """Test streaming updates to strike-off status on bulk companies."""
        database = StreamingDatabase(config)
        await database.connect()

        try:
            # Check initial strike-off company
            blk002_before = await database.get_company("BLK002")
            assert (
                blk002_before is not None
                and blk002_before["company_status_detail"] == "proposal-to-strike-off"
            )

            # Apply streaming update that removes strike-off status
            stream_event = stream_events[1]  # BLK002 update
            company_event = CompanyEvent.from_dict(stream_event)

            update_data = {
                "company_number": company_event.company_number,
                "company_name": company_event.company_name,
                "company_status": company_event.company_status,
                "company_status_detail": stream_event["data"].get("company_status_detail"),
                "stream_last_updated": datetime.now().isoformat(),
                "data_source": "stream",
                "last_stream_event_id": "stream_evt_002",
            }

            await database.upsert_company(update_data)

            # Verify strike-off status was removed
            blk002_after = await database.get_company("BLK002")
            assert blk002_after is not None and blk002_after["company_status_detail"] is None
            assert blk002_after is not None and blk002_after["data_source"] == "stream"

            # Verify we can still query for strike-off companies from bulk data
            query = "SELECT * FROM companies WHERE company_status_detail = ?"
            strike_off_companies = await database.manager.fetch_all(
                query, ("proposal-to-strike-off",)
            )
            assert len(strike_off_companies) == 0  # BLK002 should no longer be strike-off

        finally:
            await database.disconnect()

    @pytest.mark.asyncio
    async def test_new_companies_via_streaming(self, config: Any, stream_events: Any) -> None:
        """Test adding completely new companies via streaming."""
        database = StreamingDatabase(config)
        await database.connect()

        try:
            # Verify initial count
            initial_companies = await database.manager.fetch_all("SELECT * FROM companies", ())
            assert len(initial_companies) == 3

            # Add new company via streaming
            stream_event = stream_events[2]  # STR001 - new company
            company_event = CompanyEvent.from_dict(stream_event)

            new_company_data = {
                "company_number": company_event.company_number,
                "company_name": company_event.company_name,
                "company_status": company_event.company_status,
                "company_status_detail": stream_event["data"].get("company_status_detail"),
                "incorporation_date": stream_event["data"].get("incorporation_date"),
                "address_line_1": stream_event["data"]["address"]["address_line_1"],
                "locality": stream_event["data"]["address"]["locality"],
                "region": stream_event["data"]["address"]["region"],
                "country": stream_event["data"]["address"]["country"],
                "postal_code": stream_event["data"]["address"]["postal_code"],
                "stream_last_updated": datetime.now().isoformat(),
                "data_source": "stream",
                "last_stream_event_id": "stream_evt_003",
            }

            await database.upsert_company(new_company_data)

            # Verify company was added
            all_companies = await database.manager.fetch_all("SELECT * FROM companies", ())
            assert len(all_companies) == 4

            str001 = await database.get_company("STR001")
            assert str001 is not None
            assert str001 is not None and str001["company_name"] == "Stream Only Company Ltd"
            assert str001 is not None and str001["data_source"] == "stream"
            assert (
                str001 is not None and str001["company_status_detail"] == "proposal-to-strike-off"
            )

        finally:
            await database.disconnect()

    @pytest.mark.asyncio
    async def test_data_source_tracking(self, config: Any, stream_events: Any) -> None:
        """Test that data source is properly tracked for bulk vs stream data."""
        database = StreamingDatabase(config)
        await database.connect()

        try:
            # Check initial data sources
            bulk_companies = await database.manager.fetch_all(
                "SELECT * FROM companies WHERE data_source = ?", ("bulk",)
            )
            stream_companies = await database.manager.fetch_all(
                "SELECT * FROM companies WHERE data_source = ?", ("stream",)
            )

            assert len(bulk_companies) == 3
            assert len(stream_companies) == 0

            # Apply streaming updates to one company
            stream_event = stream_events[0]
            company_event = CompanyEvent.from_dict(stream_event)

            update_data = {
                "company_number": company_event.company_number,
                "company_name": company_event.company_name,
                "stream_last_updated": datetime.now().isoformat(),
                "data_source": "stream",
            }

            await database.upsert_company(update_data)

            # Check data sources after update
            bulk_companies_after = await database.manager.fetch_all(
                "SELECT * FROM companies WHERE data_source = ?", ("bulk",)
            )
            stream_companies_after = await database.manager.fetch_all(
                "SELECT * FROM companies WHERE data_source = ?", ("stream",)
            )

            assert len(bulk_companies_after) == 2  # One moved to stream
            assert len(stream_companies_after) == 1
            assert stream_companies_after[0]["company_number"] == "BLK001"

        finally:
            await database.disconnect()

    @pytest.mark.asyncio
    async def test_stream_metadata_preservation(self, config: Any) -> None:
        """Test that streaming metadata is preserved during updates."""
        database = StreamingDatabase(config)
        event_tracker = EventTracker(config)

        await database.connect()
        await event_tracker.connect()

        try:
            # Process event with tracking
            event_id = "meta_test_001"
            event_data = {
                "resource_kind": "company-profile",
                "resource_id": "BLK001",
                "data": {"company_number": "BLK001", "company_name": "Updated Name"},
                "event": {"timepoint": 6000, "published_at": "2025-08-15T15:00:00Z"},
            }

            # Track the event
            await event_tracker.track_event(event_id, event_data)
            await event_tracker.mark_processing(event_id)

            # Update company with metadata
            metadata = {
                "last_event_timepoint": 6000,
                "last_update_source": "streaming_api",
                "update_reason": "company_profile_change",
            }

            update_data = {
                "company_number": "BLK001",
                "company_name": "Updated Name",
                "stream_last_updated": datetime.now().isoformat(),
                "data_source": "stream",
                "last_stream_event_id": event_id,
                "stream_metadata": json.dumps(metadata),
            }

            await database.upsert_company(update_data)
            await event_tracker.mark_completed(event_id)

            # Verify metadata was stored
            company = await database.get_company("BLK001")
            assert company is not None and company["stream_metadata"] is not None

            assert company is not None
            stored_metadata = json.loads(company["stream_metadata"])
            assert stored_metadata is not None and stored_metadata["last_event_timepoint"] == 6000
            assert (
                stored_metadata is not None
                and stored_metadata["update_reason"] == "company_profile_change"
            )

            # Verify event was tracked
            event_record = await event_tracker.logger.get_event(event_id)
            assert event_record is not None and event_record["processing_status"] == "completed"

        finally:
            await event_tracker.disconnect()
            await database.disconnect()


class TestDataConsistencyManagement:
    """Test data consistency between bulk and streaming sources."""

    @pytest.mark.asyncio
    async def test_conflict_resolution_stream_over_bulk(self, config: Any) -> None:
        """Test that streaming data takes precedence over bulk data in conflicts."""
        database = StreamingDatabase(config)
        await database.connect()

        try:
            # Get initial bulk data
            bulk_company = await database.get_company("BLK001")
            bulk_company["company_name"] if bulk_company is not None else "Unknown"

            # Simulate newer streaming data
            stream_update = {
                "company_number": "BLK001",
                "company_name": "Streaming Updated Name",
                "company_status": "active",
                "stream_last_updated": datetime.now().isoformat(),
                "data_source": "stream",
                "last_stream_event_id": "conflict_test_001",
            }

            await database.upsert_company(stream_update)

            # Simulate later bulk update (should not override streaming data)
            bulk_update = {
                "company_number": "BLK001",
                "company_name": "Bulk Updated Name",
                "company_status": "active",
                "data_source": "bulk",
            }

            # This should not override the streaming data due to conflict resolution
            bulk_company_record = CompanyRecord.from_dict(bulk_update)
            await database.upsert_company_with_conflict_resolution(bulk_company_record)

            # Verify streaming data is preserved
            final_company = await database.get_company("BLK001")
            assert (
                final_company is not None
                and final_company["company_name"] == "Streaming Updated Name"
            )
            assert final_company is not None and final_company["data_source"] == "stream"
            assert (
                final_company is not None
                and final_company["last_stream_event_id"] == "conflict_test_001"
            )

        finally:
            await database.disconnect()

    @pytest.mark.asyncio
    async def test_bulk_data_fills_missing_stream_fields(self, config: Any) -> None:
        """Test that bulk data fills in missing fields from streaming updates."""
        database = StreamingDatabase(config)
        await database.connect()

        try:
            # Get full bulk data
            bulk_company = await database.get_company("BLK002")
            original_address = (
                bulk_company["address_line_1"] if bulk_company is not None else "Unknown"
            )
            original_sic_codes = (
                bulk_company["sic_codes"] if bulk_company is not None else "Unknown"
            )

            # Apply partial streaming update (only name and status)
            partial_stream_update = {
                "company_number": "BLK002",
                "company_name": "Partially Updated Name",
                "company_status": "active",
                "stream_last_updated": datetime.now().isoformat(),
                "data_source": "stream",
            }

            await database.upsert_company(partial_stream_update)

            # Verify streaming fields are updated but bulk fields preserved
            updated_company = await database.get_company("BLK002")
            assert (
                updated_company is not None
                and updated_company["company_name"] == "Partially Updated Name"
            )
            assert updated_company is not None and updated_company["data_source"] == "stream"
            assert (
                updated_company is not None
                and updated_company["address_line_1"] == original_address
            )  # Preserved from bulk
            assert (
                updated_company is not None and updated_company["sic_codes"] == original_sic_codes
            )  # Preserved from bulk

        finally:
            await database.disconnect()

    @pytest.mark.asyncio
    async def test_timestamp_based_conflict_resolution(self, config: Any) -> None:
        """Test conflict resolution based on timestamps."""
        database = StreamingDatabase(config)
        await database.connect()

        try:
            # Initial streaming update
            old_timestamp = (datetime.now() - timedelta(hours=1)).isoformat()
            old_update = {
                "company_number": "BLK003",
                "company_name": "Old Streaming Name",
                "stream_last_updated": old_timestamp,
                "data_source": "stream",
            }

            await database.upsert_company(old_update)

            # Newer streaming update
            new_timestamp = datetime.now().isoformat()
            new_update = {
                "company_number": "BLK003",
                "company_name": "New Streaming Name",
                "stream_last_updated": new_timestamp,
                "data_source": "stream",
            }

            await database.upsert_company(new_update)

            # Verify newer update took precedence
            final_company = await database.get_company("BLK003")
            assert (
                final_company is not None and final_company["company_name"] == "New Streaming Name"
            )
            assert (
                final_company is not None and final_company["stream_last_updated"] == new_timestamp
            )

        finally:
            await database.disconnect()


class TestBulkProcessingWorkflow:
    """Test the workflow of bulk processing alongside streaming."""

    @pytest.mark.asyncio
    async def test_bulk_import_with_existing_stream_data(self, config: Any) -> None:
        """Test bulk import when streaming data already exists."""
        database = StreamingDatabase(config)
        await database.connect()

        try:
            # First, update a company via streaming
            stream_update = {
                "company_number": "BLK001",
                "company_name": "Stream Updated Company",
                "company_status": "active",
                "stream_last_updated": datetime.now().isoformat(),
                "data_source": "stream",
                "last_stream_event_id": "bulk_test_001",
            }

            await database.upsert_company(stream_update)

            # Simulate bulk import with different data for same company
            bulk_import_data = {
                "company_number": "BLK001",
                "company_name": "Bulk Import Name",
                "company_status": "active",
                "incorporation_date": "2020-01-01",  # Additional bulk field
                "sic_codes": '[{"code": "12345", "description": "New activity"}]',
                "data_source": "bulk",
            }

            # Bulk import should not override streaming data but can add missing fields
            bulk_company_record = CompanyRecord.from_dict(bulk_import_data)
            result = await database.upsert_company_with_conflict_resolution(bulk_company_record)

            # Verify the result shows conflict resolution occurred
            assert result is not None and result["action"] in ["updated", "skipped"]

            # Verify streaming data is preserved
            final_company = await database.get_company("BLK001")
            assert (
                final_company is not None
                and final_company["company_name"] == "Stream Updated Company"
            )  # Stream name preserved
            # Source preserved
            assert final_company is not None and final_company["data_source"] == "stream"
            assert (
                final_company["last_stream_event_id"] == "bulk_test_001"
            )  # Stream metadata preserved

        finally:
            await database.disconnect()

    @pytest.mark.asyncio
    async def test_mixed_data_source_queries(self, config: Any, stream_events: Any) -> None:
        """Test querying data from mixed bulk and streaming sources."""
        database = StreamingDatabase(config)
        await database.connect()

        try:
            # Update one company via streaming
            stream_event = stream_events[0]
            company_event = CompanyEvent.from_dict(stream_event)

            stream_update = {
                "company_number": company_event.company_number,
                "company_name": company_event.company_name,
                "data_source": "stream",
                "stream_last_updated": datetime.now().isoformat(),
            }

            await database.upsert_company(stream_update)

            # Add new company via streaming
            new_stream_company = {
                "company_number": "STR002",
                "company_name": "Another Stream Company",
                "company_status": "active",
                "data_source": "stream",
                "stream_last_updated": datetime.now().isoformat(),
            }

            await database.upsert_company(new_stream_company)

            # Query all companies
            all_companies = await database.manager.fetch_all("SELECT * FROM companies", ())
            assert len(all_companies) == 4  # 3 original + 1 new (BLK001 was updated, not added)

            # Query by data source
            bulk_companies = await database.manager.fetch_all(
                "SELECT * FROM companies WHERE data_source = ?", ("bulk",)
            )
            stream_companies = await database.manager.fetch_all(
                "SELECT * FROM companies WHERE data_source = ?", ("stream",)
            )

            assert len(bulk_companies) == 2  # BLK002, BLK003 (BLK001 changed to stream)
            assert len(stream_companies) == 2  # BLK001 (updated), STR002 (new)

            # Query companies with streaming metadata
            companies_with_stream_data = await database.manager.fetch_all(
                "SELECT * FROM companies WHERE stream_last_updated IS NOT NULL"
            )

            assert len(companies_with_stream_data) == 2

        finally:
            await database.disconnect()

    @pytest.mark.asyncio
    async def test_data_quality_metrics_mixed_sources(self, config: Any) -> None:
        """Test data quality metrics across mixed data sources."""
        database = StreamingDatabase(config)
        await database.connect()

        try:
            # Update some companies via streaming to create mixed data
            stream_updates = [
                {
                    "company_number": "BLK001",
                    "company_name": "Stream Updated One",
                    "data_source": "stream",
                    "stream_last_updated": datetime.now().isoformat(),
                },
                {
                    "company_number": "STR001",
                    "company_name": "Pure Stream Company",
                    "company_status": "active",
                    "data_source": "stream",
                    "stream_last_updated": datetime.now().isoformat(),
                },
            ]

            for update in stream_updates:
                await database.upsert_company(update)

            # Get data quality metrics
            quality_metrics = await database.get_data_quality_metrics()

            # 3 original + 1 new
            assert quality_metrics is not None and quality_metrics["total_companies"] == 4
            assert quality_metrics["completeness_score"] > 0
            assert isinstance(quality_metrics["field_coverage"], dict)

            # Check data source distribution
            source_distribution = await database.manager.fetch_all(
                "SELECT data_source, COUNT(*) as count FROM companies GROUP BY data_source"
            )

            source_counts = {row["data_source"]: row["count"] for row in source_distribution}
            assert source_counts is not None and source_counts["bulk"] == 2
            assert source_counts is not None and source_counts["stream"] == 2

        finally:
            await database.disconnect()


class TestStreamingEventProcessingWithBulkData:
    """Test streaming event processing in the context of existing bulk data."""

    @pytest.mark.asyncio
    async def test_event_processing_preserves_bulk_relationships(self, config: Any) -> None:
        """Test that streaming event processing preserves relationships from bulk data."""
        database = StreamingDatabase(config)
        processor = EventProcessor(config)
        event_tracker = EventTracker(config)

        await database.connect()
        await event_tracker.connect()

        try:
            # Get initial company with bulk data
            initial_company = await database.get_company("BLK002")
            original_incorporation_date = (
                initial_company["incorporation_date"] if initial_company is not None else None
            )
            original_sic_codes = (
                initial_company["sic_codes"] if initial_company is not None else None
            )

            # Process streaming event
            event_data = {
                "resource_kind": "company-profile",
                "resource_id": "BLK002",
                "data": {
                    "company_number": "BLK002",
                    "company_name": "Updated via Event Processing",
                    "company_status": "active",
                },
                "event": {"timepoint": 7000, "published_at": "2025-08-15T16:00:00Z"},
            }

            event_id = "bulk_relation_test_001"

            # Track and process event
            await event_tracker.track_event(event_id, event_data)
            await event_tracker.mark_processing(event_id)

            processing_result = await processor.process_event(event_data)
            assert processing_result is True

            # Update database with streaming data
            company_event = CompanyEvent.from_dict(event_data)
            update_data = {
                "company_number": company_event.company_number,
                "company_name": company_event.company_name,
                "company_status": company_event.company_status,
                "stream_last_updated": datetime.now().isoformat(),
                "data_source": "stream",
                "last_stream_event_id": event_id,
            }

            await database.upsert_company(update_data)
            await event_tracker.mark_completed(event_id)

            # Verify bulk relationships are preserved
            updated_company = await database.get_company("BLK002")
            assert (
                updated_company is not None
                and updated_company["company_name"] == "Updated via Event Processing"
            )
            assert updated_company is not None and updated_company["data_source"] == "stream"
            assert (
                updated_company is not None
                and updated_company["incorporation_date"] == original_incorporation_date
            )
            assert (
                updated_company is not None and updated_company["sic_codes"] == original_sic_codes
            )

        finally:
            await event_tracker.disconnect()
            await database.disconnect()

    @pytest.mark.asyncio
    async def test_batch_event_processing_with_bulk_data(
        self, config: Any, stream_events: Any
    ) -> None:
        """Test batch processing of streaming events with existing bulk data."""
        database = StreamingDatabase(config)
        processor = EventProcessor(config)

        await database.connect()

        try:
            # Process multiple events in batch
            processing_results = await processor.process_batch(stream_events)

            # Verify all events processed successfully
            assert len(processing_results) == 3
            assert all(result is True for result in processing_results)

            # Apply updates to database
            for i, event_data in enumerate(stream_events):
                company_event = CompanyEvent.from_dict(event_data)

                update_data = {
                    "company_number": company_event.company_number,
                    "company_name": company_event.company_name,
                    "company_status": company_event.company_status,
                    "company_status_detail": event_data.get("data", {}).get(
                        "company_status_detail"
                    ),
                    "stream_last_updated": datetime.now().isoformat(),
                    "data_source": "stream",
                    "last_stream_event_id": f"batch_test_{i + 1}",
                }

                # Handle address data if present
                if "address" in event_data.get("data", {}):
                    address = event_data["data"]["address"]
                    update_data.update(
                        {
                            "address_line_1": address.get("address_line_1"),
                            "address_line_2": address.get("address_line_2"),
                            "locality": address.get("locality"),
                            "region": address.get("region"),
                            "country": address.get("country"),
                            "postal_code": address.get("postal_code"),
                        }
                    )

                await database.upsert_company(update_data)

            # Verify all updates were applied correctly
            all_companies = await database.manager.fetch_all("SELECT * FROM companies", ())
            assert len(all_companies) == 4  # 3 original + 1 new from streaming

            # Verify specific updates
            blk001 = await database.get_company("BLK001")
            assert blk001 is not None and blk001["company_name"] == "Bulk Company One Ltd - Updated"
            assert blk001 is not None and blk001["data_source"] == "stream"

            blk002 = await database.get_company("BLK002")
            assert (
                blk002 is not None and blk002["company_status_detail"] is None
            )  # Strike-off removed
            assert blk002 is not None and blk002["data_source"] == "stream"

            str001 = await database.get_company("STR001")
            assert str001 is not None
            assert str001 is not None and str001["company_name"] == "Stream Only Company Ltd"
            assert str001 is not None and str001["data_source"] == "stream"

        finally:
            await database.disconnect()
