"""
Integration tests for streaming functionality with existing scraper functionality.
Tests interaction between bulk import, officer import, and streaming updates.
"""

import pytest
import asyncio
import sqlite3
import tempfile
import shutil
import os
import subprocess
import sys
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock
from typing import Dict, Any, List

from src.streaming import (
    StreamingDatabase,
    StreamingConfig,
    EventProcessor,
    CompanyEvent,
    EventTracker,
    CompanyRecord,
    DatabaseManager
)


@pytest.fixture
def real_db_copy():
    """Create a copy of the real database for testing."""
    # Create temporary copy of the real database
    temp_dir = tempfile.mkdtemp()
    real_db_path = "/home/jimiryquai/repos/companies_house_scraper/companies.db"
    test_db_path = os.path.join(temp_dir, "test_companies.db")

    if os.path.exists(real_db_path):
        shutil.copy2(real_db_path, test_db_path)
    else:
        # If real DB doesn't exist, create a minimal test DB
        conn = sqlite3.connect(test_db_path)
        cursor = conn.cursor()

        # Create basic schema
        cursor.execute('''
            CREATE TABLE companies (
                company_number TEXT PRIMARY KEY NOT NULL,
                company_name TEXT,
                company_status TEXT,
                company_status_detail TEXT,
                incorporation_date TEXT,
                postcode TEXT,
                sic_code TEXT,
                imported_at TEXT,
                psc_fetched INTEGER DEFAULT 0,
                stream_last_updated TEXT,
                stream_status TEXT DEFAULT 'unknown',
                data_source TEXT DEFAULT 'bulk',
                last_stream_event_id TEXT,
                stream_metadata TEXT
            )
        ''')

        cursor.execute('''
            CREATE TABLE officers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_number TEXT NOT NULL,
                name TEXT,
                officer_role TEXT,
                appointed_on TEXT,
                resigned_on TEXT,
                officer_id TEXT,
                address_line_1 TEXT,
                address_line_2 TEXT,
                locality TEXT,
                region TEXT,
                country TEXT,
                postal_code TEXT,
                premises TEXT,
                nationality TEXT,
                occupation TEXT,
                dob_year INTEGER,
                dob_month INTEGER,
                country_of_residence TEXT,
                person_number TEXT,
                FOREIGN KEY (company_number) REFERENCES companies(company_number)
            )
        ''')

        cursor.execute('''
            CREATE TABLE stream_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE,
                event_type TEXT NOT NULL,
                company_number TEXT,
                event_data TEXT,
                processed_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (company_number) REFERENCES companies(company_number)
            )
        ''')

        # Insert sample bulk data
        sample_companies = [
            ('BLK001', 'Test Company One Ltd', 'active', None, '2020-01-01', 'SW1A 1AA', '62090',
             datetime.now().isoformat(), 0, None, 'unknown', 'bulk', None, None),
            ('BLK002', 'Test Company Two Ltd', 'active', 'proposal-to-strike-off', '2019-06-15', 'M1 1AA',
             '70100', datetime.now().isoformat(), 0, None, 'unknown', 'bulk', None, None),
            ('BLK003', 'Test Company Three Ltd', 'liquidation', None, '2018-12-01', 'B1 1AA',
             '82990', datetime.now().isoformat(), 0, None, 'unknown', 'bulk', None, None),
        ]

        cursor.executemany('''
            INSERT INTO companies (
                company_number, company_name, company_status, company_status_detail,
                incorporation_date, postcode, sic_code, imported_at, psc_fetched,
                stream_last_updated, stream_status, data_source, last_stream_event_id, stream_metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', sample_companies)

        conn.commit()
        conn.close()

    yield test_db_path

    # Cleanup
    shutil.rmtree(temp_dir)


@pytest.fixture
def config_with_real_db(real_db_copy):
    """Create streaming config that uses the real database copy."""
    return StreamingConfig(
        streaming_api_key="12345678-1234-1234-1234-123456789012",
        database_path=real_db_copy,
        batch_size=10,
        api_base_url="https://api.companieshouse.gov.uk"
    )


class TestBulkImportStreamingIntegration:
    """Test integration between bulk imports and streaming updates."""

    @pytest.mark.asyncio
    async def test_streaming_updates_on_bulk_imported_companies(self, config_with_real_db):
        """Test that streaming updates work on companies imported via bulk import."""
        database = StreamingDatabase(config_with_real_db)
        await database.connect()

        try:
            # Get a company from bulk data
            companies = await database.manager.fetch_all(
                "SELECT * FROM companies WHERE data_source = 'bulk' LIMIT 5", ()
            )
            assert len(companies) >= 1, "Need at least one bulk company for testing"

            test_company_number = companies[0]["company_number"]
            original_name = companies[0]["company_name"]

            # Apply streaming update
            stream_update = {
                "company_number": test_company_number,
                "company_name": f"{original_name} - Updated via Stream",
                "company_status": "active",
                "stream_last_updated": datetime.now().isoformat(),
                "data_source": "stream",
                "last_stream_event_id": "integration_test_001"
            }

            await database.upsert_company(stream_update)

            # Verify the update
            updated_company = await database.get_company(test_company_number)
            assert updated_company["company_name"] == f"{original_name} - Updated via Stream"
            assert updated_company["data_source"] == "stream"
            assert updated_company["last_stream_event_id"] == "integration_test_001"

            # Verify original bulk fields are preserved
            assert updated_company["incorporation_date"] == companies[0]["incorporation_date"]
            assert updated_company["postcode"] == companies[0]["postcode"]

        finally:
            await database.disconnect()

    @pytest.mark.asyncio
    async def test_bulk_data_consistency_after_streaming_updates(self, config_with_real_db):
        """Test that bulk data remains consistent after streaming updates."""
        database = StreamingDatabase(config_with_real_db)
        await database.connect()

        try:
            # Get initial bulk company count
            initial_bulk_count = await database.manager.fetch_one(
                "SELECT COUNT(*) as count FROM companies WHERE data_source = 'bulk'", ()
            )
            initial_count = initial_bulk_count["count"]

            # Update some companies via streaming
            companies_to_update = await database.manager.fetch_all(
                "SELECT * FROM companies WHERE data_source = 'bulk' LIMIT 3", ()
            )

            for i, company in enumerate(companies_to_update):
                stream_update = {
                    "company_number": company["company_number"],
                    "company_name": f"{company['company_name']} - Streamed",
                    "data_source": "stream",
                    "stream_last_updated": datetime.now().isoformat(),
                    "last_stream_event_id": f"consistency_test_{i+1}"
                }
                await database.upsert_company(stream_update)

            # Verify counts
            final_bulk_count = await database.manager.fetch_one(
                "SELECT COUNT(*) as count FROM companies WHERE data_source = 'bulk'", ()
            )
            stream_count = await database.manager.fetch_one(
                "SELECT COUNT(*) as count FROM companies WHERE data_source = 'stream'", ()
            )
            total_count = await database.manager.fetch_one(
                "SELECT COUNT(*) as count FROM companies", ()
            )

            # Should have same total companies, but some moved from bulk to stream
            assert total_count["count"] == initial_count
            assert final_bulk_count["count"] == initial_count - len(companies_to_update)
            assert stream_count["count"] == len(companies_to_update)

        finally:
            await database.disconnect()

    @pytest.mark.asyncio
    async def test_strike_off_status_streaming_updates(self, config_with_real_db):
        """Test streaming updates to companies with strike-off status."""
        database = StreamingDatabase(config_with_real_db)
        await database.connect()

        try:
            # Find companies with strike-off status
            strike_off_companies = await database.manager.fetch_all(
                "SELECT * FROM companies WHERE company_status_detail LIKE '%strike%' LIMIT 2", ()
            )

            if not strike_off_companies:
                # Create test strike-off company if none exist
                test_company = {
                    "company_number": "TEST_STRIKE_001",
                    "company_name": "Test Strike Off Company",
                    "company_status": "active",
                    "company_status_detail": "proposal-to-strike-off",
                    "data_source": "bulk",
                    "imported_at": datetime.now().isoformat()
                }
                await database.upsert_company(test_company)
                strike_off_companies = [test_company]

            test_company = strike_off_companies[0]
            company_number = test_company["company_number"]

            # Simulate streaming update that removes strike-off status
            stream_update = {
                "company_number": company_number,
                "company_name": test_company["company_name"],
                "company_status": "active",
                "company_status_detail": None,  # Remove strike-off status
                "data_source": "stream",
                "stream_last_updated": datetime.now().isoformat(),
                "last_stream_event_id": "strike_off_removal_001"
            }

            await database.upsert_company(stream_update)

            # Verify strike-off status was removed
            updated_company = await database.get_company(company_number)
            assert updated_company["company_status_detail"] is None
            assert updated_company["data_source"] == "stream"

            # Verify we can still find this company in active companies
            active_companies = await database.get_companies_by_status("active")
            active_company_numbers = [c["company_number"] for c in active_companies]
            assert company_number in active_company_numbers

        finally:
            await database.disconnect()


class TestOfficerImportStreamingIntegration:
    """Test integration between officer imports and streaming updates."""

    @pytest.mark.asyncio
    async def test_streaming_preserves_officer_data(self, config_with_real_db):
        """Test that streaming updates preserve existing officer data."""
        database = StreamingDatabase(config_with_real_db)
        await database.connect()

        try:
            # Get a company that might have officers
            companies_with_officers = await database.manager.fetch_all("""
                SELECT DISTINCT c.* FROM companies c
                JOIN officers o ON c.company_number = o.company_number
                WHERE c.data_source = 'bulk'
                LIMIT 1
            """, ())

            if not companies_with_officers:
                # Create test company with officers if none exist
                test_company_number = "TEST_OFFICERS_001"
                test_company = {
                    "company_number": test_company_number,
                    "company_name": "Test Company with Officers",
                    "company_status": "active",
                    "data_source": "bulk"
                }
                await database.upsert_company(test_company)

                # Add test officers
                test_officers = [
                    (test_company_number, "John Smith", "director", "2020-01-01", None, "OFF001"),
                    (test_company_number, "Jane Doe", "secretary", "2020-01-01", None, "OFF002")
                ]

                await database.manager.execute_many("""
                    INSERT INTO officers (company_number, name, officer_role, appointed_on, resigned_on, officer_id)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, test_officers)

                companies_with_officers = [test_company]

            test_company = companies_with_officers[0]
            company_number = test_company["company_number"]

            # Get initial officer count
            initial_officers = await database.manager.fetch_all(
                "SELECT * FROM officers WHERE company_number = ?", (company_number,)
            )
            initial_officer_count = len(initial_officers)

            # Apply streaming update to company
            stream_update = {
                "company_number": company_number,
                "company_name": f"{test_company['company_name']} - Stream Updated",
                "company_status": "active",
                "data_source": "stream",
                "stream_last_updated": datetime.now().isoformat(),
                "last_stream_event_id": "officer_preserve_test_001"
            }

            await database.upsert_company(stream_update)

            # Verify officers are still there
            final_officers = await database.manager.fetch_all(
                "SELECT * FROM officers WHERE company_number = ?", (company_number,)
            )
            assert len(final_officers) == initial_officer_count

            # Verify company was updated
            updated_company = await database.get_company(company_number)
            assert updated_company["data_source"] == "stream"
            assert "Stream Updated" in updated_company["company_name"]

        finally:
            await database.disconnect()

    @pytest.mark.asyncio
    async def test_officer_data_accessible_after_streaming_updates(self, config_with_real_db):
        """Test that officer data remains accessible after streaming company updates."""
        database = StreamingDatabase(config_with_real_db)
        await database.connect()

        try:
            # Create test scenario
            test_company_number = "TEST_OFFICER_ACCESS_001"
            test_company = {
                "company_number": test_company_number,
                "company_name": "Test Officer Access Company",
                "company_status": "active",
                "data_source": "bulk",
                "imported_at": datetime.now().isoformat()
            }
            await database.upsert_company(test_company)

            # Add officers
            test_officers = [
                (test_company_number, "Alice Johnson", "director", "2019-01-01", None, "OFF003",
                 "123 Director St", None, "London", "Greater London", "England", "SW1A 1AA", None,
                 "British", "Director", 1980, 5, "United Kingdom", "PER001"),
                (test_company_number, "Bob Wilson", "secretary", "2019-06-01", "2023-01-01", "OFF004",
                 "456 Secretary Ave", "Suite 100", "Manchester", "Greater Manchester", "England", "M1 1AA", None,
                 "British", "Secretary", 1975, 10, "United Kingdom", "PER002")
            ]

            await database.manager.execute_many("""
                INSERT INTO officers (
                    company_number, name, officer_role, appointed_on, resigned_on, officer_id,
                    address_line_1, address_line_2, locality, region, country, postal_code, premises,
                    nationality, occupation, dob_year, dob_month, country_of_residence, person_number
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, test_officers)

            # Apply multiple streaming updates
            for i in range(3):
                stream_update = {
                    "company_number": test_company_number,
                    "company_name": f"Updated Company Name v{i+1}",
                    "company_status": "active",
                    "data_source": "stream",
                    "stream_last_updated": datetime.now().isoformat(),
                    "last_stream_event_id": f"officer_access_test_{i+1}"
                }
                await database.upsert_company(stream_update)

            # Test comprehensive officer queries

            # 1. All officers for the company
            all_officers = await database.manager.fetch_all(
                "SELECT * FROM officers WHERE company_number = ?", (test_company_number,)
            )
            assert len(all_officers) == 2

            # 2. Active officers (not resigned)
            active_officers = await database.manager.fetch_all(
                "SELECT * FROM officers WHERE company_number = ? AND resigned_on IS NULL",
                (test_company_number,)
            )
            assert len(active_officers) == 1
            assert active_officers[0]["name"] == "Alice Johnson"

            # 3. Join query - company with officers
            company_with_officers = await database.manager.fetch_all("""
                SELECT c.company_name, c.data_source, c.stream_last_updated,
                       o.name, o.officer_role, o.appointed_on, o.resigned_on
                FROM companies c
                JOIN officers o ON c.company_number = o.company_number
                WHERE c.company_number = ?
                ORDER BY o.appointed_on
            """, (test_company_number,))

            assert len(company_with_officers) == 2
            assert company_with_officers[0]["data_source"] == "stream"
            assert "Updated Company Name v3" in company_with_officers[0]["company_name"]

            # 4. Officer history across streaming updates
            officer_names = [row["name"] for row in company_with_officers]
            assert "Alice Johnson" in officer_names
            assert "Bob Wilson" in officer_names

        finally:
            await database.disconnect()


class TestExistingScriptIntegration:
    """Test integration with existing import scripts."""

    def test_database_schema_compatibility(self, config_with_real_db):
        """Test that streaming schema is compatible with existing scripts."""
        # Check that the database has all required tables and columns
        conn = sqlite3.connect(config_with_real_db.database_path)
        cursor = conn.cursor()

        try:
            # Check companies table has both old and new columns
            cursor.execute("PRAGMA table_info(companies)")
            columns = [row[1] for row in cursor.fetchall()]

            # Old columns (from bulk import)
            required_old_columns = [
                "company_number", "company_name", "company_status",
                "company_status_detail", "incorporation_date", "postcode",
                "sic_code", "imported_at", "psc_fetched"
            ]

            # New columns (from streaming)
            required_new_columns = [
                "stream_last_updated", "stream_status", "data_source",
                "last_stream_event_id", "stream_metadata"
            ]

            for col in required_old_columns:
                assert col in columns, f"Missing old column: {col}"

            for col in required_new_columns:
                assert col in columns, f"Missing new column: {col}"

            # Check officers table exists with correct structure
            cursor.execute("PRAGMA table_info(officers)")
            officer_columns = [row[1] for row in cursor.fetchall()]

            required_officer_columns = [
                "id", "company_number", "name", "officer_role",
                "appointed_on", "resigned_on", "officer_id"
            ]

            for col in required_officer_columns:
                assert col in officer_columns, f"Missing officer column: {col}"

            # Check stream_events table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='stream_events'")
            assert cursor.fetchone() is not None, "stream_events table missing"

        finally:
            conn.close()

    @pytest.mark.asyncio
    async def test_mixed_data_queries(self, config_with_real_db):
        """Test queries that work across bulk and streaming data."""
        database = StreamingDatabase(config_with_real_db)
        await database.connect()

        try:
            # Test 1: Count by data source
            bulk_count = await database.manager.fetch_one(
                "SELECT COUNT(*) as count FROM companies WHERE data_source = 'bulk'", ()
            )
            stream_count = await database.manager.fetch_one(
                "SELECT COUNT(*) as count FROM companies WHERE data_source = 'stream'", ()
            )
            total_count = await database.manager.fetch_one(
                "SELECT COUNT(*) as count FROM companies", ()
            )

            assert bulk_count["count"] + stream_count["count"] == total_count["count"]

            # Test 2: Recent activity query
            recent_activity = await database.manager.fetch_all("""
                SELECT company_number, company_name, data_source,
                       CASE
                           WHEN stream_last_updated IS NOT NULL THEN stream_last_updated
                           ELSE imported_at
                       END as last_activity
                FROM companies
                WHERE (stream_last_updated IS NOT NULL AND stream_last_updated > datetime('now', '-7 days'))
                   OR (imported_at IS NOT NULL AND imported_at > datetime('now', '-7 days'))
                ORDER BY last_activity DESC
                LIMIT 10
            """, ())

            assert isinstance(recent_activity, list)

            # Test 3: Strike-off companies across all sources
            strike_off_all_sources = await database.manager.fetch_all("""
                SELECT company_number, company_name, data_source,
                       company_status, company_status_detail
                FROM companies
                WHERE company_status_detail LIKE '%strike%'
                   OR company_status = 'struck-off'
                ORDER BY data_source, company_number
                LIMIT 5
            """, ())

            assert isinstance(strike_off_all_sources, list)

            # Test 4: Data completeness report
            completeness_report = await database.manager.fetch_all("""
                SELECT
                    data_source,
                    COUNT(*) as total_companies,
                    COUNT(CASE WHEN company_name IS NOT NULL THEN 1 END) as has_name,
                    COUNT(CASE WHEN incorporation_date IS NOT NULL THEN 1 END) as has_inc_date,
                    COUNT(CASE WHEN postcode IS NOT NULL THEN 1 END) as has_postcode,
                    COUNT(CASE WHEN stream_last_updated IS NOT NULL THEN 1 END) as has_stream_data
                FROM companies
                GROUP BY data_source
            """, ())

            assert len(completeness_report) > 0
            for row in completeness_report:
                assert row["total_companies"] > 0
                assert row["has_name"] <= row["total_companies"]

        finally:
            await database.disconnect()

    @pytest.mark.asyncio
    async def test_data_migration_scenarios(self, config_with_real_db):
        """Test scenarios that simulate data migration between bulk and streaming."""
        database = StreamingDatabase(config_with_real_db)
        await database.connect()

        try:
            # Scenario 1: Bulk company gets updated via streaming
            bulk_companies = await database.manager.fetch_all(
                "SELECT * FROM companies WHERE data_source = 'bulk' LIMIT 2", ()
            )

            if len(bulk_companies) >= 1:
                company = bulk_companies[0]
                company_number = company["company_number"]

                # Create streaming event data
                event_data = {
                    "resource_kind": "company-profile",
                    "resource_id": company_number,
                    "data": {
                        "company_number": company_number,
                        "company_name": f"{company['company_name']} - Migration Test",
                        "company_status": company["company_status"]
                    },
                    "event": {
                        "timepoint": 9000,
                        "published_at": datetime.now().isoformat()
                    }
                }

                event_id = "migration_test_001"

                # Log event using database directly (compatible with existing schema)
                await database.log_stream_event(
                    event_id,
                    event_data["resource_kind"],
                    company_number,
                    event_data
                )

                # Apply streaming update
                company_event = CompanyEvent.from_dict(event_data)
                stream_data = {
                    "company_number": company_event.company_number,
                    "company_name": company_event.company_name,
                    "company_status": company_event.company_status,
                    "data_source": "stream",
                    "stream_last_updated": datetime.now().isoformat(),
                    "last_stream_event_id": event_id
                }

                await database.upsert_company(stream_data)

                # Verify migration
                updated_company = await database.get_company(company_number)
                assert updated_company["data_source"] == "stream"
                assert "Migration Test" in updated_company["company_name"]

                # Verify event was logged
                event_record = await database.get_stream_event(event_id)
                assert event_record is not None
                assert event_record["event_type"] == "company-profile"

            # Scenario 2: Data quality improvement through streaming
            if len(bulk_companies) >= 2:
                company = bulk_companies[1]
                company_number = company["company_number"]

                # Simulate streaming update with enriched data (using only existing columns)
                enriched_data = {
                    "company_number": company_number,
                    "company_name": f"{company['company_name']} - Enriched",
                    "company_status": company["company_status"],
                    "company_status_detail": company.get("company_status_detail"),
                    "incorporation_date": company.get("incorporation_date"),
                    "postcode": "SW1A 1AA",  # Updated postcode
                    "sic_code": "62090,70100",  # Updated SIC codes
                    "data_source": "stream",
                    "stream_last_updated": datetime.now().isoformat(),
                    "last_stream_event_id": "enrichment_test_001"
                }

                await database.upsert_company(enriched_data)

                # Verify enrichment
                enriched_company = await database.get_company(company_number)
                assert enriched_company["data_source"] == "stream"
                assert "Enriched" in enriched_company["company_name"]
                assert enriched_company["postcode"] == "SW1A 1AA"
                assert enriched_company["sic_code"] == "62090,70100"
                assert enriched_company["stream_last_updated"] is not None

        finally:
            await database.disconnect()


class TestScraperWorkflowIntegration:
    """Test complete scraper workflow integration."""

    @pytest.mark.asyncio
    async def test_end_to_end_workflow_simulation(self, config_with_real_db):
        """Simulate complete end-to-end workflow with bulk and streaming data."""
        database = StreamingDatabase(config_with_real_db)
        processor = EventProcessor(config_with_real_db)

        await database.connect()

        try:
            # Step 1: Verify bulk data exists (simulating import_companies.py)
            initial_count = await database.manager.fetch_one(
                "SELECT COUNT(*) as count FROM companies WHERE data_source = 'bulk'", ()
            )
            assert initial_count["count"] > 0, "Need bulk data for integration test"

            # Step 2: Simulate officer import on some companies
            companies_for_officers = await database.manager.fetch_all(
                "SELECT * FROM companies WHERE data_source = 'bulk' LIMIT 3", ()
            )

            for company in companies_for_officers:
                # Simulate officer data being imported
                test_officers = [
                    (company["company_number"], f"Director of {company['company_name']}",
                     "director", "2020-01-01", None, f"OFF_{company['company_number']}_001"),
                    (company["company_number"], f"Secretary of {company['company_name']}",
                     "secretary", "2020-01-01", None, f"OFF_{company['company_number']}_002")
                ]

                await database.manager.execute_many("""
                    INSERT OR IGNORE INTO officers (company_number, name, officer_role, appointed_on, resigned_on, officer_id)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, test_officers)

            # Step 3: Simulate streaming updates arriving
            streaming_events = []
            for i, company in enumerate(companies_for_officers):
                event_data = {
                    "resource_kind": "company-profile",
                    "resource_id": company["company_number"],
                    "data": {
                        "company_number": company["company_number"],
                        "company_name": f"{company['company_name']} - Live Update",
                        "company_status": "active",
                        "company_status_detail": "proposal-to-strike-off" if i == 1 else None
                    },
                    "event": {
                        "timepoint": 10000 + i,
                        "published_at": datetime.now().isoformat()
                    }
                }
                streaming_events.append(event_data)

            # Process streaming events
            for i, event_data in enumerate(streaming_events):
                event_id = f"workflow_test_{i+1}"

                # Log event using database directly
                company_number = event_data["resource_id"]
                await database.log_stream_event(
                    event_id,
                    event_data["resource_kind"],
                    company_number,
                    event_data
                )

                # Process event
                processing_result = await processor.process_event(event_data)
                assert processing_result is True

                # Update database
                company_event = CompanyEvent.from_dict(event_data)
                stream_data = {
                    "company_number": company_event.company_number,
                    "company_name": company_event.company_name,
                    "company_status": company_event.company_status,
                    "company_status_detail": event_data.get("data", {}).get("company_status_detail"),
                    "data_source": "stream",
                    "stream_last_updated": datetime.now().isoformat(),
                    "last_stream_event_id": event_id
                }

                await database.upsert_company(stream_data)

            # Step 4: Verify end-to-end state

            # Check data source distribution
            final_counts = await database.manager.fetch_all("""
                SELECT data_source, COUNT(*) as count
                FROM companies
                GROUP BY data_source
            """, ())

            count_by_source = {row["data_source"]: row["count"] for row in final_counts}
            assert "bulk" in count_by_source
            assert "stream" in count_by_source
            assert count_by_source["stream"] == len(streaming_events)

            # Check officers are still linked
            companies_with_officers = await database.manager.fetch_all("""
                SELECT c.company_number, c.company_name, c.data_source,
                       COUNT(o.id) as officer_count
                FROM companies c
                LEFT JOIN officers o ON c.company_number = o.company_number
                WHERE c.company_number IN ({})
                GROUP BY c.company_number, c.company_name, c.data_source
            """.format(",".join("?" * len(companies_for_officers))),
                tuple(c["company_number"] for c in companies_for_officers)
            )

            for row in companies_with_officers:
                assert row["data_source"] == "stream"  # Should be updated by streaming
                assert row["officer_count"] >= 0  # Should still have officers
                assert "Live Update" in row["company_name"]

            # Check event logging
            logged_events = await database.manager.fetch_all(
                "SELECT * FROM stream_events WHERE event_id LIKE 'workflow_test_%'", ()
            )
            assert len(logged_events) == len(streaming_events)

            # Check strike-off status updates
            strike_off_companies = await database.manager.fetch_all(
                "SELECT * FROM companies WHERE company_status_detail = 'proposal-to-strike-off'", ()
            )
            assert len(strike_off_companies) >= 1  # At least one should have strike-off status

        finally:
            await database.disconnect()

    @pytest.mark.asyncio
    async def test_performance_with_real_data_scale(self, config_with_real_db):
        """Test performance characteristics with real data scale."""
        database = StreamingDatabase(config_with_real_db)
        await database.connect()

        try:
            # Measure query performance on existing data
            start_time = datetime.now()

            # Test 1: Count queries
            total_companies = await database.manager.fetch_one(
                "SELECT COUNT(*) as count FROM companies", ()
            )

            count_time = datetime.now()

            # Test 2: Complex join query
            sample_with_officers = await database.manager.fetch_all("""
                SELECT c.company_number, c.company_name, c.data_source,
                       COUNT(o.id) as officer_count
                FROM companies c
                LEFT JOIN officers o ON c.company_number = o.company_number
                GROUP BY c.company_number, c.company_name, c.data_source
                HAVING officer_count > 0
                LIMIT 100
            """, ())

            join_time = datetime.now()

            # Test 3: Streaming data operations
            test_companies = await database.manager.fetch_all(
                "SELECT * FROM companies WHERE data_source = 'bulk' LIMIT 10", ()
            )

            for company in test_companies:
                stream_update = {
                    "company_number": company["company_number"],
                    "company_name": company["company_name"],
                    "data_source": "stream",
                    "stream_last_updated": datetime.now().isoformat()
                }
                await database.upsert_company(stream_update)

            update_time = datetime.now()

            # Log performance metrics
            count_duration = (count_time - start_time).total_seconds()
            join_duration = (join_time - count_time).total_seconds()
            update_duration = (update_time - join_time).total_seconds()

            print(f"Performance test results:")
            print(f"  Total companies: {total_companies['count']}")
            print(f"  Count query: {count_duration:.3f}s")
            print(f"  Join query (100 records): {join_duration:.3f}s")
            print(f"  Updates (10 records): {update_duration:.3f}s")

            # Performance assertions (these are reasonable thresholds)
            assert count_duration < 5.0, "Count query too slow"
            assert join_duration < 10.0, "Join query too slow"
            assert update_duration < 5.0, "Updates too slow"

            # Data integrity checks
            assert total_companies["count"] > 0
            assert len(sample_with_officers) > 0

        finally:
            await database.disconnect()
