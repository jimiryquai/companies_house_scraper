"""
Tests for streaming database integration functionality.
"""

import os
import tempfile
from datetime import datetime
from typing import Any

import pytest

from src.streaming.config import StreamingConfig
from src.streaming.database import (
    CompanyRecord,
    DatabaseManager,
    StreamEventRecord,
    StreamingDatabase,
)


# Shared fixtures
@pytest.fixture
def temp_db() -> Any:
    """Create a temporary database for testing."""
    import sqlite3

    temp_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)  # noqa: SIM115
    temp_path = temp_file.name
    temp_file.close()

    # Initialize database schema synchronously
    with sqlite3.connect(temp_path) as conn:
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

        conn.execute("""
            CREATE TABLE IF NOT EXISTS stream_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE,
                event_type TEXT NOT NULL,
                company_number TEXT,
                event_data TEXT,
                processed_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (company_number) REFERENCES companies(company_number)
            )
        """)

        conn.commit()

    yield temp_path

    # Cleanup
    os.unlink(temp_path)


@pytest.fixture
def config(temp_db: Any) -> Any:
    """Create test configuration with temp database."""
    return StreamingConfig(
        streaming_api_key="test-api-key-123456", database_path=temp_db, batch_size=10
    )


@pytest.fixture
def sample_company_data() -> Any:
    """Sample company data for testing."""
    return {
        "company_number": "12345678",
        "company_name": "Test Company Ltd",
        "company_status": "active-proposal-to-strike-off",
        "company_status_detail": "Active - Proposal to Strike Off",
        "incorporation_date": "2020-01-15",
        "sic_codes": "62090,62020",
        "address_line_1": "123 Test Street",
        "locality": "Test City",
        "postal_code": "TE1 2ST",
        "country": "England",
    }


@pytest.fixture
def sample_event_data() -> Any:
    """Sample event data for testing."""
    return {
        "resource_kind": "company-profile",
        "resource_id": "12345678",
        "data": {
            "company_number": "12345678",
            "company_name": "Test Company Ltd",
            "company_status": "active-proposal-to-strike-off",
        },
        "event": {"timepoint": 12345, "published_at": "2025-08-15T12:30:00Z"},
    }


class TestDatabaseManager:
    """Test DatabaseManager functionality."""

    @pytest.mark.asyncio
    async def test_database_manager_initialization(self, config: Any) -> None:
        """Test DatabaseManager initialization."""
        manager = DatabaseManager(config)

        assert manager.db_path == config.database_path
        assert manager.pool_size == 5
        assert manager._pool == []
        assert not manager._is_initialized

    @pytest.mark.asyncio
    async def test_database_manager_connect(self, config: Any) -> None:
        """Test establishing database connection."""
        manager = DatabaseManager(config)

        await manager.connect()

        assert manager._is_initialized
        assert len(manager._pool) > 0

    @pytest.mark.asyncio
    async def test_database_manager_disconnect(self, config: Any) -> None:
        """Test closing database connections."""
        manager = DatabaseManager(config)
        await manager.connect()

        await manager.disconnect()

        assert not manager._is_initialized
        assert len(manager._pool) == 0

    @pytest.mark.asyncio
    async def test_database_manager_context_manager(self, config: Any) -> None:
        """Test using DatabaseManager as context manager."""
        async with DatabaseManager(config) as manager:
            assert manager._is_initialized
            assert len(manager._pool) > 0

        assert not manager._is_initialized

    @pytest.mark.asyncio
    async def test_get_connection_from_pool(self, config: Any) -> None:
        """Test getting connection from pool."""
        manager = DatabaseManager(config)
        await manager.connect()

        async with manager.get_connection() as conn:
            # Should be able to execute queries
            cursor = await conn.execute("SELECT 1")
            result = await cursor.fetchone()
            assert result[0] == 1

    @pytest.mark.asyncio
    async def test_execute_query(self, config: Any) -> None:
        """Test executing a query."""
        manager = DatabaseManager(config)
        await manager.connect()

        result = await manager.execute(
            "SELECT company_number FROM companies WHERE company_number = ?", ("12345678",)
        )

        assert result is not None

    @pytest.mark.asyncio
    async def test_execute_many(self, config: Any) -> None:
        """Test executing multiple queries."""
        manager = DatabaseManager(config)
        await manager.connect()

        data = [("11111111", "Company 1"), ("22222222", "Company 2"), ("33333333", "Company 3")]

        await manager.execute_many(
            "INSERT OR IGNORE INTO companies (company_number, company_name) VALUES (?, ?)", data
        )

        # Verify insertions
        async with manager.get_connection() as conn:
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM companies WHERE company_number IN "
                "('11111111', '22222222', '33333333')"
            )
            result = await cursor.fetchone()
            assert result[0] == 3

    @pytest.mark.asyncio
    async def test_transaction_commit(self, config: Any) -> None:
        """Test transaction commit."""
        manager = DatabaseManager(config)
        await manager.connect()

        async with manager.transaction() as conn:
            await conn.execute(
                "INSERT INTO companies (company_number, company_name) VALUES (?, ?)",
                ("44444444", "Transaction Company"),
            )

        # Verify data was committed
        result = await manager.fetch_one(
            "SELECT company_name FROM companies WHERE company_number = ?", ("44444444",)
        )
        assert result is not None
        assert result is not None and result["company_name"] == "Transaction Company"

    @pytest.mark.asyncio
    async def test_transaction_rollback(self, config: Any) -> None:
        """Test transaction rollback on error."""
        manager = DatabaseManager(config)
        await manager.connect()

        with pytest.raises(Exception):  # noqa: B017
            async with manager.transaction() as conn:
                await conn.execute(
                    "INSERT INTO companies (company_number, company_name) VALUES (?, ?)",
                    ("55555555", "Rollback Company"),
                )
                # Force an error
                raise Exception("Test rollback")

        # Verify data was NOT committed
        result = await manager.fetch_one(
            "SELECT company_name FROM companies WHERE company_number = ?", ("55555555",)
        )
        assert result is None


class TestStreamingDatabase:
    """Test StreamingDatabase functionality."""

    @pytest.mark.asyncio
    async def test_streaming_database_initialization(self, config: Any) -> None:
        """Test StreamingDatabase initialization."""
        db = StreamingDatabase(config)

        assert db.config == config
        assert db.manager is not None
        assert isinstance(db.manager, DatabaseManager)

    @pytest.mark.asyncio
    async def test_connect_and_disconnect(self, config: Any) -> None:
        """Test connecting and disconnecting."""
        db = StreamingDatabase(config)

        await db.connect()
        assert db.manager._is_initialized

        await db.disconnect()
        assert not db.manager._is_initialized

    @pytest.mark.asyncio
    async def test_upsert_company_insert(self, config: Any, sample_company_data: Any) -> None:
        """Test inserting new company record."""
        db = StreamingDatabase(config)
        await db.connect()

        await db.upsert_company(sample_company_data)

        # Verify insertion
        result = await db.get_company("12345678")
        assert result is not None
        assert result is not None and result["company_name"] == "Test Company Ltd"
        assert result is not None and result["company_status"] == "active-proposal-to-strike-off"

        await db.disconnect()

    @pytest.mark.asyncio
    async def test_upsert_company_update(self, config: Any, sample_company_data: Any) -> None:
        """Test updating existing company record."""
        db = StreamingDatabase(config)
        await db.connect()

        # Initial insert
        await db.upsert_company(sample_company_data)

        # Update data
        updated_data = sample_company_data.copy()
        updated_data["company_status"] = "struck-off"
        updated_data["company_name"] = "Updated Company Ltd"

        await db.upsert_company(updated_data)

        # Verify update
        result = await db.get_company("12345678")
        assert result is not None
        assert result is not None and result["company_name"] == "Updated Company Ltd"
        assert result is not None and result["company_status"] == "struck-off"

        await db.disconnect()

    @pytest.mark.asyncio
    async def test_batch_upsert_companies(self, config: Any) -> None:
        """Test batch upsert of multiple companies."""
        db = StreamingDatabase(config)
        await db.connect()

        companies = [
            {"company_number": "11111111", "company_name": "Company 1", "company_status": "active"},
            {"company_number": "22222222", "company_name": "Company 2", "company_status": "active"},
            {
                "company_number": "33333333",
                "company_name": "Company 3",
                "company_status": "struck-off",
            },
        ]

        await db.batch_upsert_companies(companies)

        # Verify all were inserted
        for company in companies:
            assert company is not None
            result = await db.get_company(company["company_number"])
            assert result is not None
            assert result is not None and result["company_name"] == company["company_name"]

        await db.disconnect()

    @pytest.mark.asyncio
    async def test_get_company_not_found(self, config: Any) -> None:
        """Test getting non-existent company."""
        db = StreamingDatabase(config)
        await db.connect()

        result = await db.get_company("NOTEXIST")
        assert result is None

        await db.disconnect()

    @pytest.mark.asyncio
    async def test_update_stream_metadata(self, config: Any, sample_company_data: Any) -> None:
        """Test updating stream metadata for a company."""
        db = StreamingDatabase(config)
        await db.connect()

        # Insert company first
        await db.upsert_company(sample_company_data)

        # Update stream metadata
        metadata = {
            "last_event_id": "evt_12345",
            "stream_status": "active",
            "last_updated": datetime.now().isoformat(),
        }

        await db.update_stream_metadata("12345678", metadata)

        # Verify metadata update
        result = await db.get_company("12345678")
        assert result is not None
        assert result is not None and result["stream_status"] == "active"
        assert result is not None and result["last_stream_event_id"] == "evt_12345"
        assert result is not None and result["data_source"] == "stream"

        await db.disconnect()

    @pytest.mark.asyncio
    async def test_log_stream_event(self, config: Any, sample_event_data: Any) -> None:
        """Test logging a stream event."""
        db = StreamingDatabase(config)
        await db.connect()

        event_id = await db.log_stream_event(
            event_id="evt_12345",
            event_type="company-profile",
            company_number="12345678",
            event_data=sample_event_data,
        )

        assert event_id is not None

        # Verify event was logged
        event = await db.get_stream_event("evt_12345")
        assert event is not None
        assert event is not None and event["event_type"] == "company-profile"
        assert event is not None and event["company_number"] == "12345678"

        await db.disconnect()

    @pytest.mark.asyncio
    async def test_log_duplicate_stream_event(self, config: Any, sample_event_data: Any) -> None:
        """Test that duplicate events are handled properly."""
        db = StreamingDatabase(config)
        await db.connect()

        # Log event first time
        event_id1 = await db.log_stream_event(
            event_id="evt_duplicate",
            event_type="company-profile",
            company_number="12345678",
            event_data=sample_event_data,
        )

        # Try to log same event again
        event_id2 = await db.log_stream_event(
            event_id="evt_duplicate",
            event_type="company-profile",
            company_number="12345678",
            event_data=sample_event_data,
        )

        # Should return None for duplicate
        assert event_id1 is not None
        assert event_id2 is None

        await db.disconnect()

    @pytest.mark.asyncio
    async def test_get_companies_by_status(self, config: Any) -> None:
        """Test getting companies by status."""
        db = StreamingDatabase(config)
        await db.connect()

        # Insert test companies
        companies = [
            {
                "company_number": "11111111",
                "company_name": "Strike Off 1",
                "company_status": "active-proposal-to-strike-off",
            },
            {
                "company_number": "22222222",
                "company_name": "Strike Off 2",
                "company_status": "active-proposal-to-strike-off",
            },
            {
                "company_number": "33333333",
                "company_name": "Active Company",
                "company_status": "active",
            },
            {
                "company_number": "44444444",
                "company_name": "Struck Off",
                "company_status": "struck-off",
            },
        ]

        await db.batch_upsert_companies(companies)

        # Get strike-off companies
        strike_off_companies = await db.get_companies_by_status("active-proposal-to-strike-off")

        assert len(strike_off_companies) == 2
        assert all(
            c["company_status"] == "active-proposal-to-strike-off" for c in strike_off_companies
        )

        await db.disconnect()

    @pytest.mark.asyncio
    async def test_get_stream_stats(self, config: Any) -> None:
        """Test getting streaming statistics."""
        db = StreamingDatabase(config)
        await db.connect()

        # Insert test data
        companies = [
            {"company_number": "11111111", "company_name": "Bulk 1", "data_source": "bulk"},
            {"company_number": "22222222", "company_name": "Stream 1", "data_source": "stream"},
            {"company_number": "33333333", "company_name": "Stream 2", "data_source": "stream"},
            {"company_number": "44444444", "company_name": "Both", "data_source": "both"},
        ]

        for company in companies:
            await db.upsert_company(company)

        # Log some events
        await db.log_stream_event("evt_1", "company-profile", "22222222", {})
        await db.log_stream_event("evt_2", "company-profile", "33333333", {})
        await db.log_stream_event("evt_3", "company-officers", "44444444", {})

        # Get stats
        stats = await db.get_stream_stats()

        assert stats is not None and stats["total_companies"] == 4
        assert stats is not None and stats["stream_companies"] == 2
        assert stats is not None and stats["bulk_companies"] == 1
        assert stats is not None and stats["both_source_companies"] == 1
        assert stats is not None and stats["total_events"] == 3
        assert stats["event_types"]["company-profile"] == 2
        assert stats["event_types"]["company-officers"] == 1

        await db.disconnect()


class TestCompanyRecord:
    """Test CompanyRecord data model."""

    def test_company_record_from_dict(self, sample_company_data: Any) -> None:
        """Test creating CompanyRecord from dictionary."""
        record = CompanyRecord.from_dict(sample_company_data)

        assert record.company_number == "12345678"
        assert record.company_name == "Test Company Ltd"
        assert record.company_status == "active-proposal-to-strike-off"
        assert record.is_strike_off() is True

    def test_company_record_to_dict(self, sample_company_data: Any) -> None:
        """Test converting CompanyRecord to dictionary."""
        record = CompanyRecord.from_dict(sample_company_data)
        result = record.to_dict()

        assert result is not None and result["company_number"] == "12345678"
        assert result is not None and result["company_name"] == "Test Company Ltd"
        assert "stream_metadata" in result

    def test_company_record_is_strike_off(self) -> None:
        """Test strike-off status detection."""
        strike_off_record = CompanyRecord(
            company_number="123",
            company_name="Test",
            company_status="active-proposal-to-strike-off",
        )
        assert strike_off_record.is_strike_off() is True

        active_record = CompanyRecord(
            company_number="456", company_name="Test", company_status="active"
        )
        assert active_record.is_strike_off() is False


class TestStreamEventRecord:
    """Test StreamEventRecord data model."""

    def test_stream_event_record_creation(self) -> None:
        """Test creating StreamEventRecord."""
        event = StreamEventRecord(
            event_id="evt_123",
            event_type="company-profile",
            company_number="12345678",
            event_data={"test": "data"},
            processed_at=datetime.now(),
        )

        assert event.event_id == "evt_123"
        assert event.event_type == "company-profile"
        assert event.company_number == "12345678"

    def test_stream_event_record_to_dict(self) -> None:
        """Test converting StreamEventRecord to dictionary."""
        event = StreamEventRecord(
            event_id="evt_123",
            event_type="company-profile",
            company_number="12345678",
            event_data={"test": "data"},
            processed_at=datetime.now(),
        )

        result = event.to_dict()

        assert result is not None and result["event_id"] == "evt_123"
        assert result is not None and result["event_type"] == "company-profile"
        assert result is not None and result["company_number"] == "12345678"
        assert result is not None and result["event_data"] == '{"test": "data"}'
