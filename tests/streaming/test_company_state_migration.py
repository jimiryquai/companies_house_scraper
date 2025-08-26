"""Comprehensive tests for company processing state database schema creation and migration.

This test suite validates the database schema required for the Company Processing
State Manager, ensuring bulletproof rate limiting and state management for
autonomous cloud deployment.
"""

import sqlite3
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest


class TestCompanyStateManagerSchema:
    """Test company processing state database schema creation and migration."""

    def test_company_processing_state_table_creation(self, temp_db_path: Path) -> None:
        """Test creation of company_processing_state table with all required columns."""
        conn = sqlite3.connect(temp_db_path)

        # Create the table
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

        # Verify table exists
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='company_processing_state'"
        )
        assert cursor.fetchone() is not None

        # Verify column structure
        cursor.execute("PRAGMA table_info(company_processing_state)")
        columns = cursor.fetchall()

        expected_columns = {
            "id": (
                "INTEGER",
                0,
                None,
                1,
            ),  # (type, notnull, default, pk) - PK columns have notnull=0
            "company_number": ("TEXT", 1, None, 0),
            "processing_state": ("TEXT", 1, "'detected'", 0),
            "created_at": ("TIMESTAMP", 0, "CURRENT_TIMESTAMP", 0),
            "updated_at": ("TIMESTAMP", 0, "CURRENT_TIMESTAMP", 0),
            "status_queued_at": ("TIMESTAMP", 0, None, 0),
            "status_fetched_at": ("TIMESTAMP", 0, None, 0),
            "officers_queued_at": ("TIMESTAMP", 0, None, 0),
            "officers_fetched_at": ("TIMESTAMP", 0, None, 0),
            "completed_at": ("TIMESTAMP", 0, None, 0),
            "status_retry_count": ("INTEGER", 0, "0", 0),
            "officers_retry_count": ("INTEGER", 0, "0", 0),
            "status_request_id": ("TEXT", 0, None, 0),
            "officers_request_id": ("TEXT", 0, None, 0),
            "last_error": ("TEXT", 0, None, 0),
            "last_error_at": ("TIMESTAMP", 0, None, 0),
            "last_429_response_at": ("TIMESTAMP", 0, None, 0),
            "rate_limit_violations": ("INTEGER", 0, "0", 0),
        }

        for col_info in columns:
            _, col_name, col_type, notnull, default, pk = col_info

            assert col_name in expected_columns
            expected_type, expected_notnull, expected_default, expected_pk = expected_columns[
                col_name
            ]
            assert col_type == expected_type
            assert notnull == expected_notnull
            assert pk == expected_pk
            if expected_default is not None:
                assert default == expected_default

        conn.close()

    def test_api_rate_limit_log_table_creation(self, temp_db_path: Path) -> None:
        """Test creation of api_rate_limit_log table for rate limit tracking."""
        conn = sqlite3.connect(temp_db_path)

        # Create the table
        conn.execute("""
        CREATE TABLE api_rate_limit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            request_type TEXT NOT NULL,
            response_status INTEGER NOT NULL,
            company_number TEXT NULL,
            request_id TEXT NULL,
            processing_time_ms INTEGER NULL
        )
        """)

        # Verify table exists
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='api_rate_limit_log'"
        )
        assert cursor.fetchone() is not None

        # Verify column structure
        cursor.execute("PRAGMA table_info(api_rate_limit_log)")
        columns = cursor.fetchall()

        expected_columns = {
            "id": ("INTEGER", 0, None, 1),  # PK columns have notnull=0
            "timestamp": ("TIMESTAMP", 0, "CURRENT_TIMESTAMP", 0),
            "request_type": ("TEXT", 1, None, 0),
            "response_status": ("INTEGER", 1, None, 0),
            "company_number": ("TEXT", 0, None, 0),
            "request_id": ("TEXT", 0, None, 0),
            "processing_time_ms": ("INTEGER", 0, None, 0),
        }

        for col_info in columns:
            _, col_name, col_type, notnull, default, pk = col_info

            assert col_name in expected_columns
            expected_type, expected_notnull, expected_default, expected_pk = expected_columns[
                col_name
            ]
            assert col_type == expected_type
            assert notnull == expected_notnull
            assert pk == expected_pk
            if expected_default is not None:
                assert default == expected_default

        conn.close()

    def test_company_processing_state_indexes(self, temp_db_path: Path) -> None:
        """Test creation of performance indexes for company_processing_state table."""
        conn = sqlite3.connect(temp_db_path)

        # Create table first
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

        # Create indexes
        indexes = [
            (
                "CREATE INDEX idx_company_processing_state_status "
                "ON company_processing_state(processing_state)"
            ),
            (
                "CREATE INDEX idx_company_processing_state_updated "
                "ON company_processing_state(updated_at)"
            ),
            (
                "CREATE INDEX idx_company_processing_state_company "
                "ON company_processing_state(company_number)"
            ),
            (
                "CREATE INDEX idx_company_processing_state_requests "
                "ON company_processing_state(status_request_id, officers_request_id)"
            ),
            (
                "CREATE INDEX idx_company_processing_state_violations "
                "ON company_processing_state(rate_limit_violations, last_429_response_at)"
            ),
        ]

        for index_sql in indexes:
            conn.execute(index_sql)

        # Verify indexes exist
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND tbl_name='company_processing_state'"
        )
        created_indexes = [row[0] for row in cursor.fetchall()]

        expected_indexes = [
            "idx_company_processing_state_status",
            "idx_company_processing_state_updated",
            "idx_company_processing_state_company",
            "idx_company_processing_state_requests",
            "idx_company_processing_state_violations",
        ]

        for expected_index in expected_indexes:
            assert expected_index in created_indexes

        conn.close()

    def test_api_rate_limit_log_indexes(self, temp_db_path: Path) -> None:
        """Test creation of indexes for api_rate_limit_log table."""
        conn = sqlite3.connect(temp_db_path)

        # Create table first
        conn.execute("""
        CREATE TABLE api_rate_limit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            request_type TEXT NOT NULL,
            response_status INTEGER NOT NULL,
            company_number TEXT NULL,
            request_id TEXT NULL,
            processing_time_ms INTEGER NULL
        )
        """)

        # Create indexes
        conn.execute("CREATE INDEX idx_api_rate_limit_timestamp ON api_rate_limit_log(timestamp)")
        conn.execute(
            "CREATE INDEX idx_api_rate_limit_status ON api_rate_limit_log(response_status)"
        )

        # Verify indexes exist
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='api_rate_limit_log'"
        )
        created_indexes = [row[0] for row in cursor.fetchall()]

        expected_indexes = ["idx_api_rate_limit_timestamp", "idx_api_rate_limit_status"]
        for expected_index in expected_indexes:
            assert expected_index in created_indexes

        conn.close()

    def test_unique_constraint_enforcement(self, temp_db_path: Path) -> None:
        """Test that unique constraint on company_number is enforced."""
        conn = sqlite3.connect(temp_db_path)

        # Create table
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

        # Insert first record - should succeed
        conn.execute(
            "INSERT INTO company_processing_state (company_number) VALUES (?)", ("12345678",)
        )

        # Try to insert duplicate company_number - should fail
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO company_processing_state (company_number) VALUES (?)", ("12345678",)
            )

        conn.close()

    def test_default_values(self, temp_db_path: Path) -> None:
        """Test that default values are applied correctly."""
        conn = sqlite3.connect(temp_db_path)

        # Create table
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

        # Insert minimal record
        conn.execute(
            "INSERT INTO company_processing_state (company_number) VALUES (?)", ("12345678",)
        )

        # Verify defaults were applied
        cursor = conn.cursor()
        cursor.execute(
            "SELECT processing_state, status_retry_count, officers_retry_count, "
            "rate_limit_violations FROM company_processing_state WHERE company_number = ?",
            ("12345678",),
        )
        row = cursor.fetchone()

        assert row[0] == "detected"  # processing_state default
        assert row[1] == 0  # status_retry_count default
        assert row[2] == 0  # officers_retry_count default
        assert row[3] == 0  # rate_limit_violations default

        conn.close()

    def test_processing_state_enum_values(self, temp_db_path: Path) -> None:
        """Test that all valid processing state values can be stored."""
        conn = sqlite3.connect(temp_db_path)

        # Create table
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

        # Test all valid processing states from schema documentation
        valid_states = [
            "detected",
            "status_queued",
            "status_fetched",
            "strike_off_confirmed",
            "officers_queued",
            "officers_fetched",
            "completed",
            "failed",
        ]

        for i, state in enumerate(valid_states):
            company_number = f"1234567{i}"
            conn.execute(
                "INSERT INTO company_processing_state (company_number, processing_state) "
                "VALUES (?, ?)",
                (company_number, state),
            )

            # Verify it was stored correctly
            cursor = conn.cursor()
            cursor.execute(
                "SELECT processing_state FROM company_processing_state WHERE company_number = ?",
                (company_number,),
            )
            stored_state = cursor.fetchone()[0]
            assert stored_state == state

        conn.close()

    def test_rate_limit_log_functionality(self, temp_db_path: Path) -> None:
        """Test api_rate_limit_log table functionality for tracking API calls."""
        conn = sqlite3.connect(temp_db_path)

        # Create table
        conn.execute("""
        CREATE TABLE api_rate_limit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            request_type TEXT NOT NULL,
            response_status INTEGER NOT NULL,
            company_number TEXT NULL,
            request_id TEXT NULL,
            processing_time_ms INTEGER NULL
        )
        """)

        # Test inserting various request types
        test_requests = [
            ("company_status", 200, "12345678", "req-001", 150),
            ("officers", 200, "12345678", "req-002", 300),
            ("company_status", 429, "87654321", "req-003", 50),
            ("officers", 404, "11111111", "req-004", 75),
        ]

        for request_type, status, company_number, request_id, processing_time in test_requests:
            conn.execute(
                """
            INSERT INTO api_rate_limit_log
            (request_type, response_status, company_number, request_id, processing_time_ms)
            VALUES (?, ?, ?, ?, ?)
            """,
                (request_type, status, company_number, request_id, processing_time),
            )

        # Verify data was stored correctly
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM api_rate_limit_log")
        assert cursor.fetchone()[0] == 4

        # Test querying by response status (important for rate limit monitoring)
        cursor.execute("SELECT COUNT(*) FROM api_rate_limit_log WHERE response_status = 429")
        assert cursor.fetchone()[0] == 1  # One 429 response

        cursor.execute("SELECT COUNT(*) FROM api_rate_limit_log WHERE response_status = 200")
        assert cursor.fetchone()[0] == 2  # Two successful responses

        conn.close()

    def test_schema_migration_integration(self, temp_db_path: Path) -> None:
        """Test complete schema creation as it would be used in migration."""
        conn = sqlite3.connect(temp_db_path)

        # Create complete schema as one migration
        migration_sql = """
        -- Company processing state tracking table
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
        );

        -- API rate limiting log table
        CREATE TABLE api_rate_limit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            request_type TEXT NOT NULL,
            response_status INTEGER NOT NULL,
            company_number TEXT NULL,
            request_id TEXT NULL,
            processing_time_ms INTEGER NULL
        );

        -- Performance indexes for company_processing_state
        CREATE INDEX idx_company_processing_state_status
        ON company_processing_state(processing_state);
        CREATE INDEX idx_company_processing_state_updated
        ON company_processing_state(updated_at);
        CREATE INDEX idx_company_processing_state_company
        ON company_processing_state(company_number);
        CREATE INDEX idx_company_processing_state_requests
        ON company_processing_state(status_request_id, officers_request_id);
        CREATE INDEX idx_company_processing_state_violations
        ON company_processing_state(rate_limit_violations, last_429_response_at);

        -- Performance indexes for api_rate_limit_log
        CREATE INDEX idx_api_rate_limit_timestamp ON api_rate_limit_log(timestamp);
        CREATE INDEX idx_api_rate_limit_status ON api_rate_limit_log(response_status);
        """

        # Execute complete migration
        conn.executescript(migration_sql)

        # Verify both tables exist
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in cursor.fetchall()]

        assert "company_processing_state" in tables
        assert "api_rate_limit_log" in tables

        # Verify all indexes exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' ORDER BY name")
        indexes = [row[0] for row in cursor.fetchall()]

        expected_indexes = [
            "idx_company_processing_state_status",
            "idx_company_processing_state_updated",
            "idx_company_processing_state_company",
            "idx_company_processing_state_requests",
            "idx_company_processing_state_violations",
            "idx_api_rate_limit_timestamp",
            "idx_api_rate_limit_status",
        ]

        for expected_index in expected_indexes:
            assert expected_index in indexes

        # Test basic functionality
        conn.execute(
            "INSERT INTO company_processing_state (company_number) VALUES (?)", ("12345678",)
        )

        conn.execute(
            """
        INSERT INTO api_rate_limit_log (request_type, response_status, company_number)
        VALUES (?, ?, ?)
        """,
            ("company_status", 200, "12345678"),
        )

        # Verify data was inserted correctly
        cursor.execute("SELECT COUNT(*) FROM company_processing_state")
        assert cursor.fetchone()[0] == 1

        cursor.execute("SELECT COUNT(*) FROM api_rate_limit_log")
        assert cursor.fetchone()[0] == 1

        conn.close()


@pytest.fixture
def temp_db_path() -> Generator[Path, None, None]:
    """Create a temporary database file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
        temp_path = Path(temp_file.name)

    yield temp_path

    # Cleanup
    if temp_path.exists():
        temp_path.unlink()
