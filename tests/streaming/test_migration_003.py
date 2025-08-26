"""Test migration 003 for Company Processing State Manager tables."""

import sqlite3
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest

from src.streaming.migrations import DatabaseMigration


class TestMigration003:
    """Test migration 003 state management tables creation."""

    def test_migration_003_creates_tables(self, temp_db_path: Path) -> None:
        """Test that migration 003 creates state management tables."""
        migration = DatabaseMigration(str(temp_db_path))

        # Run all migrations to get to migration 003
        migration.run_migrations()

        # Verify tables exist
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # Check tables were created
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in cursor.fetchall()]

        assert "company_processing_state" in tables
        assert "api_rate_limit_log" in tables
        conn.close()

    def test_migration_003_creates_indexes(self, temp_db_path: Path) -> None:
        """Test that migration 003 creates all required indexes."""
        migration = DatabaseMigration(str(temp_db_path))
        migration.run_migrations()

        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # Check indexes were created
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
            assert expected_index in indexes, f"Missing index: {expected_index}"

        conn.close()

    def test_migration_003_schema_validation(self, temp_db_path: Path) -> None:
        """Test that migration 003 creates correct schema structure."""
        migration = DatabaseMigration(str(temp_db_path))
        migration.run_migrations()

        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # Validate company_processing_state schema
        cursor.execute("PRAGMA table_info(company_processing_state)")
        columns = cursor.fetchall()

        # Check key columns exist with correct types
        column_dict = {col[1]: (col[2], col[3], col[4]) for col in columns}

        assert column_dict["company_number"] == ("TEXT", 1, None)  # NOT NULL
        assert column_dict["processing_state"] == ("TEXT", 1, "'detected'")  # NOT NULL with default
        assert column_dict["rate_limit_violations"] == ("INTEGER", 0, "0")  # Default 0

        # Validate api_rate_limit_log schema
        cursor.execute("PRAGMA table_info(api_rate_limit_log)")
        api_columns = cursor.fetchall()

        api_column_dict = {col[1]: (col[2], col[3]) for col in api_columns}
        assert api_column_dict["request_type"] == ("TEXT", 1)  # NOT NULL
        assert api_column_dict["response_status"] == ("INTEGER", 1)  # NOT NULL

        conn.close()

    def test_migration_003_basic_functionality(self, temp_db_path: Path) -> None:
        """Test basic insert/query functionality of created tables."""
        migration = DatabaseMigration(str(temp_db_path))
        migration.run_migrations()

        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # First insert a company (required for foreign key)
        cursor.execute(
            "INSERT INTO companies (company_number, company_name) VALUES (?, ?)",
            ("12345678", "Test Company"),
        )

        # Test company_processing_state table
        cursor.execute(
            "INSERT INTO company_processing_state (company_number) VALUES (?)", ("12345678",)
        )

        cursor.execute(
            "SELECT processing_state, rate_limit_violations FROM company_processing_state "
            "WHERE company_number = ?",
            ("12345678",),
        )
        result = cursor.fetchone()
        assert result[0] == "detected"  # Default value
        assert result[1] == 0  # Default value

        # Test api_rate_limit_log table
        cursor.execute(
            "INSERT INTO api_rate_limit_log (request_type, response_status, company_number) "
            "VALUES (?, ?, ?)",
            ("company_status", 200, "12345678"),
        )

        cursor.execute("SELECT COUNT(*) FROM api_rate_limit_log")
        count = cursor.fetchone()[0]
        assert count == 1

        conn.close()

    def test_migration_003_schema_version_updated(self, temp_db_path: Path) -> None:
        """Test that schema version is correctly updated to 3."""
        migration = DatabaseMigration(str(temp_db_path))

        # Check initial version
        initial_version = migration.get_schema_version()
        assert initial_version == 0

        # Run migrations
        migration.run_migrations()

        # Check final version (includes migration 004 for company_status_detail)
        final_version = migration.get_schema_version()
        assert final_version == 4


@pytest.fixture
def temp_db_path() -> Generator[Path, None, None]:
    """Create a temporary database file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
        temp_path = Path(temp_file.name)

    yield temp_path

    # Cleanup
    if temp_path.exists():
        temp_path.unlink()
