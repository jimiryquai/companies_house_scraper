"""Test Snov.io database schema migrations."""

import json
import sqlite3
import tempfile
from collections.abc import Generator
from datetime import datetime
from pathlib import Path

import pytest

from src.streaming.migrations import DatabaseMigration


class TestSnovMigrations:
    """Test Snov.io database schema migrations and functionality."""

    def test_migration_005_creates_snov_tables(self, temp_db_path: Path) -> None:
        """Test that migration 005 creates all Snov.io tables with correct structure."""
        migration = DatabaseMigration(str(temp_db_path))

        # Run migrations to get the Snov.io tables (should be migration 005)
        migration.run_migrations()

        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # Check that all Snov.io tables were created
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in cursor.fetchall()]

        expected_snov_tables = [
            "company_domains",
            "officer_emails",
            "snov_credit_usage",
            "snov_webhooks",
        ]

        for table in expected_snov_tables:
            assert table in tables, f"Missing Snov.io table: {table}"

        conn.close()

    def test_migration_005_company_domains_schema(self, temp_db_path: Path) -> None:
        """Test company_domains table has correct schema structure."""
        migration = DatabaseMigration(str(temp_db_path))
        migration.run_migrations()

        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # Check company_domains schema
        cursor.execute("PRAGMA table_info(company_domains)")
        columns = cursor.fetchall()
        column_dict = {col[1]: (col[2], col[3], col[4]) for col in columns}

        # Verify required columns exist with correct types and constraints
        assert column_dict["id"][0] == "INTEGER"
        assert column_dict["company_id"] == ("TEXT", 1, None)  # NOT NULL
        assert column_dict["domain"] == ("TEXT", 1, None)  # NOT NULL
        assert column_dict["confidence_score"][0] == "REAL"
        assert column_dict["discovery_method"][0] == "TEXT"
        assert column_dict["is_primary"] == ("BOOLEAN", 0, "FALSE")  # DEFAULT FALSE
        assert column_dict["status"] == ("TEXT", 0, "'active'")  # DEFAULT 'active'

        # Check foreign key constraints
        cursor.execute("PRAGMA foreign_key_list(company_domains)")
        fk_constraints = cursor.fetchall()
        assert len(fk_constraints) == 1
        assert fk_constraints[0][2] == "companies"  # References companies table
        assert fk_constraints[0][3] == "company_id"  # From column
        assert fk_constraints[0][4] == "company_number"  # To column

        conn.close()

    def test_migration_005_officer_emails_schema(self, temp_db_path: Path) -> None:
        """Test officer_emails table has correct schema structure."""
        migration = DatabaseMigration(str(temp_db_path))
        migration.run_migrations()

        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # Check officer_emails schema
        cursor.execute("PRAGMA table_info(officer_emails)")
        columns = cursor.fetchall()
        column_dict = {col[1]: (col[2], col[3], col[4]) for col in columns}

        # Verify key columns
        assert column_dict["id"][0] == "INTEGER"
        assert column_dict["officer_id"] == ("TEXT", 1, None)  # NOT NULL
        assert column_dict["email"] == ("TEXT", 1, None)  # NOT NULL
        assert column_dict["email_type"][0] == "TEXT"
        assert column_dict["verification_status"][0] == "TEXT"
        assert column_dict["confidence_score"][0] == "REAL"
        assert column_dict["domain"][0] == "TEXT"
        assert column_dict["discovery_method"][0] == "TEXT"
        assert column_dict["snov_request_id"][0] == "TEXT"

        # Check foreign key constraints
        cursor.execute("PRAGMA foreign_key_list(officer_emails)")
        fk_constraints = cursor.fetchall()
        assert len(fk_constraints) == 1
        assert fk_constraints[0][2] == "officers"  # References officers table

        conn.close()

    def test_migration_005_snov_credit_usage_schema(self, temp_db_path: Path) -> None:
        """Test snov_credit_usage table has correct schema structure."""
        migration = DatabaseMigration(str(temp_db_path))
        migration.run_migrations()

        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # Check snov_credit_usage schema
        cursor.execute("PRAGMA table_info(snov_credit_usage)")
        columns = cursor.fetchall()
        column_dict = {col[1]: (col[2], col[3], col[4]) for col in columns}

        # Verify key columns
        assert column_dict["id"][0] == "INTEGER"
        assert column_dict["operation_type"] == ("TEXT", 1, None)  # NOT NULL
        assert column_dict["credits_consumed"] == ("INTEGER", 1, None)  # NOT NULL
        assert column_dict["success"] == ("BOOLEAN", 1, None)  # NOT NULL
        assert column_dict["request_id"][0] == "TEXT"
        assert column_dict["company_id"][0] == "TEXT"
        assert column_dict["officer_id"][0] == "TEXT"
        assert column_dict["response_data"][0] == "TEXT"

        # Check foreign key constraints (should have 2: company_id and officer_id)
        cursor.execute("PRAGMA foreign_key_list(snov_credit_usage)")
        fk_constraints = cursor.fetchall()
        assert len(fk_constraints) == 2

        # Verify both foreign keys
        fk_tables = {fk[2] for fk in fk_constraints}
        assert "companies" in fk_tables
        assert "officers" in fk_tables

        conn.close()

    def test_migration_005_snov_webhooks_schema(self, temp_db_path: Path) -> None:
        """Test snov_webhooks table has correct schema structure."""
        migration = DatabaseMigration(str(temp_db_path))
        migration.run_migrations()

        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # Check snov_webhooks schema
        cursor.execute("PRAGMA table_info(snov_webhooks)")
        columns = cursor.fetchall()
        column_dict = {col[1]: (col[2], col[3], col[4]) for col in columns}

        # Verify key columns
        assert column_dict["id"][0] == "INTEGER"
        assert column_dict["webhook_id"] == ("TEXT", 1, None)  # NOT NULL
        assert column_dict["event_type"] == ("TEXT", 1, None)  # NOT NULL
        assert column_dict["status"] == ("TEXT", 1, None)  # NOT NULL
        assert column_dict["request_id"][0] == "TEXT"
        assert column_dict["payload"] == ("TEXT", 1, None)  # NOT NULL
        assert column_dict["processed_at"][0] == "TIMESTAMP"
        assert column_dict["error_message"][0] == "TEXT"

        conn.close()

    def test_migration_006_extends_queue_jobs(self, temp_db_path: Path) -> None:
        """Test that migration 006 extends queue_jobs table with Snov.io columns."""
        migration = DatabaseMigration(str(temp_db_path))
        migration.run_migrations()

        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # Check if queue_jobs exists and has Snov.io columns
        cursor.execute("PRAGMA table_info(queue_jobs)")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]

        # Should have Snov.io specific columns
        snov_columns = ["snov_request_id", "snov_credits_estimated"]
        for col in snov_columns:
            assert col in column_names, f"Missing Snov.io column in queue_jobs: {col}"

        conn.close()

    def test_migration_creates_all_indexes(self, temp_db_path: Path) -> None:
        """Test that all required indexes are created for Snov.io tables."""
        migration = DatabaseMigration(str(temp_db_path))
        migration.run_migrations()

        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # Check all indexes were created
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' ORDER BY name")
        indexes = [row[0] for row in cursor.fetchall()]

        expected_snov_indexes = [
            # company_domains indexes
            "idx_company_domains_company_id",
            "idx_company_domains_domain",
            "idx_company_domains_primary",
            # officer_emails indexes
            "idx_officer_emails_officer_id",
            "idx_officer_emails_email",
            "idx_officer_emails_domain",
            "idx_officer_emails_status",
            # snov_credit_usage indexes
            "idx_snov_credit_usage_timestamp",
            "idx_snov_credit_usage_operation",
            "idx_snov_credit_usage_company",
            # snov_webhooks indexes
            "idx_snov_webhooks_status",
            "idx_snov_webhooks_event_type",
            "idx_snov_webhooks_request_id",
            # queue_jobs snov index
            "idx_queue_jobs_snov_request",
        ]

        for expected_index in expected_snov_indexes:
            assert expected_index in indexes, f"Missing index: {expected_index}"

        conn.close()

    def test_schema_version_tracking(self, temp_db_path: Path) -> None:
        """Test that schema version is correctly tracked through Snov.io migrations."""
        migration = DatabaseMigration(str(temp_db_path))

        # Check initial version
        initial_version = migration.get_schema_version()
        assert initial_version == 0

        # Run all migrations
        migration.run_migrations()

        # Check final version includes Snov.io migrations
        final_version = migration.get_schema_version()
        assert final_version >= 5  # Should be at least 5 to include Snov.io tables

        # Verify schema_version table has entries
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT version, applied_at FROM schema_version ORDER BY version")
        version_history = cursor.fetchall()

        assert len(version_history) >= 5
        # Each version should have a valid timestamp
        for version, applied_at in version_history:
            assert isinstance(version, int)
            assert applied_at is not None
            # Verify timestamp can be parsed
            datetime.fromisoformat(applied_at)

        conn.close()

    def test_company_domains_crud_operations(self, temp_db_path: Path) -> None:
        """Test basic CRUD operations on company_domains table."""
        migration = DatabaseMigration(str(temp_db_path))
        migration.run_migrations()

        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # First create a company (required for foreign key)
        cursor.execute(
            "INSERT INTO companies (company_number, company_name) VALUES (?, ?)",
            ("12345678", "Test Company Ltd"),
        )

        # Test INSERT
        cursor.execute(
            """
            INSERT INTO company_domains
            (company_id, domain, confidence_score, discovery_method, is_primary, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            ("12345678", "testcompany.com", 0.95, "snov_domain_search", True, "active"),
        )

        domain_id = cursor.lastrowid

        # Test SELECT
        cursor.execute(
            """SELECT company_id, domain, confidence_score, is_primary, status
               FROM company_domains WHERE id = ?""",
            (domain_id,),
        )
        result = cursor.fetchone()
        assert result[0] == "12345678"
        assert result[1] == "testcompany.com"
        assert result[2] == 0.95
        assert result[3] == 1  # is_primary (SQLite returns 1 for True)
        assert result[4] == "active"

        # Test UPDATE
        cursor.execute(
            "UPDATE company_domains SET status = ?, confidence_score = ? WHERE id = ?",
            ("verified", 0.98, domain_id),
        )

        cursor.execute(
            "SELECT status, confidence_score FROM company_domains WHERE id = ?", (domain_id,)
        )
        updated = cursor.fetchone()
        assert updated[0] == "verified"
        assert updated[1] == 0.98

        # Test unique constraint (company_id, domain)
        with pytest.raises(sqlite3.IntegrityError):
            cursor.execute(
                """
                INSERT INTO company_domains (company_id, domain) VALUES (?, ?)
            """,
                ("12345678", "testcompany.com"),
            )

        conn.close()

    def test_officer_emails_crud_operations(self, temp_db_path: Path) -> None:
        """Test basic CRUD operations on officer_emails table."""
        migration = DatabaseMigration(str(temp_db_path))
        migration.run_migrations()

        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # Create required parent records
        cursor.execute(
            "INSERT INTO companies (company_number, company_name) VALUES (?, ?)",
            ("12345678", "Test Company Ltd"),
        )
        cursor.execute(
            """
            INSERT INTO officers (company_number, name, officer_role) VALUES (?, ?, ?)
        """,
            ("12345678", "John Smith", "director"),
        )

        officer_id = cursor.lastrowid

        # Test INSERT
        cursor.execute(
            """
            INSERT INTO officer_emails
            (officer_id, email, email_type, verification_status, confidence_score,
             domain, discovery_method)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (
                str(officer_id),  # Convert to string for consistency with foreign key
                "john.smith@testcompany.com",
                "work",
                "valid",
                0.92,
                "testcompany.com",
                "snov_email_finder",
            ),
        )

        email_id = cursor.lastrowid

        # Test SELECT
        cursor.execute(
            """
            SELECT officer_id, email, email_type, verification_status, confidence_score, domain
            FROM officer_emails WHERE id = ?
        """,
            (email_id,),
        )
        result = cursor.fetchone()
        assert result[0] == str(officer_id)  # SQLite returns string for TEXT fields
        assert result[1] == "john.smith@testcompany.com"
        assert result[2] == "work"
        assert result[3] == "valid"
        assert result[4] == 0.92
        assert result[5] == "testcompany.com"

        # Test unique constraint (officer_id, email)
        with pytest.raises(sqlite3.IntegrityError):
            cursor.execute(
                """
                INSERT INTO officer_emails (officer_id, email) VALUES (?, ?)
            """,
                (str(officer_id), "john.smith@testcompany.com"),
            )

        conn.close()

    def test_snov_credit_usage_crud_operations(self, temp_db_path: Path) -> None:
        """Test basic CRUD operations on snov_credit_usage table."""
        migration = DatabaseMigration(str(temp_db_path))
        migration.run_migrations()

        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # Create required parent records
        cursor.execute(
            "INSERT INTO companies (company_number, company_name) VALUES (?, ?)",
            ("12345678", "Test Company Ltd"),
        )
        cursor.execute(
            """
            INSERT INTO officers (company_number, name, officer_role) VALUES (?, ?, ?)
        """,
            ("12345678", "John Smith", "director"),
        )

        officer_id = cursor.lastrowid

        # Test INSERT
        response_data = json.dumps({"status": "success", "credits_used": 5})
        cursor.execute(
            """
            INSERT INTO snov_credit_usage
            (operation_type, credits_consumed, success, request_id, company_id,
             officer_id, response_data)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            ("domain_search", 5, 1, "req_123", "12345678", str(officer_id), response_data),
        )

        usage_id = cursor.lastrowid

        # Test SELECT
        cursor.execute(
            """
            SELECT operation_type, credits_consumed, success, request_id, company_id,
                   officer_id, response_data
            FROM snov_credit_usage WHERE id = ?
        """,
            (usage_id,),
        )
        result = cursor.fetchone()
        assert result[0] == "domain_search"
        assert result[1] == 5
        assert result[2] == 1  # SQLite returns 1 for True boolean
        assert result[3] == "req_123"
        assert result[4] == "12345678"
        assert result[5] == str(officer_id)

        # Verify JSON data can be parsed back
        parsed_data = json.loads(result[6])
        assert parsed_data["status"] == "success"
        assert parsed_data["credits_used"] == 5

        conn.close()

    def test_snov_webhooks_crud_operations(self, temp_db_path: Path) -> None:
        """Test basic CRUD operations on snov_webhooks table."""
        migration = DatabaseMigration(str(temp_db_path))
        migration.run_migrations()

        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # Test INSERT
        webhook_payload = json.dumps(
            {
                "event_type": "bulk_email_finder_completed",
                "request_id": "req_456",
                "results": [{"email": "test@example.com", "status": "valid"}],
            }
        )

        cursor.execute(
            """
            INSERT INTO snov_webhooks
            (webhook_id, event_type, status, request_id, payload)
            VALUES (?, ?, ?, ?, ?)
        """,
            ("webhook_789", "bulk_email_finder_completed", "received", "req_456", webhook_payload),
        )

        webhook_id = cursor.lastrowid

        # Test SELECT
        cursor.execute(
            """
            SELECT webhook_id, event_type, status, request_id, payload
            FROM snov_webhooks WHERE id = ?
        """,
            (webhook_id,),
        )
        result = cursor.fetchone()
        assert result[0] == "webhook_789"
        assert result[1] == "bulk_email_finder_completed"
        assert result[2] == "received"
        assert result[3] == "req_456"

        # Verify JSON payload can be parsed back
        parsed_payload = json.loads(result[4])
        assert parsed_payload["event_type"] == "bulk_email_finder_completed"
        assert parsed_payload["request_id"] == "req_456"

        # Test UPDATE (mark as processed)
        cursor.execute(
            """
            UPDATE snov_webhooks SET status = ?, processed_at = CURRENT_TIMESTAMP WHERE id = ?
        """,
            ("processed", webhook_id),
        )

        cursor.execute("SELECT status FROM snov_webhooks WHERE id = ?", (webhook_id,))
        updated_status = cursor.fetchone()[0]
        assert updated_status == "processed"

        # Test unique constraint on webhook_id
        with pytest.raises(sqlite3.IntegrityError):
            cursor.execute(
                """
                INSERT INTO snov_webhooks (webhook_id, event_type, status, payload)
                VALUES (?, ?, ?, ?)
            """,
                ("webhook_789", "test_event", "received", "{}"),
            )

        conn.close()

    def test_foreign_key_constraints(self, temp_db_path: Path) -> None:
        """Test that foreign key constraints work correctly for all Snov.io tables."""
        migration = DatabaseMigration(str(temp_db_path))
        migration.run_migrations()

        conn = sqlite3.connect(temp_db_path)
        conn.execute("PRAGMA foreign_keys = ON")  # Enable foreign key constraints
        cursor = conn.cursor()

        # Test company_domains foreign key constraint
        with pytest.raises(sqlite3.IntegrityError):
            cursor.execute(
                """
                INSERT INTO company_domains (company_id, domain) VALUES (?, ?)
            """,
                ("nonexistent", "test.com"),
            )

        # Test officer_emails foreign key constraint
        with pytest.raises(sqlite3.IntegrityError):
            cursor.execute(
                """
                INSERT INTO officer_emails (officer_id, email) VALUES (?, ?)
            """,
                ("999999", "test@example.com"),
            )

        # Test snov_credit_usage foreign key constraints
        with pytest.raises(sqlite3.IntegrityError):
            cursor.execute(
                """
                INSERT INTO snov_credit_usage (operation_type, credits_consumed, success, company_id)
                VALUES (?, ?, ?, ?)
            """,
                ("domain_search", 1, 1, "nonexistent"),
            )

        conn.close()

    def test_queue_jobs_snov_integration(self, temp_db_path: Path) -> None:
        """Test that queue_jobs table integrates properly with Snov.io workflow."""
        migration = DatabaseMigration(str(temp_db_path))
        migration.run_migrations()

        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # Test inserting Snov.io specific job types
        snov_job_types = [
            "snov_domain_discovery",
            "snov_email_discovery",
            "snov_bulk_email_discovery",
            "snov_webhook_processing",
        ]

        for i, job_type in enumerate(snov_job_types):
            cursor.execute(
                """
                INSERT INTO queue_jobs
                (job_type, company_number, status, snov_request_id, snov_credits_estimated)
                VALUES (?, ?, ?, ?, ?)
            """,
                (job_type, f"1234567{i}", "pending", f"req_{i}", i + 1),
            )

        # Verify jobs were inserted with Snov.io data
        cursor.execute(
            """
            SELECT job_type, snov_request_id, snov_credits_estimated
            FROM queue_jobs
            WHERE job_type LIKE 'snov_%'
            ORDER BY id
        """
        )
        snov_jobs = cursor.fetchall()

        assert len(snov_jobs) == 4
        for i, (job_type, request_id, credits) in enumerate(snov_jobs):
            assert job_type == snov_job_types[i]
            assert request_id == f"req_{i}"
            assert credits == i + 1

        # Test querying by snov_request_id
        cursor.execute("SELECT job_type FROM queue_jobs WHERE snov_request_id = ?", ("req_2",))
        result = cursor.fetchone()
        assert result[0] == "snov_bulk_email_discovery"

        conn.close()

    def test_comprehensive_data_flow(self, temp_db_path: Path) -> None:
        """Test complete data flow through all Snov.io tables."""
        migration = DatabaseMigration(str(temp_db_path))
        migration.run_migrations()

        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # 1. Create company and officer
        cursor.execute(
            "INSERT INTO companies (company_number, company_name) VALUES (?, ?)",
            ("87654321", "Acme Corporation"),
        )
        cursor.execute(
            """
            INSERT INTO officers (company_number, name, officer_role) VALUES (?, ?, ?)
        """,
            ("87654321", "Jane Doe", "director"),
        )
        officer_id = cursor.lastrowid

        # 2. Queue domain discovery job
        cursor.execute(
            """
            INSERT INTO queue_jobs
            (job_type, company_number, status, snov_request_id, snov_credits_estimated)
            VALUES (?, ?, ?, ?, ?)
        """,
            ("snov_domain_discovery", "87654321", "completed", "req_domain_123", 3),
        )

        # 3. Record credit usage for domain search
        cursor.execute(
            """
            INSERT INTO snov_credit_usage
            (operation_type, credits_consumed, success, request_id, company_id, response_data)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            (
                "domain_search",
                3,
                1,  # Use 1 for True in SQLite
                "req_domain_123",
                "87654321",
                '{"domains": ["acme.com"]}',
            ),
        )

        # 4. Store discovered domain
        cursor.execute(
            """
            INSERT INTO company_domains
            (company_id, domain, confidence_score, discovery_method, is_primary)
            VALUES (?, ?, ?, ?, ?)
        """,
            ("87654321", "acme.com", 0.89, "snov_domain_search", 1),  # Use 1 for True
        )

        # 5. Queue email discovery job
        cursor.execute(
            """
            INSERT INTO queue_jobs
            (job_type, company_number, status, snov_request_id, snov_credits_estimated)
            VALUES (?, ?, ?, ?, ?)
        """,
            ("snov_email_discovery", "87654321", "completed", "req_email_456", 5),
        )

        # 6. Record credit usage for email search
        cursor.execute(
            """
            INSERT INTO snov_credit_usage
            (operation_type, credits_consumed, success, request_id, company_id,
             officer_id, response_data)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (
                "email_finder",
                5,
                1,  # Use 1 for True in SQLite
                "req_email_456",
                "87654321",
                str(officer_id),
                '{"emails": ["jane.doe@acme.com"]}',
            ),
        )

        # 7. Store discovered email
        cursor.execute(
            """
            INSERT INTO officer_emails
            (officer_id, email, email_type, verification_status, confidence_score,
             domain, discovery_method)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (
                str(officer_id),
                "jane.doe@acme.com",
                "work",
                "valid",
                0.87,
                "acme.com",
                "snov_email_finder",
            ),
        )

        # 8. Process webhook notification
        webhook_payload = json.dumps(
            {
                "request_id": "req_email_456",
                "status": "completed",
                "results": [{"email": "jane.doe@acme.com", "verification": "valid"}],
            }
        )
        cursor.execute(
            """
            INSERT INTO snov_webhooks
            (webhook_id, event_type, status, request_id, payload, processed_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
            (
                "webhook_999",
                "email_finder_completed",
                "processed",
                "req_email_456",
                webhook_payload,
            ),
        )

        # Verify complete data flow by querying across all tables
        cursor.execute(
            """
            SELECT
                c.company_name,
                o.name as officer_name,
                cd.domain,
                oe.email,
                scu1.credits_consumed as domain_credits,
                scu2.credits_consumed as email_credits,
                sw.status as webhook_status
            FROM companies c
            JOIN officers o ON c.company_number = o.company_number
            LEFT JOIN company_domains cd ON c.company_number = cd.company_id
            LEFT JOIN officer_emails oe ON o.id = oe.officer_id
            LEFT JOIN snov_credit_usage scu1 ON c.company_number = scu1.company_id
                AND scu1.operation_type = 'domain_search'
            LEFT JOIN snov_credit_usage scu2 ON c.company_number = scu2.company_id
                AND scu2.operation_type = 'email_finder'
            LEFT JOIN snov_webhooks sw ON scu2.request_id = sw.request_id
            WHERE c.company_number = ?
        """,
            ("87654321",),
        )

        result = cursor.fetchone()
        assert result[0] == "Acme Corporation"  # company_name
        assert result[1] == "Jane Doe"  # officer_name
        assert result[2] == "acme.com"  # domain
        assert result[3] == "jane.doe@acme.com"  # email
        assert result[4] == 3  # domain_credits
        assert result[5] == 5  # email_credits
        assert result[6] == "processed"  # webhook_status

        # Verify total credit consumption
        cursor.execute(
            "SELECT SUM(credits_consumed) FROM snov_credit_usage WHERE company_id = ?",
            ("87654321",),
        )
        total_credits = cursor.fetchone()[0]
        assert total_credits == 8  # 3 + 5

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
