"""Database migration utilities for streaming API integration."""

import logging
import sqlite3
from datetime import datetime

logger = logging.getLogger(__name__)


class DatabaseMigration:
    """Handles database schema migrations for streaming functionality."""

    def __init__(self, db_path: str = "companies.db"):
        """Initialize database migration manager."""
        self.db_path = db_path

    def get_connection(self) -> sqlite3.Connection:
        """Get database connection."""
        return sqlite3.connect(self.db_path)

    def get_schema_version(self) -> int:
        """Get current schema version."""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            # Check if schema_version table exists
            cursor.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='schema_version'
            """)

            if not cursor.fetchone():
                return 0

            cursor.execute("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")
            result = cursor.fetchone()
            return result[0] if result else 0
        except sqlite3.Error as e:
            logger.error(f"Error getting schema version: {e}")
            return 0
        finally:
            conn.close()

    def set_schema_version(self, version: int) -> None:
        """Set current schema version."""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            # Create schema_version table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )
            """)

            # Insert new version
            cursor.execute(
                """
                INSERT INTO schema_version (version, applied_at)
                VALUES (?, ?)
            """,
                (version, datetime.now().isoformat()),
            )

            conn.commit()
            logger.info(f"Schema version set to {version}")
        except sqlite3.Error as e:
            logger.error(f"Error setting schema version: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    def migration_001_create_base_tables(self) -> None:
        """Migration 001: Create base tables for streaming functionality.

        - Create companies table if it doesn't exist
        - Fix officers table schema (rename 'role' to 'officer_role')
        - Add missing columns for complete officer data
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()

            # First, create companies table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                    company_number TEXT PRIMARY KEY,
                    company_name TEXT,
                    company_status TEXT,
                    date_of_creation TEXT,
                    registered_office_address TEXT,
                    sic_codes TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            logger.info("Companies table created/verified")

            # Now handle officers table
            cursor.execute("PRAGMA table_info(officers)")
            columns = {row[1]: row[2] for row in cursor.fetchall()}

            logger.info(f"Current officers table columns: {list(columns.keys())}")

            # Create new officers table with correct schema
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS officers_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_number TEXT NOT NULL,
                    name TEXT,
                    officer_role TEXT,  -- Fixed: renamed from 'role'
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
            """)

            # Copy existing data with role -> officer_role mapping
            if "role" in columns:
                cursor.execute("""
                    INSERT INTO officers_new
                    (id, company_number, name, officer_role, appointed_on, resigned_on, officer_id)
                    SELECT id, company_number, name, role, appointed_on, resigned_on, officer_id
                    FROM officers
                """)
                logger.info("Migrated existing officer data with role -> officer_role mapping")

            # Drop old table and rename new one
            cursor.execute("DROP TABLE IF EXISTS officers")
            cursor.execute("ALTER TABLE officers_new RENAME TO officers")

            # Create indexes for better performance
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_officers_company_number ON officers(company_number)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_officers_officer_role ON officers(officer_role)"
            )

            conn.commit()
            logger.info("Migration 001 completed: Created base tables and fixed officers schema")

        except sqlite3.Error as e:
            logger.error(f"Migration 001 failed: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    def migration_002_add_streaming_metadata(self) -> None:
        """Migration 002: Add streaming metadata to companies table.

        - Add streaming-related columns for tracking data sources and updates
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()

            # Add streaming metadata columns to companies table
            streaming_columns = [
                ("stream_last_updated", "TEXT"),
                ("stream_status", "TEXT DEFAULT 'unknown'"),
                ("data_source", "TEXT DEFAULT 'bulk'"),  # 'bulk', 'stream', or 'both'
                ("last_stream_event_id", "TEXT"),
                ("stream_metadata", "TEXT"),  # JSON for additional metadata
            ]

            for column_name, column_type in streaming_columns:
                try:
                    cursor.execute(f"ALTER TABLE companies ADD COLUMN {column_name} {column_type}")
                    logger.info(f"Added column {column_name} to companies table")
                except sqlite3.OperationalError as e:
                    if "duplicate column name" in str(e).lower():
                        logger.info(f"Column {column_name} already exists, skipping")
                    else:
                        raise

            # Create streaming events log table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stream_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT UNIQUE,
                    event_type TEXT NOT NULL,
                    company_number TEXT,
                    event_data TEXT,  -- JSON
                    processed_at TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (company_number) REFERENCES companies(company_number)
                )
            """)

            # Create indexes for streaming tables
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_companies_stream_status ON companies(stream_status)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_companies_data_source ON companies(data_source)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_stream_events_company_number "
                "ON stream_events(company_number)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_stream_events_event_type "
                "ON stream_events(event_type)"
            )
            cursor.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_stream_events_event_id "
                "ON stream_events(event_id)"
            )

            conn.commit()
            logger.info("Migration 002 completed: Added streaming metadata")

        except sqlite3.Error as e:
            logger.error(f"Migration 002 failed: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    def migration_003_create_state_management_tables(self) -> None:
        """Migration 003: Create Company Processing State Manager tables.

        Creates bulletproof rate limiting and state management tables for
        autonomous cloud deployment with zero API violations:
        - company_processing_state: Tracks processing status for each company
        - api_rate_limit_log: Logs all API calls for rate limit monitoring
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()

            # Create company processing state table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS company_processing_state (
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
                    FOREIGN KEY (company_number) REFERENCES companies(company_number),
                    UNIQUE(company_number)
                )
            """)
            logger.info("Created company_processing_state table")

            # Create API rate limit log table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS api_rate_limit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    request_type TEXT NOT NULL,
                    response_status INTEGER NOT NULL,
                    company_number TEXT NULL,
                    request_id TEXT NULL,
                    processing_time_ms INTEGER NULL
                )
            """)
            logger.info("Created api_rate_limit_log table")

            # Create performance indexes for company_processing_state
            indexes = [
                (
                    "idx_company_processing_state_status",
                    "company_processing_state(processing_state)",
                ),
                (
                    "idx_company_processing_state_updated",
                    "company_processing_state(updated_at)",
                ),
                (
                    "idx_company_processing_state_company",
                    "company_processing_state(company_number)",
                ),
                (
                    "idx_company_processing_state_requests",
                    "company_processing_state(status_request_id, officers_request_id)",
                ),
                (
                    "idx_company_processing_state_violations",
                    "company_processing_state(rate_limit_violations, last_429_response_at)",
                ),
                ("idx_api_rate_limit_timestamp", "api_rate_limit_log(timestamp)"),
                ("idx_api_rate_limit_status", "api_rate_limit_log(response_status)"),
            ]

            for index_name, index_columns in indexes:
                cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {index_columns}")
                logger.info(f"Created index {index_name}")

            conn.commit()
            logger.info("Migration 003 completed: Created state management tables")

        except sqlite3.Error as e:
            logger.error(f"Migration 003 failed: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    def migration_004_add_company_status_detail(self) -> None:
        """Migration 004: Add company_status_detail column to companies table.

        This column is essential for strike-off detection as it contains
        detailed status information like "Active - Proposal to Strike Off".
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()

            # Add company_status_detail column
            try:
                cursor.execute("ALTER TABLE companies ADD COLUMN company_status_detail TEXT")
                logger.info("Added company_status_detail column to companies table")
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e).lower():
                    logger.info("Column company_status_detail already exists, skipping")
                else:
                    raise

            # Create index for efficient strike-off queries
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_companies_status_detail "
                "ON companies(company_status_detail)"
            )

            conn.commit()
            logger.info("Migration 004 completed: Added company_status_detail column")

        except sqlite3.Error as e:
            logger.error(f"Migration 004 failed: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    def migration_005_create_snov_tables(self) -> None:
        """Migration 005: Create Snov.io integration tables.

        Creates all Snov.io related tables for email discovery and domain management:
        - company_domains: Store company domains discovered via Snov.io
        - officer_emails: Store officer email addresses with verification status
        - snov_credit_usage: Track Snov.io API credit consumption
        - snov_webhooks: Handle webhook notifications from Snov.io
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()

            # Create company_domains table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS company_domains (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_id TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    confidence_score REAL,
                    discovery_method TEXT,
                    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_verified TIMESTAMP,
                    is_primary BOOLEAN DEFAULT FALSE,
                    status TEXT DEFAULT 'active',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                    FOREIGN KEY (company_id) REFERENCES companies(company_number),
                    UNIQUE(company_id, domain)
                )
            """)
            logger.info("Created company_domains table")

            # Create officer_emails table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS officer_emails (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    officer_id TEXT NOT NULL,
                    email TEXT NOT NULL,
                    email_type TEXT,
                    verification_status TEXT,
                    confidence_score REAL,
                    domain TEXT,
                    discovery_method TEXT,
                    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_verified TIMESTAMP,
                    snov_request_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                    FOREIGN KEY (officer_id) REFERENCES officers(id),
                    UNIQUE(officer_id, email)
                )
            """)
            logger.info("Created officer_emails table")

            # Create snov_credit_usage table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS snov_credit_usage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    operation_type TEXT NOT NULL,
                    credits_consumed INTEGER NOT NULL,
                    success BOOLEAN NOT NULL,
                    request_id TEXT,
                    company_id TEXT,
                    officer_id TEXT,
                    response_data TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                    FOREIGN KEY (company_id) REFERENCES companies(company_number),
                    FOREIGN KEY (officer_id) REFERENCES officers(id)
                )
            """)
            logger.info("Created snov_credit_usage table")

            # Create snov_webhooks table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS snov_webhooks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    webhook_id TEXT UNIQUE NOT NULL,
                    event_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    request_id TEXT,
                    payload TEXT NOT NULL,
                    processed_at TIMESTAMP,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                    UNIQUE(webhook_id)
                )
            """)
            logger.info("Created snov_webhooks table")

            # Create indexes for company_domains
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_company_domains_company_id ON company_domains(company_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_company_domains_domain ON company_domains(domain)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_company_domains_primary ON company_domains(company_id, is_primary)"
            )

            # Create indexes for officer_emails
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_officer_emails_officer_id ON officer_emails(officer_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_officer_emails_email ON officer_emails(email)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_officer_emails_domain ON officer_emails(domain)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_officer_emails_status ON officer_emails(verification_status)"
            )

            # Create indexes for snov_credit_usage
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_snov_credit_usage_timestamp ON snov_credit_usage(timestamp)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_snov_credit_usage_operation ON snov_credit_usage(operation_type)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_snov_credit_usage_company ON snov_credit_usage(company_id)"
            )

            # Create indexes for snov_webhooks
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_snov_webhooks_status ON snov_webhooks(status)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_snov_webhooks_event_type ON snov_webhooks(event_type)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_snov_webhooks_request_id ON snov_webhooks(request_id)"
            )

            conn.commit()
            logger.info("Migration 005 completed: Created Snov.io integration tables")

        except sqlite3.Error as e:
            logger.error(f"Migration 005 failed: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    def migration_006_extend_queue_jobs_snov(self) -> None:
        """Migration 006: Extend queue_jobs table with Snov.io specific columns.

        Adds columns to support Snov.io workflow integration:
        - snov_request_id: Track Snov.io request IDs for bulk operations
        - snov_credits_estimated: Estimated credit cost for job planning
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()

            # Check if queue_jobs table exists, create if not
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS queue_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_type TEXT NOT NULL,
                    company_number TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    priority INTEGER DEFAULT 1,
                    payload TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    retry_count INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3,
                    last_error TEXT
                )
            """)
            logger.info("queue_jobs table created/verified")

            # Add Snov.io specific columns
            snov_columns = [("snov_request_id", "TEXT"), ("snov_credits_estimated", "INTEGER")]

            for column_name, column_type in snov_columns:
                try:
                    cursor.execute(f"ALTER TABLE queue_jobs ADD COLUMN {column_name} {column_type}")
                    logger.info(f"Added column {column_name} to queue_jobs table")
                except sqlite3.OperationalError as e:
                    if "duplicate column name" in str(e).lower():
                        logger.info(f"Column {column_name} already exists, skipping")
                    else:
                        raise

            # Create index for Snov.io request tracking
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_queue_jobs_snov_request ON queue_jobs(snov_request_id)"
            )

            conn.commit()
            logger.info(
                "Migration 006 completed: Extended queue_jobs table for Snov.io integration"
            )

        except sqlite3.Error as e:
            logger.error(f"Migration 006 failed: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    def run_migrations(self) -> None:
        """Run all pending migrations."""
        current_version = self.get_schema_version()
        logger.info(f"Current schema version: {current_version}")

        migrations = [
            (1, self.migration_001_create_base_tables),
            (2, self.migration_002_add_streaming_metadata),
            (3, self.migration_003_create_state_management_tables),
            (4, self.migration_004_add_company_status_detail),
            (5, self.migration_005_create_snov_tables),
            (6, self.migration_006_extend_queue_jobs_snov),
        ]

        for version, migration_func in migrations:
            if current_version < version:
                logger.info(f"Running migration {version:03d}")
                try:
                    migration_func()
                    self.set_schema_version(version)
                    logger.info(f"Migration {version:03d} completed successfully")
                except Exception as e:
                    logger.error(f"Migration {version:03d} failed: {e}")
                    raise
            else:
                logger.info(f"Migration {version:03d} already applied, skipping")

        final_version = self.get_schema_version()
        logger.info(f"Database schema is now at version {final_version}")


def migrate_database(db_path: str = "companies.db") -> None:
    """Run database migrations."""
    migration = DatabaseMigration(db_path)
    migration.run_migrations()


if __name__ == "__main__":
    import sys

    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    db_path = sys.argv[1] if len(sys.argv) > 1 else "companies.db"
    migrate_database(db_path)
