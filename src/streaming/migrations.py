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

    def run_migrations(self) -> None:
        """Run all pending migrations."""
        current_version = self.get_schema_version()
        logger.info(f"Current schema version: {current_version}")

        migrations = [
            (1, self.migration_001_create_base_tables),
            (2, self.migration_002_add_streaming_metadata),
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
