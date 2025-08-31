#!/usr/bin/env python3
"""Database migration runner for Companies House scraper.

This script provides a standalone way to run database migrations for the
Companies House API scraper system, ensuring the database schema is
up-to-date for streaming API functionality.

Usage:
    python migrate_db.py [database_path]
    uv run python migrate_db.py [database_path]

If no database path is provided, defaults to 'companies.db'
"""

import argparse

# Import migrations directly to avoid loading full streaming package
import importlib.util
import logging
import sys
from pathlib import Path
from typing import Any

migration_spec = importlib.util.spec_from_file_location(
    "migrations", Path(__file__).parent / "src" / "streaming" / "migrations.py"
)
if migration_spec is None or migration_spec.loader is None:
    raise ImportError("Cannot load migrations module")

migrations = importlib.util.module_from_spec(migration_spec)
migration_spec.loader.exec_module(migrations)

DatabaseMigration: Any = migrations.DatabaseMigration
migrate_database: Any = migrations.migrate_database


def setup_logging(verbose: bool = False) -> None:
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def main() -> None:
    """Main migration runner."""
    parser = argparse.ArgumentParser(
        description="Run database migrations for Companies House scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "database",
        nargs="?",
        default="companies.db",
        help="Path to the SQLite database file (default: companies.db)",
    )

    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")

    parser.add_argument(
        "--check-version",
        action="store_true",
        help="Check current schema version without running migrations",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what migrations would run without executing them",
    )

    args = parser.parse_args()

    # Set up logging
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    # Validate database path
    db_path = Path(args.database)
    if not db_path.parent.exists():
        logger.error(f"Directory {db_path.parent} does not exist")
        sys.exit(1)

    # Initialize migration manager
    migration = DatabaseMigration(str(db_path))

    try:
        current_version = migration.get_schema_version()
        logger.info(f"Current database schema version: {current_version}")

        if args.check_version:
            print(f"Schema version: {current_version}")
            return

        if args.dry_run:
            # Show what would be migrated
            available_migrations = [
                (1, "Create base tables (companies, officers)"),
                (2, "Add streaming metadata and events tracking"),
                (3, "Create Company Processing State Manager tables"),
            ]

            pending = [
                (version, description)
                for version, description in available_migrations
                if version > current_version
            ]

            if pending:
                print("Pending migrations:")
                for version, description in pending:
                    print(f"  {version:03d}: {description}")
            else:
                print("No pending migrations")
            return

        # Run migrations
        logger.info(f"Starting migration process for database: {db_path}")
        migrate_database(str(db_path))

        final_version = migration.get_schema_version()
        logger.info(f"Migration completed successfully. Schema version: {final_version}")

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
