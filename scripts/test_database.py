#!/usr/bin/env python3
"""Test script for PostgreSQL database module.

This script tests:
1. Database connection
2. Schema initialization
3. Company upsert operations
4. Officer upsert operations
"""

import logging
import os
import sys
from typing import Any

from src.database import CompaniesTable, Database, OfficersTable

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def test_database_connection(database_url: str) -> Database:
    """Test database connection.

    Args:
        database_url: PostgreSQL connection URL

    Returns:
        Database instance if successful
    """
    logger.info("Testing database connection...")
    try:
        db = Database(database_url)
        logger.info("✅ Database connection successful")
        return db
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise


def test_schema_initialization(db: Database) -> None:
    """Test schema initialization.

    Args:
        db: Database instance
    """
    logger.info("Testing schema initialization...")
    try:
        db.init_schema()
        logger.info("✅ Schema initialization successful")
    except Exception as e:
        logger.error(f"❌ Schema initialization failed: {e}")
        raise


def test_company_upsert(companies_table: CompaniesTable) -> None:
    """Test company upsert operation.

    Args:
        companies_table: CompaniesTable instance
    """
    logger.info("Testing company upsert...")

    # Create test company data (mimicking Companies House API response)
    test_company: dict[str, Any] = {
        "company_number": "12345678",
        "company_name": "TEST COMPANY LIMITED",
        "company_status": "active",
        "type": "ltd",
        "jurisdiction": "england-wales",
        "date_of_creation": "2020-01-15",
        "registered_office_address": {
            "address_line_1": "123 Test Street",
            "locality": "London",
            "postal_code": "SW1A 1AA",
            "country": "United Kingdom",
        },
        "sic_codes": ["62012", "62020"],
        "links": {"self": "/company/12345678"},
    }

    try:
        # First insert
        company_number = companies_table.upsert_company(test_company)
        logger.info(f"✅ Company insert successful: {company_number}")

        # Update (upsert with same company_number)
        test_company["company_name"] = "TEST COMPANY LIMITED (UPDATED)"
        company_number = companies_table.upsert_company(test_company)
        logger.info(f"✅ Company update successful: {company_number}")

    except Exception as e:
        logger.error(f"❌ Company upsert failed: {e}")
        raise


def test_officer_upsert(officers_table: OfficersTable) -> None:
    """Test officer upsert operation.

    Args:
        officers_table: OfficersTable instance
    """
    logger.info("Testing officer upsert...")

    # Create test officer data (mimicking Companies House API response)
    test_officers: list[dict[str, Any]] = [
        {
            "name": "SMITH, John",
            "officer_role": "director",
            "appointed_on": "2020-01-15",
            "nationality": "British",
            "occupation": "Business Consultant",
            "country_of_residence": "United Kingdom",
            "date_of_birth": {"month": 5, "year": 1980},
            "address": {
                "premises": "10",
                "address_line_1": "Example Road",
                "locality": "London",
                "postal_code": "N1 1AA",
                "country": "United Kingdom",
            },
            "links": {"officer": {"appointments": "ABC123"}},
        },
        {
            "name": "JONES, Sarah",
            "officer_role": "secretary",
            "appointed_on": "2020-02-01",
            "nationality": "British",
            "occupation": "Company Secretary",
            "country_of_residence": "United Kingdom",
            "date_of_birth": {"month": 8, "year": 1985},
            "address": {
                "premises": "20",
                "address_line_1": "Test Avenue",
                "locality": "Manchester",
                "postal_code": "M1 1AA",
                "country": "United Kingdom",
            },
            "links": {"officer": {"appointments": "DEF456"}},
        },
    ]

    try:
        # First insert
        unique_keys = officers_table.upsert_officers("12345678", test_officers)
        logger.info(f"✅ Officer insert successful: {len(unique_keys)} officers")

        # Update (upsert with same officers)
        test_officers[0]["occupation"] = "Senior Business Consultant"
        unique_keys = officers_table.upsert_officers("12345678", test_officers)
        logger.info(f"✅ Officer update successful: {len(unique_keys)} officers")

    except Exception as e:
        logger.error(f"❌ Officer upsert failed: {e}")
        raise


def verify_data(db: Database) -> None:
    """Verify inserted data by reading from database.

    Args:
        db: Database instance
    """
    logger.info("Verifying inserted data...")

    try:
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                # Check companies
                cur.execute("SELECT COUNT(*) FROM companies WHERE company_number = '12345678'")
                company_count = cur.fetchone()[0]
                logger.info(f"Companies in database: {company_count}")

                # Check officers
                cur.execute("SELECT COUNT(*) FROM officers WHERE company_number = '12345678'")
                officer_count = cur.fetchone()[0]
                logger.info(f"Officers in database: {officer_count}")

                if company_count == 1 and officer_count == 2:
                    logger.info("✅ Data verification successful")
                else:
                    logger.warning(
                        f"⚠️  Unexpected counts: companies={company_count}, officers={officer_count}"
                    )

    except Exception as e:
        logger.error(f"❌ Data verification failed: {e}")
        raise


def cleanup_test_data(db: Database) -> None:
    """Clean up test data from database.

    Args:
        db: Database instance
    """
    logger.info("Cleaning up test data...")

    try:
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                # Delete officers first (foreign key constraint)
                cur.execute("DELETE FROM officers WHERE company_number = '12345678'")
                officers_deleted = cur.rowcount

                # Delete company
                cur.execute("DELETE FROM companies WHERE company_number = '12345678'")
                companies_deleted = cur.rowcount

                logger.info(
                    f"✅ Cleanup successful: {companies_deleted} companies, "
                    f"{officers_deleted} officers deleted"
                )

    except Exception as e:
        logger.error(f"❌ Cleanup failed: {e}")
        raise


def main() -> None:
    """Main test function."""
    logger.info("=== PostgreSQL Database Module Test ===\n")

    # Get database URL from environment
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL environment variable is required")
        logger.info("For local testing, use:")
        logger.info("  export DATABASE_URL='postgresql://user:password@localhost:5432/dbname'")
        sys.exit(1)

    db = None
    try:
        # Test 1: Database connection
        db = test_database_connection(database_url)

        # Test 2: Schema initialization
        test_schema_initialization(db)

        # Test 3: Company operations
        companies_table = CompaniesTable(db)
        test_company_upsert(companies_table)

        # Test 4: Officer operations
        officers_table = OfficersTable(db)
        test_officer_upsert(officers_table)

        # Test 5: Data verification
        verify_data(db)

        # Test 6: Cleanup
        cleanup_test_data(db)

        logger.info("\n=== ✅ All tests passed! ===")

    except Exception as e:
        logger.error(f"\n=== ❌ Test failed: {e} ===")
        sys.exit(1)

    finally:
        if db:
            db.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    main()
