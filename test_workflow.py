#!/usr/bin/env python3
"""Test the complete workflow: Fetch company → Store → Fetch officers → Store.

This simulates what the streaming service does for each event, but without
waiting for streaming events. It tests a known company directly.
"""

import logging
import os
import sys

import requests
from dotenv import load_dotenv

from src.database import CompaniesTable, Database, OfficersTable

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def test_workflow(test_company_number: str = "00000006") -> None:
    """Test complete workflow with a real company.

    Args:
        test_company_number: Company number to test (default: 00000006 - Marks & Spencer)
    """
    logger.info("=== Testing Complete Workflow ===\n")

    # Get configuration
    database_url = os.getenv("DATABASE_URL")
    companies_api_key = os.getenv("CH_COMPANIES_API_KEY")
    officers_api_key = os.getenv("CH_OFFICERS_API_KEY")

    if not database_url:
        logger.error("DATABASE_URL not set")
        sys.exit(1)
    if not companies_api_key:
        logger.error("CH_COMPANIES_API_KEY not set")
        sys.exit(1)
    if not officers_api_key:
        logger.error("CH_OFFICERS_API_KEY not set")
        sys.exit(1)

    # Initialize database
    logger.info("Step 1: Initialize database connection")
    db = Database(database_url)
    db.init_schema()
    companies_table = CompaniesTable(db)
    officers_table = OfficersTable(db)
    logger.info("✅ Database ready\n")

    try:
        # Step 2: Fetch company data
        logger.info(f"Step 2: Fetch company data for {test_company_number}")
        company_url = f"https://api.company-information.service.gov.uk/company/{test_company_number}"
        headers = {"Authorization": companies_api_key}

        response = requests.get(company_url, headers=headers, timeout=10)
        response.raise_for_status()
        company_data = response.json()

        logger.info(f"✅ Fetched: {company_data.get('company_name', 'Unknown')}")
        logger.info(f"   Status: {company_data.get('company_status', 'Unknown')}")
        logger.info(f"   Type: {company_data.get('type', 'Unknown')}\n")

        # Step 3: Upsert company to database
        logger.info("Step 3: Upsert company to database")
        company_number = companies_table.upsert_company(company_data)
        logger.info(f"✅ Company {company_number} stored in PostgreSQL\n")

        # Step 4: Fetch officers
        logger.info(f"Step 4: Fetch officers for {test_company_number}")
        officers_url = f"https://api.company-information.service.gov.uk/company/{test_company_number}/officers"
        headers = {"Authorization": officers_api_key}

        response = requests.get(officers_url, headers=headers, timeout=10)
        response.raise_for_status()
        officers_data = response.json()
        officers_list = officers_data.get("items", [])

        logger.info(f"✅ Fetched {len(officers_list)} officers")
        if officers_list:
            logger.info(f"   Sample: {officers_list[0].get('name', 'Unknown')} - {officers_list[0].get('officer_role', 'Unknown')}\n")
        else:
            logger.info("   (No officers found)\n")

        # Step 5: Upsert officers to database
        if officers_list:
            logger.info("Step 5: Upsert officers to database")
            unique_keys = officers_table.upsert_officers(test_company_number, officers_list)
            logger.info(f"✅ {len(unique_keys)} officers stored in PostgreSQL\n")
        else:
            logger.info("Step 5: No officers to store\n")

        # Step 6: Verify data in database
        logger.info("Step 6: Verify data in database")
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                # Get company
                cur.execute(
                    "SELECT company_name, company_status FROM companies WHERE company_number = %s",
                    (test_company_number,),
                )
                company_row = cur.fetchone()

                # Get officers count
                cur.execute(
                    "SELECT COUNT(*) FROM officers WHERE company_number = %s",
                    (test_company_number,),
                )
                officer_count = cur.fetchone()[0]

                logger.info(f"✅ Database verification:")
                logger.info(f"   Company: {company_row[0]}")
                logger.info(f"   Status: {company_row[1]}")
                logger.info(f"   Officers: {officer_count}\n")

        logger.info("=== ✅ Workflow test PASSED! ===")
        logger.info("\nThe complete workflow is working:")
        logger.info("  ✅ Fetch company from CH API")
        logger.info("  ✅ Store in PostgreSQL")
        logger.info("  ✅ Fetch officers from CH API")
        logger.info("  ✅ Store in PostgreSQL")
        logger.info("  ✅ Data verified in database")
        logger.info("\nReady for streaming API integration!")

    except requests.RequestException as e:
        logger.error(f"❌ API request failed: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Workflow test failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        db.close()
        logger.info("\nDatabase connection closed")


if __name__ == "__main__":
    # Test with Marks & Spencer (company 00000006)
    # This is a well-known company that should always be available
    test_workflow("00000006")
