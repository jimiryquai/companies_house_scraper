#!/usr/bin/env python3
"""Test Snov.io OAuth integration with proper client."""

import asyncio
import os
import sqlite3

# Import the proper Snov.io client
import sys
import time
from typing import Any, Dict, List

from dotenv import load_dotenv

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from enrichment.snov_client import SnovioAuthError, SnovioClient, SnovioError

# Load environment variables
load_dotenv()

# Configuration
DB_PATH = "companies.db"
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "https://8888c5d2aa9d.ngrok-free.app/webhook")
COMPANIES_TO_TEST = 250
DELAY_BETWEEN_REQUESTS = 2  # seconds


def get_test_companies(limit: int = COMPANIES_TO_TEST) -> List[Dict[str, Any]]:
    """Get strike-off companies from database for testing."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT company_number, company_name
                FROM companies
                WHERE company_status_detail LIKE '%proposal to strike off%'
                AND company_name IS NOT NULL
                AND company_name != ''
                ORDER BY RANDOM()
                LIMIT ?
            """,
                (limit,),
            )

            companies = [dict(row) for row in cursor.fetchall()]
            print(f"Found {len(companies)} companies for testing")
            return companies

    except Exception as e:
        print(f"Error getting test companies: {e}")
        return []


async def run_test():
    """Run the complete test with OAuth client."""
    print(f"Starting Snov.io OAuth test with {COMPANIES_TO_TEST} companies")
    print(f"Webhook URL: {WEBHOOK_URL}")
    print("=" * 60)

    # Check API credentials
    client_id = os.environ.get("SNOV_CLIENT_ID")
    client_secret = os.environ.get("SNOV_CLIENT_SECRET")

    if not client_id or not client_secret:
        print("ERROR: Missing Snov.io API credentials")
        print("Please set SNOV_CLIENT_ID and SNOV_CLIENT_SECRET in .env file")
        return

    # Get test companies
    companies = get_test_companies(COMPANIES_TO_TEST)
    if not companies:
        print("No companies found for testing!")
        return

    start_time = time.time()
    processed = 0
    success_count = 0
    error_count = 0

    # Initialize Snov.io client
    async with SnovioClient(client_id=client_id, client_secret=client_secret) as client:
        print("✓ OAuth client initialized successfully")

        # Process companies
        for i, company in enumerate(companies):
            company_number = company["company_number"]
            company_name = company["company_name"]

            print(f"[{i + 1}/{len(companies)}] Testing: {company_name} ({company_number})")

            try:
                # Create webhook URL with company context
                webhook_with_context = (
                    f"{WEBHOOK_URL}?company_number={company_number}&company_name={company_name}"
                )

                # Call domain search with OAuth client
                result = await client.domain_search(
                    company_name=company_name, limit=5, webhook_url=webhook_with_context
                )

                print(f"  ✓ API call successful for {company_name}: {result}")
                success_count += 1

            except SnovioAuthError as e:
                print(f"  ✗ Auth error for {company_name}: {e}")
                error_count += 1

            except SnovioError as e:
                print(f"  ✗ API error for {company_name}: {e}")
                error_count += 1

            except Exception as e:
                print(f"  ✗ Unexpected error for {company_name}: {e}")
                error_count += 1

            processed += 1

            # Progress update every 10 companies
            if (i + 1) % 10 == 0:
                elapsed = time.time() - start_time
                rate = processed / elapsed if elapsed > 0 else 0
                remaining = len(companies) - processed
                eta = remaining / rate if rate > 0 else 0

                print(f"Progress: {processed}/{len(companies)} processed")
                print(f"Success: {success_count}, Errors: {error_count}")
                print(f"Rate: {rate:.1f} req/s, ETA: {eta / 60:.1f}m")

            # Delay between requests
            if i < len(companies) - 1:
                await asyncio.sleep(DELAY_BETWEEN_REQUESTS)

    # Final summary
    total_time = time.time() - start_time
    print("\n" + "=" * 60)
    print("TEST COMPLETE")
    print(f"Total companies processed: {processed}")
    print(f"Successful API calls: {success_count}")
    print(f"Failed API calls: {error_count}")
    print(f"Success rate: {(success_count / processed) * 100:.1f}%")
    print(f"Total time: {total_time / 60:.1f} minutes")
    print(f"Average rate: {processed / total_time:.1f} requests/second")
    print("\nWebhook results will arrive asynchronously.")
    print("Check webhook handler logs for incoming results.")


if __name__ == "__main__":
    print("Snov.io OAuth Test Script")
    print("=" * 40)
    print(f"Webhook URL configured: {WEBHOOK_URL}")
    print(f"Processing {COMPANIES_TO_TEST} companies with OAuth client...")
    print("")

    asyncio.run(run_test())
