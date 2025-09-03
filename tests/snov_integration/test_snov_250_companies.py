#!/usr/bin/env python3
"""Test script to process 250 strike-off companies with Snov.io API.
Uses existing database and Snov.io client for domain discovery.
"""

import asyncio
import os
import sqlite3
import sys
import time
from typing import Any, Dict, List

# Add src to path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from enrichment.credit_manager import CreditManager
from enrichment.snov_client import SnovioClient

# Configuration
DB_PATH = "companies.db"
WEBHOOK_URL = "REPLACE_WITH_NGROK_URL"  # Will be updated when ngrok starts
COMPANIES_TO_TEST = 250
DELAY_BETWEEN_REQUESTS = 2  # seconds
BATCH_SIZE = 10  # Process in batches to avoid overwhelming


class TestRunner:
    """Manages the test execution and progress tracking."""

    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        self.snov_client = SnovioClient()
        self.credit_manager = CreditManager()
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0

    async def get_test_companies(self, limit: int = COMPANIES_TO_TEST) -> List[Dict[str, Any]]:
        """Get strike-off companies from database for testing."""
        try:
            with sqlite3.connect(DB_PATH) as conn:
                conn.row_factory = sqlite3.Row  # Enable dict-like access
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

    async def test_domain_search(self, company: Dict[str, Any]) -> bool:
        """Test domain search for a single company."""
        try:
            company_number = company["company_number"]
            company_name = company["company_name"]

            print(
                f"[{self.processed_count + 1}/{COMPANIES_TO_TEST}] Testing: {company_name} ({company_number})"
            )

            # Create request with webhook callback
            success = await self.snov_client.search_domain(
                company_name=company_name,
                webhook_url=self.webhook_url,
                company_number=company_number,  # Pass for tracking
            )

            if success:
                self.success_count += 1
                print(f"  ✓ Domain search initiated for {company_name}")
            else:
                self.error_count += 1
                print(f"  ✗ Failed to initiate domain search for {company_name}")

            return success

        except Exception as e:
            self.error_count += 1
            print(f"  ✗ Error processing {company.get('company_name', 'unknown')}: {e}")
            return False

    async def run_test_batch(self, companies: List[Dict[str, Any]]) -> None:
        """Process a batch of companies."""
        tasks = []

        for company in companies:
            task = self.test_domain_search(company)
            tasks.append(task)

            # Add delay between requests to be respectful
            if len(tasks) < len(companies):
                await asyncio.sleep(DELAY_BETWEEN_REQUESTS)

        # Wait for all tasks in this batch to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Count results
        for result in results:
            self.processed_count += 1
            if isinstance(result, Exception):
                self.error_count += 1
                print(f"  ✗ Batch error: {result}")

    async def run_full_test(self) -> None:
        """Run the complete test on 250 companies."""
        print(f"Starting Snov.io test with {COMPANIES_TO_TEST} companies")
        print(f"Webhook URL: {self.webhook_url}")
        print(f"Delay between requests: {DELAY_BETWEEN_REQUESTS}s")
        print("=" * 60)

        # Get test companies
        companies = await self.get_test_companies(COMPANIES_TO_TEST)
        if not companies:
            print("No companies found for testing!")
            return

        start_time = time.time()

        # Process in batches
        for i in range(0, len(companies), BATCH_SIZE):
            batch = companies[i : i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            total_batches = (len(companies) + BATCH_SIZE - 1) // BATCH_SIZE

            print(f"\nProcessing batch {batch_num}/{total_batches} ({len(batch)} companies)")
            await self.run_test_batch(batch)

            # Progress update
            elapsed = time.time() - start_time
            rate = self.processed_count / elapsed if elapsed > 0 else 0
            remaining = len(companies) - self.processed_count
            eta = remaining / rate if rate > 0 else 0

            print(f"Progress: {self.processed_count}/{len(companies)} processed")
            print(f"Success: {self.success_count}, Errors: {self.error_count}")
            print(f"Rate: {rate:.1f} req/s, ETA: {eta / 60:.1f}m")

            # Short break between batches
            if i + BATCH_SIZE < len(companies):
                await asyncio.sleep(5)

        # Final summary
        total_time = time.time() - start_time
        print("\n" + "=" * 60)
        print("TEST COMPLETE")
        print(f"Total companies processed: {self.processed_count}")
        print(f"Successful API calls: {self.success_count}")
        print(f"Failed API calls: {self.error_count}")
        print(f"Success rate: {(self.success_count / self.processed_count) * 100:.1f}%")
        print(f"Total time: {total_time / 60:.1f} minutes")
        print(f"Average rate: {self.processed_count / total_time:.1f} requests/second")
        print("\nWebhook results will arrive asynchronously.")
        print("Check the webhook handler logs and use /stats endpoint for real-time updates.")


async def main():
    """Main entry point."""
    print("Snov.io Test Script")
    print("=" * 40)

    # Check if webhook URL is provided
    webhook_url = input(
        "Enter your ngrok webhook URL (e.g., https://abc123.ngrok-free.app/webhook): "
    ).strip()
    if not webhook_url:
        print("Error: Webhook URL is required!")
        print("Start ngrok first: ./ngrok http 5000")
        return

    if not webhook_url.startswith("https://"):
        print("Warning: Webhook URL should start with https://")

    # Confirm before starting
    print(f"\nAbout to test {COMPANIES_TO_TEST} companies with webhook: {webhook_url}")
    confirm = input("Continue? (y/N): ").strip().lower()
    if confirm not in ["y", "yes"]:
        print("Test cancelled.")
        return

    # Run the test
    runner = TestRunner(webhook_url)
    try:
        await runner.run_full_test()
    except KeyboardInterrupt:
        print("\nTest interrupted by user.")
        print(f"Processed {runner.processed_count} companies before interruption.")
    except Exception as e:
        print(f"\nTest failed with error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
