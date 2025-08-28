#!/usr/bin/env python3
"""Automated test script to call Snov.io API with 250 companies."""

import os
import sqlite3
import time
from typing import Any, Dict, List

import requests
import yaml
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


# Load configuration
def load_config():
    """Load configuration from config.yaml and environment."""
    with open("config.yaml") as f:
        config = yaml.safe_load(f)

    # Expand environment variables in config
    snov_config = config["snov_io"]
    snov_config["client_id"] = os.environ.get("SNOV_CLIENT_ID")
    snov_config["client_secret"] = os.environ.get("SNOV_CLIENT_SECRET")

    return config


# Configuration
config = load_config()
DB_PATH = "companies.db"
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "https://8888c5d2aa9d.ngrok-free.app/webhook")
COMPANIES_TO_TEST = 250
DELAY_BETWEEN_REQUESTS = 2  # seconds

# Snov.io API configuration from config
SNOV_API_URL = config["snov_io"]["api_base_url"]
SNOV_USER_ID = config["snov_io"]["client_id"]
SNOV_SECRET = config["snov_io"]["client_secret"]


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


def call_snov_domain_search(company_name: str, company_number: str) -> bool:
    """Call Snov.io domain search API."""
    try:
        # Snov.io domain search API call
        payload = {
            "userId": SNOV_USER_ID,
            "secret": SNOV_SECRET,
            "name": company_name,
            "callback": f"{WEBHOOK_URL}?company_number={company_number}&company_name={company_name}",
        }

        response = requests.post(f"{SNOV_API_URL}/get-company-domain", json=payload, timeout=30)

        if response.status_code == 200:
            result = response.json()
            print(f"  ✓ API call successful for {company_name}: {result}")
            return True
        print(f"  ✗ API call failed for {company_name}: {response.status_code} - {response.text}")
        return False

    except Exception as e:
        print(f"  ✗ Error calling API for {company_name}: {e}")
        return False


def run_test():
    """Run the complete test."""
    print(f"Starting Snov.io test with {COMPANIES_TO_TEST} companies")
    print(f"Webhook URL: {WEBHOOK_URL}")
    print("=" * 60)

    # Check API credentials
    if not SNOV_USER_ID or not SNOV_SECRET:
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

    # Process companies
    for i, company in enumerate(companies):
        company_number = company["company_number"]
        company_name = company["company_name"]

        print(f"[{i + 1}/{len(companies)}] Testing: {company_name} ({company_number})")

        success = call_snov_domain_search(company_name, company_number)

        processed += 1
        if success:
            success_count += 1
        else:
            error_count += 1

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
            time.sleep(DELAY_BETWEEN_REQUESTS)

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
    print("Snov.io Automated Test Script")
    print("=" * 40)
    print(f"Webhook URL configured: {WEBHOOK_URL}")
    print(f"Processing {COMPANIES_TO_TEST} companies automatically...")
    print("")

    run_test()
