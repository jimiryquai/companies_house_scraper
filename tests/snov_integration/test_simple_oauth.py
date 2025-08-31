#!/usr/bin/env python3
"""Simple OAuth test for Snov.io without complex imports."""

import asyncio
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List

import aiohttp
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
DB_PATH = "companies.db"
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "https://8888c5d2aa9d.ngrok-free.app/webhook")
COMPANIES_TO_TEST = 10  # Smaller test first
BASE_URL = "https://api.snov.io/v1"
CLIENT_ID = os.environ.get("SNOV_CLIENT_ID")
CLIENT_SECRET = os.environ.get("SNOV_CLIENT_SECRET")


class SimpleOAuthClient:
    """Simple OAuth client for testing."""

    def __init__(self):
        self.access_token = None
        self.token_expires_at = None
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        await self._get_access_token()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _get_access_token(self):
        """Get OAuth access token."""
        token_url = f"{BASE_URL}/oauth/access_token"

        payload = {
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        }

        async with self.session.post(token_url, json=payload) as response:
            if response.status == 200:
                data = await response.json()
                self.access_token = data.get("access_token")
                expires_in = data.get("expires_in", 3600)
                self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)
                print(f"✓ OAuth token obtained, expires at {self.token_expires_at}")
                return True
            print(f"✗ OAuth failed: {response.status} - {await response.text()}")
            return False

    async def domain_search(self, company_name: str, webhook_url: str = None):
        """Search for domains."""
        if not self.access_token:
            return None

        url = f"{BASE_URL}/domain-search"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        params = {"name": company_name, "limit": 5}

        if webhook_url:
            params["webhook_url"] = webhook_url

        async with self.session.get(url, headers=headers, params=params) as response:
            result = await response.text()
            return {
                "status_code": response.status,
                "data": result,
                "success": response.status == 200,
            }


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
    """Run the test."""
    print(f"Starting Simple OAuth Test with {COMPANIES_TO_TEST} companies")
    print(f"Webhook URL: {WEBHOOK_URL}")
    print(f"Client ID: {CLIENT_ID[:20]}..." if CLIENT_ID else "None")
    print("=" * 60)

    if not CLIENT_ID or not CLIENT_SECRET:
        print("ERROR: Missing credentials")
        return

    companies = get_test_companies(COMPANIES_TO_TEST)
    if not companies:
        print("No companies found!")
        return

    success_count = 0
    error_count = 0

    async with SimpleOAuthClient() as client:
        if not client.access_token:
            print("Failed to get OAuth token")
            return

        for i, company in enumerate(companies):
            company_number = company["company_number"]
            company_name = company["company_name"]

            print(f"[{i + 1}/{len(companies)}] Testing: {company_name}")

            webhook_with_context = (
                f"{WEBHOOK_URL}?company_number={company_number}&company_name={company_name}"
            )

            try:
                result = await client.domain_search(company_name, webhook_with_context)

                if result and result["success"]:
                    print(f"  ✓ Success: {result['status_code']}")
                    success_count += 1
                else:
                    print(f"  ✗ Failed: {result['status_code']} - {result['data'][:200]}...")
                    error_count += 1

            except Exception as e:
                print(f"  ✗ Exception: {e}")
                error_count += 1

            if i < len(companies) - 1:
                await asyncio.sleep(2)  # Rate limit

    print(f"\nResults: {success_count} successful, {error_count} failed")


if __name__ == "__main__":
    asyncio.run(run_test())
