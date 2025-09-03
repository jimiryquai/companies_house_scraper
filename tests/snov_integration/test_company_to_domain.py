#!/usr/bin/env python3
"""Test Snov.io company name to domain API."""

import asyncio
import json
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
COMPANIES_TO_TEST = 10
BASE_URL = "https://api.snov.io/v2"
V1_BASE_URL = "https://api.snov.io/v1"
CLIENT_ID = os.environ.get("SNOV_CLIENT_ID")
CLIENT_SECRET = os.environ.get("SNOV_CLIENT_SECRET")


class SnovCompanyDomainClient:
    """Snov.io company name to domain client."""

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
        token_url = f"{V1_BASE_URL}/oauth/access_token"

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

    async def company_domain_by_name_start(self, company_names: List[str], webhook_url: str = None):
        """Find domains from company names."""
        if not self.access_token:
            return None

        url = f"{BASE_URL}/company-domain-by-name/start"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        payload = {
            "names": company_names[:10]  # Max 10 at once
        }

        if webhook_url:
            payload["webhook_url"] = webhook_url

        async with self.session.post(url, headers=headers, json=payload) as response:
            result_text = await response.text()
            try:
                result_json = json.loads(result_text)
            except:
                result_json = {"raw_response": result_text}

            return {
                "status_code": response.status,
                "data": result_json,
                "success": response.status in [200, 202],
                "raw": result_text,
            }

    async def get_company_domain_results(self, task_hash: str):
        """Get results from company domain search."""
        if not self.access_token:
            return None

        url = f"{BASE_URL}/company-domain-by-name/result/{task_hash}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        async with self.session.get(url, headers=headers) as response:
            result_text = await response.text()
            try:
                result_json = json.loads(result_text)
            except:
                result_json = {"raw_response": result_text}

            return {
                "status_code": response.status,
                "data": result_json,
                "success": response.status == 200,
                "raw": result_text,
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
    """Run the company name to domain test."""
    print("Starting Snov.io Company Name to Domain Test")
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

    # Extract company names
    company_names = [c["company_name"] for c in companies]

    async with SnovCompanyDomainClient() as client:
        if not client.access_token:
            print("Failed to get OAuth token")
            return

        print(f"Testing {len(company_names)} company names:")
        for i, name in enumerate(company_names):
            print(f"  [{i + 1}] {name}")

        # Create webhook URL with context
        webhook_with_context = f"{WEBHOOK_URL}?batch_id=test_batch_1"

        try:
            result = await client.company_domain_by_name_start(
                company_names=company_names, webhook_url=webhook_with_context
            )

            if result and result["success"]:
                print("\n✓ Batch search started successfully!")
                print(f"Status: {result['status_code']}")
                print(f"Response: {json.dumps(result['data'], indent=2)}")

                # Check if we got a task hash
                task_hash = result["data"].get("data", {}).get("task_hash")
                if task_hash:
                    print(f"\n⏳ Task hash: {task_hash}")
                    print("Waiting 10 seconds before checking results...")
                    await asyncio.sleep(10)

                    # Check results
                    results = await client.get_company_domain_results(task_hash)
                    if results:
                        print("\n📊 Results:")
                        print(f"Status: {results['status_code']}")
                        if results["success"]:
                            data = results["data"]
                            if "results" in data:
                                for result_item in data["results"]:
                                    company_name = result_item.get("company_name", "Unknown")
                                    domain = result_item.get("domain", "Not found")
                                    print(f"  {company_name} -> {domain}")
                            else:
                                print(f"Raw data: {json.dumps(data, indent=2)}")
                        else:
                            print(f"Failed to get results: {results['raw']}")
                else:
                    print("No task hash returned, likely immediate results")

            else:
                print("\n✗ Batch search failed!")
                print(f"Status: {result['status_code']}")
                print(f"Response: {result['raw']}")

        except Exception as e:
            print(f"✗ Exception: {e}")


if __name__ == "__main__":
    asyncio.run(run_test())
