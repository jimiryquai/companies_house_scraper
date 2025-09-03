#!/usr/bin/env python3
"""Test Snov.io with correct API format from documentation."""

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
COMPANIES_TO_TEST = 5  # Start with fewer
BASE_URL = "https://api.snov.io/v2"
V1_BASE_URL = "https://api.snov.io/v1"
CLIENT_ID = os.environ.get("SNOV_CLIENT_ID")
CLIENT_SECRET = os.environ.get("SNOV_CLIENT_SECRET")


class SnovCorrectClient:
    """Snov.io client with correct API format."""

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
                print("✓ OAuth token obtained")
                return True
            print(f"✗ OAuth failed: {response.status} - {await response.text()}")
            return False

    async def company_domain_by_name_start(self, company_names: List[str], webhook_url: str = None):
        """Find domains from company names - CORRECT FORMAT."""
        if not self.access_token:
            return None

        url = f"{BASE_URL}/company-domain-by-name/start"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        # Correct format according to documentation
        payload = {}
        for i, name in enumerate(company_names[:10]):
            payload[f"names[{i}]"] = name

        if webhook_url:
            payload["webhook_url"] = webhook_url

        print(f"Payload: {payload}")

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
        """Get results using correct endpoint."""
        if not self.access_token:
            return None

        # Correct endpoint from documentation
        url = f"{BASE_URL}/company-domain-by-name/result?task_hash={task_hash}"
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
    """Get test companies."""
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
    """Run test with correct API format."""
    print("Testing Snov.io with CORRECT API format")
    print("=" * 50)

    if not CLIENT_ID or not CLIENT_SECRET:
        print("ERROR: Missing credentials")
        return

    companies = get_test_companies(COMPANIES_TO_TEST)
    if not companies:
        print("No companies found!")
        return

    company_names = [c["company_name"] for c in companies]

    async with SnovCorrectClient() as client:
        if not client.access_token:
            print("Failed to get OAuth token")
            return

        print(f"Testing {len(company_names)} companies:")
        for i, name in enumerate(company_names):
            print(f"  [{i + 1}] {name}")

        webhook_with_context = f"{WEBHOOK_URL}?test=correct_format"

        try:
            result = await client.company_domain_by_name_start(
                company_names=company_names, webhook_url=webhook_with_context
            )

            print(f"\nResponse Status: {result['status_code']}")
            print(f"Response: {json.dumps(result['data'], indent=2)}")

            if result and result["success"]:
                task_hash = result["data"].get("data", {}).get("task_hash")
                if task_hash:
                    print(f"\n⏳ Task hash: {task_hash}")
                    print("Waiting 15 seconds...")
                    await asyncio.sleep(15)

                    results = await client.get_company_domain_results(task_hash)
                    print(f"\nResults Status: {results['status_code']}")
                    print(f"Results: {json.dumps(results['data'], indent=2)}")

                    if results["success"]:
                        data = results["data"]
                        status = data.get("status", "unknown")
                        print(f"\nSearch Status: {status}")

                        if status == "completed":
                            results_data = data.get("data", [])
                            print("\n🎯 DOMAIN RESULTS:")
                            for item in results_data:
                                name = item.get("name", "Unknown")
                                result_data = item.get("result", {})
                                domain = result_data.get("domain", "Not found")
                                print(f"  {name} -> {domain}")
                        else:
                            print(f"Status: {status} (still processing)")
                else:
                    print("No task hash found")
            else:
                print("Request failed")

        except Exception as e:
            print(f"Exception: {e}")


if __name__ == "__main__":
    asyncio.run(run_test())
