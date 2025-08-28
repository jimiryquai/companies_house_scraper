#!/usr/bin/env python3
"""Test Snov.io v2 API with domain search."""

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
COMPANIES_TO_TEST = 10  # Test with fewer companies first
BASE_URL = "https://api.snov.io/v2"
V1_BASE_URL = "https://api.snov.io/v1"  # For OAuth
CLIENT_ID = os.environ.get("SNOV_CLIENT_ID")
CLIENT_SECRET = os.environ.get("SNOV_CLIENT_SECRET")


class SnovV2Client:
    """Snov.io v2 API client."""

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
        """Get OAuth access token using v1 endpoint."""
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

    async def domain_search_start(self, domain: str):
        """Start domain search using v2 API."""
        if not self.access_token:
            return None

        url = f"{BASE_URL}/domain-search/start"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        payload = {"domain": domain}

        async with self.session.post(url, headers=headers, json=payload) as response:
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

    async def get_domain_search_results(self, task_hash: str):
        """Get results from domain search."""
        if not self.access_token:
            return None

        url = f"{BASE_URL}/domain-search/{task_hash}"
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


def get_test_companies_with_domains(limit: int = COMPANIES_TO_TEST) -> List[Dict[str, Any]]:
    """Get companies and try to generate likely domains."""
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
                AND company_name NOT LIKE '%LTD%LTD%'
                AND length(company_name) < 50
                ORDER BY RANDOM()
                LIMIT ?
            """,
                (limit * 3,),
            )  # Get more to filter

            companies = []
            for row in cursor.fetchall():
                company = dict(row)
                # Try to generate a likely domain
                name = company["company_name"].lower()

                # Clean up company name
                name = name.replace(" limited", "").replace(" ltd", "").replace(" plc", "")
                name = name.replace(" and ", "").replace(" & ", "")
                name = name.replace(" ", "").replace("-", "").replace("'", "")

                # Remove common words
                for word in ["the", "company", "group", "holdings", "services", "solutions"]:
                    name = name.replace(word, "")

                if len(name) > 3 and name.isalnum():
                    company["likely_domain"] = f"{name}.com"
                    companies.append(company)
                    if len(companies) >= limit:
                        break

            print(f"Found {len(companies)} companies with potential domains")
            return companies

    except Exception as e:
        print(f"Error getting test companies: {e}")
        return []


async def run_test():
    """Run the v2 API test."""
    print(f"Starting Snov.io v2 API Test with {COMPANIES_TO_TEST} companies")
    print(f"Client ID: {CLIENT_ID[:20]}..." if CLIENT_ID else "None")
    print("=" * 60)

    if not CLIENT_ID or not CLIENT_SECRET:
        print("ERROR: Missing credentials")
        return

    companies = get_test_companies_with_domains(COMPANIES_TO_TEST)
    if not companies:
        print("No companies found!")
        return

    success_count = 0
    error_count = 0
    tasks = []

    async with SnovV2Client() as client:
        if not client.access_token:
            print("Failed to get OAuth token")
            return

        # Start domain searches
        for i, company in enumerate(companies):
            company_number = company["company_number"]
            company_name = company["company_name"]
            domain = company["likely_domain"]

            print(f"[{i + 1}/{len(companies)}] Testing: {company_name} -> {domain}")

            try:
                result = await client.domain_search_start(domain)

                if result and result["success"]:
                    task_hash = result["data"].get("task_hash")
                    if task_hash:
                        print(f"  ✓ Search started: {task_hash}")
                        tasks.append({"company": company, "task_hash": task_hash, "domain": domain})
                        success_count += 1
                    else:
                        print(f"  ✗ No task hash: {result['data']}")
                        error_count += 1
                else:
                    print(f"  ✗ Failed: {result['status_code']} - {result['raw'][:200]}...")
                    error_count += 1

            except Exception as e:
                print(f"  ✗ Exception: {e}")
                error_count += 1

            if i < len(companies) - 1:
                await asyncio.sleep(1)  # Rate limit

        print(f"\nPhase 1 Results: {success_count} searches started, {error_count} failed")

        # Wait a bit and check results
        if tasks:
            print("\nWaiting 5 seconds before checking results...")
            await asyncio.sleep(5)

            print("Checking search results:")
            for i, task in enumerate(tasks):
                print(f"[{i + 1}/{len(tasks)}] Checking {task['company']['company_name']}")

                try:
                    result = await client.get_domain_search_results(task["task_hash"])

                    if result and result["success"]:
                        data = result["data"]
                        status = data.get("status", "unknown")
                        if status == "completed":
                            prospects = data.get("results", {}).get("prospects", [])
                            emails = data.get("results", {}).get("emails", [])
                            print(f"  ✓ Complete: {len(prospects)} prospects, {len(emails)} emails")
                        else:
                            print(f"  ⏳ Status: {status}")
                    else:
                        print(f"  ✗ Failed: {result['status_code']} - {result['raw'][:100]}...")

                except Exception as e:
                    print(f"  ✗ Exception: {e}")

                await asyncio.sleep(0.5)  # Small delay


if __name__ == "__main__":
    asyncio.run(run_test())
