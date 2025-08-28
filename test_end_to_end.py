#!/usr/bin/env python3
"""End-to-end test: Company Name -> Domain -> Officers -> Emails"""

import asyncio
import json
import os
import sqlite3
from typing import Any, Dict, List

import aiohttp
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
DB_PATH = "companies.db"
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "https://8888c5d2aa9d.ngrok-free.app/webhook")
BASE_URL = "https://api.snov.io/v2"
V1_BASE_URL = "https://api.snov.io/v1"
CLIENT_ID = os.environ.get("SNOV_CLIENT_ID")
CLIENT_SECRET = os.environ.get("SNOV_CLIENT_SECRET")


class EndToEndClient:
    """Complete end-to-end Snov.io client."""

    def __init__(self):
        self.access_token = None
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
                print("✓ OAuth token obtained")
                return True
            print(f"✗ OAuth failed: {response.status}")
            return False

    async def emails_by_domain_by_name(self, domain: str, names: List[str]):
        """Find emails by domain and names."""
        if not self.access_token:
            return None

        url = f"{BASE_URL}/emails-by-domain-by-name/start"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        # Format names as rows with first_name, last_name, domain
        rows = []
        for name in names:
            # Parse "SURNAME, First Middle" format
            parts = name.split(", ")
            if len(parts) >= 2:
                last_name = parts[0].strip()
                first_name = parts[1].split()[0].strip()  # Take first word of first names
            else:
                # Fallback: split on space
                name_parts = name.split()
                if len(name_parts) >= 2:
                    first_name = name_parts[0]
                    last_name = name_parts[-1]
                else:
                    first_name = name
                    last_name = "Unknown"

            rows.append({"first_name": first_name, "last_name": last_name, "domain": domain})

        payload = {"rows": rows, "webhook_url": f"{WEBHOOK_URL}?step=emails&domain={domain}"}

        print(f"📧 Looking for emails on {domain} for {len(names)} officers:")
        for name in names:
            print(f"   - {name}")

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


def get_officers_for_company(company_number: str) -> List[str]:
    """Get officer names from database using company number."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT name
                FROM officers
                WHERE company_number = ?
                AND name IS NOT NULL
                AND name != ''
            """,
                (company_number,),
            )

            officers = [row["name"] for row in cursor.fetchall()]
            return officers

    except Exception as e:
        print(f"Error getting officers for {company_number}: {e}")
        return []


def get_company_by_name(company_name: str) -> Dict[str, Any]:
    """Get company details by name."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT company_number, company_name
                FROM companies
                WHERE company_name = ?
            """,
                (company_name,),
            )

            row = cursor.fetchone()
            if row:
                return dict(row)
            return {}

    except Exception as e:
        print(f"Error getting company {company_name}: {e}")
        return {}


async def run_end_to_end_test():
    """Run complete end-to-end test."""
    print("🔄 END-TO-END TEST: Company → Domain → Officers → Emails")
    print("=" * 60)

    # We know BLACK LLAMA CAPITAL LIMITED has domain blackllamacapital.co.uk
    company_name = "BLACK LLAMA CAPITAL LIMITED"
    domain = "blackllamacapital.co.uk"

    print(f"🏢 Company: {company_name}")
    print(f"🌐 Domain: {domain}")

    # Get company number
    company = get_company_by_name(company_name)
    if not company:
        print("❌ Company not found in database")
        return

    company_number = company["company_number"]
    print(f"🔢 Company Number: {company_number}")

    # Get officers from database
    officers = get_officers_for_company(company_number)
    print(f"👥 Found {len(officers)} officers:")
    for i, officer in enumerate(officers):
        print(f"   [{i + 1}] {officer}")

    if not officers:
        print("❌ No officers found")
        return

    # Now find emails using Snov.io
    async with EndToEndClient() as client:
        if not client.access_token:
            print("❌ Failed to get OAuth token")
            return

        result = await client.emails_by_domain_by_name(domain, officers)

        if result and result["success"]:
            print("\n✅ Email search started successfully!")
            print(f"Status: {result['status_code']}")
            print(f"Response: {json.dumps(result['data'], indent=2)}")

            task_hash = result["data"].get("data", {}).get("task_hash")
            if task_hash:
                print(f"\n⏳ Task hash: {task_hash}")
                print("📧 Webhook will receive email results...")

        else:
            print("\n❌ Email search failed!")
            print(f"Status: {result['status_code']}")
            print(f"Response: {result['raw']}")


if __name__ == "__main__":
    asyncio.run(run_end_to_end_test())
