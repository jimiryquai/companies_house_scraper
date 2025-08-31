#!/usr/bin/env python3
"""Search for officer emails using discovered domains."""

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

# Companies and their discovered domains
COMPANIES_WITH_DOMAINS = [
    {
        "company_number": "10607015",
        "company_name": "BLACK LLAMA CAPITAL LIMITED",
        "domain": "blackllamacapital.co.uk",
    },
    {"company_number": "SC460757", "company_name": "GRAND GATES LTD", "domain": "grandgates.co.uk"},
    {
        "company_number": "05077366",
        "company_name": "JORDAN CONSULTANTS LIMITED",
        "domain": "jordanconsultants.com",
    },
]


class EmailSearchClient:
    """Client for searching officer emails."""

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

    async def search_emails_by_domain_by_name(
        self, domain: str, officers: List[Dict], company_name: str
    ):
        """Search for emails by domain and officer names."""
        if not self.access_token:
            return None

        url = f"{BASE_URL}/emails-by-domain-by-name/start"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        # Format officers as rows with first_name, last_name, domain
        rows = []
        for officer in officers:
            name = officer["name"]
            # Parse "SURNAME, First Middle" format
            if ", " in name:
                parts = name.split(", ")
                last_name = parts[0].strip()
                first_name = parts[1].split()[0].strip() if len(parts) > 1 else ""
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

        payload = {"rows": rows, "webhook_url": f"{WEBHOOK_URL}"}

        print(f"📧 Searching emails on {domain} for {len(officers)} officers:")
        for officer in officers:
            print(f"   - {officer['name']} ({officer['officer_role']})")

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
                "officers_searched": len(officers),
            }


def get_officers_for_company(company_number: str) -> List[Dict[str, Any]]:
    """Get officer details from database."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT name, officer_role, appointed_on
                FROM officers
                WHERE company_number = ?
                AND name IS NOT NULL
                AND name != ''
                ORDER BY appointed_on DESC
            """,
                (company_number,),
            )

            officers = [dict(row) for row in cursor.fetchall()]
            return officers

    except Exception as e:
        print(f"Error getting officers for {company_number}: {e}")
        return []


async def search_all_officer_emails():
    """Search for emails for all officers of companies with discovered domains."""
    print("🔍 OFFICER EMAIL SEARCH")
    print("=" * 60)

    total_officers = 0
    total_searches = 0

    async with EmailSearchClient() as client:
        if not client.access_token:
            print("❌ Failed to get OAuth token")
            return

        for company in COMPANIES_WITH_DOMAINS:
            print(f"\n🏢 {company['company_name']}")
            print(f"🌐 Domain: {company['domain']}")

            # Get officers from database
            officers = get_officers_for_company(company["company_number"])

            if not officers:
                print("   ❌ No officers found")
                continue

            print(f"👥 Found {len(officers)} officers")
            total_officers += len(officers)

            # Search for emails
            result = await client.search_emails_by_domain_by_name(
                company["domain"], officers, company["company_name"]
            )

            if result and result["success"]:
                print("   ✅ Email search initiated successfully!")
                print(f"   📊 Status: {result['status_code']}")

                task_hash = result["data"].get("data", {}).get("task_hash")
                if task_hash:
                    print(f"   ⏳ Task hash: {task_hash}")
                    print(
                        f"   📧 Webhook will receive results for {result['officers_searched']} officers"
                    )

                total_searches += 1
            else:
                print("   ❌ Email search failed!")
                if result:
                    print(f"   📊 Status: {result['status_code']}")
                    print(f"   📄 Response: {result['raw'][:200]}...")

            # Brief pause between companies
            await asyncio.sleep(2)

    print("\n" + "=" * 60)
    print("📊 SEARCH SUMMARY")
    print(f"🏢 Companies processed: {len(COMPANIES_WITH_DOMAINS)}")
    print(f"👥 Total officers: {total_officers}")
    print(f"📧 Successful searches: {total_searches}")
    print("\n📞 Webhook handler will receive email results...")
    print("💾 Results will be saved to officer_emails table")


if __name__ == "__main__":
    asyncio.run(search_all_officer_emails())
