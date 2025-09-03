#!/usr/bin/env python3
"""Aggressive test to exhaust 500 Snov.io credits with end-to-end flow."""

import asyncio
import json
import os
import sqlite3
import time
from typing import Any, Dict, List

import aiohttp
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
DB_PATH = "companies.db"
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "https://8888c5d2aa9d.ngrok-free.app/webhook")
BLOCK_SIZE = 10
TARGET_CREDITS = 500  # Exhaust all credits
BASE_URL = "https://api.snov.io/v2"
V1_BASE_URL = "https://api.snov.io/v1"
CLIENT_ID = os.environ.get("SNOV_CLIENT_ID")
CLIENT_SECRET = os.environ.get("SNOV_CLIENT_SECRET")


class CreditExhaustClient:
    """Client to exhaust all Snov.io credits."""

    def __init__(self):
        self.access_token = None
        self.session = None
        self.credits_used = 0
        self.companies_processed = 0
        self.domains_found = 0
        self.emails_searched = 0

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

    async def company_domain_search(self, company_names: List[str], block_id: str):
        """Search for company domains."""
        if not self.access_token:
            return None

        url = f"{BASE_URL}/company-domain-by-name/start"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        payload = {
            "names": company_names,
            "webhook_url": f"{WEBHOOK_URL}?type=domains&block={block_id}",
        }

        async with self.session.post(url, headers=headers, json=payload) as response:
            result_text = await response.text()
            try:
                result_json = json.loads(result_text)
            except:
                result_json = {"raw_response": result_text}

            if response.status in [200, 202]:
                self.companies_processed += len(company_names)
                # Each company costs 1 credit if domain found (we'll estimate 30% success rate)
                estimated_credits = int(len(company_names) * 0.3)
                self.credits_used += estimated_credits

                return {
                    "success": True,
                    "task_hash": result_json.get("data", {}).get("task_hash"),
                    "companies": len(company_names),
                }
            return {"success": False, "error": result_text}

    async def email_search(self, domain: str, officers: List[str], company_name: str):
        """Search for emails by domain and officer names."""
        if not self.access_token:
            return None

        url = f"{BASE_URL}/emails-by-domain-by-name/start"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        # Format officers as rows
        rows = []
        for officer in officers[:5]:  # Max 5 officers per request
            # Parse "SURNAME, First Middle" format
            parts = officer.split(", ")
            if len(parts) >= 2:
                last_name = parts[0].strip()
                first_name = parts[1].split()[0].strip()
            else:
                name_parts = officer.split()
                if len(name_parts) >= 2:
                    first_name = name_parts[0]
                    last_name = name_parts[-1]
                else:
                    first_name = officer
                    last_name = "Unknown"

            rows.append({"first_name": first_name, "last_name": last_name, "domain": domain})

        payload = {
            "rows": rows,
            "webhook_url": f"{WEBHOOK_URL}?type=emails&company={company_name}&domain={domain}",
        }

        async with self.session.post(url, headers=headers, json=payload) as response:
            result_text = await response.text()
            try:
                result_json = json.loads(result_text)
            except:
                result_json = {"raw_response": result_text}

            if response.status in [200, 202]:
                self.emails_searched += len(rows)
                # Each email search costs 1 credit
                self.credits_used += len(rows)

                return {
                    "success": True,
                    "task_hash": result_json.get("data", {}).get("task_hash"),
                    "officers": len(rows),
                }
            return {"success": False, "error": result_text}


def get_companies_batch(offset: int, limit: int = BLOCK_SIZE) -> List[Dict[str, Any]]:
    """Get batch of companies from database."""
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
                LIMIT ? OFFSET ?
            """,
                (limit, offset),
            )

            companies = [dict(row) for row in cursor.fetchall()]
            return companies

    except Exception as e:
        print(f"Error getting companies: {e}")
        return []


def get_officers_for_company(company_number: str) -> List[str]:
    """Get officer names from database."""
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
                LIMIT 5
            """,
                (company_number,),
            )

            officers = [row["name"] for row in cursor.fetchall()]
            return officers

    except Exception:
        return []


async def run_credit_exhaustion():
    """Run aggressive test to exhaust all 500 credits."""
    print("🔥 EXHAUSTING 500 SNOV.IO CREDITS")
    print(f"Target: {TARGET_CREDITS} credits")
    print("=" * 60)

    start_time = time.time()
    offset = 0
    block_number = 1

    # Track some companies with domains for email testing
    companies_with_domains = [
        {
            "name": "BLACK LLAMA CAPITAL LIMITED",
            "domain": "blackllamacapital.co.uk",
            "number": "10607015",
        },
        {
            "name": "GRAND GATES LTD",
            "domain": "grandgates.co.uk",
            "number": "",
        },  # We'd need to look this up
        {"name": "JORDAN CONSULTANTS LIMITED", "domain": "jordanconsultants.com", "number": ""},
    ]

    async with CreditExhaustClient() as client:
        if not client.access_token:
            print("❌ Failed to get OAuth token")
            return

        # Phase 1: Aggressive domain discovery
        print("🌐 Phase 1: Aggressive Domain Discovery")
        while client.credits_used < TARGET_CREDITS * 0.7:  # Use 70% for domain discovery
            companies = get_companies_batch(offset, BLOCK_SIZE)

            if not companies:
                print("❌ No more companies available")
                break

            company_names = [c["company_name"] for c in companies]

            print(f"📦 Block {block_number}: Processing {len(company_names)} companies")
            print(f"💰 Estimated credits used: {client.credits_used}/{TARGET_CREDITS}")

            result = await client.company_domain_search(company_names, f"block_{block_number}")

            if result and result["success"]:
                print(f"   ✓ Block {block_number} submitted")
            else:
                print(f"   ✗ Block {block_number} failed")

            offset += BLOCK_SIZE
            block_number += 1

            # Brief pause between blocks
            await asyncio.sleep(1)

            # Progress update
            elapsed = time.time() - start_time
            rate = client.companies_processed / elapsed if elapsed > 0 else 0
            print(f"   📊 {client.companies_processed} companies, {rate:.1f} companies/sec")

        # Phase 2: Email searches for companies with known domains
        print("\n📧 Phase 2: Email Discovery")
        for company_info in companies_with_domains:
            if client.credits_used >= TARGET_CREDITS:
                break

            company_name = company_info["name"]
            domain = company_info["domain"]
            company_number = company_info["number"]

            if company_number:
                officers = get_officers_for_company(company_number)
                if officers:
                    print(f"📧 Searching emails for {company_name} on {domain}")
                    print(f"   👥 Officers: {len(officers)}")

                    email_result = await client.email_search(domain, officers, company_name)

                    if email_result and email_result["success"]:
                        print(f"   ✓ Email search started for {email_result['officers']} officers")
                    else:
                        print("   ✗ Email search failed")

                    await asyncio.sleep(2)

        # Final summary
        total_time = time.time() - start_time
        print("\n" + "=" * 60)
        print("🏁 CREDIT EXHAUSTION COMPLETE")
        print(f"💰 Estimated credits used: {client.credits_used}/{TARGET_CREDITS}")
        print(f"🏢 Companies processed: {client.companies_processed}")
        print(f"📧 Email searches: {client.emails_searched}")
        print(f"⏱️  Total time: {total_time / 60:.1f} minutes")
        print(f"🚀 Average rate: {client.companies_processed / total_time:.1f} companies/second")
        print("\n📊 Check webhook handler for detailed results")
        print("💳 Check Snov.io dashboard for actual credit usage")


if __name__ == "__main__":
    asyncio.run(run_credit_exhaustion())
