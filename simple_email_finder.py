#!/usr/bin/env python3
"""Simple email finder for officers using individual Email Finder API."""

import asyncio
import json
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List

import aiohttp
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
DB_PATH = "companies.db"
V1_BASE_URL = "https://api.snov.io/v1"
V2_BASE_URL = "https://api.snov.io/v2"
WEBHOOK_URL = "https://8888c5d2aa9d.ngrok-free.app/webhook"
CLIENT_ID = os.environ.get("SNOV_CLIENT_ID")
CLIENT_SECRET = os.environ.get("SNOV_CLIENT_SECRET")

# Companies with discovered domains
COMPANIES = [
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


class SimpleEmailFinder:
    """Simple email finder using individual API calls."""

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
        token_url = "https://api.snov.io/v1/oauth/access_token"

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

    async def find_email(self, first_name: str, last_name: str, domain: str):
        """Find email using Email Finder API."""
        if not self.access_token:
            return None

        url = "https://api.snov.io/v2/emails-by-domain-by-name/start"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        payload = {
            "rows": [{"first_name": first_name, "last_name": last_name, "domain": domain}],
            "webhook_url": f"{WEBHOOK_URL}",
        }

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


def parse_officer_name(name: str):
    """Parse officer name into first and last name."""
    # Handle "SURNAME, First Middle" format
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

    return first_name, last_name


def save_email_result(
    company_number: str,
    domain: str,
    officer_name: str,
    first_name: str,
    last_name: str,
    email: str,
    confidence: float,
):
    """Save email result to database."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                INSERT OR REPLACE INTO officer_emails
                (officer_id, company_number, domain, first_name, last_name, email,
                 email_status, smtp_check, confidence_score, discovered_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, 'found', 0, ?, ?, ?)
            """,
                (
                    f"snov_{company_number}_{first_name}_{last_name}",
                    company_number,
                    domain,
                    first_name,
                    last_name,
                    email,
                    confidence,
                    datetime.now().isoformat(),
                    json.dumps({"officer_name": officer_name, "source": "snov.io_email_finder"}),
                ),
            )

            conn.commit()
            print(f"   ✅ Saved: {first_name} {last_name} -> {email}")

    except Exception as e:
        print(f"   ❌ Error saving email: {e}")


async def find_all_officer_emails():
    """Find emails for all officers using simple Email Finder API."""
    print("🔍 SIMPLE EMAIL FINDER")
    print("=" * 60)

    total_officers = 0
    total_emails_found = 0

    async with SimpleEmailFinder() as client:
        if not client.access_token:
            print("❌ Failed to get OAuth token")
            return

        for company in COMPANIES:
            print(f"\n🏢 {company['company_name']}")
            print(f"🌐 Domain: {company['domain']}")

            # Get officers from database
            officers = get_officers_for_company(company["company_number"])

            if not officers:
                print("   ❌ No officers found")
                continue

            print(f"👥 Found {len(officers)} officers")
            total_officers += len(officers)

            # Search for each officer individually
            for officer in officers:
                first_name, last_name = parse_officer_name(officer["name"])

                print(f"   📧 Searching: {first_name} {last_name} @ {company['domain']}")

                result = await client.find_email(first_name, last_name, company["domain"])

                if result and result["success"]:
                    data = result["data"]
                    email = data.get("email")
                    confidence = data.get("confidence", 0.0)

                    if email:
                        print(f"      ✅ Found: {email} (confidence: {confidence})")
                        save_email_result(
                            company["company_number"],
                            company["domain"],
                            officer["name"],
                            first_name,
                            last_name,
                            email,
                            confidence,
                        )
                        total_emails_found += 1
                    else:
                        print("      📭 No email found")
                else:
                    print("      ❌ Search failed")
                    if result:
                        print(f"         Status: {result['status_code']}")
                        print(f"         Response: {result['raw'][:100]}...")

                # Pause between requests to respect rate limits
                await asyncio.sleep(1)

    print("\n" + "=" * 60)
    print("📊 FINAL RESULTS")
    print(f"🏢 Companies processed: {len(COMPANIES)}")
    print(f"👥 Total officers searched: {total_officers}")
    print(f"📧 Emails found: {total_emails_found}")
    print("💾 Results saved to officer_emails table")


if __name__ == "__main__":
    asyncio.run(find_all_officer_emails())
