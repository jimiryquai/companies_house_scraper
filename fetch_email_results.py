#!/usr/bin/env python3
"""Fetch email search results using task hashes."""

import asyncio
import json
import os
import sqlite3
from datetime import datetime
from typing import Dict, List

import aiohttp
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
DB_PATH = "companies.db"
V1_BASE_URL = "https://api.snov.io/v1"
CLIENT_ID = os.environ.get("SNOV_CLIENT_ID")
CLIENT_SECRET = os.environ.get("SNOV_CLIENT_SECRET")

# Task hashes from email searches
TASK_HASHES = [
    {
        "task_hash": "049ef21d8ebf66f096d31ef85af83d22",
        "company_name": "BLACK LLAMA CAPITAL LIMITED",
        "company_number": "10607015",
        "domain": "blackllamacapital.co.uk",
    },
    {
        "task_hash": "479cbcb7ce2fbeb630e6bc955ac1f2b4",
        "company_name": "GRAND GATES LTD",
        "company_number": "SC460757",
        "domain": "grandgates.co.uk",
    },
    {
        "task_hash": "9b8f9c5f96a5db8f5746d8bd80d7e762",
        "company_name": "JORDAN CONSULTANTS LIMITED",
        "company_number": "05077366",
        "domain": "jordanconsultants.com",
    },
]


class ResultsFetcher:
    """Client for fetching email search results."""

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

    async def get_bulk_task_results(self, task_hash: str):
        """Get results for a bulk task."""
        if not self.access_token:
            return None

        url = f"{V1_BASE_URL}/bulk-task-result"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        params = {"task_hash": task_hash}

        async with self.session.get(url, headers=headers, params=params) as response:
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


def save_email_results(results: List[Dict], company_info: Dict):
    """Save email results to database."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()

            emails_saved = 0

            for result in results:
                email = result.get("email", "")
                first_name = result.get("first_name", "")
                last_name = result.get("last_name", "")
                confidence = result.get("confidence", 0.0)

                if email and "@" in email:
                    # Find matching officer in database
                    full_name = f"{last_name}, {first_name}".upper()
                    cursor.execute(
                        """
                        SELECT name FROM officers
                        WHERE company_number = ?
                        AND (name LIKE ? OR name LIKE ?)
                        LIMIT 1
                    """,
                        (
                            company_info["company_number"],
                            f"{last_name}%{first_name}%",
                            f"{first_name}%{last_name}%",
                        ),
                    )

                    officer_match = cursor.fetchone()
                    officer_name = officer_match[0] if officer_match else full_name

                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO officer_emails
                        (officer_id, company_number, domain, first_name, last_name, email,
                         verification_status, is_verified, confidence, discovered_at, source, metadata)
                        VALUES (?, ?, ?, ?, ?, ?, 'found', 0, ?, ?, 'snov.io', ?)
                    """,
                        (
                            f"snov_{company_info['company_number']}_{first_name}_{last_name}",
                            company_info["company_number"],
                            company_info["domain"],
                            first_name,
                            last_name,
                            email,
                            confidence,
                            datetime.now().isoformat(),
                            json.dumps(
                                {
                                    "task_hash": company_info.get("task_hash"),
                                    "officer_name": officer_name,
                                }
                            ),
                        ),
                    )

                    emails_saved += 1
                    print(f"   ✅ Saved: {first_name} {last_name} -> {email}")

            conn.commit()
            print(f"   💾 Saved {emails_saved} email addresses")

    except Exception as e:
        print(f"   ❌ Error saving results: {e}")


async def fetch_all_results():
    """Fetch results for all task hashes."""
    print("📥 FETCHING EMAIL SEARCH RESULTS")
    print("=" * 60)

    total_emails = 0

    async with ResultsFetcher() as client:
        if not client.access_token:
            print("❌ Failed to get OAuth token")
            return

        for task_info in TASK_HASHES:
            print(f"\n🏢 {task_info['company_name']}")
            print(f"🔗 Task Hash: {task_info['task_hash']}")

            result = await client.get_bulk_task_results(task_info["task_hash"])

            if result and result["success"]:
                data = result["data"]
                status = data.get("status", "unknown")

                if status == "completed":
                    emails = data.get("data", [])
                    print(f"   ✅ Task completed - found {len(emails)} emails")

                    if emails:
                        save_email_results(emails, task_info)
                        total_emails += len(emails)
                    else:
                        print("   📭 No emails found")

                elif status == "pending":
                    print("   ⏳ Task still processing...")
                elif status == "failed":
                    print("   ❌ Task failed")
                else:
                    print(f"   📊 Status: {status}")

            else:
                print("   ❌ Failed to fetch results")
                if result:
                    print(f"   📊 Status: {result['status_code']}")
                    print(f"   📄 Response: {result['raw'][:200]}...")

            # Brief pause between requests
            await asyncio.sleep(1)

    print("\n" + "=" * 60)
    print("📊 RESULTS SUMMARY")
    print(f"📧 Total emails found: {total_emails}")
    print("💾 Results saved to officer_emails table")


if __name__ == "__main__":
    asyncio.run(fetch_all_results())
