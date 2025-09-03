#!/usr/bin/env python3
"""Scale test processing companies in blocks of 10 until we find domains."""

import asyncio
import json
import os
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

import aiohttp
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
DB_PATH = "companies.db"
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "https://8888c5d2aa9d.ngrok-free.app/webhook")
BLOCK_SIZE = 10
MAX_COMPANIES = 250
BASE_URL = "https://api.snov.io/v2"
V1_BASE_URL = "https://api.snov.io/v1"
CLIENT_ID = os.environ.get("SNOV_CLIENT_ID")
CLIENT_SECRET = os.environ.get("SNOV_CLIENT_SECRET")


class SnovScaleClient:
    """Snov.io client for scaled testing."""

    def __init__(self):
        self.access_token = None
        self.token_expires_at = None
        self.session = None
        self.total_credits_used = 0
        self.total_companies_processed = 0
        self.total_domains_found = 0
        self.blocks_processed = 0

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

    async def process_company_block(self, company_names: List[str], block_number: int):
        """Process a block of companies."""
        if not self.access_token:
            return None

        url = f"{BASE_URL}/company-domain-by-name/start"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        # Use simple array format that worked
        payload = {
            "names": company_names,
            "webhook_url": f"{WEBHOOK_URL}?block={block_number}&timestamp={int(time.time())}",
        }

        print(f"\n📦 Block {block_number}: Processing {len(company_names)} companies")
        for i, name in enumerate(company_names):
            print(f"   [{i + 1}] {name}")

        async with self.session.post(url, headers=headers, json=payload) as response:
            result_text = await response.text()
            try:
                result_json = json.loads(result_text)
            except:
                result_json = {"raw_response": result_text}

            if response.status in [200, 202]:
                task_hash = result_json.get("data", {}).get("task_hash")
                print(f"   ✓ Block {block_number} submitted, task_hash: {task_hash}")
                self.blocks_processed += 1
                self.total_companies_processed += len(company_names)
                return {
                    "success": True,
                    "task_hash": task_hash,
                    "block_number": block_number,
                    "company_count": len(company_names),
                }
            print(f"   ✗ Block {block_number} failed: {response.status} - {result_text}")
            return {"success": False, "error": result_text}


def get_companies_in_batches(offset: int = 0, limit: int = MAX_COMPANIES) -> List[Dict[str, Any]]:
    """Get companies from database in batches."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Get a diverse set of companies - some may have domains
            cursor.execute(
                """
                SELECT company_number, company_name
                FROM companies
                WHERE company_status_detail LIKE '%proposal to strike off%'
                AND company_name IS NOT NULL
                AND company_name != ''
                AND length(company_name) BETWEEN 10 AND 60
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


async def run_scaled_test():
    """Run scaled test in blocks until we find domains."""
    print("🚀 SNOV.IO SCALED DOMAIN DISCOVERY TEST")
    print("=" * 60)
    print(f"Block size: {BLOCK_SIZE} companies")
    print(f"Max companies: {MAX_COMPANIES}")
    print(f"Webhook URL: {WEBHOOK_URL}")
    print("=" * 60)

    if not CLIENT_ID or not CLIENT_SECRET:
        print("ERROR: Missing credentials")
        return

    start_time = time.time()
    domains_found_in_any_block = False

    async with SnovScaleClient() as client:
        if not client.access_token:
            print("Failed to get OAuth token")
            return

        print("🎯 Starting block processing...")

        # Process companies in blocks
        offset = 0
        block_number = 1

        while offset < MAX_COMPANIES and not domains_found_in_any_block:
            # Get next batch of companies
            companies = get_companies_in_batches(offset, BLOCK_SIZE)

            if not companies:
                print(f"\n❌ No more companies available at offset {offset}")
                break

            company_names = [c["company_name"] for c in companies]

            # Process this block
            result = await client.process_company_block(company_names, block_number)

            if result and result["success"]:
                # Wait for webhook results
                print("   ⏳ Waiting 20 seconds for webhook results...")
                await asyncio.sleep(20)

                # For now, continue processing.
                # The webhook handler will log any domains found
                # In a real implementation, we'd check webhook results here

            else:
                print(f"   ❌ Block {block_number} failed, stopping")
                break

            # Progress update
            elapsed = time.time() - start_time
            rate = client.total_companies_processed / elapsed if elapsed > 0 else 0

            print("\n📊 PROGRESS UPDATE:")
            print(f"   Blocks processed: {client.blocks_processed}")
            print(f"   Companies processed: {client.total_companies_processed}")
            print(f"   Processing rate: {rate:.1f} companies/second")
            print(f"   Elapsed time: {elapsed / 60:.1f} minutes")

            # Move to next block
            offset += BLOCK_SIZE
            block_number += 1

            # Small delay between blocks to be respectful
            if offset < MAX_COMPANIES:
                print("   💤 Brief pause before next block...")
                await asyncio.sleep(3)

        # Final summary
        total_time = time.time() - start_time
        print("\n" + "=" * 60)
        print("🏁 SCALED TEST COMPLETE")
        print(f"Total blocks processed: {client.blocks_processed}")
        print(f"Total companies processed: {client.total_companies_processed}")
        print(f"Total time: {total_time / 60:.1f} minutes")
        print(f"Average rate: {client.total_companies_processed / total_time:.1f} companies/second")
        print("\n📋 Check webhook handler logs for domain discovery results")
        print("💰 Check Snov.io dashboard for credit usage")


if __name__ == "__main__":
    asyncio.run(run_scaled_test())
