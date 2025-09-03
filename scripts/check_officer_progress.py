#!/usr/bin/env python3
"""Check officer import progress and determine if ready for streaming API."""

import sqlite3
import sys


def get_progress():
    """Check current officer import progress."""
    try:
        conn = sqlite3.connect("companies.db")
        cursor = conn.cursor()

        # Get companies with officers
        cursor.execute("SELECT COUNT(DISTINCT company_number) FROM officers")
        companies_with_officers = cursor.fetchone()[0]

        # Get total officers
        cursor.execute("SELECT COUNT(*) FROM officers")
        total_officers = cursor.fetchone()[0]

        # Get total companies in database
        cursor.execute("SELECT COUNT(*) FROM companies")
        total_companies = cursor.fetchone()[0]

        conn.close()

        target = 10000
        progress_pct = (companies_with_officers / target) * 100

        print("=== Officer Import Progress ===")
        print(f"Companies with officers: {companies_with_officers:,}")
        print(f"Total officers imported: {total_officers:,}")
        print(f"Total companies in DB: {total_companies:,}")
        print(f"Target for streaming: {target:,}")
        print(f"Progress: {progress_pct:.1f}%")
        print()

        if companies_with_officers >= target:
            print("✅ READY FOR STREAMING API!")
            print("Officer import has reached 10K companies.")
            print("You can now:")
            print("  1. Stop officer import")
            print("  2. Enable streaming API")
            print("  3. Deploy to Render")
            return True
        remaining = target - companies_with_officers
        print(f"⏳ Still importing... {remaining:,} companies to go")
        print("Streaming API not ready yet.")
        return False

    except Exception as e:
        print(f"Error checking progress: {e}")
        return False


if __name__ == "__main__":
    ready = get_progress()
    sys.exit(0 if ready else 1)
