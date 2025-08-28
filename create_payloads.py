#!/usr/bin/env python3
"""Create payloads for officer email searches."""

import json
import sqlite3
from typing import Any, Dict, List

# Database path
DB_PATH = "companies.db"

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


def create_payloads():
    """Create email search payloads for all companies."""
    print("📧 CREATING EMAIL SEARCH PAYLOADS")
    print("=" * 60)

    all_payloads = []

    for company in COMPANIES:
        print(f"\n🏢 {company['company_name']} ({company['company_number']})")
        print(f"🌐 Domain: {company['domain']}")

        # Get officers from database
        officers = get_officers_for_company(company["company_number"])

        if not officers:
            print("   ❌ No officers found")
            continue

        print(f"👥 Found {len(officers)} officers:")

        # Create rows for this company
        rows = []
        for officer in officers:
            first_name, last_name = parse_officer_name(officer["name"])

            row = {"first_name": first_name, "last_name": last_name, "domain": company["domain"]}
            rows.append(row)

            print(f"   - {officer['name']} → {first_name} {last_name}")

        # Create payload for this company
        payload = {"rows": rows, "webhook_url": "https://8888c5d2aa9d.ngrok-free.app/webhook"}

        company_payload = {"company": company, "payload": payload, "officers_count": len(officers)}

        all_payloads.append(company_payload)

        print(f"   ✅ Payload created with {len(rows)} officers")

    print("\n" + "=" * 60)
    print("📋 PAYLOAD SUMMARY")

    for i, cp in enumerate(all_payloads):
        print(f"\n🏢 Company {i + 1}: {cp['company']['company_name']}")
        print(f"📧 Officers: {cp['officers_count']}")
        print(f"🔗 Domain: {cp['company']['domain']}")
        print("📄 Payload:")
        print(json.dumps(cp["payload"], indent=2))

    print(f"\n📊 Total companies: {len(all_payloads)}")
    print(f"📧 Total officers: {sum(cp['officers_count'] for cp in all_payloads)}")

    # Save to file
    with open("email_search_payloads.json", "w") as f:
        json.dump(all_payloads, f, indent=2)

    print("💾 Payloads saved to email_search_payloads.json")


if __name__ == "__main__":
    create_payloads()
