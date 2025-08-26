#!/usr/bin/env python3
"""Export first 6000 companies and their directors to CSV."""

import csv
import sqlite3


def export_companies_and_directors() -> None:
    """Export companies and their directors to CSV."""
    conn = sqlite3.connect("companies.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get first 6000 companies with their directors
    query = """
    SELECT
        c.company_number,
        c.company_name,
        c.company_status,
        c.company_status_detail,
        c.incorporation_date,
        c.postcode,
        c.sic_code,
        o.name as director_name,
        o.officer_role,
        o.appointed_on,
        o.resigned_on,
        o.dob_year,
        o.dob_month,
        o.nationality,
        o.country_of_residence,
        o.occupation,
        o.address_line_1 as director_address_1,
        o.address_line_2 as director_address_2,
        o.locality as director_locality,
        o.postal_code as director_postcode,
        o.country as director_country
    FROM companies c
    LEFT JOIN officers o ON c.company_number = o.company_number
        AND o.officer_role IN ('director', 'Director', 'DIRECTOR')
    WHERE c.company_number IN (
        SELECT company_number
        FROM companies
        ORDER BY company_number
        LIMIT 6000
    )
    ORDER BY c.company_number, o.name
    """

    cursor.execute(query)
    results = cursor.fetchall()

    # Write to CSV
    with open("companies_directors_6000.csv", "w", newline="", encoding="utf-8") as csvfile:
        if results:
            # Get column names from the first row
            fieldnames = results[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for row in results:
                writer.writerow(dict(row))

    conn.close()

    print(f"Exported {len(results)} rows to companies_directors_6000.csv")

    # Get summary statistics
    conn = sqlite3.connect("companies.db")
    cursor = conn.cursor()

    # Count unique companies
    cursor.execute("""
        SELECT COUNT(DISTINCT company_number)
        FROM companies
        WHERE company_number IN (
            SELECT company_number
            FROM companies
            ORDER BY company_number
            LIMIT 6000
        )
    """)
    company_count = cursor.fetchone()[0]

    # Count directors
    cursor.execute("""
        SELECT COUNT(*)
        FROM officers
        WHERE company_number IN (
            SELECT company_number
            FROM companies
            ORDER BY company_number
            LIMIT 6000
        ) AND officer_role IN ('director', 'Director', 'DIRECTOR')
    """)
    director_count = cursor.fetchone()[0]

    conn.close()

    print(f"Summary: {company_count} companies, {director_count} directors")


if __name__ == "__main__":
    export_companies_and_directors()
