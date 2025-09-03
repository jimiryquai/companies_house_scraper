#!/usr/bin/env python3
"""Export stakeholder data to CSV
Creates a comprehensive CSV report for stakeholders showing companies with officers
"""

import csv
import sqlite3
from datetime import datetime


def export_stakeholder_data():
    """Export comprehensive company and officer data for stakeholder review"""
    db_path = "companies.db"
    output_file = f"stakeholder_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    print(f"Connecting to database: {db_path}")
    conn = sqlite3.connect(db_path)

    try:
        # SQL query to get companies with their officers
        query = """
        SELECT 
            c.company_number,
            c.company_name,
            c.company_status,
            c.company_status_detail,
            c.incorporation_date,
            c.postcode,
            c.sic_code,
            c.data_source,
            c.stream_status,
            o.name as officer_name,
            o.officer_role,
            o.appointed_on,
            o.resigned_on,
            o.nationality,
            o.occupation,
            o.dob_year,
            o.dob_month,
            o.country_of_residence,
            o.address_line_1,
            o.locality,
            o.region,
            o.country,
            o.postal_code
        FROM companies c
        LEFT JOIN officers o ON c.company_number = o.company_number
        WHERE o.company_number IS NOT NULL
        ORDER BY c.company_number, o.appointed_on DESC
        """

        cursor = conn.cursor()
        cursor.execute(query)

        # Get column names
        columns = [desc[0] for desc in cursor.description]

        print(f"Exporting data to: {output_file}")

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)

            # Write header
            writer.writerow(columns)

            # Write data
            row_count = 0
            for row in cursor.fetchall():
                writer.writerow(row)
                row_count += 1

                if row_count % 1000 == 0:
                    print(f"Exported {row_count} rows...")

        print("\nExport completed!")
        print(f"Total rows exported: {row_count}")
        print(f"Output file: {output_file}")

        # Summary statistics
        cursor.execute("SELECT COUNT(*) FROM companies")
        total_companies = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM officers")
        total_officers = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT company_number) FROM officers")
        companies_with_officers = cursor.fetchone()[0]

        print("\nDatabase Summary:")
        print(f"Total companies: {total_companies:,}")
        print(f"Total officers: {total_officers:,}")
        print(f"Companies with officers: {companies_with_officers:,}")
        print(f"Coverage: {companies_with_officers / total_companies * 100:.1f}%")

        return output_file

    except Exception as e:
        print(f"Error during export: {e}")
        return None

    finally:
        conn.close()


if __name__ == "__main__":
    export_stakeholder_data()
