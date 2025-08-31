#!/usr/bin/env python3
"""Populate existing strike-off companies with 'completed' status in company_processing_state table.

This script identifies companies that are already in strike-off or dissolution status
and marks them as 'completed' in the company_processing_state table since no further
processing is needed for these companies.
"""

import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def populate_strike_off_companies(db_path: str = "companies.db") -> None:
    """Populate strike-off companies with completed status."""
    conn = sqlite3.connect(db_path)

    try:
        cursor = conn.cursor()

        # Check if company_processing_state table exists
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='company_processing_state'
        """)
        if not cursor.fetchone():
            logger.error("company_processing_state table does not exist. Run migrations first.")
            return

        # Find companies with strike-off or dissolution status
        strike_off_patterns = [
            "%strike%",
            "%dissolution%",
            "%liquidation%",
            "%wound-up%",
            "%removed%",
            "%closed%",
            "dissolved",
            "inactive",
        ]

        # Build WHERE clause for strike-off patterns
        where_conditions = []
        params = []
        for pattern in strike_off_patterns:
            where_conditions.append("LOWER(company_status) LIKE ?")
            params.append(pattern.lower())

        where_clause = " OR ".join(where_conditions)

        # Get companies that need to be marked as completed
        cursor.execute(
            f"""
            SELECT c.company_number, c.company_status
            FROM companies c
            LEFT JOIN company_processing_state cps ON c.company_number = cps.company_number
            WHERE ({where_clause}) AND cps.company_number IS NULL
        """,
            params,
        )

        strike_off_companies = cursor.fetchall()
        logger.info(f"Found {len(strike_off_companies)} strike-off companies to populate")

        if not strike_off_companies:
            logger.info("No strike-off companies need to be populated")
            return

        # Insert companies with 'completed' status
        now = datetime.now().isoformat()
        insert_data = []

        for company_number, company_status in strike_off_companies:
            insert_data.append(
                (
                    company_number,
                    "completed",  # processing_state
                    now,  # created_at
                    now,  # updated_at
                    now,  # completed_at
                )
            )

        # Batch insert for performance
        cursor.executemany(
            """
            INSERT INTO company_processing_state
            (company_number, processing_state, created_at, updated_at, completed_at)
            VALUES (?, ?, ?, ?, ?)
        """,
            insert_data,
        )

        conn.commit()
        logger.info(
            f"Successfully populated {len(insert_data)} strike-off companies with 'completed' status"
        )

        # Verify the population
        cursor.execute("""
            SELECT COUNT(*) FROM company_processing_state
            WHERE processing_state = 'completed'
        """)
        completed_count = cursor.fetchone()[0]
        logger.info(f"Total companies with 'completed' status: {completed_count}")

        # Show status breakdown
        cursor.execute("""
            SELECT processing_state, COUNT(*) as count
            FROM company_processing_state
            GROUP BY processing_state
            ORDER BY count DESC
        """)

        status_breakdown = cursor.fetchall()
        logger.info("Processing state breakdown:")
        for state, count in status_breakdown:
            logger.info(f"  {state}: {count:,}")

    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
        raise
    except Exception as e:
        logger.error(f"Error populating strike-off companies: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "companies.db"

    if not Path(db_path).exists():
        logger.error(f"Database file {db_path} does not exist")
        sys.exit(1)

    populate_strike_off_companies(db_path)
