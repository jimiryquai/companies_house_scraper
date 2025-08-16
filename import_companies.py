#!/usr/bin/env python
import csv
import logging
import sqlite3
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("space_fixed_import.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# Constants
CSV_FILENAME = "BasicCompanyDataAsOneFile-2025-04-01.csv"
DB_FILENAME = "companies.db"
BATCH_SIZE = 1000  # Number of records to insert at once
LOG_INTERVAL = 100000  # Log progress every this many records


def normalize_key(key: str) -> str:
    """Remove spaces from keys to handle weird CSV formats."""
    return key.strip()


def setup_fresh_database(db_filename: str = DB_FILENAME) -> sqlite3.Connection:
    """Create a fresh database with consistent schema."""
    logger.info(f"Setting up fresh database: {db_filename}")

    # Connect to database (will create it if it doesn't exist)
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()

    # Check if tables exist and drop them
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]
        if table_name != "sqlite_sequence":  # Don't drop system tables
            logger.info(f"Dropping existing table: {table_name}")
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    # Create tables with consistent schema
    logger.info("Creating companies table...")
    cursor.execute("""
    CREATE TABLE companies (
        company_number TEXT PRIMARY KEY NOT NULL,
        company_name TEXT,
        company_status TEXT,
        company_status_detail TEXT,
        incorporation_date TEXT,
        postcode TEXT,
        sic_code TEXT,
        imported_at TEXT,
        psc_fetched INTEGER DEFAULT 0
    )
    """)

    # Create officers table
    logger.info("Creating officers table...")
    cursor.execute("""
    CREATE TABLE officers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_number TEXT NOT NULL,
        name TEXT,
        role TEXT,
        appointed_on TEXT,
        resigned_on TEXT,
        officer_id TEXT,
        FOREIGN KEY (company_number) REFERENCES companies(company_number)
    )
    """)

    # Create metadata table
    logger.info("Creating metadata table...")
    cursor.execute("""
    CREATE TABLE metadata (
        key TEXT PRIMARY KEY NOT NULL,
        value TEXT
    )
    """)

    conn.commit()
    logger.info("Database setup complete")

    return conn


def import_strike_off_companies(  # noqa: C901
    conn: sqlite3.Connection, csv_filename: str = CSV_FILENAME
) -> tuple[int, int]:
    """Import companies with strike-off status from CSV."""
    logger.info(f"Starting import of strike-off companies from {csv_filename}")
    start_time = datetime.now()

    cursor = conn.cursor()

    # Track stats
    total_rows = 0
    strike_off_companies = 0
    inserted_count = 0
    skipped_count = 0
    batch = []

    # Process CSV file
    logger.info("Processing CSV file...")
    try:
        with open(csv_filename, newline="", encoding="utf-8") as csvfile:
            # Read as list to get raw header row
            sample_reader = csv.reader(csvfile)
            header_row = next(sample_reader)
            logger.info(f"CSV header row: {header_row[0:5]}...")

            # Reset file pointer
            csvfile.seek(0)

            # Create a custom field name map to handle spaces in CSV headers
            reader = csv.DictReader(csvfile)
            fieldnames = reader.fieldnames or []
            field_map = {field: normalize_key(field) for field in fieldnames}
            logger.info(f"Field mapping: {field_map}")

            # Get a sample row to check field mapping
            try:
                sample_row = next(reader)
                # Map the keys to normalized keys
                normalized_sample = {normalize_key(k): v for k, v in sample_row.items()}

                logger.info(
                    f"Sample company name: {normalized_sample.get('CompanyName', 'NOT FOUND')}"
                )
                logger.info(
                    f"Sample company number: {normalized_sample.get('CompanyNumber', 'NOT FOUND')}"
                )
                logger.info(
                    f"Sample company status: {normalized_sample.get('CompanyStatus', 'NOT FOUND')}"
                )

                # Reset file pointer
                csvfile.seek(0)
                next(csvfile)  # Skip header
            except StopIteration:
                logger.error("CSV file is empty or malformed")
                return 0, 0

            # Process all rows
            for row in reader:
                # Normalize all keys in the row
                normalized_row = {normalize_key(k): v for k, v in row.items()}

                total_rows += 1

                # Log progress
                if total_rows % LOG_INTERVAL == 0:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    rate = total_rows / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"Processed {total_rows:,} rows ({rate:.1f} rows/sec). "
                        f"Found {strike_off_companies:,} strike-off companies."
                    )

                # Check if company has strike-off status
                company_status = normalized_row.get("CompanyStatus", "").strip()
                status_detail = normalized_row.get("CompanyStatusDetail", "").strip()

                # Check both potential formats for strike-off status
                is_strike_off = False
                if "Active - Proposal to Strike off" in company_status:
                    is_strike_off = True
                    status_detail = "Active proposal to strike off"
                elif company_status == "Active" and "strike-off" in status_detail.lower():
                    is_strike_off = True

                if not is_strike_off:
                    continue

                # Found a strike-off company
                strike_off_companies += 1

                # Extract company data
                company_number = normalized_row.get("CompanyNumber", "").strip()

                # Debug the first few companies to check field mappings
                if strike_off_companies <= 5:
                    logger.info(
                        f"Strike-off company #{strike_off_companies}: "
                        f"{normalized_row.get('CompanyName', 'UNKNOWN')}, Number: {company_number}"
                    )

                if not company_number:
                    if strike_off_companies % 1000 == 0:
                        logger.warning(
                            f"Skipping company with empty number: "
                            f"{normalized_row.get('CompanyName', 'UNKNOWN')}"
                        )
                    skipped_count += 1
                    continue

                company_name = normalized_row.get("CompanyName", "").strip()
                incorporation_date = normalized_row.get("IncorporationDate", "").strip()
                postcode = normalized_row.get("RegAddress.PostCode", "").strip()
                sic_code = normalized_row.get("SICCode.SicText_1", "").strip()
                imported_at = datetime.now().isoformat()

                # Add to batch
                batch.append(
                    (
                        company_number,
                        company_name,
                        company_status,
                        status_detail,
                        incorporation_date,
                        postcode,
                        sic_code,
                        imported_at,
                        0,  # psc_fetched default
                    )
                )

                # Insert batch when it reaches the specified size
                if len(batch) >= BATCH_SIZE:
                    try:
                        cursor.executemany(
                            """INSERT OR REPLACE INTO companies
                            (company_number, company_name, company_status, company_status_detail,
                             incorporation_date, postcode, sic_code, imported_at, psc_fetched)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            batch,
                        )
                        conn.commit()
                        inserted_count += len(batch)
                        logger.info(
                            f"Committed batch of {len(batch):,} records. "
                            f"Total inserted: {inserted_count:,}"
                        )
                        batch = []
                    except Exception as e:
                        logger.error(f"ERROR: Failed to insert batch: {e}")
                        # Try to insert records one by one
                        for record in batch:
                            try:
                                cursor.execute(
                                    """INSERT OR REPLACE INTO companies
                                    (company_number, company_name, company_status,
                                     company_status_detail, incorporation_date, postcode,
                                     sic_code, imported_at, psc_fetched)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                                    record,
                                )
                                conn.commit()
                                inserted_count += 1
                            except Exception as e2:
                                logger.error(f"Failed to insert record {record[0]}: {e2}")
                                skipped_count += 1
                        batch = []

        # Insert any remaining records
        if batch:
            try:
                cursor.executemany(
                    """INSERT OR REPLACE INTO companies
                    (company_number, company_name, company_status, company_status_detail,
                     incorporation_date, postcode, sic_code, imported_at, psc_fetched)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    batch,
                )
                conn.commit()
                inserted_count += len(batch)
                logger.info(
                    f"Committed final batch of {len(batch):,} records. "
                    f"Total inserted: {inserted_count:,}"
                )
            except Exception as e:
                logger.error(f"ERROR: Failed to insert final batch: {e}")
                # Try to insert records one by one
                for record in batch:
                    try:
                        cursor.execute(
                            """INSERT OR REPLACE INTO companies
                            (company_number, company_name, company_status, company_status_detail,
                             incorporation_date, postcode, sic_code, imported_at, psc_fetched)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            record,
                        )
                        conn.commit()
                        inserted_count += 1
                    except Exception as e2:
                        logger.error(f"Failed to insert record {record[0]}: {e2}")
                        skipped_count += 1

        # Verify results
        cursor.execute("SELECT COUNT(*) FROM companies")
        final_count = cursor.fetchone()[0]

        # Show sample data
        cursor.execute("SELECT company_number, company_name, company_status FROM companies LIMIT 5")
        samples = cursor.fetchall()

        # Print summary
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"\nImport completed in {elapsed:.2f} seconds")
        logger.info(f"Processed {total_rows:,} rows from CSV")
        logger.info(f"Found {strike_off_companies:,} companies with strike-off status")
        logger.info(f"Successfully inserted {inserted_count:,} companies")
        logger.info(f"Skipped {skipped_count:,} records with issues")
        logger.info(f"Database now contains {final_count:,} companies")

        logger.info("\nSample records:")
        for sample in samples:
            logger.info(f" - {sample}")

        return inserted_count, strike_off_companies

    except Exception as e:
        logger.error(f"Error processing CSV: {e}", exc_info=True)
        return 0, 0


def main() -> None:
    """Main function to orchestrate the company import process."""
    logger.info("Starting space-fixed import process...")

    # Setup fresh database
    conn = setup_fresh_database(DB_FILENAME)

    try:
        # Import companies with strike-off status
        inserted, found = import_strike_off_companies(conn, CSV_FILENAME)

        if inserted > 0:
            logger.info(f"Successfully imported {inserted} companies with strike-off status")
        else:
            logger.warning("No companies were imported. Check the log for errors.")

    except Exception as e:
        logger.error(f"Error during import process: {e}", exc_info=True)
    finally:
        # Close connection
        conn.close()
        logger.info("Database connection closed")


if __name__ == "__main__":
    main()
