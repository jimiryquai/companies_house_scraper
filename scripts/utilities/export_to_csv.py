import csv
import logging
import sqlite3
import traceback
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("export.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def export_table_to_csv(db_path: str, table_name: str, output_file: str) -> None:
    """Export a table from SQLite database to CSV file."""
    try:
        # Connect to the database
        logger.info(f"Connecting to database {db_path}")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Get column names
        logger.info(f"Getting column information for table {table_name}")
        # Validate table name for security (PRAGMA statements can't use parameters)
        if not table_name.replace("_", "").isalnum():
            raise ValueError(f"Invalid table name: {table_name}")
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [column[1] for column in cursor.fetchall()]
        logger.info(f"Found columns: {columns}")

        # Get all data from the table
        logger.info(f"Fetching data from {table_name}")
        # Note: Table names cannot be parameterized in SQL, but we validate it's safe
        if not table_name.replace("_", "").isalnum():
            raise ValueError(f"Invalid table name: {table_name}")
        cursor.execute(f"SELECT * FROM {table_name}")  # noqa: S608
        rows = cursor.fetchall()
        logger.info(f"Fetched {len(rows)} rows from {table_name}")

        # Write to CSV
        logger.info(f"Writing data to {output_file}")
        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)  # Write header
            writer.writerows(rows)  # Write data

        logger.info(f"Successfully exported {len(rows)} rows from {table_name} to {output_file}")

    except Exception as e:
        logger.error(f"Error exporting {table_name} to CSV: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise
    finally:
        if "conn" in locals():
            conn.close()
            logger.info("Database connection closed")


def main() -> None:
    """Export database tables to CSV files."""
    # Generate timestamp for unique filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Export companies table
    companies_file = f"companies_{timestamp}.csv"
    export_table_to_csv("companies.db", "companies", companies_file)

    # Export officers table
    officers_file = f"officers_{timestamp}.csv"
    export_table_to_csv("companies.db", "officers", officers_file)

    logger.info("Export completed successfully")


if __name__ == "__main__":
    main()
