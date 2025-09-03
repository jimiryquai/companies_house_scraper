import logging
import os
import sqlite3
import sys
import time
from datetime import datetime
from typing import Any, Optional

import requests
import yaml

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("officer_import.log"), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("officer_import")


def load_config() -> dict[str, Any]:
    """Load configuration from config.yaml file."""
    try:
        with open("config.yaml") as file:
            config = yaml.safe_load(file)
            return config or {}
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        return {
            "api": {"key": os.environ.get("COMPANIES_HOUSE_API_KEY", "")},
            "rate_limit": {
                "calls": 600,
                "period": 300,  # 5 minutes
            },
        }


def get_db_connection() -> sqlite3.Connection:
    """Create a connection to the SQLite database."""
    conn = sqlite3.connect("companies.db")
    conn.row_factory = sqlite3.Row
    return conn


def get_companies_with_officers_count() -> int:
    """Get count of companies that already have officers imported."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(DISTINCT company_number) FROM officers")
        result = cursor.fetchone()
        return result[0] if result else 0
    except Exception as e:
        logger.error(f"Error counting companies with officers: {e}")
        return 0
    finally:
        conn.close()


def get_companies_with_strike_off() -> list[tuple[str, str]]:
    """Get all companies with strike-off status from the database."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        query = """
        SELECT company_number, company_name
        FROM companies
        WHERE company_status_detail LIKE '%proposal to strike off%'
        """
        cursor.execute(query)
        companies = cursor.fetchall()
        logger.info(f"Found {len(companies)} companies with strike-off status")
        return companies
    except Exception as e:
        logger.error(f"Error fetching companies: {e}")
        return []
    finally:
        conn.close()


def _make_request_with_retry(
    url: str, api_key: str, max_retries: int = 3, initial_delay: int = 1
) -> Optional[dict[str, Any]]:
    """Make a request to the Companies House API with retry logic."""
    auth = (api_key, "")  # Companies House API uses the API key as username with empty password

    for attempt in range(max_retries):
        try:
            response = requests.get(url, auth=auth, timeout=30)  # noqa: S113

            if response.status_code == 200:
                result = response.json()
                return result if isinstance(result, dict) else None
            if response.status_code == 429:  # Rate limit exceeded
                logger.warning("Rate limit exceeded. Retrying after delay.")
                time.sleep(initial_delay * (2**attempt))
                continue
            if 500 <= response.status_code < 600:  # Server error
                logger.warning(
                    f"Server error {response.status_code}. Response: {response.text[:1000]}"
                )
                time.sleep(initial_delay * (2**attempt))
                continue
            if 400 <= response.status_code < 500:  # Client error
                logger.error(
                    f"Client error {response.status_code} for URL {url}. "
                    f"Response: {response.text[:1000]}"
                )
                return None
            logger.error(f"Unexpected status code {response.status_code} for URL {url}")
            return None

        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            time.sleep(initial_delay * (2**attempt))

    logger.error(f"Failed to fetch data after {max_retries} retries for URL: {url}")
    return None


def get_company_officers(company_number: str, api_key: str) -> list[dict[str, Any]]:
    """Fetch officers data for a specific company from the Companies House API."""
    url = f"https://api.company-information.service.gov.uk/company/{company_number}/officers"

    officers_data = []
    page_index = 0
    items_per_page = 100

    while True:
        page_url = f"{url}?start_index={page_index}&items_per_page={items_per_page}"
        data = _make_request_with_retry(page_url, api_key)

        if not data:
            break

        if "items" in data and data["items"]:
            # Filter for directors only
            directors = [
                officer
                for officer in data["items"]
                if officer.get("officer_role", "").lower() == "director"
            ]
            officers_data.extend(directors)

            # Check if we've reached the end of the data
            if len(data["items"]) < items_per_page:
                break

            page_index += items_per_page
        else:
            break

    return officers_data


def save_officers_to_db(company_number: str, officers_data: list[dict[str, Any]]) -> int:
    """Save officers data to the database."""
    if not officers_data:
        return 0

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        inserted_count = 0

        for officer in officers_data:
            # Extract officer data
            name = officer.get("name", "")
            role = officer.get("officer_role", "")
            appointed_on = officer.get("appointed_on", "")
            resigned_on = officer.get("resigned_on", "")

            # Extract officer ID from links
            officer_id = ""
            if (
                "links" in officer
                and "officer" in officer["links"]
                and "appointments" in officer["links"]["officer"]
            ):
                appointments_url = officer["links"]["officer"]["appointments"]
                # Extract ID from URL, typically the last part
                parts = appointments_url.rstrip("/").split("/")
                if len(parts) > 1:
                    officer_id = parts[-2]

            # Extract address details
            address = officer.get("address", {})
            address_line_1 = address.get("address_line_1", "")
            address_line_2 = address.get("address_line_2", "")
            locality = address.get("locality", "")
            region = address.get("region", "")
            country = address.get("country", "")
            postal_code = address.get("postal_code", "")
            premises = address.get("premises", "")

            # Extract additional details
            nationality = officer.get("nationality", "")
            occupation = officer.get("occupation", "")
            dob = officer.get("date_of_birth", {})
            dob_year = dob.get("year")
            dob_month = dob.get("month")
            country_of_residence = officer.get("country_of_residence", "")
            person_number = officer.get("person_number", "")

            # Insert into database
            cursor.execute(
                """
                INSERT INTO officers (
                    company_number, name, officer_role, appointed_on, resigned_on,
                    officer_id, address_line_1, address_line_2, locality, region,
                    country, postal_code, premises, nationality, occupation,
                    dob_year, dob_month, country_of_residence, person_number
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    company_number,
                    name,
                    role,
                    appointed_on,
                    resigned_on,
                    officer_id,
                    address_line_1,
                    address_line_2,
                    locality,
                    region,
                    country,
                    postal_code,
                    premises,
                    nationality,
                    occupation,
                    dob_year,
                    dob_month,
                    country_of_residence,
                    person_number,
                ),
            )
            inserted_count += 1

        conn.commit()
        return inserted_count
    except sqlite3.Error as e:
        logger.error(f"Database error for company {company_number}: {e}")
        conn.rollback()
        return 0
    finally:
        conn.close()


def save_progress(
    processed_count: int, total_count: int, latest_company: Optional[str] = None
) -> None:
    """Save progress information to a file."""
    progress = {
        "processed_count": processed_count,
        "total_count": total_count,
        "latest_company": latest_company,
        "timestamp": datetime.now().isoformat(),
    }

    with open("officer_import_progress.log", "a") as f:
        f.write(f"{progress}\n")


def main() -> None:  # noqa: C901
    """Main function to orchestrate the officer import process."""
    config = load_config()
    api_key = config.get("api", {}).get("key", "")

    if not api_key:
        logger.error("API key not found in config or environment variables")
        return

    # Get all companies with strike-off status
    companies = get_companies_with_strike_off()
    total_companies = len(companies)

    if total_companies == 0:
        logger.info("No companies with strike-off status found in the database")
        return

    # Check if we should have a cutoff limit for companies
    MAX_COMPANIES = 10000  # Stop after processing 10K companies for customer delivery

    # Check current progress first
    current_companies_with_officers = get_companies_with_officers_count()
    if current_companies_with_officers >= MAX_COMPANIES:
        logger.info(
            f"Already have officers for {current_companies_with_officers} companies (>= {MAX_COMPANIES})"
        )
        logger.info("Officer import target reached. Ready to enable streaming API.")
        return

    companies_needed = MAX_COMPANIES - current_companies_with_officers
    logger.info(f"Starting import of officers for {total_companies} companies")
    logger.info(f"Need {companies_needed} more companies to reach {MAX_COMPANIES} limit")

    # Process rate limiting
    rate_limit_calls = config.get("rate_limit", {}).get("calls", 600)
    rate_limit_period = config.get("rate_limit", {}).get("period", 300)
    call_delay = rate_limit_period / rate_limit_calls

    # Counters for statistics
    processed_count = 0
    total_officers_count = 0

    # Resume from progress file if it exists
    last_processed_index = 0
    try:
        if os.path.exists("officer_import_progress.log"):
            with open("officer_import_progress.log") as f:
                lines = f.readlines()
                if lines:
                    last_line = lines[-1].strip()
                    import ast

                    progress = ast.literal_eval(last_line)
                    last_processed_index = progress.get("processed_count", 0)
                    logger.info(f"Resuming from company index {last_processed_index}")
    except Exception as e:
        logger.error(f"Error loading progress file: {e}")

    # Process each company
    try:
        for i, company in enumerate(companies):
            # Skip already processed companies
            if i < last_processed_index:
                continue

            company_number = company[0]  # First element of tuple
            company_name = company[1]  # Second element of tuple

            logger.info(
                f"Processing company {i + 1}/{total_companies}: {company_number} - {company_name}"
            )

            # Get officers data
            officers_data = get_company_officers(company_number, api_key)

            if officers_data:
                # Save officers to database
                inserted_count = save_officers_to_db(company_number, officers_data)
                total_officers_count += inserted_count
                logger.info(f"Saved {inserted_count} officers for company {company_number}")
            else:
                logger.info(f"No officers found for company {company_number}")

            # Update progress
            processed_count += 1

            # Check if we've reached the 10K companies limit
            current_total = current_companies_with_officers + processed_count
            if current_total >= MAX_COMPANIES:
                logger.info(f"Reached target of {MAX_COMPANIES} companies with officers!")
                logger.info(
                    f"Total processed: {processed_count}, Total companies with officers: {current_total}"
                )
                logger.info("Officer import complete. Ready to enable streaming API.")
                save_progress(
                    last_processed_index + processed_count, total_companies, company_number
                )
                break

            if processed_count % 10 == 0:
                logger.info(f"Progress: {current_total}/{MAX_COMPANIES} companies with officers")
                save_progress(
                    last_processed_index + processed_count, total_companies, company_number
                )
                logger.info(
                    f"Progress: {last_processed_index + processed_count}/"
                    f"{total_companies} companies processed"
                )

            # Respect rate limits
            time.sleep(call_delay)

    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Error processing companies: {e}")
    finally:
        # Save final progress
        save_progress(last_processed_index + processed_count, total_companies)

        logger.info(f"Import completed. Processed {processed_count} companies.")
        logger.info(f"Total officers imported: {total_officers_count}")


if __name__ == "__main__":
    main()
