"""Companies table operations for PostgreSQL.

This module handles creation and updates of company records in PostgreSQL
from Companies House API data.
"""

import logging
from typing import Any

from .client import Database

logger = logging.getLogger(__name__)


class CompaniesTable:
    """Manages company records in PostgreSQL.

    Provides methods to create and update company records from
    Companies House API responses using PostgreSQL upsert operations.

    Attributes:
        db: PostgreSQL database client
    """

    def __init__(self, db: Database) -> None:
        """Initialize companies table manager.

        Args:
            db: Configured PostgreSQL database client
        """
        self.db = db

    def upsert_company(self, company_data: dict[str, Any]) -> str:
        """Create or update a company record in PostgreSQL.

        Uses INSERT...ON CONFLICT DO UPDATE to handle upserts efficiently.

        Args:
            company_data: Company data from Companies House API

        Returns:
            Company number of the upserted record

        Raises:
            ValueError: If company_number is missing from data
            Exception: If database operation fails
        """
        company_number = company_data.get("company_number")
        if not company_number:
            raise ValueError("company_number is required")

        # Transform Companies House API data to database fields
        fields = self._transform_company_data(company_data)

        logger.info(f"Upserting company: {company_number}")

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO companies (
                        company_number, company_name, company_status, company_type,
                        jurisdiction, date_of_creation, date_of_cessation,
                        registered_office_address, postal_code, locality,
                        sic_codes, ch_url
                    ) VALUES (
                        %(company_number)s, %(company_name)s, %(company_status)s,
                        %(company_type)s, %(jurisdiction)s, %(date_of_creation)s,
                        %(date_of_cessation)s, %(registered_office_address)s,
                        %(postal_code)s, %(locality)s, %(sic_codes)s, %(ch_url)s
                    )
                    ON CONFLICT (company_number) DO UPDATE SET
                        company_name = EXCLUDED.company_name,
                        company_status = EXCLUDED.company_status,
                        company_type = EXCLUDED.company_type,
                        jurisdiction = EXCLUDED.jurisdiction,
                        date_of_creation = EXCLUDED.date_of_creation,
                        date_of_cessation = EXCLUDED.date_of_cessation,
                        registered_office_address = EXCLUDED.registered_office_address,
                        postal_code = EXCLUDED.postal_code,
                        locality = EXCLUDED.locality,
                        sic_codes = EXCLUDED.sic_codes,
                        ch_url = EXCLUDED.ch_url
                    """,
                    fields,
                )

        return company_number

    def _transform_company_data(self, company_data: dict[str, Any]) -> dict[str, Any]:
        """Transform Companies House API data to database field format.

        Maps Companies House API response fields to PostgreSQL table schema.

        Args:
            company_data: Raw company data from CH API

        Returns:
            Dict of database field names to values
        """
        fields: dict[str, Any] = {}

        # Core company identifiers
        fields["company_number"] = company_data.get("company_number", "")
        fields["company_name"] = company_data.get("company_name", "")
        fields["company_status"] = company_data.get("company_status", "")

        # Company type and jurisdiction
        fields["company_type"] = company_data.get("type")
        fields["jurisdiction"] = company_data.get("jurisdiction")

        # Dates
        fields["date_of_creation"] = company_data.get("date_of_creation")
        fields["date_of_cessation"] = company_data.get("date_of_cessation")

        # Registered office address
        if "registered_office_address" in company_data:
            address = company_data["registered_office_address"]
            fields["registered_office_address"] = self._format_address(address)
            fields["postal_code"] = address.get("postal_code")
            fields["locality"] = address.get("locality")
        else:
            fields["registered_office_address"] = None
            fields["postal_code"] = None
            fields["locality"] = None

        # SIC codes
        if "sic_codes" in company_data:
            fields["sic_codes"] = ", ".join(company_data["sic_codes"])
        else:
            fields["sic_codes"] = None

        # Company links
        if "links" in company_data and "self" in company_data["links"]:
            fields["ch_url"] = f"https://beta.companieshouse.gov.uk{company_data['links']['self']}"
        else:
            fields["ch_url"] = None

        return fields

    def _format_address(self, address: dict[str, Any]) -> str:
        """Format address dictionary as single line string.

        Args:
            address: Address dictionary from CH API

        Returns:
            Formatted address string
        """
        parts = []
        for key in [
            "address_line_1",
            "address_line_2",
            "locality",
            "region",
            "postal_code",
            "country",
        ]:
            if key in address and address[key]:
                parts.append(address[key])
        return ", ".join(parts)
